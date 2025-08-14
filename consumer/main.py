from redis.asyncio import Redis
from utils import RedisConfig,RedisService,RedisConsumerSerivce
import asyncio
import os
import json
import aiohttp
import logging
from fastapi import FastAPI
from dotenv import load_dotenv
load_dotenv()

app = FastAPI()

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

print(os.getenv("LLM_SERVICE_URL"))
LLM_SERVICE_URL = os.getenv("LLM_SERVICE_URL")

redis_config = RedisConfig()
redis_consumer = RedisConsumerSerivce(redis_config)

# Create a separate Redis client for sending responses
response_redis = Redis(
    host=redis_config.host,
    port=redis_config.port,
    # password=redis_config.password,
    db=redis_config.db,
    decode_responses=True,
    password=None
)

async def send_response_to_stream(request_id: str, token: str, is_finished: bool = False):
    """
    NEW FUNCTION: Send LLM response token to response stream
    This replaces the old pub/sub publishing
    """
    try:
        response_data = {
            "token": token,
            "is_finished": str(is_finished).lower(),
            "timestamp": str(int(asyncio.get_event_loop().time() * 1000))
        }
        
        # Use the response stream key format: "response_" + request_id
        stream_key = f"response_{request_id}"
        
        # Add to stream
        message_id = await response_redis.xadd(stream_key, response_data)
        logger.debug(f"Added response to stream {stream_key}: {token[:50]}...")
        
        return True
        
    except Exception as e:
        logger.error(f"Error sending response to stream: {e}")
        return False

async def process_llm_request(payload: dict):
    """
    MODIFIED: Handles OpenAI-compatible vLLM streaming responses
    """
    request_id = payload.get('request_id')
    if not request_id:
        logger.error("No request_id found in payload")
        return
    
    logger.info(f"Processing request {request_id}")
    
    try:
        # Prepare OpenAI-compatible request for vLLM
        vllm_request = {
            "model": payload.get("model", "default"),  # vLLM model name
            "messages": payload.get("messages", [{"role": "user", "content": payload.get("prompt", "")}]),
            "max_tokens": payload.get("max_tokens", 100),
            "temperature": payload.get("temperature", 0.7),
            "stream": True  # Enable streaming
        }
        
        logger.info(f"Sending to vLLM: {vllm_request}")
        
        # Send to vLLM's OpenAI-compatible chat/completions endpoint
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{LLM_SERVICE_URL}/v1/chat/completions", 
                json=vllm_request, 
                headers={"Content-Type": "application/json"}
            ) as response:
                logger.info(f"vLLM service response status: {response.status}")
                
                if response.status != 200:
                    error_text = await response.text()
                    await send_response_to_stream(
                        request_id, 
                        f"Error: vLLM service returned {response.status}: {error_text}", 
                        is_finished=True
                    )
                    return
                
                # Process OpenAI-style streaming response
                async for line in response.content:
                    if line:
                        line_str = line.decode('utf-8').strip()
                        
                        # Handle Server-Sent Events format
                        if line_str.startswith('data: '):
                            data_str = line_str[6:]  # Remove 'data: ' prefix
                            
                            # Check for completion signal
                            if data_str == '[DONE]':
                                await send_response_to_stream(request_id, "", is_finished=True)
                                logger.info(f"Completed processing request {request_id}")
                                return True
                            
                            try:
                                # Parse OpenAI streaming response
                                chunk = json.loads(data_str)
                                
                                # Extract content from OpenAI format
                                if 'choices' in chunk and len(chunk['choices']) > 0:
                                    choice = chunk['choices'][0]
                                    delta = choice.get('delta', {})
                                    
                                    # Get the content token from delta
                                    content = delta.get('content', '')
                                    
                                    # Send token to stream (only if there's actual content)
                                    if content:
                                        await send_response_to_stream(request_id, content)
                                    
                                    # Check if this is the last chunk
                                    if choice.get('finish_reason') is not None:
                                        await send_response_to_stream(request_id, "", is_finished=True)
                                        logger.info(f"Stream finished for request {request_id}")
                                        return True
                                        
                            except json.JSONDecodeError as e:
                                logger.warning(f"Failed to parse JSON from vLLM: {e}")
                                continue
                
    except aiohttp.ClientConnectorError as e:
        logger.error(f"Cannot connect to vLLM service: {e}")
        await send_response_to_stream(
            request_id,
            "Error: vLLM service unavailable, request will be retried",
            is_finished=True
        )
        return False  # Don't ACK - let KEDA scale up vLLM
        
    except Exception as e:
        logger.error(f"Error processing vLLM request {request_id}: {e}")
        await send_response_to_stream(
            request_id,
            f"Error: {str(e)}",
            is_finished=True
        )
        return False

async def main():
    """UNCHANGED: Main consumer loop"""
    async for message_id, message_data in redis_consumer.consume_from_group(
            stream_key="payload", 
            group_name="group1", 
            consumer_name="consumer1"
        ):
        logger.info(f"Received message: {message_id}")
        
        # Decode message (your existing logic)
        decoded_dict = {}
        for key, item in message_data.items():
            key = key.decode("utf-8") if isinstance(key, bytes) else key
            item = item.decode("utf-8") if isinstance(item, bytes) else item
            decoded_dict[key] = item
        
        logger.info(f"Processing payload with request_id: {decoded_dict.get('request_id', 'unknown')}")
        
        # Process the request asynchronously
        success = await process_llm_request(decoded_dict)
        
        # ONLY ACK if successful
        if success:
            await redis_consumer.ack_message("payload", "group1", message_id)
            logger.info(f"✅ ACK'd message {message_id}")
        else:
            logger.warning(f"❌ Left message {message_id} pending for retry")


if __name__ == "__main__":
    print("Starting Redis consumer...")
    asyncio.run(main())
    # uvicorn.run(app, host="0.0.0.0", port=8000)