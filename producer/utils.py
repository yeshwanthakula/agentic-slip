from redis.asyncio import Redis,ConnectionPool,RedisError
import os
import logging
import json
import time
import asyncio
from pydantic import BaseModel,Field
from pydantic_settings import BaseSettings
from contextlib import asynccontextmanager
import uuid
from typing import Dict,Tuple,Any
from dotenv import load_dotenv
load_dotenv()
logger = logging.getLogger(__name__)

class RedisConfig(BaseSettings):
    host: str = Field(...)
    port: int = Field(...)
    db: int = Field(...)

    
    model_config = {
        "env_file": ".env",
        "env_file_encoding": "utf-8",
        "env_prefix": "REDIS_"
    }

class RedisService:
    _instancs = {}
    def __init__(self,config:RedisConfig):
        self.config = config
        self.pool = None
    def __new__(cls, *args, **kwargs):
        if cls not in cls._instancs:
            cls._instancs[cls] = super().__new__(cls)
        return cls._instancs[cls]

    def initialise(self):
        if self.pool is None:
            redis_config = self.config.model_dump()
            self.pool =  ConnectionPool(**redis_config)
        logger.info("Redis connection pool started")
        
    async def shutdown(self) -> None:
        """Close all connections in the pool."""
        if self.pool:
            await self.pool.disconnect()
            self.pool = None
            logger.info("Redis connection pool shut down")
            
    @asynccontextmanager
    async def get_connection(self):
        if self.pool is None:
            self.initialise()
            redis_config = RedisConfig()

        redis = Redis(host=self.config.host,
                      port=self.config.port,
                      db=self.config.db,
                      password=None,
                      decode_responses=True,)
        try:
            await redis.ping()
            yield redis
        except ConnectionError as e:
            logger.error(f"Error connecting to Redis: {str(e)}")
            raise
        except Exception as e:
            logger.exception(f"Unexpected Redis error: {e}")
            raise

class RedisStreamService(RedisService):
    """Service for working with Redis Streams."""
    
    async def push_to_stream(
        self, 
        message: Dict[str, Any], 
        stream_key: str = "payload",
    ) -> Tuple[bool, str]:
        """Original method - unchanged"""
        async with self.get_connection() as redis:
            try:
                # Prepare message for Redis (serialize nested objects)
                prepared_message = {}
                for k, v in message.items():
                    if isinstance(v, (dict, list)):
                        prepared_message[k] = json.dumps(v)
                    else:
                        prepared_message[k] = str(v)
                
                prepared_message['timestamp'] = str(int(time.time() * 1000))
                
                # Push to stream with trimming for memory management
                message_id = await redis.xadd(
                    name=stream_key,
                    fields=prepared_message
                )
                
                return True, message_id.decode('utf-8') if isinstance(message_id, bytes) else message_id
                
            except RedisError as e:
                logger.error(f"Redis error while pushing message: {e}")
                return False, f"Queue service unavailable: {str(e)}"
                
            except Exception as e:
                logger.exception(f"Unexpected error while pushing message: {e}")
                return False, f"Internal server error: {str(e)}"
    
    async def push_response_to_stream(
        self,
        response_data: Dict[str, Any],
        stream_key: str
    ) -> Tuple[bool, str]:
        """
        NEW METHOD: Push LLM response tokens to response stream
        Used by consumer to send responses back to producer
        """
        async with self.get_connection() as redis:
            try:
                # Prepare response message
                prepared_message = {}
                for k, v in response_data.items():
                    if isinstance(v, (dict, list)):
                        prepared_message[k] = json.dumps(v)
                    else:
                        prepared_message[k] = str(v)
                
                prepared_message['timestamp'] = str(int(time.time() * 1000))
                
                # Push to response stream
                message_id = await redis.xadd(
                    name=stream_key,
                    fields=prepared_message
                )
                
                return True, message_id.decode('utf-8') if isinstance(message_id, bytes) else message_id
                
            except RedisError as e:
                logger.error(f"Redis error while pushing response: {e}")
                return False, f"Response service unavailable: {str(e)}"
                
            except Exception as e:
                logger.exception(f"Unexpected error while pushing response: {e}")
                return False, f"Internal server error: {str(e)}"
    
    async def consume_response_stream(self, stream_key: str):
        """
        NEW METHOD: Consume responses from stream and yield as SSE
        Replaces the old consume_from_pubsub method
        """
        async with self.get_connection() as redis:
            try:
                last_id = "0"  # Start from beginning of stream
                logger.info(f"Starting to consume from response stream: {stream_key}")
                
                while True:
                    try:
                        # Read from stream
                        messages = await redis.xread(
                            {stream_key: last_id},
                            block=1000  # Block for 1 second
                        )
                        
                        if messages:
                            # Since we're reading from one stream with count=1, we get one stream with one message
                            stream_name, stream_messages = messages[0]
                            message_id, data = stream_messages[0]
                            
                            # Update last_id for next iteration
                            last_id = message_id.decode('utf-8') if isinstance(message_id, bytes) else message_id
                            
                            # Decode message data
                            decoded_data = {}
                            for key, value in data.items():
                                key = key.decode('utf-8') if isinstance(key, bytes) else key
                                value = value.decode('utf-8') if isinstance(value, bytes) else value
                                decoded_data[key] = value
                            
                            # Extract token from the message
                            token = decoded_data.get("token", "")
                            is_finished = decoded_data.get("is_finished", "false").lower() == "true"
                            
                            logger.debug(f"Received token from stream: {token[:50]}...")
                            
                            # Yield in SSE format
                            yield f"data: {token}\n\n"
                            
                            # If finished, break the loop
                            if is_finished:
                                logger.info(f"Stream {stream_key} completed")
                                return
                        
                        # Small delay to prevent excessive polling
                        await asyncio.sleep(0.1)
                        
                    except Exception as e:
                        logger.error(f"Error reading from stream {stream_key}: {e}")
                        # Continue trying to read
                        await asyncio.sleep(1)
                        
            except RedisError as e:
                logger.error(f"Redis error while consuming from stream {stream_key}: {e}")
                raise
            except Exception as e:
                logger.exception(f"Unexpected error while consuming from stream {stream_key}: {e}")
                raise

    # Keep the old method for backward compatibility (not used anymore)
    async def consume_from_pubsub(self, channel_name: str):
        """DEPRECATED: Old pub/sub method - keeping for reference"""
        async with self.get_connection() as redis:
            try:
                pubsub = redis.pubsub()
                print("subscribed")
                await pubsub.subscribe(channel_name)
                async for message in pubsub.listen():
                    print("received message", message)
                    if message['type'] == 'message':
                        data = message['data'].decode('utf-8')
                        data = json.loads(data)
                        print(data)
                        token = data["token"]
                        if data.get("is_finished", True) == True:
                            break
                        yield f"data: {token}\n\n"
                print("finished")
            except RedisError as e:
                logger.error(f"Redis error while consuming message: {e}")
                raise