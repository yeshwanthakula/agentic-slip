from redis.asyncio import Redis
from pydantic_settings import BaseSettings
from pydantic import Field
from dotenv import load_dotenv
import json
import logging
load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
class RedisConfig(BaseSettings):
    host: str = Field(...)
    port: int = Field(...)
    db: int = Field(...)
    password: str = Field(...)
    
    model_config = {
        "env_file": ".env",
        "env_file_encoding": "utf-8",
        "env_prefix": "REDIS_"
    }


class RedisClient:
    _instances = {}
    def __init__(self, config: RedisConfig):
        self.config = config.model_dump()
        self.redis = Redis(**self.config)
    
    def __new__(cls, config=None):
        if cls not in cls._instances:
            cls._instances[cls] = super().__new__(cls)
        return cls._instances[cls]
    async def shutdown(self):
        await self.redis.close()
    
    async def publish_to_redis(self, streamer, request_id, response_channel, original_prompt):
        complete_text = ""
        prompt_len = len(original_prompt)
        should_skip = True  # Flag to skip tokens that are part of the prompt
        
        # Stream each token as it's generated
        for text in streamer:
            complete_text += text
            
            # Check if we're still in the original prompt part
            if should_skip:
                if len(complete_text) > prompt_len:
                    # We've gone past the prompt, extract just the new part
                    if complete_text.startswith(original_prompt):
                        text = complete_text[prompt_len:]
                    should_skip = False
                else:
                    # Still within potential prompt match, continue accumulating
                    continue
            
            # Package the token with request_id for correlation on the receiving end
            message = json.dumps({
                "request_id": request_id,
                "token": text,
                "is_finished": False
            })
            # Publish to Redis
            print(message,'\n')
            await self.redis.publish(response_channel, message)
        
        # Send a final message to indicate completion
        logger.info("Tokens pushed to pub sub")
        message = json.dumps({
            "request_id": request_id,
            "token": "",  # Empty token
            "is_finished": True
        })
        await self.redis.publish(response_channel, message)