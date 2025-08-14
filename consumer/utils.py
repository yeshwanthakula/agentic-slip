from pydantic_settings import BaseSettings
from pydantic import Field
from redis.asyncio import Redis,ConnectionPool,RedisError,ResponseError
import logging
from dotenv import load_dotenv
import asyncio
load_dotenv()
logger = logging.getLogger(__name__)    

class RedisConfig(BaseSettings):
    host: str = Field(...)
    port: int = Field(...)
    db: int = Field(...)
    
    model_config = {
        "env_file": ".env",
        "env_file_encoding": "utf-8",
        "env_prefix": "REDIS_",
        "extra":"ignore"
    }




class RedisService:
    _instances = {}
    def __init__(self,config:RedisConfig):
        redis_config = config.model_dump()
        self.pool =  ConnectionPool(**redis_config)
        self.redis = Redis(connection_pool=self.pool)
    def __new__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super().__new__(cls)
        return cls._instances[cls]


    async def shutdown(self) -> None:
        """Close all connections in the pool."""
        if self.pool:
            self.redis=None
            self.pool = None
            logger.info("Redis connection pool shut down")
    
    async def get_connection(self):
        
        try:
            return self.redis

        except ConnectionError as e:
            logger.error(f"Error connecting to Redis: {str(e)}")
            raise
        except Exception as e:
            logger.exception(f"Unexpected Redis error: {e}")
            raise
        


# class RedisConsumer(RedisService):
#     def __init__(self, config: RedisConfig):
#         super().__init__(config)

#     async def consume_message(self, stream_key: str = "payload"):

#             try:
#                 while(True):
#                     if(redis) == None:
#                         redis = await self.get_connection()
#                     message = await redis.xread(
#                         streams=stream_key,
#                         count=1,
#                         block=1000
#                     )
#                     yield message
#                     asyncio.sleep(1)
               
#             except RedisError as e:
#                 logger.error(f"Redis error while consuming message: {e}")
#                 raise




class RedisConsumerSerivce(RedisService):
    def __init__(self, config: RedisConfig):
        super().__init__(config)
        self._initialized_groups = set() # Track which groups have been initialized
    
    async def init_consumer_group(self, stream_key: str, group_name: str, start_id: str = "0"):
        """Initialize a consumer group if it doesn't exist and hasn't been initialized yet."""
        # Create a unique key for this stream+group combination
        group_key = f"{stream_key}:{group_name}"
        
        # Skip if we've already initialized this group in this instance
        if group_key in self._initialized_groups:
            return
            
        # self.redis = await self.get_connection()
        print("redis", self.redis)
        try:
            # Create the consumer group - will also create the stream if needed
            await self.redis.xgroup_create(stream_key, group_name, start_id, mkstream=True)
            logger.info(f"Created consumer group '{group_name}' for stream '{stream_key}'")
            # Mark this group as initialized
            self._initialized_groups.add(group_key)
        except ResponseError as e:
            # Ignore if the group already exists
            if "BUSYGROUP" in str(e):
                logger.info(f"Consumer group '{group_name}' already exists")
                # Still mark as initialized since it exists
                self._initialized_groups.add(group_key)
            else:
                logger.error(f"Error creating consumer group: {e}")
                raise

    # async def consume_from_group(self, stream_key: str, group_name: str, consumer_name: str):
    #     """Consume messages using a consumer group."""
    #     # Initialize the group if needed (will be skipped if already done)
    #     await self.init_consumer_group(stream_key, group_name)
    #     try:
    #         while True:
    #             # Read new messages for this consumer
    #             messages = await self.redis.xreadgroup(
    #                 group_name,
    #                 consumer_name,
    #                 {stream_key: '>'},  # '>' means all new messages not yet delivered to this consumer
    #                 count=1,
    #                 block=2000
    #             )
                
    #             if messages:
    #                 for stream_name, stream_messages in messages:
    #                     for message_id, data in stream_messages:
    #                         # Process the message
    #                         yield (message_id, data)
                            
    #                         # Acknowledge the message
    #                         await self.redis.xack(stream_key, group_name, message_id)
                
    #             await asyncio.sleep(0.1)
    #     except RedisError as e:
    #         logger.error(f"Redis error while consuming message: {e}")
    #         raise
    async def consume_from_group(self, stream_key: str, group_name: str, consumer_name: str):
        """Consume messages using a consumer group - FIXED VERSION"""
        await self.init_consumer_group(stream_key, group_name)
        try:
            while True:
                messages = await self.redis.xreadgroup(
                    group_name,
                    consumer_name,
                    {stream_key: '>'},
                    count=1,
                    block=2000
                )
                
                if messages:
                    for stream_name, stream_messages in messages:
                        for message_id, data in stream_messages:
                            # Process the message
                             yield (message_id, data)
                            
                            # ONLY acknowledge if processing was successful
                            # if success:
                            #     await self.redis.xack(stream_key, group_name, message_id)
                            #     logger.info(f"Successfully processed and ACK'd message {message_id}")
                            # else:
                            #     logger.warning(f"Failed to process message {message_id}, leaving in pending state")
                
                await asyncio.sleep(0.1)
        except RedisError as e:
            logger.error(f"Redis error while consuming message: {e}")
            raise
    async def ack_message(self, stream_key: str, group_name: str, message_id: str):
        """Manually ACK a message after successful processing"""
        try:
            await self.redis.xack(stream_key, group_name, message_id)
            logger.info(f"ACK'd message {message_id}")
        except Exception as e:
            logger.error(f"Failed to ACK message {message_id}: {e}")
        
    
        # Rest of the consumption logic...