from fastapi import FastAPI,Request
from fastapi.responses import StreamingResponse
import uvicorn
from utils import RedisConfig,RedisStreamService
from contextlib import asynccontextmanager
import logging
import os
import uuid
import json
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

redis_config = RedisConfig()
redis_service = RedisStreamService(redis_config)

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    using this context manager to ensure redis is set up and running before serving the requests
    """
    redis_service.initialise()
    logger.info("Redis connection pool started")
    yield
    await redis_service.shutdown()
    logger.info("Redis connection pool shut down")

app = FastAPI(lifespan=lifespan)

@app.get("/")
async def root():
    return {"message": "Hello World"}

@app.post("/generate")
async def generate_text(request: Request):
    data = await request.json()

    
    # Generate unique request ID
    request_id = str(uuid.uuid4())
    
    # Add request_id to the payload
    data['request_id'] = request_id
    
    # Push to request stream (existing flow)
    success, message_id = await redis_service.push_to_stream(data)
    
    if success:
        print(f"Message pushed to queue with request_id: {request_id}")
        # Stream responses from the response stream for this specific request
        return StreamingResponse(
            redis_service.consume_response_stream(f"response_{request_id}"), 
            media_type="text/event-stream"
        )
    else:
        return {"message": "Error pushing message to queue"}

if __name__ == "__main__":
    print("Hi")
    uvicorn.run(app, host="0.0.0.0", port=8001)