from fastapi import FastAPI, HTTPException
from transformers import GPT2LMHeadModel, GPT2Tokenizer , TextIteratorStreamer
from pydantic import BaseModel
import torch
import logging
from redis.asyncio import Redis
from threading import Thread
from utils import RedisClient , RedisConfig
from contextlib import asynccontextmanager
import asyncio

    

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)






try:
    redis_config = RedisConfig()
    redis_client  = RedisClient(redis_config)
    logger.info("Redis client initialized")

    
except Exception as e:
    logger.error(f"Error initializing Redis client: {str(e)}")
    raise



app = FastAPI()

# Model and tokenizer initialization
MODEL_NAME = "distilgpt2" 
MODEL_PATH = "app/model" # This is the 124M parameter version

try:
    tokenizer = GPT2Tokenizer.from_pretrained(MODEL_NAME, bos_token='<|startoftext|>', eos_token='<|endoftext|>', pad_token='<|pad|>')
    model = GPT2LMHeadModel.from_pretrained(MODEL_NAME)
    special_tokens = {
        'pad_token': '<|pad|>',
        'bos_token': '<|startoftext|>',
        'eos_token': '<|endoftext|>'
    }
    model.resize_token_embeddings(len(tokenizer))
    tokenizer.add_special_tokens(special_tokens)
    logger.info("Model and tokenizer loaded successfully")
except Exception as e:
    logger.error(f"Error loading model: {str(e)}")
    raise

class GenerationRequest(BaseModel):
    prompt: str
    max_length: int = 50
    temperature: float = 0.7
    request_id:str

@app.post("/llm")
async def generate_text(request: GenerationRequest):
    try:
        print("received request", request)
        formatted_prompt = f"{tokenizer.bos_token}{request.prompt}" if tokenizer.bos_token else request.prompt
        inputs = tokenizer(
            formatted_prompt,
            padding=True,
            truncation=True,
            return_tensors="pt"
        )
        streamer = TextIteratorStreamer(tokenizer, skip_special_tokens=True)
        generation_kwargs = {
        'input_ids': inputs['input_ids'],
        'max_length': request.max_length,
        'temperature': request.temperature,
        'do_sample': True,
        'top_k': 40,
        'top_p': 0.9,
        'repetition_penalty': 1.2,
        'num_return_sequences': 1,
        'pad_token_id': tokenizer.pad_token_id,
        'bos_token_id': tokenizer.bos_token_id,
        'eos_token_id': tokenizer.eos_token_id,
        'no_repeat_ngram_size': 3,
        'min_length': 20,
        'length_penalty': 1.0,
        'streamer': streamer
    }
        def generate_with_no_grad():
            with torch.no_grad():
                model.generate(**generation_kwargs)
            
    # Create thread for model generation
        thread = Thread(target=generate_with_no_grad)
        thread.start()
# Clean up the generated text
        # generated_text = tokenizer.decode(outputs[0], skip_special_tokens=True)
        # generated_text = generated_text.strip()
    
    # Remove the original prompt if it appears at the start
        # if generated_text.startswith(request.prompt):
        #     generated_text = generated_text[len(request.prompt):].strip()
        channel =  str(request.request_id)
        asyncio.create_task(redis_client.publish_to_redis(streamer, request.request_id,channel , request.prompt))
    
    # Decode and return response
        return {"status": "generation_started", "request_id": request.request_id}
    
    except Exception as e:
        logger.error(f"Generation error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
async def health_check():
    return {"status": "healthy"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)