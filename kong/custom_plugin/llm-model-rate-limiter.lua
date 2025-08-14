-- llm-model-rate-limiter.lua
-- Custom Kong plugin to apply different rate limits based on LLM model types

local typedefs = require "kong.db.schema.typedefs"
local redis = require "resty.redis"

local LlmModelRateLimiter = {
  PRIORITY = 901,  -- Run after authentication but before default rate limiting
  VERSION = "1.0.0",
}

-- Plugin configuration schema
LlmModelRateLimiter.schema = {
  name = "llm-model-rate-limiter",
  fields = {
    { consumer = typedefs.no_consumer },
    { protocols = typedefs.protocols_http },
    { config = {
        type = "record",
        fields = {
          { llama_limit_minute = { type = "number", default = 30, required = true } },
          { gpt2_limit_minute = { type = "number", default = 120, required = true } },
          { default_limit_minute = { type = "number", default = 60, required = true } },
          { redis_host = { type = "string", default = "localhost" } },
          { redis_port = { type = "number", default = 6379 } },
          { redis_timeout = { type = "number", default = 2000 } },
          { redis_database = { type = "number", default = 0 } },
        },
      },
    },
  },
}

-- Initialize the plugin
function LlmModelRateLimiter:init_worker()
  kong.log.debug("Initializing LLM Model Rate Limiter plugin")
end

-- Main access function that runs on each request
function LlmModelRateLimiter:access(conf)
  -- Get request body to check for LLM model type
  local body, err = kong.request.get_body()
  
  if err then
    kong.log.err("Error reading request body: ", err)
    return
  end
  
  -- Default rate limit
  local rate_limit = conf.default_limit_minute
  
  -- Check if body exists and contains model information
  if body and body.llm then
    local model_name = body.llm:lower()
    
    -- Apply different rate limits based on model type
    if string.find(model_name, "llama") then
      rate_limit = conf.llama_limit_minute
      kong.log.debug("Applied Llama model rate limit: ", rate_limit)
    elseif string.find(model_name, "gpt2") then
      rate_limit = conf.gpt2_limit_minute
      kong.log.debug("Applied GPT-2 model rate limit: ", rate_limit)
    else
      kong.log.debug("Applied default model rate limit: ", rate_limit)
    end
  end
  
  -- Get consumer ID if authenticated
  local consumer_id = "anonymous"
  local credential = kong.client.get_credential()
  if credential then
    consumer_id = credential.consumer.id
  end
  
  -- Create a unique identifier for this client+model combination
  local identifier = consumer_id .. ":" .. (body and body.llm or "default")
  
  -- Connect to Redis for rate limiting
  local red = redis:new()
  red:set_timeout(conf.redis_timeout)
  
  local ok, err = red:connect(conf.redis_host, conf.redis_port)
  if not ok then
    kong.log.err("Failed to connect to Redis: ", err)
    return
  end
  
  -- Select the Redis database
  local ok, err = red:select(conf.redis_database)
  if not ok then
    kong.log.err("Failed to select Redis database: ", err)
    return
  end
  
  -- Implement rate limiting logic using Redis
  local key = "ratelimit:llm:" .. identifier .. ":minute"
  local current_usage, err = red:incr(key)
  
  if err then
    kong.log.err("Failed to increment rate limit counter: ", err)
    return
  end
  
  -- Set expiration for the counter (60 seconds for a minute window)
  if current_usage == 1 then
    red:expire(key, 60)
  end
  
  -- Set rate limit headers
  kong.response.set_header("X-RateLimit-Limit-Minute", rate_limit)
  kong.response.set_header("X-RateLimit-Remaining-Minute", math.max(0, rate_limit - current_usage))
  
  -- Block the request if it exceeds the rate limit
  if current_usage > rate_limit then
    return kong.response.exit(429, {
      message = "API rate limit exceeded for this model type"
    })
  end
  
  -- Release the Redis connection
  local ok, err = red:set_keepalive(10000, 100)
  if not ok then
    kong.log.err("Failed to set Redis keepalive: ", err)
  end
end

-- Return the plugin
return LlmModelRateLimiter