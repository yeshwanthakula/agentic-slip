import asyncio
from redis.asyncio import Redis
from pprint import pprint


async def inspect_redis():
    # Connect to Redis with your configuration
    r = Redis(
        host='127.0.0.1',
        port=6379,
        db=0,
        password='password'
    )
    
    # Get all keys
    keys = await r.keys('*')
    print(f"Found {len(keys)} keys:")
    
    # Inspect each key
    results = {}
    for key in keys:
        key_str = key.decode('utf-8')
        key_type = await r.type(key)
        key_type_str = key_type.decode('utf-8')
        
        # Get value based on type
        if key_type_str == 'string':
            value = await r.get(key)
            if value:
                value = value.decode('utf-8')
            results[key_str] = value
        elif key_type_str == 'hash':
            value = await r.hgetall(key)
            decoded_value = {k.decode('utf-8'): v.decode('utf-8') for k, v in value.items()}
            results[key_str] = decoded_value
        elif key_type_str == 'list':
            value = await r.lrange(key, 0, -1)
            results[key_str] = [item.decode('utf-8') for item in value]
        elif key_type_str == 'set':
            value = await r.smembers(key)
            results[key_str] = [item.decode('utf-8') for item in value]
        elif key_type_str == 'zset':
            value = await r.zrange(key, 0, -1, withscores=True)
            results[key_str] = [(item[0].decode('utf-8'), item[1]) for item in value]
        elif key_type_str == 'stream':
            # Read all entries from the stream
            stream_entries = await r.xrange(key, '-', '+')
            
            # Decode the stream entries
            decoded_entries = []
            for entry_id, fields in stream_entries:
                entry_id = entry_id.decode('utf-8')
                decoded_fields = {k.decode('utf-8'): v.decode('utf-8') 
                                 for k, v in fields.items()}
                decoded_entries.append((entry_id, decoded_fields))
            
            results[key_str] = {
                'type': 'stream',
                'entries': decoded_entries,
                'length': len(decoded_entries)
            }
        else:
            results[key_str] = f"Unsupported type: {key_type_str}"
    
    # Pretty print the results
    pprint(results)
    
    # Close the connection
    await r.close()

if __name__ == "__main__":
    asyncio.run(inspect_redis())