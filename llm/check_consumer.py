import redis


def main():
    # Connect to Redis
    r = redis.Redis( host='127.0.0.1',
        port=6379,
        db=0,
        password='password')
    
    # Create a pubsub object
    pubsub = r.pubsub()
    
    # Subscribe to a channel
    channel_name = '123'
    pubsub.subscribe(channel_name)
    
    print(f'Subscribed to {channel_name}. Waiting for messages...')
    
    # Listen for messages
    for message in pubsub.listen():
        if message['type'] == 'message':
            data = message['data'].decode('utf-8')
            print(f'Received: {data}')

if __name__ == '__main__':
    main()