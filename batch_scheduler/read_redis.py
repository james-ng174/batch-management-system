import redis
import argparse
import pickle
from kombu.serialization import registry


def deserialize_task_message(encoded_message):
    # Unpickle the message to get a tuple of (body, headers, properties)
    message = pickle.loads(encoded_message)

    # The message is a tuple: (body, headers, properties)
    body = message[0]
    # headers = message[1]  # Not used in this case
    # properties = message[2]  # Not used since we're hardcoding

    # Hardcoded content_type and content_encoding
    content_type = 'application/json'  # Adjust based on your serializer
    content_encoding = 'utf-8'  # Adjust based on your encoding

    # Use Kombu's registry to deserialize the body
    payload = registry.loads(body, content_type, content_encoding)
    return payload


def get_all_tasks_from_queue(redis_host, redis_port, queue_name):
    # Connect to the Redis server
    client = redis.StrictRedis(host=redis_host, port=redis_port)

    try:
        # Get the length of the queue (list)
        queue_length = client.llen(queue_name)
        if queue_length == 0:
            print(f"The queue '{queue_name}' is empty.")
            return

        # Fetch all messages in the queue using LRANGE
        messages = client.lrange(queue_name, 0, -1)  # Fetch all items from the start (0) to the end (-1)

        print(f"Retrieved {len(messages)} tasks from the queue '{queue_name}':\n")
        for idx, encoded_message in enumerate(messages, 1):
            try:
                # Deserialize the task message
                task = deserialize_task_message(encoded_message)
                print(f"Task {task}:")
            except Exception as e:
                print(f"Failed to deserialize task {idx}: {e}\n")
        print("Done.")
    except redis.ConnectionError as e:
        print(f"Redis connection error: {e}")
    except redis.RedisError as e:
        print(f"Redis error: {e}")


if __name__ == "__main__":
    # Argument parser to handle command-line arguments
    parser = argparse.ArgumentParser(description="Retrieve all tasks from a specific Redis queue.")
    parser.add_argument('--queue_name', type=str, required=True,
                        help="The name of the Redis queue to retrieve tasks from.")
    parser.add_argument('--redis_host', type=str, default='localhost', help="The Redis server hostname or IP address.")
    parser.add_argument('--redis_port', type=int, default=6379, help="The Redis server port.")

    # Parse the command-line arguments
    args = parser.parse_args()

    # Call the function with arguments from the command line
    get_all_tasks_from_queue(redis_host=args.redis_host, redis_port=args.redis_port, queue_name=args.queue_name)
