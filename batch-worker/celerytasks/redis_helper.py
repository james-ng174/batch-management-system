import os
import redis
from dotenv import load_dotenv

# Load environment variables from a .env file (optional)
load_dotenv()

# Configure Redis connection from environment variables
REDIS_HOST = os.getenv('REDIS_HOST', '127.0.0.1')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
REDIS_DB = int(os.getenv('REDIS_DB', 0))
REDIS_PASSWORD = os.getenv('REDIS_PASSWORD', None)

from logger import get_logger

logger = get_logger()

# Initialize Redis client
redis_client = redis.StrictRedis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    db=REDIS_DB,
    password=REDIS_PASSWORD,
    decode_responses=True  # Decode responses from bytes to strings
)

# Test connection to Redis
try:
    redis_client.ping()
    logger.info("Connected to Redis!")
except redis.ConnectionError as e:
    logger.error(f"Failed to connect to Redis: {e}")


# --- List Functions ---

def add_to_list(key, value):
    """
    Add an item to a Redis list.
    """
    try:
        redis_client.rpush(key, value)  # Append to the list
        logger.info(f"Added {value} to list with key {key}")
    except Exception as e:
        logger.error(f"Error adding to list: {e}")


def remove_from_list(key, value):
    """
    Remove an item from a Redis list.
    """
    try:
        redis_client.lrem(key, 1, value)  # Remove first occurrence
        logger.info(f"Removed {value} from list with key {key}")
    except Exception as e:
        logger.error(f"Error removing from list: {e}")


def get_list(key):
    """
    Retrieve all items from a Redis list.
    """
    try:
        return redis_client.lrange(key, 0, -1)  # Retrieve all items
    except Exception as e:
        logger.error(f"Error getting list: {e}")
        return None


# --- Set Functions ---

def add_to_set(key, value):
    """
    Add an item to a Redis set.
    """
    try:
        redis_client.sadd(key, value)  # Add to set
        logger.info(f"Added {value} to set with key {key}")
    except Exception as e:
        logger.error(f"Error adding to set: {e}")


def remove_key(key):
    """
    Remove a key from Redis.

    :param key: The key to remove
    :return: Boolean indicating whether the key was successfully removed
    """
    try:
        result = redis_client.delete(key)
        if result == 1:
            logger.info(f"Key '{key}' successfully removed.")
            return True
        else:
            logger.error(f"Key '{key}' does not exist.")
            return False
    except Exception as e:
        logger.error(f"Error removing key '{key}': {e}")
        return False


def remove_from_set(key, value):
    """
    Remove an item from a Redis set.
    """
    try:
        redis_client.srem(key, value)  # Remove from set
        logger.info(f"Removed {value} from set with key {key}")
    except Exception as e:
        logger.error(f"Error removing from set: {e}")


def get_set(key):
    """
    Retrieve all items from a Redis set as a list.
    """
    try:
        redis_set = redis_client.smembers(key)  # Get all items in the set (returns a set)
        return list(redis_set)  # Convert the set to a list
    except Exception as e:
        logger.error(f"Error getting set: {e}")
        return []


# --- Hash Functions ---

def add_to_hash(key, field, value):
    """
    Add or update an item in a Redis hash.
    """
    try:
        redis_client.hset(key, field, value)  # Set field in hash
        logger.info(f"Added {field}: {value} to hash with key {key}")
    except Exception as e:
        logger.error(f"Error adding to hash: {e}")


def remove_from_hash(key, field):
    """
    Remove a field from a Redis hash.
    """
    try:
        redis_client.hdel(key, field)  # Delete field from hash
        logger.info(f"Removed field {field} from hash with key {key}")
    except Exception as e:
        logger.error(f"Error removing from hash: {e}")


def get_hash(key):
    """
    Retrieve all fields and values from a Redis hash.
    """
    try:
        return redis_client.hgetall(key)  # Get all fields and values
    except Exception as e:
        logger.error(f"Error getting hash: {e}")
        return None


def get_field_from_hash(key, field):
    """
    Retrieve a specific field from a Redis hash.
    """
    try:
        return redis_client.hget(key, field)  # Get a specific field
    except Exception as e:
        logger.error(f"Error getting field from hash: {e}")
        return None


def delete_keys_by_pattern(pattern):
    """
    Delete keys from Redis that match a specific pattern.

    :param pattern: The pattern to match keys (e.g., 'prefix:*')
    :return: The number of keys deleted
    """
    try:
        keys = redis_client.scan_iter(match=pattern)  # Find keys matching the pattern
        deleted_count = 0

        for key in keys:
            redis_client.delete(key)
            deleted_count += 1

        logger.info(f"Deleted {deleted_count} keys matching pattern '{pattern}'.")
        return deleted_count
    except Exception as e:
        logger.error(f"Error deleting keys with pattern '{pattern}': {e}")
        return 0


def get_all_by_pattern(pattern):
    """
    Get all keys and their values from Redis that match a specific pattern.

    :param pattern: The pattern to match keys (e.g., 'prefix:*')
    :return: A dictionary with keys and their corresponding values
    """
    try:
        matching_keys = list(redis_client.scan_iter(match=pattern))  # Find keys matching the pattern
        result = {}

        for key in matching_keys:
            value = redis_client.get(key)  # Fetch the value for each key
            result[key] = value

        print(f"Found {len(result)} keys matching pattern '{pattern}'.")
        return result
    except Exception as e:
        print(f"Error retrieving keys with pattern '{pattern}': {e}")
        return {}


def set_key(key, value):
    """
    Set a simple key-value pair in Redis.

    :param key: The Redis key
    :param value: The value to store (string)
    :return: None
    """
    try:
        redis_client.set(key, value)
        logger.info(f"Set key '{key}' with value '{value}'.")
    except Exception as e:
        logger.error(f"Error setting key '{key}': {e}")


def get_key(key):
    """
    Get the value of a simple key from Redis.

    :param key: The Redis key
    :return: The value of the key, or None if the key does not exist
    """
    try:
        value = redis_client.get(key)
        if value is None:
            logger.warning(f"Key '{key}' does not exist.")
        else:
            logger.info(f"Retrieved key '{key}' with value '{value}'.")
        return value
    except Exception as e:
        logger.error(f"Error retrieving key '{key}': {e}")
        return None


def get_keys_by_pattern(pattern):
    """
    Get a list of keys matching a specific pattern from Redis.

    :param pattern: The pattern to match keys (e.g., 'prefix:*')
    :return: A list of matching keys
    """
    try:
        keys = list(redis_client.scan_iter(match=pattern))  # Retrieve keys matching the pattern
        print(f"Found {len(keys)} keys matching pattern '{pattern}'.")
        return keys
    except Exception as e:
        print(f"Error retrieving keys with pattern '{pattern}': {e}")
        return []
