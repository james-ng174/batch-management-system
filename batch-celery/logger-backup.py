import logging
import logging.config
import yaml
import concurrent_log_handler  # Ensure this import is here to register the handler class
import os

logging_config = os.getenv('LOG_CONFIG_FILE', 'logging-file.yaml')

# Load logging configuration from YAML file
with open(logging_config, 'r') as f:
    config = yaml.safe_load(f)

try:
    # Apply the logging configuration
    logging.config.dictConfig(config)
    # Log a message to verify connection
    logging.getLogger('batch-celery-logger').info('Logging system initialized successfully.')
except Exception as e:
    print("Failed to configure logging: ", str(e))

# Create a logger
logger = logging.getLogger('batch-celery-logger')


# Function to get the logger
def get_logger():
    return logger
