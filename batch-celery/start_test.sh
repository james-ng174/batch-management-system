#!/bin/bash
# Activate the Conda environment
conda activate base

# Check if pip command is available
if ! command -v pip &> /dev/null; then
    echo "Error: pip command not found"
    exit 1
fi

# Check if .env file exists
if [ ! -f .env ]; then
  echo ".env file not found"
  exit 1
fi

# Export variables from .env file
set -a
source .env
set +a

# Define log filename and append it to LOG_DIR
LOG_FILENAME="batch-be.log"
LOG_FILE="${LOG_DIR}${LOG_FILENAME}"
export LOG_FILE

# Check if pip install was successful
if [ $? -ne 0 ]; then
    echo "Error: pip install failed"
    exit 1
fi

# Run database migrations
python manage.py makemigrations celerytasks
python manage.py migrate

python manage.py test celerytasks.tests.RruleScheduleTaskTestCase

exit 0
