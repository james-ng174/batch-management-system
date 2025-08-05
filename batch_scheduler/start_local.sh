#!/bin/bash

# Activate the Conda environment
conda activate base

# Check if pip command is available
if ! command -v pip &> /dev/null; then
    echo "Error: pip command not found"
    exit 1
fi

# Install dependencies
timeout 300 pip install --proxy http://hungnv219:17X0Y0%2AKneWT@fsoft-proxy:8080 --upgrade pip
timeout 300 pip install --proxy http://hungnv219:17X0Y0%2AKneWT@fsoft-proxy:8080 --no-cache-dir -r requirements.txt --verbose --no-input

# Check if pip install was successful
if [ $? -ne 0 ]; then
    echo "Error: pip install failed"
    exit 1
fi

# Check if .env file exists
if [ ! -f .env ]; then
  echo ".env file not found"
  exit 1
fi

# Start batch_scheduler
python manage.py db migrate
python manage.py db upgrade
nohup flask run --host 0.0.0.0 --port 5500 &

# Echo success message and exit
echo "Batch scheduler started successfully"

exit 0
