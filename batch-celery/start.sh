#!/bin/bash

# Load environment variables from .env file
if [ -f .env ]; then
    export $(grep -v '^#' .env | xargs)
fi

# Check if Conda is installed and source it
CONDA_PATH=~/miniconda3/etc/profile.d/conda.sh
if [ -f "$CONDA_PATH" ]; then
    source "$CONDA_PATH"
else
    echo "Error: Conda not found at $CONDA_PATH"
    exit 1
fi

# Check if conda command is available
if ! command -v conda &> /dev/null; then
    echo "Error: conda command not found"
    exit 1
fi

# Activate the Conda environment
conda activate celery

# Check if pip command is available
if ! command -v pip &> /dev/null; then
    echo "Error: pip command not found"
    exit 1
fi

# Install dependencies
pip install -r requirements.txt

# Check if .env file exists
if [ ! -f .env ]; then
  echo ".env file not found"
  exit 1
fi

# Export variables from .env file
set -a
source .env
set +a

# Default values
QUEUE_NAME="default"

# Parse arguments
for ARG in "$@"
do
    case $ARG in
        QUEUE_NAME=*)
        QUEUE_NAME="${ARG#*=}"
        ;;
    esac
    shift
done

# Check and echo the provided arguments
echo "Using QUEUE_NAME: ${QUEUE_NAME}"

# Start Celery worker with specified queue name and worker name in the background
nohup celery -A batchbe worker -Q ${QUEUE_NAME} -n ${QUEUE_NAME} --loglevel=info > celery_worker.log 2>&1 &

# Echo success message and exit
echo "Celery worker started successfully with QUEUE_NAME: ${QUEUE_NAME}"

exit 0
