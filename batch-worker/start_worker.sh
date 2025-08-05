#!/bin/bash

unset HTTP_PROXY
unset HTTPS_PROXY
unset http_proxy
unset https_proxy

# Update requests library to ensure compatibility
pip install --upgrade requests
conda update --all

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

# Check for Conda environment with Python 3.12.x
ENV_NAME=$(conda env list | grep -E "^[^#]+$" | awk '{print $1}' | while read env; do
    PYTHON_VERSION=$(conda run -n $env python --version 2>/dev/null)
    if [[ "$PYTHON_VERSION" == "Python 3.12"* ]]; then
        echo $env
        break
    fi
done)

if [ -z "$ENV_NAME" ]; then
    echo "Error: No Conda environment found with Python 3.12.x"
    exit 1
fi

# Activate the Conda environment
conda activate "$ENV_NAME"

# Check if pip command is available
if ! command -v pip &> /dev/null; then
    echo "Error: pip command not found"
    exit 1
fi

# Install dependencies with verbose output and no-input option
timeout 300 pip install -r requirements.txt --verbose --no-input

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

# Define log filename and append it to LOG_DIR
LOG_FILENAME="batch-worker.log"
mkdir -p "${LOG_DIR}${QUEUE_NAME}"
LOG_FILE="${LOG_DIR}${QUEUE_NAME}/${LOG_FILENAME}"
export LOG_FILE

# Start Celery worker with specified queue name and worker name in the background
nohup celery -A batchbe worker -Q ${QUEUE_NAME} -n ${QUEUE_NAME} --loglevel=debug > celery_worker.log 2>&1 &

# Echo success message and exit
echo "Celery worker started successfully with QUEUE_NAME: ${QUEUE_NAME}"

exit 0
