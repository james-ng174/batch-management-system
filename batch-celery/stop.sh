#!/bin/bash
# The stop.sh script is used to stop a Celery worker process.
# It takes an optional argument QUEUE_NAME, which specifies the queue name of the worker to be stopped.
# If no queue name is provided, the default queue name is used.
# Default values
QUEUE_NAME="default"

# Parse arguments
for ARG in "$@"
do
    case $ARG in
        QUEUE_NAME=*)
        QUEUE_NAME="${ARG#*=}"
        ;;
        *)
        OTHER_ARGUMENTS+=("$1")
        ;;
    esac
    shift
done

# Find and kill the Celery worker process
if pgrep -f "celery -A batchbe worker -Q ${QUEUE_NAME}" > /dev/null; then
    pkill -f "celery -A batchbe worker -Q ${QUEUE_NAME}"
    echo "Celery worker with QUEUE_NAME: ${QUEUE_NAME} has been stopped."
else
    echo "No Celery worker found with QUEUE_NAME: ${QUEUE_NAME}."
fi

exit 0
