#!/bin/bash

# Find and kill the Celery Flower process
if pgrep -f "celery -A batchbe flower" > /dev/null; then
    pkill -f "celery -A batchbe flower"
    echo "Celery Flower has been stopped."
else
    echo "No Celery Flower process found."
fi

exit 0