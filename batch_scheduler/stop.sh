#!/bin/bash

# Find and kill the Celery worker process
if pgrep -f "flask run --host 0.0.0.0 --port 5500" > /dev/null; then
    pkill -f "flask run --host 0.0.0.0 --port 5500"
    echo "Stopped batch scheduler successfully"
else
    echo "No batch scheduler process found"
fi

exit 0
