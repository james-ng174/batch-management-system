#!/bin/bash

# Find and kill the Celery beat process
if pgrep -f "celery -A batchbe beat" > /dev/null; then
    pkill -f "celery -A batchbe beat"
    echo "Celery Beat has been stopped."
else
    echo "No Celery Beat process found."
fi

exit 0