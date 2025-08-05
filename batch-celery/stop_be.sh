#!/bin/bash

# Find and kill the Celery backend process
if pgrep -f "python manage.py runserver" > /dev/null; then
    pkill -f "python manage.py runserver"
    echo "Celery Backend has been stopped."
else
    echo "No Celery Backend process found."
fi

exit 0