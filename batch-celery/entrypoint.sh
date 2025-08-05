#!/bin/sh

set -e

# Run database migrations
python manage.py makemigrations celerytasks
python manage.py migrate

# Start the Django development server
exec "$@"
