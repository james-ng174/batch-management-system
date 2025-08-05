# Batchbe Django Celery App

This project is a Django application integrated with Celery for background task processing. 

It uses Redis as the message broker and PostgreSQL as the result backend. The application supports dynamic task scheduling with interval and cron-like schedules, and it includes endpoints for CRUD operations on these tasks. Docker is used to containerize the application.

## Table of Contents
- Requirements
- Installation
- Configuration
- Running the Application
- API Endpoints
- Usage
- Monitoring
- References

## Requirements
- Docker
- Docker Compose
- PostgreSQL (running on host machine)
- Python 3.12

## Installation
#### 1. Set up PostgreSQL on your host machine:

Ensure that PostgreSQL is running and accessible from your host machine.

Configure environment variables:

Update the environment variables in docker-compose.yml with your PostgreSQL credentials:

```yaml
environment:
  - DATABASE_URL=postgres://your_db_user:your_db_password@host.docker.internal:5432/your_db_name
```

#### 2.Install dependencies:

Ensure that requirements.txt includes all necessary dependencies:

```plaintext
Django>=3.0,<4.0
celery~=5.4.0
redis
psycopg2-binary
django-celery-results
django-celery-beat
dj-database-url
flower
python-logging-loki==0.3.1
python-dotenv==1.0.1
tzlocal~=5.2
pytz~=2024.1
```

## Configuration
#### 1.Update settings.py:

Ensure that your settings.py includes configurations for Celery

## Running the Application
#### 1.Build and start the Docker containers:

```sh
docker-compose up --build
```

#### 2.Create a superuser for Django admin:

```sh
docker-compose run web python manage.py createsuperuser
```

## API Endpoints
### Create Interval Task
- URL: /celerytasks/create_rrule_task/
- Method: POST
- Payload:
- 
```json
{
    "start_date": "2024-06-13T03:52:00",
    "end_date": "2024-06-13T08:55:00",
    "run_count": 5,
    "system_id": "fb6f18ef-6741-4daf-9dbb-09e2e4f851a3",
    "group_id": "c29c778b-f809-4ac1-aaa1-788c12a62a3f",
    "job_id": "c29c778b-f809-4ac1-aaa1-788c12a62a3f",
    "job_name": "Job 2",
    "repeat_interval": "FREQ=MINUTELY",
    "max_run_duration": "000 01:00:00",
    "max_run": 5,
    "max_failure": 0,
    "priority": 1,
    "is_enable": true,
    "auto_drop": true,
    "restart_on_failure": true,
    "restartable": true,
    "job_comments": "Test",
    "job_type": "EXECUTABLE",
    "job_action": "test",
    "frst_reg_user_id": "731a70d1-ca94-48f1-a63b-d55af1c1ec52",
    "worker_id" : 1
}
```

### Manually run task
- URL: /celerytasks/manually_run/
- Method: POST
- Payload:
- 
```json
{
    "job_id": "12345",
    "job_type": "rest_api",
    "job_action": "run",
    "queue_name": "worker_2_queue"
}
```

### Force stop
- URL: /celerytasks/force_stop/
- Method: POST
- Payload:
- 
```json
{
    "job_id": "12345"
}
```

## Usage
#### 1.Create an interval task:

```sh
curl --location 'http://localhost:8000/celerytasks/create_rrule_task/' \
--header 'Content-Type: application/json' \
--data '{
    "start_date": "2024-06-13T03:52:00",
    "end_date": "2024-06-13T08:55:00",
    "run_count": 5,
    "system_id": "fb6f18ef-6741-4daf-9dbb-09e2e4f851a3",
    "group_id": "c29c778b-f809-4ac1-aaa1-788c12a62a3f",
    "job_id": "c29c778b-f809-4ac1-aaa1-788c12a62a3f",
    "job_name": "Job 2",
    "repeat_interval": "FREQ=MINUTELY",
    "max_run_duration": "000 01:00:00",
    "max_run": 5,
    "max_failure": 0,
    "priority": 1,
    "is_enable": true,
    "auto_drop": true,
    "restart_on_failure": true,
    "restartable": true,
    "job_comments": "Test",
    "job_type": "EXECUTABLE",
    "job_action": "test",
    "frst_reg_user_id": "731a70d1-ca94-48f1-a63b-d55af1c1ec52",
    "worker_id" : 1
}'
```

#### 2.Manually run task

```sh
curl --location 'http://localhost:8000/celerytasks/manually_run/' \
--header 'Content-Type: application/json' \
--data '{
    "job_id": "12345",
    "job_type": "rest_api",
    "job_action": "run",
    "queue_name": "worker_2_queue"
}'
```

#### 3.Force stop

```sh
curl --location 'http://localhost:8000/celerytasks/force_stop/' \
--header 'Content-Type: application/json' \
--data '{
    "job_id": "12345"
}'
```

## Monitoring
#### Access the Flower monitoring dashboard:

Open your browser and navigate to http://localhost:5555.

#### Grafana dashboard
Open your browser and navigate to http://localhost:3000.

## References

- [Django Documentation](https://docs.djangoproject.com/en/stable/)
- [Celery Documentation](https://docs.celeryproject.org/en/stable/)
- [Redis Documentation](https://redis.io/documentation)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [Docker Documentation](https://docs.docker.com/)
- [Docker Compose Documentation](https://docs.docker.com/compose/)
- [Django-Celery-Beat Documentation](https://django-celery-beat.readthedocs.io/en/latest/)
- [Django-Celery-Results Documentation](https://django-celery-results.readthedocs.io/en/latest/)
- [dj-database-url Documentation](https://pypi.org/project/dj-database-url/)
- [Flower Documentation](https://flower.readthedocs.io/en/latest/)
