# Use an official Python runtime as a parent image
FROM python:3.12

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# Set work directory
WORKDIR /code

# Install dependencies
COPY requirements.txt /code/
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

# Copy project
COPY . /code/

# Copy and set permissions for the entrypoint script
COPY entrypoint.sh /code/entrypoint.sh
RUN chmod +x /code/entrypoint.sh

# Set the entrypoint script
ENTRYPOINT ["/code/entrypoint.sh"]
