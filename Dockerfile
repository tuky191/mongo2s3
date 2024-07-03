# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Install Poetry and Supervisord
RUN apt-get update && apt-get install -y supervisor && pip install poetry

# Set the working directory
WORKDIR /app

# Copy the pyproject.toml and poetry.lock files
COPY pyproject.toml poetry.lock ./

# Install the dependencies
RUN poetry install --no-root

# Copy the rest of the application code
COPY scripts/ ./scripts
COPY SFSRootCAG2.pem /app/SFSRootCAG2.pem
COPY supervisord.conf /app/supervisord.conf

# Run Supervisord
CMD ["supervisord", "-c", "/app/supervisord.conf"]
