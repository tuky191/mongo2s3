# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Install Poetry
RUN pip install poetry

# Set the working directory
WORKDIR /app

# Copy the pyproject.toml and poetry.lock files
COPY pyproject.toml poetry.lock ./

# Install the dependencies
RUN poetry install --no-root

# Copy the rest of the application code
COPY scripts/ ./scripts

# Run the application
CMD ["poetry", "run", "python", "./scripts/export_to_s3.py"]
