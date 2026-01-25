FROM python:3.13.8-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Install Python packages
RUN pip install --upgrade pip && \
    pip install pandas pyarrow sqlalchemy psycopg2-binary click tqdm

# Set up the working directory inside the container
WORKDIR /app

# Copy the script to the container
COPY ingest_data.py .

# Define what to do first when the container runs
ENTRYPOINT ["python", "ingest_data.py"]