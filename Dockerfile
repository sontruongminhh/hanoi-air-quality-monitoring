FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Install system dependencies (bao gồm AWS CLI)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    curl \
    unzip \
    && curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" \
    && unzip awscliv2.zip \
    && ./aws/install \
    && rm -rf awscliv2.zip aws \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application files
COPY producer.py .
COPY consumer.py .
COPY s3_consumer.py .
COPY util_email.py .
COPY .env .

# Set environment variables
ENV PYTHONUNBUFFERED=1

# Default command (can be overridden)
CMD ["python", "producer.py"]