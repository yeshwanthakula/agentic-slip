# Use CUDA base image if you need GPU support
FROM python:3.9-slim

WORKDIR /app

# Combine all system updates and installations in one RUN
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    git \
    && rm -rf /var/lib/apt/lists/*

# Pre-download and cache model weights during build
COPY download_model.py .
RUN pip install --no-cache-dir torch transformers
RUN python download_model.py

# Copy and install requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy only necessary files
COPY main.py .

EXPOSE 8000

# Changed from app:app to main:app to match the file name
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]