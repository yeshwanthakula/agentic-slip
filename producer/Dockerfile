FROM python:3.9-slim

WORKDIR /app
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    git \
    && rm -rf /var/lib/apt/lists/*

COPY requirement.txt .
RUN pip install  --no-cache-dir  -r requirement.txt
COPY . /app

CMD ["python", "producer.py"]