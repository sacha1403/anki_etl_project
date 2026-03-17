FROM apache/airflow:3.1.7 

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    g++ \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*
    
USER airflow

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt 