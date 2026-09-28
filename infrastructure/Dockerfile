FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY common ./common
COPY ingestion ./ingestion
COPY processing ./processing
COPY storage ./storage
COPY dashboard ./dashboard

RUN useradd --create-home app
USER app
