FROM python:3.11-slim

WORKDIR /app

COPY ingest/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY ingest /app/ingest

ENTRYPOINT ["python", "/app/ingest/news_ingest.py"]

