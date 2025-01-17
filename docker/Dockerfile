FROM python:3.9-slim

RUN apt-get update && apt-get install -y \
    gcc \
    && apt-get clean

WORKDIR /app

COPY docker/requirements.txt /app/requirements.txt

RUN pip install --no-cache-dir -r requirements.txt

COPY src/ingest/ /app/ingest/
COPY data/database/ /app/database/

CMD ["python", "/app/ingest/data_ingest.py"]
