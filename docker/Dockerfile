FROM python:3.9-buster

RUN apt-get update --fix-missing && apt-get install -y gcc && apt-get clean


WORKDIR /app

COPY docker/requirements.txt /app/requirements.txt

RUN pip install --no-cache-dir -r requirements.txt

COPY src/ /app/src/
COPY data/database/ /app/database/

CMD ["python", "/app/ingest/data_ingest.py"]

