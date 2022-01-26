FROM python:3.9

RUN pip install pandas sqlalchemy psycopg2 requests

WORKDIR /app
COPY ingest-data.py ingest-data.py
COPY taxi.csv taxi.csv
COPY zones.csv zones.csv

ENTRYPOINT [ "python","ingest-data.py" ]
