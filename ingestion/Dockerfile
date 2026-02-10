FROM python:3.11-slim

WORKDIR /app

RUN pip install requests pandas pyarrow

COPY ingest_velib.py .

CMD ["python", "ingest_velib.py"]