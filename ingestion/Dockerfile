FROM python:3.11-slim

WORKDIR /app

COPY requirement.txt .

RUN pip install -r requirement.txt

COPY ingest_velib.py .

CMD ["python", "ingest_velib.py"]