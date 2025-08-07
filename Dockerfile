# Dockerfile
FROM python:3.11-slim
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

RUN mkdir -p /data
ENV PERSIST_DIR=/data                 

COPY . .

CMD ["python", "exim-import-data.py"] # or whatever the filename is
