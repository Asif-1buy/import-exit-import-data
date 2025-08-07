# Dockerfile
FROM python:3.11-slim
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Create the default persistence directory so the path always exists
RUN mkdir -p /data
ENV PERSIST_DIR=/data                 # same default as in the script

COPY . .

CMD ["python", "exim-import-data.py"] # or whatever the filename is
