FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

ENV REGISTRY_DATA_DIR=/data \
    REGISTRY_UPLOAD_LEASE_SECONDS=3600 \
    REGISTRY_GC_GRACE_SECONDS=3600

VOLUME /data
EXPOSE 8000

# 单进程运行：SQLite 单写者 + 进程内锁保证一致性
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
