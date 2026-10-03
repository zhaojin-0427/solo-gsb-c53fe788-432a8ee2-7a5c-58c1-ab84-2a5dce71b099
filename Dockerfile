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

# 单实例 uvicorn：多个进程可安全挂载同一 /data（SQLite WAL + busy_timeout
# 负责元数据写串行，.registry.lock 的 flock 负责发布/GC/拉取跨进程协调）；
# 生产多 worker/多节点请改用外部数据库与共享存储
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
