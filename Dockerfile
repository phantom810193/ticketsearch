FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    DEBIAN_FRONTEND=noninteractive

WORKDIR /app

# 基本工具（build-essential 讓某些套件能編譯；curl/ca-certificates 方便除錯）
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential curl ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# 先裝套件，再複製原始碼（可利用 docker layer cache）
COPY requirements.txt .
RUN python -m pip install --no-cache-dir --upgrade pip \
 && pip install --no-cache-dir -r requirements.txt

COPY . .

# Cloud Run 會注入 PORT=8080；預設值保險也設 8080
CMD ["sh","-c","gunicorn -k uvicorn.workers.UvicornWorker -w 2 -b 0.0.0.0:${PORT:-8080} app:app"]
