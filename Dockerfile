# 1) 輕量 Python 基底
FROM python:3.11-slim

# 2) 系統相依套件（Chromium 需要的 lib、中文字型、以及 CA 憑證！）
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl \
    libglib2.0-0 libnss3 libx11-6 libxcb1 libxcomposite1 libxdamage1 \
    libxext6 libxfixes3 libxrandr2 libgtk-3-0 libdrm2 libgbm1 \
    libasound2 libxshmfence1 libxtst6 fonts-noto-cjk \
    && rm -rf /var/lib/apt/lists/*

# 讓某些環境找得到 CA 憑證（通常不必，但設了較保險）
ENV SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt

WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt \
    && pip install --no-cache-dir playwright==1.47.0

RUN python -m playwright install --with-deps chromium

COPY . .
ENV PORT=8080
ENV PYTHONUNBUFFERED=1
CMD ["python", "app.py"]