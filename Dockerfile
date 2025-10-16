# 1) Python 基底
FROM python:3.11-slim

# 2) 系統相依 + 憑證 + Chromium/Driver + 字型
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl \
    chromium chromium-driver \
    libglib2.0-0 libnss3 libx11-6 libxcb1 libxcomposite1 libxdamage1 \
    libxext6 libxfixes3 libxrandr2 libgtk-3-0 libdrm2 libgbm1 \
    libasound2 libxshmfence1 libxtst6 fonts-noto-cjk \
    && rm -rf /var/lib/apt/lists/*
ENV SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt

# 3) 套件
WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# 4) 程式碼
COPY . .

# 5) 讓 Selenium 找到瀏覽器與 driver
ENV CHROME_BIN=/usr/bin/chromium
ENV CHROMEDRIVER=/usr/lib/chromium/chromedriver
ENV PORT=8080
ENV PYTHONUNBUFFERED=1

# 6) 啟動
CMD ["gunicorn", "-w", "1", "-k", "sync", "-b", ":8080", "app:app"]
