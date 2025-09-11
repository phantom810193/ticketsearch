# 1) 輕量 Python 基底
FROM python:3.11-slim

ARG DEBIAN_FRONTEND=noninteractive

# 2) 安裝 Chromium + Chromedriver（給 Selenium），以及必要 lib / 字型
RUN apt-get update && apt-get install -y --no-install-recommends \
    chromium chromium-driver \
    libglib2.0-0 libnss3 libx11-6 libxcb1 libxcomposite1 libxdamage1 \
    libxext6 libxfixes3 libxrandr2 libgtk-3-0 libdrm2 libgbm1 \
    libasound2 libxshmfence1 libxtst6 fonts-noto-cjk ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# 3) 安裝相依套件
WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# 4) 安裝 Playwright 的 Chromium（做為後援）
RUN python -m playwright install --with-deps chromium

# 5) 複製程式碼
COPY . .

# 6) 環境變數
ENV PORT=8080 \
    PYTHONUNBUFFERED=1 \
    CHROMEDRIVER_PATH=/usr/bin/chromedriver \
    USE_SELENIUM=1

# 7) 以 gunicorn 啟動（Cloud Run 推薦）
CMD ["gunicorn", "-w", "2", "-k", "gthread", "-b", "0.0.0.0:8080", "--timeout", "120", "app:app"]