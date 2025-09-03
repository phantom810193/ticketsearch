# Python base（Debian bookworm-slim）
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DEBIAN_FRONTEND=noninteractive \
    PLAYWRIGHT_BROWSERS_PATH=/ms-playwright

# 系統相依（含 Playwright/Chromium 依賴）
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl gnupg \
    build-essential gcc \
    libnss3 libatk1.0-0 libatk-bridge2.0-0 libxkbcommon0 libdrm2 \
    libxcomposite1 libxdamage1 libxfixes3 libxrandr2 libgbm1 libasound2 \
    libatspi2.0-0 libxshmfence1 libx11-6 libx11-xcb1 libxcb1 libxext6 \
    libxss1 libxi6 libxrender1 libpangocairo-1.0-0 libpango-1.0-0 \
    libcairo2 libcups2 libgtk-3-0 fonts-liberation fonts-noto-color-emoji \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# 先安裝需求（可被快取）
COPY requirements.txt .
RUN pip install --upgrade pip setuptools wheel \
 && pip install --no-cache-dir -r requirements.txt

# 安裝 Playwright 瀏覽器與相依（requirements 會裝 playwright 套件）
RUN python -m playwright install --with-deps chromium

# 複製程式碼
COPY . .

EXPOSE 8080
# Cloud Run 入口（Flask app: "app" 物件在 app.py）
CMD ["gunicorn", "-b", "0.0.0.0:8080", "app:app"]