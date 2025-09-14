FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    # 提供給 Selenium 程式碼讀取
    CHROME_BIN=/usr/bin/chromium \
    CHROMEDRIVER=/usr/bin/chromedriver \
    # Cloud Run 上跑無沙箱模式較穩
    PLAYWRIGHT_BROWSERS_PATH=/ms-playwright

# ---- 系統相依與瀏覽器（給 Selenium）----
# 用 Debian 套件同時安裝 chromium 與 chromedriver（版本自然匹配）
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates wget gnupg \
    chromium chromium-driver \
    fonts-liberation libasound2 libatk1.0-0 libatk-bridge2.0-0 libcups2 \
    libdbus-1-3 libdrm2 libxkbcommon0 libxcomposite1 libxdamage1 libxfixes3 \
    libnss3 libxrandr2 libgbm1 libgtk-3-0 libx11-xcb1 libpango-1.0-0 \
    libxshmfence1 libx11-6 libxext6 libxrender1 libxcb1 \
    && rm -rf /var/lib/apt/lists/*

# ---- Python 依賴 ----
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ---- 安裝 Playwright 的瀏覽器與依賴（給 fallback）----
# 注意：這會再安裝一份 Playwright 專用 Chromium（最穩定做法）
RUN python -m playwright install --with-deps chromium

# ---- App 檔案與啟動 ----
COPY . /app

# 你若是 Flask 直接跑：
# CMD ["python", "app.py"]
# 若是 Uvicorn/FastAPI：
# CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8080"]

# Cloud Run 預設用 8080；若你用 gunicorn 可改為：
# CMD ["gunicorn", "-w", "2", "-k", "gthread", "-b", "0.0.0.0:8080", "app:app"]