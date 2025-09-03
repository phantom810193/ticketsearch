# Python base（體積小）
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PLAYWRIGHT_BROWSERS_PATH=/ms-playwright

# 系統相依：grpcio 等常見套件需要的編譯工具；Chromium 依賴也一併裝好
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential gcc curl ca-certificates \
    libnss3 libatk1.0-0 libatk-bridge2.0-0 libxkbcommon0 libdrm2 libxcomposite1 \
    libxdamage1 libxfixes3 libxrandr2 libgbm1 libasound2 fonts-noto-color-emoji \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# 先只送 requirements.txt，讓 pip 安裝可被快取
COPY requirements.txt .
RUN pip install --upgrade pip setuptools wheel \
 && pip install --no-cache-dir -r requirements.txt

# 這一步很關鍵：安裝 Chromium + 相依套件（playwright 已在 requirements 裡）
# 若你要切回「不裝瀏覽器」的版本，可改用 ARG 控制
RUN python -m playwright install --with-deps chromium

# 再送其餘程式碼
COPY . .

# Cloud Run 入口
CMD ["gunicorn", "-b", "0.0.0.0:8080", "app:app"]