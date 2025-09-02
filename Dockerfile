# Python base（體積小）
FROM python:3.11-slim

# 安裝 Chromium 及 Playwright 需要的系統套件
RUN apt-get update && apt-get install -y --no-install-recommends \
    libnss3 libatk1.0-0 libatk-bridge2.0-0 libxkbcommon0 libdrm2 libxcomposite1 \
    libxdamage1 libxfixes3 libxrandr2 libgbm1 libasound2 fonts-noto-color-emoji \
    curl ca-certificates \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 如果 requirements 有 playwright，就安裝 chromium
# （沒裝 playwright 的話，這步會自動略過）
RUN python -c "import importlib; import sys; \
    sys.exit(0) if importlib.util.find_spec('playwright') else sys.exit(0)" && \
    python -m playwright install chromium || true

COPY . .
# Cloud Run 入口
CMD ["gunicorn", "-b", "0.0.0.0:8080", "app:app"]