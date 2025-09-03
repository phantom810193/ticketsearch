# 基底映像：體積小、支援 Python 3.11
FROM python:3.11-slim

# 一些通用最佳化
ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    POETRY_VIRTUALENVS_CREATE=false

# 必要系統套件（若你沒有用 Playwright/Chromium，這些也可保留）
RUN apt-get update && apt-get install -y --no-install-recommends \
    libnss3 libatk1.0-0 libatk-bridge2.0-0 libxkbcommon0 libdrm2 libxcomposite1 \
    libxdamage1 libxfixes3 libxrandr2 libgbm1 libasound2 fonts-noto-color-emoji \
    curl ca-certificates \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# 先裝套件（可利用快取）
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 如果 requirements 有 playwright，你才會想裝 chromium
# 無論如何失敗都不讓建置中止（|| true）
RUN python - <<'PY'\nimport importlib, os, sys\nsys.exit(1) if importlib.util.find_spec('playwright') else sys.exit(0)\nPY \\\n && python -m playwright install chromium || true

# 複製專案
COPY . .

# 預設埠（Cloud Run 會覆蓋為 $PORT）
ENV PORT=8080 \
    GUNICORN_TIMEOUT=120 \
    GUNICORN_WORKERS=1 \
    GUNICORN_THREADS=8

# 使用 wsgi:application 作為入口；用 sh -c 才能展開 $PORT
CMD ["sh", "-c", "exec gunicorn -b :${PORT:-8080} --timeout ${GUNICORN_TIMEOUT} --workers ${GUNICORN_WORKERS} --threads ${GUNICORN_THREADS} --access-logfile - --error-logfile - --log-level info wsgi:application"]