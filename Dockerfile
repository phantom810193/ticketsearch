# 1) 輕量 Python 基底
FROM python:3.11-slim

# 2) 系統相依套件（Chromium 需要的 lib、中文字型）
RUN apt-get update && apt-get install -y --no-install-recommends \
    libglib2.0-0 libnss3 libx11-6 libxcb1 libxcomposite1 libxdamage1 \
    libxext6 libxfixes3 libxrandr2 libgtk-3-0 libdrm2 libgbm1 \
    libasound2 libxshmfence1 libxtst6 fonts-noto-cjk \
    && rm -rf /var/lib/apt/lists/*

# 3) 安裝相依套件
WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt \
    && pip install --no-cache-dir playwright==1.47.2

# 4) 安裝瀏覽器到映像檔（不要在執行期下載）
RUN python -m playwright install --with-deps chromium

# 5) 複製程式碼
COPY . .

# 6) Cloud Run 預設 PORT 環境變數
ENV PORT=8080
ENV PYTHONUNBUFFERED=1

# 7) 啟動（可用 gunicorn，也可直接 python app.py）
CMD ["python", "app.py"]