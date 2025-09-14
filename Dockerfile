FROM python:3.11-slim

# 安裝 Chromium、Chromedriver、中文字型
RUN apt-get update && apt-get install -y --no-install-recommends \
    chromium chromium-driver fonts-noto-cjk tzdata ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# 讓你的程式找到瀏覽器與驅動
ENV CHROME_BIN=/usr/bin/chromium \
    CHROMEDRIVER=/usr/bin/chromedriver \
    PYTHONUNBUFFERED=1

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
CMD ["python", "app.py"]