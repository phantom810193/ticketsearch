# Dockerfile
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    CHROME_BIN=/usr/bin/google-chrome \
    CHROMEDRIVER=/usr/bin/chromedriver \
    PLAYWRIGHT_BROWSERS_PATH=/ms-playwright

# 1) 基礎系統依賴 + 加入 Google Chrome 軟體庫並安裝
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl gnupg unzip \
    fonts-liberation libasound2 libatk1.0-0 libatk-bridge2.0-0 libcups2 \
    libdbus-1-3 libdrm2 libxkbcommon0 libxcomposite1 libxdamage1 libxfixes3 \
    libnss3 libxrandr2 libgbm1 libgtk-3-0 libx11-xcb1 libpango-1.0-0 \
    libxshmfence1 libx11-6 libxext6 libxrender1 libxcb1 \
 && install -d -m 0755 /etc/apt/keyrings \
 && curl -fsSL https://dl-ssl.google.com/linux/linux_signing_key.pub \
    | gpg --dearmor -o /etc/apt/keyrings/google-chrome.gpg \
 && echo "deb [arch=amd64 signed-by=/etc/apt/keyrings/google-chrome.gpg] http://dl.google.com/linux/chrome/deb/ stable main" \
    > /etc/apt/sources.list.d/google-chrome.list \
 && apt-get update && apt-get install -y --no-install-recommends google-chrome-stable \
 && rm -rf /var/lib/apt/lists/*

# 2) 安裝與 Chrome 版本匹配的 chromedriver（給 Selenium）
RUN CHROME_VERSION=$(google-chrome --version | sed -E 's/.* ([0-9]+(\.[0-9]+){3}).*/\1/') \
 && echo "Chrome: ${CHROME_VERSION}" \
 && curl -fsSL -o /tmp/chromedriver.zip \
      "https://storage.googleapis.com/chrome-for-testing-public/${CHROME_VERSION}/linux64/chromedriver-linux64.zip" \
 && unzip /tmp/chromedriver.zip -d /tmp/ \
 && mv /tmp/chromedriver-linux64/chromedriver /usr/bin/chromedriver \
 && chmod +x /usr/bin/chromedriver \
 && rm -rf /tmp/chromedriver.zip /tmp/chromedriver-linux64

# 3) Python 套件
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 4) 安裝 Playwright 的 Chromium（做為 Selenium 失敗時的備援）
RUN python -m playwright install --with-deps chromium

# 5) 複製程式碼並啟動
COPY . /app
CMD ["gunicorn", "-w", "2", "-k", "gthread", "-b", "0.0.0.0:8080", "app:app"]