# 以 Playwright 官方基底，版本與你的 requirements 相容（1.47.x）
FROM mcr.microsoft.com/playwright/python:v1.47.0-jammy

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    # 給程式端讀取（Selenium 用）
    CHROME_BIN=/opt/chrome/chrome \
    CHROMEDRIVER=/usr/bin/chromedriver \
    # Playwright 已內建瀏覽器於 /ms-playwright
    PLAYWRIGHT_BROWSERS_PATH=/ms-playwright

# 安裝 Chrome for Testing（穩定版）與對應 chromedriver（供 Selenium 使用）
RUN apt-get update && apt-get install -y --no-install-recommends curl unzip ca-certificates \
 && CFT_VER=$(curl -fsSL https://googlechromelabs.github.io/chrome-for-testing/LATEST_RELEASE_STABLE) \
 && mkdir -p /opt/chrome /tmp/cft \
 && curl -fsSL -o /tmp/cft/chrome.zip "https://storage.googleapis.com/chrome-for-testing-public/${CFT_VER}/linux64/chrome-linux64.zip" \
 && curl -fsSL -o /tmp/cft/chromedriver.zip "https://storage.googleapis.com/chrome-for-testing-public/${CFT_VER}/linux64/chromedriver-linux64.zip" \
 && unzip /tmp/cft/chrome.zip -d /tmp/cft \
 && cp -r /tmp/cft/chrome-linux64/* /opt/chrome/ \
 && ln -sf /opt/chrome/chrome /usr/bin/google-chrome \
 && unzip /tmp/cft/chromedriver.zip -d /tmp/cft \
 && mv /tmp/cft/chromedriver-linux64/chromedriver /usr/bin/chromedriver \
 && chmod +x /usr/bin/chromedriver \
 && rm -rf /var/lib/apt/lists/* /tmp/cft

# Python 相依
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 程式碼
COPY . /app

# 以 gunicorn 啟動 Flask
CMD ["gunicorn", "-w", "2", "-k", "gthread", "-b", "0.0.0.0:8080", "app:app"]