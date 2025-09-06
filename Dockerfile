# 使用內建瀏覽器與依賴的官方 Playwright 基底，最穩定
FROM mcr.microsoft.com/playwright/python:v1.45.0-jammy

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app
COPY liff ./liff

# 安裝套件（requirements.txt 內若有 'playwright==1.45.0' 也 OK，版本相容）
COPY requirements.txt .
RUN pip install --upgrade pip setuptools wheel \
 && pip install --no-cache-dir -r requirements.txt

# 複製程式
COPY . .

EXPOSE 8080
CMD ["gunicorn", "-b", "0.0.0.0:8080", "app:app"]