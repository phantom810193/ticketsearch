# worker.py — 住宅網路抓票 Worker（搭配雲端 Cloud Run Webhook/Firestore）
# 需求套件：google-cloud-firestore, requests, beautifulsoup4, cloudscraper(可選), python-dotenv

import os, time, random, hashlib, re, unicodedata, logging, sys
try:
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
except Exception:
    pass

from datetime import datetime
from typing import Tuple, Optional, List, Dict

from dotenv import load_dotenv
from urllib.parse import urlparse

# ---- HTTP 抓取 ----
import requests
try:
    import cloudscraper  # 可選：繞過部分 Cloudflare
except ImportError:
    cloudscraper = None

from bs4 import BeautifulSoup

# ---- Firestore ----
from google.cloud import firestore

# ========= 基本設定 =========
load_dotenv(override=False)

LINE_CHANNEL_ACCESS_TOKEN = (os.getenv("LINE_CHANNEL_ACCESS_TOKEN") or "").strip()
if not LINE_CHANNEL_ACCESS_TOKEN:
    print("❌ 請在 .env 或環境變數設定 LINE_CHANNEL_ACCESS_TOKEN")
    sys.exit(1)

# 本機請先執行：gcloud auth application-default login
# 讓 Firestore Client 能用 ADC 認證
db = firestore.Client()  # 自動取用預設專案；若要指定專案可傳 project="your-project-id"
TASKS = db.collection("tasks")

DEFAULT_INTERVAL = int(os.getenv("DEFAULT_INTERVAL", "15"))  # 秒
SLEEP_BETWEEN_TASKS = (0.5, 1.2)  # 每個任務之間的抖動休息
LOOP_IDLE_SLEEP = (5, 10)         # 若沒任務可做，暫停幾秒後再輪詢
REQUEST_TIMEOUT = 12

# ========= Logger =========
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
    force=True,
)
logger = logging.getLogger("tixworker")

# ========= 關鍵字規則 =========
SOLDOUT_KEYWORDS = [
    "售完", "完售", "已售完", "已售罄", "已無票",
    "sold out", "soldout", "no tickets", "unavailable"
]
TICKET_KEYWORDS = [
    "立即購票", "購票", "加入購物車", "選擇座位", "剩餘", "可售", "尚有", "開賣",
    "tickets", "buy now", "add to cart", "select seats", "available"
]

# ========= 小工具 =========
def _now_ts() -> int:
    return int(time.time())

def normalize_text(s: str) -> str:
    s = unicodedata.normalize("NFKC", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

UA_POOL = [
    # 常見瀏覽器 UA（隨機選一個）
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:128.0) Gecko/20100101 Firefox/128.0",
]

def build_session():
    # cloudscraper 若存在，優先用；否則 requests.Session
    s = cloudscraper.create_scraper(
        browser={"browser": "chrome", "platform": "windows"},
        delay=random.uniform(1.0, 3.0),
    ) if cloudscraper else requests.Session()
    s.headers.update({
        "User-Agent": random.choice(UA_POOL),
        "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    })
    return s

def fetch_html(url: str, timeout=REQUEST_TIMEOUT) -> str:
    s = build_session()
    # 盡量帶上合理 Referer（同網域）
    try:
        host = urlparse(url).scheme + "://" + urlparse(url).netloc
        s.headers["Referer"] = host
    except Exception:
        pass
    r = s.get(url, timeout=timeout)
    r.raise_for_status()
    return r.text

def extract_snapshot_and_ticket(html: str) -> Tuple[str, bool]:
    """
    取出頁面純文字快照 + 是否判定「有票」（關鍵字邏輯）
    """
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()

    text = normalize_text(soup.get_text(" ", strip=True))
    t_low = text.lower()

    has_ticket_kw = any(kw.lower() in t_low for kw in TICKET_KEYWORDS)
    has_soldout_kw = any(kw.lower() in t_low for kw in SOLDOUT_KEYWORDS)
    has_ticket = has_ticket_kw and not has_soldout_kw

    # 蒐集可能重要的按鈕文字
    btns = []
    for btn in soup.find_all(["a", "button"]):
        t = btn.get_text(" ", strip=True)
        if t:
            btns.append(normalize_text(t))
    snapshot = text + "\n\nBTN:" + "|".join(btns[:50])

    return snapshot, has_ticket

def has_ticket_in_snapshot(snapshot: str) -> bool:
    t_low = normalize_text(snapshot).lower()
    return (any(kw.lower() in t_low for kw in TICKET_KEYWORDS)
            and not any(kw.lower() in t_low for kw in SOLDOUT_KEYWORDS))

def sha(s: str) -> str:
    return hashlib.sha256((s or "").encode("utf-8")).hexdigest()

# ========= Firestore 資料層 =========
def list_all_active_tasks() -> List[Dict]:
    docs = TASKS.where("is_active", "==", True).stream()
    return [d.to_dict() for d in docs]

def update_after_check(tid: str, snapshot: str):
    TASKS.document(tid).update({
        "last_snapshot": snapshot,
        "last_checked": _now_ts(),
    })

# ========= LINE Push =========
def line_push_text(to_user_id: str, text: str) -> bool:
    url = "https://api.line.me/v2/bot/message/push"
    headers = {
        "Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}",
        "Content-Type": "application/json; charset=utf-8",
    }
    data = {
        "to": to_user_id,
        "messages": [{"type": "text", "text": text[:1000]}],  # LINE 單則上限 1000 字左右
    }
    try:
        resp = requests.post(url, json=data, headers=headers, timeout=10)
        if resp.status_code >= 300:
            logger.error(f"[push] {resp.status_code} {resp.text}")
            return False
        return True
    except Exception as e:
        logger.exception(f"[push] error: {e}")
        return False

# ========= 主流程 =========
def do_one_pass() -> int:
    """
    進行一輪掃描；回傳本輪「實際檢查的任務數」
    """
    tasks = list_all_active_tasks()
    if not tasks:
        return 0

    random.shuffle(tasks)
    checked = 0

    for t in tasks:
        try:
            tid = t.get("tid")
            url = t.get("url")
            user_id = t.get("user_id")
            interval_sec = int(t.get("interval_sec", DEFAULT_INTERVAL))
            last_checked = int(t.get("last_checked", 0))
            last_snapshot = t.get("last_snapshot") or ""

            # 間隔控制
            if _now_ts() - last_checked < max(5, min(300, interval_sec)):
                continue

            # 抓頁
            try:
                html = fetch_html(url)
            except requests.HTTPError as he:
                status = he.response.status_code if he.response is not None else "?"
                logger.warning(f"[task {tid}] HTTP {status} for {url}")
                # 照樣更新 last_checked，避免一直猛打被擋的頁
                update_after_check(tid, last_snapshot)
                checked += 1
                time.sleep(random.uniform(*SLEEP_BETWEEN_TASKS))
                continue
            except Exception as e:
                logger.warning(f"[task {tid}] fetch error: {e}")
                update_after_check(tid, last_snapshot)
                checked += 1
                time.sleep(random.uniform(*SLEEP_BETWEEN_TASKS))
                continue

            snapshot, has_ticket_now = extract_snapshot_and_ticket(html)
            prev_has = has_ticket_in_snapshot(last_snapshot)

            # 更新 Firestore（先更新時間與快照）
            update_after_check(tid, snapshot)
            checked += 1

            # 通知條件：現在判定有票，且上一版沒有
            if has_ticket_now and not prev_has:
                msg = (
                    "🎉 疑似有票釋出！\n"
                    f"任務 #{tid}\n"
                    f"{url}\n"
                    "（建議立刻點進去檢查與購買）"
                )
                ok = line_push_text(user_id, msg)
                logger.info(f"[task {tid}] push {'OK' if ok else 'FAIL'}")

            time.sleep(random.uniform(*SLEEP_BETWEEN_TASKS))

        except Exception as e:
            logger.exception(f"[task {t.get('tid')}] unhandled: {e}")
            # 不退出，繼續跑下一個

    return checked

def main():
    logger.info("worker 啟動（住宅網路模式）")
    while True:
        try:
            n = do_one_pass()
            if n == 0:
                # 沒任務可做或都未到時間
                time.sleep(random.uniform(*LOOP_IDLE_SLEEP))
            # 否則立刻再跑下一輪（每個任務內已有節流）
        except KeyboardInterrupt:
            logger.info("收到中斷，結束。")
            break
        except Exception as e:
            logger.exception(f"[loop] error: {e}")
            time.sleep(3)

if __name__ == "__main__":
    main()