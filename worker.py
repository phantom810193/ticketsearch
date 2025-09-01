# worker.py — 住宅網路模式輪詢 Worker（使用 Firestore FieldFilter）
import os, sys, time, random, hashlib, re, unicodedata, logging
from typing import Tuple, Optional, List, Dict
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

# ---- HTTP 抓取 ----
import requests
try:
    import cloudscraper  # 可選：減少被 CF 擋的機率
except ImportError:
    cloudscraper = None

from bs4 import BeautifulSoup

# ---- LINE v3（僅需 Access Token 可推播）----
from linebot.v3.messaging import (
    Configuration, ApiClient, MessagingApi,
    PushMessageRequest, TextMessage as V3TextMessage, ApiException
)

# ---- Firestore（新版 where 寫法）----
from google.cloud import firestore
from google.cloud.firestore_v1 import FieldFilter, Query

# ========= 基本設定 =========
ENV_PATH = Path(__file__).with_name(".env")
load_dotenv(override=False)  # 本機可放 .env；Cloud 上不覆蓋

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
    force=True,
)
logger = logging.getLogger("tixworker")
logger.setLevel(logging.INFO)
logger.handlers.clear()
logger.addHandler(logging.StreamHandler(sys.stdout))
logger.propagate = False

# 票頁抓取 UA
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126 Safari/537.36"

# 讀取環境變數
LINE_CHANNEL_ACCESS_TOKEN = (os.getenv("LINE_CHANNEL_ACCESS_TOKEN") or "").strip()
DEFAULT_INTERVAL = int(os.getenv("DEFAULT_INTERVAL", "15"))  # 秒

if not LINE_CHANNEL_ACCESS_TOKEN:
    logger.warning("環境變數 LINE_CHANNEL_ACCESS_TOKEN 未設定，將無法推播 LINE 訊息。")

# ========= 建立外部服務客戶端 =========
# LINE client（只有 token 就能 push）
line_conf = Configuration(access_token=LINE_CHANNEL_ACCESS_TOKEN) if LINE_CHANNEL_ACCESS_TOKEN else None
api_client = ApiClient(line_conf) if line_conf else None
messaging_api = MessagingApi(api_client) if api_client else None

# Firestore（本機請先設定 ADC）
db = firestore.Client()
TASKS = db.collection("tasks")

# ========= 共用工具 =========
def _now_ts() -> int:
    return int(time.time())

def normalize_text(s: str) -> str:
    s = unicodedata.normalize("NFKC", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def fetch_html(url: str, timeout: int = 15) -> str:
    """
    盡量模擬正常瀏覽器請求。若有 cloudscraper 就用，否則退回 requests。
    """
    headers = {
        "User-Agent": UA,
        "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }
    session = cloudscraper.create_scraper(
        browser={"browser": "chrome", "platform": "windows"}
    ) if cloudscraper else requests.Session()
    r = session.get(url, headers=headers, timeout=timeout)
    r.raise_for_status()
    return r.text

def extract_snapshot_and_ticket(html: str) -> Tuple[str, bool]:
    """
    擷取頁面文字快照，並以關鍵字粗略判定是否「疑似有票」。
    """
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()

    text = normalize_text(soup.get_text(" ", strip=True))

    # 可依站點微調
    soldout_keywords = ["售完", "完售", "已售完", "已售罄", "已無票", "sold out", "soldout"]
    ticket_keywords  = ["立即購票", "購票", "加入購物車", "選擇座位", "剩餘", "可售", "尚有", "開賣", "tickets"]

    t_low = text.lower()
    has_ticket = any(kw.lower() in t_low for kw in ticket_keywords) and not any(
        kw.lower() in t_low for kw in soldout_keywords
    )

    important_bits = []
    for btn in soup.find_all(["a", "button"]):
        t = btn.get_text(" ", strip=True)
        if t:
            important_bits.append(normalize_text(t))
    snapshot = text + "\n\nBTN:" + "|".join(important_bits[:80])
    return snapshot, has_ticket

def push_line(user_id: str, message: str):
    if not messaging_api:
        logger.warning(f"[push] 無 LINE client，略過推播：{message[:60]}...")
        return
    try:
        messaging_api.push_message(
            PushMessageRequest(to=user_id, messages=[V3TextMessage(text=message)])
        )
        logger.info(f"[push] 推播成功 -> {user_id}")
    except ApiException as e:
        logger.error(f"[push] LINE API error: {e}")

# ========= Firestore 資料操作（使用 FieldFilter）=========
def list_due_active_tasks(now_ts: int) -> List[Dict]:
    """
    取出 is_active=True 的任務；是否到期在迴圈中判斷，避免複雜索引。
    """
    docs = TASKS.where(filter=FieldFilter("is_active", "==", True)).stream()
    return [d.to_dict() for d in docs]

def update_after_check(tid: str, snapshot: str):
    TASKS.document(tid).update({
        "last_snapshot": snapshot,
        "last_checked": _now_ts(),
    })

# ========= 單次輪詢邏輯 =========
def run_once():
    now = _now_ts()
    tasks = list_due_active_tasks(now)
    random.shuffle(tasks)

    checked = 0
    for t in tasks:
        try:
            tid = t.get("tid")
            url = t.get("url") or ""
            user_id = t.get("user_id") or ""
            interval_sec = int(t.get("interval_sec", DEFAULT_INTERVAL))
            last_checked = int(t.get("last_checked", 0))

            # 間隔控管
            if (now - last_checked) < max(5, min(300, interval_sec)):
                continue

            logger.info(f"[tick] checking #{tid} {url}")

            # 抓頁
            try:
                html = fetch_html(url)
            except requests.HTTPError as he:
                # 例如 403/404/5xx
                status = getattr(he.response, "status_code", None)
                logger.warning(f"[tick] task#{tid} HTTPError {status} for {url}")
                # 即便失敗也更新 last_checked，避免連續轟炸
                TASKS.document(tid).update({"last_checked": _now_ts()})
                time.sleep(random.uniform(0.2, 0.6))
                continue
            except Exception as e:
                logger.error(f"[tick] task#{tid} fetch error: {e}")
                TASKS.document(tid).update({"last_checked": _now_ts()})
                time.sleep(random.uniform(0.2, 0.6))
                continue

            # 判定
            snapshot, has_ticket = extract_snapshot_and_ticket(html)
            new_hash = hashlib.sha256(snapshot.encode("utf-8")).hexdigest()
            old_hash = hashlib.sha256((t.get("last_snapshot") or "").encode("utf-8")).hexdigest()

            update_after_check(tid, snapshot)

            # 有變化而且疑似有票 -> 推播
            if new_hash != old_hash and has_ticket:
                msg = (
                    "🎉 疑似有票釋出！\n"
                    f"任務#{tid}\n{url}\n"
                    "（建議立刻點進去檢查與購買）"
                )
                push_line(user_id, msg)
            else:
                logger.info(f"[tick] task#{tid} has_ticket={has_ticket} changed={new_hash != old_hash}")

            checked += 1
            time.sleep(random.uniform(0.2, 0.6))

        except Exception as e:
            logger.exception(f"[tick] task#{t.get('tid')} unexpected error: {e}")

    return checked

# ========= 主程式：持續輪詢 =========
def main():
    logger.info("worker 啟動（住宅網路模式）")
    oneshot = os.getenv("ONESHOT", "").lower() in ("1", "true", "yes")

    if oneshot:
        c = run_once()
        logger.info(f"oneshot 完成，checked={c}")
        return

    # 常駐輪詢
    base_sleep = int(os.getenv("WORKER_LOOP_SLEEP", "3"))  # 每輪間隔
    while True:
        start = time.time()
        try:
            checked = run_once()
            dur = time.time() - start
            logger.info(f"[loop] 本輪完成 checked={checked} duration={dur:.2f}s")
        except Exception as e:
            logger.exception(f"[loop] fatal: {e}")
        # 輪與輪之間稍微休息，避免過度打擾網站
        time.sleep(base_sleep + random.uniform(0.0, 1.0))

if __name__ == "__main__":
    # 可選：消音舊版 Firestore where 警告（我們已改新寫法，理論上不會再看到）
    # import warnings
    # warnings.filterwarnings(
    #     "ignore",
    #     category=UserWarning,
    #     module="google.cloud.firestore_v1.base_collection",
    # )
    main()