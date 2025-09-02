# worker.py — 住宅網路輪詢推播（Firestore + LINE v3）
import os
import sys
import time
import random
import logging
import hashlib
import re
import unicodedata
from typing import Tuple, List, Dict
from urllib.parse import urlparse

import requests
try:
    import cloudscraper  # 可選：較能處理部分 Cloudflare
except ImportError:
    cloudscraper = None

from bs4 import BeautifulSoup

# ========= Logging（避免 Windows 主控台編碼問題）=========
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

def _safe(s: str) -> str:
    """避免 console cp950 等編碼問題，去掉無法列印的字元。"""
    try:
        return s.encode(sys.stdout.encoding or "utf-8", errors="ignore").decode(sys.stdout.encoding or "utf-8", errors="ignore")
    except Exception:
        return s

# ========= 環境變數 =========
LINE_CHANNEL_ACCESS_TOKEN = (os.getenv("LINE_CHANNEL_ACCESS_TOKEN") or "").strip()
if not LINE_CHANNEL_ACCESS_TOKEN:
    raise RuntimeError("缺少環境變數 LINE_CHANNEL_ACCESS_TOKEN（用於推播）")

DEFAULT_INTERVAL = int(os.getenv("DEFAULT_INTERVAL", "15"))   # 預設任務輪詢秒數上限下限會在程式再控
WORKER_IDLE_SEC = float(os.getenv("WORKER_IDLE_SEC", "1.5"))  # 每輪閒置秒數
MAX_RETRY = int(os.getenv("FETCH_MAX_RETRY", "2"))
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "15"))
USER_AGENT = os.getenv("USER_AGENT") or "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126 Safari/537.36"
COOKIES_RAW = os.getenv("TIXCRAFT_COOKIES", "").strip()  # 可選："name=value; name2=value2"

# ========= LINE v3（僅推播用）=========
from linebot.v3.messaging import (
    Configuration, ApiClient, MessagingApi,
    PushMessageRequest, TextMessage as V3TextMessage, ApiException
)
configuration = Configuration(access_token=LINE_CHANNEL_ACCESS_TOKEN)
api_client = ApiClient(configuration)
messaging_api = MessagingApi(api_client)

def push(user_id: str, message: str):
    """推播給使用者：log 不印 emoji，避免 Windows 編碼錯誤。"""
    try:
        messaging_api.push_message(
            PushMessageRequest(
                to=user_id,
                messages=[V3TextMessage(text=message)]
            )
        )
        logger.info("[push] sent to %s: %s", user_id, _safe(message[:80].replace("\n", " ")))
    except ApiException as e:
        logger.error("[push] LINE API error: %s", e)

# ========= Firestore =========
from google.cloud import firestore
from google.cloud.firestore_v1 import FieldFilter

PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT") or os.getenv("GCP_PROJECT") or "ticketsearch-470701"
db = firestore.Client(project=PROJECT_ID)
TASKS = db.collection("tasks")
print("[ENV] PROJECT_ID =", PROJECT_ID)

def _now_ts() -> int:
    return int(time.time())

def all_active_tasks() -> List[Dict]:
    # 使用 FieldFilter 避免 where 的警告
    docs = TASKS.where(filter=FieldFilter("is_active", "==", True)).stream()
    return [d.to_dict() for d in docs]

def update_after_check(tid: str, snapshot: str):
    TASKS.document(tid).update({"last_snapshot": snapshot, "last_checked": _now_ts()})

# ========= 取頁面 =========
def _cookies_dict(raw: str) -> Dict[str, str]:
    if not raw:
        return {}
    pairs = [p.strip() for p in raw.split(";") if p.strip()]
    out = {}
    for p in pairs:
        if "=" in p:
            k, v = p.split("=", 1)
            out[k.strip()] = v.strip()
    return out

def _make_session():
    if cloudscraper:
        return cloudscraper.create_scraper(browser={"browser": "chrome", "platform": "windows"})
    s = requests.Session()
    return s

def fetch_html(url: str, timeout: int = REQUEST_TIMEOUT, retries: int = MAX_RETRY) -> str:
    """以一般/residential 環境存取，偵測 403/5xx 重試。"""
    sess = _make_session()
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Connection": "keep-alive",
        "DNT": "1",
        "Referer": f"{urlparse(url).scheme}://{urlparse(url).hostname}/",
    }
    cookies = _cookies_dict(COOKIES_RAW)

    last_exc = None
    for attempt in range(retries + 1):
        try:
            r = sess.get(url, headers=headers, cookies=cookies or None, timeout=timeout)
            # 某些網站對 403/429/503 才需要重試
            if r.status_code in (403, 429, 503):
                raise requests.HTTPError(f"{r.status_code} for {url}", response=r)
            r.raise_for_status()
            # 取文字
            return r.text
        except Exception as e:
            last_exc = e
            code = getattr(getattr(e, "response", None), "status_code", None)
            logger.warning("[fetch] attempt=%s code=%s url=%s", attempt, code, url)
            time.sleep(0.8 + attempt * 0.8 * random.random())
    # 全部失敗
    raise last_exc

# ========= 文字正規化 & 票券偵測 =========
SOLDOUT_KWS = ["售完", "完售", "已售完", "已售罄", "已無票", "sold out", "soldout"]
TICKET_KWS  = ["立即購票", "購票", "加入購物車", "選擇座位", "剩餘", "可售", "尚有", "開賣", "tickets"]

def normalize_text(s: str) -> str:
    s = unicodedata.normalize("NFKC", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def extract_snapshot_and_ticket(html: str) -> Tuple[str, bool, List[str]]:
    """
    萃取頁面文字快照與是否判定「有票」。
    另外傳回 area_hits：例如 ['A區 剩餘 12', 'B區 尚有 5']，可放進推播訊息。
    """
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()

    text = normalize_text(soup.get_text(" ", strip=True))
    low = text.lower()

    # 關鍵字判斷（不包含 soldout）
    has_kw = any(kw.lower() in low for kw in TICKET_KWS)
    is_soldout = any(kw.lower() in low for kw in SOLDOUT_KWS)
    has_ticket = has_kw and not is_soldout

    # 進一步抓各區「剩餘/尚有/可售 數字」
    area_hits = []
    # 範例：A區 剩餘 12、搖滾區 尚有10、B3 可售 3
    for m in re.finditer(r"([A-Za-z0-9\u4e00-\u9fff]{1,12}區)\s*(?:座位|門票|票)?\s*(剩餘|尚有|可售)\s*(\d+)", text):
        g = f"{m.group(1)} {m.group(2)} {m.group(3)}"
        if g not in area_hits:
            area_hits.append(g)
    # 補抓「立即購票/選擇座位/加入購物車」的按鈕文字
    important_bits = []
    for btn in soup.find_all(["a", "button"]):
        t = btn.get_text(" ", strip=True)
        if t:
            important_bits.append(normalize_text(t))
    snapshot = text + "\n\nBTN:" + "|".join(important_bits[:60])

    # 如果解析到 area_hits，則一定視為 has_ticket
    if area_hits:
        has_ticket = True

    return snapshot, has_ticket, area_hits

# ========= 主迴圈 =========
def _clamp_interval(v: int) -> int:
    try:
        v = int(v)
    except Exception:
        v = DEFAULT_INTERVAL
    return max(5, min(300, v))

def run_once() -> int:
    """執行一輪：掃描到期的任務，回傳已檢查數量。"""
    tasks = all_active_tasks()
    random.shuffle(tasks)

    logger.info("抓到 %d 個活躍任務", len(tasks))
    checked = 0
    now = _now_ts()

    for t in tasks:
        try:
            last_checked = int(t.get("last_checked", 0) or 0)
            interval_sec = _clamp_interval(t.get("interval_sec", DEFAULT_INTERVAL))
            if (now - last_checked) < interval_sec:
                continue

            tid = t.get("tid")
            url = t.get("url")
            user_id = t.get("user_id")
            logger.info("→ 檢查 task#%s 每 %ss url=%s", tid, interval_sec, url)

            html = fetch_html(url)
            snapshot, has_ticket, area_hits = extract_snapshot_and_ticket(html)

            new_hash = hashlib.sha256(snapshot.encode("utf-8")).hexdigest()
            old_hash = hashlib.sha256((t.get("last_snapshot") or "").encode("utf-8")).hexdigest()
            first_run = not bool(t.get("last_snapshot"))

            update_after_check(tid, snapshot)
            changed = (new_hash != old_hash)

            logger.info("[check] task#%s has_ticket=%s first_run=%s changed=%s", tid, has_ticket, first_run, changed)

            # == 通知條件 ==
            # 1) 有票 且 (第一次 | 內容變更) 就推播
            if has_ticket and (first_run or changed):
                detail = f"\n" + "\n".join(f"・{h}" for h in area_hits[:10]) if area_hits else ""
                # 推播文字可以包含 emoji，不寫入 log
                msg = f"🎉 疑似有票釋出！\n任務#{tid}\n{url}{detail}\n（建議立刻點進去檢查與購買）"
                push(user_id, msg)

            checked += 1
            time.sleep(random.uniform(0.2, 0.6))

        except requests.HTTPError as he:
            code = getattr(getattr(he, "response", None), "status_code", None)
            logger.error("[check] HTTPError %s for %s", code, t.get("url"))
        except Exception as e:
            logger.exception("[check] task#%s error: %s", t.get("tid"), e)

    return checked

def main():
    logger.info("%s", _safe("worker 啟動（住宅網路模式）"))
    try:
        while True:
            n = run_once()
            # 沒任務就稍微休息久一點
            time.sleep(WORKER_IDLE_SEC if n else max(WORKER_IDLE_SEC, 2.5))
    except KeyboardInterrupt:
        logger.info("收到中斷訊號，結束。")

if __name__ == "__main__":
    main()