# worker.py — 住宅網路輪詢 Worker（讀 Firestore 任務，抓票況，LINE 推播）
# 需求：
#   pip install google-cloud-firestore line-bot-sdk requests cloudscraper bs4 python-dotenv
# 必要環境變數：
#   LINE_CHANNEL_ACCESS_TOKEN   （LINE 長期存取權杖，用於 push）
#   （可選）GOOGLE_CLOUD_PROJECT 或預設 ADC
#   （可選）HTTP(S)_PROXY / PROXY_URL / COOKIE / USER_AGENT / REQUEST_TIMEOUT
# 使用：
#   python worker.py    # 會持續輪詢
#   Windows 如要即時看 log：python worker.py *>&1 | Tee-Object -FilePath .\worker.log

import os
import sys
import time
import random
import hashlib
import re
import logging
from datetime import datetime
from typing import Dict, List, Tuple, Optional

import requests
try:
    import cloudscraper  # 若有安裝可稍微降低被擋風險
except ImportError:
    cloudscraper = None

from bs4 import BeautifulSoup
from google.cloud import firestore

# ========= Logging（避免 Windows 編碼炸裂，全部使用 ASCII）=========
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
    force=True,
)
logger = logging.getLogger("tixworker")
logger.propagate = False

# ========= 環境變數 =========
LINE_CHANNEL_ACCESS_TOKEN = (os.getenv("LINE_CHANNEL_ACCESS_TOKEN") or "").strip()
if not LINE_CHANNEL_ACCESS_TOKEN:
    raise RuntimeError("缺少環境變數 LINE_CHANNEL_ACCESS_TOKEN")

PROJECT_ID = (os.getenv("GOOGLE_CLOUD_PROJECT") or os.getenv("PROJECT_ID") or "").strip() or None

REQUEST_TIMEOUT = float(os.getenv("REQUEST_TIMEOUT", "15"))
UA = os.getenv(
    "USER_AGENT",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126 Safari/537.36",
)

# 代理（如不需要可不設）
PROXY_URL = os.getenv("PROXY_URL", "").strip()
HTTP_PROXY = os.getenv("HTTP_PROXY", "").strip()
HTTPS_PROXY = os.getenv("HTTPS_PROXY", "").strip()

# 站台 Cookie（選填，若需要繞過身分/排程限制）
COOKIE_RAW = os.getenv("COOKIE", "").strip()


# ========= LINE v3 推播 =========
from linebot.v3.messaging import (
    Configuration, ApiClient, MessagingApi,
    PushMessageRequest, TextMessage as V3TextMessage,
)
from linebot.v3.messaging import ApiException as LineApiException

_line_cfg = Configuration(access_token=LINE_CHANNEL_ACCESS_TOKEN)
_line_cli = ApiClient(_line_cfg)
_line_api = MessagingApi(_line_cli)

def push_line(user_id: str, text: str) -> bool:
    """送 LINE 推播；失敗回 False。"""
    try:
        _line_api.push_message(
            PushMessageRequest(
                to=user_id,
                messages=[V3TextMessage(text=text)]
            )
        )
        logger.info(f"[push] sent to {user_id[:8]}... ({len(text)} chars)")
        return True
    except LineApiException as e:
        logger.error(f"[push] LINE error: {e}")
        return False


# ========= Firestore =========
# 本機請先設定 ADC；PROJECT_ID 可省略使用預設。Cloud 本地/住宅網路都可。
db = firestore.Client(project=PROJECT_ID) if PROJECT_ID else firestore.Client()
TASKS = db.collection("tasks")

def _now_ts() -> int:
    return int(time.time())

def list_active_tasks() -> List[Dict]:
    # 只取 is_active==True 的任務；可依需要調整排序
    q = TASKS.where("is_active", "==", True)
    return [d.to_dict() for d in q.stream()]

def update_after_check(tid: str, snapshot: str):
    TASKS.document(tid).update({
        "last_snapshot": snapshot,
        "last_checked": _now_ts(),
    })


# ========= 抓頁面 =========
def _cookies_dict(raw: str) -> Dict[str, str]:
    """把 'a=1; b=2' 轉成 {'a':'1','b':'2'}；忽略 Path/HttpOnly 等屬性片段。"""
    if not raw:
        return {}
    cookies: Dict[str, str] = {}
    for part in re.split(r";\s*", raw.strip()):
        if not part or "=" not in part:
            continue
        k, v = part.split("=", 1)
        cookies[k.strip()] = v.strip()
    return cookies

def build_session() -> requests.Session:
    if cloudscraper:
        sess = cloudscraper.create_scraper(
            browser={"browser": "chrome", "platform": "windows"}
        )
    else:
        sess = requests.Session()

    sess.headers.update({
        "User-Agent": UA,
        "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "DNT": "1",
        "Connection": "keep-alive",
    })

    # 代理
    proxies: Dict[str, str] = {}
    if PROXY_URL:
        proxies["http"] = PROXY_URL
        proxies["https"] = PROXY_URL
    if HTTP_PROXY:
        proxies["http"] = HTTP_PROXY
    if HTTPS_PROXY:
        proxies["https"] = HTTPS_PROXY
    if proxies:
        sess.proxies.update(proxies)
        logger.info("[net] using proxies: %s", proxies)

    # Cookie
    if COOKIE_RAW:
        sess.cookies.update(_cookies_dict(COOKIE_RAW))
        logger.info("[net] using COOKIE from env (len=%d)", len(COOKIE_RAW))

    return sess

def fetch_html(sess: requests.Session, url: str) -> Tuple[int, str]:
    try:
        r = sess.get(url, timeout=REQUEST_TIMEOUT, allow_redirects=True)
        return r.status_code, r.text if r.ok else ""
    except requests.HTTPError as e:
        logger.warning(f"[fetch] HTTPError {e}")
        return 0, ""
    except requests.RequestException as e:
        logger.warning(f"[fetch] RequestException {type(e).__name__}: {e}")
        return 0, ""


# ========= 解析與偵測 =========
def normalize_text(s: str) -> str:
    # 簡化空白、避免雜訊
    s = re.sub(r"\s+", " ", s or "")
    return s.strip()

def extract_availability(html: str) -> Tuple[str, bool, Dict[str, int]]:
    """
    回傳 (snapshot, has_ticket, area_left_map)
    - snapshot：拿來做 diff（避免重複推播）
    - has_ticket：是否偵測到有票字樣
    - area_left_map：嘗試從區塊附近抓出 '剩餘/可售/尚有' 數字
    """
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()

    text = normalize_text(soup.get_text(" ", strip=True))

    # 一般關鍵字
    soldout_keywords = ["售完", "完售", "已售完", "已售罄", "已無票", "sold out", "soldout"]
    ticket_keywords  = ["立即購票", "購票", "加入購物車", "選擇座位", "剩餘", "可售", "尚有", "tickets"]

    t_low = text.lower()
    has_ticket = any(kw.lower() in t_low for kw in ticket_keywords) and not any(
        kw.lower() in t_low for kw in soldout_keywords
    )

    # 針對 tixcraft area 頁面常見樣式，嘗試抓「區域／尚有／剩餘」數字
    area_left: Dict[str, int] = {}
    # 找可能的「剩餘/可售/尚有 + 數字」
    for m in re.finditer(r"(剩餘|可售|尚有)\s*(\d+)", text):
        count = int(m.group(2))
        if count <= 0:
            continue
        # 嘗試在匹配附近拿一小段字作為區域名稱（很 heuristics，但通常夠用）
        start = max(0, m.start() - 20)
        ctx = text[start:m.start()]
        # 取最後一個「區/樓/排/座/票種」等字樣附近的短字串當區名
        area_match = re.search(r"([A-Za-z0-9一-龥]{1,8}區|[A-Za-z0-9一-龥]{1,8}樓|[A-Za-z0-9一-龥]{1,8}側|[A-Za-z0-9一-龥]{1,8}排)?$", ctx)
        area_name = (area_match.group(0) if area_match else "").strip(" ，:;")
        if not area_name:
            area_name = "某區"
        area_left[area_name] = max(count, area_left.get(area_name, 0))
        has_ticket = True  # 有找到數字也視為疑似有票

    # 另外把頁面上常見 button/連結文字收集至 snapshot
    important_bits: List[str] = []
    for btn in soup.find_all(["a", "button"]):
        t = normalize_text(btn.get_text(" ", strip=True))
        if t:
            important_bits.append(t)
    snapshot = text[:4000] + "\n\nBTN:" + "|".join(important_bits[:80])
    return snapshot, has_ticket, area_left


# ========= 主流程 =========
def sweep_once(sess: requests.Session) -> int:
    """跑一輪：挑達到間隔的任務去抓，回傳檢查數量。"""
    tasks = list_active_tasks()
    random.shuffle(tasks)
    logger.info(f"active tasks = {len(tasks)}")
    checked = 0
    now = _now_ts()

    for t in tasks:
        try:
            tid = t.get("tid")
            url = t.get("url")
            user_id = t.get("user_id")
            interval_sec = int(t.get("interval_sec") or 15)
            last_checked = int(t.get("last_checked") or 0)

            if not tid or not url or not user_id:
                continue

            if now - last_checked < max(5, min(300, interval_sec)):
                continue

            logger.info(f"-> check task#{tid} {url}")

            code, html = fetch_html(sess, url)
            if code == 403:
                logger.warning(f"[fetch] code=403 for {url}")
                # 更新 last_checked，避免短時間內被擋重試過多
                update_after_check(tid, t.get("last_snapshot") or "")
                time.sleep(random.uniform(0.2, 0.6))
                continue
            if code == 0:
                # 網路錯誤，不更新 last_checked 以便下輪重試
                continue
            if code != 200:
                logger.warning(f"[fetch] code={code} for {url}")
                update_after_check(tid, t.get("last_snapshot") or "")
                time.sleep(random.uniform(0.2, 0.6))
                continue

            snapshot, has_ticket, area_left = extract_availability(html)
            new_hash = hashlib.sha256(snapshot.encode("utf-8", errors="ignore")).hexdigest()
            old_hash = hashlib.sha256((t.get("last_snapshot") or "").encode("utf-8", errors="ignore")).hexdigest()

            # 一定要更新 last_snapshot / last_checked（避免重複抓）
            update_after_check(tid, snapshot)
            checked += 1

            # 推播條件：內容有變化且偵測到疑似有票
            if has_ticket and new_hash != old_hash:
                # 組裝摘要
                summary_lines: List[str] = []
                if area_left:
                    # 只取前幾個區域以免訊息太長
                    top = list(area_left.items())[:6]
                    summary = ", ".join([f"{k}:{v}" for k, v in top])
                    summary_lines.append(f"區域剩餘：{summary}")
                msg = "疑似有票釋出！\n" + f"任務#{tid}\n{url}"
                if summary_lines:
                    msg += "\n" + "\n".join(summary_lines)
                # 無表情符號，避免 Windows 主控台編碼問題
                push_line(user_id, msg)

            time.sleep(random.uniform(0.2, 0.6))
        except Exception as e:
            logger.error(f"[sweep] task#{t.get('tid')} error: {e}")
            # 錯誤也不要卡住其他任務
            time.sleep(0.2)

    return checked


def main():
    logger.info("worker start (residential mode)")
    sess = build_session()
    # 立即先跑一輪，之後固定間隔巡迴
    while True:
        try:
            n = sweep_once(sess)
            # 若沒有檢查任何任務，稍微等久一點
            sleep_sec = 2 if n > 0 else 5
            time.sleep(sleep_sec)
        except KeyboardInterrupt:
            logger.info("stopped by user")
            break
        except Exception as e:
            logger.error(f"[main] unhandled error: {e}")
            time.sleep(3)


if __name__ == "__main__":
    main()