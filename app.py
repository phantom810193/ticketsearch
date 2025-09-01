# app.py — LINE 票數偵測 Bot（Cloud Run/本機均可）
import os, time, random, hashlib, re, unicodedata
import sys, logging
from datetime import datetime
from typing import Tuple, Optional, List, Dict

from fastapi import FastAPI, Request, Header, HTTPException
from fastapi.responses import JSONResponse, PlainTextResponse
from fastapi import Header
from dotenv import load_dotenv
from pathlib import Path

from linebot.v3.webhook import WebhookHandler
from linebot.v3.webhooks import MessageEvent, TextMessageContent
from linebot.v3.messaging import (
    Configuration, ApiClient, MessagingApi,
    ReplyMessageRequest, TextMessage as V3TextMessage,
    ApiException,  # v3 的 API 例外
)
from linebot.v3.exceptions import InvalidSignatureError

import requests
try:
    import cloudscraper
except ImportError:
    cloudscraper = None

from bs4 import BeautifulSoup
from google.cloud import firestore

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
    force=True,  # 覆蓋預設設定，避免沒有 handler
)

ENV_PATH = Path(__file__).with_name(".env")   # 固定讀取和 app.py 同一層的 .env
load_dotenv(override=False)

# 啟動時印診斷，方便確認是否讀到
print(f"[ENV] path={ENV_PATH} exists={ENV_PATH.exists()} loaded={ok}")
print("[ENV] TOKEN?", bool(os.getenv("LINE_CHANNEL_ACCESS_TOKEN")))
print("[ENV] SECRET?", bool(os.getenv("LINE_CHANNEL_SECRET")))

# ---- 必填環境變數（建議放 Secret Manager 並在部署時綁到環境）----
LINE_CHANNEL_SECRET = (os.getenv("LINE_CHANNEL_SECRET") or "").strip()
LINE_CHANNEL_ACCESS_TOKEN = (os.getenv("LINE_CHANNEL_ACCESS_TOKEN") or "").strip()
DEFAULT_INTERVAL = int(os.getenv("DEFAULT_INTERVAL", "15"))  # 秒
CRON_KEY = os.getenv("CRON_KEY", "")  # 可選：保護 /cron/tick

app = FastAPI(title="tixwatch-linebot")

# 若 token/secret 沒給，避免初始化失敗
if not LINE_CHANNEL_SECRET or not LINE_CHANNEL_ACCESS_TOKEN:
    raise RuntimeError("請設定 LINE_CHANNEL_ACCESS_TOKEN 與 LINE_CHANNEL_SECRET 環境變數")

line_bot_api = LineBotApi(LINE_CHANNEL_ACCESS_TOKEN)
handler = WebhookHandler(LINE_CHANNEL_SECRET)

# Firestore（本機請先設定 ADC；Cloud Run 上會自動帶服務帳戶）
db = firestore.Client()

USAGE = (
    "🎫 票數偵測 Bot 使用說明\n"
    "・/watch <URL> [秒數]：開始監看（例：/watch https://tixcraft.com/... 15）\n"
    "・/list：查看我的監看列表\n"
    "・/stop <任務ID>：停止指定監看任務\n"
    "・輸入「取得說明」或 /start 可再看此訊息"
)
HELP_ALIASES = {"/start", "/help", "help", "取得說明", "說明", "指令", "使用說明", "開始使用", "教我用"}

# ========== Firestore 資料層 ==========
TASKS = db.collection("tasks")

def _now_ts() -> int:
    return int(time.time())

def _gen_tid() -> str:
    import secrets, string
    alphabet = string.ascii_lowercase + string.digits
    return "".join(secrets.choice(alphabet) for _ in range(8))

def add_task(user_id: str, url: str, interval_sec: int) -> str:
    tid = _gen_tid()
    TASKS.document(tid).set({
        "tid": tid,
        "user_id": user_id,
        "url": url,
        "interval_sec": interval_sec,
        "is_active": True,
        "last_snapshot": "",
        "last_checked": 0,
        "created_at": _now_ts(),
    })
    return tid

def list_tasks(user_id: str) -> List[Dict]:
    docs = TASKS.where("user_id", "==", user_id).order_by(
        "created_at", direction=firestore.Query.DESCENDING
    ).stream()
    return [d.to_dict() for d in docs]

def deactivate_task(user_id: str, tid: str) -> bool:
    ref = TASKS.document(tid)
    snap = ref.get()
    if not snap.exists:
        return False
    data = snap.to_dict()
    if data.get("user_id") != user_id:
        return False
    ref.update({"is_active": False})
    return True

def all_active_tasks() -> List[Dict]:
    docs = TASKS.where("is_active", "==", True).stream()
    return [d.to_dict() for d in docs]

def update_after_check(tid: str, snapshot: str):
    TASKS.document(tid).update({"last_snapshot": snapshot, "last_checked": _now_ts()})

# ========== 抓頁 & 判定 ==========
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126 Safari/537.36"

def fetch_html(url: str, timeout=15) -> str:
    headers = {"User-Agent": UA, "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8"}
    session = cloudscraper.create_scraper(
        browser={"browser": "chrome", "platform": "windows"}
    ) if cloudscraper else requests.Session()
    r = session.get(url, headers=headers, timeout=timeout)
    r.raise_for_status()
    return r.text

def normalize_text(s: str) -> str:
    s = unicodedata.normalize("NFKC", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def extract_snapshot_and_ticket(html: str) -> Tuple[str, bool]:
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()

    text = normalize_text(soup.get_text(" ", strip=True))
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
    snapshot = text + "\n\nBTN:" + "|".join(important_bits[:50])
    return snapshot, has_ticket

def norm(s: Optional[str]) -> str:
    if not s:
        return ""
    s = unicodedata.normalize("NFKC", s).strip().lower()
    return re.sub(r"\s+", " ", s)

# ========== Logger ==========
logger = logging.getLogger("tixwatch")
logger.setLevel(logging.INFO)
logger.handlers.clear()
logger.addHandler(logging.StreamHandler(sys.stdout))
logger.propagate = False

# ========== Webhook（LINE → /callback）==========
@app.post("/callback")
@app.post("/callback/")
async def callback(request: Request,
                   x_line_signature: str | None = Header(None, alias="X-Line-Signature")):
    body_bytes = await request.body()
    body_text = body_bytes.decode("utf-8", errors="ignore")
    logger.info("[callback] UA=%s sig=%s body=%s",
                request.headers.get("user-agent"),
                "Y" if x_line_signature else "N",
                body_text[:200])

    if not x_line_signature:
        return PlainTextResponse("OK", status_code=200)

    try:
        handler.handle(body_text, x_line_signature)
    except InvalidSignatureError:
        logger.error("[callback] Invalid signature (check LINE_CHANNEL_SECRET)")
        raise HTTPException(status_code=400, detail="Invalid signature")

    return PlainTextResponse("OK", status_code=200)

# ========== 訊息處理 ==========
@handler.add(MessageEvent, message=TextMessage)
def handle_message(event: MessageEvent):
    text = (event.message.text or "").strip()
    # ✅ 一次正確取得 user_id；群組/聊天室可自行擴充
    user_id = getattr(event.source, "user_id", None)
    logger.info(f"[event] user={user_id} text={text} replyToken={event.reply_token}")

    try:
        if text.startswith("/start") or text in HELP_ALIASES:
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text=USAGE)
            )
            logger.info("[event] reply sent OK")
            return

        # ---- /watch ----
        if text.startswith("/watch"):
            parts = text.split()  # ✅ 修正：原本用 raw.split()（未定義）
            if len(parts) < 2:
                reply(event, "用法：/watch <URL> [秒數]")
                return
            url = parts[1]
            try:
                interval_sec = int(parts[2]) if len(parts) >= 3 else DEFAULT_INTERVAL
                interval_sec = max(5, min(300, interval_sec))
            except ValueError:
                interval_sec = DEFAULT_INTERVAL

            if not user_id:
                reply(event, "請在與機器人「1 對 1」聊天中使用 /watch 指令。")
                return

            tid = add_task(user_id, url, interval_sec)
            reply(event, f"已建立監看任務 #{tid}\nURL: {url}\n頻率：每 {interval_sec} 秒")
            return

        # ---- /list ----
        if text.startswith("/list"):
            items = list_tasks(user_id or "")
            if not items:
                reply(event, "目前沒有監看任務。用 /watch <URL> 開始吧！")
                return
            lines = ["你的監看任務："]
            for it in items:
                status = "監看中" if it.get("is_active") else "已停止"
                dt = datetime.fromtimestamp(it.get("created_at", 0)).strftime("%Y-%m-%d %H:%M")
                lines.append(f"#{it.get('tid')}｜{status}｜每{it.get('interval_sec')}秒｜{it.get('url')}｜{dt}")
            reply(event, "\n".join(lines))
            return

        # ---- /stop ----
        if text.startswith("/stop"):
            parts = text.split()  # ✅ 修正：原本用 raw.split()（未定義）
            if len(parts) < 2:
                reply(event, "用法：/stop <任務ID>")
                return
            tid = parts[1]
            ok = deactivate_task(user_id or "", tid)
            reply(event, f"{'已停止' if ok else '找不到'}任務 #{tid}")
            return

        # 其他訊息：先 echo 確認回覆正常
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text=f"echo: {text}"))
        logger.info("[event] reply sent OK")

    except LineBotApiError as e:
        # 更穩健的錯誤輸出
        detail = getattr(e, "error", None)
        logger.error(f"[event] LINE API error status={getattr(e, 'status_code', '?')} detail={detail}")
    except Exception as e:
        logger.exception(f"[event] unhandled error: {e}")

# ========== LINE 工具 ==========
def reply(event: MessageEvent, message: str):
    try:
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text=message))
    except LineBotApiError as e:
        logger.error(f"[reply] error status={getattr(e, 'status_code', '?')} detail={getattr(e, 'error', None)}")

def push(user_id: str, message: str):
    try:
        line_bot_api.push_message(user_id, TextSendMessage(text=message))
    except LineBotApiError as e:
        logger.error(f"[push] error status={getattr(e, 'status_code', '?')} detail={getattr(e, 'error', None)}")

# ========== 定時偵測（Cloud Scheduler → /cron/tick）==========
@app.get("/cron/tick")
async def cron_tick(request: Request):
    # 可選：用簡單金鑰防護
    if CRON_KEY and request.headers.get("X-Cron-Key") != CRON_KEY:
        raise HTTPException(status_code=403, detail="Forbidden")

    tasks = all_active_tasks()
    random.shuffle(tasks)
    checked = 0
    for t in tasks:
        # 確保達到用戶設定的輪詢間隔
        if _now_ts() - int(t.get("last_checked", 0)) < int(t.get("interval_sec", DEFAULT_INTERVAL)):
            continue
        try:
            html = fetch_html(t["url"])
            snapshot, has_ticket = extract_snapshot_and_ticket(html)
            new_hash = hashlib.sha256(snapshot.encode("utf-8")).hexdigest()
            old_hash = hashlib.sha256((t.get("last_snapshot") or "").encode("utf-8")).hexdigest()
            update_after_check(t["tid"], snapshot)
            if new_hash != old_hash and has_ticket:
                push(t["user_id"], f"🎉 疑似有票釋出！\n任務#{t['tid']}\n{t['url']}\n（建議立刻點進去檢查與購買）")
            checked += 1
            time.sleep(random.uniform(0.2, 0.6))
        except Exception as e:
            logger.error(f"[tick] task#{t.get('tid')} error: {e}")
    return JSONResponse({"ok": True, "checked": checked})

@app.get("/")
def health():
    return JSONResponse({"ok": True, "time": datetime.now().isoformat()})

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "8000"))
    uvicorn.run("app:app", host="0.0.0.0", port=port, reload=True)
