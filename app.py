# app.py — LINE 票數偵測 Bot（Cloud Run/本機均可）
import os, sys, time, random, hashlib, re, unicodedata, logging
from datetime import datetime
from typing import Tuple, Optional, List, Dict
from pathlib import Path

from fastapi import FastAPI, Request, Header, HTTPException
from fastapi.responses import JSONResponse, PlainTextResponse
from dotenv import load_dotenv

# ---- LINE Bot SDK v3 ----
from linebot.v3.webhook import WebhookHandler
from linebot.v3.webhooks import MessageEvent, TextMessageContent
from linebot.v3.messaging import (
    Configuration, ApiClient, MessagingApi,
    ReplyMessageRequest, PushMessageRequest,
    TextMessage as V3TextMessage,
    ApiException,
)
from linebot.v3.exceptions import InvalidSignatureError

# ---- HTTP / 解析 ----
import requests
try:
    import cloudscraper  # 可選：繞過部分 Cloudflare 防護
except ImportError:
    cloudscraper = None

from bs4 import BeautifulSoup

# ---- Firestore ----
from google.cloud import firestore


# ========= Logging =========
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
    force=True,
)
logger = logging.getLogger("tixwatch")
logger.setLevel(logging.INFO)
logger.handlers.clear()
logger.addHandler(logging.StreamHandler(sys.stdout))
logger.propagate = False


# ========= 環境變數 =========
ENV_PATH = Path(__file__).with_name(".env")  # 固定讀取和 app.py 同層 .env
load_dotenv(override=False)                  # 不覆蓋 Cloud Run 的環境

print("[ENV] TOKEN?", bool(os.getenv("LINE_CHANNEL_ACCESS_TOKEN")))
print("[ENV] SECRET?", bool(os.getenv("LINE_CHANNEL_SECRET")))

LINE_CHANNEL_SECRET = (os.getenv("LINE_CHANNEL_SECRET") or "").strip()
LINE_CHANNEL_ACCESS_TOKEN = (os.getenv("LINE_CHANNEL_ACCESS_TOKEN") or "").strip()
DEFAULT_INTERVAL = int(os.getenv("DEFAULT_INTERVAL", "15"))  # 秒
CRON_KEY = os.getenv("CRON_KEY", "")  # 可選：保護 /cron/tick

if not LINE_CHANNEL_SECRET or not LINE_CHANNEL_ACCESS_TOKEN:
    raise RuntimeError("請設定 LINE_CHANNEL_ACCESS_TOKEN 與 LINE_CHANNEL_SECRET 環境變數")


# ========= FastAPI =========
app = FastAPI(title="tixwatch-linebot")


# ========= LINE v3 初始化 =========
configuration = Configuration(access_token=LINE_CHANNEL_ACCESS_TOKEN)
api_client = ApiClient(configuration)
messaging_api = MessagingApi(api_client)
handler = WebhookHandler(LINE_CHANNEL_SECRET)


# ========= Firestore =========
# 本機需先設定 ADC；Cloud Run 上會自動帶服務帳戶
db = firestore.Client()
TASKS = db.collection("tasks")


# ========= 使用說明 =========
USAGE = (
    "🎫 票數偵測 Bot 使用說明\n"
    "・/watch <URL> [秒數]：開始監看（例：/watch https://tixcraft.com/... 15）\n"
    "・/list：查看我的監看列表\n"
    "・/stop <任務ID>：停止指定監看任務\n"
    "・輸入「取得說明」或 /start 可再看此訊息"
)
HELP_ALIASES = {"/start", "/help", "help", "取得說明", "說明", "指令", "使用說明", "開始使用", "教我用"}


# ========= 小工具 =========
def get_user_id(src) -> Optional[str]:
    # v3 的 SourceUser 欄位是 userId；保險再兼容 user_id
    return getattr(src, "userId", None) or getattr(src, "user_id", None)

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


# ========= 抓頁 & 判定 =========
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126 Safari/537.36"

def fetch_html(url: str, timeout=15) -> str:
    """
    強化版抓取：
    - 優先使用 cloudscraper（若可用）
    - 帶常見瀏覽器標頭、Referer
    - 指數退避重試數次
    """
    # 推測 Referer（使用同網域）
    try:
        from urllib.parse import urlparse
        pu = urlparse(url)
        referer = f"{pu.scheme}://{pu.netloc}/"
    except Exception:
        referer = "https://tixcraft.com/"

    headers = {
        "User-Agent": UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Referer": referer,
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-User": "?1",
    }

    sess = cloudscraper.create_scraper(
        browser={"browser": "chrome", "platform": "windows"}
    ) if cloudscraper else requests.Session()

    last_exc = None
    for i in range(4):  # 最多嘗試 4 次
        try:
            r = sess.get(url, headers=headers, timeout=timeout)
            if r.status_code >= 400:
                raise requests.HTTPError(f"{r.status_code} for {url}", response=r)
            return r.text
        except Exception as e:
            last_exc = e
            time.sleep(0.6 * (2 ** i) + random.uniform(0, 0.4))
    raise last_exc if last_exc else RuntimeError("fetch failed")

def normalize_text(s: str) -> str:
    s = unicodedata.normalize("NFKC", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def _parse_availability_from_line(line: str) -> Optional[tuple]:
    """
    從一行文字解析「<名稱> 剩餘 <數字>」。
    會回傳 (name, count) 或 None。
    """
    m = re.search(r"(.{1,40}?)\s*剩餘\s*(\d+)", line)
    if m:
        name = normalize_text(m.group(1))
        try:
            count = int(m.group(2))
        except ValueError:
            return None
        return name, count
    return None

def _merge_availability(items: List[tuple]) -> List[tuple]:
    """依名稱彙整剩餘數（同名取最大）。"""
    agg: Dict[str, int] = {}
    for name, cnt in items:
        agg[name] = max(cnt, agg.get(name, 0))
    return [(k, v) for k, v in agg.items()]

def extract_snapshot_and_ticket(html: str) -> Tuple[str, bool, List[tuple]]:
    """
    回傳：
      - snapshot: 純文字快照（含部分按鈕文字）
      - has_ticket: 初步是否可能有票
      - avail_list: [(區域名稱, 剩餘數), ...]
    """
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()

    # 全文文字
    text = normalize_text(soup.get_text(" ", strip=True))

    # 找「剩餘 N」的元素
    avail_raw: List[tuple] = []
    for node in soup.find_all(string=re.compile(r"剩餘\s*\d+")):
        line = normalize_text(node.parent.get_text(" ", strip=True))
        item = _parse_availability_from_line(line)
        if item:
            avail_raw.append(item)
    avail_list = _merge_availability(avail_raw)

    soldout_keywords = ["售完", "完售", "已售完", "已售罄", "已無票", "sold out", "soldout"]
    ticket_keywords  = ["立即購票", "購票", "加入購物車", "選擇座位", "剩餘", "可售", "尚有", "開賣", "tickets"]

    t_low = text.lower()
    has_ticket = bool(avail_list) or (
        any(kw.lower() in t_low for kw in ticket_keywords) and
        not any(kw.lower() in t_low for kw in soldout_keywords)
    )

    # 把常見可點按鈕文字也收進快照（幫助之後比對）
    important_bits = []
    for btn in soup.find_all(["a", "button"]):
        t = btn.get_text(" ", strip=True)
        if t:
            important_bits.append(normalize_text(t))
    snapshot = text + "\n\nBTN:" + "|".join(important_bits[:50])

    return snapshot, has_ticket, avail_list

def parse_availability_from_snapshot(snapshot: str) -> List[tuple]:
    """從舊的 snapshot 純文字中再抓一次『剩餘 N』，用於新舊對比。"""
    items: List[tuple] = []
    for m in re.finditer(r"(.{1,40}?)\s*剩餘\s*(\d+)", snapshot):
        name = normalize_text(m.group(1))
        try:
            cnt = int(m.group(2))
        except ValueError:
            continue
        items.append((name, cnt))
    return _merge_availability(items)


# ========= LINE 回覆/推播（v3）=========
def reply(event: MessageEvent, text: str):
    try:
        messaging_api.reply_message(
            ReplyMessageRequest(
                reply_token=event.reply_token,
                messages=[V3TextMessage(text=text)]
            )
        )
    except ApiException as e:
        logger.exception(f"[reply] LINE API error: {e}")

def push(user_id: str, message: str):
    try:
        messaging_api.push_message(
            PushMessageRequest(
                to=user_id,
                messages=[V3TextMessage(text=message)]
            )
        )
    except ApiException as e:
        logger.error(f"[push] LINE API error: {e}")


# ========= Webhook（LINE → /callback）=========
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


# ========= 訊息處理 =========
@handler.add(MessageEvent, message=TextMessageContent)
def handle_message(event: MessageEvent):
    text = (event.message.text or "").strip()
    user_id = get_user_id(event.source)
    logger.info(f"[event] user={user_id} text={text} replyToken={event.reply_token}")

    try:
        # 說明
        if text.startswith("/start") or text in HELP_ALIASES:
            reply(event, USAGE)
            return

        # /watch <URL> [秒數]
        if text.startswith("/watch"):
            parts = text.split()
            if len(parts) < 2:
                reply(event, "用法：/watch <URL> [秒數]")
                return
            if not user_id:
                reply(event, "請在與機器人「1 對 1」聊天中使用 /watch 指令。")
                return

            url = parts[1]
            try:
                interval_sec = int(parts[2]) if len(parts) >= 3 else DEFAULT_INTERVAL
                interval_sec = max(5, min(300, interval_sec))
            except ValueError:
                interval_sec = DEFAULT_INTERVAL

            # 先回覆已收到
            reply(event, f"收到，準備監看：\n{url}\n頻率：每 {interval_sec} 秒")
            try:
                tid = add_task(user_id, url, interval_sec)
                reply(event, f"已建立監看任務 #{tid}")
            except Exception as e:
                logger.exception(f"[watch] Firestore error: {e}")
                reply(event, "⚠️ 目前無法存取資料庫，請稍後再試。")
            return

        # /list
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

        # /stop <tid>
        if text.startswith("/stop"):
            parts = text.split()
            if len(parts) < 2:
                reply(event, "用法：/stop <任務ID>")
                return
            tid = parts[1]
            ok = deactivate_task(user_id or "", tid)
            reply(event, f"{'已停止' if ok else '找不到'}任務 #{tid}")
            return

        # 其他訊息先 echo
        reply(event, f"echo: {text}")

    except Exception as e:
        logger.exception(f"[event] unhandled error: {e}")
        reply(event, "系統發生錯誤，請稍後再試。")


# ========= 定時偵測（Cloud Scheduler → /cron/tick）=========
@app.get("/cron/tick")
async def cron_tick(request: Request):
    if CRON_KEY and request.headers.get("X-Cron-Key") != CRON_KEY:
        raise HTTPException(status_code=403, detail="Forbidden")

    try:
        tasks = all_active_tasks()
        random.shuffle(tasks)
        checked = 0

        for t in tasks:
            # 遵守用戶設定輪詢間隔
            if _now_ts() - int(t.get("last_checked", 0)) < int(t.get("interval_sec", DEFAULT_INTERVAL)):
                continue

            try:
                html = fetch_html(t["url"])
                snapshot, has_ticket, avail_now = extract_snapshot_and_ticket(html)

                # 舊快照的可售資訊，用於比較
                old_snapshot = t.get("last_snapshot") or ""
                avail_old = parse_availability_from_snapshot(old_snapshot)
                had_before = any(cnt > 0 for _, cnt in avail_old)
                has_now = any(cnt > 0 for _, cnt in avail_now)

                # 更新快照時間
                update_after_check(t["tid"], snapshot)

                # 僅在「過去無票 → 現在有票」時推播，避免洗通知
                if has_now and not had_before:
                    preview = "\n".join([f"• {name} 剩餘 {cnt}" for name, cnt in sorted(avail_now)[:8]])
                    msg = (
                        f"🎉 疑似有票釋出！\n"
                        f"任務#{t['tid']}\n{t['url']}\n"
                        f"{preview if preview else ''}\n"
                        f"（建議立刻點進去檢查與購買）"
                    )
                    push(t["user_id"], msg)

                checked += 1
                time.sleep(random.uniform(0.2, 0.6))  # 輕微節流

            except Exception as e:
                logger.error(f"[tick] task#{t.get('tid')} error: {e}")

        return JSONResponse({"ok": True, "checked": checked})

    except Exception as e:
        logger.exception(f"[cron_tick] unhandled: {e}")
        # 回 200 + 錯誤內容，避免 Scheduler 判定失敗
        return JSONResponse({"ok": False, "error": str(e)})


# ========= 健康檢查 =========
@app.get("/")
def health():
    return JSONResponse({"ok": True, "time": datetime.now().isoformat()})


# ========= 本機啟動 =========
if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "8000"))
    uvicorn.run("app:app", host="0.0.0.0", port=port, reload=True)