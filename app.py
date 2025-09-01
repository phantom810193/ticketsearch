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

def get_user_id(src) -> Optional[str]:
    # v3 的 SourceUser 欄位是 userId；保險再兼容 user_id
    return getattr(src, "userId", None) or getattr(src, "user_id", None)

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
    # 需要 Firestore 複合索引：user_id (升冪) + created_at (降冪)
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

def _parse_tixcraft_areas(soup: BeautifulSoup) -> List[tuple]:
    """
    嘗試從拓元/年代(Tixcraft) 區域列表抓出「區域名稱 + 剩餘數量」。
    Tixcraft 常見格式：'特E區4280 剩餘 52'（在 <li> 或 <div> 內）
    """
    results: List[tuple] = []
    for node in soup.find_all(["li", "div", "span"]):
        t = node.get_text(" ", strip=True)
        if not t:
            continue
        t = t.replace("\u3000", " ").replace("\xa0", " ")
        # 忽略僅有「已售完」的項目
        if "已售" in t or "售完" in t:
            # 若同一行同時有「剩餘 N」，仍視為有票
            m = re.search(r"(剩餘|尚有)\s*(\d+)", t)
            if not m:
                continue
        m = re.search(r"(剩餘|尚有)\s*(\d+)", t)
        if m:
            cnt = int(m.group(2))
            if cnt > 0:
                area = t.split(m.group(1))[0].strip()
                # 清掉前導顏色圖例等雜字元
                area = re.sub(r"^\W+", "", area)
                results.append((area, cnt))
    # 去重、排序
    uniq = {}
    for name, cnt in results:
        uniq[name] = max(cnt, uniq.get(name, 0))
    return sorted(uniq.items(), key=lambda x: (-x[1], x[0]))  # 先多到少

def extract_snapshot_and_ticket(html: str) -> Tuple[str, bool, List[tuple]]:
    """
    回傳：
      snapshot: 會存進 Firestore 用於變更偵測的摘要字串
      has_ticket: 是否偵測到有票
      details: [(區域, 剩餘數)] 清單（若可解析）
    """
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()

    # 先嘗試 Tixcraft 區域偵測
    areas = _parse_tixcraft_areas(soup)
    if areas:
        # 只將「有票區域+數量」納入 snapshot，避免被大量「已售完」文字影響
        top = [f"{n}:{c}" for n, c in areas[:50]]
        snapshot = "TIXCRAFT_AVAIL|" + "|".join(top)
        return snapshot, True, areas

    # 若沒抓到區域，就退回關鍵字判定（不再因為頁面上有 '已售完' 就直接否決）
    text = normalize_text(soup.get_text(" ", strip=True))
    positive = ["剩餘", "尚有", "可售", "立即購票", "購票", "加入購物車", "選擇座位", "開賣", "tickets"]
    has_ticket = any(p in text for p in positive)
    # 蒐集前幾個按鈕文字，作為 snapshot 的一部分
    important_bits = []
    for btn in soup.find_all(["a", "button"]):
        t = btn.get_text(" ", strip=True)
        if t:
            important_bits.append(normalize_text(t))
    btns = "|".join(important_bits[:30])
    snapshot = "GENERIC|" + btns
    return snapshot, has_ticket, []

def norm(s: Optional[str]) -> str:
    if not s:
        return ""
    s = unicodedata.normalize("NFKC", s).strip().lower()
    return re.sub(r"\s+", " ", s)

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
                   x_line_signature: Optional[str] = Header(None, alias="X-Line-Signature")):
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

            # 先回覆已收到，避免 Firestore 出錯就沒回覆
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
        # 你已經把 header 帶進 Scheduler，這裡留著即可
        raise HTTPException(status_code=403, detail="Forbidden")

    try:
        tasks = all_active_tasks()
        random.shuffle(tasks)
        checked = 0
        for t in tasks:
            # 確保達到用戶設定的輪詢間隔
            if _now_ts() - int(t.get("last_checked", 0)) < int(t.get("interval_sec", DEFAULT_INTERVAL)):
                continue
            try:
                html = fetch_html(t["url"])
                snapshot, has_ticket, areas = extract_snapshot_and_ticket(html)

                # 只用「有票區域摘要」當快照，能準確偵測變化
                new_hash = hashlib.sha256(snapshot.encode("utf-8")).hexdigest()
                old_hash = hashlib.sha256((t.get("last_snapshot") or "").encode("utf-8")).hexdigest()
                update_after_check(t["tid"], snapshot)

                if has_ticket and new_hash != old_hash:
                    # 組推播內容
                    if areas:
                        top_lines = "\n".join([f"• {name} 剩餘 {cnt}" for name, cnt in areas[:10]])
                        msg = (
                            "🎉 偵測到可購票區域！\n"
                            f"任務#{t['tid']}\n{t['url']}\n"
                            f"{top_lines}\n\n（建議立刻點進去檢查與購買）"
                        )
                    else:
                        msg = (
                            "🎉 偵測到可購票！\n"
                            f"任務#{t['tid']}\n{t['url']}\n"
                            "（偵測到購票按鈕/關鍵字）"
                        )
                    push(t["user_id"], msg)

                checked += 1
                time.sleep(random.uniform(0.2, 0.6))
            except Exception as e:
                logger.error(f"[tick] task#{t.get('tid')} error: {e}")
        return JSONResponse({"ok": True, "checked": checked})
    except Exception as e:
        logger.exception(f"[cron_tick] unhandled: {e}")
        # 回 200 + 錯誤內容，避免 Scheduler 看到 5xx
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