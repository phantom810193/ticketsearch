# app.py — LINE 票數偵測 Bot（支援 ibon，Cloud Run/本機均可）
import os, sys, time, random, re, json, unicodedata, logging
from datetime import datetime
from typing import Tuple, Optional, List, Dict
from pathlib import Path
from urllib.parse import urlparse, urlparse as _urlparse

from fastapi import FastAPI, Request, Header, HTTPException, Query
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

ALLOWED_HOSTS = {"ticket.ibon.com.tw"}  # 僅允許 ibon 連結


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
    "・/watch <URL> [秒數]：開始監看（例：/watch https://ticket.ibon.com.tw/... 15）\n"
    "・/list：查看我的監看列表\n"
    "・/stop <任務ID>：停止指定監看任務\n"
    "・輸入「取得說明」或 /start 可再看此訊息"
)
HELP_ALIASES = {"/start", "/help", "help", "取得說明", "說明", "指令", "使用說明", "開始使用", "教我用"}


# ========= 小工具 =========
def get_user_id(src) -> Optional[str]:
    # v3 的 SourceUser 是 userId；保險兼容舊屬性 user_id
    return getattr(src, "userId", None) or getattr(src, "user_id", None)

def _now_ts() -> int:
    return int(time.time())

def _gen_tid() -> str:
    import secrets, string
    alphabet = string.ascii_lowercase + string.digits
    return "".join(secrets.choice(alphabet) for _ in range(8))

def normalize_text(s: str) -> str:
    s = unicodedata.normalize("NFKC", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def fmt_ts(ts: int) -> str:
    if not ts:
        return "從未"
    dt = datetime.fromtimestamp(ts)
    return dt.strftime("%Y-%m-%d %H:%M:%S")

def is_allowed_url(url: str) -> bool:
    try:
        u = urlparse(url)
        return u.scheme in ("http", "https") and u.netloc in ALLOWED_HOSTS
    except Exception:
        return False


# ========= 任務存取 =========
def add_task(user_id: str, url: str, interval_sec: int) -> str:
    tid = _gen_tid()
    TASKS.document(tid).set({
        "tid": tid,
        "user_id": user_id,
        "url": url,
        "interval_sec": max(5, int(interval_sec)),
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

def find_active_task_by_url(user_id: str, url: str) -> Optional[Dict]:
    # 避免 composite index 需求：先抓使用者全部，再過濾
    for t in list_tasks(user_id):
        if t.get("is_active") and t.get("url") == url:
            return t
    return None

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

def touch_last_checked(tid: str):
    TASKS.document(tid).update({"last_checked": _now_ts()})


# ========= 抓頁 =========
def fetch_html(url: str, timeout=15) -> str:
    """
    強化抓取：
    - Referer 直接用目標 URL
    - 帶常見瀏覽器 header（含 sec-ch-ua）
    - 命中 ibon 站台時補 Host/Origin，降低 403/跳轉機率
    - 指數退避重試
    - 若環境有設定 HTTP(S)_PROXY 會自動走代理
    """
    u = urlparse(url)
    is_ibon = u.netloc.endswith("ticket.ibon.com.tw")

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Referer": url,
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-User": "?1",
        "sec-ch-ua": '"Chromium";v="126", "Google Chrome";v="126", "Not.A/Brand";v="24"',
        "sec-ch-ua-platform": '"Windows"',
        "sec-ch-ua-mobile": "?0",
    }
    if is_ibon:
        headers["Host"] = u.netloc
        headers["Origin"] = f"{u.scheme}://{u.netloc}"

    sess = cloudscraper.create_scraper(
        browser={"browser": "chrome", "platform": "windows"}
    ) if cloudscraper else requests.Session()

    last_exc = None
    for i in range(4):  # 最多 4 次
        try:
            r = sess.get(url, headers=headers, timeout=timeout, allow_redirects=True)
            if r.status_code >= 400:
                raise requests.HTTPError(f"{r.status_code} for {url}", response=r)
            return r.text
        except Exception as e:
            last_exc = e
            time.sleep(0.6 * (2 ** i) + random.uniform(0, 0.4))
    raise last_exc if last_exc else RuntimeError("fetch failed")


# ========= 解析 =========
def extract_areas_left_from_text(text: str) -> Dict[str, int]:
    """
    從整頁文字中抓出「xxx區... 剩餘 N / 餘票 N / 可售 N」的片段，回傳 {區名: 數量}
    - 盡量包容 ibon/tixcraft 不同用詞
    """
    areas: Dict[str, int] = {}
    t = normalize_text(text)
    # 例：特G區3980 剩餘 85、黃2B區3680 餘票29、A區 3000 可售：12
    patt = re.compile(
        r"([^\s\|]{1,12}區[^\s\|]{0,12})\s*(?:剩餘|餘票|可售|尚有|Available|Remain|Remaining)\s*[:：]?\s*(\d+)"
    )
    for name, num in patt.findall(t):
        n = int(num)
        if n > 0:
            areas[name] = areas.get(name, 0) + n
    return areas

def extract_snapshot_and_ticket(html: str) -> Tuple[str, bool, Dict[str, int]]:
    """
    綜合判斷是否有票：
    1) 頁面可見文字的關鍵字
    2) 從 <script> 內的 JSON 抓出區名/餘票（ibon 常見）
    3) 以區名+剩餘數彙總
    """
    soup = BeautifulSoup(html, "html.parser")

    # 先把所有 script 內容保留一份字串，等等掃 JSON 線索
    raw_scripts = " ".join(s.get_text(" ", strip=True) for s in soup.find_all("script"))

    # 清 script/style 再做可見文字擷取
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()

    text = normalize_text(soup.get_text(" ", strip=True))

    # 關鍵字（包含 ibon 用語）
    soldout_keywords = [
        "售完", "完售", "已售完", "已售罄", "販售結束", "銷售完畢", "本場次完售",
        "sold out", "soldout"
    ]
    ticket_keywords = [
        "立即購票", "我要購票", "立即購買", "選擇座位", "加入購物車",
        "剩餘", "餘票", "可售", "尚有", "開賣", "tickets"
    ]

    t_low = (text + " " + raw_scripts).lower()
    has_ticket_by_kw = any(kw.lower() in t_low for kw in ticket_keywords) and not any(
        kw.lower() in t_low for kw in soldout_keywords
    )

    # 先從可見文字抽（泛用）
    areas_left = extract_areas_left_from_text(text)

    # 再從 script 內 JSON 抓（ibon 常用字段：AreaName/Remain/RemainQty/Available 等）
    # 形式 1: {"AreaName":"A區","Remain":12}
    patt1 = re.compile(
        r'"(?:AreaName|areaName|Area|區名|區域)"\s*:\s*"([^"]{1,20})"[^{}]{0,200}?"(?:Remain|remain|RemainQty|remainQty|Available|available)"\s*:\s*(\d+)'
    )
    for name, num in patt1.findall(raw_scripts):
        n = int(num)
        if n > 0:
            areas_left[name] = areas_left.get(name, 0) + n

    # 形式 2: 先找出一段可能的陣列再 JSON decode（例如 var areaData = [...]）
    arr_match = re.search(r'(?:var\s+\w+\s*=\s*|\b)\[{"?.*?}?]\s*;?', raw_scripts)
    if arr_match:
        blob = arr_match.group(0)
        # 去掉前綴 "var xxx ="
        blob = re.sub(r'^var\s+\w+\s*=\s*', '', blob).strip().rstrip(';')
        try:
            # 嘗試將類 JSON 正規化
            blob = blob.replace("\\'", "'")
            data = json.loads(blob)
            if isinstance(data, list):
                for it in data:
                    name = it.get("AreaName") or it.get("areaName") or it.get("Area") or it.get("區名") or it.get("區域")
                    remain = (
                        it.get("Remain") or it.get("remain") or
                        it.get("RemainQty") or it.get("remainQty") or
                        it.get("Available") or it.get("available")
                    )
                    if name and isinstance(remain, int) and remain > 0:
                        areas_left[name] = areas_left.get(name, 0) + int(remain)
        except Exception:
            pass  # JSON 容錯

    has_ticket = has_ticket_by_kw or (sum(areas_left.values()) > 0)

    # 收集可見按鈕文案，便於快照比對
    important_bits = []
    for btn in soup.find_all(["a", "button"]):
        t = btn.get_text(" ", strip=True)
        if t:
            important_bits.append(normalize_text(t))
    snapshot = text + "\n\nBTN:" + "|".join(important_bits[:50])

    return snapshot, has_ticket, areas_left


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


# ========= 推播訊息組裝 =========
def compose_ticket_message(url: str, areas_left: Dict[str, int], snapshot: str) -> str:
    parts = [f"🎟️ 偵測到票券釋出！", url]
    if areas_left:
        show = []
        # 只顯示前幾個區名，避免太長
        for i, (k, v) in enumerate(sorted(areas_left.items(), key=lambda x: -x[1])):
            if i >= 8:
                break
            show.append(f"{k}: {v}")
        parts.append("、".join(show))
    return "\n".join(parts)


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


# ========= 健康檢查 =========
@app.get("/healthz")
def healthz():
    return PlainTextResponse("ok", status_code=200)


# ========= Cron（Cloud Scheduler 打）=========
@app.get("/cron/tick")
@app.post("/cron/tick")
def cron_tick(key: Optional[str] = Query(None), x_cron_key: Optional[str] = Header(None, alias="X-Cron-Key")):
    if CRON_KEY:
        provided = (key or x_cron_key or "").strip()
        if provided != CRON_KEY:
            raise HTTPException(status_code=403, detail="Forbidden")
    total = run_tick()
    return JSONResponse({"ok": True, "checked": total})


def run_tick() -> int:
    """
    檢查所有 is_active 的任務；若已到檢查間隔就抓頁解析。
    當偵測到有票且 snapshot 與上次不同時，推播通知。
    """
    tasks = all_active_tasks()
    now = _now_ts()
    checked = 0
    for t in tasks:
        try:
            tid = t.get("tid")
            url = t.get("url", "")
            interval = int(t.get("interval_sec", DEFAULT_INTERVAL))
            last_checked = int(t.get("last_checked", 0))

            # 尚未到時間就跳過
            if last_checked and now - last_checked < max(5, interval):
                continue

            checked += 1
            # 先更新 last_checked，避免重複打爆
            touch_last_checked(tid)

            html = fetch_html(url)
            snapshot, has_ticket, areas_left = extract_snapshot_and_ticket(html)

            if has_ticket:
                prev = (t.get("last_snapshot") or "")
                # 僅在內容變化時推播，避免重複通知
                if normalize_text(snapshot) != normalize_text(prev):
                    msg = compose_ticket_message(url, areas_left, snapshot)
                    push(t.get("user_id"), msg)
                    update_after_check(tid, snapshot)
                else:
                    # 沒變化也記錄一次
                    update_after_check(tid, prev or snapshot)
            else:
                # 沒票就僅更新 last_checked
                update_after_check(tid, t.get("last_snapshot", ""))

            # 小睡一下，避免連續請求太密集
            time.sleep(0.2 + random.uniform(0, 0.2))
        except Exception as e:
            logger.exception(f"[cron] check task error: tid={t.get('tid')} url={t.get('url')} err={e}")
            # 失敗也不要瘋狂重試：touch 一次 last_checked
            try:
                touch_last_checked(t.get("tid"))
            except Exception:
                pass
    return checked


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

        # /list
        if text.startswith("/list"):
            if not user_id:
                reply(event, "請在與機器人「1 對 1」聊天中使用 /list 指令。")
                return
            tasks = list_tasks(user_id)
            if not tasks:
                reply(event, "你的監看列表是空的。\n使用 /watch <URL> [秒數] 開始監看（僅支援 https://ticket.ibon.com.tw/）。")
                return

            lines = ["🧾 你的監看任務："]
            for t in tasks:
                mark = "✅" if t.get("is_active") else "⏸️"
                lines.append(
                    f"{mark} {t.get('tid')} | 每 {t.get('interval_sec')} 秒 | "
                    f"{t.get('url')}\n    上次檢查：{fmt_ts(t.get('last_checked', 0))}"
                )
            reply(event, "\n".join(lines))
            return

        # /stop <tid>
        if text.startswith("/stop"):
            parts = text.split()
            if len(parts) < 2:
                reply(event, "用法：/stop <任務ID>")
                return
            if not user_id:
                reply(event, "請在與機器人「1 對 1」聊天中使用 /stop 指令。")
                return
            tid = parts[1].strip()
            ok = deactivate_task(user_id, tid)
            reply(event, "已停止該任務。" if ok else "找不到任務 ID 或你沒有權限。")
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

            url = parts[1].strip()
            interval = int(parts[2]) if len(parts) >= 3 and parts[2].isdigit() else DEFAULT_INTERVAL

            if not is_allowed_url(url):
                reply(event, "目前僅支援 ibon 連結，請貼上 https://ticket.ibon.com.tw/ 開頭的網址喔！")
                return

            # 避免重複任務
            existed = find_active_task_by_url(user_id, url)
            if existed:
                reply(event, f"你已經在監看此連結（任務 {existed['tid']}，每 {existed['interval_sec']} 秒）。")
                return

            tid = add_task(user_id, url, interval)
            reply(event, f"已開始監看 ✅\n任務ID：{tid}\n每 {interval} 秒檢查一次\nURL：{url}")
            return

        # 直接貼 ibon 連結（當作 /watch）
        if text.startswith("http"):
            if not user_id:
                reply(event, "請在與機器人「1 對 1」聊天中使用。")
                return
            if is_allowed_url(text):
                existed = find_active_task_by_url(user_id, text)
                if existed:
                    reply(event, f"你已經在監看此連結（任務 {existed['tid']}，每 {existed['interval_sec']} 秒）。")
                else:
                    tid = add_task(user_id, text, DEFAULT_INTERVAL)
                    reply(event, f"已開始監看 ✅\n任務ID：{tid}\n每 {DEFAULT_INTERVAL} 秒檢查一次\nURL：{text}")
            else:
                reply(event, "目前僅支援 ibon 連結，請貼上 https://ticket.ibon.com.tw/ 開頭的網址喔！")
            return

        # 其他文字 => 顯示說明
        reply(event, USAGE)

    except Exception as e:
        logger.exception(f"[event] handle error: {e}")
        reply(event, "抱歉，處理你的請求時發生錯誤。你可以再試一次或輸入 /help 查看指令。")