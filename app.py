# app.py
# ------------------------------------------------------------
# Flask + Firestore + Cloud Scheduler + LINE Bot
# 票券監看：支援 ibon 活動頁與 orders 內頁，回傳各區剩餘/合計
#
# 需要的環境變數：
# - LINE_CHANNEL_ACCESS_TOKEN（或 LINE_TOKEN）
# - LINE_CHANNEL_SECRET（或 LINE_SECRET，可留空：跳過驗簽）
# - DEFAULT_PERIOD_SEC（預設 60）
# - MAX_TASKS_PER_TICK（預設 25）
# - USE_PLAYWRIGHT=1（可選，啟用備援解析）
# ------------------------------------------------------------
import base64
import hashlib
import hmac
import json
import logging
import os
import re
import secrets
import time
from typing import Dict, Tuple, Optional
from urllib.parse import urlparse, urljoin

import requests
from bs4 import BeautifulSoup
from flask import Flask, abort, jsonify, request
from google.cloud import firestore

# -------------------- 基本設定 --------------------
app = Flask(__name__)
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))

# Firestore（使用預設 Application Default Credentials）
db = firestore.Client()

LINE_TOKEN = (
    os.environ.get("LINE_CHANNEL_ACCESS_TOKEN")
    or os.environ.get("LINE_TOKEN")
)
LINE_SECRET = (
    os.environ.get("LINE_CHANNEL_SECRET")
    or os.environ.get("LINE_SECRET")
)

DEFAULT_PERIOD_SEC = int(os.environ.get("DEFAULT_PERIOD_SEC", "60"))  # Cloud Scheduler 最小 60 秒
MAX_TASKS_PER_TICK = int(os.environ.get("MAX_TASKS_PER_TICK", "25"))

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0 Safari/537.36"
)

# 文字偵測規則
_RE_QTY = re.compile(r"(剩餘|尚餘|尚有|可售|餘票|名額)[^\d]{0,5}(\d+)", re.I)
_RE_SOLDOUT = re.compile(r"(售罄|完售|無票|已售完|暫無|暫時無|售完)", re.I)


# -------------------- LINE 基本函式 --------------------
def _line_reply(reply_token: str, text: str) -> None:
    if not LINE_TOKEN or not reply_token:
        app.logger.warning("No LINE_TOKEN or reply_token; skip reply")
        return
    url = "https://api.line.me/v2/bot/message/reply"
    headers = {"Authorization": f"Bearer {LINE_TOKEN}", "Content-Type": "application/json"}
    body = {"replyToken": reply_token, "messages": [{"type": "text", "text": text[:4000]}]}
    try:
        r = requests.post(url, headers=headers, json=body, timeout=10)
        r.raise_for_status()
    except Exception as e:
        app.logger.exception(f"LINE reply failed: {e}")


def _line_push(to: str, text: str) -> None:
    if not LINE_TOKEN or not to:
        app.logger.warning("No LINE_TOKEN or target; skip push")
        return
    url = "https://api.line.me/v2/bot/message/push"
    headers = {"Authorization": f"Bearer {LINE_TOKEN}", "Content-Type": "application/json"}
    body = {"to": to, "messages": [{"type": "text", "text": text[:4000]}]}
    try:
        r = requests.post(url, headers=headers, json=body, timeout=10)
        r.raise_for_status()
    except Exception as e:
        app.logger.exception(f"LINE push failed: {e}")


def _verify_line_signature(raw_body: bytes) -> bool:
    """有設 LINE_SECRET 就驗簽；沒設就跳過（開發用）"""
    if not LINE_SECRET:
        return True
    sig = request.headers.get("X-Line-Signature", "")
    digest = hmac.new(LINE_SECRET.encode("utf-8"), raw_body, hashlib.sha256).digest()
    expected = base64.b64encode(digest).decode("utf-8")
    return hmac.compare_digest(sig, expected)


# -------------------- ibon 解析 --------------------
def _req_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": UA,
        "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
        "Cache-Control": "no-cache",
    })
    return s


def resolve_ibon_orders_url(any_url: str) -> Optional[str]:
    """
    解析出真正的 orders.ibon 下單頁（UTK0201_xxx.aspx）連結。
    1) 如果已經是 orders 內頁就直接回傳。
    2) 如果是 ticket.ibon 活動頁，從頁面找「立即訂購」按鈕連結。
    """
    u = urlparse(any_url)
    if "orders.ibon.com.tw" in u.netloc and "UTK0201" in u.path.upper():
        return any_url

    if "ticket.ibon.com.tw" in u.netloc:
        s = _req_session()
        r = s.get(any_url, timeout=15)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        a = soup.select_one('a[href*="orders.ibon.com.tw"][href*="UTK02"][href*="UTK0201"]')
        if a and a.get("href"):
            return urljoin(any_url, a["href"])
        # 備援：有些會放在 data-url 或 onclick
        for tag in soup.find_all(["a", "button"]):
            href = (tag.get("href") or tag.get("data-url") or "").strip()
            if "orders.ibon.com.tw" in href and "UTK0201" in href.upper():
                return urljoin(any_url, href)
    return None


def parse_ibon_orders_static(html: str) -> Dict:
    """
    從 UTK0201 頁面的 HTML 直接抓各區「剩餘xx」字樣。
    回傳: {"sections": {"A區": 12, ...}, "total": 12, "soldout": bool}
    """
    soup = BeautifulSoup(html, "html.parser")
    sections: Dict[str, int] = {}
    total = 0
    soldout_hint = False

    candidates = soup.select("section, div, li, tr, p, span")
    for node in candidates:
        txt = node.get_text(" ", strip=True)
        if not txt:
            continue
        if _RE_SOLDOUT.search(txt):
            soldout_hint = True
        m = _RE_QTY.search(txt)
        if m:
            qty = int(m.group(2))
            # 嘗試取區名（範例：A區、B看台、內野等）
            raw = _RE_QTY.sub("", txt)
            raw = re.sub(r"\s+", " ", raw)
            name_match = re.findall(r"([\u4e00-\u9fa5A-Za-z0-9]{1,12}(區|看台|內野|外野|座|樓|層|排)?)", raw)
            key = name_match[0][0] if name_match else "未命名區"
            sections[key] = sections.get(key, 0) + qty
            total += qty

    if total == 0 and soldout_hint:
        return {"sections": {}, "total": 0, "soldout": True}
    return {"sections": sections, "total": total, "soldout": total == 0 and not sections}


def check_ibon(any_url: str) -> Tuple[bool, str, str]:
    """
    回傳: (ok, message, signature)
     - ok: 是否成功解析
     - message: 要推播的內容
     - signature: 根據結果產生的 md5，用來判斷是否變化
    """
    orders_url = resolve_ibon_orders_url(any_url)
    if not orders_url:
        return False, "找不到 ibon 下單頁（UTK0201）。可能尚未開賣或按鈕未顯示。", "NA"

    s = _req_session()
    r = s.get(orders_url, timeout=15)
    r.raise_for_status()

    info = parse_ibon_orders_static(r.text)
    if info["total"] > 0:
        parts = [f"{k}: {v} 張" for k, v in sorted(info["sections"].items(), key=lambda kv: (-kv[1], kv[0]))]
        msg = "✅ 監看結果：目前可售\n" + "\n".join(parts) + f"\n合計：{info['total']} 張\n{orders_url}"
        sig = hashlib.md5(("|" + "|".join(parts)).encode()).hexdigest()
        return True, msg, sig

    if info.get("soldout"):
        msg = f"目前顯示完售/無票\n{orders_url}"
        sig = hashlib.md5("soldout".encode()).hexdigest()
        return True, msg, sig

    # 靜態抓不到 → 可選擇啟用 Playwright 備援
    if os.getenv("USE_PLAYWRIGHT") == "1":
        try:
            ok, msg, sig = check_ibon_playwright(any_url)
            return ok, msg, sig
        except Exception as e:
            app.logger.exception(f"Playwright 解析失敗: {e}")

    return False, "暫時讀不到剩餘數（可能為動態載入）。", "NA"


def check_ibon_playwright(any_url: str) -> Tuple[bool, str, str]:
    """
    備援：用 Playwright 攔截 JSON/XHR 來推估各區剩餘。
    只有在設置 USE_PLAYWRIGHT=1 時才會被呼叫。
    """
    # 延遲匯入，避免未安裝 playwright 造成啟動失敗
    from playwright.sync_api import sync_playwright  # type: ignore

    orders_url = resolve_ibon_orders_url(any_url)
    if not orders_url:
        return False, "找不到 ibon 下單頁（UTK0201）。", "NA"

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--no-sandbox"])
        page = browser.new_page(user_agent=UA, locale="zh-TW")
        bucket = []

        def handle_response(resp):
            try:
                ctype = (resp.headers or {}).get("content-type", "")
            except Exception:
                ctype = ""
            if "application/json" in ctype:
                try:
                    data = resp.json()
                except Exception:
                    return
                txt = json.dumps(data, ensure_ascii=False)
                if re.search(r"(Remain|Remaining|Available|可售|餘|剩)", txt):
                    bucket.append(data)

        page.on("response", handle_response)
        page.goto(orders_url, wait_until="networkidle", timeout=30000)
        page.wait_for_timeout(2000)
        browser.close()

    if not bucket:
        return False, "動態載入也未取得剩餘 JSON。", "NA"

    sections: Dict[str, int] = {}
    total = 0

    def walk(node):
        nonlocal total
        if isinstance(node, dict):
            name = (node.get("AreaName") or node.get("Section")
                    or node.get("Zone") or node.get("Name"))
            qty = (node.get("Remain") or node.get("Remaining")
                   or node.get("Available") or node.get("Qty"))
            if name and isinstance(qty, (int, float)):
                sections[str(name)] = sections.get(str(name), 0) + int(qty)
                total += int(qty)
            for v in node.values():
                walk(v)
        elif isinstance(node, list):
            for v in node:
                walk(v)

    for item in bucket:
        walk(item)

    if total > 0:
        parts = [f"{k}: {v} 張" for k, v in sorted(sections.items(), key=lambda kv: (-kv[1], kv[0]))]
        msg = "✅ 監看結果：目前可售\n" + "\n".join(parts) + f"\n合計：{total} 張\n{orders_url}"
        sig = hashlib.md5(("|" + "|".join(parts)).encode()).hexdigest()
        return True, msg, sig
    else:
        return True, f"目前顯示完售/無票\n{orders_url}", hashlib.md5("soldout".encode()).hexdigest()


# -------------------- LINE Webhook --------------------
@app.post("/webhook")
def webhook():
    raw = request.get_data()
    if not _verify_line_signature(raw):
        abort(400, "Invalid LINE signature")

    body = request.get_json(silent=True) or {}
    events = body.get("events", [])
    for ev in events:
        etype = ev.get("type")
        if etype != "message":
            continue
        msg = ev.get("message", {})
        if msg.get("type") != "text":
            continue

        text: str = (msg.get("text") or "").strip()
        reply_token = ev.get("replyToken")
        source = ev.get("source", {})  # 可能有 userId / groupId / roomId

        if text.lower().startswith("/watch"):
            # 用法：/watch <URL> [秒]（最小 60 秒）
            parts = text.split()
            if len(parts) < 2:
                _line_reply(reply_token, "用法：/watch <票券網址>\n可貼活動頁或 orders 內頁")
                continue
            url = parts[1]
            period = DEFAULT_PERIOD_SEC
            if len(parts) >= 3:
                try:
                    p = int(parts[2])
                    period = max(60, min(3600, p))  # 60~3600s
                except Exception:
                    pass

            target_type = "user" if source.get("userId") else ("group" if source.get("groupId") else "room")
            target_id = source.get("userId") or source.get("groupId") or source.get("roomId")

            task_id = secrets.token_urlsafe(4)
            now = int(time.time())
            db.collection("watches").document(task_id).set({
                "url": url,
                "targetType": target_type,
                "targetId": target_id,
                "periodSec": period,
                "nextCheckAt": now,     # 立刻 due
                "lastSig": None,
                "active": True,
                "createdAt": now,
            })
            _line_reply(
                reply_token,
                f"已開始監看 ✅\n任務ID：{task_id}\n每 {period} 秒檢查一次\nURL：{url}"
            )
            continue

        if text.lower().startswith("/unwatch"):
            parts = text.split()
            if len(parts) < 2:
                _line_reply(reply_token, "用法：/unwatch <任務ID>")
                continue
            tid = parts[1]
            doc = db.collection("watches").document(tid)
            if doc.get().exists:
                doc.update({"active": False})
                _line_reply(reply_token, f"任務 {tid} 已停用")
            else:
                _line_reply(reply_token, f"找不到任務 {tid}")
            continue

        if text.lower().startswith("/list"):
            # 列出這位使用者/群組的前 10 個任務
            target_id = source.get("userId") or source.get("groupId") or source.get("roomId")
            q = (db.collection("watches")
                 .where("targetId", "==", target_id)
                 .order_by("createdAt", direction=firestore.Query.DESCENDING)
                 .limit(10))
            docs = list(q.stream())
            if not docs:
                _line_reply(reply_token, "目前沒有任務")
            else:
                lines = []
                for d in docs:
                    x = d.to_dict()
                    flag = "啟用" if x.get("active") else "停用"
                    lines.append(f"{d.id}｜{flag}｜{x.get('periodSec', 60)}s\n{x.get('url')}")
                _line_reply(reply_token, "你的任務：\n" + "\n\n".join(lines))
            continue

        # 其它訊息 → 提示用法
        _line_reply(reply_token, "指令：\n/watch <URL> [秒]\n/unwatch <任務ID>\n/list")

    return "OK"


# -------------------- Cloud Scheduler 入口（熱修版：永遠回 200） --------------------
@app.get("/cron/tick")
def cron_tick():
    now = int(time.time())
    q = (db.collection("watches")
         .where("active", "==", True)
         .where("nextCheckAt", "<=", now)
         .limit(MAX_TASKS_PER_TICK))
    try:
        docs = list(q.stream())
    except Exception as e:
        app.logger.exception(f"[tick] Firestore query failed: {e}")
        return jsonify({"ok": False, "stage": "query", "error": str(e)}), 200

    processed, errors = 0, []
    for d in docs:
        task = d.to_dict()
        try:
            ok, msg, sig = check_ibon(task["url"])
            if ok and sig != task.get("lastSig"):
                _line_push(task["targetId"], msg)
                d.reference.update({"lastSig": sig})
        except Exception as e:
            app.logger.exception(f"[tick] task {d.id} failed: {e}")
            errors.append(f"{d.id}:{type(e).__name__}")
        finally:
            period = int(task.get("periodSec", DEFAULT_PERIOD_SEC))
            d.reference.update({"nextCheckAt": now + max(60, period)})
            processed += 1

    return jsonify({"ok": True, "processed": processed, "due": len(docs), "errors": errors, "ts": now}), 200


# 健康檢查
@app.get("/healthz")
def healthz():
    return "ok", 200


# 本地啟動
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8080")))