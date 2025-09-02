# app.py — ibon 票券監看（含 /check 手動查詢、/list 過濾、/watch 去重）
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
from urllib.parse import urlparse, urljoin, parse_qsl, urlencode

import requests
from bs4 import BeautifulSoup
from flask import Flask, jsonify, request
from google.cloud import firestore

app = Flask(__name__)
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))

# ---------- 基礎設定 ----------
db = firestore.Client()
LINE_TOKEN = os.environ.get("LINE_CHANNEL_ACCESS_TOKEN") or os.environ.get("LINE_TOKEN")
LINE_SECRET = os.environ.get("LINE_CHANNEL_SECRET") or os.environ.get("LINE_SECRET")

# 相容 DEFAULT_INTERVAL 舊名稱
DEFAULT_PERIOD_SEC = int(
    (os.environ.get("DEFAULT_PERIOD_SEC") or os.environ.get("DEFAULT_INTERVAL") or "60")
)
MAX_TASKS_PER_TICK = int(os.environ.get("MAX_TASKS_PER_TICK", "25"))

PROJECT_ID = os.environ.get("GOOGLE_CLOUD_PROJECT")
TASKS_QUEUE = os.environ.get("TASKS_QUEUE", "tick-queue")
TASKS_LOCATION = os.environ.get("TASKS_LOCATION", "asia-east1")
TASKS_SERVICE_ACCOUNT = os.environ.get("TASKS_SERVICE_ACCOUNT", "")
TASKS_TARGET_URL = os.environ.get("TASKS_TARGET_URL", "")
TICK_FANOUT = os.environ.get("TICK_FANOUT", "1") == "1"

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0 Safari/537.36"
)

# 文字偵測規則
_RE_QTY = re.compile(r"(空位|剩餘|尚餘|尚有|可售|餘票|名額)[^\d]{0,5}(\d+)", re.I)
_RE_SOLDOUT = re.compile(r"(售罄|完售|無票|已售完|暫無|暫時無|售完|已售空)", re.I)

HELP_TEXT = (
    "我是票券監看機器人 👋\n"
    "指令：\n"
    "/start 或 /help － 顯示這個說明\n"
    "/watch <URL> [秒] － 開始監看（最小 15 秒；同網址不可重複建立，帶新秒數會更新既有任務）\n"
    "/unwatch <任務ID> － 停用任務\n"
    "/list － 顯示啟用中任務（/list all 看全部；/list off 只看停用）\n"
    "/check <URL> － 立刻手動查詢該頁剩餘票數\n"
    "/checkid <任務ID> － 立刻手動查詢該任務的 URL\n"
    "也可直接輸入「查詢」或 /check 不帶參數，會查你最新一筆啟用中的任務"
)

# ---------- LINE 基本函式 ----------
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

def _line_reply_rich(reply_token: str, text: str, image_url: Optional[str] = None) -> None:
    if not LINE_TOKEN or not reply_token:
        app.logger.warning("No LINE_TOKEN or reply_token; skip reply")
        return
    url = "https://api.line.me/v2/bot/message/reply"
    headers = {"Authorization": f"Bearer {LINE_TOKEN}", "Content-Type": "application/json"}
    messages = []
    if image_url:
        messages.append({"type": "image","originalContentUrl": image_url,"previewImageUrl": image_url})
    messages.append({"type": "text", "text": text[:4000]})
    try:
        r = requests.post(url, headers=headers, json={"replyToken": reply_token, "messages": messages}, timeout=10)
        r.raise_for_status()
    except Exception as e:
        app.logger.exception(f"LINE reply (rich) failed: {e}")

def _line_push_rich(to: str, text: str, image_url: Optional[str] = None) -> None:
    if not LINE_TOKEN or not to:
        app.logger.warning("No LINE_TOKEN or target; skip push")
        return
    url = "https://api.line.me/v2/bot/message/push"
    headers = {"Authorization": f"Bearer {LINE_TOKEN}", "Content-Type": "application/json"}
    messages = []
    if image_url:
        messages.append({"type": "image","originalContentUrl": image_url,"previewImageUrl": image_url})
    messages.append({"type": "text", "text": text[:4000]})
    try:
        r = requests.post(url, headers=headers, json={"to": to, "messages": messages}, timeout=10)
        r.raise_for_status()
    except Exception as e:
        app.logger.exception(f"LINE push (rich) failed: {e}")

def _verify_line_signature(raw_body: bytes) -> bool:
    if not LINE_SECRET:
        return True
    sig = request.headers.get("X-Line-Signature", "")
    digest = hmac.new(LINE_SECRET.encode("utf-8"), raw_body, hashlib.sha256).digest()
    expected = base64.b64encode(digest).decode("utf-8")
    return hmac.compare_digest(sig, expected)

# ---------- ibon 解析與網址正規化 ----------
def _req_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": UA, "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8", "Cache-Control": "no-cache"})
    return s

def resolve_ibon_orders_url(any_url: str) -> Optional[str]:
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
        for tag in soup.find_all(["a", "button"]):
            href = (tag.get("href") or tag.get("data-url") or "").strip()
            if "orders.ibon.com.tw" in href and "UTK0201" in href.upper():
                return urljoin(any_url, href)
    return None

def canonicalize_ibon_url(any_url: str) -> str:
    """將 ibon 網址正規化為唯一 key：僅保留 UTK0201 訂購頁 + PERFORMANCE_ID/PRODUCT_ID"""
    url = resolve_ibon_orders_url(any_url) or any_url
    p = urlparse(url)
    host = p.netloc.lower()
    path = re.sub(r"/+", "/", p.path)
    pairs = parse_qsl(p.query, keep_blank_values=True)

    # ibon 訂購頁：只留關鍵參數
    keep = {"PERFORMANCE_ID", "PRODUCT_ID"}
    skip = {"STRITM", "UTM_SOURCE", "UTM_MEDIUM", "UTM_CAMPAIGN", "UTM_ID", "UTM_TERM", "UTM_CONTENT", "REF"}

    kept = []
    if "ibon.com.tw" in host and "UTK02" in path.upper():
        for k, v in pairs:
            ku = k.upper()
            if ku in keep:
                kept.append((ku, v.strip()))
        kept.sort()
        q = "&".join(f"{k}={v}" for k, v in kept)
    else:
        # 一般情況：排序 + 去除追蹤參數
        for k, v in pairs:
            if k.upper() in skip:
                continue
            kept.append((k, v))
        kept.sort()
        q = urlencode(kept, doseq=True)

    canon = f"https://{host}{path}"
    if q:
        canon += "?" + q
    return canon

def _first_text(*cands: Optional[str]) -> str:
    for c in cands:
        if c and str(c).strip():
            return str(c).strip()
    return ""

def _extract_activity_meta(soup: BeautifulSoup) -> Dict[str, str]:
    def find_label(labels):
        pat = re.compile("|".join(map(re.escape, labels)))
        for node in soup.find_all(text=pat):
            tag = node.parent
            if tag and tag.name in ("td", "th"):
                cells = [c.get_text(" ", strip=True) for c in tag.parent.find_all(["td", "th"])]
                for i, val in enumerate(cells):
                    if re.search(pat, val) and i + 1 < len(cells):
                        return cells[i + 1]
            text = tag.get_text(" ", strip=True) if tag else ""
            if re.search(pat, text):
                cleaned = pat.sub("", text).replace("：", "").strip()
                if cleaned:
                    return cleaned
            sib = tag.find_next_sibling() if tag else None
            if sib:
                t = sib.get_text(" ", strip=True)
                if t:
                    return t
        return ""
    title = _first_text(
        find_label(["活動名稱"]),
        soup.select_one('meta[property="og:title"]') and soup.select_one('meta[property="og:title"]').get("content"),
    )
    dt = _first_text(find_label(["活動時間", "活動日期"]))
    venue = _first_text(find_label(["活動地點", "地點"]))
    image = _first_text(
        soup.select_one('meta[property="og:image"]') and soup.select_one('meta[property="og:image"]').get("content"),
    )
    if not image:
        img = soup.find("img", src=re.compile(r"ActivityImage|azureedge|image/ActivityImage", re.I))
        if img and img.get("src"):
            image = urljoin("https://orders.ibon.com.tw/", img.get("src"))
    return {"title": title, "datetime": dt, "venue": venue, "image_url": image}

def parse_ibon_orders_static(html: str) -> Dict:
    soup = BeautifulSoup(html, "html.parser")
    sections: Dict[str, int] = {}
    total = 0
    soldout_hint = False

    # A) 表格解析
    for table in soup.find_all("table"):
        header_tr = None
        for tr in table.find_all("tr", recursive=True):
            if tr.find("th"):
                heads = [c.get_text(" ", strip=True) for c in tr.find_all(["th", "td"])]
                if any(h for h in heads if ("空位" in h or "剩餘" in h or "可售" in h)):
                    header_tr = tr
                    break
        if not header_tr:
            continue
        heads = [c.get_text(" ", strip=True) for c in header_tr.find_all(["th", "td"])]
        try:
            idx_qty = next(i for i, h in enumerate(heads) if ("空位" in h or "剩餘" in h or "可售" in h))
        except StopIteration:
            continue
        try:
            idx_area = next(i for i, h in enumerate(heads) if ("票區" in h or h == "區" or "座位區" in h))
        except StopIteration:
            idx_area = 1 if len(heads) > 1 else 0

        tr = header_tr.find_next_sibling("tr")
        while tr and tr.name == "tr":
            tds = tr.find_all("td")
            if len(tds) > max(idx_qty, idx_area):
                area = tds[idx_area].get_text(" ", strip=True)
                qty_text = tds[idx_qty].get_text(" ", strip=True)
                if _RE_SOLDOUT.search(qty_text):
                    soldout_hint = True
                m = re.search(r"(\d+)", qty_text)
                if m:
                    qty = int(m.group(1))
                    if qty > 0:
                        area = re.sub(r"\s+", "", area) or "未命名區"
                        sections[area] = sections.get(area, 0) + qty
                        total += qty
            tr = tr.find_next_sibling("tr")
        if total > 0:
            break

    # B) 關鍵字掃描
    if total == 0:
        candidates = soup.select("tr, li, div, p, span")
        for node in candidates:
            txt = node.get_text(" ", strip=True)
            if not txt:
                continue
            if _RE_SOLDOUT.search(txt):
                soldout_hint = True
            m = _RE_QTY.search(txt)
            if not m:
                continue
            qty = int(m.group(2))
            if qty <= 0:
                continue
            key = "未命名區"
            tr = node if node.name == "tr" else node.find_parent("tr")
            if tr:
                cells = [c.get_text(" ", strip=True) for c in tr.find_all(["td", "th"])]
                for c in cells:
                    if re.search(r"(區|看台|內野|外野|座|樓|層)", c):
                        key = re.sub(r"\s+", "", c)
                        break
            sections[key] = sections.get(key, 0) + qty
            total += qty

    return {"sections": sections, "total": total, "soldout": (total == 0 and soldout_hint), "soup": soup}

def check_ibon(any_url: str) -> Tuple[bool, str, str, Dict[str, str]]:
    orders_url = resolve_ibon_orders_url(any_url)
    if not orders_url:
        return False, "找不到 ibon 下單頁（UTK0201）。可能尚未開賣或按鈕未顯示。", "NA", {}
    s = _req_session()
    r = s.get(orders_url, timeout=15)
    r.raise_for_status()

    parsed = parse_ibon_orders_static(r.text)
    meta = _extract_activity_meta(parsed["soup"])
    title = meta.get("title") or "活動資訊"
    venue = meta.get("venue")
    dt = meta.get("datetime")
    img = meta.get("image_url")

    prefix_lines = [f"🎫 {title}"]
    if venue: prefix_lines.append(f"地點：{venue}")
    if dt: prefix_lines.append(f"日期：{dt}")
    prefix = "\n".join(prefix_lines) + "\n\n"

    if parsed["total"] > 0:
        parts = [f"{k}: {v} 張" for k, v in sorted(parsed["sections"].items(), key=lambda kv: (-kv[1], kv[0]))]
        msg = prefix + "✅ 監看結果：目前可售\n" + "\n".join(parts) + f"\n合計：{parsed['total']} 張\n{orders_url}"
        sig = hashlib.md5(("|" + "|".join(parts)).encode()).hexdigest()
        return True, msg, sig, {"image_url": img}
    if parsed.get("soldout"):
        msg = prefix + f"目前顯示完售/無票\n{orders_url}"
        sig = hashlib.md5("soldout".encode()).hexdigest()
        return True, msg, sig, {"image_url": img}

    return False, prefix + "暫時讀不到剩餘數（可能為動態載入）。\n" + orders_url, "NA", {"image_url": img}

# ---------- Webhook ----------
def _get_target_id(src: dict) -> str:
    return src.get("userId") or src.get("groupId") or src.get("roomId") or ""

@app.post("/webhook")
def webhook():
    raw = request.get_data()
    if not _verify_line_signature(raw):
        app.logger.error("Invalid LINE signature")
        return "bad signature", 400

    body = request.get_json(silent=True) or {}
    events = body.get("events", [])

    for ev in events:
        etype = ev.get("type")
        if etype in ("follow", "join"):
            _line_reply(ev.get("replyToken"), HELP_TEXT); continue
        if etype != "message": continue

        msg = ev.get("message", {})
        if msg.get("type") != "text": continue

        text = (msg.get("text") or "").strip()
        reply_token = ev.get("replyToken")
        src = ev.get("source", {})
        low = text.lower()

        if low in ("/start", "start", "/help", "help", "？"):
            _line_reply(reply_token, HELP_TEXT); continue

        # ---- 手動查詢 ----
        if low.startswith("/checkid"):
            parts = text.split()
            if len(parts) < 2:
                _line_reply(reply_token, "用法：/checkid <任務ID>"); continue
            tid = parts[1].strip()
            docref = db.collection("watches").document(tid)
            doc = docref.get()
            if not doc.exists:
                _line_reply(reply_token, f"找不到任務 {tid}"); continue
            task = doc.to_dict()
            ok, msg_out, _sig, meta = check_ibon(task.get("url", ""))
            _line_reply_rich(reply_token, msg_out, (meta or {}).get("image_url")); continue

        if low.startswith("/check") or text == "查詢":
            parts = text.split()
            url = None
            if len(parts) >= 2 and parts[1].startswith("http"):
                url = parts[1]
            if not url:
                target_id = _get_target_id(src)
                q = (db.collection("watches")
                     .where("targetId", "==", target_id)
                     .where("active", "==", True)
                     .order_by("createdAt", direction=firestore.Query.DESCENDING)
                     .limit(1))
                docs = list(q.stream())
                if docs: url = docs[0].to_dict().get("url")
            if not url:
                _line_reply(reply_token, "用法：/check <票券網址>\n或先用 /watch 建立任務後輸入「查詢」"); continue
            ok, msg_out, _sig, meta = check_ibon(url)
            _line_reply_rich(reply_token, msg_out, (meta or {}).get("image_url")); continue
        # -------------------

        # ---- watch：加入去重/復用/更新 period ----
        if low.startswith("/watch"):
            parts = text.split()
            if len(parts) < 2:
                _line_reply(reply_token, "用法：/watch <票券網址> [秒]\n可貼活動頁或 orders 內頁"); continue

            raw_url = parts[1]
            period = DEFAULT_PERIOD_SEC
            if len(parts) >= 3:
                try:
                    p = int(parts[2]); period = max(15, min(3600, p))
                except Exception: pass

            target_id = _get_target_id(src)
            target_type = "user" if src.get("userId") else ("group" if src.get("groupId") else "room")
            url_canon = canonicalize_ibon_url(raw_url)

            # 1) 嘗試用 urlCanon 精準查詢（若需要索引會自動降級）
            existing = None
            try:
                q0 = (db.collection("watches")
                      .where("targetId", "==", target_id)
                      .where("urlCanon", "==", url_canon)
                      .limit(1))
                docs0 = list(q0.stream())
                existing = docs0[0] if docs0 else None
            except Exception as e:
                app.logger.warning(f"/watch query by urlCanon failed, fallback scan: {e}")

            # 2) 後備：掃描近期任務比對（避免缺索引）
            if not existing:
                q1 = (db.collection("watches")
                      .where("targetId", "==", target_id)
                      .order_by("createdAt", direction=firestore.Query.DESCENDING)
                      .limit(50))
                for d in q1.stream():
                    x = d.to_dict()
                    cand = x.get("urlCanon") or canonicalize_ibon_url(x.get("url", ""))
                    if cand == url_canon:
                        existing = d; break

            now = int(time.time())
            if existing:
                data = existing.to_dict()
                if data.get("active", True):
                    # 已在監看：更新 period（若有帶新值）
                    if int(data.get("periodSec", DEFAULT_PERIOD_SEC)) != period:
                        existing.reference.update({"periodSec": period, "nextCheckAt": now})
                        _line_reply(reply_token, f"此網址已在監看 ✅\n任務ID：{existing.id}\n已更新為每 {period} 秒檢查\nURL：{data.get('url')}")
                    else:
                        _line_reply(reply_token, f"此網址已在監看 ✅\n任務ID：{existing.id}\n每 {data.get('periodSec', DEFAULT_PERIOD_SEC)} 秒檢查\nURL：{data.get('url')}")
                else:
                    # 存在但停用 → 直接重新啟用
                    existing.reference.update({"active": True, "periodSec": period, "nextCheckAt": now})
                    _line_reply(reply_token, f"已重新啟用 ✅\n任務ID：{existing.id}\n每 {period} 秒檢查\nURL：{data.get('url')}")
                continue

            # 3) 完全新任務 → 建立
            task_id = secrets.token_urlsafe(4)
            db.collection("watches").document(task_id).set({
                "url": raw_url,
                "urlCanon": url_canon,
                "targetType": target_type,
                "targetId": target_id,
                "periodSec": period,
                "nextCheckAt": now,     # 立刻 due
                "lastSig": None,
                "active": True,
                "createdAt": now,
            })
            _line_reply(reply_token, f"已開始監看 ✅\n任務ID：{task_id}\n每 {period} 秒檢查一次\nURL：{raw_url}")
            continue
        # -------------------

        if low.startswith("/unwatch"):
            parts = text.split()
            if len(parts) < 2:
                _line_reply(reply_token, "用法：/unwatch <任務ID>"); continue
            tid = parts[1]
            doc = db.collection("watches").document(tid)
            if doc.get().exists:
                doc.update({"active": False})
                _line_reply(reply_token, f"任務 {tid} 已停用")
            else:
                _line_reply(reply_token, f"找不到任務 {tid}")
            continue

        if low.startswith("/list"):
            mode = "on"  # 預設只列啟用中
            if len(text.split()) >= 2:
                opt = text.split()[1].lower()
                if opt in ("all", "-a", "--all"): mode = "all"
                elif opt in ("off", "--off"): mode = "off"

            target_id = _get_target_id(src)
            q = db.collection("watches").where("targetId", "==", target_id)

            if mode == "on":
                q = q.where("active", "==", True)
            elif mode == "off":
                q = q.where("active", "==", False)

            q = q.order_by("createdAt", direction=firestore.Query.DESCENDING).limit(20)
            docs = list(q.stream())
            if not docs:
                _line_reply(reply_token, "目前沒有符合條件的任務" + ("" if mode=="on" else f"（{mode}）"))
            else:
                lines = []
                for d in docs:
                    x = d.to_dict()
                    flag = "啟用" if x.get("active") else "停用"
                    if mode == "on" and not x.get("active"):  # 保險再過濾一次
                        continue
                    lines.append(f"{d.id}｜{flag}｜{x.get('periodSec', 60)}s\n{x.get('url')}")
                if not lines:
                    _line_reply(reply_token, "目前沒有符合條件的任務")
                else:
                    title = "你的任務：" if mode=="on" else ("你的任務（全部）：" if mode=="all" else "你的任務（停用）：")
                    _line_reply(reply_token, title + "\n" + "\n\n".join(lines))
            continue

        _line_reply(reply_token, HELP_TEXT)

    return "OK"

# ---------- 15 秒扇出：Cloud Tasks ----------
def enqueue_tick_runs(delays=(0, 15, 30, 45)) -> int:
    try:
        from google.cloud import tasks_v2
        from google.protobuf import timestamp_pb2
    except Exception as e:
        app.logger.warning(f"[fanout] cloud-tasks lib missing, run once. {e}")
        return 0

    if not (PROJECT_ID and TASKS_QUEUE and TASKS_LOCATION and TASKS_SERVICE_ACCOUNT and TASKS_TARGET_URL):
        app.logger.warning("[fanout] env incomplete; run once instead.")
        return 0

    client = tasks_v2.CloudTasksClient()
    parent = client.queue_path(PROJECT_ID, TASKS_LOCATION, TASKS_QUEUE)
    created = 0

    for d in delays:
        ts = timestamp_pb2.Timestamp()
        ts.FromSeconds(int(time.time()) + int(d))
        task = {
            "http_request": {
                "http_method": tasks_v2.HttpMethod.GET,
                "url": f"{TASKS_TARGET_URL}/cron/tick?mode=run",
                "headers": {"User-Agent": "Cloud-Tasks", "X-From-Tasks": "1"},
                "oidc_token": {"service_account_email": TASKS_SERVICE_ACCOUNT, "audience": TASKS_TARGET_URL},
            },
            "schedule_time": ts
        }
        try:
            client.create_task(request={"parent": parent, "task": task}); created += 1
        except Exception as e:
            app.logger.exception(f"[fanout] create_task failed (delay={d}): {e}")

    return created

def do_tick():
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
            ok, msg, sig, meta = check_ibon(task["url"])
            if ok and sig != task.get("lastSig"):
                _line_push_rich(task["targetId"], msg, (meta or {}).get("image_url"))
                d.reference.update({"lastSig": sig})
        except Exception as e:
            app.logger.exception(f"[tick] task {d.id} failed: {e}")
            errors.append(f"{d.id}:{type(e).__name__}")
        finally:
            period = int(task.get("periodSec", DEFAULT_PERIOD_SEC))
            d.reference.update({"nextCheckAt": now + max(15, period)})
            processed += 1

    return jsonify({"ok": True, "processed": processed, "due": len(docs), "errors": errors, "ts": now}), 200

@app.get("/cron/tick")
def cron_tick():
    if request.args.get("mode") == "run" or request.headers.get("X-From-Tasks") == "1":
        return do_tick()
    if TICK_FANOUT:
        n = enqueue_tick_runs((0, 15, 30, 45))
        if n > 0:
            return jsonify({"ok": True, "fanout": n}), 200
    return do_tick()

@app.get("/healthz")
def healthz():
    return "ok", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8080")))