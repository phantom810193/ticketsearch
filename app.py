# -*- coding: utf-8 -*-
import os
import re
import json
import time
import uuid
import base64
import hashlib
import logging
import threading
import traceback
import requests
from datetime import datetime, timezone, timedelta
from typing import Dict, Tuple, Optional, Any, List
from urllib.parse import urlparse, urlunparse, parse_qs, urlencode, urljoin, unquote
from flask import (
    Flask,
    jsonify,
    request,
    send_from_directory,
    Blueprint,
    current_app,
)

app = Flask(__name__)
app.url_map.strict_slashes = False

liff_api_bp = Blueprint("liff_api", __name__, url_prefix="/api/liff")
# --- Browser engines (optional) ---
try:
    from playwright.sync_api import sync_playwright
    _PLAYWRIGHT_AVAILABLE = True
except Exception:
    _PLAYWRIGHT_AVAILABLE = False
    def sync_playwright():
        raise RuntimeError("Playwright not available")

try:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.chrome.service import Service as ChromeService
    _SELENIUM_AVAILABLE = True
except Exception:
    _SELENIUM_AVAILABLE = False

# ==== ibon API circuit breaker ====
_IBON_BREAK_OPEN_UNTIL = 0.0  # timestamp，> now 代表 API 暫停使用
_IBON_BREAK_COOLDOWN = int(os.getenv("IBON_API_COOLDOWN_SEC", "300"))  # 500 後冷卻秒數（預設 5 分鐘）
_IBON_API_DISABLED = os.getenv("IBON_API_DISABLE", "0") == "1"  # 緊急開關：1 => 永遠不用 API

def _breaker_open_now() -> bool:
    return (time.time() < _IBON_BREAK_OPEN_UNTIL) or _IBON_API_DISABLED

def _open_breaker():
    global _IBON_BREAK_OPEN_UNTIL
    _IBON_BREAK_OPEN_UNTIL = time.time() + _IBON_BREAK_COOLDOWN

def _sleep_backoff(attempt: int):
    # 0.4s, 0.8s, 1.6s 上限 2s，加一點抖動
    d = min(2.0, 0.4 * (2 ** attempt)) + (0.05 * attempt)
    time.sleep(d)

def _as_list(x):
    return x if isinstance(x, list) else []

IBON_API = "https://ticket.ibon.com.tw/api/ActivityInfo/GetIndexData"
IBON_TOKEN_API = "https://ticket.ibon.com.tw/api/ActivityInfo/GetToken"
IBON_BASE = "https://ticket.ibon.com.tw/"
IBON_HOST = "https://ticket.ibon.com.tw"
# NEW: 首頁輪播 URL 抽成環境變數（可覆寫）
IBON_ENT_URL = os.getenv("IBON_ENT_URL", "https://ticket.ibon.com.tw/Index/entertainment")
UTK_BACKOFF = (0.7, 1.5, 3.0)

# 簡單快取（5 分鐘）
_cache = {"ts": 0, "data": []}
_CACHE_TTL = 300  # 秒

_CONCERT_WORDS = (
    "演唱會",
    "演場會",
    "音樂會",
    "音樂節",
    "音樂祭",
    "演唱",
    "演出",
    "巡演",
    "演奏",
    "見面會",
    "Fan Meeting",
    "LIVE",
    "Live",
)

def _looks_like_concert(title: str) -> bool:
    t = title or ""
    low = t.lower()
    return any(w.lower() in low for w in _CONCERT_WORDS)


def build_ibon_details_url(activity_id: str, pattern: str = "ENTERTAINMENT") -> str:
    aid = "".join(ch for ch in str(activity_id) if ch.isdigit())
    pat = (pattern or "ENTERTAINMENT").strip() or "ENTERTAINMENT"
    return f"{IBON_HOST}/ActivityInfo/Details?{urlencode({'id': aid, 'pattern': pat})}"


def sanitize_details_url(url: str) -> str:
    try:
        parsed = urlparse(url)
    except Exception:
        return build_ibon_details_url(str(url))

    qs = parse_qs(parsed.query)
    aid = "".join(ch for ch in (qs.get("id", [""])[0] or "") if ch.isdigit())
    if not aid:
        m = re.search(r"/ActivityInfo/Details/(\d+)", parsed.path or "")
        if m:
            aid = m.group(1)
    pattern = (qs.get("pattern", ["ENTERTAINMENT"])[0] or "ENTERTAINMENT").strip() or "ENTERTAINMENT"
    if not aid:
        return build_ibon_details_url("", pattern)
    return build_ibon_details_url(aid, pattern)


def _decode_ibon_html(response: requests.Response) -> str:
    response.encoding = response.encoding or getattr(response, "apparent_encoding", None) or "utf-8"
    html = response.text
    if "�" not in html and html.strip():
        return html
    raw = response.content
    try:
        import chardet  # type: ignore

        detected = chardet.detect(raw) or {}
        enc = detected.get("encoding") or "utf-8"
    except Exception:
        enc = "utf-8"
    try:
        return raw.decode(enc, errors="replace")
    except Exception:
        return raw.decode("utf-8", errors="replace")


def _extract_xsrf_token(payload: Any) -> Optional[str]:
    """從 ibon 的 GetToken 結構中提取 XSRF token。"""

    def _collect_tokens(src: Any) -> List[str]:
        out: List[str] = []
        if isinstance(src, dict):
            for key in ("token", "Token", "xsrfToken", "XSRFToken", "XsrfToken", "Xsrf", "XSRF", "Message", "message"):
                val = src.get(key)
                if isinstance(val, str) and val.strip():
                    out.append(val.strip())
            item = src.get("Item") or src.get("item")
            if isinstance(item, dict):
                out.extend(_collect_tokens(item))
            for v in src.values():
                if isinstance(v, dict):
                    out.extend(_collect_tokens(v))
                elif isinstance(v, list):
                    out.extend(_collect_tokens(v))
        elif isinstance(src, list):
            for v in src:
                out.extend(_collect_tokens(v))
        elif isinstance(src, str) and src.strip():
            out.append(src.strip())
        return out

    def _maybe_decode(candidate: str) -> Optional[str]:
        cand = candidate.strip()
        if not cand:
            return None
        if "|" in cand:
            parts = [p.strip() for p in cand.split("|") if p.strip()]
            if parts:
                return parts[-1]
        # Base64 (新版 API Message)
        b64_charset = set("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=_-")
        if all((ch in b64_charset) for ch in cand):
            padded = cand + "=" * ((4 - len(cand) % 4) % 4)
            try:
                decoded = base64.b64decode(padded).decode("utf-8", errors="ignore")
                if decoded:
                    return _maybe_decode(decoded)
            except Exception:
                pass
        return cand

    candidates = _collect_tokens(payload)
    for cand in candidates:
        token = _maybe_decode(cand)
        if not token:
            continue
        token = unquote(token)
        token = token.strip()
        if token:
            return token
    return None


def _prepare_ibon_session() -> Tuple[requests.Session, Optional[str]]:
    """建立與 ibon API 溝通所需的 session 與 XSRF token。"""

    s = requests.Session()
    s.headers.update({
        "User-Agent": UA,
        "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.6",
        "Accept": "application/json, text/plain, */*",
        "Connection": "close",
    })

    try:
        s.get(IBON_ENT_URL, timeout=10)
    except Exception as e:
        app.logger.info(f"[ibon token] warm-up failed: {e}")

    token: Optional[str] = None
    try:
        token_resp = s.post(
            IBON_TOKEN_API,
            headers={
                "Origin": "https://ticket.ibon.com.tw",
                "Referer": IBON_ENT_URL,
                "X-Requested-With": "XMLHttpRequest",
            },
            timeout=10,
        )
        if token_resp.status_code == 200:
            data: Any
            try:
                data = token_resp.json()
            except Exception:
                data = token_resp.text
            token = _extract_xsrf_token(data)
        else:
            app.logger.info(f"[ibon token] http={token_resp.status_code}")
    except Exception as e:
        app.logger.warning(f"[ibon token] err: {e}")

    if not token:
        token = s.cookies.get("XSRF-TOKEN")
        if token:
            token = unquote(token)

    if token:
        domain = urlparse(IBON_BASE).netloc or urlparse(IBON_ENT_URL).netloc
        try:
            s.cookies.set("XSRF-TOKEN", token, domain=domain, path="/")
        except Exception:
            s.cookies.set("XSRF-TOKEN", token)

    return s, token

# -------- HTML 解析 --------
from bs4 import BeautifulSoup

def _normalize_item(row):
    """
    將 ibon API 的活動項目轉為 {title,url,image}
    兼容不同欄位名稱（含 ActivityInfoId / Link 等）。
    """
    # title
    title = (row.get("Title") or row.get("ActivityTitle") or row.get("ActivityName")
             or row.get("GameName") or row.get("Name") or row.get("Subject") or "活動")

    # image
    img = (row.get("ImgUrl") or row.get("ImageUrl") or row.get("Image")
           or row.get("PicUrl") or row.get("PictureUrl")
           or row.get("ActivityImage") or row.get("ActivityImageUrl")
           or row.get("ActivityImageURL") or "").strip() or None

    # url / id
    url = (row.get("GameTicketURL") or row.get("GameTicketUrl")
           or row.get("Url") or row.get("URL") or row.get("LinkUrl")
           or row.get("LinkURL") or row.get("Link") or "").strip()

    act_id = (row.get("ActivityID") or row.get("ActivityId")
              or row.get("ActivityInfoId") or row.get("ActivityInfoID")
              or row.get("GameId") or row.get("GameID")
              or row.get("Id") or row.get("ID"))
    pattern = row.get("Pattern") or row.get("pattern") or row.get("Category") or "ENTERTAINMENT"

    if not url and act_id:
        url = urljoin(IBON_BASE, f"/ActivityInfo/Details?id={act_id}")

    # 統一為絕對網址
    if url and not url.lower().startswith("http"):
        url = urljoin(IBON_BASE, url)

    details_url: Optional[str] = None
    if url and "/ActivityInfo/Details" in url:
        details_url = sanitize_details_url(url)
    else:
        inferred_id = _activity_id_from_url(url) if url else None
        use_id = str(act_id or inferred_id or "").strip()
        if use_id:
            details_url = build_ibon_details_url(use_id, pattern)

    if not details_url and url:
        details_url = sanitize_details_url(url)

    # 有些圖片給相對路徑
    if img and not img.lower().startswith("http"):
        img = urljoin(IBON_BASE, img)

    payload = {
        "title": str(title).strip() or "活動",
        "url": details_url or url,
        "details_url": details_url or url,
        "image": img,
        "image_url": img,
    }
    if act_id:
        payload["activity_id"] = str(act_id)
    if pattern:
        payload["pattern"] = pattern
    return payload


def _activity_id_from_url(url: str) -> Optional[str]:
    try:
        parsed = urlparse(url)
        q = parse_qs(parsed.query)
        for key in ("id", "ID", "activityId", "ActivityId"):
            vals = q.get(key)
            if vals:
                return str(vals[0])
        m = re.search(r"/ActivityInfo/Details/(\d+)", parsed.path or "")
        if m:
            return m.group(1)
    except Exception:
        pass
    return None

# --------- 直接打 API 拉清單 ---------
def fetch_ibon_list_via_api(limit=10, keyword=None, only_concert=False):
    """
    直接打 ibon 官方 API 抽取活動清單（泛抓）。
    有 5xx 時開啟斷路器，讓後續請求優先走 HTML 兜底，避免一直噴 500。
    永遠回傳 list。
    """
    global _cache
    now = time.time()
    if now - _cache["ts"] < _CACHE_TTL and _cache["data"]:
        base_rows = _cache["data"]
    else:
        if _breaker_open_now():
            return []  # 斷路器期間直接跳過 API

        session: Optional[requests.Session] = None
        token: Optional[str] = None
        base_rows: List[Dict[str, Optional[str]]] = []
        seen_urls: set[str] = set()

        patterns = ["ENTERTAINMENT", "CONCERT", "LEISURE"]

        def _extract_lists(data: Any) -> List[Dict[str, Any]]:
            buckets: List[Dict[str, Any]] = []

            def walk(node: Any):
                if isinstance(node, list):
                    for it in node:
                        if isinstance(it, dict):
                            buckets.append(it)
                        walk(it)
                elif isinstance(node, dict):
                    for v in node.values():
                        if isinstance(v, list):
                            walk(v)
                        elif isinstance(v, dict):
                            walk(v)

            if isinstance(data, dict):
                item = data.get("Item") or data.get("item")
                if item is not None:
                    walk(item)
                else:
                    walk(data)
            elif isinstance(data, list):
                walk(data)
            return buckets

        def _append_rows(payload: Any):
            nonlocal base_rows

            container: Any = payload.get("Item") if isinstance(payload, dict) else payload
            candidate_lists: List[List[Dict[str, Any]]] = []

            if isinstance(container, dict):
                for key in ("List", "HotList", "ActivityList"):
                    val = container.get(key)
                    if isinstance(val, list) and val:
                        candidate_lists.append(val)

            if not candidate_lists:
                candidate_lists.append(_extract_lists(container))

            for arr in candidate_lists:
                for raw in arr or []:
                    if not isinstance(raw, dict):
                        continue
                    try:
                        item = _normalize_item(raw)
                    except Exception:
                        continue

                    act_id = (
                        raw.get("ActivityID")
                        or raw.get("ActivityId")
                        or raw.get("ActivityInfoId")
                        or raw.get("GameId")
                        or raw.get("GameID")
                    )
                    if act_id:
                        act_id = str(act_id)
                    url = item.get("url")
                    if not url and act_id:
                        url = urljoin(IBON_BASE, f"/ActivityInfo/Details?id={act_id}")
                        item["url"] = url

                    if not url:
                        continue

                    canon = canonicalize_url(url)
                    if canon in seen_urls:
                        continue

                    seen_urls.add(canon)
                    base_rows.append(item)

        for pattern in patterns:
            for attempt in range(3):
                if session is None:
                    session, token = _prepare_ibon_session()

                if session is None:
                    break

                try:
                    headers = {
                        "Origin": "https://ticket.ibon.com.tw",
                        "Referer": IBON_ENT_URL,
                        "X-Requested-With": "XMLHttpRequest",
                    }
                    if token:
                        headers["X-XSRF-TOKEN"] = token

                    r = session.post(
                        IBON_API,
                        headers=headers,
                        data={"pattern": pattern or ""},
                        timeout=10,
                    )
                    if r.status_code == 200:
                        try:
                            data = r.json()
                        except Exception:
                            data = {}
                        status = data.get("StatusCode") if isinstance(data, dict) else None
                        if status not in (None, 0):
                            app.logger.info(f"[ibon api] status={status} pattern={pattern}")
                        _append_rows(data)
                        break

                    if r.status_code in (401, 403, 419):
                        app.logger.info(f"[ibon api] auth http={r.status_code}, refresh token")
                        session, token = _prepare_ibon_session()
                        continue

                    if 500 <= r.status_code < 600:
                        app.logger.warning(f"[ibon api] http={r.status_code} pattern={pattern} -> open breaker")
                        _open_breaker()
                        base_rows = []
                        break

                    app.logger.info(f"[ibon api] http={r.status_code} pattern={pattern}")
                except Exception as e:
                    app.logger.info(f"[ibon api] err: {e}")
                _sleep_backoff(attempt)

            if _breaker_open_now():
                break

        _cache = {"ts": now, "data": base_rows}

    # 過濾 + 截斷（這個 return 要在迴圈外！）
    out = []
    kw = (keyword or "").strip()
    kw_lower = kw.lower()
    for it in base_rows or []:
        title = it.get("title", "")
        if kw and kw_lower not in title.lower():
            continue
        if only_concert and not _looks_like_concert(title):
            continue
        out.append(it)
        if len(out) >= max(1, int(limit)):
            break
    return out

# --------- LINE SDK（可選）---------
HAS_LINE = True
try:
    from linebot import LineBotApi, WebhookHandler
    from linebot.exceptions import InvalidSignatureError
    from linebot.models import (
        MessageEvent, TextMessage, TextSendMessage, ImageSendMessage,
        FollowEvent, JoinEvent,
    )
except Exception as e:
    HAS_LINE = False
    LineBotApi = WebhookHandler = InvalidSignatureError = None
    MessageEvent = TextMessage = TextSendMessage = ImageSendMessage = None
    logging.warning(f"[init] line-bot-sdk not available: {e}")

# --------- Firestore（可失敗不致命）---------
from google.cloud import firestore

try:  # pragma: no cover - optional dependency paths
    from google.auth.exceptions import DefaultCredentialsError  # type: ignore
except Exception:  # pragma: no cover - fallback when auth extras missing
    class DefaultCredentialsError(Exception):
        """Fallback DefaultCredentialsError replacement."""

        pass

try:  # pragma: no cover - optional dependency paths
    from google.api_core.exceptions import Forbidden, GoogleAPIError  # type: ignore
except Exception:  # pragma: no cover - fallback when api_core missing
    GoogleAPIError = Exception  # type: ignore

    class Forbidden(GoogleAPIError):  # type: ignore
        """Fallback Forbidden replacement."""

        pass

# === Browser helper: Selenium → Playwright fallback ===
def _run_js_with_fallback(url: str, js_func_literal: str):
    """
    在指定 URL 上執行一段『函式字面量』JS（例如 "() => {...}"），
    先用 Selenium，失敗再用 Playwright。回傳 JS 的 return 值（通常是 list）。
    """
    # 1) Selenium 先試
    if _SELENIUM_AVAILABLE:
        try:
            chrome_path = (os.environ.get("CHROME_BIN")
                           or ("/usr/bin/google-chrome" if os.path.exists("/usr/bin/google-chrome") else "/usr/bin/chromium"))
            chromedriver_path = os.environ.get("CHROMEDRIVER") or "/usr/bin/chromedriver"

            opts = Options()
            # headless on Cloud Run
            opts.add_argument("--headless=new")
            opts.add_argument("--no-sandbox")
            opts.add_argument("--disable-dev-shm-usage")
            opts.add_argument("--disable-gpu")
            opts.binary_location = chrome_path

            driver = webdriver.Chrome(service=ChromeService(executable_path=chromedriver_path), options=opts)
            driver.set_page_load_timeout(30)
            driver.get(url)
            # 給一點時間讓前端輪播初始
            try:
                time.sleep(1.5)
            except Exception:
                pass

            res = driver.execute_script(f"return ({js_func_literal})();")
            driver.quit()
            app.logger.info("[browser] Selenium path OK")
            return res
        except Exception as e:
            try:
                driver.quit()
            except Exception:
                pass
            app.logger.warning(f"[browser] Selenium failed: {e}")

    # 2) Playwright fallback
    if _PLAYWRIGHT_AVAILABLE:
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"])
                ctx = browser.new_context(locale="zh-TW")
                page = ctx.new_page()
                page.goto(url, wait_until="networkidle")
                res = page.evaluate(js_func_literal)
                browser.close()
            app.logger.info("[browser] Playwright path OK")
            return res
        except Exception as e:
            app.logger.warning(f"[browser] Playwright failed: {e}")

    # 3) 最後備援：純 requests 抓 HTML 用正則撈 Details（回傳 URL list）
    try:
        s = requests.Session()
        s.headers.update({"User-Agent": "Mozilla/5.0"})
        r = s.get(url, timeout=12)
        html = r.text
        urls = []
        for m in re.finditer(r'(?i)(?:https?://ticket\.ibon\.com\.tw)?/ActivityInfo/Details/(\d+)', html):
            u = urljoin("https://ticket.ibon.com.tw/", m.group(0))
            urls.append(u)
        seen = set()
        return [u for u in urls if not (u in seen or seen.add(u))]
    except Exception as e:
        app.logger.error(f"[browser] no engine available and HTML fallback failed: {e}")
        return []

def grab_ibon_carousel_urls():
    # 直接在 ibon 首頁的瀏覽器環境做同步 XHR 抓 JSON，比點擊穩很多
    script = r"""
    () => {
      try {
        const url = "https://ticket.ibon.com.tw/api/ActivityInfo/GetIndexData";
        const token = (() => {
          const raw = document.cookie.split(";").map(v => v.trim()).find(v => v.startsWith("XSRF-TOKEN="));
          if (!raw) return null;
          try {
            return decodeURIComponent(raw.split("=")[1] || "");
          } catch (e) {
            return (raw.split("=")[1] || "");
          }
        })();

        const form = new FormData();
        form.append("pattern", "");

        const xhr = new XMLHttpRequest();
        xhr.open("POST", url, false);                 // 同步 XHR（Selenium/Playwright 都吃）
        xhr.setRequestHeader("X-Requested-With", "XMLHttpRequest");
        if (token) xhr.setRequestHeader("X-XSRF-TOKEN", token);
        xhr.send(form);
        if (xhr.status < 200 || xhr.status >= 300) return [];

        let data; try { data = JSON.parse(xhr.responseText); } catch (e) { return []; }

        // 走訪 JSON，把可能的 Details 連結/ID 全撿出來
        const out = new Set();
        const base = "https://ticket.ibon.com.tw";

        const norm = (obj) => {
          if (!obj || typeof obj !== "object") return;
          const title = obj.Title || obj.ActivityTitle || obj.ActivityName || obj.GameName || obj.Name || obj.Subject;
          const link  = obj.GameTicketURL || obj.GameTicketUrl || obj.Url || obj.URL || obj.LinkUrl || obj.LinkURL || obj.Link;
          const id    = obj.ActivityID || obj.ActivityId || obj.ActivityInfoId || obj.ActivityInfoID ||
                        obj.GameId || obj.GameID || obj.Id || obj.ID;

          let href = null;
          if (typeof link === "string" && link.trim()) {
            href = new URL(link, base).href;
          } else if (id) {
            href = `${base}/ActivityInfo/Details/${id}`;
          }
          if (href && href.includes("/ActivityInfo/Details/")) out.add(href);

          Object.values(obj).forEach(v => {
            if (Array.isArray(v)) v.forEach(norm);
            else if (v && typeof v === "object") norm(v);
          });
        };

        norm(data);
        return Array.from(out);
      } catch (e) {
        return [];
      }
    }
    """
    url = IBON_ENT_URL
    raw_links = _run_js_with_fallback(url, script) or []

    # 這裡**不要**再用 typeof（那是 JS），只保留字串即可
    if not isinstance(raw_links, (list, tuple, set)):
        raw_links = [raw_links]
    cleaned = []
    for u in raw_links:
        if isinstance(u, str) and u:
            try:
                # 統一為絕對網址
                cleaned.append(urljoin(IBON_BASE, u))
            except Exception:
                pass

    only_details = sorted({u for u in cleaned if "/ActivityInfo/Details/" in u})
    return only_details

def _items_from_details_urls(urls: List[str], limit=10, keyword=None, only_concert=False):
    items = []
    s = sess_default()
    for u in urls:
        try:
            info = fetch_from_ticket_details(u, s) or {}
            details_url = info.get("details_url") or sanitize_details_url(u)
            title = info.get("title") or "活動"
            if keyword and keyword not in title:
                continue
            if only_concert and not _looks_like_concert(title):
                continue
            img = info.get("poster") or None
            items.append({
                "title": title,
                "url": details_url,
                "details_url": details_url,
                "image": img,
                "image_url": img,
            })
            if len(items) >= max(1, int(limit)):
                break
        except Exception:
            continue
    return items

@app.get("/ibon/carousel")
def ibon_carousel():
    urls = grab_ibon_carousel_urls()
    return jsonify({"count": len(urls), "urls": urls})

# 建議：白名單（可多個網域）
# 建議：白名單用環境變數，逗號分隔
_ALLOWED_ORIGINS_ENV = os.getenv("ALLOWED_ORIGINS", "https://liff.line.me")
ALLOWED_ORIGINS = [o.strip() for o in _ALLOWED_ORIGINS_ENV.split(",") if o.strip()]

try:
    from flask_cors import CORS  # type: ignore
    CORS(
        app,
        resources={r"/liff/*": {"origins": ALLOWED_ORIGINS}},
        supports_credentials=True,
    )
except Exception as e:
    app.logger.warning(f"flask-cors not available: {e}")
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
    app.logger.setLevel(logging.INFO)

# ======== 環境變數 ========
LINE_CHANNEL_ACCESS_TOKEN = os.getenv("LINE_CHANNEL_ACCESS_TOKEN", "")
LINE_CHANNEL_SECRET = os.getenv("LINE_CHANNEL_SECRET", "")
DEFAULT_PERIOD_SEC = int(os.getenv("DEFAULT_PERIOD_SEC", "60"))
ALWAYS_NOTIFY = os.getenv("ALWAYS_NOTIFY", "0") == "1"
FOLLOW_AREAS_PER_CHECK = int(os.getenv("FOLLOW_AREAS_PER_CHECK", "0"))

# 可選：手動覆蓋宣傳圖或 Details 連結（通常不需要）
PROMO_IMAGE_MAP: Dict[str, str] = {}
PROMO_DETAILS_MAP: Dict[str, str] = {}
try:
    PROMO_IMAGE_MAP = json.loads(os.getenv("PROMO_IMAGE_MAP", "{}"))
except Exception:
    PROMO_IMAGE_MAP = {}
try:
    PROMO_DETAILS_MAP = json.loads(os.getenv("PROMO_DETAILS_MAP", "{}"))
except Exception:
    PROMO_DETAILS_MAP = {}

if not LINE_CHANNEL_ACCESS_TOKEN or not LINE_CHANNEL_SECRET:
    app.logger.warning("LINE env not set: LINE_CHANNEL_ACCESS_TOKEN / LINE_CHANNEL_SECRET")

line_bot_api = LineBotApi(LINE_CHANNEL_ACCESS_TOKEN) if (HAS_LINE and LINE_CHANNEL_ACCESS_TOKEN) else None
handler = WebhookHandler(LINE_CHANNEL_SECRET) if (HAS_LINE and LINE_CHANNEL_SECRET) else None

MAX_PER_TICK = int(os.getenv("MAX_PER_TICK", "6"))
TICK_SOFT_DEADLINE_SEC = int(os.getenv("TICK_SOFT_DEADLINE_SEC", "50"))

# Firestore client
fs_client = None
FS_ERROR_MSG = ""
try:
    fs_client = firestore.Client()
    FS_OK = True
    FS_ERROR_MSG = ""
except (DefaultCredentialsError, Forbidden) as e:
    FS_OK = False
    FS_ERROR_MSG = "watch service unavailable"
    app.logger.warning(f"Firestore init failed (auth/permission): {e}")
except GoogleAPIError as e:
    FS_OK = False
    FS_ERROR_MSG = str(e) or "watch service unavailable"
    app.logger.warning(f"Firestore init failed: {e}")
except Exception as e:
    FS_OK = False
    FS_ERROR_MSG = str(e) or "watch service unavailable"
    app.logger.warning(f"Firestore init failed: {e}")

COL = "watchers"

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0 Safari/537.36"
)

_RE_DATE = re.compile(
    r"(?P<date>\d{4}(?:/|\.)\d{1,2}(?:/|\.)\d{1,2}|\d{4}年\d{1,2}月\d{1,2}日?)"
    r"(?:\s*[（(][^0-9)]+[)）])?"
    r"\s*(?P<time>\d{1,2}[：:]\d{2})"
)
_RE_AREA_TAG = re.compile(r"<area\b[^>]*>", re.I)
_SALE_KEYWORDS = ("售票", "販售", "銷售", "開賣", "購票")
_EVENT_DATE_KEYWORDS = (
    "演出",
    "活動",
    "日期",
    "時間",
    "Time",
    "Date",
    "開演",
    "場次",
)



def _normalize_date_text(text: Optional[str]) -> Optional[str]:
    if not text:
        return None
    digits = re.findall(r"\d+", text)
    if len(digits) >= 3:
        y, m, d = digits[:3]
        try:
            return f"{int(y):04d}/{int(m):02d}/{int(d):02d}"
        except Exception:
            pass
    cleaned = str(text).strip()
    return cleaned or None


def _normalize_time_text(text: Optional[str]) -> Optional[str]:
    if not text:
        return None
    digits = re.findall(r"\d+", text)
    if len(digits) >= 2:
        h, minute = digits[:2]
        try:
            return f"{int(h):02d}:{int(minute):02d}"
        except Exception:
            pass
    cleaned = str(text).strip()
    return cleaned or None


def _format_datetime_match(m: Optional[re.Match]) -> Optional[str]:
    if not m:
        return None
    group_map = m.groupdict() if hasattr(m, "groupdict") else {}
    raw_date = group_map.get("date")
    raw_time = group_map.get("time")
    if raw_date is None and m.lastindex and m.lastindex >= 1:
        raw_date = m.group(1)
    if raw_time is None and m.lastindex and m.lastindex >= 2:
        raw_time = m.group(2)
    date_text = _normalize_date_text(raw_date)
    time_text = _normalize_time_text(raw_time)
    if date_text and time_text:
        return f"{date_text} {time_text}"
    return None


def _is_sale_context(text: str) -> bool:
    if not text:
        return False
    low = str(text).lower()
    return any(kw in low for kw in _SALE_KEYWORDS)


def _parse_datetime_string(dt_text: str) -> Tuple[Optional[datetime], bool]:
    candidate = (dt_text or "").strip()
    if not candidate:
        return None, False
    cleaned = candidate.replace("年", "/").replace("月", "/").replace("日", "")
    cleaned = cleaned.replace(".", "/")
    date_match = re.search(r"\d{4}/\d{1,2}/\d{1,2}", cleaned)
    time_match = re.search(r"\d{1,2}[：:]\d{2}", cleaned)
    date_text = _normalize_date_text(date_match.group(0) if date_match else cleaned)
    time_text = _normalize_time_text(time_match.group(0) if time_match else None)
    if date_text and time_text:
        try:
            return datetime.strptime(f"{date_text} {time_text}", "%Y/%m/%d %H:%M"), True
        except Exception:
            pass
    if date_text:
        try:
            return datetime.strptime(date_text, "%Y/%m/%d"), bool(time_text)
        except Exception:
            return None, bool(time_text)
    return None, bool(time_text)


def _parse_price_value(text: Optional[str]) -> Optional[int]:
    if not text:
        return None
    raw = str(text)
    m = re.search(r"(?:NT\$|NT\s*\$|\$|＄|元|價格|票價)\s*([\d,]+)", raw)
    candidate = m.group(1) if m else None
    if not candidate:
        digits = re.findall(r"\d{3,}", raw.replace(",", ""))
        candidate = digits[0] if digits else None
    if not candidate:
        return None
    try:
        return int(str(candidate).replace(",", ""))
    except Exception:
        return None


def _collect_datetime_candidates(lines: List[str]) -> List[Tuple[datetime, bool, str]]:
    candidates: List[Tuple[datetime, bool, str]] = []
    pending_date: Optional[str] = None
    for raw_line in lines:
        line = (raw_line or "").strip()
        if not line:
            continue
        if _is_sale_context(line):
            pending_date = None
            continue
        compact = re.sub(r"\s+", " ", line)

        if any(ch in compact for ch in ("~", "～")):

            pending_date = None
            continue
        dt_match = _format_datetime_match(_RE_DATE.search(compact))
        if dt_match:
            dt_obj, has_time = _parse_datetime_string(dt_match)
            if dt_obj:
                candidates.append((dt_obj, has_time, compact))
            continue
        date_match = re.search(r"\d{4}(?:/|\.)\d{1,2}(?:/|\.)\d{1,2}|\d{4}年\d{1,2}月\d{1,2}日?", compact)
        time_match = re.search(r"\d{1,2}[：:]\d{2}", compact)
        if date_match and not time_match:
            pending_date = _normalize_date_text(date_match.group(0))
            continue
        if time_match and pending_date:
            time_norm = _normalize_time_text(time_match.group(0))
            if time_norm and pending_date:
                dt_obj, has_time = _parse_datetime_string(f"{pending_date} {time_norm}")
                if dt_obj:
                    candidates.append((dt_obj, has_time, f"{pending_date} {time_norm}"))
            pending_date = None
    return candidates

def _clean_venue_text(text: Optional[str]) -> Optional[str]:
    if not text:
        return None
    cleaned = re.sub(r"\s+", " ", str(text)).strip()
    cleaned = re.sub(r"\b\d{1,2}[：:]\d{2}\b", "", cleaned)
    cleaned = re.sub(r"[~～].*", "", cleaned)
    cleaned = cleaned.strip(" ，,、;:；：-")
    return cleaned or None

LOGO = "https://ticketimg2.azureedge.net/logo.png"
TICKET_API = "https://ticket.ibon.com.tw/api/ActivityInfo/GetGameInfoList"
IBON_DETAIL_API = "https://ticket.ibon.com.tw/api/ActivityInfo/GetDetailData"

# ================= 小工具 =================
def soup_parse(html: str) -> BeautifulSoup:
    try:
        return BeautifulSoup(html, "lxml")
    except Exception:
        return BeautifulSoup(html, "html.parser")

def hash_state(sections: Dict[str, int], selling: List[str]) -> str:
    items = sorted((k, int(v)) for k, v in sections.items())
    hot = sorted(selling)
    raw = json.dumps({"num": items, "hot": hot}, ensure_ascii=False, separators=(",", ":"))
    return hashlib.md5(raw.encode("utf-8")).hexdigest()

def canonicalize_url(u: str) -> str:
    p = urlparse(u.strip())
    q = parse_qs(p.query, keep_blank_values=True)
    q_sorted = []
    for k in sorted(q.keys()):
        for v in q[k]:
            q_sorted.append((k, v))
    new_q = urlencode(q_sorted, doseq=True)
    return urlunparse((p.scheme, p.netloc, p.path, "", new_q, ""))

def send_text(to_id: str, text: str):
    if not line_bot_api:
        app.logger.info(f"[dry-run] send_text to {to_id}: {text}")
        return
    try:
        line_bot_api.push_message(to_id, TextSendMessage(text=text))
    except Exception as e:
        app.logger.error(f"[LINE] push text failed: {e}")

def send_image(to_id: str, img_url: str):
    if not line_bot_api:
        app.logger.info(f"[dry-run] send_image to {to_id}: {img_url}")
        return
    try:
        line_bot_api.push_message(
            to_id,
            ImageSendMessage(original_content_url=img_url, preview_image_url=img_url)
        )
    except Exception as e:
        app.logger.error(f"[LINE] push image failed: {e}")

def sess_default() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": UA,
        "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.6",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Connection": "close",
    })
    return s

def _url_ok(u: str) -> bool:
    if not u or not u.startswith("http"):
        return False
    try:
        r = requests.head(u, timeout=6, allow_redirects=True)
        if r.status_code in (403, 405):  # 某些 CDN 禁 HEAD
            r = requests.get(u, stream=True, timeout=8)
        return 200 <= r.status_code < 400
    except Exception:
        return False

def _first_http_url(s: str) -> Optional[str]:
    m = re.search(r'https?://[^\s"\'<>]+', str(s))
    return m.group(0) if m else None

def find_activity_image_any(s: str) -> Optional[str]:
    m = re.search(r"https?://[^\"'<>]+/image/ActivityImage/[^\s\"'<>]+\.(?:jpg|jpeg|png)", s, flags=re.I)
    if m: return m.group(0)
    m = re.search(r"https?://ticketimg2\.azureedge\.net/[^\s\"'<>]+\.(?:jpg|jpeg|png)", s, flags=re.I)
    if m: return m.group(0)
    m = re.search(r"https?://img\.ibon\.com\.tw/[^\s\"'<>]+\.(?:jpg|jpeg|png)", s, flags=re.I)
    return m.group(0) if m else None

def find_details_url_candidates_from_html(html: str, base: str) -> List[str]:
    soup = soup_parse(html)
    urls: set[str] = set()
    for a in soup.select('a[href*="ActivityInfo/Details"]'):
        href = (a.get("href") or "").strip()
        if href:
            urls.add(urljoin(base, href))
    for m in re.finditer(r"(?:https?://ticket\.ibon\.com\.tw)?/ActivityInfo/Details/\d+", html):
        urls.add(urljoin("https://ticket.ibon.com.tw", m.group(0)))
    return list(urls)

def _try_decode_ticket_target(val: str) -> Optional[str]:
    if not val:
        return None

    queue: List[str] = [val]
    seen: set[str] = set()

    while queue:
        cur = queue.pop(0)
        if cur in seen:
            continue
        seen.add(cur)

        cur = cur.strip()
        if not cur:
            continue

        if cur.lower().startswith("http"):
            return cur
        if cur.startswith("//"):
            return "https:" + cur
        if cur.startswith("/"):
            return urljoin("https://orders.ibon.com.tw/", cur)

        unquoted = unquote(cur)
        if unquoted != cur and unquoted not in seen:
            queue.append(unquoted)

        padded = cur + "=" * ((4 - len(cur) % 4) % 4)
        try:
            decoded = base64.b64decode(padded).decode("utf-8", errors="ignore")
            if decoded and decoded not in seen:
                queue.append(decoded)
        except Exception:
            pass

    return None

def _unwrap_go_ticket_url(u: str) -> Optional[str]:
    if not u:
        return None

    abs_url = urljoin(IBON_BASE, u)
    if "UTK0201_000" in abs_url.upper():
        return abs_url

    try:
        parsed = urlparse(abs_url)
    except Exception:
        return None

    if "GoTicketURL" not in parsed.path:
        return abs_url if abs_url.lower().startswith("http") else None

    q = parse_qs(parsed.query, keep_blank_values=True)
    for key in ("GoUrl", "GoURL", "gourl", "RedirectUrl", "redirectUrl"):
        vals = q.get(key)
        if not vals:
            continue
        for raw in vals:
            candidate = _try_decode_ticket_target(raw)
            if candidate and "UTK0201_000" in candidate.upper():
                return candidate

    return None

def _resolve_utk_url(
    activity_id: Optional[str],
    pattern: Optional[str],
    sess: requests.Session,
    details_url: str,
    trace: Optional[List[Dict[str, Any]]] = None,
) -> Optional[str]:
    if not activity_id:
        return None

    headers = {
        "Referer": details_url,
        "User-Agent": UA,
        "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.6",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }

    base_url = f"{IBON_HOST}/ActivityInfo/GoTicketURL"
    combos: List[Dict[str, str]] = [
        {"hasDeadline": "1", "SystemBrowseType": "2"},
        {"hasDeadline": "1", "SystemBrowseType": "1"},
        {"SystemBrowseType": "2"},
        {},
    ]

    for combo in combos:
        query: Dict[str, str] = {"ActivityID": str(activity_id)}
        if pattern:
            query["pattern"] = pattern
        for key, val in combo.items():
            if val is not None:
                query[key] = val

        for attempt in range(len(UTK_BACKOFF)):
            start = time.time()
            status: Optional[int] = None
            reason: Optional[str] = None
            resolved: Optional[str] = None
            request_url: Optional[str] = None
            try:
                resp = sess.get(
                    base_url,
                    params=query,
                    headers=headers,
                    allow_redirects=True,
                    timeout=12,
                )
                status = resp.status_code
                request_url = resp.url

                final_url = resp.url or ""
                if final_url and "UTK0201_000" in final_url.upper():
                    resolved = final_url
                elif 200 <= status < 400:
                    urls = _extract_ticket_urls_from_text(resp.text)
                    if urls:
                        resolved = urls[0]
                    elif resp.history:
                        for hist in reversed(resp.history):
                            loc = hist.headers.get("Location") or ""
                            candidate = _unwrap_go_ticket_url(loc) or urljoin(IBON_HOST, loc)
                            if candidate and "UTK0201_000" in candidate.upper():
                                resolved = candidate
                                break

                else:
                    reason = f"http={status}"
            except Exception as exc:
                reason = str(exc)

            if not resolved and not reason:
                reason = "no-ticket-url"

            elapsed_ms = int((time.time() - start) * 1000)
            if trace is not None:
                trace.append(
                    {
                        "phase": "utk_resolve",
                        "ok": bool(resolved),
                        "url": request_url or base_url,
                        "status": status or 0,
                        "elapsed_ms": elapsed_ms,
                        "count": 1 if resolved else 0,
                        "reason": reason,
                    }
                )

            if resolved:
                return resolved

            backoff = UTK_BACKOFF[min(attempt, len(UTK_BACKOFF) - 1)]
            time.sleep(backoff)

    return None

def _extract_ticket_urls_from_text(text: str) -> List[str]:
    if not text:
        return []

    found: List[str] = []

    def _append(candidate: Optional[str]):
        if not candidate:
            return
        url = candidate.strip()
        if not url:
            return
        if not url.lower().startswith("http"):
            url = urljoin("https://orders.ibon.com.tw/", url.lstrip("/"))
        if "UTK0201_000" not in url.upper():
            return
        if url not in found:
            found.append(url)

    patterns = [
        r'https?://[^\s"\'<>]+UTK0201_000[^\s"\'<>]*',
        r'//orders\.ibon\.com\.tw/[^\s"\'<>]*UTK0201_000[^\s"\'<>]*',
        r'/Application/UTK02/UTK0201_000\.aspx[^\s"\'<>]*',
        r'/UTK02/UTK0201_000\.aspx[^\s"\'<>]*',
    ]

    for pat in patterns:
        for m in re.finditer(pat, text, flags=re.I):
            _append(m.group(0))

    for m in re.finditer(r'(?:https?://ticket\.ibon\.com\.tw)?/ActivityInfo/GoTicketURL[^\s"\'<>]+', text, flags=re.I):
        unwrapped = _unwrap_go_ticket_url(m.group(0))
        if unwrapped:
            _append(unwrapped)

    return found

def _extract_details_any(html: str) -> List[str]:
    """盡可能把 /ActivityInfo/Details/<id> 都撿出來（避免只靠固定版型）。"""
    urls: List[str] = []

    # 1) 直接正則掃全頁
    for m in re.finditer(r'(?i)(?:https?://ticket\.ibon\.com\.tw)?/ActivityInfo/Details/(\d+)', html):
        urls.append(urljoin(IBON_BASE, m.group(0)))

    # 2) 拿 a[href]（有時候 href 是相對路徑）
    try:
        soup = soup_parse(html)
        for a in soup.select('a[href*="ActivityInfo/Details"]'):
            href = (a.get("href") or "").strip()
            if href:
                urls.append(urljoin(IBON_BASE, href))
    except Exception:
        pass

    # 3) script 內 JSON/字串
    for m in re.finditer(r'ActivityInfoId"\s*:\s*(\d+)|ActivityId"\s*:\s*(\d+)', html):
        gid = next(g for g in m.groups() if g)
        urls.append(f"https://ticket.ibon.com.tw/ActivityInfo/Details/{gid}")

    # 去重
    seen, out = set(), []
    for u in urls:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out

# ---------- 活動資訊與圖片（API/Details） ----------
def _deep_pick_activity_info(data: Any) -> Dict[str, str]:
    out: Dict[str, Optional[str]] = {"title": None, "place": None, "dt": None, "poster": None}
    def walk(x):
        if isinstance(x, dict):
            for k, v in x.items():
                kl = str(k).lower()
                if not out["title"] and any(t in kl for t in ("activityname","gamename","title","actname","activity_title","name")):
                    if isinstance(v, str) and v.strip(): out["title"] = v.strip()
                if not out["place"] and any(t in kl for t in ("placename","venue","place","site","location")):
                    if isinstance(v, str) and v.strip(): out["place"] = v.strip()
                if not out["dt"] and any(t in kl for t in ("starttime","startdatetime","gamedatetime","gamedate","begindatetime","datetime")):
                    s = str(v)
                    m = re.search(r"(\d{4})[-/](\d{1,2})[-/](\d{1,2})[\sT]+(\d{1,2}):(\d{2})", s)
                    if m:
                        out["dt"] = f"{int(m.group(1))}/{int(m.group(2)):02d}/{int(m.group(3)):02d} {int(m.group(4)):02d}:{m.group(5)}"
                if not out["poster"] and ("image" in kl or "poster" in kl):
                    url = _first_http_url(v) if isinstance(v, str) else None
                    if url: out["poster"] = url
            for v in x.values(): walk(v)
        elif isinstance(x, list):
            for it in x: walk(it)
    walk(data)
    return {k: v for k, v in out.items() if v}

def fetch_game_info_from_api(perf_id: Optional[str], product_id: Optional[str], referer_url: str, sess: requests.Session) -> Dict[str, str]:
    session, token = _prepare_ibon_session()
    if session is None:
        return {}

    headers = {
        "Origin": "https://ticket.ibon.com.tw",
        "Referer": referer_url,
        "User-Agent": UA,
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json;charset=UTF-8",
        "X-Requested-With": "XMLHttpRequest",
        "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.6",
    }
    if token:
        headers["X-XSRF-TOKEN"] = token

    params_list: List[Dict[str, Any]] = []
    payload_keys: set[tuple[tuple[str, Any], ...]] = set()

    def add_payload(**kwargs):
        payload = {k: v for k, v in kwargs.items() if v not in (None, "", [])}
        key = tuple(sorted(payload.items()))
        if key in payload_keys:
            return
        payload_keys.add(key)
        params_list.append(payload)

    if perf_id or product_id:
        add_payload(Performance_ID=perf_id, Product_ID=product_id)
        add_payload(PerformanceId=perf_id, ProductId=product_id)
        add_payload(PERFORMANCE_ID=perf_id, PRODUCT_ID=product_id)
        add_payload(PERFORMANCEID=perf_id, PRODUCTID=product_id)

    activity_ids: List[str] = []
    if referer_url:
        act = _activity_id_from_url(referer_url)
        if act:
            activity_ids.append(act)
    if perf_id and perf_id in PROMO_DETAILS_MAP:
        act = _activity_id_from_url(PROMO_DETAILS_MAP[perf_id])
        if act:
            activity_ids.append(act)

    for act in activity_ids:
        numeric_id: Optional[int] = None
        try:
            numeric_id = int(act)
        except Exception:
            numeric_id = None

        if numeric_id is not None:
            add_payload(id=numeric_id)
        add_payload(id=act)
        add_payload(ActivityID=act)
        add_payload(ActivityId=act)
        add_payload(ActivityInfoID=act)
        add_payload(ActivityInfoId=act)

        for browse in (None, 1, 2):
            for has_deadline in (None, True):
                add_payload(ActivityID=act, SystemBrowseType=browse, hasDeadline=has_deadline)
                add_payload(ActivityId=act, SystemBrowseType=browse, hasDeadline=has_deadline)

    add_payload()

    picked: Dict[str, str] = {}
    all_ticket_urls: List[str] = []

    for params in params_list:
        payload = params
        try:
            resp = session.post(TICKET_API, json=payload, headers=headers, timeout=12)
        except Exception as e:
            app.logger.info(f"[api] fetch fail ({params}): {e}")
            continue

        if resp.status_code == 200:
            try:
                data = resp.json()
            except Exception as e:
                app.logger.info(f"[api] bad json ({params}): {e}")
                continue

            text_blob = json.dumps(data, ensure_ascii=False)
            info = _deep_pick_activity_info(data)

            act_id = None
            m = re.search(r'"ActivityID"\s*:\s*(\d+)', text_blob)
            if m:
                act_id = m.group(1)
            elif activity_ids:
                act_id = activity_ids[0]

            if act_id:
                info.setdefault("details", build_ibon_details_url(act_id))
                info.setdefault("activity_id", act_id)

            if not info.get("details"):
                m = re.search(r'https?://ticket\.ibon\.com\.tw/ActivityInfo/Details/(\d+)', text_blob)
                if m:
                    info["details"] = m.group(0)
                    info.setdefault("activity_id", m.group(1))

            if not info.get("poster"):
                promo = find_activity_image_any(text_blob)
                if promo:
                    info["poster"] = promo

            ticket_urls = _extract_ticket_urls_from_text(text_blob)
            for t in ticket_urls:
                if t not in all_ticket_urls:
                    all_ticket_urls.append(t)
            if ticket_urls and not info.get("ticket_urls"):
                info["ticket_urls"] = ticket_urls

            def match_obj(obj: Any) -> bool:
                if not isinstance(obj, (dict, list)):
                    return False
                blob = json.dumps(obj, ensure_ascii=False)
                ok = True
                if perf_id:
                    ok = ok and (perf_id in blob)
                if product_id:
                    ok = ok and (product_id in blob)
                return ok

            if isinstance(data, list):
                for it in data:
                    if match_obj(it):
                        info.update(_deep_pick_activity_info(it))
                        break
            elif isinstance(data, dict):
                for v in data.values():
                    if isinstance(v, list):
                        for it in v:
                            if match_obj(it):
                                info.update(_deep_pick_activity_info(it))
                                break

            if info:
                details_raw = info.get("details") or info.get("details_url")
                if isinstance(details_raw, str) and details_raw:
                    sanitized = sanitize_details_url(details_raw)
                    info["details"] = sanitized
                    info["details_url"] = sanitized
                picked = info
                break

        elif resp.status_code in (401, 403, 419):
            session, token = _prepare_ibon_session()
            if session is None:
                break
            headers = {
                "Origin": "https://ticket.ibon.com.tw",
                "Referer": referer_url,
                "User-Agent": UA,
                "Accept": "application/json, text/plain, */*",
                "Content-Type": "application/json;charset=UTF-8",
                "X-Requested-With": "XMLHttpRequest",
                "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.6",
            }
            if token:
                headers["X-XSRF-TOKEN"] = token
            continue

    if all_ticket_urls:
        existing = picked.get("ticket_urls") if picked else None
        merged: List[str] = []
        for src in (existing if isinstance(existing, list) else []) + all_ticket_urls:
            if src not in merged:
                merged.append(src)
        if picked:
            picked["ticket_urls"] = merged
        else:
            picked = {"ticket_urls": merged}

    return picked

def fetch_from_ticket_details(details_url: str, sess: requests.Session) -> Dict[str, Any]:
    clean_details = sanitize_details_url(details_url)
    details_url = clean_details
    out: Dict[str, Any] = {"details_url": details_url}
    ticket_urls: List[str] = []
    parsed_details = urlparse(details_url)
    pattern = (parse_qs(parsed_details.query).get("pattern", ["ENTERTAINMENT"])[0] or "ENTERTAINMENT").strip() or "ENTERTAINMENT"
    out["pattern"] = pattern

    detail_html: Optional[str] = None
    html_lines: List[str] = []
    api_dt_values: List[Tuple[str, int]] = []
    try:
        resp = sess.get(details_url, timeout=12)
        if resp.status_code == 200:
            detail_html = _decode_ibon_html(resp)
    except Exception as exc:
        app.logger.info(f"[details] fetch fail: {exc}")


    def _abs_url(u: Optional[str]) -> Optional[str]:
        if not u:
            return None
        try:
            return urljoin(details_url, u)
        except Exception:
            return u

    def _format_api_dt(val: Optional[str]) -> Optional[str]:
        if not val or not isinstance(val, str):
            return None
        raw = val.strip()
        if not raw:
            return None
        clean = raw.replace("Z", "")
        if "+" in clean:
            clean = clean.split("+")[0]
        for fmt in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S"):
            try:
                dt = datetime.strptime(clean, fmt)
                return dt.strftime("%Y/%m/%d %H:%M")
            except ValueError:
                continue
        return None

    activity_id = _activity_id_from_url(details_url)
    if activity_id:
        out.setdefault("activity_id", str(activity_id))

    api_item: Optional[Dict[str, Any]] = None
    if activity_id:
        session: Optional[requests.Session] = None
        token: Optional[str] = None
        for attempt in range(3):
            if session is None:
                session, token = _prepare_ibon_session()
            if session is None:
                break
            headers = {
                "Origin": "https://ticket.ibon.com.tw",
                "Referer": details_url,
                "X-Requested-With": "XMLHttpRequest",
            }
            if token:
                headers["X-XSRF-TOKEN"] = token
            try:
                resp = session.post(
                    IBON_DETAIL_API,
                    files={"id": (None, str(activity_id))},
                    headers=headers,
                    timeout=10,
                )
                if resp.status_code == 200:
                    data = resp.json()
                    item = data.get("Item") if isinstance(data, dict) else None
                    if isinstance(item, dict):
                        api_item = item
                    break
                if resp.status_code in (401, 403, 419):
                    session, token = _prepare_ibon_session()
                    continue
                if 500 <= resp.status_code < 600:
                    app.logger.info(f"[details-api] http={resp.status_code}")
                    break
            except Exception as e:
                app.logger.info(f"[details-api] err: {e}")
            _sleep_backoff(attempt)

    content_lines: List[str] = []
    content_html = ""
    text_venue_candidates: List[str] = []
    text_address_candidates: List[str] = []

    if api_item:
        if api_item.get("ActivityID"):
            activity_id = str(api_item.get("ActivityID"))
            out.setdefault("activity_id", activity_id)

        title = (api_item.get("ActivityName") or api_item.get("ActivityTitle") or "").strip()
        if title:
            out.setdefault("title", title)

        poster = (
            api_item.get("ActivityImageURL")
            or api_item.get("ActivityImage")
            or api_item.get("PlatformImageURL")
        )
        poster = _abs_url(str(poster).strip()) if poster else None
        if poster:
            out.setdefault("poster", poster)

        place = (api_item.get("ActivityLocation") or api_item.get("ActivityPlace") or "").strip()
        if place:
            out.setdefault("place", place)

        show_from = _format_api_dt(api_item.get("ActivityShowFrom"))
        show_to = _format_api_dt(api_item.get("ActivityShowTo"))
        if show_from:
            api_dt_values.append((show_from, 0))
        if show_to:
            api_dt_values.append((show_to, 0))
        event_start = _format_api_dt(api_item.get("ActivitySDate"))
        event_end = _format_api_dt(api_item.get("ActivityEDate"))
        if event_start:
            api_dt_values.append((event_start, 1))
        if event_end:
            api_dt_values.append((event_end, 1))

        content_html = api_item.get("ActivityContent") or ""
        if content_html:
            try:
                regex_place = None
                m = re.search(r'(?:演出|活動)?地點[：:]+\s*([^<\n\r]+)', content_html, flags=re.I)
                if m:
                    regex_place = re.sub(r"\s+", " ", m.group(1)).strip()
                if not regex_place:
                    m = re.search(r'(?:演出|活動)?地點[：:][^<]*<[^>]*>([^<]+)', content_html, flags=re.I)
                    if m:
                        regex_place = re.sub(r"\s+", " ", m.group(1)).strip()
                if regex_place:
                    out.setdefault("place", regex_place)

                soup = soup_parse(content_html)
                content_lines = [ln.strip() for ln in soup.get_text("\n").split("\n") if ln.strip()]
                for idx, line in enumerate(content_lines):
                    if "地址" in line or "Address" in line:
                        candidate = line.split("：", 1)[-1].strip()
                        if candidate:
                            text_address_candidates.append(candidate)
                    if any(key in line for key in ("地點", "場地", "演出地點", "活動地點")):
                        candidate = line.split("：", 1)[-1].strip() if "：" in line else line
                        if candidate:
                            text_venue_candidates.append(candidate)
                        if idx + 1 < len(content_lines):
                            nxt = content_lines[idx + 1].strip()
                            if nxt and not any(k in nxt for k in ("日期", "時間", "票價", "售票")):
                                text_venue_candidates.append(nxt)
                if not out.get("poster"):
                    img = soup.find("img")
                    if img and img.get("src"):
                        out["poster"] = _abs_url(img.get("src")) or out.get("poster")
                if not out.get("poster"):
                    promo = find_activity_image_any(content_html)
                    if promo:
                        out["poster"] = promo

                ticket_urls.extend(_extract_ticket_urls_from_text(content_html))
            except Exception as e:
                app.logger.info(f"[details-api] content parse err: {e}")

    if detail_html:
        try:
            soup = soup_parse(detail_html)
            html_lines = [ln.strip() for ln in soup.get_text("\n").split("\n") if ln.strip()]
            if html_lines:
                seen_lines = set(content_lines)
                for line in html_lines:
                    if line not in seen_lines:
                        content_lines.append(line)
                        seen_lines.add(line)
            for idx, line in enumerate(html_lines):
                if "地址" in line or "Address" in line:
                    candidate = line.split("：", 1)[-1].strip()
                    if candidate:
                        text_address_candidates.append(candidate)
                if any(key in line for key in ("地點", "場地", "演出地點", "活動地點")):
                    candidate = line.split("：", 1)[-1].strip() if "：" in line else line
                    if candidate:
                        text_venue_candidates.append(candidate)
                    if idx + 1 < len(html_lines):
                        nxt = html_lines[idx + 1].strip()
                        if nxt and not any(k in nxt for k in ("日期", "時間", "價", "售票")):
                            text_venue_candidates.append(nxt)

            if not out.get("poster"):
                for sel in [
                    'meta[property="og:image:secure_url"]',
                    'meta[property="og:image"]',
                    'meta[name="twitter:image"]',
                ]:
                    m = soup.select_one(sel)
                    if m and m.get("content"):
                        out["poster"] = urljoin(details_url, m["content"].strip())
                        break
                if not out.get("poster"):
                    for img in soup.find_all("img"):
                        src = (img.get("src") or "").strip()
                        if src and any(k in src.lower() for k in ("activityimage", "azureedge", "banner", "cover", "adimage")):
                            out["poster"] = urljoin(details_url, src)
                            break

            if not out.get("title"):
                h1 = soup.select_one("h1")
                if h1:
                    t = h1.get_text(" ", strip=True)
                    if t:
                        out["title"] = t
                if not out.get("title") and soup.title and soup.title.text:
                    t = soup.title.get_text(" ", strip=True)
                    if t:
                        out["title"] = t

            if not out.get("place"):
                title_html, place_html, _ = extract_title_place_from_html(detail_html)
                if place_html:
                    out["place"] = place_html
                if title_html and not out.get("title"):
                    out["title"] = title_html

            ticket_urls.extend(_extract_ticket_urls_from_text(detail_html))
        except Exception as e:
            app.logger.info(f"[details] parse err: {e}")

    # 透過內容文字補齊場地
    if content_lines:
        if not out.get("place"):
            for idx, line in enumerate(content_lines):
                if any(key in line for key in ("地點", "場地", "地址")):
                    candidate = line.split("：", 1)[-1].strip()
                    if candidate and candidate != line and len(candidate) > 2:
                        out["place"] = candidate
                        break
                    for j in range(idx + 1, len(content_lines)):
                        nxt = content_lines[j].strip()
                        if not nxt:
                            continue
                        if nxt.endswith("："):
                            continue
                        if any(key in nxt for key in ("日期", "時間", "票價", "售票")):
                            continue
                        out["place"] = nxt
                        break
                    if out.get("place"):
                        break

    def _ensure_https(url: Optional[str]) -> Optional[str]:
        if not url:
            return None
        val = str(url).strip()
        if not val:
            return None
        if val.startswith("//"):
            val = "https:" + val
        if val.startswith("http://"):
            val = "https://" + val[len("http://"):]
        if not val.startswith("https://"):
            return None
        return val

    image_candidates: List[str] = []
    poster_val = out.get("poster")
    if poster_val:
        image_candidates.append(str(poster_val))
    final_image = None
    for img in image_candidates:
        norm = img
        if norm and not norm.startswith("http"):
            norm = _abs_url(norm)
        norm = _ensure_https(norm)
        if norm:
            final_image = norm
            break
    if not final_image:
        final_image = LOGO
    out["poster"] = final_image
    out["image_url"] = final_image


    # datetime candidates derived from API values and page text
    dt_candidates: List[Tuple[int, datetime, bool]] = []

    def _add_dt_candidate(raw: Optional[str], priority: int):
        if not raw or not isinstance(raw, str):
            return
        dt_obj, has_time = _parse_datetime_string(raw)
        if dt_obj:
            dt_candidates.append((priority, dt_obj, has_time))

    for raw, priority in api_dt_values:
        _add_dt_candidate(raw, priority)

    for dt_obj, has_time, line_text in _collect_datetime_candidates(content_lines):
        if not line_text:
            dt_candidates.append((2, dt_obj, has_time))
            continue
        if _is_sale_context(line_text):
            continue
        line_priority = 2
        lower_line = line_text.lower()
        if any(keyword.lower() in lower_line for keyword in _EVENT_DATE_KEYWORDS):
            line_priority = 0
        dt_candidates.append((line_priority, dt_obj, has_time))

    seen_dt: set[tuple] = set()
    dedup_dt: List[Tuple[int, datetime, bool]] = []
    for priority, dt_obj, has_time in dt_candidates:
        key = (dt_obj, has_time)
        if key in seen_dt:
            continue
        seen_dt.add(key)
        dedup_dt.append((priority, dt_obj, has_time))
    dt_candidates = dedup_dt

    dt_candidates.sort(key=lambda x: (x[0], x[1], 0 if x[2] else 1))
    final_date = None
    if dt_candidates:
        _, dt_obj, has_time = dt_candidates[0]
        final_date = dt_obj.strftime("%Y/%m/%d %H:%M") if has_time else dt_obj.strftime("%Y/%m/%d")

    if final_date:
        out["dt"] = final_date
        out["date"] = final_date
    else:
        out.pop("dt", None)

    venue_candidates: List[Tuple[int, str]] = []
    if api_item:
        for key in ("ActivityLocation", "ActivityPlace", "ActivitySite", "ActivityVenue"):
            val = api_item.get(key)
            if isinstance(val, str):
                cleaned = _clean_venue_text(val)
                if cleaned:
                    venue_candidates.append((0, cleaned))
    if out.get("place"):
        cleaned = _clean_venue_text(out.get("place"))
        if cleaned:
            venue_candidates.append((0, cleaned))
    for txt in text_venue_candidates:
        cleaned = _clean_venue_text(txt)
        if cleaned:
            venue_candidates.append((1, cleaned))

    final_venue = None
    seen_names: set[str] = set()
    for priority, name in sorted(venue_candidates, key=lambda x: (x[0], len(x[1]))):
        key = name.lower()
        if key in seen_names:
            continue
        seen_names.add(key)
        final_venue = name
        break

    if final_venue:
        out["place"] = final_venue
        out["venue"] = final_venue
    elif out.get("place"):
        cleaned = _clean_venue_text(out.get("place"))
        if cleaned:
            out["place"] = cleaned
            out["venue"] = cleaned

    address_val = None
    for cand in text_address_candidates:
        cleaned_addr = re.sub(r"\s+", " ", cand).strip()
        if cleaned_addr:
            address_val = cleaned_addr
            break
    if address_val and final_venue and address_val.startswith(final_venue):
        trimmed = address_val[len(final_venue):].lstrip(" ，,")
        if trimmed:
            address_val = trimmed
    if address_val and (not final_venue or address_val != final_venue):
        out["address"] = address_val
    else:
        out.pop("address", None)

    if out.get("title"):
        out["title"] = str(out["title"]).strip()

    if ticket_urls:
        uniq: List[str] = []
        for t in ticket_urls:
            if t not in uniq:
                uniq.append(t)
        out["ticket_urls"] = uniq

    cleaned: Dict[str, Any] = {}
    for key, value in out.items():
        if value is None:
            continue
        if isinstance(value, str):
            trimmed = value.strip()
            if not trimmed and key not in {"details_url"}:
                continue
            cleaned[key] = trimmed if trimmed else value
        elif isinstance(value, list):
            if value:
                cleaned[key] = value
        else:
            cleaned[key] = value
    return cleaned

# ---- 圖片（宣傳圖 + 座位圖）----
def pick_event_images_from_000(html: str, base_url: str) -> Tuple[str, Optional[str]]:
    poster = LOGO
    seatmap = None
    try:
        soup = soup_parse(html)
        for img in soup.find_all("img"):
            src = (img.get("src") or "").strip()
            if src and "static_bigmap" in src.lower():
                seatmap = urljoin(base_url, src); break
        if not seatmap:
            m = re.search(r'https?://[^\s"\'<>]+static_bigmap[^\s"\'<>]+?\.(?:jpg|jpeg|png)', html, flags=re.I)
            if m: seatmap = m.group(0)

        promo = find_activity_image_any(html)
        if promo:
            poster = promo
        else:
            for sel in ['meta[property="og:image"]', 'meta[name="twitter:image"]']:
                m = soup.select_one(sel)
                if m and m.get("content"):
                    poster = urljoin(base_url, m["content"]); break
            if poster == LOGO:
                for img in soup.find_all("img"):
                    src = (img.get("src") or "").strip()
                    if src and any(k in src.lower() for k in ("activityimage","azureedge","adimage")):
                        poster = urljoin(base_url, src); break
    except Exception as e:
        app.logger.warning(f"[image] pick failed: {e}")
    return poster, seatmap

def extract_title_place_from_html(html: str) -> tuple[Optional[str], Optional[str], Optional[str]]:
    soup = soup_parse(html)

    title: Optional[str] = None
    place: Optional[str] = None
    dt_text: Optional[str] = None

    for gt in soup.select('.grid-title'):
        lab = gt.get_text(" ", strip=True)
        sib = gt.find_next_sibling()
        if not sib:
            continue
        content = sib.get_text(" ", strip=True)
        if not content:
            continue

        if any(k in lab for k in ("活動名稱", "演出名稱", "節目名稱", "場次名稱")) and not title:
            title = content

        if any(k in lab for k in ("活動地點", "地點", "場地")) and not place:
            place = re.sub(r"\s+", " ", content).strip()

    if not title:
        m = soup.select_one('[id$="_NAME"]')
        if m:
            t = m.get_text(" ", strip=True)
            if t: title = t

    if not title:
        h1 = soup.select_one("h1")
        if h1:
            t = h1.get_text(" ", strip=True)
            if len(t) >= 6: title = t

    if not title:
        mt = soup.select_one('meta[property="og:title"]')
        if mt and mt.get("content"):
            title = mt["content"].strip()

    dt_text = _format_datetime_match(_RE_DATE.search(html)) or dt_text

    return title, place, dt_text

# ============= 票區與 live.map 解析 =============
def extract_area_meta_from_000(html: str) -> Tuple[Dict[str, str], Dict[str, str], Dict[str, int], Dict[str, int], Dict[str, int]]:
    name_map: Dict[str, str] = {}
    status_map: Dict[str, str] = {}
    qty_map: Dict[str, int] = {}
    order_map: Dict[str, int] = {}
    price_map: Dict[str, int] = {}

    soup = soup_parse(html)

    # (a) script jsonData
    for sc in soup.find_all("script"):
        s = sc.string or sc.text or ""
        m = re.search(r"jsonData\s*=\s*'(\[.*?\])'", s, flags=re.S)
        if not m:
            continue
        try:
            arr = json.loads(m.group(1))
            for it in arr:
                code = (it.get("PERFORMANCE_PRICE_AREA_ID") or "").strip()
                name = (it.get("NAME") or "").strip()
                amt  = (it.get("AMOUNT") or "").strip()
                srt  = it.get("SORT")
                if code and name:
                    name_map.setdefault(code, re.sub(r"\s+", "", name))
                if code and amt:
                    status_map.setdefault(code, amt)
                    nums = [int(x) for x in re.findall(r"\d+", amt) if int(x) < 1000]
                    if nums:
                        qty_map.setdefault(code, nums[-1])
                    price_val = _parse_price_value(amt)
                    if price_val is not None:
                        price_map.setdefault(code, price_val)
                if code:
                    price_fields = [
                        it.get("PRICE"),
                        it.get("Price"),
                        it.get("PRICE_TEXT"),
                        it.get("PriceText"),
                    ]
                    for pf in price_fields:
                        price_val = _parse_price_value(pf)
                        if price_val is not None:
                            price_map.setdefault(code, price_val)
                            break
                if code and isinstance(srt, int):
                    order_map.setdefault(code, srt)
        except Exception:
            pass

    # (b) 表格列
    row_idx = 0
    for a in soup.select('a[href*="PERFORMANCE_PRICE_AREA_ID="]'):
        href = a.get("href", "")
        m = re.search(r'PERFORMANCE_PRICE_AREA_ID=([A-Za-z0-9]+)', href)
        if not m:
            continue
        code = m.group(1)
        row_idx += 1
        order_map.setdefault(code, 10000 + row_idx)

        tr = a.find_parent("tr")
        if not tr:
            continue

        tds = [td.get_text(" ", strip=True) for td in tr.find_all("td")]
        if tds:
            if code not in name_map:
                cand = None
                for t in tds:
                    if re.search(r"(樓|區|包廂)", t):
                        cand = t; break
                if not cand:
                    cand = tds[0]
                name_map[code] = re.sub(r"\s+", "", cand)

            status_cell = ""
            for t in reversed(tds):
                if ("已售完" in t) or ("熱賣" in t) or re.search(r"\b\d{1,3}\b", t):
                    status_cell = t
                    break
            if status_cell:
                if code not in status_map:
                    if "已售完" in status_cell:
                        status_map[code] = "已售完"
                    elif "熱賣" in status_cell:
                        status_map[code] = "熱賣中"
                nums = [int(x) for x in re.findall(r"\d+", status_cell) if int(x) < 1000]
                if nums and code not in qty_map:
                    qty_map[code] = nums[-1]
                price_val = _parse_price_value(status_cell)
                if price_val is not None and code not in price_map:
                    price_map[code] = price_val

            if code not in price_map:
                for cell in tds[1:]:
                    price_val = _parse_price_value(cell)
                    if price_val is not None:
                        price_map[code] = price_val
                        break

    return name_map, status_map, qty_map, order_map, price_map

def _parse_livemap_text(txt: str) -> Tuple[Dict[str, int], int]:
    sections: Dict[str, int] = {}
    for tag in _RE_AREA_TAG.findall(txt):
        code = None
        m = re.search(
            r"javascript:Send\([^)]*'(?:B0[0-9A-Z]{6,10})'\s*,\s*'(B0[0-9A-Z]{6,10})'",
            tag, re.I
        )
        if m: code = m.group(1)
        if not code:
            m = re.search(r'(?:data-(?:area|area-id|price-area-id))=["\'](B0[0-9A-Z]{6,10})["\']', tag, re.I)
            if m: code = m.group(1)
        if not code:
            continue

        qty = None
        m = re.search(r'\bdata-(?:left|remain|qty|count)=["\']?(\d{1,3})["\']?', tag, re.I)
        if m:
            qty = int(m.group(1))

        if qty is None:
            text = ""
            m = re.search(r'title="([^"]*)"', tag, re.I)
            if m: text = m.group(1)
            if not text:
                m = re.search(r'(?:alt|aria-label)=["\']([^"\']*)["\']', tag, re.I)
                if m: text = m.group(1)
            if text:
                m = re.search(r'(?:剩餘|尚餘|可售|可購)[^\d]{0,6}(\d{1,3})', text)
                if not m:
                    m = re.search(r'(\d{1,3})\s*張', text)
                if m:
                    qty = int(m.group(1))

        if not qty or qty <= 0 or qty > 500:
            continue

        prev = sections.get(code)
        if prev is None or qty > prev:
            sections[code] = qty

    total = sum(sections.values())
    return sections, total

def try_fetch_livemap_by_perf(perf_id: str, sess: requests.Session, html: Optional[str] = None) -> Tuple[Dict[str, int], int]:
    if not perf_id:
        return {}, 0
    bases = [f"https://qwareticket-asysimg.azureedge.net/QWARE_TICKET/images/Temp/{perf_id}/"]
    if html:
        poster, seatmap = pick_event_images_from_000(html, "https://orders.ibon.com.tw/")
        if seatmap:
            m = re.match(r'(https?://.*/images/[^/]+/)', seatmap)
            if m: bases.insert(0, m.group(1))
    prefixes = ["", "1_", "2_", "3_", "01_", "02_", "03_"]
    tried = set()
    for base in bases:
        for pref in prefixes:
            url = f"{base}{pref}{perf_id}_live.map"
            if url in tried: continue
            tried.add(url)
            try:
                app.logger.info(f"[livemap] try {url}")
                r = sess.get(url, timeout=12)
                if r.status_code == 200:
                    html = _decode_ibon_html(r)
                    if "<area" in html:
                        app.logger.info(f"[livemap] hit {url}")
                        return _parse_livemap_text(html)
            except Exception as e:
                app.logger.info(f"[livemap] miss {url}: {e}")
    return {}, 0

# （可選）進第二步票區頁補抓數字
def fetch_area_left_from_utk0101(base_000_url: str, perf_id: str, product_id: str, area_id: str, sess: requests.Session) -> Optional[int]:
    try:
        url = "https://orders.ibon.com.tw/Application/UTK01/UTK0101_02.aspx"
        params = {
            "PERFORMANCE_ID": perf_id,
            "PERFORMANCE_PRICE_AREA_ID": area_id,
            "PRODUCT_ID": product_id,
            "strItem": "WEB網站入手A1",
        }
        headers = {"Referer": base_000_url, "User-Agent": UA, "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.6"}
        r = sess.get(url, params=params, headers=headers, timeout=12)
        if r.status_code != 200:
            return None
        html = r.text
        m = re.search(r'(?:剩餘|尚餘|可購買|可售)[^\d]{0,6}(\d{1,3})', html)
        if not m:
            m = re.search(r'(\d{1,3})\s*張', html)
        if m:
            return int(m.group(1))

        soup = soup_parse(html)
        qty = None
        for inp in soup.select('input[type="number"],input[name*="QTY" i],select[name*="QTY" i]'):
            for attr in ("max", "data-max", "data-left", "data-remain"):
                v = inp.get(attr)
                if v and str(v).isdigit():
                    qty = max(qty or 0, int(v))
        return qty
    except Exception as e:
        app.logger.info(f"[area-left] fail {area_id}: {e}")
        return None

# --------- 主要解析器 ---------
def parse_UTK0201_000(url: str, sess: requests.Session, referer: Optional[str] = None) -> dict:
    out = {"ok": False, "sig": "NA", "url": url, "image": LOGO}
    headers = {
        "User-Agent": UA,
        "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.6",
    }
    headers["Referer"] = referer or url
    r = sess.get(url, headers=headers, timeout=15)
    if r.status_code != 200:
        out["msg"] = f"讀取失敗（HTTP {r.status_code}）"
        return out
    html = _decode_ibon_html(r)

    q = parse_qs(urlparse(url).query)
    perf_id = (q.get("PERFORMANCE_ID") or [None])[0]
    product_id = (q.get("PRODUCT_ID") or [None])[0]

    # 圖片
    poster_from_000, seatmap = pick_event_images_from_000(html, url)
    if seatmap: out["seatmap"] = seatmap

    # 活動基本資訊
    api_info: Dict[str, str] = {}
    try:
        api_info = fetch_game_info_from_api(perf_id, product_id, url, sess)
    except Exception as e:
        app.logger.info(f"[api] fail: {e}")

    html_title, html_place, html_dt = extract_title_place_from_html(html)

    html_details = find_details_url_candidates_from_html(html, url)
    details_url = (
        (html_details[0] if html_details else None)
        or api_info.get("details")
        or (PROMO_DETAILS_MAP.get(perf_id) if perf_id else None)
    )
    details_info: Dict[str, str] = {}
    if details_url:
        details_info = fetch_from_ticket_details(details_url, sess)

    chosen_img = (
        (PROMO_IMAGE_MAP.get(perf_id) if perf_id else None)
        or details_info.get("poster")
        or api_info.get("poster")
        or poster_from_000
        or LOGO
    )
    if not _url_ok(chosen_img):
        app.logger.info(f"[image] chosen invalid, fallback: {chosen_img}")
        chosen_img = seatmap if seatmap and _url_ok(seatmap) else LOGO
    out["image"] = chosen_img

    out["title"] = details_info.get("title") or api_info.get("title") or html_title or "（未取到標題）"
    out["place"] = details_info.get("place") or api_info.get("place") or html_place or "（未取到場地）"
    out["date"]  = details_info.get("dt")    or api_info.get("dt")    or html_dt    or "（未取到日期）"

    # 票區中文名 + 狀態（AMOUNT）+ 順序
    area_name_map, area_status_map, area_qty_map, area_order_map, area_price_map = extract_area_meta_from_000(html)
    out["area_names"] = area_name_map

    # live.map 數字（僅取可信數字，且同一區取最大值）
    sections_by_code, _ = try_fetch_livemap_by_perf(perf_id, sess, html=html)
    numeric_counts: Dict[str, int] = dict(sections_by_code)
    for code, n in area_qty_map.items():
        if isinstance(n, int) and n > 0 and code not in numeric_counts:
            numeric_counts[code] = n

    selling_unknown_codes: List[str] = []
    for code, status in area_status_map.items():
        if (status and ("熱賣" in status or "可售" in status)) and not numeric_counts.get(code):
            selling_unknown_codes.append(code)

    if FOLLOW_AREAS_PER_CHECK > 0 and perf_id and product_id and area_name_map:
        need_follow = [code for code, st in area_status_map.items()
                       if (st and "熱賣" in st) and (code not in numeric_counts)]
        for code in need_follow[:FOLLOW_AREAS_PER_CHECK]:
            n = fetch_area_left_from_utk0101(url, perf_id, product_id, code, sess)
            if isinstance(n, int) and n > 0:
                numeric_counts[code] = n

    selling_unknown_codes = [
        code for code, amt in area_status_map.items()
        if (amt and ("熱賣" in amt or "可售" in amt)) and not numeric_counts.get(code)
    ]

    human_numeric: Dict[str, int] = {}
    for code, n in numeric_counts.items():
        name = area_name_map.get(code, code)
        v = int(n)
        human_numeric[name] = max(human_numeric.get(name, 0), v)

    def order_key(name: str) -> tuple:
        codes = [c for c, nm in area_name_map.items() if nm == name]
        order_vals = [area_order_map.get(c, 99999) for c in codes] or [99999]
        return (min(order_vals), name)

    ordered_names = sorted(human_numeric.keys(), key=order_key)
    selling_names = sorted({area_name_map.get(code, code) for code in selling_unknown_codes}, key=order_key)

    total_num = sum(human_numeric.values())

    sold_out = False
    if area_name_map:
        any_hot = any(("熱賣" in s) or ("可售" in s) or ("可購" in s) for s in area_status_map.values())
        any_num = any(v > 0 for v in numeric_counts.values())
        if not any_hot and not any_num and area_status_map:
            sold_out = all(("已售完" in area_status_map.get(code, "")) for code in area_name_map.keys())

    out["sections"] = human_numeric
    out["sections_order"] = ordered_names
    out["selling"] = selling_names
    out["total"] = total_num
    out["soldout"] = bool(sold_out)

    ticket_map: Dict[tuple, Dict[str, Any]] = {}
    for code, area_name in area_name_map.items():
        area = area_name or code
        price_val = area_price_map.get(code)
        if price_val is None:
            price_val = _parse_price_value(area_status_map.get(code))
        price_int: Optional[int]
        if isinstance(price_val, int):
            price_int = price_val
        elif isinstance(price_val, str):
            price_int = _parse_price_value(price_val)
        else:
            price_int = price_val if isinstance(price_val, int) else None
        if price_int is None:
            price_int = 0
        remaining_val = numeric_counts.get(code)
        if remaining_val is None:
            status_text = area_status_map.get(code, "")
            if status_text and any(kw in status_text for kw in ("售完", "完售")):
                remaining_val = 0
            else:
                remaining_val = 0
        key = (area, int(price_int))
        rem_int = int(max(0, remaining_val or 0))
        entry = ticket_map.get(key)
        order_val = area_order_map.get(code, 99999)
        if entry:
            entry["remaining"] = max(entry["remaining"], rem_int)
            entry["_order"] = min(entry["_order"], order_val)
        else:
            ticket_map[key] = {
                "area": area,
                "price": int(price_int),
                "remaining": rem_int,
                "_order": order_val,
            }

    for code, count in numeric_counts.items():
        if code in area_name_map:
            continue
        area = code
        key = (area, 0)
        rem_int = int(max(0, count or 0))
        entry = ticket_map.get(key)
        if entry:
            entry["remaining"] = max(entry["remaining"], rem_int)
        else:
            ticket_map[key] = {"area": area, "price": 0, "remaining": rem_int, "_order": area_order_map.get(code, 99999)}

    tickets = sorted(ticket_map.values(), key=lambda t: (t.get("_order", 99999), t["area"], t.get("price", 0)))
    for t in tickets:
        t.pop("_order", None)
    out["tickets"] = tickets

    sig_base = hash_state(human_numeric, selling_names)
    out["sig"] = hashlib.md5((sig_base + ("|SO" if sold_out else "")).encode("utf-8")).hexdigest()

    out["ok"] = (total_num > 0) or bool(selling_names)

    if out["ok"]:
        lines = [f"🎫 {out['title']}",
                 f"地點：{out['place']}",
                 f"日期：{out['date']}",
                 ""]
        if total_num > 0:
            lines.append("✅ 監看結果：目前可售")
            for name in ordered_names:
                lines.append(f"{name}: {human_numeric[name]} 張")
            lines.append(f"合計：{total_num} 張")
        if selling_names:
            if total_num > 0:
                lines.append("")  # 分段
            lines.append("🟢 目前熱賣中（數量未公開）：")
            for n in selling_names:
                lines.append(f"・{n}（熱賣中）")
        lines.append(out["url"])
        out["msg"] = "\n".join(lines)
        return out

    if sold_out:
        out["msg"] = (
            f"🎫 {out['title']}\n"
            f"📍地點：{out['place']}\n"
            f"📅日期：{out['date']}\n\n"
            f"🔴 全區已售完\n"
            f"{url}"
        )
        return out

    out["msg"] = (
        f"🎫 {out['title']}\n"
        f"地點：{out['place']}\n"
        f"日期：{out['date']}\n\n"
        "暫時讀不到剩餘數（可能為動態載入）。\n"
        f"{url}"
    )
    return out

def _probe_activity_details(url: str, sess: requests.Session, trace: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
    start_details = time.time()
    details_info = fetch_from_ticket_details(url, sess)
    elapsed_details = int((time.time() - start_details) * 1000)
    details_dict = details_info if isinstance(details_info, dict) else {}
    if trace is not None:
        trace.append({
            "phase": "details",
            "ok": bool(details_dict),
            "elapsed_ms": elapsed_details,
            "count": len(details_dict.get("ticket_urls", [])),
        })

    start_api = time.time()
    api_info = fetch_game_info_from_api(None, None, url, sess)
    elapsed_api = int((time.time() - start_api) * 1000)
    api_dict = api_info if isinstance(api_info, dict) else {}
    if trace is not None:
        trace.append({
            "phase": "html_parse",
            "ok": True,
            "elapsed_ms": elapsed_api,
            "count": len(details_dict.get("ticket_urls", [])),
        })

    base_title = details_dict.get("title") or api_dict.get("title") or "（未取到標題）"
    base_place = details_dict.get("venue") or details_dict.get("place") or api_dict.get("place") or ""
    base_dt = details_dict.get("date") or details_dict.get("dt") or api_dict.get("dt") or ""
    base_img = (
        details_dict.get("image_url")
        or details_dict.get("poster")
        or api_dict.get("poster")
        or LOGO
    )
    base_address = details_dict.get("address") or api_dict.get("address") or ""

    details_url_clean = details_dict.get("details_url") or sanitize_details_url(url)
    pattern = details_dict.get("pattern")
    if not pattern:
        qs = parse_qs(urlparse(details_url_clean).query)
        pattern = (qs.get("pattern", ["ENTERTAINMENT"])[0] or "ENTERTAINMENT").strip()

    activity_id = details_dict.get("activity_id") or _activity_id_from_url(details_url_clean) or _activity_id_from_url(url)

    ticket_candidates: List[str] = []
    for source in (
        details_dict.get("ticket_urls"),
        api_dict.get("ticket_urls"),
    ):
        if not source:
            continue
        for t in source:
            if t not in ticket_candidates:
                ticket_candidates.append(t)

    resolved_ticket = _resolve_utk_url(activity_id, pattern, sess, details_url_clean, trace=trace)
    if resolved_ticket and resolved_ticket not in ticket_candidates:
        ticket_candidates.insert(0, resolved_ticket)

    result_base = {
        "ok": bool(base_title),
        "sig": "NA",
        "url": details_url_clean,
        "details_url": details_url_clean,
        "image": base_img,
        "image_url": base_img,
        "title": base_title,
        "place": base_place,
        "venue": base_place,
        "date": base_dt,
        "address": base_address,
        "msg": details_url_clean,
        "tickets": [],
        "pattern": pattern,
    }

    if activity_id:
        result_base["activity_id"] = activity_id

        if ticket_candidates:
            for ticket_url in ticket_candidates:
                start_parse = time.time()
                try:

                    parsed = parse_UTK0201_000(ticket_url, sess, referer=details_url_clean)

                except Exception as e:
                    if trace is not None:
                        trace.append({
                            "phase": "utk_parse",
                            "ok": False,
                            "url": ticket_url,
                            "elapsed_ms": int((time.time() - start_parse) * 1000),
                            "reason": str(e),
                        })
                    app.logger.info(f"[probe] parse ticket fail {ticket_url}: {e}")
                    continue
                if not isinstance(parsed, dict):
                    if trace is not None:
                        trace.append({
                            "phase": "utk_parse",
                            "ok": False,
                            "url": ticket_url,
                            "elapsed_ms": int((time.time() - start_parse) * 1000),
                            "reason": "invalid-response",
                        })
                    continue

                parsed.setdefault("details_url", details_url_clean)
                parsed["ticket_url"] = ticket_url
                parsed["url"] = details_url_clean
                parsed["image"] = base_img or parsed.get("image", LOGO)
                parsed["image_url"] = parsed.get("image")

                if base_title and not base_title.startswith("（未取到"):
                    parsed["title"] = base_title
                else:
                    parsed["title"] = parsed.get("title") or base_title

                if base_place:
                    parsed["place"] = base_place
                    parsed["venue"] = base_place
                else:
                    parsed["place"] = parsed.get("place", "")
                    parsed["venue"] = parsed.get("venue", parsed.get("place", ""))

                if base_dt:
                    parsed["date"] = base_dt
                else:
                    parsed["date"] = parsed.get("date", "")

                if activity_id:
                    parsed.setdefault("activity_id", activity_id)

                if base_address:
                    parsed.setdefault("address", base_address)

                parsed.setdefault("pattern", pattern)

                if trace is not None:
                    parsed_tickets = parsed.get("tickets") or []
                    trace.append({
                        "phase": "utk_parse",
                        "ok": True,
                        "url": ticket_url,
                        "elapsed_ms": int((time.time() - start_parse) * 1000),
                        "count": len(parsed_tickets),
                    })

                if parsed.get("tickets"):
                    try:
                        total_remaining = sum(
                            max(0, int(t.get("remaining", 0)))
                            for t in parsed.get("tickets", [])
                            if isinstance(t, dict)
                        )
                    except Exception:
                        total_remaining = 0
                    if total_remaining:
                        parsed["remain"] = total_remaining
                        parsed["remaining"] = total_remaining

                return parsed

    if ticket_candidates:
        result_base["ticket_urls"] = ticket_candidates
    try:
        total_remaining = sum(
            max(0, int(t.get("remaining", 0)))
            for t in result_base.get("tickets", [])
            if isinstance(t, dict)
        )
    except Exception:
        total_remaining = 0
    if total_remaining:
        result_base["remain"] = total_remaining
        result_base["remaining"] = total_remaining
    return result_base


def probe(url: str) -> dict:
    s = sess_default()
    p = urlparse(url)
    if "orders.ibon.com.tw" in p.netloc and p.path.upper().endswith("/UTK0201_000.ASPX"):
        return parse_UTK0201_000(url, s)
    if "ticket.ibon.com.tw" in p.netloc and "/ActivityInfo/Details" in p.path:
        return _probe_activity_details(url, s)

    r = s.get(url, timeout=12)
    if r.apparent_encoding:
        r.encoding = r.apparent_encoding
    title = ""
    try:
        soup = soup_parse(r.text)
        if soup.title and soup.title.text:
            title = soup.title.text.strip()
    except Exception:
        pass
    return {
        "ok": False, "sig": "NA", "url": url, "image": LOGO,
        "title": title or "（未取到標題）", "place": "", "date": "", "msg": url,
    }

# ============= LINE 指令 =============
HELP = (
    "親愛的用戶您好 🖐️\n"
    "歡迎來到巴拉圭の專屬搶票助手 🤗\n"
    "我是您的票券監看機器人 🤖\n\n"
    "您可以使用以下指令來進行操作： 👇\n\n"
    "➊/start 或 /help － 顯示操作說明\n"
    "➋/watch <URL> [秒] － 開始監看（最小 15 秒）\n"
    "➌/unwatch <任務ID> － 停用任務\n"
    "➍/list － 顯示啟用中任務（/list all 看全部、/list off 看停用）\n"
    "➎/check <URL|任務ID> － 立刻手動查詢該頁剩餘數\n"
    "➏/probe <URL> － 回傳診斷 JSON（除錯用）\n\n"
    "➐ibon售票網站首頁連結:https://ticket.ibon.com.tw/Index/entertainment \n"
    "將用戶想追蹤的ibon售票網站連結貼入<URL>欄位即可 \n"
    "🤖任務ID會在用戶輸入/watch開始監看後生成一個六位數的代碼 🤖\n"
)
WELCOME_TEXT = HELP

CMD_PREFIX = ("/", "／")
def is_command(text: Optional[str]) -> bool:
    if not text:
        return False
    return text.strip().startswith(CMD_PREFIX)

def source_id(ev):
    src = ev.source
    return getattr(src, "user_id", None) or getattr(src, "group_id", None) or getattr(src, "room_id", None) or ""

def make_task_id() -> str:
    return uuid.uuid4().hex[:6]

def fs_get_task_by_canon(chat_id: str, url_canon: str):
    if not FS_OK: return None
    q = (fs_client.collection(COL)
         .where("chat_id", "==", chat_id)
         .where("url_canon", "==", url_canon)
         .limit(1).stream())
    for d in q: return d
    return None

def fs_get_task_by_id(chat_id: str, tid: str):
    if not FS_OK: return None
    q = (fs_client.collection(COL)
         .where("chat_id", "==", chat_id)
         .where("id", "==", tid)
         .limit(1).stream())
    for d in q: return d
    return None

def fs_upsert_watch(chat_id: str, url: str, sec: int):
    if not FS_OK:
        raise RuntimeError("Firestore not available")
    url_c = canonicalize_url(url)
    sec = max(15, int(sec))
    now = datetime.now(timezone.utc)
    doc = fs_get_task_by_canon(chat_id, url_c)
    if doc:
        fs_client.collection(COL).document(doc.id).update({
            "period": sec, "enabled": True, "updated_at": now,
        })
        return doc.to_dict()["id"], False
    tid = make_task_id()
    fs_client.collection(COL).add({
        "id": tid, "chat_id": chat_id, "url": url, "url_canon": url_c,
        "period": sec, "enabled": True, "created_at": now, "updated_at": now,
        "last_sig": "", "last_total": 0, "last_ok": False, "next_run_at": now,
    })
    return tid, True

def fs_list(chat_id: str, show: str = "on"):
    if not FS_OK:
        return []

    base = fs_client.collection(COL).where("chat_id", "==", chat_id)
    if show == "on":
        base = base.where("enabled", "==", True)
    elif show == "off":
        base = base.where("enabled", "==", False)

    try:
        cur = base.order_by("updated_at", direction=firestore.Query.DESCENDING).stream()
        rows = []
        for d in cur:
            rows.append(d.to_dict())
        return rows
    except Exception as e:
        app.logger.info(f"[fs_list] order_by stream failed, fallback to unsorted: {e}")

    try:
        rows = [d.to_dict() for d in base.stream()]
        def _k(x):
            v = x.get("updated_at")
            return v if v is not None else datetime.fromtimestamp(0, timezone.utc)
        rows.sort(key=_k, reverse=True)
        return rows
    except Exception as e2:
        app.logger.error(f"[fs_list] fallback stream failed: {e2}")
        return []

def fs_disable(chat_id: str, tid: str) -> bool:
    doc = fs_get_task_by_id(chat_id, tid)
    if not doc: return False
    fs_client.collection(COL).document(doc.id).update({
        "enabled": False, "updated_at": datetime.now(timezone.utc),
    })
    return True

def fmt_result_text(res: dict) -> str:
    lines = []
    if res.get("task_id"):
        lines.append(f"任務代碼：{res['task_id']}")
    lines += [
        f"🎫 {res.get('title','')}".strip(),
        f"📍地點：{res.get('place','')}",
        f"📅日期：{res.get('date','')}",
    ]
    if res.get("soldout"):
        lines.append("\n🔴 全區已售完")
        lines.append(res.get("url", ""))
        return "\n".join(lines)

    if res.get("ok"):
        secs = res.get("sections", {})
        order = res.get("sections_order") or []
        selling = res.get("selling", [])
        if secs:
            lines.append("\n✅ 監看結果：目前可售")
            if order:
                for name in order:
                    if name in secs:
                        lines.append(f"{name}: {secs[name]} 張")
            else:
                for k, v in sorted(secs.items(), key=lambda x: (-x[1], x[0])):
                    lines.append(f"{k}: {v} 張")
            lines.append(f"合計：{res.get('total',0)} 張")
        if selling:
            lines.append("\n🟢 目前熱賣中（數量未公開）：")
            for n in selling:
                lines.append(f"・{n}（熱賣中）")
    else:
        lines.append("\n暫時讀不到剩餘數（可能為動態載入）。")
    lines.append(res.get("url", ""))
    return "\n".join(lines)

def handle_command(text: str, chat_id: str):
    try:
        parts = text.strip().split()
        cmd = parts[0].lower()
        if cmd in ("/start", "/help"):
            return [TextSendMessage(text=HELP)] if HAS_LINE else [HELP]

        if cmd == "/watch" and len(parts) >= 2:
            url = parts[1].strip()
            sec = int(parts[2]) if len(parts) >= 3 and parts[2].isdigit() else DEFAULT_PERIOD_SEC
            tid, created = fs_upsert_watch(chat_id, url, sec)
            status = "啟用" if created else "更新"
            msg = f"你的任務：\n{tid}｜{status}｜{sec}s\n{canonicalize_url(url)}"
            return [TextSendMessage(text=msg)] if HAS_LINE else [msg]

        if cmd == "/unwatch" and len(parts) >= 2:
            ok = fs_disable(chat_id, parts[1].strip())
            msg = "已停用" if ok else "找不到該任務"
            return [TextSendMessage(text=msg)] if HAS_LINE else [msg]

        if cmd == "/list":
            try:
                mode = "on"
                if len(parts) >= 2 and parts[1].lower() in ("all", "off"):
                    mode = parts[1].lower()

                rows = fs_list(chat_id, show=mode)
                if not rows:
                    out = "（沒有任務）"
                    return [TextSendMessage(text=out)] if HAS_LINE else [out]

                chunks = []
                buf = "你的任務：\n"
                for r in rows:
                    try:
                        rid    = str(r.get("id", "?"))
                        state  = "啟用" if r.get("enabled") else "停用"
                        period = str(r.get("period", "?"))
                        u      = str(r.get("url", ""))
                        line = f"{rid}｜{state}｜{period}s\n{u}\n\n"
                    except Exception as e:
                        app.logger.info(f"[list] format row fail: {e}; row={r}")
                        line = f"{r}\n\n"

                    if len(buf) + len(line) > 4800:
                        chunks.append(buf.rstrip())
                        buf = ""
                    buf += line
                if buf:
                    chunks.append(buf.rstrip())

                if HAS_LINE:
                    to_reply = chunks[:5]
                    to_push  = chunks[5:]
                    msgs = [TextSendMessage(text=c) for c in to_reply]
                    for c in to_push:
                        try:
                            send_text(chat_id, c)
                        except Exception as e:
                            app.logger.error(f"[list] push remainder failed: {e}")
                    return msgs
                else:
                    return chunks

            except Exception as e:
                app.logger.error(f"/list failed: {e}\n{traceback.format_exc()}")
                out = "（讀取任務清單時發生例外）"
                return [TextSendMessage(text=out)] if HAS_LINE else [out]

        if cmd == "/check" and len(parts) >= 2:
            target = parts[1].strip()
            tid_for_msg = None
            if target.lower().startswith("http"):
                url = target
            else:
                doc = fs_get_task_by_id(chat_id, target)
                if not doc:
                    msg = "找不到該任務 ID"
                    return [TextSendMessage(text=msg)] if HAS_LINE else [msg]
                url = doc.to_dict().get("url")
                tid_for_msg = target

            res = probe(url)
            if tid_for_msg:
                res["task_id"] = tid_for_msg

            if HAS_LINE:
                msgs = []
                sent = set()
                sm  = res.get("seatmap")
                img = res.get("image")
                if sm and _url_ok(sm):
                    msgs.append(ImageSendMessage(original_content_url=sm, preview_image_url=sm))
                    sent.add(sm)
                if img and _url_ok(img) and img not in sent:
                    msgs.append(ImageSendMessage(original_content_url=img, preview_image_url=img))
                msgs.append(TextSendMessage(text=fmt_result_text(res)))
                return msgs
            else:
                return [fmt_result_text(res)]

        if cmd == "/probe" and len(parts) >= 2:
            url = parts[1].strip()
            res = probe(url)
            out = json.dumps(res, ensure_ascii=False)
            return [TextSendMessage(text=out)] if HAS_LINE else [out]

        return [TextSendMessage(text=HELP)] if HAS_LINE else [HELP]
    except Exception as e:
        app.logger.error(f"handle_command error: {e}\n{traceback.format_exc()}")
        msg = "指令處理發生錯誤，請稍後再試。"
        return [TextSendMessage(text=msg)] if HAS_LINE else [msg]

# ============= Webhook / Scheduler / Diag =============

@app.post("/webhook")
@app.post("/line/webhook")
@app.post("/callback")

def webhook():
    if not (HAS_LINE and handler):
        app.logger.warning("Webhook invoked but handler not ready")
        return jsonify({"ok": True}), 200
    signature = request.headers.get("X-Line-Signature", "")
    body = request.get_data(as_text=True)

    def _handle_async():
        try:
            handler.handle(body, signature)
        except InvalidSignatureError:
            app.logger.warning("InvalidSignature on /webhook")
        except Exception as exc:
            app.logger.exception(f"/webhook handler error: {exc}")

    try:
        threading.Thread(target=_handle_async, daemon=True).start()
    except Exception as exc:
        app.logger.exception(f"/webhook thread error: {exc}")
        return jsonify({"ok": False}), 200
    return jsonify({"ok": True}), 200

if HAS_LINE and handler:

    @handler.add(FollowEvent)
    def on_follow(ev):
        try:
            line_bot_api.reply_message(ev.reply_token, [TextSendMessage(text=WELCOME_TEXT)])
        except Exception as e:
            app.logger.error(f"[follow] reply failed: {e}")

    @handler.add(JoinEvent)
    def on_join(ev):
        try:
            line_bot_api.reply_message(ev.reply_token, [TextSendMessage(text=WELCOME_TEXT)])
        except Exception as e:
            app.logger.error(f"[join] reply failed: {e}")

    @handler.add(MessageEvent, message=TextMessage)
    def on_message(ev):
        raw = getattr(ev.message, "text", "") or ""
        text = raw.strip()
        if not is_command(text):
            app.logger.info(f"[IGNORED NON-COMMAND] chat={source_id(ev)} text={text!r}")
            return

        chat = source_id(ev)
        msgs = handle_command(text, chat)
        if isinstance(msgs, list) and msgs and not isinstance(msgs[0], str):
            line_bot_api.reply_message(ev.reply_token, msgs)
        else:
            line_bot_api.reply_message(ev.reply_token, [TextSendMessage(text=str(msgs))])

@app.route("/cron/tick", methods=["GET"])
def cron_tick():
    start = time.time()
    resp = {"ok": True, "processed": 0, "skipped": 0, "errors": []}
    try:
        if not FS_OK:
            resp["ok"] = False
            resp["errors"].append("No Firestore client")
            return jsonify(resp), 200

        now = datetime.now(timezone.utc)

        try:
            docs = list(fs_client.collection(COL).where("enabled", "==", True).stream())
        except Exception as e:
            app.logger.error(f"[tick] list watchers failed: {e}")
            resp["ok"] = False
            resp["errors"].append(f"list failed: {e}")
            return jsonify(resp), 200

        handled = 0
        for d in docs:
            if (time.time() - start) > TICK_SOFT_DEADLINE_SEC:
                resp["errors"].append("soft-deadline reached; remaining will run next tick")
                break
            if handled >= MAX_PER_TICK:
                resp["errors"].append("max-per-tick reached; remaining will run next tick")
                break

            r = d.to_dict()
            period = int(r.get("period", DEFAULT_PERIOD_SEC))
            next_run_at = r.get("next_run_at") or (now - timedelta(seconds=1))
            if now < next_run_at:
                resp["skipped"] += 1
                continue

            url = r.get("url")
            try:
                res = probe(url)
            except Exception as e:
                app.logger.error(f"[tick] probe error for {url}: {e}")
                res = {"ok": False, "msg": f"probe error: {e}", "sig": "NA", "url": url}

            try:
                fs_client.collection(COL).document(d.id).update({
                    "last_sig": res.get("sig", "NA"),
                    "last_total": res.get("total", 0),
                    "last_ok": bool(res.get("ok", False)),
                    "updated_at": now,
                    "next_run_at": now + timedelta(seconds=period),
                })
            except Exception as e:
                app.logger.error(f"[tick] update doc error: {e}")
                resp["errors"].append(f"update error: {e}")

            changed = (res.get("sig", "NA") != r.get("last_sig", ""))
            if ALWAYS_NOTIFY or changed:
                try:
                    res["task_id"] = r.get("id")
                    chat_id = r.get("chat_id")
                    sent = set()
                    sm = res.get("seatmap")
                    img = res.get("image")
                    if sm and _url_ok(sm):
                        send_image(chat_id, sm); sent.add(sm)
                    if img and _url_ok(img) and img not in sent:
                        send_image(chat_id, img)
                    send_text(chat_id, fmt_result_text(res))
                except Exception as e:
                    app.logger.error(f"[tick] notify error: {e}")
                    resp["errors"].append(f"notify error: {e}")

            handled += 1
            resp["processed"] += 1

        app.logger.info(f"[tick] processed={resp['processed']} skipped={resp['skipped']} "
                        f"errors={len(resp['errors'])} duration={time.time()-start:.1f}s")
        return jsonify(resp), 200

    except Exception as e:
        app.logger.error(f"[tick] fatal: {e}\n{traceback.format_exc()}")
        resp["ok"] = False
        resp["errors"].append(str(e))
        return jsonify(resp), 200

@app.route("/diag", methods=["GET"])
def diag():
    url = request.args.get("url", "").strip()
    if not url:
        return jsonify({"ok": False, "msg": "missing url"}), 400
    try:
        res = probe(url)
        return jsonify(res), 200
    except Exception as e:
        return jsonify({"ok": False, "msg": str(e)}), 500


@app.get("/diag/routes")
def diag_routes():
    rules = []
    for rule in app.url_map.iter_rules():
        methods = sorted(m for m in rule.methods if m not in {"HEAD", "OPTIONS"})
        rules.append({
            "rule": rule.rule,
            "methods": methods,
            "endpoint": rule.endpoint,
        })
    return jsonify({"ok": True, "routes": rules}), 200


@app.route("/healthz", methods=["GET"])
def healthz():
    return "ok", 200

@app.route("/check", methods=["GET"])
def http_check_once():
    url = request.args.get("url", "").strip()
    if not url:
        return jsonify({"ok": False, "msg": "provide ?url=<UTK0201_000 url>"}), 400
    res = probe(url)
    return jsonify(res), 200

# ====== Entertainment helpers & LIFF API ======

def fetch_ibon_ent_html_hard(limit=10, keyword=None, only_concert=False):
    """
    超寬鬆 HTML 兜底版本：
    - 先抓到所有 /ActivityInfo/Details/<id>
    - 盡量從近鄰元素、img alt、title、strong/h3 取標題
    - 標題拿不到時，用「活動」；圖片拿不到時給 None
    - 永遠回傳 list
    """
    url = IBON_ENT_URL
    s = requests.Session()
    s.headers.update({
        "User-Agent": UA,
        "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.6",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Connection": "close",
    })
    items: List[Dict[str, Any]] = []
    seen: set = set()

    try:
        r = s.get(url, timeout=15)
        r.raise_for_status()
        html = _decode_ibon_html(r)
        soup = soup_parse(html)

        # 先把所有 Details 連結撿出來
        all_details = _extract_details_any(html)

        def _pick_title_from_node(node) -> Optional[str]:
            # 1) node 本身的 title 屬性
            t = (node.get("title") or "").strip() if hasattr(node, "get") else ""
            if t: return t
            # 2) 近鄰的 img[alt]
            img = None
            try:
                img = node.find("img") if hasattr(node, "find") else None
            except Exception:
                img = None
            if img and (img.get("alt") or "").strip():
                return img.get("alt").strip()
            # 3) 近鄰的 strong/h3/span 文字
            for sel in ("strong", "h3", ".title", ".txt", "span"):
                try:
                    cand = node.select_one(sel) if hasattr(node, "select_one") else None
                    if cand:
                        txt = cand.get_text(" ", strip=True)
                        if txt and len(txt) >= 2:
                            return txt
                except Exception:
                    pass
            # 4) a 本身文字
            try:
                tx = node.get_text(" ", strip=True) if hasattr(node, "get_text") else ""
                if tx and len(tx) >= 2:
                    return tx
            except Exception:
                pass
            return None

        # 先嘗試用 DOM 找「卡片」
        try:
            card_nodes = soup.select('.owl-item, .item, .swiper-slide, .card, .banner, .list, a[href*="ActivityInfo/Details"]')
        except Exception:
            card_nodes = []

        for nd in card_nodes:
            try:
                # 試從卡片內找 Details
                href = None
                atag = nd.select_one('a[href*="ActivityInfo/Details"]')
                if atag and atag.get("href"):
                    href = urljoin(IBON_BASE, atag["href"].strip())
                # 沒有就跳過
                if not href:
                    continue
                if href in seen:
                    continue

                title = _pick_title_from_node(nd) or "活動"
                if keyword and keyword not in title:
                    continue
                if only_concert and not _looks_like_concert(title):
                    continue

                # 圖片：src / data-src / data-original
                img_url = None
                img = nd.find("img")
                if img:
                    for k in ("src", "data-src", "data-original", "data-lazy"):
                        v = (img.get(k) or "").strip()
                        if v:
                            img_url = urljoin(IBON_BASE, v)
                            break

                clean = sanitize_details_url(href)
                items.append({
                    "title": title,
                    "url": clean,
                    "details_url": clean,
                    "image": img_url,
                    "image_url": img_url,
                })
                seen.add(href)
                if len(items) >= max(1, int(limit)):
                    return items
            except Exception:
                continue

        # 如果上面的卡片法抓不到，退而求其次：用所有 Details 列表配對標題
        for href in all_details:
            if href in seen:
                continue
            # 在 HTML 內找這個 href 出現附近的文字當標題
            title = None
            try:
                # 取出 href 周邊 300 字元尋找候選文字
                m = re.search(re.escape(href), html)
                if m:
                    start = max(0, m.start() - 300)
                    end   = min(len(html), m.end() + 300)
                    blob  = html[start:end]
                    # title 屬性
                    mt = re.search(r'title\s*=\s*"([^"]{2,})"', blob)
                    if mt: title = mt.group(1).strip()
                    # strong/h3
                    if not title:
                        mt = re.search(r'(?is)<(?:strong|h3)[^>]*>\s*([^<]{2,})\s*</(?:strong|h3)>', blob)
                        if mt: title = mt.group(1).strip()
                    # img alt
                    if not title:
                        mt = re.search(r'(?is)<img[^>]*\balt\s*=\s*"([^"]{2,})"', blob)
                        if mt: title = mt.group(1).strip()
            except Exception:
                pass

            title = title or "活動"
            if keyword and keyword not in title:
                continue
            if only_concert and not _looks_like_concert(title):
                continue

            clean = sanitize_details_url(href)
            items.append({"title": title, "url": clean, "details_url": clean, "image": None, "image_url": None})
            seen.add(href)
            if len(items) >= max(1, int(limit)):
                break

        return items

    except Exception as e:
        app.logger.error(f"[html_hard] failed: {e}")
        return []

# --- backward-compat alias（舊名→新實作；一定要放在函式「外面」） ---
def fetch_ibon_entertainments(limit=10, keyword=None, only_concert=False):
    items = fetch_ibon_list_via_api(limit=limit, keyword=keyword, only_concert=only_concert)
    if items:
        return items
    return fetch_ibon_ent_html_hard(limit=limit, keyword=keyword, only_concert=only_concert)

def _truthy(v: Optional[str]) -> bool:
    if v is None:
        return False
    s = str(v).strip().lower()
    return s in ("1", "true", "t", "yes", "y")

# 簡單保險絲（30 分鐘）
_API_BREAK_UNTIL = 0

def fetch_ibon_carousel_from_api(limit=10, keyword=None, only_concert=False):
    global _API_BREAK_UNTIL
    now = time.time()
    if now < _API_BREAK_UNTIL:
        app.logger.info("[carousel-api] breaker open -> skip API, go HTML")
        return []

    session, token = _prepare_ibon_session()
    if session is None:
        return []

    headers = {
        "Origin": "https://ticket.ibon.com.tw",
        "Referer": IBON_ENT_URL,
        "X-Requested-With": "XMLHttpRequest",
    }
    if token:
        headers["X-XSRF-TOKEN"] = token

    kw = (keyword or "").strip()
    kw_lower = kw.lower()
    max_items = max(1, int(limit))
    items: List[Dict[str, Optional[str]]] = []
    seen_urls: set[str] = set()

    def _should_keep(title: str) -> bool:
        if kw and kw_lower not in title.lower():
            return False
        if only_concert and not _looks_like_concert(title):
            return False
        return True

    def _try_append(raw: Dict[str, Any]) -> bool:
        if not isinstance(raw, dict):
            return False
        try:
            item = _normalize_item(raw)
        except Exception:
            return False

        url = item.get("url")
        if not url:
            return False

        canon = canonicalize_url(url)
        if canon in seen_urls:
            return False

        title = item.get("title", "")
        if not _should_keep(title):
            return False

        seen_urls.add(canon)
        items.append(item)
        return len(items) >= max_items

    def _parse_activity_list(val: Any) -> List[Dict[str, Any]]:
        if isinstance(val, list):
            return [it for it in val if isinstance(it, (dict, str, int))]
        if isinstance(val, str) and val.strip():
            try:
                parsed = json.loads(val)
                if isinstance(parsed, list):
                    return [it for it in parsed if isinstance(it, (dict, str, int))]
            except Exception:
                pass
            return [{"ActivityID": x.strip()} for x in val.split(",") if x.strip()]
        return []

    patterns = ["ENTERTAINMENT", "CONCERT"]

    for pattern in patterns:
        try:
            resp = session.post(
                IBON_API,
                data={"pattern": pattern},
                headers=headers,
                timeout=12,
            )
        except Exception as e:
            app.logger.warning(f"[carousel-api] POST err {e}")
            _API_BREAK_UNTIL = time.time() + 1800
            return []

        if resp.status_code == 200:
            try:
                payload = resp.json()
            except Exception:
                payload = {}

            container: Any = payload.get("Item") if isinstance(payload, dict) else payload
            if not isinstance(container, dict):
                container = {}

            base_list = _as_list(container.get("List"))
            activity_by_id: Dict[str, Dict[str, Any]] = {}
            for raw in base_list:
                if isinstance(raw, dict):
                    act_id = (
                        raw.get("ActivityID")
                        or raw.get("ActivityId")
                        or raw.get("Id")
                        or raw.get("ID")
                    )
                    if act_id is not None:
                        activity_by_id[str(act_id)] = raw

            atap_entries = _as_list(container.get("ATAP"))
            ordered_refs: List[tuple[int, int, str]] = []  # (bucket_idx, order, act_id)
            for bucket_idx, bucket in enumerate(atap_entries):
                if not isinstance(bucket, dict):
                    continue
                parsed_list = _parse_activity_list(bucket.get("ActivityList"))
                if not parsed_list:
                    parsed_list = _parse_activity_list(bucket.get("Activitys"))
                for idx, entry in enumerate(parsed_list):
                    act_id = None
                    order_val = idx
                    if isinstance(entry, dict):
                        act_id = (
                            entry.get("ActivityID")
                            or entry.get("ActivityId")
                            or entry.get("ID")
                            or entry.get("Id")
                        )
                        try:
                            order_val = int(entry.get("ActivityNo", idx))
                        except Exception:
                            order_val = idx
                    elif isinstance(entry, (str, int)):
                        act_id = entry
                    if act_id is None:
                        continue
                    ordered_refs.append((bucket_idx, order_val, str(act_id)))

            ordered_refs.sort(key=lambda x: (x[0], x[1]))

            for _, _, act_id in ordered_refs:
                row = activity_by_id.get(act_id)
                if row and _try_append(row):
                    break

            if len(items) < max_items:
                for raw in base_list:
                    if _try_append(raw):
                        break

            if len(items) >= max_items:

                break
            headers = {
                "Origin": "https://ticket.ibon.com.tw",
                "Referer": IBON_ENT_URL,
                "X-Requested-With": "XMLHttpRequest",
            }
            if token:
                headers["X-XSRF-TOKEN"] = token
            continue
        elif resp.status_code in (401, 403, 419):
            session, token = _prepare_ibon_session()
            if session is None:
                break
            headers = {
                "Origin": "https://ticket.ibon.com.tw",
                "Referer": IBON_ENT_URL,
                "X-Requested-With": "XMLHttpRequest",
            }
            if token:
                headers["X-XSRF-TOKEN"] = token
            continue
        else:
            app.logger.warning(
                f"[carousel-api] http={resp.status_code} pattern={pattern} -> open breaker"
            )
            _API_BREAK_UNTIL = time.time() + 1800
            return []

    return items[:max_items] if items else []

def _extract_carousel_html_hard(html: str, limit=10, keyword=None, only_concert=False):
    """
    只靠正則把 <img ... alt=... src=...> 與 Details/<id> 抓出來，
    不依賴任何 CSS class / 解析器（避免再踩 lxml 缺、Angular 結構變動）。
    回傳: [{title, url, image}, ...]
    """
    items = []
    seen = set()

    # 1) 先抓所有卡片區塊（盡量縮小範圍，但就算抓到整頁也沒關係）
    #    這裡以 <div class="item">... 或 <div class="owl-item">... 為線索，但不強制
    blocks = re.split(r'(?i)<div[^>]+class="[^"]*(?:item|owl-item)[^"]*"', html)
    if len(blocks) <= 1:
        blocks = [html]  # 退路：整頁掃

    def _pick_img(block):
        # 支援 src / data-src / data-original
        m = re.search(r'(?is)<img[^>]+(?:src|data-src|data-original)\s*=\s*["\']([^"\']+)["\'][^>]*>', block)
        return m.group(1).strip() if m else None

    def _pick_title(block):
        # 先 a[title] → 再 img[alt] → 再 h3/strong 文字
        m = re.search(r'(?is)<a[^>]+title\s*=\s*["\']([^"\']+)["\']', block)
        if m and m.group(1).strip():
            return m.group(1).strip()
        m = re.search(r'(?is)<img[^>]+alt\s*=\s*["\']([^"\']+)["\']', block)
        if m and m.group(1).strip():
            return m.group(1).strip()
        m = re.search(r'(?is)<h3[^>]*>\s*([^<]{2,})\s*</h3>', block)
        if m and m.group(1).strip():
            return m.group(1).strip()
        m = re.search(r'(?is)<strong[^>]*>\s*([^<]{2,})\s*</strong>', block)
        if m and m.group(1).strip():
            return m.group(1).strip()
        return None

    def _pick_url(block, title):
        # 優先抓 Details 連結；沒有就用搜尋連結保底
        m = re.search(r'(?i)(?:https?://ticket\.ibon\.com\.tw)?/ActivityInfo/Details/(\d+)', block)
        if m:
            return urljoin(IBON_BASE, m.group(0))
        # 也掃一下 a[href]
        m = re.search(r'(?is)<a[^>]+href\s*=\s*["\']([^"\']+)["\']', block)
        if m:
            href = urljoin(IBON_BASE, m.group(1))
            if "/ActivityInfo/Details/" in href:
                return href
        # 最後保底：用搜尋
        return f"https://ticket.ibon.com.tw/SearchResult?keyword={title}"

    for b in blocks:
        title = _pick_title(b)
        if not title:
            continue
        if keyword and keyword not in title:
            continue
        if only_concert and not _looks_like_concert(title):
            continue

        img = _pick_img(b)
        if img:
            img = urljoin(IBON_BASE, img)

        href = _pick_url(b, title)

        if href in seen:
            continue
        seen.add(href)
        items.append({"title": title, "url": href, "image": img})

        if len(items) >= max(1, int(limit)):
            break

    return items

# ====== 替換：/liff/activities 以多來源 fallback（優先輪播） ======


def _build_probe_detail_payload(data: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not isinstance(data, dict):
        return None
    payload = dict(data)
    total = payload.get("total")
    if isinstance(total, int):
        payload.setdefault("remain", total)
        payload.setdefault("remaining", total)
    msg = payload.get("msg")
    if isinstance(msg, str) and msg and not payload.get("status_text"):
        payload["status_text"] = msg
    return payload


def _collect_liff_items(limit: int, keyword: Optional[str], only_concert: bool, mode: str, debug: bool) -> tuple[List[Dict[str, Any]], str, List[Dict[str, Any]]]:
    trace: List[Dict[str, Any]] = []
    items: List[Dict[str, Any]] = []
    actual_mode = mode

    if mode == "carousel":
        items = fetch_ibon_carousel_from_api(limit=limit, keyword=keyword, only_concert=only_concert) or []
        trace.append({"phase": "carousel", "count": len(items)})
        return items, actual_mode, trace

    attempts = [
        ("carousel", lambda: fetch_ibon_carousel_from_api(limit=limit, keyword=keyword, only_concert=only_concert)),
        ("api_generic", lambda: fetch_ibon_list_via_api(limit=limit, keyword=keyword, only_concert=only_concert)),
        ("html_fallback", lambda: fetch_ibon_entertainments(limit=limit, keyword=keyword, only_concert=only_concert)),
    ]

    for label, func in attempts:
        try:
            candidate = func() or []
        except Exception as e:
            app.logger.info(f"[liff_api] {label} error: {e}")
            candidate = []
        trace.append({"phase": label, "count": len(candidate)})
        if candidate:
            items = candidate
            actual_mode = label
            break

    if not items:
        try:
            urls = grab_ibon_carousel_urls() or []
            trace.append({"phase": "browser_carousel", "count": len(urls)})
            if urls:
                items = _items_from_details_urls(urls, limit=limit, keyword=keyword, only_concert=only_concert)
                if items:
                    actual_mode = "browser_carousel"
        except Exception as e:
            app.logger.warning(f"[liff_api] browser fallback failed: {e}")

    if items:
        sess = sess_default()
        enriched: List[Dict[str, Any]] = []
        for base in items:
            details_url = base.get("details_url") or base.get("url")
            if not isinstance(details_url, str) or not details_url:
                continue
            item_trace: Optional[List[Dict[str, Any]]] = [] if debug else None
            try:
                data = _probe_activity_details(details_url, sess, trace=item_trace)
            except Exception as exc:
                app.logger.info(f"[liff_api] enrich fail {details_url}: {exc}")
                if item_trace is not None:
                    item_trace.append({"phase": "error", "reason": str(exc)})
                continue
            base_image = base.get("image_url") or base.get("image")
            if base_image and not data.get("image"):
                data["image"] = base_image
                data["image_url"] = base_image
            if item_trace:
                data["trace"] = item_trace
            enriched.append(data)
        if enriched:
            items = enriched
            trace.append({"phase": "enrich", "count": len(items)})

    return items or [], actual_mode, trace

def _read_json_payload() -> Dict[str, Any]:
    try:
        payload = request.get_json(silent=True) or {}
    except Exception:
        payload = {}
    if not isinstance(payload, dict):
        payload = {}
    return payload

def _maybe_probe(url: str) -> Optional[Dict[str, Any]]:
    if not url:
        return None

    if current_app.testing:
        return {"url": url, "status_text": "testing", "msg": url, "remain": 0}

    detail = _build_probe_detail_payload(probe(url))
    return detail

@liff_api_bp.get("/concerts")
def concerts():
    mode = (request.args.get("mode") or "carousel").strip().lower() or "carousel"
    try:
        limit = int(request.args.get("limit", "10"))
    except Exception:
        limit = 10
    keyword = request.args.get("q") or None
    only_concert = _truthy(request.args.get("onlyConcert"))
    debug = _truthy(request.args.get("debug"))

    try:
        items, actual_mode, trace = _collect_liff_items(
            limit=limit,
            keyword=keyword,
            only_concert=only_concert,
            mode=mode,
            debug=debug,
        )
        body = {"ok": True, "mode": actual_mode, "items": items, "trace": trace}
        return jsonify(body), 200
    except Exception as exc:  # pragma: no cover - defensive logging path
        current_app.logger.error(f"/api/liff/concerts error: {exc}\n{traceback.format_exc()}")
        return jsonify({"ok": False, "error": str(exc)}), 500

@liff_api_bp.post("/watch")
def watch():
    payload = _read_json_payload()
    chat_id = str(payload.get("chat_id") or payload.get("chatId") or "").strip()
    url = str(payload.get("url") or "").strip()
    period_raw = payload.get("period") or payload.get("sec") or payload.get("seconds")
    try:
        sec = int(period_raw)
    except Exception:
        sec = DEFAULT_PERIOD_SEC
    sec = max(15, sec)

    if not chat_id:
        return jsonify({"ok": False, "error": "missing chat_id"}), 200
    if not url:
        return jsonify({"ok": False, "error": "missing url"}), 200
    if not FS_OK:
        return jsonify({"ok": False, "error": FS_ERROR_MSG or "watch service unavailable"}), 200

    try:
        task_id, created = fs_upsert_watch(chat_id, url, sec)
    except Exception as exc:  # pragma: no cover - Firestore runtime errors
        current_app.logger.error(f"/api/liff/watch error: {exc}")
        return jsonify({"ok": False, "error": str(exc)}), 200

    detail: Optional[Dict[str, Any]] = None
    try:
        detail = _maybe_probe(url)
    except Exception as exc:  # pragma: no cover - probe network errors
        current_app.logger.info(f"/api/liff/watch probe failed: {exc}")

    message = f"任務 {task_id} 已{'建立' if created else '更新'}，每 {sec} 秒監看。"
    response: Dict[str, Any] = {
        "ok": True,
        "task_id": task_id,
        "created": created,
        "period": sec,
        "message": message,
        "detail": detail,
    }
    if isinstance(detail, dict):
        remain_val = detail.get("remain")
        if isinstance(remain_val, int):
            response["remain"] = remain_val
        status_text = detail.get("status_text")
        if isinstance(status_text, str):
            response["status_text"] = status_text
    return jsonify(response), 200

@liff_api_bp.post("/unwatch")
def unwatch():
    payload = _read_json_payload()
    chat_id = str(payload.get("chat_id") or payload.get("chatId") or "").strip()
    url = str(payload.get("url") or "").strip()
    task_code = str(
        payload.get("task_code")
        or payload.get("taskId")
        or payload.get("task_id")
        or ""
    ).strip()

    if not chat_id:
        return jsonify({"ok": False, "error": "missing chat_id"}), 200
    if not task_code and not url:
        return jsonify({"ok": False, "error": "missing url"}), 200
    if not FS_OK:
        return jsonify({"ok": False, "error": FS_ERROR_MSG or "watch service unavailable"}), 200

    doc = None
    if task_code:
        doc = fs_get_task_by_id(chat_id, task_code)
    if doc is None and url:
        try:
            doc = fs_get_task_by_canon(chat_id, canonicalize_url(url))
        except Exception as exc:
            current_app.logger.error(f"/api/liff/unwatch canonicalize fail: {exc}")
            return jsonify({"ok": False, "error": str(exc)}), 200

    if doc is None:
        return jsonify({"ok": False, "reason": "no_watch", "message": "此活動目前沒有監看任務。"}), 200

    data = doc.to_dict() if hasattr(doc, "to_dict") else {}
    task_id = str(data.get("id") or task_code or "").strip()
    target_url = url or data.get("url") or ""

    if not task_id:
        return jsonify({"ok": False, "error": "missing task id"}), 200

    try:
        disabled = fs_disable(chat_id, task_id)
    except Exception as exc:  # pragma: no cover - Firestore runtime errors
        current_app.logger.error(f"/api/liff/unwatch disable error: {exc}")
        return jsonify({"ok": False, "error": str(exc)}), 200

    if not disabled:
        return jsonify({"ok": False, "error": "unable to disable task"}), 200

    detail: Optional[Dict[str, Any]] = None
    if target_url:
        try:
            detail = _maybe_probe(target_url)
        except Exception as exc:  # pragma: no cover - probe network errors
            current_app.logger.info(f"/api/liff/unwatch probe failed: {exc}")

    response: Dict[str, Any] = {"ok": True, "task_id": task_id, "message": "stopped", "detail": detail}
    if isinstance(detail, dict):
        remain_val = detail.get("remain")
        if isinstance(remain_val, int):
            response["remain"] = remain_val
        status_text = detail.get("status_text")
        if isinstance(status_text, str):
            response["status_text"] = status_text
    return jsonify(response), 200

def _quick_check_impl(url: str):
    url = (url or "").strip()
    if not url:
        return jsonify({"ok": False, "error": "missing url"}), 200

    try:
        detail = _maybe_probe(url)
    except Exception as exc:  # pragma: no cover - probe network errors
        current_app.logger.error(f"/api/liff/quick-check error: {exc}")
        return jsonify({"ok": False, "error": str(exc)}), 200

    response: Dict[str, Any] = {"ok": True, "detail": detail}
    if isinstance(detail, dict):
        remain_val = detail.get("remain")
        if isinstance(remain_val, int):
            response["remain"] = remain_val
        status_text = detail.get("status_text")
        if isinstance(status_text, str):
            response["status_text"] = status_text
        # 這裡改成回傳 /check <url>，前端就會把這段直接送到聊天室
        response["message"] = f"/check {detail.get('url') or url}"
    return jsonify(response), 200

@liff_api_bp.get("/quick-check")
def quick_check_get():
    return _quick_check_impl(request.args.get("url"))

@liff_api_bp.post("/quick-check")
def quick_check_post():
    payload = _read_json_payload()
    return _quick_check_impl(payload.get("url"))

app.register_blueprint(liff_api_bp)

@app.get("/liff/activities")
def liff_activities():
    want_debug = _truthy(request.args.get("debug"))
    response = concerts()
    if want_debug:
        return response

    if isinstance(response, tuple):
        flask_response = response[0]
        status = response[1]
        if hasattr(flask_response, "get_json"):
            data = flask_response.get_json(silent=True)
        else:
            data = None
        if status == 200 and isinstance(data, dict) and "items" in data:
            return jsonify(data["items"]), 200
        return response
    return response

@app.post("/liff/watch")
def liff_watch_compat():
    return watch()

@app.post("/liff/unwatch")
def liff_unwatch_compat():
    return unwatch()

@app.post("/liff/watch_status")

def liff_watch_status():
    try:
        payload = request.get_json(silent=True) or {}
        if not isinstance(payload, dict):
            payload = {}
    except Exception:
        payload = {}

    chat_id = str(payload.get("chatId") or payload.get("userId") or "").strip()
    urls_raw = payload.get("urls")
    if isinstance(urls_raw, str):
        urls_list = [urls_raw]
    elif isinstance(urls_raw, list):
        urls_list = urls_raw
    else:
        urls_list = []

    clean_urls: List[str] = []
    for u in urls_list:
        if not isinstance(u, str):
            continue
        val = u.strip()
        if val:
            clean_urls.append(val)

    if not chat_id:
        return jsonify({"ok": False, "error": "missing chatId"}), 400

    if not clean_urls:
        return jsonify({"ok": True, "results": {}}), 200

    results: Dict[str, Dict[str, Any]] = {}

    for url in clean_urls:
        entry = {"watching": False, "enabled": False, "taskId": None, "found": False}

        if not FS_OK:
            results[url] = entry
            continue

        try:
            canon = canonicalize_url(url)
            doc = fs_get_task_by_canon(chat_id, canon)
        except Exception as e:
            app.logger.error(f"[liff_watch_status] lookup failed for {url}: {e}")
            entry["error"] = str(e)
            results[url] = entry
            continue

        if doc:
            data = doc.to_dict()
            tid = str(data.get("id", "")).strip() or None
            enabled = bool(data.get("enabled"))
            entry.update({
                "found": True,
                "taskId": tid,
                "enabled": enabled,
                "watching": enabled,
            })
            period = data.get("period")
            if period is not None:
                try:
                    entry["period"] = int(period)
                except Exception:
                    entry["period"] = period

        results[url] = entry

    return jsonify({"ok": True, "results": results}), 200

# ✅ 新增這段：提供 /api/liff/status，轉呼叫上面那支
@app.post("/api/liff/status")
def liff_status_api():
    return liff_watch_status()

@app.get("/liff/activities_debug")
def liff_activities_debug():
    try:
        limit = int(request.args.get("limit", "10"))
    except Exception:
        limit = 10
    kw = request.args.get("q") or None
    only_concert = (str(request.args.get("onlyConcert","")).lower() in ("1","true","t","yes","y"))

    trace = []

    acts1 = fetch_ibon_carousel_from_api(limit=limit, keyword=kw, only_concert=only_concert)
    trace.append({"phase": "carousel_api+html_fallback", "count": len(acts1)})
    if acts1:
        return jsonify({"ok": True, "count": len(acts1), "items": acts1, "trace": trace}), 200

    acts2 = fetch_ibon_list_via_api(limit=limit, keyword=kw, only_concert=only_concert)
    trace.append({"phase": "api_generic", "count": len(acts2)})
    if acts2:
        return jsonify({"ok": True, "count": len(acts2), "items": acts2, "trace": trace}), 200

    return jsonify({"ok": True, "count": 0, "items": [], "trace": trace}), 200

@app.get("/liff")
@app.get("/liff/")

def liff_index():
    try:
        return send_from_directory("liff", "index.html")
    except Exception:
        return "LIFF OK", 200

@app.route("/liff/ping", methods=["GET"])
def liff_ping():
    return jsonify({"ok": True, "time": datetime.utcnow().isoformat()+"Z"}), 200

@app.get("/netcheck")
def netcheck():
    urls = [
        "https://www.google.com",
        "https://ticket.ibon.com.tw/Index/entertainment",
        "https://ticket.ibon.com.tw/api/ActivityInfo/GetIndexData",
    ]
    out = []
    for u in urls:
        try:
            r = requests.get(u, timeout=10)
            out.append({"url": u, "http": r.status_code, "len": len(r.text)})
        except Exception as e:
            out.append({"url": u, "error": repr(e)})
    return jsonify({"results": out})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8080")))

# === debug: dump all routes ===
try:
    from flask import jsonify
    def _dump_routes(flask_app):
        out=[]
        for r in flask_app.url_map.iter_rules():
            methods = sorted(m for m in r.methods if m in ("GET","POST","PUT","DELETE","PATCH"))
            out.append({"rule": str(r), "endpoint": r.endpoint, "methods": methods})
        return out

    @_r.get("/__routes")
    def __routes():
        return jsonify(routes=_dump_routes(app)), 200
except Exception:
    pass

@app.get("/__whoami")
def __whoami():
    return jsonify({
        "module": __name__,
        "strict_slashes": getattr(app.url_map, "strict_slashes", None),
        "routes": sorted([str(r) for r in app.url_map.iter_rules()])[:80],
    }), 200

@app.errorhandler(404)
def __nf(e):  # noqa: D401
    print("[NF]", request.method, request.path)
    return jsonify({"error": "not found", "path": request.path}), 404
