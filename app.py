# -*- coding: utf-8 -*-
import os
import re
import json
import time
import uuid
import hashlib
import logging
import traceback
import requests
from datetime import datetime, timezone, timedelta
from typing import Dict, Tuple, Optional, Any, List
from urllib.parse import urlparse, urlunparse, parse_qs, urlencode, urljoin, unquote
from flask import send_from_directory
from flask import jsonify, Flask, request, abort
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

IBON_API = "https://ticketapi.ibon.com.tw/api/ActivityInfo/GetIndexData"
IBON_TOKEN_API = "https://ticketapi.ibon.com.tw/api/ActivityInfo/GetToken"
IBON_BASE = "https://ticket.ibon.com.tw/"
# NEW: 首頁輪播 URL 抽成環境變數（可覆寫）
IBON_ENT_URL = os.getenv("IBON_ENT_URL", "https://ticket.ibon.com.tw/Index/entertainment")
IBON_BIG_BANNER_API = "https://ticketapi.ibon.com.tw/api/ADInfo/GetBigBanner"
IBON_AD_IMAGE_BASE = os.getenv("IBON_AD_IMAGE_BASE", "https://ticketimg2.azureedge.net/image/ADImage/")
if not IBON_AD_IMAGE_BASE.endswith("/"):
    IBON_AD_IMAGE_BASE = IBON_AD_IMAGE_BASE + "/"


def _parse_ibon_index_patterns(raw: Optional[str]) -> List[str]:
    if raw is None:
        return ["Entertainment"]
    patterns: List[str] = []
    for part in raw.split(","):
        p = part.strip()
        if not p:
            continue
        if p not in patterns:
            patterns.append(p)
    return patterns or ["Entertainment"]


_IBON_INDEX_PATTERNS = tuple(_parse_ibon_index_patterns(os.getenv("IBON_INDEX_PATTERNS")))

# 簡單快取（5 分鐘）
_cache = {"ts": 0, "data": []}
_CACHE_TTL = 300  # 秒

_CONCERT_WORDS = ("演唱會", "演場會", "音樂會", "演唱", "演出", "LIVE", "Live")

def _looks_like_concert(title: str) -> bool:
    t = title or ""
    return any(w.lower() in t.lower() for w in _CONCERT_WORDS)


def _extract_xsrf_token(payload: Any) -> Optional[str]:
    """從 ibon 的 GetToken 結構中提取 XSRF token。"""

    def _collect_tokens(src: Any) -> List[str]:
        out: List[str] = []
        if isinstance(src, dict):
            for val in src.values():
                if isinstance(val, str) and val.strip():
                    out.append(val.strip())
                elif isinstance(val, (dict, list)):
                    out.extend(_collect_tokens(val))
        elif isinstance(src, list):
            for v in src:
                out.extend(_collect_tokens(v))
        elif isinstance(src, str) and src.strip():
            out.append(src.strip())
        return out

    candidates = _collect_tokens(payload)
    for cand in candidates:
        token = cand
        if "|" in token:
            parts = [p.strip() for p in token.split("|") if p.strip()]
            if parts:
                token = parts[-1]
        token = unquote(token)
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
    title = (row.get("Title") or row.get("ActivityTitle") or row.get("GameName")
             or row.get("ActivityName") or row.get("Name") or row.get("Subject")
             or row.get("ADTitle") or row.get("ADName") or "活動")

    # image
    img = (row.get("ImgUrl") or row.get("ImageUrl") or row.get("Image")
           or row.get("ActivityImage") or row.get("ADImageURL")
           or row.get("PicUrl") or row.get("PictureUrl") or "").strip() or None

    # url / id
    url = (row.get("Url") or row.get("URL") or row.get("LinkUrl")
           or row.get("LinkURL") or row.get("Link") or row.get("ADURL")
           or row.get("GameTicketURL") or row.get("GameTicketUrl")
           or "").strip()

    if not url:
        act_id = (row.get("ActivityInfoId") or row.get("ActivityInfoID")
                  or row.get("ActivityId") or row.get("ActivityID")
                  or row.get("GameId") or row.get("GameID")
                  or row.get("Id") or row.get("ID"))
        if act_id:
            url = urljoin(IBON_BASE, f"/ActivityInfo/Details/{act_id}")

    if url and "/ActivityInfo/Details" in url:
        try:
            parsed = urlparse(url)
            act_id = None
            if parsed.path:
                m = re.search(r"/ActivityInfo/Details/(\d+)", parsed.path)
                if m:
                    act_id = m.group(1)
            if not act_id:
                qs = parse_qs(parsed.query or "")
                for key in ("id", "activityid", "activityId", "activityInfoId", "gameId", "gameID"):
                    vals = qs.get(key)
                    if vals:
                        act_id = vals[-1]
                        break
            if act_id:
                url = urljoin(IBON_BASE, f"/ActivityInfo/Details/{act_id}")
        except Exception:
            pass

    # 統一為絕對網址
    if url and not url.lower().startswith("http"):
        url = urljoin(IBON_BASE, url)

    # 有些圖片給相對路徑
    if img and not img.lower().startswith("http"):
        if img.startswith("//"):
            img = "https:" + img
        elif row.get("ADLocation") or "ADImage" in img:
            img = urljoin(IBON_AD_IMAGE_BASE, img)
        else:
            img = urljoin(IBON_BASE, img)

    return {"title": str(title).strip(), "url": url, "image": img}


def _iter_activity_dicts(obj: Any):
    if isinstance(obj, dict):
        yield obj
        for v in obj.values():
            yield from _iter_activity_dicts(v)
    elif isinstance(obj, list):
        for v in obj:
            yield from _iter_activity_dicts(v)
    elif isinstance(obj, str):
        text = obj.strip()
        if text.startswith("{") or text.startswith("["):
            try:
                parsed = json.loads(text)
            except Exception:
                return
            else:
                yield from _iter_activity_dicts(parsed)


def _fetch_big_banner_documents(session: requests.Session, token: Optional[str], patterns: List[str]) -> Tuple[List[Dict[str, Any]], bool, bool]:
    docs: List[Dict[str, Any]] = []
    unauthorized = False
    server_error = False

    headers = {
        "Origin": "https://ticket.ibon.com.tw",
        "Referer": IBON_ENT_URL,
        "X-Requested-With": "XMLHttpRequest",
    }
    if token:
        headers["X-XSRF-TOKEN"] = token

    for pattern in patterns or ["Entertainment"]:
        pat = "" if pattern is None else str(pattern)
        files = {"patternCode": (None, pat)}
        try:
            resp = session.post(
                IBON_BIG_BANNER_API,
                headers=headers,
                files=files,
                timeout=10,
            )
        except Exception as e:
            app.logger.info(f"[ibon big banner] pattern={pat or 'Entertainment'} err: {e}")
            continue

        if resp.status_code == 200:
            try:
                docs.append(resp.json())
            except Exception as e:
                app.logger.info(f"[ibon big banner] pattern={pat or 'Entertainment'} json err: {e}")
            continue

        if resp.status_code in (401, 403, 419):
            unauthorized = True
            app.logger.info(f"[ibon big banner] pattern={pat or 'Entertainment'} auth http={resp.status_code}")
            break

        if 500 <= resp.status_code < 600:
            server_error = True
            app.logger.warning(f"[ibon big banner] pattern={pat or 'Entertainment'} http={resp.status_code}")
        else:
            app.logger.info(f"[ibon big banner] pattern={pat or 'Entertainment'} http={resp.status_code}")

    return docs, unauthorized, server_error


def _fetch_index_documents(session: requests.Session, token: Optional[str], patterns: List[str]) -> Tuple[List[Dict[str, Any]], bool, bool]:
    docs: List[Dict[str, Any]] = []
    unauthorized = False
    server_error = False

    headers = {
        "Origin": "https://ticket.ibon.com.tw",
        "Referer": IBON_ENT_URL,
        "X-Requested-With": "XMLHttpRequest",
    }
    if token:
        headers["X-XSRF-TOKEN"] = token

    for pattern in patterns or ["Entertainment"]:
        pat = "" if pattern is None else str(pattern)
        files = {"pattern": (None, pat)}
        try:
            resp = session.post(
                IBON_API,
                headers=headers,
                files=files,
                timeout=10,
            )
        except Exception as e:
            app.logger.info(f"[ibon api] pattern={pat or 'Entertainment'} err: {e}")
            continue

        if resp.status_code == 200:
            try:
                docs.append(resp.json())
            except Exception as e:
                app.logger.info(f"[ibon api] pattern={pat or 'Entertainment'} json err: {e}")
            continue

        if resp.status_code in (401, 403, 419):
            unauthorized = True
            app.logger.info(f"[ibon api] pattern={pat or 'Entertainment'} auth http={resp.status_code}")
            break

        if 500 <= resp.status_code < 600:
            server_error = True
            app.logger.warning(f"[ibon api] pattern={pat or 'Entertainment'} http={resp.status_code}")
        else:
            app.logger.info(f"[ibon api] pattern={pat or 'Entertainment'} http={resp.status_code}")

    return docs, unauthorized, server_error


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
        patterns = list(_IBON_INDEX_PATTERNS) or ["Entertainment"]
        base_rows: List[Dict[str, Any]] = []

        for attempt in range(3):
            if session is None:
                session, token = _prepare_ibon_session()

            docs, unauthorized, server_error = _fetch_index_documents(session, token, patterns)

            if docs:
                merged: Dict[str, Dict[str, Any]] = {}
                order: List[str] = []
                for doc in docs:
                    payload: Any
                    if isinstance(doc, dict):
                        payload = doc.get("Item") or doc.get("Data") or doc.get("data") or doc
                    else:
                        payload = doc
                    for cand in _iter_activity_dicts(payload):
                        if not isinstance(cand, dict):
                            continue
                        try:
                            item = _normalize_item(cand)
                        except Exception:
                            continue
                        url = item.get("url")
                        if not url:
                            continue
                        existing = merged.get(url)
                        if not existing:
                            merged[url] = item
                            order.append(url)
                        else:
                            title = item.get("title")
                            if title and title.strip() and existing.get("title") in (None, "", "活動") and title != "活動":
                                existing["title"] = title
                            img = item.get("image")
                            if img and not existing.get("image"):
                                existing["image"] = img
                base_rows = [merged[k] for k in order]
                break

            if unauthorized:
                session, token = _prepare_ibon_session()
                continue

            if server_error:
                _open_breaker()
                base_rows = []
                break

            _sleep_backoff(attempt)

        _cache = {"ts": now, "data": base_rows}

    # 過濾 + 截斷（這個 return 要在迴圈外！）
    out = []
    kw = (keyword or "").strip()
    for it in base_rows or []:
        if kw and kw not in it["title"]:
            continue
        if only_concert and not _looks_like_concert(it["title"]):
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

# ===== Flask & CORS =====
app = Flask(__name__)

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
    patterns_js = json.dumps(list(_IBON_INDEX_PATTERNS) or ["Entertainment"])
    script = r"""
    () => {
      try {
        const url = "https://ticketapi.ibon.com.tw/api/ActivityInfo/GetIndexData";
        const token = (() => {
          const raw = document.cookie.split(";").map(v => v.trim()).find(v => v.startsWith("XSRF-TOKEN="));
          if (!raw) return null;
          try {
            return decodeURIComponent(raw.split("=")[1] || "");
          } catch (e) {
            return (raw.split("=")[1] || "");
          }
        })();

        const patterns = __PATTERNS__;
        // 走訪 JSON，把可能的 Details 連結/ID 全撿出來
        const out = new Set();
        const base = "https://ticket.ibon.com.tw";

        const norm = (obj) => {
          if (!obj || typeof obj !== "object") return;
          const title = obj.Title || obj.ActivityTitle || obj.GameName || obj.Name || obj.Subject;
          const link  = obj.Url || obj.URL || obj.LinkUrl || obj.LinkURL || obj.Link;
          const id    = obj.ActivityInfoId || obj.ActivityInfoID || obj.ActivityId || obj.ActivityID ||
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
            else if (typeof v === "string") {
              const text = v.trim();
              if (text.startsWith("[") || text.startsWith("{")) {
                try {
                  const parsed = JSON.parse(text);
                  if (Array.isArray(parsed)) parsed.forEach(norm);
                  else if (parsed && typeof parsed === "object") norm(parsed);
                } catch (err) {}
              }
            }
          });
        };

        const targets = (Array.isArray(patterns) && patterns.length ? patterns : ["Entertainment"]);
        for (const pattern of targets) {
          try {
            const form = new FormData();
            form.append("pattern", pattern);

            const xhr = new XMLHttpRequest();
            xhr.open("POST", url, false);                 // 同步 XHR（Selenium/Playwright 都吃）
            xhr.setRequestHeader("X-Requested-With", "XMLHttpRequest");
            if (token) xhr.setRequestHeader("X-XSRF-TOKEN", token);
            xhr.send(form);
            if (xhr.status < 200 || xhr.status >= 300) continue;

            let data; try { data = JSON.parse(xhr.responseText); } catch (e) { continue; }
            norm(data);
          } catch (inner) {
            // ignore single-pattern errors
          }
        }
        return Array.from(out);
      } catch (e) {
        return [];
      }
    }
    """.replace("__PATTERNS__", patterns_js)
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
            title = info.get("title") or "活動"
            if keyword and keyword not in title:
                continue
            if only_concert and not _looks_like_concert(title):
                continue
            img = info.get("poster") or None
            items.append({"title": title, "url": u, "image": img})
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
try:
    fs_client = firestore.Client()
    FS_OK = True
except Exception as e:
    app.logger.warning(f"Firestore init failed: {e}")
    fs_client = None
    FS_OK = False

COL = "watchers"

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0 Safari/537.36"
)

_RE_DATE = re.compile(r"(\d{4}/\d{2}/\d{2})\s+(\d{2}:\d{2})")
_RE_AREA_TAG = re.compile(r"<area\b[^>]*>", re.I)

LOGO = "https://ticketimg2.azureedge.net/logo.png"
TICKET_API = "https://ticketapi.ibon.com.tw/api/ActivityInfo/GetGameInfoList"

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

def _resolve_ticket_url(u: str, depth: int = 0) -> str:
    if depth > 3:
        return u
    try:
        parsed = urlparse(u)
    except Exception:
        return u

    netloc = (parsed.netloc or "").lower()
    path = parsed.path or ""
    query = parse_qs(parsed.query, keep_blank_values=True)
    query_by_lower = {k.lower(): v for k, v in query.items()}

    if netloc.endswith("ticket.ibon.com.tw"):
        lower_path = path.lower()
        if "/activityinfo/goticketurl" in lower_path:
            # 官方會把目標網址塞進 GoUrl 參數，通常會再 encode 一次
            for key in ("GoUrl", "GoURL", "gourl", "go"):
                vals = query.get(key) or query_by_lower.get(key.lower())
                if not vals:
                    continue
                target = vals[-1]
                if not target:
                    continue
                resolved = unquote(target)
                if "%" in resolved:
                    second = unquote(resolved)
                    if second != resolved:
                        resolved = second
                if resolved.startswith("//"):
                    resolved = "https:" + resolved
                if resolved.startswith("/"):
                    resolved = urljoin(IBON_BASE, resolved)
                if resolved:
                    return _resolve_ticket_url(resolved, depth + 1)
        if lower_path.startswith("/eventbuy/") or lower_path.startswith("/event/"):
            parts = [p for p in path.split("/") if p]
            product_id = parts[1] if len(parts) >= 2 else None
            performance_id = parts[2] if len(parts) >= 3 else None
            if product_id and performance_id:
                return ("https://orders.ibon.com.tw/application/UTK02/UTK0201_000.aspx"
                        f"?PRODUCT_ID={product_id}&PERFORMANCE_ID={performance_id}")

    return u


def canonicalize_url(u: str) -> str:
    resolved = _resolve_ticket_url((u or "").strip())
    p = urlparse(resolved)
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
    headers = {
        "Origin": "https://orders.ibon.com.tw",
        "Referer": referer_url,
        "User-Agent": UA,
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json;charset=UTF-8",
        "X-Requested-With": "XMLHttpRequest",
        "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.6",
    }
    tries: List[Tuple[str, Dict[str, Optional[str]]]] = []
    if perf_id or product_id:
        tries += [
            ("GET",  {"Performance_ID": perf_id, "Product_ID": product_id}),
            ("GET",  {"PerformanceId": perf_id,  "ProductId":  product_id}),
            ("GET",  {"PERFORMANCE_ID": perf_id,"PRODUCT_ID": product_id}),
            ("POST", {"Performance_ID": perf_id, "Product_ID": product_id}),
            ("POST", {"PerformanceId": perf_id,  "ProductId":  product_id}),
        ]
    tries.append(("GET", {}))  # 無參數也試

    picked: Dict[str, str] = {}
    for method, params in tries:
        try:
            if method == "GET":
                r = sess.get(TICKET_API, params={k: v for k, v in (params or {}).items() if v}, headers=headers, timeout=12)
            else:
                r = sess.post(TICKET_API, json={k: v for k, v in (params or {}).items() if v}, headers=headers, timeout=12)
            if r.status_code != 200:
                continue

            data = r.json()
            s = json.dumps(data, ensure_ascii=False)

            info = _deep_pick_activity_info(data)

            m = re.search(r'https?://ticket\.ibon\.com\.tw/ActivityInfo/Details/(\d+)', s)
            if m:
                info["details"] = m.group(0)
            else:
                m = (re.search(r'"ActivityInfoId"\s*:\s*(\d+)', s) or
                     re.search(r'"ActivityId"\s*:\s*(\d+)', s) or
                     re.search(r'"Id"\s*:\s*(\d+)', s))
                if m:
                    info["details"] = f"https://ticket.ibon.com.tw/ActivityInfo/Details/{m.group(1)}"

            if not info.get("poster"):
                promo = find_activity_image_any(s)
                if promo:
                    info["poster"] = promo

            def match_obj(obj):
                text = json.dumps(obj, ensure_ascii=False)
                ok = True
                if perf_id: ok = ok and (perf_id in text)
                if product_id: ok = ok and (product_id in text)
                return ok
            if isinstance(data, list):
                for it in data:
                    if match_obj(it): info.update(_deep_pick_activity_info(it)); break
            elif isinstance(data, dict):
                for v in data.values():
                    if isinstance(v, list):
                        for it in v:
                            if match_obj(it): info.update(_deep_pick_activity_info(it)); break

            if info:
                picked = info
                break

        except Exception as e:
            app.logger.info(f"[api] fetch fail ({method} {params}): {e}")
            continue

    return picked

def fetch_from_ticket_details(details_url: str, sess: requests.Session) -> Dict[str, str]:
    out: Dict[str, str] = {}
    try:
        r = sess.get(details_url, timeout=12)
        if r.status_code != 200:
            return out
        html = r.text
        soup = soup_parse(html)

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
                if src and any(k in src.lower() for k in ("activityimage","azureedge","banner","cover","adimage")):
                    out["poster"] = urljoin(details_url, src); break

        h1 = soup.select_one("h1")
        if h1:
            t = h1.get_text(" ", strip=True)
            if t: out["title"] = t

        tx = soup.get_text(" ", strip=True)
        m = re.search(r'(\d{4}/\d{2}/\d{2})\s*(?:([\u4e00-\u9fff]))?\s*(\d{2}:\d{2})', tx)
        if m: out["dt"] = f"{m.group(1)} {m.group(3)}"

    except Exception as e:
        app.logger.info(f"[details] fetch fail: {e}")
    return out

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

    m = _RE_DATE.search(html)
    if m:
        dt_text = f"{m.group(1)} {m.group(2)}"

    return title, place, dt_text

# ============= 票區與 live.map 解析 =============
def extract_area_meta_from_000(html: str) -> Tuple[Dict[str, str], Dict[str, str], Dict[str, int], Dict[str, int]]:
    name_map: Dict[str, str] = {}
    status_map: Dict[str, str] = {}
    qty_map: Dict[str, int] = {}
    order_map: Dict[str, int] = {}

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

    return name_map, status_map, qty_map, order_map

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
                if r.status_code == 200 and "<area" in r.text:
                    app.logger.info(f"[livemap] hit {url}")
                    return _parse_livemap_text(r.text)
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
def parse_UTK0201_000(url: str, sess: requests.Session) -> dict:
    out = {"ok": False, "sig": "NA", "url": url, "image": LOGO}
    r = sess.get(url, timeout=15)
    if r.status_code != 200:
        out["msg"] = f"讀取失敗（HTTP {r.status_code}）"
        return out
    html = r.text

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
    area_name_map, area_status_map, area_qty_map, area_order_map = extract_area_meta_from_000(html)
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

def probe(url: str) -> dict:
    original = (url or "").strip()
    resolved = _resolve_ticket_url(original)
    s = sess_default()
    p = urlparse(resolved)
    if "orders.ibon.com.tw" in p.netloc and p.path.upper().endswith("/UTK0201_000.ASPX"):
        return parse_UTK0201_000(resolved, s)
    r = s.get(resolved, timeout=12)
    title = ""
    try:
        soup = soup_parse(r.text)
        if soup.title and soup.title.text:
            title = soup.title.text.strip()
    except Exception:
        pass
    return {
        "ok": False, "sig": "NA", "url": resolved, "image": LOGO,
        "title": title or "（未取到標題）", "place": "", "date": "", "msg": original or resolved,
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
@app.route("/webhook", methods=["POST"])
def webhook():
    if not (HAS_LINE and handler):
        app.logger.warning("Webhook invoked but handler not ready")
        return "OK", 200
    signature = request.headers.get("X-Line-Signature", "")
    body = request.get_data(as_text=True)
    try:
        handler.handle(body, signature)
    except InvalidSignatureError:
        app.logger.warning("InvalidSignature on /webhook")
        abort(400)
    return "OK"

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
        html = r.text
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

                items.append({"title": title, "url": href, "image": img_url})
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

            items.append({"title": title, "url": href, "image": None})
            seen.add(href)
            if len(items) >= max(1, int(limit)):
                break

        return items

    except Exception as e:
        app.logger.error(f"[html_hard] failed: {e}")
        return []

# --- backward-compat alias（舊名→新實作；一定要放在函式「外面」） ---
def fetch_ibon_entertainments(limit=10, keyword=None, only_concert=False):
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

    session: Optional[requests.Session] = None
    token: Optional[str] = None
    patterns = list(_IBON_INDEX_PATTERNS) or ["Entertainment"]

    items: List[Dict[str, Any]] = []
    seen: set[str] = set()
    target = max(1, int(limit))

    def _append_from_docs(docs: List[Any]) -> bool:
        appended = False
        for doc in docs or []:
            if isinstance(doc, dict):
                payload: Any = doc.get("Item") or doc.get("Data") or doc.get("data") or doc
            else:
                payload = doc
            for cand in _iter_activity_dicts(payload):
                if not isinstance(cand, dict):
                    continue
                try:
                    it = _normalize_item(cand)
                except Exception:
                    continue
                url = it.get("url")
                if not url:
                    continue
                title = it.get("title") or ""
                if keyword and keyword not in title:
                    continue
                if only_concert and not _looks_like_concert(title):
                    continue
                if url in seen:
                    continue
                seen.add(url)
                items.append(it)
                appended = True
                if len(items) >= target:
                    return True
        return appended

    # 1) 首先嘗試首頁大圖輪播（ADInfo/GetBigBanner）
    banner_docs: List[Any] = []
    for attempt in range(3):
        if session is None:
            session, token = _prepare_ibon_session()

        docs, unauthorized, server_error = _fetch_big_banner_documents(session, token, patterns)

        if docs:
            banner_docs = docs
            _append_from_docs(banner_docs)
            break

        if unauthorized:
            session, token = _prepare_ibon_session()
            continue

        if server_error:
            app.logger.warning("[carousel-api] big banner 5xx -> fallback to other sources")
            break

        _sleep_backoff(attempt)

    if len(items) >= target:
        return items[:target]

    # 2) 若輪播不足，再拉一般 Index Data 兜底
    payloads: List[Any] = []

    for attempt in range(3):
        if session is None:
            session, token = _prepare_ibon_session()

        docs, unauthorized, server_error = _fetch_index_documents(session, token, patterns)

        if docs:
            payloads = docs
            break

        if unauthorized:
            session, token = _prepare_ibon_session()
            continue

        if server_error:
            app.logger.warning("[carousel-api] index data 5xx -> open breaker")
            _API_BREAK_UNTIL = time.time() + 1800
            return items[:target] if items else []

        _sleep_backoff(attempt)

    if not payloads:
        return items[:target] if items else []

    def _append_from_list(arr):
        nonlocal items, seen
        for r in arr or []:
            try:
                it = _normalize_item(r if isinstance(r, dict) else {})
                url = it.get("url")
                if not url:
                    continue
                title = it.get("title") or ""
                if keyword and keyword not in title:
                    continue
                if only_concert and not _looks_like_concert(title):
                    continue
                if url in seen:
                    continue
                seen.add(url)
                items.append(it)
                if len(items) >= target:
                    return True
            except Exception:
                continue
        return False

    def _iter_lists(obj, path=""):
        if isinstance(obj, list):
            yield (path, obj)
        elif isinstance(obj, dict):
            for k, v in obj.items():
                p = f"{path}.{k}" if path else str(k)
                yield from _iter_lists(v, p)
        elif isinstance(obj, str):
            text = obj.strip()
            if text.startswith("[") or text.startswith("{"):
                try:
                    parsed = json.loads(text)
                except Exception:
                    return
                else:
                    yield from _iter_lists(parsed, path)

    car_lists, other_lists = [], []
    for doc in payloads:
        base_obj: Any
        if isinstance(doc, dict):
            base_obj = doc.get("Item") or doc.get("Data") or doc.get("data") or doc
        else:
            base_obj = doc
        for path, arr in _iter_lists(base_obj):
            if isinstance(arr, list) and arr:
                (car_lists if any(k in path.lower() for k in ("banner", "carousel", "ad", "focus", "slider", "swiper"))
                 else other_lists).append((path, arr))
    for _, arr in car_lists:
        if _append_from_list(arr):
            break
    if len(items) < target:
        for _, arr in other_lists:
            if _append_from_list(arr):
                break

    return items[:target] if items else []

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
@app.route("/liff/activities", methods=["GET"])
def liff_activities():
    trace = []
    try:
        try:
            limit = int(request.args.get("limit", "10"))
        except Exception:
            limit = 10
        kw = request.args.get("q") or None
        only_concert = _truthy(request.args.get("onlyConcert"))
        want_debug = _truthy(request.args.get("debug"))

        # 1) 輪播（API + HTML 兜底）
        acts = fetch_ibon_carousel_from_api(limit=limit, keyword=kw, only_concert=only_concert)
        trace.append({"phase": "api_carousel_mixed", "count": len(acts or [])})

        # 2) 一般 API
        if not acts:
            acts = fetch_ibon_list_via_api(limit=limit, keyword=kw, only_concert=only_concert)
            trace.append({"phase": "api_generic", "count": len(acts or [])})

        # 3) HTML 兜底
        if not acts:
            acts = fetch_ibon_entertainments(limit=limit, keyword=kw, only_concert=only_concert)
            trace.append({"phase": "html_fallback", "count": len(acts or [])})
        # 4) 仍為空 → 用瀏覽器引擎（Selenium→Playwright）按輪播抓連結，再還原成 items
        if not acts:
            try:
                urls = grab_ibon_carousel_urls()
                trace.append({"phase": "browser_carousel", "count": len(urls or [])})
                if urls:
                    acts = _items_from_details_urls(urls, limit=limit, keyword=kw, only_concert=only_concert)
            except Exception as e:
                app.logger.warning(f"[browser_fallback] {e}")

        acts = acts or []  # 防 None

        if want_debug:
            return jsonify({"ok": True, "count": len(acts), "preview": acts[:2], "trace": trace}), 200

        return jsonify(acts[:limit]), 200

    except Exception as e:
        app.logger.error(f"/liff/activities error: {e}\n{traceback.format_exc()}")
        want_debug = _truthy(request.args.get("debug"))
        if want_debug:
            return jsonify({"ok": False, "error": str(e), "trace": trace}), 200
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route("/liff/activities_debug", methods=["GET"])
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

@app.route("/liff/", methods=["GET"])
def liff_index():
    return send_from_directory("liff", "index.html")

@app.route("/liff/ping", methods=["GET"])
def liff_ping():
    return jsonify({"ok": True, "time": datetime.utcnow().isoformat()+"Z"}), 200

@app.get("/netcheck")
def netcheck():
    urls = [
        "https://www.google.com",
        "https://ticket.ibon.com.tw/Index/entertainment",
        "https://ticketapi.ibon.com.tw/api/ActivityInfo/GetIndexData",
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