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
from urllib.parse import urlparse, urlunparse, parse_qs, urlencode, urljoin
from flask import send_from_directory
from flask import jsonify, Flask, request, abort

IBON_API = "https://ticketapi.ibon.com.tw/api/ActivityInfo/GetIndexData"
IBON_BASE = "https://ticket.ibon.com.tw/"

# 簡單快取（5 分鐘）
_cache = {"ts": 0, "data": []}
_CACHE_TTL = 300  # 秒

_CONCERT_WORDS = ("演唱會", "演場會", "音樂會", "演唱", "演出", "LIVE", "Live")

def _looks_like_concert(title: str) -> bool:
    t = title or ""
    return any(w.lower() in t.lower() for w in _CONCERT_WORDS)

def _normalize_item(row):
    """
    將 ibon API 的活動項目轉為 {title,url,image}
    兼容不同欄位名稱。
    """
    title = row.get("Title") or row.get("ActivityTitle") or row.get("Name") or "活動"
    img = (row.get("ImgUrl") or row.get("ImageUrl") or row.get("Image") or "").strip() or None

    # 可能直接給 Url，也可能只有 ActivityId/Id
    url = (row.get("Url") or row.get("LinkUrl") or "").strip()
    if not url:
        act_id = row.get("ActivityId") or row.get("Id") or row.get("ID")
        if act_id:
            url = urljoin(IBON_BASE, f"/ActivityInfo/Details/{act_id}")

    # 統一為絕對網址
    if url and not url.lower().startswith("http"):
        url = urljoin(IBON_BASE, url)

    # 有些圖片給相對路徑
    if img and not img.lower().startswith("http"):
        img = urljoin(IBON_BASE, img)

    return {"title": title.strip(), "url": url, "image": img}

def fetch_ibon_list_via_api(limit=10, keyword=None, only_concert=False):
    """
    直接打 ibon 官方 API 抽取活動清單。
    回傳：[{title, url, image}, ...]
    """
    global _cache
    now = time.time()
    if now - _cache["ts"] < _CACHE_TTL and _cache["data"]:
        rows = _cache["data"]
    else:
        headers = {
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json, text/plain, */*",
            "Origin": "https://ticket.ibon.com.tw",
            "Referer": "https://ticket.ibon.com.tw/Index/entertainment",
        }
        # 多數情況用 POST；若 GET 也能回資料，可改成 requests.get(...)
        try:
            resp = requests.post(IBON_API, headers=headers, timeout=12, json={})
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            logging.getLogger().error(f"[ibon api] request failed: {e}")
            return []

        # 解析可能的包裝層
        # 常見層級：{ "Data": { <多個區塊> } } 或 { "data": [...] } 等
        blobs = []
        root = data.get("Data") or data.get("data") or data
        if isinstance(root, dict):
            # 收集所有陣列欄位（包含 "ActivityList", "Items" 等）
            for v in root.values():
                if isinstance(v, list):
                    blobs.extend(v)
                elif isinstance(v, dict) and isinstance(v.get("ActivityList"), list):
                    blobs.extend(v["ActivityList"])
        elif isinstance(root, list):
            blobs = root

        # 最後再保底：如果還是 dict，找出所有內層 list
        if not blobs and isinstance(data, dict):
            for v in data.values():
                if isinstance(v, list):
                    blobs.extend(v)

        rows = []
        for r in blobs:
            try:
                item = _normalize_item(r)
                if not item.get("url"):
                    continue
                rows.append(item)
            except Exception:
                continue

        _cache = {"ts": now, "data": rows}

    # 關鍵字/只要演唱會的過濾
    out = []
    kw = (keyword or "").strip()
    for it in rows:
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

# HTML 解析
from bs4 import BeautifulSoup

# ===== Flask & CORS =====
app = Flask(__name__)

# 建議：白名單（可多個網域）
# 1) 一定要包含 LIFF 的網域： https://liff.line.me
# 2) 再加入你 Cloud Run 的完整網址（請改成你的實際網址）
ALLOWED_ORIGINS = [
    "https://liff.line.me",
    "https://ticketsearch-419460755270.asia-east1.run.app",  # ← 換成你的
]

try:
    from flask_cors import CORS  # type: ignore
    # 只開 /liff/* 路徑（/liff/activities、/liff/ 等）
    CORS(
        app,
        resources={
            r"/liff/*": {
                "origins": ALLOWED_ORIGINS,
                # 可視需要補上允許的方法/標頭（預設就夠用）
                # "methods": ["GET", "OPTIONS"],
                # "allow_headers": ["Content-Type"],
            }
        },
        supports_credentials=True,  # 你前端 fetch 有帶 credentials 時需要
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
FOLLOW_AREAS_PER_CHECK = int(os.getenv("FOLLOW_AREAS_PER_CHECK", "0"))  # 預設 0：不追票區第二步頁

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
    """將『有數字區』與『熱賣中區』一起做簽章，避免只看數字漏通知。"""
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
        m = re.search(r'(\d{4}/\d{2}/\d{2})\s*(?:\([\u4e00-\u9fff]\))?\s*(\d{2}:\d{2})', tx)
        if m: out["dt"] = f"{m.group(1)} {m.group(2)}"

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

    # 取標題 / 場地
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
    """
    從 UTK0201_000 抽出：
      - name_map   : 區代碼 -> 中文名稱
      - status_map : 區代碼 -> 狀態字（熱賣中/已售完/…）
      - qty_map    : 區代碼 -> 表格「空位」欄的數字（若有）
      - order_map  : 區代碼 -> 顯示順序（jsonData SORT；否則表格列順）
    """
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
        order_map.setdefault(code, 10000 + row_idx)  # 沒 SORT 就用表格序

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
    """只認 data-left / 關鍵字『剩餘|尚餘|可售|可購』或『(\d+) 張』；同一區取最大值。"""
    sections: Dict[str, int] = {}
    for tag in _RE_AREA_TAG.findall(txt):
        code = None
        m = re.search(
            r"javascript:Send\([^)]*'(?P<perf>B0[0-9A-Z]{6,10})'\s*,\s*'(?P<area>B0[0-9A-Z]{6,10})'",
            tag, re.I
        )
        if m: code = m.group("area")
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
    soup = soup_parse(html)

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

    # live.map 數字（僅取可信數字，且同區取最大值）
    sections_by_code, _ = try_fetch_livemap_by_perf(perf_id, sess, html=html)
    numeric_counts: Dict[str, int] = dict(sections_by_code)
    # 表格「空位」數字補進來（live.map 沒有的才補）
    for code, n in area_qty_map.items():
        if isinstance(n, int) and n > 0 and code not in numeric_counts:
            numeric_counts[code] = n

    # 只知道「熱賣中/可售」但沒數字
    selling_unknown_codes: List[str] = []
    for code, status in area_status_map.items():
        if (status and ("熱賣" in status or "可售" in status)) and not numeric_counts.get(code):
            selling_unknown_codes.append(code)

    # 針對「熱賣中」但沒有數字的區：是否進第二步頁補抓？
    if FOLLOW_AREAS_PER_CHECK > 0 and perf_id and product_id and area_name_map:
        need_follow = [code for code, st in area_status_map.items()
                       if (st and "熱賣" in st) and (code not in numeric_counts)]
        for code in need_follow[:FOLLOW_AREAS_PER_CHECK]:
            n = fetch_area_left_from_utk0101(url, perf_id, product_id, code, sess)
            if isinstance(n, int) and n > 0:
                numeric_counts[code] = n

    # 再次聚集 selling_unknown（確保覆蓋後仍為未知）
    selling_unknown_codes = [
        code for code, amt in area_status_map.items()
        if (amt and ("熱賣" in amt or "可售" in amt)) and not numeric_counts.get(code)
    ]

    # ==== 聚合輸出 ====
    # 把代碼映成人類名稱（同名區取最大值；不把「熱賣中(未知)」算進 total）
    human_numeric: Dict[str, int] = {}
    for code, n in numeric_counts.items():
        name = area_name_map.get(code, code)
        v = int(n)
        human_numeric[name] = max(human_numeric.get(name, 0), v)

    # 順序：依網站順序（SORT 或表格列序）
    def order_key(name: str) -> tuple:
        codes = [c for c, nm in area_name_map.items() if nm == name]
        order_vals = [area_order_map.get(c, 99999) for c in codes] or [99999]
        return (min(order_vals), name)

    ordered_names = sorted(human_numeric.keys(), key=order_key)
    selling_names = sorted({area_name_map.get(code, code) for code in selling_unknown_codes}, key=order_key)

    total_num = sum(human_numeric.values())

    # ---- 售完偵測：所有票區皆「已售完」，且沒有數字與「熱賣/可售」標記 ----
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

    # 簽章：把售完狀態也納入，避免「售完 <-> 有票」時不觸發通知
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

    # ---- 無數字、無熱賣：若判定為售完 → 回覆售完；否則保留原本說明 ----
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
    s = sess_default()
    p = urlparse(url)
    if "orders.ibon.com.tw" in p.netloc and p.path.upper().endswith("/UTK0201_000.ASPX"):
        return parse_UTK0201_000(url, s)
    r = s.get(url, timeout=12)
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

# === 全域只回覆指令：新增判斷（半形 / 全形斜線） ===
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
    # 若為售完狀態，優先輸出售完訊息
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
                # 先座位圖、後宣傳圖
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
        # 被加入好友時：回覆一次（等同 /start 內容）
        try:
            line_bot_api.reply_message(ev.reply_token, [TextSendMessage(text=WELCOME_TEXT)])
        except Exception as e:
            app.logger.error(f"[follow] reply failed: {e}")

    @handler.add(JoinEvent)
    def on_join(ev):
        # 被邀入群/聊天室時：回覆一次（等同 /start 內容）
        try:
            line_bot_api.reply_message(ev.reply_token, [TextSendMessage(text=WELCOME_TEXT)])
        except Exception as e:
            app.logger.error(f"[join] reply failed: {e}")

    @handler.add(MessageEvent, message=TextMessage)
    def on_message(ev):
        # 全域規則：只有「指令」（以 / 或 ／ 開頭）才回覆，其餘忽略
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

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8080")))

def fetch_ibon_entertainments(limit=10, keyword=None):
    url = "https://ticket.ibon.com.tw/Index/entertainment"
    try:
        r = requests.get(url, timeout=12, headers={
            "User-Agent": "Mozilla/5.0",
            "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.6",
        })
        r.raise_for_status()
        try:
            soup = BeautifulSoup(r.text, "lxml")
        except Exception:
            soup = BeautifulSoup(r.text, "html.parser")

        items, seen = [], set()
        for a in soup.select('a[href*="/ActivityInfo/Details/"]'):
            href = urljoin(url, (a.get("href") or "").strip())
            if href in seen:
                continue
            seen.add(href)
            title = (a.get("title") or a.get_text(" ", strip=True) or "活動").strip()
            if keyword and keyword not in title:
                continue
            img = None
            img_tag = a.select_one("img")
            if img_tag and img_tag.get("src"):
                img = urljoin(url, img_tag["src"])
            items.append({"title": title, "url": href, "image": img})
            if len(items) >= max(1, int(limit)):
                break
        return items
    except Exception as e:
        app.logger.error(f"[ent] fetch failed: {e}")
        return []

@app.route("/liff/activities", methods=["GET"])
def liff_activities():
    try:
        limit = int(request.args.get("limit", "10"))
    except Exception:
        limit = 10
    kw = request.args.get("q") or None
    try:
        acts = fetch_ibon_entertainments(limit=limit, keyword=kw)
        return jsonify(acts), 200
    except Exception as e:
        app.logger.error(f"/liff/activities error: {e}")
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route("/liff/", methods=["GET"])
def liff_index():
    return send_from_directory("liff", "index.html")