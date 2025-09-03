# -*- coding: utf-8 -*-
import os
import re
import json
import time
import uuid
import hashlib
import logging
import traceback
from datetime import datetime, timezone, timedelta
from urllib.parse import urlparse, urlunparse, parse_qs, urlencode, urljoin
from typing import Dict, Tuple, Optional, Any, List

import requests
from flask import Flask, request, abort, jsonify

# --------- LINE SDK（可選）---------
HAS_LINE = True
try:
    from linebot import LineBotApi, WebhookHandler
    from linebot.exceptions import InvalidSignatureError
    from linebot.models import MessageEvent, TextMessage, TextSendMessage, ImageSendMessage
except Exception as e:
    HAS_LINE = False
    LineBotApi = WebhookHandler = InvalidSignatureError = None
    MessageEvent = TextMessage = TextSendMessage = ImageSendMessage = None
    logging.warning(f"[init] line-bot-sdk not available: {e}")

# --------- Firestore（可失敗不致命）---------
from google.cloud import firestore

# HTML 解析
from bs4 import BeautifulSoup

app = Flask(__name__)
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
app.logger.setLevel(logging.INFO)

# ======== 環境變數 ========
LINE_CHANNEL_ACCESS_TOKEN = os.getenv("LINE_CHANNEL_ACCESS_TOKEN", "")
LINE_CHANNEL_SECRET = os.getenv("LINE_CHANNEL_SECRET", "")
DEFAULT_PERIOD_SEC = int(os.getenv("DEFAULT_PERIOD_SEC", "60"))
ALWAYS_NOTIFY = os.getenv("ALWAYS_NOTIFY", "0") == "1"

# 可選：直接指定某些 PERF_ID 的宣傳圖或 Details 網址
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

def hash_sections(d: dict) -> str:
    items = sorted((k, int(v)) for k, v in d.items())
    raw = json.dumps(items, ensure_ascii=False, separators=(",", ":"))
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

# ---------- 活動資訊與圖片 ----------
def _first_http_url(s: str) -> Optional[str]:
    m = re.search(r'https?://[^\s"\'<>]+', str(s))
    return m.group(0) if m else None

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

def find_activity_image_any(s: str) -> Optional[str]:
    # 1) ActivityImage（最優先）
    m = re.search(r"https?://[^\"'<>]+/image/ActivityImage/[^\s\"'<>]+\.(?:jpg|jpeg|png)", s, flags=re.I)
    if m: return m.group(0)
    # 2) 其他 ticketimg2 圖片
    m = re.search(r"https?://ticketimg2\.azureedge\.net/[^\s\"'<>]+\.(?:jpg|jpeg|png)", s, flags=re.I)
    return m.group(0) if m else None

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

    wanted: Dict[str, str] = {}
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

            # 通用抽取：標題 / 場地 / 時間 / 圖片
            info = _deep_pick_activity_info(data)

            # (1) JSON 內直接有完整 Details 連結
            m = re.search(r'https?://ticket\.ibon\.com\.tw/ActivityInfo/Details/(\d+)', s)
            if m:
                info["details"] = m.group(0)
            else:
                # (2) 找 ID 再拼接
                m = (re.search(r'"ActivityInfoId"\s*:\s*(\d+)', s) or
                     re.search(r'"ActivityId"\s*:\s*(\d+)', s) or
                     re.search(r'"Id"\s*:\s*(\d+)', s))
                if m:
                    info["details"] = f"https://ticket.ibon.com.tw/ActivityInfo/Details/{m.group(1)}"

            # (3) 若還沒有圖片，從 JSON 再掃一次 ActivityImage
            if not info.get("poster"):
                promo = find_activity_image_any(s)
                if promo:
                    info["poster"] = promo

            # 嘗試把含 perf/product id 的那筆資訊疊上去
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
                wanted = info
                break

        except Exception as e:
            app.logger.info(f"[api] fetch fail ({method} {params}): {e}")
            continue

    return wanted

# ---- 解析 ticket.ibon.com.tw/ActivityInfo/Details/{id} ----
def find_details_url_in_html(html: str) -> Optional[str]:
    m = re.search(r"https?://ticket\.ibon\.com\.tw/ActivityInfo/Details/\d+", html)
    return m.group(0) if m else None

def fetch_from_ticket_details(details_url: str, sess: requests.Session) -> Dict[str, str]:
    """到 ticket.ibon 的活動頁抓 title/place/dt/宣傳圖"""
    out: Dict[str, str] = {}
    try:
        r = sess.get(details_url, timeout=12)
        if r.status_code != 200: return out
        html = r.text
        soup = soup_parse(html)

        # 宣傳圖
        mt = soup.select_one('meta[property="og:image"], meta[name="twitter:image"]')
        if mt and mt.get("content"):
            out["poster"] = urljoin(details_url, mt["content"])
        if not out.get("poster"):
            for img in soup.find_all("img"):
                src = (img.get("src") or "").strip()
                if src and any(k in src.lower() for k in ("activityimage", "azureedge", "banner", "cover")):
                    out["poster"] = urljoin(details_url, src); break

        # 標題
        h1 = soup.select_one("h1")
        if h1:
            title = h1.get_text(" ", strip=True)
            if title: out["title"] = title

        # 場地：先找 class/語意名稱帶 location 的元素
        for sel in ['[class*=location]', '.fa-location-dot', '.icon-location', '.address', '.place']:
            el = soup.select_one(sel)
            if el:
                txt = el.find_parent().get_text(" ", strip=True)
                txt = re.sub(r"\s+", " ", txt)
                if txt: out["place"] = txt; break
        if "place" not in out:
            tx = soup.get_text(" ", strip=True)
            m = re.search(r'(TICC[^，\n]{0,40})', tx)
            if m: out["place"] = m.group(1).strip()

        # 時間
        tx = soup.get_text(" ", strip=True)
        m = re.search(r'(\d{4}/\d{2}/\d{2})\s*(?:\([\u4e00-\u9fff]\))?\s*(\d{2}:\d{2})', tx)
        if m: out["dt"] = f"{m.group(1)} {m.group(2)}"

    except Exception as e:
        app.logger.info(f"[details] fetch fail: {e}")
    return out

# ============= 票區與 live.map 解析 =============
_RE_AREA_TAG = re.compile(r"<area\b[^>]*>", re.I)

def extract_area_name_map_from_000(html: str) -> dict:
    name_map: Dict[str, str] = {}
    soup = soup_parse(html)

    # (1) script jsonData
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
                if code and name:
                    name_map.setdefault(code, re.sub(r"\s+", "", name))
        except Exception:
            pass

    # (2) a[href] 直接帶代碼
    for a in soup.select('a[href*="PERFORMANCE_PRICE_AREA_ID="]'):
        href = a.get("href", "")
        m = re.search(r'PERFORMANCE_PRICE_AREA_ID=([A-Za-z0-9]+)', href)
        if not m:
            continue
        code = m.group(1)
        name = a.get_text(" ", strip=True) or a.get("title", "") or ""
        tr = a.find_parent("tr")
        if tr:
            cells = [td.get_text(" ", strip=True) for td in tr.find_all("td")]
            cands = [c for c in cells if re.search(r"(樓|區|包廂)", c) and not re.fullmatch(r"[\d,\.]+", c)]
            if cands:
                name = max(cands, key=len)
        name = re.sub(r"\s+", "", name)
        if name:
            name_map.setdefault(code, name)

    # (3) 全文近鄰
    text = soup.get_text("\n", strip=True)
    for m in re.finditer(r"\b(B0[0-9A-Z]{6,10})\b", text):
        code = m.group(1)
        if code in name_map:
            continue
        start = max(0, m.start() - 120)
        end   = min(len(text), m.end() + 120)
        ctx = text[start:end]
        m2 = re.search(r"([0-9一二三四五六七八九十]+樓[^\s，,。；;]{1,12}區|包廂[^\s，,。；;]{0,12}區|[0-9一二三四五六七八九十]+樓[^\s，,。；;]{1,20})", ctx)
        if m2:
            name_map.setdefault(code, re.sub(r"\s+", "", m2.group(1)))

    return name_map

def _parse_livemap_text(txt: str) -> Tuple[Dict[str, int], int]:
    sections: Dict[str, int] = {}
    total = 0
    for tag in _RE_AREA_TAG.findall(txt):
        # 取第二參數的 AreaId，避免誤用 PerfId
        code = None
        m = re.search(
            r"javascript:Send\([^)]*'(?P<perf>B0[0-9A-Z]{6,10})'\s*,\s*'(?P<area>B0[0-9A-Z]{6,10})'\s*,\s*'(\d+)'",
            tag, re.I)
        if m:
            code = m.group("area")
        else:
            codes = re.findall(r"\b(B0[0-9A-Z]{6,10})\b", tag)
            if codes: code = codes[-1]

        qty = None
        m_title = re.search(r'title="([^"]*)"', tag, re.I)
        title_text = m_title.group(1) if m_title else ""
        nums = [int(n) for n in re.findall(r"(\d+)", title_text)]
        for n in reversed(nums):
            if n < 1000: qty = n; break
        if qty is None:
            m = re.search(r'\bdata-(?:left|remain|qty|count)=["\']?(\d+)["\']?', tag, re.I)
            if m: qty = int(m.group(1))
        if qty is None:
            m = re.search(r'\b(?:alt|aria-label)=["\'][^"\']*?(\d+)[^"\']*["\']', tag, re.I)
            if m: qty = int(m.group(1))

        if code and qty and qty > 0:
            sections[code] = sections.get(code, 0) + qty
            total += qty
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

# --------- 備援解析 ---------
def _parse_counts_from_text(full_text: str) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for m in re.finditer(r"(B0[0-9A-Z]{6,10}).{0,40}?(\d{1,3})\s*張", full_text):
        code, qty = m.group(1), int(m.group(2))
        if qty > 0: counts[code] = counts.get(code, 0) + qty
    return counts

def _parse_counts_from_scripts(soup: BeautifulSoup) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for sc in soup.find_all("script"):
        s = (sc.string or sc.text or "")
        for m in re.finditer(r"(B0[0-9A-Z]{6,10})[^0-9]{0,40}?(\d{1,3})\s*張", s):
            code, qty = m.group(1), int(m.group(2))
            if qty > 0: counts[code] = counts.get(code, 0) + qty
    return counts

def map_counts_to_zones(counts: Dict[str, int], area_name_map: Dict[str, str]) -> Tuple[List[Tuple[str,str,int]], List[Tuple[str,int]]]:
    matched: List[Tuple[str,str,int]] = []
    unmatched: List[Tuple[str,int]] = []
    for k, v in counts.items():
        if re.fullmatch(r"B0[0-9A-Z]{6,10}", k) and k in area_name_map:
            matched.append((area_name_map[k], k, int(v)))
        elif re.fullmatch(r"B0[0-9A-Z]{6,10}", k) and k not in area_name_map:
            unmatched.append((k, int(v)))
        else:
            matched.append((k, k, int(v)))
    return matched, unmatched

# --------- Playwright（可選） ---------
def _try_dynamic_counts(event_url: str, timeout_sec: int = 20) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    try:
        from playwright.sync_api import sync_playwright  # type: ignore
    except Exception as e:
        app.logger.info(f"[dyn] playwright not installed: {e}")
        return counts
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-setuid-sandbox"])
            ctx = browser.new_context(user_agent=UA, locale="zh-TW", java_script_enabled=True)
            page = ctx.new_page()
            live_map_text = {"txt": ""}
            def on_response(resp):
                try:
                    u = resp.url
                    if re.search(r"_live\.map$", u):
                        t = resp.text()
                        if "<area" in t: live_map_text["txt"] = t
                except Exception: pass
            page.on("response", on_response)
            page.goto(event_url, wait_until="networkidle", timeout=timeout_sec * 1000)
            time.sleep(1.0)
            if live_map_text["txt"]:
                secs, total = _parse_livemap_text(live_map_text["txt"])
                if total > 0: counts.update(secs); ctx.close(); browser.close(); return counts
            html = page.content()
            soup = soup_parse(html)
            c = _parse_counts_from_text(soup.get_text("\n", strip=True)) or _parse_counts_from_scripts(soup)
            counts.update(c)
            ctx.close(); browser.close()
    except Exception as e:
        app.logger.info(f"[dyn] fail: {e}")
    return counts

# ---- 圖片（宣傳圖 + 座位圖） & 標題/場地（HTML 版型）----
def pick_event_images_from_000(html: str, base_url: str) -> Tuple[str, Optional[str]]:
    poster = LOGO
    seatmap = None
    try:
        soup = soup_parse(html)

        # seatmap
        for img in soup.find_all("img"):
            src = (img.get("src") or "").strip()
            if src and "static_bigmap" in src.lower():
                seatmap = urljoin(base_url, src); break
        if not seatmap:
            m = re.search(r'https?://[^\s"\'<>]+static_bigmap[^\s"\'<>]+?\.(?:jpg|jpeg|png)', html, flags=re.I)
            if m: seatmap = m.group(0)

        # 宣傳圖：先掃全文 ActivityImage / ticketimg2
        promo = find_activity_image_any(html)
        if promo:
            poster = promo
        else:
            # 退回 og/twitter image
            soup = soup_parse(html)
            for sel in ['meta[property="og:image"]', 'meta[name="twitter:image"]']:
                m = soup.select_one(sel)
                if m and m.get("content"):
                    poster = urljoin(base_url, m["content"]); break
            if poster == LOGO:
                for img in soup.find_all("img"):
                    src = (img.get("src") or "").strip()
                    if src and any(k in src.lower() for k in ("activityimage","azureedge")):
                        poster = urljoin(base_url, src); break
    except Exception as e:
        app.logger.warning(f"[image] pick failed: {e}")
    return poster, seatmap

def extract_title_place_from_html(html: str) -> tuple[Optional[str], Optional[str], Optional[str]]:
    """從頁面 DOM 近鄰標籤/列表版型/圖片 alt/title 推斷 (title, place, datetime)"""
    soup = soup_parse(html)

    title: Optional[str] = None
    place: Optional[str] = None
    dt_text: Optional[str] = None

    # list-grid 版型（.grid-title / .grid-content）
    for gt in soup.select('.grid-title'):
        lab = gt.get_text(" ", strip=True)
        sib = gt.find_next_sibling()
        if not sib:
            continue
        content = sib.get_text(" ", strip=True)
        if not content:
            continue
        if ("活動名稱" in lab or "演出名稱" in lab or "節目名稱" in lab or "場次名稱" in lab) and not title:
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

    # 圖片（宣傳圖 + 座位圖）
    poster, seatmap = pick_event_images_from_000(html, url)
    if seatmap: out["seatmap"] = seatmap
    out["image"] = poster or LOGO

    # 先打 API 拿活動資訊（能抓就抓）
    api_info: Dict[str, str] = {}
    try:
        api_info = fetch_game_info_from_api(perf_id, product_id, url, sess)
    except Exception as e:
        app.logger.info(f"[api] fail: {e}")

    # HTML 強化抽取
    html_title, html_place, html_dt = extract_title_place_from_html(html)

    # 可能的 Details 連結（頁面找不到就看環境變數）
    details_url = (
        find_details_url_in_html(html)      # 先從 000 頁掃
        or api_info.get("details")          # 再用 API 自動找
        or (PROMO_DETAILS_MAP.get(perf_id) if perf_id else None)  # 寫死對照留作最後備援
    )

    details_info: Dict[str, str] = {}
    if details_url:
        details_info = fetch_from_ticket_details(details_url, sess)

    # 宣傳圖覆蓋優先順序：環境變數 > Details > API > 頁面 > LOGO
    if perf_id and perf_id in PROMO_IMAGE_MAP:
        out["image"] = PROMO_IMAGE_MAP[perf_id]
    elif details_info.get("poster"):
        out["image"] = details_info["poster"]
    elif api_info.get("poster"):
        out["image"] = api_info["poster"]

    # 標題 / 場地 / 時間（Details > API > HTML）
    out["title"] = details_info.get("title") or api_info.get("title") or html_title or "（未取到標題）"
    out["place"] = details_info.get("place") or api_info.get("place") or html_place or "（未取到場地）"
    out["date"]  = details_info.get("dt")    or api_info.get("dt")    or html_dt    or "（未取到日期）"

    # 票區中文名對照
    area_name_map = extract_area_name_map_from_000(html)
    out["area_names"] = area_name_map

    # 票數
    sections_by_code, total = try_fetch_livemap_by_perf(perf_id, sess, html=html)
    counts: Dict[str, int] = {}
    if total <= 0:
        counts = _parse_counts_from_text(soup.get_text("\n", strip=True)) or _parse_counts_from_scripts(soup)
    if total <= 0 and not counts:
        counts = _try_dynamic_counts(url)

    if total > 0 or counts:
        if not counts: counts = sections_by_code
        matched, unmatched = map_counts_to_zones(counts, area_name_map)
        human: Dict[str, int] = {}
        for name, code, n in matched: human[name] = human.get(name, 0) + int(n)
        for code, n in unmatched:     human[code] = human.get(code, 0) + int(n)
        total = sum(human.values())
        out["sections"] = human
        out["total"] = total
        out["ok"] = total > 0
        out["sig"] = hash_sections(human) if total > 0 else "NA"
        if total > 0:
            lines = ["✅ 監看結果：目前可售"]
            for k, v in sorted(human.items(), key=lambda x: (-x[1], x[0])):
                lines.append(f"{k}: {v} 張")
            lines.append(f"合計：{total} 張")
            out["msg"] = "\n".join(lines) + f"\n{url}"
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
    "我是票券監看機器人 🤖\n"
    "指令：\n"
    "/start 或 /help － 顯示這個說明\n"
    "/watch <URL> [秒] － 開始監看（同網址不重複；秒數可更新；最小 15 秒）\n"
    "/unwatch <任務ID> － 停用任務\n"
    "/list － 顯示啟用中任務（/list all 看全部、/list off 看停用）\n"
    "/check <URL|任務ID> － 立刻手動查詢該頁剩餘數\n"
    "/probe <URL> － 回傳診斷 JSON（除錯用）\n"
)

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

# --- 替換 fs_list ---
def fs_list(chat_id: str, show: str = "on"):
    if not FS_OK:
        return []
    q = fs_client.collection(COL).where("chat_id", "==", chat_id)
    if show == "on":
        q = q.where("enabled", "==", True)
    elif show == "off":
        q = q.where("enabled", "==", False)

    # 有索引就排序，沒有索引就退回不排序
    try:
        cur = q.order_by("updated_at", direction=firestore.Query.DESCENDING).stream()
    except Exception as e:
        app.logger.info(f"[fs_list] no index for order_by(updated_at): {e}; fallback to unsorted")
        cur = q.stream()
    return [d.to_dict() for d in cur]

def fs_disable(chat_id: str, tid: str) -> bool:
    doc = fs_get_task_by_id(chat_id, tid)
    if not doc: return False
    fs_client.collection(COL).document(doc.id).update({
        "enabled": False, "updated_at": datetime.now(timezone.utc),
    })
    return True

def fmt_result_text(res: dict) -> str:
    lines = [f"🎫 {res.get('title','')}".strip(),
             f"地點：{res.get('place','')}",
             f"日期：{res.get('date','')}"]
    if res.get("ok"):
        lines.append("\n✅ 監看結果：目前可售")
        for k, v in sorted(res.get("sections", {}).items(), key=lambda x: (-x[1], x[0])):
            lines.append(f"{k}: {v} 張")
        lines.append(f"合計：{res.get('total',0)} 張")
    else:
        lines.append("\n暫時讀不到剩餘數（可能為動態載入）。")
    lines.append(res.get("url", ""))
    return "\n".join(lines)

def handle_command(text: str, chat_id: str):
    try:
        parts = text.strip().split()
        cmd = parts[0].lower()
        if cmd in ("/start", "/help"):
            return [TextSendMessage(text=HELP)] if HAS_LINE else [fmt_result_text({"msg": HELP})]

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
            mode = "on"
            if len(parts) >= 2 and parts[1].lower() in ("all", "off"):
                mode = parts[1].lower()

            rows = fs_list(chat_id, show=mode)  # fs_list 內已對 order_by 做 try/except 容錯
            if not rows:
                out = "（沒有任務）"
                return [TextSendMessage(text=out)] if HAS_LINE else [out]

            lines = ["你的任務："]
            for r in rows:
                rid = r.get("id", "?")
                state = "啟用" if r.get("enabled") else "停用"
                period = r.get("period", "?")
                u = r.get("url", "")
                lines.append(f"{rid}｜{state}｜{period}s\n{u}")

            big = "\n\n".join(lines)

            # 分段（LINE 單訊息最大 ~5000 字，留點緩衝）
            chunks = [big[i:i+4800] for i in range(0, len(big), 4800)]
            if HAS_LINE:
                return [TextSendMessage(text=c) for c in chunks]
            else:
                return chunks

        if cmd == "/check" and len(parts) >= 2:
            target = parts[1].strip()
            if target.lower().startswith("http"):
                url = target
            else:
                doc = fs_get_task_by_id(chat_id, target)
                if not doc:
                    msg = "找不到該任務 ID"
                    return [TextSendMessage(text=msg)] if HAS_LINE else [msg]
                url = doc.to_dict().get("url")
            res = probe(url)
            msgs: List[Any] = []
            if HAS_LINE:
                if res.get("image") and res["image"] != LOGO:
                    msgs.append(ImageSendMessage(original_content_url=res["image"], preview_image_url=res["image"]))
                if res.get("seatmap"):
                    sm = res["seatmap"]
                    msgs.append(ImageSendMessage(original_content_url=sm, preview_image_url=sm))
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
    @handler.add(MessageEvent, message=TextMessage)
    def on_message(ev):
        text = ev.message.text.strip()
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
                    text_out = fmt_result_text(res)
                    img = res.get("image", "")
                    seatmap = res.get("seatmap")
                    chat_id = r.get("chat_id")
                    if img and img != LOGO: send_image(chat_id, img)
                    if seatmap: send_image(chat_id, seatmap)
                    send_text(chat_id, text_out)
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