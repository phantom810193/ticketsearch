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

if not LINE_CHANNEL_ACCESS_TOKEN or not LINE_CHANNEL_SECRET:
    app.logger.warning("LINE env not set: LINE_CHANNEL_ACCESS_TOKEN / LINE_CHANNEL_SECRET")

line_bot_api = LineBotApi(LINE_CHANNEL_ACCESS_TOKEN) if (HAS_LINE and LINE_CHANNEL_ACCESS_TOKEN) else None
handler = WebhookHandler(LINE_CHANNEL_SECRET) if (HAS_LINE and LINE_CHANNEL_SECRET) else None

MAX_PER_TICK = int(os.getenv("MAX_PER_TICK", "6"))                     # 每次最多處理幾個任務
TICK_SOFT_DEADLINE_SEC = int(os.getenv("TICK_SOFT_DEADLINE_SEC", "50"))  # 軟性截止(秒)

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

_RE_DATE = re.compile(r"(\d{4}/\d{2}/\d{2}\s+\d{2}:\d{2})")
_RE_AREA_TAG = re.compile(r"<area\b[^>]*>", re.I)

LOGO = "https://ticketimg2.azureedge.net/logo.png"

# ibon 官方 API（活動資訊）
TICKET_API = "https://ticketapi.ibon.com.tw/api/ActivityInfo/GetGameInfoList"

# ================= 小工具 =================
def soup_parse(html: str) -> BeautifulSoup:
    """優先用 lxml，沒有再退回 html.parser。"""
    try:
        return BeautifulSoup(html, "lxml")
    except Exception:
        return BeautifulSoup(html, "html.parser")

def now_ts() -> float:
    return time.time()

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

# ---------- API 活動資訊 ----------
def _first_http_url(s: str) -> Optional[str]:
    m = re.search(r'https?://[^\s"\'<>]+', str(s))
    return m.group(0) if m else None

def _deep_pick_activity_info(data: Any) -> Dict[str, str]:
    """很耐髒的遞迴蒐集器：從 API 回傳 JSON 裡抓常見鍵名。"""
    out: Dict[str, Optional[str]] = {"title": None, "place": None, "dt": None, "poster": None}

    def walk(x):
        if isinstance(x, dict):
            for k, v in x.items():
                kl = str(k).lower()
                if not out["title"] and any(t in kl for t in ("activityname","gamename","title","actname","activity_title")):
                    if isinstance(v, str) and v.strip(): out["title"] = v.strip()
                if not out["place"] and any(t in kl for t in ("placename","venue","place","site","location")):
                    if isinstance(v, str) and v.strip(): out["place"] = v.strip()
                if not out["dt"] and any(t in kl for t in ("starttime","startdatetime","gamedatetime","gamedate","begindatetime","datetime")):
                    s = str(v)
                    m = re.search(r"(\d{4})[-/](\d{1,2})[-/](\d{1,2})[\sT]+(\d{1,2}):(\d{2})", s)
                    if m:
                        out["dt"] = f"{int(m.group(1))}/{int(m.group(2)):02d}/{int(m.group(3)):02d} {int(m.group(4)):02d}:{m.group(5)}"
                if not out["poster"] and "image" in kl:
                    url = _first_http_url(v) if isinstance(v, str) else None
                    if url: out["poster"] = url
            for v in x.values(): walk(v)
        elif isinstance(x, list):
            for it in x: walk(it)

    walk(data)
    return {k: v for k, v in out.items() if v}

def find_activity_image(html: str) -> Optional[str]:
    """從整頁字串直接撈 ActivityImage 的宣傳圖（有時不在 <img> 或 meta 裡）。"""
    m = re.search(r"https?://[^\"'<>]+/image/ActivityImage/[^\s\"'<>]+\.(?:jpg|jpeg|png)", html, flags=re.I)
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
            ("GET", {"Performance_ID": perf_id, "Product_ID": product_id}),
            ("GET", {"PerformanceId": perf_id, "ProductId": product_id}),
            ("GET", {"PERFORMANCE_ID": perf_id, "PRODUCT_ID": product_id}),
            ("POST", {"Performance_ID": perf_id, "Product_ID": product_id}),
            ("POST", {"PerformanceId": perf_id, "ProductId": product_id}),
        ]
    tries.append(("GET", {}))  # 無參也試一次

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

            info = _deep_pick_activity_info(data)

            # 若回傳為清單，盡量找包含 perf/product id 的那筆
            def match_obj(obj):
                s = json.dumps(obj, ensure_ascii=False)
                ok = True
                if perf_id:
                    ok = ok and (perf_id in s)
                if product_id:
                    ok = ok and (product_id in s)
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
                # 宣傳圖優先選 ActivityImage
                if info.get("poster") and "ActivityImage" not in info["poster"]:
                    promo = find_activity_image(json.dumps(data, ensure_ascii=False))
                    if promo:
                        info["poster"] = promo
                wanted = info
                break
        except Exception as e:
            app.logger.info(f"[api] fetch fail ({method} {params}): {e}")
            continue
    return wanted

# ============= ibon 解析 =============
def pick_event_images_from_000(html: str, base_url: str) -> Tuple[str, Optional[str]]:
    """回傳 (poster, seatmap)。poster 優先 ActivityImage/azureedge；seatmap 尋找 static_bigmap。"""
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

        # poster（og: / twitter:）
        for sel in ['meta[property="og:image"]', 'meta[name="twitter:image"]']:
            m = soup.select_one(sel)
            if m and m.get("content"):
                poster = urljoin(base_url, m["content"]); break

        # 次選：頁面上的 ActivityImage/azureedge
        if poster == LOGO:
            for img in soup.find_all("img"):
                src = (img.get("src") or "").strip()
                if src and any(k in src.lower() for k in ("activityimage","azureedge")):
                    poster = urljoin(base_url, src); break

        # 最後：挑最大 img
        if poster == LOGO:
            best = None; best_area = -1
            for img in soup.find_all("img"):
                src = (img.get("src") or "").strip()
                if not src: continue
                w = int(img.get("width") or 0) or 0
                h = int(img.get("height") or 0) or 0
                area = w*h
                if area > best_area:
                    best = src; best_area = area
            if best: poster = urljoin(base_url, best)
    except Exception as e:
        app.logger.warning(f"[image] pick failed: {e}")
    return poster, seatmap

def extract_area_name_map_from_000(html: str) -> dict:
    """
    從 000 頁面抽 {票區代碼 -> 區域中文名}。
    來源：
      1) script 的 const jsonData = ' [...] '（最可靠）
      2) a[href*=PERFORMANCE_PRICE_AREA_ID=...]
      3) 頁面全文近鄰（B0… 附近含「樓/區/包廂」）
    """
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
    """解析 azureedge live.map，回傳 {票區代碼: 張數}, total。避免把 PerfId 當成區代碼。"""
    sections: Dict[str, int] = {}
    total = 0
    for tag in _RE_AREA_TAG.findall(txt):
        # 1) 優先從 javascript:Send('PERF','AREA','qty') 取「第二個」參數 → AreaId
        code = None
        m = re.search(
            r"javascript:Send\([^)]*'(?P<perf>B0[0-9A-Z]{6,10})'\s*,\s*'(?P<area>B0[0-9A-Z]{6,10})'\s*,\s*'(\d+)'",
            tag, re.I)
        if m:
            code = m.group("area")
        else:
            # 2) 退而求其次：同一個 tag 內若有多個 B0…，通常最後一個是 AreaId
            codes = re.findall(r"\b(B0[0-9A-Z]{6,10})\b", tag)
            if codes:
                code = codes[-1]

        # 數量
        qty = None
        m_title = re.search(r'title="([^"]*)"', tag, re.I)
        title_text = m_title.group(1) if m_title else ""
        nums = [int(n) for n in re.findall(r"(\d+)", title_text)]
        for n in reversed(nums):
            if n < 1000:
                qty = n; break
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
    """從多個可能路徑猜 live.map；若頁面有 static_bigmap，則優先用其資料夾。"""
    if not perf_id:
        return {}, 0

    bases = [f"https://qwareticket-asysimg.azureedge.net/QWARE_TICKET/images/Temp/{perf_id}/"]

    # 由 static_bigmap 反推 images/UTKxxxx/
    if html:
        poster, seatmap = pick_event_images_from_000(html, "https://orders.ibon.com.tw/")
        if seatmap:
            m = re.match(r'(https?://.*/images/[^/]+/)', seatmap)
            if m:
                bases.insert(0, m.group(1))  # 優先用這個

    prefixes = ["", "1_", "2_", "3_", "01_", "02_", "03_"]
    tried = set()
    for base in bases:
        for pref in prefixes:
            url = f"{base}{pref}{perf_id}_live.map"
            if url in tried:
                continue
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

# --------- 輔助：從文字/script 粗略解析數量（做為 live.map 的備援） ---------
def _parse_counts_from_text(full_text: str) -> Dict[str, int]:
    """從整頁純文字找 (B0... , 數字) 的配對；數字後面常見 '張'。"""
    counts: Dict[str, int] = {}
    for m in re.finditer(r"(B0[0-9A-Z]{6,10}).{0,40}?(\d{1,3})\s*張", full_text):
        code, qty = m.group(1), int(m.group(2))
        if qty > 0:
            counts[code] = counts.get(code, 0) + qty
    return counts

def _parse_counts_from_scripts(soup: BeautifulSoup) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for sc in soup.find_all("script"):
        s = (sc.string or sc.text or "")
        for m in re.finditer(r"(B0[0-9A-Z]{6,10})[^0-9]{0,40}?(\d{1,3})\s*張", s):
            code, qty = m.group(1), int(m.group(2))
            if qty > 0:
                counts[code] = counts.get(code, 0) + qty
    return counts

def map_counts_to_zones(counts: Dict[str, int], area_name_map: Dict[str, str]) -> Tuple[List[Tuple[str,str,int]], List[Tuple[str,int]]]:
    """
    把 {code/name: qty} 分成兩群：
    - matched: [(中文名, code, qty)]
    - unmatched: [(code_or_name, qty)]
    """
    matched: List[Tuple[str,str,int]] = []
    unmatched: List[Tuple[str,int]] = []
    for k, v in counts.items():
        if re.fullmatch(r"B0[0-9A-Z]{6,10}", k) and k in area_name_map:
            matched.append((area_name_map[k], k, int(v)))
        elif re.fullmatch(r"B0[0-9A-Z]{6,10}", k) and k not in area_name_map:
            unmatched.append((k, int(v)))
        else:
            # k 本身就是中文名
            matched.append((k, k, int(v)))
    return matched, unmatched

# --------- Playwright 動態備援（可選） ---------
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
                        if "<area" in t:
                            live_map_text["txt"] = t
                except Exception:
                    pass

            page.on("response", on_response)
            page.goto(event_url, wait_until="networkidle", timeout=timeout_sec * 1000)
            time.sleep(1.0)

            # 若攔到 live.map，直接解析
            if live_map_text["txt"]:
                secs, total = _parse_livemap_text(live_map_text["txt"])
                if total > 0:
                    counts.update(secs)
                    ctx.close(); browser.close()
                    return counts

            # 否則用頁面文字備援
            html = page.content()
            soup = soup_parse(html)
            c = _parse_counts_from_text(soup.get_text("\n", strip=True)) or _parse_counts_from_scripts(soup)
            counts.update(c)

            ctx.close()
            browser.close()
    except Exception as e:
        app.logger.info(f"[dyn] fail: {e}")
    return counts

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

    # 圖片：座位圖 + 主宣傳圖（若頁面字串找到 ActivityImage，優先用）
    poster, seatmap = pick_event_images_from_000(html, url)
    promo = find_activity_image(html)
    if promo:
        poster = promo
    if seatmap: out["seatmap"] = seatmap
    out["image"] = poster or LOGO

    # 先打 API 拿標題/場地/時間/宣傳圖
    api_info: Dict[str, str] = {}
    try:
        api_info = fetch_game_info_from_api(perf_id, product_id, url, sess)
    except Exception as e:
        app.logger.info(f"[api] fail: {e}")

    # 標題
    title = api_info.get("title") or ""
    if not title:
        m = soup.select_one("title")
        if m and m.text.strip(): title = m.text.strip().replace("ibon售票系統","").strip()
        mt = soup.select_one('meta[property="og:title"]')
        if not title and mt and mt.get("content"): title = mt["content"].strip()
    out["title"] = title or "（未取到標題）"

    # 場地
    place = api_info.get("place") or ""
    if not place:
        for lab in ("地點","場地","地區"):
            node = soup.find(lambda t: t.name in ("th","td","span","div") and t.get_text(strip=True) == lab)
            if not node: continue
            tr = node.find_parent("tr")
            if tr:
                tds = tr.find_all("td")
                if len(tds) >= 2:
                    place = tds[-1].get_text(" ", strip=True)
                    if place: break
    out["place"] = place or "（未取到場地）"

    # 日期/時間
    dt_text = api_info.get("dt") or ""
    if not dt_text:
        m = _RE_DATE.search(html)
        if m: dt_text = re.sub(r"\s+", " ", m.group(1)).strip()
    out["date"] = dt_text or "（未取到日期）"

    # 票區中文名對照
    area_name_map = extract_area_name_map_from_000(html)
    out["area_names"] = area_name_map

    # 票數：live.map → 靜態/腳本 → 動態
    sections_by_code, total = try_fetch_livemap_by_perf(perf_id, sess, html=html)

    counts: Dict[str, int] = {}
    if total <= 0:
        counts = _parse_counts_from_text(soup.get_text("\n", strip=True)) or _parse_counts_from_scripts(soup)
    if total <= 0 and not counts:
        counts = _try_dynamic_counts(url)

    if total > 0 or counts:
        if not counts:
            counts = sections_by_code  # live.map 命中
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
    """入口：目前只針對 UTK0201_000 處理，其餘網址原樣回報。"""
    s = sess_default()
    p = urlparse(url)
    if "orders.ibon.com.tw" in p.netloc and p.path.upper().endswith("/UTK0201_000.ASPX"):
        return parse_UTK0201_000(url, s)
    # 其他網址：僅回基本訊息
    r = s.get(url, timeout=12)
    title = ""
    try:
        soup = soup_parse(r.text)
        if soup.title and soup.title.text:
            title = soup.title.text.strip()
    except Exception:
        pass
    return {
        "ok": False,
        "sig": "NA",
        "url": url,
        "image": LOGO,
        "title": title or "（未取到標題）",
        "place": "",
        "date": "",
        "msg": url,
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
    for d in q:
        return d
    return None

def fs_get_task_by_id(chat_id: str, tid: str):
    if not FS_OK: return None
    q = (fs_client.collection(COL)
         .where("chat_id", "==", chat_id)
         .where("id", "==", tid)
         .limit(1).stream())
    for d in q:
        return d
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
            "period": sec,
            "enabled": True,
            "updated_at": now,
        })
        return doc.to_dict()["id"], False
    tid = make_task_id()
    fs_client.collection(COL).add({
        "id": tid,
        "chat_id": chat_id,
        "url": url,
        "url_canon": url_c,
        "period": sec,
        "enabled": True,
        "created_at": now,
        "updated_at": now,
        "last_sig": "",
        "last_total": 0,
        "last_ok": False,
        "next_run_at": now,
    })
    return tid, True

def fs_list(chat_id: str, show: str = "on"):
    if not FS_OK:
        return []
    q = fs_client.collection(COL).where("chat_id", "==", chat_id)
    if show == "on":
        q = q.where("enabled", "==", True)
    elif show == "off":
        q = q.where("enabled", "==", False)
    try:
        cur = q.order_by("updated_at", direction=firestore.Query.DESCENDING).stream()
    except Exception as e:
        app.logger.warning(f"[fs_list] order_by fallback: {e}")
        cur = q.stream()
    return [d.to_dict() for d in cur]

def fs_disable(chat_id: str, tid: str) -> bool:
    doc = fs_get_task_by_id(chat_id, tid)
    if not doc: return False
    fs_client.collection(COL).document(doc.id).update({
        "enabled": False,
        "updated_at": datetime.now(timezone.utc),
    })
    return True

def fmt_result_text(res: dict) -> str:
    lines = [f"🎫 {res.get('title','')}".strip(),
             f"地點：{res.get('place','')}",
             f"日期：{res.get('date','')}"]
    if res.get("ok"):
        lines.append("\n✅ 監看結果：目前可售")
        secs = res.get("sections", {})
        for k, v in sorted(secs.items(), key=lambda x: (-x[1], x[0])):
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
            if len(parts) >= 2:
                t = parts[1].lower()
                if t in ("all", "off"):
                    mode = t
            rows = fs_list(chat_id, show="off" if mode=="off" else ("all" if mode=="all" else "on"))
            if not rows:
                out = "（沒有任務）"
                return [TextSendMessage(text=out)] if HAS_LINE else [out]

            def _chunk(s, n=4500):
                for i in range(0, len(s), n):
                    yield s[i:i+n]

            lines = ["你的任務："]
            for r in rows:
                rid = r.get("id", "?")
                state = "啟用" if r.get("enabled") else "停用"
                period = r.get("period", "?")
                u = r.get("url", "")
                lines.append(f"{rid}｜{state}｜{period}s\n{u}")
            big = "\n\n".join(lines)

            if HAS_LINE:
                return [TextSendMessage(text=chunk) for chunk in _chunk(big)]
            else:
                return [big]

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
                    if img and img != LOGO:
                        send_image(chat_id, img)
                    if seatmap:
                        send_image(chat_id, seatmap)
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

# 方便直接用 GET 測 /check（不經 LINE）
@app.route("/check", methods=["GET"])
def http_check_once():
    url = request.args.get("url", "").strip()
    if not url:
        return jsonify({"ok": False, "msg": "provide ?url=<UTK0201_000 url>"}), 400
    res = probe(url)
    return jsonify(res), 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8080")))