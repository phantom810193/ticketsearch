# app.py
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

# === 新增：抓代碼/數量用 ===
COUNT_CODE_RE = re.compile(r"\b(B0[0-9A-Z]{6,10})\b")
COUNT_PAIR_RE = re.compile(r"\b(B0[0-9A-Z]{6,10})\D{0,12}(\d{1,5})\s*張")
IBON_HOST_RE = re.compile(r"(^|\.)orders\.ibon\.com\.tw$", re.I)

LOGO = "https://ticketimg2.azureedge.net/logo.png"

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

# ============= 共同解析：標題/地點/日期/圖片 =============
def pick_event_image_from_000(html: str, base_url: str) -> str:
    """從 000 頁面挑一張活動圖：og:image / twitter:image / 內嵌含 azureedge | ActivityImage | static_bigmap"""
    try:
        soup = soup_parse(html)
        for sel in ['meta[property="og:image"]', 'meta[name="twitter:image"]']:
            m = soup.select_one(sel)
            if m and m.get("content"):
                return urljoin(base_url, m["content"])

        urls = []
        for img in soup.find_all("img"):
            if img.get("src"):
                urls.append(img["src"])
            if img.get("srcset"):
                urls.extend([p.split()[0] for p in img["srcset"].split(",") if p.strip()])

        urls += re.findall(r'https?://[^\s"\'<>]+\.(?:jpg|jpeg|png)', html, flags=re.I)

        for u in urls:
            lu = u.lower()
            if any(key in lu for key in ["azureedge", "activityimage", "static_bigmap", "bigmap", "image"]):
                return urljoin(base_url, u)
    except Exception as e:
        app.logger.warning(f"[image] pick failed: {e}")
    return LOGO

def pick_event_image_generic(html: str, base_url: str) -> str:
    """通用頁面的主圖：先 og:image，再找最大張圖片"""
    try:
        soup = soup_parse(html)
        for sel in ['meta[property="og:image"]', 'meta[name="twitter:image"]']:
            m = soup.select_one(sel)
            if m and m.get("content"):
                return urljoin(base_url, m["content"])
        imgs = soup.find_all("img")
        best = None
        best_area = -1
        for img in imgs:
            src = img.get("src") or ""
            if not src:
                continue
            w = int(img.get("width") or 0) or 0
            h = int(img.get("height") or 0) or 0
            area = w * h
            if area > best_area:
                best = src; best_area = area
        if best:
            return urljoin(base_url, best)
    except Exception as e:
        app.logger.warning(f"[image] generic pick failed: {e}")
    return LOGO

def extract_area_name_map_from_000(html: str) -> dict:
    """從 UTK0201_000 表格抽 {區代碼: 中文名稱}。"""
    name_map = {}
    try:
        soup = soup_parse(html)
        for a in soup.select('a[href*="PERFORMANCE_PRICE_AREA_ID="]'):
            href = a.get("href", "")
            m = re.search(r'PERFORMANCE_PRICE_AREA_ID=([A-Za-z0-9]+)', href)
            if not m:
                continue
            code = m.group(1)
            tr = a.find_parent("tr")
            cand_text = ""
            if tr:
                tds = [td.get_text(" ", strip=True) for td in tr.find_all("td")]
                pick = None
                for t in tds:
                    if re.search(r"[A-Z0-9一二三四五六七八九十]+.*[區区]", t):
                        pick = t
                        break
                cand_text = pick or (tds[0] if tds else "")
            else:
                cand_text = a.get_text(strip=True) or a.get("title", "")
            cand_text = re.sub(r"\s+", "", cand_text)
            if cand_text:
                name_map[code] = cand_text
    except Exception as e:
        app.logger.warning(f"[area-map] extract failed: {e}")
    return name_map

def _parse_livemap_text(txt: str):
    """解析 azureedge live.map。"""
    sections = {}
    total = 0
    for tag in _RE_AREA_TAG.findall(txt):
        # 區代碼/名稱
        name = "未命名區"
        m_href = re.search(
            r"javascript:Send\([^)]*'([A-Za-z0-9]+)'\s*,\s*'([A-Za-z0-9]+)'\s*,\s*'(\d+)'",
            tag, re.I)
        if m_href:
            name = m_href.group(2)

        # 數量：title 內最後一個 <1000 的數字
        qty = None
        m_title = re.search(r'title="([^"]*)"', tag, re.I)
        title_text = m_title.group(1) if m_title else ""
        nums = [int(n) for n in re.findall(r"(\d+)", title_text)]
        for n in reversed(nums):
            if n < 1000:
                qty = n
                break

        if qty is None:
            m = re.search(r'\bdata-(?:left|remain|qty|count)=["\']?(\d+)["\']?', tag, re.I)
            if m: qty = int(m.group(1))
        if qty is None:
            m = re.search(r'\b(?:alt|aria-label)=["\'][^"\']*?(\d+)[^"\']*["\']', tag, re.I)
            if m: qty = int(m.group(1))

        if not qty or qty <= 0:
            continue

        key = re.sub(r"\s+", "", name) or "未命名區"
        sections[key] = sections.get(key, 0) + qty
        total += qty
    return sections, total

def try_fetch_livemap_by_perf(perf_id: str, sess: requests.Session):
    """猜測 live.map 的 URL，優先 1_ 前綴；命中後解析。"""
    if not perf_id:
        return {}, 0
    pids = {perf_id, perf_id.upper(), perf_id.lower()}
    prefixes = ["1", "2", "3", "0", "4", "5", "01", "02", "03", ""]
    base = "https://qwareticket-asysimg.azureedge.net/QWARE_TICKET/images/Temp"
    for pid in pids:
        for pref in prefixes:
            prefix = f"{pref}_" if pref else ""
            url = f"{base}/{pid}/{prefix}{pid}_live.map"
            try:
                app.logger.info(f"[livemap] try {url}")
                r = sess.get(url, timeout=12)
                if r.status_code == 200 and "<area" in r.text:
                    app.logger.info(f"[livemap] guessed and hit: {url}")
                    return _parse_livemap_text(r.text)
            except Exception as e:
                app.logger.warning(f"[livemap] guess fail {url}: {e}")
    return {}, 0

# === 新增：通用 counts 解析（文字 & script JSON） ===
def _parse_counts_from_text(text: str) -> dict:
    counts = {}
    # 直接抓「代碼 ... N 張」
    for m in COUNT_PAIR_RE.finditer(text):
        code, n = m.group(1), int(m.group(2))
        counts[code] = max(n, counts.get(code, 0))
    # 若沒抓到，嘗試鄰近行的「代碼 + N 張」
    if not counts:
        lines = [x.strip() for x in text.splitlines() if x.strip()]
        for i, ln in enumerate(lines):
            m = COUNT_CODE_RE.search(ln)
            if not m:
                continue
            code = m.group(1)
            ctx = " ".join(lines[max(0, i-1): i+2])
            m2 = re.search(r"(\d{1,5})\s*張", ctx)
            if m2:
                counts[code] = max(int(m2.group(1)), counts.get(code, 0))
    return counts

def _parse_counts_from_scripts(soup: BeautifulSoup) -> dict:
    counts = {}
    for sc in soup.find_all("script"):
        sc_txt = (sc.string or sc.text or "").strip()
        if not sc_txt:
            continue
        # 先用正則補抓
        c2 = _parse_counts_from_text(sc_txt)
        for k, v in c2.items():
            counts[k] = max(v, counts.get(k, 0))
        # 盡量解析 JSON 結構
        if "{" in sc_txt and "}" in sc_txt and any(k in sc_txt.lower() for k in ("remain", "qty", "quantity", "left")):
            try:
                blob = sc_txt[sc_txt.find("{"): sc_txt.rfind("}") + 1]
                data = json.loads(blob)
            except Exception:
                data = None
            if isinstance(data, dict):
                stack = [data]
                while stack:
                    node = stack.pop()
                    if isinstance(node, dict):
                        code = None; qty = None
                        for k, v in node.items():
                            k_l = str(k).lower()
                            if isinstance(v, str) and COUNT_CODE_RE.fullmatch(v):
                                code = v
                            if isinstance(v, (int, str)) and k_l in ("remain", "remainqty", "qty", "quantity", "left", "remaincount"):
                                try: qty = int(v)
                                except: pass
                        if code and isinstance(qty, int):
                            counts[code] = max(qty, counts.get(code, 0))
                        stack.extend(node.values())
                    elif isinstance(node, list):
                        stack.extend(node)
    return counts

# === 新增：Playwright 動態備援 ===
def _try_dynamic_counts(event_url: str, timeout_sec: int = 20) -> dict:
    counts = {}
    try:
        from playwright.sync_api import sync_playwright
    except Exception as e:
        app.logger.info(f"[dyn] playwright not installed: {e}")
        return counts

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage"])
        ctx = browser.new_context(user_agent=UA, locale="zh-TW", java_script_enabled=True)
        page = ctx.new_page()

        def on_response(resp):
            try:
                ct = resp.headers.get("content-type", "")
                if "application/json" not in ct:
                    return
                if "ibon.com.tw" not in resp.url:
                    return
                data = resp.json()
            except Exception:
                return
            # 深度找 code+qty
            stack = [data]
            while stack:
                node = stack.pop()
                if isinstance(node, dict):
                    code = None; qty = None
                    for k, v in node.items():
                        k_l = str(k).lower()
                        if isinstance(v, str) and COUNT_CODE_RE.fullmatch(v):
                            code = v
                        if isinstance(v, (int, str)) and k_l in ("remain", "remainqty", "qty", "quantity", "left", "remaincount"):
                            try: qty = int(v)
                            except: pass
                    if code and isinstance(qty, int):
                        counts[code] = max(qty, counts.get(code, 0))
                    stack.extend(node.values())
                elif isinstance(node, list):
                    stack.extend(node)

        page.on("response", on_response)
        page.set_extra_http_headers({"User-Agent": UA, "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.6"})
        page.goto(event_url, wait_until="domcontentloaded")
        try:
            page.wait_for_load_state("networkidle", timeout=timeout_sec * 1000)
        except Exception:
            pass

        body_text = page.evaluate("document.body.innerText")
        cc = _parse_counts_from_text(body_text)
        for k, v in cc.items():
            counts[k] = max(v, counts.get(k, 0))

        html = page.content()
        soup = BeautifulSoup(html, "html.parser")
        cc2 = _parse_counts_from_text(soup.get_text("\n", strip=True))
        for k, v in cc2.items():
            counts[k] = max(v, counts.get(k, 0))

        ctx.close()
        browser.close()
    return counts

# === 區域名稱對應（最長前綴） ===
def map_counts_to_zones(counts: dict, area_map: dict) -> tuple[list[tuple[str, str, int]], list[tuple[str, int]]]:
    matched, unmatched = [], []
    for code, n in counts.items():
        name = area_map.get(code)
        if not name:
            best = None
            for k, v in area_map.items():
                if code.startswith(k) or k.startswith(code):
                    if best is None or len(k) > len(best[0]):
                        best = (k, v)
            if best:
                name = best[1]
        if name:
            matched.append((name, code, int(n)))
        else:
            unmatched.append((code, int(n)))
    matched.sort(key=lambda x: x[2], reverse=True)
    return matched, unmatched

# ============= ibon 解析 =============
def parse_UTK0201_000(url: str, sess: requests.Session) -> dict:
    """解析 000 頁，抓標題/地點/日期/活動圖 + live.map 或頁面文字票數。"""
    out = {"ok": False, "sig": "NA", "url": url, "image": LOGO}
    r = sess.get(url, timeout=15)
    if r.status_code != 200:
        out["msg"] = f"讀取失敗（HTTP {r.status_code}）"
        return out
    html = r.text

    # 標題/地點/日期
    title = ""
    place = ""
    date_str = ""
    try:
        soup = soup_parse(html)
        # 標題
        m = soup.select_one("title")
        if m and m.text.strip():
            title = m.text.strip().replace("ibon售票系統", "").strip()
        mt = soup.select_one('meta[property="og:title"]')
        if not title and mt and mt.get("content"):
            title = mt["content"].strip()
        # 地點
        candidates = soup.find_all(string=re.compile(r"場地|地區"))
        if candidates:
            for t in candidates:
                td = getattr(t, "parent", None)
                if not td:
                    continue
                tr = td.find_parent("tr")
                if tr:
                    tds = tr.find_all("td")
                    if len(tds) >= 3:
                        place = tds[2].get_text(" ", strip=True)
                        if place:
                            break
        # 日期
        m = _RE_DATE.search(html)
        if m:
            date_str = m.group(1)
    except Exception as e:
        app.logger.warning(f"[parse000] meta fail: {e}")

    out["title"] = title or "（未取到標題）"
    out["place"] = place or "（未取到場地）"
    out["date"]  = date_str or "（未取到日期）"

    # 主圖
    out["image"] = pick_event_image_from_000(html, url)

    # 票區名稱映射
    area_name_map = extract_area_name_map_from_000(html)
    out["area_names"] = area_name_map

    # 先試 live.map
    q = parse_qs(urlparse(url).query)
    perf_id = (q.get("PERFORMANCE_ID") or [None])[0]
    sections_by_code, total = try_fetch_livemap_by_perf(perf_id, sess)

    # 若 live.map 抓不到 → 直接從頁面文字/腳本試抓
    if total <= 0:
        soup = soup_parse(html)
        counts = _parse_counts_from_text(soup.get_text("\n", strip=True))
        if not counts:
            counts = _parse_counts_from_scripts(soup)
        if counts:
            # 把代碼對應中文
            matched, unmatched = map_counts_to_zones(counts, area_name_map)
            human = {}
            for name, code, n in matched:
                human[name] = human.get(name, 0) + int(n)
            # 未對到的保留代碼
            for code, n in unmatched:
                human[code] = human.get(code, 0) + int(n)
            total = sum(human.values())
            if total > 0:
                out["sections"] = human
                out["total"] = total
                out["ok"] = True
                out["sig"] = hash_sections(human)
                lines = [f"✅ 監看結果：目前可售"]
                for k, v in sorted(human.items(), key=lambda x: (-x[1], x[0])):
                    lines.append(f"{k}: {v} 張")
                lines.append(f"合計：{total} 張")
                out["msg"] = "\n".join(lines) + f"\n{url}"
                return out

    if total > 0:
        # live.map 命中：區代碼 → 中文名稱（若無對應就用原代碼）
        human = {}
        for code, qty in sections_by_code.items():
            disp = area_name_map.get(code, code)
            human[disp] = human.get(disp, 0) + int(qty)
        out["sections"] = human
        out["total"] = total
        out["ok"] = True
        out["sig"] = hash_sections(human)
        lines = [f"✅ 監看結果：目前可售"]
        for k, v in sorted(human.items(), key=lambda x: (-x[1], x[0])):
            lines.append(f"{k}: {v} 張")
        lines.append(f"合計：{total} 張")
        out["msg"] = "\n".join(lines) + f"\n{url}"
    else:
        out["msg"] = (
            f"🎫 {out['title']}\n"
            f"地點：{out['place']}\n"
            f"日期：{out['date']}\n\n"
            "暫時讀不到剩餘數（可能為動態載入）。\n"
            f"{url}"
        )
    return out

def parse_ibon_generic(url: str, sess: requests.Session) -> dict:
    """
    通用 ibon 頁處理：
    - 若是 000 頁 → 走 parse_UTK0201_000
    - 其它頁：先靜態抓（頁面文字 + script），抓不到再用 Playwright 動態備援
    """
    p = urlparse(url)
    if p.path.upper().endswith("/UTK0201_000.ASPX"):
        return parse_UTK0201_000(url, sess)

    out = {"ok": False, "sig": "NA", "url": url, "image": LOGO}
    r = sess.get(url, timeout=15)
    if r.status_code != 200:
        out["msg"] = f"讀取失敗（HTTP {r.status_code}）"
        return out
    html = r.text
    soup = soup_parse(html)

    # 標題/地點/日期/圖片（通用）
    title = (soup.title.text.strip() if soup.title and soup.title.text else "")
    if not title:
        mt = soup.select_one('meta[property="og:title"]')
        if mt and mt.get("content"): title = mt["content"].strip()
    out["title"] = title or "（未取到標題）"

    place = ""
    for t in soup.find_all(string=re.compile(r"場地|地區|地點")):
        td = getattr(t, "parent", None)
        if not td: continue
        tr = td.find_parent("tr")
        if tr:
            tds = tr.find_all("td")
            if len(tds) >= 2:
                place = tds[-1].get_text(" ", strip=True)
                if place: break
    out["place"] = place or "（未取到場地）"

    m = _RE_DATE.search(html)
    out["date"] = m.group(1) if m else "（未取到日期）"

    out["image"] = pick_event_image_generic(html, url)

    # 先靜態抓 counts
    counts = _parse_counts_from_text(soup.get_text("\n", strip=True))
    if not counts:
        counts = _parse_counts_from_scripts(soup)

    # 票區名稱字典（能抓到就對應）
    area_name_map = extract_area_name_map_from_000(html)  # 有些頁也沿用相同樣式
    out["area_names"] = area_name_map

    if not counts:
        # 退到 Playwright 動態
        counts = _try_dynamic_counts(url)

    if counts:
        matched, unmatched = map_counts_to_zones(counts, area_name_map)
        human = {}
        for name, code, n in matched:
            human[name] = human.get(name, 0) + int(n)
        for code, n in unmatched:
            human[code] = human.get(code, 0) + int(n)
        total = sum(human.values())
        out["sections"] = human
        out["total"] = total
        out["ok"] = total > 0
        if out["ok"]:
            out["sig"] = hash_sections(human)
            lines = [f"✅ 監看結果：目前可售"]
            for k, v in sorted(human.items(), key=lambda x: (-x[1], x[0])):
                lines.append(f"{k}: {v} 張")
            lines.append(f"合計：{total} 張")
            out["msg"] = "\n".join(lines) + f"\n{url}"
            return out

    # 都抓不到
    out["msg"] = (
        f"🎫 {out['title']}\n"
        f"地點：{out['place']}\n"
        f"日期：{out['date']}\n\n"
        "暫時讀不到剩餘數（可能為動態載入）。\n"
        f"{url}"
    )
    return out

def probe(url: str) -> dict:
    """入口：改為支援所有 ibon 頁。非 ibon 仍回基本訊息。"""
    s = sess_default()
    p = urlparse(url)
    if IBON_HOST_RE.search(p.netloc):
        return parse_ibon_generic(url, s)

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
    if not FS_OK: return []
    q = fs_client.collection(COL).where("chat_id", "==", chat_id)
    if show == "on":
        q = q.where("enabled", "==", True)
    elif show == "off":
        q = q.where("enabled", "==", False)
    return [d.to_dict() for d in q.order_by("updated_at", direction=firestore.Query.DESCENDING).stream()]

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
            else:
                lines = ["你的任務："]
                for r in rows:
                    state = "啟用" if r.get("enabled") else "停用"
                    lines.append(f"{r['id']}｜{state}｜{r.get('period')}s\n{r.get('url')}")
                out = "\n\n".join(lines)
            return [TextSendMessage(text=out)] if HAS_LINE else [out]

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
            msgs = []
            if HAS_LINE and res.get("image", LOGO) and res["image"] != LOGO:
                msgs.append(ImageSendMessage(original_content_url=res["image"], preview_image_url=res["image"]))
            text_out = fmt_result_text(res)
            msgs.append(TextSendMessage(text=text_out) if HAS_LINE else text_out)
            return msgs

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
        # 沒有 handler 也回 200，避免 LINE 重試連發
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

# 只有在 handler 存在時才掛 decorator，避免載入期報錯
if HAS_LINE and handler:
    @handler.add(MessageEvent, message=TextMessage)
    def on_message(ev):
        text = ev.message.text.strip()
        chat = source_id(ev)
        msgs = handle_command(text, chat)
        # SDK 需要 list[SendMessage]
        if isinstance(msgs, list) and msgs and not isinstance(msgs[0], str):
            line_bot_api.reply_message(ev.reply_token, msgs)
        else:
            # 理論上不會進來；保底
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
                    chat_id = r.get("chat_id")
                    if img and img != LOGO:
                        send_image(chat_id, img)
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
        return jsonify({"ok": False, "msg": "provide ?url=<ibon url>"}), 400
    res = probe(url)
    return jsonify(res), 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8080")))