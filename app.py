# app.py — ibon 票券監看（Cloud Run + LINE Bot + Firestore）
# 解析順序：live.map → 由 PERFORMANCE_ID 推導 live.map → 001 逐區 → 靜態表格/就近字串
# 指令：
# /check <URL>、/watch <URL> [秒]、/unwatch <ID>、/list（/list all /list off）
# /checkid <ID>、/probe <URL>、/cron/tick、/diag?url=...、/healthz

import base64
import hashlib
import hmac
import json
import logging
import os
import re
import secrets
import time
from typing import Dict, Tuple, Optional, List, Set
from urllib.parse import urlparse, urljoin, parse_qsl, urlencode, quote

import requests
from bs4 import BeautifulSoup
from flask import Flask, jsonify, request
from google.cloud import firestore

app = Flask(__name__)
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))

# -------------------- 環境 --------------------
db = firestore.Client()
LINE_TOKEN = os.getenv("LINE_CHANNEL_ACCESS_TOKEN") or os.getenv("LINE_TOKEN")
LINE_SECRET = os.getenv("LINE_CHANNEL_SECRET") or os.getenv("LINE_SECRET")
DEFAULT_PERIOD_SEC = int(os.getenv("DEFAULT_PERIOD_SEC", "60"))
ALWAYS_NOTIFY = os.getenv("ALWAYS_NOTIFY", "0") == "1"

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0 Safari/537.36")

HELP_TEXT = (
    "我是票券監看機器人 👋\n"
    "指令：\n"
    "/start 或 /help － 顯示這個說明\n"
    "/watch <URL> [秒] － 開始監看（同網址不重複；帶新秒數會更新；最小 15 秒）\n"
    "/unwatch <任務ID> － 停用任務\n"
    "/list － 顯示啟用中任務（/list all 看全部；/list off 只看停用）\n"
    "/check <URL> － 立即查詢剩餘票數\n"
    "/checkid <任務ID> － 立即查該任務的 URL\n"
    "/probe <URL> － 掃描頁面裡疑似 XHR/Fetch API\n"
    "也可輸入「查詢」或 /check 不帶參數，會查你最近一筆啟用中的任務"
)

# -------------------- 正則 --------------------
_RE_QTY = re.compile(r"(空位|剩餘|尚餘|尚有|可售|餘票|名額|席位|剩餘張數|剩餘票數|餘數|可購|剩下)[^\d]{0,6}(\d+)", re.I)
_RE_SOLDOUT = re.compile(r"(售罄|完售|無票|已售完|暫無|暫時無|售完|已售空|無可售|無剩餘)", re.I)
_RE_AREANAME_NEAR = re.compile(
    r"(搖滾.?[A-Z]?\s*區|搖滾區|身障席|身心障礙席|無障礙席|內野|外野|[A-Z]\s*區|[A-Z]\d+\s*區|看台\d+|[一二三四五六七八九十]\s*樓|[東西南北上下]\s*(?=區|層)|\S{1,8}區)",
    re.I
)
_RE_ACTIVITY_IMG = re.compile(r"https?://[^\"'<>]*azureedge\.net/[^\"'<>]*ActivityImage[^\"'<>]*\.(?:jpg|jpeg|png)", re.I)
_RE_ACTIVITY_INFO = re.compile(r"(https?://ticket\.ibon\.com\.tw)?/?ActivityInfo/Details/\d+", re.I)
_RE_SUSPECT_API = re.compile(r"https?://[^\"'<>]+/(?:api|API|Application)/[^\"'<>]+", re.I)

_DATE_PAT = re.compile(
    r"(\d{4}[./-年]\s*\d{1,2}[./-月]\s*\d{1,2}(?:\s*[（(]?[一二三四五六日天週周MonTueWedThuFriSatSun星期]{1,3}[)）]?)?(?:\s*\d{1,2}:\d{2})?"
    r"|"
    r"\d{1,2}\s*月\s*\d{1,2}\s*日(?:\s*\d{1,2}:\d{2})?"
    r"|"
    r"\d{1,2}/\d{1,2}(?:\s*\d{1,2}:\d{2})?"
    r")"
)

# live.map
_RE_LIVEMAP_URL = re.compile(
    r"https?://[^\"'>]+/QWARE_TICKET/images/Temp/[A-Za-z0-9]+/\d+_[A-Za-z0-9]+_live\.map[^\"'>]*",
    re.I
)
_RE_AREA_TAG = re.compile(r"<area\b[^>]+>", re.I)
_RE_AREA_QTY_IN_TITLE = re.compile(r'title="([^"]*?)"', re.I)

# -------------------- LINE --------------------
def _line_reply(reply_token: str, text: str) -> None:
    if not LINE_TOKEN or not reply_token:
        return
    try:
        requests.post(
            "https://api.line.me/v2/bot/message/reply",
            headers={"Authorization": f"Bearer {LINE_TOKEN}", "Content-Type": "application/json"},
            json={"replyToken": reply_token, "messages": [{"type": "text", "text": text[:4000]}]},
            timeout=10,
        ).raise_for_status()
    except Exception as e:
        app.logger.exception(f"LINE reply failed: {e}")

def _line_send_rich(endpoint: str, payload: dict) -> None:
    if not LINE_TOKEN:
        return
    try:
        requests.post(
            f"https://api.line.me/v2/bot/message/{endpoint}",
            headers={"Authorization": f"Bearer {LINE_TOKEN}", "Content-Type": "application/json"},
            json=payload,
            timeout=10,
        ).raise_for_status()
    except Exception as e:
        app.logger.exception(f"LINE send {endpoint} failed: {e}")

def _line_reply_rich(reply_token: str, text: str, image_url: Optional[str] = None) -> None:
    msgs = []
    if image_url:
        msgs.append({"type": "image", "originalContentUrl": image_url, "previewImageUrl": image_url})
    msgs.append({"type": "text", "text": text[:4000]})
    _line_send_rich("reply", {"replyToken": reply_token, "messages": msgs})

def _line_push_rich(to: str, text: str, image_url: Optional[str] = None) -> None:
    msgs = []
    if image_url:
        msgs.append({"type": "image", "originalContentUrl": image_url, "previewImageUrl": image_url})
    msgs.append({"type": "text", "text": text[:4000]})
    _line_send_rich("push", {"to": to, "messages": msgs})

def _verify_line_signature(raw_body: bytes) -> bool:
    if not LINE_SECRET:
        return True
    sig = request.headers.get("X-Line-Signature", "")
    calc = base64.b64encode(hmac.new(LINE_SECRET.encode(), raw_body, hashlib.sha256).digest()).decode()
    return hmac.compare_digest(sig, calc)

# -------------------- 小工具 --------------------
def _req_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": UA, "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8", "Cache-Control": "no-cache"})
    return s

def _strip_scripts(soup: BeautifulSoup) -> None:
    for tag in soup.find_all(["script", "style", "noscript"]):
        tag.decompose()

def _clean_text(s: str) -> str:
    s = re.sub(r"\s+", " ", s or "").strip()
    if re.search(r"\b(var|function|\$\(window|if\s*\(|for\s*\(|while\s*\(|\{|\}|;)\b", s):
        return ""
    return s[:120]

def _only_date_like(s: str) -> str:
    m = _DATE_PAT.search(s or "")
    return m.group(0) if m else ""

def _guess_area_from_text(txt: str) -> str:
    m = _RE_AREANAME_NEAR.search(txt or "")
    if not m:
        return "未命名區"
    return re.sub(r"\s+", "", m.group(0))

# -------------------- URL 整理 --------------------
def resolve_ibon_orders_url(any_url: str) -> Optional[str]:
    """由活動頁或外部頁導回 UTK0201 票頁"""
    u = urlparse(any_url)
    if "orders.ibon.com.tw" in u.netloc and "UTK0201" in u.path.upper():
        return any_url
    if "ticket.ibon.com.tw" in u.netloc:
        s = _req_session()
        r = s.get(any_url, timeout=15); r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        _strip_scripts(soup)
        a = soup.select_one('a[href*="orders.ibon.com.tw"][href*="UTK02"][href*="UTK0201"]')
        if a and a.get("href"):
            return urljoin(any_url, a["href"])
        for tag in soup.find_all(["a", "button"]):
            href = (tag.get("href") or tag.get("data-url") or "").strip()
            if "orders.ibon.com.tw" in href and "UTK0201" in href.upper():
                return urljoin(any_url, href)
    return None

def canonicalize_ibon_url(any_url: str) -> str:
    url = resolve_ibon_orders_url(any_url) or any_url
    p = urlparse(url)
    host = p.netloc.lower()
    path = re.sub(r"/+", "/", p.path)
    pairs = parse_qsl(p.query, keep_blank_values=True)

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
        for k, v in pairs:
            if k.upper() in skip: continue
            kept.append((k, v))
        kept.sort()
        q = urlencode(kept, doseq=True)

    canon = f"https://{host}{path}"
    if q: canon += "?" + q
    return canon

# -------------------- 000/001 解析 --------------------
def _extract_activity_meta(soup: BeautifulSoup) -> Dict[str, str]:
    _strip_scripts(soup)
    def find_label(labels):
        pat = re.compile("|".join(map(re.escape, labels)))
        for node in soup.find_all(string=pat):
            parent = node.parent
            if not parent or parent.name in ("script", "style", "noscript"): continue
            if parent.name in ("td", "th") and parent.parent:
                cells = [c.get_text(" ", strip=True) for c in parent.parent.find_all(["td", "th"])]
                for i, val in enumerate(cells):
                    if re.search(pat, val) and i + 1 < len(cells):
                        out = _clean_text(cells[i + 1]); 
                        if out: return out
            txt = parent.get_text(" ", strip=True)
            if re.search(pat, txt):
                out = _clean_text(pat.sub("", txt).replace("：", " ").strip())
                if out: return out
            sib = parent.find_next_sibling()
            if sib:
                out = _clean_text(sib.get_text(" ", strip=True))
                if out: return out
        return ""
    title = _clean_text(find_label(["活動名稱"]) or (soup.select_one('meta[property="og:title"]') and soup.select_one('meta[property="og:title"]').get("content", "")))
    dt = _only_date_like(find_label(["活動時間", "演出時間"])) or _only_date_like(soup.get_text(" ", strip=True))
    venue = _clean_text(find_label(["活動地點", "地點", "場館", "地點/場館"]))
    return {"title": title, "datetime": dt, "venue": venue}

def parse_ibon_orders_static(html: str) -> Dict:
    soup = BeautifulSoup(html, "html.parser")
    _strip_scripts(soup)
    sections: Dict[str, int] = {}
    total = 0
    soldout_hint = False

    # 表格模式
    for table in soup.find_all("table"):
        header_tr = None
        for tr in table.find_all("tr", recursive=True):
            if tr.find("th"):
                heads = [c.get_text(" ", strip=True) for c in tr.find_all(["th", "td"])]
                if any(("空位" in h or "剩餘" in h or "可售" in h) for h in heads):
                    header_tr = tr; break
        if not header_tr: continue
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
                area = re.sub(r"\s+", "", tds[idx_area].get_text(" ", strip=True)) or "未命名區"
                qty_text = tds[idx_qty].get_text(" ", strip=True)
                if _RE_SOLDOUT.search(qty_text): soldout_hint = True
                m = re.search(r"(\d+)", qty_text)
                if m:
                    qty = int(m.group(1))
                    if qty > 0:
                        sections[area] = sections.get(area, 0) + qty
                        total += qty
            tr = tr.find_next_sibling("tr")
        if total > 0: break

    # 區塊就近模式
    if total == 0:
        for node in soup.select("li, div, section, article, tr, p"):
            txt = node.get_text(" ", strip=True)
            if not txt: continue
            if _RE_SOLDOUT.search(txt): soldout_hint = True
            for m in _RE_QTY.finditer(txt):
                qty = int(m.group(2))
                if qty <= 0: continue
                area = _guess_area_from_text(txt[max(0, m.start()-40):m.end()+40])
                sections[area] = sections.get(area, 0) + qty
                total += qty

    return {"sections": sections, "total": total, "soldout": (total == 0 and soldout_hint), "soup": soup}

# -------------------- live.map 解析 & 後備 --------------------
def _parse_livemap_text(txt: str) -> Tuple[Dict[str, int], int]:
    sections: Dict[str, int] = {}
    total = 0
    for tag in _RE_AREA_TAG.findall(txt):
        title = ""
        mt = _RE_AREA_QTY_IN_TITLE.search(tag)
        if mt: title = mt.group(1)
        q = None
        mq = re.search(r"(空位|剩餘|可售)[^\d]{0,6}(\d+)", title)
        if mq:
            q = int(mq.group(2))
        else:
            mr = re.search(r'\brel=["\'](\d+)["\']', tag, re.I)
            if mr:
                q = int(mr.group(1))
        if not q or q <= 0:
            continue
        name = _guess_area_from_text(title) or "未命名區"
        key = re.sub(r"\s+", "", name)
        sections[key] = sections.get(key, 0) + q
        total += q
    return sections, total

def parse_livemap_counts_from_html_or_fetch(base_page_html: str, base_url: str, sess: requests.Session) -> Tuple[Dict[str, int], int]:
    m = _RE_LIVEMAP_URL.search(base_page_html)
    if not m:
        app.logger.info(f"[livemap] not found in {base_url}")
        return {}, 0
    live_url = m.group(0)
    try:
        r = sess.get(live_url, timeout=12)
        r.raise_for_status()
    except Exception as e:
        app.logger.warning(f"[livemap] fetch fail: {live_url} {e}")
        return {}, 0
    return _parse_livemap_text(r.text)

def parse_livemap_direct(live_url: str, sess: requests.Session) -> Tuple[Dict[str, int], int]:
    r = sess.get(live_url, timeout=12); r.raise_for_status()
    return _parse_livemap_text(r.text)

def _extract_performance_id_from_any(html: str, url: str) -> str:
    m = re.search(r"[?&]PERFORMANCE_ID=([A-Za-z0-9]+)", url, re.I)
    if m: return m.group(1)
    m = re.search(r"PERFORMANCE_ID['\"=:\s]+([A-Za-z0-9]+)", html, re.I)
    if m: return m.group(1)
    return ""

def try_fetch_livemap_by_perf(perf_id: str, sess: requests.Session) -> Tuple[Dict[str, int], int]:
    """000/001 頁抓不到 live.map 時，依 PERF_ID 主動猜連結（1_/2_/3_）"""
    if not perf_id:
        return {}, 0
    base = f"https://qwareticket-asysimg.azureedge.net/QWARE_TICKET/images/Temp/{perf_id}"
    candidates = [
        f"{base}/1_{perf_id}_live.map",
        f"{base}/2_{perf_id}_live.map",
        f"{base}/3_{perf_id}_live.map",
    ]
    for u in candidates:
        try:
            r = sess.get(u, timeout=12)
            if r.status_code == 200 and "<area" in r.text:
                app.logger.info(f"[livemap] guessed and hit: {u}")
                return _parse_livemap_text(r.text)
        except Exception as e:
            app.logger.warning(f"[livemap] guess fail {u}: {e}")
    return {}, 0

# -------------------- 主視覺圖 --------------------
def _find_image_in_soup(base_url: str, soup: BeautifulSoup) -> str:
    for prop in ("og:image", "og:image:url", "twitter:image"):
        og = soup.select_one(f'meta[property="{prop}"], meta[name="{prop}"]')
        if og and og.get("content"):
            return urljoin(base_url, og["content"])
    for img in soup.find_all("img"):
        cand = img.get("src") or img.get("data-src") or ""
        if not cand and img.get("srcset"):
            cand = img["srcset"].split(",")[0].split()[0]
        if cand and ("ActivityImage" in cand or "azureedge" in cand):
            return urljoin(base_url, cand)
    for u in re.findall(r'url\(([^)]+)\)', soup.decode()):
        u = u.strip('\'"')
        if "ActivityImage" in u or "azureedge" in u:
            return urljoin(base_url, u)
    return ""

def _resolve_activity_image(orders_url: str, html_000: str, soup_000: BeautifulSoup, sess: requests.Session) -> str:
    img = _find_image_in_soup(orders_url, soup_000)
    if img: return img
    m = _RE_ACTIVITY_IMG.search(html_000)
    if m: return m.group(0)
    m2 = _RE_ACTIVITY_INFO.search(html_000)
    if m2:
        details_url = m2.group(0)
        if not details_url.startswith("http"):
            details_url = urljoin(orders_url, details_url)
        try:
            r2 = sess.get(details_url, timeout=15); r2.raise_for_status()
            soup2 = BeautifulSoup(r2.text, "html.parser")
            _strip_scripts(soup2)
            img2 = _find_image_in_soup(details_url, soup2)
            if img2: return img2
            m3 = _RE_ACTIVITY_IMG.search(r2.text)
            if m3: return m3.group(0)
        except Exception as e:
            app.logger.warning(f"[image] details fetch fail: {details_url} {e}")
    return "https://ticketimg2.azureedge.net/logo.png"

# -------------------- 深入 001 票區 --------------------
def _extract_ids_from_url(url: str) -> Dict[str, str]:
    q = dict((k.upper(), v) for k, v in parse_qsl(urlparse(url).query, keep_blank_values=True))
    return {"PERFORMANCE_ID": q.get("PERFORMANCE_ID",""), "PRODUCT_ID": q.get("PRODUCT_ID",""), "STRITEM": q.get("STRITEM","")}

def _collect_area_ids_from_html(html: str) -> Set[str]:
    ids: Set[str] = set()
    for m in re.finditer(r"PERFORMANCE_PRICE_AREA_ID\s*=\s*([A-Za-z0-9]+)", html, re.I):
        ids.add(m.group(1))
    for m in re.finditer(r"(UTK0201_001\.aspx[^\"'>)]+)", html):
        mm = re.search(r"PERFORMANCE_PRICE_AREA_ID=([A-Za-z0-9]+)", m.group(1), re.I)
        if mm: ids.add(mm.group(1))
    for m in re.finditer(r"data-?areaid\s*=\s*['\"]?([A-Za-z0-9]+)['\"]?", html, re.I):
        ids.add(m.group(1))
    return ids

def _build_001_base_url(base: str, perf_id: str, prod_id: str, stritem: str = "") -> str:
    p = urlparse(base)
    q = f"PERFORMANCE_ID={quote(perf_id)}&PRODUCT_ID={quote(prod_id)}"
    if stritem: q += f"&strItem={quote(stritem)}"
    return f"{p.scheme}://{p.netloc}/application/UTK02/UTK0201_001.aspx?{q}"

def _build_001_url(base: str, perf_id: str, prod_id: str, area_id: str, stritem: str = "") -> str:
    p = urlparse(base)
    q = f"PERFORMANCE_ID={quote(perf_id)}&PRODUCT_ID={quote(prod_id)}&PERFORMANCE_PRICE_AREA_ID={quote(area_id)}"
    if stritem: q += f"&strItem={quote(stritem)}"
    return f"{p.scheme}://{p.netloc}/application/UTK02/UTK0201_001.aspx?{q}"

def _extract_area_name_001(soup: BeautifulSoup, fallback: str) -> str:
    _strip_scripts(soup)
    name = _guess_area_from_text(soup.get_text(" ", strip=True))
    if name and name != "未命名區": return name
    og = soup.select_one('meta[property="og:title"]')
    if og and og.get("content"): return re.sub(r"\s+", "", og["content"])[:20]
    return fallback or "票區"

def deep_parse_areas(orders_url: str, soup_000: BeautifulSoup, html_000: str, limit: int = 20) -> Tuple[Dict[str, int], int, bool]:
    ids = _collect_area_ids_from_html(html_000)
    info = _extract_ids_from_url(orders_url)
    perf, prod, stritem = info["PERFORMANCE_ID"], info["PRODUCT_ID"], info["STRITEM"]

    s = _req_session()
    s.headers.update({"Referer": orders_url})

    if not ids:
        url_001_base = _build_001_base_url(orders_url, perf, prod, stritem)
        try:
            r0 = s.get(url_001_base, timeout=15); r0.raise_for_status()
            parsed0 = parse_ibon_orders_static(r0.text)
            if parsed0["total"] > 0:
                return parsed0["sections"], parsed0["total"], parsed0["soldout"]
            ids = _collect_area_ids_from_html(r0.text)
        except Exception as e:
            app.logger.warning(f"[deep] fetch 001 base fail: {e}")

    if not ids:
        return {}, 0, False

    ids = set(list(ids)[:limit])
    sections: Dict[str, int] = {}
    total = 0
    soldout_hint = False

    for area_id in ids:
        url_001 = _build_001_url(orders_url, perf, prod, area_id, stritem)
        try:
            r = s.get(url_001, timeout=15); r.raise_for_status()
        except Exception as e:
            app.logger.warning(f"[deep] fetch fail: {url_001} {e}")
            continue

        soup = BeautifulSoup(r.text, "html.parser")
        _strip_scripts(soup)
        text = soup.get_text(" ", strip=True)
        html = r.text

        if _RE_SOLDOUT.search(text) or _RE_SOLDOUT.search(html):
            soldout_hint = True

        area = _extract_area_name_001(soup, fallback=f"區-{area_id[-4:]}")

        qsum = 0
        for m in _RE_QTY.finditer(text):
            try: qsum += int(m.group(2))
            except: pass
        if qsum == 0:
            for m in _RE_QTY.finditer(html):
                try: qsum += int(m.group(2))
                except: pass
        if qsum == 0:
            for m in re.finditer(r'"(?:AvailableQty|Qty|Remain|Left|剩餘|可售)"\s*:\s*(\d+)', html, re.I):
                qsum += int(m.group(1))

        if qsum > 0:
            key = re.sub(r"\s+", "", area) or f"區-{area_id[-4:]}"
            sections[key] = sections.get(key, 0) + qsum
            total += qsum

    return sections, total, soldout_hint

# -------------------- 主流程：check_ibon --------------------
def check_ibon(any_url: str) -> Tuple[bool, str, str, Dict[str, str]]:
    # 若直接丟 live.map 連結
    if _RE_LIVEMAP_URL.search(any_url):
        s = _req_session()
        sections, total = parse_livemap_direct(any_url, s)
        if total > 0:
            parts = [f"{k}: {v} 張" for k, v in sorted(sections.items(), key=lambda kv: (-kv[1], kv[0]))]
            msg = "✅ 監看結果：目前可售\n" + "\n".join(parts) + f"\n合計：{total} 張\n{any_url}"
            sig = hashlib.md5(("|" + "|".join(parts)).encode()).hexdigest()
            return True, msg, sig, {"image_url": ""}
        else:
            return False, f"live.map 解析不到數字（可能全售完或格式變更）\n{any_url}", "NA", {"image_url": ""}

    orders_url = resolve_ibon_orders_url(any_url)
    if not orders_url:
        return False, "找不到 ibon 下單頁（UTK0201）。可能尚未開賣或按鈕未顯示。", "NA", {}

    s = _req_session()
    r = s.get(orders_url, timeout=15); r.raise_for_status()

    parsed = parse_ibon_orders_static(r.text)
    meta = _extract_activity_meta(parsed["soup"])
    title = meta.get("title") or "活動資訊"
    venue = meta.get("venue")
    dt = meta.get("datetime")
    img = _resolve_activity_image(orders_url, r.text, parsed["soup"], s)

    # 先試：從 000/001 HTML 直接找 live.map
    secL, totL = parse_livemap_counts_from_html_or_fetch(r.text, orders_url, s)
    if totL == 0:
        # 還是沒有 → 由 PERFORMANCE_ID 主動猜 live.map
        perf_id = _extract_performance_id_from_any(r.text, orders_url)
        secG, totG = try_fetch_livemap_by_perf(perf_id, s)
        if totG > 0:
            secL, totL = secG, totG

    if totL > 0:
        parsed["sections"], parsed["total"], parsed["soldout"] = secL, totL, False

    # 若仍無 → 走 001 逐區
    if parsed["total"] == 0:
        app.logger.info("[deep] entering 001 fallback")
        sec2, tot2, sold2 = deep_parse_areas(orders_url, parsed["soup"], r.text, limit=20)
        if tot2 > 0:
            parsed["sections"], parsed["total"], parsed["soldout"] = sec2, tot2, False
        else:
            parsed["soldout"] = parsed.get("soldout", False) or sold2

    prefix_lines = [f"🎫 {title}"]
    if venue: prefix_lines.append(f"地點：{venue}")
    if dt:    prefix_lines.append(f"日期：{dt}")
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

# -------------------- /probe：疑似 API --------------------
def probe_candidates(any_url: str) -> List[str]:
    url = resolve_ibon_orders_url(any_url) or any_url
    s = _req_session()
    r = s.get(url, timeout=15); r.raise_for_status()
    html = r.text
    urls = set(u.group(0) for u in _RE_SUSPECT_API.finditer(html))
    ids = dict((k.upper(), v) for k, v in parse_qsl(urlparse(url).query, keep_blank_values=True))
    if "UTK0201_000" in url or "UTK0201_001" in url:
        base_001 = _build_001_base_url(url, ids.get("PERFORMANCE_ID",""), ids.get("PRODUCT_ID",""), ids.get("STRITEM",""))
        try:
            r2 = s.get(base_001, timeout=15); r2.raise_for_status()
            urls |= set(u.group(0) for u in _RE_SUSPECT_API.finditer(r2.text))
        except: pass
    m = _RE_LIVEMAP_URL.search(html)
    if m: urls.add(m.group(0))
    return sorted(list(urls))[:10]

# -------------------- Webhook --------------------
def _get_target_id(src: dict) -> str:
    return src.get("userId") or src.get("groupId") or src.get("roomId") or ""

@app.post("/webhook")
def webhook():
    raw = request.get_data()
    if not _verify_line_signature(raw):
        return "bad signature", 400

    body = request.get_json(silent=True) or {}
    events = body.get("events", [])

    for ev in events:
        etype = ev.get("type")
        if etype in ("follow", "join"):
            _line_reply(ev.get("replyToken"), HELP_TEXT); continue
        if etype != "message": 
            continue

        msg = ev.get("message", {})
        if msg.get("type") != "text":
            continue

        text = (msg.get("text") or "").strip()
        low = text.lower()
        reply_token = ev.get("replyToken")
        src = ev.get("source", {})
        target_id = _get_target_id(src)
        target_type = "user" if src.get("userId") else ("group" if src.get("groupId") else "room")

        if low in ("/start", "/help", "start", "help", "？"):
            _line_reply(reply_token, HELP_TEXT); continue

        if low.startswith("/probe"):
            parts = text.split()
            if len(parts) < 2:
                _line_reply(reply_token, "用法：/probe <票券網址>"); continue
            cand = probe_candidates(parts[1])
            out = "疑似 API（前幾筆）：\n" + ("\n".join(cand) if cand else "（未找到）")
            _line_reply(reply_token, out[:4000]); continue

        if low.startswith("/checkid"):
            parts = text.split()
            if len(parts) < 2:
                _line_reply(reply_token, "用法：/checkid <任務ID>"); continue
            tid = parts[1]
            doc = db.collection("watches").document(tid).get()
            if not doc.exists:
                _line_reply(reply_token, f"找不到任務 {tid}"); continue
            ok, msg_out, _sig, meta = check_ibon(doc.to_dict().get("url", ""))
            _line_reply_rich(reply_token, msg_out, (meta or {}).get("image_url")); continue

        if low.startswith("/check") or text == "查詢":
            parts = text.split()
            url = parts[1] if len(parts) >= 2 and parts[1].startswith("http") else None
            if not url:
                q = (db.collection("watches")
                     .where("targetId", "==", target_id)
                     .where("active", "==", True)
                     .order_by("createdAt", direction=firestore.Query.DESCENDING)
                     .limit(1))
                docs = list(q.stream())
                if docs: url = docs[0].to_dict().get("url")
            if not url:
                _line_reply(reply_token, "用法：/check <票券網址>\n或先 /watch 後輸入「查詢」"); continue
            ok, msg_out, _sig, meta = check_ibon(url)
            _line_reply_rich(reply_token, msg_out, (meta or {}).get("image_url")); continue

        if low.startswith("/watch"):
            parts = text.split()
            if len(parts) < 2:
                _line_reply(reply_token, "用法：/watch <票券網址> [秒]"); continue
            raw_url = parts[1]
            period = DEFAULT_PERIOD_SEC
            if len(parts) >= 3:
                try: period = max(15, min(3600, int(parts[2])))
                except: pass

            url_canon = canonicalize_ibon_url(raw_url)
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

            now = int(time.time())
            if existing:
                data = existing.to_dict()
                if data.get("active", True):
                    if int(data.get("periodSec", DEFAULT_PERIOD_SEC)) != period:
                        existing.reference.update({"periodSec": period, "nextCheckAt": now})
                        _line_reply(reply_token, f"此網址已在監看 ✅\n任務ID：{existing.id}\n已更新為每 {period} 秒檢查\nURL：{data.get('url')}")
                    else:
                        _line_reply(reply_token, f"此網址已在監看 ✅\n任務ID：{existing.id}\n每 {data.get('periodSec', DEFAULT_PERIOD_SEC)} 秒檢查\nURL：{data.get('url')}")
                else:
                    existing.reference.update({"active": True, "periodSec": period, "nextCheckAt": now})
                    _line_reply(reply_token, f"已重新啟用 ✅\n任務ID：{existing.id}\n每 {period} 秒檢查\nURL：{data.get('url')}")
                continue

            task_id = secrets.token_urlsafe(4)
            db.collection("watches").document(task_id).set({
                "url": raw_url,
                "urlCanon": url_canon,
                "targetType": target_type,
                "targetId": target_id,
                "periodSec": period,
                "nextCheckAt": now,
                "lastSig": None,
                "active": True,
                "createdAt": now,
            })
            _line_reply(reply_token, f"已開始監看 ✅\n任務ID：{task_id}\n每 {period} 秒檢查一次\nURL：{raw_url}")
            continue

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
            mode = "on"
            if len(text.split()) >= 2:
                opt = text.split()[1].lower()
                if opt in ("all", "-a", "--all"): mode = "all"
                elif opt in ("off", "--off"):      mode = "off"

            q = db.collection("watches").where("targetId", "==", target_id)
            if mode == "on":  q = q.where("active", "==", True)
            if mode == "off": q = q.where("active", "==", False)
            q = q.order_by("createdAt", direction=firestore.Query.DESCENDING).limit(20)
            docs = list(q.stream())

            if not docs:
                _line_reply(reply_token, "目前沒有符合條件的任務" + ("" if mode=="on" else f"（{mode}）"))
            else:
                lines = []
                for d in docs:
                    x = d.to_dict()
                    flag = "啟用" if x.get("active") else "停用"
                    if mode == "on" and not x.get("active"): continue
                    lines.append(f"{d.id}｜{flag}｜{x.get('periodSec', 60)}s\n{x.get('url')}")
                prefix = "你的任務：" if mode=="on" else ("你的任務（全部）：" if mode=="all" else "你的任務（停用）：")
                _line_reply(reply_token, prefix + "\n" + "\n\n".join(lines))
            continue

        _line_reply(reply_token, HELP_TEXT)

    return "OK"

# -------------------- 排程 --------------------
def do_tick():
    now = int(time.time())
    q = (db.collection("watches")
         .where("active", "==", True)
         .where("nextCheckAt", "<=", now)
         .limit(25))
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
            if ok:
                should_push = (ALWAYS_NOTIFY or sig != task.get("lastSig"))
                if should_push:
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
    return do_tick()

# -------------------- 健康檢查 & 自測 --------------------
@app.get("/healthz")
def healthz(): return "ok", 200

@app.get("/diag")
def diag():
    url = request.args.get("url", "")
    if not url: return jsonify({"error":"missing url"}), 400
    ok, msg, sig, meta = check_ibon(url)
    return jsonify({"ok": ok, "sig": sig, "image": (meta or {}).get("image_url"), "msg": msg})

# -------------------- 入口 --------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8080")))