import json
import os
import sys

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from app import (  # noqa: E402
    app as flask_app,
    build_ibon_details_url,
    sanitize_details_url,
    _clean_venue_text,
    _collect_datetime_candidates,
    _parse_price_value,
)


@pytest.fixture()
def client():
    flask_app.testing = True
    with flask_app.test_client() as client:
        yield client


def _assert_status(resp, allowed_codes):
    assert resp.status_code in allowed_codes, f"unexpected status {resp.status_code} for {resp.request.path}"


def test_liff_activities(client):
    resp = client.get("/liff/activities")
    _assert_status(resp, {200})


def test_api_liff_concerts(client):
    resp = client.get("/api/liff/concerts")
    _assert_status(resp, {200})


def test_api_liff_concerts_debug_trace(client):
    resp = client.get("/api/liff/concerts", query_string={"debug": "1"})
    _assert_status(resp, {200})
    payload = resp.get_json()
    assert isinstance(payload, dict)
    assert "trace" in payload
    assert isinstance(payload["trace"], list)


def test_helper_build_and_sanitize_details_url():
    built = build_ibon_details_url("39125", "ENTERTAINMENT")
    assert built.endswith("id=39125&pattern=ENTERTAINMENT")
    messy = "https://ticket.ibon.com.tw/ActivityInfo/Details/39125?foo=1"
    cleaned = sanitize_details_url(messy)
    assert cleaned.endswith("id=39125&pattern=ENTERTAINMENT")


def test_clean_venue_text_removes_time():
    raw = "TICC 臺北國際會議中心 18:00"
    cleaned = _clean_venue_text(raw)
    assert cleaned == "TICC 臺北國際會議中心"


def test_collect_datetime_candidates_filters_sale():
    lines = [
        "售票期間：2024/05/01 10:00",
        "演出日期：2024/06/10 19:30",
    ]
    candidates = _collect_datetime_candidates(lines)
    assert candidates and candidates[0][0].year == 2024
    assert candidates[0][0].month == 6


def test_collect_datetime_candidates_skips_ranges():
    lines = [
        "演出日期：2024/07/01 19:30",
        "演出時間 2024/07/01 19:30 ~ 21:30",
        "活動時間 2024/07/02 14:00",
    ]
    candidates = _collect_datetime_candidates(lines)
    assert candidates
    for _, _, raw in candidates:
        assert "~" not in raw and "～" not in raw


def test_parse_price_value_handles_currency():
    assert _parse_price_value("票價 NT$2,800") == 2800


def test_api_liff_quick_check(client):
    resp = client.get("/api/liff/quick-check", query_string={"url": "https://example.com"})
    _assert_status(resp, {200})


@pytest.mark.parametrize("endpoint", ["/api/liff/watch", "/api/liff/unwatch"])
def test_watch_endpoints_no_firestore(client, endpoint):
    payload = {"chat_id": "test", "url": "https://example.com", "period": 60}
    resp = client.post(endpoint, data=json.dumps(payload), content_type="application/json")
    _assert_status(resp, {200})
    data = resp.get_json()
    assert isinstance(data, dict)
    assert data.get("ok") is False
