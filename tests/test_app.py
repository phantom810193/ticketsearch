import json
import os
import sys

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from app import app as flask_app


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


def test_api_liff_quick_check(client):
    resp = client.get("/api/liff/quick-check", query_string={"url": "https://example.com"})
    _assert_status(resp, {200})


@pytest.mark.parametrize("endpoint", ["/api/liff/watch", "/api/liff/unwatch"])
def test_watch_endpoints_no_firestore(client, endpoint):
    payload = {"chat_id": "test", "url": "https://example.com", "period": 60}
    resp = client.post(endpoint, data=json.dumps(payload), content_type="application/json")
    _assert_status(resp, {200, 401, 403, 503})
