#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
One‑click LINE Rich Menu setup script.
- Creates a 4‑area rich menu (2500x1686) for your bot
- Uploads a background image
- Sets it as the default rich menu

Usage:
  export LINE_CHANNEL_ACCESS_TOKEN="YOUR_LONG_LIFF_MESSAGING_TOKEN"
  python scripts/richmenu_setup.py --image path/to/richmenu.png \
    --ibon "https://ticket.ibon.com.tw/Index/entertainment"

If you don't pass --image, the menu will still be created (with a blank image step).
You can re-run with --image later to update the background.

Actions (editable in build_richmenu_body):
  1) 🔍 選活動監看 -> sends "/menu"
  2) 🧾 我的任務  -> sends "/list"
  3) 📖 說明       -> sends "/help"
  4) 🔗 ibon 活動頁 -> opens given URL (default ibon entertainment)

"""

import os
import json
import argparse
import requests

API_HOST = "https://api.line.me/v2/bot"
HEADERS = lambda token: {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}

def build_richmenu_body(ibon_url: str):
    # Full size rich menu 2500x1686, 4 areas (2x2 grid)
    # Adjust coordinates if you have a different design.
    return {
        "size": {"width": 2500, "height": 1686},
        "selected": True,
        "name": "TicketWatcher Main",
        "chatBarText": "功能選單",
        "areas": [
            {   # top-left: /menu
                "bounds": {"x": 0, "y": 0, "width": 1250, "height": 843},
                "action": {"type": "message", "label": "選活動監看", "text": "/menu"}
            },
            {   # top-right: /list
                "bounds": {"x": 1250, "y": 0, "width": 1250, "height": 843},
                "action": {"type": "message", "label": "我的任務", "text": "/list"}
            },
            {   # bottom-left: /help
                "bounds": {"x": 0, "y": 843, "width": 1250, "height": 843},
                "action": {"type": "message", "label": "說明", "text": "/help"}
            },
            {   # bottom-right: open ibon
                "bounds": {"x": 1250, "y": 843, "width": 1250, "height": 843},
                "action": {"type": "uri", "label": "ibon 活動頁", "uri": ibon_url}
            },
        ]
    }

def create_richmenu(token: str, body: dict) -> str:
    resp = requests.post(f"{API_HOST}/richmenu", headers=HEADERS(token), data=json.dumps(body))
    if resp.status_code != 200:
        raise SystemExit(f"[create] {resp.status_code} {resp.text}")
    rm_id = resp.json().get("richMenuId")
    print(f"[OK] Created rich menu: {rm_id}")
    return rm_id

def upload_image(token: str, richmenu_id: str, image_path: str):
    with open(image_path, "rb") as f:
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "image/png"}
        resp = requests.post(f"{API_HOST}/richmenu/{richmenu_id}/content", headers=headers, data=f.read())
    if resp.status_code != 200:
        raise SystemExit(f"[upload] {resp.status_code} {resp.text}")
    print(f"[OK] Uploaded image to rich menu {richmenu_id}")

def set_default(token: str, richmenu_id: str):
    resp = requests.post(f"{API_HOST}/user/all/richmenu/{richmenu_id}", headers=HEADERS(token))
    if resp.status_code != 200:
        raise SystemExit(f"[default] {resp.status_code} {resp.text}")
    print(f"[OK] Set default rich menu: {richmenu_id}")

def delete_all(token: str):
    # Utility: delete all existing rich menus (optional)
    resp = requests.get(f"{API_HOST}/richmenu/list", headers=HEADERS(token))
    if resp.status_code != 200:
        raise SystemExit(f"[list] {resp.status_code} {resp.text}")
    for rm in resp.json().get("richmenus", []):
        rid = rm.get("richMenuId")
        r = requests.delete(f"{API_HOST}/richmenu/{rid}", headers=HEADERS(token))
        print(f"[del] {rid} -> {r.status_code}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--image", help="PNG image path for the rich menu background")
    ap.add_argument("--ibon", default="https://ticket.ibon.com.tw/Index/entertainment", help="ibon entertainment URL")
    ap.add_argument("--delete-all", action="store_true", help="Delete all existing rich menus first")
    args = ap.parse_args()

    token = os.getenv("LINE_CHANNEL_ACCESS_TOKEN")
    if not token:
        raise SystemExit("Please set LINE_CHANNEL_ACCESS_TOKEN env var.")

    if args.delete_all:
        delete_all(token)

    body = build_richmenu_body(args.ibon)
    rid = create_richmenu(token, body)

    if args.image:
        upload_image(token, rid, args.image)

    set_default(token, rid)
    print("[DONE] Rich menu is ready.")

if __name__ == "__main__":
    main()
