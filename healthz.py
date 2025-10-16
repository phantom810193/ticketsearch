from flask import Blueprint, jsonify
import os, socket, datetime

bp = Blueprint("healthz", __name__, url_prefix="")

def _safe(s):  # 防守式轉字串
    try:
        return str(s) if s is not None else None
    except Exception:
        return None

def _payload():
    return {
        "status": "ok",
        "service": "ticketsearch",
        "time": datetime.datetime.utcnow().isoformat() + "Z",
        "host": _safe(socket.gethostname()),
        "region": os.environ.get("REGION", "asia-east1"),
        "project": os.environ.get("GOOGLE_CLOUD_PROJECT") or os.environ.get("PROJECT_ID"),
        "revision": os.environ.get("K_REVISION"),
    }

@bp.get("/")
def root():
    return (
        "ticketsearch backend is running. Try /healthz or /liff/activities?debug=1\n",
        200,
        {"Content-Type": "text/plain; charset=utf-8"},
    )

# 明確註冊 /healthz 與 /healthz/（不依賴 strict_slashes）
@bp.get("/healthz")
@bp.get("/healthz/")
def healthz():
    try:
        return jsonify(_payload()), 200
    except Exception as e:
        # 就算發生意外，也避免 500
        return jsonify({"status": "fail", "reason": f"health_error:{type(e).__name__}:{e}"}), 200

# 路由清單（簡化版；失敗也不丟 500）
@bp.get("/__routes")
def routes():
    try:
        from flask import current_app
        rules = []
        for r in sorted(current_app.url_map.iter_rules(), key=lambda x: x.rule):
            meth = sorted(m for m in (getattr(r, "methods", []) or []) if m not in ("HEAD", "OPTIONS"))
            rules.append({"rule": r.rule, "methods": meth})
        return jsonify({"count": len(rules), "routes": rules}), 200
    except Exception as e:
        return jsonify({"count": 0, "routes": [], "note": f"routes_lite:{type(e).__name__}:{e}"}), 200
