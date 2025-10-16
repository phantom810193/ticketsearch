import os, logging
logging.basicConfig(level=logging.INFO)

app = None
# 1) 先嘗試工廠函式 create_app()
try:
    from app import create_app  # 你的 repo 根目錄 app.py
    app = create_app()
    logging.info("Created app via app.create_app().")
except Exception as e:
    logging.info("create_app() not used or failed: %s", e)
    # 2) 再嘗試直接匯入 app 實例
    try:
        from app import app as _app
        app = _app
        logging.info("Imported app instance from app:app.")
    except Exception as e2:
        logging.error("Import app:app failed: %s", e2)
        app = None
# 3) 有 app 才調整 strict_slashes（Flask）
if app is not None and hasattr(app, "url_map"):
    try:
        app.url_map.strict_slashes = False
    except Exception as e:
        logging.info("skip strict_slashes tweak: %s", e)

# 4) 若前兩步都失敗，提供最小保底（至少 / 與 /healthz 可用）
if app is None:
    from flask import Flask, jsonify
    app = Flask(__name__)

    @app.get("/healthz")
    def _healthz_failed():
        return jsonify(status="fail", reason="app_factory_failed"), 503

    @app.get("/")
    def _root_failed():
        return jsonify(ok=False, reason="app_factory_failed"), 503
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8080)))
