from flask import Flask, jsonify

def create_app():
    app = Flask(__name__)

    @app.get("/healthz")
    def healthz():
        return jsonify(status="ok", source="app_min"), 200

    @app.get("/")
    def root():
        return jsonify(ok=True, from_min=True), 200

    return app

app = create_app()
