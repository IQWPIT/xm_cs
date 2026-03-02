import json
import os
from datetime import datetime
from functools import lru_cache
from pathlib import Path

from flask import Flask, jsonify, render_template, request

BASE_DIR = Path(__file__).resolve().parent
MOCK_DATA_PATH = BASE_DIR / "data" / "mock_dashboard.json"


def create_app() -> Flask:
    app = Flask(__name__)

    runtime_state = {"dashboard": None}

    @lru_cache(maxsize=1)
    def load_mock_data() -> dict:
        with MOCK_DATA_PATH.open("r", encoding="utf-8") as fp:
            payload = json.load(fp)
        payload["isMock"] = True
        return payload

    def get_dashboard_payload() -> dict:
        if runtime_state["dashboard"] is not None:
            return runtime_state["dashboard"]

        payload = load_mock_data().copy()
        payload["updatedAt"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        payload["isMock"] = True
        return payload

    @app.get("/")
    def home():
        return render_template("index.html")

    @app.get("/api/dashboard")
    def dashboard_data():
        return jsonify(get_dashboard_payload())

    @app.get("/api/test-data")
    def test_data():
        return jsonify(load_mock_data())

    @app.post("/api/ingest")
    def ingest_data():
        payload = request.get_json(silent=True) or {}

        required = ["kpis", "dailyOrders", "channelConversion", "regionSales"]
        missing = [key for key in required if not isinstance(payload.get(key), list) or not payload.get(key)]
        if missing:
            return jsonify({"ok": False, "message": f"缺少或无效字段: {', '.join(missing)}"}), 400

        runtime_state["dashboard"] = {
            "isMock": False,
            "updatedAt": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "kpis": payload["kpis"],
            "dailyOrders": payload["dailyOrders"],
            "channelConversion": payload["channelConversion"],
            "regionSales": payload["regionSales"],
        }
        return jsonify({"ok": True, "message": "数据已写入内存（当前进程有效）"})

    @app.post("/api/reset")
    def reset_data():
        runtime_state["dashboard"] = None
        return jsonify({"ok": True, "message": "已重置为测试数据模式"})

    return app


app = create_app()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8080")), debug=False)
