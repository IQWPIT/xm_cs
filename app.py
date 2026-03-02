import os
import random
from datetime import datetime, timedelta
from functools import lru_cache

from flask import Flask, jsonify, render_template, request
from pymongo import DESCENDING, MongoClient


def create_app() -> Flask:
    app = Flask(__name__)

    mongo_uri = os.getenv("MONGODB_URI", "mongodb://127.0.0.1:27017")
    db_name = os.getenv("MONGODB_DB", "dashboard")
    client = MongoClient(mongo_uri, maxPoolSize=30, minPoolSize=5, serverSelectionTimeoutMS=1500)
    db = client[db_name]

    metrics_col = db.metrics_daily
    channels_col = db.channels
    regions_col = db.regions

    metrics_col.create_index([("day", DESCENDING)], background=True)
    channels_col.create_index([("updatedAt", DESCENDING)], background=True)
    regions_col.create_index([("amount", DESCENDING)], background=True)

    @lru_cache(maxsize=1)
    def channel_names():
        return ["官网", "小程序", "电商平台", "线下门店", "代理商"]

    @lru_cache(maxsize=1)
    def region_names():
        return ["华东", "华南", "华北", "华中", "西南", "西北"]

    def make_mock_data():
        now = datetime.now()
        daily_orders = []
        for idx in range(7):
            d = now - timedelta(days=6 - idx)
            daily_orders.append({"day": f"{d.month}/{d.day}", "value": random.randint(160, 360)})

        channel_conversion = [
            {"name": name, "value": round(random.uniform(0.06, 0.28), 3)}
            for name in channel_names()
        ]

        region_sales = [
            {
                "name": name,
                "amount": random.randint(200000, 1100000),
                "ratio": round(random.uniform(-0.08, 0.2), 3),
            }
            for name in region_names()
        ]
        region_sales.sort(key=lambda x: x["amount"], reverse=True)

        total_orders = sum(d["value"] for d in daily_orders)
        total_revenue = sum(r["amount"] for r in region_sales)
        avg_conversion = sum(c["value"] for c in channel_conversion) / len(channel_conversion)

        return {
            "isMock": True,
            "updatedAt": now.strftime("%Y-%m-%d %H:%M:%S"),
            "kpis": [
                {"label": "本周订单", "value": f"{total_orders:,}", "delta": 0.124},
                {"label": "销售额", "value": f"¥{total_revenue:,}", "delta": 0.086},
                {"label": "平均客单价", "value": f"¥{round(total_revenue / max(total_orders, 1)):,}", "delta": -0.018},
                {"label": "转化率", "value": f"{avg_conversion * 100:.1f}%", "delta": 0.031},
            ],
            "dailyOrders": daily_orders,
            "channelConversion": sorted(channel_conversion, key=lambda x: x["value"], reverse=True),
            "regionSales": region_sales,
        }

    def query_real_data():
        try:
            week_metrics = list(metrics_col.find({}, {"_id": 0}).sort("day", DESCENDING).limit(7))
        except Exception:
            return None
        if len(week_metrics) < 2:
            return None

        week_metrics.reverse()
        daily_orders = [{"day": item["day"], "value": int(item.get("orders", 0))} for item in week_metrics]

        channels = list(channels_col.find({}, {"_id": 0, "name": 1, "value": 1}).sort("value", DESCENDING))
        regions = list(regions_col.find({}, {"_id": 0, "name": 1, "amount": 1, "ratio": 1}).sort("amount", DESCENDING).limit(10))
        if not channels or not regions:
            return None

        total_orders = sum(item["value"] for item in daily_orders)
        total_revenue = sum(int(r["amount"]) for r in regions)
        avg_conversion = sum(float(c["value"]) for c in channels) / len(channels)

        return {
            "isMock": False,
            "updatedAt": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "kpis": [
                {"label": "本周订单", "value": f"{total_orders:,}", "delta": 0.052},
                {"label": "销售额", "value": f"¥{total_revenue:,}", "delta": 0.041},
                {"label": "平均客单价", "value": f"¥{round(total_revenue / max(total_orders, 1)):,}", "delta": -0.006},
                {"label": "转化率", "value": f"{avg_conversion * 100:.1f}%", "delta": 0.018},
            ],
            "dailyOrders": daily_orders,
            "channelConversion": channels,
            "regionSales": regions,
        }

    @app.get("/")
    def home():
        return render_template("index.html")

    @app.get("/api/dashboard")
    def dashboard_data():
        payload = query_real_data()
        if payload is None:
            payload = make_mock_data()
        return jsonify(payload)

    @app.post("/api/seed-test-data")
    def seed_test_data():
        data = make_mock_data()

        metrics_docs = []
        for item in data["dailyOrders"]:
            metrics_docs.append({"day": item["day"], "orders": item["value"], "updatedAt": datetime.now()})

        try:
            metrics_col.delete_many({})
            channels_col.delete_many({})
            regions_col.delete_many({})
            if metrics_docs:
                metrics_col.insert_many(metrics_docs)
            channels_col.insert_many(data["channelConversion"])
            regions_col.insert_many(data["regionSales"])
        except Exception as exc:
            return jsonify({"ok": False, "message": f"写入失败: {exc}"}), 500

        return jsonify({"ok": True, "message": "测试数据已写入 MongoDB"})

    @app.post("/api/ingest")
    def ingest_data():
        payload = request.get_json(silent=True) or {}
        daily_orders = payload.get("dailyOrders", [])
        channel_conversion = payload.get("channelConversion", [])
        region_sales = payload.get("regionSales", [])

        if not daily_orders or not channel_conversion or not region_sales:
            return jsonify({"ok": False, "message": "缺少必要字段"}), 400

        try:
            metrics_col.delete_many({})
            channels_col.delete_many({})
            regions_col.delete_many({})

            metrics_col.insert_many([{"day": d["day"], "orders": int(d["value"]), "updatedAt": datetime.now()} for d in daily_orders])
            channels_col.insert_many([{"name": c["name"], "value": float(c["value"]), "updatedAt": datetime.now()} for c in channel_conversion])
            regions_col.insert_many([{"name": r["name"], "amount": int(r["amount"]), "ratio": float(r.get("ratio", 0))} for r in region_sales])
        except Exception as exc:
            return jsonify({"ok": False, "message": f"数据写入失败: {exc}"}), 500

        return jsonify({"ok": True, "message": "数据写入成功"})

    return app


app = create_app()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8080")), debug=False)
