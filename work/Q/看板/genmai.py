import sys
import os
from pymongo import UpdateOne
from collections import defaultdict

os.environ["NET"] = "TUNNEL"
os.environ["NET3"] = "NXQ"
from dm.connector.mongo.manager3 import get_collection

visual_plus = get_collection("main_ml_mx", "ml_mx", "visual_plus")
sku = get_collection("main_ml_mx", "ml_mx", "sku")

# 固定时间参数
time = "2510"
year_time = "2509"
chain_time = "2410"
times = f"20{time}"

# 只获取需要计算的 cat_id
cat_ids = {
    d["cat_id"]
    for d in visual_plus.find(
        {f"offersInf.{times}": {"$exists": False}},  # true/False
        {"cat_id": 1}
    )
}

print(f"共 {len(cat_ids)} 个类目需要处理")

# ------------------------------------
#   Streaming 拉取 SKU（Projection + batch）
# ------------------------------------
cursor = sku.find(
    {"category_id": {"$in": list(cat_ids)}},
    {
        "category_id": 1,
        "monthly_sale_trend": 1,
        "offersInf": 1
    },
    batch_size=2000     # ⭐流式加载
)

# category 聚合容器（自动 初始化 0）
agg = defaultdict(lambda: {
    "order_g": 0, "order_f": 0,
    f"order_g_{year_time}": 0, f"order_f_{year_time}": 0,
    f"order_g_{chain_time}": 0, f"order_f_{chain_time}": 0,
})

update_requests = []
processed = 0
FLUSH_INTERVAL = 50000     # ⭐每 5 万条 SKU 清一次内存

for doc in cursor:
    processed += 1
    c = doc["category_id"]

    offers = doc.get("offersInf") or []
    trend = doc.get("monthly_sale_trend") or {}

    is_follow = isinstance(offers, list) and len(offers) > 0

    t_now = trend.get(time, 0)
    t_2509 = trend.get(year_time, 0)
    t_2410 = trend.get(chain_time, 0)

    if is_follow:
        agg[c]["order_g"] += t_now
        agg[c]["order_g_2509"] += t_2509
        agg[c]["order_g_2410"] += t_2410
    else:
        agg[c]["order_f"] += t_now
        agg[c]["order_f_2509"] += t_2509
        agg[c]["order_f_2410"] += t_2410

    # 如果数据量很大，定期 flush 到 DB，防止 OOM
    if processed % FLUSH_INTERVAL == 0:
        print(f"已处理 {processed} 条 SKU，写入 MongoDB 中……")
        update_requests = []

        # 对每个 category 执行 update
        for c, v in agg.items():
            order_g, order_f = v["order_g"], v["order_f"]
            total = order_g + order_f or 1

            order_g_2509 = v["order_g_2509"] or 1
            order_f_2509 = v["order_f_2509"] or 1
            order_g_2410 = v["order_g_2410"] or 1
            order_f_2410 = v["order_f_2410"] or 1

            update_data = {
                f"offersInf.{times}": {
                    "follow": {
                        "order": order_g,
                        "proportion": order_g / total,
                        "chain_ratio": order_g / order_g_2410,
                        "year_ratio": order_g / order_g_2509,
                    },
                    "non_follow": {
                        "order": order_f,
                        "proportion": order_f / total,
                        "chain_ratio": order_f / order_f_2410,
                        "year_ratio": order_f / order_f_2509,
                    }
                }
            }

            update_requests.append(
                UpdateOne({"cat_id": c}, {"$set": update_data}, upsert=True)
            )

        visual_plus.bulk_write(update_requests)
        agg.clear()     # 清空内存

# ---------------------- 结束后再 Flush 一次 ----------------------------
print("完成全部 SKU 遍历，执行最终写入…")

update_requests = []
for c, v in agg.items():
    order_g, order_f = v["order_g"], v["order_f"]
    total = order_g + order_f or 1

    order_g_2509 = v["order_g_2509"] or 1
    order_f_2509 = v["order_f_2509"] or 1
    order_g_2410 = v["order_g_2410"] or 1
    order_f_2410 = v["order_f_2410"] or 1

    update_data = {
        f"offersInf.{times}": {
            "follow": {
                "order": order_g,
                "proportion": order_g / total,
                "chain_ratio": order_g / order_g_2410,
                "year_ratio": order_g / order_g_2509,
            },
            "non_follow": {
                "order": order_f,
                "proportion": order_f / total,
                "chain_ratio": order_f / order_f_2410,
                "year_ratio": order_f / order_f_2509,
            }
        }
    }

    update_requests.append(
        UpdateOne({"cat_id": c}, {"$set": update_data}, upsert=True)
    )

if update_requests:
    visual_plus.bulk_write(update_requests)

print(f"完成！共处理 {processed} 条 SKU。")
