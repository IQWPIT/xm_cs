# -*- coding: utf-8 -*-
import os
import re
from calendar import monthrange
from collections import defaultdict
from pymongo import MongoClient, UpdateOne
from tqdm import tqdm
os.environ["NET"] = "TUNNEL"
os.environ["NET3"] = "NXQ"
from dm.connector.mongo.manager3 import get_collection
from datetime import datetime, timezone, timedelta
# ================= Mongo 连接 =================
visual_plus_col = get_collection("main_ml_mx", "ml_mx", "visual_plus")
sku_col = get_collection("main_ml_mx", "ml_mx", "sku")
every_day_sku_col = get_collection("main_ml_mx", "ml_mx", "every_day_sku")
_big_Excel_col = get_collection("main_ml_mx", "ml_mx", "_big_Excel")
scrapy_buffer_col = get_collection("slave_ml_mx", "ml_scrapy_buffer", "ml_mx")
# 外部 Mongo
# uri = "mongodb://common:ase5yDFHG%24%25FDSdif%40%23GH@localhost:37031/admin?authMechanism=SCRAM-SHA-1&directConnection=true&readPreference=primary"
# client = MongoClient(uri)
# scrapy_buffer_col = client["ml_scrapy_buffer"]["ml_mx"]

# ================= 参数 =================
cat_ids = [doc["cat_id"] for doc in visual_plus_col.find({}, {"cat_id":1})]
print(f"总类目数: {len(cat_ids)}")

time_list = [
    # 202411, 202412, 202501, 202502, 202503, 202504, 202505, 202506,
    # 202507, 202508, 202509, 202510,
    202511,
    # 202512
]
d222 = {}
SUCCESS_LOG = "top50.txt"

def load_success_set():
    s = set()
    if os.path.exists(SUCCESS_LOG):
        with open(SUCCESS_LOG, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                cat, month = line.split(",")
                s.add((cat, int(month)))
    return s

def append_success(cat_id, month):
    with open(SUCCESS_LOG, "a", encoding="utf-8") as f:
        f.write(f"{cat_id},{month}\n")

# ================= 工具函数 =================
def parse_puton_date(v):
    if isinstance(v, datetime):
        return v
    try:
        return datetime.strptime(str(int(v)), "%Y%m%d")
    except:
        return None

def pct(val):
    return val if val else 0

def extract_volume_weight(attributes):
    length = width = height = weight = None
    for attr in attributes or []:
        aid, val = attr.get("id"), attr.get("v_name_en")
        if not aid or not val:
            continue
        nums = re.findall(r"[\d.]+", val)
        if not nums:
            continue
        try:
            num = float(nums[0])
        except:
            continue
        val_lower = val.lower()
        if aid == "PACKAGE_LENGTH":
            length = num * 0.01 if "cm" in val_lower else num * 0.001 if "mm" in val_lower else num
        elif aid == "PACKAGE_WIDTH":
            width = num * 0.01 if "cm" in val_lower else num * 0.001 if "mm" in val_lower else num
        elif aid == "PACKAGE_HEIGHT":
            height = num * 0.01 if "cm" in val_lower else num * 0.001 if "mm" in val_lower else num
        elif aid == "PACKAGE_WEIGHT":
            weight = num / 1000 if "g" in val_lower else num
    volume = length * width * height if length and width and height else 0
    return volume, weight or 0

# ================= dT 类型检测 =================
sample_dT = every_day_sku_col.find_one({}, {"dT": 1, "_id": 0})
dT_type = type(sample_dT["dT"]) if sample_dT and "dT" in sample_dT else None

# ================= 批量新品销量查询 =================
from collections import defaultdict
from datetime import datetime, timedelta
import calendar

def batch_new_sales(sku_infos, sku_ids, start, end_delta, end2):
    if not sku_ids:
        return {}

    start_int = int(start.strftime("%Y%m%d"))
    month_end_int = int(end2.strftime("%Y%m%d"))

    sku_date_map = {}
    invalid_skus = set()

    for sku in sku_ids:
        puton_date_str = str(sku_infos.get(sku, {}).get("puton_date", ""))
        if not puton_date_str.isdigit():
            invalid_skus.add(sku)
            continue

        # ✅ 处理 6 位 YYYYMM，补 01
        if len(puton_date_str) == 6:
            puton_date_str += "01"
        elif len(puton_date_str) != 8:
            invalid_skus.add(sku)
            continue

        year, month_num, day = int(puton_date_str[:4]), int(puton_date_str[4:6]), int(puton_date_str[6:])
        # ✅ 修正 day 超出当月天数
        max_day = calendar.monthrange(year, month_num)[1]
        if day > max_day:
            day = max_day

        try:
            t2 = datetime(year, month_num, day) + end_delta
        except ValueError:
            invalid_skus.add(sku)
            continue

        t2_int = int(t2.strftime("%Y%m%d"))
        if t2_int < start_int:
            invalid_skus.add(sku)
            continue
        if t2_int > month_end_int:
            t2_int = month_end_int

        sku_date_map[sku] = t2_int

    # 初始化无效 SKU
    result = {sku: 0 for sku in invalid_skus}
    if not sku_date_map:
        return result

    # 查询 MongoDB
    cursor = every_day_sku_col.find(
        {
            "sl": {"$in": list(sku_date_map.keys())},
            "dT": {"$gte": start_int, "$lt": month_end_int}
        },
        {"sl": 1, "oD": 1, "dT": 1}
    )

    # Python 中按 SKU + t2_int 聚合
    totals = defaultdict(int)
    for doc in cursor:
        sku = doc["sl"]
        dt = doc.get("dT", 0)
        if dt <= sku_date_map.get(sku, month_end_int):
            totals[sku] += doc.get("oD", 0)

    # 写入结果
    for sku in sku_date_map:
        result[sku] = totals.get(sku, 0)

    return result

# ================= 主处理函数 =================
def process_cat_month(cat_id, month):
    data = _big_Excel_col.find_one({"cat_id": cat_id}, {"top50_skus": 1, "_id": 0})
    if not data or "top50_skus" not in data:
        return None

    month_data = data["top50_skus"].get(str(month))
    if not month_data:
        return None

    sku_ids = list(month_data.keys())[:50]
    # sku_ids = ["MLM3836889136"]
    year, month_num = int(str(month)[:4]), int(str(month)[4:])
    month_start = datetime(year, month_num, 1)
    month_end = datetime(year, month_num, monthrange(year, month_num)[1])

    # 批量查询 SKU 信息
    sku_infos = {doc["sku_id"]: doc for doc in sku_col.find(
        {"sku_id": {"$in": sku_ids}},
        {"sku_id":1,"active_price":1,"stock_type":1,"sellerName":1,"brand":1,
         "puton_date":1,"offersInf":1,"conversion_all_order":1,"conversion_all_view":1,
         "attributes":1,"_id":0}
    )}

    # 批量查询 rating AVG

    month_start2 = datetime(month_start.year,month_start.month, 1, tzinfo=timezone.utc)
    month_end2 = datetime(month_end.year,month_end.month, month_end.day,tzinfo=timezone.utc)
    rating_map = {
        d["_id"]: d["avg"]
        for d in scrapy_buffer_col.aggregate([
            {"$match": {
                "sku_id": {"$in": sku_ids},
                "t": {"$gte": month_start2, "$lte": month_end2}
            }},
            {"$group": {
                "_id": "$sku_id",
                "avg": {"$avg": "$rating_avg"}
            }}
        ])
    }
    new30_map = batch_new_sales(sku_infos,sku_ids, month_start, timedelta(days=30),month_end)
    new90_map = batch_new_sales(sku_infos,sku_ids, month_start, timedelta(days=90),month_end)
    new180_map = batch_new_sales(sku_infos,sku_ids, month_start, timedelta(days=180),month_end)

    stats = defaultdict(float)
    stats.update({
        "month_sales":0,"month_gmv":0,"sku_cnt":0,
        "full_sales":0,"full_sku_cnt":0,
        "nor_sales":0,"nor_sku_cnt":0,
        "shop_set":set(),"brand_set":set(),
        "new_30_cnt":0,"new_30_sales":0,
        "new_90_cnt":0,"new_90_sales":0,
        "new_180_cnt":0,"new_180_sales":0,
        "rating_sum":0,"rating_cnt":0,
        "offers_sku_cnt":0,"offers_sales":0,
        "conversion_all_order":0,"conversion_all_view":0,
        "volume_m3_sum":0,"volume_m3_cnt":0,
        "volume_weight_sum":0,"volume_weight_cnt":0
    })

    for sku_id in sku_ids:
        sku_info = sku_infos.get(sku_id)
        if not sku_info:
            continue

        sku_month = month_data.get(sku_id,{})
        # order = sum(d.get("oD",0) for d in sku_month.get("day",{}).values())
        order = sku_month["order"]

        # 基础统计
        stats["month_sales"] += order
        stats["month_gmv"] += order*int(sku_info.get("active_price") or 0)
        stats["sku_cnt"] += 1
        stats["conversion_all_order"] += int(sku_info.get("conversion_all_order") or 0)
        stats["conversion_all_view"] += int(sku_info.get("conversion_all_view") or 0)
        if sku_info.get("sellerName"): stats["shop_set"].add(sku_info["sellerName"])
        if sku_info.get("brand"): stats["brand_set"].add(sku_info["brand"])
        if sku_info.get("stock_type") == "ful":
            stats["full_sales"] += order
            stats["full_sku_cnt"] += 1
        if sku_info.get("stock_type") == "nor":
            stats["nor_sales"] += order
            stats["nor_sku_cnt"] += 1
        if sku_info.get("offersInf"):
            stats["offers_sku_cnt"] += 1
            stats["offers_sales"] += order

        vol, wgt = extract_volume_weight(sku_info.get("attributes"))
        if vol>0: stats["volume_m3_sum"] += vol; stats["volume_m3_cnt"] += 1
        if wgt>0: stats["volume_weight_sum"] += wgt; stats["volume_weight_cnt"] += 1

        # 新品销量
        if sku_id in new30_map and new30_map[sku_id]>0:
            stats["new_30_cnt"] += 1
            stats["new_30_sales"] += new30_map[sku_id]
        if sku_id in new90_map and new90_map[sku_id]>0:
            stats["new_90_cnt"] += 1
            stats["new_90_sales"] += new90_map[sku_id]
        if sku_id in new180_map and new180_map[sku_id]>0:
            stats["new_180_cnt"] += 1
            stats["new_180_sales"] += new180_map[sku_id]

        # rating
        r = rating_map.get(sku_id)
        if r:
            stats["rating_sum"] += r
            stats["rating_cnt"] += 1

    avg_rating = round(stats["rating_sum"]/stats["rating_cnt"],2) if stats["rating_cnt"] else 0

    result = {
        "cat_id": cat_id,
        "time": month,
        "top50_month_sales": stats["month_sales"],
        "top50_month_gmv": stats["month_gmv"],
        "top50_avg_order_price": stats["month_gmv"]/stats["month_sales"] if stats["month_sales"] else 0,
        "top50_avg_product_sales": stats["month_sales"]/stats["sku_cnt"] if stats["sku_cnt"] else 0,
        "top50_sales_growth_index": 0,  # 简化
        "top50_full_sales_ratio": pct(stats["full_sales"]/stats["month_sales"] if stats["month_sales"] else 0),
        "top50_avg_volume_m3": stats["volume_m3_sum"]/stats["volume_m3_cnt"] if stats["volume_m3_cnt"] else 0,
        "top50_avg_volume_weight_kg": stats["volume_weight_sum"]/stats["volume_weight_cnt"] if stats["volume_weight_cnt"] else 0,
        "top50_offers_sku_count": stats["offers_sku_cnt"],
        "top50_offers_sales": stats["offers_sales"],
        "top50_offers_sales_ratio": pct(stats["offers_sales"]/stats["month_sales"] if stats["month_sales"] else 0),
        "top50_seller_count": len(stats["shop_set"]),
        "top50_brand_count": len(stats["brand_set"]),
        "top50_full_sku_count": stats["full_sku_cnt"],
        "top50_full_sku_ratio": pct(stats["full_sku_cnt"]/stats["sku_cnt"] if stats["sku_cnt"] else 0),
        "top50_full_last30d_sales": stats["full_sales"],
        "top50_full_avg_sales": stats["full_sales"]/stats["full_sku_cnt"] if stats["full_sku_cnt"] else 0,
        "top50_nor_sku_count": stats["nor_sku_cnt"],
        "top50_nor_last30d_sales": stats["nor_sales"],
        "top50_nor_avg_sales": stats["nor_sales"]/stats["nor_sku_cnt"] if stats["nor_sku_cnt"] else 0,
        "top50_new_30d_count": stats["new_30_cnt"],
        "top50_new_30d_sales": stats["new_30_sales"],
        "top50_new_30d_sales_ratio": pct(stats["new_30_sales"]/stats["month_sales"] if stats["month_sales"] else 0),
        "top50_new_90d_count": stats["new_90_cnt"],
        "top50_new_90d_sales": stats["new_90_sales"],
        "top50_new_90d_sales_ratio": pct(stats["new_90_sales"]/stats["month_sales"] if stats["month_sales"] else 0),
        "top50_new_180d_count": stats["new_180_cnt"],
        "top50_new_180d_sales": stats["new_180_sales"],
        "top50_new_180d_sales_ratio": pct(stats["new_180_sales"]/stats["month_sales"] if stats["month_sales"] else 0),
        "top50_avg_rating": avg_rating,
        "top50_avg_conversion_rate": pct(stats["conversion_all_order"]/stats["conversion_all_view"] if stats["conversion_all_view"] else 0)
    }
    return result

# ================= 单个类目顺序处理 =================
if __name__ == "__main__":
    success_set = load_success_set()
    print(f"已成功处理记录数: {len(success_set)}")

    for cat in tqdm(cat_ids, desc="处理类目"):
        # 当前 cat_id 待处理月份
        months_to_process = [month for month in time_list if (cat, month) not in success_set]
        if not months_to_process:
            continue

        bulk_ops = []

        # 多线程处理当前 cat_id 的月份
        from concurrent.futures import ThreadPoolExecutor, as_completed

        with ThreadPoolExecutor(max_workers=min(12, len(months_to_process))) as executor:
            futures = {executor.submit(process_cat_month, cat, month): month for month in months_to_process}

            for future in as_completed(futures):
                month = futures[future]
                try:
                    res = future.result()
                except Exception as e:
                    print(f"[ERROR] {cat}-{month}: {e}")
                    continue

                if res:
                    bulk_ops.append(UpdateOne(
                        {"cat_id": cat, "time": month},
                        {"$set": res},
                        upsert=True
                    ))
                    append_success(cat, month)

                # 批量写入，每 50 条写一次
                if len(bulk_ops) >= 5:
                    if bulk_ops:
                        _big_Excel_col.bulk_write(bulk_ops, ordered=False)
                        bulk_ops.clear()

        # 写入剩余数据
        if bulk_ops:
            _big_Excel_col.bulk_write(bulk_ops, ordered=False)
            bulk_ops.clear()
