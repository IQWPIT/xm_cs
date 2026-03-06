import pandas as pd
import ast
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import heapq
# ==== Mongo 初始化 ====
os.environ["NET"] = "TUNNEL"
os.environ["NET3"] = "NXQ"
from dm.connector.mongo.manager3 import get_collection
from pathlib import Path



visual_plus = get_collection("main_ml_mx", "ml_mx", "_big_Excel")
sku_col = get_collection("main_ml_mx", "ml_mx", "sku")
visual = get_collection("main_ml_mx", "ml_mx", "visual_plus")

# ==== 日志 ====
success_log = Path(r"D:\data\看板\日志\top50_skus_2.txt")
error_log = Path(r"D:\data\看板\日志\top50_skus_error_2.txt")
success_log.touch(exist_ok=True)
error_log.touch(exist_ok=True)

# ==== 已处理类目集合 ====
processed_cats = set()
with open(success_log, "r", encoding="utf-8") as f:
    for line in f:
        if line.strip():
            processed_cats.add(line.split()[0])

# ==== 时间映射 ====
times_list = [202601]
time_map = {202601: (2601, 2512, 2501)}

# ==== 工具函数：安全解析 ====
def safe_eval(x, default):
    if isinstance(x, str):
        try:
            return ast.literal_eval(x)
        except Exception:
            return default
    elif x is None:
        return default
    return x

# ==== 单类目处理函数 ====
def process_cat(cat_id: str):
    if cat_id in processed_cats:
        return None, None  # 已处理过

    try:
        # 获取 cat 下的所有 SKU cat_id 列表
        cat_doc = visual.find_one({"cat_id": cat_id}, {"cat": 1})
        if not cat_doc or "cat" not in cat_doc:
            raise ValueError("visual 中未找到 cat 列表")
        cat_ids = cat_doc["cat"]

        # 初始化每个时间点的堆
        top50_heaps = {t: [] for t in times_list}
        for cat_id_data in tqdm(cat_ids):
            # 分批读取 SKU
            cursor = sku_col.find({"category_id": cat_id_data})
            # cursor = sku_col.find_one({"cat_id": {"$in": cat_ids}})
            for row in cursor:
                monthly_sale_trend = safe_eval(row.get("monthly_sale_trend", {}), {})
                offersInf = safe_eval(row.get("offersInf", []), [])
                sku_id = row.get("sku_id")
                offers_len = len(offersInf)

                for times in times_list:
                    time, _, _ = time_map[times]
                    sale = monthly_sale_trend.get(str(time), 0)
                    heapq.heappush(top50_heaps[times], (sale, offers_len, sku_id))
                    if len(top50_heaps[times]) > 50:  # 只保留 Top50
                        heapq.heappop(top50_heaps[times])

        # 构建更新数据
        update_data = {}
        for times in times_list:
            sorted_top50 = sorted(top50_heaps[times], key=lambda x: (-x[0], -x[1]))
            top50_sku_dict = {sku: {"offersInf_len": offers_len, "order": sale}
                              for sale, offers_len, sku in sorted_top50}
            update_data[f"top50_skus.{times}"] = top50_sku_dict

        visual_plus.update_one({"cat_id": cat_id}, {"$set": update_data}, upsert=True)
        return cat_id, None

    except Exception as e:
        return cat_id, str(e)

# ==== 获取待处理 cat_id ====
all_cats = visual.find({"cat_id":"MLM189045"}, {"cat_id": 1})  # 可修改 limit 或去掉限制
pending_cats = [c.get("cat_id") for c in all_cats if c.get("cat_id")]
print(f"待处理类目数量: {len(pending_cats)}")

# ==== 多线程处理 ====
if pending_cats:
    success_list, error_list = [], []

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = {executor.submit(process_cat, cat_id): cat_id for cat_id in pending_cats}
        for future in tqdm(as_completed(futures), total=len(futures), desc="总进度"):
            cat_id, error = future.result()
            if error:
                error_list.append(f"{cat_id} ❌ 出错：{error}\n")
            elif cat_id:
                success_list.append(f"{cat_id} ✅ 成功处理\n")

    # 写入日志
    if success_list:
        with open(success_log, "a", encoding="utf-8") as f:
            f.writelines(success_list)

    if error_list:
        with open(error_log, "a", encoding="utf-8") as f:
            f.writelines(error_list)
else:
    print("所有类目已处理，无需重复处理。")
