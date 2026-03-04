# -*- coding: utf-8 -*-
import os
from pymongo import UpdateOne
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
os.environ["NET"] = "TUNNEL"
os.environ["NET3"] = "NXQ"
from dm.connector.mongo.manager3 import get_collection
def get_mongo_collections():
    visual_plus_col = get_collection("main_ml_mx", "ml_mx", "visual_plus")
    cat_col = get_collection("main_ml_mx", "ml_mx", "cat")
    return visual_plus_col, cat_col

def fetch_all_sub_cats(visual_plus_col, cat_col):
    # 获取所有 cat_id
    cat_ids = [doc["cat_id"] for doc in visual_plus_col.find({}, {"cat_id": 1})]

    # 收集所有子类目
    sub_cats = []
    for parent_cat_id in cat_ids:
        sub_cats.extend(list(cat_col.find(
            {"id": parent_cat_id},
            {"level": 1, "leaf": 1, "catName": 1, "id": 1}
        )))
    return sub_cats

def update_batch(batch, visual_plus_col):
    """批量更新函数，返回更新条目数"""
    operations = []
    for cat in batch:
        operations.append(
            UpdateOne(
                {"cat_id": cat["id"]},
                {"$set": {
                    "catName": cat["catName"],
                    "leaf": cat["leaf"],
                    "level": cat["level"],
                }},
                upsert=True
            )
        )
    if operations:
        visual_plus_col.bulk_write(operations)
    return len(batch)

def main(batch_size=50, max_workers=4):
    visual_plus_col, cat_col = get_mongo_collections()
    sub_cats = fetch_all_sub_cats(visual_plus_col, cat_col)

    if not sub_cats:
        print("没有子类目需要更新。")
        return

    # 分批处理
    batches = [sub_cats[i:i + batch_size] for i in range(0, len(sub_cats), batch_size)]

    # 多线程 + 进度条
    results = []
    with tqdm(total=len(sub_cats), desc="Updating cats") as pbar:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(update_batch, batch, visual_plus_col) for batch in batches]
            for future in as_completed(futures):
                count = future.result()
                pbar.update(count)
                results.append(count)

    print(f"更新完成，共处理 {len(sub_cats)} 条子类目。")

if __name__ == "__main__":
    main()
