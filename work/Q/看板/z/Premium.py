import os
from pymongo import UpdateOne
from tqdm import tqdm

os.environ["NET"] = "TUNNEL"
os.environ["NET3"] = "NXQ"
from dm.connector.mongo.manager3 import get_collection

cat_col = get_collection("main_ml_mx", "ml_mx", "cat")
visual_plus_col = get_collection("main_ml_mx", "ml_mx", "visual_plus")
cat_id = "MLM1055"

def update_listing_prices(cat_id):
    """
    从 cat 集合获取 listing_prices 并更新 visual_plus。
    处理整个集合。
    """
    data_cursor = visual_plus_col.find({"cat_id": cat_id}, {"cat_id": 1})

    updates = []
    processed = 0

    for doc in tqdm(data_cursor, desc="Updating listing_prices"):
        cat_id = doc.get("cat_id")
        if not cat_id:
            continue

        cat_doc = cat_col.find_one({"id": cat_id}, {"listing_prices": 1})
        if not cat_doc or not cat_doc.get("listing_prices"):
            continue

        listing = cat_doc["listing_prices"][0]
        updates.append(
            UpdateOne(
                {"cat_id": cat_id},
                {"$set": {
                    "listing_prices": {
                        "name": listing.get("listing_type_name"),
                        "sale_fee_ratio": listing.get("sale_fee_ratio")
                    }
                }}
            )
        )
        processed += 1

        # 批量提交，避免内存占用过高
        if len(updates) >= 500:
            result = visual_plus_col.bulk_write(updates)
            print(f"Batch updated: Matched {result.matched_count}, Modified {result.modified_count}")
            updates = []

    # 提交剩余的更新
    if updates:
        result = visual_plus_col.bulk_write(updates)
        print(f"Final batch updated: Matched {result.matched_count}, Modified {result.modified_count}")

    print(f"Total processed documents: {processed}")


# 执行更新整个集合
update_listing_prices()
