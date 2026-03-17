import sys
import os
from pymongo import UpdateOne
from tqdm import tqdm   # ← 进度条

os.environ["NET"] = "TUNNEL"
os.environ["NET3"] = "NXQ"
from dm.connector.mongo.manager3 import get_collection

cat_analyze = get_collection("main_ml_mx", "ml_mx", "cat_analyze")
visual_plus = get_collection("main_ml_mx", "ml_mx", "visual_plus")

# 先一次性取出所有 cat_id，提高效率
cat_ids = list(visual_plus.find({}, {"cat_id": 1}))

for cat in tqdm(cat_ids, desc="Processing cat_id"):
    cat_id = cat["cat_id"]

    data = cat_analyze.find_one({"id": cat_id})
    if not data:
        continue

    visual_plus.update_one(
        {"cat_id": cat_id},
        {"$set": {
            "review_analyzes": data.get("review_analyzes"),
            "update_review": data.get("update_review"),
            "question_analyzes": data.get("question_analyzes"),
            "question_count": data.get("question_count"),
            "update_question": data.get("update_question"),
            "reviewCount": data.get("reviewCount"),
        }},
    )
