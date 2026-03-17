import os
import time
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
import pymysql
import pandas as pd
from pymongo import MongoClient
from tqdm import tqdm
os.environ["NET"] = "TUNNEL"
os.environ["NET3"] = "NXQ"
from dm.connector.mongo.manager3 import get_collection
ml = get_collection("ml","lj","lj_variation_info")
print(ml.find_one())
input()
from datetime import datetime
# ---------------- MySQL ----------------
def get_mysql_conn():
    return pymysql.connect(
        host="114.132.88.73",
        user="root",
        password="damaitoken@2026#",
        database="DM_spiders",
        port=3306,
        charset="utf8mb4"
    )

def fetch_tasks(batch_size=50):
    conn = get_mysql_conn()#AND status=0
    sql = f"""
    SELECT id, item_id, site_id, state, status, create_time, update_time
    FROM lj_variation_task
    WHERE state=1 AND ( status=0 OR status=-1)
    ORDER BY update_time DESC
    LIMIT {batch_size}
    """
    df = pd.read_sql(sql, conn)
    conn.close()
    return df

def update_task_status(task_id, status, _id, src_col):
    try:
        conn = get_mysql_conn()
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with conn.cursor() as cursor:
            cursor.execute("UPDATE lj_variation_task SET status=%s WHERE id=%s", (status, task_id))
            cursor.execute("UPDATE lj_variation_task SET clean_time=%s WHERE id=%s", (now_str, task_id))
        conn.commit()
        if _id:
            src_col.update_one({"_id": _id}, {"$set": {"status": status}})
    except Exception as e:
        print(f"MySQL 更新失败 task_id={task_id} status={status} 错误：{e}")
    finally:
        conn.close()

# ---------------- MongoDB ----------------
MONGO_URI = (
    "mongodb://common:ase5yDFHG%24%25FDSdif%40%23GH@localhost:10001/lj"
    "?authMechanism=SCRAM-SHA-1&authSource=admin&directConnection=true"
)
client = MongoClient(MONGO_URI)
db = client["lj"]
src_col = db["lj_variation_info"]

SITE_ID_MAP = {
    "MLM": "ml_mx",
    "MLB": "ml_br",
    "MLC": "ml_cl",
    "MCO": "ml_co",
    "MLA": "ml_la"
}

# ---------------- 任务处理函数 ----------------
def process_task(row):
    task_id = row["id"]
    item_id = row["item_id"]
    site_code = row["site_id"]
    site = SITE_ID_MAP.get(site_code)

    try:
        doc = src_col.find_one(
            {
                "mysql_task_id": task_id,
                "item_id": item_id,
                # "status": {"$exists": False}
            },
            sort=[("_id", -1)]
        )
        if not doc:
            update_task_status(task_id, -1, None, src_col)
            return f"任务 {task_id} 未找到 Mongo 数据"

        _id = doc["_id"]
        variationInfo = doc.get("response", {}).get("data", {}).get("data", {}).get("variationInfo")
        if not variationInfo:
            update_task_status(task_id, -1, _id, src_col)
            return f"任务 {task_id} 数据结构异常"

        # ---------------- 聚合 ----------------
        attr_stat = defaultdict(lambda: defaultdict(lambda: {"soldQuantity":0,"sold_true":0,"picture_ids":[]}))
        combo_stat = defaultdict(lambda: {"soldQuantity":0,"sold_true":0,"picture_ids":[]})
        total_sold_true = 0
        total_soldQuantity = 0

        for v in variationInfo:
            soldQuantity = v.get("soldQuantity", 0)
            sold_true = v.get("sold_true", 0)
            picture_ids = v.get("pictures", [])
            attrs = v.get("attrs", [])

            total_sold_true += sold_true
            total_soldQuantity += soldQuantity

            combo_key_parts = []
            for attr in attrs:
                name = attr.get("name")
                value = attr.get("value_name")
                if not name or not value:
                    continue
                attr_stat[name][value]["soldQuantity"] += soldQuantity
                attr_stat[name][value]["sold_true"] += sold_true
                attr_stat[name][value]["picture_ids"].extend(picture_ids)
                combo_key_parts.append(f"{name}:{value}")

            if combo_key_parts:
                combo_key = "|".join(combo_key_parts)
                combo_stat[combo_key]["soldQuantity"] += soldQuantity
                combo_stat[combo_key]["sold_true"] += sold_true
                combo_stat[combo_key]["picture_ids"].extend(picture_ids)

        # ---------------- 生成 variationInfo ----------------
        variations3 = []
        combo_list = []
        for combo_key, data in combo_stat.items():
            sold_true_ratio = round(data["sold_true"]/total_sold_true,4) if total_sold_true else 0
            soldQuantity_ratio = round(data["soldQuantity"]/total_soldQuantity,4) if total_soldQuantity else 0
            attr_value = "/".join([part.split(":",1)[1] for part in combo_key.split("|")])
            combo_list.append({
                "attr_name":"Color/Talla",
                "attr_value":attr_value,
                "pictures": list(set(data["picture_ids"])),
                "soldQuantity": data["soldQuantity"],
                "sold_true": data["sold_true"],
                "sold_true_ratio": sold_true_ratio,
                "soldQuantity_ratio": soldQuantity_ratio
            })
        variations3.append(combo_list)

        for attr_name, values in attr_stat.items():
            cur_list = []
            for attr_value, data in values.items():
                sold_true_ratio = round(data["sold_true"]/total_sold_true,4) if total_sold_true else 0
                soldQuantity_ratio = round(data["soldQuantity"]/total_soldQuantity,4) if total_soldQuantity else 0
                cur_list.append({
                    "attr_name": attr_name,
                    "attr_value": attr_value,
                    "pictures": list(set(data["picture_ids"])),
                    "soldQuantity": data["soldQuantity"],
                    "sold_true": data["sold_true"],
                    "sold_true_ratio": sold_true_ratio,
                    "soldQuantity_ratio": soldQuantity_ratio
                })
            # 固定顺序
            if attr_name in ["Talla", "Color"]:
                variations3.append(cur_list)
        # ---------------- 写入 MongoDB ----------------
        main_ = get_collection(f"main_{site}", f"{site}", "sku")
        main_.update_one({"item_id": item_id}, {"$set": {"variations3": variations3}})
        update_task_status(task_id, 1, _id, src_col)
        return f"任务 {task_id} 处理成功"

    except Exception as e:
        update_task_status(task_id, -1, None, src_col)
        return f"任务 {task_id} 异常: {e}"


if __name__ == "__main__":
    import argparse

    # ---------------- 命令行参数 ----------------
    parser = argparse.ArgumentParser(description="Variation Task Processor")

    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        help="最大进程数 (默认: 1)"
    )

    parser.add_argument(
        "--sleep",
        type=int,
        default=2,
        help="每批任务完成后休眠秒数 (默认: 5)"
    )

    parser.add_argument(
        "--batch",
        type=int,
        default=100,
        help="每次拉取任务数量 (默认: 50)"
    )

    args = parser.parse_args()

    # ---------------- 使用参数 ----------------
    MAX_WORKERS = args.workers  # 可根据 CPU 核数调整
    SLEEP_INTERVAL = args.sleep # 每批任务处理完等待时间（秒）
    BATCH_SIZE = args.batch
    STOP_SIGNAL = False

    print("任务处理器启动...")

    try:
        while not STOP_SIGNAL:
            try:
                df_tasks = fetch_tasks(BATCH_SIZE)
            except Exception as e:
                print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] 拉取任务失败: {e}")
                time.sleep(SLEEP_INTERVAL)
                continue

            if df_tasks.empty:
                print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] 暂无任务，等待 {SLEEP_INTERVAL}s...")
                time.sleep(SLEEP_INTERVAL)
                continue

            print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] 拉取到 {len(df_tasks)} 条任务")
            results = []

            # ---------------- 多进程处理任务 ----------------
            try:
                with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
                    futures = {executor.submit(process_task, row): row["id"] for _, row in df_tasks.iterrows()}

                    for f in tqdm(as_completed(futures), total=len(futures)):
                        try:
                            res = f.result()
                            results.append(res)
                        except Exception as task_e:
                            task_id = futures.get(f)
                            print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] 任务 {task_id} 异常: {task_e}")
            except Exception as e:
                print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] 多进程执行异常: {e}")

            # 输出本批次结果
            for r in results:
                print(r)

            print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] 本批任务处理完毕，等待 {SLEEP_INTERVAL}s 再拉取下一批...")
            time.sleep(SLEEP_INTERVAL)

    except KeyboardInterrupt:
        STOP_SIGNAL = True
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] 接收到停止信号，程序退出...")

