import os
from collections import defaultdict
import pymysql
import pandas as pd
from pymongo import MongoClient

# --------------------------------------------------------------------------------------------------
# 环境变量
# --------------------------------------------------------------------------------------------------
os.environ["NET"] = "TUNNEL"
os.environ["NET3"] = "NXQ"

# --------------------------------------------------------------------------------------------------
# MySQL 连接函数
# --------------------------------------------------------------------------------------------------
def get_mysql_conn():
    return pymysql.connect(
        host="114.132.88.73",
        user="root",
        password="damaitoken@2026#",
        database="DM_spiders",
        port=3306,
        charset="utf8mb4"
    )

def update_task_status(task_id, status):
    """更新 MySQL 任务状态"""
    try:
        conn = get_mysql_conn()
        with conn.cursor() as cursor:
            sql = "UPDATE lj_variation_task SET status=%s WHERE id=%s"
            cursor.execute(sql, (status, task_id))
        conn.commit()
    except Exception as e:
        print(f"MySQL 更新失败 task_id={task_id} status={status} 错误：{e}")
    finally:
        conn.close()

# --------------------------------------------------------------------------------------------------
# MySQL 拉取任务
# --------------------------------------------------------------------------------------------------
# conn = get_mysql_conn()
# sql = """
# SELECT
#     id,
#     item_id,
#     site_id,
#     state,
#     status,
#     create_time,
#     update_time
# FROM lj_variation_task
# WHERE state = 1
# ORDER BY update_time DESC
# """

conn = get_mysql_conn()
sql = """
SELECT id, item_id, site_id, state, status
FROM lj_variation_task
WHERE state = 1 
ORDER BY update_time DESC
"""
df = pd.read_sql(sql, conn)
conn.close()

print(df)
print(f"MySQL 读取任务数：{len(df)}")