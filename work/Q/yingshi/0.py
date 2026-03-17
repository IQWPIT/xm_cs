import os
os.environ["NET"] = "TUNNEL"
os.environ["NET3"] = "NXQ"

from dm.connector.mongo.manager3 import get_collection

m = get_collection("yingshi","yingshi","ml_mx_monthly_sku")
_ml_br_l = get_collection("yingshi","yingshi","_ml_mx_l")

# 1. 一次性取出所有商品ID 与 月销量
cursor = list(_ml_br_l.find({}, {'_id':0, '商品ID': 1, '202510月销量':1}))


# 2. 提前把 ml_br_monthly_sku 的 monthlyorder 做成字典（批量查询更快）
sku_ids = [x["商品ID"] for x in cursor]

# 批量查 sku_id
docs = m.find(
    {
        "$or": [
            {"sku_id": {"$in": sku_ids}},
            {"item_id": {"$in": sku_ids}}
        ]
    },
    {'_id': 0, 'sku_id': 1, 'item_id': 1, 'monthlyorder': 1}
)

# 做成字典，加速查询 O(1)
monthlyorder_map = {d["sku_id"]: d.get("monthlyorder") for d in docs}

# 3. 聚合后输出
n = 0
sum = 0
sum_ = 0
sum_l = 0
d = 0
d2 = 0
for row in cursor:
    ID = row["商品ID"]
    _202510 = row.get("202510月销量")
    monthly_order = monthlyorder_map.get(ID)
    try:
        if monthly_order is None :
            monthly_order = 0
        sum_ = sum_ + _202510
        sum_l = sum_l+monthly_order
        if int (_202510)-int(monthly_order) > 20 or int (_202510)-int(monthly_order) < -20 :
            sum += int(_202510) - int(monthly_order)
            n = n + 1
        if  monthly_order!=0 and (int(_202510) - int(monthly_order) > 150 or int(_202510) - int(monthly_order) < -150):
            d += int(_202510) - int(monthly_order)
            print([ID, monthly_order, _202510],d)
        if  monthly_order==0 and (int(_202510) - int(monthly_order) > 200 or int(_202510) - int(monthly_order) < -200):
            d2 += int(_202510) - int(monthly_order)
            print([ID, monthly_order, _202510],d2)
    except:
        pass
    # print([ID,monthly_order, _202510])
print(n,sum,sum_,sum_l,d,d2)

