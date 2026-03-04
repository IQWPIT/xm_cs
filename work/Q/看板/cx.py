import os
from tqdm import tqdm

os.environ["NET"] = "TUNNEL"
os.environ["NET3"] = "NXQ"
from dm.connector.mongo.manager3 import get_collection
visual_plus = get_collection("main_ml_mx", "ml_mx", "visual_plus")
cat = get_collection("main_ml_mx", "ml_mx", "cat")
a = [
"MLM1747",
"MLM189530",
"MLM1403",
"MLM1367",
"MLM1368",
"MLM1384",
"MLM1246",
"MLM1051",
"MLM1648",
"MLM1144",
"MLM1500",
"MLM1039",
"MLM1276",
"MLM1575",
"MLM1000",
"MLM186863",
"MLM1574",
"MLM1499",
"MLM1182",
"MLM3937",
"MLM1132",
"MLM3025",
"MLM1071",
"MLM1953",
"MLM44011",
"MLM1430",
"MLM187772"
]
s = 0
p = visual_plus.find({},{"cat_id":1,"cat":1})
c = []
for i in p:
    try:
        if len(i["cat"])>0:
            print(i["cat_id"])
            c.append(i["cat_id"])
            s = s + 1
    except:
        pass
print(s)
print(len(c))
print(c)
# # 1️⃣ 一次性把 cat 集合全部加载到内存
# cat_docs = cat.find({}, {"id": 1, "path": 1})
# cat_map = {}
# for doc in cat_docs:
#     if "path" in doc:
#         cat_map[doc["id"]] = doc["path"]  # path 转成列表，按顺序存储
#
# # 2️⃣ 构建父子关系
# c = {}
# cat_ids_cursor = visual_plus.find({}, {"cat_id": 1})
#
# for doc in tqdm(cat_ids_cursor, desc="Processing cat_ids"):
#     cat_id = doc["cat_id"]
#
#     if cat_id not in cat_map:
#         continue
#
#     path_values = cat_map[cat_id]
#
#     if cat_id not in path_values:
#         continue
#
#     idx = path_values.index(cat_id)
#     parent_ids = path_values[:idx]
#
#     for parent_id in parent_ids:
#         if parent_id not in c:
#             c[parent_id] = []
#         c[parent_id].append(cat_id)
#
# # 3️⃣ 输出每个父类及其子类数量
# print("父类数量:", len(c))
# sum = 0
# for parent_id, child_list in c.items():
#     if parent_id in a:
#         sum += len(child_list)
#         print(f"父类 {parent_id} 的子类数量: {len(child_list)}")
# print(sum)
