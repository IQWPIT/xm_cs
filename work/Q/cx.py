import base64
import json
import requests

# ========================
# 配置参数
# ========================
API_URL = "http://60.190.243.79:8001/api/search"
TOKEN = "*damaiapitoken#220818"
SITE = "ml_mx"          # ml_br / ml_cl / ml_co / ml_mx / ml_ar
CATEGORY_ID = ""  # 可填具体分类ID，也可以为空
IMAGE_PATH = "image1.jpg"  # 本地图片路径

# ========================
# 读取图片并转 base64
# ========================
with open(IMAGE_PATH, "rb") as f:
    img_bytes = f.read()
img_base64 = base64.b64encode(img_bytes).decode("utf-8")

# ========================
# 构造请求数据
# ========================
payload = {
    "token": TOKEN,
    "site": SITE,
    "category_id": CATEGORY_ID,
    "img_base64": img_base64
}

headers = {
    "Content-Type": "application/json"
}

# ========================
# 发送请求
# ========================
response = requests.post(API_URL, headers=headers, data=json.dumps(payload))

# ========================
# 输出结果
# ========================
try:
    resp_json = response.json()
    print("返回结果:")
    print(json.dumps(resp_json, indent=2, ensure_ascii=False))
except Exception as e:
    print("请求失败，原始响应:")
    print(response.text)