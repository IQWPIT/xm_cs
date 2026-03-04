import gc
import json
import random
import time
from io import BytesIO
import sys
sys.path.append("/home/debian/code/dm")

import os
os.environ["NET"] = "TUNNEL"
os.environ["NET3"] = "NXQ"

from dm.connector.mongo.manager3 import get_collection

# from amazon_us_product.chat_ai_helper import Doubao
# from amazon_us_product.promotes import promote

# 关键：在导入torch和模型之前设置环境变量
os.environ["PYTORCH_CUDA_ALLOC_CONF"] = "expandable_segments:True"

import requests
from modelscope import Qwen2_5_VLForConditionalGeneration, AutoProcessor
from qwen_vl_utils import process_vision_info
import torch
from PIL import Image
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import urllib3
import pandas as pd
from pandas import json_normalize
df_existing = pd.DataFrame()

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)  # 禁用警告

product_prompt = """
请详细描述该电商商品，按以下维度客观呈现：
- "product_name": 明确的品类. 必须采用spu_title内容，将spu_title翻译成中文后，再提炼识别出商品名
- "spu_title_chinese": 将spu_title翻译成中文
- "main_colors": 主体颜色+关键部位颜色
- "material_features": 视觉可判断的材质
- "core_features": 核心功能/设计
- "size_sense": 相对尺寸描述 （定性的描述）
- "weight_sense": 相对重量描述 （定性的描述，例如"轻便，单手可提"或"较重，需双手搬运"；）
- "size_predict": 视觉可判断的预测大约尺寸[长X宽X高]（定量描述，单位cm），必须必须必须定量描述（数值,例如"10cm*10cm*10cm"）
- "weight_predict": 视觉可判断的预测大约重量（定量描述，单位kg），必须必须必须定量描述（数值，例如"0.5kg"）
- "additional_details": 图案、文字、配件等
- "description": 结合产品特征、材质、独特细节及整体画面语境的详细叙述，如有明确的物品个数，在描述时加入数量。（例如背景、展示风格等，不包含预测的大约尺寸和重量）

仅描述图片中可见的信息，不添加猜测或主观评价。输出必须是有效的JSON字符串（全部用中文），不包含额外文本。"""

promote_doubao = """
#### 定位
- 智能助手名称 ：电商商品识别专家
- 主要任务 ：对输入的电商商品的信息进行自动识别，识别商品里面的内容，包括显示的和隐藏的信息。返回商品的json格式化信息。需要预估出商品的大约尺寸和重量。

#### 使用说明
- 输出国际单位：cm,kg,s,l,ml,A,K,W,V,Hz,kg/m^3,Pa,N,m^2,m/s
- 输入 ：一段商品的信息。
- 输出 ：以json格式化的商品信息，不要带'json'字符串。
- 格式 ：json的key必须是英文
- 单位 ：非国际单位必须转化为国际单位，比如：inches->cm, pounds->kg。
"""

# 模型本地路径
# model_name = r"E:\AI_models\Qwen\Qwen2___5-VL-3B-Instruct"
model_name = "/home/debian/bigmodels/Qwen/Qwen2___5-VL-3B-Instruct"

# 1. 内存优化：使用float16精度加载模型（减少50%内存占用）
model = Qwen2_5_VLForConditionalGeneration.from_pretrained(
    model_name,
    torch_dtype=torch.float16,  # 强制半精度，比auto更省内存
    device_map="cuda:0",
    # low_cpu_mem_usage=True,  # 加载模型时减少CPU内存峰值
    # max_memory={0: "5GiB"}  # 适配更小显存（如5GB）
)

# 2. 内存优化：限制图像token数量（核心！减少视觉特征内存）
# 降低max_pixels，减少生成的视觉token（原代码可能设置过高）
min_pixels = 128 * 28 * 28  # 进一步降低最小token数（默认256）
max_pixels = 256 * 28 * 28  # 最大token数从1024降至512（减少50%视觉特征内存）
processor = AutoProcessor.from_pretrained(
    model_name,
    torch_dtype=torch.float16,  # 强制半精度，比auto更省内存
    min_pixels=min_pixels,
    max_pixels=max_pixels,
    device_map="cuda:0",
)
# def mongo(data_dict,jsondate,n):
#     c_weight_g = get_collection(f"yingshi", "yingshi", "_weight_g")
#     print("--------1")
#     sku_id = jsondate["sku_id"]
#     pic_url = jsondate["pic_url"]
#     data_dict["sku_id"] = sku_id
#     data_dict["pic_url"] = pic_url
#     result = c_weight_g.update_one(
#         {"sku_id": sku_id},
#         {"$set.n": data_dict},  # ← 必须使用操作符
#         upsert=True
#     )
#     return result

def mongo(data_dict, jsondate, n):
    c_weight_g = get_collection("yingshi", "yingshi", "_weight_g_2")
    sku_id = jsondate.get("sku_id")
    pic_url = jsondate.get("pic_url")
    categories_cat_1 = jsondate.get("categories.cat_1")

    if not sku_id:
        print("mongo: missing sku_id, skip update")
        return None

    updates = {}

    # pic_url 去重追加
    if pic_url:
        updates.setdefault("$addToSet", {})["pic_url_list"] = pic_url

    # >>> 新增：写入 p_info_2 <<<
    p_info_2 = jsondate.get("p_info_2")
    if p_info_2 is not None:
        updates.setdefault("$set", {})["p_info_2"] = p_info_2

    categories_cat_1 = jsondate.get("categories.cat_1")
    if categories_cat_1 is not None:
        updates.setdefault("$set", {})["categories.cat_1"] = categories_cat_1

    # round_n 写入
    round_field = f"round_{n}"
    updates.setdefault("$set", {})[round_field] = data_dict

    # 更新时间
    updates["$set"]["last_updated_at"] = int(time.time())

    try:
        result = c_weight_g.update_one(
            {"sku_id": sku_id},
            updates,
            upsert=True
        )
        print(f"mongo: sku_id={sku_id} n={n} matched={result.matched_count} modified={result.modified_count} upserted_id={result.upserted_id}")
        return result
    except Exception as e:
        print(f"mongo update error for sku {sku_id}: {e}")
        return None




def deal_message(messages):
    # 处理输入
    text = processor.apply_chat_template(
        messages, tokenize=False, add_generation_prompt=True
    )
    image_inputs, _ = process_vision_info(messages)

    # 3. 内存优化：限制批次大小（仅处理单张图片）
    inputs = processor(
        text=[text],  # 单样本输入，避免批量处理
        images=image_inputs,
        padding="max_length",  # 固定长度padding，减少内存碎片化
        max_length=256,  # 限制输入文本长度
        return_tensors="pt",
    )
    inputs = inputs.to(model.device)

    # 4. 内存优化：限制生成长度，减少中间缓存
    generated_ids = model.generate(
        **inputs,
        max_new_tokens=500,
        num_beams=2,
        repetition_penalty=1.2,
        do_sample=False  # 关闭随机采样，减少内存波动
    )

    # 解码结果
    generated_ids_trimmed = [
        out_ids[len(in_ids):] for in_ids, out_ids in zip(inputs.input_ids, generated_ids)
    ]
    output_text = processor.batch_decode(
        generated_ids_trimmed,
        skip_special_tokens=True,
        clean_up_tokenization_spaces=False
    )

    clean_str = output_text[0].replace('```json', '').replace('```', '').strip()
    data_dict = json.loads(clean_str)
    print(f"输出数据{data_dict}")


    del text
    del inputs
    del generated_ids
    del generated_ids_trimmed
    del output_text

    # 清理CUDA缓存
    torch.cuda.empty_cache()
    # 重置CUDA内存池（关键：解决碎片问题）
    torch.cuda.reset_peak_memory_stats()
    torch.cuda.reset_accumulated_memory_stats()

    # 5. 清理跨进程内存和垃圾回收
    torch.cuda.ipc_collect()
    gc.collect()

    torch.cuda.synchronize()  # 确保CUDA操作完成

    return data_dict


def img2text(json_data,image_data, prior_knowledge=None, images_other=[]):
    if prior_knowledge:
        template = """关键先验知识（必须仅供参考;先验知识如果与图片识别出的商品存在严重不符，那么忽略先验知识;spu_title必须采用，识别product_name时，必须跟spu_title有强相关;可能不是中文，请自动识别语言并转换为中文，使用时保持语言的一致性）""" + json.dumps(
            prior_knowledge, ensure_ascii=False) + "\n\n\n\n\n\n"
    else:
        template = "\n"

    messages = [
        {"role": "user",
         "content": "第一轮，请记住来自先验知识，必须参考先验知识，如果和第二轮的图片识别有冲突，优先参考先验知识。" + template},
        {"role": "assistant",
         "content": "好的，记住先验知识，后续轮次的图片识别必须参考先验知识，且必须考虑，先验知识如果与图片识别出的商品存在严重不符，那么可以忽略先验知识。"},
        {
            "role": "user",
            "content": [
                {
                    "type": "image",
                    "image": image_data,
                },
                {"type": "text", "text": product_prompt},
            ],
        }
    ]

    # 只进行第一轮
    data_dict = deal_message(messages)

    data_dict_str = json.dumps(data_dict, ensure_ascii=False)

    # 生成第1轮回复
    print("模型第1轮回复：\n", data_dict_str)
    messages.append({"role": "assistant", "content": data_dict_str})

    # 加入数据库
    n = 1
    mongo(data_dict, json_data,n)

    # 第2轮：传入第2张图片（细节图），验证材质和颜色
    print("\n===== 第2轮对话 =====")
    prompt_round2 = f"""请分析以下新图片（第2张：细节图），重点验证材质，结合前两轮结果给出最终结论。""" + product_prompt

    img_url2 = images_other[random.randint(0, len(images_other) - 1)]
    image_data2 = load_image(img_url2)

    messages.append({
        "role": "user",
        "content": [
            {"type": "text", "text": prompt_round2},
            {"type": "image", "image": image_data2}
        ]
    })

    data_dict = deal_message(messages)
    data_dict_str = json.dumps(data_dict, ensure_ascii=False)


    # 加入数据库
    n = 2
    mongo(data_dict, json_data,n)

    # 生成第3轮回复（最终结论）
    print("模型第3轮回复（最终结论）：\n", data_dict_str)
    return data_dict


def load_image(image_path: str) -> Image.Image:
    """加载图像（支持本地路径和URL）"""
    if image_path.startswith(('http://', 'https://')):
        try:
            print(f"从URL加载图像: {image_path}")
            # 配置重试策略：重试3次，每次间隔0.5秒
            session = requests.Session()
            retry = Retry(
                total=3,
                backoff_factor=0.5,  # 重试间隔：0.5, 1, 2秒...
                status_forcelist=(429, 500, 502, 503, 504),  # 针对这些状态码重试
                allowed_methods=["GET"]
            )
            adapter = HTTPAdapter(max_retries=retry)
            session.mount("http://", adapter)
            session.mount("https://", adapter)

            # 禁用SSL证书验证（谨慎使用，仅在信任的服务器上）
            response = session.get(
                img_url,
                timeout=15,
                verify=False,  # 跳过SSL证书验证（解决握手超时的关键）
                headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
                }
            )
            response.raise_for_status()
            # response = requests.get(image_path, timeout=10)
            image = Image.open(BytesIO(response.content)).convert("RGB")
            return image
        except Exception as e:
            print(f"从URL加载图像失败: {e}")
            raise ValueError(f"无法加载图像: {image_path}")
    else:
        # 处理Windows路径
        image_path = os.path.abspath(image_path)
        if not os.path.exists(image_path):
            raise FileNotFoundError(f"本地图像文件不存在: {image_path}")

        try:
            print(f"从本地加载图像: {image_path}")
            image = Image.open(image_path).convert("RGB")
            return image
        except Exception as e:
            print(f"从本地加载图像失败: {e}")
            raise ValueError(f"无法加载图像: {image_path}")


if __name__ == '__main__':

    c = get_collection("main_ml_mx", "ml_mx", "sku")
    weight = get_collection("yingshi", "yingshi", "_weight_g_2")

    c_find = c.find({
        "categories.cat_1": "Hogar, Muebles y Jardín",
        "outstock":0,
        "order30d":{"$gt": 5},
        "p_info_2.weight": {"$exists": True}
    }).sort("p_info_2.weight", -1).limit(10000)

    for i in c_find:
        sku_id = i.get("sku_id")
        print(sku_id)
        if not sku_id:
            continue

        # ✅ 检查是否已经处理过
        if weight.find_one({"sku_id": sku_id}) is not None:
            print(f"SKU {sku_id} 已存在，跳过")
            continue

        json_data = i
        start = time.time()

        # 准备图片列表
        images_other = []
        try:
            for pic in json_data.get("pic_arr", []):
                pic_url = json_data["pic_template"].replace("{id}", pic)
                images_other.append(pic_url)
        except Exception as e:
            print(f"处理 pic_arr 失败: {e}")

        try:
            img_url = json_data.get("pic_url")
            if not img_url.startswith(('http://', 'https://')):
                img_url = "../cache/" + img_url

            prior_knowledge = json_data.get("attributes") or []
            spu_title = json_data.get("spu_title")
            if spu_title:
                prior_knowledge.append({"spu_title": spu_title})

            image_data = load_image(img_url)

            # 处理图片并写入 MongoDB
            result = img2text(json_data, image_data, prior_knowledge, images_other)
            print(result)

        except Exception as e:
            print(f"处理URL {img_url} 失败: {e}")

        end_time = time.time()
        print(f"处理时间: {end_time - start} 秒")
