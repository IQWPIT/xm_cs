# model_patterns.py
# 扩展品牌型号正则，覆盖更多常见及小众品牌

model_patterns = [
    # Motorola
    r"Moto\s+[A-Za-z0-9]+",
    r"Edge\s+[0-9]+(?:\s+[A-Za-z]+)?",

    # Samsung
    r"(?:Samsung\s+)?Galaxy\s+[A-Za-z0-9]+(?:\s+[A-Za-z0-9]+)?",

    # Xiaomi / Redmi / Poco / Mi
    r"(?:Redmi|Xiaomi)\s+[A-Za-z0-9]+(?:\s+[A-Za-z0-9]+)?",
    r"(?:Poco|POCO)\s+[A-Za-z0-9]+(?:\s+[A-Za-z0-9]+)?",
    r"Mi\s+[0-9]+(?:\s+[A-Za-z]+)?",

    # Apple
    r"iPhone\s+[0-9A-Za-z]+(?:\s+[A-Za-z]+)?",

    # Realme / Infinix / OPPO / Vivo / Huawei / Honor
    r"(?:Realme|Infinix|OPPO|Oppo|Vivo|vivo|Huawei|Honor)\s+[A-Za-z0-9]+(?:\s+[A-Za-z0-9]+)?",

    # Tecno / LG / Nokia / Sony / TCL / Philco / Asus / ZenFone
    r"(?:Tecno|LG|Nokia|Sony|TCL|Philco|Asus|ZenFone)\s+[A-Za-z0-9]+",
    r"Xperia\s+[0-9]+(?:\s+[A-Za-z]+)?",  # Sony

    # 其他品牌补充
    r"(?:Lenovo|Alcatel|Sharp|Meizu|Coolpad|Doogee|Itel|Kingkong|Multilaser|Oukitel|UMIDIGI|ZTE|BLU|BQ|Ulefone|Cubot|Blackview)\s+[A-Za-z0-9]+",
    r"(?:OnePlus|Oppo\s+Realme|Vsmart|Fairphone|Gionee|Inoi|Micromax|Panasonic|Honor)\s+[A-Za-z0-9]+",

    # 虚拟/山寨型号
    r"(i[0-9]+\s+Pro(?:\s+Max)?)",
    r"(GT[0-9]+\s+Pro)",
    r"(S[0-9]+\s+Ultra)",
    r"(Note[0-9]+\s+[A-Za-z]+)",

    # 通用型号模式（最后匹配）
    r"[A-Z][a-z]*\s+[0-9]+[A-Za-z]*(?:\s+[A-Za-z]+)?",  # Note 14 Pro
    r"[A-Z][0-9]+\s+[A-Za-z]+",  # A17 Pro
    r"[A-Za-z]+\s+[0-9]+" , # Edge 30
    
    r"Galaxy|S2[34]|S2[56]|S2[789] Ultra|",
    r"i1[23456789]|i1[0-7] Pro Max|i1[0-7]promax|i[1-9]\d* Pro Max|i\d+ Pro Max|i\d+promax|",
    r"Redmi|Xioami|Xiaomi|POVA|Pova\d+|",
    r"Motorola|Moto|GT\d+|M\d+|",
    r"Philco|TCL|Itel|Sony|Nokia|UMIDIGI|Oukitel|Blackview|Vovo|Vovofone|Cubot|Rino|Doogee|",
    r"Multilaser|Blu|Asus|Pixel|Nothing|Reno|ZenFone|POVA|XS\d+|M\d+|A\d+|",
    r"S\d+ Ultra|S\d+ Pro|S\d+ U|S\d+ Super|S\d+ Mini|S\d+ UItra",
    r"\b(UMIDIGI)\s+(Note\s*\d+[A-Z]?)\b",
]
