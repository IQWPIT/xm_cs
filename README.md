# xm_cs（无 MongoDB 版本数据看板）

当前版本基于 **Flask + 本地测试数据**，不依赖 MongoDB，可直接跑起来看效果。

## 功能概览

- 页面：`GET /`
- 看板数据 API：`GET /api/dashboard`
- 获取原始测试数据：`GET /api/test-data`
- 注入实时数据（进程内存）：`POST /api/ingest`
- 重置回测试数据：`POST /api/reset`

> 说明：
> - 现在还没有数据库，因此默认使用 `data/mock_dashboard.json` 的测试数据。
> - `POST /api/ingest` 写入的是**内存数据**，仅当前 Python 进程有效，重启后失效。

---

## 1. 目录结构

```text
.
├── app.py
├── requirements.txt
├── data/
│   └── mock_dashboard.json
├── templates/
│   └── index.html
├── static/
│   ├── styles.css
│   └── script.js
└── README.md
```

## 2. 启动方式

### 2.1 安装依赖

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2.2 启动服务

```bash
python3 app.py
```

默认端口 `8080`，访问：

- 页面：`http://localhost:8080/`
- 看板 API：`http://localhost:8080/api/dashboard`

---

## 3. 测试数据说明

测试数据文件：`data/mock_dashboard.json`，包含：

- `kpis`（4 个核心指标）
- `dailyOrders`（近 7 天订单）
- `channelConversion`（渠道转化率）
- `regionSales`（区域销售排行）

你可以直接修改此文件来调整页面展示内容。

---

## 4. API 说明

### 4.1 获取看板数据

`GET /api/dashboard`

- 若未注入实时数据：返回测试数据（`isMock: true`）
- 若已调用 `/api/ingest`：返回内存实时数据（`isMock: false`）

### 4.2 获取测试数据原文

`GET /api/test-data`

用于前端联调、对照结构。

### 4.3 注入实时数据（内存）

`POST /api/ingest`

请求示例：

```bash
curl -X POST http://localhost:8080/api/ingest \
  -H 'Content-Type: application/json' \
  -d '{
    "kpis": [
      {"label":"本周订单","value":"3,120","delta":0.10},
      {"label":"销售额","value":"¥4,510,000","delta":0.09},
      {"label":"平均客单价","value":"¥1,445","delta":-0.02},
      {"label":"转化率","value":"19.1%","delta":0.03}
    ],
    "dailyOrders": [
      {"day":"3/1","value":410},
      {"day":"3/2","value":438}
    ],
    "channelConversion": [
      {"name":"官网","value":0.23},
      {"name":"小程序","value":0.19}
    ],
    "regionSales": [
      {"name":"华东","amount":980000,"ratio":0.08},
      {"name":"华南","amount":860000,"ratio":0.03}
    ]
  }'
```

### 4.4 重置为测试数据

`POST /api/reset`

```bash
curl -X POST http://localhost:8080/api/reset
```

---

## 5. 性能与可维护性（当前版本）

- 测试数据读取使用 `lru_cache` 缓存，避免重复磁盘 IO。
- 前端只请求一个主接口 `/api/dashboard`，数据结构固定，渲染开销低。
- 代码已预留 API 结构，后续接入 MySQL / MongoDB / ES 时前端可基本无缝复用。
