# xm_cs（Python + MongoDB 数据看板）

这是一个基于 **Flask + MongoDB** 的运营数据看板示例，支持：

- Python 后端渲染页面（`/`）。
- MongoDB 数据读取与聚合（`/api/dashboard`）。
- 无真实数据时自动返回测试数据（兜底可视化）。
- 提供数据写入 API，方便联调与压测。

## 1. 目录结构

```text
.
├── app.py
├── requirements.txt
├── templates/
│   └── index.html
├── static/
│   ├── styles.css
│   └── script.js
└── README.md
```

## 2. 环境准备

### 2.1 安装依赖

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2.2 准备 MongoDB

本地默认连接：`mongodb://127.0.0.1:27017`。

你也可以通过环境变量覆盖：

```bash
export MONGODB_URI="mongodb://127.0.0.1:27017"
export MONGODB_DB="dashboard"
```

## 3. 启动项目

```bash
python3 app.py
```

默认监听 `0.0.0.0:8080`，浏览器打开：

- 页面：`http://localhost:8080/`
- 看板 API：`http://localhost:8080/api/dashboard`

## 4. API 说明

### 4.1 获取看板数据

`GET /api/dashboard`

- 优先从 MongoDB 读取真实数据。
- 如果数据不足（如近 7 天订单不足、渠道/区域为空），自动回退为测试数据。

返回示例：

```json
{
  "isMock": false,
  "updatedAt": "2026-03-02 10:00:00",
  "kpis": [
    {"label": "本周订单", "value": "2,103", "delta": 0.052}
  ],
  "dailyOrders": [{"day": "3/1", "value": 301}],
  "channelConversion": [{"name": "官网", "value": 0.182}],
  "regionSales": [{"name": "华东", "amount": 820000, "ratio": 0.06}]
}
```

### 4.2 一键写入测试数据

`POST /api/seed-test-data`

用途：快速填充 MongoDB，验证 UI 与接口。

```bash
curl -X POST http://localhost:8080/api/seed-test-data
```

### 4.3 写入业务数据（联调用）

`POST /api/ingest`

请求体需包含：`dailyOrders`、`channelConversion`、`regionSales`。

```bash
curl -X POST http://localhost:8080/api/ingest \
  -H 'Content-Type: application/json' \
  -d '{
    "dailyOrders": [{"day":"3/1","value":320}],
    "channelConversion": [{"name":"官网","value":0.21}],
    "regionSales": [{"name":"华东","amount":900000,"ratio":0.08}]
  }'
```

## 5. 性能设计说明（已落地）

- `MongoClient` 使用连接池参数（`maxPoolSize`、`minPoolSize`），减少高并发下频繁建连开销。
- 为高频查询字段创建索引：
  - `metrics_daily.day`
  - `channels.updatedAt`
  - `regions.amount`
- 看板查询只取必要字段（投影 `_id: 0`），降低网络与序列化成本。
- 趋势、排名等计算在后端一次成型，前端仅渲染，减少浏览器负担。

## 6. 前端说明

前端页面使用现代深色风格（渐变 + 玻璃拟态），并通过 `/api/dashboard` 拉取数据：

- 实时显示“当前展示 MongoDB 线上数据”或“已自动填充测试数据”。
- 包含 KPI、趋势图、渠道转化率、区域销售排行。
- 移动端自动响应式布局。

## 7. 常见问题

1. **页面显示“加载失败”**
   - 检查后端是否启动。
   - 检查 MongoDB 连接是否可达。

2. **一直显示测试数据**
   - 说明真实数据不足；可先调用 `/api/seed-test-data`。

3. **如何改端口**
   - 启动前设置：`export PORT=9000`
