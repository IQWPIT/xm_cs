"""使用机器学习对电商商品销量进行估计。

输入数据建议是多个 ASIN、多个时间快照，而不是单条记录。
本脚本会把每个 ASIN 的时间序列转换为监督学习样本：
- 特征 X: t 时刻的价格、折扣、评分、评论数、库存状态等
- 标签 y: t+1 时刻的 sales_30d_est
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Tuple

import numpy as np
from sklearn.ensemble import RandomForestRegressor
from sklearn.feature_extraction import DictVectorizer
from sklearn.metrics import mean_absolute_error, mean_squared_error
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline


@dataclass
class TrainingResult:
    model: Pipeline
    mae: float
    rmse: float
    n_samples: int


def _safe_get(mapping: Dict[str, Any], key: str, date: str, default: Any = None) -> Any:
    value = mapping.get(key, {})
    if isinstance(value, dict):
        return value.get(date, default)
    return default


def _stock_to_flag(stock_text: str | None) -> int:
    if not stock_text:
        return 0
    text = stock_text.strip().lower()
    return 0 if ("sem estoque" in text or "indispon" in text) else 1


def build_training_samples(records: Iterable[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[float]]:
    """从历史抓取数据构建监督学习样本。"""
    X: List[Dict[str, Any]] = []
    y: List[float] = []

    for r in records:
        order = r.get("order", {})
        if not isinstance(order, dict) or len(order) < 2:
            continue

        dates = sorted(order.keys())
        original_price = float(r.get("original_price") or 0.0)
        brand = r.get("brand") or "UNKNOWN"

        for i in range(len(dates) - 1):
            d_now, d_next = dates[i], dates[i + 1]
            now = order.get(d_now, {})
            nxt = order.get(d_next, {})

            price_now = float(now.get("price") or 0.0)
            price_next = float(nxt.get("price") or 0.0)
            sales_next = nxt.get("sales_30d_est")
            if sales_next is None:
                continue

            rating_count = _safe_get(r, "rating_count", d_now, 0) or 0
            rating_score = _safe_get(r, "rating_score", d_now, 0.0) or 0.0
            stock_text = _safe_get(r, "stock_text", d_now, "")

            discount_rate = 0.0
            if original_price > 0 and price_now > 0:
                discount_rate = 1.0 - (price_now / original_price)

            price_change = 0.0
            if price_now > 0 and price_next > 0:
                price_change = (price_next - price_now) / price_now

            X.append(
                {
                    "brand": brand,
                    "price": price_now,
                    "original_price": original_price,
                    "discount_rate": discount_rate,
                    "rating_count": float(rating_count),
                    "rating_score": float(rating_score),
                    "in_stock": _stock_to_flag(stock_text),
                    "price_change_next_step": price_change,
                }
            )
            y.append(float(sales_next))

    return X, y


def train_sales_estimator(records: Iterable[Dict[str, Any]], random_state: int = 42) -> TrainingResult:
    """训练销量估计模型（随机森林回归）。"""
    X, y = build_training_samples(records)
    if len(X) < 20:
        raise ValueError("样本太少：至少需要 20 条时间步样本，建议 500+。")

    model = Pipeline(
        steps=[
            ("dv", DictVectorizer(sparse=False)),
            (
                "rf",
                RandomForestRegressor(
                    n_estimators=300,
                    max_depth=12,
                    min_samples_leaf=3,
                    random_state=random_state,
                    n_jobs=-1,
                ),
            ),
        ]
    )

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=random_state)
    model.fit(X_train, y_train)
    pred = model.predict(X_test)

    mae = mean_absolute_error(y_test, pred)
    rmse = float(np.sqrt(mean_squared_error(y_test, pred)))
    return TrainingResult(model=model, mae=float(mae), rmse=rmse, n_samples=len(X))


def explain_single_record_limit() -> str:
    return (
        "你给的是单条商品记录，只能做规则估计，不能训练可靠的机器学习模型。"
        "至少要准备大量商品、多天快照数据，才能训练并泛化。"
    )


if __name__ == "__main__":
    print(explain_single_record_limit())
