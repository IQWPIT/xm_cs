import os

os.environ["NET"] = "TUNNEL"
os.environ["NET3"] = "NXQ"

import streamlit as st
import pandas as pd
from dm.connector.mongo.manager3 import get_collection
import plotly.graph_objects as go

st.set_page_config(page_title="品牌 & 价格段分析", layout="wide")
st.title("手机销量分析（Top10品牌 & 价格段按月份 + 最新月分析）")

# ================== MongoDB ==================
dst_col = get_collection("ml", "customize_common", "vivo_amazon_parsed_2")

price_ranges_labels = [
    "0-1000", "1000-1500", "1500-2000", "2000-2500", "2500-3000",
    "3000-4000", "4000-5000", "5000-6000", "6000-8000", "8000-10000", "10000+"
]

display_type = st.radio("显示类型", ["订单量", "GMV"], horizontal=True)


# ================== 数据缓存 ==================
@st.cache_data(ttl=300)
def load_data(display_type):
    sku_docs = list(dst_col.find({}, {"brand": 1, "bInfo": 1}))

    all_months = set()
    brand_set = set()
    for doc in sku_docs:
        all_months.update(doc.get("bInfo", {}).keys())
        brand_set.add(doc.get("brand", "other"))
    all_months = sorted(list(all_months))

    recent_months = all_months[-8:]
    latest_month = all_months[-1]

    # ================== 处理 Top7 + other ==================
    brand_total = {}
    for doc in sku_docs:
        brand = doc.get("brand", "other")
        binfo = doc.get("bInfo", {})
        for month in recent_months:
            if month in binfo:
                pt = binfo[month]
                for p in pt:
                    brand_total[brand] = brand_total.get(brand, 0) + (p["od"] if display_type=="订单量" else p["gmv"])

    # 取 Top7
    top7_brands = sorted(brand_total, key=lambda x: brand_total[x], reverse=True)[:7]
    all_brands = top7_brands + ["other"]

    # 初始化 DataFrame
    brand_summary = pd.DataFrame(0, index=pd.MultiIndex.from_product([all_brands, recent_months], names=["品牌", "月份"]),
                                 columns=["value"])
    price_summary = pd.DataFrame(0, index=pd.MultiIndex.from_product([price_ranges_labels, recent_months],
                                                                    names=["价格区间", "月份"]),
                                 columns=["value"])
    latest_brand_summary = pd.DataFrame(0, index=all_brands, columns=["value"])
    latest_price_summary = pd.DataFrame(0, index=price_ranges_labels, columns=["value"])
    latest_price_brand_summary = pd.DataFrame(0, index=pd.MultiIndex.from_product([price_ranges_labels, all_brands],
                                                                                  names=["价格区间", "品牌"]),
                                              columns=["value"])

    for doc in sku_docs:
        brand = doc.get("brand", "other")
        if brand not in top7_brands:
            brand = "other"
        binfo = doc.get("bInfo", {})
        for month in recent_months:
            if month in binfo:
                pt = binfo[month]
                for idx, p in enumerate(pt):
                    value = p["od"] if display_type == "订单量" else p["gmv"]
                    brand_summary.loc[(brand, month), "value"] += value
                    price_summary.loc[(price_ranges_labels[idx], month), "value"] += value
                    if month == latest_month:
                        latest_brand_summary.loc[brand, "value"] += value
                        latest_price_summary.loc[price_ranges_labels[idx], "value"] += value
                        latest_price_brand_summary.loc[(price_ranges_labels[idx], brand), "value"] += value

    return brand_summary, price_summary, recent_months, all_brands, latest_month, latest_brand_summary, latest_price_summary, latest_price_brand_summary

top10_brand_summary, price_summary, recent_months, top10_brands, latest_month, latest_brand_summary, latest_price_summary, latest_price_brand_summary = load_data(
    display_type)

colors = [
    "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd",
    "#8c564b", "#e377c2", "#7f7f7f", "#bcbd22", "#17becf", "#aec7e8"
]

# ================== Top10品牌按月份堆叠柱状图 ==================
st.subheader("Top10 品牌按月份堆叠柱状图")
fig1 = go.Figure()
for i, brand in enumerate(top10_brands):
    fig1.add_trace(go.Bar(
        x=recent_months,
        y=[top10_brand_summary.loc[(brand, month), "value"] for month in recent_months],
        name=brand,
        marker_color=colors[i % len(colors)],
        text=[top10_brand_summary.loc[(brand, month), "value"] for month in recent_months],
        textposition='inside',
        insidetextanchor='middle',
        width=0.4
    ))
fig1.update_layout(barmode='stack', xaxis_title="月份", yaxis_title=display_type,
                   legend_title="品牌", template="plotly_white", height=700,
                   margin=dict(l=60, r=20, t=80, b=80))
st.plotly_chart(fig1, use_container_width=True)

# ================== 价格段按月份堆叠柱状图 ==================
st.subheader("价格段按月份堆叠柱状图（跨品牌）")
fig2 = go.Figure()
for i, price_range in enumerate(price_ranges_labels):
    fig2.add_trace(go.Bar(
        x=recent_months,
        y=[price_summary.loc[(price_range, month), "value"] for month in recent_months],
        name=price_range,
        marker_color=colors[i % len(colors)],
        text=[price_summary.loc[(price_range, month), "value"] for month in recent_months],
        textposition='inside',
        insidetextanchor='middle',
        width=0.4
    ))
fig2.update_layout(barmode='stack', xaxis_title="月份", yaxis_title=display_type,
                   legend_title="价格区间", template="plotly_white", height=700,
                   margin=dict(l=60, r=20, t=80, b=80))
st.plotly_chart(fig2, use_container_width=True)
# ================== 最新月各品牌销量图（统一颜色） ==================
st.subheader(f"最新月 ({latest_month}) 各品牌手机销量")
fig3 = go.Figure()
bar_color = "#1f77b4"  # 统一颜色
sorted_brands = latest_brand_summary.sort_values("value", ascending=False).index.tolist()
fig3.add_trace(go.Bar(
    x=sorted_brands,
    y=[latest_brand_summary.loc[brand, "value"] for brand in sorted_brands],
    name="品牌销量",
    marker_color=bar_color,
    text=[latest_brand_summary.loc[brand, "value"] for brand in sorted_brands],
    textposition='inside',
    insidetextanchor='middle',
    width=0.5
))
fig3.update_layout(barmode='stack', xaxis_title="品牌", yaxis_title=display_type,
                   showlegend=False, template="plotly_white", height=600)
st.plotly_chart(fig3, use_container_width=True)

# ================== 最新月各价格段销量图（统一颜色） ==================
st.subheader(f"最新月 ({latest_month}) 各价格段手机销量")
fig4 = go.Figure()
fig4.add_trace(go.Bar(
    x=price_ranges_labels,
    y=[latest_price_summary.loc[price_range, "value"] for price_range in price_ranges_labels],
    name="价格段销量",
    marker_color=bar_color,
    text=[latest_price_summary.loc[price_range, "value"] for price_range in price_ranges_labels],
    textposition='inside',
    insidetextanchor='middle',
    width=0.5
))
fig4.update_layout(barmode='stack', xaxis_title="价格段", yaxis_title=display_type,
                   showlegend=False, template="plotly_white", height=600)
st.plotly_chart(fig4, use_container_width=True)

# ================== 最新月价格段-Top10品牌堆叠柱状图 ==================
st.subheader(f"最新月 ({latest_month}) 价格段销量分布堆叠图（Top10品牌）")
fig5 = go.Figure()
for i, brand in enumerate(top10_brands):
    fig5.add_trace(go.Bar(
        x=price_ranges_labels,
        y=[latest_price_brand_summary.loc[(price_range, brand), "value"] for price_range in price_ranges_labels],
        name=brand,
        marker_color=colors[i % len(colors)],
        text=[latest_price_brand_summary.loc[(price_range, brand), "value"] for price_range in price_ranges_labels],
        textposition='inside',
        insidetextanchor='middle',
        width=0.4
    ))
fig5.update_layout(barmode='stack', xaxis_title="价格段", yaxis_title=display_type,
                   legend_title="品牌", template="plotly_white", height=700,
                   margin=dict(l=60, r=20, t=80, b=80))
st.plotly_chart(fig5, use_container_width=True)