const sourceData = window.__DASHBOARD_DATA__;

function makeMockData() {
  const channels = ["官网", "小程序", "电商平台", "线下门店", "代理商"];
  const regions = ["华东", "华南", "华北", "华中", "西南", "西北"];

  const dailyOrders = Array.from({ length: 7 }, (_, idx) => {
    const date = new Date();
    date.setDate(date.getDate() - (6 - idx));
    return {
      day: `${date.getMonth() + 1}/${date.getDate()}`,
      value: Math.floor(160 + Math.random() * 210)
    };
  });

  const channelConversion = channels.map((name) => ({
    name,
    value: Number((0.06 + Math.random() * 0.23).toFixed(3))
  }));

  const regionSales = regions
    .map((name) => ({
      name,
      amount: Math.floor(200000 + Math.random() * 900000),
      ratio: Number((Math.random() * 0.28 - 0.08).toFixed(3))
    }))
    .sort((a, b) => b.amount - a.amount);

  const totalOrders = dailyOrders.reduce((sum, d) => sum + d.value, 0);
  const totalRevenue = regionSales.reduce((sum, r) => sum + r.amount, 0);

  return {
    isMock: true,
    updatedAt: new Date().toLocaleString("zh-CN"),
    kpis: [
      { label: "本周订单", value: totalOrders.toLocaleString(), delta: 0.124 },
      { label: "销售额", value: `¥${totalRevenue.toLocaleString()}`, delta: 0.086 },
      { label: "平均客单价", value: `¥${Math.round(totalRevenue / totalOrders)}`, delta: -0.018 },
      { label: "转化率", value: `${(channelConversion.reduce((s, c) => s + c.value, 0) / channelConversion.length * 100).toFixed(1)}%`, delta: 0.031 }
    ],
    dailyOrders,
    channelConversion,
    regionSales
  };
}

function normalizeData(input) {
  if (!input || !Array.isArray(input.dailyOrders) || input.dailyOrders.length === 0) {
    return makeMockData();
  }
  return { ...input, isMock: false };
}

function renderKpis(kpis) {
  const grid = document.getElementById("kpi-grid");
  grid.innerHTML = "";
  kpis.forEach((item) => {
    const div = document.createElement("div");
    div.className = "kpi-card";
    const trendClass = item.delta >= 0 ? "up" : "down";
    const arrow = item.delta >= 0 ? "▲" : "▼";

    div.innerHTML = `
      <div class="label">${item.label}</div>
      <div class="value">${item.value}</div>
      <div class="trend ${trendClass}">${arrow} ${(Math.abs(item.delta) * 100).toFixed(1)}%</div>
    `;
    grid.appendChild(div);
  });
}

function drawTrendChart(data) {
  const canvas = document.getElementById("trend-chart");
  const ctx = canvas.getContext("2d");
  const points = data.map((d) => d.value);
  const labels = data.map((d) => d.day);

  const width = canvas.width;
  const height = canvas.height;
  const padding = 36;

  const min = Math.min(...points);
  const max = Math.max(...points);
  const span = Math.max(max - min, 1);

  ctx.clearRect(0, 0, width, height);

  const grad = ctx.createLinearGradient(0, 0, 0, height);
  grad.addColorStop(0, "rgba(94, 216, 255, 0.32)");
  grad.addColorStop(1, "rgba(94, 216, 255, 0)");

  const xStep = (width - padding * 2) / (points.length - 1);

  const pointXY = points.map((value, i) => {
    const x = padding + xStep * i;
    const y = height - padding - ((value - min) / span) * (height - padding * 2);
    return { x, y, value };
  });

  ctx.strokeStyle = "rgba(114, 228, 255, 0.28)";
  ctx.lineWidth = 1;
  for (let i = 0; i < 4; i++) {
    const y = padding + ((height - padding * 2) / 3) * i;
    ctx.beginPath();
    ctx.moveTo(padding, y);
    ctx.lineTo(width - padding, y);
    ctx.stroke();
  }

  ctx.beginPath();
  ctx.moveTo(pointXY[0].x, height - padding);
  pointXY.forEach((p) => ctx.lineTo(p.x, p.y));
  ctx.lineTo(pointXY[pointXY.length - 1].x, height - padding);
  ctx.closePath();
  ctx.fillStyle = grad;
  ctx.fill();

  ctx.beginPath();
  pointXY.forEach((p, idx) => {
    if (idx === 0) ctx.moveTo(p.x, p.y);
    else ctx.lineTo(p.x, p.y);
  });
  ctx.strokeStyle = "#76efff";
  ctx.lineWidth = 2.5;
  ctx.stroke();

  ctx.fillStyle = "#c8f8ff";
  pointXY.forEach((p) => {
    ctx.beginPath();
    ctx.arc(p.x, p.y, 3.5, 0, Math.PI * 2);
    ctx.fill();
  });

  ctx.fillStyle = "#95a4d6";
  ctx.font = "12px sans-serif";
  labels.forEach((label, idx) => {
    const x = padding + xStep * idx;
    ctx.fillText(label, x - 11, height - 12);
  });
}

function renderChannelList(items) {
  const list = document.getElementById("channel-list");
  list.innerHTML = "";
  items.sort((a, b) => b.value - a.value).forEach((item) => {
    const li = document.createElement("li");
    const percent = (item.value * 100).toFixed(1);
    li.innerHTML = `
      <div class="row"><span>${item.name}</span><strong>${percent}%</strong></div>
      <div class="progress"><span style="width:${percent}%"></span></div>
    `;
    list.appendChild(li);
  });
}

function renderRegionTable(items) {
  const tbody = document.getElementById("region-table");
  tbody.innerHTML = "";
  items.forEach((item, idx) => {
    const tr = document.createElement("tr");
    const cls = item.ratio >= 0 ? "up" : "down";
    const text = `${item.ratio >= 0 ? "+" : ""}${(item.ratio * 100).toFixed(1)}%`;
    tr.innerHTML = `
      <td>${idx + 1}</td>
      <td>${item.name}</td>
      <td>¥${item.amount.toLocaleString()}</td>
      <td class="${cls}">${text}</td>
    `;
    tbody.appendChild(tr);
  });
}

function initDashboard() {
  const data = normalizeData(sourceData);
  document.getElementById("updated-time").textContent = `更新时间：${data.updatedAt ?? new Date().toLocaleString("zh-CN")}`;
  document.getElementById("data-status").textContent = data.isMock
    ? "当前无线上数据，已自动填充测试数据进行展示。"
    : "当前展示线上实时数据。";

  renderKpis(data.kpis);
  drawTrendChart(data.dailyOrders);
  renderChannelList(data.channelConversion);
  renderRegionTable(data.regionSales);
}

initDashboard();
