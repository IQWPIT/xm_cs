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

  const xStep = (width - padding * 2) / Math.max(points.length - 1, 1);
  const pointXY = points.map((value, i) => {
    const x = padding + xStep * i;
    const y = height - padding - ((value - min) / span) * (height - padding * 2);
    return { x, y };
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
  pointXY.forEach((p, idx) => (idx === 0 ? ctx.moveTo(p.x, p.y) : ctx.lineTo(p.x, p.y)));
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
  [...items].sort((a, b) => b.value - a.value).forEach((item) => {
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
      <td>¥${Number(item.amount).toLocaleString()}</td>
      <td class="${cls}">${text}</td>
    `;
    tbody.appendChild(tr);
  });
}

function validateData(data) {
  return data && Array.isArray(data.kpis) && Array.isArray(data.dailyOrders) && Array.isArray(data.channelConversion) && Array.isArray(data.regionSales);
}

async function initDashboard() {
  try {
    const res = await fetch("/api/dashboard", { cache: "no-store" });
    const data = await res.json();
    if (!validateData(data)) {
      throw new Error("API返回结构不正确");
    }

    document.getElementById("updated-time").textContent = `更新时间：${data.updatedAt || new Date().toLocaleString("zh-CN")}`;
    document.getElementById("data-status").textContent = data.isMock
      ? "当前无线上数据，已自动填充测试数据进行展示。"
      : "当前展示 MongoDB 线上数据。";

    renderKpis(data.kpis);
    drawTrendChart(data.dailyOrders);
    renderChannelList(data.channelConversion);
    renderRegionTable(data.regionSales);
  } catch (error) {
    document.getElementById("data-status").textContent = `加载失败：${error.message}`;
  }
}

initDashboard();
