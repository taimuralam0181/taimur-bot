const state = {
  payload: window.__INITIAL_DASHBOARD__ || null,
};

function badgeClass(value, prefix) {
  if (!value) return `${prefix}-normal`;
  const normalized = String(value).toUpperCase();
  if (normalized.includes("LIVE")) return `${prefix}-live`;
  if (normalized.includes("VIP")) return `${prefix}-vip`;
  return `${prefix}-normal`;
}

function renderPills(containerId, values) {
  const container = document.getElementById(containerId);
  if (!container) return;
  container.innerHTML = values.map((value) => `<span class="pill">${value}</span>`).join("");
}

function renderOverview(cards) {
  const grid = document.getElementById("overviewGrid");
  if (!grid) return;
  grid.innerHTML = cards
    .map(
      (card) => `
        <article class="metric-card">
          <p class="metric-label">${card.label}</p>
          <p class="metric-value">${card.value}</p>
        </article>
      `
    )
    .join("");
}

function renderSignalGrid(rows) {
  const body = document.getElementById("signalGridBody");
  if (!body) return;
  body.innerHTML = rows
    .map(
      (row) => `
        <tr>
          <td>${row.symbol}</td>
          <td>${row.interval}</td>
          <td><span class="status-badge ${row.status === "LIVE TRADE" ? "status-live" : "status-scan"}">${row.status}</span></td>
          <td><span class="tier-badge ${row.tier === "VIP" ? "tier-vip" : "tier-normal"}">${row.tier}</span></td>
          <td>${row.score}</td>
          <td>${row.entry}</td>
          <td>${row.stop_loss}</td>
          <td>${row.remaining}</td>
          <td>${row.last_checked}</td>
        </tr>
      `
    )
    .join("");
}

function renderRecentClosures(items) {
  const container = document.getElementById("recentClosures");
  if (!container) return;

  if (!items.length) {
    container.innerHTML = '<article class="recent-card"><p class="metric-label">No recent closed trades yet.</p></article>';
    return;
  }

  container.innerHTML = items
    .map(
      (item) => `
        <article class="recent-card">
          <div class="recent-top">
            <span class="recent-symbol">${item.symbol} ${item.interval}</span>
            <span class="recent-result ${Number(item.result_r) >= 0 ? "positive" : "negative"}">${item.result_r}R</span>
          </div>
          <div class="recent-meta">
            <span>${item.side}</span>
            <span>${item.tier}</span>
            <span>${item.close_reason}</span>
            <span>${item.closed_at}</span>
          </div>
        </article>
      `
    )
    .join("");
}

function renderAccuracy(containerId, items) {
  const container = document.getElementById(containerId);
  if (!container) return;

  if (!items.length) {
    container.innerHTML = '<article class="accuracy-card"><p class="metric-label">No closed-trade data yet.</p></article>';
    return;
  }

  container.innerHTML = items
    .map(
      (item) => `
        <article class="accuracy-card">
          <div class="accuracy-top">
            <span class="accuracy-label">${item.label}</span>
            <span class="recent-result ${parseFloat(item.result) >= 0 ? "positive" : "negative"}">${item.result}</span>
          </div>
          <div class="accuracy-meta">
            <span>${item.win_rate}</span>
            <span>${item.closed} closed</span>
          </div>
        </article>
      `
    )
    .join("");
}

function render(payload) {
  state.payload = payload;
  document.getElementById("modeLabel").textContent = payload.mode;
  document.getElementById("refreshTime").textContent = payload.timestamp;
  renderPills("symbolPills", payload.symbols || []);
  renderPills("intervalPills", payload.intervals || []);
  renderOverview(payload.overview_cards || []);
  renderSignalGrid(payload.signal_grid || []);
  renderRecentClosures(payload.recent_closures || []);
  renderAccuracy("accuracyPairs", (payload.accuracy && payload.accuracy.by_symbol) || []);
  renderAccuracy("accuracyIntervals", (payload.accuracy && payload.accuracy.by_interval) || []);
}

async function refreshDashboard() {
  try {
    const response = await fetch("/api/dashboard", { cache: "no-store" });
    if (!response.ok) return;
    const payload = await response.json();
    render(payload);
  } catch (error) {
    console.error("Dashboard refresh failed", error);
  }
}

render(state.payload);
setInterval(refreshDashboard, 15000);
