const state = {
  payload: window.__INITIAL_DASHBOARD__ || null,
  autoRefresh: true,
  lastSignalSignature: "",
  refreshTimer: null,
};

function renderPills(selectId, values, selected) {
  const select = document.getElementById(selectId);
  if (!select) return;
  select.innerHTML = values
    .map((value) => `<option value="${value}" ${value === selected ? "selected" : ""}>${value}</option>`)
    .join("");
}

function renderTicker(rows) {
  const strip = document.getElementById("tickerStrip");
  strip.innerHTML = rows
    .map(
      (row) => `
        <article class="ticker-card">
          <div class="ticker-top">
            <span class="ticker-symbol">${row.symbol}</span>
            <span class="${row.direction}">${row.change_pct}</span>
          </div>
          <div class="ticker-bottom">
            <strong>${row.price}</strong>
            <span class="flat">${row.trend}</span>
          </div>
        </article>
      `
    )
    .join("");
}

function renderOverview(cards) {
  const grid = document.getElementById("overviewGrid");
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

function renderLatestSignal(signal) {
  const box = document.getElementById("latestSignalBox");
  if (!signal || !signal.symbol) {
    box.className = "signal-card empty-card";
    box.innerHTML = "<p>No active real signal right now.</p>";
    return;
  }

  box.className = "signal-card";
  box.innerHTML = `
    <div class="signal-top">
      <span class="signal-main">${signal.symbol} ${signal.interval}</span>
      <span class="tier-badge ${signal.tier === "VIP" ? "tier-vip" : "tier-normal"}">${signal.tier} ${signal.grade}</span>
    </div>
    <div class="signal-meta">
      <span>${signal.side}</span>
      <span>Score ${signal.score}</span>
      <span>Entry ${signal.entry}</span>
      <span>SL ${signal.stop_loss}</span>
      <span>Remaining ${signal.remaining}</span>
      <span>${signal.opened_at}</span>
    </div>
  `;
}

function renderFakeBreakoutWarning(warning) {
  const box = document.getElementById("fakeBreakoutBox");
  const status = (warning.status || "NEUTRAL").toLowerCase().replace(" ", "-");
  box.className = `warning-card ${status}`;
  box.innerHTML = `
    <div class="warning-top">
      <span class="signal-main">${warning.status}</span>
      <span class="flat">${warning.trend}</span>
    </div>
    <div class="warning-meta">
      <span>${warning.detail}</span>
      <span>${warning.verdict}</span>
    </div>
  `;
}

function renderSessions(items) {
  const container = document.getElementById("sessionStatusList");
  container.innerHTML = items
    .map(
      (item) => `
        <article class="session-card">
          <div class="session-top">
            <span class="session-name">${item.name}</span>
            <span class="session-badge ${item.status === "ACTIVE" ? "session-active" : "session-quiet"}">${item.status}</span>
          </div>
          <div class="session-meta">
            <span>${item.window}</span>
          </div>
        </article>
      `
    )
    .join("");
}

function renderMarketCards(cards) {
  const container = document.getElementById("marketCards");
  container.innerHTML = cards
    .map(
      (card) => `
        <article class="market-card">
          <div class="market-top">
            <span class="market-interval">${card.interval}</span>
            <span class="${card.change.startsWith("-") ? "negative" : "positive"}">${card.change}</span>
          </div>
          <div class="market-meta">
            <span>Price ${card.price}</span>
            <span>${card.trend}</span>
            <span>${card.breakout}</span>
            <span>${card.verdict}</span>
            <span>${card.momentum}</span>
          </div>
        </article>
      `
    )
    .join("");
}

function renderSignalGrid(rows) {
  const body = document.getElementById("signalGridBody");
  body.innerHTML = rows
    .map(
      (row) => `
        <tr>
          <td>${row.symbol}</td>
          <td>${row.interval}</td>
          <td><span class="status-badge ${row.status === "LIVE TRADE" ? "status-live" : "status-scan"}">${row.status}</span></td>
          <td><span class="tier-badge ${row.tier === "VIP" ? "tier-vip" : "tier-normal"}">${row.tier}</span></td>
          <td>${row.side}</td>
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
  if (!items.length) {
    container.innerHTML = `<article class="recent-card"><p class="metric-label">No recent closed trades yet.</p></article>`;
    return;
  }
  container.innerHTML = items
    .map(
      (item) => `
        <article class="recent-card">
          <div class="recent-top">
            <span class="recent-symbol">${item.symbol} ${item.interval}</span>
            <span class="${Number(item.result_r) >= 0 ? "positive" : "negative"}">${item.result_r}R</span>
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
  if (!items.length) {
    container.innerHTML = `<article class="accuracy-card"><p class="metric-label">No closed-trade data yet.</p></article>`;
    return;
  }
  container.innerHTML = items
    .map(
      (item) => `
        <article class="accuracy-card">
          <div class="accuracy-top">
            <span class="accuracy-label">${item.label}</span>
            <span class="${parseFloat(item.result) >= 0 ? "positive" : "negative"}">${item.result}</span>
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

function renderBarChart(containerId, items) {
  const container = document.getElementById(containerId);
  if (!items.length) {
    container.innerHTML = `<article class="accuracy-card"><p class="metric-label">No chart data yet.</p></article>`;
    return;
  }
  const max = Math.max(...items.map((item) => Math.abs(Number(item.value) || 0)), 1);
  container.innerHTML = items
    .map((item) => {
      const value = Number(item.value) || 0;
      const height = Math.max((Math.abs(value) / max) * 180, 10);
      const klass = value > 0 ? "positive" : value < 0 ? "negative" : "neutral";
      return `
        <article class="bar-item ${klass}">
          <span class="${klass}">${value}</span>
          <div class="bar-value" style="height:${height}px"></div>
          <span class="bar-label">${item.label}</span>
        </article>
      `;
    })
    .join("");
}

function renderCandles(candles) {
  const svg = document.getElementById("candlesChart");
  if (!svg || !candles.length) return;
  const width = 1000;
  const height = 340;
  const padding = 24;
  const candleWidth = (width - padding * 2) / candles.length;
  const highs = candles.map((c) => c.high);
  const lows = candles.map((c) => c.low);
  const max = Math.max(...highs);
  const min = Math.min(...lows);
  const scaleY = (value) => padding + ((max - value) / Math.max(max - min, 1e-9)) * (height - padding * 2);

  const markup = candles
    .map((candle, index) => {
      const x = padding + index * candleWidth + candleWidth / 2;
      const openY = scaleY(candle.open);
      const closeY = scaleY(candle.close);
      const highY = scaleY(candle.high);
      const lowY = scaleY(candle.low);
      const top = Math.min(openY, closeY);
      const bodyHeight = Math.max(Math.abs(closeY - openY), 3);
      const color = candle.close >= candle.open ? "#0ecb81" : "#f6465d";

      return `
        <line x1="${x}" y1="${highY}" x2="${x}" y2="${lowY}" stroke="${color}" stroke-width="2"></line>
        <rect x="${x - candleWidth * 0.25}" y="${top}" width="${candleWidth * 0.5}" height="${bodyHeight}" fill="${color}" rx="2"></rect>
      `;
    })
    .join("");

  svg.innerHTML = markup;
}

function maybePlaySignalSound(payload) {
  const latest = payload.latest_signal || {};
  const signature = `${latest.symbol || ""}|${latest.interval || ""}|${latest.side || ""}|${latest.opened_at || ""}`;
  if (!latest.symbol || !signature) return;
  if (!state.lastSignalSignature) {
    state.lastSignalSignature = signature;
    return;
  }
  if (signature !== state.lastSignalSignature) {
    const audio = document.getElementById("signalAudio");
    if (audio) audio.play().catch(() => {});
    state.lastSignalSignature = signature;
  }
}

function renderCheckerHelp(help) {
  const output = document.getElementById("signalCheckerOutput");
  if (!output) return;
  output.innerHTML = `<pre>${help}</pre>`;
}

function render(payload) {
  state.payload = payload;
  document.getElementById("heroTitle").textContent = `${payload.mode} - ${payload.selected_symbol}`;
  renderPills("pairSelector", payload.symbols || [], payload.selected_symbol);
  renderTicker(payload.ticker_rows || []);
  renderOverview(payload.overview_cards || []);
  renderLatestSignal(payload.latest_signal || {});
  renderFakeBreakoutWarning(payload.fake_breakout_warning || {});
  renderSessions(payload.session_status || []);
  renderMarketCards(payload.market_cards || []);
  renderSignalGrid(payload.signal_grid || []);
  renderRecentClosures(payload.recent_closures || []);
  renderAccuracy("accuracyPairs", payload.accuracy?.by_symbol || []);
  renderAccuracy("accuracyIntervals", payload.accuracy?.by_interval || []);
  renderBarChart("winLossChart", payload.win_loss_chart || []);
  renderBarChart("signalHistoryChart", payload.signal_history_chart || []);
  renderCandles(payload.candles_chart || []);
  renderCheckerHelp(payload.user_signal_checker?.help || "");
  maybePlaySignalSound(payload);
}

async function refreshDashboard() {
  try {
    const selected = document.getElementById("pairSelector").value || state.payload.selected_symbol;
    const response = await fetch(`/api/dashboard?symbol=${encodeURIComponent(selected)}`, { cache: "no-store" });
    if (!response.ok) return;
    const payload = await response.json();
    render(payload);
  } catch (error) {
    console.error("Dashboard refresh failed", error);
  }
}

function setAutoRefresh(enabled) {
  state.autoRefresh = enabled;
  if (state.refreshTimer) {
    clearInterval(state.refreshTimer);
    state.refreshTimer = null;
  }
  if (enabled) {
    state.refreshTimer = setInterval(refreshDashboard, 15000);
  }
}

async function checkUserSignal() {
  const text = document.getElementById("signalInput").value.trim();
  if (!text) return;
  const selected = document.getElementById("pairSelector").value || state.payload.selected_symbol;
  const output = document.getElementById("signalCheckerOutput");
  output.innerHTML = "<pre>Checking signal...</pre>";
  try {
    const response = await fetch("/api/check-user-signal", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ text, symbol: selected, interval: "5m" }),
    });
    const payload = await response.json();
    output.innerHTML = `<pre>${payload.message || payload.detail || "Could not check signal."}</pre>`;
  } catch (error) {
    output.innerHTML = `<pre>Signal checker failed.</pre>`;
  }
}

document.getElementById("manualRefresh").addEventListener("click", refreshDashboard);
document.getElementById("pairSelector").addEventListener("change", refreshDashboard);
document.getElementById("autoRefreshToggle").addEventListener("change", (event) => {
  setAutoRefresh(event.target.checked);
});
document.getElementById("checkSignalBtn").addEventListener("click", checkUserSignal);
document.getElementById("pasteTemplateBtn").addEventListener("click", () => {
  document.getElementById("signalInput").value = state.payload.user_signal_checker?.help || "";
});

render(state.payload);
setAutoRefresh(true);
