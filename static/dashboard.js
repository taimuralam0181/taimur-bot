const state = {
  payload: window.__INITIAL_DASHBOARD__ || null,
  autoRefresh: true,
  lastSignalSignature: "",
  refreshTimer: null,
  chartInterval: "5m",
  checkerSide: "LONG",
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
  const tpList = (signal.take_profits || [])
    .map((tp, index) => {
      const hit = index === 0 ? signal.tp1_hit : index === 1 ? signal.tp2_hit : false;
      return `<span class="${hit ? "positive" : "flat"}">TP${index + 1} ${tp}${hit ? " HIT" : ""}</span>`;
    })
    .join("");
  const marketOverview = (signal.market_overview || [])
    .map((line) => `<li>${escapeHtml(line)}</li>`)
    .join("");
  const reasons = (signal.reasons || [])
    .map((line) => `<li>${escapeHtml(line)}</li>`)
    .join("");
  box.innerHTML = `
    <div class="signal-top">
      <span class="signal-main">${signal.symbol} ${signal.interval}</span>
      <span class="tier-badge ${signal.tier === "VIP" ? "tier-vip" : "tier-normal"}">${signal.tier} ${signal.grade}</span>
    </div>
    <div class="signal-meta">
      <span>${signal.side}</span>
      <span>${signal.verdict}</span>
      <span>Score ${signal.score}</span>
      <span>Opened ${signal.opened_at}</span>
    </div>
    <div class="signal-detail-grid">
      <div class="signal-detail-block">
        <p class="metric-label">Setup</p>
        <p class="signal-detail-title">${signal.setup_type || "-"}</p>
        <p class="signal-detail-copy">${signal.setup_note || "-"}</p>
      </div>
      <div class="signal-detail-block">
        <p class="metric-label">Trade Plan</p>
        <div class="signal-detail-list">
          <span>Entry ${signal.entry}</span>
          <span>Structure ${signal.market_structure}</span>
          <span>ATR ${signal.atr}</span>
          <span>SL ${signal.stop_loss}</span>
          <span>Remaining ${signal.remaining}</span>
        </div>
      </div>
      <div class="signal-detail-block">
        <p class="metric-label">Risk</p>
        <div class="signal-detail-list">
          <span>Risk ${signal.risk_pct}</span>
          <span>Leverage ${signal.leverage}</span>
          <span>Margin ${signal.margin_mode}</span>
          <span>${signal.leverage_note}</span>
        </div>
      </div>
      <div class="signal-detail-block">
        <p class="metric-label">Targets</p>
        <div class="signal-detail-list">
          ${tpList || "<span>-</span>"}
        </div>
      </div>
    </div>
    <div class="signal-detail-block">
      <p class="metric-label">Mentor Profit Plan</p>
      <div class="signal-detail-list">
        <span>TP1: Book 40%, move SL to entry</span>
        <span>TP2: Book 30%, move SL to TP1</span>
        <span>TP3: Close remaining 30%</span>
      </div>
    </div>
    <div class="signal-detail-block">
      <p class="metric-label">Market Condition</p>
      <ul class="signal-bullet-list">${marketOverview || "<li>No live market condition attached</li>"}</ul>
    </div>
    <div class="signal-detail-block">
      <p class="metric-label">Reasons</p>
      <ul class="signal-bullet-list">${reasons || "<li>No reasons saved</li>"}</ul>
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

function renderSignalHistory(items) {
  const container = document.getElementById("signalHistoryList");
  if (!items.length) {
    container.innerHTML = `<article class="recent-card"><p class="metric-label">No persistent signal history yet.</p></article>`;
    return;
  }
  container.innerHTML = items
    .map(
      (item) => {
        const resultValue = Number(item.result_r);
        const statusClass =
          item.status === "ACTIVE" ? "positive" : Number.isFinite(resultValue) && resultValue >= 0 ? "positive" : "negative";
        const tailText = item.status === "CLOSED" ? `${item.result_r}R` : item.opened_at;
        return `
        <article class="recent-card">
          <div class="recent-top">
            <span class="recent-symbol">${item.symbol} ${item.interval}</span>
            <span class="${statusClass}">${item.status}</span>
          </div>
          <div class="recent-meta">
            <span>${item.side}</span>
            <span>${item.tier} ${item.grade}</span>
            <span>Entry ${item.entry}</span>
            <span>${tailText}</span>
          </div>
        </article>
      `;
      }
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

function renderCandles(chartPayload) {
  const svg = document.getElementById("candlesChart");
  const meta = document.getElementById("chartMeta");
  const candles = chartPayload?.candles || [];
  if (!svg || !candles.length) return;
  const width = 1000;
  const height = 340;
  const padding = 36;
  const candleWidth = (width - padding * 2) / candles.length;
  const highs = candles.map((c) => c.high);
  const lows = candles.map((c) => c.low);
  const max = Math.max(...highs);
  const min = Math.min(...lows);
  const scaleY = (value) => padding + ((max - value) / Math.max(max - min, 1e-9)) * (height - padding * 2);
  const grid = Array.from({ length: 5 }, (_, index) => {
    const y = padding + ((height - padding * 2) / 4) * index;
    return `<line x1="${padding}" y1="${y}" x2="${width - padding}" y2="${y}" stroke="rgba(255,255,255,0.06)" stroke-width="1"></line>`;
  }).join("");
  const priceLabels = Array.from({ length: 5 }, (_, index) => {
    const price = max - ((max - min) / 4) * index;
    const y = padding + ((height - padding * 2) / 4) * index + 4;
    return `<text x="${width - padding + 6}" y="${y}" fill="rgba(255,255,255,0.56)" font-size="11">${price.toFixed(2)}</text>`;
  }).join("");

  const bodies = candles
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
  const timeLabels = [candles[0], candles[Math.floor(candles.length / 2)], candles[candles.length - 1]]
    .filter(Boolean)
    .map((candle, index, list) => {
      const x = index === 0 ? padding : index === list.length - 1 ? width - padding - 48 : width / 2 - 24;
      return `<text x="${x}" y="${height - 10}" fill="rgba(255,255,255,0.56)" font-size="11">${candle.time}</text>`;
    })
    .join("");

  svg.innerHTML = `${grid}${bodies}${priceLabels}${timeLabels}`;
  if (meta) {
    meta.textContent = `${chartPayload.symbol} ${chartPayload.interval} | Price ${chartPayload.price} | ${chartPayload.trend} | ${chartPayload.breakout} | ${chartPayload.verdict}`;
  }
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

function escapeHtml(value) {
  return String(value || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function buildQuickSignalText() {
  const pair = document.getElementById("checkerPair").value || state.payload.selected_symbol || "BTCUSDT";
  const timeframe = document.getElementById("checkerTimeframe").value || "5m";
  const side = state.checkerSide || "LONG";
  const entry = document.getElementById("checkerEntry").value.trim();
  const stopLoss = document.getElementById("checkerStopLoss").value.trim();
  const tp1 = document.getElementById("checkerTp1").value.trim();
  const tp2 = document.getElementById("checkerTp2").value.trim();
  const tp3 = document.getElementById("checkerTp3").value.trim();
  const lines = [`PAIR: ${pair}`, `TIMEFRAME: ${timeframe}`, `SIDE: ${side}`, `ENTRY: ${entry}`];
  if (stopLoss) lines.push(`SL: ${stopLoss}`);
  if (tp1) lines.push(`TP1: ${tp1}`);
  if (tp2) lines.push(`TP2: ${tp2}`);
  if (tp3) lines.push(`TP3: ${tp3}`);
  return lines.join("\n");
}

function renderCheckerHelp(help) {
  const output = document.getElementById("signalCheckerOutput");
  if (!output) return;
  output.innerHTML = `
    <div class="checker-result checker-result-neutral">
      <p class="checker-result-kicker">How To Use</p>
      <h4>Quick form diye signal check koro</h4>
      <p class="checker-result-summary">Pair, timeframe, side, entry, SL, TP dile bot market-er sathe compare kore clean verdict dibe.</p>
      <div class="checker-result-points">
        <span>GOOD = entry possible</span>
        <span>WATCH = wait koro</span>
        <span>BAD = avoid</span>
      </div>
      <div class="checker-message">${escapeHtml(help)}</div>
    </div>
  `;
}

function renderCheckerResult(payload) {
  const output = document.getElementById("signalCheckerOutput");
  if (!output) return;
  const verdict = String(payload.verdict || "NEUTRAL").toUpperCase();
  const lowered = verdict.toLowerCase();
  let klass = "checker-result-neutral";
  if (lowered.includes("good") || lowered.includes("valid")) klass = "checker-result-good";
  else if (lowered.includes("watch") || lowered.includes("late")) klass = "checker-result-watch";
  else if (lowered.includes("bad") || lowered.includes("invalid")) klass = "checker-result-bad";

  const message = String(payload.message || payload.detail || "No checker response.");
  const headline =
    klass === "checker-result-good"
      ? "Signal ta market-er sathe aligned."
      : klass === "checker-result-watch"
      ? "Ekhono wait kora better."
      : klass === "checker-result-bad"
      ? "Ei entry avoid kora better."
      : "Checker result ready.";

  output.innerHTML = `
    <div class="checker-result ${klass}">
      <p class="checker-result-kicker">Bot Verdict</p>
      <h4>${escapeHtml(verdict)}</h4>
      <p class="checker-result-summary">${escapeHtml(headline)}</p>
      <div class="checker-result-points">
        <span>${escapeHtml(payload.note || "Market check done")}</span>
        <span>${escapeHtml(document.getElementById("checkerPair").value || "")}</span>
        <span>${escapeHtml(document.getElementById("checkerTimeframe").value || "")}</span>
      </div>
      <div class="checker-message">${escapeHtml(message)}</div>
    </div>
  `;
}

function syncSideButtons() {
  document.querySelectorAll(".side-btn").forEach((button) => {
    button.classList.toggle("active", button.dataset.side === state.checkerSide);
  });
}

function render(payload) {
  state.payload = payload;
  state.chartInterval = state.chartInterval || "5m";
  document.getElementById("heroTitle").textContent = `${payload.mode} - ${payload.selected_symbol}`;
  renderPills("pairSelector", payload.symbols || [], payload.selected_symbol);
  renderPills("chartIntervalSelector", payload.intervals || [], state.chartInterval);
  renderPills("checkerPair", payload.symbols || [], payload.selected_symbol);
  renderPills("checkerTimeframe", payload.intervals || [], "5m");
  renderTicker(payload.ticker_rows || []);
  renderOverview(payload.overview_cards || []);
  renderLatestSignal(payload.latest_signal || {});
  renderFakeBreakoutWarning(payload.fake_breakout_warning || {});
  renderSessions(payload.session_status || []);
  renderMarketCards(payload.market_cards || []);
  renderSignalGrid(payload.signal_grid || []);
  renderRecentClosures(payload.recent_closures || []);
  renderSignalHistory(payload.signal_history || []);
  renderAccuracy("accuracyPairs", payload.accuracy?.by_symbol || []);
  renderAccuracy("accuracyIntervals", payload.accuracy?.by_interval || []);
  renderBarChart("winLossChart", payload.win_loss_chart || []);
  renderBarChart("signalHistoryChart", payload.signal_history_chart || []);
  renderCandles(payload.candles_chart || []);
  renderCheckerHelp(payload.user_signal_checker?.help || "");
  syncSideButtons();
  maybePlaySignalSound(payload);
}

async function refreshDashboard() {
  try {
    const selected = document.getElementById("pairSelector").value || state.payload.selected_symbol;
    const response = await fetch(`/api/dashboard?symbol=${encodeURIComponent(selected)}`, { cache: "no-store" });
    if (!response.ok) return;
    const payload = await response.json();
    render(payload);
    await refreshChartOnly();
  } catch (error) {
    console.error("Dashboard refresh failed", error);
  }
}

async function refreshChartOnly() {
  try {
    const symbol = document.getElementById("pairSelector").value || state.payload.selected_symbol;
    const interval = document.getElementById("chartIntervalSelector").value || state.chartInterval || "5m";
    state.chartInterval = interval;
    const response = await fetch(
      `/api/chart?symbol=${encodeURIComponent(symbol)}&interval=${encodeURIComponent(interval)}`,
      { cache: "no-store" }
    );
    if (!response.ok) return;
    const payload = await response.json();
    renderCandles(payload);
  } catch (error) {
    console.error("Chart refresh failed", error);
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
  const advancedText = document.getElementById("signalInput").value.trim();
  const entry = document.getElementById("checkerEntry").value.trim();
  const text = advancedText || buildQuickSignalText();
  if (!advancedText && !entry) {
    renderCheckerResult({
      verdict: "WATCH",
      note: "Entry needed",
      message: "Quick check-er jonno at least entry price ba zone dao.",
    });
    return;
  }
  if (!text) return;
  const selected = document.getElementById("checkerPair").value || state.payload.selected_symbol;
  const interval = document.getElementById("checkerTimeframe").value || "5m";
  renderCheckerResult({
    verdict: "CHECKING",
    note: "Live market scan",
    message: "Signal analyze kortese...",
  });
  try {
    const response = await fetch("/api/check-user-signal", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ text, symbol: selected, interval, side: state.checkerSide }),
    });
    const payload = await response.json();
    renderCheckerResult(payload);
  } catch (error) {
    renderCheckerResult({
      verdict: "BAD",
      note: "Request failed",
      message: "Signal checker fail korse. Abar try koro.",
    });
  }
}

document.getElementById("manualRefresh").addEventListener("click", refreshDashboard);
document.getElementById("pairSelector").addEventListener("change", refreshDashboard);
document.getElementById("chartIntervalSelector").addEventListener("change", refreshChartOnly);
document.getElementById("autoRefreshToggle").addEventListener("change", (event) => {
  setAutoRefresh(event.target.checked);
});
document.getElementById("checkSignalBtn").addEventListener("click", checkUserSignal);
document.getElementById("pasteTemplateBtn").addEventListener("click", () => {
  document.getElementById("signalInput").value = state.payload.user_signal_checker?.help || "";
});
document.getElementById("useQuickTemplateBtn").addEventListener("click", () => {
  document.getElementById("signalInput").value = buildQuickSignalText();
});
document.querySelectorAll(".side-btn").forEach((button) => {
  button.addEventListener("click", () => {
    state.checkerSide = button.dataset.side || "LONG";
    syncSideButtons();
  });
});

render(state.payload);
refreshChartOnly();
setAutoRefresh(true);
