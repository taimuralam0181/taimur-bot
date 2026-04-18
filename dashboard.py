import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates


ROOT_DIR = Path(__file__).resolve().parent
STATE_FILE = Path(os.getenv("BOT_STATE_FILE", str(ROOT_DIR / "bot_state.json"))).expanduser()
TEMPLATES_DIR = ROOT_DIR / "templates"
STATIC_DIR = ROOT_DIR / "static"

app = FastAPI(title="Scalping Dashboard")
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))


def load_state() -> Dict[str, Any]:
    if not STATE_FILE.exists():
        return {}

    try:
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}


def parse_env_list(name: str, fallback: List[str]) -> List[str]:
    value = os.getenv(name, "").strip()
    if not value:
        return fallback
    return [item.strip() for item in value.split(",") if item.strip()]


def format_candle_time(timestamp_ms: Any) -> str:
    if not timestamp_ms:
        return "-"
    try:
        return (
            datetime.fromtimestamp(int(timestamp_ms) / 1000, tz=timezone.utc)
            .astimezone()
            .strftime("%Y-%m-%d %I:%M %p")
        )
    except (TypeError, ValueError, OSError):
        return "-"


def format_number(value: Any) -> str:
    try:
        return f"{float(value):.2f}"
    except (TypeError, ValueError):
        return "0.00"


def format_percent(value: Any) -> str:
    try:
        return f"{float(value):.1f}%"
    except (TypeError, ValueError):
        return "0.0%"


def calculate_win_rate(stats_bucket: Dict[str, Any]) -> float:
    closed_trades = int(stats_bucket.get("closed_trades", 0) or 0)
    if closed_trades <= 0:
        return 0.0
    wins = int(stats_bucket.get("wins", 0) or 0)
    return (wins / closed_trades) * 100.0


def build_overview_cards(state: Dict[str, Any]) -> List[Dict[str, str]]:
    performance_state = state.get("performance", {})
    overall_stats = performance_state.get("overall", {}) if isinstance(performance_state, dict) else {}
    recent_closed = performance_state.get("recent_closed", []) if isinstance(performance_state, dict) else []
    symbols_state = state.get("symbols", {}) if isinstance(state.get("symbols", {}), dict) else {}

    active_trades = 0
    latest_checked = 0
    for symbol_state in symbols_state.values():
        intervals_state = symbol_state.get("intervals", {}) if isinstance(symbol_state, dict) else {}
        for interval_state in intervals_state.values():
            if not isinstance(interval_state, dict):
                continue
            if isinstance(interval_state.get("active_trade"), dict):
                active_trades += 1
            latest_checked = max(latest_checked, int(interval_state.get("last_checked_candle_time", 0) or 0))

    return [
        {"label": "Mode", "value": "Real Breakout Scalping"},
        {"label": "Active Trades", "value": str(active_trades)},
        {"label": "Signals Sent", "value": str(int(overall_stats.get("signals_sent", 0) or 0))},
        {"label": "Win Rate", "value": format_percent(calculate_win_rate(overall_stats))},
        {"label": "Closed Trades", "value": str(int(overall_stats.get("closed_trades", 0) or 0))},
        {"label": "Recent Closures", "value": str(len(recent_closed))},
        {"label": "Total Result", "value": format_number(overall_stats.get("total_r", 0.0)) + "R"},
        {"label": "Last Scan", "value": format_candle_time(latest_checked)},
    ]


def build_signal_grid(state: Dict[str, Any]) -> List[Dict[str, Any]]:
    symbols_state = state.get("symbols", {})
    rows: List[Dict[str, Any]] = []

    if not isinstance(symbols_state, dict):
        return rows

    for symbol, symbol_state in symbols_state.items():
        intervals_state = symbol_state.get("intervals", {}) if isinstance(symbol_state, dict) else {}
        for interval, interval_state in intervals_state.items():
            if not isinstance(interval_state, dict):
                continue

            trade = interval_state.get("active_trade") if isinstance(interval_state.get("active_trade"), dict) else None
            row = {
                "symbol": symbol,
                "interval": interval,
                "last_checked": format_candle_time(interval_state.get("last_checked_candle_time", 0)),
                "last_signal_side": interval_state.get("last_signal_side", "-"),
                "last_signal_time": interval_state.get("last_signal_sent_time", "-"),
                "status": "LIVE TRADE" if trade else "SCANNING",
                "score": str(trade.get("score", "-")) if trade else "-",
                "entry": format_number(trade.get("entry")) if trade else "-",
                "stop_loss": format_number(trade.get("current_stop_loss", trade.get("stop_loss"))) if trade else "-",
                "remaining": str(int(float(trade.get("remaining_position_pct", 0) or 0))) + "%" if trade else "-",
                "tier": trade.get("tier", "-") if trade else "-",
            }
            rows.append(row)

    rows.sort(key=lambda item: (item["symbol"], item["interval"]))
    return rows


def build_recent_closures(state: Dict[str, Any]) -> List[Dict[str, Any]]:
    performance_state = state.get("performance", {})
    recent_closed = performance_state.get("recent_closed", []) if isinstance(performance_state, dict) else []
    if not isinstance(recent_closed, list):
        return []
    return list(reversed(recent_closed[-12:]))


def build_accuracy_panels(state: Dict[str, Any]) -> Dict[str, List[Dict[str, str]]]:
    performance_state = state.get("performance", {})
    if not isinstance(performance_state, dict):
        return {"by_symbol": [], "by_interval": []}

    panels: Dict[str, List[Dict[str, str]]] = {"by_symbol": [], "by_interval": []}
    for key, target in (("by_symbol", "by_symbol"), ("by_interval", "by_interval")):
        section = performance_state.get(key, {})
        if not isinstance(section, dict):
            continue

        for label, stats in section.items():
            if not isinstance(stats, dict):
                continue
            panels[target].append(
                {
                    "label": label,
                    "win_rate": format_percent(calculate_win_rate(stats)),
                    "closed": str(int(stats.get("closed_trades", 0) or 0)),
                    "result": format_number(stats.get("total_r", 0.0)) + "R",
                }
            )

        panels[target].sort(key=lambda item: float(item["win_rate"].rstrip("%")), reverse=True)
    return panels


def build_dashboard_payload() -> Dict[str, Any]:
    state = load_state()
    default_symbols = parse_env_list("BOT_SYMBOLS", ["BTCUSDT", "ETHUSDT", "SOLUSDT"])
    default_intervals = parse_env_list("BOT_INTERVALS", ["5m", "15m"])

    return {
        "timestamp": datetime.now().astimezone().strftime("%Y-%m-%d %I:%M:%S %p"),
        "mode": "Real Breakout Scalping",
        "symbols": default_symbols,
        "intervals": default_intervals,
        "overview_cards": build_overview_cards(state),
        "signal_grid": build_signal_grid(state),
        "recent_closures": build_recent_closures(state),
        "accuracy": build_accuracy_panels(state),
    }


@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request) -> HTMLResponse:
    payload = build_dashboard_payload()
    return templates.TemplateResponse(
        request,
        "dashboard.html",
        {"request": request, "payload": payload},
    )


@app.get("/api/dashboard", response_class=JSONResponse)
async def dashboard_api() -> JSONResponse:
    return JSONResponse(build_dashboard_payload())


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("dashboard:app", host="0.0.0.0", port=int(os.getenv("PORT", "8000")), reload=False)
