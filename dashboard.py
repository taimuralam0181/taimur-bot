import json
import os
import sqlite3
from datetime import datetime, time, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

import bot as trading_bot


ROOT_DIR = Path(__file__).resolve().parent
STATE_FILE = Path(os.getenv("BOT_STATE_FILE", str(ROOT_DIR / "bot_state.json"))).expanduser()
HISTORY_DB_FILE = Path(
    os.getenv("BOT_HISTORY_DB_FILE", str(ROOT_DIR / "dashboard_history.db"))
).expanduser()
TEMPLATES_DIR = ROOT_DIR / "templates"
STATIC_DIR = ROOT_DIR / "static"

DEFAULT_SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
DEFAULT_INTERVALS = ["5m", "15m"]

app = FastAPI(title="Scalping Dashboard")
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))


class UserSignalCheckRequest(BaseModel):
    text: str
    symbol: Optional[str] = None
    interval: Optional[str] = None
    side: Optional[str] = None


def parse_env_list(name: str, fallback: List[str]) -> List[str]:
    value = os.getenv(name, "").strip()
    if not value:
        return fallback
    return [item.strip() for item in value.split(",") if item.strip()]


def load_state() -> Dict[str, Any]:
    if not STATE_FILE.exists():
        return {}

    try:
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}


def get_db_connection() -> sqlite3.Connection:
    connection = sqlite3.connect(HISTORY_DB_FILE)
    connection.row_factory = sqlite3.Row
    return connection


def init_history_db() -> None:
    with get_db_connection() as connection:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS signal_history (
                signal_key TEXT PRIMARY KEY,
                symbol TEXT NOT NULL,
                interval TEXT NOT NULL,
                side TEXT,
                tier TEXT,
                grade TEXT,
                score INTEGER,
                entry REAL,
                stop_loss REAL,
                status TEXT NOT NULL,
                candle_time INTEGER,
                opened_at TEXT,
                closed_at TEXT,
                close_reason TEXT,
                result_r REAL,
                exit_price REAL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_signal_history_status_updated ON signal_history(status, updated_at DESC)"
        )
        connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_signal_history_symbol_interval ON signal_history(symbol, interval, updated_at DESC)"
        )


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


def format_number(value: Any, precision: int = 2) -> str:
    try:
        return f"{float(value):.{precision}f}"
    except (TypeError, ValueError):
        return "0.00"


def format_optional_number(value: Any, precision: int = 2) -> str:
    if value is None:
        return "-"
    return format_number(value, precision)


def format_percent(value: Any) -> str:
    try:
        return f"{float(value):.2f}%"
    except (TypeError, ValueError):
        return "0.00%"


def calculate_win_rate(stats_bucket: Dict[str, Any]) -> float:
    closed_trades = int(stats_bucket.get("closed_trades", 0) or 0)
    if closed_trades <= 0:
        return 0.0
    wins = int(stats_bucket.get("wins", 0) or 0)
    return (wins / closed_trades) * 100.0


def build_analysis_config(symbol: str, interval: str) -> trading_bot.Config:
    symbols = parse_env_list("BOT_SYMBOLS", DEFAULT_SYMBOLS)
    intervals = parse_env_list("BOT_INTERVALS", DEFAULT_INTERVALS)

    return trading_bot.Config(
        telegram_token=os.getenv("BOT_TOKEN", "dashboard"),
        telegram_chat_id=os.getenv("CHAT_ID", ""),
        symbol=symbol,
        symbols=symbols,
        interval=interval,
        intervals=intervals,
        poll_seconds=int(os.getenv("BOT_POLL_SECONDS", "15")),
        lookback_limit=int(os.getenv("BOT_LOOKBACK_LIMIT", "260")),
        ema_fast_period=int(os.getenv("BOT_EMA_FAST", "20")),
        ema_slow_period=int(os.getenv("BOT_EMA_SLOW", "50")),
        rsi_period=int(os.getenv("BOT_RSI_PERIOD", "14")),
        atr_period=int(os.getenv("BOT_ATR_PERIOD", "14")),
        sr_lookback=int(os.getenv("BOT_SR_LOOKBACK", "12")),
        volume_period=int(os.getenv("BOT_VOLUME_PERIOD", "14")),
        volume_spike_factor=float(os.getenv("BOT_VOLUME_SPIKE_FACTOR", "1.03")),
        breakout_buffer_pct=float(os.getenv("BOT_BREAKOUT_BUFFER_PCT", "0.0005")),
        cooldown_candles=int(os.getenv("BOT_COOLDOWN_CANDLES", "4")),
        min_signal_score=int(os.getenv("BOT_MIN_SIGNAL_SCORE", "70")),
        vip_signal_score=int(os.getenv("BOT_VIP_SIGNAL_SCORE", "82")),
        normal_risk_pct=float(os.getenv("BOT_NORMAL_RISK_PCT", "0.35")),
        vip_risk_pct=float(os.getenv("BOT_VIP_RISK_PCT", "0.55")),
        normal_leverage=os.getenv("BOT_NORMAL_LEVERAGE", "5x-8x").strip(),
        vip_leverage=os.getenv("BOT_VIP_LEVERAGE", "8x-12x").strip(),
        margin_mode=os.getenv("BOT_MARGIN_MODE", "Isolated").strip() or "Isolated",
        require_higher_timeframe_confirmation=trading_bot.parse_bool_env(
            "BOT_REQUIRE_HTF_CONFIRMATION", True
        ),
        watch_alert_enabled=trading_bot.parse_bool_env("BOT_WATCH_ALERT_ENABLED", True),
        watch_alert_score_gap=int(os.getenv("BOT_WATCH_ALERT_SCORE_GAP", "10")),
        max_extension_atr=float(os.getenv("BOT_MAX_EXTENSION_ATR", "1.6")),
        atr_stop_multiplier=float(os.getenv("BOT_ATR_STOP_MULTIPLIER", "1.0")),
        tp_one_r=float(os.getenv("BOT_TP1_R", "1.0")),
        tp_two_r=float(os.getenv("BOT_TP2_R", "1.8")),
        tp_three_r=float(os.getenv("BOT_TP3_R", "2.8")),
        hourly_update_enabled=trading_bot.parse_bool_env("BOT_HOURLY_UPDATE_ENABLED", True),
        hourly_update_interval_minutes=int(os.getenv("BOT_HOURLY_UPDATE_MINUTES", "60")),
        hourly_update_timeframe=os.getenv("BOT_HOURLY_UPDATE_TIMEFRAME", "5m").strip() or "5m",
        daily_report_hour=int(os.getenv("BOT_DAILY_REPORT_HOUR", "23")),
        state_file=STATE_FILE,
        adaptive_learning_enabled=trading_bot.parse_bool_env("BOT_ADAPTIVE_LEARNING_ENABLED", True),
        adaptive_learning_max_adjustment=int(os.getenv("BOT_ADAPTIVE_LEARNING_MAX_ADJUSTMENT", "8")),
    )


def fetch_market_bundle(symbol: str, interval: str) -> Dict[str, Any]:
    config = build_analysis_config(symbol, interval)
    candles = trading_bot.fetch_klines(config)
    overview = trading_bot.calculate_market_overview_for_candles(candles, config)
    higher_trend = trading_bot.fetch_confirmation_trend(config, interval)
    higher_overview = trading_bot.fetch_confirmation_overview(config, interval)
    analysis = trading_bot.analyze_market(candles, config, higher_trend, higher_overview)
    last = candles[-1]
    previous_close = candles[-2].close if len(candles) > 1 else last.close
    pct_change = ((last.close - previous_close) / previous_close) * 100 if previous_close else 0.0

    return {
        "symbol": symbol,
        "interval": interval,
        "price": last.close,
        "price_text": format_number(last.close, 4 if last.close < 100 else 2),
        "change_pct": pct_change,
        "change_text": format_percent(pct_change),
        "overview": overview,
        "analysis": analysis,
        "candles": candles[-160:],
    }


def build_ticker_rows(symbols: List[str]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for symbol in symbols:
        try:
            bundle = fetch_market_bundle(symbol, "5m")
            rows.append(
                {
                    "symbol": symbol,
                    "price": bundle["price_text"],
                    "change_pct": bundle["change_text"],
                    "direction": "up" if bundle["change_pct"] >= 0 else "down",
                    "trend": bundle["overview"].trend,
                }
            )
        except Exception:
            rows.append(
                {
                    "symbol": symbol,
                    "price": "-",
                    "change_pct": "-",
                    "direction": "flat",
                    "trend": "Unavailable",
                }
            )
    return rows


def build_overview_cards(state: Dict[str, Any], signal_grid: List[Dict[str, Any]]) -> List[Dict[str, str]]:
    performance_state = state.get("performance", {})
    overall_stats = performance_state.get("overall", {}) if isinstance(performance_state, dict) else {}
    recent_closed = performance_state.get("recent_closed", []) if isinstance(performance_state, dict) else []

    latest_checked = 0
    for row in signal_grid:
        latest_checked = max(latest_checked, int(row.get("last_checked_raw", 0) or 0))

    return [
        {"label": "Mode", "value": "Real Breakout Scalping"},
        {"label": "Signals Sent", "value": str(int(overall_stats.get("signals_sent", 0) or 0))},
        {"label": "Win Rate", "value": format_percent(calculate_win_rate(overall_stats))},
        {"label": "Closed Trades", "value": str(int(overall_stats.get("closed_trades", 0) or 0))},
        {"label": "Recent Closures", "value": str(len(recent_closed))},
        {"label": "Total Result", "value": format_number(overall_stats.get("total_r", 0.0)) + "R"},
        {"label": "Last Scan", "value": format_candle_time(latest_checked)},
        {"label": "Open Trades", "value": str(sum(1 for row in signal_grid if row["status"] == "LIVE TRADE"))},
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
            last_checked_raw = int(interval_state.get("last_checked_candle_time", 0) or 0)
            rows.append(
                {
                    "symbol": symbol,
                    "interval": interval,
                    "last_checked": format_candle_time(last_checked_raw),
                    "last_checked_raw": last_checked_raw,
                    "last_signal_side": interval_state.get("last_signal_side", "-"),
                    "last_signal_time": interval_state.get("last_signal_sent_time", "-"),
                    "status": "LIVE TRADE" if trade else "SCANNING",
                    "score": str(trade.get("score", "-")) if trade else "-",
                    "entry": format_number(trade.get("entry")) if trade else "-",
                    "stop_loss": format_number(trade.get("current_stop_loss", trade.get("stop_loss"))) if trade else "-",
                    "remaining": str(int(float(trade.get("remaining_position_pct", 0) or 0))) + "%" if trade else "-",
                    "tier": trade.get("tier", "-") if trade else "-",
                    "side": trade.get("side", interval_state.get("last_signal_side", "-")) if trade else interval_state.get("last_signal_side", "-"),
                }
            )

    rows.sort(key=lambda item: (item["symbol"], item["interval"]))
    return rows


def sync_signal_history_database(state: Dict[str, Any]) -> None:
    symbols_state = state.get("symbols", {})
    performance_state = state.get("performance", {})
    recent_closed = performance_state.get("recent_closed", []) if isinstance(performance_state, dict) else []
    now_iso = datetime.now(timezone.utc).isoformat()

    if not isinstance(symbols_state, dict):
        symbols_state = {}
    if not isinstance(recent_closed, list):
        recent_closed = []

    with get_db_connection() as connection:
        for symbol, symbol_state in symbols_state.items():
            intervals_state = symbol_state.get("intervals", {}) if isinstance(symbol_state, dict) else {}
            if not isinstance(intervals_state, dict):
                continue

            for interval, interval_state in intervals_state.items():
                if not isinstance(interval_state, dict):
                    continue
                trade = interval_state.get("active_trade")
                if not isinstance(trade, dict):
                    continue

                side = str(trade.get("side", "-") or "-")
                candle_time = int(trade.get("candle_time", 0) or 0)
                signal_key = f"{symbol}|{interval}|{side}|{candle_time or trade.get('opened_at', '-')}"

                connection.execute(
                    """
                    INSERT INTO signal_history (
                        signal_key, symbol, interval, side, tier, grade, score, entry, stop_loss,
                        status, candle_time, opened_at, closed_at, close_reason, result_r, exit_price,
                        created_at, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'ACTIVE', ?, ?, NULL, NULL, ?, NULL, ?, ?)
                    ON CONFLICT(signal_key) DO UPDATE SET
                        tier = excluded.tier,
                        grade = excluded.grade,
                        score = excluded.score,
                        entry = excluded.entry,
                        stop_loss = excluded.stop_loss,
                        status = 'ACTIVE',
                        opened_at = excluded.opened_at,
                        result_r = excluded.result_r,
                        updated_at = excluded.updated_at
                    """,
                    (
                        signal_key,
                        symbol,
                        interval,
                        side,
                        str(trade.get("tier", "-") or "-"),
                        str(trade.get("grade", "-") or "-"),
                        int(trade.get("score", 0) or 0),
                        float(trade.get("entry", 0.0) or 0.0),
                        float(trade.get("current_stop_loss", trade.get("stop_loss", 0.0)) or 0.0),
                        candle_time,
                        str(trade.get("opened_at", "-") or "-"),
                        float(trade.get("realized_r", 0.0) or 0.0),
                        now_iso,
                        now_iso,
                    ),
                )

        for item in recent_closed:
            if not isinstance(item, dict):
                continue

            symbol = str(item.get("symbol", "-") or "-")
            interval = str(item.get("interval", "-") or "-")
            side = str(item.get("side", "-") or "-")
            closed_at = str(item.get("closed_at", "-") or "-")
            matched = connection.execute(
                """
                SELECT signal_key FROM signal_history
                WHERE symbol = ? AND interval = ? AND side = ? AND status != 'CLOSED'
                ORDER BY COALESCE(candle_time, 0) DESC, updated_at DESC
                LIMIT 1
                """,
                (symbol, interval, side),
            ).fetchone()

            signal_key = (
                str(matched["signal_key"])
                if matched
                else f"CLOSED|{symbol}|{interval}|{side}|{closed_at}"
            )

            connection.execute(
                """
                INSERT INTO signal_history (
                    signal_key, symbol, interval, side, tier, grade, score, entry, stop_loss,
                    status, candle_time, opened_at, closed_at, close_reason, result_r, exit_price,
                    created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, NULL, NULL, 'CLOSED', 0, NULL, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(signal_key) DO UPDATE SET
                    status = 'CLOSED',
                    tier = excluded.tier,
                    closed_at = excluded.closed_at,
                    close_reason = excluded.close_reason,
                    result_r = excluded.result_r,
                    exit_price = excluded.exit_price,
                    updated_at = excluded.updated_at
                """,
                (
                    signal_key,
                    symbol,
                    interval,
                    side,
                    str(item.get("tier", "-") or "-"),
                    "-",
                    0,
                    closed_at,
                    str(item.get("close_reason", "-") or "-"),
                    float(item.get("result_r", 0.0) or 0.0),
                    float(item.get("exit_price", 0.0) or 0.0),
                    now_iso,
                    now_iso,
                ),
            )


def load_signal_history(limit: int = 20) -> List[Dict[str, Any]]:
    with get_db_connection() as connection:
        rows = connection.execute(
            """
            SELECT *
            FROM signal_history
            ORDER BY
                CASE WHEN status = 'ACTIVE' THEN 0 ELSE 1 END,
                COALESCE(candle_time, 0) DESC,
                updated_at DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()

    history: List[Dict[str, Any]] = []
    for row in rows:
        history.append(
            {
                "signal_key": row["signal_key"],
                "symbol": row["symbol"],
                "interval": row["interval"],
                "side": row["side"] or "-",
                "tier": row["tier"] or "-",
                "grade": row["grade"] or "-",
                "score": row["score"] or 0,
                "entry": format_optional_number(row["entry"]),
                "stop_loss": format_optional_number(row["stop_loss"]),
                "status": row["status"],
                "opened_at": row["opened_at"] or "-",
                "closed_at": row["closed_at"] or "-",
                "close_reason": row["close_reason"] or "-",
                "result_r": format_number(row["result_r"]) if row["result_r"] is not None else "-",
                "exit_price": format_optional_number(row["exit_price"]),
            }
        )
    return history


def build_recent_closures_from_db(limit: int = 12) -> List[Dict[str, Any]]:
    with get_db_connection() as connection:
        rows = connection.execute(
            """
            SELECT symbol, interval, side, tier, result_r, exit_price, closed_at, close_reason
            FROM signal_history
            WHERE status = 'CLOSED'
            ORDER BY updated_at DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    return [
        {
            "symbol": row["symbol"],
            "interval": row["interval"],
            "side": row["side"] or "-",
            "tier": row["tier"] or "-",
            "result_r": float(row["result_r"] or 0.0),
            "exit_price": format_optional_number(row["exit_price"]),
            "closed_at": row["closed_at"] or "-",
            "close_reason": row["close_reason"] or "-",
        }
        for row in rows
    ]


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


def get_latest_real_signal(state: Dict[str, Any]) -> Dict[str, Any]:
    latest: Dict[str, Any] = {}
    symbols_state = state.get("symbols", {})
    if not isinstance(symbols_state, dict):
        return {}

    latest_time = 0
    for symbol, symbol_state in symbols_state.items():
        intervals_state = symbol_state.get("intervals", {}) if isinstance(symbol_state, dict) else {}
        for interval, interval_state in intervals_state.items():
            if not isinstance(interval_state, dict):
                continue
            trade = interval_state.get("active_trade")
            if not isinstance(trade, dict):
                continue
            candle_time = int(trade.get("candle_time", 0) or interval_state.get("last_signal_candle_time", 0) or 0)
            if candle_time >= latest_time:
                latest_time = candle_time
                latest = {
                    "symbol": symbol,
                    "interval": interval,
                    "side": trade.get("side", "-"),
                    "tier": trade.get("tier", "-"),
                    "grade": trade.get("grade", "-"),
                    "verdict": trade.get("verdict", "-"),
                    "setup_type": trade.get("setup_type", "-"),
                    "setup_note": trade.get("setup_note", "-"),
                    "score": str(trade.get("score", "-")),
                    "entry": format_number(trade.get("entry")),
                    "market_structure": format_number(trade.get("market_structure_level")),
                    "atr": format_number(trade.get("atr")),
                    "stop_loss": format_number(trade.get("current_stop_loss", trade.get("stop_loss"))),
                    "take_profits": [
                        format_number(tp) for tp in (trade.get("take_profits", []) or [])
                    ],
                    "risk_pct": str(trade.get("risk_pct", "-")),
                    "leverage": str(trade.get("leverage", "-")),
                    "leverage_note": str(trade.get("leverage_note", "-")),
                    "margin_mode": str(trade.get("margin_mode", "-")),
                    "opened_at": trade.get("opened_at", "-"),
                    "remaining": str(int(float(trade.get("remaining_position_pct", 0) or 0))) + "%",
                    "tp1_hit": bool(trade.get("tp1_hit")),
                    "tp2_hit": bool(trade.get("tp2_hit")),
                    "market_overview": list(trade.get("market_overview", []) or []),
                    "reasons": list(trade.get("reasons", []) or []),
                }
    return latest


def build_fake_breakout_warning(bundle_15m: Dict[str, Any]) -> Dict[str, str]:
    overview = bundle_15m["overview"]
    breakout_text = overview.breakout_check
    if breakout_text.startswith("Fake breakout"):
        status = "HIGH RISK"
        detail = breakout_text
    elif breakout_text.startswith("Real breakout"):
        status = "CLEAR"
        detail = breakout_text
    else:
        status = "NEUTRAL"
        detail = breakout_text

    return {
        "status": status,
        "detail": detail,
        "trend": overview.trend,
        "verdict": overview.entry_rule,
    }


def build_session_status() -> List[Dict[str, str]]:
    now_utc = datetime.now(timezone.utc).time()
    sessions = [
        ("London", time(7, 0), time(16, 0)),
        ("New York", time(12, 0), time(21, 0)),
        ("Asia", time(0, 0), time(9, 0)),
    ]

    items: List[Dict[str, str]] = []
    for name, start, end in sessions:
        active = start <= now_utc <= end
        items.append(
            {
                "name": name,
                "status": "ACTIVE" if active else "QUIET",
                "window": f"{start.strftime('%H:%M')} - {end.strftime('%H:%M')} UTC",
            }
        )
    return items


def build_candles_chart_data(candles: List[Any]) -> List[Dict[str, Any]]:
    data: List[Dict[str, Any]] = []
    for candle in candles[-72:]:
        data.append(
            {
                "open_time": candle.open_time,
                "time": format_candle_time(candle.open_time),
                "open": candle.open,
                "high": candle.high,
                "low": candle.low,
                "close": candle.close,
            }
        )
    return data


def build_win_loss_chart(state: Dict[str, Any]) -> List[Dict[str, Any]]:
    overall = state.get("performance", {}).get("overall", {}) if isinstance(state.get("performance", {}), dict) else {}
    return [
        {"label": "Wins", "value": int(overall.get("wins", 0) or 0)},
        {"label": "Losses", "value": int(overall.get("losses", 0) or 0)},
        {"label": "Breakeven", "value": int(overall.get("breakeven", 0) or 0)},
    ]


def build_signal_history_chart(state: Dict[str, Any]) -> List[Dict[str, Any]]:
    items = build_recent_closures_from_db(limit=16)
    chart: List[Dict[str, Any]] = []
    for item in items:
        chart.append(
            {
                "label": f"{item.get('symbol', '-')}-{item.get('interval', '-')}",
                "value": float(item.get("result_r", 0) or 0),
            }
        )
    return chart


def build_user_signal_help_payload() -> Dict[str, str]:
    return {
        "title": "User Signal Checker",
        "help": trading_bot.build_user_signal_help_message(),
    }


def build_bot_mirror_panels(state: Dict[str, Any], selected_symbol: str) -> Dict[str, str]:
    selected_config = build_analysis_config(selected_symbol, "5m")
    return {
        "status": trading_bot.build_status_message(selected_config, state),
        "active_trades": trading_bot.build_active_trades_message(state),
        "accuracy": trading_bot.build_accuracy_message(state),
        "daily_report": trading_bot.build_daily_report_message(
            selected_config,
            trading_bot.current_local_date(),
            state,
        ),
        "market_status": trading_bot.build_market_message(selected_config),
        "hourly_update": trading_bot.build_hourly_update_message(selected_config),
        "signal_checker": trading_bot.build_signal_checker_message(selected_config),
    }


def build_chart_payload(symbol: str, interval: str) -> Dict[str, Any]:
    bundle = fetch_market_bundle(symbol, interval)
    return {
        "symbol": symbol,
        "interval": interval,
        "price": bundle["price_text"],
        "change": bundle["change_text"],
        "trend": bundle["overview"].trend,
        "breakout": bundle["overview"].breakout_check,
        "verdict": bundle["overview"].entry_rule,
        "candles": build_candles_chart_data(bundle["candles"]),
    }


def build_dashboard_payload(selected_symbol: Optional[str] = None) -> Dict[str, Any]:
    state = load_state()
    sync_signal_history_database(state)
    symbols = parse_env_list("BOT_SYMBOLS", DEFAULT_SYMBOLS)
    intervals = parse_env_list("BOT_INTERVALS", DEFAULT_INTERVALS)
    selected = selected_symbol or symbols[0]
    signal_grid = build_signal_grid(state)

    bundle_5m = fetch_market_bundle(selected, "5m")
    bundle_15m = fetch_market_bundle(selected, "15m")

    return {
        "timestamp": datetime.now().astimezone().strftime("%Y-%m-%d %I:%M:%S %p"),
        "mode": "Real Breakout Scalping",
        "selected_symbol": selected,
        "symbols": symbols,
        "intervals": intervals,
        "overview_cards": build_overview_cards(state, signal_grid),
        "ticker_rows": build_ticker_rows(symbols),
        "signal_grid": signal_grid,
        "latest_signal": get_latest_real_signal(state),
        "fake_breakout_warning": build_fake_breakout_warning(bundle_15m),
        "recent_closures": build_recent_closures_from_db(),
        "accuracy": build_accuracy_panels(state),
        "win_loss_chart": build_win_loss_chart(state),
        "signal_history_chart": build_signal_history_chart(state),
        "signal_history": load_signal_history(limit=16),
        "session_status": build_session_status(),
        "bot_mirror": build_bot_mirror_panels(state, selected),
        "market_cards": [
            {
                "interval": "5m",
                "price": bundle_5m["price_text"],
                "change": bundle_5m["change_text"],
                "trend": bundle_5m["overview"].trend,
                "breakout": bundle_5m["overview"].breakout_check,
                "verdict": bundle_5m["overview"].entry_rule,
                "momentum": bundle_5m["overview"].volume_momentum,
            },
            {
                "interval": "15m",
                "price": bundle_15m["price_text"],
                "change": bundle_15m["change_text"],
                "trend": bundle_15m["overview"].trend,
                "breakout": bundle_15m["overview"].breakout_check,
                "verdict": bundle_15m["overview"].entry_rule,
                "momentum": bundle_15m["overview"].volume_momentum,
            },
        ],
        "candles_chart": build_chart_payload(selected, "5m"),
        "user_signal_checker": build_user_signal_help_payload(),
    }


@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request, symbol: Optional[str] = Query(default=None)) -> HTMLResponse:
    payload = build_dashboard_payload(symbol)
    return templates.TemplateResponse(
        request,
        "dashboard.html",
        {"request": request, "payload": payload},
    )


@app.get("/api/dashboard", response_class=JSONResponse)
async def dashboard_api(symbol: Optional[str] = Query(default=None)) -> JSONResponse:
    return JSONResponse(build_dashboard_payload(symbol))


@app.get("/api/chart", response_class=JSONResponse)
async def chart_api(
    symbol: Optional[str] = Query(default=None),
    interval: str = Query(default="5m"),
) -> JSONResponse:
    symbols = parse_env_list("BOT_SYMBOLS", DEFAULT_SYMBOLS)
    selected_symbol = symbol or symbols[0]
    return JSONResponse(build_chart_payload(selected_symbol, interval))


@app.post("/api/check-user-signal", response_class=JSONResponse)
async def check_user_signal(payload: UserSignalCheckRequest) -> JSONResponse:
    config = build_analysis_config(
        payload.symbol or parse_env_list("BOT_SYMBOLS", DEFAULT_SYMBOLS)[0],
        payload.interval or "5m",
    )
    parsed_signal = trading_bot.parse_user_signal_text(
        payload.text,
        config,
        default_symbol=payload.symbol or "",
        default_interval=payload.interval or "",
        default_side=payload.side or "",
    )
    structure_verdict, structure_note = trading_bot.assess_user_signal_structure(parsed_signal)
    message = trading_bot.build_user_signal_check_message(
        config,
        parsed_signal.symbol,
        parsed_signal.interval,
        parsed_signal.side or None,
        parsed_signal,
        structure_verdict,
        structure_note,
    )
    return JSONResponse(
        {
            "verdict": structure_verdict,
            "note": structure_note,
            "message": message,
        }
    )


init_history_db()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("dashboard:app", host="0.0.0.0", port=int(os.getenv("PORT", "8000")), reload=False)
