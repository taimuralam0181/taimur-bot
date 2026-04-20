import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import requests

import bot
import self_learning


DEFAULT_BOOTSTRAP_SYMBOL = os.getenv("BOT_BOOTSTRAP_SYMBOL", "BTCUSDT").strip().upper() or "BTCUSDT"
DEFAULT_BOOTSTRAP_INTERVALS = [
    item.strip()
    for item in os.getenv("BOT_BOOTSTRAP_INTERVALS", "5m,15m").split(",")
    if item.strip()
]
DEFAULT_LOOKBACK_DAYS = max(1, int(os.getenv("BOT_BOOTSTRAP_DAYS", "30")))
HORIZON_CANDLES = {
    "5m": int(os.getenv("BOT_BOOTSTRAP_5M_HORIZON", "24")),
    "15m": int(os.getenv("BOT_BOOTSTRAP_15M_HORIZON", "12")),
}


def build_bootstrap_config(symbol: str, interval: str) -> bot.Config:
    return bot.Config(
        telegram_token=os.getenv("BOT_TOKEN", "bootstrap"),
        telegram_chat_id=os.getenv("CHAT_ID", ""),
        symbol=symbol,
        symbols=[symbol],
        interval=interval,
        intervals=[interval],
        poll_seconds=int(os.getenv("BOT_POLL_SECONDS", "15")),
        lookback_limit=int(os.getenv("BOT_LOOKBACK_LIMIT", "260")),
        ema_fast_period=int(os.getenv("BOT_EMA_FAST", "20")),
        ema_slow_period=int(os.getenv("BOT_EMA_SLOW", "50")),
        rsi_period=int(os.getenv("BOT_RSI_PERIOD", "14")),
        atr_period=int(os.getenv("BOT_ATR_PERIOD", "14")),
        sr_lookback=int(os.getenv("BOT_SR_LOOKBACK", "20")),
        volume_period=int(os.getenv("BOT_VOLUME_PERIOD", "20")),
        volume_spike_factor=float(os.getenv("BOT_VOLUME_SPIKE_FACTOR", "1.15")),
        breakout_buffer_pct=float(os.getenv("BOT_BREAKOUT_BUFFER_PCT", "0.0008")),
        cooldown_candles=int(os.getenv("BOT_COOLDOWN_CANDLES", "4")),
        min_signal_score=int(os.getenv("BOT_MIN_SIGNAL_SCORE", "74")),
        vip_signal_score=int(os.getenv("BOT_VIP_SIGNAL_SCORE", "84")),
        normal_risk_pct=float(os.getenv("BOT_NORMAL_RISK_PCT", "0.5")),
        vip_risk_pct=float(os.getenv("BOT_VIP_RISK_PCT", "0.8")),
        normal_leverage=os.getenv("BOT_NORMAL_LEVERAGE", "3x-5x"),
        vip_leverage=os.getenv("BOT_VIP_LEVERAGE", "5x-8x"),
        margin_mode=os.getenv("BOT_MARGIN_MODE", "Isolated"),
        require_higher_timeframe_confirmation=os.getenv(
            "BOT_REQUIRE_HTF_CONFIRMATION",
            "true",
        ).strip().lower() in {"1", "true", "yes", "on"},
        watch_alert_enabled=False,
        watch_alert_score_gap=int(os.getenv("BOT_WATCH_ALERT_SCORE_GAP", "8")),
        max_extension_atr=float(os.getenv("BOT_MAX_EXTENSION_ATR", "2.0")),
        atr_stop_multiplier=float(os.getenv("BOT_ATR_STOP_MULTIPLIER", "1.0")),
        tp_one_r=float(os.getenv("BOT_TP_ONE_R", "0.7")),
        tp_two_r=float(os.getenv("BOT_TP_TWO_R", "1.2")),
        tp_three_r=float(os.getenv("BOT_TP_THREE_R", "1.8")),
        hourly_update_enabled=False,
        hourly_update_interval_minutes=int(os.getenv("BOT_HOURLY_UPDATE_INTERVAL_MINUTES", "60")),
        hourly_update_timeframe=os.getenv("BOT_HOURLY_UPDATE_TIMEFRAME", "15m"),
        daily_report_hour=int(os.getenv("BOT_DAILY_REPORT_HOUR", "23")),
        state_file=bot.STATE_FILE,
        adaptive_learning_enabled=False,
        adaptive_learning_max_adjustment=int(os.getenv("BOT_ADAPTIVE_LEARNING_MAX_ADJUSTMENT", "6")),
    )


def fetch_historical_klines(symbol: str, interval: str, start_ms: int, end_ms: int) -> List[bot.Candle]:
    candles: List[bot.Candle] = []
    cursor = start_ms

    while cursor < end_ms:
        last_error: Optional[Exception] = None
        payload = None
        for market_url in bot.market_data_urls():
            try:
                response = requests.get(
                    market_url,
                    params={
                        "symbol": symbol,
                        "interval": interval,
                        "startTime": cursor,
                        "endTime": end_ms,
                        "limit": 1000,
                    },
                    timeout=20,
                )
                response.raise_for_status()
                payload = response.json()
                break
            except Exception as exc:  # pragma: no cover - runtime network path
                last_error = exc

        if payload is None:
            raise RuntimeError(f"Historical fetch failed for {symbol} {interval}: {last_error}") from last_error

        if not payload:
            break

        batch: List[bot.Candle] = []
        for item in payload:
            batch.append(
                bot.Candle(
                    open_time=int(item[0]),
                    open=float(item[1]),
                    high=float(item[2]),
                    low=float(item[3]),
                    close=float(item[4]),
                    volume=float(item[5]),
                    close_time=int(item[6]),
                )
            )

        candles.extend(batch)
        cursor = batch[-1].close_time + 1
        if len(batch) < 1000:
            break

    deduped: List[bot.Candle] = []
    seen = set()
    for candle in candles:
        if candle.open_time in seen:
            continue
        seen.add(candle.open_time)
        deduped.append(candle)
    return deduped


def history_slice(candles: List[bot.Candle], close_time: int, limit: int) -> List[bot.Candle]:
    eligible = [candle for candle in candles if candle.close_time <= close_time]
    return eligible[-limit:]


def simulate_signal_outcome(signal: bot.Signal, future_candles: List[bot.Candle]) -> Dict[str, object]:
    risk = abs(signal.entry - signal.stop_loss)
    if risk <= 1e-9:
        return {
            "outcome": "breakeven",
            "result_r": 0.0,
            "tp1_hit": False,
            "tp2_hit": False,
            "close_reason": "BOOTSTRAP_INVALID_RISK",
            "closed_at": datetime.now().astimezone().isoformat(),
        }

    tp1 = signal.take_profits[0] if len(signal.take_profits) > 0 else signal.entry
    tp2 = signal.take_profits[1] if len(signal.take_profits) > 1 else tp1
    tp3 = signal.take_profits[2] if len(signal.take_profits) > 2 else tp2
    tp1_hit = False
    tp2_hit = False
    result_r = 0.0
    outcome = "breakeven"
    close_reason = "BOOTSTRAP_TIMEOUT"
    closed_time = signal.candle_time

    for candle in future_candles:
        closed_time = candle.close_time
        if signal.side == "LONG":
            if candle.low <= signal.stop_loss and not tp1_hit:
                return {
                    "outcome": "losses",
                    "result_r": -1.0,
                    "tp1_hit": False,
                    "tp2_hit": False,
                    "close_reason": "BOOTSTRAP_STOP_LOSS",
                    "closed_at": datetime.fromtimestamp(closed_time / 1000).astimezone().isoformat(),
                }
            if candle.high >= tp3:
                return {
                    "outcome": "wins",
                    "result_r": round(max(result_r, 1.8), 4),
                    "tp1_hit": True,
                    "tp2_hit": True,
                    "close_reason": "BOOTSTRAP_TP3_HIT",
                    "closed_at": datetime.fromtimestamp(closed_time / 1000).astimezone().isoformat(),
                }
            if candle.high >= tp2:
                tp1_hit = True
                tp2_hit = True
                result_r = max(result_r, 1.2)
                outcome = "wins"
                close_reason = "BOOTSTRAP_TP2_HIT"
            elif candle.high >= tp1:
                tp1_hit = True
                result_r = max(result_r, 0.7)
                outcome = "wins"
                close_reason = "BOOTSTRAP_TP1_HIT"
        else:
            if candle.high >= signal.stop_loss and not tp1_hit:
                return {
                    "outcome": "losses",
                    "result_r": -1.0,
                    "tp1_hit": False,
                    "tp2_hit": False,
                    "close_reason": "BOOTSTRAP_STOP_LOSS",
                    "closed_at": datetime.fromtimestamp(closed_time / 1000).astimezone().isoformat(),
                }
            if candle.low <= tp3:
                return {
                    "outcome": "wins",
                    "result_r": round(max(result_r, 1.8), 4),
                    "tp1_hit": True,
                    "tp2_hit": True,
                    "close_reason": "BOOTSTRAP_TP3_HIT",
                    "closed_at": datetime.fromtimestamp(closed_time / 1000).astimezone().isoformat(),
                }
            if candle.low <= tp2:
                tp1_hit = True
                tp2_hit = True
                result_r = max(result_r, 1.2)
                outcome = "wins"
                close_reason = "BOOTSTRAP_TP2_HIT"
            elif candle.low <= tp1:
                tp1_hit = True
                result_r = max(result_r, 0.7)
                outcome = "wins"
                close_reason = "BOOTSTRAP_TP1_HIT"

    if future_candles:
        last_close = future_candles[-1].close
        directional_move_r = (
            (last_close - signal.entry) / risk
            if signal.side == "LONG"
            else (signal.entry - last_close) / risk
        )
        if not tp1_hit:
            result_r = round(directional_move_r, 4)
            outcome = "wins" if result_r > bot.RESULT_EPSILON_R else "losses" if result_r < -bot.RESULT_EPSILON_R else "breakeven"
            close_reason = "BOOTSTRAP_TIMEOUT_CLOSE"

    return {
        "outcome": outcome,
        "result_r": round(result_r, 4),
        "tp1_hit": tp1_hit,
        "tp2_hit": tp2_hit,
        "close_reason": close_reason,
        "closed_at": datetime.fromtimestamp(closed_time / 1000).astimezone().isoformat(),
    }


def bootstrap_interval(
    symbol: str,
    interval: str,
    candles: List[bot.Candle],
    context_map: Dict[str, List[bot.Candle]],
) -> Tuple[int, int]:
    config = build_bootstrap_config(symbol, interval)
    minimum_required = bot.minimum_required_candles(config)
    horizon = max(4, HORIZON_CANDLES.get(interval, 12))
    imported = 0
    scanned = 0

    for index in range(minimum_required, len(candles) - 1):
        scanned += 1
        window = candles[max(0, index - config.lookback_limit + 1) : index + 1]

        higher_trend = None
        higher_overview = None
        higher_interval = bot.confirmation_interval(interval)
        if higher_interval:
            higher_candles = history_slice(
                context_map.get(higher_interval, []),
                candles[index].close_time,
                config.lookback_limit,
            )
            if higher_candles:
                higher_config = build_bootstrap_config(symbol, higher_interval)
                if len(higher_candles) >= bot.minimum_required_candles(higher_config):
                    higher_trend = bot.calculate_trend_snapshot(higher_candles, higher_config)
                    higher_overview = bot.calculate_market_overview_for_candles(higher_candles, higher_config)

        analysis = bot.analyze_market(
            window,
            config,
            higher_trend,
            higher_overview,
            state=None,
        )
        if analysis.signal is None:
            continue

        signal = analysis.signal
        signal_key = f"bootstrap|{symbol}|{interval}|{signal.side}|{signal.candle_time}"
        features = signal.features or {}
        tp_values = list(signal.take_profits) + [signal.entry, signal.entry, signal.entry]

        self_learning.record_signal(
            signal_key,
            {
                "symbol": symbol,
                "interval": interval,
                "side": signal.side,
                "candle_time": signal.candle_time,
                "opened_at": datetime.fromtimestamp(signal.candle_time / 1000).astimezone().isoformat(),
                "tier": signal.tier,
                "grade": signal.grade,
                "score_base": int(features.get("score_before_learning", signal.score) or signal.score),
                "score_final": signal.score,
                "entry": signal.entry,
                "stop_loss": signal.stop_loss,
                "tp1": tp_values[0],
                "tp2": tp_values[1],
                "tp3": tp_values[2],
                "trend_bias": analysis.market_overview.trend_bias,
                "breakout_type": analysis.market_overview.breakout_check,
                "entry_rule": analysis.market_overview.entry_rule,
                "volume_ratio": features.get("volume_ratio", 0.0),
                "rsi": features.get("rsi", 0.0),
                "body_ratio": features.get("body_ratio", 0.0),
                "extension_atr": features.get("extension_atr", 0.0),
                "htf_confirmed": features.get("htf_confirmed", False),
                "htf_fake_breakout": features.get("htf_fake_breakout", False),
                "structure_confirmed": features.get("structure_confirmed", False),
            },
        )

        outcome = simulate_signal_outcome(signal, candles[index + 1 : index + 1 + horizon])
        self_learning.close_signal(signal_key, outcome)
        imported += 1

    return imported, scanned


def main() -> None:
    self_learning.init_dataset_db()

    end_time = datetime.now().astimezone()
    start_time = end_time - timedelta(days=DEFAULT_LOOKBACK_DAYS)
    start_ms = int(start_time.timestamp() * 1000)
    end_ms = int(end_time.timestamp() * 1000)

    required_intervals = set(DEFAULT_BOOTSTRAP_INTERVALS)
    for interval in list(required_intervals):
        higher_interval = bot.confirmation_interval(interval)
        if higher_interval:
            required_intervals.add(higher_interval)

    candle_map: Dict[str, List[bot.Candle]] = {}
    for interval in sorted(required_intervals, key=bot.interval_to_milliseconds):
        candles = fetch_historical_klines(DEFAULT_BOOTSTRAP_SYMBOL, interval, start_ms, end_ms)
        candle_map[interval] = candles
        print(f"Fetched {len(candles)} candles for {DEFAULT_BOOTSTRAP_SYMBOL} {interval}")

    total_imported = 0
    total_scanned = 0
    for interval in DEFAULT_BOOTSTRAP_INTERVALS:
        imported, scanned = bootstrap_interval(
            DEFAULT_BOOTSTRAP_SYMBOL,
            interval,
            candle_map.get(interval, []),
            candle_map,
        )
        total_imported += imported
        total_scanned += scanned
        print(f"Bootstrapped {imported} signals from {DEFAULT_BOOTSTRAP_SYMBOL} {interval} across {scanned} windows")

    model = self_learning.train_model()
    print("")
    print(self_learning.build_training_report(model))
    print("")
    print(f"Bootstrap complete: {total_imported} signals imported from {total_scanned} windows")


if __name__ == "__main__":
    main()
