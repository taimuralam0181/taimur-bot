import asyncio
import copy
import json
import logging
import os
import re
import threading
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
)

import self_learning


LOGGER = logging.getLogger("pro_trader_bot")
DEFAULT_MARKET_DATA_URLS = [
    "https://data-api.binance.vision/api/v3/klines",
    "https://api.binance.com/api/v3/klines",
    "https://api1.binance.com/api/v3/klines",
    "https://api2.binance.com/api/v3/klines",
    "https://api3.binance.com/api/v3/klines",
]
STATE_FILE = Path("bot_state.json")
TP1_PARTIAL_PCT = 40
TP2_PARTIAL_PCT = 30
TP3_PARTIAL_PCT = 30
RESULT_EPSILON_R = 0.05
DEFAULT_SYMBOLS = [
    "BTCUSDT",
    "ETHUSDT",
    "SOLUSDT",
]
DEFAULT_INTERVALS = [
    "5m",
    "15m",
]
USER_SIGNAL_INPUT = 1
USER_SIGNAL_TEMPLATE = (
    "Send your signal in this format.\n"
    "Bot will also try to auto-detect other formats.\n\n"
    "PAIR: BTCUSDT\n"
    "TIMEFRAME: 15m\n"
    "SIDE: SHORT\n"
    "ENTRY: 69800-70300\n"
    "SL: 71000\n"
    "TP1: 68800\n"
    "TP2: 68000\n"
    "TP3: 66800"
)
USER_SIGNAL_FIELD_LOOKAHEAD = (
    r"(?=\b(?:PAIR|SYMBOL|COIN|TIMEFRAME|TF|SIDE|ENTRY(?:\s+ZONE)?|BUY\s+ZONE|SELL\s+ZONE|"
    r"STOP(?:\s+LOSS)?|SL|TP\s*1|TP\s*2|TP\s*3|TARGET\s*1|TARGET\s*2|TARGET\s*3|TARGETS?|TPS?)\b|$)"
)


@dataclass
class Config:
    telegram_token: str
    telegram_chat_id: str
    symbol: str
    symbols: List[str]
    interval: str
    intervals: List[str]
    poll_seconds: int
    lookback_limit: int
    ema_fast_period: int
    ema_slow_period: int
    rsi_period: int
    atr_period: int
    sr_lookback: int
    volume_period: int
    volume_spike_factor: float
    breakout_buffer_pct: float
    cooldown_candles: int
    min_signal_score: int
    vip_signal_score: int
    normal_risk_pct: float
    vip_risk_pct: float
    normal_leverage: str
    vip_leverage: str
    margin_mode: str
    require_higher_timeframe_confirmation: bool
    watch_alert_enabled: bool
    watch_alert_score_gap: int
    max_extension_atr: float
    atr_stop_multiplier: float
    tp_one_r: float
    tp_two_r: float
    tp_three_r: float
    hourly_update_enabled: bool
    hourly_update_interval_minutes: int
    hourly_update_timeframe: str
    daily_report_hour: int
    state_file: Path
    adaptive_learning_enabled: bool
    adaptive_learning_max_adjustment: int


@dataclass
class Candle:
    open_time: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    close_time: int


@dataclass
class Signal:
    tier: str
    grade: str
    setup_type: str
    setup_note: str
    verdict: str
    side: str
    score: int
    entry: float
    stop_loss: float
    take_profits: List[float]
    reasons: List[str]
    candle_time: int
    market_structure_level: float
    atr: float
    market_overview: List[str] = field(default_factory=list)
    features: Dict[str, object] = field(default_factory=dict)


@dataclass
class TrendSnapshot:
    interval: str
    bias: str
    close: float
    ema_fast: float
    ema_slow: float
    rsi: float


@dataclass
class MarketOverview:
    trend: str
    support_zone: str
    resistance_zone: str
    entry_condition: str
    rejection_candle: str
    breakout_check: str
    volume_momentum: str
    entry_rule: str
    trend_bias: str
    entry_zone_side: str
    bullish_rejection_valid: bool
    bearish_rejection_valid: bool
    strong_bullish_candle: bool
    strong_bearish_candle: bool


@dataclass
class AnalysisResult:
    signal: Optional[Signal]
    market_overview: MarketOverview
    long_score: int
    short_score: int


@dataclass
class UserSignalInput:
    symbol: str
    interval: str
    side: str = ""
    entry_low: Optional[float] = None
    entry_high: Optional[float] = None
    stop_loss: Optional[float] = None
    take_profits: List[float] = field(default_factory=list)
    notes: List[str] = field(default_factory=list)
    raw_text: str = ""


def parse_intervals() -> List[str]:
    intervals_value = os.getenv("BOT_INTERVALS", "").strip()
    if intervals_value:
        intervals = [item.strip() for item in intervals_value.split(",") if item.strip()]
    else:
        single_interval = os.getenv("BOT_INTERVAL", "").strip()
        intervals = [single_interval] if single_interval else DEFAULT_INTERVALS

    deduplicated: List[str] = []
    for interval in intervals:
        if interval not in deduplicated:
            deduplicated.append(interval)

    if not deduplicated:
        raise SystemExit("No valid timeframe configured. Set BOT_INTERVALS or BOT_INTERVAL.")

    return deduplicated


def parse_symbols() -> List[str]:
    symbols_value = os.getenv("BOT_SYMBOLS", "").strip()
    if symbols_value:
        symbols = [item.strip().upper() for item in symbols_value.split(",") if item.strip()]
    else:
        single_symbol = os.getenv("BOT_SYMBOL", "").strip().upper()
        symbols = [single_symbol] if single_symbol else DEFAULT_SYMBOLS

    deduplicated: List[str] = []
    for symbol in symbols:
        if symbol not in deduplicated:
            deduplicated.append(symbol)

    if not deduplicated:
        raise SystemExit("No valid symbol configured. Set BOT_SYMBOLS or BOT_SYMBOL.")

    return deduplicated


def parse_bool_env(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default

    return value.strip().lower() in {"1", "true", "yes", "on"}


def market_data_urls() -> List[str]:
    configured_urls = os.getenv("BOT_MARKET_DATA_URLS", "").strip()
    if configured_urls:
        raw_urls = [item.strip() for item in configured_urls.split(",") if item.strip()]
    else:
        raw_urls = DEFAULT_MARKET_DATA_URLS

    deduplicated: List[str] = []
    for url in raw_urls:
        if url not in deduplicated:
            deduplicated.append(url)

    return deduplicated


def load_config() -> Config:
    token = os.getenv("BOT_TOKEN", "").strip()
    chat_id = os.getenv("CHAT_ID", "").strip()

    if not token:
        raise SystemExit("Missing BOT_TOKEN environment variable.")

    symbols = parse_symbols()
    intervals = parse_intervals()

    return Config(
        telegram_token=token,
        telegram_chat_id=chat_id,
        symbol=symbols[0],
        symbols=symbols,
        interval=intervals[0],
        intervals=intervals,
        poll_seconds=int(os.getenv("BOT_POLL_SECONDS", "15")),
        lookback_limit=int(os.getenv("BOT_LOOKBACK_LIMIT", "260")),
        ema_fast_period=int(os.getenv("BOT_EMA_FAST", "20")),
        ema_slow_period=int(os.getenv("BOT_EMA_SLOW", "50")),
        rsi_period=int(os.getenv("BOT_RSI_PERIOD", "14")),
        atr_period=int(os.getenv("BOT_ATR_PERIOD", "14")),
        sr_lookback=int(os.getenv("BOT_SR_LOOKBACK", "12")),
        volume_period=int(os.getenv("BOT_VOLUME_PERIOD", "14")),
        volume_spike_factor=float(os.getenv("BOT_VOLUME_SPIKE_FACTOR", "1.08")),
        breakout_buffer_pct=float(os.getenv("BOT_BREAKOUT_BUFFER_PCT", "0.0009")),
        cooldown_candles=int(os.getenv("BOT_COOLDOWN_CANDLES", "5")),
        min_signal_score=int(os.getenv("BOT_MIN_SIGNAL_SCORE", "78")),
        vip_signal_score=int(os.getenv("BOT_VIP_SIGNAL_SCORE", "88")),
        normal_risk_pct=float(os.getenv("BOT_NORMAL_RISK_PCT", "0.35")),
        vip_risk_pct=float(os.getenv("BOT_VIP_RISK_PCT", "0.55")),
        normal_leverage=os.getenv("BOT_NORMAL_LEVERAGE", "5x-8x").strip(),
        vip_leverage=os.getenv("BOT_VIP_LEVERAGE", "8x-12x").strip(),
        margin_mode=os.getenv("BOT_MARGIN_MODE", "Isolated").strip() or "Isolated",
        require_higher_timeframe_confirmation=parse_bool_env(
            "BOT_REQUIRE_HTF_CONFIRMATION", True
        ),
        watch_alert_enabled=parse_bool_env("BOT_WATCH_ALERT_ENABLED", True),
        watch_alert_score_gap=int(os.getenv("BOT_WATCH_ALERT_SCORE_GAP", "8")),
        max_extension_atr=float(os.getenv("BOT_MAX_EXTENSION_ATR", "1.2")),
        atr_stop_multiplier=float(os.getenv("BOT_ATR_STOP_MULTIPLIER", "1.0")),
        tp_one_r=float(os.getenv("BOT_TP1_R", "0.7")),
        tp_two_r=float(os.getenv("BOT_TP2_R", "1.2")),
        tp_three_r=float(os.getenv("BOT_TP3_R", "1.8")),
        hourly_update_enabled=parse_bool_env("BOT_HOURLY_UPDATE_ENABLED", True),
        hourly_update_interval_minutes=int(os.getenv("BOT_HOURLY_UPDATE_MINUTES", "60")),
        hourly_update_timeframe=os.getenv("BOT_HOURLY_UPDATE_TIMEFRAME", "5m").strip() or "5m",
        daily_report_hour=int(os.getenv("BOT_DAILY_REPORT_HOUR", "23")),
        state_file=Path(os.getenv("BOT_STATE_FILE", str(STATE_FILE))).expanduser(),
        adaptive_learning_enabled=parse_bool_env("BOT_ADAPTIVE_LEARNING_ENABLED", True),
        adaptive_learning_max_adjustment=int(os.getenv("BOT_ADAPTIVE_LEARNING_MAX_ADJUSTMENT", "8")),
    )


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )


def load_state(path: Path) -> Dict[str, object]:
    if not path.exists():
        return {}

    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        LOGGER.warning("State file could not be loaded: %s", exc)
        return {}


def save_state(path: Path, state: Dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(f"{path.suffix}.tmp")
    temp_path.write_text(json.dumps(state, indent=2), encoding="utf-8")
    temp_path.replace(path)


def replace_state(target: Dict[str, object], source: Dict[str, object]) -> None:
    target.clear()
    target.update(source)


def get_symbol_state(state: Dict[str, object], symbol: str) -> Dict[str, object]:
    symbols_state = state.setdefault("symbols", {})
    if not isinstance(symbols_state, dict):
        symbols_state = {}
        state["symbols"] = symbols_state

    symbol_state = symbols_state.setdefault(symbol, {})
    if not isinstance(symbol_state, dict):
        symbol_state = {}
        symbols_state[symbol] = symbol_state

    return symbol_state


def get_interval_state(state: Dict[str, object], interval: str) -> Dict[str, object]:
    intervals_state = state.setdefault("intervals", {})
    if not isinstance(intervals_state, dict):
        intervals_state = {}
        state["intervals"] = intervals_state

    interval_state = intervals_state.setdefault(interval, {})
    if not isinstance(interval_state, dict):
        interval_state = {}
        intervals_state[interval] = interval_state

    return interval_state


def minimum_required_candles(config: Config) -> int:
    return max(
        config.ema_slow_period + 2,
        config.rsi_period + 2,
        config.atr_period + 2,
        config.sr_lookback + 2,
        config.volume_period + 2,
    )


def interval_to_milliseconds(interval: str) -> int:
    unit = interval[-1]
    value = int(interval[:-1])
    mapping = {
        "m": 60_000,
        "h": 3_600_000,
        "d": 86_400_000,
        "w": 604_800_000,
        "M": 2_592_000_000,
    }

    if unit not in mapping:
        raise ValueError(f"Unsupported interval: {interval}")

    return value * mapping[unit]


def send_telegram(message: str, config: Config) -> None:
    if not config.telegram_chat_id:
        LOGGER.warning("CHAT_ID is not configured. Skipping outbound alert message.")
        return

    url = f"https://api.telegram.org/bot{config.telegram_token}/sendMessage"
    response = requests.post(
        url,
        data={"chat_id": config.telegram_chat_id, "text": message},
        timeout=15,
    )
    response.raise_for_status()


def config_for_interval(config: Config, interval: str) -> Config:
    return Config(
        telegram_token=config.telegram_token,
        telegram_chat_id=config.telegram_chat_id,
        symbol=config.symbol,
        symbols=config.symbols,
        interval=interval,
        intervals=config.intervals,
        poll_seconds=config.poll_seconds,
        lookback_limit=config.lookback_limit,
        ema_fast_period=config.ema_fast_period,
        ema_slow_period=config.ema_slow_period,
        rsi_period=config.rsi_period,
        atr_period=config.atr_period,
        sr_lookback=config.sr_lookback,
        volume_period=config.volume_period,
        volume_spike_factor=config.volume_spike_factor,
        breakout_buffer_pct=config.breakout_buffer_pct,
        cooldown_candles=config.cooldown_candles,
        min_signal_score=config.min_signal_score,
        vip_signal_score=config.vip_signal_score,
        normal_risk_pct=config.normal_risk_pct,
        vip_risk_pct=config.vip_risk_pct,
        normal_leverage=config.normal_leverage,
        vip_leverage=config.vip_leverage,
        margin_mode=config.margin_mode,
        require_higher_timeframe_confirmation=config.require_higher_timeframe_confirmation,
        watch_alert_enabled=config.watch_alert_enabled,
        watch_alert_score_gap=config.watch_alert_score_gap,
        max_extension_atr=config.max_extension_atr,
        atr_stop_multiplier=config.atr_stop_multiplier,
        tp_one_r=config.tp_one_r,
        tp_two_r=config.tp_two_r,
        tp_three_r=config.tp_three_r,
        hourly_update_enabled=config.hourly_update_enabled,
        hourly_update_interval_minutes=config.hourly_update_interval_minutes,
        hourly_update_timeframe=config.hourly_update_timeframe,
        daily_report_hour=config.daily_report_hour,
        state_file=config.state_file,
        adaptive_learning_enabled=config.adaptive_learning_enabled,
        adaptive_learning_max_adjustment=config.adaptive_learning_max_adjustment,
    )


def config_for_symbol(config: Config, symbol: str) -> Config:
    return Config(
        telegram_token=config.telegram_token,
        telegram_chat_id=config.telegram_chat_id,
        symbol=symbol,
        symbols=config.symbols,
        interval=config.interval,
        intervals=config.intervals,
        poll_seconds=config.poll_seconds,
        lookback_limit=config.lookback_limit,
        ema_fast_period=config.ema_fast_period,
        ema_slow_period=config.ema_slow_period,
        rsi_period=config.rsi_period,
        atr_period=config.atr_period,
        sr_lookback=config.sr_lookback,
        volume_period=config.volume_period,
        volume_spike_factor=config.volume_spike_factor,
        breakout_buffer_pct=config.breakout_buffer_pct,
        cooldown_candles=config.cooldown_candles,
        min_signal_score=config.min_signal_score,
        vip_signal_score=config.vip_signal_score,
        normal_risk_pct=config.normal_risk_pct,
        vip_risk_pct=config.vip_risk_pct,
        normal_leverage=config.normal_leverage,
        vip_leverage=config.vip_leverage,
        margin_mode=config.margin_mode,
        require_higher_timeframe_confirmation=config.require_higher_timeframe_confirmation,
        watch_alert_enabled=config.watch_alert_enabled,
        watch_alert_score_gap=config.watch_alert_score_gap,
        max_extension_atr=config.max_extension_atr,
        atr_stop_multiplier=config.atr_stop_multiplier,
        tp_one_r=config.tp_one_r,
        tp_two_r=config.tp_two_r,
        tp_three_r=config.tp_three_r,
        hourly_update_enabled=config.hourly_update_enabled,
        hourly_update_interval_minutes=config.hourly_update_interval_minutes,
        hourly_update_timeframe=config.hourly_update_timeframe,
        daily_report_hour=config.daily_report_hour,
        state_file=config.state_file,
        adaptive_learning_enabled=config.adaptive_learning_enabled,
        adaptive_learning_max_adjustment=config.adaptive_learning_max_adjustment,
    )


def fetch_klines(config: Config) -> List[Candle]:
    last_error: Optional[Exception] = None

    for market_url in market_data_urls():
        try:
            response = requests.get(
                market_url,
                params={
                    "symbol": config.symbol,
                    "interval": config.interval,
                    "limit": config.lookback_limit,
                },
                timeout=15,
            )
            response.raise_for_status()
            raw = response.json()

            candles = [
                Candle(
                    open_time=int(row[0]),
                    open=float(row[1]),
                    high=float(row[2]),
                    low=float(row[3]),
                    close=float(row[4]),
                    volume=float(row[5]),
                    close_time=int(row[6]),
                )
                for row in raw
            ]

            if len(candles) < 3:
                raise ValueError("Not enough market data returned from Binance.")

            # Binance may include the currently-forming candle as the last item.
            return candles[:-1]
        except requests.HTTPError as exc:
            status_code = exc.response.status_code if exc.response is not None else "unknown"
            LOGGER.warning(
                "Market data endpoint failed for %s %s on %s with status %s.",
                config.symbol,
                config.interval,
                market_url,
                status_code,
            )
            last_error = exc
        except requests.RequestException as exc:
            LOGGER.warning(
                "Market data request failed for %s %s on %s: %s",
                config.symbol,
                config.interval,
                market_url,
                exc,
            )
            last_error = exc

    if last_error:
        raise last_error

    raise RuntimeError("No market data endpoint is configured.")


def calculate_ema(values: List[float], period: int) -> List[float]:
    if len(values) < period:
        raise ValueError(f"EMA period {period} requires more data.")

    multiplier = 2 / (period + 1)
    ema_values = [sum(values[:period]) / period]

    for price in values[period:]:
        ema_values.append(((price - ema_values[-1]) * multiplier) + ema_values[-1])

    prefix = [ema_values[0]] * (period - 1)
    return prefix + ema_values


def calculate_rsi(values: List[float], period: int) -> List[float]:
    if len(values) <= period:
        raise ValueError(f"RSI period {period} requires more data.")

    gains: List[float] = []
    losses: List[float] = []

    for current, previous in zip(values[1:], values[:-1]):
        change = current - previous
        gains.append(max(change, 0))
        losses.append(abs(min(change, 0)))

    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    rsi_values = [50.0] * period

    for gain, loss in zip(gains[period:], losses[period:]):
        avg_gain = ((avg_gain * (period - 1)) + gain) / period
        avg_loss = ((avg_loss * (period - 1)) + loss) / period

        if avg_loss == 0:
            rsi_values.append(100.0)
            continue

        rs = avg_gain / avg_loss
        rsi_values.append(100 - (100 / (1 + rs)))

    return [50.0] + rsi_values


def calculate_atr(candles: List[Candle], period: int) -> List[float]:
    if len(candles) <= period:
        raise ValueError(f"ATR period {period} requires more data.")

    true_ranges: List[float] = []

    for index, candle in enumerate(candles):
        if index == 0:
            true_ranges.append(candle.high - candle.low)
            continue

        previous_close = candles[index - 1].close
        true_ranges.append(
            max(
                candle.high - candle.low,
                abs(candle.high - previous_close),
                abs(candle.low - previous_close),
            )
        )

    atr = sum(true_ranges[:period]) / period
    atr_values = [atr] * period

    for tr in true_ranges[period:]:
        atr = ((atr * (period - 1)) + tr) / period
        atr_values.append(atr)

    return atr_values


def calculate_macd(
    values: List[float],
    fast_period: int = 12,
    slow_period: int = 26,
    signal_period: int = 9,
) -> Tuple[List[float], List[float], List[float]]:
    if len(values) < slow_period + signal_period:
        raise ValueError("MACD requires more price history.")

    fast_ema = calculate_ema(values, fast_period)
    slow_ema = calculate_ema(values, slow_period)
    macd_line = [fast - slow for fast, slow in zip(fast_ema, slow_ema)]
    signal_line = calculate_ema(macd_line, signal_period)
    histogram = [macd - signal for macd, signal in zip(macd_line, signal_line)]
    return macd_line, signal_line, histogram


def average(values: List[float]) -> float:
    return sum(values) / len(values)


def round_price(price: float) -> float:
    if price >= 1000:
        return round(price, 2)
    if price >= 1:
        return round(price, 4)
    return round(price, 6)


def price_decimals(price: float) -> int:
    price = abs(price)
    if price >= 100:
        return 2
    if price >= 1:
        return 4
    if price >= 0.01:
        return 5
    return 6


def format_price(price: float) -> str:
    return f"{price:.{price_decimals(price)}f}"


def format_local_time(timestamp_ms: int) -> str:
    return datetime.fromtimestamp(timestamp_ms / 1000).astimezone().strftime(
        "%Y-%m-%d %I:%M:%S %p %Z"
    )


def format_now_local() -> str:
    return datetime.now().astimezone().strftime("%Y-%m-%d %I:%M:%S %p %Z")


def current_local_date() -> str:
    return datetime.now().astimezone().strftime("%Y-%m-%d")


def local_date_from_timestamp(timestamp_ms: int) -> str:
    return datetime.fromtimestamp(timestamp_ms / 1000).astimezone().strftime("%Y-%m-%d")


def format_r_multiple(value: float) -> str:
    return f"{value:+.2f}R"


def format_zone(center: float, atr: float, multiplier: float = 0.35) -> Tuple[str, float, float]:
    zone_buffer = max(atr * multiplier, abs(center) * 0.001)
    lower = round_price(center - zone_buffer)
    upper = round_price(center + zone_buffer)
    return f"{format_price(lower)} - {format_price(upper)}", lower, upper


def build_market_overview(
    candles: List[Candle],
    ema_fast: List[float],
    ema_slow: List[float],
    rsi_values: List[float],
    atr_values: List[float],
    config: Config,
) -> MarketOverview:
    last = candles[-1]
    closes = [c.close for c in candles]
    highs = [c.high for c in candles]
    lows = [c.low for c in candles]
    volumes = [c.volume for c in candles]

    support = min(lows[-config.sr_lookback - 1 : -1])
    resistance = max(highs[-config.sr_lookback - 1 : -1])
    atr = atr_values[-1]
    avg_volume = average(volumes[-config.volume_period - 1 : -1])
    macd_line, signal_line, histogram = calculate_macd(closes)

    support_zone, support_lower, support_upper = format_zone(support, atr)
    resistance_zone, resistance_lower, resistance_upper = format_zone(resistance, atr)

    body_size = abs(last.close - last.open)
    candle_range = max(last.high - last.low, 1e-9)
    body_ratio = body_size / candle_range
    upper_wick = last.high - max(last.open, last.close)
    lower_wick = min(last.open, last.close) - last.low
    close_near_support = support_lower <= last.close <= support_upper
    close_near_resistance = resistance_lower <= last.close <= resistance_upper

    if last.close > ema_fast[-1] > ema_slow[-1] and ema_fast[-1] > ema_fast[-2]:
        trend = "Bullish"
    elif last.close < ema_fast[-1] < ema_slow[-1] and ema_fast[-1] < ema_fast[-2]:
        trend = "Bearish"
    else:
        trend = "Sideways"

    if close_near_support:
        entry_condition = "Price is in support entry zone"
        entry_zone_side = "SUPPORT"
    elif close_near_resistance:
        entry_condition = "Price is in resistance entry zone"
        entry_zone_side = "RESISTANCE"
    else:
        entry_condition = "Price is outside entry zone - NO TRADE"
        entry_zone_side = "OUTSIDE"

    bullish_rejection = lower_wick >= body_size * 1.2 and last.close >= last.open
    bearish_rejection = upper_wick >= body_size * 1.2 and last.close <= last.open

    if close_near_support and bullish_rejection:
        rejection_candle = "Bullish rejection candle: strong lower wick"
    elif close_near_resistance and bearish_rejection:
        rejection_candle = "Bearish rejection candle: strong upper wick"
    elif bullish_rejection:
        rejection_candle = "Lower wick exists, but not at support zone"
    elif bearish_rejection:
        rejection_candle = "Upper wick exists, but not at resistance zone"
    else:
        rejection_candle = "No valid rejection candle"

    real_bullish_breakout = last.close > resistance * (1 + config.breakout_buffer_pct) and body_ratio >= 0.5
    fake_bullish_breakout = last.high > resistance * (1 + config.breakout_buffer_pct) and last.close <= resistance
    real_bearish_breakdown = last.close < support * (1 - config.breakout_buffer_pct) and body_ratio >= 0.5
    fake_bearish_breakdown = last.low < support * (1 - config.breakout_buffer_pct) and last.close >= support

    if real_bullish_breakout:
        breakout_check = "Real breakout: strong close above resistance"
    elif real_bearish_breakdown:
        breakout_check = "Real breakout: strong close below support"
    elif fake_bullish_breakout:
        breakout_check = "Fake breakout: wick above resistance only"
    elif fake_bearish_breakdown:
        breakout_check = "Fake breakout: wick below support only"
    else:
        breakout_check = "No breakout trigger"

    volume_ratio = last.volume / max(avg_volume, 1e-9)
    if volume_ratio >= config.volume_spike_factor:
        volume_state = "Volume increasing"
    elif volume_ratio >= 0.9:
        volume_state = "Volume stable"
    else:
        volume_state = "Volume decreasing"

    if macd_line[-1] > signal_line[-1] and histogram[-1] > 0:
        macd_state = "MACD bullish"
    elif macd_line[-1] < signal_line[-1] and histogram[-1] < 0:
        macd_state = "MACD bearish"
    else:
        macd_state = "MACD neutral"

    strong_bullish_candle = (
        trend == "Bullish"
        and last.close > last.open
        and body_ratio >= 0.6
        and (last.high - last.close) <= candle_range * 0.25
        and volume_ratio >= 1.0
        and macd_state == "MACD bullish"
    )
    strong_bearish_candle = (
        trend == "Bearish"
        and last.close < last.open
        and body_ratio >= 0.6
        and (last.close - last.low) <= candle_range * 0.25
        and volume_ratio >= 1.0
        and macd_state == "MACD bearish"
    )

    if trend == "Bullish" and close_near_support and bullish_rejection:
        entry_rule = "LONG: support + bullish rejection"
    elif trend == "Bearish" and close_near_resistance and bearish_rejection:
        entry_rule = "SHORT: resistance + bearish rejection"
    elif real_bullish_breakout and macd_state == "MACD bullish":
        entry_rule = "LONG: real breakout continuation"
    elif real_bearish_breakdown and macd_state == "MACD bearish":
        entry_rule = "SHORT: real breakdown continuation"
    else:
        entry_rule = "NO TRADE"

    if trend == "Sideways" and entry_rule != "NO TRADE":
        entry_rule = "NO TRADE"

    return MarketOverview(
        trend=f"{trend} (RSI {rsi_values[-1]:.1f})",
        support_zone=f"Strong support zone: {support_zone}",
        resistance_zone=f"Strong resistance zone: {resistance_zone}",
        entry_condition=entry_condition,
        rejection_candle=rejection_candle,
        breakout_check=breakout_check,
        volume_momentum=f"{volume_state}, {macd_state}",
        entry_rule=entry_rule,
        trend_bias=trend,
        entry_zone_side=entry_zone_side,
        bullish_rejection_valid=close_near_support and bullish_rejection,
        bearish_rejection_valid=close_near_resistance and bearish_rejection,
        strong_bullish_candle=strong_bullish_candle,
        strong_bearish_candle=strong_bearish_candle,
    )


def build_market_overview_lines(overview: MarketOverview) -> List[str]:
    return [
        f"Market Trend: {overview.trend}",
        overview.support_zone,
        overview.resistance_zone,
        f"Entry Condition: {overview.entry_condition}",
        f"Rejection Candle: {overview.rejection_candle}",
        f"Breakout Check: {overview.breakout_check}",
        f"Volume & Momentum: {overview.volume_momentum}",
        f"Entry Rule Verdict: {overview.entry_rule}",
    ]


def calculate_market_overview_for_candles(candles: List[Candle], config: Config) -> MarketOverview:
    closes = [c.close for c in candles]
    ema_fast = calculate_ema(closes, config.ema_fast_period)
    ema_slow = calculate_ema(closes, config.ema_slow_period)
    rsi_values = calculate_rsi(closes, config.rsi_period)
    atr_values = calculate_atr(candles, config.atr_period)
    return build_market_overview(
        candles,
        ema_fast,
        ema_slow,
        rsi_values,
        atr_values,
        config,
    )


def build_market_condition_alert_message(
    config: Config,
    title: str,
    candle_time: int,
    current_price: float,
    overview: MarketOverview,
    focus_lines: List[str],
) -> str:
    if overview.entry_rule == "NO TRADE":
        status_line = "Watch Only - setup ekhono confirm hoyni"
    elif overview.entry_rule.startswith("LONG"):
        status_line = "Possible LONG setup"
    elif overview.entry_rule.startswith("SHORT"):
        status_line = "Possible SHORT setup"
    else:
        status_line = overview.entry_rule

    if overview.entry_zone_side == "SUPPORT":
        zone_line = overview.support_zone
    elif overview.entry_zone_side == "RESISTANCE":
        zone_line = overview.resistance_zone
    else:
        zone_line = "Entry Zone: Outside zone"

    action_lines = "\n".join(f"- {line}" for line in focus_lines[:3])

    return (
        f"{title}\n\n"
        f"Pair: {config.symbol}\n"
        f"Timeframe: {config.interval}\n"
        f"Time: {format_local_time(candle_time)}\n"
        f"Current Price: {format_price(current_price)}\n"
        f"Status: {status_line}\n"
        f"Trend: {overview.trend}\n"
        f"Zone: {zone_line}\n"
        f"Breakout: {overview.breakout_check}\n"
        f"Momentum: {overview.volume_momentum}\n"
        f"Verdict: {overview.entry_rule}\n\n"
        f"What Now:\n{action_lines}"
    )


def build_watch_alert_message(
    config: Config,
    candle: Candle,
    overview: MarketOverview,
    watch_side: str,
    watch_score: int,
) -> str:
    if watch_side == "LONG":
        action_lines = [
            "Setup almost ready, next candle confirm hole LONG signal aste pare",
            "Support hold / breakout follow-through ache naki dekho",
            "Fresh candle close-er age entry nio na",
        ]
    else:
        action_lines = [
            "Setup almost ready, next candle confirm hole SHORT signal aste pare",
            "Resistance reject / breakdown follow-through ache naki dekho",
            "Fresh candle close-er age entry nio na",
        ]

    action_text = "\n".join(f"- {line}" for line in action_lines)

    return (
        "SETUP WATCH ALERT - NO ENTRY\n\n"
        f"Pair: {config.symbol}\n"
        f"Timeframe: {config.interval}\n"
        f"Time: {format_local_time(candle.open_time)}\n"
        f"Current Price: {format_price(candle.close)}\n"
        "Trade Status: WATCH ONLY - DO NOT ENTER YET\n"
        f"Watch Side: {watch_side}\n"
        f"Watch Score: {watch_score}/{config.min_signal_score}\n"
        f"Trend: {overview.trend}\n"
        f"Zone: {overview.support_zone if watch_side == 'LONG' else overview.resistance_zone}\n"
        f"Breakout: {overview.breakout_check}\n"
        f"Momentum: {overview.volume_momentum}\n"
        f"Verdict: {overview.entry_rule}\n\n"
        f"What Now:\n{action_text}"
    )


def send_setup_watch_alert(
    config: Config,
    candle: Candle,
    overview: MarketOverview,
    long_score: int,
    short_score: int,
    interval_state: Dict[str, object],
) -> bool:
    if not config.watch_alert_enabled:
        return False

    watch_threshold = max(config.min_signal_score - config.watch_alert_score_gap, 54)
    watch_side = ""
    watch_score = 0

    if long_score >= short_score:
        watch_side = "LONG"
        watch_score = long_score
        is_aligned = (
            overview.entry_rule.startswith("LONG")
            or overview.bullish_rejection_valid
            or overview.strong_bullish_candle
            or (overview.entry_zone_side == "SUPPORT" and overview.trend_bias == "Bullish")
            or ("Real breakout" in overview.breakout_check and overview.trend_bias == "Bullish")
        )
    else:
        watch_side = "SHORT"
        watch_score = short_score
        is_aligned = (
            overview.entry_rule.startswith("SHORT")
            or overview.bearish_rejection_valid
            or overview.strong_bearish_candle
            or (overview.entry_zone_side == "RESISTANCE" and overview.trend_bias == "Bearish")
            or ("Real breakout" in overview.breakout_check and overview.trend_bias == "Bearish")
        )

    if watch_score < watch_threshold or not is_aligned:
        return False

    watch_key = f"{watch_side}:{candle.open_time}:{watch_score}"
    if interval_state.get("last_watch_alert_key") == watch_key:
        return False

    send_telegram(
        build_watch_alert_message(config, candle, overview, watch_side, watch_score),
        config,
    )
    interval_state["last_watch_alert_key"] = watch_key
    return True


def send_market_condition_alerts(
    config: Config,
    candle: Candle,
    overview: MarketOverview,
    interval_state: Dict[str, object],
) -> bool:
    state_changed = False
    current_entry_zone = overview.entry_zone_side
    previous_entry_zone = str(interval_state.get("last_entry_zone_state", "OUTSIDE"))

    if current_entry_zone in {"SUPPORT", "RESISTANCE"} and previous_entry_zone != current_entry_zone:
        send_telegram(
            build_market_condition_alert_message(
                config,
                "ENTRY ZONE ALERT",
                candle.open_time,
                candle.close,
                overview,
                [
                    "Price important zone-e esheche",
                    "Ekhono entry nio na, candle confirmation wait koro",
                    "Rejection candle ba strong breakout close hole next setup ashbe",
                ],
            ),
            config,
        )
        state_changed = True

    interval_state["last_entry_zone_state"] = current_entry_zone

    rejection_key = ""
    rejection_title = "REJECTION CANDLE ALERT"
    rejection_focus: List[str] = []
    if overview.bullish_rejection_valid:
        rejection_key = f"bullish:{candle.open_time}"
        rejection_title = "BULLISH REJECTION ALERT"
        rejection_focus = [
            "Support theke strong bullish rejection peyechi",
            "Lower wick support defend koreche",
            "Next candle follow-through thakle LONG setup stronger hobe",
        ]
    elif overview.bearish_rejection_valid:
        rejection_key = f"bearish:{candle.open_time}"
        rejection_title = "BEARISH REJECTION ALERT"
        rejection_focus = [
            "Resistance theke strong bearish rejection peyechi",
            "Upper wick resistance reject koreche",
            "Next candle follow-through thakle SHORT setup stronger hobe",
        ]

    if rejection_key and interval_state.get("last_rejection_alert_key") != rejection_key:
        send_telegram(
            build_market_condition_alert_message(
                config,
                rejection_title,
                candle.open_time,
                candle.close,
                overview,
                rejection_focus,
            ),
            config,
        )
        interval_state["last_rejection_alert_key"] = rejection_key
        state_changed = True

    bullish_candle_key = f"bullish-candle:{candle.open_time}"
    if (
        overview.strong_bullish_candle
        and interval_state.get("last_strong_bullish_alert_key") != bullish_candle_key
    ):
        send_telegram(
            build_market_condition_alert_message(
                config,
                "STRONG BULLISH CANDLE ALERT",
                candle.open_time,
                candle.close,
                overview,
                [
                    "Strong bullish candle close hoyeche",
                    "Trend + volume + MACD bullish",
                    "Price follow-through korle LONG setup gorte pare",
                ],
            ),
            config,
        )
        interval_state["last_strong_bullish_alert_key"] = bullish_candle_key
        state_changed = True

    bearish_candle_key = f"bearish-candle:{candle.open_time}"
    if (
        overview.strong_bearish_candle
        and interval_state.get("last_strong_bearish_alert_key") != bearish_candle_key
    ):
        send_telegram(
            build_market_condition_alert_message(
                config,
                "STRONG BEARISH CANDLE ALERT",
                candle.open_time,
                candle.close,
                overview,
                [
                    "Strong bearish candle close hoyeche",
                    "Trend + volume + MACD bearish",
                    "Price follow-through korle SHORT setup gorte pare",
                ],
            ),
            config,
        )
        interval_state["last_strong_bearish_alert_key"] = bearish_candle_key
        state_changed = True

    return state_changed


def get_performance_state(state: Dict[str, object]) -> Dict[str, object]:
    performance_state = state.setdefault("performance", {})
    if not isinstance(performance_state, dict):
        performance_state = {}
        state["performance"] = performance_state

    daily_state = performance_state.setdefault("daily", {})
    if not isinstance(daily_state, dict):
        daily_state = {}
        performance_state["daily"] = daily_state

    by_symbol_state = performance_state.setdefault("by_symbol", {})
    if not isinstance(by_symbol_state, dict):
        by_symbol_state = {}
        performance_state["by_symbol"] = by_symbol_state

    by_interval_state = performance_state.setdefault("by_interval", {})
    if not isinstance(by_interval_state, dict):
        by_interval_state = {}
        performance_state["by_interval"] = by_interval_state

    by_symbol_interval_state = performance_state.setdefault("by_symbol_interval", {})
    if not isinstance(by_symbol_interval_state, dict):
        by_symbol_interval_state = {}
        performance_state["by_symbol_interval"] = by_symbol_interval_state

    performance_state.setdefault("last_daily_report_date", "")
    performance_state.setdefault("last_hourly_update_key", "")
    performance_state.setdefault("last_training_date", "")
    performance_state.setdefault("last_training_report", "")
    performance_state.setdefault("recent_closed", [])
    return performance_state


def get_stats_bucket(container: Dict[str, object], key: str) -> Dict[str, object]:
    bucket = container.setdefault(key, {})
    if not isinstance(bucket, dict):
        bucket = {}
        container[key] = bucket

    defaults = {
        "signals_sent": 0,
        "vip_signals": 0,
        "normal_signals": 0,
        "closed_trades": 0,
        "vip_closed": 0,
        "normal_closed": 0,
        "wins": 0,
        "losses": 0,
        "breakeven": 0,
        "total_r": 0.0,
    }
    for stat_key, default_value in defaults.items():
        bucket.setdefault(stat_key, default_value)

    return bucket


def update_signal_stats(state: Dict[str, object], signal: Signal, config: Config) -> None:
    performance_state = get_performance_state(state)
    overall_stats = get_stats_bucket(performance_state, "overall")
    daily_stats = get_stats_bucket(
        performance_state["daily"],
        local_date_from_timestamp(signal.candle_time),
    )
    symbol_stats = get_stats_bucket(performance_state["by_symbol"], config.symbol)
    interval_stats = get_stats_bucket(performance_state["by_interval"], config.interval)
    symbol_interval_stats = get_stats_bucket(
        performance_state["by_symbol_interval"],
        f"{config.symbol} {config.interval}",
    )

    for stats_bucket in (
        overall_stats,
        daily_stats,
        symbol_stats,
        interval_stats,
        symbol_interval_stats,
    ):
        stats_bucket["signals_sent"] = int(stats_bucket.get("signals_sent", 0)) + 1
        tier_key = "vip_signals" if signal.tier == "VIP" else "normal_signals"
        stats_bucket[tier_key] = int(stats_bucket.get(tier_key, 0)) + 1


def log_signal_dataset_entry(
    signal_key: str,
    signal: Signal,
    config: Config,
    market_overview: MarketOverview,
) -> None:
    features = signal.features if isinstance(signal.features, dict) else {}
    tp_values = list(signal.take_profits) + [0.0, 0.0, 0.0]
    self_learning.record_signal(
        signal_key,
        {
            "symbol": config.symbol,
            "interval": config.interval,
            "side": signal.side,
            "candle_time": signal.candle_time,
            "opened_at": format_now_local(),
            "tier": signal.tier,
            "grade": signal.grade,
            "score_base": int(features.get("score_before_learning", signal.score)),
            "score_final": signal.score,
            "entry": signal.entry,
            "stop_loss": signal.stop_loss,
            "tp1": tp_values[0],
            "tp2": tp_values[1],
            "tp3": tp_values[2],
            "trend_bias": market_overview.trend_bias,
            "breakout_type": market_overview.breakout_check,
            "entry_rule": market_overview.entry_rule,
            "volume_ratio": float(features.get("volume_ratio", 0.0) or 0.0),
            "rsi": float(features.get("rsi", 0.0) or 0.0),
            "body_ratio": float(features.get("body_ratio", 0.0) or 0.0),
            "extension_atr": float(features.get("extension_atr", 0.0) or 0.0),
            "htf_confirmed": bool(features.get("htf_confirmed", False)),
            "htf_fake_breakout": bool(features.get("htf_fake_breakout", False)),
            "structure_confirmed": bool(features.get("structure_confirmed", False)),
        },
    )


def calculate_trade_r_at_price(trade: Dict[str, object], exit_price: float) -> float:
    entry = float(trade["entry"])
    stop_loss = float(trade["stop_loss"])
    risk = max(abs(entry - stop_loss), 1e-9)
    side = str(trade["side"])

    if side == "LONG":
        return (exit_price - entry) / risk

    return (entry - exit_price) / risk


def book_trade_realization(trade: Dict[str, object], close_pct: float, exit_price: float) -> float:
    remaining_position = float(trade.get("remaining_position_pct", 100.0) or 0.0)
    booked_pct = min(remaining_position, close_pct)
    if booked_pct <= 0:
        return 0.0

    realized_r = float(trade.get("realized_r", 0.0) or 0.0)
    realized_r += calculate_trade_r_at_price(trade, exit_price) * (booked_pct / 100.0)
    trade["realized_r"] = round(realized_r, 4)
    trade["remaining_position_pct"] = round(max(0.0, remaining_position - booked_pct), 4)
    return booked_pct


def classify_trade_result(realized_r: float) -> str:
    if realized_r > RESULT_EPSILON_R:
        return "wins"
    if realized_r < -RESULT_EPSILON_R:
        return "losses"
    return "breakeven"


def record_closed_trade(
    state: Dict[str, object],
    trade: Dict[str, object],
    config: Config,
    exit_price: float,
    candle_time: int,
    close_reason: str,
) -> None:
    performance_state = get_performance_state(state)
    overall_stats = get_stats_bucket(performance_state, "overall")
    daily_stats = get_stats_bucket(
        performance_state["daily"],
        local_date_from_timestamp(candle_time),
    )
    symbol_stats = get_stats_bucket(performance_state["by_symbol"], config.symbol)
    interval_stats = get_stats_bucket(performance_state["by_interval"], config.interval)
    symbol_interval_stats = get_stats_bucket(
        performance_state["by_symbol_interval"],
        f"{config.symbol} {config.interval}",
    )
    realized_r = float(trade.get("realized_r", 0.0) or 0.0)
    result_key = classify_trade_result(realized_r)
    tier_key = "vip_closed" if str(trade.get("tier")) == "VIP" else "normal_closed"

    for stats_bucket in (
        overall_stats,
        daily_stats,
        symbol_stats,
        interval_stats,
        symbol_interval_stats,
    ):
        stats_bucket["closed_trades"] = int(stats_bucket.get("closed_trades", 0)) + 1
        stats_bucket[tier_key] = int(stats_bucket.get(tier_key, 0)) + 1
        stats_bucket[result_key] = int(stats_bucket.get(result_key, 0)) + 1
        stats_bucket["total_r"] = round(float(stats_bucket.get("total_r", 0.0)) + realized_r, 4)

    recent_closed = performance_state.setdefault("recent_closed", [])
    if not isinstance(recent_closed, list):
        recent_closed = []
        performance_state["recent_closed"] = recent_closed

    recent_closed.append(
        {
            "symbol": config.symbol,
            "interval": config.interval,
            "side": trade["side"],
            "tier": trade["tier"],
            "market_regime_key": str(trade.get("market_regime_key", "")),
            "market_trend_bias": str(trade.get("market_trend_bias", "")),
            "market_breakout": str(trade.get("market_breakout", "")),
            "result_r": round(realized_r, 4),
            "exit_price": round_price(exit_price),
            "closed_at": format_local_time(candle_time),
            "close_reason": close_reason,
        }
    )
    performance_state["recent_closed"] = recent_closed[-25:]

    signal_key = str(trade.get("signal_key", ""))
    if signal_key:
        self_learning.close_signal(
            signal_key,
            {
                "closed_at": format_local_time(candle_time),
                "outcome": result_key,
                "result_r": round(realized_r, 4),
                "tp1_hit": bool(trade.get("tp1_hit")),
                "tp2_hit": bool(trade.get("tp2_hit")),
                "close_reason": close_reason,
            },
        )


def calculate_win_rate(stats_bucket: Dict[str, object]) -> float:
    closed_trades = int(stats_bucket.get("closed_trades", 0) or 0)
    if closed_trades <= 0:
        return 0.0

    wins = int(stats_bucket.get("wins", 0) or 0)
    return (wins / closed_trades) * 100.0


def format_accuracy_line(label: str, stats_bucket: Dict[str, object]) -> str:
    return (
        f"{label}: {calculate_win_rate(stats_bucket):.1f}% | "
        f"closed {int(stats_bucket.get('closed_trades', 0))} | "
        f"W/L/BE {int(stats_bucket.get('wins', 0))}/"
        f"{int(stats_bucket.get('losses', 0))}/"
        f"{int(stats_bucket.get('breakeven', 0))} | "
        f"{format_r_multiple(float(stats_bucket.get('total_r', 0.0) or 0.0))}"
    )


def build_market_regime_key(overview: MarketOverview, side: str) -> str:
    trend = overview.trend_bias
    if overview.breakout_check.startswith("Real breakout"):
        breakout = "REAL_BREAKOUT"
    elif overview.breakout_check.startswith("Fake breakout"):
        breakout = "FAKE_BREAKOUT"
    else:
        breakout = "NO_BREAKOUT"

    if "Volume increasing" in overview.volume_momentum:
        volume = "VOL_UP"
    elif "Volume decreasing" in overview.volume_momentum:
        volume = "VOL_DOWN"
    else:
        volume = "VOL_FLAT"

    return f"{side}|{trend}|{breakout}|{volume}|{overview.entry_zone_side}"


def calculate_adaptive_score_adjustment(
    state: Dict[str, object],
    config: Config,
    side: str,
) -> Tuple[int, str]:
    if not config.adaptive_learning_enabled:
        return 0, "Adaptive learning off"

    performance_state = get_performance_state(state)
    pair_interval_key = f"{config.symbol} {config.interval}"
    pair_interval_stats = performance_state.get("by_symbol_interval", {}).get(pair_interval_key, {})
    closed_trades = int(pair_interval_stats.get("closed_trades", 0) or 0) if isinstance(pair_interval_stats, dict) else 0

    if closed_trades < 5:
        return 0, "Not enough closed trades for learning"

    recent_closed = performance_state.get("recent_closed", [])
    if not isinstance(recent_closed, list):
        recent_closed = []

    side_trades = [
        item
        for item in recent_closed
        if isinstance(item, dict)
        and str(item.get("symbol", "")) == config.symbol
        and str(item.get("interval", "")) == config.interval
        and str(item.get("side", "")) == side
    ][-12:]

    if len(side_trades) < 3:
        return 0, "Not enough side-specific trades for learning"

    wins = 0
    losses = 0
    total_r = 0.0
    for trade in side_trades:
        result_r = float(trade.get("result_r", 0.0) or 0.0)
        total_r += result_r
        if result_r > RESULT_EPSILON_R:
            wins += 1
        elif result_r < -RESULT_EPSILON_R:
            losses += 1

    trade_count = max(len(side_trades), 1)
    win_rate = (wins / trade_count) * 100.0
    average_r = total_r / trade_count
    adjustment = 0

    if win_rate >= 65 and average_r > 0.15:
        adjustment += 4
    elif win_rate >= 55 and average_r > 0.05:
        adjustment += 2
    elif win_rate <= 35 and average_r < -0.10:
        adjustment -= 4
    elif win_rate <= 45 and average_r < 0:
        adjustment -= 2

    if losses >= wins + 3:
        adjustment -= 2
    elif wins >= losses + 3:
        adjustment += 2

    bounded = max(-config.adaptive_learning_max_adjustment, min(config.adaptive_learning_max_adjustment, adjustment))
    if bounded == 0:
        return 0, f"Learning neutral from {trade_count} recent {side} trades"
    sign = "+" if bounded > 0 else ""
    return bounded, f"Learning bias {sign}{bounded} from {trade_count} recent {side} trades"


def calculate_market_regime_adjustment(
    state: Dict[str, object],
    config: Config,
    side: str,
    overview: MarketOverview,
) -> Tuple[int, str]:
    performance_state = get_performance_state(state)
    recent_closed = performance_state.get("recent_closed", [])
    if not isinstance(recent_closed, list):
        return 0, "No market memory yet"

    current_key = build_market_regime_key(overview, side)
    matched = [
        item
        for item in recent_closed
        if isinstance(item, dict)
        and str(item.get("symbol", "")) == config.symbol
        and str(item.get("interval", "")) == config.interval
        and str(item.get("side", "")) == side
        and str(item.get("market_regime_key", "")) == current_key
    ][-8:]

    if len(matched) < 3:
        return 0, "Not enough same-market examples"

    total_r = sum(float(item.get("result_r", 0.0) or 0.0) for item in matched)
    wins = sum(1 for item in matched if float(item.get("result_r", 0.0) or 0.0) > RESULT_EPSILON_R)
    losses = sum(1 for item in matched if float(item.get("result_r", 0.0) or 0.0) < -RESULT_EPSILON_R)
    average_r = total_r / max(len(matched), 1)
    adjustment = 0

    if losses >= wins + 2 and average_r < 0:
        adjustment -= 4
    elif wins >= losses + 2 and average_r > 0:
        adjustment += 2

    if "FAKE_BREAKOUT" in current_key and losses >= wins:
        adjustment -= 2

    if adjustment == 0:
        return 0, f"Market memory neutral for {len(matched)} similar setups"
    sign = "+" if adjustment > 0 else ""
    return adjustment, f"Market memory {sign}{adjustment} on similar candle regime"


def build_ranked_accuracy_lines(
    container: Dict[str, object],
    minimum_closed_trades: int = 1,
) -> List[str]:
    ranked_items: List[Tuple[str, Dict[str, object]]] = []

    for key, value in container.items():
        if not isinstance(value, dict):
            continue
        if int(value.get("closed_trades", 0) or 0) < minimum_closed_trades:
            continue
        ranked_items.append((key, value))

    ranked_items.sort(
        key=lambda item: (
            -calculate_win_rate(item[1]),
            -int(item[1].get("closed_trades", 0) or 0),
            item[0],
        )
    )
    return [format_accuracy_line(label, stats_bucket) for label, stats_bucket in ranked_items]


def build_accuracy_message(state: Dict[str, object]) -> str:
    performance_state = get_performance_state(state)
    overall_stats = get_stats_bucket(performance_state, "overall")
    symbol_lines = build_ranked_accuracy_lines(performance_state["by_symbol"])
    interval_lines = build_ranked_accuracy_lines(performance_state["by_interval"])
    symbol_interval_lines = build_ranked_accuracy_lines(
        performance_state["by_symbol_interval"],
        minimum_closed_trades=2,
    )

    message_parts = [
        "REAL ACCURACY TRACKER",
        "",
        "Overall:",
        format_accuracy_line("All setups", overall_stats),
    ]

    if symbol_lines:
        message_parts.extend(["", "By Pair:"])
        message_parts.extend(f"- {line}" for line in symbol_lines[:10])

    if interval_lines:
        message_parts.extend(["", "By Timeframe:"])
        message_parts.extend(f"- {line}" for line in interval_lines[:10])

    if symbol_interval_lines:
        message_parts.extend(["", "Best Pair + Timeframe:"])
        message_parts.extend(f"- {line}" for line in symbol_interval_lines[:10])

    if not symbol_lines and not interval_lines:
        message_parts.extend(
            [
                "",
                "No closed trades yet.",
                "Accuracy will become meaningful after some real closed signals.",
            ]
        )

    return "\n".join(message_parts)


def build_daily_report_message(config: Config, report_date: str, state: Dict[str, object]) -> str:
    performance_state = get_performance_state(state)
    today_stats = get_stats_bucket(performance_state["daily"], report_date)
    overall_stats = get_stats_bucket(performance_state, "overall")
    today_total_r = float(today_stats.get("total_r", 0.0) or 0.0)

    if today_total_r >= 1.5:
        mentor_note = "Strong green day. Protect profit, do not force extra trades."
    elif today_total_r > 0:
        mentor_note = "Green day. Stay disciplined and do not overtrade."
    elif today_total_r < 0:
        mentor_note = "Red day. Reduce risk and wait for cleaner setups tomorrow."
    else:
        mentor_note = "Flat day. Stay patient and keep risk controlled."

    return (
        "DAILY PERFORMANCE REPORT\n\n"
        f"Date: {report_date}\n"
        f"Report Time: {format_now_local()}\n"
        f"Watchlist: {', '.join(config.symbols)}\n"
        f"Signals Today: {int(today_stats.get('signals_sent', 0))} "
        f"(VIP {int(today_stats.get('vip_signals', 0))}, NORMAL {int(today_stats.get('normal_signals', 0))})\n"
        f"Closed Trades Today: {int(today_stats.get('closed_trades', 0))}\n"
        f"Today W/L/BE: {int(today_stats.get('wins', 0))} / "
        f"{int(today_stats.get('losses', 0))} / {int(today_stats.get('breakeven', 0))}\n"
        f"Today Win Rate: {calculate_win_rate(today_stats):.1f}%\n"
        f"Today Result: {format_r_multiple(today_total_r)}\n\n"
        f"Overall Closed Trades: {int(overall_stats.get('closed_trades', 0))}\n"
        f"Overall W/L/BE: {int(overall_stats.get('wins', 0))} / "
        f"{int(overall_stats.get('losses', 0))} / {int(overall_stats.get('breakeven', 0))}\n"
        f"Overall Win Rate: {calculate_win_rate(overall_stats):.1f}%\n"
        f"Overall Result: {format_r_multiple(float(overall_stats.get('total_r', 0.0) or 0.0))}\n\n"
        f"Mentor Note: {mentor_note}"
    )


def maybe_send_daily_report(config: Config, state: Dict[str, object]) -> bool:
    now = datetime.now().astimezone()
    report_date = now.strftime("%Y-%m-%d")
    performance_state = get_performance_state(state)

    if now.hour < config.daily_report_hour:
        return False
    if str(performance_state.get("last_daily_report_date", "")) == report_date:
        return False

    send_telegram(build_daily_report_message(config, report_date, state), config)
    performance_state["last_daily_report_date"] = report_date
    return True


def maybe_run_daily_training(state: Dict[str, object]) -> bool:
    performance_state = get_performance_state(state)
    training_date = current_local_date()
    if str(performance_state.get("last_training_date", "")) == training_date:
        return False

    model = self_learning.train_model()
    performance_state["last_training_date"] = training_date
    performance_state["last_training_report"] = self_learning.build_training_report(model)
    return True


def maybe_send_hourly_update(config: Config, state: Dict[str, object]) -> bool:
    if not config.hourly_update_enabled:
        return False

    performance_state = get_performance_state(state)
    now = datetime.now().astimezone()
    minute_bucket = max(config.hourly_update_interval_minutes, 1)
    bucket_start = (now.minute // minute_bucket) * minute_bucket
    update_key = now.strftime("%Y-%m-%d %H") + f":{bucket_start:02d}"

    if str(performance_state.get("last_hourly_update_key", "")) == update_key:
        return False

    send_telegram(build_hourly_update_message(config), config)
    performance_state["last_hourly_update_key"] = update_key
    return True


def build_profit_plan_lines(signal: Signal) -> List[str]:
    return [
        f"TP1 {format_price(signal.take_profits[0])}: Book {TP1_PARTIAL_PCT}%, move SL to entry",
        f"TP2 {format_price(signal.take_profits[1])}: Book {TP2_PARTIAL_PCT}%, move SL to TP1",
        f"TP3 {format_price(signal.take_profits[2])}: Close remaining {TP3_PARTIAL_PCT}%",
    ]


def classify_signal(score: int, config: Config) -> Tuple[str, str]:
    if score >= config.vip_signal_score:
        return "VIP", "Best quality setup. VIP signal is better than normal."

    return "NORMAL", "Usable setup. Normal signal is lower priority than VIP."


def classify_setup_grade(
    score: int,
    side: str,
    reasons: List[str],
    config: Config,
) -> Tuple[str, str, str]:
    has_htf = any("Higher timeframe trend confirms" in reason for reason in reasons)
    has_volume = any("Volume expansion confirmed" in reason for reason in reasons)
    has_retest = any("retest" in reason.lower() for reason in reasons)

    if side == "LONG":
        setup_type = "Bullish Breakout Continuation"
        setup_note = "Trend up, resistance break, then continuation follow-through."
    else:
        setup_type = "Bearish Breakdown Continuation"
        setup_note = "Trend down, support break, then continuation follow-through."

    if score >= max(config.vip_signal_score + 6, 90) and has_htf and has_volume and has_retest:
        return "A+", setup_type, setup_note
    if score >= config.vip_signal_score:
        return "A", setup_type, setup_note
    if score >= max(config.min_signal_score + 6, config.vip_signal_score - 4):
        return "A-", setup_type, setup_note
    return "B+", setup_type, setup_note


def build_trade_plan(signal: Signal, config: Config) -> Tuple[str, str, str]:
    if signal.grade in {"A+", "A"} or signal.tier == "VIP":
        risk_pct = f"{config.vip_risk_pct:.2f}%"
        leverage = config.vip_leverage
    else:
        risk_pct = f"{config.normal_risk_pct:.2f}%"
        leverage = config.normal_leverage

    leverage_note = "50x is very high risk. Use only if you fully accept liquidation risk."
    return risk_pct, leverage, leverage_note


def build_active_trade(signal: Signal, sent_time: str, config: Config) -> Dict[str, object]:
    risk_pct, leverage, leverage_note = build_trade_plan(signal, config)
    signal_key = f"{config.symbol}|{config.interval}|{signal.side}|{signal.candle_time}"
    return {
        "signal_key": signal_key,
        "tier": signal.tier,
        "grade": signal.grade,
        "setup_type": signal.setup_type,
        "setup_note": signal.setup_note,
        "verdict": signal.verdict,
        "side": signal.side,
        "score": signal.score,
        "entry": signal.entry,
        "market_structure_level": signal.market_structure_level,
        "atr": signal.atr,
        "stop_loss": signal.stop_loss,
        "current_stop_loss": signal.stop_loss,
        "take_profits": signal.take_profits,
        "reasons": copy.deepcopy(signal.reasons),
        "market_overview": copy.deepcopy(signal.market_overview),
        "risk_pct": risk_pct,
        "leverage": leverage,
        "leverage_note": leverage_note,
        "margin_mode": config.margin_mode,
        "candle_time": signal.candle_time,
        "opened_at": sent_time,
        "features": copy.deepcopy(signal.features),
        "remaining_position_pct": 100,
        "realized_r": 0.0,
        "tp1_hit": False,
        "tp2_hit": False,
        "best_entry_alert_key": "",
    }


def format_remaining_tp_map(trade: Dict[str, object]) -> str:
    take_profits = [float(tp) for tp in trade["take_profits"]]
    labels = ["TP1", "TP2", "TP3"]
    remaining_targets: List[str] = []

    for index, target in enumerate(take_profits):
        if index == 0 and bool(trade.get("tp1_hit")):
            continue
        if index == 1 and bool(trade.get("tp2_hit")):
            continue
        remaining_targets.append(f"{labels[index]} {format_price(target)}")

    return " / ".join(remaining_targets) if remaining_targets else "No open target"


def format_trade_update_message(
    config: Config,
    trade: Dict[str, object],
    title: str,
    reason: str,
    action_lines: List[str],
    candle_time: int,
    current_price: float,
) -> str:
    actions = "\n".join(f"- {line}" for line in action_lines)
    remaining_tps = format_remaining_tp_map(trade)
    remaining_position = int(trade.get("remaining_position_pct", 100) or 0)
    realized_r = float(trade.get("realized_r", 0.0) or 0.0)

    return (
        f"{title}\n\n"
        f"Pair: {config.symbol}\n"
        f"Timeframe: {config.interval}\n"
        f"Original Signal: {trade.get('grade', trade['tier'])} {trade['side']}\n"
        f"Setup Type: {trade.get('setup_type', 'Tracked setup')}\n"
        f"Entry: {format_price(float(trade['entry']))}\n"
        f"Opened At: {trade.get('opened_at', '-')}\n"
        f"Current Price: {format_price(current_price)}\n"
        f"Current Stop: {format_price(float(trade['current_stop_loss']))}\n"
        f"Remaining Position: {remaining_position}%\n"
        f"Locked Result: {format_r_multiple(realized_r)}\n"
        f"Next Targets: {remaining_tps}\n"
        f"Update Candle Time: {format_local_time(candle_time)}\n"
        f"Reason: {reason}\n\n"
        f"Action:\n{actions}"
    )


def maybe_send_best_entry_alert(
    config: Config,
    trade: Dict[str, object],
    candle: Candle,
    ema_fast_value: float,
) -> bool:
    if bool(trade.get("tp1_hit")):
        return False

    alert_key = f"best-entry:{candle.open_time}"
    if str(trade.get("best_entry_alert_key", "")) == alert_key:
        return False

    entry = float(trade["entry"])
    structure = float(trade.get("market_structure_level", entry))
    atr = max(float(trade.get("atr", 0.0) or 0.0), 1e-9)
    side = str(trade["side"])
    zone_buffer = max(atr * 0.25, entry * 0.001)

    if side == "LONG":
        touched_best_entry_zone = candle.low <= (structure + zone_buffer)
        confirmed_reclaim = candle.close >= structure and candle.close > candle.open
        trend_ok = candle.close >= ema_fast_value
        if not (touched_best_entry_zone and confirmed_reclaim and trend_ok):
            return False

        best_entry_price = min(entry, structure + zone_buffer)
        send_telegram(
            format_trade_update_message(
                config,
                trade,
                "BEST ENTRY ALERT",
                "Price retested the breakout/support zone and closed back strong.",
                [
                    f"Best entry zone: {format_price(best_entry_price)} around support/retest area",
                    f"Keep invalidation below {format_price(float(trade['current_stop_loss']))}",
                    "Take entry only if you still do not have position",
                ],
                candle.open_time,
                candle.close,
            ),
            config,
        )
        trade["best_entry_alert_key"] = alert_key
        return True

    touched_best_entry_zone = candle.high >= (structure - zone_buffer)
    confirmed_reject = candle.close <= structure and candle.close < candle.open
    trend_ok = candle.close <= ema_fast_value
    if not (touched_best_entry_zone and confirmed_reject and trend_ok):
        return False

    best_entry_price = max(entry, structure - zone_buffer)
    send_telegram(
        format_trade_update_message(
            config,
            trade,
            "BEST ENTRY ALERT",
            "Price retested the breakdown/resistance zone and closed back weak.",
            [
                f"Best entry zone: {format_price(best_entry_price)} around resistance/retest area",
                f"Keep invalidation above {format_price(float(trade['current_stop_loss']))}",
                "Take entry only if you still do not have position",
            ],
            candle.open_time,
            candle.close,
        ),
        config,
    )
    trade["best_entry_alert_key"] = alert_key
    return True


def manage_active_trade(
    candles: List[Candle],
    interval_state: Dict[str, object],
    root_state: Dict[str, object],
    config: Config,
) -> bool:
    active_trade = interval_state.get("active_trade")
    if not isinstance(active_trade, dict):
        return False

    trade = copy.deepcopy(active_trade)
    last = candles[-1]
    closes = [c.close for c in candles]
    ema_fast = calculate_ema(closes, config.ema_fast_period)
    tp1 = float(trade["take_profits"][0])
    tp2 = float(trade["take_profits"][1])
    tp3 = float(trade["take_profits"][2])
    side = str(trade["side"])

    def persist_open_trade() -> None:
        interval_state["active_trade"] = copy.deepcopy(trade)

    def close_trade(
        title: str,
        reason: str,
        action_lines: List[str],
        exit_price: float,
        close_reason: str,
    ) -> bool:
        book_trade_realization(
            trade,
            float(trade.get("remaining_position_pct", 0.0) or 0.0),
            exit_price,
        )
        send_telegram(
            format_trade_update_message(
                config,
                trade,
                title,
                reason,
                action_lines,
                last.open_time,
                last.close,
            ),
            config,
        )
        record_closed_trade(
            root_state,
            trade,
            config,
            exit_price,
            last.open_time,
            close_reason,
        )
        interval_state.pop("active_trade", None)
        return True

    if side == "LONG":
        current_stop = float(trade.get("current_stop_loss", trade["stop_loss"]))
        if last.low <= current_stop:
            return close_trade(
                "CLOSE SIGNAL",
                "Stop loss level touched. Close the remaining position now.",
                ["Close full remaining position", "Wait for the next fresh setup"],
                current_stop,
                "Stop loss touched",
            )

        if maybe_send_best_entry_alert(config, trade, last, ema_fast[-1]):
            persist_open_trade()

        if not bool(trade.get("tp1_hit")) and last.high >= tp1:
            trade["tp1_hit"] = True
            trade["current_stop_loss"] = trade["entry"]
            book_trade_realization(trade, TP1_PARTIAL_PCT, tp1)
            send_telegram(
                format_trade_update_message(
                    config,
                    trade,
                    "TRADE UPDATE: TP1 HIT",
                    "First take-profit reached.",
                    [
                        f"Book {TP1_PARTIAL_PCT}% profit now",
                        f"Move stop loss to entry {format_price(float(trade['entry']))}",
                    ],
                    last.open_time,
                    last.close,
                ),
                config,
            )
            persist_open_trade()

        if bool(trade.get("tp1_hit")) and not bool(trade.get("tp2_hit")) and last.high >= tp2:
            trade["tp2_hit"] = True
            trade["current_stop_loss"] = tp1
            book_trade_realization(trade, TP2_PARTIAL_PCT, tp2)
            send_telegram(
                format_trade_update_message(
                    config,
                    trade,
                    "TRADE UPDATE: TP2 HIT",
                    "Second take-profit reached.",
                    [
                        f"Book another {TP2_PARTIAL_PCT}% profit now",
                        f"Move stop loss to TP1 {format_price(tp1)}",
                    ],
                    last.open_time,
                    last.close,
                ),
                config,
            )
            persist_open_trade()

        if last.high >= tp3:
            return close_trade(
                "CLOSE SIGNAL: TP3 HIT",
                "Final target reached.",
                [f"Close the remaining {TP3_PARTIAL_PCT}% position now"],
                tp3,
                "TP3 reached",
            )

        if bool(trade.get("tp1_hit")) and last.close < ema_fast[-1] and last.close < last.open:
            return close_trade(
                "CLOSE SIGNAL",
                "Momentum weakened below EMA50 after entry.",
                ["Close the remaining position", "Protect booked profit"],
                last.close,
                "Momentum weakened after TP1",
            )

    else:
        current_stop = float(trade.get("current_stop_loss", trade["stop_loss"]))
        if last.high >= current_stop:
            return close_trade(
                "CLOSE SIGNAL",
                "Stop loss level touched. Close the remaining position now.",
                ["Close full remaining position", "Wait for the next fresh setup"],
                current_stop,
                "Stop loss touched",
            )

        if maybe_send_best_entry_alert(config, trade, last, ema_fast[-1]):
            persist_open_trade()

        if not bool(trade.get("tp1_hit")) and last.low <= tp1:
            trade["tp1_hit"] = True
            trade["current_stop_loss"] = trade["entry"]
            book_trade_realization(trade, TP1_PARTIAL_PCT, tp1)
            send_telegram(
                format_trade_update_message(
                    config,
                    trade,
                    "TRADE UPDATE: TP1 HIT",
                    "First take-profit reached.",
                    [
                        f"Book {TP1_PARTIAL_PCT}% profit now",
                        f"Move stop loss to entry {format_price(float(trade['entry']))}",
                    ],
                    last.open_time,
                    last.close,
                ),
                config,
            )
            persist_open_trade()

        if bool(trade.get("tp1_hit")) and not bool(trade.get("tp2_hit")) and last.low <= tp2:
            trade["tp2_hit"] = True
            trade["current_stop_loss"] = tp1
            book_trade_realization(trade, TP2_PARTIAL_PCT, tp2)
            send_telegram(
                format_trade_update_message(
                    config,
                    trade,
                    "TRADE UPDATE: TP2 HIT",
                    "Second take-profit reached.",
                    [
                        f"Book another {TP2_PARTIAL_PCT}% profit now",
                        f"Move stop loss to TP1 {format_price(tp1)}",
                    ],
                    last.open_time,
                    last.close,
                ),
                config,
            )
            persist_open_trade()

        if last.low <= tp3:
            return close_trade(
                "CLOSE SIGNAL: TP3 HIT",
                "Final target reached.",
                [f"Close the remaining {TP3_PARTIAL_PCT}% position now"],
                tp3,
                "TP3 reached",
            )

        if bool(trade.get("tp1_hit")) and last.close > ema_fast[-1] and last.close > last.open:
            return close_trade(
                "CLOSE SIGNAL",
                "Momentum weakened above EMA50 against the short position.",
                ["Close the remaining position", "Protect booked profit"],
                last.close,
                "Momentum weakened after TP1",
            )

    persist_open_trade()
    return True


def confirmation_interval(interval: str) -> Optional[str]:
    mapping = {
        "1m": "5m",
        "3m": "5m",
        "5m": "15m",
        "30m": "4h",
        "1h": "4h",
        "2h": "4h",
        "4h": "1d",
        "6h": "1d",
        "8h": "1d",
        "12h": "1d",
        "1d": "1w",
        "3d": "1w",
        "1w": "1M",
    }
    return mapping.get(interval)


def calculate_trend_snapshot(candles: List[Candle], config: Config) -> TrendSnapshot:
    minimum_required = minimum_required_candles(config)
    if len(candles) < minimum_required:
        raise ValueError(
            f"Not enough candles for trend snapshot. Required at least {minimum_required}, got {len(candles)}."
        )

    closes = [c.close for c in candles]
    ema_fast = calculate_ema(closes, config.ema_fast_period)
    ema_slow = calculate_ema(closes, config.ema_slow_period)
    rsi_values = calculate_rsi(closes, config.rsi_period)

    last_close = closes[-1]
    bias = "NEUTRAL"

    if last_close > ema_fast[-1] > ema_slow[-1] and ema_fast[-1] > ema_fast[-2] and rsi_values[-1] >= 52:
        bias = "LONG"
    elif (
        last_close < ema_fast[-1] < ema_slow[-1]
        and ema_fast[-1] < ema_fast[-2]
        and rsi_values[-1] <= 48
    ):
        bias = "SHORT"

    return TrendSnapshot(
        interval=config.interval,
        bias=bias,
        close=round_price(last_close),
        ema_fast=round_price(ema_fast[-1]),
        ema_slow=round_price(ema_slow[-1]),
        rsi=round(rsi_values[-1], 2),
    )


def fetch_confirmation_trend(base_config: Config, interval: str) -> Optional[TrendSnapshot]:
    higher_interval = confirmation_interval(interval)
    if not higher_interval:
        return None

    confirmation_config = config_for_interval(base_config, higher_interval)
    candles = fetch_klines(confirmation_config)
    if len(candles) < minimum_required_candles(confirmation_config):
        return None

    return calculate_trend_snapshot(candles, confirmation_config)


def fetch_confirmation_overview(base_config: Config, interval: str) -> Optional[MarketOverview]:
    higher_interval = confirmation_interval(interval)
    if not higher_interval:
        return None

    confirmation_config = config_for_interval(base_config, higher_interval)
    candles = fetch_klines(confirmation_config)
    if len(candles) < minimum_required_candles(confirmation_config):
        return None

    return calculate_market_overview_for_candles(candles, confirmation_config)


def build_signal(
    side: str,
    score: int,
    entry: float,
    stop_loss: float,
    reasons: List[str],
    candle_time: int,
    market_structure_level: float,
    atr: float,
    config: Config,
    features: Optional[Dict[str, object]] = None,
) -> Optional[Signal]:
    risk = abs(entry - stop_loss)
    if risk <= 0:
        return None

    tier, verdict = classify_signal(score, config)
    grade, setup_type, setup_note = classify_setup_grade(score, side, reasons, config)

    if side == "LONG":
        take_profits = [
            round_price(entry + (risk * config.tp_one_r)),
            round_price(entry + (risk * config.tp_two_r)),
            round_price(entry + (risk * config.tp_three_r)),
        ]
    else:
        take_profits = [
            round_price(entry - (risk * config.tp_one_r)),
            round_price(entry - (risk * config.tp_two_r)),
            round_price(entry - (risk * config.tp_three_r)),
        ]

    return Signal(
        tier=tier,
        grade=grade,
        setup_type=setup_type,
        setup_note=setup_note,
        verdict=verdict,
        side=side,
        score=score,
        entry=round_price(entry),
        stop_loss=round_price(stop_loss),
        take_profits=take_profits,
        reasons=reasons,
        candle_time=candle_time,
        market_structure_level=round_price(market_structure_level),
        atr=round_price(atr),
        features=features or {},
    )


def retune_signal_score(
    signal: Optional[Signal],
    score: int,
    config: Config,
    learning_note: str = "",
) -> Optional[Signal]:
    if signal is None:
        return None
    if score < config.min_signal_score:
        return None

    signal.score = score
    signal.tier, signal.verdict = classify_signal(score, config)
    signal.grade, signal.setup_type, signal.setup_note = classify_setup_grade(
        score,
        signal.side,
        signal.reasons,
        config,
    )
    if learning_note:
        signal.reasons.append(learning_note)
    return signal


def evaluate_long_setup(
    candles: List[Candle],
    ema_fast: List[float],
    ema_slow: List[float],
    rsi_values: List[float],
    atr_values: List[float],
    config: Config,
    higher_timeframe_trend: Optional[TrendSnapshot],
    higher_timeframe_overview: Optional[MarketOverview],
    market_overview: MarketOverview,
    state: Optional[Dict[str, object]] = None,
) -> Tuple[int, Optional[Signal]]:
    last = candles[-1]
    lows = [c.low for c in candles]
    highs = [c.high for c in candles]
    volumes = [c.volume for c in candles]

    resistance = max(highs[-config.sr_lookback - 1 : -1])
    recent_support = min(lows[-config.sr_lookback :])
    avg_volume = average(volumes[-config.volume_period - 1 : -1])
    atr = atr_values[-1]
    body_size = abs(last.close - last.open)
    candle_range = max(last.high - last.low, 1e-9)
    extension_atr = abs(last.close - ema_fast[-1]) / max(atr, 1e-9)

    reasons: List[str] = []
    score = 0

    htf_confirmed = higher_timeframe_trend is not None and higher_timeframe_trend.bias == "LONG"
    htf_fake_breakout = (
        higher_timeframe_overview is not None
        and higher_timeframe_overview.breakout_check.startswith("Fake breakout")
    )
    trend_up = last.close > ema_fast[-1] > ema_slow[-1] and ema_fast[-1] > ema_fast[-2]
    breakout = last.close > resistance * (1 + config.breakout_buffer_pct)
    retest_hold = last.low <= resistance * (1 + config.breakout_buffer_pct) and last.close > resistance
    volume_spike = last.volume >= avg_volume * config.volume_spike_factor
    rsi_ok = 54 <= rsi_values[-1] <= 68
    bullish_body = last.close > last.open and (body_size / candle_range) >= 0.55
    close_near_high = (last.high - last.close) <= candle_range * 0.30
    not_overextended = extension_atr <= config.max_extension_atr

    if htf_confirmed:
        score += 25
        reasons.append(f"Higher timeframe trend confirms LONG on {higher_timeframe_trend.interval}")
    if htf_fake_breakout:
        reasons.append("Higher timeframe shows fake breakout risk")
    if trend_up:
        score += 20
        reasons.append("Trend aligned above EMA50 and EMA200")
    if breakout:
        score += 20
        reasons.append("Closed above recent resistance")
    if retest_hold:
        score += 10
        reasons.append("Breakout level held on retest")
    if volume_spike:
        score += 10
        reasons.append("Volume expansion confirmed")
    if rsi_ok:
        score += 5
        reasons.append("RSI supports bullish momentum")
    if bullish_body:
        score += 5
        reasons.append("Strong bullish candle body")
    if close_near_high:
        score += 5
        reasons.append("Close finished near candle high")
    if not_overextended:
        score += 10
        reasons.append("Price is not overextended from EMA50")

    structure_confirmed = breakout and retest_hold

    mandatory_checks = [
        trend_up,
        structure_confirmed,
        volume_spike,
        rsi_ok,
        bullish_body,
        close_near_high,
        not_overextended,
    ]
    if config.require_higher_timeframe_confirmation and confirmation_interval(config.interval):
        mandatory_checks.append(htf_confirmed)
        mandatory_checks.append(not htf_fake_breakout)

    score_before_learning = score
    learning_note = ""
    if state is not None:
        adjustment, note = calculate_adaptive_score_adjustment(state, config, "LONG")
        score = max(0, min(100, score + adjustment))
        learning_note = note if adjustment != 0 else ""
        regime_adjustment, regime_note = calculate_market_regime_adjustment(
            state,
            config,
            "LONG",
            market_overview,
        )
        score = max(0, min(100, score + regime_adjustment))
        if regime_adjustment != 0:
            learning_note = f"{learning_note} | {regime_note}".strip(" |")
        model_adjustment, model_note = self_learning.score_adjustment(
            self_learning.load_model(),
            symbol=config.symbol,
            interval=config.interval,
            side="LONG",
            trend_bias=market_overview.trend_bias,
            breakout_type=market_overview.breakout_check,
        )
        score = max(0, min(100, score + model_adjustment))
        if model_adjustment != 0:
            learning_note = f"{learning_note} | model {model_note}".strip(" |")

    if not all(mandatory_checks) or score < config.min_signal_score:
        return score, None

    stop_loss = min(last.low - (atr * 0.2), last.close - (atr * config.atr_stop_multiplier))
    stop_loss = max(stop_loss, recent_support - atr)

    return score, retune_signal_score(build_signal(
        side="LONG",
        score=score,
        entry=last.close,
        stop_loss=stop_loss,
        reasons=reasons,
        candle_time=last.open_time,
        market_structure_level=resistance,
        atr=atr,
        config=config,
        features={
            "score_before_learning": score_before_learning,
            "volume_ratio": last.volume / max(avg_volume, 1e-9),
            "rsi": rsi_values[-1],
            "body_ratio": body_size / candle_range,
            "extension_atr": extension_atr,
            "htf_confirmed": htf_confirmed,
            "htf_fake_breakout": htf_fake_breakout,
            "structure_confirmed": structure_confirmed,
        },
    ), score, config, learning_note)


def evaluate_short_setup(
    candles: List[Candle],
    ema_fast: List[float],
    ema_slow: List[float],
    rsi_values: List[float],
    atr_values: List[float],
    config: Config,
    higher_timeframe_trend: Optional[TrendSnapshot],
    higher_timeframe_overview: Optional[MarketOverview],
    market_overview: MarketOverview,
    state: Optional[Dict[str, object]] = None,
) -> Tuple[int, Optional[Signal]]:
    last = candles[-1]
    lows = [c.low for c in candles]
    highs = [c.high for c in candles]
    volumes = [c.volume for c in candles]

    support = min(lows[-config.sr_lookback - 1 : -1])
    recent_resistance = max(highs[-config.sr_lookback :])
    avg_volume = average(volumes[-config.volume_period - 1 : -1])
    atr = atr_values[-1]
    body_size = abs(last.close - last.open)
    candle_range = max(last.high - last.low, 1e-9)
    extension_atr = abs(last.close - ema_fast[-1]) / max(atr, 1e-9)

    reasons: List[str] = []
    score = 0

    htf_confirmed = higher_timeframe_trend is not None and higher_timeframe_trend.bias == "SHORT"
    htf_fake_breakout = (
        higher_timeframe_overview is not None
        and higher_timeframe_overview.breakout_check.startswith("Fake breakout")
    )
    trend_down = last.close < ema_fast[-1] < ema_slow[-1] and ema_fast[-1] < ema_fast[-2]
    breakdown = last.close < support * (1 - config.breakout_buffer_pct)
    retest_fail = last.high >= support * (1 - config.breakout_buffer_pct) and last.close < support
    volume_spike = last.volume >= avg_volume * config.volume_spike_factor
    rsi_ok = 32 <= rsi_values[-1] <= 46
    bearish_body = last.close < last.open and (body_size / candle_range) >= 0.55
    close_near_low = (last.close - last.low) <= candle_range * 0.30
    not_overextended = extension_atr <= config.max_extension_atr

    if htf_confirmed:
        score += 25
        reasons.append(f"Higher timeframe trend confirms SHORT on {higher_timeframe_trend.interval}")
    if htf_fake_breakout:
        reasons.append("Higher timeframe shows fake breakout risk")
    if trend_down:
        score += 20
        reasons.append("Trend aligned below EMA50 and EMA200")
    if breakdown:
        score += 20
        reasons.append("Closed below recent support")
    if retest_fail:
        score += 10
        reasons.append("Breakdown level rejected on retest")
    if volume_spike:
        score += 10
        reasons.append("Volume expansion confirmed")
    if rsi_ok:
        score += 5
        reasons.append("RSI supports bearish momentum")
    if bearish_body:
        score += 5
        reasons.append("Strong bearish candle body")
    if close_near_low:
        score += 5
        reasons.append("Close finished near candle low")
    if not_overextended:
        score += 10
        reasons.append("Price is not overextended from EMA50")

    structure_confirmed = breakdown and retest_fail

    mandatory_checks = [
        trend_down,
        structure_confirmed,
        volume_spike,
        rsi_ok,
        bearish_body,
        close_near_low,
        not_overextended,
    ]
    if config.require_higher_timeframe_confirmation and confirmation_interval(config.interval):
        mandatory_checks.append(htf_confirmed)
        mandatory_checks.append(not htf_fake_breakout)

    score_before_learning = score
    learning_note = ""
    if state is not None:
        adjustment, note = calculate_adaptive_score_adjustment(state, config, "SHORT")
        score = max(0, min(100, score + adjustment))
        learning_note = note if adjustment != 0 else ""
        regime_adjustment, regime_note = calculate_market_regime_adjustment(
            state,
            config,
            "SHORT",
            market_overview,
        )
        score = max(0, min(100, score + regime_adjustment))
        if regime_adjustment != 0:
            learning_note = f"{learning_note} | {regime_note}".strip(" |")
        model_adjustment, model_note = self_learning.score_adjustment(
            self_learning.load_model(),
            symbol=config.symbol,
            interval=config.interval,
            side="SHORT",
            trend_bias=market_overview.trend_bias,
            breakout_type=market_overview.breakout_check,
        )
        score = max(0, min(100, score + model_adjustment))
        if model_adjustment != 0:
            learning_note = f"{learning_note} | model {model_note}".strip(" |")

    if not all(mandatory_checks) or score < config.min_signal_score:
        return score, None

    stop_loss = max(last.high + (atr * 0.2), last.close + (atr * config.atr_stop_multiplier))
    stop_loss = min(stop_loss, recent_resistance + atr)

    return score, retune_signal_score(build_signal(
        side="SHORT",
        score=score,
        entry=last.close,
        stop_loss=stop_loss,
        reasons=reasons,
        candle_time=last.open_time,
        market_structure_level=support,
        atr=atr,
        config=config,
        features={
            "score_before_learning": score_before_learning,
            "volume_ratio": last.volume / max(avg_volume, 1e-9),
            "rsi": rsi_values[-1],
            "body_ratio": body_size / candle_range,
            "extension_atr": extension_atr,
            "htf_confirmed": htf_confirmed,
            "htf_fake_breakout": htf_fake_breakout,
            "structure_confirmed": structure_confirmed,
        },
    ), score, config, learning_note)


def is_in_cooldown(signal: Signal, state: Dict[str, object], config: Config) -> bool:
    last_side = str(state.get("last_signal_side", ""))
    last_candle_time = int(state.get("last_signal_candle_time", 0) or 0)
    if last_side != signal.side or last_candle_time == 0:
        return False

    cooldown_window = config.cooldown_candles * interval_to_milliseconds(config.interval)
    return (signal.candle_time - last_candle_time) < cooldown_window


def format_signal_message(
    signal: Signal,
    config: Config,
    sent_time: Optional[str] = None,
    is_demo: bool = False,
) -> str:
    take_profits = " / ".join(format_price(tp) for tp in signal.take_profits)
    profit_plan = "\n".join(f"- {line}" for line in build_profit_plan_lines(signal))
    reasons = "\n".join(f"- {reason}" for reason in signal.reasons)
    market_overview = "\n".join(f"- {line}" for line in signal.market_overview)
    if not market_overview:
        market_overview = "- No live market condition attached"
    signal_time = format_local_time(signal.candle_time)
    alert_sent_time = sent_time or format_now_local()
    risk_pct, leverage, leverage_note = build_trade_plan(signal, config)
    title = f"{signal.tier} SIGNAL: {signal.side}"
    demo_lines = ""
    mentor_tracking_line = "Mentor Tracking: ON - TP/CLOSE updates come after candle close\n"

    if is_demo:
        title = f"DEMO SIGNAL: {signal.grade} {signal.side}"
        demo_lines = "Message Type: Demo/Test\nTrade Status: DO NOT TRADE THIS MESSAGE\n"
        mentor_tracking_line = "Mentor Tracking: OFF - demo message only\n"
    else:
        title = f"{signal.grade} SIGNAL: {signal.side}"

    return (
        f"{title}\n\n"
        f"Pair: {config.symbol}\n"
        f"Timeframe: {config.interval}\n"
        f"{demo_lines}"
        f"{mentor_tracking_line}"
        f"Setup Grade: {signal.grade}\n"
        f"Setup Type: {signal.setup_type}\n"
        f"Setup Note: {signal.setup_note}\n"
        f"Signal Tier: {signal.tier}\n"
        f"Quality Verdict: {signal.verdict}\n"
        f"Margin Mode: {config.margin_mode}\n"
        f"Suggested Position Risk: {risk_pct} of wallet\n"
        f"Suggested Leverage: {leverage}\n"
        f"Leverage Note: {leverage_note}\n"
        f"Signal Candle Time: {signal_time}\n"
        f"Alert Sent Time: {alert_sent_time}\n"
        f"Setup Score: {signal.score}/100\n"
        f"Entry: {format_price(signal.entry)}\n"
        f"Market Structure: {format_price(signal.market_structure_level)}\n"
        f"ATR: {format_price(signal.atr)}\n"
        f"SL: {format_price(signal.stop_loss)}\n"
        f"TP: {take_profits}\n\n"
        f"Market Condition:\n{market_overview}\n\n"
        f"Mentor Profit Plan:\n{profit_plan}\n\n"
        f"Reasons:\n{reasons}"
    )


def build_market_message(config: Config) -> str:
    candles = fetch_klines(config)
    overview = calculate_market_overview_for_candles(candles, config)
    overview_lines = "\n".join(f"- {line}" for line in build_market_overview_lines(overview))

    return (
        "MARKET STATUS\n\n"
        f"Pair: {config.symbol}\n"
        f"Timeframe: {config.interval}\n"
        f"Checked At: {format_now_local()}\n\n"
        f"{overview_lines}"
    )


def build_hourly_update_message(config: Config) -> str:
    lines = [
        "HOURLY MARKET UPDATE",
        "",
        f"Update Time: {format_now_local()}",
        f"Timeframe: {config.hourly_update_timeframe}",
        f"Watchlist: {', '.join(config.symbols)}",
        "Trade Status: INFO ONLY - not a real entry signal",
        "",
        "Real signal thakle alada signal message jabe.",
        "Na thakle ei update diye current market bias bujhte parba.",
        "",
    ]

    for symbol in config.symbols:
        symbol_config = config_for_symbol(config, symbol)
        interval_config = config_for_interval(symbol_config, config.hourly_update_timeframe)
        candles = fetch_klines(interval_config)
        overview = calculate_market_overview_for_candles(candles, interval_config)
        last = candles[-1]

        lines.extend(
            [
                f"{symbol}",
                f"- Price: {format_price(last.close)}",
                f"- Trend: {overview.trend}",
                f"- Verdict: {overview.entry_rule}",
                f"- Entry Zone: {overview.entry_condition}",
                f"- Breakout: {overview.breakout_check}",
                f"- Momentum: {overview.volume_momentum}",
                "",
            ]
        )

    return "\n".join(lines).rstrip()


def build_demo_message(config: Config) -> str:
    try:
        candles = fetch_klines(config)
        closes = [c.close for c in candles]
        highs = [c.high for c in candles]
        atr_values = calculate_atr(candles, config.atr_period)

        last = candles[-1]
        atr = atr_values[-1]
        market_structure = max(highs[-config.sr_lookback - 1 : -1])
        stop_loss = last.close - (atr * config.atr_stop_multiplier)
        reasons = [
            "Current market price used for demo",
            "Trend aligned above EMA50 and EMA200",
            "Breakout style setup preview",
            "Volume expansion confirmed",
            "RSI supports bullish momentum",
        ]
        demo_score = max(config.vip_signal_score, 88)
        demo_signal = build_signal(
            side="LONG",
            score=demo_score,
            entry=last.close,
            stop_loss=stop_loss,
            reasons=reasons,
            candle_time=last.open_time,
            market_structure_level=market_structure,
            atr=atr,
            config=config,
        )
        if demo_signal:
            return format_signal_message(
                demo_signal,
                config,
                format_now_local(),
                is_demo=True,
            )
    except Exception as exc:
        LOGGER.warning("Demo message is using fallback values: %s", exc)

    alert_sent_time = format_now_local()
    fallback_entry = round_price(3500.0 if config.symbol == "ETHUSDT" else 100.0)
    fallback_atr = round_price(max(fallback_entry * 0.01, 0.1))
    fallback_market_structure = round_price(fallback_entry * 0.995)
    fallback_stop = round_price(fallback_entry - (fallback_atr * config.atr_stop_multiplier))
    risk = abs(fallback_entry - fallback_stop)
    fallback_tps = [
        round_price(fallback_entry + (risk * config.tp_one_r)),
        round_price(fallback_entry + (risk * config.tp_two_r)),
        round_price(fallback_entry + (risk * config.tp_three_r)),
    ]
    take_profits = " / ".join(format_price(tp) for tp in fallback_tps)
    profit_plan = "\n".join(
        [
            f"- TP1 {format_price(fallback_tps[0])}: Book {TP1_PARTIAL_PCT}%, move SL to entry",
            f"- TP2 {format_price(fallback_tps[1])}: Book {TP2_PARTIAL_PCT}%, move SL to TP1",
            f"- TP3 {format_price(fallback_tps[2])}: Close remaining {TP3_PARTIAL_PCT}%",
        ]
    )

    return (
        "DEMO SIGNAL: A SIGNAL LONG\n\n"
        f"Pair: {config.symbol}\n"
        f"Timeframe: {config.interval}\n"
        "Message Type: Demo/Test\n"
        "Trade Status: DO NOT TRADE THIS MESSAGE\n"
        "Setup Grade: A\n"
        "Setup Type: Bullish Breakout Continuation\n"
        "Setup Note: Trend up, resistance break, then continuation follow-through.\n"
        "Signal Tier: VIP\n"
        "Quality Verdict: Best quality setup. VIP signal is better than normal.\n"
        f"Margin Mode: {config.margin_mode}\n"
        f"Suggested Position Risk: {config.vip_risk_pct:.2f}% of wallet\n"
        f"Suggested Leverage: {config.vip_leverage}\n"
        "Leverage Note: 50x is very high risk. Use only if you fully accept liquidation risk.\n"
        f"Alert Sent Time: {alert_sent_time}\n"
        f"Setup Score: {max(config.vip_signal_score, 88)}/100\n"
        f"Entry: {format_price(fallback_entry)}\n"
        f"Market Structure: {format_price(fallback_market_structure)}\n"
        f"ATR: {format_price(fallback_atr)}\n"
        f"SL: {format_price(fallback_stop)}\n"
        f"TP: {take_profits}\n\n"
        f"Mentor Profit Plan:\n{profit_plan}\n\n"
        "Reasons:\n"
        "- Demo fallback values used\n"
        "- Trend aligned above EMA50 and EMA200\n"
        "- Breakout style setup preview\n"
        "- Volume expansion confirmed\n"
        "- RSI supports bullish momentum"
    )


def analyze_market(
    candles: List[Candle],
    config: Config,
    higher_timeframe_trend: Optional[TrendSnapshot] = None,
    higher_timeframe_overview: Optional[MarketOverview] = None,
    state: Optional[Dict[str, object]] = None,
) -> AnalysisResult:
    closes = [c.close for c in candles]
    minimum_required = minimum_required_candles(config)
    if len(candles) < minimum_required:
        raise ValueError(
            f"Not enough candles. Required at least {minimum_required}, got {len(candles)}."
        )

    ema_fast = calculate_ema(closes, config.ema_fast_period)
    ema_slow = calculate_ema(closes, config.ema_slow_period)
    rsi_values = calculate_rsi(closes, config.rsi_period)
    atr_values = calculate_atr(candles, config.atr_period)
    market_overview = build_market_overview(
        candles,
        ema_fast,
        ema_slow,
        rsi_values,
        atr_values,
        config,
    )

    long_score, long_signal = evaluate_long_setup(
        candles,
        ema_fast,
        ema_slow,
        rsi_values,
        atr_values,
        config,
        higher_timeframe_trend,
        higher_timeframe_overview,
        market_overview,
        state,
    )
    short_score, short_signal = evaluate_short_setup(
        candles,
        ema_fast,
        ema_slow,
        rsi_values,
        atr_values,
        config,
        higher_timeframe_trend,
        higher_timeframe_overview,
        market_overview,
        state,
    )

    selected_signal: Optional[Signal] = None
    if long_signal and short_signal:
        selected_signal = long_signal if long_score >= short_score else short_signal
    elif long_signal:
        selected_signal = long_signal
    elif short_signal:
        selected_signal = short_signal

    if selected_signal:
        selected_signal.market_overview = build_market_overview_lines(market_overview)
        return AnalysisResult(
            signal=selected_signal,
            market_overview=market_overview,
            long_score=long_score,
            short_score=short_score,
        )

    LOGGER.info(
        "No trade setup on %s %s. Long score=%s, Short score=%s",
        config.symbol,
        config.interval,
        long_score,
        short_score,
    )
    return AnalysisResult(
        signal=None,
        market_overview=market_overview,
        long_score=long_score,
        short_score=short_score,
    )


def process_interval(
    base_config: Config,
    interval: str,
    state: Dict[str, object],
    root_state: Dict[str, object],
) -> bool:
    interval_config = config_for_interval(base_config, interval)
    interval_state = get_interval_state(state, interval)
    candles = fetch_klines(interval_config)
    current_candle_time = candles[-1].open_time

    if int(interval_state.get("last_checked_candle_time", 0) or 0) == current_candle_time:
        return False

    working_interval_state = copy.deepcopy(interval_state)

    minimum_required = minimum_required_candles(interval_config)
    if len(candles) < minimum_required:
        LOGGER.info(
            "Skipping %s: not enough candles. Required at least %s, got %s.",
            interval,
            minimum_required,
            len(candles),
        )
        working_interval_state["last_checked_candle_time"] = current_candle_time
        working_interval_state["insufficient_data_required"] = minimum_required
        working_interval_state["insufficient_data_available"] = len(candles)
        replace_state(interval_state, working_interval_state)
        return True

    working_interval_state["last_checked_candle_time"] = current_candle_time
    working_interval_state.pop("insufficient_data_required", None)
    working_interval_state.pop("insufficient_data_available", None)

    if manage_active_trade(candles, interval_state, root_state, interval_config):
        managed_interval_state = copy.deepcopy(interval_state)
        managed_interval_state["last_checked_candle_time"] = current_candle_time
        managed_interval_state.pop("insufficient_data_required", None)
        managed_interval_state.pop("insufficient_data_available", None)
        replace_state(interval_state, managed_interval_state)
        LOGGER.info(
            "Trade management checked on %s %s. Waiting for next update or close condition.",
            interval_config.symbol,
            interval,
        )
        return True

    higher_timeframe_trend = fetch_confirmation_trend(base_config, interval)
    higher_timeframe_overview = fetch_confirmation_overview(base_config, interval)

    analysis = analyze_market(
        candles,
        interval_config,
        higher_timeframe_trend,
        higher_timeframe_overview,
        root_state,
    )
    signal = analysis.signal
    if not signal:
        market_overview = analysis.market_overview
        if send_market_condition_alerts(
            interval_config,
            candles[-1],
            market_overview,
            working_interval_state,
        ):
            LOGGER.info(
                "Context alert sent on %s %s with verdict %s.",
                interval_config.symbol,
                interval,
                market_overview.entry_rule,
            )
        if send_setup_watch_alert(
            interval_config,
            candles[-1],
            market_overview,
            analysis.long_score,
            analysis.short_score,
            working_interval_state,
        ):
            LOGGER.info(
                "Setup watch alert sent on %s %s. Long score=%s, Short score=%s",
                interval_config.symbol,
                interval,
                analysis.long_score,
                analysis.short_score,
            )
        replace_state(interval_state, working_interval_state)
        return True

    if is_in_cooldown(signal, working_interval_state, interval_config):
        LOGGER.info("Signal skipped due to cooldown: %s on %s", signal.side, interval)
        replace_state(interval_state, working_interval_state)
        return True

    sent_time = format_now_local()
    send_telegram(format_signal_message(signal, interval_config, sent_time), interval_config)
    update_signal_stats(root_state, signal, interval_config)
    active_trade = build_active_trade(signal, sent_time, interval_config)
    active_trade["market_regime_key"] = build_market_regime_key(analysis.market_overview, signal.side)
    active_trade["market_trend_bias"] = analysis.market_overview.trend_bias
    active_trade["market_breakout"] = analysis.market_overview.breakout_check
    log_signal_dataset_entry(
        str(active_trade["signal_key"]),
        signal,
        interval_config,
        analysis.market_overview,
    )
    working_interval_state["active_trade"] = active_trade
    working_interval_state["last_signal_side"] = signal.side
    working_interval_state["last_signal_candle_time"] = signal.candle_time
    working_interval_state["last_signal_sent_time"] = sent_time
    replace_state(interval_state, working_interval_state)
    LOGGER.info(
        "Mentor tracking started for %s %s %s signal.",
        interval_config.symbol,
        interval,
        signal.side,
    )
    LOGGER.info("Signal sent: %s at %s on %s (%s)", signal.side, signal.entry, interval, sent_time)
    return True


def iter_active_trades(state: Dict[str, object]) -> List[Tuple[str, str, Dict[str, object]]]:
    active_trades: List[Tuple[str, str, Dict[str, object]]] = []
    symbols_state = state.get("symbols", {})
    if not isinstance(symbols_state, dict):
        return active_trades

    for symbol, symbol_state in symbols_state.items():
        if not isinstance(symbol_state, dict):
            continue

        intervals_state = symbol_state.get("intervals", {})
        if not isinstance(intervals_state, dict):
            continue

        for interval, interval_state in intervals_state.items():
            if not isinstance(interval_state, dict):
                continue

            trade = interval_state.get("active_trade")
            if isinstance(trade, dict):
                active_trades.append((symbol, interval, trade))

    return active_trades


def build_active_trades_message(state: Dict[str, object]) -> str:
    active_trades = iter_active_trades(state)
    if not active_trades:
        return "No active mentor trades right now."

    sections: List[str] = []
    for symbol, interval, trade in active_trades:
        sections.append(
            "\n".join(
                [
                    f"{symbol} {interval}",
                    f"Side: {trade.get('tier', 'UNKNOWN')} {trade.get('side', 'UNKNOWN')}",
                    f"Entry: {format_price(float(trade.get('entry', 0.0) or 0.0))}",
                    f"Current Stop: {format_price(float(trade.get('current_stop_loss', trade.get('stop_loss', 0.0)) or 0.0))}",
                    f"Remaining Position: {int(float(trade.get('remaining_position_pct', 0.0) or 0.0))}%",
                    f"Locked Result: {format_r_multiple(float(trade.get('realized_r', 0.0) or 0.0))}",
                    f"Next Targets: {format_remaining_tp_map(trade)}",
                    f"Opened At: {trade.get('opened_at', '-')}",
                ]
            )
        )

    return "Active Trades\n\n" + "\n\n".join(sections)


def classify_signal_checker_result(
    analysis: AnalysisResult,
    config: Config,
) -> Tuple[str, int, str, str]:
    if analysis.long_score > analysis.short_score:
        best_side = "LONG"
        best_score = analysis.long_score
    elif analysis.short_score > analysis.long_score:
        best_side = "SHORT"
        best_score = analysis.short_score
    else:
        best_side = "WAIT"
        best_score = analysis.long_score

    watch_threshold = max(config.min_signal_score - config.watch_alert_score_gap, 54)

    if analysis.signal:
        quality = "GOOD"
        note = f"{analysis.signal.tier} {analysis.signal.grade}"
    elif best_score >= watch_threshold:
        quality = "WATCH"
        note = "Almost ready"
    else:
        quality = "BAD"
        note = "No clean setup"

    return best_side, best_score, quality, note


def build_signal_checker_message(config: Config) -> str:
    lines = [
        "SIGNAL CHECKER",
        "",
        f"Checked At: {format_now_local()}",
        "GOOD = tradable signal | WATCH = near setup | BAD = no clean setup",
        "",
    ]

    for symbol in config.symbols:
        symbol_config = config_for_symbol(config, symbol)
        lines.append(symbol)

        for interval in config.intervals:
            interval_config = config_for_interval(symbol_config, interval)
            candles = fetch_klines(interval_config)
            higher_timeframe_trend = fetch_confirmation_trend(symbol_config, interval)
            higher_timeframe_overview = fetch_confirmation_overview(symbol_config, interval)
            analysis = analyze_market(
                candles,
                interval_config,
                higher_timeframe_trend,
                higher_timeframe_overview,
                state,
            )
            best_side, best_score, quality, note = classify_signal_checker_result(
                analysis,
                interval_config,
            )

            lines.append(
                f"- {interval}: LONG {analysis.long_score} | SHORT {analysis.short_score} | "
                f"Best {best_side} {best_score} | {quality} | {note}"
            )

        lines.append("")

    return "\n".join(lines).rstrip()


def build_user_signal_help_message() -> str:
    return (
        f"{USER_SIGNAL_TEMPLATE}\n\n"
        "Example free-form:\n"
        "Short BTCUSDT 15m entry 69800 to 70300, stop loss above 71000, "
        "targets 68800, 68000, 66800\n\n"
        "Loose format also works:\n"
        "BTCUSDT 5m long 84500 83900 85100 85600 86200"
    )


def normalize_user_signal_text(text: str) -> str:
    return (
        text.upper()
        .replace("—", "-")
        .replace("–", "-")
        .replace("−", "-")
        .replace("~", "-")
    )


def normalize_symbol_token(token: str) -> str:
    cleaned = token.strip().upper().replace(" ", "").replace("/", "")
    if not cleaned:
        return ""
    if cleaned.endswith("USDT"):
        return cleaned
    return f"{cleaned}USDT"


def normalize_interval_token(token: str) -> str:
    return token.strip().replace(" ", "").lower()


def looks_like_symbol_token(token: str) -> bool:
    cleaned = token.strip().upper().replace(" ", "")
    if cleaned in {"LONG", "SHORT", "BUY", "SELL", "BULLISH", "BEARISH"}:
        return False
    return bool(re.fullmatch(r"[A-Z]{2,15}(?:/USDT|USDT)?", cleaned))


def looks_like_interval_token(token: str) -> bool:
    return bool(re.fullmatch(r"\d+\s*[mhdw]", token.strip(), re.IGNORECASE))


def extract_numeric_values(text: str) -> List[float]:
    values: List[float] = []
    for match in re.findall(r"\d[\d,]*(?:\.\d+)?", text):
        values.append(float(match.replace(",", "")))
    return values


def extract_labeled_segment(text: str, label_pattern: str) -> str:
    pattern = rf"\b(?:{label_pattern})\b\s*[:=\-]?\s*(.*?){USER_SIGNAL_FIELD_LOOKAHEAD}"
    match = re.search(pattern, text, flags=re.IGNORECASE | re.DOTALL)
    if not match:
        return ""
    return match.group(1).strip(" \n\r\t-")


def detect_symbol_from_text(text: str, config: Config) -> str:
    labeled = extract_labeled_segment(text, r"PAIR|SYMBOL|COIN")
    if labeled:
        candidate = detect_symbol_from_text(labeled, config)
        if candidate:
            return candidate

    slash_match = re.search(r"\b([A-Z]{2,15})\s*/\s*USDT\b", text)
    if slash_match:
        return f"{slash_match.group(1)}USDT"

    full_match = re.search(r"\b([A-Z]{2,15}USDT)\b", text)
    if full_match:
        return full_match.group(1)

    for configured_symbol in config.symbols:
        if configured_symbol.endswith("USDT"):
            base_symbol = configured_symbol[:-4]
            if re.search(rf"\b{re.escape(base_symbol)}\b", text):
                return configured_symbol

    return ""


def detect_interval_from_text(text: str) -> str:
    labeled = extract_labeled_segment(text, r"TIMEFRAME|TF")
    if labeled:
        candidate = detect_interval_from_text(labeled)
        if candidate:
            return candidate

    match = re.search(r"\b(\d+)\s*([mhdw])\b", text, flags=re.IGNORECASE)
    if not match:
        return ""
    return f"{match.group(1)}{match.group(2).lower()}"


def detect_side_from_text(text: str) -> str:
    labeled = extract_labeled_segment(text, r"SIDE")
    search_text = labeled or text
    has_long = bool(re.search(r"\b(LONG|BUY|BULLISH)\b", search_text))
    has_short = bool(re.search(r"\b(SHORT|SELL|BEARISH)\b", search_text))

    if has_long and has_short:
        raise ValueError("Both LONG and SHORT found. Please send only one side.")
    if has_long:
        return "LONG"
    if has_short:
        return "SHORT"
    return ""


def extract_entry_range(text: str) -> Tuple[Optional[float], Optional[float], List[str]]:
    segment = extract_labeled_segment(text, r"ENTRY(?:\s+ZONE)?|BUY\s+ZONE|SELL\s+ZONE")
    if not segment:
        return None, None, []

    numbers = extract_numeric_values(segment)
    if not numbers:
        return None, None, []

    notes: List[str] = []
    if len(numbers) == 1:
        return numbers[0], numbers[0], notes

    entry_low = numbers[0]
    entry_high = numbers[1]
    if entry_low > entry_high:
        entry_low, entry_high = entry_high, entry_low
        notes.append("Entry range reordered low-to-high.")
    return entry_low, entry_high, notes


def extract_stop_loss(text: str) -> Optional[float]:
    segment = extract_labeled_segment(text, r"STOP(?:\s+LOSS)?|SL")
    if not segment:
        return None

    numbers = extract_numeric_values(segment)
    return numbers[0] if numbers else None


def extract_take_profits(text: str) -> List[float]:
    take_profits: List[float] = []

    for index in range(1, 4):
        segment = extract_labeled_segment(text, rf"TP\s*{index}|TARGET\s*{index}")
        if not segment:
            continue
        numbers = extract_numeric_values(segment)
        if numbers:
            take_profits.append(numbers[0])

    if take_profits:
        return take_profits

    targets_segment = extract_labeled_segment(text, r"TARGETS?|TPS?")
    if not targets_segment:
        return []

    return extract_numeric_values(targets_segment)[:3]


def infer_unlabeled_signal_levels(
    text: str,
) -> Tuple[Optional[float], Optional[float], Optional[float], List[float], List[str]]:
    sanitized = re.sub(r"\b\d+\s*[MHDW]\b", " ", text, flags=re.IGNORECASE)
    numbers = extract_numeric_values(sanitized)
    if not numbers:
        return None, None, None, [], []

    notes: List[str] = []
    cursor = 0
    entry_low: Optional[float]
    entry_high: Optional[float]

    if len(numbers) >= 2 and abs(numbers[0] - numbers[1]) / max(abs(numbers[1]), 1.0) <= 0.03:
        entry_low, entry_high = sorted([numbers[0], numbers[1]])
        cursor = 2
        notes.append("Entry range inferred from unlabeled numbers.")
    else:
        entry_low = numbers[0]
        entry_high = numbers[0]
        cursor = 1
        notes.append("Single entry inferred from unlabeled numbers.")

    stop_loss = numbers[cursor] if len(numbers) > cursor else None
    if stop_loss is not None:
        cursor += 1
        notes.append("Stop loss inferred from unlabeled numbers.")

    take_profits = numbers[cursor : cursor + 3]
    if take_profits:
        notes.append("Take profits inferred from unlabeled numbers.")

    return entry_low, entry_high, stop_loss, take_profits, notes


def infer_side_from_levels(
    entry_low: Optional[float],
    entry_high: Optional[float],
    stop_loss: Optional[float],
    take_profits: List[float],
) -> str:
    if entry_low is None or entry_high is None or stop_loss is None or not take_profits:
        return ""

    if stop_loss < entry_low and all(tp > entry_high for tp in take_profits):
        return "LONG"
    if stop_loss > entry_high and all(tp < entry_low for tp in take_profits):
        return "SHORT"
    return ""


def resolve_user_signal_value(field_name: str, detected: str, default: str) -> str:
    if detected and default and detected != default:
        raise ValueError(f"{field_name} mismatch. Command and signal text do not match.")
    return detected or default


def parse_user_signal_text(
    text: str,
    config: Config,
    default_symbol: str = "",
    default_interval: str = "",
    default_side: str = "",
) -> UserSignalInput:
    normalized_text = normalize_user_signal_text(text)
    notes: List[str] = []

    symbol = resolve_user_signal_value(
        "Pair",
        detect_symbol_from_text(normalized_text, config),
        normalize_symbol_token(default_symbol) if default_symbol else "",
    )
    interval = resolve_user_signal_value(
        "Timeframe",
        detect_interval_from_text(normalized_text),
        normalize_interval_token(default_interval) if default_interval else "",
    )
    side = resolve_user_signal_value(
        "Side",
        detect_side_from_text(normalized_text),
        default_side.strip().upper(),
    )
    entry_low, entry_high, entry_notes = extract_entry_range(normalized_text)
    stop_loss = extract_stop_loss(normalized_text)
    take_profits = extract_take_profits(normalized_text)
    notes.extend(entry_notes)

    if entry_low is None and stop_loss is None and not take_profits:
        (
            fallback_entry_low,
            fallback_entry_high,
            fallback_stop_loss,
            fallback_take_profits,
            fallback_notes,
        ) = infer_unlabeled_signal_levels(normalized_text)
        if fallback_entry_low is not None:
            entry_low = fallback_entry_low
            entry_high = fallback_entry_high
            stop_loss = fallback_stop_loss
            take_profits = fallback_take_profits
            notes.extend(fallback_notes)

    inferred_side = infer_side_from_levels(entry_low, entry_high, stop_loss, take_profits)
    if inferred_side:
        if side and side != inferred_side:
            raise ValueError("Side conflicts with entry/SL/TP structure.")
        if not side:
            side = inferred_side
            notes.append(f"Side inferred from levels as {side}.")

    if not symbol:
        raise ValueError("Pair missing. Use BTCUSDT or BTC/USDT.")
    if not interval:
        raise ValueError("Timeframe missing. Use 15m, 1h, or 4h.")
    interval_to_milliseconds(interval)

    return UserSignalInput(
        symbol=symbol,
        interval=interval,
        side=side,
        entry_low=entry_low,
        entry_high=entry_high,
        stop_loss=stop_loss,
        take_profits=take_profits,
        notes=notes,
        raw_text=text,
    )


def parse_user_signal_command_args(args: List[str], config: Config) -> UserSignalInput:
    if not args:
        raise ValueError("Signal input missing.")

    raw_text = " ".join(args)
    default_symbol = ""
    default_interval = ""
    default_side = ""
    remaining_text = raw_text

    if len(args) >= 2 and looks_like_symbol_token(args[0]) and looks_like_interval_token(args[1]):
        default_symbol = normalize_symbol_token(args[0])
        default_interval = normalize_interval_token(args[1])
        remaining_text = " ".join(args[2:])

        if len(args) >= 3 and args[2].strip().upper() in {"LONG", "SHORT"}:
            default_side = args[2].strip().upper()
            remaining_text = " ".join(args[3:])

    return parse_user_signal_text(
        remaining_text or raw_text,
        config,
        default_symbol=default_symbol,
        default_interval=default_interval,
        default_side=default_side,
    )


def format_user_signal_entry(signal: UserSignalInput) -> str:
    if signal.entry_low is None or signal.entry_high is None:
        return "-"
    if abs(signal.entry_low - signal.entry_high) < 1e-9:
        return format_price(signal.entry_low)
    return f"{format_price(signal.entry_low)} - {format_price(signal.entry_high)}"


def format_normalized_user_signal_lines(signal: UserSignalInput) -> List[str]:
    lines = [
        f"PAIR: {signal.symbol}",
        f"TIMEFRAME: {signal.interval}",
        f"SIDE: {signal.side or '-'}",
        f"ENTRY: {format_user_signal_entry(signal)}",
        f"SL: {format_price(signal.stop_loss) if signal.stop_loss is not None else '-'}",
    ]

    for index in range(3):
        tp_label = f"TP{index + 1}"
        if index < len(signal.take_profits):
            lines.append(f"{tp_label}: {format_price(signal.take_profits[index])}")
        else:
            lines.append(f"{tp_label}: -")

    return lines


def assess_user_signal_structure(signal: UserSignalInput) -> Tuple[str, str]:
    notes = list(signal.notes)
    missing_fields: List[str] = []
    issues: List[str] = []

    if not signal.side:
        missing_fields.append("SIDE")
    if signal.entry_low is None or signal.entry_high is None:
        missing_fields.append("ENTRY")
    if signal.stop_loss is None:
        missing_fields.append("SL")
    if len(signal.take_profits) < 3:
        for index in range(len(signal.take_profits) + 1, 4):
            missing_fields.append(f"TP{index}")

    if signal.side == "LONG" and signal.entry_low is not None and signal.entry_high is not None:
        if signal.stop_loss is not None and signal.stop_loss >= signal.entry_low:
            issues.append("LONG signal-e SL entry-r niche hote hobe.")
        if signal.take_profits:
            if any(tp <= signal.entry_high for tp in signal.take_profits):
                issues.append("LONG signal-e shob TP entry-r upore thakte hobe.")
            if any(curr <= prev for prev, curr in zip(signal.take_profits, signal.take_profits[1:])):
                issues.append("LONG TP order ascending hote hobe.")

    if signal.side == "SHORT" and signal.entry_low is not None and signal.entry_high is not None:
        if signal.stop_loss is not None and signal.stop_loss <= signal.entry_high:
            issues.append("SHORT signal-e SL entry-r upore hote hobe.")
        if signal.take_profits:
            if any(tp >= signal.entry_low for tp in signal.take_profits):
                issues.append("SHORT signal-e shob TP entry-r niche thakte hobe.")
            if any(curr >= prev for prev, curr in zip(signal.take_profits, signal.take_profits[1:])):
                issues.append("SHORT TP order descending hote hobe.")

    if issues:
        notes.extend(issues)
        return "NEED FIX", " | ".join(notes)

    if missing_fields:
        notes.append(f"Missing fields: {', '.join(missing_fields)}.")
        if not signal.side:
            notes.append("Bot will compare both LONG and SHORT.")
        return "PARTIAL", " | ".join(notes)

    notes.append("Signal normalized successfully.")
    return "OK", " | ".join(notes)


def build_user_signal_check_message(
    base_config: Config,
    symbol: str,
    interval: str,
    requested_side: Optional[str],
    parsed_signal: Optional[UserSignalInput] = None,
    format_verdict: str = "",
    format_note: str = "",
) -> str:
    symbol = symbol.strip().upper()
    interval = interval.strip()
    side = requested_side.strip().upper() if requested_side else ""

    if side and side not in {"LONG", "SHORT"}:
        raise ValueError("Side must be LONG or SHORT.")

    interval_to_milliseconds(interval)
    interval_config = config_for_interval(config_for_symbol(base_config, symbol), interval)
    candles = fetch_klines(interval_config)
    higher_timeframe_trend = fetch_confirmation_trend(interval_config, interval)
    higher_timeframe_overview = fetch_confirmation_overview(interval_config, interval)
    analysis = analyze_market(
        candles,
        interval_config,
        higher_timeframe_trend,
        higher_timeframe_overview,
        load_state(interval_config.state_file),
    )
    best_side, best_score, quality, note = classify_signal_checker_result(analysis, interval_config)
    current_price = candles[-1].close
    watch_threshold = max(interval_config.min_signal_score - interval_config.watch_alert_score_gap, 54)
    normalized_section = ""

    if parsed_signal:
        normalized_lines = "\n".join(format_normalized_user_signal_lines(parsed_signal))
        normalized_section = (
            f"Normalized Signal:\n{normalized_lines}\n\n"
            f"Format Verdict: {format_verdict or 'PARTIAL'}\n"
            f"Format Note: {format_note or 'Signal parsed.'}\n\n"
        )

    if side in {"LONG", "SHORT"}:
        requested_score = analysis.long_score if side == "LONG" else analysis.short_score

        if analysis.signal and analysis.signal.side == side:
            verdict = "GOOD"
            detail_note = f"Bot agrees: {analysis.signal.tier} {analysis.signal.grade}"
        elif requested_score >= watch_threshold:
            verdict = "WATCH"
            detail_note = "Setup close. Next candle confirm wait koro."
        else:
            verdict = "BAD"
            detail_note = "Current market-e ei side weak."

        if best_side in {"LONG", "SHORT"} and best_side != side and best_score >= requested_score + 6:
            verdict = "BAD"
            detail_note = f"Bot prefers {best_side} side right now."

        return (
            "USER SIGNAL CHECK\n\n"
            f"{normalized_section}"
            f"Pair: {symbol}\n"
            f"Timeframe: {interval}\n"
            f"Learning Mode: {'Adaptive feedback ON' if interval_config.adaptive_learning_enabled else 'OFF'}\n"
            f"User Side: {side}\n"
            f"Current Price: {format_price(current_price)}\n"
            f"LONG Score: {analysis.long_score}\n"
            f"SHORT Score: {analysis.short_score}\n"
            f"Requested Side Score: {requested_score}\n"
            f"Bot Best Side: {best_side} {best_score}\n"
            f"Bot Verdict: {verdict}\n"
            f"Quality Note: {detail_note}\n"
            f"Market Trend: {analysis.market_overview.trend}\n"
            f"Entry Verdict: {analysis.market_overview.entry_rule}\n"
            f"Breakout: {analysis.market_overview.breakout_check}\n"
            f"Momentum: {analysis.market_overview.volume_momentum}\n"
            f"Checker Note: {note}"
        )

    return (
        "USER SIGNAL CHECK\n\n"
        f"{normalized_section}"
        f"Pair: {symbol}\n"
        f"Timeframe: {interval}\n"
        f"Learning Mode: {'Adaptive feedback ON' if interval_config.adaptive_learning_enabled else 'OFF'}\n"
        f"Current Price: {format_price(current_price)}\n"
        f"LONG Score: {analysis.long_score}\n"
        f"SHORT Score: {analysis.short_score}\n"
        f"Bot Best Side: {best_side} {best_score}\n"
        f"Bot Verdict: {quality}\n"
        f"Quality Note: {note}\n"
        f"Market Trend: {analysis.market_overview.trend}\n"
        f"Entry Verdict: {analysis.market_overview.entry_rule}\n"
        f"Breakout: {analysis.market_overview.breakout_check}\n"
        f"Momentum: {analysis.market_overview.volume_momentum}"
    )


def build_status_message(config: Config, state: Dict[str, object]) -> str:
    performance_state = get_performance_state(state)
    overall_stats = get_stats_bucket(performance_state, "overall")
    active_trade_count = len(iter_active_trades(state))

    return (
        "Bot Status\n\n"
        "Profile: Real Breakout Scalping\n"
        f"Symbols: {', '.join(config.symbols)}\n"
        f"Intervals: {', '.join(config.intervals)}\n"
        f"Poll Seconds: {config.poll_seconds}\n"
        f"Hourly Update: {'ON' if config.hourly_update_enabled else 'OFF'} "
        f"({config.hourly_update_interval_minutes}m, {config.hourly_update_timeframe})\n"
        f"Alerts Chat ID: {config.telegram_chat_id or 'Not configured'}\n"
        f"Active Trades: {active_trade_count}\n"
        f"Signals Sent: {int(overall_stats.get('signals_sent', 0))}\n"
        f"Closed Trades: {int(overall_stats.get('closed_trades', 0))}\n"
        f"Win Rate: {calculate_win_rate(overall_stats):.1f}%\n"
        f"Total Result: {format_r_multiple(float(overall_stats.get('total_r', 0.0) or 0.0))}"
    )


def scan_markets_once(config: Config, state: Dict[str, object]) -> None:
    state_changed = False

    for symbol in config.symbols:
        symbol_config = config_for_symbol(config, symbol)
        symbol_state = get_symbol_state(state, symbol)

        for interval in symbol_config.intervals:
            try:
                if process_interval(symbol_config, interval, symbol_state, state):
                    state_changed = True
            except requests.RequestException as exc:
                LOGGER.warning("Network/API error on %s %s: %s", symbol, interval, exc)
            except Exception as exc:
                LOGGER.exception("Bot loop failed on %s %s: %s", symbol, interval, exc)

    try:
        if maybe_send_hourly_update(config, state):
            LOGGER.info(
                "Hourly market update sent for %s on %s.",
                ", ".join(config.symbols),
                config.hourly_update_timeframe,
            )
            state_changed = True
    except requests.RequestException as exc:
        LOGGER.warning("Hourly update send failed: %s", exc)

    try:
        if maybe_send_daily_report(config, state):
            LOGGER.info("Daily performance report sent for %s.", current_local_date())
            state_changed = True
    except requests.RequestException as exc:
        LOGGER.warning("Daily report send failed: %s", exc)

    try:
        if maybe_run_daily_training(state):
            LOGGER.info("Daily self-learning training completed for %s.", current_local_date())
            state_changed = True
    except Exception as exc:
        LOGGER.warning("Daily training failed: %s", exc)

    if state_changed:
        try:
            save_state(config.state_file, state)
        except OSError as exc:
            LOGGER.warning("Failed to save state: %s", exc)


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    config: Config = context.application.bot_data["config"]
    message = (
        "Bot is running.\n\n"
        "Profile: Real Breakout Scalping\n"
        f"Symbols: {', '.join(config.symbols)}\n"
        f"Intervals: {', '.join(config.intervals)}\n"
        f"Poll Seconds: {config.poll_seconds}\n"
        f"Hourly Update: {'ON' if config.hourly_update_enabled else 'OFF'} "
        f"({config.hourly_update_interval_minutes}m, {config.hourly_update_timeframe})\n"
        f"Alerts Chat ID: {config.telegram_chat_id or 'Not configured'}\n\n"
        "Commands:\n"
        "/status - live bot summary\n"
        "/checksignal - score check, good or bad\n"
        "/usersignal - send a structured or free-form signal\n"
        "/market - current market condition\n"
        "/accuracy - real accuracy tracker\n"
        "/active - open mentor trades\n"
        "/report - today's performance report\n"
        "/scan - run a manual scan now\n"
        "/demo - test message\n"
        "/chatid - show your chat id"
    )
    if update.effective_message:
        await update.effective_message.reply_text(message)


async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    config: Config = context.application.bot_data["config"]
    state: Dict[str, object] = context.application.bot_data["state"]
    if update.effective_message:
        await update.effective_message.reply_text(build_status_message(config, state))


async def checksignal_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    config: Config = context.application.bot_data["config"]
    if update.effective_message:
        await update.effective_message.reply_text("Checking live signal quality...")

    try:
        message = await asyncio.to_thread(build_signal_checker_message, config)
    except requests.RequestException as exc:
        LOGGER.warning("Signal checker failed due to network/API error: %s", exc)
        if update.effective_message:
            await update.effective_message.reply_text(f"Signal checker failed: {exc}")
        return
    except Exception as exc:
        LOGGER.exception("Signal checker crashed: %s", exc)
        if update.effective_message:
            await update.effective_message.reply_text(f"Signal checker crashed: {exc}")
        return

    if update.effective_message:
        await update.effective_message.reply_text(message)


async def reply_with_user_signal_check(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    parsed_signal: UserSignalInput,
) -> int:
    config: Config = context.application.bot_data["config"]
    if update.effective_message:
        await update.effective_message.reply_text("Checking your signal...")

    structure_verdict, structure_note = assess_user_signal_structure(parsed_signal)

    try:
        message = await asyncio.to_thread(
            build_user_signal_check_message,
            config,
            parsed_signal.symbol,
            parsed_signal.interval,
            parsed_signal.side or None,
            parsed_signal,
            structure_verdict,
            structure_note,
        )
    except requests.RequestException as exc:
        LOGGER.warning("User signal check failed due to network/API error: %s", exc)
        if update.effective_message:
            await update.effective_message.reply_text(f"User signal check failed: {exc}")
        return ConversationHandler.END
    except Exception as exc:
        LOGGER.exception("User signal check crashed: %s", exc)
        if update.effective_message:
            await update.effective_message.reply_text(f"User signal check crashed: {exc}")
        return ConversationHandler.END

    if update.effective_message:
        await update.effective_message.reply_text(message)
    return ConversationHandler.END


async def usersignal_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    config: Config = context.application.bot_data["config"]
    args = context.args

    if not args:
        if update.effective_message:
            await update.effective_message.reply_text(build_user_signal_help_message())
        return USER_SIGNAL_INPUT

    try:
        parsed_signal = parse_user_signal_command_args(args, config)
    except ValueError as exc:
        if update.effective_message:
            await update.effective_message.reply_text(
                f"Invalid input: {exc}\n\n{build_user_signal_help_message()}"
            )
        return USER_SIGNAL_INPUT

    return await reply_with_user_signal_check(update, context, parsed_signal)


async def usersignal_text_input(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    config: Config = context.application.bot_data["config"]
    text = update.effective_message.text if update.effective_message else ""

    try:
        parsed_signal = parse_user_signal_text(text, config)
    except ValueError as exc:
        if update.effective_message:
            await update.effective_message.reply_text(
                f"Invalid input: {exc}\n\n{build_user_signal_help_message()}"
            )
        return USER_SIGNAL_INPUT

    return await reply_with_user_signal_check(update, context, parsed_signal)


async def usersignal_cancel_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if update.effective_message:
        await update.effective_message.reply_text("User signal input cancelled.")
    return ConversationHandler.END


async def market_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    config: Config = context.application.bot_data["config"]
    messages: List[str] = []

    for symbol in config.symbols:
        symbol_config = config_for_symbol(config, symbol)
        for interval in config.intervals:
            interval_config = config_for_interval(symbol_config, interval)
            try:
                messages.append(await asyncio.to_thread(build_market_message, interval_config))
            except Exception as exc:
                LOGGER.warning("Market status build failed on %s %s: %s", symbol, interval, exc)
                messages.append(
                    "\n".join(
                        [
                            "MARKET STATUS",
                            "",
                            f"Pair: {symbol}",
                            f"Timeframe: {interval}",
                            f"Checked At: {format_now_local()}",
                            "",
                            f"- Could not build market condition: {exc}",
                        ]
                    )
                )

    if update.effective_message:
        for message in messages:
            await update.effective_message.reply_text(message)


async def accuracy_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    state: Dict[str, object] = context.application.bot_data["state"]
    if update.effective_message:
        await update.effective_message.reply_text(build_accuracy_message(state))


async def active_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    state: Dict[str, object] = context.application.bot_data["state"]
    if update.effective_message:
        await update.effective_message.reply_text(build_active_trades_message(state))


async def chatid_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id if update.effective_chat else "Unknown"
    if update.effective_message:
        await update.effective_message.reply_text(f"Your chat ID is: {chat_id}")


async def report_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    config: Config = context.application.bot_data["config"]
    state: Dict[str, object] = context.application.bot_data["state"]
    if update.effective_message:
        await update.effective_message.reply_text(
            build_daily_report_message(config, current_local_date(), state)
        )


async def demo_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    config: Config = context.application.bot_data["config"]
    message = await asyncio.to_thread(build_demo_message, config)
    if update.effective_message:
        await update.effective_message.reply_text(message)


async def scan_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    config: Config = context.application.bot_data["config"]
    state: Dict[str, object] = context.application.bot_data["state"]
    scan_lock: threading.Lock = context.application.bot_data["scan_lock"]

    if not scan_lock.acquire(blocking=False):
        if update.effective_message:
            await update.effective_message.reply_text("A scan is already running.")
        return

    try:
        await asyncio.to_thread(scan_markets_once, config, state)
    except requests.RequestException as exc:
        LOGGER.warning("Manual scan failed due to network/API error: %s", exc)
        if update.effective_message:
            await update.effective_message.reply_text(f"Manual scan failed: {exc}")
        return
    except Exception as exc:
        LOGGER.exception("Manual scan crashed: %s", exc)
        if update.effective_message:
            await update.effective_message.reply_text(f"Manual scan crashed: {exc}")
        return
    finally:
        scan_lock.release()

    if update.effective_message:
        await update.effective_message.reply_text(
            "Manual scan complete.\n\n" + build_status_message(config, state)
        )


async def market_scan_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    config: Config = context.application.bot_data["config"]
    state: Dict[str, object] = context.application.bot_data["state"]
    scan_lock: threading.Lock = context.application.bot_data["scan_lock"]

    if not scan_lock.acquire(blocking=False):
        LOGGER.info("Skipping scan cycle because a previous scan is still running.")
        return

    try:
        await asyncio.to_thread(scan_markets_once, config, state)
    finally:
        scan_lock.release()


def build_application(config: Config, state: Dict[str, object]) -> Application:
    application = Application.builder().token(config.telegram_token).build()
    application.bot_data["config"] = config
    application.bot_data["state"] = state
    application.bot_data["scan_lock"] = threading.Lock()
    usersignal_handler = ConversationHandler(
        entry_points=[CommandHandler("usersignal", usersignal_command)],
        states={
            USER_SIGNAL_INPUT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, usersignal_text_input)
            ]
        },
        fallbacks=[CommandHandler("cancel", usersignal_cancel_command)],
    )

    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("status", status_command))
    application.add_handler(CommandHandler("checksignal", checksignal_command))
    application.add_handler(usersignal_handler)
    application.add_handler(CommandHandler("market", market_command))
    application.add_handler(CommandHandler("accuracy", accuracy_command))
    application.add_handler(CommandHandler("active", active_command))
    application.add_handler(CommandHandler("report", report_command))
    application.add_handler(CommandHandler("scan", scan_command))
    application.add_handler(CommandHandler("chatid", chatid_command))
    application.add_handler(CommandHandler("demo", demo_command))
    application.job_queue.run_repeating(
        market_scan_job,
        interval=config.poll_seconds,
        first=1,
        name="market-scan",
    )
    return application


def run() -> None:
    self_learning.init_dataset_db()
    config = load_config()
    state = load_state(config.state_file)
    application = build_application(config, state)

    LOGGER.info(
        "Starting Railway-ready trader bot for %s on %s. Polling every %s seconds.",
        ", ".join(config.symbols),
        ", ".join(config.intervals),
        config.poll_seconds,
    )

    application.run_polling(drop_pending_updates=True)


def main() -> None:
    setup_logging()
    run()


if __name__ == "__main__":
    main()
