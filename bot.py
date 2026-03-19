import asyncio
import copy
import json
import logging
import os
import threading
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes


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
    "ZECUSDT",
    "ETHUSDT",
    "BTCUSDT",
]
DEFAULT_INTERVALS = [
    "15m",
    "1h",
    "4h",
]


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
    max_extension_atr: float
    atr_stop_multiplier: float
    tp_one_r: float
    tp_two_r: float
    tp_three_r: float
    daily_report_hour: int
    state_file: Path


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
        poll_seconds=int(os.getenv("BOT_POLL_SECONDS", "20")),
        lookback_limit=int(os.getenv("BOT_LOOKBACK_LIMIT", "320")),
        ema_fast_period=int(os.getenv("BOT_EMA_FAST", "50")),
        ema_slow_period=int(os.getenv("BOT_EMA_SLOW", "200")),
        rsi_period=int(os.getenv("BOT_RSI_PERIOD", "14")),
        atr_period=int(os.getenv("BOT_ATR_PERIOD", "14")),
        sr_lookback=int(os.getenv("BOT_SR_LOOKBACK", "20")),
        volume_period=int(os.getenv("BOT_VOLUME_PERIOD", "20")),
        volume_spike_factor=float(os.getenv("BOT_VOLUME_SPIKE_FACTOR", "1.02")),
        breakout_buffer_pct=float(os.getenv("BOT_BREAKOUT_BUFFER_PCT", "0.0006")),
        cooldown_candles=int(os.getenv("BOT_COOLDOWN_CANDLES", "2")),
        min_signal_score=int(os.getenv("BOT_MIN_SIGNAL_SCORE", "66")),
        vip_signal_score=int(os.getenv("BOT_VIP_SIGNAL_SCORE", "78")),
        normal_risk_pct=float(os.getenv("BOT_NORMAL_RISK_PCT", "0.5")),
        vip_risk_pct=float(os.getenv("BOT_VIP_RISK_PCT", "0.8")),
        normal_leverage=os.getenv("BOT_NORMAL_LEVERAGE", "3x-5x").strip(),
        vip_leverage=os.getenv("BOT_VIP_LEVERAGE", "5x-8x").strip(),
        margin_mode=os.getenv("BOT_MARGIN_MODE", "Isolated").strip() or "Isolated",
        require_higher_timeframe_confirmation=parse_bool_env(
            "BOT_REQUIRE_HTF_CONFIRMATION", False
        ),
        max_extension_atr=float(os.getenv("BOT_MAX_EXTENSION_ATR", "2.3")),
        atr_stop_multiplier=float(os.getenv("BOT_ATR_STOP_MULTIPLIER", "1.2")),
        tp_one_r=float(os.getenv("BOT_TP1_R", "1.5")),
        tp_two_r=float(os.getenv("BOT_TP2_R", "2.5")),
        tp_three_r=float(os.getenv("BOT_TP3_R", "4.0")),
        daily_report_hour=int(os.getenv("BOT_DAILY_REPORT_HOUR", "23")),
        state_file=Path(os.getenv("BOT_STATE_FILE", str(STATE_FILE))).expanduser(),
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
        max_extension_atr=config.max_extension_atr,
        atr_stop_multiplier=config.atr_stop_multiplier,
        tp_one_r=config.tp_one_r,
        tp_two_r=config.tp_two_r,
        tp_three_r=config.tp_three_r,
        daily_report_hour=config.daily_report_hour,
        state_file=config.state_file,
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
        max_extension_atr=config.max_extension_atr,
        atr_stop_multiplier=config.atr_stop_multiplier,
        tp_one_r=config.tp_one_r,
        tp_two_r=config.tp_two_r,
        tp_three_r=config.tp_three_r,
        daily_report_hour=config.daily_report_hour,
        state_file=config.state_file,
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
            "result_r": round(realized_r, 4),
            "exit_price": round_price(exit_price),
            "closed_at": format_local_time(candle_time),
            "close_reason": close_reason,
        }
    )
    performance_state["recent_closed"] = recent_closed[-25:]


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


def build_active_trade(signal: Signal, sent_time: str) -> Dict[str, object]:
    return {
        "tier": signal.tier,
        "grade": signal.grade,
        "setup_type": signal.setup_type,
        "side": signal.side,
        "score": signal.score,
        "entry": signal.entry,
        "market_structure_level": signal.market_structure_level,
        "atr": signal.atr,
        "stop_loss": signal.stop_loss,
        "current_stop_loss": signal.stop_loss,
        "take_profits": signal.take_profits,
        "candle_time": signal.candle_time,
        "opened_at": sent_time,
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
        "3m": "15m",
        "5m": "15m",
        "15m": "1h",
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
    )


def evaluate_long_setup(
    candles: List[Candle],
    ema_fast: List[float],
    ema_slow: List[float],
    rsi_values: List[float],
    atr_values: List[float],
    config: Config,
    higher_timeframe_trend: Optional[TrendSnapshot],
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
    trend_up = last.close > ema_fast[-1] > ema_slow[-1] and ema_fast[-1] > ema_fast[-2]
    breakout = last.close > resistance * (1 + config.breakout_buffer_pct)
    retest_hold = last.low <= resistance * (1 + config.breakout_buffer_pct) and last.close > resistance
    volume_spike = last.volume >= avg_volume * config.volume_spike_factor
    rsi_ok = 50 <= rsi_values[-1] <= 72
    bullish_body = last.close > last.open and (body_size / candle_range) >= 0.45
    close_near_high = (last.high - last.close) <= candle_range * 0.45
    not_overextended = extension_atr <= config.max_extension_atr

    if htf_confirmed:
        score += 25
        reasons.append(f"Higher timeframe trend confirms LONG on {higher_timeframe_trend.interval}")
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

    structure_confirmed = breakout or retest_hold

    mandatory_checks = [trend_up, structure_confirmed, bullish_body, not_overextended]
    if config.require_higher_timeframe_confirmation and confirmation_interval(config.interval):
        mandatory_checks.append(htf_confirmed)

    if not all(mandatory_checks) or score < config.min_signal_score:
        return score, None

    stop_loss = min(last.low - (atr * 0.2), last.close - (atr * config.atr_stop_multiplier))
    stop_loss = max(stop_loss, recent_support - atr)

    return score, build_signal(
        side="LONG",
        score=score,
        entry=last.close,
        stop_loss=stop_loss,
        reasons=reasons,
        candle_time=last.open_time,
        market_structure_level=resistance,
        atr=atr,
        config=config,
    )


def evaluate_short_setup(
    candles: List[Candle],
    ema_fast: List[float],
    ema_slow: List[float],
    rsi_values: List[float],
    atr_values: List[float],
    config: Config,
    higher_timeframe_trend: Optional[TrendSnapshot],
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
    trend_down = last.close < ema_fast[-1] < ema_slow[-1] and ema_fast[-1] < ema_fast[-2]
    breakdown = last.close < support * (1 - config.breakout_buffer_pct)
    retest_fail = last.high >= support * (1 - config.breakout_buffer_pct) and last.close < support
    volume_spike = last.volume >= avg_volume * config.volume_spike_factor
    rsi_ok = 28 <= rsi_values[-1] <= 50
    bearish_body = last.close < last.open and (body_size / candle_range) >= 0.45
    close_near_low = (last.close - last.low) <= candle_range * 0.45
    not_overextended = extension_atr <= config.max_extension_atr

    if htf_confirmed:
        score += 25
        reasons.append(f"Higher timeframe trend confirms SHORT on {higher_timeframe_trend.interval}")
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

    structure_confirmed = breakdown or retest_fail

    mandatory_checks = [trend_down, structure_confirmed, bearish_body, not_overextended]
    if config.require_higher_timeframe_confirmation and confirmation_interval(config.interval):
        mandatory_checks.append(htf_confirmed)

    if not all(mandatory_checks) or score < config.min_signal_score:
        return score, None

    stop_loss = max(last.high + (atr * 0.2), last.close + (atr * config.atr_stop_multiplier))
    stop_loss = min(stop_loss, recent_resistance + atr)

    return score, build_signal(
        side="SHORT",
        score=score,
        entry=last.close,
        stop_loss=stop_loss,
        reasons=reasons,
        candle_time=last.open_time,
        market_structure_level=support,
        atr=atr,
        config=config,
    )


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
) -> Optional[Signal]:
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
        candles, ema_fast, ema_slow, rsi_values, atr_values, config, higher_timeframe_trend
    )
    short_score, short_signal = evaluate_short_setup(
        candles, ema_fast, ema_slow, rsi_values, atr_values, config, higher_timeframe_trend
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
        return selected_signal

    LOGGER.info(
        "No trade setup on %s %s. Long score=%s, Short score=%s",
        config.symbol,
        config.interval,
        long_score,
        short_score,
    )
    return None


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

    signal = analyze_market(candles, interval_config, higher_timeframe_trend)
    if not signal:
        market_overview = calculate_market_overview_for_candles(candles, interval_config)
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
        replace_state(interval_state, working_interval_state)
        return True

    if is_in_cooldown(signal, working_interval_state, interval_config):
        LOGGER.info("Signal skipped due to cooldown: %s on %s", signal.side, interval)
        replace_state(interval_state, working_interval_state)
        return True

    sent_time = format_now_local()
    send_telegram(format_signal_message(signal, interval_config, sent_time), interval_config)
    update_signal_stats(root_state, signal, interval_config)
    working_interval_state["active_trade"] = build_active_trade(signal, sent_time)
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


def build_status_message(config: Config, state: Dict[str, object]) -> str:
    performance_state = get_performance_state(state)
    overall_stats = get_stats_bucket(performance_state, "overall")
    active_trade_count = len(iter_active_trades(state))

    return (
        "Bot Status\n\n"
        f"Symbols: {', '.join(config.symbols)}\n"
        f"Intervals: {', '.join(config.intervals)}\n"
        f"Poll Seconds: {config.poll_seconds}\n"
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
        if maybe_send_daily_report(config, state):
            LOGGER.info("Daily performance report sent for %s.", current_local_date())
            state_changed = True
    except requests.RequestException as exc:
        LOGGER.warning("Daily report send failed: %s", exc)

    if state_changed:
        try:
            save_state(config.state_file, state)
        except OSError as exc:
            LOGGER.warning("Failed to save state: %s", exc)


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    config: Config = context.application.bot_data["config"]
    message = (
        "Bot is running.\n\n"
        f"Symbols: {', '.join(config.symbols)}\n"
        f"Intervals: {', '.join(config.intervals)}\n"
        f"Poll Seconds: {config.poll_seconds}\n"
        f"Alerts Chat ID: {config.telegram_chat_id or 'Not configured'}\n\n"
        "Commands:\n"
        "/status - live bot summary\n"
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

    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("status", status_command))
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
