# Pro Trader Telegram Bot

Telegram-based crypto signal bot with mentor-style trade tracking.

## What It Does

- Scans multiple symbols and timeframes from Binance market data
- Scores LONG and SHORT breakout setups using EMA, RSI, ATR, volume, and structure
- Sends Telegram alerts with entry, stop loss, and 3 take-profit levels
- Tracks active trades and sends TP / close updates after candle close
- Stores signal stats and daily performance in `bot_state.json`

## Telegram Commands

- `/start` - bot overview and command list
- `/status` - live bot summary
- `/active` - active tracked trades
- `/report` - today's performance report
- `/scan` - trigger a manual scan
- `/demo` - send a demo signal message
- `/chatid` - show your Telegram chat id

## Required Environment Variables

- `BOT_TOKEN` - Telegram bot token
- `CHAT_ID` - destination Telegram chat id

## Optional Strategy Settings

- `BOT_SYMBOLS=BTCUSDT,ETHUSDT,ZECUSDT`
- `BOT_INTERVALS=3m,5m,15m`
- `BOT_POLL_SECONDS=20`
- `BOT_REQUIRE_HTF_CONFIRMATION=false`
- `BOT_MIN_SIGNAL_SCORE=72`
- `BOT_VIP_SIGNAL_SCORE=84`

## Run

```bash
python bot.py
```
