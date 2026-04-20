import json
import os
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple


ROOT_DIR = Path(__file__).resolve().parent
DATASET_DB_FILE = Path(
    os.getenv("BOT_TRAINING_DATASET_FILE", str(ROOT_DIR / "training_dataset.db"))
).expanduser()
MODEL_FILE = Path(
    os.getenv("BOT_TRAINING_MODEL_FILE", str(ROOT_DIR / "training_model.json"))
).expanduser()


def get_dataset_connection() -> sqlite3.Connection:
    connection = sqlite3.connect(DATASET_DB_FILE)
    connection.row_factory = sqlite3.Row
    return connection


def init_dataset_db() -> None:
    DATASET_DB_FILE.parent.mkdir(parents=True, exist_ok=True)
    with get_dataset_connection() as connection:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS signal_dataset (
                signal_key TEXT PRIMARY KEY,
                symbol TEXT NOT NULL,
                interval TEXT NOT NULL,
                side TEXT NOT NULL,
                candle_time INTEGER,
                opened_at TEXT,
                closed_at TEXT,
                tier TEXT,
                grade TEXT,
                score_base INTEGER,
                score_final INTEGER,
                entry REAL,
                stop_loss REAL,
                tp1 REAL,
                tp2 REAL,
                tp3 REAL,
                trend_bias TEXT,
                breakout_type TEXT,
                entry_rule TEXT,
                volume_ratio REAL,
                rsi REAL,
                body_ratio REAL,
                extension_atr REAL,
                htf_confirmed INTEGER,
                htf_fake_breakout INTEGER,
                structure_confirmed INTEGER,
                outcome TEXT,
                result_r REAL,
                tp1_hit INTEGER,
                tp2_hit INTEGER,
                close_reason TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_signal_dataset_closed ON signal_dataset(closed_at, symbol, interval)"
        )


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _to_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def record_signal(signal_key: str, row: Dict[str, Any]) -> None:
    init_dataset_db()
    now_iso = datetime.now().astimezone().isoformat()
    with get_dataset_connection() as connection:
        connection.execute(
            """
            INSERT INTO signal_dataset (
                signal_key, symbol, interval, side, candle_time, opened_at, closed_at,
                tier, grade, score_base, score_final, entry, stop_loss, tp1, tp2, tp3,
                trend_bias, breakout_type, entry_rule, volume_ratio, rsi, body_ratio,
                extension_atr, htf_confirmed, htf_fake_breakout, structure_confirmed,
                outcome, result_r, tp1_hit, tp2_hit, close_reason, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, 0, 0, NULL, ?, ?)
            ON CONFLICT(signal_key) DO UPDATE SET
                tier = excluded.tier,
                grade = excluded.grade,
                score_base = excluded.score_base,
                score_final = excluded.score_final,
                entry = excluded.entry,
                stop_loss = excluded.stop_loss,
                tp1 = excluded.tp1,
                tp2 = excluded.tp2,
                tp3 = excluded.tp3,
                trend_bias = excluded.trend_bias,
                breakout_type = excluded.breakout_type,
                entry_rule = excluded.entry_rule,
                volume_ratio = excluded.volume_ratio,
                rsi = excluded.rsi,
                body_ratio = excluded.body_ratio,
                extension_atr = excluded.extension_atr,
                htf_confirmed = excluded.htf_confirmed,
                htf_fake_breakout = excluded.htf_fake_breakout,
                structure_confirmed = excluded.structure_confirmed,
                updated_at = excluded.updated_at
            """,
            (
                signal_key,
                str(row.get("symbol", "")),
                str(row.get("interval", "")),
                str(row.get("side", "")),
                _to_int(row.get("candle_time")),
                str(row.get("opened_at", "")),
                str(row.get("tier", "")),
                str(row.get("grade", "")),
                _to_int(row.get("score_base")),
                _to_int(row.get("score_final")),
                _to_float(row.get("entry")),
                _to_float(row.get("stop_loss")),
                _to_float(row.get("tp1")),
                _to_float(row.get("tp2")),
                _to_float(row.get("tp3")),
                str(row.get("trend_bias", "")),
                str(row.get("breakout_type", "")),
                str(row.get("entry_rule", "")),
                _to_float(row.get("volume_ratio")),
                _to_float(row.get("rsi")),
                _to_float(row.get("body_ratio")),
                _to_float(row.get("extension_atr")),
                1 if row.get("htf_confirmed") else 0,
                1 if row.get("htf_fake_breakout") else 0,
                1 if row.get("structure_confirmed") else 0,
                now_iso,
                now_iso,
            ),
        )


def close_signal(signal_key: str, row: Dict[str, Any]) -> None:
    init_dataset_db()
    now_iso = datetime.now().astimezone().isoformat()
    with get_dataset_connection() as connection:
        connection.execute(
            """
            UPDATE signal_dataset
            SET
                closed_at = ?,
                outcome = ?,
                result_r = ?,
                tp1_hit = ?,
                tp2_hit = ?,
                close_reason = ?,
                updated_at = ?
            WHERE signal_key = ?
            """,
            (
                str(row.get("closed_at", "")),
                str(row.get("outcome", "")),
                _to_float(row.get("result_r")),
                1 if row.get("tp1_hit") else 0,
                1 if row.get("tp2_hit") else 0,
                str(row.get("close_reason", "")),
                now_iso,
                signal_key,
            ),
        )


def _compute_adjustment(closed_trades: int, wins: int, total_r: float, tp1_hits: int) -> int:
    if closed_trades < 5:
        return 0
    win_rate = wins / max(closed_trades, 1)
    average_r = total_r / max(closed_trades, 1)
    tp1_rate = tp1_hits / max(closed_trades, 1)
    if win_rate >= 0.65 and tp1_rate >= 0.60 and average_r > 0:
        return 4
    if win_rate >= 0.55 and average_r >= 0:
        return 2
    if win_rate <= 0.35 or average_r < -0.20:
        return -4
    if win_rate <= 0.45:
        return -2
    return 0


def train_model() -> Dict[str, Any]:
    init_dataset_db()
    with get_dataset_connection() as connection:
        rows = connection.execute(
            """
            SELECT *
            FROM signal_dataset
            WHERE closed_at IS NOT NULL AND outcome IS NOT NULL
            ORDER BY candle_time ASC
            """
        ).fetchall()

    total_closed = len(rows)
    wins = sum(1 for row in rows if str(row["outcome"]) == "wins")
    tp1_hits = sum(int(row["tp1_hit"] or 0) for row in rows)
    total_r = sum(_to_float(row["result_r"]) for row in rows)

    segments: Dict[str, Dict[str, Any]] = {}
    regime_segments: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        pair_key = f"{row['symbol']}|{row['interval']}|{row['side']}"
        regime_key = f"{row['symbol']}|{row['interval']}|{row['side']}|{row['trend_bias']}|{row['breakout_type']}"
        for key, container in ((pair_key, segments), (regime_key, regime_segments)):
            bucket = container.setdefault(
                key,
                {"closed_trades": 0, "wins": 0, "tp1_hits": 0, "total_r": 0.0},
            )
            bucket["closed_trades"] += 1
            bucket["wins"] += 1 if str(row["outcome"]) == "wins" else 0
            bucket["tp1_hits"] += int(row["tp1_hit"] or 0)
            bucket["total_r"] += _to_float(row["result_r"])

    for container in (segments, regime_segments):
        for bucket in container.values():
            bucket["adjustment"] = _compute_adjustment(
                int(bucket["closed_trades"]),
                int(bucket["wins"]),
                float(bucket["total_r"]),
                int(bucket["tp1_hits"]),
            )
            bucket["win_rate"] = round(bucket["wins"] / max(bucket["closed_trades"], 1), 4)
            bucket["avg_r"] = round(bucket["total_r"] / max(bucket["closed_trades"], 1), 4)

    model = {
        "generated_at": datetime.now().astimezone().isoformat(),
        "total_closed_trades": total_closed,
        "overall_win_rate": round(wins / max(total_closed, 1), 4) if total_closed else 0.0,
        "overall_total_r": round(total_r, 4),
        "overall_tp1_rate": round(tp1_hits / max(total_closed, 1), 4) if total_closed else 0.0,
        "pair_segments": segments,
        "regime_segments": regime_segments,
    }
    MODEL_FILE.write_text(json.dumps(model, indent=2), encoding="utf-8")
    return model


def load_model() -> Dict[str, Any]:
    if not MODEL_FILE.exists():
        return {}
    try:
        return json.loads(MODEL_FILE.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def score_adjustment(
    model: Dict[str, Any],
    *,
    symbol: str,
    interval: str,
    side: str,
    trend_bias: str,
    breakout_type: str,
) -> Tuple[int, str]:
    if not model:
        return 0, "Training model not ready"

    pair_key = f"{symbol}|{interval}|{side}"
    regime_key = f"{symbol}|{interval}|{side}|{trend_bias}|{breakout_type}"
    pair_bucket = model.get("pair_segments", {}).get(pair_key, {})
    regime_bucket = model.get("regime_segments", {}).get(regime_key, {})

    pair_adjustment = int(pair_bucket.get("adjustment", 0) or 0)
    regime_adjustment = int(regime_bucket.get("adjustment", 0) or 0)
    total_adjustment = max(-6, min(6, pair_adjustment + regime_adjustment))

    notes: List[str] = []
    if pair_bucket:
        notes.append(f"pair adj {pair_adjustment}")
    if regime_bucket:
        notes.append(f"regime adj {regime_adjustment}")
    if not notes:
        notes.append("no trained segment match")
    return total_adjustment, " | ".join(notes)


def build_training_report(model: Dict[str, Any]) -> str:
    if not model:
        return "Training model not ready yet."

    pair_segments = model.get("pair_segments", {})
    top_segments = sorted(
        pair_segments.items(),
        key=lambda item: (
            int(item[1].get("adjustment", 0) or 0),
            float(item[1].get("win_rate", 0.0) or 0.0),
            int(item[1].get("closed_trades", 0) or 0),
        ),
        reverse=True,
    )[:5]

    lines = [
        "TRAINING REPORT",
        "",
        f"Generated At: {model.get('generated_at', '-')}",
        f"Closed Trades Used: {int(model.get('total_closed_trades', 0) or 0)}",
        f"Overall Win Rate: {float(model.get('overall_win_rate', 0.0) or 0.0) * 100:.2f}%",
        f"Overall Result: {float(model.get('overall_total_r', 0.0) or 0.0):+.2f}R",
        f"TP1 Hit Rate: {float(model.get('overall_tp1_rate', 0.0) or 0.0) * 100:.2f}%",
        "",
        "Top Learned Segments:",
    ]

    if not top_segments:
        lines.append("- No trained segments yet")
    else:
        for key, bucket in top_segments:
            lines.append(
                f"- {key}: adj {int(bucket.get('adjustment', 0) or 0):+d} | "
                f"win {float(bucket.get('win_rate', 0.0) or 0.0) * 100:.1f}% | "
                f"closed {int(bucket.get('closed_trades', 0) or 0)} | "
                f"avgR {float(bucket.get('avg_r', 0.0) or 0.0):+.2f}"
            )
    return "\n".join(lines)
