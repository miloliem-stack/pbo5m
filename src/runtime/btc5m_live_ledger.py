from __future__ import annotations

import json
import sqlite3
from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Any, Optional

from ..time_utils import isoformat_utc, utc_now


SCHEMA = """
CREATE TABLE IF NOT EXISTS live_orders (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    strategy_id TEXT,
    market_id TEXT,
    condition_id TEXT,
    token_id TEXT,
    side TEXT,
    order_id TEXT,
    client_order_id TEXT,
    idempotency_key TEXT UNIQUE,
    order_type TEXT,
    limit_price REAL,
    intended_notional_usd REAL,
    submitted_ts TEXT,
    terminal_status TEXT,
    raw_response_json TEXT
);
CREATE TABLE IF NOT EXISTS live_fills (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id TEXT,
    market_id TEXT,
    condition_id TEXT,
    token_id TEXT,
    side TEXT,
    fill_qty_shares REAL,
    avg_fill_price REAL,
    spent_pusd REAL,
    fill_ts TEXT,
    trade_id TEXT,
    source_key TEXT UNIQUE,
    raw_response_json TEXT
);
CREATE TABLE IF NOT EXISTS outcome_lots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    market_id TEXT,
    condition_id TEXT,
    token_id TEXT,
    side TEXT,
    acquired_qty REAL,
    remaining_qty REAL,
    avg_cost REAL,
    status TEXT,
    source_order_id TEXT,
    source_fill_id INTEGER,
    created_ts TEXT,
    updated_ts TEXT
);
CREATE TABLE IF NOT EXISTS market_resolution_state (
    condition_id TEXT PRIMARY KEY,
    market_id TEXT,
    resolved INTEGER,
    winning_side TEXT,
    payout_vector_json TEXT,
    resolution_source TEXT,
    resolved_ts TEXT,
    last_checked_ts TEXT
);
CREATE TABLE IF NOT EXISTS redemption_attempts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    condition_id TEXT,
    market_id TEXT,
    token_ids_json TEXT,
    index_sets_json TEXT,
    pre_yes_balance REAL,
    pre_no_balance REAL,
    tx_hash TEXT,
    status TEXT,
    error_code TEXT,
    raw_error TEXT,
    created_ts TEXT,
    confirmed_ts TEXT
);
CREATE TABLE IF NOT EXISTS redeemed_lots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    condition_id TEXT,
    market_id TEXT,
    pre_token_balance REAL,
    redeemed_pusd_amount REAL,
    tx_hash TEXT,
    receipt_json TEXT,
    created_ts TEXT
);
"""


class LiveLedger:
    def __init__(self, path: str | Path = "state/btc5m_live_ledger.db") -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.ensure_schema()

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        return conn

    def ensure_schema(self) -> None:
        with sqlite3.connect(self.path) as conn:
            conn.executescript(SCHEMA)

    def record_order_intent(self, intent: Any, *, raw_response: Optional[dict[str, Any]] = None) -> None:
        now = isoformat_utc(utc_now())
        payload = _intent_to_dict(intent)
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO live_orders (
                    strategy_id, market_id, condition_id, token_id, side, order_id, client_order_id,
                    idempotency_key, order_type, limit_price, intended_notional_usd, submitted_ts,
                    terminal_status, raw_response_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(idempotency_key) DO UPDATE SET
                    raw_response_json=excluded.raw_response_json
                """,
                (
                    payload.get("policy_id"),
                    payload.get("market_id"),
                    payload.get("condition_id"),
                    payload.get("token_id"),
                    payload.get("selected_side"),
                    None,
                    payload.get("client_order_id"),
                    payload.get("idempotency_key"),
                    "FAK",
                    payload.get("limit_price") or payload.get("max_price"),
                    payload.get("stake_usd"),
                    now,
                    "intent_created",
                    _json(raw_response or payload),
                ),
            )

    def record_order_submission(self, intent: Any, *, order_id: Optional[str], response: dict[str, Any]) -> None:
        payload = _intent_to_dict(intent)
        now = isoformat_utc(utc_now())
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE live_orders
                SET order_id=?, submitted_ts=?, terminal_status=?, raw_response_json=?
                WHERE idempotency_key=?
                """,
                (order_id, now, str(response.get("status") or "submitted"), _json(response), payload.get("idempotency_key")),
            )

    def record_order_event(self, event: dict[str, Any]) -> None:
        status = str(event.get("event_type") or event.get("clob_status") or "")
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE live_orders
                SET terminal_status=?, raw_response_json=?
                WHERE idempotency_key=?
                """,
                (status, _json(event.get("raw_response") or event), event.get("idempotency_key")),
            )
        if event.get("event_type") in {"order_filled", "order_partially_filled"}:
            self.record_fill_from_event(event)

    def record_fill_from_event(self, event: dict[str, Any]) -> bool:
        qty = _float(event.get("filled_size"))
        price = _float(event.get("avg_fill_price")) or _float(event.get("selected_ask"))
        if qty is None or qty <= 0:
            return False
        spent = qty * price if price is not None else None
        trade_id = _first(event.get("raw_response") or {}, "trade_id", "tradeID", "transaction_hash")
        source_key = str(trade_id or f"{event.get('order_id')}:{qty}:{price}")
        now = isoformat_utc(utc_now())
        with self.connect() as conn:
            try:
                cur = conn.execute(
                    """
                    INSERT INTO live_fills (
                        order_id, market_id, condition_id, token_id, side, fill_qty_shares, avg_fill_price,
                        spent_pusd, fill_ts, trade_id, source_key, raw_response_json
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        event.get("order_id"),
                        event.get("market_id"),
                        event.get("condition_id"),
                        event.get("token_id"),
                        event.get("selected_side"),
                        qty,
                        price,
                        spent,
                        now,
                        trade_id,
                        source_key,
                        _json(event.get("raw_response") or event),
                    ),
                )
            except sqlite3.IntegrityError:
                return False
            fill_id = int(cur.lastrowid)
            conn.execute(
                """
                INSERT INTO outcome_lots (
                    market_id, condition_id, token_id, side, acquired_qty, remaining_qty, avg_cost,
                    status, source_order_id, source_fill_id, created_ts, updated_ts
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event.get("market_id"),
                    event.get("condition_id"),
                    event.get("token_id"),
                    event.get("selected_side"),
                    qty,
                    qty,
                    price,
                    "open",
                    event.get("order_id"),
                    fill_id,
                    now,
                    now,
                ),
            )
        return True

    def upsert_resolution(self, *, condition_id: str, market_id: Optional[str], resolved: bool, winning_side: str = "UNKNOWN", source: str = "manual", payout_vector: Any = None) -> None:
        now = isoformat_utc(utc_now())
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO market_resolution_state (
                    condition_id, market_id, resolved, winning_side, payout_vector_json,
                    resolution_source, resolved_ts, last_checked_ts
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(condition_id) DO UPDATE SET
                    market_id=excluded.market_id,
                    resolved=excluded.resolved,
                    winning_side=excluded.winning_side,
                    payout_vector_json=excluded.payout_vector_json,
                    resolution_source=excluded.resolution_source,
                    resolved_ts=excluded.resolved_ts,
                    last_checked_ts=excluded.last_checked_ts
                """,
                (condition_id, market_id, int(resolved), winning_side, _json(payout_vector), source, now if resolved else None, now),
            )

    def redeemable_lots(self) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT l.*, r.resolved, r.winning_side
                FROM outcome_lots l
                JOIN market_resolution_state r ON r.condition_id = l.condition_id
                WHERE l.status IN ('open', 'resolved_win') AND r.resolved=1 AND r.winning_side = l.side
                """
            ).fetchall()
        return [dict(row) for row in rows]

    def terminalize_resolved_lots(self) -> None:
        now = isoformat_utc(utc_now())
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE outcome_lots
                SET status='resolved_win', updated_ts=?
                WHERE status='open' AND EXISTS (
                    SELECT 1 FROM market_resolution_state r
                    WHERE r.condition_id=outcome_lots.condition_id
                      AND r.resolved=1 AND r.winning_side=outcome_lots.side
                )
                """,
                (now,),
            )
            conn.execute(
                """
                UPDATE outcome_lots
                SET status='resolved_loss', remaining_qty=0, updated_ts=?
                WHERE status='open' AND EXISTS (
                    SELECT 1 FROM market_resolution_state r
                    WHERE r.condition_id=outcome_lots.condition_id
                      AND r.resolved=1 AND r.winning_side != outcome_lots.side
                      AND r.winning_side != 'UNKNOWN'
                )
                """,
                (now,),
            )

    def record_redemption_attempt(self, *, condition_id: str, market_id: Optional[str], token_ids: list[str], index_sets: list[int], status: str, tx_hash: Optional[str] = None, raw_error: Optional[str] = None) -> int:
        now = isoformat_utc(utc_now())
        with self.connect() as conn:
            cur = conn.execute(
                """
                INSERT INTO redemption_attempts (
                    condition_id, market_id, token_ids_json, index_sets_json, tx_hash,
                    status, raw_error, created_ts
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (condition_id, market_id, _json(token_ids), _json(index_sets), tx_hash, status, raw_error, now),
            )
            return int(cur.lastrowid)

    def mark_lots_redeemed(self, *, condition_id: str, tx_hash: str, redeemed_pusd_amount: Optional[float] = None, receipt: Any = None) -> None:
        now = isoformat_utc(utc_now())
        with self.connect() as conn:
            total = conn.execute(
                "SELECT COALESCE(SUM(remaining_qty), 0) FROM outcome_lots WHERE condition_id=? AND status IN ('resolved_win', 'open')",
                (condition_id,),
            ).fetchone()[0]
            conn.execute(
                "UPDATE outcome_lots SET status='redeemed', remaining_qty=0, updated_ts=? WHERE condition_id=? AND status IN ('resolved_win', 'open')",
                (now, condition_id),
            )
            conn.execute(
                """
                INSERT INTO redeemed_lots (condition_id, pre_token_balance, redeemed_pusd_amount, tx_hash, receipt_json, created_ts)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (condition_id, total, redeemed_pusd_amount, tx_hash, _json(receipt), now),
            )

    def open_reserved_pusd(self) -> float:
        with self.connect() as conn:
            value = conn.execute(
                """
                SELECT COALESCE(SUM(intended_notional_usd), 0)
                FROM live_orders
                WHERE terminal_status IN ('intent_created', 'submitted', 'open', 'order_status_polled')
                """
            ).fetchone()[0]
        return float(value or 0.0)

    def unredeemed_winning_estimate(self) -> float:
        with self.connect() as conn:
            value = conn.execute(
                """
                SELECT COALESCE(SUM(remaining_qty), 0)
                FROM outcome_lots l
                JOIN market_resolution_state r ON r.condition_id=l.condition_id
                WHERE l.status IN ('open', 'resolved_win') AND r.resolved=1 AND r.winning_side=l.side
                """
            ).fetchone()[0]
        return float(value or 0.0)


def _intent_to_dict(intent: Any) -> dict[str, Any]:
    if is_dataclass(intent):
        return asdict(intent)
    if isinstance(intent, dict):
        return dict(intent)
    return {key: getattr(intent, key, None) for key in dir(intent) if not key.startswith("_")}


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, default=str)


def _float(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _first(row: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if row.get(key) not in (None, ""):
            return row[key]
    return None
