from __future__ import annotations

import json
from pathlib import Path

from scripts.reconcile_btc5m_ledger_from_execution_journal import reconcile
from src.runtime.btc5m_live_ledger import LiveLedger


def test_reconstructs_order_fill_lot_idempotently_from_jsonl(tmp_path: Path):
    journal_root = tmp_path / "journal"
    path = journal_root / "2026-05-27" / "execution_events.jsonl"
    path.parent.mkdir(parents=True)
    events = [
        {
            "event_type": "order_intent_created",
            "policy_id": "brownian_no_hmm_conservative_v1",
            "market_id": "m1",
            "condition_id": "c1",
            "token_id": "tok",
            "selected_side": "YES",
            "client_order_id": "cid",
            "idempotency_key": "idem",
            "limit_price": 0.4,
            "stake_usd": 5,
            "execution_ts": "2026-05-27T00:00:00+00:00",
        },
        {
            "event_type": "order_filled",
            "order_id": "ord1",
            "market_id": "m1",
            "condition_id": "c1",
            "token_id": "tok",
            "selected_side": "YES",
            "filled_size": 10,
            "avg_fill_price": 0.4,
            "raw_response": {"trade_id": "trade1"},
            "execution_ts": "2026-05-27T00:00:01+00:00",
        },
        {
            "event_type": "order_filled",
            "order_id": "ord1",
            "market_id": "m1",
            "condition_id": "c1",
            "token_id": "tok",
            "selected_side": "YES",
            "filled_size": 10,
            "avg_fill_price": 0.4,
            "raw_response": {"trade_id": "trade1"},
            "execution_ts": "2026-05-27T00:00:01+00:00",
        },
    ]
    path.write_text("\n".join(json.dumps(e) for e in events), encoding="utf-8")
    ledger = LiveLedger(tmp_path / "ledger.db")

    first = reconcile(ledger, journal_root=journal_root)
    second = reconcile(ledger, journal_root=journal_root)

    assert first["fills_inserted"] == 1
    assert first["skipped_duplicates"] == 1
    assert second["fills_inserted"] == 0
    assert ledger.count_rows("live_fills") == 1
    assert ledger.count_rows("outcome_lots") == 1
