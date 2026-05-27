from __future__ import annotations

from pathlib import Path

from scripts.update_btc5m_market_resolutions import update_once
from src.runtime.btc5m_live_ledger import LiveLedger


class Source:
    def __init__(self, resolved=True, side="YES"):
        self.resolved = resolved
        self.side = side

    def resolve(self, lot):
        return {"resolved": self.resolved, "winning_side": self.side, "source": "mock"}


def _ledger(tmp_path: Path) -> LiveLedger:
    ledger = LiveLedger(tmp_path / "ledger.db")
    ledger.record_fill_from_event(
        {
            "event_type": "order_filled",
            "order_id": "ord1",
            "market_id": "m1",
            "condition_id": "c1",
            "token_id": "tok",
            "selected_side": "YES",
            "filled_size": 1,
            "avg_fill_price": 0.4,
            "raw_response": {"trade_id": "trade1"},
        }
    )
    return ledger


def test_updates_win_lots_from_mocked_resolution_source(tmp_path: Path):
    ledger = _ledger(tmp_path)

    summary = update_once(ledger, source=Source(resolved=True, side="YES"))

    assert summary["resolved_wins"] == 1
    with ledger.connect() as conn:
        status = conn.execute("SELECT status FROM outcome_lots").fetchone()[0]
    assert status == "resolved_win"


def test_unresolved_markets_do_not_become_redeemable(tmp_path: Path):
    ledger = _ledger(tmp_path)

    summary = update_once(ledger, source=Source(resolved=False, side="UNKNOWN"))

    assert summary["unresolved"] == 1
    assert ledger.redeemable_lots() == []
