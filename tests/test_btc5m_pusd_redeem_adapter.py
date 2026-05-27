from __future__ import annotations

from pathlib import Path

import pytest

from scripts.run_btc5m_redeemer import run_once
from src.runtime import btc5m_pusd_redeem_adapter as redeem_mod
from src.runtime.btc5m_live_ledger import LiveLedger
from src.runtime.btc5m_pusd_redeem_adapter import (
    PusdCtfRedeemAdapter,
    REDEEM_POSITIONS_SELECTOR,
    condition_id_to_bytes32,
    verify_adapter_abi,
)
from src.runtime.polymarket_funder_setup import PolymarketFunderConfig


CONDITION = "0x" + "11" * 32


class FakeRedeemAdapter:
    def __init__(self, *, status="confirmed", delta=1.0, error=None):
        self.calls = []
        self.status = status
        self.delta = delta
        self.error = error

    def redeem_condition(self, *, condition_id, token_ids, index_sets=None):
        self.calls.append({"condition_id": condition_id, "token_ids": token_ids, "index_sets": index_sets})
        if self.error:
            raise RuntimeError(self.error)
        return {
            "status": self.status,
            "tx_hash": "0xabc",
            "receipt": {"status": 1, "transactionHash": "0xabc"},
            "redeemed_pusd_delta": self.delta,
            "burned_token_balances": {token_ids[0]: 1.0},
        }


def _ledger_with_redeemable(tmp_path: Path) -> LiveLedger:
    ledger = LiveLedger(tmp_path / "ledger.db")
    ledger.record_fill_from_event(
        {
            "event_type": "order_filled",
            "order_id": "ord1",
            "market_id": "m1",
            "condition_id": CONDITION,
            "token_id": "123",
            "selected_side": "YES",
            "filled_size": 1.0,
            "avg_fill_price": 0.4,
            "raw_response": {"trade_id": "trade-1"},
        }
    )
    ledger.upsert_resolution(condition_id=CONDITION, market_id="m1", resolved=True, winning_side="YES")
    return ledger


def test_redeem_selector_constant_is_verified():
    assert REDEEM_POSITIONS_SELECTOR == "0x01b7037c"
    verify_adapter_abi()


def test_condition_id_to_bytes32_requires_32_bytes():
    assert condition_id_to_bytes32(CONDITION) == bytes.fromhex("11" * 32)
    with pytest.raises(RuntimeError, match="condition_id_invalid"):
        condition_id_to_bytes32("0x1234")


def test_missing_adapter_abi_fails_closed(monkeypatch):
    monkeypatch.setattr(redeem_mod, "REDEEM_POSITIONS_SELECTOR", "0xdeadbeef")
    with pytest.raises(RuntimeError, match="adapter_abi_unverified"):
        verify_adapter_abi()


def test_redeemer_success_marks_redeemed_and_records_delta(tmp_path: Path):
    ledger = _ledger_with_redeemable(tmp_path)
    adapter = FakeRedeemAdapter(delta=1.23)

    result = run_once(ledger, config=PolymarketFunderConfig(), dry_run=False, allow_tx=True, adapter=adapter)

    assert result["events"][0]["status"] == "confirmed"
    assert adapter.calls[0]["index_sets"] == [1, 2]
    with ledger.connect() as conn:
        lot_status = conn.execute("SELECT status FROM outcome_lots").fetchone()[0]
        redeemed = conn.execute("SELECT redeemed_pusd_amount FROM redeemed_lots").fetchone()[0]
        attempt_status = conn.execute("SELECT status FROM redemption_attempts").fetchone()[0]
    assert lot_status == "redeemed"
    assert redeemed == pytest.approx(1.23)
    assert attempt_status == "confirmed"


def test_redeemer_receipt_failure_marks_failed_retryable(tmp_path: Path):
    ledger = _ledger_with_redeemable(tmp_path)
    adapter = FakeRedeemAdapter(status="failed_retryable", delta=None)

    result = run_once(ledger, config=PolymarketFunderConfig(), dry_run=False, allow_tx=True, adapter=adapter)

    assert result["events"][0]["status"] == "failed_retryable"
    with ledger.connect() as conn:
        status = conn.execute("SELECT status FROM redemption_attempts").fetchone()[0]
    assert status == "failed_retryable"


def test_duplicate_run_does_not_double_redeem(tmp_path: Path):
    ledger = _ledger_with_redeemable(tmp_path)
    adapter = FakeRedeemAdapter()

    run_once(ledger, config=PolymarketFunderConfig(), dry_run=False, allow_tx=True, adapter=adapter)
    second = run_once(ledger, config=PolymarketFunderConfig(), dry_run=False, allow_tx=True, adapter=adapter)

    assert len(adapter.calls) == 1
    assert second["redeemable_conditions"] == 0


def test_zero_token_balance_skipped_as_terminal(tmp_path: Path):
    ledger = _ledger_with_redeemable(tmp_path)
    adapter = FakeRedeemAdapter(error="zero_token_balance")

    result = run_once(ledger, config=PolymarketFunderConfig(), dry_run=False, allow_tx=True, adapter=adapter)

    assert result["events"][0]["error_code"] == "zero_token_balance"
    with ledger.connect() as conn:
        status = conn.execute("SELECT status FROM redemption_attempts").fetchone()[0]
    assert status == "failed_terminal"


def test_unresolved_condition_is_skipped(tmp_path: Path):
    ledger = LiveLedger(tmp_path / "ledger.db")
    ledger.record_fill_from_event(
        {
            "event_type": "order_filled",
            "order_id": "ord1",
            "market_id": "m1",
            "condition_id": CONDITION,
            "token_id": "123",
            "selected_side": "YES",
            "filled_size": 1.0,
            "avg_fill_price": 0.4,
            "raw_response": {"trade_id": "trade-1"},
        }
    )

    result = run_once(ledger, config=PolymarketFunderConfig(), dry_run=True, allow_tx=False)

    assert result["redeemable_conditions"] == 0


def test_adapter_builds_redeem_call_with_binary_index_sets():
    class Functions:
        def redeemPositions(self, collateral, parent, condition, index_sets):
            return {"collateral": collateral, "parent": parent, "condition": condition, "index_sets": index_sets}

    class Contract:
        functions = Functions()

    class Eth:
        chain_id = 137

        def contract(self, address, abi):
            return Contract()

    class Web3:
        eth = Eth()

        def to_checksum_address(self, address):
            return address

    adapter = PusdCtfRedeemAdapter(funder_config=PolymarketFunderConfig(owner_private_key="x"), web3=Web3())
    fn = adapter.build_redeem_function(condition_id=CONDITION)

    assert fn["parent"] == bytes.fromhex("00" * 32)
    assert fn["condition"] == bytes.fromhex("11" * 32)
    assert fn["index_sets"] == [1, 2]
