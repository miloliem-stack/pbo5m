"""tests/test_btc5m_lifecycle_supervisor.py

Unit tests for:
  - SupervisorConfig.from_env
  - LiveLedger supervisor query helpers (count_live_open_orders, etc.)
  - evaluate_trading_safety_gates (all gate conditions)
  - reconciliation_worker_tick
  - resolution_worker_tick
  - redemption_worker_tick
  - supervisor_status_summary
  - run_supervisor (integration smoke test with stubbed workers)
"""
from __future__ import annotations

import time
from pathlib import Path
from typing import Any, Optional
from unittest.mock import MagicMock, patch

import pytest

from src.runtime.btc5m_live_ledger import LiveLedger
from src.runtime.btc5m_lifecycle_supervisor import (
    SafetyGateResult,
    SupervisorConfig,
    evaluate_trading_safety_gates,
    reconciliation_worker_tick,
    redemption_worker_tick,
    resolution_worker_tick,
    run_supervisor,
    supervisor_status_summary,
    trading_worker_tick,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ledger(tmp_path: Path) -> LiveLedger:
    return LiveLedger(tmp_path / "ledger.db")


def _fill_event(
    *,
    order_id: str = "ord1",
    condition_id: str = "cond-a",
    market_id: str = "mkt-a",
    token_id: str = "tok-yes",
    side: str = "YES",
    qty: float = 1.0,
    price: float = 0.4,
    trade_id: str = "t1",
) -> dict[str, Any]:
    return {
        "event_type": "order_filled",
        "order_id": order_id,
        "market_id": market_id,
        "condition_id": condition_id,
        "token_id": token_id,
        "selected_side": side,
        "filled_size": qty,
        "avg_fill_price": price,
        "raw_response": {"trade_id": trade_id},
        "idempotency_key": f"key-{order_id}",
    }


def _submit_order(ledger: LiveLedger, *, order_id: str, condition_id: str = "cond-a") -> None:
    """Helper: insert a submitted (non-terminal) order into the ledger."""
    with ledger.connect() as conn:
        conn.execute(
            """
            INSERT INTO live_orders (
                strategy_id, market_id, condition_id, token_id, side, order_id,
                client_order_id, idempotency_key, order_type, limit_price,
                intended_notional_usd, submitted_ts, terminal_status, raw_response_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), ?, '{}')
            """,
            (
                "btc5m",
                "mkt-a",
                condition_id,
                "tok-yes",
                "YES",
                order_id,
                f"client-{order_id}",
                f"idem-{order_id}",
                "FAK",
                0.40,
                1.0,
                "live_order_submitted",
            ),
        )


# ---------------------------------------------------------------------------
# SupervisorConfig.from_env
# ---------------------------------------------------------------------------

class TestSupervisorConfigFromEnv:
    def test_defaults(self):
        cfg = SupervisorConfig.from_env({})
        assert cfg.max_unresolved_markets == 3
        assert cfg.max_live_open_orders == 1
        assert cfg.block_on_unknown_order is True
        assert cfg.block_on_reconciliation_stale is True
        assert cfg.reconciliation_stale_sec == 120.0
        assert cfg.block_on_redeemer_health_failure is True
        assert cfg.max_redemption_failures == 3
        assert cfg.max_total_unredeemed_notional_usd is None

    def test_all_overridden_from_env(self):
        env = {
            "BTC5M_MAX_UNRESOLVED_MARKETS": "5",
            "BTC5M_MAX_TOTAL_UNREDEEMED_NOTIONAL_USD": "50.0",
            "BTC5M_MAX_LIVE_OPEN_ORDERS": "2",
            "BTC5M_BLOCK_ON_UNKNOWN_ORDER": "false",
            "BTC5M_BLOCK_ON_RECONCILIATION_STALE": "0",
            "BTC5M_RECONCILIATION_STALE_SEC": "300",
            "BTC5M_BLOCK_ON_REDEEMER_HEALTH_FAILURE": "no",
            "BTC5M_MAX_REDEMPTION_FAILURES": "7",
        }
        cfg = SupervisorConfig.from_env(env)
        assert cfg.max_unresolved_markets == 5
        assert cfg.max_total_unredeemed_notional_usd == 50.0
        assert cfg.max_live_open_orders == 2
        assert cfg.block_on_unknown_order is False
        assert cfg.block_on_reconciliation_stale is False
        assert cfg.reconciliation_stale_sec == 300.0
        assert cfg.block_on_redeemer_health_failure is False
        assert cfg.max_redemption_failures == 7

    def test_invalid_int_falls_back_to_default(self):
        cfg = SupervisorConfig.from_env({"BTC5M_MAX_UNRESOLVED_MARKETS": "not_a_number"})
        assert cfg.max_unresolved_markets == 3


# ---------------------------------------------------------------------------
# LiveLedger supervisor query helpers
# ---------------------------------------------------------------------------

class TestLiveLedgerSupervisorQueries:
    def test_empty_ledger_returns_zeros(self, tmp_path: Path):
        ledger = _ledger(tmp_path)
        assert ledger.count_live_open_orders() == 0
        assert ledger.count_unknown_orders() == 0
        assert ledger.count_unresolved_markets() == 0
        assert ledger.total_unredeemed_notional_estimate() == 0.0
        assert ledger.open_orders_for_reconciliation() == []

    def test_count_live_open_orders_counts_non_terminal(self, tmp_path: Path):
        ledger = _ledger(tmp_path)
        _submit_order(ledger, order_id="o1")
        _submit_order(ledger, order_id="o2")
        assert ledger.count_live_open_orders() == 2

    def test_count_live_open_orders_excludes_terminal(self, tmp_path: Path):
        ledger = _ledger(tmp_path)
        _submit_order(ledger, order_id="o1")
        with ledger.connect() as conn:
            conn.execute(
                "UPDATE live_orders SET terminal_status='order_filled' WHERE order_id='o1'"
            )
        assert ledger.count_live_open_orders() == 0

    def test_count_live_open_orders_excludes_no_order_id(self, tmp_path: Path):
        """Intent-only rows (no order_id) are not counted as open CLOB orders."""
        ledger = _ledger(tmp_path)
        with ledger.connect() as conn:
            conn.execute(
                """
                INSERT INTO live_orders (
                    strategy_id, market_id, condition_id, token_id, side,
                    client_order_id, idempotency_key, order_type, limit_price,
                    intended_notional_usd, submitted_ts, terminal_status, raw_response_json
                )
                VALUES ('btc5m', 'm1', 'c1', 't1', 'YES',
                        'clt1', 'idem1', 'FAK', 0.4, 1.0, datetime('now'),
                        'intent_created', '{}')
                """
            )
        assert ledger.count_live_open_orders() == 0

    def test_count_unknown_orders(self, tmp_path: Path):
        ledger = _ledger(tmp_path)
        _submit_order(ledger, order_id="u1")
        with ledger.connect() as conn:
            conn.execute(
                "UPDATE live_orders SET terminal_status='order_unknown_after_submit' WHERE order_id='u1'"
            )
        assert ledger.count_unknown_orders() == 1
        assert ledger.count_live_open_orders() == 0  # unknown is terminal for open-order count

    def test_count_unresolved_markets(self, tmp_path: Path):
        ledger = _ledger(tmp_path)
        ledger.record_fill_from_event(_fill_event(order_id="o1", condition_id="c1", trade_id="t1"))
        ledger.record_fill_from_event(_fill_event(order_id="o2", condition_id="c2", trade_id="t2"))
        assert ledger.count_unresolved_markets() == 2

    def test_count_unresolved_markets_excludes_redeemed(self, tmp_path: Path):
        ledger = _ledger(tmp_path)
        ledger.record_fill_from_event(_fill_event(order_id="o1", condition_id="c1", trade_id="t1"))
        ledger.upsert_resolution(condition_id="c1", market_id="mkt-a", resolved=True, winning_side="YES")
        ledger.terminalize_resolved_lots()
        ledger.mark_lots_redeemed(condition_id="c1", tx_hash="0xabc", redeemed_pusd_amount=1.0)
        assert ledger.count_unresolved_markets() == 0

    def test_total_unredeemed_notional_estimate(self, tmp_path: Path):
        ledger = _ledger(tmp_path)
        # 2 shares at 0.4 → notional = 0.8
        ledger.record_fill_from_event(
            _fill_event(order_id="o1", condition_id="c1", qty=2.0, price=0.4, trade_id="t1")
        )
        est = ledger.total_unredeemed_notional_estimate()
        assert est == pytest.approx(0.8, rel=1e-4)

    def test_open_orders_for_reconciliation_returns_submittable_orders(self, tmp_path: Path):
        ledger = _ledger(tmp_path)
        _submit_order(ledger, order_id="o1")
        rows = ledger.open_orders_for_reconciliation()
        assert len(rows) == 1
        assert rows[0]["order_id"] == "o1"

    def test_open_orders_for_reconciliation_excludes_terminal(self, tmp_path: Path):
        ledger = _ledger(tmp_path)
        _submit_order(ledger, order_id="o1")
        with ledger.connect() as conn:
            conn.execute("UPDATE live_orders SET terminal_status='order_filled' WHERE order_id='o1'")
        assert ledger.open_orders_for_reconciliation() == []

    def test_supervisor_summary_returns_dict(self, tmp_path: Path):
        ledger = _ledger(tmp_path)
        ledger.record_fill_from_event(_fill_event())
        summary = ledger.supervisor_summary()
        assert summary["live_open_orders"] == 0
        assert summary["unresolved_markets"] == 1
        assert summary["lots_total"] == 1


# ---------------------------------------------------------------------------
# evaluate_trading_safety_gates
# ---------------------------------------------------------------------------

class TestEvaluateTradingSafetyGates:
    def test_allows_when_all_clear(self, tmp_path: Path):
        ledger = _ledger(tmp_path)
        cfg = SupervisorConfig(
            max_live_open_orders=1,
            block_on_unknown_order=True,
            max_unresolved_markets=3,
            block_on_reconciliation_stale=False,
        )
        result = evaluate_trading_safety_gates(ledger, cfg)
        assert result.trading_allowed is True
        assert result.block_reasons == []

    def test_blocks_when_open_orders_at_limit(self, tmp_path: Path):
        ledger = _ledger(tmp_path)
        _submit_order(ledger, order_id="o1")
        cfg = SupervisorConfig(max_live_open_orders=1, block_on_unknown_order=False)
        result = evaluate_trading_safety_gates(ledger, cfg)
        assert result.trading_allowed is False
        assert any("max_live_open_orders_exceeded" in r for r in result.block_reasons)

    def test_allows_when_open_orders_below_limit(self, tmp_path: Path):
        ledger = _ledger(tmp_path)
        _submit_order(ledger, order_id="o1")
        cfg = SupervisorConfig(max_live_open_orders=2, block_on_unknown_order=False)
        result = evaluate_trading_safety_gates(ledger, cfg)
        assert result.trading_allowed is True

    def test_blocks_on_unknown_order_when_flag_set(self, tmp_path: Path):
        ledger = _ledger(tmp_path)
        _submit_order(ledger, order_id="u1")
        with ledger.connect() as conn:
            conn.execute(
                "UPDATE live_orders SET terminal_status='order_unknown_after_submit' WHERE order_id='u1'"
            )
        cfg = SupervisorConfig(max_live_open_orders=5, block_on_unknown_order=True)
        result = evaluate_trading_safety_gates(ledger, cfg)
        assert result.trading_allowed is False
        assert any("unknown_order_blocked" in r for r in result.block_reasons)

    def test_allows_unknown_order_when_flag_off(self, tmp_path: Path):
        ledger = _ledger(tmp_path)
        _submit_order(ledger, order_id="u1")
        with ledger.connect() as conn:
            conn.execute(
                "UPDATE live_orders SET terminal_status='order_unknown_after_submit' WHERE order_id='u1'"
            )
        cfg = SupervisorConfig(max_live_open_orders=5, block_on_unknown_order=False)
        result = evaluate_trading_safety_gates(ledger, cfg)
        # unknown_order gate is off; only check unresolved/open-order gates
        assert not any("unknown_order_blocked" in r for r in result.block_reasons)

    def test_blocks_when_unresolved_markets_at_limit(self, tmp_path: Path):
        ledger = _ledger(tmp_path)
        for i in range(3):
            ledger.record_fill_from_event(
                _fill_event(order_id=f"o{i}", condition_id=f"c{i}", trade_id=f"t{i}")
            )
        cfg = SupervisorConfig(max_unresolved_markets=3, block_on_unknown_order=False)
        result = evaluate_trading_safety_gates(ledger, cfg)
        assert result.trading_allowed is False
        assert any("max_unresolved_markets_exceeded" in r for r in result.block_reasons)

    def test_allows_with_older_unresolved_markets_below_limit(self, tmp_path: Path):
        ledger = _ledger(tmp_path)
        for i in range(2):
            ledger.record_fill_from_event(
                _fill_event(order_id=f"o{i}", condition_id=f"c{i}", trade_id=f"t{i}")
            )
        cfg = SupervisorConfig(max_unresolved_markets=3, block_on_unknown_order=False)
        result = evaluate_trading_safety_gates(ledger, cfg)
        assert result.trading_allowed is True

    def test_blocks_on_unredeemed_notional_ceiling(self, tmp_path: Path):
        ledger = _ledger(tmp_path)
        # 10 shares at 0.4 = 4.0 notional
        ledger.record_fill_from_event(
            _fill_event(order_id="o1", condition_id="c1", qty=10.0, price=0.4, trade_id="t1")
        )
        cfg = SupervisorConfig(
            max_total_unredeemed_notional_usd=3.0,
            block_on_unknown_order=False,
            max_live_open_orders=10,
        )
        result = evaluate_trading_safety_gates(ledger, cfg)
        assert result.trading_allowed is False
        assert any("max_unredeemed_notional_exceeded" in r for r in result.block_reasons)

    def test_blocks_on_reconciliation_stale(self, tmp_path: Path):
        ledger = _ledger(tmp_path)
        now_mono = time.monotonic()
        # last_reconciliation_ts was 200 seconds ago; stale threshold is 120 s
        last_rec = now_mono - 200.0
        cfg = SupervisorConfig(
            block_on_reconciliation_stale=True,
            reconciliation_stale_sec=120.0,
            block_on_unknown_order=False,
        )
        result = evaluate_trading_safety_gates(
            ledger, cfg, last_reconciliation_ts=last_rec, now_mono=now_mono
        )
        assert result.trading_allowed is False
        assert any("reconciliation_stale" in r for r in result.block_reasons)

    def test_no_reconciliation_stale_if_ts_is_none(self, tmp_path: Path):
        """If reconciliation has never run, the stale gate should not trigger."""
        ledger = _ledger(tmp_path)
        cfg = SupervisorConfig(block_on_reconciliation_stale=True, reconciliation_stale_sec=5.0)
        result = evaluate_trading_safety_gates(
            ledger, cfg, last_reconciliation_ts=None, now_mono=time.monotonic() + 1000
        )
        assert not any("reconciliation_stale" in r for r in result.block_reasons)

    def test_blocks_on_redeemer_health_failure(self, tmp_path: Path):
        ledger = _ledger(tmp_path)
        # Create a winning lot then record enough redemption failures
        ledger.record_fill_from_event(_fill_event())
        ledger.upsert_resolution(
            condition_id="cond-a", market_id="mkt-a", resolved=True, winning_side="YES"
        )
        ledger.terminalize_resolved_lots()
        for i in range(3):
            attempt_id = ledger.record_redemption_attempt(
                condition_id="cond-a",
                market_id="mkt-a",
                token_ids=["tok-yes"],
                index_sets=[1],
                status="failed_retryable",
            )
            ledger.update_redemption_attempt(attempt_id, status="failed_retryable")

        cfg = SupervisorConfig(
            block_on_redeemer_health_failure=True,
            max_redemption_failures=3,
            block_on_unknown_order=False,
        )
        result = evaluate_trading_safety_gates(ledger, cfg)
        assert result.trading_allowed is False
        assert any("redeemer_health_failure" in r for r in result.block_reasons)

    def test_redeemer_health_gate_off_allows_trade(self, tmp_path: Path):
        ledger = _ledger(tmp_path)
        ledger.record_fill_from_event(_fill_event())
        ledger.upsert_resolution(
            condition_id="cond-a", market_id="mkt-a", resolved=True, winning_side="YES"
        )
        ledger.terminalize_resolved_lots()
        for _ in range(5):
            attempt_id = ledger.record_redemption_attempt(
                condition_id="cond-a", market_id="mkt-a",
                token_ids=["tok-yes"], index_sets=[1], status="failed_retryable",
            )
            ledger.update_redemption_attempt(attempt_id, status="failed_retryable")

        cfg = SupervisorConfig(
            block_on_redeemer_health_failure=False,
            block_on_unknown_order=False,
            max_live_open_orders=5,
        )
        result = evaluate_trading_safety_gates(ledger, cfg)
        assert not any("redeemer_health_failure" in r for r in result.block_reasons)


# ---------------------------------------------------------------------------
# reconciliation_worker_tick
# ---------------------------------------------------------------------------

class TestReconciliationWorkerTick:
    def test_returns_no_orders_when_empty(self, tmp_path: Path):
        ledger = _ledger(tmp_path)
        result = reconciliation_worker_tick(
            ledger=ledger, get_order_status_fn=lambda _: {"status": "filled"}
        )
        assert result["status"] == "no_orders_to_reconcile"
        assert result["polled"] == 0

    def test_polls_open_order_and_updates_to_filled(self, tmp_path: Path):
        ledger = _ledger(tmp_path)
        _submit_order(ledger, order_id="o1")

        def _mock_status(order_id: str) -> dict:
            return {
                "status": "filled",
                "filled_size": 1.0,
                "avg_fill_price": 0.4,
            }

        result = reconciliation_worker_tick(
            ledger=ledger, get_order_status_fn=_mock_status
        )
        assert result["polled"] == 1
        assert result["filled"] == 1
        assert result["errors"] == []

        # The ledger should reflect the filled status
        with ledger.connect() as conn:
            status = conn.execute(
                "SELECT terminal_status FROM live_orders WHERE order_id='o1'"
            ).fetchone()[0]
        assert status == "order_filled"

    def test_filled_order_creates_outcome_lot(self, tmp_path: Path):
        ledger = _ledger(tmp_path)
        _submit_order(ledger, order_id="o1")
        # Also insert market/token metadata so record_fill_from_event works
        with ledger.connect() as conn:
            conn.execute(
                "UPDATE live_orders SET market_id='mkt-a', condition_id='cond-a', token_id='tok-yes', side='YES' WHERE order_id='o1'"
            )

        def _mock_status(order_id: str) -> dict:
            return {"status": "filled", "filled_size": 2.0, "avg_fill_price": 0.4}

        reconciliation_worker_tick(ledger=ledger, get_order_status_fn=_mock_status)

        lots = ledger.open_outcome_lots()
        assert len(lots) == 1
        assert lots[0]["acquired_qty"] == pytest.approx(2.0)

    def test_handles_polling_error_gracefully(self, tmp_path: Path):
        ledger = _ledger(tmp_path)
        _submit_order(ledger, order_id="o1")

        def _error_fn(order_id: str) -> dict:
            raise RuntimeError("network error")

        result = reconciliation_worker_tick(ledger=ledger, get_order_status_fn=_error_fn)
        assert result["polled"] == 0
        assert len(result["errors"]) == 1
        assert "network error" in result["errors"][0]["error"]

    def test_does_not_re_poll_terminal_orders(self, tmp_path: Path):
        ledger = _ledger(tmp_path)
        _submit_order(ledger, order_id="o1")
        with ledger.connect() as conn:
            conn.execute("UPDATE live_orders SET terminal_status='order_filled' WHERE order_id='o1'")

        calls: list = []
        result = reconciliation_worker_tick(
            ledger=ledger, get_order_status_fn=lambda oid: calls.append(oid) or {}
        )
        assert calls == []
        assert result["status"] == "no_orders_to_reconcile"


# ---------------------------------------------------------------------------
# resolution_worker_tick
# ---------------------------------------------------------------------------

class TestResolutionWorkerTick:
    def test_no_lots_returns_empty(self, tmp_path: Path):
        ledger = _ledger(tmp_path)
        mock_src = MagicMock()
        result = resolution_worker_tick(ledger=ledger, resolution_source=mock_src)
        assert result["status"] == "no_lots_to_resolve"
        assert result["checked"] == 0
        mock_src.resolve.assert_not_called()

    def test_resolved_market_updates_ledger(self, tmp_path: Path):
        ledger = _ledger(tmp_path)
        ledger.record_fill_from_event(_fill_event())

        resolution_result = MagicMock()
        resolution_result.resolved = True
        resolution_result.winning_side = "YES"
        resolution_result.source = "gamma_ctf"
        resolution_result.payout_vector = [1, 0]

        mock_src = MagicMock()
        mock_src.resolve.return_value = resolution_result

        result = resolution_worker_tick(ledger=ledger, resolution_source=mock_src)
        assert result["newly_resolved"] == 1
        assert result["checked"] == 1
        assert result["errors"] == []

        # Lots should now be resolved_win
        with ledger.connect() as conn:
            status = conn.execute("SELECT status FROM outcome_lots").fetchone()[0]
        assert status == "resolved_win"

    def test_unresolved_market_records_last_checked(self, tmp_path: Path):
        ledger = _ledger(tmp_path)
        ledger.record_fill_from_event(_fill_event())

        resolution_result = MagicMock()
        resolution_result.resolved = False

        mock_src = MagicMock()
        mock_src.resolve.return_value = resolution_result

        result = resolution_worker_tick(ledger=ledger, resolution_source=mock_src)
        assert result["newly_resolved"] == 0
        assert result["checked"] == 1

        # last_checked_ts should be updated in market_resolution_state
        with ledger.connect() as conn:
            row = conn.execute("SELECT * FROM market_resolution_state").fetchone()
        assert row is not None
        assert row["resolved"] == 0

    def test_resolution_error_is_caught(self, tmp_path: Path):
        ledger = _ledger(tmp_path)
        ledger.record_fill_from_event(_fill_event())

        mock_src = MagicMock()
        mock_src.resolve.side_effect = RuntimeError("gamma error")

        result = resolution_worker_tick(ledger=ledger, resolution_source=mock_src)
        assert result["newly_resolved"] == 0
        assert len(result["errors"]) == 1
        assert "gamma error" in result["errors"][0]["error"]

    def test_only_checks_each_condition_once(self, tmp_path: Path):
        ledger = _ledger(tmp_path)
        # Two lots from the same condition
        ledger.record_fill_from_event(_fill_event(order_id="o1", side="YES", trade_id="t1"))
        ledger.record_fill_from_event(_fill_event(order_id="o2", side="YES", trade_id="t2"))

        resolution_result = MagicMock()
        resolution_result.resolved = False
        mock_src = MagicMock()
        mock_src.resolve.return_value = resolution_result

        resolution_worker_tick(ledger=ledger, resolution_source=mock_src)
        assert mock_src.resolve.call_count == 1


# ---------------------------------------------------------------------------
# redemption_worker_tick
# ---------------------------------------------------------------------------

class TestRedemptionWorkerTick:
    def _setup_winning_lot(self, ledger: LiveLedger) -> None:
        ledger.record_fill_from_event(_fill_event())
        ledger.upsert_resolution(
            condition_id="cond-a", market_id="mkt-a", resolved=True, winning_side="YES"
        )
        ledger.terminalize_resolved_lots()

    def test_no_lots_returns_empty(self, tmp_path: Path):
        ledger = _ledger(tmp_path)
        mock_adapter = MagicMock()
        cfg = SupervisorConfig()
        result = redemption_worker_tick(ledger=ledger, redeem_adapter=mock_adapter, config=cfg)
        assert result["status"] == "no_lots_to_redeem"
        assert result["attempted"] == 0
        mock_adapter.redeem_condition.assert_not_called()

    def test_successful_redemption_marks_lots_redeemed(self, tmp_path: Path):
        ledger = _ledger(tmp_path)
        self._setup_winning_lot(ledger)

        mock_adapter = MagicMock()
        mock_adapter.redeem_condition.return_value = {
            "status": "confirmed",
            "tx_hash": "0xdeadbeef",
            "redeemed_pusd_delta": 1.0,
            "receipt": {"block": 1},
        }

        result = redemption_worker_tick(
            ledger=ledger, redeem_adapter=mock_adapter, config=SupervisorConfig()
        )
        assert result["attempted"] == 1
        assert result["redeemed"] == 1
        assert result["errors"] == []

        # Lots should now be redeemed
        with ledger.connect() as conn:
            status = conn.execute("SELECT status FROM outcome_lots").fetchone()[0]
        assert status == "redeemed"

    def test_failed_redemption_records_attempt(self, tmp_path: Path):
        ledger = _ledger(tmp_path)
        self._setup_winning_lot(ledger)

        mock_adapter = MagicMock()
        mock_adapter.redeem_condition.return_value = {
            "status": "failed_retryable",
            "tx_hash": None,
        }

        result = redemption_worker_tick(
            ledger=ledger, redeem_adapter=mock_adapter, config=SupervisorConfig()
        )
        assert result["attempted"] == 1
        assert result["redeemed"] == 0
        assert len(result["errors"]) == 1

        with ledger.connect() as conn:
            attempt = conn.execute("SELECT status FROM redemption_attempts").fetchone()[0]
        assert attempt == "failed_retryable"

    def test_blocks_condition_after_max_failures(self, tmp_path: Path):
        ledger = _ledger(tmp_path)
        self._setup_winning_lot(ledger)

        # Pre-load max_redemption_failures failures
        for _ in range(3):
            attempt_id = ledger.record_redemption_attempt(
                condition_id="cond-a",
                market_id="mkt-a",
                token_ids=["tok-yes"],
                index_sets=[1],
                status="failed_retryable",
            )
            ledger.update_redemption_attempt(attempt_id, status="failed_retryable")

        mock_adapter = MagicMock()
        cfg = SupervisorConfig(max_redemption_failures=3)
        result = redemption_worker_tick(ledger=ledger, redeem_adapter=mock_adapter, config=cfg)
        # Should be blocked – no new attempt
        assert result["attempted"] == 0
        mock_adapter.redeem_condition.assert_not_called()
        assert any("max_redemption_failures_exceeded" in str(e) for e in result["errors"])

    def test_skips_already_redeemed_condition(self, tmp_path: Path):
        ledger = _ledger(tmp_path)
        self._setup_winning_lot(ledger)

        # Record a successful attempt
        attempt_id = ledger.record_redemption_attempt(
            condition_id="cond-a",
            market_id="mkt-a",
            token_ids=["tok-yes"],
            index_sets=[1],
            status="confirmed",
        )
        ledger.update_redemption_attempt(attempt_id, status="confirmed", confirmed=True)

        mock_adapter = MagicMock()
        result = redemption_worker_tick(
            ledger=ledger, redeem_adapter=mock_adapter, config=SupervisorConfig()
        )
        assert result["attempted"] == 0
        mock_adapter.redeem_condition.assert_not_called()

    def test_exception_in_redeem_recorded_as_failure(self, tmp_path: Path):
        ledger = _ledger(tmp_path)
        self._setup_winning_lot(ledger)

        mock_adapter = MagicMock()
        mock_adapter.redeem_condition.side_effect = RuntimeError("tx reverted")

        result = redemption_worker_tick(
            ledger=ledger, redeem_adapter=mock_adapter, config=SupervisorConfig()
        )
        assert result["attempted"] == 1
        assert result["redeemed"] == 0
        assert any("tx reverted" in str(e) for e in result["errors"])

        with ledger.connect() as conn:
            attempt_status = conn.execute("SELECT status FROM redemption_attempts ORDER BY id DESC LIMIT 1").fetchone()[0]
        assert attempt_status == "failed_retryable"


# ---------------------------------------------------------------------------
# supervisor_status_summary
# ---------------------------------------------------------------------------

class TestSupervisorStatusSummary:
    def test_empty_ledger(self, tmp_path: Path):
        ledger = _ledger(tmp_path)
        s = supervisor_status_summary(ledger)
        assert s["live_open_orders"] == 0
        assert s["unknown_orders"] == 0
        assert s["unresolved_markets"] == 0
        assert s["unredeemed_notional_estimate"] == 0.0
        assert s["orders_total"] == 0
        assert s["lots_total"] == 0

    def test_summary_reflects_active_state(self, tmp_path: Path):
        ledger = _ledger(tmp_path)
        _submit_order(ledger, order_id="o1")
        ledger.record_fill_from_event(_fill_event(order_id="o2", condition_id="c2", trade_id="t2"))
        s = supervisor_status_summary(ledger)
        assert s["live_open_orders"] == 1
        assert s["unresolved_markets"] == 1
        assert s["orders_total"] == 1
        assert s["lots_total"] == 1


# ---------------------------------------------------------------------------
# run_supervisor (integration smoke test)
# ---------------------------------------------------------------------------

class TestRunSupervisor:
    def test_exits_at_deadline(self, tmp_path: Path):
        ledger = _ledger(tmp_path)
        cfg = SupervisorConfig(loop_sleep_sec=0.0)
        mock_builder = MagicMock()
        mock_builder.build.return_value = {"ok": False, "missing_input_reason": "no_market"}

        slept: list[float] = []

        result = run_supervisor(
            ledger=ledger,
            supervisor_config=cfg,
            brownian_config=None,
            execution_callback=None,
            live_input_builder=mock_builder,
            max_runtime_sec=0.05,
            sleep_fn=lambda s: slept.append(s),
        )
        assert result["status"] == "supervisor_deadline_reached"
        assert result["iterations"] >= 1

    def test_reconciliation_worker_is_called_when_order_status_fn_provided(self, tmp_path: Path):
        """If get_order_status_fn is provided and there are open orders, reconciliation runs."""
        ledger = _ledger(tmp_path)
        _submit_order(ledger, order_id="o1")

        polled_ids: list[str] = []

        def _status_fn(order_id: str) -> dict:
            polled_ids.append(order_id)
            return {"status": "filled", "filled_size": 1.0, "avg_fill_price": 0.4}

        cfg = SupervisorConfig(
            reconciliation_tick_interval_sec=0.0,
            trading_tick_interval_sec=9999.0,
            resolution_tick_interval_sec=9999.0,
            redemption_tick_interval_sec=9999.0,
            loop_sleep_sec=0.0,
        )

        run_supervisor(
            ledger=ledger,
            supervisor_config=cfg,
            brownian_config=None,
            execution_callback=None,
            live_input_builder=None,
            get_order_status_fn=_status_fn,
            max_runtime_sec=0.05,
            sleep_fn=lambda _: None,
        )
        assert "o1" in polled_ids

    def test_no_workers_when_all_none(self, tmp_path: Path):
        """Supervisor runs cleanly when all optional workers are disabled."""
        ledger = _ledger(tmp_path)
        cfg = SupervisorConfig(loop_sleep_sec=0.0)
        result = run_supervisor(
            ledger=ledger,
            supervisor_config=cfg,
            brownian_config=None,
            execution_callback=None,
            live_input_builder=None,
            max_runtime_sec=0.02,
            sleep_fn=lambda _: None,
        )
        assert result["status"] == "supervisor_deadline_reached"
        assert result["iterations"] >= 1

    def test_redemption_worker_called_when_adapter_provided(self, tmp_path: Path):
        ledger = _ledger(tmp_path)
        # Set up a winning lot
        ledger.record_fill_from_event(_fill_event())
        ledger.upsert_resolution(
            condition_id="cond-a", market_id="mkt-a", resolved=True, winning_side="YES"
        )
        ledger.terminalize_resolved_lots()

        mock_adapter = MagicMock()
        mock_adapter.redeem_condition.return_value = {
            "status": "confirmed",
            "tx_hash": "0xabc",
            "redeemed_pusd_delta": 0.4,
        }

        cfg = SupervisorConfig(
            redemption_tick_interval_sec=0.0,
            reconciliation_tick_interval_sec=9999.0,
            resolution_tick_interval_sec=9999.0,
            trading_tick_interval_sec=9999.0,
            loop_sleep_sec=0.0,
        )
        run_supervisor(
            ledger=ledger,
            supervisor_config=cfg,
            brownian_config=None,
            execution_callback=None,
            live_input_builder=None,
            redeem_adapter=mock_adapter,
            max_runtime_sec=0.05,
            sleep_fn=lambda _: None,
        )
        mock_adapter.redeem_condition.assert_called_once()

        with ledger.connect() as conn:
            status = conn.execute("SELECT status FROM outcome_lots").fetchone()[0]
        assert status == "redeemed"
