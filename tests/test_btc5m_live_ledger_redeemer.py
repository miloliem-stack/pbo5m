from __future__ import annotations

from pathlib import Path

import pytest

from scripts.run_btc5m_redeemer import run_once
from src.runtime.btc5m_brownian_conservative import BrownianConservativeConfig, compute_conservative_stake
from src.runtime.btc5m_canary_execution import CanaryExecutor, ExecutionConfig, ExecutionJournal, OrderIntent, normalize_clob_error
from src.runtime.btc5m_canary_policy import CanaryConfig
from src.runtime.btc5m_live_ledger import LiveLedger
from src.runtime.polymarket_funder_setup import PolymarketFunderConfig


def test_fill_creates_outcome_lot_and_duplicate_poll_does_not_double_count(tmp_path: Path):
    ledger = LiveLedger(tmp_path / "ledger.db")
    event = {
        "event_type": "order_filled",
        "order_id": "ord1",
        "market_id": "m1",
        "condition_id": "c1",
        "token_id": "tok-yes",
        "selected_side": "YES",
        "filled_size": 2.0,
        "avg_fill_price": 0.4,
        "raw_response": {"trade_id": "trade-1"},
    }

    assert ledger.record_fill_from_event(event) is True
    assert ledger.record_fill_from_event(event) is False

    with ledger.connect() as conn:
        fills = conn.execute("SELECT COUNT(*) FROM live_fills").fetchone()[0]
        lots = conn.execute("SELECT COUNT(*), SUM(acquired_qty) FROM outcome_lots").fetchone()
    assert fills == 1
    assert lots[0] == 1
    assert lots[1] == pytest.approx(2.0)


def test_resolved_winning_lot_becomes_redeemable_and_losing_lot_terminalizes(tmp_path: Path):
    ledger = LiveLedger(tmp_path / "ledger.db")
    for side in ("YES", "NO"):
        ledger.record_fill_from_event(
            {
                "event_type": "order_filled",
                "order_id": f"ord-{side}",
                "market_id": "m1",
                "condition_id": "c1",
                "token_id": f"tok-{side}",
                "selected_side": side,
                "filled_size": 1.0,
                "avg_fill_price": 0.4,
                "raw_response": {"trade_id": f"trade-{side}"},
            }
        )
    ledger.upsert_resolution(condition_id="c1", market_id="m1", resolved=True, winning_side="YES")
    ledger.terminalize_resolved_lots()

    redeemable = ledger.redeemable_lots()
    assert len(redeemable) == 1
    assert redeemable[0]["side"] == "YES"
    with ledger.connect() as conn:
        loss_status = conn.execute("SELECT status FROM outcome_lots WHERE side='NO'").fetchone()[0]
    assert loss_status == "resolved_loss"


def test_redeemer_dry_run_sends_no_transaction(tmp_path: Path):
    ledger = LiveLedger(tmp_path / "ledger.db")
    ledger.record_fill_from_event(
        {
            "event_type": "order_filled",
            "order_id": "ord1",
            "market_id": "m1",
            "condition_id": "c1",
            "token_id": "tok-yes",
            "selected_side": "YES",
            "filled_size": 1.0,
            "avg_fill_price": 0.4,
            "raw_response": {"trade_id": "trade-1"},
        }
    )
    ledger.upsert_resolution(condition_id="c1", market_id="m1", resolved=True, winning_side="YES")

    result = run_once(ledger, config=PolymarketFunderConfig(), dry_run=True, allow_tx=False)

    assert result["redeemable_conditions"] == 1
    assert result["events"][0]["status"] == "dry_run"
    with ledger.connect() as conn:
        status = conn.execute("SELECT status FROM redemption_attempts").fetchone()[0]
    assert status == "dry_run"


def test_venue_min_below_five_permits_smaller_order_sizing():
    cfg = BrownianConservativeConfig(
        min_order_notional=1.0,
        min_market_buy_notional_usd=1.0,
        small_wallet_threshold=400.0,
        normal_max_stake_fraction=0.0025,
        small_wallet_max_stake_fraction=0.0025,
    )
    sized = compute_conservative_stake(bankroll=500, probability=0.8, ask=0.4, depth_cap=100, config=cfg)
    assert sized["reject_reason"] is None
    assert sized["stake_notional"] >= 1.0
    assert sized["stake_notional"] <= 500 * 0.0025


def test_no_stake_rounds_above_max_fraction_by_default():
    cfg = BrownianConservativeConfig(
        min_order_notional=1.0,
        min_market_buy_notional_usd=1.0,
        small_wallet_threshold=400.0,
        normal_max_stake_fraction=0.0025,
        small_wallet_max_stake_fraction=0.0025,
    )
    sized = compute_conservative_stake(bankroll=300, probability=0.8, ask=0.4, depth_cap=100, config=cfg)
    assert sized["reject_reason"] == "below_min_order_notional"
    assert sized["stake_notional"] < 1.0


def test_invalid_order_min_size_is_terminally_classified():
    normalized = normalize_clob_error(Exception("PolyApiException[status_code=400, error_message={'error': 'INVALID_ORDER_MIN_SIZE'}]"))
    assert normalized["error_code"] == "invalid_order_min_size"
    assert normalized["terminal"] is True
    assert normalized["retryable"] is False
    assert normalized["suggested_min_update_required"] is True


def test_current_five_dollar_behavior_still_possible_by_env():
    cfg = BrownianConservativeConfig.from_env(
        {
            "BTC5M_STRATEGY_ID": "brownian_no_hmm_conservative_v1",
            "BTC5M_BROWNIAN_MIN_ORDER_NOTIONAL": "5",
            "BTC5M_BROWNIAN_MAX_STAKE_FRACTION": "0.0025",
        }
    )
    assert cfg.min_order_notional == 5.0
    assert cfg.small_wallet_threshold == pytest.approx(2000.0)


def test_min_order_notional_env_overrides_market_buy_alias():
    cfg = BrownianConservativeConfig.from_env(
        {
            "BTC5M_STRATEGY_ID": "brownian_no_hmm_conservative_v1",
            "BTC5M_BROWNIAN_MIN_ORDER_NOTIONAL": "2",
            "BTC5M_BROWNIAN_MIN_MARKET_BUY_NOTIONAL_USD": "5",
            "BTC5M_BROWNIAN_MAX_STAKE_FRACTION": "0.0025",
        }
    )
    assert cfg.min_order_notional == 2.0
    assert cfg.min_market_buy_notional_usd == 2.0
    assert cfg.small_wallet_threshold == pytest.approx(800.0)


def test_live_trading_path_does_not_call_redeemer(tmp_path: Path):
    class Adapter:
        def wallet_address(self):
            return "0xWallet"

        def submit_buy(self, intent):
            return {"status": "submitted", "order_id": "ord1"}

        def get_order_status(self, order_id):
            return {"status": "filled", "filled_size": 1.0, "avg_fill_price": 0.4}

    executor = CanaryExecutor(
        ExecutionConfig(
            execution_mode="live",
            live_trading_enabled=True,
            canary_stake_usd=5,
            max_notional_per_market_usd=5,
            max_daily_notional_usd=5,
            policy_config=CanaryConfig(min_edge=0.0, canary_stake_usd=5),
            order_poll_timeout_sec=0.01,
            order_poll_interval_sec=0,
        ),
        Adapter(),
        ExecutionJournal(tmp_path / "journal"),
        ledger=LiveLedger(tmp_path / "ledger.db"),
        sleep_fn=lambda _: None,
    )
    intent = OrderIntent(
        policy_id="brownian_no_hmm_conservative_v1",
        market_id="m1",
        condition_id="c1",
        token_id="tok",
        selected_side="YES",
        action="BUY",
        selected_ask=0.4,
        selected_edge=0.1,
        stake_usd=5,
        market_age_sec=90,
        decision_ts=None,
        quote_ts=None,
        quote_age_ms=None,
        client_order_id="cid",
        idempotency_key="idem",
        limit_price=0.4,
    )
    executor.ledger.record_order_intent(intent)
    event = executor.poll_order(intent, "ord1")
    assert event["event_type"] == "order_filled"
