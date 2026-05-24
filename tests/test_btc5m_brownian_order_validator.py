import json
from pathlib import Path

import pytest

from src.runtime.btc5m_brownian_conservative import BrownianConservativeConfig, decide_brownian_conservative
from src.runtime.btc5m_brownian_order_validator import (
    BrownianOrderValidationInput,
    validate_and_log_brownian_order_intent,
    validate_brownian_order_intent,
    validation_log_row,
)


NOW = "2026-05-24T10:01:31Z"


def cfg(**kwargs):
    base = {"enabled": True, "paper_only": True, "min_order_notional": 5.0}
    base.update(kwargs)
    return BrownianConservativeConfig(**base)


def market(age=90):
    return {
        "market_id": "m1",
        "condition_id": "c1",
        "slug": "btc-updown",
        "market_start_ts": "2026-05-24T10:00:00Z",
        "market_end_ts": "2026-05-24T10:05:00Z",
        "market_age_seconds": age,
        "yes_token_id": "yes-token",
        "no_token_id": "no-token",
    }


def quote(yes=0.40, no=0.90, yes_cap=1000, no_cap=1000):
    return {
        "valid_topbook": True,
        "yes_ask": yes,
        "no_ask": no,
        "yes_token_id": "yes-token",
        "no_token_id": "no-token",
        "yes_asks": [{"price": yes, "size": yes_cap / yes}],
        "no_asks": [{"price": no, "size": no_cap / no}],
    }


def price(current=100.01, reference=100.0, sigma=0.01):
    return {"reference_price": reference, "current_price": current, "sigma": sigma}


def risk(bankroll=2000, **kwargs):
    row = {"bankroll": bankroll, "session_start_bankroll": bankroll, "day_start_bankroll": bankroll, "daily_pnl": 0.0}
    row.update(kwargs)
    return row


def accepted_decision(*, config=None, bankroll=2000, q=None, p=None, market_row=None, quote_row=None):
    config = config or cfg()
    market_row = market_row or market()
    quote_row = quote_row or quote()
    row = decide_brownian_conservative(
        market=market_row,
        quote=quote_row,
        price_state=price(),
        risk_state=risk(bankroll),
        config=config,
        decision_ts="2026-05-24T10:01:30Z",
    )
    assert row["should_trade"], row
    if q is not None:
        row["chosen_ask"] = q
        row["order_intent"]["intended_ask"] = q
        row["order_intent"]["limit_price"] = q
        row["order_intent"]["selected_ask"] = q
    if p is not None:
        row["chosen_probability"] = p
        row["order_intent"]["model_probability"] = p
    return row


def snapshot(**kwargs):
    row = {
        "market_id": "m1",
        "condition_id": "c1",
        "slug": "btc-updown",
        "market_start_ts": "2026-05-24T10:00:00Z",
        "market_end_ts": "2026-05-24T10:05:00Z",
        "tradable": True,
        "is_open": True,
        "valid_topbook": True,
        "yes_token_id": "yes-token",
        "no_token_id": "no-token",
        "yes_ask": 0.40,
        "no_ask": 0.90,
        "yes_top10_depth_cap": 1000.0,
        "no_top10_depth_cap": 1000.0,
    }
    row.update(kwargs)
    return row


def vin(row=None, *, snap=None, bankroll=2000, paper_only=True, live_enabled=False, config=None, already=False, now=NOW):
    row = row or accepted_decision(config=config or cfg(), bankroll=bankroll)
    return BrownianOrderValidationInput(
        order_intent=row["order_intent"],
        decision_row=row,
        current_market_snapshot=snap or snapshot(),
        bankroll=bankroll,
        already_traded_market=already,
        paper_only=paper_only,
        live_enabled=live_enabled,
        config=config or cfg(),
        now_ts=now,
    )


def test_accepts_valid_paper_order_intent_and_not_live_executable():
    result = validate_brownian_order_intent(vin())
    assert result.accepted
    assert result.normalized_order_intent["paper_only"] is True
    assert result.normalized_order_intent["executable_live"] is False


def test_rejects_wrong_strategy_and_wrong_model_id():
    row = accepted_decision()
    row["order_intent"]["strategy_id"] = "other"
    assert validate_brownian_order_intent(vin(row)).reject_reason == "wrong_strategy_id"
    row = accepted_decision()
    row["order_intent"]["model_id"] = "baseline_50"
    assert validate_brownian_order_intent(vin(row)).reject_reason == "wrong_model_id"


def test_rejects_missing_required_fields_and_bad_side():
    row = accepted_decision()
    del row["order_intent"]["expected_log_growth"]
    del row["expected_log_growth"]
    assert validate_brownian_order_intent(vin(row)).reject_reason == "missing_required_fields"
    row = accepted_decision()
    row["order_intent"]["side"] = "MAYBE"
    assert validate_brownian_order_intent(vin(row)).reject_reason == "invalid_side"


def test_rejects_duplicate_stale_age_and_closed_market():
    assert validate_brownian_order_intent(vin(already=True)).reject_reason == "already_traded_market"
    assert validate_brownian_order_intent(vin(now="2026-05-24T10:01:40Z")).reject_reason == "decision_stale"
    young = accepted_decision(market_row=market(age=60))
    young["decision_ts"] = "2026-05-24T10:00:59Z"
    young["order_intent"]["decision_ts"] = "2026-05-24T10:00:59Z"
    assert validate_brownian_order_intent(vin(young, now="2026-05-24T10:00:59Z")).reject_reason == "market_too_young"
    old = accepted_decision(market_row=market(age=239))
    old["decision_ts"] = "2026-05-24T10:04:00Z"
    old["order_intent"]["decision_ts"] = "2026-05-24T10:04:00Z"
    assert validate_brownian_order_intent(vin(old, now="2026-05-24T10:04:00Z")).reject_reason == "market_too_old"
    assert validate_brownian_order_intent(vin(snap=snapshot(tradable=False))).reject_reason == "market_not_tradable"


def test_rejects_market_and_token_mismatch():
    assert validate_brownian_order_intent(vin(snap=snapshot(market_id="m2"))).reject_reason == "market_identity_mismatch"
    row = accepted_decision()
    row["order_intent"]["token_id"] = "wrong"
    assert validate_brownian_order_intent(vin(row)).reject_reason == "token_side_mismatch"


def test_rejects_quote_refresh_and_invalid_topbook_conditions():
    assert validate_brownian_order_intent(vin(snap=snapshot(yes_ask=0.42))).reject_reason == "current_ask_above_slippage"
    assert validate_brownian_order_intent(vin(snap=snapshot(yes_ask=0.30))).reject_reason == "ask_below_min"
    assert validate_brownian_order_intent(vin(snap=snapshot(valid_topbook=False))).reject_reason == "invalid_topbook"
    assert validate_brownian_order_intent(vin(snap=snapshot(yes_top10_depth_cap=0))).reject_reason == "missing_depth"


def test_rejects_edge_and_growth_after_current_ask_refresh():
    row = accepted_decision(p=0.425)
    assert validate_brownian_order_intent(vin(row, snap=snapshot(yes_ask=0.415), config=cfg(ask_slippage_abs=0.02))).reject_reason == "edge_below_threshold_current"
    high_growth_haircut = cfg(probability_haircut_abs=0.49, ask_slippage_abs=0.40, min_order_notional=5.0)
    row = accepted_decision(config=cfg(), bankroll=2000)
    result = validate_brownian_order_intent(vin(row, config=high_growth_haircut))
    assert result.reject_reason == "expected_growth_not_positive_current"


def test_rejects_stake_above_bankroll_fraction_and_depth():
    row = accepted_decision(bankroll=2000)
    row["order_intent"]["stake_notional"] = 2001
    row["order_intent"]["notional_usd"] = 2001
    assert validate_brownian_order_intent(vin(row, bankroll=2000)).reject_reason == "stake_above_bankroll"
    row = accepted_decision(bankroll=2000)
    row["order_intent"]["stake_notional"] = 6
    row["order_intent"]["notional_usd"] = 6
    assert validate_brownian_order_intent(vin(row, bankroll=2000)).reject_reason == "stake_above_max_fraction"
    assert validate_brownian_order_intent(vin(snap=snapshot(yes_top10_depth_cap=4.99))).reject_reason == "stake_above_depth"


def test_min_order_threshold_behavior():
    row = accepted_decision(bankroll=2000)
    ok = validate_brownian_order_intent(vin(row, bankroll=2000))
    assert ok.accepted
    row = accepted_decision(bankroll=2000)
    assert row["order_intent"]["stake_notional"] == pytest.approx(5.0)
    assert validate_brownian_order_intent(vin(row, bankroll=1999)).reject_reason == "stake_above_max_fraction"


def test_live_requires_live_enabled_and_then_accepts():
    assert validate_brownian_order_intent(vin(paper_only=False, live_enabled=False)).reject_reason == "live_not_enabled"
    result = validate_brownian_order_intent(vin(paper_only=False, live_enabled=True))
    assert result.accepted
    assert result.normalized_order_intent["executable_live"] is True


def test_normalized_intent_and_log_row_have_audit_fields(tmp_path: Path):
    result = validate_brownian_order_intent(vin())
    normalized = result.normalized_order_intent
    for key in ["strategy_id", "market_id", "side", "notional_usd", "limit_price", "token_id", "model_probability", "edge", "validation_id"]:
        assert key in normalized
    log_row = validation_log_row(vin(), result)
    for key in ["accepted", "reject_reason", "intended_ask", "current_ask", "edge_using_current_ask", "expected_log_growth_recomputed", "depth_utilization"]:
        assert key in log_row
    path = tmp_path / "order_validation.jsonl"
    validate_and_log_brownian_order_intent(vin(), path=path)
    written = json.loads(path.read_text().splitlines()[0])
    assert written["accepted"] is True
