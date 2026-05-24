import json
import math
from pathlib import Path

import pytest

from src.runtime.btc5m_brownian_conservative import (
    BrownianConservativeConfig,
    brownian_zero_drift_p_yes,
    compute_conservative_stake,
    decide_brownian_conservative,
    expected_log_growth,
    full_kelly_fraction_binary_contract,
    move_probability_toward_half,
    normal_cdf,
    write_decision_log_row,
)


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
        "yes_token_id": "yes",
        "no_token_id": "no",
    }


def quote(yes=0.40, no=0.80, yes_cap=1000, no_cap=1000):
    return {
        "valid_topbook": True,
        "yes_ask": yes,
        "no_ask": no,
        "yes_asks": [{"price": yes, "size": yes_cap / yes}],
        "no_asks": [{"price": no, "size": no_cap / no}],
    }


def price(current=100.001, reference=100.0, sigma=0.001):
    return {"reference_price": reference, "current_price": current, "sigma": sigma}


def risk(bankroll=2000, **kwargs):
    row = {"bankroll": bankroll, "session_start_bankroll": bankroll, "day_start_bankroll": bankroll, "daily_pnl": 0.0}
    row.update(kwargs)
    return row


def decision(**kwargs):
    return decide_brownian_conservative(market=kwargs.pop("market", market()), quote=kwargs.pop("quote", quote()), price_state=kwargs.pop("price_state", price()), risk_state=kwargs.pop("risk_state", risk()), config=kwargs.pop("config", cfg()), decision_ts="2026-05-24T10:01:30Z")


def test_math_helpers_match_expected_formula():
    assert normal_cdf(0) == pytest.approx(0.5)
    assert full_kelly_fraction_binary_contract(0.52, 0.40) == pytest.approx((0.52 - 0.40) / (1 - 0.40))
    assert move_probability_toward_half(0.60, 0.02) == pytest.approx(0.58)
    f = 0.0025
    p = 0.55
    ask = 0.40
    expected = p * math.log(1 + f * ((1 - ask) / ask)) + (1 - p) * math.log(1 - f)
    assert expected_log_growth(p, ask, f) == pytest.approx(expected)


def test_brownian_formula_prefers_current_above_reference():
    assert brownian_zero_drift_p_yes(100, 101, 0.01, 120) > 0.5
    assert brownian_zero_drift_p_yes(100, 99, 0.01, 120) < 0.5


@pytest.mark.parametrize("age,reason", [(59, "market_too_young"), (240, "market_too_old")])
def test_market_age_rejects(age, reason):
    row = decision(market=market(age))
    assert not row["should_trade"]
    assert row["reject_reason"] == reason


def test_yes_side_accepted_when_yes_edge_passes():
    row = decision(quote=quote(yes=0.40, no=0.90), price_state=price(current=100.01, sigma=0.01))
    assert row["should_trade"]
    assert row["chosen_side"] == "YES"


def test_no_side_accepted_when_no_edge_passes():
    row = decision(quote=quote(yes=0.90, no=0.40), price_state=price(current=99.99, sigma=0.01))
    assert row["should_trade"]
    assert row["chosen_side"] == "NO"


def test_higher_edge_side_selected_when_both_pass():
    row = decision(quote=quote(yes=0.45, no=0.45), price_state=price(current=100.01, sigma=0.01))
    assert row["should_trade"]
    assert row["chosen_side"] == "YES"
    assert row["yes_edge"] > row["no_edge"]


def test_ask_below_min_and_edge_below_threshold_reject():
    assert decision(quote=quote(yes=0.30, no=0.95))["reject_reason"] == "ask_below_min"
    low_edge = decision(quote=quote(yes=0.80, no=0.80), price_state=price(current=100.0, sigma=0.01))
    assert low_edge["reject_reason"] == "edge_below_threshold"


def test_expected_growth_gate_rejects_after_haircut_and_slippage():
    row = decision(config=cfg(probability_haircut_abs=0.49, ask_slippage_abs=0.40, min_order_notional=0.0), quote=quote(yes=0.40, no=0.90), price_state=price(current=101.0, sigma=0.01), risk_state=risk(2000))
    assert row["reject_reason"] == "expected_growth_not_positive"


def test_below_2000_computed_stake_below_5_is_skipped():
    row = decision(config=cfg(), risk_state=risk(100), quote=quote(yes=0.40, no=0.90), price_state=price(current=100.01, sigma=0.01))
    assert row["reject_reason"] == "below_min_order_notional"


def test_at_2000_min_order_allowed_when_equal_to_fraction_cap():
    row = decision(config=cfg(), risk_state=risk(2000), quote=quote(yes=0.40, no=0.90), price_state=price(current=100.01, sigma=0.01))
    assert row["should_trade"]
    assert row["stake_notional"] == pytest.approx(5.0)


def test_rounding_to_min_order_rejected_if_violates_fraction_cap():
    stake = compute_conservative_stake(bankroll=1000, probability=0.9, ask=0.40, depth_cap=1000, config=cfg(min_order_notional=5.0))
    assert stake["reject_reason"] == "below_min_order_notional"


def test_top10_depth_caps_stake():
    row = decision(config=cfg(min_order_notional=0.0), quote=quote(yes=0.40, no=0.90, yes_cap=0.50), price_state=price(current=100.01, sigma=0.01), risk_state=risk(2000))
    assert row["should_trade"]
    assert row["stake_notional"] == pytest.approx(0.50)
    assert row["depth_utilization"] == pytest.approx(1.0)


def test_duplicate_and_stop_guards_reject():
    assert decision(risk_state=risk(2000, already_traded_market=True))["reject_reason"] == "already_traded_market"
    assert decision(risk_state=risk(2000, daily_pnl=-100))["reject_reason"] == "daily_stop_loss_guard"
    assert decision(risk_state=risk(1800, session_start_bankroll=2000))["reject_reason"] == "session_stop_loss_guard"


def test_decision_log_contains_required_fields(tmp_path: Path):
    row = decision(config=cfg(decision_log_path=tmp_path / "decision_state.jsonl"))
    write_decision_log_row(tmp_path / "decision_state.jsonl", row)
    written = json.loads((tmp_path / "decision_state.jsonl").read_text().splitlines()[0])
    for key in ["timestamp", "strategy_id", "model_id", "p_yes", "yes_ask", "chosen_side", "stake_notional", "bankroll_before", "should_trade", "reject_reason"]:
        assert key in written
