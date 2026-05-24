import json
from pathlib import Path

from src.runtime.btc5m_brownian_conservative import BrownianConservativeConfig
from src.runtime.btc5m_brownian_runner import (
    brownian_normalized_intent_to_execution_request,
    run_brownian_conservative_cycle,
)


NOW = "2026-05-24T10:01:30Z"


def cfg(tmp_path: Path, **kwargs):
    base = {
        "enabled": True,
        "paper_only": True,
        "live_enabled": False,
        "decision_log_path": tmp_path / "decision_state.jsonl",
        "validation_log_path": tmp_path / "order_validation.jsonl",
    }
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
        "yes_top10_depth_cap": yes_cap,
        "no_top10_depth_cap": no_cap,
    }


def price(current=100.01, reference=100.0, sigma=0.01):
    return {"reference_price": reference, "current_price": current, "sigma": sigma}


def risk(bankroll=2000, **kwargs):
    row = {"bankroll": bankroll, "session_start_bankroll": bankroll, "day_start_bankroll": bankroll, "daily_pnl": 0.0}
    row.update(kwargs)
    return row


def run(tmp_path: Path, **kwargs):
    return run_brownian_conservative_cycle(
        market=kwargs.pop("market_row", market()),
        quote=kwargs.pop("quote_row", quote()),
        price_state=kwargs.pop("price_row", price()),
        risk_state=kwargs.pop("risk_row", risk()),
        config=kwargs.pop("config", cfg(tmp_path)),
        now_ts=kwargs.pop("now_ts", NOW),
        decision_log_path=tmp_path / "decision_state.jsonl",
        validation_log_path=tmp_path / "order_validation.jsonl",
        paper_intent_log_path=tmp_path / "paper_order_intents.jsonl",
        **kwargs,
    )


def test_no_trade_decision_does_not_call_validator_or_execution(tmp_path: Path):
    def validator(_):
        raise AssertionError("validator should not be called")

    def execution(_):
        raise AssertionError("execution should not be called")

    result = run(tmp_path, market_row=market(age=30), validator=validator, execution_callback=execution)
    assert result.status == "no_trade"
    assert result.reason == "market_too_young"
    assert not (tmp_path / "order_validation.jsonl").exists()


def test_accepted_paper_decision_validates_but_does_not_execute(tmp_path: Path):
    calls = {"execution": 0}

    def execution(_):
        calls["execution"] += 1
        return {"event_type": "live_order_submitted"}

    result = run(tmp_path, execution_callback=execution)
    assert result.status == "paper_validated"
    assert calls["execution"] == 0
    assert (tmp_path / "order_validation.jsonl").exists()
    assert (tmp_path / "paper_order_intents.jsonl").exists()


def test_validation_rejection_does_not_execute(tmp_path: Path):
    calls = {"execution": 0}

    def execution(_):
        calls["execution"] += 1
        return {"event_type": "live_order_submitted"}

    result = run(tmp_path, current_market_snapshot={"yes_ask": 0.50}, execution_callback=execution)
    assert result.status == "validation_rejected"
    assert result.reason == "current_ask_above_slippage"
    assert calls["execution"] == 0


def test_live_disabled_does_not_execute(tmp_path: Path):
    result = run(tmp_path, config=cfg(tmp_path, paper_only=False, live_enabled=False))
    assert result.status == "validation_rejected"
    assert result.reason == "live_not_enabled"


def test_live_enabled_calls_execution_once_with_normalized_request(tmp_path: Path):
    seen = []

    def execution(request):
        seen.append(request)
        return {"event_type": "live_order_submitted", "order_id": "ord1"}

    result = run(tmp_path, config=cfg(tmp_path, paper_only=False, live_enabled=True), execution_callback=execution)
    assert result.status == "submitted_live"
    assert len(seen) == 1
    request = seen[0]
    assert request["strategy_id"] == "brownian_no_hmm_conservative_v1"
    assert request["policy_id"] == "brownian_no_hmm_conservative_v1"
    assert request["validation_id"]
    assert request["metadata"]["validation_id"] == request["validation_id"]
    assert request["notional_usd"] == 5.0
    assert request["side"] == "YES"
    assert "order_intent" not in request


def test_execution_rejected_status_is_propagated(tmp_path: Path):
    def execution(_):
        return {"event_type": "execution_skipped", "skip_reason": "duplicate_journal_entry"}

    result = run(tmp_path, config=cfg(tmp_path, paper_only=False, live_enabled=True), execution_callback=execution)
    assert result.status == "execution_rejected"
    assert result.reason == "duplicate_journal_entry"


def test_execution_error_status_is_returned(tmp_path: Path):
    def execution(_):
        raise RuntimeError("boom")

    result = run(tmp_path, config=cfg(tmp_path, paper_only=False, live_enabled=True), execution_callback=execution)
    assert result.status == "execution_error"
    assert result.reason == "boom"


def test_duplicate_market_result_does_not_execute(tmp_path: Path):
    calls = {"execution": 0}

    def execution(_):
        calls["execution"] += 1
        return {"event_type": "live_order_submitted"}

    result = run(tmp_path, risk_row=risk(already_traded_market=True), execution_callback=execution)
    assert result.status == "no_trade"
    assert result.reason == "already_traded_market"
    assert calls["execution"] == 0


def test_stale_decision_result_does_not_execute(tmp_path: Path):
    calls = {"execution": 0}

    def execution(_):
        calls["execution"] += 1
        return {"event_type": "live_order_submitted"}

    result = run(tmp_path, validation_now_ts="2026-05-24T10:01:40Z", execution_callback=execution)
    assert result.status == "validation_rejected"
    assert result.reason == "decision_stale"
    assert calls["execution"] == 0


def test_paper_order_intents_jsonl_row_is_written(tmp_path: Path):
    result = run(tmp_path)
    assert result.status == "paper_validated"
    row = json.loads((tmp_path / "paper_order_intents.jsonl").read_text().splitlines()[0])
    for key in ["timestamp", "strategy_id", "market_id", "side", "notional_usd", "limit_price", "model_probability", "edge", "expected_log_growth", "bankroll", "stake_fraction", "validation_id"]:
        assert key in row


def test_conversion_contains_execution_metadata():
    request = brownian_normalized_intent_to_execution_request(
        {
            "market_id": "m1",
            "condition_id": "c1",
            "market_slug": "slug",
            "side": "YES",
            "notional_usd": 5.0,
            "limit_price": 0.4,
            "token_id": "yes",
            "model_id": "brownian_zero_drift__rv30",
            "model_probability": 0.5,
            "edge": 0.1,
            "expected_log_growth_recomputed": 0.001,
            "stake_fraction_recomputed": 0.0025,
            "depth_utilization_recomputed": 0.1,
            "validation_id": "val1",
            "validation_debug": {"x": 1},
        }
    )
    assert request["strategy_id"] == "brownian_no_hmm_conservative_v1"
    assert request["validation_id"] == "val1"
    assert request["metadata"]["validation_debug"] == {"x": 1}
