from __future__ import annotations

import json
from pathlib import Path

import pytest

from src.runtime.btc5m_canary_policy import (
    CanaryConfig,
    evaluate_canary_policy,
    select_previous_hmm_state,
    write_decision_log_row,
)


def _config(**kwargs):
    base = {
        "min_edge": 0.02,
        "canary_stake_usd": 5.0,
        "daily_max_loss_usd": 20.0,
    }
    base.update(kwargs)
    return CanaryConfig(**base)


def _market(age):
    return {
        "market_id": "m1",
        "condition_id": "c1",
        "token_yes": "yes-token",
        "token_no": "no-token",
        "market_start_ts": "2026-05-22T10:00:00+00:00",
        "market_age_sec": age,
    }


def _quote(yes_ask=0.40, no_ask=0.52, valid=True, quote_age_ms=1000):
    return {
        "valid_topbook": valid,
        "quote_ts": "2026-05-22T10:01:00+00:00",
        "quote_age_ms": quote_age_ms,
        "yes_ask": yes_ask,
        "no_ask": no_ask,
        "yes_depth": 42.0,
        "no_depth": 31.0,
    }


def _prediction(model_id="brownian_zero_drift__rv30", p_yes=0.45):
    return {"model_id": model_id, "model_p_yes": p_yes}


def _hmm(state=3):
    return {
        "hmm_model_id": "laplace_1m__gaussian_hmm__k4",
        "hmm_state": state,
        "hmm_pmax": 0.82,
        "hmm_state_ts": "2026-05-22T10:00:00+00:00",
    }


def _decision(**overrides):
    args = {
        "market": _market(90),
        "quote": _quote(),
        "predictions": _prediction(),
        "hmm_state": _hmm(),
        "risk_state": {"open_positions": 0, "daily_loss_usd": 0.0},
        "config": _config(),
        "decision_ts": "2026-05-22T10:01:30+00:00",
    }
    args.update(overrides)
    return evaluate_canary_policy(**args)


def test_market_age_below_60_blocks_entry():
    row = _decision(market=_market(59.9))
    assert row["final_decision"] == "ABSTAIN"
    assert row["market_age_gate_pass"] is False
    assert row["abstain_reason"] == "market_age_too_young"


@pytest.mark.parametrize("age", [60.0, 120.0, 240.0])
def test_market_age_live_window_allows_evaluation(age):
    row = _decision(market=_market(age))
    assert row["final_decision"] == "BUY_YES"
    assert row["market_age_gate_pass"] is True
    assert row["abstain_reason"] is None


def test_market_age_after_240_blocks_live_but_shadow_logs_final_minute_candidate():
    row = _decision(market=_market(260))
    assert row["final_decision"] == "SHADOW_ONLY"
    assert row["market_age_gate_pass"] is False
    assert row["abstain_reason"] == "market_age_after_live_window"
    assert row["would_trade_if_final_minute_enabled"] is True


def test_market_age_after_shadow_window_abstains():
    row = _decision(market=_market(301))
    assert row["final_decision"] == "ABSTAIN"
    assert row["would_trade_if_final_minute_enabled"] is False
    assert row["abstain_reason"] == "market_age_after_live_window"


def test_hmm_state_3_passes_and_state_1_blocks():
    passing = _decision(hmm_state=_hmm(3))
    blocked = _decision(hmm_state=_hmm(1))
    assert passing["hmm_gate_pass"] is True
    assert passing["final_decision"] == "BUY_YES"
    assert blocked["hmm_gate_pass"] is False
    assert blocked["final_decision"] == "ABSTAIN"
    assert blocked["abstain_reason"] == "hmm_state_not_allowed"


def test_missing_hmm_state_blocks_with_stable_reason():
    row = _decision(hmm_state=None)
    assert row["hmm_gate_pass"] is False
    assert row["final_decision"] == "ABSTAIN"
    assert row["abstain_reason"] == "hmm_state_missing"


def test_mismatched_hmm_model_blocks_live_entry_as_missing_required_hmm():
    row = _decision(
        hmm_state={
            "hmm_model_id": "core_1m__gaussian_hmm__k4",
            "hmm_state": 3,
            "hmm_pmax": 0.99,
            "hmm_state_ts": "2026-05-22T10:00:00+00:00",
        }
    )
    assert row["hmm_gate_pass"] is False
    assert row["final_decision"] == "ABSTAIN"
    assert row["abstain_reason"] == "hmm_model_missing"


def test_disabling_hmm_gate_does_not_allow_live_entry():
    row = _decision(config=_config(hmm_gate_enabled=False))
    assert row["hmm_gate_pass"] is False
    assert row["final_decision"] == "ABSTAIN"
    assert row["abstain_reason"] == "hmm_model_missing"


def test_previous_only_hmm_selection_never_uses_future_state():
    selected = select_previous_hmm_state(
        [
            {"hmm_model_id": "laplace_1m__gaussian_hmm__k4", "hmm_state": 1, "hmm_state_ts": "2026-05-22T10:00:00+00:00"},
            {"hmm_model_id": "laplace_1m__gaussian_hmm__k4", "hmm_state": 3, "hmm_state_ts": "2026-05-22T10:01:31+00:00"},
            {"hmm_model_id": "core_1m__gaussian_hmm__k4", "hmm_state": 2, "hmm_state_ts": "2026-05-22T10:01:29+00:00"},
        ],
        decision_ts="2026-05-22T10:01:30+00:00",
        model_id="laplace_1m__gaussian_hmm__k4",
    )
    assert selected is not None
    assert selected["hmm_state"] == 1


def test_only_brownian_model_can_trade_and_baseline_blocks():
    row = _decision(predictions=_prediction("baseline_50", 0.45))
    assert row["model_gate_pass"] is False
    assert row["final_decision"] == "ABSTAIN"
    assert row["abstain_reason"] == "probability_model_missing"


def test_mismatched_probability_model_allowlist_blocks_live_entry():
    row = _decision(config=_config(model_allowlist=frozenset({"calibrated_logistic__gbm_rv30"})))
    assert row["model_gate_pass"] is False
    assert row["final_decision"] == "ABSTAIN"
    assert row["abstain_reason"] == "probability_model_mismatch"


def test_probability_formula_mismatch_blocks_live_entry():
    row = _decision(predictions={**_prediction(), "probability_formula": "hand_rolled_formula"})
    assert row["model_gate_pass"] is False
    assert row["final_decision"] == "ABSTAIN"
    assert row["abstain_reason"] == "probability_model_mismatch"


@pytest.mark.parametrize("ask", [0.30, 0.47])
def test_ask_filter_uses_strict_open_interval(ask):
    row = _decision(quote=_quote(yes_ask=ask), predictions=_prediction(p_yes=0.52))
    assert row["ask_filter_pass"] is False
    assert row["final_decision"] == "ABSTAIN"
    assert row["abstain_reason"] == "ask_filter_failed"


def test_ask_inside_range_passes():
    row = _decision(quote=_quote(yes_ask=0.31, no_ask=0.75), predictions=_prediction(p_yes=0.35))
    assert row["ask_filter_pass"] is True
    assert row["final_decision"] == "BUY_YES"


def test_selected_side_is_larger_passing_edge():
    row = _decision(quote=_quote(yes_ask=0.47, no_ask=0.36), predictions=_prediction(p_yes=0.55))
    assert row["selected_side"] == "NO"
    assert row["selected_ask"] == 0.36
    assert row["selected_edge"] == pytest.approx(0.09)
    assert row["final_decision"] == "BUY_NO"


def test_no_trade_when_edge_below_threshold():
    row = _decision(quote=_quote(yes_ask=0.44, no_ask=0.56), predictions=_prediction(p_yes=0.45))
    assert row["edge_gate_pass"] is False
    assert row["final_decision"] == "ABSTAIN"
    assert row["abstain_reason"] == "edge_below_threshold"


def test_one_entry_per_market_blocks_duplicate_entries():
    row = _decision(risk_state={"open_positions": 0, "daily_loss_usd": 0.0, "active_orders": [{"status": "pending"}]})
    assert row["one_entry_gate_pass"] is False
    assert row["final_decision"] == "ABSTAIN"
    assert row["abstain_reason"] == "duplicate_market_entry"


def test_risk_cap_blocks_entries():
    open_cap = _decision(risk_state={"open_positions": 1, "daily_loss_usd": 0.0})
    loss_cap = _decision(risk_state={"open_positions": 0, "daily_loss_usd": 25.0})
    assert open_cap["risk_gate_pass"] is False
    assert open_cap["abstain_reason"] == "risk_max_open_positions"
    assert loss_cap["risk_gate_pass"] is False
    assert loss_cap["abstain_reason"] == "risk_daily_max_loss"


def test_invalid_or_stale_quote_blocks_entry():
    invalid = _decision(quote=_quote(valid=False))
    stale = _decision(quote=_quote(quote_age_ms=6000))
    assert invalid["abstain_reason"] == "quote_invalid"
    assert stale["abstain_reason"] == "quote_stale"


def test_missing_quote_logs_quote_missing():
    row = _decision(quote={"valid_topbook": True, "quote_age_ms": 100})
    assert row["valid_topbook"] is True
    assert row["final_decision"] == "ABSTAIN"
    assert row["abstain_reason"] == "quote_missing"


def test_decision_row_logs_identity_metadata_and_config_hash():
    row = _decision(
        config=_config(
            hmm_model_version="hmm-v1",
            hmm_artifact_path="artifacts/hmm.pkl",
            probability_model_version="prob-v1",
            probability_model_artifact_path="artifacts/prob.parquet",
        )
    )
    assert row["required_hmm_model_id"] == "laplace_1m__gaussian_hmm__k4"
    assert row["required_probability_model_id"] == "brownian_zero_drift__rv30"
    assert row["hmm_model_version"] == "hmm-v1"
    assert row["probability_model_artifact_path"] == "artifacts/prob.parquet"
    assert isinstance(row["config_hash"], str)
    assert len(row["config_hash"]) == 16


def test_from_env_requires_min_edge_for_policy_startup():
    with pytest.raises(ValueError, match="BTC5M_MIN_EDGE"):
        CanaryConfig.from_env({"BTC5M_POLICY_ID": "state3_ask_brownian_age60_v0"})


def test_from_env_rejects_component_identity_substitution():
    with pytest.raises(ValueError, match="hmm_model_mismatch"):
        CanaryConfig.from_env(
            {
                "BTC5M_POLICY_ID": "state3_ask_brownian_age60_v0",
                "BTC5M_MIN_EDGE": "0.02",
                "BTC5M_HMM_MODEL_ID": "core_1m__gaussian_hmm__k4",
            }
        )
    with pytest.raises(ValueError, match="probability_model_allowlist_mismatch"):
        CanaryConfig.from_env(
            {
                "BTC5M_POLICY_ID": "state3_ask_brownian_age60_v0",
                "BTC5M_MIN_EDGE": "0.02",
                "BTC5M_MODEL_ALLOWLIST": "baseline_50",
            }
        )


def test_missing_stake_runs_shadow_only_when_everything_else_passes():
    row = _decision(config=_config(canary_stake_usd=None))
    assert row["final_decision"] == "ABSTAIN"
    assert row["abstain_reason"] == "missing_stake"


def test_decision_log_row_is_structured_jsonl(tmp_path: Path):
    path = tmp_path / "decisions.jsonl"
    row = _decision()
    write_decision_log_row(path, row)
    written = json.loads(path.read_text(encoding="utf-8"))
    assert written["policy_id"] == "state3_ask_brownian_age60_v0"
    assert written["market_id"] == "m1"
    assert written["final_decision"] == "BUY_YES"
