import argparse
import json

import numpy as np
import pandas as pd
import pytest

from scripts import evaluate_hmm_regime_utility as util


def _prices(rows: int = 240) -> pd.DataFrame:
    timestamps = pd.date_range("2026-01-01", periods=rows, freq="min", tz="UTC")
    returns = np.asarray([0.0002 * np.sin(i / 4.0) + (0.0004 if (i // 30) % 2 == 0 else -0.0002) for i in range(rows)])
    close = 100.0 * np.exp(np.cumsum(returns))
    return pd.DataFrame({"timestamp": timestamps, "close": close})


def test_5m_window_assignment_and_open_close_proxy():
    prices = pd.DataFrame(
        {
            "timestamp": pd.date_range("2026-01-01T00:00:00Z", periods=6, freq="min"),
            "close": [100.0, 101.0, 102.0, 103.0, 104.0, 110.0],
        }
    )
    out = util.assign_market_window_outcomes(prices, 300)

    first = out.iloc[2]
    assert first["market_window_start"] == pd.Timestamp("2026-01-01T00:00:00Z")
    assert first["market_window_end"] == pd.Timestamp("2026-01-01T00:05:00Z")
    assert first["current_window_open_price_proxy"] == pytest.approx(100.0)
    assert first["current_window_end_price_proxy"] == pytest.approx(104.0)
    assert first["current_window_result_up_proxy"] == pytest.approx(1.0)
    assert first["market_age_seconds"] == pytest.approx(120.0)


def test_future_return_label_calculation():
    prices = pd.DataFrame(
        {
            "timestamp": pd.date_range("2026-01-01T00:00:00Z", periods=12, freq="min"),
            "close": np.arange(100.0, 112.0),
        }
    )
    out = util.assign_market_window_outcomes(prices, 300)
    row = out[out["timestamp"] == pd.Timestamp("2026-01-01T00:00:00Z")].iloc[0]

    assert row["fixed_horizon_return_1m"] == pytest.approx(np.log(101.0 / 100.0))
    assert row["fixed_horizon_return_5m"] == pytest.approx(np.log(105.0 / 100.0))
    assert row["next_5m_return"] == pytest.approx(np.log(109.0 / 105.0))
    assert row["next_5m_result_up_proxy"] == pytest.approx(1.0)


def test_regime_age_and_transition_type_calculation():
    fields = util.transition_fields(np.asarray([1, 1, 2, 2, 2, 1]))

    assert pd.isna(fields["previous_map_state"].iloc[0])
    assert fields["previous_map_state"].iloc[1:].tolist() == [1, 1, 2, 2, 2]
    assert fields["transition_type"].tolist() == ["START", "1->1", "1->2", "2->2", "2->2", "2->1"]
    assert fields["is_transition"].tolist() == [False, False, True, False, False, True]
    assert fields["regime_age_minutes"].tolist() == [0, 1, 0, 1, 2, 0]
    assert fields["minutes_since_last_transition"].tolist() == [0, 1, 0, 1, 2, 0]


def test_confidence_bucket_assignment():
    thresholds = [0.60, 0.70, 0.75, 0.80, 0.90]
    assert util.confidence_bucket(0.55, thresholds) == "<0.60"
    assert util.confidence_bucket(0.75, thresholds) == ">=0.75"
    assert util.confidence_bucket(0.99, thresholds) == ">=0.90"


def test_guardrail_flags():
    low = pd.Series({"n": 10, "up_rate_next_5m": 0.51, "mean_next_5m_abs_return": 0.02})
    flags = util.utility_flags(low, min_sample=500)
    assert "LOW_SAMPLE" in flags
    assert "WEAK_EDGE" in flags
    assert "MAGNITUDE_ONLY" in flags

    strong = pd.Series({"n": 1000, "up_rate_next_5m": 0.60, "mean_next_5m_abs_return": 0.01})
    assert util.utility_flags(strong, min_sample=500) == ""


def test_synthetic_transition_predicts_future_return():
    frame = pd.DataFrame(
        {
            "timestamp": pd.date_range("2026-01-01", periods=6, freq="min", tz="UTC"),
            "candidate_model_id": ["m"] * 6,
            "previous_map_state": [0, 0, 0, 1, 0, 1],
            "raw_state_id": [1, 1, 1, 0, 1, 0],
            "transition_type": ["0->1", "0->1", "0->1", "1->0", "0->1", "1->0"],
            "previous_canonical_label": ["a"] * 6,
            "canonical_state_label": ["b"] * 6,
            "confidence_bucket": [">=0.75"] * 6,
            "market_age_bucket": ["0-60s"] * 6,
            "age_since_switch_bucket": ["0m"] * 6,
            "current_window_result_up_proxy": [1, 1, 1, 0, 1, 0],
            "next_5m_result_up_proxy": [1, 1, 1, 0, 1, 0],
            "current_window_remaining_return": [0.1, 0.2, 0.1, -0.1, 0.1, -0.1],
            "current_window_remaining_abs_return": [0.1, 0.2, 0.1, 0.1, 0.1, 0.1],
            "next_5m_return": [0.2, 0.3, 0.2, -0.2, 0.1, -0.1],
            "next_5m_abs_return": [0.2, 0.3, 0.2, 0.2, 0.1, 0.1],
            "continuation_rate_indicator": [1, 1, 1, 0, 1, 0],
            "reversal_rate_indicator": [0, 0, 0, 1, 0, 1],
            "p_max": [0.9] * 6,
            "seconds_to_market_end": [240] * 6,
        }
    )
    grouped = util.grouped_utility(
        frame,
        [
            "candidate_model_id",
            "previous_map_state",
            "raw_state_id",
            "transition_type",
            "previous_canonical_label",
            "canonical_state_label",
            "confidence_bucket",
            "market_age_bucket",
            "age_since_switch_bucket",
        ],
        min_sample=1,
    )
    up_transition = grouped[grouped["transition_type"] == "0->1"].iloc[0]
    down_transition = grouped[grouped["transition_type"] == "1->0"].iloc[0]
    assert up_transition["up_rate_next_5m"] == pytest.approx(1.0)
    assert down_transition["up_rate_next_5m"] == pytest.approx(0.0)


def test_cli_smoke_on_tiny_synthetic_data(tmp_path):
    input_path = tmp_path / "prices.csv"
    _prices(260).to_csv(input_path, index=False)
    health_dir = tmp_path / "health"
    health_dir.mkdir()
    (health_dir / "sweep_config.json").write_text(
        json.dumps(
            {
                "train_rows": 90,
                "test_rows": 40,
                "step_rows": 40,
                "random_seed": 3,
            }
        ),
        encoding="utf-8",
    )

    out = tmp_path / "utility"
    diagnostics = util.run_evaluation(
        argparse.Namespace(
            input=input_path,
            regime_health_dir=health_dir,
            output_dir=out,
            candidates="core_1m__gaussian_hmm__k2",
            market_window_seconds=300,
            confidence_thresholds="0.60,0.70,0.75,0.80,0.90",
            min_confidence=0.75,
            max_rows=None,
            max_folds=1,
            train_rows=None,
            test_rows=None,
            step_rows=None,
            random_seed=3,
            market_metadata=None,
            price_column=None,
            min_sample=5,
            per_timestamp_sample_rows=100,
        )
    )

    assert diagnostics["per_timestamp_state_source"] == "reconstructed_candidate_only"
    assert diagnostics["state_utility_rows"] > 0
    for name in [
        "state_utility_by_candidate.csv",
        "transition_utility_by_candidate.csv",
        "regime_age_utility.csv",
        "reevaluation_trigger_utility.csv",
        "abstention_candidate_states.csv",
        "fold_stability_utility.csv",
        "utility_config.json",
        "summary_readme.txt",
    ]:
        assert (out / name).exists()
