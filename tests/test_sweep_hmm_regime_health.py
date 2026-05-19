import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from scripts import sweep_hmm_regime_health as sweep


def _synthetic_prices(rows: int = 360) -> pd.DataFrame:
    timestamps = pd.date_range("2026-01-01", periods=rows, freq="min", tz="UTC")
    returns = []
    for i in range(rows):
        block = (i // 60) % 3
        if block == 0:
            returns.append(0.00015 + 0.00015 * np.sin(i / 3.0))
        elif block == 1:
            returns.append(-0.00012 + 0.0005 * np.sin(i / 2.0))
        else:
            returns.append(0.00002 + 0.0010 * np.sin(i / 1.7))
    close = 100.0 * np.exp(np.cumsum(returns))
    return pd.DataFrame({"timestamp": timestamps, "close": close})


def test_feature_builder_is_causal_for_future_price_mutation():
    prices = _synthetic_prices(140)
    baseline, _ = sweep.build_features(prices, "laplace_1m")
    mutated = prices.copy()
    mutated.loc[len(mutated) - 1, "close"] *= 10.0
    changed, _ = sweep.build_features(mutated, "laplace_1m")

    cutoff = baseline["timestamp"].iloc[-10]
    baseline_past = baseline[baseline["timestamp"] <= cutoff].reset_index(drop=True)
    changed_past = changed[changed["timestamp"] <= cutoff].reset_index(drop=True)
    pd.testing.assert_frame_equal(baseline_past, changed_past)


def test_load_price_frame_accepts_headerless_binance_directory(tmp_path):
    input_dir = tmp_path / "binance-btc1m"
    input_dir.mkdir()
    rows = [
        [1577836800000, "7195.24", "7196.25", "7183.14", "7186.68", "51.64", 1577836859999, "0", 1, "0", "0", 0],
        [1577836860000, "7187.67", "7188.06", "7182.20", "7184.03", "7.24", 1577836919999, "0", 1, "0", "0", 0],
    ]
    pd.DataFrame(rows).to_csv(input_dir / "BTCUSDT-1m-2020-01.csv", header=False, index=False)

    loaded = sweep.load_price_frame(input_dir)

    assert loaded["timestamp"].tolist() == [
        pd.Timestamp("2020-01-01T00:00:00Z"),
        pd.Timestamp("2020-01-01T00:01:00Z"),
    ]
    assert loaded["close"].tolist() == pytest.approx([7186.68, 7184.03])


def test_discover_input_files_resolves_legacy_binance_alias(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    actual = tmp_path / "data" / "binance-btc1m"
    actual.mkdir(parents=True)
    fixture = actual / "BTCUSDT-1m-2020-01.csv"
    fixture.write_text("1577836800000,1,1,1,1,1,1577836859999,1,1,1,1,0\n", encoding="utf-8")

    files = sweep.discover_input_files(Path("data/binance/btcusdt_1m"))

    assert [path.resolve() for path in files] == [fixture.resolve()]


def test_run_length_metric_correctness_on_synthetic_sequence():
    metrics = sweep.run_length_metrics(np.asarray([0, 0, 1, 1, 1, 0]))
    assert metrics["transitions_per_hour"] == pytest.approx(20.0)
    assert metrics["self_transition_rate_empirical"] == pytest.approx(0.6)
    assert metrics["mean_run_length_minutes"] == pytest.approx(2.0)
    assert metrics["median_run_length_minutes"] == pytest.approx(2.0)
    assert metrics["pct_runs_lt_2m"] == pytest.approx(1 / 3)
    assert metrics["pct_runs_gte_5m"] == pytest.approx(0.0)


def test_occupancy_and_effective_state_metrics():
    metrics = sweep.occupancy_metrics(np.asarray([0, 0, 0, 1]), 3)
    assert metrics["state_shares"] == {"0": 0.75, "1": 0.25, "2": 0.0}
    assert metrics["largest_state_share"] == pytest.approx(0.75)
    assert metrics["smallest_state_share"] == pytest.approx(0.0)
    assert metrics["dead_state_count"] == 1
    assert metrics["low_occupancy_state_count"] == 1
    assert metrics["effective_n_states"] == pytest.approx(np.exp(-(0.75 * np.log(0.75) + 0.25 * np.log(0.25))))
    assert metrics["normalized_occupancy_entropy"] == pytest.approx((-(0.75 * np.log(0.75) + 0.25 * np.log(0.25))) / np.log(3))


def test_confidence_coverage_metrics():
    metrics = sweep.confidence_metrics(np.asarray([0.55, 0.75, 0.9]), [0.60, 0.75, 0.90])
    assert metrics["mean_pmax"] == pytest.approx((0.55 + 0.75 + 0.9) / 3)
    assert metrics["median_pmax"] == pytest.approx(0.75)
    assert metrics["coverage_pmax_ge_0_60"] == pytest.approx(2 / 3)
    assert metrics["coverage_pmax_ge_0_75"] == pytest.approx(2 / 3)
    assert metrics["coverage_pmax_ge_0_90"] == pytest.approx(1 / 3)


def test_rejection_flags():
    flags = sweep.rejection_flags(
        {
            "median_run_length_minutes": 3.0,
            "transitions_per_hour": 14.0,
            "largest_confident_state_share": 0.9,
            "confident_low_occupancy_state_count": 2,
            "coverage_pmax_ge_0_75": 0.2,
            "minimum_pairwise_separation": 0.1,
        },
        n_states=8,
        min_confidence=0.75,
    )
    assert "REJECT_FLICKER" in flags
    assert "REJECT_COLLAPSE" in flags
    assert "REJECT_DEAD_STATES" in flags
    assert "REJECT_LOW_CONFIDENCE" in flags
    assert "REJECT_LOW_SEPARATION" in flags
    assert "WARN_COMPLEXITY" in flags


def test_cli_smoke_and_deterministic_output(tmp_path):
    input_path = tmp_path / "prices.csv"
    _synthetic_prices(360).to_csv(input_path, index=False)

    common = {
        "input": input_path,
        "state_counts": "2",
        "families": "gaussian_hmm",
        "feature_sets": "core_1m",
        "train_days": None,
        "train_rows": 120,
        "test_days": None,
        "test_rows": 60,
        "step_days": None,
        "step_rows": 60,
        "max_rows": None,
        "random_seed": 7,
        "min_confidence": 0.75,
        "confidence_thresholds": "0.60,0.70,0.75,0.80,0.90",
    }
    out1 = tmp_path / "out1"
    out2 = tmp_path / "out2"
    diag1 = sweep.run_sweep(argparse.Namespace(output_dir=out1, **common))
    diag2 = sweep.run_sweep(argparse.Namespace(output_dir=out2, **common))

    assert diag1["summary_rows"] == 1
    assert diag2["summary_rows"] == 1
    for name in [
        "regime_health_summary.csv",
        "fold_metrics.csv",
        "state_occupancy.csv",
        "run_length_metrics.csv",
        "state_feature_signatures.csv",
        "transition_matrices.json",
        "survival_metrics.csv",
        "feature_manifest.json",
        "sweep_config.json",
        "regime_health_readme_summary.txt",
    ]:
        assert (out1 / name).exists()

    s1 = pd.read_csv(out1 / "regime_health_summary.csv")
    s2 = pd.read_csv(out2 / "regime_health_summary.csv")
    assert s1["model_id"].tolist() == ["core_1m__gaussian_hmm__k2"]
    assert s2["model_id"].tolist() == s1["model_id"].tolist()
    assert s2["regime_health_score"].tolist() == pytest.approx(s1["regime_health_score"].tolist())
    manifest = json.loads((out1 / "feature_manifest.json").read_text(encoding="utf-8"))
    assert manifest["core_1m"]["causality"].startswith("strictly trailing")
