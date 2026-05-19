import argparse
import json

import numpy as np
import pandas as pd
import pytest

from scripts import sweep_probability_models_5m as sweep


def _synthetic_prices(rows: int = 320) -> pd.DataFrame:
    ts = pd.date_range("2026-01-01", periods=rows, freq="min", tz="UTC")
    returns = np.asarray([0.0002 * np.sin(i / 5.0) + (0.00025 if (i // 50) % 2 == 0 else -0.00015) for i in range(rows)])
    close = 100.0 * np.exp(np.cumsum(returns))
    return pd.DataFrame({"timestamp": ts, "close": close})


def test_5m_proxy_window_open_close_and_tie_labels():
    prices = pd.DataFrame(
        {
            "timestamp": pd.date_range("2026-01-01T00:00:00Z", periods=6, freq="min"),
            "close": [100.0, 101.0, 102.0, 101.0, 100.0, 99.0],
        }
    )
    out = sweep.assign_market_windows(prices, 300)
    row = out.iloc[2]
    assert row["market_window_start"] == pd.Timestamp("2026-01-01T00:00:00Z")
    assert row["K"] == pytest.approx(100.0)
    assert row["S_end"] == pytest.approx(100.0)
    assert pd.isna(row["result_up"])
    assert row["result_tie"] == pytest.approx(1.0)


def test_log_moneyness_and_tau_seconds_to_end():
    prices = pd.DataFrame(
        {
            "timestamp": pd.date_range("2026-01-01T00:00:00Z", periods=5, freq="min"),
            "close": [100.0, 101.0, 102.0, 103.0, 104.0],
        }
    )
    out = sweep.assign_market_windows(prices, 300)
    row = out.iloc[1]
    assert row["log_moneyness"] == pytest.approx(np.log(101.0 / 100.0))
    assert row["market_age_seconds"] == pytest.approx(60.0)
    assert row["seconds_to_market_end"] == pytest.approx(240.0)
    assert row["tau_minutes"] == pytest.approx(4.0)


def test_brownian_probability_formula():
    p = sweep.brownian_probability(np.asarray([0.0, 0.01, -0.01]), np.asarray([0.01, 0.01, 0.01]), np.asarray([1.0, 1.0, 1.0]))
    assert p[0] == pytest.approx(0.5)
    assert p[1] == pytest.approx(0.841344746, rel=1e-6)
    assert p[2] == pytest.approx(0.158655254, rel=1e-6)


def test_gbm_probability_formula_and_ito_comparison():
    log_m = np.asarray([0.0])
    sigma = np.asarray([0.02])
    tau = np.asarray([5.0])
    no_ito = sweep.gbm_probability(log_m, sigma, tau, include_ito=False)
    with_ito = sweep.gbm_probability(log_m, sigma, tau, include_ito=True)
    assert no_ito[0] == pytest.approx(0.5)
    assert with_ito[0] < 0.5


def test_sigma_floor_and_cap_behavior():
    low = sweep.brownian_probability(np.asarray([0.001]), np.asarray([0.0]), np.asarray([1.0]), sigma_floor=0.01)
    floored = sweep.brownian_probability(np.asarray([0.001]), np.asarray([0.01]), np.asarray([1.0]), sigma_floor=0.01)
    high = sweep.brownian_probability(np.asarray([0.001]), np.asarray([10.0]), np.asarray([1.0]), sigma_cap=0.02)
    capped = sweep.brownian_probability(np.asarray([0.001]), np.asarray([0.02]), np.asarray([1.0]), sigma_cap=0.02)
    assert low[0] == pytest.approx(floored[0])
    assert high[0] == pytest.approx(capped[0])


def test_zero_drift_vs_shrunk_drift_behavior():
    log_m = np.asarray([0.0])
    sigma = np.asarray([0.01])
    tau = np.asarray([2.0])
    zero = sweep.gbm_probability(log_m, sigma, tau, include_ito=False)
    drift = sweep.gbm_probability(log_m, sigma, tau, mu_per_minute=np.asarray([0.002]), include_ito=False)
    assert drift[0] > zero[0]


def test_empirical_bucket_model_uses_train_data_only():
    train = pd.DataFrame(
        {
            "market_age_bucket": ["0-60s"] * 5,
            "log_moneyness": [0.001] * 5,
            "result_up": [1, 1, 1, 1, 1],
        }
    )
    test = pd.DataFrame(
        {
            "market_age_bucket": ["0-60s"] * 3,
            "log_moneyness": [0.001] * 3,
            "result_up": [0, 0, 0],
        }
    )
    pred = sweep.empirical_bucket_predict(train, test, smoothing=0.0)
    assert pred.tolist() == pytest.approx([1.0 - 1e-6] * 3)


def test_bucketed_calibration_does_not_use_test_labels():
    y_train = np.asarray([1, 1, 1, 0])
    p_train = np.asarray([0.8, 0.85, 0.9, 0.1])
    p_test = np.asarray([0.85, 0.85])
    pred_a = sweep.bucketed_calibration_predict(y_train, p_train, p_test)
    pred_b = sweep.bucketed_calibration_predict(y_train, p_train, p_test)
    assert pred_a.tolist() == pytest.approx(pred_b.tolist())


def test_reliability_table_correctness():
    frame = pd.DataFrame({"model_id": ["m", "m"], "result_up": [1.0, 0.0], "p_up": [0.9, 0.1]})
    rel = sweep.reliability_table(frame)
    high = rel[rel["p_bucket"] == "0.85-0.90"].iloc[0]
    low = rel[rel["p_bucket"] == "0.05-0.10"].iloc[0]
    assert high["n"] == 1
    assert high["empirical_up_rate"] == pytest.approx(1.0)
    assert low["empirical_up_rate"] == pytest.approx(0.0)


def test_cli_smoke_on_tiny_synthetic_fixture(tmp_path):
    input_path = tmp_path / "prices.csv"
    _synthetic_prices(340).to_csv(input_path, index=False)
    output_dir = tmp_path / "out"
    diagnostics = sweep.run_sweep(
        argparse.Namespace(
            input=input_path,
            output_dir=output_dir,
            market_window_seconds=300,
            market_metadata=None,
            hmm_regime_utility_dir=None,
            candidate_regime_models="core_1m__gaussian_hmm__k4",
            model_families="baseline_50,empirical_moneyness_age,brownian_zero_drift,gbm_zero_drift,calibrated_logistic",
            max_rows=None,
            random_seed=42,
            train_days=None,
            train_rows=120,
            test_days=None,
            test_rows=60,
            step_days=None,
            step_rows=60,
            max_folds=1,
            calibration_methods="logistic,bucketed",
            price_column=None,
            prediction_sample_rows=1000,
        )
    )
    assert diagnostics["fold_count"] == 1
    assert diagnostics["model_rows"] >= 4
    for name in [
        "probability_model_summary.csv",
        "fold_metrics.csv",
        "reliability_by_model.csv",
        "metrics_by_market_age.csv",
        "metrics_by_moneyness.csv",
        "metrics_by_edge_bucket.csv",
        "metrics_by_volatility_bucket.csv",
        "probability_sweep_config.json",
        "probability_sweep_diagnostics.json",
        "summary_readme.txt",
    ]:
        assert (output_dir / name).exists()
