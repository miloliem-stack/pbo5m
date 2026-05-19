import argparse

import numpy as np
import pandas as pd
import pytest

from scripts import rescore_probability_predictions_by_window as rescore


def _frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "model_id": ["a"] * 8 + ["b"] * 8,
            "p_up": [0.6, 0.6, 0.6, 0.6, 0.4, 0.4, 0.4, 0.4] + [0.9, 0.8, 0.7, 0.6, 0.4, 0.3, 0.2, 0.1],
            "result_up": [1, 1, 0, 0, 0, 0, 1, 1] * 2,
            "market_age_seconds": [0, 59.9, 120, 179.9, 218, 239.9, 240, 299] * 2,
            "fold_id": [0, 0, 0, 0, 1, 1, 1, 1] * 2,
            "market_window_start": ["m1", "m1", "m2", "m2", "m3", "m3", "m4", "m4"] * 2,
        }
    )


def test_window_assignment_boundaries():
    ages = pd.Series([0, 59.9, 120, 179.9, 218, 239.9, 240, 299])
    assert rescore.window_mask(ages, "pre_120").tolist() == [True, True, False, False, False, False, False, False]
    assert rescore.window_mask(ages, "pre_180").tolist() == [True, True, True, True, False, False, False, False]
    assert rescore.window_mask(ages, "pre_218").tolist() == [True, True, True, True, False, False, False, False]
    assert rescore.window_mask(ages, "pre_240").tolist() == [True, True, True, True, True, True, False, False]
    assert rescore.window_mask(ages, "post_218").tolist() == [False, False, False, False, True, True, True, True]
    assert rescore.window_mask(ages, "218_240").tolist() == [False, False, False, False, True, True, False, False]
    assert rescore.window_mask(ages, "240_300").tolist() == [False, False, False, False, False, False, True, True]


def test_brier_logloss_accuracy_calculations():
    group = pd.DataFrame({"p": [0.9, 0.1], "y": [1, 0], "age": [1, 2]})
    row = rescore.metric_row(group, model="m", window="w", prob_col="p", label_col="y", age_col="age", fold_col=None, market_col=None, eps=1e-12, ece_bins=10)
    assert row["brier"] == pytest.approx(0.01)
    assert row["log_loss"] == pytest.approx(-np.log(0.9))
    assert row["accuracy"] == pytest.approx(1.0)


def test_ece_calculation():
    assert rescore.ece(np.asarray([1.0, 0.0]), np.asarray([0.9, 0.1]), 10) == pytest.approx(0.1)


def test_rank_comparison_vs_full_window():
    metrics = rescore.compute_metrics(
        _frame(),
        model_col="model_id",
        prob_col="p_up",
        label_col="result_up",
        age_col="market_age_seconds",
        fold_col="fold_id",
        market_col="market_window_start",
        windows=["full_window", "pre_218"],
        eps=1e-12,
        ece_bins=10,
    )
    comparison = rescore.rank_comparison(metrics)
    assert {"model", "full_window_brier", "pre_218_brier", "rank_change_pre_218_vs_full"}.issubset(comparison.columns)
    assert len(comparison) == 2


def test_column_auto_detection():
    df = _frame()
    assert rescore.detect_column(df, "auto", rescore.MODEL_CANDIDATES, "model") == "model_id"
    assert rescore.detect_column(df, "auto", rescore.PROB_CANDIDATES, "prob") == "p_up"
    assert rescore.detect_column(df, "auto", rescore.LABEL_CANDIDATES, "label") == "result_up"


def test_clear_failure_on_ambiguous_or_missing_columns():
    ambiguous = pd.DataFrame({"model": ["a"], "model_id": ["b"], "p_up": [0.5], "result_up": [1], "market_age_seconds": [0]})
    with pytest.raises(ValueError, match="Ambiguous model column"):
        rescore.detect_column(ambiguous, "auto", rescore.MODEL_CANDIDATES, "model")
    missing = pd.DataFrame({"model_id": ["a"]})
    with pytest.raises(ValueError, match="Could not detect prob column"):
        rescore.detect_column(missing, "auto", rescore.PROB_CANDIDATES, "prob")


def test_csv_input_smoke(tmp_path):
    path = tmp_path / "predictions.csv"
    _frame().to_csv(path, index=False)
    out = tmp_path / "out"
    diagnostics = rescore.run(
        argparse.Namespace(
            predictions=path,
            summary=None,
            output_dir=out,
            model_col="auto",
            prob_col="auto",
            label_col="auto",
            age_col="auto",
            fold_col="auto",
            windows="default",
            clip_eps=1e-12,
            ece_bins=10,
            top_n=5,
        )
    )
    assert diagnostics["rows"] == 16
    assert (out / "probability_metrics_by_window.csv").exists()
    assert (out / "probability_model_window_rank_comparison.csv").exists()
    assert (out / "probability_window_reliability.csv").exists()
    assert (out / "probability_metrics_by_window_and_fold.csv").exists()
    assert (out / "probability_window_scorecard_readme.txt").exists()


def test_parquet_missing_engine_falls_back_to_same_stem_csv(tmp_path, monkeypatch):
    parquet_path = tmp_path / "predictions.parquet"
    csv_path = tmp_path / "predictions.csv"
    parquet_path.write_text("placeholder", encoding="utf-8")
    _frame().to_csv(csv_path, index=False)

    def _raise_import_error(_path):
        raise ImportError("missing parquet engine")

    monkeypatch.setattr(rescore.pd, "read_parquet", _raise_import_error)
    loaded = rescore.read_frame(parquet_path)
    assert len(loaded) == len(_frame())
    assert "p_up" in loaded.columns


def test_missing_csv_falls_back_to_same_stem_parquet(tmp_path, monkeypatch):
    csv_path = tmp_path / "predictions.csv"
    parquet_path = tmp_path / "predictions.parquet"
    parquet_path.write_text("placeholder", encoding="utf-8")

    def _read_parquet(path):
        assert path == parquet_path
        return _frame()

    monkeypatch.setattr(rescore.pd, "read_parquet", _read_parquet)
    loaded = rescore.read_frame(csv_path)
    assert len(loaded) == len(_frame())
    assert "result_up" in loaded.columns


def test_parquet_missing_engine_clear_error_without_csv(tmp_path, monkeypatch):
    parquet_path = tmp_path / "predictions.parquet"
    parquet_path.write_text("placeholder", encoding="utf-8")

    def _raise_import_error(_path):
        raise ImportError("missing parquet engine")

    monkeypatch.setattr(rescore.pd, "read_parquet", _raise_import_error)
    with pytest.raises(RuntimeError, match="Cannot read parquet predictions"):
        rescore.read_frame(parquet_path)


def test_parquet_input_if_available(tmp_path):
    path = tmp_path / "predictions.parquet"
    try:
        _frame().to_parquet(path, index=False)
    except Exception:
        pytest.skip("parquet support unavailable")
    out = tmp_path / "out_parquet"
    diagnostics = rescore.run(
        argparse.Namespace(
            predictions=path,
            summary=None,
            output_dir=out,
            model_col="auto",
            prob_col="auto",
            label_col="auto",
            age_col="auto",
            fold_col="auto",
            windows="full_window,pre_218",
            clip_eps=1e-12,
            ece_bins=10,
            top_n=5,
        )
    )
    assert diagnostics["rows"] == 16
    assert (out / "probability_metrics_by_window.csv").exists()
