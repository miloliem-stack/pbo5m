from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

from scripts import fit_export_btc5m_frozen_hmm as fit_export


def _price_csv(path: Path, rows: int = 260) -> Path:
    ts = pd.date_range("2026-01-01", periods=rows, freq="1min", tz="UTC")
    price = 100 + np.sin(np.arange(rows) / 7.0).cumsum() * 0.02 + np.arange(rows) * 0.001
    frame = pd.DataFrame({"timestamp": ts, "close": price})
    out = path / "prices.csv"
    frame.to_csv(out, index=False)
    return out


def test_frozen_fit_writes_model_bundle_and_manifest(tmp_path: Path):
    price = _price_csv(tmp_path)
    out = tmp_path / "bundle"
    args = fit_export.build_parser().parse_args(
        [
            "--price-input",
            str(price),
            "--hmm-root",
            str(tmp_path / "missing_hmm_root"),
            "--output-dir",
            str(out),
            "--train-rows",
            "120",
            "--random-seed",
            "7",
        ]
    )
    manifest = fit_export.fit_frozen_hmm(args)

    assert (out / "model.joblib").exists()
    assert (out / "scaler.joblib").exists()
    assert (out / "feature_config.json").exists()
    assert (out / "feature_schema.json").exists()
    assert (out / "manifest.json").exists()
    loaded = json.loads((out / "manifest.json").read_text(encoding="utf-8"))
    assert loaded["hmm_model_id"] == "laplace_1m__gaussian_hmm__k4_frozen_v1"
    assert loaded["base_model_family"] == "laplace_1m__gaussian_hmm__k4"
    assert loaded["random_seed"] == 7
    assert loaded["feature_columns"] == json.loads((out / "feature_schema.json").read_text(encoding="utf-8"))["columns"]
    assert loaded["model_hash"] == fit_export.file_sha256(out / "model.joblib")
    assert manifest["train_rows"] == 120


def test_feature_schema_order_is_preserved(tmp_path: Path):
    price = _price_csv(tmp_path)
    out = tmp_path / "bundle"
    args = fit_export.build_parser().parse_args(["--price-input", str(price), "--hmm-root", str(tmp_path / "missing"), "--output-dir", str(out), "--train-rows", "120"])
    fit_export.fit_frozen_hmm(args)
    schema = json.loads((out / "feature_schema.json").read_text(encoding="utf-8"))
    assert schema["columns"][0:5] == ["log_return_1m", "r_lag_1m", "r_lag_2m", "r_lag_3m", "r_lag_5m"]
    assert "ew_mean_return_hl_3m" in schema["columns"]
