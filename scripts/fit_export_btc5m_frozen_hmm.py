#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Optional

import joblib
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts import sweep_hmm_regime_health as health


DEFAULT_HMM_ROOT = Path("artifacts/hmm_regime_health/phase1_core_laplace_2to8")
DEFAULT_PRICE_INPUT = Path("data/binance/btcusdt_1m")
DEFAULT_OUTPUT_DIR = Path("models/btc5m/laplace_1m_gaussian_hmm_k4_frozen_v1")
BASE_MODEL_ID = "laplace_1m__gaussian_hmm__k4"
FROZEN_MODEL_ID = "laplace_1m__gaussian_hmm__k4_frozen_v1"
FEATURE_SET = "laplace_1m"
N_STATES = 4
COVARIANCE_TYPE = "diag"


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def stable_hash(payload: Any) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, default=str, separators=(",", ":")).encode("utf-8")).hexdigest()


def git_commit() -> Optional[str]:
    try:
        return subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True, stderr=subprocess.DEVNULL).strip()
    except Exception:
        return None


def load_or_build_features(hmm_root: Path, price_input: Path, *, max_rows: Optional[int] = None) -> tuple[pd.DataFrame, dict[str, Any]]:
    feature_path = hmm_root / f"features_{FEATURE_SET}.csv"
    manifest_path = hmm_root / "feature_manifest.json"
    if feature_path.exists():
        features = pd.read_csv(feature_path, parse_dates=["timestamp"])
        features["timestamp"] = pd.to_datetime(features["timestamp"], utc=True)
        feature_manifest = json.loads(manifest_path.read_text(encoding="utf-8")) if manifest_path.exists() else {}
        columns = feature_manifest.get(FEATURE_SET, {}).get("columns") or [c for c in features.columns if c not in {"timestamp", "close"}]
        manifest = feature_manifest.get(FEATURE_SET, {"feature_set": FEATURE_SET, "columns": columns})
        return features[["timestamp", "close"] + columns].copy(), manifest
    prices = health.load_price_frame(price_input, max_rows=max_rows)
    return health.build_features(prices, FEATURE_SET)


def select_training_rows(features: pd.DataFrame, args: argparse.Namespace) -> pd.DataFrame:
    frame = features.copy()
    if args.train_start_ts:
        frame = frame[frame["timestamp"] >= pd.Timestamp(args.train_start_ts, tz="UTC")]
    if args.train_end_ts:
        frame = frame[frame["timestamp"] <= pd.Timestamp(args.train_end_ts, tz="UTC")]
    if args.train_start_row is not None or args.train_end_row is not None:
        start = args.train_start_row or 0
        end = args.train_end_row if args.train_end_row is not None else len(frame)
        frame = frame.iloc[int(start) : int(end)]
    elif args.train_rows:
        frame = frame.tail(int(args.train_rows))
    if frame.empty:
        raise ValueError("no training rows selected")
    return frame.reset_index(drop=True)


def scaler_from_training(train: pd.DataFrame, columns: list[str]) -> dict[str, Any]:
    means = train[columns].astype(float).mean(axis=0)
    stds = train[columns].astype(float).std(axis=0, ddof=0).replace(0.0, 1.0).fillna(1.0)
    return {
        "type": "zscore_train_only",
        "columns": columns,
        "mean": {col: float(means[col]) for col in columns},
        "std": {col: float(stds[col]) for col in columns},
    }


def transform_with_scaler(frame: pd.DataFrame, scaler: dict[str, Any]):
    columns = list(scaler["columns"])
    x = frame[columns].astype(float).copy()
    for col in columns:
        x[col] = (x[col] - float(scaler["mean"][col])) / float(scaler["std"][col])
    return x.to_numpy()


def fit_frozen_hmm(args: argparse.Namespace) -> dict[str, Any]:
    from hmmlearn.hmm import GaussianHMM

    output_dir = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    features, feature_manifest = load_or_build_features(args.hmm_root, args.price_input, max_rows=args.max_rows)
    columns = list(feature_manifest["columns"])
    train = select_training_rows(features, args)
    scaler = scaler_from_training(train, columns)
    x_train = transform_with_scaler(train, scaler)
    model = GaussianHMM(
        n_components=N_STATES,
        covariance_type=COVARIANCE_TYPE,
        n_iter=200,
        tol=1e-3,
        random_state=int(args.random_seed),
    )
    model.fit(x_train)
    model_path = output_dir / "model.joblib"
    scaler_path = output_dir / "scaler.joblib"
    joblib.dump(model, model_path)
    joblib.dump(scaler, scaler_path)
    feature_config = {
        "feature_set": FEATURE_SET,
        "feature_builder": "scripts/sweep_hmm_regime_health.py:build_features",
        "feature_manifest": feature_manifest,
        "causality": feature_manifest.get("causality"),
    }
    feature_schema = {"columns": columns, "timestamp_column": "timestamp", "price_column": "close"}
    (output_dir / "feature_config.json").write_text(json.dumps(feature_config, indent=2, default=str), encoding="utf-8")
    (output_dir / "feature_schema.json").write_text(json.dumps(feature_schema, indent=2, default=str), encoding="utf-8")
    manifest = {
        "hmm_model_id": FROZEN_MODEL_ID,
        "base_model_family": BASE_MODEL_ID,
        "feature_set": FEATURE_SET,
        "n_states": N_STATES,
        "covariance_type": COVARIANCE_TYPE,
        "random_seed": int(args.random_seed),
        "train_start_ts": train["timestamp"].iloc[0].isoformat(),
        "train_end_ts": train["timestamp"].iloc[-1].isoformat(),
        "train_rows": int(len(train)),
        "feature_columns": columns,
        "model_artifact": "model.joblib",
        "scaler_artifact": "scaler.joblib",
        "model_hash": file_sha256(model_path),
        "scaler_hash": file_sha256(scaler_path),
        "feature_config_hash": stable_hash(feature_config),
        "git_commit": git_commit(),
        "state_label_warning": "HMM state labels are artifact-specific. Do not assume rolling-fold state labels map to this frozen model.",
    }
    (output_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, default=str), encoding="utf-8")
    return manifest


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Fit and export frozen BTC-5M laplace_1m Gaussian HMM k4.")
    parser.add_argument("--hmm-root", type=Path, default=DEFAULT_HMM_ROOT)
    parser.add_argument("--price-input", type=Path, default=DEFAULT_PRICE_INPUT)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--random-seed", type=int, default=42)
    parser.add_argument("--train-rows", type=int, default=86400)
    parser.add_argument("--train-start-row", type=int)
    parser.add_argument("--train-end-row", type=int)
    parser.add_argument("--train-start-ts")
    parser.add_argument("--train-end-ts")
    parser.add_argument("--max-rows", type=int)
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    manifest = fit_frozen_hmm(args)
    print(json.dumps({"output_dir": str(args.output_dir), "hmm_model_id": manifest["hmm_model_id"], "train_rows": manifest["train_rows"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
