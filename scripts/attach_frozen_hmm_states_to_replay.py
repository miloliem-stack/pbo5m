#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Optional

import joblib
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts import fit_export_btc5m_frozen_hmm as fit_export
from scripts import sweep_hmm_regime_health as health


DEFAULT_REPLAY_PATH = Path("artifacts/market_age_policy_replay/compact_20260423_20260511_state3_ask_age_v1/trade_level_policy_results.parquet")
DEFAULT_PRICE_INPUT = Path("data/binance/btcusdt_1m")
DEFAULT_OUTPUT_ROOT = Path("artifacts/frozen_hmm_state_attribution")


def load_bundle(bundle_dir: Path) -> tuple[Any, dict[str, Any], dict[str, Any], dict[str, Any]]:
    manifest_path = bundle_dir / "manifest.json"
    schema_path = bundle_dir / "feature_schema.json"
    if not manifest_path.exists() or not schema_path.exists():
        raise FileNotFoundError(f"missing frozen HMM manifest/schema in {bundle_dir}")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    model_path = bundle_dir / manifest["model_artifact"]
    scaler_path = bundle_dir / manifest["scaler_artifact"]
    if not model_path.exists() or not scaler_path.exists():
        raise FileNotFoundError(f"missing frozen HMM model/scaler in {bundle_dir}")
    if fit_export.file_sha256(model_path) != manifest.get("model_hash"):
        raise ValueError("frozen HMM model hash mismatch")
    if fit_export.file_sha256(scaler_path) != manifest.get("scaler_hash"):
        raise ValueError("frozen HMM scaler hash mismatch")
    return joblib.load(model_path), joblib.load(scaler_path), manifest, schema


def filtered_probabilities(model: Any, x: np.ndarray) -> np.ndarray:
    return health.filtered_probabilities(model, x)


def build_feature_scores(bundle_dir: Path, price_input: Path) -> pd.DataFrame:
    model, scaler, manifest, schema = load_bundle(bundle_dir)
    prices = health.load_price_frame(price_input)
    features, _ = health.build_features(prices, manifest["feature_set"])
    columns = list(schema["columns"])
    missing = [col for col in columns if col not in features.columns]
    if missing:
        raise ValueError(f"feature columns missing for frozen HMM scoring: {missing[:5]}")
    x = fit_export.transform_with_scaler(features, scaler)
    probs = filtered_probabilities(model, x)
    out = features[["timestamp"]].copy()
    out["hmm_asof_ts"] = out["timestamp"]
    out["frozen_hmm_model_id"] = manifest["hmm_model_id"]
    out["frozen_hmm_state"] = probs.argmax(axis=1).astype(int)
    out["frozen_hmm_pmax"] = probs.max(axis=1)
    for idx in range(probs.shape[1]):
        out[f"frozen_hmm_p{idx}"] = probs[:, idx]
    changed = out["frozen_hmm_state"].ne(out["frozen_hmm_state"].shift())
    run_start = out["timestamp"].where(changed).ffill()
    out["hmm_state_age_sec"] = (out["timestamp"] - run_start).dt.total_seconds()
    return out


def attach_states(replay: pd.DataFrame, states: pd.DataFrame) -> pd.DataFrame:
    out = replay.copy()
    ts_col = "entry_ts" if "entry_ts" in out.columns else "timestamp"
    out[ts_col] = pd.to_datetime(out[ts_col], utc=True).dt.as_unit("ns")
    states = states.sort_values("timestamp").copy()
    states["hmm_asof_ts"] = pd.to_datetime(states["hmm_asof_ts"], utc=True).dt.as_unit("ns")
    merged = pd.merge_asof(
        out.sort_values(ts_col),
        states.drop(columns=["timestamp"]).sort_values("hmm_asof_ts"),
        left_on=ts_col,
        right_on="hmm_asof_ts",
        direction="backward",
        allow_exact_matches=True,
    )
    if merged["frozen_hmm_state"].isna().any():
        raise ValueError("frozen HMM coverage missing for some replay rows")
    return merged


def run(args: argparse.Namespace) -> dict[str, Any]:
    run_id = args.run_id or args.bundle_dir.name
    output_dir = args.output_root / run_id
    output_dir.mkdir(parents=True, exist_ok=args.overwrite)
    replay = pd.read_parquet(args.replay_path)
    states = build_feature_scores(args.bundle_dir, args.price_input)
    attached = attach_states(replay, states)
    out_path = output_dir / "trade_level_with_frozen_hmm.parquet"
    attached.to_parquet(out_path, index=False)
    manifest = {
        "bundle_dir": str(args.bundle_dir),
        "replay_path": str(args.replay_path),
        "price_input": str(args.price_input),
        "output_path": str(out_path),
        "rows": int(len(attached)),
        "state_min_ts": states["timestamp"].min().isoformat(),
        "state_max_ts": states["timestamp"].max().isoformat(),
        "previous_only_asof": True,
    }
    (output_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, default=str), encoding="utf-8")
    return manifest


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Attach frozen HMM states to BTC-5M replay rows causally.")
    parser.add_argument("--bundle-dir", type=Path, required=True)
    parser.add_argument("--replay-path", type=Path, default=DEFAULT_REPLAY_PATH)
    parser.add_argument("--price-input", type=Path, default=DEFAULT_PRICE_INPUT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--run-id")
    parser.add_argument("--overwrite", action="store_true")
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    print(json.dumps(run(build_parser().parse_args(argv)), sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
