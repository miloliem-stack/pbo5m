#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Optional

import joblib

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts import attach_frozen_hmm_states_to_replay as attach
from scripts import fit_export_btc5m_frozen_hmm as fit_export
from scripts.run_btc5m_live_state_producer import atomic_write_json
from src.runtime.env_file import load_default_env_file

load_default_env_file()


def load_deploy_policy(bundle_dir: Path) -> dict[str, Any]:
    path = bundle_dir / "deploy_policy.json"
    if not path.exists():
        raise FileNotFoundError(f"deploy_policy.json missing: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("deploy_policy.json must be a JSON object")
    return payload


def validate_bundle_against_deploy_policy(bundle_dir: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    manifest_path = bundle_dir / "manifest.json"
    if not manifest_path.exists():
        raise FileNotFoundError(f"manifest.json missing: {manifest_path}")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    policy = load_deploy_policy(bundle_dir)
    if policy.get("policy_hmm_model_id") != manifest.get("hmm_model_id"):
        raise ValueError("deploy policy model id does not match frozen HMM manifest")
    model_path = bundle_dir / manifest["model_artifact"]
    if fit_export.file_sha256(model_path) != manifest.get("model_hash"):
        raise ValueError("frozen HMM model hash mismatch")
    if policy.get("model_artifact_hash") and policy.get("model_artifact_hash") != manifest.get("model_hash"):
        raise ValueError("deploy policy model hash does not match manifest")
    return manifest, policy


def score_latest(bundle_dir: Path, price_input: Path) -> dict[str, Any]:
    manifest, policy = validate_bundle_against_deploy_policy(bundle_dir)
    model, scaler, _, schema = attach.load_bundle(bundle_dir)
    from scripts import sweep_hmm_regime_health as health

    prices = health.load_price_frame(price_input)
    features, _ = health.build_features(prices, manifest["feature_set"])
    x = fit_export.transform_with_scaler(features.tail(5000), scaler)
    probs = attach.filtered_probabilities(model, x)
    latest = features.tail(5000).iloc[-1]
    posterior = probs[-1]
    return {
        "hmm_model_id": manifest["hmm_model_id"],
        "hmm_state": int(posterior.argmax()),
        "hmm_pmax": float(posterior.max()),
        "asof_ts": latest["timestamp"].isoformat(),
        "feature_config_hash": manifest.get("feature_config_hash"),
        "model_artifact_path": str(bundle_dir / manifest["model_artifact"]),
        "model_artifact_hash": manifest.get("model_hash"),
        "allowed_states": policy.get("allowed_states", []),
        "market_window_seconds": 300,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Score live/latest BTC-5M frozen HMM state.")
    parser.add_argument("--bundle-dir", type=Path, required=True)
    parser.add_argument("--price-input", type=Path, default=Path("data/binance/btcusdt_1m"))
    parser.add_argument("--output-path", type=Path, default=None)
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    row = score_latest(args.bundle_dir, args.price_input)
    if args.output_path:
        atomic_write_json(args.output_path, row)
    print(json.dumps(row, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
