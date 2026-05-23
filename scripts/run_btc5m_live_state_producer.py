#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from statistics import NormalDist
from typing import Any, Optional

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.runtime.env_file import load_default_env_file

load_default_env_file()

from src.binance_price_feed import rest_binance_price_row
from src.market_router_5m import route_btc_5m_market
from src.runtime.btc5m_canary_policy import DEFAULT_HMM_MODEL_ID, REQUIRED_PROBABILITY_MODEL_ID
from src.time_utils import isoformat_utc, parse_datetime, utc_now


DEFAULT_OUTPUT_DIR = Path("artifacts/live_state")
DEFAULT_BROWNIAN_PATH = DEFAULT_OUTPUT_DIR / "btc5m_brownian_prediction.json"
DEFAULT_HMM_PATH = DEFAULT_OUTPUT_DIR / "btc5m_hmm_state.json"
DEFAULT_REFERENCE_CACHE_PATH = DEFAULT_OUTPUT_DIR / "btc5m_brownian_reference_cache.json"
DEFAULT_HMM_SOURCE_NAME = "live_hmm_state_source.json"
BROWNIAN_CONVENTION = "replay-matched brownian_zero_drift__rv30"


def atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent))
    tmp_path = Path(tmp_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, sort_keys=True, indent=2, default=str)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        tmp_path.replace(path)
    finally:
        if tmp_path.exists():
            tmp_path.unlink()


def sha256_file(path: Path) -> Optional[str]:
    if not path.exists() or not path.is_file():
        return None
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError(f"expected JSON object: {path}")
    return payload


def brownian_probability(current_price: float, reference_price: float, rv30: float, tau_minutes: float) -> float:
    sigma = min(max(float(rv30), 1e-5), 0.05)
    tau = max(float(tau_minutes), 1e-9)
    z = math.log(float(current_price) / float(reference_price)) / (sigma * math.sqrt(tau))
    return min(max(NormalDist().cdf(z), 1e-6), 1.0 - 1e-6)


def active_market_fields(router_result: dict[str, Any], *, now: datetime) -> dict[str, Any]:
    market = router_result.get("market") if isinstance(router_result, dict) else None
    if not market:
        raise RuntimeError("active_market_missing")
    start = parse_datetime(market.get("start_time") or market.get("market_start_ts"))
    end = parse_datetime(market.get("end_time") or market.get("market_end_ts"))
    if start is None or end is None:
        raise RuntimeError("active_market_timestamps_missing")
    return {
        "market_id": market.get("market_id"),
        "condition_id": market.get("condition_id"),
        "market_start_ts": isoformat_utc(start),
        "market_end_ts": isoformat_utc(end),
        "seconds_to_market_end": max(0.0, (end - now).total_seconds()),
    }


def load_or_update_reference_price(cache_path: Path, *, market: dict[str, Any], current_price: float, now: datetime) -> tuple[float, str]:
    cache: dict[str, Any] = {}
    if cache_path.exists():
        try:
            cache = load_json(cache_path)
        except Exception:
            cache = {}
    market_key = market.get("condition_id") or market.get("market_id") or market.get("market_start_ts")
    if cache.get("market_key") == market_key and cache.get("reference_price") is not None:
        return float(cache["reference_price"]), str(cache.get("reference_price_source") or "producer_cache")
    reference = float(os.environ.get("BTC5M_LIVE_REFERENCE_PRICE") or current_price)
    source = "env_BTC5M_LIVE_REFERENCE_PRICE" if os.environ.get("BTC5M_LIVE_REFERENCE_PRICE") else "producer_first_observed_price"
    atomic_write_json(
        cache_path,
        {
            "market_key": market_key,
            "market_id": market.get("market_id"),
            "condition_id": market.get("condition_id"),
            "market_start_ts": market.get("market_start_ts"),
            "reference_price": reference,
            "reference_price_source": source,
            "generated_ts": isoformat_utc(now),
        },
    )
    return reference, source


def build_brownian_state(
    *,
    router_fn=route_btc_5m_market,
    price_fn=rest_binance_price_row,
    reference_cache_path: Path = DEFAULT_REFERENCE_CACHE_PATH,
    now: Optional[datetime] = None,
) -> dict[str, Any]:
    ref_now = now or utc_now()
    market = active_market_fields(router_fn(), now=ref_now)
    price_row = price_fn()
    current_price = price_row.get("price")
    if current_price is None:
        raise RuntimeError("binance_price_missing")
    rv30 = os.environ.get("BTC5M_LIVE_RV30")
    if rv30 is None:
        raise RuntimeError("BTC5M_LIVE_RV30_missing")
    reference_price, reference_source = load_or_update_reference_price(
        reference_cache_path,
        market=market,
        current_price=float(current_price),
        now=ref_now,
    )
    p_yes = brownian_probability(float(current_price), reference_price, float(rv30), market["seconds_to_market_end"] / 60.0)
    return {
        "valid": True,
        "model_id": REQUIRED_PROBABILITY_MODEL_ID,
        "model_p_yes": p_yes,
        "model_p_no": 1.0 - p_yes,
        "market_id": market.get("market_id"),
        "condition_id": market.get("condition_id"),
        "market_start_ts": market.get("market_start_ts"),
        "market_end_ts": market.get("market_end_ts"),
        "reference_price": reference_price,
        "reference_price_source": reference_source,
        "current_price": float(current_price),
        "asof_ts": price_row.get("ts") or isoformat_utc(ref_now),
        "generated_ts": isoformat_utc(ref_now),
        "vol_window": "rv30",
        "rv30": float(rv30),
        "probability_convention": BROWNIAN_CONVENTION,
        "probability_replay_convention": BROWNIAN_CONVENTION,
    }


def resolve_hmm_source(artifact_dir: Path, source_name: str = DEFAULT_HMM_SOURCE_NAME) -> Path:
    if not artifact_dir.exists():
        raise RuntimeError(f"hmm_artifact_dir_missing:{artifact_dir}")
    source = artifact_dir / source_name
    if not source.exists():
        raise RuntimeError(f"hmm_live_state_source_missing:{source}")
    return source


def build_hmm_state(*, artifact_dir: Path, source_name: str = DEFAULT_HMM_SOURCE_NAME, now: Optional[datetime] = None) -> dict[str, Any]:
    ref_now = now or utc_now()
    source = resolve_hmm_source(artifact_dir, source_name)
    payload = load_json(source)
    if payload.get("hmm_model_id") != DEFAULT_HMM_MODEL_ID:
        raise RuntimeError("hmm_model_id_mismatch")
    if payload.get("hmm_state") is None:
        raise RuntimeError("hmm_state_missing")
    manifest_path = artifact_dir / "manifest.json"
    manifest = load_json(manifest_path) if manifest_path.exists() else {}
    model_artifact = artifact_dir / str(manifest.get("model_artifact", source.name))
    return {
        "valid": True,
        "hmm_model_id": DEFAULT_HMM_MODEL_ID,
        "hmm_state": int(payload["hmm_state"]),
        "hmm_pmax": payload.get("hmm_pmax"),
        "asof_ts": payload.get("asof_ts") or payload.get("timestamp") or isoformat_utc(ref_now),
        "generated_ts": isoformat_utc(ref_now),
        "feature_config_hash": payload.get("feature_config_hash") or manifest.get("feature_config_hash"),
        "model_artifact_path": str(model_artifact),
        "model_artifact_hash": payload.get("model_artifact_hash") or sha256_file(model_artifact),
        "market_window_seconds": payload.get("market_window_seconds") or manifest.get("market_window_seconds", 300),
        "source_path": str(source),
    }


def write_invalid_state(path: Path, *, reason: str, model_id: str, now: Optional[datetime] = None) -> None:
    ref_now = now or utc_now()
    atomic_write_json(path, {"valid": False, "model_id": model_id, "missing_reason": reason, "generated_ts": isoformat_utc(ref_now)})


def run_once(args: argparse.Namespace) -> dict[str, Any]:
    written: dict[str, Any] = {}
    try:
        brownian = build_brownian_state(reference_cache_path=args.reference_cache_path)
        atomic_write_json(args.brownian_output_path, brownian)
        written["brownian"] = str(args.brownian_output_path)
    except Exception as exc:
        if args.write_invalid_on_error:
            write_invalid_state(args.brownian_output_path, reason=str(exc), model_id=REQUIRED_PROBABILITY_MODEL_ID)
        if args.live:
            raise
        written["brownian_error"] = str(exc)
    try:
        hmm = build_hmm_state(artifact_dir=args.hmm_artifact_dir, source_name=args.hmm_source_name)
        atomic_write_json(args.hmm_output_path, hmm)
        written["hmm"] = str(args.hmm_output_path)
    except Exception as exc:
        if args.write_invalid_on_error:
            write_invalid_state(args.hmm_output_path, reason=str(exc), model_id=DEFAULT_HMM_MODEL_ID)
        if args.live:
            raise
        written["hmm_error"] = str(exc)
    return written


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Produce BTC-5M canary live Brownian and HMM state files.")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--brownian-output-path", type=Path, default=Path(os.environ.get("BTC5M_LIVE_BROWNIAN_STATE_PATH", DEFAULT_BROWNIAN_PATH)))
    parser.add_argument("--hmm-output-path", type=Path, default=Path(os.environ.get("BTC5M_LIVE_HMM_STATE_PATH", DEFAULT_HMM_PATH)))
    parser.add_argument("--reference-cache-path", type=Path, default=DEFAULT_REFERENCE_CACHE_PATH)
    parser.add_argument("--hmm-artifact-dir", type=Path, default=Path(os.environ["BTC5M_HMM_ARTIFACT_DIR"]) if os.environ.get("BTC5M_HMM_ARTIFACT_DIR") else None)
    parser.add_argument("--hmm-source-name", default=os.environ.get("BTC5M_HMM_SOURCE_NAME", DEFAULT_HMM_SOURCE_NAME))
    parser.add_argument("--poll-sec", type=float, default=float(os.environ.get("BTC5M_LIVE_STATE_PRODUCER_POLL_SEC", "2")))
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--live", action="store_true", help="Fail loudly on missing required inputs.")
    parser.add_argument("--write-invalid-on-error", action="store_true", default=True)
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    if args.hmm_artifact_dir is None:
        print("BTC5M_HMM_ARTIFACT_DIR is required", file=sys.stderr)
        return 2
    while True:
        try:
            result = run_once(args)
        except Exception as exc:
            print(f"btc5m live state producer failed: {exc}", file=sys.stderr)
            return 2
        print(json.dumps(result, sort_keys=True, default=str))
        if args.once:
            return 0
        time.sleep(args.poll_sec)


if __name__ == "__main__":
    raise SystemExit(main())
