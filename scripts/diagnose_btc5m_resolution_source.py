#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.runtime.btc5m_resolution_source import GammaCtfResolutionSource, infer_gamma_resolution, side_from_winning_index
from src.runtime.env_file import load_env_file


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Read-only diagnostic for BTC-5m Gamma+CTF resolution source.")
    parser.add_argument("--env-file", type=Path)
    parser.add_argument("--condition-id", required=True)
    parser.add_argument("--market-id")
    parser.add_argument("--expected-side", choices=["YES", "NO"])
    parser.add_argument("--raw-gamma", action="store_true")
    parser.add_argument("--json", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.env_file:
        load_env_file(args.env_file, required=True)
    try:
        row = diagnose(args)
    except Exception as exc:
        row = {"resolved": False, "error": str(exc), "condition_id": args.condition_id, "market_id": args.market_id}
    print(json.dumps(row, indent=2, sort_keys=True, default=str))
    if args.expected_side and not (row.get("resolved") is True and row.get("winning_side") == args.expected_side):
        return 1
    return 0 if row.get("error") in (None, "") else 1


def diagnose(args: argparse.Namespace, *, source: GammaCtfResolutionSource | None = None) -> dict[str, Any]:
    source = source or GammaCtfResolutionSource()
    lot = {"condition_id": args.condition_id, "market_id": args.market_id}
    market = source.gamma_fetcher(lot)
    gamma = infer_gamma_resolution(market or {}, allow_weak_mapping=source.allow_weak_gamma_mapping)
    ctf = source.read_ctf_payout(args.condition_id)
    mapped_ctf_side = side_from_winning_index(int(ctf["winning_index"]), gamma.get("side_by_index") or {}) if ctf.get("resolved") else "UNKNOWN"
    result = source.resolve(lot)
    out = {
        "condition_id": args.condition_id,
        "market_id": args.market_id,
        "gamma_status": gamma.get("status"),
        "gamma_winning_side": gamma.get("winning_side"),
        "gamma_side_by_index": gamma.get("side_by_index"),
        "gamma_warnings": gamma.get("warnings") or [],
        "ctf_denominator": ctf.get("denominator"),
        "ctf_payout_vector": ctf.get("payout_vector"),
        "ctf_winning_index": ctf.get("winning_index"),
        "mapped_ctf_winning_side": mapped_ctf_side,
        "resolved": result.get("resolved"),
        "winning_side": result.get("winning_side"),
        "error": result.get("error"),
        "source": result.get("source"),
        "onchain_confirmed": result.get("onchain_confirmed"),
        "source_diagnostics": source.diagnostics(),
    }
    if args.raw_gamma:
        out["raw_gamma"] = market
    return out


if __name__ == "__main__":
    raise SystemExit(main())
