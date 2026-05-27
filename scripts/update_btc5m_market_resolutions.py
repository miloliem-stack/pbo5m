#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Protocol

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.runtime.btc5m_live_ledger import LiveLedger
from src.runtime.env_file import load_env_file
from src.runtime.btc5m_resolution_source import UnavailableResolutionSource, build_resolution_source
from src.time_utils import isoformat_utc, utc_now


class ResolutionSource(Protocol):
    def resolve(self, lot: dict[str, Any]) -> dict[str, Any]:
        ...

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Update BTC-5m market resolution state from reliable sources.")
    parser.add_argument("--env-file", type=Path)
    parser.add_argument("--ledger-db", type=Path, default=Path("state/btc5m_live_ledger.db"))
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--max-runtime-sec", type=float, default=0.0)
    parser.add_argument("--interval-sec", type=float, default=30.0)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--resolution-source", choices=["gamma_ctf", "unavailable"], default=os.environ.get("BTC5M_RESOLUTION_SOURCE", "gamma_ctf"))
    parser.add_argument("--require-onchain-confirmation", dest="require_onchain_confirmation", action="store_true", default=None)
    parser.add_argument("--no-require-onchain-confirmation", dest="require_onchain_confirmation", action="store_false")
    parser.add_argument("--allow-unavailable-resolution-source", action="store_true", default=False)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.env_file:
        load_env_file(args.env_file, required=True)
    if args.require_onchain_confirmation is not None:
        os.environ["BTC5M_RESOLUTION_REQUIRE_ONCHAIN_CONFIRMATION"] = "true" if args.require_onchain_confirmation else "false"
    os.environ["BTC5M_RESOLUTION_SOURCE"] = args.resolution_source
    ledger = LiveLedger(args.ledger_db)
    try:
        source = build_resolution_source(env=os.environ, allow_unavailable=args.allow_unavailable_resolution_source)
    except Exception as exc:
        result = {"ok": False, "error": str(exc), "resolution_source": args.resolution_source}
        print(json.dumps(result, indent=2, sort_keys=True, default=str))
        return 2
    deadline = time.monotonic() + args.max_runtime_sec if args.max_runtime_sec else None
    result: dict[str, Any] = {}
    while True:
        result = update_once(ledger, source=source, dry_run=args.dry_run)
        print(json.dumps(result, indent=2, sort_keys=True, default=str))
        if args.once:
            return 0
        if deadline is not None and time.monotonic() >= deadline:
            return 0
        time.sleep(max(1.0, args.interval_sec))


def update_once(ledger: LiveLedger, *, source: ResolutionSource | None = None, dry_run: bool = False) -> dict[str, Any]:
    source = source or UnavailableResolutionSource()
    lots = ledger.open_outcome_lots()
    seen: dict[str, dict[str, Any]] = {}
    for lot in lots:
        condition_id = str(lot.get("condition_id") or "")
        if not condition_id:
            continue
        seen.setdefault(condition_id, lot)
    summary = {
        "checked_ts": isoformat_utc(utc_now()),
        "conditions_checked": len(seen),
        "resolved_wins": 0,
        "resolved_losses": 0,
        "unresolved": 0,
        "warnings": [],
        "dry_run": dry_run,
    }
    for condition_id, lot in seen.items():
        result = source.resolve(lot)
        if result.get("error"):
            summary["warnings"].append({"condition_id": condition_id, "warning": result["error"]})
        resolved = bool(result.get("resolved"))
        winning_side = str(result.get("winning_side") or "UNKNOWN").upper()
        if not dry_run:
            ledger.upsert_resolution(
                condition_id=condition_id,
                market_id=lot.get("market_id"),
                resolved=resolved,
                winning_side=winning_side if resolved else "UNKNOWN",
                source=str(result.get("source") or "unknown"),
                payout_vector=result.get("payout_vector"),
            )
            ledger.terminalize_resolved_lots()
        if resolved:
            if winning_side == str(lot.get("side")).upper():
                summary["resolved_wins"] += 1
            else:
                summary["resolved_losses"] += 1
        else:
            summary["unresolved"] += 1
    return summary


if __name__ == "__main__":
    raise SystemExit(main())
