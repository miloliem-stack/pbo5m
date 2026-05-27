#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.runtime.btc5m_live_ledger import LiveLedger
from src.runtime.env_file import load_env_file
from src.runtime.polymarket_funder_setup import PolymarketFunderConfig
from src.time_utils import isoformat_utc, utc_now


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Independent BTC-5m Polymarket outcome-token redeemer.")
    parser.add_argument("--env-file", type=Path)
    parser.add_argument("--ledger-db", type=Path, default=Path(os.environ.get("BTC5M_LIVE_LEDGER_DB", "state/btc5m_live_ledger.db")))
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--interval-sec", type=float, default=60.0)
    parser.add_argument("--max-runtime-sec", type=float, default=0.0)
    parser.add_argument("--dry-run", action="store_true", default=False)
    parser.add_argument("--yes-i-understand-this-sends-transactions", action="store_true", default=False)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.env_file:
        load_env_file(args.env_file, required=True)
    ledger = LiveLedger(args.ledger_db)
    config = PolymarketFunderConfig.from_env()
    deadline = time.monotonic() + args.max_runtime_sec if args.max_runtime_sec else None
    result: dict[str, Any] = {}
    while True:
        result = run_once(ledger, config=config, dry_run=args.dry_run, allow_tx=args.yes_i_understand_this_sends_transactions)
        print(json.dumps(result, indent=2, sort_keys=True, default=str))
        if args.once:
            return 0 if result.get("ok") else 1
        if deadline is not None and time.monotonic() >= deadline:
            return 0 if result.get("ok") else 1
        time.sleep(max(1.0, args.interval_sec))


def run_once(ledger: LiveLedger, *, config: PolymarketFunderConfig, dry_run: bool, allow_tx: bool) -> dict[str, Any]:
    ledger.terminalize_resolved_lots()
    lots = ledger.redeemable_lots()
    grouped: dict[str, list[dict[str, Any]]] = {}
    for lot in lots:
        grouped.setdefault(str(lot["condition_id"]), []).append(lot)
    events: list[dict[str, Any]] = []
    for condition_id, condition_lots in grouped.items():
        token_ids = sorted({str(lot["token_id"]) for lot in condition_lots if lot.get("token_id")})
        market_id = condition_lots[0].get("market_id")
        if dry_run:
            attempt_id = ledger.record_redemption_attempt(
                condition_id=condition_id,
                market_id=market_id,
                token_ids=token_ids,
                index_sets=[1, 2],
                status="dry_run",
            )
            events.append({"condition_id": condition_id, "attempt_id": attempt_id, "status": "dry_run", "token_ids": token_ids})
            continue
        if not allow_tx:
            attempt_id = ledger.record_redemption_attempt(
                condition_id=condition_id,
                market_id=market_id,
                token_ids=token_ids,
                index_sets=[1, 2],
                status="failed_terminal",
                raw_error="redemption_requires_confirmation_flag",
            )
            events.append({"condition_id": condition_id, "attempt_id": attempt_id, "status": "failed_terminal", "error_code": "redemption_requires_confirmation_flag"})
            continue
        attempt_id = ledger.record_redemption_attempt(
            condition_id=condition_id,
            market_id=market_id,
            token_ids=token_ids,
            index_sets=[1, 2],
            status="failed_terminal",
            raw_error="redeem_adapter_not_implemented",
        )
        events.append(
            {
                "condition_id": condition_id,
                "attempt_id": attempt_id,
                "status": "failed_terminal",
                "error_code": "redeem_adapter_not_implemented",
                "config": config.redacted(),
            }
        )
    return {
        "ok": True,
        "dry_run": dry_run,
        "checked_ts": isoformat_utc(utc_now()),
        "redeemable_conditions": len(grouped),
        "redeemable_lots": len(lots),
        "events": events,
    }


if __name__ == "__main__":
    raise SystemExit(main())
