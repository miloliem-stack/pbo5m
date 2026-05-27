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
from src.runtime.btc5m_pusd_redeem_adapter import PusdCtfRedeemAdapter
from src.runtime.env_file import load_env_file
from src.runtime.polymarket_funder_setup import PolymarketFunderConfig
from src.time_utils import isoformat_utc, parse_datetime, utc_now


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Independent BTC-5m Polymarket outcome-token redeemer.")
    parser.add_argument("--env-file", type=Path)
    parser.add_argument("--ledger-db", type=Path, default=Path(os.environ.get("BTC5M_LIVE_LEDGER_DB", "state/btc5m_live_ledger.db")))
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--interval-sec", type=float, default=60.0)
    parser.add_argument("--max-runtime-sec", type=float, default=0.0)
    parser.add_argument("--dry-run", action="store_true", default=False)
    parser.add_argument("--min-retry-interval-sec", type=float, default=3600.0)
    parser.add_argument("--max-failures", type=int, default=3)
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
        result = run_once(
            ledger,
            config=config,
            dry_run=args.dry_run,
            allow_tx=args.yes_i_understand_this_sends_transactions,
            min_retry_interval_sec=args.min_retry_interval_sec,
            max_failures=args.max_failures,
        )
        print(json.dumps(result, indent=2, sort_keys=True, default=str))
        if args.once:
            return 0 if result.get("ok") else 1
        if deadline is not None and time.monotonic() >= deadline:
            return 0 if result.get("ok") else 1
        time.sleep(max(1.0, args.interval_sec))


def run_once(
    ledger: LiveLedger,
    *,
    config: PolymarketFunderConfig,
    dry_run: bool,
    allow_tx: bool,
    adapter: PusdCtfRedeemAdapter | None = None,
    min_retry_interval_sec: float = 3600.0,
    max_failures: int = 3,
) -> dict[str, Any]:
    ledger.terminalize_resolved_lots()
    lots = ledger.redeemable_lots()
    grouped: dict[str, list[dict[str, Any]]] = {}
    for lot in lots:
        grouped.setdefault(str(lot["condition_id"]), []).append(lot)
    events: list[dict[str, Any]] = []
    for condition_id, condition_lots in grouped.items():
        token_ids = sorted({str(lot["token_id"]) for lot in condition_lots if lot.get("token_id")})
        market_id = condition_lots[0].get("market_id")
        if not condition_id or condition_id == "None":
            events.append({"condition_id": condition_id, "status": "skipped", "error_code": "condition_id_missing"})
            continue
        if ledger.has_successful_redemption(condition_id):
            events.append({"condition_id": condition_id, "status": "skipped", "error_code": "redemption_already_submitted_or_confirmed"})
            continue
        failures = ledger.recent_redemption_failures(condition_id, limit=max_failures)
        if len(failures) >= max_failures:
            events.append({"condition_id": condition_id, "status": "skipped", "error_code": "redemption_manual_review_max_failures"})
            continue
        if failures and not retry_cooldown_elapsed(failures[0].get("created_ts"), min_retry_interval_sec):
            events.append({"condition_id": condition_id, "status": "skipped", "error_code": "redemption_backoff_active"})
            continue
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
            status="submitted",
        )
        try:
            adapter = adapter or PusdCtfRedeemAdapter(funder_config=config)
            response = adapter.redeem_condition(condition_id=condition_id, token_ids=token_ids, index_sets=[1, 2])
            tx_hash = response.get("tx_hash")
            if response.get("status") == "confirmed":
                ledger.update_redemption_attempt(attempt_id, status="confirmed", tx_hash=tx_hash, confirmed=True)
                ledger.mark_lots_redeemed(
                    condition_id=condition_id,
                    tx_hash=str(tx_hash),
                    redeemed_pusd_amount=response.get("redeemed_pusd_delta"),
                    receipt=response.get("receipt"),
                )
            else:
                ledger.update_redemption_attempt(
                    attempt_id,
                    status=response.get("status") or "failed_retryable",
                    tx_hash=tx_hash,
                    error_code=response.get("error_code"),
                    raw_error=json.dumps(response, sort_keys=True, default=str),
                )
            events.append({"condition_id": condition_id, "attempt_id": attempt_id, **response})
        except Exception as exc:
            code = normalize_redeem_error(exc)
            status = "failed_terminal" if code in {"adapter_abi_unverified", "condition_id_missing", "condition_id_invalid", "zero_token_balance"} else "failed_retryable"
            ledger.update_redemption_attempt(attempt_id, status=status, error_code=code, raw_error=str(exc))
            events.append({"condition_id": condition_id, "attempt_id": attempt_id, "status": status, "error_code": code, "raw_error": str(exc)})
    return {
        "ok": True,
        "dry_run": dry_run,
        "checked_ts": isoformat_utc(utc_now()),
        "redeemable_conditions": len(grouped),
        "redeemable_lots": len(lots),
        "events": events,
    }


def retry_cooldown_elapsed(created_ts: Any, min_retry_interval_sec: float) -> bool:
    created = parse_datetime(created_ts)
    if created is None:
        return True
    return (utc_now() - created).total_seconds() >= float(min_retry_interval_sec)


def normalize_redeem_error(exc: BaseException) -> str:
    text = str(exc)
    lowered = text.lower()
    if "adapter_abi_unverified" in lowered:
        return "adapter_abi_unverified"
    if "condition_id_missing" in lowered:
        return "condition_id_missing"
    if "condition_id_invalid" in lowered:
        return "condition_id_invalid"
    if "zero_token_balance" in lowered:
        return "zero_token_balance"
    if "missing_ctf_redeem_adapter_approval" in lowered or "need operator approval" in lowered:
        return "missing_ctf_redeem_adapter_approval"
    if "receipt" in lowered or "timeout" in lowered:
        return "redeem_receipt_timeout"
    return "redeem_tx_failed"


if __name__ == "__main__":
    raise SystemExit(main())
