#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import timedelta
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.runtime.btc5m_live_ledger import LiveLedger
from src.time_utils import parse_datetime, utc_now


ORDER_EVENTS = {
    "order_intent_created",
    "live_order_submitted",
    "order_filled",
    "order_partially_filled",
    "order_rejected",
    "order_cancelled",
    "order_unknown_after_submit",
    "execution_rejected_by_venue",
    "execution_error_after_submit",
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Reconcile BTC-5m live ledger from execution JSONL journal.")
    parser.add_argument("--ledger-db", type=Path, default=Path("state/btc5m_live_ledger.db"))
    parser.add_argument("--journal-root", type=Path, default=Path("artifacts/btc5m_canary_execution"))
    parser.add_argument("--since-hours", type=float)
    parser.add_argument("--dry-run", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    ledger = LiveLedger(args.ledger_db)
    summary = reconcile(ledger, journal_root=args.journal_root, since_hours=args.since_hours, dry_run=args.dry_run)
    print(json.dumps(summary, indent=2, sort_keys=True, default=str))
    return 0 if not summary["warnings"] else 1


def reconcile(ledger: LiveLedger, *, journal_root: Path, since_hours: float | None = None, dry_run: bool = False) -> dict[str, Any]:
    cutoff = utc_now() - timedelta(hours=since_hours) if since_hours else None
    before_fills = ledger.count_rows("live_fills")
    before_lots = ledger.count_rows("outcome_lots")
    summary = {
        "orders_seen": 0,
        "orders_inserted_or_updated": 0,
        "fills_seen": 0,
        "fills_inserted": 0,
        "lots_created": 0,
        "skipped_duplicates": 0,
        "warnings": [],
        "dry_run": dry_run,
    }
    for event in iter_events(journal_root):
        event_type = event.get("event_type")
        if event_type not in ORDER_EVENTS:
            continue
        ts = parse_datetime(event.get("execution_ts"))
        if cutoff is not None and ts is not None and ts < cutoff:
            continue
        if event_type in {"order_intent_created", "live_order_submitted", "order_rejected", "order_cancelled", "order_unknown_after_submit", "execution_rejected_by_venue", "execution_error_after_submit"}:
            summary["orders_seen"] += 1
            if not dry_run:
                if event_type == "order_intent_created":
                    ledger.record_order_intent(event, raw_response=event)
                elif event_type == "live_order_submitted":
                    ledger.record_order_intent(event, raw_response=event)
                    ledger.record_order_submission(event, order_id=event.get("order_id"), response=event.get("raw_response") or event)
                else:
                    ledger.record_order_event(event)
                summary["orders_inserted_or_updated"] += 1
        if event_type in {"order_filled", "order_partially_filled"}:
            summary["fills_seen"] += 1
            if not dry_run:
                inserted = ledger.record_fill_from_event(event)
                if inserted:
                    summary["fills_inserted"] += 1
                else:
                    summary["skipped_duplicates"] += 1
    if not dry_run:
        summary["fills_inserted"] = max(summary["fills_inserted"], ledger.count_rows("live_fills") - before_fills)
        summary["lots_created"] = max(0, ledger.count_rows("outcome_lots") - before_lots)
    return summary


def iter_events(journal_root: Path):
    if not journal_root.exists():
        return
    for path in sorted(journal_root.glob("*/execution_events.jsonl")):
        for line_no, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
            if not line.strip():
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                yield {"event_type": "invalid_json", "path": str(path), "line_no": line_no}


if __name__ == "__main__":
    raise SystemExit(main())
