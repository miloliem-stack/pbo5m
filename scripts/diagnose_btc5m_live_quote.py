#!/usr/bin/env python3
"""Diagnose BTC-5m live quote availability for a routed market.

Usage:
    python scripts/diagnose_btc5m_live_quote.py --env-file .env
    python scripts/diagnose_btc5m_live_quote.py --env-file .env --repeat-sec 120 --interval-sec 1
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any, Optional


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.market_quotes import get_quote_snapshot  # noqa: E402
from src.market_router_5m import route_btc_5m_market  # noqa: E402
from src.runtime.env_file import load_env_file  # noqa: E402
from src.time_utils import isoformat_utc, utc_now  # noqa: E402


_REDACT_KEYS = frozenset({"PRIVATE_KEY", "SECRET", "TOKEN", "PASSPHRASE", "PASSWORD", "API_KEY"})


def _safe(value: Any, max_len: int = 120) -> Any:
    if isinstance(value, str) and len(value) > max_len:
        return value[:max_len] + "…"
    return value


def _quote_summary(q: dict[str, Any], token_id: str) -> dict[str, Any]:
    """Extract compact diagnostics from a quote snapshot — no raw book payload."""
    return {
        "token_id": token_id,
        "fetch_ok": q.get("fetch_ok"),
        "http_status": q.get("http_status"),
        "error_kind": q.get("error_kind"),
        "error": _safe(q.get("error")),
        "response_text_sample": _safe(q.get("response_text_sample")),
        "best_bid": q.get("best_bid"),
        "best_ask": q.get("best_ask"),
        "bid_size": q.get("bid_size"),
        "ask_size": q.get("ask_size"),
        "is_empty": q.get("is_empty"),
        "is_crossed": q.get("is_crossed"),
        "age_seconds": q.get("age_seconds"),
        "fetched_at": q.get("fetched_at"),
    }


def _run_once(verbose: bool = False) -> dict[str, Any]:
    now = utc_now()
    out: dict[str, Any] = {"ts": isoformat_utc(now)}

    try:
        routed = route_btc_5m_market()
    except Exception as exc:
        out["route_error"] = str(exc)
        out["route_ok"] = False
        return out

    market = routed.get("market") if isinstance(routed, dict) else None
    if not market:
        out["route_ok"] = False
        out["route_result"] = str(routed)
        return out

    out["route_ok"] = True
    out["market_id"] = market.get("market_id")
    out["slug"] = market.get("slug")
    out["condition_id"] = market.get("condition_id")
    out["detection_source"] = routed.get("detection_source") if isinstance(routed, dict) else None
    out["market_start_ts"] = market.get("start_time") or market.get("market_start_ts")
    out["market_end_ts"] = market.get("end_time") or market.get("market_end_ts")

    yes_token: Optional[str] = market.get("token_yes") or market.get("yes_token_id")
    no_token: Optional[str] = market.get("token_no") or market.get("no_token_id")
    out["yes_token_id"] = yes_token
    out["no_token_id"] = no_token

    if not yes_token or not no_token:
        out["token_error"] = "missing yes or no token"
        return out

    try:
        yes_q = get_quote_snapshot(str(yes_token), force_refresh=True)
    except Exception as exc:
        yes_q = {"fetch_ok": False, "error": str(exc), "error_kind": "exception"}

    try:
        no_q = get_quote_snapshot(str(no_token), force_refresh=True)
    except Exception as exc:
        no_q = {"fetch_ok": False, "error": str(exc), "error_kind": "exception"}

    out["yes"] = _quote_summary(yes_q, str(yes_token))
    out["no"] = _quote_summary(no_q, str(no_token))
    out["valid_topbook"] = bool(
        yes_q.get("fetch_ok") and no_q.get("fetch_ok")
        and yes_q.get("best_ask") is not None and no_q.get("best_ask") is not None
    )
    return out


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Diagnose BTC-5m live quote availability for the routed market.")
    p.add_argument("--env-file", type=Path, help="Path to .env file to load before running.")
    p.add_argument("--repeat-sec", type=float, default=0.0,
                   help="Total duration in seconds to repeat the check (0 = run once).")
    p.add_argument("--interval-sec", type=float, default=1.0,
                   help="Interval in seconds between repeated checks.")
    p.add_argument("--verbose", action="store_true", help="Print full JSON (default: compact single-line).")
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    if args.env_file:
        load_env_file(args.env_file, required=True)

    deadline = time.monotonic() + args.repeat_sec if args.repeat_sec > 0 else None
    first = True
    while True:
        result = _run_once(verbose=args.verbose)
        if args.verbose:
            print(json.dumps(result, indent=2, sort_keys=True))
        else:
            print(json.dumps(result, sort_keys=True))
        sys.stdout.flush()

        if deadline is None or time.monotonic() >= deadline:
            break
        if not first:
            time.sleep(args.interval_sec)
        first = False

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
