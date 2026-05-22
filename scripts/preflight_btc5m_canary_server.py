#!/usr/bin/env python3
from __future__ import annotations

import argparse
import importlib.util
import json
import os
import sys
import tempfile
from pathlib import Path
from typing import Any, Callable, Optional

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.binance_price_feed import rest_binance_price_row
from src.market_quotes import get_quote_snapshot
from src.market_router_5m import route_btc_5m_market
from src.runtime.btc5m_live_input_builder import LiveInputBuilderConfig
from scripts.run_btc5m_canary_live import validate_brownian_state_file, validate_hmm_state_file


def check_writable(path: Path) -> tuple[bool, str]:
    try:
        path.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile("w", dir=path, delete=True, encoding="utf-8") as handle:
            handle.write("ok")
            handle.flush()
        return True, "ok"
    except Exception as exc:
        return False, str(exc)


def check_import(module: str) -> tuple[bool, str]:
    return (importlib.util.find_spec(module) is not None, "ok" if importlib.util.find_spec(module) is not None else "missing")


def run_checks(
    *,
    live: bool,
    route_fn: Callable[[], dict[str, Any]] = route_btc_5m_market,
    quote_fn: Callable[[str], dict[str, Any]] = get_quote_snapshot,
    price_fn: Callable[[], dict[str, Any]] = rest_binance_price_row,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []

    def add(name: str, ok: bool, detail: str = "ok", live_blocking: bool = True) -> None:
        rows.append({"check": name, "status": "PASS" if ok else "FAIL", "detail": detail, "live_blocking": live_blocking})

    ok, detail = check_import("py_clob_client")
    add("python_dep_py_clob_client", ok, detail, live_blocking=live)
    add("polymarket_private_key_present", bool(os.getenv("POLY_WALLET_PRIVATE_KEY")), "POLY_WALLET_PRIVATE_KEY", live_blocking=live)
    if os.getenv("BTC5M_EXPECTED_WALLET_ADDRESS"):
        add("expected_wallet_configured", True, os.getenv("BTC5M_EXPECTED_WALLET_ADDRESS", ""), live_blocking=False)
    else:
        add("expected_wallet_configured", False, "BTC5M_EXPECTED_WALLET_ADDRESS missing", live_blocking=False)

    for name, path in [
        ("live_state_dir_writable", Path(os.getenv("BTC5M_LIVE_STATE_DIR", "artifacts/live_state"))),
        ("execution_journal_writable", Path(os.getenv("BTC5M_EXECUTION_JOURNAL_ROOT", "artifacts/btc5m_canary_execution"))),
    ]:
        ok, detail = check_writable(path)
        add(name, ok, detail)

    hmm_artifact_dir = os.getenv("BTC5M_HMM_ARTIFACT_DIR")
    add("hmm_artifact_dir_present", bool(hmm_artifact_dir and Path(hmm_artifact_dir).exists()), hmm_artifact_dir or "BTC5M_HMM_ARTIFACT_DIR missing")

    cfg = LiveInputBuilderConfig.from_env()
    if cfg.hmm_state_path and cfg.hmm_state_path.exists():
        errors = validate_hmm_state_file(cfg.hmm_state_path, cfg.max_state_age_sec)
        add("hmm_live_state_fresh_valid", not errors, ",".join(errors) if errors else "ok")
    else:
        add("hmm_live_state_fresh_valid", False, str(cfg.hmm_state_path or "BTC5M_LIVE_HMM_STATE_PATH missing"))
    if cfg.brownian_state_path and cfg.brownian_state_path.exists():
        errors = validate_brownian_state_file(cfg.brownian_state_path, cfg.max_state_age_sec)
        add("brownian_live_state_fresh_valid", not errors, ",".join(errors) if errors else "ok")
    else:
        add("brownian_live_state_fresh_valid", False, str(cfg.brownian_state_path or "BTC5M_LIVE_BROWNIAN_STATE_PATH missing"))

    try:
        price = price_fn()
        add("binance_price_access", price.get("price") is not None, json.dumps({"price": price.get("price"), "ts": price.get("ts")}))
    except Exception as exc:
        add("binance_price_access", False, str(exc))

    try:
        routed = route_fn()
        market = routed.get("market") if isinstance(routed, dict) else None
        add("active_market_discovery", bool(market), json.dumps({"market_id": (market or {}).get("market_id"), "condition_id": (market or {}).get("condition_id")}))
        token = (market or {}).get("token_yes") or (market or {}).get("yes_token_id")
        if token:
            quote = quote_fn(str(token))
            add("clob_quote_fetch", bool(quote.get("fetch_ok") and quote.get("best_ask") is not None), json.dumps({"best_ask": quote.get("best_ask"), "error": quote.get("error")}))
        else:
            add("clob_quote_fetch", False, "no YES token from active market")
    except Exception as exc:
        add("active_market_discovery", False, str(exc))
        add("clob_quote_fetch", False, "skipped")

    return rows


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Preflight BTC-5M canary server deployment.")
    parser.add_argument("--live", action="store_true", help="Treat live-blocking failures as fatal.")
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    rows = run_checks(live=args.live)
    fatal = [row for row in rows if row["status"] != "PASS" and (args.live and row["live_blocking"])]
    for row in rows:
        print(f"{row['status']} {row['check']}: {row['detail']}")
    return 1 if fatal else 0


if __name__ == "__main__":
    raise SystemExit(main())
