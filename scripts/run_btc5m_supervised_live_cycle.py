#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Callable

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.reconcile_btc5m_ledger_from_execution_journal import reconcile
from scripts.run_btc5m_redeemer import run_once as run_redeemer_once
from scripts.update_btc5m_market_resolutions import update_once as update_resolutions_once
from src.runtime.btc5m_live_ledger import LiveLedger
from src.runtime.env_file import load_env_file
from src.runtime.btc5m_resolution_source import UnavailableResolutionSource, build_resolution_source
from src.runtime.polymarket_funder_setup import PolymarketFunderConfig, make_web3, read_erc1155_approval, read_erc20_balance
from src.time_utils import isoformat_utc, utc_now


HARD_STOP_EVENTS = {
    "order_unknown_after_submit",
    "execution_error_after_submit",
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Supervised one-hour BTC-5m Brownian live canary cycle.")
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    parser.add_argument("--max-runtime-sec", type=float, default=3600.0)
    parser.add_argument("--order-cycle-runtime-sec", type=float, default=900.0)
    parser.add_argument("--poll-interval-sec", type=float, default=10.0)
    parser.add_argument("--redeem-interval-sec", type=float, default=30.0)
    parser.add_argument("--resolution-interval-sec", type=float, default=30.0)
    parser.add_argument("--ledger-db", type=Path, default=Path("state/btc5m_live_ledger.db"))
    parser.add_argument("--journal-root", type=Path, default=Path("artifacts/btc5m_canary_execution"))
    parser.add_argument("--max-live-order-attempts", type=int, default=12)
    parser.add_argument("--max-filled-orders", type=int, default=12)
    parser.add_argument("--max-redemptions", type=int, default=12)
    parser.add_argument("--dry-run-redemptions", action="store_true", default=False)
    parser.add_argument("--allow-unavailable-resolution-source", action="store_true", default=False)
    parser.add_argument("--resolution-source", choices=["gamma_ctf", "unavailable"], default=os.environ.get("BTC5M_RESOLUTION_SOURCE", "gamma_ctf"))
    parser.add_argument("--redeemer-min-retry-interval-sec", type=float, default=float(os.environ.get("BTC5M_REDEEMER_MIN_RETRY_INTERVAL_SEC", "3600")))
    parser.add_argument("--redeemer-max-failures", type=int, default=int(os.environ.get("BTC5M_REDEEMER_MAX_FAILURES", "3")))
    parser.add_argument("--yes-i-understand-this-sends-transactions", action="store_true", default=False)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    load_env_file(args.env_file, required=True)
    ledger = LiveLedger(args.ledger_db)
    log_dir = Path("artifacts/btc5m_supervised_live_cycle") / utc_now().strftime("%Y-%m-%d/%H")
    log_dir.mkdir(parents=True, exist_ok=True)
    try:
        validate_supervised_env(os.environ)
        resolution_source = build_supervised_resolution_source(args, os.environ)
        summary = run_supervised_cycle(args=args, ledger=ledger, log_dir=log_dir, resolution_source=resolution_source)
    except Exception as exc:
        summary = base_summary(args, hard_stop_reason=str(exc))
        write_json(log_dir / "summary.json", summary)
        print(json.dumps(summary, indent=2, sort_keys=True, default=str))
        return 1
    write_json(log_dir / "summary.json", summary)
    print(json.dumps(summary, indent=2, sort_keys=True, default=str))
    return 0 if not summary.get("hard_stop_reason") else 1


def validate_supervised_env(env: dict[str, str], *, approval_checker: Callable[[dict[str, str]], bool] | None = None) -> None:
    required = {
        "BTC5M_STRATEGY_ID": "brownian_no_hmm_conservative_v1",
        "BTC5M_BROWNIAN_PAPER_ONLY": "false",
        "BTC5M_BROWNIAN_LIVE_ENABLED": "true",
        "BTC5M_EXECUTION_MODE": "live",
        "BTC5M_LIVE_ONE_SHOT": "true",
    }
    for key, expected in required.items():
        if str(env.get(key, "")).strip().lower() != expected:
            raise RuntimeError(f"unsafe_env:{key}")
    if str(env.get("BTC5M_ALLOW_CONTINUOUS_LIVE", "false")).strip().lower() == "true":
        raise RuntimeError("continuous_live_env_detected")
    if str(env.get("BTC5M_RESOLUTION_FAIL_OPEN", "false")).strip().lower() == "true":
        raise RuntimeError("resolution_fail_open_forbidden_live")
    if str(env.get("BTC5M_RESOLUTION_REQUIRE_ONCHAIN_CONFIRMATION", "true")).strip().lower() == "false":
        raise RuntimeError("resolution_requires_onchain_confirmation_live")
    for key in ["POLYGON_RPC", "POLY_WALLET_PRIVATE_KEY", "BTC5M_EXPECTED_WALLET_ADDRESS"]:
        if not env.get(key):
            raise RuntimeError(f"missing_env:{key}")
    if not (env.get("POLY_API_KEY") and env.get("POLY_API_SECRET") and env.get("POLY_API_PASSPHRASE")):
        raise RuntimeError("missing_clob_l2_credentials")
    if approval_checker is not None and not approval_checker(env):
        raise RuntimeError("missing_ctf_redeem_adapter_approval")
    if approval_checker is None:
        config = PolymarketFunderConfig.from_env(env)
        web3 = make_web3(config)
        funder = config.effective_funder
        if read_erc20_balance(web3, config.pusd_token_address, funder) is None:
            raise RuntimeError("pusd_preflight_unavailable")
        approved = read_erc1155_approval(web3, config.ctf_contract_address, funder, config.ctf_collateral_adapter_address)
        if approved is not True:
            raise RuntimeError("missing_ctf_redeem_adapter_approval")


def build_supervised_resolution_source(args: argparse.Namespace, env: dict[str, str]) -> Any:
    source_name = str(args.resolution_source or env.get("BTC5M_RESOLUTION_SOURCE", "gamma_ctf")).strip().lower()
    if source_name == "unavailable":
        if not args.allow_unavailable_resolution_source:
            raise RuntimeError("resolution_source_unavailable")
        if not args.dry_run_redemptions or args.yes_i_understand_this_sends_transactions:
            raise RuntimeError("unavailable_resolution_source_requires_dry_run_redemptions")
        return UnavailableResolutionSource()
    env["BTC5M_RESOLUTION_SOURCE"] = source_name
    return build_resolution_source(env=env, allow_unavailable=False)


def run_supervised_cycle(
    *,
    args: argparse.Namespace,
    ledger: LiveLedger,
    log_dir: Path,
    order_runner: Callable[[argparse.Namespace], dict[str, Any]] | None = None,
    resolution_source: Any = None,
    redeemer_adapter: Any = None,
) -> dict[str, Any]:
    started = time.monotonic()
    deadline = started + args.max_runtime_sec
    summary = base_summary(args)
    last_resolution = 0.0
    last_redeem = 0.0
    while time.monotonic() <= deadline:
        order_result = (order_runner or run_order_subprocess)(args)
        append_jsonl(log_dir / "supervised_cycle.jsonl", {"step": "order", "result": order_result})
        rec = reconcile(ledger, journal_root=args.journal_root, since_hours=24, dry_run=False)
        append_jsonl(log_dir / "supervised_cycle.jsonl", {"step": "reconcile", "result": rec})
        now = time.monotonic()
        if now - last_resolution >= args.resolution_interval_sec:
            res = update_resolutions_once(ledger, source=resolution_source or UnavailableResolutionSource(), dry_run=False)
            append_jsonl(log_dir / "supervised_cycle.jsonl", {"step": "resolution", "result": res})
            last_resolution = now
        if now - last_redeem >= args.redeem_interval_sec:
            redeem = run_redeemer_once(
                ledger,
                config=PolymarketFunderConfig.from_env(),
                dry_run=args.dry_run_redemptions or not args.yes_i_understand_this_sends_transactions,
                allow_tx=args.yes_i_understand_this_sends_transactions,
                adapter=redeemer_adapter,
                min_retry_interval_sec=args.redeemer_min_retry_interval_sec,
                max_failures=args.redeemer_max_failures,
            )
            append_jsonl(log_dir / "supervised_cycle.jsonl", {"step": "redeemer", "result": redeem})
            last_redeem = now
        summary.update(summarize_ledger_and_journal(ledger, args.journal_root))
        stop = hard_stop_reason(args, summary, order_result)
        if stop:
            summary["hard_stop_reason"] = stop
            break
        time.sleep(max(1.0, args.poll_interval_sec))
    summary["runtime_sec"] = time.monotonic() - started
    return summary


def run_order_subprocess(args: argparse.Namespace) -> dict[str, Any]:
    cmd = [
        sys.executable,
        "scripts/run_btc5m_canary_live.py",
        "--env-file",
        str(args.env_file),
        "--build-live-input",
        "--max-runtime-sec",
        str(args.order_cycle_runtime_sec),
    ]
    proc = subprocess.run(cmd, cwd=ROOT, text=True, capture_output=True, check=False)
    return {"returncode": proc.returncode, "stdout_tail": proc.stdout[-4000:], "stderr_tail": proc.stderr[-4000:]}


def summarize_ledger_and_journal(ledger: LiveLedger, journal_root: Path) -> dict[str, Any]:
    events = list_recent_journal_events(journal_root)
    return {
        "live_order_attempts": sum(1 for e in events if e.get("event_type") == "order_intent_created"),
        "submitted_orders": sum(1 for e in events if e.get("event_type") == "live_order_submitted"),
        "filled_orders": ledger.count_rows("live_fills"),
        "partial_fills": sum(1 for e in events if e.get("event_type") == "order_partially_filled"),
        "rejected_orders": sum(1 for e in events if e.get("event_type") in {"order_rejected", "execution_rejected_by_venue"}),
        "unknown_orders": sum(1 for e in events if e.get("event_type") == "order_unknown_after_submit"),
        "redemption_attempts": ledger.count_rows("redemption_attempts"),
        "redemptions_confirmed": count_confirmed_redemptions(ledger),
        "resolved_wins": count_lots_by_status(ledger, "resolved_win"),
        "resolved_losses": count_lots_by_status(ledger, "resolved_loss"),
    }


def hard_stop_reason(args: argparse.Namespace, summary: dict[str, Any], order_result: dict[str, Any]) -> str | None:
    if order_result.get("returncode", 0) not in {0, None}:
        return "order_cycle_subprocess_failed"
    if summary["live_order_attempts"] > args.max_live_order_attempts:
        return "max_live_order_attempts_reached"
    if summary["filled_orders"] > args.max_filled_orders:
        return "max_filled_orders_reached"
    if summary["redemptions_confirmed"] > args.max_redemptions:
        return "max_redemptions_reached"
    if summary["unknown_orders"]:
        return "unknown_order_state_after_submit"
    return None


def base_summary(args: argparse.Namespace, *, hard_stop_reason: str | None = None) -> dict[str, Any]:
    return {
        "runtime_sec": 0.0,
        "live_order_attempts": 0,
        "submitted_orders": 0,
        "filled_orders": 0,
        "partial_fills": 0,
        "rejected_orders": 0,
        "unknown_orders": 0,
        "resolved_wins": 0,
        "resolved_losses": 0,
        "redemption_attempts": 0,
        "redemptions_confirmed": 0,
        "hard_stop_reason": hard_stop_reason,
        "ledger_db": str(args.ledger_db),
        "journal_root": str(args.journal_root),
        "resolution_source": str(getattr(args, "resolution_source", "gamma_ctf")),
    }


def list_recent_journal_events(journal_root: Path) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    if not journal_root.exists():
        return out
    for path in sorted(journal_root.glob("*/execution_events.jsonl")):
        for line in path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                try:
                    out.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
    return out


def count_lots_by_status(ledger: LiveLedger, status: str) -> int:
    with ledger.connect() as conn:
        return int(conn.execute("SELECT COUNT(*) FROM outcome_lots WHERE status=?", (status,)).fetchone()[0])


def count_confirmed_redemptions(ledger: LiveLedger) -> int:
    with ledger.connect() as conn:
        return int(conn.execute("SELECT COUNT(*) FROM redemption_attempts WHERE status='confirmed'").fetchone()[0])


def append_jsonl(path: Path, row: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    row = {"timestamp": isoformat_utc(utc_now()), **row}
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(row, sort_keys=True, default=str) + "\n")


def write_json(path: Path, row: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(row, indent=2, sort_keys=True, default=str), encoding="utf-8")


if __name__ == "__main__":
    raise SystemExit(main())
