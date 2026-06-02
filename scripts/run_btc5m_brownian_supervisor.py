#!/usr/bin/env python3
"""run_btc5m_brownian_supervisor.py

Entry-point script for the BTC-5m Brownian lifecycle supervisor.

Unlike the one-shot ``run_btc5m_canary_live.py``, this script runs a
long-lived supervisor loop that orchestrates four independent workers:

  - Trading worker       – evaluates the current market and submits orders.
  - Reconciliation worker– polls open CLOB orders until terminal state.
  - Resolution worker    – checks GammaCtf for market resolution.
  - Redemption worker    – redeems resolved winning lots on-chain.

All safety gates are configurable via environment variables (see
:class:`SupervisorConfig` in btc5m_lifecycle_supervisor.py).

Usage
-----
  python scripts/run_btc5m_brownian_supervisor.py --build-live-input

Key environment variables
-------------------------
  BTC5M_EXECUTION_MODE                    observe | live (default: observe)
  BTC5M_BROWNIAN_LIVE_ENABLED             true | false (default: false)
  BTC5M_MAX_RUNTIME_SEC                   supervisor deadline (default: 36000)
  BTC5M_SUPERVISOR_TRADING_TICK_SEC       trading tick interval (default: 5)
  BTC5M_SUPERVISOR_RECONCILIATION_TICK_SEC
  BTC5M_SUPERVISOR_RESOLUTION_TICK_SEC
  BTC5M_SUPERVISOR_REDEMPTION_TICK_SEC
  BTC5M_MAX_UNRESOLVED_MARKETS
  BTC5M_MAX_LIVE_OPEN_ORDERS
  BTC5M_BLOCK_ON_UNKNOWN_ORDER
  BTC5M_BLOCK_ON_RECONCILIATION_STALE
  BTC5M_RECONCILIATION_STALE_SEC
  BTC5M_BLOCK_ON_REDEEMER_HEALTH_FAILURE
  BTC5M_MAX_REDEMPTION_FAILURES
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Callable, Optional

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.runtime.env_file import load_default_env_file, load_env_file, loaded_env_summary
from src.runtime.operator_trace import trace_event, trace_stage_done

load_default_env_file()

from src.runtime.btc5m_brownian_conservative import (
    STRATEGY_ID as BROWNIAN_STRATEGY_ID,
    BrownianConservativeConfig,
    validate_brownian_runtime_env,
)
from src.runtime.btc5m_canary_execution import (
    CanaryExecutor,
    ExecutionConfig,
    ExecutionJournal,
    PyClobClientAdapter,
)
import json as _json
from src.runtime.btc5m_lifecycle_supervisor import (
    SupervisorConfig,
    run_supervisor,
    supervisor_status_summary,
)
from src.runtime.btc5m_live_input_builder import BTC5MCanaryLiveInputBuilder, LiveInputBuilderConfig
from src.runtime.btc5m_live_ledger import LiveLedger
from src.runtime.btc5m_strategy_router import execute_brownian_request_with_canary_route


def _env_bool(key: str, default: bool = False) -> bool:
    raw = os.environ.get(key)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


def _make_event_fn(verbose: bool):
    """Return an event callback that prints JSONL lines to stdout.

    In verbose mode every supervisor event is printed.  In normal mode only
    events with real content (trades, fills, resolutions, redemptions, errors,
    heartbeats) are printed so the console stays readable.
    """
    ALWAYS_PRINT = {
        "trading_tick",
        "reconciliation_tick",
        "resolution_tick",
        "redemption_tick",
        "supervisor_heartbeat",
        "reconciliation_tick_error",
        "resolution_tick_error",
        "redemption_tick_error",
        "trading_tick_error",
    }

    def _fn(event: dict) -> None:
        name = event.get("event", "")
        if verbose or name in ALWAYS_PRINT:
            print(_json.dumps(event, default=str), flush=True)

    return _fn


def brownian_execution_config_from_env() -> ExecutionConfig:
    """Build ExecutionConfig from environment, mirroring the one-shot runner."""
    env = os.environ
    canary_stake_usd = float(env.get("BTC5M_CANARY_STAKE_USD", "1.0"))
    _raw_max_notional = env.get("BTC5M_MAX_NOTIONAL_PER_MARKET_USD")
    max_notional = float(_raw_max_notional) if _raw_max_notional else canary_stake_usd
    _raw_max_daily = env.get("BTC5M_MAX_DAILY_NOTIONAL_USD")
    max_daily = float(_raw_max_daily) if _raw_max_daily else None
    return ExecutionConfig(
        execution_mode=env.get("BTC5M_EXECUTION_MODE", "observe").strip().lower(),
        live_trading_enabled=_env_bool("BTC5M_BROWNIAN_LIVE_ENABLED"),
        live_one_shot=_env_bool("BTC5M_LIVE_ONE_SHOT", default=False),
        max_order_attempts_per_process=int(env.get("BTC5M_MAX_ORDER_ATTEMPTS_PER_PROCESS", "100")),
        canary_stake_usd=canary_stake_usd,
        max_notional_per_market_usd=max_notional,
        max_daily_notional_usd=max_daily,
        max_open_positions=int(env.get("BTC5M_MAX_OPEN_POSITIONS", "1")),
        one_entry_per_market=True,
        expected_wallet_address=env.get("BTC5M_EXPECTED_WALLET_ADDRESS"),
        order_poll_timeout_sec=float(env.get("BTC5M_ORDER_POLL_TIMEOUT_SEC", "20")),
        order_poll_interval_sec=float(env.get("BTC5M_ORDER_POLL_INTERVAL_SEC", "1")),
        max_quote_age_ms=float(
            env.get("BTC5M_MAX_QUOTE_AGE_MS") or env.get("BTC5M_QUOTE_MAX_AGE_MS", "5000")
        ),
        max_price_slippage=float(env.get("BTC5M_BROWNIAN_ASK_SLIPPAGE_ABS", "0.01")),
        max_limit_price=0.99,
        journal_root=Path(env.get("BTC5M_EXECUTION_JOURNAL_ROOT", "artifacts/btc5m_canary_execution")),
        ledger_path=Path(env.get("BTC5M_LIVE_LEDGER_DB", "state/btc5m_live_ledger.db")),
    )


def _build_resolution_source() -> Optional[Any]:
    """Construct GammaCtfResolutionSource if dependencies are available."""
    try:
        from src.runtime.btc5m_resolution_source import build_resolution_source

        return build_resolution_source(env=os.environ)
    except Exception as exc:
        trace_event("resolution_source_init_skipped", reason=str(exc))
        print(json.dumps({"event": "resolution_source_init_skipped", "reason": str(exc)}))
        return None


def _build_redeem_adapter() -> Optional[Any]:
    """Construct PusdCtfRedeemAdapter if dependencies are available."""
    try:
        from src.runtime.btc5m_pusd_redeem_adapter import PusdCtfRedeemAdapter
        from src.runtime.polymarket_funder_setup import PolymarketFunderConfig

        funder_cfg = PolymarketFunderConfig.from_env()
        return PusdCtfRedeemAdapter(funder_config=funder_cfg)
    except Exception as exc:
        trace_event("redeem_adapter_init_skipped", reason=str(exc))
        print(json.dumps({"event": "redeem_adapter_init_skipped", "reason": str(exc)}))
        return None


def build_live_input_builder() -> BTC5MCanaryLiveInputBuilder:
    """Build the live input builder with HMM state requirement disabled
    (Brownian strategy does not require HMM state)."""
    base = LiveInputBuilderConfig.from_env()
    cfg = LiveInputBuilderConfig(
        hmm_state_path=base.hmm_state_path,
        brownian_state_path=base.brownian_state_path,
        max_quote_age_ms=base.max_quote_age_ms,
        max_state_age_sec=base.max_state_age_sec,
        require_hmm_state=False,
    )
    return BTC5MCanaryLiveInputBuilder(cfg)


def run(args: argparse.Namespace) -> dict[str, Any]:
    run_started = time.monotonic()

    trace_event(
        "supervisor_run_start",
        max_runtime_sec=float(args.max_runtime_sec),
        build_live_input=bool(args.build_live_input),
        execution_mode=os.environ.get("BTC5M_EXECUTION_MODE", "observe"),
    )

    # ── Strategy config ───────────────────────────────────────────────────────
    cfg_start = time.monotonic()
    brownian_cfg = BrownianConservativeConfig.from_env()
    trace_stage_done(
        "brownian_config_loaded",
        stage="brownian_config_load",
        started_mono=cfg_start,
        strategy_id=brownian_cfg.strategy_id,
        paper_only=brownian_cfg.paper_only,
        live_enabled=brownian_cfg.live_enabled,
    )

    env_errors = validate_brownian_runtime_env()
    if env_errors:
        raise RuntimeError("BTC5M Brownian runtime env refused: " + ", ".join(env_errors))

    supervisor_cfg = SupervisorConfig.from_env()
    trace_event(
        "supervisor_config_loaded",
        max_unresolved_markets=supervisor_cfg.max_unresolved_markets,
        max_live_open_orders=supervisor_cfg.max_live_open_orders,
        block_on_unknown_order=supervisor_cfg.block_on_unknown_order,
        block_on_reconciliation_stale=supervisor_cfg.block_on_reconciliation_stale,
        reconciliation_stale_sec=supervisor_cfg.reconciliation_stale_sec,
        block_on_redeemer_health_failure=supervisor_cfg.block_on_redeemer_health_failure,
        max_redemption_failures=supervisor_cfg.max_redemption_failures,
    )

    if not args.build_live_input:
        return {
            "status": "observe_no_decision",
            "strategy_id": BROWNIAN_STRATEGY_ID,
            "note": "Supervisor requires --build-live-input to run autonomously.",
        }

    # ── Live input builder ────────────────────────────────────────────────────
    builder_start = time.monotonic()
    live_input_builder = build_live_input_builder()
    trace_stage_done(
        "live_input_builder_init_done",
        stage="live_input_builder_init",
        started_mono=builder_start,
    )

    # ── Executor and adapter (only in live mode) ──────────────────────────────
    executor: Optional[CanaryExecutor] = None
    execution_callback: Optional[Callable[[dict[str, Any]], dict[str, Any]]] = None
    get_order_status_fn: Optional[Callable[[str], dict[str, Any]]] = None
    ledger_path = Path(os.environ.get("BTC5M_LIVE_LEDGER_DB", "state/btc5m_live_ledger.db"))
    ledger = LiveLedger(ledger_path)

    if not brownian_cfg.paper_only and brownian_cfg.live_enabled:
        exec_start = time.monotonic()
        exec_config = brownian_execution_config_from_env()
        trace_event(
            "exec_config_loaded",
            execution_mode=exec_config.execution_mode,
            live_trading_enabled=exec_config.live_trading_enabled,
            canary_stake_usd=exec_config.canary_stake_usd,
        )

        adapter = PyClobClientAdapter() if exec_config.execution_mode == "live" else None
        journal = ExecutionJournal(exec_config.journal_root)
        journal.ensure_writable()

        executor = CanaryExecutor(exec_config, adapter, journal, ledger=ledger)
        executor.startup_check()

        if adapter is not None:
            get_order_status_fn = adapter.get_order_status  # type: ignore[assignment]

        def _execution_callback(request: dict[str, Any]) -> dict[str, Any]:
            return execute_brownian_request_with_canary_route(executor, request)  # type: ignore[arg-type]

        execution_callback = _execution_callback
        trace_stage_done(
            "executor_init_done",
            stage="executor_init",
            started_mono=exec_start,
            execution_mode=exec_config.execution_mode,
        )

    # ── Resolution source ─────────────────────────────────────────────────────
    resolution_source = _build_resolution_source()
    trace_event("resolution_source_ready", present=resolution_source is not None)

    # ── Redeem adapter ────────────────────────────────────────────────────────
    redeem_adapter = _build_redeem_adapter()
    trace_event("redeem_adapter_ready", present=redeem_adapter is not None)

    # ── Initial ledger status ─────────────────────────────────────────────────
    try:
        trace_event("initial_ledger_status", **supervisor_status_summary(ledger))
    except Exception:
        pass

    # ── Run supervisor ────────────────────────────────────────────────────────
    result = run_supervisor(
        ledger=ledger,
        supervisor_config=supervisor_cfg,
        brownian_config=brownian_cfg,
        execution_callback=execution_callback,
        live_input_builder=live_input_builder,
        executor=executor,
        resolution_source=resolution_source,
        redeem_adapter=redeem_adapter,
        get_order_status_fn=get_order_status_fn,
        max_runtime_sec=float(args.max_runtime_sec),
        event_fn=_make_event_fn(verbose=getattr(args, "verbose", False)),
    )

    trace_stage_done(
        "supervisor_run_done",
        stage="supervisor_run",
        started_mono=run_started,
        status=result.get("status"),
        iterations=result.get("iterations"),
    )
    return result


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="BTC-5m Brownian lifecycle supervisor – long-lived multi-market loop."
    )
    parser.add_argument(
        "--env-file",
        type=Path,
        help="Load a KEY=VALUE env profile before reading runtime settings.",
    )
    parser.add_argument(
        "--build-live-input",
        action="store_true",
        help="Build live inputs autonomously (required for trading/resolution/redemption workers).",
    )
    parser.add_argument(
        "--max-runtime-sec",
        type=float,
        default=float(os.environ.get("BTC5M_MAX_RUNTIME_SEC", "36000")),
        help="Hard deadline for the supervisor loop in seconds (default: 36000).",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        default=_env_bool("BTC5M_SUPERVISOR_VERBOSE"),
        help="Print every internal supervisor event, not just key decisions.",
    )
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    try:
        args = build_parser().parse_args(argv)
        if args.env_file is not None:
            loaded = load_env_file(args.env_file, override=True, required=True)
            print(
                json.dumps(
                    {
                        "env_file": str(args.env_file),
                        "loaded_keys": sorted(loaded),
                        "loaded": loaded_env_summary(loaded),
                    },
                    sort_keys=True,
                )
            )
        result = run(args)
    except KeyboardInterrupt:
        print(
            json.dumps(
                {"status": "interrupted", "strategy_id": BROWNIAN_STRATEGY_ID},
                indent=2,
                sort_keys=True,
            )
        )
        return 130
    except Exception as exc:
        print(f"btc5m brownian supervisor failed: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(result, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
