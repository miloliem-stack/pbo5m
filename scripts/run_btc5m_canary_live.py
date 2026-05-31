#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.runtime.env_file import loaded_env_summary, load_default_env_file, load_env_file
from src.runtime.operator_trace import trace_event, trace_stage_done

load_default_env_file()

from src.runtime.btc5m_canary_execution import (
    CanaryExecutor,
    ExecutionConfig,
    ExecutionJournal,
    PyClobClientAdapter,
    add_decision_provenance,
)
from src.runtime.btc5m_canary_policy import evaluate_canary_policy
from src.runtime.btc5m_brownian_conservative import STRATEGY_ID as BROWNIAN_STRATEGY_ID
from src.runtime.btc5m_brownian_conservative import BrownianConservativeConfig, validate_brownian_runtime_env
from src.runtime.btc5m_live_input_builder import BTC5MCanaryLiveInputBuilder, LiveInputBuilderConfig
from src.runtime.btc5m_live_ledger import LiveLedger
from src.runtime.btc5m_strategy_router import execute_brownian_request_with_canary_route, run_btc5m_strategy_cycle, selected_strategy_id
from src.time_utils import utc_now
from scripts.run_btc5m_live_state_producer import load_json as load_state_json


def load_decision(path: Path) -> dict:
    if not path.exists():
        raise FileNotFoundError(f"decision JSON does not exist: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def load_decision_input(path: Path) -> dict:
    if not path.exists():
        raise FileNotFoundError(f"decision input JSON does not exist: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def build_policy_decision(input_payload: dict, config: ExecutionConfig, *, now: Optional[datetime] = None) -> dict:
    generated = now or datetime.now(timezone.utc)
    decision = evaluate_canary_policy(
        market=input_payload["market"],
        quote=input_payload["quote"],
        predictions=input_payload["predictions"],
        hmm_state=input_payload.get("hmm_state"),
        risk_state=input_payload.get("risk_state") or {"open_positions": 0, "daily_loss_usd": 0.0},
        config=config.policy_config,
        decision_ts=input_payload.get("decision_ts") or generated,
    )
    return add_decision_provenance(
        decision,
        policy_config=config.policy_config,
        input_payload=input_payload,
        now=generated,
        expiry_ms=config.decision_expiry_ms,
    )


def write_decision(path: Optional[Path], decision: dict) -> None:
    if path is None:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.suffix.lower() in {".jsonl", ".ndjson"}:
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(decision, sort_keys=True, default=str) + "\n")
    else:
        path.write_text(json.dumps(decision, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")


class LiveStateLogger:
    def __init__(self, root: Path, *, now_fn=utc_now) -> None:
        self.root = root
        self.now_fn = now_fn

    @property
    def dir(self) -> Path:
        now = self.now_fn()
        return self.root / now.strftime("%Y-%m-%d") / now.strftime("%H")

    def write_live_input(self, row: dict) -> None:
        self._write("live_input_state.jsonl", row)

    def write_decision(self, row: dict) -> None:
        self._write("decision_state.jsonl", row)

    def _write(self, filename: str, row: dict) -> None:
        target = self.dir / filename
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(row, sort_keys=True, default=str) + "\n")


def run(args: argparse.Namespace) -> dict:
    trace_event(
        "run_start",
        max_runtime_sec=float(args.max_runtime_sec),
        poll_interval_sec=float(args.poll_interval_sec),
        build_live_input=bool(args.build_live_input),
    )
    strategy_id = selected_strategy_id()
    trace_event("selected_strategy_id", strategy_id=strategy_id)
    if strategy_id == BROWNIAN_STRATEGY_ID:
        return run_brownian_strategy(args)

    config = ExecutionConfig.from_env()
    adapter = PyClobClientAdapter() if config.execution_mode == "live" else None
    journal = ExecutionJournal(config.journal_root)
    executor = CanaryExecutor(config, adapter, journal)
    startup = executor.startup_check()
    live_logger = LiveStateLogger(args.live_log_root)
    builder = BTC5MCanaryLiveInputBuilder(LiveInputBuilderConfig.from_env()) if args.build_live_input else None
    if args.build_live_input and config.execution_mode == "live":
        builder_startup_errors = live_builder_startup_errors(builder)
        if builder_startup_errors:
            raise RuntimeError("BTC5M live input builder startup refused: " + ", ".join(builder_startup_errors))
    if args.decision_json is None and args.decision_input_json is None and not args.build_live_input:
        return {
            "status": "observe_no_decision",
            "startup": startup,
            "note": "Pass --build-live-input for autonomous live input generation, --decision-input-json for debug generation, or --decision-json for a pre-generated provenance-stamped decision.",
        }
    deadline = time.monotonic() + float(args.max_runtime_sec)
    result = {"status": "no_decision_before_timeout"}
    while time.monotonic() <= deadline:
        if args.build_live_input:
            built = builder.build()
            live_logger.write_live_input(built)
            if not built.get("ok"):
                result = {
                    "status": "live_input_missing",
                    "missing_input_reason": built.get("missing_input_reason"),
                    "missing_components": built.get("missing_components") or [],
                }
                if not sleep_until_deadline(deadline, args.poll_interval_sec):
                    break
                continue
            decision = build_policy_decision(built["input"], config)
            live_logger.write_decision(decision)
            write_decision(args.decision_output_json, decision)
        elif args.decision_input_json is not None:
            decision = build_policy_decision(load_decision_input(Path(args.decision_input_json)), config)
            write_decision(args.decision_output_json, decision)
        else:
            decision = load_decision(Path(args.decision_json))
        result = executor.execute_decision(decision)
        attempted_live_order = result.get("event_type") in {
            "live_order_submitted",
            "order_status_polled",
            "order_filled",
            "order_partially_filled",
            "order_rejected",
            "order_cancelled",
            "order_unknown_after_submit",
            "execution_error_after_submit",
            "live_one_shot_exit",
        }
        eligible_decision = decision.get("final_decision") in {"BUY_YES", "BUY_NO", "SHADOW_ONLY"}
        if config.live_one_shot and attempted_live_order:
            break
        if args.stop_after_first_eligible_decision and eligible_decision:
            break
        if not sleep_until_deadline(deadline, args.poll_interval_sec):
            break
    return result


def run_brownian_strategy(args: argparse.Namespace) -> dict:
    run_started_mono = time.monotonic()
    last_stage = "start"
    iterations = 0
    deadline_reached = False
    live_execution_invoked = False

    def _set_stage(stage: str) -> None:
        nonlocal last_stage
        last_stage = stage

    _set_stage("brownian_config_load")
    cfg_start = time.monotonic()
    cfg = BrownianConservativeConfig.from_env()
    trace_stage_done(
        "brownian_config_loaded",
        stage="brownian_config_load",
        started_mono=cfg_start,
        strategy_id=cfg.strategy_id,
        paper_only=cfg.paper_only,
        live_enabled=cfg.live_enabled,
        min_order_notional=cfg.min_order_notional,
        max_stake_fraction=cfg.normal_max_stake_fraction,
    )

    _set_stage("brownian_env_validation")
    trace_event("brownian_env_validation_start")
    env_start = time.monotonic()
    env_errors = validate_brownian_runtime_env()
    if env_errors:
        trace_stage_done(
            "brownian_env_validation_refused",
            stage="brownian_env_validation",
            started_mono=env_start,
            errors=env_errors,
        )
        raise RuntimeError("BTC5M Brownian runtime env refused: " + ", ".join(env_errors))
    trace_stage_done("brownian_env_validation_ok", stage="brownian_env_validation", started_mono=env_start)

    if not args.build_live_input:
        result = {
            "status": "observe_no_decision",
            "strategy_id": BROWNIAN_STRATEGY_ID,
            "note": "Brownian server mode requires --build-live-input so current quote, price, volatility, bankroll, and market-age gates are rebuilt.",
        }
        trace_event(
            "runner_exit",
            final_status=result.get("status"),
            reason=result.get("reason") or result.get("missing_input_reason") or result.get("reject_reason"),
            last_stage=last_stage,
            iterations=iterations,
            runtime_elapsed_sec=round(max(0.0, time.monotonic() - run_started_mono), 6),
            deadline_reached=deadline_reached,
            live_execution_callback_invoked=live_execution_invoked,
        )
        return result

    _set_stage("live_input_builder_init")
    trace_event("live_input_builder_init_start")
    builder_init_start = time.monotonic()
    live_logger = LiveStateLogger(args.live_log_root)
    builder_cfg = LiveInputBuilderConfig.from_env()
    builder_cfg = LiveInputBuilderConfig(
        hmm_state_path=builder_cfg.hmm_state_path,
        brownian_state_path=builder_cfg.brownian_state_path,
        max_quote_age_ms=builder_cfg.max_quote_age_ms,
        max_state_age_sec=builder_cfg.max_state_age_sec,
        require_hmm_state=False,
    )
    builder = BTC5MCanaryLiveInputBuilder(builder_cfg)
    trace_stage_done(
        "live_input_builder_init_done",
        stage="live_input_builder_init",
        started_mono=builder_init_start,
        max_quote_age_ms=builder_cfg.max_quote_age_ms,
        max_state_age_sec=builder_cfg.max_state_age_sec,
        brownian_state_path=str(builder_cfg.brownian_state_path) if builder_cfg.brownian_state_path is not None else None,
        hmm_state_path=str(builder_cfg.hmm_state_path) if builder_cfg.hmm_state_path is not None else None,
    )

    executor = None
    execution_callback = None
    if not cfg.paper_only and cfg.live_enabled:
        _set_stage("executor_init")
        trace_event("executor_init_start")
        executor_init_start = time.monotonic()
        exec_config = brownian_execution_config_from_env()
        trace_event(
            "brownian_exec_config_loaded",
            canary_stake_usd=exec_config.canary_stake_usd,
            max_notional_per_market_usd=exec_config.max_notional_per_market_usd,
            max_daily_notional_usd=exec_config.max_daily_notional_usd,
            execution_mode=exec_config.execution_mode,
        )
        trace_event("pyclob_adapter_init_start")
        adapter_start = time.monotonic()
        adapter = PyClobClientAdapter() if exec_config.execution_mode == "live" else None
        trace_stage_done(
            "pyclob_adapter_init_done",
            stage="pyclob_adapter_init",
            started_mono=adapter_start,
            execution_mode=exec_config.execution_mode,
            adapter_present=adapter is not None,
        )
        journal = ExecutionJournal(exec_config.journal_root)
        trace_event("execution_journal_writable_start", journal_root=str(exec_config.journal_root))
        journal_start = time.monotonic()
        journal.ensure_writable()
        trace_stage_done("execution_journal_writable_done", stage="execution_journal_writable", started_mono=journal_start)
        trace_event("ledger_init_start", ledger_path=str(exec_config.ledger_path))
        ledger_start = time.monotonic()
        ledger = LiveLedger(exec_config.ledger_path)
        trace_stage_done("ledger_init_done", stage="ledger_init", started_mono=ledger_start)
        executor = CanaryExecutor(exec_config, adapter, journal, ledger=ledger)
        executor.startup_check()

        def _execution_callback(request: dict[str, Any]) -> dict[str, Any]:
            nonlocal live_execution_invoked
            live_execution_invoked = True
            trace_event(
                "live_execution_callback_start",
                market_id=request.get("market_id"),
                market_slug=request.get("market_slug"),
                side=request.get("side"),
                notional_usd=request.get("notional_usd"),
            )
            cb_start = time.monotonic()
            try:
                return execute_brownian_request_with_canary_route(executor, request)
            finally:
                trace_stage_done("live_execution_callback_done", stage="live_execution_callback", started_mono=cb_start)

        execution_callback = _execution_callback
        trace_stage_done(
            "executor_init_done",
            stage="executor_init",
            started_mono=executor_init_start,
            execution_mode=exec_config.execution_mode,
            live_trading_enabled=exec_config.live_trading_enabled,
            max_order_attempts_per_process=exec_config.max_order_attempts_per_process,
            max_quote_age_ms=exec_config.max_quote_age_ms,
            journal_root=str(exec_config.journal_root),
            ledger_path=str(exec_config.ledger_path),
        )

    deadline = time.monotonic() + float(args.max_runtime_sec)
    trace_event(
        "loop_start",
        deadline_mono=round(deadline, 6),
        max_runtime_sec=float(args.max_runtime_sec),
        poll_interval_sec=float(args.poll_interval_sec),
    )
    result: dict[str, Any] = {"status": "no_decision_before_timeout", "strategy_id": BROWNIAN_STRATEGY_ID}
    while time.monotonic() <= deadline:
        iterations += 1
        _set_stage("loop_iteration")
        trace_event(
            "loop_iteration_start",
            iteration=iterations,
            remaining_sec=round(max(0.0, deadline - time.monotonic()), 6),
        )

        _set_stage("live_input_build")
        trace_event("live_input_build_start", iteration=iterations)
        build_start = time.monotonic()
        built = builder.build()
        trace_stage_done(
            "live_input_build_done",
            stage="live_input_build",
            started_mono=build_start,
            iteration=iterations,
            ok=bool(built.get("ok")),
            missing_input_reason=built.get("missing_input_reason"),
            missing_components=built.get("missing_components") or [],
            brownian_source=((built.get("input") or {}).get("live_input_meta") or {}).get("brownian_source"),
            brownian_error=((built.get("input") or {}).get("live_input_meta") or {}).get("brownian_error"),
        )
        live_logger.write_live_input(built)
        if not built.get("ok"):
            meta = (built.get("input") or {}).get("live_input_meta") or {}
            result = {
                "status": "live_input_missing",
                "strategy_id": BROWNIAN_STRATEGY_ID,
                "missing_input_reason": built.get("missing_input_reason"),
                "missing_components": built.get("missing_components") or [],
                "brownian_error": meta.get("brownian_error"),
                "brownian_source": meta.get("brownian_source"),
                "hmm_error": meta.get("hmm_error"),
            }
            _set_stage("sleep")
            trace_event("sleep_start", reason="live_input_missing", poll_interval_sec=float(args.poll_interval_sec))
            sleep_start = time.monotonic()
            if not sleep_until_deadline(deadline, args.poll_interval_sec):
                deadline_reached = True
                trace_stage_done("sleep_done", stage="sleep", started_mono=sleep_start, deadline_reached=True)
                break
            trace_stage_done("sleep_done", stage="sleep", started_mono=sleep_start, deadline_reached=False)
            continue
        if executor is not None:
            _set_stage("capital_risk_state")
            trace_event("capital_risk_state_start")
            capital_start = time.monotonic()
            capital_error = apply_live_capital_risk_state(built["input"], executor, cfg)
            capital_meta = (built.get("input") or {}).get("live_input_meta", {}).get("capital_state") or {}
            trace_stage_done(
                "capital_risk_state_done",
                stage="capital_risk_state",
                started_mono=capital_start,
                capital_error=capital_error,
                pusd_balance=capital_meta.get("pusd_balance"),
                reserved_pusd=capital_meta.get("reserved_pusd"),
                available_trade_bankroll=capital_meta.get("available_trade_bankroll"),
                unredeemed_winning_estimate=capital_meta.get("unredeemed_winning_estimate"),
            )
            if capital_error is not None:
                result = {
                    "status": "live_input_missing",
                    "strategy_id": BROWNIAN_STRATEGY_ID,
                    "missing_input_reason": capital_error,
                    "missing_components": ["live_bankroll"],
                }
                live_logger.write_decision(result)
                _set_stage("sleep")
                trace_event("sleep_start", reason="capital_risk_state_error", poll_interval_sec=float(args.poll_interval_sec))
                sleep_start = time.monotonic()
                if not sleep_until_deadline(deadline, args.poll_interval_sec):
                    deadline_reached = True
                    trace_stage_done("sleep_done", stage="sleep", started_mono=sleep_start, deadline_reached=True)
                    break
                trace_stage_done("sleep_done", stage="sleep", started_mono=sleep_start, deadline_reached=False)
                continue

        _set_stage("strategy_cycle")
        trace_event("strategy_cycle_start")
        cycle_start = time.monotonic()
        routed = run_btc5m_strategy_cycle(
            strategy_id=BROWNIAN_STRATEGY_ID,
            live_input=built["input"],
            brownian_execution_callback=execution_callback,
            brownian_config=cfg,
        )
        result = routed.result
        trace_stage_done(
            "strategy_cycle_done",
            stage="strategy_cycle",
            started_mono=cycle_start,
            status=result.get("status"),
            reason=result.get("reason"),
            final_decision=(result.get("decision_debug") or {}).get("final_decision"),
            reject_reason=(result.get("decision_debug") or {}).get("reject_reason") or result.get("reason"),
            market_id=result.get("market_id"),
            market_slug=result.get("market_slug"),
            market_age_seconds=(result.get("decision_debug") or {}).get("market_age_seconds"),
            chosen_side=result.get("side"),
            notional_usd=result.get("notional_usd"),
        )
        live_logger.write_decision(result)
        if result.get("status") in {"submitted_live", "execution_rejected", "execution_error", "paper_validated"}:
            if result.get("status") == "paper_validated":
                if args.stop_after_first_eligible_decision:
                    break
            else:
                break

        _set_stage("sleep")
        trace_event("sleep_start", reason="loop_poll_interval", poll_interval_sec=float(args.poll_interval_sec))
        sleep_start = time.monotonic()
        if not sleep_until_deadline(deadline, args.poll_interval_sec):
            deadline_reached = True
            trace_stage_done("sleep_done", stage="sleep", started_mono=sleep_start, deadline_reached=True)
            break
        trace_stage_done("sleep_done", stage="sleep", started_mono=sleep_start, deadline_reached=False)

    if time.monotonic() > deadline:
        deadline_reached = True
    if deadline_reached:
        trace_event("deadline_reached", iterations=iterations)
    trace_event(
        "runner_exit",
        final_status=result.get("status"),
        reason=result.get("reason") or result.get("missing_input_reason") or result.get("reject_reason"),
        last_stage=last_stage,
        iterations=iterations,
        runtime_elapsed_sec=round(max(0.0, time.monotonic() - run_started_mono), 6),
        deadline_reached=deadline_reached,
        live_execution_callback_invoked=live_execution_invoked,
    )
    return result


def sleep_until_deadline(deadline: float, poll_interval_sec: float) -> bool:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        return False
    interval = max(0.0, float(poll_interval_sec))
    time.sleep(min(interval, remaining))
    return time.monotonic() <= deadline


def apply_live_capital_risk_state(input_payload: dict[str, Any], executor: CanaryExecutor, cfg: BrownianConservativeConfig) -> str | None:
    try:
        if executor.adapter is None or not hasattr(executor.adapter, "capital_state"):
            return "pusd_capital_state_unavailable"
        capital = executor.adapter.capital_state()  # type: ignore[attr-defined]
        pusd_balance = _optional_float(capital.get("pusd_balance"))
        if pusd_balance is None:
            return "pusd_balance_unavailable"
        reserved_pusd = executor.ledger.open_reserved_pusd() if executor.ledger is not None else 0.0
        unredeemed_winners = executor.ledger.unredeemed_winning_estimate() if executor.ledger is not None else 0.0
        available = max(0.0, pusd_balance - reserved_pusd)
    except Exception as exc:
        meta = input_payload.setdefault("live_input_meta", {})
        meta["capital_error"] = str(exc)
        return "pusd_capital_state_unavailable"

    existing = dict(input_payload.get("risk_state") or {})
    session_start = _optional_float(os.environ.get("BTC5M_BROWNIAN_SESSION_START_BANKROLL_USD")) or available
    day_start = _optional_float(os.environ.get("BTC5M_BROWNIAN_DAY_START_BANKROLL_USD")) or available
    daily_pnl = _optional_float(os.environ.get("BTC5M_BROWNIAN_DAILY_PNL_USD")) or 0.0
    if available < cfg.min_order_notional and unredeemed_winners > 0:
        existing["capital_skip_hint"] = "insufficient_pusd_unredeemed_winners_pending"
    existing.update(
        {
            "bankroll": available,
            "current_bankroll": available,
            "bankroll_before": available,
            "session_start_bankroll": session_start,
            "day_start_bankroll": day_start,
            "daily_pnl": daily_pnl,
            "pusd_balance": pusd_balance,
            "reserved_pusd": reserved_pusd,
            "available_trade_bankroll": available,
            "unredeemed_winning_estimate": unredeemed_winners,
            "bankroll_source": "live_pusd_balance_minus_ledger_reservations",
        }
    )
    input_payload["risk_state"] = existing
    input_payload["bankroll"] = available
    input_payload.setdefault("live_input_meta", {})["capital_state"] = {
        "pusd_balance": pusd_balance,
        "reserved_pusd": reserved_pusd,
        "available_trade_bankroll": available,
        "unredeemed_winning_estimate": unredeemed_winners,
        "bankroll_source": "live_pusd_balance_minus_ledger_reservations",
    }
    return None


def brownian_execution_config_from_env() -> ExecutionConfig:
    env = os.environ
    canary_stake_usd = float(env.get("BTC5M_CANARY_STAKE_USD", "1.0"))
    _raw_max_notional = env.get("BTC5M_MAX_NOTIONAL_PER_MARKET_USD")
    max_notional_per_market_usd = float(_raw_max_notional) if _raw_max_notional is not None else canary_stake_usd
    _raw_max_daily = env.get("BTC5M_MAX_DAILY_NOTIONAL_USD")
    max_daily_notional_usd = float(_raw_max_daily) if _raw_max_daily is not None else None
    return ExecutionConfig(
        execution_mode=env.get("BTC5M_EXECUTION_MODE", "observe").strip().lower(),
        live_trading_enabled=str(env.get("BTC5M_BROWNIAN_LIVE_ENABLED", "false")).lower() in {"1", "true", "yes", "on"},
        live_one_shot=str(env.get("BTC5M_LIVE_ONE_SHOT", "true")).lower() in {"1", "true", "yes", "on"},
        max_order_attempts_per_process=int(env.get("BTC5M_MAX_ORDER_ATTEMPTS_PER_PROCESS", "1")),
        canary_stake_usd=canary_stake_usd,
        max_notional_per_market_usd=max_notional_per_market_usd,
        max_daily_notional_usd=max_daily_notional_usd,
        max_open_positions=int(env.get("BTC5M_MAX_OPEN_POSITIONS", "1")),
        one_entry_per_market=True,
        expected_wallet_address=env.get("BTC5M_EXPECTED_WALLET_ADDRESS"),
        order_poll_timeout_sec=float(env.get("BTC5M_ORDER_POLL_TIMEOUT_SEC", "20")),
        order_poll_interval_sec=float(env.get("BTC5M_ORDER_POLL_INTERVAL_SEC", "1")),
        max_quote_age_ms=float(env.get("BTC5M_MAX_QUOTE_AGE_MS") or env.get("BTC5M_QUOTE_MAX_AGE_MS", "5000")),
        max_price_slippage=float(env.get("BTC5M_BROWNIAN_ASK_SLIPPAGE_ABS", "0.01")),
        max_limit_price=0.99,
        journal_root=Path(env.get("BTC5M_EXECUTION_JOURNAL_ROOT", "artifacts/btc5m_canary_execution")),
    )


def live_builder_startup_errors(builder: BTC5MCanaryLiveInputBuilder) -> list[str]:
    errors: list[str] = []
    if builder.config.hmm_state_path is None:
        errors.append("hmm_artifact_unavailable")
    elif not builder.config.hmm_state_path.exists():
        errors.append("hmm_artifact_unavailable")
    else:
        errors.extend(validate_hmm_state_file(builder.config.hmm_state_path, builder.config.max_state_age_sec))
    if builder.config.brownian_state_path is None and (
        os.getenv("BTC5M_LIVE_REFERENCE_PRICE") is None or os.getenv("BTC5M_LIVE_RV30") is None
    ):
        errors.append("brownian_probability_builder_unavailable")
    elif builder.config.brownian_state_path is not None and not builder.config.brownian_state_path.exists():
        errors.append("brownian_probability_builder_unavailable")
    elif builder.config.brownian_state_path is not None:
        errors.extend(validate_brownian_state_file(builder.config.brownian_state_path, builder.config.max_state_age_sec))
    hmm_artifact_dir = os.getenv("BTC5M_HMM_ARTIFACT_DIR")
    if not hmm_artifact_dir or not Path(hmm_artifact_dir).exists():
        errors.append("hmm_model_artifact_dir_missing")
    return errors


def validate_brownian_state_file(path: Path, max_age_sec: float) -> list[str]:
    try:
        payload = load_state_json(path)
    except Exception:
        return ["brownian_state_invalid_json"]
    errors: list[str] = []
    if payload.get("model_id") != "brownian_zero_drift__rv30":
        errors.append("brownian_model_id_mismatch")
    convention = payload.get("probability_convention") or payload.get("probability_replay_convention")
    if convention != "replay-matched brownian_zero_drift__rv30":
        errors.append("brownian_convention_mismatch")
    if _state_is_stale(payload, max_age_sec):
        errors.append("brownian_state_stale")
    return errors


def validate_hmm_state_file(path: Path, max_age_sec: float) -> list[str]:
    try:
        payload = load_state_json(path)
    except Exception:
        return ["hmm_state_invalid_json"]
    errors: list[str] = []
    if payload.get("hmm_model_id") != "laplace_1m__gaussian_hmm__k4":
        errors.append("hmm_model_id_mismatch")
    if payload.get("hmm_state") is None:
        errors.append("hmm_state_missing")
    if not payload.get("model_artifact_path"):
        errors.append("hmm_model_artifact_path_missing")
    elif not Path(str(payload["model_artifact_path"])).exists():
        errors.append("hmm_model_artifact_missing")
    if _state_is_stale(payload, max_age_sec):
        errors.append("hmm_state_stale")
    return errors


def _state_is_stale(payload: dict, max_age_sec: float) -> bool:
    from src.time_utils import parse_datetime

    asof = parse_datetime(payload.get("asof_ts") or payload.get("generated_ts") or payload.get("timestamp"))
    if asof is None:
        return True
    return (utc_now() - asof).total_seconds() > max_age_sec


def _optional_float(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the guarded BTC-5M canary live executor.")
    parser.add_argument("--env-file", type=Path, help="Load a KEY=VALUE env profile before reading runtime settings. Existing shell env wins by default.")
    parser.add_argument("--build-live-input", action="store_true", help="Discover the live BTC-5M market, build canary inputs, evaluate policy, and execute if eligible.")
    parser.add_argument("--decision-json", type=Path, help="Provenance-stamped policy decision JSON produced by the canary evaluator.")
    parser.add_argument("--decision-input-json", type=Path, help="Raw policy input JSON; runner evaluates policy and stamps provenance before execution.")
    parser.add_argument("--decision-output-json", type=Path, default=Path(__import__("os").environ.get("BTC5M_DECISION_OUTPUT_JSON", "artifacts/btc5m_canary_execution/latest_decision.json")))
    parser.add_argument("--live-log-root", type=Path, default=Path(__import__("os").environ.get("BTC5M_LIVE_LOG_ROOT", "artifacts/btc5m_canary_live")))
    parser.add_argument("--max-runtime-sec", type=float, default=float(__import__("os").environ.get("BTC5M_MAX_RUNTIME_SEC", "30")))
    parser.add_argument("--poll-interval-sec", type=float, default=float(__import__("os").environ.get("BTC5M_CANARY_TICK_SEC", "1")))
    parser.add_argument(
        "--stop-after-first-eligible-decision",
        action="store_true",
        default=str(__import__("os").environ.get("BTC5M_STOP_AFTER_FIRST_ELIGIBLE_DECISION", "true")).lower() in {"1", "true", "yes"},
    )
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    try:
        args = build_parser().parse_args(argv)
        trace_event(
            "args_parsed",
            env_file=str(args.env_file) if args.env_file is not None else None,
            build_live_input=bool(args.build_live_input),
            max_runtime_sec=float(args.max_runtime_sec),
            poll_interval_sec=float(args.poll_interval_sec),
            stop_after_first_eligible_decision=bool(args.stop_after_first_eligible_decision),
        )
        if args.env_file is not None:
            trace_event("env_file_load_start", env_file=str(args.env_file))
            env_load_start = time.monotonic()
            loaded = load_env_file(args.env_file, override=False, required=True)
            trace_stage_done(
                "env_file_load_done",
                stage="env_file_load",
                started_mono=env_load_start,
                env_file=str(args.env_file),
                loaded_keys=sorted(loaded),
            )
            print(json.dumps({"env_file": str(args.env_file), "loaded_keys": sorted(loaded), "loaded": loaded_env_summary(loaded)}, sort_keys=True))
        result = run(args)
    except KeyboardInterrupt:
        print(json.dumps({"status": "interrupted", "strategy_id": os.environ.get("BTC5M_STRATEGY_ID")}, indent=2, sort_keys=True))
        return 130
    except Exception as exc:
        print(f"btc5m canary live runner failed: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(result, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
