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
    strategy_id = selected_strategy_id()
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
                time.sleep(float(args.poll_interval_sec))
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
        time.sleep(float(args.poll_interval_sec))
    return result


def run_brownian_strategy(args: argparse.Namespace) -> dict:
    cfg = BrownianConservativeConfig.from_env()
    env_errors = validate_brownian_runtime_env()
    if env_errors:
        raise RuntimeError("BTC5M Brownian runtime env refused: " + ", ".join(env_errors))
    if not args.build_live_input:
        return {
            "status": "observe_no_decision",
            "strategy_id": BROWNIAN_STRATEGY_ID,
            "note": "Brownian server mode requires --build-live-input so current quote, price, volatility, bankroll, and market-age gates are rebuilt.",
        }
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
    executor = None
    execution_callback = None
    if not cfg.paper_only and cfg.live_enabled:
        exec_config = brownian_execution_config_from_env()
        adapter = PyClobClientAdapter() if exec_config.execution_mode == "live" else None
        journal = ExecutionJournal(exec_config.journal_root)
        journal.ensure_writable()
        ledger = LiveLedger(exec_config.ledger_path)
        executor = CanaryExecutor(exec_config, adapter, journal, ledger=ledger)
        execution_callback = lambda request: execute_brownian_request_with_canary_route(executor, request)
    deadline = time.monotonic() + float(args.max_runtime_sec)
    result: dict[str, Any] = {"status": "no_decision_before_timeout", "strategy_id": BROWNIAN_STRATEGY_ID}
    while time.monotonic() <= deadline:
        built = builder.build()
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
            time.sleep(float(args.poll_interval_sec))
            continue
        routed = run_btc5m_strategy_cycle(
            strategy_id=BROWNIAN_STRATEGY_ID,
            live_input=built["input"],
            brownian_execution_callback=execution_callback,
            brownian_config=cfg,
        )
        result = routed.result
        live_logger.write_decision(result)
        if result.get("status") in {"submitted_live", "execution_rejected", "execution_error", "paper_validated"}:
            if result.get("status") == "paper_validated":
                if args.stop_after_first_eligible_decision:
                    break
            else:
                break
        time.sleep(float(args.poll_interval_sec))
    return result


def brownian_execution_config_from_env() -> ExecutionConfig:
    env = os.environ
    return ExecutionConfig(
        execution_mode=env.get("BTC5M_EXECUTION_MODE", "observe").strip().lower(),
        live_trading_enabled=str(env.get("BTC5M_BROWNIAN_LIVE_ENABLED", "false")).lower() in {"1", "true", "yes", "on"},
        live_one_shot=str(env.get("BTC5M_LIVE_ONE_SHOT", "true")).lower() in {"1", "true", "yes", "on"},
        max_order_attempts_per_process=int(env.get("BTC5M_MAX_ORDER_ATTEMPTS_PER_PROCESS", "1")),
        canary_stake_usd=1.0,
        max_notional_per_market_usd=None,
        max_daily_notional_usd=None,
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
        if args.env_file is not None:
            loaded = load_env_file(args.env_file, override=False, required=True)
            print(json.dumps({"env_file": str(args.env_file), "loaded_keys": sorted(loaded), "loaded": loaded_env_summary(loaded)}, sort_keys=True))
        result = run(args)
    except Exception as exc:
        print(f"btc5m canary live runner failed: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(result, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
