from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Optional

from ..time_utils import isoformat_utc, parse_datetime, utc_now
from .btc5m_brownian_conservative import (
    STRATEGY_ID,
    BrownianConservativeConfig,
    decide_brownian_conservative,
    write_decision_log_row,
)
from .btc5m_brownian_order_validator import (
    BrownianOrderValidationInput,
    BrownianOrderValidationResult,
    validate_brownian_order_intent,
    validation_log_row,
    write_validation_log_row,
)


PAPER_INTENT_LOG = Path("artifacts/live_strategy_decisions/brownian_no_hmm_conservative_v1/paper_order_intents.jsonl")


@dataclass(frozen=True)
class BrownianRunnerResult:
    status: str
    strategy_id: str
    market_id: Optional[str] = None
    market_slug: Optional[str] = None
    side: Optional[str] = None
    notional_usd: Optional[float] = None
    reason: Optional[str] = None
    decision_debug: Optional[dict[str, Any]] = None
    validation_debug: Optional[dict[str, Any]] = None
    execution_result: Optional[dict[str, Any]] = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "strategy_id": self.strategy_id,
            "market_id": self.market_id,
            "market_slug": self.market_slug,
            "side": self.side,
            "notional_usd": self.notional_usd,
            "reason": self.reason,
            "decision_debug": self.decision_debug,
            "validation_debug": self.validation_debug,
            "execution_result": self.execution_result,
        }


ExecutionCallback = Callable[[dict[str, Any]], dict[str, Any]]
ValidatorCallback = Callable[[BrownianOrderValidationInput], BrownianOrderValidationResult]


def run_brownian_conservative_cycle(
    *,
    market: dict[str, Any],
    quote: dict[str, Any],
    price_state: dict[str, Any],
    risk_state: Optional[dict[str, Any]] = None,
    current_market_snapshot: Optional[dict[str, Any]] = None,
    bankroll: Optional[float] = None,
    already_traded_market: Optional[bool] = None,
    config: Optional[BrownianConservativeConfig] = None,
    now_ts: Optional[Any] = None,
    validation_now_ts: Optional[Any] = None,
    execution_callback: Optional[ExecutionCallback] = None,
    validator: ValidatorCallback = validate_brownian_order_intent,
    decision_log_path: Optional[str | Path] = None,
    validation_log_path: Optional[str | Path] = None,
    paper_intent_log_path: Optional[str | Path] = None,
) -> BrownianRunnerResult:
    cfg = config or BrownianConservativeConfig.from_env()
    risk = dict(risk_state or {})
    if bankroll is not None:
        risk["bankroll"] = bankroll
    if already_traded_market is not None:
        risk["already_traded_market"] = already_traded_market
    paper_log = Path(paper_intent_log_path or PAPER_INTENT_LOG)
    if cfg.paper_only and cfg.one_entry_per_market and not risk.get("already_traded_market"):
        risk["already_traded_market"] = has_paper_market_entry(paper_log, market)

    decision = decide_brownian_conservative(
        market=market,
        quote=quote,
        price_state=price_state,
        risk_state=risk,
        config=cfg,
        decision_ts=now_ts,
    )
    write_decision_log_row(decision_log_path or cfg.decision_log_path, decision)
    if not decision.get("should_trade"):
        return _result("no_trade", decision=decision, reason=decision.get("reject_reason"))

    snapshot = _build_snapshot(current_market_snapshot, market, quote)
    validation_input = BrownianOrderValidationInput(
        order_intent=decision.get("order_intent") or {},
        decision_row=decision,
        current_market_snapshot=snapshot,
        bankroll=float(risk.get("bankroll") or decision.get("bankroll_before") or 0.0),
        already_traded_market=bool(already_traded_market if already_traded_market is not None else risk.get("already_traded_market", False)),
        paper_only=bool(cfg.paper_only),
        live_enabled=bool(cfg.live_enabled),
        config=cfg,
        now_ts=validation_now_ts or now_ts or decision.get("decision_ts") or utc_now(),
    )
    validation = validator(validation_input)
    write_validation_log_row(validation_log_path or cfg.validation_log_path, validation_log_row(validation_input, validation))
    if not validation.accepted:
        return _result("validation_rejected", decision=decision, validation=validation, reason=validation.reject_reason)

    normalized = validation.normalized_order_intent or {}
    if not normalized.get("executable_live"):
        write_paper_order_intent(paper_intent_log_path or PAPER_INTENT_LOG, normalized)
        return _result("paper_validated", decision=decision, validation=validation)

    request = brownian_normalized_intent_to_execution_request(normalized)
    if execution_callback is None:
        return _result("execution_rejected", decision=decision, validation=validation, reason="missing_execution_route")
    try:
        execution_result = execution_callback(request)
    except Exception as exc:
        return _result("execution_error", decision=decision, validation=validation, execution_result={"error": str(exc)}, reason=str(exc))
    status = "submitted_live"
    execution_status = str(execution_result.get("event_type") or execution_result.get("status") or "").lower()
    if execution_status in {
        "execution_skipped",
        "execution_rejected",
        "execution_error",
        "execution_error_after_submit",
        "execution_rejected_by_venue",
        "order_unknown_after_submit",
        "rejected",
        "error",
        "unknown",
    }:
        status = "execution_rejected"
    return _result(status, decision=decision, validation=validation, execution_result=execution_result, reason=execution_result.get("skip_reason") or execution_result.get("reject_reason"))


def brownian_normalized_intent_to_execution_request(normalized_intent: dict[str, Any]) -> dict[str, Any]:
    debug = normalized_intent.get("validation_debug") or {}
    return {
        "strategy_id": STRATEGY_ID,
        "policy_id": STRATEGY_ID,
        "market_id": normalized_intent.get("market_id"),
        "condition_id": normalized_intent.get("condition_id"),
        "market_slug": normalized_intent.get("market_slug"),
        "side": normalized_intent.get("side"),
        "selected_side": normalized_intent.get("selected_side") or normalized_intent.get("side"),
        "token_id": normalized_intent.get("token_id"),
        "yes_token_id": debug.get("yes_token_id"),
        "no_token_id": debug.get("no_token_id"),
        "action": "BUY",
        "notional_usd": normalized_intent.get("notional_usd"),
        "stake_usd": normalized_intent.get("stake_usd") or normalized_intent.get("notional_usd"),
        "limit_price": normalized_intent.get("limit_price"),
        "max_slippage_abs": normalized_intent.get("max_slippage_abs"),
        "model_id": normalized_intent.get("model_id"),
        "model_probability": normalized_intent.get("model_probability"),
        "edge": normalized_intent.get("edge"),
        "expected_log_growth": normalized_intent.get("expected_log_growth_recomputed"),
        "stake_fraction": normalized_intent.get("stake_fraction_recomputed"),
        "depth_utilization": normalized_intent.get("depth_utilization_recomputed"),
        "validation_id": normalized_intent.get("validation_id"),
        "validation_timestamp": normalized_intent.get("validation_timestamp"),
        "market_start_ts": normalized_intent.get("market_start_ts"),
        "decision_ts": normalized_intent.get("decision_ts"),
        "quote_ts": normalized_intent.get("quote_ts"),
        "quote_age_ms": normalized_intent.get("quote_age_ms"),
        "market_age_sec": debug.get("market_age_seconds"),
        "metadata": {
            "strategy_id": STRATEGY_ID,
            "validation_id": normalized_intent.get("validation_id"),
            "validation_debug": debug,
        },
    }


def write_paper_order_intent(path: str | Path, normalized_intent: dict[str, Any]) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    debug = normalized_intent.get("validation_debug") or {}
    row = {
        "timestamp": isoformat_utc(parse_datetime(normalized_intent.get("validation_timestamp")) or utc_now()),
        "strategy_id": STRATEGY_ID,
        "market_id": normalized_intent.get("market_id"),
        "market_slug": normalized_intent.get("market_slug"),
        "side": normalized_intent.get("side"),
        "notional_usd": normalized_intent.get("notional_usd"),
        "limit_price": normalized_intent.get("limit_price"),
        "model_probability": normalized_intent.get("model_probability"),
        "edge": normalized_intent.get("edge"),
        "expected_log_growth": normalized_intent.get("expected_log_growth_recomputed"),
        "bankroll": debug.get("bankroll"),
        "stake_fraction": normalized_intent.get("stake_fraction_recomputed"),
        "validation_id": normalized_intent.get("validation_id"),
        "reason": "paper_validated",
        "debug": debug,
    }
    with target.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(row, sort_keys=True, default=str) + "\n")


def has_paper_market_entry(path: str | Path, market: dict[str, Any]) -> bool:
    target = Path(path)
    if not target.exists():
        return False
    market_ids = {str(value) for value in [market.get("market_id"), market.get("condition_id"), market.get("slug"), market.get("market_slug")] if value}
    if not market_ids:
        return False
    try:
        lines = target.read_text(encoding="utf-8").splitlines()
    except OSError:
        return False
    for line in lines:
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        row_ids = {
            str(value)
            for value in [
                row.get("market_id"),
                row.get("condition_id"),
                row.get("market_slug"),
                (row.get("debug") or {}).get("condition_id"),
                (row.get("debug") or {}).get("market_slug"),
            ]
            if value
        }
        if market_ids & row_ids:
            return True
    return False


def _build_snapshot(current: Optional[dict[str, Any]], market: dict[str, Any], quote: dict[str, Any]) -> dict[str, Any]:
    snapshot = {**market, **quote}
    if current:
        snapshot.update(current)
    if "slug" in snapshot and "market_slug" not in snapshot:
        snapshot["market_slug"] = snapshot["slug"]
    return snapshot


def _result(
    status: str,
    *,
    decision: dict[str, Any],
    validation: Optional[BrownianOrderValidationResult] = None,
    execution_result: Optional[dict[str, Any]] = None,
    reason: Optional[str] = None,
) -> BrownianRunnerResult:
    normalized = validation.normalized_order_intent if validation and validation.normalized_order_intent else {}
    return BrownianRunnerResult(
        status=status,
        strategy_id=STRATEGY_ID,
        market_id=decision.get("market_id") or normalized.get("market_id"),
        market_slug=decision.get("slug") or normalized.get("market_slug"),
        side=decision.get("chosen_side") or normalized.get("side"),
        notional_usd=decision.get("stake_notional") or normalized.get("notional_usd"),
        reason=reason,
        decision_debug=decision,
        validation_debug=validation.validation_debug if validation else None,
        execution_result=execution_result,
    )
