from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Optional

from .btc5m_brownian_conservative import STRATEGY_ID as BROWNIAN_STRATEGY_ID
from .btc5m_brownian_conservative import BrownianConservativeConfig
from .btc5m_brownian_runner import BrownianRunnerResult, run_brownian_conservative_cycle
from .btc5m_canary_execution import CanaryExecutor, OrderIntent, extract_order_id, extract_status, idempotency_key, normalize_clob_error, quantize_price
from .btc5m_canary_policy import POLICY_ID as HMM_CANARY_POLICY_ID


HMM_CANARY_STRATEGY_IDS = frozenset({HMM_CANARY_POLICY_ID, "state3_ask_brownian_age60_v0"})


@dataclass(frozen=True)
class StrategyRouterResult:
    strategy_id: str
    route: str
    result: dict[str, Any]


CanaryCycle = Callable[[dict[str, Any]], dict[str, Any]]
ExecutionCallback = Callable[[dict[str, Any]], dict[str, Any]]


def selected_strategy_id(env: Optional[dict[str, str]] = None) -> str:
    source = env if env is not None else os.environ
    return source.get("BTC5M_STRATEGY_ID") or source.get("BTC5M_POLICY_ID") or HMM_CANARY_POLICY_ID


def run_btc5m_strategy_cycle(
    *,
    strategy_id: str,
    live_input: dict[str, Any],
    canary_cycle: Optional[CanaryCycle] = None,
    brownian_execution_callback: Optional[ExecutionCallback] = None,
    brownian_config: Optional[BrownianConservativeConfig] = None,
    validation_now_ts: Optional[Any] = None,
) -> StrategyRouterResult:
    if strategy_id in HMM_CANARY_STRATEGY_IDS:
        if canary_cycle is None:
            raise ValueError("canary_cycle is required for state3_ask_brownian_age60_v0")
        return StrategyRouterResult(strategy_id=strategy_id, route="hmm_canary", result=canary_cycle(live_input))
    if strategy_id == BROWNIAN_STRATEGY_ID:
        result = run_brownian_from_live_input(
            live_input,
            config=brownian_config,
            execution_callback=brownian_execution_callback,
            validation_now_ts=validation_now_ts,
        )
        return StrategyRouterResult(strategy_id=strategy_id, route="brownian_conservative", result=result.as_dict())
    raise ValueError(f"unsupported BTC5M_STRATEGY_ID={strategy_id!r}")


def run_brownian_from_live_input(
    live_input: dict[str, Any],
    *,
    config: Optional[BrownianConservativeConfig] = None,
    execution_callback: Optional[ExecutionCallback] = None,
    validation_now_ts: Optional[Any] = None,
) -> BrownianRunnerResult:
    market = dict(live_input.get("market") or {})
    quote = dict(live_input.get("quote") or {})
    price_state = dict(live_input.get("price_state") or {})
    risk_state = dict(live_input.get("risk_state") or {})
    risk_state.setdefault("bankroll", live_input.get("bankroll") or risk_state.get("current_bankroll") or risk_state.get("bankroll_before") or 0.0)
    snapshot = {**market, **quote}
    paper_intent_log_path = None
    if config is not None:
        paper_intent_log_path = Path(config.validation_log_path).parent / "paper_order_intents.jsonl"
    return run_brownian_conservative_cycle(
        market=market,
        quote=quote,
        price_state=price_state,
        risk_state=risk_state,
        current_market_snapshot=snapshot,
        config=config,
        now_ts=live_input.get("decision_ts"),
        validation_now_ts=validation_now_ts,
        execution_callback=execution_callback,
        paper_intent_log_path=paper_intent_log_path,
    )


def execute_brownian_request_with_canary_route(executor: CanaryExecutor, request: dict[str, Any]) -> dict[str, Any]:
    intent, reason = brownian_execution_request_to_order_intent(request, executor)
    if intent is None:
        event = executor._event("execution_skipped", decision=request, skip_reason=reason)
        executor.journal.write(event)
        return event
    if executor.journal.has_blocking_duplicate(intent.idempotency_key, allow_reentry_after_reject=executor.config.allow_reentry_after_reject):
        event = executor._event("execution_skipped", intent=intent, skip_reason="duplicate_journal_entry")
        executor.journal.write(event)
        return event
    if not executor.config.is_live():
        event = executor._event("execution_skipped", intent=intent, skip_reason="not_live_mode")
        executor.journal.write(event)
        return event
    executor.journal.write(executor._event("order_intent_created", intent=intent, brownian_validation_id=request.get("validation_id")))
    if executor.order_attempts >= executor.config.max_order_attempts_per_process:
        event = executor._event("execution_skipped", intent=intent, skip_reason="max_order_attempts_reached")
        executor.journal.write(event)
        return event
    if executor.config.live_one_shot and executor.order_attempts >= 1:
        event = executor._event("live_one_shot_exit", intent=intent, skip_reason="live_one_shot_already_used")
        executor.journal.write(event)
        return event
    if executor.adapter is None:
        event = executor._event("execution_error", intent=intent, raw_error_reason="missing_clob_adapter")
        executor.journal.write(event)
        return event
    executor.order_attempts += 1
    try:
        submitted = executor.adapter.submit_buy(intent)
    except Exception as exc:
        normalized = normalize_clob_error(exc)
        event_type = "execution_rejected_by_venue" if normalized["terminal"] else "execution_error_after_submit"
        event = executor._event(event_type, intent=intent, **normalized)
        executor.journal.write(event)
        return event
    order_id = extract_order_id(submitted)
    executor.journal.write(
        executor._event(
            "live_order_submitted",
            intent=intent,
            order_id=order_id,
            clob_status=extract_status(submitted),
            raw_response=submitted,
            brownian_validation_id=request.get("validation_id"),
        )
    )
    final_event = executor.poll_order(intent, order_id)
    if executor.config.live_one_shot:
        executor.journal.write(executor._event("live_one_shot_exit", intent=intent, order_id=order_id))
    return final_event


def brownian_execution_request_to_order_intent(request: dict[str, Any], executor: CanaryExecutor) -> tuple[Optional[OrderIntent], Optional[str]]:
    if request.get("strategy_id") != BROWNIAN_STRATEGY_ID:
        return None, "strategy_id_mismatch"
    token_id = request.get("token_id")
    if not token_id:
        return None, "selected_token_missing"
    stake = _float(request.get("notional_usd") or request.get("stake_usd"))
    if stake is None or stake <= 0:
        return None, "missing_or_invalid_stake"
    limit_price = _float(request.get("limit_price"))
    if limit_price is None or limit_price <= 0 or limit_price >= 1:
        return None, "invalid_limit_price"
    side = str(request.get("side") or request.get("selected_side") or "").upper()
    if side not in {"YES", "NO"}:
        return None, "invalid_side"
    wallet = executor.wallet_address()
    key = idempotency_key(
        policy_id=BROWNIAN_STRATEGY_ID,
        condition_id=request.get("condition_id"),
        market_id=request.get("market_id"),
        token_id=str(token_id),
        side=side,
        market_start_ts=request.get("market_start_ts"),
        wallet_address=wallet,
    )
    return (
        OrderIntent(
            policy_id=BROWNIAN_STRATEGY_ID,
            market_id=request.get("market_id"),
            condition_id=request.get("condition_id"),
            token_id=str(token_id),
            selected_side=side,
            action="BUY",
            selected_ask=float(limit_price),
            selected_edge=float(request.get("edge") or 0.0),
            stake_usd=float(stake),
            market_age_sec=float(request.get("market_age_sec") or 0.0),
            decision_ts=request.get("decision_ts"),
            quote_ts=request.get("quote_ts"),
            quote_age_ms=_float(request.get("quote_age_ms")),
            client_order_id=f"btc5m-brownian-{key[:24]}",
            idempotency_key=key,
            market_start_ts=request.get("market_start_ts"),
            yes_token_id=request.get("yes_token_id"),
            no_token_id=request.get("no_token_id"),
            limit_price=quantize_price(float(limit_price)),
            max_price=float(limit_price),
        ),
        None,
    )


def _float(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
