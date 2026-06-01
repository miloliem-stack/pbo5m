"""btc5m_lifecycle_supervisor.py

Supervised multi-market lifecycle supervisor for the BTC-5m Brownian canary
strategy.  Four independent, cooperative worker functions handle the full
market lifecycle inside a single-threaded tick loop:

  1. Trading worker   – evaluates the routed market and submits at most one
                        order per market after all safety gates pass.
  2. Reconciliation   – polls live CLOB orders until they reach a terminal
     worker            fill / cancel / reject state, updating the ledger.
  3. Resolution       – queries GammaCtfResolutionSource for each market where
     worker            we hold outcome lots, records resolution in the ledger,
                       and terminalizes the lots.
  4. Redemption       – finds resolved winning lots, invokes PusdCtfRedeemAdapter,
     worker            records results, and marks lots as redeemed.

All external I/O is injected via callable arguments so every worker function
is fully unit-testable without live network access.

Safety-gate env vars (all read by SupervisorConfig.from_env):
  BTC5M_MAX_UNRESOLVED_MARKETS           (default 3)
  BTC5M_MAX_TOTAL_UNREDEEMED_NOTIONAL_USD (default: no limit)
  BTC5M_MAX_LIVE_OPEN_ORDERS             (default 1)
  BTC5M_BLOCK_ON_UNKNOWN_ORDER           (default true)
  BTC5M_BLOCK_ON_RECONCILIATION_STALE    (default true)
  BTC5M_RECONCILIATION_STALE_SEC         (default 120)
  BTC5M_BLOCK_ON_REDEEMER_HEALTH_FAILURE (default true)
  BTC5M_MAX_REDEMPTION_FAILURES          (default 3)
"""
from __future__ import annotations

import os
import time
from dataclasses import dataclass
from typing import Any, Callable, Optional

from ..runtime.btc5m_live_ledger import LiveLedger
from ..runtime.operator_trace import trace_event, trace_stage_done


# ---------------------------------------------------------------------------
# SupervisorConfig
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class SupervisorConfig:
    """All tunable safety-gate and timing parameters for the supervisor loop.

    Read from environment with :meth:`from_env`.  All fields have safe
    defaults so the supervisor can start without explicit configuration.
    """
    max_unresolved_markets: int = 3
    max_total_unredeemed_notional_usd: Optional[float] = None
    max_live_open_orders: int = 1
    block_on_unknown_order: bool = True
    block_on_reconciliation_stale: bool = True
    reconciliation_stale_sec: float = 120.0
    block_on_redeemer_health_failure: bool = True
    max_redemption_failures: int = 3
    # Tick intervals
    trading_tick_interval_sec: float = 5.0
    reconciliation_tick_interval_sec: float = 3.0
    resolution_tick_interval_sec: float = 10.0
    redemption_tick_interval_sec: float = 15.0
    loop_sleep_sec: float = 1.0

    @classmethod
    def from_env(cls, env: Optional[dict[str, str]] = None) -> "SupervisorConfig":
        e = env if env is not None else os.environ

        def _int(key: str, default: int) -> int:
            try:
                return int(e.get(key, str(default)))
            except (TypeError, ValueError):
                return default

        def _float(key: str, default: float) -> float:
            try:
                return float(e.get(key, str(default)))
            except (TypeError, ValueError):
                return default

        def _bool(key: str, default: bool) -> bool:
            raw = e.get(key)
            if raw is None:
                return default
            return str(raw).strip().lower() in {"1", "true", "yes", "on"}

        def _optional_float(key: str) -> Optional[float]:
            raw = e.get(key)
            if not raw:
                return None
            try:
                return float(raw)
            except (TypeError, ValueError):
                return None

        return cls(
            max_unresolved_markets=_int("BTC5M_MAX_UNRESOLVED_MARKETS", 3),
            max_total_unredeemed_notional_usd=_optional_float("BTC5M_MAX_TOTAL_UNREDEEMED_NOTIONAL_USD"),
            max_live_open_orders=_int("BTC5M_MAX_LIVE_OPEN_ORDERS", 1),
            block_on_unknown_order=_bool("BTC5M_BLOCK_ON_UNKNOWN_ORDER", True),
            block_on_reconciliation_stale=_bool("BTC5M_BLOCK_ON_RECONCILIATION_STALE", True),
            reconciliation_stale_sec=_float("BTC5M_RECONCILIATION_STALE_SEC", 120.0),
            block_on_redeemer_health_failure=_bool("BTC5M_BLOCK_ON_REDEEMER_HEALTH_FAILURE", True),
            max_redemption_failures=_int("BTC5M_MAX_REDEMPTION_FAILURES", 3),
            trading_tick_interval_sec=_float("BTC5M_SUPERVISOR_TRADING_TICK_SEC", 5.0),
            reconciliation_tick_interval_sec=_float("BTC5M_SUPERVISOR_RECONCILIATION_TICK_SEC", 3.0),
            resolution_tick_interval_sec=_float("BTC5M_SUPERVISOR_RESOLUTION_TICK_SEC", 30.0),
            redemption_tick_interval_sec=_float("BTC5M_SUPERVISOR_REDEMPTION_TICK_SEC", 60.0),
            loop_sleep_sec=_float("BTC5M_SUPERVISOR_LOOP_SLEEP_SEC", 1.0),
        )


# ---------------------------------------------------------------------------
# Safety gate evaluation
# ---------------------------------------------------------------------------

@dataclass
class SafetyGateResult:
    trading_allowed: bool
    block_reasons: list[str]
    open_orders: int
    unknown_orders: int
    unresolved_markets: int
    unredeemed_notional: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "trading_allowed": self.trading_allowed,
            "block_reasons": self.block_reasons,
            "open_orders": self.open_orders,
            "unknown_orders": self.unknown_orders,
            "unresolved_markets": self.unresolved_markets,
            "unredeemed_notional": self.unredeemed_notional,
        }


def evaluate_trading_safety_gates(
    ledger: LiveLedger,
    config: SupervisorConfig,
    *,
    last_reconciliation_ts: Optional[float] = None,
    now_mono: Optional[float] = None,
) -> SafetyGateResult:
    """Evaluate all configurable safety gates against current ledger state.

    Returns a :class:`SafetyGateResult` whose ``trading_allowed`` field is
    ``True`` only when every gate passes.  The ``block_reasons`` list contains
    a human-readable code for each failed gate.

    Parameters
    ----------
    ledger:
        The live SQLite ledger – all counts are derived from it.
    config:
        Supervisor configuration containing gate thresholds.
    last_reconciliation_ts:
        ``time.monotonic()`` timestamp of the last successful reconciliation
        tick.  ``None`` if reconciliation has not run yet this session.
    now_mono:
        Override for ``time.monotonic()`` (useful in tests).
    """
    reasons: list[str] = []

    open_orders = ledger.count_live_open_orders()
    unknown_orders = ledger.count_unknown_orders()
    unresolved_markets = ledger.count_unresolved_markets()
    unredeemed_notional = ledger.total_unredeemed_notional_estimate()

    # Gate 1 – too many live open CLOB orders
    if open_orders >= config.max_live_open_orders:
        reasons.append(
            f"max_live_open_orders_exceeded:{open_orders}>={config.max_live_open_orders}"
        )

    # Gate 2 – unknown order state (submitted but status never confirmed)
    if unknown_orders > 0 and config.block_on_unknown_order:
        reasons.append(f"unknown_order_blocked:count={unknown_orders}")

    # Gate 3 – too many unresolved markets with active lots
    if unresolved_markets >= config.max_unresolved_markets:
        reasons.append(
            f"max_unresolved_markets_exceeded:{unresolved_markets}>={config.max_unresolved_markets}"
        )

    # Gate 4 – unredeemed notional ceiling
    if (
        config.max_total_unredeemed_notional_usd is not None
        and unredeemed_notional >= config.max_total_unredeemed_notional_usd
    ):
        reasons.append(
            f"max_unredeemed_notional_exceeded:"
            f"{unredeemed_notional:.4f}>={config.max_total_unredeemed_notional_usd:.4f}"
        )

    # Gate 5 – reconciliation worker staleness
    if config.block_on_reconciliation_stale and last_reconciliation_ts is not None:
        now = now_mono if now_mono is not None else time.monotonic()
        stale_sec = now - last_reconciliation_ts
        if stale_sec > config.reconciliation_stale_sec:
            reasons.append(
                f"reconciliation_stale:{stale_sec:.1f}s>{config.reconciliation_stale_sec}s"
            )

    # Gate 6 – redeemer health (too many consecutive redemption failures)
    if config.block_on_redeemer_health_failure:
        redeemable = ledger.redeemable_lots()
        seen_conditions: set[str] = set()
        for lot in redeemable:
            cid = lot.get("condition_id")
            if not cid or cid in seen_conditions:
                continue
            seen_conditions.add(cid)
            failures = ledger.recent_redemption_failures(
                cid, limit=config.max_redemption_failures + 1
            )
            if len(failures) >= config.max_redemption_failures:
                reasons.append(
                    f"redeemer_health_failure:condition={cid}:failures={len(failures)}"
                )

    return SafetyGateResult(
        trading_allowed=not reasons,
        block_reasons=reasons,
        open_orders=open_orders,
        unknown_orders=unknown_orders,
        unresolved_markets=unresolved_markets,
        unredeemed_notional=unredeemed_notional,
    )


# ---------------------------------------------------------------------------
# Reconciliation worker
# ---------------------------------------------------------------------------

def reconciliation_worker_tick(
    *,
    ledger: LiveLedger,
    get_order_status_fn: Callable[[str], dict[str, Any]],
) -> dict[str, Any]:
    """Poll every live CLOB order that has not yet reached a terminal state.

    For each order returned by :meth:`LiveLedger.open_orders_for_reconciliation`
    the worker calls ``get_order_status_fn(order_id)``, maps the CLOB status to
    an event type, and writes the event back to the ledger via
    :meth:`LiveLedger.record_order_event`.  Filled orders also create
    outcome_lots via the ledger's built-in fill handler.

    Returns
    -------
    dict with keys: ``status``, ``polled``, ``filled``, ``errors``.
    """
    from .btc5m_canary_execution import event_type_for_status, extract_float, extract_status

    orders = ledger.open_orders_for_reconciliation()
    if not orders:
        return {"status": "no_orders_to_reconcile", "polled": 0, "filled": 0, "errors": []}

    polled = 0
    filled = 0
    errors: list[dict[str, Any]] = []

    for order_row in orders:
        order_id = order_row.get("order_id")
        if not order_id:
            continue
        try:
            status_response = get_order_status_fn(order_id)
            clob_status = extract_status(status_response)
            event_type = event_type_for_status(clob_status)

            event: dict[str, Any] = {
                "event_type": event_type,
                "order_id": order_id,
                "idempotency_key": order_row.get("idempotency_key"),
                "market_id": order_row.get("market_id"),
                "condition_id": order_row.get("condition_id"),
                "token_id": order_row.get("token_id"),
                "selected_side": order_row.get("side"),
                "clob_status": clob_status,
                "filled_size": extract_float(
                    status_response, "filled_size", "filled_qty", "matched_size", "size_matched"
                ),
                "avg_fill_price": extract_float(
                    status_response, "avg_fill_price", "average_price", "price"
                ),
                "remaining_size": extract_float(
                    status_response, "remaining_size", "remaining_qty"
                ),
                "raw_response": status_response,
            }
            ledger.record_order_event(event)
            polled += 1
            if event_type in {"order_filled", "order_partially_filled"}:
                filled += 1
        except Exception as exc:
            errors.append({"order_id": order_id, "error": str(exc)})

    return {
        "status": "reconciliation_done",
        "polled": polled,
        "filled": filled,
        "errors": errors,
    }


# ---------------------------------------------------------------------------
# Resolution worker
# ---------------------------------------------------------------------------

def resolution_worker_tick(
    *,
    ledger: LiveLedger,
    resolution_source: Any,
) -> dict[str, Any]:
    """For each market with open outcome lots, query the resolution source.

    If a market is confirmed resolved, the resolution is written to the ledger
    via :meth:`LiveLedger.upsert_resolution` and lots are terminalized via
    :meth:`LiveLedger.terminalize_resolved_lots`.

    ``resolution_source`` must expose a ``resolve(lot: dict) -> ResolutionResult``
    method compatible with :class:`GammaCtfResolutionSource`.

    Returns
    -------
    dict with keys: ``status``, ``checked``, ``newly_resolved``, ``errors``.
    """
    lots = ledger.open_outcome_lots()
    if not lots:
        return {"status": "no_lots_to_resolve", "checked": 0, "newly_resolved": 0, "errors": []}

    seen_conditions: set[str] = set()
    checked = 0
    newly_resolved = 0
    errors: list[dict[str, Any]] = []

    for lot in lots:
        condition_id = lot.get("condition_id")
        if not condition_id or condition_id in seen_conditions:
            continue
        seen_conditions.add(condition_id)

        try:
            result = resolution_source.resolve(lot)
            checked += 1
            if result.get("resolved") if isinstance(result, dict) else result.resolved:
                winning_side = result.get("winning_side") if isinstance(result, dict) else result.winning_side
                payout_vector = result.get("payout_vector") if isinstance(result, dict) else getattr(result, "payout_vector", None)
                source = result.get("source") if isinstance(result, dict) else getattr(result, "source", None)
                ledger.upsert_resolution(
                    condition_id=condition_id,
                    market_id=lot.get("market_id"),
                    resolved=True,
                    winning_side=winning_side or "UNKNOWN",
                    source=source or "gamma_ctf",
                    payout_vector=payout_vector,
                )
                ledger.terminalize_resolved_lots()
                newly_resolved += 1
            else:
                # Record that we checked but the market has not resolved yet.
                # This updates last_checked_ts without marking as resolved.
                ledger.upsert_resolution(
                    condition_id=condition_id,
                    market_id=lot.get("market_id"),
                    resolved=False,
                )
                errors.append({
                    "condition_id": condition_id,
                    "error": "not_resolved",
                    "resolution_result": result if isinstance(result, dict) else vars(result),
                })
        except Exception as exc:
            errors.append({"condition_id": condition_id, "error": str(exc)})

    return {
        "status": "resolution_done",
        "checked": checked,
        "newly_resolved": newly_resolved,
        "errors": errors,
    }


# ---------------------------------------------------------------------------
# Redemption worker
# ---------------------------------------------------------------------------

def redemption_worker_tick(
    *,
    ledger: LiveLedger,
    redeem_adapter: Any,
    config: SupervisorConfig,
) -> dict[str, Any]:
    """Attempt to redeem every resolved-win lot that has not yet been redeemed.

    For each distinct condition_id in the redeemable lots:
    - Skip if a successful redemption already exists for that condition.
    - Skip (and log) if recent failure count >= max_redemption_failures.
    - Call ``redeem_adapter.redeem_condition(condition_id, token_ids, index_sets)``.
    - Record the attempt and outcome in the ledger.

    ``redeem_adapter`` must expose a ``redeem_condition`` method compatible with
    :class:`PusdCtfRedeemAdapter`.

    Returns
    -------
    dict with keys: ``status``, ``attempted``, ``redeemed``, ``errors``.
    """
    redeemable = ledger.redeemable_lots()
    if not redeemable:
        return {"status": "no_lots_to_redeem", "attempted": 0, "redeemed": 0, "errors": []}

    # Group lots by condition_id
    by_condition: dict[str, list[dict[str, Any]]] = {}
    for lot in redeemable:
        cid = str(lot.get("condition_id") or "")
        if cid:
            by_condition.setdefault(cid, []).append(lot)

    attempted = 0
    redeemed = 0
    errors: list[dict[str, Any]] = []

    for condition_id, lots in by_condition.items():
        # Skip if already successfully redeemed
        if ledger.has_successful_redemption(condition_id):
            continue

        # Skip if too many recent failures
        recent_failures = ledger.recent_redemption_failures(
            condition_id, limit=config.max_redemption_failures + 1
        )
        if len(recent_failures) >= config.max_redemption_failures:
            errors.append(
                {
                    "condition_id": condition_id,
                    "blocked": "max_redemption_failures_exceeded",
                    "failure_count": len(recent_failures),
                }
            )
            continue

        market_id = lots[0].get("market_id")
        token_ids: list[str] = [str(lot["token_id"]) for lot in lots if lot.get("token_id")]
        # CTF index sets: YES = bit-0 → index_set 1, NO = bit-1 → index_set 2
        index_sets: list[int] = [1 if lot.get("side") == "YES" else 2 for lot in lots if lot.get("token_id")]

        attempt_id = ledger.record_redemption_attempt(
            condition_id=condition_id,
            market_id=market_id,
            token_ids=token_ids,
            index_sets=index_sets,
            status="pending",
        )
        attempted += 1

        try:
            result = redeem_adapter.redeem_condition(
                condition_id=condition_id,
                token_ids=token_ids,
                index_sets=index_sets,
            )
            status = str(result.get("status") or "unknown")
            tx_hash = result.get("tx_hash")

            if status == "confirmed":
                ledger.update_redemption_attempt(
                    attempt_id,
                    status="confirmed",
                    tx_hash=tx_hash,
                    confirmed=True,
                )
                ledger.mark_lots_redeemed(
                    condition_id=condition_id,
                    tx_hash=tx_hash or "",
                    redeemed_pusd_amount=result.get("redeemed_pusd_delta"),
                    receipt=result.get("receipt"),
                )
                redeemed += 1
            else:
                ledger.update_redemption_attempt(
                    attempt_id,
                    status=status if status.startswith("failed") else "failed_retryable",
                    tx_hash=tx_hash,
                    raw_error=str(result.get("raw_error") or result),
                )
                errors.append({"condition_id": condition_id, "status": status})
        except Exception as exc:
            ledger.update_redemption_attempt(
                attempt_id,
                status="failed_retryable",
                raw_error=str(exc),
            )
            errors.append({"condition_id": condition_id, "error": str(exc)})

    return {
        "status": "redemption_done",
        "attempted": attempted,
        "redeemed": redeemed,
        "errors": errors,
    }


# ---------------------------------------------------------------------------
# Trading worker
# ---------------------------------------------------------------------------

def _apply_capital_risk_state(
    live_input: dict[str, Any],
    capital: dict[str, Any],
    ledger: LiveLedger,
    min_order_notional: float,
) -> Optional[str]:
    """Mutate *live_input* in-place with live bankroll data.

    Returns an error string if capital data is unavailable, ``None`` otherwise.
    """
    pusd_balance = _opt_float(capital.get("pusd_balance"))
    if pusd_balance is None:
        return "pusd_balance_unavailable"
    reserved_pusd = ledger.open_reserved_pusd()
    unredeemed_winners = ledger.unredeemed_winning_estimate()
    available = max(0.0, pusd_balance - reserved_pusd)

    existing = dict(live_input.get("risk_state") or {})
    if available < min_order_notional and unredeemed_winners > 0:
        existing["capital_skip_hint"] = "insufficient_pusd_unredeemed_winners_pending"
    existing.update(
        {
            "bankroll": available,
            "current_bankroll": available,
            "bankroll_before": available,
            "pusd_balance": pusd_balance,
            "reserved_pusd": reserved_pusd,
            "available_trade_bankroll": available,
            "unredeemed_winning_estimate": unredeemed_winners,
            "bankroll_source": "live_pusd_balance_minus_ledger_reservations",
        }
    )
    live_input["risk_state"] = existing
    live_input["bankroll"] = available
    live_input.setdefault("live_input_meta", {})["capital_state"] = {
        "pusd_balance": pusd_balance,
        "reserved_pusd": reserved_pusd,
        "available_trade_bankroll": available,
        "unredeemed_winning_estimate": unredeemed_winners,
    }
    return None


def trading_worker_tick(
    *,
    ledger: LiveLedger,
    supervisor_config: SupervisorConfig,
    brownian_config: Any,
    execution_callback: Optional[Callable[[dict[str, Any]], dict[str, Any]]],
    live_input_builder: Any,
    executor: Optional[Any] = None,
    last_reconciliation_ts: Optional[float] = None,
    now_mono: Optional[float] = None,
) -> dict[str, Any]:
    """One trading worker tick: evaluate safety gates then run one strategy cycle.

    Parameters
    ----------
    ledger:
        Shared SQLite ledger used for all gate queries.
    supervisor_config:
        Safety-gate thresholds.
    brownian_config:
        :class:`BrownianConservativeConfig` instance with strategy parameters.
    execution_callback:
        Callback passed to :func:`run_btc5m_strategy_cycle`; set to ``None``
        in paper/observe mode.
    live_input_builder:
        An object with a ``build() -> dict`` method (BTC5MCanaryLiveInputBuilder).
    executor:
        The :class:`CanaryExecutor` instance; used to pull capital state if its
        ``adapter`` exposes ``capital_state()``.  May be ``None`` in observe mode.
    last_reconciliation_ts:
        Monotonic timestamp of the last successful reconciliation tick, forwarded
        to gate evaluation.
    now_mono:
        Optional monotonic clock override (for tests).

    Returns
    -------
    dict with key ``status`` describing the outcome.
    """
    from .btc5m_brownian_conservative import STRATEGY_ID as BROWNIAN_STRATEGY_ID
    from .btc5m_strategy_router import run_btc5m_strategy_cycle

    # ── Safety gates ────────────────────────────────────────────────────────
    gate_start = time.monotonic()
    gate = evaluate_trading_safety_gates(
        ledger,
        supervisor_config,
        last_reconciliation_ts=last_reconciliation_ts,
        now_mono=now_mono,
    )
    trace_stage_done(
        "trading_gate_evaluated",
        stage="trading_gate",
        started_mono=gate_start,
        trading_allowed=gate.trading_allowed,
        block_reasons=gate.block_reasons,
        open_orders=gate.open_orders,
        unknown_orders=gate.unknown_orders,
        unresolved_markets=gate.unresolved_markets,
        unredeemed_notional=gate.unredeemed_notional,
    )

    if not gate.trading_allowed:
        return {
            "status": "trading_blocked",
            "block_reasons": gate.block_reasons,
            "open_orders": gate.open_orders,
            "unknown_orders": gate.unknown_orders,
            "unresolved_markets": gate.unresolved_markets,
            "unredeemed_notional": gate.unredeemed_notional,
        }

    # ── Build live input ─────────────────────────────────────────────────────
    build_start = time.monotonic()
    trace_event("trading_live_input_build_start")
    built = live_input_builder.build()
    trace_stage_done(
        "trading_live_input_build_done",
        stage="trading_live_input_build",
        started_mono=build_start,
        ok=bool(built.get("ok")),
        missing_input_reason=built.get("missing_input_reason"),
    )

    if not built.get("ok"):
        meta = (built.get("input") or {}).get("live_input_meta") or {}
        ret: dict[str, Any] = {
            "status": "live_input_missing",
            "missing_input_reason": built.get("missing_input_reason"),
            "missing_components": built.get("missing_components") or [],
        }
        if meta.get("brownian_error"):
            ret["brownian_error"] = meta["brownian_error"]
        if meta.get("brownian_convention_found"):
            ret["brownian_convention_found"] = meta["brownian_convention_found"]
        if meta.get("hmm_error"):
            ret["hmm_error"] = meta["hmm_error"]
        return ret

    live_input: dict[str, Any] = built["input"]

    # ── Apply capital / bankroll state ───────────────────────────────────────
    if (
        executor is not None
        and executor.adapter is not None
        and hasattr(executor.adapter, "capital_state")
    ):
        capital_start = time.monotonic()
        trace_event("trading_capital_state_start")
        try:
            capital = executor.adapter.capital_state()
            error = _apply_capital_risk_state(
                live_input,
                capital,
                ledger,
                min_order_notional=float(getattr(brownian_config, "min_order_notional", 1.0)),
            )
            trace_stage_done(
                "trading_capital_state_done",
                stage="trading_capital_state",
                started_mono=capital_start,
                capital_error=error,
                pusd_balance=(live_input.get("live_input_meta") or {})
                .get("capital_state", {})
                .get("pusd_balance"),
            )
            if error is not None:
                return {
                    "status": "live_input_missing",
                    "missing_input_reason": error,
                    "missing_components": ["live_bankroll"],
                }
        except Exception as exc:
            trace_stage_done(
                "trading_capital_state_error",
                stage="trading_capital_state",
                started_mono=capital_start,
                error=str(exc),
            )
            return {
                "status": "live_input_missing",
                "missing_input_reason": "pusd_capital_state_unavailable",
                "missing_components": ["live_bankroll"],
            }

    # ── Run strategy cycle ───────────────────────────────────────────────────
    cycle_start = time.monotonic()
    trace_event("trading_strategy_cycle_start")
    routed = run_btc5m_strategy_cycle(
        strategy_id=BROWNIAN_STRATEGY_ID,
        live_input=live_input,
        brownian_execution_callback=execution_callback,
        brownian_config=brownian_config,
    )
    result = routed.result
    trace_stage_done(
        "trading_strategy_cycle_done",
        stage="trading_strategy_cycle",
        started_mono=cycle_start,
        status=result.get("status"),
        reason=result.get("reason"),
        market_id=result.get("market_id"),
    )
    return result


# ---------------------------------------------------------------------------
# Supervisor status summary
# ---------------------------------------------------------------------------

def supervisor_status_summary(ledger: LiveLedger) -> dict[str, Any]:
    """Return a comprehensive supervisor status snapshot derived from the ledger.

    This is suitable for periodic operator trace events and for the entry-point
    script's final JSON output.
    """
    return ledger.supervisor_summary()


# ---------------------------------------------------------------------------
# Main supervisor loop
# ---------------------------------------------------------------------------

def run_supervisor(
    *,
    ledger: LiveLedger,
    supervisor_config: SupervisorConfig,
    brownian_config: Any,
    execution_callback: Optional[Callable[[dict[str, Any]], dict[str, Any]]],
    live_input_builder: Any,
    executor: Optional[Any] = None,
    resolution_source: Optional[Any] = None,
    redeem_adapter: Optional[Any] = None,
    get_order_status_fn: Optional[Callable[[str], dict[str, Any]]] = None,
    max_runtime_sec: float = 3600.0,
    sleep_fn: Callable[[float], None] = time.sleep,
    event_fn: Optional[Callable[[dict[str, Any]], None]] = None,
) -> dict[str, Any]:
    """Run the four-worker supervisor loop until ``max_runtime_sec`` expires or
    an unhandled exception propagates.

    Workers run cooperatively inside a single OS thread.  Each worker has its
    own configurable tick interval; the loop sleeps for
    ``supervisor_config.loop_sleep_sec`` between main iterations so it does not
    busy-spin.

    Parameters
    ----------
    ledger:
        The live SQLite ledger shared by all workers.
    supervisor_config:
        Safety-gate thresholds and tick-interval configuration.
    brownian_config:
        BrownianConservativeConfig passed to the trading worker.
    execution_callback:
        Live-execution callback for the trading worker; ``None`` in observe mode.
    live_input_builder:
        Builds the live input dict for the trading worker.
    executor:
        CanaryExecutor used for capital-state queries in the trading worker.
    resolution_source:
        GammaCtfResolutionSource; skip resolution worker if ``None``.
    redeem_adapter:
        PusdCtfRedeemAdapter; skip redemption worker if ``None``.
    get_order_status_fn:
        Callable(order_id) → status dict; skip reconciliation if ``None``.
    max_runtime_sec:
        Hard deadline for the supervisor loop.
    sleep_fn:
        Injected sleep function (override in tests).

    Returns
    -------
    dict with ``status``, ``iterations``, ``runtime_elapsed_sec``, and the
    final :func:`supervisor_status_summary`.
    """
    run_started = time.monotonic()
    deadline = run_started + max_runtime_sec

    last_trading_ts: float = 0.0
    last_reconciliation_ts: Optional[float] = None
    last_resolution_ts: float = 0.0
    last_redemption_ts: float = 0.0

    iterations = 0
    total_orders_polled = 0
    total_lots_resolved = 0
    total_lots_redeemed = 0

    trace_event(
        "supervisor_start",
        max_runtime_sec=max_runtime_sec,
        max_unresolved_markets=supervisor_config.max_unresolved_markets,
        max_live_open_orders=supervisor_config.max_live_open_orders,
        block_on_unknown_order=supervisor_config.block_on_unknown_order,
        block_on_reconciliation_stale=supervisor_config.block_on_reconciliation_stale,
        reconciliation_stale_sec=supervisor_config.reconciliation_stale_sec,
        block_on_redeemer_health_failure=supervisor_config.block_on_redeemer_health_failure,
        max_redemption_failures=supervisor_config.max_redemption_failures,
        reconciliation_worker_enabled=get_order_status_fn is not None,
        resolution_worker_enabled=resolution_source is not None,
        redemption_worker_enabled=redeem_adapter is not None,
        trading_worker_enabled=live_input_builder is not None,
    )
    if event_fn is not None:
        event_fn({
            "event": "supervisor_workers_ready",
            "trading": live_input_builder is not None,
            "reconciliation": get_order_status_fn is not None,
            "resolution": resolution_source is not None,
            "redemption": redeem_adapter is not None,
            "max_runtime_sec": max_runtime_sec,
            "trading_tick_interval_sec": supervisor_config.trading_tick_interval_sec,
            "resolution_tick_interval_sec": supervisor_config.resolution_tick_interval_sec,
            "redemption_tick_interval_sec": supervisor_config.redemption_tick_interval_sec,
        })

    while time.monotonic() <= deadline:
        now = time.monotonic()
        iterations += 1

        trace_event(
            "supervisor_tick_start",
            iteration=iterations,
            remaining_sec=round(max(0.0, deadline - now), 3),
        )

        # ── Reconciliation worker ─────────────────────────────────────────────
        if (
            get_order_status_fn is not None
            and (now - (last_reconciliation_ts or 0.0)) >= supervisor_config.reconciliation_tick_interval_sec
        ):
            rec_start = time.monotonic()
            try:
                rec = reconciliation_worker_tick(
                    ledger=ledger,
                    get_order_status_fn=get_order_status_fn,
                )
                last_reconciliation_ts = time.monotonic()
                total_orders_polled += int(rec.get("polled") or 0)
                trace_stage_done(
                    "reconciliation_tick_done",
                    stage="reconciliation_tick",
                    started_mono=rec_start,
                    polled=rec.get("polled"),
                    filled=rec.get("filled"),
                    error_count=len(rec.get("errors") or []),
                )
                if event_fn is not None and (rec.get("polled") or rec.get("errors")):
                    event_fn({
                        "event": "reconciliation_tick",
                        "polled": rec.get("polled"),
                        "filled": rec.get("filled"),
                        "errors": rec.get("errors"),
                        "elapsed_sec": round(time.monotonic() - rec_start, 3),
                    })
            except Exception as exc:
                trace_event("reconciliation_tick_error", error=str(exc))
                if event_fn is not None:
                    event_fn({"event": "reconciliation_tick_error", "error": str(exc)})

        # ── Resolution worker ─────────────────────────────────────────────────
        if (
            resolution_source is not None
            and (now - last_resolution_ts) >= supervisor_config.resolution_tick_interval_sec
        ):
            res_start = time.monotonic()
            try:
                res = resolution_worker_tick(
                    ledger=ledger,
                    resolution_source=resolution_source,
                )
                last_resolution_ts = time.monotonic()
                total_lots_resolved += int(res.get("newly_resolved") or 0)
                trace_stage_done(
                    "resolution_tick_done",
                    stage="resolution_tick",
                    started_mono=res_start,
                    checked=res.get("checked"),
                    newly_resolved=res.get("newly_resolved"),
                    error_count=len(res.get("errors") or []),
                )
                if event_fn is not None and (res.get("checked") or res.get("errors")):
                    event_fn({
                        "event": "resolution_tick",
                        "checked": res.get("checked"),
                        "newly_resolved": res.get("newly_resolved"),
                        "errors": res.get("errors"),
                        "elapsed_sec": round(time.monotonic() - res_start, 3),
                    })
            except Exception as exc:
                trace_event("resolution_tick_error", error=str(exc))
                if event_fn is not None:
                    event_fn({"event": "resolution_tick_error", "error": str(exc)})

        # ── Redemption worker ─────────────────────────────────────────────────
        if (
            redeem_adapter is not None
            and (now - last_redemption_ts) >= supervisor_config.redemption_tick_interval_sec
        ):
            red_start = time.monotonic()
            try:
                red = redemption_worker_tick(
                    ledger=ledger,
                    redeem_adapter=redeem_adapter,
                    config=supervisor_config,
                )
                last_redemption_ts = time.monotonic()
                total_lots_redeemed += int(red.get("redeemed") or 0)
                trace_stage_done(
                    "redemption_tick_done",
                    stage="redemption_tick",
                    started_mono=red_start,
                    attempted=red.get("attempted"),
                    redeemed=red.get("redeemed"),
                    error_count=len(red.get("errors") or []),
                )
                if event_fn is not None and (red.get("attempted") or red.get("errors")):
                    event_fn({
                        "event": "redemption_tick",
                        "attempted": red.get("attempted"),
                        "redeemed": red.get("redeemed"),
                        "errors": red.get("errors"),
                        "elapsed_sec": round(time.monotonic() - red_start, 3),
                    })
            except Exception as exc:
                trace_event("redemption_tick_error", error=str(exc))
                if event_fn is not None:
                    event_fn({"event": "redemption_tick_error", "error": str(exc)})

        # ── Trading worker ────────────────────────────────────────────────────
        if (
            live_input_builder is not None
            and (now - last_trading_ts) >= supervisor_config.trading_tick_interval_sec
        ):
            trade_start = time.monotonic()
            try:
                trade = trading_worker_tick(
                    ledger=ledger,
                    supervisor_config=supervisor_config,
                    brownian_config=brownian_config,
                    execution_callback=execution_callback,
                    live_input_builder=live_input_builder,
                    executor=executor,
                    last_reconciliation_ts=last_reconciliation_ts,
                    now_mono=now,
                )
                last_trading_ts = time.monotonic()
                trace_stage_done(
                    "trading_tick_done",
                    stage="trading_tick",
                    started_mono=trade_start,
                    status=trade.get("status"),
                    block_reasons=trade.get("block_reasons"),
                    market_id=trade.get("market_id"),
                )
                if event_fn is not None:
                    _emit_trading_event(event_fn, trade, elapsed_sec=round(time.monotonic() - trade_start, 3))
            except Exception as exc:
                trace_event("trading_tick_error", error=str(exc))
                if event_fn is not None:
                    event_fn({"event": "trading_tick_error", "error": str(exc)})
                last_trading_ts = time.monotonic()

        # ── Periodic status summary ───────────────────────────────────────────
        if iterations % 10 == 0:
            try:
                summary = supervisor_status_summary(ledger)
                trace_event("supervisor_status", **summary)
                if event_fn is not None:
                    event_fn({"event": "supervisor_heartbeat", **summary})
            except Exception:
                pass

        # ── Sleep ─────────────────────────────────────────────────────────────
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        sleep_fn(min(supervisor_config.loop_sleep_sec, remaining))

    elapsed = time.monotonic() - run_started
    summary = supervisor_status_summary(ledger)

    trace_event(
        "supervisor_exit",
        iterations=iterations,
        runtime_elapsed_sec=round(elapsed, 3),
        total_orders_polled=total_orders_polled,
        total_lots_resolved=total_lots_resolved,
        total_lots_redeemed=total_lots_redeemed,
        **summary,
    )

    return {
        "status": "supervisor_deadline_reached",
        "iterations": iterations,
        "runtime_elapsed_sec": round(elapsed, 3),
        "total_orders_polled": total_orders_polled,
        "total_lots_resolved": total_lots_resolved,
        "total_lots_redeemed": total_lots_redeemed,
        **summary,
    }


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _emit_trading_event(
    event_fn: Callable[[dict[str, Any]], None],
    trade: dict[str, Any],
    *,
    elapsed_sec: float,
) -> None:
    """Emit a concise trading-tick event via *event_fn*.

    Includes decision debug fields (side, notional, stake, reject reason, sizing
    policy) when available so the operator can see why a trade was or wasn't made.
    """
    debug = trade.get("decision_debug") or {}
    event: dict[str, Any] = {
        "event": "trading_tick",
        "status": trade.get("status"),
        "market_id": trade.get("market_id"),
        "market_slug": trade.get("market_slug"),
        "elapsed_sec": elapsed_sec,
    }
    # Live-input missing — surface reason so operator can diagnose
    if trade.get("missing_input_reason"):
        event["missing_input_reason"] = trade["missing_input_reason"]
    if trade.get("missing_components"):
        event["missing_components"] = trade["missing_components"]
    if trade.get("brownian_error"):
        event["brownian_error"] = trade["brownian_error"]
    if trade.get("brownian_convention_found"):
        event["brownian_convention_found"] = trade["brownian_convention_found"]
    if trade.get("hmm_error"):
        event["hmm_error"] = trade["hmm_error"]
    # Dig into live_input_meta for sub-reasons (brownian/hmm errors)
    meta = (trade.get("input") or {}).get("live_input_meta") or {}
    if meta.get("brownian_error") and not trade.get("brownian_error"):
        event["brownian_error"] = meta["brownian_error"]
    if meta.get("hmm_error") and not trade.get("hmm_error"):
        event["hmm_error"] = meta["hmm_error"]
    # Gate-blocked trades
    if trade.get("block_reasons"):
        event["block_reasons"] = trade["block_reasons"]
    # No-trade decisions
    if trade.get("reason") or debug.get("reject_reason"):
        event["reject_reason"] = trade.get("reason") or debug.get("reject_reason")
    # Trade accepted
    if trade.get("side") or debug.get("chosen_side"):
        event["side"] = trade.get("side") or debug.get("chosen_side")
    if trade.get("notional_usd") is not None or debug.get("stake_notional") is not None:
        event["notional_usd"] = trade.get("notional_usd") if trade.get("notional_usd") is not None else debug.get("stake_notional")
    if debug.get("stake_fraction") is not None:
        event["stake_fraction"] = round(float(debug["stake_fraction"]), 6)
    if debug.get("sizing_policy"):
        event["sizing_policy"] = debug["sizing_policy"]
    if debug.get("canary_force_min_notional_applied"):
        event["canary_force_min_notional_applied"] = True
    if debug.get("expected_log_growth") is not None:
        event["expected_log_growth"] = round(float(debug["expected_log_growth"]), 6)
    if debug.get("chosen_edge") is not None:
        event["chosen_edge"] = round(float(debug["chosen_edge"]), 4)
    if debug.get("final_decision"):
        event["final_decision"] = debug["final_decision"]
    if debug.get("bankroll_before") is not None:
        event["bankroll_usd"] = round(float(debug["bankroll_before"]), 4)
    if debug.get("bankroll_source"):
        event["bankroll_source"] = debug["bankroll_source"]
    # Execution outcome
    exec_result = trade.get("execution_result")
    if isinstance(exec_result, dict):
        event["execution_event_type"] = exec_result.get("event_type")
        event["order_id"] = exec_result.get("order_id")
    event_fn(event)


def _opt_float(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
