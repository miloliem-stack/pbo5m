from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from ..time_utils import isoformat_utc, parse_datetime, utc_now
from .btc5m_brownian_conservative import (
    MODEL_ID,
    STRATEGY_ID,
    BrownianConservativeConfig,
    expected_log_growth,
    move_probability_toward_half,
)


REQUIRED_INTENT_FIELDS = (
    "strategy_id",
    "side",
    "model_probability",
    "edge",
    "expected_log_growth",
    "market_age_seconds",
    "seconds_to_expiry",
    "bankroll_before",
    "stake_fraction",
)


@dataclass(frozen=True)
class BrownianOrderValidationInput:
    order_intent: dict[str, Any]
    decision_row: dict[str, Any]
    current_market_snapshot: dict[str, Any]
    bankroll: float
    already_traded_market: bool
    paper_only: bool
    live_enabled: bool
    config: BrownianConservativeConfig
    now_ts: Any


@dataclass(frozen=True)
class BrownianOrderValidationResult:
    accepted: bool
    reject_reason: Optional[str]
    normalized_order_intent: Optional[dict[str, Any]]
    validation_debug: dict[str, Any]


def validate_brownian_order_intent(input: BrownianOrderValidationInput) -> BrownianOrderValidationResult:
    intent = dict(input.order_intent or {})
    decision = dict(input.decision_row or {})
    snapshot = dict(input.current_market_snapshot or {})
    cfg = input.config
    now = parse_datetime(input.now_ts) or utc_now()

    debug: dict[str, Any] = {
        "strategy_id": _value(intent, decision, "strategy_id"),
        "model_id": _value(intent, decision, "model_id"),
        "paper_only": bool(input.paper_only),
        "live_enabled": bool(input.live_enabled),
        "bankroll": _float(input.bankroll),
    }

    missing = _missing_required(intent, decision)
    if missing:
        return _reject("missing_required_fields", debug | {"missing_fields": missing})
    if _value(intent, decision, "strategy_id") != STRATEGY_ID:
        return _reject("wrong_strategy_id", debug)
    if _value(intent, decision, "model_id") != MODEL_ID:
        return _reject("wrong_model_id", debug)
    if not input.paper_only and not input.live_enabled:
        return _reject("live_not_enabled", debug)

    market_id = _value(intent, decision, "market_id")
    market_slug = _value(intent, decision, "market_slug", "slug")
    condition_id = _value(intent, decision, "condition_id")
    if not market_id and not market_slug:
        return _reject("missing_market_identity", debug)
    if not _market_matches(intent, decision, snapshot):
        return _reject("market_identity_mismatch", debug | {"snapshot_market_id": snapshot.get("market_id"), "snapshot_slug": snapshot.get("slug") or snapshot.get("market_slug")})

    side = str(_value(intent, decision, "side", "selected_side") or "").upper()
    if side not in {"YES", "NO"}:
        return _reject("invalid_side", debug | {"side": side})
    token_id = _token_id_for_side(intent, decision, snapshot, side)
    if _value(intent, decision, "token_id") and token_id and str(_value(intent, decision, "token_id")) != str(token_id):
        return _reject("token_side_mismatch", debug | {"side": side, "expected_token_id": token_id, "intent_token_id": _value(intent, decision, "token_id")})
    if not _is_tradable(snapshot):
        return _reject("market_not_tradable", debug)
    if input.already_traded_market:
        return _reject("already_traded_market", debug)

    market_age = _float(_value(intent, decision, "market_age_seconds", "market_age_sec"))
    seconds_to_expiry = _float(_value(intent, decision, "seconds_to_expiry"))
    if market_age is None or market_age < cfg.min_market_age_seconds:
        return _reject("market_too_young", debug | {"market_age_seconds": market_age})
    if market_age >= cfg.max_market_age_seconds:
        return _reject("market_too_old", debug | {"market_age_seconds": market_age})
    if seconds_to_expiry is None or seconds_to_expiry <= 0:
        return _reject("market_expired", debug | {"seconds_to_expiry": seconds_to_expiry})

    decision_ts = parse_datetime(_value(intent, decision, "decision_ts", "timestamp", "generated_ts"))
    if decision_ts is None:
        return _reject("decision_timestamp_missing", debug)
    decision_age = (now - decision_ts).total_seconds()
    if decision_age < -1.0 or decision_age > cfg.max_decision_staleness_seconds:
        return _reject("decision_stale", debug | {"decision_age_seconds": decision_age})

    start = parse_datetime(_value(snapshot, intent, decision, "market_start_ts", "start_ts", "start_time"))
    if start is not None:
        recomputed_age = (now - start).total_seconds()
        debug["market_age_seconds_recomputed"] = recomputed_age
        if recomputed_age < cfg.min_market_age_seconds:
            return _reject("market_too_young", debug | {"market_age_seconds": recomputed_age})
        if recomputed_age >= cfg.max_market_age_seconds:
            return _reject("market_too_old", debug | {"market_age_seconds": recomputed_age})

    intended_ask = _float(_value(intent, decision, "intended_ask", "limit_price", "selected_ask", "chosen_ask"))
    if intended_ask is None or not _finite_between(intended_ask, 0.0, 1.0):
        return _reject("invalid_intended_ask", debug | {"intended_ask": intended_ask})
    current_ask = _current_ask(snapshot, side)
    if current_ask is None:
        return _reject("current_ask_missing", debug | {"side": side})
    if current_ask > intended_ask + cfg.ask_slippage_abs:
        return _reject("current_ask_above_slippage", debug | {"intended_ask": intended_ask, "current_ask": current_ask, "allowed_slippage_abs": cfg.ask_slippage_abs})
    if current_ask <= cfg.min_ask:
        return _reject("ask_below_min", debug | {"current_ask": current_ask, "min_ask": cfg.min_ask})
    if snapshot.get("valid_topbook") is False:
        return _reject("invalid_topbook", debug)
    depth_cap = _current_depth_cap(snapshot, intent, decision, side, cfg.top_n_levels)
    if depth_cap is None or depth_cap <= 0:
        return _reject("missing_depth", debug | {"depth_cap": depth_cap})

    model_probability = _float(_value(intent, decision, "model_probability", "chosen_probability"))
    intended_edge = _float(_value(intent, decision, "edge", "selected_edge", "chosen_edge"))
    intended_growth = _float(_value(intent, decision, "expected_log_growth"))
    if model_probability is None or not _finite_between(model_probability, 0.0, 1.0, inclusive=True):
        return _reject("invalid_model_probability", debug | {"model_probability": model_probability})
    if intended_edge is None or intended_edge < cfg.edge_threshold:
        return _reject("edge_below_threshold", debug | {"edge": intended_edge})
    if intended_growth is None or not (intended_growth > cfg.min_expected_log_growth):
        return _reject("expected_growth_not_positive", debug | {"expected_log_growth": intended_growth})
    edge_current = model_probability - current_ask
    if edge_current < cfg.edge_threshold:
        return _reject("edge_below_threshold_current", debug | {"edge_using_current_ask": edge_current, "current_ask": current_ask})

    bankroll = _float(input.bankroll)
    stake = _float(_value(intent, decision, "notional_usd", "stake_notional", "stake_usd"))
    if bankroll is None or bankroll <= 0:
        return _reject("invalid_bankroll", debug | {"bankroll": bankroll})
    if stake is None or stake <= 0 or not math.isfinite(stake):
        return _reject("invalid_stake", debug | {"stake": stake})
    if stake > bankroll:
        return _reject("stake_above_bankroll", debug | {"stake": stake, "bankroll": bankroll})
    max_fraction = cfg.normal_max_stake_fraction
    if stake > max_fraction * bankroll + 1e-9:
        return _reject("stake_above_max_fraction", debug | {"stake": stake, "bankroll": bankroll, "max_stake_fraction": max_fraction})
    if bankroll < cfg.small_wallet_threshold and stake < cfg.min_order_notional:
        return _reject("below_min_order_notional", debug | {"stake": stake, "min_order_notional": cfg.min_order_notional, "bankroll": bankroll})
    if bankroll >= cfg.small_wallet_threshold and cfg.min_order_notional > max_fraction * bankroll + 1e-9 and stake <= cfg.min_order_notional:
        return _reject("below_min_order_notional", debug | {"stake": stake, "min_order_notional": cfg.min_order_notional, "bankroll": bankroll})
    if stake > depth_cap + 1e-9:
        return _reject("stake_above_depth", debug | {"stake": stake, "top10_depth_cap": depth_cap})

    stake_fraction = stake / bankroll
    reported_fraction = _float(_value(intent, decision, "stake_fraction"))
    p_growth = move_probability_toward_half(model_probability, cfg.probability_haircut_abs)
    ask_growth = min(0.99, current_ask + cfg.ask_slippage_abs)
    growth_current = expected_log_growth(p_growth, ask_growth, stake_fraction)
    if not (growth_current > cfg.min_expected_log_growth):
        return _reject("expected_growth_not_positive_current", debug | {"expected_log_growth_recomputed": growth_current})
    depth_utilization = stake / depth_cap
    if depth_utilization > cfg.max_depth_utilization + 1e-9:
        return _reject("depth_utilization_too_high", debug | {"depth_utilization": depth_utilization, "max_depth_utilization": cfg.max_depth_utilization})

    validation_debug = {
        **debug,
        "market_id": market_id,
        "market_slug": market_slug,
        "condition_id": condition_id,
        "market_start_ts": _value(snapshot, intent, decision, "market_start_ts", "start_ts", "start_time"),
        "decision_ts": _value(intent, decision, "decision_ts", "timestamp", "generated_ts"),
        "quote_ts": _value(snapshot, intent, decision, "quote_ts"),
        "quote_age_ms": _float(_value(snapshot, intent, decision, "quote_age_ms")),
        "side": side,
        "token_id": token_id,
        "yes_token_id": _value(snapshot, intent, decision, "yes_token_id"),
        "no_token_id": _value(snapshot, intent, decision, "no_token_id"),
        "intended_ask": intended_ask,
        "current_ask": current_ask,
        "allowed_slippage_abs": cfg.ask_slippage_abs,
        "model_probability": model_probability,
        "edge_using_intended_ask": intended_edge,
        "edge_using_current_ask": edge_current,
        "expected_log_growth_intended": intended_growth,
        "expected_log_growth_recomputed": growth_current,
        "intended_stake": stake,
        "validated_stake": stake,
        "bankroll": bankroll,
        "stake_fraction_reported": reported_fraction,
        "stake_fraction_recomputed": stake_fraction,
        "max_stake_fraction": max_fraction,
        "small_wallet_mode_active": bankroll < cfg.small_wallet_threshold,
        "min_order_notional": cfg.min_order_notional,
        "top10_depth_cap": depth_cap,
        "depth_utilization": depth_utilization,
        "already_traded_market": bool(input.already_traded_market),
        "decision_age_seconds": decision_age,
        "market_age_seconds": market_age,
        "seconds_to_expiry": seconds_to_expiry,
    }
    normalized = {
        "strategy_id": STRATEGY_ID,
        "market_id": market_id,
        "condition_id": condition_id,
        "market_slug": market_slug,
        "market_start_ts": validation_debug.get("market_start_ts"),
        "decision_ts": validation_debug.get("decision_ts"),
        "quote_ts": validation_debug.get("quote_ts"),
        "quote_age_ms": validation_debug.get("quote_age_ms"),
        "side": side,
        "selected_side": side,
        "action": "BUY",
        "notional_usd": stake,
        "stake_usd": stake,
        "limit_price": current_ask,
        "intended_ask": intended_ask,
        "max_slippage_abs": cfg.ask_slippage_abs,
        "token_id": token_id,
        "model_id": MODEL_ID,
        "model_probability": model_probability,
        "edge": edge_current,
        "expected_log_growth_recomputed": growth_current,
        "stake_fraction_recomputed": stake_fraction,
        "depth_utilization_recomputed": depth_utilization,
        "paper_only": bool(input.paper_only),
        "executable_live": bool((not input.paper_only) and input.live_enabled),
        "validation_timestamp": isoformat_utc(now),
        "validation_id": _validation_id(intent, validation_debug),
        "validation_debug": validation_debug,
    }
    return BrownianOrderValidationResult(True, None, normalized, validation_debug)


def validate_and_log_brownian_order_intent(input: BrownianOrderValidationInput, path: str | Path | None = None) -> BrownianOrderValidationResult:
    result = validate_brownian_order_intent(input)
    write_validation_log_row(path or input.config.validation_log_path, validation_log_row(input, result))
    return result


def validation_log_row(input: BrownianOrderValidationInput, result: BrownianOrderValidationResult) -> dict[str, Any]:
    debug = result.validation_debug
    return {
        "timestamp": isoformat_utc(parse_datetime(input.now_ts) or utc_now()),
        "strategy_id": debug.get("strategy_id"),
        "market_id": debug.get("market_id") or _value(input.order_intent, input.decision_row, "market_id"),
        "market_slug": debug.get("market_slug") or _value(input.order_intent, input.decision_row, "market_slug", "slug"),
        "side": debug.get("side") or _value(input.order_intent, input.decision_row, "side", "selected_side"),
        "accepted": result.accepted,
        "reject_reason": result.reject_reason,
        "paper_only": input.paper_only,
        "executable_live": bool(result.normalized_order_intent and result.normalized_order_intent.get("executable_live")),
        "intended_ask": debug.get("intended_ask"),
        "current_ask": debug.get("current_ask"),
        "allowed_slippage_abs": debug.get("allowed_slippage_abs"),
        "intended_stake": debug.get("intended_stake"),
        "validated_stake": debug.get("validated_stake"),
        "bankroll": debug.get("bankroll"),
        "stake_fraction": debug.get("stake_fraction_recomputed"),
        "max_stake_fraction": debug.get("max_stake_fraction"),
        "small_wallet_mode_active": debug.get("small_wallet_mode_active"),
        "min_order_notional": debug.get("min_order_notional"),
        "top10_depth_cap": debug.get("top10_depth_cap"),
        "depth_utilization": debug.get("depth_utilization"),
        "model_probability": debug.get("model_probability"),
        "edge_using_intended_ask": debug.get("edge_using_intended_ask"),
        "edge_using_current_ask": debug.get("edge_using_current_ask"),
        "expected_log_growth_intended": debug.get("expected_log_growth_intended"),
        "expected_log_growth_recomputed": debug.get("expected_log_growth_recomputed"),
        "already_traded_market": input.already_traded_market,
        "decision_age_seconds": debug.get("decision_age_seconds"),
        "market_age_seconds": debug.get("market_age_seconds"),
        "seconds_to_expiry": debug.get("seconds_to_expiry"),
    }


def write_validation_log_row(path: str | Path, row: dict[str, Any]) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(row, sort_keys=True, default=str) + "\n")


def _reject(reason: str, debug: dict[str, Any]) -> BrownianOrderValidationResult:
    return BrownianOrderValidationResult(False, reason, None, debug)


def _missing_required(intent: dict[str, Any], decision: dict[str, Any]) -> list[str]:
    missing = [field for field in REQUIRED_INTENT_FIELDS if _value(intent, decision, field) in (None, "")]
    if not (_value(intent, decision, "market_id") or _value(intent, decision, "market_slug", "slug")):
        missing.append("market_id_or_market_slug")
    if not (_value(intent, decision, "notional_usd", "stake_notional", "stake_usd")):
        missing.append("notional_usd_or_stake_notional")
    if not (_value(intent, decision, "limit_price", "intended_ask", "selected_ask", "chosen_ask")):
        missing.append("limit_price_or_intended_ask")
    if not (_value(intent, decision, "top10_depth_cap", "executable_depth_cap")):
        missing.append("top10_depth_cap_or_executable_depth_cap")
    return missing


def _market_matches(intent: dict[str, Any], decision: dict[str, Any], snapshot: dict[str, Any]) -> bool:
    intent_market = _value(intent, decision, "market_id")
    snapshot_market = _value(snapshot, "market_id")
    if intent_market and snapshot_market and str(intent_market) != str(snapshot_market):
        return False
    intent_slug = _value(intent, decision, "market_slug", "slug")
    snapshot_slug = _value(snapshot, "market_slug", "slug")
    if intent_slug and snapshot_slug and str(intent_slug) != str(snapshot_slug):
        return False
    intent_condition = _value(intent, decision, "condition_id")
    snapshot_condition = _value(snapshot, "condition_id")
    if intent_condition and snapshot_condition and str(intent_condition) != str(snapshot_condition):
        return False
    return bool(intent_market or intent_slug)


def _is_tradable(snapshot: dict[str, Any]) -> bool:
    if snapshot.get("closed") is True:
        return False
    if snapshot.get("is_open") is False:
        return False
    if snapshot.get("tradable") is False:
        return False
    return True


def _current_ask(snapshot: dict[str, Any], side: str) -> Optional[float]:
    side_key = side.lower()
    return _float(_value(snapshot, f"{side_key}_ask", f"executable_{side_key}_ask", f"current_{side_key}_ask"))


def _current_depth_cap(snapshot: dict[str, Any], intent: dict[str, Any], decision: dict[str, Any], side: str, top_n: int) -> Optional[float]:
    side_key = side.lower()
    explicit = _float(_value(snapshot, f"{side_key}_top10_depth_cap", f"{side_key}_depth_cap", f"executable_{side_key}_depth_cap"))
    if explicit is not None:
        return explicit
    levels = _value(snapshot, f"{side_key}_asks", f"{side_key}_ask_levels", f"{side_key}_book") or []
    cap = 0.0
    if isinstance(levels, list):
        for level in levels[:top_n]:
            if isinstance(level, dict):
                px = _float(_value(level, "price", "ask", "px"))
                size = _float(_value(level, "size", "shares", "sz"))
            else:
                px = _float(level[0]) if len(level) > 0 else None
                size = _float(level[1]) if len(level) > 1 else None
            if px and size:
                cap += px * size
    if cap > 0:
        return cap
    return _float(_value(intent, decision, "top10_depth_cap", "executable_depth_cap"))


def _token_id_for_side(intent: dict[str, Any], decision: dict[str, Any], snapshot: dict[str, Any], side: str) -> Optional[Any]:
    side_key = side.lower()
    return _value(snapshot, intent, decision, f"{side_key}_token_id")


def _validation_id(intent: dict[str, Any], debug: dict[str, Any]) -> str:
    payload = json.dumps(
        {
            "strategy_id": STRATEGY_ID,
            "market_id": debug.get("market_id"),
            "market_slug": debug.get("market_slug"),
            "side": debug.get("side"),
            "stake": debug.get("validated_stake"),
            "ask": debug.get("current_ask"),
            "decision_ts": intent.get("decision_ts"),
        },
        sort_keys=True,
        default=str,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:24]


def _value(*items: Any) -> Any:
    mappings: list[dict[str, Any]] = []
    key_list: list[str] = []
    for item in items:
        if isinstance(item, dict):
            mappings.append(item)
        elif isinstance(item, (list, tuple)):
            key_list.extend(str(k) for k in item)
        else:
            key_list.append(str(item))
    for mapping in mappings:
        for key in key_list:
            if mapping.get(key) not in (None, ""):
                return mapping.get(key)
    return None


def _float(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if math.isfinite(out) else None


def _finite_between(value: float, low: float, high: float, *, inclusive: bool = False) -> bool:
    if not math.isfinite(value):
        return False
    if inclusive:
        return low <= value <= high
    return low < value < high
