from __future__ import annotations

import hashlib
import json
import math
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from ..time_utils import isoformat_utc, parse_datetime, utc_now


STRATEGY_ID = "brownian_no_hmm_conservative_v1"
MODEL_ID = "brownian_zero_drift__rv30"
DECISION_LOG = Path("artifacts/live_strategy_decisions/brownian_no_hmm_conservative_v1/decision_state.jsonl")
VALIDATION_LOG = Path("artifacts/live_strategy_decisions/brownian_no_hmm_conservative_v1/order_validation.jsonl")
BLOCKING_STATUSES = frozenset({"accepted", "booked", "filled", "open", "partially_filled", "pending", "submitted"})


@dataclass(frozen=True)
class BrownianConservativeConfig:
    strategy_id: str = STRATEGY_ID
    model_id: str = MODEL_ID
    enabled: bool = False
    paper_only: bool = True
    live_enabled: bool = False
    min_market_age_seconds: float = 60.0
    max_market_age_seconds: float = 240.0
    edge_threshold: float = 0.02
    min_ask: float = 0.30
    max_ask: float = 0.99
    probability_haircut_abs: float = 0.02
    ask_slippage_abs: float = 0.01
    min_expected_log_growth: float = 0.0
    top_n_levels: int = 10
    kelly_multiplier: float = 1.0 / 40.0
    normal_max_stake_fraction: float = 0.0025
    max_depth_utilization: float = 1.0
    small_wallet_threshold: float = 400.0
    small_wallet_max_stake_fraction: float = 0.0025
    min_order_notional: float = 1.0
    min_market_buy_notional_usd: float = 1.0
    min_limit_buy_size_shares: Optional[float] = None
    venue_min_discovery_mode: str = "static"
    skip_below_min_order: bool = True
    one_entry_per_market: bool = True
    daily_stop_loss_fraction: float = 0.03
    session_stop_loss_fraction: float = 0.08
    max_decision_staleness_seconds: float = 3.0
    decision_log_path: Path = DECISION_LOG
    validation_log_path: Path = VALIDATION_LOG
    live_one_shot: bool = True
    canary_force_min_notional_enabled: bool = False
    canary_force_min_notional_usd: float = 1.0
    canary_force_max_wallet_usd: float = 50.0
    canary_force_max_stake_fraction: float = 0.10
    canary_force_live_only: bool = True
    canary_force_require_one_shot: bool = True

    @classmethod
    def from_env(cls, env: Optional[dict[str, str]] = None) -> "BrownianConservativeConfig":
        source = env if env is not None else os.environ
        strategy_id = source.get("BTC5M_STRATEGY_ID", STRATEGY_ID)
        if strategy_id != STRATEGY_ID:
            raise ValueError(f"unsupported BTC5M_STRATEGY_ID={strategy_id!r}")
        normal_max_stake_fraction = float(source.get("BTC5M_BROWNIAN_MAX_STAKE_FRACTION", "0.0025"))
        min_market_buy_notional = float(
            source.get("BTC5M_BROWNIAN_MIN_ORDER_NOTIONAL", source.get("BTC5M_BROWNIAN_MIN_MARKET_BUY_NOTIONAL_USD", "1"))
        )
        small_wallet_threshold = min_market_buy_notional / normal_max_stake_fraction if normal_max_stake_fraction > 0 else float("inf")
        if (
            "BTC5M_BROWNIAN_MIN_ORDER_NOTIONAL" not in source
            and "BTC5M_BROWNIAN_MIN_MARKET_BUY_NOTIONAL_USD" not in source
            and "BTC5M_BROWNIAN_SMALL_WALLET_THRESHOLD" in source
        ):
            small_wallet_threshold = float(source["BTC5M_BROWNIAN_SMALL_WALLET_THRESHOLD"])
        return cls(
            strategy_id=strategy_id,
            enabled=_env_bool(source.get("BTC5M_BROWNIAN_ENABLED", "false")),
            paper_only=_env_bool(source.get("BTC5M_BROWNIAN_PAPER_ONLY", "true")),
            live_enabled=_env_bool(source.get("BTC5M_BROWNIAN_LIVE_ENABLED", "false")),
            min_market_age_seconds=float(source.get("BTC5M_BROWNIAN_MIN_MARKET_AGE_SECONDS", "60")),
            max_market_age_seconds=float(source.get("BTC5M_BROWNIAN_MAX_MARKET_AGE_SECONDS", "240")),
            edge_threshold=float(source.get("BTC5M_BROWNIAN_EDGE_THRESHOLD", "0.02")),
            min_ask=float(source.get("BTC5M_BROWNIAN_MIN_ASK", "0.30")),
            probability_haircut_abs=float(source.get("BTC5M_BROWNIAN_PROBABILITY_HAIRCUT_ABS", "0.02")),
            ask_slippage_abs=float(source.get("BTC5M_BROWNIAN_ASK_SLIPPAGE_ABS", "0.01")),
            kelly_multiplier=float(source.get("BTC5M_BROWNIAN_KELLY_MULTIPLIER", str(1.0 / 40.0))),
            normal_max_stake_fraction=normal_max_stake_fraction,
            max_depth_utilization=float(source.get("BTC5M_BROWNIAN_MAX_DEPTH_UTILIZATION", "1.0")),
            small_wallet_threshold=small_wallet_threshold,
            small_wallet_max_stake_fraction=float(source.get("BTC5M_BROWNIAN_SMALL_WALLET_MAX_STAKE_FRACTION", source.get("BTC5M_BROWNIAN_MAX_STAKE_FRACTION", "0.0025"))),
            min_order_notional=min_market_buy_notional,
            min_market_buy_notional_usd=min_market_buy_notional,
            min_limit_buy_size_shares=_float(source.get("BTC5M_BROWNIAN_MIN_LIMIT_BUY_SIZE_SHARES")),
            venue_min_discovery_mode=source.get("BTC5M_BROWNIAN_VENUE_MIN_DISCOVERY_MODE", "static"),
            top_n_levels=int(source.get("BTC5M_BROWNIAN_TOP_N_LEVELS", "10")),
            max_decision_staleness_seconds=float(source.get("BTC5M_BROWNIAN_MAX_DECISION_STALENESS_SECONDS", "3.0")),
            decision_log_path=Path(source.get("BTC5M_BROWNIAN_DECISION_LOG", str(DECISION_LOG))),
            validation_log_path=Path(source.get("BTC5M_BROWNIAN_VALIDATION_LOG", str(VALIDATION_LOG))),
            live_one_shot=_env_bool(source.get("BTC5M_LIVE_ONE_SHOT", "true")),
            canary_force_min_notional_enabled=_env_bool(source.get("BTC5M_BROWNIAN_CANARY_FORCE_MIN_NOTIONAL_ENABLED", "false")),
            canary_force_min_notional_usd=float(source.get("BTC5M_BROWNIAN_CANARY_FORCE_MIN_NOTIONAL_USD", "1.0")),
            canary_force_max_wallet_usd=float(source.get("BTC5M_BROWNIAN_CANARY_FORCE_MAX_WALLET_USD", "50.0")),
            canary_force_max_stake_fraction=float(source.get("BTC5M_BROWNIAN_CANARY_FORCE_MAX_STAKE_FRACTION", "0.10")),
            canary_force_live_only=_env_bool(source.get("BTC5M_BROWNIAN_CANARY_FORCE_LIVE_ONLY", "true")),
            canary_force_require_one_shot=_env_bool(source.get("BTC5M_BROWNIAN_CANARY_FORCE_REQUIRE_ONE_SHOT", "true")),
        )

    def config_hash(self) -> str:
        payload = {k: str(v) for k, v in self.__dict__.items() if k not in {"decision_log_path", "validation_log_path"}}
        return hashlib.sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()[:16]


def normal_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(float(x) / math.sqrt(2.0)))


def brownian_zero_drift_p_yes(reference_price: float, current_price: float, sigma: float, seconds_to_expiry: float) -> float:
    if reference_price is None or current_price is None:
        raise ValueError("missing price")
    sigma_eff = min(max(float(sigma), 1e-5), 0.05)
    tau_minutes = max(float(seconds_to_expiry) / 60.0, 1e-9)
    z = math.log(float(current_price) / float(reference_price)) / (sigma_eff * math.sqrt(tau_minutes))
    return min(max(normal_cdf(z), 1e-6), 1.0 - 1e-6)


def full_kelly_fraction_binary_contract(probability: float, ask: float) -> float:
    if ask >= 1.0:
        return 0.0
    return max(0.0, (float(probability) - float(ask)) / (1.0 - float(ask)))


def move_probability_toward_half(probability: float, haircut_abs: float) -> float:
    probability = float(probability)
    haircut_abs = float(haircut_abs)
    if probability >= 0.5:
        return max(0.5, probability - haircut_abs)
    return min(0.5, probability + haircut_abs)


def expected_log_growth(probability: float, ask: float, fraction: float) -> float:
    probability = float(probability)
    ask = float(ask)
    fraction = float(fraction)
    if ask <= 0.0 or ask >= 1.0 or fraction <= 0.0 or fraction >= 1.0:
        return 0.0 if fraction == 0.0 else float("nan")
    return float(probability * math.log(1.0 + fraction * ((1.0 - ask) / ask)) + (1.0 - probability) * math.log(1.0 - fraction))


def compute_conservative_stake(
    *,
    bankroll: float,
    probability: float,
    ask: float,
    depth_cap: float,
    config: BrownianConservativeConfig,
) -> dict[str, Any]:
    bankroll = float(bankroll)
    active_max_fraction = config.small_wallet_max_stake_fraction if bankroll < config.small_wallet_threshold else config.normal_max_stake_fraction
    full = full_kelly_fraction_binary_contract(probability, ask)
    raw_fraction = config.kelly_multiplier * full
    stake_fraction = min(max(raw_fraction, 0.0), active_max_fraction)
    stake = bankroll * stake_fraction
    rounded_to_min_order = False
    if stake < config.min_order_notional:
        if config.skip_below_min_order and bankroll < config.small_wallet_threshold:
            return _try_canary_override(full, depth_cap, bankroll, config)
        if config.min_order_notional <= active_max_fraction * bankroll:
            stake = config.min_order_notional
            stake_fraction = stake / bankroll if bankroll else 0.0
            rounded_to_min_order = True
        else:
            return _try_canary_override(full, depth_cap, bankroll, config)
    capacity_bound = math.isfinite(depth_cap) and stake > depth_cap
    if capacity_bound:
        stake = depth_cap
        stake_fraction = stake / bankroll if bankroll else 0.0
    if stake <= 0:
        return _stake_result(stake, stake_fraction, full, depth_cap, capacity_bound, "insufficient_depth", bankroll)
    return {
        **_stake_result(stake, stake_fraction, full, depth_cap, capacity_bound, None, bankroll),
        "rounded_to_min_order": rounded_to_min_order,
    }


def decide_brownian_conservative(
    *,
    market: dict[str, Any],
    quote: dict[str, Any],
    price_state: dict[str, Any],
    risk_state: Optional[dict[str, Any]] = None,
    config: Optional[BrownianConservativeConfig] = None,
    decision_ts: Optional[Any] = None,
) -> dict[str, Any]:
    cfg = config or BrownianConservativeConfig.from_env()
    risk = risk_state or {}
    now = parse_datetime(decision_ts) or utc_now()
    market_start = parse_datetime(_first(market, "market_start_ts", "start_ts", "start_time"))
    market_end = parse_datetime(_first(market, "market_end_ts", "end_ts", "end_time"))
    market_age = _float(_first(market, "market_age_seconds", "market_age_sec"))
    if market_age is None and market_start is not None:
        market_age = (now - market_start).total_seconds()
    seconds_to_expiry = _float(market.get("seconds_to_expiry"))
    if seconds_to_expiry is None and market_end is not None:
        seconds_to_expiry = (market_end - now).total_seconds()

    row = _base_row(cfg, market, quote, price_state, now, market_start, market_end, market_age, seconds_to_expiry, risk)
    reason = _precheck_reason(cfg, market_age, quote, price_state, risk, row)
    if reason is None:
        p_yes = brownian_zero_drift_p_yes(row["reference_price"], row["current_price"], row["sigma"], row["seconds_to_expiry"])
        row["p_yes"] = p_yes
        row["p_no"] = 1.0 - p_yes
        yes = _candidate("YES", p_yes, row["yes_ask"], cfg)
        no = _candidate("NO", 1.0 - p_yes, row["no_ask"], cfg)
        row["yes_edge"] = yes["edge"]
        row["no_edge"] = no["edge"]
        passing = [c for c in (yes, no) if c["passes"]]
        if not passing:
            reason = _side_reject_reason(yes, no)
        else:
            chosen = max(passing, key=lambda c: c["edge"])
            row.update({"chosen_side": chosen["side"], "chosen_ask": chosen["ask"], "chosen_probability": chosen["probability"], "chosen_edge": chosen["edge"]})
            depth_cap = _top_depth_cap(quote, chosen["side"], cfg.top_n_levels)
            row["top10_depth_cap"] = depth_cap
            stake = compute_conservative_stake(bankroll=row["bankroll_before"], probability=chosen["probability"], ask=chosen["ask"], depth_cap=depth_cap, config=cfg)
            row.update(stake)
            p_growth = move_probability_toward_half(chosen["probability"], cfg.probability_haircut_abs)
            ask_growth = min(0.99, chosen["ask"] + cfg.ask_slippage_abs)
            growth = expected_log_growth(p_growth, ask_growth, row["stake_fraction"])
            row["expected_log_growth"] = growth
            if stake.get("reject_reason"):
                reason = stake["reject_reason"]
            elif not (growth > cfg.min_expected_log_growth):
                reason = "expected_growth_not_positive"
            else:
                row["should_trade"] = True
                row["final_decision"] = "PAPER_BUY" if cfg.paper_only else ("BUY_YES" if chosen["side"] == "YES" else "BUY_NO")
                row["order_intent_id"] = _intent_id(row)
                row["order_intent"] = _order_intent(row)
    if reason is not None:
        row["reject_reason"] = reason
        row["final_decision"] = "NO_TRADE"
    return row


def write_decision_log_row(path: str | Path, row: dict[str, Any]) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(row, sort_keys=True, default=str) + "\n")


def evaluate_and_log_brownian_conservative(**kwargs: Any) -> dict[str, Any]:
    row = decide_brownian_conservative(**kwargs)
    cfg = kwargs.get("config") or BrownianConservativeConfig.from_env()
    write_decision_log_row(cfg.decision_log_path, row)
    return row


def _base_row(cfg: BrownianConservativeConfig, market: dict[str, Any], quote: dict[str, Any], price: dict[str, Any], now: Any, start: Any, end: Any, age: Any, seconds_to_expiry: Any, risk: dict[str, Any]) -> dict[str, Any]:
    yes_ask = _float(_first(quote, "yes_ask", "executable_yes_ask"))
    no_ask = _float(_first(quote, "no_ask", "executable_no_ask"))
    bankroll = _float(_first(risk, "bankroll", "bankroll_before", "current_bankroll")) or 0.0
    return {
        "timestamp": isoformat_utc(now),
        "decision_ts": isoformat_utc(now),
        "market_id": market.get("market_id"),
        "condition_id": market.get("condition_id"),
        "slug": market.get("slug"),
        "yes_token_id": market.get("yes_token_id") or quote.get("yes_token_id"),
        "no_token_id": market.get("no_token_id") or quote.get("no_token_id"),
        "market_start_ts": isoformat_utc(start),
        "market_end_ts": isoformat_utc(end),
        "market_age_seconds": age,
        "seconds_to_expiry": seconds_to_expiry,
        "strategy_id": cfg.strategy_id,
        "policy_id": cfg.strategy_id,
        "model_id": cfg.model_id,
        "reference_price": _float(_first(price, "reference_price", "market_start_price", "K")),
        "current_price": _float(_first(price, "current_price", "S_t", "price")),
        "sigma": _float(_first(price, "sigma", "rv_30m", "volatility")),
        "p_yes": None,
        "p_no": None,
        "yes_ask": yes_ask,
        "no_ask": no_ask,
        "yes_edge": None,
        "no_edge": None,
        "chosen_side": None,
        "chosen_ask": None,
        "chosen_probability": None,
        "chosen_edge": None,
        "probability_haircut_abs": cfg.probability_haircut_abs,
        "ask_slippage_abs": cfg.ask_slippage_abs,
        "expected_log_growth": None,
        "full_kelly_fraction": 0.0,
        "applied_kelly_multiplier": cfg.kelly_multiplier,
        "stake_fraction": 0.0,
        "stake_notional": 0.0,
        "min_order_notional": cfg.min_order_notional,
        "top10_depth_cap": None,
        "depth_utilization": None,
        "bankroll_before": bankroll,
        "small_wallet_mode_active": bankroll < cfg.small_wallet_threshold,
        "should_trade": False,
        "reject_reason": None,
        "order_intent_id": None,
        "valid_topbook": bool(quote.get("valid_topbook", yes_ask is not None and no_ask is not None)),
        "config_hash": cfg.config_hash(),
        "paper_only": cfg.paper_only,
    }


def _precheck_reason(cfg: BrownianConservativeConfig, age: Optional[float], quote: dict[str, Any], price: dict[str, Any], risk: dict[str, Any], row: dict[str, Any]) -> Optional[str]:
    if cfg.one_entry_per_market and _already_traded(risk):
        return "already_traded_market"
    if age is None:
        return "market_too_young"
    if age < cfg.min_market_age_seconds:
        return "market_too_young"
    if age >= cfg.max_market_age_seconds:
        return "market_too_old"
    if row["reference_price"] is None:
        return "missing_reference_price"
    if row["current_price"] is None:
        return "missing_current_price"
    if row["sigma"] is None or row["sigma"] <= 0:
        return "missing_or_invalid_sigma"
    if not row["valid_topbook"] or row["yes_ask"] is None or row["no_ask"] is None:
        return "invalid_topbook"
    if _float(risk.get("daily_pnl")) is not None and _float(risk.get("day_start_bankroll")):
        if _float(risk["daily_pnl"]) <= -cfg.daily_stop_loss_fraction * _float(risk["day_start_bankroll"]):
            return "daily_stop_loss_guard"
    if _float(risk.get("bankroll")) is not None and _float(risk.get("session_start_bankroll")):
        if _float(risk["bankroll"]) <= (1.0 - cfg.session_stop_loss_fraction) * _float(risk["session_start_bankroll"]):
            return "session_stop_loss_guard"
    return None


def _candidate(side: str, probability: float, ask: Optional[float], cfg: BrownianConservativeConfig) -> dict[str, Any]:
    edge = probability - ask if ask is not None else None
    passes = ask is not None and ask > cfg.min_ask and ask < cfg.max_ask and edge is not None and edge >= cfg.edge_threshold
    return {"side": side, "probability": probability, "ask": ask, "edge": edge, "passes": passes}


def _side_reject_reason(yes: dict[str, Any], no: dict[str, Any]) -> str:
    asks = [yes.get("ask"), no.get("ask")]
    if any(a is not None and a <= 0.30 for a in asks):
        return "ask_below_min"
    edges = [e for e in [yes.get("edge"), no.get("edge")] if e is not None]
    if edges and max(edges) < 0.02:
        return "edge_below_threshold"
    return "no_side_passed_edge_and_ask_gates"


def _top_depth_cap(quote: dict[str, Any], side: str, top_n: int) -> float:
    side_key = side.lower()
    levels = quote.get(f"{side_key}_asks") or quote.get(f"{side_key}_ask_levels") or quote.get(f"{side_key}_book") or []
    cap = 0.0
    if isinstance(levels, list):
        for level in levels[:top_n]:
            if isinstance(level, dict):
                px = _float(_first(level, "price", "ask", "px"))
                size = _float(_first(level, "size", "shares", "sz"))
            else:
                px = _float(level[0]) if len(level) > 0 else None
                size = _float(level[1]) if len(level) > 1 else None
            if px and size:
                cap += px * size
    explicit = _float(quote.get(f"{side_key}_top10_depth_cap") or quote.get(f"{side_key}_depth_cap"))
    return explicit if explicit is not None else cap


def _stake_result(stake: float, fraction: float, full: float, depth_cap: float, capacity_bound: bool, reason: Optional[str], bankroll: float, *, canary_force_min_notional_reject_reason: Optional[str] = None) -> dict[str, Any]:
    return {
        "stake_notional": float(stake),
        "stake_fraction": float(fraction),
        "full_kelly_fraction": float(full),
        "capacity_bound": bool(capacity_bound),
        "depth_utilization": float(stake / depth_cap) if depth_cap and math.isfinite(depth_cap) else None,
        "reject_reason": reason,
        "sizing_policy": None,
        "canary_force_min_notional_applied": False,
        "canary_force_min_notional_reason": None,
        "canary_force_min_notional_reject_reason": canary_force_min_notional_reject_reason,
    }


def _try_canary_override(full_kelly: float, depth_cap: float, bankroll: float, config: "BrownianConservativeConfig") -> dict[str, Any]:
    """Attempt the canary_force_min_notional_override when conservative sizing falls below min_order_notional."""

    def _reject_override(reason: str) -> dict[str, Any]:
        return _stake_result(0.0, 0.0, full_kelly, depth_cap, False, "below_min_order_notional", bankroll,
                             canary_force_min_notional_reject_reason=reason)

    if not config.canary_force_min_notional_enabled:
        return _reject_override("override_disabled")
    if config.canary_force_live_only and config.paper_only:
        return _reject_override("paper_mode")
    if config.canary_force_live_only and not config.live_enabled:
        return _reject_override("live_not_enabled")
    if config.canary_force_require_one_shot and not config.live_one_shot:
        return _reject_override("not_live_one_shot")
    if bankroll > config.canary_force_max_wallet_usd:
        return _reject_override("wallet_above_force_max")
    forced = config.canary_force_min_notional_usd
    if forced <= 0:
        return _reject_override("invalid_forced_notional")
    if forced > bankroll * config.canary_force_max_stake_fraction:
        return _reject_override("forced_notional_exceeds_force_max_stake_fraction")
    if math.isfinite(depth_cap) and forced > depth_cap:
        return _reject_override("forced_notional_exceeds_depth_cap")
    if forced < config.min_order_notional:
        return _reject_override("forced_notional_below_venue_floor")
    forced_fraction = forced / bankroll if bankroll else 0.0
    return {
        **_stake_result(forced, forced_fraction, full_kelly, depth_cap, False, None, bankroll),
        "rounded_to_min_order": True,
        "canary_force_min_notional_applied": True,
        "canary_force_min_notional_reason": "tiny_wallet_live_canary_plumbing_test",
        "sizing_policy": "canary_force_min_notional_override",
    }


def _already_traded(risk: dict[str, Any]) -> bool:
    if risk.get("already_traded_market") or risk.get("has_blocking_entry") or risk.get("had_prior_entry"):
        return True
    for order in risk.get("orders", []) or []:
        if str(order.get("status", "")).lower() in BLOCKING_STATUSES:
            return True
    return False


def _intent_id(row: dict[str, Any]) -> str:
    payload = "|".join(str(row.get(k) or "") for k in ["strategy_id", "condition_id", "market_id", "chosen_side", "market_start_ts"])
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:24]


def _order_intent(row: dict[str, Any]) -> dict[str, Any]:
    side = row["chosen_side"]
    token_id = row.get("yes_token_id") if side == "YES" else row.get("no_token_id")
    return {
        "policy_id": row["strategy_id"],
        "strategy_id": row["strategy_id"],
        "model_id": row["model_id"],
        "market_id": row.get("market_id"),
        "condition_id": row.get("condition_id"),
        "market_slug": row.get("slug"),
        "market_start_ts": row.get("market_start_ts"),
        "market_end_ts": row.get("market_end_ts"),
        "token_id": token_id,
        "yes_token_id": row.get("yes_token_id"),
        "no_token_id": row.get("no_token_id"),
        "side": side,
        "selected_side": side,
        "action": "BUY",
        "intended_ask": row["chosen_ask"],
        "limit_price": row["chosen_ask"],
        "selected_ask": row["chosen_ask"],
        "model_probability": row["chosen_probability"],
        "edge": row["chosen_edge"],
        "selected_edge": row["chosen_edge"],
        "expected_log_growth": row["expected_log_growth"],
        "market_age_seconds": row["market_age_seconds"],
        "seconds_to_expiry": row["seconds_to_expiry"],
        "bankroll_before": row["bankroll_before"],
        "stake_fraction": row["stake_fraction"],
        "stake_notional": row["stake_notional"],
        "notional_usd": row["stake_notional"],
        "stake_usd": row["stake_notional"],
        "top10_depth_cap": row["top10_depth_cap"],
        "executable_depth_cap": row["top10_depth_cap"],
        "depth_utilization": row["depth_utilization"],
        "decision_ts": row["decision_ts"],
        "paper_only": row["paper_only"],
        "client_order_id": f"btc5m-brownian-{row['order_intent_id']}",
    }


def _first(mapping: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if mapping.get(key) not in (None, ""):
            return mapping.get(key)
    return None


def _float(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _env_bool(value: str | bool) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def validate_brownian_runtime_env(env: Optional[dict[str, str]] = None) -> list[str]:
    source = env if env is not None else os.environ
    errors: list[str] = []
    cfg = BrownianConservativeConfig.from_env(source)
    execution_mode = str(source.get("BTC5M_EXECUTION_MODE", "observe")).strip().lower()
    live_one_shot = _env_bool(source.get("BTC5M_LIVE_ONE_SHOT", "true"))
    allow_continuous_live = _env_bool(source.get("BTC5M_ALLOW_CONTINUOUS_LIVE", "false"))
    bankroll_raw = source.get("BTC5M_BROWNIAN_BANKROLL_USD")
    try:
        bankroll = float(bankroll_raw) if bankroll_raw is not None else 0.0
    except (TypeError, ValueError):
        bankroll = 0.0

    if cfg.enabled and cfg.paper_only and bankroll <= 0:
        errors.append("brownian_bankroll_missing_or_invalid")
    if not cfg.paper_only and not cfg.live_enabled:
        errors.append("live_not_enabled")
    if cfg.live_enabled and execution_mode != "live":
        errors.append("execution_mode_not_live")
    if cfg.live_enabled and not live_one_shot and not allow_continuous_live:
        errors.append("continuous_live_blocked")
    if bankroll > 0 and bankroll < cfg.small_wallet_threshold and cfg.min_order_notional > cfg.small_wallet_max_stake_fraction * bankroll:
        canary_handles_it = (
            cfg.canary_force_min_notional_enabled
            and bankroll <= cfg.canary_force_max_wallet_usd
            and cfg.canary_force_min_notional_usd <= cfg.canary_force_max_stake_fraction * bankroll
        )
        if not canary_handles_it:
            errors.append("small_wallet_min_order_violates_max_stake_fraction")
    if cfg.live_enabled:
        private_key = str(source.get("POLY_WALLET_PRIVATE_KEY", "")).strip()
        expected_wallet = str(source.get("BTC5M_EXPECTED_WALLET_ADDRESS", "")).strip()
        if not private_key or private_key == "REPLACE_ME_DO_NOT_COMMIT":
            errors.append("polymarket_private_key_missing_or_placeholder")
        if not expected_wallet or expected_wallet == "REPLACE_ME_DO_NOT_COMMIT":
            errors.append("expected_wallet_missing_or_placeholder")
    return errors
