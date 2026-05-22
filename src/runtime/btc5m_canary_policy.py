from __future__ import annotations

import json
import os
import hashlib
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Optional

from ..time_utils import isoformat_utc, parse_datetime, utc_now


POLICY_ID = "state3_ask_brownian_age60_v0"
DEFAULT_HMM_MODEL_ID = "laplace_1m__gaussian_hmm__k4"
REQUIRED_PROBABILITY_MODEL_ID = "brownian_zero_drift__rv30"
DEFAULT_ALLOWED_STATES = frozenset({3})
DEFAULT_MODEL_ALLOWLIST = frozenset({REQUIRED_PROBABILITY_MODEL_ID})
DEFAULT_MODEL_BLOCKLIST = frozenset(
    {
        "baseline_50",
        "calibrated_logistic__gbm_rv30",
        "gbm_zero_drift__rv30_no_ito",
        "gbm_winsorized_sigma__w30__z2.5",
        "gbm_blended_sigma__50_30_20",
    }
)
BLOCKING_ENTRY_STATUSES = frozenset(
    {
        "accepted",
        "booked",
        "filled",
        "open",
        "partially_filled",
        "pending",
        "pending_submit",
        "recently_accepted",
        "submitted",
    }
)


@dataclass(frozen=True)
class CanaryConfig:
    policy_id: str = POLICY_ID
    min_entry_age_sec: float = 60.0
    max_entry_age_sec: float = 240.0
    shadow_max_entry_age_sec: float = 300.0
    hmm_gate_enabled: bool = True
    hmm_model_id: str = DEFAULT_HMM_MODEL_ID
    hmm_allowed_states: frozenset[int] = DEFAULT_ALLOWED_STATES
    model_allowlist: frozenset[str] = DEFAULT_MODEL_ALLOWLIST
    model_blocklist: frozenset[str] = DEFAULT_MODEL_BLOCKLIST
    ask_filter_enabled: bool = True
    min_ask: float = 0.30
    max_ask: float = 0.47
    min_edge: Optional[float] = None
    one_entry_per_market: bool = True
    canary_stake_usd: Optional[float] = None
    max_open_positions: int = 1
    daily_max_loss_usd: Optional[float] = None
    max_quote_age_ms: float = 5000.0
    hmm_artifact_path: Optional[str] = None
    hmm_model_version: Optional[str] = None
    probability_model_artifact_path: Optional[str] = None
    probability_model_version: Optional[str] = None
    blocking_entry_statuses: frozenset[str] = field(default_factory=lambda: BLOCKING_ENTRY_STATUSES)

    @classmethod
    def from_env(cls, env: Optional[dict[str, str]] = None, *, strict: bool = True) -> "CanaryConfig":
        source = env if env is not None else os.environ
        policy_id = source.get("BTC5M_POLICY_ID", POLICY_ID)
        if policy_id != POLICY_ID:
            raise ValueError(f"unsupported BTC5M_POLICY_ID={policy_id!r}; expected {POLICY_ID!r}")
        min_edge = _optional_float(source.get("BTC5M_MIN_EDGE"))
        if strict and min_edge is None:
            raise ValueError("BTC5M_MIN_EDGE is required for state3_ask_brownian_age60_v0")
        config = cls(
            policy_id=policy_id,
            min_entry_age_sec=float(source.get("BTC5M_MIN_ENTRY_AGE_SEC", "60")),
            max_entry_age_sec=float(source.get("BTC5M_MAX_ENTRY_AGE_SEC", "240")),
            shadow_max_entry_age_sec=float(source.get("BTC5M_SHADOW_MAX_ENTRY_AGE_SEC", "300")),
            hmm_gate_enabled=_env_bool(source.get("BTC5M_HMM_GATE_ENABLED", "true")),
            hmm_model_id=source.get("BTC5M_HMM_MODEL_ID", DEFAULT_HMM_MODEL_ID),
            hmm_allowed_states=frozenset(int(x) for x in _split_csv(source.get("BTC5M_HMM_ALLOWED_STATES", "3"))),
            model_allowlist=frozenset(_split_csv(source.get("BTC5M_MODEL_ALLOWLIST", "brownian_zero_drift__rv30"))),
            model_blocklist=frozenset(
                _split_csv(
                    source.get(
                        "BTC5M_MODEL_BLOCKLIST",
                        ",".join(sorted(DEFAULT_MODEL_BLOCKLIST)),
                    )
                )
            ),
            ask_filter_enabled=_env_bool(source.get("BTC5M_ASK_FILTER_ENABLED", "true")),
            min_ask=float(source.get("BTC5M_MIN_ASK", "0.30")),
            max_ask=float(source.get("BTC5M_MAX_ASK", "0.47")),
            min_edge=min_edge,
            one_entry_per_market=_env_bool(source.get("BTC5M_ONE_ENTRY_PER_MARKET", "true")),
            canary_stake_usd=_optional_float(source.get("BTC5M_CANARY_STAKE_USD")),
            max_open_positions=int(source.get("BTC5M_MAX_OPEN_POSITIONS", "1")),
            daily_max_loss_usd=_optional_float(source.get("BTC5M_DAILY_MAX_LOSS_USD")),
            max_quote_age_ms=float(
                source.get("BTC5M_MAX_QUOTE_AGE_MS")
                or source.get("BTC5M_QUOTE_MAX_AGE_MS")
                or "5000"
            ),
            hmm_artifact_path=source.get("BTC5M_HMM_ARTIFACT_PATH"),
            hmm_model_version=source.get("BTC5M_HMM_MODEL_VERSION"),
            probability_model_artifact_path=source.get("BTC5M_PROBABILITY_MODEL_ARTIFACT_PATH"),
            probability_model_version=source.get("BTC5M_PROBABILITY_MODEL_VERSION"),
        )
        if strict:
            identity_errors = config.identity_errors()
            if identity_errors:
                raise ValueError(
                    "BTC5M canary component identity mismatch: "
                    + ", ".join(identity_errors)
                )
        return config

    def startup_errors(self) -> list[str]:
        errors: list[str] = []
        errors.extend(self.identity_errors())
        if self.min_edge is None:
            errors.append("missing_min_edge")
        if self.canary_stake_usd is None:
            errors.append("missing_stake")
        return errors

    def identity_errors(self) -> list[str]:
        errors: list[str] = []
        if self.policy_id != POLICY_ID:
            errors.append("unsupported_policy_id")
        if not self.hmm_gate_enabled:
            errors.append("hmm_gate_disabled")
        if self.hmm_model_id != DEFAULT_HMM_MODEL_ID:
            errors.append("hmm_model_mismatch")
        if self.hmm_allowed_states != DEFAULT_ALLOWED_STATES:
            errors.append("hmm_allowed_states_mismatch")
        if self.model_allowlist != DEFAULT_MODEL_ALLOWLIST:
            errors.append("probability_model_allowlist_mismatch")
        if REQUIRED_PROBABILITY_MODEL_ID in self.model_blocklist:
            errors.append("probability_model_blocked")
        if not self.ask_filter_enabled:
            errors.append("ask_filter_disabled")
        if self.min_entry_age_sec != 60.0 or self.max_entry_age_sec != 240.0:
            errors.append("market_age_gate_mismatch")
        if self.min_ask != 0.30 or self.max_ask != 0.47:
            errors.append("ask_filter_mismatch")
        return errors

    def config_hash(self) -> str:
        payload = {
            "policy_id": self.policy_id,
            "min_entry_age_sec": self.min_entry_age_sec,
            "max_entry_age_sec": self.max_entry_age_sec,
            "hmm_gate_enabled": self.hmm_gate_enabled,
            "hmm_model_id": self.hmm_model_id,
            "hmm_allowed_states": sorted(self.hmm_allowed_states),
            "model_allowlist": sorted(self.model_allowlist),
            "model_blocklist": sorted(self.model_blocklist),
            "ask_filter_enabled": self.ask_filter_enabled,
            "min_ask": self.min_ask,
            "max_ask": self.max_ask,
            "min_edge": self.min_edge,
            "max_quote_age_ms": self.max_quote_age_ms,
            "hmm_artifact_path": self.hmm_artifact_path,
            "hmm_model_version": self.hmm_model_version,
            "probability_model_artifact_path": self.probability_model_artifact_path,
            "probability_model_version": self.probability_model_version,
        }
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()[:16]


def evaluate_canary_policy(
    *,
    market: dict[str, Any],
    quote: dict[str, Any],
    predictions: dict[str, Any] | Iterable[dict[str, Any]],
    hmm_state: Optional[dict[str, Any]],
    risk_state: Optional[dict[str, Any]] = None,
    config: Optional[CanaryConfig] = None,
    decision_ts: Optional[Any] = None,
) -> dict[str, Any]:
    cfg = config or CanaryConfig.from_env()
    risk = risk_state or {}
    decision_time = parse_datetime(decision_ts) or utc_now()
    market_start = _market_start(market)
    market_age_sec = _market_age_seconds(market, decision_time, market_start)
    prediction = _select_prediction(predictions, cfg)
    quote_view = _quote_view(quote, decision_time, cfg)
    hmm_view = _hmm_view(hmm_state)

    row = _base_decision_row(
        cfg=cfg,
        market=market,
        decision_time=decision_time,
        market_start=market_start,
        market_age_sec=market_age_sec,
        quote_view=quote_view,
        prediction=prediction,
        hmm_view=hmm_view,
    )

    reasons: list[str] = []
    in_live_age_window = True
    in_shadow_age_window = False
    if market_age_sec is None:
        in_live_age_window = False
        reasons.append("market_age_missing")
    elif market_age_sec < cfg.min_entry_age_sec:
        in_live_age_window = False
        reasons.append("market_age_too_young")
    elif market_age_sec > cfg.max_entry_age_sec:
        in_live_age_window = False
        in_shadow_age_window = market_age_sec <= cfg.shadow_max_entry_age_sec
        reasons.append("market_age_after_live_window")
    row["market_age_gate_pass"] = bool(in_live_age_window)

    quote_missing = quote_view["yes_ask"] is None or quote_view["no_ask"] is None
    quote_pass = quote_view["valid_topbook"] and not quote_missing and not quote_view["quote_stale"]
    if quote_missing:
        reasons.append("quote_missing")
    elif not quote_view["valid_topbook"]:
        reasons.append("quote_invalid")
    elif quote_view["quote_stale"]:
        reasons.append("quote_stale")
    row["valid_topbook"] = bool(quote_view["valid_topbook"])

    hmm_pass = (
        cfg.hmm_gate_enabled
        and cfg.hmm_model_id == DEFAULT_HMM_MODEL_ID
        and cfg.hmm_allowed_states == DEFAULT_ALLOWED_STATES
        and hmm_view["hmm_model_id"] == DEFAULT_HMM_MODEL_ID
        and hmm_view["hmm_state"] in DEFAULT_ALLOWED_STATES
    )
    if hmm_state is None:
        reasons.append("hmm_state_missing")
    elif not cfg.hmm_gate_enabled or cfg.hmm_model_id != DEFAULT_HMM_MODEL_ID:
        reasons.append("hmm_model_missing")
    elif cfg.hmm_allowed_states != DEFAULT_ALLOWED_STATES:
        reasons.append("hmm_state_missing")
    elif hmm_view["hmm_model_id"] != DEFAULT_HMM_MODEL_ID:
        reasons.append("hmm_model_missing")
    elif hmm_view["hmm_state"] is None:
        reasons.append("hmm_state_missing")
    elif hmm_view["hmm_state"] not in DEFAULT_ALLOWED_STATES:
        reasons.append("hmm_state_not_allowed")
    row["hmm_gate_pass"] = bool(hmm_pass)

    model_id = prediction.get("model_id")
    model_pass = bool(
        model_id == REQUIRED_PROBABILITY_MODEL_ID
        and cfg.model_allowlist == DEFAULT_MODEL_ALLOWLIST
        and REQUIRED_PROBABILITY_MODEL_ID not in cfg.model_blocklist
        and prediction.get("model_p_yes") is not None
        and prediction.get("model_p_no") is not None
        and prediction.get("probability_formula_ok") is not False
    )
    if prediction.get("missing_required_model"):
        reasons.append("probability_model_missing")
    elif model_id != REQUIRED_PROBABILITY_MODEL_ID or cfg.model_allowlist != DEFAULT_MODEL_ALLOWLIST or REQUIRED_PROBABILITY_MODEL_ID in cfg.model_blocklist:
        reasons.append("probability_model_mismatch")
    elif prediction.get("model_p_yes") is None or prediction.get("model_p_no") is None:
        reasons.append("probability_model_missing")
    elif prediction.get("probability_formula_ok") is False:
        reasons.append("probability_model_mismatch")
    row["model_gate_pass"] = bool(model_pass)

    selected_side, selected_ask, selected_edge = _select_side(prediction, quote_view)
    row["selected_side"] = selected_side
    row["selected_ask"] = selected_ask
    row["selected_edge"] = selected_edge

    ask_pass = True
    if cfg.ask_filter_enabled:
        ask_pass = selected_ask is not None and cfg.min_ask < selected_ask < cfg.max_ask
        if not ask_pass:
            reasons.append("ask_filter_failed")
    row["ask_filter_pass"] = bool(ask_pass)

    edge_pass = selected_edge is not None and cfg.min_edge is not None and selected_edge >= cfg.min_edge
    if cfg.min_edge is None:
        reasons.append("missing_min_edge")
    elif not edge_pass:
        reasons.append("edge_below_threshold")
    row["edge_gate_pass"] = bool(edge_pass)

    one_entry_pass = True
    if cfg.one_entry_per_market:
        one_entry_pass = not has_blocking_market_entry(risk, cfg)
        if not one_entry_pass:
            reasons.append("duplicate_market_entry")
    row["one_entry_gate_pass"] = bool(one_entry_pass)

    risk_pass, risk_reason = _risk_gate_pass(risk, cfg)
    if risk_reason:
        reasons.append(risk_reason)
    row["risk_gate_pass"] = bool(risk_pass)

    stake_present = cfg.canary_stake_usd is not None and cfg.canary_stake_usd > 0
    if not stake_present:
        reasons.append("missing_stake")

    non_age_gates_pass = all(
        [
            quote_pass,
            hmm_pass,
            model_pass,
            ask_pass,
            edge_pass,
            one_entry_pass,
            risk_pass,
        ]
    )
    row["would_trade_if_final_minute_enabled"] = bool(in_shadow_age_window and non_age_gates_pass and stake_present)

    if in_live_age_window and non_age_gates_pass and stake_present:
        row["final_decision"] = "BUY_YES" if selected_side == "YES" else "BUY_NO"
    elif in_shadow_age_window and non_age_gates_pass:
        row["final_decision"] = "SHADOW_ONLY"
    else:
        row["final_decision"] = "ABSTAIN"
    row["abstain_reason"] = _first_reason(reasons)
    row["abstain_reasons"] = _dedupe(reasons)
    return row


def has_blocking_market_entry(risk_state: dict[str, Any], config: Optional[CanaryConfig] = None) -> bool:
    cfg = config or CanaryConfig()
    if risk_state.get("has_blocking_entry") or risk_state.get("had_prior_entry"):
        return True
    for key in ("active_orders", "existing_orders", "market_orders", "orders"):
        orders = risk_state.get(key) or []
        if isinstance(orders, dict):
            orders = [orders]
        for order in orders:
            if not isinstance(order, dict):
                continue
            status = str(order.get("status") or "").lower()
            if status in cfg.blocking_entry_statuses:
                return True
    for status in risk_state.get("blocking_statuses") or []:
        if str(status).lower() in cfg.blocking_entry_statuses:
            return True
    return False


def select_previous_hmm_state(
    state_rows: Iterable[dict[str, Any]],
    *,
    decision_ts: Any,
    model_id: str = DEFAULT_HMM_MODEL_ID,
) -> Optional[dict[str, Any]]:
    decision_time = parse_datetime(decision_ts)
    if decision_time is None:
        raise ValueError("decision_ts is required for previous-only HMM state selection")
    selected: Optional[dict[str, Any]] = None
    selected_ts: Optional[datetime] = None
    for row in state_rows:
        row_model_id = row.get("hmm_model_id") or row.get("model_id")
        if row_model_id is not None and row_model_id != model_id:
            continue
        row_ts = parse_datetime(row.get("hmm_state_ts") or row.get("timestamp") or row.get("ts"))
        if row_ts is None or row_ts > decision_time:
            continue
        if selected_ts is None or row_ts > selected_ts:
            selected = dict(row)
            selected.setdefault("hmm_model_id", model_id)
            selected.setdefault("hmm_state_ts", isoformat_utc(row_ts))
            selected_ts = row_ts
    return selected


def write_decision_log_row(path: str | Path, row: dict[str, Any]) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(row, sort_keys=True, default=str) + "\n")


def _base_decision_row(
    *,
    cfg: CanaryConfig,
    market: dict[str, Any],
    decision_time: datetime,
    market_start: Optional[datetime],
    market_age_sec: Optional[float],
    quote_view: dict[str, Any],
    prediction: dict[str, Any],
    hmm_view: dict[str, Any],
) -> dict[str, Any]:
    return {
        "policy_id": cfg.policy_id,
        "market_id": market.get("market_id"),
        "condition_id": market.get("condition_id"),
        "yes_token_id": market.get("yes_token_id") or market.get("token_yes"),
        "no_token_id": market.get("no_token_id") or market.get("token_no"),
        "market_start_ts": isoformat_utc(market_start),
        "decision_ts": isoformat_utc(decision_time),
        "market_age_sec": market_age_sec,
        "hmm_model_id": hmm_view["hmm_model_id"],
        "required_hmm_model_id": DEFAULT_HMM_MODEL_ID,
        "hmm_state": hmm_view["hmm_state"],
        "required_hmm_allowed_states": sorted(DEFAULT_ALLOWED_STATES),
        "hmm_pmax": hmm_view["hmm_pmax"],
        "hmm_state_ts": hmm_view["hmm_state_ts"],
        "hmm_model_version": hmm_view["hmm_model_version"] or cfg.hmm_model_version,
        "hmm_artifact_path": hmm_view["hmm_artifact_path"] or cfg.hmm_artifact_path,
        "model_id": prediction.get("model_id"),
        "required_probability_model_id": REQUIRED_PROBABILITY_MODEL_ID,
        "model_p_yes": prediction.get("model_p_yes"),
        "model_p_no": prediction.get("model_p_no"),
        "probability_model_version": prediction.get("model_version") or cfg.probability_model_version,
        "probability_model_artifact_path": prediction.get("artifact_path") or cfg.probability_model_artifact_path,
        "probability_formula": prediction.get("probability_formula"),
        "probability_replay_convention": prediction.get("probability_replay_convention"),
        "config_hash": cfg.config_hash(),
        "yes_ask": quote_view["yes_ask"],
        "no_ask": quote_view["no_ask"],
        "selected_side": None,
        "selected_ask": None,
        "selected_edge": None,
        "valid_topbook": quote_view["valid_topbook"],
        "quote_ts": quote_view["quote_ts"],
        "quote_age_ms": quote_view["quote_age_ms"],
        "capacity_used": None,
        "yes_depth": quote_view["yes_depth"],
        "no_depth": quote_view["no_depth"],
        "market_age_gate_pass": False,
        "hmm_gate_pass": False,
        "model_gate_pass": False,
        "ask_filter_pass": False,
        "edge_gate_pass": False,
        "risk_gate_pass": False,
        "one_entry_gate_pass": False,
        "final_decision": "ABSTAIN",
        "abstain_reason": None,
        "abstain_reasons": [],
        "would_trade_if_final_minute_enabled": False,
        "order_id": None,
        "client_order_id": None,
        "fill_status": None,
        "stake_usd": cfg.canary_stake_usd,
    }


def _quote_view(quote: dict[str, Any], decision_time: datetime, cfg: CanaryConfig) -> dict[str, Any]:
    yes = quote.get("yes") if isinstance(quote.get("yes"), dict) else {}
    no = quote.get("no") if isinstance(quote.get("no"), dict) else {}
    yes_ask = _optional_float(_first_not_none(quote.get("yes_ask"), quote.get("executable_yes_ask"), yes.get("best_ask")))
    no_ask = _optional_float(_first_not_none(quote.get("no_ask"), quote.get("executable_no_ask"), no.get("best_ask")))
    yes_depth = _optional_float(_first_not_none(quote.get("yes_depth"), quote.get("yes_ask_size"), yes.get("ask_size")))
    no_depth = _optional_float(_first_not_none(quote.get("no_depth"), quote.get("no_ask_size"), no.get("ask_size")))
    quote_ts_value = _first_not_none(quote.get("quote_ts"), quote.get("ts"), quote.get("fetched_at"))
    quote_ts = parse_datetime(quote_ts_value)
    quote_age_ms = _optional_float(quote.get("quote_age_ms"))
    if quote_age_ms is None:
        age_seconds = _optional_float(_first_not_none(quote.get("quote_age_seconds"), quote.get("age_seconds")))
        if age_seconds is None and quote_ts is not None:
            age_seconds = max(0.0, (decision_time - quote_ts).total_seconds())
        quote_age_ms = age_seconds * 1000.0 if age_seconds is not None else None
    valid = quote.get("valid_topbook")
    if valid is None:
        quote_capture_ok = quote.get("quote_capture_ok")
        valid = bool(
            yes_ask is not None
            and no_ask is not None
            and (quote_capture_ok is not False)
            and (yes.get("fetch_ok") is not False)
            and (no.get("fetch_ok") is not False)
        )
    return {
        "yes_ask": yes_ask,
        "no_ask": no_ask,
        "yes_depth": yes_depth,
        "no_depth": no_depth,
        "valid_topbook": bool(valid),
        "quote_ts": isoformat_utc(quote_ts) if quote_ts else quote_ts_value,
        "quote_age_ms": quote_age_ms,
        "quote_stale": quote_age_ms is not None and quote_age_ms > cfg.max_quote_age_ms,
    }


def _hmm_view(hmm_state: Optional[dict[str, Any]]) -> dict[str, Any]:
    state = hmm_state or {}
    return {
        "hmm_model_id": state.get("hmm_model_id") or state.get("model_id"),
        "hmm_state": _optional_int(state.get("hmm_state") if "hmm_state" in state else state.get("state")),
        "hmm_pmax": _optional_float(state.get("hmm_pmax") if "hmm_pmax" in state else state.get("pmax")),
        "hmm_state_ts": isoformat_utc(state.get("hmm_state_ts") or state.get("timestamp") or state.get("ts")),
        "hmm_model_version": state.get("hmm_model_version") or state.get("model_version"),
        "hmm_artifact_path": state.get("hmm_artifact_path") or state.get("artifact_path"),
    }


def _select_prediction(predictions: dict[str, Any] | Iterable[dict[str, Any]], cfg: CanaryConfig) -> dict[str, Any]:
    if isinstance(predictions, dict) and "model_id" in predictions:
        candidates = [predictions]
    elif isinstance(predictions, dict):
        candidates = []
        for model_id, payload in predictions.items():
            if isinstance(payload, dict):
                row = dict(payload)
                row.setdefault("model_id", model_id)
                candidates.append(row)
            else:
                candidates.append({"model_id": model_id, "model_p_yes": payload})
    else:
        candidates = [dict(row) for row in predictions]
    required = next((row for row in candidates if row.get("model_id") == REQUIRED_PROBABILITY_MODEL_ID), None)
    if required is None:
        first = candidates[0] if candidates else {}
        return {
            "model_id": first.get("model_id"),
            "model_p_yes": None,
            "model_p_no": None,
            "missing_required_model": True,
            "model_version": first.get("model_version"),
            "artifact_path": first.get("artifact_path"),
            "probability_formula": first.get("probability_formula"),
            "probability_replay_convention": first.get("probability_replay_convention"),
            "probability_formula_ok": None,
        }
    first = required
    model_id = first.get("model_id")
    p_yes = _optional_float(_first_not_none(first.get("model_p_yes"), first.get("p_yes"), first.get("prob_yes"), first.get("probability")))
    p_no = _optional_float(_first_not_none(first.get("model_p_no"), first.get("p_no")))
    if p_no is None and p_yes is not None:
        p_no = 1.0 - p_yes
    formula = first.get("probability_formula")
    convention = first.get("probability_replay_convention")
    formula_ok = True
    if formula not in (None, "", "replay", "model_p_yes", "brownian_zero_drift__rv30"):
        formula_ok = False
    if convention not in (None, "", "btc5m_replay", "capacity_stress_replay", "model_p_yes"):
        formula_ok = False
    return {
        "model_id": model_id,
        "model_p_yes": p_yes,
        "model_p_no": p_no,
        "missing_required_model": False,
        "model_version": first.get("model_version"),
        "artifact_path": first.get("artifact_path"),
        "probability_formula": formula,
        "probability_replay_convention": convention,
        "probability_formula_ok": formula_ok,
    }


def _select_side(prediction: dict[str, Any], quote: dict[str, Any]) -> tuple[Optional[str], Optional[float], Optional[float]]:
    p_yes = prediction.get("model_p_yes")
    p_no = prediction.get("model_p_no")
    yes_ask = quote.get("yes_ask")
    no_ask = quote.get("no_ask")
    edge_yes = p_yes - yes_ask if p_yes is not None and yes_ask is not None else None
    edge_no = p_no - no_ask if p_no is not None and no_ask is not None else None
    if edge_yes is None and edge_no is None:
        return None, None, None
    if edge_no is None or (edge_yes is not None and edge_yes >= edge_no):
        return "YES", yes_ask, edge_yes
    return "NO", no_ask, edge_no


def _risk_gate_pass(risk: dict[str, Any], cfg: CanaryConfig) -> tuple[bool, Optional[str]]:
    open_positions = _optional_int(risk.get("open_positions"))
    if open_positions is not None and open_positions >= cfg.max_open_positions:
        return False, "risk_max_open_positions"
    if cfg.daily_max_loss_usd is not None:
        daily_loss = _optional_float(risk.get("daily_loss_usd"))
        if daily_loss is None:
            daily_pnl = _optional_float(risk.get("daily_pnl_usd"))
            if daily_pnl is not None:
                daily_loss = max(0.0, -daily_pnl)
        if daily_loss is not None and daily_loss >= cfg.daily_max_loss_usd:
            return False, "risk_daily_max_loss"
    return True, None


def _market_start(market: dict[str, Any]) -> Optional[datetime]:
    return parse_datetime(
        _first_not_none(
            market.get("market_start_ts"),
            market.get("market_start_time"),
            market.get("start_time"),
            market.get("start_ts"),
        )
    )


def _market_age_seconds(market: dict[str, Any], decision_time: datetime, market_start: Optional[datetime]) -> Optional[float]:
    explicit = _optional_float(_first_not_none(market.get("market_age_sec"), market.get("market_age_seconds")))
    if explicit is not None:
        return explicit
    if market_start is None:
        return None
    return (decision_time - market_start).total_seconds()


def _optional_float(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _optional_int(value: Any) -> Optional[int]:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _first_not_none(*values: Any) -> Any:
    for value in values:
        if value is not None:
            return value
    return None


def _split_csv(value: str) -> list[str]:
    return [part.strip() for part in str(value or "").split(",") if part.strip()]


def _env_bool(value: str) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _dedupe(values: Iterable[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        if value not in seen:
            out.append(value)
            seen.add(value)
    return out


def _first_reason(values: Iterable[str]) -> Optional[str]:
    for value in values:
        return value
    return None
