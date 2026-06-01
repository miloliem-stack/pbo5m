from __future__ import annotations

import json
import math
import os
from dataclasses import dataclass
from pathlib import Path
from statistics import NormalDist
from typing import Any, Callable, Optional

from ..binance_price_feed import rest_binance_price_row
from ..market_quotes import get_quote_snapshot
from ..market_router_5m import route_btc_5m_market
from ..time_utils import isoformat_utc, parse_datetime, utc_now
from .btc5m_canary_policy import DEFAULT_HMM_MODEL_ID, REQUIRED_PROBABILITY_MODEL_ID

REQUIRED_BROWNIAN_CONVENTIONS = {
    "replay-matched brownian_zero_drift__rv30",
    "brownian_zero_drift__rv30",
    "model_p_yes",
    "btc5m_replay",
}


@dataclass(frozen=True)
class LiveInputBuilderConfig:
    hmm_state_path: Optional[Path] = None
    brownian_state_path: Optional[Path] = None
    max_quote_age_ms: float = 5000.0
    max_state_age_sec: float = 15.0
    require_hmm_state: bool = True

    @classmethod
    def from_env(cls, env: Optional[dict[str, str]] = None) -> "LiveInputBuilderConfig":
        source = env if env is not None else os.environ
        hmm_path = source.get("BTC5M_LIVE_HMM_STATE_PATH")
        brownian_path = source.get("BTC5M_LIVE_BROWNIAN_STATE_PATH")
        return cls(
            hmm_state_path=Path(hmm_path) if hmm_path else None,
            brownian_state_path=Path(brownian_path) if brownian_path else None,
            max_quote_age_ms=float(source.get("BTC5M_MAX_QUOTE_AGE_MS") or source.get("BTC5M_QUOTE_MAX_AGE_MS", "5000")),
            max_state_age_sec=float(source.get("BTC5M_LIVE_STATE_MAX_AGE_SEC", "15")),
            require_hmm_state=_env_bool(source.get("BTC5M_REQUIRE_HMM_STATE", "true")),
        )


class BTC5MCanaryLiveInputBuilder:
    def __init__(
        self,
        config: Optional[LiveInputBuilderConfig] = None,
        *,
        market_fn: Callable[[], dict[str, Any]] = route_btc_5m_market,
        quote_fn: Callable[[str], dict[str, Any]] = get_quote_snapshot,
        binance_price_fn: Callable[[], dict[str, Any]] = rest_binance_price_row,
        now_fn=utc_now,
    ) -> None:
        self.config = config or LiveInputBuilderConfig.from_env()
        self.market_fn = market_fn
        self.quote_fn = quote_fn
        self.binance_price_fn = binance_price_fn
        self.now_fn = now_fn

    def build(self) -> dict[str, Any]:
        now = self.now_fn()
        missing: list[str] = []
        routed = self.market_fn()
        market = routed.get("market") if isinstance(routed, dict) else None
        if not market:
            return {"ok": False, "missing_input_reason": "missing_active_market", "missing_components": ["active_market"]}
        start = parse_datetime(market.get("start_time") or market.get("market_start_ts"))
        end = parse_datetime(market.get("end_time") or market.get("market_end_ts"))
        if start is None:
            return {"ok": False, "missing_input_reason": "market_timestamp_missing", "missing_components": ["market_start_ts"], "market": market}
        yes_token = market.get("token_yes") or market.get("yes_token_id")
        no_token = market.get("token_no") or market.get("no_token_id")
        if not yes_token or not no_token:
            return {"ok": False, "missing_input_reason": "market_token_missing", "missing_components": ["yes_token_id", "no_token_id"], "market": market}

        yes_quote = self.quote_fn(str(yes_token))
        no_quote = self.quote_fn(str(no_token))
        quote_ts = max(parse_datetime(yes_quote.get("fetched_at")) or now, parse_datetime(no_quote.get("fetched_at")) or now)
        quote_age_ms = max(float(yes_quote.get("age_seconds") or 0.0), float(no_quote.get("age_seconds") or 0.0)) * 1000.0

        yes_fetch_ok = bool(yes_quote.get("fetch_ok"))
        no_fetch_ok = bool(no_quote.get("fetch_ok"))
        yes_best_ask = _optional_float(yes_quote.get("best_ask"))
        no_best_ask = _optional_float(no_quote.get("best_ask"))
        valid_topbook = yes_fetch_ok and no_fetch_ok and yes_best_ask is not None and no_best_ask is not None

        # Compute specific quote missing reason
        quote_missing_reason: Optional[str] = None
        if not valid_topbook:
            if not yes_fetch_ok and not no_fetch_ok:
                quote_missing_reason = "quote_both_fetch_failed"
            elif not yes_fetch_ok:
                quote_missing_reason = "quote_yes_fetch_failed"
            elif not no_fetch_ok:
                quote_missing_reason = "quote_no_fetch_failed"
            elif yes_best_ask is None and no_best_ask is None:
                quote_missing_reason = "quote_both_best_ask_missing"
            elif yes_best_ask is None:
                quote_missing_reason = "quote_yes_best_ask_missing"
            else:
                quote_missing_reason = "quote_no_best_ask_missing"

        market_age_sec = (now - start).total_seconds()
        skip_quote_age_sec = float(os.environ.get("BTC5M_SKIP_IF_QUOTE_MISSING_AFTER_MARKET_AGE_SEC", "240"))

        if not valid_topbook:
            if market_age_sec >= skip_quote_age_sec:
                quote_missing_reason = "quote_missing_near_market_end"
            missing.append(quote_missing_reason or "quote")

        brownian = self.build_brownian_probability(now=now, market_start=start, market_end=end)
        if not brownian.get("ok"):
            missing.append("brownian_probability")
        hmm = self.load_hmm_state(now=now)
        if self.config.require_hmm_state and not hmm.get("ok"):
            missing.append("hmm_state")

        input_payload = {
            "market": {
                "market_id": market.get("market_id"),
                "condition_id": market.get("condition_id"),
                "slug": market.get("slug"),
                "token_yes": str(yes_token),
                "token_no": str(no_token),
                "yes_token_id": str(yes_token),
                "no_token_id": str(no_token),
                "market_start_ts": isoformat_utc(start),
                "market_end_ts": isoformat_utc(end),
                "market_age_sec": (now - start).total_seconds(),
                "market_age_seconds": (now - start).total_seconds(),
                "tradable": market.get("tradable", market.get("active", True)),
                "is_open": market.get("is_open", market.get("active", True)),
            },
            "quote": {
                "valid_topbook": valid_topbook,
                "quote_ts": isoformat_utc(quote_ts),
                "quote_age_ms": quote_age_ms,
                "yes_ask": yes_quote.get("best_ask"),
                "no_ask": no_quote.get("best_ask"),
                "yes_depth": yes_quote.get("ask_size"),
                "no_depth": no_quote.get("ask_size"),
                "yes_asks": _ask_levels(yes_quote),
                "no_asks": _ask_levels(no_quote),
                "yes_top10_depth_cap": _topn_depth_cap(yes_quote, 10),
                "no_top10_depth_cap": _topn_depth_cap(no_quote, 10),
                "yes_token_id": str(yes_token),
                "no_token_id": str(no_token),
            },
            "predictions": brownian.get("prediction"),
            "price_state": brownian.get("price_state"),
            "hmm_state": hmm.get("hmm_state"),
            "risk_state": _brownian_risk_state_from_env(),
            "decision_ts": isoformat_utc(now),
            "live_input_meta": {
                "generated_ts": isoformat_utc(now),
                "market_source": routed.get("detection_source") if isinstance(routed, dict) else None,
                "missing_components": missing,
                "brownian_error": brownian.get("missing_input_reason"),
                "brownian_convention_found": brownian.get("convention_found"),
                "hmm_error": hmm.get("missing_input_reason"),
                "brownian_source": brownian.get("source"),
                "hmm_source": hmm.get("source"),
                "yes_quote_fetch_ok": yes_fetch_ok,
                "no_quote_fetch_ok": no_fetch_ok,
                "yes_quote_error_kind": yes_quote.get("error_kind"),
                "no_quote_error_kind": no_quote.get("error_kind"),
                "yes_quote_error": yes_quote.get("error"),
                "no_quote_error": no_quote.get("error"),
                "yes_quote_http_status": yes_quote.get("http_status"),
                "no_quote_http_status": no_quote.get("http_status"),
                "yes_best_bid": _optional_float(yes_quote.get("best_bid")),
                "yes_best_ask": yes_best_ask,
                "no_best_bid": _optional_float(no_quote.get("best_bid")),
                "no_best_ask": no_best_ask,
                "yes_is_empty": yes_quote.get("is_empty"),
                "no_is_empty": no_quote.get("is_empty"),
                "yes_is_crossed": yes_quote.get("is_crossed"),
                "no_is_crossed": no_quote.get("is_crossed"),
                "yes_response_text_sample": yes_quote.get("response_text_sample"),
                "no_response_text_sample": no_quote.get("response_text_sample"),
                "yes_token_id": str(yes_token),
                "no_token_id": str(no_token),
                "quote_missing_reason": quote_missing_reason,
            },
        }
        return {
            "ok": not missing,
            "missing_input_reason": missing[0] if len(missing) == 1 else (",".join(missing) if missing else None),
            "missing_components": missing,
            "input": input_payload,
        }

    def build_brownian_probability(self, *, now, market_start, market_end) -> dict[str, Any]:
        if self.config.brownian_state_path:
            try:
                state = load_json(self.config.brownian_state_path)
            except (FileNotFoundError, ValueError):
                return {"ok": False, "missing_input_reason": "brownian_artifact_missing_or_invalid"}
            if state.get("model_id") != REQUIRED_PROBABILITY_MODEL_ID:
                return {"ok": False, "missing_input_reason": "brownian_model_id_mismatch"}
            convention = state.get("probability_convention") or state.get("probability_replay_convention")
            if convention not in REQUIRED_BROWNIAN_CONVENTIONS:
                return {"ok": False, "missing_input_reason": "brownian_convention_mismatch",
                        "convention_found": convention}
            stale_reason = self._stale_reason(state)
            if stale_reason:
                return {"ok": False, "missing_input_reason": stale_reason}
            p_yes = state.get("model_p_yes", state.get("p_yes"))
            if p_yes is None:
                return {"ok": False, "missing_input_reason": "brownian_probability_missing"}
            return {
                "ok": True,
                "source": str(self.config.brownian_state_path),
                "prediction": {
                    "model_id": REQUIRED_PROBABILITY_MODEL_ID,
                    "model_p_yes": float(p_yes),
                    "model_p_no": 1.0 - float(p_yes),
                    "probability_replay_convention": convention,
                    "probability_convention": convention,
                    "model_version": state.get("model_version"),
                    "artifact_path": str(self.config.brownian_state_path),
                },
                "price_state": {
                    "reference_price": _optional_float(state.get("reference_price")),
                    "current_price": _optional_float(state.get("current_price")),
                    "sigma": _optional_float(state.get("rv30") or state.get("sigma")),
                    "rv30": _optional_float(state.get("rv30") or state.get("sigma")),
                    "asof_ts": state.get("asof_ts") or state.get("generated_ts"),
                },
            }
        price = self.binance_price_fn()
        current_price = price.get("price")
        if current_price is None:
            return {"ok": False, "missing_input_reason": "binance_price_missing"}
        reference_price = os.getenv("BTC5M_LIVE_REFERENCE_PRICE")
        rv30 = os.getenv("BTC5M_LIVE_RV30")
        if reference_price is None or rv30 is None:
            return {"ok": False, "missing_input_reason": "brownian_live_reference_or_rv30_missing"}
        p_yes = brownian_probability(float(current_price), float(reference_price), float(rv30), max(0.0, (market_end - now).total_seconds()) / 60.0)
        return {
            "ok": True,
            "source": "live_formula_env_reference_rv30",
            "prediction": {
                "model_id": REQUIRED_PROBABILITY_MODEL_ID,
                "model_p_yes": p_yes,
                "model_p_no": 1.0 - p_yes,
                "probability_replay_convention": "model_p_yes",
                "probability_formula": "brownian_zero_drift__rv30",
            },
            "price_state": {
                "reference_price": float(reference_price),
                "current_price": float(current_price),
                "sigma": float(rv30),
                "rv30": float(rv30),
                "asof_ts": price.get("ts") or isoformat_utc(now),
            },
        }

    def load_hmm_state(self, *, now) -> dict[str, Any]:
        if not self.config.hmm_state_path:
            return {"ok": False, "missing_input_reason": "hmm_state_path_missing"}
        try:
            state = load_json(self.config.hmm_state_path)
        except (FileNotFoundError, ValueError):
            return {"ok": False, "missing_input_reason": "hmm_artifact_missing_or_invalid"}
        if state.get("hmm_model_id") != DEFAULT_HMM_MODEL_ID:
            return {"ok": False, "missing_input_reason": "hmm_model_id_mismatch"}
        if state.get("hmm_state") is None:
            return {"ok": False, "missing_input_reason": "hmm_state_missing"}
        stale_reason = self._stale_reason(state, now=now)
        if stale_reason:
            return {"ok": False, "missing_input_reason": stale_reason}
        return {
            "ok": True,
            "source": str(self.config.hmm_state_path),
            "hmm_state": {
                "hmm_model_id": DEFAULT_HMM_MODEL_ID,
                "hmm_state": int(state["hmm_state"]),
                "hmm_pmax": state.get("hmm_pmax"),
                "hmm_state_ts": state.get("hmm_state_ts") or state.get("timestamp"),
                "hmm_model_version": state.get("hmm_model_version"),
                "hmm_artifact_path": state.get("model_artifact_path") or str(self.config.hmm_state_path),
                "hmm_model_artifact_hash": state.get("model_artifact_hash"),
                "hmm_feature_config_hash": state.get("feature_config_hash"),
            },
        }

    def _stale_reason(self, state: dict[str, Any], *, now=None) -> Optional[str]:
        ref = now or self.now_fn()
        asof = parse_datetime(state.get("asof_ts") or state.get("generated_ts") or state.get("timestamp") or state.get("hmm_state_ts"))
        if asof is None:
            return "live_state_timestamp_missing"
        if (ref - asof).total_seconds() > self.config.max_state_age_sec:
            return "live_state_stale"
        return None


def brownian_probability(current_price: float, reference_price: float, rv30: float, tau_minutes: float) -> float:
    sigma = min(max(float(rv30), 1e-5), 0.05)
    tau = max(float(tau_minutes), 1e-9)
    z = math.log(float(current_price) / float(reference_price)) / (sigma * math.sqrt(tau))
    return min(max(NormalDist().cdf(z), 1e-6), 1.0 - 1e-6)


def load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"live input artifact does not exist: {path}")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"live input artifact is invalid JSON: {path}") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"live input artifact must be a JSON object: {path}")
    return payload


def _ask_levels(quote: dict[str, Any]) -> list[dict[str, float]]:
    book = quote.get("raw", {}).get("book") if isinstance(quote.get("raw"), dict) else None
    if isinstance(book, dict):
        payload = book.get("book") or book.get("data") or book.get("orderbook") or book
        levels = payload.get("asks") if isinstance(payload, dict) else None
    else:
        levels = None
    parsed: list[dict[str, float]] = []
    if isinstance(levels, list):
        for level in levels:
            if isinstance(level, dict):
                px = _optional_float(level.get("price") or level.get("p"))
                size = _optional_float(level.get("size") or level.get("quantity") or level.get("qty") or level.get("q"))
            elif isinstance(level, (list, tuple)) and len(level) >= 2:
                px = _optional_float(level[0])
                size = _optional_float(level[1])
            else:
                px = None
                size = None
            if px is not None and size is not None and size > 0:
                parsed.append({"price": px, "size": size})
    if not parsed and quote.get("best_ask") is not None and quote.get("ask_size") is not None:
        parsed.append({"price": float(quote["best_ask"]), "size": float(quote["ask_size"])})
    return sorted(parsed, key=lambda row: row["price"])


def _topn_depth_cap(quote: dict[str, Any], top_n: int) -> Optional[float]:
    levels = _ask_levels(quote)
    if not levels:
        return None
    return float(sum(level["price"] * level["size"] for level in levels[:top_n]))


def _optional_float(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _env_bool(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def _brownian_risk_state_from_env() -> dict[str, Any]:
    bankroll = _optional_float(os.getenv("BTC5M_BROWNIAN_BANKROLL_USD"))
    session_start = _optional_float(os.getenv("BTC5M_BROWNIAN_SESSION_START_BANKROLL_USD")) or bankroll
    day_start = _optional_float(os.getenv("BTC5M_BROWNIAN_DAY_START_BANKROLL_USD")) or bankroll
    daily_pnl = _optional_float(os.getenv("BTC5M_BROWNIAN_DAILY_PNL_USD")) or 0.0
    already = str(os.getenv("BTC5M_BROWNIAN_ALREADY_TRADED_MARKET", "false")).strip().lower() in {"1", "true", "yes", "on"}
    return {
        "open_positions": 0,
        "daily_loss_usd": 0.0,
        "bankroll": bankroll or 0.0,
        # session_start_bankroll and day_start_bankroll default to None when
        # BTC5M_BROWNIAN_BANKROLL_USD is not set so that the live capital state
        # (set later by _apply_capital_risk_state) becomes the authoritative
        # baseline and the stop-loss guards are not triggered by a stale static value.
        "session_start_bankroll": session_start or 0.0,
        "day_start_bankroll": day_start or 0.0,
        "daily_pnl": daily_pnl,
        "already_traded_market": already,
    }
