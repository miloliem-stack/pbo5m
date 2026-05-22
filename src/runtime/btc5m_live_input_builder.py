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

REQUIRED_BROWNIAN_CONVENTIONS = {"replay-matched brownian_zero_drift__rv30", "model_p_yes", "btc5m_replay"}


@dataclass(frozen=True)
class LiveInputBuilderConfig:
    hmm_state_path: Optional[Path] = None
    brownian_state_path: Optional[Path] = None
    max_quote_age_ms: float = 5000.0
    max_state_age_sec: float = 15.0

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
        valid_topbook = bool(yes_quote.get("fetch_ok") and no_quote.get("fetch_ok") and yes_quote.get("best_ask") is not None and no_quote.get("best_ask") is not None)
        if not valid_topbook:
            missing.append("quote")

        brownian = self.build_brownian_probability(now=now, market_start=start, market_end=end)
        if not brownian.get("ok"):
            missing.append("brownian_probability")
        hmm = self.load_hmm_state(now=now)
        if not hmm.get("ok"):
            missing.append("hmm_state")

        input_payload = {
            "market": {
                "market_id": market.get("market_id"),
                "condition_id": market.get("condition_id"),
                "token_yes": str(yes_token),
                "token_no": str(no_token),
                "yes_token_id": str(yes_token),
                "no_token_id": str(no_token),
                "market_start_ts": isoformat_utc(start),
                "market_end_ts": isoformat_utc(end),
                "market_age_sec": (now - start).total_seconds(),
            },
            "quote": {
                "valid_topbook": valid_topbook,
                "quote_ts": isoformat_utc(quote_ts),
                "quote_age_ms": quote_age_ms,
                "yes_ask": yes_quote.get("best_ask"),
                "no_ask": no_quote.get("best_ask"),
                "yes_depth": yes_quote.get("ask_size"),
                "no_depth": no_quote.get("ask_size"),
            },
            "predictions": brownian.get("prediction"),
            "hmm_state": hmm.get("hmm_state"),
            "risk_state": {"open_positions": 0, "daily_loss_usd": 0.0},
            "decision_ts": isoformat_utc(now),
            "live_input_meta": {
                "generated_ts": isoformat_utc(now),
                "market_source": routed.get("detection_source") if isinstance(routed, dict) else None,
                "missing_components": missing,
                "brownian_error": brownian.get("missing_input_reason"),
                "hmm_error": hmm.get("missing_input_reason"),
                "brownian_source": brownian.get("source"),
                "hmm_source": hmm.get("source"),
            },
        }
        return {
            "ok": not missing,
            "missing_input_reason": ",".join(missing) if missing else None,
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
                return {"ok": False, "missing_input_reason": "brownian_convention_mismatch"}
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
