from __future__ import annotations

import hashlib
import importlib.metadata
import importlib.util
import inspect
import json
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN
from pathlib import Path
from typing import Any, Optional, Protocol

from ..time_utils import isoformat_utc, parse_datetime, utc_now
from .btc5m_canary_policy import POLICY_ID, CanaryConfig


LIVE_BLOCKING_EVENT_TYPES = {
    "order_intent_created",
    "live_order_submitted",
    "order_status_polled",
    "order_filled",
    "order_partially_filled",
    "order_unknown_after_submit",
    "execution_error_after_submit",
    "execution_rejected_by_venue",
}
FINAL_SUCCESS_STATUSES = {"filled", "matched"}
PARTIAL_STATUSES = {"partially_filled", "partial", "partially_matched"}
REJECTED_STATUSES = {"rejected", "failed", "error"}
CANCELLED_STATUSES = {"cancelled", "canceled"}


@dataclass(frozen=True)
class ExecutionConfig:
    execution_mode: str = "observe"
    live_trading_enabled: bool = False
    live_one_shot: bool = True
    max_order_attempts_per_process: int = 1
    canary_stake_usd: Optional[float] = None
    max_notional_per_market_usd: Optional[float] = None
    max_daily_notional_usd: Optional[float] = None
    max_open_positions: int = 1
    one_entry_per_market: bool = True
    expected_wallet_address: Optional[str] = None
    order_poll_timeout_sec: float = 20.0
    order_poll_interval_sec: float = 1.0
    max_quote_age_ms: float = 5000.0
    max_price_slippage: float = 0.01
    max_limit_price: float = 0.49
    decision_expiry_ms: int = 2000
    allow_reentry_after_reject: bool = False
    journal_root: Path = Path("artifacts/btc5m_canary_execution")
    policy_config: CanaryConfig = CanaryConfig(min_edge=0.0, canary_stake_usd=1.0)

    @classmethod
    def from_env(cls, env: Optional[dict[str, str]] = None) -> "ExecutionConfig":
        source = env if env is not None else os.environ
        mode = source.get("BTC5M_EXECUTION_MODE", "observe").strip().lower()
        live = _env_bool(source.get("BTC5M_LIVE_TRADING_ENABLED", "false"))
        stake = _optional_float(source.get("BTC5M_CANARY_STAKE_USD"))
        policy_config = CanaryConfig.from_env(source, strict=True)
        return cls(
            execution_mode=mode,
            live_trading_enabled=live,
            live_one_shot=_env_bool(source.get("BTC5M_LIVE_ONE_SHOT", "true")),
            max_order_attempts_per_process=int(source.get("BTC5M_MAX_ORDER_ATTEMPTS_PER_PROCESS", "1")),
            canary_stake_usd=stake,
            max_notional_per_market_usd=_optional_float(source.get("BTC5M_MAX_NOTIONAL_PER_MARKET_USD")),
            max_daily_notional_usd=_optional_float(source.get("BTC5M_MAX_DAILY_NOTIONAL_USD")),
            max_open_positions=int(source.get("BTC5M_MAX_OPEN_POSITIONS", "1")),
            one_entry_per_market=_env_bool(source.get("BTC5M_ONE_ENTRY_PER_MARKET", "true")),
            expected_wallet_address=source.get("BTC5M_EXPECTED_WALLET_ADDRESS"),
            order_poll_timeout_sec=float(source.get("BTC5M_ORDER_POLL_TIMEOUT_SEC", "20")),
            order_poll_interval_sec=float(source.get("BTC5M_ORDER_POLL_INTERVAL_SEC", "1")),
            max_quote_age_ms=float(source.get("BTC5M_MAX_QUOTE_AGE_MS") or source.get("BTC5M_QUOTE_MAX_AGE_MS", "5000")),
            max_price_slippage=float(source.get("BTC5M_MAX_PRICE_SLIPPAGE", "0.01")),
            max_limit_price=float(source.get("BTC5M_MAX_LIMIT_PRICE", "0.49")),
            decision_expiry_ms=int(source.get("BTC5M_DECISION_EXPIRY_MS", "2000")),
            allow_reentry_after_reject=_env_bool(source.get("BTC5M_ALLOW_REENTRY_AFTER_REJECT", "false")),
            journal_root=Path(source.get("BTC5M_EXECUTION_JOURNAL_ROOT", "artifacts/btc5m_canary_execution")),
            policy_config=policy_config,
        )

    def is_live(self) -> bool:
        return self.execution_mode == "live" and self.live_trading_enabled


@dataclass(frozen=True)
class OrderIntent:
    policy_id: str
    market_id: Optional[str]
    condition_id: Optional[str]
    token_id: str
    selected_side: str
    action: str
    selected_ask: float
    selected_edge: float
    stake_usd: float
    market_age_sec: float
    decision_ts: Optional[str]
    quote_ts: Optional[str]
    quote_age_ms: Optional[float]
    client_order_id: str
    idempotency_key: str
    market_start_ts: Optional[str] = None
    yes_token_id: Optional[str] = None
    no_token_id: Optional[str] = None
    limit_price: Optional[float] = None
    max_price: Optional[float] = None


class ClobOrderAdapter(Protocol):
    def wallet_address(self) -> Optional[str]:
        ...

    def submit_buy(self, intent: OrderIntent) -> dict[str, Any]:
        ...

    def get_order_status(self, order_id: str) -> dict[str, Any]:
        ...


class PyClobClientAdapter:
    def __init__(self, *, env: Optional[dict[str, str]] = None) -> None:
        self.env = env if env is not None else os.environ
        try:
            ClobClient, MarketOrderArgs, OrderType, ApiCreds, Side = import_clob_v2_sdk()
        except Exception as exc:  # pragma: no cover - depends on live optional dep
            raise RuntimeError(str(exc)) from exc
        self.ClobClient = ClobClient
        self.MarketOrderArgs = MarketOrderArgs
        self.OrderType = OrderType
        self.ApiCreds = ApiCreds
        self.Side = Side
        sdk_meta = clob_sdk_metadata()
        private_key = self.env.get("POLY_WALLET_PRIVATE_KEY")
        if not private_key:
            raise RuntimeError("POLY_WALLET_PRIVATE_KEY is required for live BTC5M canary execution")
        wallet_address = resolve_wallet_address(self.env)
        raw_funder = self.env.get("POLY_FUNDER")
        funder = raw_funder.strip() if raw_funder and raw_funder.strip() else wallet_address
        signature_type = parse_signature_type(self.env.get("POLY_SIGNATURE_TYPE", "0"))
        chain = int(self.env.get("POLYGON_CHAIN_ID", "137"))
        self.adapter_config = {
            "host": self.env.get("POLY_CLOB_BASE", "https://clob.polymarket.com"),
            "chain": chain,
            "signature_type": signature_type,
            "funder": funder,
            "funder_source": "POLY_FUNDER" if raw_funder and raw_funder.strip() else "wallet_address",
            "wallet_address": wallet_address,
            **sdk_meta,
        }
        self.client = self._make_client(private_key=private_key, creds=None)
        creds = None
        if self.env.get("POLY_API_KEY") and self.env.get("POLY_API_SECRET") and self.env.get("POLY_API_PASSPHRASE"):
            creds = make_api_creds(
                self.ApiCreds,
                api_key=self.env["POLY_API_KEY"],
                api_secret=self.env["POLY_API_SECRET"],
                api_passphrase=self.env["POLY_API_PASSPHRASE"],
            )
        else:
            creds = derive_api_creds(self.client)
        self._attach_api_creds(private_key=private_key, creds=creds)

    def _make_client(self, *, private_key: str, creds: Any = None) -> Any:
        kwargs = {
            "host": self.adapter_config["host"],
            "key": private_key,
            "signature_type": self.adapter_config["signature_type"],
            "funder": self.adapter_config["funder"],
        }
        if creds is not None:
            kwargs["creds"] = creds
        client_sig = inspect.signature(self.ClobClient)
        if "chain" in client_sig.parameters:
            kwargs["chain"] = self.adapter_config["chain"]
        else:
            kwargs["chain_id"] = self.adapter_config["chain"]
        return self.ClobClient(**kwargs)

    def _attach_api_creds(self, *, private_key: str, creds: Any) -> None:
        if hasattr(self.client, "set_api_creds"):
            self.client.set_api_creds(creds)
            return
        self.client = self._make_client(private_key=private_key, creds=creds)

    def wallet_address(self) -> Optional[str]:
        return self.adapter_config.get("wallet_address") or resolve_wallet_address(self.env)

    def redacted_adapter_config(self) -> dict[str, Any]:
        return {
            "clob_sdk_family": self.adapter_config.get("clob_sdk_family"),
            "clob_sdk_version": self.adapter_config.get("clob_sdk_version"),
            "host": self.adapter_config.get("host"),
            "chain": self.adapter_config.get("chain"),
            "signature_type": self.adapter_config.get("signature_type"),
            "funder_set": bool(self.adapter_config.get("funder")),
            "funder_source": self.adapter_config.get("funder_source"),
            "wallet_address": self.adapter_config.get("wallet_address"),
        }

    def submit_buy(self, intent: OrderIntent) -> dict[str, Any]:  # pragma: no cover - live adapter
        order_type = getattr(self.OrderType, "FAK", "FAK")
        buy_side = getattr(self.Side, "BUY", "BUY") if self.Side is not None else "BUY"
        order_args = self.MarketOrderArgs(
            token_id=str(intent.token_id),
            amount=float(intent.stake_usd),
            side=buy_side,
            price=float(intent.limit_price or intent.max_price or intent.selected_ask),
            order_type=order_type,
        )
        order = self.client.create_market_order(order_args)
        result = self.client.post_order(order, orderType="FAK", post_only=False)
        return result if isinstance(result, dict) else {"result": result}

    def get_order_status(self, order_id: str) -> dict[str, Any]:  # pragma: no cover - live adapter
        result = self.client.get_order(order_id)
        return result if isinstance(result, dict) else {"result": result}


class ExecutionJournal:
    def __init__(self, root: str | Path, *, now_fn=utc_now) -> None:
        self.root = Path(root)
        self.now_fn = now_fn

    @property
    def path(self) -> Path:
        now = self.now_fn()
        return self.root / now.strftime("%Y-%m-%d") / "execution_events.jsonl"

    def ensure_writable(self) -> None:
        path = self.path
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8"):
            pass

    def write(self, event: dict[str, Any]) -> None:
        path = self.path
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(event, sort_keys=True, default=str) + "\n")

    def recent_events(self) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        if not self.root.exists():
            return out
        for path in sorted(self.root.glob("*/execution_events.jsonl")):
            try:
                for line in path.read_text(encoding="utf-8").splitlines():
                    if line.strip():
                        out.append(json.loads(line))
            except FileNotFoundError:
                continue
        return out

    def has_blocking_duplicate(self, idempotency_key: str, *, allow_reentry_after_reject: bool = False) -> bool:
        blocking = LIVE_BLOCKING_EVENT_TYPES.copy()
        if not allow_reentry_after_reject:
            blocking |= {"order_rejected", "order_cancelled", "execution_error"}
        for event in self.recent_events():
            if event.get("idempotency_key") == idempotency_key and event.get("event_type") in blocking:
                return True
        return False


class CanaryExecutor:
    def __init__(
        self,
        config: ExecutionConfig,
        adapter: Optional[ClobOrderAdapter],
        journal: ExecutionJournal,
        *,
        now_fn=utc_now,
        sleep_fn=time.sleep,
    ) -> None:
        self.config = config
        self.adapter = adapter
        self.journal = journal
        self.now_fn = now_fn
        self.sleep_fn = sleep_fn
        self.order_attempts = 0

    def startup_check(self) -> dict[str, Any]:
        wallet = self.adapter.wallet_address() if self.adapter is not None else os.getenv("POLY_WALLET_ADDRESS")
        event = self._event("live_startup_check", wallet_address=wallet)
        if self.adapter is not None and hasattr(self.adapter, "redacted_adapter_config"):
            event["clob_adapter_config"] = self.adapter.redacted_adapter_config()  # type: ignore[attr-defined]
        errors: list[str] = []
        if self.config.execution_mode not in {"observe", "live"}:
            errors.append("invalid_execution_mode")
        if self.config.policy_config.policy_id != POLICY_ID or self.config.policy_config.identity_errors():
            errors.append("policy_identity_invalid")
        if self.config.execution_mode == "live":
            if not self.config.live_trading_enabled:
                errors.append("live_trading_disabled")
            if self.config.canary_stake_usd is None or self.config.canary_stake_usd <= 0:
                errors.append("missing_or_invalid_stake")
            if self.config.max_notional_per_market_usd is None or (
                self.config.canary_stake_usd is not None and self.config.max_notional_per_market_usd < self.config.canary_stake_usd
            ):
                errors.append("invalid_max_notional_per_market")
            if self.config.max_order_attempts_per_process < 1:
                errors.append("invalid_max_order_attempts")
            if self.adapter is None:
                errors.append("missing_clob_adapter")
            elif hasattr(self.adapter, "redacted_adapter_config"):
                adapter_config = self.adapter.redacted_adapter_config()  # type: ignore[attr-defined]
                if adapter_config.get("clob_sdk_family") != "py-clob-client-v2":
                    errors.append("clob_sdk_legacy_v1_refused")
                if not adapter_config.get("clob_sdk_version"):
                    errors.append("clob_sdk_version_unknown")
            if not wallet:
                errors.append("wallet_missing")
            if self.config.expected_wallet_address and normalize_address(wallet) != normalize_address(self.config.expected_wallet_address):
                errors.append("expected_wallet_mismatch")
        try:
            self.journal.ensure_writable()
        except Exception as exc:
            errors.append(f"journal_not_writable:{exc}")
        event["startup_ok"] = not errors
        event["startup_errors"] = errors
        self.journal.write(event)
        if errors and self.config.execution_mode == "live":
            raise RuntimeError("BTC5M live startup refused: " + ", ".join(errors))
        return event

    def execute_decision(self, decision: dict[str, Any]) -> dict[str, Any]:
        intent, skip_reason = create_order_intent(decision, self.config, wallet_address=self.wallet_address(), execution_ts=self.now_fn())
        if intent is None:
            event = self._event("execution_skipped", decision=decision, skip_reason=skip_reason)
            self.journal.write(event)
            return event
        if self.journal.has_blocking_duplicate(intent.idempotency_key, allow_reentry_after_reject=self.config.allow_reentry_after_reject):
            event = self._event("execution_skipped", intent=intent, skip_reason="duplicate_journal_entry")
            self.journal.write(event)
            return event
        if not self.config.is_live():
            event = self._event("execution_skipped", intent=intent, skip_reason="not_live_mode")
            self.journal.write(event)
            return event
        self.journal.write(self._event("order_intent_created", intent=intent))
        if self.order_attempts >= self.config.max_order_attempts_per_process:
            event = self._event("execution_skipped", intent=intent, skip_reason="max_order_attempts_reached")
            self.journal.write(event)
            return event
        if self.config.live_one_shot and self.order_attempts >= 1:
            event = self._event("live_one_shot_exit", intent=intent, skip_reason="live_one_shot_already_used")
            self.journal.write(event)
            return event
        if self.adapter is None:
            event = self._event("execution_error", intent=intent, raw_error_reason="missing_clob_adapter")
            self.journal.write(event)
            return event
        self.order_attempts += 1
        try:
            submitted = self.adapter.submit_buy(intent)
        except Exception as exc:
            normalized = normalize_clob_error(exc)
            event_type = "execution_rejected_by_venue" if normalized["terminal"] else "execution_error_after_submit"
            event = self._event(event_type, intent=intent, **normalized)
            self.journal.write(event)
            return event
        order_id = extract_order_id(submitted)
        submitted_event = self._event("live_order_submitted", intent=intent, order_id=order_id, clob_status=extract_status(submitted), raw_response=submitted)
        self.journal.write(submitted_event)
        final_event = self.poll_order(intent, order_id)
        if self.config.live_one_shot:
            self.journal.write(self._event("live_one_shot_exit", intent=intent, order_id=order_id))
        return final_event

    def poll_order(self, intent: OrderIntent, order_id: Optional[str]) -> dict[str, Any]:
        if not order_id:
            event = self._event("order_unknown_after_submit", intent=intent, raw_error_reason="missing_order_id")
            self.journal.write(event)
            return event
        deadline = time.monotonic() + self.config.order_poll_timeout_sec
        last_event: Optional[dict[str, Any]] = None
        while time.monotonic() <= deadline:
            status = self.adapter.get_order_status(order_id) if self.adapter is not None else {"status": "unknown"}
            clob_status = extract_status(status)
            event_type = event_type_for_status(clob_status)
            event = self._event(
                event_type,
                intent=intent,
                order_id=order_id,
                clob_status=clob_status,
                filled_size=extract_float(status, "filled_size", "filled_qty", "matched_size", "size_matched"),
                avg_fill_price=extract_float(status, "avg_fill_price", "average_price", "price"),
                remaining_size=extract_float(status, "remaining_size", "remaining_qty"),
                raw_response=status,
            )
            self.journal.write(event)
            last_event = event
            if event_type in {"order_filled", "order_partially_filled", "order_rejected", "order_cancelled"}:
                return event
            self.sleep_fn(self.config.order_poll_interval_sec)
        event = self._event("order_unknown_after_submit", intent=intent, order_id=order_id, clob_status=last_event.get("clob_status") if last_event else None)
        self.journal.write(event)
        return event

    def wallet_address(self) -> Optional[str]:
        if self.adapter is not None:
            return self.adapter.wallet_address()
        return os.getenv("POLY_WALLET_ADDRESS")

    def _event(self, event_type: str, *, intent: Optional[OrderIntent] = None, decision: Optional[dict[str, Any]] = None, **extra: Any) -> dict[str, Any]:
        source = decision or {}
        event = {
            "event_type": event_type,
            "policy_id": getattr(intent, "policy_id", None) or source.get("policy_id") or POLICY_ID,
            "market_id": getattr(intent, "market_id", None) or source.get("market_id"),
            "condition_id": getattr(intent, "condition_id", None) or source.get("condition_id"),
            "yes_token_id": getattr(intent, "yes_token_id", None) or source.get("yes_token_id"),
            "no_token_id": getattr(intent, "no_token_id", None) or source.get("no_token_id"),
            "token_id": getattr(intent, "token_id", None),
            "selected_side": getattr(intent, "selected_side", None) or source.get("selected_side"),
            "selected_ask": getattr(intent, "selected_ask", None) or source.get("selected_ask"),
            "selected_edge": getattr(intent, "selected_edge", None) or source.get("selected_edge"),
            "stake_usd": getattr(intent, "stake_usd", None) or self.config.canary_stake_usd,
            "limit_price": getattr(intent, "limit_price", None),
            "max_price": getattr(intent, "max_price", None),
            "market_age_sec": getattr(intent, "market_age_sec", None) or source.get("market_age_sec"),
            "decision_ts": getattr(intent, "decision_ts", None) or source.get("decision_ts"),
            "execution_ts": isoformat_utc(self.now_fn()),
            "quote_ts": getattr(intent, "quote_ts", None) or source.get("quote_ts"),
            "quote_age_ms": getattr(intent, "quote_age_ms", None) or source.get("quote_age_ms"),
            "client_order_id": getattr(intent, "client_order_id", None),
            "idempotency_key": getattr(intent, "idempotency_key", None),
            "wallet_address": extra.pop("wallet_address", self.wallet_address()),
            "execution_mode": self.config.execution_mode,
            "live_trading_enabled": self.config.live_trading_enabled,
            "live_one_shot": self.config.live_one_shot,
        }
        event.update(extra)
        return event


def create_order_intent(
    decision: dict[str, Any],
    config: ExecutionConfig,
    *,
    wallet_address: Optional[str],
    execution_ts: Optional[datetime] = None,
) -> tuple[Optional[OrderIntent], Optional[str]]:
    skip = validate_executable_decision(decision, config, execution_ts=execution_ts)
    if skip:
        return None, skip
    side = str(decision["selected_side"]).upper()
    token_id = decision.get("yes_token_id") if side == "YES" else decision.get("no_token_id")
    if not token_id:
        return None, "selected_token_missing"
    selected_ask = float(decision["selected_ask"])
    max_price = min(selected_ask + config.max_price_slippage, config.max_limit_price)
    limit_price = quantize_price(max_price)
    if config.canary_stake_usd is None or config.canary_stake_usd <= 0:
        return None, "missing_or_invalid_stake"
    key = idempotency_key(
        policy_id=str(decision.get("policy_id")),
        condition_id=decision.get("condition_id"),
        market_id=decision.get("market_id"),
        token_id=str(token_id),
        side=side,
        market_start_ts=decision.get("market_start_ts"),
        wallet_address=wallet_address,
    )
    client_order_id = f"btc5m-{key[:24]}"
    return (
        OrderIntent(
            policy_id=str(decision.get("policy_id")),
            market_id=decision.get("market_id"),
            condition_id=decision.get("condition_id"),
            token_id=str(token_id),
            selected_side=side,
            action="BUY",
            selected_ask=selected_ask,
            selected_edge=float(decision["selected_edge"]),
            stake_usd=float(config.canary_stake_usd),
            market_age_sec=float(decision["market_age_sec"]),
            decision_ts=decision.get("decision_ts"),
            quote_ts=decision.get("quote_ts"),
            quote_age_ms=_optional_float(decision.get("quote_age_ms")),
            client_order_id=client_order_id,
            idempotency_key=key,
            market_start_ts=decision.get("market_start_ts"),
            yes_token_id=decision.get("yes_token_id"),
            no_token_id=decision.get("no_token_id"),
            limit_price=limit_price,
            max_price=max_price,
        ),
        None,
    )


def validate_executable_decision(decision: dict[str, Any], config: ExecutionConfig, *, execution_ts: Optional[datetime] = None) -> Optional[str]:
    provenance_error = validate_decision_provenance(decision, config, now=execution_ts)
    if provenance_error:
        return provenance_error
    if decision.get("policy_id") != POLICY_ID:
        return "decision_policy_mismatch"
    if decision.get("final_decision") not in {"BUY_YES", "BUY_NO"}:
        return "not_buy_decision"
    if decision.get("final_decision") == "SHADOW_ONLY":
        return "shadow_only"
    for gate in ["market_age_gate_pass", "hmm_gate_pass", "model_gate_pass", "ask_filter_pass", "edge_gate_pass", "risk_gate_pass", "one_entry_gate_pass"]:
        if decision.get(gate) is not True:
            return f"{gate}_false"
    age = current_market_age(decision, execution_ts=execution_ts)
    if age is None:
        return "decision_market_age_invalid"
    if age < 60.0:
        return "decision_market_age_invalid"
    if age > 240.0:
        return "decision_market_age_invalid"
    quote_age = _optional_float(decision.get("quote_age_ms"))
    if quote_age is not None and quote_age > config.max_quote_age_ms:
        return "decision_quote_stale"
    ask = _optional_float(decision.get("selected_ask"))
    if ask is None:
        return "selected_ask_missing"
    if not (0.30 < ask < 0.47):
        return "ask_filter_failed"
    edge = _optional_float(decision.get("selected_edge"))
    min_edge = config.policy_config.min_edge
    if edge is None or min_edge is None or edge < min_edge:
        return "edge_below_threshold"
    return None


def validate_decision_provenance(decision: dict[str, Any], config: ExecutionConfig, *, now: Optional[datetime] = None) -> Optional[str]:
    if decision.get("generated_by") != "btc5m_canary_policy_evaluator":
        return "decision_provenance_missing"
    if decision.get("policy_id") != POLICY_ID:
        return "decision_policy_mismatch"
    if decision.get("config_hash") != config.policy_config.config_hash():
        return "decision_config_hash_mismatch"
    generated_ts = parse_datetime(decision.get("generated_ts"))
    expires_ts = parse_datetime(decision.get("expires_ts"))
    if generated_ts is None or expires_ts is None or not decision.get("input_hash"):
        return "decision_provenance_missing"
    ref = now or utc_now()
    if ref > expires_ts:
        return "decision_expired"
    quote_age = _optional_float(decision.get("quote_age_ms"))
    if quote_age is not None and quote_age > config.max_quote_age_ms:
        return "decision_quote_stale"
    age = current_market_age(decision, execution_ts=ref)
    if age is None or age < 60.0 or age > 240.0:
        return "decision_market_age_invalid"
    if decision.get("model_id") != "brownian_zero_drift__rv30":
        return "decision_component_mismatch"
    try:
        hmm_state = int(decision.get("hmm_state", -1))
    except (TypeError, ValueError):
        hmm_state = -1
    if decision.get("hmm_model_id") != "laplace_1m__gaussian_hmm__k4" or hmm_state != 3:
        return "decision_component_mismatch"
    return None


def add_decision_provenance(
    decision: dict[str, Any],
    *,
    policy_config: CanaryConfig,
    input_payload: dict[str, Any],
    now: Optional[datetime] = None,
    expiry_ms: int = 2000,
) -> dict[str, Any]:
    generated = now or utc_now()
    expires = generated.timestamp() + (int(expiry_ms) / 1000.0)
    enriched = dict(decision)
    enriched.update(
        {
            "generated_by": "btc5m_canary_policy_evaluator",
            "config_hash": policy_config.config_hash(),
            "input_hash": stable_hash(input_payload),
            "generated_ts": isoformat_utc(generated),
            "expires_ts": isoformat_utc(datetime.fromtimestamp(expires, tz=timezone.utc)),
        }
    )
    return enriched


def stable_hash(payload: dict[str, Any]) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def current_market_age(decision: dict[str, Any], *, execution_ts: Optional[datetime]) -> Optional[float]:
    start = parse_datetime(decision.get("market_start_ts"))
    if start is not None and execution_ts is not None:
        return (execution_ts - start).total_seconds()
    return _optional_float(decision.get("market_age_sec"))


def idempotency_key(**parts: Any) -> str:
    payload = "|".join(str(parts.get(key) or "") for key in ["policy_id", "condition_id", "market_id", "token_id", "side", "market_start_ts", "wallet_address"])
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def quantize_price(value: float) -> float:
    dec = Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
    return float(dec)


def extract_order_id(response: dict[str, Any]) -> Optional[str]:
    for key in ["order_id", "orderID", "id"]:
        if response.get(key):
            return str(response[key])
    return None


def extract_status(response: dict[str, Any]) -> str:
    return str(response.get("status") or response.get("clob_status") or "unknown").lower()


def event_type_for_status(status: str) -> str:
    value = str(status or "unknown").lower()
    if value in FINAL_SUCCESS_STATUSES:
        return "order_filled"
    if value in PARTIAL_STATUSES:
        return "order_partially_filled"
    if value in REJECTED_STATUSES:
        return "order_rejected"
    if value in CANCELLED_STATUSES:
        return "order_cancelled"
    return "order_status_polled"


def extract_float(response: dict[str, Any], *keys: str) -> Optional[float]:
    for key in keys:
        if key in response and response[key] is not None:
            return _optional_float(response[key])
    return None


def normalize_address(value: Optional[str]) -> Optional[str]:
    return str(value).strip().lower() if value else None


def clob_sdk_metadata() -> dict[str, Optional[str]]:
    try:
        version = importlib.metadata.version("py-clob-client-v2")
    except importlib.metadata.PackageNotFoundError:
        version = None
    return {"clob_sdk_family": "py-clob-client-v2", "clob_sdk_version": version}


def import_clob_v2_sdk() -> tuple[Any, Any, Any, Any, Any]:
    if importlib.util.find_spec("py_clob_client_v2") is None:
        if importlib.util.find_spec("py_clob_client") is not None:
            raise RuntimeError("clob_sdk_legacy_v1_refused: install py-clob-client-v2 and remove py-clob-client")
        raise RuntimeError("py-clob-client-v2 is required for live BTC5M canary execution")
    try:
        from py_clob_client_v2 import ApiCreds, ClobClient, MarketOrderArgs, OrderType, Side
    except ImportError:
        from py_clob_client_v2 import ApiCreds, ClobClient, MarketOrderArgs, OrderType

        Side = None
    return ClobClient, MarketOrderArgs, OrderType, ApiCreds, Side


def parse_signature_type(value: str) -> int:
    try:
        parsed = int(str(value).strip())
    except (TypeError, ValueError) as exc:
        raise RuntimeError("unsupported_poly_signature_type") from exc
    if parsed not in {0, 1, 2}:
        raise RuntimeError("unsupported_poly_signature_type")
    return parsed


def make_api_creds(api_creds_cls: Any, *, api_key: str, api_secret: str, api_passphrase: str) -> Any:
    try:
        return api_creds_cls(api_key=api_key, api_secret=api_secret, api_passphrase=api_passphrase)
    except TypeError:
        try:
            return api_creds_cls(apiKey=api_key, secret=api_secret, passphrase=api_passphrase)
        except TypeError:
            return {"apiKey": api_key, "secret": api_secret, "passphrase": api_passphrase}


def derive_api_creds(client: Any) -> Any:
    if hasattr(client, "create_or_derive_api_key"):
        return client.create_or_derive_api_key()
    if hasattr(client, "create_or_derive_api_creds"):
        return client.create_or_derive_api_creds()
    raise RuntimeError("clob_sdk_missing_api_credential_derivation")


def resolve_wallet_address(env: dict[str, str]) -> Optional[str]:
    if env.get("POLY_WALLET_ADDRESS") or env.get("POLY_ADDRESS"):
        return env.get("POLY_WALLET_ADDRESS") or env.get("POLY_ADDRESS")
    private_key = env.get("POLY_WALLET_PRIVATE_KEY")
    if not private_key:
        return None
    try:
        from eth_account import Account

        return Account.from_key(private_key).address
    except Exception:
        return None


def normalize_clob_error(exc: BaseException) -> dict[str, Any]:
    raw = str(exc)
    lowered = raw.lower()
    code = "clob_submit_error"
    terminal = False
    retryable = True
    if "order_version_mismatch" in lowered:
        code = "order_version_mismatch"
        terminal = True
        retryable = False
    elif "not enough balance" in lowered or "allowance" in lowered:
        code = "balance_or_allowance"
        terminal = True
        retryable = False
    elif "maker address not allowed" in lowered:
        code = "maker_address_not_allowed"
        terminal = True
        retryable = False
    return {
        "error_code": code,
        "raw_error_reason": raw,
        "terminal": terminal,
        "retryable": retryable,
        **clob_sdk_metadata(),
    }


def _optional_float(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _env_bool(value: Optional[str]) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}
