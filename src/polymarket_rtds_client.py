from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime, timezone
from typing import Any, Optional

from .polymarket_api import POLY_RTDS_URL
from .time_utils import isoformat_utc, utc_now

try:
    import websockets
except Exception:  # pragma: no cover
    websockets = None


RTDS_RETRY_DELAY_SEC = max(1.0, float(os.getenv("POLY_RTDS_RETRY_DELAY_SEC", "5")))
RTDS_OPEN_TIMEOUT_SEC = max(1.0, float(os.getenv("POLY_RTDS_OPEN_TIMEOUT_SEC", "10")))


def _parse_json_env(name: str) -> list[dict[str, Any]]:
    raw = os.getenv(name, "").strip()
    if not raw:
        return []
    try:
        parsed = json.loads(raw)
    except Exception:
        return []
    if isinstance(parsed, dict):
        return [parsed]
    if isinstance(parsed, list):
        return [item for item in parsed if isinstance(item, dict)]
    return []


def default_chainlink_subscribe_payloads() -> list[dict[str, Any]]:
    payloads = _parse_json_env("POLY_CHAINLINK_SUBSCRIBE_PAYLOADS")
    if payloads:
        return payloads
    return [
        {
            "action": "subscribe",
            "subscriptions": [
                {
                    "topic": "crypto_prices_chainlink",
                    "type": "update",
                    "filters": "{\"symbol\":\"btc/usd\"}",
                }
            ],
        }
    ]


def _find_numeric(payload: Any, keys: tuple[str, ...]) -> Optional[float]:
    if payload is None:
        return None
    if isinstance(payload, (int, float)):
        return float(payload)
    if isinstance(payload, str):
        try:
            return float(payload)
        except Exception:
            return None
    if isinstance(payload, dict):
        for key in keys:
            value = payload.get(key)
            if value is not None:
                parsed = _find_numeric(value, keys)
                if parsed is not None:
                    return parsed
        for nested_key in ("data", "payload", "event", "price"):
            nested = payload.get(nested_key)
            parsed = _find_numeric(nested, keys)
            if parsed is not None:
                return parsed
    if isinstance(payload, list):
        for item in payload:
            parsed = _find_numeric(item, keys)
            if parsed is not None:
                return parsed
    return None


def _coerce_source_ts(value: Any) -> Optional[str]:
    if value in (None, ""):
        return None
    try:
        if isinstance(value, (int, float)) or (isinstance(value, str) and value.isdigit()):
            raw = int(value)
            if raw > 10_000_000_000:
                dt = datetime.fromtimestamp(raw / 1000.0, tz=timezone.utc)
            else:
                dt = datetime.fromtimestamp(raw, tz=timezone.utc)
            return dt.isoformat()
    except Exception:
        pass
    return str(value)


def normalize_chainlink_message(message: Any) -> dict[str, Any]:
    now = utc_now()
    if message == "":
        return {
            "ts": isoformat_utc(now),
            "received_ts": isoformat_utc(now),
            "record_type": "keepalive",
            "source": "polymarket_rtds",
            "source_ts": None,
            "symbol": None,
            "value": None,
            "topic": None,
            "type": None,
            "message_family": "empty_frame",
            "full_accuracy_value": None,
            "warning": None,
            "raw_payload_fragment": message,
        }
    if not isinstance(message, (dict, list)):
        return {
            "ts": isoformat_utc(now),
            "received_ts": isoformat_utc(now),
            "record_type": "warning",
            "source": "polymarket_rtds",
            "source_ts": None,
            "symbol": "BTC/USD",
            "value": None,
            "topic": None,
            "type": None,
            "message_family": "non_json_message",
            "full_accuracy_value": None,
            "warning": "non_json_message",
            "raw_payload_fragment": str(message),
        }
    symbol = "btc/usd"
    topic = None
    event_type = None
    source_ts = None
    message_family = "unknown"
    price = None
    full_accuracy_value = None

    if isinstance(message, dict):
        topic = message.get("topic")
        event_type = message.get("type")
        envelope_payload = message.get("payload") if isinstance(message.get("payload"), dict) else message
        symbol = str(envelope_payload.get("symbol") or message.get("symbol") or message.get("pair") or symbol)
        data_items = envelope_payload.get("data")
        if isinstance(data_items, list) and data_items:
            latest = data_items[-1] if isinstance(data_items[-1], dict) else None
            if latest is not None:
                source_ts = _coerce_source_ts(latest.get("timestamp") or latest.get("ts") or latest.get("time"))
                price = _find_numeric(latest, ("value", "price", "answer"))
                if topic == "crypto_prices" and event_type == "subscribe":
                    message_family = "snapshot_subscribe"
                else:
                    message_family = "data_points"
        if price is None and envelope_payload.get("full_accuracy_value") is not None:
            full_accuracy_value = str(envelope_payload.get("full_accuracy_value"))
            price = _find_numeric(envelope_payload, ("value", "full_accuracy_value", "price", "answer"))
            source_ts = _coerce_source_ts(
                envelope_payload.get("timestamp")
                or envelope_payload.get("ts")
                or envelope_payload.get("time")
            )
            if topic == "crypto_prices_chainlink" and event_type == "update":
                message_family = "live_update"
            else:
                message_family = "full_accuracy_value"
        if source_ts is None:
            source_ts = _coerce_source_ts(
                envelope_payload.get("timestamp")
                or envelope_payload.get("ts")
                or envelope_payload.get("time")
                or message.get("timestamp")
            )

    if price is None:
        price = _find_numeric(message, ("price", "value", "answer", "mark_price", "aggregate_price", "full_accuracy_value"))

    return {
        "ts": isoformat_utc(now),
        "received_ts": isoformat_utc(now),
        "record_type": "tick" if price is not None else "warning",
        "source": "polymarket_rtds",
        "source_ts": source_ts,
        "symbol": symbol,
        "value": price,
        "price": price,
        "topic": topic,
        "type": event_type,
        "message_family": message_family,
        "full_accuracy_value": full_accuracy_value,
        "warning": None if price is not None else "unparsed_chainlink_payload",
        "raw_payload_fragment": message,
    }


class PolymarketRTDSClient:
    def __init__(self, *, url: Optional[str] = None, subscribe_payloads: Optional[list[dict[str, Any]]] = None) -> None:
        self.url = (url or POLY_RTDS_URL or "").strip()
        self.subscribe_payloads = subscribe_payloads or default_chainlink_subscribe_payloads()
        self.connected = False
        self.last_message_ts: Optional[str] = None
        self.last_error: Optional[str] = None

    @property
    def enabled(self) -> bool:
        return bool(self.url and websockets is not None)

    async def run(self, queue: asyncio.Queue[dict[str, Any]], stop_event: asyncio.Event) -> None:
        if not self.url:
            await queue.put(
                {
                    "ts": isoformat_utc(utc_now()),
                    "record_type": "warning",
                    "source": "polymarket_rtds",
                    "symbol": "BTC/USD",
                    "warning": "missing_poly_rtds_url",
                    "raw_payload_fragment": {"env": "POLY_RTDS_URL"},
                }
            )
            return
        if websockets is None:
            await queue.put(
                {
                    "ts": isoformat_utc(utc_now()),
                    "record_type": "warning",
                    "source": "polymarket_rtds",
                    "symbol": "BTC/USD",
                    "warning": "websockets_dependency_unavailable",
                    "raw_payload_fragment": {"url": self.url},
                }
            )
            return

        while not stop_event.is_set():
            try:
                async with websockets.connect(self.url, open_timeout=RTDS_OPEN_TIMEOUT_SEC) as ws:
                    self.connected = True
                    self.last_error = None
                    for payload in self.subscribe_payloads:
                        await ws.send(json.dumps(payload))
                    while not stop_event.is_set():
                        raw = await asyncio.wait_for(ws.recv(), timeout=1.0)
                        self.last_message_ts = isoformat_utc(utc_now())
                        try:
                            message = json.loads(raw)
                        except Exception:
                            message = raw
                        normalized = normalize_chainlink_message(message)
                        if normalized.get("message_family") == "empty_frame":
                            continue
                        await queue.put(normalized)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.connected = False
                self.last_error = str(exc)
                await queue.put(
                    {
                        "ts": isoformat_utc(utc_now()),
                        "record_type": "warning",
                        "source": "polymarket_rtds",
                        "symbol": "BTC/USD",
                        "warning": "rtds_reconnect",
                        "error": str(exc),
                        "raw_payload_fragment": {"url": self.url},
                    }
                )
                await asyncio.sleep(RTDS_RETRY_DELAY_SEC)
