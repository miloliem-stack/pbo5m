from __future__ import annotations

import asyncio
import json
import os
from typing import Any, Optional
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from .time_utils import isoformat_utc, utc_now

try:
    import websockets
except Exception:  # pragma: no cover
    websockets = None


BINANCE_WS_URL = os.getenv("BINANCE_WS_URL", "wss://stream.binance.com:9443/ws/btcusdt@trade")
BINANCE_REST_TICKER_URL = os.getenv("BINANCE_REST_TICKER_URL", "https://api.binance.com/api/v3/ticker/price")
BINANCE_RETRY_DELAY_SEC = max(1.0, float(os.getenv("BINANCE_RETRY_DELAY_SEC", "5")))
BINANCE_POLL_SECONDS = max(1.0, float(os.getenv("BINANCE_POLL_SECONDS", "3")))


def normalize_binance_message(message: Any) -> dict[str, Any]:
    now = utc_now()
    if not isinstance(message, dict):
        return {
            "ts": isoformat_utc(now),
            "record_type": "warning",
            "source": "binance_ws",
            "symbol": "BTCUSDT",
            "warning": "non_json_message",
            "raw_payload_fragment": str(message),
        }
    price = message.get("p") or message.get("price")
    event_time = message.get("E") or message.get("T") or message.get("eventTime")
    try:
        parsed_price = float(price) if price is not None else None
    except Exception:
        parsed_price = None
    return {
        "ts": isoformat_utc(now),
        "record_type": "tick" if parsed_price is not None else "warning",
        "source": "binance_ws",
        "symbol": "BTCUSDT",
        "price": parsed_price,
        "observed_at": event_time,
        "warning": None if parsed_price is not None else "unparsed_binance_payload",
        "raw_payload_fragment": message,
    }


def rest_binance_price_row() -> dict[str, Any]:
    now = utc_now()
    url = f"{BINANCE_REST_TICKER_URL}?{urlencode({'symbol': 'BTCUSDT'})}"
    request = Request(url, headers={"Accept": "application/json"})
    with urlopen(request, timeout=5) as response:
        payload = json.loads(response.read().decode("utf-8"))
    price = payload.get("price")
    return {
        "ts": isoformat_utc(now),
        "record_type": "tick",
        "source": "binance_rest",
        "symbol": "BTCUSDT",
        "price": float(price),
        "observed_at": None,
        "warning": None,
        "raw_payload_fragment": payload,
    }


class BinancePriceFeed:
    def __init__(self, *, ws_url: Optional[str] = None) -> None:
        self.ws_url = ws_url or BINANCE_WS_URL
        self.connected = False
        self.using_rest_fallback = False
        self.last_message_ts: Optional[str] = None
        self.last_error: Optional[str] = None

    async def run(self, queue: asyncio.Queue[dict[str, Any]], stop_event: asyncio.Event) -> None:
        if websockets is None:
            await self._run_rest_poll(queue, stop_event, initial_warning="websockets_dependency_unavailable")
            return
        while not stop_event.is_set():
            try:
                async with websockets.connect(self.ws_url, open_timeout=10) as ws:
                    self.connected = True
                    self.using_rest_fallback = False
                    self.last_error = None
                    while not stop_event.is_set():
                        raw = await asyncio.wait_for(ws.recv(), timeout=1.0)
                        self.last_message_ts = isoformat_utc(utc_now())
                        try:
                            message = json.loads(raw)
                        except Exception:
                            message = raw
                        await queue.put(normalize_binance_message(message))
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
                        "source": "binance_ws",
                        "symbol": "BTCUSDT",
                        "warning": "ws_reconnect_rest_fallback",
                        "error": str(exc),
                        "raw_payload_fragment": {"url": self.ws_url},
                    }
                )
                await self._run_rest_poll(queue, stop_event)

    async def _run_rest_poll(self, queue: asyncio.Queue[dict[str, Any]], stop_event: asyncio.Event, initial_warning: Optional[str] = None) -> None:
        self.using_rest_fallback = True
        if initial_warning:
            await queue.put(
                {
                    "ts": isoformat_utc(utc_now()),
                    "record_type": "warning",
                    "source": "binance_rest",
                    "symbol": "BTCUSDT",
                    "warning": initial_warning,
                    "raw_payload_fragment": {"url": BINANCE_REST_TICKER_URL},
                }
            )
        while not stop_event.is_set():
            try:
                row = await asyncio.to_thread(rest_binance_price_row)
                self.last_message_ts = row["ts"]
                await queue.put(row)
            except Exception as exc:
                self.last_error = str(exc)
                await queue.put(
                    {
                        "ts": isoformat_utc(utc_now()),
                        "record_type": "warning",
                        "source": "binance_rest",
                        "symbol": "BTCUSDT",
                        "warning": "rest_poll_failed",
                        "error": str(exc),
                        "raw_payload_fragment": {"url": BINANCE_REST_TICKER_URL},
                    }
                )
            await asyncio.sleep(BINANCE_POLL_SECONDS)
            if websockets is not None and initial_warning is None:
                return
