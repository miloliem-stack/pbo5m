from __future__ import annotations

import argparse
import asyncio
import json
import os
import socket
import ssl
import sys
from pathlib import Path
from typing import Any, Optional

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.jsonl_writer import JsonlWriter
from src.polymarket_api import POLY_RTDS_URL
from src.polymarket_rtds_client import default_chainlink_subscribe_payloads
from src.time_utils import isoformat_utc, utc_now

try:
    import websockets
    from websockets.exceptions import ConnectionClosed, InvalidHandshake, InvalidStatus, InvalidURI
except Exception:  # pragma: no cover
    websockets = None
    ConnectionClosed = Exception
    InvalidHandshake = Exception
    InvalidStatus = Exception
    InvalidURI = Exception


DEFAULT_OUTPUT_PATH = "artifacts/rtds_probe/polymarket_rtds_probe.jsonl"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Probe the Polymarket RTDS websocket before running the BTC-5m recorder.",
        epilog=(
            "Typical flow: set POLY_RTDS_URL plus any optional POLY_RTDS_HEADERS_JSON or "
            "POLY_RTDS_SUBSCRIBE_PAYLOADS, then run this probe first to confirm connect, "
            "subscribe, pong, and inbound-frame behavior. Default subscribe payload targets "
            "the Polymarket browser-observed Chainlink BTC/USD stream."
        ),
    )
    parser.add_argument("--url", default=POLY_RTDS_URL or "")
    parser.add_argument("--output", default=DEFAULT_OUTPUT_PATH)
    parser.add_argument("--open-timeout-seconds", type=float, default=float(os.getenv("POLY_RTDS_OPEN_TIMEOUT_SEC", "10")))
    parser.add_argument("--message-timeout-seconds", type=float, default=float(os.getenv("POLY_RTDS_MESSAGE_TIMEOUT_SEC", "10")))
    parser.add_argument("--ping-timeout-seconds", type=float, default=float(os.getenv("POLY_RTDS_PING_TIMEOUT_SEC", "5")))
    parser.add_argument("--max-messages", type=int, default=int(os.getenv("POLY_RTDS_PROBE_MAX_MESSAGES", "5")))
    parser.add_argument("--subprotocols-json", default=os.getenv("POLY_RTDS_SUBPROTOCOLS_JSON", ""))
    parser.add_argument("--headers-json", default=os.getenv("POLY_RTDS_HEADERS_JSON", ""))
    parser.add_argument("--subscribe-payloads-json", default=os.getenv("POLY_RTDS_SUBSCRIBE_PAYLOADS", ""))
    return parser.parse_args()


def _parse_json_mapping(raw: str) -> dict[str, str]:
    text = str(raw or "").strip()
    if not text:
        return {}
    parsed = json.loads(text)
    if not isinstance(parsed, dict):
        raise ValueError("expected JSON object")
    return {str(key): str(value) for key, value in parsed.items()}


def _parse_json_list(raw: str) -> list[Any]:
    text = str(raw or "").strip()
    if not text:
        return []
    parsed = json.loads(text)
    if isinstance(parsed, list):
        return parsed
    return [parsed]


def _load_subscribe_payloads(raw: str) -> list[Any]:
    try:
        payloads = _parse_json_list(raw)
    except Exception:
        payloads = []
    if payloads:
        return payloads
    return default_chainlink_subscribe_payloads()


def _connect_kwargs(args: argparse.Namespace) -> dict[str, Any]:
    headers = _parse_json_mapping(args.headers_json)
    subprotocols = [str(item) for item in _parse_json_list(args.subprotocols_json)] if args.subprotocols_json.strip() else None
    kwargs: dict[str, Any] = {
        "open_timeout": max(1.0, float(args.open_timeout_seconds)),
        "ping_interval": None,
    }
    if headers:
        kwargs["extra_headers"] = headers
    if subprotocols:
        kwargs["subprotocols"] = subprotocols
    return kwargs


def _classify_exception(exc: BaseException) -> str:
    if isinstance(exc, InvalidURI):
        return "bad_url"
    if isinstance(exc, socket.gaierror):
        return "bad_url_dns"
    if isinstance(exc, TimeoutError):
        return "timeout"
    if isinstance(exc, ssl.SSLError):
        return "handshake_failure"
    if isinstance(exc, InvalidStatus):
        status = getattr(exc, "status_code", None) or getattr(getattr(exc, "response", None), "status_code", None)
        if status in (401, 403):
            return "auth_header_failure"
        return "handshake_failure"
    if isinstance(exc, InvalidHandshake):
        return "handshake_failure"
    if isinstance(exc, ConnectionClosed):
        return "connection_closed"
    return "probe_error"


def _diagnostic_row(category: str, **fields: Any) -> dict[str, Any]:
    row = {"ts": isoformat_utc(utc_now()), "event_type": "diagnostic", "category": category}
    row.update(fields)
    return row


def _safe_json(raw: str) -> Any:
    try:
        return json.loads(raw)
    except Exception:
        return raw


async def _probe(args: argparse.Namespace) -> int:
    if websockets is None:
        print("WEBSOCKETS_DEPENDENCY_UNAVAILABLE", flush=True)
        return 2
    if not args.url.strip():
        print("BAD_URL missing POLY_RTDS_URL or --url", flush=True)
        return 2

    output_path = Path(args.output)
    writer = JsonlWriter(output_path)
    subscribe_payloads = _load_subscribe_payloads(args.subscribe_payloads_json)
    connect_kwargs = _connect_kwargs(args)
    message_timeout = max(0.1, float(args.message_timeout_seconds))
    ping_timeout = max(0.1, float(args.ping_timeout_seconds))
    max_messages = max(1, int(args.max_messages))
    inbound_count = 0
    saw_any_data = False

    writer.write(
        _diagnostic_row(
            "probe_start",
            url=args.url,
            output=str(output_path),
            subscribe_payloads=subscribe_payloads,
            connect_kwargs={key: value for key, value in connect_kwargs.items() if key != "extra_headers"} | {
                "has_headers": "extra_headers" in connect_kwargs
            },
        )
    )
    print(f"CONNECT_START url={args.url}", flush=True)

    try:
        async with websockets.connect(args.url, **connect_kwargs) as ws:
            writer.write(
                _diagnostic_row(
                    "connect_success",
                    url=args.url,
                    subprotocol=getattr(ws, "subprotocol", None),
                    local_address=str(getattr(ws, "local_address", None)),
                    remote_address=str(getattr(ws, "remote_address", None)),
                )
            )
            print("CONNECT_SUCCESS", flush=True)

            try:
                pong_waiter = await ws.ping()
                await asyncio.wait_for(pong_waiter, timeout=ping_timeout)
                writer.write(_diagnostic_row("ping_pong_success", timeout_seconds=ping_timeout))
                print("PING_PONG_SUCCESS", flush=True)
            except Exception as exc:
                category = "ping_pong_failure"
                writer.write(_diagnostic_row(category, error=str(exc), error_class=type(exc).__name__))
                print(f"PING_PONG_FAILURE error={exc}", flush=True)

            for index, payload in enumerate(subscribe_payloads, start=1):
                try:
                    encoded = json.dumps(payload)
                    await ws.send(encoded)
                    writer.write(_diagnostic_row("subscribe_sent", index=index, payload=payload))
                    print(f"SUBSCRIBE_SENT index={index}", flush=True)
                except Exception as exc:
                    category = "invalid_subscribe_payload"
                    writer.write(
                        _diagnostic_row(
                            category,
                            index=index,
                            payload=payload,
                            error=str(exc),
                            error_class=type(exc).__name__,
                        )
                    )
                    print(f"INVALID_SUBSCRIBE_PAYLOAD index={index} error={exc}", flush=True)
                    return 1

            while inbound_count < max_messages:
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=message_timeout)
                except asyncio.TimeoutError:
                    writer.write(
                        _diagnostic_row(
                            "silent_subscribe_no_data",
                            message_timeout_seconds=message_timeout,
                            inbound_count=inbound_count,
                        )
                    )
                    print(f"SILENT_SUBSCRIBE_NO_DATA timeout={message_timeout}s inbound_count={inbound_count}", flush=True)
                    return 1 if inbound_count == 0 else 0
                except ConnectionClosed as exc:
                    writer.write(
                        _diagnostic_row(
                            "connection_closed_after_subscribe",
                            close_code=getattr(exc, "code", None),
                            close_reason=getattr(exc, "reason", None),
                            inbound_count=inbound_count,
                        )
                    )
                    print(
                        f"CONNECTION_CLOSED_AFTER_SUBSCRIBE code={getattr(exc, 'code', None)} "
                        f"reason={getattr(exc, 'reason', None)}",
                        flush=True,
                    )
                    return 1

                inbound_count += 1
                saw_any_data = True
                writer.write(
                    {
                        "ts": isoformat_utc(utc_now()),
                        "event_type": "inbound_frame",
                        "frame_index": inbound_count,
                        "payload": _safe_json(raw),
                        "raw_text": raw if isinstance(raw, str) else None,
                    }
                )
                print(f"INBOUND_FRAME index={inbound_count}", flush=True)

            writer.write(_diagnostic_row("probe_success", inbound_count=inbound_count))
            print(f"PROBE_SUCCESS inbound_count={inbound_count}", flush=True)
            return 0
    except Exception as exc:
        category = _classify_exception(exc)
        status = getattr(exc, "status_code", None) or getattr(getattr(exc, "response", None), "status_code", None)
        headers = getattr(getattr(exc, "response", None), "headers", None)
        writer.write(
            _diagnostic_row(
                category,
                error=str(exc),
                error_class=type(exc).__name__,
                http_status=status,
                response_headers=dict(headers) if headers is not None else None,
                saw_any_data=saw_any_data,
            )
        )
        print(f"{category.upper()} error={exc}", flush=True)
        return 1
    finally:
        writer.close()


def main() -> int:
    args = parse_args()
    return asyncio.run(_probe(args))


if __name__ == "__main__":
    raise SystemExit(main())
