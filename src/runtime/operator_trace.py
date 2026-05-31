from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from typing import Any, Optional

_SECRET_PARTS = (
    "PRIVATE_KEY",
    "SECRET",
    "TOKEN",
    "PASSPHRASE",
    "PASSWORD",
    "API_KEY",
)
_MAX_VALUE_LEN = 240
_PROCESS_START_MONO = time.monotonic()


def trace_enabled(env: Optional[dict[str, str]] = None) -> bool:
    source = env if env is not None else os.environ
    return str(source.get("BTC5M_OPERATOR_TRACE", "false")).strip().lower() in {"1", "true", "yes", "on"}


def trace_event(event_type: str, **fields: Any) -> None:
    if not trace_enabled():
        return
    payload: dict[str, Any] = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "event_type": str(event_type),
        "pid": os.getpid(),
        "monotonic_elapsed_sec": round(max(0.0, time.monotonic() - _PROCESS_START_MONO), 6),
    }
    payload.update(_sanitize_fields(fields))
    print(json.dumps(payload, sort_keys=True, default=str), file=os.sys.stderr, flush=True)


def trace_stage_done(event_type: str, *, stage: str, started_mono: float, **fields: Any) -> float:
    elapsed = max(0.0, time.monotonic() - float(started_mono))
    trace_event(event_type, stage=stage, elapsed_sec=round(elapsed, 6), **fields)
    warn_if_stage_slow(stage=stage, elapsed_sec=elapsed)
    return elapsed


def warn_if_stage_slow(*, stage: str, elapsed_sec: float, warn_sec: Optional[float] = None, **fields: Any) -> None:
    threshold = warn_sec if warn_sec is not None else _warn_sec_from_env()
    if threshold <= 0:
        return
    if float(elapsed_sec) > threshold:
        trace_event(
            "stage_slow_warning",
            stage=stage,
            elapsed_sec=round(float(elapsed_sec), 6),
            warn_sec=threshold,
            **fields,
        )


def _warn_sec_from_env() -> float:
    raw = os.environ.get("BTC5M_TRACE_STAGE_WARN_SEC", "10")
    try:
        return float(raw)
    except (TypeError, ValueError):
        return 10.0


def _sanitize_fields(fields: dict[str, Any]) -> dict[str, Any]:
    return {str(key): _sanitize_value(str(key), value) for key, value in fields.items()}


def _sanitize_value(key: str, value: Any) -> Any:
    key_upper = key.upper()
    if any(part in key_upper for part in _SECRET_PARTS):
        return "<redacted>"
    if isinstance(value, dict):
        return {str(k): _sanitize_value(str(k), v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_sanitize_value(key, item) for item in value]
    if isinstance(value, bytes):
        return _truncate(value.decode("utf-8", errors="replace"))
    if isinstance(value, str):
        return _truncate(value)
    return value


def _truncate(value: str) -> str:
    text = str(value)
    if len(text) <= _MAX_VALUE_LEN:
        return text
    return text[:_MAX_VALUE_LEN] + "...<truncated>"
