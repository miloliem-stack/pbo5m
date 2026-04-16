from __future__ import annotations

import json
import os
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from typing import Any, Optional


POLY_GAMMA_BASE = os.getenv("POLY_GAMMA_BASE", "https://gamma-api.polymarket.com").rstrip("/")
POLY_CLOB_BASE = os.getenv("POLY_CLOB_BASE", "https://clob.polymarket.com").rstrip("/")
POLY_RTDS_URL = os.getenv("POLY_RTDS_URL", "").strip()
REQUEST_TIMEOUT_SEC = max(1.0, float(os.getenv("RECORDER_HTTP_TIMEOUT_SEC", "5")))


def request_json(base_url: str, path: str, *, params: Optional[dict[str, Any]] = None, timeout: Optional[float] = None) -> Any:
    url = f"{base_url}{path}"
    if params:
        url = f"{url}?{urlencode(params)}"
    request = Request(url, headers={"Accept": "application/json"})
    with urlopen(request, timeout=timeout or REQUEST_TIMEOUT_SEC) as response:
        return json.loads(response.read().decode("utf-8"))


def gamma_get(path: str, *, params: Optional[dict[str, Any]] = None, timeout: Optional[float] = None) -> Any:
    return request_json(POLY_GAMMA_BASE, path, params=params, timeout=timeout)


def clob_get(path: str, *, params: Optional[dict[str, Any]] = None, timeout: Optional[float] = None) -> Any:
    return request_json(POLY_CLOB_BASE, path, params=params, timeout=timeout)


def coerce_json_list(value: Any) -> Optional[list[Any]]:
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except Exception:
            return None
        if isinstance(parsed, list):
            return parsed
    return None


def coerce_items(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        for key in ("items", "data", "markets", "events"):
            value = payload.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
        return [payload]
    return []
