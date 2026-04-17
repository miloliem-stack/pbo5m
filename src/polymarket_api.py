from __future__ import annotations

import json
import os
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from typing import Any, Optional


POLY_GAMMA_BASE = os.getenv("POLY_GAMMA_BASE", "https://gamma-api.polymarket.com").rstrip("/")
POLY_CLOB_BASE = os.getenv("POLY_CLOB_BASE", "https://clob.polymarket.com").rstrip("/")
POLY_RTDS_URL = os.getenv("POLY_RTDS_URL", "wss://ws-live-data.polymarket.com/").strip()
REQUEST_TIMEOUT_SEC = max(1.0, float(os.getenv("RECORDER_HTTP_TIMEOUT_SEC", "5")))
DEFAULT_GAMMA_HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Origin": "https://polymarket.com",
    "Referer": "https://polymarket.com/",
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/135.0.0.0 Safari/537.36"
    ),
}


def _gamma_headers() -> dict[str, str]:
    headers = dict(DEFAULT_GAMMA_HEADERS)
    raw = os.getenv("POLY_GAMMA_HEADERS_JSON", "").strip()
    if raw:
        try:
            parsed = json.loads(raw)
        except Exception:
            parsed = None
        if isinstance(parsed, dict):
            headers.update({str(key): str(value) for key, value in parsed.items()})
    return headers


def request_json(base_url: str, path: str, *, params: Optional[dict[str, Any]] = None, timeout: Optional[float] = None) -> Any:
    url = f"{base_url}{path}"
    if params:
        url = f"{url}?{urlencode(params)}"
    headers = _gamma_headers() if base_url == POLY_GAMMA_BASE else {"Accept": "application/json"}
    request = Request(url, headers=headers)
    with urlopen(request, timeout=timeout or REQUEST_TIMEOUT_SEC) as response:
        return json.loads(response.read().decode("utf-8"))


def request_json_diagnostic(
    base_url: str,
    path: str,
    *,
    params: Optional[dict[str, Any]] = None,
    timeout: Optional[float] = None,
) -> dict[str, Any]:
    url = f"{base_url}{path}"
    if params:
        url = f"{url}?{urlencode(params)}"
    headers = _gamma_headers() if base_url == POLY_GAMMA_BASE else {"Accept": "application/json"}
    request = Request(url, headers=headers)
    try:
        with urlopen(request, timeout=timeout or REQUEST_TIMEOUT_SEC) as response:
            text = response.read().decode("utf-8")
            try:
                payload = json.loads(text)
            except Exception as exc:
                return {
                    "ok": False,
                    "url": url,
                    "path": path,
                    "params": params,
                    "http_status": getattr(response, "status", None),
                    "headers_applied": headers,
                    "error_kind": "parse_failure",
                    "error": str(exc),
                    "response_text_sample": text[:500],
                }
            return {
                "ok": True,
                "url": url,
                "path": path,
                "params": params,
                "http_status": getattr(response, "status", None),
                "headers_applied": headers,
                "payload": payload,
            }
    except HTTPError as exc:
        body = ""
        try:
            body = exc.read().decode("utf-8")
        except Exception:
            body = ""
        return {
            "ok": False,
            "url": url,
            "path": path,
            "params": params,
            "http_status": exc.code,
            "headers_applied": headers,
            "error_kind": "http_failure",
            "error": str(exc),
            "response_text_sample": body[:500],
        }
    except URLError as exc:
        return {
            "ok": False,
            "url": url,
            "path": path,
            "params": params,
            "http_status": None,
            "headers_applied": headers,
            "error_kind": "transport_failure",
            "error": str(exc),
        }
    except Exception as exc:
        return {
            "ok": False,
            "url": url,
            "path": path,
            "params": params,
            "http_status": None,
            "headers_applied": headers,
            "error_kind": "unexpected_failure",
            "error": str(exc),
        }


def gamma_get(path: str, *, params: Optional[dict[str, Any]] = None, timeout: Optional[float] = None) -> Any:
    return request_json(POLY_GAMMA_BASE, path, params=params, timeout=timeout)


def gamma_get_diagnostic(path: str, *, params: Optional[dict[str, Any]] = None, timeout: Optional[float] = None) -> dict[str, Any]:
    return request_json_diagnostic(POLY_GAMMA_BASE, path, params=params, timeout=timeout)


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
