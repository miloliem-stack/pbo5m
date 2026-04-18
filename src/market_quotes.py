from __future__ import annotations

import os
import time
from typing import Any, Optional

from .polymarket_api import clob_get_diagnostic
from .time_utils import isoformat_utc, seconds_since, utc_now


QUOTE_CACHE_TTL_SEC = max(0.2, float(os.getenv("QUOTE_CACHE_TTL_SEC", "1")))
_QUOTE_CACHE: dict[str, dict[str, Any]] = {}


def _parse_book_level(level: Any) -> Optional[tuple[float, float]]:
    if isinstance(level, (list, tuple)) and len(level) >= 2:
        return float(level[0]), float(level[1])
    if isinstance(level, dict):
        price = level.get("price") or level.get("p")
        size = level.get("size") or level.get("quantity") or level.get("qty") or level.get("q")
        if price is None or size is None:
            return None
        return float(price), float(size)
    return None


def _extract_orderbook_payload(raw: Any) -> Any:
    if not isinstance(raw, dict):
        return raw
    for key in ("book", "data", "orderbook"):
        nested = raw.get(key)
        if isinstance(nested, dict):
            return nested
    return raw


def _build_quote_snapshot_from_book(
    token_id: str,
    raw: Any,
    *,
    source: str,
    fetched_at_epoch: float,
    fetch_ok: bool,
    error: Optional[str] = None,
    error_kind: Optional[str] = None,
    http_status: Optional[int] = None,
    transport: Optional[str] = None,
    response_text_sample: Optional[str] = None,
) -> dict[str, Any]:
    best_bid = None
    best_ask = None
    bid_size = None
    ask_size = None
    mid = None
    spread = None
    parse_error = None
    is_crossed = False
    is_empty = True
    book = _extract_orderbook_payload(raw)
    if isinstance(book, dict):
        try:
            bids = book.get("bids") or []
            asks = book.get("asks") or []
            bid_level = _parse_book_level(bids[0]) if bids else None
            ask_level = _parse_book_level(asks[0]) if asks else None
            if bid_level is not None:
                best_bid, bid_size = bid_level
            if ask_level is not None:
                best_ask, ask_size = ask_level
            is_empty = best_bid is None and best_ask is None
            if best_bid is not None and best_ask is not None:
                spread = float(best_ask - best_bid)
                mid = float((best_ask + best_bid) / 2.0)
                is_crossed = best_bid > best_ask
        except Exception as exc:
            parse_error = str(exc)
    else:
        parse_error = "non-dict-orderbook"
    final_error_kind = error_kind
    final_error = error or parse_error
    if parse_error and fetch_ok:
        final_error_kind = final_error_kind or "parse_failure"
    return {
        "token_id": token_id,
        "best_bid": best_bid,
        "best_ask": best_ask,
        "mid": mid,
        "spread": spread,
        "bid_size": bid_size,
        "ask_size": ask_size,
        "is_empty": is_empty,
        "is_crossed": is_crossed,
        "source": source,
        "fetched_at": isoformat_utc(utc_now()),
        "age_seconds": max(0.0, time.time() - fetched_at_epoch),
        "fetch_ok": bool(fetch_ok and not is_empty and not parse_error),
        "error_kind": final_error_kind,
        "http_status": http_status,
        "transport": transport,
        "error": final_error,
        "response_text_sample": response_text_sample,
        "raw": raw,
    }


def get_quote_snapshot(token_id: str, *, force_refresh: bool = False) -> dict[str, Any]:
    now_epoch = time.time()
    cached = _QUOTE_CACHE.get(token_id)
    if cached and not force_refresh and (now_epoch - cached["fetched_at_epoch"]) <= QUOTE_CACHE_TTL_SEC:
        snapshot = dict(cached["snapshot"])
        snapshot["age_seconds"] = max(0.0, now_epoch - cached["fetched_at_epoch"])
        return snapshot

    book_result = clob_get_diagnostic("/book", params={"token_id": token_id})
    snapshot = _build_quote_snapshot_from_book(
        token_id,
        book_result.get("payload"),
        source="clob_orderbook",
        fetched_at_epoch=now_epoch,
        fetch_ok=bool(book_result.get("ok")),
        error=book_result.get("error"),
        error_kind=book_result.get("error_kind"),
        http_status=book_result.get("http_status"),
        transport=book_result.get("transport"),
        response_text_sample=book_result.get("response_text_sample"),
    )
    snapshot["fetch_failed"] = not snapshot["fetch_ok"]
    snapshot["raw"] = {
        "book": book_result.get("payload"),
        "book_diagnostic": {
            "ok": book_result.get("ok"),
            "http_status": book_result.get("http_status"),
            "error_kind": book_result.get("error_kind"),
            "error": book_result.get("error"),
            "transport": book_result.get("transport"),
            "response_text_sample": book_result.get("response_text_sample"),
        },
    }
    snapshot["quote_age_seconds"] = seconds_since(snapshot["fetched_at"])
    _QUOTE_CACHE[token_id] = {"snapshot": snapshot, "fetched_at_epoch": now_epoch}
    return dict(snapshot)


def build_market_quote_row(market: dict[str, Any], yes_quote: dict[str, Any], no_quote: dict[str, Any], *, source: str, raw_payload_fragment: Any) -> dict[str, Any]:
    yes_ok = bool(yes_quote.get("fetch_ok"))
    no_ok = bool(no_quote.get("fetch_ok"))
    if yes_ok and no_ok:
        capture_status = "ok"
    elif yes_ok or no_ok:
        capture_status = "partial_failure"
    else:
        capture_status = "failed"
    return {
        "ts": isoformat_utc(utc_now()),
        "record_type": "quote_snapshot",
        "source": source,
        "quote_capture_ok": yes_ok or no_ok,
        "quote_capture_status": capture_status,
        "market_id": market.get("market_id"),
        "slug": market.get("slug"),
        "title": market.get("title"),
        "condition_id": market.get("condition_id"),
        "token_yes": market.get("token_yes"),
        "token_no": market.get("token_no"),
        "market_start_time": market.get("start_time"),
        "market_end_time": market.get("end_time"),
        "status": market.get("status"),
        "yes": {
            "fetch_ok": yes_quote.get("fetch_ok"),
            "error_kind": yes_quote.get("error_kind"),
            "http_status": yes_quote.get("http_status"),
            "transport": yes_quote.get("transport"),
            "best_bid": yes_quote.get("best_bid"),
            "best_ask": yes_quote.get("best_ask"),
            "mid": yes_quote.get("mid"),
            "spread": yes_quote.get("spread"),
            "age_seconds": yes_quote.get("age_seconds"),
            "error": yes_quote.get("error"),
        },
        "no": {
            "fetch_ok": no_quote.get("fetch_ok"),
            "error_kind": no_quote.get("error_kind"),
            "http_status": no_quote.get("http_status"),
            "transport": no_quote.get("transport"),
            "best_bid": no_quote.get("best_bid"),
            "best_ask": no_quote.get("best_ask"),
            "mid": no_quote.get("mid"),
            "spread": no_quote.get("spread"),
            "age_seconds": no_quote.get("age_seconds"),
            "error": no_quote.get("error"),
        },
        "quote_age_diagnostics": {
            "yes_age_seconds": yes_quote.get("age_seconds"),
            "no_age_seconds": no_quote.get("age_seconds"),
            "max_age_seconds": max(
                float(yes_quote.get("age_seconds") or 0.0),
                float(no_quote.get("age_seconds") or 0.0),
            ),
        },
        "raw_payload_fragment": raw_payload_fragment,
    }
