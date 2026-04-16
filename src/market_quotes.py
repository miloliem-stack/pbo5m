from __future__ import annotations

import os
import time
from typing import Any, Optional

from .polymarket_api import clob_get
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


def _extract_numeric(raw: Any, *keys: str) -> Optional[float]:
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        return float(raw)
    if isinstance(raw, str):
        try:
            return float(raw)
        except Exception:
            return None
    if isinstance(raw, dict):
        for key in keys:
            value = raw.get(key)
            if value is not None:
                parsed = _extract_numeric(value)
                if parsed is not None:
                    return parsed
        for key in ("data", "result", "price", "spread", "value"):
            value = raw.get(key)
            if value is not None:
                parsed = _extract_numeric(value, *keys)
                if parsed is not None:
                    return parsed
    return None


def _build_quote_snapshot_from_book(token_id: str, raw: Any, *, source: str, fetched_at_epoch: float, error: Optional[str] = None) -> dict[str, Any]:
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
        "error": error or parse_error,
        "raw": raw,
    }


def get_quote_snapshot(token_id: str, *, force_refresh: bool = False) -> dict[str, Any]:
    now_epoch = time.time()
    cached = _QUOTE_CACHE.get(token_id)
    if cached and not force_refresh and (now_epoch - cached["fetched_at_epoch"]) <= QUOTE_CACHE_TTL_SEC:
        snapshot = dict(cached["snapshot"])
        snapshot["age_seconds"] = max(0.0, now_epoch - cached["fetched_at_epoch"])
        return snapshot

    raw: dict[str, Any] = {}
    errors: list[str] = []

    for side, key in (("BUY", "buy_price"), ("SELL", "sell_price")):
        try:
            raw[key] = clob_get("/price", params={"token_id": token_id, "side": side})
        except Exception as exc:
            errors.append(f"{key}:{exc}")
    try:
        raw["spread"] = clob_get("/spread", params={"token_id": token_id})
    except Exception as exc:
        errors.append(f"spread:{exc}")
    try:
        raw["book"] = clob_get("/book", params={"token_id": token_id})
    except Exception as exc:
        errors.append(f"book:{exc}")

    best_bid = _extract_numeric(raw.get("sell_price"), "price")
    best_ask = _extract_numeric(raw.get("buy_price"), "price")
    spread = _extract_numeric(raw.get("spread"), "spread")
    book_snapshot = _build_quote_snapshot_from_book(token_id, raw.get("book"), source="clob_orderbook", fetched_at_epoch=now_epoch)
    if best_bid is None:
        best_bid = book_snapshot.get("best_bid")
    if best_ask is None:
        best_ask = book_snapshot.get("best_ask")
    if spread is None and best_bid is not None and best_ask is not None:
        spread = float(best_ask - best_bid)
    mid = None
    if best_bid is not None and best_ask is not None:
        mid = float((best_ask + best_bid) / 2.0)
    elif book_snapshot.get("mid") is not None:
        mid = book_snapshot["mid"]

    snapshot = {
        "token_id": token_id,
        "best_bid": best_bid,
        "best_ask": best_ask,
        "mid": mid,
        "spread": spread,
        "bid_size": book_snapshot.get("bid_size"),
        "ask_size": book_snapshot.get("ask_size"),
        "is_empty": best_bid is None and best_ask is None,
        "is_crossed": bool(best_bid is not None and best_ask is not None and best_bid > best_ask),
        "source": "clob_price_and_book",
        "fetched_at": isoformat_utc(utc_now()),
        "age_seconds": 0.0,
        "fetch_failed": not bool(raw),
        "error": "; ".join(errors) if errors else None,
        "raw": raw,
        "quote_age_seconds": seconds_since(isoformat_utc(utc_now())),
    }
    _QUOTE_CACHE[token_id] = {"snapshot": snapshot, "fetched_at_epoch": now_epoch}
    return dict(snapshot)


def build_market_quote_row(market: dict[str, Any], yes_quote: dict[str, Any], no_quote: dict[str, Any], *, source: str, raw_payload_fragment: Any) -> dict[str, Any]:
    return {
        "ts": isoformat_utc(utc_now()),
        "record_type": "quote_snapshot",
        "source": source,
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
            "best_bid": yes_quote.get("best_bid"),
            "best_ask": yes_quote.get("best_ask"),
            "mid": yes_quote.get("mid"),
            "spread": yes_quote.get("spread"),
            "age_seconds": yes_quote.get("age_seconds"),
            "error": yes_quote.get("error"),
        },
        "no": {
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
