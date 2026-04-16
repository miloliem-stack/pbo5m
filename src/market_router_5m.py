from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Optional

from .polymarket_api import coerce_items, coerce_json_list, gamma_get
from .time_utils import isoformat_utc, parse_datetime, utc_now


ROUTER_LOOKBACK_SECONDS = max(300, int(os.getenv("BTC_5M_ROUTER_LOOKBACK_SECONDS", "3600")))
ROUTER_LOOKAHEAD_SECONDS = max(300, int(os.getenv("BTC_5M_ROUTER_LOOKAHEAD_SECONDS", "3600")))
ROUTER_PAGE_LIMIT = max(20, int(os.getenv("BTC_5M_ROUTER_PAGE_LIMIT", "200")))
ROUTER_MAX_PAGES = max(1, int(os.getenv("BTC_5M_ROUTER_MAX_PAGES", "4")))
TARGET_DURATION_SECONDS = 300
BTC_KEYWORDS = ("bitcoin", "btc")
UP_WORDS = ("up", "higher", "above", "yes")
DOWN_WORDS = ("down", "lower", "below", "no")


@dataclass
class RoutedMarket:
    market_id: Optional[str]
    slug: Optional[str]
    title: Optional[str]
    condition_id: Optional[str]
    token_yes: Optional[str]
    token_no: Optional[str]
    start_time: Optional[str]
    end_time: Optional[str]
    status: Optional[str]
    detection_source: Optional[str]
    routing_reason: str
    routing_score: Optional[int]
    raw_market: Optional[dict[str, Any]]

    def as_dict(self) -> dict[str, Any]:
        return {
            "market_id": self.market_id,
            "slug": self.slug,
            "title": self.title,
            "condition_id": self.condition_id,
            "token_yes": self.token_yes,
            "token_no": self.token_no,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "status": self.status,
            "detection_source": self.detection_source,
            "routing_reason": self.routing_reason,
            "routing_score": self.routing_score,
            "raw_market": self.raw_market,
        }


def _extract_tokens_from_outcomes(container: dict[str, Any]) -> dict[str, Optional[str]]:
    token_yes = None
    token_no = None
    clob_token_ids = coerce_json_list(container.get("clobTokenIds"))
    outcomes = coerce_json_list(container.get("outcomes"))
    if isinstance(clob_token_ids, list) and isinstance(outcomes, list) and len(clob_token_ids) == len(outcomes):
        for outcome, token_id in zip(outcomes, clob_token_ids):
            label = str(outcome or "").strip().lower()
            if label in ("yes", "up"):
                token_yes = token_yes or str(token_id)
            elif label in ("no", "down"):
                token_no = token_no or str(token_id)
    for key in ("tokens", "outcomeTokens"):
        group = container.get(key)
        if isinstance(group, dict):
            group = list(group.values())
        if not isinstance(group, list):
            continue
        for item in group:
            if not isinstance(item, dict):
                continue
            token_id = item.get("id") or item.get("tokenId") or item.get("token_id") or item.get("assetId")
            label = str(item.get("outcome") or item.get("label") or item.get("name") or item.get("side") or "").strip().lower()
            if label in ("yes", "up"):
                token_yes = token_yes or str(token_id)
            elif label in ("no", "down"):
                token_no = token_no or str(token_id)
    return {"token_yes": token_yes, "token_no": token_no}


def _extract_tokens(container: dict[str, Any]) -> dict[str, Optional[str]]:
    extracted = _extract_tokens_from_outcomes(container)
    token_yes = extracted["token_yes"] or container.get("yesTokenId") or container.get("tokenYes") or container.get("token_yes")
    token_no = extracted["token_no"] or container.get("noTokenId") or container.get("tokenNo") or container.get("token_no")
    return {
        "token_yes": str(token_yes) if token_yes not in (None, "") else None,
        "token_no": str(token_no) if token_no not in (None, "") else None,
    }


def _iter_market_candidates(payload: dict[str, Any]) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    if isinstance(payload, dict):
        candidates.append(payload)
        for key in ("market", "markets"):
            value = payload.get(key)
            if isinstance(value, dict):
                candidates.append(value)
            elif isinstance(value, list):
                candidates.extend(item for item in value if isinstance(item, dict))
        events = payload.get("events")
        if isinstance(events, list):
            for event in events:
                if isinstance(event, dict):
                    candidates.extend(_iter_market_candidates(event))
    deduped: list[dict[str, Any]] = []
    seen: set[int] = set()
    for candidate in candidates:
        marker = id(candidate)
        if marker in seen:
            continue
        seen.add(marker)
        deduped.append(candidate)
    return deduped


def _normalize_status(candidate: dict[str, Any], start, end, *, now=None) -> Optional[str]:
    raw_status = str(candidate.get("status") or candidate.get("state") or candidate.get("marketStatus") or "").strip().lower()
    if raw_status in ("open", "active", "tradable", "trading"):
        return "open"
    if raw_status in ("closed", "resolved", "archived", "paused", "settled", "finalized"):
        return raw_status
    if candidate.get("closed") is True:
        return "closed"
    if candidate.get("acceptingOrders") is True or candidate.get("enableOrderBook") is True or candidate.get("active") is True:
        current = now or utc_now()
        if start is None or end is None or start <= current < end:
            return "open"
    return raw_status or None


def _normalize_candidate(payload: dict[str, Any], *, detection_source: str, now=None) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for candidate in _iter_market_candidates(payload):
        start = parse_datetime(candidate.get("startDate") or candidate.get("start") or payload.get("startDate") or payload.get("start"))
        end = parse_datetime(candidate.get("endDate") or candidate.get("end") or payload.get("endDate") or payload.get("end"))
        tokens = _extract_tokens(candidate)
        if not tokens["token_yes"] or not tokens["token_no"]:
            merged = _extract_tokens(payload)
            tokens["token_yes"] = tokens["token_yes"] or merged["token_yes"]
            tokens["token_no"] = tokens["token_no"] or merged["token_no"]
        market_id = candidate.get("id") or candidate.get("marketId") or candidate.get("market_id") or candidate.get("questionID")
        condition_id = candidate.get("conditionId") or candidate.get("condition_id") or candidate.get("condition")
        title = candidate.get("question") or candidate.get("title") or candidate.get("name") or payload.get("question") or payload.get("title")
        slug = candidate.get("slug") or payload.get("slug") or payload.get("eventSlug")
        status = _normalize_status(candidate, start, end, now=now)
        normalized.append(
            {
                "market_id": str(market_id) if market_id not in (None, "") else None,
                "slug": slug,
                "title": title,
                "condition_id": str(condition_id) if condition_id not in (None, "") else None,
                "token_yes": tokens["token_yes"],
                "token_no": tokens["token_no"],
                "start": start,
                "end": end,
                "status": status,
                "detection_source": detection_source,
                "raw_market": candidate,
                "raw_payload": payload,
            }
        )
    return normalized


def _candidate_reason(candidate: dict[str, Any], *, now=None) -> tuple[int, str]:
    current = now or utc_now()
    title = str(candidate.get("title") or "").strip()
    title_l = title.lower()
    slug_l = str(candidate.get("slug") or "").strip().lower()
    text = f"{title_l} {slug_l}"

    start = candidate.get("start")
    end = candidate.get("end")
    duration_penalty = 10_000
    window_penalty = 0
    if start is None or end is None:
        return 50_000, "missing_market_window"

    duration_seconds = int((end - start).total_seconds())
    duration_penalty = abs(duration_seconds - TARGET_DURATION_SECONDS)
    if duration_seconds <= 0:
        return 40_000, "invalid_market_window"
    if current < start:
        window_penalty = int((start - current).total_seconds())
    elif current >= end:
        window_penalty = 5_000 + int((current - end).total_seconds())

    keyword_penalty = 0 if any(word in text for word in BTC_KEYWORDS) else 8_000
    direction_penalty = 0 if any(word in text for word in UP_WORDS) and any(word in text for word in DOWN_WORDS) else 1_500
    token_penalty = 0 if candidate.get("token_yes") and candidate.get("token_no") else 7_500
    orderbook_penalty = 0 if str(candidate.get("status") or "").lower() == "open" else 2_000

    score = duration_penalty + window_penalty + keyword_penalty + direction_penalty + token_penalty + orderbook_penalty
    reason = (
        f"duration_off_seconds={duration_penalty}; "
        f"window_penalty={window_penalty}; "
        f"btc_keyword_penalty={keyword_penalty}; "
        f"direction_penalty={direction_penalty}; "
        f"token_penalty={token_penalty}; "
        f"status_penalty={orderbook_penalty}"
    )
    return score, reason


def _fetch_market_scan(now=None) -> list[dict[str, Any]]:
    current = now or utc_now()
    end_min = (current - timedelta(seconds=ROUTER_LOOKBACK_SECONDS)).isoformat()
    end_max = (current + timedelta(seconds=ROUTER_LOOKAHEAD_SECONDS)).isoformat()
    attempts: list[dict[str, Any]] = []
    for page in range(ROUTER_MAX_PAGES):
        params = {
            "closed": False,
            "end_date_min": end_min,
            "end_date_max": end_max,
            "order": "endDate",
            "ascending": True,
            "limit": ROUTER_PAGE_LIMIT,
            "offset": page * ROUTER_PAGE_LIMIT,
        }
        try:
            payload = gamma_get("/markets", params=params)
        except Exception as exc:
            attempts.append({"source": "gamma_markets_window", "page": page, "error": str(exc)})
            break
        items = coerce_items(payload)
        attempts.append({"source": "gamma_markets_window", "page": page, "count": len(items)})
        if not items:
            break
        for item in items:
            item["_router_source"] = "gamma_markets_window"
        if len(items) < ROUTER_PAGE_LIMIT:
            yield from items
            break
        yield from items


def _fetch_event_scan(now=None) -> list[dict[str, Any]]:
    current = now or utc_now()
    start_min = (current - timedelta(seconds=ROUTER_LOOKBACK_SECONDS)).isoformat()
    end_max = (current + timedelta(seconds=ROUTER_LOOKAHEAD_SECONDS)).isoformat()
    params = {"start_date_min": start_min, "end_date_max": end_max, "limit": ROUTER_PAGE_LIMIT}
    try:
        payload = gamma_get("/events", params=params, timeout=8)
    except Exception:
        return []
    items = coerce_items(payload)
    for item in items:
        item["_router_source"] = "gamma_events_window"
    return items


def route_btc_5m_market(*, now=None) -> dict[str, Any]:
    current = now or utc_now()
    raw_candidates: list[dict[str, Any]] = []
    attempts: list[dict[str, Any]] = []

    try:
        market_scan = list(_fetch_market_scan(current))
        attempts.append({"source": "gamma_markets_window", "count": len(market_scan)})
        raw_candidates.extend(market_scan)
    except Exception as exc:
        attempts.append({"source": "gamma_markets_window", "error": str(exc)})

    try:
        event_scan = _fetch_event_scan(current)
        attempts.append({"source": "gamma_events_window", "count": len(event_scan)})
        raw_candidates.extend(event_scan)
    except Exception as exc:
        attempts.append({"source": "gamma_events_window", "error": str(exc)})

    normalized: list[dict[str, Any]] = []
    for payload in raw_candidates:
        detection_source = payload.get("_router_source") or "gamma_scan"
        normalized.extend(_normalize_candidate(payload, detection_source=detection_source, now=current))

    if not normalized:
        return {
            "market": None,
            "routing_reason": "no_candidates_found",
            "detection_source": None,
            "diagnostics": {"attempts": attempts, "candidate_count": 0},
        }

    scored: list[tuple[int, dict[str, Any], str]] = []
    for candidate in normalized:
        score, reason = _candidate_reason(candidate, now=current)
        scored.append((score, candidate, reason))
    scored.sort(key=lambda item: item[0])
    best_score, best, reason = scored[0]

    routed = RoutedMarket(
        market_id=best.get("market_id"),
        slug=best.get("slug"),
        title=best.get("title"),
        condition_id=best.get("condition_id"),
        token_yes=best.get("token_yes"),
        token_no=best.get("token_no"),
        start_time=isoformat_utc(best.get("start")),
        end_time=isoformat_utc(best.get("end")),
        status=best.get("status"),
        detection_source=best.get("detection_source"),
        routing_reason=reason,
        routing_score=best_score,
        raw_market={
            "candidate": best.get("raw_market"),
            "payload_fragment": best.get("raw_payload"),
        },
    )
    return {
        "market": routed.as_dict(),
        "routing_reason": reason,
        "detection_source": best.get("detection_source"),
        "diagnostics": {
            "attempts": attempts,
            "candidate_count": len(normalized),
            "top_candidates": [
                {
                    "market_id": candidate.get("market_id"),
                    "slug": candidate.get("slug"),
                    "title": candidate.get("title"),
                    "score": score,
                    "reason": candidate_reason,
                    "detection_source": candidate.get("detection_source"),
                }
                for score, candidate, candidate_reason in scored[:5]
            ],
        },
    }
