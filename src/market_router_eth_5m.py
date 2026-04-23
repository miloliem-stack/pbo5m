from __future__ import annotations

import os
import time
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Optional

from .polymarket_api import coerce_items, coerce_json_list, gamma_get_diagnostic
from .time_utils import isoformat_utc, parse_datetime, utc_now


SLUG_PREFIX = os.getenv("ETH_5M_SLUG_PREFIX", "eth-updown-5m-").strip().lower()
ROUTER_PAGE_LIMIT = max(20, int(os.getenv("ETH_5M_ROUTER_PAGE_LIMIT", "200")))
ROUTER_MAX_PAGES = max(1, int(os.getenv("ETH_5M_ROUTER_MAX_PAGES", "4")))
ROUTER_UPCOMING_TOLERANCE_SECONDS = max(300, int(os.getenv("ETH_5M_ROUTER_UPCOMING_TOLERANCE_SECONDS", "1800")))
ROUTER_SAMPLE_LIMIT = max(1, int(os.getenv("ETH_5M_ROUTER_SAMPLE_LIMIT", "8")))
TARGET_DURATION_SECONDS = 300
ETH_KEYWORDS = ("ethereum", "eth")
FIVE_MINUTE_WORDS = ("5m", "5-minute", "5 minute", "five minute")


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


def _normalize_series_text(value: Any) -> str:
    return str(value or "").strip().lower()


def _bucket_ts(now=None) -> int:
    current = now or utc_now()
    return int(current.timestamp() // TARGET_DURATION_SECONDS) * TARGET_DURATION_SECONDS


def _candidate_slugs(now=None) -> list[str]:
    bucket = _bucket_ts(now)
    slugs = []
    for offset in (-300, 0, 300, 600):
        slug = f"{SLUG_PREFIX}{bucket + offset}"
        if slug not in slugs:
            slugs.append(slug)
    return slugs


def _sample_records(items: list[dict[str, Any]], *, limit: int = ROUTER_SAMPLE_LIMIT) -> list[dict[str, Any]]:
    sample = []
    for item in items[:limit]:
        sample.append(
            {
                "slug": item.get("slug") or item.get("eventSlug"),
                "title": item.get("question") or item.get("title") or item.get("name"),
                "start": isoformat_utc(item.get("startDate") or item.get("start")),
                "end": isoformat_utc(item.get("endDate") or item.get("end")),
            }
        )
    return sample


def _resolve_market_times(candidate: dict[str, Any], payload: dict[str, Any]) -> tuple[Any, Any, dict[str, Any]]:
    events = payload.get("events")
    first_event = events[0] if isinstance(events, list) and events and isinstance(events[0], dict) else {}
    top_level_values = {
        "eventStartTime": payload.get("eventStartTime"),
        "startTime": payload.get("startTime"),
        "startDate": payload.get("startDate"),
        "endDate": payload.get("endDate"),
    }
    candidate_values = {
        "eventStartTime": candidate.get("eventStartTime"),
        "startTime": candidate.get("startTime"),
        "startDate": candidate.get("startDate"),
        "endDate": candidate.get("endDate"),
    }
    event_values = {
        "eventStartTime": first_event.get("eventStartTime"),
        "startTime": first_event.get("startTime"),
        "startDate": first_event.get("startDate"),
        "endDate": first_event.get("endDate"),
    }
    start_sources = [
        ("candidate.eventStartTime", candidate_values["eventStartTime"]),
        ("candidate.startTime", candidate_values["startTime"]),
        ("payload.eventStartTime", top_level_values["eventStartTime"]),
        ("payload.startTime", top_level_values["startTime"]),
        ("events[0].startTime", event_values["startTime"]),
        ("events[0].eventStartTime", event_values["eventStartTime"]),
        ("candidate.startDate", candidate_values["startDate"]),
        ("events[0].startDate", event_values["startDate"]),
        ("payload.startDate", top_level_values["startDate"]),
    ]
    end_sources = [
        ("events[0].endDate", event_values["endDate"]),
        ("candidate.endDate", candidate_values["endDate"]),
        ("payload.endDate", top_level_values["endDate"]),
    ]
    chosen_start_source = None
    chosen_end_source = None
    start = None
    end = None
    for source_name, value in start_sources:
        parsed = parse_datetime(value)
        if parsed is not None:
            start = parsed
            chosen_start_source = source_name
            break
    for source_name, value in end_sources:
        parsed = parse_datetime(value)
        if parsed is not None:
            end = parsed
            chosen_end_source = source_name
            break
    return start, end, {
        "chosen_start_source": chosen_start_source,
        "chosen_end_source": chosen_end_source,
        "top_level_values": top_level_values,
        "candidate_values": candidate_values,
        "event_values": event_values,
    }


def _extract_tokens_from_outcomes(container: dict[str, Any]) -> dict[str, Optional[str]]:
    token_yes = None
    token_no = None
    clob_token_ids = coerce_json_list(container.get("clobTokenIds"))
    outcomes = coerce_json_list(container.get("outcomes"))
    if (
        isinstance(clob_token_ids, list)
        and isinstance(outcomes, list)
        and len(clob_token_ids) == len(outcomes)
    ):
        for label, token_id in zip(outcomes, clob_token_ids):
            normalized_label = str(label or "").strip().lower()
            if normalized_label in ("yes", "up"):
                token_yes = token_yes or str(token_id)
            elif normalized_label in ("no", "down"):
                token_no = token_no or str(token_id)
    outcome_groups = [
        container.get("tokens"),
        outcomes,
        container.get("outcomeTokens"),
        clob_token_ids,
    ]
    for group in outcome_groups:
        if isinstance(group, dict):
            group = list(group.values())
        if not isinstance(group, list):
            continue
        for item in group:
            if isinstance(item, str) or not isinstance(item, dict):
                continue
            token_id = item.get("id") or item.get("tokenId") or item.get("token_id") or item.get("assetId")
            label = str(item.get("side") or item.get("name") or item.get("outcome") or item.get("label") or "").strip().upper()
            if label in ("YES", "UP"):
                token_yes = token_yes or str(token_id)
            elif label in ("NO", "DOWN"):
                token_no = token_no or str(token_id)
    return {"token_yes": token_yes, "token_no": token_no}


def _extract_tokens(container: dict[str, Any]) -> dict[str, Optional[str]]:
    tokens = _extract_tokens_from_outcomes(container)
    token_yes = tokens["token_yes"] or container.get("yesTokenId") or container.get("tokenYes") or container.get("token_yes")
    token_no = tokens["token_no"] or container.get("noTokenId") or container.get("tokenNo") or container.get("token_no")
    return {
        "token_yes": str(token_yes) if token_yes not in (None, "") else None,
        "token_no": str(token_no) if token_no not in (None, "") else None,
    }


def _iter_market_candidates(payload: dict[str, Any]) -> list[dict[str, Any]]:
    candidates = []
    if isinstance(payload, dict):
        candidates.append(payload)
        for key in ("market", "markets"):
            value = payload.get(key)
            if isinstance(value, dict):
                candidates.append(value)
            elif isinstance(value, list):
                candidates.extend(item for item in value if isinstance(item, dict))
        for key in ("events",):
            value = payload.get(key)
            if isinstance(value, list):
                for item in value:
                    if isinstance(item, dict):
                        candidates.extend(_iter_market_candidates(item))
    deduped = []
    seen = set()
    for item in candidates:
        marker = id(item)
        if marker in seen:
            continue
        seen.add(marker)
        deduped.append(item)
    return deduped


def _normalize_market_status(market: dict[str, Any], start, end, *, now=None) -> Optional[str]:
    raw_status = str(market.get("status") or market.get("state") or market.get("marketStatus") or "").strip().lower()
    if raw_status in ("open", "active", "tradable", "trading"):
        return "open"
    if raw_status in ("closed", "resolved", "redeemed", "archived", "paused", "settled", "finalized"):
        return raw_status
    if market.get("closed") is True:
        return "closed"
    if market.get("archived") is True:
        return "archived"
    if market.get("enableOrderBook") is True or market.get("acceptingOrders") is True or market.get("tradable") is True or market.get("active") is True:
        current = now or utc_now()
        if start is None or end is None or start <= current < end:
            return "open"
    return raw_status or None


def _normalize_market_bundle(payload: dict[str, Any], *, detection_source: str, now=None) -> list[dict[str, Any]]:
    candidates = _iter_market_candidates(payload)
    normalized_candidates = []
    for candidate in candidates:
        start, end, timing_debug = _resolve_market_times(candidate, payload)
        tokens = _extract_tokens(candidate)
        if not tokens["token_yes"] or not tokens["token_no"]:
            merged_tokens = _extract_tokens(payload)
            tokens["token_yes"] = tokens["token_yes"] or merged_tokens["token_yes"]
            tokens["token_no"] = tokens["token_no"] or merged_tokens["token_no"]
        market_id = (
            candidate.get("id")
            or candidate.get("marketId")
            or candidate.get("market_id")
            or candidate.get("questionID")
            or payload.get("marketId")
            or payload.get("market_id")
        )
        condition_id = (
            candidate.get("conditionId")
            or candidate.get("condition_id")
            or candidate.get("condition")
            or payload.get("conditionId")
            or payload.get("condition_id")
        )
        slug = candidate.get("slug") or payload.get("slug") or payload.get("eventSlug")
        title = candidate.get("title") or candidate.get("question") or candidate.get("name") or payload.get("title") or payload.get("name")
        status = _normalize_market_status(candidate, start, end, now=now)
        market = {
            "market_id": str(market_id) if market_id not in (None, "") else None,
            "condition_id": str(condition_id) if condition_id not in (None, "") else None,
            "slug": slug,
            "title": title,
            "token_yes": tokens["token_yes"],
            "token_no": tokens["token_no"],
            "start": start,
            "end": end,
            "status": status,
            "detection_source": detection_source,
            "raw_market": candidate,
            "raw_payload": payload,
            "timing_debug": timing_debug,
        }
        if any(market.get(field) is not None for field in ("market_id", "slug", "token_yes", "token_no", "start", "end")):
            normalized_candidates.append(market)
    return normalized_candidates


def _dedupe_candidates(candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped = []
    seen: set[tuple[Any, ...]] = set()
    for candidate in candidates:
        marker = (
            candidate.get("market_id"),
            candidate.get("condition_id"),
            candidate.get("slug"),
            candidate.get("start"),
            candidate.get("end"),
        )
        if marker in seen:
            continue
        seen.add(marker)
        deduped.append(candidate)
    return deduped


def _normalize_result_summary(candidates: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "extracted_candidate_count": len(candidates),
        "sample_candidates": [
            {
                "market_id": item.get("market_id"),
                "slug": item.get("slug"),
                "title": item.get("title"),
                "condition_id": item.get("condition_id"),
                "token_yes": item.get("token_yes"),
                "token_no": item.get("token_no"),
                "start": isoformat_utc(item.get("start")),
                "end": isoformat_utc(item.get("end")),
                "status": item.get("status"),
            }
            for item in candidates[:ROUTER_SAMPLE_LIMIT]
        ],
    }


def _slug_fetch_attempt(path: str, *, params: Optional[dict[str, Any]], slug: str, source: str, timeout: float = 6.0) -> dict[str, Any]:
    result = gamma_get_diagnostic(path, params=params, timeout=timeout)
    attempt = {
        "source": source,
        "slug": slug,
        "path": path,
        "params": params,
        "exact_slug_endpoint_used": path.startswith("/markets/slug/") or path.startswith("/events/slug/"),
        "http_status": result.get("http_status"),
        "ok": result.get("ok", False),
        "error_kind": result.get("error_kind"),
        "error": result.get("error"),
        "browser_like_headers_applied": bool(result.get("headers_applied")),
        "headers_applied": result.get("headers_applied"),
        "payload_empty": None,
        "payload_sample_rows": [],
        "normalization_result": None,
        "candidates": [],
    }
    if not result.get("ok"):
        return attempt
    items = coerce_items(result.get("payload"))
    attempt["payload_empty"] = len(items) == 0
    attempt["payload_sample_rows"] = _sample_records(items)
    detection_source = f"{source}:{path}"
    candidates = _dedupe_candidates(_normalize_market_bundle(result.get("payload"), detection_source=detection_source, now=utc_now()))
    attempt["candidates"] = candidates
    attempt["normalization_result"] = _normalize_result_summary(candidates)
    return attempt


def _slug_attempts(now=None) -> list[dict[str, Any]]:
    attempts = []
    for slug in _candidate_slugs(now):
        attempts.append(_slug_fetch_attempt(f"/markets/slug/{slug}", params=None, slug=slug, source="slug_exact_market_path"))
        attempts.append(_slug_fetch_attempt("/markets", params={"slug": slug}, slug=slug, source="slug_market_query"))
        attempts.append(_slug_fetch_attempt(f"/events/slug/{slug}", params=None, slug=slug, source="slug_exact_event_path"))
        attempts.append(_slug_fetch_attempt("/events", params={"slug": slug}, slug=slug, source="slug_event_query"))
    return attempts


def _broad_scan_attempt(path: str, *, params: dict[str, Any], source: str, timeout: float = 8.0) -> dict[str, Any]:
    result = gamma_get_diagnostic(path, params=params, timeout=timeout)
    attempt = {
        "source": source,
        "path": path,
        "params": params,
        "exact_slug_endpoint_used": False,
        "http_status": result.get("http_status"),
        "ok": result.get("ok", False),
        "error_kind": result.get("error_kind"),
        "error": result.get("error"),
        "browser_like_headers_applied": bool(result.get("headers_applied")),
        "headers_applied": result.get("headers_applied"),
        "payload_empty": None,
        "payload_sample_rows": [],
        "normalization_result": None,
        "candidates": [],
    }
    if not result.get("ok"):
        return attempt
    items = coerce_items(result.get("payload"))
    attempt["payload_empty"] = len(items) == 0
    attempt["payload_sample_rows"] = _sample_records(items)
    candidates = _dedupe_candidates(_normalize_market_bundle(result.get("payload"), detection_source=source, now=utc_now()))
    attempt["candidates"] = candidates
    attempt["normalization_result"] = _normalize_result_summary(candidates)
    return attempt


def _broad_scan_attempts(now=None) -> list[dict[str, Any]]:
    current = now or utc_now()
    end_min = (current - timedelta(seconds=1800)).isoformat()
    end_max = (current + timedelta(seconds=1800)).isoformat()
    attempts = []
    for page in range(ROUTER_MAX_PAGES):
        offset = page * ROUTER_PAGE_LIMIT
        attempts.append(
            _broad_scan_attempt(
                "/markets",
                params={
                    "closed": False,
                    "end_date_min": end_min,
                    "end_date_max": end_max,
                    "order": "endDate",
                    "ascending": True,
                    "limit": ROUTER_PAGE_LIMIT,
                    "offset": offset,
                },
                source="broad_market_window_scan",
            )
        )
    attempts.append(
        _broad_scan_attempt(
            "/markets",
            params={"closed": False, "order": "endDate", "ascending": True, "limit": ROUTER_PAGE_LIMIT, "offset": 0},
            source="broad_market_open_scan",
        )
    )
    attempts.append(
        _broad_scan_attempt(
            "/events",
            params={"closed": False, "limit": ROUTER_PAGE_LIMIT},
            source="broad_event_open_scan",
        )
    )
    attempts.append(
        _broad_scan_attempt(
            "/events",
            params={"limit": ROUTER_PAGE_LIMIT},
            source="broad_event_scan",
        )
    )
    return attempts


def _looks_like_eth_5m(candidate: dict[str, Any]) -> bool:
    slug = _normalize_series_text(candidate.get("slug"))
    title = _normalize_series_text(candidate.get("title"))
    if slug.startswith(SLUG_PREFIX):
        return True
    text = f"{slug} {title}"
    return any(word in text for word in ETH_KEYWORDS) and any(word in text for word in FIVE_MINUTE_WORDS)


def _candidate_reason(candidate: dict[str, Any], *, now=None) -> tuple[int, str]:
    current = now or utc_now()
    start = candidate.get("start")
    end = candidate.get("end")
    if start is None or end is None:
        return 50_000, "missing_market_window"
    if end <= start:
        return 40_000, "invalid_market_window"
    duration_penalty = abs(int((end - start).total_seconds()) - TARGET_DURATION_SECONDS)
    if start <= current < end:
        timing_penalty = 0
        timing_reason = "active"
    elif current < start <= current + timedelta(seconds=ROUTER_UPCOMING_TOLERANCE_SECONDS):
        timing_penalty = int((start - current).total_seconds())
        timing_reason = "upcoming"
    else:
        return 35_000, "outside_relevant_window"
    slug_penalty = 0 if _normalize_series_text(candidate.get("slug")).startswith(SLUG_PREFIX) else 5_000
    token_penalty = 0 if candidate.get("token_yes") and candidate.get("token_no") else 7_500
    status_penalty = 0 if str(candidate.get("status") or "").lower() == "open" else 1_000
    score = duration_penalty + timing_penalty + slug_penalty + token_penalty + status_penalty
    reason = (
        f"{timing_reason}; "
        f"duration_off_seconds={duration_penalty}; "
        f"timing_penalty={timing_penalty}; "
        f"slug_penalty={slug_penalty}; "
        f"token_penalty={token_penalty}; "
        f"status_penalty={status_penalty}"
    )
    return score, reason


def _filter_candidates(candidates: list[dict[str, Any]], *, now=None) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    current = now or utc_now()
    diagnostics = {
        "counts": {
            "normalized_total": len(candidates),
            "eth_5m_family_match": 0,
            "time_relevant": 0,
            "token_complete": 0,
            "parse_failure": 0,
            "filtered_final": 0,
        },
        "exclusion_counts": {
            "missing_market_window": 0,
            "outside_relevant_window": 0,
            "not_eth_5m_family": 0,
            "missing_tokens": 0,
        },
        "samples": {
            "normalized": _sample_records(candidates),
            "final_candidates": [],
        },
    }
    filtered = []
    for candidate in candidates:
        start = candidate.get("start")
        end = candidate.get("end")
        if start is None or end is None or end <= start:
            diagnostics["counts"]["parse_failure"] += 1
            diagnostics["exclusion_counts"]["missing_market_window"] += 1
            continue
        if not _looks_like_eth_5m(candidate):
            diagnostics["exclusion_counts"]["not_eth_5m_family"] += 1
            continue
        diagnostics["counts"]["eth_5m_family_match"] += 1
        is_active = start <= current < end
        is_upcoming = current < start <= current + timedelta(seconds=ROUTER_UPCOMING_TOLERANCE_SECONDS)
        if not (is_active or is_upcoming):
            diagnostics["exclusion_counts"]["outside_relevant_window"] += 1
            continue
        diagnostics["counts"]["time_relevant"] += 1
        if not (candidate.get("token_yes") and candidate.get("token_no")):
            diagnostics["exclusion_counts"]["missing_tokens"] += 1
            continue
        diagnostics["counts"]["token_complete"] += 1
        candidate["selection_bucket"] = "active" if is_active else "upcoming"
        filtered.append(candidate)
    diagnostics["counts"]["filtered_final"] = len(filtered)
    diagnostics["samples"]["final_candidates"] = _sample_records(filtered)
    return filtered, diagnostics


def _collect_candidates(attempts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    combined = []
    for attempt in attempts:
        combined.extend(attempt.get("candidates") or [])
    return _dedupe_candidates(combined)


def _select_candidate(candidates: list[dict[str, Any]], *, now=None) -> tuple[Optional[dict[str, Any]], Optional[int], Optional[str]]:
    if not candidates:
        return None, None, None
    scored = []
    for candidate in candidates:
        score, reason = _candidate_reason(candidate, now=now)
        scored.append((score, candidate, reason))
    scored.sort(key=lambda item: item[0])
    best_score, best, reason = scored[0]
    return best, best_score, reason


def route_eth_5m_market(*, now=None) -> dict[str, Any]:
    current = now or utc_now()
    now_epoch = int(current.timestamp())
    bucket_ts = _bucket_ts(current)
    slug_attempts = _slug_attempts(current)
    slug_candidates = _collect_candidates(slug_attempts)
    filtered_slug_candidates, slug_filter_summary = _filter_candidates(slug_candidates, now=current)

    all_attempts = list(slug_attempts)
    broad_attempts: list[dict[str, Any]] = []
    broad_candidates: list[dict[str, Any]] = []
    broad_filter_summary: Optional[dict[str, Any]] = None

    if not filtered_slug_candidates:
        broad_attempts = _broad_scan_attempts(current)
        broad_candidates = _collect_candidates(broad_attempts)
        _, broad_filter_summary = _filter_candidates(broad_candidates, now=current)
        all_attempts.extend(broad_attempts)

    combined_candidates = _dedupe_candidates(slug_candidates + broad_candidates)
    filtered_candidates, combined_filter_summary = _filter_candidates(combined_candidates, now=current)
    best, best_score, best_reason = _select_candidate(filtered_candidates, now=current)

    diagnostics = {
        "now_utc_epoch": now_epoch,
        "bucket_ts": bucket_ts,
        "candidate_slugs_attempted": _candidate_slugs(current),
        "request_diagnostics": [
            {
                "source": attempt.get("source"),
                "slug": attempt.get("slug"),
                "path": attempt.get("path"),
                "params": attempt.get("params"),
                "exact_slug_endpoint_used": attempt.get("exact_slug_endpoint_used"),
                "http_status": attempt.get("http_status"),
                "ok": attempt.get("ok"),
                "error_kind": attempt.get("error_kind"),
                "error": attempt.get("error"),
                "browser_like_headers_applied": attempt.get("browser_like_headers_applied"),
                "headers_applied": attempt.get("headers_applied"),
                "payload_empty": attempt.get("payload_empty"),
                "payload_sample_rows": attempt.get("payload_sample_rows"),
                "normalization_result": attempt.get("normalization_result"),
            }
            for attempt in all_attempts
        ],
        "slug_phase_summary": {
            "normalized_candidate_count": len(slug_candidates),
            "filtered_candidate_count": len(filtered_slug_candidates),
            "filter_summary": slug_filter_summary,
        },
        "broad_phase_summary": {
            "attempted": bool(broad_attempts),
            "normalized_candidate_count": len(broad_candidates),
            "filter_summary": broad_filter_summary,
        },
        "selection_summary": {
            "normalized_candidate_count": len(combined_candidates),
            "filtered_candidate_count": len(filtered_candidates),
            "selected_market_id": best.get("market_id") if best else None,
            "selected_slug": best.get("slug") if best else None,
            "selection_mode": best.get("selection_bucket") if best else None,
        },
        "combined_filter_summary": combined_filter_summary,
    }

    if best is None:
        request_ok_count = sum(1 for attempt in all_attempts if attempt.get("ok"))
        request_row_count = sum(
            int((attempt.get("normalization_result") or {}).get("extracted_candidate_count") or 0)
            for attempt in all_attempts
            if attempt.get("ok")
        )
        if request_ok_count == 0:
            routing_reason = "gamma_http_failure"
        elif request_row_count == 0:
            routing_reason = "no_candidates_found"
        else:
            routing_reason = "rows_returned_but_filtered_out"
        return {
            "market": None,
            "routing_reason": routing_reason,
            "detection_source": None,
            "diagnostics": diagnostics,
        }

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
        routing_reason=best_reason or "candidate_selected",
        routing_score=best_score,
        raw_market={
            "candidate": best.get("raw_market"),
            "payload_fragment": best.get("raw_payload"),
        },
    )
    return {
        "market": routed.as_dict(),
        "routing_reason": routed.routing_reason,
        "detection_source": routed.detection_source,
        "diagnostics": diagnostics,
    }
