from __future__ import annotations

import json
import math
import re
from collections import Counter
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


QUOTE_FILENAMES = {"market_quotes.jsonl"}
META_FILENAMES = {"market_meta.jsonl"}


def parse_csv_floats(value: str) -> list[float]:
    items = [item.strip() for item in str(value).split(",") if item.strip()]
    if not items:
        raise ValueError("empty comma-separated float list")
    return [float(item) for item in items]


def parse_csv_strings(value: str) -> list[str]:
    items = [item.strip() for item in str(value).split(",") if item.strip()]
    if not items:
        raise ValueError("empty comma-separated list")
    return items


def utc_ts(value: Any) -> pd.Timestamp | pd.NaT:
    if value is None or value == "":
        return pd.NaT
    ts = pd.to_datetime(value, utc=True, errors="coerce")
    return ts


def number(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        result = float(value)
    except Exception:
        return None
    return result if np.isfinite(result) else None


def side_value(payload: Any, *keys: str) -> Any:
    if isinstance(payload, (int, float, str)):
        return payload
    if not isinstance(payload, dict):
        return None
    for key in keys:
        if key in payload and payload.get(key) is not None:
            return payload.get(key)
    return None


def _book_payload(raw_payload_fragment: Any, side: str) -> dict[str, Any] | None:
    if not isinstance(raw_payload_fragment, dict):
        return None
    raw = raw_payload_fragment.get(f"{side}_raw")
    if not isinstance(raw, dict):
        return None
    book = raw.get("book")
    if isinstance(book, dict):
        nested = book.get("book")
        return nested if isinstance(nested, dict) else book
    return None


def _level_price_size(level: Any) -> tuple[float | None, float | None]:
    if isinstance(level, dict):
        return number(level.get("price") or level.get("p")), number(level.get("size") or level.get("quantity") or level.get("qty") or level.get("q"))
    if isinstance(level, (list, tuple)) and len(level) >= 2:
        return number(level[0]), number(level[1])
    return None, None


def best_bid_level_from_book(book: dict[str, Any] | None) -> tuple[float | None, float | None]:
    if not isinstance(book, dict):
        return None, None
    best_price: float | None = None
    best_size: float | None = None
    for level in book.get("bids") or []:
        price, size = _level_price_size(level)
        if price is not None and (size is None or size > 0):
            if best_price is None or price > best_price:
                best_price = price
                best_size = size
    return best_price, best_size


def best_ask_level_from_book(book: dict[str, Any] | None) -> tuple[float | None, float | None]:
    if not isinstance(book, dict):
        return None, None
    best_price: float | None = None
    best_size: float | None = None
    for level in book.get("asks") or []:
        price, size = _level_price_size(level)
        if price is not None and (size is None or size > 0):
            if best_price is None or price < best_price:
                best_price = price
                best_size = size
    return best_price, best_size


def best_bid_from_book(book: dict[str, Any] | None) -> float | None:
    return best_bid_level_from_book(book)[0]


def best_ask_from_book(book: dict[str, Any] | None) -> float | None:
    return best_ask_level_from_book(book)[0]


def last_trade_from_book(book: dict[str, Any] | None) -> float | None:
    if not isinstance(book, dict):
        return None
    return number(book.get("last_trade_price") or book.get("lastTradePrice"))


def discover_jsonl_files(path: Path, filenames: set[str]) -> list[Path]:
    if not path.exists():
        raise FileNotFoundError(path)
    if path.is_file():
        return [path]
    files = [child for child in sorted(path.rglob("*.jsonl")) if child.name in filenames]
    if not files:
        files = [child for child in sorted(path.rglob("*.jsonl"))]
    return files


def read_jsonl_records(path_or_dir: Path, filenames: set[str]) -> tuple[list[dict[str, Any]], list[str]]:
    records: list[dict[str, Any]] = []
    errors: list[str] = []
    for path in discover_jsonl_files(path_or_dir, filenames):
        with path.open("r", encoding="utf-8") as handle:
            for line_no, line in enumerate(handle, start=1):
                line = line.strip()
                if not line:
                    continue
                try:
                    payload = json.loads(line)
                except Exception as exc:
                    errors.append(f"{path}:{line_no}: {exc}")
                    continue
                if isinstance(payload, dict):
                    records.append(payload)
                else:
                    errors.append(f"{path}:{line_no}: non-dict-json")
    return records, errors


def market_key_from_slug(slug: Any) -> str | None:
    if not slug:
        return None
    return str(slug)


def infer_window_from_slug(slug: Any, market_window_seconds: int) -> tuple[pd.Timestamp | pd.NaT, pd.Timestamp | pd.NaT]:
    if not slug:
        return pd.NaT, pd.NaT
    match = re.search(r"-(\d{10})(?:$|[^0-9])", str(slug))
    if not match:
        return pd.NaT, pd.NaT
    start = pd.to_datetime(int(match.group(1)), unit="s", utc=True)
    return start, start + pd.Timedelta(seconds=market_window_seconds)


def _nested(row: dict[str, Any], side: str, key: str) -> Any:
    nested = row.get(side)
    if isinstance(nested, dict):
        return nested.get(key)
    if key in {"price", "last", "last_price", "mid"} and isinstance(nested, (int, float, str)):
        return nested
    return row.get(f"{side}_{key}")


def normalize_quote_record(row: dict[str, Any], market_window_seconds: int) -> dict[str, Any]:
    slug = row.get("slug") or row.get("market_slug")
    market_id = row.get("market_id") or row.get("id")
    condition_id = row.get("condition_id") or row.get("conditionId")
    market_key = market_key_from_slug(slug) or (str(condition_id) if condition_id else None) or (str(market_id) if market_id else None)
    ts = utc_ts(row.get("ts") or row.get("timestamp") or row.get("quote_ts") or row.get("fetched_at"))
    start = utc_ts(row.get("market_start_time") or row.get("start_time") or row.get("startDate") or row.get("market_start_ts"))
    end = utc_ts(row.get("market_end_time") or row.get("end_time") or row.get("endDate") or row.get("market_end_ts"))
    if pd.isna(start) and not pd.isna(end):
        start = end - pd.Timedelta(seconds=market_window_seconds)
    if pd.isna(start) or pd.isna(end):
        inferred_start, inferred_end = infer_window_from_slug(slug, market_window_seconds)
        if pd.isna(start):
            start = inferred_start
        if pd.isna(end):
            end = inferred_end
    yes_payload = row.get("yes")
    no_payload = row.get("no")
    yes_book = _book_payload(row.get("raw_payload_fragment"), "yes")
    no_book = _book_payload(row.get("raw_payload_fragment"), "no")
    yes_book_bid, yes_book_bid_size = best_bid_level_from_book(yes_book)
    yes_book_ask, yes_book_ask_size = best_ask_level_from_book(yes_book)
    no_book_bid, no_book_bid_size = best_bid_level_from_book(no_book)
    no_book_ask, no_book_ask_size = best_ask_level_from_book(no_book)
    yes_bid = yes_book_bid if yes_book_bid is not None else number(side_value(yes_payload, "best_bid", "bid") or row.get("yes_bid"))
    yes_ask = yes_book_ask if yes_book_ask is not None else number(side_value(yes_payload, "best_ask", "ask") or row.get("yes_ask"))
    yes_mid = number(side_value(yes_payload, "mid") or row.get("yes_mid"))
    yes_last = last_trade_from_book(yes_book) or number(side_value(yes_payload, "last", "last_price", "price") or row.get("yes_last") or row.get("yes_price"))
    yes_spread = number(_nested(row, "yes", "spread") or row.get("yes_spread"))
    no_bid = no_book_bid if no_book_bid is not None else number(side_value(no_payload, "best_bid", "bid") or row.get("no_bid"))
    no_ask = no_book_ask if no_book_ask is not None else number(side_value(no_payload, "best_ask", "ask") or row.get("no_ask"))
    no_mid = number(side_value(no_payload, "mid") or row.get("no_mid"))
    no_last = last_trade_from_book(no_book) or number(side_value(no_payload, "last", "last_price", "price") or row.get("no_last") or row.get("no_price"))
    no_spread = number(_nested(row, "no", "spread") or row.get("no_spread"))
    if yes_mid is None and yes_bid is not None and yes_ask is not None:
        yes_mid = (yes_bid + yes_ask) / 2.0
    if no_mid is None and no_bid is not None and no_ask is not None:
        no_mid = (no_bid + no_ask) / 2.0
    if yes_spread is None and yes_bid is not None and yes_ask is not None:
        yes_spread = yes_ask - yes_bid
    if no_spread is None and no_bid is not None and no_ask is not None:
        no_spread = no_ask - no_bid
    return {
        "market_key": market_key,
        "market_slug": None if slug is None else str(slug),
        "market_id": None if market_id is None else str(market_id),
        "condition_id": None if condition_id is None else str(condition_id),
        "quote_ts": ts,
        "market_start_ts": start,
        "market_end_ts": end,
        "source_type": row.get("source"),
        "yes_bid": yes_bid,
        "yes_ask": yes_ask,
        "yes_bid_size": yes_book_bid_size,
        "yes_ask_size": yes_book_ask_size,
        "yes_mid": yes_mid,
        "yes_last": yes_last,
        "yes_spread": yes_spread,
        "no_bid": no_bid,
        "no_ask": no_ask,
        "no_bid_size": no_book_bid_size,
        "no_ask_size": no_book_ask_size,
        "no_mid": no_mid,
        "no_last": no_last,
        "no_spread": no_spread,
    }


def load_quote_frame(path_or_dir: Path, market_window_seconds: int) -> tuple[pd.DataFrame, dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    errors: list[str] = []
    schema_fields: set[str] = set()
    loaded_rows = 0
    for path in discover_jsonl_files(path_or_dir, QUOTE_FILENAMES):
        with path.open("r", encoding="utf-8") as handle:
            for line_no, line in enumerate(handle, start=1):
                line = line.strip()
                if not line:
                    continue
                try:
                    payload = json.loads(line)
                except Exception as exc:
                    errors.append(f"{path}:{line_no}: {exc}")
                    continue
                if not isinstance(payload, dict):
                    errors.append(f"{path}:{line_no}: non-dict-json")
                    continue
                loaded_rows += 1
                if loaded_rows <= 200:
                    schema_fields.update(payload.keys())
                normalized.append(normalize_quote_record(payload, market_window_seconds))
    frame = pd.DataFrame(normalized)
    diagnostics = {
        "loaded_rows": loaded_rows,
        "json_errors": errors[:20],
        "json_error_count": len(errors),
        "schema_fields_detected": sorted(schema_fields),
    }
    if frame.empty:
        return frame, diagnostics
    frame = frame.sort_values(["market_key", "quote_ts"]).reset_index(drop=True)
    diagnostics["markets_discovered"] = int(frame["market_key"].nunique(dropna=True))
    return frame, diagnostics


def load_market_meta(path_or_dir: Path | None, market_window_seconds: int) -> tuple[pd.DataFrame, dict[str, Any]]:
    if path_or_dir is None:
        return pd.DataFrame(), {"loaded_rows": 0, "markets_with_metadata": 0}
    if path_or_dir.is_dir():
        files = [child for child in sorted(path_or_dir.rglob("*.jsonl")) if child.name in META_FILENAMES]
        if not files:
            return pd.DataFrame(), {"loaded_rows": 0, "markets_with_metadata": 0, "note": "no dedicated market metadata JSONL files found"}
    else:
        files = [path_or_dir]
    rows = []
    errors: list[str] = []
    loaded_rows = 0
    for path in files:
        with path.open("r", encoding="utf-8") as handle:
            for line_no, line in enumerate(handle, start=1):
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except Exception as exc:
                    errors.append(f"{path}:{line_no}: {exc}")
                    continue
                if not isinstance(record, dict):
                    errors.append(f"{path}:{line_no}: non-dict-json")
                    continue
                loaded_rows += 1
                market = record.get("market") if isinstance(record.get("market"), dict) else record
                slug = market.get("slug") or record.get("slug")
                market_id = market.get("market_id") or market.get("id") or record.get("market_id")
                condition_id = market.get("condition_id") or market.get("conditionId") or record.get("condition_id")
                key = market_key_from_slug(slug) or (str(condition_id) if condition_id else None) or (str(market_id) if market_id else None)
                start = utc_ts(market.get("start_time") or market.get("start") or market.get("eventStartTime") or market.get("startTime"))
                end = utc_ts(market.get("end_time") or market.get("end") or market.get("endDate"))
                if pd.isna(start) and not pd.isna(end):
                    start = end - pd.Timedelta(seconds=market_window_seconds)
                if pd.isna(start) or pd.isna(end):
                    inferred_start, inferred_end = infer_window_from_slug(slug, market_window_seconds)
                    start = start if not pd.isna(start) else inferred_start
                    end = end if not pd.isna(end) else inferred_end
                rows.append({"market_key": key, "market_slug": slug, "market_id": market_id, "condition_id": condition_id, "market_start_ts_meta": start, "market_end_ts_meta": end})
    frame = pd.DataFrame(rows)
    if not frame.empty:
        frame = frame.dropna(subset=["market_key"]).drop_duplicates("market_key", keep="last")
    return frame, {"loaded_rows": loaded_rows, "json_error_count": len(errors), "json_errors": errors[:20], "markets_with_metadata": int(frame["market_key"].nunique()) if not frame.empty else 0}


def apply_metadata(quotes: pd.DataFrame, meta: pd.DataFrame) -> pd.DataFrame:
    if quotes.empty or meta.empty:
        return quotes
    merged = quotes.merge(meta[["market_key", "market_start_ts_meta", "market_end_ts_meta"]], on="market_key", how="left")
    merged["market_start_ts"] = merged["market_start_ts"].where(merged["market_start_ts"].notna(), merged["market_start_ts_meta"])
    merged["market_end_ts"] = merged["market_end_ts"].where(merged["market_end_ts"].notna(), merged["market_end_ts_meta"])
    return merged.drop(columns=["market_start_ts_meta", "market_end_ts_meta"])


def quality_filter_quotes(
    quotes: pd.DataFrame,
    *,
    price_source: str,
    max_spread: float,
    mid_complement_tolerance: float,
) -> tuple[pd.DataFrame, dict[str, int]]:
    counts: Counter[str] = Counter()
    if quotes.empty:
        return quotes.copy(), {}
    keep = pd.Series(True, index=quotes.index)
    checks = {
        "missing_timestamp": quotes["quote_ts"].isna(),
        "missing_market_key": quotes["market_key"].isna(),
        "missing_market_window": quotes["market_start_ts"].isna() | quotes["market_end_ts"].isna(),
    }
    if price_source == "mid":
        checks["missing_mid"] = quotes["yes_mid"].isna() | quotes["no_mid"].isna()
        checks["mid_complement"] = (quotes["yes_mid"] + quotes["no_mid"] - 1.0).abs() > mid_complement_tolerance
    elif price_source == "bid":
        checks["missing_bid"] = quotes["yes_bid"].isna() | quotes["no_bid"].isna()
    else:
        raise ValueError(f"unsupported price source: {price_source}")
    spread_cols = [col for col in ("yes_spread", "no_spread") if col in quotes.columns]
    if spread_cols:
        checks["wide_spread"] = quotes[spread_cols].max(axis=1, skipna=True) > max_spread
    for reason, mask in checks.items():
        mask = mask.fillna(False)
        counts[reason] = int(mask.sum())
        keep &= ~mask
    filtered = quotes.loc[keep].copy()
    if price_source == "mid":
        filtered["yes_price_source"] = filtered["yes_mid"]
        filtered["no_price_source"] = filtered["no_mid"]
    else:
        filtered["yes_price_source"] = filtered["yes_bid"]
        filtered["no_price_source"] = filtered["no_bid"]
    counts["kept_rows"] = int(len(filtered))
    counts["dropped_rows"] = int(len(quotes) - len(filtered))
    return filtered.sort_values(["market_key", "quote_ts"]).reset_index(drop=True), dict(counts)


def _side_conviction_time(
    market: pd.DataFrame,
    side_col: str,
    threshold: float,
    definition: str,
    *,
    min_later_share: float,
    tolerant_floor: float,
    min_later_quotes: int,
) -> tuple[pd.Timestamp | None, str | None]:
    crosses = market[market[side_col] >= threshold]
    if crosses.empty:
        return None, None
    for idx in crosses.index:
        candidate_ts = market.loc[idx, "quote_ts"]
        later = market[market["quote_ts"] >= candidate_ts]
        if len(later) < min_later_quotes:
            end = market["market_end_ts"].iloc[0]
            if pd.notna(end) and (end - candidate_ts).total_seconds() > 15:
                continue
        floor = threshold if definition == "strict" else tolerant_floor
        later_share = float((later[side_col] >= floor).mean()) if len(later) else 0.0
        if definition == "strict" and later_share == 1.0:
            return candidate_ts, None
        if definition == "tolerant" and later_share >= min_later_share:
            return candidate_ts, None
    return None, "insufficient_later_quotes_or_failed_persistence"


def terminal_conviction_for_market(
    market: pd.DataFrame,
    *,
    threshold: float,
    price_source: str,
    definition: str,
    min_later_share: float,
    tolerant_floor_offset: float,
    min_later_quotes: int,
    min_quality_quotes_per_market: int,
) -> dict[str, Any]:
    market = market.sort_values("quote_ts").reset_index(drop=True)
    base = {
        "market_key": market["market_key"].iloc[0] if not market.empty else None,
        "market_slug": market["market_slug"].iloc[0] if not market.empty and "market_slug" in market else None,
        "market_start_ts": market["market_start_ts"].iloc[0] if not market.empty else pd.NaT,
        "market_end_ts": market["market_end_ts"].iloc[0] if not market.empty else pd.NaT,
        "threshold": threshold,
        "price_source": price_source,
        "conviction_definition": definition,
        "quality_quote_count": int(len(market)),
        "reached_terminal_conviction": False,
        "convicted_side": None,
        "terminal_conviction_ts": pd.NaT,
        "conviction_market_age_seconds": None,
        "conviction_remaining_seconds": None,
        "notes": "",
    }
    if market.empty or len(market) < min_quality_quotes_per_market:
        base["notes"] = "insufficient_quality_quotes"
        return base
    tolerant_floor = max(0.0, threshold - tolerant_floor_offset)
    yes_ts, yes_note = _side_conviction_time(market, "yes_price_source", threshold, definition, min_later_share=min_later_share, tolerant_floor=tolerant_floor, min_later_quotes=min_later_quotes)
    no_ts, no_note = _side_conviction_time(market, "no_price_source", threshold, definition, min_later_share=min_later_share, tolerant_floor=tolerant_floor, min_later_quotes=min_later_quotes)
    candidates = [(side, ts) for side, ts in (("YES", yes_ts), ("NO", no_ts)) if ts is not None]
    if candidates:
        side, ts = min(candidates, key=lambda item: item[1])
        base["reached_terminal_conviction"] = True
        base["convicted_side"] = side
        base["terminal_conviction_ts"] = ts
        start = base["market_start_ts"]
        end = base["market_end_ts"]
        base["conviction_market_age_seconds"] = None if pd.isna(start) else float((ts - start).total_seconds())
        base["conviction_remaining_seconds"] = None if pd.isna(end) else float((end - ts).total_seconds())
    else:
        base["notes"] = ";".join(sorted({note for note in (yes_note, no_note) if note}))
    base.update(
        {
            "first_quality_quote_ts": market["quote_ts"].min(),
            "last_quality_quote_ts": market["quote_ts"].max(),
            "max_yes_price_source": float(market["yes_price_source"].max()),
            "max_no_price_source": float(market["no_price_source"].max()),
            "final_yes_price_source": float(market["yes_price_source"].iloc[-1]),
            "final_no_price_source": float(market["no_price_source"].iloc[-1]),
        }
    )
    return base


def compute_terminal_convictions(
    quotes: pd.DataFrame,
    *,
    thresholds: list[float],
    sources: list[str],
    definitions: list[str],
    max_spread: float,
    mid_complement_tolerance: float,
    min_later_share: float,
    tolerant_floor_offset: float,
    min_later_quotes: int,
    min_quality_quotes_per_market: int,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    quality_rows: list[dict[str, Any]] = []
    for source in sources:
        filtered, counts = quality_filter_quotes(quotes, price_source=source, max_spread=max_spread, mid_complement_tolerance=mid_complement_tolerance)
        quality_rows.append({"price_source": source, **counts})
        for market_key, group in filtered.groupby("market_key", dropna=False):
            for threshold in thresholds:
                for definition in definitions:
                    rows.append(
                        terminal_conviction_for_market(
                            group,
                            threshold=threshold,
                            price_source=source,
                            definition=definition,
                            min_later_share=min_later_share,
                            tolerant_floor_offset=tolerant_floor_offset,
                            min_later_quotes=min_later_quotes,
                            min_quality_quotes_per_market=min_quality_quotes_per_market,
                        )
                    )
    by_market = pd.DataFrame(rows)
    quality = pd.DataFrame(quality_rows)
    diagnostics = {"quality_by_source": quality_rows}
    return by_market, quality, diagnostics


def summarize_convictions(by_market: pd.DataFrame) -> pd.DataFrame:
    if by_market.empty:
        return pd.DataFrame()
    rows = []
    for keys, group in by_market.groupby(["threshold", "price_source", "conviction_definition"], dropna=False):
        threshold, source, definition = keys
        sufficient = group[group["quality_quote_count"] > 0]
        convicted = group[group["reached_terminal_conviction"] == True]
        age = pd.to_numeric(convicted["conviction_market_age_seconds"], errors="coerce").dropna()
        rem = pd.to_numeric(convicted["conviction_remaining_seconds"], errors="coerce").dropna()
        row = {
            "threshold": threshold,
            "price_source": source,
            "conviction_definition": definition,
            "markets_total": int(group["market_key"].nunique()),
            "markets_with_sufficient_quotes": int(sufficient["market_key"].nunique()),
            "markets_convicted": int(convicted["market_key"].nunique()),
            "share_convicted": float(len(convicted) / len(group)) if len(group) else None,
            "share_never_convicted": float(1.0 - len(convicted) / len(group)) if len(group) else None,
            "yes_conviction_share": float((convicted["convicted_side"] == "YES").mean()) if len(convicted) else None,
            "no_conviction_share": float((convicted["convicted_side"] == "NO").mean()) if len(convicted) else None,
        }
        for name, series in (("conviction_age_seconds", age), ("remaining_seconds", rem)):
            row[f"mean_{name}"] = float(series.mean()) if len(series) else None
            row[f"median_{name}"] = float(series.median()) if len(series) else None
            for q in (10, 25, 75, 90):
                row[f"p{q}_{name}"] = float(np.percentile(series, q)) if len(series) else None
        rows.append(row)
    return pd.DataFrame(rows)


def brier(y: np.ndarray, p: np.ndarray) -> float:
    p = np.clip(p.astype(float), 1e-6, 1 - 1e-6)
    return float(np.mean((p - y.astype(float)) ** 2))


def log_loss(y: np.ndarray, p: np.ndarray) -> float:
    p = np.clip(p.astype(float), 1e-6, 1 - 1e-6)
    y = y.astype(float)
    return float(-np.mean(y * np.log(p) + (1 - y) * np.log(1 - p)))


def ece(y: np.ndarray, p: np.ndarray, bins: int = 10) -> float:
    frame = pd.DataFrame({"y": y.astype(float), "p": np.clip(p.astype(float), 1e-6, 1 - 1e-6)})
    frame["bucket"] = pd.cut(frame["p"], bins=np.linspace(0, 1, bins + 1), include_lowest=True)
    grouped = frame.groupby("bucket", observed=False)
    total = len(frame)
    if total == 0:
        return float("nan")
    value = 0.0
    for _, group in grouped:
        if group.empty:
            continue
        value += len(group) / total * abs(float(group["p"].mean()) - float(group["y"].mean()))
    return float(value)


def auc(y: np.ndarray, p: np.ndarray) -> float | None:
    try:
        from sklearn.metrics import roc_auc_score
    except Exception:
        return None
    if len(np.unique(y)) < 2:
        return None
    return float(roc_auc_score(y, p))


def detect_prediction_columns(predictions: pd.DataFrame) -> dict[str, str]:
    cols = set(predictions.columns)
    def pick(candidates: list[str], required: bool = True) -> str | None:
        matches = [c for c in candidates if c in cols]
        if matches:
            return matches[0]
        if required:
            raise ValueError(f"Could not detect required prediction column from {candidates}. Available columns: {sorted(cols)}")
        return None
    return {
        "timestamp": pick(["prediction_ts", "timestamp", "ts", "quote_ts"]),
        "market_key": pick(["market_key", "market_id", "slug", "market_slug"], required=False),
        "market_start": pick(["market_start_ts", "market_window_start"], required=False),
        "market_end": pick(["market_end_ts", "market_window_end"], required=False),
        "model": pick(["model", "model_id", "model_name", "family"]),
        "p": pick(["p_up", "probability", "p", "predicted_probability"]),
        "y": pick(["y_true", "result_up", "outcome", "label"]),
        "age": pick(["market_age_seconds"], required=False),
    }


def load_predictions(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".parquet":
        return pd.read_parquet(path)
    return pd.read_csv(path)


def normalize_predictions(predictions: pd.DataFrame, market_window_seconds: int) -> pd.DataFrame:
    detected = detect_prediction_columns(predictions)
    out = pd.DataFrame()
    out["prediction_ts"] = pd.to_datetime(predictions[detected["timestamp"]], utc=True, errors="coerce")
    out["model"] = predictions[detected["model"]].astype(str)
    out["p_up"] = pd.to_numeric(predictions[detected["p"]], errors="coerce")
    out["y_true"] = pd.to_numeric(predictions[detected["y"]], errors="coerce")
    if detected["market_key"]:
        out["market_key"] = predictions[detected["market_key"]].astype(str)
    elif detected["market_start"]:
        starts = pd.to_datetime(predictions[detected["market_start"]], utc=True, errors="coerce")
        out["market_key"] = starts.dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    else:
        starts = out["prediction_ts"].dt.floor(f"{market_window_seconds}s")
        out["market_key"] = starts.dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    if detected["market_start"]:
        out["market_start_ts"] = pd.to_datetime(predictions[detected["market_start"]], utc=True, errors="coerce")
    else:
        out["market_start_ts"] = out["prediction_ts"].dt.floor(f"{market_window_seconds}s")
    if detected["market_end"]:
        out["market_end_ts"] = pd.to_datetime(predictions[detected["market_end"]], utc=True, errors="coerce")
    else:
        out["market_end_ts"] = out["market_start_ts"] + pd.Timedelta(seconds=market_window_seconds)
    if detected["age"]:
        out["market_age_seconds"] = pd.to_numeric(predictions[detected["age"]], errors="coerce")
    else:
        out["market_age_seconds"] = (out["prediction_ts"] - out["market_start_ts"]).dt.total_seconds()
    return out.dropna(subset=["prediction_ts", "model", "p_up", "y_true"])


def align_prediction_market_keys(predictions: pd.DataFrame, convictions: pd.DataFrame, market_window_seconds: int) -> pd.DataFrame:
    out = predictions.copy()
    if convictions.empty or "market_key" not in convictions.columns:
        return out
    conviction_keys = set(convictions["market_key"].astype(str).unique()) if not convictions.empty else set()
    if out["market_key"].astype(str).isin(conviction_keys).any():
        return out
    slug_map = convictions[["market_key", "market_start_ts"]].dropna().drop_duplicates()
    if slug_map.empty:
        return out
    out = out.drop(columns=["market_key"]).merge(slug_map, on="market_start_ts", how="left")
    out["market_key"] = out["market_key"].fillna(out["market_start_ts"].dt.strftime("%Y-%m-%dT%H:%M:%SZ"))
    return out


def join_predictions_to_convictions(predictions: pd.DataFrame, convictions: pd.DataFrame, market_window_seconds: int) -> pd.DataFrame:
    preds = align_prediction_market_keys(predictions, convictions, market_window_seconds)
    join_cols = ["market_key", "threshold", "price_source", "conviction_definition", "terminal_conviction_ts", "convicted_side", "reached_terminal_conviction"]
    if convictions.empty or any(column not in convictions.columns for column in join_cols):
        joined = preds.copy()
        joined["threshold"] = np.nan
        joined["price_source"] = None
        joined["conviction_definition"] = None
        joined["terminal_conviction_ts"] = pd.NaT
        joined["convicted_side"] = None
        joined["reached_terminal_conviction"] = np.nan
        joined["prediction_phase"] = "unknown"
        return joined
    joined = preds.merge(convictions[join_cols], on="market_key", how="left")
    joined["prediction_ts"] = pd.to_datetime(joined["prediction_ts"], utc=True, errors="coerce")
    joined["terminal_conviction_ts"] = pd.to_datetime(joined["terminal_conviction_ts"], utc=True, errors="coerce")
    joined["prediction_phase"] = "unknown"
    never = joined["reached_terminal_conviction"] == False
    reached = joined["reached_terminal_conviction"] == True
    joined.loc[never, "prediction_phase"] = "never_convicted"
    joined.loc[reached & (joined["prediction_ts"] < joined["terminal_conviction_ts"]), "prediction_phase"] = "pre_conviction"
    joined.loc[reached & (joined["prediction_ts"] >= joined["terminal_conviction_ts"]), "prediction_phase"] = "post_conviction"
    return joined


def market_age_bucket(seconds: Any) -> str:
    value = number(seconds)
    if value is None:
        return "unknown"
    if value < 60:
        return "0_60"
    if value < 120:
        return "60_120"
    if value < 180:
        return "120_180"
    if value < 240:
        return "180_240"
    return "240_300"


def prediction_metrics(joined: pd.DataFrame, extra_group_cols: list[str] | None = None) -> pd.DataFrame:
    if joined.empty:
        return pd.DataFrame()
    group_cols = ["model", "threshold", "price_source", "conviction_definition", "prediction_phase"] + (extra_group_cols or [])
    rows = []
    for keys, group in joined.groupby(group_cols, dropna=False):
        y = pd.to_numeric(group["y_true"], errors="coerce").to_numpy(dtype=float)
        p = pd.to_numeric(group["p_up"], errors="coerce").to_numpy(dtype=float)
        mask = np.isfinite(y) & np.isfinite(p)
        y = y[mask]
        p = p[mask]
        row = dict(zip(group_cols, keys if isinstance(keys, tuple) else (keys,), strict=False))
        row.update(
            {
                "rows": int(len(y)),
                "markets": int(group["market_key"].nunique()),
                "brier": brier(y, p) if len(y) else None,
                "log_loss": log_loss(y, p) if len(y) else None,
                "accuracy": float(np.mean((p >= 0.5) == y)) if len(y) else None,
                "auc": auc(y, p) if len(y) else None,
                "mean_p": float(np.mean(p)) if len(y) else None,
                "realized_up_rate": float(np.mean(y)) if len(y) else None,
                "ece": ece(y, p) if len(y) else None,
                "avg_market_age_seconds": float(pd.to_numeric(group["market_age_seconds"], errors="coerce").mean()),
            }
        )
        rows.append(row)
    return pd.DataFrame(rows)


def write_parquet_or_csv(frame: pd.DataFrame, parquet_path: Path, csv_path: Path) -> str:
    try:
        frame.to_parquet(parquet_path, index=False)
        return str(parquet_path)
    except Exception:
        frame.to_csv(csv_path, index=False)
        return str(csv_path)


def source_columns(price_source: str) -> tuple[str, str]:
    if price_source == "mid":
        return "yes_mid", "no_mid"
    if price_source in {"bid", "direct_bid"}:
        return "yes_bid", "no_bid"
    if price_source in {"ask", "direct_ask"}:
        return "yes_ask", "no_ask"
    if price_source in {"last", "price"}:
        return "yes_last", "no_last"
    if price_source in {"complement_bid", "raw_best_bid_plus_complement"}:
        return "yes_complement_bid", "no_complement_bid"
    raise ValueError(f"unsupported price source: {price_source}")


def quote_diagnostics(frame: pd.DataFrame, *, debug_schema_sample: int = 0) -> dict[str, Any]:
    if frame.empty:
        return {
            "parsed_rows": 0,
            "rows_with_market_key": 0,
            "rows_with_market_window": 0,
            "markets_discovered": 0,
        }
    diagnostics: dict[str, Any] = {
        "parsed_rows": int(len(frame)),
        "rows_with_market_key": int(frame["market_key"].notna().sum()),
        "rows_with_market_window": int((frame["market_start_ts"].notna() & frame["market_end_ts"].notna()).sum()),
        "rows_with_yes_bid": int(frame["yes_bid"].notna().sum()),
        "rows_with_yes_ask": int(frame["yes_ask"].notna().sum()),
        "rows_with_yes_mid": int(frame["yes_mid"].notna().sum()),
        "rows_with_yes_last": int(frame["yes_last"].notna().sum()) if "yes_last" in frame else 0,
        "rows_with_no_bid": int(frame["no_bid"].notna().sum()),
        "rows_with_no_ask": int(frame["no_ask"].notna().sum()),
        "rows_with_no_mid": int(frame["no_mid"].notna().sum()),
        "rows_with_no_last": int(frame["no_last"].notna().sum()) if "no_last" in frame else 0,
        "markets_discovered": int(frame["market_key"].nunique(dropna=True)),
    }
    missing_key = frame["market_key"].isna()
    missing_window = frame["market_start_ts"].isna() | frame["market_end_ts"].isna()
    no_side_price = frame[["yes_bid", "yes_ask", "yes_mid", "yes_last", "no_bid", "no_ask", "no_mid", "no_last"]].isna().all(axis=1)
    diagnostics["rows_dropped_by_reason"] = {
        "missing_market_key": int(missing_key.sum()),
        "missing_market_window": int(missing_window.sum()),
        "no_side_price": int(no_side_price.sum()),
    }
    if debug_schema_sample > 0:
        sample_cols = [
            "market_key",
            "market_slug",
            "quote_ts",
            "market_start_ts",
            "market_end_ts",
            "yes_bid",
            "yes_ask",
            "yes_mid",
            "yes_last",
            "no_bid",
            "no_ask",
            "no_mid",
            "no_last",
        ]
        diagnostics["sample_parsed_rows"] = frame[[c for c in sample_cols if c in frame]].head(debug_schema_sample).to_dict(orient="records")
        diagnostics["sample_dropped_rows"] = frame.loc[missing_key | missing_window | no_side_price, [c for c in sample_cols if c in frame]].head(debug_schema_sample).to_dict(orient="records")
    return diagnostics


def filter_quotes_for_distribution(
    frame: pd.DataFrame,
    *,
    price_source: str,
    disable_spread_filter: bool,
    max_spread: float | None,
    mid_complement_tolerance: float | None,
    max_post_end_lag_seconds: float | None = 0.0,
) -> tuple[pd.DataFrame, dict[str, int]]:
    yes_col, no_col = source_columns(price_source)
    work = frame.copy()
    work["yes_complement_bid"] = pd.concat(
        [work["yes_bid"], 1.0 - work["no_ask"]],
        axis=1,
    ).max(axis=1, skipna=True)
    work["no_complement_bid"] = pd.concat(
        [work["no_bid"], 1.0 - work["yes_ask"]],
        axis=1,
    ).max(axis=1, skipna=True)
    counts: Counter[str] = Counter()
    if work.empty:
        return work, {}
    base_mask = work["quote_ts"].notna() & work["market_key"].notna() & work["market_start_ts"].notna() & work["market_end_ts"].notna()
    counts["missing_timestamp"] = int(work["quote_ts"].isna().sum())
    counts["missing_market_key"] = int(work["market_key"].isna().sum())
    counts["missing_market_window"] = int((work["market_start_ts"].isna() | work["market_end_ts"].isna()).sum())
    side_mask = work[yes_col].notna() | work[no_col].notna()
    counts[f"missing_{price_source}_both_sides"] = int((~side_mask).sum())
    keep = base_mask & side_mask
    if max_post_end_lag_seconds is not None:
        lower = work["quote_ts"] >= work["market_start_ts"]
        upper = work["quote_ts"] <= (work["market_end_ts"] + pd.to_timedelta(max_post_end_lag_seconds, unit="s"))
        outside = ~(lower & upper)
        counts["outside_market_window"] = int((outside & keep).sum())
        keep &= ~outside
    else:
        counts["outside_market_window"] = 0
    if not disable_spread_filter and max_spread is not None and price_source in {"mid", "bid", "ask"}:
        wide = pd.Series(False, index=work.index)
        if "yes_spread" in work:
            wide |= (work["yes_spread"].notna() & (work["yes_spread"] > max_spread))
        if "no_spread" in work:
            wide |= (work["no_spread"].notna() & (work["no_spread"] > max_spread))
        counts["wide_spread"] = int((wide & keep).sum())
        keep &= ~wide
    else:
        counts["wide_spread"] = 0
    if mid_complement_tolerance is not None and price_source == "mid":
        both_mid = work["yes_mid"].notna() & work["no_mid"].notna()
        bad_complement = both_mid & ((work["yes_mid"] + work["no_mid"] - 1.0).abs() > mid_complement_tolerance)
        counts["bad_mid_complement"] = int((bad_complement & keep).sum())
        keep &= ~bad_complement
    else:
        counts["bad_mid_complement"] = 0
    out = work.loc[keep].copy()
    out["yes_price"] = out[yes_col]
    out["no_price"] = out[no_col]
    out["market_age_seconds"] = (out["quote_ts"] - out["market_start_ts"]).dt.total_seconds()
    out["remaining_seconds"] = (out["market_end_ts"] - out["quote_ts"]).dt.total_seconds()
    out = out.sort_values(["market_key", "quote_ts"]).reset_index(drop=True)
    counts["kept_rows"] = int(len(out))
    counts["dropped_rows"] = int(len(work) - len(out))
    return out, dict(counts)


def _side_terminal_candidate(
    side: pd.DataFrame,
    price_col: str,
    threshold: float,
    definition: str,
    *,
    min_later_share: float,
    tolerant_floor_offset: float,
    min_later_quotes: int,
) -> tuple[pd.Timestamp | None, str | None]:
    side = side.dropna(subset=[price_col, "quote_ts"]).sort_values("quote_ts")
    if side.empty:
        return None, "no_side_quotes"
    crosses = side[side[price_col] >= threshold]
    if crosses.empty:
        return None, "never_crossed"
    floor = threshold if definition == "strict" else threshold - tolerant_floor_offset
    for _, candidate in crosses.iterrows():
        later = side[side["quote_ts"] >= candidate["quote_ts"]]
        if len(later) < min_later_quotes:
            continue
        share = float((later[price_col] >= floor).mean())
        if definition == "strict" and share == 1.0:
            return candidate["quote_ts"], None
        if definition == "tolerant" and share >= min_later_share:
            return candidate["quote_ts"], None
    return None, "failed_persistence"


def terminal_conviction_distribution_market_row(
    market: pd.DataFrame,
    *,
    threshold: float,
    price_source: str,
    definition: str,
    min_later_share: float,
    tolerant_floor_offset: float,
    min_later_quotes: int,
    min_quality_quotes_per_market: int,
) -> dict[str, Any]:
    market = market.sort_values("quote_ts")
    yes_count = int(market["yes_price"].notna().sum())
    no_count = int(market["no_price"].notna().sum())
    row: dict[str, Any] = {
        "market_key": market["market_key"].iloc[0],
        "market_slug": market["market_slug"].iloc[0],
        "market_start_ts": market["market_start_ts"].iloc[0],
        "market_end_ts": market["market_end_ts"].iloc[0],
        "threshold": threshold,
        "price_source": price_source,
        "conviction_definition": definition,
        "quality_quote_count": int(len(market)),
        "side_quote_count_yes": yes_count,
        "side_quote_count_no": no_count,
        "first_quote_age_seconds": float(market["market_age_seconds"].min()) if len(market) else None,
        "last_quote_age_seconds": float(market["market_age_seconds"].max()) if len(market) else None,
        "max_yes_price": float(market["yes_price"].max()) if yes_count else None,
        "max_no_price": float(market["no_price"].max()) if no_count else None,
        "final_yes_price": float(market["yes_price"].dropna().iloc[-1]) if yes_count else None,
        "final_no_price": float(market["no_price"].dropna().iloc[-1]) if no_count else None,
        "reached_terminal_conviction": False,
        "convicted_side": None,
        "terminal_conviction_ts": pd.NaT,
        "conviction_market_age_seconds": None,
        "conviction_remaining_seconds": None,
        "notes": "",
    }
    if len(market) < min_quality_quotes_per_market:
        row["notes"] = "insufficient_quality_quotes"
        return row
    yes_ts, yes_note = _side_terminal_candidate(
        market,
        "yes_price",
        threshold,
        definition,
        min_later_share=min_later_share,
        tolerant_floor_offset=tolerant_floor_offset,
        min_later_quotes=min_later_quotes,
    )
    no_ts, no_note = _side_terminal_candidate(
        market,
        "no_price",
        threshold,
        definition,
        min_later_share=min_later_share,
        tolerant_floor_offset=tolerant_floor_offset,
        min_later_quotes=min_later_quotes,
    )
    candidates = [(side, ts) for side, ts in (("YES", yes_ts), ("NO", no_ts)) if ts is not None]
    if candidates:
        side, ts = min(candidates, key=lambda item: item[1])
        row["reached_terminal_conviction"] = True
        row["convicted_side"] = side
        row["terminal_conviction_ts"] = ts
        row["conviction_market_age_seconds"] = float((ts - row["market_start_ts"]).total_seconds())
        row["conviction_remaining_seconds"] = float((row["market_end_ts"] - ts).total_seconds())
    else:
        row["notes"] = ";".join(sorted({note for note in (yes_note, no_note) if note}))
    return row


def compute_terminal_conviction_distribution_rows(
    quotes: pd.DataFrame,
    *,
    thresholds: list[float],
    sources: list[str],
    definitions: list[str],
    disable_spread_filter: bool,
    max_spread: float | None,
    mid_complement_tolerance: float | None,
    min_later_share: float,
    tolerant_floor_offset: float,
    min_later_quotes: int,
    min_quality_quotes_per_market: int,
    max_post_end_lag_seconds: float | None = 0.0,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    rows: list[dict[str, Any]] = []
    quality_rows: list[dict[str, Any]] = []
    for source in sources:
        filtered, counts = filter_quotes_for_distribution(
            quotes,
            price_source=source,
            disable_spread_filter=disable_spread_filter,
            max_spread=max_spread,
            mid_complement_tolerance=mid_complement_tolerance,
            max_post_end_lag_seconds=max_post_end_lag_seconds,
        )
        quality_rows.append({"price_source": source, **counts})
        for _, market in filtered.groupby("market_key", dropna=False):
            for threshold in thresholds:
                for definition in definitions:
                    rows.append(
                        terminal_conviction_distribution_market_row(
                            market,
                            threshold=threshold,
                            price_source=source,
                            definition=definition,
                            min_later_share=min_later_share,
                            tolerant_floor_offset=tolerant_floor_offset,
                            min_later_quotes=min_later_quotes,
                            min_quality_quotes_per_market=min_quality_quotes_per_market,
                        )
                    )
    return pd.DataFrame(rows), pd.DataFrame(quality_rows)


def distribution_by_second(by_market: pd.DataFrame, market_window_seconds: int) -> pd.DataFrame:
    if by_market.empty:
        return pd.DataFrame()
    rows: list[dict[str, Any]] = []
    combos = ["threshold", "price_source", "conviction_definition"]
    for keys, group in by_market.groupby(combos, dropna=False):
        threshold, source, definition = keys
        markets_total = int(group["market_key"].nunique())
        sufficient = group[group["quality_quote_count"] > 0]
        sufficient_n = int(sufficient["market_key"].nunique())
        convicted_seconds = (
            group[group["reached_terminal_conviction"] == True]["conviction_market_age_seconds"]
            .dropna()
            .clip(lower=0, upper=market_window_seconds)
            .map(lambda value: int(math.floor(value)))
        )
        counts = convicted_seconds.value_counts().to_dict()
        cumulative = 0
        for second in range(0, market_window_seconds + 1):
            at_second = int(counts.get(second, 0))
            not_yet_start = max(sufficient_n - cumulative, 0)
            cumulative += at_second
            rows.append(
                {
                    "threshold": threshold,
                    "price_source": source,
                    "conviction_definition": definition,
                    "market_age_second": second,
                    "markets_total": markets_total,
                    "markets_with_sufficient_quotes": sufficient_n,
                    "first_convictions_at_second": at_second,
                    "first_conviction_share_of_total": at_second / markets_total if markets_total else None,
                    "first_conviction_share_of_sufficient": at_second / sufficient_n if sufficient_n else None,
                    "cumulative_convictions": cumulative,
                    "cumulative_conviction_share": cumulative / sufficient_n if sufficient_n else None,
                    "not_yet_convicted_count": max(sufficient_n - cumulative, 0),
                    "survival_share": max(sufficient_n - cumulative, 0) / sufficient_n if sufficient_n else None,
                    "hazard_rate": at_second / not_yet_start if not_yet_start else None,
                }
            )
    return pd.DataFrame(rows)


def distribution_binned(by_second: pd.DataFrame, bin_seconds_values: list[int]) -> pd.DataFrame:
    if by_second.empty:
        return pd.DataFrame()
    rows: list[dict[str, Any]] = []
    combos = ["threshold", "price_source", "conviction_definition"]
    for bin_seconds in bin_seconds_values:
        for keys, group in by_second.groupby(combos, dropna=False):
            threshold, source, definition = keys
            max_second = int(group["market_age_second"].max())
            for start in range(0, max_second + 1, bin_seconds):
                end = min(start + bin_seconds - 1, max_second)
                part = group[(group["market_age_second"] >= start) & (group["market_age_second"] <= end)]
                if part.empty:
                    continue
                first = int(part["first_convictions_at_second"].sum())
                first_row = part.iloc[0]
                last_row = part.iloc[-1]
                not_yet_start = int(first_row["not_yet_convicted_count"] + first_row["first_convictions_at_second"])
                rows.append(
                    {
                        "threshold": threshold,
                        "price_source": source,
                        "conviction_definition": definition,
                        "bin_seconds": bin_seconds,
                        "age_bin_start": start,
                        "age_bin_end": end,
                        "markets_total": int(last_row["markets_total"]),
                        "markets_with_sufficient_quotes": int(last_row["markets_with_sufficient_quotes"]),
                        "first_convictions_in_bin": first,
                        "cumulative_convictions_through_bin": int(last_row["cumulative_convictions"]),
                        "cumulative_conviction_share": float(last_row["cumulative_conviction_share"]) if pd.notna(last_row["cumulative_conviction_share"]) else None,
                        "survival_share": float(last_row["survival_share"]) if pd.notna(last_row["survival_share"]) else None,
                        "hazard_rate_in_bin": first / not_yet_start if not_yet_start else None,
                    }
                )
    return pd.DataFrame(rows)
