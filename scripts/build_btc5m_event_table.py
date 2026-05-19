#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import pandas as pd


JSONL_FILENAMES = [
    "chainlink_prices.jsonl",
    "binance_prices.jsonl",
    "market_quotes.jsonl",
    "market_meta.jsonl",
    "recorder_heartbeat.jsonl",
]
DEFAULT_SWEEP_METHODS = ("nearest", "previous")
DEFAULT_SWEEP_DISTANCES = (1.0, 2.0, 5.0, 10.0)


@dataclass(frozen=True)
class RecorderSource:
    path: Path
    source_kind: str  # "legacy_dir" or "segment_dir"
    files_present: tuple[str, ...]
    missing_files: tuple[str, ...]


@dataclass(frozen=True)
class SourceDiscoveryResult:
    sources: list[RecorderSource]
    diagnostics: dict[str, Any]


@dataclass(frozen=True)
class FeedLoadResult:
    raw_df: pd.DataFrame
    deduped_df: pd.DataFrame
    diagnostics: dict[str, Any]


def iter_jsonl_records(path: Path) -> Iterable[dict[str, Any]]:
    with path.open("r", encoding="utf-8") as f:
        for i, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                if i == sum(1 for _ in path.open("r", encoding="utf-8")):
                    return
                raise


def discover_sources(root: Path) -> SourceDiscoveryResult:
    if not root.exists():
        raise FileNotFoundError(root)

    included_sources: list[RecorderSource] = []
    diagnostics: dict[str, Any] = {
        "root": str(root),
        "included_legacy_dirs": 0,
        "included_segment_dirs": 0,
        "included_sources": [],
        "skipped_active_segment_dirs": 0,
        "skipped_unclean_segment_dirs": 0,
        "skipped_unreadable_manifest_dirs": 0,
        "segment_dirs_with_missing_files": 0,
        "file_missing_counts": {name: 0 for name in JSONL_FILENAMES},
        "skipped_details": [],
    }

    flat_present = tuple(name for name in JSONL_FILENAMES if (root / name).exists())
    flat_missing = tuple(name for name in JSONL_FILENAMES if name not in flat_present)
    if flat_present:
        source = RecorderSource(
            path=root,
            source_kind="legacy_dir",
            files_present=flat_present,
            missing_files=flat_missing,
        )
        included_sources.append(source)
        diagnostics["included_legacy_dirs"] = 1
        diagnostics["included_sources"].append(_source_detail(source))
        if flat_missing:
            diagnostics["segment_dirs_with_missing_files"] += 1
            for name in flat_missing:
                diagnostics["file_missing_counts"][name] += 1

    for day_dir in sorted([p for p in root.iterdir() if p.is_dir()]):
        for hour_dir in sorted([p for p in day_dir.iterdir() if p.is_dir()]):
            files_present = tuple(name for name in JSONL_FILENAMES if (hour_dir / name).exists())
            manifest_path = hour_dir / "segment_manifest.json"
            if not files_present and not manifest_path.exists():
                continue
            missing_files = tuple(name for name in JSONL_FILENAMES if name not in files_present)
            if not manifest_path.exists():
                diagnostics["skipped_active_segment_dirs"] += 1
                diagnostics["skipped_details"].append(
                    {
                        "path": str(hour_dir),
                        "reason": "active_segment_no_manifest",
                        "files_present": list(files_present),
                        "missing_files": list(missing_files),
                    }
                )
                continue
            try:
                manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            except Exception as exc:
                diagnostics["skipped_unreadable_manifest_dirs"] += 1
                diagnostics["skipped_details"].append(
                    {
                        "path": str(hour_dir),
                        "reason": "manifest_unreadable",
                        "error": str(exc),
                        "files_present": list(files_present),
                        "missing_files": list(missing_files),
                    }
                )
                continue
            if manifest.get("closed_cleanly") is not True:
                diagnostics["skipped_unclean_segment_dirs"] += 1
                diagnostics["skipped_details"].append(
                    {
                        "path": str(hour_dir),
                        "reason": "manifest_not_cleanly_closed",
                        "files_present": list(files_present),
                        "missing_files": list(missing_files),
                    }
                )
                continue
            source = RecorderSource(
                path=hour_dir,
                source_kind="segment_dir",
                files_present=files_present,
                missing_files=missing_files,
            )
            included_sources.append(source)
            diagnostics["included_segment_dirs"] += 1
            diagnostics["included_sources"].append(_source_detail(source))
            if missing_files:
                diagnostics["segment_dirs_with_missing_files"] += 1
                for name in missing_files:
                    diagnostics["file_missing_counts"][name] += 1

    return SourceDiscoveryResult(sources=included_sources, diagnostics=diagnostics)


def load_chainlink(paths: list[Path]) -> FeedLoadResult:
    rows: list[dict[str, Any]] = []
    for path in paths:
        for row in iter_jsonl_records(path):
            rows.append(
                {
                    "source_path": str(path),
                    "recv_ts": row.get("ts"),
                    "source_ts": row.get("source_ts"),
                    "price": row.get("price"),
                    "record_type": row.get("record_type"),
                    "message_family": row.get("message_family"),
                    "topic": row.get("topic"),
                    "warning": row.get("warning"),
                }
            )
    df = pd.DataFrame(rows)
    if df.empty:
        return FeedLoadResult(raw_df=_empty_feed_frame("source_time"), deduped_df=_empty_feed_frame("source_time"), diagnostics=_empty_feed_diagnostics("chainlink", paths))
    df["recv_time"] = pd.to_datetime(df["recv_ts"], utc=True, errors="coerce", format="ISO8601")
    df["source_time"] = pd.to_datetime(df["source_ts"], utc=True, errors="coerce", format="ISO8601")
    raw_df = df.dropna(subset=["source_time", "price"]).sort_values(["source_time", "recv_time"]).reset_index(drop=True)
    deduped_df = raw_df.groupby("source_time", as_index=False).last()
    diagnostics = {
        "feed": "chainlink",
        "paths": [str(path) for path in paths],
        "raw_row_count": int(len(df)),
        "parsed_timestamp_null_count": int(df["source_time"].isna().sum()),
        "price_null_count": int(df["price"].isna().sum()),
        "rows_after_parse_filter": int(len(raw_df)),
        "rows_after_dedupe": int(len(deduped_df)),
        "duplicate_rows_removed": int(len(raw_df) - len(deduped_df)),
        "duplicate_rows_by_key": int(raw_df.duplicated(subset=["source_time"], keep="last").sum()),
        "min_time": _series_timestamp_min(raw_df["source_time"]),
        "max_time": _series_timestamp_max(raw_df["source_time"]),
    }
    return FeedLoadResult(raw_df=raw_df, deduped_df=deduped_df, diagnostics=diagnostics)


def load_binance(paths: list[Path]) -> FeedLoadResult:
    rows: list[dict[str, Any]] = []
    for path in paths:
        for row in iter_jsonl_records(path):
            rows.append(
                {
                    "source_path": str(path),
                    "recv_ts": row.get("ts"),
                    "observed_at": row.get("observed_at"),
                    "price": row.get("price"),
                    "source": row.get("source"),
                    "record_type": row.get("record_type"),
                    "warning": row.get("warning"),
                }
            )
    df = pd.DataFrame(rows)
    if df.empty:
        return FeedLoadResult(raw_df=_empty_feed_frame("event_time"), deduped_df=_empty_feed_frame("event_time"), diagnostics=_empty_feed_diagnostics("binance", paths))
    df["recv_time"] = pd.to_datetime(df["recv_ts"], utc=True, errors="coerce", format="ISO8601")
    df["event_time"] = pd.to_datetime(df["observed_at"], utc=True, errors="coerce", unit="ms")
    raw_df = df.dropna(subset=["event_time", "price"]).sort_values(["event_time", "recv_time"]).reset_index(drop=True)
    deduped_df = raw_df.groupby("event_time", as_index=False).last()
    diagnostics = {
        "feed": "binance",
        "paths": [str(path) for path in paths],
        "raw_row_count": int(len(df)),
        "parsed_timestamp_null_count": int(df["event_time"].isna().sum()),
        "price_null_count": int(df["price"].isna().sum()),
        "rows_after_parse_filter": int(len(raw_df)),
        "rows_after_dedupe": int(len(deduped_df)),
        "duplicate_rows_removed": int(len(raw_df) - len(deduped_df)),
        "duplicate_rows_by_key": int(raw_df.duplicated(subset=["event_time"], keep="last").sum()),
        "min_time": _series_timestamp_min(raw_df["event_time"]),
        "max_time": _series_timestamp_max(raw_df["event_time"]),
        "observed_at_digit_lengths": _digit_length_histogram(df["observed_at"]),
    }
    return FeedLoadResult(raw_df=raw_df, deduped_df=deduped_df, diagnostics=diagnostics)


def load_quotes(paths: list[Path]) -> FeedLoadResult:
    rows: list[dict[str, Any]] = []
    for path in paths:
        for row in iter_jsonl_records(path):
            yes = row.get("yes") or {}
            no = row.get("no") or {}
            rows.append(
                {
                    "source_path": str(path),
                    "ts": row.get("ts"),
                    "record_type": row.get("record_type"),
                    "slug": row.get("slug"),
                    "market_id": row.get("market_id"),
                    "market_start_time": row.get("market_start_time"),
                    "market_end_time": row.get("market_end_time"),
                    "quote_capture_ok": row.get("quote_capture_ok"),
                    "quote_capture_status": row.get("quote_capture_status"),
                    "yes_fetch_ok": yes.get("fetch_ok"),
                    "no_fetch_ok": no.get("fetch_ok"),
                    "yes_best_bid": yes.get("best_bid"),
                    "yes_best_ask": yes.get("best_ask"),
                    "yes_mid": yes.get("mid"),
                    "yes_spread": yes.get("spread"),
                    "no_best_bid": no.get("best_bid"),
                    "no_best_ask": no.get("best_ask"),
                    "no_mid": no.get("mid"),
                    "no_spread": no.get("spread"),
                    "yes_error": yes.get("error"),
                    "no_error": no.get("error"),
                }
            )
    df = pd.DataFrame(rows)
    if df.empty:
        return FeedLoadResult(raw_df=pd.DataFrame(), deduped_df=pd.DataFrame(), diagnostics=_empty_feed_diagnostics("quotes", paths))
    df["ts_dt"] = pd.to_datetime(df["ts"], utc=True, errors="coerce", format="ISO8601")
    df["market_start"] = pd.to_datetime(df["market_start_time"], utc=True, errors="coerce", format="ISO8601")
    df["market_end"] = pd.to_datetime(df["market_end_time"], utc=True, errors="coerce", format="ISO8601")
    df["quote_capture_ok"] = _infer_quote_capture_ok(df)
    df["quote_capture_status"] = _infer_quote_capture_status(df)
    raw_df = df.sort_values(["ts_dt"]).reset_index(drop=True)
    deduped_df = raw_df.drop_duplicates(subset=["ts_dt", "slug", "market_id", "record_type"], keep="last")
    diagnostics = {
        "feed": "quotes",
        "paths": [str(path) for path in paths],
        "raw_row_count": int(len(df)),
        "parsed_timestamp_null_count": int(df["ts_dt"].isna().sum()),
        "rows_after_parse_filter": int(len(raw_df)),
        "rows_after_dedupe": int(len(deduped_df)),
        "duplicate_rows_removed": int(len(raw_df) - len(deduped_df)),
        "duplicate_rows_by_key": int(raw_df.duplicated(subset=["ts_dt", "slug", "market_id", "record_type"], keep="last").sum()),
        "min_time": _series_timestamp_min(raw_df["ts_dt"]),
        "max_time": _series_timestamp_max(raw_df["ts_dt"]),
    }
    return FeedLoadResult(raw_df=raw_df, deduped_df=deduped_df, diagnostics=diagnostics)


def load_meta(paths: list[Path]) -> FeedLoadResult:
    rows: list[dict[str, Any]] = []
    for path in paths:
        for row in iter_jsonl_records(path):
            market = row.get("market") or {}
            rows.append(
                {
                    "source_path": str(path),
                    "ts": row.get("ts"),
                    "ts_dt": pd.to_datetime(row.get("ts"), utc=True, errors="coerce", format="ISO8601"),
                    "record_type": row.get("record_type"),
                    "market_changed": bool(row.get("market_changed")),
                    "slug": market.get("slug"),
                    "market_id": market.get("market_id"),
                    "market_start": pd.to_datetime(market.get("start_time"), utc=True, errors="coerce", format="ISO8601"),
                    "market_end": pd.to_datetime(market.get("end_time"), utc=True, errors="coerce", format="ISO8601"),
                }
            )
    df = pd.DataFrame(rows)
    if df.empty:
        return FeedLoadResult(raw_df=pd.DataFrame(), deduped_df=pd.DataFrame(), diagnostics=_empty_feed_diagnostics("meta", paths))
    raw_df = df.sort_values(["ts_dt"]).reset_index(drop=True)
    deduped_df = raw_df.drop_duplicates(subset=["ts_dt", "slug", "market_id", "record_type"], keep="last")
    diagnostics = {
        "feed": "meta",
        "paths": [str(path) for path in paths],
        "raw_row_count": int(len(df)),
        "parsed_timestamp_null_count": int(df["ts_dt"].isna().sum()),
        "rows_after_parse_filter": int(len(raw_df)),
        "rows_after_dedupe": int(len(deduped_df)),
        "duplicate_rows_removed": int(len(raw_df) - len(deduped_df)),
        "duplicate_rows_by_key": int(raw_df.duplicated(subset=["ts_dt", "slug", "market_id", "record_type"], keep="last").sum()),
        "min_time": _series_timestamp_min(raw_df["ts_dt"]),
        "max_time": _series_timestamp_max(raw_df["ts_dt"]),
    }
    return FeedLoadResult(raw_df=raw_df, deduped_df=deduped_df, diagnostics=diagnostics)


def sign_label(value: float | None) -> str | None:
    if value is None or pd.isna(value):
        return None
    if value > 0:
        return "UP"
    if value < 0:
        return "DOWN"
    return "FLAT"


def build_windows(quotes: pd.DataFrame, meta: pd.DataFrame) -> pd.DataFrame:
    q = pd.DataFrame(columns=["slug", "market_id", "market_start", "market_end"])
    m = pd.DataFrame(columns=["slug", "market_id", "market_start", "market_end"])
    if not quotes.empty:
        q = quotes.loc[
            quotes["record_type"] == "quote_snapshot",
            ["slug", "market_id", "market_start", "market_end"],
        ].dropna(subset=["slug", "market_start", "market_end"])
    if not meta.empty:
        m = meta[["slug", "market_id", "market_start", "market_end"]].dropna(subset=["slug", "market_start", "market_end"])
    windows = pd.concat([q, m], ignore_index=True).drop_duplicates()
    if windows.empty:
        return pd.DataFrame(columns=["slug", "market_id", "market_start", "market_end"])
    return windows.sort_values(["market_start", "slug", "market_id"]).reset_index(drop=True)


def summarize_quotes(quotes: pd.DataFrame, slug: str, start: pd.Timestamp, end: pd.Timestamp) -> dict[str, Any]:
    quote_rows = quotes[
        (quotes["record_type"] == "quote_snapshot")
        & (quotes["slug"] == slug)
        & (quotes["market_start"] == start)
        & (quotes["market_end"] == end)
    ]
    if quote_rows.empty:
        return {
            "quote_rows": 0,
            "quote_capture_ok_rows": 0,
            "quote_capture_failed_rows": 0,
            "quote_two_sided_rows": 0,
            "quote_one_sided_rows": 0,
            "quote_mid_extreme_rows": 0,
            "quote_first_seen": None,
            "quote_last_seen": None,
            "quote_yes_mid_median": None,
            "quote_no_mid_median": None,
            "quote_yes_spread_median": None,
            "quote_no_spread_median": None,
        }
    two_sided = (
        quote_rows["yes_best_bid"].notna() & quote_rows["yes_best_ask"].notna()
        & quote_rows["no_best_bid"].notna() & quote_rows["no_best_ask"].notna()
    )
    one_sided = (
        (quote_rows["yes_best_bid"].notna() ^ quote_rows["yes_best_ask"].notna())
        | (quote_rows["no_best_bid"].notna() ^ quote_rows["no_best_ask"].notna())
    )
    mid_extreme = (
        (quote_rows["yes_mid"].fillna(-1) >= 0.9) | (quote_rows["yes_mid"].fillna(2) <= 0.1)
        | (quote_rows["no_mid"].fillna(-1) >= 0.9) | (quote_rows["no_mid"].fillna(2) <= 0.1)
    )
    return {
        "quote_rows": int(len(quote_rows)),
        "quote_capture_ok_rows": int(quote_rows["quote_capture_ok"].fillna(False).sum()),
        "quote_capture_failed_rows": int((quote_rows["quote_capture_status"] == "failed").sum()),
        "quote_two_sided_rows": int(two_sided.sum()),
        "quote_one_sided_rows": int(one_sided.sum()),
        "quote_mid_extreme_rows": int(mid_extreme.sum()),
        "quote_first_seen": quote_rows["ts_dt"].min(),
        "quote_last_seen": quote_rows["ts_dt"].max(),
        "quote_yes_mid_median": None if quote_rows["yes_mid"].dropna().empty else float(quote_rows["yes_mid"].median()),
        "quote_no_mid_median": None if quote_rows["no_mid"].dropna().empty else float(quote_rows["no_mid"].median()),
        "quote_yes_spread_median": None if quote_rows["yes_spread"].dropna().empty else float(quote_rows["yes_spread"].median()),
        "quote_no_spread_median": None if quote_rows["no_spread"].dropna().empty else float(quote_rows["no_spread"].median()),
    }


def summarize_meta(meta: pd.DataFrame, slug: str, start: pd.Timestamp, end: pd.Timestamp) -> dict[str, Any]:
    meta_rows = meta[
        (meta["slug"] == slug)
        & (meta["market_start"] == start)
        & (meta["market_end"] == end)
    ]
    if meta_rows.empty:
        return {
            "route_rows": 0,
            "route_market_changed_rows": 0,
            "route_first_seen": None,
            "route_last_seen": None,
            "route_unique_market_ids": 0,
        }
    return {
        "route_rows": int(len(meta_rows)),
        "route_market_changed_rows": int(meta_rows["market_changed"].sum()),
        "route_first_seen": meta_rows["ts_dt"].min(),
        "route_last_seen": meta_rows["ts_dt"].max(),
        "route_unique_market_ids": int(meta_rows["market_id"].nunique()),
    }


def build_event_table(
    input_roots: list[Path],
    *,
    boundary_method: str,
    max_boundary_distance_seconds: float | None,
    diagnostic_buffer_seconds: float = 10.0,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    source_discoveries = [discover_sources(root) for root in input_roots]
    sources = [source for discovery in source_discoveries for source in discovery.sources]
    if not sources:
        raise RuntimeError("No recorder sources found.")

    chain_paths = [source.path / "chainlink_prices.jsonl" for source in sources if (source.path / "chainlink_prices.jsonl").exists()]
    binance_paths = [source.path / "binance_prices.jsonl" for source in sources if (source.path / "binance_prices.jsonl").exists()]
    quote_paths = [source.path / "market_quotes.jsonl" for source in sources if (source.path / "market_quotes.jsonl").exists()]
    meta_paths = [source.path / "market_meta.jsonl" for source in sources if (source.path / "market_meta.jsonl").exists()]

    chain = load_chainlink(chain_paths)
    binance = load_binance(binance_paths)
    quotes = load_quotes(quote_paths)
    meta = load_meta(meta_paths)
    windows = build_windows(quotes.deduped_df, meta.deduped_df)

    out_rows: list[dict[str, Any]] = []
    for _, window in windows.iterrows():
        row: dict[str, Any] = {
            "slug": window["slug"],
            "market_id": window["market_id"],
            "market_start_time": window["market_start"],
            "market_end_time": window["market_end"],
        }

        chain_diag = feed_event_diagnostics(
            chain,
            time_col="source_time",
            price_col="price",
            start=window["market_start"],
            end=window["market_end"],
            method=boundary_method,
            max_dist_sec=max_boundary_distance_seconds,
            diagnostic_buffer_seconds=diagnostic_buffer_seconds,
        )
        binance_diag = feed_event_diagnostics(
            binance,
            time_col="event_time",
            price_col="price",
            start=window["market_start"],
            end=window["market_end"],
            method=boundary_method,
            max_dist_sec=max_boundary_distance_seconds,
            diagnostic_buffer_seconds=diagnostic_buffer_seconds,
        )
        row.update(_prefix_boundary_diagnostics("chainlink", chain_diag))
        row.update(_prefix_boundary_diagnostics("binance", binance_diag))

        chain_open = chain_diag["open"]
        chain_close = chain_diag["close"]
        binance_open = binance_diag["open"]
        binance_close = binance_diag["close"]
        row["chainlink_open_price"] = chain_open["chosen_price"]
        row["chainlink_open_ts"] = chain_open["chosen_ts"]
        row["chainlink_open_distance_sec"] = chain_open["chosen_distance_sec"]
        row["chainlink_close_price"] = chain_close["chosen_price"]
        row["chainlink_close_ts"] = chain_close["chosen_ts"]
        row["chainlink_close_distance_sec"] = chain_close["chosen_distance_sec"]
        row["binance_open_price"] = binance_open["chosen_price"]
        row["binance_open_ts"] = binance_open["chosen_ts"]
        row["binance_open_distance_sec"] = binance_open["chosen_distance_sec"]
        row["binance_close_price"] = binance_close["chosen_price"]
        row["binance_close_ts"] = binance_close["chosen_ts"]
        row["binance_close_distance_sec"] = binance_close["chosen_distance_sec"]

        c_open = row["chainlink_open_price"]
        c_close = row["chainlink_close_price"]
        b_open = row["binance_open_price"]
        b_close = row["binance_close_price"]
        row["chainlink_return"] = None if c_open is None or c_close is None else float(c_close - c_open)
        row["binance_return"] = None if b_open is None or b_close is None else float(b_close - b_open)
        row["chainlink_abs_return"] = None if row["chainlink_return"] is None else abs(row["chainlink_return"])
        row["binance_abs_return"] = None if row["binance_return"] is None else abs(row["binance_return"])
        row["chainlink_label"] = sign_label(row["chainlink_return"])
        row["binance_label"] = sign_label(row["binance_return"])
        row["proxy_agrees_with_chainlink"] = (
            None if row["chainlink_label"] is None or row["binance_label"] is None else row["chainlink_label"] == row["binance_label"]
        )
        row["abs_move_diff"] = (
            None if row["chainlink_return"] is None or row["binance_return"] is None else abs(row["chainlink_return"] - row["binance_return"])
        )
        row["complete_chainlink_label"] = row["chainlink_label"] is not None
        row["complete_binance_label"] = row["binance_label"] is not None
        row["proxy_comparable"] = row["complete_chainlink_label"] and row["complete_binance_label"]
        row.update(summarize_quotes(quotes.deduped_df, window["slug"], window["market_start"], window["market_end"]))
        row.update(summarize_meta(meta.deduped_df, window["slug"], window["market_start"], window["market_end"]))
        out_rows.append(row)

    events = pd.DataFrame(out_rows)
    if not events.empty:
        events = events.sort_values(["market_start_time", "slug"]).reset_index(drop=True)

    summary: dict[str, Any] = {
        "source_roots": [str(root) for root in input_roots],
        "discovered_sources": [str(source.path) for source in sources],
        "discovered_source_count": len(sources),
        "boundary_method": boundary_method,
        "max_boundary_distance_seconds": max_boundary_distance_seconds,
        "diagnostic_buffer_seconds": diagnostic_buffer_seconds,
        "source_discovery": _combine_source_discovery(source_discoveries),
        "feed_diagnostics": {
            "chainlink": chain.diagnostics,
            "binance": binance.diagnostics,
            "quotes": quotes.diagnostics,
            "meta": meta.diagnostics,
        },
        "event_rows": int(len(events)),
        "chainlink_complete_rows": int(events["complete_chainlink_label"].sum()) if not events.empty else 0,
        "binance_complete_rows": int(events["complete_binance_label"].sum()) if not events.empty else 0,
        "proxy_comparable_rows": int(events["proxy_comparable"].sum()) if not events.empty else 0,
        "binance_missing_given_chainlink_complete_rows": int(
            ((events["complete_chainlink_label"]) & (~events["complete_binance_label"])).sum()
        ) if not events.empty else 0,
    }

    if not events.empty:
        comparable = events[events["proxy_comparable"]].copy()
        summary["proxy_disagreement_rows"] = int((comparable["proxy_agrees_with_chainlink"] == False).sum())
        summary["proxy_disagreement_rate"] = None if comparable.empty else float((comparable["proxy_agrees_with_chainlink"] == False).mean())
        nonzero_chain = comparable[comparable["chainlink_label"] != "FLAT"]
        summary["proxy_disagreement_rate_nonflat_chainlink"] = (
            None if nonzero_chain.empty else float((nonzero_chain["proxy_agrees_with_chainlink"] == False).mean())
        )
        nz_abs = comparable["chainlink_abs_return"].dropna()
        nz_abs = nz_abs[nz_abs > 0]
        tiny_threshold = None if nz_abs.empty else float(nz_abs.median())
        summary["tiny_move_threshold_abs_return"] = tiny_threshold
        if tiny_threshold is not None:
            tiny = comparable[comparable["chainlink_abs_return"] <= tiny_threshold]
            non_tiny = comparable[comparable["chainlink_abs_return"] > tiny_threshold]
            summary["proxy_disagreement_rate_tiny_moves"] = None if tiny.empty else float((tiny["proxy_agrees_with_chainlink"] == False).mean())
            summary["proxy_disagreement_rate_non_tiny_moves"] = None if non_tiny.empty else float((non_tiny["proxy_agrees_with_chainlink"] == False).mean())
        summary["quote_capture_ok_event_rate"] = float((events["quote_capture_ok_rows"] > 0).mean())
        summary["quote_two_sided_event_rate"] = float((events["quote_two_sided_rows"] > 0).mean())
        summary["quote_one_sided_event_rate"] = float((events["quote_one_sided_rows"] > 0).mean())
        summary["quote_mid_extreme_event_rate"] = float((events["quote_mid_extreme_rows"] > 0).mean())

    return events, summary


def build_boundary_sweep(
    input_roots: list[Path],
    *,
    methods: Iterable[str] = DEFAULT_SWEEP_METHODS,
    max_boundary_distances: Iterable[float] = DEFAULT_SWEEP_DISTANCES,
    diagnostic_buffer_seconds: float = 10.0,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for method in methods:
        for distance in max_boundary_distances:
            _, summary = build_event_table(
                input_roots,
                boundary_method=method,
                max_boundary_distance_seconds=float(distance),
                diagnostic_buffer_seconds=diagnostic_buffer_seconds,
            )
            rows.append(
                {
                    "boundary_method": method,
                    "max_boundary_distance_seconds": float(distance),
                    "event_rows": summary["event_rows"],
                    "chainlink_complete_rows": summary["chainlink_complete_rows"],
                    "binance_complete_rows": summary["binance_complete_rows"],
                    "proxy_comparable_rows": summary["proxy_comparable_rows"],
                    "proxy_disagreement_rows": summary.get("proxy_disagreement_rows", 0),
                    "proxy_disagreement_rate": summary.get("proxy_disagreement_rate"),
                    "proxy_disagreement_rate_tiny_moves": summary.get("proxy_disagreement_rate_tiny_moves"),
                    "proxy_disagreement_rate_non_tiny_moves": summary.get("proxy_disagreement_rate_non_tiny_moves"),
                }
            )
    return pd.DataFrame(rows).sort_values(["boundary_method", "max_boundary_distance_seconds"]).reset_index(drop=True)


def feed_event_diagnostics(
    feed: FeedLoadResult,
    *,
    time_col: str,
    price_col: str,
    start: pd.Timestamp,
    end: pd.Timestamp,
    method: str,
    max_dist_sec: float | None,
    diagnostic_buffer_seconds: float,
) -> dict[str, Any]:
    raw_df = feed.raw_df
    deduped_df = feed.deduped_df
    buffer_delta = pd.Timedelta(seconds=diagnostic_buffer_seconds)
    raw_window_count = _count_rows_between(raw_df, time_col, start, end, left_inclusive=True, right_inclusive=True)
    raw_before_open_count = _count_rows_between(raw_df, time_col, start - buffer_delta, start, left_inclusive=True, right_inclusive=False)
    raw_after_close_count = _count_rows_between(raw_df, time_col, end, end + buffer_delta, left_inclusive=False, right_inclusive=True)
    overall_first = _series_value_or_none(deduped_df[time_col].min()) if not deduped_df.empty else None
    overall_last = _series_value_or_none(deduped_df[time_col].max()) if not deduped_df.empty else None
    open_diag = _boundary_selection_diagnostics(
        raw_df=raw_df,
        deduped_df=deduped_df,
        time_col=time_col,
        price_col=price_col,
        target=start,
        method=method,
        max_dist_sec=max_dist_sec,
        diagnostic_buffer_seconds=diagnostic_buffer_seconds,
        feed_diagnostics=feed.diagnostics,
        overall_first=overall_first,
        overall_last=overall_last,
    )
    close_diag = _boundary_selection_diagnostics(
        raw_df=raw_df,
        deduped_df=deduped_df,
        time_col=time_col,
        price_col=price_col,
        target=end,
        method=method,
        max_dist_sec=max_dist_sec,
        diagnostic_buffer_seconds=diagnostic_buffer_seconds,
        feed_diagnostics=feed.diagnostics,
        overall_first=overall_first,
        overall_last=overall_last,
    )
    return {
        "rows_in_window": raw_window_count,
        "rows_in_buffer_before_open": raw_before_open_count,
        "rows_in_buffer_after_close": raw_after_close_count,
        "open": open_diag,
        "close": close_diag,
    }


def _boundary_selection_diagnostics(
    *,
    raw_df: pd.DataFrame,
    deduped_df: pd.DataFrame,
    time_col: str,
    price_col: str,
    target: pd.Timestamp,
    method: str,
    max_dist_sec: float | None,
    diagnostic_buffer_seconds: float,
    feed_diagnostics: dict[str, Any],
    overall_first: pd.Timestamp | None,
    overall_last: pd.Timestamp | None,
) -> dict[str, Any]:
    raw_candidates = _candidate_context(raw_df, time_col, price_col, target)
    deduped_candidates = _candidate_context(deduped_df, time_col, price_col, target)
    chosen = _choose_candidate(deduped_candidates, method, max_dist_sec)
    raw_rows_near = _count_rows_between(
        raw_df,
        time_col,
        target - pd.Timedelta(seconds=diagnostic_buffer_seconds),
        target + pd.Timedelta(seconds=diagnostic_buffer_seconds),
        left_inclusive=True,
        right_inclusive=True,
    )
    deduped_rows_near = _count_rows_between(
        deduped_df,
        time_col,
        target - pd.Timedelta(seconds=diagnostic_buffer_seconds),
        target + pd.Timedelta(seconds=diagnostic_buffer_seconds),
        left_inclusive=True,
        right_inclusive=True,
    )
    missing_reason = _classify_missing_reason(
        chosen_available=chosen["chosen_ts"] is not None,
        nearest_distance_sec=deduped_candidates["nearest_distance_sec"],
        raw_nearest_distance_sec=raw_candidates["nearest_distance_sec"],
        max_dist_sec=max_dist_sec,
        raw_rows_near=raw_rows_near,
        deduped_rows_near=deduped_rows_near,
        overall_first=overall_first,
        overall_last=overall_last,
        target=target,
        parsed_timestamp_null_count=feed_diagnostics.get("parsed_timestamp_null_count", 0),
        rows_after_parse_filter=feed_diagnostics.get("rows_after_parse_filter", 0),
        raw_row_count=feed_diagnostics.get("raw_row_count", 0),
    )
    return {
        **chosen,
        "available": chosen["chosen_ts"] is not None,
        "missing_reason": missing_reason,
        "nearest_ts": deduped_candidates["nearest_ts"],
        "nearest_distance_sec": deduped_candidates["nearest_distance_sec"],
        "previous_candidate_ts": deduped_candidates["previous_ts"],
        "previous_candidate_distance_sec": deduped_candidates["previous_distance_sec"],
        "next_candidate_ts": deduped_candidates["next_ts"],
        "next_candidate_distance_sec": deduped_candidates["next_distance_sec"],
    }


def _candidate_context(df: pd.DataFrame, time_col: str, price_col: str, target: pd.Timestamp) -> dict[str, Any]:
    if df.empty:
        return {
            "previous_ts": None,
            "previous_distance_sec": None,
            "previous_price": None,
            "next_ts": None,
            "next_distance_sec": None,
            "next_price": None,
            "nearest_ts": None,
            "nearest_distance_sec": None,
            "nearest_price": None,
        }
    times = df[time_col]
    pos = int(times.searchsorted(target))
    previous = _candidate_at(df, time_col, price_col, target, pos - 1)
    nxt = _candidate_at(df, time_col, price_col, target, pos)
    nearest = _nearest_candidate(previous, nxt)
    return {
        "previous_ts": previous["ts"],
        "previous_distance_sec": previous["distance_sec"],
        "previous_price": previous["price"],
        "next_ts": nxt["ts"],
        "next_distance_sec": nxt["distance_sec"],
        "next_price": nxt["price"],
        "nearest_ts": nearest["ts"],
        "nearest_distance_sec": nearest["distance_sec"],
        "nearest_price": nearest["price"],
    }


def _choose_candidate(candidate_context: dict[str, Any], method: str, max_dist_sec: float | None) -> dict[str, Any]:
    if method == "previous":
        chosen_ts = candidate_context["previous_ts"]
        chosen_distance = candidate_context["previous_distance_sec"]
        chosen_price = candidate_context["previous_price"]
    elif method == "nearest":
        chosen_ts = candidate_context["nearest_ts"]
        chosen_distance = candidate_context["nearest_distance_sec"]
        chosen_price = candidate_context["nearest_price"]
    else:
        raise ValueError(f"unsupported method: {method}")
    if chosen_ts is None:
        return {"chosen_ts": None, "chosen_distance_sec": None, "chosen_price": None}
    if max_dist_sec is not None and chosen_distance is not None and chosen_distance > max_dist_sec:
        return {"chosen_ts": None, "chosen_distance_sec": None, "chosen_price": None}
    return {
        "chosen_ts": chosen_ts,
        "chosen_distance_sec": chosen_distance,
        "chosen_price": chosen_price,
    }


def _classify_missing_reason(
    *,
    chosen_available: bool,
    nearest_distance_sec: float | None,
    raw_nearest_distance_sec: float | None,
    max_dist_sec: float | None,
    raw_rows_near: int,
    deduped_rows_near: int,
    overall_first: pd.Timestamp | None,
    overall_last: pd.Timestamp | None,
    target: pd.Timestamp,
    parsed_timestamp_null_count: int,
    rows_after_parse_filter: int,
    raw_row_count: int,
) -> str:
    if chosen_available:
        return "available"
    if raw_row_count > 0 and rows_after_parse_filter == 0 and parsed_timestamp_null_count > 0:
        return "timestamp_parse_failed"
    if overall_first is not None and target < overall_first:
        return "boundary_before_first_observation"
    if overall_last is not None and target > overall_last:
        return "boundary_after_last_observation"
    if raw_rows_near == 0:
        return "no_rows_anywhere_near_boundary"
    if (
        max_dist_sec is not None
        and nearest_distance_sec is not None
        and nearest_distance_sec > max_dist_sec
        and raw_nearest_distance_sec is not None
        and raw_nearest_distance_sec <= max_dist_sec
        and raw_rows_near > deduped_rows_near
    ):
        return "deduped_away"
    if max_dist_sec is not None and nearest_distance_sec is not None and nearest_distance_sec > max_dist_sec:
        return "nearest_row_too_far"
    return "unknown"


def _prefix_boundary_diagnostics(prefix: str, diagnostics: dict[str, Any]) -> dict[str, Any]:
    open_diag = diagnostics["open"]
    close_diag = diagnostics["close"]
    return {
        f"{prefix}_rows_in_window": diagnostics["rows_in_window"],
        f"{prefix}_rows_in_buffer_before_open": diagnostics["rows_in_buffer_before_open"],
        f"{prefix}_rows_in_buffer_after_close": diagnostics["rows_in_buffer_after_close"],
        f"{prefix}_open_available": open_diag["available"],
        f"{prefix}_close_available": close_diag["available"],
        f"{prefix}_open_missing_reason": open_diag["missing_reason"],
        f"{prefix}_close_missing_reason": close_diag["missing_reason"],
        f"{prefix}_open_nearest_ts": open_diag["nearest_ts"],
        f"{prefix}_close_nearest_ts": close_diag["nearest_ts"],
        f"{prefix}_open_nearest_distance_sec": open_diag["nearest_distance_sec"],
        f"{prefix}_close_nearest_distance_sec": close_diag["nearest_distance_sec"],
        f"{prefix}_open_previous_candidate_ts": open_diag["previous_candidate_ts"],
        f"{prefix}_close_previous_candidate_ts": close_diag["previous_candidate_ts"],
        f"{prefix}_open_previous_candidate_distance_sec": open_diag["previous_candidate_distance_sec"],
        f"{prefix}_close_previous_candidate_distance_sec": close_diag["previous_candidate_distance_sec"],
        f"{prefix}_open_next_candidate_ts": open_diag["next_candidate_ts"],
        f"{prefix}_close_next_candidate_ts": close_diag["next_candidate_ts"],
        f"{prefix}_open_next_candidate_distance_sec": open_diag["next_candidate_distance_sec"],
        f"{prefix}_close_next_candidate_distance_sec": close_diag["next_candidate_distance_sec"],
    }


def _infer_quote_capture_ok(df: pd.DataFrame) -> pd.Series:
    explicit = df["quote_capture_ok"]
    if explicit.notna().any():
        return explicit.fillna(False).astype(bool)
    return (~df["yes_error"].notna()) & (~df["no_error"].notna())


def _infer_quote_capture_status(df: pd.DataFrame) -> pd.Series:
    explicit = df["quote_capture_status"]
    if explicit.notna().any():
        return explicit.fillna("unknown")
    failed = df["yes_error"].notna() & df["no_error"].notna()
    partial = df["yes_error"].notna() ^ df["no_error"].notna()
    ok = (~df["yes_error"].notna()) & (~df["no_error"].notna())
    return pd.Series(
        [
            "failed" if is_failed else "partial_failure" if is_partial else "ok" if is_ok else "unknown"
            for is_failed, is_partial, is_ok in zip(failed, partial, ok, strict=False)
        ],
        index=df.index,
    )


def _candidate_at(df: pd.DataFrame, time_col: str, price_col: str, target: pd.Timestamp, idx: int) -> dict[str, Any]:
    if not 0 <= idx < len(df):
        return {"ts": None, "distance_sec": None, "price": None}
    row = df.iloc[idx]
    timestamp = row[time_col]
    return {
        "ts": timestamp,
        "distance_sec": abs((timestamp - target).total_seconds()),
        "price": float(row[price_col]),
    }


def _nearest_candidate(previous: dict[str, Any], nxt: dict[str, Any]) -> dict[str, Any]:
    candidates = [candidate for candidate in (previous, nxt) if candidate["ts"] is not None]
    if not candidates:
        return {"ts": None, "distance_sec": None, "price": None}
    return min(candidates, key=lambda candidate: candidate["distance_sec"])


def _count_rows_between(
    df: pd.DataFrame,
    time_col: str,
    start: pd.Timestamp,
    end: pd.Timestamp,
    *,
    left_inclusive: bool,
    right_inclusive: bool,
) -> int:
    if df.empty:
        return 0
    left_mask = df[time_col] >= start if left_inclusive else df[time_col] > start
    right_mask = df[time_col] <= end if right_inclusive else df[time_col] < end
    return int((left_mask & right_mask).sum())


def _digit_length_histogram(series: pd.Series) -> dict[str, int]:
    counts: dict[str, int] = {}
    for value in series.dropna():
        digits = "".join(ch for ch in str(value) if ch.isdigit())
        key = str(len(digits))
        counts[key] = counts.get(key, 0) + 1
    return counts


def _series_timestamp_min(series: pd.Series) -> str | None:
    if series.empty or series.dropna().empty:
        return None
    return str(series.min())


def _series_timestamp_max(series: pd.Series) -> str | None:
    if series.empty or series.dropna().empty:
        return None
    return str(series.max())


def _series_value_or_none(value: Any) -> Any:
    return None if pd.isna(value) else value


def _source_detail(source: RecorderSource) -> dict[str, Any]:
    return {
        "path": str(source.path),
        "source_kind": source.source_kind,
        "files_present": list(source.files_present),
        "missing_files": list(source.missing_files),
    }


def _combine_source_discovery(discoveries: list[SourceDiscoveryResult]) -> dict[str, Any]:
    combined = {
        "roots": [discovery.diagnostics for discovery in discoveries],
        "included_legacy_dirs": sum(discovery.diagnostics["included_legacy_dirs"] for discovery in discoveries),
        "included_segment_dirs": sum(discovery.diagnostics["included_segment_dirs"] for discovery in discoveries),
        "skipped_active_segment_dirs": sum(discovery.diagnostics["skipped_active_segment_dirs"] for discovery in discoveries),
        "skipped_unclean_segment_dirs": sum(discovery.diagnostics["skipped_unclean_segment_dirs"] for discovery in discoveries),
        "skipped_unreadable_manifest_dirs": sum(discovery.diagnostics["skipped_unreadable_manifest_dirs"] for discovery in discoveries),
        "segment_dirs_with_missing_files": sum(discovery.diagnostics["segment_dirs_with_missing_files"] for discovery in discoveries),
        "file_missing_counts": {name: 0 for name in JSONL_FILENAMES},
    }
    for discovery in discoveries:
        for name, count in discovery.diagnostics["file_missing_counts"].items():
            combined["file_missing_counts"][name] += count
    return combined


def _empty_feed_frame(time_col: str) -> pd.DataFrame:
    return pd.DataFrame(columns=[time_col, "price", "recv_time"])


def _empty_feed_diagnostics(feed: str, paths: list[Path]) -> dict[str, Any]:
    return {
        "feed": feed,
        "paths": [str(path) for path in paths],
        "raw_row_count": 0,
        "parsed_timestamp_null_count": 0,
        "price_null_count": 0,
        "rows_after_parse_filter": 0,
        "rows_after_dedupe": 0,
        "duplicate_rows_removed": 0,
        "duplicate_rows_by_key": 0,
        "min_time": None,
        "max_time": None,
    }


def _write_dataframe(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.suffix.lower() == ".parquet":
        df.to_parquet(path, index=False)
        return
    if path.suffix.lower() == ".csv":
        df.to_csv(path, index=False)
        return
    raise SystemExit(f"unsupported tabular output path: {path}")


def _missing_binance_events(events: pd.DataFrame) -> pd.DataFrame:
    if events.empty:
        return events
    return events[(events["complete_chainlink_label"]) & (~events["complete_binance_label"])].copy()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build BTC-5m event windows and proxy diagnostics from recorder data."
    )
    parser.add_argument("--input-root", type=Path, action="append", required=True)
    parser.add_argument("--output-events", type=Path, required=True, help="Path ending in .parquet or .csv")
    parser.add_argument("--output-summary", type=Path, required=True, help="Path ending in .json")
    parser.add_argument("--output-missing-binance-events", type=Path)
    parser.add_argument("--boundary-method", choices=["nearest", "previous"], default="nearest")
    parser.add_argument("--max-boundary-distance-seconds", type=float, default=1.0)
    parser.add_argument("--diagnostic-buffer-seconds", type=float, default=10.0)
    parser.add_argument("--sweep-output", type=Path, help="Optional CSV or parquet output for a boundary sweep summary table")
    parser.add_argument("--sweep-distances", type=float, nargs="*", default=list(DEFAULT_SWEEP_DISTANCES))
    parser.add_argument("--sweep-methods", choices=["nearest", "previous"], nargs="*", default=list(DEFAULT_SWEEP_METHODS))
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    events, summary = build_event_table(
        args.input_root,
        boundary_method=args.boundary_method,
        max_boundary_distance_seconds=args.max_boundary_distance_seconds,
        diagnostic_buffer_seconds=args.diagnostic_buffer_seconds,
    )
    _write_dataframe(args.output_events, events)
    args.output_summary.parent.mkdir(parents=True, exist_ok=True)
    args.output_summary.write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")

    if args.output_missing_binance_events:
        _write_dataframe(args.output_missing_binance_events, _missing_binance_events(events))

    if args.sweep_output:
        sweep = build_boundary_sweep(
            args.input_root,
            methods=args.sweep_methods,
            max_boundary_distances=args.sweep_distances,
            diagnostic_buffer_seconds=args.diagnostic_buffer_seconds,
        )
        _write_dataframe(args.sweep_output, sweep)
        print(sweep.to_string(index=False))

    print(json.dumps(summary, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
