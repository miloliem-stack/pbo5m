#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.build_btc5m_event_table import (
    build_windows,
    discover_sources,
    load_binance,
    load_chainlink,
    load_meta,
    load_quotes,
    sign_label,
)

DEFAULT_INPUT_ROOTS = [
    Path("data/legacy_market_recordings"),
    Path("artifacts/market_recorder"),
]
DEFAULT_OUTPUT_PARQUET = Path("artifacts/legacy_event_sets/btc5m_events_nearest_2s_v1.parquet")
DEFAULT_OUTPUT_CSV = Path("artifacts/legacy_event_sets/btc5m_events_nearest_2s_v1.csv")
DEFAULT_MANIFEST = Path("artifacts/legacy_event_sets/btc5m_events_nearest_2s_v1_manifest.json")


def discover_legacy_sources(input_roots: list[Path]) -> tuple[list[Any], dict[str, Any]]:
    discoveries = [discover_sources(root) for root in input_roots if root.exists()]
    sources = [source for discovery in discoveries for source in discovery.sources]
    return sources, {
        "roots": [discovery.diagnostics for discovery in discoveries],
        "input_roots": [str(root) for root in input_roots],
        "discovered_source_count": len(sources),
    }


def _windows_from_metadata(meta_df: pd.DataFrame, quote_df: pd.DataFrame) -> pd.DataFrame:
    if not meta_df.empty:
        windows = meta_df[["slug", "market_id", "market_start", "market_end"]].dropna(subset=["slug", "market_start", "market_end"])
        windows = windows.drop_duplicates().sort_values(["market_start", "slug", "market_id"]).reset_index(drop=True)
        if not windows.empty:
            return windows
    return build_windows(quote_df, meta_df)


def _nearest_observation(
    df: pd.DataFrame,
    *,
    time_col: str,
    target: pd.Timestamp,
    tolerance_sec: float,
) -> dict[str, Any]:
    if df.empty:
        return {"matched": False, "lag_sec": None, "ts": None, "row": None}
    times = df[time_col]
    pos = int(times.searchsorted(target))
    candidates: list[tuple[float, Any]] = []
    for idx in (pos - 1, pos):
        if 0 <= idx < len(df):
            row = df.iloc[idx]
            lag = abs((row[time_col] - target).total_seconds())
            candidates.append((lag, row))
    if not candidates:
        return {"matched": False, "lag_sec": None, "ts": None, "row": None}
    lag_sec, row = min(candidates, key=lambda item: item[0])
    if lag_sec > tolerance_sec:
        return {"matched": False, "lag_sec": lag_sec, "ts": row[time_col], "row": row}
    return {"matched": True, "lag_sec": lag_sec, "ts": row[time_col], "row": row}


def _quote_slice(quotes_df: pd.DataFrame, slug: str, market_id: str, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    if quotes_df.empty:
        return quotes_df
    return quotes_df[
        (quotes_df["record_type"] == "quote_snapshot")
        & (quotes_df["slug"] == slug)
        & (quotes_df["market_id"] == market_id)
        & (quotes_df["market_start"] == start)
        & (quotes_df["market_end"] == end)
    ].sort_values("ts_dt")


def _safe_spread(bid: Any, ask: Any) -> float | None:
    if pd.isna(bid) or pd.isna(ask):
        return None
    return float(ask) - float(bid)


def build_legacy_event_set(
    *,
    input_roots: list[Path],
    nearest_tolerance_sec: float = 2.0,
    tiny_move_threshold: float = 50.0,
    max_quote_spread: float = 0.10,
    max_stale_quote_sec: float = 2.0,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    sources, source_info = discover_legacy_sources(input_roots)
    if not sources:
        raise RuntimeError("No recorder sources found.")

    chainlink_paths = [source.path / "chainlink_prices.jsonl" for source in sources if (source.path / "chainlink_prices.jsonl").exists()]
    binance_paths = [source.path / "binance_prices.jsonl" for source in sources if (source.path / "binance_prices.jsonl").exists()]
    quote_paths = [source.path / "market_quotes.jsonl" for source in sources if (source.path / "market_quotes.jsonl").exists()]
    meta_paths = [source.path / "market_meta.jsonl" for source in sources if (source.path / "market_meta.jsonl").exists()]

    chainlink = load_chainlink(chainlink_paths)
    binance = load_binance(binance_paths)
    quotes = load_quotes(quote_paths)
    meta = load_meta(meta_paths)

    windows = _windows_from_metadata(meta.deduped_df, quotes.deduped_df)
    rows: list[dict[str, Any]] = []
    for _, window in windows.iterrows():
        slug = window["slug"]
        market_id = window["market_id"]
        start = window["market_start"]
        end = window["market_end"]

        chainlink_start = _nearest_observation(chainlink.deduped_df, time_col="source_time", target=start, tolerance_sec=nearest_tolerance_sec)
        chainlink_end = _nearest_observation(chainlink.deduped_df, time_col="source_time", target=end, tolerance_sec=nearest_tolerance_sec)
        binance_start = _nearest_observation(binance.deduped_df, time_col="event_time", target=start, tolerance_sec=nearest_tolerance_sec)
        binance_end = _nearest_observation(binance.deduped_df, time_col="event_time", target=end, tolerance_sec=nearest_tolerance_sec)

        quote_window = _quote_slice(quotes.deduped_df, slug, market_id, start, end)
        quote_match = _nearest_observation(quote_window, time_col="ts_dt", target=start, tolerance_sec=nearest_tolerance_sec)
        quote_row = quote_match["row"]

        chainlink_start_price = None if chainlink_start["row"] is None else float(chainlink_start["row"]["price"])
        chainlink_end_price = None if chainlink_end["row"] is None else float(chainlink_end["row"]["price"])
        binance_start_price = None if binance_start["row"] is None else float(binance_start["row"]["price"])
        binance_end_price = None if binance_end["row"] is None else float(binance_end["row"]["price"])

        chainlink_move = None if not (chainlink_start["matched"] and chainlink_end["matched"]) else float(chainlink_end_price - chainlink_start_price)
        binance_move = None if not (binance_start["matched"] and binance_end["matched"]) else float(binance_end_price - binance_start_price)
        chainlink_label = sign_label(chainlink_move)
        binance_label = sign_label(binance_move)
        label_agreement = None if chainlink_label is None or binance_label is None else chainlink_label == binance_label

        yes_bid = None if quote_row is None else quote_row.get("yes_best_bid")
        yes_ask = None if quote_row is None else quote_row.get("yes_best_ask")
        no_bid = None if quote_row is None else quote_row.get("no_best_bid")
        no_ask = None if quote_row is None else quote_row.get("no_best_ask")
        yes_spread = None if quote_row is None else quote_row.get("yes_spread")
        no_spread = None if quote_row is None else quote_row.get("no_spread")
        if yes_spread is None:
            yes_spread = _safe_spread(yes_bid, yes_ask)
        if no_spread is None:
            no_spread = _safe_spread(no_bid, no_ask)

        quote_abs_lag_sec = quote_match["lag_sec"] if quote_match["matched"] else None
        chainlink_abs_move = None if chainlink_move is None else abs(chainlink_move)
        quote_missing = quote_row is None or not quote_match["matched"]

        row = {
            "slug": slug,
            "market_id": market_id,
            "market_start_time": start,
            "market_end_time": end,
            "nearest_tolerance_sec": nearest_tolerance_sec,
            "chainlink_start_ts": chainlink_start["ts"],
            "chainlink_end_ts": chainlink_end["ts"],
            "binance_start_ts": binance_start["ts"],
            "binance_end_ts": binance_end["ts"],
            "chainlink_start_price": chainlink_start_price if chainlink_start["matched"] else None,
            "chainlink_end_price": chainlink_end_price if chainlink_end["matched"] else None,
            "binance_start_price": binance_start_price if binance_start["matched"] else None,
            "binance_end_price": binance_end_price if binance_end["matched"] else None,
            "chainlink_start_abs_lag_sec": chainlink_start["lag_sec"] if chainlink_start["matched"] else None,
            "chainlink_end_abs_lag_sec": chainlink_end["lag_sec"] if chainlink_end["matched"] else None,
            "binance_start_abs_lag_sec": binance_start["lag_sec"] if binance_start["matched"] else None,
            "binance_end_abs_lag_sec": binance_end["lag_sec"] if binance_end["matched"] else None,
            "quote_ts": None if quote_row is None else quote_row["ts_dt"],
            "quote_abs_lag_sec": quote_abs_lag_sec,
            "quote_capture_ok": None if quote_row is None else bool(quote_row.get("quote_capture_ok", False)),
            "quote_capture_status": None if quote_row is None else quote_row.get("quote_capture_status"),
            "yes_bid": yes_bid,
            "yes_ask": yes_ask,
            "no_bid": no_bid,
            "no_ask": no_ask,
            "yes_mid": None if quote_row is None else quote_row.get("yes_mid"),
            "no_mid": None if quote_row is None else quote_row.get("no_mid"),
            "yes_spread": yes_spread,
            "no_spread": no_spread,
            "chainlink_move": chainlink_move,
            "binance_move": binance_move,
            "chainlink_label": chainlink_label,
            "binance_label": binance_label,
            "label_agreement": label_agreement,
            "missing_chainlink_start": not chainlink_start["matched"],
            "missing_chainlink_end": not chainlink_end["matched"],
            "missing_binance_start": not binance_start["matched"],
            "missing_binance_end": not binance_end["matched"],
            "chainlink_binance_label_disagree": bool(label_agreement is False),
            "tiny_move_near_boundary": bool(chainlink_abs_move is not None and chainlink_abs_move <= tiny_move_threshold),
            "wide_or_missing_quote": bool(
                quote_missing
                or yes_bid is None
                or yes_ask is None
                or no_bid is None
                or no_ask is None
                or (yes_spread is not None and yes_spread > max_quote_spread)
                or (no_spread is not None and no_spread > max_quote_spread)
            ),
            "stale_quote": bool(quote_abs_lag_sec is not None and quote_abs_lag_sec > max_stale_quote_sec),
        }
        rows.append(row)

    events = pd.DataFrame(rows)
    if not events.empty:
        events = events.sort_values(["market_start_time", "slug"]).reset_index(drop=True)

    manifest = build_manifest(
        events=events,
        input_roots=input_roots,
        nearest_tolerance_sec=nearest_tolerance_sec,
        source_info=source_info,
    )
    return events, manifest


def build_manifest(
    *,
    events: pd.DataFrame,
    input_roots: list[Path],
    nearest_tolerance_sec: float,
    source_info: dict[str, Any],
) -> dict[str, Any]:
    total_events = int(len(events))
    full_chainlink = int((~events["missing_chainlink_start"] & ~events["missing_chainlink_end"]).sum()) if not events.empty else 0
    full_binance = int((~events["missing_binance_start"] & ~events["missing_binance_end"]).sum()) if not events.empty else 0
    comparable = events[events["chainlink_label"].notna() & events["binance_label"].notna()] if not events.empty else pd.DataFrame()
    agreement_count = int((comparable["label_agreement"] == True).sum()) if not comparable.empty else 0
    quote_covered = int(events["quote_ts"].notna().sum()) if not events.empty else 0
    flag_columns = [
        "missing_chainlink_start",
        "missing_chainlink_end",
        "missing_binance_start",
        "missing_binance_end",
        "chainlink_binance_label_disagree",
        "tiny_move_near_boundary",
        "wide_or_missing_quote",
        "stale_quote",
    ]
    return {
        "input_roots": [str(root) for root in input_roots],
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "nearest_tolerance_sec": nearest_tolerance_sec,
        "number_of_markets_discovered": total_events,
        "number_of_events_emitted": total_events,
        "full_chainlink_labels_count": full_chainlink,
        "full_chainlink_labels_pct": _pct(full_chainlink, total_events),
        "full_binance_labels_count": full_binance,
        "full_binance_labels_pct": _pct(full_binance, total_events),
        "label_agreement_count": agreement_count,
        "label_agreement_pct_of_comparable": _pct(agreement_count, len(comparable)),
        "quote_coverage_count": quote_covered,
        "quote_coverage_pct": _pct(quote_covered, total_events),
        "min_market_start_time": None if events.empty else str(events["market_start_time"].min()),
        "max_market_end_time": None if events.empty else str(events["market_end_time"].max()),
        "column_schema": [{"name": column, "dtype": str(dtype)} for column, dtype in events.dtypes.items()],
        "data_quality_flag_counts": {column: int(events[column].fillna(False).sum()) for column in flag_columns} if not events.empty else {column: 0 for column in flag_columns},
        "source_info": source_info,
    }


def _pct(numerator: int, denominator: int) -> float | None:
    if not denominator:
        return None
    return float(numerator / denominator)


def write_frozen_event_set(
    *,
    events: pd.DataFrame,
    manifest: dict[str, Any],
    output_parquet: Path,
    output_csv: Path,
    manifest_path: Path,
) -> dict[str, str]:
    output_parquet.parent.mkdir(parents=True, exist_ok=True)
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)

    artifact_path = output_parquet
    artifact_format = "parquet"
    try:
        events.to_parquet(output_parquet, index=False)
    except Exception:
        events.to_csv(output_csv, index=False)
        artifact_path = output_csv
        artifact_format = "csv"

    manifest_payload = dict(manifest)
    manifest_payload["artifact_path"] = str(artifact_path)
    manifest_payload["artifact_format"] = artifact_format
    manifest_path.write_text(json.dumps(manifest_payload, indent=2, default=str), encoding="utf-8")
    return {
        "artifact_path": str(artifact_path),
        "artifact_format": artifact_format,
        "manifest_path": str(manifest_path),
    }


def inspect_event_set(events: pd.DataFrame, artifact_path: str) -> str:
    if events.empty:
        return f"total_events=0 output_path={artifact_path}"
    lag_columns = [
        "chainlink_start_abs_lag_sec",
        "chainlink_end_abs_lag_sec",
        "binance_start_abs_lag_sec",
        "binance_end_abs_lag_sec",
        "quote_abs_lag_sec",
    ]
    lag_quantiles = {
        column: events[column].dropna().quantile([0.5, 0.9, 0.99]).to_dict()
        for column in lag_columns
        if column in events and not events[column].dropna().empty
    }
    summary = {
        "total_events": int(len(events)),
        "chainlink_label_coverage": int((events["chainlink_label"].notna()).sum()),
        "binance_label_coverage": int((events["binance_label"].notna()).sum()),
        "agreement_count": int((events["label_agreement"] == True).sum()),
        "disagreement_count": int((events["label_agreement"] == False).sum()),
        "quote_coverage": int(events["quote_ts"].notna().sum()),
        "tiny_move_count": int(events["tiny_move_near_boundary"].fillna(False).sum()),
        "lag_quantiles": lag_quantiles,
        "output_path": artifact_path,
    }
    return json.dumps(summary, indent=2, default=str)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build and freeze a BTC-5m legacy event set.")
    parser.add_argument("--input-root", type=Path, action="append", default=None)
    parser.add_argument("--nearest-tolerance-sec", type=float, default=2.0)
    parser.add_argument("--tiny-move-threshold", type=float, default=50.0)
    parser.add_argument("--max-quote-spread", type=float, default=0.10)
    parser.add_argument("--max-stale-quote-sec", type=float, default=2.0)
    parser.add_argument("--output-parquet", type=Path, default=DEFAULT_OUTPUT_PARQUET)
    parser.add_argument("--output-csv", type=Path, default=DEFAULT_OUTPUT_CSV)
    parser.add_argument("--manifest-path", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--inspect-only", type=Path, help="Inspect an existing CSV or parquet artifact and print a compact summary.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.inspect_only:
        events = pd.read_csv(args.inspect_only) if args.inspect_only.suffix.lower() == ".csv" else pd.read_parquet(args.inspect_only)
        print(inspect_event_set(events, str(args.inspect_only)))
        return 0

    input_roots = args.input_root or DEFAULT_INPUT_ROOTS
    events, manifest = build_legacy_event_set(
        input_roots=input_roots,
        nearest_tolerance_sec=args.nearest_tolerance_sec,
        tiny_move_threshold=args.tiny_move_threshold,
        max_quote_spread=args.max_quote_spread,
        max_stale_quote_sec=args.max_stale_quote_sec,
    )
    outputs = write_frozen_event_set(
        events=events,
        manifest=manifest,
        output_parquet=args.output_parquet,
        output_csv=args.output_csv,
        manifest_path=args.manifest_path,
    )
    print(inspect_event_set(events, outputs["artifact_path"]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
