#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

DEFAULT_INPUT_ROOTS = [
    Path("data/binance"),
    Path("data/binance/btcusdt_1m"),
    Path("data/binance/BTCUSDT_1m"),
    Path("artifacts/binance"),
    Path("data/binance-btc1m"),
]
DEFAULT_OUTPUT_CSV = Path("artifacts/binance_btc5m_research/btc5m_binance_1m_events_v1.csv")
DEFAULT_MANIFEST = Path("artifacts/binance_btc5m_research/btc5m_binance_1m_events_v1_manifest.json")
DEFAULT_TINY_MOVE_THRESHOLD = 50.0
STANDARD_BINANCE_COLUMNS = [
    "open_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "close_time",
    "quote_asset_volume",
    "number_of_trades",
    "taker_buy_base_asset_volume",
    "taker_buy_quote_asset_volume",
    "ignore",
]


@dataclass
class LoadedKlines:
    rows_loaded: int
    rows_after_dedup: int
    duplicate_count: int
    files: list[str]
    frame: pd.DataFrame
    gap_summary: list[dict[str, Any]]


def discover_binance_files(input_roots: list[Path]) -> list[Path]:
    files: list[Path] = []
    for root in input_roots:
        if not root.exists():
            continue
        if root.is_file() and root.suffix.lower() in {".csv", ".parquet"}:
            files.append(root)
            continue
        for path in sorted(root.rglob("*")):
            if not path.is_file() or path.suffix.lower() not in {".csv", ".parquet"}:
                continue
            if "BTCUSDT" in path.name.upper() or "BINANCE" in str(path).upper():
                files.append(path)
    unique: list[Path] = []
    seen: set[Path] = set()
    for path in files:
        resolved = path.resolve()
        if resolved in seen:
            continue
        seen.add(resolved)
        unique.append(path)
    return unique


def _infer_csv_has_header(path: Path) -> bool:
    first_line = path.read_text(encoding="utf-8", errors="ignore").splitlines()[0]
    return any(ch.isalpha() for ch in first_line)


def _coerce_timestamp(series: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    if numeric.notna().any():
        sample = float(numeric.dropna().iloc[0])
        if sample >= 1e15:
            return pd.to_datetime(numeric, unit="us", utc=True, errors="coerce").astype("datetime64[ns, UTC]")
        if sample > 1e12:
            return pd.to_datetime(numeric, unit="ms", utc=True, errors="coerce").astype("datetime64[ns, UTC]")
        if sample > 1e10:
            return pd.to_datetime(numeric, unit="ms", utc=True, errors="coerce").astype("datetime64[ns, UTC]")
        if sample > 1e9:
            return pd.to_datetime(numeric, unit="s", utc=True, errors="coerce").astype("datetime64[ns, UTC]")
    return pd.to_datetime(series, utc=True, errors="coerce").astype("datetime64[ns, UTC]")


def _normalize_kline_columns(frame: pd.DataFrame, source_file: Path) -> pd.DataFrame:
    normalized = frame.copy()
    if list(normalized.columns) == list(range(len(normalized.columns))):
        rename = {idx: name for idx, name in enumerate(STANDARD_BINANCE_COLUMNS[: len(normalized.columns)])}
        normalized = normalized.rename(columns=rename)

    aliases = {
        "event_time": ["event_time", "open_time", "open_time_ms", "timestamp", "datetime", "date"],
        "close_time": ["close_time", "close_time_ms"],
        "open": ["open", "open_price"],
        "high": ["high", "high_price"],
        "low": ["low", "low_price"],
        "close": ["close", "close_price"],
        "volume": ["volume", "base_volume"],
    }
    selected: dict[str, pd.Series] = {}
    for target, candidates in aliases.items():
        for candidate in candidates:
            if candidate in normalized.columns:
                selected[target] = normalized[candidate]
                break
    if "event_time" not in selected or "close" not in selected:
        raise ValueError(f"Could not normalize Binance kline columns for {source_file}")

    output = pd.DataFrame(selected)
    output["event_time"] = _coerce_timestamp(output["event_time"])
    if "close_time" in output.columns:
        output["close_time"] = _coerce_timestamp(output["close_time"])
    else:
        output["close_time"] = output["event_time"] + pd.Timedelta(minutes=1) - pd.Timedelta(milliseconds=1)
    for column in ("open", "high", "low", "close", "volume"):
        if column in output.columns:
            output[column] = pd.to_numeric(output[column], errors="coerce")
        else:
            output[column] = np.nan
    output["source_file"] = str(source_file)
    return output.dropna(subset=["event_time", "close"]).reset_index(drop=True)


def load_binance_1m_klines(input_roots: list[Path]) -> LoadedKlines:
    files = discover_binance_files(input_roots)
    if not files:
        raise RuntimeError("No Binance 1m files discovered.")
    frames: list[pd.DataFrame] = []
    rows_loaded = 0
    for path in files:
        if path.suffix.lower() == ".parquet":
            raw = pd.read_parquet(path)
        else:
            if _infer_csv_has_header(path):
                raw = pd.read_csv(path)
            else:
                raw = pd.read_csv(path, header=None)
        normalized = _normalize_kline_columns(raw, path)
        rows_loaded += len(normalized)
        frames.append(normalized)
    combined = pd.concat(frames, ignore_index=True).sort_values("event_time").reset_index(drop=True)
    duplicate_mask = combined.duplicated(subset=["event_time"], keep="last")
    duplicate_count = int(duplicate_mask.sum())
    deduped = combined.loc[~duplicate_mask].sort_values("event_time").reset_index(drop=True)
    gap_summary = detect_gaps(deduped["event_time"])
    return LoadedKlines(
        rows_loaded=rows_loaded,
        rows_after_dedup=int(len(deduped)),
        duplicate_count=duplicate_count,
        files=[str(path) for path in files],
        frame=deduped,
        gap_summary=gap_summary,
    )


def detect_gaps(times: pd.Series) -> list[dict[str, Any]]:
    ordered = pd.Series(times).dropna().sort_values().reset_index(drop=True)
    if len(ordered) < 2:
        return []
    diffs = ordered.diff()
    gaps = ordered[diffs > pd.Timedelta(minutes=1)]
    summaries: list[dict[str, Any]] = []
    for idx in gaps.index:
        previous = ordered.iloc[idx - 1]
        current = ordered.iloc[idx]
        missing_minutes = int((current - previous) / pd.Timedelta(minutes=1) - 1)
        summaries.append(
            {
                "gap_start": previous.isoformat(),
                "gap_end": current.isoformat(),
                "missing_minutes": missing_minutes,
            }
        )
    return summaries


def sign_label(move: float) -> str:
    return "UP" if move > 0 else "DOWN"


def build_binance_btc5m_events(
    klines: pd.DataFrame,
    *,
    tiny_move_threshold: float = DEFAULT_TINY_MOVE_THRESHOLD,
) -> pd.DataFrame:
    ordered = klines.sort_values("event_time").reset_index(drop=True)
    start = ordered.rename(
        columns={
            "event_time": "event_start_time",
            "close": "reference_price",
            "source_file": "source_start_file",
        }
    )[["event_start_time", "reference_price", "source_start_file"]].copy()
    start["event_end_time"] = start["event_start_time"] + pd.Timedelta(minutes=5)
    end_source = ordered.rename(
        columns={
            "event_time": "source_end_ts",
            "close": "settlement_price",
            "source_file": "source_end_file",
        }
    )[["source_end_ts", "settlement_price", "source_end_file"]].copy()
    merged = pd.merge_asof(
        start.sort_values("event_end_time"),
        end_source.sort_values("source_end_ts"),
        left_on="event_end_time",
        right_on="source_end_ts",
        direction="backward",
        allow_exact_matches=True,
    ).sort_values("event_start_time").reset_index(drop=True)
    merged["source_start_ts"] = merged["event_start_time"]
    merged["source_start_lag_sec"] = 0.0
    merged["source_end_lag_sec"] = (merged["event_end_time"] - merged["source_end_ts"]).dt.total_seconds()
    merged["source_end_exact_match"] = merged["source_end_ts"] == merged["event_end_time"]
    merged["used_last_close_before_end"] = ~merged["source_end_exact_match"]
    merged["missing_settlement"] = merged["source_end_ts"].isna()
    merged = merged.loc[~merged["missing_settlement"]].reset_index(drop=True)
    merged["binance_move"] = merged["settlement_price"] - merged["reference_price"]
    merged["abs_binance_move"] = merged["binance_move"].abs()
    merged["binance_log_move"] = np.log(merged["settlement_price"] / merged["reference_price"])
    merged["binance_label"] = merged["binance_move"].apply(sign_label)
    merged["tiny_move_near_boundary"] = merged["abs_binance_move"] <= tiny_move_threshold
    merged["gap_crossed"] = merged["source_end_lag_sec"] > 60.0
    merged["data_quality_flags"] = merged.apply(
        lambda row: json.dumps(
            [
                flag
                for flag, active in (
                    ("used_last_close_before_end", bool(row["used_last_close_before_end"])),
                    ("gap_crossed", bool(row["gap_crossed"])),
                )
                if active
            ]
        ),
        axis=1,
    )
    merged["event_id"] = merged["event_start_time"].dt.strftime("btc5m_%Y%m%dT%H%M%SZ")
    columns = [
        "event_id",
        "event_start_time",
        "event_end_time",
        "reference_price",
        "settlement_price",
        "binance_label",
        "binance_move",
        "abs_binance_move",
        "binance_log_move",
        "tiny_move_near_boundary",
        "source_start_ts",
        "source_end_ts",
        "source_start_lag_sec",
        "source_end_lag_sec",
        "source_end_exact_match",
        "used_last_close_before_end",
        "gap_crossed",
        "source_start_file",
        "source_end_file",
        "data_quality_flags",
    ]
    return merged[columns].sort_values("event_start_time").reset_index(drop=True)


def build_manifest(
    *,
    loaded: LoadedKlines,
    events: pd.DataFrame,
    input_roots: list[Path],
    tiny_move_threshold: float,
    output_path: Path,
) -> dict[str, Any]:
    label_counts = events["binance_label"].value_counts().to_dict() if not events.empty else {}
    return {
        "input_roots": [str(root) for root in input_roots],
        "input_files": loaded.files,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "rows_loaded": loaded.rows_loaded,
        "rows_after_dedup": loaded.rows_after_dedup,
        "duplicate_count": loaded.duplicate_count,
        "gap_count": len(loaded.gap_summary),
        "gap_summaries": loaded.gap_summary[:100],
        "event_count": int(len(events)),
        "min_timestamp_utc": None if loaded.frame.empty else loaded.frame["event_time"].min().isoformat(),
        "max_timestamp_utc": None if loaded.frame.empty else loaded.frame["event_time"].max().isoformat(),
        "label_counts": label_counts,
        "tiny_move_count": int(events["tiny_move_near_boundary"].sum()) if not events.empty else 0,
        "output_path": str(output_path),
        "schema": events.columns.tolist(),
        "assumptions": {
            "reference_price": "Uses the 1m candle close at event_start_time.",
            "settlement_price": "Uses the 1m candle close at event_end_time when present, otherwise the last close at or before event_end_time.",
            "label_definition": "UP if settlement_price > reference_price else DOWN.",
            "tiny_move_threshold": tiny_move_threshold,
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build synthetic BTC-5m research events from Binance 1m candles.")
    parser.add_argument("--input-root", type=Path, action="append", default=None)
    parser.add_argument("--output-csv", type=Path, default=DEFAULT_OUTPUT_CSV)
    parser.add_argument("--manifest-path", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--tiny-move-threshold", type=float, default=DEFAULT_TINY_MOVE_THRESHOLD)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    input_roots = args.input_root or DEFAULT_INPUT_ROOTS
    loaded = load_binance_1m_klines(input_roots)
    events = build_binance_btc5m_events(loaded.frame, tiny_move_threshold=args.tiny_move_threshold)
    args.output_csv.parent.mkdir(parents=True, exist_ok=True)
    events.to_csv(args.output_csv, index=False)
    manifest = build_manifest(
        loaded=loaded,
        events=events,
        input_roots=input_roots,
        tiny_move_threshold=args.tiny_move_threshold,
        output_path=args.output_csv,
    )
    args.manifest_path.parent.mkdir(parents=True, exist_ok=True)
    args.manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(json.dumps({"event_count": len(events), "output_csv": str(args.output_csv), "manifest_path": str(args.manifest_path)}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
