#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


DEFAULT_KEYS = ["market_id", "model_id", "decision_age", "latency_ms"]
CAPACITY_COLS = [
    "capacity_usdc_at_edge_10",
    "capacity_usdc_at_edge_07",
    "capacity_usdc_at_edge_05",
    "capacity_usdc_until_baseline_edge",
    "max_fillable_usdc",
]
VWAP_PREFIXES = ("vwap_at_", "edge_after_vwap_at_")


def parse_csv(value: str) -> list[str]:
    return [item.strip() for item in str(value).split(",") if item.strip()]


def read_frame(path: Path) -> pd.DataFrame:
    return pd.read_parquet(path) if path.suffix.lower() == ".parquet" else pd.read_csv(path)


def aggregation_for(frame: pd.DataFrame, keys: list[str], capacity_method: str) -> dict[str, Any]:
    agg: dict[str, Any] = {}
    numeric_cols = set(frame.select_dtypes(include=[np.number]).columns)
    for col in frame.columns:
        if col in keys:
            continue
        if col in CAPACITY_COLS or col.startswith(VWAP_PREFIXES):
            agg[col] = capacity_method
        elif col in numeric_cols:
            agg[col] = "first"
        else:
            agg[col] = "first"
    return agg


def dedupe_capacity(frame: pd.DataFrame, keys: list[str], capacity_method: str = "first") -> tuple[pd.DataFrame, pd.DataFrame]:
    missing = [key for key in keys if key not in frame.columns]
    if missing:
        raise ValueError(f"Missing dedupe key columns: {missing}. Available columns: {list(frame.columns)}")
    group_sizes = frame.groupby(keys, dropna=False).size().rename("source_row_count").reset_index()
    unique_counts = []
    for col in CAPACITY_COLS + [c for c in frame.columns if c.startswith(VWAP_PREFIXES)]:
        if col in frame.columns:
            unique_counts.append(frame.groupby(keys, dropna=False)[col].nunique(dropna=False).rename(f"{col}_unique_count"))
    diagnostics = group_sizes
    for series in unique_counts:
        diagnostics = diagnostics.merge(series.reset_index(), on=keys, how="left")
    out = frame.groupby(keys, dropna=False).agg(aggregation_for(frame, keys, capacity_method)).reset_index()
    out = out.merge(group_sizes, on=keys, how="left")
    return out, diagnostics


def summarize(frame: pd.DataFrame, group_cols: list[str]) -> pd.DataFrame:
    rows = []
    if frame.empty:
        return pd.DataFrame()
    for keys, group in frame.groupby(group_cols, dropna=False):
        if not isinstance(keys, tuple):
            keys = (keys,)
        row = dict(zip(group_cols, keys))
        row["rows"] = int(len(group))
        row["markets"] = int(group["market_id"].nunique()) if "market_id" in group else None
        for col in CAPACITY_COLS:
            if col in group:
                row[f"mean_{col}"] = float(group[col].mean())
                row[f"median_{col}"] = float(group[col].median())
                row[f"p10_{col}"] = float(np.nanpercentile(group[col], 10))
                row[f"p90_{col}"] = float(np.nanpercentile(group[col], 90))
        if "best_ask" in group:
            row["mean_best_ask"] = float(group["best_ask"].mean())
        rows.append(row)
    return pd.DataFrame(rows)


def run(args: argparse.Namespace) -> dict[str, Any]:
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    frame = read_frame(Path(args.input))
    keys = parse_csv(args.keys)
    deduped, dupes = dedupe_capacity(frame, keys, args.capacity_method)
    deduped.to_parquet(output_dir / "capacity_per_market_dedup.parquet", index=False)
    deduped.to_csv(output_dir / "capacity_per_market_dedup.csv", index=False)
    dupes.to_csv(output_dir / "capacity_dedupe_diagnostics_by_key.csv", index=False)
    summarize(deduped, ["model_id"]).to_csv(output_dir / "capacity_dedup_by_model.csv", index=False)
    summarize(deduped, ["decision_age"]).to_csv(output_dir / "capacity_dedup_by_age.csv", index=False)
    summarize(deduped, ["latency_ms"]).to_csv(output_dir / "capacity_dedup_by_latency.csv", index=False)
    if "chainlink_terminal_margin_band" in deduped.columns:
        summarize(deduped, ["chainlink_terminal_margin_band"]).to_csv(output_dir / "capacity_dedup_by_terminal_margin_band.csv", index=False)
    diagnostics = {
        "input": str(args.input),
        "output_dir": str(output_dir),
        "keys": keys,
        "capacity_method": args.capacity_method,
        "input_rows": int(len(frame)),
        "deduped_rows": int(len(deduped)),
        "duplicate_rows_removed": int(len(frame) - len(deduped)),
        "unique_markets": int(deduped["market_id"].nunique()) if "market_id" in deduped else None,
        "mean_source_rows_per_deduped_row": float(deduped["source_row_count"].mean()) if "source_row_count" in deduped else None,
        "max_source_rows_per_deduped_row": int(deduped["source_row_count"].max()) if "source_row_count" in deduped else None,
    }
    (output_dir / "capacity_dedupe_diagnostics.json").write_text(json.dumps(diagnostics, indent=2, default=str), encoding="utf-8")
    (output_dir / "capacity_dedupe_readme.txt").write_text(
        "Capacity de-duplication\n\n"
        "This collapses repeated capacity rows to one row per configured key. Defaults are market_id, model_id, decision_age, latency_ms.\n"
        "The resulting capacities are per market/setting, not daily totals.\n",
        encoding="utf-8",
    )
    return diagnostics


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="De-duplicate capacity curve rows to one capacity row per model/setting/market.")
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--keys", default=",".join(DEFAULT_KEYS))
    parser.add_argument("--capacity-method", choices=["first", "max", "min", "median", "mean"], default="first")
    return parser


def main(argv: list[str] | None = None) -> int:
    diagnostics = run(build_parser().parse_args(argv))
    print(json.dumps(diagnostics, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
