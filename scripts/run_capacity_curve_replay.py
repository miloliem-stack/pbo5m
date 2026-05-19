#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.research import execution_realism_replay as ex


DEFAULT_PROBES = [1, 5, 10, 25, 50, 100]


def read_frame(path: Path) -> pd.DataFrame:
    return pd.read_parquet(path) if path.suffix.lower() == ".parquet" else pd.read_csv(path)


def parse_csv(value: str) -> list[str]:
    return [item.strip() for item in str(value).split(",") if item.strip()]


def normalize_entries(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    if "model_id" not in out and "model" in out:
        out["model_id"] = out["model"]
    if "market_key" not in out and "prediction_market_key" in out:
        out["market_key"] = out["prediction_market_key"]
    out["prediction_ts"] = pd.to_datetime(out["prediction_ts"], utc=True, errors="coerce")
    out["market_age_seconds"] = pd.to_numeric(out["market_age_seconds"], errors="coerce")
    out["edge_threshold"] = pd.to_numeric(out["edge_threshold"], errors="coerce")
    out["p_up"] = pd.to_numeric(out["p_up"], errors="coerce")
    out["raw_entry_price"] = pd.to_numeric(out.get("raw_entry_price", out.get("selected_price")), errors="coerce")
    out["raw_edge"] = pd.to_numeric(out.get("raw_edge", out.get("predicted_edge")), errors="coerce")
    return out.dropna(subset=["model_id", "market_key", "prediction_ts", "market_age_seconds", "p_up", "side"])


def p_chosen(row: pd.Series) -> float:
    return float(row["p_up"]) if row["side"] == "YES" else 1.0 - float(row["p_up"])


def vwap_for_stake(asks: list[dict[str, float]], stake: float) -> float:
    fill = ex.simulate_vwap_fill(asks, stake, min_trade_notional_usdc=0.0, min_fill_ratio=0.0, allow_partial_fills=True)
    return float(fill.get("vwap_price", np.nan))


def edge_after_vwap(p: float, vwap: float, fee_rate: float) -> float:
    if not np.isfinite(vwap):
        return np.nan
    fee_per_share = fee_rate * vwap * (1.0 - vwap)
    return float(p - vwap - fee_per_share)


def capacity_at_edge(asks: list[dict[str, float]], p: float, threshold: float, fee_rate: float) -> float:
    clean = sorted([{"price": float(x["price"]), "size": float(x["size"])} for x in asks if x.get("price") is not None and x.get("size") is not None and float(x["size"]) > 0], key=lambda x: x["price"])
    gross = 0.0
    shares = 0.0
    capacity = 0.0
    for level in clean:
        level_notional = level["price"] * level["size"]
        next_gross = gross + level_notional
        next_shares = shares + level["size"]
        next_vwap = next_gross / next_shares if next_shares else np.nan
        next_edge = edge_after_vwap(p, next_vwap, fee_rate)
        if next_edge >= threshold:
            gross = next_gross
            shares = next_shares
            capacity = gross
            continue
        # If edge fails after consuming the whole level, solve within this level by binary search.
        lo, hi = 0.0, level_notional
        for _ in range(40):
            mid = (lo + hi) / 2.0
            mid_shares = shares + mid / level["price"]
            mid_vwap = (gross + mid) / mid_shares if mid_shares else np.nan
            if edge_after_vwap(p, mid_vwap, fee_rate) >= threshold:
                lo = mid
            else:
                hi = mid
        return float(gross + lo)
    return float(capacity)


def best_book_for_entry(books: pd.DataFrame, row: pd.Series, latency_ms: float, max_book_age_seconds: float) -> dict[str, Any]:
    return ex.select_execution_book(books, str(row["market_key"]), str(row["side"]), row["prediction_ts"], latency_ms, max_book_age_seconds)


def capacity_rows(entries: pd.DataFrame, books: pd.DataFrame, latencies: list[float], fee_rate: float, max_book_age_seconds: float, probes: list[float]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for _, entry in entries.iterrows():
        for latency in latencies:
            book = best_book_for_entry(books, entry, latency, max_book_age_seconds)
            p = p_chosen(entry)
            base = {
                "market_id": entry.get("market_key"),
                "model_id": entry.get("model_id"),
                "decision_age": entry.get("market_age_seconds"),
                "latency_ms": latency,
                "side": entry.get("side"),
                "decision_ts": entry.get("prediction_ts"),
                "execution_book_ts": book.get("execution_book_ts"),
                "execution_book_status": book.get("execution_book_status"),
                "book_lag_seconds": book.get("execution_book_lag_seconds"),
                "p_chosen_side": p,
                "edge_threshold": entry.get("edge_threshold"),
                "raw_edge": entry.get("raw_edge"),
                "age_bucket": entry.get("age_bucket"),
                "fold_id": entry.get("fold_id"),
                "chainlink_terminal_margin_band": entry.get("chainlink_terminal_margin_band"),
            }
            asks = book.get("asks") or []
            best_ask = book.get("best_ask", np.nan)
            row = {
                **base,
                "best_ask": best_ask,
                "capacity_usdc_at_edge_10": capacity_at_edge(asks, p, 0.10, fee_rate) if asks else 0.0,
                "capacity_usdc_at_edge_07": capacity_at_edge(asks, p, 0.07, fee_rate) if asks else 0.0,
                "capacity_usdc_at_edge_05": capacity_at_edge(asks, p, 0.05, fee_rate) if asks else 0.0,
                "capacity_usdc_until_baseline_edge": capacity_at_edge(asks, p, 0.0, fee_rate) if asks else 0.0,
                "max_fillable_usdc": float(sum(float(x["price"]) * float(x["size"]) for x in asks)) if asks else 0.0,
            }
            for stake in probes:
                vwap = vwap_for_stake(asks, stake) if asks else np.nan
                row[f"vwap_at_{int(stake)}"] = vwap
                row[f"edge_after_vwap_at_{int(stake)}"] = edge_after_vwap(p, vwap, fee_rate)
            rows.append(row)
    return pd.DataFrame(rows)


def aggregate_capacity(frame: pd.DataFrame, group_cols: list[str]) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame()
    rows = []
    cap_cols = ["capacity_usdc_at_edge_10", "capacity_usdc_at_edge_07", "capacity_usdc_at_edge_05", "capacity_usdc_until_baseline_edge", "max_fillable_usdc"]
    for keys, group in frame.groupby(group_cols, dropna=False):
        if not isinstance(keys, tuple):
            keys = (keys,)
        row = dict(zip(group_cols, keys))
        row["rows"] = int(len(group))
        row["markets"] = int(group["market_id"].nunique())
        row["execution_book_ok_rate"] = float(group["execution_book_status"].eq("ok").mean())
        for col in cap_cols:
            row[f"mean_{col}"] = float(group[col].mean())
            row[f"median_{col}"] = float(group[col].median())
            row[f"p10_{col}"] = float(np.nanpercentile(group[col], 10))
            row[f"p90_{col}"] = float(np.nanpercentile(group[col], 90))
        row["mean_best_ask"] = float(group["best_ask"].mean())
        row["mean_edge_after_vwap_at_5"] = float(group["edge_after_vwap_at_5"].mean()) if "edge_after_vwap_at_5" in group else np.nan
        rows.append(row)
    return pd.DataFrame(rows)


def marginal_edge_decay(frame: pd.DataFrame, probes: list[float]) -> pd.DataFrame:
    rows = []
    for _, row in frame.iterrows():
        previous_edge = None
        previous_stake = None
        for stake in probes:
            edge = row.get(f"edge_after_vwap_at_{int(stake)}")
            rows.append(
                {
                    "market_id": row.get("market_id"),
                    "model_id": row.get("model_id"),
                    "decision_age": row.get("decision_age"),
                    "latency_ms": row.get("latency_ms"),
                    "stake_usdc": stake,
                    "edge_after_vwap": edge,
                    "vwap": row.get(f"vwap_at_{int(stake)}"),
                    "edge_decay_from_previous_probe": None if previous_edge is None else edge - previous_edge,
                    "previous_stake_usdc": previous_stake,
                }
            )
            previous_edge = edge
            previous_stake = stake
    return pd.DataFrame(rows)


def run(args: argparse.Namespace) -> dict[str, Any]:
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    entries = normalize_entries(read_frame(Path(args.selected_entries)))
    if args.market_label_audit:
        labels = read_frame(Path(args.market_label_audit))
        label_cols = [c for c in ["market_key", "chainlink_terminal_margin_band", "abs_chainlink_terminal_margin_usd"] if c in labels.columns]
        entries = entries.drop(columns=[c for c in label_cols if c in entries.columns and c != "market_key"], errors="ignore").merge(labels[label_cols], on="market_key", how="left")
    if args.models:
        entries = entries[entries["model_id"].isin(parse_csv(args.models))]
    if args.edge_thresholds:
        thresholds = ex.parse_csv_floats(args.edge_thresholds)
        entries = entries[entries["edge_threshold"].isin(thresholds)]
    ages = ex.parse_csv_floats(args.entry_ages)
    if ages:
        mask = pd.Series(False, index=entries.index)
        for age in ages:
            mask |= (entries["market_age_seconds"] - age).abs() < 1e-9
        entries = entries[mask]
    latencies = ex.parse_csv_floats(args.latency_ms_sweep)
    max_latency = max(latencies) if latencies else 0.0
    target_times = entries["prediction_ts"] + pd.to_timedelta(max_latency, unit="ms")
    quote_files = ex.quote_files_for_targets(Path(args.quotes_root), target_times)
    books, book_diag = ex.load_books_from_files(quote_files, set(entries["market_key"].dropna().astype(str).unique()))
    probes = ex.parse_csv_floats(args.stake_probe_usdc)
    cap = capacity_rows(entries, books, latencies, args.fee_rate, args.max_book_age_seconds, probes)
    cap.to_parquet(output_dir / "capacity_per_market.parquet", index=False)
    aggregate_capacity(cap, ["model_id"]).to_csv(output_dir / "capacity_curve_by_model.csv", index=False)
    aggregate_capacity(cap, ["decision_age"]).to_csv(output_dir / "capacity_curve_by_age.csv", index=False)
    aggregate_capacity(cap, ["latency_ms"]).to_csv(output_dir / "capacity_curve_by_latency.csv", index=False)
    aggregate_capacity(cap, ["chainlink_terminal_margin_band"]).to_csv(output_dir / "capacity_curve_by_terminal_margin_band.csv", index=False)
    marginal_edge_decay(cap, probes).to_csv(output_dir / "marginal_edge_decay.csv", index=False)
    diagnostics = {
        **book_diag,
        "selected_entries_loaded": int(len(entries)),
        "capacity_rows": int(len(cap)),
        "quote_files_read": [str(path) for path in quote_files],
        "note": "capacity_usdc_until_baseline_edge is defined as capacity until cost-adjusted edge falls below 0.0.",
    }
    (output_dir / "capacity_curve_diagnostics.json").write_text(json.dumps(diagnostics, indent=2, default=str), encoding="utf-8")
    (output_dir / "capacity_curve_readme.txt").write_text(
        "Capacity curve replay\n\n"
        "Offline research only. This reads only recorder hour files implied by selected entry timestamps and latency settings.\n"
        "capacity_usdc_until_baseline_edge is the visible ask-depth notional until cost-adjusted edge drops below zero.\n",
        encoding="utf-8",
    )
    return diagnostics


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Targeted capacity curve replay from selected entries and recorded orderbook snapshots.")
    parser.add_argument("--selected-entries", type=Path, required=True)
    parser.add_argument("--quotes-root", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--market-label-audit", type=Path)
    parser.add_argument("--models")
    parser.add_argument("--edge-thresholds", default="0.10")
    parser.add_argument("--entry-ages", default="60,120,180")
    parser.add_argument("--latency-ms-sweep", default="0,250,500,1000,2000")
    parser.add_argument("--max-book-age-seconds", type=float, default=2.0)
    parser.add_argument("--fee-rate", type=float, default=0.07)
    parser.add_argument("--stake-probe-usdc", default="1,5,10,25,50,100")
    return parser


def main(argv: list[str] | None = None) -> int:
    diagnostics = run(build_parser().parse_args(argv))
    print(json.dumps(diagnostics, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
