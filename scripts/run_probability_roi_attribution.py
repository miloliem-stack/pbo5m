#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import shutil
import sys
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


EDGE_BINS = [-np.inf, 0.01, 0.02, 0.03, 0.04, 0.05, 0.075, 0.10, np.inf]
EDGE_LABELS = ["<0.01", "0.01_0.02", "0.02_0.03", "0.03_0.04", "0.04_0.05", "0.05_0.075", "0.075_0.10", ">0.10"]
ASK_BINS = [-np.inf, 0.30, 0.35, 0.40, 0.45, 0.47, 0.49, 0.50, 0.55, 0.60, np.inf]
ASK_LABELS = ["<=0.30", "0.30_0.35", "0.35_0.40", "0.40_0.45", "0.45_0.47", "0.47_0.49", "0.49_0.50", "0.50_0.55", "0.55_0.60", ">0.60"]
AGE_BINS = [-np.inf, 5, 10, 15, 30, 60, 120, 180, 240, 300]
AGE_LABELS = ["0_5s", "5_10s", "10_15s", "15_30s", "30_60s", "60_120s", "120_180s", "180_240s", "240_300s"]
SPREAD_BINS = [-np.inf, 0.01, 0.02, 0.03, 0.05, np.inf]
SPREAD_LABELS = ["0_0.01", "0.01_0.02", "0.02_0.03", "0.03_0.05", ">0.05"]
DEPTH_BINS = [-np.inf, 1, 5, 10, 25, 100, np.inf]
DEPTH_LABELS = ["0_1", "1_5", "5_10", "10_25", "25_100", ">100"]


def read_frame(path: Path) -> pd.DataFrame:
    if path.exists() and path.suffix.lower() == ".parquet":
        try:
            return pd.read_parquet(path)
        except ImportError as exc:
            sidecar = Path(str(path) + ".as.json")
            if sidecar.exists():
                return pd.read_json(sidecar, lines=True)
            raise exc
    sidecar = Path(str(path) + ".as.json")
    if sidecar.exists():
        return pd.read_json(sidecar, lines=True)
    return pd.read_csv(path)


def chronological_slice(ts: pd.Series | pd.Timestamp) -> pd.Series | str:
    def one(value: Any) -> str:
        if pd.isna(value):
            return "unknown"
        day = pd.Timestamp(value).tz_convert("UTC").date()
        if pd.Timestamp("2026-04-23").date() <= day <= pd.Timestamp("2026-04-30").date():
            return "early"
        if pd.Timestamp("2026-05-01").date() <= day <= pd.Timestamp("2026-05-08").date():
            return "main"
        if pd.Timestamp("2026-05-09").date() <= day <= pd.Timestamp("2026-05-11").date():
            return "fresh"
        return "out_of_named_range"

    if isinstance(ts, pd.Series):
        return ts.map(one)
    return one(ts)


def assign_bins(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "model_edge" in out.columns:
        edge = pd.to_numeric(out["model_edge"], errors="coerce")
    elif "predicted_edge" in out.columns:
        edge = pd.to_numeric(out["predicted_edge"], errors="coerce")
        out["model_edge"] = edge
    elif "edge" in out.columns:
        edge = pd.to_numeric(out["edge"], errors="coerce")
        out["model_edge"] = edge
    elif {"p_yes", "side", "entry_ask"}.issubset(out.columns):
        p_yes = pd.to_numeric(out["p_yes"], errors="coerce")
        p_side = np.where(out["side"].astype(str).str.upper().eq("YES"), p_yes, 1.0 - p_yes)
        edge = pd.Series(p_side, index=out.index) - pd.to_numeric(out["entry_ask"], errors="coerce")
        out["model_edge"] = edge
    else:
        edge = pd.Series(np.nan, index=out.index)
        out["model_edge"] = edge
    out["edge_bin"] = pd.cut(edge, EDGE_BINS, labels=EDGE_LABELS, right=False).astype("object").fillna("missing")

    ask = pd.to_numeric(out.get("entry_ask", out.get("selected_price")), errors="coerce")
    out["entry_ask"] = ask
    out["ask_bin"] = pd.cut(ask, ASK_BINS, labels=ASK_LABELS, right=True).astype("object").fillna("missing")

    age = pd.to_numeric(out.get("entry_age_sec", out.get("market_age_seconds")), errors="coerce")
    out["entry_age_sec"] = age
    out["market_age_bucket"] = pd.cut(age, AGE_BINS, labels=AGE_LABELS, right=False).astype("object").fillna("missing")

    if "market_start_ts" in out.columns:
        out["market_start_ts"] = pd.to_datetime(out["market_start_ts"], utc=True, errors="coerce")
        out["chronological_slice"] = chronological_slice(out["market_start_ts"])
        out["entry_day"] = out["market_start_ts"].dt.date.astype(str)
    elif "ts" in out.columns:
        out["ts"] = pd.to_datetime(out["ts"], utc=True, errors="coerce")
        out["chronological_slice"] = chronological_slice(out["ts"])
        out["entry_day"] = out["ts"].dt.date.astype(str)
    else:
        out["chronological_slice"] = "missing"
        out["entry_day"] = "missing"

    if "spread" in out.columns:
        spread = pd.to_numeric(out["spread"], errors="coerce")
        out["spread_bucket"] = pd.cut(spread, SPREAD_BINS, labels=SPREAD_LABELS, right=False).astype("object").fillna("missing")
    if "visible_ask_depth" in out.columns:
        depth = pd.to_numeric(out["visible_ask_depth"], errors="coerce")
        out["depth_bucket"] = pd.cut(depth, DEPTH_BINS, labels=DEPTH_LABELS, right=False).astype("object").fillna("missing")
    return out


def maybe_enrich_from_compact(trades: pd.DataFrame, compact_root: Path) -> tuple[pd.DataFrame, dict[str, Any]]:
    diagnostics: dict[str, Any] = {"compact_enrichment": "not_attempted", "optional_columns_added": []}
    ticks_path = compact_root / "book_ticks.parquet"
    windows_path = compact_root / "market_windows.parquet"
    out = trades.copy()
    if windows_path.exists():
        windows = read_frame(windows_path)
        keep = [c for c in ["market_key", "reference_price", "chainlink_close_price"] if c in windows.columns]
        if keep and "market_key" in out.columns:
            out = out.merge(windows[keep].drop_duplicates("market_key"), on="market_key", how="left")
    if not ticks_path.exists() or not {"market_key", "ts", "side"}.issubset(out.columns):
        return out, diagnostics
    ticks = read_frame(ticks_path)
    if not {"market_key", "ts", "side"}.issubset(ticks.columns):
        diagnostics["compact_enrichment"] = "book_ticks_missing_join_keys"
        return out, diagnostics
    out["ts"] = pd.to_datetime(out["ts"], utc=True, errors="coerce")
    ticks["ts"] = pd.to_datetime(ticks["ts"], utc=True, errors="coerce")
    ask_sz_cols = [c for c in ticks.columns if c.startswith("ask_sz_")]
    if ask_sz_cols and "visible_ask_depth" not in ticks.columns:
        ticks["visible_ask_depth"] = ticks[ask_sz_cols].apply(pd.to_numeric, errors="coerce").sum(axis=1)
    optional = [c for c in ["market_key", "ts", "side", "spread", "visible_ask_depth", "mid"] if c in ticks.columns]
    before_cols = set(out.columns)
    out = out.merge(ticks[optional].drop_duplicates(["market_key", "ts", "side"]), on=["market_key", "ts", "side"], how="left")
    diagnostics["compact_enrichment"] = "attempted"
    diagnostics["optional_columns_added"] = sorted(set(out.columns) - before_cols)
    return out, diagnostics


def filter_dates(df: pd.DataFrame, start_date: str | None, end_date: str | None) -> pd.DataFrame:
    if "market_start_ts" not in df.columns:
        return df
    out = df.copy()
    out["market_start_ts"] = pd.to_datetime(out["market_start_ts"], utc=True, errors="coerce")
    if start_date:
        out = out[out["market_start_ts"] >= pd.Timestamp(start_date, tz="UTC")]
    if end_date:
        out = out[out["market_start_ts"] < pd.Timestamp(end_date, tz="UTC") + pd.Timedelta(days=1)]
    return out


def aggregate_metrics(df: pd.DataFrame, group_cols: list[str]) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=group_cols)
    rows = []
    for keys, group in df.groupby(group_cols, dropna=False, sort=True):
        if not isinstance(keys, tuple):
            keys = (keys,)
        gross_cost = pd.to_numeric(group["gross_cost"], errors="coerce").sum()
        gross_pnl = pd.to_numeric(group["gross_pnl"], errors="coerce").sum()
        side_upper = group.get("side", pd.Series(dtype=str)).astype(str).str.upper()
        row = dict(zip(group_cols, keys))
        row.update(
            {
                "trade_count": int(len(group)),
                "market_count": int(group["market_key"].nunique()) if "market_key" in group.columns else int(len(group)),
                "gross_cost": float(gross_cost),
                "gross_payout": float(pd.to_numeric(group["gross_payout"], errors="coerce").sum()) if "gross_payout" in group.columns else np.nan,
                "gross_pnl": float(gross_pnl),
                "roi_on_filled_cost": float(gross_pnl / gross_cost) if gross_cost else np.nan,
                "win_rate": float(pd.to_numeric(group.get("win"), errors="coerce").mean()) if "win" in group.columns else np.nan,
                "avg_entry_ask": float(pd.to_numeric(group.get("entry_ask"), errors="coerce").mean()) if "entry_ask" in group.columns else np.nan,
                "avg_model_edge": float(pd.to_numeric(group.get("model_edge"), errors="coerce").mean()) if "model_edge" in group.columns else np.nan,
                "median_model_edge": float(pd.to_numeric(group.get("model_edge"), errors="coerce").median()) if "model_edge" in group.columns else np.nan,
                "avg_entry_age_sec": float(pd.to_numeric(group.get("entry_age_sec"), errors="coerce").mean()) if "entry_age_sec" in group.columns else np.nan,
                "fill_rate": float(pd.to_numeric(group.get("fill_rate"), errors="coerce").mean()) if "fill_rate" in group.columns else np.nan,
                "capacity_shortfall_rate": float(pd.to_numeric(group.get("capacity_shortfall"), errors="coerce").mean()) if "capacity_shortfall" in group.columns else np.nan,
                "avg_p_yes": float(pd.to_numeric(group.get("p_yes"), errors="coerce").mean()) if "p_yes" in group.columns else np.nan,
                "yes_trade_share": float(side_upper.eq("YES").mean()) if len(side_upper) else np.nan,
                "no_trade_share": float(side_upper.eq("NO").mean()) if len(side_upper) else np.nan,
            }
        )
        rows.append(row)
    return pd.DataFrame(rows)


def original_roi(df: pd.DataFrame) -> float:
    cost = pd.to_numeric(df.get("gross_cost"), errors="coerce").sum()
    pnl = pd.to_numeric(df.get("gross_pnl"), errors="coerce").sum()
    return float(pnl / cost) if cost else np.nan


def make_veto_report(df: pd.DataFrame, bucket_cols: list[str], min_markets_per_bucket: int) -> pd.DataFrame:
    original = aggregate_metrics(df.assign(_all="all"), ["_all"]).iloc[0].to_dict() if not df.empty else {}
    original_roi_value = float(original.get("roi_on_filled_cost", np.nan))
    rows = []
    for col in bucket_cols:
        if col not in df.columns:
            continue
        for value, removed in df.groupby(col, dropna=False):
            remaining = df[df[col].ne(value)].copy()
            removed_cost = pd.to_numeric(removed["gross_cost"], errors="coerce").sum()
            removed_pnl = pd.to_numeric(removed["gross_pnl"], errors="coerce").sum()
            remaining_cost = pd.to_numeric(remaining["gross_cost"], errors="coerce").sum()
            remaining_pnl = pd.to_numeric(remaining["gross_pnl"], errors="coerce").sum()
            remaining_roi = float(remaining_pnl / remaining_cost) if remaining_cost else np.nan
            removed_market_count = int(removed["market_key"].nunique()) if "market_key" in removed.columns else len(removed)
            slice_rois: dict[str, float] = {}
            for slice_name in ["early", "main", "fresh"]:
                slice_remaining = remaining[remaining["chronological_slice"].eq(slice_name)] if "chronological_slice" in remaining.columns else remaining.iloc[0:0]
                slice_rois[f"{slice_name}_remaining_roi"] = original_roi(slice_remaining) if not slice_remaining.empty else np.nan
            slice_removed_pnl = removed.groupby("chronological_slice")["gross_pnl"].sum() if "chronological_slice" in removed.columns else pd.Series(dtype=float)
            dominant_slice_share = float(slice_removed_pnl.abs().max() / slice_removed_pnl.abs().sum()) if slice_removed_pnl.abs().sum() else np.nan
            fresh_ok = True
            fresh_remaining = remaining[remaining["chronological_slice"].eq("fresh")] if "chronological_slice" in remaining.columns else remaining.iloc[0:0]
            fresh_original = df[df["chronological_slice"].eq("fresh")] if "chronological_slice" in df.columns else df.iloc[0:0]
            if fresh_remaining["market_key"].nunique() >= min_markets_per_bucket and fresh_original["market_key"].nunique() >= min_markets_per_bucket:
                fresh_ok = original_roi(fresh_remaining) >= original_roi(fresh_original) - 0.02
            row = {
                "factor": col,
                "bucket_value": value,
                "removed_trade_count": int(len(removed)),
                "removed_market_count": removed_market_count,
                "removed_gross_cost": float(removed_cost),
                "removed_gross_pnl": float(removed_pnl),
                "remaining_trade_count": int(len(remaining)),
                "remaining_gross_cost": float(remaining_cost),
                "remaining_gross_pnl": float(remaining_pnl),
                "remaining_roi": remaining_roi,
                "roi_delta_vs_original": float(remaining_roi - original_roi_value) if np.isfinite(remaining_roi) and np.isfinite(original_roi_value) else np.nan,
                "pnl_saved": float(-removed_pnl),
                "removed_win_rate": float(pd.to_numeric(removed.get("win"), errors="coerce").mean()) if "win" in removed.columns else np.nan,
                "removed_avg_edge": float(pd.to_numeric(removed.get("model_edge"), errors="coerce").mean()) if "model_edge" in removed.columns else np.nan,
                "removed_avg_ask": float(pd.to_numeric(removed.get("entry_ask"), errors="coerce").mean()) if "entry_ask" in removed.columns else np.nan,
                "removed_capacity_shortfall_rate": float(pd.to_numeric(removed.get("capacity_shortfall"), errors="coerce").mean()) if "capacity_shortfall" in removed.columns else np.nan,
                "dominant_removed_slice_abs_pnl_share": dominant_slice_share,
                "is_candidate_veto": bool(
                    removed_market_count >= min_markets_per_bucket
                    and len(remaining) > 0
                    and np.isfinite(remaining_roi)
                    and remaining_roi > original_roi_value
                    and (not np.isfinite(dominant_slice_share) or dominant_slice_share < 0.90)
                    and fresh_ok
                ),
            }
            row.update(slice_rois)
            rows.append(row)
    return pd.DataFrame(rows).sort_values(["is_candidate_veto", "roi_delta_vs_original", "pnl_saved"], ascending=[False, False, False]) if rows else pd.DataFrame()


def write_readme(path: Path, args: argparse.Namespace, manifest: dict[str, Any]) -> None:
    lines = [
        "Probability ROI attribution",
        "",
        "Offline research only. This reads existing replay artifacts and does not modify live trading behavior.",
        "The goal is execution-aware ROI attribution and candidate veto diagnostics, not Brier/log-loss ranking.",
        "",
        f"strategy_run_root={args.strategy_run_root}",
        f"compact_root={args.compact_root}",
        f"min_markets_per_bucket={args.min_markets_per_bucket}",
        "",
        f"trade_rows_loaded={manifest.get('trade_rows_loaded')}",
        f"trade_rows_used={manifest.get('trade_rows_used')}",
        f"missing_required_columns={manifest.get('missing_required_columns')}",
        f"missing_optional_columns={manifest.get('missing_optional_columns')}",
        "",
        "Candidate veto rows remove one bucket at a time and recompute remaining ROI.",
        "A veto is only marked as a candidate when it has enough markets, improves ROI, is not dominated by one chronological slice, and does not materially worsen fresh-slice ROI where enough fresh data exists.",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run(args: argparse.Namespace) -> dict[str, Any]:
    output_dir = Path(args.output_root) / args.run_name
    if output_dir.exists():
        if not args.overwrite:
            raise FileExistsError(f"{output_dir} exists; pass --overwrite")
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    trade_path = Path(args.strategy_run_root) / "trade_level_results.parquet"
    trades = read_frame(trade_path)
    rows_loaded = len(trades)
    trades = filter_dates(trades, args.start_date, args.end_date)
    trades, enrich_diag = maybe_enrich_from_compact(trades, Path(args.compact_root))
    source_cols_before_bins = set(trades.columns)
    trades = assign_bins(trades)

    required = ["market_key", "side", "winner_side", "entry_ask", "gross_cost", "gross_pnl"]
    missing_required = [c for c in required if c not in trades.columns]
    if missing_required:
        raise ValueError(f"trade_level_results is missing required columns: {missing_required}")
    optional = [
        "p_yes",
        "model_edge",
        "spread",
        "visible_ask_depth",
        "reference_price",
        "recent_return",
        "realized_vol",
        "sign_flip_rate",
        "shock_score",
    ]
    missing_optional = [c for c in optional if c not in source_cols_before_bins]

    if "spread_bucket" not in trades.columns and "spread" in trades.columns:
        trades = assign_bins(trades)
    if "depth_bucket" not in trades.columns and "visible_ask_depth" in trades.columns:
        trades = assign_bins(trades)

    output_specs = [
        ("roi_by_edge_bin.csv", ["edge_bin"]),
        ("roi_by_ask_bin.csv", ["ask_bin"]),
        ("roi_by_side.csv", ["side"]),
        ("roi_by_market_age_bucket.csv", ["market_age_bucket"]),
        ("roi_by_chronological_slice.csv", ["chronological_slice"]),
        ("roi_by_day.csv", ["entry_day"]),
        ("roi_by_edge_and_ask_bin.csv", ["edge_bin", "ask_bin"]),
        ("roi_by_side_and_edge_bin.csv", ["side", "edge_bin"]),
        ("roi_by_side_and_ask_bin.csv", ["side", "ask_bin"]),
        ("roi_by_slice_and_edge_bin.csv", ["chronological_slice", "edge_bin"]),
    ]
    conditional_specs = [
        ("roi_by_spread_bucket.csv", ["spread_bucket"]),
        ("roi_by_depth_bucket.csv", ["depth_bucket"]),
        ("roi_by_distance_from_reference.csv", ["distance_from_reference_bucket"]),
        ("roi_by_recent_return.csv", ["recent_return_bucket"]),
        ("roi_by_volatility_bucket.csv", ["volatility_bucket"]),
        ("roi_by_sign_flip_bucket.csv", ["sign_flip_bucket"]),
        ("roi_by_shock_bucket.csv", ["shock_bucket"]),
    ]
    written = []
    for filename, group_cols in output_specs + [(f, g) for f, g in conditional_specs if all(c in trades.columns for c in g)]:
        aggregate_metrics(trades, group_cols).to_csv(output_dir / filename, index=False)
        written.append(filename)

    veto_bucket_cols = [
        "edge_bin",
        "ask_bin",
        "side",
        "market_age_bucket",
        "chronological_slice",
        "spread_bucket",
        "depth_bucket",
        "distance_from_reference_bucket",
        "recent_return_bucket",
        "volatility_bucket",
        "sign_flip_bucket",
        "shock_bucket",
    ]
    veto = make_veto_report(trades, [c for c in veto_bucket_cols if c in trades.columns], args.min_markets_per_bucket)
    veto.to_csv(output_dir / "candidate_veto_report.csv", index=False)
    written.append("candidate_veto_report.csv")

    manifest = {
        "strategy_run_root": str(args.strategy_run_root),
        "compact_root": str(args.compact_root),
        "output_dir": str(output_dir),
        "trade_rows_loaded": int(rows_loaded),
        "trade_rows_used": int(len(trades)),
        "missing_required_columns": missing_required,
        "missing_optional_columns": missing_optional,
        "compact_enrichment": enrich_diag,
        "output_files": written + ["run_manifest.json", "README.txt"],
    }
    (output_dir / "run_manifest.json").write_text(json.dumps(manifest, indent=2, default=str), encoding="utf-8")
    write_readme(output_dir / "README.txt", args, manifest)
    return manifest


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="ROI attribution and candidate-veto diagnostics for BTC-5m probability-distribution replay artifacts.")
    parser.add_argument("--strategy-run-root", type=Path, required=True)
    parser.add_argument("--compact-root", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--run-name", required=True)
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    parser.add_argument("--binance-feature-path", type=Path)
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--min-markets-per-bucket", type=int, default=100)
    return parser


def main(argv: list[str] | None = None) -> int:
    manifest = run(build_parser().parse_args(argv))
    print(json.dumps(manifest, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
