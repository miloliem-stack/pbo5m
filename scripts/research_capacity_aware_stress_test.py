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


DEFAULT_OUTPUT_DIR = Path("artifacts/capacity_stress/quote_overlap_apr2026_2d_v1")
FIXED_STAKES = [1.0, 5.0, 10.0, 25.0, 50.0, 100.0]
CAPACITY_FRACTIONS = {
    "cap_frac_100pct": 1.0,
    "cap_frac_50pct": 0.50,
    "cap_frac_25pct": 0.25,
    "cap_frac_10pct": 0.10,
    "cap_frac_5pct": 0.05,
}
MAX_TRADE_CAPS = [25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0]
VISIBLE_DEPTH_HAIRCUTS = [1.0, 0.5, 0.25, 0.10]


def parse_csv_floats(value: str | None) -> list[float]:
    return [float(item.strip()) for item in str(value or "").split(",") if item.strip()]


def read_frame(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".parquet":
        try:
            return pd.read_parquet(path)
        except ImportError as exc:
            sidecar = path.with_suffix(path.suffix + ".as.json")
            if sidecar.exists():
                return pd.read_json(sidecar, lines=True)
            raise ImportError(f"Parquet support is unavailable for {path}; install pyarrow/fastparquet or provide CSV") from exc
    return pd.read_csv(path)


def write_optional_parquet(frame: pd.DataFrame, path: Path) -> bool:
    try:
        frame.to_parquet(path, index=False)
        return True
    except Exception:
        return False


def normalize_selected_entries(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    if "model_id" not in out.columns and "model" in out.columns:
        out["model_id"] = out["model"]
    if "market_key" not in out.columns and "prediction_market_key" in out.columns:
        out["market_key"] = out["prediction_market_key"]
    if "prediction_ts" not in out.columns and "decision_ts" in out.columns:
        out["prediction_ts"] = out["decision_ts"]
    if "market_age_seconds" not in out.columns and "decision_age" in out.columns:
        out["market_age_seconds"] = out["decision_age"]
    if "raw_entry_price" not in out.columns:
        out["raw_entry_price"] = out.get("adjusted_entry_price", out.get("selected_price", out.get("entry_price")))
    if "fee_rate" not in out.columns:
        out["fee_rate"] = 0.07
    if "win" not in out.columns:
        if "hit" in out.columns:
            out["win"] = out["hit"]
        elif "result_up" in out.columns and "side" in out.columns:
            out["win"] = np.where(out["side"].astype(str).str.upper().eq("YES"), pd.to_numeric(out["result_up"], errors="coerce").eq(1.0), pd.to_numeric(out["result_up"], errors="coerce").eq(0.0)).astype(float)
        else:
            raise ValueError("selected entries must contain win, hit, or result_up+side columns")
    required = ["model_id", "market_key", "prediction_ts", "market_age_seconds", "side", "raw_entry_price", "win"]
    missing = [col for col in required if col not in out.columns]
    if missing:
        raise ValueError(f"selected entries missing required columns {missing}; available columns: {list(frame.columns)}")
    out["prediction_ts"] = pd.to_datetime(out["prediction_ts"], utc=True, errors="coerce")
    out["market_age_seconds"] = pd.to_numeric(out["market_age_seconds"], errors="coerce")
    out["raw_entry_price"] = pd.to_numeric(out["raw_entry_price"], errors="coerce")
    out["win"] = pd.to_numeric(out["win"], errors="coerce")
    out["fee_rate"] = pd.to_numeric(out["fee_rate"], errors="coerce").fillna(0.07)
    for col in ["edge_threshold", "raw_edge", "cost_adjusted_edge", "fold_id", "slippage_bps"]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    if "label_source" not in out.columns:
        out["label_source"] = "existing_label"
    return out.dropna(subset=required).reset_index(drop=True)


def normalize_capacity(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    if "market_key" not in out.columns and "market_id" in out.columns:
        out["market_key"] = out["market_id"]
    if "prediction_ts" not in out.columns and "decision_ts" in out.columns:
        out["prediction_ts"] = out["decision_ts"]
    if "market_age_seconds" not in out.columns and "decision_age" in out.columns:
        out["market_age_seconds"] = out["decision_age"]
    required = ["market_key", "model_id", "market_age_seconds"]
    missing = [col for col in required if col not in out.columns]
    if missing:
        raise ValueError(f"capacity output missing required columns {missing}; available columns: {list(frame.columns)}")
    out["prediction_ts"] = pd.to_datetime(out["prediction_ts"], utc=True, errors="coerce") if "prediction_ts" in out.columns else pd.NaT
    out["market_age_seconds"] = pd.to_numeric(out["market_age_seconds"], errors="coerce")
    if "latency_ms" in out.columns:
        out["latency_ms"] = pd.to_numeric(out["latency_ms"], errors="coerce")
    for col in out.columns:
        if col.startswith("capacity_usdc") or col in {"max_fillable_usdc", "best_ask"}:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    return out.dropna(subset=["market_key", "model_id", "market_age_seconds"]).reset_index(drop=True)


def choose_join_keys(selected: pd.DataFrame, capacity: pd.DataFrame) -> list[str]:
    candidates = ["market_key", "model_id", "market_age_seconds", "side", "prediction_ts"]
    keys = [col for col in candidates if col in selected.columns and col in capacity.columns]
    if "market_key" not in keys or "model_id" not in keys or "market_age_seconds" not in keys:
        raise ValueError(f"Could not find stable join keys. selected={list(selected.columns)} capacity={list(capacity.columns)}")
    return keys


def join_selected_capacity(selected: pd.DataFrame, capacity: pd.DataFrame, capacity_col: str) -> tuple[pd.DataFrame, dict[str, Any]]:
    if capacity_col not in capacity.columns:
        raise ValueError(f"capacity column {capacity_col!r} not found. Available capacity columns: {[c for c in capacity.columns if c.startswith('capacity_usdc')]}")
    keys = choose_join_keys(selected, capacity)
    cap_cols = keys + [c for c in ["latency_ms", "execution_book_status", "book_lag_seconds", "best_ask", capacity_col, "max_fillable_usdc", "chainlink_terminal_margin_band"] if c in capacity.columns and c not in keys]
    cap = capacity[cap_cols].copy()
    joined = selected.merge(cap, on=keys, how="left", suffixes=("", "_capacity"), indicator=True)
    diagnostics = {
        "join_keys": keys,
        "selected_rows": int(len(selected)),
        "capacity_rows": int(len(capacity)),
        "joined_rows": int(len(joined)),
        "matched_rows": int(joined["_merge"].eq("both").sum()),
        "unmatched_selected_rows": int(joined["_merge"].eq("left_only").sum()),
        "capacity_col": capacity_col,
    }
    joined = joined.drop(columns=["_merge"])
    joined["reported_capacity_usdc"] = pd.to_numeric(joined[capacity_col], errors="coerce")
    return joined, diagnostics


def age_bucket(age: float) -> str:
    if pd.isna(age):
        return "missing"
    edges = [(0, 60), (60, 120), (120, 180), (180, 218), (218, 240), (240, 300)]
    for lo, hi in edges:
        if lo <= age < hi:
            return f"{lo}_{hi}"
    return "other"


def edge_bucket(edge: float) -> str:
    if pd.isna(edge):
        return "missing"
    bins = [(-np.inf, 0, "lt_0"), (0, 0.01, "0_1pct"), (0.01, 0.02, "1_2pct"), (0.02, 0.03, "2_3pct"), (0.03, 0.05, "3_5pct"), (0.05, 0.07, "5_7pct"), (0.07, 0.10, "7_10pct"), (0.10, np.inf, "10pct_plus")]
    for lo, hi, label in bins:
        if lo <= edge < hi:
            return label
    return "missing"


def capacity_bucket(capacity: float) -> str:
    if pd.isna(capacity):
        return "missing"
    edges = [(0, 0, "zero"), (0, 25, "0_25"), (25, 50, "25_50"), (50, 100, "50_100"), (100, 250, "100_250"), (250, 500, "250_500"), (500, 1000, "500_1000"), (1000, 2500, "1000_2500"), (2500, 5000, "2500_5000"), (5000, np.inf, "5000_plus")]
    for lo, hi, label in edges:
        if label == "zero" and capacity == 0:
            return label
        if lo < capacity <= hi:
            return label
    return "missing"


def max_drawdown(pnl: pd.Series) -> float:
    values = pd.to_numeric(pnl, errors="coerce").fillna(0.0).to_numpy()
    if values.size == 0:
        return 0.0
    equity = np.cumsum(values)
    peaks = np.maximum.accumulate(np.insert(equity, 0, 0.0))[1:]
    drawdowns = equity - peaks
    return float(drawdowns.min()) if drawdowns.size else 0.0


def concentration_metrics(frame: pd.DataFrame) -> dict[str, float | None]:
    if frame.empty:
        return {
            "top_1_trade_pnl_share": None,
            "top_5_trade_pnl_share": None,
            "top_10_trade_pnl_share": None,
            "pnl_without_top_1": None,
            "pnl_without_top_5": None,
            "pnl_without_top_10": None,
            "pnl_without_largest_capacity_1pct": None,
            "pnl_without_largest_capacity_5pct": None,
            "pnl_without_largest_capacity_10pct": None,
        }
    pnl = pd.to_numeric(frame["scenario_pnl"], errors="coerce").fillna(0.0)
    total = float(pnl.sum())
    winners = pnl.sort_values(ascending=False)
    row: dict[str, float | None] = {}
    for n in [1, 5, 10]:
        top_sum = float(winners.head(n).sum())
        row[f"top_{n}_trade_pnl_share"] = top_sum / total if abs(total) > 1e-12 else None
        row[f"pnl_without_top_{n}"] = float(total - top_sum)
    by_capacity = frame.assign(_cap=pd.to_numeric(frame["effective_capacity_usdc"], errors="coerce").fillna(0.0)).sort_values("_cap", ascending=False)
    for pct in [1, 5, 10]:
        drop_n = max(1, int(np.ceil(len(by_capacity) * pct / 100.0)))
        row[f"pnl_without_largest_capacity_{pct}pct"] = float(by_capacity.iloc[drop_n:]["scenario_pnl"].sum())
    return row


def score_sized_entries(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    out["scenario_stake"] = pd.to_numeric(out["scenario_stake"], errors="coerce").fillna(0.0).clip(lower=0.0)
    out["adjusted_entry_price"] = pd.to_numeric(out.get("adjusted_entry_price", out["raw_entry_price"]), errors="coerce").fillna(out["raw_entry_price"])
    out["fee_rate"] = pd.to_numeric(out.get("fee_rate", 0.07), errors="coerce").fillna(0.07)
    out["scenario_shares"] = np.where(out["adjusted_entry_price"] > 0, out["scenario_stake"] / out["adjusted_entry_price"], 0.0)
    out["scenario_fee_per_share"] = out["fee_rate"] * out["adjusted_entry_price"] * (1.0 - out["adjusted_entry_price"])
    out["scenario_fee"] = out["scenario_shares"] * out["scenario_fee_per_share"]
    out["scenario_total_cost"] = out["scenario_stake"] + out["scenario_fee"]
    out["scenario_gross_payout"] = np.where(out["win"].eq(1.0), out["scenario_shares"], 0.0)
    out["scenario_pnl"] = out["scenario_gross_payout"] - out["scenario_total_cost"]
    out["scenario_trade_roi"] = np.where(out["scenario_total_cost"] > 0, out["scenario_pnl"] / out["scenario_total_cost"], np.nan)
    return out


def scenario_frames(joined: pd.DataFrame, *, capacity_col: str, max_caps: list[float], haircuts: list[float]) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    base = joined.copy()
    base["reported_capacity_usdc"] = pd.to_numeric(base["reported_capacity_usdc"], errors="coerce")
    for missing_mode in ["full_join_conservative_zero_missing", "full_depth_only"]:
        if missing_mode == "full_depth_only":
            source = base[base["reported_capacity_usdc"].notna() & (base["reported_capacity_usdc"] > 0)].copy()
            if "execution_book_status" in source.columns:
                source = source[source["execution_book_status"].eq("ok")]
        else:
            source = base.copy()
            source["reported_capacity_usdc"] = source["reported_capacity_usdc"].fillna(0.0)
        for stake in FIXED_STAKES:
            frame = source.copy()
            frame["scenario_name"] = f"fixed_{int(stake)}"
            frame["scenario_type"] = "fixed"
            frame["capacity_fraction"] = np.nan
            frame["max_trade_cap"] = stake
            frame["visible_depth_haircut"] = 1.0
            frame["missing_capacity_mode"] = missing_mode
            frame["effective_capacity_usdc"] = frame["reported_capacity_usdc"]
            frame["scenario_stake"] = stake
            frames.append(score_sized_entries(frame))
        for haircut in haircuts:
            effective_capacity = source["reported_capacity_usdc"] * haircut
            for name, frac in CAPACITY_FRACTIONS.items():
                for max_cap in max_caps:
                    frame = source.copy()
                    frame["scenario_name"] = name
                    frame["scenario_type"] = "capacity_fraction"
                    frame["capacity_fraction"] = frac
                    frame["max_trade_cap"] = max_cap
                    frame["visible_depth_haircut"] = haircut
                    frame["missing_capacity_mode"] = missing_mode
                    frame["effective_capacity_usdc"] = effective_capacity
                    frame["scenario_stake"] = np.minimum(frac * effective_capacity, max_cap)
                    frames.append(score_sized_entries(frame))
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def iter_scored_scenarios(joined: pd.DataFrame, *, max_caps: list[float], haircuts: list[float]):
    base = joined.copy()
    base["reported_capacity_usdc"] = pd.to_numeric(base["reported_capacity_usdc"], errors="coerce")
    for missing_mode in ["full_join_conservative_zero_missing", "full_depth_only"]:
        if missing_mode == "full_depth_only":
            source = base[base["reported_capacity_usdc"].notna() & (base["reported_capacity_usdc"] > 0)].copy()
            if "execution_book_status" in source.columns:
                source = source[source["execution_book_status"].eq("ok")]
        else:
            source = base.copy()
            source["reported_capacity_usdc"] = source["reported_capacity_usdc"].fillna(0.0)
        for stake in FIXED_STAKES:
            frame = source.copy()
            frame["scenario_name"] = f"fixed_{int(stake)}"
            frame["scenario_type"] = "fixed"
            frame["capacity_fraction"] = np.nan
            frame["max_trade_cap"] = stake
            frame["visible_depth_haircut"] = 1.0
            frame["missing_capacity_mode"] = missing_mode
            frame["effective_capacity_usdc"] = frame["reported_capacity_usdc"]
            frame["scenario_stake"] = stake
            yield score_sized_entries(frame)
        for haircut in haircuts:
            effective_capacity = source["reported_capacity_usdc"] * haircut
            for name, frac in CAPACITY_FRACTIONS.items():
                for max_cap in max_caps:
                    frame = source.copy()
                    frame["scenario_name"] = name
                    frame["scenario_type"] = "capacity_fraction"
                    frame["capacity_fraction"] = frac
                    frame["max_trade_cap"] = max_cap
                    frame["visible_depth_haircut"] = haircut
                    frame["missing_capacity_mode"] = missing_mode
                    frame["effective_capacity_usdc"] = effective_capacity
                    frame["scenario_stake"] = np.minimum(frac * effective_capacity, max_cap)
                    yield score_sized_entries(frame)


def summarize(frame: pd.DataFrame, group_cols: list[str]) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame()
    rows = []
    sort_cols = [col for col in ["prediction_ts", "market_key", "model_id"] if col in frame.columns]
    for keys, group in frame.groupby(group_cols, dropna=False):
        if not isinstance(keys, tuple):
            keys = (keys,)
        positive = group[group["scenario_stake"] > 0].copy()
        ordered = positive.sort_values(sort_cols) if sort_cols else positive
        row = dict(zip(group_cols, keys))
        total_stake = float(positive["scenario_stake"].sum())
        total_pnl = float(positive["scenario_pnl"].sum())
        row.update(
            {
                "rows": int(len(group)),
                "trades_with_positive_stake": int(len(positive)),
                "markets": int(positive["market_key"].nunique()) if "market_key" in positive else int(len(positive)),
                "total_stake": total_stake,
                "total_pnl": total_pnl,
                "roi": total_pnl / total_stake if total_stake else np.nan,
                "hit_rate": float(positive["win"].mean()) if len(positive) else np.nan,
                "average_stake": float(positive["scenario_stake"].mean()) if len(positive) else 0.0,
                "median_stake": float(positive["scenario_stake"].median()) if len(positive) else 0.0,
                "p95_stake": float(np.nanpercentile(positive["scenario_stake"], 95)) if len(positive) else 0.0,
                "max_stake": float(positive["scenario_stake"].max()) if len(positive) else 0.0,
                "average_pnl": float(positive["scenario_pnl"].mean()) if len(positive) else 0.0,
                "median_pnl": float(positive["scenario_pnl"].median()) if len(positive) else 0.0,
                "max_drawdown": max_drawdown(ordered["scenario_pnl"]) if len(ordered) else 0.0,
            }
        )
        row.update(concentration_metrics(positive))
        rows.append(row)
    return pd.DataFrame(rows)


def concentration_table(frame: pd.DataFrame) -> pd.DataFrame:
    group_cols = ["missing_capacity_mode", "scenario_name", "visible_depth_haircut", "max_trade_cap", "model_id"]
    out = summarize(frame, group_cols)
    keep = group_cols + [c for c in out.columns if c.startswith("top_") or c.startswith("pnl_without")]
    return out[keep] if not out.empty else out


def add_breakdown_columns(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    if "age_bucket" not in out.columns:
        out["age_bucket"] = out["market_age_seconds"].map(age_bucket)
    edge_col = "cost_adjusted_edge" if "cost_adjusted_edge" in out.columns else "raw_edge"
    out["edge_bucket"] = pd.to_numeric(out.get(edge_col), errors="coerce").map(edge_bucket)
    out["capacity_bucket"] = pd.to_numeric(out["reported_capacity_usdc"], errors="coerce").map(capacity_bucket)
    if "prediction_ts" in out.columns:
        ts = pd.to_datetime(out["prediction_ts"], utc=True, errors="coerce")
        out["entry_date"] = ts.dt.date.astype(str)
        out["entry_hour"] = ts.dt.strftime("%Y-%m-%d %H:00")
    return out


def write_readme(path: Path, args: argparse.Namespace, diagnostics: dict[str, Any]) -> None:
    lines = [
        "Capacity-aware stress test",
        "",
        "Offline research only. This does not change live bot behavior and does not introduce a live strategy rule.",
        "",
        f"selected_entries={args.selected_entries}",
        f"capacity_output={args.capacity_output}",
        f"capacity_col={args.capacity_col}",
        f"join_keys={diagnostics.get('join_keys')}",
        "",
        "Stake formulas:",
        "- fixed_N: scenario_stake = N.",
        "- cap_frac_Xpct: scenario_stake = min(capacity_fraction * reported_capacity_usdc * visible_depth_haircut, max_trade_cap).",
        "- Missing-capacity view full_join_conservative_zero_missing fills missing capacity with zero.",
        "- Missing-capacity view full_depth_only keeps rows with positive reported capacity and ok execution books where available.",
        "",
        "PnL formula:",
        "- Binary buy payoff: winning contract pays 1, losing contract pays 0.",
        "- shares = scenario_stake / adjusted_entry_price.",
        "- fee_per_share = fee_rate * adjusted_entry_price * (1 - adjusted_entry_price).",
        "- pnl = winning_shares - scenario_stake - fee.",
        "",
        "Caveats:",
        "- Visible-depth capacity is not guaranteed live executable liquidity.",
        "- This rescales already-selected entries; it does not retrain or reselect models.",
        "- Capacity comes from recorded CLOB snapshots and inherits recorder/schema quality limits.",
        "",
        f"selected_rows={diagnostics.get('selected_rows')}",
        f"capacity_rows={diagnostics.get('capacity_rows')}",
        f"joined_rows={diagnostics.get('joined_rows')}",
        f"unmatched_selected_rows={diagnostics.get('unmatched_selected_rows')}",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run(args: argparse.Namespace) -> dict[str, Any]:
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    selected = normalize_selected_entries(read_frame(Path(args.selected_entries)))
    capacity = normalize_capacity(read_frame(Path(args.capacity_output)))
    joined, diagnostics = join_selected_capacity(selected, capacity, args.capacity_col)
    joined = add_breakdown_columns(joined)
    max_caps = parse_csv_floats(args.max_trade_caps)
    haircuts = parse_csv_floats(args.visible_depth_haircuts)
    group_cols = ["missing_capacity_mode", "scenario_name", "scenario_type", "capacity_fraction", "max_trade_cap", "visible_depth_haircut", "model_id", "label_source"]
    summary_parts: list[pd.DataFrame] = []
    concentration_parts: list[pd.DataFrame] = []
    capacity_parts: list[pd.DataFrame] = []
    age_parts: list[pd.DataFrame] = []
    edge_parts: list[pd.DataFrame] = []
    side_parts: list[pd.DataFrame] = []
    margin_parts: list[pd.DataFrame] = []
    date_parts: list[pd.DataFrame] = []
    hour_parts: list[pd.DataFrame] = []
    sample_parts: list[pd.DataFrame] = []
    scenario_count = 0
    for scored in iter_scored_scenarios(joined, max_caps=max_caps, haircuts=haircuts):
        scenario_count += 1
        scored = add_breakdown_columns(scored)
        summary_parts.append(summarize(scored, group_cols))
        concentration_parts.append(concentration_table(scored))
        capacity_parts.append(summarize(scored, group_cols + ["capacity_bucket"]))
        age_parts.append(summarize(scored, group_cols + ["age_bucket"]))
        edge_parts.append(summarize(scored, group_cols + ["edge_bucket"]))
        if "side" in scored.columns:
            side_parts.append(summarize(scored, group_cols + ["side"]))
        if "chainlink_terminal_margin_band" in scored.columns:
            margin_parts.append(summarize(scored, group_cols + ["chainlink_terminal_margin_band"]))
        date_parts.append(summarize(scored, group_cols + ["entry_date"]))
        hour_parts.append(summarize(scored, group_cols + ["entry_hour"]))
        if sum(len(x) for x in sample_parts) < args.sample_rows:
            sample_cols = [c for c in ["market_key", "model_id", "prediction_ts", "market_age_seconds", "latency_ms", "side", "label_source", "scenario_name", "max_trade_cap", "visible_depth_haircut", "reported_capacity_usdc", "scenario_stake", "win", "scenario_pnl"] if c in scored.columns]
            remaining = args.sample_rows - sum(len(x) for x in sample_parts)
            sample_parts.append(scored[sample_cols].head(remaining))

    def concat(parts: list[pd.DataFrame]) -> pd.DataFrame:
        return pd.concat([p for p in parts if not p.empty], ignore_index=True) if parts else pd.DataFrame()

    summary = concat(summary_parts)
    summary.to_csv(output_dir / "capacity_stress_summary.csv", index=False)
    concat(concentration_parts).to_csv(output_dir / "capacity_stress_concentration.csv", index=False)
    concat(capacity_parts).to_csv(output_dir / "capacity_stress_by_capacity_bucket.csv", index=False)
    concat(age_parts).to_csv(output_dir / "capacity_stress_by_market_age_bucket.csv", index=False)
    concat(edge_parts).to_csv(output_dir / "capacity_stress_by_edge_bucket.csv", index=False)
    concat(side_parts).to_csv(output_dir / "capacity_stress_by_side.csv", index=False)
    concat(margin_parts).to_csv(output_dir / "capacity_stress_by_terminal_margin_bucket.csv", index=False)
    concat(date_parts).to_csv(output_dir / "capacity_stress_by_date.csv", index=False)
    concat(hour_parts).to_csv(output_dir / "capacity_stress_by_hour.csv", index=False)
    sample = concat(sample_parts)
    sample.to_csv(output_dir / "capacity_stress_scored_sample.csv", index=False)
    write_optional_parquet(sample, output_dir / "capacity_stress_scored_sample.parquet")
    diagnostics.update(
        {
            "output_dir": str(output_dir),
            "scenario_count": int(scenario_count),
            "summary_rows": int(len(summary)),
            "max_trade_caps": max_caps,
            "visible_depth_haircuts": haircuts,
            "fixed_stakes": FIXED_STAKES,
            "capacity_fractions": CAPACITY_FRACTIONS,
        }
    )
    (output_dir / "capacity_stress_join_diagnostics.json").write_text(json.dumps(diagnostics, indent=2, default=str), encoding="utf-8")
    write_readme(output_dir / "capacity_stress_readme.txt", args, diagnostics)
    return diagnostics


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Offline capacity-aware stake sizing stress test for selected BTC-5m edge replay entries.")
    parser.add_argument("--selected-entries", type=Path, required=True)
    parser.add_argument("--capacity-output", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--capacity-col", default="capacity_usdc_at_edge_10")
    parser.add_argument("--max-trade-caps", default=",".join(f"{x:g}" for x in MAX_TRADE_CAPS))
    parser.add_argument("--visible-depth-haircuts", default=",".join(f"{x:g}" for x in VISIBLE_DEPTH_HAIRCUTS))
    parser.add_argument("--sample-rows", type=int, default=10000)
    return parser


def main(argv: list[str] | None = None) -> int:
    diagnostics = run(build_parser().parse_args(argv))
    print(json.dumps(diagnostics, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
