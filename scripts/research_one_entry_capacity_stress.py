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

from scripts import research_capacity_aware_stress_test as stress


DEFAULT_SELECTED_ENTRIES = Path("artifacts/probability_edge_replay/quote_overlap_apr2026_2d_quotes_v1_label_source/selected_first_entries.parquet")
DEFAULT_CAPACITY_OUTPUT = Path("artifacts/capacity_curve/quote_overlap_apr2026_2d_v1_dedup/capacity_per_market_dedup.parquet")
DEFAULT_OUTPUT_DIR = Path("artifacts/capacity_stress_one_entry/quote_overlap_apr2026_2d_v1")
ENTRY_POLICIES = [
    "first_entry",
    "first_positive_capacity_entry",
    "first_entry_after_60s",
    "first_entry_after_90s",
    "max_edge_entry",
    "max_capacity_adjusted_ev_entry",
]


def edge_series(frame: pd.DataFrame) -> pd.Series:
    for col in ["cost_adjusted_edge", "raw_edge", "predicted_edge"]:
        if col in frame.columns:
            return pd.to_numeric(frame[col], errors="coerce").fillna(-np.inf)
    return pd.Series(0.0, index=frame.index)


def capacity_join_keys(selected: pd.DataFrame, capacity: pd.DataFrame) -> list[str]:
    return stress.choose_join_keys(selected, capacity)


def dedupe_capacity_for_join(capacity: pd.DataFrame, join_keys: list[str], capacity_col: str) -> tuple[pd.DataFrame, dict[str, Any], pd.DataFrame]:
    missing = [key for key in join_keys if key not in capacity.columns]
    if missing:
        raise ValueError(f"capacity missing join keys {missing}")
    frame = capacity.copy().reset_index(names="_source_index")
    frame["_valid_depth_rank"] = 1
    if "execution_book_status" in frame.columns:
        frame["_valid_depth_rank"] = np.where(frame["execution_book_status"].eq("ok"), 0, 1)
    frame["_parse_rank"] = 0
    if "book_parse_status" in frame.columns:
        frame["_parse_rank"] = np.where(frame["book_parse_status"].isin(["ok_full_depth", "ok_top_of_book_only", "ok"]), 0, 1)
    age_col = next((col for col in ["book_lag_seconds", "quote_lag_seconds", "execution_book_lag_seconds"] if col in frame.columns), None)
    frame["_abs_book_age"] = pd.to_numeric(frame[age_col], errors="coerce").abs() if age_col else 0.0
    sort_cols = join_keys + ["_valid_depth_rank", "_parse_rank", "_abs_book_age", "_source_index"]
    unique = frame.sort_values(sort_cols, kind="mergesort").drop_duplicates(join_keys, keep="first")
    dupes = frame.groupby(join_keys, dropna=False).size().rename("source_capacity_rows").reset_index()
    duplicate_groups = dupes[dupes["source_capacity_rows"] > 1]
    diagnostics = {
        "capacity_rows_before_dedupe": int(len(capacity)),
        "capacity_rows_after_dedupe": int(len(unique)),
        "duplicate_capacity_key_groups": int(len(duplicate_groups)),
        "duplicate_capacity_rows_removed": int(len(capacity) - len(unique)),
        "dedupe_order": ["prefer execution_book_status == ok", "prefer ok book_parse_status if present", "prefer lowest absolute book/quote age if present", "prefer earliest source row index"],
        "capacity_col": capacity_col,
    }
    drop_cols = [c for c in unique.columns if c.startswith("_")]
    return unique.drop(columns=drop_cols).reset_index(drop=True), diagnostics, duplicate_groups


def join_without_expansion(selected: pd.DataFrame, capacity_unique: pd.DataFrame, join_keys: list[str], capacity_col: str) -> tuple[pd.DataFrame, dict[str, Any]]:
    cap_cols = join_keys + [c for c in ["latency_ms", "execution_book_status", "book_lag_seconds", "best_ask", capacity_col, "max_fillable_usdc", "chainlink_terminal_margin_band"] if c in capacity_unique.columns and c not in join_keys]
    joined = selected.merge(capacity_unique[cap_cols], on=join_keys, how="left", suffixes=("", "_capacity"), indicator=True)
    diagnostics = {
        "join_keys": join_keys,
        "selected_rows": int(len(selected)),
        "capacity_unique_rows": int(len(capacity_unique)),
        "joined_rows": int(len(joined)),
        "matched_rows": int(joined["_merge"].eq("both").sum()),
        "unmatched_selected_rows": int(joined["_merge"].eq("left_only").sum()),
    }
    if len(joined) > len(selected):
        raise ValueError(f"capacity join expanded selected rows after dedupe: selected={len(selected)} joined={len(joined)} join_keys={join_keys}")
    joined = joined.drop(columns=["_merge"])
    joined["reported_capacity_usdc"] = pd.to_numeric(joined[capacity_col], errors="coerce")
    joined["_candidate_row_order"] = np.arange(len(joined))
    return joined, diagnostics


def apply_missing_capacity_mode(frame: pd.DataFrame, mode: str) -> pd.DataFrame:
    out = frame.copy()
    out["reported_capacity_usdc"] = pd.to_numeric(out["reported_capacity_usdc"], errors="coerce")
    if mode == "full_join_conservative_zero_missing":
        out["reported_capacity_usdc"] = out["reported_capacity_usdc"].fillna(0.0)
        return out
    if mode == "full_depth_only":
        out = out[out["reported_capacity_usdc"].notna() & (out["reported_capacity_usdc"] > 0)].copy()
        if "execution_book_status" in out.columns:
            out = out[out["execution_book_status"].eq("ok")]
        return out
    raise ValueError(f"unknown missing capacity mode: {mode}")


def select_one_entry(frame: pd.DataFrame, policy: str) -> tuple[pd.DataFrame, dict[str, Any]]:
    if policy not in ENTRY_POLICIES:
        raise ValueError(f"unknown entry policy: {policy}")
    source = frame.copy()
    candidate_rows = int(len(source))
    before_markets = int(source.groupby(["market_key", "model_id", "label_source"], dropna=False).ngroups) if not source.empty else 0
    if policy == "first_positive_capacity_entry":
        source = source[source["reported_capacity_usdc"] > 0].copy()
    elif policy == "first_entry_after_60s":
        source = source[source["market_age_seconds"] >= 60].copy()
    elif policy == "first_entry_after_90s":
        source = source[source["market_age_seconds"] >= 90].copy()
    source["_edge_for_selection"] = edge_series(source)
    source["_capacity_ev_for_selection"] = source["_edge_for_selection"] * pd.to_numeric(source["reported_capacity_usdc"], errors="coerce").fillna(0.0)
    if policy in {"first_entry", "first_positive_capacity_entry", "first_entry_after_60s", "first_entry_after_90s"}:
        sorted_frame = source.sort_values(["prediction_ts", "market_age_seconds", "_candidate_row_order"], ascending=[True, True, True], kind="mergesort")
    elif policy == "max_edge_entry":
        sorted_frame = source.sort_values(["_edge_for_selection", "prediction_ts", "market_age_seconds", "_candidate_row_order"], ascending=[False, True, True, True], kind="mergesort")
    else:
        sorted_frame = source.sort_values(["_capacity_ev_for_selection", "prediction_ts", "market_age_seconds", "_candidate_row_order"], ascending=[False, True, True, True], kind="mergesort")
    selected = sorted_frame.drop_duplicates(["market_key", "model_id", "label_source"], keep="first").copy()
    candidate_counts = source.groupby(["model_id", "label_source"], dropna=False).size().rename("_candidate_rows_before_one_entry_filter").reset_index()
    selected_counts = selected.groupby(["model_id", "label_source"], dropna=False).size().rename("_selected_trade_rows_after_one_entry_filter").reset_index()
    selected = selected.merge(candidate_counts, on=["model_id", "label_source"], how="left").merge(selected_counts, on=["model_id", "label_source"], how="left")
    group_sizes = selected.groupby(["market_key", "model_id", "label_source"], dropna=False).size() if not selected.empty else pd.Series(dtype=int)
    max_group = int(group_sizes.max()) if len(group_sizes) else 0
    if max_group > 1:
        raise AssertionError("one-entry invariant violated after selection")
    diagnostics = {
        "entry_selection_policy": policy,
        "candidate_rows": candidate_rows,
        "unique_markets_before_selection": before_markets,
        "selected_rows_after_selection": int(len(selected)),
        "selected_rows_per_market_max": max_group,
        "markets_skipped_due_to_no_eligible_positive_capacity_entry": int(before_markets - len(selected)) if policy == "first_positive_capacity_entry" else 0,
        "median_selected_market_age_seconds": float(selected["market_age_seconds"].median()) if len(selected) else np.nan,
        "yes_selected": int(selected["side"].astype(str).str.upper().eq("YES").sum()) if "side" in selected else 0,
        "no_selected": int(selected["side"].astype(str).str.upper().eq("NO").sum()) if "side" in selected else 0,
        "median_selected_capacity_usdc": float(selected["reported_capacity_usdc"].median()) if len(selected) else np.nan,
    }
    return selected.drop(columns=["_edge_for_selection", "_capacity_ev_for_selection"], errors="ignore"), diagnostics


def iter_one_entry_scenarios(selected: pd.DataFrame, *, max_caps: list[float], haircuts: list[float]):
    for stake in stress.FIXED_STAKES:
        frame = selected.copy()
        price = pd.to_numeric(frame["adjusted_entry_price"] if "adjusted_entry_price" in frame.columns else frame["raw_entry_price"], errors="coerce")
        frame["scenario_name"] = f"fixed_{int(stake)}"
        frame["scenario_type"] = "fixed"
        frame["capacity_fraction"] = np.nan
        frame["max_trade_cap"] = stake
        frame["visible_depth_haircut"] = 1.0
        frame["effective_capacity_usdc"] = frame["reported_capacity_usdc"]
        frame["scenario_stake"] = np.where((price > 0) & (price < 1), stake, 0.0)
        yield stress.score_sized_entries(frame)
    for haircut in haircuts:
        effective_capacity = selected["reported_capacity_usdc"] * haircut
        for name, frac in stress.CAPACITY_FRACTIONS.items():
            for max_cap in max_caps:
                frame = selected.copy()
                price = pd.to_numeric(frame["adjusted_entry_price"] if "adjusted_entry_price" in frame.columns else frame["raw_entry_price"], errors="coerce")
                frame["scenario_name"] = name
                frame["scenario_type"] = "capacity_fraction"
                frame["capacity_fraction"] = frac
                frame["max_trade_cap"] = max_cap
                frame["visible_depth_haircut"] = haircut
                frame["effective_capacity_usdc"] = effective_capacity
                frame["scenario_stake"] = np.where((price > 0) & (price < 1), np.minimum(frac * effective_capacity, max_cap), 0.0)
                yield stress.score_sized_entries(frame)


def summarize_one_entry(frame: pd.DataFrame, group_cols: list[str]) -> pd.DataFrame:
    out = stress.summarize(frame, group_cols)
    if out.empty:
        return out
    out = out.rename(columns={"trades_with_positive_stake": "markets_with_positive_stake", "markets": "unique_markets"})
    count_cols = ["_candidate_rows_before_one_entry_filter", "_selected_trade_rows_after_one_entry_filter"]
    count_source = frame[group_cols + count_cols].drop_duplicates(group_cols)
    out = out.merge(count_source, on=group_cols, how="left")
    out = out.rename(columns={"_candidate_rows_before_one_entry_filter": "candidate_rows_before_one_entry_filter", "_selected_trade_rows_after_one_entry_filter": "selected_trade_rows_after_one_entry_filter"})
    ordered_cols = group_cols + [
        "candidate_rows_before_one_entry_filter",
        "selected_trade_rows_after_one_entry_filter",
        "unique_markets",
        "markets_with_positive_stake",
    ]
    rest = [c for c in out.columns if c not in ordered_cols]
    return out[ordered_cols + rest]


def selection_diagnostics_for(selected: pd.DataFrame, base_diag: dict[str, Any]) -> dict[str, Any]:
    diag = dict(base_diag)
    if selected.empty:
        diag.update({"p10_capacity_usdc": np.nan, "p90_capacity_usdc": np.nan})
    else:
        diag.update(
            {
                "p10_capacity_usdc": float(np.nanpercentile(selected["reported_capacity_usdc"], 10)),
                "p90_capacity_usdc": float(np.nanpercentile(selected["reported_capacity_usdc"], 90)),
            }
        )
    return diag


def write_readme(path: Path, args: argparse.Namespace, diagnostics: dict[str, Any]) -> None:
    text = f"""One-entry capacity stress test

Offline research only. This does not change live bot behavior and does not add a strategy rule.

This run imposes at most one executed entry per market_key/model_id/label_source before scenario expansion. It does not allow one YES and one NO in the same market for the headline grouping, and it does not allow repeated threshold firing inside the same 5-minute market.

The prior dense replay-stream capacity stress rescaled every selected candidate row and should not be interpreted as live executable repeated-entry behavior.

Inputs:
- selected_entries={args.selected_entries}
- capacity_output={args.capacity_output}

Join:
- join_keys={diagnostics.get('join_keys')}
- capacity_deduped={diagnostics.get('duplicate_capacity_rows_removed', 0) > 0}
- capacity_dedupe_order={diagnostics.get('dedupe_order')}
- joined_rows={diagnostics.get('joined_rows')} selected_rows={diagnostics.get('selected_rows')}

Stake formulas:
- fixed_N: scenario_stake = N when adjusted_entry_price is valid.
- cap_frac_Xpct: scenario_stake = min(capacity_fraction * reported_capacity_usdc * visible_depth_haircut, max_trade_cap).

PnL formula:
- winning binary contract pays 1, losing contract pays 0.
- shares = scenario_stake / adjusted_entry_price.
- fee_per_share = fee_rate * adjusted_entry_price * (1 - adjusted_entry_price).
- pnl = winning_shares - scenario_stake - fee.

Missing capacity modes:
- full_join_conservative_zero_missing: missing capacity becomes zero.
- full_depth_only: keeps only positive-capacity rows and ok execution books where available.

Caveats:
- Visible book depth is not guaranteed live executable liquidity.
- Queue position, adverse selection, partial fills, latency, and disappearing liquidity are not fully modeled here.
- max_edge_entry and max_capacity_adjusted_ev_entry are optimistic hindsight selection policies.

Recommended conservative headline slice:
- label_source=chainlink
- entry_selection_policy=first_positive_capacity_entry
- scenario=cap_frac_10pct
- visible_depth_haircut=0.25
- max_trade_cap in {{100, 250, 500}}
"""
    path.write_text(text, encoding="utf-8")


def run(args: argparse.Namespace) -> dict[str, Any]:
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    selected = stress.normalize_selected_entries(stress.read_frame(Path(args.selected_entries)))
    capacity = stress.normalize_capacity(stress.read_frame(Path(args.capacity_output)))
    join_keys = capacity_join_keys(selected, capacity)
    capacity_unique, dedupe_diag, duplicate_groups = dedupe_capacity_for_join(capacity, join_keys, args.capacity_col)
    duplicate_groups.to_csv(output_dir / "one_entry_capacity_duplicate_capacity_keys.csv", index=False)
    joined, join_diag = join_without_expansion(selected, capacity_unique, join_keys, args.capacity_col)
    joined = stress.add_breakdown_columns(joined)
    max_caps = stress.parse_csv_floats(args.max_trade_caps)
    haircuts = stress.parse_csv_floats(args.visible_depth_haircuts)
    policies = [p.strip() for p in args.entry_selection_policies.split(",") if p.strip()]
    group_cols = ["label_source", "model_id", "missing_capacity_mode", "entry_selection_policy", "scenario_name", "scenario_type", "max_trade_cap", "visible_depth_haircut"]

    summary_parts: list[pd.DataFrame] = []
    concentration_parts: list[pd.DataFrame] = []
    date_parts: list[pd.DataFrame] = []
    age_parts: list[pd.DataFrame] = []
    capacity_parts: list[pd.DataFrame] = []
    edge_parts: list[pd.DataFrame] = []
    margin_parts: list[pd.DataFrame] = []
    selection_diags: list[dict[str, Any]] = []

    for missing_mode in ["full_join_conservative_zero_missing", "full_depth_only"]:
        mode_frame = apply_missing_capacity_mode(joined, missing_mode)
        for policy in policies:
            selected_one, diag = select_one_entry(mode_frame, policy)
            selected_one["missing_capacity_mode"] = missing_mode
            selected_one["entry_selection_policy"] = policy
            invariant = selected_one.groupby(["market_key", "model_id", "label_source", "entry_selection_policy"], dropna=False).size() if not selected_one.empty else pd.Series(dtype=int)
            if len(invariant) and int(invariant.max()) > 1:
                raise AssertionError("headline one-entry invariant violated before scenario expansion")
            selection_diags.append(selection_diagnostics_for(selected_one, {"missing_capacity_mode": missing_mode, **diag}))
            for scored in iter_one_entry_scenarios(selected_one, max_caps=max_caps, haircuts=haircuts):
                scored = stress.add_breakdown_columns(scored)
                summary_parts.append(summarize_one_entry(scored, group_cols))
                concentration = summarize_one_entry(scored, group_cols)
                keep = group_cols + [c for c in concentration.columns if c.startswith("top_") or c.startswith("pnl_without")]
                concentration_parts.append(concentration[keep])
                date_parts.append(summarize_one_entry(scored, group_cols + ["entry_date"]))
                age_parts.append(summarize_one_entry(scored, group_cols + ["age_bucket"]))
                capacity_parts.append(summarize_one_entry(scored, group_cols + ["capacity_bucket"]))
                edge_parts.append(summarize_one_entry(scored, group_cols + ["edge_bucket"]))
                if "chainlink_terminal_margin_band" in scored.columns:
                    margin_parts.append(summarize_one_entry(scored, group_cols + ["chainlink_terminal_margin_band"]))

    def concat(parts: list[pd.DataFrame]) -> pd.DataFrame:
        return pd.concat([p for p in parts if not p.empty], ignore_index=True) if parts else pd.DataFrame()

    summary = concat(summary_parts)
    summary.to_csv(output_dir / "one_entry_capacity_stress_summary.csv", index=False)
    concat(date_parts).to_csv(output_dir / "one_entry_capacity_stress_by_date.csv", index=False)
    concat(age_parts).to_csv(output_dir / "one_entry_capacity_stress_by_market_age_bucket.csv", index=False)
    concat(capacity_parts).to_csv(output_dir / "one_entry_capacity_stress_by_capacity_bucket.csv", index=False)
    concat(edge_parts).to_csv(output_dir / "one_entry_capacity_stress_by_edge_bucket.csv", index=False)
    concat(margin_parts).to_csv(output_dir / "one_entry_capacity_stress_by_terminal_margin_bucket.csv", index=False)
    concat(concentration_parts).to_csv(output_dir / "one_entry_capacity_stress_concentration.csv", index=False)
    pd.DataFrame(selection_diags).to_csv(output_dir / "one_entry_capacity_stress_selection_diagnostics.csv", index=False)
    diagnostics = {
        **dedupe_diag,
        **join_diag,
        "output_dir": str(output_dir),
        "entry_selection_policies": policies,
        "missing_capacity_modes": ["full_join_conservative_zero_missing", "full_depth_only"],
        "max_trade_caps": max_caps,
        "visible_depth_haircuts": haircuts,
        "one_entry_invariant": "max group size over market_key/model_id/label_source/entry_selection_policy == 1 before scenario expansion",
    }
    (output_dir / "one_entry_capacity_stress_join_diagnostics.json").write_text(json.dumps(diagnostics, indent=2, default=str), encoding="utf-8")
    write_readme(output_dir / "one_entry_capacity_stress_readme.txt", args, diagnostics)
    return diagnostics


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Offline one-entry-per-market capacity stress test for BTC-5m selected edge replay entries.")
    parser.add_argument("--selected-entries", type=Path, default=DEFAULT_SELECTED_ENTRIES)
    parser.add_argument("--capacity-output", type=Path, default=DEFAULT_CAPACITY_OUTPUT)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--capacity-col", default="capacity_usdc_at_edge_10")
    parser.add_argument("--max-trade-caps", default="25,50,100,250,500,1000,2500,5000")
    parser.add_argument("--visible-depth-haircuts", default="1.0,0.5,0.25,0.10")
    parser.add_argument("--entry-selection-policies", default=",".join(ENTRY_POLICIES))
    return parser


def main(argv: list[str] | None = None) -> int:
    diagnostics = run(build_parser().parse_args(argv))
    print(json.dumps(diagnostics, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
