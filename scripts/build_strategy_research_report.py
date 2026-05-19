#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import pandas as pd

REQUIRED_SECTIONS = [
    "Strategy thesis",
    "Why it should work on paper",
    "Exact rule tested",
    "Data used",
    "Replay assumptions",
    "Aggregate result",
    "Failure attribution",
    "Robustness checks",
    "Failure explanation",
    "Decision: reject / revise / keep as filter / promote",
    "Diary-entry draft",
]

ATTRIBUTION_COLUMNS = {
    "side": ["side"],
    "ask bin": ["ask_bin", "entry_ask_bin", "ask_bucket"],
    "market age": ["market_age", "market_age_bucket", "entry_age_sec"],
    "distance from reference": ["distance_from_reference", "distance_to_reference", "reference_distance"],
    "recent return direction": ["recent_return_direction", "return_direction", "recent_direction"],
    "volatility bucket": ["volatility_bucket", "vol_bucket", "vol_regime"],
    "sign-flip/chop bucket": ["sign_flip_chop_bucket", "chop_bucket", "sign_flip_bucket"],
    "shock age": ["shock_age", "shock_age_bucket"],
    "chronological slice": ["chronological_slice", "time_slice"],
    "regime/HMM state": ["regime_state", "hmm_state", "regime"],
}

REQUIRED_NUMERIC_COLUMNS = ["gross_cost", "gross_pnl"]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build standardized strategy research markdown report")
    parser.add_argument("--input", required=True, type=Path, help="Replay output folder or file")
    parser.add_argument("--output", required=True, type=Path, help="Markdown output path")
    parser.add_argument("--strategy-name", required=True, help="Display strategy name")
    parser.add_argument("--thesis", default="TODO: add strategy thesis.")
    parser.add_argument("--mechanism", default="TODO: explain why the strategy should work on paper.")
    parser.add_argument("--exact-rule", default="TODO: describe exact rule tested.")
    parser.add_argument("--decision", default="revise", choices=["reject", "revise", "keep as filter", "promote"])
    return parser


def read_frame(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".parquet":
        return pd.read_parquet(path)
    if path.suffix.lower() == ".csv":
        return pd.read_csv(path)
    if path.suffix.lower() in {".json", ".jsonl"}:
        return pd.read_json(path, lines=True)
    raise ValueError(f"Unsupported tabular input: {path}")


def resolve_trade_results(input_path: Path) -> Path:
    if input_path.is_file():
        return input_path
    for candidate in ["trade_level_results.parquet", "trade_level_results.csv", "trade_level_results.jsonl"]:
        trial = input_path / candidate
        if trial.exists():
            return trial
    raise FileNotFoundError("Could not find trade-level results under input path")


def aggregate_metrics(df: pd.DataFrame) -> dict[str, float]:
    gross_cost = float(pd.to_numeric(df.get("gross_cost"), errors="coerce").sum())
    gross_pnl = float(pd.to_numeric(df.get("gross_pnl"), errors="coerce").sum())
    gross_payout = float(pd.to_numeric(df.get("gross_payout"), errors="coerce").sum()) if "gross_payout" in df.columns else float("nan")
    trades = int(len(df))
    markets = int(df["market_key"].nunique()) if "market_key" in df.columns else trades
    roi = gross_pnl / gross_cost if gross_cost else float("nan")
    win_rate = float(pd.to_numeric(df["win"], errors="coerce").mean()) if "win" in df.columns else float("nan")
    return {
        "trade_count": trades,
        "market_count": markets,
        "gross_cost": gross_cost,
        "gross_payout": gross_payout,
        "gross_pnl": gross_pnl,
        "roi_on_filled_cost": roi,
        "win_rate": win_rate,
    }


def attribution_summary(df: pd.DataFrame) -> tuple[list[str], list[str]]:
    present: list[str] = []
    missing: list[str] = []
    columns = set(df.columns)
    for label, aliases in ATTRIBUTION_COLUMNS.items():
        if any(alias in columns for alias in aliases):
            present.append(label)
        else:
            missing.append(label)
    return present, missing


def build_report(args: argparse.Namespace, df: pd.DataFrame, source_path: Path) -> str:
    metrics = aggregate_metrics(df)
    present, missing = attribution_summary(df)
    missing_required = [c for c in REQUIRED_NUMERIC_COLUMNS if c not in df.columns]

    lines = [
        f"# Strategy Research Report: {args.strategy_name}",
        "",
        "## 1. Strategy thesis",
        args.thesis,
        "",
        "## 2. Why it should work on paper",
        args.mechanism,
        "",
        "## 3. Exact rule tested",
        args.exact_rule,
        "",
        "## 4. Data used",
        f"- Input source: `{source_path}`",
        f"- Rows (trades): {metrics['trade_count']}",
        f"- Unique markets: {metrics['market_count']}",
        "",
        "## 5. Replay assumptions",
        "- Assumes replay artifacts are historical outputs and no future information was injected at report-build time.",
        "- No strategy rules are changed by this report generator; it only reads artifacts.",
        "- Assumes aggregate PnL is represented by `gross_pnl` and cost by `gross_cost` in the input artifact.",
        "",
        "## 6. Aggregate result",
        f"- Gross cost: {metrics['gross_cost']:.6f}",
        f"- Gross payout: {metrics['gross_payout']:.6f}" if metrics["gross_payout"] == metrics["gross_payout"] else "- Gross payout: missing",
        f"- Gross PnL: {metrics['gross_pnl']:.6f}",
        f"- ROI on filled cost: {metrics['roi_on_filled_cost']:.6f}" if metrics["roi_on_filled_cost"] == metrics["roi_on_filled_cost"] else "- ROI on filled cost: missing",
        f"- Win rate: {metrics['win_rate']:.6f}" if metrics["win_rate"] == metrics["win_rate"] else "- Win rate: missing",
        "",
        "## 7. Failure attribution",
        f"- Attribution dimensions present: {', '.join(present) if present else 'none'}",
        f"- Attribution dimensions missing: {', '.join(missing) if missing else 'none'}",
        "",
        "## 8. Robustness checks",
        "- Missing required aggregate columns: " + (", ".join(missing_required) if missing_required else "none"),
        "- Replay assumption guard: report generation does not call any sweep or simulation routines.",
        "",
        "## 9. Failure explanation",
        "TODO: Describe largest loss buckets and probable mechanism-level failure cause.",
        "",
        "## 10. Decision: reject / revise / keep as filter / promote",
        f"Decision: **{args.decision}**",
        "",
        "## 11. Diary-entry draft",
        f"{args.strategy_name}: replay reviewed with standardized report; aggregate pnl={metrics['gross_pnl']:.6f}, roi={metrics['roi_on_filled_cost'] if metrics['roi_on_filled_cost']==metrics['roi_on_filled_cost'] else 'missing'}.",
        "",
    ]
    return "\n".join(lines)


def run(args: argparse.Namespace) -> dict[str, Any]:
    source_path = resolve_trade_results(args.input)
    df = read_frame(source_path)
    report = build_report(args, df, source_path)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(report, encoding="utf-8")
    present, missing = attribution_summary(df)
    manifest = {
        "strategy_name": args.strategy_name,
        "input": str(source_path),
        "output": str(args.output),
        "rows": int(len(df)),
        "required_sections": REQUIRED_SECTIONS,
        "missing_required_columns": [c for c in REQUIRED_NUMERIC_COLUMNS if c not in df.columns],
        "present_attribution_dimensions": present,
        "missing_attribution_dimensions": missing,
    }
    return manifest


if __name__ == "__main__":
    parsed = build_parser().parse_args()
    result = run(parsed)
    print(json.dumps(result, indent=2, sort_keys=True))
