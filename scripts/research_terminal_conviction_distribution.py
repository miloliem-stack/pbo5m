#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.research import terminal_conviction as tc


def parse_optional_float(value: str | None) -> float | None:
    if value is None or str(value).strip() == "":
        return None
    return float(value)


def parse_bin_seconds(value: str) -> list[int]:
    return [int(item.strip()) for item in str(value).split(",") if item.strip()]


def summarize(by_market: pd.DataFrame, min_quality_quotes_per_market: int) -> pd.DataFrame:
    if by_market.empty:
        return pd.DataFrame()
    rows = []
    for keys, group in by_market.groupby(["threshold", "price_source", "conviction_definition"], dropna=False):
        threshold, source, definition = keys
        sufficient = group[group["quality_quote_count"] >= min_quality_quotes_per_market]
        convicted = sufficient[sufficient["reached_terminal_conviction"] == True]
        age = pd.to_numeric(convicted["conviction_market_age_seconds"], errors="coerce").dropna()
        rem = pd.to_numeric(convicted["conviction_remaining_seconds"], errors="coerce").dropna()
        row = {
            "threshold": threshold,
            "price_source": source,
            "conviction_definition": definition,
            "markets_total": int(group["market_key"].nunique()),
            "markets_with_sufficient_quotes": int(sufficient["market_key"].nunique()),
            "markets_convicted": int(convicted["market_key"].nunique()),
            "share_convicted": len(convicted) / len(sufficient) if len(sufficient) else None,
            "share_never_convicted": 1.0 - len(convicted) / len(sufficient) if len(sufficient) else None,
            "yes_conviction_share": float((convicted["convicted_side"] == "YES").mean()) if len(convicted) else None,
            "no_conviction_share": float((convicted["convicted_side"] == "NO").mean()) if len(convicted) else None,
        }
        for name, series in (("conviction_age_seconds", age), ("remaining_seconds", rem)):
            row[f"mean_{name}"] = float(series.mean()) if len(series) else None
            row[f"median_{name}"] = float(series.median()) if len(series) else None
            for q in (10, 25, 75, 90):
                row[f"p{q}_{name}"] = float(series.quantile(q / 100.0)) if len(series) else None
        rows.append(row)
    return pd.DataFrame(rows)


def render_readme(summary: pd.DataFrame, by_second: pd.DataFrame, diagnostics: dict) -> str:
    lines = [
        "Terminal conviction timing distribution",
        "",
        "This is an offline, hindsight-defined research diagnostic. It is not a live trading signal.",
        "It measures when recorded Polymarket BTC 5-minute markets first enter terminal conviction under configurable quote thresholds.",
        "",
        f"quote_rows_loaded={diagnostics.get('loaded_rows')}",
        f"quote_rows_parsed={diagnostics.get('parsed_rows')}",
        f"markets_discovered={diagnostics.get('markets_discovered')}",
        f"markets_with_sufficient_quotes={diagnostics.get('markets_with_sufficient_quotes')}",
        "",
        "Summary by threshold/source/definition:",
    ]
    if summary.empty:
        lines.append("- no rows")
    else:
        for _, row in summary.head(30).iterrows():
            lines.append(
                f"- threshold={row['threshold']} source={row['price_source']} def={row['conviction_definition']} "
                f"convicted={row['markets_convicted']}/{row['markets_with_sufficient_quotes']} "
                f"median_age={row.get('median_conviction_age_seconds')} "
                f"median_remaining={row.get('median_remaining_seconds')}"
            )
    dropped = diagnostics.get("rows_dropped_by_reason", {})
    parsed = diagnostics.get("parsed_rows") or 0
    dropped_total = sum(int(v) for v in dropped.values()) if isinstance(dropped, dict) else 0
    if parsed and dropped_total / parsed > 0.5:
        lines.append("")
        lines.append("WARNING: more than 50% of parsed rows are missing key fields or side prices.")
    quality = diagnostics.get("quality_by_source", [])
    for row in quality:
        if row.get("dropped_rows", 0) and row.get("kept_rows", 0) < row.get("dropped_rows", 0):
            lines.append(f"WARNING: source={row.get('price_source')} dropped more rows than it kept. Inspect diagnostics before relying on timings.")
    lines.extend(["", "Top distribution rows for threshold 0.80 mid/tolerant:"])
    if not by_second.empty:
        subset = by_second[
            (by_second["threshold"] == 0.80)
            & (by_second["price_source"] == "mid")
            & (by_second["conviction_definition"] == "tolerant")
            & (by_second["first_convictions_at_second"] > 0)
        ].head(20)
        if subset.empty:
            lines.append("- none")
        else:
            for _, row in subset.iterrows():
                lines.append(f"- age={row['market_age_second']}s first={row['first_convictions_at_second']} cumulative_share={row['cumulative_conviction_share']}")
    return "\n".join(lines) + "\n"


def run(args: argparse.Namespace) -> dict:
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    thresholds = tc.parse_csv_floats(args.thresholds)
    sources = tc.parse_csv_strings(args.sources)
    definitions = tc.parse_csv_strings(args.definitions)
    bin_seconds = parse_bin_seconds(args.bin_seconds)
    quotes, load_diag = tc.load_quote_frame(Path(args.quotes), args.market_window_seconds)
    meta, meta_diag = tc.load_market_meta(args.market_meta, args.market_window_seconds)
    quotes = tc.apply_metadata(quotes, meta)
    parser_diag = tc.quote_diagnostics(quotes, debug_schema_sample=args.debug_schema_sample)
    by_market, quality = tc.compute_terminal_conviction_distribution_rows(
        quotes,
        thresholds=thresholds,
        sources=sources,
        definitions=definitions,
        disable_spread_filter=args.disable_spread_filter,
        max_spread=parse_optional_float(args.max_spread),
        mid_complement_tolerance=parse_optional_float(args.mid_complement_tolerance),
        min_later_share=args.min_later_share,
        tolerant_floor_offset=args.tolerant_floor_offset,
        min_later_quotes=args.min_later_quotes,
        min_quality_quotes_per_market=args.min_quality_quotes_per_market,
        max_post_end_lag_seconds=args.max_post_end_lag_seconds,
    )
    summary = summarize(by_market, args.min_quality_quotes_per_market)
    by_second = tc.distribution_by_second(by_market, args.market_window_seconds)
    binned = tc.distribution_binned(by_second, bin_seconds)
    quality_records = quality.to_dict(orient="records") if not quality.empty else []
    sufficient = by_market[by_market["quality_quote_count"] >= args.min_quality_quotes_per_market]
    diagnostics = {
        **load_diag,
        **parser_diag,
        "market_meta": meta_diag,
        "quality_by_source": quality_records,
        "markets_with_sufficient_quotes": int(sufficient["market_key"].nunique()) if not sufficient.empty else 0,
        "config": {
            "thresholds": thresholds,
            "sources": sources,
            "definitions": definitions,
            "market_window_seconds": args.market_window_seconds,
            "bin_seconds": bin_seconds,
            "max_spread": args.max_spread,
            "mid_complement_tolerance": args.mid_complement_tolerance,
            "disable_spread_filter": args.disable_spread_filter,
            "min_quality_quotes_per_market": args.min_quality_quotes_per_market,
            "min_later_quotes": args.min_later_quotes,
            "min_later_share": args.min_later_share,
            "tolerant_floor_offset": args.tolerant_floor_offset,
            "max_post_end_lag_seconds": args.max_post_end_lag_seconds,
        },
    }
    by_market.to_csv(output_dir / "terminal_conviction_by_market.csv", index=False)
    by_second.to_csv(output_dir / "terminal_conviction_distribution_by_second.csv", index=False)
    binned.to_csv(output_dir / "terminal_conviction_distribution_binned.csv", index=False)
    summary.to_csv(output_dir / "terminal_conviction_summary.csv", index=False)
    diagnostics_csv = {
        key: value
        for key, value in diagnostics.items()
        if key
        in {
            "loaded_rows",
            "parsed_rows",
            "rows_with_market_key",
            "rows_with_market_window",
            "rows_with_yes_bid",
            "rows_with_yes_ask",
            "rows_with_yes_mid",
            "rows_with_yes_last",
            "rows_with_no_bid",
            "rows_with_no_ask",
            "rows_with_no_mid",
            "rows_with_no_last",
            "markets_discovered",
            "markets_with_sufficient_quotes",
        }
    }
    pd.DataFrame([diagnostics_csv]).to_csv(output_dir / "terminal_conviction_quote_diagnostics.csv", index=False)
    quality.to_csv(output_dir / "terminal_conviction_quality_by_source.csv", index=False)
    (output_dir / "terminal_conviction_quote_diagnostics.json").write_text(json.dumps(diagnostics, indent=2, default=str), encoding="utf-8")
    (output_dir / "terminal_conviction_readme.txt").write_text(render_readme(summary, by_second, diagnostics), encoding="utf-8")
    return diagnostics


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Offline terminal-conviction timing distribution for BTC 5m Polymarket quotes.")
    parser.add_argument("--quotes", type=Path, required=True)
    parser.add_argument("--market-meta", type=Path)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--thresholds", default="0.70,0.75,0.80,0.85,0.90")
    parser.add_argument("--sources", default="complement_bid")
    parser.add_argument("--definitions", default="strict,tolerant")
    parser.add_argument("--market-window-seconds", type=int, default=300)
    parser.add_argument("--bin-seconds", default="1,5,10,30")
    parser.add_argument("--max-spread")
    parser.add_argument("--mid-complement-tolerance")
    parser.add_argument("--min-quality-quotes-per-market", type=int, default=5)
    parser.add_argument("--min-later-quotes", type=int, default=2)
    parser.add_argument("--min-later-share", type=float, default=0.95)
    parser.add_argument("--tolerant-floor-offset", type=float, default=0.02)
    parser.add_argument("--disable-spread-filter", action="store_true")
    parser.add_argument("--debug-schema-sample", type=int, default=0)
    parser.add_argument("--max-post-end-lag-seconds", type=float, default=0.0)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    diagnostics = run(args)
    print(json.dumps(diagnostics, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
