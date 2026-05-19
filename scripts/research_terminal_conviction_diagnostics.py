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


DEFAULT_OUTPUT_DIR = Path("artifacts/terminal_conviction_diagnostics")


def render_readme(summary: pd.DataFrame, quality: pd.DataFrame, metrics: pd.DataFrame | None) -> str:
    lines = [
        "Terminal conviction diagnostics",
        "",
        "Measured the first quote timestamp where one Polymarket side crossed a threshold and remained effectively convicted through market end.",
        "Terminal conviction is hindsight-defined and research-only. It is not a live trading signal.",
        "Prediction pre/post metrics are intended to detect late-market obvious-state inflation.",
        "Binance proxy labels are not final Chainlink/Polymarket truth.",
        "",
        "Top-line conviction timing:",
    ]
    if summary.empty:
        lines.append("- no conviction rows produced")
    else:
        for _, row in summary.head(20).iterrows():
            lines.append(
                f"- threshold={row['threshold']} source={row['price_source']} def={row['conviction_definition']} "
                f"convicted={row['markets_convicted']}/{row['markets_total']} "
                f"median_age={row.get('median_conviction_age_seconds')}"
            )
    lines.extend(["", "Quote quality:"])
    if quality.empty:
        lines.append("- no quality diagnostics")
    else:
        for _, row in quality.iterrows():
            lines.append(f"- source={row.get('price_source')} kept={row.get('kept_rows')} dropped={row.get('dropped_rows')}")
    if metrics is not None and not metrics.empty:
        lines.extend(["", "Top-line pre/post model metric comparison:"])
        best_model = metrics.sort_values("brier").iloc[0]["model"]
        subset = metrics[metrics["model"] == best_model]
        lines.append(f"- best_model_by_available_brier={best_model}")
        for _, row in subset.head(10).iterrows():
            lines.append(
                f"- phase={row['prediction_phase']} threshold={row['threshold']} source={row['price_source']} "
                f"rows={row['rows']} brier={row['brier']} log_loss={row['log_loss']}"
            )
    lines.extend(
        [
            "",
            "Warnings:",
            "- Missing or malformed quote rows are dropped and counted by reason.",
            "- Strict/tolerant conviction only checks later quality-filtered quotes.",
            "- Markets with sparse quotes can have unstable conviction timing.",
        ]
    )
    return "\n".join(lines) + "\n"


def run(args: argparse.Namespace) -> dict[str, object]:
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    thresholds = tc.parse_csv_floats(args.thresholds)
    sources = tc.parse_csv_strings(args.sources)
    definitions = tc.parse_csv_strings(args.definitions)
    config = {
        "quotes": str(args.quotes),
        "market_meta": None if args.market_meta is None else str(args.market_meta),
        "predictions": None if args.predictions is None else str(args.predictions),
        "model_summary": None if args.model_summary is None else str(args.model_summary),
        "output_dir": str(output_dir),
        "thresholds": thresholds,
        "sources": sources,
        "definitions": definitions,
        "max_spread": args.max_spread,
        "mid_complement_tolerance": args.mid_complement_tolerance,
        "min_later_share": args.min_later_share,
        "tolerant_floor_offset": args.tolerant_floor_offset,
        "min_quality_quotes_per_market": args.min_quality_quotes_per_market,
        "min_later_quotes": args.min_later_quotes,
        "market_window_seconds": args.market_window_seconds,
    }
    (output_dir / "terminal_conviction_config.json").write_text(json.dumps(config, indent=2, default=str), encoding="utf-8")
    quotes, quote_diag = tc.load_quote_frame(Path(args.quotes), args.market_window_seconds)
    meta, meta_diag = tc.load_market_meta(args.market_meta, args.market_window_seconds)
    quotes = tc.apply_metadata(quotes, meta)
    by_market, quality, conviction_diag = tc.compute_terminal_convictions(
        quotes,
        thresholds=thresholds,
        sources=sources,
        definitions=definitions,
        max_spread=args.max_spread,
        mid_complement_tolerance=args.mid_complement_tolerance,
        min_later_share=args.min_later_share,
        tolerant_floor_offset=args.tolerant_floor_offset,
        min_later_quotes=args.min_later_quotes,
        min_quality_quotes_per_market=args.min_quality_quotes_per_market,
    )
    summary = tc.summarize_convictions(by_market)
    by_market.to_csv(output_dir / "terminal_conviction_by_market.csv", index=False)
    summary.to_csv(output_dir / "terminal_conviction_summary.csv", index=False)
    quality.to_csv(output_dir / "quote_quality_diagnostics.csv", index=False)
    diagnostics = {
        "config": config,
        "quote_diagnostics": quote_diag,
        "market_meta_diagnostics": meta_diag,
        "conviction_diagnostics": conviction_diag,
        "rows": {
            "quotes_normalized": int(len(quotes)),
            "terminal_conviction_by_market": int(len(by_market)),
            "terminal_conviction_summary": int(len(summary)),
        },
    }
    metrics = None
    joined = None
    if args.predictions is not None:
        predictions_raw = tc.load_predictions(Path(args.predictions))
        predictions = tc.normalize_predictions(predictions_raw, args.market_window_seconds)
        joined = tc.join_predictions_to_convictions(predictions, by_market, args.market_window_seconds)
        joined_path = tc.write_parquet_or_csv(
            joined,
            output_dir / "prediction_conviction_join.parquet",
            output_dir / "prediction_conviction_join.csv",
        )
        metrics = tc.prediction_metrics(joined)
        joined["market_age_bucket"] = joined["market_age_seconds"].map(tc.market_age_bucket)
        metrics_age = tc.prediction_metrics(joined, ["market_age_bucket"])
        metrics.to_csv(output_dir / "model_metrics_by_conviction_phase.csv", index=False)
        metrics_age.to_csv(output_dir / "model_metrics_by_conviction_phase_and_age.csv", index=False)
        diagnostics["prediction_join"] = {
            "raw_prediction_rows": int(len(predictions_raw)),
            "normalized_prediction_rows": int(len(predictions)),
            "joined_rows": int(len(joined)),
            "join_output": joined_path,
        }
    summary_payload = {
        "summary": summary.to_dict(orient="records"),
        "diagnostics": diagnostics,
    }
    (output_dir / "terminal_conviction_summary.json").write_text(json.dumps(summary_payload, indent=2, default=str), encoding="utf-8")
    (output_dir / "quote_quality_diagnostics.json").write_text(json.dumps(diagnostics, indent=2, default=str), encoding="utf-8")
    (output_dir / "conviction_readme_summary.txt").write_text(render_readme(summary, quality, metrics), encoding="utf-8")
    if args.dry_run:
        diagnostics["dry_run"] = True
    return diagnostics


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Offline terminal-conviction diagnostics for Polymarket BTC 5m quotes.")
    parser.add_argument("--quotes", type=Path, required=True)
    parser.add_argument("--market-meta", type=Path)
    parser.add_argument("--predictions", type=Path)
    parser.add_argument("--model-summary", type=Path)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--thresholds", default="0.70,0.75,0.80,0.85,0.90")
    parser.add_argument("--sources", default="mid,bid")
    parser.add_argument("--definitions", default="strict,tolerant")
    parser.add_argument("--max-spread", type=float, default=0.15)
    parser.add_argument("--mid-complement-tolerance", type=float, default=0.10)
    parser.add_argument("--min-later-share", type=float, default=0.95)
    parser.add_argument("--tolerant-floor-offset", type=float, default=0.0)
    parser.add_argument("--min-quality-quotes-per-market", type=int, default=5)
    parser.add_argument("--min-later-quotes", type=int, default=2)
    parser.add_argument("--market-window-seconds", type=int, default=300)
    parser.add_argument("--prediction-time-column", default="auto")
    parser.add_argument("--prediction-market-key", default="auto")
    parser.add_argument("--dry-run", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    diagnostics = run(args)
    print(json.dumps(diagnostics, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
