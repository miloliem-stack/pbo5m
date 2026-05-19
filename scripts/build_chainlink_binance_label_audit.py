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

from src.research import chainlink_binance_label_audit as audit_lib


def write_optional_parquet(frame: pd.DataFrame, path: Path) -> bool:
    try:
        frame.to_parquet(path, index=False)
        return True
    except Exception:
        return False


def render_readme(args: argparse.Namespace, summary: dict, diagnostics: dict) -> str:
    return "\n".join(
        [
            "Chainlink vs Binance market label audit",
            "",
            "Offline research only. This does not change live trading behavior.",
            "Labels use Up = terminal price > strike/reference price; equality is treated as not-up.",
            "",
            f"predictions={args.predictions}",
            f"market_meta_root={args.market_meta_root}",
            f"binance_root={args.binance_root}",
            f"chainlink_root={args.chainlink_root}",
            f"strike_source={diagnostics.get('strike_source')}",
            f"binance_source={diagnostics.get('binance_source')}",
            f"chainlink_tolerance_seconds={args.chainlink_end_tolerance_seconds}",
            f"binance_tolerance_seconds={args.binance_end_tolerance_seconds}",
            "",
            f"markets_total={summary.get('markets_total')}",
            f"markets_with_binance_label={summary.get('markets_with_binance_label')}",
            f"markets_with_chainlink_label={summary.get('markets_with_chainlink_label')}",
            f"markets_with_both_labels={summary.get('markets_with_both_labels')}",
            f"label_agreement_rate={summary.get('label_agreement_rate')}",
            f"label_disagreement_rate={summary.get('label_disagreement_rate')}",
            "",
            "Binance proxy labels are not final Chainlink/Polymarket settlement truth.",
        ]
    ) + "\n"


def run(args: argparse.Namespace) -> dict:
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    bands = audit_lib.parse_csv_floats(args.terminal_margin_bands_usd)
    audit, diagnostics = audit_lib.build_label_audit(
        Path(args.predictions),
        Path(args.binance_root),
        Path(args.chainlink_root),
        chainlink_tolerance_seconds=args.chainlink_end_tolerance_seconds,
        binance_tolerance_seconds=args.binance_end_tolerance_seconds,
        terminal_margin_bands=bands,
    )
    summary = audit_lib.summarize_audit(audit, bands, diagnostics)
    by_band = pd.concat([audit_lib.agreement_by_band(audit, "binance"), audit_lib.agreement_by_band(audit, "chainlink")], ignore_index=True)
    audit.to_csv(output_dir / "market_label_audit.csv", index=False)
    write_optional_parquet(audit, output_dir / "market_label_audit.parquet")
    by_band.to_csv(output_dir / "label_agreement_by_terminal_margin_band.csv", index=False)
    (output_dir / "label_agreement_summary.json").write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
    (output_dir / "label_audit_readme.txt").write_text(render_readme(args, summary, diagnostics), encoding="utf-8")
    if args.dry_run:
        print(json.dumps(summary, indent=2, default=str))
    return summary


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build offline Chainlink-vs-Binance label audit for BTC 5m markets.")
    parser.add_argument("--predictions", type=Path, required=True)
    parser.add_argument("--market-meta-root", type=Path)
    parser.add_argument("--binance-root", type=Path, required=True)
    parser.add_argument("--chainlink-root", type=Path, required=True)
    parser.add_argument("--replay-trades", type=Path)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--market-window-seconds", type=int, default=300)
    parser.add_argument("--chainlink-end-tolerance-seconds", type=float, default=10)
    parser.add_argument("--binance-end-tolerance-seconds", type=float, default=60)
    parser.add_argument("--terminal-margin-bands-usd", default="1,2,5,10,20,50,100")
    parser.add_argument("--dry-run", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    summary = run(build_parser().parse_args(argv))
    print(json.dumps(summary, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
