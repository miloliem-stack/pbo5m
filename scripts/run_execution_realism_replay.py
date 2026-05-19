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


def read_frame(path: Path) -> pd.DataFrame:
    return pd.read_parquet(path) if path.suffix.lower() == ".parquet" else pd.read_csv(path)


def write_optional_parquet(frame: pd.DataFrame, path: Path) -> bool:
    try:
        frame.to_parquet(path, index=False)
        return True
    except Exception:
        return False


def bool_arg(value: str | bool) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).lower() in {"1", "true", "yes", "y"}


def normalize_entries(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    if "model_id" not in out and "model" in out:
        out["model_id"] = out["model"]
    if "market_key" not in out and "prediction_market_key" in out:
        out["market_key"] = out["prediction_market_key"]
    out["prediction_ts"] = pd.to_datetime(out["prediction_ts"], utc=True, errors="coerce")
    out["market_age_seconds"] = pd.to_numeric(out["market_age_seconds"], errors="coerce")
    out["edge_threshold"] = pd.to_numeric(out["edge_threshold"], errors="coerce")
    out["raw_entry_price"] = pd.to_numeric(out.get("raw_entry_price", out.get("selected_price")), errors="coerce")
    out["raw_edge"] = pd.to_numeric(out.get("raw_edge", out.get("predicted_edge")), errors="coerce")
    out["p_up"] = pd.to_numeric(out["p_up"], errors="coerce")
    return out.dropna(subset=["model_id", "market_key", "prediction_ts", "side", "p_up", "raw_entry_price"])


def filter_entries(entries: pd.DataFrame, models: list[str] | None, thresholds: list[float], ages: list[float], label_sources: list[str]) -> pd.DataFrame:
    out = entries.copy()
    if models:
        out = out[out["model_id"].isin(models)]
    out = out[out["edge_threshold"].isin(thresholds)]
    out = out[out["label_source"].isin(label_sources)] if "label_source" in out else out
    if ages:
        mask = pd.Series(False, index=out.index)
        for age in ages:
            mask |= (out["market_age_seconds"] - age).abs() < 1e-9
        out = out[mask]
    return out.reset_index(drop=True)


def execute_grid(entries: pd.DataFrame, books: pd.DataFrame, args: argparse.Namespace, grids: dict[str, list[float]]) -> tuple[pd.DataFrame, pd.DataFrame]:
    rows: list[dict[str, Any]] = []
    markouts: list[dict[str, Any]] = []
    for _, entry in entries.iterrows():
        label = ex.label_for_source(entry, entry["label_source"])
        for stake in grids["stake_usdc"]:
            for latency in grids["latency_ms"]:
                for max_age in grids["max_book_age_seconds"]:
                    base = entry.to_dict()
                    base.update({"stake_usdc": stake, "latency_ms": latency, "max_book_age_seconds": max_age, "fee_rate": args.fee_rate})
                    if label is None or pd.isna(label):
                        rows.append({**base, "label_status": "missing_label", "execution_book_status": "not_attempted", "fill_status": "not_attempted", "score_status": "missing_label"})
                        continue
                    book = ex.select_execution_book(books, entry["market_key"], entry["side"], entry["prediction_ts"], latency, max_age)
                    base.update({k: book.get(k) for k in ["target_exec_ts", "execution_book_ts", "execution_book_lag_seconds", "book_is_after_target", "execution_book_status", "best_ask", "best_bid", "book_parse_status", "execution_depth_mode"]})
                    base["label_status"] = "ok"
                    if book.get("execution_book_status") != "ok":
                        rows.append({**base, "fill_status": "not_attempted", "score_status": book.get("execution_book_status")})
                        continue
                    fill = ex.simulate_vwap_fill(book.get("asks") or [], stake, min_trade_notional_usdc=args.min_trade_notional_usdc, min_fill_ratio=args.min_fill_ratio, allow_partial_fills=args.allow_partial_fills)
                    scored = ex.apply_fee_and_score(
                        fill,
                        fee_rate=args.fee_rate,
                        p_chosen_side=ex.p_chosen(entry),
                        edge_threshold=entry["edge_threshold"] if args.edge_after_vwap_threshold_same_as_entry_threshold else 0.0,
                        require_edge=args.require_cost_adjusted_edge,
                        label_up=label,
                        side=entry["side"],
                    )
                    scored["vwap_minus_original_entry_price"] = scored.get("vwap_price", np.nan) - entry["raw_entry_price"] if scored.get("vwap_price") is not None else np.nan
                    row = {**base, **scored}
                    rows.append(row)
                    if scored.get("score_status") == "filled":
                        for horizon in grids["markout_horizons_seconds"]:
                            markouts.append(
                                {
                                    **{k: row.get(k) for k in ["label_source", "model_id", "edge_threshold", "stake_usdc", "latency_ms", "max_book_age_seconds", "market_key", "side", "prediction_ts", "execution_book_ts", "total_cost", "shares_filled"]},
                                    **ex.find_markout(books, entry["market_key"], entry["side"], row["execution_book_ts"], horizon, row["total_cost"], row["shares_filled"]),
                                }
                            )
    return pd.DataFrame(rows), pd.DataFrame(markouts)


def render_readme(args: argparse.Namespace, diagnostics: dict[str, Any], score: pd.DataFrame, markouts: pd.DataFrame) -> str:
    lines = [
        "Execution realism replay",
        "",
        "Offline research only. No live bot behavior changed. No HMM/regime filter included.",
        "Labels depend on recorded Chainlink/Binance quality and configured tolerance.",
        "",
        f"selected_entries={args.selected_entries}",
        f"market_label_audit={args.market_label_audit}",
        f"quotes_root={args.quotes_root}",
        f"selected_entries_loaded={diagnostics.get('selected_entries_loaded')}",
        f"book_parse_status_counts={diagnostics.get('book_parse_status_counts')}",
        f"execution_depth_mode_counts={diagnostics.get('execution_depth_mode_counts')}",
        f"label_sources={args.label_sources}",
        f"stake_usdc_sweep={args.stake_usdc_sweep}",
        f"latency_ms_sweep={args.latency_ms_sweep}",
        f"max_book_age_seconds_sweep={args.max_book_age_seconds_sweep}",
        "",
        "Warnings: full-depth unavailable rows are rejected; top-of-book-only fallback is marked if present.",
        "",
    ]
    if not score.empty:
        lines.append("Top rows by Chainlink aggregate ROI:")
        for _, row in score[score["label_source"].eq("chainlink")].sort_values("aggregate_roi", ascending=False).head(10).iterrows():
            lines.append(f"- {row['model_id']} stake={row['stake_usdc']} latency={row['latency_ms']} edge={row['edge_threshold']} fills={int(row['trades_filled'])} roi={row['aggregate_roi']:.4f} pnl={row['total_pnl']:.4f}")
        lines.append("")
        lines.append("Top rows by agreement-only aggregate ROI:")
        for _, row in score[score["label_source"].eq("agreement_only")].sort_values("aggregate_roi", ascending=False).head(10).iterrows():
            lines.append(f"- {row['model_id']} stake={row['stake_usdc']} latency={row['latency_ms']} edge={row['edge_threshold']} fills={int(row['trades_filled'])} roi={row['aggregate_roi']:.4f} pnl={row['total_pnl']:.4f}")
        lines.append("")
        lines.append("Top rows by incremental PnL vs baseline_50:")
        for _, row in score.sort_values("incremental_pnl_vs_baseline_50", ascending=False).head(10).iterrows():
            lines.append(f"- {row['label_source']} {row['model_id']} edge={row['edge_threshold']} incr_pnl={row['incremental_pnl_vs_baseline_50']:.4f} baseline={row.get('baseline_status')}")
    if not markouts.empty:
        ok = markouts[markouts["markout_status"].eq("ok")]
        lines.extend(["", f"markout_rows={len(markouts)} ok_markout_rows={len(ok)}"])
        if not ok.empty:
            lines.append(f"mean_5s_bid_markout_roi={ok[ok['horizon_seconds'].eq(5)]['markout_roi_using_bid'].mean()}")
    return "\n".join(lines) + "\n"


def run(args: argparse.Namespace) -> dict[str, Any]:
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    labels = read_frame(Path(args.market_label_audit))
    entries = normalize_entries(read_frame(Path(args.selected_entries)))
    if "market_key" not in labels.columns:
        raise ValueError("market label audit must include market_key")
    label_cols = [c for c in ["market_key", "binance_label_up", "chainlink_label_up", "label_agree", "binance_terminal_margin_usd", "chainlink_terminal_margin_usd", "abs_binance_terminal_margin_usd", "abs_chainlink_terminal_margin_usd", "chainlink_terminal_margin_band"] if c in labels]
    entries = entries.drop(columns=[c for c in label_cols if c in entries.columns and c != "market_key"], errors="ignore").merge(labels[label_cols], on="market_key", how="left")
    label_sources = ex.parse_csv_strings(args.label_sources)
    models = ex.parse_csv_strings(args.models) if args.models else None
    thresholds = ex.parse_csv_floats(args.edge_thresholds)
    ages = ex.parse_csv_floats(args.entry_ages)
    entries = filter_entries(entries, models, thresholds, ages, label_sources)
    market_keys = set(entries["market_key"].dropna().astype(str).unique())
    books, book_diag = ex.load_books(Path(args.quotes_root), market_keys)
    grids = {
        "stake_usdc": ex.parse_csv_floats(args.stake_usdc_sweep),
        "latency_ms": ex.parse_csv_floats(args.latency_ms_sweep),
        "max_book_age_seconds": ex.parse_csv_floats(args.max_book_age_seconds_sweep),
        "markout_horizons_seconds": ex.parse_csv_floats(args.markout_horizons_seconds),
    }
    fills, markouts = execute_grid(entries, books, args, grids)
    group_cols = ["label_source", "model_id", "edge_threshold", "stake_usdc", "latency_ms", "max_book_age_seconds"]
    entry_age_set = ",".join(f"{x:g}" for x in ages)
    score = ex.aggregate_scorecard(fills, entries.assign(stake_usdc=np.nan, latency_ms=np.nan, max_book_age_seconds=np.nan), group_cols, entry_age_set, args.fee_rate)
    score = ex.add_baseline_incremental(score) if not score.empty else score
    by_model = ex.aggregate_scorecard(fills, entries.assign(stake_usdc=np.nan, latency_ms=np.nan, max_book_age_seconds=np.nan), ["label_source", "model_id"], entry_age_set, args.fee_rate)
    by_stake = ex.aggregate_scorecard(fills, entries.assign(stake_usdc=np.nan), ["label_source", "stake_usdc"], entry_age_set, args.fee_rate)
    by_latency = ex.aggregate_scorecard(fills, entries.assign(latency_ms=np.nan), ["label_source", "latency_ms"], entry_age_set, args.fee_rate)
    by_book_age = ex.aggregate_scorecard(fills, entries.assign(max_book_age_seconds=np.nan), ["label_source", "max_book_age_seconds"], entry_age_set, args.fee_rate)
    by_age = ex.aggregate_scorecard(fills, entries, ["label_source", "age_bucket"], entry_age_set, args.fee_rate) if "age_bucket" in fills else pd.DataFrame()
    by_fold = ex.aggregate_scorecard(fills, entries, ["label_source", "fold_id"], entry_age_set, args.fee_rate) if "fold_id" in fills else pd.DataFrame()
    if "abs_chainlink_terminal_margin_usd" in fills:
        bands = ex.parse_csv_floats(args.terminal_margin_bands_usd)
        fills["chainlink_terminal_margin_band_exec"] = fills["abs_chainlink_terminal_margin_usd"].map(lambda x: ex.margin_band(x, bands))
        by_margin = ex.aggregate_scorecard(fills, entries, ["label_source", "chainlink_terminal_margin_band_exec"], entry_age_set, args.fee_rate)
    else:
        by_margin = pd.DataFrame()
    fail = ex.fail_reason_table(fills)
    incremental = score[[c for c in ["label_source", "model_id", "edge_threshold", "stake_usdc", "latency_ms", "max_book_age_seconds", "total_pnl", "baseline_50_pnl", "incremental_pnl_vs_baseline_50", "incremental_roi_vs_baseline_50", "baseline_status"] if c in score]] if not score.empty else pd.DataFrame()
    write_optional_parquet(fills, output_dir / "execution_realism_selected_fills.parquet")
    fills.head(50000).to_csv(output_dir / "execution_realism_selected_fills.csv", index=False)
    score.to_csv(output_dir / "execution_realism_scorecard.csv", index=False)
    write_optional_parquet(score, output_dir / "execution_realism_scorecard.parquet")
    by_model.to_csv(output_dir / "execution_realism_by_model.csv", index=False)
    by_stake.to_csv(output_dir / "execution_realism_by_stake.csv", index=False)
    by_latency.to_csv(output_dir / "execution_realism_by_latency.csv", index=False)
    by_book_age.to_csv(output_dir / "execution_realism_by_book_age.csv", index=False)
    by_age.to_csv(output_dir / "execution_realism_by_age_bucket.csv", index=False)
    by_fold.to_csv(output_dir / "execution_realism_by_fold.csv", index=False)
    by_margin.to_csv(output_dir / "execution_realism_by_terminal_margin_band.csv", index=False)
    markouts.to_csv(output_dir / "execution_realism_markouts.csv", index=False)
    fail.to_csv(output_dir / "execution_realism_fail_reasons.csv", index=False)
    incremental.to_csv(output_dir / "execution_realism_incremental_vs_baseline.csv", index=False)
    diagnostics = {
        **book_diag,
        "selected_entries_loaded": int(len(entries)),
        "fills_rows": int(len(fills)),
        "scorecard_rows": int(len(score)),
        "baseline_duplicate_rows": int(score.attrs.get("baseline_duplicate_rows", 0)) if hasattr(score, "attrs") else 0,
        "fallback_columns": {"model_id": "model if missing", "market_key": "prediction_market_key if missing", "raw_entry_price": "selected_price if missing", "raw_edge": "predicted_edge if missing"},
    }
    (output_dir / "execution_realism_diagnostics.json").write_text(json.dumps(diagnostics, indent=2, default=str), encoding="utf-8")
    (output_dir / "execution_realism_readme.txt").write_text(render_readme(args, diagnostics, score, markouts), encoding="utf-8")
    if args.dry_run:
        print(json.dumps(diagnostics, indent=2, default=str))
    return diagnostics


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Offline execution realism replay for BTC 5m probability edge selections.")
    parser.add_argument("--selected-entries", type=Path, required=True)
    parser.add_argument("--market-label-audit", type=Path, required=True)
    parser.add_argument("--quotes-root", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--label-sources", default="chainlink,agreement_only")
    parser.add_argument("--models")
    parser.add_argument("--edge-thresholds", default="0.10")
    parser.add_argument("--entry-ages", default="60,120,180")
    parser.add_argument("--stake-usdc-sweep", default="1,2,5,10")
    parser.add_argument("--latency-ms-sweep", default="0,500,1000,2000")
    parser.add_argument("--max-book-age-seconds-sweep", default="1.0,2.0")
    parser.add_argument("--min-fill-ratio", type=float, default=1.0)
    parser.add_argument("--allow-partial-fills", type=bool_arg, default=False)
    parser.add_argument("--min-trade-notional-usdc", type=float, default=1.0)
    parser.add_argument("--fee-mode", default="polymarket_crypto_formula")
    parser.add_argument("--fee-rate", type=float, default=0.07)
    parser.add_argument("--require-cost-adjusted-edge", type=bool_arg, default=True)
    parser.add_argument("--edge-after-vwap-threshold-same-as-entry-threshold", type=bool_arg, default=True)
    parser.add_argument("--markout-horizons-seconds", default="1,5,15,30,60")
    parser.add_argument("--terminal-margin-bands-usd", default="1,2,5,10,20,50,100")
    parser.add_argument("--dry-run", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    diagnostics = run(build_parser().parse_args(argv))
    print(json.dumps(diagnostics, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
