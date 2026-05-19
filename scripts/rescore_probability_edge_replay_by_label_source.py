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

from src.research.chainlink_binance_label_audit import margin_band, parse_csv_floats


def parse_csv(value: str) -> list[str]:
    return [item.strip() for item in str(value).split(",") if item.strip()]


def read_frame(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".parquet":
        return pd.read_parquet(path)
    return pd.read_csv(path)


def write_optional_parquet(frame: pd.DataFrame, path: Path) -> bool:
    try:
        frame.to_parquet(path, index=False)
        return True
    except Exception:
        return False


def normalize_trades(trades: pd.DataFrame) -> pd.DataFrame:
    out = trades.copy()
    if "model_id" not in out.columns:
        out["model_id"] = out["model"]
    if "market_key" not in out.columns:
        out["market_key"] = out.get("prediction_market_key", out.get("market_start_key"))
    out["prediction_ts"] = pd.to_datetime(out["prediction_ts"], utc=True, errors="coerce")
    out["market_age_seconds"] = pd.to_numeric(out["market_age_seconds"], errors="coerce")
    out["raw_entry_price"] = pd.to_numeric(out.get("selected_price", out.get("entry_price")), errors="coerce")
    out["raw_edge"] = pd.to_numeric(out.get("predicted_edge"), errors="coerce")
    out["p_up"] = pd.to_numeric(out["p_up"], errors="coerce")
    out["edge_threshold"] = pd.to_numeric(out["edge_threshold"], errors="coerce")
    return out.dropna(subset=["model_id", "market_key", "prediction_ts", "raw_entry_price", "p_up"])


def allowed_age_mask(frame: pd.DataFrame, entry_ages: list[float]) -> pd.Series:
    if not entry_ages:
        return pd.Series(True, index=frame.index)
    ages = pd.to_numeric(frame["market_age_seconds"], errors="coerce")
    mask = pd.Series(False, index=frame.index)
    for age in entry_ages:
        mask |= (ages - age).abs() < 1e-9
    return mask


def selected_side_probability(frame: pd.DataFrame) -> pd.Series:
    return np.where(frame["side"].eq("YES"), frame["p_up"], 1.0 - frame["p_up"])


def side_wins(frame: pd.DataFrame, label_col: str) -> pd.Series:
    return np.where(frame["side"].eq("YES"), frame[label_col].eq(1.0), frame[label_col].eq(0.0))


def apply_costs(frame: pd.DataFrame, *, slippage_bps: float, fee_rate: float, stake_usdc: float) -> pd.DataFrame:
    out = frame.copy()
    out["adjusted_entry_price"] = np.minimum(out["raw_entry_price"] * (1.0 + slippage_bps / 10000.0), 0.999999)
    out["stake_usdc"] = stake_usdc
    out["shares"] = stake_usdc / out["adjusted_entry_price"]
    out["fee_per_share"] = fee_rate * out["adjusted_entry_price"] * (1.0 - out["adjusted_entry_price"])
    out["fee"] = out["shares"] * out["fee_per_share"]
    out["total_cost"] = stake_usdc + out["fee"]
    out["selected_side_probability"] = selected_side_probability(out)
    out["cost_adjusted_edge"] = out["selected_side_probability"] - out["adjusted_entry_price"] - out["fee_per_share"]
    return out


def one_entry_per_market(frame: pd.DataFrame) -> pd.DataFrame:
    return frame.sort_values(["prediction_ts", "cost_adjusted_edge"], ascending=[True, False]).drop_duplicates(["model_id", "edge_threshold", "slippage_bps", "label_source", "market_key"], keep="first")


def score_entries(frame: pd.DataFrame, label_col: str) -> pd.DataFrame:
    out = frame.copy()
    wins = side_wins(out, label_col)
    out["win"] = wins.astype(float)
    out["gross_payout"] = np.where(wins, out["shares"], 0.0)
    out["pnl"] = out["gross_payout"] - out["total_cost"]
    out["trade_roi"] = out["pnl"] / out["total_cost"]
    return out


def prepare_entries(
    trades: pd.DataFrame,
    audit: pd.DataFrame,
    *,
    label_source: str,
    slippage_bps: float,
    fee_rate: float,
    stake_usdc: float,
    entry_ages: list[float],
    edge_thresholds: list[float],
    require_cost_adjusted_edge: bool,
    max_entry_price: float | None,
    one_entry: bool,
    models: list[str] | None,
) -> pd.DataFrame:
    frame = normalize_trades(trades)
    if models:
        frame = frame[frame["model_id"].isin(models)]
    frame = frame[frame["edge_threshold"].isin(edge_thresholds)]
    frame = frame[allowed_age_mask(frame, entry_ages)]
    if max_entry_price is not None:
        frame = frame[frame["raw_entry_price"] <= max_entry_price]
    labels = audit[["market_key", "binance_label_up", "chainlink_label_up", "label_agree", "binance_terminal_margin_usd", "chainlink_terminal_margin_usd", "abs_binance_terminal_margin_usd", "abs_chainlink_terminal_margin_usd"]].copy()
    frame = frame.merge(labels, on="market_key", how="left")
    if label_source == "binance":
        frame = frame[frame["binance_label_up"].notna()].copy()
        label_col = "binance_label_up"
    elif label_source == "chainlink":
        frame = frame[frame["chainlink_label_up"].notna()].copy()
        label_col = "chainlink_label_up"
    elif label_source == "agreement_only":
        frame = frame[frame["label_agree"].eq(True)].copy()
        frame["agreed_label_up"] = frame["binance_label_up"]
        label_col = "agreed_label_up"
    elif label_source == "disagreement_only":
        frame = frame[frame["label_agree"].eq(False)].copy()
        label_col = "chainlink_label_up"
    else:
        raise ValueError(f"unsupported label source: {label_source}")
    frame["label_source"] = label_source
    frame["slippage_bps"] = slippage_bps
    frame = apply_costs(frame, slippage_bps=slippage_bps, fee_rate=fee_rate, stake_usdc=stake_usdc)
    if require_cost_adjusted_edge:
        frame = frame[frame["cost_adjusted_edge"] >= frame["edge_threshold"]]
    if "ask_size" in frame.columns:
        frame["min_notional_check_status"] = np.where(frame["ask_size"].notna(), "checked_top_of_book_size", "missing_size_column")
    else:
        frame["min_notional_check_status"] = "missing_size_column"
    if one_entry:
        frame = one_entry_per_market(frame)
    return score_entries(frame, label_col)


def aggregate_scorecard(entries: pd.DataFrame, group_cols: list[str], fee_rate: float, one_entry: bool, entry_ages: list[float]) -> pd.DataFrame:
    if entries.empty:
        return pd.DataFrame()
    rows = []
    for keys, group in entries.groupby(group_cols, dropna=False):
        if not isinstance(keys, tuple):
            keys = (keys,)
        row = dict(zip(group_cols, keys))
        wins = group["win"].sum()
        both = group[group["label_agree"].notna()]
        row.update(
            {
                "fee_rate": fee_rate,
                "entry_age_set": ",".join(f"{x:g}" for x in entry_ages),
                "one_entry_per_market": one_entry,
                "trades": int(len(group)),
                "markets": int(group["market_key"].nunique()),
                "wins": int(wins),
                "losses": int(len(group) - wins),
                "hit_rate": float(group["win"].mean()) if len(group) else None,
                "total_stake": float(group["stake_usdc"].sum()),
                "total_fees": float(group["fee"].sum()),
                "total_cost": float(group["total_cost"].sum()),
                "total_payout": float(group["gross_payout"].sum()),
                "total_pnl": float(group["pnl"].sum()),
                "aggregate_roi": float(group["pnl"].sum() / group["total_cost"].sum()) if group["total_cost"].sum() else None,
                "mean_trade_roi": float(group["trade_roi"].mean()),
                "median_trade_roi": float(group["trade_roi"].median()),
                "avg_entry_price": float(group["adjusted_entry_price"].mean()),
                "median_entry_price": float(group["adjusted_entry_price"].median()),
                "avg_cost_adjusted_edge": float(group["cost_adjusted_edge"].mean()),
                "avg_raw_edge": float(group["raw_edge"].mean()),
                "binance_chainlink_agreement_rate_in_selected_trades": float(both["label_agree"].mean()) if len(both) else None,
                "selected_trade_disagreement_rate": float((~both["label_agree"].astype(bool)).mean()) if len(both) else None,
                "binance_win_chainlink_loss_count": int((side_wins(group, "binance_label_up") & ~side_wins(group, "chainlink_label_up")).sum()) if "chainlink_label_up" in group else 0,
                "binance_loss_chainlink_win_count": int((~side_wins(group, "binance_label_up") & side_wins(group, "chainlink_label_up")).sum()) if "chainlink_label_up" in group else 0,
            }
        )
        if "binance_label_up" in group and "chainlink_label_up" in group:
            binance_scored = score_entries(group, "binance_label_up")
            chainlink_scored = score_entries(group, "chainlink_label_up")
            row["pnl_binance_label"] = float(binance_scored["pnl"].sum())
            row["pnl_chainlink_label"] = float(chainlink_scored["pnl"].sum())
            row["pnl_delta_chainlink_minus_binance"] = row["pnl_chainlink_label"] - row["pnl_binance_label"]
        else:
            row["pnl_binance_label"] = None
            row["pnl_chainlink_label"] = None
            row["pnl_delta_chainlink_minus_binance"] = None
        rows.append(row)
    return pd.DataFrame(rows)


def add_incremental(score: pd.DataFrame) -> pd.DataFrame:
    if score.empty or "model_id" not in score:
        return score
    keys = [c for c in ["label_source", "edge_threshold", "slippage_bps"] if c in score.columns]
    base = score[score["model_id"].eq("baseline_50")][keys + ["total_pnl", "aggregate_roi"]].rename(columns={"total_pnl": "baseline_50_pnl", "aggregate_roi": "baseline_50_roi"})
    out = score.merge(base, on=keys, how="left")
    out["incremental_pnl_vs_baseline_50"] = out["total_pnl"] - out["baseline_50_pnl"]
    out["incremental_roi_vs_baseline_50"] = out["aggregate_roi"] - out["baseline_50_roi"]
    return out


def disagreement_attribution(entries: pd.DataFrame) -> pd.DataFrame:
    both = entries[entries["binance_label_up"].notna() & entries["chainlink_label_up"].notna()].copy()
    if both.empty:
        return pd.DataFrame()
    both["binance_win"] = side_wins(both, "binance_label_up")
    both["chainlink_win"] = side_wins(both, "chainlink_label_up")
    rows = []
    for keys, group in both.groupby(["model_id", "edge_threshold", "slippage_bps"], dropna=False):
        model, threshold, slip = keys
        binance_scored = score_entries(group, "binance_label_up")
        chain_scored = score_entries(group, "chainlink_label_up")
        flipped = group[group["binance_win"].ne(group["chainlink_win"])]
        rows.append(
            {
                "model_id": model,
                "edge_threshold": threshold,
                "slippage_bps": slip,
                "selected_trades_with_both_labels": int(len(group)),
                "binance_win_chainlink_loss_count": int((group["binance_win"] & ~group["chainlink_win"]).sum()),
                "binance_loss_chainlink_win_count": int((~group["binance_win"] & group["chainlink_win"]).sum()),
                "pnl_binance_label": float(binance_scored["pnl"].sum()),
                "pnl_chainlink_label": float(chain_scored["pnl"].sum()),
                "pnl_delta_chainlink_minus_binance": float(chain_scored["pnl"].sum() - binance_scored["pnl"].sum()),
                "average_entry_price_of_flipped_trades": float(flipped["adjusted_entry_price"].mean()) if len(flipped) else None,
                "average_terminal_margin_of_flipped_trades": float(flipped["chainlink_terminal_margin_usd"].abs().mean()) if len(flipped) else None,
            }
        )
    return pd.DataFrame(rows)


def terminal_margin_score(entries: pd.DataFrame, bands: list[float]) -> pd.DataFrame:
    if entries.empty:
        return pd.DataFrame()
    frame = entries.copy()
    frame["chainlink_terminal_margin_band"] = frame["abs_chainlink_terminal_margin_usd"].map(lambda x: margin_band(x, bands))
    score = aggregate_scorecard(frame, ["model_id", "edge_threshold", "slippage_bps", "chainlink_terminal_margin_band"], frame["fee_rate"].iloc[0], True, [])
    return add_incremental(score)


def render_readme(args: argparse.Namespace, score: pd.DataFrame, diagnostics: dict[str, Any]) -> str:
    lines = [
        "Label-source probability edge replay rescore",
        "",
        "Offline research only. No live bot behavior changed. No HMM/regime filter is included.",
        "Binance proxy labels are compared against Chainlink-derived labels from recorded market data.",
        f"replay_trades={args.replay_trades}",
        f"market_label_audit={args.market_label_audit}",
        f"min_notional_check_status={diagnostics.get('min_notional_check_status')}",
        "",
        f"selected_entries={diagnostics.get('selected_entries')}",
        f"selected_trade_disagreement_rate={diagnostics.get('selected_trade_disagreement_rate')}",
        "",
    ]
    if not score.empty:
        lines.append("Top scorecard rows by Chainlink aggregate ROI:")
        chain = score[score["label_source"].eq("chainlink")].sort_values("aggregate_roi", ascending=False).head(10)
        for _, row in chain.iterrows():
            lines.append(f"- {row['model_id']} thr={row['edge_threshold']} slip={row['slippage_bps']} trades={int(row['trades'])} roi={row['aggregate_roi']:.4f} pnl={row['total_pnl']:.4f}")
        lines.append("")
        lines.append("Top agreement-only rows by aggregate ROI:")
        agree = score[score["label_source"].eq("agreement_only")].sort_values("aggregate_roi", ascending=False).head(10)
        for _, row in agree.iterrows():
            lines.append(f"- {row['model_id']} thr={row['edge_threshold']} slip={row['slippage_bps']} trades={int(row['trades'])} roi={row['aggregate_roi']:.4f} pnl={row['total_pnl']:.4f}")
    return "\n".join(lines) + "\n"


def run(args: argparse.Namespace) -> dict[str, Any]:
    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=True)
    trades = read_frame(Path(args.replay_trades))
    audit = read_frame(Path(args.market_label_audit))
    models = parse_csv(args.models) if args.models else None
    label_sources = parse_csv(args.label_sources)
    thresholds = parse_csv_floats(args.edge_thresholds)
    entry_ages = parse_csv_floats(args.entry_ages)
    slips = parse_csv_floats(args.slippage_bps)
    max_prices = parse_csv_floats(args.max_entry_price) if args.max_entry_price else [None]
    entries = []
    for label_source in label_sources:
        for slip in slips:
            for max_price in max_prices:
                entries.append(
                    prepare_entries(
                        trades,
                        audit,
                        label_source=label_source,
                        slippage_bps=slip,
                        fee_rate=args.fee_rate,
                        stake_usdc=args.stake_usdc,
                        entry_ages=entry_ages,
                        edge_thresholds=thresholds,
                        require_cost_adjusted_edge=args.require_cost_adjusted_edge,
                        max_entry_price=max_price,
                        one_entry=args.one_entry_per_market,
                        models=models,
                    )
                )
    selected = pd.concat([x for x in entries if not x.empty], ignore_index=True) if entries else pd.DataFrame()
    if not selected.empty:
        selected["fee_rate"] = args.fee_rate
    score = aggregate_scorecard(selected, ["label_source", "model_id", "edge_threshold", "slippage_bps"], args.fee_rate, args.one_entry_per_market, entry_ages)
    score = add_incremental(score)
    by_model = score.copy()
    by_fold = aggregate_scorecard(selected, ["label_source", "model_id", "edge_threshold", "slippage_bps", "fold_id"], args.fee_rate, args.one_entry_per_market, entry_ages) if not selected.empty and "fold_id" in selected else pd.DataFrame()
    by_age = aggregate_scorecard(selected, ["label_source", "model_id", "edge_threshold", "slippage_bps", "age_bucket"], args.fee_rate, args.one_entry_per_market, entry_ages) if not selected.empty and "age_bucket" in selected else pd.DataFrame()
    by_margin = terminal_margin_score(selected, parse_csv_floats(args.terminal_margin_bands_usd))
    attribution = disagreement_attribution(selected)
    incremental = score[["label_source", "model_id", "edge_threshold", "slippage_bps", "total_pnl", "baseline_50_pnl", "incremental_pnl_vs_baseline_50", "incremental_roi_vs_baseline_50"]] if not score.empty else pd.DataFrame()
    score.to_csv(out / "label_source_replay_scorecard.csv", index=False)
    write_optional_parquet(score, out / "label_source_replay_scorecard.parquet")
    by_model.to_csv(out / "label_source_replay_by_model_threshold.csv", index=False)
    by_fold.to_csv(out / "label_source_replay_by_fold.csv", index=False)
    by_age.to_csv(out / "label_source_replay_by_age.csv", index=False)
    by_margin.to_csv(out / "label_source_replay_by_terminal_margin_band.csv", index=False)
    attribution.to_csv(out / "label_source_replay_disagreement_attribution.csv", index=False)
    incremental.to_csv(out / "label_source_replay_incremental_vs_baseline.csv", index=False)
    write_optional_parquet(selected, out / "selected_first_entries.parquet")
    diagnostics = {
        "selected_entries": int(len(selected)),
        "selected_trade_disagreement_rate": float((~selected["label_agree"].astype(bool)).mean()) if not selected.empty and "label_agree" in selected else None,
        "min_notional_check_status": selected["min_notional_check_status"].value_counts().to_dict() if not selected.empty else {},
    }
    (out / "label_source_replay_diagnostics.json").write_text(json.dumps(diagnostics, indent=2, default=str), encoding="utf-8")
    (out / "label_source_replay_readme.txt").write_text(render_readme(args, score, diagnostics), encoding="utf-8")
    return diagnostics


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Rescore offline probability edge replay by Binance/Chainlink label source.")
    parser.add_argument("--replay-trades", type=Path, required=True)
    parser.add_argument("--market-label-audit", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--label-sources", default="binance,chainlink,agreement_only,disagreement_only")
    parser.add_argument("--one-entry-per-market", type=lambda x: str(x).lower() in {"1", "true", "yes"}, default=True)
    parser.add_argument("--entry-ages", default="60,120,180")
    parser.add_argument("--edge-thresholds", default="0.0,0.01,0.02,0.03,0.05,0.07,0.10")
    parser.add_argument("--models")
    parser.add_argument("--min-trade-notional-usdc", type=float, default=1.0)
    parser.add_argument("--stake-usdc", type=float, default=1.0)
    parser.add_argument("--slippage-bps", default="0,50,100,200,500")
    parser.add_argument("--fee-mode", default="polymarket_crypto_formula")
    parser.add_argument("--fee-rate", type=float, default=0.07)
    parser.add_argument("--require-cost-adjusted-edge", type=lambda x: str(x).lower() in {"1", "true", "yes"}, default=True)
    parser.add_argument("--max-entry-price")
    parser.add_argument("--terminal-margin-bands-usd", default="1,2,5,10,20,50,100")
    return parser


def main(argv: list[str] | None = None) -> int:
    diagnostics = run(build_parser().parse_args(argv))
    print(json.dumps(diagnostics, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
