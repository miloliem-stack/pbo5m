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


DEFAULT_MODELS = [
    "brownian_zero_drift__rv30",
    "gbm_zero_drift__rv30_no_ito",
    "gbm_winsorized_sigma__w30__z2.5",
    "gbm_blended_sigma__50_30_20",
    "calibrated_logistic__gbm_rv30",
    "baseline_50",
]
DEFAULT_STAKES = [1.0, 2.0, 5.0, 10.0, 20.0, 50.0]
DEFAULT_THRESHOLDS = [0.01, 0.02, 0.03, 0.04, 0.05]
DEFAULT_ENTRY_WINDOWS = "0:60,0:120,0:180,0:300"


def parse_csv(value: str) -> list[str]:
    return [item.strip() for item in str(value).split(",") if item.strip()]


def parse_floats(value: str) -> list[float]:
    return [float(item) for item in parse_csv(value)]


def parse_windows(value: str) -> list[tuple[float, float, str]]:
    windows = []
    for item in parse_csv(value):
        lo, hi = item.split(":", 1)
        windows.append((float(lo), float(hi), f"{float(lo):g}_{float(hi):g}"))
    return windows


def bool_arg(value: str | bool) -> bool:
    if isinstance(value, bool):
        return value
    lower = str(value).strip().lower()
    if lower in {"1", "true", "yes", "y", "on"}:
        return True
    if lower in {"0", "false", "no", "n", "off"}:
        return False
    raise argparse.ArgumentTypeError(f"expected bool, got {value!r}")


def read_frame(path: Path, columns: list[str] | None = None) -> pd.DataFrame:
    if path.exists() and path.suffix.lower() == ".parquet":
        try:
            return pd.read_parquet(path, columns=columns)
        except ImportError as exc:
            sidecar = Path(str(path) + ".as.json")
            if sidecar.exists():
                return pd.read_json(sidecar, lines=True)
            raise exc
    sidecar = Path(str(path) + ".as.json")
    if sidecar.exists():
        return pd.read_json(sidecar, lines=True)
    return pd.read_csv(path, usecols=lambda col: columns is None or col in columns)


def write_parquet_or_json(frame: pd.DataFrame, path: Path) -> str:
    try:
        frame.to_parquet(path, index=False)
        return str(path)
    except Exception:
        fallback = Path(str(path) + ".as.json")
        frame.to_json(fallback, orient="records", lines=True, date_format="iso")
        return str(fallback)


def find_prediction_file(root: Path) -> Path | None:
    if root.is_file():
        return root
    names = [
        "probability_predictions.parquet",
        "probability_predictions_sample.parquet",
        "probability_predictions.csv",
        "probability_predictions_sample.csv",
    ]
    for name in names:
        candidate = root / name
        if candidate.exists():
            return candidate
    return None


def chronological_slice(ts: pd.Timestamp) -> str:
    if pd.isna(ts):
        return "unknown"
    day = pd.Timestamp(ts).tz_convert("UTC").date()
    if pd.Timestamp("2026-04-23").date() <= day <= pd.Timestamp("2026-04-30").date():
        return "early"
    if pd.Timestamp("2026-05-01").date() <= day <= pd.Timestamp("2026-05-08").date():
        return "main"
    if pd.Timestamp("2026-05-09").date() <= day <= pd.Timestamp("2026-05-11").date():
        return "fresh"
    return "out_of_named_range"


def load_compact(compact_root: Path, start_date: str | None, end_date: str | None, max_markets: int | None) -> tuple[pd.DataFrame, pd.DataFrame]:
    windows = read_frame(compact_root / "market_windows.parquet")
    ticks = read_frame(compact_root / "book_ticks.parquet")
    windows["market_start_ts"] = pd.to_datetime(windows["market_start_ts"], utc=True, errors="coerce")
    windows["market_end_ts"] = pd.to_datetime(windows["market_end_ts"], utc=True, errors="coerce")
    ticks["ts"] = pd.to_datetime(ticks["ts"], utc=True, errors="coerce")
    if start_date:
        windows = windows[windows["market_start_ts"] >= pd.Timestamp(start_date, tz="UTC")]
    if end_date:
        windows = windows[windows["market_start_ts"] < pd.Timestamp(end_date, tz="UTC") + pd.Timedelta(days=1)]
    windows = windows.sort_values("market_start_ts").reset_index(drop=True)
    if max_markets:
        windows = windows.head(max_markets)
    ticks = ticks[ticks["market_key"].isin(set(windows["market_key"]))]
    return windows, ticks


def prepare_quote_snapshots(ticks: pd.DataFrame, windows: pd.DataFrame, *, valid_topbook_only: bool) -> pd.DataFrame:
    frame = ticks.copy()
    if valid_topbook_only and "is_valid_topbook" in frame.columns:
        frame = frame[frame["is_valid_topbook"].astype(bool)]
    frame["side"] = frame["side"].astype(str).str.upper()
    ask_sz_cols = [c for c in frame.columns if c.startswith("ask_sz_")]
    if "visible_ask_depth_top1" not in frame.columns and "ask_sz_1" in frame.columns:
        frame["visible_ask_depth_top1"] = pd.to_numeric(frame["ask_sz_1"], errors="coerce")
    if "visible_ask_depth_top3" not in frame.columns:
        frame["visible_ask_depth_top3"] = frame[[c for c in ask_sz_cols if c in {"ask_sz_1", "ask_sz_2", "ask_sz_3"}]].apply(pd.to_numeric, errors="coerce").sum(axis=1)

    keys = ["market_key", "ts"]
    meta_cols = [c for c in ["market_age_sec", "seconds_to_end", "spread"] if c in frame.columns]
    yes = frame[frame["side"].eq("YES")][keys + meta_cols + [c for c in frame.columns if c.startswith("ask_px_") or c.startswith("ask_sz_") or c in ["visible_ask_depth_top1", "visible_ask_depth_top3"]]].copy()
    no = frame[frame["side"].eq("NO")][keys + meta_cols + [c for c in frame.columns if c.startswith("ask_px_") or c.startswith("ask_sz_") or c in ["visible_ask_depth_top1", "visible_ask_depth_top3"]]].copy()
    snap = yes.merge(no, on=keys, how="outer", suffixes=("_yes", "_no"))
    if "market_age_sec_yes" in snap.columns:
        snap["entry_age_sec"] = snap["market_age_sec_yes"].combine_first(snap.get("market_age_sec_no"))
    elif "market_age_sec" in snap.columns:
        snap["entry_age_sec"] = snap["market_age_sec"]
    else:
        snap["entry_age_sec"] = np.nan
    snap["yes_ask"] = pd.to_numeric(snap.get("ask_px_1_yes"), errors="coerce")
    snap["no_ask"] = pd.to_numeric(snap.get("ask_px_1_no"), errors="coerce")
    if "spread_yes" in snap.columns or "spread_no" in snap.columns:
        snap["spread"] = pd.concat([pd.to_numeric(snap.get("spread_yes"), errors="coerce"), pd.to_numeric(snap.get("spread_no"), errors="coerce")], axis=1).mean(axis=1)
    else:
        snap["spread"] = np.nan
    cols = ["market_key", "market_start_ts", "market_end_ts", "winner_side"]
    snap = snap.merge(windows[cols], on="market_key", how="inner")
    snap["chronological_slice"] = snap["market_start_ts"].map(chronological_slice)
    snap["entry_date"] = snap["market_start_ts"].dt.date.astype(str)
    return snap.sort_values(["market_key", "ts"], kind="mergesort").reset_index(drop=True)


def load_predictions(predictions_root: Path | None, requested_models: list[str], windows: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any], list[str]]:
    resolution: dict[str, Any] = {}
    non_baseline = [m for m in requested_models if m != "baseline_50"]
    if not non_baseline:
        return pd.DataFrame(), {"baseline_50": {"source": "direct_constant", "p_yes": 0.5}}, []
    if predictions_root is None:
        missing = non_baseline
        return pd.DataFrame(), {"baseline_50": {"source": "direct_constant", "p_yes": 0.5}}, missing
    pred_file = find_prediction_file(predictions_root)
    if pred_file is None:
        return pd.DataFrame(), {"error": f"no prediction file found under {predictions_root}"}, non_baseline
    preds = read_frame(pred_file)
    model_col = next((c for c in ["model_id", "model_name", "model"] if c in preds.columns), None)
    prob_col = next((c for c in ["p_up", "p_yes", "probability", "y_prob"] if c in preds.columns), None)
    ts_col = next((c for c in ["timestamp", "prediction_ts", "ts"] if c in preds.columns), None)
    start_col = next((c for c in ["market_window_start", "market_start_ts"] if c in preds.columns), None)
    if model_col is None or prob_col is None or ts_col is None or start_col is None:
        return pd.DataFrame(), {"error": f"prediction artifact missing required columns; found={preds.columns.tolist()}"}, non_baseline
    preds = preds.rename(columns={model_col: "model_name", prob_col: "p_yes", ts_col: "prediction_ts", start_col: "market_start_ts"})
    preds["model_name"] = preds["model_name"].astype(str)
    preds["prediction_ts"] = pd.to_datetime(preds["prediction_ts"], utc=True, errors="coerce")
    preds["market_start_ts"] = pd.to_datetime(preds["market_start_ts"], utc=True, errors="coerce")
    all_found = sorted(preds[preds["model_name"].isin(non_baseline)]["model_name"].dropna().unique().tolist())
    prediction_start_min = preds["market_start_ts"].min()
    prediction_start_max = preds["market_start_ts"].max()
    compact_start_min = windows["market_start_ts"].min()
    compact_start_max = windows["market_start_ts"].max()
    preds = preds[preds["model_name"].isin(non_baseline)].copy()
    starts = set(windows["market_start_ts"])
    preds = preds[preds["market_start_ts"].isin(starts)]
    preds["p_yes"] = pd.to_numeric(preds["p_yes"], errors="coerce").clip(0.0, 1.0)
    found = sorted(preds["model_name"].dropna().unique().tolist())
    missing = [m for m in non_baseline if m not in found]
    resolution = {
        "prediction_file": str(pred_file),
        "baseline_50": {"source": "direct_constant", "p_yes": 0.5},
        "models_available_before_date_filter": all_found,
        "prediction_market_start_min": prediction_start_min,
        "prediction_market_start_max": prediction_start_max,
        "compact_market_start_min": compact_start_min,
        "compact_market_start_max": compact_start_max,
        "overlap_note": "requested non-baseline models must exist after filtering predictions to compact market_start_ts values",
    }
    for model in found:
        resolution[model] = {"source": "prediction_artifact", "prediction_file": str(pred_file), "implementation_reference": "scripts/sweep_probability_models_5m.py:model_predictions_for_fold"}
    return preds[["market_start_ts", "prediction_ts", "model_name", "p_yes"]].dropna(), resolution, missing


def attach_predictions(snapshots: pd.DataFrame, predictions: pd.DataFrame, models: list[str]) -> pd.DataFrame:
    frames = []
    base_models = [m for m in models if m == "baseline_50"]
    if base_models:
        base = snapshots.copy()
        base["model_name"] = "baseline_50"
        base["p_yes"] = 0.5
        frames.append(base)
    non_base = [m for m in models if m != "baseline_50"]
    if non_base and not predictions.empty:
        left = snapshots.sort_values("ts").copy()
        pieces = []
        for model in non_base:
            pred = predictions[predictions["model_name"].eq(model)].sort_values("prediction_ts")
            if pred.empty:
                continue
            merged = pd.merge_asof(
                left.sort_values("ts"),
                pred.sort_values("prediction_ts"),
                left_on="ts",
                right_on="prediction_ts",
                by="market_start_ts",
                direction="backward",
                allow_exact_matches=True,
            )
            merged["model_name"] = model
            pieces.append(merged[merged["p_yes"].notna()])
        frames.extend(pieces)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def side_level_value(row: pd.Series, side: str, prefix: str) -> float:
    suffix = "yes" if side == "YES" else "no"
    return float(pd.to_numeric(row.get(f"{prefix}_{suffix}"), errors="coerce"))


def choose_candidates(frame: pd.DataFrame, edge_thresholds: list[float], entry_windows: list[tuple[float, float, str]], first_entry_only: bool) -> tuple[pd.DataFrame, int]:
    rows = []
    tie_count = 0
    data = frame.copy()
    data["yes_edge"] = pd.to_numeric(data["p_yes"], errors="coerce") - pd.to_numeric(data["yes_ask"], errors="coerce")
    data["no_edge"] = (1.0 - pd.to_numeric(data["p_yes"], errors="coerce")) - pd.to_numeric(data["no_ask"], errors="coerce")
    data["tie"] = np.isclose(data["yes_edge"], data["no_edge"], equal_nan=False)
    tie_count = int(data["tie"].sum())
    data["side"] = np.where(data["yes_edge"].fillna(-np.inf) >= data["no_edge"].fillna(-np.inf), "YES", "NO")
    data["model_edge"] = np.where(data["side"].eq("YES"), data["yes_edge"], data["no_edge"])
    data["best_edge"] = data["model_edge"]
    data["entry_ask"] = np.where(data["side"].eq("YES"), data["yes_ask"], data["no_ask"])
    data = data[np.isfinite(data["model_edge"]) & np.isfinite(data["entry_ask"])]
    for threshold in edge_thresholds:
        eligible_threshold = data[data["model_edge"] >= threshold].copy()
        eligible_threshold["edge_threshold"] = threshold
        for lo, hi, label in entry_windows:
            eligible = eligible_threshold[(eligible_threshold["entry_age_sec"] >= lo) & (eligible_threshold["entry_age_sec"] < hi)].copy()
            eligible["entry_age_window"] = label
            eligible = eligible.sort_values(["model_name", "market_key", "ts", "entry_age_sec"], kind="mergesort")
            if first_entry_only:
                eligible = eligible.drop_duplicates(["model_name", "market_key", "edge_threshold", "entry_age_window"], keep="first")
            rows.append(eligible)
    return (pd.concat(rows, ignore_index=True) if rows else pd.DataFrame(), tie_count)


def fill_row(row: pd.Series, stake: float, top_n: int, capacity_aware: bool) -> dict[str, float | bool]:
    suffix = "yes" if str(row["side"]).upper() == "YES" else "no"
    remaining = float(stake)
    shares = 0.0
    cost = 0.0
    levels = top_n if capacity_aware else min(1, top_n)
    for idx in range(1, levels + 1):
        px = pd.to_numeric(row.get(f"ask_px_{idx}_{suffix}"), errors="coerce")
        sz = pd.to_numeric(row.get(f"ask_sz_{idx}_{suffix}"), errors="coerce")
        if not np.isfinite(px) or not np.isfinite(sz) or px <= 0 or px > 1 or sz <= 0:
            continue
        spend = min(remaining, float(px * sz))
        if spend <= 0:
            continue
        shares += spend / float(px)
        cost += spend
        remaining -= spend
        if remaining <= 1e-12:
            break
    return {"filled_shares": shares, "gross_cost": cost, "fill_rate": cost / stake if stake else 0.0, "capacity_shortfall": bool(cost + 1e-12 < stake)}


def simulate(candidates: pd.DataFrame, stakes: list[float], top_n: int, capacity_aware: bool) -> tuple[pd.DataFrame, pd.DataFrame]:
    trade_rows = []
    market_rows = []
    for _, row in candidates.iterrows():
        for stake in stakes:
            fill = fill_row(row, stake, top_n, capacity_aware)
            win = str(row["side"]).upper() == str(row["winner_side"]).upper()
            payout = fill["filled_shares"] if win else 0.0
            pnl = payout - fill["gross_cost"]
            visible_top1 = side_level_value(row, str(row["side"]).upper(), "visible_ask_depth_top1") if f"visible_ask_depth_top1_{str(row['side']).lower()}" in row.index else np.nan
            visible_top3 = side_level_value(row, str(row["side"]).upper(), "visible_ask_depth_top3") if f"visible_ask_depth_top3_{str(row['side']).lower()}" in row.index else np.nan
            base = {
                "model_name": row["model_name"],
                "model_id": row["model_name"],
                "market_key": row["market_key"],
                "market_start_ts": row["market_start_ts"],
                "market_end_ts": row["market_end_ts"],
                "ts": row["ts"],
                "entry_ts": row["ts"],
                "side": row["side"],
                "winner_side": row["winner_side"],
                "p_yes": row["p_yes"],
                "yes_edge": row["yes_edge"],
                "no_edge": row["no_edge"],
                "model_edge": row["model_edge"],
                "best_edge": row["best_edge"],
                "edge_threshold": row["edge_threshold"],
                "stake_size": stake,
                "notional_requested": stake,
                "entry_age_sec": row["entry_age_sec"],
                "entry_age_window": row["entry_age_window"],
                "chronological_slice": row["chronological_slice"],
                "entry_date": row["entry_date"],
                "entry_ask": row["entry_ask"],
                "filled_shares": fill["filled_shares"],
                "notional_filled": fill["gross_cost"],
                "gross_cost": fill["gross_cost"],
                "gross_payout": payout,
                "gross_pnl": pnl,
                "roi_on_filled_cost": pnl / fill["gross_cost"] if fill["gross_cost"] else np.nan,
                "win": bool(win),
                "fill_rate": fill["fill_rate"],
                "capacity_shortfall": fill["capacity_shortfall"],
                "spread": row.get("spread", np.nan),
                "visible_ask_depth_top1": visible_top1,
                "visible_ask_depth_top3": visible_top3,
                "status": "filled" if fill["gross_cost"] > 0 else "no_fill",
            }
            market_rows.append(base)
            if base["status"] == "filled":
                trade_rows.append({k: v for k, v in base.items() if k != "status"})
    return pd.DataFrame(trade_rows), pd.DataFrame(market_rows)


def add_full_slice(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame
    full = frame.copy()
    full["chronological_slice"] = "full"
    return pd.concat([frame, full], ignore_index=True)


def summarize(market_results: pd.DataFrame, group_cols: list[str]) -> pd.DataFrame:
    if market_results.empty:
        return pd.DataFrame()
    rows = []
    for keys, group in market_results.groupby(group_cols, dropna=False, sort=True):
        if not isinstance(keys, tuple):
            keys = (keys,)
        filled = group[group["status"].eq("filled")]
        cost = pd.to_numeric(filled.get("gross_cost"), errors="coerce").sum() if not filled.empty else 0.0
        pnl = pd.to_numeric(filled.get("gross_pnl"), errors="coerce").sum() if not filled.empty else 0.0
        side = filled.get("side", pd.Series(dtype=str)).astype(str).str.upper()
        row = dict(zip(group_cols, keys))
        row.update(
            {
                "trade_count": int(len(filled)),
                "market_count": int(filled["market_key"].nunique()) if not filled.empty else 0,
                "notional_requested": float(pd.to_numeric(group.get("notional_requested"), errors="coerce").sum()),
                "notional_filled": float(cost),
                "fill_rate": float(cost / pd.to_numeric(group.get("notional_requested"), errors="coerce").sum()) if pd.to_numeric(group.get("notional_requested"), errors="coerce").sum() else np.nan,
                "gross_cost": float(cost),
                "gross_payout": float(pd.to_numeric(filled.get("gross_payout"), errors="coerce").sum()) if not filled.empty else 0.0,
                "gross_pnl": float(pnl),
                "roi_on_filled_cost": float(pnl / cost) if cost else np.nan,
                "win_rate": float(pd.to_numeric(filled.get("win"), errors="coerce").mean()) if not filled.empty else np.nan,
                "avg_p_yes": float(pd.to_numeric(filled.get("p_yes"), errors="coerce").mean()) if not filled.empty else np.nan,
                "avg_entry_ask": float(pd.to_numeric(filled.get("entry_ask"), errors="coerce").mean()) if not filled.empty else np.nan,
                "avg_model_edge": float(pd.to_numeric(filled.get("model_edge"), errors="coerce").mean()) if not filled.empty else np.nan,
                "median_model_edge": float(pd.to_numeric(filled.get("model_edge"), errors="coerce").median()) if not filled.empty else np.nan,
                "avg_entry_age_sec": float(pd.to_numeric(filled.get("entry_age_sec"), errors="coerce").mean()) if not filled.empty else np.nan,
                "median_entry_age_sec": float(pd.to_numeric(filled.get("entry_age_sec"), errors="coerce").median()) if not filled.empty else np.nan,
                "capacity_shortfall_count": int(pd.to_numeric(group.get("capacity_shortfall"), errors="coerce").sum()),
                "capacity_shortfall_rate": float(pd.to_numeric(group.get("capacity_shortfall"), errors="coerce").mean()) if len(group) else np.nan,
                "yes_trade_count": int(side.eq("YES").sum()),
                "no_trade_count": int(side.eq("NO").sum()),
                "yes_trade_share": float(side.eq("YES").mean()) if len(side) else np.nan,
                "no_trade_share": float(side.eq("NO").mean()) if len(side) else np.nan,
            }
        )
        rows.append(row)
    return pd.DataFrame(rows)


def ensure_model_summary_rows(summary: pd.DataFrame, models: list[str]) -> pd.DataFrame:
    if "model_name" not in summary.columns:
        return summary
    missing = [model for model in models if model not in set(summary["model_name"].astype(str))]
    if not missing:
        return summary
    rows = []
    for model in missing:
        row = {col: np.nan for col in summary.columns}
        row["model_name"] = model
        for col in [
            "trade_count",
            "market_count",
            "notional_requested",
            "notional_filled",
            "gross_cost",
            "gross_payout",
            "gross_pnl",
            "capacity_shortfall_count",
            "yes_trade_count",
            "no_trade_count",
        ]:
            if col in row:
                row[col] = 0
        rows.append(row)
    return pd.concat([summary, pd.DataFrame(rows)], ignore_index=True)


def write_readme(path: Path, args: argparse.Namespace, manifest: dict[str, Any]) -> None:
    lines = [
        "Probability model set capacity stress",
        "",
        "Offline research only. No live trading behavior is changed.",
        "Uses compact recorder market_windows/book_ticks and Chainlink-aligned winner_side settlement.",
        "Non-baseline model probabilities are loaded from the prediction artifact; they are not refit on this replay slice.",
        "Quote prices use executable asks from compact topbook/depth columns, not raw recorder best_bid/best_ask fields.",
        "",
        f"compact_root={args.compact_root}",
        f"predictions_root={args.predictions_root}",
        f"models={args.models}",
        f"stake_sizes={args.stake_sizes}",
        f"edge_thresholds={args.edge_thresholds}",
        f"entry_age_windows={args.entry_age_windows}",
        f"top_n_levels={args.top_n_levels}",
        f"valid_topbook_only={args.valid_topbook_only}",
        f"first_entry_only={args.first_entry_only}",
        f"capacity_aware={args.capacity_aware}",
        "",
        f"models_resolved={manifest.get('models_resolved')}",
        f"missing_models={manifest.get('missing_models')}",
        f"trade_rows={manifest.get('trade_rows')}",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run(args: argparse.Namespace) -> dict[str, Any]:
    output_dir = Path(args.output_root) / args.run_name
    if output_dir.exists():
        if not args.overwrite:
            raise FileExistsError(f"{output_dir} exists; pass --overwrite")
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    models = parse_csv(args.models)
    stakes = parse_floats(args.stake_sizes)
    thresholds = parse_floats(args.edge_thresholds)
    entry_windows = parse_windows(args.entry_age_windows)
    windows, ticks = load_compact(Path(args.compact_root), args.start_date, args.end_date, args.max_markets)
    if args.settlement_source != "chainlink":
        raise ValueError("only --settlement-source chainlink is currently supported")
    windows = windows[windows["winner_side"].isin(["YES", "NO"])].copy()
    snapshots = prepare_quote_snapshots(ticks, windows, valid_topbook_only=args.valid_topbook_only)
    predictions, resolution, missing_models = load_predictions(Path(args.predictions_root) if args.predictions_root else None, models, windows)
    (output_dir / "model_resolution_report.json").write_text(json.dumps(resolution, indent=2, default=str), encoding="utf-8")
    if missing_models:
        missing_report = {
            "missing_models": missing_models,
            "requested_models": models,
            "models_available_before_date_filter": resolution.get("models_available_before_date_filter"),
            "prediction_market_start_min": resolution.get("prediction_market_start_min"),
            "prediction_market_start_max": resolution.get("prediction_market_start_max"),
            "compact_market_start_min": resolution.get("compact_market_start_min"),
            "compact_market_start_max": resolution.get("compact_market_start_max"),
            "note": "non-baseline models must be present in --predictions-root for the compact market dates; no stale probabilities or silent substitution are used",
        }
        (output_dir / "missing_model_report.json").write_text(json.dumps(missing_report, indent=2, default=str), encoding="utf-8")
        raise RuntimeError(f"missing requested models: {missing_models}")
    predicted = attach_predictions(snapshots, predictions, models)
    candidates, tie_count = choose_candidates(predicted, thresholds, entry_windows, args.first_entry_only)
    trades, market_results = simulate(candidates, stakes, min(args.top_n_levels, 100), args.capacity_aware)
    market_summary_frame = add_full_slice(market_results)

    outputs = {
        "stress_summary.csv": ["chronological_slice", "model_name", "stake_size", "edge_threshold", "entry_age_window"],
        "stress_summary_by_model.csv": ["model_name"],
        "stress_summary_by_model_and_stake.csv": ["model_name", "stake_size"],
        "stress_summary_by_model_and_edge_threshold.csv": ["model_name", "edge_threshold"],
        "stress_summary_by_model_and_entry_age_window.csv": ["model_name", "entry_age_window"],
        "stress_summary_by_model_and_chronological_slice.csv": ["model_name", "chronological_slice"],
        "stress_summary_by_model_and_date.csv": ["model_name", "entry_date"],
        "stress_summary_by_model_and_side.csv": ["model_name", "side"],
    }
    for filename, group_cols in outputs.items():
        source = market_summary_frame if "chronological_slice" in group_cols else market_results
        summary = summarize(source, group_cols)
        if filename == "stress_summary_by_model.csv":
            summary = ensure_model_summary_rows(summary, models)
        summary.to_csv(output_dir / filename, index=False)
    write_parquet_or_json(trades, output_dir / "trade_level_results.parquet")
    write_parquet_or_json(market_results, output_dir / "market_level_results.parquet")
    skipped = pd.DataFrame(
        {
            "skip_reason": ["no_label_or_no_valid_quote", "no_prediction", "no_edge_entry"],
            "count": [
                int(read_frame(Path(args.compact_root) / "market_windows.parquet")["market_key"].nunique() - windows["market_key"].nunique()),
                int(max(0, len(snapshots) * len([m for m in models if m != "baseline_50"]) - len(predicted[predicted["model_name"].ne("baseline_50")]))),
                int(max(0, len(predicted) - len(candidates))),
            ],
        }
    )
    skipped.to_csv(output_dir / "skipped_markets.csv", index=False)
    resolved_models = sorted(set(trades["model_name"].unique().tolist())) if not trades.empty else []
    manifest = {
        "compact_root": str(args.compact_root),
        "predictions_root": str(args.predictions_root) if args.predictions_root else None,
        "output_dir": str(output_dir),
        "requested_models": models,
        "models_resolved": resolved_models,
        "missing_models": missing_models,
        "settlement_source": args.settlement_source,
        "market_rows": int(len(windows)),
        "snapshot_rows": int(len(snapshots)),
        "predicted_snapshot_rows": int(len(predicted)),
        "candidate_rows_before_stake_expansion": int(len(candidates)),
        "trade_rows": int(len(trades)),
        "tie_count": int(tie_count),
        "p_yes_min": float(trades["p_yes"].min()) if not trades.empty else None,
        "p_yes_max": float(trades["p_yes"].max()) if not trades.empty else None,
        "first_entry_only": bool(args.first_entry_only),
        "valid_topbook_only": bool(args.valid_topbook_only),
        "top_n_levels": int(args.top_n_levels),
        "capacity_aware": bool(args.capacity_aware),
    }
    (output_dir / "run_manifest.json").write_text(json.dumps(manifest, indent=2, default=str), encoding="utf-8")
    write_readme(output_dir / "README.txt", args, manifest)
    return manifest


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Focused BTC-5m probability-model capacity stress replay from compact recorder data.")
    parser.add_argument("--compact-root", type=Path, required=True)
    parser.add_argument("--binance-root", type=Path)
    parser.add_argument("--predictions-root", type=Path)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--run-name", required=True)
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    parser.add_argument("--models", default=",".join(DEFAULT_MODELS))
    parser.add_argument("--stake-sizes", default=",".join(f"{x:g}" for x in DEFAULT_STAKES))
    parser.add_argument("--edge-thresholds", default=",".join(f"{x:g}" for x in DEFAULT_THRESHOLDS))
    parser.add_argument("--entry-age-windows", default=DEFAULT_ENTRY_WINDOWS)
    parser.add_argument("--top-n-levels", type=int, default=3)
    parser.add_argument("--valid-topbook-only", type=bool_arg, default=True)
    parser.add_argument("--first-entry-only", type=bool_arg, default=True)
    parser.add_argument("--capacity-aware", type=bool_arg, default=True)
    parser.add_argument("--settlement-source", default="chainlink")
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--max-markets", type=int)
    return parser


def main(argv: list[str] | None = None) -> int:
    manifest = run(build_parser().parse_args(argv))
    print(json.dumps(manifest, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
