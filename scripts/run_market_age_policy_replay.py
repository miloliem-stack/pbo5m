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

from scripts import run_hmm_regime_veto_attribution as hmm_veto
from scripts import run_probability_model_set_capacity_stress as stress


DEFAULT_COMPACT_ROOT = Path("artifacts/compact_market_recorder/2026-04-23_to_2026-05-11")
DEFAULT_PREDICTIONS_ROOT = Path("artifacts/probability_models_5m/compact_overlap_20260423_20260511_predictions")
DEFAULT_STRESS_ROOT = Path("artifacts/probability_model_set_capacity_stress/compact_20260423_20260511_six_models_v1")
DEFAULT_HMM_ATTR_ROOT = Path("artifacts/hmm_regime_veto_attribution/compact_20260423_20260511_phase1_v2")
DEFAULT_HMM_CONTEXT_PATH = Path("artifacts/macro_context_edge_attribution/compact_20260423_20260511_phase1_v1/trade_level_with_macro_context.parquet")
FOCUS_HMM_MODEL = "laplace_1m__gaussian_hmm__k4"
FOCUS_HMM_STATE = 3
BAD_HMM_MODEL = "core_1m__gaussian_hmm__k4"
LOGISTIC_MODEL = "calibrated_logistic__gbm_rv30"
SIMPLE_MODELS = {
    "brownian_zero_drift__rv30",
    "gbm_zero_drift__rv30_no_ito",
    "gbm_winsorized_sigma__w30__z2.5",
    "gbm_blended_sigma__50_30_20",
}
DEFAULT_WINDOWS = "0:30,30:60,60:90,90:120,120:180,180:240,240:300,0:300,30:300,60:300,90:300,120:300,180:300,240:300,30:180,60:180,60:240,90:240"
ASK_BINS = [-np.inf, 0.30, 0.35, 0.40, 0.45, 0.47, 0.49, 0.50, 0.55, 0.60, np.inf]
ASK_LABELS = ["<=0.30", "0.30_0.35", "0.35_0.40", "0.40_0.45", "0.45_0.47", "0.47_0.49", "0.49_0.50", "0.50_0.55", "0.55_0.60", ">0.60"]


def parse_csv(value: str) -> list[str]:
    return [item.strip() for item in str(value).split(",") if item.strip()]


def parse_windows(value: str) -> list[tuple[float, float, str]]:
    out = []
    for item in parse_csv(value):
        lo, hi = item.split(":", 1)
        out.append((float(lo), float(hi), f"{float(lo):g}_{float(hi):g}"))
    return out


def bool_arg(value: str | bool) -> bool:
    return stress.bool_arg(value)


def model_slug(model_id: str) -> str:
    return model_id.replace("__", "_").replace("-", "_").replace(".", "_")


def add_edge_columns(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    out["yes_edge"] = pd.to_numeric(out["p_yes"], errors="coerce") - pd.to_numeric(out["yes_ask"], errors="coerce")
    out["no_edge"] = (1.0 - pd.to_numeric(out["p_yes"], errors="coerce")) - pd.to_numeric(out["no_ask"], errors="coerce")
    out["side"] = np.where(out["yes_edge"].fillna(-np.inf) >= out["no_edge"].fillna(-np.inf), "YES", "NO")
    out["model_edge"] = np.where(out["side"].eq("YES"), out["yes_edge"], out["no_edge"])
    out["best_edge"] = out["model_edge"]
    out["entry_ask"] = np.where(out["side"].eq("YES"), out["yes_ask"], out["no_ask"])
    out = out[np.isfinite(out["model_edge"]) & np.isfinite(out["entry_ask"])].copy()
    out["ask_bin"] = pd.cut(pd.to_numeric(out["entry_ask"], errors="coerce"), ASK_BINS, labels=ASK_LABELS, right=True).astype("object").fillna("missing")
    return out


def state_rows_from_context(path: Path, models: list[str]) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"HMM context path does not exist: {path}")
    frame = hmm_veto.read_frame(path)
    required = {"timestamp", "hmm_model_id", "hmm_state", "hmm_pmax"}
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f"HMM context path missing required columns: {sorted(missing)}")
    out = frame[list(required)].copy()
    out["timestamp"] = pd.to_datetime(out["timestamp"], utc=True, errors="coerce").dt.as_unit("ns")
    out["hmm_model_id"] = out["hmm_model_id"].astype(str)
    out = out[out["hmm_model_id"].isin(models)].dropna(subset=["timestamp", "hmm_state", "hmm_pmax"])
    out = out.sort_values(["hmm_model_id", "timestamp"]).drop_duplicates(["hmm_model_id", "timestamp"], keep="last")
    if out.empty:
        raise ValueError(f"HMM context has no state rows for requested models: {models}")
    return out


def attach_hmm_context(candidates: pd.DataFrame, state_rows: pd.DataFrame, models: list[str]) -> pd.DataFrame:
    out = candidates.sort_values("ts", kind="mergesort").copy()
    out["ts"] = pd.to_datetime(out["ts"], utc=True, errors="coerce").dt.as_unit("ns")
    for model in models:
        slug = model_slug(model)
        states = state_rows[state_rows["hmm_model_id"].eq(model)].sort_values("timestamp")
        if states.empty:
            raise ValueError(f"missing HMM state rows for {model}")
        joined = pd.merge_asof(
            out[["ts"]].sort_values("ts"),
            states[["timestamp", "hmm_state", "hmm_pmax"]].assign(timestamp=lambda x: pd.to_datetime(x["timestamp"], utc=True, errors="coerce").dt.as_unit("ns")).rename(columns={"timestamp": "ts"}),
            on="ts",
            direction="backward",
            allow_exact_matches=True,
        )
        out = out.sort_values("ts", kind="mergesort").reset_index(drop=True)
        out[f"{slug}_state"] = joined["hmm_state"].to_numpy()
        out[f"{slug}_pmax"] = joined["hmm_pmax"].to_numpy()
        if out[f"{slug}_state"].isna().any():
            raise ValueError(f"previous-only HMM join produced missing state for {model}")
    return out


def policy_specs(pmax_thresholds: list[float], include_bad_state_veto: bool) -> list[dict[str, Any]]:
    specs = [{"policy_name": "base_all_models_original_like", "kind": "base", "pmax": None}]
    for threshold in pmax_thresholds:
        specs.append({"policy_name": f"state3_only_pmax_{threshold:.2f}", "kind": "state3", "pmax": threshold})
    specs.extend(
        [
            {"policy_name": "state3_ask_0.30_0.47", "kind": "state3_ask", "pmax": None},
            {"policy_name": "state3_ask_0.30_0.47_excl_logistic", "kind": "state3_ask_excl_logistic", "pmax": None},
            {"policy_name": "state3_ask_0.30_0.47_simple_models", "kind": "state3_ask_simple_models", "pmax": None},
        ]
    )
    if include_bad_state_veto:
        specs.append({"policy_name": "state3_ask_0.30_0.47_plus_bad_state_veto", "kind": "state3_ask_bad_veto", "pmax": None})
    return specs


def policy_mask(frame: pd.DataFrame, spec: dict[str, Any]) -> pd.Series:
    focus_slug = model_slug(FOCUS_HMM_MODEL)
    bad_slug = model_slug(BAD_HMM_MODEL)
    mask = pd.Series(True, index=frame.index)
    if spec["kind"] == "base":
        return mask
    mask &= pd.to_numeric(frame[f"{focus_slug}_state"], errors="coerce").eq(FOCUS_HMM_STATE)
    if spec.get("pmax") is not None:
        mask &= pd.to_numeric(frame[f"{focus_slug}_pmax"], errors="coerce").ge(float(spec["pmax"]))
    if "ask" in spec["kind"]:
        ask = pd.to_numeric(frame["entry_ask"], errors="coerce")
        mask &= ask.gt(0.30) & ask.lt(0.47)
    if spec["kind"] == "state3_ask_excl_logistic":
        mask &= ~frame["model_name"].astype(str).eq(LOGISTIC_MODEL)
    if spec["kind"] == "state3_ask_simple_models":
        mask &= frame["model_name"].astype(str).isin(SIMPLE_MODELS)
    if spec["kind"] == "state3_ask_bad_veto":
        mask &= ~pd.to_numeric(frame[f"{bad_slug}_state"], errors="coerce").eq(1)
    return mask


def select_first_entries(frame: pd.DataFrame, policies: list[dict[str, Any]], windows: list[tuple[float, float, str]], edge_thresholds: list[float]) -> pd.DataFrame:
    rows = []
    base = frame.sort_values(["model_name", "market_key", "ts", "entry_age_sec"], kind="mergesort").copy()
    for threshold in edge_thresholds:
        threshold_frame = base[pd.to_numeric(base["model_edge"], errors="coerce").ge(threshold)].copy()
        threshold_frame["edge_threshold"] = float(threshold)
        for spec in policies:
            policy_frame = threshold_frame[policy_mask(threshold_frame, spec)].copy()
            if policy_frame.empty:
                continue
            policy_frame["policy_name"] = spec["policy_name"]
            for lo, hi, label in windows:
                eligible = policy_frame[(pd.to_numeric(policy_frame["entry_age_sec"], errors="coerce") >= lo) & (pd.to_numeric(policy_frame["entry_age_sec"], errors="coerce") < hi)].copy()
                if eligible.empty:
                    continue
                eligible["entry_age_window"] = label
                selected = eligible.drop_duplicates(["policy_name", "model_name", "market_key", "edge_threshold", "entry_age_window"], keep="first")
                rows.append(selected)
    return pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()


def simulate_policy(candidates: pd.DataFrame, stakes: list[float], top_n: int = 3, capacity_aware: bool = True) -> pd.DataFrame:
    rows = []
    focus_slug = model_slug(FOCUS_HMM_MODEL)
    for _, row in candidates.iterrows():
        for stake in stakes:
            fill = stress.fill_row(row, stake, top_n, capacity_aware)
            if fill["gross_cost"] <= 0:
                continue
            win = str(row["side"]).upper() == str(row["winner_side"]).upper()
            payout = fill["filled_shares"] if win else 0.0
            pnl = payout - fill["gross_cost"]
            rows.append(
                {
                    "policy_name": row["policy_name"],
                    "model_name": row["model_name"],
                    "model_id": row["model_name"],
                    "market_key": row["market_key"],
                    "market_id": row["market_key"],
                    "market_start_ts": row["market_start_ts"],
                    "market_end_ts": row["market_end_ts"],
                    "ts": row["ts"],
                    "entry_ts": row["ts"],
                    "side": row["side"],
                    "winner_side": row["winner_side"],
                    "p_yes": row["p_yes"],
                    "model_edge": row["model_edge"],
                    "edge_threshold": row["edge_threshold"],
                    "stake_size": float(stake),
                    "entry_age_seconds": row["entry_age_sec"],
                    "entry_age_sec": row["entry_age_sec"],
                    "entry_age_window": row["entry_age_window"],
                    "chronological_slice": row["chronological_slice"],
                    "entry_date": row["entry_date"],
                    "date": row["entry_date"],
                    "entry_ask": row["entry_ask"],
                    "ask_price": row["entry_ask"],
                    "ask_bin": row.get("ask_bin", "missing"),
                    "filled_shares": fill["filled_shares"],
                    "gross_cost": fill["gross_cost"],
                    "gross_payout": payout,
                    "gross_pnl": pnl,
                    "pnl": pnl,
                    "roi": pnl / fill["gross_cost"] if fill["gross_cost"] else np.nan,
                    "win": bool(win),
                    "fill_rate": fill["fill_rate"],
                    "capacity_shortfall": fill["capacity_shortfall"],
                    "hmm_model_id": FOCUS_HMM_MODEL if str(row["policy_name"]).startswith("state3") else "",
                    "hmm_state": row.get(f"{focus_slug}_state", np.nan),
                    "hmm_pmax": row.get(f"{focus_slug}_pmax", np.nan),
                }
            )
    return pd.DataFrame(rows)


def _slice_metrics(group: pd.DataFrame) -> tuple[dict[str, float], dict[str, float]]:
    pnl_by = group.groupby("chronological_slice")["pnl"].sum().to_dict()
    roi_by = {}
    for slice_name, part in group.groupby("chronological_slice"):
        cost = pd.to_numeric(part["gross_cost"], errors="coerce").sum()
        pnl = pd.to_numeric(part["pnl"], errors="coerce").sum()
        roi_by[str(slice_name)] = float(pnl / cost) if cost else np.nan
    return {str(k): float(v) for k, v in pnl_by.items()}, roi_by


def summarize(frame: pd.DataFrame, group_cols: list[str], min_trades: int, min_markets: int) -> pd.DataFrame:
    rows = []
    grouped = [((), frame)] if not group_cols else frame.groupby(group_cols, dropna=False, sort=True)
    for keys, group in grouped:
        if not isinstance(keys, tuple):
            keys = (keys,)
        cost = pd.to_numeric(group["gross_cost"], errors="coerce").sum()
        pnl = pd.to_numeric(group["pnl"], errors="coerce").sum()
        slice_pnl, slice_roi = _slice_metrics(group)
        row = dict(zip(group_cols, keys))
        row.update(
            {
                "trade_count": int(len(group)),
                "unique_markets": int(group["market_key"].nunique()),
                "gross_cost": float(cost),
                "pnl": float(pnl),
                "roi": float(pnl / cost) if cost else np.nan,
                "win_rate": float(pd.to_numeric(group["win"], errors="coerce").mean()) if len(group) else np.nan,
                "avg_ask": float(pd.to_numeric(group["entry_ask"], errors="coerce").mean()) if len(group) else np.nan,
                "avg_hmm_pmax": float(pd.to_numeric(group["hmm_pmax"], errors="coerce").mean()) if "hmm_pmax" in group.columns and len(group) else np.nan,
                "avg_entry_age_seconds": float(pd.to_numeric(group["entry_age_seconds"], errors="coerce").mean()) if len(group) else np.nan,
                "median_entry_age_seconds": float(pd.to_numeric(group["entry_age_seconds"], errors="coerce").median()) if len(group) else np.nan,
                "pnl_by_chronological_slice": json.dumps(slice_pnl, sort_keys=True),
                "roi_by_chronological_slice": json.dumps(slice_roi, sort_keys=True),
                "min_slice_pnl": float(min(slice_pnl.values())) if slice_pnl else np.nan,
                "max_slice_pnl": float(max(slice_pnl.values())) if slice_pnl else np.nan,
                "min_slice_roi": float(np.nanmin(list(slice_roi.values()))) if slice_roi else np.nan,
                "max_slice_roi": float(np.nanmax(list(slice_roi.values()))) if slice_roi else np.nan,
                "passes_support": bool(len(group) >= min_trades and group["market_key"].nunique() >= min_markets),
            }
        )
        rows.append(row)
    return pd.DataFrame(rows)


def frozen_validation(summary_source: pd.DataFrame, min_trades: int, min_markets: int) -> pd.DataFrame:
    configs = [("early", "main,fresh"), ("early,main", "fresh")]
    rows = []
    keys = ["policy_name", "entry_age_window"]
    for train_s, test_s in configs:
        train_names = set(parse_csv(train_s))
        test_names = set(parse_csv(test_s))
        for key_vals, _ in summary_source.groupby(keys, dropna=False):
            if not isinstance(key_vals, tuple):
                key_vals = (key_vals,)
            subset = summary_source
            for col, val in zip(keys, key_vals):
                subset = subset[subset[col].eq(val)]
            train = subset[subset["chronological_slice"].isin(train_names)]
            test = subset[subset["chronological_slice"].isin(test_names)]
            train_m = summarize(train, [], min_trades, min_markets).iloc[0].to_dict() if not train.empty else {}
            test_m = summarize(test, [], min_trades, min_markets).iloc[0].to_dict() if not test.empty else {}
            rows.append(
                {
                    "train_slices": train_s,
                    "test_slices": test_s,
                    "policy_name": key_vals[0],
                    "entry_age_window": key_vals[1],
                    **{f"train_{k}": v for k, v in train_m.items()},
                    **{f"test_{k}": v for k, v in test_m.items()},
                }
            )
    return pd.DataFrame(rows)


def load_models_stakes_thresholds(args: argparse.Namespace) -> tuple[list[str], list[float], list[float]]:
    if args.models:
        models = parse_csv(args.models)
        return models, stress.parse_floats(args.stake_sizes), stress.parse_floats(args.edge_thresholds)
    manifest_path = Path(args.stress_artifact_root) / "run_manifest.json"
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        models = manifest.get("requested_models") or stress.DEFAULT_MODELS
    else:
        models = stress.DEFAULT_MODELS
    return models, stress.parse_floats(args.stake_sizes), stress.parse_floats(args.edge_thresholds)


def write_readme(path: Path, args: argparse.Namespace, manifest: dict[str, Any]) -> None:
    lines = [
        "Market-age policy replay",
        "",
        "Offline research only. No live trading behavior was changed.",
        "",
        "This replay rebuilds candidate rows from compact snapshots and predictions, applies each policy before first-entry selection, and then selects the first eligible row within each explicit entry-age interval.",
        "Lower-bound windows such as 60:300 ignore all candidates before 60 seconds; they do not filter old 0:300 first-entry rows.",
        "",
        f"compact_root={args.compact_root}",
        f"predictions_root={args.predictions_root}",
        f"hmm_context_path={args.hmm_context_path}",
        f"stress_artifact_root={args.stress_artifact_root}",
        f"trade_rows={manifest.get('trade_rows')}",
        f"candidate_rows_selected_before_stakes={manifest.get('candidate_rows_selected_before_stakes')}",
        "",
        "Policy outputs include cumulative 0:300, non-cumulative exact bins, and true lower-bound windows.",
        "Frozen validation is reported for early -> main,fresh and early,main -> fresh without selecting windows on the full sample.",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run(args: argparse.Namespace) -> dict[str, Any]:
    output_dir = Path(args.output_dir)
    if output_dir.exists():
        if not args.overwrite:
            raise FileExistsError(f"{output_dir} exists; pass --overwrite")
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    models, stakes, thresholds = load_models_stakes_thresholds(args)
    windows_spec = parse_windows(args.entry_age_windows)
    windows, ticks = stress.load_compact(args.compact_root, args.start_date, args.end_date, args.max_markets)
    windows = windows[windows["winner_side"].isin(["YES", "NO"])].copy()
    snapshots = stress.prepare_quote_snapshots(ticks, windows, valid_topbook_only=True)
    preds, resolution, missing = stress.load_predictions(args.predictions_root, models, windows)
    if missing:
        detail = resolution.get("error") if isinstance(resolution, dict) else None
        suffix = f"; {detail}" if detail else ""
        raise RuntimeError(f"missing requested prediction models: {missing}{suffix}")
    predicted = stress.attach_predictions(snapshots, preds, models)
    candidate_base = add_edge_columns(predicted)
    hmm_context = args.hmm_context_path if args.hmm_context_path and args.hmm_context_path.exists() else Path(args.hmm_attribution_root) / "trade_level_with_hmm.parquet"
    if args.base_only:
        policies = [{"policy_name": "base_all_models_original_like", "kind": "base", "pmax": None}]
    else:
        state_rows = state_rows_from_context(hmm_context, [FOCUS_HMM_MODEL, BAD_HMM_MODEL])
        candidate_base = attach_hmm_context(candidate_base, state_rows, [FOCUS_HMM_MODEL, BAD_HMM_MODEL])
        policies = policy_specs(stress.parse_floats(args.pmax_thresholds), args.include_bad_state_veto)
    selected = select_first_entries(candidate_base, policies, windows_spec, thresholds)
    trades = simulate_policy(selected, stakes, top_n=3, capacity_aware=True)
    hmm_veto.write_frame(trades, output_dir / "trade_level_policy_results.parquet")

    min_trades = int(args.min_trades_per_policy_window)
    min_markets = int(args.min_unique_markets)
    outputs = {
        "policy_market_age_summary.csv": ["policy_name", "entry_age_window"],
        "policy_market_age_by_chronological_slice.csv": ["policy_name", "entry_age_window", "chronological_slice"],
        "policy_market_age_by_date.csv": ["policy_name", "entry_age_window", "entry_date"],
        "policy_market_age_by_model_id.csv": ["policy_name", "entry_age_window", "model_id"],
        "policy_market_age_by_side.csv": ["policy_name", "entry_age_window", "side"],
        "policy_market_age_by_ask_bin.csv": ["policy_name", "entry_age_window", "ask_bin"],
        "policy_market_age_by_edge_threshold.csv": ["policy_name", "entry_age_window", "edge_threshold"],
        "policy_market_age_by_stake_size.csv": ["policy_name", "entry_age_window", "stake_size"],
    }
    for filename, group_cols in outputs.items():
        summarize(trades, group_cols, min_trades, min_markets).to_csv(output_dir / filename, index=False)
    frozen_validation(trades, min_trades, min_markets).to_csv(output_dir / "policy_market_age_frozen_validation.csv", index=False)
    schema = {
        "trade_level_policy_results": sorted(trades.columns.tolist()),
        "summary_metrics": [
            "policy_name",
            "entry_age_window",
            "trade_count",
            "unique_markets",
            "gross_cost",
            "pnl",
            "roi",
            "win_rate",
            "avg_ask",
            "avg_hmm_pmax",
            "avg_entry_age_seconds",
            "median_entry_age_seconds",
            "pnl_by_chronological_slice",
            "roi_by_chronological_slice",
            "min_slice_pnl",
            "min_slice_roi",
            "max_slice_pnl",
            "max_slice_roi",
            "passes_support",
        ],
    }
    (output_dir / "output_schema.json").write_text(json.dumps(schema, indent=2), encoding="utf-8")
    manifest = {
        "compact_root": str(args.compact_root),
        "predictions_root": str(args.predictions_root),
        "hmm_context_path": str(hmm_context),
        "stress_artifact_root": str(args.stress_artifact_root),
        "models": models,
        "base_only": bool(args.base_only),
        "stakes": stakes,
        "edge_thresholds": thresholds,
        "entry_age_windows": args.entry_age_windows,
        "policies": [p["policy_name"] for p in policies],
        "snapshot_rows": int(len(snapshots)),
        "predicted_rows": int(len(predicted)),
        "candidate_base_rows": int(len(candidate_base)),
        "candidate_rows_selected_before_stakes": int(len(selected)),
        "trade_rows": int(len(trades)),
        "valid_topbook_only": True,
        "first_entry_only_within_policy_window": True,
        "capacity_aware": True,
        "top_n_levels": 3,
        "prediction_resolution": resolution,
    }
    (output_dir / "run_manifest.json").write_text(json.dumps(manifest, indent=2, default=str), encoding="utf-8")
    write_readme(output_dir / "README.txt", args, manifest)
    return manifest


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Offline BTC-5m market-age-aware candidate policy replay.")
    parser.add_argument("--compact-root", type=Path, default=DEFAULT_COMPACT_ROOT)
    parser.add_argument("--predictions-root", type=Path, default=DEFAULT_PREDICTIONS_ROOT)
    parser.add_argument("--hmm-context-path", type=Path, default=DEFAULT_HMM_CONTEXT_PATH)
    parser.add_argument("--hmm-attribution-root", type=Path, default=DEFAULT_HMM_ATTR_ROOT)
    parser.add_argument("--stress-artifact-root", type=Path, default=DEFAULT_STRESS_ROOT)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--models", default="", help="Optional comma-separated model list. Defaults to the stress manifest model set.")
    parser.add_argument("--base-only", action="store_true", help="Emit only base_all_models_original_like rows and skip HMM context attachment.")
    parser.add_argument("--entry-age-windows", default=DEFAULT_WINDOWS)
    parser.add_argument("--stake-sizes", default=",".join(f"{x:g}" for x in stress.DEFAULT_STAKES))
    parser.add_argument("--edge-thresholds", default=",".join(f"{x:g}" for x in stress.DEFAULT_THRESHOLDS))
    parser.add_argument("--pmax-thresholds", default="0.60,0.70,0.75,0.80,0.90")
    parser.add_argument("--include-bad-state-veto", type=bool_arg, default=True)
    parser.add_argument("--min-trades-per-policy-window", type=int, default=500)
    parser.add_argument("--min-unique-markets", type=int, default=50)
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    parser.add_argument("--max-markets", type=int)
    parser.add_argument("--overwrite", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    manifest = run(build_parser().parse_args(argv))
    print(json.dumps(manifest, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
