#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import shutil
import sys
from pathlib import Path
from typing import Any, Optional

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


DEFAULT_OUTPUT_ROOT = Path("artifacts/frozen_hmm_state_attribution")


def parse_slices(value: str) -> list[str]:
    return [part.strip() for part in str(value).split(",") if part.strip()]


def metric_row(frame: pd.DataFrame) -> dict[str, Any]:
    if frame.empty:
        return {
            "trade_count": 0,
            "unique_markets": 0,
            "gross_cost": 0.0,
            "pnl": 0.0,
            "roi": np.nan,
            "win_rate": np.nan,
            "avg_ask": np.nan,
            "avg_edge": np.nan,
            "avg_pmax": np.nan,
        }
    gross_cost = pd.to_numeric(frame.get("gross_cost", frame.get("notional_filled", 0.0)), errors="coerce").fillna(0.0).sum()
    pnl = pd.to_numeric(frame.get("pnl", frame.get("gross_pnl", 0.0)), errors="coerce").fillna(0.0).sum()
    ask = pd.to_numeric(frame.get("ask_price", frame.get("entry_ask")), errors="coerce")
    edge = pd.to_numeric(frame.get("best_edge", frame.get("model_edge")), errors="coerce")
    return {
        "trade_count": int(len(frame)),
        "unique_markets": int(frame["market_id"].nunique() if "market_id" in frame else frame["market_key"].nunique()),
        "gross_cost": float(gross_cost),
        "pnl": float(pnl),
        "roi": float(pnl / gross_cost) if gross_cost else np.nan,
        "win_rate": float(pd.to_numeric(frame.get("win"), errors="coerce").mean()) if "win" in frame else np.nan,
        "avg_ask": float(ask.mean()) if len(ask) else np.nan,
        "avg_edge": float(edge.mean()) if len(edge) else np.nan,
        "avg_pmax": float(pd.to_numeric(frame.get("frozen_hmm_pmax"), errors="coerce").mean()) if "frozen_hmm_pmax" in frame else np.nan,
    }


def base_policy_rows(frame: pd.DataFrame, *, min_edge: float) -> pd.DataFrame:
    out = frame.copy()
    if "model_id" in out:
        out = out[out["model_id"].eq("brownian_zero_drift__rv30")]
    age = pd.to_numeric(out.get("entry_age_seconds", out.get("entry_age_sec")), errors="coerce")
    ask = pd.to_numeric(out.get("ask_price", out.get("entry_ask")), errors="coerce")
    edge = pd.to_numeric(out.get("best_edge", out.get("model_edge")), errors="coerce")
    out = out[age.between(60, 240, inclusive="both") & ask.gt(0.30) & ask.lt(0.47) & edge.ge(float(min_edge))].copy()
    if "policy_name" in out:
        keep = out["policy_name"].isin(["state3_ask_0.30_0.47", "state3_ask_0.30_0.47_simple_models", "state3_ask_0.30_0.47_excl_logistic"])
        if keep.any():
            out = out[keep].copy()
    return out


def group_metrics(frame: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    rows = []
    if frame.empty:
        return pd.DataFrame()
    for keys, group in frame.groupby(cols, dropna=False):
        if not isinstance(keys, tuple):
            keys = (keys,)
        rows.append({**dict(zip(cols, keys)), **metric_row(group)})
    return pd.DataFrame(rows)


def choose_states(train_metrics: pd.DataFrame, *, min_trades: int, min_unique_markets: int) -> list[int]:
    eligible = train_metrics[
        (train_metrics["trade_count"] >= int(min_trades))
        & (train_metrics["unique_markets"] >= int(min_unique_markets))
        & (train_metrics["pnl"] > 0)
        & (train_metrics["roi"] > 0)
    ].copy()
    if eligible.empty:
        return []
    eligible = eligible.sort_values(["roi", "pnl"], ascending=[False, False])
    return [int(eligible.iloc[0]["frozen_hmm_state"])]


def date_concentration(frame: pd.DataFrame) -> float:
    if frame.empty or "entry_date" not in frame.columns:
        return 1.0
    by_date = group_metrics(frame, ["entry_date"])
    positive = by_date[by_date["pnl"] > 0]["pnl"]
    total = positive.sum()
    if total <= 0:
        return 1.0
    return float(positive.max() / total)


def deployable(selected: list[int], holdout_selected: pd.DataFrame, holdout_base: pd.DataFrame, *, min_unique_markets: int, max_date_pnl_share: float) -> tuple[bool, list[str]]:
    reasons: list[str] = []
    if not selected:
        reasons.append("no_selected_states")
    selected_metrics = metric_row(holdout_selected)
    base_metrics = metric_row(holdout_base)
    if selected_metrics["unique_markets"] < min_unique_markets:
        reasons.append("holdout_support_too_low")
    if not (selected_metrics["roi"] > base_metrics["roi"] or selected_metrics["pnl"] > base_metrics["pnl"]):
        reasons.append("holdout_not_better_than_no_hmm")
    if date_concentration(holdout_selected) > max_date_pnl_share:
        reasons.append("holdout_pnl_date_concentrated")
    return (not reasons), reasons


def run(args: argparse.Namespace) -> dict[str, Any]:
    output_dir = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=args.overwrite)
    frame = pd.read_parquet(args.attached_path)
    base = base_policy_rows(frame, min_edge=args.min_edge)
    train_slices = parse_slices(args.train_slices)
    holdout_slices = parse_slices(args.holdout_slices)
    train = base[base["chronological_slice"].isin(train_slices)].copy()
    holdout = base[base["chronological_slice"].isin(holdout_slices)].copy()
    train_metrics = group_metrics(train, ["frozen_hmm_model_id", "frozen_hmm_state"]).sort_values("frozen_hmm_state")
    holdout_metrics = group_metrics(holdout, ["frozen_hmm_model_id", "frozen_hmm_state"]).sort_values("frozen_hmm_state")
    selected = choose_states(train_metrics, min_trades=args.min_trades, min_unique_markets=args.min_unique_markets)
    train_selected = train[train["frozen_hmm_state"].isin(selected)]
    holdout_selected = holdout[holdout["frozen_hmm_state"].isin(selected)]
    no_hmm = {"train": metric_row(train), "holdout": metric_row(holdout)}
    is_deployable, deploy_reasons = deployable(
        selected,
        holdout_selected,
        holdout,
        min_unique_markets=args.min_unique_markets,
        max_date_pnl_share=args.max_date_pnl_share,
    )
    train_metrics.to_csv(output_dir / "state_train_metrics.csv", index=False)
    holdout_metrics.to_csv(output_dir / "state_holdout_metrics.csv", index=False)
    for name, cols in {
        "state_by_date.csv": ["frozen_hmm_state", "entry_date"],
        "state_by_chronological_slice.csv": ["frozen_hmm_state", "chronological_slice"],
        "state_by_side.csv": ["frozen_hmm_state", "side"],
        "state_by_ask_bin.csv": ["frozen_hmm_state", "ask_bin"],
        "state_by_market_age.csv": ["frozen_hmm_state", "entry_age_window"],
    }.items():
        existing = [col for col in cols if col in base.columns]
        if existing:
            group_metrics(base, existing).to_csv(output_dir / name, index=False)
    selected_payload = {
        "selected_states": selected,
        "selection_basis": "train_slices_only",
        "train_slices": train_slices,
        "holdout_slices": holdout_slices,
        "train_selected_metrics": metric_row(train_selected),
        "holdout_selected_metrics": metric_row(holdout_selected),
        "deployable": bool(is_deployable),
        "deploy_reasons": deploy_reasons,
    }
    (output_dir / "selected_states.json").write_text(json.dumps(selected_payload, indent=2, default=str), encoding="utf-8")
    (output_dir / "no_hmm_baseline_metrics.json").write_text(json.dumps(no_hmm, indent=2, default=str), encoding="utf-8")
    (output_dir / "state_selection_report.md").write_text(render_report(selected_payload, no_hmm), encoding="utf-8")
    manifest = {
        "attached_path": str(args.attached_path),
        "output_dir": str(output_dir),
        "min_edge": args.min_edge,
        "min_trades": args.min_trades,
        "min_unique_markets": args.min_unique_markets,
        "selected_states": selected,
        "deployable": bool(is_deployable),
    }
    (output_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, default=str), encoding="utf-8")
    if is_deployable or args.force_deploy_policy:
        deploy_policy = {
            "policy_hmm_model_id": str(base["frozen_hmm_model_id"].dropna().iloc[0]) if not base.empty else None,
            "allowed_states": selected,
            "selection_train_metrics": selected_payload["train_selected_metrics"],
            "holdout_metrics": selected_payload["holdout_selected_metrics"],
            "min_required_hmm_pmax": args.min_required_hmm_pmax,
            "feature_schema_hash": args.feature_schema_hash,
            "model_artifact_hash": args.model_artifact_hash,
            "deployable": bool(is_deployable),
            "forced": bool(args.force_deploy_policy and not is_deployable),
        }
        (output_dir / "deploy_policy.json").write_text(json.dumps(deploy_policy, indent=2, default=str), encoding="utf-8")
        if args.deploy_bundle_dir:
            args.deploy_bundle_dir.mkdir(parents=True, exist_ok=True)
            shutil.copy2(output_dir / "deploy_policy.json", args.deploy_bundle_dir / "deploy_policy.json")
    return manifest


def render_report(selected: dict[str, Any], no_hmm: dict[str, Any]) -> str:
    return "\n".join(
        [
            "# Frozen HMM State Selection",
            "",
            f"selected_states={selected['selected_states']}",
            f"deployable={selected['deployable']}",
            f"deploy_reasons={selected['deploy_reasons']}",
            "",
            "Selection used train slices only. Holdout metrics were computed after freezing the selected state set.",
            "",
            f"no_hmm_train={no_hmm['train']}",
            f"no_hmm_holdout={no_hmm['holdout']}",
            f"selected_train={selected['train_selected_metrics']}",
            f"selected_holdout={selected['holdout_selected_metrics']}",
            "",
        ]
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Evaluate frozen HMM state allow policy with train-only state selection.")
    parser.add_argument("--attached-path", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_ROOT / "frozen_eval_v1")
    parser.add_argument("--train-slices", default="early")
    parser.add_argument("--holdout-slices", default="main,fresh")
    parser.add_argument("--min-edge", type=float, default=0.02)
    parser.add_argument("--min-trades", type=int, default=500)
    parser.add_argument("--min-unique-markets", type=int, default=50)
    parser.add_argument("--max-date-pnl-share", type=float, default=0.60)
    parser.add_argument("--min-required-hmm-pmax", type=float)
    parser.add_argument("--feature-schema-hash")
    parser.add_argument("--model-artifact-hash")
    parser.add_argument("--deploy-bundle-dir", type=Path, help="Optional frozen model bundle directory to receive deploy_policy.json.")
    parser.add_argument("--force-deploy-policy", action="store_true")
    parser.add_argument("--overwrite", action="store_true")
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    print(json.dumps(run(build_parser().parse_args(argv)), sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
