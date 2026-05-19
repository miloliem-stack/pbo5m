#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


MODEL_CANDIDATES = ["model", "model_id", "model_name", "family", "variant", "model_key"]
PROB_CANDIDATES = ["p_up", "probability", "prob", "y_prob", "pred_prob"]
LABEL_CANDIDATES = ["y_true", "result_up", "outcome", "label", "up", "target"]
AGE_CANDIDATES = ["market_age_seconds", "age_seconds", "elapsed_seconds", "market_age"]
FOLD_CANDIDATES = ["fold", "fold_id", "split_id", "test_fold"]
MARKET_CANDIDATES = ["market_key", "market_id", "slug", "market_slug", "market_window_start"]

DEFAULT_WINDOWS = [
    "full_window",
    "pre_120",
    "pre_180",
    "pre_218",
    "pre_240",
    "post_218",
    "post_240",
    "0_60",
    "60_120",
    "120_180",
    "180_218",
    "218_240",
    "240_300",
]


def read_frame(path: Path) -> pd.DataFrame:
    if not path.exists() and path.suffix.lower() == ".csv":
        parquet_fallback = path.with_suffix(".parquet")
        if parquet_fallback.exists():
            print(
                f"CSV predictions file not found, reading same-stem parquet instead: {parquet_fallback}",
                file=sys.stderr,
            )
            path = parquet_fallback
    if path.suffix.lower() == ".parquet":
        try:
            return pd.read_parquet(path)
        except ImportError as exc:
            csv_fallback = path.with_suffix(".csv")
            if csv_fallback.exists():
                print(
                    f"Parquet support is unavailable, reading same-stem CSV instead: {csv_fallback}",
                    file=sys.stderr,
                )
                return pd.read_csv(csv_fallback)
            raise RuntimeError(
                "Cannot read parquet predictions because neither pyarrow nor fastparquet is installed. "
                f"Use a CSV predictions artifact if available, or install parquet support. "
                f"Tried CSV fallback: {csv_fallback}"
            ) from exc
    return pd.read_csv(path)


def detect_column(df: pd.DataFrame, requested: str, candidates: list[str], label: str, required: bool = True) -> str | None:
    if requested != "auto":
        if requested not in df.columns:
            raise ValueError(f"Requested {label} column {requested!r} not found. Available columns: {list(df.columns)}")
        return requested
    matches = [column for column in candidates if column in df.columns]
    if len(matches) == 1:
        return matches[0]
    if len(matches) > 1:
        raise ValueError(f"Ambiguous {label} column. Candidates found: {matches}. Use --{label}-col explicitly. Available columns: {list(df.columns)}")
    if required:
        raise ValueError(f"Could not detect {label} column. Tried {candidates}. Available columns: {list(df.columns)}")
    return None


def window_mask(age: pd.Series, window: str) -> pd.Series:
    if window == "full_window":
        return pd.Series(True, index=age.index)
    if window.startswith("pre_"):
        return age < float(window.split("_")[1])
    if window.startswith("post_"):
        return age >= float(window.split("_")[1])
    lo, hi = window.split("_")
    return (age >= float(lo)) & (age < float(hi))


def brier(y: np.ndarray, p: np.ndarray) -> float:
    return float(np.mean((p - y) ** 2))


def log_loss(y: np.ndarray, p: np.ndarray, eps: float) -> float:
    p = np.clip(p, eps, 1.0 - eps)
    return float(-np.mean(y * np.log(p) + (1.0 - y) * np.log(1.0 - p)))


def ece(y: np.ndarray, p: np.ndarray, bins: int) -> float:
    edges = np.linspace(0.0, 1.0, bins + 1)
    total = len(y)
    if total == 0:
        return float("nan")
    value = 0.0
    for i in range(bins):
        if i == 0:
            mask = (p >= edges[i]) & (p <= edges[i + 1])
        else:
            mask = (p > edges[i]) & (p <= edges[i + 1])
        if not mask.any():
            continue
        value += float(mask.sum() / total * abs(p[mask].mean() - y[mask].mean()))
    return value


def auc_score(y: np.ndarray, p: np.ndarray) -> float | None:
    try:
        from sklearn.metrics import roc_auc_score
    except Exception:
        return None
    if len(np.unique(y)) < 2:
        return None
    return float(roc_auc_score(y, p))


def metric_row(group: pd.DataFrame, *, model: str, window: str, prob_col: str, label_col: str, age_col: str, fold_col: str | None, market_col: str | None, eps: float, ece_bins: int) -> dict[str, Any]:
    usable = group[[prob_col, label_col, age_col] + ([fold_col] if fold_col else []) + ([market_col] if market_col else [])].copy()
    usable[prob_col] = pd.to_numeric(usable[prob_col], errors="coerce")
    usable[label_col] = pd.to_numeric(usable[label_col], errors="coerce")
    usable[age_col] = pd.to_numeric(usable[age_col], errors="coerce")
    usable = usable.dropna(subset=[prob_col, label_col, age_col])
    y = usable[label_col].astype(float).to_numpy()
    p = np.clip(usable[prob_col].astype(float).to_numpy(), eps, 1.0 - eps)
    if len(usable) == 0:
        return {"model": model, "evaluation_window": window, "rows": 0}
    baseline_brier = brier(y, np.full(len(y), 0.5))
    return {
        "model": model,
        "evaluation_window": window,
        "rows": int(len(usable)),
        "markets": int(usable[market_col].nunique()) if market_col else None,
        "folds": int(usable[fold_col].nunique()) if fold_col else None,
        "brier": brier(y, p),
        "brier_improvement_vs_0_50": baseline_brier - brier(y, p),
        "log_loss": log_loss(y, p, eps),
        "accuracy": float(np.mean((p >= 0.5) == y)),
        "auc": auc_score(y, p),
        "ece": ece(y, p, ece_bins),
        "mean_p": float(np.mean(p)),
        "realized_up_rate": float(np.mean(y)),
        "avg_market_age_seconds": float(usable[age_col].mean()),
    }


def compute_metrics(df: pd.DataFrame, *, model_col: str, prob_col: str, label_col: str, age_col: str, fold_col: str | None, market_col: str | None, windows: list[str], eps: float, ece_bins: int) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    ages = pd.to_numeric(df[age_col], errors="coerce")
    for model, model_df in df.groupby(model_col, dropna=False):
        for window in windows:
            subset = model_df.loc[window_mask(ages.loc[model_df.index], window)]
            rows.append(metric_row(subset, model=str(model), window=window, prob_col=prob_col, label_col=label_col, age_col=age_col, fold_col=fold_col, market_col=market_col, eps=eps, ece_bins=ece_bins))
    metrics = pd.DataFrame(rows)
    for window, group_idx in metrics.groupby("evaluation_window").groups.items():
        metrics.loc[group_idx, "rank_by_brier"] = metrics.loc[group_idx, "brier"].rank(method="min", ascending=True)
        metrics.loc[group_idx, "rank_by_log_loss"] = metrics.loc[group_idx, "log_loss"].rank(method="min", ascending=True)
    full = metrics[metrics["evaluation_window"] == "full_window"][["model", "brier", "log_loss", "rank_by_brier"]].rename(
        columns={"brier": "full_window_brier", "log_loss": "full_window_log_loss", "rank_by_brier": "full_window_rank_by_brier"}
    )
    metrics = metrics.merge(full, on="model", how="left")
    metrics["rank_change_vs_full_window_brier"] = metrics["rank_by_brier"] - metrics["full_window_rank_by_brier"]
    metrics["brier_delta_vs_full_window"] = metrics["brier"] - metrics["full_window_brier"]
    metrics["log_loss_delta_vs_full_window"] = metrics["log_loss"] - metrics["full_window_log_loss"]
    return metrics.sort_values(["evaluation_window", "rank_by_brier", "model"]).reset_index(drop=True)


def fold_metrics(df: pd.DataFrame, *, model_col: str, prob_col: str, label_col: str, age_col: str, fold_col: str, market_col: str | None, windows: list[str], eps: float, ece_bins: int) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    ages = pd.to_numeric(df[age_col], errors="coerce")
    for (model, fold), group in df.groupby([model_col, fold_col], dropna=False):
        for window in windows:
            subset = group.loc[window_mask(ages.loc[group.index], window)]
            row = metric_row(subset, model=str(model), window=window, prob_col=prob_col, label_col=label_col, age_col=age_col, fold_col=None, market_col=market_col, eps=eps, ece_bins=ece_bins)
            row["fold"] = fold
            rows.append(row)
    return pd.DataFrame(rows).sort_values(["evaluation_window", "model", "fold"]).reset_index(drop=True)


def reliability(df: pd.DataFrame, *, model_col: str, prob_col: str, label_col: str, age_col: str, windows: list[str], bins: int) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    ages = pd.to_numeric(df[age_col], errors="coerce")
    edges = np.linspace(0.0, 1.0, bins + 1)
    for model, model_df in df.groupby(model_col, dropna=False):
        for window in windows:
            subset = model_df.loc[window_mask(ages.loc[model_df.index], window)].copy()
            subset[prob_col] = pd.to_numeric(subset[prob_col], errors="coerce")
            subset[label_col] = pd.to_numeric(subset[label_col], errors="coerce")
            subset = subset.dropna(subset=[prob_col, label_col])
            p = subset[prob_col].to_numpy(dtype=float)
            y = subset[label_col].to_numpy(dtype=float)
            for i in range(bins):
                low, high = edges[i], edges[i + 1]
                mask = ((p >= low) & (p <= high)) if i == 0 else ((p > low) & (p <= high))
                rows.append(
                    {
                        "model": str(model),
                        "evaluation_window": window,
                        "bin_low": low,
                        "bin_high": high,
                        "rows": int(mask.sum()),
                        "mean_pred": float(p[mask].mean()) if mask.any() else None,
                        "realized_rate": float(y[mask].mean()) if mask.any() else None,
                        "abs_calibration_error": float(abs(p[mask].mean() - y[mask].mean())) if mask.any() else None,
                    }
                )
    return pd.DataFrame(rows)


def rank_comparison(metrics: pd.DataFrame) -> pd.DataFrame:
    brier = metrics.pivot(index="model", columns="evaluation_window", values="brier")
    logloss = metrics.pivot(index="model", columns="evaluation_window", values="log_loss")
    ranks = metrics.pivot(index="model", columns="evaluation_window", values="rank_by_brier")
    out = pd.DataFrame(index=brier.index)
    for window in ["full_window", "pre_120", "pre_180", "pre_218", "pre_240", "post_218", "post_240"]:
        if window in brier:
            out[f"{window}_brier"] = brier[window]
    for window in ["full_window", "pre_180", "pre_218", "pre_240"]:
        if window in ranks:
            out[f"{window}_rank"] = ranks[window]
    if "pre_218" in ranks and "full_window" in ranks:
        out["rank_change_pre_218_vs_full"] = ranks["pre_218"] - ranks["full_window"]
    if "pre_218" in brier and "full_window" in brier:
        out["brier_delta_pre_218_vs_full"] = brier["pre_218"] - brier["full_window"]
    if "pre_218" in logloss and "full_window" in logloss:
        out["log_loss_delta_pre_218_vs_full"] = logloss["pre_218"] - logloss["full_window"]
    return out.reset_index().sort_values("full_window_brier" if "full_window_brier" in out else "model")


def render_readme(metrics: pd.DataFrame, comparison: pd.DataFrame, top_n: int) -> str:
    lines = [
        "Probability prediction window rescore",
        "",
        "This is a rescore of existing predictions only. No models were trained and no probability sweep was run.",
        "Windows are market-age cuts intended to prevent late obvious-state predictions from dominating interpretation.",
        "Binance proxy labels are not final Chainlink/Polymarket settlement truth.",
        "",
    ]
    for window in ["full_window", "pre_180", "pre_218", "pre_240"]:
        lines.append(f"Top {min(top_n, 10)} by Brier for {window}:")
        subset = metrics[metrics["evaluation_window"] == window].sort_values("brier").head(min(top_n, 10))
        if subset.empty:
            lines.append("- none")
        for _, row in subset.iterrows():
            lines.append(f"- {row['model']} brier={row['brier']:.6f} log_loss={row['log_loss']:.6f} rows={int(row['rows'])}")
        lines.append("")
    pre218 = metrics[metrics["evaluation_window"] == "pre_218"].sort_values("brier")
    if not pre218.empty:
        winner = str(pre218.iloc[0]["model"])
        lines.append(f"pre_218 winner: {winner}")
        lines.append(f"calibrated_logistic__gbm_rv30 remains best pre_218: {winner == 'calibrated_logistic__gbm_rv30'}")
    collapsed = comparison[(comparison.get("brier_delta_pre_218_vs_full", 0) > 0.02)] if not comparison.empty else pd.DataFrame()
    if not collapsed.empty:
        lines.append("")
        lines.append("Warning: models whose full-window Brier advantage degrades materially pre_218:")
        for _, row in collapsed.head(20).iterrows():
            lines.append(f"- {row['model']} delta={row['brier_delta_pre_218_vs_full']:.6f}")
    return "\n".join(lines) + "\n"


def run(args: argparse.Namespace) -> dict[str, Any]:
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    df = read_frame(Path(args.predictions))
    model_col = detect_column(df, args.model_col, MODEL_CANDIDATES, "model")
    prob_col = detect_column(df, args.prob_col, PROB_CANDIDATES, "prob")
    label_col = detect_column(df, args.label_col, LABEL_CANDIDATES, "label")
    age_col = detect_column(df, args.age_col, AGE_CANDIDATES, "age")
    fold_col = detect_column(df, args.fold_col, FOLD_CANDIDATES, "fold", required=False)
    market_col = detect_column(df, "auto", MARKET_CANDIDATES, "market", required=False)
    windows = DEFAULT_WINDOWS if args.windows == "default" else [item.strip() for item in args.windows.split(",") if item.strip()]
    metrics = compute_metrics(df, model_col=model_col, prob_col=prob_col, label_col=label_col, age_col=age_col, fold_col=fold_col, market_col=market_col, windows=windows, eps=args.clip_eps, ece_bins=args.ece_bins)
    comparison = rank_comparison(metrics)
    rel = reliability(df, model_col=model_col, prob_col=prob_col, label_col=label_col, age_col=age_col, windows=windows, bins=args.ece_bins)
    metrics.to_csv(output_dir / "probability_metrics_by_window.csv", index=False)
    comparison.to_csv(output_dir / "probability_model_window_rank_comparison.csv", index=False)
    rel.to_csv(output_dir / "probability_window_reliability.csv", index=False)
    if fold_col is not None:
        by_fold = fold_metrics(df, model_col=model_col, prob_col=prob_col, label_col=label_col, age_col=age_col, fold_col=fold_col, market_col=market_col, windows=windows, eps=args.clip_eps, ece_bins=args.ece_bins)
        by_fold.to_csv(output_dir / "probability_metrics_by_window_and_fold.csv", index=False)
    else:
        pd.DataFrame().to_csv(output_dir / "probability_metrics_by_window_and_fold.csv", index=False)
    config = {
        "predictions": str(args.predictions),
        "summary": None if args.summary is None else str(args.summary),
        "output_dir": str(output_dir),
        "columns": {"model": model_col, "probability": prob_col, "label": label_col, "age": age_col, "fold": fold_col, "market": market_col},
        "windows": windows,
        "clip_eps": args.clip_eps,
        "ece_bins": args.ece_bins,
    }
    (output_dir / "probability_window_rescore_config.json").write_text(json.dumps(config, indent=2, default=str), encoding="utf-8")
    (output_dir / "probability_window_scorecard_readme.txt").write_text(render_readme(metrics, comparison, args.top_n), encoding="utf-8")
    return {"rows": int(len(df)), "models": int(df[model_col].nunique()), "output_dir": str(output_dir), "columns": config["columns"]}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Offline rescore of existing BTC 5m probability predictions by market-age windows.")
    parser.add_argument("--predictions", type=Path, required=True)
    parser.add_argument("--summary", type=Path)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--model-col", default="auto")
    parser.add_argument("--prob-col", default="auto")
    parser.add_argument("--label-col", default="auto")
    parser.add_argument("--age-col", default="auto")
    parser.add_argument("--fold-col", default="auto")
    parser.add_argument("--windows", default="default")
    parser.add_argument("--clip-eps", type=float, default=1e-12)
    parser.add_argument("--ece-bins", type=int, default=10)
    parser.add_argument("--top-n", type=int, default=20)
    return parser


def main(argv: list[str] | None = None) -> int:
    diagnostics = run(build_parser().parse_args(argv))
    print(json.dumps(diagnostics, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
