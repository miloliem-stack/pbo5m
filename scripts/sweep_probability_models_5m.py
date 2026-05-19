#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts import sweep_hmm_regime_health as health

DEFAULT_OUTPUT_DIR = Path("artifacts/probability_models_5m/smoke")
DEFAULT_MODEL_FAMILIES = [
    "baseline_50",
    "empirical_moneyness_age",
    "brownian_zero_drift",
    "gbm_zero_drift",
    "gbm_shrunk_drift",
    "gbm_ewma_sigma",
    "gbm_winsorized_sigma",
    "gbm_blended_sigma",
    "calibrated_logistic",
]
DEFAULT_CANDIDATE_REGIMES = [
    "core_1m__gaussian_hmm__k4",
    "laplace_1m__gaussian_hmm__k4",
    "core_1m__gaussian_hmm__k5",
    "laplace_1m__gaussian_hmm__k5",
]
P_BUCKETS = np.linspace(0.0, 1.0, 21)
EPS = 1e-9


def parse_csv(value: str) -> list[str]:
    items = [item.strip() for item in str(value).split(",") if item.strip()]
    if not items:
        raise ValueError("comma-separated list cannot be empty")
    return items


def rows_from_days(days: float | None) -> int | None:
    if days is None:
        return None
    return int(round(days * 24 * 60))


def resolve_rows(days: float | None, rows: int | None, default_rows: int, name: str) -> int:
    if days is not None and rows is not None:
        raise ValueError(f"use either --{name}-days or --{name}-rows, not both")
    value = rows_from_days(days) if days is not None else rows
    if value is None:
        value = default_rows
    if value <= 0:
        raise ValueError(f"{name} rows must be positive")
    return int(value)


@dataclass(frozen=True)
class Fold:
    fold_id: int
    train_start: int
    train_end: int
    test_start: int
    test_end: int


def make_splits(n_rows: int, train_rows: int, test_rows: int, step_rows: int, max_folds: int | None = None) -> list[Fold]:
    folds: list[Fold] = []
    start = 0
    fold_id = 0
    while start + train_rows + test_rows <= n_rows:
        folds.append(Fold(fold_id, start, start + train_rows, start + train_rows, start + train_rows + test_rows))
        fold_id += 1
        if max_folds is not None and fold_id >= max_folds:
            break
        start += step_rows
    return folds


def parse_optional_utc(value: str | None) -> pd.Timestamp | None:
    if value is None:
        return None
    parsed = pd.to_datetime(value, utc=True, errors="coerce")
    if pd.isna(parsed):
        raise ValueError(f"could not parse timestamp: {value!r}")
    return parsed


def filter_folds_by_test_time(features: pd.DataFrame, folds: list[Fold], *, min_test_start: str | None, max_test_end: str | None) -> list[Fold]:
    min_ts = parse_optional_utc(min_test_start)
    max_ts = parse_optional_utc(max_test_end)
    if min_ts is None and max_ts is None:
        return folds
    kept: list[Fold] = []
    for fold in folds:
        test = features.iloc[fold.test_start : fold.test_end]
        if test.empty:
            continue
        test_start = test["timestamp"].iloc[0]
        test_end = test["timestamp"].iloc[-1]
        if min_ts is not None and test_end < min_ts:
            continue
        if max_ts is not None and test_start > max_ts:
            continue
        kept.append(fold)
    return kept


def norm_cdf(x: np.ndarray | float) -> np.ndarray | float:
    return 0.5 * (1.0 + np.vectorize(math.erf)(np.asarray(x) / math.sqrt(2.0)))


def clip_probability(p: np.ndarray | pd.Series | float) -> np.ndarray:
    return np.clip(np.asarray(p, dtype=float), 1e-6, 1.0 - 1e-6)


def brier_score(y: np.ndarray, p: np.ndarray) -> float:
    return float(np.mean((clip_probability(p) - y) ** 2))


def log_loss_score(y: np.ndarray, p: np.ndarray) -> float:
    clipped = clip_probability(p)
    return float(-np.mean(y * np.log(clipped) + (1.0 - y) * np.log(1.0 - clipped)))


def bucket_labels(edges: np.ndarray) -> list[str]:
    return [f"{edges[i]:.2f}-{edges[i + 1]:.2f}" for i in range(len(edges) - 1)]


def assign_market_windows(prices: pd.DataFrame, market_window_seconds: int) -> pd.DataFrame:
    frame = prices[["timestamp", "close"]].sort_values("timestamp").drop_duplicates("timestamp").reset_index(drop=True).copy()
    frame["market_window_start"] = frame["timestamp"].dt.floor(f"{int(market_window_seconds)}s")
    frame["market_window_end"] = frame["market_window_start"] + pd.to_timedelta(market_window_seconds, unit="s")
    grouped = frame.groupby("market_window_start", sort=True)
    windows = grouped.agg(
        K=("close", "first"),
        S_end=("close", "last"),
        first_timestamp=("timestamp", "first"),
        last_timestamp=("timestamp", "last"),
    ).reset_index()
    windows["window_log_return"] = np.log(windows["S_end"] / windows["K"])
    windows["result_up"] = np.where(windows["window_log_return"] > 0, 1.0, np.where(windows["window_log_return"] < 0, 0.0, np.nan))
    windows["result_down"] = np.where(windows["window_log_return"] < 0, 1.0, np.where(windows["window_log_return"] > 0, 0.0, np.nan))
    windows["result_tie"] = np.where(windows["window_log_return"] == 0, 1.0, 0.0)
    out = frame.merge(windows, on="market_window_start", how="left")
    out["market_age_seconds"] = (out["timestamp"] - out["market_window_start"]).dt.total_seconds()
    out["seconds_to_market_end"] = (out["market_window_end"] - out["timestamp"]).dt.total_seconds()
    out["S_t"] = out["close"]
    out["log_moneyness"] = np.log(out["S_t"] / out["K"])
    out["tau_minutes"] = (out["seconds_to_market_end"].clip(lower=0.0) / 60.0).astype(float)
    out["tau_years"] = out["tau_minutes"] / (365.0 * 24.0 * 60.0)
    return out


def _rolling_sign_flip_rate(log_returns: pd.Series, window: int) -> pd.Series:
    signs = np.sign(log_returns.fillna(0.0))
    previous = signs.shift(1)
    valid = ((signs != 0) & (previous != 0)).astype(float)
    flips = ((signs != previous) & (valid == 1.0)).astype(float)
    valid_count = valid.rolling(window=window, min_periods=window).sum()
    return flips.rolling(window=window, min_periods=window).sum() / valid_count.replace(0.0, np.nan)


def _ewm_alpha(half_life: float) -> float:
    return float(1.0 - math.exp(math.log(0.5) / half_life))


def build_probability_features(prices: pd.DataFrame, market_window_seconds: int) -> tuple[pd.DataFrame, dict[str, Any]]:
    frame = assign_market_windows(prices, market_window_seconds)
    frame["log_close"] = np.log(frame["close"])
    frame["log_return_1m"] = frame["log_close"].diff()
    for lag in (1, 2, 3, 5):
        frame[f"r_{lag}m"] = frame["log_close"] - frame["log_close"].shift(lag)
    abs_ret = frame["log_return_1m"].abs()
    for window in (5, 10, 15, 30, 60, 180):
        frame[f"rv_{window}m"] = frame["log_return_1m"].rolling(window=window, min_periods=window).std(ddof=0)
        frame[f"absret_{window}m"] = abs_ret.rolling(window=window, min_periods=window).mean()
    for half_life in (3, 10, 30, 60):
        alpha = _ewm_alpha(half_life)
        frame[f"ew_mean_{half_life}m"] = frame["log_return_1m"].ewm(alpha=alpha, adjust=False).mean()
        frame[f"ew_var_{half_life}m"] = (frame["log_return_1m"] ** 2).ewm(alpha=alpha, adjust=False).mean()
        frame[f"ew_sigma_{half_life}m"] = np.sqrt(frame[f"ew_var_{half_life}m"].clip(lower=0.0))
    for window in (5, 15, 30):
        frame[f"sign_flip_rate_{window}m"] = _rolling_sign_flip_rate(frame["log_return_1m"], window)
        frame[f"drift_to_vol_{window}m"] = frame[f"r_{min(window, 5)}m"] / frame[f"rv_{window}m"].clip(lower=1e-8)
    frame["shock_score"] = abs_ret / frame["rv_30m"].clip(lower=1e-8)
    frame["market_age_bucket"] = pd.cut(
        frame["market_age_seconds"],
        bins=[-1, 60, 120, 180, 240, 301],
        labels=["0-60s", "60-120s", "120-180s", "180-240s", "240-300s"],
    ).astype(str)
    frame["seconds_to_end_bucket"] = pd.cut(
        frame["seconds_to_market_end"],
        bins=[-1, 60, 120, 180, 240, 301],
        labels=["0-60s", "60-120s", "120-180s", "180-240s", "240-300s"],
    ).astype(str)
    frame["abs_log_moneyness"] = frame["log_moneyness"].abs()
    frame["signed_moneyness_bucket"] = pd.cut(
        frame["log_moneyness"],
        bins=[-np.inf, -0.002, -0.0005, 0.0005, 0.002, np.inf],
        labels=["deep_down", "slightly_down", "near_flat", "slightly_up", "deep_up"],
    ).astype(str)
    frame["volatility_bucket"] = pd.qcut(frame["rv_30m"].rank(method="first"), q=5, labels=["vol_q1", "vol_q2", "vol_q3", "vol_q4", "vol_q5"])
    frame["shock_score_bucket"] = pd.cut(
        frame["shock_score"],
        bins=[-np.inf, 0.5, 1.0, 2.0, 3.0, np.inf],
        labels=["shock_lt_0_5", "shock_0_5_1", "shock_1_2", "shock_2_3", "shock_gte_3"],
    ).astype(str)
    required = ["result_up", "log_moneyness", "tau_minutes", "rv_30m", "rv_60m", "ew_sigma_10m", "ew_sigma_30m"]
    before = len(frame)
    frame = frame[np.isfinite(frame[required].astype(float).to_numpy()).all(axis=1)].reset_index(drop=True)
    manifest = {
        "input_rows": int(before),
        "rows": int(len(frame)),
        "rows_dropped_for_nonfinite_core_features": int(before - len(frame)),
        "feature_causality": "all rolling/EW features use trailing windows or adjust=False EW summaries",
    }
    return frame, manifest


def brownian_probability(log_moneyness: np.ndarray, sigma: np.ndarray, tau_minutes: np.ndarray, *, sigma_floor: float = 1e-5, sigma_cap: float = 0.05) -> np.ndarray:
    sigma_eff = np.clip(np.asarray(sigma, dtype=float), sigma_floor, sigma_cap)
    tau = np.maximum(np.asarray(tau_minutes, dtype=float), 1e-9)
    z = np.asarray(log_moneyness, dtype=float) / (sigma_eff * np.sqrt(tau))
    return clip_probability(norm_cdf(z))


def gbm_probability(
    log_moneyness: np.ndarray,
    sigma: np.ndarray,
    tau_minutes: np.ndarray,
    *,
    mu_per_minute: np.ndarray | float = 0.0,
    include_ito: bool = True,
    sigma_floor: float = 1e-5,
    sigma_cap: float = 0.05,
) -> np.ndarray:
    sigma_eff = np.clip(np.asarray(sigma, dtype=float), sigma_floor, sigma_cap)
    tau = np.maximum(np.asarray(tau_minutes, dtype=float), 1e-9)
    mu = np.asarray(mu_per_minute, dtype=float)
    drift_term = (mu - (0.5 * sigma_eff**2 if include_ito else 0.0)) * tau
    z = (np.asarray(log_moneyness, dtype=float) + drift_term) / (sigma_eff * np.sqrt(tau))
    return clip_probability(norm_cdf(z))


def winsorized_sigma(returns: pd.Series, window: int, z: float) -> pd.Series:
    rolling_std = returns.rolling(window=window, min_periods=window).std(ddof=0)
    clipped = returns.clip(lower=-z * rolling_std, upper=z * rolling_std)
    return clipped.rolling(window=window, min_periods=window).std(ddof=0)


def empirical_bucket_predict(train: pd.DataFrame, test: pd.DataFrame, smoothing: float = 50.0) -> np.ndarray:
    train = train.copy()
    test = test.copy()
    train["moneyness_bucket"] = pd.cut(
        train["log_moneyness"],
        bins=[-np.inf, -0.002, -0.0005, 0.0005, 0.002, np.inf],
        labels=["deep_down", "slightly_down", "near_flat", "slightly_up", "deep_up"],
    ).astype(str)
    test["moneyness_bucket"] = pd.cut(
        test["log_moneyness"],
        bins=[-np.inf, -0.002, -0.0005, 0.0005, 0.002, np.inf],
        labels=["deep_down", "slightly_down", "near_flat", "slightly_up", "deep_up"],
    ).astype(str)
    global_p = float(train["result_up"].mean())
    grouped = train.groupby(["market_age_bucket", "moneyness_bucket"], dropna=False)["result_up"].agg(["sum", "count"])
    probs = ((grouped["sum"] + smoothing * 0.5) / (grouped["count"] + smoothing)).to_dict()
    fallback = (global_p + 0.5) / 2.0 if np.isfinite(global_p) else 0.5
    return clip_probability([probs.get((row.market_age_bucket, row.moneyness_bucket), fallback) for row in test.itertuples()])


def logistic_calibration_predict(train: pd.DataFrame, test: pd.DataFrame, base_train_p: np.ndarray, base_test_p: np.ndarray) -> tuple[np.ndarray, str]:
    feature_cols = ["log_moneyness", "market_age_seconds", "seconds_to_market_end", "r_1m", "r_2m", "r_3m", "r_5m", "rv_15m", "rv_30m", "sign_flip_rate_15m", "shock_score"]
    try:
        from sklearn.impute import SimpleImputer
        from sklearn.linear_model import LogisticRegression
        from sklearn.pipeline import make_pipeline
        from sklearn.preprocessing import StandardScaler
    except Exception:
        return bucketed_calibration_predict(train["result_up"].to_numpy(), base_train_p, base_test_p), "bucketed_fallback_no_sklearn"
    x_train = train[feature_cols].copy()
    x_test = test[feature_cols].copy()
    x_train["base_logit"] = np.log(clip_probability(base_train_p) / (1.0 - clip_probability(base_train_p)))
    x_test["base_logit"] = np.log(clip_probability(base_test_p) / (1.0 - clip_probability(base_test_p)))
    model = make_pipeline(SimpleImputer(strategy="median"), StandardScaler(), LogisticRegression(max_iter=200, random_state=0))
    model.fit(x_train, train["result_up"].astype(int))
    return clip_probability(model.predict_proba(x_test)[:, 1]), "sklearn_logistic"


def bucketed_calibration_predict(y_train: np.ndarray, p_train: np.ndarray, p_test: np.ndarray) -> np.ndarray:
    buckets = pd.cut(clip_probability(p_train), bins=P_BUCKETS, labels=bucket_labels(P_BUCKETS), include_lowest=True)
    train = pd.DataFrame({"bucket": buckets, "y": y_train})
    rates = train.groupby("bucket", observed=False)["y"].agg(["mean", "count"])
    global_p = float(np.mean(y_train))
    smoothed = ((rates["mean"] * rates["count"]).fillna(0.0) + 50.0 * 0.5) / (rates["count"].fillna(0.0) + 50.0)
    test_buckets = pd.cut(clip_probability(p_test), bins=P_BUCKETS, labels=bucket_labels(P_BUCKETS), include_lowest=True)
    return clip_probability([float(smoothed.get(bucket, global_p)) for bucket in test_buckets])


def model_predictions_for_fold(train: pd.DataFrame, test: pd.DataFrame, families: list[str]) -> list[tuple[str, str, dict[str, Any], np.ndarray, dict[str, Any]]]:
    rows: list[tuple[str, str, dict[str, Any], np.ndarray, dict[str, Any]]] = []
    if "baseline_50" in families:
        rows.append(("baseline_50", "baseline_50", {}, np.full(len(test), 0.5), {}))
    if "empirical_moneyness_age" in families:
        rows.append(("empirical_moneyness_age", "empirical_moneyness_age", {"smoothing": 50.0}, empirical_bucket_predict(train, test), {}))
    if "brownian_zero_drift" in families:
        rows.append(("brownian_zero_drift__rv30", "brownian_zero_drift", {"sigma": "rv_30m"}, brownian_probability(test["log_moneyness"], test["rv_30m"], test["tau_minutes"]), {}))
    if "gbm_zero_drift" in families:
        rows.append(("gbm_zero_drift__rv30_ito", "gbm_zero_drift", {"sigma": "rv_30m", "ito": True}, gbm_probability(test["log_moneyness"], test["rv_30m"], test["tau_minutes"], include_ito=True), {}))
        rows.append(("gbm_zero_drift__rv30_no_ito", "gbm_zero_drift", {"sigma": "rv_30m", "ito": False}, gbm_probability(test["log_moneyness"], test["rv_30m"], test["tau_minutes"], include_ito=False), {}))
    if "gbm_shrunk_drift" in families:
        for source in ("ew_mean_10m", "ew_mean_30m"):
            for weight in (0.05, 0.25):
                p = gbm_probability(test["log_moneyness"], test["rv_30m"], test["tau_minutes"], mu_per_minute=weight * test[source].fillna(0.0).to_numpy())
                rows.append((f"gbm_shrunk_drift__{source}__w{weight:g}", "gbm_shrunk_drift", {"mu": source, "shrinkage_weight": weight}, p, {}))
    if "gbm_ewma_sigma" in families:
        for half_life in (10, 30, 60):
            rows.append((f"gbm_ewma_sigma__hl{half_life}", "gbm_ewma_sigma", {"sigma": f"ew_sigma_{half_life}m"}, gbm_probability(test["log_moneyness"], test[f"ew_sigma_{half_life}m"], test["tau_minutes"]), {}))
    if "gbm_winsorized_sigma" in families:
        for window, z in ((30, 2.5), (60, 3.0)):
            sigma = winsorized_sigma(pd.concat([train["log_return_1m"], test["log_return_1m"]], ignore_index=True), window, z).iloc[len(train) :].reset_index(drop=True)
            rows.append((f"gbm_winsorized_sigma__w{window}__z{z:g}", "gbm_winsorized_sigma", {"window": window, "winsor_z": z}, gbm_probability(test["log_moneyness"], sigma.fillna(test["rv_30m"]), test["tau_minutes"]), {}))
    if "gbm_blended_sigma" in families:
        blends = [("50_30_20", (0.50, 0.30, 0.20)), ("33_34_33", (0.33, 0.34, 0.33)), ("20_50_30", (0.20, 0.50, 0.30))]
        for name, weights in blends:
            sigma = weights[0] * test["rv_10m"] + weights[1] * test["rv_30m"] + weights[2] * test["rv_60m"]
            rows.append((f"gbm_blended_sigma__{name}", "gbm_blended_sigma", {"weights": weights, "sigmas": ["rv_10m", "rv_30m", "rv_60m"]}, gbm_probability(test["log_moneyness"], sigma, test["tau_minutes"]), {}))
    if "calibrated_logistic" in families:
        base_train = gbm_probability(train["log_moneyness"], train["rv_30m"], train["tau_minutes"], include_ito=False)
        base_test = gbm_probability(test["log_moneyness"], test["rv_30m"], test["tau_minutes"], include_ito=False)
        p, method = logistic_calibration_predict(train, test, base_train, base_test)
        rows.append(("calibrated_logistic__gbm_rv30", "calibrated_logistic", {"base": "gbm_zero_drift__rv30_no_ito", "method": method}, p, {"calibration_backend": method}))
    return rows


def reliability_table(predictions: pd.DataFrame) -> pd.DataFrame:
    if predictions.empty:
        return pd.DataFrame()
    frame = predictions.copy()
    frame["p_bucket"] = pd.cut(frame["p_up"], bins=P_BUCKETS, labels=bucket_labels(P_BUCKETS), include_lowest=True).astype(str)
    out = (
        frame.groupby(["model_id", "p_bucket"], observed=False)
        .agg(n=("result_up", "size"), mean_predicted_p=("p_up", "mean"), empirical_up_rate=("result_up", "mean"))
        .reset_index()
    )
    out["calibration_error"] = (out["mean_predicted_p"] - out["empirical_up_rate"]).abs()
    return out


def expected_calibration_error(y: np.ndarray, p: np.ndarray) -> float:
    table = reliability_table(pd.DataFrame({"model_id": "m", "result_up": y, "p_up": p}))
    if table.empty:
        return float("nan")
    total = table["n"].sum()
    return float((table["n"] / total * table["calibration_error"].fillna(0.0)).sum())


def auc_score(y: np.ndarray, p: np.ndarray) -> float | None:
    try:
        from sklearn.metrics import roc_auc_score
    except Exception:
        return None
    if len(np.unique(y)) < 2:
        return None
    return float(roc_auc_score(y, p))


def metric_row(model_id: str, family: str, params: dict[str, Any], frame: pd.DataFrame, baseline: dict[str, float] | None = None) -> dict[str, Any]:
    valid = frame.dropna(subset=["result_up", "p_up"])
    y = valid["result_up"].astype(float).to_numpy()
    p = valid["p_up"].astype(float).to_numpy()
    brier = brier_score(y, p)
    logloss = log_loss_score(y, p)
    row = {
        "model_id": model_id,
        "family": family,
        "parameters": json.dumps(params, sort_keys=True, default=str),
        "n": int(len(valid)),
        "brier": brier,
        "log_loss": logloss,
        "accuracy": float(np.mean((p >= 0.5) == y)) if len(valid) else None,
        "mean_predicted_p": float(np.mean(p)) if len(valid) else None,
        "empirical_up_rate": float(np.mean(y)) if len(valid) else None,
        "ece": expected_calibration_error(y, p),
        "auc": auc_score(y, p),
    }
    if baseline:
        row["brier_improvement_vs_50"] = baseline["brier"] - brier
        row["logloss_improvement_vs_50"] = baseline["log_loss"] - logloss
    else:
        row["brier_improvement_vs_50"] = 0.0
        row["logloss_improvement_vs_50"] = 0.0
    return row


def warning_flags(row: pd.Series, early_improvement: float | None, baseline_beat: bool, min_sample: int = 500) -> str:
    flags = ["PROXY_LABEL_WARNING"]
    if int(row.get("n", 0) or 0) < min_sample:
        flags.append("LOW_SAMPLE")
    if float(row.get("ece", 0.0) or 0.0) > 0.05:
        flags.append("MISCALIBRATED")
    if not baseline_beat:
        flags.append("NO_BASELINE_BEAT")
    if (row.get("brier_improvement_vs_50") or 0.0) > 0 and (early_improvement is None or early_improvement <= 0):
        flags.append("LATE_MARKET_ONLY")
    return ",".join(flags)


def aggregate_summary(fold_metrics: pd.DataFrame, metrics_by_age: pd.DataFrame) -> pd.DataFrame:
    if fold_metrics.empty:
        return pd.DataFrame()
    grouped = fold_metrics.groupby(["model_id", "family", "parameters"], dropna=False)
    summary = grouped.agg(
        n=("n", "sum"),
        brier=("brier", "mean"),
        log_loss=("log_loss", "mean"),
        accuracy=("accuracy", "mean"),
        mean_predicted_p=("mean_predicted_p", "mean"),
        empirical_up_rate=("empirical_up_rate", "mean"),
        ece=("ece", "mean"),
        auc=("auc", "mean"),
        brier_improvement_vs_50=("brier_improvement_vs_50", "mean"),
        logloss_improvement_vs_50=("logloss_improvement_vs_50", "mean"),
        fold_brier_std=("brier", "std"),
        fold_log_loss_std=("log_loss", "std"),
        fold_count=("fold_id", "nunique"),
    ).reset_index()
    baseline = summary[summary["model_id"] == "baseline_50"]
    empirical = summary[summary["model_id"] == "empirical_moneyness_age"]
    empirical_brier = float(empirical["brier"].iloc[0]) if not empirical.empty else None
    early = metrics_by_age[metrics_by_age["market_age_bucket"].isin(["0-60s", "60-120s"])]
    early_imp = early.groupby("model_id")["brier_improvement_vs_50"].mean().to_dict() if not early.empty else {}
    warnings = []
    for _, row in summary.iterrows():
        beat_50 = bool((row["brier_improvement_vs_50"] or 0.0) > 0)
        beat_emp = True if empirical_brier is None or row["model_id"] == "empirical_moneyness_age" else bool(row["brier"] < empirical_brier)
        warnings.append(warning_flags(row, early_imp.get(row["model_id"]), beat_50 and beat_emp))
    summary["warnings"] = warnings
    return summary.sort_values(["brier", "log_loss"]).reset_index(drop=True)


def append_warning(existing: str, warning: str) -> str:
    parts = [part for part in str(existing).split(",") if part]
    if warning not in parts:
        parts.append(warning)
    return ",".join(parts)


def add_reliability_guardrails(summary: pd.DataFrame, reliability: pd.DataFrame, min_extreme_n: int = 100) -> pd.DataFrame:
    if summary.empty or reliability.empty:
        return summary
    out = summary.copy()
    extreme = reliability[reliability["p_bucket"].isin(["0.00-0.05", "0.05-0.10", "0.90-0.95", "0.95-1.00"])]
    overconfident = set(extreme[(extreme["n"] >= min_extreme_n) & (extreme["calibration_error"] > 0.08)]["model_id"])
    for idx, row in out.iterrows():
        warnings = str(row.get("warnings", ""))
        if row["model_id"] in overconfident:
            warnings = append_warning(warnings, "OVERCONFIDENT")
        if row["family"] == "gbm_shrunk_drift" and float(row.get("brier_improvement_vs_50", 0.0) or 0.0) <= 0.0:
            warnings = append_warning(warnings, "DRIFT_OVERFIT")
        out.at[idx, "warnings"] = warnings
    return out


def metrics_by_group(predictions: pd.DataFrame, group_cols: list[str], baseline_by_group: pd.DataFrame | None = None) -> pd.DataFrame:
    if predictions.empty:
        return pd.DataFrame()
    rows = []
    for keys, group in predictions.groupby(["model_id", "family", "parameters"] + group_cols, dropna=False):
        model_id, family, params, *rest = keys if isinstance(keys, tuple) else (keys,)
        row = metric_row(model_id, family, json.loads(params), group)
        for col, value in zip(group_cols, rest, strict=False):
            row[col] = value
        rows.append(row)
    out = pd.DataFrame(rows)
    if baseline_by_group is not None and not out.empty:
        base_cols = group_cols + ["brier", "log_loss"]
        base = baseline_by_group[baseline_by_group["model_id"] == "baseline_50"][base_cols].rename(columns={"brier": "baseline_brier", "log_loss": "baseline_log_loss"})
        out = out.merge(base, on=group_cols, how="left")
        out["brier_improvement_vs_50"] = out["baseline_brier"] - out["brier"]
        out["logloss_improvement_vs_50"] = out["baseline_log_loss"] - out["log_loss"]
    return out


def prepare_prediction_buckets(predictions: pd.DataFrame) -> pd.DataFrame:
    frame = predictions.copy()
    frame["p_bucket"] = pd.cut(frame["p_up"], bins=P_BUCKETS, labels=bucket_labels(P_BUCKETS), include_lowest=True).astype(str)
    frame["edge_bucket"] = pd.cut(
        (frame["p_up"] - 0.5).abs(),
        bins=[-0.001, 0.02, 0.05, 0.10, 0.20, 0.50],
        labels=["edge_0_2pct", "edge_2_5pct", "edge_5_10pct", "edge_10_20pct", "edge_gt_20pct"],
    ).astype(str)
    frame["abs_moneyness_bucket"] = pd.cut(
        frame["abs_log_moneyness"],
        bins=[-np.inf, 0.00025, 0.0005, 0.001, 0.002, np.inf],
        labels=["abs_mny_lt_2_5bp", "abs_mny_2_5_5bp", "abs_mny_5_10bp", "abs_mny_10_20bp", "abs_mny_gte_20bp"],
    ).astype(str)
    z = frame["log_moneyness"] / (frame["rv_30m"].clip(lower=1e-5) * np.sqrt(frame["tau_minutes"].clip(lower=1e-9)))
    frame["z_moneyness_bucket"] = pd.cut(
        z,
        bins=[-np.inf, -2, -1, -0.25, 0.25, 1, 2, np.inf],
        labels=["z_lt_-2", "z_-2_-1", "z_-1_-0_25", "z_near_0", "z_0_25_1", "z_1_2", "z_gt_2"],
    ).astype(str)
    return frame


def load_optional_hmm_regimes(path: Path | None, candidates: list[str]) -> pd.DataFrame | None:
    if path is None:
        return None
    parquet_path = path / "per_timestamp_regime_utility_sample.parquet"
    csv_path = path / "per_timestamp_regime_utility_sample.csv"
    if parquet_path.exists():
        frame = pd.read_parquet(parquet_path)
    elif csv_path.exists():
        frame = pd.read_csv(csv_path, parse_dates=["timestamp"])
    else:
        return None
    keep = [
        "timestamp",
        "candidate_model_id",
        "raw_state_id",
        "canonical_state_label",
        "p_max",
        "regime_age_minutes",
    ]
    frame = frame[[column for column in keep if column in frame.columns]].copy()
    frame = frame[frame["candidate_model_id"].isin(candidates)]
    return frame


def render_summary(summary: pd.DataFrame, by_age: pd.DataFrame, diagnostics: dict[str, Any]) -> str:
    lines = [
        "BTC 5m probability model sweep",
        "",
        "Binance 5-minute proxy labels are not final Chainlink/Polymarket settlement truth.",
        "Late-market predictions can look strong because current price is already above/below the opening reference; review market-age tables.",
        "",
    ]
    if summary.empty:
        return "\n".join(lines + ["No model rows produced."])
    for title, sort_col in [("Top models by Brier", "brier"), ("Top models by log loss", "log_loss"), ("Top models by calibration", "ece")]:
        lines.append(f"{title}:")
        for _, row in summary.sort_values(sort_col).head(8).iterrows():
            lines.append(f"- {row['model_id']} {sort_col}={row[sort_col]:.6f} brier_imp={row['brier_improvement_vs_50']:.6f} warnings={row['warnings']}")
        lines.append("")
    early = by_age[by_age["market_age_bucket"].isin(["0-60s", "60-120s"])] if not by_age.empty else pd.DataFrame()
    lines.append("Top early-market performance:")
    if early.empty:
        lines.append("- none")
    else:
        early_rank = early.groupby("model_id")["brier_improvement_vs_50"].mean().sort_values(ascending=False).head(8)
        for model_id, value in early_rank.items():
            lines.append(f"- {model_id} early_brier_imp={value:.6f}")
    drift = summary[summary["family"] == "gbm_shrunk_drift"]
    lines.extend(["", "Drift variants:"])
    lines.append("- not evaluated" if drift.empty else f"- best drift brier={drift['brier'].min():.6f}; compare against zero-drift rows in probability_model_summary.csv")
    lines.extend(
        [
            "",
            "Sigma variants:",
            "- compare gbm_ewma_sigma, gbm_winsorized_sigma, and gbm_blended_sigma rows against gbm_zero_drift in the summary.",
            "",
            f"sklearn_calibration_available={diagnostics.get('sklearn_calibration_available')}",
            f"hmm_regime_joined={diagnostics.get('hmm_regime_joined')}",
        ]
    )
    return "\n".join(lines) + "\n"


def run_sweep(args: argparse.Namespace) -> dict[str, Any]:
    started = time.perf_counter()
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    families = parse_csv(args.model_families)
    candidate_regimes = parse_csv(args.candidate_regime_models)
    train_rows = resolve_rows(args.train_days, args.train_rows, 240, "train")
    test_rows = resolve_rows(args.test_days, args.test_rows, 120, "test")
    step_rows = resolve_rows(args.step_days, args.step_rows, test_rows, "step")
    max_folds = None if args.max_folds == 0 else args.max_folds
    min_test_start = getattr(args, "min_test_start", None)
    max_test_end = getattr(args, "max_test_end", None)
    config = {
        "input": str(args.input),
        "output_dir": str(output_dir),
        "market_window_seconds": args.market_window_seconds,
        "market_metadata": None if args.market_metadata is None else str(args.market_metadata),
        "hmm_regime_utility_dir": None if args.hmm_regime_utility_dir is None else str(args.hmm_regime_utility_dir),
        "candidate_regime_models": candidate_regimes,
        "model_families": families,
        "train_rows": train_rows,
        "test_rows": test_rows,
        "step_rows": step_rows,
        "max_rows": args.max_rows,
        "max_folds": max_folds,
        "min_test_start": min_test_start,
        "max_test_end": max_test_end,
        "random_seed": args.random_seed,
        "calibration_methods": parse_csv(args.calibration_methods),
        "price_column": args.price_column,
        "proxy_label_warning": "Binance UTC 5-minute proxy labels are not final Chainlink/Polymarket truth.",
    }
    (output_dir / "probability_sweep_config.json").write_text(json.dumps(config, indent=2, default=str), encoding="utf-8")
    prices = health.load_price_frame(Path(args.input), max_rows=args.max_rows)
    features, feature_manifest = build_probability_features(prices, args.market_window_seconds)
    hmm = load_optional_hmm_regimes(args.hmm_regime_utility_dir, candidate_regimes)
    hmm_joined = hmm is not None and not hmm.empty
    if hmm_joined:
        features = features.merge(hmm, on="timestamp", how="left")
    all_folds = make_splits(len(features), train_rows, test_rows, step_rows, max_folds=max_folds)
    folds = filter_folds_by_test_time(features, all_folds, min_test_start=min_test_start, max_test_end=max_test_end)
    prediction_frames: list[pd.DataFrame] = []
    fold_rows: list[dict[str, Any]] = []
    for fold in folds:
        train = features.iloc[fold.train_start : fold.train_end].reset_index(drop=True)
        test = features.iloc[fold.test_start : fold.test_end].reset_index(drop=True)
        variants = model_predictions_for_fold(train, test, families)
        base_frame = None
        for model_id, family, params, p_up, extra in variants:
            pred = test[
                [
                    "timestamp",
                    "market_window_start",
                    "market_window_end",
                    "market_age_seconds",
                    "seconds_to_market_end",
                    "S_t",
                    "K",
                    "S_end",
                    "result_up",
                    "log_moneyness",
                    "abs_log_moneyness",
                    "tau_minutes",
                    "rv_30m",
                    "shock_score",
                    "market_age_bucket",
                    "seconds_to_end_bucket",
                    "signed_moneyness_bucket",
                    "volatility_bucket",
                    "shock_score_bucket",
                ]
            ].copy()
            for optional in ("candidate_model_id", "raw_state_id", "canonical_state_label", "p_max", "regime_age_minutes"):
                if optional in test.columns:
                    pred[optional] = test[optional]
            pred["model_id"] = model_id
            pred["family"] = family
            pred["parameters"] = json.dumps(params, sort_keys=True, default=str)
            pred["p_up"] = clip_probability(p_up)
            pred["fold_id"] = fold.fold_id
            if model_id == "baseline_50":
                base_frame = pred
            prediction_frames.append(pred)
        if base_frame is None:
            base_frame = test.assign(model_id="baseline_50", family="baseline_50", parameters="{}", p_up=0.5, fold_id=fold.fold_id)
        baseline_metrics = metric_row("baseline_50", "baseline_50", {}, base_frame)
        for model_id, family, params, p_up, _ in variants:
            tmp = test[["result_up"]].copy()
            tmp["p_up"] = p_up
            row = metric_row(model_id, family, params, tmp, baseline_metrics)
            row["fold_id"] = fold.fold_id
            fold_rows.append(row)
    predictions = pd.concat(prediction_frames, ignore_index=True) if prediction_frames else pd.DataFrame()
    predictions = prepare_prediction_buckets(predictions) if not predictions.empty else predictions
    fold_metrics = pd.DataFrame(fold_rows)
    by_age = metrics_by_group(predictions, ["market_age_bucket"])
    by_age = metrics_by_group(predictions, ["market_age_bucket"], baseline_by_group=by_age)
    summary = aggregate_summary(fold_metrics, by_age)
    reliability = reliability_table(predictions)
    by_moneyness = metrics_by_group(predictions, ["abs_moneyness_bucket", "signed_moneyness_bucket", "z_moneyness_bucket"])
    by_edge = metrics_by_group(predictions, ["edge_bucket"])
    by_vol = metrics_by_group(predictions, ["volatility_bucket", "shock_score_bucket"])
    by_hmm = metrics_by_group(predictions.dropna(subset=["candidate_model_id"]), ["candidate_model_id", "raw_state_id", "canonical_state_label"]) if hmm_joined else pd.DataFrame()
    summary = add_reliability_guardrails(summary, reliability)

    outputs = {
        "probability_model_summary.csv": summary,
        "fold_metrics.csv": fold_metrics,
        "reliability_by_model.csv": reliability,
        "metrics_by_market_age.csv": by_age,
        "metrics_by_moneyness.csv": by_moneyness,
        "metrics_by_edge_bucket.csv": by_edge,
        "metrics_by_volatility_bucket.csv": by_vol,
    }
    if hmm_joined:
        outputs["metrics_by_hmm_regime.csv"] = by_hmm
    for name, frame in outputs.items():
        frame.to_csv(output_dir / name, index=False)
    sample = predictions.head(args.prediction_sample_rows) if args.prediction_sample_rows and args.prediction_sample_rows > 0 else predictions
    if not health.write_optional_parquet(sample, output_dir / "probability_predictions_sample.parquet"):
        sample.to_csv(output_dir / "probability_predictions_sample.csv", index=False)
    try:
        import sklearn  # noqa: F401
        sklearn_available = True
    except Exception:
        sklearn_available = False
    diagnostics = {
        "rows_loaded": int(len(prices)),
        "feature_rows": int(len(features)),
        "fold_count": int(len(folds)),
        "candidate_fold_count_before_time_filter": int(len(all_folds)),
        "prediction_rows": int(len(predictions)),
        "model_rows": int(len(summary)),
        "feature_manifest": feature_manifest,
        "sklearn_calibration_available": sklearn_available,
        "hmm_regime_joined": bool(hmm_joined),
        "market_metadata_note": "market metadata argument is accepted but UTC proxy windows are currently used",
        "elapsed_seconds": float(time.perf_counter() - started),
    }
    (output_dir / "probability_sweep_diagnostics.json").write_text(json.dumps(diagnostics, indent=2, default=str), encoding="utf-8")
    (output_dir / "summary_readme.txt").write_text(render_summary(summary, by_age, diagnostics), encoding="utf-8")
    return diagnostics


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Offline BTC 5-minute probability-model sweep/evaluator.")
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--market-window-seconds", type=int, default=300)
    parser.add_argument("--market-metadata", type=Path)
    parser.add_argument("--hmm-regime-utility-dir", type=Path)
    parser.add_argument("--candidate-regime-models", default=",".join(DEFAULT_CANDIDATE_REGIMES))
    parser.add_argument("--model-families", default=",".join(DEFAULT_MODEL_FAMILIES))
    parser.add_argument("--max-rows", type=int)
    parser.add_argument("--random-seed", type=int, default=42)
    parser.add_argument("--train-days", type=float)
    parser.add_argument("--train-rows", type=int)
    parser.add_argument("--test-days", type=float)
    parser.add_argument("--test-rows", type=int)
    parser.add_argument("--step-days", type=float)
    parser.add_argument("--step-rows", type=int)
    parser.add_argument("--max-folds", type=int, default=2, help="Safety limiter; use 0 for all folds.")
    parser.add_argument("--min-test-start", help="Only run folds whose test window overlaps this UTC timestamp or later.")
    parser.add_argument("--max-test-end", help="Only run folds whose test window overlaps this UTC timestamp or earlier.")
    parser.add_argument("--calibration-methods", default="logistic,bucketed")
    parser.add_argument("--price-column")
    parser.add_argument("--prediction-sample-rows", type=int, default=20000)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    diagnostics = run_sweep(args)
    print(json.dumps(diagnostics, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
