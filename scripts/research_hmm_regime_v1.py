#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.build_btc5m_event_table import load_binance, load_chainlink
from scripts.build_legacy_btc5m_event_set import DEFAULT_INPUT_ROOTS, discover_legacy_sources

DEFAULT_EVENT_SET = Path("artifacts/legacy_event_sets/btc5m_events_nearest_2s_v1.csv")
DEFAULT_OUTPUT_DIR = Path("artifacts/hmm_research/legacy_nearest_2s_v1")
SHOCK_AGE_CAP_SECONDS = 300.0
HMM_FEATURE_CLIP_ABS = 6.0
DEFAULT_HMM_STATE_COUNTS = [2, 3]
DEFAULT_HMM_SEEDS = [1, 2, 3, 4, 5]
SPARSE_60S_MIN_OBS = 5
SPARSE_120S_MIN_OBS = 8
SPARSE_180S_MIN_OBS = 10
SPARSE_300S_MIN_OBS = 15
FULL_FEATURE_COLUMNS = [
    "r_15s",
    "r_30s",
    "r_60s",
    "r_120s",
    "realized_vol_60s",
    "realized_vol_180s",
    "sign_flip_rate_60s",
    "sign_flip_rate_180s",
    "drift_to_vol_60s",
    "drift_to_vol_120s",
    "ew_return_tau_10s",
    "ew_return_tau_30s",
    "ew_return_tau_90s",
    "ew_abs_return_tau_10s",
    "ew_abs_return_tau_30s",
    "ew_abs_return_tau_90s",
    "ew_signed_imbalance_fast_minus_slow",
    "ew_abs_activity_fast_minus_slow",
    "price_transition_entropy_120s",
    "price_transition_entropy_300s",
    "shock_score_60s",
    "has_recent_shock",
    "shock_age_seconds_capped",
]
REDUCED_HMM_FEATURE_COLUMNS = [
    "r_60s",
    "r_120s",
    "realized_vol_180s",
    "sign_flip_rate_180s",
    "drift_to_vol_120s",
    "ew_return_tau_30s",
    "ew_abs_return_tau_30s",
    "price_transition_entropy_300s",
    "shock_score_60s",
    "has_recent_shock",
    "shock_age_seconds_capped",
]
FEATURE_COLUMNS = FULL_FEATURE_COLUMNS
DIAGNOSTIC_COLUMNS = [
    "market_id",
    "condition_id",
    "slug",
    "market_start_time",
    "market_end_time",
    "chainlink_label",
    "binance_label",
    "label_agreement",
    "tiny_move_near_boundary",
    "wide_or_missing_quote",
    "quote_abs_lag_sec",
    "chainlink_move",
    "binance_move",
]
OBS_COUNT_COLUMNS = [
    "obs_count_15s",
    "obs_count_30s",
    "obs_count_60s",
    "obs_count_120s",
    "obs_count_180s",
    "obs_count_300s",
]
SPARSE_FLAG_COLUMNS = [
    "sparse_60s_window",
    "sparse_120s_window",
    "sparse_180s_window",
    "sparse_300s_window",
]


def load_event_set(path: Path) -> pd.DataFrame:
    events = pd.read_csv(path)
    for column in ("market_start_time", "market_end_time", "chainlink_start_ts", "chainlink_end_ts", "binance_start_ts", "binance_end_ts", "quote_ts"):
        if column in events.columns:
            events[column] = pd.to_datetime(events[column], utc=True, errors="coerce")
    for column in ("label_agreement", "missing_chainlink_start", "missing_chainlink_end", "missing_binance_start", "missing_binance_end", "chainlink_binance_label_disagree", "tiny_move_near_boundary", "wide_or_missing_quote", "stale_quote"):
        if column in events.columns:
            events[column] = events[column].astype("boolean")
    if "condition_id" not in events.columns:
        events["condition_id"] = pd.NA
    return events.sort_values("market_start_time").reset_index(drop=True)


def load_price_data(input_roots: list[Path]) -> dict[str, Any]:
    sources, source_info = discover_legacy_sources(input_roots)
    chainlink_paths = [source.path / "chainlink_prices.jsonl" for source in sources if (source.path / "chainlink_prices.jsonl").exists()]
    binance_paths = [source.path / "binance_prices.jsonl" for source in sources if (source.path / "binance_prices.jsonl").exists()]
    chainlink = load_chainlink(chainlink_paths)
    binance = load_binance(binance_paths)
    return {
        "chainlink": chainlink.deduped_df.sort_values("source_time").reset_index(drop=True),
        "binance": binance.deduped_df.sort_values("event_time").reset_index(drop=True),
        "source_info": source_info,
    }


def asof_observation(df: pd.DataFrame, time_col: str, target: pd.Timestamp) -> dict[str, Any] | None:
    if df.empty:
        return None
    pos = int(df[time_col].searchsorted(target, side="right")) - 1
    if pos < 0:
        return None
    row = df.iloc[pos]
    if row[time_col] > target:
        return None
    return {
        "ts": row[time_col],
        "price": float(row["price"]),
        "lag_sec": float((target - row[time_col]).total_seconds()),
    }


def trailing_observations(df: pd.DataFrame, time_col: str, decision_ts: pd.Timestamp, lookback_sec: float) -> pd.DataFrame:
    if df.empty:
        return df
    start_ts = decision_ts - pd.Timedelta(seconds=lookback_sec)
    return df[(df[time_col] >= start_ts) & (df[time_col] <= decision_ts)].copy()


def compute_log_return(current_price: float | None, previous_price: float | None) -> float | None:
    if current_price is None or previous_price is None or current_price <= 0 or previous_price <= 0:
        return None
    return float(math.log(current_price / previous_price))


def trailing_log_returns(df: pd.DataFrame, time_col: str) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=[time_col, "log_return"])
    series = df[[time_col, "price"]].copy().sort_values(time_col).reset_index(drop=True)
    series["prev_price"] = series["price"].shift(1)
    series["log_return"] = np.log(series["price"] / series["prev_price"])
    return series.dropna(subset=["log_return"])[[time_col, "log_return"]].reset_index(drop=True)


def sign_flip_rate(returns: pd.Series) -> float | None:
    nonzero = returns[returns != 0]
    if len(nonzero) < 2:
        return None
    signs = np.sign(nonzero.to_numpy())
    flips = np.sum(signs[1:] != signs[:-1])
    return float(flips / (len(signs) - 1))


def exp_weighted_mean(values: np.ndarray, ages_sec: np.ndarray, tau_sec: float) -> float | None:
    if len(values) == 0:
        return None
    weights = np.exp(-ages_sec / tau_sec)
    denom = float(weights.sum())
    if denom <= 0:
        return None
    return float(np.dot(values, weights) / denom)


def transition_entropy(returns: pd.Series, flat_threshold: float = 1e-9) -> float | None:
    if len(returns) < 2:
        return None
    def _state(value: float) -> int:
        if value > flat_threshold:
            return 1
        if value < -flat_threshold:
            return -1
        return 0
    states = [_state(float(value)) for value in returns]
    if len(states) < 2:
        return None
    transition_counts: dict[tuple[int, int], int] = {}
    for previous, current in zip(states[:-1], states[1:], strict=False):
        transition_counts[(previous, current)] = transition_counts.get((previous, current), 0) + 1
    total = sum(transition_counts.values())
    if total == 0:
        return None
    probabilities = np.array([count / total for count in transition_counts.values()], dtype=float)
    entropy = float(-(probabilities * np.log(probabilities)).sum())
    return float(entropy / math.log(9))


def compute_shock_metrics(returns_df: pd.DataFrame, decision_ts: pd.Timestamp, baseline_vol: float | None, z_threshold: float = 3.0) -> tuple[float | None, float | None]:
    if returns_df.empty:
        return None, None
    epsilon = 1e-9
    scale = max(baseline_vol or 0.0, epsilon)
    zscores = returns_df["log_return"].abs() / scale
    shock_score = float(zscores.max()) if not zscores.empty else None
    shocked = returns_df.loc[zscores >= z_threshold]
    if shocked.empty:
        return shock_score, None
    age = float((decision_ts - shocked.iloc[-1]["ts"]).total_seconds())
    return shock_score, age


def feature_columns_for_set(feature_set: str) -> list[str]:
    if feature_set == "reduced":
        return REDUCED_HMM_FEATURE_COLUMNS
    if feature_set == "full":
        return FULL_FEATURE_COLUMNS
    raise ValueError(f"Unknown feature set: {feature_set}")


def compute_feature_row(
    event_row: pd.Series,
    *,
    chainlink_df: pd.DataFrame,
    binance_df: pd.DataFrame,
    shock_age_cap_seconds: float = SHOCK_AGE_CAP_SECONDS,
) -> tuple[dict[str, Any] | None, str | None]:
    decision_ts = event_row["market_start_time"]
    candidates = [
        ("chainlink", chainlink_df, "source_time"),
        ("binance", binance_df, "event_time"),
    ]
    last_reason = "no_usable_source"
    for source_name, source_df, time_col in candidates:
        feature_row, reason = _compute_feature_row_for_source(
            event_row,
            source_name,
            source_df,
            time_col,
            decision_ts,
            shock_age_cap_seconds=shock_age_cap_seconds,
        )
        if feature_row is not None:
            return feature_row, None
        last_reason = reason
    return None, last_reason


def _compute_feature_row_for_source(
    event_row: pd.Series,
    source_name: str,
    source_df: pd.DataFrame,
    time_col: str,
    decision_ts: pd.Timestamp,
    *,
    shock_age_cap_seconds: float,
) -> tuple[dict[str, Any] | None, str]:
    trailing_15 = trailing_observations(source_df, time_col, decision_ts, 15)
    trailing_30 = trailing_observations(source_df, time_col, decision_ts, 30)
    trailing_60 = trailing_observations(source_df, time_col, decision_ts, 60)
    trailing_120 = trailing_observations(source_df, time_col, decision_ts, 120)
    trailing_180 = trailing_observations(source_df, time_col, decision_ts, 180)
    trailing_300 = trailing_observations(source_df, time_col, decision_ts, 300)
    if trailing_300.empty:
        return None, f"{source_name}_no_trailing_prices"
    current_obs = asof_observation(source_df, time_col, decision_ts)
    if current_obs is None:
        return None, f"{source_name}_no_price_at_decision"

    used_timestamps = [current_obs["ts"]]
    returns_features: dict[str, Any] = {}
    for horizon in (15, 30, 60, 120):
        previous_obs = asof_observation(source_df, time_col, decision_ts - pd.Timedelta(seconds=horizon))
        if previous_obs is None:
            return None, f"{source_name}_missing_{horizon}s_history"
        used_timestamps.append(previous_obs["ts"])
        returns_features[f"r_{horizon}s"] = compute_log_return(current_obs["price"], previous_obs["price"])

    trailing_60_returns = trailing_log_returns(trailing_60, time_col).rename(columns={time_col: "ts"})
    trailing_120_returns = trailing_log_returns(trailing_120, time_col).rename(columns={time_col: "ts"})
    trailing_180_returns = trailing_log_returns(trailing_180, time_col).rename(columns={time_col: "ts"})
    trailing_300_returns = trailing_log_returns(trailing_300, time_col).rename(columns={time_col: "ts"})

    if trailing_60_returns.empty or trailing_180_returns.empty or trailing_300_returns.empty:
        return None, f"{source_name}_insufficient_return_history"

    used_timestamps.extend(trailing_300_returns["ts"].tolist())
    realized_vol_60 = float(trailing_60_returns["log_return"].std(ddof=0))
    realized_vol_180 = float(trailing_180_returns["log_return"].std(ddof=0))
    epsilon = 1e-9

    ew_signed = {}
    ew_abs = {}
    ages_300 = np.array((decision_ts - trailing_300_returns["ts"]).dt.total_seconds(), dtype=float)
    values_300 = trailing_300_returns["log_return"].to_numpy(dtype=float)
    abs_values_300 = np.abs(values_300)
    for tau in (10, 30, 90):
        ew_signed[tau] = exp_weighted_mean(values_300, ages_300, tau)
        ew_abs[tau] = exp_weighted_mean(abs_values_300, ages_300, tau)

    shock_score, shock_age = compute_shock_metrics(trailing_60_returns, decision_ts, realized_vol_180)
    has_recent_shock = 0.0 if shock_age is None else 1.0
    shock_age_seconds_capped = float(shock_age_cap_seconds if shock_age is None else min(shock_age, shock_age_cap_seconds))
    max_feature_source_ts = max(used_timestamps)
    if max_feature_source_ts > decision_ts:
        return None, f"{source_name}_feature_leakage"

    feature_row = {
        "feature_price_source": source_name,
        "decision_timestamp": decision_ts,
        "max_feature_source_ts": max_feature_source_ts,
        "feature_source_lag_sec": float((decision_ts - max_feature_source_ts).total_seconds()),
        **returns_features,
        "realized_vol_60s": realized_vol_60,
        "realized_vol_180s": realized_vol_180,
        "sign_flip_rate_60s": sign_flip_rate(trailing_60_returns["log_return"]),
        "sign_flip_rate_180s": sign_flip_rate(trailing_180_returns["log_return"]),
        "drift_to_vol_60s": returns_features["r_60s"] / max(realized_vol_60, epsilon) if returns_features["r_60s"] is not None else None,
        "drift_to_vol_120s": returns_features["r_120s"] / max(realized_vol_180, epsilon) if returns_features["r_120s"] is not None else None,
        "ew_return_tau_10s": ew_signed[10],
        "ew_return_tau_30s": ew_signed[30],
        "ew_return_tau_90s": ew_signed[90],
        "ew_abs_return_tau_10s": ew_abs[10],
        "ew_abs_return_tau_30s": ew_abs[30],
        "ew_abs_return_tau_90s": ew_abs[90],
        "ew_signed_imbalance_fast_minus_slow": None if ew_signed[10] is None or ew_signed[90] is None else ew_signed[10] - ew_signed[90],
        "ew_abs_activity_fast_minus_slow": None if ew_abs[10] is None or ew_abs[90] is None else ew_abs[10] - ew_abs[90],
        "price_transition_entropy_120s": transition_entropy(trailing_120_returns["log_return"]),
        "price_transition_entropy_300s": transition_entropy(trailing_300_returns["log_return"]),
        "shock_score_60s": shock_score,
        "has_recent_shock": has_recent_shock,
        "shock_age_seconds_capped": shock_age_seconds_capped,
        "shock_age_seconds": shock_age,
        "obs_count_15s": int(len(trailing_15)),
        "obs_count_30s": int(len(trailing_30)),
        "obs_count_60s": int(len(trailing_60)),
        "obs_count_120s": int(len(trailing_120)),
        "obs_count_180s": int(len(trailing_180)),
        "obs_count_300s": int(len(trailing_300)),
        "sparse_60s_window": bool(len(trailing_60) < SPARSE_60S_MIN_OBS),
        "sparse_120s_window": bool(len(trailing_120) < SPARSE_120S_MIN_OBS),
        "sparse_180s_window": bool(len(trailing_180) < SPARSE_180S_MIN_OBS),
        "sparse_300s_window": bool(len(trailing_300) < SPARSE_300S_MIN_OBS),
    }
    for column in DIAGNOSTIC_COLUMNS:
        feature_row[column] = event_row[column] if column in event_row else pd.NA
    return feature_row, "ok"


def assign_splits(df: pd.DataFrame) -> pd.DataFrame:
    ordered = df.sort_values("market_start_time").reset_index(drop=True).copy()
    n = len(ordered)
    train_end = int(math.floor(n * 0.6))
    validation_end = int(math.floor(n * 0.8))
    ordered["split"] = "test"
    ordered.loc[: train_end - 1, "split"] = "train"
    ordered.loc[train_end: validation_end - 1, "split"] = "validation"
    return ordered


def standardize_features(df: pd.DataFrame, feature_columns: list[str]) -> tuple[pd.DataFrame, dict[str, dict[str, float]]]:
    standardized = df.copy()
    train = standardized[standardized["split"] == "train"]
    params: dict[str, dict[str, float]] = {}
    for column in feature_columns:
        mean = float(train[column].mean())
        std = float(train[column].std(ddof=0))
        if not np.isfinite(std) or std == 0.0:
            std = 1.0
        params[column] = {"mean": mean, "std": std}
        standardized[column] = (standardized[column] - mean) / std
    return standardized, params


def apply_hmm_quality_filter(df: pd.DataFrame, feature_columns: list[str]) -> tuple[pd.DataFrame, dict[str, int]]:
    reason_masks = {
        "sparse_60s_window": df["sparse_60s_window"].fillna(False).astype(bool),
        "sparse_120s_window": df["sparse_120s_window"].fillna(False).astype(bool),
    }
    finite_mask = np.isfinite(df[feature_columns].astype(float).to_numpy()).all(axis=1)
    reason_masks["nonfinite_selected_feature"] = ~pd.Series(finite_mask, index=df.index)
    excluded_mask = pd.Series(False, index=df.index)
    reason_counts: dict[str, int] = {}
    for reason, mask in reason_masks.items():
        count = int(mask.sum())
        if count:
            reason_counts[reason] = count
        excluded_mask |= mask
    return df.loc[~excluded_mask].copy(), reason_counts


def clip_standardized_features(
    df: pd.DataFrame,
    feature_columns: list[str],
    clip_abs: float,
) -> tuple[pd.DataFrame, dict[str, int]]:
    clipped = df.copy()
    clip_counts: dict[str, int] = {}
    for column in feature_columns:
        series = clipped[column].astype(float)
        count = int((series.abs() > clip_abs).sum())
        clip_counts[column] = count
        clipped[column] = series.clip(lower=-clip_abs, upper=clip_abs)
    return clipped, clip_counts


def try_fit_hmms(
    standardized: pd.DataFrame,
    *,
    ks: list[int],
    feature_columns: list[str],
    seeds: list[int],
) -> tuple[dict[str, Any], list[str]]:
    warnings: list[str] = []
    try:
        from hmmlearn.hmm import GaussianHMM
    except Exception:
        return {
            "hmmlearn_available": False,
            "models": {},
            "candidate_fit_diagnostics": {},
        }, ["hmmlearn unavailable; wrote features only, skipped HMM fitting."]

    train = standardized[standardized["split"] == "train"]
    x_train = train[feature_columns].to_numpy()
    x_all = standardized[feature_columns].to_numpy()
    if len(x_train) == 0 or len(x_all) == 0:
        warnings.append("no eligible rows remained for HMM fitting")
        return {
            "hmmlearn_available": True,
            "models": {},
            "candidate_fit_diagnostics": {},
        }, warnings
    models: dict[str, Any] = {}
    candidate_fit_diagnostics: dict[str, Any] = {}
    for k in ks:
        fits: list[dict[str, Any]] = []
        best_fit: dict[str, Any] | None = None
        for seed in seeds:
            fit_warning: str | None = None
            try:
                model = GaussianHMM(
                    n_components=k,
                    covariance_type="diag",
                    n_iter=300,
                    tol=1e-3,
                    random_state=seed,
                )
                model.fit(x_train)
                train_log_likelihood = float(model.score(x_train))
                assignments = model.predict(x_all)
                occupancy = pd.Series(assignments).value_counts(normalize=True).sort_index().to_dict()
                min_occupancy = float(min(occupancy.values())) if occupancy else 0.0
                converged = bool(getattr(getattr(model, "monitor_", None), "converged", False))
                n_iter = int(getattr(getattr(model, "monitor_", None), "iter", 0))
                try:
                    _, posteriors = model.score_samples(x_all)
                    posterior_max = posteriors.max(axis=1)
                except Exception:
                    posterior_max = np.full(len(assignments), np.nan)
                fit = {
                    "seed": seed,
                    "converged": converged,
                    "final_log_likelihood": train_log_likelihood,
                    "n_iter": n_iter,
                    "state_occupancy": occupancy,
                    "min_state_occupancy": min_occupancy,
                    "warnings": [],
                    "model": model,
                    "assignments": assignments,
                    "posterior_max": posterior_max,
                }
            except Exception as exc:
                fit = {
                    "seed": seed,
                    "converged": False,
                    "final_log_likelihood": None,
                    "n_iter": 0,
                    "state_occupancy": {},
                    "min_state_occupancy": 0.0,
                    "warnings": [str(exc)],
                    "model": None,
                    "assignments": None,
                    "posterior_max": None,
                }
                fit_warning = f"k={k} seed={seed} fit failed: {exc}"
            fits.append(fit)
            if fit_warning:
                warnings.append(fit_warning)
            if fit["model"] is None:
                continue
            if best_fit is None:
                best_fit = fit
                continue
            best_score = (best_fit["converged"], best_fit["final_log_likelihood"])
            fit_score = (fit["converged"], fit["final_log_likelihood"])
            if fit_score > best_score:
                best_fit = fit
        candidate_fit_diagnostics[str(k)] = {
            "selected_seed": None if best_fit is None else best_fit["seed"],
            "fits": [
                {
                    "seed": fit["seed"],
                    "converged": fit["converged"],
                    "final_log_likelihood": fit["final_log_likelihood"],
                    "n_iter": fit["n_iter"],
                    "state_occupancy": fit["state_occupancy"],
                    "min_state_occupancy": fit["min_state_occupancy"],
                    "warnings": fit["warnings"],
                }
                for fit in fits
            ],
        }
        if best_fit is None:
            warnings.append(f"all HMM fits failed for k={k}")
            continue
        models[str(k)] = {
            "model": best_fit["model"],
            "assignments": best_fit["assignments"],
            "posterior_max": best_fit["posterior_max"],
            "selected_seed": best_fit["seed"],
            "converged": best_fit["converged"],
            "final_log_likelihood": best_fit["final_log_likelihood"],
            "n_iter": best_fit["n_iter"],
            "state_occupancy": best_fit["state_occupancy"],
            "min_state_occupancy": best_fit["min_state_occupancy"],
        }
    return {"hmmlearn_available": True, "models": models, "candidate_fit_diagnostics": candidate_fit_diagnostics}, warnings


def build_feature_matrix(
    *,
    event_set: pd.DataFrame,
    chainlink_df: pd.DataFrame,
    binance_df: pd.DataFrame,
    shock_age_cap_seconds: float = SHOCK_AGE_CAP_SECONDS,
) -> tuple[pd.DataFrame, dict[str, int], list[str]]:
    rows: list[dict[str, Any]] = []
    dropped: dict[str, int] = {}
    leakage_warnings: list[str] = []
    for _, event_row in event_set.iterrows():
        feature_row, drop_reason = compute_feature_row(
            event_row,
            chainlink_df=chainlink_df,
            binance_df=binance_df,
            shock_age_cap_seconds=shock_age_cap_seconds,
        )
        if feature_row is None:
            dropped[drop_reason or "unknown"] = dropped.get(drop_reason or "unknown", 0) + 1
            continue
        if feature_row["max_feature_source_ts"] > feature_row["decision_timestamp"]:
            leakage_warnings.append(f"leakage_detected:{event_row['market_id']}")
            continue
        rows.append(feature_row)
    features = pd.DataFrame(rows)
    if not features.empty:
        features = assign_splits(features)
    return features, dropped, leakage_warnings


def prepare_hmm_matrix(
    df: pd.DataFrame,
    feature_columns: list[str],
) -> tuple[pd.DataFrame, dict[str, int], int]:
    prepared = df.copy()
    nan_counts = {
        column: int((~np.isfinite(prepared[column].astype(float))).sum())
        for column in feature_columns
    }
    nonfinite_mask = ~np.isfinite(prepared[feature_columns].astype(float).to_numpy()).all(axis=1)
    dropped = int(nonfinite_mask.sum())
    if dropped:
        prepared = prepared.loc[~nonfinite_mask].reset_index(drop=True)
    return prepared, nan_counts, dropped


def state_assignments_frame(features: pd.DataFrame, assignments: np.ndarray, posterior_max: np.ndarray) -> pd.DataFrame:
    frame = features[
        [
            "market_id",
            "slug",
            "market_start_time",
            "split",
            "chainlink_label",
            "tiny_move_near_boundary",
            "label_agreement",
            "wide_or_missing_quote",
            "obs_count_60s",
            "obs_count_120s",
            "sparse_60s_window",
            "sparse_120s_window",
        ]
    ].copy()
    frame["assigned_state"] = assignments
    frame["state_posterior_max"] = posterior_max
    return frame


def summarize_state_diagnostics(
    *,
    features_raw: pd.DataFrame,
    features_std: pd.DataFrame,
    assignments: pd.DataFrame,
    model: Any,
    feature_columns: list[str],
    warnings: list[str],
    dropped_rows: dict[str, int],
) -> dict[str, Any]:
    diagnostics: dict[str, Any] = {
        "transition_matrix": model.transmat_.tolist(),
        "state_occupancy_overall": assignments["assigned_state"].value_counts(normalize=True).sort_index().to_dict(),
        "state_occupancy_by_split": {
            split: group["assigned_state"].value_counts(normalize=True).sort_index().to_dict()
            for split, group in assignments.groupby("split")
        },
        "mean_raw_feature_values_by_state": features_raw.assign(assigned_state=assignments["assigned_state"]).groupby("assigned_state")[feature_columns].mean().to_dict(orient="index"),
        "mean_standardized_feature_values_by_state": features_std.assign(assigned_state=assignments["assigned_state"]).groupby("assigned_state")[feature_columns].mean().to_dict(orient="index"),
        "chainlink_label_distribution_by_state": {
            str(state): group["chainlink_label"].value_counts(normalize=True).to_dict()
            for state, group in assignments.groupby("assigned_state")
        },
        "chainlink_label_distribution_by_state_excluding_tiny_moves": {
            str(state): group.loc[~group["tiny_move_near_boundary"].fillna(False), "chainlink_label"].value_counts(normalize=True).to_dict()
            for state, group in assignments.groupby("assigned_state")
        },
        "tiny_move_count_by_state": assignments.groupby("assigned_state")["tiny_move_near_boundary"].apply(lambda s: int(s.fillna(False).sum())).to_dict(),
        "tiny_move_rate_by_state": assignments.groupby("assigned_state")["tiny_move_near_boundary"].apply(lambda s: float(s.fillna(False).mean())).to_dict(),
        "disagreement_count_by_state": assignments.groupby("assigned_state")["label_agreement"].apply(lambda s: int((s == False).sum())).to_dict(),
        "average_absolute_chainlink_move_by_state": features_raw.assign(assigned_state=assignments["assigned_state"]).groupby("assigned_state")["chainlink_move"].apply(lambda s: float(s.abs().mean())).to_dict(),
        "missing_feature_counts": features_raw[feature_columns].isna().sum().to_dict(),
        "dropped_row_counts": dropped_rows,
        "warnings": warnings,
    }
    return diagnostics


def build_warnings(features: pd.DataFrame, event_count: int, dropped_rows: dict[str, int], leakage_warnings: list[str]) -> list[str]:
    warnings: list[str] = []
    if len(features) < 100:
        warnings.append("too few usable rows after feature construction")
    if leakage_warnings:
        warnings.append(f"feature leakage detected for {len(leakage_warnings)} rows")
    missing_rates = features[FULL_FEATURE_COLUMNS].isna().mean() if not features.empty else pd.Series(dtype=float)
    high_missing = missing_rates[missing_rates > 0.1]
    if not high_missing.empty:
        warnings.append(f"high missingness in features: {', '.join(high_missing.index.tolist())}")
    if event_count and int(features["tiny_move_near_boundary"].fillna(False).sum()) / max(len(features), 1) > 0.5:
        warnings.append("diagnostics dominated by tiny_move_near_boundary events")
    if not features.empty and int(features["wide_or_missing_quote"].fillna(False).sum()) >= len(features) * 0.95:
        warnings.append("all or nearly all quote rows marked wide_or_missing_quote; quote state belongs to later decision-layer interpretation")
    if dropped_rows:
        warnings.append(f"dropped rows: {dropped_rows}")
    return warnings


def write_outputs(
    *,
    output_dir: Path,
    features_raw: pd.DataFrame,
    features_std: pd.DataFrame,
    diagnostics: dict[str, Any],
    hmm_results: dict[str, Any],
) -> dict[str, str]:
    output_dir.mkdir(parents=True, exist_ok=True)
    raw_path = output_dir / "hmm_features_raw.csv"
    std_path = output_dir / "hmm_features_standardized.csv"
    diagnostics_path = output_dir / "hmm_diagnostics.json"
    readme_path = output_dir / "hmm_readme_summary.txt"
    features_raw.to_csv(raw_path, index=False)
    features_std.to_csv(std_path, index=False)
    diagnostics_path.write_text(json.dumps(diagnostics, indent=2, default=str), encoding="utf-8")
    readme_path.write_text(_render_summary(diagnostics, hmm_results, raw_path), encoding="utf-8")

    paths = {
        "hmm_features_raw": str(raw_path),
        "hmm_features_standardized": str(std_path),
        "hmm_diagnostics": str(diagnostics_path),
        "hmm_readme_summary": str(readme_path),
    }
    for k, model_payload in hmm_results["models"].items():
        assignment_path = output_dir / f"hmm_state_assignments_k{k}.csv"
        model_payload["assignments_frame"].to_csv(assignment_path, index=False)
        paths[f"hmm_state_assignments_k{k}"] = str(assignment_path)
    return paths


def _render_summary(diagnostics: dict[str, Any], hmm_results: dict[str, Any], raw_path: Path) -> str:
    lines = [
        f"event_rows_loaded={diagnostics['event_rows_loaded']}",
        f"feature_rows_emitted={diagnostics['feature_rows_emitted']}",
        f"rows_dropped={diagnostics['rows_dropped_total']}",
        f"hmmlearn_available={diagnostics['hmmlearn_available']}",
        f"output_raw_features={raw_path}",
        "warnings:",
    ]
    lines.extend(f"- {warning}" for warning in diagnostics["warnings"])
    for k, section in diagnostics.get("hmm_models", {}).items():
        lines.append(f"state_occupancy_k{k}={section['state_occupancy_overall']}")
    return "\n".join(lines) + "\n"


def run_research(
    *,
    event_set_path: Path,
    input_roots: list[Path],
    output_dir: Path,
    shock_age_cap_seconds: float = SHOCK_AGE_CAP_SECONDS,
    feature_set: str = "reduced",
    ks: list[int] | None = None,
    seeds: list[int] | None = None,
    hmm_feature_clip_abs: float = HMM_FEATURE_CLIP_ABS,
) -> dict[str, Any]:
    event_set = load_event_set(event_set_path)
    price_data = load_price_data(input_roots)
    selected_hmm_feature_columns = feature_columns_for_set(feature_set)
    features_raw, dropped_rows, leakage_warnings = build_feature_matrix(
        event_set=event_set,
        chainlink_df=price_data["chainlink"],
        binance_df=price_data["binance"],
        shock_age_cap_seconds=shock_age_cap_seconds,
    )
    warnings = build_warnings(features_raw, len(event_set), dropped_rows, leakage_warnings)
    if features_raw.empty:
        raise RuntimeError("No usable feature rows emitted.")

    missing_feature_counts_before_encoding = features_raw[selected_hmm_feature_columns + ["shock_age_seconds"]].isna().sum().to_dict()
    features_std, scaler_params = standardize_features(features_raw, FULL_FEATURE_COLUMNS)
    hmm_rows_available_before_quality_filter = int(len(features_raw))
    eligible_raw, hmm_quality_exclusion_reasons = apply_hmm_quality_filter(features_raw, selected_hmm_feature_columns)
    eligible_std = features_std.loc[eligible_raw.index].reset_index(drop=True)
    hmm_rows_excluded_by_quality_filter = int(hmm_rows_available_before_quality_filter - len(eligible_raw))
    clipped_std, clipped_value_counts_by_feature = clip_standardized_features(eligible_std, selected_hmm_feature_columns, hmm_feature_clip_abs)
    features_raw_hmm, hmm_feature_nan_counts_after_encoding, hmm_rows_dropped_for_nonfinite = prepare_hmm_matrix(eligible_raw, selected_hmm_feature_columns)
    features_std_hmm, _, _ = prepare_hmm_matrix(clipped_std, selected_hmm_feature_columns)
    if hmm_rows_dropped_for_nonfinite:
        warnings.append(f"dropped {hmm_rows_dropped_for_nonfinite} rows for nonfinite HMM features")
    if hmm_rows_excluded_by_quality_filter:
        warnings.append(f"excluded {hmm_rows_excluded_by_quality_filter} rows from HMM quality filter")
    if len(features_raw_hmm) == 0:
        warnings.append("no eligible rows remained after HMM quality filtering")
    hmm_results, hmm_warnings = try_fit_hmms(
        features_std_hmm,
        ks=ks or DEFAULT_HMM_STATE_COUNTS,
        feature_columns=selected_hmm_feature_columns,
        seeds=seeds or DEFAULT_HMM_SEEDS,
    )
    warnings.extend(hmm_warnings)

    diagnostics: dict[str, Any] = {
        "event_rows_loaded": int(len(event_set)),
        "feature_rows_emitted": int(len(features_raw)),
        "rows_dropped_total": int(sum(dropped_rows.values())),
        "rows_dropped_by_reason": dropped_rows,
        "hmmlearn_available": bool(hmm_results["hmmlearn_available"]),
        "selected_feature_set": feature_set,
        "selected_hmm_feature_columns": selected_hmm_feature_columns,
        "candidate_k_values": ks or DEFAULT_HMM_STATE_COUNTS,
        "candidate_seeds": seeds or DEFAULT_HMM_SEEDS,
        "shock_age_cap_seconds": shock_age_cap_seconds,
        "has_recent_shock_added": True,
        "missing_feature_counts_before_encoding": missing_feature_counts_before_encoding,
        "hmm_feature_nan_counts_after_encoding": hmm_feature_nan_counts_after_encoding,
        "hmm_rows_available_before_quality_filter": hmm_rows_available_before_quality_filter,
        "hmm_rows_excluded_by_quality_filter": hmm_rows_excluded_by_quality_filter,
        "hmm_rows_used_after_quality_filter": int(len(eligible_raw)),
        "hmm_rows_used": int(len(features_std_hmm)),
        "hmm_rows_dropped_for_nonfinite": int(hmm_rows_dropped_for_nonfinite),
        "hmm_quality_exclusion_reasons": hmm_quality_exclusion_reasons,
        "hmm_feature_clip_abs": hmm_feature_clip_abs,
        "clipped_value_counts_by_feature": clipped_value_counts_by_feature,
        "source_info": price_data["source_info"],
        "scaler_params": scaler_params,
        "warnings": warnings,
        "hmm_models": {},
        "candidate_fit_diagnostics": hmm_results.get("candidate_fit_diagnostics", {}),
    }
    if hmm_results["hmmlearn_available"]:
        for k, model_payload in hmm_results["models"].items():
            assignments_frame = state_assignments_frame(features_raw_hmm, model_payload["assignments"], model_payload["posterior_max"])
            model_payload["assignments_frame"] = assignments_frame
            state_diag = summarize_state_diagnostics(
                features_raw=features_raw_hmm,
                features_std=features_std_hmm,
                assignments=assignments_frame,
                model=model_payload["model"],
                feature_columns=selected_hmm_feature_columns,
                warnings=warnings,
                dropped_rows=dropped_rows,
            )
            occupancy = state_diag["state_occupancy_overall"]
            if any(value < 0.05 for value in occupancy.values()):
                warnings.append(f"HMM state occupancy below 5% for k={k}")
            state_diag["selected_seed"] = model_payload["selected_seed"]
            state_diag["converged"] = model_payload["converged"]
            state_diag["final_log_likelihood"] = model_payload["final_log_likelihood"]
            state_diag["n_iter"] = model_payload["n_iter"]
            state_diag["min_state_occupancy"] = model_payload["min_state_occupancy"]
            diagnostics["hmm_models"][k] = state_diag

    paths = write_outputs(
        output_dir=output_dir,
        features_raw=features_raw,
        features_std=features_std,
        diagnostics=diagnostics,
        hmm_results=hmm_results,
    )
    diagnostics["output_paths"] = paths
    (output_dir / "hmm_diagnostics.json").write_text(json.dumps(diagnostics, indent=2, default=str), encoding="utf-8")
    (output_dir / "hmm_readme_summary.txt").write_text(_render_summary(diagnostics, hmm_results, Path(paths["hmm_features_raw"])), encoding="utf-8")
    return diagnostics


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Offline BTC-5m HMM regime research scaffold.")
    parser.add_argument("--event-set-path", type=Path, default=DEFAULT_EVENT_SET)
    parser.add_argument("--input-root", type=Path, action="append", default=None)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--shock-age-cap-seconds", type=float, default=SHOCK_AGE_CAP_SECONDS)
    parser.add_argument("--feature-set", choices=["reduced", "full"], default="reduced")
    parser.add_argument("--k", dest="ks", type=int, nargs="+", default=DEFAULT_HMM_STATE_COUNTS)
    parser.add_argument("--seed", dest="seeds", type=int, nargs="+", default=DEFAULT_HMM_SEEDS)
    parser.add_argument("--hmm-feature-clip-abs", type=float, default=HMM_FEATURE_CLIP_ABS)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    diagnostics = run_research(
        event_set_path=args.event_set_path,
        input_roots=args.input_root or DEFAULT_INPUT_ROOTS,
        output_dir=args.output_dir,
        shock_age_cap_seconds=args.shock_age_cap_seconds,
        feature_set=args.feature_set,
        ks=args.ks,
        seeds=args.seeds,
        hmm_feature_clip_abs=args.hmm_feature_clip_abs,
    )
    print(json.dumps({
        "event_rows_loaded": diagnostics["event_rows_loaded"],
        "feature_rows_emitted": diagnostics["feature_rows_emitted"],
        "rows_dropped_total": diagnostics["rows_dropped_total"],
        "hmmlearn_available": diagnostics["hmmlearn_available"],
        "selected_feature_set": diagnostics["selected_feature_set"],
        "hmm_rows_used_after_quality_filter": diagnostics["hmm_rows_used_after_quality_filter"],
        "warnings": diagnostics["warnings"],
        "output_paths": diagnostics["output_paths"],
    }, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
