#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import sys
import time
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.build_binance_btc5m_research_events import (
    DEFAULT_INPUT_ROOTS,
    DEFAULT_OUTPUT_CSV,
    load_binance_1m_klines,
)

DEFAULT_OUTPUT_DIR = Path("artifacts/binance_btc5m_research/hmm_v1")
SHOCK_AGE_CAP_MINUTES = 30.0
HMM_FEATURE_CLIP_ABS = 6.0
DEFAULT_HMM_STATE_COUNTS = [2, 3, 4]
DEFAULT_HMM_SEEDS = [1, 2, 3, 4, 5]
DEFAULT_FIT_TAIL_ROWS = 300000
ENTROPY_MODES = {"exact", "fast", "off"}
FEATURE_COLUMNS = [
    "r_1m",
    "r_2m",
    "r_3m",
    "r_5m",
    "r_10m",
    "r_15m",
    "realized_vol_5m",
    "realized_vol_15m",
    "realized_vol_30m",
    "sign_flip_rate_5m",
    "sign_flip_rate_15m",
    "sign_flip_rate_30m",
    "drift_to_vol_5m",
    "drift_to_vol_15m",
    "drift_to_vol_30m",
    "ew_return_tau_2m",
    "ew_return_tau_5m",
    "ew_return_tau_15m",
    "ew_abs_return_tau_2m",
    "ew_abs_return_tau_5m",
    "ew_abs_return_tau_15m",
    "ew_signed_imbalance_fast_minus_slow",
    "ew_abs_activity_fast_minus_slow",
    "price_transition_entropy_15m",
    "price_transition_entropy_30m",
    "shock_score_5m",
    "has_recent_shock",
    "shock_age_minutes_capped",
]


def load_event_set(path: Path) -> pd.DataFrame:
    events = pd.read_csv(path)
    for column in ("event_start_time", "event_end_time", "source_start_ts", "source_end_ts"):
        if column in events.columns:
            events[column] = pd.to_datetime(events[column], utc=True, errors="coerce")
    if "tiny_move_near_boundary" in events.columns:
        events["tiny_move_near_boundary"] = events["tiny_move_near_boundary"].astype(bool)
    return events.sort_values("event_start_time").reset_index(drop=True)


def compute_log_return(current_price: float | None, previous_price: float | None) -> float | None:
    if current_price is None or previous_price is None or current_price <= 0 or previous_price <= 0:
        return None
    return float(math.log(current_price / previous_price))


def assign_splits(df: pd.DataFrame) -> pd.DataFrame:
    ordered = df.sort_values("event_start_time").reset_index(drop=True).copy()
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


def clip_standardized_features(df: pd.DataFrame, feature_columns: list[str], clip_abs: float) -> tuple[pd.DataFrame, dict[str, int]]:
    clipped = df.copy()
    counts: dict[str, int] = {}
    for column in feature_columns:
        series = clipped[column].astype(float)
        counts[column] = int((series.abs() > clip_abs).sum())
        clipped[column] = series.clip(-clip_abs, clip_abs)
    return clipped, counts


def filter_events(
    events: pd.DataFrame,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    max_events: int | None = None,
    tail_events: int | None = None,
) -> pd.DataFrame:
    selected = events.sort_values("event_start_time").reset_index(drop=True)
    if start_date is not None:
        selected = selected[selected["event_start_time"] >= pd.Timestamp(start_date, tz="UTC")]
    if end_date is not None:
        selected = selected[selected["event_start_time"] < pd.Timestamp(end_date, tz="UTC") + pd.Timedelta(days=1)]
    selected = selected.reset_index(drop=True)
    if tail_events is not None:
        selected = selected.tail(int(tail_events)).reset_index(drop=True)
    elif max_events is not None:
        selected = selected.head(int(max_events)).reset_index(drop=True)
    return selected


def select_hmm_fit_rows(
    features: pd.DataFrame,
    *,
    fit_max_rows: int | None = None,
    fit_tail_rows: int | None = DEFAULT_FIT_TAIL_ROWS,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    selected = features.sort_values("event_start_time").reset_index(drop=True)
    mode = "all"
    if fit_tail_rows is not None:
        selected = selected.tail(int(fit_tail_rows)).reset_index(drop=True)
        mode = "tail"
    elif fit_max_rows is not None:
        selected = selected.head(int(fit_max_rows)).reset_index(drop=True)
        mode = "head"
    return selected, {
        "fit_subset_mode": mode,
        "fit_requested_max_rows": fit_max_rows,
        "fit_requested_tail_rows": fit_tail_rows,
        "hmm_fit_rows": int(len(selected)),
        "hmm_fit_start_time": None if selected.empty else selected["event_start_time"].min().isoformat(),
        "hmm_fit_end_time": None if selected.empty else selected["event_start_time"].max().isoformat(),
    }


def _rolling_sign_flip_rate(log_returns: pd.Series, window: int) -> pd.Series:
    signs = np.sign(log_returns.fillna(0.0))
    previous = signs.shift(1)
    valid = ((signs != 0) & (previous != 0)).astype(float)
    flips = ((signs != previous) & (valid == 1.0)).astype(float)
    valid_count = valid.rolling(window=window, min_periods=window).sum()
    flip_count = flips.rolling(window=window, min_periods=window).sum()
    result = flip_count / valid_count.replace(0.0, np.nan)
    return result


def _ewm_alpha(tau_minutes: float) -> float:
    return float(1.0 - math.exp(-1.0 / tau_minutes))


def _state_entropy_fast(log_returns: pd.Series, window: int) -> pd.Series:
    states = pd.Series(
        np.where(log_returns > 1e-9, 1, np.where(log_returns < -1e-9, -1, 0)),
        index=log_returns.index,
    )
    probs = []
    for state_value in (-1, 0, 1):
        indicator = (states == state_value).astype(float)
        probs.append(indicator.rolling(window=window, min_periods=window).mean())
    entropy = pd.Series(0.0, index=log_returns.index, dtype=float)
    for prob in probs:
        entropy = entropy - prob.where(prob > 0.0, 1.0).map(np.log) * prob
    entropy = entropy / math.log(3)
    entropy[(probs[0].isna()) | (probs[1].isna()) | (probs[2].isna())] = np.nan
    return entropy


def _transition_entropy_exact_window(values: np.ndarray) -> float:
    if len(values) < 2:
        return float("nan")
    counts = np.zeros((3, 3), dtype=float)
    prev = values[:-1] + 1
    curr = values[1:] + 1
    for p, c in zip(prev, curr, strict=False):
        counts[int(p), int(c)] += 1.0
    total = float(counts.sum())
    if total == 0.0:
        return float("nan")
    probs = counts[counts > 0.0] / total
    return float((-(probs * np.log(probs)).sum()) / math.log(9))


def _state_entropy_exact(log_returns: pd.Series, window: int) -> pd.Series:
    states = np.where(log_returns > 1e-9, 1, np.where(log_returns < -1e-9, -1, 0))
    return pd.Series(states, index=log_returns.index).rolling(window=window, min_periods=window).apply(_transition_entropy_exact_window, raw=True)


def _compute_vectorized_price_features(
    prices: pd.DataFrame,
    *,
    entropy_mode: str,
    shock_age_cap_minutes: float,
) -> pd.DataFrame:
    ordered = prices.sort_values("event_time").reset_index(drop=True).copy()
    ordered["log_close"] = np.log(ordered["close"])
    ordered["log_return_1m"] = ordered["log_close"].diff()

    for horizon in (1, 2, 3, 5, 10, 15):
        ordered[f"r_{horizon}m"] = ordered["log_close"] - ordered["log_close"].shift(horizon)

    for window in (5, 15, 30):
        ordered[f"realized_vol_{window}m"] = ordered["log_return_1m"].rolling(window=window, min_periods=window).std(ddof=0)
        ordered[f"sign_flip_rate_{window}m"] = _rolling_sign_flip_rate(ordered["log_return_1m"], window)

    epsilon = 1e-9
    ordered["drift_to_vol_5m"] = ordered["r_5m"] / ordered["realized_vol_5m"].clip(lower=epsilon)
    ordered["drift_to_vol_15m"] = ordered["r_15m"] / ordered["realized_vol_15m"].clip(lower=epsilon)
    ordered["drift_to_vol_30m"] = ordered["r_15m"] / ordered["realized_vol_30m"].clip(lower=epsilon)

    abs_return = ordered["log_return_1m"].abs()
    for tau in (2, 5, 15):
        alpha = _ewm_alpha(tau)
        ordered[f"ew_return_tau_{tau}m"] = ordered["log_return_1m"].ewm(alpha=alpha, adjust=False).mean()
        ordered[f"ew_abs_return_tau_{tau}m"] = abs_return.ewm(alpha=alpha, adjust=False).mean()

    ordered["ew_signed_imbalance_fast_minus_slow"] = ordered["ew_return_tau_2m"] - ordered["ew_return_tau_15m"]
    ordered["ew_abs_activity_fast_minus_slow"] = ordered["ew_abs_return_tau_2m"] - ordered["ew_abs_return_tau_15m"]

    if entropy_mode == "off":
        ordered["price_transition_entropy_15m"] = 0.0
        ordered["price_transition_entropy_30m"] = 0.0
    elif entropy_mode == "fast":
        ordered["price_transition_entropy_15m"] = _state_entropy_fast(ordered["log_return_1m"], 15)
        ordered["price_transition_entropy_30m"] = _state_entropy_fast(ordered["log_return_1m"], 30)
    else:
        ordered["price_transition_entropy_15m"] = _state_entropy_exact(ordered["log_return_1m"], 15)
        ordered["price_transition_entropy_30m"] = _state_entropy_exact(ordered["log_return_1m"], 30)

    zscore = abs_return / ordered["realized_vol_30m"].clip(lower=epsilon)
    ordered["shock_score_5m"] = zscore.rolling(window=5, min_periods=5).max()
    shock_flag = (zscore >= 3.0).astype(float)
    shock_position = pd.Series(np.where(shock_flag == 1.0, np.arange(len(ordered), dtype=float), np.nan), index=ordered.index)
    last_shock_position = shock_position.ffill()
    shock_age = pd.Series(np.arange(len(ordered), dtype=float), index=ordered.index) - last_shock_position
    ordered["shock_age_minutes"] = shock_age.where(last_shock_position.notna(), np.nan)
    ordered["has_recent_shock"] = ordered["shock_age_minutes"].notna().astype(float)
    ordered["shock_age_minutes_capped"] = ordered["shock_age_minutes"].clip(upper=shock_age_cap_minutes)
    ordered.loc[ordered["shock_age_minutes_capped"].isna(), "shock_age_minutes_capped"] = shock_age_cap_minutes

    ordered["forward_abs_return_10m"] = (ordered["log_close"].shift(-10) - ordered["log_close"]).abs()
    ordered["forward_return_10m"] = ordered["log_close"].shift(-10) - ordered["log_close"]
    ordered["max_feature_source_ts"] = ordered["event_time"]
    ordered["feature_source_lag_minutes"] = 0.0
    return ordered


def build_feature_matrix(
    events: pd.DataFrame,
    prices: pd.DataFrame,
    *,
    shock_age_cap_minutes: float = SHOCK_AGE_CAP_MINUTES,
    entropy_mode: str = "fast",
) -> tuple[pd.DataFrame, dict[str, int], list[str]]:
    if entropy_mode not in ENTROPY_MODES:
        raise ValueError(f"Unsupported entropy mode: {entropy_mode}")
    leakage_warnings: list[str] = []
    if events.empty:
        return pd.DataFrame(), {}, leakage_warnings

    start_needed = events["event_start_time"].min() - pd.Timedelta(minutes=30)
    end_needed = events["event_start_time"].max() + pd.Timedelta(minutes=10)
    price_slice = prices[(prices["event_time"] >= start_needed) & (prices["event_time"] <= end_needed)].copy()
    vectorized = _compute_vectorized_price_features(price_slice, entropy_mode=entropy_mode, shock_age_cap_minutes=shock_age_cap_minutes)
    merged = events.merge(vectorized, left_on="event_start_time", right_on="event_time", how="left")
    merged["decision_timestamp"] = merged["event_start_time"]
    merged["forward_abs_return_5m"] = merged["abs_binance_move"]
    merged["continuation_reversal_10m"] = np.where(
        merged["forward_return_10m"].isna(),
        None,
        np.where(
            (np.sign(merged["binance_move"]) == 0) | (np.sign(merged["forward_return_10m"]) == 0),
            "flat",
            np.where(np.sign(merged["binance_move"]) == np.sign(merged["forward_return_10m"]), "continuation", "reversal"),
        ),
    )
    for column in FEATURE_COLUMNS:
        merged[column] = pd.to_numeric(merged[column], errors="coerce")
    merged = merged[merged["max_feature_source_ts"] <= merged["decision_timestamp"]]
    dropped = {
        "missing_feature_rows_after_join": int(merged["r_15m"].isna().sum()),
    }
    features = merged.dropna(subset=["r_15m", "realized_vol_30m"]).copy()
    if not features.empty:
        features = assign_splits(features)
    dropped = {key: value for key, value in dropped.items() if value}
    return features, dropped, leakage_warnings


def prepare_hmm_matrix(df: pd.DataFrame, feature_columns: list[str]) -> tuple[pd.DataFrame, dict[str, int], int]:
    prepared = df.copy()
    counts = {column: int((~np.isfinite(prepared[column].astype(float))).sum()) for column in feature_columns}
    mask = np.isfinite(prepared[feature_columns].astype(float).to_numpy()).all(axis=1)
    dropped = int((~mask).sum())
    return prepared.loc[mask].reset_index(drop=True), counts, dropped


def try_fit_hmms(standardized: pd.DataFrame, *, ks: list[int], feature_columns: list[str], seeds: list[int]) -> tuple[dict[str, Any], list[str]]:
    warnings: list[str] = []
    try:
        from hmmlearn.hmm import GaussianHMM
    except Exception:
        return {"hmmlearn_available": False, "models": {}, "candidate_fit_diagnostics": {}}, ["hmmlearn unavailable; wrote features only, skipped HMM fitting."]
    train = standardized[standardized["split"] == "train"]
    x_train = train[feature_columns].to_numpy()
    x_all = standardized[feature_columns].to_numpy()
    if len(x_train) == 0 or len(x_all) == 0:
        return {"hmmlearn_available": True, "models": {}, "candidate_fit_diagnostics": {}}, ["no usable rows remained for HMM fitting."]
    models: dict[str, Any] = {}
    fit_diags: dict[str, Any] = {}
    for k in ks:
        fits: list[dict[str, Any]] = []
        best: dict[str, Any] | None = None
        for seed in seeds:
            try:
                model = GaussianHMM(n_components=k, covariance_type="diag", n_iter=300, tol=1e-3, random_state=seed)
                model.fit(x_train)
                loglik = float(model.score(x_train))
                assignments = model.predict(x_all)
                converged = bool(getattr(getattr(model, "monitor_", None), "converged", False))
                n_iter = int(getattr(getattr(model, "monitor_", None), "iter", 0))
                occupancy = pd.Series(assignments).value_counts(normalize=True).sort_index().to_dict()
                min_occ = float(min(occupancy.values())) if occupancy else 0.0
                try:
                    _, posteriors = model.score_samples(x_all)
                    posterior_max = posteriors.max(axis=1)
                except Exception:
                    posterior_max = np.full(len(assignments), np.nan)
                fit = {
                    "seed": seed,
                    "converged": converged,
                    "final_log_likelihood": loglik,
                    "n_iter": n_iter,
                    "state_occupancy": occupancy,
                    "min_state_occupancy": min_occ,
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
                warnings.append(f"k={k} seed={seed} fit failed: {exc}")
            fits.append(fit)
            if fit["model"] is None:
                continue
            if best is None or (fit["converged"], fit["final_log_likelihood"]) > (best["converged"], best["final_log_likelihood"]):
                best = fit
        fit_diags[str(k)] = {
            "selected_seed": None if best is None else best["seed"],
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
        if best is None:
            warnings.append(f"all HMM fits failed for k={k}")
            continue
        models[str(k)] = best
    return {"hmmlearn_available": True, "models": models, "candidate_fit_diagnostics": fit_diags}, warnings


def _state_run_lengths(assignments: pd.Series) -> dict[str, list[int]]:
    if assignments.empty:
        return {}
    runs: dict[str, list[int]] = {}
    current_state = int(assignments.iloc[0])
    current_len = 1
    for value in assignments.iloc[1:]:
        if int(value) == current_state:
            current_len += 1
            continue
        runs.setdefault(str(current_state), []).append(current_len)
        current_state = int(value)
        current_len = 1
    runs.setdefault(str(current_state), []).append(current_len)
    return runs


def summarize_state_diagnostics(features_raw: pd.DataFrame, features_std: pd.DataFrame, assignments_frame: pd.DataFrame, model: Any, feature_columns: list[str], warnings: list[str]) -> dict[str, Any]:
    enriched_raw = features_raw.assign(assigned_state=assignments_frame["assigned_state"])
    enriched_std = features_std.assign(assigned_state=assignments_frame["assigned_state"])
    transition_counts = (
        assignments_frame.assign(next_state=assignments_frame["assigned_state"].shift(-1))
        .dropna(subset=["next_state"])
        .groupby(["assigned_state", "next_state"])
        .size()
        .to_dict()
    )
    return {
        "transition_matrix": model.transmat_.tolist(),
        "state_occupancy_overall": assignments_frame["assigned_state"].value_counts(normalize=True).sort_index().to_dict(),
        "state_occupancy_by_split": {
            split: group["assigned_state"].value_counts(normalize=True).sort_index().to_dict()
            for split, group in assignments_frame.groupby("split")
        },
        "mean_raw_feature_values_by_state": enriched_raw.groupby("assigned_state")[feature_columns].mean().to_dict(orient="index"),
        "mean_standardized_feature_values_by_state": enriched_std.groupby("assigned_state")[feature_columns].mean().to_dict(orient="index"),
        "forward_abs_return_5m_by_state": enriched_raw.groupby("assigned_state")["forward_abs_return_5m"].mean().to_dict(),
        "forward_abs_return_10m_by_state": enriched_raw.groupby("assigned_state")["forward_abs_return_10m"].mean().to_dict(),
        "forward_direction_distribution_by_state": {
            str(state): group["binance_label"].value_counts(normalize=True).to_dict()
            for state, group in assignments_frame.groupby("assigned_state")
        },
        "continuation_reversal_tendency_by_state": {
            str(state): group["continuation_reversal_10m"].value_counts(normalize=True).to_dict()
            for state, group in enriched_raw.groupby("assigned_state")
        },
        "tiny_move_rate_by_state": assignments_frame.groupby("assigned_state")["tiny_move_near_boundary"].mean().to_dict(),
        "state_run_length_distribution": _state_run_lengths(assignments_frame["assigned_state"]),
        "state_transition_counts": {f"{int(src)}->{int(dst)}": int(count) for (src, dst), count in transition_counts.items()},
        "warnings": warnings,
    }


def build_warnings(features: pd.DataFrame, dropped_rows: dict[str, int], leakage_warnings: list[str], diagnostics: dict[str, Any]) -> list[str]:
    warnings: list[str] = []
    if len(features) < 500:
        warnings.append("too few usable rows after feature construction")
    if leakage_warnings:
        warnings.append(f"feature leakage detected for {len(leakage_warnings)} rows")
    missing = features[FEATURE_COLUMNS].isna().mean() if not features.empty else pd.Series(dtype=float)
    high_missing = missing[missing > 0.1]
    if not high_missing.empty:
        warnings.append(f"high missingness in features: {', '.join(high_missing.index.tolist())}")
    if dropped_rows:
        warnings.append(f"dropped rows: {dropped_rows}")
    if diagnostics["selected_event_rows"] < diagnostics["input_event_rows"]:
        warnings.append("event set was bounded before feature construction")
    if diagnostics["hmm_fit_rows"] < diagnostics["feature_rows_emitted"]:
        warnings.append("HMM fitting used a bounded subset of feature rows")
    return warnings


def state_assignments_frame(features: pd.DataFrame, assignments: np.ndarray, posterior_max: np.ndarray) -> pd.DataFrame:
    frame = features[
        [
            "event_id",
            "event_start_time",
            "event_end_time",
            "split",
            "binance_label",
            "binance_move",
            "abs_binance_move",
            "tiny_move_near_boundary",
            "forward_abs_return_5m",
            "forward_abs_return_10m",
        ]
    ].copy()
    frame["assigned_state"] = assignments
    frame["state_posterior_max"] = posterior_max
    return frame


def _remove_stale_assignment_files(output_dir: Path) -> None:
    if not output_dir.exists():
        return
    for path in output_dir.glob("hmm_state_assignments_k*.csv"):
        path.unlink()


def write_outputs(output_dir: Path, features_raw: pd.DataFrame, features_std: pd.DataFrame, diagnostics: dict[str, Any], hmm_results: dict[str, Any]) -> dict[str, str]:
    output_dir.mkdir(parents=True, exist_ok=True)
    _remove_stale_assignment_files(output_dir)
    raw_path = output_dir / "hmm_features_raw.csv"
    std_path = output_dir / "hmm_features_standardized.csv"
    diagnostics_path = output_dir / "hmm_diagnostics.json"
    readme_path = output_dir / "hmm_readme_summary.txt"
    features_raw.to_csv(raw_path, index=False)
    features_std.to_csv(std_path, index=False)
    diagnostics_path.write_text(json.dumps(diagnostics, indent=2, default=str), encoding="utf-8")
    readme_lines = [
        f"input_event_rows={diagnostics['input_event_rows']}",
        f"selected_event_rows={diagnostics['selected_event_rows']}",
        f"feature_rows_emitted={diagnostics['feature_rows_emitted']}",
        f"feature_construction_seconds={diagnostics['feature_construction_seconds']}",
        f"hmm_fit_rows={diagnostics['hmm_fit_rows']}",
        f"hmmlearn_available={diagnostics['hmmlearn_available']}",
        "warnings:",
        *[f"- {warning}" for warning in diagnostics["warnings"]],
    ]
    for k, section in diagnostics.get("hmm_models", {}).items():
        readme_lines.append(f"state_occupancy_k{k}={section['state_occupancy_overall']}")
    readme_path.write_text("\n".join(readme_lines) + "\n", encoding="utf-8")
    paths = {
        "hmm_features_raw": str(raw_path),
        "hmm_features_standardized": str(std_path),
        "hmm_diagnostics": str(diagnostics_path),
        "hmm_readme_summary": str(readme_path),
    }
    for k, payload in hmm_results["models"].items():
        assignment_path = output_dir / f"hmm_state_assignments_k{k}.csv"
        payload["assignments_frame"].to_csv(assignment_path, index=False)
        paths[f"hmm_state_assignments_k{k}"] = str(assignment_path)
    return paths


def run_research(
    *,
    event_table_path: Path,
    input_roots: list[Path],
    output_dir: Path,
    shock_age_cap_minutes: float = SHOCK_AGE_CAP_MINUTES,
    ks: list[int] | None = None,
    seeds: list[int] | None = None,
    hmm_feature_clip_abs: float = HMM_FEATURE_CLIP_ABS,
    start_date: str | None = None,
    end_date: str | None = None,
    max_events: int | None = None,
    tail_events: int | None = None,
    fit_max_rows: int | None = None,
    fit_tail_rows: int | None = DEFAULT_FIT_TAIL_ROWS,
    entropy_mode: str = "fast",
) -> dict[str, Any]:
    all_events = load_event_set(event_table_path)
    selected_events = filter_events(
        all_events,
        start_date=start_date,
        end_date=end_date,
        max_events=max_events,
        tail_events=tail_events,
    )
    prices = load_binance_1m_klines(input_roots).frame
    feature_start = time.perf_counter()
    features_raw, dropped_rows, leakage_warnings = build_feature_matrix(
        selected_events,
        prices,
        shock_age_cap_minutes=shock_age_cap_minutes,
        entropy_mode=entropy_mode,
    )
    feature_construction_seconds = float(time.perf_counter() - feature_start)
    if features_raw.empty:
        raise RuntimeError("No usable feature rows emitted.")
    missing_feature_counts = features_raw[FEATURE_COLUMNS + ["shock_age_minutes"]].isna().sum().to_dict()
    features_std, scaler_params = standardize_features(features_raw, FEATURE_COLUMNS)
    clipped_std, clipped_counts = clip_standardized_features(features_std, FEATURE_COLUMNS, hmm_feature_clip_abs)
    hmm_raw_all, hmm_nan_counts, hmm_dropped = prepare_hmm_matrix(features_raw, FEATURE_COLUMNS)
    hmm_std_all, _, _ = prepare_hmm_matrix(clipped_std, FEATURE_COLUMNS)
    hmm_raw, fit_subset_diag = select_hmm_fit_rows(hmm_raw_all, fit_max_rows=fit_max_rows, fit_tail_rows=fit_tail_rows)
    hmm_std = hmm_std_all.loc[hmm_raw.index].reset_index(drop=True)
    fit_start = time.perf_counter()
    hmm_results, hmm_warnings = try_fit_hmms(
        hmm_std,
        ks=ks or DEFAULT_HMM_STATE_COUNTS,
        feature_columns=FEATURE_COLUMNS,
        seeds=seeds or DEFAULT_HMM_SEEDS,
    )
    hmm_fit_seconds = float(time.perf_counter() - fit_start)
    diagnostics: dict[str, Any] = {
        "input_event_rows": int(len(all_events)),
        "selected_event_rows": int(len(selected_events)),
        "selected_start_time": None if selected_events.empty else selected_events["event_start_time"].min().isoformat(),
        "selected_end_time": None if selected_events.empty else selected_events["event_start_time"].max().isoformat(),
        "feature_rows_emitted": int(len(features_raw)),
        "feature_construction_seconds": feature_construction_seconds,
        "rows_dropped_by_reason": dropped_rows,
        "hmmlearn_available": bool(hmm_results["hmmlearn_available"]),
        "selected_hmm_feature_columns": FEATURE_COLUMNS,
        "candidate_k_values": ks or DEFAULT_HMM_STATE_COUNTS,
        "candidate_seeds": seeds or DEFAULT_HMM_SEEDS,
        "shock_age_cap_minutes": shock_age_cap_minutes,
        "missing_feature_counts_before_encoding": missing_feature_counts,
        "hmm_feature_nan_counts_after_encoding": hmm_nan_counts,
        "hmm_feature_clip_abs": hmm_feature_clip_abs,
        "clipped_value_counts_by_feature": clipped_counts,
        "scaler_params": scaler_params,
        "entropy_mode": entropy_mode,
        "fit_subset_mode": fit_subset_diag["fit_subset_mode"],
        "hmm_fit_rows": fit_subset_diag["hmm_fit_rows"],
        "hmm_fit_start_time": fit_subset_diag["hmm_fit_start_time"],
        "hmm_fit_end_time": fit_subset_diag["hmm_fit_end_time"],
        "hmm_fit_seconds": hmm_fit_seconds,
        "warnings": [],
        "candidate_fit_diagnostics": hmm_results.get("candidate_fit_diagnostics", {}),
        "hmm_models": {},
    }
    warnings = build_warnings(features_raw, dropped_rows, leakage_warnings, diagnostics)
    warnings.extend(hmm_warnings)
    if hmm_dropped:
        warnings.append(f"dropped {hmm_dropped} rows for nonfinite HMM features")
    if entropy_mode == "fast":
        warnings.append("entropy_mode=fast uses rolling sign-state entropy, not exact transition entropy")
    diagnostics["warnings"] = warnings
    if hmm_results["hmmlearn_available"]:
        for k, payload in hmm_results["models"].items():
            assignments_frame = state_assignments_frame(hmm_raw, payload["assignments"], payload["posterior_max"])
            payload["assignments_frame"] = assignments_frame
            state_diag = summarize_state_diagnostics(hmm_raw, hmm_std, assignments_frame, payload["model"], FEATURE_COLUMNS, warnings)
            state_diag["selected_seed"] = payload["seed"]
            state_diag["converged"] = payload["converged"]
            state_diag["final_log_likelihood"] = payload["final_log_likelihood"]
            state_diag["n_iter"] = payload["n_iter"]
            state_diag["min_state_occupancy"] = payload["min_state_occupancy"]
            if payload["min_state_occupancy"] < 0.05:
                diagnostics["warnings"].append(f"HMM state occupancy below 5% for k={k}")
            diagnostics["hmm_models"][k] = state_diag
    paths = write_outputs(output_dir, features_raw, features_std, diagnostics, hmm_results)
    diagnostics["output_paths"] = paths
    (output_dir / "hmm_diagnostics.json").write_text(json.dumps(diagnostics, indent=2, default=str), encoding="utf-8")
    return diagnostics


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Offline Binance 1m BTC-5m regime research.")
    parser.add_argument("--event-table-path", type=Path, default=DEFAULT_OUTPUT_CSV)
    parser.add_argument("--input-root", type=Path, action="append", default=None)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--shock-age-cap-minutes", type=float, default=SHOCK_AGE_CAP_MINUTES)
    parser.add_argument("--k", dest="ks", type=int, nargs="+", default=DEFAULT_HMM_STATE_COUNTS)
    parser.add_argument("--seed", dest="seeds", type=int, nargs="+", default=DEFAULT_HMM_SEEDS)
    parser.add_argument("--hmm-feature-clip-abs", type=float, default=HMM_FEATURE_CLIP_ABS)
    parser.add_argument("--start-date", type=str, default=None)
    parser.add_argument("--end-date", type=str, default=None)
    parser.add_argument("--max-events", type=int, default=None)
    parser.add_argument("--tail-events", type=int, default=None)
    parser.add_argument("--fit-max-rows", type=int, default=None)
    parser.add_argument("--fit-tail-rows", type=int, default=DEFAULT_FIT_TAIL_ROWS)
    parser.add_argument("--entropy-mode", choices=sorted(ENTROPY_MODES), default="fast")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    diagnostics = run_research(
        event_table_path=args.event_table_path,
        input_roots=args.input_root or DEFAULT_INPUT_ROOTS,
        output_dir=args.output_dir,
        shock_age_cap_minutes=args.shock_age_cap_minutes,
        ks=args.ks,
        seeds=args.seeds,
        hmm_feature_clip_abs=args.hmm_feature_clip_abs,
        start_date=args.start_date,
        end_date=args.end_date,
        max_events=args.max_events,
        tail_events=args.tail_events,
        fit_max_rows=args.fit_max_rows,
        fit_tail_rows=args.fit_tail_rows,
        entropy_mode=args.entropy_mode,
    )
    print(
        json.dumps(
            {
                "selected_event_rows": diagnostics["selected_event_rows"],
                "feature_rows_emitted": diagnostics["feature_rows_emitted"],
                "feature_construction_seconds": diagnostics["feature_construction_seconds"],
                "hmm_fit_rows": diagnostics["hmm_fit_rows"],
                "hmmlearn_available": diagnostics["hmmlearn_available"],
                "warnings": diagnostics["warnings"],
                "output_paths": diagnostics["output_paths"],
            },
            indent=2,
            default=str,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
