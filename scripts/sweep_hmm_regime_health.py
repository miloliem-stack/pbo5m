#!/usr/bin/env python3
from __future__ import annotations

import argparse
import difflib
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

DEFAULT_OUTPUT_DIR = Path("artifacts/hmm_regime_health_sweep")
DEFAULT_STATE_COUNTS = [2, 3, 4, 5, 6, 7, 8]
DEFAULT_FAMILIES = ["gaussian_hmm", "sticky_gaussian_hmm"]
DEFAULT_FEATURE_SETS = ["core_1m", "laplace_1m"]
DEFAULT_SMOKE_STATE_COUNTS = [2]
DEFAULT_SMOKE_FAMILIES = ["gaussian_hmm"]
DEFAULT_SMOKE_FEATURE_SETS = ["core_1m"]
DEFAULT_CONFIDENCE_THRESHOLDS = [0.60, 0.70, 0.75, 0.80, 0.90]
DEFAULT_INPUT_CANDIDATES = [
    Path("data/binance-btc1m"),
    Path("data/binance/btcusdt_1m"),
    Path("data/binance/BTCUSDT_1m"),
    Path("data/binance"),
]
INPUT_PATH_ALIASES = {
    Path("data/binance/btcusdt_1m"): Path("data/binance-btc1m"),
    Path("data/binance/BTCUSDT_1m"): Path("data/binance-btc1m"),
}
SUPPORTED_FAMILIES = {"gaussian_hmm", "sticky_gaussian_hmm"}
SUPPORTED_FEATURE_SETS = {"core_1m", "laplace_1m"}
CANONICAL_FEATURES = ["realized_vol_30m", "shock_score", "drift_to_vol_15m", "sign_flip_rate_15m"]
STANDARD_BINANCE_COLUMNS = [
    "open_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "close_time",
    "quote_asset_volume",
    "number_of_trades",
    "taker_buy_base_asset_volume",
    "taker_buy_quote_asset_volume",
    "ignore",
]


def _safe_float(value: Any) -> float | None:
    try:
        result = float(value)
    except Exception:
        return None
    if not np.isfinite(result):
        return None
    return result


def parse_csv_list(value: str, *, item_type=str) -> list[Any]:
    items = [item.strip() for item in str(value).split(",") if item.strip()]
    if not items:
        raise ValueError("comma-separated list cannot be empty")
    return [item_type(item) for item in items]


def parse_state_counts(value: str) -> list[int]:
    counts = parse_csv_list(value, item_type=int)
    invalid = [count for count in counts if count < 2]
    if invalid:
        raise ValueError(f"state counts must be >=2: {invalid}")
    return counts


def parse_confidence_thresholds(value: str) -> list[float]:
    thresholds = parse_csv_list(value, item_type=float)
    invalid = [threshold for threshold in thresholds if threshold <= 0.0 or threshold >= 1.0]
    if invalid:
        raise ValueError(f"confidence thresholds must be between 0 and 1: {invalid}")
    return thresholds


def discover_input_files(path: Path) -> list[Path]:
    if not path.exists():
        alias = INPUT_PATH_ALIASES.get(path)
        if alias is not None and alias.exists():
            path = alias
        else:
            existing = []
            seen: set[Path] = set()
            for candidate in DEFAULT_INPUT_CANDIDATES:
                if candidate.exists() and candidate not in seen:
                    existing.append(candidate)
                    seen.add(candidate)
            search_root = path.parent if path.parent.exists() else Path("data")
            nearby = sorted(p for p in search_root.glob("*") if p.exists()) if search_root.exists() else []
            names = [str(p) for p in existing + nearby]
            matches = difflib.get_close_matches(str(path), names, n=3, cutoff=0.35)
            hint = f" Did you mean: {', '.join(matches)}?" if matches else ""
            raise FileNotFoundError(f"input path does not exist: {path}.{hint}")
    if path.is_file():
        if path.suffix.lower() not in {".csv", ".parquet"}:
            raise ValueError(f"unsupported input file type: {path.suffix}; expected .csv or .parquet")
        return [path]
    files = [
        child
        for child in sorted(path.rglob("*"))
        if child.is_file() and child.suffix.lower() in {".csv", ".parquet"} and "BTCUSDT" in child.name.upper()
    ]
    if not files:
        raise ValueError(f"no BTCUSDT CSV/Parquet files found under input directory: {path}")
    return files


def _infer_csv_has_header(path: Path) -> bool:
    first_line = path.read_text(encoding="utf-8", errors="ignore").splitlines()[0]
    return any(ch.isalpha() for ch in first_line)


def _coerce_timestamp(series: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    if numeric.notna().any():
        sample = float(numeric.dropna().abs().iloc[0])
        if sample >= 1e15:
            return pd.to_datetime(numeric, unit="us", utc=True, errors="coerce")
        if sample > 1e12:
            return pd.to_datetime(numeric, unit="ms", utc=True, errors="coerce")
        if sample > 1e9:
            return pd.to_datetime(numeric, unit="s", utc=True, errors="coerce")
    return pd.to_datetime(series, utc=True, errors="coerce")


def _load_one_price_file(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".parquet":
        raw = pd.read_parquet(path)
    else:
        raw = pd.read_csv(path) if _infer_csv_has_header(path) else pd.read_csv(path, header=None)
    if list(raw.columns) == list(range(len(raw.columns))):
        raw = raw.rename(columns={idx: name for idx, name in enumerate(STANDARD_BINANCE_COLUMNS[: len(raw.columns)])})

    timestamp_candidates = [
        "timestamp",
        "event_time",
        "open_time",
        "close_time",
        "time",
        "date",
        "datetime",
        "source_time",
    ]
    price_candidates = ["close", "price", "last_price", "mid", "mark_price", "weighted_average_price"]
    timestamp_column = next((column for column in timestamp_candidates if column in raw.columns), None)
    price_column = next((column for column in price_candidates if column in raw.columns), None)
    if timestamp_column is None:
        raise ValueError(f"No supported timestamp column found. Tried: {timestamp_candidates}")
    if price_column is None:
        raise ValueError(f"No supported close/price column found. Tried: {price_candidates}")

    frame = raw[[timestamp_column, price_column]].copy()
    frame.columns = ["timestamp", "close"]
    frame["timestamp"] = _coerce_timestamp(frame["timestamp"])
    frame["close"] = pd.to_numeric(frame["close"], errors="coerce")
    return frame


def load_price_frame(path: Path, max_rows: int | None = None) -> pd.DataFrame:
    files = discover_input_files(path)
    frames: list[pd.DataFrame] = []
    rows_loaded = 0
    for file_path in files:
        frame = _load_one_price_file(file_path)
        rows_loaded += len(frame)
        frames.append(frame)
        if max_rows is not None and rows_loaded >= max_rows:
            break
    frame = pd.concat(frames, ignore_index=True)
    if max_rows is not None:
        frame = frame.head(int(max_rows))
    frame = frame.dropna(subset=["timestamp", "close"])
    frame = frame[frame["close"] > 0.0]
    frame = frame.sort_values("timestamp").drop_duplicates("timestamp", keep="last").reset_index(drop=True)
    if frame.empty:
        raise ValueError("input has no usable timestamp/close rows")
    if not frame["timestamp"].is_monotonic_increasing:
        raise ValueError("timestamps are not monotonic after sorting")
    return frame


def _rolling_sign_flip_rate(log_returns: pd.Series, window: int) -> pd.Series:
    signs = np.sign(log_returns.fillna(0.0))
    previous = signs.shift(1)
    valid = ((signs != 0) & (previous != 0)).astype(float)
    flips = ((signs != previous) & (valid == 1.0)).astype(float)
    valid_count = valid.rolling(window=window, min_periods=window).sum()
    flip_count = flips.rolling(window=window, min_periods=window).sum()
    return flip_count / valid_count.replace(0.0, np.nan)


def _ewm_alpha_from_halflife(minutes: float) -> float:
    return float(1.0 - math.exp(math.log(0.5) / minutes))


def build_features(prices: pd.DataFrame, feature_set: str) -> tuple[pd.DataFrame, dict[str, Any]]:
    if feature_set not in SUPPORTED_FEATURE_SETS:
        raise ValueError(f"unsupported feature set {feature_set!r}")
    ordered = prices.sort_values("timestamp").reset_index(drop=True).copy()
    before_rows = len(ordered)
    ordered["log_close"] = np.log(ordered["close"])
    ordered["log_return_1m"] = ordered["log_close"].diff()
    for lag in (1, 2, 3, 5):
        ordered[f"r_lag_{lag}m"] = ordered["log_close"] - ordered["log_close"].shift(lag)
    abs_return = ordered["log_return_1m"].abs()
    for window in (5, 15, 30, 60):
        ordered[f"realized_vol_{window}m"] = ordered["log_return_1m"].rolling(window=window, min_periods=window).std(ddof=0)
        ordered[f"rolling_abs_return_{window}m"] = abs_return.rolling(window=window, min_periods=window).mean()
        ordered[f"signed_return_{window}m"] = ordered["log_return_1m"].rolling(window=window, min_periods=window).sum()
    for window in (5, 15, 30):
        ordered[f"sign_flip_rate_{window}m"] = _rolling_sign_flip_rate(ordered["log_return_1m"], window)
        ordered[f"drift_to_vol_{window}m"] = ordered[f"signed_return_{window}m"] / ordered[f"realized_vol_{window}m"].clip(lower=1e-12)
    ordered["shock_score"] = abs_return / ordered["realized_vol_30m"].clip(lower=1e-12)

    feature_columns = [
        "log_return_1m",
        "r_lag_1m",
        "r_lag_2m",
        "r_lag_3m",
        "r_lag_5m",
        "realized_vol_5m",
        "realized_vol_15m",
        "realized_vol_30m",
        "realized_vol_60m",
        "rolling_abs_return_5m",
        "rolling_abs_return_15m",
        "rolling_abs_return_30m",
        "rolling_abs_return_60m",
        "signed_return_5m",
        "signed_return_15m",
        "signed_return_30m",
        "signed_return_60m",
        "sign_flip_rate_5m",
        "sign_flip_rate_15m",
        "sign_flip_rate_30m",
        "drift_to_vol_5m",
        "drift_to_vol_15m",
        "drift_to_vol_30m",
        "shock_score",
    ]
    if feature_set == "laplace_1m":
        for half_life in (3, 10, 30, 60):
            alpha = _ewm_alpha_from_halflife(half_life)
            suffix = f"{half_life}m"
            ordered[f"ew_mean_return_hl_{suffix}"] = ordered["log_return_1m"].ewm(alpha=alpha, adjust=False).mean()
            ordered[f"ew_abs_return_hl_{suffix}"] = abs_return.ewm(alpha=alpha, adjust=False).mean()
            ordered[f"ew_squared_return_hl_{suffix}"] = (ordered["log_return_1m"] ** 2).ewm(alpha=alpha, adjust=False).mean()
            ordered[f"ew_sign_imbalance_hl_{suffix}"] = np.sign(ordered["log_return_1m"]).ewm(alpha=alpha, adjust=False).mean()
            ordered[f"ew_shock_intensity_hl_{suffix}"] = ordered["shock_score"].fillna(0.0).ewm(alpha=alpha, adjust=False).mean()
            feature_columns.extend(
                [
                    f"ew_mean_return_hl_{suffix}",
                    f"ew_abs_return_hl_{suffix}",
                    f"ew_squared_return_hl_{suffix}",
                    f"ew_sign_imbalance_hl_{suffix}",
                    f"ew_shock_intensity_hl_{suffix}",
                ]
            )

    nan_counts = ordered[feature_columns].isna().sum().astype(int).to_dict()
    mask = np.isfinite(ordered[feature_columns].astype(float).to_numpy()).all(axis=1)
    features = ordered.loc[mask, ["timestamp", "close"] + feature_columns].reset_index(drop=True)
    stats = {
        column: {
            "mean": _safe_float(features[column].mean()) if not features.empty else None,
            "std": _safe_float(features[column].std(ddof=0)) if not features.empty else None,
            "min": _safe_float(features[column].min()) if not features.empty else None,
            "max": _safe_float(features[column].max()) if not features.empty else None,
        }
        for column in feature_columns
    }
    manifest = {
        "feature_set": feature_set,
        "columns": feature_columns,
        "input_rows": int(before_rows),
        "rows": int(len(features)),
        "first_timestamp": None if features.empty else features["timestamp"].iloc[0].isoformat(),
        "last_timestamp": None if features.empty else features["timestamp"].iloc[-1].isoformat(),
        "nan_counts_before_drop": nan_counts,
        "rows_dropped_for_nan_or_nonfinite": int(before_rows - len(features)),
        "feature_stats_after_drop": stats,
        "causality": "strictly trailing rolling windows and adjust=False EW summaries; no centered/future windows",
    }
    return features, manifest


def standardize_train_test(train: pd.DataFrame, test: pd.DataFrame, columns: list[str]) -> tuple[np.ndarray, np.ndarray, dict[str, dict[str, float]]]:
    params: dict[str, dict[str, float]] = {}
    x_train = train[columns].astype(float)
    x_test = test[columns].astype(float)
    means = x_train.mean(axis=0)
    stds = x_train.std(axis=0, ddof=0).replace(0.0, 1.0).fillna(1.0)
    standardized_train = (x_train - means) / stds
    standardized_test = (x_test - means) / stds
    for column in columns:
        params[column] = {"mean": float(means[column]), "std": float(stds[column])}
    return standardized_train.to_numpy(), standardized_test.to_numpy(), params


@dataclass(frozen=True)
class Fold:
    fold_id: int
    train_start: int
    train_end: int
    test_start: int
    test_end: int


def rows_from_days(days: float | None) -> int | None:
    if days is None:
        return None
    return int(round(float(days) * 24 * 60))


def resolve_window_rows(days: float | None, rows: int | None, default_rows: int, name: str) -> int:
    if days is not None and rows is not None:
        raise ValueError(f"use either --{name}-days or --{name}-rows, not both")
    resolved = rows_from_days(days) if days is not None else rows
    if resolved is None:
        resolved = default_rows
    if int(resolved) <= 0:
        raise ValueError(f"{name} window must be positive")
    return int(resolved)


def make_walk_forward_splits(n_rows: int, train_rows: int, test_rows: int, step_rows: int) -> list[Fold]:
    folds: list[Fold] = []
    start = 0
    fold_id = 0
    while start + train_rows + test_rows <= n_rows:
        folds.append(
            Fold(
                fold_id=fold_id,
                train_start=start,
                train_end=start + train_rows,
                test_start=start + train_rows,
                test_end=start + train_rows + test_rows,
            )
        )
        fold_id += 1
        start += step_rows
    return folds


def _logsumexp(values: np.ndarray) -> float:
    max_value = float(np.max(values))
    if not np.isfinite(max_value):
        return max_value
    return max_value + float(np.log(np.exp(values - max_value).sum()))


def filtered_probabilities(model: Any, x_test: np.ndarray) -> np.ndarray:
    log_likelihood = model._compute_log_likelihood(x_test)
    log_start = np.log(np.clip(np.asarray(model.startprob_, dtype=float), 1e-300, 1.0))
    log_trans = np.log(np.clip(np.asarray(model.transmat_, dtype=float), 1e-300, 1.0))
    log_alpha = log_start + log_likelihood[0]
    log_alpha = log_alpha - _logsumexp(log_alpha)
    rows = [np.exp(log_alpha)]
    for t in range(1, len(x_test)):
        next_alpha = np.empty_like(log_alpha)
        for state in range(len(log_alpha)):
            next_alpha[state] = _logsumexp(log_alpha + log_trans[:, state]) + log_likelihood[t, state]
        log_alpha = next_alpha - _logsumexp(next_alpha)
        rows.append(np.exp(log_alpha))
    return np.vstack(rows)


def fit_hmm(family: str, n_states: int, x_train: np.ndarray, random_seed: int) -> Any:
    if family not in SUPPORTED_FAMILIES:
        raise NotImplementedError(f"family {family!r} is not implemented")
    from hmmlearn.hmm import GaussianHMM

    kwargs: dict[str, Any] = {
        "n_components": n_states,
        "covariance_type": "diag",
        "n_iter": 200,
        "tol": 1e-3,
        "random_state": random_seed,
    }
    if family == "sticky_gaussian_hmm":
        transmat_prior = np.ones((n_states, n_states), dtype=float)
        np.fill_diagonal(transmat_prior, 25.0)
        kwargs["transmat_prior"] = transmat_prior
    model = GaussianHMM(**kwargs)
    model.fit(x_train)
    return model


def run_lengths(states: np.ndarray) -> list[tuple[int, int]]:
    if len(states) == 0:
        return []
    runs: list[tuple[int, int]] = []
    current = int(states[0])
    length = 1
    for value in states[1:]:
        value = int(value)
        if value == current:
            length += 1
        else:
            runs.append((current, length))
            current = value
            length = 1
    runs.append((current, length))
    return runs


def confidence_metrics(p_max: np.ndarray, thresholds: list[float]) -> dict[str, float | None]:
    result: dict[str, float | None] = {
        "mean_pmax": _safe_float(np.mean(p_max)) if len(p_max) else None,
        "median_pmax": _safe_float(np.median(p_max)) if len(p_max) else None,
    }
    for threshold in thresholds:
        key = f"coverage_pmax_ge_{threshold:.2f}".replace(".", "_")
        result[key] = _safe_float(np.mean(p_max >= threshold)) if len(p_max) else None
    return result


def run_length_metrics(states: np.ndarray) -> dict[str, float | None]:
    runs = run_lengths(states)
    lengths = np.asarray([length for _, length in runs], dtype=float)
    n = len(states)
    transitions = max(len(runs) - 1, 0)
    result: dict[str, float | None] = {
        "transitions_per_hour": _safe_float(transitions / (n / 60.0)) if n else None,
        "self_transition_rate_empirical": _safe_float(1.0 - transitions / max(n - 1, 1)) if n > 1 else None,
        "mean_run_length_minutes": _safe_float(np.mean(lengths)) if len(lengths) else None,
        "median_run_length_minutes": _safe_float(np.median(lengths)) if len(lengths) else None,
        "p10_run_length_minutes": _safe_float(np.percentile(lengths, 10)) if len(lengths) else None,
        "p90_run_length_minutes": _safe_float(np.percentile(lengths, 90)) if len(lengths) else None,
    }
    for horizon in (2, 5, 10, 15, 30, 60):
        if horizon in (2, 5):
            result[f"pct_runs_lt_{horizon}m"] = _safe_float(np.mean(lengths < horizon)) if len(lengths) else None
        result[f"pct_runs_gte_{horizon}m"] = _safe_float(np.mean(lengths >= horizon)) if len(lengths) else None
    for horizon in (5, 15):
        result[f"pct_time_in_runs_gte_{horizon}m"] = _safe_float(lengths[lengths >= horizon].sum() / n) if n else None
    return result


def occupancy_metrics(states: np.ndarray, n_states: int) -> dict[str, Any]:
    counts = np.bincount(states.astype(int), minlength=n_states).astype(float) if len(states) else np.zeros(n_states)
    shares = counts / counts.sum() if counts.sum() else np.zeros(n_states)
    positive = shares[shares > 0.0]
    entropy = float(-(positive * np.log(positive)).sum()) if len(positive) else 0.0
    return {
        "state_shares": {str(i): float(shares[i]) for i in range(n_states)},
        "largest_state_share": _safe_float(np.max(shares)) if len(shares) else None,
        "smallest_state_share": _safe_float(np.min(shares)) if len(shares) else None,
        "dead_state_count": int((shares < 0.01).sum()),
        "low_occupancy_state_count": int((shares < 0.03).sum()),
        "effective_n_states": _safe_float(math.exp(entropy)),
        "normalized_occupancy_entropy": _safe_float(entropy / math.log(n_states)) if n_states > 1 else None,
    }


def separability_metrics(x_std: np.ndarray, states: np.ndarray, n_states: int) -> tuple[dict[str, float | None], dict[str, dict[str, float | None]]]:
    centroids: list[np.ndarray | None] = []
    variances: list[np.ndarray | None] = []
    for state in range(n_states):
        group = x_std[states == state]
        centroids.append(group.mean(axis=0) if len(group) else None)
        variances.append(group.var(axis=0) + 1e-6 if len(group) else None)
    distances: list[float] = []
    smds: list[float] = []
    kls: list[float] = []
    pairwise: dict[str, dict[str, float | None]] = {}
    for i in range(n_states):
        for j in range(i + 1, n_states):
            key = f"{i}-{j}"
            if centroids[i] is None or centroids[j] is None:
                pairwise[key] = {"centroid_distance": None, "standardized_mean_difference": None, "symmetric_kl_diag": None}
                continue
            diff = centroids[i] - centroids[j]
            distance = float(np.linalg.norm(diff))
            pooled = np.sqrt((variances[i] + variances[j]) / 2.0)
            smd = float(np.mean(np.abs(diff) / pooled))
            kl_ij = 0.5 * np.sum((variances[i] / variances[j]) + (diff**2 / variances[j]) - 1.0 + np.log(variances[j] / variances[i]))
            kl_ji = 0.5 * np.sum((variances[j] / variances[i]) + (diff**2 / variances[i]) - 1.0 + np.log(variances[i] / variances[j]))
            skl = float((kl_ij + kl_ji) / 2.0)
            distances.append(distance)
            smds.append(smd)
            kls.append(skl)
            pairwise[key] = {"centroid_distance": distance, "standardized_mean_difference": smd, "symmetric_kl_diag": skl}
    summary = {
        "minimum_pairwise_separation": _safe_float(min(smds)) if smds else None,
        "mean_pairwise_separation": _safe_float(np.mean(smds)) if smds else None,
        "minimum_pairwise_centroid_distance": _safe_float(min(distances)) if distances else None,
        "mean_pairwise_centroid_distance": _safe_float(np.mean(distances)) if distances else None,
        "minimum_pairwise_symmetric_kl_diag": _safe_float(min(kls)) if kls else None,
        "mean_pairwise_symmetric_kl_diag": _safe_float(np.mean(kls)) if kls else None,
    }
    return summary, pairwise


def empirical_survival(states: np.ndarray, horizons: list[int]) -> dict[int, float | None]:
    result: dict[int, float | None] = {}
    for horizon in horizons:
        if len(states) <= horizon:
            result[horizon] = None
        else:
            result[horizon] = _safe_float(np.mean(states[:-horizon] == states[horizon:]))
    return result


def model_implied_survival(transmat: np.ndarray, horizons: list[int]) -> dict[int, dict[str, float]]:
    result: dict[int, dict[str, float]] = {}
    diag = np.diag(transmat)
    for horizon in horizons:
        result[horizon] = {str(i): float(diag[i] ** horizon) for i in range(len(diag))}
    return result


def canonical_order(signatures: pd.DataFrame) -> list[int]:
    available = [column for column in CANONICAL_FEATURES if column in signatures.columns]
    if not available:
        return sorted(int(index) for index in signatures.index)
    ordered = signatures.copy()
    for column in available:
        ordered[column] = pd.to_numeric(ordered[column], errors="coerce").fillna(0.0)
    ordered["_state"] = [int(index) for index in ordered.index]
    return ordered.sort_values(available + ["_state"], ascending=[True] * len(available) + [True])["_state"].tolist()


def rejection_flags(metrics: dict[str, Any], n_states: int, min_confidence: float) -> list[str]:
    flags: list[str] = []
    if (metrics.get("median_run_length_minutes") or 0.0) < 5.0 or (metrics.get("transitions_per_hour") or 0.0) > 12.0:
        flags.append("REJECT_FLICKER")
    if (metrics.get("largest_confident_state_share") or 0.0) > 0.85:
        flags.append("REJECT_COLLAPSE")
    if (metrics.get("confident_low_occupancy_state_count") or 0) > 1:
        flags.append("REJECT_DEAD_STATES")
    if (metrics.get("coverage_pmax_ge_0_75") or 0.0) < min_confidence:
        flags.append("REJECT_LOW_CONFIDENCE")
    if (metrics.get("minimum_pairwise_separation") or 0.0) < 0.25:
        flags.append("REJECT_LOW_SEPARATION")
    if n_states >= 7 and not ((metrics.get("median_run_length_minutes") or 0.0) >= 10.0 and (metrics.get("minimum_pairwise_separation") or 0.0) >= 0.5):
        flags.append("WARN_COMPLEXITY")
    return flags


def state_count_role(n_states: int) -> str:
    if n_states == 2:
        return "sanity_baseline"
    if n_states in (3, 4):
        return "conservative_baseline"
    if n_states in (5, 6):
        return "primary_candidate_zone"
    if n_states in (7, 8):
        return "overfit_churn_stress_test"
    return "custom"


def quality_score(row: dict[str, Any], n_states: int) -> dict[str, float]:
    confidence = float(row.get("coverage_pmax_ge_0_75") or 0.0)
    persistence = min(float(row.get("median_run_length_minutes") or 0.0) / 30.0, 1.0)
    occupancy = float(row.get("normalized_occupancy_entropy") or 0.0)
    separability = min(float(row.get("mean_pairwise_separation") or 0.0) / 1.0, 1.0)
    stability = float(row.get("fold_stability_score") or 0.0)
    flicker_penalty = max(0.0, 5.0 - float(row.get("median_run_length_minutes") or 0.0)) / 5.0
    dead_penalty = min(float(row.get("dead_state_count") or 0.0) / max(n_states, 1), 1.0)
    collapse_penalty = max(0.0, float(row.get("largest_state_share") or 0.0) - 0.75) / 0.25
    complexity_penalty = 0.08 * max(n_states - 6, 0)
    score = confidence + persistence + occupancy + separability + stability - flicker_penalty - dead_penalty - collapse_penalty - complexity_penalty
    return {
        "confidence_score": confidence,
        "persistence_score": persistence,
        "occupancy_score": occupancy,
        "separability_score": separability,
        "fold_stability_score": stability,
        "flicker_penalty": flicker_penalty,
        "dead_state_penalty": dead_penalty,
        "single_state_collapse_penalty": collapse_penalty,
        "overcomplexity_penalty": complexity_penalty,
        "regime_health_score": score,
    }


def aggregate_stability(fold_rows: list[dict[str, Any]], signature_rows: list[dict[str, Any]], n_states: int) -> dict[str, float | None]:
    if len(fold_rows) < 2:
        return {
            "occupancy_stability": None,
            "state_feature_signature_stability": None,
            "transition_rate_stability": None,
            "median_run_length_stability": None,
            "fold_stability_score": 0.5,
        }
    occ_vectors = []
    for row in fold_rows:
        shares = json.loads(row["state_shares_json"])
        occ_vectors.append(np.asarray([shares.get(str(i), 0.0) for i in range(n_states)], dtype=float))
    occ_std = float(np.mean(np.std(np.vstack(occ_vectors), axis=0)))
    transition_values = np.asarray([row["transitions_per_hour"] for row in fold_rows if row["transitions_per_hour"] is not None], dtype=float)
    median_run_values = np.asarray([row["median_run_length_minutes"] for row in fold_rows if row["median_run_length_minutes"] is not None], dtype=float)
    sig = pd.DataFrame(signature_rows)
    signature_stability = None
    if not sig.empty:
        pivot_cols = [column for column in CANONICAL_FEATURES if column in sig.columns]
        distances = []
        for state in range(n_states):
            state_sig = sig[sig["canonical_state"] == state]
            if len(state_sig) >= 2 and pivot_cols:
                distances.append(float(state_sig[pivot_cols].astype(float).std(ddof=0).mean()))
        if distances:
            signature_stability = 1.0 / (1.0 + float(np.mean(distances)))
    transition_stability = 1.0 / (1.0 + float(np.std(transition_values))) if len(transition_values) else None
    median_run_stability = 1.0 / (1.0 + float(np.std(median_run_values) / max(float(np.mean(median_run_values)), 1e-9))) if len(median_run_values) else None
    occupancy_stability = max(0.0, 1.0 - occ_std)
    components = [value for value in [occupancy_stability, signature_stability, transition_stability, median_run_stability] if value is not None]
    return {
        "occupancy_stability": _safe_float(occupancy_stability),
        "state_feature_signature_stability": _safe_float(signature_stability),
        "transition_rate_stability": _safe_float(transition_stability),
        "median_run_length_stability": _safe_float(median_run_stability),
        "fold_stability_score": _safe_float(np.mean(components)) if components else 0.0,
    }


def write_optional_parquet(frame: pd.DataFrame, path: Path) -> bool:
    try:
        frame.to_parquet(path, index=False)
        return True
    except Exception:
        return False


def summarize_text(summary: pd.DataFrame) -> str:
    lines = ["BTC-5m HMM regime-health sweep summary", ""]
    if summary.empty:
        return "\n".join(lines + ["No fitted model rows were produced. Check warnings and hmmlearn availability."])
    top = summary.sort_values("regime_health_score", ascending=False).head(10)
    lines.append("Top models by composite score:")
    for _, row in top.iterrows():
        lines.append(
            f"- {row['model_id']} score={row['regime_health_score']:.3f} "
            f"median_run={row.get('median_run_length_minutes')} "
            f"conf75={row.get('coverage_pmax_ge_0_75')} flags={row.get('rejection_flags', '')}"
        )
    primary = summary[summary["n_states"].isin([5, 6])].sort_values("regime_health_score", ascending=False).head(5)
    lines.extend(["", "Top 5-6 state candidates:"])
    lines.extend([f"- {row['model_id']} score={row['regime_health_score']:.3f} flags={row.get('rejection_flags', '')}" for _, row in primary.iterrows()] or ["- none"])
    for title, mask in [
        ("Rejected models and reasons", summary["rejection_flags"].astype(str).str.contains("REJECT")),
        ("High confidence but bad flicker", (summary["coverage_pmax_ge_0_75"] >= 0.75) & summary["rejection_flags"].astype(str).str.contains("REJECT_FLICKER")),
        ("Good persistence but state collapse", (summary["median_run_length_minutes"] >= 10.0) & summary["rejection_flags"].astype(str).str.contains("REJECT_COLLAPSE")),
        ("Balanced occupancy but weak separability", (summary["normalized_occupancy_entropy"] >= 0.75) & summary["rejection_flags"].astype(str).str.contains("REJECT_LOW_SEPARATION")),
    ]:
        lines.extend(["", f"{title}:"])
        subset = summary[mask].head(10)
        lines.extend([f"- {row['model_id']}: {row.get('rejection_flags', '')}" for _, row in subset.iterrows()] or ["- none"])
    return "\n".join(lines) + "\n"


def run_sweep(args: argparse.Namespace) -> dict[str, Any]:
    started = time.perf_counter()
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    state_counts = parse_state_counts(args.state_counts)
    families = parse_csv_list(args.families)
    feature_sets = parse_csv_list(args.feature_sets)
    thresholds = parse_confidence_thresholds(args.confidence_thresholds)
    unsupported_families = sorted(set(families) - SUPPORTED_FAMILIES)
    unsupported_feature_sets = sorted(set(feature_sets) - SUPPORTED_FEATURE_SETS)
    if unsupported_families:
        raise ValueError(f"unsupported families: {unsupported_families}")
    if unsupported_feature_sets:
        raise ValueError(f"unsupported feature sets: {unsupported_feature_sets}")
    train_rows = resolve_window_rows(args.train_days, args.train_rows, 240, "train")
    test_rows = resolve_window_rows(args.test_days, args.test_rows, 60, "test")
    step_rows = resolve_window_rows(args.step_days, args.step_rows, test_rows, "step")

    config = {
        "input": str(args.input),
        "output_dir": str(output_dir),
        "state_counts": state_counts,
        "families": families,
        "feature_sets": feature_sets,
        "train_rows": train_rows,
        "test_rows": test_rows,
        "step_rows": step_rows,
        "max_rows": args.max_rows,
        "random_seed": args.random_seed,
        "covariance_type": "diag",
        "min_confidence": args.min_confidence,
        "confidence_thresholds": thresholds,
        "filtered_probability_note": "Current-regime metrics use forward-filtered posteriors computed only from observations through each timestamp.",
        "hindsight_note": "This harness does not use smoothed hindsight probabilities in regime-health metrics.",
        "not_implemented_families": {
            "t_like_robust_hmm": "placeholder only; no dependency present",
            "ar_hmm_feature_expanded": "approximate by adding causal lagged returns in feature sets",
            "duration_aware_hsmm": "diagnostics approximated through run-length and survival metrics",
        },
    }
    (output_dir / "sweep_config.json").write_text(json.dumps(config, indent=2, default=str), encoding="utf-8")

    prices = load_price_frame(Path(args.input), max_rows=args.max_rows)
    feature_manifest: dict[str, Any] = {}
    fold_metrics: list[dict[str, Any]] = []
    occupancy_rows: list[dict[str, Any]] = []
    run_rows: list[dict[str, Any]] = []
    signature_rows: list[dict[str, Any]] = []
    survival_rows: list[dict[str, Any]] = []
    transition_matrices: dict[str, Any] = {}
    summary_rows: list[dict[str, Any]] = []
    warnings: list[str] = []

    try:
        from hmmlearn.hmm import GaussianHMM  # noqa: F401
        hmmlearn_available = True
    except Exception as exc:
        hmmlearn_available = False
        warnings.append(f"hmmlearn unavailable; fitting skipped: {exc}")

    for feature_set in feature_sets:
        features, manifest = build_features(prices, feature_set)
        feature_columns = manifest["columns"]
        feature_manifest[feature_set] = manifest
        feature_path = output_dir / f"features_{feature_set}.csv"
        features.to_csv(feature_path, index=False)
        folds = make_walk_forward_splits(len(features), train_rows, test_rows, step_rows)
        feature_manifest[feature_set]["fold_count"] = len(folds)
        if not folds:
            warnings.append(f"feature_set={feature_set} has no walk-forward folds for requested windows")
            continue
        if not hmmlearn_available:
            continue
        for family in families:
            for n_states in state_counts:
                model_id = f"{feature_set}__{family}__k{n_states}"
                model_fold_rows: list[dict[str, Any]] = []
                model_signature_rows: list[dict[str, Any]] = []
                model_survival_rows: list[dict[str, Any]] = []
                transition_matrices[model_id] = {}
                for fold in folds:
                    train = features.iloc[fold.train_start : fold.train_end].reset_index(drop=True)
                    test = features.iloc[fold.test_start : fold.test_end].reset_index(drop=True)
                    x_train, x_test, norm = standardize_train_test(train, test, feature_columns)
                    if len(x_train) <= n_states * 4 or len(x_test) == 0:
                        warnings.append(f"{model_id} fold={fold.fold_id} skipped for too few rows")
                        continue
                    try:
                        model = fit_hmm(family, n_states, x_train, args.random_seed + fold.fold_id)
                        filtered = filtered_probabilities(model, x_test)
                    except Exception as exc:
                        warnings.append(f"{model_id} fold={fold.fold_id} fit/filter failed: {exc}")
                        continue
                    states = filtered.argmax(axis=1).astype(int)
                    p_max = filtered.max(axis=1)
                    conf = confidence_metrics(p_max, thresholds)
                    runs = run_length_metrics(states)
                    occ = occupancy_metrics(states, n_states)
                    confident_mask = p_max >= args.min_confidence
                    confident_occ = occupancy_metrics(states[confident_mask], n_states)
                    sep, pairwise = separability_metrics(x_test, states, n_states)
                    signatures = pd.DataFrame(x_test, columns=feature_columns).assign(state=states).groupby("state")[feature_columns].agg(["mean", "median", "std"])
                    flat_signatures = pd.DataFrame(index=range(n_states))
                    for column in feature_columns:
                        if (column, "mean") in signatures:
                            flat_signatures[column] = signatures[(column, "mean")]
                    order = canonical_order(flat_signatures.fillna(0.0))
                    canonical = {state: i for i, state in enumerate(order)}
                    canonical_states = np.asarray([canonical.get(int(state), int(state)) for state in states], dtype=int)
                    model_survival_metrics = model_implied_survival(np.asarray(model.transmat_), [5, 10, 15, 30, 60])
                    emp_survival = empirical_survival(canonical_states, [5, 10, 15, 30, 60])
                    transition_matrices[model_id][str(fold.fold_id)] = {
                        "raw_state_order": np.asarray(model.transmat_).tolist(),
                        "canonical_state_order": order,
                        "normalization_stats": norm,
                        "pairwise_separation": pairwise,
                    }
                    row = {
                        "model_id": model_id,
                        "feature_set": feature_set,
                        "family": family,
                        "covariance_type": "diag",
                        "n_states": n_states,
                        "state_count_role": state_count_role(n_states),
                        "fold_id": fold.fold_id,
                        "train_start": train["timestamp"].iloc[0].isoformat(),
                        "train_end": train["timestamp"].iloc[-1].isoformat(),
                        "test_start": test["timestamp"].iloc[0].isoformat(),
                        "test_end": test["timestamp"].iloc[-1].isoformat(),
                        "test_rows": len(test),
                        "state_shares_json": json.dumps(occ["state_shares"], sort_keys=True),
                        "confident_state_shares_json": json.dumps(confident_occ["state_shares"], sort_keys=True),
                        "largest_confident_state_share": confident_occ["largest_state_share"],
                        "confident_low_occupancy_state_count": confident_occ["low_occupancy_state_count"],
                        **conf,
                        **runs,
                        **{key: value for key, value in occ.items() if key != "state_shares"},
                        **sep,
                    }
                    fold_metrics.append(row)
                    model_fold_rows.append(row)
                    run_rows.append({"model_id": model_id, "fold_id": fold.fold_id, **runs})
                    for state, share in occ["state_shares"].items():
                        occupancy_rows.append({"model_id": model_id, "fold_id": fold.fold_id, "state": int(state), "state_share": share, "confident_state_share": confident_occ["state_shares"][state]})
                    summary_by_state = test.assign(state=states).groupby("state")[feature_columns].agg(["mean", "median", "std"])
                    for raw_state in range(n_states):
                        sig_row = {"model_id": model_id, "fold_id": fold.fold_id, "raw_state": raw_state, "canonical_state": canonical.get(raw_state, raw_state)}
                        if raw_state in summary_by_state.index:
                            for column in feature_columns:
                                sig_row[f"{column}_mean"] = _safe_float(summary_by_state.loc[raw_state, (column, "mean")])
                                sig_row[f"{column}_median"] = _safe_float(summary_by_state.loc[raw_state, (column, "median")])
                                sig_row[f"{column}_std"] = _safe_float(summary_by_state.loc[raw_state, (column, "std")])
                                if column in CANONICAL_FEATURES:
                                    sig_row[column] = sig_row[f"{column}_mean"]
                        signature_rows.append(sig_row)
                        model_signature_rows.append(sig_row)
                    for horizon, empirical in emp_survival.items():
                        model_mean_survival = _safe_float(np.mean(list(model_survival_metrics[horizon].values())))
                        survival_row = {
                            "model_id": model_id,
                            "fold_id": fold.fold_id,
                            "horizon_minutes": horizon,
                            "empirical_same_state_survival": empirical,
                            "model_implied_mean_same_state_survival": model_mean_survival,
                            "empirical_minus_model_implied_survival": None
                            if empirical is None or model_mean_survival is None
                            else float(empirical - model_mean_survival),
                            "model_implied_survival_by_state_json": json.dumps(model_survival_metrics[horizon], sort_keys=True),
                        }
                        survival_rows.append(survival_row)
                        model_survival_rows.append(survival_row)
                if not model_fold_rows:
                    continue
                aggregate = pd.DataFrame(model_fold_rows).mean(numeric_only=True).to_dict()
                stability = aggregate_stability(model_fold_rows, model_signature_rows, n_states)
                summary = {
                    "model_id": model_id,
                    "feature_set": feature_set,
                    "family": family,
                    "covariance_type": "diag",
                    "n_states": n_states,
                    "state_count_role": state_count_role(n_states),
                    "fold_count": len(model_fold_rows),
                    **aggregate,
                    **stability,
                }
                summary.update(quality_score(summary, n_states))
                flags = rejection_flags(summary, n_states, args.min_confidence)
                summary["rejection_flags"] = ",".join(flags)
                summary_rows.append(summary)

    summary_df = pd.DataFrame(summary_rows)
    if not summary_df.empty:
        summary_df = summary_df.sort_values("regime_health_score", ascending=False)
    fold_df = pd.DataFrame(fold_metrics)
    occupancy_df = pd.DataFrame(occupancy_rows)
    run_df = pd.DataFrame(run_rows)
    signature_df = pd.DataFrame(signature_rows)
    survival_df = pd.DataFrame(survival_rows)
    summary_df.to_csv(output_dir / "regime_health_summary.csv", index=False)
    write_optional_parquet(summary_df, output_dir / "regime_health_summary.parquet")
    fold_df.to_csv(output_dir / "fold_metrics.csv", index=False)
    occupancy_df.to_csv(output_dir / "state_occupancy.csv", index=False)
    run_df.to_csv(output_dir / "run_length_metrics.csv", index=False)
    signature_df.to_csv(output_dir / "state_feature_signatures.csv", index=False)
    survival_df.to_csv(output_dir / "survival_metrics.csv", index=False)
    (output_dir / "transition_matrices.json").write_text(json.dumps(transition_matrices, indent=2, default=str), encoding="utf-8")
    (output_dir / "feature_manifest.json").write_text(json.dumps(feature_manifest, indent=2, default=str), encoding="utf-8")
    (output_dir / "regime_health_readme_summary.txt").write_text(summarize_text(summary_df), encoding="utf-8")
    diagnostics = {
        "output_dir": str(output_dir),
        "hmmlearn_available": hmmlearn_available,
        "summary_rows": int(len(summary_df)),
        "fold_metric_rows": int(len(fold_df)),
        "warnings": warnings,
        "elapsed_seconds": float(time.perf_counter() - started),
    }
    (output_dir / "sweep_diagnostics.json").write_text(json.dumps(diagnostics, indent=2, default=str), encoding="utf-8")
    return diagnostics


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Offline-only BTC-5m HMM regime-health sweep harness.")
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--state-counts", default=",".join(map(str, DEFAULT_SMOKE_STATE_COUNTS)))
    parser.add_argument("--families", default=",".join(DEFAULT_SMOKE_FAMILIES))
    parser.add_argument("--feature-sets", default=",".join(DEFAULT_SMOKE_FEATURE_SETS))
    parser.add_argument("--train-days", type=float)
    parser.add_argument("--train-rows", type=int)
    parser.add_argument("--test-days", type=float)
    parser.add_argument("--test-rows", type=int)
    parser.add_argument("--step-days", type=float)
    parser.add_argument("--step-rows", type=int)
    parser.add_argument("--max-rows", type=int)
    parser.add_argument("--random-seed", type=int, default=1)
    parser.add_argument("--min-confidence", type=float, default=0.75)
    parser.add_argument("--confidence-thresholds", default=",".join(f"{value:.2f}" for value in DEFAULT_CONFIDENCE_THRESHOLDS))
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    diagnostics = run_sweep(args)
    print(json.dumps(diagnostics, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
