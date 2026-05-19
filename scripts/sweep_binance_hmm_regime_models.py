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

from scripts.build_binance_btc5m_research_events import DEFAULT_INPUT_ROOTS, DEFAULT_OUTPUT_CSV, load_binance_1m_klines
from scripts.research_hmm_regime_binance_1m import (
    ENTROPY_MODES,
    FEATURE_COLUMNS as FULL_FEATURE_COLUMNS,
    HMM_FEATURE_CLIP_ABS,
    SHOCK_AGE_CAP_MINUTES,
    build_feature_matrix,
    clip_standardized_features,
    prepare_hmm_matrix,
    standardize_features,
)

DEFAULT_OUTPUT_DIR = Path("artifacts/binance_btc5m_research/hmm_sweep_v1")
DEFAULT_K_VALUES = [2, 3, 4, 5, 6, 7, 8]
DEFAULT_SEEDS = [1, 2, 3, 4, 5]
DEFAULT_COVARIANCE_TYPES = ["diag", "spherical"]
COVARIANCE_TYPES = {"diag", "spherical", "full"}
FEATURE_SETS = {"reduced", "full"}
REDUCED_HMM_FEATURE_COLUMNS = [
    "r_5m",
    "r_10m",
    "r_15m",
    "realized_vol_15m",
    "realized_vol_30m",
    "sign_flip_rate_15m",
    "sign_flip_rate_30m",
    "drift_to_vol_15m",
    "drift_to_vol_30m",
    "ew_return_tau_5m",
    "ew_abs_return_tau_5m",
    "ew_signed_imbalance_fast_minus_slow",
    "ew_abs_activity_fast_minus_slow",
    "price_transition_entropy_30m",
    "shock_score_5m",
    "has_recent_shock",
    "shock_age_minutes_capped",
]
SIGNED_PRESSURE_FEATURES = [
    "r_5m",
    "r_10m",
    "r_15m",
    "drift_to_vol_15m",
    "drift_to_vol_30m",
    "ew_return_tau_5m",
    "ew_signed_imbalance_fast_minus_slow",
]
EVENT_TIMESTAMP_COLUMNS = [
    "event_start_time",
    "event_start_utc",
    "timestamp_utc",
    "window_start_utc",
    "start_timestamp_utc",
    "market_start_time",
    "start_time",
    "source_start_ts",
]
PAIR_ORDER_WARNING = (
    "Quiet-regime persistence may identify candidate windows for passive pair-order research, "
    "but Binance-only data cannot prove pair-arb profitability. Fillability, order priority, "
    "adverse selection, and orphan-leg risk require Polymarket quote/order replay."
)


def parse_int_list(value: str, *, name: str) -> list[int]:
    try:
        parsed = [int(part.strip()) for part in value.split(",") if part.strip()]
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"{name} must be a comma-separated integer list") from exc
    if not parsed:
        raise argparse.ArgumentTypeError(f"{name} must not be empty")
    return parsed


def parse_k_values(value: str) -> list[int]:
    values = parse_int_list(value, name="k-values")
    if any(k < 2 for k in values):
        raise argparse.ArgumentTypeError("k-values must all be >= 2")
    return values


def parse_seeds(value: str) -> list[int]:
    return parse_int_list(value, name="seeds")


def parse_covariance_types(value: str) -> list[str]:
    values = [part.strip() for part in value.split(",") if part.strip()]
    if not values:
        raise argparse.ArgumentTypeError("covariance-types must not be empty")
    unsupported = sorted(set(values) - COVARIANCE_TYPES)
    if unsupported:
        raise argparse.ArgumentTypeError(f"unsupported covariance type(s): {', '.join(unsupported)}")
    return values


def parse_utc_date(value: str | None) -> pd.Timestamp | None:
    if value is None:
        return None
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is None:
        return timestamp.tz_localize("UTC")
    return timestamp.tz_convert("UTC")


def detect_event_timestamp_column(events: pd.DataFrame) -> str:
    for column in EVENT_TIMESTAMP_COLUMNS:
        if column in events.columns:
            return column
    available = ", ".join(map(str, events.columns))
    supported = ", ".join(EVENT_TIMESTAMP_COLUMNS)
    raise ValueError(f"No supported event timestamp column found. Supported columns: {supported}. Available columns: {available}")


def load_event_set(path: Path) -> pd.DataFrame:
    events = pd.read_csv(path)
    timestamp_column = detect_event_timestamp_column(events)
    events[timestamp_column] = pd.to_datetime(events[timestamp_column], utc=True, errors="coerce")
    if events[timestamp_column].isna().all():
        raise ValueError(f"Timestamp column {timestamp_column!r} could not be parsed as UTC datetimes")
    if timestamp_column != "event_start_time":
        events["event_start_time"] = events[timestamp_column]
    for column in ("event_end_time", "source_start_ts", "source_end_ts"):
        if column in events.columns:
            events[column] = pd.to_datetime(events[column], utc=True, errors="coerce")
    if "tiny_move_near_boundary" in events.columns:
        events["tiny_move_near_boundary"] = events["tiny_move_near_boundary"].astype(bool)
    return events.sort_values("event_start_time").reset_index(drop=True)


def date_filter_metadata(events: pd.DataFrame, timestamp_column: str) -> dict[str, Any]:
    timestamps = pd.to_datetime(events[timestamp_column], utc=True, errors="coerce") if timestamp_column in events.columns else pd.Series(dtype="datetime64[ns, UTC]")
    label_counts: dict[str, int] = {}
    for label_column in ("binance_label", "label", "chainlink_label"):
        if label_column in events.columns:
            label_counts = {str(label): int(count) for label, count in events[label_column].value_counts(dropna=False).items()}
            break
    return {
        "filtered_min_timestamp": None if timestamps.dropna().empty else timestamps.min().isoformat(),
        "filtered_max_timestamp": None if timestamps.dropna().empty else timestamps.max().isoformat(),
        "label_counts_after_filtering": label_counts,
    }


def filter_events_for_sweep(
    events: pd.DataFrame,
    *,
    start_date: str | None,
    end_date: str | None,
    tail_events: int | None,
) -> tuple[pd.DataFrame, dict[str, Any], list[str]]:
    timestamp_column = detect_event_timestamp_column(events)
    input_rows = int(len(events))
    ordered = events.copy()
    ordered[timestamp_column] = pd.to_datetime(ordered[timestamp_column], utc=True, errors="coerce")
    if timestamp_column != "event_start_time":
        ordered["event_start_time"] = ordered[timestamp_column]
    ordered = ordered.dropna(subset=["event_start_time"]).sort_values("event_start_time").reset_index(drop=True)
    start_ts = parse_utc_date(start_date)
    end_ts = parse_utc_date(end_date)
    if start_ts is not None:
        ordered = ordered[ordered["event_start_time"] >= start_ts]
    if end_ts is not None:
        ordered = ordered[ordered["event_start_time"] < end_ts]
    date_filtered_rows = int(len(ordered))
    if tail_events is not None:
        ordered = ordered.tail(int(tail_events))
    selected = ordered.reset_index(drop=True)
    if selected.empty:
        raise RuntimeError(
            f"Date/tail filtering selected zero event rows "
            f"(start_date={start_date}, end_date={end_date}, tail_events={tail_events}, input_rows={input_rows}, date_filtered_rows={date_filtered_rows})"
        )
    warnings: list[str] = []
    if len(selected) < 100:
        warnings.append(f"filtered sample is very small: {len(selected)} event rows")
    metadata = {
        "event_timestamp_column": timestamp_column,
        "input_event_rows_before_filtering": input_rows,
        "event_rows_after_date_filtering": date_filtered_rows,
        "selected_event_rows_after_tail": int(len(selected)),
        "start_date": start_date,
        "end_date": end_date,
        "start_timestamp_utc": None if start_ts is None else start_ts.isoformat(),
        "end_timestamp_utc_exclusive": None if end_ts is None else end_ts.isoformat(),
        "tail_events": tail_events,
        **date_filter_metadata(selected, "event_start_time"),
    }
    return selected, metadata, warnings


def feature_columns_for_set(feature_set: str) -> list[str]:
    if feature_set == "reduced":
        return REDUCED_HMM_FEATURE_COLUMNS
    if feature_set == "full":
        return FULL_FEATURE_COLUMNS
    raise ValueError(f"Unsupported feature set: {feature_set}")


def _safe_float(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if np.isfinite(result) else None


def hmm_parameter_count(k: int, d: int, covariance_type: str) -> int:
    covariance_params = {
        "spherical": k,
        "diag": k * d,
        "full": int(k * d * (d + 1) / 2),
    }[covariance_type]
    return int((k - 1) + k * (k - 1) + k * d + covariance_params)


def approximate_information_criteria(log_likelihood: float | None, *, n_rows: int, k: int, n_features: int, covariance_type: str) -> tuple[float | None, float | None]:
    if log_likelihood is None or n_rows <= 0:
        return None, None
    n_params = hmm_parameter_count(k, n_features, covariance_type)
    aic = 2.0 * n_params - 2.0 * log_likelihood
    bic = math.log(n_rows) * n_params - 2.0 * log_likelihood
    return float(aic), float(bic)


def enrich_outcome_columns(features: pd.DataFrame) -> pd.DataFrame:
    enriched = features.sort_values("event_start_time").reset_index(drop=True).copy()
    enriched["current_abs_move"] = enriched["abs_binance_move"].astype(float)
    enriched["next_abs_move_5m"] = enriched["abs_binance_move"].shift(-1).astype(float)
    enriched["next_binance_label"] = enriched["binance_label"].shift(-1)
    enriched["next_binance_move"] = enriched["binance_move"].shift(-1).astype(float)
    enriched["tiny_move_near_boundary"] = _clean_binary_series(enriched["tiny_move_near_boundary"])
    enriched["next_tiny_move_near_boundary"] = _clean_binary_series(enriched["tiny_move_near_boundary"].shift(-1))
    if "forward_abs_return_10m" in enriched.columns:
        enriched["next_abs_move_10m"] = pd.to_numeric(enriched["forward_abs_return_10m"], errors="coerce")
    return enriched


def _clean_binary_series(series: pd.Series) -> pd.Series:
    mapped = series.map(
        lambda value: np.nan
        if pd.isna(value)
        else 1.0
        if value is True or str(value).strip().lower() in {"true", "1", "1.0", "yes"}
        else 0.0
        if value is False or str(value).strip().lower() in {"false", "0", "0.0", "no"}
        else np.nan
    )
    return pd.to_numeric(mapped, errors="coerce")


def state_run_length_summary(assignments: pd.Series | np.ndarray) -> dict[str, dict[str, float | int | None]]:
    series = pd.Series(assignments).dropna()
    if series.empty:
        return {}
    runs: dict[str, list[int]] = {}
    current_state = int(series.iloc[0])
    current_len = 1
    for value in series.iloc[1:]:
        state = int(value)
        if state == current_state:
            current_len += 1
            continue
        runs.setdefault(str(current_state), []).append(current_len)
        current_state = state
        current_len = 1
    runs.setdefault(str(current_state), []).append(current_len)
    return {
        state: {
            "run_count": int(len(lengths)),
            "run_length_mean": float(np.mean(lengths)),
            "run_length_median": float(np.median(lengths)),
            "run_length_p75": float(np.percentile(lengths, 75)),
            "run_length_p90": float(np.percentile(lengths, 90)),
            "run_length_p95": float(np.percentile(lengths, 95)),
            "run_length_max": int(np.max(lengths)),
            "p_run_length_ge_2": float(np.mean(np.asarray(lengths) >= 2)),
            "p_run_length_ge_3": float(np.mean(np.asarray(lengths) >= 3)),
            "p_run_length_ge_5": float(np.mean(np.asarray(lengths) >= 5)),
            "p_run_length_ge_10": float(np.mean(np.asarray(lengths) >= 10)),
            "mean": float(np.mean(lengths)),
            "median": float(np.median(lengths)),
            "p90": float(np.percentile(lengths, 90)),
            "max": int(np.max(lengths)),
        }
        for state, lengths in runs.items()
    }


def _chronological_runs(assignments: pd.Series | np.ndarray) -> list[tuple[int, int, int]]:
    series = pd.Series(assignments).dropna().astype(int).reset_index(drop=True)
    if series.empty:
        return []
    runs: list[tuple[int, int, int]] = []
    start = 0
    current_state = int(series.iloc[0])
    for index, value in enumerate(series.iloc[1:], start=1):
        state = int(value)
        if state == current_state:
            continue
        runs.append((current_state, start, index))
        current_state = state
        start = index
    runs.append((current_state, start, len(series)))
    return runs


def post_confirmation_diagnostics(assignment_frame: pd.DataFrame) -> dict[str, dict[str, Any]]:
    if assignment_frame.empty:
        return {}
    result: dict[str, dict[str, Any]] = {}
    for state, start, end in _chronological_runs(assignment_frame["assigned_state"]):
        state_key = str(state)
        payload = result.setdefault(
            state_key,
            {
                "_remaining_after_first": [],
                "_post_indices": [],
                "_exit_within": {1: [], 2: [], 3: [], 5: []},
            },
        )
        run_length = end - start
        payload["_remaining_after_first"].append(run_length - 1)
        for index in range(start + 1, end):
            payload["_post_indices"].append(index)
            remaining_after_current = end - index - 1
            for horizon in (1, 2, 3, 5):
                payload["_exit_within"][horizon].append(remaining_after_current < horizon)

    clean: dict[str, dict[str, Any]] = {}
    for state_key, payload in result.items():
        post_indices = payload["_post_indices"]
        post_rows = assignment_frame.iloc[post_indices] if post_indices else assignment_frame.iloc[0:0]
        remaining = np.asarray(payload["_remaining_after_first"], dtype=float)
        state_payload: dict[str, Any] = {
            "post_confirmation_rows": int(len(post_indices)),
            "remaining_after_first_mean": _safe_float(np.mean(remaining)) if len(remaining) else None,
            "remaining_after_first_median": _safe_float(np.median(remaining)) if len(remaining) else None,
            "p_remaining_after_first_ge_1": _safe_float(np.mean(remaining >= 1)) if len(remaining) else None,
            "p_remaining_after_first_ge_2": _safe_float(np.mean(remaining >= 2)) if len(remaining) else None,
            "p_remaining_after_first_ge_4": _safe_float(np.mean(remaining >= 4)) if len(remaining) else None,
            "p_remaining_after_first_ge_9": _safe_float(np.mean(remaining >= 9)) if len(remaining) else None,
            "post_confirmation_abs_move_mean": _safe_float(post_rows["current_abs_move"].mean()) if len(post_rows) else None,
            "post_confirmation_abs_move_median": _safe_float(post_rows["current_abs_move"].median()) if len(post_rows) else None,
            "post_confirmation_tiny_rate": _safe_float(post_rows["tiny_move_near_boundary"].astype(float).mean()) if len(post_rows) else None,
            "post_confirmation_label_distribution": post_rows["binance_label"].dropna().value_counts(normalize=True).to_dict() if len(post_rows) else {},
        }
        for horizon, values in payload["_exit_within"].items():
            state_payload[f"p_regime_exit_within_next_{horizon}_markets_after_confirmation"] = _safe_float(np.mean(values)) if values else None
        clean[state_key] = state_payload
    return clean


def duplicate_state_pairs(state_feature_means: pd.DataFrame, *, cosine_threshold: float = 0.98, distance_threshold: float = 0.35) -> list[dict[str, float | int]]:
    pairs: list[dict[str, float | int]] = []
    if len(state_feature_means) < 2:
        return pairs
    values = state_feature_means.astype(float).to_numpy()
    states = [int(state) for state in state_feature_means.index]
    for i in range(len(states)):
        for j in range(i + 1, len(states)):
            left = values[i]
            right = values[j]
            denom = float(np.linalg.norm(left) * np.linalg.norm(right))
            cosine = float(np.dot(left, right) / denom) if denom > 0.0 else 0.0
            distance = float(np.linalg.norm(left - right) / math.sqrt(len(left)))
            if cosine > cosine_threshold or distance < distance_threshold:
                pairs.append(
                    {
                        "state_a": states[i],
                        "state_b": states[j],
                        "cosine_similarity": cosine,
                        "standardized_distance": distance,
                    }
                )
    return pairs


def occupancy_by_split(assignments: pd.Series, splits: pd.Series, k: int) -> dict[str, dict[str, float]]:
    frame = pd.DataFrame({"assigned_state": assignments.astype(int), "split": splits.astype(str)})
    result: dict[str, dict[str, float]] = {}
    for split, group in frame.groupby("split", sort=False):
        counts = group["assigned_state"].value_counts(normalize=True).reindex(range(k), fill_value=0.0)
        result[str(split)] = {str(state): float(value) for state, value in counts.sort_index().items()}
    return result


def train_test_occupancy_shift(occupancy_split: dict[str, dict[str, float]], k: int) -> float:
    train = occupancy_split.get("train", {})
    test = occupancy_split.get("test", {})
    if not train or not test:
        return 0.0
    return float(max(abs(float(train.get(str(state), 0.0)) - float(test.get(str(state), 0.0))) for state in range(k)))


def build_model_warnings(
    *,
    state_occupancy: dict[str, float],
    occupancy_split: dict[str, dict[str, float]],
    duplicate_pairs: list[dict[str, Any]],
    k: int,
) -> list[str]:
    warnings: list[str] = []
    low_2 = [state for state, value in state_occupancy.items() if float(value) < 0.02]
    low_5 = [state for state, value in state_occupancy.items() if float(value) < 0.05]
    if low_2:
        warnings.append(f"state occupancy below 2%: {', '.join(low_2)}")
    if low_5:
        warnings.append(f"state occupancy below 5%: {', '.join(low_5)}")
    if duplicate_pairs:
        warnings.append(f"duplicate-like state pairs: {len(duplicate_pairs)}")
    if duplicate_pairs and len(duplicate_pairs) >= max(1, k // 3):
        warnings.append("higher k mostly creates duplicate states")
    for state in range(k):
        split_values = {split: float(values.get(str(state), 0.0)) for split, values in occupancy_split.items()}
        total = sum(split_values.values())
        if total > 0.0 and max(split_values.values()) / total > 0.85 and min(split_values.values()) < 0.01:
            warnings.append(f"state {state} exists almost entirely in one split")
    shift = train_test_occupancy_shift(occupancy_split, k)
    if shift > 0.20:
        warnings.append(f"heavy train/test occupancy shift: {shift:.3f}")
    return warnings


def _label_distribution_by_state(frame: pd.DataFrame, column: str) -> dict[str, dict[str, float]]:
    if column not in frame.columns:
        return {}
    result: dict[str, dict[str, float]] = {}
    for state, group in frame.groupby("assigned_state"):
        result[str(int(state))] = group[column].dropna().value_counts(normalize=True).to_dict()
    return result


def _mean_median_by_state(frame: pd.DataFrame, column: str) -> dict[str, dict[str, float | None]]:
    if column not in frame.columns:
        return {}
    grouped = frame.groupby("assigned_state")[column]
    return {
        str(int(state)): {
            "mean": _safe_float(values.mean()),
            "median": _safe_float(values.median()),
        }
        for state, values in grouped
    }


def _rate_by_state(frame: pd.DataFrame, column: str) -> dict[str, float | None]:
    if column not in frame.columns:
        return {}
    return {str(int(state)): _safe_float(values.astype(float).mean()) for state, values in frame.groupby("assigned_state")[column]}


def identify_likely_quiet_state(
    *,
    current_abs_move: dict[str, dict[str, float | None]],
    tiny_move_rate: dict[str, float | None],
    feature_means_by_state: dict[int, dict[str, float]],
) -> tuple[int | None, str]:
    candidate_states = sorted({int(state) for state in current_abs_move} | {int(state) for state in tiny_move_rate})
    if not candidate_states:
        return None, "no state metrics available"

    scores = {state: 0 for state in candidate_states}
    reasons: list[str] = []

    abs_values = {state: current_abs_move.get(str(state), {}).get("mean") for state in candidate_states}
    abs_values = {state: float(value) for state, value in abs_values.items() if value is not None and np.isfinite(float(value))}
    if abs_values:
        state = min(abs_values, key=abs_values.get)
        scores[state] += 1
        reasons.append(f"state {state} has lowest current_abs_move mean {abs_values[state]:.8f}")

    tiny_values = {state: tiny_move_rate.get(str(state)) for state in candidate_states}
    tiny_values = {state: float(value) for state, value in tiny_values.items() if value is not None and np.isfinite(float(value))}
    if tiny_values:
        state = max(tiny_values, key=tiny_values.get)
        scores[state] += 1
        reasons.append(f"state {state} has highest tiny_move_rate {tiny_values[state]:.6f}")

    vol_values = {
        state: feature_means_by_state.get(state, {}).get("realized_vol_30m")
        for state in candidate_states
    }
    vol_values = {state: float(value) for state, value in vol_values.items() if value is not None and np.isfinite(float(value))}
    if vol_values:
        state = min(vol_values, key=vol_values.get)
        scores[state] += 1
        reasons.append(f"state {state} has lowest realized_vol_30m mean {vol_values[state]:.8f}")

    entropy_values = {
        state: feature_means_by_state.get(state, {}).get("price_transition_entropy_30m")
        for state in candidate_states
    }
    entropy_values = {state: float(value) for state, value in entropy_values.items() if value is not None and np.isfinite(float(value))}
    if entropy_values:
        state = max(entropy_values, key=entropy_values.get)
        scores[state] += 1
        reasons.append(f"state {state} has highest price_transition_entropy_30m mean {entropy_values[state]:.8f}")

    def sort_key(state: int) -> tuple[int, float, float]:
        return (
            scores[state],
            tiny_values.get(state, float("-inf")),
            -abs_values.get(state, float("inf")),
        )

    quiet_state = max(candidate_states, key=sort_key)
    return quiet_state, "; ".join(reasons) if reasons else "selected from available state metrics"


def _direction_hit_diagnostics(frame: pd.DataFrame, features_std: pd.DataFrame, feature_columns: list[str]) -> dict[str, dict[str, float | None]]:
    signed_columns = [column for column in SIGNED_PRESSURE_FEATURES if column in feature_columns]
    if not signed_columns:
        return {}
    enriched = frame.copy()
    enriched["signed_pressure"] = features_std[signed_columns].mean(axis=1).to_numpy()
    diagnostics: dict[str, dict[str, float | None]] = {}
    for state, group in enriched.groupby("assigned_state"):
        pressure = float(group["signed_pressure"].mean())
        if abs(pressure) < 0.35:
            continue
        direction = 1.0 if pressure > 0.0 else -1.0
        current = np.sign(group["binance_move"].astype(float)) == direction
        next_move = group["next_binance_move"].dropna()
        next_hit = np.sign(next_move.astype(float)) == direction
        diagnostics[str(int(state))] = {
            "signed_pressure_mean": pressure,
            "current_direction_hit": _safe_float(current.mean()),
            "next_direction_hit": _safe_float(next_hit.mean()) if len(next_hit) else None,
        }
    return diagnostics


def summarize_selected_model(
    *,
    k: int,
    covariance_type: str,
    selected_fit: dict[str, Any],
    features_raw: pd.DataFrame,
    features_std: pd.DataFrame,
    feature_columns: list[str],
    x_train_rows: int,
) -> dict[str, Any]:
    assignments = pd.Series(selected_fit["assignments"], name="assigned_state")
    assignment_frame = features_raw[
        [
            "event_id",
            "event_start_time",
            "event_end_time",
            "split",
            "binance_label",
            "binance_move",
            "abs_binance_move",
            "tiny_move_near_boundary",
            "current_abs_move",
            "next_abs_move_5m",
            "next_binance_label",
            "next_binance_move",
            "next_tiny_move_near_boundary",
        ]
        + (["next_abs_move_10m"] if "next_abs_move_10m" in features_raw.columns else [])
    ].copy()
    assignment_frame["assigned_state"] = assignments.astype(int).to_numpy()
    assignment_frame["state_posterior_max"] = selected_fit["posterior_max"]
    enriched_raw = features_raw.assign(assigned_state=assignment_frame["assigned_state"])
    enriched_std = features_std.assign(assigned_state=assignment_frame["assigned_state"])
    occupancy = assignment_frame["assigned_state"].value_counts(normalize=True).reindex(range(k), fill_value=0.0).sort_index()
    occupancy_dict = {str(state): float(value) for state, value in occupancy.items()}
    occupancy_split = occupancy_by_split(assignment_frame["assigned_state"], assignment_frame["split"], k)
    feature_means_std = enriched_std.groupby("assigned_state")[feature_columns].mean().reindex(range(k))
    duplicate_pairs = duplicate_state_pairs(feature_means_std.dropna())
    warnings = build_model_warnings(
        state_occupancy=occupancy_dict,
        occupancy_split=occupancy_split,
        duplicate_pairs=duplicate_pairs,
        k=k,
    )
    aic, bic = approximate_information_criteria(
        selected_fit["train_log_likelihood"],
        n_rows=x_train_rows,
        k=k,
        n_features=len(feature_columns),
        covariance_type=covariance_type,
    )
    current_abs = _mean_median_by_state(enriched_raw, "current_abs_move")
    next_abs_5m = _mean_median_by_state(enriched_raw, "next_abs_move_5m")
    next_abs_10m = _mean_median_by_state(enriched_raw, "next_abs_move_10m")
    tiny_move_rate = _rate_by_state(enriched_raw, "tiny_move_near_boundary")
    next_tiny_move_rate = _rate_by_state(enriched_raw, "next_tiny_move_near_boundary")
    run_length = state_run_length_summary(assignment_frame["assigned_state"])
    post_confirmation = post_confirmation_diagnostics(assignment_frame)
    raw_feature_means = enriched_raw.groupby("assigned_state")[feature_columns].mean().to_dict(orient="index")
    quiet_state, quiet_reason = identify_likely_quiet_state(
        current_abs_move=current_abs,
        tiny_move_rate=tiny_move_rate,
        feature_means_by_state={int(state): values for state, values in raw_feature_means.items()},
    )
    median_next_values = [value["median"] for value in next_abs_5m.values() if value["median"] is not None]
    mean_next_values = [value["mean"] for value in next_abs_5m.values() if value["mean"] is not None]
    return {
        "k": k,
        "covariance_type": covariance_type,
        "selected_hmm_feature_columns": feature_columns,
        "selected_seed": selected_fit["seed"],
        "converged": selected_fit["converged"],
        "n_iter": selected_fit["n_iter"],
        "train_log_likelihood": selected_fit["train_log_likelihood"],
        "aic": aic,
        "bic": bic,
        "state_occupancy": occupancy_dict,
        "state_occupancy_overall": occupancy_dict,
        "state_occupancy_by_split": occupancy_split,
        "min_state_occupancy": float(occupancy.min()) if len(occupancy) else 0.0,
        "max_state_occupancy": float(occupancy.max()) if len(occupancy) else 0.0,
        "transition_matrix": selected_fit["model"].transmat_.tolist(),
        "run_length_diagnostics": run_length,
        "state_run_length_summary": run_length,
        "feature_means_by_state": raw_feature_means,
        "mean_raw_feature_values_by_state": raw_feature_means,
        "mean_standardized_feature_values_by_state": feature_means_std.to_dict(orient="index"),
        "current_abs_move_mean_median_by_state": current_abs,
        "current_abs_move_by_state": current_abs,
        "next_abs_move_5m_mean_median_by_state": next_abs_5m,
        "next_abs_move_5m_by_state": next_abs_5m,
        "next_abs_move_10m_mean_median_by_state": next_abs_10m,
        "next_abs_move_10m_by_state": next_abs_10m,
        "tiny_move_rate_by_state": tiny_move_rate,
        "next_tiny_move_rate_by_state": next_tiny_move_rate,
        "label_distribution_by_state": _label_distribution_by_state(assignment_frame, "binance_label"),
        "next_label_distribution_by_state": _label_distribution_by_state(assignment_frame, "next_binance_label"),
        "post_confirmation_diagnostics": post_confirmation,
        "likely_quiet_state": quiet_state,
        "quiet_state_reason": quiet_reason,
        "direction_hit_by_signed_pressure_state": _direction_hit_diagnostics(assignment_frame, features_std, feature_columns),
        "duplicate_state_pairs": duplicate_pairs,
        "max_train_test_occupancy_shift": train_test_occupancy_shift(occupancy_split, k),
        "mean_next_abs_move_spread_across_states": float(max(mean_next_values) - min(mean_next_values)) if mean_next_values else None,
        "median_next_abs_move_spread_across_states": float(max(median_next_values) - min(median_next_values)) if median_next_values else None,
        "warnings": warnings,
        "assignment_frame": assignment_frame,
    }


def fit_hmm_sweep(
    standardized: pd.DataFrame,
    *,
    k_values: list[int],
    covariance_types: list[str],
    seeds: list[int],
    feature_columns: list[str],
) -> tuple[dict[str, Any], list[str]]:
    try:
        from hmmlearn.hmm import GaussianHMM
    except Exception:
        return {"hmmlearn_available": False, "models": {}, "seed_fits": {}}, ["hmmlearn unavailable; skipped HMM fitting."]

    warnings: list[str] = []
    train = standardized[standardized["split"] == "train"]
    x_train = train[feature_columns].to_numpy()
    x_all = standardized[feature_columns].to_numpy()
    if len(x_train) == 0 or len(x_all) == 0:
        return {"hmmlearn_available": True, "models": {}, "seed_fits": {}}, ["no usable rows remained for HMM fitting."]

    models: dict[str, Any] = {}
    seed_fits: dict[str, Any] = {}
    for covariance_type in covariance_types:
        for k in k_values:
            model_key = f"{covariance_type}_k{k}"
            fits: list[dict[str, Any]] = []
            best_fit: dict[str, Any] | None = None
            for seed in seeds:
                try:
                    model = GaussianHMM(
                        n_components=k,
                        covariance_type=covariance_type,
                        n_iter=300,
                        tol=1e-3,
                        random_state=seed,
                    )
                    model.fit(x_train)
                    train_log_likelihood = float(model.score(x_train))
                    assignments = model.predict(x_all)
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
                        "n_iter": n_iter,
                        "train_log_likelihood": train_log_likelihood,
                        "model": model,
                        "assignments": assignments,
                        "posterior_max": posterior_max,
                        "warnings": [],
                    }
                except Exception as exc:
                    fit = {
                        "seed": seed,
                        "converged": False,
                        "n_iter": 0,
                        "train_log_likelihood": None,
                        "model": None,
                        "assignments": None,
                        "posterior_max": None,
                        "warnings": [str(exc)],
                    }
                    warnings.append(f"{model_key} seed={seed} fit failed: {exc}")
                fits.append(fit)
                if fit["model"] is not None and (best_fit is None or fit["train_log_likelihood"] > best_fit["train_log_likelihood"]):
                    best_fit = fit
            seed_fits[model_key] = {
                "k": k,
                "covariance_type": covariance_type,
                "selected_seed": None if best_fit is None else best_fit["seed"],
                "fits": [
                    {
                        "seed": fit["seed"],
                        "converged": fit["converged"],
                        "n_iter": fit["n_iter"],
                        "train_log_likelihood": fit["train_log_likelihood"],
                        "warnings": fit["warnings"],
                    }
                    for fit in fits
                ],
            }
            if best_fit is None:
                warnings.append(f"all HMM fits failed for {model_key}")
            else:
                models[model_key] = best_fit | {"k": k, "covariance_type": covariance_type}
    return {"hmmlearn_available": True, "models": models, "seed_fits": seed_fits, "x_train_rows": int(len(x_train))}, warnings


def summary_row_from_diagnostics(diagnostics: dict[str, Any]) -> dict[str, Any]:
    occupancy = diagnostics["state_occupancy_overall"]
    low_2 = sum(1 for value in occupancy.values() if float(value) < 0.02)
    low_5 = sum(1 for value in occupancy.values() if float(value) < 0.05)
    quiet_state = diagnostics.get("likely_quiet_state")
    quiet_key = None if quiet_state is None else str(int(quiet_state))
    run_length = diagnostics.get("run_length_diagnostics", {}).get(quiet_key, {}) if quiet_key is not None else {}
    post_confirmation = diagnostics.get("post_confirmation_diagnostics", {}).get(quiet_key, {}) if quiet_key is not None else {}
    current_abs = diagnostics.get("current_abs_move_by_state", {}).get(quiet_key, {}) if quiet_key is not None else {}
    return {
        "k": diagnostics["k"],
        "covariance_type": diagnostics["covariance_type"],
        "selected_seed": diagnostics["selected_seed"],
        "converged": diagnostics["converged"],
        "n_iter": diagnostics["n_iter"],
        "train_log_likelihood": diagnostics["train_log_likelihood"],
        "aic": diagnostics["aic"],
        "bic": diagnostics["bic"],
        "min_state_occupancy": diagnostics["min_state_occupancy"],
        "max_state_occupancy": diagnostics["max_state_occupancy"],
        "low_occupancy_state_count_lt_2pct": low_2,
        "low_occupancy_state_count_lt_5pct": low_5,
        "duplicate_state_pair_count": len(diagnostics["duplicate_state_pairs"]),
        "mean_next_abs_move_spread_across_states": diagnostics["mean_next_abs_move_spread_across_states"],
        "median_next_abs_move_spread_across_states": diagnostics["median_next_abs_move_spread_across_states"],
        "max_train_test_occupancy_shift": diagnostics["max_train_test_occupancy_shift"],
        "likely_quiet_state": quiet_state,
        "quiet_state_tiny_rate": diagnostics.get("tiny_move_rate_by_state", {}).get(quiet_key) if quiet_key is not None else None,
        "quiet_state_abs_move_mean": current_abs.get("mean"),
        "quiet_state_run_length_mean": run_length.get("run_length_mean"),
        "quiet_state_run_length_p90": run_length.get("run_length_p90"),
        "quiet_state_p_run_length_ge_5": run_length.get("p_run_length_ge_5"),
        "quiet_state_post_confirmation_tiny_rate": post_confirmation.get("post_confirmation_tiny_rate"),
        "quiet_state_p_remaining_after_first_ge_4": post_confirmation.get("p_remaining_after_first_ge_4"),
        "warnings": "; ".join(diagnostics["warnings"]),
    }


def write_sweep_outputs(
    output_dir: Path,
    diagnostics: dict[str, Any],
    model_diagnostics: dict[str, dict[str, Any]],
    summary_rows: list[dict[str, Any]],
    *,
    features_raw: pd.DataFrame | None = None,
    features_standardized: pd.DataFrame | None = None,
) -> dict[str, str]:
    output_dir.mkdir(parents=True, exist_ok=True)
    for stale in output_dir.glob("best_model_assignments_k*_*.csv"):
        stale.unlink()
    for stale in output_dir.glob("model_*_k*_diagnostics.json"):
        stale.unlink()
    for stale in output_dir.glob("model_diag_k*_*.json"):
        stale.unlink()

    summary_path = output_dir / "sweep_summary.csv"
    diagnostics_path = output_dir / "sweep_diagnostics.json"
    readme_path = output_dir / "hmm_sweep_readme_summary.txt"
    pd.DataFrame(summary_rows).to_csv(summary_path, index=False)

    serializable_models: dict[str, Any] = {}
    paths = {
        "sweep_summary": str(summary_path),
        "sweep_diagnostics": str(diagnostics_path),
        "hmm_sweep_readme_summary": str(readme_path),
    }
    if features_raw is not None:
        raw_path = output_dir / "hmm_features_raw.csv"
        features_raw.to_csv(raw_path, index=False)
        paths["hmm_features_raw"] = str(raw_path)
    if features_standardized is not None:
        standardized_path = output_dir / "hmm_features_standardized.csv"
        features_standardized.to_csv(standardized_path, index=False)
        paths["hmm_features_standardized"] = str(standardized_path)
    for model_key, payload in model_diagnostics.items():
        assignment_frame = payload.pop("assignment_frame")
        covariance_type = payload["covariance_type"]
        k = payload["k"]
        assignment_path = output_dir / f"best_model_assignments_k{k}_{covariance_type}.csv"
        model_path = output_dir / f"model_diag_k{k}_{covariance_type}.json"
        assignment_frame.to_csv(assignment_path, index=False)
        model_path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
        serializable_models[model_key] = payload
        paths[f"best_model_assignments_k{k}_{covariance_type}"] = str(assignment_path)
        paths[f"model_diag_k{k}_{covariance_type}"] = str(model_path)

    diagnostics["selected_model_diagnostics"] = serializable_models
    diagnostics["output_paths"] = paths
    diagnostics_path.write_text(json.dumps(diagnostics, indent=2, default=str), encoding="utf-8")
    readme_path.write_text(build_readme_summary(summary_rows, diagnostics), encoding="utf-8")
    return paths


def build_readme_summary(summary_rows: list[dict[str, Any]], diagnostics: dict[str, Any]) -> str:
    summary = pd.DataFrame(summary_rows)
    lines = [
        f"event_table_path={diagnostics['event_table_path']}",
        f"selected_event_rows={diagnostics['selected_event_rows']}",
        f"event_timestamp_column={diagnostics.get('event_filter', {}).get('event_timestamp_column')}",
        f"start_date={diagnostics.get('event_filter', {}).get('start_date')}",
        f"end_date={diagnostics.get('event_filter', {}).get('end_date')}",
        f"filtered_min_timestamp={diagnostics.get('event_filter', {}).get('filtered_min_timestamp')}",
        f"filtered_max_timestamp={diagnostics.get('event_filter', {}).get('filtered_max_timestamp')}",
        f"hmm_fit_rows={diagnostics['hmm_fit_rows']}",
        f"feature_set={diagnostics['feature_set']}",
        f"feature_construction_seconds={diagnostics['feature_construction_seconds']:.3f}",
        f"hmm_fit_seconds={diagnostics['hmm_fit_seconds']:.3f}",
        f"hmmlearn_available={diagnostics['hmmlearn_available']}",
        "",
        "Recommendation summary:",
    ]
    if summary.empty:
        lines.extend(["- best simple candidate by interpretability: none", "- best magnitude-separation candidate: none"])
    else:
        clean = summary.copy()
        clean["warning_count"] = clean["warnings"].fillna("").map(lambda value: 0 if not str(value) else len(str(value).split("; ")))
        simple_pool = clean[(clean["low_occupancy_state_count_lt_5pct"] == 0) & (clean["duplicate_state_pair_count"] == 0)]
        simple = simple_pool.sort_values(["k", "warning_count", "max_train_test_occupancy_shift"]).head(1)
        if simple.empty:
            simple = clean.sort_values(["warning_count", "k", "max_train_test_occupancy_shift"]).head(1)
        mag = clean.sort_values(["mean_next_abs_move_spread_across_states", "warning_count"], ascending=[False, True]).head(1)
        simple_row = simple.iloc[0]
        mag_row = mag.iloc[0]
        lines.append(
            f"- best simple candidate by interpretability: k={int(simple_row['k'])} {simple_row['covariance_type']} seed={int(simple_row['selected_seed'])}, min_occ={simple_row['min_state_occupancy']:.3f}"
        )
        lines.append(
            f"- best magnitude-separation candidate: k={int(mag_row['k'])} {mag_row['covariance_type']} seed={int(mag_row['selected_seed'])}, mean_next_abs_spread={mag_row['mean_next_abs_move_spread_across_states']:.8f}"
        )
        micro = clean[clean["low_occupancy_state_count_lt_5pct"] > 0]
        duplicates = clean[clean["duplicate_state_pair_count"] > 0]
        needs = clean[(clean["warnings"].fillna("") != "") & (clean["low_occupancy_state_count_lt_5pct"] == 0) & (clean["duplicate_state_pair_count"] == 0)]
        lines.append("- candidates rejected for microstates: " + _compact_candidate_list(micro))
        lines.append("- candidates rejected for duplicate states: " + _compact_candidate_list(duplicates))
        lines.append("- candidates needing deeper inspection: " + _compact_candidate_list(needs))
    lines.extend(["", "Pair-order warning:", f"- {PAIR_ORDER_WARNING}"])
    lines.extend(["", "Warnings:", *[f"- {warning}" for warning in diagnostics.get("warnings", [])]])
    return "\n".join(lines) + "\n"


def _compact_candidate_list(frame: pd.DataFrame, limit: int = 8) -> str:
    if frame.empty:
        return "none"
    values = [f"k={int(row.k)} {row.covariance_type}" for row in frame.head(limit).itertuples(index=False)]
    suffix = "" if len(frame) <= limit else f" (+{len(frame) - limit} more)"
    return ", ".join(values) + suffix


def run_sweep(
    *,
    event_table_path: Path,
    input_roots: list[Path],
    output_dir: Path,
    tail_events: int | None,
    k_values: list[int],
    covariance_types: list[str],
    seeds: list[int],
    feature_set: str,
    entropy_mode: str,
    start_date: str | None = None,
    end_date: str | None = None,
    hmm_feature_clip_abs: float = HMM_FEATURE_CLIP_ABS,
) -> dict[str, Any]:
    if entropy_mode not in ENTROPY_MODES:
        raise ValueError(f"Unsupported entropy mode: {entropy_mode}")
    feature_columns = feature_columns_for_set(feature_set)
    start = time.perf_counter()
    all_events = load_event_set(event_table_path)
    selected_events, event_filter_metadata, filter_warnings = filter_events_for_sweep(
        all_events,
        start_date=start_date,
        end_date=end_date,
        tail_events=tail_events,
    )
    prices = load_binance_1m_klines(input_roots).frame
    feature_start = time.perf_counter()
    features_raw, dropped_rows, leakage_warnings = build_feature_matrix(
        selected_events,
        prices,
        shock_age_cap_minutes=SHOCK_AGE_CAP_MINUTES,
        entropy_mode=entropy_mode,
    )
    feature_seconds = float(time.perf_counter() - feature_start)
    if features_raw.empty:
        raise RuntimeError("No usable feature rows emitted.")
    features_raw = enrich_outcome_columns(features_raw)
    features_std_all, scaler_params = standardize_features(features_raw, FULL_FEATURE_COLUMNS)
    clipped_std_all, clipped_counts = clip_standardized_features(features_std_all, FULL_FEATURE_COLUMNS, hmm_feature_clip_abs)
    hmm_raw, hmm_nan_counts, hmm_dropped = prepare_hmm_matrix(features_raw, feature_columns)
    hmm_std, _, _ = prepare_hmm_matrix(clipped_std_all, feature_columns)
    hmm_raw = hmm_raw.reset_index(drop=True)
    hmm_std = hmm_std.reset_index(drop=True)

    fit_start = time.perf_counter()
    hmm_results, hmm_warnings = fit_hmm_sweep(
        hmm_std,
        k_values=k_values,
        covariance_types=covariance_types,
        seeds=seeds,
        feature_columns=feature_columns,
    )
    fit_seconds = float(time.perf_counter() - fit_start)
    diagnostics: dict[str, Any] = {
        "event_table_path": str(event_table_path),
        "input_roots": [str(root) for root in input_roots],
        "input_event_rows": event_filter_metadata["input_event_rows_before_filtering"],
        "selected_event_rows": int(len(selected_events)),
        "event_filter": event_filter_metadata,
        "filtered_min_timestamp": event_filter_metadata["filtered_min_timestamp"],
        "filtered_max_timestamp": event_filter_metadata["filtered_max_timestamp"],
        "label_counts_after_filtering": event_filter_metadata["label_counts_after_filtering"],
        "feature_rows_emitted": int(len(features_raw)),
        "hmm_fit_rows": int(len(hmm_std)),
        "feature_construction_seconds": feature_seconds,
        "hmm_fit_seconds": fit_seconds,
        "total_seconds": float(time.perf_counter() - start),
        "feature_set": feature_set,
        "selected_hmm_feature_columns": feature_columns,
        "k_values": k_values,
        "covariance_types": covariance_types,
        "seeds": seeds,
        "entropy_mode": entropy_mode,
        "hmm_feature_clip_abs": hmm_feature_clip_abs,
        "scaler_params": scaler_params,
        "clipped_value_counts_by_feature": clipped_counts,
        "hmm_feature_nan_counts_after_encoding": hmm_nan_counts,
        "rows_dropped_by_reason": dropped_rows,
        "hmmlearn_available": bool(hmm_results["hmmlearn_available"]),
        "seed_fit_diagnostics": hmm_results.get("seed_fits", {}),
        "warnings": [],
    }
    warnings = filter_warnings + list(leakage_warnings) + hmm_warnings
    if tail_events is not None:
        warnings.append("event set was tail-bounded after date filtering before feature construction")
    if start_date is not None or end_date is not None:
        warnings.append("event set was date-filtered before feature construction")
    if hmm_dropped:
        warnings.append(f"dropped {hmm_dropped} rows for nonfinite HMM features")
    if entropy_mode == "fast":
        warnings.append("entropy_mode=fast uses rolling sign-state entropy, not exact transition entropy")
    warnings.append(PAIR_ORDER_WARNING)
    diagnostics["warnings"] = warnings

    model_diagnostics: dict[str, dict[str, Any]] = {}
    summary_rows: list[dict[str, Any]] = []
    if hmm_results["hmmlearn_available"]:
        for model_key, selected_fit in hmm_results["models"].items():
            selected_diag = summarize_selected_model(
                k=selected_fit["k"],
                covariance_type=selected_fit["covariance_type"],
                selected_fit=selected_fit,
                features_raw=hmm_raw,
                features_std=hmm_std,
                feature_columns=feature_columns,
                x_train_rows=hmm_results["x_train_rows"],
            )
            model_diagnostics[model_key] = selected_diag
            summary_rows.append(summary_row_from_diagnostics(selected_diag))

    paths = write_sweep_outputs(
        output_dir,
        diagnostics,
        model_diagnostics,
        summary_rows,
        features_raw=hmm_raw,
        features_standardized=hmm_std,
    )
    diagnostics["output_paths"] = paths
    return diagnostics


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Offline sweep of Binance 1m BTC-5m HMM regime models.")
    parser.add_argument("--event-table-path", type=Path, default=DEFAULT_OUTPUT_CSV)
    parser.add_argument("--input-root", type=Path, action="append", default=None)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--tail-events", type=int, default=300000)
    parser.add_argument("--start-date", type=str, default=None, help="UTC inclusive lower bound, YYYY-MM-DD")
    parser.add_argument("--end-date", type=str, default=None, help="UTC exclusive upper bound, YYYY-MM-DD")
    parser.add_argument("--k-values", type=parse_k_values, default=DEFAULT_K_VALUES)
    parser.add_argument("--covariance-types", type=parse_covariance_types, default=DEFAULT_COVARIANCE_TYPES)
    parser.add_argument("--seeds", type=parse_seeds, default=DEFAULT_SEEDS)
    parser.add_argument("--feature-set", choices=sorted(FEATURE_SETS), default="reduced")
    parser.add_argument("--entropy-mode", choices=sorted(ENTROPY_MODES), default="fast")
    parser.add_argument("--hmm-feature-clip-abs", type=float, default=HMM_FEATURE_CLIP_ABS)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    diagnostics = run_sweep(
        event_table_path=args.event_table_path,
        input_roots=args.input_root or DEFAULT_INPUT_ROOTS,
        output_dir=args.output_dir,
        tail_events=args.tail_events,
        start_date=args.start_date,
        end_date=args.end_date,
        k_values=args.k_values,
        covariance_types=args.covariance_types,
        seeds=args.seeds,
        feature_set=args.feature_set,
        entropy_mode=args.entropy_mode,
        hmm_feature_clip_abs=args.hmm_feature_clip_abs,
    )
    print(
        json.dumps(
            {
                "selected_event_rows": diagnostics["selected_event_rows"],
                "event_filter": diagnostics["event_filter"],
                "feature_rows_emitted": diagnostics["feature_rows_emitted"],
                "hmm_fit_rows": diagnostics["hmm_fit_rows"],
                "feature_construction_seconds": diagnostics["feature_construction_seconds"],
                "hmm_fit_seconds": diagnostics["hmm_fit_seconds"],
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
