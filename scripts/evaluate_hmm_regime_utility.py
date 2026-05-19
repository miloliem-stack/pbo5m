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

from scripts import sweep_hmm_regime_health as health

DEFAULT_OUTPUT_DIR = Path("artifacts/hmm_regime_utility")
DEFAULT_CANDIDATES = [
    "core_1m__gaussian_hmm__k4",
    "laplace_1m__gaussian_hmm__k4",
    "core_1m__gaussian_hmm__k5",
    "laplace_1m__gaussian_hmm__k5",
]
DEFAULT_CONFIDENCE_THRESHOLDS = [0.60, 0.70, 0.75, 0.80, 0.90]
FIXED_HORIZON_MINUTES = [1, 2, 3, 5, 10]


def parse_csv_list(value: str) -> list[str]:
    items = [item.strip() for item in str(value).split(",") if item.strip()]
    if not items:
        raise ValueError("comma-separated list cannot be empty")
    return items


def parse_float_csv(value: str) -> list[float]:
    return [float(item) for item in parse_csv_list(value)]


def parse_candidate_id(model_id: str) -> dict[str, Any]:
    parts = model_id.split("__")
    if len(parts) != 3 or not parts[2].startswith("k"):
        raise ValueError(f"invalid candidate model id: {model_id}")
    return {"feature_set": parts[0], "family": parts[1], "n_states": int(parts[2][1:])}


def confidence_bucket(p_max: float, thresholds: list[float]) -> str:
    ordered = sorted(thresholds)
    if not np.isfinite(p_max):
        return "missing"
    bucket = f"<{ordered[0]:.2f}"
    for threshold in ordered:
        if p_max >= threshold:
            bucket = f">={threshold:.2f}"
    return bucket


def minute_bucket(minutes: float | None, cuts: list[int], prefix: str = "") -> str:
    if minutes is None or not np.isfinite(minutes):
        return "missing"
    value = float(minutes)
    previous = 0
    for cut in cuts:
        if value < cut:
            return f"{prefix}{previous}-{cut}m"
        previous = cut
    return f"{prefix}>={cuts[-1]}m"


def market_age_bucket(seconds: float | None) -> str:
    if seconds is None or not np.isfinite(seconds):
        return "missing"
    if seconds < 60:
        return "0-60s"
    if seconds < 120:
        return "60-120s"
    if seconds < 180:
        return "120-180s"
    if seconds < 240:
        return "180-240s"
    return "240-300s"


def transition_age_bucket(minutes: float | None) -> str:
    if minutes is None or not np.isfinite(minutes):
        return "missing"
    value = int(math.floor(minutes))
    if value <= 0:
        return "0m"
    if value in (1, 2, 3, 4):
        return f"{value}m"
    return ">=5m"


def transition_fields(states: np.ndarray) -> pd.DataFrame:
    previous: list[int | None] = []
    transition_types: list[str] = []
    is_transition: list[bool] = []
    ages: list[int] = []
    since: list[int] = []
    current_state: int | None = None
    current_age = 0
    minutes_since_switch = 0
    for idx, state_value in enumerate(states.astype(int)):
        prev = None if idx == 0 else int(states[idx - 1])
        previous.append(prev)
        if idx == 0:
            current_state = state_value
            current_age = 0
            minutes_since_switch = 0
            is_transition.append(False)
            transition_types.append("START")
        elif state_value != current_state:
            is_transition.append(True)
            transition_types.append(f"{prev}->{state_value}")
            current_state = state_value
            current_age = 0
            minutes_since_switch = 0
        else:
            is_transition.append(False)
            transition_types.append(f"{state_value}->{state_value}")
            current_age += 1
            minutes_since_switch += 1
        ages.append(current_age)
        since.append(minutes_since_switch)
    return pd.DataFrame(
        {
            "previous_map_state": previous,
            "transition_type": transition_types,
            "is_transition": is_transition,
            "regime_age_minutes": ages,
            "minutes_since_last_transition": since,
        }
    )


def write_optional_parquet(frame: pd.DataFrame, path: Path) -> bool:
    try:
        frame.to_parquet(path, index=False)
        return True
    except Exception:
        return False


def assign_market_window_outcomes(prices: pd.DataFrame, market_window_seconds: int) -> pd.DataFrame:
    frame = prices[["timestamp", "close"]].sort_values("timestamp").drop_duplicates("timestamp").reset_index(drop=True).copy()
    freq = f"{int(market_window_seconds)}s"
    frame["market_window_start"] = frame["timestamp"].dt.floor(freq)
    frame["market_window_end"] = frame["market_window_start"] + pd.to_timedelta(market_window_seconds, unit="s")
    grouped = frame.groupby("market_window_start", sort=True)
    windows = grouped.agg(
        current_window_open_price_proxy=("close", "first"),
        current_window_end_price_proxy=("close", "last"),
        window_first_timestamp=("timestamp", "first"),
        window_last_timestamp=("timestamp", "last"),
    ).reset_index()
    windows["current_window_return"] = np.log(
        windows["current_window_end_price_proxy"] / windows["current_window_open_price_proxy"]
    )
    windows["current_window_result_up_proxy"] = np.where(
        windows["current_window_return"] > 0,
        1.0,
        np.where(windows["current_window_return"] < 0, 0.0, np.nan),
    )
    windows["next_5m_return"] = windows["current_window_return"].shift(-1)
    windows["next_5m_abs_return"] = windows["next_5m_return"].abs()
    windows["next_5m_result_up_proxy"] = np.where(
        windows["next_5m_return"] > 0,
        1.0,
        np.where(windows["next_5m_return"] < 0, 0.0, np.nan),
    )
    enriched = frame.merge(windows, on="market_window_start", how="left")
    enriched["market_age_seconds"] = (enriched["timestamp"] - enriched["market_window_start"]).dt.total_seconds()
    enriched["seconds_to_market_end"] = (enriched["market_window_end"] - enriched["timestamp"]).dt.total_seconds()
    enriched["current_window_remaining_return"] = np.log(enriched["current_window_end_price_proxy"] / enriched["close"])
    enriched["current_window_remaining_abs_return"] = enriched["current_window_remaining_return"].abs()
    price_by_time = pd.Series(enriched["close"].to_numpy(), index=enriched["timestamp"]).to_dict()
    for horizon in FIXED_HORIZON_MINUTES:
        future_times = enriched["timestamp"] + pd.Timedelta(minutes=horizon)
        future_prices = future_times.map(price_by_time)
        returns = np.log(pd.to_numeric(future_prices, errors="coerce") / enriched["close"])
        enriched[f"fixed_horizon_return_{horizon}m"] = returns
        enriched[f"fixed_horizon_abs_return_{horizon}m"] = returns.abs()
    return enriched


def canonical_label_from_features(row: pd.Series) -> str:
    vol = float(row.get("realized_vol_30m_mean", row.get("realized_vol_30m", 0.0)) or 0.0)
    shock = float(row.get("shock_score_mean", row.get("shock_score", 0.0)) or 0.0)
    flip = float(row.get("sign_flip_rate_15m_mean", row.get("sign_flip_rate_15m", 0.0)) or 0.0)
    drift = float(row.get("drift_to_vol_15m_mean", row.get("drift_to_vol_15m", 0.0)) or 0.0)
    signed = float(row.get("signed_return_15m_mean", row.get("signed_return_15m", 0.0)) or 0.0)
    if vol >= 0.75 or shock >= 2.0:
        return "shock_or_high_vol"
    if drift > 0.25 or signed > 0.0:
        return "trend_up_like"
    if drift < -0.25 or signed < 0.0:
        return "trend_down_like"
    if vol <= 0.25 and flip >= 0.45:
        return "chop_or_calm_chop"
    return "neutral_or_other"


def load_signature_labels(regime_health_dir: Path, candidates: list[str]) -> dict[tuple[str, int, int], str]:
    path = regime_health_dir / "state_feature_signatures.csv"
    if not path.exists():
        return {}
    signatures = pd.read_csv(path)
    labels: dict[tuple[str, int, int], str] = {}
    for _, row in signatures[signatures["model_id"].isin(candidates)].iterrows():
        labels[(str(row["model_id"]), int(row["fold_id"]), int(row["raw_state"]))] = canonical_label_from_features(row)
    return labels


def reconstruct_candidate_states(
    prices: pd.DataFrame,
    regime_health_dir: Path,
    candidates: list[str],
    thresholds: list[float],
    max_folds: int | None,
    random_seed: int | None,
    train_rows_override: int | None = None,
    test_rows_override: int | None = None,
    step_rows_override: int | None = None,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    config_path = regime_health_dir / "sweep_config.json"
    if not config_path.exists():
        raise FileNotFoundError(f"missing regime-health sweep_config.json: {config_path}")
    config = json.loads(config_path.read_text(encoding="utf-8"))
    train_rows = int(train_rows_override if train_rows_override is not None else config["train_rows"])
    test_rows = int(test_rows_override if test_rows_override is not None else config["test_rows"])
    step_rows = int(step_rows_override if step_rows_override is not None else config["step_rows"])
    seed = int(config.get("random_seed", 1) if random_seed is None else random_seed)
    labels = load_signature_labels(regime_health_dir, candidates)
    rows: list[pd.DataFrame] = []
    feature_cache: dict[str, tuple[pd.DataFrame, list[str]]] = {}

    for candidate in candidates:
        parsed = parse_candidate_id(candidate)
        feature_set = parsed["feature_set"]
        family = parsed["family"]
        n_states = int(parsed["n_states"])
        if feature_set not in feature_cache:
            features_path = regime_health_dir / f"features_{feature_set}.csv"
            if features_path.exists() and len(prices) > train_rows + test_rows and train_rows_override is None and test_rows_override is None:
                features = pd.read_csv(features_path, parse_dates=["timestamp"])
                manifest = json.loads((regime_health_dir / "feature_manifest.json").read_text(encoding="utf-8"))
                feature_columns = manifest[feature_set]["columns"]
                if features["timestamp"].dt.tz is None:
                    features["timestamp"] = features["timestamp"].dt.tz_localize("UTC")
                else:
                    features["timestamp"] = features["timestamp"].dt.tz_convert("UTC")
            else:
                features, manifest = health.build_features(prices, feature_set)
                feature_columns = manifest["columns"]
            feature_cache[feature_set] = (features, feature_columns)
        features, feature_columns = feature_cache[feature_set]
        folds = health.make_walk_forward_splits(len(features), train_rows, test_rows, step_rows)
        if max_folds is not None and max_folds > 0:
            folds = folds[:max_folds]
        for fold in folds:
            train = features.iloc[fold.train_start : fold.train_end].reset_index(drop=True)
            test = features.iloc[fold.test_start : fold.test_end].reset_index(drop=True)
            if len(train) <= n_states * 4 or test.empty:
                continue
            x_train, x_test, _ = health.standardize_train_test(train, test, feature_columns)
            model = health.fit_hmm(family, n_states, x_train, seed + fold.fold_id)
            filtered = health.filtered_probabilities(model, x_test)
            states = filtered.argmax(axis=1).astype(int)
            p_max = filtered.max(axis=1)
            transition = transition_fields(states)
            frame = pd.DataFrame(
                {
                    "timestamp": test["timestamp"],
                    "candidate_model_id": candidate,
                    "feature_set": feature_set,
                    "family": family,
                    "n_states": n_states,
                    "fold_id": fold.fold_id,
                    "map_state": states,
                    "raw_state_id": states,
                    "p_max": p_max,
                }
            )
            frame = pd.concat([frame.reset_index(drop=True), transition], axis=1)
            frame["confidence_bucket"] = frame["p_max"].map(lambda value: confidence_bucket(float(value), thresholds))
            frame["canonical_state_label"] = [
                labels.get((candidate, int(fold.fold_id), int(state)), "neutral_or_other") for state in states
            ]
            frame["previous_canonical_label"] = [
                None if pd.isna(prev) else labels.get((candidate, int(fold.fold_id), int(prev)), "neutral_or_other")
                for prev in frame["previous_map_state"]
            ]
            rows.append(frame)
    reconstructed = pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()
    diagnostics = {
        "per_timestamp_state_source": "reconstructed_candidate_only",
        "train_rows": train_rows,
        "test_rows": test_rows,
        "step_rows": step_rows,
        "max_folds": max_folds,
        "candidate_count": len(candidates),
        "per_timestamp_rows": int(len(reconstructed)),
    }
    return reconstructed, diagnostics


def add_outcomes(regime: pd.DataFrame, outcome_frame: pd.DataFrame) -> pd.DataFrame:
    merged = regime.merge(outcome_frame, on="timestamp", how="left", suffixes=("", "_price"))
    merged["regime_age_bucket"] = merged["regime_age_minutes"].map(lambda value: minute_bucket(value, [1, 2, 5, 10, 30, 60]))
    merged["market_age_bucket"] = merged["market_age_seconds"].map(market_age_bucket)
    merged["age_since_switch_bucket"] = merged["minutes_since_last_transition"].map(transition_age_bucket)
    current_direction = np.sign(merged["close"] - merged["current_window_open_price_proxy"])
    remaining_direction = np.sign(merged["current_window_end_price_proxy"] - merged["close"])
    next_direction = np.sign(merged["next_5m_return"])
    merged["continuation_rate_indicator"] = np.where(
        (current_direction != 0) & (remaining_direction != 0),
        current_direction == remaining_direction,
        np.nan,
    )
    merged["reversal_rate_indicator"] = np.where(
        (current_direction != 0) & (remaining_direction != 0),
        current_direction != remaining_direction,
        np.nan,
    )
    merged["next_continuation_indicator"] = np.where(
        (current_direction != 0) & (next_direction != 0),
        current_direction == next_direction,
        np.nan,
    )
    return merged


def up_rate_se(series: pd.Series) -> float | None:
    values = pd.to_numeric(series, errors="coerce").dropna()
    n = len(values)
    if n == 0:
        return None
    p = float(values.mean())
    return float(math.sqrt(p * (1.0 - p) / n))


def z_vs_half(series: pd.Series) -> float | None:
    values = pd.to_numeric(series, errors="coerce").dropna()
    n = len(values)
    if n == 0:
        return None
    return float((values.mean() - 0.5) / math.sqrt(0.25 / n))


def utility_flags(row: pd.Series, min_sample: int) -> str:
    flags: list[str] = []
    n = int(row.get("n", 0) or 0)
    up = row.get("up_rate_next_5m", np.nan)
    abs_move = row.get("mean_next_5m_abs_return", np.nan)
    if n < min_sample:
        flags.append("LOW_SAMPLE")
    if pd.notna(up) and abs(float(up) - 0.5) < 0.03:
        flags.append("WEAK_EDGE")
    if pd.notna(up) and pd.notna(abs_move) and abs(float(up) - 0.5) < 0.04 and float(abs_move) > 0:
        flags.append("MAGNITUDE_ONLY")
    return ",".join(flags)


def grouped_utility(frame: pd.DataFrame, group_cols: list[str], min_sample: int) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=group_cols)
    grouped = frame.groupby(group_cols, dropna=False)
    out = grouped.agg(
        n=("timestamp", "size"),
        up_rate_current_window=("current_window_result_up_proxy", "mean"),
        up_rate_next_5m=("next_5m_result_up_proxy", "mean"),
        mean_current_remaining_return=("current_window_remaining_return", "mean"),
        median_current_remaining_return=("current_window_remaining_return", "median"),
        mean_current_remaining_abs_return=("current_window_remaining_abs_return", "mean"),
        mean_next_5m_return=("next_5m_return", "mean"),
        median_next_5m_return=("next_5m_return", "median"),
        mean_next_5m_abs_return=("next_5m_abs_return", "mean"),
        continuation_rate=("continuation_rate_indicator", "mean"),
        reversal_rate=("reversal_rate_indicator", "mean"),
        mean_p_max=("p_max", "mean"),
        median_p_max=("p_max", "median"),
        average_seconds_to_market_end=("seconds_to_market_end", "mean"),
    ).reset_index()
    out["up_rate_next_5m_se"] = grouped["next_5m_result_up_proxy"].apply(up_rate_se).to_numpy()
    out["up_rate_next_5m_z_vs_50pct"] = grouped["next_5m_result_up_proxy"].apply(z_vs_half).to_numpy()
    out["warnings"] = out.apply(lambda row: utility_flags(row, min_sample), axis=1)
    return out.sort_values(["candidate_model_id", "n"], ascending=[True, False]).reset_index(drop=True)


def regime_age_utility(frame: pd.DataFrame, min_sample: int) -> pd.DataFrame:
    cols = ["candidate_model_id", "raw_state_id", "canonical_state_label", "regime_age_bucket"]
    out = grouped_utility(frame, cols, min_sample)
    keep = cols + ["n", "up_rate_next_5m", "mean_next_5m_abs_return", "continuation_rate", "reversal_rate", "up_rate_next_5m_se", "up_rate_next_5m_z_vs_50pct", "warnings"]
    return out[[column for column in keep if column in out.columns]]


def reevaluation_trigger_utility(frame: pd.DataFrame, min_sample: int) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame()
    entries = frame[frame["market_age_seconds"] < 120].copy()
    entries["entry_bucket"] = np.where(entries["market_age_seconds"] < 60, "0-60s", "60-120s")
    entry_rows = (
        entries.sort_values("timestamp")
        .groupby(["candidate_model_id", "fold_id", "market_window_start", "entry_bucket"], as_index=False)
        .first()
    )
    entry_rows = entry_rows[
        [
            "candidate_model_id",
            "fold_id",
            "market_window_start",
            "entry_bucket",
            "raw_state_id",
            "canonical_state_label",
            "p_max",
            "close",
        ]
    ].rename(
        columns={
            "raw_state_id": "entry_state",
            "canonical_state_label": "entry_canonical_label",
            "p_max": "entry_p_max",
            "close": "entry_price",
        }
    )
    current = frame[frame["market_age_seconds"] >= 60].copy()
    joined = current.merge(entry_rows, on=["candidate_model_id", "fold_id", "market_window_start"], how="inner")
    joined = joined[joined["timestamp"] > joined["market_window_start"] + pd.to_timedelta(joined["entry_bucket"].map({"0-60s": 0, "60-120s": 60}), unit="s")]
    joined["current_state"] = joined["raw_state_id"]
    joined["state_changed"] = joined["entry_state"] != joined["current_state"]
    joined["p_max_drop"] = joined["entry_p_max"] - joined["p_max"]
    joined["transition_type"] = joined["entry_state"].astype(str) + "->" + joined["current_state"].astype(str)
    joined["trigger_type"] = "no_state_change"
    joined.loc[joined["state_changed"], "trigger_type"] = "state_changed"
    joined.loc[
        joined["state_changed"] & joined["canonical_state_label"].isin(["chop_or_calm_chop", "shock_or_high_vol"]),
        "trigger_type",
    ] = "transition_into_chop_or_high_vol"
    joined.loc[
        joined["state_changed"] & joined["entry_canonical_label"].isin(["trend_up_like", "trend_down_like"]),
        "trigger_type",
    ] = "transition_out_of_trend_like"
    entry_direction = np.sign(joined["entry_price"] - joined["current_window_open_price_proxy"])
    after_direction = np.sign(joined["current_window_end_price_proxy"] - joined["close"])
    joined["continuation_from_entry_direction_indicator"] = np.where(
        (entry_direction != 0) & (after_direction != 0),
        entry_direction == after_direction,
        np.nan,
    )
    joined["adverse_move_indicator"] = np.where(
        (entry_direction != 0) & (after_direction != 0),
        entry_direction != after_direction,
        np.nan,
    )
    group_cols = ["candidate_model_id", "entry_state", "current_state", "transition_type", "trigger_type", "market_age_bucket"]
    grouped = joined.groupby(group_cols, dropna=False)
    out = grouped.agg(
        n=("timestamp", "size"),
        mean_remaining_return_after_trigger=("current_window_remaining_return", "mean"),
        mean_abs_remaining_return_after_trigger=("current_window_remaining_abs_return", "mean"),
        up_rate_after_trigger=("current_window_result_up_proxy", "mean"),
        continuation_from_entry_direction_rate=("continuation_from_entry_direction_indicator", "mean"),
        adverse_move_rate=("adverse_move_indicator", "mean"),
        mean_p_max_drop=("p_max_drop", "mean"),
    ).reset_index()
    out["warnings"] = out.apply(lambda row: "LOW_SAMPLE" if int(row["n"]) < min_sample else "", axis=1)
    return out.sort_values(["candidate_model_id", "n"], ascending=[True, False]).reset_index(drop=True)


def fold_stability_utility(frame: pd.DataFrame, min_sample: int) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame()
    fold = (
        frame.groupby(["candidate_model_id", "raw_state_id", "canonical_state_label", "fold_id"], dropna=False)
        .agg(
            n=("timestamp", "size"),
            up_rate_next_5m=("next_5m_result_up_proxy", "mean"),
            mean_next_5m_return=("next_5m_return", "mean"),
            mean_next_5m_abs_return=("next_5m_abs_return", "mean"),
        )
        .reset_index()
    )
    fold["direction_sign"] = np.sign(fold["up_rate_next_5m"] - 0.5)
    fold["sufficient_sample"] = fold["n"] >= min_sample
    grouped = fold.groupby(["candidate_model_id", "raw_state_id", "canonical_state_label"], dropna=False)
    out = grouped.agg(
        fold_count=("fold_id", "nunique"),
        folds_with_sufficient_samples=("sufficient_sample", "sum"),
        mean_up_rate_next_5m_across_folds=("up_rate_next_5m", "mean"),
        std_up_rate_next_5m_across_folds=("up_rate_next_5m", "std"),
        mean_next_5m_return_across_folds=("mean_next_5m_return", "mean"),
        std_next_5m_return_across_folds=("mean_next_5m_return", "std"),
        mean_next_5m_abs_return_across_folds=("mean_next_5m_abs_return", "mean"),
    ).reset_index()
    agreement = grouped["direction_sign"].apply(lambda values: int(max((values > 0).sum(), (values < 0).sum()))).reset_index(name="folds_where_direction_agrees")
    return out.merge(agreement, on=["candidate_model_id", "raw_state_id", "canonical_state_label"], how="left")


def abstention_candidates(state_utility: pd.DataFrame, transition_utility: pd.DataFrame, min_sample: int) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for _, row in state_utility.iterrows():
        warnings = str(row.get("warnings", ""))
        if "LOW_SAMPLE" in warnings:
            continue
        if "MAGNITUDE_ONLY" in warnings or "WEAK_EDGE" in warnings:
            rows.append(
                {
                    "candidate_model_id": row["candidate_model_id"],
                    "condition": f"state={row['raw_state_id']} label={row['canonical_state_label']} conf={row['confidence_bucket']}",
                    "reason": warnings,
                    "n": row["n"],
                    "up_rate_next_5m": row.get("up_rate_next_5m"),
                    "mean_next_5m_abs_return": row.get("mean_next_5m_abs_return"),
                }
            )
    for _, row in transition_utility.iterrows():
        if int(row.get("n", 0) or 0) >= min_sample and "WEAK_EDGE" in str(row.get("warnings", "")):
            rows.append(
                {
                    "candidate_model_id": row["candidate_model_id"],
                    "condition": f"transition={row['transition_type']} age={row['age_since_switch_bucket']}",
                    "reason": row["warnings"],
                    "n": row["n"],
                    "up_rate_next_5m": row.get("up_rate_next_5m"),
                    "mean_next_5m_abs_return": row.get("mean_next_5m_abs_return"),
                }
            )
    return pd.DataFrame(rows)


def render_summary(
    state_utility: pd.DataFrame,
    transition_utility: pd.DataFrame,
    reevaluation: pd.DataFrame,
    abstention: pd.DataFrame,
    diagnostics: dict[str, Any],
) -> str:
    lines = [
        "BTC-5m HMM regime utility evaluation",
        "",
        "All decision-origin regime fields use filtered/online-available state probabilities reconstructed candidate-only from the regime-health schedule.",
        "Future returns are evaluation labels, not live features. Binance 5-minute proxy labels are not final Polymarket/Chainlink truth.",
        f"per_timestamp_state_source={diagnostics.get('per_timestamp_state_source')}",
        "",
    ]
    if not state_utility.empty:
        directional = state_utility[state_utility["n"] >= diagnostics["min_sample"]].copy()
        directional["edge_abs"] = (directional["up_rate_next_5m"] - 0.5).abs()
        lines.append("Top directional states:")
        for _, row in directional.sort_values("edge_abs", ascending=False).head(10).iterrows():
            lines.append(f"- {row['candidate_model_id']} state={row['raw_state_id']} {row['canonical_state_label']} n={row['n']} up_next={row['up_rate_next_5m']:.3f}")
        lines.append("")
        lines.append("Top magnitude states:")
        for _, row in directional.sort_values("mean_next_5m_abs_return", ascending=False).head(10).iterrows():
            lines.append(f"- {row['candidate_model_id']} state={row['raw_state_id']} n={row['n']} abs_next={row['mean_next_5m_abs_return']:.6f}")
        lines.append("")
    if not transition_utility.empty:
        fresh = transition_utility[(transition_utility["age_since_switch_bucket"].isin(["0m", "1m"])) & (transition_utility["n"] >= diagnostics["min_sample"])].copy()
        fresh["edge_abs"] = (fresh["up_rate_next_5m"] - 0.5).abs()
        lines.append("Most useful fresh transitions:")
        for _, row in fresh.sort_values("edge_abs", ascending=False).head(10).iterrows():
            lines.append(f"- {row['candidate_model_id']} {row['transition_type']} n={row['n']} up_next={row['up_rate_next_5m']:.3f}")
        lines.append("")
    lines.append("Abstention candidates:")
    if abstention.empty:
        lines.append("- none at current thresholds")
    else:
        for _, row in abstention.head(20).iterrows():
            lines.append(f"- {row['candidate_model_id']} {row['condition']} reason={row['reason']} n={row['n']}")
    lines.append("")
    lines.append("Reevaluation triggers:")
    if reevaluation.empty:
        lines.append("- none")
    else:
        useful = reevaluation[reevaluation["n"] >= diagnostics["min_sample"]].sort_values("mean_abs_remaining_return_after_trigger", ascending=False)
        for _, row in useful.head(10).iterrows():
            lines.append(f"- {row['candidate_model_id']} {row['transition_type']} {row['trigger_type']} n={row['n']} abs_remaining={row['mean_abs_remaining_return_after_trigger']:.6f}")
    lines.extend(
        [
            "",
            "Warnings:",
            "- LOW_SAMPLE groups are not ranked as strong candidates.",
            "- Edges that appear in one fold or narrow epoch should be treated as overfit until fold stability is reviewed.",
            "- This is an offline research evaluator only; no live trading behavior was modified.",
        ]
    )
    return "\n".join(lines) + "\n"


def run_evaluation(args: argparse.Namespace) -> dict[str, Any]:
    started = time.perf_counter()
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    candidates = parse_csv_list(args.candidates)
    thresholds = parse_float_csv(args.confidence_thresholds)
    config = {
        "input": str(args.input),
        "regime_health_dir": str(args.regime_health_dir),
        "output_dir": str(output_dir),
        "candidates": candidates,
        "market_window_seconds": args.market_window_seconds,
        "confidence_thresholds": thresholds,
        "min_confidence": args.min_confidence,
        "max_rows": args.max_rows,
        "max_folds": args.max_folds,
        "train_rows_override": args.train_rows,
        "test_rows_override": args.test_rows,
        "step_rows_override": args.step_rows,
        "random_seed": args.random_seed,
        "market_metadata": None if args.market_metadata is None else str(args.market_metadata),
        "price_column": args.price_column,
        "min_sample": args.min_sample,
    }
    (output_dir / "utility_config.json").write_text(json.dumps(config, indent=2, default=str), encoding="utf-8")
    prices = health.load_price_frame(Path(args.input), max_rows=args.max_rows)
    outcomes = assign_market_window_outcomes(prices, args.market_window_seconds)
    per_ts, reconstruction_diag = reconstruct_candidate_states(
        prices,
        Path(args.regime_health_dir),
        candidates,
        thresholds,
        args.max_folds,
        args.random_seed,
        args.train_rows,
        args.test_rows,
        args.step_rows,
    )
    enriched = add_outcomes(per_ts, outcomes)
    enriched["confidence_bucket"] = enriched["p_max"].map(lambda value: confidence_bucket(float(value), thresholds))
    state_cols = ["candidate_model_id", "raw_state_id", "canonical_state_label", "confidence_bucket", "regime_age_bucket", "market_age_bucket"]
    transition_cols = [
        "candidate_model_id",
        "previous_map_state",
        "raw_state_id",
        "transition_type",
        "previous_canonical_label",
        "canonical_state_label",
        "confidence_bucket",
        "market_age_bucket",
        "age_since_switch_bucket",
    ]
    state_utility = grouped_utility(enriched, state_cols, args.min_sample)
    transition_utility = grouped_utility(enriched[enriched["is_transition"]], transition_cols, args.min_sample)
    age_utility = regime_age_utility(enriched, args.min_sample)
    reevaluation = reevaluation_trigger_utility(enriched, args.min_sample)
    stability = fold_stability_utility(enriched, args.min_sample)
    abstention = abstention_candidates(state_utility, transition_utility, args.min_sample)

    outputs = {
        "state_utility_by_candidate.csv": state_utility,
        "transition_utility_by_candidate.csv": transition_utility,
        "regime_age_utility.csv": age_utility,
        "reevaluation_trigger_utility.csv": reevaluation,
        "abstention_candidate_states.csv": abstention,
        "fold_stability_utility.csv": stability,
    }
    for filename, frame in outputs.items():
        frame.to_csv(output_dir / filename, index=False)
        write_optional_parquet(frame, output_dir / filename.replace(".csv", ".parquet"))
    sample = enriched.head(args.per_timestamp_sample_rows) if args.per_timestamp_sample_rows and args.per_timestamp_sample_rows > 0 else enriched
    if not write_optional_parquet(sample, output_dir / "per_timestamp_regime_utility_sample.parquet"):
        sample.to_csv(output_dir / "per_timestamp_regime_utility_sample.csv", index=False)
    diagnostics = {
        **reconstruction_diag,
        "output_dir": str(output_dir),
        "min_sample": args.min_sample,
        "state_utility_rows": int(len(state_utility)),
        "transition_utility_rows": int(len(transition_utility)),
        "regime_age_utility_rows": int(len(age_utility)),
        "reevaluation_rows": int(len(reevaluation)),
        "elapsed_seconds": float(time.perf_counter() - started),
        "market_metadata_note": "market metadata not implemented yet; used UTC 5-minute Binance proxy windows"
        if args.market_metadata is None
        else "market metadata argument accepted but UTC proxy windows are currently used",
    }
    (output_dir / "utility_diagnostics.json").write_text(json.dumps(diagnostics, indent=2, default=str), encoding="utf-8")
    (output_dir / "summary_readme.txt").write_text(
        render_summary(state_utility, transition_utility, reevaluation, abstention, diagnostics),
        encoding="utf-8",
    )
    return diagnostics


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Offline HMM regime utility evaluator for BTC 5-minute proxy outcomes.")
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--regime-health-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--candidates", default=",".join(DEFAULT_CANDIDATES))
    parser.add_argument("--market-window-seconds", type=int, default=300)
    parser.add_argument("--confidence-thresholds", default=",".join(f"{value:.2f}" for value in DEFAULT_CONFIDENCE_THRESHOLDS))
    parser.add_argument("--min-confidence", type=float, default=0.75)
    parser.add_argument("--max-rows", type=int)
    parser.add_argument("--max-folds", type=int, default=2, help="Safety limiter; use 0 to evaluate all folds.")
    parser.add_argument("--train-rows", type=int, help="Optional smoke override for the regime-health train window.")
    parser.add_argument("--test-rows", type=int, help="Optional smoke override for the regime-health test window.")
    parser.add_argument("--step-rows", type=int, help="Optional smoke override for the regime-health step window.")
    parser.add_argument("--random-seed", type=int)
    parser.add_argument("--market-metadata", type=Path)
    parser.add_argument("--price-column")
    parser.add_argument("--min-sample", type=int, default=500)
    parser.add_argument("--per-timestamp-sample-rows", type=int, default=10000)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.max_folds == 0:
        args.max_folds = None
    diagnostics = run_evaluation(args)
    print(json.dumps(diagnostics, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
