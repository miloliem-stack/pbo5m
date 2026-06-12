#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.build_binance_btc5m_research_events import DEFAULT_INPUT_ROOTS, load_binance_1m_klines

SCRIPT_VERSION = "btc5m_candidate_snapshot_table_v1"
SCHEMA_VERSION = "btc5m_candidate_snapshot_schema_v1"

SOURCE_MODES = {"recorder_chainlink", "binance_synthetic"}
DEFAULT_DECISION_FREQUENCY_SEC = 60
DEFAULT_TOP_N_LEVELS = 10

SNAPSHOT_COLUMNS = [
    "market_id",
    "condition_id",
    "decision_ts",
    "market_start_ts",
    "market_end_ts",
    "market_age_sec",
    "seconds_to_expiry",
    "source_mode",
    "price_to_beat",
    "current_price",
    "settlement_price",
    "distance_to_beat",
    "log_distance_to_beat",
    "label_above_beat",
    "label_source",
    "binance_price",
    "chainlink_price",
    "binance_chainlink_basis",
    "yes_ask",
    "no_ask",
    "yes_bid",
    "no_bid",
    "spread",
    "valid_quote_flag",
    "two_sided_quote_flag",
    "yes_depth_top1",
    "no_depth_top1",
    "yes_depth_top3",
    "no_depth_top3",
    "yes_depth_top10",
    "no_depth_top10",
    "quote_ts",
    "quote_age_ms",
    "price_ts",
    "feature_ts",
    "regime_ts",
    "return_30s",
    "return_60s",
    "return_120s",
    "return_180s",
    "return_300s",
    "rv_30s",
    "rv_60s",
    "rv_120s",
    "rv_300s",
    "winsorized_rv_30s",
    "winsorized_rv_120s",
    "max_abs_return_60s",
    "shock_flag",
    "shock_age_sec",
    "sign_flip_rate_60s",
    "sign_flip_rate_180s",
    "trend_strength",
    "chop_score",
    "regime_id",
    "regime_model_id",
    "regime_pmax",
    "regime_probabilities_json",
    "p_brownian_raw",
    "p_struct",
    "p_calibrated",
    "edge_yes",
    "edge_no",
]

CRITICAL_COLUMNS = [
    "decision_ts",
    "market_start_ts",
    "market_end_ts",
    "market_age_sec",
    "seconds_to_expiry",
    "price_to_beat",
    "current_price",
]


@dataclass(frozen=True)
class BuildResult:
    frame: pd.DataFrame
    manifest: dict[str, Any]
    quality: dict[str, Any]


def parse_ts(value: str | None) -> pd.Timestamp | None:
    if value in (None, ""):
        return None
    parsed = pd.to_datetime(value, utc=True, errors="coerce")
    if pd.isna(parsed):
        raise argparse.ArgumentTypeError(f"could not parse timestamp: {value!r}")
    return pd.Timestamp(parsed)


def utc_series(values: pd.Series) -> pd.Series:
    return pd.to_datetime(values, utc=True, errors="coerce").astype("datetime64[ns, UTC]")


def json_default(value: Any) -> Any:
    if isinstance(value, (pd.Timestamp,)):
        return value.isoformat()
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        if np.isnan(value):
            return None
        return float(value)
    if isinstance(value, (np.bool_,)):
        return bool(value)
    if pd.isna(value):
        return None
    return str(value)


def bool_arg(value: str | bool) -> bool:
    if isinstance(value, bool):
        return value
    lower = str(value).strip().lower()
    if lower in {"1", "true", "yes", "y", "on"}:
        return True
    if lower in {"0", "false", "no", "n", "off"}:
        return False
    raise argparse.ArgumentTypeError(f"expected bool, got {value!r}")


def git_revision() -> str | None:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
        )
    except Exception:
        return None
    return result.stdout.strip() or None


def require_columns(frame: pd.DataFrame, columns: set[str], name: str) -> None:
    missing = sorted(columns - set(frame.columns))
    if missing:
        raise RuntimeError(f"{name} missing required columns: {missing}")


def normalize_klines(klines: pd.DataFrame) -> pd.DataFrame:
    out = klines.copy()
    require_columns(out, {"event_time", "close"}, "Binance klines")
    out["event_time"] = utc_series(out["event_time"])
    out["close"] = pd.to_numeric(out["close"], errors="coerce")
    out = out.dropna(subset=["event_time", "close"]).sort_values("event_time").drop_duplicates("event_time", keep="last")
    if out.empty:
        raise RuntimeError("Binance klines are empty after timestamp/price normalization")
    return out.reset_index(drop=True)


def build_price_feature_frame(klines: pd.DataFrame) -> pd.DataFrame:
    price = normalize_klines(klines)[["event_time", "close"]].rename(columns={"event_time": "feature_ts", "close": "binance_price"})
    log_close = np.log(price["binance_price"].astype(float))
    log_ret = log_close.diff()

    price["return_30s"] = np.nan
    price["return_60s"] = log_close - log_close.shift(1)
    price["return_120s"] = log_close - log_close.shift(2)
    price["return_180s"] = log_close - log_close.shift(3)
    price["return_300s"] = log_close - log_close.shift(5)
    price["rv_30s"] = np.nan
    price["rv_60s"] = log_ret.rolling(2, min_periods=2).std(ddof=0)
    price["rv_120s"] = log_ret.rolling(2, min_periods=2).std(ddof=0)
    price["rv_300s"] = log_ret.rolling(5, min_periods=2).std(ddof=0)
    price["rv_180s"] = log_ret.rolling(3, min_periods=2).std(ddof=0)

    rolling_mean = log_ret.rolling(120, min_periods=10).mean()
    rolling_std = log_ret.rolling(120, min_periods=10).std(ddof=0)
    clipped_ret = log_ret.clip(lower=rolling_mean - 3.0 * rolling_std, upper=rolling_mean + 3.0 * rolling_std)
    price["winsorized_rv_30s"] = np.nan
    price["winsorized_rv_120s"] = clipped_ret.rolling(2, min_periods=2).std(ddof=0)
    price["max_abs_return_60s"] = log_ret.abs()

    rv_floor = price["rv_300s"].replace(0.0, np.nan)
    price["shock_flag"] = (price["return_300s"].abs() > 3.0 * rv_floor).fillna(False)
    shock_index = np.where(price["shock_flag"].to_numpy(), np.arange(len(price)), np.nan)
    last_shock_index = pd.Series(shock_index).ffill()
    price["shock_age_sec"] = (np.arange(len(price)) - last_shock_index) * 60.0
    price.loc[last_shock_index.isna(), "shock_age_sec"] = np.nan

    signs = np.sign(log_ret)
    flips = (signs != signs.shift(1)) & signs.notna() & signs.shift(1).notna()
    price["sign_flip_rate_60s"] = flips.rolling(2, min_periods=1).mean()
    price["sign_flip_rate_180s"] = flips.rolling(3, min_periods=1).mean()
    price["trend_strength"] = (price["return_300s"].abs() / (price["rv_300s"] + 1e-12)).replace([np.inf, -np.inf], np.nan)
    price["chop_score"] = price["sign_flip_rate_180s"]
    price["price_ts"] = price["feature_ts"]
    return price


def attach_price_features(decisions: pd.DataFrame, klines: pd.DataFrame | None) -> pd.DataFrame:
    out = decisions.copy()
    if klines is None or klines.empty:
        for column in [
            "binance_price",
            "price_ts",
            "feature_ts",
            "return_30s",
            "return_60s",
            "return_120s",
            "return_180s",
            "return_300s",
            "rv_30s",
            "rv_60s",
            "rv_120s",
            "rv_300s",
            "winsorized_rv_30s",
            "winsorized_rv_120s",
            "max_abs_return_60s",
            "shock_flag",
            "shock_age_sec",
            "sign_flip_rate_60s",
            "sign_flip_rate_180s",
            "trend_strength",
            "chop_score",
        ]:
            out[column] = np.nan
        return out

    features = build_price_feature_frame(klines).sort_values("feature_ts")
    out = pd.merge_asof(
        out.sort_values("decision_ts"),
        features,
        left_on="decision_ts",
        right_on="feature_ts",
        direction="backward",
        allow_exact_matches=True,
    ).sort_index()
    future = out["feature_ts"].notna() & (out["feature_ts"] > out["decision_ts"])
    if future.any():
        raise RuntimeError(f"future Binance feature join detected: {int(future.sum())} rows")
    return out


def depth_usd(frame: pd.DataFrame, top_n: int, prefix: str = "ask") -> pd.Series:
    total = pd.Series(0.0, index=frame.index, dtype=float)
    for level in range(1, top_n + 1):
        px_col = f"{prefix}_px_{level}"
        sz_col = f"{prefix}_sz_{level}"
        if px_col not in frame.columns or sz_col not in frame.columns:
            continue
        total += pd.to_numeric(frame[px_col], errors="coerce").fillna(0.0) * pd.to_numeric(frame[sz_col], errors="coerce").fillna(0.0)
    return total


def load_compact(compact_root: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    windows_path = compact_root / "market_windows.parquet"
    ticks_path = compact_root / "book_ticks.parquet"
    if not windows_path.exists():
        raise FileNotFoundError(f"missing compact recorder market windows: {windows_path}")
    if not ticks_path.exists():
        raise FileNotFoundError(f"missing compact recorder book ticks: {ticks_path}")
    windows = pd.read_parquet(windows_path)
    ticks = pd.read_parquet(ticks_path)
    require_columns(windows, {"market_key", "condition_id", "market_start_ts", "market_end_ts", "reference_price"}, "market_windows.parquet")
    require_columns(ticks, {"market_key", "ts", "side", "ask_px_1", "bid_px_1"}, "book_ticks.parquet")
    windows["market_start_ts"] = utc_series(windows["market_start_ts"])
    windows["market_end_ts"] = utc_series(windows["market_end_ts"])
    ticks["ts"] = utc_series(ticks["ts"])
    ticks["side"] = ticks["side"].astype(str).str.upper()
    return windows, ticks


def _first_existing(frame: pd.DataFrame, candidates: list[str], default: Any = np.nan) -> pd.Series:
    for column in candidates:
        if column in frame.columns:
            return frame[column]
    return pd.Series(default, index=frame.index)


def build_recorder_chainlink(
    *,
    compact_root: Path,
    binance_roots: list[Path],
    start_ts: pd.Timestamp | None,
    end_ts: pd.Timestamp | None,
    valid_topbook_only: bool,
    top_n_levels: int,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    windows, ticks = load_compact(compact_root)
    raw_tick_rows = int(len(ticks))
    if valid_topbook_only and "is_valid_topbook" in ticks.columns:
        ticks = ticks[ticks["is_valid_topbook"].fillna(False)].copy()
    if start_ts is not None:
        ticks = ticks[ticks["ts"] >= start_ts].copy()
    if end_ts is not None:
        ticks = ticks[ticks["ts"] < end_ts].copy()

    for n in (1, 3, 10):
        ticks[f"depth_top{n}"] = depth_usd(ticks, min(n, top_n_levels))

    base_cols = [
        "market_key",
        "ts",
        "side",
        "ask_px_1",
        "bid_px_1",
        "spread",
        "is_valid_topbook",
        "depth_top1",
        "depth_top3",
        "depth_top10",
    ]
    keep = [column for column in base_cols if column in ticks.columns]
    side_ticks = ticks[keep].copy()
    duplicate_side_rows = int(side_ticks.duplicated(["market_key", "ts", "side"]).sum())
    side_ticks = side_ticks.sort_values(["market_key", "ts", "side"]).drop_duplicates(["market_key", "ts", "side"], keep="last")
    yes = side_ticks[side_ticks["side"].eq("YES")].drop(columns=["side"]).rename(
        columns={
            "ask_px_1": "yes_ask",
            "bid_px_1": "yes_bid",
            "spread": "yes_spread",
            "is_valid_topbook": "yes_valid_topbook",
            "depth_top1": "yes_depth_top1",
            "depth_top3": "yes_depth_top3",
            "depth_top10": "yes_depth_top10",
        }
    )
    no = side_ticks[side_ticks["side"].eq("NO")].drop(columns=["side"]).rename(
        columns={
            "ask_px_1": "no_ask",
            "bid_px_1": "no_bid",
            "spread": "no_spread",
            "is_valid_topbook": "no_valid_topbook",
            "depth_top1": "no_depth_top1",
            "depth_top3": "no_depth_top3",
            "depth_top10": "no_depth_top10",
        }
    )
    quote = yes.merge(no, on=["market_key", "ts"], how="outer")

    meta_cols = [
        "market_key",
        "market_id",
        "condition_id",
        "slug",
        "market_start_ts",
        "market_end_ts",
        "reference_price",
        "chainlink_close_price",
        "label_up",
        "winner_side",
    ]
    available_meta = [column for column in meta_cols if column in windows.columns]
    out = quote.merge(windows[available_meta].drop_duplicates("market_key"), on="market_key", how="left")
    out = out.rename(columns={"ts": "decision_ts", "slug": "market_slug"})
    out["source_mode"] = "recorder_chainlink"
    out["market_age_sec"] = (out["decision_ts"] - out["market_start_ts"]).dt.total_seconds()
    out["seconds_to_expiry"] = (out["market_end_ts"] - out["decision_ts"]).dt.total_seconds()
    out["price_to_beat"] = pd.to_numeric(out["reference_price"], errors="coerce")
    out["settlement_price"] = pd.to_numeric(_first_existing(out, ["chainlink_close_price"]), errors="coerce")
    out["label_above_beat"] = _first_existing(out, ["label_up"], default=np.nan)
    if "winner_side" in out.columns:
        out["label_above_beat"] = out["label_above_beat"].where(out["label_above_beat"].notna(), out["winner_side"].astype(str).str.upper().eq("YES"))
    out["label_source"] = np.where(out["label_above_beat"].notna(), "chainlink", "unknown")
    out["quote_ts"] = out["decision_ts"]
    out["quote_age_ms"] = 0.0
    out["valid_quote_flag"] = out.get("yes_valid_topbook", True).fillna(False) & out.get("no_valid_topbook", True).fillna(False)
    out["two_sided_quote_flag"] = out[["yes_ask", "no_ask", "yes_bid", "no_bid"]].notna().all(axis=1)
    out["spread"] = pd.to_numeric(out.get("yes_spread"), errors="coerce").combine_first(pd.to_numeric(out.get("no_spread"), errors="coerce"))
    out["chainlink_price"] = np.nan
    out["regime_ts"] = pd.NaT

    klines = load_optional_binance(binance_roots, start_ts, end_ts)
    out = attach_price_features(out, klines)
    out["current_price"] = out["binance_price"]
    out["distance_to_beat"] = out["current_price"] - out["price_to_beat"]
    out["log_distance_to_beat"] = np.log(out["current_price"] / out["price_to_beat"])
    out["binance_chainlink_basis"] = out["binance_price"] - out["chainlink_price"]
    out["market_id"] = out.get("market_id").astype("string") if "market_id" in out.columns else out["condition_id"].astype("string")
    out["condition_id"] = out.get("condition_id").astype("string")

    diagnostics = {
        "compact_root": str(compact_root),
        "raw_book_tick_rows": raw_tick_rows,
        "book_tick_rows_after_filters": int(len(ticks)),
        "duplicate_side_tick_rows_dropped": duplicate_side_rows,
        "market_window_rows": int(len(windows)),
        "binance_feature_rows": int(0 if klines is None else len(klines)),
    }
    return finalize_snapshot_frame(out), diagnostics


def load_optional_binance(binance_roots: list[Path], start_ts: pd.Timestamp | None, end_ts: pd.Timestamp | None) -> pd.DataFrame | None:
    try:
        loaded = load_binance_1m_klines(binance_roots)
    except RuntimeError:
        return None
    klines = loaded.frame
    if start_ts is not None:
        klines = klines[klines["event_time"] >= start_ts - pd.Timedelta(hours=2)].copy()
    if end_ts is not None:
        klines = klines[klines["event_time"] <= end_ts].copy()
    return klines.reset_index(drop=True)


def build_binance_synthetic(
    *,
    binance_roots: list[Path],
    start_ts: pd.Timestamp | None,
    end_ts: pd.Timestamp | None,
    decision_frequency_sec: int,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    loaded = load_binance_1m_klines(binance_roots)
    klines = normalize_klines(loaded.frame)
    if start_ts is not None:
        klines = klines[klines["event_time"] >= start_ts - pd.Timedelta(minutes=10)].copy()
    if end_ts is not None:
        klines = klines[klines["event_time"] <= end_ts + pd.Timedelta(minutes=5)].copy()
    if klines.empty:
        raise RuntimeError("no Binance klines remain after requested date range")

    prices = klines[["event_time", "close"]].rename(columns={"event_time": "decision_ts", "close": "current_price"}).copy()
    prices["market_start_ts"] = prices["decision_ts"].dt.floor("5min")
    prices["market_end_ts"] = prices["market_start_ts"] + pd.Timedelta(minutes=5)
    window_ref = prices.sort_values("decision_ts").groupby("market_start_ts", as_index=False).first()[["market_start_ts", "current_price"]]
    window_ref = window_ref.rename(columns={"current_price": "price_to_beat"})
    settle_source = klines[["event_time", "close"]].rename(columns={"event_time": "settlement_source_ts", "close": "settlement_price"}).sort_values("settlement_source_ts")
    windows = window_ref.copy()
    windows["market_end_ts"] = windows["market_start_ts"] + pd.Timedelta(minutes=5)
    windows = pd.merge_asof(
        windows.sort_values("market_end_ts"),
        settle_source,
        left_on="market_end_ts",
        right_on="settlement_source_ts",
        direction="backward",
        allow_exact_matches=True,
    )
    windows = windows[windows["settlement_source_ts"].notna()].copy()
    windows["condition_id"] = windows["market_start_ts"].dt.strftime("binance_synth_btc5m_%Y%m%dT%H%M%SZ")
    windows["market_id"] = windows["condition_id"]

    out = prices.merge(windows[["market_start_ts", "price_to_beat", "settlement_price", "condition_id", "market_id"]], on="market_start_ts", how="inner")
    if decision_frequency_sec > 60:
        epoch = out["decision_ts"].astype("int64") // 1_000_000_000
        out = out[(epoch % decision_frequency_sec).eq(0)].copy()
    if start_ts is not None:
        out = out[out["decision_ts"] >= start_ts].copy()
    if end_ts is not None:
        out = out[out["decision_ts"] < end_ts].copy()
    out["source_mode"] = "binance_synthetic"
    out["market_age_sec"] = (out["decision_ts"] - out["market_start_ts"]).dt.total_seconds()
    out["seconds_to_expiry"] = (out["market_end_ts"] - out["decision_ts"]).dt.total_seconds()
    out = out[(out["market_age_sec"] >= 0) & (out["seconds_to_expiry"] >= 0)].copy()
    out["distance_to_beat"] = out["current_price"] - out["price_to_beat"]
    out["log_distance_to_beat"] = np.log(out["current_price"] / out["price_to_beat"])
    out["label_above_beat"] = out["settlement_price"] > out["price_to_beat"]
    out["label_source"] = "binance_proxy"
    out["binance_price"] = out["current_price"]
    out["chainlink_price"] = np.nan
    out["binance_chainlink_basis"] = np.nan
    out["quote_ts"] = pd.NaT
    out["quote_age_ms"] = np.nan
    out["valid_quote_flag"] = np.nan
    out["two_sided_quote_flag"] = np.nan
    out["regime_ts"] = pd.NaT
    for column in [
        "yes_ask",
        "no_ask",
        "yes_bid",
        "no_bid",
        "spread",
        "yes_depth_top1",
        "no_depth_top1",
        "yes_depth_top3",
        "no_depth_top3",
        "yes_depth_top10",
        "no_depth_top10",
    ]:
        out[column] = np.nan
    out = attach_price_features(out.drop(columns=["binance_price"], errors="ignore"), klines)
    out["current_price"] = out["binance_price"]
    out["distance_to_beat"] = out["current_price"] - out["price_to_beat"]
    out["log_distance_to_beat"] = np.log(out["current_price"] / out["price_to_beat"])

    diagnostics = {
        "input_files": loaded.files,
        "rows_loaded": loaded.rows_loaded,
        "rows_after_dedup": loaded.rows_after_dedup,
        "duplicate_input_klines": loaded.duplicate_count,
        "gap_summary": loaded.gap_summary[:20],
        "decision_frequency_sec": decision_frequency_sec,
    }
    return finalize_snapshot_frame(out), diagnostics


def finalize_snapshot_frame(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    placeholders = [
        "regime_id",
        "regime_model_id",
        "regime_pmax",
        "regime_probabilities_json",
        "p_brownian_raw",
        "p_struct",
        "p_calibrated",
        "edge_yes",
        "edge_no",
    ]
    for column in placeholders:
        if column not in out.columns:
            out[column] = np.nan
    for column in SNAPSHOT_COLUMNS:
        if column not in out.columns:
            out[column] = np.nan
    out = out[SNAPSHOT_COLUMNS].copy()
    for column in ["decision_ts", "market_start_ts", "market_end_ts", "quote_ts", "price_ts", "feature_ts", "regime_ts"]:
        out[column] = utc_series(out[column])
    numeric_columns = [
        "market_age_sec",
        "seconds_to_expiry",
        "price_to_beat",
        "current_price",
        "settlement_price",
        "distance_to_beat",
        "log_distance_to_beat",
        "binance_price",
        "chainlink_price",
        "binance_chainlink_basis",
        "yes_ask",
        "no_ask",
        "yes_bid",
        "no_bid",
        "spread",
        "yes_depth_top1",
        "no_depth_top1",
        "yes_depth_top3",
        "no_depth_top3",
        "yes_depth_top10",
        "no_depth_top10",
        "quote_age_ms",
        "return_30s",
        "return_60s",
        "return_120s",
        "return_180s",
        "return_300s",
        "rv_30s",
        "rv_60s",
        "rv_120s",
        "rv_300s",
        "winsorized_rv_30s",
        "winsorized_rv_120s",
        "max_abs_return_60s",
        "shock_age_sec",
        "sign_flip_rate_60s",
        "sign_flip_rate_180s",
        "trend_strength",
        "chop_score",
        "regime_pmax",
        "p_brownian_raw",
        "p_struct",
        "p_calibrated",
        "edge_yes",
        "edge_no",
    ]
    for column in numeric_columns:
        out[column] = pd.to_numeric(out[column], errors="coerce")
    out = out.sort_values(["market_start_ts", "decision_ts", "condition_id"], na_position="last").reset_index(drop=True)
    impossible = (out["market_age_sec"] < -1e-9) | (out["seconds_to_expiry"] < -1e-9)
    if impossible.any():
        raise RuntimeError(f"impossible market timing in snapshot table: {int(impossible.sum())} rows")
    future_violations = count_future_timestamp_violations(out)
    if future_violations:
        raise RuntimeError(f"future timestamp leakage detected: {future_violations} rows")
    return out


def count_future_timestamp_violations(frame: pd.DataFrame) -> int:
    total = 0
    for column in ("price_ts", "feature_ts", "quote_ts", "regime_ts"):
        if column not in frame.columns:
            continue
        mask = frame[column].notna() & (frame[column] > frame["decision_ts"])
        total += int(mask.sum())
    return total


def duplicate_key_count(frame: pd.DataFrame) -> int:
    keys = ["condition_id", "decision_ts"]
    if frame["source_mode"].eq("binance_synthetic").all():
        keys = ["market_id", "decision_ts"]
    return int(frame.duplicated(keys).sum())


def build_quality_summary(frame: pd.DataFrame) -> dict[str, Any]:
    row_count = int(len(frame))
    unique_market_count = int(frame["condition_id"].nunique(dropna=True)) if "condition_id" in frame.columns else 0
    missing = {column: int(frame[column].isna().sum()) for column in CRITICAL_COLUMNS if column in frame.columns}
    source_counts = frame["source_mode"].value_counts(dropna=False).to_dict() if row_count else {}
    return {
        "row_count": row_count,
        "unique_market_count": unique_market_count,
        "min_decision_ts": None if row_count == 0 else frame["decision_ts"].min().isoformat(),
        "max_decision_ts": None if row_count == 0 else frame["decision_ts"].max().isoformat(),
        "rows_by_source_mode": {str(k): int(v) for k, v in source_counts.items()},
        "rows_with_chainlink_label": int(frame["label_source"].eq("chainlink").sum()) if row_count else 0,
        "rows_with_binance_proxy_label": int(frame["label_source"].eq("binance_proxy").sum()) if row_count else 0,
        "quote_coverage_rate": float(frame[["yes_ask", "no_ask"]].notna().any(axis=1).mean()) if row_count else 0.0,
        "valid_quote_rate": float(frame["valid_quote_flag"].fillna(False).astype(bool).mean()) if row_count else 0.0,
        "two_sided_quote_rate": float(frame["two_sided_quote_flag"].fillna(False).astype(bool).mean()) if row_count else 0.0,
        "missing_critical_column_counts": missing,
        "decision_ts_feature_ts_violation_count": count_future_timestamp_violations(frame),
        "duplicate_key_count": duplicate_key_count(frame),
    }


def build_schema(frame: pd.DataFrame) -> dict[str, Any]:
    descriptions = {
        "decision_ts": "Causal decision timestamp. All feature/price/quote/regime timestamps must be <= this value.",
        "price_to_beat": "Market reference price used for the up/down settlement threshold.",
        "label_above_beat": "Final label only; not permitted as a policy/model input feature.",
        "source_mode": "recorder_chainlink for compact recorder rows, binance_synthetic for long-history proxy rows.",
        "quote_ts": "Quote timestamp, populated in recorder mode when available.",
        "feature_ts": "Timestamp of latest previous-only Binance feature row.",
    }
    return {
        "schema_version": SCHEMA_VERSION,
        "columns": [
            {
                "name": column,
                "dtype": str(frame[column].dtype) if column in frame.columns else "missing",
                "description": descriptions.get(column, ""),
            }
            for column in SNAPSHOT_COLUMNS
        ],
        "causality_rules": [
            "Feature, price, quote, and regime timestamps must be less than or equal to decision_ts.",
            "Settlement and label fields are evaluation-only and must not be used for selection or model inputs.",
            "Recorder labels use Chainlink-aligned market_windows fields; Binance synthetic labels are proxy labels.",
        ],
    }


def write_readme(output_dir: Path, manifest: dict[str, Any]) -> None:
    text = f"""BTC-5m Candidate Snapshot Table
================================

Generated by `{SCRIPT_VERSION}`.

This artifact standardizes causal BTC-5m decision snapshots for probability-model
research. It does not train models and does not touch live execution state.

Primary table:
- `candidate_snapshots.parquet`

Metadata:
- `output_schema.json`
- `run_manifest.json`
- `data_quality_summary.json`

Causality rules:
- `price_ts <= decision_ts`
- `feature_ts <= decision_ts`
- `quote_ts <= decision_ts`
- `regime_ts <= decision_ts`
- Label and settlement columns are evaluation-only.

Source mode for this run: `{manifest.get("source_mode")}`

Recorder/Chainlink example:

```bash
.venv/bin/python scripts/build_btc5m_candidate_snapshot_table.py \\
  --source-mode recorder_chainlink \\
  --compact-root artifacts/compact_market_recorder/2026-04-23_to_2026-05-28_depth10 \\
  --output-dir artifacts/candidate_snapshots/recorder_chainlink_20260423_20260528 \\
  --top-n-levels 10 \\
  --overwrite
```

Binance synthetic example:

```bash
.venv/bin/python scripts/build_btc5m_candidate_snapshot_table.py \\
  --source-mode binance_synthetic \\
  --binance-root data/binance-btc1m \\
  --output-dir artifacts/candidate_snapshots/binance_synthetic_btc5m \\
  --overwrite
```

Small smoke example:

```bash
.venv/bin/python scripts/build_btc5m_candidate_snapshot_table.py \\
  --source-mode recorder_chainlink \\
  --compact-root artifacts/compact_market_recorder/2026-04-23_to_2026-05-28_depth10 \\
  --output-dir /tmp/btc5m_candidate_snapshot_smoke \\
  --start-ts 2026-05-01T00:00:00Z \\
  --end-ts 2026-05-01T01:00:00Z \\
  --overwrite
```
"""
    (output_dir / "README.txt").write_text(text, encoding="utf-8")


def build_manifest(args: argparse.Namespace, diagnostics: dict[str, Any], quality: dict[str, Any]) -> dict[str, Any]:
    return {
        "script_version": SCRIPT_VERSION,
        "schema_version": SCHEMA_VERSION,
        "git_revision": git_revision(),
        "source_mode": args.source_mode,
        "created_at": pd.Timestamp.now(tz="UTC").isoformat(),
        "inputs": {
            "compact_root": str(args.compact_root) if args.compact_root else None,
            "binance_roots": [str(path) for path in args.binance_root],
        },
        "date_range": {
            "start_ts": None if args.start_ts is None else args.start_ts.isoformat(),
            "end_ts": None if args.end_ts is None else args.end_ts.isoformat(),
        },
        "feature_settings": {
            "top_n_levels": args.top_n_levels,
            "valid_topbook_only": args.valid_topbook_only,
            "decision_frequency_sec": args.decision_frequency_sec,
        },
        "diagnostics": diagnostics,
        "quality_summary": quality,
    }


def write_outputs(frame: pd.DataFrame, output_dir: Path, manifest: dict[str, Any], quality: dict[str, Any], overwrite: bool) -> None:
    if output_dir.exists() and any(output_dir.iterdir()) and not overwrite:
        raise FileExistsError(f"output directory already exists and is non-empty: {output_dir}; pass --overwrite")
    output_dir.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(output_dir / "candidate_snapshots.parquet", index=False)
    (output_dir / "output_schema.json").write_text(json.dumps(build_schema(frame), indent=2, default=json_default), encoding="utf-8")
    (output_dir / "run_manifest.json").write_text(json.dumps(manifest, indent=2, default=json_default), encoding="utf-8")
    (output_dir / "data_quality_summary.json").write_text(json.dumps(quality, indent=2, default=json_default), encoding="utf-8")
    write_readme(output_dir, manifest)


def build(args: argparse.Namespace) -> BuildResult:
    if args.source_mode == "recorder_chainlink":
        if args.compact_root is None:
            raise RuntimeError("--compact-root is required for recorder_chainlink mode")
        frame, diagnostics = build_recorder_chainlink(
            compact_root=args.compact_root,
            binance_roots=args.binance_root,
            start_ts=args.start_ts,
            end_ts=args.end_ts,
            valid_topbook_only=args.valid_topbook_only,
            top_n_levels=args.top_n_levels,
        )
    elif args.source_mode == "binance_synthetic":
        frame, diagnostics = build_binance_synthetic(
            binance_roots=args.binance_root,
            start_ts=args.start_ts,
            end_ts=args.end_ts,
            decision_frequency_sec=args.decision_frequency_sec,
        )
    else:
        raise RuntimeError(f"unsupported source mode: {args.source_mode}")
    quality = build_quality_summary(frame)
    manifest = build_manifest(args, diagnostics, quality)
    return BuildResult(frame=frame, manifest=manifest, quality=quality)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build canonical causal BTC-5m candidate snapshot tables.")
    parser.add_argument("--source-mode", choices=sorted(SOURCE_MODES), required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--compact-root", type=Path)
    parser.add_argument("--binance-root", type=Path, action="append", default=None)
    parser.add_argument("--start-ts", type=parse_ts)
    parser.add_argument("--end-ts", type=parse_ts)
    parser.add_argument("--valid-topbook-only", type=bool_arg, default=True)
    parser.add_argument("--top-n-levels", type=int, default=DEFAULT_TOP_N_LEVELS)
    parser.add_argument("--decision-frequency-sec", type=int, default=DEFAULT_DECISION_FREQUENCY_SEC)
    parser.add_argument("--overwrite", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    args.binance_root = args.binance_root or DEFAULT_INPUT_ROOTS
    result = build(args)
    write_outputs(result.frame, args.output_dir, result.manifest, result.quality, args.overwrite)
    print(json.dumps({"output_dir": str(args.output_dir), "quality_summary": result.quality}, indent=2, default=json_default))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
