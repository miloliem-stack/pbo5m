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

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

DEFAULT_TARGET_LEVELS = [0.45, 0.46, 0.47, 0.48]
DEFAULT_TIMEOUT_WINDOWS_SEC = [5, 15, 30, 60]
DEFAULT_OUTPUT_DIR = Path("artifacts/quiet_regime_pair_replay/legacy_nearest_2s_v1")
QUOTE_LIMITATION_WARNING = (
    "Offline quote replay is an opportunity proxy, not a fill simulation. It does not model "
    "queue priority, fillability, adverse selection, order priority, fees, or orphan-leg execution risk."
)
NO_PROFIT_WARNING = "No orders are placed and no profitability is claimed."


def parse_float_list(value: str) -> list[float]:
    values = [float(part.strip()) for part in value.split(",") if part.strip()]
    if not values:
        raise argparse.ArgumentTypeError("list must not be empty")
    return values


def _safe_float(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if np.isfinite(result) else None


def normalize_quote_price(value: Any) -> float | None:
    price = _safe_float(value)
    if price is None:
        return None
    if 1.0 < price <= 100.0:
        return float(price / 100.0)
    return price


def _bool_from_any(value: Any) -> bool | None:
    if pd.isna(value):
        return None
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"true", "1", "1.0", "yes"}:
        return True
    if text in {"false", "0", "0.0", "no"}:
        return False
    return None


def load_hmm_assignments(path: Path, quiet_state: int) -> pd.DataFrame:
    frame = pd.read_csv(path)
    frame["event_start_time"] = pd.to_datetime(frame["event_start_time"], utc=True)
    frame["event_end_time"] = pd.to_datetime(frame["event_end_time"], utc=True)
    frame = frame.sort_values("event_start_time").reset_index(drop=True)
    frame["is_quiet_market"] = frame["assigned_state"].astype(int) == int(quiet_state)
    frame["is_post_confirmation_quiet_market"] = post_confirmation_quiet_mask(frame["assigned_state"], quiet_state)
    return frame


def post_confirmation_quiet_mask(assignments: pd.Series | list[int], quiet_state: int) -> pd.Series:
    states = pd.Series(assignments).astype(int).reset_index(drop=True)
    quiet = states == int(quiet_state)
    previous_quiet = quiet.shift(1, fill_value=False)
    return quiet & previous_quiet


def load_event_set(path: Path) -> pd.DataFrame:
    frame = pd.read_csv(path)
    for column in ("market_start_time", "market_end_time", "quote_ts"):
        if column in frame.columns:
            frame[column] = pd.to_datetime(frame[column], utc=True, errors="coerce")
    if "market_id" in frame.columns:
        frame["market_id"] = frame["market_id"].astype(str)
    return frame


def quote_paths(input_roots: list[Path]) -> list[Path]:
    paths: list[Path] = []
    for root in input_roots:
        if root.is_file() and root.name == "market_quotes.jsonl":
            paths.append(root)
        elif root.exists():
            paths.extend(sorted(root.glob("market_quotes.jsonl")))
            paths.extend(sorted(root.glob("*/*/market_quotes.jsonl")))
    return list(dict.fromkeys(paths))


def _quote_side(row: dict[str, Any], side: str, key: str) -> Any:
    side_payload = row.get(side)
    if isinstance(side_payload, dict):
        return side_payload.get(key)
    return None


def _raw_last_trade_price(payload: dict[str, Any], side: str) -> float | None:
    raw = payload.get("raw_payload_fragment", {})
    if not isinstance(raw, dict):
        return None
    side_raw = raw.get(f"{side}_raw", {})
    if not isinstance(side_raw, dict):
        return None
    book = side_raw.get("book", side_raw)
    if not isinstance(book, dict):
        return None
    return normalize_quote_price(book.get("last_trade_price") or book.get("lastTradePrice") or book.get("last_price"))


def load_quote_snapshots(input_roots: list[Path]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for path in quote_paths(input_roots):
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                if not line.strip():
                    continue
                try:
                    payload = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if payload.get("record_type") != "quote_snapshot":
                    continue
                rows.append(flatten_quote_snapshot(payload, str(path)))
    if not rows:
        return pd.DataFrame(columns=quote_columns())
    frame = pd.DataFrame(rows)
    frame["quote_ts"] = pd.to_datetime(frame["quote_ts"], utc=True, errors="coerce")
    frame["market_start_time"] = pd.to_datetime(frame["market_start_time"], utc=True, errors="coerce")
    frame["market_end_time"] = pd.to_datetime(frame["market_end_time"], utc=True, errors="coerce")
    frame["market_id"] = frame["market_id"].astype(str)
    return frame.dropna(subset=["quote_ts"])


def quote_columns() -> list[str]:
    return [
        "quote_ts",
        "market_id",
        "slug",
        "market_start_time",
        "market_end_time",
        "token_yes",
        "token_no",
        "quote_capture_ok",
        "quote_capture_status",
        "yes_bid",
        "yes_ask",
        "no_bid",
        "no_ask",
        "yes_mid",
        "no_mid",
        "yes_last_trade",
        "no_last_trade",
        "yes_age_seconds",
        "no_age_seconds",
        "source_file",
    ]


def flatten_quote_snapshot(payload: dict[str, Any], source_file: str) -> dict[str, Any]:
    return {
        "quote_ts": payload.get("ts"),
        "market_id": payload.get("market_id"),
        "slug": payload.get("slug"),
        "market_start_time": payload.get("market_start_time"),
        "market_end_time": payload.get("market_end_time"),
        "token_yes": payload.get("token_yes"),
        "token_no": payload.get("token_no"),
        "quote_capture_ok": payload.get("quote_capture_ok"),
        "quote_capture_status": payload.get("quote_capture_status"),
        "yes_bid": normalize_quote_price(_quote_side(payload, "yes", "best_bid")),
        "yes_ask": normalize_quote_price(_quote_side(payload, "yes", "best_ask")),
        "no_bid": normalize_quote_price(_quote_side(payload, "no", "best_bid")),
        "no_ask": normalize_quote_price(_quote_side(payload, "no", "best_ask")),
        "yes_mid": normalize_quote_price(_quote_side(payload, "yes", "mid")),
        "no_mid": normalize_quote_price(_quote_side(payload, "no", "mid")),
        "yes_last_trade": _raw_last_trade_price(payload, "yes"),
        "no_last_trade": _raw_last_trade_price(payload, "no"),
        "yes_age_seconds": _safe_float(_quote_side(payload, "yes", "age_seconds")),
        "no_age_seconds": _safe_float(_quote_side(payload, "no", "age_seconds")),
        "source_file": source_file,
    }


def fallback_quote_from_event(event: pd.Series) -> pd.DataFrame:
    if "quote_ts" not in event or pd.isna(event["quote_ts"]):
        return pd.DataFrame(columns=quote_columns())
    return pd.DataFrame(
        [
            {
                "quote_ts": event.get("quote_ts"),
                "market_id": str(event.get("market_id")),
                "slug": event.get("slug"),
                "market_start_time": event.get("market_start_time"),
                "market_end_time": event.get("market_end_time"),
                "token_yes": event.get("token_yes"),
                "token_no": event.get("token_no"),
                "quote_capture_ok": event.get("quote_capture_ok"),
                "quote_capture_status": event.get("quote_capture_status"),
                "yes_bid": normalize_quote_price(event.get("yes_bid")),
                "yes_ask": normalize_quote_price(event.get("yes_ask")),
                "no_bid": normalize_quote_price(event.get("no_bid")),
                "no_ask": normalize_quote_price(event.get("no_ask")),
                "yes_mid": normalize_quote_price(event.get("yes_mid")),
                "no_mid": normalize_quote_price(event.get("no_mid")),
                "yes_last_trade": normalize_quote_price(event.get("yes_last_trade")),
                "no_last_trade": normalize_quote_price(event.get("no_last_trade")),
                "yes_age_seconds": None,
                "no_age_seconds": None,
                "source_file": "event_set_fallback",
            }
        ]
    )


def add_snapshot_flags(snapshots: pd.DataFrame, *, stale_quote_sec: float = 10.0) -> pd.DataFrame:
    if snapshots.empty:
        return snapshots.copy()
    frame = snapshots.copy()
    for column in quote_columns():
        if column not in frame.columns:
            frame[column] = np.nan
    for side in ("yes", "no"):
        mid = f"{side}_mid"
        bid = f"{side}_bid"
        ask = f"{side}_ask"
        if mid not in frame.columns:
            frame[mid] = np.nan
        computed_mid = (frame[bid] + frame[ask]) / 2.0
        frame[mid] = frame[mid].where(frame[mid].notna(), computed_mid)
    frame["market_age_seconds"] = (frame["quote_ts"] - frame["market_start_time"]).dt.total_seconds()
    max_age = pd.concat([frame["yes_age_seconds"], frame["no_age_seconds"]], axis=1).max(axis=1, skipna=True)
    frame["quote_abs_lag_sec"] = max_age
    frame["quote_stale"] = max_age.fillna(float("inf")) > stale_quote_sec
    frame["one_sided_quote"] = frame[["yes_bid", "yes_ask"]].notna().any(axis=1) ^ frame[["no_bid", "no_ask"]].notna().any(axis=1)
    frame["terminal_conviction_quote"] = (
        (frame["yes_ask"].fillna(0.0) >= 0.98)
        | (frame["no_ask"].fillna(0.0) >= 0.98)
        | (frame["yes_bid"].fillna(1.0) <= 0.02)
        | (frame["no_bid"].fillna(1.0) <= 0.02)
    )
    spread_sum = frame["yes_ask"] + frame["no_ask"]
    frame["wide_quote"] = spread_sum > 1.10
    frame["ask_pair_cost"] = spread_sum
    frame["both_asks_available"] = frame[["yes_ask", "no_ask"]].notna().all(axis=1)
    frame["yes_available"] = frame[["yes_bid", "yes_ask", "yes_mid", "yes_last_trade"]].notna().any(axis=1)
    frame["no_available"] = frame[["no_bid", "no_ask", "no_mid", "no_last_trade"]].notna().any(axis=1)
    frame["both_sides_available"] = frame["yes_available"] & frame["no_available"]
    return frame


def _value_and_timestamp(frame: pd.DataFrame, column: str, op: str) -> tuple[float | None, str | None]:
    if frame.empty or column not in frame.columns:
        return None, None
    values = pd.to_numeric(frame[column], errors="coerce").dropna()
    if values.empty:
        return None, None
    idx = values.idxmin() if op == "min" else values.idxmax()
    return _safe_float(frame.loc[idx, column]), frame.loc[idx, "quote_ts"].isoformat()


def window_price_diagnostics(snapshots: pd.DataFrame, *, prefix: str = "") -> dict[str, Any]:
    columns = [
        ("yes_bid", "min"),
        ("yes_ask", "min"),
        ("no_bid", "min"),
        ("no_ask", "min"),
        ("yes_bid", "max"),
        ("no_bid", "max"),
        ("yes_mid", "min"),
        ("no_mid", "min"),
        ("yes_last_trade", "min"),
        ("no_last_trade", "min"),
    ]
    result: dict[str, Any] = {
        f"{prefix}quote_count": int(len(snapshots)),
        f"{prefix}both_sides_available_count": int(snapshots["both_sides_available"].sum()) if len(snapshots) else 0,
        f"{prefix}yes_available_count": int(snapshots["yes_available"].sum()) if len(snapshots) else 0,
        f"{prefix}no_available_count": int(snapshots["no_available"].sum()) if len(snapshots) else 0,
    }
    for column, op in columns:
        value, ts = _value_and_timestamp(snapshots, column, op)
        name = f"{prefix}{op}_{column}"
        result[name] = value
        result[f"timestamp_{name}"] = ts
    value, ts = _value_and_timestamp(snapshots.dropna(subset=["ask_pair_cost"]) if "ask_pair_cost" in snapshots.columns else snapshots, "ask_pair_cost", "min")
    result[f"{prefix}min_ask_pair_cost"] = value
    result[f"timestamp_{prefix}min_ask_pair_cost"] = ts
    if ts is not None:
        row = snapshots.loc[snapshots["quote_ts"].astype(str) == str(pd.Timestamp(ts))]
        if row.empty:
            row = snapshots[snapshots["quote_ts"] == pd.Timestamp(ts)]
        if not row.empty:
            result[f"yes_ask_at_{prefix}min_pair"] = _safe_float(row.iloc[0].get("yes_ask"))
            result[f"no_ask_at_{prefix}min_pair"] = _safe_float(row.iloc[0].get("no_ask"))
        else:
            result[f"yes_ask_at_{prefix}min_pair"] = None
            result[f"no_ask_at_{prefix}min_pair"] = None
    else:
        result[f"yes_ask_at_{prefix}min_pair"] = None
        result[f"no_ask_at_{prefix}min_pair"] = None
    return result


def independent_leg_target_diagnostics(snapshots: pd.DataFrame, *, target: float, suffix: str) -> dict[str, Any]:
    yes = snapshots["yes_ask"].notna() & (snapshots["yes_ask"] <= target) if len(snapshots) else pd.Series(dtype=bool)
    no = snapshots["no_ask"].notna() & (snapshots["no_ask"] <= target) if len(snapshots) else pd.Series(dtype=bool)
    return {
        f"target_{target:.2f}_yes_ask_ever_lte_target_{suffix}": bool(yes.any()),
        f"target_{target:.2f}_no_ask_ever_lte_target_{suffix}": bool(no.any()),
        f"target_{target:.2f}_both_asks_ever_lte_target_independently_{suffix}": bool(yes.any() and no.any()),
        f"target_{target:.2f}_both_asks_lte_target_same_snapshot_{suffix}": bool((yes & no).any()),
    }


def target_touch_metrics(
    snapshots: pd.DataFrame,
    *,
    target: float,
    timeout_windows_sec: list[int],
) -> dict[str, Any]:
    ask_available = "yes_ask" in snapshots.columns and "no_ask" in snapshots.columns and snapshots[["yes_ask", "no_ask"]].notna().any().any()
    yes_touches = snapshots[snapshots["yes_ask"].notna() & (snapshots["yes_ask"] <= target)] if ask_available else snapshots.iloc[0:0]
    no_touches = snapshots[snapshots["no_ask"].notna() & (snapshots["no_ask"] <= target)] if ask_available else snapshots.iloc[0:0]
    yes_time = yes_touches["quote_ts"].min() if len(yes_touches) else pd.NaT
    no_time = no_touches["quote_ts"].min() if len(no_touches) else pd.NaT
    yes_touched = pd.notna(yes_time)
    no_touched = pd.notna(no_time)
    both = bool(yes_touched and no_touched)
    delta = abs((yes_time - no_time).total_seconds()) if both else None
    result: dict[str, Any] = {
        f"target_{target:.2f}_yes_touched": bool(yes_touched),
        f"target_{target:.2f}_no_touched": bool(no_touched),
        f"target_{target:.2f}_both_touched": both,
        f"target_{target:.2f}_only_yes_touched": bool(yes_touched and not no_touched),
        f"target_{target:.2f}_only_no_touched": bool(no_touched and not yes_touched),
        f"target_{target:.2f}_earliest_yes_touch_time": None if pd.isna(yes_time) else yes_time.isoformat(),
        f"target_{target:.2f}_earliest_no_touch_time": None if pd.isna(no_time) else no_time.isoformat(),
        f"target_{target:.2f}_seconds_between_first_and_second_leg_touch": delta,
    }
    for timeout in timeout_windows_sec:
        result[f"target_{target:.2f}_both_touched_within_{timeout}s"] = bool(both and delta is not None and delta <= timeout)
    return result


def market_replay_row(
    event: pd.Series,
    snapshots: pd.DataFrame,
    *,
    target_levels: list[float],
    timeout_windows_sec: list[int],
    stale_quote_sec: float,
    full_window_snapshots: pd.DataFrame | None = None,
) -> dict[str, Any]:
    flagged = add_snapshot_flags(snapshots, stale_quote_sec=stale_quote_sec)
    full_flagged = add_snapshot_flags(full_window_snapshots if full_window_snapshots is not None else snapshots, stale_quote_sec=stale_quote_sec)
    row: dict[str, Any] = {
        "slug": event.get("slug"),
        "event_id": event.get("event_id"),
        "market_id": str(event.get("market_id")),
        "market_start_time": event.get("market_start_time"),
        "market_end_time": event.get("market_end_time"),
        "assigned_state": event.get("assigned_state"),
        "is_quiet_market": bool(event.get("is_quiet_market", False)),
        "is_post_confirmation_quiet_market": bool(event.get("is_post_confirmation_quiet_market", False)),
        "group_quiet_status": "post_confirmation_quiet"
        if bool(event.get("is_post_confirmation_quiet_market", False))
        else "quiet"
        if bool(event.get("is_quiet_market", False))
        else "non_quiet",
        "tiny_move_near_boundary": event.get("tiny_move_near_boundary"),
        "label_agreement": event.get("label_agreement"),
        "binance_label": event.get("binance_label"),
        "chainlink_label": event.get("chainlink_label"),
        "snapshot_count_early_window": int(len(flagged)),
        "quote_coverage": bool(len(flagged)),
        "quote_stale": bool(flagged["quote_stale"].any()) if len(flagged) else True,
        "one_sided_quote": bool(flagged["one_sided_quote"].any()) if len(flagged) else False,
        "terminal_conviction_quote": bool(flagged["terminal_conviction_quote"].any()) if len(flagged) else False,
        "wide_quote": bool(flagged["wide_quote"].any()) if len(flagged) else False,
        "market_age_seconds_min": _safe_float(flagged["market_age_seconds"].min()) if len(flagged) else None,
        "market_age_seconds_max": _safe_float(flagged["market_age_seconds"].max()) if len(flagged) else None,
        "quote_abs_lag_sec_max": _safe_float(flagged["quote_abs_lag_sec"].max()) if len(flagged) else None,
        "yes_bid": _safe_float(flagged["yes_bid"].dropna().iloc[0]) if len(flagged["yes_bid"].dropna()) else None,
        "yes_ask": _safe_float(flagged["yes_ask"].dropna().iloc[0]) if len(flagged["yes_ask"].dropna()) else None,
        "no_bid": _safe_float(flagged["no_bid"].dropna().iloc[0]) if len(flagged["no_bid"].dropna()) else None,
        "no_ask": _safe_float(flagged["no_ask"].dropna().iloc[0]) if len(flagged["no_ask"].dropna()) else None,
        "yes_mid": _safe_float(flagged["yes_mid"].dropna().iloc[0]) if len(flagged["yes_mid"].dropna()) else None,
        "no_mid": _safe_float(flagged["no_mid"].dropna().iloc[0]) if len(flagged["no_mid"].dropna()) else None,
    }
    row.update(window_price_diagnostics(flagged, prefix=""))
    row.update(window_price_diagnostics(full_flagged, prefix="full_window_"))
    if len(flagged):
        ask_sum = flagged["yes_ask"] + flagged["no_ask"]
        row["min_yes_ask_plus_no_ask"] = _safe_float(ask_sum.min())
        for threshold in (1.00, 0.95, 0.92, 0.90):
            valid = ask_sum.dropna()
            row[f"fraction_yes_ask_plus_no_ask_lt_{threshold:.2f}"] = _safe_float((valid < threshold).mean()) if len(valid) else None
    else:
        row["min_yes_ask_plus_no_ask"] = None
        for threshold in (1.00, 0.95, 0.92, 0.90):
            row[f"fraction_yes_ask_plus_no_ask_lt_{threshold:.2f}"] = None

    row["min_ask_pair_cost_early"] = row.get("min_ask_pair_cost")
    row["timestamp_min_ask_pair_cost_early"] = row.get("timestamp_min_ask_pair_cost")
    row["yes_ask_at_min_pair_early"] = row.get("yes_ask_at_min_pair")
    row["no_ask_at_min_pair_early"] = row.get("no_ask_at_min_pair")
    row["min_ask_pair_cost_full_window"] = row.get("full_window_min_ask_pair_cost")
    row["timestamp_min_ask_pair_cost_full_window"] = row.get("timestamp_full_window_min_ask_pair_cost")
    row["yes_ask_at_min_pair_full_window"] = row.get("yes_ask_at_full_window_min_pair")
    row["no_ask_at_min_pair_full_window"] = row.get("no_ask_at_full_window_min_pair")

    for target in target_levels:
        row.update(target_touch_metrics(flagged, target=target, timeout_windows_sec=timeout_windows_sec))
        row.update(independent_leg_target_diagnostics(flagged, target=target, suffix="early"))
        row.update(independent_leg_target_diagnostics(full_flagged, target=target, suffix="full_window"))
        prefix = f"target_{target:.2f}"
        yes_won = str(event.get("binance_label")).upper() == "UP"
        no_won = str(event.get("binance_label")).upper() == "DOWN"
        row[f"{prefix}_only_yes_won"] = bool(row[f"{prefix}_only_yes_touched"] and yes_won)
        row[f"{prefix}_only_no_won"] = bool(row[f"{prefix}_only_no_touched"] and no_won)
        row[f"{prefix}_one_sided_losing_touch"] = bool(
            (row[f"{prefix}_only_yes_touched"] and not yes_won)
            or (row[f"{prefix}_only_no_touched"] and not no_won)
        )
    return row


def build_market_replay(
    joined_events: pd.DataFrame,
    quote_snapshots: pd.DataFrame,
    *,
    early_window_sec: int,
    full_window_sec: int,
    target_levels: list[float],
    timeout_windows_sec: list[int],
    stale_quote_sec: float,
) -> pd.DataFrame:
    by_market_id = {market_id: group.sort_values("quote_ts") for market_id, group in quote_snapshots.groupby("market_id")} if not quote_snapshots.empty else {}
    rows: list[dict[str, Any]] = []
    for _, event in joined_events.iterrows():
        start = event["market_start_time"]
        early_end = start + pd.Timedelta(seconds=early_window_sec)
        full_end = start + pd.Timedelta(seconds=full_window_sec)
        market_id = str(event.get("market_id"))
        market_snapshots = by_market_id.get(market_id, pd.DataFrame(columns=quote_columns()))
        if not market_snapshots.empty:
            early_snapshots = market_snapshots[(market_snapshots["quote_ts"] >= start) & (market_snapshots["quote_ts"] <= early_end)].copy()
            full_snapshots = market_snapshots[(market_snapshots["quote_ts"] >= start) & (market_snapshots["quote_ts"] <= full_end)].copy()
        else:
            early_snapshots = pd.DataFrame(columns=quote_columns())
            full_snapshots = pd.DataFrame(columns=quote_columns())
        if early_snapshots.empty and full_snapshots.empty:
            fallback = fallback_quote_from_event(event)
            if not fallback.empty:
                early_snapshots = fallback[(fallback["quote_ts"] >= start) & (fallback["quote_ts"] <= early_end)].copy()
                full_snapshots = fallback[(fallback["quote_ts"] >= start) & (fallback["quote_ts"] <= full_end)].copy()
        rows.append(
            market_replay_row(
                event,
                early_snapshots,
                target_levels=target_levels,
                timeout_windows_sec=timeout_windows_sec,
                stale_quote_sec=stale_quote_sec,
                full_window_snapshots=full_snapshots,
            )
        )
    return pd.DataFrame(rows)


def empty_replay_frame(target_levels: list[float], timeout_windows_sec: list[int]) -> pd.DataFrame:
    columns = [
        "slug",
        "market_id",
        "market_start_time",
        "market_end_time",
        "assigned_state",
        "is_quiet_market",
        "is_post_confirmation_quiet_market",
        "group_quiet_status",
        "tiny_move_near_boundary",
        "label_agreement",
        "binance_label",
        "chainlink_label",
        "snapshot_count_early_window",
        "quote_count",
        "both_sides_available_count",
        "yes_available_count",
        "no_available_count",
        "quote_coverage",
        "quote_stale",
        "one_sided_quote",
        "terminal_conviction_quote",
        "wide_quote",
        "market_age_seconds_min",
        "market_age_seconds_max",
        "quote_abs_lag_sec_max",
        "yes_bid",
        "yes_ask",
        "no_bid",
        "no_ask",
        "yes_mid",
        "no_mid",
        "min_yes_ask",
        "min_no_ask",
        "min_yes_bid",
        "min_no_bid",
        "max_yes_bid",
        "max_no_bid",
        "min_yes_mid",
        "min_no_mid",
        "min_yes_last_trade",
        "min_no_last_trade",
        "full_window_quote_count",
        "full_window_both_sides_available_count",
        "full_window_min_yes_ask",
        "full_window_min_no_ask",
        "full_window_min_yes_bid",
        "full_window_min_no_bid",
        "full_window_max_yes_bid",
        "full_window_max_no_bid",
        "full_window_min_yes_mid",
        "full_window_min_no_mid",
        "full_window_min_yes_last_trade",
        "full_window_min_no_last_trade",
        "min_yes_ask_plus_no_ask",
        "min_ask_pair_cost_early",
        "timestamp_min_ask_pair_cost_early",
        "yes_ask_at_min_pair_early",
        "no_ask_at_min_pair_early",
        "min_ask_pair_cost_full_window",
        "timestamp_min_ask_pair_cost_full_window",
        "yes_ask_at_min_pair_full_window",
        "no_ask_at_min_pair_full_window",
        "fraction_yes_ask_plus_no_ask_lt_1.00",
        "fraction_yes_ask_plus_no_ask_lt_0.95",
        "fraction_yes_ask_plus_no_ask_lt_0.92",
        "fraction_yes_ask_plus_no_ask_lt_0.90",
    ]
    for target in target_levels:
        prefix = f"target_{target:.2f}"
        columns.extend(
            [
                f"{prefix}_yes_touched",
                f"{prefix}_no_touched",
                f"{prefix}_both_touched",
                f"{prefix}_only_yes_touched",
                f"{prefix}_only_no_touched",
                f"{prefix}_earliest_yes_touch_time",
                f"{prefix}_earliest_no_touch_time",
                f"{prefix}_seconds_between_first_and_second_leg_touch",
                f"{prefix}_only_yes_won",
                f"{prefix}_only_no_won",
                f"{prefix}_one_sided_losing_touch",
            ]
        )
        for suffix in ("early", "full_window"):
            columns.extend(
                [
                    f"{prefix}_yes_ask_ever_lte_target_{suffix}",
                    f"{prefix}_no_ask_ever_lte_target_{suffix}",
                    f"{prefix}_both_asks_ever_lte_target_independently_{suffix}",
                    f"{prefix}_both_asks_lte_target_same_snapshot_{suffix}",
                ]
            )
        columns.extend([f"{prefix}_both_touched_within_{timeout}s" for timeout in timeout_windows_sec])
    return pd.DataFrame(columns=columns)


def join_events_to_hmm(events: pd.DataFrame, hmm: pd.DataFrame) -> pd.DataFrame:
    hmm_columns = [
        "event_start_time",
        "assigned_state",
        "is_quiet_market",
        "is_post_confirmation_quiet_market",
        "state_posterior_max",
    ]
    joined = events.merge(
        hmm[hmm_columns],
        left_on="market_start_time",
        right_on="event_start_time",
        how="inner",
    )
    return joined.drop(columns=["event_start_time"])


def rate(series: pd.Series) -> float | None:
    if len(series) == 0:
        return None
    return _safe_float(series.astype(float).mean())


def target_summary(frame: pd.DataFrame, target: float, timeout_windows_sec: list[int]) -> dict[str, Any]:
    prefix = f"target_{target:.2f}"
    one_leg = frame[f"{prefix}_only_yes_touched"] | frame[f"{prefix}_only_no_touched"]
    one_sided = frame[f"{prefix}_only_yes_touched"] | frame[f"{prefix}_only_no_touched"]
    losing = frame[f"{prefix}_one_sided_losing_touch"]
    summary = {
        "market_count": int(len(frame)),
        "both_touch_rate": rate(frame[f"{prefix}_both_touched"]),
        "only_yes_touch_rate": rate(frame[f"{prefix}_only_yes_touched"]),
        "only_no_touch_rate": rate(frame[f"{prefix}_only_no_touched"]),
        "one_leg_only_rate": rate(one_leg),
        "orphan_toxicity_proxy": rate(losing[one_sided]) if int(one_sided.sum()) else None,
    }
    for timeout in timeout_windows_sec:
        summary[f"both_touch_within_{timeout}s_rate"] = rate(frame[f"{prefix}_both_touched_within_{timeout}s"])
    for suffix in ("early", "full_window"):
        for metric in (
            "yes_ask_ever_lte_target",
            "no_ask_ever_lte_target",
            "both_asks_ever_lte_target_independently",
            "both_asks_lte_target_same_snapshot",
        ):
            column = f"{prefix}_{metric}_{suffix}"
            summary[f"{metric}_{suffix}_rate"] = rate(frame[column]) if column in frame.columns else None
    return summary


def quantile_summary(series: pd.Series) -> dict[str, float | None]:
    values = pd.to_numeric(series, errors="coerce").dropna()
    if values.empty:
        return {"p01": None, "p05": None, "p10": None, "p25": None, "median": None}
    return {
        "p01": _safe_float(values.quantile(0.01)),
        "p05": _safe_float(values.quantile(0.05)),
        "p10": _safe_float(values.quantile(0.10)),
        "p25": _safe_float(values.quantile(0.25)),
        "median": _safe_float(values.quantile(0.50)),
    }


def audit_summary(frame: pd.DataFrame) -> dict[str, Any]:
    return {
        "full_window_min_yes_ask_quantiles": quantile_summary(frame["full_window_min_yes_ask"]) if "full_window_min_yes_ask" in frame.columns else quantile_summary(pd.Series(dtype=float)),
        "full_window_min_no_ask_quantiles": quantile_summary(frame["full_window_min_no_ask"]) if "full_window_min_no_ask" in frame.columns else quantile_summary(pd.Series(dtype=float)),
        "min_ask_pair_cost_full_window_quantiles": quantile_summary(frame["min_ask_pair_cost_full_window"]) if "min_ask_pair_cost_full_window" in frame.columns else quantile_summary(pd.Series(dtype=float)),
    }


def grouped_summary(frame: pd.DataFrame, target_levels: list[float], timeout_windows_sec: list[int]) -> dict[str, Any]:
    groups: dict[str, pd.Series] = {
        "all_markets": pd.Series(True, index=frame.index),
        "quiet_markets": frame["is_quiet_market"].astype(bool),
        "post_confirmation_quiet_markets": frame["is_post_confirmation_quiet_market"].astype(bool),
        "non_quiet_markets": ~frame["is_quiet_market"].astype(bool),
    }
    if "tiny_move_near_boundary" in frame.columns:
        tiny = frame["tiny_move_near_boundary"].map(_bool_from_any)
        groups["tiny_move_near_boundary_true"] = tiny == True
        groups["tiny_move_near_boundary_false"] = tiny == False
    if "label_agreement" in frame.columns:
        agreement = frame["label_agreement"].map(_bool_from_any)
        groups["chainlink_binance_agree"] = agreement == True
        groups["chainlink_binance_disagree"] = agreement == False

    result: dict[str, Any] = {}
    for name, mask in groups.items():
        subset = frame[mask.fillna(False)]
        result[name] = {
            "market_count": int(len(subset)),
            "quote_coverage": rate(subset["quote_coverage"]) if len(subset) else None,
            "one_sided_quote_rate": rate(subset["one_sided_quote"]) if len(subset) else None,
            "wide_quote_rate": rate(subset["wide_quote"]) if len(subset) else None,
            "audit": audit_summary(subset),
            "targets": {f"{target:.2f}": target_summary(subset, target, timeout_windows_sec) for target in target_levels},
        }
    return result


def build_summary(
    *,
    replay: pd.DataFrame,
    recorder_event_rows: int,
    recorder_quote_rows: int,
    joined_events: int,
    quiet_count: int,
    post_confirmation_count: int,
    target_levels: list[float],
    timeout_windows_sec: list[int],
    warnings: list[str],
) -> dict[str, Any]:
    return {
        "recorder_event_rows": int(recorder_event_rows),
        "recorder_quote_rows": int(recorder_quote_rows),
        "joined_recorder_events": int(joined_events),
        "quiet_markets": int(quiet_count),
        "post_confirmation_quiet_markets": int(post_confirmation_count),
        "quote_coverage": rate(replay["quote_coverage"]) if len(replay) else None,
        "target_levels": target_levels,
        "timeout_windows_sec": timeout_windows_sec,
        "grouped_metrics": grouped_summary(replay, target_levels, timeout_windows_sec),
        "warnings": warnings,
    }


def price_scale_warnings(quotes: pd.DataFrame, replay: pd.DataFrame) -> list[str]:
    warnings: list[str] = []
    price_columns = ["yes_bid", "yes_ask", "no_bid", "no_ask", "yes_mid", "no_mid", "yes_last_trade", "no_last_trade"]
    values = pd.concat([pd.to_numeric(quotes[column], errors="coerce") for column in price_columns if column in quotes.columns], ignore_index=True).dropna()
    if len(values):
        if float((values > 1.0).mean()) > 0.05:
            warnings.append("suspicious quote scale: more than 5% of normalized prices are above 1.0")
        if float((values < 0.001).mean()) > 0.50:
            warnings.append("suspicious quote scale: more than 50% of normalized prices are below 0.001")
    if len(replay) and "min_ask_pair_cost_full_window" in replay.columns:
        pair_cost = pd.to_numeric(replay["min_ask_pair_cost_full_window"], errors="coerce").dropna()
        if len(pair_cost) and float(((pair_cost < 0.50) | (pair_cost > 1.50)).mean()) > 0.25:
            warnings.append("suspicious ask sums: more than 25% of markets with pair asks have min ask-pair cost outside [0.50, 1.50]")
    if len(quotes):
        ask_missing = quotes[["yes_ask", "no_ask"]].isna().all(axis=1) if {"yes_ask", "no_ask"}.issubset(quotes.columns) else pd.Series(dtype=bool)
        if len(ask_missing) and float(ask_missing.mean()) > 0.25:
            warnings.append("missing asks despite quote rows: more than 25% of quote rows have neither YES nor NO ask")
    return warnings


def quote_debug_sample(joined_events: pd.DataFrame, quotes: pd.DataFrame, replay: pd.DataFrame, *, target_levels: list[float], full_window_sec: int, sample_markets: int = 5) -> pd.DataFrame:
    if joined_events.empty or quotes.empty or replay.empty:
        return pd.DataFrame(columns=quote_columns())
    ids: set[str] = set()
    for column in ("full_window_min_yes_ask", "full_window_min_no_ask", "min_ask_pair_cost_full_window"):
        if column in replay.columns:
            ids.update(replay.dropna(subset=[column]).sort_values(column).head(sample_markets)["market_id"].astype(str).tolist())
    for target in target_levels:
        column = f"target_{target:.2f}_both_asks_ever_lte_target_independently_full_window"
        if column in replay.columns:
            ids.update(replay[replay[column].fillna(False)]["market_id"].astype(str).tolist())
    if not ids:
        return pd.DataFrame(columns=quote_columns())
    event_lookup = joined_events.set_index(joined_events["market_id"].astype(str), drop=False)
    rows: list[pd.DataFrame] = []
    for market_id in sorted(ids):
        if market_id not in event_lookup.index:
            continue
        event = event_lookup.loc[market_id]
        if isinstance(event, pd.DataFrame):
            event = event.iloc[0]
        start = event["market_start_time"]
        end = start + pd.Timedelta(seconds=full_window_sec)
        subset = quotes[(quotes["market_id"].astype(str) == market_id) & (quotes["quote_ts"] >= start) & (quotes["quote_ts"] <= end)].copy()
        if subset.empty:
            continue
        subset["event_id"] = event.get("event_id")
        subset["event_market_start_time"] = event.get("market_start_time")
        subset["event_market_end_time"] = event.get("market_end_time")
        subset["ask_pair_cost"] = subset["yes_ask"] + subset["no_ask"]
        rows.append(subset)
    if not rows:
        return pd.DataFrame(columns=quote_columns())
    columns = [
        "event_id",
        "market_id",
        "slug",
        "quote_ts",
        "market_start_time",
        "market_end_time",
        "event_market_start_time",
        "event_market_end_time",
        "token_yes",
        "token_no",
        "yes_bid",
        "yes_ask",
        "no_bid",
        "no_ask",
        "yes_mid",
        "no_mid",
        "yes_last_trade",
        "no_last_trade",
        "ask_pair_cost",
        "quote_capture_ok",
        "quote_capture_status",
        "source_file",
    ]
    return pd.concat(rows, ignore_index=True).sort_values(["market_id", "quote_ts"])[columns]


def write_outputs(output_dir: Path, replay: pd.DataFrame, summary: dict[str, Any], debug_sample: pd.DataFrame | None = None) -> dict[str, str]:
    output_dir.mkdir(parents=True, exist_ok=True)
    replay_path = output_dir / "quiet_pair_market_replay.csv"
    summary_path = output_dir / "quiet_pair_summary.json"
    readme_path = output_dir / "quiet_pair_readme_summary.txt"
    debug_path = output_dir / "quiet_pair_quote_debug_sample.csv"
    replay.to_csv(replay_path, index=False)
    (debug_sample if debug_sample is not None else pd.DataFrame()).to_csv(debug_path, index=False)
    summary_path.write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
    readme_path.write_text(build_readme_summary(summary), encoding="utf-8")
    return {
        "quiet_pair_market_replay": str(replay_path),
        "quiet_pair_quote_debug_sample": str(debug_path),
        "quiet_pair_summary": str(summary_path),
        "quiet_pair_readme_summary": str(readme_path),
    }


def build_readme_summary(summary: dict[str, Any]) -> str:
    lines = [
        f"recorder_event_rows={summary['recorder_event_rows']}",
        f"recorder_quote_rows={summary['recorder_quote_rows']}",
        f"joined_recorder_events={summary['joined_recorder_events']}",
        f"quiet_markets={summary['quiet_markets']}",
        f"post_confirmation_quiet_markets={summary['post_confirmation_quiet_markets']}",
        f"quote_coverage={summary['quote_coverage']}",
        "",
        "Opportunity rates:",
    ]
    all_targets = summary["grouped_metrics"].get("all_markets", {}).get("targets", {})
    quiet_targets = summary["grouped_metrics"].get("quiet_markets", {}).get("targets", {})
    post_targets = summary["grouped_metrics"].get("post_confirmation_quiet_markets", {}).get("targets", {})
    for target in summary["target_levels"]:
        key = f"{target:.2f}"
        all_metrics = all_targets.get(key, {})
        quiet_metrics = quiet_targets.get(key, {})
        post_metrics = post_targets.get(key, {})
        lines.append(
            f"- target={key}: all_both={all_metrics.get('both_touch_rate')}, "
            f"quiet_both={quiet_metrics.get('both_touch_rate')}, "
            f"post_confirm_quiet_both={post_metrics.get('both_touch_rate')}, "
            f"post_confirm_orphan_toxicity_proxy={post_metrics.get('orphan_toxicity_proxy')}"
        )
    lines.extend(["", "Warnings:", *[f"- {warning}" for warning in summary.get("warnings", [])]])
    return "\n".join(lines) + "\n"


def run_replay(
    *,
    hmm_assignments: Path,
    quiet_state: int,
    event_set: Path,
    input_roots: list[Path],
    output_dir: Path,
    early_window_sec: int,
    full_window_sec: int,
    target_levels: list[float],
    timeout_windows_sec: list[int],
    stale_quote_sec: float,
) -> dict[str, Any]:
    hmm = load_hmm_assignments(hmm_assignments, quiet_state)
    events = load_event_set(event_set)
    joined = join_events_to_hmm(events, hmm)
    quotes = load_quote_snapshots(input_roots)
    if joined.empty:
        replay = empty_replay_frame(target_levels, timeout_windows_sec)
    else:
        replay = build_market_replay(
            joined,
            quotes,
            early_window_sec=early_window_sec,
            full_window_sec=full_window_sec,
            target_levels=target_levels,
            timeout_windows_sec=timeout_windows_sec,
            stale_quote_sec=stale_quote_sec,
        )
    warnings = [QUOTE_LIMITATION_WARNING, NO_PROFIT_WARNING]
    if joined.empty:
        warnings.append("no event-set markets joined to HMM assignments; check time coverage and join keys")
    if quotes.empty:
        warnings.append("no recorder quote snapshots found; replay used event-set quote fallback where available")
    if len(replay) and replay["quote_coverage"].mean() < 1.0:
        warnings.append("some joined events have no early-window quote coverage")
    if len(replay) and replay["one_sided_quote"].mean() > 0.0:
        warnings.append("some early-window snapshots have only one side available")
    warnings.extend(price_scale_warnings(quotes, replay))
    summary = build_summary(
        replay=replay,
        recorder_event_rows=len(events),
        recorder_quote_rows=len(quotes),
        joined_events=len(joined),
        quiet_count=int(joined["is_quiet_market"].sum()) if len(joined) else 0,
        post_confirmation_count=int(joined["is_post_confirmation_quiet_market"].sum()) if len(joined) else 0,
        target_levels=target_levels,
        timeout_windows_sec=timeout_windows_sec,
        warnings=warnings,
    )
    debug_sample = quote_debug_sample(joined, quotes, replay, target_levels=target_levels, full_window_sec=full_window_sec)
    paths = write_outputs(output_dir, replay, summary, debug_sample)
    return {"summary": summary, "output_paths": paths}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Offline quiet-regime Polymarket pair-order opportunity replay.")
    parser.add_argument("--hmm-assignments", type=Path, required=True)
    parser.add_argument("--quiet-state", type=int, required=True)
    parser.add_argument("--event-set", type=Path, required=True)
    parser.add_argument("--input-root", type=Path, action="append", default=[])
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--early-window-sec", type=int, default=60)
    parser.add_argument("--full-window-sec", type=int, default=300)
    parser.add_argument("--target-levels", type=parse_float_list, default=DEFAULT_TARGET_LEVELS)
    parser.add_argument("--timeout-windows-sec", type=parse_float_list, default=DEFAULT_TIMEOUT_WINDOWS_SEC)
    parser.add_argument("--stale-quote-sec", type=float, default=10.0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    result = run_replay(
        hmm_assignments=args.hmm_assignments,
        quiet_state=args.quiet_state,
        event_set=args.event_set,
        input_roots=args.input_root,
        output_dir=args.output_dir,
        early_window_sec=args.early_window_sec,
        full_window_sec=args.full_window_sec,
        target_levels=[float(value) for value in args.target_levels],
        timeout_windows_sec=[int(value) for value in args.timeout_windows_sec],
        stale_quote_sec=args.stale_quote_sec,
    )
    print(json.dumps({"summary": result["summary"], "output_paths": result["output_paths"]}, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
