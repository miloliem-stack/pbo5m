#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import re
import sys
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.research import terminal_conviction as quotes_lib


DEFAULT_MODELS = [
    "calibrated_logistic__gbm_rv30",
    "gbm_winsorized_sigma__w30__z2.5",
    "gbm_blended_sigma__50_30_20",
    "gbm_zero_drift__rv30_no_ito",
    "brownian_zero_drift__rv30",
    "empirical_moneyness_age",
    "baseline_50",
]
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
PREDICTION_READ_COLUMNS = [
    "model_id",
    "model",
    "model_name",
    "p_up",
    "probability",
    "y_prob",
    "result_up",
    "y_true",
    "label",
    "outcome",
    "market_age_seconds",
    "age_seconds",
    "elapsed_seconds",
    "fold_id",
    "fold",
    "test_fold",
    "prediction_ts",
    "timestamp",
    "ts",
    "market_window_start",
    "market_start_ts",
    "market_window_end",
    "market_end_ts",
    "market_key",
    "slug",
    "market_id",
]


def parse_csv(value: str) -> list[str]:
    return [item.strip() for item in str(value).split(",") if item.strip()]


def parse_floats(value: str) -> list[float]:
    return [float(item) for item in parse_csv(value)]


def _existing_parquet_columns(path: Path, wanted: list[str]) -> list[str] | None:
    try:
        import pyarrow.parquet as pq
    except Exception:
        return None
    schema_names = set(pq.ParquetFile(path).schema_arrow.names)
    return [column for column in wanted if column in schema_names]


def read_frame(path: Path, columns: list[str] | None = None) -> pd.DataFrame:
    if path.suffix.lower() == ".parquet":
        try:
            existing = _existing_parquet_columns(path, columns) if columns is not None else None
            return pd.read_parquet(path, columns=existing)
        except ImportError as exc:
            csv_fallback = path.with_suffix(".csv")
            if csv_fallback.exists():
                print(f"Parquet support unavailable, reading CSV fallback: {csv_fallback}", file=sys.stderr)
                return pd.read_csv(csv_fallback, usecols=lambda col: columns is None or col in columns)
            raise RuntimeError(f"Cannot read parquet file without pyarrow/fastparquet: {path}") from exc
    return pd.read_csv(path, usecols=lambda col: columns is None or col in columns)


def read_prediction_frame(path: Path, models: list[str], market_start_keys: set[str]) -> pd.DataFrame:
    columns = PREDICTION_READ_COLUMNS
    if path.suffix.lower() == ".parquet":
        existing = _existing_parquet_columns(path, columns)
        try:
            import pyarrow.parquet as pq

            filters = []
            if "model_id" in (existing or []):
                filters.append(("model_id", "in", models))
            if "market_window_start" in (existing or []) and market_start_keys:
                starts = [pd.Timestamp(value).to_pydatetime() for value in sorted(market_start_keys)]
                filters.append(("market_window_start", "in", starts))
            if filters:
                return pq.read_table(path, columns=existing, filters=filters).to_pandas()
        except Exception as exc:
            print(f"Parquet predicate pushdown unavailable; falling back to column-pruned read: {exc}", file=sys.stderr)
        return read_frame(path, columns=columns)
    frame = read_frame(path, columns=columns)
    if "model_id" in frame.columns:
        frame = frame[frame["model_id"].astype(str).isin(models)]
    if "market_window_start" in frame.columns and market_start_keys:
        starts = pd.to_datetime(frame["market_window_start"], utc=True, errors="coerce").dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        frame = frame[starts.isin(market_start_keys)]
    return frame


def prediction_market_start_range(path: Path) -> tuple[pd.Timestamp | None, pd.Timestamp | None]:
    if path.suffix.lower() == ".parquet":
        try:
            import pyarrow.parquet as pq

            pf = pq.ParquetFile(path)
            names = pf.schema_arrow.names
            if "market_window_start" not in names:
                return None, None
            idx = names.index("market_window_start")
            mins = []
            maxs = []
            for i in range(pf.num_row_groups):
                stats = pf.metadata.row_group(i).column(idx).statistics
                if stats is not None and stats.min is not None and stats.max is not None:
                    mins.append(pd.Timestamp(stats.min, tz="UTC") if pd.Timestamp(stats.min).tzinfo is None else pd.Timestamp(stats.min).tz_convert("UTC"))
                    maxs.append(pd.Timestamp(stats.max, tz="UTC") if pd.Timestamp(stats.max).tzinfo is None else pd.Timestamp(stats.max).tz_convert("UTC"))
            return (min(mins), max(maxs)) if mins and maxs else (None, None)
        except Exception:
            return None, None
    return None, None


def quote_path_date_range(path: Path) -> tuple[pd.Timestamp | None, pd.Timestamp | None]:
    try:
        files = quotes_lib.discover_jsonl_files(path, quotes_lib.QUOTE_FILENAMES)
    except Exception:
        return None, None
    dates = []
    for file in files:
        match = re.search(r"(20\d{2}-\d{2}-\d{2})", str(file))
        if match:
            dates.append(pd.Timestamp(match.group(1), tz="UTC"))
    return (min(dates), max(dates) + pd.Timedelta(days=1)) if dates else (None, None)


def ranges_disjoint(pred_range: tuple[pd.Timestamp | None, pd.Timestamp | None], quote_range: tuple[pd.Timestamp | None, pd.Timestamp | None]) -> bool:
    pred_min, pred_max = pred_range
    quote_min, quote_max = quote_range
    if pred_min is None or pred_max is None or quote_min is None or quote_max is None:
        return False
    return pred_max < quote_min - pd.Timedelta(days=1) or pred_min > quote_max + pd.Timedelta(days=1)


def _regex_string(line: str, key: str) -> str | None:
    match = re.search(rf'"{re.escape(key)}"\s*:\s*"([^"]*)"', line)
    return match.group(1) if match else None


def _side_text(line: str, side: str) -> str:
    start = line.find(f'"{side}_raw"')
    if start < 0:
        return ""
    other = "no_raw" if side == "yes" else "yes_raw"
    end = line.find(f'"{other}"', start + 1)
    if end < 0:
        end = len(line)
    return line[start:end]


def _array_text(block: str, name: str) -> str:
    match = re.search(rf'"{name}"\s*:\s*\[(.*?)\]', block)
    return match.group(1) if match else ""


def _best_level_from_array(array_text: str, *, side: str) -> tuple[float | None, float | None]:
    best_price: float | None = None
    best_size: float | None = None
    for match in re.finditer(r'"price"\s*:\s*"?([0-9.]+)"?\s*,\s*"size"\s*:\s*"?([0-9.]+)"?', array_text):
        price = float(match.group(1))
        size = float(match.group(2))
        if size <= 0:
            continue
        if best_price is None or (side == "bid" and price > best_price) or (side == "ask" and price < best_price):
            best_price = price
            best_size = size
    return best_price, best_size


def _minimal_quote_record(line: str, market_window_seconds: int) -> dict[str, Any] | None:
    ts = quotes_lib.utc_ts(_regex_string(line, "ts") or _regex_string(line, "timestamp"))
    slug = _regex_string(line, "slug") or _regex_string(line, "market_slug")
    start = quotes_lib.utc_ts(_regex_string(line, "market_start_time") or _regex_string(line, "market_start_ts"))
    end = quotes_lib.utc_ts(_regex_string(line, "market_end_time") or _regex_string(line, "market_end_ts"))
    if pd.isna(start) or pd.isna(end):
        inferred_start, inferred_end = quotes_lib.infer_window_from_slug(slug, market_window_seconds)
        start = start if not pd.isna(start) else inferred_start
        end = end if not pd.isna(end) else inferred_end
    if pd.isna(ts) or pd.isna(start):
        return None
    yes_block = _side_text(line, "yes")
    no_block = _side_text(line, "no")
    yes_bid, yes_bid_size = _best_level_from_array(_array_text(yes_block, "bids"), side="bid")
    yes_ask, yes_ask_size = _best_level_from_array(_array_text(yes_block, "asks"), side="ask")
    no_bid, no_bid_size = _best_level_from_array(_array_text(no_block, "bids"), side="bid")
    no_ask, no_ask_size = _best_level_from_array(_array_text(no_block, "asks"), side="ask")
    return {
        "market_key": slug,
        "market_slug": slug,
        "quote_ts": ts,
        "market_start_ts": start,
        "market_end_ts": end,
        "yes_bid": yes_bid,
        "yes_ask": yes_ask,
        "yes_bid_size": yes_bid_size,
        "yes_ask_size": yes_ask_size,
        "no_bid": no_bid,
        "no_ask": no_ask,
        "no_bid_size": no_bid_size,
        "no_ask_size": no_ask_size,
    }


def load_quotes_minimal(path: Path, market_window_seconds: int, allow_post_end_quotes: bool) -> tuple[pd.DataFrame, dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    loaded = 0
    parsed = 0
    for file in quotes_lib.discover_jsonl_files(path, quotes_lib.QUOTE_FILENAMES):
        with file.open("r", encoding="utf-8") as handle:
            for line in handle:
                loaded += 1
                row = _minimal_quote_record(line, market_window_seconds)
                if row is None:
                    continue
                parsed += 1
                rows.append(row)
    frame = pd.DataFrame(rows)
    before_window = len(frame)
    if not frame.empty and not allow_post_end_quotes:
        frame = frame[
            frame["quote_ts"].notna()
            & frame["market_start_ts"].notna()
            & frame["market_end_ts"].notna()
            & (frame["quote_ts"] >= frame["market_start_ts"])
            & (frame["quote_ts"] <= frame["market_end_ts"])
        ].copy()
    if not frame.empty:
        frame["market_start_key"] = pd.to_datetime(frame["market_start_ts"], utc=True, errors="coerce").dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        frame = frame.dropna(subset=["quote_ts", "market_start_key"]).sort_values(["market_start_key", "quote_ts"]).reset_index(drop=True)
    diagnostics = {
        "quote_rows_loaded": int(loaded),
        "quote_rows_parsed": int(parsed),
        "quote_rows_before_window_filter": int(before_window),
        "quote_rows_dropped_outside_window": int(before_window - len(frame)),
        "quote_markets_found": int(frame["market_start_key"].nunique()) if not frame.empty else 0,
        "quote_loader": "minimal_regex_raw_book_best_bid_ask",
    }
    return frame, diagnostics


def write_empty_outputs(output_dir: Path, diagnostics: dict[str, Any], args: argparse.Namespace) -> None:
    pd.DataFrame().to_csv(output_dir / "replay_summary_by_model_window_threshold.csv", index=False)
    pd.DataFrame().to_csv(output_dir / "replay_summary_by_edge_bucket.csv", index=False)
    pd.DataFrame().to_csv(output_dir / "replay_summary_by_age_bucket.csv", index=False)
    pd.DataFrame().to_csv(output_dir / "replay_summary_by_fold.csv", index=False)
    pd.DataFrame().to_csv(output_dir / "replay_trades.csv", index=False)
    pd.DataFrame().to_csv(output_dir / "replay_opportunities.csv", index=False)
    pd.DataFrame([diagnostics]).to_csv(output_dir / "quote_join_diagnostics.csv", index=False)
    (output_dir / "quote_join_diagnostics.json").write_text(json.dumps(diagnostics, indent=2, default=str), encoding="utf-8")
    (output_dir / "replay_config.json").write_text(json.dumps(vars(args), indent=2, default=str), encoding="utf-8")
    (output_dir / "replay_scorecard_readme.txt").write_text(
        "Probability edge replay vs recorded quotes\n\n"
        "No replay rows were produced because prediction and quote date coverage do not overlap.\n"
        "This is offline research only; no live behavior was changed.\n",
        encoding="utf-8",
    )


def write_frame(frame: pd.DataFrame, path: Path) -> Path:
    parquet_path = path.with_suffix(".parquet")
    try:
        frame.to_parquet(parquet_path, index=False)
        return parquet_path
    except Exception:
        csv_path = path.with_suffix(".csv")
        frame.to_csv(csv_path, index=False)
        return csv_path


def detect_column(df: pd.DataFrame, candidates: list[str], label: str, required: bool = True) -> str | None:
    matches = [col for col in candidates if col in df.columns]
    if len(matches) == 1:
        return matches[0]
    if len(matches) > 1:
        return matches[0]
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


def age_bucket(age: pd.Series) -> pd.Series:
    return pd.cut(
        age,
        bins=[-np.inf, 60, 120, 180, 218, 240, 300],
        labels=["0_60", "60_120", "120_180", "180_218", "218_240", "240_300"],
        right=False,
    ).astype(str)


def normalize_predictions(raw: pd.DataFrame, models: list[str], market_window_seconds: int) -> tuple[pd.DataFrame, dict[str, str]]:
    columns = {
        "model": detect_column(raw, ["model_id", "model", "model_name"], "model"),
        "p": detect_column(raw, ["p_up", "probability", "y_prob"], "probability"),
        "label": detect_column(raw, ["result_up", "y_true", "label", "outcome"], "label"),
        "age": detect_column(raw, ["market_age_seconds", "age_seconds", "elapsed_seconds"], "market age"),
        "fold": detect_column(raw, ["fold_id", "fold", "test_fold"], "fold", required=False),
        "timestamp": detect_column(raw, ["prediction_ts", "timestamp", "ts"], "prediction timestamp", required=False),
        "market_start": detect_column(raw, ["market_window_start", "market_start_ts"], "market start", required=False),
        "market_end": detect_column(raw, ["market_window_end", "market_end_ts"], "market end", required=False),
        "market_key": detect_column(raw, ["market_key", "slug", "market_id"], "market key", required=False),
    }
    available = sorted(raw[columns["model"]].dropna().astype(str).unique())
    missing = [model for model in models if model not in set(available)]
    if missing:
        raise ValueError(f"Requested models not found: {missing}. Available model ids: {available[:100]}")
    raw = raw[raw[columns["model"]].astype(str).isin(models)].copy()
    out = pd.DataFrame()
    out["model"] = raw[columns["model"]].astype(str)
    out["p_up"] = pd.to_numeric(raw[columns["p"]], errors="coerce")
    out["result_up"] = pd.to_numeric(raw[columns["label"]], errors="coerce")
    out["market_age_seconds"] = pd.to_numeric(raw[columns["age"]], errors="coerce")
    if columns["market_start"]:
        out["market_start_ts"] = pd.to_datetime(raw[columns["market_start"]], utc=True, errors="coerce")
    elif columns["timestamp"]:
        ts = pd.to_datetime(raw[columns["timestamp"]], utc=True, errors="coerce")
        out["market_start_ts"] = ts.dt.floor(f"{market_window_seconds}s")
    else:
        raise ValueError("Need either prediction timestamp or market start column to assign markets.")
    if columns["timestamp"]:
        out["prediction_ts"] = pd.to_datetime(raw[columns["timestamp"]], utc=True, errors="coerce")
    else:
        out["prediction_ts"] = out["market_start_ts"] + pd.to_timedelta(out["market_age_seconds"], unit="s")
    if columns["market_end"]:
        out["market_end_ts"] = pd.to_datetime(raw[columns["market_end"]], utc=True, errors="coerce")
    else:
        out["market_end_ts"] = out["market_start_ts"] + pd.Timedelta(seconds=market_window_seconds)
    if columns["market_key"]:
        out["prediction_market_key"] = raw[columns["market_key"]].astype(str)
    else:
        out["prediction_market_key"] = out["market_start_ts"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    if columns["fold"]:
        out["fold_id"] = raw[columns["fold"]]
    out = out.dropna(subset=["prediction_ts", "market_start_ts", "p_up", "result_up", "market_age_seconds"])
    out["market_start_key"] = out["market_start_ts"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    return out.reset_index(drop=True), {key: str(value) for key, value in columns.items() if value is not None}


def load_quotes(path: Path, meta_path: Path | None, market_window_seconds: int, allow_post_end_quotes: bool) -> tuple[pd.DataFrame, dict[str, Any]]:
    if meta_path is None or meta_path == path:
        return load_quotes_minimal(path, market_window_seconds, allow_post_end_quotes)
    quote_frame, quote_diag = quotes_lib.load_quote_frame(path, market_window_seconds)
    meta, meta_diag = quotes_lib.load_market_meta(meta_path, market_window_seconds)
    quote_frame = quotes_lib.apply_metadata(quote_frame, meta)
    before_window = len(quote_frame)
    if not allow_post_end_quotes and not quote_frame.empty:
        quote_frame = quote_frame[
            quote_frame["quote_ts"].notna()
            & quote_frame["market_start_ts"].notna()
            & quote_frame["market_end_ts"].notna()
            & (quote_frame["quote_ts"] >= quote_frame["market_start_ts"])
            & (quote_frame["quote_ts"] <= quote_frame["market_end_ts"])
        ].copy()
    quote_frame["market_start_key"] = pd.to_datetime(quote_frame["market_start_ts"], utc=True, errors="coerce").dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    quote_frame = quote_frame.dropna(subset=["quote_ts", "market_start_key"]).sort_values(["market_start_key", "quote_ts"]).reset_index(drop=True)
    diagnostics = {
        "quote_rows_loaded": quote_diag.get("loaded_rows", 0),
        "quote_rows_parsed": int(len(quote_frame)),
        "quote_rows_before_window_filter": int(before_window),
        "quote_rows_dropped_outside_window": int(before_window - len(quote_frame)),
        "quote_markets_found": int(quote_frame["market_start_key"].nunique()) if not quote_frame.empty else 0,
        "meta": meta_diag,
    }
    return quote_frame, diagnostics


def join_nearest_quotes(predictions: pd.DataFrame, quotes: pd.DataFrame, tolerance_seconds: float) -> pd.DataFrame:
    if predictions.empty or quotes.empty:
        out = predictions.copy()
        out["quote_join_status"] = "missing_quote"
        return out
    needed = ["market_start_key", "quote_ts", "market_key", "market_slug", "yes_ask", "no_ask", "yes_ask_size", "no_ask_size", "yes_bid", "no_bid"]
    quote_cols = [col for col in needed if col in quotes.columns]
    frames = []
    tolerance = pd.Timedelta(seconds=tolerance_seconds)
    for key, pred_group in predictions.groupby("market_start_key", sort=False):
        q = quotes[quotes["market_start_key"] == key]
        if q.empty:
            missing = pred_group.copy()
            missing["quote_join_status"] = "missing_quote"
            frames.append(missing)
            continue
        joined = pd.merge_asof(
            pred_group.sort_values("prediction_ts"),
            q[quote_cols].sort_values("quote_ts"),
            left_on="prediction_ts",
            right_on="quote_ts",
            direction="nearest",
            tolerance=tolerance,
        )
        joined["quote_join_status"] = np.where(joined["quote_ts"].notna(), "joined", "missing_quote")
        frames.append(joined)
    out = pd.concat(frames, ignore_index=True) if frames else predictions.copy()
    out["quote_lag_seconds"] = (out["prediction_ts"] - out["quote_ts"]).dt.total_seconds().abs()
    return out


def make_opportunities(joined: pd.DataFrame) -> pd.DataFrame:
    out = joined.copy()
    out["yes_edge"] = out["p_up"] - pd.to_numeric(out.get("yes_ask"), errors="coerce")
    out["no_edge"] = (1.0 - out["p_up"]) - pd.to_numeric(out.get("no_ask"), errors="coerce")
    out["best_side"] = np.where(out["yes_edge"] >= out["no_edge"], "YES", "NO")
    out["best_edge"] = np.where(out["best_side"] == "YES", out["yes_edge"], out["no_edge"])
    out["age_bucket"] = age_bucket(pd.to_numeric(out["market_age_seconds"], errors="coerce"))
    return out


def expand_trades(opportunities: pd.DataFrame, thresholds: list[float], min_entry_price: float, max_entry_price: float, min_ask_size: float, fee_bps: float, slippage_bps: float) -> pd.DataFrame:
    rows = []
    valid = opportunities[opportunities["quote_join_status"].eq("joined")].copy()
    for threshold in thresholds:
        frame = valid.copy()
        yes_ok = frame["yes_edge"] >= threshold
        no_ok = frame["no_edge"] >= threshold
        frame["side"] = np.where(yes_ok & (~no_ok | (frame["yes_edge"] >= frame["no_edge"])), "YES", np.where(no_ok, "NO", None))
        frame = frame[frame["side"].notna()].copy()
        if frame.empty:
            continue
        frame["selected_price"] = np.where(frame["side"].eq("YES"), frame["yes_ask"], frame["no_ask"]).astype(float)
        frame["ask_size"] = np.where(frame["side"].eq("YES"), frame.get("yes_ask_size"), frame.get("no_ask_size"))
        frame["predicted_edge"] = np.where(frame["side"].eq("YES"), frame["yes_edge"], frame["no_edge"]).astype(float)
        frame = frame[(frame["selected_price"] >= min_entry_price) & (frame["selected_price"] <= max_entry_price)]
        if min_ask_size > 0:
            frame = frame[pd.to_numeric(frame["ask_size"], errors="coerce").fillna(0.0) >= min_ask_size]
        friction = frame["selected_price"] * ((fee_bps + slippage_bps) / 10000.0)
        frame["entry_price"] = frame["selected_price"] + friction
        win = np.where(frame["side"].eq("YES"), frame["result_up"].eq(1.0), frame["result_up"].eq(0.0))
        frame["hit"] = win.astype(float)
        frame["pnl_per_contract"] = np.where(win, 1.0 - frame["entry_price"], -frame["entry_price"])
        frame["roi_per_dollar"] = frame["pnl_per_contract"] / frame["entry_price"].replace(0.0, np.nan)
        frame["realized_edge"] = np.where(frame["side"].eq("YES"), frame["result_up"] - frame["yes_ask"], (1.0 - frame["result_up"]) - frame["no_ask"])
        frame["edge_threshold"] = threshold
        rows.append(frame)
    trades = pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()
    keep = [
        "model",
        "prediction_market_key",
        "market_start_key",
        "prediction_ts",
        "market_age_seconds",
        "fold_id",
        "p_up",
        "result_up",
        "edge_threshold",
        "side",
        "yes_ask",
        "no_ask",
        "selected_price",
        "entry_price",
        "predicted_edge",
        "realized_edge",
        "pnl_per_contract",
        "roi_per_dollar",
        "hit",
        "quote_ts",
        "quote_lag_seconds",
        "ask_size",
        "age_bucket",
    ]
    return trades[[col for col in keep if col in trades.columns]] if not trades.empty else trades


def max_drawdown(values: pd.Series) -> float | None:
    if values.empty:
        return None
    cumulative = values.cumsum()
    drawdown = cumulative - cumulative.cummax()
    return float(drawdown.min())


def summarize_trades(trades: pd.DataFrame, opportunities: pd.DataFrame, group_cols: list[str]) -> pd.DataFrame:
    if trades.empty:
        return pd.DataFrame()
    rows = []
    opp_counts = opportunities.groupby([col for col in group_cols if col in opportunities.columns], dropna=False).size().rename("joined_rows").reset_index()
    for keys, group in trades.groupby(group_cols, dropna=False):
        if not isinstance(keys, tuple):
            keys = (keys,)
        row = dict(zip(group_cols, keys))
        roi = pd.to_numeric(group["roi_per_dollar"], errors="coerce")
        row.update(
            {
                "trades": int(len(group)),
                "markets_traded": int(group["market_start_key"].nunique()) if "market_start_key" in group else None,
                "yes_trades": int(group["side"].eq("YES").sum()),
                "no_trades": int(group["side"].eq("NO").sum()),
                "avg_selected_price": float(group["selected_price"].mean()),
                "avg_predicted_edge": float(group["predicted_edge"].mean()),
                "avg_realized_edge": float(group["realized_edge"].mean()),
                "hit_rate": float(group["hit"].mean()),
                "mean_pnl_per_contract": float(group["pnl_per_contract"].mean()),
                "total_pnl_per_contract": float(group["pnl_per_contract"].sum()),
                "mean_roi_per_dollar": float(roi.mean()),
                "median_roi_per_dollar": float(roi.median()),
                "p10_roi_per_dollar": float(np.nanpercentile(roi, 10)),
                "p90_roi_per_dollar": float(np.nanpercentile(roi, 90)),
                "sharpe_like_mean_over_std_roi": float(roi.mean() / roi.std(ddof=0)) if float(roi.std(ddof=0) or 0.0) > 0 else None,
                "max_drawdown": max_drawdown(group.sort_values("prediction_ts")["pnl_per_contract"]),
                "avg_market_age_seconds": float(group["market_age_seconds"].mean()),
                "avg_quote_lag_seconds": float(group["quote_lag_seconds"].mean()),
            }
        )
        rows.append(row)
    out = pd.DataFrame(rows)
    if not opp_counts.empty:
        out = out.merge(opp_counts, on=[col for col in group_cols if col in opp_counts.columns], how="left")
        out["trade_rate"] = out["trades"] / out["joined_rows"].replace(0, np.nan)
    return out


def summary_by_model_window_threshold(trades: pd.DataFrame, opportunities: pd.DataFrame, windows: list[str], thresholds: list[float]) -> pd.DataFrame:
    rows = []
    for window in windows:
        opp = opportunities.loc[window_mask(opportunities["market_age_seconds"], window)].copy()
        for threshold in thresholds:
            tr = trades[(trades["edge_threshold"] == threshold) & window_mask(trades["market_age_seconds"], window)].copy() if not trades.empty else pd.DataFrame()
            if tr.empty:
                continue
            summary = summarize_trades(tr, opp, ["model", "edge_threshold"])
            summary["timing_window"] = window
            summary["prediction_rows"] = int(len(opportunities.loc[window_mask(opportunities["market_age_seconds"], window)]))
            summary["quote_join_rate"] = float(opp["quote_join_status"].eq("joined").mean()) if len(opp) else None
            rows.append(summary)
    return pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()


def edge_bucket_summary(trades: pd.DataFrame) -> pd.DataFrame:
    if trades.empty:
        return pd.DataFrame()
    out = trades.copy()
    out["edge_bucket"] = pd.cut(
        out["predicted_edge"],
        bins=[-np.inf, 0, 0.01, 0.02, 0.03, 0.05, 0.07, 0.10, np.inf],
        labels=["lt_0", "0_1pct", "1_2pct", "2_3pct", "3_5pct", "5_7pct", "7_10pct", "10pct_plus"],
    ).astype(str)
    return summarize_trades(out, out, ["model", "age_bucket", "edge_bucket"])


def render_readme(summary: pd.DataFrame, fold_summary: pd.DataFrame, diagnostics: dict[str, Any]) -> str:
    lines = [
        "Probability edge replay vs recorded quotes",
        "",
        "Offline research only. This does not train models, place trades, or change live behavior.",
        "Buy-only executable prices use corrected direct YES/NO best asks from recorded CLOB books.",
        "Binance proxy labels are not final Chainlink/Polymarket settlement truth.",
        "No HMM/regime filter is included.",
        "",
        f"quote_join_rate={diagnostics.get('quote_join_rate')}",
        "",
    ]
    if summary.empty:
        return "\n".join(lines + ["No trades passed the configured edge thresholds."]) + "\n"
    for window in ["pre_180", "pre_218", "60_120", "120_180", "180_218"]:
        subset = summary[summary["timing_window"] == window].sort_values("mean_roi_per_dollar", ascending=False).head(8)
        lines.append(f"Top rows by mean ROI for {window}:")
        if subset.empty:
            lines.append("- none")
        for _, row in subset.iterrows():
            lines.append(
                f"- {row['model']} thr={row['edge_threshold']:.2f} trades={int(row['trades'])} "
                f"mean_roi={row['mean_roi_per_dollar']:.4f} total_pnl={row['total_pnl_per_contract']:.4f}"
            )
        lines.append("")
    champ = summary[(summary["model"] == "calibrated_logistic__gbm_rv30") & (summary["timing_window"].isin(["pre_180", "pre_218"])) & (summary["edge_threshold"].isin([0.01, 0.02, 0.03, 0.05]))]
    lines.append("calibrated_logistic__gbm_rv30 selected thresholds:")
    if champ.empty:
        lines.append("- none")
    for _, row in champ.sort_values(["timing_window", "edge_threshold"]).iterrows():
        lines.append(f"- {row['timing_window']} thr={row['edge_threshold']:.2f} trades={int(row['trades'])} mean_roi={row['mean_roi_per_dollar']:.4f} total_pnl={row['total_pnl_per_contract']:.4f}")
    if diagnostics.get("quote_join_rate", 0) < 0.5:
        lines.append("")
        lines.append("Warning: quote join rate is low; interpret replay results as coverage-limited.")
    return "\n".join(lines) + "\n"


def run(args: argparse.Namespace) -> dict[str, Any]:
    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=True)
    models = parse_csv(args.models) if args.models else DEFAULT_MODELS
    thresholds = parse_floats(args.edge_thresholds)
    windows = DEFAULT_WINDOWS if args.windows == "default" else parse_csv(args.windows)
    pred_range = prediction_market_start_range(Path(args.predictions))
    quote_range = quote_path_date_range(Path(args.quotes))
    if ranges_disjoint(pred_range, quote_range):
        diagnostics = {
            "prediction_rows_loaded": 0,
            "prediction_rows_after_model_filter_and_quote_market_filter": 0,
            "models_requested": models,
            "prediction_market_start_min": pred_range[0],
            "prediction_market_start_max": pred_range[1],
            "quote_path_date_min": quote_range[0],
            "quote_path_date_max": quote_range[1],
            "joined_rows": 0,
            "missing_quote_rows": 0,
            "quote_join_rate": 0.0,
            "no_overlap": True,
            "note": "Prediction market window dates and recorder quote path dates do not overlap; replay not attempted.",
        }
        write_empty_outputs(out, diagnostics, args)
        return diagnostics

    quotes, quote_diag = load_quotes(Path(args.quotes), args.market_meta, args.market_window_seconds, args.allow_post_end_quotes)
    quote_market_starts = set(quotes["market_start_key"].dropna().unique()) if not quotes.empty else set()
    raw_predictions = read_prediction_frame(Path(args.predictions), models, quote_market_starts)
    predictions, detected_columns = normalize_predictions(raw_predictions, models, args.market_window_seconds)
    predictions = predictions[predictions["market_start_key"].isin(quote_market_starts)].reset_index(drop=True)
    joined = join_nearest_quotes(predictions, quotes, args.quote_tolerance_seconds)
    opportunities = make_opportunities(joined)
    trades = expand_trades(opportunities, thresholds, args.min_entry_price, args.max_entry_price, args.min_ask_size, args.fee_bps, args.slippage_bps)

    by_model_window = summary_by_model_window_threshold(trades, opportunities, windows, thresholds)
    by_edge = edge_bucket_summary(trades)
    by_age = summarize_trades(trades, trades, ["model", "edge_threshold", "age_bucket"]) if not trades.empty else pd.DataFrame()
    by_fold = summarize_trades(trades, trades, ["model", "edge_threshold", "fold_id", "age_bucket"]) if not trades.empty and "fold_id" in trades.columns else pd.DataFrame()

    diagnostics = {
        "prediction_rows_loaded": int(len(raw_predictions)),
        "prediction_rows_after_model_filter_and_quote_market_filter": int(len(predictions)),
        "models_requested": models,
        "models_found": sorted(predictions["model"].dropna().unique().tolist()) if not predictions.empty else [],
        "markets_found_in_predictions": int(predictions["market_start_key"].nunique()) if not predictions.empty else 0,
        "joined_rows": int(opportunities["quote_join_status"].eq("joined").sum()) if not opportunities.empty else 0,
        "missing_quote_rows": int(opportunities["quote_join_status"].ne("joined").sum()) if not opportunities.empty else 0,
        "quote_join_rate": float(opportunities["quote_join_status"].eq("joined").mean()) if len(opportunities) else 0.0,
        "quote_lag_quantiles": opportunities.loc[opportunities["quote_join_status"].eq("joined"), "quote_lag_seconds"].quantile([0.5, 0.9, 0.99]).to_dict() if not opportunities.empty else {},
        "rows_with_missing_yes_ask": int(opportunities["yes_ask"].isna().sum()) if "yes_ask" in opportunities else 0,
        "rows_with_missing_no_ask": int(opportunities["no_ask"].isna().sum()) if "no_ask" in opportunities else 0,
        "detected_prediction_columns": detected_columns,
        **quote_diag,
    }
    write_frame(trades, out / "replay_trades")
    write_frame(opportunities, out / "replay_opportunities")
    by_model_window.to_csv(out / "replay_summary_by_model_window_threshold.csv", index=False)
    by_edge.to_csv(out / "replay_summary_by_edge_bucket.csv", index=False)
    by_age.to_csv(out / "replay_summary_by_age_bucket.csv", index=False)
    by_fold.to_csv(out / "replay_summary_by_fold.csv", index=False)
    pd.DataFrame([diagnostics]).to_csv(out / "quote_join_diagnostics.csv", index=False)
    (out / "quote_join_diagnostics.json").write_text(json.dumps(diagnostics, indent=2, default=str), encoding="utf-8")
    config = vars(args).copy()
    config["models"] = models
    config["edge_thresholds"] = thresholds
    config["windows"] = windows
    (out / "replay_config.json").write_text(json.dumps(config, indent=2, default=str), encoding="utf-8")
    (out / "replay_scorecard_readme.txt").write_text(render_readme(by_model_window, by_fold, diagnostics), encoding="utf-8")
    if args.dry_run:
        print(json.dumps(diagnostics, indent=2, default=str))
    return diagnostics


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Offline replay of probability edge against recorded Polymarket BTC 5m quotes.")
    parser.add_argument("--predictions", type=Path, required=True)
    parser.add_argument("--quotes", type=Path, required=True)
    parser.add_argument("--market-meta", type=Path)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--models")
    parser.add_argument("--edge-thresholds", default="0.00,0.01,0.02,0.03,0.05,0.07,0.10")
    parser.add_argument("--quote-tolerance-seconds", type=float, default=3.0)
    parser.add_argument("--market-window-seconds", type=int, default=300)
    parser.add_argument("--min-ask-size", type=float, default=0.0)
    parser.add_argument("--max-entry-price", type=float, default=0.99)
    parser.add_argument("--min-entry-price", type=float, default=0.01)
    parser.add_argument("--fee-bps", type=float, default=0.0)
    parser.add_argument("--slippage-bps", type=float, default=0.0)
    parser.add_argument("--windows", default="default")
    parser.add_argument("--allow-post-end-quotes", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    diagnostics = run(build_parser().parse_args(argv))
    print(json.dumps(diagnostics, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
