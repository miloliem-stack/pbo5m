#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


MODEL_ID = "brownian_zero_drift__rv30"
PREDICTION_JOIN_TOLERANCE = pd.Timedelta("10min")
BTC_JOIN_TOLERANCE = pd.Timedelta("2min")
SPLITS = {
    "discovery": ("2026-04-23T00:00:00Z", "2026-05-12T00:00:00Z"),
    "validation": ("2026-05-12T00:00:00Z", "2026-05-23T00:00:00Z"),
    "holdout": ("2026-05-23T00:00:00Z", "2026-05-29T00:00:00Z"),
}
BTC_CONTEXT_COLUMNS = [
    "return_1m",
    "return_5m",
    "return_15m",
    "return_30m",
    "return_1h",
    "realized_vol_5m",
    "realized_vol_15m",
    "realized_vol_30m",
    "realized_vol_1h",
    "sign_flip_rate_5m",
    "sign_flip_rate_15m",
    "sign_flip_rate_30m",
    "shock_flag_5m",
    "shock_age_minutes",
    "range_position_30m",
    "range_position_1h",
]


def bool_arg(value: str | bool) -> bool:
    if isinstance(value, bool):
        return value
    lower = str(value).strip().lower()
    if lower in {"1", "true", "yes", "y", "on"}:
        return True
    if lower in {"0", "false", "no", "n", "off"}:
        return False
    raise argparse.ArgumentTypeError(f"expected bool, got {value!r}")


def parse_ts(value: Any) -> pd.Timestamp | None:
    if value in (None, ""):
        return None
    parsed = pd.to_datetime(value, utc=True, errors="coerce")
    if pd.isna(parsed):
        raise ValueError(f"could not parse timestamp: {value!r}")
    return pd.Timestamp(parsed)


def utc_series(values: pd.Series) -> pd.Series:
    return pd.to_datetime(values, utc=True, errors="coerce")


def resolve_output_paths(out: Path) -> tuple[Path, Path]:
    if out.suffix == ".parquet":
        return out, out.parent
    return out / "opportunity_tape.parquet", out


def load_compact(compact_root: Path, valid_topbook_only: bool, start_ts: pd.Timestamp | None, end_ts: pd.Timestamp | None) -> tuple[pd.DataFrame, pd.DataFrame]:
    windows_path = compact_root / "market_windows.parquet"
    ticks_path = compact_root / "book_ticks.parquet"
    if not windows_path.exists():
        raise FileNotFoundError(f"missing compact market windows: {windows_path}")
    if not ticks_path.exists():
        raise FileNotFoundError(f"missing compact book ticks: {ticks_path}")
    windows = pd.read_parquet(windows_path)
    ticks = pd.read_parquet(ticks_path)
    windows["market_start_ts"] = utc_series(windows["market_start_ts"])
    windows["market_end_ts"] = utc_series(windows["market_end_ts"])
    ticks["ts"] = utc_series(ticks["ts"])
    if valid_topbook_only and "is_valid_topbook" in ticks.columns:
        ticks = ticks[ticks["is_valid_topbook"].fillna(False)].copy()
    if start_ts is not None:
        ticks = ticks[ticks["ts"] >= start_ts].copy()
    if end_ts is not None:
        ticks = ticks[ticks["ts"] < end_ts].copy()
    return windows, ticks


def load_predictions(predictions_root: Path, model_id: str) -> pd.DataFrame:
    files = sorted(predictions_root.rglob("*.parquet")) if predictions_root.is_dir() else [predictions_root]
    if not files:
        raise FileNotFoundError(f"no prediction parquet files under {predictions_root}")
    frames = [pd.read_parquet(path) for path in files]
    pred = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    if pred.empty:
        raise RuntimeError(f"empty predictions under {predictions_root}")
    if "model_id" in pred.columns:
        pred = pred[pred["model_id"].astype(str).eq(model_id)].copy()
    if pred.empty:
        raise RuntimeError(f"missing requested prediction model: {model_id}")
    required = {"timestamp", "market_window_start", "p_up"}
    missing = sorted(required - set(pred.columns))
    if missing:
        raise RuntimeError(f"prediction columns missing: {missing}")
    pred["timestamp"] = utc_series(pred["timestamp"])
    pred["market_window_start"] = utc_series(pred["market_window_start"])
    return pred.dropna(subset=["timestamp", "market_window_start", "p_up"]).sort_values("timestamp").reset_index(drop=True)


def side_depth_usd(frame: pd.DataFrame, top_n_levels: int) -> pd.Series:
    total = pd.Series(0.0, index=frame.index, dtype=float)
    for idx in range(1, int(top_n_levels) + 1):
        px_col = f"ask_px_{idx}"
        sz_col = f"ask_sz_{idx}"
        if px_col in frame.columns and sz_col in frame.columns:
            px = pd.to_numeric(frame[px_col], errors="coerce").fillna(0.0)
            sz = pd.to_numeric(frame[sz_col], errors="coerce").fillna(0.0)
            total += px * sz
    return total


def attach_window_metadata(ticks: pd.DataFrame, windows: pd.DataFrame, top_n_levels: int) -> pd.DataFrame:
    meta_cols = [
        "market_key",
        "market_id",
        "condition_id",
        "slug",
        "yes_token_id",
        "no_token_id",
        "market_start_ts",
        "market_end_ts",
        "reference_price",
        "winner_side",
        "chainlink_reference_quality",
        "chainlink_close_quality",
    ]
    available = [col for col in meta_cols if col in windows.columns]
    out = ticks.merge(windows[available], on="market_key", how="left")
    out = out[out["condition_id"].notna()].copy()
    out["market_slug"] = out.get("slug")
    out["side"] = out["side"].astype(str).str.upper()
    out["token_id"] = np.where(out["side"].eq("YES"), out["yes_token_id"], out["no_token_id"])
    out["market_age_seconds"] = pd.to_numeric(out.get("market_age_sec"), errors="coerce")
    out["seconds_to_expiry"] = pd.to_numeric(out.get("seconds_to_end"), errors="coerce")
    out["valid_topbook"] = out.get("is_valid_topbook", True)
    out["ask"] = pd.to_numeric(out.get("ask_px_1"), errors="coerce")
    out["bid"] = pd.to_numeric(out.get("bid_px_1"), errors="coerce")
    out["selected_side_ask"] = out["ask"]
    out["side_top_depth_10_usd"] = side_depth_usd(out, top_n_levels)
    return out


def attach_wide_quote_features(tape: pd.DataFrame) -> pd.DataFrame:
    base = tape[["market_key", "ts", "side", "ask", "bid", "side_top_depth_10_usd"]].copy()
    yes = base[base["side"].eq("YES")].rename(
        columns={"ask": "yes_ask", "bid": "yes_bid", "side_top_depth_10_usd": "yes_top_depth_10_usd"}
    )[["market_key", "ts", "yes_ask", "yes_bid", "yes_top_depth_10_usd"]]
    no = base[base["side"].eq("NO")].rename(
        columns={"ask": "no_ask", "bid": "no_bid", "side_top_depth_10_usd": "no_top_depth_10_usd"}
    )[["market_key", "ts", "no_ask", "no_bid", "no_top_depth_10_usd"]]
    wide = yes.merge(no, on=["market_key", "ts"], how="outer")
    out = tape.merge(wide, on=["market_key", "ts"], how="left")
    yes_fallback = pd.Series(np.where(out["side"].eq("YES"), out["ask"], np.nan), index=out.index)
    no_fallback = pd.Series(np.where(out["side"].eq("NO"), out["ask"], np.nan), index=out.index)
    out["yes_ask"] = out["yes_ask"].fillna(yes_fallback)
    out["no_ask"] = out["no_ask"].fillna(no_fallback)
    out["spread"] = pd.to_numeric(out.get("spread"), errors="coerce")
    if out["spread"].isna().all():
        out["spread"] = out["ask"] - out["bid"]
    return out


def join_predictions(tape: pd.DataFrame, windows: pd.DataFrame, pred: pd.DataFrame) -> pd.DataFrame:
    keyed = pred.merge(
        windows[["market_key", "market_start_ts"]].drop_duplicates(),
        left_on="market_window_start",
        right_on="market_start_ts",
        how="left",
    )
    keyed = keyed[keyed["market_key"].notna()].copy()
    if keyed.empty:
        out = tape.copy()
        out["_p_up"] = np.nan
        out["prediction_ts"] = pd.NaT
        return out
    keyed["market_key"] = keyed["market_key"].astype(tape["market_key"].dtype)
    rename = {
        "timestamp": "prediction_ts",
        "p_up": "_p_up",
        "rv_30m": "rv30",
        "S_t": "prediction_source_price",
    }
    keep = [c for c in ["market_key", "timestamp", "p_up", "rv_30m", "model_id", "market_window_start", "market_window_end", "S_t"] if c in keyed.columns]
    pred_out = keyed[keep].rename(columns=rename).sort_values(["market_key", "prediction_ts"])

    pieces: list[pd.DataFrame] = []
    for market_key, left in tape.sort_values(["market_key", "ts"]).groupby("market_key", sort=False):
        right = pred_out[pred_out["market_key"].eq(market_key)].sort_values("prediction_ts")
        if right.empty:
            merged = left.copy()
            for col in [c for c in pred_out.columns if c not in {"market_key"}]:
                merged[col] = np.nan
        else:
            merged = pd.merge_asof(
                left.sort_values("ts"),
                right.drop(columns=["market_key"]).sort_values("prediction_ts"),
                left_on="ts",
                right_on="prediction_ts",
                direction="backward",
                tolerance=PREDICTION_JOIN_TOLERANCE,
            )
        pieces.append(merged)
    out = pd.concat(pieces, ignore_index=True) if pieces else tape.copy()
    future = out["prediction_ts"].notna() & (out["prediction_ts"] > out["ts"])
    if future.any():
        raise RuntimeError(f"future prediction join detected: {int(future.sum())} rows")
    return out


def load_binance_1m(data_root: Path, start_ts: pd.Timestamp, end_ts: pd.Timestamp) -> pd.DataFrame:
    root = data_root / "binance-btc1m"
    files = sorted(root.glob("BTCUSDT-1m-*.csv"))
    if not files:
        return pd.DataFrame(columns=["ts", "close"])
    rows = []
    for path in files:
        frame = pd.read_csv(path, header=None, usecols=[0, 4], names=["open_time", "close"])
        rows.append(frame)
    raw = pd.concat(rows, ignore_index=True)
    raw["ts"] = pd.to_datetime(raw["open_time"], unit="us", utc=True, errors="coerce")
    raw["close"] = pd.to_numeric(raw["close"], errors="coerce")
    raw = raw.dropna(subset=["ts", "close"]).drop_duplicates("ts").sort_values("ts")
    return raw[(raw["ts"] >= start_ts - pd.Timedelta("2h")) & (raw["ts"] <= end_ts + pd.Timedelta("2h"))].reset_index(drop=True)


def compute_btc_context(binance: pd.DataFrame) -> pd.DataFrame:
    if binance.empty:
        return binance
    out = binance[["ts", "close"]].copy().sort_values("ts")
    out["log_ret"] = np.log(out["close"] / out["close"].shift(1))
    for n, label in [(1, "1m"), (5, "5m"), (15, "15m"), (30, "30m"), (60, "1h")]:
        out[f"return_{label}"] = out["log_ret"].shift(1).rolling(n, min_periods=n).sum()
    for n, label in [(5, "5m"), (15, "15m"), (30, "30m"), (60, "1h")]:
        out[f"realized_vol_{label}"] = out["log_ret"].shift(1).rolling(n, min_periods=n).std(ddof=0)
    signs = np.sign(out["log_ret"])
    flips = ((signs != signs.shift(1)) & (signs != 0) & (signs.shift(1) != 0)).astype(float)
    for n, label in [(5, "5m"), (15, "15m"), (30, "30m")]:
        out[f"sign_flip_rate_{label}"] = flips.shift(1).rolling(n, min_periods=n).mean()
    out["shock_flag_5m"] = (out["return_5m"].abs() > 2.0 * out["realized_vol_1h"]).astype(float)
    last_shock = np.nan
    ages = []
    for idx, flag in enumerate(out["shock_flag_5m"]):
        if flag == 1.0:
            last_shock = idx
        ages.append(np.nan if np.isnan(last_shock) else idx - last_shock)
    out["shock_age_minutes"] = ages
    for n, label in [(30, "30m"), (60, "1h")]:
        prior = out["close"].shift(1)
        high = prior.rolling(n, min_periods=n).max()
        low = prior.rolling(n, min_periods=n).min()
        rng = (high - low).replace(0.0, np.nan)
        out[f"range_position_{label}"] = ((prior - low) / rng).fillna(0.5)
    return out


def attach_btc_context(tape: pd.DataFrame, binance: pd.DataFrame) -> pd.DataFrame:
    out = tape.copy()
    if binance.empty:
        out["btc_price_at_ts"] = np.nan
        for col in BTC_CONTEXT_COLUMNS:
            out[col] = np.nan
        return out
    features = compute_btc_context(binance)
    left = pd.DataFrame({"_row": np.arange(len(out)), "ts": out["ts"]}).sort_values("ts")
    right = features.sort_values("ts")
    joined = pd.merge_asof(left, right, on="ts", direction="backward", tolerance=BTC_JOIN_TOLERANCE)
    if (joined["ts"] > left["ts"]).any():
        raise RuntimeError("future BTC context join detected")
    joined = joined.sort_values("_row")
    out["btc_price_at_ts"] = joined["close"].to_numpy()
    for col in BTC_CONTEXT_COLUMNS:
        out[col] = joined[col].to_numpy() if col in joined.columns else np.nan
    return out


def compute_final_columns(tape: pd.DataFrame, model_id: str) -> pd.DataFrame:
    out = tape.copy()
    out["p_yes_model"] = pd.to_numeric(out["_p_up"], errors="coerce")
    out["p_no_model"] = 1.0 - out["p_yes_model"]
    out["p_side_model"] = np.where(out["side"].eq("YES"), out["p_yes_model"], out["p_no_model"])
    out["raw_edge"] = out["p_side_model"] - out["ask"]
    out["expected_value_per_share_raw"] = out["raw_edge"]
    out["expected_roi_raw"] = np.where(out["ask"] > 0, out["raw_edge"] / out["ask"], np.nan)
    out["model_id"] = model_id
    out["prediction_source_ts"] = out.get("prediction_ts")
    out["settlement_winning_side"] = out.get("winner_side")
    out["won_if_bought"] = out["settlement_winning_side"].astype(str).str.upper().eq(out["side"]).astype(float)
    out.loc[out["settlement_winning_side"].isna(), "won_if_bought"] = np.nan
    out["realized_payout_per_share"] = out["won_if_bought"]
    out["realized_pnl_per_share"] = out["realized_payout_per_share"] - out["ask"]
    out["realized_roi_if_bought"] = np.where(out["ask"] > 0, out["realized_pnl_per_share"] / out["ask"], np.nan)
    out["market_yes_mid_implied"] = (out["yes_ask"] + (1.0 - out["no_ask"])) / 2.0
    out["market_prob_spread"] = out["yes_ask"] + out["no_ask"] - 1.0
    out["model_market_gap_yes"] = out["p_yes_model"] - out["market_yes_mid_implied"]
    side_mid = np.where(out["side"].eq("YES"), out["market_yes_mid_implied"], 1.0 - out["market_yes_mid_implied"])
    out["model_market_gap_side"] = out["p_side_model"] - side_mid
    edge_yes = out["p_yes_model"] - out["yes_ask"]
    edge_no = out["p_no_model"] - out["no_ask"]
    out["best_buy_edge"] = np.fmax(edge_yes, edge_no)
    out["best_buy_side"] = np.where(edge_yes >= edge_no, "YES", "NO")
    out["is_best_buy_side"] = out["side"].eq(out["best_buy_side"])
    out["distance_from_reference"] = out["btc_price_at_ts"] - out["reference_price"]
    out["distance_from_reference_bps"] = (out["distance_from_reference"] / out["reference_price"].replace(0, np.nan)) * 10000.0
    out["signed_distance_for_side_bps"] = np.where(out["side"].eq("YES"), out["distance_from_reference_bps"], -out["distance_from_reference_bps"])
    if "quote_age_ms" not in out.columns:
        out["quote_age_ms"] = np.nan
    return out


def output_columns(frame: pd.DataFrame) -> list[str]:
    requested = [
        "ts",
        "condition_id",
        "market_id",
        "market_slug",
        "side",
        "token_id",
        "market_start_ts",
        "market_end_ts",
        "market_age_seconds",
        "seconds_to_expiry",
        "reference_price",
        "btc_price_at_ts",
        "distance_from_reference",
        "distance_from_reference_bps",
        "signed_distance_for_side_bps",
        "yes_ask",
        "no_ask",
        "selected_side_ask",
        "ask",
        "bid",
        "spread",
        "yes_top_depth_10_usd",
        "no_top_depth_10_usd",
        "side_top_depth_10_usd",
        "quote_age_ms",
        "valid_topbook",
        "p_yes_model",
        "p_no_model",
        "p_side_model",
        "raw_edge",
        "expected_value_per_share_raw",
        "expected_roi_raw",
        "model_id",
        "prediction_ts",
        "prediction_source_ts",
        "rv30",
        "settlement_winning_side",
        "won_if_bought",
        "realized_payout_per_share",
        "realized_pnl_per_share",
        "realized_roi_if_bought",
        "market_yes_mid_implied",
        "market_prob_spread",
        "model_market_gap_yes",
        "model_market_gap_side",
        "best_buy_edge",
        "best_buy_side",
        "is_best_buy_side",
        *BTC_CONTEXT_COLUMNS,
        "market_key",
    ]
    return [col for col in requested if col in frame.columns]


def coverage_report(tape: pd.DataFrame, windows: pd.DataFrame, drop_counts: Counter[str]) -> tuple[dict[str, Any], pd.DataFrame]:
    summary: dict[str, Any] = {
        "number_of_markets": int(windows["condition_id"].nunique()) if "condition_id" in windows.columns else int(len(windows)),
        "number_of_opportunity_rows": int(len(tape)),
        "quote_coverage": float(tape["valid_topbook"].fillna(False).mean()) if len(tape) else 0.0,
        "prediction_coverage": float(tape["p_yes_model"].notna().mean()) if len(tape) else 0.0,
        "label_coverage": float(tape["settlement_winning_side"].notna().mean()) if len(tape) else 0.0,
        "dropped_row_counts_by_reason": dict(drop_counts),
    }
    rows = []
    for name, (start, end) in SPLITS.items():
        start_ts = parse_ts(start)
        end_ts = parse_ts(end)
        sub = tape[(tape["ts"] >= start_ts) & (tape["ts"] < end_ts)] if len(tape) else tape
        rows.append(
            {
                "split": name,
                "start_ts": start_ts,
                "end_ts": end_ts,
                "rows": len(sub),
                "conditions": sub["condition_id"].nunique() if len(sub) else 0,
                "prediction_coverage": float(sub["p_yes_model"].notna().mean()) if len(sub) else 0.0,
                "label_coverage": float(sub["settlement_winning_side"].notna().mean()) if len(sub) else 0.0,
            }
        )
    return summary, pd.DataFrame(rows)


def build_tape(args: argparse.Namespace) -> tuple[pd.DataFrame, dict[str, Any], pd.DataFrame]:
    start_ts = parse_ts(args.start_ts)
    end_ts = parse_ts(args.end_ts)
    windows, ticks = load_compact(args.compact_root, args.valid_topbook_only, start_ts, end_ts)
    drop_counts: Counter[str] = Counter()
    before = len(ticks)
    tape = attach_window_metadata(ticks, windows, args.top_n_levels)
    drop_counts["missing_condition_id"] = before - len(tape)
    tape = attach_wide_quote_features(tape)
    pred = load_predictions(args.predictions_root, args.model_id)
    tape = join_predictions(tape, windows, pred)
    binance_start = start_ts or tape["ts"].min()
    binance_end = end_ts or tape["ts"].max()
    binance = load_binance_1m(args.data_root, binance_start, binance_end)
    tape = attach_btc_context(tape, binance)
    tape = compute_final_columns(tape, args.model_id)
    tape = tape.sort_values(["ts", "condition_id", "side"], kind="mergesort").reset_index(drop=True)
    missing_predictions = int(tape["p_yes_model"].isna().sum())
    drop_counts["missing_prediction"] = missing_predictions
    if args.fail_on_missing_predictions and missing_predictions:
        raise RuntimeError(f"prediction_coverage_incomplete: missing_prediction_rows={missing_predictions} total_rows={len(tape)}")
    tape = tape[output_columns(tape)].copy()
    summary, cov = coverage_report(tape, windows, drop_counts)
    return tape, summary, cov


README = """BTC-5m Brownian opportunity tape.

Each row is one side-level candidate at one recorded quote timestamp. The tape is
not filtered by edge or by policy eligibility. Policy tooling must apply filters
first, then select the first eligible row per condition_id.

Leakage rules:
- Prediction joins are backward-only by market window.
- BTC context joins are backward-only and rolling context uses prior bars.
- Chainlink settlement labels are included only for evaluation and must not be
  used as policy inputs.
"""


def write_outputs(tape: pd.DataFrame, summary: dict[str, Any], coverage: pd.DataFrame, out: Path, args: argparse.Namespace) -> dict[str, Any]:
    parquet_path, out_dir = resolve_output_paths(out)
    out_dir.mkdir(parents=True, exist_ok=True)
    tape.to_parquet(parquet_path, index=False)
    tape.head(500).to_csv(out_dir / "opportunity_tape_sample.csv", index=False)
    coverage.to_csv(out_dir / "coverage_report.csv", index=False)
    payload = {
        "model_id": args.model_id,
        "compact_root": str(args.compact_root),
        "predictions_root": str(args.predictions_root),
        "out": str(parquet_path),
        **summary,
    }
    (out_dir / "build_summary.json").write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    (out_dir / "README.txt").write_text(README, encoding="utf-8")
    return payload


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build a causal BTC-5m Brownian opportunity tape.")
    parser.add_argument("--compact-root", type=Path, required=True)
    parser.add_argument("--predictions-root", type=Path, required=True)
    parser.add_argument("--model-id", default=MODEL_ID)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--data-root", type=Path, default=Path("data"))
    parser.add_argument("--start-ts")
    parser.add_argument("--end-ts")
    parser.add_argument("--valid-topbook-only", type=bool_arg, default=True)
    parser.add_argument("--top-n-levels", type=int, default=10)
    parser.add_argument("--fail-on-missing-predictions", type=bool_arg, nargs="?", const=True, default=True)
    return parser


def run(args: argparse.Namespace) -> dict[str, Any]:
    tape, summary, cov = build_tape(args)
    return write_outputs(tape, summary, cov, args.out, args)


def main(argv: list[str] | None = None) -> int:
    summary = run(build_parser().parse_args(argv))
    print(json.dumps(summary, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
