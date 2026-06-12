#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import shutil
import sys
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


SPLITS = {
    "discovery": ("2026-04-23T00:00:00Z", "2026-05-12T00:00:00Z"),
    "validation": ("2026-05-12T00:00:00Z", "2026-05-23T00:00:00Z"),
    "holdout": ("2026-05-23T00:00:00Z", "2026-05-29T00:00:00Z"),
}


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


def load_policy_config(path: Path) -> dict[str, Any]:
    text = path.read_text(encoding="utf-8")
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise RuntimeError("policy config must be JSON-compatible YAML because PyYAML is not a project dependency") from exc
    if "policies" not in payload or not isinstance(payload["policies"], dict):
        raise RuntimeError("policy config missing object key: policies")
    return payload


def _range_mask(series: pd.Series, *, min_value: Any = None, max_value: Any = None, max_inclusive: bool = False) -> pd.Series:
    values = pd.to_numeric(series, errors="coerce")
    mask = pd.Series(True, index=series.index)
    if min_value is not None:
        mask &= values.ge(float(min_value))
    if max_value is not None:
        mask &= values.le(float(max_value)) if max_inclusive else values.lt(float(max_value))
    return mask


def apply_policy_filters(tape: pd.DataFrame, policy: dict[str, Any]) -> pd.DataFrame:
    mask = pd.Series(True, index=tape.index)
    mask &= _range_mask(tape["market_age_seconds"], min_value=policy.get("min_market_age_seconds"), max_value=policy.get("max_market_age_seconds"))
    mask &= _range_mask(tape["ask"], min_value=policy.get("min_ask"), max_value=policy.get("max_ask"))
    mask &= _range_mask(tape["raw_edge"], min_value=policy.get("min_edge"), max_value=policy.get("max_edge"))
    if policy.get("require_best_buy_side"):
        mask &= tape["is_best_buy_side"].fillna(False).astype(bool)
    if policy.get("min_depth_usd") is not None:
        mask &= pd.to_numeric(tape["side_top_depth_10_usd"], errors="coerce").ge(float(policy["min_depth_usd"]))
    if policy.get("max_quote_age_ms") is not None and "quote_age_ms" in tape.columns:
        mask &= pd.to_numeric(tape["quote_age_ms"], errors="coerce").le(float(policy["max_quote_age_ms"]))
    for interval in policy.get("exclude_age_intervals") or []:
        start = float(interval["start"])
        end = float(interval["end"])
        age = pd.to_numeric(tape["market_age_seconds"], errors="coerce")
        mask &= ~(age.ge(start) & age.lt(end))
    side_rules = policy.get("side_rules") or {}
    if side_rules:
        side_mask = pd.Series(False, index=tape.index)
        for side, rules in side_rules.items():
            sub = tape["side"].astype(str).str.upper().eq(str(side).upper())
            sub &= _range_mask(tape["ask"], min_value=rules.get("min_ask"), max_value=rules.get("max_ask"))
            sub &= _range_mask(tape["raw_edge"], min_value=rules.get("min_edge"), max_value=rules.get("max_edge"))
            side_mask |= sub
        mask &= side_mask
    optional_ranges = {
        "model_market_gap_side": "model_market_gap_side",
        "realized_vol_5m": "realized_vol_5m",
        "realized_vol_15m": "realized_vol_15m",
        "realized_vol_30m": "realized_vol_30m",
        "realized_vol_1h": "realized_vol_1h",
        "sign_flip_rate_5m": "sign_flip_rate_5m",
        "sign_flip_rate_15m": "sign_flip_rate_15m",
        "sign_flip_rate_30m": "sign_flip_rate_30m",
        "shock_flag_5m": "shock_flag_5m",
    }
    for key, col in optional_ranges.items():
        if col not in tape.columns:
            continue
        if f"min_{key}" in policy or f"max_{key}" in policy:
            mask &= _range_mask(tape[col], min_value=policy.get(f"min_{key}"), max_value=policy.get(f"max_{key}"), max_inclusive=True)
    return tape[mask].copy()


def select_first_entries(filtered: pd.DataFrame, first_entry_per_condition: bool) -> pd.DataFrame:
    if filtered.empty:
        return filtered.copy()
    out = filtered.sort_values(["ts", "condition_id", "side"], kind="mergesort")
    if first_entry_per_condition:
        out = out.drop_duplicates("condition_id", keep="first")
    return out.reset_index(drop=True)


def add_pnl(trades: pd.DataFrame, stake_usd: float) -> pd.DataFrame:
    out = trades.copy()
    out["stake_usd"] = float(stake_usd)
    out["gross_cost"] = float(stake_usd)
    out["shares"] = np.where(pd.to_numeric(out["ask"], errors="coerce") > 0, float(stake_usd) / pd.to_numeric(out["ask"], errors="coerce"), np.nan)
    out["payout"] = out["shares"] * pd.to_numeric(out["realized_payout_per_share"], errors="coerce")
    out["pnl"] = out["payout"] - out["gross_cost"]
    out["roi_on_cost"] = np.where(out["gross_cost"] > 0, out["pnl"] / out["gross_cost"], np.nan)
    return out


def max_drawdown(equity: pd.Series) -> float:
    if equity.empty:
        return 0.0
    running_max = equity.cummax()
    return float((equity - running_max).min())


def split_name(ts: Any) -> str:
    t = parse_ts(ts)
    if t is None:
        return "unknown"
    for name, (start, end) in SPLITS.items():
        if parse_ts(start) <= t < parse_ts(end):
            return name
    return "outside"


def summarize(trades: pd.DataFrame) -> dict[str, Any]:
    if trades.empty:
        return {
            "trade_count": 0,
            "unique_markets": 0,
            "gross_cost": 0.0,
            "pnl": 0.0,
            "roi": np.nan,
            "win_rate": np.nan,
            "avg_ask": np.nan,
            "avg_edge": np.nan,
            "avg_market_age": np.nan,
            "avg_model_probability": np.nan,
            "max_drawdown": 0.0,
            "worst_day_pnl": 0.0,
            "best_day_pnl": 0.0,
        }
    pnl = pd.to_numeric(trades["pnl"], errors="coerce")
    gross = pd.to_numeric(trades["gross_cost"], errors="coerce")
    equity = pnl.cumsum()
    by_day = trades.assign(date=pd.to_datetime(trades["ts"], utc=True).dt.date).groupby("date")["pnl"].sum()
    return {
        "trade_count": int(len(trades)),
        "unique_markets": int(trades["condition_id"].nunique()),
        "gross_cost": float(gross.sum()),
        "pnl": float(pnl.sum()),
        "roi": float(pnl.sum() / gross.sum()) if gross.sum() else np.nan,
        "win_rate": float(pd.to_numeric(trades["won_if_bought"], errors="coerce").mean()),
        "avg_ask": float(pd.to_numeric(trades["ask"], errors="coerce").mean()),
        "avg_edge": float(pd.to_numeric(trades["raw_edge"], errors="coerce").mean()),
        "avg_market_age": float(pd.to_numeric(trades["market_age_seconds"], errors="coerce").mean()),
        "avg_model_probability": float(pd.to_numeric(trades["p_side_model"], errors="coerce").mean()),
        "max_drawdown": max_drawdown(equity),
        "worst_day_pnl": float(by_day.min()) if not by_day.empty else 0.0,
        "best_day_pnl": float(by_day.max()) if not by_day.empty else 0.0,
    }


def group_summary(trades: pd.DataFrame, group_cols: list[str]) -> pd.DataFrame:
    if trades.empty:
        return pd.DataFrame(columns=group_cols + ["trade_count", "gross_cost", "pnl", "roi", "win_rate"])
    rows = []
    for keys, group in trades.groupby(group_cols, dropna=False, sort=True):
        keys_tuple = keys if isinstance(keys, tuple) else (keys,)
        s = summarize(group)
        row = dict(zip(group_cols, keys_tuple))
        row.update({k: s[k] for k in ["trade_count", "gross_cost", "pnl", "roi", "win_rate"]})
        rows.append(row)
    return pd.DataFrame(rows)


def add_bins(trades: pd.DataFrame) -> pd.DataFrame:
    out = trades.copy()
    out["date"] = pd.to_datetime(out["ts"], utc=True).dt.date.astype(str)
    out["hour_utc"] = pd.to_datetime(out["ts"], utc=True).dt.hour
    out["chronological_split"] = [split_name(ts) for ts in out["ts"]]
    out["ask_bin"] = pd.cut(pd.to_numeric(out["ask"], errors="coerce"), [0, 0.3, 0.35, 0.4, 0.45, 0.5, 1.0], right=False).astype(str)
    out["market_age_bin"] = pd.cut(pd.to_numeric(out["market_age_seconds"], errors="coerce"), [0, 60, 90, 120, 180, 240, 300], right=False).astype(str)
    out["edge_bin"] = pd.cut(pd.to_numeric(out["raw_edge"], errors="coerce"), [-np.inf, 0, 0.02, 0.05, 0.08, 0.12, np.inf], right=False).astype(str)
    return out


def write_policy_outputs(name: str, trades: pd.DataFrame, output_dir: Path) -> dict[str, Any]:
    policy_dir = output_dir / name
    policy_dir.mkdir(parents=True, exist_ok=True)
    trades = add_bins(trades)
    trades.to_parquet(policy_dir / "selected_trades.parquet", index=False)
    trades.to_csv(policy_dir / "selected_trades.csv", index=False)
    summary = summarize(trades)
    summary["policy"] = name
    (policy_dir / "summary.json").write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
    trades.assign(equity=trades["pnl"].cumsum() if not trades.empty else []).to_csv(policy_dir / "equity_path.csv", index=False)
    outputs = {
        "by_date.csv": ["date"],
        "by_chronological_slice.csv": ["chronological_split"],
        "by_hour_utc.csv": ["hour_utc"],
        "by_side.csv": ["side"],
        "by_ask_bin.csv": ["ask_bin"],
        "by_market_age_bin.csv": ["market_age_bin"],
        "by_edge_bin.csv": ["edge_bin"],
        "by_side_x_ask.csv": ["side", "ask_bin"],
        "by_age_x_ask.csv": ["market_age_bin", "ask_bin"],
        "by_age_x_edge.csv": ["market_age_bin", "edge_bin"],
    }
    for filename, cols in outputs.items():
        group_summary(trades, cols).to_csv(policy_dir / filename, index=False)
    return summary


def run_selector(args: argparse.Namespace) -> dict[str, Any]:
    out_dir = Path(args.out_dir)
    if out_dir.exists() and args.overwrite:
        shutil.rmtree(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    tape = pd.read_parquet(args.tape)
    tape["ts"] = pd.to_datetime(tape["ts"], utc=True, errors="coerce")
    if args.start_ts:
        tape = tape[tape["ts"] >= parse_ts(args.start_ts)].copy()
    if args.end_ts:
        tape = tape[tape["ts"] < parse_ts(args.end_ts)].copy()
    config = load_policy_config(args.policies)
    summaries = []
    split_rows = []
    for name, policy in config["policies"].items():
        filtered = apply_policy_filters(tape, policy)
        selected = select_first_entries(filtered, args.first_entry_per_condition)
        trades = add_pnl(selected, args.stake_usd)
        summary = write_policy_outputs(name, trades, out_dir)
        summaries.append(summary)
        if not trades.empty:
            binned = add_bins(trades)
            by_split = group_summary(binned, ["chronological_split"])
            by_split.insert(0, "policy", name)
            split_rows.append(by_split)
    comparison = pd.DataFrame(summaries)
    comparison.to_csv(out_dir / "policy_comparison.csv", index=False)
    split_comparison = pd.concat(split_rows, ignore_index=True) if split_rows else pd.DataFrame()
    split_comparison.to_csv(out_dir / "policy_comparison_by_split.csv", index=False)
    (out_dir / "README.txt").write_text(
        "Policies are applied to the full opportunity tape before first-entry selection per condition_id.\n"
        "Settlement labels are used only after selection to compute PnL.\n",
        encoding="utf-8",
    )
    return {"policies": list(config["policies"]), "out_dir": str(out_dir), "policy_count": len(summaries)}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run causal Brownian policy rules over an opportunity tape.")
    parser.add_argument("--tape", type=Path, required=True)
    parser.add_argument("--out-dir", type=Path, required=True)
    parser.add_argument("--policies", type=Path, required=True)
    parser.add_argument("--stake-usd", type=float, default=1.0)
    parser.add_argument("--first-entry-per-condition", type=bool_arg, default=True)
    parser.add_argument("--start-ts")
    parser.add_argument("--end-ts")
    parser.add_argument("--overwrite", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    result = run_selector(build_parser().parse_args(argv))
    print(json.dumps(result, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
