#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import shutil
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

from scripts import run_hmm_regime_veto_attribution as hmm_veto


DEFAULT_REPLAY_PATH = Path("artifacts/market_age_policy_replay/compact_20260423_20260511_state3_ask_age_v1/trade_level_policy_results.parquet")
DEFAULT_OUTPUT_DIR = Path("artifacts/bankroll_compounding_replay/brownian_no_hmm_fresh_v1")
DEFAULT_MODEL_ID = "brownian_zero_drift__rv30"
DEFAULT_POLICY_NAME = "base_all_models_original_like"


@dataclass(frozen=True)
class SizingPolicy:
    name: str
    accounting_mode: str
    fixed_fraction: float | None = None
    kelly_fraction_multiplier: float | None = None
    max_fraction_per_market: float | None = None


def parse_csv(value: str | None) -> list[str]:
    if value is None:
        return []
    return [item.strip() for item in str(value).split(",") if item.strip()]


def parse_optional_ts(value: str | None) -> pd.Timestamp | None:
    if value is None or str(value).strip() == "":
        return None
    ts = pd.to_datetime(value, utc=True, errors="raise")
    return pd.Timestamp(ts).as_unit("ns")


def _json_default(value: Any) -> Any:
    if isinstance(value, (pd.Timestamp,)):
        return value.isoformat()
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return float(value)
    if isinstance(value, (np.bool_,)):
        return bool(value)
    return str(value)


def git_commit() -> str | None:
    try:
        return subprocess.check_output(["git", "rev-parse", "HEAD"], text=True, stderr=subprocess.DEVNULL).strip()
    except Exception:
        return None


def find_col(frame: pd.DataFrame, candidates: list[str], *, required: bool = True) -> str | None:
    for col in candidates:
        if col in frame.columns:
            return col
    if required:
        raise ValueError(f"missing required column; tried {candidates}")
    return None


def binary_return_on_cost(ask: float, outcome: int | bool) -> float:
    ask = float(ask)
    if not np.isfinite(ask) or ask <= 0:
        raise ValueError(f"invalid ask for binary return: {ask}")
    return (1.0 / ask - 1.0) if bool(outcome) else -1.0


def full_kelly_fraction(probability: float, ask: float) -> float:
    probability = float(probability)
    ask = float(ask)
    if not np.isfinite(probability) or not np.isfinite(ask) or ask >= 1.0:
        return 0.0
    return max(0.0, (probability - ask) / (1.0 - ask))


def selected_probability(row: pd.Series) -> float:
    side = str(row["selected_side"]).upper()
    p_yes = float(row["model_p_yes"])
    return p_yes if side == "YES" else 1.0 - p_yes


def compute_max_drawdown(values: pd.Series) -> float:
    series = pd.to_numeric(values, errors="coerce").dropna()
    if series.empty:
        return 0.0
    peaks = series.cummax()
    drawdown = series / peaks - 1.0
    return float(drawdown.min())


def capacity_usd_for_row(row: pd.Series) -> float:
    ask = float(row["selected_ask"])
    side = str(row["selected_side"]).lower()
    depth_cols = [f"ask_sz_1_{side}", f"ask_sz_2_{side}", f"ask_sz_3_{side}"]
    if any(col in row.index for col in depth_cols):
        shares = 0.0
        for col in depth_cols:
            value = pd.to_numeric(row.get(col), errors="coerce") if col in row.index else np.nan
            if pd.notna(value):
                shares += float(value)
        return max(0.0, float(ask * shares))
    if bool(row.get("capacity_shortfall", False)) and "gross_cost" in row.index and pd.notna(row.get("gross_cost")):
        return max(0.0, float(row["gross_cost"]))
    return float("inf")


def normalize_replay(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    ts_col = find_col(out, ["entry_ts", "ts", "timestamp"])
    market_col = find_col(out, ["market_key", "market_id", "condition_id"])
    ask_col = find_col(out, ["entry_ask", "ask_price", "selected_ask"])
    edge_col = find_col(out, ["model_edge", "selected_edge", "best_edge"])
    age_col = find_col(out, ["entry_age_seconds", "entry_age_sec", "market_age_sec"])
    model_col = find_col(out, ["model_id", "model_name"])
    p_col = find_col(out, ["p_yes", "model_p_yes"])
    side_col = find_col(out, ["side", "selected_side"])
    winner_col = find_col(out, ["winner_side"], required=False)
    win_col = find_col(out, ["win"], required=False)
    end_col = find_col(out, ["market_end_ts", "settlement_ts", "resolution_ts"], required=False)
    start_col = find_col(out, ["market_start_ts"], required=False)

    out["decision_ts"] = pd.to_datetime(out[ts_col], utc=True, errors="coerce").dt.as_unit("ns")
    out["market_key_for_sizing"] = out[market_col].astype(str)
    out["selected_ask"] = pd.to_numeric(out[ask_col], errors="coerce")
    out["selected_edge"] = pd.to_numeric(out[edge_col], errors="coerce")
    out["market_age_sec"] = pd.to_numeric(out[age_col], errors="coerce")
    out["model_id_for_sizing"] = out[model_col].astype(str)
    out["model_p_yes"] = pd.to_numeric(out[p_col], errors="coerce")
    out["selected_side"] = out[side_col].astype(str).str.upper()
    if start_col:
        out["market_start_ts_for_sizing"] = pd.to_datetime(out[start_col], utc=True, errors="coerce").dt.as_unit("ns")
    else:
        out["market_start_ts_for_sizing"] = pd.NaT
    if end_col:
        out["settlement_ts"] = pd.to_datetime(out[end_col], utc=True, errors="coerce").dt.as_unit("ns")
    else:
        out["settlement_ts"] = out["decision_ts"]
    out["settlement_ts"] = out["settlement_ts"].where(out["settlement_ts"].notna(), out["decision_ts"])
    if win_col:
        out["outcome"] = out[win_col].astype(bool).astype(int)
    elif winner_col:
        out["outcome"] = out["selected_side"].eq(out[winner_col].astype(str).str.upper()).astype(int)
    else:
        raise ValueError("replay rows must include either win or winner_side")
    out["selected_probability"] = np.where(out["selected_side"].eq("YES"), out["model_p_yes"], 1.0 - out["model_p_yes"])
    out["return_on_cost"] = [binary_return_on_cost(ask, outcome) for ask, outcome in zip(out["selected_ask"], out["outcome"])]
    out["entry_date_for_sizing"] = out["decision_ts"].dt.date.astype(str)
    if "chronological_slice" not in out.columns:
        out["chronological_slice"] = "all"
    if "policy_name" not in out.columns:
        out["policy_name"] = "unknown"
    if "entry_age_window" not in out.columns:
        out["entry_age_window"] = "unknown"
    if "ask_bin" not in out.columns:
        out["ask_bin"] = pd.cut(out["selected_ask"], [-np.inf, 0.30, 0.35, 0.40, 0.45, 0.47, np.inf]).astype(str)
    return out


def select_policy_trades(frame: pd.DataFrame, args: argparse.Namespace) -> pd.DataFrame:
    out = normalize_replay(frame)
    mask = out["model_id_for_sizing"].eq(args.model_id)
    if args.source_policy_name:
        mask &= out["policy_name"].astype(str).eq(args.source_policy_name)
    start_ts = parse_optional_ts(getattr(args, "start_ts", None))
    end_ts = parse_optional_ts(getattr(args, "end_ts", None))
    if start_ts is not None:
        mask &= out["decision_ts"].ge(start_ts)
    if end_ts is not None:
        mask &= out["decision_ts"].lt(end_ts)
    slices = parse_csv(args.chronological_slices)
    if slices:
        mask &= out["chronological_slice"].astype(str).isin(slices)
    if "stake_size" in out.columns and args.fixed_stake_usd is not None:
        mask &= pd.to_numeric(out["stake_size"], errors="coerce").eq(float(args.fixed_stake_usd))
    mask &= out["market_age_sec"].ge(float(args.min_age_sec)) & out["market_age_sec"].le(float(args.max_age_sec))
    mask &= out["selected_ask"].gt(float(args.min_ask)) & out["selected_ask"].lt(float(args.max_ask))
    mask &= out["selected_edge"].ge(float(args.min_edge))
    out = out[mask].dropna(subset=["decision_ts", "selected_ask", "selected_edge", "model_p_yes"]).copy()
    out = out.sort_values(["decision_ts", "market_key_for_sizing", "selected_edge"], ascending=[True, True, False], kind="mergesort")
    before = len(out)
    out = out.drop_duplicates(["market_key_for_sizing"], keep="first").copy()
    out["deduped_rows_removed"] = before - len(out)
    out["capacity_usd"] = [capacity_usd_for_row(row) for _, row in out.iterrows()]
    return out.reset_index(drop=True)


def additive_summary(trades: pd.DataFrame, fixed_stake_usd: float) -> dict[str, Any]:
    if trades.empty:
        return {
            "accounting_mode": "additive_fixed_notional",
            "fixed_stake_usd": fixed_stake_usd,
            "trade_count": 0,
            "unique_markets": 0,
            "gross_cost": 0.0,
            "pnl": 0.0,
            "roi": np.nan,
        }
    if {"gross_cost", "pnl"}.issubset(trades.columns):
        gross_cost = float(pd.to_numeric(trades["gross_cost"], errors="coerce").sum())
        pnl = float(pd.to_numeric(trades["pnl"], errors="coerce").sum())
        source = "replay_gross_cost_pnl"
    else:
        stake = np.minimum(float(fixed_stake_usd), pd.to_numeric(trades["capacity_usd"], errors="coerce").replace([np.inf], float(fixed_stake_usd)).fillna(float(fixed_stake_usd)))
        gross_cost = float(stake.sum())
        pnl = float((stake * pd.to_numeric(trades["return_on_cost"], errors="coerce")).sum())
        source = "recomputed_binary_return"
    return {
        "accounting_mode": "additive_fixed_notional",
        "fixed_stake_usd": float(fixed_stake_usd),
        "trade_count": int(len(trades)),
        "unique_markets": int(trades["market_key_for_sizing"].nunique()),
        "gross_cost": gross_cost,
        "pnl": pnl,
        "roi": float(pnl / gross_cost) if gross_cost else np.nan,
        "win_rate": float(pd.to_numeric(trades["outcome"], errors="coerce").mean()) if len(trades) else np.nan,
        "avg_ask": float(pd.to_numeric(trades["selected_ask"], errors="coerce").mean()) if len(trades) else np.nan,
        "avg_edge": float(pd.to_numeric(trades["selected_edge"], errors="coerce").mean()) if len(trades) else np.nan,
        "accounting_source": source,
    }


def release_settled(open_trades: list[dict[str, Any]], now: pd.Timestamp, cash: float, daily_pnl: dict[str, float], path_rows: list[dict[str, Any]], policy: SizingPolicy) -> tuple[list[dict[str, Any]], float]:
    remaining = []
    for item in open_trades:
        if item["settlement_ts"] <= now:
            cash += item["stake"] + item["pnl"]
            day = item["settlement_ts"].date().isoformat()
            daily_pnl[day] = daily_pnl.get(day, 0.0) + item["pnl"]
            path_rows.append(
                {
                    **item["path_base"],
                    "event_type": "settlement",
                    "accounting_policy": policy.name,
                    "accounting_mode": policy.accounting_mode,
                    "event_ts": item["settlement_ts"],
                    "cash": cash,
                    "reserved_exposure": sum(x["stake"] for x in open_trades if x is not item),
                    "realized_pnl": item["pnl"],
                    "bankroll": cash + sum(x["stake"] for x in open_trades if x is not item),
                    "skip_reason": "",
                }
            )
        else:
            remaining.append(item)
    return remaining, cash


def stake_for_policy(row: pd.Series, policy: SizingPolicy, cash: float, args: argparse.Namespace) -> tuple[float, float]:
    if policy.accounting_mode == "bankroll_fixed_fraction":
        fraction = float(policy.fixed_fraction or 0.0)
        return cash * fraction, fraction
    if policy.accounting_mode == "bankroll_fractional_kelly":
        full = full_kelly_fraction(float(row["selected_probability"]), float(row["selected_ask"]))
        fraction = float(policy.kelly_fraction_multiplier or 0.0) * full
        if policy.max_fraction_per_market is not None:
            fraction = min(fraction, float(policy.max_fraction_per_market))
        return cash * fraction, fraction
    raise ValueError(f"unsupported bankroll policy: {policy}")


def simulate_bankroll_policy(trades: pd.DataFrame, policy: SizingPolicy, args: argparse.Namespace) -> tuple[dict[str, Any], pd.DataFrame, pd.DataFrame]:
    cash = float(args.initial_bankroll_usd)
    initial = cash
    open_trades: list[dict[str, Any]] = []
    daily_pnl: dict[str, float] = {}
    path_rows: list[dict[str, Any]] = []
    accepted_rows: list[dict[str, Any]] = []
    skipped: dict[str, int] = {}

    for idx, row in trades.sort_values("decision_ts", kind="mergesort").iterrows():
        now = row["decision_ts"]
        open_trades, cash = release_settled(open_trades, now, cash, daily_pnl, path_rows, policy)
        reserved = sum(item["stake"] for item in open_trades)
        day = now.date().isoformat()
        base = {
            "source_row_index": int(idx),
            "decision_ts": now,
            "settlement_ts": row["settlement_ts"],
            "market_key": row["market_key_for_sizing"],
            "market_id": row.get("market_id", row["market_key_for_sizing"]),
            "chronological_slice": row.get("chronological_slice", "all"),
            "model_id": row["model_id_for_sizing"],
            "selected_side": row["selected_side"],
            "selected_ask": float(row["selected_ask"]),
            "selected_edge": float(row["selected_edge"]),
            "selected_probability": float(row["selected_probability"]),
            "outcome": int(row["outcome"]),
            "return_on_cost": float(row["return_on_cost"]),
            "capacity_usd": float(row["capacity_usd"]),
        }
        if args.max_daily_loss_usd is not None and daily_pnl.get(day, 0.0) <= -abs(float(args.max_daily_loss_usd)):
            skipped["daily_loss"] = skipped.get("daily_loss", 0) + 1
            path_rows.append({**base, "event_type": "skip", "accounting_policy": policy.name, "accounting_mode": policy.accounting_mode, "event_ts": now, "cash": cash, "reserved_exposure": reserved, "bankroll": cash + reserved, "stake": 0.0, "pnl": 0.0, "kelly_fraction": 0.0, "skip_reason": "daily_loss"})
            continue
        raw_stake, fraction = stake_for_policy(row, policy, cash, args)
        stake = min(float(raw_stake), float(args.max_stake_usd), cash)
        if args.max_open_exposure_usd is not None:
            stake = min(stake, max(0.0, float(args.max_open_exposure_usd) - reserved))
        if np.isfinite(float(row["capacity_usd"])):
            stake = min(stake, float(row["capacity_usd"]))
        if stake < float(args.min_stake_usd):
            skipped["too_small"] = skipped.get("too_small", 0) + 1
            path_rows.append({**base, "event_type": "skip", "accounting_policy": policy.name, "accounting_mode": policy.accounting_mode, "event_ts": now, "cash": cash, "reserved_exposure": reserved, "bankroll": cash + reserved, "stake": float(max(stake, 0.0)), "pnl": 0.0, "kelly_fraction": float(fraction), "skip_reason": "too_small"})
            continue
        pnl = stake * float(row["return_on_cost"])
        cash -= stake
        accepted = {**base, "accounting_policy": policy.name, "accounting_mode": policy.accounting_mode, "stake": float(stake), "pnl": float(pnl), "kelly_fraction": float(fraction), "cash_after_entry": float(cash), "reserved_after_entry": float(reserved + stake), "skip_reason": ""}
        accepted_rows.append(accepted)
        item = {"settlement_ts": row["settlement_ts"], "stake": float(stake), "pnl": float(pnl), "path_base": base}
        open_trades.append(item)
        path_rows.append({**accepted, "event_type": "entry", "event_ts": now, "cash": cash, "reserved_exposure": reserved + stake, "bankroll": cash + reserved + stake})

    final_ts = pd.Timestamp.max.tz_localize("UTC")
    open_trades, cash = release_settled(open_trades, final_ts, cash, daily_pnl, path_rows, policy)
    accepted_df = pd.DataFrame(accepted_rows)
    path = pd.DataFrame(path_rows)
    bankroll_series = pd.to_numeric(path["bankroll"], errors="coerce") if "bankroll" in path.columns else pd.Series(dtype=float)
    summary = {
        "accounting_policy": policy.name,
        "accounting_mode": policy.accounting_mode,
        "initial_bankroll": initial,
        "final_bankroll": float(cash),
        "total_return": float(cash / initial - 1.0) if initial else np.nan,
        "max_drawdown": compute_max_drawdown(bankroll_series),
        "trade_count": int(len(accepted_df)),
        "skipped_trades": int(sum(skipped.values())),
        "skipped_by_reason": skipped,
        "average_stake": float(accepted_df["stake"].mean()) if not accepted_df.empty else 0.0,
        "median_stake": float(accepted_df["stake"].median()) if not accepted_df.empty else 0.0,
        "max_stake": float(accepted_df["stake"].max()) if not accepted_df.empty else 0.0,
        "average_kelly_fraction": float(accepted_df["kelly_fraction"].mean()) if not accepted_df.empty else 0.0,
        "max_kelly_fraction": float(accepted_df["kelly_fraction"].max()) if not accepted_df.empty else 0.0,
        "ruin": bool(cash <= 0),
    }
    return summary, path, accepted_df


def build_sizing_policies() -> list[SizingPolicy]:
    return [
        SizingPolicy("fixed_1pct_bankroll", "bankroll_fixed_fraction", fixed_fraction=0.01),
        SizingPolicy("kelly_1_20_cap_2pct", "bankroll_fractional_kelly", kelly_fraction_multiplier=1.0 / 20.0, max_fraction_per_market=0.02),
        SizingPolicy("kelly_1_10_cap_3pct", "bankroll_fractional_kelly", kelly_fraction_multiplier=1.0 / 10.0, max_fraction_per_market=0.03),
        SizingPolicy("kelly_1_5_cap_5pct", "bankroll_fractional_kelly", kelly_fraction_multiplier=1.0 / 5.0, max_fraction_per_market=0.05),
    ]


def by_date(additive_trades: pd.DataFrame, bankroll_paths: pd.DataFrame) -> pd.DataFrame:
    rows = []
    if not additive_trades.empty:
        group = additive_trades.copy()
        group["date"] = group["decision_ts"].dt.date.astype(str)
        for date, part in group.groupby("date"):
            cost = float(pd.to_numeric(part.get("gross_cost", pd.Series(dtype=float)), errors="coerce").sum())
            pnl = float(pd.to_numeric(part.get("pnl", pd.Series(dtype=float)), errors="coerce").sum())
            rows.append({"date": date, "accounting_policy": "fixed_5usd_additive", "accounting_mode": "additive_fixed_notional", "trade_count": int(len(part)), "gross_cost": cost, "pnl": pnl, "roi": pnl / cost if cost else np.nan})
    if not bankroll_paths.empty and "event_type" in bankroll_paths.columns:
        entries = bankroll_paths[bankroll_paths["event_type"].eq("entry")].copy()
        if not entries.empty:
            entries["date"] = pd.to_datetime(entries["decision_ts"], utc=True).dt.date.astype(str)
            for (policy, mode, date), part in entries.groupby(["accounting_policy", "accounting_mode", "date"], dropna=False):
                stake = float(pd.to_numeric(part["stake"], errors="coerce").sum())
                pnl = float(pd.to_numeric(part["pnl"], errors="coerce").sum())
                day_path = bankroll_paths[
                    bankroll_paths["accounting_policy"].eq(policy)
                    & bankroll_paths["accounting_mode"].eq(mode)
                    & pd.to_datetime(bankroll_paths["event_ts"], utc=True, errors="coerce").dt.date.astype(str).eq(date)
                ].sort_values("event_ts")
                ending_bankroll = float(pd.to_numeric(day_path["bankroll"], errors="coerce").dropna().iloc[-1]) if not day_path.empty and pd.to_numeric(day_path["bankroll"], errors="coerce").notna().any() else np.nan
                rows.append({"date": date, "accounting_policy": policy, "accounting_mode": mode, "trade_count": int(len(part)), "gross_cost": stake, "pnl": pnl, "roi": pnl / stake if stake else np.nan, "ending_bankroll": ending_bankroll})
    return pd.DataFrame(rows)


def write_readme(output_dir: Path, args: argparse.Namespace, manifest: dict[str, Any], additive: dict[str, Any], bankroll: list[dict[str, Any]]) -> None:
    best = sorted(bankroll, key=lambda x: x.get("final_bankroll", 0.0), reverse=True)[:3]
    lines = [
        "BTC-5m bankroll compounding replay",
        "",
        "Offline research only. No live trading behavior was changed.",
        "",
        "Policy filter:",
        f"- model_id == {args.model_id}",
        f"- source_policy_name == {args.source_policy_name}",
        f"- chronological_slices == {args.chronological_slices or 'all'}",
        f"- start_ts <= decision_ts when provided: {args.start_ts or 'none'}",
        f"- decision_ts < end_ts when provided: {args.end_ts or 'none'}",
        f"- {args.min_age_sec} <= market_age_sec <= {args.max_age_sec}",
        f"- {args.min_ask} < selected_ask < {args.max_ask}",
        f"- selected_edge >= {args.min_edge}",
        "- one entry per market is enforced before sizing",
        "- no HMM gate",
        "",
        "Settlement timing:",
        "The simulator releases reserved stake plus payout at market_end_ts when available; otherwise it falls back to decision_ts and records that caveat in run_manifest.json.",
        "",
        f"selected_trades={manifest.get('selected_trades')}",
        f"additive_pnl={additive.get('pnl')}",
        f"additive_roi={additive.get('roi')}",
        "",
        "Top bankroll policies by final bankroll:",
    ]
    for item in best:
        lines.append(f"- {item['accounting_policy']}: final_bankroll={item['final_bankroll']:.6f}, total_return={item['total_return']:.6f}, max_drawdown={item['max_drawdown']:.6f}")
    output_dir.joinpath("README.txt").write_text("\n".join(lines) + "\n", encoding="utf-8")


def run(args: argparse.Namespace) -> dict[str, Any]:
    output_dir = Path(args.output_dir)
    if output_dir.exists():
        if not args.overwrite:
            raise FileExistsError(f"{output_dir} exists; pass --overwrite")
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    if not Path(args.replay_path).exists():
        raise FileNotFoundError(
            f"replay artifact not found: {args.replay_path}. "
            "Build it first with scripts/run_market_age_policy_replay.py, or point --replay-path at an existing trade_level_policy_results.parquet."
        )
    replay = hmm_veto.read_frame(args.replay_path)
    selected = select_policy_trades(replay, args)
    additive = additive_summary(selected, float(args.fixed_stake_usd))

    all_paths = []
    all_sized = []
    bankroll_summaries = []
    for policy in build_sizing_policies():
        summary, path, sized = simulate_bankroll_policy(selected, policy, args)
        bankroll_summaries.append(summary)
        if not path.empty:
            all_paths.append(path)
        if not sized.empty:
            all_sized.append(sized)

    path_frame = pd.concat(all_paths, ignore_index=True) if all_paths else pd.DataFrame()
    sized_frame = pd.concat(all_sized, ignore_index=True) if all_sized else pd.DataFrame()
    if not path_frame.empty:
        path_frame.to_csv(output_dir / "bankroll_path.csv", index=False)
    else:
        pd.DataFrame().to_csv(output_dir / "bankroll_path.csv", index=False)
    hmm_veto.write_frame(sized_frame, output_dir / "selected_trades_with_sizing.parquet")
    by_date(selected, path_frame).to_csv(output_dir / "by_date.csv", index=False)

    manifest = {
        "replay_path": str(args.replay_path),
        "output_dir": str(output_dir),
        "git_commit": git_commit(),
        "replay_rows_loaded": int(len(replay)),
        "selected_trades": int(len(selected)),
        "selected_unique_markets": int(selected["market_key_for_sizing"].nunique()) if not selected.empty else 0,
        "policy_filter": {
            "model_id": args.model_id,
            "source_policy_name": args.source_policy_name,
            "start_ts": str(parse_optional_ts(args.start_ts)) if parse_optional_ts(args.start_ts) is not None else None,
            "end_ts": str(parse_optional_ts(args.end_ts)) if parse_optional_ts(args.end_ts) is not None else None,
            "end_ts_is_exclusive": True,
            "chronological_slices": parse_csv(args.chronological_slices),
            "min_age_sec": float(args.min_age_sec),
            "max_age_sec": float(args.max_age_sec),
            "min_ask": float(args.min_ask),
            "max_ask": float(args.max_ask),
            "min_edge": float(args.min_edge),
            "hmm_gate": "disabled",
            "final_minute_live_trading": "disabled",
        },
        "settlement_fallback_rows": int(pd.to_datetime(replay["market_end_ts"], utc=True, errors="coerce").isna().sum()) if "market_end_ts" in replay.columns else int(len(replay)),
        "capacity_depth_cap_available": bool(any(col.startswith("ask_sz_") for col in replay.columns)),
        "risk_inputs": {
            "initial_bankroll_usd": float(args.initial_bankroll_usd),
            "fixed_stake_usd": float(args.fixed_stake_usd),
            "max_stake_usd": float(args.max_stake_usd),
            "min_stake_usd": float(args.min_stake_usd),
            "max_daily_loss_usd": args.max_daily_loss_usd,
            "max_open_exposure_usd": args.max_open_exposure_usd,
        },
    }
    summary = {
        "additive_fixed_notional": additive,
        "bankroll": bankroll_summaries,
        "manifest": manifest,
    }
    (output_dir / "additive_summary.json").write_text(json.dumps(additive, indent=2, default=_json_default), encoding="utf-8")
    (output_dir / "bankroll_summary.json").write_text(json.dumps(bankroll_summaries, indent=2, default=_json_default), encoding="utf-8")
    (output_dir / "summary.json").write_text(json.dumps(summary, indent=2, default=_json_default), encoding="utf-8")
    (output_dir / "run_manifest.json").write_text(json.dumps(manifest, indent=2, default=_json_default), encoding="utf-8")
    schema = {
        "summary_json": ["additive_fixed_notional", "bankroll", "manifest"],
        "bankroll_path_csv": ["accounting_policy", "accounting_mode", "event_type", "event_ts", "decision_ts", "settlement_ts", "market_key", "selected_side", "selected_ask", "selected_probability", "stake", "pnl", "cash", "reserved_exposure", "bankroll", "skip_reason"],
        "selected_trades_with_sizing_parquet": ["accounting_policy", "accounting_mode", "decision_ts", "settlement_ts", "market_key", "selected_side", "selected_ask", "selected_edge", "selected_probability", "stake", "pnl", "kelly_fraction", "cash_after_entry", "reserved_after_entry"],
        "by_date_csv": ["date", "accounting_policy", "accounting_mode", "trade_count", "gross_cost", "pnl", "roi"],
    }
    (output_dir / "output_schema.json").write_text(json.dumps(schema, indent=2), encoding="utf-8")
    write_readme(output_dir, args, manifest, additive, bankroll_summaries)
    return summary


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="BTC-5m offline bankroll compounding replay for Brownian no-HMM fresh holdout policy.")
    parser.add_argument("--replay-path", type=Path, default=DEFAULT_REPLAY_PATH)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--model-id", default=DEFAULT_MODEL_ID)
    parser.add_argument("--source-policy-name", default=DEFAULT_POLICY_NAME)
    parser.add_argument("--chronological-slices", default="")
    parser.add_argument("--start-ts", default=None, help="Inclusive decision timestamp lower bound, e.g. 2026-04-23T00:00:00Z.")
    parser.add_argument("--end-ts", default=None, help="Exclusive decision timestamp upper bound, e.g. 2026-05-08T00:00:00Z.")
    parser.add_argument("--min-age-sec", type=float, default=60.0)
    parser.add_argument("--max-age-sec", type=float, default=240.0)
    parser.add_argument("--min-ask", type=float, default=0.30)
    parser.add_argument("--max-ask", type=float, default=0.47)
    parser.add_argument("--min-edge", type=float, default=0.02)
    parser.add_argument("--initial-bankroll-usd", type=float, default=1000.0)
    parser.add_argument("--fixed-stake-usd", type=float, default=5.0)
    parser.add_argument("--max-stake-usd", type=float, default=250.0)
    parser.add_argument("--min-stake-usd", type=float, default=1.0)
    parser.add_argument("--max-daily-loss-usd", type=float, default=None)
    parser.add_argument("--max-open-exposure-usd", type=float, default=None)
    return parser


def main(argv: list[str] | None = None) -> int:
    print(json.dumps(run(build_parser().parse_args(argv)), indent=2, default=_json_default))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
