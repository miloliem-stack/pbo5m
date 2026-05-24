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
from scripts import run_market_age_policy_replay as age_replay
from scripts import run_probability_model_set_capacity_stress as stress


DEFAULT_MODEL_ID = "brownian_zero_drift__rv30"
POLICY_VARIANTS = [
    "raw_policy",
    "ask_gt_0.30",
    "ask_0.30_0.60",
    "expected_growth_positive",
    "expected_growth_positive_plus_ask_gt_0.30",
    "expected_growth_positive_plus_ask_0.30_0.60",
]


@dataclass(frozen=True)
class SizingPolicy:
    sizing_policy: str
    accounting_mode: str
    fixed_stake: float | None = None
    fixed_fraction: float | None = None
    kelly_multiplier: float | None = None
    cap_fraction: float | None = None


def bool_arg(value: str | bool) -> bool:
    if isinstance(value, bool):
        return value
    lower = str(value).strip().lower()
    if lower in {"1", "true", "yes", "y", "on"}:
        return True
    if lower in {"0", "false", "no", "n", "off"}:
        return False
    raise argparse.ArgumentTypeError(f"expected bool, got {value!r}")


def parse_float_csv(value: str | None) -> list[float]:
    if value is None or str(value).strip() == "":
        return []
    return [float(item.strip()) for item in str(value).split(",") if item.strip()]


def json_default(value: Any) -> Any:
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        return float(value)
    if isinstance(value, np.bool_):
        return bool(value)
    return str(value)


def git_commit() -> str | None:
    try:
        return subprocess.check_output(["git", "rev-parse", "HEAD"], text=True, stderr=subprocess.DEVNULL).strip()
    except Exception:
        return None


def full_kelly_fraction(probability: float, ask: float) -> float:
    if not np.isfinite(probability) or not np.isfinite(ask) or ask >= 1.0:
        return 0.0
    return max(0.0, (float(probability) - float(ask)) / (1.0 - float(ask)))


def haircut_probability(probability: float, haircut_abs: float) -> float:
    probability = float(probability)
    haircut_abs = float(haircut_abs)
    if probability >= 0.5:
        return max(0.5, probability - haircut_abs)
    return min(0.5, probability + haircut_abs)


def expected_log_growth(probability: float, ask: float, fraction: float) -> float:
    probability = float(probability)
    ask = float(ask)
    fraction = float(fraction)
    if not np.isfinite(probability) or not np.isfinite(ask) or not np.isfinite(fraction):
        return np.nan
    if ask <= 0.0 or ask >= 1.0 or fraction <= 0.0 or fraction >= 1.0:
        return np.nan if fraction < 0.0 or fraction >= 1.0 else 0.0
    win_growth = 1.0 + fraction * ((1.0 - ask) / ask)
    lose_growth = 1.0 - fraction
    if win_growth <= 0.0 or lose_growth <= 0.0:
        return np.nan
    return float(probability * np.log(win_growth) + (1.0 - probability) * np.log(lose_growth))


def binary_return_on_cost(ask: float, outcome: bool) -> float:
    return (1.0 / float(ask) - 1.0) if outcome else -1.0


def build_sizing_policies(fixed_stake: float) -> list[SizingPolicy]:
    return [
        SizingPolicy("additive_fixed_notional", "additive_fixed_notional", fixed_stake=float(fixed_stake)),
        SizingPolicy("fixed_fraction_0_25pct", "bankroll_fixed_fraction", fixed_fraction=0.0025),
        SizingPolicy("fixed_fraction_0_50pct", "bankroll_fixed_fraction", fixed_fraction=0.005),
        SizingPolicy("fixed_fraction_1_00pct", "bankroll_fixed_fraction", fixed_fraction=0.01),
        SizingPolicy("kelly_1_40_cap_0_25pct", "bankroll_fractional_kelly", kelly_multiplier=1.0 / 40.0, cap_fraction=0.0025),
        SizingPolicy("kelly_1_20_cap_0_50pct", "bankroll_fractional_kelly", kelly_multiplier=1.0 / 20.0, cap_fraction=0.005),
        SizingPolicy("kelly_1_10_cap_1_00pct", "bankroll_fractional_kelly", kelly_multiplier=1.0 / 10.0, cap_fraction=0.01),
        SizingPolicy("kelly_1_5_cap_2_00pct", "bankroll_fractional_kelly", kelly_multiplier=1.0 / 5.0, cap_fraction=0.02),
    ]


def sizing_policy_by_name(name: str, fixed_stake: float) -> SizingPolicy:
    policies = {p.sizing_policy: p for p in build_sizing_policies(fixed_stake)}
    if name not in policies:
        raise ValueError(f"unknown sizing policy {name}; available={sorted(policies)}")
    return policies[name]


def capacity_spend_cap(row: pd.Series, top_n_levels: int, allow_missing_depth: bool) -> tuple[float, bool]:
    suffix = "yes" if str(row["side"]).upper() == "YES" else "no"
    found = False
    spend = 0.0
    for idx in range(1, int(top_n_levels) + 1):
        px_col = f"ask_px_{idx}_{suffix}"
        sz_col = f"ask_sz_{idx}_{suffix}"
        if px_col not in row.index or sz_col not in row.index:
            continue
        found = True
        px = pd.to_numeric(row.get(px_col), errors="coerce")
        sz = pd.to_numeric(row.get(sz_col), errors="coerce")
        if np.isfinite(px) and np.isfinite(sz) and float(px) > 0.0 and float(sz) > 0.0:
            spend += float(px) * float(sz)
    if not found:
        if allow_missing_depth:
            return float("inf"), False
        raise ValueError("depth columns are missing; pass --allow-missing-depth to disable capacity caps")
    return max(0.0, spend), True


def add_trade_columns(frame: pd.DataFrame, args: argparse.Namespace) -> pd.DataFrame:
    out = age_replay.add_edge_columns(frame)
    out = out[out["model_name"].astype(str).eq(args.model_id)].copy()
    out["model_id"] = out["model_name"]
    out["selected_side"] = out["side"].astype(str).str.upper()
    out["selected_ask"] = pd.to_numeric(out["entry_ask"], errors="coerce")
    out["selected_edge"] = pd.to_numeric(out["model_edge"], errors="coerce")
    out["model_probability"] = np.where(out["selected_side"].eq("YES"), pd.to_numeric(out["p_yes"], errors="coerce"), 1.0 - pd.to_numeric(out["p_yes"], errors="coerce"))
    out["win"] = out["selected_side"].eq(out["winner_side"].astype(str).str.upper())
    out["return_on_cost"] = [binary_return_on_cost(a, w) for a, w in zip(out["selected_ask"], out["win"])]
    out["capacity_spend_cap"], depth_flags = zip(*[capacity_spend_cap(row, args.top_n_levels, args.allow_missing_depth) for _, row in out.iterrows()]) if len(out) else ([], [])
    out["capacity_depth_cap_available"] = list(depth_flags) if len(out) else []
    out["p_for_growth"] = [haircut_probability(p, args.probability_haircut_abs) for p in out["model_probability"]]
    out["ask_for_growth"] = np.minimum(0.99, pd.to_numeric(out["selected_ask"], errors="coerce") + float(args.ask_slippage_abs))
    selection_fraction = [full_kelly_fraction(p, a) for p, a in zip(out["p_for_growth"], out["ask_for_growth"])]
    out["selection_growth_fraction"] = selection_fraction
    out["selection_expected_log_growth"] = [expected_log_growth(p, a, f) for p, a, f in zip(out["p_for_growth"], out["ask_for_growth"], selection_fraction)]
    return out


def variant_mask(frame: pd.DataFrame, variant: str, args: argparse.Namespace) -> pd.Series:
    ask = pd.to_numeric(frame["selected_ask"], errors="coerce")
    growth = pd.to_numeric(frame["selection_expected_log_growth"], errors="coerce")
    mask = pd.Series(True, index=frame.index)
    if "ask_gt_0.30" in variant:
        mask &= ask.gt(0.30)
    if "ask_0.30_0.60" in variant:
        mask &= ask.gt(0.30) & ask.lt(0.60)
    if "expected_growth_positive" in variant:
        mask &= growth.gt(float(args.min_expected_log_growth))
    return mask


def select_first_entries_by_variant(candidates: pd.DataFrame, args: argparse.Namespace) -> tuple[pd.DataFrame, pd.DataFrame]:
    rows = []
    skipped = []
    base = candidates[
        pd.to_numeric(candidates["selected_edge"], errors="coerce").ge(float(args.edge_threshold))
        & pd.to_numeric(candidates["entry_age_sec"], errors="coerce").ge(float(args.entry_age_min_seconds))
        & pd.to_numeric(candidates["entry_age_sec"], errors="coerce").lt(float(args.entry_age_max_seconds))
    ].copy()
    base = base.sort_values(["market_key", "ts", "entry_age_sec"], kind="mergesort")
    for variant in POLICY_VARIANTS:
        eligible_mask = variant_mask(base, variant, args)
        rejected = base[~eligible_mask].copy()
        if "expected_growth_positive" in variant and not rejected.empty:
            growth_rejected = rejected[pd.to_numeric(rejected["selection_expected_log_growth"], errors="coerce").le(float(args.min_expected_log_growth))].copy()
            if not growth_rejected.empty:
                growth_rejected["policy_variant"] = variant
                growth_rejected["sizing_policy"] = ""
                growth_rejected["skip_reason"] = "expected_growth"
                skipped.append(growth_rejected)
        eligible = base[eligible_mask].copy()
        eligible["policy_variant"] = variant
        selected = eligible.drop_duplicates(["policy_variant", "market_key"], keep="first")
        rows.append(selected)
    selected_all = pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()
    skipped_all = pd.concat(skipped, ignore_index=True) if skipped else pd.DataFrame()
    return selected_all, skipped_all


def proposed_fraction(row: pd.Series, sizing: SizingPolicy, bankroll: float) -> float:
    if sizing.accounting_mode == "additive_fixed_notional":
        return min(float(sizing.fixed_stake or 0.0) / bankroll, 1.0) if bankroll > 0 else 0.0
    if sizing.accounting_mode == "bankroll_fixed_fraction":
        return float(sizing.fixed_fraction or 0.0)
    raw = float(sizing.kelly_multiplier or 0.0) * full_kelly_fraction(float(row["model_probability"]), float(row["selected_ask"]))
    return min(max(raw, 0.0), float(sizing.cap_fraction or raw))


def small_wallet_active(bankroll: float, args: argparse.Namespace) -> bool:
    return bool(args.small_wallet_mode) and float(bankroll) < float(args.small_wallet_threshold)


def skip_row(row: pd.Series, sizing: SizingPolicy, reason: str, bankroll: float, growth: float = np.nan, stake: float = 0.0, extra: dict[str, Any] | None = None) -> dict[str, Any]:
    out = row.to_dict()
    out.update(
        {
            "decision_ts": row.get("ts"),
            "sizing_policy": sizing.sizing_policy,
            "accounting_mode": sizing.accounting_mode,
            "effective_sizing_policy": extra.get("effective_sizing_policy") if extra else sizing.sizing_policy,
            "expected_log_growth": growth,
            "skip_reason": reason,
            "stake_spend": float(max(stake, 0.0)),
            "pnl": 0.0,
            "bankroll_before": float(bankroll),
            "bankroll_after": float(bankroll),
        }
    )
    if extra:
        out.update(extra)
    return out


def compute_drawdown_episodes(path: pd.DataFrame) -> pd.DataFrame:
    rows = []
    if path.empty:
        return pd.DataFrame()
    for (variant, sizing), group in path.sort_values("decision_ts").groupby(["policy_variant", "sizing_policy"], dropna=False):
        equity = pd.to_numeric(group["bankroll_after"], errors="coerce").reset_index(drop=True)
        times = pd.to_datetime(group["decision_ts"], utc=True, errors="coerce").reset_index(drop=True)
        peak_value = -np.inf
        peak_idx = 0
        in_dd = False
        trough_idx = 0
        trough_dd = 0.0
        for i, value in enumerate(equity):
            if not np.isfinite(value):
                continue
            if value >= peak_value:
                if in_dd:
                    rows.append(
                        {
                            "policy_variant": variant,
                            "sizing_policy": sizing,
                            "start_ts": times.iloc[peak_idx],
                            "trough_ts": times.iloc[trough_idx],
                            "recovery_ts": times.iloc[i],
                            "drawdown_amount": float(equity.iloc[trough_idx] - peak_value),
                            "drawdown_pct": float(trough_dd),
                            "trade_count": int(i - peak_idx + 1),
                        }
                    )
                    in_dd = False
                peak_value = float(value)
                peak_idx = i
                trough_idx = i
                trough_dd = 0.0
            elif peak_value > 0:
                dd = float(value / peak_value - 1.0)
                if not in_dd or dd < trough_dd:
                    in_dd = True
                    trough_idx = i
                    trough_dd = dd
        if in_dd:
            rows.append(
                {
                    "policy_variant": variant,
                    "sizing_policy": sizing,
                    "start_ts": times.iloc[peak_idx],
                    "trough_ts": times.iloc[trough_idx],
                    "recovery_ts": pd.NaT,
                    "drawdown_amount": float(equity.iloc[trough_idx] - peak_value),
                    "drawdown_pct": float(trough_dd),
                    "trade_count": int(len(equity) - peak_idx),
                }
            )
    return pd.DataFrame(rows)


def summarize_group(group: pd.DataFrame, starting_bankroll: float) -> dict[str, Any]:
    entries = group[group["skip_reason"].eq("")].copy()
    skipped = group[~group["skip_reason"].eq("")]
    gross_cost = float(pd.to_numeric(entries["stake_spend"], errors="coerce").sum()) if not entries.empty else 0.0
    pnl = float(pd.to_numeric(entries["pnl"], errors="coerce").sum()) if not entries.empty else 0.0
    bankroll_after = pd.to_numeric(entries["bankroll_after"], errors="coerce")
    ending = float(bankroll_after.iloc[-1]) if not bankroll_after.empty else float(starting_bankroll)
    ts_col = "decision_ts" if "decision_ts" in entries.columns else "ts"
    by_day = entries.assign(date=pd.to_datetime(entries[ts_col], utc=True).dt.date.astype(str)).groupby("date")["pnl"].sum() if not entries.empty else pd.Series(dtype=float)
    returns_100 = entries["pnl"].rolling(100).sum() if len(entries) else pd.Series(dtype=float)
    stake = pd.to_numeric(entries["stake_spend"], errors="coerce") if not entries.empty else pd.Series(dtype=float)
    fraction = pd.to_numeric(entries["applied_stake_fraction"], errors="coerce") if not entries.empty else pd.Series(dtype=float)
    depth_util = pd.to_numeric(entries["depth_utilization"], errors="coerce") if not entries.empty else pd.Series(dtype=float)
    return {
        "starting_bankroll": float(starting_bankroll),
        "ending_bankroll": ending,
        "total_return": ending - float(starting_bankroll),
        "total_return_pct": ending / float(starting_bankroll) - 1.0 if starting_bankroll else np.nan,
        "trade_count": int(len(entries)),
        "skipped_count": int(len(skipped)),
        "skipped_expected_growth_count": int(skipped["skip_reason"].eq("expected_growth").sum()) if not skipped.empty else 0,
        "skipped_capacity_count": int(skipped["skip_reason"].eq("capacity").sum()) if not skipped.empty else 0,
        "skipped_insufficient_bankroll_count": int(skipped["skip_reason"].eq("insufficient_bankroll").sum()) if not skipped.empty else 0,
        "skipped_below_min_order_count": int(skipped["skip_reason"].eq("below_min_order_notional").sum()) if not skipped.empty else 0,
        "skipped_daily_stop_count": int(skipped["skip_reason"].eq("daily_stop_loss_guard").sum()) if not skipped.empty else 0,
        "skipped_session_stop_count": int(skipped["skip_reason"].eq("session_stop_loss_guard").sum()) if not skipped.empty else 0,
        "gross_cost": gross_cost,
        "pnl": pnl,
        "fixed_stake_comparable_roi": pnl / gross_cost if gross_cost else np.nan,
        "win_rate": float(entries["win"].mean()) if not entries.empty else np.nan,
        "avg_ask": float(entries["selected_ask"].mean()) if not entries.empty else np.nan,
        "avg_model_probability": float(entries["model_probability"].mean()) if not entries.empty else np.nan,
        "avg_edge": float(entries["selected_edge"].mean()) if not entries.empty else np.nan,
        "avg_entry_age_seconds": float(entries["entry_age_sec"].mean()) if not entries.empty else np.nan,
        "median_entry_age_seconds": float(entries["entry_age_sec"].median()) if not entries.empty else np.nan,
        "avg_stake": float(stake.mean()) if not stake.empty else 0.0,
        "median_stake": float(stake.median()) if not stake.empty else 0.0,
        "p95_stake": float(stake.quantile(0.95)) if not stake.empty else 0.0,
        "max_stake": float(stake.max()) if not stake.empty else 0.0,
        "avg_stake_fraction": float(fraction.mean()) if not fraction.empty else 0.0,
        "p95_stake_fraction": float(fraction.quantile(0.95)) if not fraction.empty else 0.0,
        "max_stake_fraction": float(fraction.max()) if not fraction.empty else 0.0,
        "max_drawdown": float((entries["bankroll_after"] - entries["bankroll_after"].cummax()).min()) if not entries.empty else 0.0,
        "max_drawdown_pct": float((entries["bankroll_after"] / entries["bankroll_after"].cummax() - 1.0).min()) if not entries.empty else 0.0,
        "worst_day_pnl": float(by_day.min()) if not by_day.empty else 0.0,
        "worst_day_return_pct": float((by_day / starting_bankroll).min()) if not by_day.empty and starting_bankroll else np.nan,
        "worst_100_trade_pnl": float(returns_100.min()) if not returns_100.empty else np.nan,
        "worst_100_trade_return_pct": float((returns_100 / starting_bankroll).min()) if not returns_100.empty and starting_bankroll else np.nan,
        "capacity_binding_rate": float(entries["capacity_bound"].mean()) if not entries.empty else 0.0,
        "avg_depth_utilization": float(depth_util.replace([np.inf], np.nan).mean()) if not depth_util.empty else np.nan,
        "p95_depth_utilization": float(depth_util.replace([np.inf], np.nan).quantile(0.95)) if not depth_util.empty else np.nan,
        "max_depth_utilization": float(depth_util.replace([np.inf], np.nan).max()) if not depth_util.empty else np.nan,
        "largest_trade_pnl_contribution_pct": float(entries["pnl"].abs().max() / abs(pnl)) if not entries.empty and pnl else np.nan,
        "largest_date_pnl_contribution_pct": float(by_day.abs().max() / abs(pnl)) if not by_day.empty and pnl else np.nan,
        "min_bankroll_seen": float(pd.to_numeric(entries["bankroll_after"], errors="coerce").min()) if not entries.empty else float(starting_bankroll),
        "ruin_flag": bool((pd.to_numeric(entries["bankroll_after"], errors="coerce") <= 0).any()) if not entries.empty else False,
    }


def simulate_variant_sizing(selected: pd.DataFrame, variant_skips: pd.DataFrame, sizing: SizingPolicy, args: argparse.Namespace) -> tuple[pd.DataFrame, pd.DataFrame]:
    rows = []
    skips = []
    for variant, group in selected.groupby("policy_variant", sort=False):
        bankroll = float(args.starting_bankroll)
        day_start_bankroll: dict[str, float] = {}
        day_realized_pnl: dict[str, float] = {}
        session_stopped = False
        for _, row in group.sort_values("ts", kind="mergesort").iterrows():
            day = pd.Timestamp(row["ts"]).tz_convert("UTC").date().isoformat()
            if day not in day_start_bankroll:
                day_start_bankroll[day] = bankroll
                day_realized_pnl[day] = 0.0
            active_small_wallet = small_wallet_active(bankroll, args)
            effective_sizing = sizing_policy_by_name(args.small_wallet_sizing_policy, args.fixed_stake) if active_small_wallet else sizing
            effective_name = effective_sizing.sizing_policy
            if session_stopped or (active_small_wallet and bankroll <= float(args.starting_bankroll) * (1.0 - float(args.session_stop_loss_fraction))):
                session_stopped = True
                skips.append(skip_row(row, sizing, "session_stop_loss_guard", bankroll, extra={"effective_sizing_policy": effective_name}))
                continue
            if active_small_wallet and day_realized_pnl.get(day, 0.0) <= -float(args.daily_stop_loss_fraction) * day_start_bankroll[day]:
                skips.append(skip_row(row, sizing, "daily_stop_loss_guard", bankroll, extra={"effective_sizing_policy": effective_name}))
                continue
            fraction = proposed_fraction(row, effective_sizing, bankroll)
            if active_small_wallet:
                fraction = min(fraction, float(args.small_wallet_max_stake_fraction))
            p_adj = haircut_probability(float(row["model_probability"]), float(args.probability_haircut_abs))
            ask_adj = min(0.99, float(row["selected_ask"]) + float(args.ask_slippage_abs))
            growth = expected_log_growth(p_adj, ask_adj, min(max(fraction, 0.0), 0.999999))
            if "expected_growth_positive" in variant and not (np.isfinite(growth) and growth > float(args.min_expected_log_growth)):
                skips.append(skip_row(row, sizing, "expected_growth", bankroll, growth, extra={"effective_sizing_policy": effective_name}))
                continue
            stake = float(effective_sizing.fixed_stake) if effective_sizing.accounting_mode == "additive_fixed_notional" else bankroll * fraction
            if active_small_wallet:
                available_risk_bankroll = bankroll * (1.0 - float(args.reserve_bankroll_fraction))
                if available_risk_bankroll <= 0:
                    skips.append(skip_row(row, sizing, "reserve_bankroll_guard", bankroll, growth, stake, {"effective_sizing_policy": effective_name}))
                    continue
                stake = min(stake, available_risk_bankroll, bankroll * float(args.small_wallet_max_stake_fraction))
                if stake < float(args.min_order_notional) and bool(args.skip_below_min_order):
                    skips.append(skip_row(row, sizing, "below_min_order_notional", bankroll, growth, stake, {"effective_sizing_policy": effective_name}))
                    continue
                if stake < float(args.min_order_notional) and bool(args.allow_round_up_to_min_order):
                    stake = min(float(args.min_order_notional), available_risk_bankroll, bankroll * float(args.small_wallet_max_stake_fraction))
            stake = min(stake, bankroll)
            cap = float(row["capacity_spend_cap"])
            capacity_bound = np.isfinite(cap) and stake > cap
            if capacity_bound:
                stake = cap
            if stake <= 0:
                skips.append(skip_row(row, sizing, "capacity", bankroll, growth, stake, {"effective_sizing_policy": effective_name}))
                continue
            if stake < 1e-9:
                skips.append(skip_row(row, sizing, "insufficient_bankroll", bankroll, growth, stake, {"effective_sizing_policy": effective_name}))
                continue
            shares = stake / float(row["selected_ask"])
            payout = shares if bool(row["win"]) else 0.0
            pnl = payout - stake
            before = bankroll
            bankroll += pnl
            applied_fraction = stake / before if before else 0.0
            out = row.to_dict()
            out.update(
                {
                    "decision_ts": row["ts"],
                    "sizing_policy": sizing.sizing_policy,
                    "accounting_mode": sizing.accounting_mode,
                    "effective_sizing_policy": effective_name,
                    "small_wallet_mode_active": bool(active_small_wallet),
                    "proposed_full_kelly_fraction": full_kelly_fraction(float(row["model_probability"]), float(row["selected_ask"])),
                    "applied_stake_fraction": applied_fraction,
                    "stake_spend": stake,
                    "shares_bought": shares,
                    "capacity_bound": bool(capacity_bound),
                    "depth_utilization": stake / cap if np.isfinite(cap) and cap > 0 else np.nan,
                    "expected_log_growth": growth,
                    "skip_reason": "",
                    "pnl": pnl,
                    "bankroll_before": before,
                    "bankroll_after": bankroll,
                }
            )
            rows.append(out)
            day_realized_pnl[day] = day_realized_pnl.get(day, 0.0) + pnl
    sized = pd.DataFrame(rows)
    skipped = pd.concat([variant_skips, pd.DataFrame(skips)], ignore_index=True, sort=False) if not variant_skips.empty or skips else pd.DataFrame()
    return sized, skipped


def build_candidates(args: argparse.Namespace) -> tuple[pd.DataFrame, dict[str, Any]]:
    windows, ticks = stress.load_compact(args.compact_root, None, None, None)
    windows = windows[windows["winner_side"].isin(["YES", "NO"])].copy()
    snapshots = stress.prepare_quote_snapshots(ticks, windows, valid_topbook_only=True)
    preds, resolution, missing = stress.load_predictions(args.predictions_root, [args.model_id], windows)
    if missing:
        raise RuntimeError(f"missing requested prediction models: {missing}; {resolution.get('error', '') if isinstance(resolution, dict) else ''}")
    predicted = stress.attach_predictions(snapshots, preds, [args.model_id])
    candidates = add_trade_columns(predicted, args)
    selected, variant_skips = select_first_entries_by_variant(candidates, args)
    compact_min = windows["market_start_ts"].min()
    compact_max = windows["market_start_ts"].max()
    pred_min = resolution.get("prediction_market_start_min") if isinstance(resolution, dict) else pd.NaT
    pred_max = resolution.get("prediction_market_start_max") if isinstance(resolution, dict) else pd.NaT
    effective_min = max(pd.Timestamp(compact_min), pd.Timestamp(pred_min)) if pd.notna(compact_min) and pd.notna(pred_min) else pd.NaT
    effective_max = min(pd.Timestamp(compact_max), pd.Timestamp(pred_max)) if pd.notna(compact_max) and pd.notna(pred_max) else pd.NaT
    evaluated_dates = sorted(pd.to_datetime(selected["market_start_ts"], utc=True, errors="coerce").dt.date.astype(str).dropna().unique().tolist()) if not selected.empty else []
    manifest = {
        "compact_windows": int(len(windows)),
        "book_ticks": int(len(ticks)),
        "snapshots": int(len(snapshots)),
        "predicted_rows": int(len(predicted)),
        "candidate_rows": int(len(candidates)),
        "selected_rows_before_sizing": int(len(selected)),
        "prediction_resolution": resolution,
        "compact_market_start_min": compact_min,
        "compact_market_start_max": compact_max,
        "prediction_market_start_min": pred_min,
        "prediction_market_start_max": pred_max,
        "effective_overlap_min": effective_min,
        "effective_overlap_max": effective_max,
        "dates_evaluated": evaluated_dates,
        "coverage_warning": bool(pd.notna(compact_max) and pd.notna(pred_max) and pd.Timestamp(compact_max) > pd.Timestamp(pred_max)),
    }
    return selected, variant_skips, manifest


def aggregate_tables(sized: pd.DataFrame, skipped: pd.DataFrame, args: argparse.Namespace) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
    pieces = []
    if not sized.empty:
        pieces.append(sized)
    if not skipped.empty and {"policy_variant", "sizing_policy", "skip_reason"}.issubset(skipped.columns):
        pieces.append(skipped)
    combined = pd.concat(pieces, ignore_index=True, sort=False) if pieces else pd.DataFrame(columns=["policy_variant", "sizing_policy", "skip_reason"])
    summary_rows = []
    if "skip_reason" not in combined.columns:
        combined["skip_reason"] = ""
    combined["skip_reason"] = combined["skip_reason"].fillna("")
    combined = combined[combined["policy_variant"].notna() & combined["sizing_policy"].notna()]
    for (variant, sizing), group in combined.groupby(["policy_variant", "sizing_policy"], dropna=False, sort=True):
        row = {"policy_variant": variant, "sizing_policy": sizing}
        row.update(summarize_group(group, float(args.starting_bankroll)))
        summary_rows.append(row)
    summary = pd.DataFrame(summary_rows)
    tables = {
        "by_policy_variant.csv": summary.groupby("policy_variant", dropna=False).agg({"trade_count": "sum", "gross_cost": "sum", "pnl": "sum"}).reset_index() if not summary.empty else pd.DataFrame(),
        "by_sizing_policy.csv": summary.groupby("sizing_policy", dropna=False).agg({"trade_count": "sum", "gross_cost": "sum", "pnl": "sum"}).reset_index() if not summary.empty else pd.DataFrame(),
        "by_policy_and_sizing.csv": summary,
    }
    for filename, cols in {
        "by_date.csv": ["policy_variant", "sizing_policy", sized["ts"].dt.date.astype(str).rename("date") if not sized.empty else "date"],
        "by_chronological_slice.csv": ["policy_variant", "sizing_policy", "chronological_slice"],
        "by_ask_bin.csv": ["policy_variant", "sizing_policy", "ask_bin"],
        "by_side.csv": ["policy_variant", "sizing_policy", "side"],
        "by_depth_utilization_bin.csv": ["policy_variant", "sizing_policy", "depth_utilization_bin"],
        "by_capacity_binding.csv": ["policy_variant", "sizing_policy", "capacity_bound"],
    }.items():
        if sized.empty:
            tables[filename] = pd.DataFrame()
            continue
        temp = sized.copy()
        if filename == "by_date.csv":
            temp["date"] = pd.to_datetime(temp["ts"], utc=True).dt.date.astype(str)
            group_cols = ["policy_variant", "sizing_policy", "date"]
        elif filename == "by_depth_utilization_bin.csv":
            temp["depth_utilization_bin"] = pd.cut(pd.to_numeric(temp["depth_utilization"], errors="coerce"), [-np.inf, 0.25, 0.5, 0.75, 0.95, 1.0, np.inf], labels=["<=0.25", "0.25_0.50", "0.50_0.75", "0.75_0.95", "0.95_1.00", ">1.00"]).astype("object").fillna("missing")
            group_cols = ["policy_variant", "sizing_policy", "depth_utilization_bin"]
        else:
            group_cols = cols
        tables[filename] = temp.groupby(group_cols, dropna=False).agg(trade_count=("pnl", "size"), gross_cost=("stake_spend", "sum"), pnl=("pnl", "sum"), win_rate=("win", "mean")).reset_index()
    return summary, tables


def write_outputs(output_root: Path, sized: pd.DataFrame, skipped: pd.DataFrame, summary: pd.DataFrame, tables: dict[str, pd.DataFrame], drawdowns: pd.DataFrame, manifest: dict[str, Any]) -> None:
    summary.to_csv(output_root / "bankroll_summary.csv", index=False)
    for filename, table in tables.items():
        table.to_csv(output_root / filename, index=False)
    drawdowns.to_csv(output_root / "drawdown_episodes.csv", index=False)
    skipped.to_csv(output_root / "skipped_trades.csv", index=False)
    sized.to_csv(output_root / "bankroll_path.csv", index=False)
    hmm_veto.write_frame(sized, output_root / "selected_trades_with_sizing.parquet")
    if len(sized) <= 250_000:
        sized.to_csv(output_root / "selected_trades_with_sizing.csv", index=False)
    summary_json = {
        "rows": summary.to_dict(orient="records"),
        "fixed_stake_reproduction": manifest.get("fixed_stake_reproduction", {}),
        "warnings": manifest.get("warnings", []),
    }
    (output_root / "summary.json").write_text(json.dumps(summary_json, indent=2, default=json_default), encoding="utf-8")
    (output_root / "run_manifest.json").write_text(json.dumps(manifest, indent=2, default=json_default), encoding="utf-8")
    schema = {
        "bankroll_summary.csv": summary.columns.tolist(),
        "selected_trades_with_sizing": sized.columns.tolist(),
        "skipped_trades.csv": skipped.columns.tolist(),
    }
    (output_root / "output_schema.json").write_text(json.dumps(schema, indent=2, default=json_default), encoding="utf-8")
    lines = [
        "BTC-5m Brownian no-HMM bankroll compounding validation",
        "",
        "Focused offline research only. No live behavior changed.",
        "",
        f"model_id={manifest.get('model_id')}",
        f"entry_age_window={manifest.get('entry_age_min_seconds')}:{manifest.get('entry_age_max_seconds')}",
        f"edge_threshold={manifest.get('edge_threshold')}",
        f"top_n_levels={manifest.get('top_n_levels')}",
        f"capacity_depth_cap_available={manifest.get('capacity_depth_cap_available')}",
        f"fixed_stake_reproduction={manifest.get('fixed_stake_reproduction')}",
        "",
        "Warnings:",
    ]
    lines.extend([f"- {w}" for w in manifest.get("warnings", [])] or ["- none"])
    (output_root / "README.txt").write_text("\n".join(lines) + "\n", encoding="utf-8")


def execute_run(args: argparse.Namespace, output_root: Path, selected: pd.DataFrame, variant_skips: pd.DataFrame, build_manifest: dict[str, Any]) -> dict[str, Any]:
    if output_root.exists():
        if not args.overwrite:
            raise FileExistsError(f"{output_root} exists; pass --overwrite")
        shutil.rmtree(output_root)
    output_root.mkdir(parents=True, exist_ok=True)
    all_sized = []
    all_skipped = [variant_skips] if not variant_skips.empty else []
    for sizing in build_sizing_policies(args.fixed_stake):
        sized, skipped = simulate_variant_sizing(selected, variant_skips.iloc[0:0].copy(), sizing, args)
        if not sized.empty:
            all_sized.append(sized)
        if not skipped.empty:
            all_skipped.append(skipped)
    sized_all = pd.concat(all_sized, ignore_index=True, sort=False) if all_sized else pd.DataFrame()
    skipped_all = pd.concat(all_skipped, ignore_index=True, sort=False) if all_skipped else pd.DataFrame()
    summary, tables = aggregate_tables(sized_all, skipped_all, args)
    drawdowns = compute_drawdown_episodes(sized_all)
    raw_add = summary[(summary["policy_variant"].eq("raw_policy")) & (summary["sizing_policy"].eq("additive_fixed_notional"))] if {"policy_variant", "sizing_policy"}.issubset(summary.columns) else pd.DataFrame()
    reproduction = raw_add.iloc[0].to_dict() if not raw_add.empty else {}
    warnings = []
    if reproduction:
        tc = int(reproduction.get("trade_count", 0))
        roi = float(reproduction.get("fixed_stake_comparable_roi", np.nan))
        if not (4500 <= tc <= 5500 and np.isfinite(roi) and 0.10 <= roi <= 0.18):
            warnings.append(f"fixed_stake_reproduction_outside_expected_band trade_count={tc} roi={roi}")
    if selected["capacity_depth_cap_available"].fillna(False).mean() < 1.0:
        warnings.append("some selected rows lack top-n depth capacity columns")
    if build_manifest.get("coverage_warning"):
        warnings.append("WARNING: compact recorder extends beyond prediction artifact; validation only covers effective overlap.")
    manifest = {
        **build_manifest,
        "git_commit": git_commit(),
        "compact_root": str(args.compact_root),
        "predictions_root": str(args.predictions_root),
        "output_root": str(output_root),
        "model_id": args.model_id,
        "entry_age_min_seconds": float(args.entry_age_min_seconds),
        "entry_age_max_seconds": float(args.entry_age_max_seconds),
        "edge_threshold": float(args.edge_threshold),
        "top_n_levels": int(args.top_n_levels),
        "starting_bankroll": float(args.starting_bankroll),
        "fixed_stake": float(args.fixed_stake),
        "probability_haircut_abs": float(args.probability_haircut_abs),
        "ask_slippage_abs": float(args.ask_slippage_abs),
        "min_expected_log_growth": float(args.min_expected_log_growth),
        "capacity_depth_cap_available": bool(selected["capacity_depth_cap_available"].fillna(False).all()) if not selected.empty else False,
        "hmm_variants": "skipped_no_full_coverage_hmm_context_requested",
        "fixed_stake_reproduction": reproduction,
        "warnings": warnings,
        "small_wallet": {
            "enabled": bool(args.small_wallet_mode),
            "threshold": float(args.small_wallet_threshold),
            "sizing_policy": args.small_wallet_sizing_policy,
            "max_stake_fraction": float(args.small_wallet_max_stake_fraction),
            "min_order_notional": float(args.min_order_notional),
            "skip_below_min_order": bool(args.skip_below_min_order),
            "reserve_bankroll_fraction": float(args.reserve_bankroll_fraction),
            "daily_stop_loss_fraction": float(args.daily_stop_loss_fraction),
            "session_stop_loss_fraction": float(args.session_stop_loss_fraction),
        },
    }
    write_outputs(output_root, sized_all, skipped_all, summary, tables, drawdowns, manifest)
    return manifest


def sweep_summary_row(output_dir: Path, starting_bankroll: float) -> dict[str, Any]:
    path = output_dir / "bankroll_summary.csv"
    if not path.exists():
        return {"starting_bankroll": starting_bankroll, "error": "missing_bankroll_summary"}
    summary = pd.read_csv(path)
    preferred = summary[
        summary["policy_variant"].eq("expected_growth_positive_plus_ask_gt_0.30")
        & summary["sizing_policy"].eq("kelly_1_40_cap_0_25pct")
    ]
    row = (preferred if not preferred.empty else summary).iloc[0].to_dict()
    row["starting_bankroll"] = float(starting_bankroll)
    row["run_dir"] = str(output_dir)
    try:
        stops = pd.read_csv(output_dir / "skipped_trades.csv") if (output_dir / "skipped_trades.csv").exists() else pd.DataFrame()
    except pd.errors.EmptyDataError:
        stops = pd.DataFrame()
    first_stop = pd.NaT
    if not stops.empty and "skip_reason" in stops.columns:
        stop_rows = stops[stops["skip_reason"].isin(["session_stop_loss_guard", "daily_stop_loss_guard", "insufficient_bankroll"])]
        if not stop_rows.empty:
            ts_col = "decision_ts" if "decision_ts" in stop_rows.columns else "ts"
            first_stop = pd.to_datetime(stop_rows[ts_col], utc=True, errors="coerce").min()
    row["date_of_first_ruin_or_stop"] = first_stop
    return row


def run(args: argparse.Namespace) -> dict[str, Any]:
    output_root = Path(args.output_root)
    sweep_values = parse_float_csv(args.starting_bankroll_sweep)
    selected, variant_skips, build_manifest = build_candidates(args)
    if sweep_values:
        if output_root.exists() and any(output_root.iterdir()) and not args.overwrite:
            raise FileExistsError(f"{output_root} exists and is not empty; pass --overwrite")
        output_root.mkdir(parents=True, exist_ok=True)
        rows = []
        manifests = []
        for bankroll in sweep_values:
            child_args = argparse.Namespace(**vars(args))
            child_args.starting_bankroll = float(bankroll)
            child_dir = output_root / f"starting_bankroll_{bankroll:g}"
            manifest = execute_run(child_args, child_dir, selected, variant_skips, build_manifest)
            manifests.append(manifest)
            rows.append(sweep_summary_row(child_dir, bankroll))
        sweep = pd.DataFrame(rows)
        sweep.to_csv(output_root / "bankroll_floor_sweep.csv", index=False)
        sweep.to_csv(output_root / "by_starting_bankroll_and_policy.csv", index=False)
        (output_root / "bankroll_floor_sweep.json").write_text(json.dumps(rows, indent=2, default=json_default), encoding="utf-8")
        root_manifest = {
            "output_root": str(output_root),
            "starting_bankroll_sweep": sweep_values,
            "run_count": len(sweep_values),
            "child_manifests": [m.get("output_root") for m in manifests],
            "warnings": sorted({w for m in manifests for w in m.get("warnings", [])}),
        }
        (output_root / "run_manifest.json").write_text(json.dumps(root_manifest, indent=2, default=json_default), encoding="utf-8")
        return root_manifest
    return execute_run(args, output_root, selected, variant_skips, build_manifest)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Focused BTC-5m Brownian no-HMM bankroll/Kelly compounding validation.")
    parser.add_argument("--compact-root", type=Path, required=True)
    parser.add_argument("--predictions-root", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--starting-bankroll", type=float, default=1000.0)
    parser.add_argument("--fixed-stake", type=float, default=5.0)
    parser.add_argument("--entry-age-min-seconds", type=float, default=60.0)
    parser.add_argument("--entry-age-max-seconds", type=float, default=240.0)
    parser.add_argument("--model-id", default=DEFAULT_MODEL_ID)
    parser.add_argument("--edge-threshold", type=float, default=0.02)
    parser.add_argument("--top-n-levels", type=int, default=10)
    parser.add_argument("--probability-haircut-abs", type=float, default=0.0)
    parser.add_argument("--ask-slippage-abs", type=float, default=0.0)
    parser.add_argument("--min-expected-log-growth", type=float, default=0.0)
    parser.add_argument("--allow-missing-depth", action="store_true")
    parser.add_argument("--small-wallet-mode", type=bool_arg, default=False)
    parser.add_argument("--small-wallet-threshold", type=float, default=1000.0)
    parser.add_argument("--small-wallet-sizing-policy", default="kelly_1_40_cap_0_25pct")
    parser.add_argument("--small-wallet-max-stake-fraction", type=float, default=0.0025)
    parser.add_argument("--min-order-notional", type=float, default=0.0)
    parser.add_argument("--skip-below-min-order", type=bool_arg, default=True)
    parser.add_argument("--reserve-bankroll-fraction", type=float, default=0.20)
    parser.add_argument("--daily-stop-loss-fraction", type=float, default=0.03)
    parser.add_argument("--session-stop-loss-fraction", type=float, default=0.08)
    parser.add_argument("--allow-round-up-to-min-order", action="store_true")
    parser.add_argument("--starting-bankroll-sweep", default="")
    parser.add_argument("--overwrite", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    print(json.dumps(run(build_parser().parse_args(argv)), indent=2, default=json_default))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
