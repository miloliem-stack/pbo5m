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

from scripts import sweep_hmm_regime_health as health


DEFAULT_STRESS_ROOT = Path("artifacts/probability_model_set_capacity_stress/compact_20260423_20260511_six_models_v1")
DEFAULT_HMM_ROOT = Path("artifacts/hmm_regime_health/phase1_core_laplace_2to8")
DEFAULT_COMPACT_ROOT = Path("artifacts/compact_market_recorder/2026-04-23_to_2026-05-11")
DEFAULT_MODELS = [
    "core_1m__gaussian_hmm__k4",
    "laplace_1m__gaussian_hmm__k4",
    "core_1m__gaussian_hmm__k3",
    "laplace_1m__gaussian_hmm__k3",
]
DEFAULT_THRESHOLDS = [0.60, 0.70, 0.75, 0.80, 0.90]
ASK_BINS = [-np.inf, 0.30, 0.35, 0.40, 0.45, 0.47, 0.49, 0.50, 0.55, 0.60, np.inf]
ASK_LABELS = ["<=0.30", "0.30_0.35", "0.35_0.40", "0.40_0.45", "0.45_0.47", "0.47_0.49", "0.49_0.50", "0.50_0.55", "0.55_0.60", ">0.60"]


def parse_csv(value: str) -> list[str]:
    return [item.strip() for item in str(value).split(",") if item.strip()]


def parse_floats(value: str) -> list[float]:
    return [float(item) for item in parse_csv(value)]


def read_frame(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".parquet":
        try:
            return pd.read_parquet(path)
        except ImportError as exc:
            sidecar = Path(str(path) + ".as.json")
            if sidecar.exists():
                return pd.read_json(sidecar, lines=True)
            raise exc
    sidecar = Path(str(path) + ".as.json")
    if sidecar.exists():
        return pd.read_json(sidecar, lines=True)
    return pd.read_csv(path)


def write_frame(frame: pd.DataFrame, path: Path) -> str:
    if path.suffix.lower() == ".parquet":
        try:
            frame.to_parquet(path, index=False)
            return str(path)
        except Exception:
            fallback = Path(str(path) + ".as.json")
            frame.to_json(fallback, orient="records", lines=True, date_format="iso")
            return str(fallback)
    frame.to_csv(path, index=False)
    return str(path)


def find_trade_replay_path(trade_replay_path: Path | None, stress_artifact_root: Path | None) -> Path:
    if trade_replay_path is not None:
        if not trade_replay_path.exists():
            raise FileNotFoundError(f"--trade-replay-path does not exist: {trade_replay_path}")
        return trade_replay_path
    root = stress_artifact_root or DEFAULT_STRESS_ROOT
    for name in ["trade_level_results.parquet", "trade_level_results.csv", "trade_level_results.parquet.as.json"]:
        candidate = root / name
        if candidate.exists():
            return candidate
    raise FileNotFoundError(f"no trade-level replay artifact found under {root}; expected trade_level_results.parquet/csv")


def normalize_trades(trades: pd.DataFrame) -> pd.DataFrame:
    out = trades.copy()
    aliases = {
        "timestamp": ["timestamp", "entry_ts", "ts", "prediction_ts"],
        "market_id": ["market_id", "market_key"],
        "model_id": ["model_id", "model_name", "model"],
        "ask_price": ["ask_price", "executable_ask", "entry_ask", "selected_price"],
        "shares": ["shares", "filled_qty", "filled_shares"],
        "pnl": ["pnl", "realized_pnl", "gross_pnl"],
        "roi": ["roi", "roi_on_filled_cost"],
        "date": ["date", "entry_date", "entry_day"],
        "entry_age_seconds": ["entry_age_seconds", "entry_age_sec", "market_age_seconds"],
    }
    for canonical, choices in aliases.items():
        if canonical not in out.columns:
            found = next((col for col in choices if col in out.columns), None)
            if found is not None:
                out[canonical] = out[found]
    if "timestamp" not in out.columns:
        raise ValueError("trade replay is missing timestamp/entry_ts/ts")
    out["timestamp"] = pd.to_datetime(out["timestamp"], utc=True, errors="coerce")
    if out["timestamp"].isna().any():
        raise ValueError("trade replay contains nonparseable trade timestamps")
    if "gross_cost" not in out.columns and {"ask_price", "shares"}.issubset(out.columns):
        out["gross_cost"] = pd.to_numeric(out["ask_price"], errors="coerce") * pd.to_numeric(out["shares"], errors="coerce")
    if "pnl" not in out.columns:
        raise ValueError("trade replay is missing pnl/realized_pnl/gross_pnl")
    out["pnl"] = pd.to_numeric(out["pnl"], errors="coerce")
    if "gross_pnl" not in out.columns:
        out["gross_pnl"] = out["pnl"]
    if "roi" not in out.columns:
        if "gross_cost" not in out.columns:
            raise ValueError("trade replay is missing roi and cannot compute it without gross_cost")
        out["roi"] = out["pnl"] / pd.to_numeric(out["gross_cost"], errors="coerce").replace(0.0, np.nan)
    if "win" not in out.columns:
        if {"side", "winner_side"}.issubset(out.columns):
            out["win"] = out["side"].astype(str).str.upper().eq(out["winner_side"].astype(str).str.upper())
        else:
            out["win"] = out["pnl"] > 0
    if "date" not in out.columns:
        out["date"] = out["timestamp"].dt.date.astype(str)
    if "chronological_slice" not in out.columns:
        out["chronological_slice"] = "unknown"
    if "ask_price" in out.columns:
        ask = pd.to_numeric(out["ask_price"], errors="coerce")
        out["ask_bin"] = pd.cut(ask, ASK_BINS, labels=ASK_LABELS, right=True).astype("object").fillna("missing")
    else:
        out["ask_bin"] = "missing"
    required = ["market_id", "model_id", "side", "stake_size", "edge_threshold", "entry_age_window", "ask_price", "gross_cost", "pnl", "chronological_slice", "date", "winner_side", "win"]
    missing = [col for col in required if col not in out.columns]
    if missing:
        raise ValueError(f"trade replay is missing required columns or equivalents: {missing}")
    return out


def normalize_hmm_states(states: pd.DataFrame) -> pd.DataFrame:
    out = states.copy()
    aliases = {
        "timestamp": ["timestamp", "ts"],
        "hmm_model_id": ["hmm_model_id", "candidate_model_id", "model_id"],
        "hmm_state": ["hmm_state", "raw_state_id", "map_state", "state"],
        "hmm_pmax": ["hmm_pmax", "p_max", "pmax", "posterior_max"],
    }
    for canonical, choices in aliases.items():
        if canonical not in out.columns:
            found = next((col for col in choices if col in out.columns), None)
            if found is not None:
                out[canonical] = out[found]
    missing = [col for col in ["timestamp", "hmm_model_id", "hmm_state", "hmm_pmax"] if col not in out.columns]
    if missing:
        raise ValueError(f"HMM state rows missing required columns: {missing}")
    out["timestamp"] = pd.to_datetime(out["timestamp"], utc=True, errors="coerce")
    out["hmm_model_id"] = out["hmm_model_id"].astype(str)
    out["hmm_state"] = pd.to_numeric(out["hmm_state"], errors="coerce").astype("Int64")
    out["hmm_pmax"] = pd.to_numeric(out["hmm_pmax"], errors="coerce")
    out = out.dropna(subset=["timestamp", "hmm_model_id", "hmm_state", "hmm_pmax"]).copy()
    return add_state_age(out)


def add_state_age(states: pd.DataFrame) -> pd.DataFrame:
    parts = []
    for model_id, group in states.sort_values(["hmm_model_id", "timestamp"]).groupby("hmm_model_id", sort=False):
        g = group.copy()
        changed = g["hmm_state"].ne(g["hmm_state"].shift())
        run_start = g["timestamp"].where(changed).ffill()
        g["hmm_state_age_seconds"] = (g["timestamp"] - run_start).dt.total_seconds()
        g["minutes_since_state_change"] = g["hmm_state_age_seconds"] / 60.0
        g["hmm_state_changed_recently"] = g["minutes_since_state_change"].le(5.0)
        parts.append(g)
    return pd.concat(parts, ignore_index=True) if parts else states


def discover_precomputed_state_file(hmm_root: Path) -> Path | None:
    names = [
        "hmm_state_assignments.parquet",
        "hmm_state_assignments.csv",
        "per_timestamp_hmm_states.parquet",
        "per_timestamp_hmm_states.csv",
        "per_timestamp_regime_states.parquet",
        "per_timestamp_regime_states.csv",
    ]
    for name in names:
        path = hmm_root / name
        if path.exists():
            return path
    matches = sorted(hmm_root.glob("*state*assignment*.parquet")) + sorted(hmm_root.glob("*state*assignment*.csv"))
    return matches[0] if matches else None


def parse_hmm_model_id(model_id: str) -> tuple[str, str, int]:
    parts = model_id.split("__")
    if len(parts) < 3 or not parts[-1].startswith("k"):
        raise ValueError(f"unsupported HMM model id format: {model_id}")
    return parts[0], "__".join(parts[1:-1]), int(parts[-1][1:])


def make_reconstruction_splits(n_rows: int, train_rows: int, test_rows: int, step_rows: int) -> list[health.Fold]:
    folds = health.make_walk_forward_splits(n_rows, train_rows, test_rows, step_rows)
    if n_rows <= train_rows:
        return folds
    last_test_end = folds[-1].test_end if folds else train_rows
    if last_test_end >= n_rows:
        return folds
    tail_train_end = last_test_end
    tail_train_start = max(0, tail_train_end - train_rows)
    folds.append(
        health.Fold(
            fold_id=(folds[-1].fold_id + 1) if folds else 0,
            train_start=tail_train_start,
            train_end=tail_train_end,
            test_start=tail_train_end,
            test_end=n_rows,
        )
    )
    return folds


def reconstruct_hmm_states(hmm_root: Path, models: list[str], price_input: Path | None, max_folds: int | None) -> pd.DataFrame:
    config_path = hmm_root / "sweep_config.json"
    if not config_path.exists():
        raise FileNotFoundError(f"cannot reconstruct HMM states without sweep_config.json: {config_path}")
    config = json.loads(config_path.read_text(encoding="utf-8"))
    source = price_input or Path(config["input"])
    prices = health.load_price_frame(source, max_rows=config.get("max_rows"))
    feature_manifest = json.loads((hmm_root / "feature_manifest.json").read_text(encoding="utf-8")) if (hmm_root / "feature_manifest.json").exists() else {}
    feature_cache: dict[str, tuple[pd.DataFrame, list[str]]] = {}
    rows: list[pd.DataFrame] = []
    for model_id in models:
        feature_set, family, n_states = parse_hmm_model_id(model_id)
        if feature_set not in feature_cache:
            features_path = hmm_root / f"features_{feature_set}.csv"
            if features_path.exists():
                features = pd.read_csv(features_path, parse_dates=["timestamp"])
                if features["timestamp"].dt.tz is None:
                    features["timestamp"] = features["timestamp"].dt.tz_localize("UTC")
                else:
                    features["timestamp"] = features["timestamp"].dt.tz_convert("UTC")
                if prices["timestamp"].max() > features["timestamp"].max():
                    features, manifest = health.build_features(prices, feature_set)
                    feature_columns = manifest["columns"]
                else:
                    feature_columns = feature_manifest.get(feature_set, {}).get("columns") or [c for c in features.columns if c not in {"timestamp", "close"}]
            else:
                features, manifest = health.build_features(prices, feature_set)
                feature_columns = manifest["columns"]
            feature_cache[feature_set] = (features, feature_columns)
        features, feature_columns = feature_cache[feature_set]
        folds = make_reconstruction_splits(len(features), int(config["train_rows"]), int(config["test_rows"]), int(config["step_rows"]))
        if max_folds:
            folds = folds[:max_folds]
        for fold in folds:
            train = features.iloc[fold.train_start : fold.train_end].reset_index(drop=True)
            test = features.iloc[fold.test_start : fold.test_end].reset_index(drop=True)
            if len(train) <= n_states * 4 or test.empty:
                continue
            x_train, x_test, _ = health.standardize_train_test(train, test, feature_columns)
            model = health.fit_hmm(family, n_states, x_train, int(config.get("random_seed", 1)) + fold.fold_id)
            filtered = health.filtered_probabilities(model, x_test)
            rows.append(
                pd.DataFrame(
                    {
                        "timestamp": test["timestamp"],
                        "hmm_model_id": model_id,
                        "hmm_state": filtered.argmax(axis=1).astype(int),
                        "hmm_pmax": filtered.max(axis=1),
                        "fold_id": fold.fold_id,
                    }
                )
            )
    return normalize_hmm_states(pd.concat(rows, ignore_index=True) if rows else pd.DataFrame())


def load_hmm_states(hmm_root: Path, models: list[str], allow_reconstruct: bool, price_input: Path | None, max_folds: int | None) -> tuple[pd.DataFrame, dict[str, Any]]:
    state_file = discover_precomputed_state_file(hmm_root)
    if state_file is not None:
        states = normalize_hmm_states(read_frame(state_file))
        source = str(state_file)
    elif allow_reconstruct:
        states = reconstruct_hmm_states(hmm_root, models, price_input, max_folds)
        source = "reconstructed_from_sweep_config_and_price_input"
    else:
        raise FileNotFoundError(
            f"no per-timestamp HMM state file found under {hmm_root}; pass --allow-reconstruct-hmm-from-prices true to rebuild selected model states causally from Binance 1m prices"
        )
    states = states[states["hmm_model_id"].isin(models)].copy()
    if states.empty:
        raise ValueError(f"no HMM state rows found for requested models: {models}")
    return states, {"hmm_state_source": source, "hmm_state_rows": int(len(states))}


def assert_hmm_coverage(trades: pd.DataFrame, states: pd.DataFrame, models: list[str]) -> dict[str, Any]:
    trade_min = trades["timestamp"].min()
    trade_max = trades["timestamp"].max()
    rows = []
    failures = []
    for model_id in models:
        model_states = states[states["hmm_model_id"].eq(model_id)]
        state_min = model_states["timestamp"].min() if not model_states.empty else pd.NaT
        state_max = model_states["timestamp"].max() if not model_states.empty else pd.NaT
        ok = bool(pd.notna(state_min) and pd.notna(state_max) and state_min <= trade_min and state_max >= trade_max)
        rows.append({"hmm_model_id": model_id, "state_min": state_min, "state_max": state_max, "trade_min": trade_min, "trade_max": trade_max, "coverage_ok": ok})
        if not ok:
            failures.append(f"{model_id}: state_range=[{state_min}, {state_max}] trade_range=[{trade_min}, {trade_max}]")
    if failures:
        raise ValueError("HMM state coverage does not span full replay period; refusing to silently drop trades. " + "; ".join(failures))
    return {"hmm_coverage": rows}


def attach_hmm_to_trades(trades: pd.DataFrame, states: pd.DataFrame, models: list[str]) -> pd.DataFrame:
    attached = []
    base = trades.sort_values("timestamp", kind="mergesort").reset_index(drop=True)
    for model_id in models:
        model_states = states[states["hmm_model_id"].eq(model_id)].sort_values("timestamp", kind="mergesort")
        merged = pd.merge_asof(
            base,
            model_states[["timestamp", "hmm_model_id", "hmm_state", "hmm_pmax", "hmm_state_age_seconds", "minutes_since_state_change", "hmm_state_changed_recently"]],
            on="timestamp",
            direction="backward",
            allow_exact_matches=True,
        )
        if merged[["hmm_state", "hmm_pmax"]].isna().any().any():
            raise ValueError(f"asof join produced missing HMM states for {model_id}; check coverage and timestamp ordering")
        attached.append(merged)
    return pd.concat(attached, ignore_index=True)


def aggregate_pnl(frame: pd.DataFrame, group_cols: list[str]) -> pd.DataFrame:
    rows = []
    for keys, group in frame.groupby(group_cols, dropna=False, sort=True):
        if not isinstance(keys, tuple):
            keys = (keys,)
        cost = pd.to_numeric(group["gross_cost"], errors="coerce").sum()
        pnl = pd.to_numeric(group["pnl"], errors="coerce").sum()
        row = dict(zip(group_cols, keys))
        row.update(
            {
                "trade_count": int(len(group)),
                "unique_markets": int(group["market_id"].nunique()),
                "gross_cost": float(cost),
                "pnl": float(pnl),
                "roi": float(pnl / cost) if cost else np.nan,
                "win_rate": float(pd.to_numeric(group["win"], errors="coerce").mean()),
                "avg_ask": float(pd.to_numeric(group["ask_price"], errors="coerce").mean()),
                "avg_hmm_pmax": float(pd.to_numeric(group["hmm_pmax"], errors="coerce").mean()),
            }
        )
        rows.append(row)
    return pd.DataFrame(rows)


def performance_metrics(before: pd.DataFrame, remaining: pd.DataFrame, vetoed: pd.DataFrame) -> dict[str, Any]:
    before_cost = pd.to_numeric(before["gross_cost"], errors="coerce").sum()
    after_cost = pd.to_numeric(remaining["gross_cost"], errors="coerce").sum()
    pnl_before = pd.to_numeric(before["pnl"], errors="coerce").sum()
    pnl_after = pd.to_numeric(remaining["pnl"], errors="coerce").sum()
    losses_before = before[pd.to_numeric(before["pnl"], errors="coerce") < 0]
    profits_before = before[pd.to_numeric(before["pnl"], errors="coerce") > 0]
    losses_removed = vetoed[pd.to_numeric(vetoed["pnl"], errors="coerce") < 0]
    profits_removed = vetoed[pd.to_numeric(vetoed["pnl"], errors="coerce") > 0]
    return {
        "total_trades_before": int(len(before)),
        "total_trades_after": int(len(remaining)),
        "vetoed_trades": int(len(vetoed)),
        "vetoed_trade_share": float(len(vetoed) / len(before)) if len(before) else np.nan,
        "pnl_before": float(pnl_before),
        "pnl_after": float(pnl_after),
        "pnl_lift": float(pnl_after - pnl_before),
        "roi_before": float(pnl_before / before_cost) if before_cost else np.nan,
        "roi_after": float(pnl_after / after_cost) if after_cost else np.nan,
        "roi_lift": float((pnl_after / after_cost) - (pnl_before / before_cost)) if before_cost and after_cost else np.nan,
        "total_losses_before": int(len(losses_before)),
        "losses_removed": int(len(losses_removed)),
        "loss_share_removed": float(len(losses_removed) / len(losses_before)) if len(losses_before) else np.nan,
        "total_profits_before": int(len(profits_before)),
        "profits_removed": int(len(profits_removed)),
        "profit_share_removed": float(len(profits_removed) / len(profits_before)) if len(profits_before) else np.nan,
        "avg_ask_before": float(pd.to_numeric(before["ask_price"], errors="coerce").mean()) if len(before) else np.nan,
        "avg_ask_after": float(pd.to_numeric(remaining["ask_price"], errors="coerce").mean()) if len(remaining) else np.nan,
        "win_rate_before": float(pd.to_numeric(before["win"], errors="coerce").mean()) if len(before) else np.nan,
        "win_rate_after": float(pd.to_numeric(remaining["win"], errors="coerce").mean()) if len(remaining) else np.nan,
        "remaining_unique_markets": int(remaining["market_id"].nunique()) if len(remaining) else 0,
        "vetoed_unique_markets": int(vetoed["market_id"].nunique()) if len(vetoed) else 0,
    }


def single_state_veto_scan(frame: pd.DataFrame, thresholds: list[float], group_cols: list[str] | None = None) -> pd.DataFrame:
    group_cols = group_cols or []
    base_group_cols = ["hmm_model_id"] + [col for col in group_cols if col != "hmm_model_id"]
    rows = []
    grouped = frame.groupby(base_group_cols, dropna=False, sort=True)
    for keys, base in grouped:
        if not isinstance(keys, tuple):
            keys = (keys,)
        group_prefix = dict(zip(base_group_cols, keys))
        model_id = group_prefix["hmm_model_id"]
        for state, _ in base.groupby("hmm_state", dropna=False):
            for threshold in thresholds:
                veto_mask = base["hmm_state"].eq(state) & (pd.to_numeric(base["hmm_pmax"], errors="coerce") >= threshold)
                vetoed = base[veto_mask]
                remaining = base[~veto_mask]
                row = {**group_prefix, "hmm_state": int(state), "pmax_threshold": float(threshold)}
                row.update(performance_metrics(base, remaining, vetoed))
                rows.append(row)
    return pd.DataFrame(rows)


def chronological_train_test_slices(frame: pd.DataFrame) -> tuple[list[str], list[str], str]:
    ordered = [s for s in ["early", "main", "fresh"] if s in set(frame["chronological_slice"].astype(str))]
    if len(ordered) >= 2:
        return ordered[: max(1, len(ordered) // 2)], ordered[max(1, len(ordered) // 2) :], "named_slice_half"
    dates = sorted(frame["date"].astype(str).dropna().unique().tolist())
    midpoint = max(1, len(dates) // 2)
    train_dates = set(dates[:midpoint])
    test_dates = set(dates[midpoint:])
    out = frame.copy()
    out["_chronological_validation_side"] = np.where(out["date"].astype(str).isin(train_dates), "train", np.where(out["date"].astype(str).isin(test_dates), "test", "unused"))
    return ["train"], ["test"], "date_half"


def frozen_veto_validation(frame: pd.DataFrame, scan: pd.DataFrame, min_vetoed_trades: int, min_remaining_trades: int, min_vetoed_unique_markets: int) -> pd.DataFrame:
    work = frame.copy()
    train_slices, test_slices, split_name = chronological_train_test_slices(work)
    if split_name == "date_half":
        dates = sorted(work["date"].astype(str).dropna().unique().tolist())
        midpoint = max(1, len(dates) // 2)
        train_mask = work["date"].astype(str).isin(set(dates[:midpoint]))
        test_mask = work["date"].astype(str).isin(set(dates[midpoint:]))
    else:
        train_mask = work["chronological_slice"].astype(str).isin(train_slices)
        test_mask = work["chronological_slice"].astype(str).isin(test_slices)
    train_scan = single_state_veto_scan(work[train_mask], sorted(scan["pmax_threshold"].dropna().unique().tolist()))
    eligible = train_scan[
        (train_scan["vetoed_trades"] >= min_vetoed_trades)
        & (train_scan["total_trades_after"] >= min_remaining_trades)
        & (train_scan["vetoed_unique_markets"] >= min_vetoed_unique_markets)
    ].copy()
    if eligible.empty:
        return pd.DataFrame()
    eligible = eligible.sort_values(["pnl_lift", "roi_lift", "vetoed_trades"], ascending=[False, False, False]).head(25)
    rows = []
    test_all = work[test_mask]
    for rank, row in enumerate(eligible.itertuples(index=False), start=1):
        test = test_all[test_all["hmm_model_id"].eq(row.hmm_model_id)]
        mask = test["hmm_state"].eq(row.hmm_state) & (pd.to_numeric(test["hmm_pmax"], errors="coerce") >= row.pmax_threshold)
        metrics = performance_metrics(test, test[~mask], test[mask])
        rows.append(
            {
                "selection_rank": rank,
                "split_name": split_name,
                "train_slices": ",".join(train_slices),
                "test_slices": ",".join(test_slices),
                "hmm_model_id": row.hmm_model_id,
                "hmm_state": int(row.hmm_state),
                "pmax_threshold": float(row.pmax_threshold),
                "train_pnl_lift": float(row.pnl_lift),
                "train_roi_lift": float(row.roi_lift),
                **{f"test_{k}": v for k, v in metrics.items()},
            }
        )
    return pd.DataFrame(rows)


def write_readme(path: Path, args: argparse.Namespace, manifest: dict[str, Any], top_validation: pd.DataFrame) -> None:
    lines = [
        "HMM regime veto attribution",
        "",
        "Offline research only. No live trading behavior was changed.",
        "",
        "Data inputs:",
        f"- trade_replay_path={manifest.get('trade_replay_path')}",
        f"- stress_artifact_root={args.stress_artifact_root}",
        f"- compact_root={args.compact_root}",
        f"- hmm_artifact_root={args.hmm_artifact_root}",
        f"- hmm_state_source={manifest.get('hmm_state_source')}",
        "",
        f"HMM models evaluated={manifest.get('hmm_models')}",
        f"HMM coverage matched full replay period={manifest.get('hmm_coverage_ok')}",
        f"chronological_validation_split={manifest.get('chronological_validation_split')}",
        "",
        "Top frozen validation candidates:",
    ]
    if top_validation.empty:
        lines.append("- none passed the minimum support guardrails")
    else:
        for row in top_validation.head(10).itertuples(index=False):
            lines.append(
                f"- rank={row.selection_rank} {row.hmm_model_id} state={row.hmm_state} pmax>={row.pmax_threshold:g} "
                f"train_pnl_lift={row.train_pnl_lift:.6g} test_pnl_lift={getattr(row, 'test_pnl_lift'):.6g} "
                f"test_roi_lift={getattr(row, 'test_roi_lift'):.6g}"
            )
    lines.extend(
        [
            "",
            "Caveats:",
            "- Single-state vetoes only; no multi-layer stacking or interaction search.",
            "- HMM posteriors are forward-filtered only; no smoothed hindsight probabilities.",
            "- Outcome labels are used only for replay evaluation, never for HMM feature construction.",
            "- If states were reconstructed, selected HMM models were refit walk-forward from Binance 1m prices because the health artifact did not contain per-timestamp state rows.",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def bool_arg(value: str | bool) -> bool:
    if isinstance(value, bool):
        return value
    lower = str(value).strip().lower()
    if lower in {"1", "true", "yes", "y", "on"}:
        return True
    if lower in {"0", "false", "no", "n", "off"}:
        return False
    raise argparse.ArgumentTypeError(f"expected bool, got {value!r}")


def run(args: argparse.Namespace) -> dict[str, Any]:
    output_dir = Path(args.output_dir)
    if output_dir.exists():
        if not args.overwrite:
            raise FileExistsError(f"{output_dir} exists; pass --overwrite")
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    models = parse_csv(args.hmm_models)
    thresholds = parse_floats(args.pmax_thresholds)
    trade_path = find_trade_replay_path(args.trade_replay_path, args.stress_artifact_root)
    trades = normalize_trades(read_frame(trade_path))
    states, state_diag = load_hmm_states(args.hmm_artifact_root, models, args.allow_reconstruct_hmm_from_prices, args.hmm_price_input, args.max_hmm_folds)
    coverage = assert_hmm_coverage(trades, states, models)
    attached = attach_hmm_to_trades(trades, states, models)
    write_frame(attached, output_dir / "trade_level_with_hmm.parquet")

    summary_specs = {
        "pnl_by_hmm_model_state.csv": ["hmm_model_id", "hmm_state"],
        "pnl_by_hmm_model_state_side.csv": ["hmm_model_id", "hmm_state", "side"],
        "pnl_by_hmm_model_state_entry_age_window.csv": ["hmm_model_id", "hmm_state", "entry_age_window"],
        "pnl_by_hmm_model_state_ask_bin.csv": ["hmm_model_id", "hmm_state", "ask_bin"],
        "pnl_by_hmm_model_state_chronological_slice.csv": ["hmm_model_id", "hmm_state", "chronological_slice"],
        "pnl_by_hmm_model_state_date.csv": ["hmm_model_id", "hmm_state", "date"],
        "pnl_by_hmm_model_state_model_id.csv": ["hmm_model_id", "hmm_state", "model_id"],
    }
    for filename, group_cols in summary_specs.items():
        aggregate_pnl(attached, group_cols).to_csv(output_dir / filename, index=False)

    scan = single_state_veto_scan(attached, thresholds)
    scan.to_csv(output_dir / "single_state_veto_scan.csv", index=False)
    single_state_veto_scan(attached, thresholds, ["chronological_slice"]).to_csv(output_dir / "single_state_veto_scan_by_chronological_slice.csv", index=False)
    single_state_veto_scan(attached, thresholds, ["model_id"]).to_csv(output_dir / "single_state_veto_scan_by_model.csv", index=False)
    single_state_veto_scan(attached, thresholds, ["side"]).to_csv(output_dir / "single_state_veto_scan_by_side.csv", index=False)

    validation = frozen_veto_validation(attached, scan, args.min_vetoed_trades, args.min_remaining_trades, args.min_vetoed_unique_markets)
    validation.to_csv(output_dir / "frozen_veto_validation.csv", index=False)
    split = "unavailable"
    if not validation.empty:
        split = str(validation["split_name"].iloc[0])

    schema = {
        "single_state_veto_scan": [
            "hmm_model_id",
            "hmm_state",
            "pmax_threshold",
            "total_trades_before",
            "total_trades_after",
            "vetoed_trades",
            "vetoed_trade_share",
            "pnl_before",
            "pnl_after",
            "pnl_lift",
            "roi_before",
            "roi_after",
            "roi_lift",
            "total_losses_before",
            "losses_removed",
            "loss_share_removed",
            "total_profits_before",
            "profits_removed",
            "profit_share_removed",
            "avg_ask_before",
            "avg_ask_after",
            "win_rate_before",
            "win_rate_after",
            "remaining_unique_markets",
            "vetoed_unique_markets",
        ],
        "attached_trade_columns": sorted(attached.columns.tolist()),
    }
    (output_dir / "output_schema.json").write_text(json.dumps(schema, indent=2), encoding="utf-8")
    manifest = {
        "trade_replay_path": str(trade_path),
        "hmm_artifact_root": str(args.hmm_artifact_root),
        "compact_root": str(args.compact_root),
        "output_dir": str(output_dir),
        "hmm_models": models,
        "pmax_thresholds": thresholds,
        "trade_rows_loaded": int(len(trades)),
        "attached_trade_rows": int(len(attached)),
        "hmm_coverage_ok": True,
        "chronological_validation_split": split,
        "guardrails": {
            "min_vetoed_trades": args.min_vetoed_trades,
            "min_remaining_trades": args.min_remaining_trades,
            "min_vetoed_unique_markets": args.min_vetoed_unique_markets,
            "single_layer_only": True,
            "live_code_changes": False,
        },
        **state_diag,
        **coverage,
    }
    (output_dir / "run_manifest.json").write_text(json.dumps(manifest, indent=2, default=str), encoding="utf-8")
    write_readme(output_dir / "README.txt", args, manifest, validation)
    return manifest


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="One-layer HMM regime veto attribution for BTC-5m probability-model capacity stress replay.")
    parser.add_argument("--trade-replay-path", type=Path)
    parser.add_argument("--stress-artifact-root", type=Path, default=DEFAULT_STRESS_ROOT)
    parser.add_argument("--hmm-artifact-root", type=Path, default=DEFAULT_HMM_ROOT)
    parser.add_argument("--compact-root", type=Path, default=DEFAULT_COMPACT_ROOT)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--hmm-models", default=",".join(DEFAULT_MODELS))
    parser.add_argument("--pmax-thresholds", default=",".join(f"{x:g}" for x in DEFAULT_THRESHOLDS))
    parser.add_argument("--min-vetoed-trades", type=int, default=500)
    parser.add_argument("--min-remaining-trades", type=int, default=500)
    parser.add_argument("--min-vetoed-unique-markets", type=int, default=100)
    parser.add_argument("--allow-reconstruct-hmm-from-prices", type=bool_arg, default=False)
    parser.add_argument("--hmm-price-input", type=Path)
    parser.add_argument("--max-hmm-folds", type=int)
    parser.add_argument("--overwrite", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    manifest = run(build_parser().parse_args(argv))
    print(json.dumps(manifest, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
