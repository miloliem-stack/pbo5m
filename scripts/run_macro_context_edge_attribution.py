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

from scripts import run_hmm_regime_veto_attribution as hmm_veto
from scripts import sweep_hmm_regime_health as health


DEFAULT_TRADE_REPLAY = Path("artifacts/probability_model_set_capacity_stress/compact_20260423_20260511_six_models_v1/trade_level_results.parquet")
DEFAULT_HMM_ATTR_ROOT = Path("artifacts/hmm_regime_veto_attribution/compact_20260423_20260511_phase1_v2")
DEFAULT_COMPACT_ROOT = Path("artifacts/compact_market_recorder/2026-04-23_to_2026-05-11")
DEFAULT_HMM_ARTIFACT_ROOT = Path("artifacts/hmm_regime_health/phase1_core_laplace_2to8")
DEFAULT_BINANCE_INPUT = Path("data/binance/btcusdt_1m")
DEFAULT_MICRO_MODELS = [
    "laplace_1m__gaussian_hmm__k4",
    "core_1m__gaussian_hmm__k4",
    "core_1m__gaussian_hmm__k3",
    "laplace_1m__gaussian_hmm__k3",
]
DEFAULT_BAD_STATES = [
    "core_1m__gaussian_hmm__k4:1",
    "laplace_1m__gaussian_hmm__k4:1",
    "core_1m__gaussian_hmm__k3:1",
]
PRICE_FEATURES = [
    "signed_return_30m",
    "signed_return_1h",
    "signed_return_4h",
    "signed_return_24h",
    "realized_vol_30m",
    "realized_vol_1h",
    "realized_vol_4h",
    "realized_vol_24h",
    "vol_ratio_30m_vs_4h",
    "vol_ratio_1h_vs_24h",
    "rolling_abs_return_30m",
    "rolling_abs_return_1h",
    "sign_flip_rate_30m",
    "sign_flip_rate_1h",
    "sign_flip_rate_4h",
    "shock_count_30m",
    "shock_count_1h",
    "shock_age_minutes",
    "range_position_1h",
    "range_position_4h",
    "range_position_24h",
    "distance_from_1h_high",
    "distance_from_1h_low",
    "distance_from_4h_high",
    "distance_from_4h_low",
    "same_sign_5m_streak",
    "same_sign_15m_streak",
    "same_sign_30m_streak",
]
VENUE_FEATURES = [
    "venue_median_spread_5m",
    "venue_median_spread_15m",
    "venue_median_spread_30m",
    "venue_median_top_depth_5m",
    "venue_median_top_depth_15m",
    "venue_median_top_depth_30m",
    "venue_quote_count_5m",
    "venue_quote_count_15m",
    "venue_quote_count_30m",
    "venue_valid_ratio_5m",
    "venue_valid_ratio_15m",
    "venue_valid_ratio_30m",
    "venue_avg_yes_ask_5m",
    "venue_avg_no_ask_5m",
]


def parse_csv(value: str) -> list[str]:
    return [item.strip() for item in str(value).split(",") if item.strip()]


def bool_arg(value: str | bool) -> bool:
    if isinstance(value, bool):
        return value
    lower = str(value).strip().lower()
    if lower in {"1", "true", "yes", "y", "on"}:
        return True
    if lower in {"0", "false", "no", "n", "off"}:
        return False
    raise argparse.ArgumentTypeError(f"expected bool, got {value!r}")


def parse_bad_states(value: str) -> list[tuple[str, int]]:
    out = []
    for item in parse_csv(value):
        model, state = item.rsplit(":", 1)
        out.append((model, int(state)))
    return out


def read_frame(path: Path) -> pd.DataFrame:
    return hmm_veto.read_frame(path)


def write_frame(frame: pd.DataFrame, path: Path) -> str:
    return hmm_veto.write_frame(frame, path)


def load_hmm_attached_rows(args: argparse.Namespace, output_dir: Path) -> tuple[pd.DataFrame, dict[str, Any]]:
    attached_path = Path(args.hmm_attribution_root) / "trade_level_with_hmm.parquet"
    if attached_path.exists():
        frame = read_frame(attached_path)
        source = str(attached_path)
    else:
        recon_dir = output_dir / "_reconstructed_hmm_attached"
        manifest = hmm_veto.run(
            hmm_veto.build_parser().parse_args(
                [
                    "--trade-replay-path",
                    str(args.trade_replay_path),
                    "--hmm-artifact-root",
                    str(args.hmm_artifact_root),
                    "--compact-root",
                    str(args.compact_root),
                    "--output-dir",
                    str(recon_dir),
                    "--hmm-models",
                    args.micro_hmm_models,
                    "--allow-reconstruct-hmm-from-prices",
                    "true",
                    "--hmm-price-input",
                    str(args.binance_input),
                    "--overwrite",
                ]
            )
        )
        frame = read_frame(recon_dir / "trade_level_with_hmm.parquet")
        source = manifest["output_dir"] + "/trade_level_with_hmm.parquet"
    frame = hmm_veto.normalize_trades(frame)
    required = ["hmm_model_id", "hmm_state", "hmm_pmax", "timestamp"]
    missing = [col for col in required if col not in frame.columns]
    if missing:
        raise ValueError(f"HMM-attached rows missing columns: {missing}")
    models = set(parse_csv(args.micro_hmm_models))
    frame = frame[frame["hmm_model_id"].astype(str).isin(models)].copy()
    return frame, {"hmm_attached_source": source, "hmm_attached_rows": int(len(frame))}


def _sign_flip_rate(log_returns: pd.Series, window: int) -> pd.Series:
    signs = np.sign(log_returns.fillna(0.0))
    prev = signs.shift(1)
    valid = ((signs != 0) & (prev != 0)).astype(float)
    flips = ((signs != prev) & valid.eq(1.0)).astype(float)
    return flips.rolling(window=window, min_periods=1).sum() / valid.rolling(window=window, min_periods=1).sum().replace(0.0, np.nan)


def _same_sign_streak(values: pd.Series) -> pd.Series:
    signs = np.sign(values.fillna(0.0)).astype(int)
    streak = []
    current_sign = 0
    current_len = 0
    for sign in signs:
        if sign == 0:
            current_sign = 0
            current_len = 0
        elif sign == current_sign:
            current_len += 1
        else:
            current_sign = int(sign)
            current_len = 1
        streak.append(current_len * current_sign)
    return pd.Series(streak, index=values.index, dtype=float)


def build_price_macro_features_from_prices(prices: pd.DataFrame) -> pd.DataFrame:
    out = prices.sort_values("timestamp").drop_duplicates("timestamp", keep="last").reset_index(drop=True).copy()
    out["log_close"] = np.log(pd.to_numeric(out["close"], errors="coerce"))
    out["log_return_1m"] = out["log_close"].diff()
    windows = {"30m": 30, "1h": 60, "4h": 240, "24h": 1440}
    for label, window in windows.items():
        out[f"signed_return_{label}"] = out["log_close"] - out["log_close"].shift(window)
        out[f"realized_vol_{label}"] = out["log_return_1m"].rolling(window=window, min_periods=max(2, min(window, 30))).std(ddof=0)
        out[f"rolling_abs_return_{label}"] = out["log_return_1m"].abs().rolling(window=window, min_periods=1).mean()
        rolling_high = out["close"].rolling(window=window, min_periods=1).max()
        rolling_low = out["close"].rolling(window=window, min_periods=1).min()
        denom = (rolling_high - rolling_low).replace(0.0, np.nan)
        out[f"range_position_{label}"] = (out["close"] - rolling_low) / denom
        out[f"distance_from_{label}_high"] = np.log(out["close"] / rolling_high.replace(0.0, np.nan))
        out[f"distance_from_{label}_low"] = np.log(out["close"] / rolling_low.replace(0.0, np.nan))
    out["vol_ratio_30m_vs_4h"] = out["realized_vol_30m"] / out["realized_vol_4h"].replace(0.0, np.nan)
    out["vol_ratio_1h_vs_24h"] = out["realized_vol_1h"] / out["realized_vol_24h"].replace(0.0, np.nan)
    for label, window in {"30m": 30, "1h": 60, "4h": 240}.items():
        out[f"sign_flip_rate_{label}"] = _sign_flip_rate(out["log_return_1m"], window)
    shock = out["log_return_1m"].abs() > (3.0 * out["realized_vol_30m"].shift(1))
    out["shock_count_30m"] = shock.astype(float).rolling(window=30, min_periods=1).sum()
    out["shock_count_1h"] = shock.astype(float).rolling(window=60, min_periods=1).sum()
    last_shock_ts = out["timestamp"].where(shock).ffill()
    out["shock_age_minutes"] = (out["timestamp"] - last_shock_ts).dt.total_seconds() / 60.0
    out.loc[last_shock_ts.isna(), "shock_age_minutes"] = np.inf
    for label, window in {"5m": 5, "15m": 15, "30m": 30}.items():
        ret = out["log_close"] - out["log_close"].shift(window)
        out[f"same_sign_{label}_streak"] = _same_sign_streak(ret)
    keep = ["timestamp", "close"] + [col for col in PRICE_FEATURES if col in out.columns]
    return out[keep]


def build_price_macro_features(binance_input: Path) -> pd.DataFrame:
    prices = health.load_price_frame(binance_input)
    return build_price_macro_features_from_prices(prices)


def build_venue_features(compact_root: Path) -> tuple[pd.DataFrame, list[str]]:
    ticks_path = compact_root / "book_ticks.parquet"
    if not ticks_path.exists():
        return pd.DataFrame(), [f"missing compact book_ticks parquet: {ticks_path}"]
    ticks = read_frame(ticks_path)
    required = {"ts", "side", "spread", "is_valid_topbook", "ask_px_1", "ask_sz_1"}
    missing = sorted(required - set(ticks.columns))
    if missing:
        return pd.DataFrame(), [f"book_ticks missing venue feature columns: {missing}"]
    t = ticks[["ts", "side", "spread", "is_valid_topbook", "ask_px_1", "ask_sz_1"]].copy()
    t["timestamp"] = pd.to_datetime(t["ts"], utc=True, errors="coerce")
    t["side"] = t["side"].astype(str).str.upper()
    t["top_depth"] = pd.to_numeric(t["ask_px_1"], errors="coerce") * pd.to_numeric(t["ask_sz_1"], errors="coerce")
    t["spread"] = pd.to_numeric(t["spread"], errors="coerce")
    t["is_valid_topbook"] = t["is_valid_topbook"].astype(float)
    agg = (
        t.groupby("timestamp", dropna=True)
        .agg(
            spread=("spread", "median"),
            top_depth=("top_depth", "median"),
            quote_count=("side", "size"),
            valid_ratio=("is_valid_topbook", "mean"),
            yes_ask=("ask_px_1", lambda s: pd.to_numeric(s[t.loc[s.index, "side"].eq("YES")], errors="coerce").mean()),
            no_ask=("ask_px_1", lambda s: pd.to_numeric(s[t.loc[s.index, "side"].eq("NO")], errors="coerce").mean()),
        )
        .sort_index()
    )
    # closed="left" excludes the current compact tick, so exact timestamp joins remain previous-only.
    for minutes in (5, 15, 30):
        window = f"{minutes}min"
        suffix = f"{minutes}m"
        agg[f"venue_median_spread_{suffix}"] = agg["spread"].rolling(window, closed="left").median()
        agg[f"venue_median_top_depth_{suffix}"] = agg["top_depth"].rolling(window, closed="left").median()
        agg[f"venue_quote_count_{suffix}"] = agg["quote_count"].rolling(window, closed="left").sum()
        agg[f"venue_valid_ratio_{suffix}"] = agg["valid_ratio"].rolling(window, closed="left").mean()
    agg["venue_avg_yes_ask_5m"] = agg["yes_ask"].rolling("5min", closed="left").mean()
    agg["venue_avg_no_ask_5m"] = agg["no_ask"].rolling("5min", closed="left").mean()
    return agg.reset_index()[["timestamp"] + [col for col in VENUE_FEATURES if col in agg.columns]], []


def asof_attach_features(trades: pd.DataFrame, features: pd.DataFrame, feature_cols: list[str], source_name: str) -> tuple[pd.DataFrame, dict[str, Any]]:
    if features.empty:
        return trades.copy(), {f"{source_name}_coverage": 0.0, f"{source_name}_features": []}
    left = trades.sort_values("timestamp", kind="mergesort").copy()
    right = features[["timestamp"] + feature_cols].sort_values("timestamp", kind="mergesort").copy()
    left["timestamp"] = pd.to_datetime(left["timestamp"], utc=True, errors="coerce").dt.as_unit("ns")
    right["timestamp"] = pd.to_datetime(right["timestamp"], utc=True, errors="coerce").dt.as_unit("ns")
    attached = pd.merge_asof(left, right, on="timestamp", direction="backward", allow_exact_matches=True)
    coverage = float(attached[feature_cols].notna().any(axis=1).mean()) if feature_cols else 0.0
    return attached, {f"{source_name}_coverage": coverage, f"{source_name}_features": feature_cols}


def named_bucket(feature: str, values: pd.Series) -> pd.Series | None:
    x = pd.to_numeric(values, errors="coerce")
    if feature.startswith("signed_return_"):
        return pd.Series(np.select([x < -0.001, x > 0.001], ["negative", "positive"], default="flat"), index=values.index, dtype=object).where(x.notna(), "missing")
    if feature.startswith("range_position_"):
        return pd.Series(np.select([x < 1 / 3, x > 2 / 3], ["low", "high"], default="middle"), index=values.index, dtype=object).where(x.notna(), "missing")
    if feature == "shock_age_minutes":
        return pd.Series(np.select([x <= 15, x <= 120], ["fresh_shock", "recent_shock"], default="stale_or_none"), index=values.index, dtype=object).where(x.notna(), "missing")
    return None


def fit_bucket_edges(frame: pd.DataFrame, features: list[str], train_slices: list[str], q: int) -> dict[str, Any]:
    train = frame[frame["chronological_slice"].astype(str).isin(train_slices)]
    edges: dict[str, Any] = {}
    for feature in features:
        if feature not in frame.columns:
            continue
        if named_bucket(feature, frame[feature]) is not None:
            edges[feature] = {"method": "named"}
            continue
        values = pd.to_numeric(train[feature], errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()
        if values.empty:
            edges[feature] = {"method": "missing"}
            continue
        quantiles = np.linspace(0.0, 1.0, q + 1)
        bins = np.unique(np.nanquantile(values, quantiles)).astype(float)
        if len(bins) < 3:
            edges[feature] = {"method": "constant", "value": float(values.median())}
        else:
            bins[0] = -np.inf
            bins[-1] = np.inf
            edges[feature] = {"method": "quantile", "q": int(q), "edges": bins.tolist()}
    return edges


def apply_buckets(frame: pd.DataFrame, edges: dict[str, Any]) -> pd.DataFrame:
    out = frame.copy()
    for feature, spec in edges.items():
        bucket_col = f"{feature}_bucket"
        if spec["method"] == "named":
            out[bucket_col] = named_bucket(feature, out[feature])
        elif spec["method"] == "quantile":
            labels = [f"q{i + 1}" for i in range(len(spec["edges"]) - 1)]
            out[bucket_col] = pd.cut(pd.to_numeric(out[feature], errors="coerce"), spec["edges"], labels=labels, include_lowest=True).astype("object").fillna("missing")
        elif spec["method"] == "constant":
            out[bucket_col] = np.where(pd.to_numeric(out[feature], errors="coerce").notna(), "all", "missing")
        else:
            out[bucket_col] = "missing"
    return out


def metrics(frame: pd.DataFrame) -> dict[str, Any]:
    cost = pd.to_numeric(frame.get("gross_cost"), errors="coerce").sum()
    pnl = pd.to_numeric(frame.get("pnl"), errors="coerce").sum()
    by_slice = frame.groupby("chronological_slice")["pnl"].sum().to_dict() if "chronological_slice" in frame.columns and len(frame) else {}
    return {
        "trades": int(len(frame)),
        "unique_markets": int(frame["market_id"].nunique()) if "market_id" in frame.columns and len(frame) else 0,
        "gross_cost": float(cost),
        "pnl": float(pnl),
        "roi": float(pnl / cost) if cost else np.nan,
        "win_rate": float(pd.to_numeric(frame.get("win"), errors="coerce").mean()) if len(frame) else np.nan,
        "avg_ask": float(pd.to_numeric(frame.get("ask_price"), errors="coerce").mean()) if len(frame) else np.nan,
        "avg_hmm_pmax": float(pd.to_numeric(frame.get("hmm_pmax"), errors="coerce").mean()) if len(frame) else np.nan,
        "slice_pnl_min": float(min(by_slice.values())) if by_slice else np.nan,
        "slice_pnl_max": float(max(by_slice.values())) if by_slice else np.nan,
    }


def aggregate_long(frame: pd.DataFrame, feature_bucket_cols: list[str], group_cols: list[str], min_trades: int, min_markets: int) -> pd.DataFrame:
    rows = []
    for bucket_col in feature_bucket_cols:
        feature = bucket_col.removesuffix("_bucket")
        for keys, group in frame.groupby([bucket_col] + group_cols, dropna=False, sort=True):
            if not isinstance(keys, tuple):
                keys = (keys,)
            row = {"macro_feature": feature, "macro_bucket": keys[0]}
            row.update(dict(zip(group_cols, keys[1:])))
            row.update(metrics(group))
            row["passes_support"] = bool(row["trades"] >= min_trades and row["unique_markets"] >= min_markets)
            rows.append(row)
    return pd.DataFrame(rows)


def focus_rows(frame: pd.DataFrame, model_id: str, state: int) -> pd.DataFrame:
    return frame[frame["hmm_model_id"].eq(model_id) & pd.to_numeric(frame["hmm_state"], errors="coerce").eq(state)].copy()


def bad_state_rows(frame: pd.DataFrame, bad_states: list[tuple[str, int]]) -> pd.DataFrame:
    parts = []
    for model_id, state in bad_states:
        part = focus_rows(frame, model_id, state)
        part["bad_state_key"] = f"{model_id}:{state}"
        parts.append(part)
    return pd.concat(parts, ignore_index=True) if parts else frame.iloc[0:0].copy()


def allow_filter_scan(focus: pd.DataFrame, feature_bucket_cols: list[str], train_slices: list[str], test_slices: list[str], min_trades: int, min_markets: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    specs: list[dict[str, Any]] = [{"filter_name": "focus_state_only", "macro_feature": None, "macro_bucket": None, "ask_filter": "none", "exclude_model": "none"}]
    ask_mask = pd.to_numeric(focus["ask_price"], errors="coerce").between(0.30, 0.47, inclusive="neither")
    specs.append({"filter_name": "focus_state_ask_0.30_0.47", "macro_feature": None, "macro_bucket": None, "ask_filter": "0.30_0.47", "exclude_model": "none"})
    if "model_id" in focus.columns:
        specs.append({"filter_name": "focus_state_exclude_calibrated_logistic", "macro_feature": None, "macro_bucket": None, "ask_filter": "none", "exclude_model": "calibrated_logistic__gbm_rv30"})
    for bucket_col in feature_bucket_cols:
        feature = bucket_col.removesuffix("_bucket")
        for bucket in sorted(focus[bucket_col].dropna().astype(str).unique()):
            if bucket == "missing":
                continue
            specs.append({"filter_name": f"{feature}={bucket}", "macro_feature": feature, "macro_bucket": bucket, "ask_filter": "none", "exclude_model": "none"})
            specs.append({"filter_name": f"{feature}={bucket}__ask_0.30_0.47", "macro_feature": feature, "macro_bucket": bucket, "ask_filter": "0.30_0.47", "exclude_model": "none"})
    rows = []
    val_rows = []
    train = focus[focus["chronological_slice"].astype(str).isin(train_slices)]
    test = focus[focus["chronological_slice"].astype(str).isin(test_slices)]
    for spec in specs:
        mask = pd.Series(True, index=focus.index)
        train_mask = pd.Series(True, index=train.index)
        test_mask = pd.Series(True, index=test.index)
        if spec["macro_feature"] is not None:
            col = f"{spec['macro_feature']}_bucket"
            mask &= focus[col].astype(str).eq(spec["macro_bucket"])
            train_mask &= train[col].astype(str).eq(spec["macro_bucket"])
            test_mask &= test[col].astype(str).eq(spec["macro_bucket"])
        if spec["ask_filter"] != "none":
            mask &= ask_mask
            train_mask &= pd.to_numeric(train["ask_price"], errors="coerce").between(0.30, 0.47, inclusive="neither")
            test_mask &= pd.to_numeric(test["ask_price"], errors="coerce").between(0.30, 0.47, inclusive="neither")
        if spec["exclude_model"] != "none":
            mask &= ~focus["model_id"].astype(str).eq(spec["exclude_model"])
            train_mask &= ~train["model_id"].astype(str).eq(spec["exclude_model"])
            test_mask &= ~test["model_id"].astype(str).eq(spec["exclude_model"])
        full_metrics = metrics(focus[mask])
        rows.append({**spec, **full_metrics, "passes_support": bool(full_metrics["trades"] >= min_trades and full_metrics["unique_markets"] >= min_markets)})
        train_metrics = metrics(train[train_mask])
        test_metrics = metrics(test[test_mask])
        val_rows.append(
            {
                **spec,
                **{f"train_{k}": v for k, v in train_metrics.items()},
                **{f"test_{k}": v for k, v in test_metrics.items()},
                "passes_train_support": bool(train_metrics["trades"] >= min_trades and train_metrics["unique_markets"] >= min_markets),
                "passes_test_support": bool(test_metrics["trades"] >= min_trades and test_metrics["unique_markets"] >= min_markets),
            }
        )
    return pd.DataFrame(rows).sort_values(["passes_support", "roi", "pnl"], ascending=[False, False, False]), pd.DataFrame(val_rows).sort_values(["passes_train_support", "test_roi", "test_pnl"], ascending=[False, False, False])


def ensure_feature_coverage(frame: pd.DataFrame, feature_cols: list[str], max_missing_share: float) -> dict[str, Any]:
    has_any = frame[feature_cols].notna().any(axis=1) if feature_cols else pd.Series(False, index=frame.index)
    missing_share = float((~has_any).mean()) if len(frame) else 1.0
    if missing_share > max_missing_share:
        raise ValueError(f"macro feature coverage dropped too many rows: missing_share={missing_share:.3f} max={max_missing_share:.3f}")
    return {"macro_feature_missing_share": missing_share}


def write_readme(path: Path, args: argparse.Namespace, manifest: dict[str, Any], focus_attr: pd.DataFrame, validation: pd.DataFrame, caveats: list[str]) -> None:
    main_fail = focus_attr[focus_attr.get("chronological_slice", pd.Series(dtype=str)).astype(str).eq("main")].sort_values("pnl").head(5) if not focus_attr.empty and "chronological_slice" in focus_attr.columns else pd.DataFrame()
    lines = [
        "Macro context edge attribution",
        "",
        "Offline research only. No live trading behavior was changed.",
        "",
        "Inputs:",
        f"- trade_replay_path={args.trade_replay_path}",
        f"- hmm_attribution_root={args.hmm_attribution_root}",
        f"- compact_root={args.compact_root}",
        f"- binance_input={args.binance_input}",
        "",
        f"trade_rows_loaded={manifest.get('trade_rows_loaded')}",
        f"hmm_attached_rows={manifest.get('hmm_attached_rows')}",
        f"macro_feature_coverage={1.0 - manifest.get('macro_feature_missing_share', 1.0):.6f}",
        f"bucket_method=train-slice quantiles q={args.quantile_buckets} plus named sign/range/shock buckets",
        f"bucket_train_slices={args.train_slices}",
        f"validation_test_slices={args.test_slices}",
        f"focus_state={args.focus_hmm_model}:{args.focus_hmm_state}",
        "",
        "Top macro buckets for focus-state main-slice losses:",
    ]
    if main_fail.empty:
        lines.append("- none")
    else:
        for row in main_fail.itertuples(index=False):
            lines.append(f"- {row.macro_feature}={row.macro_bucket} pnl={row.pnl:.6g} roi={row.roi:.6g} trades={row.trades}")
    lines.append("")
    lines.append("Macro-conditioned allow validation:")
    if validation.empty:
        lines.append("- no validation rows")
    else:
        top = validation[validation["passes_train_support"]].head(5)
        if top.empty:
            lines.append("- no allow filter passed train support")
        for row in top.itertuples(index=False):
            lines.append(f"- {row.filter_name} train_roi={row.train_roi:.6g} test_roi={row.test_roi:.6g} test_pnl={row.test_pnl:.6g} test_trades={row.test_trades}")
    lines.append("")
    lines.append("Caveats:")
    base_caveats = [
        "Macro and venue features are previous-only asof joins; no future rows are used.",
        "Bucket edges are fit only on configured train slices, then reused for other slices.",
        "This is simple one-condition filtering only; no macro HMM or multi-layer HMM stack is built.",
        "Outcome labels/PnL are not used in feature construction.",
    ]
    for caveat in base_caveats + caveats:
        lines.append(f"- {caveat}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run(args: argparse.Namespace) -> dict[str, Any]:
    output_dir = Path(args.output_dir)
    if output_dir.exists():
        if not args.overwrite:
            raise FileExistsError(f"{output_dir} exists; pass --overwrite")
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    caveats: list[str] = []
    train_slices = parse_csv(args.train_slices)
    test_slices = parse_csv(args.test_slices)
    attached, load_diag = load_hmm_attached_rows(args, output_dir)
    trade_rows_loaded = len(read_frame(args.trade_replay_path))

    price_features = build_price_macro_features(args.binance_input)
    price_cols = [col for col in PRICE_FEATURES if col in price_features.columns]
    attached, price_diag = asof_attach_features(attached, price_features, price_cols, "price")
    venue_features, venue_caveats = build_venue_features(args.compact_root)
    caveats.extend(venue_caveats)
    venue_cols = [col for col in VENUE_FEATURES if col in venue_features.columns]
    attached, venue_diag = asof_attach_features(attached, venue_features, venue_cols, "venue")
    feature_cols = price_cols + venue_cols
    coverage_diag = ensure_feature_coverage(attached, price_cols, args.max_missing_feature_share)

    edges = fit_bucket_edges(attached, feature_cols, train_slices, args.quantile_buckets)
    attached = apply_buckets(attached, edges)
    bucket_cols = [f"{feature}_bucket" for feature in feature_cols if f"{feature}_bucket" in attached.columns]
    write_frame(attached, output_dir / "trade_level_with_macro_context.parquet")
    (output_dir / "bucket_edges.json").write_text(json.dumps(edges, indent=2, default=str), encoding="utf-8")

    focus = focus_rows(attached, args.focus_hmm_model, int(args.focus_hmm_state))
    bad = bad_state_rows(attached, parse_bad_states(args.bad_hmm_state_candidates))
    min_trades = int(args.min_trades_per_bucket)
    min_markets = int(args.min_unique_markets_per_bucket)

    focus_attr = aggregate_long(focus, bucket_cols, [], min_trades, min_markets)
    focus_by_slice = aggregate_long(focus, bucket_cols, ["chronological_slice"], min_trades, min_markets)
    focus_attr.to_csv(output_dir / "focus_state3_macro_bucket_attribution.csv", index=False)
    focus_by_slice.to_csv(output_dir / "focus_state3_macro_bucket_by_slice.csv", index=False)
    aggregate_long(focus, bucket_cols, ["ask_bin"], min_trades, min_markets).to_csv(output_dir / "focus_state3_macro_bucket_by_ask_bin.csv", index=False)
    aggregate_long(focus, bucket_cols, ["side"], min_trades, min_markets).to_csv(output_dir / "focus_state3_macro_bucket_by_side.csv", index=False)
    aggregate_long(focus, bucket_cols, ["model_id"], min_trades, min_markets).to_csv(output_dir / "focus_state3_macro_bucket_by_model_id.csv", index=False)
    aggregate_long(focus, bucket_cols, ["entry_age_window"], min_trades, min_markets).to_csv(output_dir / "focus_state3_macro_bucket_by_entry_age_window.csv", index=False)

    aggregate_long(bad, bucket_cols, ["bad_state_key"], min_trades, min_markets).to_csv(output_dir / "bad_state_macro_bucket_attribution.csv", index=False)
    aggregate_long(bad, bucket_cols, ["bad_state_key", "chronological_slice"], min_trades, min_markets).to_csv(output_dir / "bad_state_macro_bucket_by_slice.csv", index=False)

    scan, validation = allow_filter_scan(focus, bucket_cols, train_slices, test_slices, min_trades, min_markets)
    scan.to_csv(output_dir / "focus_state3_allow_filter_scan.csv", index=False)
    validation.to_csv(output_dir / "focus_state3_allow_filter_validation.csv", index=False)

    schema = {
        "focus_macro_bucket_outputs": ["macro_feature", "macro_bucket", "trades", "unique_markets", "gross_cost", "pnl", "roi", "win_rate", "avg_ask", "avg_hmm_pmax", "passes_support"],
        "allow_filter_validation": ["filter_name", "macro_feature", "macro_bucket", "ask_filter", "exclude_model", "train_* metrics", "test_* metrics", "passes_train_support", "passes_test_support"],
        "attached_trade_columns": sorted(attached.columns.tolist()),
    }
    (output_dir / "output_schema.json").write_text(json.dumps(schema, indent=2), encoding="utf-8")
    manifest = {
        "trade_replay_path": str(args.trade_replay_path),
        "hmm_attribution_root": str(args.hmm_attribution_root),
        "compact_root": str(args.compact_root),
        "binance_input": str(args.binance_input),
        "output_dir": str(output_dir),
        "trade_rows_loaded": int(trade_rows_loaded),
        "focus_hmm_model": args.focus_hmm_model,
        "focus_hmm_state": int(args.focus_hmm_state),
        "focus_rows": int(len(focus)),
        "bucket_train_slices": train_slices,
        "validation_test_slices": test_slices,
        "bucket_count": len(bucket_cols),
        "caveats": caveats,
        **load_diag,
        **price_diag,
        **venue_diag,
        **coverage_diag,
    }
    (output_dir / "run_manifest.json").write_text(json.dumps(manifest, indent=2, default=str), encoding="utf-8")
    write_readme(output_dir / "README.txt", args, manifest, focus_by_slice, validation, caveats)
    return manifest


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Offline macro/venue context attribution for BTC-5m micro-HMM regime edge instability.")
    parser.add_argument("--trade-replay-path", type=Path, default=DEFAULT_TRADE_REPLAY)
    parser.add_argument("--hmm-attribution-root", type=Path, default=DEFAULT_HMM_ATTR_ROOT)
    parser.add_argument("--compact-root", type=Path, default=DEFAULT_COMPACT_ROOT)
    parser.add_argument("--binance-price-root", "--binance-input", dest="binance_input", type=Path, default=DEFAULT_BINANCE_INPUT)
    parser.add_argument("--hmm-artifact-root", type=Path, default=DEFAULT_HMM_ARTIFACT_ROOT)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--micro-hmm-models", default=",".join(DEFAULT_MICRO_MODELS))
    parser.add_argument("--focus-hmm-model", default="laplace_1m__gaussian_hmm__k4")
    parser.add_argument("--focus-hmm-state", type=int, default=3)
    parser.add_argument("--bad-hmm-state-candidates", default=",".join(DEFAULT_BAD_STATES))
    parser.add_argument("--train-slices", default="early")
    parser.add_argument("--test-slices", default="main,fresh")
    parser.add_argument("--quantile-buckets", type=int, default=5)
    parser.add_argument("--min-trades-per-bucket", type=int, default=500)
    parser.add_argument("--min-unique-markets-per-bucket", type=int, default=50)
    parser.add_argument("--max-missing-feature-share", type=float, default=0.05)
    parser.add_argument("--overwrite", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    manifest = run(build_parser().parse_args(argv))
    print(json.dumps(manifest, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
