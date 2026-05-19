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


DEFAULT_THRESHOLDS = [0.45, 0.47, 0.49, 0.50]
DEFAULT_STAKES = [1.0, 2.0, 5.0, 10.0, 20.0, 50.0]
DEFAULT_AGE_BUCKETS = "0:60,60:120,120:180,180:240,240:300"
DEFAULT_OUTPUT_ROOT = Path("artifacts/baseline_capacity_stress")


def parse_csv_floats(value: str) -> list[float]:
    return [float(item.strip()) for item in str(value).split(",") if item.strip()]


def parse_age_buckets(value: str) -> list[tuple[float, float, str]]:
    out = []
    for item in str(value).split(","):
        if not item.strip():
            continue
        lo, hi = item.split(":", 1)
        label = f"{float(lo):g}_{float(hi):g}"
        out.append((float(lo), float(hi), label))
    return out


def bool_arg(value: str | bool) -> bool:
    if isinstance(value, bool):
        return value
    lower = str(value).strip().lower()
    if lower in {"1", "true", "yes", "y", "on"}:
        return True
    if lower in {"0", "false", "no", "n", "off"}:
        return False
    raise argparse.ArgumentTypeError(f"expected bool, got {value!r}")


def read_frame(path: Path) -> pd.DataFrame:
    if path.exists() and path.suffix.lower() == ".parquet":
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


def write_parquet_or_json(frame: pd.DataFrame, path: Path) -> str:
    try:
        frame.to_parquet(path, index=False)
        return str(path)
    except Exception:
        fallback = Path(str(path) + ".as.json")
        frame.to_json(fallback, orient="records", lines=True, date_format="iso")
        return str(fallback)


def load_compact(compact_root: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    windows = read_frame(compact_root / "market_windows.parquet")
    ticks = read_frame(compact_root / "book_ticks.parquet")
    windows["market_start_ts"] = pd.to_datetime(windows["market_start_ts"], utc=True, errors="coerce")
    windows["market_end_ts"] = pd.to_datetime(windows["market_end_ts"], utc=True, errors="coerce")
    ticks["ts"] = pd.to_datetime(ticks["ts"], utc=True, errors="coerce")
    return windows, ticks


def filter_windows(windows: pd.DataFrame, start_date: str | None, end_date: str | None, max_markets: int | None) -> pd.DataFrame:
    out = windows.copy()
    if start_date:
        out = out[out["market_start_ts"] >= pd.Timestamp(start_date, tz="UTC")]
    if end_date:
        out = out[out["market_start_ts"] < pd.Timestamp(end_date, tz="UTC") + pd.Timedelta(days=1)]
    out = out.sort_values("market_start_ts")
    if max_markets:
        out = out.head(max_markets)
    return out.reset_index(drop=True)


def age_bucket(age: float, buckets: list[tuple[float, float, str]]) -> str:
    if pd.isna(age):
        return "missing"
    for lo, hi, label in buckets:
        if lo <= age < hi:
            return label
    return "other"


def chronological_slice(ts: pd.Timestamp) -> str:
    if pd.isna(ts):
        return "unknown"
    day = pd.Timestamp(ts).tz_convert("UTC").date()
    if pd.Timestamp("2026-04-23").date() <= day <= pd.Timestamp("2026-05-11").date():
        if day <= pd.Timestamp("2026-04-30").date():
            return "early"
        if day <= pd.Timestamp("2026-05-08").date():
            return "main"
        return "fresh"
    return "out_of_named_range"


def fill_against_asks(row: pd.Series, stake: float, top_n: int, capacity_aware: bool) -> dict[str, float | bool]:
    remaining = float(stake)
    shares = 0.0
    cost = 0.0
    levels_used = top_n if capacity_aware else min(1, top_n)
    for idx in range(1, levels_used + 1):
        price = pd.to_numeric(row.get(f"ask_px_{idx}"), errors="coerce")
        size = pd.to_numeric(row.get(f"ask_sz_{idx}"), errors="coerce")
        if not np.isfinite(price) or not np.isfinite(size) or price <= 0 or price > 1 or size <= 0:
            continue
        level_notional = float(price * size)
        spend = min(remaining, level_notional)
        if spend <= 0:
            continue
        shares += spend / float(price)
        cost += spend
        remaining -= spend
        if remaining <= 1e-12:
            break
    return {
        "filled_shares": float(shares),
        "filled_notional": float(cost),
        "fill_rate": float(cost / stake) if stake else 0.0,
        "capacity_shortfall": bool(cost + 1e-12 < stake),
    }


def prepare_ticks(ticks: pd.DataFrame, windows: pd.DataFrame, *, valid_topbook_only: bool, entry_age_min: float, entry_age_max: float) -> pd.DataFrame:
    cols = ["market_key", "winner_side", "market_start_ts"]
    out = ticks.merge(windows[cols], on="market_key", how="inner")
    out["market_age_sec"] = pd.to_numeric(out["market_age_sec"], errors="coerce")
    out["ask_px_1"] = pd.to_numeric(out["ask_px_1"], errors="coerce")
    out = out[(out["market_age_sec"] >= entry_age_min) & (out["market_age_sec"] <= entry_age_max)]
    if valid_topbook_only and "is_valid_topbook" in out.columns:
        out = out[out["is_valid_topbook"].astype(bool)]
    return out.reset_index(drop=True)


def select_first_entries(
    prepared_ticks: pd.DataFrame,
    windows: pd.DataFrame,
    thresholds: list[float],
    first_entry_only: bool,
    invalid_book_keys: set[Any] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    entries = []
    skipped = []
    labelled = windows[windows["winner_side"].isin(["YES", "NO"])]
    for threshold in thresholds:
        candidates = prepared_ticks[(prepared_ticks["winner_side"].isin(["YES", "NO"])) & (prepared_ticks["ask_px_1"] <= threshold)].copy()
        candidates["ask_threshold"] = threshold
        candidates = candidates.sort_values(["market_key", "ts", "market_age_sec", "ask_px_1", "side"], kind="mergesort")
        if first_entry_only:
            selected = candidates.drop_duplicates(["market_key", "ask_threshold"], keep="first")
        else:
            selected = candidates
        entries.append(selected)
        selected_keys = set(selected["market_key"].unique())
        tick_keys = set(prepared_ticks["market_key"].unique())
        valid_tick_keys = set(prepared_ticks[prepared_ticks["winner_side"].isin(["YES", "NO"])]["market_key"].unique())
        for _, market in windows.iterrows():
            key = market["market_key"]
            reason = None
            if market.get("winner_side") not in {"YES", "NO"}:
                reason = "no_label"
            elif invalid_book_keys and key in invalid_book_keys and key not in valid_tick_keys:
                reason = "invalid_book"
            elif key not in tick_keys:
                reason = "no_quote"
            elif key not in valid_tick_keys:
                reason = "invalid_book"
            elif key not in selected_keys:
                reason = "no_eligible_entry"
            if reason:
                skipped.append({"market_key": key, "ask_threshold": threshold, "skip_reason": reason})
    return (pd.concat(entries, ignore_index=True) if entries else pd.DataFrame(), pd.DataFrame(skipped))


def simulate_trades(entries: pd.DataFrame, stakes: list[float], top_n: int, capacity_aware: bool, buckets: list[tuple[float, float, str]]) -> tuple[pd.DataFrame, pd.DataFrame]:
    trade_rows = []
    market_rows = []
    for _, entry in entries.iterrows():
        for stake in stakes:
            fill = fill_against_asks(entry, stake, top_n, capacity_aware)
            win = str(entry["side"]) == str(entry["winner_side"])
            payout = fill["filled_shares"] if win else 0.0
            pnl = float(payout - fill["filled_notional"])
            base = {
                "market_key": entry["market_key"],
                "market_start_ts": entry["market_start_ts"],
                "ts": entry["ts"],
                "side": entry["side"],
                "winner_side": entry["winner_side"],
                "ask_threshold": entry["ask_threshold"],
                "stake_size": stake,
                "entry_ask": entry["ask_px_1"],
                "entry_age_sec": entry["market_age_sec"],
                "entry_age_bucket": age_bucket(float(entry["market_age_sec"]), buckets),
                "chronological_slice": chronological_slice(entry["market_start_ts"]),
                "notional_requested": stake,
                "notional_filled": fill["filled_notional"],
                "filled_shares": fill["filled_shares"],
                "gross_cost": fill["filled_notional"],
                "gross_payout": payout,
                "gross_pnl": pnl,
                "roi_on_filled_cost": pnl / fill["filled_notional"] if fill["filled_notional"] else np.nan,
                "win": bool(win),
                "fill_rate": fill["fill_rate"],
                "capacity_shortfall": fill["capacity_shortfall"],
            }
            status = "filled" if fill["filled_notional"] > 0 else "no_fill"
            market_rows.append({**base, "status": status})
            if status == "filled":
                trade_rows.append(base)
    return pd.DataFrame(trade_rows), pd.DataFrame(market_rows)


def max_drawdown(pnl: pd.Series) -> float:
    values = pd.to_numeric(pnl, errors="coerce").fillna(0.0).to_numpy()
    if values.size == 0:
        return 0.0
    equity = np.cumsum(values)
    peaks = np.maximum.accumulate(np.insert(equity, 0, 0.0))[1:]
    return float((equity - peaks).min()) if len(equity) else 0.0


def summarize(trades: pd.DataFrame, market_results: pd.DataFrame, skipped: pd.DataFrame, windows: pd.DataFrame, group_cols: list[str]) -> pd.DataFrame:
    if market_results.empty:
        return pd.DataFrame()
    rows = []
    for keys, group in market_results.groupby(group_cols, dropna=False):
        if not isinstance(keys, tuple):
            keys = (keys,)
        trade_group = group[group["status"].eq("filled")].copy()
        skip_group = skipped
        for col, key in zip(group_cols, keys):
            if col in skip_group.columns:
                skip_group = skip_group[skip_group[col].eq(key)]
        total_cost = float(trade_group["gross_cost"].sum()) if not trade_group.empty else 0.0
        row = dict(zip(group_cols, keys))
        row.update(
            {
                "markets_total": int(windows["market_key"].nunique()),
                "markets_labelled": int(windows["winner_side"].isin(["YES", "NO"]).sum()),
                "markets_with_valid_quotes": int(group["market_key"].nunique()),
                "markets_traded": int(trade_group["market_key"].nunique()) if not trade_group.empty else 0,
                "trade_count": int(len(trade_group)),
                "notional_requested": float(group["notional_requested"].sum()),
                "notional_filled": float(trade_group["notional_filled"].sum()) if not trade_group.empty else 0.0,
                "fill_rate": float(trade_group["notional_filled"].sum() / group["notional_requested"].sum()) if group["notional_requested"].sum() else np.nan,
                "gross_cost": total_cost,
                "gross_payout": float(trade_group["gross_payout"].sum()) if not trade_group.empty else 0.0,
                "gross_pnl": float(trade_group["gross_pnl"].sum()) if not trade_group.empty else 0.0,
                "roi_on_filled_cost": float(trade_group["gross_pnl"].sum() / total_cost) if total_cost else np.nan,
                "win_rate": float(trade_group["win"].mean()) if not trade_group.empty else np.nan,
                "avg_entry_ask": float(trade_group["entry_ask"].mean()) if not trade_group.empty else np.nan,
                "avg_entry_age_sec": float(trade_group["entry_age_sec"].mean()) if not trade_group.empty else np.nan,
                "median_entry_age_sec": float(trade_group["entry_age_sec"].median()) if not trade_group.empty else np.nan,
                "capacity_shortfall_count": int(group["capacity_shortfall"].sum()),
                "skipped_no_label": int(skip_group["skip_reason"].eq("no_label").sum()) if "skip_reason" in skip_group else 0,
                "skipped_no_quote": int(skip_group["skip_reason"].isin(["no_quote", "no_eligible_entry"]).sum()) if "skip_reason" in skip_group else 0,
                "skipped_invalid_book": int(skip_group["skip_reason"].eq("invalid_book").sum()) if "skip_reason" in skip_group else 0,
                "skipped_no_fill": int(group["status"].eq("no_fill").sum()),
                "max_drawdown": max_drawdown(trade_group.sort_values("ts")["gross_pnl"]) if not trade_group.empty else 0.0,
            }
        )
        rows.append(row)
    return pd.DataFrame(rows)


def add_full_slice(market_results: pd.DataFrame, trades: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    m = market_results.copy()
    t = trades.copy()
    m_full = m.copy()
    t_full = t.copy()
    m_full["chronological_slice"] = "full"
    t_full["chronological_slice"] = "full"
    return pd.concat([m, m_full], ignore_index=True), pd.concat([t, t_full], ignore_index=True)


def write_readme(path: Path, args: argparse.Namespace, manifest: dict[str, Any]) -> None:
    path.write_text(
        "\n".join(
            [
                "Baseline capacity stress",
                "",
                "Offline research only. Uses compact recorder data only; raw recorder JSONL is not read.",
                "Labels come from market_windows.parquet winner_side, which is Chainlink-aligned.",
                "Baseline rule: buy YES or NO when the side ask is below the configured ask threshold.",
                "One first eligible entry per market per threshold is enforced by default.",
                "Fills walk preserved ask depth up to --top-n-levels. No infinite liquidity is assumed.",
                "",
                f"compact_root={args.compact_root}",
                f"stake_sizes={args.stake_sizes}",
                f"ask_thresholds={args.ask_thresholds}",
                f"top_n_levels={args.top_n_levels}",
                f"valid_topbook_only={args.valid_topbook_only}",
                f"first_entry_only={args.first_entry_only}",
                f"capacity_aware={args.capacity_aware}",
                "",
                f"markets_total={manifest.get('markets_total')}",
                f"trade_rows={manifest.get('trade_rows')}",
            ]
        )
        + "\n",
        encoding="utf-8",
    )


def run(args: argparse.Namespace) -> dict[str, Any]:
    output_dir = Path(args.output_root) / args.run_name
    if output_dir.exists():
        if not args.overwrite:
            raise FileExistsError(f"{output_dir} exists; pass --overwrite")
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    stakes = parse_csv_floats(args.stake_sizes)
    thresholds = parse_csv_floats(args.ask_thresholds)
    buckets = parse_age_buckets(args.entry_age_buckets)
    windows, ticks = load_compact(Path(args.compact_root))
    windows = filter_windows(windows, args.start_date, args.end_date, args.max_markets)
    ticks = ticks[ticks["market_key"].isin(set(windows["market_key"]))]
    unfiltered_prepared = prepare_ticks(ticks, windows, valid_topbook_only=False, entry_age_min=args.entry_age_min_sec, entry_age_max=args.entry_age_max_sec)
    prepared = prepare_ticks(ticks, windows, valid_topbook_only=args.valid_topbook_only, entry_age_min=args.entry_age_min_sec, entry_age_max=args.entry_age_max_sec)
    invalid_keys = set()
    if args.valid_topbook_only and "is_valid_topbook" in unfiltered_prepared.columns:
        labelled_invalid = unfiltered_prepared[unfiltered_prepared["winner_side"].isin(["YES", "NO"]) & (~unfiltered_prepared["is_valid_topbook"].astype(bool))]
        invalid_keys = set(labelled_invalid["market_key"].unique()) - set(prepared["market_key"].unique())
    entries, skipped = select_first_entries(prepared, windows, thresholds, args.first_entry_only, invalid_keys)
    trades, market_results = simulate_trades(entries, stakes, min(args.top_n_levels, 100), args.capacity_aware, buckets)
    skipped = skipped.merge(windows[["market_key", "market_start_ts"]], on="market_key", how="left")
    skipped["chronological_slice"] = skipped["market_start_ts"].map(chronological_slice)
    market_results_for_summary, trades_for_summary = add_full_slice(market_results, trades)
    skipped_for_summary = pd.concat([skipped, skipped.assign(chronological_slice="full")], ignore_index=True)
    summary_groups = ["chronological_slice", "ask_threshold", "stake_size"]
    summary = summarize(trades_for_summary, market_results_for_summary, skipped_for_summary, windows, summary_groups)
    summary.to_csv(output_dir / "stress_summary.csv", index=False)
    market_results["entry_date"] = pd.to_datetime(market_results["market_start_ts"], utc=True).dt.date.astype(str)
    trades["entry_date"] = pd.to_datetime(trades["market_start_ts"], utc=True).dt.date.astype(str) if not trades.empty else []
    summarize(trades, market_results, skipped, windows, ["entry_date", "ask_threshold", "stake_size"]).to_csv(output_dir / "stress_summary_by_date.csv", index=False)
    summarize(trades, market_results, skipped, windows, ["entry_age_bucket", "ask_threshold", "stake_size"]).to_csv(output_dir / "stress_summary_by_entry_age_bucket.csv", index=False)
    summarize(trades, market_results, skipped, windows, ["stake_size", "ask_threshold"]).to_csv(output_dir / "stress_summary_by_stake.csv", index=False)
    write_parquet_or_json(market_results, output_dir / "market_level_results.parquet")
    write_parquet_or_json(trades, output_dir / "trade_level_results.parquet")
    skipped.to_csv(output_dir / "skipped_markets.csv", index=False)
    manifest = {
        "compact_root": str(args.compact_root),
        "output_dir": str(output_dir),
        "run_name": args.run_name,
        "markets_total": int(windows["market_key"].nunique()),
        "markets_labelled": int(windows["winner_side"].isin(["YES", "NO"]).sum()),
        "book_tick_rows_loaded": int(len(ticks)),
        "prepared_tick_rows": int(len(prepared)),
        "candidate_entries": int(len(entries)),
        "trade_rows": int(len(trades)),
        "market_result_rows": int(len(market_results)),
        "stake_sizes": stakes,
        "ask_thresholds": thresholds,
        "top_n_levels": args.top_n_levels,
        "valid_topbook_only": args.valid_topbook_only,
        "first_entry_only": args.first_entry_only,
        "capacity_aware": args.capacity_aware,
    }
    (output_dir / "run_manifest.json").write_text(json.dumps(manifest, indent=2, default=str), encoding="utf-8")
    write_readme(output_dir / "README.txt", args, manifest)
    return manifest


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Baseline first-entry capacity-aware stress replay from compact BTC-5m recorder data.")
    parser.add_argument("--compact-root", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--run-name", required=True)
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    parser.add_argument("--stake-sizes", default="1,2,5,10,20,50")
    parser.add_argument("--ask-thresholds", default=",".join(f"{x:g}" for x in DEFAULT_THRESHOLDS))
    parser.add_argument("--top-n-levels", type=int, default=3)
    parser.add_argument("--valid-topbook-only", type=bool_arg, default=True)
    parser.add_argument("--first-entry-only", type=bool_arg, default=True)
    parser.add_argument("--capacity-aware", type=bool_arg, default=True)
    parser.add_argument("--entry-age-min-sec", type=float, default=0.0)
    parser.add_argument("--entry-age-max-sec", type=float, default=300.0)
    parser.add_argument("--entry-age-buckets", default=DEFAULT_AGE_BUCKETS)
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--max-markets", type=int)
    return parser


def main(argv: list[str] | None = None) -> int:
    manifest = run(build_parser().parse_args(argv))
    print(json.dumps(manifest, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
