#!/usr/bin/env python3
"""
run_btc5m_live_failure_attribution.py
--------------------------------------
Read-only post-mortem attribution for live BTC-5m canary trading losses.

Joins execution fills from the SQLite ledger with:
  - order_validation.jsonl  (model + stake inputs at the moment of the order)
  - execution_events.jsonl  (raw order + fill events from the execution journal)
  - compact_market_recorder parquet  (CLOB snapshots for market-context features)
  - market_resolution_state + outcome_lots (realised PnL)

Outputs (all written to --out-dir, default: artifacts/live_failure_attribution/):
  live_trades_enriched.parquet   full per-fill row set
  live_trades_enriched.csv       same, CSV for quick inspection
  summary.json                   aggregate stats
  README.txt                     column-level documentation
  bucket_<name>.csv              one file per attribution bucket (14 buckets)

SAFETY: No writes to the ledger database.  All SQLite connections are opened
read-only via URI mode (file:...?mode=ro).
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import textwrap
import warnings
from collections import defaultdict
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# ---------------------------------------------------------------------------
# Default paths (all relative to repo root)
# ---------------------------------------------------------------------------
DEFAULT_LEDGER = ROOT / "state" / "btc5m_live_ledger.db"
DEFAULT_DECISIONS_DIR = (
    ROOT / "artifacts" / "live_strategy_decisions" / "brownian_no_hmm_conservative_v1"
)
DEFAULT_EXECUTION_DIR = ROOT / "artifacts" / "btc5m_canary_execution"
DEFAULT_COMPACT_DIR = ROOT / "artifacts" / "compact_market_recorder"
DEFAULT_OUT_DIR = ROOT / "artifacts" / "live_failure_attribution"

STRATEGY_ID = "brownian_no_hmm_conservative_v1"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _die(msg: str) -> None:
    print(f"[FATAL] {msg}", file=sys.stderr)
    sys.exit(1)


def _warn(msg: str) -> None:
    print(f"[WARN]  {msg}", file=sys.stderr)


def _info(msg: str) -> None:
    print(f"[INFO]  {msg}")


def _open_ro(db_path: Path) -> sqlite3.Connection:
    uri = f"file:{db_path}?mode=ro"
    return sqlite3.connect(uri, uri=True)


def _utc_ts(value: Any) -> pd.Timestamp | float:
    """Parse ISO-8601 string → tz-aware UTC Timestamp, or NaT."""
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return pd.NaT
    try:
        ts = pd.Timestamp(value)
        if ts.tzinfo is None:
            ts = ts.tz_localize("UTC")
        else:
            ts = ts.tz_convert("UTC")
        return ts
    except Exception:
        return pd.NaT


# ---------------------------------------------------------------------------
# Step 1: load fills from SQLite
# ---------------------------------------------------------------------------

def load_fills(ledger: Path) -> pd.DataFrame:
    """Return live_fills joined to live_orders."""
    if not ledger.exists():
        _die(f"Ledger not found: {ledger}")
    conn = _open_ro(ledger)
    try:
        # Probe available columns (live_orders uses terminal_status not status)
        cur = conn.execute("pragma table_info(live_orders)")
        order_cols = {row[1] for row in cur.fetchall()}
        status_col = "terminal_status" if "terminal_status" in order_cols else "status"

        fills = pd.read_sql_query(
            """
            SELECT
                f.id             AS fill_id,
                f.order_id,
                f.market_id,
                f.condition_id,
                f.token_id,
                f.side,
                f.fill_qty_shares,
                f.avg_fill_price,
                f.spent_pusd,
                f.fill_ts,
                f.trade_id,
                f.source_key,
                o.idempotency_key,
                o.client_order_id,
                o.order_type,
                o.limit_price,
                o.intended_notional_usd
            FROM live_fills f
            LEFT JOIN live_orders o ON f.order_id = o.order_id
            """,
            conn,
        )
    finally:
        conn.close()

    if fills.empty:
        _die("No fills found in live_fills – nothing to analyse.")

    # Deduplicate on source_key (should already be UNIQUE, but enforce)
    before = len(fills)
    fills = fills.drop_duplicates(subset=["source_key"])
    if len(fills) < before:
        _warn(f"Dropped {before - len(fills)} duplicate fill rows (source_key collision)")

    fills["fill_ts"] = fills["fill_ts"].apply(_utc_ts)
    fills["cost_pusd"] = fills["spent_pusd"].where(fills["spent_pusd"].notna(), fills["avg_fill_price"] * fills["fill_qty_shares"])

    _info(f"Loaded {len(fills)} fills from ledger")
    return fills.reset_index(drop=True)


# ---------------------------------------------------------------------------
# Step 2: load outcome_lots + resolution from SQLite
# ---------------------------------------------------------------------------

def load_outcomes(ledger: Path) -> pd.DataFrame:
    conn = _open_ro(ledger)
    try:
        lots = pd.read_sql_query(
            """
            SELECT
                ol.id             AS lot_id,
                ol.market_id,
                ol.condition_id,
                ol.token_id,
                ol.side,
                ol.acquired_qty,
                ol.remaining_qty,
                ol.avg_cost,
                ol.status        AS lot_status,
                ol.source_order_id,
                ol.source_fill_id,
                ol.created_ts,
                ol.updated_ts,
                mrs.resolved,
                mrs.winning_side,
                mrs.payout_vector_json,
                mrs.resolution_source,
                mrs.resolved_ts
            FROM outcome_lots ol
            LEFT JOIN market_resolution_state mrs
                ON ol.condition_id = mrs.condition_id
            """,
            conn,
        )
    finally:
        conn.close()

    _info(f"Loaded {len(lots)} outcome lots")
    return lots.reset_index(drop=True)


# ---------------------------------------------------------------------------
# Step 3: load order_validation.jsonl (nearest-prior only)
# ---------------------------------------------------------------------------

def load_order_validation(decisions_dir: Path) -> pd.DataFrame:
    path = decisions_dir / "order_validation.jsonl"
    if not path.exists():
        _warn(f"order_validation.jsonl not found at {path} – validation context will be missing")
        return pd.DataFrame()

    rows = []
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except Exception:
                pass

    if not rows:
        _warn("order_validation.jsonl was empty")
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    if "timestamp" in df.columns:
        df["val_ts"] = df["timestamp"].apply(_utc_ts)
    else:
        df["val_ts"] = pd.NaT

    _info(f"Loaded {len(df)} order_validation rows")
    return df.reset_index(drop=True)


# ---------------------------------------------------------------------------
# Step 4: load execution_events.jsonl (order_filled only)
# ---------------------------------------------------------------------------

def load_execution_events(execution_dir: Path) -> pd.DataFrame:
    if not execution_dir.exists():
        _warn(f"Execution dir not found: {execution_dir}")
        return pd.DataFrame()

    rows = []
    for path in sorted(execution_dir.rglob("execution_events.jsonl")):
        with path.open("r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    ev = json.loads(line)
                except Exception:
                    continue
                if ev.get("event_type") == "order_filled":
                    rows.append(ev)

    if not rows:
        _warn("No order_filled events found in execution journals")
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    df["exec_fill_ts"] = df.get("execution_ts", pd.Series(dtype="object")).apply(_utc_ts)
    df["decision_ts_exec"] = df.get("decision_ts", pd.Series(dtype="object")).apply(_utc_ts)

    _info(f"Loaded {len(df)} order_filled execution events")
    return df.reset_index(drop=True)


# ---------------------------------------------------------------------------
# Step 5: discover and load compact_market_recorder parquet
# ---------------------------------------------------------------------------

def _find_latest_compact_dir(compact_root: Path) -> Path | None:
    """Return the most recent dated compact_market_recorder dataset directory."""
    candidates = [
        p for p in compact_root.iterdir()
        if p.is_dir() and p.name[0].isdigit()
    ]
    if not candidates:
        return None
    return sorted(candidates)[-1]


def load_compact_quotes(compact_root: Path) -> pd.DataFrame:
    """Load all quotes.parquet (or quotes/*.parquet) from the latest compact dataset."""
    dataset_dir = _find_latest_compact_dir(compact_root)
    if dataset_dir is None:
        _warn(f"No dated compact_market_recorder dataset found under {compact_root}")
        return pd.DataFrame()

    parquet_files = sorted(dataset_dir.rglob("quotes*.parquet"))
    if not parquet_files:
        _warn(f"No quotes parquet files found under {dataset_dir}")
        return pd.DataFrame()

    parts = []
    for pf in parquet_files:
        try:
            parts.append(pd.read_parquet(pf))
        except Exception as exc:
            _warn(f"Could not read {pf}: {exc}")

    if not parts:
        return pd.DataFrame()

    df = pd.concat(parts, ignore_index=True)
    if "ts" in df.columns:
        df["ts"] = df["ts"].apply(lambda v: _utc_ts(v) if not isinstance(v, pd.Timestamp) else v)
        # Ensure tz-aware
        if df["ts"].dtype.tz is None:
            try:
                df["ts"] = df["ts"].dt.tz_localize("UTC")
            except Exception:
                pass

    _info(f"Loaded {len(df)} compact quote rows from {dataset_dir.name}")
    return df.reset_index(drop=True)


def load_compact_windows(compact_root: Path) -> pd.DataFrame:
    """Load market_windows.parquet for slug/condition_id/market metadata."""
    dataset_dir = _find_latest_compact_dir(compact_root)
    if dataset_dir is None:
        return pd.DataFrame()

    parquet_files = sorted(dataset_dir.rglob("windows*.parquet")) + sorted(dataset_dir.rglob("market_windows*.parquet"))
    if not parquet_files:
        return pd.DataFrame()

    parts = []
    for pf in parquet_files:
        try:
            parts.append(pd.read_parquet(pf))
        except Exception as exc:
            _warn(f"Could not read {pf}: {exc}")

    if not parts:
        return pd.DataFrame()

    df = pd.concat(parts, ignore_index=True)
    _info(f"Loaded {len(df)} compact window rows")
    return df.reset_index(drop=True)


# ---------------------------------------------------------------------------
# Step 6: compute realised PnL per fill
# ---------------------------------------------------------------------------

def compute_pnl(fills: pd.DataFrame, lots: pd.DataFrame) -> pd.DataFrame:
    """
    Attach PnL columns to each fill.

    Logic:
    - For each fill, find matching outcome_lot via source_fill_id (or source_order_id).
    - pnl_gross = acquired_qty * payout_per_share - cost_pusd
      where payout_per_share = 1.0 for winning side, 0.0 for loser.
    - For unredeemed winning lots: pnl_gross uses 1.0 payout.
    - For `resolved_loss` lots: pnl_gross = -cost_pusd.
    - For `redeemed` lots: pnl_gross = redeemed_pusd - cost_pusd (best estimate).
    """
    if lots.empty:
        fills["lot_status"] = None
        fills["pnl_gross"] = np.nan
        fills["outcome_side_won"] = None
        return fills

    # Parse payout vector
    def winning_payout(row: Any) -> float | None:
        if row.get("lot_status") in ("resolved_loss",):
            return 0.0
        if row.get("lot_status") in ("redeemed",):
            return 1.0  # simplification – payout is $1 per winning share
        if row.get("winning_side") and row.get("side"):
            return 1.0 if row["winning_side"] == row["side"] else 0.0
        return None

    # Build lot lookup by source_fill_id, then source_order_id
    lot_by_fill = lots.set_index("source_fill_id").to_dict("index") if "source_fill_id" in lots.columns else {}
    lot_by_order = lots.set_index("source_order_id").to_dict("index") if "source_order_id" in lots.columns else {}

    pnl_rows = []
    for _, row in fills.iterrows():
        lot = lot_by_fill.get(row.get("fill_id")) or lot_by_order.get(row.get("order_id"))
        if lot is None:
            pnl_rows.append({"lot_status": None, "pnl_gross": np.nan, "outcome_side_won": None, "winning_side": None})
            continue
        payout = winning_payout({**lot, "side": lot.get("side") or row.get("side")})
        cost = row.get("cost_pusd") or (row.get("avg_fill_price", 0) * row.get("fill_qty_shares", 0))
        qty = float(lot.get("acquired_qty") or row.get("fill_qty_shares") or 0)
        pnl = (qty * (payout or 0.0)) - float(cost or 0.0) if payout is not None else np.nan
        pnl_rows.append({
            "lot_status": lot.get("lot_status") or lot.get("status"),
            "pnl_gross": pnl,
            "outcome_side_won": lot.get("winning_side"),
            "winning_side": lot.get("winning_side"),
        })

    pnl_df = pd.DataFrame(pnl_rows)
    return pd.concat([fills.reset_index(drop=True), pnl_df.reset_index(drop=True)], axis=1)


# ---------------------------------------------------------------------------
# Step 7: asof-join validation context (previous-only)
# ---------------------------------------------------------------------------

def attach_validation_context(fills: pd.DataFrame, validation: pd.DataFrame) -> pd.DataFrame:
    """
    For each fill, attach the most-recent order_validation row that is
    strictly *before* the fill timestamp, matched on (condition_id, token_id, side).
    """
    if validation.empty or "val_ts" not in validation.columns:
        return fills

    key_cols = ["condition_id", "side"]
    # Also try token_id if available
    if "token_id" in validation.columns and "token_id" in fills.columns:
        key_cols = ["condition_id", "token_id", "side"]

    validation_sorted = validation.sort_values("val_ts").reset_index(drop=True)
    fills_sorted = fills.sort_values("fill_ts").reset_index(drop=True)

    # Prefix ALL validation columns (including key columns) with "val_" to
    # prevent merge_asof from silently renaming fills columns to _x/_y.
    val_rename = {
        c: f"val_{c}"
        for c in validation_sorted.columns
        if c != "val_ts"  # val_ts was already set above
    }
    val_rename["val_ts"] = "val_ts"
    validation_sorted = validation_sorted.rename(columns=val_rename)

    # Key columns as they appear in the renamed validation frame
    val_key_cols = [f"val_{c}" for c in key_cols]

    merged_parts = []
    for keys, group_fills in fills_sorted.groupby(key_cols, dropna=False):
        if not isinstance(keys, tuple):
            keys = (keys,)
        mask = validation_sorted.copy()
        for vcol, val in zip(val_key_cols, keys):
            if vcol in mask.columns:
                mask = mask[mask[vcol] == val]

        if mask.empty:
            merged_parts.append(group_fills)
            continue

        # merge_asof requires sorted, tz-compatible timestamps
        try:
            merged = pd.merge_asof(
                group_fills.sort_values("fill_ts"),
                mask.sort_values("val_ts"),
                left_on="fill_ts",
                right_on="val_ts",
                direction="backward",
                tolerance=pd.Timedelta("10 minutes"),
            )
        except Exception:
            merged_parts.append(group_fills)
            continue
        merged_parts.append(merged)

    if not merged_parts:
        return fills

    out = pd.concat(merged_parts, ignore_index=True)
    # Guard: drop any future-context rows (val_ts > fill_ts)
    if "val_ts" in out.columns:
        future_mask = out["val_ts"] > out["fill_ts"]
        if future_mask.any():
            _die(f"Future context detected in validation join: {future_mask.sum()} rows would violate temporal integrity.")
    return out


# ---------------------------------------------------------------------------
# Step 8: attach compact market context
# ---------------------------------------------------------------------------

def attach_market_context(fills: pd.DataFrame, compact_quotes: pd.DataFrame) -> pd.DataFrame:
    """
    For each fill, attach the most-recent compact quote snapshot that is
    strictly *before* the fill timestamp, matched on condition_id (via
    market_key / market_id).

    Because compact quotes are keyed by market_key (integer) not condition_id,
    we first try to map via the condition_id / market_id columns present in fills.
    """
    if compact_quotes.empty or "ts" not in compact_quotes.columns:
        return fills

    # We may not have a direct condition_id join in compact quotes.
    # Try joining on market_id if it exists.
    join_col_q = None
    join_col_f = None
    for qcol, fcol in [("market_id", "market_id"), ("condition_id", "condition_id")]:
        if qcol in compact_quotes.columns and fcol in fills.columns:
            join_col_q, join_col_f = qcol, fcol
            break

    if join_col_q is None:
        _warn("Cannot join compact quotes to fills – no shared key column found")
        return fills

    # Rename compact cols to avoid collision
    ctx_cols = ["ts", join_col_q, "side", "mid", "spread", "ask_px_1", "bid_px_1",
                "is_valid_topbook", "market_age_sec", "seconds_to_end"]
    ctx_cols = [c for c in ctx_cols if c in compact_quotes.columns]
    compact_sub = compact_quotes[ctx_cols].copy()
    compact_sub = compact_sub.rename(columns={c: f"ctx_{c}" for c in ctx_cols if c != join_col_q and c != "ts"})
    compact_sub = compact_sub.rename(columns={"ts": "ctx_ts"})

    merged_parts = []
    for (market_id, side), group_fills in fills.groupby([join_col_f, "side"], dropna=False):
        q_group = compact_sub[
            (compact_sub[join_col_q] == market_id) &
            (compact_sub.get(f"ctx_side", compact_sub.get("ctx_side", pd.Series(dtype="object"))) == side)
        ] if "ctx_side" in compact_sub.columns else compact_sub[compact_sub[join_col_q] == market_id]

        if q_group.empty:
            merged_parts.append(group_fills)
            continue

        try:
            merged = pd.merge_asof(
                group_fills.sort_values("fill_ts"),
                q_group.sort_values("ctx_ts"),
                left_on="fill_ts",
                right_on="ctx_ts",
                by=join_col_f if join_col_f == join_col_q else None,
                direction="backward",
                tolerance=pd.Timedelta("5 minutes"),
            )
        except Exception:
            merged_parts.append(group_fills)
            continue

        # Temporal guard: ctx_ts must be ≤ fill_ts
        if "ctx_ts" in merged.columns:
            future = merged["ctx_ts"] > merged["fill_ts"]
            if future.any():
                _die("Future market context detected – asof join violated temporal integrity.")

        merged_parts.append(merged)

    if not merged_parts:
        return fills

    return pd.concat(merged_parts, ignore_index=True)


# ---------------------------------------------------------------------------
# Step 9: compute attribution features
# ---------------------------------------------------------------------------

def compute_features(df: pd.DataFrame) -> pd.DataFrame:
    """Add derived columns used for bucketing and attribution."""

    # -- PnL metrics --
    df["roi"] = np.where(
        df["cost_pusd"].notna() & (df["cost_pusd"] > 0),
        df["pnl_gross"] / df["cost_pusd"],
        np.nan,
    )

    # -- Model-market price gap (model p vs fill price) --
    model_p_col = next(
        (c for c in ["val_model_probability", "val_model_p", "model_probability"]
         if c in df.columns),
        None,
    )
    fill_price_col = "avg_fill_price"
    if model_p_col and fill_price_col in df.columns:
        df["model_market_gap"] = df[model_p_col] - df[fill_price_col]
    else:
        df["model_market_gap"] = np.nan

    # -- Edge at fill (model prob minus fill price, adjusted for side) --
    if model_p_col and fill_price_col in df.columns:
        # For YES bets: edge = p_yes - ask_price
        # For NO bets:  edge = (1-p_yes) - ask_price
        p_yes = df[model_p_col].copy()
        is_no = df["side"].str.upper() == "NO"
        p_side = np.where(is_no, 1.0 - p_yes, p_yes)
        df["edge_at_fill"] = p_side - df[fill_price_col]
    else:
        df["edge_at_fill"] = np.nan

    # -- Distance from fair (mid) at time of fill --
    if "ctx_mid" in df.columns:
        df["fill_vs_mid"] = df[fill_price_col] - df["ctx_mid"] if fill_price_col in df.columns else np.nan
    else:
        df["fill_vs_mid"] = np.nan

    # -- Market age at fill --
    if "market_age_sec" in df.columns:
        df["market_age_at_fill_sec"] = df["market_age_sec"]
    elif "val_market_age_seconds" in df.columns:
        df["market_age_at_fill_sec"] = df["val_market_age_seconds"]
    else:
        df["market_age_at_fill_sec"] = np.nan

    # -- Seconds to expiry at fill --
    if "val_seconds_to_expiry" in df.columns:
        df["seconds_to_expiry_at_fill"] = df["val_seconds_to_expiry"]
    elif "ctx_seconds_to_end" in df.columns:
        df["seconds_to_expiry_at_fill"] = df["ctx_seconds_to_end"]
    else:
        df["seconds_to_expiry_at_fill"] = np.nan

    # -- Outcome binary --
    if "winning_side" in df.columns and "side" in df.columns:
        df["won"] = df["winning_side"].str.upper() == df["side"].str.upper()
    else:
        df["won"] = np.nan

    # -- Resolved flag --
    if "lot_status" in df.columns:
        df["is_resolved"] = df["lot_status"].isin(["resolved_loss", "redeemed"])
    else:
        df["is_resolved"] = False

    return df


# ---------------------------------------------------------------------------
# Step 10: attribution buckets
# ---------------------------------------------------------------------------

BUCKETS: list[tuple[str, str]] = [
    # (bucket_name, pandas_query_or_description)
    ("all_fills",           "index == index"),          # everything
    ("winners",             "won == True"),
    ("losers",              "won == False"),
    ("unresolved",          "is_resolved == False"),
    ("resolved",            "is_resolved == True"),
    ("high_edge_losers",    "edge_at_fill >= 0.05 and won == False"),
    ("low_edge_losers",     "edge_at_fill < 0.05  and won == False"),
    ("early_market_losers", "market_age_at_fill_sec < 90 and won == False"),
    ("late_market_losers",  "market_age_at_fill_sec >= 90 and won == False"),
    ("fill_above_mid",      "fill_vs_mid > 0.01"),
    ("fill_below_mid",      "fill_vs_mid < -0.01"),
    ("model_p_above_55",    "val_model_probability >= 0.55 if 'val_model_probability' in @df else False"),
    ("no_side_trades",      "side == 'NO'"),
    ("yes_side_trades",     "side == 'YES'"),
]


def build_buckets(df: pd.DataFrame, out_dir: Path) -> dict[str, Any]:
    """Write one CSV per bucket; return summary stats per bucket."""
    summary: dict[str, Any] = {}
    for bucket_name, query in BUCKETS:
        try:
            if query == "index == index":
                subset = df.copy()
            else:
                # Some queries reference columns that may not exist
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore")
                    try:
                        subset = df.query(query, local_dict={"df": df})
                    except Exception:
                        subset = pd.DataFrame(columns=df.columns)
        except Exception:
            subset = pd.DataFrame(columns=df.columns)

        n = len(subset)
        pnl = float(subset["pnl_gross"].sum()) if "pnl_gross" in subset else 0.0
        cost = float(subset["cost_pusd"].sum()) if "cost_pusd" in subset else 0.0
        roi = pnl / cost if cost > 0 else np.nan
        win_rate = float((subset["won"] == True).mean()) if "won" in subset and n > 0 else np.nan  # noqa: E712

        summary[bucket_name] = {
            "n_fills": n,
            "total_cost_pusd": round(cost, 4),
            "total_pnl_gross": round(pnl, 4),
            "roi": round(roi, 4) if not np.isnan(roi) else None,
            "win_rate": round(win_rate, 4) if not np.isnan(win_rate) else None,
        }

        csv_path = out_dir / f"bucket_{bucket_name}.csv"
        subset.to_csv(csv_path, index=False)

    return summary


# ---------------------------------------------------------------------------
# Step 11: summary JSON
# ---------------------------------------------------------------------------

def build_summary(df: pd.DataFrame, bucket_stats: dict[str, Any]) -> dict[str, Any]:
    n = len(df)
    resolved_mask = df["is_resolved"] if "is_resolved" in df else pd.Series(False, index=df.index)
    pnl_resolved = float(df.loc[resolved_mask, "pnl_gross"].sum()) if "pnl_gross" in df else 0.0
    cost_total = float(df["cost_pusd"].sum()) if "cost_pusd" in df else 0.0
    roi_total = pnl_resolved / cost_total if cost_total > 0 else None

    fill_prices = df["avg_fill_price"].dropna()

    summary: dict[str, Any] = {
        "n_fills_total": n,
        "n_resolved": int(resolved_mask.sum()),
        "n_unresolved": int((~resolved_mask).sum()),
        "total_cost_pusd": round(cost_total, 4),
        "pnl_gross_resolved": round(pnl_resolved, 4),
        "roi_resolved_fills": round(roi_total, 4) if roi_total is not None else None,
        "avg_fill_price": round(float(fill_prices.mean()), 4) if len(fill_prices) else None,
        "fill_price_p5": round(float(fill_prices.quantile(0.05)), 4) if len(fill_prices) else None,
        "fill_price_p95": round(float(fill_prices.quantile(0.95)), 4) if len(fill_prices) else None,
        "fill_dates": sorted(
            df["fill_ts"].dropna().dt.strftime("%Y-%m-%d").unique().tolist()
        ) if "fill_ts" in df else [],
        "buckets": bucket_stats,
    }

    # Add edge stats if available
    if "edge_at_fill" in df:
        edge = df["edge_at_fill"].dropna()
        summary["avg_edge_at_fill"] = round(float(edge.mean()), 4) if len(edge) else None
        summary["pct_fills_negative_edge"] = round(float((edge < 0).mean()), 4) if len(edge) else None

    if "model_market_gap" in df:
        mmg = df["model_market_gap"].dropna()
        summary["avg_model_market_gap"] = round(float(mmg.mean()), 4) if len(mmg) else None

    return summary


# ---------------------------------------------------------------------------
# README
# ---------------------------------------------------------------------------

README_TEXT = textwrap.dedent("""
# live_failure_attribution output

Generated by scripts/run_btc5m_live_failure_attribution.py (read-only).
Strategy: brownian_no_hmm_conservative_v1

## Files
- live_trades_enriched.parquet / .csv   Full enriched fill table
- summary.json                          Aggregate stats + bucket breakdown
- bucket_<name>.csv                     Per-attribution-bucket subsets (14 files)

## Key columns in live_trades_enriched

fill_id               INTEGER  Primary key from live_fills.id
order_id              TEXT     Polymarket order ID (0x…)
condition_id          TEXT     Polymarket condition ID (0x…)
token_id              TEXT     Polymarket token asset ID
side                  TEXT     YES or NO
fill_qty_shares       REAL     Shares acquired
avg_fill_price        REAL     Fill price (0-1 scale)
spent_pusd            REAL     USD equivalent spent (from ledger)
fill_ts               TIMESTAMP Fill execution timestamp (UTC)
cost_pusd             REAL     Best estimate of cost (spent_pusd or price*qty)
lot_status            TEXT     outcome_lot.status: resolved_loss | redeemed | open | …
pnl_gross             REAL     Gross PnL in USD: shares*payout - cost
roi                   REAL     pnl_gross / cost_pusd
won                   BOOL     True if outcome matched our side
is_resolved           BOOL     True if lot_status in {resolved_loss, redeemed}
winning_side          TEXT     YES or NO (from market_resolution_state)
model_market_gap      REAL     val_model_probability - avg_fill_price
edge_at_fill          REAL     Model edge in the direction of our bet
fill_vs_mid           REAL     avg_fill_price - compact_mid at fill time
market_age_at_fill_sec REAL   Market age in seconds at decision time
seconds_to_expiry_at_fill REAL Seconds remaining at decision time

val_* columns         All fields from order_validation.jsonl at time of fill
ctx_* columns         Compact market recorder snapshot nearest to fill time

## Attribution Buckets
all_fills, winners, losers, unresolved, resolved,
high_edge_losers, low_edge_losers,
early_market_losers, late_market_losers,
fill_above_mid, fill_below_mid,
model_p_above_55, no_side_trades, yes_side_trades

## Temporal integrity
- Validation context: asof join, backward only, ≤10 minute tolerance
- Market context:    asof join, backward only, ≤5 minute tolerance
- Any future context use triggers a FATAL error and aborts the script.
""").strip()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Post-mortem failure attribution for BTC-5m live trading.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--ledger", type=Path, default=DEFAULT_LEDGER,
                   help="Path to btc5m_live_ledger.db")
    p.add_argument("--decisions-dir", type=Path, default=DEFAULT_DECISIONS_DIR,
                   help="Directory containing order_validation.jsonl and decision_state.jsonl")
    p.add_argument("--execution-dir", type=Path, default=DEFAULT_EXECUTION_DIR,
                   help="Root dir for per-date execution_events.jsonl files")
    p.add_argument("--compact-dir", type=Path, default=DEFAULT_COMPACT_DIR,
                   help="Root dir for compact_market_recorder datasets")
    p.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR,
                   help="Output directory (will be created)")
    p.add_argument("--skip-compact", action="store_true",
                   help="Skip loading compact market recorder context (faster, less context)")
    p.add_argument("--no-parquet", action="store_true",
                   help="Skip writing .parquet output (write CSV only)")
    return p.parse_args()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    args = parse_args()

    out_dir: Path = args.out_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    # --- Load data sources ---
    _info("=== Loading fills from ledger ===")
    fills = load_fills(args.ledger)

    _info("=== Loading outcome lots + resolution ===")
    lots = load_outcomes(args.ledger)

    _info("=== Loading order validation context ===")
    validation = load_order_validation(args.decisions_dir)

    _info("=== Loading execution events ===")
    exec_events = load_execution_events(args.execution_dir)

    # --- Merge execution event fields onto fills (by order_id) ---
    if not exec_events.empty and "order_id" in exec_events.columns:
        exec_cols = ["order_id", "market_age_sec", "selected_edge",
                     "selected_ask", "selected_side", "decision_ts_exec",
                     "exec_fill_ts", "brownian_validation_id"]
        exec_cols = [c for c in exec_cols if c in exec_events.columns]
        exec_deduped = exec_events[exec_cols].drop_duplicates(subset=["order_id"])
        fills = fills.merge(exec_deduped, on="order_id", how="left", suffixes=("", "_exec"))
        _info(f"Merged {exec_cols} from execution events onto fills")

    # --- Compute PnL ---
    _info("=== Computing PnL ===")
    fills = compute_pnl(fills, lots)

    # --- Attach validation context ---
    _info("=== Attaching order validation context (asof, backward only) ===")
    fills = attach_validation_context(fills, validation)

    # --- Attach compact market context ---
    if not args.skip_compact:
        _info("=== Loading compact market recorder quotes ===")
        compact_quotes = load_compact_quotes(args.compact_dir)
        if not compact_quotes.empty:
            _info("=== Attaching compact market context (asof, backward only) ===")
            fills = attach_market_context(fills, compact_quotes)
        else:
            _warn("No compact quotes loaded – skipping market context attachment")
    else:
        _info("Skipping compact market recorder (--skip-compact)")

    # --- Compute attribution features ---
    _info("=== Computing attribution features ===")
    fills = compute_features(fills)

    # --- Sanity checks ---
    if len(fills) == 0:
        _die("Enriched fill table is empty after all joins.")

    dup_fills = fills.duplicated(subset=["fill_id"]) if "fill_id" in fills.columns else fills.duplicated(subset=["source_key"])
    if dup_fills.any():
        _die(f"Duplicate fills detected after enrichment ({dup_fills.sum()} rows) – aborting.")

    _info(f"Final enriched table: {len(fills)} rows, {len(fills.columns)} columns")

    # --- Write outputs ---
    csv_path = out_dir / "live_trades_enriched.csv"
    fills.to_csv(csv_path, index=False)
    _info(f"Written: {csv_path}")

    if not args.no_parquet:
        try:
            pq_path = out_dir / "live_trades_enriched.parquet"
            fills.to_parquet(pq_path, index=False)
            _info(f"Written: {pq_path}")
        except Exception as exc:
            _warn(f"Could not write parquet: {exc}")

    _info("=== Building attribution buckets ===")
    bucket_stats = build_buckets(fills, out_dir)

    _info("=== Building summary ===")
    summary = build_summary(fills, bucket_stats)

    summary_path = out_dir / "summary.json"
    with summary_path.open("w", encoding="utf-8") as fh:
        json.dump(summary, fh, indent=2, default=str)
    _info(f"Written: {summary_path}")

    readme_path = out_dir / "README.txt"
    readme_path.write_text(README_TEXT + "\n")
    _info(f"Written: {readme_path}")

    # Print quick top-line to stdout
    print("\n=== TOP-LINE SUMMARY ===")
    print(f"  Fills analysed       : {summary['n_fills_total']}")
    print(f"  Resolved             : {summary['n_resolved']}")
    print(f"  Total cost (pUSD)    : {summary['total_cost_pusd']:.4f}")
    print(f"  Gross PnL (resolved) : {summary['pnl_gross_resolved']:.4f}")
    if summary["roi_resolved_fills"] is not None:
        print(f"  ROI (resolved)       : {summary['roi_resolved_fills']:.2%}")
    if summary.get("avg_edge_at_fill") is not None:
        print(f"  Avg edge at fill     : {summary['avg_edge_at_fill']:.4f}")
    if summary.get("pct_fills_negative_edge") is not None:
        print(f"  % fills w/ neg edge  : {summary['pct_fills_negative_edge']:.1%}")
    print(f"\nOutputs written to: {out_dir}")


if __name__ == "__main__":
    main()
