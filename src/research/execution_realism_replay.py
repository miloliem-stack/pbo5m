from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


def parse_csv_floats(value: str) -> list[float]:
    return [float(item.strip()) for item in str(value).split(",") if item.strip()]


def parse_csv_strings(value: str) -> list[str]:
    return [item.strip() for item in str(value).split(",") if item.strip()]


def utc_ts(value: Any) -> pd.Timestamp | pd.NaT:
    return pd.to_datetime(value, utc=True, errors="coerce") if value not in (None, "") else pd.NaT


def number(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        out = float(value)
    except Exception:
        return None
    return out if np.isfinite(out) else None


def market_key_from_start(value: Any) -> str | None:
    ts = utc_ts(value)
    if pd.isna(ts):
        return None
    return ts.strftime("%Y-%m-%dT%H:%M:%SZ")


def margin_band(abs_margin: float, bands: list[float]) -> str:
    if pd.isna(abs_margin):
        return "missing"
    prev = 0.0
    for band in bands:
        if abs_margin <= band:
            return f"{prev:g}_{band:g}"
        prev = band
    return f"gt_{bands[-1]:g}"


def levels_from_book(book: dict[str, Any] | None, side: str) -> list[dict[str, float]]:
    if not isinstance(book, dict):
        return []
    rows = []
    for level in book.get(side) or []:
        if isinstance(level, dict):
            price = number(level.get("price") or level.get("p"))
            size = number(level.get("size") or level.get("quantity") or level.get("qty") or level.get("q"))
        elif isinstance(level, (list, tuple)) and len(level) >= 2:
            price = number(level[0])
            size = number(level[1])
        else:
            price, size = None, None
        if price is not None and size is not None and size > 0:
            rows.append({"price": price, "size": size})
    rows.sort(key=lambda row: row["price"], reverse=(side == "bids"))
    return rows


def _book_from_raw(raw: Any, side: str) -> dict[str, Any] | None:
    if not isinstance(raw, dict):
        return None
    side_raw = raw.get(f"{side.lower()}_raw")
    if not isinstance(side_raw, dict):
        return None
    book = side_raw.get("book")
    if not isinstance(book, dict):
        return None
    nested = book.get("book")
    return nested if isinstance(nested, dict) else book


def normalize_quote_record_to_books(row: dict[str, Any]) -> list[dict[str, Any]]:
    ts = utc_ts(row.get("ts") or row.get("timestamp") or row.get("quote_ts"))
    start = utc_ts(row.get("market_start_time") or row.get("market_start_ts"))
    end = utc_ts(row.get("market_end_time") or row.get("market_end_ts"))
    market_key = market_key_from_start(start)
    raw = row.get("raw_payload_fragment")
    out = []
    for side in ("YES", "NO"):
        book = _book_from_raw(raw, side)
        asks = levels_from_book(book, "asks")
        bids = levels_from_book(book, "bids")
        if book is None:
            status = "missing_side"
        elif asks or bids:
            status = "ok_full_depth"
        else:
            status = "missing_depth"
        out.append(
            {
                "market_key": market_key,
                "market_slug": row.get("slug") or row.get("market_slug"),
                "condition_id": row.get("condition_id"),
                "timestamp": ts,
                "market_start_ts": start,
                "market_end_ts": end,
                "asset_side": side,
                "asks": asks,
                "bids": bids,
                "best_ask": asks[0]["price"] if asks else np.nan,
                "best_bid": bids[0]["price"] if bids else np.nan,
                "source_file": None,
                "raw_source_type": row.get("source"),
                "book_parse_status": status,
                "execution_depth_mode": "full_depth" if asks else "missing_depth",
            }
        )
    return out


def discover_quote_files(path: Path) -> list[Path]:
    if path.is_file():
        return [path]
    return sorted(path.rglob("market_quotes.jsonl"))


def quote_file_for_timestamp(root: Path, ts: pd.Timestamp) -> Path:
    ts = pd.Timestamp(ts).tz_convert("UTC")
    return root / ts.strftime("%Y-%m-%d") / ts.strftime("%H") / "market_quotes.jsonl"


def quote_files_for_targets(root: Path, target_times: pd.Series, include_next_hour: bool = True) -> list[Path]:
    files: set[Path] = set()
    for value in pd.to_datetime(target_times, utc=True, errors="coerce").dropna():
        ts = pd.Timestamp(value)
        files.add(quote_file_for_timestamp(root, ts))
        if include_next_hour:
            files.add(quote_file_for_timestamp(root, ts + pd.Timedelta(hours=1)))
    return sorted(path for path in files if path.exists())


def load_books_from_files(files: list[Path], selected_market_keys: set[str] | None = None) -> tuple[pd.DataFrame, dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    loaded = 0
    errors = 0
    for file in files:
        with file.open("r", encoding="utf-8") as handle:
            for line in handle:
                loaded += 1
                try:
                    payload = json.loads(line)
                except Exception:
                    errors += 1
                    continue
                if not isinstance(payload, dict):
                    errors += 1
                    continue
                for row in normalize_quote_record_to_books(payload):
                    row["source_file"] = str(file)
                    if selected_market_keys is None or row["market_key"] in selected_market_keys:
                        rows.append(row)
    frame = pd.DataFrame(rows)
    if not frame.empty:
        frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True, errors="coerce")
        frame = frame.dropna(subset=["timestamp", "market_key", "asset_side"]).sort_values(["market_key", "asset_side", "timestamp"]).reset_index(drop=True)
    diagnostics = {
        "quote_files_read": len(files),
        "quote_rows_loaded": loaded,
        "quote_json_errors": errors,
        "book_rows": int(len(frame)),
        "book_parse_status_counts": frame["book_parse_status"].value_counts().to_dict() if not frame.empty else {},
        "execution_depth_mode_counts": frame["execution_depth_mode"].value_counts().to_dict() if not frame.empty else {},
    }
    return frame, diagnostics


def load_books(path: Path, selected_market_keys: set[str] | None = None) -> tuple[pd.DataFrame, dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    loaded = 0
    errors = 0
    for file in discover_quote_files(path):
        with file.open("r", encoding="utf-8") as handle:
            for line in handle:
                loaded += 1
                try:
                    payload = json.loads(line)
                except Exception:
                    errors += 1
                    continue
                if not isinstance(payload, dict):
                    errors += 1
                    continue
                normalized = normalize_quote_record_to_books(payload)
                for row in normalized:
                    row["source_file"] = str(file)
                    if selected_market_keys is None or row["market_key"] in selected_market_keys:
                        rows.append(row)
    frame = pd.DataFrame(rows)
    if not frame.empty:
        frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True, errors="coerce")
        frame = frame.dropna(subset=["timestamp", "market_key", "asset_side"]).sort_values(["market_key", "asset_side", "timestamp"]).reset_index(drop=True)
    diagnostics = {
        "quote_rows_loaded": loaded,
        "quote_json_errors": errors,
        "book_rows": int(len(frame)),
        "book_parse_status_counts": frame["book_parse_status"].value_counts().to_dict() if not frame.empty else {},
        "execution_depth_mode_counts": frame["execution_depth_mode"].value_counts().to_dict() if not frame.empty else {},
    }
    return frame, diagnostics


def select_execution_book(books: pd.DataFrame, market_key: str, side: str, decision_ts: pd.Timestamp, latency_ms: float, max_book_age_seconds: float) -> dict[str, Any]:
    target = pd.Timestamp(decision_ts).tz_convert("UTC") + pd.Timedelta(milliseconds=latency_ms)
    subset = books[(books["market_key"].eq(market_key)) & (books["asset_side"].eq(side)) & (books["timestamp"] >= target)].sort_values("timestamp")
    base = {"target_exec_ts": target, "execution_book_ts": pd.NaT, "execution_book_lag_seconds": np.nan, "book_is_after_target": None}
    if subset.empty:
        return {**base, "execution_book_status": "no_execution_book"}
    row = subset.iloc[0].to_dict()
    lag = (row["timestamp"] - target).total_seconds()
    status = "ok" if lag <= max_book_age_seconds else "stale_book"
    if row.get("book_parse_status") not in ("ok_full_depth", "ok_top_of_book_only"):
        status = "missing_side_book" if row.get("book_parse_status") == "missing_side" else "malformed_book"
    return {
        **row,
        **base,
        "target_exec_ts": target,
        "execution_book_ts": row["timestamp"],
        "execution_book_lag_seconds": lag,
        "book_is_after_target": True,
        "execution_book_status": status,
    }


def simulate_vwap_fill(
    asks: list[dict[str, float]],
    stake_usdc: float,
    *,
    min_trade_notional_usdc: float,
    min_fill_ratio: float,
    allow_partial_fills: bool,
) -> dict[str, Any]:
    clean = sorted([{"price": float(x["price"]), "size": float(x["size"])} for x in asks if x.get("price") and x.get("size") and float(x["size"]) > 0], key=lambda x: x["price"])
    if not clean:
        return {"fill_status": "missing_depth", "shares_filled": 0.0, "gross_trade_notional": 0.0, "fill_ratio": 0.0}
    visible_notional = sum(level["price"] * level["size"] for level in clean)
    if visible_notional < min_trade_notional_usdc:
        return {"fill_status": "below_min_notional", "visible_notional": visible_notional, "shares_filled": 0.0, "gross_trade_notional": 0.0, "fill_ratio": 0.0}
    remaining = stake_usdc
    shares = 0.0
    gross = 0.0
    for level in clean:
        if remaining <= 1e-12:
            break
        spend = min(remaining, level["price"] * level["size"])
        shares += spend / level["price"]
        gross += spend
        remaining -= spend
    fill_ratio = gross / stake_usdc if stake_usdc else 0.0
    if fill_ratio + 1e-12 < min_fill_ratio:
        if not allow_partial_fills:
            return {"fill_status": "insufficient_depth", "visible_notional": visible_notional, "shares_filled": shares, "gross_trade_notional": gross, "fill_ratio": fill_ratio}
        status = "partial_fill"
    else:
        status = "filled"
    vwap = gross / shares if shares else np.nan
    return {
        "fill_status": status,
        "visible_notional": visible_notional,
        "shares_filled": shares,
        "gross_trade_notional": gross,
        "vwap_price": vwap,
        "unspent_cash": remaining,
        "fill_ratio": fill_ratio,
        "best_ask_at_execution": clean[0]["price"],
    }


def apply_fee_and_score(fill: dict[str, Any], *, fee_rate: float, p_chosen_side: float, edge_threshold: float, require_edge: bool, label_up: float, side: str) -> dict[str, Any]:
    shares = float(fill.get("shares_filled") or 0.0)
    gross = float(fill.get("gross_trade_notional") or 0.0)
    vwap = float(fill.get("vwap_price") or np.nan)
    if not shares or not np.isfinite(vwap):
        return {**fill, "score_status": fill.get("fill_status", "not_filled")}
    fee = shares * fee_rate * vwap * (1.0 - vwap)
    fee_per_share = fee / shares
    edge_after_vwap = p_chosen_side - vwap - fee_per_share
    if require_edge and edge_after_vwap < edge_threshold:
        return {**fill, "fee": fee, "fee_per_share": fee_per_share, "edge_after_vwap": edge_after_vwap, "score_status": "failed_edge_after_vwap"}
    win = (side == "YES" and label_up == 1.0) or (side == "NO" and label_up == 0.0)
    total_cost = gross + fee
    payout = shares if win else 0.0
    pnl = payout - total_cost
    return {
        **fill,
        "fee": fee,
        "fee_per_share": fee_per_share,
        "total_cost": total_cost,
        "gross_payout": payout,
        "pnl": pnl,
        "trade_roi": pnl / total_cost if total_cost else np.nan,
        "edge_after_vwap": edge_after_vwap,
        "win": float(win),
        "score_status": "filled",
    }


def label_for_source(row: pd.Series, label_source: str) -> float | None:
    if label_source == "chainlink":
        return row.get("chainlink_label_up") if pd.notna(row.get("chainlink_label_up")) else None
    if label_source == "binance":
        return row.get("binance_label_up") if pd.notna(row.get("binance_label_up")) else None
    if label_source == "agreement_only":
        if row.get("label_agree") is True or row.get("label_agree") == 1:
            return row.get("binance_label_up")
        return None
    if label_source == "disagreement_only":
        if row.get("label_agree") is False or row.get("label_agree") == 0:
            return row.get("chainlink_label_up")
        return None
    raise ValueError(f"unsupported label source: {label_source}")


def p_chosen(row: pd.Series) -> float:
    p = float(row["p_up"])
    return p if row["side"] == "YES" else 1.0 - p


def find_markout(books: pd.DataFrame, market_key: str, side: str, execution_ts: pd.Timestamp, horizon_seconds: float, total_cost: float, shares: float, tolerance_seconds: float = 2.0) -> dict[str, Any]:
    target = execution_ts + pd.Timedelta(seconds=horizon_seconds)
    subset = books[(books["market_key"].eq(market_key)) & (books["asset_side"].eq(side)) & (books["timestamp"] >= target)].sort_values("timestamp")
    if subset.empty:
        return {"horizon_seconds": horizon_seconds, "markout_status": "missing_later_book"}
    row = subset.iloc[0]
    lag = (row["timestamp"] - target).total_seconds()
    if lag > tolerance_seconds:
        return {"horizon_seconds": horizon_seconds, "markout_status": "stale_later_book", "markout_lag_seconds": lag}
    bid = row.get("best_bid")
    ask = row.get("best_ask")
    if pd.isna(bid):
        return {"horizon_seconds": horizon_seconds, "markout_status": "missing_bid", "markout_lag_seconds": lag}
    mid = (bid + ask) / 2.0 if pd.notna(ask) else np.nan
    return {
        "horizon_seconds": horizon_seconds,
        "markout_status": "ok",
        "markout_lag_seconds": lag,
        "markout_best_bid": bid,
        "markout_best_ask": ask,
        "markout_mid": mid,
        "markout_exit_value_using_bid": shares * bid,
        "markout_pnl_using_bid": shares * bid - total_cost,
        "markout_roi_using_bid": (shares * bid - total_cost) / total_cost if total_cost else np.nan,
        "markout_mid_pnl": shares * mid - total_cost if pd.notna(mid) else np.nan,
    }


def add_baseline_incremental(score: pd.DataFrame) -> pd.DataFrame:
    keys = ["label_source", "edge_threshold", "stake_usdc", "latency_ms", "max_book_age_seconds", "fee_rate", "entry_age_set"]
    if score.empty:
        return score
    base = score[score["model_id"].eq("baseline_50")][keys + ["total_pnl", "aggregate_roi"]].drop_duplicates(keys, keep="first")
    dupes = base.duplicated(keys).sum()
    base = base.rename(columns={"total_pnl": "baseline_50_pnl", "aggregate_roi": "baseline_50_roi"})
    out = score.merge(base, on=keys, how="left", validate="many_to_one")
    out["baseline_status"] = np.where(out["baseline_50_pnl"].notna(), "ok", "missing")
    out["incremental_pnl_vs_baseline_50"] = out["total_pnl"] - out["baseline_50_pnl"]
    out["incremental_roi_vs_baseline_50"] = out["aggregate_roi"] - out["baseline_50_roi"]
    out.attrs["baseline_duplicate_rows"] = int(dupes)
    return out


def aggregate_scorecard(fills: pd.DataFrame, selected: pd.DataFrame, group_cols: list[str], entry_age_set: str, fee_rate: float) -> pd.DataFrame:
    rows = []
    if fills.empty:
        return pd.DataFrame()
    for keys, group in fills.groupby(group_cols, dropna=False):
        if not isinstance(keys, tuple):
            keys = (keys,)
        row = dict(zip(group_cols, keys))
        filled = group[group["score_status"].eq("filled")]
        total_cost = filled["total_cost"].sum() if not filled.empty else 0.0
        both = group[group["label_agree"].notna()] if "label_agree" in group else pd.DataFrame()
        row.update(
            {
                "fee_rate": fee_rate,
                "entry_age_set": entry_age_set,
                "trades_selected_before_execution": int(len(group)),
                "trades_with_execution_book": int(group["execution_book_status"].eq("ok").sum()),
                "trades_fillable": int(group["fill_status"].isin(["filled", "partial_fill"]).sum()),
                "trades_filled": int(len(filled)),
                "trades_rejected_no_book": int(group["execution_book_status"].eq("no_execution_book").sum()),
                "trades_rejected_stale_book": int(group["execution_book_status"].eq("stale_book").sum()),
                "trades_rejected_insufficient_depth": int(group["fill_status"].eq("insufficient_depth").sum()),
                "trades_rejected_below_min_notional": int(group["fill_status"].eq("below_min_notional").sum()),
                "trades_rejected_failed_edge_after_vwap": int(group["score_status"].eq("failed_edge_after_vwap").sum()),
                "fillable_rate": float(group["fill_status"].isin(["filled", "partial_fill"]).mean()) if len(group) else np.nan,
                "fill_rate": float(group["score_status"].eq("filled").mean()) if len(group) else np.nan,
                "avg_fill_ratio": float(group["fill_ratio"].mean()) if "fill_ratio" in group else np.nan,
                "total_stake_requested": float(group["stake_usdc"].sum()) if "stake_usdc" in group else np.nan,
                "total_gross_trade_notional": float(filled["gross_trade_notional"].sum()) if not filled.empty else 0.0,
                "total_fees": float(filled["fee"].sum()) if not filled.empty else 0.0,
                "total_cost": float(total_cost),
                "total_payout": float(filled["gross_payout"].sum()) if not filled.empty else 0.0,
                "total_pnl": float(filled["pnl"].sum()) if not filled.empty else 0.0,
                "aggregate_roi": float(filled["pnl"].sum() / total_cost) if total_cost else np.nan,
                "mean_trade_roi": float(filled["trade_roi"].mean()) if not filled.empty else np.nan,
                "median_trade_roi": float(filled["trade_roi"].median()) if not filled.empty else np.nan,
                "wins": int(filled["win"].sum()) if not filled.empty else 0,
                "losses": int(len(filled) - filled["win"].sum()) if not filled.empty else 0,
                "hit_rate": float(filled["win"].mean()) if not filled.empty else np.nan,
                "avg_original_entry_price": float(group["raw_entry_price"].mean()) if "raw_entry_price" in group else np.nan,
                "avg_execution_best_ask": float(group["best_ask_at_execution"].mean()) if "best_ask_at_execution" in group else np.nan,
                "avg_vwap_price": float(filled["vwap_price"].mean()) if not filled.empty else np.nan,
                "avg_vwap_minus_original_entry": float((group["vwap_price"] - group["raw_entry_price"]).mean()) if "vwap_price" in group and "raw_entry_price" in group else np.nan,
                "avg_edge_before_vwap": float(group["raw_edge"].mean()) if "raw_edge" in group else np.nan,
                "avg_edge_after_vwap": float(filled["edge_after_vwap"].mean()) if not filled.empty else np.nan,
                "median_edge_after_vwap": float(filled["edge_after_vwap"].median()) if not filled.empty else np.nan,
                "p10_edge_after_vwap": float(np.nanpercentile(filled["edge_after_vwap"], 10)) if not filled.empty else np.nan,
                "p90_edge_after_vwap": float(np.nanpercentile(filled["edge_after_vwap"], 90)) if not filled.empty else np.nan,
                "selected_trade_disagreement_rate": float((~both["label_agree"].astype(bool)).mean()) if not both.empty else np.nan,
                "chainlink_binance_agreement_rate": float(both["label_agree"].mean()) if not both.empty else np.nan,
            }
        )
        rows.append(row)
    return pd.DataFrame(rows)


def fail_reason_table(fills: pd.DataFrame) -> pd.DataFrame:
    if fills.empty:
        return pd.DataFrame()
    reasons = []
    for _, row in fills.iterrows():
        reason = None
        if row.get("label_status") == "missing_label":
            reason = "missing_label"
        elif row.get("execution_book_status") != "ok":
            reason = row.get("execution_book_status")
        elif row.get("fill_status") != "filled":
            reason = row.get("fill_status")
        elif row.get("score_status") != "filled":
            reason = row.get("score_status")
        if reason:
            reasons.append({**{c: row.get(c) for c in ["model_id", "stake_usdc", "latency_ms", "age_bucket"]}, "fail_reason": reason})
    frame = pd.DataFrame(reasons)
    if frame.empty:
        return pd.DataFrame()
    total = len(fills)
    return frame.groupby(["fail_reason", "model_id", "stake_usdc", "latency_ms", "age_bucket"], dropna=False).size().rename("count").reset_index().assign(share_of_selected=lambda x: x["count"] / total)
