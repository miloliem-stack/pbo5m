#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.research import execution_realism_replay as ex


CAPACITY_COLUMNS_BY_THRESHOLD = {
    0.10: "capacity_usdc_at_edge_10",
    0.07: "capacity_usdc_at_edge_07",
    0.05: "capacity_usdc_at_edge_05",
    0.00: "capacity_usdc_until_baseline_edge",
}
DEFAULT_DECISION_AGES = "60,120,180"


def parse_csv(value: str | None) -> list[str]:
    return [item.strip() for item in str(value or "").split(",") if item.strip()]


def parse_csv_floats(value: str | None) -> list[float]:
    return [float(item.strip()) for item in str(value or "").split(",") if item.strip()]


def parse_bool(value: str | bool) -> bool:
    if isinstance(value, bool):
        return value
    lower = str(value).strip().lower()
    if lower in {"1", "true", "yes", "y", "on"}:
        return True
    if lower in {"0", "false", "no", "n", "off"}:
        return False
    raise argparse.ArgumentTypeError(f"expected boolean value, got {value!r}")


def read_frame(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".parquet":
        try:
            return pd.read_parquet(path)
        except ImportError as exc:
            sidecar = path.with_suffix(path.suffix + ".as.json")
            if sidecar.exists():
                return pd.read_json(sidecar, lines=True)
            raise ImportError(f"Parquet support is unavailable for {path}; install pyarrow/fastparquet or provide CSV") from exc
    return pd.read_csv(path)


def capacity_col_for_threshold(threshold: float) -> str | None:
    for key, col in CAPACITY_COLUMNS_BY_THRESHOLD.items():
        if abs(threshold - key) < 1e-9:
            return col
    return None


def normalize_selected_entries(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    if "model_id" not in out.columns and "model" in out.columns:
        out["model_id"] = out["model"]
    if "market_key" not in out.columns and "prediction_market_key" in out.columns:
        out["market_key"] = out["prediction_market_key"]
    if "prediction_ts" not in out.columns and "decision_ts" in out.columns:
        out["prediction_ts"] = out["decision_ts"]
    if "market_age_seconds" not in out.columns and "decision_age" in out.columns:
        out["market_age_seconds"] = out["decision_age"]
    if "raw_entry_price" not in out.columns and "selected_price" in out.columns:
        out["raw_entry_price"] = out["selected_price"]
    if "raw_edge" not in out.columns and "predicted_edge" in out.columns:
        out["raw_edge"] = out["predicted_edge"]
    required = ["model_id", "market_key", "prediction_ts", "market_age_seconds", "side"]
    missing = [col for col in required if col not in out.columns]
    if missing:
        raise ValueError(f"selected entries missing required columns {missing}; available columns: {list(frame.columns)}")
    out["prediction_ts"] = pd.to_datetime(out["prediction_ts"], utc=True, errors="coerce")
    out["market_age_seconds"] = pd.to_numeric(out["market_age_seconds"], errors="coerce")
    if "p_up" in out.columns:
        out["p_up"] = pd.to_numeric(out["p_up"], errors="coerce")
    if "selected_side_probability" in out.columns:
        out["selected_side_probability"] = pd.to_numeric(out["selected_side_probability"], errors="coerce")
    for col in ["edge_threshold", "raw_entry_price", "raw_edge", "fold_id"]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce") if col != "fold_id" else out[col]
    return out.dropna(subset=["model_id", "market_key", "prediction_ts", "market_age_seconds", "side"]).reset_index(drop=True)


def normalize_capacity_output(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    if "market_key" not in out.columns and "market_id" in out.columns:
        out["market_key"] = out["market_id"]
    if "prediction_ts" not in out.columns and "decision_ts" in out.columns:
        out["prediction_ts"] = out["decision_ts"]
    if "market_age_seconds" not in out.columns and "decision_age" in out.columns:
        out["market_age_seconds"] = out["decision_age"]
    out["prediction_ts"] = pd.to_datetime(out["prediction_ts"], utc=True, errors="coerce")
    out["market_age_seconds"] = pd.to_numeric(out["market_age_seconds"], errors="coerce")
    if "latency_ms" in out.columns:
        out["latency_ms"] = pd.to_numeric(out["latency_ms"], errors="coerce")
    for col in ["p_chosen_side", "best_ask", *CAPACITY_COLUMNS_BY_THRESHOLD.values(), "max_fillable_usdc"]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    return out


def p_chosen(row: pd.Series) -> float:
    if "p_chosen_side" in row and pd.notna(row.get("p_chosen_side")):
        return float(row["p_chosen_side"])
    if "selected_side_probability" in row and pd.notna(row.get("selected_side_probability")):
        return float(row["selected_side_probability"])
    p_up = float(row["p_up"])
    return p_up if str(row["side"]).upper() == "YES" else 1.0 - p_up


def fee_per_share(vwap: float, fee_rate: float) -> float:
    return fee_rate * vwap * (1.0 - vwap)


def edge_after_vwap(p: float, vwap: float, fee_rate: float) -> float:
    if not np.isfinite(p) or not np.isfinite(vwap):
        return np.nan
    return float(p - vwap - fee_per_share(vwap, fee_rate))


def clean_asks(asks: list[dict[str, Any]]) -> list[dict[str, float]]:
    rows: list[dict[str, float]] = []
    for level in asks or []:
        try:
            price = float(level.get("price"))
            size = float(level.get("size"))
        except Exception:
            continue
        if np.isfinite(price) and np.isfinite(size) and size > 0:
            rows.append({"price": price, "size": size})
    return sorted(rows, key=lambda row: row["price"])


def walk_capacity_ladder(
    asks: list[dict[str, Any]],
    *,
    p: float,
    threshold: float,
    fee_rate: float,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    clean = clean_asks(asks)
    if not np.isfinite(p):
        return pd.DataFrame(), {"capacity_stop_reason": "missing_probability", "computed_capacity_usdc": 0.0, "max_fillable_usdc": 0.0}
    if not clean:
        return pd.DataFrame(), {"capacity_stop_reason": "missing_side_book", "computed_capacity_usdc": 0.0, "max_fillable_usdc": 0.0}

    ladder_rows: list[dict[str, Any]] = []
    gross = 0.0
    shares = 0.0
    computed_capacity = 0.0
    last_accepted: dict[str, float] | None = None
    first_rejected: dict[str, float] | None = None
    stop_reason = "exhausted_depth"
    previous_vwap = np.nan
    previous_edge = np.nan
    suspicious: set[str] = set()
    max_fillable = float(sum(level["price"] * level["size"] for level in clean))

    seen_level_keys: set[tuple[float, float]] = set()
    duplicate_level_count = 0
    for level_index, level in enumerate(clean):
        price = float(level["price"])
        size = float(level["size"])
        if (price, size) in seen_level_keys:
            duplicate_level_count += 1
        seen_level_keys.add((price, size))
        if price <= 0 or price >= 1:
            suspicious.add("invalid_price_unit")
            reason = "invalid_price"
        elif size <= 0:
            suspicious.add("invalid_size")
            reason = "invalid_size"
        else:
            reason = ""

        level_notional = price * size
        next_gross = gross + level_notional
        next_shares = shares + size
        next_vwap = next_gross / next_shares if next_shares else np.nan
        next_fee = fee_per_share(next_vwap, fee_rate) if np.isfinite(next_vwap) else np.nan
        next_edge = edge_after_vwap(p, next_vwap, fee_rate)
        if np.isfinite(previous_vwap) and next_vwap + 1e-12 < previous_vwap:
            suspicious.add("vwap_decreased")
        if np.isfinite(previous_edge) and next_edge > previous_edge + 1e-10:
            suspicious.add("edge_increased_after_worse_level")

        level_row = {
            "price_level": price,
            "shares_available": size,
            "level_notional_usdc": level_notional,
            "cumulative_shares": next_shares,
            "cumulative_notional_usdc": next_gross,
            "vwap_price": next_vwap,
            "fee_per_share": next_fee,
            "edge_after_vwap": next_edge,
            "capacity_stop_reason": reason,
            "level_index": level_index,
        }

        if reason:
            stop_reason = reason
            ladder_rows.append(level_row)
            first_rejected = {"price": price, "vwap": next_vwap, "edge": next_edge}
            break

        if next_edge >= threshold:
            gross = next_gross
            shares = next_shares
            computed_capacity = gross
            last_accepted = {"price": price, "vwap": next_vwap, "edge": next_edge}
            level_row["capacity_stop_reason"] = ""
            ladder_rows.append(level_row)
            previous_vwap = next_vwap
            previous_edge = next_edge
            continue

        stop_reason = "edge_below_threshold"
        first_rejected = {"price": price, "vwap": next_vwap, "edge": next_edge}
        # Match the capacity replay behavior: accept a partial amount of this level
        # when the full level would push edge below the threshold.
        lo, hi = 0.0, level_notional
        for _ in range(40):
            mid = (lo + hi) / 2.0
            mid_shares = shares + mid / price
            mid_vwap = (gross + mid) / mid_shares if mid_shares else np.nan
            if edge_after_vwap(p, mid_vwap, fee_rate) >= threshold:
                lo = mid
            else:
                hi = mid
        if lo > 1e-12:
            partial_shares = lo / price
            partial_gross = gross + lo
            partial_total_shares = shares + partial_shares
            partial_vwap = partial_gross / partial_total_shares
            partial_edge = edge_after_vwap(p, partial_vwap, fee_rate)
            computed_capacity = partial_gross
            last_accepted = {"price": price, "vwap": partial_vwap, "edge": partial_edge}
            ladder_rows.append(
                {
                    "price_level": price,
                    "shares_available": partial_shares,
                    "level_notional_usdc": lo,
                    "cumulative_shares": partial_total_shares,
                    "cumulative_notional_usdc": partial_gross,
                    "vwap_price": partial_vwap,
                    "fee_per_share": fee_per_share(partial_vwap, fee_rate),
                    "edge_after_vwap": partial_edge,
                    "capacity_stop_reason": "",
                    "level_index": level_index,
                    "partial_level": True,
                }
            )
        level_row["capacity_stop_reason"] = "edge_below_threshold"
        level_row["partial_level"] = False
        ladder_rows.append(level_row)
        break

    if duplicate_level_count:
        suspicious.add("duplicate_price_size_levels_in_book")

    ladder = pd.DataFrame(ladder_rows)
    summary = {
        "computed_capacity_usdc": float(computed_capacity),
        "max_fillable_usdc": float(max_fillable),
        "levels_walked": int(len(ladder_rows)),
        "last_accepted_price": None if last_accepted is None else last_accepted["price"],
        "last_accepted_vwap": None if last_accepted is None else last_accepted["vwap"],
        "last_accepted_edge_after_vwap": None if last_accepted is None else last_accepted["edge"],
        "first_rejected_price": None if first_rejected is None else first_rejected["price"],
        "first_rejected_vwap": None if first_rejected is None else first_rejected["vwap"],
        "first_rejected_edge_after_vwap": None if first_rejected is None else first_rejected["edge"],
        "capacity_stop_reason": stop_reason,
        "duplicate_level_count": duplicate_level_count,
        "suspicious_flags": ",".join(sorted(suspicious)),
    }
    return ladder, summary


def filter_rows(frame: pd.DataFrame, *, models: list[str], decision_ages: list[float], latency_ms: float | None) -> pd.DataFrame:
    out = frame.copy()
    if models:
        out = out[out["model_id"].astype(str).isin(models)]
    if decision_ages and "market_age_seconds" in out.columns:
        age_mask = pd.Series(False, index=out.index)
        for age in decision_ages:
            age_mask |= (pd.to_numeric(out["market_age_seconds"], errors="coerce") - age).abs() < 1e-9
        out = out[age_mask]
    if latency_ms is not None and "latency_ms" in out.columns:
        out = out[(pd.to_numeric(out["latency_ms"], errors="coerce") - latency_ms).abs() < 1e-9]
    return out.reset_index(drop=True)


def build_samples(
    selected_entries: pd.DataFrame,
    capacity_output: pd.DataFrame | None,
    *,
    sample_size: int,
    seed: int,
    threshold: float,
    include_random: bool,
    include_largest: bool,
    include_smallest: bool,
) -> pd.DataFrame:
    reported_col = capacity_col_for_threshold(threshold)
    source = capacity_output if capacity_output is not None else selected_entries
    samples = []
    if include_random and not source.empty:
        samples.append(source.sample(n=min(sample_size, len(source)), random_state=seed).assign(sample_group="random"))
    if capacity_output is not None and reported_col and reported_col in capacity_output.columns:
        nonnull = capacity_output.dropna(subset=[reported_col]).copy()
        if include_largest and not nonnull.empty:
            samples.append(nonnull.sort_values(reported_col, ascending=False).head(sample_size).assign(sample_group="largest_capacity"))
        if include_smallest:
            smallest = nonnull[nonnull[reported_col] > 0].sort_values(reported_col, ascending=True).head(sample_size)
            if not smallest.empty:
                samples.append(smallest.assign(sample_group="smallest_nonzero_capacity"))
    if not samples:
        return pd.DataFrame()
    out = pd.concat(samples, ignore_index=True)
    key_cols = [col for col in ["market_key", "model_id", "market_age_seconds", "latency_ms", "side", "prediction_ts"] if col in out.columns]
    out = out.drop_duplicates(subset=key_cols).reset_index(drop=True)
    out["sample_id"] = [f"sample_{idx:04d}" for idx in range(len(out))]
    if capacity_output is not None and reported_col and reported_col in out.columns:
        out["reported_capacity_usdc"] = pd.to_numeric(out[reported_col], errors="coerce")
    return out


def enrich_from_selected(samples: pd.DataFrame, selected_entries: pd.DataFrame) -> pd.DataFrame:
    if samples.empty:
        return samples
    selected_cols = [col for col in selected_entries.columns if col not in samples.columns or col in {"market_key", "model_id", "prediction_ts", "market_age_seconds", "side"}]
    keys = ["market_key", "model_id", "prediction_ts", "market_age_seconds", "side"]
    missing_keys = [key for key in keys if key not in samples.columns or key not in selected_entries.columns]
    if missing_keys:
        return samples
    selected_small = selected_entries[selected_cols].drop_duplicates(subset=keys)
    return samples.merge(selected_small, on=keys, how="left", suffixes=("", "_selected"))


def audit_samples(samples: pd.DataFrame, books: pd.DataFrame, *, latency_ms: float, max_book_age_seconds: float, threshold: float, fee_rate: float) -> tuple[pd.DataFrame, pd.DataFrame]:
    ladder_frames: list[pd.DataFrame] = []
    summary_rows: list[dict[str, Any]] = []
    for _, row in samples.iterrows():
        sample_latency = float(row.get("latency_ms")) if "latency_ms" in row and pd.notna(row.get("latency_ms")) else latency_ms
        side = str(row.get("side")).upper()
        decision_ts = pd.Timestamp(row["prediction_ts"])
        book = ex.select_execution_book(books, str(row["market_key"]), side, decision_ts, sample_latency, max_book_age_seconds)
        p = p_chosen(row)
        asks = book.get("asks") or []
        if book.get("execution_book_status") != "ok":
            ladder, ladder_summary = pd.DataFrame(), {
                "computed_capacity_usdc": 0.0,
                "max_fillable_usdc": 0.0,
                "capacity_stop_reason": book.get("execution_book_status"),
                "levels_walked": 0,
                "suspicious_flags": "",
            }
        else:
            ladder, ladder_summary = walk_capacity_ladder(asks, p=p, threshold=threshold, fee_rate=fee_rate)
        reported = row.get("reported_capacity_usdc", np.nan)
        diff = ladder_summary.get("computed_capacity_usdc", np.nan) - reported if pd.notna(reported) else np.nan
        base = {
            "sample_id": row.get("sample_id"),
            "sample_group": row.get("sample_group"),
            "market_id": row.get("market_key"),
            "model_id": row.get("model_id"),
            "decision_age": row.get("market_age_seconds"),
            "latency_ms": sample_latency,
            "decision_ts": decision_ts,
            "target_exec_ts": book.get("target_exec_ts", decision_ts + pd.Timedelta(milliseconds=sample_latency)),
            "execution_book_ts": book.get("execution_book_ts"),
            "side": side,
            "label_source": row.get("label_source"),
            "p_chosen_side": p,
            "raw_entry_price": row.get("raw_entry_price", row.get("selected_price", np.nan)),
            "execution_best_ask": book.get("best_ask", np.nan),
            "edge_threshold": threshold,
            "fee_rate": fee_rate,
            "book_depth_mode": book.get("execution_depth_mode"),
            "book_parse_status": book.get("book_parse_status"),
            "execution_book_status": book.get("execution_book_status"),
        }
        if not ladder.empty:
            ladder = ladder.assign(**base)
            ordered = [
                "sample_id",
                "market_id",
                "model_id",
                "decision_age",
                "latency_ms",
                "decision_ts",
                "target_exec_ts",
                "execution_book_ts",
                "side",
                "label_source",
                "p_chosen_side",
                "raw_entry_price",
                "execution_best_ask",
                "edge_threshold",
                "fee_rate",
                "price_level",
                "shares_available",
                "level_notional_usdc",
                "cumulative_shares",
                "cumulative_notional_usdc",
                "vwap_price",
                "fee_per_share",
                "edge_after_vwap",
                "capacity_stop_reason",
                "level_index",
                "partial_level",
            ]
            ladder_frames.append(ladder[[col for col in ordered if col in ladder.columns]])
        summary_rows.append(
            {
                **base,
                "computed_capacity_usdc_at_edge_10": ladder_summary.get("computed_capacity_usdc") if abs(threshold - 0.10) < 1e-9 else np.nan,
                "computed_capacity_usdc": ladder_summary.get("computed_capacity_usdc"),
                "reported_capacity_usdc_at_edge_10": reported if abs(threshold - 0.10) < 1e-9 else np.nan,
                "reported_capacity_usdc": reported,
                "difference_computed_minus_reported": diff,
                "max_fillable_usdc": ladder_summary.get("max_fillable_usdc"),
                "levels_walked": ladder_summary.get("levels_walked"),
                "last_accepted_price": ladder_summary.get("last_accepted_price"),
                "last_accepted_vwap": ladder_summary.get("last_accepted_vwap"),
                "last_accepted_edge_after_vwap": ladder_summary.get("last_accepted_edge_after_vwap"),
                "first_rejected_price": ladder_summary.get("first_rejected_price"),
                "first_rejected_vwap": ladder_summary.get("first_rejected_vwap"),
                "first_rejected_edge_after_vwap": ladder_summary.get("first_rejected_edge_after_vwap"),
                "capacity_stop_reason": ladder_summary.get("capacity_stop_reason"),
                "duplicate_level_count": ladder_summary.get("duplicate_level_count", 0),
                "suspicious_flags": ladder_summary.get("suspicious_flags", ""),
            }
        )
    ladders = pd.concat(ladder_frames, ignore_index=True) if ladder_frames else pd.DataFrame()
    summary = pd.DataFrame(summary_rows)
    return ladders, summary


def write_readme(path: Path, diagnostics: dict[str, Any], summary: pd.DataFrame) -> None:
    mismatch_count = int(diagnostics.get("reported_capacity_mismatches", 0))
    lines = [
        "Capacity ladder audit",
        "",
        "Offline research only. This reconstructs sampled selected-side execution ladders from recorder books and recomputes visible USDC notional capacity.",
        "",
        f"selected_entries: {diagnostics.get('selected_entries_path')}",
        f"quotes_root: {diagnostics.get('quotes_root')}",
        f"capacity_output: {diagnostics.get('capacity_output_path') or 'not provided'}",
        f"samples_requested_per_group: {diagnostics.get('sample_size')}",
        f"samples_audited: {diagnostics.get('samples_audited')}",
        f"capacity_output_provided: {diagnostics.get('capacity_output_provided')}",
        "",
        "Unit checks:",
        "- ask sizes are treated as shares/contracts from the CLOB book levels.",
        "- USDC notional is recomputed as price_level * shares_available.",
        "- YES and NO books are normalized as separate asset_side rows; this audit walks only the selected side.",
        "- duplicate price levels inside one book are preserved as recorded and flagged if exact price/size levels repeat.",
        "- one execution-time book snapshot is selected per sampled entry after the configured latency.",
        "- prices are expected in 0-1 binary-market probability units.",
        "",
        f"max_abs_difference_computed_vs_reported: {diagnostics.get('max_abs_difference_computed_vs_reported')}",
        f"reported_capacity_exact_matches: {diagnostics.get('reported_capacity_exact_matches')}",
        f"reported_capacity_mismatches: {mismatch_count}",
    ]
    if diagnostics.get("suspicious_flag_counts"):
        lines += ["", "Suspicious flags:", json.dumps(diagnostics["suspicious_flag_counts"], indent=2)]
    if mismatch_count:
        lines.append("")
        lines.append("WARNING: Some reported capacities differ from the recomputed ladder capacity beyond tolerance.")
    if summary.get("book_depth_mode", pd.Series(dtype=object)).ne("full_depth").any():
        lines.append("WARNING: Some sampled entries did not have full-depth books.")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run(args: argparse.Namespace) -> dict[str, Any]:
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    selected_entries = normalize_selected_entries(read_frame(Path(args.selected_entries)))
    models = parse_csv(args.models)
    decision_ages = parse_csv_floats(args.decision_ages)
    selected_entries = filter_rows(selected_entries, models=models, decision_ages=decision_ages, latency_ms=None)

    capacity_output = None
    if args.capacity_output:
        capacity_output = normalize_capacity_output(read_frame(Path(args.capacity_output)))
        capacity_output = filter_rows(capacity_output, models=models, decision_ages=decision_ages, latency_ms=args.latency_ms)

    samples = build_samples(
        selected_entries,
        capacity_output,
        sample_size=args.sample_size,
        seed=args.seed,
        threshold=args.edge_threshold,
        include_random=args.include_random,
        include_largest=args.include_largest_capacity,
        include_smallest=args.include_smallest_capacity,
    )
    samples = enrich_from_selected(samples, selected_entries)
    samples.to_csv(output_dir / "capacity_ladder_audit_samples.csv", index=False)
    if args.dry_run:
        diagnostics = {"dry_run": True, "samples": int(len(samples))}
        (output_dir / "capacity_ladder_audit_diagnostics.json").write_text(json.dumps(diagnostics, indent=2, default=str), encoding="utf-8")
        return diagnostics

    if samples.empty:
        raise ValueError("No samples selected. Check model, age, latency, and capacity-output filters.")

    sample_latency = pd.to_numeric(samples.get("latency_ms", args.latency_ms), errors="coerce").fillna(args.latency_ms)
    target_times = pd.to_datetime(samples["prediction_ts"], utc=True, errors="coerce") + pd.to_timedelta(sample_latency, unit="ms")
    quote_files = ex.quote_files_for_targets(Path(args.quotes_root), target_times)
    books, book_diag = ex.load_books_from_files(quote_files, set(samples["market_key"].dropna().astype(str).unique()))
    ladders, summary = audit_samples(
        samples,
        books,
        latency_ms=args.latency_ms,
        max_book_age_seconds=args.max_book_age_seconds,
        threshold=args.edge_threshold,
        fee_rate=args.fee_rate,
    )
    ladders.to_csv(output_dir / "capacity_ladder_audit_ladders.csv", index=False)
    summary.to_csv(output_dir / "capacity_ladder_audit_summary.csv", index=False)

    diff = pd.to_numeric(summary.get("difference_computed_minus_reported"), errors="coerce")
    exact = diff.abs() <= args.compare_tolerance
    suspicious_counts: dict[str, int] = {}
    if "suspicious_flags" in summary:
        for value in summary["suspicious_flags"].dropna().astype(str):
            for flag in [item for item in value.split(",") if item]:
                suspicious_counts[flag] = suspicious_counts.get(flag, 0) + 1
    diagnostics = {
        **book_diag,
        "selected_entries_path": str(args.selected_entries),
        "quotes_root": str(args.quotes_root),
        "capacity_output_path": str(args.capacity_output) if args.capacity_output else None,
        "capacity_output_provided": bool(args.capacity_output),
        "sample_size": int(args.sample_size),
        "samples_audited": int(len(summary)),
        "ladder_rows": int(len(ladders)),
        "edge_threshold": float(args.edge_threshold),
        "latency_ms": float(args.latency_ms),
        "max_book_age_seconds": float(args.max_book_age_seconds),
        "fee_rate": float(args.fee_rate),
        "quote_files_read_paths": [str(path) for path in quote_files],
        "max_abs_difference_computed_vs_reported": None if diff.dropna().empty else float(diff.abs().max()),
        "reported_capacity_exact_matches": int(exact.sum()) if not diff.dropna().empty else 0,
        "reported_capacity_mismatches": int((diff.abs() > args.compare_tolerance).sum()) if not diff.dropna().empty else 0,
        "suspicious_flag_counts": suspicious_counts,
        "book_depth_mode_counts_sampled": summary["book_depth_mode"].value_counts(dropna=False).to_dict() if "book_depth_mode" in summary else {},
        "book_parse_status_counts_sampled": summary["book_parse_status"].value_counts(dropna=False).to_dict() if "book_parse_status" in summary else {},
    }
    (output_dir / "capacity_ladder_audit_diagnostics.json").write_text(json.dumps(diagnostics, indent=2, default=str), encoding="utf-8")
    write_readme(output_dir / "capacity_ladder_audit_readme.txt", diagnostics, summary)
    return diagnostics


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Audit sampled capacity replay ladders against raw recorder orderbook depth.")
    parser.add_argument("--selected-entries", type=Path, required=True)
    parser.add_argument("--quotes-root", type=Path, required=True)
    parser.add_argument("--capacity-output", type=Path)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--sample-size", type=int, default=20)
    parser.add_argument("--models")
    parser.add_argument("--edge-threshold", type=float, default=0.10)
    parser.add_argument("--latency-ms", type=float, default=1000.0)
    parser.add_argument("--decision-ages", default=DEFAULT_DECISION_AGES)
    parser.add_argument("--seed", type=int, default=123)
    parser.add_argument("--include-largest-capacity", type=parse_bool, default=True)
    parser.add_argument("--include-random", type=parse_bool, default=True)
    parser.add_argument("--include-smallest-capacity", type=parse_bool, default=True)
    parser.add_argument("--max-book-age-seconds", type=float, default=2.0)
    parser.add_argument("--fee-rate", type=float, default=0.07)
    parser.add_argument("--compare-tolerance", type=float, default=1e-6)
    parser.add_argument("--dry-run", type=parse_bool, default=False)
    return parser


def main(argv: list[str] | None = None) -> int:
    diagnostics = run(build_parser().parse_args(argv))
    print(json.dumps(diagnostics, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
