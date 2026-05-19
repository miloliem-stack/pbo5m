#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import shutil
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.research.chainlink_binance_label_audit import parse_binance_record, parse_chainlink_record
from src.research.execution_realism_replay import levels_from_book, number, utc_ts


SCRIPT_VERSION = "compact_market_recorder_v1"
SCHEMA_VERSION = "compact_market_recorder_schema_v1"
CHAINLINK_TOLERANCE_MS = 2000
STREAM_FILENAMES = {
    "meta": "market_meta.jsonl",
    "quotes": "market_quotes.jsonl",
    "chainlink": "chainlink_prices.jsonl",
    "binance": "binance_prices.jsonl",
    "heartbeat": "recorder_heartbeat.jsonl",
}


def parse_date(value: str | None) -> pd.Timestamp | None:
    if not value:
        return None
    return pd.Timestamp(value, tz="UTC").normalize()


def path_date(path: Path) -> pd.Timestamp | None:
    for part in path.parts:
        if len(part) == 10 and part[4] == "-" and part[7] == "-":
            try:
                return pd.Timestamp(part, tz="UTC").normalize()
            except Exception:
                continue
    return None


def in_date_range(path: Path, start: pd.Timestamp | None, end: pd.Timestamp | None) -> bool:
    dt = path_date(path)
    if dt is None:
        return True
    if start is not None and dt < start:
        return False
    if end is not None and dt > end:
        return False
    return True


def discover_files(root: Path, filename: str, start: pd.Timestamp | None, end: pd.Timestamp | None, max_files: int | None) -> list[Path]:
    if root.is_file():
        files = [root] if root.name == filename else []
    else:
        files = sorted(root.rglob(filename))
    files = [path for path in files if in_date_range(path, start, end)]
    return files[:max_files] if max_files else files


def payload_in_date_range(payload: dict[str, Any], start: pd.Timestamp | None, end: pd.Timestamp | None) -> bool:
    if start is None and end is None:
        return True
    ts = utc_ts(payload.get("ts") or payload.get("received_ts") or payload.get("source_ts") or payload.get("timestamp"))
    if pd.isna(ts):
        return True
    day = pd.Timestamp(ts).tz_convert("UTC").normalize()
    if start is not None and day < start:
        return False
    if end is not None and day > end:
        return False
    return True


def iter_jsonl(paths: Iterable[Path], manifest: dict[str, Any], stream: str, start: pd.Timestamp | None = None, end: pd.Timestamp | None = None):
    for path in paths:
        manifest["raw_files_scanned_by_stream"][stream].append(str(path))
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                manifest["rows_read_by_stream"][stream] += 1
                try:
                    payload = json.loads(line)
                except Exception:
                    manifest["json_errors_by_stream"][stream] += 1
                    continue
                if isinstance(payload, dict):
                    if not payload_in_date_range(payload, start, end):
                        manifest["rows_skipped_by_date_by_stream"][stream] += 1
                        continue
                    yield payload, path


def as_str(value: Any) -> str | None:
    if value in (None, ""):
        return None
    return str(value)


def first_value(*values: Any) -> Any:
    for value in values:
        if value not in (None, ""):
            return value
    return None


def nested_get(obj: dict[str, Any] | None, *keys: str) -> Any:
    cur: Any = obj
    for key in keys:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(key)
    return cur


def extract_market_meta(row: dict[str, Any]) -> dict[str, Any] | None:
    market = row.get("market") if isinstance(row.get("market"), dict) else row
    raw = market.get("raw_market") if isinstance(market.get("raw_market"), dict) else {}
    candidate = raw.get("candidate") if isinstance(raw.get("candidate"), dict) else {}
    payload = raw.get("payload_fragment") if isinstance(raw.get("payload_fragment"), dict) else {}
    start = utc_ts(first_value(market.get("start_time"), market.get("market_start_time"), market.get("start"), market.get("startDate"), candidate.get("eventStartTime"), nested_get(candidate, "events", "startTime")))
    end = utc_ts(first_value(market.get("end_time"), market.get("market_end_time"), market.get("end"), market.get("endDate"), candidate.get("endDate"), payload.get("endDate")))
    slug = as_str(first_value(market.get("slug"), candidate.get("slug"), payload.get("slug")))
    condition_id = as_str(first_value(market.get("condition_id"), candidate.get("conditionId"), payload.get("conditionId")))
    if pd.isna(start) or pd.isna(end) or not slug:
        return None
    reference = number(first_value(market.get("reference_price"), market.get("open_price"), market.get("start_price"), candidate.get("referencePrice"), payload.get("referencePrice")))
    return {
        "market_id": as_str(first_value(market.get("market_id"), market.get("id"), candidate.get("id"), payload.get("id"))),
        "condition_id": condition_id,
        "slug": slug,
        "yes_token_id": as_str(first_value(market.get("token_yes"), market.get("yes_token_id"), candidate.get("token_yes"), payload.get("token_yes"))),
        "no_token_id": as_str(first_value(market.get("token_no"), market.get("no_token_id"), candidate.get("token_no"), payload.get("token_no"))),
        "market_start_ts": start,
        "market_end_ts": end,
        "reference_price": reference,
        "reference_price_source": "metadata" if reference is not None else "chainlink_derived",
    }


def load_market_windows(meta_files: list[Path], manifest: dict[str, Any], start: pd.Timestamp | None, end: pd.Timestamp | None) -> pd.DataFrame:
    rows = []
    for payload, _ in iter_jsonl(meta_files, manifest, "meta", start, end):
        row = extract_market_meta(payload)
        if row:
            rows.append(row)
    frame = pd.DataFrame(rows)
    if frame.empty:
        return pd.DataFrame(columns=["market_key", "market_id", "condition_id", "slug", "yes_token_id", "no_token_id", "market_start_ts", "market_end_ts", "reference_price"])
    frame["market_start_ts"] = pd.to_datetime(frame["market_start_ts"], utc=True, errors="coerce")
    frame["market_end_ts"] = pd.to_datetime(frame["market_end_ts"], utc=True, errors="coerce")
    frame = frame.dropna(subset=["slug", "market_start_ts", "market_end_ts"])
    score = frame[["market_id", "condition_id", "yes_token_id", "no_token_id"]].notna().sum(axis=1)
    frame["_score"] = score
    frame = frame.sort_values(["market_start_ts", "slug", "_score"], ascending=[True, True, False])
    frame = frame.drop_duplicates(["slug", "market_start_ts", "market_end_ts"], keep="first").drop(columns=["_score"]).reset_index(drop=True)
    frame.insert(0, "market_key", np.arange(len(frame), dtype=np.int32))
    return frame


def load_price_stream(files: list[Path], manifest: dict[str, Any], stream: str, start: pd.Timestamp | None, end: pd.Timestamp | None) -> pd.DataFrame:
    rows = []
    parser = parse_chainlink_record if stream == "chainlink" else parse_binance_record
    for payload, path in iter_jsonl(files, manifest, stream, start, end):
        rows.extend(parser(payload, str(path)))
    frame = pd.DataFrame(rows)
    if frame.empty:
        return pd.DataFrame(columns=["timestamp", "price", "source_file", "raw_source_type"])
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True, errors="coerce")
    frame["price"] = pd.to_numeric(frame["price"], errors="coerce")
    frame = frame.dropna(subset=["timestamp", "price"]).drop_duplicates(["timestamp", "price", "raw_source_type"]).sort_values("timestamp").reset_index(drop=True)
    update_ts_manifest(manifest, stream, frame["timestamp"])
    return frame


def choose_price_at_or_before(prices: pd.DataFrame, target: pd.Timestamp, tolerance_ms: int) -> tuple[float | None, pd.Timestamp | pd.NaT, float | None]:
    if prices.empty or pd.isna(target):
        return None, pd.NaT, None
    target = pd.Timestamp(target).tz_convert("UTC")
    before = prices[prices["timestamp"] <= target]
    candidate = before.tail(1)
    if candidate.empty:
        nearby = prices.iloc[(prices["timestamp"] - target).abs().argsort()[:1]]
        candidate = nearby
    if candidate.empty:
        return None, pd.NaT, None
    ts = pd.Timestamp(candidate["timestamp"].iloc[0])
    lag_ms = float(abs((ts - target).total_seconds()) * 1000.0)
    if lag_ms > tolerance_ms:
        return None, ts, lag_ms
    return float(candidate["price"].iloc[0]), ts, lag_ms


def label_market_windows(windows: pd.DataFrame, chainlink: pd.DataFrame, tolerance_ms: int, manifest: dict[str, Any]) -> pd.DataFrame:
    out = windows.copy()
    if out.empty:
        for col in [
            "chainlink_reference_price",
            "chainlink_reference_ts",
            "chainlink_reference_tolerance_ms",
            "chainlink_close_price",
            "chainlink_close_ts",
            "chainlink_close_tolerance_ms",
            "label_up",
            "label_down",
            "winner_side",
            "chainlink_reference_quality",
            "chainlink_close_quality",
        ]:
            out[col] = pd.Series(dtype="object")
        manifest["markets_with_labels"] = 0
        manifest["markets_missing_chainlink_close"] = 0
        manifest["markets_missing_reference_price"] = 0
        return out
    close_prices, close_ts, close_lags = [], [], []
    open_prices, open_ts, open_lags = [], [], []
    for _, row in out.iterrows():
        open_price, open_time, open_lag = choose_price_at_or_before(chainlink, row["market_start_ts"], tolerance_ms)
        close_price, close_time, close_lag = choose_price_at_or_before(chainlink, row["market_end_ts"], tolerance_ms)
        open_prices.append(open_price)
        open_ts.append(open_time)
        open_lags.append(open_lag)
        close_prices.append(close_price)
        close_ts.append(close_time)
        close_lags.append(close_lag)
    out["chainlink_reference_price"] = open_prices
    out["chainlink_reference_ts"] = open_ts
    out["chainlink_reference_tolerance_ms"] = open_lags
    out["chainlink_close_price"] = close_prices
    out["chainlink_close_ts"] = close_ts
    out["chainlink_close_tolerance_ms"] = close_lags
    missing_ref = out["reference_price"].isna()
    out.loc[missing_ref, "reference_price"] = out.loc[missing_ref, "chainlink_reference_price"]
    out.loc[missing_ref & out["reference_price"].notna(), "reference_price_source"] = "chainlink_derived"
    out["label_up"] = np.where(out["chainlink_close_price"].notna() & out["reference_price"].notna(), out["chainlink_close_price"] > out["reference_price"], np.nan)
    out["label_down"] = np.where(out["label_up"].notna(), ~out["label_up"].astype(bool), np.nan)
    out["winner_side"] = np.where(out["label_up"].eq(True), "YES", np.where(out["label_down"].eq(True), "NO", None))
    out["chainlink_reference_quality"] = np.where(out["chainlink_reference_tolerance_ms"].notna() & (out["chainlink_reference_tolerance_ms"] <= tolerance_ms), "ok", "missing_or_outside_tolerance")
    out["chainlink_close_quality"] = np.where(out["chainlink_close_tolerance_ms"].notna() & (out["chainlink_close_tolerance_ms"] <= tolerance_ms), "ok", "missing_or_outside_tolerance")
    manifest["markets_with_labels"] = int(out["label_up"].notna().sum())
    manifest["markets_missing_chainlink_close"] = int(out["chainlink_close_price"].isna().sum())
    manifest["markets_missing_reference_price"] = int(out["reference_price"].isna().sum())
    return out


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


def extract_side_books(row: dict[str, Any], token_map: dict[str, tuple[int, str]]) -> list[tuple[int | None, str | None, dict[str, Any] | None]]:
    raw = row.get("raw_payload_fragment")
    out: list[tuple[int | None, str | None, dict[str, Any] | None]] = []
    yes_book = _book_from_raw(raw, "YES")
    no_book = _book_from_raw(raw, "NO")
    if yes_book is not None or no_book is not None:
        out.append((None, "YES", yes_book))
        out.append((None, "NO", no_book))
        return out
    token = as_str(first_value(row.get("token_id"), row.get("asset_id"), row.get("token"), row.get("clob_token_id")))
    side = as_str(first_value(row.get("outcome"), row.get("side"), row.get("asset_side")))
    side = side.upper() if side else None
    market_key = None
    if token and token in token_map:
        market_key, side = token_map[token]
    book = row.get("book") if isinstance(row.get("book"), dict) else raw if isinstance(raw, dict) else None
    out.append((market_key, side, book))
    return out


def market_lookup(windows: pd.DataFrame) -> tuple[dict[str, int], dict[str, tuple[int, str]]]:
    by_slug: dict[str, int] = {}
    by_token: dict[str, tuple[int, str]] = {}
    for _, row in windows.iterrows():
        key = int(row["market_key"])
        if pd.notna(row.get("slug")):
            by_slug[str(row["slug"])] = key
        if pd.notna(row.get("yes_token_id")):
            by_token[str(row["yes_token_id"])] = (key, "YES")
        if pd.notna(row.get("no_token_id")):
            by_token[str(row["no_token_id"])] = (key, "NO")
    return by_slug, by_token


def top_levels(levels: list[dict[str, float]], n: int) -> list[dict[str, float]]:
    return levels[:n] + [{"price": np.nan, "size": np.nan}] * max(0, n - len(levels))


def compact_quote_rows(row: dict[str, Any], windows_by_key: pd.DataFrame, slug_map: dict[str, int], token_map: dict[str, tuple[int, str]], top_n: int, manifest: dict[str, Any]) -> list[dict[str, Any]]:
    ts = utc_ts(row.get("ts") or row.get("timestamp") or row.get("quote_ts"))
    if pd.isna(ts):
        manifest["invalid_quote_rows"] += 1
        return []
    slug = as_str(row.get("slug") or row.get("market_slug"))
    row_market_key = slug_map.get(slug) if slug else None
    output = []
    for mapped_key, side, book in extract_side_books(row, token_map):
        market_key = mapped_key if mapped_key is not None else row_market_key
        if market_key is None or side not in {"YES", "NO"}:
            manifest["unmapped_quote_rows"] += 1
            continue
        if market_key not in windows_by_key.index:
            manifest["unmapped_quote_rows"] += 1
            continue
        bids = levels_from_book(book, "bids")
        asks = levels_from_book(book, "asks")
        if not bids and not asks:
            manifest["quote_side_rows_without_depth"] += 1
        bid_top = top_levels(bids, top_n)
        ask_top = top_levels(asks, top_n)
        bid1 = bid_top[0]["price"]
        ask1 = ask_top[0]["price"]
        valid = bool(np.isfinite(bid1) and np.isfinite(ask1) and 0 < bid1 <= 1 and 0 < ask1 <= 1 and bid1 <= ask1)
        crossed = bool(np.isfinite(bid1) and np.isfinite(ask1) and bid1 > ask1)
        if crossed:
            manifest["crossed_book_rows"] += 1
        if not valid:
            manifest["invalid_quote_rows"] += 1
        market = windows_by_key.loc[market_key]
        compact = {
            "market_key": np.int32(market_key),
            "ts": ts,
            "side": side,
            "source": as_str(row.get("source") or "unknown"),
            "market_age_sec": np.float32((ts - market["market_start_ts"]).total_seconds()),
            "seconds_to_end": np.float32((market["market_end_ts"] - ts).total_seconds()),
            "mid": np.float32((bid1 + ask1) / 2.0) if valid else np.float32(np.nan),
            "spread": np.float32(ask1 - bid1) if valid else np.float32(np.nan),
            "is_crossed": crossed,
            "is_valid_topbook": valid,
        }
        for idx in range(top_n):
            compact[f"bid_px_{idx+1}"] = np.float32(bid_top[idx]["price"])
            compact[f"bid_sz_{idx+1}"] = np.float32(bid_top[idx]["size"])
            compact[f"ask_px_{idx+1}"] = np.float32(ask_top[idx]["price"])
            compact[f"ask_sz_{idx+1}"] = np.float32(ask_top[idx]["size"])
        output.append(compact)
    return output


@dataclass
class ParquetBatchWriter:
    path: Path
    writer: Any = None
    rows_written: int = 0

    def write(self, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        import pyarrow as pa
        import pyarrow.parquet as pq

        frame = pd.DataFrame(rows)
        table = pa.Table.from_pandas(frame, preserve_index=False)
        if self.writer is None:
            self.writer = pq.ParquetWriter(self.path, table.schema)
        self.writer.write_table(table)
        self.rows_written += len(rows)

    def close(self) -> None:
        if self.writer is not None:
            self.writer.close()
        elif not self.path.exists():
            pd.DataFrame().to_parquet(self.path, index=False)


def update_ts_manifest(manifest: dict[str, Any], stream: str, values: pd.Series) -> None:
    if values.empty:
        return
    mn = pd.to_datetime(values, utc=True, errors="coerce").min()
    mx = pd.to_datetime(values, utc=True, errors="coerce").max()
    manifest["min_max_timestamps_by_stream"][stream] = {"min": None if pd.isna(mn) else str(mn), "max": None if pd.isna(mx) else str(mx)}


def stream_book_ticks(
    quote_files: list[Path],
    output_path: Path,
    windows: pd.DataFrame,
    top_n: int,
    manifest: dict[str, Any],
    start: pd.Timestamp | None,
    end: pd.Timestamp | None,
    batch_size: int = 50_000,
) -> None:
    slug_map, token_map = market_lookup(windows)
    windows_by_key = windows.set_index("market_key")
    writer = ParquetBatchWriter(output_path)
    batch: list[dict[str, Any]] = []
    ts_values = []
    for payload, _ in iter_jsonl(quote_files, manifest, "quotes", start, end):
        rows = compact_quote_rows(payload, windows_by_key, slug_map, token_map, top_n, manifest)
        if rows:
            ts_values.extend(row["ts"] for row in rows)
            batch.extend(rows)
        if len(batch) >= batch_size:
            writer.write(batch)
            batch = []
    writer.write(batch)
    writer.close()
    manifest["rows_written"]["book_ticks"] = int(writer.rows_written)
    update_ts_manifest(manifest, "quotes", pd.Series(ts_values))


def count_heartbeats(paths: list[Path], manifest: dict[str, Any], start: pd.Timestamp | None, end: pd.Timestamp | None) -> None:
    timestamps = []
    for payload, _ in iter_jsonl(paths, manifest, "heartbeat", start, end):
        ts = utc_ts(payload.get("ts") or payload.get("timestamp"))
        if not pd.isna(ts):
            timestamps.append(ts)
    update_ts_manifest(manifest, "heartbeat", pd.Series(timestamps))


def write_parquet(frame: pd.DataFrame, path: Path) -> None:
    frame.to_parquet(path, index=False)


def initial_manifest(args: argparse.Namespace) -> dict[str, Any]:
    return {
        "input_root": str(args.input_root),
        "output_root": str(args.output_root),
        "slice_name": args.slice_name,
        "date_range": {"start_date": args.start_date, "end_date": args.end_date},
        "script_version": SCRIPT_VERSION,
        "schema_version": SCHEMA_VERSION,
        "top_n_levels": args.top_n_levels,
        "chainlink_tolerance_ms": CHAINLINK_TOLERANCE_MS,
        "raw_files_scanned_by_stream": defaultdict(list),
        "rows_read_by_stream": Counter(),
        "json_errors_by_stream": Counter(),
        "rows_skipped_by_date_by_stream": Counter(),
        "rows_written": {},
        "unmapped_quote_rows": 0,
        "invalid_quote_rows": 0,
        "crossed_book_rows": 0,
        "quote_side_rows_without_depth": 0,
        "markets_discovered": 0,
        "markets_with_labels": 0,
        "markets_missing_chainlink_close": 0,
        "markets_missing_reference_price": 0,
        "min_max_timestamps_by_stream": {},
        "output_file_sizes": {},
    }


def finalize_manifest(manifest: dict[str, Any], output_dir: Path) -> dict[str, Any]:
    out = dict(manifest)
    out["raw_files_scanned_by_stream"] = {k: list(v) for k, v in manifest["raw_files_scanned_by_stream"].items()}
    out["rows_read_by_stream"] = dict(manifest["rows_read_by_stream"])
    out["json_errors_by_stream"] = dict(manifest["json_errors_by_stream"])
    out["rows_skipped_by_date_by_stream"] = dict(manifest["rows_skipped_by_date_by_stream"])
    out["output_file_sizes"] = {path.name: path.stat().st_size for path in output_dir.glob("*") if path.is_file()}
    return out


def run(args: argparse.Namespace) -> dict[str, Any]:
    output_dir = Path(args.output_root) / args.slice_name
    if output_dir.exists():
        if not args.overwrite:
            raise FileExistsError(f"{output_dir} exists; pass --overwrite to replace derived compact artifacts")
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    debug_dir = output_dir / "debug_samples"
    if args.write_debug_samples:
        debug_dir.mkdir(parents=True, exist_ok=True)
    start = parse_date(args.start_date)
    end = parse_date(args.end_date)
    manifest = initial_manifest(args)
    files = {stream: discover_files(Path(args.input_root), filename, start, end, args.max_files) for stream, filename in STREAM_FILENAMES.items()}

    windows = load_market_windows(files["meta"], manifest, start, end)
    manifest["markets_discovered"] = int(len(windows))
    chainlink = load_price_stream(files["chainlink"], manifest, "chainlink", start, end)
    _ = load_price_stream(files["binance"], manifest, "binance", start, end) if files["binance"] else pd.DataFrame()
    windows = label_market_windows(windows, chainlink, CHAINLINK_TOLERANCE_MS, manifest)
    write_parquet(windows, output_dir / "market_windows.parquet")
    manifest["rows_written"]["market_windows"] = int(len(windows))
    stream_book_ticks(files["quotes"], output_dir / "book_ticks.parquet", windows, args.top_n_levels, manifest, start, end)
    count_heartbeats(files["heartbeat"], manifest, start, end)
    if args.write_debug_samples:
        windows.head(20).to_json(debug_dir / "market_windows_head.json", orient="records", lines=True, date_format="iso")
        if (output_dir / "book_ticks.parquet").exists():
            pd.read_parquet(output_dir / "book_ticks.parquet").head(20).to_json(debug_dir / "book_ticks_head.json", orient="records", lines=True, date_format="iso")
    final = finalize_manifest(manifest, output_dir)
    (output_dir / "compact_manifest.json").write_text(json.dumps(final, indent=2, default=str), encoding="utf-8")
    if args.strict:
        severe = []
        if final["markets_discovered"] == 0:
            severe.append("no_markets_discovered")
        if final["markets_with_labels"] == 0:
            severe.append("no_chainlink_labels")
        if final["rows_written"].get("book_ticks", 0) == 0:
            severe.append("no_book_ticks")
        if severe:
            hint = ""
            if "no_book_ticks" in severe:
                hint = (
                    "; no compact book ticks were written. Check compact_manifest.json: if quotes were read but "
                    "quote_side_rows_without_depth/unmapped_quote_rows is high, the raw quote rows likely contain no usable "
                    "raw CLOB book depth for the requested date range. Use data/market_recorder for rotated May data, or run "
                    "without --strict if you intentionally want market_windows only."
                )
            raise RuntimeError(f"strict compact build failed: {severe}{hint}")
    return final


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build compact offline BTC-5m market-recorder Parquet replay dataset.")
    parser.add_argument("--input-root", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--slice-name", required=True)
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    parser.add_argument("--top-n-levels", type=int, default=3)
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--max-files", type=int)
    parser.add_argument("--strict", action="store_true")
    parser.add_argument("--write-debug-samples", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    manifest = run(build_parser().parse_args(argv))
    print(json.dumps(manifest, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
