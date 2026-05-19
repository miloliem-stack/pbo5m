from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


CHAINLINK_FILENAMES = {"chainlink_prices.jsonl"}
BINANCE_FILENAMES = {"binance_prices.jsonl"}


def parse_csv_floats(value: str) -> list[float]:
    return [float(item.strip()) for item in str(value).split(",") if item.strip()]


def utc_ts(value: Any) -> pd.Timestamp | pd.NaT:
    if value is None or value == "":
        return pd.NaT
    return pd.to_datetime(value, utc=True, errors="coerce")


def number(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        result = float(value)
    except Exception:
        return None
    return result if np.isfinite(result) else None


def discover_jsonl(path: Path, filenames: set[str]) -> list[Path]:
    if not path.exists():
        raise FileNotFoundError(path)
    if path.is_file():
        return [path]
    return [child for child in sorted(path.rglob("*.jsonl")) if child.name in filenames]


def timestamp_from_ms(value: Any) -> pd.Timestamp | pd.NaT:
    n = number(value)
    if n is None:
        return pd.NaT
    return pd.to_datetime(int(n), unit="ms", utc=True, errors="coerce")


def _full_accuracy_to_price(value: Any) -> float | None:
    n = number(value)
    if n is None:
        return None
    return n / 1e18


def parse_chainlink_record(row: dict[str, Any], source_file: str = "") -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    payload = row.get("payload") if isinstance(row.get("payload"), dict) else None
    raw = row.get("raw_payload_fragment") if isinstance(row.get("raw_payload_fragment"), dict) else {}
    raw_payload = raw.get("payload") if isinstance(raw.get("payload"), dict) else {}
    data = None
    if payload and isinstance(payload.get("data"), list):
        data = payload.get("data")
    elif isinstance(raw_payload.get("data"), list):
        data = raw_payload.get("data")
    if data is not None:
        for item in data:
            if not isinstance(item, dict):
                continue
            price = number(item.get("value") or item.get("price"))
            ts = timestamp_from_ms(item.get("timestamp")) or utc_ts(item.get("ts"))
            if price is not None and not pd.isna(ts):
                rows.append({"timestamp": ts, "price": price, "source_file": source_file, "raw_source_type": "payload.data"})
    price = number(row.get("price") or row.get("value"))
    full = row.get("full_accuracy_value") or raw_payload.get("full_accuracy_value")
    if price is None:
        price = _full_accuracy_to_price(full)
    ts = utc_ts(row.get("source_ts")) if row.get("source_ts") else pd.NaT
    if pd.isna(ts):
        ts = timestamp_from_ms(raw_payload.get("timestamp")) if raw_payload else pd.NaT
    if pd.isna(ts):
        ts = utc_ts(row.get("ts") or row.get("received_ts") or row.get("timestamp"))
    if price is not None and not pd.isna(ts):
        kind = "payload.full_accuracy_value" if full is not None else "flat"
        rows.append({"timestamp": ts, "price": price, "source_file": source_file, "raw_source_type": kind})
    return rows


def parse_binance_record(row: dict[str, Any], source_file: str = "") -> list[dict[str, Any]]:
    price = number(row.get("price"))
    raw = row.get("raw_payload_fragment") if isinstance(row.get("raw_payload_fragment"), dict) else {}
    if price is None:
        price = number(raw.get("p") or row.get("close"))
    ts = timestamp_from_ms(row.get("observed_at") or raw.get("T") or raw.get("E"))
    if pd.isna(ts):
        ts = utc_ts(row.get("ts") or row.get("timestamp"))
    if price is None or pd.isna(ts):
        return []
    return [{"timestamp": ts, "price": price, "source_file": source_file, "raw_source_type": str(row.get("source") or "binance")}]


def load_price_jsonl(root: Path, filenames: set[str], parser) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for file in discover_jsonl(root, filenames):
        with file.open("r", encoding="utf-8") as handle:
            for line in handle:
                try:
                    payload = json.loads(line)
                except Exception:
                    continue
                if isinstance(payload, dict):
                    rows.extend(parser(payload, str(file)))
    frame = pd.DataFrame(rows)
    if frame.empty:
        return frame
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True, errors="coerce")
    frame["price"] = pd.to_numeric(frame["price"], errors="coerce")
    return frame.dropna(subset=["timestamp", "price"]).drop_duplicates(["timestamp", "price", "raw_source_type"]).sort_values("timestamp").reset_index(drop=True)


def load_predictions_markets(path: Path) -> tuple[pd.DataFrame, dict[str, Any]]:
    if path.suffix.lower() == ".parquet":
        cols = pd.read_parquet(path, columns=None).columns.tolist()
        use = [c for c in ["market_window_start", "market_window_end", "K", "S_end", "timestamp", "market_key", "slug", "market_id"] if c in cols]
        raw = pd.read_parquet(path, columns=use)
    else:
        raw = pd.read_csv(path)
    start_col = "market_window_start" if "market_window_start" in raw.columns else "market_start_ts"
    end_col = "market_window_end" if "market_window_end" in raw.columns else "market_end_ts"
    if start_col not in raw.columns or end_col not in raw.columns:
        raise ValueError(f"predictions need market window start/end columns; available={list(raw.columns)}")
    out = pd.DataFrame()
    out["market_window_start"] = pd.to_datetime(raw[start_col], utc=True, errors="coerce")
    out["market_window_end"] = pd.to_datetime(raw[end_col], utc=True, errors="coerce")
    if "K" in raw.columns:
        out["strike_price"] = pd.to_numeric(raw["K"], errors="coerce")
        strike_source = "predictions.K"
    else:
        raise ValueError("missing strike/reference price; expected predictions column K")
    if "S_end" in raw.columns:
        out["prediction_binance_end_price"] = pd.to_numeric(raw["S_end"], errors="coerce")
    out["market_key"] = out["market_window_start"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    out = out.dropna(subset=["market_window_start", "market_window_end"]).drop_duplicates("market_key").sort_values("market_window_start").reset_index(drop=True)
    return out, {"strike_source": strike_source, "market_count": int(len(out))}


def nearest_at_times(markets: pd.DataFrame, prices: pd.DataFrame, prefix: str, tolerance_seconds: float) -> pd.DataFrame:
    out = markets[["market_key", "market_window_end"]].copy()
    out["market_window_end"] = pd.to_datetime(out["market_window_end"], utc=True, errors="coerce").astype("datetime64[ns, UTC]")
    if prices.empty:
        out[f"{prefix}_end_price"] = np.nan
        out[f"{prefix}_end_ts"] = pd.NaT
        out[f"{prefix}_end_lag_seconds"] = np.nan
        return out
    right = prices[["timestamp", "price"]].copy()
    right["timestamp"] = pd.to_datetime(right["timestamp"], utc=True, errors="coerce").astype("datetime64[ns, UTC]")
    joined = pd.merge_asof(
        out.sort_values("market_window_end"),
        right.sort_values("timestamp"),
        left_on="market_window_end",
        right_on="timestamp",
        direction="nearest",
        tolerance=pd.Timedelta(seconds=tolerance_seconds),
    )
    joined = joined.rename(columns={"price": f"{prefix}_end_price", "timestamp": f"{prefix}_end_ts"})
    joined[f"{prefix}_end_lag_seconds"] = (joined[f"{prefix}_end_ts"] - joined["market_window_end"]).dt.total_seconds()
    return joined.drop(columns=["market_window_end"])


def margin_band(abs_margin: float, bands: list[float]) -> str:
    if pd.isna(abs_margin):
        return "missing"
    prev = 0.0
    for band in bands:
        if abs_margin <= band:
            return f"{prev:g}_{band:g}"
        prev = band
    return f"gt_{bands[-1]:g}"


def build_label_audit(
    predictions_path: Path,
    binance_root: Path,
    chainlink_root: Path,
    *,
    chainlink_tolerance_seconds: float,
    binance_tolerance_seconds: float,
    terminal_margin_bands: list[float],
) -> tuple[pd.DataFrame, dict[str, Any]]:
    markets, manifest = load_predictions_markets(predictions_path)
    chainlink = load_price_jsonl(chainlink_root, CHAINLINK_FILENAMES, parse_chainlink_record)
    binance = load_price_jsonl(binance_root, BINANCE_FILENAMES, parse_binance_record)
    if binance.empty and "prediction_binance_end_price" in markets.columns:
        binance_join = markets[["market_key", "prediction_binance_end_price"]].rename(columns={"prediction_binance_end_price": "binance_end_price"})
        binance_join["binance_end_ts"] = markets["market_window_end"]
        binance_join["binance_end_lag_seconds"] = 0.0
        binance_source = "predictions.S_end"
    else:
        binance_join = nearest_at_times(markets, binance, "binance", binance_tolerance_seconds)
        binance_source = "binance_prices.jsonl"
    chainlink_join = nearest_at_times(markets, chainlink, "chainlink", chainlink_tolerance_seconds)
    audit = markets.merge(binance_join, on="market_key", how="left").merge(chainlink_join, on="market_key", how="left")
    audit["binance_label_up"] = np.where(audit["binance_end_price"] > audit["strike_price"], 1.0, np.where(audit["binance_end_price"].notna(), 0.0, np.nan))
    audit["chainlink_label_up"] = np.where(audit["chainlink_end_price"] > audit["strike_price"], 1.0, np.where(audit["chainlink_end_price"].notna(), 0.0, np.nan))
    audit["label_agree"] = np.where(audit["binance_label_up"].notna() & audit["chainlink_label_up"].notna(), audit["binance_label_up"].eq(audit["chainlink_label_up"]), np.nan)
    audit["binance_terminal_margin_usd"] = audit["binance_end_price"] - audit["strike_price"]
    audit["chainlink_terminal_margin_usd"] = audit["chainlink_end_price"] - audit["strike_price"]
    audit["abs_binance_terminal_margin_usd"] = audit["binance_terminal_margin_usd"].abs()
    audit["abs_chainlink_terminal_margin_usd"] = audit["chainlink_terminal_margin_usd"].abs()
    for band in terminal_margin_bands:
        audit[f"binance_clean_band_{band:g}"] = audit["abs_binance_terminal_margin_usd"] > band
        audit[f"chainlink_clean_band_{band:g}"] = audit["abs_chainlink_terminal_margin_usd"] > band
    audit["binance_terminal_margin_band"] = audit["abs_binance_terminal_margin_usd"].map(lambda x: margin_band(x, terminal_margin_bands))
    audit["chainlink_terminal_margin_band"] = audit["abs_chainlink_terminal_margin_usd"].map(lambda x: margin_band(x, terminal_margin_bands))
    status = np.full(len(audit), "ok", dtype=object)
    status[audit["strike_price"].isna().to_numpy()] = "missing_strike"
    status[audit["binance_end_price"].isna().to_numpy()] = "missing_binance"
    status[audit["chainlink_end_price"].isna().to_numpy()] = "missing_chainlink"
    status[(audit["binance_end_price"].isna() & audit["chainlink_end_price"].isna()).to_numpy()] = "outside_tolerance"
    audit["label_source_status"] = status
    diagnostics = {
        **manifest,
        "binance_source": binance_source,
        "chainlink_rows": int(len(chainlink)),
        "binance_rows": int(len(binance)),
        "chainlink_tolerance_seconds": chainlink_tolerance_seconds,
        "binance_tolerance_seconds": binance_tolerance_seconds,
    }
    return audit, diagnostics


def summarize_audit(audit: pd.DataFrame, bands: list[float], diagnostics: dict[str, Any]) -> dict[str, Any]:
    both = audit[audit["binance_label_up"].notna() & audit["chainlink_label_up"].notna()]
    summary: dict[str, Any] = {
        **diagnostics,
        "markets_total": int(len(audit)),
        "markets_with_binance_label": int(audit["binance_label_up"].notna().sum()),
        "markets_with_chainlink_label": int(audit["chainlink_label_up"].notna().sum()),
        "markets_with_both_labels": int(len(both)),
        "label_agreement_rate": float(both["label_agree"].mean()) if len(both) else None,
        "label_disagreement_rate": float((~both["label_agree"].astype(bool)).mean()) if len(both) else None,
        "missing_chainlink_count": int(audit["chainlink_label_up"].isna().sum()),
        "missing_binance_count": int(audit["binance_label_up"].isna().sum()),
        "chainlink_lag_quantiles": audit["chainlink_end_lag_seconds"].dropna().abs().quantile([0.5, 0.9, 0.99]).to_dict(),
        "binance_lag_quantiles": audit["binance_end_lag_seconds"].dropna().abs().quantile([0.5, 0.9, 0.99]).to_dict(),
    }
    return summary


def agreement_by_band(audit: pd.DataFrame, source: str) -> pd.DataFrame:
    band_col = f"{source}_terminal_margin_band"
    if audit.empty or band_col not in audit:
        return pd.DataFrame()
    rows = []
    for band, group in audit.groupby(band_col, dropna=False):
        both = group[group["label_agree"].notna()]
        rows.append(
            {
                "source": source,
                "terminal_margin_band": band,
                "markets": int(len(group)),
                "markets_with_both_labels": int(len(both)),
                "label_agreement_rate": float(both["label_agree"].mean()) if len(both) else None,
                "label_disagreement_rate": float((~both["label_agree"].astype(bool)).mean()) if len(both) else None,
            }
        )
    return pd.DataFrame(rows)
