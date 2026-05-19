import argparse
import json

import numpy as np
import pandas as pd
import pytest

from scripts import research_terminal_conviction_diagnostics as cli
from src.research import terminal_conviction as tc


def _quote(ts: str, yes_mid: float | None, no_mid: float | None, yes_bid: float | None = None, no_bid: float | None = None) -> dict:
    return {
        "ts": ts,
        "record_type": "quote_snapshot",
        "market_id": "m1",
        "slug": "btc-updown-5m-1777251300",
        "market_start_time": "2026-04-27T00:55:00Z",
        "market_end_time": "2026-04-27T01:00:00Z",
        "yes": {
            "best_bid": yes_bid if yes_bid is not None else (None if yes_mid is None else yes_mid - 0.01),
            "best_ask": None if yes_mid is None else yes_mid + 0.01,
            "mid": yes_mid,
            "spread": 0.02 if yes_mid is not None else None,
        },
        "no": {
            "best_bid": no_bid if no_bid is not None else (None if no_mid is None else no_mid - 0.01),
            "best_ask": None if no_mid is None else no_mid + 0.01,
            "mid": no_mid,
            "spread": 0.02 if no_mid is not None else None,
        },
    }


def _frame(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame([tc.normalize_quote_record(row, 300) for row in rows])


def _conviction(quotes: pd.DataFrame, threshold: float = 0.8, source: str = "mid", definition: str = "strict") -> pd.DataFrame:
    by_market, _, _ = tc.compute_terminal_convictions(
        quotes,
        thresholds=[threshold],
        sources=[source],
        definitions=[definition],
        max_spread=0.15,
        mid_complement_tolerance=0.10,
        min_later_share=0.95,
        tolerant_floor_offset=0.0,
        min_later_quotes=2,
        min_quality_quotes_per_market=2,
    )
    return by_market


def test_strict_conviction_found_when_yes_crosses_and_never_falls():
    rows = [
        _quote("2026-04-27T00:55:00Z", 0.55, 0.45),
        _quote("2026-04-27T00:56:00Z", 0.81, 0.19),
        _quote("2026-04-27T00:57:00Z", 0.83, 0.17),
        _quote("2026-04-27T00:58:00Z", 0.90, 0.10),
    ]
    out = _conviction(_frame(rows))
    row = out.iloc[0]
    assert row["reached_terminal_conviction"] is True or row["reached_terminal_conviction"] == True
    assert row["convicted_side"] == "YES"
    assert row["terminal_conviction_ts"] == pd.Timestamp("2026-04-27T00:56:00Z")


def test_strict_conviction_rejected_when_yes_later_falls_below():
    rows = [
        _quote("2026-04-27T00:55:00Z", 0.55, 0.45),
        _quote("2026-04-27T00:56:00Z", 0.82, 0.18),
        _quote("2026-04-27T00:57:00Z", 0.79, 0.21),
        _quote("2026-04-27T00:58:00Z", 0.83, 0.17),
    ]
    out = _conviction(_frame(rows))
    assert out.iloc[0]["reached_terminal_conviction"] == False


def test_tolerant_conviction_accepts_95pct_later_above_floor():
    rows = [_quote("2026-04-27T00:55:00Z", 0.81, 0.19)]
    for i in range(1, 20):
        rows.append(_quote(f"2026-04-27T00:55:{i:02d}Z", 0.81, 0.19))
    rows.append(_quote("2026-04-27T00:56:00Z", 0.79, 0.21))
    out = _conviction(_frame(rows), definition="tolerant")
    assert out.iloc[0]["reached_terminal_conviction"] == True


def test_bid_vs_mid_source_produce_different_conviction_times():
    rows = [
        _quote("2026-04-27T00:55:00Z", 0.79, 0.21, yes_bid=0.70, no_bid=0.20),
        _quote("2026-04-27T00:56:00Z", 0.81, 0.19, yes_bid=0.79, no_bid=0.18),
        _quote("2026-04-27T00:57:00Z", 0.83, 0.17, yes_bid=0.81, no_bid=0.16),
        _quote("2026-04-27T00:58:00Z", 0.85, 0.15, yes_bid=0.83, no_bid=0.14),
    ]
    mid = _conviction(_frame(rows), source="mid").iloc[0]
    bid = _conviction(_frame(rows), source="bid").iloc[0]
    assert mid["terminal_conviction_ts"] == pd.Timestamp("2026-04-27T00:56:00Z")
    assert bid["terminal_conviction_ts"] == pd.Timestamp("2026-04-27T00:57:00Z")


def test_never_convicted_market_is_represented():
    rows = [
        _quote("2026-04-27T00:55:00Z", 0.55, 0.45),
        _quote("2026-04-27T00:56:00Z", 0.60, 0.40),
        _quote("2026-04-27T00:57:00Z", 0.65, 0.35),
    ]
    out = _conviction(_frame(rows))
    assert out.iloc[0]["reached_terminal_conviction"] == False
    assert pd.isna(out.iloc[0]["terminal_conviction_ts"])


def test_prediction_rows_before_after_conviction_are_classified():
    convictions = pd.DataFrame(
        {
            "market_key": ["m1"],
            "threshold": [0.8],
            "price_source": ["mid"],
            "conviction_definition": ["strict"],
            "terminal_conviction_ts": [pd.Timestamp("2026-01-01T00:02:00Z")],
            "convicted_side": ["YES"],
            "reached_terminal_conviction": [True],
        }
    )
    preds = pd.DataFrame(
        {
            "market_key": ["m1", "m1"],
            "prediction_ts": pd.to_datetime(["2026-01-01T00:01:00Z", "2026-01-01T00:03:00Z"], utc=True),
            "model": ["a", "a"],
            "p_up": [0.6, 0.9],
            "y_true": [1, 1],
            "market_age_seconds": [60, 180],
        }
    )
    joined = tc.join_predictions_to_convictions(preds, convictions, 300)
    assert joined["prediction_phase"].tolist() == ["pre_conviction", "post_conviction"]


def test_prediction_join_handles_all_nat_conviction_timestamp_dtype():
    convictions = pd.DataFrame(
        {
            "market_key": ["m1"],
            "threshold": [0.8],
            "price_source": ["mid"],
            "conviction_definition": ["strict"],
            "terminal_conviction_ts": [pd.NaT],
            "convicted_side": [None],
            "reached_terminal_conviction": [False],
        }
    )
    preds = pd.DataFrame(
        {
            "market_key": ["m1"],
            "prediction_ts": pd.to_datetime(["2026-01-01T00:01:00Z"], utc=True),
            "model": ["a"],
            "p_up": [0.6],
            "y_true": [1],
            "market_age_seconds": [60],
        }
    )
    joined = tc.join_predictions_to_convictions(preds, convictions, 300)
    assert joined["prediction_phase"].tolist() == ["never_convicted"]


def test_malformed_quote_rows_do_not_crash_and_emit_diagnostics():
    frame = _frame(
        [
            {"record_type": "warning", "ts": None},
            _quote("2026-04-27T00:55:00Z", 0.55, 0.45),
            _quote("2026-04-27T00:56:00Z", None, None),
        ]
    )
    filtered, counts = tc.quality_filter_quotes(frame, price_source="mid", max_spread=0.15, mid_complement_tolerance=0.10)
    assert len(filtered) == 1
    assert counts["missing_timestamp"] >= 1
    assert counts["missing_mid"] >= 1


def test_ece_brier_logloss_are_numerically_safe():
    y = np.asarray([1.0, 0.0])
    p = np.asarray([1.0, 0.0])
    assert tc.brier(y, p) < 1e-10
    assert np.isfinite(tc.log_loss(y, p))
    assert np.isfinite(tc.ece(y, p))


def test_cli_smoke_writes_outputs(tmp_path):
    quotes = tmp_path / "market_quotes.jsonl"
    rows = [
        _quote("2026-04-27T00:55:00Z", 0.55, 0.45),
        _quote("2026-04-27T00:56:00Z", 0.81, 0.19),
        _quote("2026-04-27T00:57:00Z", 0.83, 0.17),
    ]
    quotes.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")
    preds = tmp_path / "predictions.csv"
    pd.DataFrame(
        {
            "market_key": ["btc-updown-5m-1777251300", "btc-updown-5m-1777251300"],
            "timestamp": ["2026-04-27T00:55:30Z", "2026-04-27T00:56:30Z"],
            "model_id": ["m", "m"],
            "p_up": [0.55, 0.85],
            "result_up": [1, 1],
            "market_age_seconds": [30, 90],
        }
    ).to_csv(preds, index=False)
    out = tmp_path / "out"
    cli.run(
        argparse.Namespace(
            quotes=quotes,
            market_meta=None,
            predictions=preds,
            model_summary=None,
            output_dir=out,
            thresholds="0.80",
            sources="mid",
            definitions="strict",
            max_spread=0.15,
            mid_complement_tolerance=0.10,
            min_later_share=0.95,
            tolerant_floor_offset=0.0,
            min_quality_quotes_per_market=2,
            min_later_quotes=2,
            market_window_seconds=300,
            prediction_time_column="auto",
            prediction_market_key="auto",
            dry_run=False,
        )
    )
    for name in [
        "terminal_conviction_by_market.csv",
        "terminal_conviction_summary.csv",
        "terminal_conviction_summary.json",
        "quote_quality_diagnostics.csv",
        "quote_quality_diagnostics.json",
        "model_metrics_by_conviction_phase.csv",
        "model_metrics_by_conviction_phase_and_age.csv",
        "conviction_readme_summary.txt",
    ]:
        assert (out / name).exists()
