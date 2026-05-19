import argparse
import json

import pandas as pd
import pytest

from scripts import replay_probability_edge_vs_quotes as replay
from src.research import terminal_conviction as tc


def _book(bids=None, asks=None):
    return {"bids": bids or [], "asks": asks or []}


def test_best_ask_extraction_from_unsorted_orderbook_asks():
    book = _book(asks=[{"price": "0.91", "size": "5"}, {"price": "0.87", "size": "2"}, {"price": "0.89", "size": "3"}])
    assert tc.best_ask_from_book(book) == pytest.approx(0.87)
    assert tc.best_ask_level_from_book(book) == pytest.approx((0.87, 2.0))


def test_best_bid_extraction_from_unsorted_orderbook_bids():
    book = _book(bids=[{"price": "0.02", "size": "5"}, {"price": "0.11", "size": "2"}, {"price": "0.05", "size": "3"}])
    assert tc.best_bid_from_book(book) == pytest.approx(0.11)
    assert tc.best_bid_level_from_book(book) == pytest.approx((0.11, 2.0))


def test_parser_does_not_use_first_array_element_as_best_price():
    row = {
        "ts": "2026-04-25T00:55:10Z",
        "slug": "btc-updown-5m-1777078500",
        "market_start_time": "2026-04-25T00:55:00Z",
        "market_end_time": "2026-04-25T01:00:00Z",
        "raw_payload_fragment": {
            "yes_raw": {"book": _book(bids=[{"price": "0.01", "size": "1"}, {"price": "0.99", "size": "1"}], asks=[{"price": "0.80", "size": "1"}, {"price": "0.70", "size": "1"}])},
            "no_raw": {"book": _book(bids=[{"price": "0.03", "size": "1"}], asks=[{"price": "0.99", "size": "1"}, {"price": "0.01", "size": "1"}])},
        },
    }
    parsed = tc.normalize_quote_record(row, 300)
    assert parsed["yes_bid"] == pytest.approx(0.99)
    assert parsed["yes_ask"] == pytest.approx(0.70)
    assert parsed["no_ask"] == pytest.approx(0.01)


def _predictions():
    return pd.DataFrame(
        {
            "model": ["m", "m"],
            "prediction_ts": pd.to_datetime(["2026-01-01T00:00:02Z", "2026-01-01T00:00:10Z"], utc=True),
            "market_start_ts": pd.to_datetime(["2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"], utc=True),
            "market_end_ts": pd.to_datetime(["2026-01-01T00:05:00Z", "2026-01-01T00:05:00Z"], utc=True),
            "market_age_seconds": [2.0, 10.0],
            "p_up": [0.8, 0.2],
            "result_up": [1.0, 0.0],
            "market_start_key": ["2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"],
        }
    )


def _quotes():
    return pd.DataFrame(
        {
            "market_start_key": ["2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"],
            "quote_ts": pd.to_datetime(["2026-01-01T00:00:03Z", "2026-01-01T00:00:20Z"], utc=True),
            "market_key": ["slug", "slug"],
            "market_slug": ["slug", "slug"],
            "yes_ask": [0.55, 0.6],
            "no_ask": [0.6, 0.55],
            "yes_ask_size": [10.0, 10.0],
            "no_ask_size": [10.0, 10.0],
            "yes_bid": [0.5, 0.4],
            "no_bid": [0.4, 0.5],
        }
    )


def test_quote_join_picks_nearest_quote_within_tolerance():
    joined = replay.join_nearest_quotes(_predictions().iloc[[0]], _quotes(), tolerance_seconds=3)
    assert joined["quote_join_status"].iloc[0] == "joined"
    assert joined["quote_lag_seconds"].iloc[0] == pytest.approx(1.0)


def test_quote_join_rejects_quote_outside_tolerance():
    joined = replay.join_nearest_quotes(_predictions().iloc[[1]], _quotes().iloc[[0]], tolerance_seconds=3)
    assert joined["quote_join_status"].iloc[0] == "missing_quote"


def test_yes_trade_selected_when_edge_clears_threshold():
    opp = replay.make_opportunities(replay.join_nearest_quotes(_predictions().iloc[[0]], _quotes(), 3))
    trades = replay.expand_trades(opp, [0.2], 0.01, 0.99, 0, 0, 0)
    assert trades["side"].iloc[0] == "YES"
    assert trades["predicted_edge"].iloc[0] == pytest.approx(0.25)


def test_no_trade_selected_when_edge_clears_threshold():
    opp = replay.make_opportunities(replay.join_nearest_quotes(_predictions().iloc[[1]], _quotes(), 15))
    trades = replay.expand_trades(opp, [0.2], 0.01, 0.99, 0, 0, 0)
    assert trades["side"].iloc[0] == "NO"
    assert trades["predicted_edge"].iloc[0] == pytest.approx(0.20)


def test_larger_edge_selected_when_both_sides_qualify():
    opp = pd.DataFrame(
        {
            "quote_join_status": ["joined"],
            "model": ["m"],
            "market_start_key": ["k"],
            "prediction_ts": pd.to_datetime(["2026-01-01T00:00:01Z"], utc=True),
            "market_age_seconds": [1],
            "p_up": [0.52],
            "result_up": [1.0],
            "yes_ask": [0.48],
            "no_ask": [0.45],
            "yes_edge": [0.04],
            "no_edge": [0.03],
            "quote_ts": pd.to_datetime(["2026-01-01T00:00:01Z"], utc=True),
            "quote_lag_seconds": [0],
        }
    )
    trades = replay.expand_trades(opp, [0.01], 0.01, 0.99, 0, 0, 0)
    assert trades["side"].iloc[0] == "YES"


@pytest.mark.parametrize(
    ("side", "result_up", "price", "expected"),
    [("YES", 1.0, 0.4, 0.6), ("YES", 0.0, 0.4, -0.4), ("NO", 0.0, 0.3, 0.7), ("NO", 1.0, 0.3, -0.3)],
)
def test_pnl_calculation(side, result_up, price, expected):
    opp = pd.DataFrame(
        {
            "quote_join_status": ["joined"],
            "model": ["m"],
            "market_start_key": ["k"],
            "prediction_ts": pd.to_datetime(["2026-01-01T00:00:01Z"], utc=True),
            "market_age_seconds": [1],
            "p_up": [0.8 if side == "YES" else 0.2],
            "result_up": [result_up],
            "yes_ask": [price if side == "YES" else 0.99],
            "no_ask": [price if side == "NO" else 0.99],
            "yes_edge": [1.0 if side == "YES" else -1.0],
            "no_edge": [1.0 if side == "NO" else -1.0],
            "quote_ts": pd.to_datetime(["2026-01-01T00:00:01Z"], utc=True),
            "quote_lag_seconds": [0],
        }
    )
    trade = replay.expand_trades(opp, [0.0], 0.01, 0.99, 0, 0, 0)
    assert trade["pnl_per_contract"].iloc[0] == pytest.approx(expected)


def test_age_window_assignment():
    ages = pd.Series([0, 59.9, 120, 179.9, 218, 239.9, 240, 299])
    assert replay.window_mask(ages, "pre_218").tolist() == [True, True, True, True, False, False, False, False]
    assert replay.window_mask(ages, "218_240").tolist() == [False, False, False, False, True, True, False, False]


def test_summary_aggregation_by_model_window_threshold():
    opp = replay.make_opportunities(replay.join_nearest_quotes(_predictions(), _quotes(), 15))
    trades = replay.expand_trades(opp, [0.2], 0.01, 0.99, 0, 0, 0)
    summary = replay.summary_by_model_window_threshold(trades, opp, ["full_window"], [0.2])
    assert summary["trades"].iloc[0] == 2
    assert summary["timing_window"].iloc[0] == "full_window"


def test_clear_failure_when_expected_columns_missing():
    with pytest.raises(ValueError, match="Could not detect probability column"):
        replay.normalize_predictions(pd.DataFrame({"model_id": ["m"]}), ["m"], 300)


def test_cli_smoke_on_tiny_fixture(tmp_path):
    pred = pd.DataFrame(
        {
            "timestamp": ["2026-01-01T00:00:02Z"],
            "market_window_start": ["2026-01-01T00:00:00Z"],
            "market_window_end": ["2026-01-01T00:05:00Z"],
            "market_age_seconds": [2],
            "model_id": ["baseline_50"],
            "p_up": [0.8],
            "result_up": [1],
            "fold_id": [0],
        }
    )
    pred_path = tmp_path / "pred.csv"
    pred.to_csv(pred_path, index=False)
    quote_dir = tmp_path / "quotes"
    quote_dir.mkdir()
    row = {
        "ts": "2026-01-01T00:00:03Z",
        "slug": "btc-updown-5m-1767225600",
        "market_start_time": "2026-01-01T00:00:00Z",
        "market_end_time": "2026-01-01T00:05:00Z",
        "raw_payload_fragment": {
            "yes_raw": {"book": _book(asks=[{"price": "0.55", "size": "10"}])},
            "no_raw": {"book": _book(asks=[{"price": "0.60", "size": "10"}])},
        },
    }
    (quote_dir / "market_quotes.jsonl").write_text(json.dumps(row) + "\n", encoding="utf-8")
    out = tmp_path / "out"
    diagnostics = replay.run(
        argparse.Namespace(
            predictions=pred_path,
            quotes=quote_dir,
            market_meta=None,
            output_dir=out,
            models="baseline_50",
            edge_thresholds="0.01",
            quote_tolerance_seconds=3,
            market_window_seconds=300,
            min_ask_size=0,
            max_entry_price=0.99,
            min_entry_price=0.01,
            fee_bps=0,
            slippage_bps=0,
            windows="default",
            allow_post_end_quotes=False,
            dry_run=False,
        )
    )
    assert diagnostics["quote_join_rate"] == pytest.approx(1.0)
    assert (out / "replay_summary_by_model_window_threshold.csv").exists()
