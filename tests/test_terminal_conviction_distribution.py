import argparse
import json

import pandas as pd
import pytest

from scripts import research_terminal_conviction_distribution as dist_cli
from src.research import terminal_conviction as tc


def _row(ts: str, yes, no, **kwargs):
    row = {
        "ts": ts,
        "slug": "btc-updown-5m-1777251300",
        "market_start_time": "2026-04-27T00:55:00Z",
        "market_end_time": "2026-04-27T01:00:00Z",
        "yes": yes,
        "no": no,
    }
    row.update(kwargs)
    return row


def _book_row(ts: str):
    return _row(
        ts,
        {"best_bid": 0.01, "best_ask": None, "mid": None},
        {"best_bid": None, "best_ask": 0.99, "mid": None},
        raw_payload_fragment={
            "yes_raw": {
                "book": {
                    "bids": [{"price": "0.01", "size": "1"}, {"price": "0.80", "size": "1"}, {"price": "0.99", "size": "1"}],
                    "asks": [],
                    "last_trade_price": "0.99",
                }
            },
            "no_raw": {
                "book": {
                    "bids": [],
                    "asks": [{"price": "0.99", "size": "1"}, {"price": "0.20", "size": "1"}, {"price": "0.01", "size": "1"}],
                    "last_trade_price": "0.99",
                }
            },
        },
    )


def _normalize(rows):
    return pd.DataFrame([tc.normalize_quote_record(row, 300) for row in rows])


def _by_market(rows, threshold=0.8, source="mid", definition="strict", min_later_share=0.95):
    frame = _normalize(rows)
    by_market, _ = tc.compute_terminal_conviction_distribution_rows(
        frame,
        thresholds=[threshold],
        sources=[source],
        definitions=[definition],
        disable_spread_filter=True,
        max_spread=None,
        mid_complement_tolerance=None,
        min_later_share=min_later_share,
        tolerant_floor_offset=0.02,
        min_later_quotes=2,
        min_quality_quotes_per_market=2,
        max_post_end_lag_seconds=0.0,
    )
    return by_market


def test_parser_handles_numeric_yes_no_fields():
    frame = _normalize([_row("2026-04-27T00:55:00Z", 0.7, 0.3)])
    assert frame.iloc[0]["yes_mid"] == pytest.approx(0.7)
    assert frame.iloc[0]["yes_last"] == pytest.approx(0.7)
    assert frame.iloc[0]["no_mid"] == pytest.approx(0.3)


def test_parser_handles_nested_bid_ask_mid_fields():
    frame = _normalize([_row("2026-04-27T00:55:00Z", {"bid": "0.61", "ask": "0.63", "mid": "0.62"}, {"best_bid": "0.37", "best_ask": "0.39", "mid": "0.38"})])
    assert frame.iloc[0]["yes_bid"] == pytest.approx(0.61)
    assert frame.iloc[0]["yes_ask"] == pytest.approx(0.63)
    assert frame.iloc[0]["yes_mid"] == pytest.approx(0.62)
    assert frame.iloc[0]["no_bid"] == pytest.approx(0.37)


def test_parser_uses_economic_best_levels_from_unsorted_raw_books():
    frame = _normalize([_book_row("2026-04-27T00:55:00Z")])
    row = frame.iloc[0]
    assert row["yes_bid"] == pytest.approx(0.99)
    assert row["no_ask"] == pytest.approx(0.01)


def test_parser_computes_mid_from_bid_ask_when_mid_missing():
    frame = _normalize([_row("2026-04-27T00:55:00Z", {"best_bid": 0.60, "best_ask": 0.64}, {"best_bid": 0.36, "best_ask": 0.40})])
    assert frame.iloc[0]["yes_mid"] == pytest.approx(0.62)
    assert frame.iloc[0]["no_mid"] == pytest.approx(0.38)


def test_spread_filter_does_not_use_yes_vs_no_difference_as_spread():
    frame = _normalize([_row("2026-04-27T00:55:00Z", {"best_bid": 0.80, "best_ask": 0.82}, {"best_bid": 0.18, "best_ask": 0.20})])
    filtered, counts = tc.filter_quotes_for_distribution(
        frame,
        price_source="bid",
        disable_spread_filter=False,
        max_spread=0.05,
        mid_complement_tolerance=None,
        max_post_end_lag_seconds=0.0,
    )
    assert len(filtered) == 1
    assert counts["wide_spread"] == 0


def test_strict_conviction_finds_yes_crossing_and_never_falling():
    out = _by_market(
        [
            _row("2026-04-27T00:55:00Z", {"mid": 0.60}, {"mid": 0.40}),
            _row("2026-04-27T00:56:00Z", {"mid": 0.81}, {"mid": 0.19}),
            _row("2026-04-27T00:57:00Z", {"mid": 0.83}, {"mid": 0.17}),
        ]
    )
    row = out.iloc[0]
    assert row["reached_terminal_conviction"] == True
    assert row["convicted_side"] == "YES"
    assert row["conviction_market_age_seconds"] == pytest.approx(60.0)


def test_strict_conviction_rejects_later_fall():
    out = _by_market(
        [
            _row("2026-04-27T00:55:00Z", {"mid": 0.60}, {"mid": 0.40}),
            _row("2026-04-27T00:56:00Z", {"mid": 0.81}, {"mid": 0.19}),
            _row("2026-04-27T00:57:00Z", {"mid": 0.79}, {"mid": 0.21}),
        ]
    )
    assert out.iloc[0]["reached_terminal_conviction"] == False


def test_tolerant_conviction_accepts_small_number_of_dips():
    rows = [_row("2026-04-27T00:55:00Z", {"mid": 0.81}, {"mid": 0.19})]
    for i in range(1, 20):
        rows.append(_row(f"2026-04-27T00:55:{i:02d}Z", {"mid": 0.81}, {"mid": 0.19}))
    rows.append(_row("2026-04-27T00:56:00Z", {"mid": 0.77}, {"mid": 0.23}))
    out = _by_market(rows, definition="tolerant", min_later_share=0.95)
    assert out.iloc[0]["reached_terminal_conviction"] == True


def test_distribution_by_second_counts_first_conviction_at_correct_second():
    out = _by_market(
        [
            _row("2026-04-27T00:55:00Z", {"mid": 0.60}, {"mid": 0.40}),
            _row("2026-04-27T00:56:05Z", {"mid": 0.81}, {"mid": 0.19}),
            _row("2026-04-27T00:57:00Z", {"mid": 0.83}, {"mid": 0.17}),
        ]
    )
    by_second = tc.distribution_by_second(out, 300)
    row = by_second[by_second["market_age_second"] == 65].iloc[0]
    assert row["first_convictions_at_second"] == 1
    assert row["cumulative_convictions"] == 1


def test_binned_distribution_aggregates_correctly():
    out = _by_market(
        [
            _row("2026-04-27T00:55:00Z", {"mid": 0.60}, {"mid": 0.40}),
            _row("2026-04-27T00:56:05Z", {"mid": 0.81}, {"mid": 0.19}),
            _row("2026-04-27T00:57:00Z", {"mid": 0.83}, {"mid": 0.17}),
        ]
    )
    binned = tc.distribution_binned(tc.distribution_by_second(out, 300), [10])
    row = binned[(binned["age_bin_start"] == 60) & (binned["age_bin_end"] == 69)].iloc[0]
    assert row["first_convictions_in_bin"] == 1


def test_never_convicted_markets_remain_in_denominator_and_survival():
    out = _by_market(
        [
            _row("2026-04-27T00:55:00Z", {"mid": 0.60}, {"mid": 0.40}),
            _row("2026-04-27T00:56:00Z", {"mid": 0.61}, {"mid": 0.39}),
        ]
    )
    by_second = tc.distribution_by_second(out, 300)
    assert by_second.iloc[-1]["cumulative_convictions"] == 0
    assert by_second.iloc[-1]["survival_share"] == pytest.approx(1.0)


def test_missing_malformed_rows_emit_diagnostics_without_crashing():
    frame = _normalize(
        [
            {"ts": None, "yes": None, "no": None},
            _row("2026-04-27T00:55:00Z", {"mid": 0.60}, {"mid": 0.40}),
        ]
    )
    diagnostics = tc.quote_diagnostics(frame, debug_schema_sample=2)
    assert diagnostics["parsed_rows"] == 2
    assert diagnostics["rows_dropped_by_reason"]["missing_market_key"] == 1
    assert diagnostics["rows_dropped_by_reason"]["no_side_price"] == 1


def test_cli_smoke_distribution_outputs(tmp_path):
    quotes = tmp_path / "market_quotes.jsonl"
    rows = [
        _row("2026-04-27T00:55:00Z", {"mid": 0.60}, {"mid": 0.40}),
        _row("2026-04-27T00:56:00Z", {"mid": 0.81}, {"mid": 0.19}),
        _row("2026-04-27T00:57:00Z", {"mid": 0.83}, {"mid": 0.17}),
    ]
    quotes.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")
    out = tmp_path / "out"
    diagnostics = dist_cli.run(
        argparse.Namespace(
            quotes=quotes,
            market_meta=None,
            output_dir=out,
            thresholds="0.80",
            sources="mid",
            definitions="strict,tolerant",
            market_window_seconds=300,
            bin_seconds="1,5,10",
            max_spread=None,
            mid_complement_tolerance=None,
            min_quality_quotes_per_market=2,
            min_later_quotes=2,
            min_later_share=0.95,
            tolerant_floor_offset=0.02,
            disable_spread_filter=True,
            debug_schema_sample=2,
            max_post_end_lag_seconds=0.0,
        )
    )
    assert diagnostics["parsed_rows"] == 3
    assert (out / "terminal_conviction_distribution_by_second.csv").exists()
    assert (out / "terminal_conviction_summary.csv").exists()
