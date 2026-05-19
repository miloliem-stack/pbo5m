import pandas as pd
import pytest

from scripts import run_capacity_curve_replay as cap
from src.research import execution_realism_replay as ex


def test_vwap_for_stake_and_edge_after_vwap():
    asks = [{"price": 0.4, "size": 1}, {"price": 0.6, "size": 10}]
    assert cap.vwap_for_stake(asks, 1.0) == pytest.approx(0.5)
    assert cap.edge_after_vwap(0.7, 0.5, 0.0) == pytest.approx(0.2)


def test_capacity_at_edge_threshold_walks_depth():
    asks = [{"price": 0.4, "size": 10}, {"price": 0.8, "size": 10}]
    # First level has vwap 0.4 and edge 0.3; second level eventually decays below 0.1.
    capacity = cap.capacity_at_edge(asks, p=0.7, threshold=0.1, fee_rate=0.0)
    assert capacity > 4.0
    assert capacity < 12.0


def test_capacity_at_edge_zero_for_no_depth():
    assert cap.capacity_at_edge([], p=0.7, threshold=0.1, fee_rate=0.0) == 0.0


def test_quote_files_for_targets_uses_timestamp_hour(tmp_path):
    target = pd.Timestamp("2026-04-25T01:59:59Z")
    p1 = tmp_path / "2026-04-25" / "01"
    p2 = tmp_path / "2026-04-25" / "02"
    p1.mkdir(parents=True)
    p2.mkdir(parents=True)
    (p1 / "market_quotes.jsonl").write_text("", encoding="utf-8")
    (p2 / "market_quotes.jsonl").write_text("", encoding="utf-8")
    files = ex.quote_files_for_targets(tmp_path, pd.Series([target]))
    assert p1 / "market_quotes.jsonl" in files
    assert p2 / "market_quotes.jsonl" in files


def test_capacity_rows_has_requested_columns():
    entries = pd.DataFrame(
        {
            "market_key": ["m"],
            "model_id": ["model"],
            "prediction_ts": pd.to_datetime(["2026-01-01T00:00:00Z"], utc=True),
            "market_age_seconds": [60.0],
            "side": ["YES"],
            "p_up": [0.7],
            "edge_threshold": [0.1],
            "raw_edge": [0.2],
        }
    )
    books = pd.DataFrame(
        {
            "market_key": ["m"],
            "asset_side": ["YES"],
            "timestamp": pd.to_datetime(["2026-01-01T00:00:00Z"], utc=True),
            "asks": [[{"price": 0.4, "size": 10}]],
            "best_ask": [0.4],
            "best_bid": [0.39],
            "book_parse_status": ["ok_full_depth"],
        }
    )
    out = cap.capacity_rows(entries, books, [0], 0.0, 1.0, [1, 5])
    assert out["best_ask"].iloc[0] == pytest.approx(0.4)
    assert out["capacity_usdc_at_edge_10"].iloc[0] == pytest.approx(4.0)
    assert "vwap_at_1" in out.columns
    assert "edge_after_vwap_at_5" in out.columns
