import pandas as pd
import pytest

from scripts import audit_capacity_ladders as audit
from src.research import execution_realism_replay as ex


def test_one_ask_level_capacity_is_usdc_notional():
    ladder, summary = audit.walk_capacity_ladder([{"price": 0.40, "size": 100}], p=0.55, threshold=0.10, fee_rate=0.0)
    assert summary["computed_capacity_usdc"] == pytest.approx(40.0)
    assert ladder["level_notional_usdc"].iloc[0] == pytest.approx(40.0)
    assert ladder["cumulative_shares"].iloc[0] == pytest.approx(100.0)


def test_multiple_ask_levels_stop_by_cumulative_vwap():
    asks = [{"price": 0.40, "size": 10}, {"price": 0.45, "size": 10}, {"price": 0.60, "size": 10}]
    ladder, summary = audit.walk_capacity_ladder(asks, p=0.55, threshold=0.10, fee_rate=0.0)
    assert summary["computed_capacity_usdc"] == pytest.approx(10.5)
    assert summary["capacity_stop_reason"] == "edge_below_threshold"
    assert summary["first_rejected_price"] == pytest.approx(0.60)
    accepted = ladder[ladder["capacity_stop_reason"].eq("")]
    assert accepted["cumulative_notional_usdc"].max() == pytest.approx(10.5)
    assert accepted["vwap_price"].iloc[-1] == pytest.approx(0.45)


def test_capacity_uses_price_times_shares_not_shares():
    _, summary = audit.walk_capacity_ladder([{"price": 0.25, "size": 100}], p=0.50, threshold=0.10, fee_rate=0.0)
    assert summary["computed_capacity_usdc"] == pytest.approx(25.0)
    assert summary["computed_capacity_usdc"] != pytest.approx(100.0)


def test_selected_side_does_not_include_opposite_book():
    books = pd.DataFrame(
        {
            "market_key": ["m", "m"],
            "asset_side": ["YES", "NO"],
            "timestamp": pd.to_datetime(["2026-01-01T00:00:01Z", "2026-01-01T00:00:01Z"], utc=True),
            "asks": [[{"price": 0.4, "size": 10}], [{"price": 0.1, "size": 999}]],
            "best_ask": [0.4, 0.1],
            "best_bid": [0.39, 0.09],
            "book_parse_status": ["ok_full_depth", "ok_full_depth"],
            "execution_depth_mode": ["full_depth", "full_depth"],
        }
    )
    selected = ex.select_execution_book(books, "m", "YES", pd.Timestamp("2026-01-01T00:00:00Z"), 0, 2.0)
    _, summary = audit.walk_capacity_ladder(selected["asks"], p=0.55, threshold=0.10, fee_rate=0.0)
    assert selected["asset_side"] == "YES"
    assert summary["computed_capacity_usdc"] == pytest.approx(4.0)


def test_duplicate_price_levels_are_preserved_and_flagged():
    asks = [{"price": 0.4, "size": 10}, {"price": 0.4, "size": 10}]
    ladder, summary = audit.walk_capacity_ladder(asks, p=0.6, threshold=0.1, fee_rate=0.0)
    assert len(ladder) == 2
    assert summary["computed_capacity_usdc"] == pytest.approx(8.0)
    assert summary["duplicate_level_count"] == 1
    assert "duplicate_price_size_levels_in_book" in summary["suspicious_flags"]


def test_reported_vs_recomputed_difference_matches_synthetic_capacity_row():
    samples = pd.DataFrame(
        {
            "sample_id": ["sample_0000"],
            "market_key": ["m"],
            "model_id": ["model"],
            "prediction_ts": pd.to_datetime(["2026-01-01T00:00:00Z"], utc=True),
            "market_age_seconds": [60.0],
            "latency_ms": [0.0],
            "side": ["YES"],
            "p_chosen_side": [0.55],
            "reported_capacity_usdc": [4.0],
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
            "execution_depth_mode": ["full_depth"],
        }
    )
    _, summary = audit.audit_samples(samples, books, latency_ms=0.0, max_book_age_seconds=1.0, threshold=0.10, fee_rate=0.0)
    assert summary["computed_capacity_usdc_at_edge_10"].iloc[0] == pytest.approx(4.0)
    assert summary["difference_computed_minus_reported"].iloc[0] == pytest.approx(0.0)
