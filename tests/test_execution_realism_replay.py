import pandas as pd
import pytest

from src.research import execution_realism_replay as ex


def test_vwap_full_fill_one_level():
    fill = ex.simulate_vwap_fill([{"price": 0.5, "size": 10}], 1.0, min_trade_notional_usdc=1.0, min_fill_ratio=1.0, allow_partial_fills=False)
    assert fill["fill_status"] == "filled"
    assert fill["shares_filled"] == pytest.approx(2.0)
    assert fill["vwap_price"] == pytest.approx(0.5)
    assert fill["fill_ratio"] == pytest.approx(1.0)


def test_vwap_full_fill_multiple_levels():
    fill = ex.simulate_vwap_fill([{"price": 0.4, "size": 1}, {"price": 0.6, "size": 2}], 1.0, min_trade_notional_usdc=1.0, min_fill_ratio=1.0, allow_partial_fills=False)
    assert fill["fill_status"] == "filled"
    assert fill["gross_trade_notional"] == pytest.approx(1.0)
    assert fill["shares_filled"] == pytest.approx(2.0)
    assert fill["vwap_price"] == pytest.approx(0.5)


def test_vwap_insufficient_depth_partial_disabled():
    fill = ex.simulate_vwap_fill([{"price": 0.5, "size": 1}], 1.0, min_trade_notional_usdc=0.1, min_fill_ratio=1.0, allow_partial_fills=False)
    assert fill["fill_status"] == "insufficient_depth"
    assert fill["fill_ratio"] == pytest.approx(0.5)


def test_vwap_insufficient_depth_partial_enabled():
    fill = ex.simulate_vwap_fill([{"price": 0.5, "size": 1}], 1.0, min_trade_notional_usdc=0.1, min_fill_ratio=1.0, allow_partial_fills=True)
    assert fill["fill_status"] == "partial_fill"
    assert fill["shares_filled"] == pytest.approx(1.0)


def test_fee_and_pnl_win_and_loss():
    fill = {"fill_status": "filled", "shares_filled": 2.0, "gross_trade_notional": 1.0, "vwap_price": 0.5}
    win = ex.apply_fee_and_score(fill, fee_rate=0.07, p_chosen_side=0.7, edge_threshold=0.0, require_edge=False, label_up=1.0, side="YES")
    loss = ex.apply_fee_and_score(fill, fee_rate=0.07, p_chosen_side=0.7, edge_threshold=0.0, require_edge=False, label_up=0.0, side="YES")
    assert win["fee"] == pytest.approx(2.0 * 0.07 * 0.5 * 0.5)
    assert win["pnl"] == pytest.approx(2.0 - win["total_cost"])
    assert loss["pnl"] == pytest.approx(-loss["total_cost"])
    assert win["trade_roi"] == pytest.approx(win["pnl"] / win["total_cost"])


def _books():
    return pd.DataFrame(
        {
            "market_key": ["m", "m"],
            "asset_side": ["YES", "YES"],
            "timestamp": pd.to_datetime(["2026-01-01T00:00:01Z", "2026-01-01T00:00:03Z"], utc=True),
            "asks": [[{"price": 0.5, "size": 10}], [{"price": 0.6, "size": 10}]],
            "bids": [[{"price": 0.49, "size": 10}], [{"price": 0.55, "size": 10}]],
            "best_ask": [0.5, 0.6],
            "best_bid": [0.49, 0.55],
            "book_parse_status": ["ok_full_depth", "ok_full_depth"],
        }
    )


def test_latency_selects_first_book_at_or_after_target():
    book = ex.select_execution_book(_books(), "m", "YES", pd.Timestamp("2026-01-01T00:00:00Z"), 1500, 2.0)
    assert book["execution_book_ts"] == pd.Timestamp("2026-01-01T00:00:03Z")
    assert book["execution_book_lag_seconds"] == pytest.approx(1.5)
    assert book["execution_book_status"] == "ok"


def test_latency_rejects_stale_and_missing():
    stale = ex.select_execution_book(_books(), "m", "YES", pd.Timestamp("2026-01-01T00:00:00Z"), 1500, 1.0)
    missing = ex.select_execution_book(_books(), "x", "YES", pd.Timestamp("2026-01-01T00:00:00Z"), 0, 1.0)
    assert stale["execution_book_status"] == "stale_book"
    assert missing["execution_book_status"] == "no_execution_book"


def test_edge_after_vwap_fails_and_passes():
    fill = {"fill_status": "filled", "shares_filled": 1.0, "gross_trade_notional": 0.8, "vwap_price": 0.8}
    failed = ex.apply_fee_and_score(fill, fee_rate=0.0, p_chosen_side=0.85, edge_threshold=0.10, require_edge=True, label_up=1, side="YES")
    passed = ex.apply_fee_and_score(fill, fee_rate=0.0, p_chosen_side=0.95, edge_threshold=0.10, require_edge=True, label_up=1, side="YES")
    assert failed["score_status"] == "failed_edge_after_vwap"
    assert passed["score_status"] == "filled"


def test_markout_bid_and_mid_and_missing():
    out = ex.find_markout(_books(), "m", "YES", pd.Timestamp("2026-01-01T00:00:01Z"), 1, 1.0, 2.0, tolerance_seconds=2.0)
    missing = ex.find_markout(_books(), "z", "YES", pd.Timestamp("2026-01-01T00:00:01Z"), 1, 1.0, 2.0)
    assert out["markout_status"] == "ok"
    assert out["markout_pnl_using_bid"] == pytest.approx(2 * 0.55 - 1.0)
    assert out["markout_mid_pnl"] == pytest.approx(2 * ((0.55 + 0.6) / 2) - 1.0)
    assert missing["markout_status"] == "missing_later_book"


def test_markout_missing_bid():
    books = _books()
    books.loc[1, "best_bid"] = float("nan")
    out = ex.find_markout(books, "m", "YES", pd.Timestamp("2026-01-01T00:00:01Z"), 1, 1.0, 2.0)
    assert out["markout_status"] == "missing_bid"


def test_baseline_join_no_duplicate_model_rows_and_missing_baseline():
    score = pd.DataFrame(
        {
            "label_source": ["chainlink", "chainlink", "chainlink"],
            "model_id": ["baseline_50", "m", "x"],
            "edge_threshold": [0.1, 0.1, 0.2],
            "stake_usdc": [1.0, 1.0, 1.0],
            "latency_ms": [0.0, 0.0, 0.0],
            "max_book_age_seconds": [1.0, 1.0, 1.0],
            "fee_rate": [0.07, 0.07, 0.07],
            "entry_age_set": ["60", "60", "60"],
            "total_pnl": [1.0, 2.0, 3.0],
            "aggregate_roi": [0.1, 0.2, 0.3],
        }
    )
    out = ex.add_baseline_incremental(score)
    assert len(out) == 3
    assert out[out["model_id"].eq("m")]["incremental_pnl_vs_baseline_50"].iloc[0] == pytest.approx(1.0)
    assert out[out["model_id"].eq("x")]["baseline_status"].iloc[0] == "missing"


def test_label_source_handling():
    row = pd.Series({"chainlink_label_up": 1.0, "binance_label_up": 1.0, "label_agree": True})
    missing = pd.Series({"chainlink_label_up": float("nan"), "binance_label_up": 1.0, "label_agree": False})
    assert ex.label_for_source(row, "chainlink") == 1.0
    assert ex.label_for_source(row, "agreement_only") == 1.0
    assert ex.label_for_source(missing, "chainlink") is None
