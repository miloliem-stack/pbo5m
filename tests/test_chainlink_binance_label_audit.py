import pandas as pd
import pytest

from src.research import chainlink_binance_label_audit as audit
from scripts import rescore_probability_edge_replay_by_label_source as rescore


def test_chainlink_parser_flat_row():
    rows = audit.parse_chainlink_record({"ts": "2026-01-01T00:00:00Z", "price": 100.5}, "f")
    assert rows[0]["price"] == pytest.approx(100.5)
    assert rows[0]["raw_source_type"] == "flat"


def test_chainlink_parser_payload_data_list():
    rows = audit.parse_chainlink_record({"payload": {"data": [{"timestamp": 1777078800000, "value": 77479.1}]}}, "f")
    assert rows[0]["timestamp"] == pd.Timestamp("2026-04-25T01:00:00Z")
    assert rows[0]["price"] == pytest.approx(77479.1)
    assert rows[0]["raw_source_type"] == "payload.data"


def test_chainlink_parser_full_accuracy_value():
    rows = audit.parse_chainlink_record(
        {"raw_payload_fragment": {"payload": {"timestamp": 1777078801000, "full_accuracy_value": "77481905220000000000000"}}},
        "f",
    )
    assert rows[0]["price"] == pytest.approx(77481.90522)
    assert rows[0]["raw_source_type"] == "payload.full_accuracy_value"


def test_label_derivation_and_equality_not_up(tmp_path):
    pred = pd.DataFrame(
        {
            "market_window_start": ["2026-01-01T00:00:00Z"],
            "market_window_end": ["2026-01-01T00:05:00Z"],
            "K": [100.0],
            "S_end": [100.0],
        }
    )
    pred_path = tmp_path / "pred.csv"
    pred.to_csv(pred_path, index=False)
    cl_dir = tmp_path / "cl"
    bn_dir = tmp_path / "bn"
    cl_dir.mkdir()
    bn_dir.mkdir()
    (cl_dir / "chainlink_prices.jsonl").write_text('{"ts":"2026-01-01T00:05:00Z","price":101}\n', encoding="utf-8")
    labels, _ = audit.build_label_audit(pred_path, bn_dir, cl_dir, chainlink_tolerance_seconds=10, binance_tolerance_seconds=60, terminal_margin_bands=[1, 2])
    assert labels["binance_label_up"].iloc[0] == 0.0
    assert labels["chainlink_label_up"].iloc[0] == 1.0
    assert labels["label_agree"].iloc[0] == False


def test_missing_outside_tolerance_status(tmp_path):
    pred = pd.DataFrame({"market_window_start": ["2026-01-01T00:00:00Z"], "market_window_end": ["2026-01-01T00:05:00Z"], "K": [100.0]})
    pred_path = tmp_path / "pred.csv"
    pred.to_csv(pred_path, index=False)
    empty = tmp_path / "empty"
    empty.mkdir()
    labels, _ = audit.build_label_audit(pred_path, empty, empty, chainlink_tolerance_seconds=1, binance_tolerance_seconds=1, terminal_margin_bands=[1])
    assert labels["label_source_status"].iloc[0] in {"missing_chainlink", "outside_tolerance"}


def _trades_and_audit():
    trades = pd.DataFrame(
        {
            "model": ["m", "m", "baseline_50", "baseline_50"],
            "prediction_market_key": ["a", "a", "a", "b"],
            "prediction_ts": pd.to_datetime(["2026-01-01T00:01:00Z", "2026-01-01T00:02:00Z", "2026-01-01T00:01:00Z", "2026-01-01T00:01:00Z"]),
            "market_age_seconds": [60, 120, 60, 60],
            "fold_id": [0, 0, 0, 0],
            "p_up": [0.8, 0.85, 0.5, 0.5],
            "edge_threshold": [0.01, 0.01, 0.01, 0.01],
            "side": ["YES", "YES", "YES", "NO"],
            "selected_price": [0.6, 0.6, 0.49, 0.49],
            "predicted_edge": [0.2, 0.25, 0.01, 0.01],
            "age_bucket": ["60_120", "120_180", "60_120", "60_120"],
        }
    )
    labels = pd.DataFrame(
        {
            "market_key": ["a", "b"],
            "binance_label_up": [1.0, 1.0],
            "chainlink_label_up": [0.0, 0.0],
            "label_agree": [False, False],
            "binance_terminal_margin_usd": [5.0, 3.0],
            "chainlink_terminal_margin_usd": [-5.0, -3.0],
            "abs_binance_terminal_margin_usd": [5.0, 3.0],
            "abs_chainlink_terminal_margin_usd": [5.0, 3.0],
        }
    )
    return trades, labels


def test_one_entry_per_market_selects_earliest_allowed_age():
    trades, labels = _trades_and_audit()
    entries = rescore.prepare_entries(
        trades,
        labels,
        label_source="binance",
        slippage_bps=0,
        fee_rate=0.0,
        stake_usdc=1.0,
        entry_ages=[60, 120],
        edge_thresholds=[0.01],
        require_cost_adjusted_edge=False,
        max_entry_price=None,
        one_entry=True,
        models=["m"],
    )
    assert len(entries) == 1
    assert entries["market_age_seconds"].iloc[0] == 60


def test_cost_model_fee_slippage_and_roi():
    trades, labels = _trades_and_audit()
    entries = rescore.prepare_entries(
        trades.iloc[[0]],
        labels,
        label_source="binance",
        slippage_bps=100,
        fee_rate=0.07,
        stake_usdc=1.0,
        entry_ages=[60],
        edge_thresholds=[0.01],
        require_cost_adjusted_edge=False,
        max_entry_price=None,
        one_entry=True,
        models=["m"],
    )
    assert entries["adjusted_entry_price"].iloc[0] == pytest.approx(0.606)
    assert entries["fee"].iloc[0] > 0
    assert entries["trade_roi"].iloc[0] == pytest.approx(entries["pnl"].iloc[0] / entries["total_cost"].iloc[0])


def test_disagreement_attribution_counts_flips():
    trades, labels = _trades_and_audit()
    entries = rescore.prepare_entries(
        trades,
        labels,
        label_source="disagreement_only",
        slippage_bps=0,
        fee_rate=0,
        stake_usdc=1,
        entry_ages=[60],
        edge_thresholds=[0.01],
        require_cost_adjusted_edge=False,
        max_entry_price=None,
        one_entry=False,
        models=None,
    )
    attr = rescore.disagreement_attribution(entries)
    assert attr["binance_win_chainlink_loss_count"].sum() >= 1
    assert attr["binance_loss_chainlink_win_count"].sum() >= 1


def test_baseline_incremental_join():
    score = pd.DataFrame(
        {
            "label_source": ["binance", "binance"],
            "model_id": ["baseline_50", "m"],
            "edge_threshold": [0.01, 0.01],
            "slippage_bps": [0, 0],
            "total_pnl": [1.0, 2.5],
            "aggregate_roi": [0.1, 0.25],
        }
    )
    out = rescore.add_incremental(score)
    assert out[out["model_id"].eq("m")]["incremental_pnl_vs_baseline_50"].iloc[0] == pytest.approx(1.5)
