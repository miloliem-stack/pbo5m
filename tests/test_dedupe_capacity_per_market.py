import pandas as pd
import pytest

from scripts import dedupe_capacity_per_market as dedupe


def test_dedupe_capacity_one_row_per_model_setting_market():
    frame = pd.DataFrame(
        {
            "market_id": ["m1", "m1", "m1", "m2"],
            "model_id": ["a", "a", "a", "a"],
            "decision_age": [60, 60, 60, 60],
            "latency_ms": [0, 0, 0, 0],
            "capacity_usdc_at_edge_10": [100.0, 100.0, 90.0, 50.0],
            "capacity_usdc_at_edge_07": [120.0, 120.0, 110.0, 60.0],
            "vwap_at_5": [0.4, 0.4, 0.41, 0.5],
            "best_ask": [0.4, 0.4, 0.4, 0.5],
        }
    )
    out, diagnostics = dedupe.dedupe_capacity(frame, ["market_id", "model_id", "decision_age", "latency_ms"], "first")
    assert len(out) == 2
    assert out[out["market_id"].eq("m1")]["capacity_usdc_at_edge_10"].iloc[0] == pytest.approx(100.0)
    assert out[out["market_id"].eq("m1")]["source_row_count"].iloc[0] == 3
    assert diagnostics["capacity_usdc_at_edge_10_unique_count"].max() == 2


def test_dedupe_capacity_can_use_max_method():
    frame = pd.DataFrame(
        {
            "market_id": ["m1", "m1"],
            "model_id": ["a", "a"],
            "decision_age": [60, 60],
            "latency_ms": [0, 0],
            "capacity_usdc_at_edge_10": [100.0, 120.0],
        }
    )
    out, _ = dedupe.dedupe_capacity(frame, ["market_id", "model_id", "decision_age", "latency_ms"], "max")
    assert out["capacity_usdc_at_edge_10"].iloc[0] == pytest.approx(120.0)
