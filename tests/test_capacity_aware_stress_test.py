import pandas as pd
import pytest

from scripts import research_capacity_aware_stress_test as stress


def _selected():
    return pd.DataFrame(
        {
            "model_id": ["m", "m", "baseline_50"],
            "market_key": ["a", "b", "a"],
            "prediction_ts": pd.to_datetime(["2026-01-01T00:01:00Z", "2026-01-01T00:02:00Z", "2026-01-01T00:01:00Z"], utc=True),
            "market_age_seconds": [60.0, 120.0, 60.0],
            "side": ["YES", "NO", "YES"],
            "raw_entry_price": [0.40, 0.50, 0.40],
            "adjusted_entry_price": [0.40, 0.50, 0.40],
            "fee_rate": [0.0, 0.0, 0.0],
            "win": [1.0, 0.0, 1.0],
            "label_source": ["chainlink", "chainlink", "chainlink"],
            "cost_adjusted_edge": [0.2, 0.1, 0.0],
        }
    )


def _capacity():
    return pd.DataFrame(
        {
            "market_id": ["a", "b", "a"],
            "model_id": ["m", "m", "baseline_50"],
            "decision_age": [60.0, 120.0, 60.0],
            "side": ["YES", "NO", "YES"],
            "decision_ts": pd.to_datetime(["2026-01-01T00:01:00Z", "2026-01-01T00:02:00Z", "2026-01-01T00:01:00Z"], utc=True),
            "latency_ms": [1000.0, 1000.0, 1000.0],
            "execution_book_status": ["ok", "ok", "ok"],
            "capacity_usdc_at_edge_10": [100.0, None, 50.0],
        }
    )


def test_fixed_stake_sizing_and_roi():
    selected = stress.normalize_selected_entries(_selected())
    capacity = stress.normalize_capacity(_capacity())
    joined, _ = stress.join_selected_capacity(selected, capacity, "capacity_usdc_at_edge_10")
    joined = stress.add_breakdown_columns(joined)
    scored = stress.scenario_frames(joined, capacity_col="capacity_usdc_at_edge_10", max_caps=[25.0], haircuts=[1.0])
    fixed = scored[(scored["scenario_name"].eq("fixed_10")) & (scored["missing_capacity_mode"].eq("full_join_conservative_zero_missing")) & (scored["model_id"].eq("m"))]
    assert set(fixed["scenario_stake"]) == {10.0}
    summary = stress.summarize(fixed, ["scenario_name", "model_id"])
    # win at 0.40 with $10 stake pays 25; pnl +15. loss at 0.50 loses 10; total pnl +5 on $20 stake.
    assert summary["total_pnl"].iloc[0] == pytest.approx(5.0)
    assert summary["roi"].iloc[0] == pytest.approx(0.25)


def test_capacity_fraction_and_max_cap_clipping():
    selected = stress.normalize_selected_entries(_selected())
    capacity = stress.normalize_capacity(_capacity())
    joined, _ = stress.join_selected_capacity(selected, capacity, "capacity_usdc_at_edge_10")
    scored = stress.scenario_frames(joined, capacity_col="capacity_usdc_at_edge_10", max_caps=[25.0], haircuts=[1.0])
    row = scored[
        scored["scenario_name"].eq("cap_frac_50pct")
        & scored["missing_capacity_mode"].eq("full_join_conservative_zero_missing")
        & scored["market_key"].eq("a")
        & scored["model_id"].eq("m")
    ].iloc[0]
    assert row["scenario_stake"] == pytest.approx(25.0)


def test_missing_capacity_zero_and_full_depth_filter():
    selected = stress.normalize_selected_entries(_selected())
    capacity = stress.normalize_capacity(_capacity())
    joined, _ = stress.join_selected_capacity(selected, capacity, "capacity_usdc_at_edge_10")
    scored = stress.scenario_frames(joined, capacity_col="capacity_usdc_at_edge_10", max_caps=[25.0], haircuts=[1.0])
    zero_missing = scored[
        scored["scenario_name"].eq("cap_frac_100pct")
        & scored["missing_capacity_mode"].eq("full_join_conservative_zero_missing")
        & scored["market_key"].eq("b")
    ].iloc[0]
    full_depth_b = scored[
        scored["scenario_name"].eq("cap_frac_100pct")
        & scored["missing_capacity_mode"].eq("full_depth_only")
        & scored["market_key"].eq("b")
    ]
    assert zero_missing["scenario_stake"] == pytest.approx(0.0)
    assert full_depth_b.empty


def test_max_drawdown_calculation():
    pnl = pd.Series([10.0, -3.0, -4.0, 2.0, -10.0])
    assert stress.max_drawdown(pnl) == pytest.approx(-15.0)


def test_concentration_calculation():
    frame = pd.DataFrame(
        {
            "scenario_pnl": [10.0, 5.0, -1.0],
            "effective_capacity_usdc": [100.0, 50.0, 10.0],
        }
    )
    out = stress.concentration_metrics(frame)
    assert out["top_1_trade_pnl_share"] == pytest.approx(10.0 / 14.0)
    assert out["pnl_without_top_1"] == pytest.approx(4.0)
    assert out["pnl_without_largest_capacity_1pct"] == pytest.approx(4.0)


def test_join_keys_are_stable_and_report_unmatched():
    selected = stress.normalize_selected_entries(_selected())
    capacity = stress.normalize_capacity(_capacity().iloc[:1])
    joined, diagnostics = stress.join_selected_capacity(selected, capacity, "capacity_usdc_at_edge_10")
    assert diagnostics["join_keys"] == ["market_key", "model_id", "market_age_seconds", "side", "prediction_ts"]
    assert diagnostics["unmatched_selected_rows"] == 2
    assert len(joined) == 3
