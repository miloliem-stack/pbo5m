import pandas as pd
import pytest

from scripts import research_capacity_aware_stress_test as stress
from scripts import research_one_entry_capacity_stress as one


def _selected() -> pd.DataFrame:
    return stress.normalize_selected_entries(
        pd.DataFrame(
            {
                "model_id": ["m", "m", "m", "m"],
                "market_key": ["a", "a", "b", "b"],
                "prediction_ts": pd.to_datetime(
                    ["2026-01-01T00:00:30Z", "2026-01-01T00:01:30Z", "2026-01-01T00:01:00Z", "2026-01-01T00:02:00Z"],
                    utc=True,
                ),
                "market_age_seconds": [30.0, 90.0, 60.0, 120.0],
                "side": ["YES", "NO", "YES", "YES"],
                "raw_entry_price": [0.4, 0.5, 0.25, 0.5],
                "adjusted_entry_price": [0.4, 0.5, 0.25, 0.5],
                "fee_rate": [0.0, 0.0, 0.0, 0.0],
                "win": [1.0, 0.0, 1.0, 0.0],
                "label_source": ["chainlink", "chainlink", "chainlink", "chainlink"],
                "cost_adjusted_edge": [0.03, 0.2, 0.1, 0.1],
            }
        )
    )


def _capacity() -> pd.DataFrame:
    return stress.normalize_capacity(
        pd.DataFrame(
            {
                "market_id": ["a", "a", "a", "b", "b"],
                "model_id": ["m", "m", "m", "m", "m"],
                "decision_age": [30.0, 30.0, 90.0, 60.0, 120.0],
                "side": ["YES", "YES", "NO", "YES", "YES"],
                "decision_ts": pd.to_datetime(
                    ["2026-01-01T00:00:30Z", "2026-01-01T00:00:30Z", "2026-01-01T00:01:30Z", "2026-01-01T00:01:00Z", "2026-01-01T00:02:00Z"],
                    utc=True,
                ),
                "execution_book_status": ["stale_book", "ok", "ok", "ok", "ok"],
                "book_lag_seconds": [0.1, 0.2, 0.1, 0.1, 0.1],
                "capacity_usdc_at_edge_10": [999.0, 100.0, 0.0, 50.0, 200.0],
            }
        )
    )


def _joined() -> pd.DataFrame:
    selected = _selected()
    capacity = _capacity()
    keys = one.capacity_join_keys(selected, capacity)
    unique, _, _ = one.dedupe_capacity_for_join(capacity, keys, "capacity_usdc_at_edge_10")
    joined, _ = one.join_without_expansion(selected, unique, keys, "capacity_usdc_at_edge_10")
    return joined


def test_capacity_duplicate_dedupe_does_not_choose_max_capacity():
    capacity = _capacity()
    keys = ["market_key", "model_id", "market_age_seconds", "side", "prediction_ts"]
    unique, diagnostics, duplicate_groups = one.dedupe_capacity_for_join(capacity, keys, "capacity_usdc_at_edge_10")
    row = unique[(unique["market_key"].eq("a")) & (unique["market_age_seconds"].eq(30.0))].iloc[0]
    assert row["execution_book_status"] == "ok"
    assert row["capacity_usdc_at_edge_10"] == pytest.approx(100.0)
    assert diagnostics["duplicate_capacity_key_groups"] == 1
    assert len(duplicate_groups) == 1


def test_many_to_many_join_expansion_fails_loudly():
    selected = _selected().iloc[:1]
    capacity = _capacity().iloc[:2]
    keys = ["market_key", "model_id", "market_age_seconds", "side", "prediction_ts"]
    with pytest.raises(ValueError, match="expanded"):
        one.join_without_expansion(selected, capacity, keys, "capacity_usdc_at_edge_10")


def test_entry_selection_policies():
    joined = one.apply_missing_capacity_mode(_joined(), "full_join_conservative_zero_missing")
    first, _ = one.select_one_entry(joined, "first_entry")
    positive, _ = one.select_one_entry(joined, "first_positive_capacity_entry")
    after60, _ = one.select_one_entry(joined, "first_entry_after_60s")
    after90, _ = one.select_one_entry(joined, "first_entry_after_90s")
    max_edge, _ = one.select_one_entry(joined, "max_edge_entry")
    max_ev, _ = one.select_one_entry(joined, "max_capacity_adjusted_ev_entry")
    assert first[first["market_key"].eq("a")]["market_age_seconds"].iloc[0] == pytest.approx(30.0)
    assert positive[positive["market_key"].eq("a")]["market_age_seconds"].iloc[0] == pytest.approx(30.0)
    assert after60[after60["market_key"].eq("a")]["market_age_seconds"].iloc[0] == pytest.approx(90.0)
    assert after90[after90["market_key"].eq("a")]["market_age_seconds"].iloc[0] == pytest.approx(90.0)
    assert max_edge[max_edge["market_key"].eq("a")]["market_age_seconds"].iloc[0] == pytest.approx(90.0)
    # The 30s row has lower edge but positive capacity; the 90s row has zero capacity.
    assert max_ev[max_ev["market_key"].eq("a")]["market_age_seconds"].iloc[0] == pytest.approx(30.0)


def test_first_positive_capacity_skips_zero_capacity():
    joined = one.apply_missing_capacity_mode(_joined(), "full_join_conservative_zero_missing")
    selected, diag = one.select_one_entry(joined[joined["market_key"].eq("b")], "first_positive_capacity_entry")
    assert selected["market_age_seconds"].iloc[0] == pytest.approx(60.0)
    assert diag["selected_rows_per_market_max"] == 1


def test_one_entry_invariant_holds():
    joined = one.apply_missing_capacity_mode(_joined(), "full_join_conservative_zero_missing")
    selected, _ = one.select_one_entry(joined, "max_edge_entry")
    sizes = selected.groupby(["market_key", "model_id", "label_source"]).size()
    assert sizes.max() == 1


def test_fixed_and_capacity_fraction_sizing_and_haircut():
    selected = one.select_one_entry(one.apply_missing_capacity_mode(_joined(), "full_join_conservative_zero_missing"), "first_entry")[0]
    scored = pd.concat(list(one.iter_one_entry_scenarios(selected, max_caps=[25.0], haircuts=[0.5])), ignore_index=True)
    fixed = scored[(scored["scenario_name"].eq("fixed_10")) & (scored["market_key"].eq("a"))].iloc[0]
    frac = scored[(scored["scenario_name"].eq("cap_frac_50pct")) & (scored["market_key"].eq("a"))].iloc[0]
    assert fixed["scenario_stake"] == pytest.approx(10.0)
    # capacity 100 * haircut 0.5 * fraction 0.5 = 25, clipped at 25.
    assert frac["scenario_stake"] == pytest.approx(25.0)


def test_zero_missing_capacity_behavior():
    joined = _joined()
    joined.loc[joined["market_key"].eq("a"), "reported_capacity_usdc"] = pd.NA
    zeroed = one.apply_missing_capacity_mode(joined, "full_join_conservative_zero_missing")
    full_depth = one.apply_missing_capacity_mode(joined, "full_depth_only")
    assert zeroed[zeroed["market_key"].eq("a")]["reported_capacity_usdc"].iloc[0] == pytest.approx(0.0)
    assert full_depth[full_depth["market_key"].eq("a")].empty


def test_roi_drawdown_and_concentration_metrics():
    frame = pd.DataFrame(
        {
            "scenario_pnl": [10.0, -4.0, 2.0, -8.0],
            "effective_capacity_usdc": [100.0, 50.0, 25.0, 10.0],
        }
    )
    assert stress.max_drawdown(frame["scenario_pnl"]) == pytest.approx(-10.0)
    concentration = stress.concentration_metrics(frame)
    assert concentration["top_1_trade_pnl_share"] is None
    scored = stress.score_sized_entries(
        pd.DataFrame(
            {
                "scenario_stake": [10.0],
                "adjusted_entry_price": [0.5],
                "raw_entry_price": [0.5],
                "fee_rate": [0.0],
                "win": [1.0],
            }
        )
    )
    assert scored["scenario_pnl"].iloc[0] == pytest.approx(10.0)
    assert scored["scenario_trade_roi"].iloc[0] == pytest.approx(1.0)
