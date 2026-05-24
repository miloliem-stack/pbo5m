import pandas as pd
import pytest

from scripts import run_btc5m_bankroll_compounding_replay as bankroll


def args(**overrides):
    defaults = {
        "model_id": "brownian_zero_drift__rv30",
        "source_policy_name": "base_all_models_original_like",
        "chronological_slices": "fresh",
        "start_ts": None,
        "end_ts": None,
        "fixed_stake_usd": 5.0,
        "min_age_sec": 60.0,
        "max_age_sec": 240.0,
        "min_ask": 0.30,
        "max_ask": 0.47,
        "min_edge": 0.02,
        "initial_bankroll_usd": 100.0,
        "max_stake_usd": 100.0,
        "min_stake_usd": 1.0,
        "max_daily_loss_usd": None,
        "max_open_exposure_usd": None,
    }
    defaults.update(overrides)
    return type("Args", (), defaults)()


def replay_frame() -> pd.DataFrame:
    rows = []
    base = pd.Timestamp("2026-05-20T00:00:00Z")
    for i, winner in enumerate(["YES", "NO", "YES"], start=1):
        ask = 0.40 if i != 2 else 0.42
        rows.append(
            {
                "policy_name": "base_all_models_original_like",
                "model_id": "brownian_zero_drift__rv30",
                "market_key": i,
                "market_id": i,
                "market_start_ts": base + pd.Timedelta(minutes=5 * i),
                "market_end_ts": base + pd.Timedelta(minutes=5 * i + 5),
                "entry_ts": base + pd.Timedelta(minutes=5 * i, seconds=90),
                "side": "YES",
                "winner_side": winner,
                "p_yes": 0.48,
                "model_edge": 0.08,
                "stake_size": 5.0,
                "entry_age_seconds": 90.0,
                "entry_age_window": "60_240",
                "chronological_slice": "fresh",
                "entry_ask": ask,
                "gross_cost": 5.0,
                "pnl": 5.0 * bankroll.binary_return_on_cost(ask, winner == "YES"),
                "win": winner == "YES",
            }
        )
    return pd.DataFrame(rows)


def test_binary_trade_return_math():
    assert bankroll.binary_return_on_cost(0.40, 1) == pytest.approx(1.5)
    assert bankroll.binary_return_on_cost(0.40, 0) == pytest.approx(-1.0)


def test_kelly_fraction_formula_and_zero_edge():
    assert bankroll.full_kelly_fraction(0.48, 0.40) == pytest.approx((0.48 - 0.40) / (1 - 0.40))
    assert bankroll.full_kelly_fraction(0.39, 0.40) == 0.0


def test_fractional_kelly_cap_is_respected():
    selected = bankroll.select_policy_trades(replay_frame().iloc[[0]], args())
    policy = bankroll.SizingPolicy("kelly_cap", "bankroll_fractional_kelly", kelly_fraction_multiplier=1.0, max_fraction_per_market=0.02)
    summary, _, sized = bankroll.simulate_bankroll_policy(selected, policy, args())
    assert summary["trade_count"] == 1
    assert sized["kelly_fraction"].iloc[0] == pytest.approx(0.02)
    assert sized["stake"].iloc[0] == pytest.approx(2.0)


def test_bankroll_compounds_chronologically_after_settlement():
    frame = replay_frame().iloc[:2].copy()
    selected = bankroll.select_policy_trades(frame, args())
    policy = bankroll.SizingPolicy("fixed_half", "bankroll_fixed_fraction", fixed_fraction=0.5)
    summary, _, sized = bankroll.simulate_bankroll_policy(selected, policy, args(min_stake_usd=0.01))
    assert summary["trade_count"] == 2
    assert sized["stake"].tolist() == pytest.approx([50.0, 87.5])
    assert summary["final_bankroll"] == pytest.approx(87.5)


def test_unresolved_exposure_cannot_be_reused_by_default():
    frame = replay_frame().iloc[:2].copy()
    frame.loc[:, "market_end_ts"] = pd.Timestamp("2026-05-20T01:00:00Z")
    selected = bankroll.select_policy_trades(frame, args())
    policy = bankroll.SizingPolicy("fixed_half", "bankroll_fixed_fraction", fixed_fraction=0.5)
    summary, _, sized = bankroll.simulate_bankroll_policy(selected, policy, args(min_stake_usd=0.01))
    assert summary["trade_count"] == 2
    assert sized["stake"].tolist() == pytest.approx([50.0, 25.0])


def test_max_daily_loss_blocks_later_trades():
    frame = replay_frame().iloc[:2].copy()
    frame.loc[0, "winner_side"] = "NO"
    frame.loc[0, "win"] = False
    frame.loc[0, "pnl"] = -5.0
    selected = bankroll.select_policy_trades(frame, args())
    policy = bankroll.SizingPolicy("fixed_10pct", "bankroll_fixed_fraction", fixed_fraction=0.1)
    summary, path, _ = bankroll.simulate_bankroll_policy(selected, policy, args(max_daily_loss_usd=5.0, min_stake_usd=0.01))
    assert summary["trade_count"] == 1
    assert summary["skipped_by_reason"] == {"daily_loss": 1}
    assert "daily_loss" in path["skip_reason"].tolist()


def test_one_entry_per_market_happens_before_sizing():
    frame = pd.concat([replay_frame().iloc[[0]], replay_frame().iloc[[0]]], ignore_index=True)
    frame.loc[1, "entry_ts"] = frame.loc[1, "entry_ts"] + pd.Timedelta(seconds=30)
    selected = bankroll.select_policy_trades(frame, args())
    assert len(selected) == 1
    assert selected["market_key_for_sizing"].nunique() == 1


def test_start_end_timestamps_filter_decision_ts_before_dedupe():
    frame = replay_frame()
    selected = bankroll.select_policy_trades(
        frame,
        args(
            chronological_slices="",
            start_ts="2026-05-20T00:10:00Z",
            end_ts="2026-05-20T00:15:00Z",
        ),
    )
    assert selected["market_key_for_sizing"].tolist() == ["2"]
    assert selected["decision_ts"].iloc[0] >= pd.Timestamp("2026-05-20T00:10:00Z")
    assert selected["decision_ts"].iloc[0] < pd.Timestamp("2026-05-20T00:15:00Z")


def test_end_timestamp_is_exclusive():
    frame = replay_frame()
    frame.loc[0, "entry_ts"] = pd.Timestamp("2026-05-20T00:10:00Z")
    selected = bankroll.select_policy_trades(
        frame.iloc[[0]],
        args(chronological_slices="", start_ts="2026-05-20T00:00:00Z", end_ts="2026-05-20T00:10:00Z"),
    )
    assert selected.empty


def test_additive_metrics_use_replay_gross_cost_and_pnl_unchanged():
    selected = bankroll.select_policy_trades(replay_frame().iloc[:2], args())
    summary = bankroll.additive_summary(selected, 5.0)
    assert summary["accounting_source"] == "replay_gross_cost_pnl"
    assert summary["gross_cost"] == pytest.approx(10.0)
    assert summary["pnl"] == pytest.approx(selected["pnl"].sum())


def test_by_date_includes_daily_bankroll_series():
    selected = bankroll.select_policy_trades(replay_frame().iloc[:1], args())
    policy = bankroll.SizingPolicy("fixed_10pct", "bankroll_fixed_fraction", fixed_fraction=0.1)
    _, path, _ = bankroll.simulate_bankroll_policy(selected, policy, args(min_stake_usd=0.01))
    daily = bankroll.by_date(selected, path)
    row = daily[daily["accounting_policy"].eq("fixed_10pct")].iloc[0]
    assert row["ending_bankroll"] > 100.0
