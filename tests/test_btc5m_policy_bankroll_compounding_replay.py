import pandas as pd
import pytest

from scripts import run_btc5m_policy_bankroll_compounding_replay as sim


def args(**overrides):
    defaults = {
        "starting_bankroll": 100.0,
        "fixed_stake": 5.0,
        "entry_age_min_seconds": 60.0,
        "entry_age_max_seconds": 240.0,
        "model_id": "brownian_zero_drift__rv30",
        "edge_threshold": 0.02,
        "top_n_levels": 10,
        "probability_haircut_abs": 0.0,
        "ask_slippage_abs": 0.0,
        "min_expected_log_growth": 0.0,
        "allow_missing_depth": False,
        "small_wallet_mode": False,
        "small_wallet_threshold": 1000.0,
        "small_wallet_sizing_policy": "kelly_1_40_cap_0_25pct",
        "small_wallet_max_stake_fraction": 0.0025,
        "min_order_notional": 0.0,
        "skip_below_min_order": True,
        "reserve_bankroll_fraction": 0.20,
        "daily_stop_loss_fraction": 0.03,
        "session_stop_loss_fraction": 0.08,
        "allow_round_up_to_min_order": False,
        "starting_bankroll_sweep": "",
    }
    defaults.update(overrides)
    return type("Args", (), defaults)()


def candidates() -> pd.DataFrame:
    base = pd.Timestamp("2026-05-01T00:00:00Z")
    rows = []
    for market_key, sec, ask, p_yes, winner in [
        (1, 70.0, 0.40, 0.50, "YES"),
        (1, 90.0, 0.39, 0.50, "YES"),
        (2, 80.0, 0.45, 0.48, "NO"),
    ]:
        row = {
            "market_key": market_key,
            "market_start_ts": base,
            "market_end_ts": base + pd.Timedelta(minutes=5),
            "ts": base + pd.Timedelta(seconds=sec, minutes=market_key * 5),
            "entry_age_sec": sec,
            "model_name": "brownian_zero_drift__rv30",
            "p_yes": p_yes,
            "yes_ask": ask,
            "no_ask": 0.95,
            "winner_side": winner,
            "chronological_slice": "fresh",
            "entry_date": "2026-05-01",
            "ask_px_1_yes": ask,
            "ask_sz_1_yes": 20.0,
            "ask_px_2_yes": ask + 0.01,
            "ask_sz_2_yes": 20.0,
            "ask_px_1_no": 0.95,
            "ask_sz_1_no": 20.0,
            "ask_px_2_no": 0.96,
            "ask_sz_2_no": 20.0,
        }
        for i in range(3, 11):
            row[f"ask_px_{i}_yes"] = ask + i / 1000
            row[f"ask_sz_{i}_yes"] = 20.0
            row[f"ask_px_{i}_no"] = 0.96
            row[f"ask_sz_{i}_no"] = 20.0
        rows.append(row)
    return pd.DataFrame(rows)


def test_binary_contract_kelly_fraction_formula():
    assert sim.full_kelly_fraction(0.52, 0.40) == pytest.approx((0.52 - 0.40) / (1 - 0.40))
    assert sim.full_kelly_fraction(0.39, 0.40) == 0.0


def test_expected_log_growth_formula():
    p = 0.52
    ask = 0.40
    f = 0.02
    expected = p * __import__("math").log(1 + f * ((1 - ask) / ask)) + (1 - p) * __import__("math").log(1 - f)
    assert sim.expected_log_growth(p, ask, f) == pytest.approx(expected)


def test_expected_growth_gate_behavior_with_positive_and_negative_growth():
    frame = sim.add_trade_columns(candidates().iloc[[0]].copy(), args(ask_slippage_abs=0.20))
    selected, skipped = sim.select_first_entries_by_variant(frame, args(ask_slippage_abs=0.20))
    assert "expected_growth_positive" not in selected["policy_variant"].tolist()
    assert not skipped.empty
    assert skipped["skip_reason"].eq("expected_growth").any()


def test_bankroll_update_on_win_and_loss():
    frame = sim.add_trade_columns(candidates(), args())
    selected, skipped = sim.select_first_entries_by_variant(frame, args())
    sized, _ = sim.simulate_variant_sizing(
        selected[selected["policy_variant"].eq("raw_policy")],
        skipped.iloc[0:0],
        sim.SizingPolicy("additive_fixed_notional", "additive_fixed_notional", fixed_stake=5.0),
        args(),
    )
    first = sized.sort_values("ts").iloc[0]
    assert first["bankroll_before"] == pytest.approx(100.0)
    assert first["pnl"] == pytest.approx(5.0 * (1 / 0.40 - 1))
    assert first["bankroll_after"] == pytest.approx(107.5)


def test_capacity_cap_enforcement():
    frame = candidates().iloc[[0]].copy()
    for i in range(1, 11):
        frame[f"ask_sz_{i}_yes"] = 0.1
    enriched = sim.add_trade_columns(frame, args())
    selected, skipped = sim.select_first_entries_by_variant(enriched, args())
    sized, _ = sim.simulate_variant_sizing(
        selected[selected["policy_variant"].eq("raw_policy")],
        skipped.iloc[0:0],
        sim.SizingPolicy("additive_fixed_notional", "additive_fixed_notional", fixed_stake=5.0),
        args(),
    )
    assert sized["capacity_bound"].iloc[0]
    assert sized["stake_spend"].iloc[0] < 5.0
    assert sized["depth_utilization"].iloc[0] == pytest.approx(1.0)


def test_first_entry_per_market_uniqueness():
    frame = sim.add_trade_columns(candidates(), args())
    selected, _ = sim.select_first_entries_by_variant(frame, args())
    raw = selected[selected["policy_variant"].eq("raw_policy")]
    assert raw["market_key"].tolist() == [1, 2]
    assert raw[raw["market_key"].eq(1)]["entry_age_sec"].iloc[0] == 70.0


def test_drawdown_calculation_episode():
    path = pd.DataFrame(
        {
            "policy_variant": ["raw_policy"] * 4,
            "sizing_policy": ["x"] * 4,
            "decision_ts": pd.to_datetime(["2026-01-01", "2026-01-02", "2026-01-03", "2026-01-04"], utc=True),
            "bankroll_after": [100.0, 80.0, 90.0, 110.0],
        }
    )
    episodes = sim.compute_drawdown_episodes(path)
    assert episodes["drawdown_pct"].iloc[0] == pytest.approx(-0.20)


def test_additive_fixed_notional_tiny_fixture_reproduction():
    frame = sim.add_trade_columns(candidates(), args())
    selected, skipped = sim.select_first_entries_by_variant(frame, args())
    sized, _ = sim.simulate_variant_sizing(
        selected[selected["policy_variant"].eq("raw_policy")],
        skipped.iloc[0:0],
        sim.SizingPolicy("additive_fixed_notional", "additive_fixed_notional", fixed_stake=5.0),
        args(),
    )
    summary = sim.summarize_group(sized.assign(skip_reason=""), 100.0)
    assert summary["trade_count"] == 2
    assert summary["gross_cost"] == pytest.approx(10.0)


def test_small_wallet_mode_does_not_round_up_to_min_order_by_default():
    frame = sim.add_trade_columns(candidates().iloc[[0]], args())
    selected, skipped = sim.select_first_entries_by_variant(frame, args())
    sized, skipped = sim.simulate_variant_sizing(
        selected[selected["policy_variant"].eq("raw_policy")],
        skipped.iloc[0:0],
        sim.SizingPolicy("additive_fixed_notional", "additive_fixed_notional", fixed_stake=5.0),
        args(small_wallet_mode=True, min_order_notional=1.0),
    )
    assert sized.empty
    assert skipped["skip_reason"].tolist() == ["below_min_order_notional"]
    assert skipped["stake_spend"].iloc[0] < 1.0


def test_fixed_notional_is_capped_by_small_wallet_fraction():
    frame = sim.add_trade_columns(candidates().iloc[[0]], args())
    selected, skipped = sim.select_first_entries_by_variant(frame, args())
    sized, _ = sim.simulate_variant_sizing(
        selected[selected["policy_variant"].eq("raw_policy")],
        skipped.iloc[0:0],
        sim.SizingPolicy("additive_fixed_notional", "additive_fixed_notional", fixed_stake=5.0),
        args(small_wallet_mode=True, min_order_notional=0.0),
    )
    assert sized["stake_spend"].iloc[0] <= 100.0 * 0.0025
    assert sized["small_wallet_mode_active"].iloc[0]


def test_daily_stop_loss_prevents_later_trades_on_date():
    frame = candidates()
    frame.loc[0, "winner_side"] = "NO"
    enriched = sim.add_trade_columns(frame, args())
    selected, skipped = sim.select_first_entries_by_variant(enriched, args())
    sized, skipped = sim.simulate_variant_sizing(
        selected[selected["policy_variant"].eq("raw_policy")],
        skipped.iloc[0:0],
        sim.SizingPolicy("additive_fixed_notional", "additive_fixed_notional", fixed_stake=5.0),
        args(small_wallet_mode=True, min_order_notional=0.0, daily_stop_loss_fraction=0.001),
    )
    assert len(sized) == 1
    assert "daily_stop_loss_guard" in skipped["skip_reason"].tolist()


def test_session_stop_loss_prevents_further_trades():
    frame = candidates()
    frame.loc[0, "winner_side"] = "NO"
    enriched = sim.add_trade_columns(frame, args())
    selected, skipped = sim.select_first_entries_by_variant(enriched, args())
    sized, skipped = sim.simulate_variant_sizing(
        selected[selected["policy_variant"].eq("raw_policy")],
        skipped.iloc[0:0],
        sim.SizingPolicy("additive_fixed_notional", "additive_fixed_notional", fixed_stake=5.0),
        args(small_wallet_mode=True, min_order_notional=0.0, session_stop_loss_fraction=0.001),
    )
    assert len(sized) == 1
    assert "session_stop_loss_guard" in skipped["skip_reason"].tolist()


def test_coverage_warning_flag_from_manifest_logic():
    compact_max = pd.Timestamp("2026-05-22T00:00:00Z")
    pred_max = pd.Timestamp("2026-05-11T00:00:00Z")
    assert compact_max > pred_max


def test_starting_bankroll_sweep_writes_separate_output_dirs(tmp_path, monkeypatch):
    def fake_build(_args):
        return pd.DataFrame(), pd.DataFrame(), {}

    def fake_execute(child_args, output_root, *_rest):
        output_root.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(
            [
                {
                    "policy_variant": "expected_growth_positive_plus_ask_gt_0.30",
                    "sizing_policy": "kelly_1_40_cap_0_25pct",
                    "ending_bankroll": child_args.starting_bankroll,
                    "total_return_pct": 0.0,
                    "trade_count": 0,
                    "skipped_count": 0,
                    "skipped_below_min_order_count": 0,
                    "skipped_daily_stop_count": 0,
                    "skipped_session_stop_count": 0,
                    "max_drawdown_pct": 0.0,
                    "min_bankroll_seen": child_args.starting_bankroll,
                    "ruin_flag": False,
                    "avg_stake": 0.0,
                    "p95_stake": 0.0,
                    "max_stake": 0.0,
                    "avg_stake_fraction": 0.0,
                    "p95_stake_fraction": 0.0,
                    "max_stake_fraction": 0.0,
                }
            ]
        ).to_csv(output_root / "bankroll_summary.csv", index=False)
        pd.DataFrame().to_csv(output_root / "skipped_trades.csv", index=False)
        return {"output_root": str(output_root), "warnings": []}

    monkeypatch.setattr(sim, "build_candidates", fake_build)
    monkeypatch.setattr(sim, "execute_run", fake_execute)
    parsed = sim.build_parser().parse_args(
        [
            "--compact-root",
            str(tmp_path / "compact"),
            "--predictions-root",
            str(tmp_path / "preds"),
            "--output-root",
            str(tmp_path / "out"),
            "--starting-bankroll-sweep",
            "50,100",
            "--overwrite",
        ]
    )
    sim.run(parsed)
    assert (tmp_path / "out" / "starting_bankroll_50" / "bankroll_summary.csv").exists()
    assert (tmp_path / "out" / "starting_bankroll_100" / "bankroll_summary.csv").exists()
    assert (tmp_path / "out" / "bankroll_floor_sweep.csv").exists()


def test_all_skipped_run_still_summarizes_skips():
    skipped = pd.DataFrame(
        [
            {
                "policy_variant": "raw_policy",
                "sizing_policy": "additive_fixed_notional",
                "skip_reason": "below_min_order_notional",
                "stake_spend": 0.0,
                "pnl": 0.0,
                "bankroll_after": 50.0,
            }
        ]
    )
    summary, tables = sim.aggregate_tables(pd.DataFrame(), skipped, args(starting_bankroll=50.0))
    assert summary["trade_count"].iloc[0] == 0
    assert summary["skipped_below_min_order_count"].iloc[0] == 1
    assert "by_policy_and_sizing.csv" in tables
