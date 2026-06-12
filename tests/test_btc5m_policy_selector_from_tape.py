from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from scripts import run_btc5m_policy_selector_from_tape as selector


def _tape() -> pd.DataFrame:
    rows = [
        # c1 first row fails age filter; second row is the correct causal selected trade.
        {"ts": "2026-05-01T00:00:50Z", "condition_id": "c1", "market_id": "m1", "side": "YES", "ask": 0.35, "raw_edge": 0.20, "market_age_seconds": 50, "p_side_model": 0.55, "won_if_bought": 1, "realized_payout_per_share": 1, "side_top_depth_10_usd": 100, "is_best_buy_side": True},
        {"ts": "2026-05-01T00:01:10Z", "condition_id": "c1", "market_id": "m1", "side": "YES", "ask": 0.35, "raw_edge": 0.10, "market_age_seconds": 70, "p_side_model": 0.45, "won_if_bought": 1, "realized_payout_per_share": 1, "side_top_depth_10_usd": 100, "is_best_buy_side": True},
        # c2 is used for side-specific and best-buy tests.
        {"ts": "2026-05-01T00:01:00Z", "condition_id": "c2", "market_id": "m2", "side": "YES", "ask": 0.39, "raw_edge": 0.03, "market_age_seconds": 60, "p_side_model": 0.42, "won_if_bought": 0, "realized_payout_per_share": 0, "side_top_depth_10_usd": 100, "is_best_buy_side": False},
        {"ts": "2026-05-01T00:01:05Z", "condition_id": "c2", "market_id": "m2", "side": "NO", "ask": 0.42, "raw_edge": 0.04, "market_age_seconds": 65, "p_side_model": 0.46, "won_if_bought": 1, "realized_payout_per_share": 1, "side_top_depth_10_usd": 100, "is_best_buy_side": True},
        # c3 falls inside excluded age interval.
        {"ts": "2026-05-01T00:01:40Z", "condition_id": "c3", "market_id": "m3", "side": "YES", "ask": 0.35, "raw_edge": 0.10, "market_age_seconds": 100, "p_side_model": 0.45, "won_if_bought": 0, "realized_payout_per_share": 0, "side_top_depth_10_usd": 100, "is_best_buy_side": True},
    ]
    out = pd.DataFrame(rows)
    out["ts"] = pd.to_datetime(out["ts"], utc=True)
    out["gross_marker"] = 1
    return out


def test_policy_filters_before_first_entry_selection():
    tape = _tape()
    policy = {"min_market_age_seconds": 60, "max_market_age_seconds": 240, "min_ask": 0.30, "max_ask": 0.40, "min_edge": 0.02}
    selected = selector.select_first_entries(selector.apply_policy_filters(tape, policy), True)

    assert selected[selected["condition_id"].eq("c1")]["market_age_seconds"].iloc[0] == 70

    wrong_posthoc = tape.sort_values("ts").drop_duplicates("condition_id", keep="first")
    wrong_posthoc = selector.apply_policy_filters(wrong_posthoc, policy)
    assert "c1" not in set(wrong_posthoc["condition_id"])


def test_first_entry_selection_is_per_condition_id():
    policy = {"min_market_age_seconds": 60, "max_market_age_seconds": 240, "min_edge": 0.02}
    selected = selector.select_first_entries(selector.apply_policy_filters(_tape(), policy), True)
    assert selected["condition_id"].is_unique


def test_side_specific_rules_and_best_buy_side():
    policy = {
        "min_market_age_seconds": 60,
        "max_market_age_seconds": 240,
        "require_best_buy_side": True,
        "side_rules": {
            "YES": {"min_ask": 0.30, "max_ask": 0.40, "min_edge": 0.02},
            "NO": {"min_ask": 0.35, "max_ask": 0.45, "min_edge": 0.02},
        },
    }
    selected = selector.select_first_entries(selector.apply_policy_filters(_tape(), policy), True)
    c2 = selected[selected["condition_id"].eq("c2")].iloc[0]
    assert c2["side"] == "NO"


def test_exclude_age_interval():
    policy = {"min_market_age_seconds": 60, "max_market_age_seconds": 240, "min_edge": 0.02, "exclude_age_intervals": [{"start": 90, "end": 120}]}
    selected = selector.select_first_entries(selector.apply_policy_filters(_tape(), policy), True)
    assert "c3" not in set(selected["condition_id"])


def test_add_pnl_formula():
    trades = selector.add_pnl(_tape().iloc[[1]], stake_usd=1.0)
    assert trades["shares"].iloc[0] == 1.0 / 0.35
    assert trades["pnl"].iloc[0] == (1.0 / 0.35) - 1.0


def test_selector_writes_outputs(tmp_path: Path):
    tape_path = tmp_path / "tape.parquet"
    policy_path = tmp_path / "policies.yaml"
    out_dir = tmp_path / "out"
    _tape().to_parquet(tape_path, index=False)
    policy_path.write_text(
        json.dumps(
            {
                "policies": {
                    "causal": {"min_market_age_seconds": 60, "max_market_age_seconds": 240, "min_ask": 0.30, "max_ask": 0.40, "min_edge": 0.02},
                    "exclude": {"min_market_age_seconds": 60, "max_market_age_seconds": 240, "min_edge": 0.02, "exclude_age_intervals": [{"start": 90, "end": 120}]},
                }
            }
        ),
        encoding="utf-8",
    )
    result = selector.run_selector(
        argparse.Namespace(
            tape=tape_path,
            out_dir=out_dir,
            policies=policy_path,
            stake_usd=1.0,
            first_entry_per_condition=True,
            start_ts=None,
            end_ts=None,
            overwrite=False,
        )
    )
    assert result["policy_count"] == 2
    assert (out_dir / "policy_comparison.csv").exists()
    assert (out_dir / "policy_comparison_by_split.csv").exists()
    assert (out_dir / "causal" / "selected_trades.parquet").exists()
