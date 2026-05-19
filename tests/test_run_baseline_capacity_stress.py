from pathlib import Path

import pandas as pd
import pytest

from scripts import run_baseline_capacity_stress as baseline


def _compact_fixture(root: Path):
    windows = pd.DataFrame(
        {
            "market_key": [1, 2, 3],
            "market_start_ts": pd.to_datetime(["2026-04-24T00:00:00Z", "2026-05-02T00:00:00Z", "2026-05-10T00:00:00Z"], utc=True),
            "market_end_ts": pd.to_datetime(["2026-04-24T00:05:00Z", "2026-05-02T00:05:00Z", "2026-05-10T00:05:00Z"], utc=True),
            "winner_side": ["YES", "YES", None],
        }
    )
    ticks = pd.DataFrame(
        {
            "market_key": [1, 1, 2, 2, 3],
            "ts": pd.to_datetime(
                [
                    "2026-04-24T00:00:10Z",
                    "2026-04-24T00:00:20Z",
                    "2026-05-02T00:01:00Z",
                    "2026-05-02T00:01:10Z",
                    "2026-05-10T00:01:00Z",
                ],
                utc=True,
            ),
            "side": ["YES", "NO", "YES", "NO", "YES"],
            "source": ["test"] * 5,
            "market_age_sec": [10.0, 20.0, 60.0, 70.0, 60.0],
            "seconds_to_end": [290.0, 280.0, 240.0, 230.0, 240.0],
            "is_valid_topbook": [True, True, False, True, True],
            "ask_px_1": [0.40, 0.40, 0.40, 0.45, 0.40],
            "ask_sz_1": [1.0, 100.0, 100.0, 1.0, 1.0],
            "ask_px_2": [0.50, 0.50, 0.50, 0.55, 0.50],
            "ask_sz_2": [2.0, 100.0, 100.0, 2.0, 1.0],
            "ask_px_3": [0.90, 0.90, 0.90, 0.90, 0.90],
            "ask_sz_3": [10.0, 100.0, 100.0, 10.0, 1.0],
            "bid_px_1": [0.39, 0.39, 0.50, 0.44, 0.39],
            "bid_sz_1": [1.0, 1.0, 1.0, 1.0, 1.0],
        }
    )
    root.mkdir(parents=True, exist_ok=True)
    windows.to_parquet(root / "market_windows.parquet", index=False)
    ticks.to_parquet(root / "book_ticks.parquet", index=False)


def test_one_entry_per_market_is_enforced(tmp_path):
    _compact_fixture(tmp_path)
    windows, ticks = baseline.load_compact(tmp_path)
    prepared = baseline.prepare_ticks(ticks, windows, valid_topbook_only=True, entry_age_min=0, entry_age_max=300)
    entries, _ = baseline.select_first_entries(prepared, windows, [0.45], True)
    assert entries[entries["market_key"].eq(1)]["ts"].iloc[0] == pd.Timestamp("2026-04-24T00:00:10Z")
    assert entries.groupby(["market_key", "ask_threshold"]).size().max() == 1


def test_capacity_aware_fill_cannot_exceed_visible_size_and_consumes_top_n():
    row = pd.Series({"ask_px_1": 0.40, "ask_sz_1": 1.0, "ask_px_2": 0.50, "ask_sz_2": 2.0, "ask_px_3": 0.90, "ask_sz_3": 10.0})
    fill = baseline.fill_against_asks(row, stake=2.0, top_n=2, capacity_aware=True)
    assert fill["filled_notional"] == pytest.approx(1.4)
    assert fill["filled_shares"] == pytest.approx(1.0 + 2.0)
    assert fill["capacity_shortfall"] is True


def test_top_n_depth_limit_is_respected():
    row = pd.Series({"ask_px_1": 0.40, "ask_sz_1": 1.0, "ask_px_2": 0.50, "ask_sz_2": 2.0, "ask_px_3": 0.90, "ask_sz_3": 10.0})
    fill_one_level = baseline.fill_against_asks(row, stake=2.0, top_n=1, capacity_aware=True)
    fill_three_levels = baseline.fill_against_asks(row, stake=2.0, top_n=3, capacity_aware=True)
    assert fill_one_level["filled_notional"] == pytest.approx(0.4)
    assert fill_three_levels["filled_notional"] == pytest.approx(2.0)


def test_winning_and_losing_payouts(tmp_path):
    _compact_fixture(tmp_path)
    windows, ticks = baseline.load_compact(tmp_path)
    prepared = baseline.prepare_ticks(ticks, windows, valid_topbook_only=True, entry_age_min=0, entry_age_max=300)
    entries, _ = baseline.select_first_entries(prepared, windows, [0.45], True)
    trades, _ = baseline.simulate_trades(entries, [1.0], 3, True, baseline.parse_age_buckets(baseline.DEFAULT_AGE_BUCKETS))
    win = trades[trades["market_key"].eq(1)].iloc[0]
    loss = trades[trades["market_key"].eq(2)].iloc[0]
    assert win["gross_payout"] == pytest.approx(win["filled_shares"])
    assert loss["gross_payout"] == pytest.approx(0.0)


def test_chronological_slice_labels():
    assert baseline.chronological_slice(pd.Timestamp("2026-04-24T00:00:00Z")) == "early"
    assert baseline.chronological_slice(pd.Timestamp("2026-05-02T00:00:00Z")) == "main"
    assert baseline.chronological_slice(pd.Timestamp("2026-05-10T00:00:00Z")) == "fresh"


def test_invalid_topbooks_are_skipped_when_enabled(tmp_path):
    _compact_fixture(tmp_path)
    windows, ticks = baseline.load_compact(tmp_path)
    unfiltered = baseline.prepare_ticks(ticks, windows, valid_topbook_only=False, entry_age_min=0, entry_age_max=300)
    prepared = baseline.prepare_ticks(ticks, windows, valid_topbook_only=True, entry_age_min=0, entry_age_max=300)
    invalid_keys = set(unfiltered[(~unfiltered["is_valid_topbook"].astype(bool)) & unfiltered["winner_side"].isin(["YES", "NO"])]["market_key"]) - set(prepared["market_key"])
    _, skipped = baseline.select_first_entries(prepared[prepared["market_key"].ne(2)], windows[windows["market_key"].eq(2)], [0.45], True, invalid_book_keys={2})
    assert skipped["skip_reason"].iloc[0] == "invalid_book"


def test_summary_totals_reconcile_and_slices_reported(tmp_path):
    _compact_fixture(tmp_path)
    manifest = baseline.run(
        baseline.build_parser().parse_args(
            [
                "--compact-root",
                str(tmp_path),
                "--output-root",
                str(tmp_path / "out"),
                "--run-name",
                "run",
                "--stake-sizes",
                "1",
                "--ask-thresholds",
                "0.45",
                "--overwrite",
            ]
        )
    )
    out = tmp_path / "out" / "run"
    summary = pd.read_csv(out / "stress_summary.csv")
    trades = pd.read_parquet(out / "trade_level_results.parquet")
    full = summary[summary["chronological_slice"].eq("full")].iloc[0]
    assert full["gross_pnl"] == pytest.approx(trades["gross_pnl"].sum())
    assert set(summary["chronological_slice"]) >= {"full", "early", "main"}
    assert manifest["markets_total"] == 3
