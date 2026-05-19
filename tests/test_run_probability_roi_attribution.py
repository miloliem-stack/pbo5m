from pathlib import Path

import pandas as pd
import pytest

from scripts import run_probability_roi_attribution as roi


def _write_strategy_fixture(root: Path) -> pd.DataFrame:
    root.mkdir(parents=True, exist_ok=True)
    trades = pd.DataFrame(
        {
            "market_key": [1, 2, 3, 4],
            "market_start_ts": pd.to_datetime(
                [
                    "2026-04-24T00:00:00Z",
                    "2026-05-02T00:00:00Z",
                    "2026-05-10T00:00:00Z",
                    "2026-05-10T01:00:00Z",
                ],
                utc=True,
            ),
            "ts": pd.to_datetime(
                [
                    "2026-04-24T00:00:04Z",
                    "2026-05-02T00:00:12Z",
                    "2026-05-10T00:01:10Z",
                    "2026-05-10T01:03:00Z",
                ],
                utc=True,
            ),
            "side": ["YES", "NO", "YES", "NO"],
            "winner_side": ["YES", "YES", "NO", "NO"],
            "p_yes": [0.50, 0.40, 0.56, 0.45],
            "entry_ask": [0.49, 0.47, 0.40, 0.55],
            "model_edge": [0.01, 0.13, 0.16, 0.00],
            "edge_threshold": [0.01, 0.01, 0.01, 0.01],
            "stake_size": [1.0, 1.0, 1.0, 1.0],
            "entry_age_sec": [4.0, 12.0, 70.0, 180.0],
            "filled_shares": [2.040816, 2.12766, 2.5, 1.81818],
            "gross_cost": [1.0, 1.0, 1.0, 1.0],
            "gross_payout": [2.040816, 0.0, 0.0, 1.81818],
            "gross_pnl": [1.040816, -1.0, -1.0, 0.81818],
            "roi_on_filled_cost": [1.040816, -1.0, -1.0, 0.81818],
            "win": [True, False, False, True],
            "fill_rate": [1.0, 1.0, 1.0, 1.0],
            "capacity_shortfall": [False, False, True, False],
        }
    )
    trades.to_parquet(root / "trade_level_results.parquet", index=False)
    pd.DataFrame({"market_key": [1, 2, 3, 4]}).to_parquet(root / "market_level_results.parquet", index=False)
    return trades


def _write_compact_fixture(root: Path) -> None:
    root.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        {
            "market_key": [1, 2, 3, 4],
            "reference_price": [100.0, 100.0, 100.0, 100.0],
            "chainlink_close_price": [101.0, 99.0, 99.0, 101.0],
        }
    ).to_parquet(root / "market_windows.parquet", index=False)
    pd.DataFrame(
        {
            "market_key": [1, 2, 3, 4],
            "ts": pd.to_datetime(
                [
                    "2026-04-24T00:00:04Z",
                    "2026-05-02T00:00:12Z",
                    "2026-05-10T00:01:10Z",
                    "2026-05-10T01:03:00Z",
                ],
                utc=True,
            ),
            "side": ["YES", "NO", "YES", "NO"],
            "spread": [0.01, 0.02, 0.04, 0.06],
            "ask_sz_1": [1.0, 2.0, 10.0, 200.0],
            "ask_sz_2": [1.0, 2.0, 10.0, 1.0],
            "ask_sz_3": [1.0, 2.0, 10.0, 1.0],
        }
    ).to_parquet(root / "book_ticks.parquet", index=False)


def test_bins_are_assigned_correctly(tmp_path):
    trades = _write_strategy_fixture(tmp_path / "strategy")
    binned = roi.assign_bins(trades)
    assert binned.loc[0, "edge_bin"] == "0.01_0.02"
    assert binned.loc[0, "ask_bin"] == "0.47_0.49"
    assert binned.loc[0, "market_age_bucket"] == "0_5s"
    assert binned.loc[1, "chronological_slice"] == "main"
    assert binned.loc[2, "chronological_slice"] == "fresh"


def test_grouped_totals_reconcile_with_raw_trades(tmp_path):
    trades = roi.assign_bins(_write_strategy_fixture(tmp_path / "strategy"))
    grouped = roi.aggregate_metrics(trades.assign(full="full"), ["full"]).iloc[0]
    assert grouped["gross_cost"] == pytest.approx(trades["gross_cost"].sum())
    assert grouped["gross_pnl"] == pytest.approx(trades["gross_pnl"].sum())
    assert grouped["roi_on_filled_cost"] == pytest.approx(trades["gross_pnl"].sum() / trades["gross_cost"].sum())


def test_candidate_veto_recomputes_remaining_roi_and_min_market_filter(tmp_path):
    trades = roi.assign_bins(_write_strategy_fixture(tmp_path / "strategy"))
    report = roi.make_veto_report(trades, ["ask_bin"], min_markets_per_bucket=2)
    small = report[report["bucket_value"].eq("0.35_0.40")]
    assert not small.empty
    assert small.iloc[0]["is_candidate_veto"] == False
    report_loose = roi.make_veto_report(trades, ["ask_bin"], min_markets_per_bucket=1)
    veto = report_loose[report_loose["bucket_value"].eq("0.35_0.40")].iloc[0]
    remaining = trades[trades["ask_bin"].ne("0.35_0.40")]
    assert veto["remaining_roi"] == pytest.approx(remaining["gross_pnl"].sum() / remaining["gross_cost"].sum())


def test_script_outputs_and_optional_columns_do_not_crash(tmp_path):
    strategy = tmp_path / "strategy"
    compact = tmp_path / "compact"
    out_root = tmp_path / "out"
    _write_strategy_fixture(strategy)
    _write_compact_fixture(compact)
    manifest = roi.run(
        roi.build_parser().parse_args(
            [
                "--strategy-run-root",
                str(strategy),
                "--compact-root",
                str(compact),
                "--output-root",
                str(out_root),
                "--run-name",
                "run",
                "--min-markets-per-bucket",
                "1",
            ]
        )
    )
    out = out_root / "run"
    assert (out / "roi_by_day.csv").exists()
    assert (out / "roi_by_spread_bucket.csv").exists()
    assert (out / "roi_by_depth_bucket.csv").exists()
    assert (out / "candidate_veto_report.csv").exists()
    assert manifest["missing_required_columns"] == []
    by_day = pd.read_csv(out / "roi_by_day.csv")
    assert by_day["gross_pnl"].sum() == pytest.approx(pd.read_parquet(strategy / "trade_level_results.parquet")["gross_pnl"].sum())


def test_missing_optional_columns_are_reported(tmp_path):
    strategy = tmp_path / "strategy"
    compact = tmp_path / "compact"
    out_root = tmp_path / "out"
    trades = _write_strategy_fixture(strategy).drop(columns=["p_yes", "model_edge"])
    trades.to_parquet(strategy / "trade_level_results.parquet", index=False)
    compact.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"market_key": [1, 2, 3, 4]}).to_parquet(compact / "market_windows.parquet", index=False)
    manifest = roi.run(
        roi.build_parser().parse_args(
            [
                "--strategy-run-root",
                str(strategy),
                "--compact-root",
                str(compact),
                "--output-root",
                str(out_root),
                "--run-name",
                "run",
            ]
        )
    )
    assert "p_yes" in manifest["missing_optional_columns"]
    assert "model_edge" in manifest["missing_optional_columns"]
