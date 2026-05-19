from pathlib import Path

import pandas as pd
import pytest

from scripts import run_probability_model_set_capacity_stress as stress
from scripts import run_probability_roi_attribution as attribution


def _compact(root: Path) -> None:
    root.mkdir(parents=True, exist_ok=True)
    windows = pd.DataFrame(
        {
            "market_key": [1, 2],
            "market_start_ts": pd.to_datetime(["2026-05-02T00:00:00Z", "2026-05-02T00:05:00Z"], utc=True),
            "market_end_ts": pd.to_datetime(["2026-05-02T00:05:00Z", "2026-05-02T00:10:00Z"], utc=True),
            "winner_side": ["YES", "NO"],
        }
    )
    ticks = pd.DataFrame(
        {
            "market_key": [1, 1, 1, 1, 2, 2],
            "ts": pd.to_datetime(
                [
                    "2026-05-02T00:00:10Z",
                    "2026-05-02T00:00:10Z",
                    "2026-05-02T00:00:20Z",
                    "2026-05-02T00:00:20Z",
                    "2026-05-02T00:05:10Z",
                    "2026-05-02T00:05:10Z",
                ],
                utc=True,
            ),
            "side": ["YES", "NO", "YES", "NO", "YES", "NO"],
            "market_age_sec": [10.0, 10.0, 20.0, 20.0, 10.0, 10.0],
            "seconds_to_end": [290.0, 290.0, 280.0, 280.0, 290.0, 290.0],
            "is_valid_topbook": [True] * 6,
            "ask_px_1": [0.40, 0.70, 0.35, 0.75, 0.60, 0.40],
            "ask_sz_1": [1.0, 10.0, 100.0, 10.0, 10.0, 1.0],
            "ask_px_2": [0.45, 0.80, 0.40, 0.80, 0.70, 0.45],
            "ask_sz_2": [2.0, 10.0, 100.0, 10.0, 10.0, 2.0],
            "ask_px_3": [0.50, 0.90, 0.45, 0.90, 0.80, 0.50],
            "ask_sz_3": [3.0, 10.0, 100.0, 10.0, 10.0, 3.0],
            "spread": [0.02] * 6,
        }
    )
    windows.to_parquet(root / "market_windows.parquet", index=False)
    ticks.to_parquet(root / "book_ticks.parquet", index=False)


def _predictions(root: Path) -> None:
    root.mkdir(parents=True, exist_ok=True)
    preds = pd.DataFrame(
        {
            "model_id": ["brownian_zero_drift__rv30", "brownian_zero_drift__rv30"],
            "timestamp": pd.to_datetime(["2026-05-02T00:00:00Z", "2026-05-02T00:05:00Z"], utc=True),
            "market_window_start": pd.to_datetime(["2026-05-02T00:00:00Z", "2026-05-02T00:05:00Z"], utc=True),
            "p_up": [0.60, 0.30],
        }
    )
    preds.to_parquet(root / "probability_predictions_sample.parquet", index=False)


def _run(tmp_path: Path, models: str = "brownian_zero_drift__rv30,baseline_50"):
    compact = tmp_path / "compact"
    preds = tmp_path / "preds"
    out_root = tmp_path / "out"
    _compact(compact)
    _predictions(preds)
    return stress.run(
        stress.build_parser().parse_args(
            [
                "--compact-root",
                str(compact),
                "--predictions-root",
                str(preds),
                "--output-root",
                str(out_root),
                "--run-name",
                "run",
                "--models",
                models,
                "--stake-sizes",
                "1,5",
                "--edge-thresholds",
                "0.01",
                "--entry-age-windows",
                "0:300",
            ]
        )
    ), out_root / "run", compact


def test_model_name_preserved_and_baseline_probability(tmp_path):
    _, out, _ = _run(tmp_path)
    trades = pd.read_parquet(out / "trade_level_results.parquet")
    assert {"model_name", "model_id"}.issubset(trades.columns)
    assert set(trades["model_name"]) == {"brownian_zero_drift__rv30", "baseline_50"}
    assert trades[trades["model_name"].eq("baseline_50")]["p_yes"].eq(0.5).all()
    by_model = pd.read_csv(out / "stress_summary_by_model.csv")
    assert set(by_model["model_name"]) == {"brownian_zero_drift__rv30", "baseline_50"}


def test_edges_and_side_selection(tmp_path):
    _, out, _ = _run(tmp_path)
    trades = pd.read_parquet(out / "trade_level_results.parquet")
    m1 = trades[(trades["model_name"].eq("brownian_zero_drift__rv30")) & (trades["market_key"].eq(1))].iloc[0]
    assert m1["yes_edge"] == pytest.approx(0.60 - 0.40)
    assert m1["no_edge"] == pytest.approx(0.40 - 0.70)
    assert m1["side"] == "YES"
    m2 = trades[(trades["model_name"].eq("brownian_zero_drift__rv30")) & (trades["market_key"].eq(2))].iloc[0]
    assert m2["side"] == "NO"
    assert m2["no_edge"] == pytest.approx(0.70 - 0.40)


def test_no_trade_when_both_edges_below_threshold(tmp_path):
    _, out, _ = _run(tmp_path, models="baseline_50")
    trades = pd.read_parquet(out / "trade_level_results.parquet")
    assert not trades[trades["market_key"].eq(1)].empty
    # Later market 1 quotes can reach a 0.15 baseline edge, so 0.16 should block all trades.
    compact = tmp_path / "compact2"
    out_root = tmp_path / "out2"
    _compact(compact)
    stress.run(
        stress.build_parser().parse_args(
            [
                "--compact-root",
                str(compact),
                "--output-root",
                str(out_root),
                "--run-name",
                "run",
                "--models",
                "baseline_50",
                "--edge-thresholds",
                "0.16",
                "--stake-sizes",
                "1",
            ]
        )
    )
    high = pd.read_parquet(out_root / "run" / "trade_level_results.parquet")
    assert high.empty


def test_one_entry_per_market_per_model_config_and_capacity_top_n(tmp_path):
    _, out, _ = _run(tmp_path)
    trades = pd.read_parquet(out / "trade_level_results.parquet")
    key_cols = ["model_name", "market_key", "stake_size", "edge_threshold", "entry_age_window"]
    assert trades.groupby(key_cols).size().max() == 1
    row = trades[(trades["model_name"].eq("brownian_zero_drift__rv30")) & (trades["market_key"].eq(1)) & (trades["stake_size"].eq(5.0))].iloc[0]
    assert row["gross_cost"] == pytest.approx(1 * 0.40 + 2 * 0.45 + 3 * 0.50)
    assert row["capacity_shortfall"] == True


def test_chainlink_winner_side_used_for_payout_and_summary_reconciles(tmp_path):
    _, out, _ = _run(tmp_path)
    trades = pd.read_parquet(out / "trade_level_results.parquet")
    yes_win = trades[(trades["market_key"].eq(1)) & (trades["side"].eq("YES"))].iloc[0]
    assert yes_win["gross_payout"] == pytest.approx(yes_win["filled_shares"])
    no_win = trades[(trades["market_key"].eq(2)) & (trades["side"].eq("NO"))].iloc[0]
    assert no_win["gross_payout"] == pytest.approx(no_win["filled_shares"])
    summary = pd.read_csv(out / "stress_summary_by_model.csv")
    assert summary["gross_pnl"].sum() == pytest.approx(trades["gross_pnl"].sum())


def test_missing_model_report_and_no_silent_substitution(tmp_path):
    compact = tmp_path / "compact"
    preds = tmp_path / "preds"
    out_root = tmp_path / "out"
    _compact(compact)
    _predictions(preds)
    with pytest.raises(RuntimeError):
        stress.run(
            stress.build_parser().parse_args(
                [
                    "--compact-root",
                    str(compact),
                    "--predictions-root",
                    str(preds),
                    "--output-root",
                    str(out_root),
                    "--run-name",
                    "run",
                    "--models",
                    "gbm_blended_sigma__50_30_20",
                ]
            )
        )
    report = out_root / "run" / "missing_model_report.json"
    assert report.exists()
    assert "gbm_blended_sigma__50_30_20" in report.read_text()


def test_roi_attribution_smoke_on_new_output(tmp_path):
    _, out, compact = _run(tmp_path)
    attr_out = tmp_path / "attr"
    manifest = attribution.run(
        attribution.build_parser().parse_args(
            [
                "--strategy-run-root",
                str(out),
                "--compact-root",
                str(compact),
                "--output-root",
                str(attr_out),
                "--run-name",
                "run",
                "--min-markets-per-bucket",
                "1",
            ]
        )
    )
    assert manifest["missing_required_columns"] == []
    assert (attr_out / "run" / "candidate_veto_report.csv").exists()
