from pathlib import Path

import pandas as pd
import pytest

from scripts import run_hmm_regime_veto_attribution as veto


def synthetic_trades() -> pd.DataFrame:
    return veto.normalize_trades(
        pd.DataFrame(
            {
                "entry_ts": pd.to_datetime(
                    [
                        "2026-05-01T00:01:30Z",
                        "2026-05-01T00:02:30Z",
                        "2026-05-02T00:01:30Z",
                        "2026-05-02T00:02:30Z",
                        "2026-05-09T00:01:30Z",
                        "2026-05-09T00:02:30Z",
                    ],
                    utc=True,
                ),
                "market_key": [1, 2, 3, 4, 5, 6],
                "model_name": ["m"] * 6,
                "side": ["YES"] * 6,
                "stake_size": [1.0] * 6,
                "edge_threshold": [0.01] * 6,
                "entry_age_window": ["0_300"] * 6,
                "entry_age_sec": [90.0, 150.0, 90.0, 150.0, 90.0, 150.0],
                "entry_ask": [0.40, 0.41, 0.42, 0.43, 0.44, 0.45],
                "gross_cost": [1.0] * 6,
                "gross_pnl": [-1.0, 0.5, -1.0, 0.5, -1.0, 0.5],
                "chronological_slice": ["early", "early", "main", "main", "fresh", "fresh"],
                "entry_date": ["2026-05-01", "2026-05-01", "2026-05-02", "2026-05-02", "2026-05-09", "2026-05-09"],
                "winner_side": ["NO", "YES", "NO", "YES", "NO", "YES"],
                "win": [False, True, False, True, False, True],
            }
        )
    )


def synthetic_states() -> pd.DataFrame:
    return veto.normalize_hmm_states(
        pd.DataFrame(
            {
                "timestamp": pd.to_datetime(
                    [
                        "2026-05-01T00:00:00Z",
                        "2026-05-01T00:02:00Z",
                        "2026-05-02T00:00:00Z",
                        "2026-05-02T00:02:00Z",
                        "2026-05-09T00:00:00Z",
                        "2026-05-09T00:02:00Z",
                        "2026-05-09T00:03:00Z",
                    ],
                    utc=True,
                ),
                "candidate_model_id": ["hmm"] * 7,
                "raw_state_id": [1, 2, 1, 2, 1, 2, 2],
                "p_max": [0.95, 0.95, 0.95, 0.95, 0.65, 0.95, 0.95],
            }
        )
    )


def test_asof_join_uses_previous_hmm_timestamp_never_future():
    trades = synthetic_trades().iloc[[0]].copy()
    states = veto.normalize_hmm_states(
        pd.DataFrame(
            {
                "timestamp": pd.to_datetime(["2026-05-01T00:01:00Z", "2026-05-01T00:02:00Z"], utc=True),
                "hmm_model_id": ["hmm", "hmm"],
                "hmm_state": [7, 9],
                "hmm_pmax": [0.8, 0.99],
            }
        )
    )
    attached = veto.attach_hmm_to_trades(trades, states, ["hmm"])
    assert attached["hmm_state"].iloc[0] == 7
    assert attached["hmm_pmax"].iloc[0] == pytest.approx(0.8)


def test_veto_recomputes_pnl_roi_and_loss_profit_shares():
    attached = veto.attach_hmm_to_trades(synthetic_trades(), synthetic_states(), ["hmm"])
    scan = veto.single_state_veto_scan(attached, [0.90])
    row = scan[(scan["hmm_state"].eq(1)) & (scan["pmax_threshold"].eq(0.90))].iloc[0]
    assert row["vetoed_trades"] == 2
    assert row["pnl_before"] == pytest.approx(-1.5)
    assert row["pnl_after"] == pytest.approx(0.5)
    assert row["pnl_lift"] == pytest.approx(2.0)
    assert row["roi_before"] == pytest.approx(-1.5 / 6.0)
    assert row["roi_after"] == pytest.approx(0.5 / 4.0)
    assert row["total_losses_before"] == 3
    assert row["losses_removed"] == 2
    assert row["loss_share_removed"] == pytest.approx(2 / 3)
    assert row["total_profits_before"] == 3
    assert row["profits_removed"] == 0
    assert row["profit_share_removed"] == pytest.approx(0.0)


def test_pmax_threshold_filtering_changes_veto_support():
    attached = veto.attach_hmm_to_trades(synthetic_trades(), synthetic_states(), ["hmm"])
    scan = veto.single_state_veto_scan(attached, [0.60, 0.90])
    low = scan[(scan["hmm_state"].eq(1)) & (scan["pmax_threshold"].eq(0.60))].iloc[0]
    high = scan[(scan["hmm_state"].eq(1)) & (scan["pmax_threshold"].eq(0.90))].iloc[0]
    assert low["vetoed_trades"] == 3
    assert high["vetoed_trades"] == 2


def test_veto_scan_uses_per_hmm_model_base_universe_not_stacked_rows():
    trades = synthetic_trades().iloc[:2].copy()
    states = veto.normalize_hmm_states(
        pd.DataFrame(
            {
                "timestamp": pd.to_datetime(
                    [
                        "2026-05-01T00:00:00Z",
                        "2026-05-01T00:02:00Z",
                        "2026-05-01T00:00:00Z",
                        "2026-05-01T00:02:00Z",
                    ],
                    utc=True,
                ),
                "hmm_model_id": ["hmm_a", "hmm_a", "hmm_b", "hmm_b"],
                "hmm_state": [1, 2, 9, 8],
                "hmm_pmax": [0.95, 0.95, 0.95, 0.95],
            }
        )
    )
    attached = veto.attach_hmm_to_trades(trades, states, ["hmm_a", "hmm_b"])
    assert len(attached) == 4
    scan = veto.single_state_veto_scan(attached, [0.90])
    row_a = scan[(scan["hmm_model_id"].eq("hmm_a")) & (scan["hmm_state"].eq(1))].iloc[0]
    row_b = scan[(scan["hmm_model_id"].eq("hmm_b")) & (scan["hmm_state"].eq(9))].iloc[0]
    assert row_a["total_trades_before"] == 2
    assert row_b["total_trades_before"] == 2
    assert row_a["pnl_before"] == pytest.approx(-0.5)
    assert row_b["pnl_before"] == pytest.approx(-0.5)
    assert row_a["vetoed_trades"] == 1
    assert row_a["vetoed_trade_share"] == pytest.approx(0.5)


def test_frozen_selection_uses_train_rows_not_test_rows():
    attached = veto.attach_hmm_to_trades(synthetic_trades(), synthetic_states(), ["hmm"])
    validation = veto.frozen_veto_validation(
        attached,
        veto.single_state_veto_scan(attached, [0.90]),
        min_vetoed_trades=1,
        min_remaining_trades=1,
        min_vetoed_unique_markets=1,
    )
    top = validation.iloc[0]
    assert top["train_slices"] == "early"
    assert top["test_slices"] == "main,fresh"
    assert top["hmm_state"] == 1
    assert top["train_pnl_lift"] == pytest.approx(1.0)


def test_frozen_validation_evaluates_per_hmm_model_without_stacked_base_duplication():
    trades = synthetic_trades().copy()
    states_a = synthetic_states().assign(hmm_model_id="hmm_a")
    states_b = synthetic_states().assign(hmm_model_id="hmm_b")
    states_b["hmm_state"] = states_b["hmm_state"].replace({1: 9, 2: 8})
    attached = veto.attach_hmm_to_trades(trades, pd.concat([states_a, states_b], ignore_index=True), ["hmm_a", "hmm_b"])
    validation = veto.frozen_veto_validation(
        attached,
        veto.single_state_veto_scan(attached, [0.90]),
        min_vetoed_trades=1,
        min_remaining_trades=1,
        min_vetoed_unique_markets=1,
    )
    top = validation.iloc[0]
    assert top["test_total_trades_before"] == 4
    assert top["test_pnl_before"] == pytest.approx(-1.0)
    assert top["test_vetoed_trade_share"] == pytest.approx(0.25)


def test_missing_hmm_coverage_fails_loudly():
    trades = synthetic_trades()
    states = synthetic_states()
    short = states[states["timestamp"] < pd.Timestamp("2026-05-03T00:00:00Z", tz="UTC")]
    with pytest.raises(ValueError, match="HMM state coverage does not span full replay period"):
        veto.assert_hmm_coverage(trades, short, ["hmm"])


def test_reconstruction_splits_add_partial_tail_without_future_training():
    folds = veto.make_reconstruction_splits(n_rows=37, train_rows=10, test_rows=10, step_rows=10)
    assert [(f.train_start, f.train_end, f.test_start, f.test_end) for f in folds] == [
        (0, 10, 10, 20),
        (10, 20, 20, 30),
        (20, 30, 30, 37),
    ]
    assert folds[-1].train_end == folds[-1].test_start


def test_run_smoke_with_precomputed_states(tmp_path: Path):
    root = tmp_path / "stress"
    hmm = tmp_path / "hmm"
    out = tmp_path / "out"
    root.mkdir()
    hmm.mkdir()
    synthetic_trades().to_parquet(root / "trade_level_results.parquet", index=False)
    synthetic_states().to_parquet(hmm / "hmm_state_assignments.parquet", index=False)
    manifest = veto.run(
        veto.build_parser().parse_args(
            [
                "--stress-artifact-root",
                str(root),
                "--hmm-artifact-root",
                str(hmm),
                "--compact-root",
                str(tmp_path / "compact"),
                "--output-dir",
                str(out),
                "--hmm-models",
                "hmm",
                "--pmax-thresholds",
                "0.6,0.9",
                "--min-vetoed-trades",
                "1",
                "--min-remaining-trades",
                "1",
                "--min-vetoed-unique-markets",
                "1",
            ]
        )
    )
    assert manifest["hmm_coverage_ok"] is True
    assert (out / "single_state_veto_scan.csv").exists()
    assert (out / "frozen_veto_validation.csv").exists()
    assert (out / "output_schema.json").exists()
