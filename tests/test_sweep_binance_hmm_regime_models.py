import json
import sys

import numpy as np
import pandas as pd
import pytest

from scripts import sweep_binance_hmm_regime_models as sweep


def test_k_values_parsing():
    assert sweep.parse_k_values("2,3,4") == [2, 3, 4]
    with pytest.raises(Exception):
        sweep.parse_k_values("1,2")


def test_seeds_parsing():
    assert sweep.parse_seeds("1, 2,5") == [1, 2, 5]


def test_covariance_variant_parsing():
    assert sweep.parse_covariance_types("diag,spherical") == ["diag", "spherical"]
    assert sweep.parse_covariance_types("full") == ["full"]
    with pytest.raises(Exception):
        sweep.parse_covariance_types("diag,tied")


def test_sweep_summary_row_generation():
    row = sweep.summary_row_from_diagnostics(
        {
            "k": 2,
            "covariance_type": "diag",
            "selected_seed": 1,
            "converged": True,
            "n_iter": 10,
            "train_log_likelihood": -100.0,
            "aic": 240.0,
            "bic": 260.0,
            "min_state_occupancy": 0.1,
            "max_state_occupancy": 0.9,
            "state_occupancy_overall": {"0": 0.1, "1": 0.9},
            "duplicate_state_pairs": [],
            "mean_next_abs_move_spread_across_states": 0.2,
            "median_next_abs_move_spread_across_states": 0.1,
            "max_train_test_occupancy_shift": 0.05,
            "likely_quiet_state": 1,
            "tiny_move_rate_by_state": {"0": 0.2, "1": 0.8},
            "current_abs_move_by_state": {"1": {"mean": 0.01}},
            "run_length_diagnostics": {"1": {"run_length_mean": 3.0, "run_length_p90": 5.0, "p_run_length_ge_5": 0.25}},
            "post_confirmation_diagnostics": {"1": {"post_confirmation_tiny_rate": 0.9, "p_remaining_after_first_ge_4": 0.2}},
            "warnings": ["inspect"],
        }
    )
    assert row["k"] == 2
    assert row["low_occupancy_state_count_lt_5pct"] == 0
    assert row["likely_quiet_state"] == 1
    assert row["quiet_state_run_length_p90"] == 5.0
    assert row["quiet_state_p_remaining_after_first_ge_4"] == 0.2
    assert row["warnings"] == "inspect"


def test_low_occupancy_warnings():
    warnings = sweep.build_model_warnings(
        state_occupancy={"0": 0.01, "1": 0.99},
        occupancy_split={"train": {"0": 0.01, "1": 0.99}, "test": {"0": 0.0, "1": 1.0}},
        duplicate_pairs=[],
        k=2,
    )
    assert any("below 2%" in warning for warning in warnings)
    assert any("below 5%" in warning for warning in warnings)


def test_duplicate_state_warning_on_synthetic_diagnostics():
    pairs = sweep.duplicate_state_pairs(
        pd.DataFrame(
            [[1.0, 1.0, 0.0], [1.01, 0.99, 0.0], [-1.0, -1.0, 0.0]],
            index=[0, 1, 2],
        )
    )
    warnings = sweep.build_model_warnings(
        state_occupancy={"0": 0.3, "1": 0.3, "2": 0.4},
        occupancy_split={"train": {"0": 0.3, "1": 0.3, "2": 0.4}, "test": {"0": 0.3, "1": 0.3, "2": 0.4}},
        duplicate_pairs=pairs,
        k=3,
    )
    assert pairs
    assert any("duplicate-like" in warning for warning in warnings)


def test_train_test_occupancy_shift_warning():
    occupancy_split = {"train": {"0": 0.8, "1": 0.2}, "test": {"0": 0.4, "1": 0.6}}
    assert sweep.train_test_occupancy_shift(occupancy_split, 2) == pytest.approx(0.4)
    warnings = sweep.build_model_warnings(
        state_occupancy={"0": 0.6, "1": 0.4},
        occupancy_split=occupancy_split,
        duplicate_pairs=[],
        k=2,
    )
    assert any("heavy train/test" in warning for warning in warnings)


def test_run_length_diagnostics():
    summary = sweep.state_run_length_summary(pd.Series([0, 0, 1, 1, 1, 0]))
    assert summary["0"]["max"] == 2
    assert summary["1"]["median"] == 3.0
    assert summary["1"]["run_count"] == 1
    assert summary["1"]["run_length_p75"] == 3.0
    assert summary["1"]["p_run_length_ge_3"] == 1.0
    assert summary["0"]["p_run_length_ge_5"] == 0.0


def test_post_confirmation_diagnostics():
    frame = pd.DataFrame(
        {
            "assigned_state": [0, 0, 0, 1, 1, 0],
            "current_abs_move": [0.1, 0.2, 0.3, 1.0, 1.2, 0.4],
            "tiny_move_near_boundary": [1.0, 1.0, 0.0, 0.0, 1.0, 1.0],
            "binance_label": ["UP", "UP", "DOWN", "DOWN", "UP", "UP"],
        }
    )
    diagnostics = sweep.post_confirmation_diagnostics(frame)
    assert diagnostics["0"]["post_confirmation_rows"] == 2
    assert diagnostics["0"]["remaining_after_first_mean"] == pytest.approx(1.0)
    assert diagnostics["0"]["p_remaining_after_first_ge_2"] == pytest.approx(0.5)
    assert diagnostics["0"]["post_confirmation_abs_move_mean"] == pytest.approx(0.25)
    assert diagnostics["0"]["post_confirmation_tiny_rate"] == pytest.approx(0.5)
    assert diagnostics["1"]["p_regime_exit_within_next_1_markets_after_confirmation"] == 1.0


def test_quiet_state_identification():
    state, reason = sweep.identify_likely_quiet_state(
        current_abs_move={"0": {"mean": 0.5}, "1": {"mean": 0.1}, "2": {"mean": 0.2}},
        tiny_move_rate={"0": 0.2, "1": 0.9, "2": 0.7},
        feature_means_by_state={
            0: {"realized_vol_30m": 0.5, "price_transition_entropy_30m": 0.1},
            1: {"realized_vol_30m": 0.1, "price_transition_entropy_30m": 0.9},
            2: {"realized_vol_30m": 0.2, "price_transition_entropy_30m": 0.8},
        },
    )
    assert state == 1
    assert "lowest current_abs_move" in reason


def test_next_outcome_diagnostics():
    features = pd.DataFrame(
        {
            "event_start_time": pd.date_range("2020-01-01", periods=3, freq="5min", tz="UTC"),
            "abs_binance_move": [0.1, 0.2, 0.3],
            "binance_label": ["UP", "DOWN", "UP"],
            "binance_move": [0.1, -0.2, 0.3],
            "tiny_move_near_boundary": [False, True, False],
            "forward_abs_return_10m": [0.4, 0.5, np.nan],
        }
    )
    enriched = sweep.enrich_outcome_columns(features)
    assert enriched["next_abs_move_5m"].tolist()[:2] == [0.2, 0.3]
    assert enriched["next_binance_label"].tolist()[:2] == ["DOWN", "UP"]
    assert enriched["next_abs_move_10m"].tolist()[:2] == [0.4, 0.5]
    assert enriched["next_tiny_move_near_boundary"].dropna().tolist() == [1.0, 0.0]
    assert pd.api.types.is_float_dtype(enriched["next_tiny_move_near_boundary"])


def test_hmmlearn_unavailable_behavior(monkeypatch):
    monkeypatch.setitem(sys.modules, "hmmlearn", None)
    monkeypatch.setitem(sys.modules, "hmmlearn.hmm", None)
    features = pd.DataFrame({"split": ["train", "train"], "r_5m": [0.1, 0.2]})
    results, warnings = sweep.fit_hmm_sweep(
        features,
        k_values=[2],
        covariance_types=["diag"],
        seeds=[1],
        feature_columns=["r_5m"],
    )
    assert results["hmmlearn_available"] is False
    assert "hmmlearn unavailable" in warnings[0]


def _date_filter_frame():
    return pd.DataFrame(
        {
            "event_start_utc": pd.to_datetime(
                [
                    "2026-04-09T23:55:00Z",
                    "2026-04-10T00:00:00Z",
                    "2026-04-11T00:00:00Z",
                    "2026-04-12T00:00:00Z",
                    "2026-04-13T00:00:00Z",
                ],
                utc=True,
            ),
            "binance_label": ["UP", "DOWN", "UP", "UP", "DOWN"],
        }
    )


def test_date_filter_start_date_only():
    selected, metadata, warnings = sweep.filter_events_for_sweep(
        _date_filter_frame(),
        start_date="2026-04-10",
        end_date=None,
        tail_events=None,
    )
    assert len(selected) == 4
    assert selected["event_start_time"].min() == pd.Timestamp("2026-04-10T00:00:00Z")
    assert metadata["event_timestamp_column"] == "event_start_utc"
    assert metadata["input_event_rows_before_filtering"] == 5
    assert metadata["event_rows_after_date_filtering"] == 4
    assert metadata["label_counts_after_filtering"] == {"UP": 2, "DOWN": 2}
    assert warnings
    assert "very small" in warnings[0]


def test_date_filter_end_date_only_is_exclusive_midnight():
    selected, metadata, _ = sweep.filter_events_for_sweep(
        _date_filter_frame(),
        start_date=None,
        end_date="2026-04-12",
        tail_events=None,
    )
    assert selected["event_start_time"].tolist() == [
        pd.Timestamp("2026-04-09T23:55:00Z"),
        pd.Timestamp("2026-04-10T00:00:00Z"),
        pd.Timestamp("2026-04-11T00:00:00Z"),
    ]
    assert metadata["end_timestamp_utc_exclusive"] == "2026-04-12T00:00:00+00:00"


def test_date_filter_start_and_end_date():
    selected, metadata, _ = sweep.filter_events_for_sweep(
        _date_filter_frame(),
        start_date="2026-04-10",
        end_date="2026-04-13",
        tail_events=None,
    )
    assert selected["event_start_time"].tolist() == [
        pd.Timestamp("2026-04-10T00:00:00Z"),
        pd.Timestamp("2026-04-11T00:00:00Z"),
        pd.Timestamp("2026-04-12T00:00:00Z"),
    ]
    assert metadata["filtered_min_timestamp"] == "2026-04-10T00:00:00+00:00"
    assert metadata["filtered_max_timestamp"] == "2026-04-12T00:00:00+00:00"


def test_date_filter_applies_before_tail_events():
    selected, metadata, _ = sweep.filter_events_for_sweep(
        _date_filter_frame(),
        start_date="2026-04-10",
        end_date="2026-04-14",
        tail_events=2,
    )
    assert selected["event_start_time"].tolist() == [
        pd.Timestamp("2026-04-12T00:00:00Z"),
        pd.Timestamp("2026-04-13T00:00:00Z"),
    ]
    assert metadata["event_rows_after_date_filtering"] == 4
    assert metadata["selected_event_rows_after_tail"] == 2


def test_missing_timestamp_column_fails_clearly():
    with pytest.raises(ValueError, match="No supported event timestamp column found"):
        sweep.filter_events_for_sweep(
            pd.DataFrame({"not_a_timestamp": ["2026-04-10"]}),
            start_date="2026-04-10",
            end_date=None,
            tail_events=None,
        )


def test_zero_row_date_filter_fails_clearly():
    with pytest.raises(RuntimeError, match="selected zero event rows"):
        sweep.filter_events_for_sweep(
            _date_filter_frame(),
            start_date="2027-01-01",
            end_date=None,
            tail_events=None,
        )


def _synthetic_feature_frame(rows=12):
    times = pd.date_range("2020-01-01", periods=rows, freq="5min", tz="UTC")
    frame = pd.DataFrame(
        {
            "event_id": [f"e{i}" for i in range(rows)],
            "event_start_time": times,
            "event_end_time": times + pd.Timedelta(minutes=5),
            "split": ["train"] * 8 + ["validation"] * 2 + ["test"] * 2,
            "binance_label": ["UP", "DOWN"] * (rows // 2),
            "binance_move": np.linspace(-1.0, 1.0, rows),
            "abs_binance_move": np.linspace(0.1, 1.2, rows),
            "tiny_move_near_boundary": [False, True] * (rows // 2),
            "forward_abs_return_10m": np.linspace(0.2, 1.3, rows),
        }
    )
    for i, column in enumerate(sweep.FULL_FEATURE_COLUMNS):
        frame[column] = np.linspace(-1.0, 1.0, rows) + i * 0.01
    return frame


def test_output_files_written(tmp_path, monkeypatch):
    features = _synthetic_feature_frame()
    monkeypatch.setattr(sweep, "load_event_set", lambda path: features)
    monkeypatch.setattr(sweep, "load_binance_1m_klines", lambda roots: type("Loaded", (), {"frame": pd.DataFrame()})())
    monkeypatch.setattr(sweep, "build_feature_matrix", lambda events, prices, shock_age_cap_minutes, entropy_mode: (features, {}, []))

    class _FakeModel:
        transmat_ = np.array([[0.8, 0.2], [0.1, 0.9]])

    def _fake_fit(standardized, k_values, covariance_types, seeds, feature_columns):
        assignments = np.array([0, 0, 1, 1, 0, 0, 1, 1, 0, 1, 0, 1])
        return (
            {
                "hmmlearn_available": True,
                "x_train_rows": 8,
                "seed_fits": {"diag_k2": {"selected_seed": 2, "fits": []}},
                "models": {
                    "diag_k2": {
                        "k": 2,
                        "covariance_type": "diag",
                        "seed": 2,
                        "converged": True,
                        "n_iter": 5,
                        "train_log_likelihood": -10.0,
                        "model": _FakeModel(),
                        "assignments": assignments,
                        "posterior_max": np.ones(len(assignments)) * 0.9,
                    }
                },
            },
            [],
        )

    monkeypatch.setattr(sweep, "fit_hmm_sweep", _fake_fit)
    diagnostics = sweep.run_sweep(
        event_table_path=tmp_path / "events.csv",
        input_roots=[tmp_path],
        output_dir=tmp_path / "out",
        tail_events=12,
        k_values=[2],
        covariance_types=["diag"],
        seeds=[1, 2],
        feature_set="reduced",
        entropy_mode="off",
    )
    assert diagnostics["hmmlearn_available"] is True
    assert (tmp_path / "out" / "sweep_summary.csv").exists()
    assert (tmp_path / "out" / "sweep_diagnostics.json").exists()
    assert (tmp_path / "out" / "hmm_features_raw.csv").exists()
    assert (tmp_path / "out" / "hmm_features_standardized.csv").exists()
    assert (tmp_path / "out" / "best_model_assignments_k2_diag.csv").exists()
    assert (tmp_path / "out" / "model_diag_k2_diag.json").exists()
    assert (tmp_path / "out" / "hmm_sweep_readme_summary.txt").exists()
    payload = json.loads((tmp_path / "out" / "sweep_diagnostics.json").read_text(encoding="utf-8"))
    assert payload["seed_fit_diagnostics"]["diag_k2"]["selected_seed"] == 2
    assert payload["output_paths"]["hmm_features_raw"].endswith("hmm_features_raw.csv")
    model_payload = json.loads((tmp_path / "out" / "model_diag_k2_diag.json").read_text(encoding="utf-8"))
    assert model_payload["selected_hmm_feature_columns"] == sweep.REDUCED_HMM_FEATURE_COLUMNS
    assert "run_length_diagnostics" in model_payload
    assert "post_confirmation_diagnostics" in model_payload
    assert model_payload["likely_quiet_state"] is not None
    assignment = pd.read_csv(tmp_path / "out" / "best_model_assignments_k2_diag.csv")
    assert pd.api.types.is_float_dtype(assignment["next_tiny_move_near_boundary"])
    summary = pd.read_csv(tmp_path / "out" / "sweep_summary.csv")
    assert "likely_quiet_state" in summary.columns
    assert "quiet_state_run_length_p90" in summary.columns
    assert "quiet_state_post_confirmation_tiny_rate" in summary.columns
