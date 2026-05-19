import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

from scripts import research_hmm_regime_v1


def test_asof_observation_is_causal():
    df = pd.DataFrame(
        {
            "source_time": pd.to_datetime(
                [
                    "2026-04-18T19:19:59+00:00",
                    "2026-04-18T19:20:01+00:00",
                ],
                utc=True,
            ),
            "price": [100.0, 101.0],
        }
    )
    target = pd.Timestamp("2026-04-18T19:20:00+00:00")
    obs = research_hmm_regime_v1.asof_observation(df, "source_time", target)
    assert obs["price"] == 100.0
    assert obs["ts"] <= target


def test_exp_weighted_mean_computation():
    values = pd.Series([1.0, 3.0])
    ages = pd.Series([0.0, 10.0])
    result = research_hmm_regime_v1.exp_weighted_mean(values.to_numpy(), ages.to_numpy(), tau_sec=10.0)
    assert round(result, 6) == round((1.0 + 3.0 * 0.36787944117) / (1.0 + 0.36787944117), 6)


def test_transition_entropy_and_sign_flip_rate():
    returns = pd.Series([0.1, -0.2, 0.3, -0.4])
    entropy = research_hmm_regime_v1.transition_entropy(returns)
    sign_flips = research_hmm_regime_v1.sign_flip_rate(returns)
    assert 0.0 <= entropy <= 1.0
    assert sign_flips == 1.0


def test_assign_splits_is_chronological():
    df = pd.DataFrame(
        {
            "market_start_time": pd.to_datetime(
                [
                    "2026-04-18T19:20:00+00:00",
                    "2026-04-18T19:25:00+00:00",
                    "2026-04-18T19:30:00+00:00",
                    "2026-04-18T19:35:00+00:00",
                    "2026-04-18T19:40:00+00:00",
                ],
                utc=True,
            )
        }
    )
    split = research_hmm_regime_v1.assign_splits(df)
    assert split["split"].tolist() == ["train", "train", "train", "validation", "test"]


def test_standardize_features_uses_train_only():
    df = pd.DataFrame(
        {
            "market_start_time": pd.to_datetime(
                [
                    "2026-04-18T19:20:00+00:00",
                    "2026-04-18T19:25:00+00:00",
                    "2026-04-18T19:30:00+00:00",
                    "2026-04-18T19:35:00+00:00",
                    "2026-04-18T19:40:00+00:00",
                ],
                utc=True,
            ),
            "split": ["train", "train", "train", "validation", "test"],
            "r_15s": [1.0, 2.0, 3.0, 100.0, 200.0],
        }
    )
    standardized, params = research_hmm_regime_v1.standardize_features(df, ["r_15s"])
    assert params["r_15s"]["mean"] == 2.0
    assert round(params["r_15s"]["std"], 6) == round((2.0 / 3.0) ** 0.5, 6)
    assert standardized.loc[3, "r_15s"] > 100.0


def test_try_fit_hmms_gracefully_handles_missing_hmmlearn(monkeypatch):
    monkeypatch.setitem(sys.modules, "hmmlearn", None)
    monkeypatch.setitem(sys.modules, "hmmlearn.hmm", None)
    features = pd.DataFrame({"split": ["train", "train"], "r_15s": [0.1, 0.2]})
    results, warnings = research_hmm_regime_v1.try_fit_hmms(features, ks=[2], feature_columns=["r_15s"], seeds=[1, 2])
    assert results["hmmlearn_available"] is False
    assert "hmmlearn unavailable" in warnings[0]


def test_no_detected_shock_encodes_to_cap():
    event_row = pd.Series(
        {
            "market_id": "m1",
            "slug": "s1",
            "market_start_time": pd.Timestamp("2026-04-18T19:20:00+00:00"),
            "market_end_time": pd.Timestamp("2026-04-18T19:25:00+00:00"),
        }
    )
    times = pd.to_datetime(
        [
            "2026-04-18T19:15:00+00:00",
            "2026-04-18T19:16:00+00:00",
            "2026-04-18T19:17:00+00:00",
            "2026-04-18T19:18:00+00:00",
            "2026-04-18T19:19:00+00:00",
            "2026-04-18T19:20:00+00:00",
        ],
        utc=True,
    )
    df = pd.DataFrame(
        {
            "source_time": times,
            "price": [100.0, 100.00005, 100.0001, 100.00015, 100.0002, 100.00025],
        }
    )
    original = research_hmm_regime_v1.compute_shock_metrics
    try:
        research_hmm_regime_v1.compute_shock_metrics = lambda *args, **kwargs: (0.5, None)
        feature_row, reason = research_hmm_regime_v1._compute_feature_row_for_source(
            event_row,
            "chainlink",
            df,
            "source_time",
            event_row["market_start_time"],
            shock_age_cap_seconds=300.0,
        )
    finally:
        research_hmm_regime_v1.compute_shock_metrics = original
    assert reason == "ok"
    assert feature_row is not None
    assert feature_row["has_recent_shock"] == 0.0
    assert feature_row["shock_age_seconds"] is None
    assert feature_row["shock_age_seconds_capped"] == 300.0


def test_detected_shock_encodes_to_capped_age():
    event_row = pd.Series(
        {
            "market_id": "m1",
            "slug": "s1",
            "market_start_time": pd.Timestamp("2026-04-18T19:20:00+00:00"),
            "market_end_time": pd.Timestamp("2026-04-18T19:25:00+00:00"),
        }
    )
    times = pd.to_datetime(
        [
            "2026-04-18T19:15:00+00:00",
            "2026-04-18T19:16:00+00:00",
            "2026-04-18T19:17:00+00:00",
            "2026-04-18T19:18:00+00:00",
            "2026-04-18T19:19:00+00:00",
            "2026-04-18T19:20:00+00:00",
        ],
        utc=True,
    )
    df = pd.DataFrame(
        {
            "source_time": times,
            "price": [100.0, 100.0, 100.0, 100.0, 100.0, 105.0],
        }
    )
    original = research_hmm_regime_v1.compute_shock_metrics
    try:
        research_hmm_regime_v1.compute_shock_metrics = lambda *args, **kwargs: (9.0, 45.0)
        feature_row, reason = research_hmm_regime_v1._compute_feature_row_for_source(
            event_row,
            "chainlink",
            df,
            "source_time",
            event_row["market_start_time"],
            shock_age_cap_seconds=30.0,
        )
    finally:
        research_hmm_regime_v1.compute_shock_metrics = original
    assert reason == "ok"
    assert feature_row is not None
    assert feature_row["has_recent_shock"] == 1.0
    assert feature_row["shock_age_seconds"] == 45.0
    assert feature_row["shock_age_seconds_capped"] == 30.0


def test_prepare_hmm_matrix_removes_nonfinite_rows_and_reports_counts():
    df = pd.DataFrame(
        {
            "split": ["train", "validation", "test"],
            "r_15s": [0.1, np.nan, np.inf],
            "has_recent_shock": [1.0, 0.0, 0.0],
            "shock_age_seconds_capped": [10.0, 300.0, 300.0],
        }
    )
    prepared, nonfinite_counts, dropped = research_hmm_regime_v1.prepare_hmm_matrix(
        df,
        ["r_15s", "has_recent_shock", "shock_age_seconds_capped"],
    )
    assert len(prepared) == 1
    assert dropped == 2
    assert nonfinite_counts["r_15s"] == 2
    assert np.isfinite(prepared[["r_15s", "has_recent_shock", "shock_age_seconds_capped"]].to_numpy()).all()


def test_feature_row_contains_observation_counts_and_sparse_flags():
    event_row = pd.Series(
        {
            "market_id": "m1",
            "slug": "s1",
            "market_start_time": pd.Timestamp("2026-04-18T19:20:00+00:00"),
            "market_end_time": pd.Timestamp("2026-04-18T19:25:00+00:00"),
        }
    )
    times = pd.to_datetime(
        [
            "2026-04-18T19:15:10+00:00",
            "2026-04-18T19:16:10+00:00",
            "2026-04-18T19:17:10+00:00",
            "2026-04-18T19:18:10+00:00",
            "2026-04-18T19:19:10+00:00",
            "2026-04-18T19:19:30+00:00",
            "2026-04-18T19:19:50+00:00",
            "2026-04-18T19:20:00+00:00",
        ],
        utc=True,
    )
    df = pd.DataFrame({"source_time": times, "price": np.linspace(100.0, 101.0, len(times))})
    feature_row, reason = research_hmm_regime_v1._compute_feature_row_for_source(
        event_row,
        "chainlink",
        df,
        "source_time",
        event_row["market_start_time"],
        shock_age_cap_seconds=300.0,
    )
    assert reason == "ok"
    assert feature_row["obs_count_15s"] == 2
    assert feature_row["obs_count_60s"] == 4
    assert feature_row["obs_count_300s"] == 8
    assert feature_row["sparse_60s_window"] is True
    assert feature_row["sparse_120s_window"] is True
    assert feature_row["sparse_300s_window"] is True


def test_quality_filter_excludes_sparse_rows():
    df = pd.DataFrame(
        {
            "r_60s": [0.1, 0.2, 0.3],
            "r_120s": [0.1, 0.2, 0.3],
            "has_recent_shock": [0.0, 1.0, 1.0],
            "shock_age_seconds_capped": [300.0, 10.0, 20.0],
            "sparse_60s_window": [False, True, False],
            "sparse_120s_window": [False, False, True],
        }
    )
    filtered, reasons = research_hmm_regime_v1.apply_hmm_quality_filter(
        df,
        ["r_60s", "r_120s", "has_recent_shock", "shock_age_seconds_capped"],
    )
    assert len(filtered) == 1
    assert reasons["sparse_60s_window"] == 1
    assert reasons["sparse_120s_window"] == 1


def test_clip_standardized_features_reports_counts():
    df = pd.DataFrame({"r_60s": [-8.0, 0.0, 9.0], "r_120s": [1.0, -7.0, 7.5]})
    clipped, counts = research_hmm_regime_v1.clip_standardized_features(df, ["r_60s", "r_120s"], 6.0)
    assert counts == {"r_60s": 2, "r_120s": 2}
    assert clipped["r_60s"].tolist() == [-6.0, 0.0, 6.0]
    assert clipped["r_120s"].tolist() == [1.0, -6.0, 6.0]


def test_feature_set_selection_and_default_k_values():
    assert research_hmm_regime_v1.feature_columns_for_set("reduced") == research_hmm_regime_v1.REDUCED_HMM_FEATURE_COLUMNS
    assert research_hmm_regime_v1.feature_columns_for_set("full") == research_hmm_regime_v1.FULL_FEATURE_COLUMNS
    assert research_hmm_regime_v1.DEFAULT_HMM_STATE_COUNTS == [2, 3]
    original_argv = sys.argv[:]
    try:
        sys.argv = ["research_hmm_regime_v1.py"]
        args = research_hmm_regime_v1.parse_args()
    finally:
        sys.argv = original_argv
    assert args.feature_set == "reduced"
    assert args.ks == [2, 3]


def test_run_research_generates_diagnostics_files(tmp_path, monkeypatch):
    event_set = pd.DataFrame(
        {
            "market_id": ["m1", "m2", "m3", "m4", "m5"],
            "condition_id": [None] * 5,
            "slug": [f"s{i}" for i in range(5)],
            "market_start_time": pd.to_datetime(
                [
                    "2026-04-18T19:20:00+00:00",
                    "2026-04-18T19:25:00+00:00",
                    "2026-04-18T19:30:00+00:00",
                    "2026-04-18T19:35:00+00:00",
                    "2026-04-18T19:40:00+00:00",
                ],
                utc=True,
            ),
            "market_end_time": pd.to_datetime(
                [
                    "2026-04-18T19:25:00+00:00",
                    "2026-04-18T19:30:00+00:00",
                    "2026-04-18T19:35:00+00:00",
                    "2026-04-18T19:40:00+00:00",
                    "2026-04-18T19:45:00+00:00",
                ],
                utc=True,
            ),
            "chainlink_label": ["UP"] * 5,
            "binance_label": ["UP"] * 5,
            "label_agreement": [True] * 5,
            "tiny_move_near_boundary": [False] * 5,
            "wide_or_missing_quote": [True] * 5,
            "quote_abs_lag_sec": [1.0] * 5,
            "chainlink_move": [1.0] * 5,
            "binance_move": [1.0] * 5,
        }
    )
    event_path = tmp_path / "events.csv"
    event_set.to_csv(event_path, index=False)

    def _fake_load_price_data(_roots):
        return {
            "chainlink": pd.DataFrame(),
            "binance": pd.DataFrame(),
            "source_info": {"discovered_source_count": 1},
        }

    def _fake_build_feature_matrix(**kwargs):
        rows = []
        for i in range(5):
            row = {
                "market_id": f"m{i+1}",
                "condition_id": None,
                "slug": f"s{i}",
                "market_start_time": pd.Timestamp(f"2026-04-18T19:{20 + i * 5:02d}:00+00:00"),
                "market_end_time": pd.Timestamp(f"2026-04-18T19:{25 + i * 5:02d}:00+00:00"),
                "feature_price_source": "chainlink",
                "decision_timestamp": pd.Timestamp(f"2026-04-18T19:{20 + i * 5:02d}:00+00:00"),
                "max_feature_source_ts": pd.Timestamp(f"2026-04-18T19:{19 + i * 5:02d}:59+00:00"),
                "feature_source_lag_sec": 1.0,
                "chainlink_label": "UP",
                "binance_label": "UP",
                "label_agreement": True,
                "tiny_move_near_boundary": False,
                "wide_or_missing_quote": True,
                "quote_abs_lag_sec": 1.0,
                "chainlink_move": 1.0,
                "binance_move": 1.0,
                "shock_age_seconds": None if i == 0 else float(i * 10),
                "has_recent_shock": 0.0 if i == 0 else 1.0,
                "shock_age_seconds_capped": 300.0 if i == 0 else float(i * 10),
                "obs_count_15s": 2 + i,
                "obs_count_30s": 3 + i,
                "obs_count_60s": 4 + i,
                "obs_count_120s": 7 + i,
                "obs_count_180s": 9 + i,
                "obs_count_300s": 14 + i,
                "sparse_60s_window": i == 0,
                "sparse_120s_window": i == 0,
                "sparse_180s_window": False,
                "sparse_300s_window": False,
            }
            for j, column in enumerate(research_hmm_regime_v1.FEATURE_COLUMNS):
                if column in row:
                    continue
                row[column] = float(i + j + 1)
            rows.append(row)
        return research_hmm_regime_v1.assign_splits(pd.DataFrame(rows)), {}, []

    class _FakeModel:
        transmat_ = np.array([[0.8, 0.2], [0.1, 0.9]])

    monkeypatch.setattr(research_hmm_regime_v1, "load_price_data", _fake_load_price_data)
    monkeypatch.setattr(research_hmm_regime_v1, "build_feature_matrix", _fake_build_feature_matrix)
    monkeypatch.setattr(
        research_hmm_regime_v1,
        "try_fit_hmms",
        lambda standardized, ks, feature_columns, seeds: (
            {
                "hmmlearn_available": True,
                "candidate_fit_diagnostics": {
                    "2": {
                        "selected_seed": 2,
                        "fits": [
                            {
                                "seed": 1,
                                "converged": True,
                                "final_log_likelihood": 10.0,
                                "n_iter": 5,
                                "state_occupancy": {"0": 0.5, "1": 0.5},
                                "min_state_occupancy": 0.5,
                                "warnings": [],
                            },
                            {
                                "seed": 2,
                                "converged": True,
                                "final_log_likelihood": 11.0,
                                "n_iter": 6,
                                "state_occupancy": {"0": 0.25, "1": 0.75},
                                "min_state_occupancy": 0.25,
                                "warnings": [],
                            },
                        ],
                    }
                },
                "models": {
                    "2": {
                        "model": _FakeModel(),
                        "assignments": np.array([0, 1, 1, 0]),
                        "posterior_max": np.array([0.8, 0.7, 0.9, 0.6]),
                        "selected_seed": 2,
                        "converged": True,
                        "final_log_likelihood": 11.0,
                        "n_iter": 6,
                        "state_occupancy": {"0": 0.5, "1": 0.5},
                        "min_state_occupancy": 0.5,
                    }
                },
            },
            [],
        ),
    )

    diagnostics = research_hmm_regime_v1.run_research(
        event_set_path=event_path,
        input_roots=[tmp_path],
        output_dir=tmp_path / "out",
    )

    assert diagnostics["feature_rows_emitted"] > 0
    assert diagnostics["selected_feature_set"] == "reduced"
    assert diagnostics["has_recent_shock_added"] is True
    assert diagnostics["shock_age_cap_seconds"] == 300.0
    assert diagnostics["hmm_rows_dropped_for_nonfinite"] == 0
    assert diagnostics["hmm_rows_available_before_quality_filter"] == 5
    assert diagnostics["hmm_rows_excluded_by_quality_filter"] == 1
    assert diagnostics["hmm_rows_used_after_quality_filter"] == 4
    assert diagnostics["hmm_quality_exclusion_reasons"]["sparse_60s_window"] == 1
    assert diagnostics["hmm_quality_exclusion_reasons"]["sparse_120s_window"] == 1
    assert diagnostics["selected_hmm_feature_columns"] == research_hmm_regime_v1.REDUCED_HMM_FEATURE_COLUMNS
    assert diagnostics["hmm_feature_nan_counts_after_encoding"]["has_recent_shock"] == 0
    assert diagnostics["hmm_feature_nan_counts_after_encoding"]["shock_age_seconds_capped"] == 0
    assert diagnostics["candidate_k_values"] == [2, 3]
    assert diagnostics["candidate_fit_diagnostics"]["2"]["selected_seed"] == 2
    assert (tmp_path / "out" / "hmm_features_raw.csv").exists()
    assert (tmp_path / "out" / "hmm_features_standardized.csv").exists()
    assert (tmp_path / "out" / "hmm_state_assignments_k2.csv").exists()
    assignments = pd.read_csv(tmp_path / "out" / "hmm_state_assignments_k2.csv")
    assert len(assignments) == 4
    assert "obs_count_60s" in assignments.columns
    assert "sparse_60s_window" in assignments.columns
    payload = json.loads((tmp_path / "out" / "hmm_diagnostics.json").read_text(encoding="utf-8"))
    assert payload["hmmlearn_available"] is True
    assert payload["has_recent_shock_added"] is True
    assert payload["shock_age_cap_seconds"] == 300.0
