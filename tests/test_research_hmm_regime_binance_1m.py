import json
import sys

import numpy as np
import pandas as pd

from scripts import research_hmm_regime_binance_1m


def _synthetic_prices():
    times = pd.date_range("2020-01-01T00:00:00Z", periods=120, freq="1min", tz="UTC")
    return pd.DataFrame({"event_time": times, "close": np.linspace(100.0, 112.0, len(times))})


def _synthetic_events():
    starts = pd.date_range("2020-01-01T00:20:00Z", periods=20, freq="5min", tz="UTC")
    return pd.DataFrame(
        {
            "event_id": [f"e{i}" for i in range(len(starts))],
            "event_start_time": starts,
            "event_end_time": starts + pd.Timedelta(minutes=5),
            "binance_label": ["UP"] * len(starts),
            "binance_move": [1.0] * len(starts),
            "abs_binance_move": [1.0] * len(starts),
            "tiny_move_near_boundary": [False] * len(starts),
        }
    )


def test_filter_events_supports_dates_max_and_tail():
    events = _synthetic_events()
    filtered = research_hmm_regime_binance_1m.filter_events(
        events,
        start_date="2020-01-01",
        end_date="2020-01-01",
        max_events=3,
    )
    assert len(filtered) == 3
    tailed = research_hmm_regime_binance_1m.filter_events(events, tail_events=4)
    assert tailed["event_id"].tolist() == ["e16", "e17", "e18", "e19"]


def test_build_feature_matrix_is_causal():
    features, dropped, leakage = research_hmm_regime_binance_1m.build_feature_matrix(_synthetic_events(), _synthetic_prices())
    assert leakage == []
    assert not features.empty
    assert (features["max_feature_source_ts"] <= features["decision_timestamp"]).all()
    assert "missing_feature_rows_after_join" not in dropped


def test_vectorized_returns_and_realized_vol_match_expected():
    prices = _synthetic_prices()
    vectorized = research_hmm_regime_binance_1m._compute_vectorized_price_features(
        prices,
        entropy_mode="off",
        shock_age_cap_minutes=30.0,
    )
    idx = 20
    expected_r5 = np.log(prices.loc[idx, "close"] / prices.loc[idx - 5, "close"])
    assert round(vectorized.loc[idx, "r_5m"], 12) == round(expected_r5, 12)
    expected_vol5 = np.std(np.diff(np.log(prices.loc[idx - 5 : idx, "close"].to_numpy())), ddof=0)
    assert round(vectorized.loc[idx, "realized_vol_5m"], 12) == round(expected_vol5, 12)


def test_vectorized_ew_features_are_deterministic():
    prices = _synthetic_prices()
    vectorized = research_hmm_regime_binance_1m._compute_vectorized_price_features(
        prices,
        entropy_mode="off",
        shock_age_cap_minutes=30.0,
    )
    assert vectorized["ew_return_tau_5m"].iloc[40] == vectorized["ew_return_tau_5m"].iloc[40]
    assert pd.notna(vectorized["ew_abs_return_tau_15m"].iloc[40])


def test_entropy_mode_off_and_fast_behavior():
    prices = _synthetic_prices()
    off = research_hmm_regime_binance_1m._compute_vectorized_price_features(prices, entropy_mode="off", shock_age_cap_minutes=30.0)
    fast = research_hmm_regime_binance_1m._compute_vectorized_price_features(prices, entropy_mode="fast", shock_age_cap_minutes=30.0)
    assert (off["price_transition_entropy_15m"] == 0.0).all()
    assert fast["price_transition_entropy_15m"].notna().sum() > 0


def test_assign_splits_and_standardize_clip():
    features = pd.DataFrame(
        {
            "event_start_time": pd.date_range("2020-01-01T00:00:00Z", periods=5, freq="5min", tz="UTC"),
            "r_1m": [1.0, 2.0, 3.0, 50.0, 60.0],
        }
    )
    split = research_hmm_regime_binance_1m.assign_splits(features)
    standardized, params = research_hmm_regime_binance_1m.standardize_features(split, ["r_1m"])
    clipped, counts = research_hmm_regime_binance_1m.clip_standardized_features(standardized, ["r_1m"], 1.0)
    assert split["split"].tolist() == ["train", "train", "train", "validation", "test"]
    assert params["r_1m"]["mean"] == 2.0
    assert counts["r_1m"] >= 1
    assert clipped["r_1m"].abs().max() <= 1.0


def test_fit_tail_rows_select_latest_rows():
    features = pd.DataFrame(
        {
            "event_start_time": pd.date_range("2020-01-01T00:00:00Z", periods=10, freq="5min", tz="UTC"),
            "value": range(10),
        }
    )
    selected, diag = research_hmm_regime_binance_1m.select_hmm_fit_rows(features, fit_tail_rows=3, fit_max_rows=None)
    assert selected["value"].tolist() == [7, 8, 9]
    assert diag["fit_subset_mode"] == "tail"


def test_try_fit_hmms_gracefully_handles_missing_hmmlearn(monkeypatch):
    monkeypatch.setitem(sys.modules, "hmmlearn", None)
    monkeypatch.setitem(sys.modules, "hmmlearn.hmm", None)
    features = pd.DataFrame({"split": ["train", "train"], "r_1m": [0.1, 0.2]})
    results, warnings = research_hmm_regime_binance_1m.try_fit_hmms(features, ks=[2], feature_columns=["r_1m"], seeds=[1, 2])
    assert results["hmmlearn_available"] is False
    assert "hmmlearn unavailable" in warnings[0]


def test_run_research_generates_diagnostics_and_removes_stale_files(tmp_path, monkeypatch):
    events = _synthetic_events()
    event_path = tmp_path / "events.csv"
    events.to_csv(event_path, index=False)

    monkeypatch.setattr(
        research_hmm_regime_binance_1m,
        "load_binance_1m_klines",
        lambda roots: type("Loaded", (), {"frame": _synthetic_prices()})(),
    )

    class _FakeModel:
        transmat_ = np.array([[0.8, 0.2], [0.1, 0.9]])

    monkeypatch.setattr(
        research_hmm_regime_binance_1m,
        "try_fit_hmms",
        lambda standardized, ks, feature_columns, seeds: (
            {
                "hmmlearn_available": True,
                "candidate_fit_diagnostics": {
                    "2": {
                        "selected_seed": 2,
                        "fits": [
                            {
                                "seed": 2,
                                "converged": True,
                                "final_log_likelihood": 10.0,
                                "n_iter": 5,
                                "state_occupancy": {"0": 0.4, "1": 0.6},
                                "min_state_occupancy": 0.4,
                                "warnings": [],
                            }
                        ],
                    }
                },
                "models": {
                    "2": {
                        "seed": 2,
                        "converged": True,
                        "final_log_likelihood": 10.0,
                        "n_iter": 5,
                        "min_state_occupancy": 0.4,
                        "model": _FakeModel(),
                        "assignments": np.array([0] * (len(standardized) // 2) + [1] * (len(standardized) - len(standardized) // 2)),
                        "posterior_max": np.array([0.9] * len(standardized)),
                    }
                },
            },
            [],
        ),
    )

    output_dir = tmp_path / "out"
    output_dir.mkdir()
    (output_dir / "hmm_state_assignments_k99.csv").write_text("stale\n", encoding="utf-8")

    diagnostics = research_hmm_regime_binance_1m.run_research(
        event_table_path=event_path,
        input_roots=[tmp_path],
        output_dir=output_dir,
        tail_events=12,
        fit_tail_rows=12,
        entropy_mode="off",
    )
    assert diagnostics["selected_event_rows"] == 12
    assert diagnostics["hmm_fit_rows"] > 0
    assert diagnostics["entropy_mode"] == "off"
    assert diagnostics["feature_construction_seconds"] >= 0.0
    assert (output_dir / "hmm_features_raw.csv").exists()
    assert (output_dir / "hmm_state_assignments_k2.csv").exists()
    assert not (output_dir / "hmm_state_assignments_k99.csv").exists()
    payload = json.loads((output_dir / "hmm_diagnostics.json").read_text(encoding="utf-8"))
    assert payload["candidate_fit_diagnostics"]["2"]["selected_seed"] == 2
    assert payload["selected_event_rows"] == 12
