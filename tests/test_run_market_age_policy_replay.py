import pandas as pd
import pytest

from scripts import run_market_age_policy_replay as age


def candidate_frame() -> pd.DataFrame:
    rows = []
    for market, winner in [(1, "YES"), (2, "NO")]:
        for sec, ask, state in [(20.0, 0.40, 1), (90.0, 0.42, 3), (130.0, 0.44, 3)]:
            rows.append(
                {
                    "model_name": "brownian_zero_drift__rv30",
                    "market_key": market,
                    "market_start_ts": pd.Timestamp("2026-05-01T00:00:00Z") + pd.Timedelta(minutes=5 * market),
                    "market_end_ts": pd.Timestamp("2026-05-01T00:05:00Z") + pd.Timedelta(minutes=5 * market),
                    "ts": pd.Timestamp("2026-05-01T00:00:00Z") + pd.Timedelta(minutes=5 * market, seconds=sec),
                    "side": "YES",
                    "winner_side": winner,
                    "p_yes": 0.60,
                    "model_edge": 0.10,
                    "best_edge": 0.10,
                    "entry_age_sec": sec,
                    "chronological_slice": "early",
                    "entry_date": "2026-05-01",
                    "entry_ask": ask,
                    "ask_bin": "0.40_0.45",
                    "ask_px_1_yes": ask,
                    "ask_sz_1_yes": 100.0,
                    "ask_px_2_yes": ask + 0.01,
                    "ask_sz_2_yes": 100.0,
                    "ask_px_3_yes": ask + 0.02,
                    "ask_sz_3_yes": 100.0,
                    "ask_px_1_no": 1 - ask,
                    "ask_sz_1_no": 100.0,
                    "ask_px_2_no": 1 - ask + 0.01,
                    "ask_sz_2_no": 100.0,
                    "ask_px_3_no": 1 - ask + 0.02,
                    "ask_sz_3_no": 100.0,
                    f"{age.model_slug(age.FOCUS_HMM_MODEL)}_state": state,
                    f"{age.model_slug(age.FOCUS_HMM_MODEL)}_pmax": 0.95,
                    f"{age.model_slug(age.BAD_HMM_MODEL)}_state": 0,
                    f"{age.model_slug(age.BAD_HMM_MODEL)}_pmax": 0.80,
                }
            )
    return pd.DataFrame(rows)


def test_lower_bound_window_ignores_earlier_candidate():
    selected = age.select_first_entries(
        candidate_frame(),
        [{"policy_name": "base_all_models_original_like", "kind": "base", "pmax": None}],
        age.parse_windows("0:300,60:300"),
        [0.01],
    )
    first_0_300 = selected[selected["entry_age_window"].eq("0_300")].sort_values("market_key")
    first_60_300 = selected[selected["entry_age_window"].eq("60_300")].sort_values("market_key")
    assert first_0_300["entry_age_sec"].tolist() == [20.0, 20.0]
    assert first_60_300["entry_age_sec"].tolist() == [90.0, 90.0]


def test_exact_bins_are_non_cumulative_and_select_different_candidates():
    selected = age.select_first_entries(
        candidate_frame(),
        [{"policy_name": "base_all_models_original_like", "kind": "base", "pmax": None}],
        age.parse_windows("0:60,60:120"),
        [0.01],
    )
    assert selected[selected["entry_age_window"].eq("0_60")]["entry_age_sec"].unique().tolist() == [20.0]
    assert selected[selected["entry_age_window"].eq("60_120")]["entry_age_sec"].unique().tolist() == [90.0]


def test_first_entry_only_applies_after_window_filtering():
    selected = age.select_first_entries(
        candidate_frame(),
        [{"policy_name": "base_all_models_original_like", "kind": "base", "pmax": None}],
        age.parse_windows("60:300"),
        [0.01],
    )
    assert selected.groupby(["market_key", "entry_age_window"]).size().max() == 1
    assert selected["entry_age_sec"].unique().tolist() == [90.0]


def test_hmm_context_join_is_previous_only():
    candidates = candidate_frame().iloc[[0]].copy()
    candidates["ts"] = pd.Timestamp("2026-05-01T00:00:30Z")
    states = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(["2026-05-01T00:00:00Z", "2026-05-01T00:01:00Z"], utc=True),
            "hmm_model_id": [age.FOCUS_HMM_MODEL, age.FOCUS_HMM_MODEL],
            "hmm_state": [7, 9],
            "hmm_pmax": [0.7, 0.99],
        }
    )
    out = age.attach_hmm_context(candidates.drop(columns=[f"{age.model_slug(age.FOCUS_HMM_MODEL)}_state", f"{age.model_slug(age.FOCUS_HMM_MODEL)}_pmax"]), states, [age.FOCUS_HMM_MODEL])
    assert out[f"{age.model_slug(age.FOCUS_HMM_MODEL)}_state"].iloc[0] == 7
    assert out[f"{age.model_slug(age.FOCUS_HMM_MODEL)}_pmax"].iloc[0] == pytest.approx(0.7)


def test_policy_filter_applies_before_first_candidate_selection():
    selected = age.select_first_entries(
        candidate_frame(),
        [{"policy_name": "state3_ask_0.30_0.47", "kind": "state3_ask", "pmax": None}],
        age.parse_windows("0:300"),
        [0.01],
    )
    assert selected["entry_age_sec"].tolist() == [90.0, 90.0]
    assert selected["entry_age_sec"].min() >= 60.0


def test_roi_pnl_metrics_are_correct():
    selected = age.select_first_entries(
        candidate_frame().head(1),
        [{"policy_name": "base_all_models_original_like", "kind": "base", "pmax": None}],
        age.parse_windows("0:300"),
        [0.01],
    )
    trades = age.simulate_policy(selected, [1.0])
    summary = age.summarize(trades, ["policy_name", "entry_age_window"], min_trades=1, min_markets=1)
    row = summary.iloc[0]
    assert row["gross_cost"] == pytest.approx(1.0)
    assert row["pnl"] == pytest.approx(1.0 / 0.40 - 1.0)
    assert row["roi"] == pytest.approx((1.0 / 0.40 - 1.0) / 1.0)


def test_models_override_avoids_stress_manifest_models(tmp_path):
    manifest_root = tmp_path / "stress"
    manifest_root.mkdir()
    (manifest_root / "run_manifest.json").write_text('{"requested_models":["baseline_50"]}', encoding="utf-8")
    parsed = age.build_parser().parse_args(
        [
            "--output-dir",
            str(tmp_path / "out"),
            "--stress-artifact-root",
            str(manifest_root),
            "--models",
            "brownian_zero_drift__rv30",
        ]
    )
    models, _, _ = age.load_models_stakes_thresholds(parsed)
    assert models == ["brownian_zero_drift__rv30"]


def test_base_only_policy_spec_skips_hmm_policy_needs():
    selected = age.select_first_entries(
        candidate_frame(),
        [{"policy_name": "base_all_models_original_like", "kind": "base", "pmax": None}],
        age.parse_windows("60:240"),
        [0.02],
    )
    trades = age.simulate_policy(selected, [5.0])
    assert set(trades["policy_name"]) == {"base_all_models_original_like"}
    assert trades["hmm_model_id"].eq("").all()
