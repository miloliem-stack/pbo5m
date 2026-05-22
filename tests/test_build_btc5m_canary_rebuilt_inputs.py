from __future__ import annotations

import json
from argparse import Namespace
from pathlib import Path

import pandas as pd
import pytest

from scripts import build_btc5m_canary_rebuilt_inputs as builder


def _write_fixture(root: Path, *, hmm_state: int = 3, brownian_p: float = 0.45, yes_ask: float = 0.40) -> dict[str, Path]:
    compact = root / "compact"
    preds = root / "preds"
    compact.mkdir()
    preds.mkdir()
    replay = root / "replay.csv"
    hmm = root / "hmm.csv"
    pd.DataFrame(
        [
            {
                "row_id": "r1",
                "policy_name": "state3_ask_0.30_0.47",
                "market_key": 10,
                "market_id": "wrong-replay-market-id",
                "condition_id": "wrong-replay-condition",
                "market_start_ts": "2026-05-22T09:00:00+00:00",
                "entry_ts": "2026-05-22T10:01:00+00:00",
                "entry_age_seconds": 60.0,
                "model_id": "brownian_zero_drift__rv30",
                "p_yes": 0.99,
                "hmm_model_id": "laplace_1m__gaussian_hmm__k4",
                "hmm_state": 3,
                "hmm_pmax": 0.11,
                "entry_ask": 0.40,
                "side": "YES",
                "model_edge": 0.59,
                "edge_threshold": 0.02,
            }
        ]
    ).to_csv(replay, index=False)
    pd.DataFrame(
        [
            {
                "market_key": 10,
                "market_id": "m10",
                "condition_id": "c10",
                "market_start_ts": "2026-05-22T10:00:00+00:00",
                "market_end_ts": "2026-05-22T10:05:00+00:00",
                "yes_token_id": "yes",
                "no_token_id": "no",
            }
        ]
    ).astype({"market_key": "int32"}).to_parquet(compact / "market_windows.parquet", index=False)
    pd.DataFrame(
        [
            {"market_key": 10, "ts": "2026-05-22T10:00:30+00:00", "side": "YES", "ask_px_1": 0.31, "is_valid_topbook": True},
            {"market_key": 10, "ts": "2026-05-22T10:00:59+00:00", "side": "YES", "ask_px_1": yes_ask, "is_valid_topbook": True},
            {"market_key": 10, "ts": "2026-05-22T10:01:01+00:00", "side": "YES", "ask_px_1": 0.47, "is_valid_topbook": True},
            {"market_key": 10, "ts": "2026-05-22T10:00:59+00:00", "side": "NO", "ask_px_1": 0.70, "is_valid_topbook": True},
        ]
    ).astype({"market_key": "int32"}).to_parquet(compact / "book_ticks.parquet", index=False)
    pd.DataFrame(
        [
            {
                "timestamp": "2026-05-22T10:00:30+00:00",
                "market_window_start": "2026-05-22T10:00:00+00:00",
                "model_id": "brownian_zero_drift__rv30",
                "p_up": 0.41,
            },
            {
                "timestamp": "2026-05-22T10:00:59+00:00",
                "market_window_start": "2026-05-22T10:00:00+00:00",
                "model_id": "brownian_zero_drift__rv30",
                "p_up": brownian_p,
            },
            {
                "timestamp": "2026-05-22T10:01:01+00:00",
                "market_window_start": "2026-05-22T10:00:00+00:00",
                "model_id": "brownian_zero_drift__rv30",
                "p_up": 0.99,
            },
        ]
    ).to_parquet(preds / "probability_predictions_sample.parquet", index=False)
    pd.DataFrame(
        [
            {
                "timestamp": "2026-05-22T10:00:30+00:00",
                "hmm_model_id": "laplace_1m__gaussian_hmm__k4",
                "hmm_state": 1,
                "hmm_pmax": 0.51,
            },
            {
                "timestamp": "2026-05-22T10:00:59+00:00",
                "hmm_model_id": "laplace_1m__gaussian_hmm__k4",
                "hmm_state": hmm_state,
                "hmm_pmax": 0.82,
            },
            {
                "timestamp": "2026-05-22T10:01:01+00:00",
                "hmm_model_id": "laplace_1m__gaussian_hmm__k4",
                "hmm_state": 2,
                "hmm_pmax": 0.99,
            },
        ]
    ).to_csv(hmm, index=False)
    return {"compact": compact, "preds": preds, "replay": replay, "hmm": hmm}


def _args(paths: dict[str, Path], output: Path) -> Namespace:
    return Namespace(
        replay_path=paths["replay"],
        compact_root=paths["compact"],
        predictions_root=paths["preds"],
        hmm_state_path=paths["hmm"],
        output_dir=output,
        policy_names="state3_ask_0.30_0.47",
        sample_size=0,
        seed=1,
        overwrite=True,
    )


def test_rebuilt_input_schema_matches_parity_expectations(tmp_path: Path):
    paths = _write_fixture(tmp_path)
    rebuilt, manifest = builder.build_rebuilt_inputs(_args(paths, tmp_path / "out"))
    required = {
        "row_id",
        "market_id",
        "condition_id",
        "market_start_ts",
        "decision_ts",
        "market_age_sec",
        "model_id",
        "model_p_yes",
        "model_p_no",
        "hmm_model_id",
        "hmm_state",
        "hmm_pmax",
        "yes_ask",
        "no_ask",
        "quote_ts",
        "quote_age_ms",
        "selected_side",
        "selected_ask",
        "selected_edge",
        "final_decision",
        "abstain_reason",
    }
    assert required.issubset(rebuilt.columns)
    assert manifest["rebuilt_rows"] == 1


def test_hmm_probability_ask_and_age_are_not_copied_from_replay(tmp_path: Path):
    paths = _write_fixture(tmp_path)
    rebuilt, _ = builder.build_rebuilt_inputs(_args(paths, tmp_path / "out"))
    row = rebuilt.iloc[0]
    assert row["market_id"] == "m10"
    assert row["condition_id"] == "c10"
    assert row["market_age_sec"] == pytest.approx(60.0)
    assert row["hmm_state"] == 3
    assert row["hmm_pmax"] == pytest.approx(0.82)
    assert row["model_p_yes"] == pytest.approx(0.45)
    assert row["model_p_no"] == pytest.approx(0.55)
    assert row["yes_ask"] == pytest.approx(0.40)
    assert row["selected_edge"] == pytest.approx(0.05)
    assert row["final_decision"] == "BUY_YES"


def test_missing_hmm_artifact_fails(tmp_path: Path):
    paths = _write_fixture(tmp_path)
    paths["hmm"] = tmp_path / "missing_hmm.csv"
    with pytest.raises(FileNotFoundError, match="missing HMM artifact"):
        builder.build_rebuilt_inputs(_args(paths, tmp_path / "out"))


def test_missing_brownian_predictions_fail(tmp_path: Path):
    paths = _write_fixture(tmp_path)
    pd.DataFrame(
        [{"timestamp": "2026-05-22T10:00:59+00:00", "market_window_start": "2026-05-22T10:00:00+00:00", "model_id": "baseline_50", "p_up": 0.5}]
    ).to_parquet(paths["preds"] / "probability_predictions_sample.parquet", index=False)
    with pytest.raises(FileNotFoundError, match="missing brownian probability artifact"):
        builder.build_rebuilt_inputs(_args(paths, tmp_path / "out"))


def test_market_age_reconstruction_is_deterministic(tmp_path: Path):
    paths = _write_fixture(tmp_path)
    first, _ = builder.build_rebuilt_inputs(_args(paths, tmp_path / "out1"))
    second, _ = builder.build_rebuilt_inputs(_args(paths, tmp_path / "out2"))
    assert first["market_age_sec"].tolist() == second["market_age_sec"].tolist() == [60.0]


def test_asof_joins_are_previous_only_never_future_looking(tmp_path: Path):
    paths = _write_fixture(tmp_path, hmm_state=3, brownian_p=0.45, yes_ask=0.40)
    rebuilt, _ = builder.build_rebuilt_inputs(_args(paths, tmp_path / "out"))
    row = rebuilt.iloc[0]
    assert row["hmm_state"] == 3
    assert row["hmm_pmax"] == pytest.approx(0.82)
    assert row["model_p_yes"] == pytest.approx(0.45)
    assert row["yes_ask"] == pytest.approx(0.40)


def test_market_key_dtype_mismatch_is_normalized(tmp_path: Path):
    paths = _write_fixture(tmp_path)
    replay = pd.read_csv(paths["replay"])
    assert str(replay["market_key"].dtype) == "int64"
    windows = pd.read_parquet(paths["compact"] / "market_windows.parquet")
    ticks = pd.read_parquet(paths["compact"] / "book_ticks.parquet")
    assert str(windows["market_key"].dtype) == "int32"
    assert str(ticks["market_key"].dtype) == "int32"
    rebuilt, _ = builder.build_rebuilt_inputs(_args(paths, tmp_path / "out"))
    assert rebuilt.iloc[0]["market_key"] == 10


def test_grouped_asof_handles_interleaved_right_timestamps():
    left = pd.DataFrame(
        [
            {"row_id": "a", "market_key": 1, "decision_ts": pd.Timestamp("2026-01-01T00:02:00Z")},
            {"row_id": "b", "market_key": 2, "decision_ts": pd.Timestamp("2026-01-01T00:02:00Z")},
        ]
    )
    right = pd.DataFrame(
        [
            {"market_key": 1, "ts": pd.Timestamp("2026-01-01T00:01:00Z"), "ask_px_1": 0.40},
            {"market_key": 2, "ts": pd.Timestamp("2026-01-01T00:00:30Z"), "ask_px_1": 0.41},
            {"market_key": 1, "ts": pd.Timestamp("2026-01-01T00:01:30Z"), "ask_px_1": 0.42},
            {"market_key": 2, "ts": pd.Timestamp("2026-01-01T00:01:15Z"), "ask_px_1": 0.43},
        ]
    )
    joined = builder.previous_asof_by_group(left, right, by="market_key", left_on="decision_ts", right_on="ts")
    by_row = joined.set_index("row_id")
    assert by_row.loc["a", "ask_px_1"] == pytest.approx(0.42)
    assert by_row.loc["b", "ask_px_1"] == pytest.approx(0.43)
