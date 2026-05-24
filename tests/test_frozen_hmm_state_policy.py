from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts import attach_frozen_hmm_states_to_replay as attach
from scripts import evaluate_frozen_hmm_state_policy as evaluate


def test_state_attachment_is_previous_only_asof_safe():
    replay = pd.DataFrame(
        {
            "entry_ts": pd.to_datetime(["2026-01-01T00:00:30Z", "2026-01-01T00:01:30Z"]),
            "market_id": ["m1", "m2"],
        }
    )
    states = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(["2026-01-01T00:00:00Z", "2026-01-01T00:01:00Z", "2026-01-01T00:02:00Z"]),
            "hmm_asof_ts": pd.to_datetime(["2026-01-01T00:00:00Z", "2026-01-01T00:01:00Z", "2026-01-01T00:02:00Z"]),
            "frozen_hmm_model_id": ["frozen"] * 3,
            "frozen_hmm_state": [1, 2, 9],
            "frozen_hmm_pmax": [0.7, 0.8, 0.99],
        }
    )
    out = attach.attach_states(replay, states)
    assert out["frozen_hmm_state"].tolist() == [1, 2]


def test_state_attachment_normalizes_datetime_units_for_asof():
    replay = pd.DataFrame(
        {
            "entry_ts": pd.Series(pd.to_datetime(["2026-01-01T00:00:30Z"])).dt.as_unit("ns"),
            "market_id": ["m1"],
        }
    )
    states = pd.DataFrame(
        {
            "timestamp": pd.Series(pd.to_datetime(["2026-01-01T00:00:00Z"])).dt.as_unit("us"),
            "hmm_asof_ts": pd.Series(pd.to_datetime(["2026-01-01T00:00:00Z"])).dt.as_unit("us"),
            "frozen_hmm_model_id": ["frozen"],
            "frozen_hmm_state": [1],
            "frozen_hmm_pmax": [0.7],
        }
    )
    out = attach.attach_states(replay, states)
    assert out["frozen_hmm_state"].tolist() == [1]


def _policy_rows() -> pd.DataFrame:
    rows = []
    for idx in range(20):
        rows.append(
            {
                "market_id": f"e{idx}",
                "model_id": "brownian_zero_drift__rv30",
                "chronological_slice": "early",
                "entry_age_seconds": 90,
                "ask_price": 0.40,
                "best_edge": 0.03,
                "gross_cost": 1.0,
                "pnl": 0.2 if idx < 12 else -0.1,
                "win": idx < 12,
                "frozen_hmm_model_id": "frozen",
                "frozen_hmm_state": 1,
                "frozen_hmm_pmax": 0.8,
                "entry_date": "2026-01-01",
                "side": "YES",
                "ask_bin": "0.40_0.45",
                "entry_age_window": "60_120",
            }
        )
    for idx in range(20):
        rows.append(
            {
                "market_id": f"h{idx}",
                "model_id": "brownian_zero_drift__rv30",
                "chronological_slice": "main",
                "entry_age_seconds": 90,
                "ask_price": 0.40,
                "best_edge": 0.03,
                "gross_cost": 1.0,
                "pnl": 0.15,
                "win": True,
                "frozen_hmm_model_id": "frozen",
                "frozen_hmm_state": 1,
                "frozen_hmm_pmax": 0.8,
                "entry_date": f"2026-01-{1 + idx % 5:02d}",
                "side": "YES",
                "ask_bin": "0.40_0.45",
                "entry_age_window": "60_120",
            }
        )
    for idx in range(10):
        rows.append(
            {
                "market_id": f"bad{idx}",
                "model_id": "brownian_zero_drift__rv30",
                "chronological_slice": "early",
                "entry_age_seconds": 90,
                "ask_price": 0.40,
                "best_edge": 0.03,
                "gross_cost": 1.0,
                "pnl": -0.5,
                "win": False,
                "frozen_hmm_model_id": "frozen",
                "frozen_hmm_state": 2,
                "frozen_hmm_pmax": 0.8,
                "entry_date": "2026-01-01",
                "side": "NO",
                "ask_bin": "0.40_0.45",
                "entry_age_window": "60_120",
            }
        )
    return pd.DataFrame(rows)


def test_state_selection_uses_train_slice_only(tmp_path: Path):
    path = tmp_path / "attached.parquet"
    _policy_rows().to_parquet(path, index=False)
    out = tmp_path / "eval"
    args = evaluate.build_parser().parse_args(
        [
            "--attached-path",
            str(path),
            "--output-dir",
            str(out),
            "--train-slices",
            "early",
            "--holdout-slices",
            "main",
            "--min-trades",
            "5",
            "--min-unique-markets",
            "5",
            "--force-deploy-policy",
        ]
    )
    evaluate.run(args)
    selected = json.loads((out / "selected_states.json").read_text(encoding="utf-8"))
    assert selected["selected_states"] == [1]
    assert selected["selection_basis"] == "train_slices_only"


def test_deploy_policy_only_written_when_forced_or_criteria_pass(tmp_path: Path):
    path = tmp_path / "attached.parquet"
    frame = _policy_rows()
    frame.loc[frame["chronological_slice"].eq("main"), "pnl"] = -0.1
    frame.to_parquet(path, index=False)
    out = tmp_path / "eval"
    args = evaluate.build_parser().parse_args(["--attached-path", str(path), "--output-dir", str(out), "--min-trades", "5", "--min-unique-markets", "5"])
    evaluate.run(args)
    assert not (out / "deploy_policy.json").exists()
    forced = tmp_path / "forced"
    args = evaluate.build_parser().parse_args(["--attached-path", str(path), "--output-dir", str(forced), "--min-trades", "5", "--min-unique-markets", "5", "--force-deploy-policy"])
    evaluate.run(args)
    assert (forced / "deploy_policy.json").exists()
