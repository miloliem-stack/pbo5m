from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts import check_btc5m_canary_parity as parity


def _row(**overrides):
    base = {
        "row_id": "r1",
        "policy_name": "state3_ask_0.30_0.47",
        "market_id": "m1",
        "condition_id": "c1",
        "market_start_ts": "2026-05-22T10:00:00+00:00",
        "decision_ts": "2026-05-22T10:01:00+00:00",
        "market_age_sec": 60.0,
        "model_id": "brownian_zero_drift__rv30",
        "model_p_yes": 0.45,
        "model_p_no": 0.55,
        "hmm_model_id": "laplace_1m__gaussian_hmm__k4",
        "hmm_state": 3,
        "hmm_pmax": 0.82,
        "yes_ask": 0.40,
        "no_ask": 0.70,
        "selected_side": "YES",
        "selected_ask": 0.40,
        "selected_edge": 0.05,
        "final_decision": "BUY_YES",
        "abstain_reason": None,
        "edge_threshold": 0.02,
        "valid_topbook": True,
        "quote_age_ms": 100.0,
    }
    base.update(overrides)
    return base


def _write_csv(path: Path, rows: list[dict]):
    pd.DataFrame(rows).to_csv(path, index=False)


def test_exact_parity_passes(tmp_path: Path):
    replay = tmp_path / "replay.csv"
    output = tmp_path / "out"
    _write_csv(replay, [_row()])

    rc = parity.main(["--replay-path", str(replay), "--output-dir", str(output), "--sample-size", "0"])

    assert rc == 0
    summary = json.loads((output / "parity_summary.json").read_text(encoding="utf-8"))
    assert summary["passed"] is True
    assert summary["mismatch_count"] == 0


def test_changed_hmm_state_fails(tmp_path: Path):
    replay = tmp_path / "replay.csv"
    rebuilt = tmp_path / "rebuilt.csv"
    output = tmp_path / "out"
    _write_csv(replay, [_row()])
    _write_csv(rebuilt, [_row(hmm_state=1)])

    rc = parity.main(["--replay-path", str(replay), "--rebuilt-input-path", str(rebuilt), "--output-dir", str(output)])

    assert rc == 1
    diagnostics = pd.read_csv(output / "parity_diagnostics.csv")
    assert "hmm_state" in set(diagnostics["field_name"])
    assert "final_decision" in set(diagnostics["field_name"])


def test_changed_brownian_probability_fails(tmp_path: Path):
    replay = tmp_path / "replay.csv"
    rebuilt = tmp_path / "rebuilt.csv"
    output = tmp_path / "out"
    _write_csv(replay, [_row()])
    _write_csv(rebuilt, [_row(model_p_yes=0.451, model_p_no=0.549)])

    rc = parity.main(
        [
            "--replay-path",
            str(replay),
            "--rebuilt-input-path",
            str(rebuilt),
            "--output-dir",
            str(output),
            "--prob-tol",
            "1e-9",
        ]
    )

    assert rc == 1
    diagnostics = pd.read_csv(output / "parity_diagnostics.csv")
    assert "model_p_yes" in set(diagnostics["field_name"])


def test_changed_market_age_boundary_fails(tmp_path: Path):
    replay = tmp_path / "replay.csv"
    rebuilt = tmp_path / "rebuilt.csv"
    output = tmp_path / "out"
    _write_csv(replay, [_row()])
    _write_csv(rebuilt, [_row(market_age_sec=59.0)])

    rc = parity.main(["--replay-path", str(replay), "--rebuilt-input-path", str(rebuilt), "--output-dir", str(output)])

    assert rc == 1
    diagnostics = pd.read_csv(output / "parity_diagnostics.csv")
    assert "final_decision" in set(diagnostics["field_name"])
    assert "abstain_reason" in set(diagnostics["field_name"])


def test_changed_ask_crossing_boundary_fails(tmp_path: Path):
    replay = tmp_path / "replay.csv"
    rebuilt = tmp_path / "rebuilt.csv"
    output = tmp_path / "out"
    _write_csv(replay, [_row()])
    _write_csv(rebuilt, [_row(yes_ask=0.30, selected_ask=0.30, selected_edge=0.15)])

    rc = parity.main(["--replay-path", str(replay), "--rebuilt-input-path", str(rebuilt), "--output-dir", str(output)])

    assert rc == 1
    diagnostics = pd.read_csv(output / "parity_diagnostics.csv")
    assert "yes_ask" in set(diagnostics["field_name"])
    assert "final_decision" in set(diagnostics["field_name"])


def test_missing_artifact_fails_loudly(tmp_path: Path):
    rc = parity.main(["--replay-path", str(tmp_path / "missing.parquet"), "--output-dir", str(tmp_path / "out")])
    assert rc == 2


def test_replay_missing_full_quote_fields_skips_unavailable_optional_fields(tmp_path: Path):
    replay = pd.DataFrame(
        [
            _row(
                condition_id=None,
                yes_ask=None,
                no_ask=None,
                selected_ask=0.40,
                entry_ask=0.40,
            )
        ]
    )
    rebuilt = pd.DataFrame([_row(condition_id="real-condition", yes_ask=0.40, no_ask=0.70)])
    diagnostics = parity.compare_rows(
        parity.canonicalize_rows(replay),
        parity.canonicalize_rows(rebuilt),
        {"prob_tol": 1e-9, "age_tol_sec": 1.0, "ask_tol": 1e-9, "edge_tol": 1e-9},
    )
    assert diagnostics.empty


def test_replay_market_id_equal_to_market_key_without_condition_is_treated_as_alias():
    canonical = parity.canonicalize_rows(
        pd.DataFrame(
            [
                _row(
                    market_key=17,
                    market_id=17,
                    condition_id=None,
                )
            ]
        )
    )
    assert canonical.loc[0, "market_key"] == 17
    assert canonical.loc[0, "market_id"] is None
