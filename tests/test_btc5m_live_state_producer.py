from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from scripts import preflight_btc5m_canary_server as preflight
from scripts import run_btc5m_live_state_producer as producer


NOW = datetime(2026, 5, 22, 10, 0, 30, tzinfo=timezone.utc)


def _router():
    return {
        "market": {
            "market_id": "m1",
            "condition_id": "c1",
            "start_time": (NOW - timedelta(seconds=30)).isoformat(),
            "end_time": (NOW + timedelta(seconds=270)).isoformat(),
            "token_yes": "yes",
            "token_no": "no",
        }
    }


def _price():
    return {"price": 100.0, "ts": NOW.isoformat()}


def _hmm_artifact_dir(tmp_path: Path):
    root = tmp_path / "hmm_bundle"
    root.mkdir()
    model = root / "model.bin"
    model.write_text("model", encoding="utf-8")
    (root / "manifest.json").write_text(
        json.dumps(
            {
                "model_artifact": "model.bin",
                "feature_config_hash": "feat123",
                "market_window_seconds": 300,
            }
        ),
        encoding="utf-8",
    )
    (root / "live_hmm_state_source.json").write_text(
        json.dumps(
            {
                "hmm_model_id": "laplace_1m__gaussian_hmm__k4",
                "hmm_state": 3,
                "hmm_pmax": 0.91,
                "asof_ts": NOW.isoformat(),
            }
        ),
        encoding="utf-8",
    )
    return root


def test_live_state_producer_writes_brownian_json_atomically(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("BTC5M_LIVE_RV30", "0.01")
    out = tmp_path / "brownian.json"
    state = producer.build_brownian_state(router_fn=_router, price_fn=_price, reference_cache_path=tmp_path / "cache.json", now=NOW)
    producer.atomic_write_json(out, state)
    payload = json.loads(out.read_text(encoding="utf-8"))
    assert payload["model_id"] == "brownian_zero_drift__rv30"
    assert payload["probability_convention"] == "replay-matched brownian_zero_drift__rv30"
    assert payload["model_p_no"] == pytest.approx(1.0 - payload["model_p_yes"])
    assert not list(tmp_path.glob(".*.tmp"))


def test_live_state_producer_writes_hmm_json_atomically(tmp_path: Path):
    artifact_dir = _hmm_artifact_dir(tmp_path)
    out = tmp_path / "hmm.json"
    state = producer.build_hmm_state(artifact_dir=artifact_dir, now=NOW)
    producer.atomic_write_json(out, state)
    payload = json.loads(out.read_text(encoding="utf-8"))
    assert payload["hmm_model_id"] == "laplace_1m__gaussian_hmm__k4"
    assert payload["hmm_state"] == 3
    assert payload["model_artifact_hash"]


def test_live_state_producer_brownian_only_skips_hmm_artifact(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("BTC5M_LIVE_RV30", "0.01")
    monkeypatch.setattr(
        producer,
        "build_brownian_state",
        lambda reference_cache_path: {
            "valid": True,
            "model_id": "brownian_zero_drift__rv30",
            "model_p_yes": 0.5,
            "model_p_no": 0.5,
            "probability_convention": "replay-matched brownian_zero_drift__rv30",
            "asof_ts": NOW.isoformat(),
            "generated_ts": NOW.isoformat(),
        },
    )
    args = type(
        "Args",
        (),
        {
            "reference_cache_path": tmp_path / "cache.json",
            "brownian_output_path": tmp_path / "brownian.json",
            "hmm_output_path": tmp_path / "hmm.json",
            "hmm_artifact_dir": None,
            "hmm_source_name": "live_hmm_state_source.json",
            "write_invalid_on_error": True,
            "brownian_only": True,
            "live": True,
        },
    )()
    result = producer.run_once(args)
    assert result["brownian"] == str(tmp_path / "brownian.json")
    assert result["hmm"] == "skipped_brownian_only"
    assert not (tmp_path / "hmm.json").exists()


def test_missing_hmm_artifact_fails_loudly(tmp_path: Path):
    with pytest.raises(RuntimeError, match="hmm_artifact_dir_missing"):
        producer.build_hmm_state(artifact_dir=tmp_path / "missing", now=NOW)


def test_preflight_fails_when_hmm_artifact_missing(monkeypatch, tmp_path: Path):
    monkeypatch.setenv("BTC5M_HMM_ARTIFACT_DIR", str(tmp_path / "missing"))
    monkeypatch.setenv("BTC5M_LIVE_HMM_STATE_PATH", str(tmp_path / "missing_hmm.json"))
    monkeypatch.setenv("BTC5M_LIVE_BROWNIAN_STATE_PATH", str(tmp_path / "missing_brownian.json"))
    rows = preflight.run_checks(live=True, route_fn=_router, quote_fn=lambda token: {"fetch_ok": True, "best_ask": 0.4}, price_fn=_price)
    failures = {row["check"]: row for row in rows if row["status"] == "FAIL"}
    assert "hmm_artifact_dir_present" in failures


def test_preflight_fails_when_live_output_dirs_not_writable(monkeypatch, tmp_path: Path):
    blocked = tmp_path / "not_a_dir"
    blocked.write_text("x", encoding="utf-8")
    monkeypatch.setenv("BTC5M_LIVE_STATE_DIR", str(blocked))
    rows = preflight.run_checks(live=True, route_fn=_router, quote_fn=lambda token: {"fetch_ok": True, "best_ask": 0.4}, price_fn=_price)
    failures = {row["check"]: row for row in rows if row["status"] == "FAIL"}
    assert "live_state_dir_writable" in failures
