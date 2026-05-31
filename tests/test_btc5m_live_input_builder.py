from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from src.runtime.btc5m_live_input_builder import BTC5MCanaryLiveInputBuilder, LiveInputBuilderConfig


NOW = datetime(2026, 5, 22, 10, 2, 0, tzinfo=timezone.utc)


def _market():
    start = NOW - timedelta(seconds=120)
    end = start + timedelta(minutes=5)
    return {
        "market": {
            "market_id": "m1",
            "condition_id": "c1",
            "token_yes": "yes-token",
            "token_no": "no-token",
            "start_time": start.isoformat(),
            "end_time": end.isoformat(),
        },
        "detection_source": "fixture",
    }


def _quote(token_id: str):
    return {
        "token_id": token_id,
        "fetch_ok": True,
        "best_ask": 0.40 if token_id == "yes-token" else 0.68,
        "ask_size": 100.0,
        "fetched_at": NOW.isoformat(),
        "age_seconds": 0.1,
    }


def _state_paths(tmp_path: Path):
    hmm_path = tmp_path / "hmm.json"
    brownian_path = tmp_path / "brownian.json"
    hmm_path.write_text(
        json.dumps(
            {
                "hmm_model_id": "laplace_1m__gaussian_hmm__k4",
                "hmm_state": 3,
                "hmm_pmax": 0.82,
                "asof_ts": NOW.isoformat(),
                "timestamp": NOW.isoformat(),
            }
        ),
        encoding="utf-8",
    )
    brownian_path.write_text(
        json.dumps(
            {
                "model_id": "brownian_zero_drift__rv30",
                "model_p_yes": 0.46,
                "probability_convention": "replay-matched brownian_zero_drift__rv30",
                "probability_replay_convention": "replay-matched brownian_zero_drift__rv30",
                "asof_ts": NOW.isoformat(),
            }
        ),
        encoding="utf-8",
    )
    return hmm_path, brownian_path


def _builder(tmp_path: Path, **overrides):
    hmm_path, brownian_path = _state_paths(tmp_path)
    config = LiveInputBuilderConfig(
        hmm_state_path=overrides.pop("hmm_state_path", hmm_path),
        brownian_state_path=overrides.pop("brownian_state_path", brownian_path),
        max_quote_age_ms=5000,
    )
    return BTC5MCanaryLiveInputBuilder(
        config,
        market_fn=overrides.pop("market_fn", _market),
        quote_fn=overrides.pop("quote_fn", _quote),
        binance_price_fn=overrides.pop("binance_price_fn", lambda: {"price": 100.0}),
        now_fn=overrides.pop("now_fn", lambda: NOW),
    )


def test_live_input_builder_schema(tmp_path: Path):
    built = _builder(tmp_path).build()
    assert built["ok"] is True
    payload = built["input"]
    assert payload["market"]["condition_id"] == "c1"
    assert payload["market"]["market_age_sec"] == 120.0
    assert payload["quote"]["yes_ask"] == 0.40
    assert payload["quote"]["no_ask"] == 0.68
    assert payload["predictions"]["model_id"] == "brownian_zero_drift__rv30"
    assert payload["predictions"]["model_p_yes"] == 0.46
    assert payload["hmm_state"]["hmm_model_id"] == "laplace_1m__gaussian_hmm__k4"
    assert payload["hmm_state"]["hmm_state"] == 3


def test_live_input_builder_missing_active_market(tmp_path: Path):
    built = _builder(tmp_path, market_fn=lambda: {"market": None}).build()
    assert built["ok"] is False
    assert built["missing_input_reason"] == "missing_active_market"


def test_live_input_builder_missing_quote(tmp_path: Path):
    def bad_quote(token_id: str):
        return {"token_id": token_id, "fetch_ok": False, "best_ask": None}

    built = _builder(tmp_path, quote_fn=bad_quote).build()
    assert built["ok"] is False
    assert built["missing_input_reason"] == "quote_both_fetch_failed"
    assert any("quote" in c for c in built["missing_components"])


def test_live_input_builder_missing_hmm(tmp_path: Path):
    built = _builder(tmp_path, hmm_state_path=tmp_path / "missing_hmm.json").build()
    assert built["ok"] is False
    assert "hmm_state" in built["missing_components"]


def test_live_input_builder_invalid_partial_json_rejected_safely(tmp_path: Path):
    hmm_path, _ = _state_paths(tmp_path)
    bad_brownian = tmp_path / "bad_brownian.json"
    bad_brownian.write_text("{", encoding="utf-8")
    built = _builder(tmp_path, hmm_state_path=hmm_path, brownian_state_path=bad_brownian).build()
    assert built["ok"] is False
    assert "brownian_probability" in built["missing_components"]


def test_live_input_builder_wrong_model_id_rejected(tmp_path: Path):
    hmm_path, brownian_path = _state_paths(tmp_path)
    payload = json.loads(brownian_path.read_text(encoding="utf-8"))
    payload["model_id"] = "baseline_50"
    brownian_path.write_text(json.dumps(payload), encoding="utf-8")
    built = BTC5MCanaryLiveInputBuilder(
        LiveInputBuilderConfig(hmm_state_path=hmm_path, brownian_state_path=brownian_path, max_quote_age_ms=5000),
        market_fn=_market,
        quote_fn=_quote,
        binance_price_fn=lambda: {"price": 100.0},
        now_fn=lambda: NOW,
    ).build()
    assert built["ok"] is False
    assert "brownian_probability" in built["missing_components"]


def test_live_input_builder_stale_state_rejected(tmp_path: Path):
    hmm_path, brownian_path = _state_paths(tmp_path)
    stale = (NOW - timedelta(seconds=60)).isoformat()
    for path in (hmm_path, brownian_path):
        payload = json.loads(path.read_text(encoding="utf-8"))
        payload["asof_ts"] = stale
        path.write_text(json.dumps(payload), encoding="utf-8")
    built = BTC5MCanaryLiveInputBuilder(
        LiveInputBuilderConfig(hmm_state_path=hmm_path, brownian_state_path=brownian_path, max_quote_age_ms=5000, max_state_age_sec=15),
        market_fn=_market,
        quote_fn=_quote,
        binance_price_fn=lambda: {"price": 100.0},
        now_fn=lambda: NOW,
    ).build()
    assert built["ok"] is False
    assert {"brownian_probability", "hmm_state"}.issubset(set(built["missing_components"]))


def test_live_input_builder_missing_brownian_probability(tmp_path: Path, monkeypatch):
    monkeypatch.delenv("BTC5M_LIVE_REFERENCE_PRICE", raising=False)
    monkeypatch.delenv("BTC5M_LIVE_RV30", raising=False)
    built = _builder(tmp_path, brownian_state_path=None).build()
    assert built["ok"] is False
    assert "brownian_probability" in built["missing_components"]


# ---- quote diagnostics tests ----

def test_live_input_builder_quote_yes_fetch_failed_specific_reason(tmp_path: Path):
    """When YES book fails to fetch, missing_input_reason is quote_yes_fetch_failed."""
    def bad_yes_quote(token_id: str):
        if token_id == "yes-token":
            return {"token_id": token_id, "fetch_ok": False, "best_ask": None, "error_kind": "http_error", "http_status": 503, "error": "service unavailable"}
        return _quote(token_id)

    built = _builder(tmp_path, quote_fn=bad_yes_quote).build()
    assert built["ok"] is False
    assert built["missing_input_reason"] == "quote_yes_fetch_failed"
    assert "quote_yes_fetch_failed" in built["missing_components"]


def test_live_input_builder_quote_no_best_ask_missing_specific_reason(tmp_path: Path):
    """When NO book fetches OK but has no best_ask, missing_input_reason is quote_no_best_ask_missing."""
    def no_missing_ask(token_id: str):
        q = dict(_quote(token_id))
        if token_id == "no-token":
            q["best_ask"] = None
        return q

    built = _builder(tmp_path, quote_fn=no_missing_ask).build()
    assert built["ok"] is False
    assert built["missing_input_reason"] == "quote_no_best_ask_missing"
    assert "quote_no_best_ask_missing" in built["missing_components"]


def test_live_input_builder_quote_both_fetch_failed(tmp_path: Path):
    """When both YES and NO fetch fail, reason is quote_both_fetch_failed."""
    def both_fail(token_id: str):
        return {"token_id": token_id, "fetch_ok": False, "best_ask": None, "error_kind": "timeout"}

    built = _builder(tmp_path, quote_fn=both_fail).build()
    assert built["ok"] is False
    assert built["missing_input_reason"] == "quote_both_fetch_failed"


def test_live_input_builder_includes_quote_diagnostics_in_meta_when_ok(tmp_path: Path):
    """Even on success, live_input_meta includes per-side quote diagnostic fields."""
    built = _builder(tmp_path).build()
    assert built["ok"] is True
    meta = built["input"]["live_input_meta"]
    assert meta["yes_quote_fetch_ok"] is True
    assert meta["no_quote_fetch_ok"] is True
    assert meta["yes_best_ask"] == 0.40
    assert meta["no_best_ask"] == 0.68
    assert meta["yes_token_id"] == "yes-token"
    assert meta["no_token_id"] == "no-token"
    assert meta["quote_missing_reason"] is None


def test_live_input_builder_includes_quote_diagnostics_in_meta_when_failed(tmp_path: Path):
    """On quote failure, live_input_meta includes error_kind and http_status."""
    def bad_yes_quote(token_id: str):
        if token_id == "yes-token":
            return {"token_id": token_id, "fetch_ok": False, "best_ask": None,
                    "error_kind": "http_error", "http_status": 503, "error": "bad gateway"}
        return _quote(token_id)

    built = _builder(tmp_path, quote_fn=bad_yes_quote).build()
    assert built["ok"] is False
    meta = built["input"]["live_input_meta"]
    assert meta["yes_quote_fetch_ok"] is False
    assert meta["yes_quote_error_kind"] == "http_error"
    assert meta["yes_quote_http_status"] == 503
    assert meta["quote_missing_reason"] == "quote_yes_fetch_failed"


def test_live_input_builder_quote_missing_near_market_end(tmp_path: Path, monkeypatch):
    """When market age >= BTC5M_SKIP_IF_QUOTE_MISSING_AFTER_MARKET_AGE_SEC and quote is missing,
    reason becomes quote_missing_near_market_end."""
    monkeypatch.setenv("BTC5M_SKIP_IF_QUOTE_MISSING_AFTER_MARKET_AGE_SEC", "120")

    # NOW - 120s puts market at exactly the boundary; use 130s old market
    old_start = NOW - timedelta(seconds=130)
    old_end = old_start + timedelta(minutes=5)

    def old_market():
        return {
            "market": {
                "market_id": "m1",
                "condition_id": "c1",
                "token_yes": "yes-token",
                "token_no": "no-token",
                "start_time": old_start.isoformat(),
                "end_time": old_end.isoformat(),
            },
            "detection_source": "fixture",
        }

    def fail_quote(token_id: str):
        return {"token_id": token_id, "fetch_ok": False, "best_ask": None, "error_kind": "timeout"}

    built = _builder(tmp_path, market_fn=old_market, quote_fn=fail_quote).build()
    assert built["ok"] is False
    assert built["missing_input_reason"] == "quote_missing_near_market_end"
    meta = built["input"]["live_input_meta"]
    assert meta["quote_missing_reason"] == "quote_missing_near_market_end"
