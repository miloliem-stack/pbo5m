"""Tests for scripts/diagnose_btc5m_live_quote.py"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest

from scripts.diagnose_btc5m_live_quote import _quote_summary, _run_once, main


NOW = datetime(2026, 5, 22, 10, 2, 0, tzinfo=timezone.utc)


def _market(start_offset_sec: float = -120):
    start = NOW - timedelta(seconds=abs(start_offset_sec))
    end = start + timedelta(minutes=5)
    return {
        "market": {
            "market_id": "m1",
            "condition_id": "c1",
            "slug": "btc-updown",
            "token_yes": "yes-token",
            "token_no": "no-token",
            "start_time": start.isoformat(),
            "end_time": end.isoformat(),
        },
        "detection_source": "fixture",
    }


def _good_quote(token_id: str) -> dict:
    return {
        "token_id": token_id,
        "fetch_ok": True,
        "best_bid": 0.38,
        "best_ask": 0.40 if token_id == "yes-token" else 0.62,
        "bid_size": 200.0,
        "ask_size": 100.0,
        "is_empty": False,
        "is_crossed": False,
        "http_status": 200,
        "error_kind": None,
        "error": None,
        "response_text_sample": None,
        "age_seconds": 0.1,
        "fetched_at": NOW.isoformat(),
    }


def _bad_quote(token_id: str) -> dict:
    return {
        "token_id": token_id,
        "fetch_ok": False,
        "best_bid": None,
        "best_ask": None,
        "bid_size": None,
        "ask_size": None,
        "is_empty": True,
        "is_crossed": False,
        "http_status": 503,
        "error_kind": "http_error",
        "error": "service unavailable",
        "response_text_sample": "upstream error",
        "age_seconds": 0.0,
        "fetched_at": NOW.isoformat(),
    }


def test_quote_summary_excludes_raw_book():
    summary = _quote_summary(_good_quote("yes-token"), "yes-token")
    assert "raw" not in summary
    assert summary["fetch_ok"] is True
    assert summary["best_ask"] == 0.40
    assert summary["best_bid"] == 0.38
    assert summary["is_empty"] is False


def test_quote_summary_captures_error_fields():
    summary = _quote_summary(_bad_quote("no-token"), "no-token")
    assert summary["fetch_ok"] is False
    assert summary["error_kind"] == "http_error"
    assert summary["http_status"] == 503
    assert summary["error"] == "service unavailable"


def test_quote_summary_truncates_long_error(monkeypatch):
    long_error = "x" * 300
    q = dict(_bad_quote("yes-token"))
    q["error"] = long_error
    summary = _quote_summary(q, "yes-token")
    assert len(summary["error"]) <= 130  # 120 + ellipsis


def test_run_once_valid_quotes(monkeypatch):
    monkeypatch.setattr("scripts.diagnose_btc5m_live_quote.route_btc_5m_market", lambda: _market())
    monkeypatch.setattr("scripts.diagnose_btc5m_live_quote.get_quote_snapshot", lambda token_id, force_refresh=False: _good_quote(token_id))
    monkeypatch.setattr("scripts.diagnose_btc5m_live_quote.utc_now", lambda: NOW)

    result = _run_once()

    assert result["route_ok"] is True
    assert result["valid_topbook"] is True
    assert result["yes"]["fetch_ok"] is True
    assert result["yes"]["best_ask"] == 0.40
    assert result["no"]["fetch_ok"] is True
    assert result["no"]["best_ask"] == 0.62
    assert result["slug"] == "btc-updown"
    # No private keys in output
    dumped = json.dumps(result)
    for secret in ("PRIVATE_KEY", "SECRET", "PASSPHRASE", "PASSWORD"):
        assert secret not in dumped


def test_run_once_failed_quote_shows_error(monkeypatch):
    monkeypatch.setattr("scripts.diagnose_btc5m_live_quote.route_btc_5m_market", lambda: _market())
    monkeypatch.setattr(
        "scripts.diagnose_btc5m_live_quote.get_quote_snapshot",
        lambda token_id, force_refresh=False: _bad_quote(token_id),
    )
    monkeypatch.setattr("scripts.diagnose_btc5m_live_quote.utc_now", lambda: NOW)

    result = _run_once()

    assert result["route_ok"] is True
    assert result["valid_topbook"] is False
    assert result["yes"]["fetch_ok"] is False
    assert result["yes"]["error_kind"] == "http_error"
    assert result["no"]["fetch_ok"] is False


def test_run_once_no_market(monkeypatch):
    monkeypatch.setattr("scripts.diagnose_btc5m_live_quote.route_btc_5m_market", lambda: {"market": None})
    monkeypatch.setattr("scripts.diagnose_btc5m_live_quote.utc_now", lambda: NOW)

    result = _run_once()

    assert result["route_ok"] is False


def test_run_once_route_exception(monkeypatch):
    def _raise():
        raise RuntimeError("network timeout")

    monkeypatch.setattr("scripts.diagnose_btc5m_live_quote.route_btc_5m_market", _raise)
    monkeypatch.setattr("scripts.diagnose_btc5m_live_quote.utc_now", lambda: NOW)

    result = _run_once()

    assert result["route_ok"] is False
    assert "network timeout" in result["route_error"]


def test_main_prints_json_and_exits_zero(monkeypatch, capsys):
    monkeypatch.setattr("scripts.diagnose_btc5m_live_quote.route_btc_5m_market", lambda: _market())
    monkeypatch.setattr("scripts.diagnose_btc5m_live_quote.get_quote_snapshot", lambda token_id, force_refresh=False: _good_quote(token_id))
    monkeypatch.setattr("scripts.diagnose_btc5m_live_quote.utc_now", lambda: NOW)

    rc = main([])

    assert rc == 0
    out = capsys.readouterr().out.strip()
    parsed = json.loads(out)
    assert parsed["route_ok"] is True
    assert parsed["valid_topbook"] is True
