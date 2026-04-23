import json
import sys
from datetime import datetime, timezone
from pathlib import Path

from scripts import run_btc_5m_market_recorder, run_eth_5m_market_recorder
from src.market_router_5m import route_btc_5m_market
from src.market_router_eth_5m import _candidate_slugs as eth_candidate_slugs
from src.market_router_eth_5m import _looks_like_eth_5m
from src.runtime.market_recorder import (
    MarketRecorderRuntime,
    _market_identity,
    list_market_recorder_segments,
)


def test_market_identity_ignores_raw_market_jitter():
    base_market = {
        "market_id": "m1",
        "slug": "btc-up",
        "condition_id": "c1",
        "token_yes": "yes",
        "token_no": "no",
        "start_time": "2026-04-18T20:00:00Z",
        "end_time": "2026-04-18T20:05:00Z",
        "routing_score": 10,
        "raw_market": {"foo": "bar"},
    }
    jittered_market = {
        **base_market,
        "routing_score": 4,
        "raw_market": {"foo": "baz", "changed": True},
    }

    assert _market_identity(base_market) == _market_identity(jittered_market)


def test_market_identity_changes_when_market_identity_changes():
    first_market = {
        "market_id": "m1",
        "slug": "btc-up",
        "condition_id": "c1",
        "token_yes": "yes",
        "token_no": "no",
        "start_time": "2026-04-18T20:00:00Z",
        "end_time": "2026-04-18T20:05:00Z",
    }
    second_market = {
        **first_market,
        "market_id": "m2",
    }

    assert _market_identity(first_market) != _market_identity(second_market)


def test_eth_router_slug_and_keyword_recognition():
    now = datetime(2026, 4, 20, 12, 3, tzinfo=timezone.utc)

    slugs = eth_candidate_slugs(now)

    assert slugs[0].startswith("eth-updown-5m-")
    assert _looks_like_eth_5m({"slug": "eth-updown-5m-123", "title": "ignored"}) is True
    assert _looks_like_eth_5m({"slug": "market-x", "title": "Will Ethereum be up in the next 5 minute market?"}) is True
    assert _looks_like_eth_5m({"slug": "btc-updown-5m-123", "title": "Will Bitcoin be up in 5 minutes?"}) is False


def test_runtime_allows_router_injection_and_optional_asset():
    def fake_router():
        return {"market": None}

    runtime = MarketRecorderRuntime(router_fn=fake_router, asset="eth")

    assert runtime.router_fn is fake_router
    assert runtime._with_asset({"record_type": "market_route"}) == {"record_type": "market_route", "asset": "eth"}
    assert MarketRecorderRuntime(router_fn=fake_router)._with_asset({"record_type": "market_route"}) == {"record_type": "market_route"}


def test_eth_runner_wires_eth_defaults(monkeypatch):
    captured = {}

    class FakeRuntime:
        def __init__(self, **kwargs):
            captured["kwargs"] = kwargs

        def run(self, *, duration_seconds=None):
            captured["duration_seconds"] = duration_seconds
            return {"ok": True}

    monkeypatch.setattr(run_eth_5m_market_recorder, "MarketRecorderRuntime", FakeRuntime)
    monkeypatch.setattr(sys, "argv", ["run_eth_5m_market_recorder.py"])

    assert run_eth_5m_market_recorder.main() == 0
    assert captured["kwargs"]["output_dir"] == "artifacts/market_recorder_eth"
    assert captured["kwargs"]["asset"] == "eth"
    assert captured["kwargs"]["router_fn"].__name__ == "route_eth_5m_market"


def test_btc_runner_keeps_btc_defaults(monkeypatch):
    captured = {}

    class FakeRuntime:
        def __init__(self, **kwargs):
            captured["kwargs"] = kwargs

        def run(self, *, duration_seconds=None):
            captured["duration_seconds"] = duration_seconds
            return {"ok": True}

    monkeypatch.setattr(run_btc_5m_market_recorder, "MarketRecorderRuntime", FakeRuntime)
    monkeypatch.setattr(sys, "argv", ["run_btc_5m_market_recorder.py"])

    assert run_btc_5m_market_recorder.main() == 0
    assert captured["kwargs"]["output_dir"] == "artifacts/market_recorder"
    assert "asset" not in captured["kwargs"]
    assert "router_fn" not in captured["kwargs"]
    assert MarketRecorderRuntime().router_fn is route_btc_5m_market


class _FakeClock:
    def __init__(self, initial: datetime):
        self.current = initial

    def now(self) -> datetime:
        return self.current

    def set(self, value: datetime) -> None:
        self.current = value


def _read_jsonl(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


def test_market_recorder_rotates_by_utc_hour_without_losing_rows(tmp_path):
    clock = _FakeClock(datetime(2026, 4, 23, 10, 59, 59, tzinfo=timezone.utc))
    runtime = MarketRecorderRuntime(output_dir=tmp_path, now_fn=clock.now, router_fn=lambda: {"market": None})
    first_row = {"ts": "2026-04-23T10:59:59+00:00", "record_type": "quote_snapshot", "value": 1}
    second_row = {"ts": "2026-04-23T11:00:00+00:00", "record_type": "quote_snapshot", "value": 2}

    runtime._write_record("quotes", first_row)
    clock.set(datetime(2026, 4, 23, 11, 0, 0, tzinfo=timezone.utc))
    runtime._write_record("quotes", second_row)

    first_segment = tmp_path / "2026-04-23" / "10"
    second_segment = tmp_path / "2026-04-23" / "11"

    assert _read_jsonl(first_segment / "market_quotes.jsonl") == [first_row]
    assert _read_jsonl(second_segment / "market_quotes.jsonl") == [second_row]

    first_manifest = json.loads((first_segment / "segment_manifest.json").read_text(encoding="utf-8"))
    assert first_manifest["segment_start_utc"] == "2026-04-23T10:00:00+00:00"
    assert first_manifest["segment_end_utc"] == "2026-04-23T11:00:00+00:00"
    assert first_manifest["closed_cleanly"] is True
    assert first_manifest["row_counts"]["market_quotes.jsonl"] == 1

    runtime._close_active_segment(closed_cleanly=True)


def test_market_recorder_writes_final_manifest_and_lists_closed_segments(tmp_path):
    clock = _FakeClock(datetime(2026, 4, 23, 12, 15, 0, tzinfo=timezone.utc))
    runtime = MarketRecorderRuntime(output_dir=tmp_path, now_fn=clock.now, router_fn=lambda: {"market": None})

    runtime._write_record("meta", {"ts": "2026-04-23T12:15:00+00:00", "record_type": "market_route"})

    active_report = list_market_recorder_segments(tmp_path)
    assert active_report["active_segment"]["segment_dir"].endswith("/2026-04-23/12")
    assert active_report["closed_segments"] == []

    clock.set(datetime(2026, 4, 23, 12, 59, 59, tzinfo=timezone.utc))
    runtime._close_active_segment(closed_cleanly=True)

    closed_report = list_market_recorder_segments(tmp_path)
    assert closed_report["active_segment"] is None
    assert len(closed_report["closed_segments"]) == 1
    manifest = closed_report["closed_segments"][0]
    assert manifest["segment_start_utc"] == "2026-04-23T12:00:00+00:00"
    assert manifest["segment_end_utc"] == "2026-04-23T12:59:59+00:00"
    assert manifest["closed_cleanly"] is True
    assert manifest["row_counts"]["market_meta.jsonl"] == 1


def test_market_recorder_jsonl_rows_remain_backward_compatible(tmp_path):
    clock = _FakeClock(datetime(2026, 4, 23, 13, 0, 1, tzinfo=timezone.utc))
    runtime = MarketRecorderRuntime(output_dir=tmp_path, now_fn=clock.now, router_fn=lambda: {"market": None})
    row = {
        "ts": "2026-04-23T13:00:01+00:00",
        "record_type": "heartbeat",
        "output_dir": "artifacts/market_recorder",
        "freshness_seconds": {"quotes": 1.0},
    }

    runtime._write_record("heartbeat", row)
    runtime._close_active_segment(closed_cleanly=True)

    path = tmp_path / "2026-04-23" / "13" / "recorder_heartbeat.jsonl"
    assert _read_jsonl(path) == [row]
