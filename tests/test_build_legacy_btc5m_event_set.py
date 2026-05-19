import json
from pathlib import Path

from scripts import build_legacy_btc5m_event_set


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")


def _write_legacy_root(root: Path, *, chainlink_rows: list[dict], binance_rows: list[dict], meta_rows: list[dict], quote_rows: list[dict]) -> None:
    _write_jsonl(root / "chainlink_prices.jsonl", chainlink_rows)
    _write_jsonl(root / "binance_prices.jsonl", binance_rows)
    _write_jsonl(root / "market_meta.jsonl", meta_rows)
    _write_jsonl(root / "market_quotes.jsonl", quote_rows)
    _write_jsonl(root / "recorder_heartbeat.jsonl", [])


def _base_meta() -> dict:
    return {
        "ts": "2026-04-18T19:24:32+00:00",
        "record_type": "market_route",
        "market_changed": True,
        "market": {
            "slug": "btc-updown-5m-1776540000",
            "market_id": "m1",
            "start_time": "2026-04-18T19:20:00+00:00",
            "end_time": "2026-04-18T19:25:00+00:00",
        },
    }


def _base_quote(ts: str = "2026-04-18T19:20:01+00:00") -> dict:
    return {
        "ts": ts,
        "record_type": "quote_snapshot",
        "quote_capture_ok": True,
        "quote_capture_status": "ok",
        "slug": "btc-updown-5m-1776540000",
        "market_id": "m1",
        "market_start_time": "2026-04-18T19:20:00+00:00",
        "market_end_time": "2026-04-18T19:25:00+00:00",
        "yes": {"best_bid": 0.40, "best_ask": 0.45, "mid": 0.425, "spread": 0.05},
        "no": {"best_bid": 0.55, "best_ask": 0.60, "mid": 0.575, "spread": 0.05},
    }


def test_nearest_observation_inside_two_seconds_is_accepted(tmp_path):
    root = tmp_path / "legacy"
    _write_legacy_root(
        root,
        chainlink_rows=[
            {"ts": "2026-04-18T19:20:00.500000+00:00", "source_ts": "2026-04-18T19:20:00.500000+00:00", "price": 100.0, "record_type": "tick"},
            {"ts": "2026-04-18T19:25:01+00:00", "source_ts": "2026-04-18T19:25:01+00:00", "price": 101.0, "record_type": "tick"},
        ],
        binance_rows=[
            {"ts": "2026-04-18T19:20:01+00:00", "observed_at": 1776540001000, "price": 99.0, "record_type": "tick"},
            {"ts": "2026-04-18T19:25:01+00:00", "observed_at": 1776540301000, "price": 100.0, "record_type": "tick"},
        ],
        meta_rows=[_base_meta()],
        quote_rows=[_base_quote()],
    )

    events, _ = build_legacy_btc5m_event_set.build_legacy_event_set(input_roots=[root], nearest_tolerance_sec=2.0)

    row = events.iloc[0]
    assert bool(row["missing_chainlink_start"]) is False
    assert row["chainlink_start_abs_lag_sec"] == 0.5
    assert bool(row["missing_binance_start"]) is False


def test_nearest_observation_outside_two_seconds_is_rejected(tmp_path):
    root = tmp_path / "legacy"
    _write_legacy_root(
        root,
        chainlink_rows=[
            {"ts": "2026-04-18T19:20:03+00:00", "source_ts": "2026-04-18T19:20:03+00:00", "price": 100.0, "record_type": "tick"},
            {"ts": "2026-04-18T19:25:03+00:00", "source_ts": "2026-04-18T19:25:03+00:00", "price": 101.0, "record_type": "tick"},
        ],
        binance_rows=[],
        meta_rows=[_base_meta()],
        quote_rows=[],
    )

    events, _ = build_legacy_btc5m_event_set.build_legacy_event_set(input_roots=[root], nearest_tolerance_sec=2.0)

    row = events.iloc[0]
    assert bool(row["missing_chainlink_start"]) is True
    assert row["chainlink_start_abs_lag_sec"] is None


def test_chainlink_and_binance_labels_computed_separately_and_disagreement_flagged(tmp_path):
    root = tmp_path / "legacy"
    _write_legacy_root(
        root,
        chainlink_rows=[
            {"ts": "2026-04-18T19:20:00+00:00", "source_ts": "2026-04-18T19:20:00+00:00", "price": 100.0, "record_type": "tick"},
            {"ts": "2026-04-18T19:25:00+00:00", "source_ts": "2026-04-18T19:25:00+00:00", "price": 101.0, "record_type": "tick"},
        ],
        binance_rows=[
            {"ts": "2026-04-18T19:20:00+00:00", "observed_at": 1776540000000, "price": 101.0, "record_type": "tick"},
            {"ts": "2026-04-18T19:25:00+00:00", "observed_at": 1776540300000, "price": 100.0, "record_type": "tick"},
        ],
        meta_rows=[_base_meta()],
        quote_rows=[_base_quote()],
    )

    events, manifest = build_legacy_btc5m_event_set.build_legacy_event_set(input_roots=[root], nearest_tolerance_sec=2.0)

    row = events.iloc[0]
    assert row["chainlink_label"] == "UP"
    assert row["binance_label"] == "DOWN"
    assert bool(row["label_agreement"]) is False
    assert bool(row["chainlink_binance_label_disagree"]) is True
    assert manifest["data_quality_flag_counts"]["chainlink_binance_label_disagree"] == 1


def test_manifest_counts_match_emitted_rows(tmp_path):
    root = tmp_path / "legacy"
    _write_legacy_root(
        root,
        chainlink_rows=[
            {"ts": "2026-04-18T19:20:00+00:00", "source_ts": "2026-04-18T19:20:00+00:00", "price": 100.0, "record_type": "tick"},
            {"ts": "2026-04-18T19:25:00+00:00", "source_ts": "2026-04-18T19:25:00+00:00", "price": 101.0, "record_type": "tick"},
        ],
        binance_rows=[
            {"ts": "2026-04-18T19:20:00+00:00", "observed_at": 1776540000000, "price": 100.0, "record_type": "tick"},
            {"ts": "2026-04-18T19:25:00+00:00", "observed_at": 1776540300000, "price": 101.0, "record_type": "tick"},
        ],
        meta_rows=[_base_meta()],
        quote_rows=[_base_quote()],
    )

    events, manifest = build_legacy_btc5m_event_set.build_legacy_event_set(input_roots=[root], nearest_tolerance_sec=2.0)
    outputs = build_legacy_btc5m_event_set.write_frozen_event_set(
        events=events,
        manifest=manifest,
        output_parquet=tmp_path / "events.parquet",
        output_csv=tmp_path / "events.csv",
        manifest_path=tmp_path / "manifest.json",
    )
    manifest_payload = json.loads((tmp_path / "manifest.json").read_text(encoding="utf-8"))

    assert manifest["number_of_events_emitted"] == len(events) == 1
    assert manifest_payload["quote_coverage_count"] == 1
    assert manifest_payload["artifact_path"] == outputs["artifact_path"]
