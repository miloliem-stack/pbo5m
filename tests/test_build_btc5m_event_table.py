import json
from pathlib import Path

from scripts import build_btc5m_event_table


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")


def _write_segment(root: Path, day: str, hour: str, rows_by_file: dict[str, list[dict]], *, closed_cleanly: bool = True, with_manifest: bool = True) -> Path:
    segment = root / day / hour
    segment.mkdir(parents=True, exist_ok=True)
    counts = {}
    for name in build_btc5m_event_table.JSONL_FILENAMES:
        rows = rows_by_file.get(name, [])
        _write_jsonl(segment / name, rows)
        counts[name] = len(rows)
    if with_manifest:
        (segment / "segment_manifest.json").write_text(
            json.dumps(
                {
                    "segment_start_utc": f"{day}T{hour}:00:00+00:00",
                    "segment_end_utc": f"{day}T{hour}:59:59+00:00",
                    "closed_cleanly": closed_cleanly,
                    "row_counts": counts,
                    "recorder_version": "test",
                    "asset": None,
                    "output_root": str(root),
                }
            ),
            encoding="utf-8",
        )
    return segment


def test_load_quotes_infers_legacy_quote_capture_status(tmp_path):
    quotes_path = tmp_path / "market_quotes.jsonl"
    _write_jsonl(
        quotes_path,
        [
            {
                "ts": "2026-04-17T13:05:00+00:00",
                "record_type": "quote_snapshot",
                "slug": "btc-updown-5m-1",
                "market_id": "m1",
                "market_start_time": "2026-04-17T13:00:00+00:00",
                "market_end_time": "2026-04-17T13:05:00+00:00",
                "yes": {"error": "HTTP 403"},
                "no": {"error": "HTTP 403"},
            },
            {
                "ts": "2026-04-17T13:05:02+00:00",
                "record_type": "quote_snapshot",
                "slug": "btc-updown-5m-2",
                "market_id": "m2",
                "market_start_time": "2026-04-17T13:05:00+00:00",
                "market_end_time": "2026-04-17T13:10:00+00:00",
                "yes": {"best_bid": 0.4, "best_ask": 0.6, "mid": 0.5, "spread": 0.2},
                "no": {"best_bid": 0.4, "best_ask": 0.6, "mid": 0.5, "spread": 0.2},
            },
        ],
    )

    quotes = build_btc5m_event_table.load_quotes([quotes_path]).deduped_df
    failed = quotes.loc[quotes["market_id"] == "m1"].iloc[0]
    succeeded = quotes.loc[quotes["market_id"] == "m2"].iloc[0]

    assert bool(failed["quote_capture_ok"]) is False
    assert failed["quote_capture_status"] == "failed"
    assert bool(succeeded["quote_capture_ok"]) is True
    assert succeeded["quote_capture_status"] == "ok"


def test_discover_sources_supports_legacy_only_root(tmp_path):
    legacy_root = tmp_path / "legacy"
    _write_jsonl(legacy_root / "chainlink_prices.jsonl", [])
    _write_jsonl(legacy_root / "binance_prices.jsonl", [])
    _write_jsonl(legacy_root / "market_quotes.jsonl", [])
    _write_jsonl(legacy_root / "market_meta.jsonl", [])
    _write_jsonl(legacy_root / "recorder_heartbeat.jsonl", [])
    legacy = build_btc5m_event_table.discover_sources(legacy_root)

    assert len(legacy.sources) == 1
    assert legacy.sources[0].source_kind == "legacy_dir"
    assert legacy.diagnostics["included_legacy_dirs"] == 1
    assert legacy.diagnostics["included_segment_dirs"] == 0


def test_discover_sources_supports_rotated_only_root_and_skips_active_segments(tmp_path):
    rotated_root = tmp_path / "rotated"
    _write_segment(rotated_root, "2026-04-24", "00", {}, closed_cleanly=True, with_manifest=True)
    _write_segment(rotated_root, "2026-04-24", "01", {}, closed_cleanly=True, with_manifest=False)
    rotated = build_btc5m_event_table.discover_sources(rotated_root)

    assert len(rotated.sources) == 1
    assert rotated.sources[0].source_kind == "segment_dir"
    assert rotated.diagnostics["included_legacy_dirs"] == 0
    assert rotated.diagnostics["included_segment_dirs"] == 1
    assert rotated.diagnostics["skipped_active_segment_dirs"] == 1


def test_discover_sources_includes_legacy_and_rotated_segments_from_same_root(tmp_path):
    root = tmp_path / "mixed"
    _write_jsonl(root / "chainlink_prices.jsonl", [])
    _write_jsonl(root / "binance_prices.jsonl", [])
    _write_jsonl(root / "market_quotes.jsonl", [])
    _write_jsonl(root / "market_meta.jsonl", [])
    _write_jsonl(root / "recorder_heartbeat.jsonl", [])
    closed_segment = _write_segment(root, "2026-04-24", "00", {}, closed_cleanly=True, with_manifest=True)

    discovery = build_btc5m_event_table.discover_sources(root)

    assert len(discovery.sources) == 2
    assert discovery.diagnostics["included_legacy_dirs"] == 1
    assert discovery.diagnostics["included_segment_dirs"] == 1
    assert str(root) in [source["path"] for source in discovery.diagnostics["included_sources"]]
    assert str(closed_segment) in [source["path"] for source in discovery.diagnostics["included_sources"]]


def test_discover_sources_skips_unreadable_and_unclean_manifests(tmp_path):
    root = tmp_path / "segments"
    _write_segment(root, "2026-04-24", "00", {}, closed_cleanly=False, with_manifest=True)
    unreadable = _write_segment(root, "2026-04-24", "01", {}, closed_cleanly=True, with_manifest=True)
    (unreadable / "segment_manifest.json").write_text("{broken json", encoding="utf-8")

    discovery = build_btc5m_event_table.discover_sources(root)

    assert discovery.diagnostics["included_segment_dirs"] == 0
    assert discovery.diagnostics["skipped_unclean_segment_dirs"] == 1
    assert discovery.diagnostics["skipped_unreadable_manifest_dirs"] == 1


def test_build_windows_handles_empty_inputs():
    windows = build_btc5m_event_table.build_windows(
        build_btc5m_event_table.load_quotes([]).deduped_df,
        build_btc5m_event_table.load_meta([]).deduped_df,
    )
    assert windows.empty
    assert list(windows.columns) == ["slug", "market_id", "market_start", "market_end"]


def test_build_event_table_handles_closed_segment_without_windows(tmp_path):
    root = tmp_path / "market_recorder"
    _write_segment(
        root,
        "2026-04-24",
        "00",
        {
            "chainlink_prices.jsonl": [
                {
                    "ts": "2026-04-24T00:00:00+00:00",
                    "source_ts": "2026-04-24T00:00:00+00:00",
                    "price": 1.0,
                    "record_type": "price",
                }
            ]
        },
    )

    events, summary = build_btc5m_event_table.build_event_table(
        [root],
        boundary_method="nearest",
        max_boundary_distance_seconds=1.0,
    )

    assert events.empty
    assert summary["discovered_source_count"] == 1
    assert summary["event_rows"] == 0


def test_boundary_diagnostics_and_missing_reason_for_binance(tmp_path):
    root = tmp_path / "market_recorder"
    rows_by_file = {
        "chainlink_prices.jsonl": [
            {"ts": "2026-04-24T00:00:00+00:00", "source_ts": "2026-04-24T00:00:00+00:00", "price": 100.0, "record_type": "price"},
            {"ts": "2026-04-24T00:05:00+00:00", "source_ts": "2026-04-24T00:05:00+00:00", "price": 101.0, "record_type": "price"},
        ],
        "binance_prices.jsonl": [
            {"ts": "2026-04-24T00:00:03+00:00", "observed_at": 1776988803000, "price": 100.5, "record_type": "price"},
            {"ts": "2026-04-24T00:05:07+00:00", "observed_at": 1776989107000, "price": 100.7, "record_type": "price"},
        ],
        "market_quotes.jsonl": [
            {
                "ts": "2026-04-24T00:00:04+00:00",
                "record_type": "quote_snapshot",
                "slug": "btc-updown-5m-1",
                "market_id": "m1",
                "market_start_time": "2026-04-24T00:00:00+00:00",
                "market_end_time": "2026-04-24T00:05:00+00:00",
                "quote_capture_ok": True,
                "quote_capture_status": "ok",
                "yes": {"fetch_ok": True, "best_bid": 0.4, "best_ask": 0.6, "mid": 0.5, "spread": 0.2},
                "no": {"fetch_ok": True, "best_bid": 0.4, "best_ask": 0.6, "mid": 0.5, "spread": 0.2},
            }
        ],
        "market_meta.jsonl": [
            {
                "ts": "2026-04-24T00:00:01+00:00",
                "record_type": "market_route",
                "market_changed": True,
                "market": {
                    "slug": "btc-updown-5m-1",
                    "market_id": "m1",
                    "start_time": "2026-04-24T00:00:00+00:00",
                    "end_time": "2026-04-24T00:05:00+00:00",
                },
            }
        ],
    }
    _write_segment(root, "2026-04-24", "00", rows_by_file)

    events, summary = build_btc5m_event_table.build_event_table(
        [root],
        boundary_method="nearest",
        max_boundary_distance_seconds=1.0,
        diagnostic_buffer_seconds=10.0,
    )

    row = events.iloc[0]
    assert bool(row["chainlink_open_available"]) is True
    assert bool(row["binance_open_available"]) is False
    assert row["binance_open_missing_reason"] == "boundary_before_first_observation"
    assert row["binance_open_nearest_distance_sec"] == 3.0
    assert row["binance_rows_in_window"] == 1
    assert row["binance_rows_in_buffer_before_open"] == 0
    assert row["binance_rows_in_buffer_after_close"] == 1
    assert summary["binance_complete_rows"] == 0
    assert summary["binance_missing_given_chainlink_complete_rows"] == 1


def test_build_boundary_sweep_output_shape(tmp_path):
    root = tmp_path / "market_recorder"
    _write_segment(
        root,
        "2026-04-24",
        "00",
        {
            "chainlink_prices.jsonl": [
                {"ts": "2026-04-24T00:00:00+00:00", "source_ts": "2026-04-24T00:00:00+00:00", "price": 100.0, "record_type": "price"},
                {"ts": "2026-04-24T00:05:00+00:00", "source_ts": "2026-04-24T00:05:00+00:00", "price": 101.0, "record_type": "price"},
            ],
            "binance_prices.jsonl": [
                {"ts": "2026-04-24T00:00:01+00:00", "observed_at": 1776988801000, "price": 100.0, "record_type": "price"},
                {"ts": "2026-04-24T00:05:01+00:00", "observed_at": 1776989101000, "price": 101.0, "record_type": "price"},
            ],
            "market_quotes.jsonl": [
                {
                    "ts": "2026-04-24T00:00:02+00:00",
                    "record_type": "quote_snapshot",
                    "slug": "btc-updown-5m-1",
                    "market_id": "m1",
                    "market_start_time": "2026-04-24T00:00:00+00:00",
                    "market_end_time": "2026-04-24T00:05:00+00:00",
                    "quote_capture_ok": True,
                    "quote_capture_status": "ok",
                    "yes": {"fetch_ok": True, "best_bid": 0.4, "best_ask": 0.6, "mid": 0.5, "spread": 0.2},
                    "no": {"fetch_ok": True, "best_bid": 0.4, "best_ask": 0.6, "mid": 0.5, "spread": 0.2},
                }
            ],
            "market_meta.jsonl": [
                {
                    "ts": "2026-04-24T00:00:01+00:00",
                    "record_type": "market_route",
                    "market_changed": True,
                    "market": {
                        "slug": "btc-updown-5m-1",
                        "market_id": "m1",
                        "start_time": "2026-04-24T00:00:00+00:00",
                        "end_time": "2026-04-24T00:05:00+00:00",
                    },
                }
            ],
        },
    )

    sweep = build_btc5m_event_table.build_boundary_sweep(
        [root],
        methods=["nearest", "previous"],
        max_boundary_distances=[1.0, 2.0],
    )

    assert list(sweep.columns) == [
        "boundary_method",
        "max_boundary_distance_seconds",
        "event_rows",
        "chainlink_complete_rows",
        "binance_complete_rows",
        "proxy_comparable_rows",
        "proxy_disagreement_rows",
        "proxy_disagreement_rate",
        "proxy_disagreement_rate_tiny_moves",
        "proxy_disagreement_rate_non_tiny_moves",
    ]
    assert len(sweep) == 4
