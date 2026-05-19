import json
from pathlib import Path

import pandas as pd
import pytest

from scripts import build_compact_market_recorder_dataset as compact


def _write_jsonl(path: Path, rows: list[dict]):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(json.dumps(row) for row in rows) + ("\n" if rows else ""), encoding="utf-8")


def _book(bids, asks):
    return {"book": {"bids": [{"price": str(p), "size": str(s)} for p, s in bids], "asks": [{"price": str(p), "size": str(s)} for p, s in asks]}}


def _fixture(root: Path):
    hour = root / "2026-05-01" / "00"
    _write_jsonl(
        hour / "market_meta.jsonl",
        [
            {
                "ts": "2026-05-01T00:00:00Z",
                "market": {
                    "market_id": "m1",
                    "condition_id": "c1",
                    "slug": "btc-updown-5m-1",
                    "token_yes": "yes-token",
                    "token_no": "no-token",
                    "start_time": "2026-05-01T00:00:00Z",
                    "end_time": "2026-05-01T00:05:00Z",
                    "reference_price": 100.0,
                },
            }
        ],
    )
    _write_jsonl(
        hour / "chainlink_prices.jsonl",
        [
            {"ts": "2026-05-01T00:00:00Z", "price": 100.0},
            {"ts": "2026-05-01T00:05:00Z", "price": 101.0},
        ],
    )
    _write_jsonl(hour / "binance_prices.jsonl", [{"ts": "2026-05-01T00:05:00Z", "price": 99.0}])
    _write_jsonl(hour / "recorder_heartbeat.jsonl", [{"ts": "2026-05-01T00:00:01Z"}])
    _write_jsonl(
        hour / "market_quotes.jsonl",
        [
            {
                "ts": "2026-05-01T00:01:00Z",
                "source": "poll",
                "slug": "btc-updown-5m-1",
                "best_bid": 0.01,
                "best_ask": 0.99,
                "raw_payload_fragment": {
                    "yes_raw": _book(bids=[(0.2, 10), (0.5, 5), (0.4, 1)], asks=[(0.7, 2), (0.6, 3)]),
                    "no_raw": _book(bids=[(0.3, 4)], asks=[(0.8, 2)]),
                },
            },
            {
                "ts": "2026-05-01T00:02:00Z",
                "source": "ws",
                "token_id": "yes-token",
                "book": {"bids": [{"price": "0.9", "size": "1"}], "asks": [{"price": "0.8", "size": "1"}]},
            },
        ],
    )


def test_broken_top_level_best_bid_ask_are_ignored_and_books_recomputed(tmp_path):
    _fixture(tmp_path)
    out = tmp_path / "out"
    compact.run(
        compact.build_parser().parse_args(
            [
                "--input-root",
                str(tmp_path),
                "--output-root",
                str(out),
                "--slice-name",
                "smoke",
                "--overwrite",
            ]
        )
    )
    ticks = pd.read_parquet(out / "smoke" / "book_ticks.parquet")
    yes = ticks[(ticks["side"].eq("YES")) & (ticks["source"].eq("poll"))].iloc[0]
    assert yes["bid_px_1"] == pytest.approx(0.5)
    assert yes["ask_px_1"] == pytest.approx(0.6)
    assert yes["mid"] == pytest.approx(0.55)
    assert yes["spread"] == pytest.approx(0.1)


def test_token_mapping_assigns_side_and_no_verbose_ids_in_book_ticks(tmp_path):
    _fixture(tmp_path)
    out = tmp_path / "out"
    compact.run(compact.build_parser().parse_args(["--input-root", str(tmp_path), "--output-root", str(out), "--slice-name", "smoke", "--overwrite"]))
    ticks = pd.read_parquet(out / "smoke" / "book_ticks.parquet")
    token_tick = ticks[ticks["source"].eq("ws")].iloc[0]
    assert token_tick["side"] == "YES"
    assert "market_id" not in ticks.columns
    assert "token_id" not in ticks.columns


def test_market_windows_labels_from_chainlink_close_vs_reference(tmp_path):
    _fixture(tmp_path)
    out = tmp_path / "out"
    compact.run(compact.build_parser().parse_args(["--input-root", str(tmp_path), "--output-root", str(out), "--slice-name", "smoke", "--overwrite"]))
    windows = pd.read_parquet(out / "smoke" / "market_windows.parquet")
    assert bool(windows["label_up"].iloc[0]) is True
    assert windows["winner_side"].iloc[0] == "YES"
    assert windows["chainlink_close_price"].iloc[0] == pytest.approx(101.0)


def test_invalid_crossed_topbooks_are_flagged(tmp_path):
    _fixture(tmp_path)
    out = tmp_path / "out"
    manifest = compact.run(compact.build_parser().parse_args(["--input-root", str(tmp_path), "--output-root", str(out), "--slice-name", "smoke", "--overwrite"]))
    ticks = pd.read_parquet(out / "smoke" / "book_ticks.parquet")
    crossed = ticks[ticks["source"].eq("ws")].iloc[0]
    assert bool(crossed["is_crossed"]) is True
    assert bool(crossed["is_valid_topbook"]) is False
    assert manifest["crossed_book_rows"] == 1
    assert manifest["invalid_quote_rows"] >= 1


def test_max_files_smoke_and_manifest_counts(tmp_path):
    _fixture(tmp_path)
    out = tmp_path / "out"
    manifest = compact.run(
        compact.build_parser().parse_args(
            ["--input-root", str(tmp_path), "--output-root", str(out), "--slice-name", "smoke", "--overwrite", "--max-files", "1", "--write-debug-samples"]
        )
    )
    assert (out / "smoke" / "market_windows.parquet").exists()
    assert (out / "smoke" / "book_ticks.parquet").exists()
    assert (out / "smoke" / "compact_manifest.json").exists()
    assert (out / "smoke" / "debug_samples").exists()
    assert manifest["rows_read_by_stream"]["quotes"] == 2
    assert manifest["rows_written"]["book_ticks"] == 3
    assert manifest["markets_discovered"] == 1


def test_flat_import_row_level_date_filtering(tmp_path):
    _fixture(tmp_path)
    flat = tmp_path / "flat"
    flat.mkdir()
    hour = tmp_path / "2026-05-01" / "00"
    for name in ["market_meta.jsonl", "chainlink_prices.jsonl", "binance_prices.jsonl", "market_quotes.jsonl", "recorder_heartbeat.jsonl"]:
        (flat / name).write_text((hour / name).read_text(encoding="utf-8"), encoding="utf-8")
    out = tmp_path / "out"
    manifest = compact.run(
        compact.build_parser().parse_args(
            [
                "--input-root",
                str(flat),
                "--output-root",
                str(out),
                "--slice-name",
                "smoke",
                "--start-date",
                "2026-05-02",
                "--end-date",
                "2026-05-02",
                "--overwrite",
            ]
        )
    )
    assert manifest["rows_read_by_stream"]["quotes"] == 2
    assert manifest["rows_skipped_by_date_by_stream"]["quotes"] == 2
    assert manifest["markets_discovered"] == 0
    assert manifest["rows_written"]["book_ticks"] == 0
