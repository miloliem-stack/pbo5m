from __future__ import annotations

import json
from argparse import Namespace
from pathlib import Path

import pandas as pd
import pytest

from scripts import build_btc5m_candidate_snapshot_table as builder


def _write_binance_csv(root: Path, rows: list[tuple[str, float]]) -> None:
    root.mkdir(parents=True, exist_ok=True)
    lines = []
    for ts_text, close in rows:
        ts = pd.Timestamp(ts_text, tz="UTC")
        open_ms = int(ts.timestamp() * 1000)
        close_ms = open_ms + 59_999
        lines.append(f"{open_ms},{close},{close},{close},{close},1,{close_ms},0,0,0,0,0")
    (root / "BTCUSDT-1m-fixture.csv").write_text("\n".join(lines), encoding="utf-8")


def _write_compact_fixture(root: Path, duplicate: bool = False) -> None:
    root.mkdir(parents=True, exist_ok=True)
    windows = pd.DataFrame(
        {
            "market_key": [1],
            "market_id": ["m1"],
            "condition_id": ["c1"],
            "slug": ["btc-updown-5m-fixture"],
            "market_start_ts": [pd.Timestamp("2026-05-01T00:00:00Z")],
            "market_end_ts": [pd.Timestamp("2026-05-01T00:05:00Z")],
            "reference_price": [100.0],
            "chainlink_close_price": [101.0],
            "label_up": [True],
            "winner_side": ["YES"],
        }
    )
    ticks = []
    for side, ask, bid in [("YES", 0.55, 0.54), ("NO", 0.47, 0.46)]:
        row = {
            "market_key": 1,
            "ts": pd.Timestamp("2026-05-01T00:01:30Z"),
            "side": side,
            "ask_px_1": ask,
            "ask_sz_1": 10.0,
            "ask_px_2": ask + 0.01,
            "ask_sz_2": 20.0,
            "ask_px_3": ask + 0.02,
            "ask_sz_3": 30.0,
            "bid_px_1": bid,
            "bid_sz_1": 11.0,
            "spread": ask - bid,
            "is_valid_topbook": True,
        }
        for level in range(4, 11):
            row[f"ask_px_{level}"] = ask + level / 100
            row[f"ask_sz_{level}"] = 1.0
        ticks.append(row)
        if duplicate and side == "YES":
            duplicate_row = row.copy()
            duplicate_row["ask_px_1"] = 0.56
            ticks.append(duplicate_row)
    windows.to_parquet(root / "market_windows.parquet", index=False)
    pd.DataFrame(ticks).to_parquet(root / "book_ticks.parquet", index=False)


def _args(tmp_path: Path, **overrides) -> Namespace:
    base = {
        "source_mode": "recorder_chainlink",
        "output_dir": tmp_path / "out",
        "compact_root": tmp_path / "compact",
        "binance_root": [tmp_path / "binance"],
        "start_ts": None,
        "end_ts": None,
        "valid_topbook_only": True,
        "top_n_levels": 10,
        "decision_frequency_sec": 60,
        "overwrite": True,
    }
    base.update(overrides)
    return Namespace(**base)


def test_recorder_builder_uses_previous_only_binance_join(tmp_path):
    _write_compact_fixture(tmp_path / "compact")
    _write_binance_csv(
        tmp_path / "binance",
        [
            ("2026-05-01T00:00:00Z", 100.0),
            ("2026-05-01T00:01:00Z", 100.5),
            ("2026-05-01T00:02:00Z", 999.0),
        ],
    )

    result = builder.build(_args(tmp_path))
    row = result.frame.iloc[0]

    assert row["current_price"] == 100.5
    assert row["feature_ts"] == pd.Timestamp("2026-05-01T00:01:00Z")
    assert row["feature_ts"] <= row["decision_ts"]
    assert result.quality["decision_ts_feature_ts_violation_count"] == 0


def test_market_age_expiry_label_and_depth_formula(tmp_path):
    _write_compact_fixture(tmp_path / "compact")
    _write_binance_csv(tmp_path / "binance", [("2026-05-01T00:01:00Z", 100.5)])

    result = builder.build(_args(tmp_path))
    row = result.frame.iloc[0]

    assert row["market_age_sec"] == 90.0
    assert row["seconds_to_expiry"] == 210.0
    assert bool(row["label_above_beat"]) is True
    assert row["label_source"] == "chainlink"
    assert row["yes_depth_top1"] == pytest.approx(0.55 * 10.0)
    assert row["yes_depth_top3"] == pytest.approx(0.55 * 10.0 + 0.56 * 20.0 + 0.57 * 30.0)


def test_binance_synthetic_labels_and_schema(tmp_path):
    _write_binance_csv(
        tmp_path / "binance",
        [
            ("2026-01-01T00:00:00Z", 100.0),
            ("2026-01-01T00:01:00Z", 101.0),
            ("2026-01-01T00:02:00Z", 102.0),
            ("2026-01-01T00:03:00Z", 103.0),
            ("2026-01-01T00:04:00Z", 104.0),
            ("2026-01-01T00:05:00Z", 105.0),
        ],
    )
    args = _args(
        tmp_path,
        source_mode="binance_synthetic",
        compact_root=None,
        start_ts=pd.Timestamp("2026-01-01T00:00:00Z"),
        end_ts=pd.Timestamp("2026-01-01T00:05:00Z"),
    )

    result = builder.build(args)

    assert set(builder.SNAPSHOT_COLUMNS).issubset(result.frame.columns)
    first = result.frame.iloc[0]
    assert first["price_to_beat"] == 100.0
    assert first["settlement_price"] == 105.0
    assert bool(first["label_above_beat"]) is True
    assert first["label_source"] == "binance_proxy"
    assert pd.isna(first["yes_ask"])


def test_duplicate_compact_ticks_are_dropped_and_reported(tmp_path):
    _write_compact_fixture(tmp_path / "compact", duplicate=True)
    _write_binance_csv(tmp_path / "binance", [("2026-05-01T00:01:00Z", 100.5)])

    result = builder.build(_args(tmp_path))

    assert len(result.frame) == 1
    assert result.manifest["diagnostics"]["duplicate_side_tick_rows_dropped"] == 1
    assert result.quality["duplicate_key_count"] == 0


def test_output_artifacts_are_written(tmp_path):
    _write_compact_fixture(tmp_path / "compact")
    _write_binance_csv(tmp_path / "binance", [("2026-05-01T00:01:00Z", 100.5)])
    args = _args(tmp_path)
    result = builder.build(args)

    builder.write_outputs(result.frame, args.output_dir, result.manifest, result.quality, overwrite=True)

    assert (args.output_dir / "candidate_snapshots.parquet").exists()
    assert (args.output_dir / "output_schema.json").exists()
    assert (args.output_dir / "run_manifest.json").exists()
    assert (args.output_dir / "README.txt").exists()
    quality = json.loads((args.output_dir / "data_quality_summary.json").read_text(encoding="utf-8"))
    assert quality["row_count"] == 1


def test_missing_required_compact_input_fails_closed(tmp_path):
    (tmp_path / "compact").mkdir()

    with pytest.raises(FileNotFoundError):
        builder.build(_args(tmp_path))
