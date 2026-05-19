import json

import pandas as pd

from scripts import build_binance_btc5m_research_events


def test_load_binance_kline_csv_and_normalize_utc(tmp_path):
    path = tmp_path / "BTCUSDT-1m-sample.csv"
    path.write_text(
        "\n".join(
            [
                "1577836800000,100,101,99,100.5,10,1577836859999,0,0,0,0,0",
                "1577836860000,100.5,102,100,101,11,1577836919999,0,0,0,0,0",
            ]
        ),
        encoding="utf-8",
    )
    loaded = build_binance_btc5m_research_events.load_binance_1m_klines([tmp_path])
    assert len(loaded.frame) == 2
    assert "UTC" in str(loaded.frame["event_time"].dtype)
    assert loaded.frame.loc[0, "close"] == 100.5


def test_load_binance_kline_microsecond_timestamps(tmp_path):
    path = tmp_path / "BTCUSDT-1m-us.csv"
    path.write_text(
        "\n".join(
            [
                "1735689600000000,100,101,99,100.5,10,1735689659999999,0,0,0,0,0",
                "1735689660000000,100.5,102,100,101,11,1735689719999999,0,0,0,0,0",
            ]
        ),
        encoding="utf-8",
    )
    loaded = build_binance_btc5m_research_events.load_binance_1m_klines([tmp_path])
    assert loaded.frame.loc[0, "event_time"] == pd.Timestamp("2025-01-01T00:00:00Z")


def test_dedup_and_gap_detection(tmp_path):
    path = tmp_path / "BTCUSDT-1m-gap.csv"
    path.write_text(
        "\n".join(
            [
                "1577836800000,100,101,99,100.5,10,1577836859999,0,0,0,0,0",
                "1577836860000,100.5,102,100,101,11,1577836919999,0,0,0,0,0",
                "1577836860000,100.5,102,100,101.1,11,1577836919999,0,0,0,0,0",
                "1577836980000,101.1,103,101,102,9,1577837039999,0,0,0,0,0",
            ]
        ),
        encoding="utf-8",
    )
    loaded = build_binance_btc5m_research_events.load_binance_1m_klines([tmp_path])
    assert loaded.duplicate_count == 1
    assert len(loaded.gap_summary) == 1
    assert loaded.gap_summary[0]["missing_minutes"] == 1
    assert loaded.frame.loc[1, "close"] == 101.1


def test_event_construction_and_labels():
    times = pd.date_range("2020-01-01T00:00:00Z", periods=7, freq="1min", tz="UTC")
    klines = pd.DataFrame(
        {
            "event_time": times,
            "close_time": times + pd.Timedelta(minutes=1) - pd.Timedelta(milliseconds=1),
            "open": [100, 101, 102, 103, 104, 105, 106],
            "high": [100, 101, 102, 103, 104, 105, 106],
            "low": [100, 101, 102, 103, 104, 105, 106],
            "close": [100, 101, 102, 103, 104, 105, 106],
            "volume": [1, 1, 1, 1, 1, 1, 1],
            "source_file": ["synthetic.csv"] * 7,
        }
    )
    events = build_binance_btc5m_research_events.build_binance_btc5m_events(klines, tiny_move_threshold=0.5)
    assert len(events) == 7
    first = events.iloc[0]
    assert first["reference_price"] == 100
    assert first["settlement_price"] == 105
    assert first["binance_label"] == "UP"
    assert bool(first["tiny_move_near_boundary"]) is False


def test_manifest_contains_assumptions(tmp_path):
    klines = pd.DataFrame(
        {
            "event_time": pd.date_range("2020-01-01T00:00:00Z", periods=6, freq="1min", tz="UTC"),
            "close_time": pd.date_range("2020-01-01T00:00:59Z", periods=6, freq="1min", tz="UTC"),
            "open": [1, 1, 1, 1, 1, 1],
            "high": [1, 1, 1, 1, 1, 1],
            "low": [1, 1, 1, 1, 1, 1],
            "close": [1, 2, 3, 4, 5, 6],
            "volume": [1, 1, 1, 1, 1, 1],
            "source_file": ["synthetic.csv"] * 6,
        }
    )
    events = build_binance_btc5m_research_events.build_binance_btc5m_events(klines)
    loaded = build_binance_btc5m_research_events.LoadedKlines(
        rows_loaded=6,
        rows_after_dedup=6,
        duplicate_count=0,
        files=["synthetic.csv"],
        frame=klines,
        gap_summary=[],
    )
    manifest = build_binance_btc5m_research_events.build_manifest(
        loaded=loaded,
        events=events,
        input_roots=[tmp_path],
        tiny_move_threshold=50.0,
        output_path=tmp_path / "events.csv",
    )
    assert manifest["event_count"] == 6
    assert manifest["assumptions"]["reference_price"].startswith("Uses the 1m candle close")
    assert "binance_label" in manifest["schema"]
    json.dumps(manifest)
