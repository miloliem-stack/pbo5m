from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd
import pytest

from scripts import build_btc5m_opportunity_tape as tape_builder


def _write_compact(root: Path) -> None:
    windows = pd.DataFrame(
        [
            {
                "market_key": 1,
                "market_id": "m1",
                "condition_id": "c1",
                "slug": "btc-updown-5m-1",
                "yes_token_id": "yes1",
                "no_token_id": "no1",
                "market_start_ts": pd.Timestamp("2026-05-01T00:00:00Z"),
                "market_end_ts": pd.Timestamp("2026-05-01T00:05:00Z"),
                "reference_price": 100.0,
                "winner_side": "YES",
                "chainlink_reference_quality": "ok",
                "chainlink_close_quality": "ok",
            },
            {
                "market_key": 2,
                "market_id": "m2",
                "condition_id": "c2",
                "slug": "btc-updown-5m-2",
                "yes_token_id": "yes2",
                "no_token_id": "no2",
                "market_start_ts": pd.Timestamp("2026-05-01T00:05:00Z"),
                "market_end_ts": pd.Timestamp("2026-05-01T00:10:00Z"),
                "reference_price": 101.0,
                "winner_side": "NO",
                "chainlink_reference_quality": "ok",
                "chainlink_close_quality": "ok",
            },
        ]
    )
    ticks = []
    for ts, age in [
        ("2026-05-01T00:01:00Z", 60.0),
        ("2026-05-01T00:02:00Z", 120.0),
        ("2026-05-01T00:03:00Z", 180.0),
    ]:
        for side, bid, ask in [("YES", 0.39, 0.40), ("NO", 0.59, 0.60)]:
            row = {
                "market_key": 1,
                "ts": pd.Timestamp(ts),
                "side": side,
                "source": "fixture",
                "market_age_sec": age,
                "seconds_to_end": 300.0 - age,
                "mid": (bid + ask) / 2,
                "spread": ask - bid,
                "is_crossed": False,
                "is_valid_topbook": True,
                "bid_px_1": bid,
                "bid_sz_1": 3.0,
            }
            for i in range(1, 11):
                row[f"ask_px_{i}"] = ask + (i - 1) * 0.01
                row[f"ask_sz_{i}"] = float(i)
                row[f"bid_px_{i}"] = bid - (i - 1) * 0.01
                row[f"bid_sz_{i}"] = float(i)
            ticks.append(row)
    for side, bid, ask in [("YES", 0.50, 0.51), ("NO", 0.48, 0.49)]:
        row = {
            "market_key": 2,
            "ts": pd.Timestamp("2026-05-01T00:06:00Z"),
            "side": side,
            "source": "fixture",
            "market_age_sec": 60.0,
            "seconds_to_end": 240.0,
            "mid": (bid + ask) / 2,
            "spread": ask - bid,
            "is_crossed": False,
            "is_valid_topbook": True,
            "bid_px_1": bid,
            "bid_sz_1": 1.0,
            "ask_px_1": ask,
            "ask_sz_1": 1.0,
        }
        ticks.append(row)
    root.mkdir(parents=True, exist_ok=True)
    windows.to_parquet(root / "market_windows.parquet", index=False)
    pd.DataFrame(ticks).to_parquet(root / "book_ticks.parquet", index=False)


def _write_predictions(root: Path, include_c2: bool = True) -> None:
    rows = [
        {"timestamp": "2026-05-01T00:00:30Z", "market_window_start": "2026-05-01T00:00:00Z", "market_window_end": "2026-05-01T00:05:00Z", "model_id": "brownian_zero_drift__rv30", "p_up": 0.20, "rv_30m": 0.01},
        {"timestamp": "2026-05-01T00:01:30Z", "market_window_start": "2026-05-01T00:00:00Z", "market_window_end": "2026-05-01T00:05:00Z", "model_id": "brownian_zero_drift__rv30", "p_up": 0.40, "rv_30m": 0.01},
        {"timestamp": "2026-05-01T00:02:30Z", "market_window_start": "2026-05-01T00:00:00Z", "market_window_end": "2026-05-01T00:05:00Z", "model_id": "brownian_zero_drift__rv30", "p_up": 0.70, "rv_30m": 0.01},
    ]
    if include_c2:
        rows.append({"timestamp": "2026-05-01T00:05:30Z", "market_window_start": "2026-05-01T00:05:00Z", "market_window_end": "2026-05-01T00:10:00Z", "model_id": "brownian_zero_drift__rv30", "p_up": 0.52, "rv_30m": 0.01})
    root.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_parquet(root / "predictions.parquet", index=False)


def _write_binance(root: Path) -> None:
    out = root / "binance-btc1m"
    out.mkdir(parents=True)
    rows = []
    for i in range(80):
        ts = pd.Timestamp("2026-04-30T23:00:00Z") + pd.Timedelta(minutes=i)
        rows.append([int(ts.timestamp() * 1_000_000), 0, 0, 0, 100.0 + i * 0.1])
    pd.DataFrame(rows).to_csv(out / "BTCUSDT-1m-fixture.csv", index=False, header=False)


def _args(tmp_path: Path, *, include_c2_predictions: bool = True) -> argparse.Namespace:
    compact = tmp_path / "compact"
    preds = tmp_path / "preds"
    data = tmp_path / "data"
    _write_compact(compact)
    _write_predictions(preds, include_c2=include_c2_predictions)
    _write_binance(data)
    return argparse.Namespace(
        compact_root=compact,
        predictions_root=preds,
        model_id="brownian_zero_drift__rv30",
        out=tmp_path / "out" / "opportunity_tape.parquet",
        data_root=data,
        start_ts="2026-05-01T00:00:00Z",
        end_ts="2026-05-01T00:07:00Z",
        valid_topbook_only=True,
        top_n_levels=10,
        fail_on_missing_predictions=True,
    )


def test_tape_does_not_prefilter_edge_and_pnl_formula_is_correct(tmp_path: Path):
    tape, _, _ = tape_builder.build_tape(_args(tmp_path))

    yes = tape[tape["side"].eq("YES")]
    assert yes["raw_edge"].lt(0).any()
    assert yes["raw_edge"].abs().min() == pytest.approx(0.0)
    assert yes["raw_edge"].gt(0).any()
    row = yes[yes["ts"].eq(pd.Timestamp("2026-05-01T00:03:00Z"))].iloc[0]
    assert row["won_if_bought"] == 1.0
    assert row["realized_pnl_per_share"] == pytest.approx(1.0 - row["ask"])
    assert row["realized_roi_if_bought"] == pytest.approx((1.0 - row["ask"]) / row["ask"])


def test_top10_depth_is_usd_not_share_count(tmp_path: Path):
    tape, _, _ = tape_builder.build_tape(_args(tmp_path))
    row = tape[(tape["condition_id"].eq("c1")) & (tape["side"].eq("YES"))].iloc[0]
    ask = 0.40
    expected = sum((ask + (i - 1) * 0.01) * i for i in range(1, 11))
    assert row["side_top_depth_10_usd"] == pytest.approx(expected)


def test_prediction_coverage_gap_fails_loudly(tmp_path: Path):
    args = _args(tmp_path, include_c2_predictions=False)
    with pytest.raises(RuntimeError, match="prediction_coverage_incomplete"):
        tape_builder.build_tape(args)


def test_btc_context_uses_backward_only_join(tmp_path: Path):
    tape, _, _ = tape_builder.build_tape(_args(tmp_path))
    row = tape[tape["ts"].eq(pd.Timestamp("2026-05-01T00:01:00Z"))].iloc[0]
    assert row["btc_price_at_ts"] <= 106.1


def test_prediction_join_refuses_future_timestamp(monkeypatch, tmp_path: Path):
    compact = tmp_path / "compact"
    preds = tmp_path / "preds"
    _write_compact(compact)
    _write_predictions(preds)
    windows, ticks = tape_builder.load_compact(compact, True, None, None)
    tape = tape_builder.attach_window_metadata(ticks, windows, 10)
    pred = tape_builder.load_predictions(preds, "brownian_zero_drift__rv30")

    def fake_merge_asof(*args, **kwargs):
        left = args[0].copy()
        left["prediction_ts"] = left["ts"] + pd.Timedelta(seconds=1)
        left["_p_up"] = 0.5
        return left

    monkeypatch.setattr(tape_builder.pd, "merge_asof", fake_merge_asof)
    with pytest.raises(RuntimeError, match="future prediction join"):
        tape_builder.join_predictions(tape, windows, pred)
