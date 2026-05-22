from pathlib import Path
from math import log as np_log

import pandas as pd
import pytest

from scripts import run_macro_context_edge_attribution as macro
from scripts import run_hmm_regime_veto_attribution as veto


def price_frame() -> pd.DataFrame:
    ts = pd.date_range("2026-05-01T00:00:00Z", periods=1500, freq="min")
    close = [100.0 + i for i in range(len(ts))]
    return pd.DataFrame({"timestamp": ts, "close": close})


def attached_rows_two_models() -> pd.DataFrame:
    trades = veto.normalize_trades(
        pd.DataFrame(
            {
                "entry_ts": pd.to_datetime(
                    [
                        "2026-05-01T00:30:30Z",
                        "2026-05-01T00:31:30Z",
                        "2026-05-02T00:30:30Z",
                        "2026-05-09T00:30:30Z",
                    ],
                    utc=True,
                ),
                "market_key": [1, 2, 3, 4],
                "model_name": ["pm"] * 4,
                "side": ["YES", "NO", "YES", "NO"],
                "stake_size": [1.0] * 4,
                "edge_threshold": [0.01] * 4,
                "entry_age_window": ["0_300"] * 4,
                "entry_ask": [0.40, 0.42, 0.44, 0.46],
                "gross_cost": [1.0] * 4,
                "gross_pnl": [1.0, -1.0, 1.0, -1.0],
                "chronological_slice": ["early", "early", "main", "fresh"],
                "entry_date": ["2026-05-01", "2026-05-01", "2026-05-02", "2026-05-09"],
                "winner_side": ["YES", "YES", "YES", "YES"],
                "win": [True, False, True, False],
            }
        )
    )
    parts = []
    for model, states in [("hmm_a", [3, 1, 3, 3]), ("hmm_b", [9, 9, 8, 8])]:
        part = trades.copy()
        part["hmm_model_id"] = model
        part["hmm_state"] = states
        part["hmm_pmax"] = [0.9, 0.8, 0.95, 0.7]
        parts.append(part)
    return pd.concat(parts, ignore_index=True)


def test_price_macro_features_are_trailing_not_future():
    features = macro.build_price_macro_features_from_prices(price_frame())
    row = features.iloc[30]
    assert row["timestamp"] == pd.Timestamp("2026-05-01T00:30:00Z")
    assert row["signed_return_30m"] == pytest.approx(np_log(130.0 / 100.0))
    assert row["signed_return_30m"] != pytest.approx(np_log(131.0 / 101.0))


def test_asof_attach_uses_previous_feature_timestamp_only():
    trades = attached_rows_two_models().iloc[[0]].copy()
    feats = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(["2026-05-01T00:30:00Z", "2026-05-01T00:31:00Z"], utc=True),
            "signed_return_30m": [1.0, 999.0],
        }
    )
    out, diag = macro.asof_attach_features(trades, feats, ["signed_return_30m"], "price")
    assert diag["price_coverage"] == pytest.approx(1.0)
    assert out["signed_return_30m"].iloc[0] == pytest.approx(1.0)


def test_bucket_edges_fit_on_train_only_and_apply_to_test():
    frame = attached_rows_two_models()
    frame["macro_x"] = [0.0, 10.0, 1000.0, 2000.0] * 2
    edges = macro.fit_bucket_edges(frame, ["macro_x"], ["early"], q=2)
    assert edges["macro_x"]["method"] == "quantile"
    assert edges["macro_x"]["edges"][1] == pytest.approx(5.0)
    out = macro.apply_buckets(frame, edges)
    # Main/fresh values are bucketed with early-fitted edges, not refit around test values.
    assert set(out[out["chronological_slice"].isin(["main", "fresh"])]["macro_x_bucket"].astype(str)) == {"q2"}


def test_allow_filters_use_correct_per_hmm_model_focus_universe():
    frame = attached_rows_two_models()
    frame["signed_return_30m_bucket"] = ["positive", "negative", "positive", "negative"] * 2
    focus = macro.focus_rows(frame, "hmm_a", 3)
    assert len(focus) == 3
    scan, validation = macro.allow_filter_scan(
        focus,
        ["signed_return_30m_bucket"],
        train_slices=["early"],
        test_slices=["main", "fresh"],
        min_trades=1,
        min_markets=1,
    )
    base = scan[scan["filter_name"].eq("focus_state_only")].iloc[0]
    assert base["trades"] == 3
    assert base["pnl"] == pytest.approx(1.0)
    val_base = validation[validation["filter_name"].eq("focus_state_only")].iloc[0]
    assert val_base["train_trades"] == 1
    assert val_base["test_trades"] == 2


def test_focus_state_filtering_and_bad_state_rows():
    frame = attached_rows_two_models()
    focus = macro.focus_rows(frame, "hmm_a", 3)
    assert set(focus["market_id"]) == {1, 3, 4}
    bad = macro.bad_state_rows(frame, [("hmm_a", 1), ("hmm_b", 8)])
    assert set(bad["bad_state_key"]) == {"hmm_a:1", "hmm_b:8"}
    assert len(bad) == 3


def test_missing_feature_coverage_fails_loudly():
    frame = attached_rows_two_models()
    frame["signed_return_30m"] = pd.NA
    with pytest.raises(ValueError, match="macro feature coverage dropped too many rows"):
        macro.ensure_feature_coverage(frame, ["signed_return_30m"], max_missing_share=0.1)


def test_no_future_timestamp_can_influence_trade_row(tmp_path: Path):
    prices = pd.DataFrame(
        {
            "open_time": [pd.Timestamp("2026-05-01T00:00:00Z").value // 10**6, pd.Timestamp("2026-05-01T00:01:00Z").value // 10**6],
            "close": [100.0, 200.0],
        }
    )
    p = tmp_path / "BTCUSDT-1m-test.csv"
    prices.to_csv(p, index=False)
    feats = macro.build_price_macro_features(p)
    trade = attached_rows_two_models().iloc[[0]].copy()
    trade["timestamp"] = pd.Timestamp("2026-05-01T00:00:30Z")
    out, _ = macro.asof_attach_features(trade, feats, ["close"], "price")
    assert out["close"].iloc[0] == pytest.approx(100.0)
