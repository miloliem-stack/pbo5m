import json

import pandas as pd
import pytest

from scripts import research_quiet_regime_pair_replay as replay


def test_post_confirmation_quiet_market_detection():
    mask = replay.post_confirmation_quiet_mask(pd.Series([1, 5, 5, 5, 2, 5, 5]), quiet_state=5)
    assert mask.tolist() == [False, False, True, True, False, False, True]


def _snapshots():
    return pd.DataFrame(
        {
            "quote_ts": pd.to_datetime(
                [
                    "2026-04-17T13:00:05Z",
                    "2026-04-17T13:00:12Z",
                    "2026-04-17T13:00:20Z",
                ],
                utc=True,
            ),
            "market_start_time": pd.to_datetime(["2026-04-17T13:00:00Z"] * 3, utc=True),
            "market_end_time": pd.to_datetime(["2026-04-17T13:05:00Z"] * 3, utc=True),
            "market_id": ["m1"] * 3,
            "slug": ["btc-updown-5m"] * 3,
            "quote_capture_ok": [True] * 3,
            "quote_capture_status": ["ok"] * 3,
            "yes_bid": [0.43, 0.44, 0.45],
            "yes_ask": [0.49, 0.46, 0.44],
            "no_bid": [0.42, 0.43, 0.44],
            "no_ask": [0.50, 0.48, 0.45],
            "yes_mid": [0.46, 0.45, 0.445],
            "no_mid": [0.46, 0.455, 0.445],
            "yes_age_seconds": [0.1, 0.1, 0.1],
            "no_age_seconds": [0.1, 0.1, 0.1],
        }
    )


def test_pair_target_touch_detection_and_timeout():
    metrics = replay.target_touch_metrics(_snapshots(), target=0.45, timeout_windows_sec=[5, 15])
    assert metrics["target_0.45_yes_touched"] is True
    assert metrics["target_0.45_no_touched"] is True
    assert metrics["target_0.45_both_touched"] is True
    assert metrics["target_0.45_seconds_between_first_and_second_leg_touch"] == 0.0
    assert metrics["target_0.45_both_touched_within_5s"] is True


def test_independent_lows_at_different_times_are_counted_separately():
    snapshots = replay.add_snapshot_flags(_snapshots().iloc[:2].copy())
    diagnostics = replay.independent_leg_target_diagnostics(snapshots, target=0.46, suffix="early")
    assert diagnostics["target_0.46_yes_ask_ever_lte_target_early"] is True
    assert diagnostics["target_0.46_no_ask_ever_lte_target_early"] is False
    assert diagnostics["target_0.46_both_asks_ever_lte_target_independently_early"] is False

    snapshots = replay.add_snapshot_flags(_snapshots().copy())
    diagnostics = replay.independent_leg_target_diagnostics(snapshots, target=0.46, suffix="early")
    assert diagnostics["target_0.46_both_asks_ever_lte_target_independently_early"] is True
    assert diagnostics["target_0.46_both_asks_lte_target_same_snapshot_early"] is True


def test_same_snapshot_lows_are_counted_separately_from_independent_lows():
    snapshots = _snapshots().copy()
    snapshots.loc[0, "yes_ask"] = 0.44
    snapshots.loc[0, "no_ask"] = 0.44
    flagged = replay.add_snapshot_flags(snapshots)
    diagnostics = replay.independent_leg_target_diagnostics(flagged, target=0.45, suffix="early")
    assert diagnostics["target_0.45_both_asks_ever_lte_target_independently_early"] is True
    assert diagnostics["target_0.45_both_asks_lte_target_same_snapshot_early"] is True


def test_early_and_full_window_counts_differ_correctly():
    event = pd.Series(
        {
            "slug": "btc-updown-5m",
            "market_id": "m1",
            "market_start_time": pd.Timestamp("2026-04-17T13:00:00Z"),
            "market_end_time": pd.Timestamp("2026-04-17T13:05:00Z"),
            "assigned_state": 5,
            "is_quiet_market": True,
            "is_post_confirmation_quiet_market": True,
            "binance_label": "UP",
        }
    )
    early = _snapshots().iloc[:1].copy()
    full = _snapshots().copy()
    row = replay.market_replay_row(
        event,
        early,
        target_levels=[0.45],
        timeout_windows_sec=[5],
        stale_quote_sec=10.0,
        full_window_snapshots=full,
    )
    assert row["target_0.45_both_asks_ever_lte_target_independently_early"] is False
    assert row["target_0.45_both_asks_ever_lte_target_independently_full_window"] is True
    assert row["quote_count"] == 1
    assert row["full_window_quote_count"] == 3


def test_price_scale_normalization_works_and_warnings_fire():
    assert replay.normalize_quote_price("46") == pytest.approx(0.46)
    assert replay.normalize_quote_price("0.46") == pytest.approx(0.46)
    quotes = pd.DataFrame({"yes_bid": [2.0], "yes_ask": [2.5], "no_bid": [2.0], "no_ask": [2.5]})
    warnings = replay.price_scale_warnings(quotes, pd.DataFrame())
    assert any("above 1.0" in warning for warning in warnings)


def test_token_side_mapping_does_not_silently_swap_yes_no():
    payload = {
        "ts": "2026-04-17T13:00:00Z",
        "record_type": "quote_snapshot",
        "market_id": "m1",
        "slug": "btc-updown-5m",
        "market_start_time": "2026-04-17T13:00:00Z",
        "market_end_time": "2026-04-17T13:05:00Z",
        "token_yes": "yes-token",
        "token_no": "no-token",
        "yes": {"best_bid": "0.41", "best_ask": "0.46", "mid": "0.435", "age_seconds": 0.1},
        "no": {"best_bid": "0.52", "best_ask": "0.57", "mid": "0.545", "age_seconds": 0.1},
        "raw_payload_fragment": {
            "yes_raw": {"book": {"last_trade_price": "0.45"}},
            "no_raw": {"book": {"last_trade_price": "0.58"}},
        },
    }
    row = replay.flatten_quote_snapshot(payload, "quotes.jsonl")
    assert row["token_yes"] == "yes-token"
    assert row["token_no"] == "no-token"
    assert row["yes_ask"] == 0.46
    assert row["no_ask"] == 0.57
    assert row["yes_last_trade"] == 0.45
    assert row["no_last_trade"] == 0.58


def test_one_leg_only_classification_and_orphan_toxicity_proxy():
    snapshots = _snapshots().iloc[:2].copy()
    event = pd.Series(
        {
            "slug": "btc-updown-5m",
            "market_id": "m1",
            "market_start_time": pd.Timestamp("2026-04-17T13:00:00Z"),
            "market_end_time": pd.Timestamp("2026-04-17T13:05:00Z"),
            "assigned_state": 5,
            "is_quiet_market": True,
            "is_post_confirmation_quiet_market": True,
            "tiny_move_near_boundary": True,
            "label_agreement": True,
            "binance_label": "DOWN",
            "chainlink_label": "DOWN",
        }
    )
    row = replay.market_replay_row(
        event,
        snapshots,
        target_levels=[0.46],
        timeout_windows_sec=[5],
        stale_quote_sec=10.0,
    )
    assert row["target_0.46_only_yes_touched"] is True
    assert row["target_0.46_only_no_touched"] is False
    assert row["target_0.46_one_sided_losing_touch"] is True


def test_grouped_summary_generation():
    frame = pd.DataFrame(
        {
            "is_quiet_market": [True, True, False],
            "is_post_confirmation_quiet_market": [False, True, False],
            "tiny_move_near_boundary": [True, False, True],
            "label_agreement": [True, True, False],
            "quote_coverage": [True, True, False],
            "one_sided_quote": [False, False, True],
            "wide_quote": [False, True, False],
            "target_0.45_both_touched": [True, False, False],
            "target_0.45_only_yes_touched": [False, True, False],
            "target_0.45_only_no_touched": [False, False, True],
            "target_0.45_one_sided_losing_touch": [False, True, False],
            "target_0.45_both_touched_within_5s": [True, False, False],
        }
    )
    summary = replay.grouped_summary(frame, [0.45], [5])
    assert summary["all_markets"]["market_count"] == 3
    assert summary["quiet_markets"]["targets"]["0.45"]["one_leg_only_rate"] == pytest.approx(0.5)
    assert summary["post_confirmation_quiet_markets"]["targets"]["0.45"]["orphan_toxicity_proxy"] == 1.0


def test_missing_quote_side_is_graceful():
    snapshots = _snapshots()
    snapshots["no_ask"] = pd.NA
    event = pd.Series(
        {
            "slug": "btc-updown-5m",
            "market_id": "m1",
            "market_start_time": pd.Timestamp("2026-04-17T13:00:00Z"),
            "market_end_time": pd.Timestamp("2026-04-17T13:05:00Z"),
            "assigned_state": 5,
            "is_quiet_market": True,
            "is_post_confirmation_quiet_market": False,
            "tiny_move_near_boundary": False,
            "label_agreement": True,
            "binance_label": "UP",
            "chainlink_label": "UP",
        }
    )
    row = replay.market_replay_row(event, snapshots, target_levels=[0.45], timeout_windows_sec=[5], stale_quote_sec=10.0)
    assert row["target_0.45_yes_touched"] is True
    assert row["target_0.45_no_touched"] is False
    assert row["one_sided_quote"] is False
    assert row["min_no_ask"] is None


def test_output_files_written(tmp_path):
    replay_frame = pd.DataFrame(
        {
            "quote_coverage": [True],
            "one_sided_quote": [False],
            "wide_quote": [False],
            "is_quiet_market": [True],
            "is_post_confirmation_quiet_market": [True],
            "tiny_move_near_boundary": [False],
            "label_agreement": [True],
            "target_0.45_both_touched": [True],
            "target_0.45_only_yes_touched": [False],
            "target_0.45_only_no_touched": [False],
            "target_0.45_one_sided_losing_touch": [False],
            "target_0.45_both_touched_within_5s": [True],
        }
    )
    summary = replay.build_summary(
        replay=replay_frame,
        recorder_event_rows=1,
        recorder_quote_rows=2,
        joined_events=1,
        quiet_count=1,
        post_confirmation_count=1,
        target_levels=[0.45],
        timeout_windows_sec=[5],
        warnings=["offline only"],
    )
    paths = replay.write_outputs(tmp_path, replay_frame, summary)
    assert (tmp_path / "quiet_pair_market_replay.csv").exists()
    assert (tmp_path / "quiet_pair_summary.json").exists()
    assert (tmp_path / "quiet_pair_readme_summary.txt").exists()
    payload = json.loads((tmp_path / "quiet_pair_summary.json").read_text(encoding="utf-8"))
    assert payload["recorder_event_rows"] == 1
    assert payload["joined_recorder_events"] == 1
    assert paths["quiet_pair_market_replay"].endswith("quiet_pair_market_replay.csv")


def test_empty_replay_summary_generation():
    frame = replay.empty_replay_frame([0.45], [5])
    summary = replay.build_summary(
        replay=frame,
        recorder_event_rows=0,
        recorder_quote_rows=0,
        joined_events=0,
        quiet_count=0,
        post_confirmation_count=0,
        target_levels=[0.45],
        timeout_windows_sec=[5],
        warnings=["no join"],
    )
    assert summary["joined_recorder_events"] == 0
    assert summary["grouped_metrics"]["all_markets"]["market_count"] == 0
    assert summary["grouped_metrics"]["all_markets"]["targets"]["0.45"]["both_touch_rate"] is None
