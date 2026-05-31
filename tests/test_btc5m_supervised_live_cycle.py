from __future__ import annotations

import argparse
from pathlib import Path

import pytest

from scripts.run_btc5m_supervised_live_cycle import (
    brownian_runtime_config_snapshot,
    build_parser,
    build_supervised_resolution_source,
    run_order_subprocess,
    run_supervised_cycle,
    validate_supervised_env,
)
from src.runtime.btc5m_resolution_source import GammaCtfResolutionSource, UnavailableResolutionSource
from src.runtime.btc5m_live_ledger import LiveLedger
from src.time_utils import isoformat_utc, utc_now


def _env(**overrides):
    env = {
        "BTC5M_STRATEGY_ID": "brownian_no_hmm_conservative_v1",
        "BTC5M_BROWNIAN_PAPER_ONLY": "false",
        "BTC5M_BROWNIAN_LIVE_ENABLED": "true",
        "BTC5M_EXECUTION_MODE": "live",
        "BTC5M_LIVE_ONE_SHOT": "true",
        "BTC5M_ALLOW_CONTINUOUS_LIVE": "false",
        "POLYGON_RPC": "https://rpc",
        "POLY_WALLET_PRIVATE_KEY": "secret",
        "BTC5M_EXPECTED_WALLET_ADDRESS": "0xWallet",
        "POLY_API_KEY": "key",
        "POLY_API_SECRET": "secret",
        "POLY_API_PASSPHRASE": "pass",
    }
    env.update(overrides)
    return env


def _args(tmp_path: Path, **overrides):
    base = {
        "env_file": tmp_path / ".env",
        "max_runtime_sec": 0.01,
        "order_cycle_runtime_sec": 0.01,
        "poll_interval_sec": 1,
        "redeem_interval_sec": 0,
        "resolution_interval_sec": 0,
        "ledger_db": tmp_path / "ledger.db",
        "journal_root": tmp_path / "journal",
        "max_live_order_attempts": 12,
        "max_filled_orders": 12,
        "max_redemptions": 12,
        "dry_run_redemptions": True,
        "allow_unavailable_resolution_source": False,
        "resolution_source": "gamma_ctf",
        "redeemer_min_retry_interval_sec": 3600,
        "redeemer_max_failures": 3,
        "yes_i_understand_this_sends_transactions": False,
    }
    base.update(overrides)
    return argparse.Namespace(**base)


def test_refuses_unsafe_env():
    with pytest.raises(RuntimeError, match="unsafe_env"):
        validate_supervised_env(_env(BTC5M_BROWNIAN_PAPER_ONLY="true"), approval_checker=lambda env: True)


def test_default_env_file_is_complete_dotenv():
    args = build_parser().parse_args([])
    assert args.env_file == Path(".env")


def test_complete_dotenv_style_env_passes_validation():
    validate_supervised_env(_env(), approval_checker=lambda env: True)


def test_selected_env_missing_polygon_rpc_fails_clearly():
    with pytest.raises(RuntimeError, match="missing_env:POLYGON_RPC"):
        validate_supervised_env(_env(POLYGON_RPC=""), approval_checker=lambda env: True)


def test_brownian_runtime_snapshot_uses_min_order_notional_over_alias():
    snap = brownian_runtime_config_snapshot(
        _env(
            BTC5M_BROWNIAN_BANKROLL_USD="2000",
            BTC5M_BROWNIAN_MIN_ORDER_NOTIONAL="2",
            BTC5M_BROWNIAN_MIN_MARKET_BUY_NOTIONAL_USD="5",
            BTC5M_BROWNIAN_MAX_STAKE_FRACTION="0.0025",
        )
    )

    assert snap["min_order_notional"] == 2.0
    assert snap["deprecated_min_market_buy_notional_usd"] == 2.0
    assert snap["small_wallet_threshold"] == pytest.approx(800.0)
    assert snap["deprecated_env_warnings"] == [
        "BTC5M_BROWNIAN_MIN_MARKET_BUY_NOTIONAL_USD is deprecated; use BTC5M_BROWNIAN_MIN_ORDER_NOTIONAL"
    ]


def test_brownian_runtime_snapshot_reports_effective_sizing_controls():
    snap = brownian_runtime_config_snapshot(
        _env(
            BTC5M_BROWNIAN_BANKROLL_USD="100",
            BTC5M_BROWNIAN_MIN_ORDER_NOTIONAL="1",
            BTC5M_BROWNIAN_MAX_STAKE_FRACTION="0.0025",
            BTC5M_BROWNIAN_KELLY_MULTIPLIER="0.025",
        )
    )

    assert snap["min_order_notional"] == 1.0
    assert snap["normal_max_stake_fraction"] == 0.0025
    assert snap["kelly_multiplier"] == 0.025
    assert snap["bankroll_usd"] == 100.0
    assert snap["max_stake_for_bankroll"] == pytest.approx(0.25)
    assert "BTC5M_BROWNIAN_MIN_MARKET_BUY_NOTIONAL_USD" not in snap["raw_env"]


def test_refuses_continuous_live():
    with pytest.raises(RuntimeError, match="continuous_live_env_detected"):
        validate_supervised_env(_env(BTC5M_ALLOW_CONTINUOUS_LIVE="true"), approval_checker=lambda env: True)


def test_refuses_resolution_fail_open_live():
    with pytest.raises(RuntimeError, match="resolution_fail_open_forbidden_live"):
        validate_supervised_env(_env(BTC5M_RESOLUTION_FAIL_OPEN="true"), approval_checker=lambda env: True)


def test_refuses_resolution_without_onchain_confirmation_live():
    with pytest.raises(RuntimeError, match="resolution_requires_onchain_confirmation_live"):
        validate_supervised_env(_env(BTC5M_RESOLUTION_REQUIRE_ONCHAIN_CONFIRMATION="false"), approval_checker=lambda env: True)


def test_missing_approval_refuses():
    with pytest.raises(RuntimeError, match="missing_ctf_redeem_adapter_approval"):
        validate_supervised_env(_env(), approval_checker=lambda env: False)


def test_supervised_harness_refuses_unavailable_resolution_source_without_allow():
    with pytest.raises(RuntimeError, match="resolution_source_unavailable"):
        build_supervised_resolution_source(_args(Path("/tmp"), resolution_source="unavailable"), _env(BTC5M_RESOLUTION_SOURCE="unavailable"))


def test_unavailable_resolution_with_real_redemptions_refused():
    args = _args(Path("/tmp"), resolution_source="unavailable", allow_unavailable_resolution_source=True, dry_run_redemptions=False, yes_i_understand_this_sends_transactions=True)
    with pytest.raises(RuntimeError, match="unavailable_resolution_source_requires_dry_run_redemptions"):
        build_supervised_resolution_source(args, _env(BTC5M_RESOLUTION_SOURCE="unavailable"))


def test_unavailable_resolution_allowed_only_for_dry_run_redemptions():
    args = _args(Path("/tmp"), resolution_source="unavailable", allow_unavailable_resolution_source=True, dry_run_redemptions=True)
    source = build_supervised_resolution_source(args, _env(BTC5M_RESOLUTION_SOURCE="unavailable"))
    assert isinstance(source, UnavailableResolutionSource)


def test_supervised_harness_constructs_gamma_ctf_source(monkeypatch):
    class FakeSource:
        pass

    monkeypatch.setattr("scripts.run_btc5m_supervised_live_cycle.build_resolution_source", lambda env, allow_unavailable=False: FakeSource())
    source = build_supervised_resolution_source(_args(Path("/tmp"), resolution_source="gamma_ctf"), _env())
    assert isinstance(source, FakeSource)


def test_calls_order_reconcile_resolution_redeemer_steps(tmp_path: Path):
    ledger = LiveLedger(tmp_path / "ledger.db")
    calls = []

    def order_runner(args):
        calls.append("order")
        journal = args.journal_root / "2026-05-27" / "execution_events.jsonl"
        journal.parent.mkdir(parents=True)
        ts = isoformat_utc(utc_now())
        journal.write_text(
            '{"event_type":"order_intent_created","execution_ts":"' + ts + '","policy_id":"brownian_no_hmm_conservative_v1","market_id":"m1","condition_id":"c1","token_id":"tok","selected_side":"YES","client_order_id":"cid","idempotency_key":"idem","limit_price":0.4,"stake_usd":5}\\n',
            encoding="utf-8",
        )
        return {"returncode": 0}

    args = _args(tmp_path)
    summary = run_supervised_cycle(args=args, ledger=ledger, log_dir=tmp_path / "log", order_runner=order_runner)

    assert calls == ["order"]
    assert summary["hard_stop_reason"] is None
    assert (tmp_path / "log" / "supervised_cycle.jsonl").exists()


def test_stops_on_unknown_order(tmp_path: Path):
    ledger = LiveLedger(tmp_path / "ledger.db")

    def order_runner(args):
        journal = args.journal_root / "2026-05-27" / "execution_events.jsonl"
        journal.parent.mkdir(parents=True)
        journal.write_text('{"event_type":"order_unknown_after_submit","execution_ts":"' + isoformat_utc(utc_now()) + '"}\n', encoding="utf-8")
        return {"returncode": 0}

    summary = run_supervised_cycle(args=_args(tmp_path), ledger=ledger, log_dir=tmp_path / "log", order_runner=order_runner)

    assert summary["hard_stop_reason"] == "unknown_order_state_after_submit"


def test_supervised_summary_ignores_old_journal_events(tmp_path: Path):
    ledger = LiveLedger(tmp_path / "ledger.db")
    journal = tmp_path / "journal" / "2026-05-27" / "execution_events.jsonl"
    journal.parent.mkdir(parents=True)
    journal.write_text(
        "\n".join(
            '{"event_type":"order_intent_created","execution_ts":"2026-01-01T00:00:00+00:00"}'
            for _ in range(20)
        )
        + "\n",
        encoding="utf-8",
    )

    summary = run_supervised_cycle(
        args=_args(tmp_path, max_live_order_attempts=1),
        ledger=ledger,
        log_dir=tmp_path / "log",
        order_runner=lambda args: {"returncode": 0},
    )

    assert summary["live_order_attempts"] == 0
    assert summary["hard_stop_reason"] is None
    assert summary["log_dir"] == str(tmp_path / "log")


def test_order_subprocess_receives_selected_env_file(monkeypatch, tmp_path: Path):
    captured = {}

    def fake_run(cmd, cwd, text, capture_output, check, env):
        captured["cmd"] = cmd
        captured["env"] = env

        class Result:
            returncode = 0
            stdout = "{}"
            stderr = ""

        return Result()

    monkeypatch.setattr("scripts.run_btc5m_supervised_live_cycle.subprocess.run", fake_run)
    args = _args(tmp_path)
    result = run_order_subprocess(args)

    assert result["returncode"] == 0
    assert captured["env"]["BTC5M_ENV_FILE"] == str(args.env_file)
    assert "--env-file" in captured["cmd"]
    assert str(args.env_file) in captured["cmd"]
