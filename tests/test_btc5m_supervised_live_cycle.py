from __future__ import annotations

import argparse
from pathlib import Path

import pytest

from scripts.run_btc5m_supervised_live_cycle import run_supervised_cycle, validate_supervised_env
from src.runtime.btc5m_live_ledger import LiveLedger


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
        "yes_i_understand_this_sends_transactions": False,
    }
    base.update(overrides)
    return argparse.Namespace(**base)


def test_refuses_unsafe_env():
    with pytest.raises(RuntimeError, match="unsafe_env"):
        validate_supervised_env(_env(BTC5M_BROWNIAN_PAPER_ONLY="true"), approval_checker=lambda env: True)


def test_refuses_continuous_live():
    with pytest.raises(RuntimeError, match="continuous_live_env_detected"):
        validate_supervised_env(_env(BTC5M_ALLOW_CONTINUOUS_LIVE="true"), approval_checker=lambda env: True)


def test_missing_approval_refuses():
    with pytest.raises(RuntimeError, match="missing_ctf_redeem_adapter_approval"):
        validate_supervised_env(_env(), approval_checker=lambda env: False)


def test_calls_order_reconcile_resolution_redeemer_steps(tmp_path: Path):
    ledger = LiveLedger(tmp_path / "ledger.db")
    calls = []

    def order_runner(args):
        calls.append("order")
        journal = args.journal_root / "2026-05-27" / "execution_events.jsonl"
        journal.parent.mkdir(parents=True)
        journal.write_text(
            '{"event_type":"order_intent_created","policy_id":"brownian_no_hmm_conservative_v1","market_id":"m1","condition_id":"c1","token_id":"tok","selected_side":"YES","client_order_id":"cid","idempotency_key":"idem","limit_price":0.4,"stake_usd":5}\\n',
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
        journal.write_text('{"event_type":"order_unknown_after_submit"}\n', encoding="utf-8")
        return {"returncode": 0}

    summary = run_supervised_cycle(args=_args(tmp_path), ledger=ledger, log_dir=tmp_path / "log", order_runner=order_runner)

    assert summary["hard_stop_reason"] == "unknown_order_state_after_submit"
