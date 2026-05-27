from __future__ import annotations

import argparse
from pathlib import Path

from scripts.run_btc5m_redeemer import build_parser
from scripts.run_btc5m_supervised_live_cycle import run_supervised_cycle
from src.runtime.btc5m_live_ledger import LiveLedger


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


def test_redeemer_min_retry_interval_default_from_env(monkeypatch):
    monkeypatch.setenv("BTC5M_REDEEMER_MIN_RETRY_INTERVAL_SEC", "123")
    args = build_parser().parse_args([])
    assert args.min_retry_interval_sec == 123


def test_redeemer_max_failures_default_from_env(monkeypatch):
    monkeypatch.setenv("BTC5M_REDEEMER_MAX_FAILURES", "7")
    args = build_parser().parse_args([])
    assert args.max_failures == 7


def test_redeemer_cli_overrides_env(monkeypatch):
    monkeypatch.setenv("BTC5M_REDEEMER_MAX_FAILURES", "7")
    args = build_parser().parse_args(["--max-failures", "2", "--min-retry-interval-sec", "9"])
    assert args.max_failures == 2
    assert args.min_retry_interval_sec == 9


def test_harness_passes_backoff_to_redeemer(monkeypatch, tmp_path: Path):
    captured = {}

    def fake_redeemer(*_args, **kwargs):
        captured["min_retry_interval_sec"] = kwargs["min_retry_interval_sec"]
        captured["max_failures"] = kwargs["max_failures"]
        return {"ok": True, "events": []}

    monkeypatch.setattr("scripts.run_btc5m_supervised_live_cycle.run_redeemer_once", fake_redeemer)
    args = _args(tmp_path, redeemer_min_retry_interval_sec=77, redeemer_max_failures=8)
    run_supervised_cycle(args=args, ledger=LiveLedger(tmp_path / "ledger.db"), log_dir=tmp_path / "log", order_runner=lambda args: {"returncode": 0}, resolution_source=object())

    assert captured == {"min_retry_interval_sec": 77, "max_failures": 8}
