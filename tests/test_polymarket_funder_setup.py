from __future__ import annotations

import json
import sys
import types
from pathlib import Path

import pytest

from src.runtime.btc5m_canary_execution import CanaryExecutor, ExecutionConfig, ExecutionJournal, OrderIntent
from src.runtime.btc5m_canary_policy import CanaryConfig
from src.runtime import polymarket_funder_setup as funder_setup
from src.runtime.polymarket_funder_setup import PolymarketFunderConfig, redact_mapping, to_units, validate_mode
from scripts import setup_polymarket_funder


def test_setup_diagnose_mode_does_not_send_txs(monkeypatch, tmp_path: Path):
    env_file = tmp_path / ".env"
    env_file.write_text("POLY_SIGNATURE_TYPE=0\nPOLY_WALLET_ADDRESS=0xOwner\n", encoding="utf-8")
    calls = {"diagnose": 0}

    def fake_diagnose(config, clob_adapter=None):
        calls["diagnose"] += 1
        return {"errors": [], "config": config.redacted()}

    monkeypatch.setattr(setup_polymarket_funder, "diagnose_funder", fake_diagnose)
    monkeypatch.setattr(setup_polymarket_funder, "maybe_adapter", lambda: None)
    assert setup_polymarket_funder.main(["--env-file", str(env_file), "--diagnose-only", "--setup-log", str(tmp_path / "setup.jsonl")]) == 0
    assert calls["diagnose"] == 1


def test_diagnose_includes_ctf_redeem_adapter_approval(monkeypatch):
    monkeypatch.setattr(funder_setup, "make_web3", lambda config: object())
    monkeypatch.setattr(funder_setup, "read_erc20_balance", lambda *args, **kwargs: 0)
    monkeypatch.setattr(funder_setup, "read_erc20_allowance", lambda *args, **kwargs: 0)
    monkeypatch.setattr(funder_setup, "read_erc1155_approval", lambda *args, **kwargs: True)

    row = funder_setup.diagnose_funder(PolymarketFunderConfig(owner_address="0xOwner"))

    assert row["ctf_redeem_adapter_approved"] is True
    assert row["ctf_contract_address"] == "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"


def test_mutation_flags_require_confirmation(tmp_path: Path):
    env_file = tmp_path / ".env"
    env_file.write_text("POLY_SIGNATURE_TYPE=0\nPOLY_WALLET_ADDRESS=0xOwner\n", encoding="utf-8")
    assert setup_polymarket_funder.main(["--env-file", str(env_file), "--approve-onramp", "1"]) == 2


def test_deposit_wallet_mode_requires_signature_type_3_and_funder():
    assert validate_mode(PolymarketFunderConfig(signature_type=0, funder="0xFunder"), deposit_wallet_mode=True) == [
        "deposit_wallet_mode_requires_signature_type_3"
    ]
    assert validate_mode(PolymarketFunderConfig(signature_type=3, funder=None), deposit_wallet_mode=True) == ["missing_deposit_wallet_funder"]


def test_eoa_mode_rejects_signature_type_3():
    assert validate_mode(PolymarketFunderConfig(signature_type=3, funder="0xFunder"), eoa_mode=True) == ["eoa_mode_rejects_signature_type_3"]


def test_to_units_uses_six_decimals():
    assert to_units("1.2345679") == 1234567


def test_polygon_poa_middleware_is_injected(monkeypatch):
    class Onion:
        def __init__(self):
            self.calls = []

        def inject(self, middleware, layer=0):
            self.calls.append((middleware, layer))

    class FakeWeb3:
        def __init__(self):
            self.middleware_onion = Onion()

    fake = FakeWeb3()
    monkeypatch.setattr(funder_setup, "_load_poa_middleware", lambda: "poa_middleware")

    assert funder_setup.inject_polygon_poa_middleware(fake) is True
    assert fake.middleware_onion.calls == [("poa_middleware", 0)]


def test_update_clob_collateral_allowance_uses_v2_params(monkeypatch):
    fake_module = types.ModuleType("py_clob_client_v2")

    class AssetType:
        COLLATERAL = "COLLATERAL"

    class SignatureTypeV2:
        EOA = "EOA"

    class BalanceAllowanceParams:
        def __init__(self, asset_type=None, signature_type=None):
            self.asset_type = asset_type
            self.signature_type = signature_type

    fake_module.AssetType = AssetType
    fake_module.SignatureTypeV2 = SignatureTypeV2
    fake_module.BalanceAllowanceParams = BalanceAllowanceParams
    monkeypatch.setitem(sys.modules, "py_clob_client_v2", fake_module)
    captured = {}

    class Client:
        def update_balance_allowance(self, params):
            captured["params"] = params
            return {"ok": True}

    class Adapter:
        client = Client()
        adapter_config = {"signature_type": 0}

    response = funder_setup.update_clob_collateral_allowance(Adapter())

    assert response == {"ok": True}
    assert captured["params"].asset_type == "COLLATERAL"
    assert captured["params"].signature_type == "EOA"


def test_setup_eoa_ctf_approval_builds_set_approval_for_all(monkeypatch, tmp_path: Path):
    env_file = tmp_path / ".env"
    env_file.write_text("POLY_SIGNATURE_TYPE=0\nPOLY_WALLET_ADDRESS=0xOwner\n", encoding="utf-8")
    calls = {}

    monkeypatch.setattr(setup_polymarket_funder, "maybe_adapter", lambda: None)
    monkeypatch.setattr(setup_polymarket_funder, "diagnose_funder", lambda config, clob_adapter=None: {"errors": [], "config": config.redacted()})
    monkeypatch.setattr(setup_polymarket_funder, "make_web3", lambda config: object())

    def fake_approve(web3, config):
        calls["operator"] = config.ctf_collateral_adapter_address
        calls["ctf"] = config.ctf_contract_address
        return "0xtx"

    monkeypatch.setattr(setup_polymarket_funder, "approve_ctf_redeem_adapter", fake_approve)

    rc = setup_polymarket_funder.main(
        [
            "--env-file",
            str(env_file),
            "--eoa-mode",
            "--approve-ctf-redeem-adapter",
            "--yes-i-understand-this-sends-transactions",
            "--setup-log",
            str(tmp_path / "setup.jsonl"),
        ]
    )

    assert rc == 0
    assert calls["operator"] == "0xAdA100Db00Ca00073811820692005400218FcE1f"


def test_setup_deposit_wallet_ctf_approval_refuses(tmp_path: Path):
    env_file = tmp_path / ".env"
    env_file.write_text("POLY_SIGNATURE_TYPE=3\nPOLY_FUNDER=0xFunder\nPOLY_WALLET_ADDRESS=0xOwner\n", encoding="utf-8")

    rc = setup_polymarket_funder.main(
        [
            "--env-file",
            str(env_file),
            "--deposit-wallet-mode",
            "--approve-ctf-redeem-adapter",
            "--yes-i-understand-this-sends-transactions",
        ]
    )

    assert rc == 2


def test_setup_check_ctf_redeem_adapter_approval(monkeypatch, tmp_path: Path):
    env_file = tmp_path / ".env"
    env_file.write_text("POLY_SIGNATURE_TYPE=0\nPOLY_WALLET_ADDRESS=0xOwner\n", encoding="utf-8")

    monkeypatch.setattr(setup_polymarket_funder, "maybe_adapter", lambda: None)
    monkeypatch.setattr(setup_polymarket_funder, "diagnose_funder", lambda config, clob_adapter=None: {"errors": [], "config": config.redacted()})
    monkeypatch.setattr(setup_polymarket_funder, "make_web3", lambda config: object())
    monkeypatch.setattr(setup_polymarket_funder, "read_erc1155_approval", lambda web3, token, owner, operator: True)

    rc = setup_polymarket_funder.main(
        [
            "--env-file",
            str(env_file),
            "--check-ctf-redeem-adapter-approval",
            "--setup-log",
            str(tmp_path / "setup.jsonl"),
        ]
    )

    assert rc == 0
    assert "ctf_redeem_adapter_approved" in (tmp_path / "setup.jsonl").read_text(encoding="utf-8")


def test_runtime_preflight_fails_closed_on_missing_pusd_balance(tmp_path: Path):
    class Adapter:
        def wallet_address(self):
            return "0xWallet"

        def redacted_adapter_config(self):
            return {"clob_sdk_family": "py-clob-client-v2", "clob_sdk_version": "2.0.0", "l2_credentials_present": True}

        def preflight_order(self, intent):
            return {"skip_reason": "insufficient_pusd_balance", "pusd_balance": 0.0, "required_pusd": intent.stake_usd}

        def submit_buy(self, intent):
            raise AssertionError("submit must not be called")

        def get_order_status(self, order_id):
            return {}

    executor = CanaryExecutor(
        ExecutionConfig(
            execution_mode="live",
            live_trading_enabled=True,
            canary_stake_usd=5,
            max_notional_per_market_usd=5,
            max_daily_notional_usd=5,
            policy_config=CanaryConfig(min_edge=0.0, canary_stake_usd=5),
        ),
        Adapter(),
        ExecutionJournal(tmp_path),
        sleep_fn=lambda _: None,
    )
    intent = OrderIntent(
        policy_id="p",
        market_id="m",
        condition_id="c",
        token_id="t",
        selected_side="YES",
        action="BUY",
        selected_ask=0.4,
        selected_edge=0.1,
        stake_usd=5,
        market_age_sec=90,
        decision_ts=None,
        quote_ts=None,
        quote_age_ms=None,
        client_order_id="cid",
        idempotency_key="idem",
        limit_price=0.4,
    )
    event = executor._preflight_order(intent)
    assert event["skip_reason"] == "insufficient_pusd_balance"


def test_redacted_logs_contain_no_secret_values():
    row = {"POLY_WALLET_PRIVATE_KEY": "private", "POLY_API_SECRET": "secret", "nested": {"POLY_API_PASSPHRASE": "pass"}}
    encoded = json.dumps(redact_mapping(row))
    assert "private" not in encoded
    assert "secret" not in encoded
    assert "pass" not in encoded
