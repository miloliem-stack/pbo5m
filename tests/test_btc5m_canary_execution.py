from __future__ import annotations

import types
import json
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pytest

import src.runtime.btc5m_canary_execution as canary_execution
from src.runtime.btc5m_canary_execution import (
    CanaryExecutor,
    ExecutionConfig,
    ExecutionJournal,
    PyClobClientAdapter,
    add_decision_provenance,
    create_order_intent,
    import_clob_v2_sdk,
    normalize_clob_error,
)
from src.runtime.btc5m_canary_policy import CanaryConfig, POLICY_ID
from scripts import run_btc5m_canary_live as live_runner


class FakeAdapter:
    def __init__(self, *, wallet="0xabc", submit_response=None, statuses=None):
        self._wallet = wallet
        self.submit_response = submit_response or {"status": "submitted", "order_id": "ord1"}
        self.statuses = list(statuses or [{"status": "filled", "filled_size": 1.0, "avg_fill_price": 0.4, "remaining_size": 0.0}])
        self.submits = []

    def wallet_address(self):
        return self._wallet

    def submit_buy(self, intent):
        self.submits.append(intent)
        return self.submit_response

    def get_order_status(self, order_id):
        if self.statuses:
            return self.statuses.pop(0)
        return {"status": "unknown"}


def _config(tmp_path: Path, **overrides):
    base = {
        "execution_mode": "live",
        "live_trading_enabled": True,
        "live_one_shot": True,
        "max_order_attempts_per_process": 1,
        "canary_stake_usd": 5.0,
        "max_notional_per_market_usd": 5.0,
        "max_daily_notional_usd": 20.0,
        "journal_root": tmp_path,
        "policy_config": CanaryConfig(min_edge=0.02, canary_stake_usd=5.0),
        "order_poll_interval_sec": 0.0,
        "order_poll_timeout_sec": 0.01,
    }
    base.update(overrides)
    return ExecutionConfig(**base)


def _decision(**overrides):
    generated_ts = overrides.pop("_generated_ts", datetime(2026, 5, 22, 10, 1, 30, tzinfo=timezone.utc))
    expiry_ms = overrides.pop("_expiry_ms", 2000)
    base = {
        "policy_id": POLICY_ID,
        "market_id": "m1",
        "condition_id": "c1",
        "yes_token_id": "yes-token",
        "no_token_id": "no-token",
        "market_start_ts": "2026-05-22T10:00:00+00:00",
        "decision_ts": "2026-05-22T10:01:30+00:00",
        "market_age_sec": 90.0,
        "quote_ts": "2026-05-22T10:01:30+00:00",
        "quote_age_ms": 100.0,
        "selected_side": "YES",
        "selected_ask": 0.40,
        "selected_edge": 0.05,
        "final_decision": "BUY_YES",
        "market_age_gate_pass": True,
        "hmm_gate_pass": True,
        "model_gate_pass": True,
        "ask_filter_pass": True,
        "edge_gate_pass": True,
        "risk_gate_pass": True,
        "one_entry_gate_pass": True,
        "model_id": "brownian_zero_drift__rv30",
        "hmm_model_id": "laplace_1m__gaussian_hmm__k4",
        "hmm_state": 3,
        "probability_replay_convention": "model_p_yes",
    }
    base.update(overrides)
    return add_decision_provenance(
        base,
        policy_config=CanaryConfig(min_edge=0.02, canary_stake_usd=5.0),
        input_payload={"test": "input", "decision_ts": base.get("decision_ts")},
        now=generated_ts,
        expiry_ms=expiry_ms,
    )


def _executor(tmp_path: Path, adapter=None, config=None, now=None):
    now_fn = lambda: now or datetime(2026, 5, 22, 10, 1, 30, tzinfo=timezone.utc)
    return CanaryExecutor(
        config or _config(tmp_path),
        adapter if adapter is not None else FakeAdapter(),
        ExecutionJournal(tmp_path, now_fn=now_fn),
        now_fn=now_fn,
        sleep_fn=lambda _: None,
    )


def _create_intent(decision, config, wallet_address="0xabc"):
    return create_order_intent(
        decision,
        config,
        wallet_address=wallet_address,
        execution_ts=datetime(2026, 5, 22, 10, 1, 30, tzinfo=timezone.utc),
    )


def _events(root: Path):
    rows = []
    for path in sorted(root.glob("*/execution_events.jsonl")):
        import json

        rows.extend(json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip())
    return rows


def test_observe_mode_never_submits(tmp_path: Path):
    adapter = FakeAdapter()
    executor = _executor(tmp_path, adapter=adapter, config=_config(tmp_path, execution_mode="observe", live_trading_enabled=False))
    event = executor.execute_decision(_decision())
    assert event["event_type"] == "execution_skipped"
    assert event["skip_reason"] == "not_live_mode"
    assert adapter.submits == []


def test_live_mode_refuses_when_live_flag_false(tmp_path: Path):
    executor = _executor(tmp_path, config=_config(tmp_path, live_trading_enabled=False))
    with pytest.raises(RuntimeError, match="live_trading_disabled"):
        executor.startup_check()


def test_missing_stake_refuses_startup(tmp_path: Path):
    executor = _executor(tmp_path, config=_config(tmp_path, canary_stake_usd=None))
    with pytest.raises(RuntimeError, match="missing_or_invalid_stake"):
        executor.startup_check()


def test_expected_wallet_mismatch_refuses_startup(tmp_path: Path):
    executor = _executor(tmp_path, adapter=FakeAdapter(wallet="0xabc"), config=_config(tmp_path, expected_wallet_address="0xdef"))
    with pytest.raises(RuntimeError, match="expected_wallet_mismatch"):
        executor.startup_check()


def test_live_startup_refuses_legacy_clob_sdk(tmp_path: Path):
    class LegacyAdapter(FakeAdapter):
        def redacted_adapter_config(self):
            return {"clob_sdk_family": "py-clob-client", "clob_sdk_version": "0.23.0"}

    executor = _executor(tmp_path, adapter=LegacyAdapter())
    with pytest.raises(RuntimeError, match="clob_sdk_legacy_v1_refused"):
        executor.startup_check()


def test_live_startup_refuses_unknown_clob_sdk_version(tmp_path: Path):
    class UnknownVersionAdapter(FakeAdapter):
        def redacted_adapter_config(self):
            return {"clob_sdk_family": "py-clob-client-v2", "clob_sdk_version": None}

    executor = _executor(tmp_path, adapter=UnknownVersionAdapter())
    with pytest.raises(RuntimeError, match="clob_sdk_version_unknown"):
        executor.startup_check()


def test_buy_yes_creates_yes_token_order_intent(tmp_path: Path):
    intent, reason = _create_intent(_decision(selected_side="YES", final_decision="BUY_YES"), _config(tmp_path))
    assert reason is None
    assert intent.token_id == "yes-token"
    assert intent.selected_side == "YES"


def test_buy_no_creates_no_token_order_intent(tmp_path: Path):
    intent, reason = _create_intent(_decision(selected_side="NO", final_decision="BUY_NO"), _config(tmp_path))
    assert reason is None
    assert intent.token_id == "no-token"
    assert intent.selected_side == "NO"


def test_abstain_creates_no_order_intent(tmp_path: Path):
    intent, reason = _create_intent(_decision(final_decision="ABSTAIN"), _config(tmp_path))
    assert intent is None
    assert reason == "not_buy_decision"


@pytest.mark.parametrize(
    ("kwargs", "reason"),
    [
        ({"quote_age_ms": 99999.0}, "decision_quote_stale"),
        ({"market_age_sec": 59.0, "market_start_ts": None}, "decision_market_age_invalid"),
        ({"market_age_sec": 241.0, "market_start_ts": None}, "decision_market_age_invalid"),
        ({"selected_ask": 0.30}, "ask_filter_failed"),
        ({"selected_ask": 0.47}, "ask_filter_failed"),
    ],
)
def test_execution_gate_blocks_bad_decisions(tmp_path: Path, kwargs, reason):
    intent, actual = _create_intent(_decision(**kwargs), _config(tmp_path))
    assert intent is None
    assert actual == reason


def test_selected_ask_plus_slippage_cannot_exceed_max_limit_price(tmp_path: Path):
    cfg = _config(tmp_path, max_price_slippage=0.05, max_limit_price=0.49)
    intent, reason = _create_intent(_decision(selected_ask=0.46), cfg)
    assert reason is None
    assert intent.max_price == pytest.approx(0.49)
    assert intent.limit_price == pytest.approx(0.49)


def test_duplicate_journal_blocks_same_market_reentry(tmp_path: Path):
    adapter = FakeAdapter()
    executor = _executor(tmp_path, adapter=adapter)
    first = executor.execute_decision(_decision())
    second = executor.execute_decision(_decision())
    assert first["event_type"] in {"order_filled", "order_status_polled"}
    assert second["skip_reason"] == "duplicate_journal_entry"


def test_restart_reloads_journal_and_blocks_duplicate(tmp_path: Path):
    _executor(tmp_path, adapter=FakeAdapter()).execute_decision(_decision())
    restarted = _executor(tmp_path, adapter=FakeAdapter())
    event = restarted.execute_decision(_decision())
    assert event["skip_reason"] == "duplicate_journal_entry"


def test_live_one_shot_submits_at_most_one_mocked_order(tmp_path: Path):
    adapter = FakeAdapter()
    executor = _executor(tmp_path, adapter=adapter)
    executor.execute_decision(_decision(condition_id="c1"))
    executor.execute_decision(_decision(condition_id="c2", market_id="m2"))
    assert len(adapter.submits) == 1


def test_order_filled_event_is_logged(tmp_path: Path):
    executor = _executor(tmp_path, adapter=FakeAdapter(statuses=[{"status": "filled", "filled_size": 2.0, "avg_fill_price": 0.41}]))
    event = executor.execute_decision(_decision())
    assert event["event_type"] == "order_filled"
    assert any(row["event_type"] == "order_filled" for row in _events(tmp_path))


def test_partial_fill_event_is_logged(tmp_path: Path):
    executor = _executor(tmp_path, adapter=FakeAdapter(statuses=[{"status": "partially_filled", "filled_size": 1.0, "remaining_size": 2.0}]))
    event = executor.execute_decision(_decision())
    assert event["event_type"] == "order_partially_filled"


def test_unknown_after_submit_blocks_reentry(tmp_path: Path):
    executor = _executor(tmp_path, adapter=FakeAdapter(submit_response={"status": "submitted"}))
    event = executor.execute_decision(_decision())
    assert event["event_type"] == "order_unknown_after_submit"
    again = executor.execute_decision(_decision())
    assert again["skip_reason"] == "duplicate_journal_entry"


def test_execution_events_jsonl_schema_is_stable(tmp_path: Path):
    executor = _executor(tmp_path, adapter=FakeAdapter())
    executor.execute_decision(_decision())
    event = _events(tmp_path)[0]
    required = {
        "event_type",
        "policy_id",
        "market_id",
        "condition_id",
        "token_id",
        "selected_side",
        "selected_ask",
        "selected_edge",
        "stake_usd",
        "limit_price",
        "market_age_sec",
        "decision_ts",
        "execution_ts",
        "quote_ts",
        "quote_age_ms",
        "client_order_id",
        "idempotency_key",
        "wallet_address",
        "execution_mode",
        "live_trading_enabled",
        "live_one_shot",
    }
    assert required.issubset(event)


def test_pyclob_adapter_defaults_funder_to_wallet_address(monkeypatch):
    created = {}

    class FakeClobClient:
        def __init__(self, host, key, chain, signature_type, funder, creds=None):
            created.update({"host": host, "key": key, "chain": chain, "signature_type": signature_type, "funder": funder, "creds": creds})

        def create_or_derive_api_key(self):
            return object()

        def set_api_creds(self, creds):
            self.creds = creds

    monkeypatch.setattr(
        canary_execution,
        "import_clob_v2_sdk",
        lambda: (
            FakeClobClient,
            lambda **kwargs: kwargs,
            lambda **kwargs: kwargs,
            types.SimpleNamespace(FAK="FAK_ENUM"),
            lambda **kwargs: kwargs,
            lambda **kwargs: kwargs,
            types.SimpleNamespace(BUY="BUY"),
            types.SimpleNamespace(EOA="EOA_ENUM", POLY_PROXY="POLY_PROXY_ENUM", GNOSIS_SAFE="GNOSIS_SAFE_ENUM", POLY_1271="POLY_1271_ENUM"),
        ),
    )
    monkeypatch.setattr(canary_execution, "clob_sdk_metadata", lambda: {"clob_sdk_family": "py-clob-client-v2", "clob_sdk_version": "2.0.0"})

    adapter = PyClobClientAdapter(
        env={
            "POLY_WALLET_PRIVATE_KEY": "redacted",
            "POLY_WALLET_ADDRESS": "0xWallet",
            "POLY_CLOB_BASE": "https://clob.polymarket.com",
            "POLYGON_CHAIN_ID": "137",
            "POLY_SIGNATURE_TYPE": "0",
        }
    )

    assert created["funder"] == "0xWallet"
    assert created["chain"] == 137
    assert created["signature_type"] == "EOA_ENUM"
    assert adapter.redacted_adapter_config()["funder_source"] == "wallet_address"
    assert adapter.redacted_adapter_config()["clob_sdk_family"] == "py-clob-client-v2"
    assert "POLY_WALLET_PRIVATE_KEY" not in adapter.redacted_adapter_config()


def test_pyclob_adapter_honors_explicit_funder_and_posts_fak_enum(monkeypatch):
    posted = {}

    class FakeClobClient:
        def __init__(self, host, key, chain, signature_type, funder, creds=None):
            self.kwargs = {"host": host, "key": key, "chain": chain, "signature_type": signature_type, "funder": funder, "creds": creds}

        def create_or_derive_api_key(self):
            return object()

        def set_api_creds(self, creds):
            self.creds = creds

        def create_and_post_market_order(self, order_args, options, order_type):
            posted["order_args"] = order_args
            posted["options"] = options
            posted["order_type"] = order_type
            return {"status": "submitted", "order_id": "ord1"}

    monkeypatch.setattr(
        canary_execution,
        "import_clob_v2_sdk",
        lambda: (
            FakeClobClient,
            lambda **kwargs: kwargs,
            lambda **kwargs: kwargs,
            types.SimpleNamespace(FAK="FAK_ENUM"),
            lambda **kwargs: kwargs,
            lambda **kwargs: kwargs,
            types.SimpleNamespace(BUY="BUY"),
            types.SimpleNamespace(EOA="EOA_ENUM", POLY_PROXY="POLY_PROXY_ENUM", GNOSIS_SAFE="GNOSIS_SAFE_ENUM", POLY_1271="POLY_1271_ENUM"),
        ),
    )
    monkeypatch.setattr(canary_execution, "clob_sdk_metadata", lambda: {"clob_sdk_family": "py-clob-client-v2", "clob_sdk_version": "2.0.0"})
    adapter = PyClobClientAdapter(
        env={
            "POLY_WALLET_PRIVATE_KEY": "redacted",
            "POLY_WALLET_ADDRESS": "0xWallet",
            "POLY_FUNDER": "0xFunder",
            "POLY_SIGNATURE_TYPE": "1",
        }
    )
    result = adapter.submit_buy(
        type(
            "Intent",
            (),
            {
                "token_id": "token",
                "stake_usd": 5.0,
                "limit_price": 0.38,
                "max_price": 0.39,
                "selected_ask": 0.37,
            },
        )()
    )

    assert adapter.adapter_config["funder"] == "0xFunder"
    assert adapter.adapter_config["signature_type"] == 1
    assert posted["order_args"]["price"] == 0.38
    assert posted["order_args"]["amount"] == 5.0
    assert posted["order_type"] == "FAK_ENUM"
    assert posted["options"]["tick_size"] == "0.01"
    assert result["order_id"] == "ord1"


def test_pyclob_market_buy_amount_is_rounded_to_two_decimals(monkeypatch):
    posted = {}

    class FakeClobClient:
        def __init__(self, host, key, chain, signature_type, funder, creds=None):
            pass

        def set_api_creds(self, creds):
            self.creds = creds

        def create_and_post_market_order(self, order_args, options, order_type):
            posted["order_args"] = order_args
            return {"status": "submitted", "order_id": "ord1"}

    monkeypatch.setattr(
        canary_execution,
        "import_clob_v2_sdk",
        lambda: (
            FakeClobClient,
            lambda **kwargs: kwargs,
            lambda **kwargs: kwargs,
            types.SimpleNamespace(FAK="FAK_ENUM"),
            lambda **kwargs: kwargs,
            lambda **kwargs: kwargs,
            types.SimpleNamespace(BUY="BUY"),
            None,
        ),
    )
    monkeypatch.setattr(canary_execution, "clob_sdk_metadata", lambda: {"clob_sdk_family": "py-clob-client-v2", "clob_sdk_version": "2.0.0"})
    adapter = PyClobClientAdapter(
        env={
            "POLY_WALLET_PRIVATE_KEY": "redacted",
            "POLY_WALLET_ADDRESS": "0xWallet",
            "POLY_API_KEY": "key",
            "POLY_API_SECRET": "secret",
            "POLY_API_PASSPHRASE": "pass",
        }
    )
    adapter.submit_buy(
        type(
            "Intent",
            (),
            {
                "token_id": "token",
                "stake_usd": 5.009,
                "limit_price": 0.38,
                "max_price": 0.39,
                "selected_ask": 0.37,
            },
        )()
    )
    assert posted["order_args"]["amount"] == 5.0
    assert posted["order_args"]["price"] == 0.38


def test_pyclob_adapter_accepts_signature_type_3_with_explicit_funder(monkeypatch):
    created = {}

    class FakeClobClient:
        def __init__(self, host, key, chain, signature_type, funder, creds=None):
            created.update({"signature_type": signature_type, "funder": funder, "creds": creds})

        def set_api_creds(self, creds):
            self.creds = creds

    monkeypatch.setattr(
        canary_execution,
        "import_clob_v2_sdk",
        lambda: (
            FakeClobClient,
            lambda **kwargs: kwargs,
            lambda **kwargs: kwargs,
            types.SimpleNamespace(FAK="FAK_ENUM"),
            lambda **kwargs: kwargs,
            lambda **kwargs: kwargs,
            types.SimpleNamespace(BUY="BUY"),
            types.SimpleNamespace(EOA="EOA_ENUM", POLY_PROXY="POLY_PROXY_ENUM", GNOSIS_SAFE="GNOSIS_SAFE_ENUM", POLY_1271="POLY_1271_ENUM"),
        ),
    )
    monkeypatch.setattr(canary_execution, "clob_sdk_metadata", lambda: {"clob_sdk_family": "py-clob-client-v2", "clob_sdk_version": "2.0.0"})
    adapter = PyClobClientAdapter(
        env={
            "POLY_WALLET_PRIVATE_KEY": "redacted",
            "POLY_WALLET_ADDRESS": "0xWallet",
            "POLY_FUNDER": "0xDepositWallet",
            "POLY_SIGNATURE_TYPE": "3",
            "POLY_API_KEY": "key",
            "POLY_API_SECRET": "secret",
            "POLY_API_PASSPHRASE": "pass",
        }
    )
    assert created["signature_type"] == "POLY_1271_ENUM"
    assert created["funder"] == "0xDepositWallet"
    assert adapter.redacted_adapter_config()["signature_type_name"] == "POLY_1271"
    assert adapter.redacted_adapter_config()["credential_error"] is None


def test_live_startup_without_l2_creds_and_no_bootstrap_fails_closed(monkeypatch, tmp_path: Path):
    class FakeClobClient:
        def __init__(self, host, key, chain, signature_type, funder, creds=None):
            pass

    monkeypatch.setattr(
        canary_execution,
        "import_clob_v2_sdk",
        lambda: (FakeClobClient, lambda **kwargs: kwargs, lambda **kwargs: kwargs, types.SimpleNamespace(FAK="FAK_ENUM"), lambda **kwargs: kwargs, lambda **kwargs: kwargs, types.SimpleNamespace(BUY="BUY"), None),
    )
    monkeypatch.setattr(canary_execution, "clob_sdk_metadata", lambda: {"clob_sdk_family": "py-clob-client-v2", "clob_sdk_version": "2.0.0"})
    adapter = PyClobClientAdapter(env={"POLY_WALLET_PRIVATE_KEY": "redacted", "POLY_WALLET_ADDRESS": "0xWallet"})
    executor = _executor(tmp_path, adapter=adapter)
    with pytest.raises(RuntimeError, match="missing_clob_l2_credentials"):
        executor.startup_check()


def test_live_startup_with_l2_creds_does_not_bootstrap(monkeypatch, tmp_path: Path):
    calls = {"derive": 0}

    class FakeClobClient:
        def __init__(self, host, key, chain, signature_type, funder, creds=None):
            pass

        def create_or_derive_api_key(self):
            calls["derive"] += 1
            return object()

        def set_api_creds(self, creds):
            self.creds = creds

    monkeypatch.setattr(
        canary_execution,
        "import_clob_v2_sdk",
        lambda: (FakeClobClient, lambda **kwargs: kwargs, lambda **kwargs: kwargs, types.SimpleNamespace(FAK="FAK_ENUM"), lambda **kwargs: kwargs, lambda **kwargs: kwargs, types.SimpleNamespace(BUY="BUY"), None),
    )
    monkeypatch.setattr(canary_execution, "clob_sdk_metadata", lambda: {"clob_sdk_family": "py-clob-client-v2", "clob_sdk_version": "2.0.0"})
    adapter = PyClobClientAdapter(
        env={
            "POLY_WALLET_PRIVATE_KEY": "redacted",
            "POLY_WALLET_ADDRESS": "0xWallet",
            "POLY_API_KEY": "key",
            "POLY_API_SECRET": "secret",
            "POLY_API_PASSPHRASE": "pass",
        }
    )
    executor = _executor(tmp_path, adapter=adapter)
    startup = executor.startup_check()
    assert startup["startup_ok"] is True
    assert calls["derive"] == 0


def test_bootstrap_flag_may_call_create_or_derive(monkeypatch):
    calls = {"derive": 0}

    class FakeClobClient:
        def __init__(self, host, key, chain, signature_type, funder, creds=None):
            pass

        def create_or_derive_api_key(self):
            calls["derive"] += 1
            return {"apiKey": "key", "secret": "secret", "passphrase": "pass"}

        def set_api_creds(self, creds):
            self.creds = creds

    monkeypatch.setattr(
        canary_execution,
        "import_clob_v2_sdk",
        lambda: (FakeClobClient, lambda **kwargs: kwargs, lambda **kwargs: kwargs, types.SimpleNamespace(FAK="FAK_ENUM"), lambda **kwargs: kwargs, lambda **kwargs: kwargs, types.SimpleNamespace(BUY="BUY"), None),
    )
    monkeypatch.setattr(canary_execution, "clob_sdk_metadata", lambda: {"clob_sdk_family": "py-clob-client-v2", "clob_sdk_version": "2.0.0"})
    adapter = PyClobClientAdapter(
        env={
            "POLY_WALLET_PRIVATE_KEY": "redacted",
            "POLY_WALLET_ADDRESS": "0xWallet",
            "BTC5M_ALLOW_CLOB_API_KEY_BOOTSTRAP": "true",
        }
    )
    assert calls["derive"] == 1
    assert adapter.redacted_adapter_config()["l2_credentials_present"] is True


def test_signature_type_3_without_explicit_funder_fails_startup(monkeypatch, tmp_path: Path):
    class FakeClobClient:
        def __init__(self, host, key, chain, signature_type, funder, creds=None):
            pass

        def set_api_creds(self, creds):
            self.creds = creds

    monkeypatch.setattr(
        canary_execution,
        "import_clob_v2_sdk",
        lambda: (FakeClobClient, lambda **kwargs: kwargs, lambda **kwargs: kwargs, types.SimpleNamespace(FAK="FAK_ENUM"), lambda **kwargs: kwargs, lambda **kwargs: kwargs, types.SimpleNamespace(BUY="BUY"), None),
    )
    monkeypatch.setattr(canary_execution, "clob_sdk_metadata", lambda: {"clob_sdk_family": "py-clob-client-v2", "clob_sdk_version": "2.0.0"})
    adapter = PyClobClientAdapter(
        env={
            "POLY_WALLET_PRIVATE_KEY": "redacted",
            "POLY_WALLET_ADDRESS": "0xWallet",
            "POLY_SIGNATURE_TYPE": "3",
            "POLY_API_KEY": "key",
            "POLY_API_SECRET": "secret",
            "POLY_API_PASSPHRASE": "pass",
        }
    )
    executor = _executor(tmp_path, adapter=adapter)
    with pytest.raises(RuntimeError, match="missing_deposit_wallet_funder"):
        executor.startup_check()


def test_redacted_adapter_config_has_no_secrets(monkeypatch):
    class FakeClobClient:
        def __init__(self, host, key, chain, signature_type, funder, creds=None):
            pass

        def set_api_creds(self, creds):
            self.creds = creds

    monkeypatch.setattr(
        canary_execution,
        "import_clob_v2_sdk",
        lambda: (FakeClobClient, lambda **kwargs: kwargs, lambda **kwargs: kwargs, types.SimpleNamespace(FAK="FAK_ENUM"), lambda **kwargs: kwargs, lambda **kwargs: kwargs, types.SimpleNamespace(BUY="BUY"), None),
    )
    monkeypatch.setattr(canary_execution, "clob_sdk_metadata", lambda: {"clob_sdk_family": "py-clob-client-v2", "clob_sdk_version": "2.0.0"})
    adapter = PyClobClientAdapter(
        env={
            "POLY_WALLET_PRIVATE_KEY": "private",
            "POLY_WALLET_ADDRESS": "0xWallet",
            "POLY_API_KEY": "key",
            "POLY_API_SECRET": "secret",
            "POLY_API_PASSPHRASE": "pass",
        }
    )
    encoded = str(adapter.redacted_adapter_config())
    assert "private" not in encoded
    assert "secret" not in encoded
    assert "pass" not in encoded


def test_requirements_do_not_reference_legacy_pyclob_package():
    text = Path("requirements.txt").read_text(encoding="utf-8")
    assert "py-clob-client-v2" in text
    assert "py-clob-client>=" not in text


def test_import_clob_v2_sdk_refuses_legacy_only(monkeypatch):
    def fake_find_spec(name):
        if name == "py_clob_client_v2":
            return None
        if name == "py_clob_client":
            return object()
        return None

    monkeypatch.setattr(canary_execution.importlib.util, "find_spec", fake_find_spec)
    with pytest.raises(RuntimeError, match="clob_sdk_legacy_v1_refused"):
        import_clob_v2_sdk()


def test_order_version_mismatch_normalizes_as_terminal_protocol_rejection():
    normalized = normalize_clob_error(Exception("PolyApiException[status_code=400, error_message={'error': 'order_version_mismatch'}]"))
    assert normalized["error_code"] == "order_version_mismatch"
    assert normalized["terminal"] is True
    assert normalized["retryable"] is False


def test_clob_api_key_create_failure_normalizes_as_terminal_auth_rejection():
    normalized = normalize_clob_error(Exception("[py_clob_client_v2] request error status=400 url=https://clob.polymarket.com/auth/api-key body={\"error\":\"Could not create api key\"}"))
    assert normalized["error_code"] == "clob_api_key_create_failed"
    assert normalized["terminal"] is True
    assert normalized["retryable"] is False


def test_order_version_mismatch_journaled_as_venue_rejection_without_poll(tmp_path: Path):
    class RejectingAdapter(FakeAdapter):
        def __init__(self):
            super().__init__()
            self.polls = 0

        def submit_buy(self, intent):
            self.submits.append(intent)
            raise RuntimeError("PolyApiException[status_code=400, error_message={'error': 'order_version_mismatch'}]")

        def get_order_status(self, order_id):
            self.polls += 1
            return super().get_order_status(order_id)

    adapter = RejectingAdapter()
    executor = _executor(tmp_path, adapter=adapter)
    event = executor.execute_decision(_decision())
    assert event["event_type"] == "execution_rejected_by_venue"
    assert event["error_code"] == "order_version_mismatch"
    assert event["terminal"] is True
    assert event["retryable"] is False
    assert adapter.polls == 0


def test_valid_generated_decision_passes_executor_validation(tmp_path: Path):
    intent, reason = _create_intent(_decision(), _config(tmp_path))
    assert reason is None
    assert intent is not None


def test_hand_written_decision_without_provenance_is_rejected(tmp_path: Path):
    decision = _decision()
    for key in ["generated_by", "config_hash", "input_hash", "generated_ts", "expires_ts"]:
        decision.pop(key, None)
    intent, reason = _create_intent(decision, _config(tmp_path))
    assert intent is None
    assert reason == "decision_provenance_missing"


def test_expired_decision_is_rejected(tmp_path: Path):
    decision = _decision(_generated_ts=datetime(2026, 5, 22, 10, 1, 20, tzinfo=timezone.utc), _expiry_ms=1000)
    intent, reason = _create_intent(decision, _config(tmp_path))
    assert intent is None
    assert reason == "decision_expired"


def test_config_hash_mismatch_is_rejected(tmp_path: Path):
    decision = _decision()
    decision["config_hash"] = "wrong"
    intent, reason = _create_intent(decision, _config(tmp_path))
    assert intent is None
    assert reason == "decision_config_hash_mismatch"


@pytest.mark.parametrize(
    ("kwargs", "reason"),
    [
        ({"quote_age_ms": 99999.0}, "decision_quote_stale"),
        ({"policy_id": "other"}, "decision_policy_mismatch"),
        ({"model_id": "baseline_50"}, "decision_component_mismatch"),
        ({"hmm_model_id": "core_1m__gaussian_hmm__k4"}, "decision_component_mismatch"),
        ({"hmm_state": 1}, "decision_component_mismatch"),
    ],
)
def test_provenance_component_validation_rejections(tmp_path: Path, kwargs, reason):
    decision = _decision(**kwargs)
    intent, actual = _create_intent(decision, _config(tmp_path))
    assert intent is None
    assert actual == reason


def _input_payload():
    now = datetime.now(timezone.utc)
    start = now - timedelta(seconds=90)
    return {
        "market": {
            "market_id": "m1",
            "condition_id": "c1",
            "token_yes": "yes-token",
            "token_no": "no-token",
            "market_start_ts": start.isoformat(),
            "market_age_sec": 90.0,
        },
        "quote": {
            "valid_topbook": True,
            "quote_ts": now.isoformat(),
            "quote_age_ms": 100.0,
            "yes_ask": 0.40,
            "no_ask": 0.70,
        },
        "predictions": {"model_id": "brownian_zero_drift__rv30", "model_p_yes": 0.45},
        "hmm_state": {
            "hmm_model_id": "laplace_1m__gaussian_hmm__k4",
            "hmm_state": 3,
            "hmm_pmax": 0.82,
        },
        "risk_state": {"open_positions": 0, "daily_loss_usd": 0.0},
        "decision_ts": now.isoformat(),
    }


def test_autonomous_runner_observe_mode_produces_decision_without_clob_calls(tmp_path: Path, monkeypatch):
    input_path = tmp_path / "input.json"
    output_path = tmp_path / "decision.json"
    import json

    input_path.write_text(json.dumps(_input_payload()), encoding="utf-8")
    cfg = _config(tmp_path, execution_mode="observe", live_trading_enabled=False)
    monkeypatch.setattr(live_runner.ExecutionConfig, "from_env", classmethod(lambda cls: cfg))
    monkeypatch.setattr(live_runner, "PyClobClientAdapter", lambda: (_ for _ in ()).throw(AssertionError("no clob in observe")))
    result = live_runner.run(
        type(
            "Args",
            (),
            {
                "build_live_input": False,
                "decision_json": None,
                "decision_input_json": input_path,
                "decision_output_json": output_path,
                "live_log_root": tmp_path / "live",
                "max_runtime_sec": 1,
                "poll_interval_sec": 0,
                "stop_after_first_eligible_decision": True,
            },
        )()
    )
    assert result["event_type"] == "execution_skipped"
    assert output_path.exists()
    generated = json.loads(output_path.read_text(encoding="utf-8"))
    assert generated["generated_by"] == "btc5m_canary_policy_evaluator"


def test_autonomous_runner_live_mode_with_mocked_buy_calls_executor_once(tmp_path: Path, monkeypatch):
    input_path = tmp_path / "input.json"
    import json

    input_path.write_text(json.dumps(_input_payload()), encoding="utf-8")
    cfg = _config(tmp_path, execution_mode="live", live_trading_enabled=True)
    adapter = FakeAdapter()
    monkeypatch.setattr(live_runner.ExecutionConfig, "from_env", classmethod(lambda cls: cfg))
    monkeypatch.setattr(live_runner, "PyClobClientAdapter", lambda: adapter)
    result = live_runner.run(
        type(
            "Args",
            (),
            {
                "build_live_input": False,
                "decision_json": None,
                "decision_input_json": input_path,
                "decision_output_json": tmp_path / "decision.json",
                "live_log_root": tmp_path / "live",
                "max_runtime_sec": 1,
                "poll_interval_sec": 0,
                "stop_after_first_eligible_decision": True,
            },
        )()
    )
    assert result["event_type"] == "order_filled"
    assert len(adapter.submits) == 1


def test_runner_sleep_is_bounded_by_remaining_runtime(monkeypatch):
    sleeps = []
    times = iter([100.0, 100.0, 109.0])
    monkeypatch.setattr(live_runner.time, "monotonic", lambda: next(times))
    monkeypatch.setattr(live_runner.time, "sleep", lambda seconds: sleeps.append(seconds))

    assert live_runner.sleep_until_deadline(110.0, 999.0) is True
    assert sleeps == [10.0]


def test_runner_sleep_returns_false_after_deadline(monkeypatch):
    sleeps = []
    monkeypatch.setattr(live_runner.time, "monotonic", lambda: 111.0)
    monkeypatch.setattr(live_runner.time, "sleep", lambda seconds: sleeps.append(seconds))

    assert live_runner.sleep_until_deadline(110.0, 1.0) is False
    assert sleeps == []


class FakeLiveInputBuilder:
    next_payload = None

    def __init__(self, *args, **kwargs):
        self.config = type("BuilderConfig", (), {"hmm_state_path": Path("hmm.json"), "brownian_state_path": Path("brownian.json")})()

    def build(self):
        return {"ok": True, "missing_components": [], "missing_input_reason": None, "input": self.next_payload or _input_payload()}


def _live_args(tmp_path: Path):
    return type(
        "Args",
        (),
        {
            "build_live_input": True,
            "decision_json": None,
            "decision_input_json": None,
            "decision_output_json": tmp_path / "decision.jsonl",
            "live_log_root": tmp_path / "live",
            "max_runtime_sec": 1,
            "poll_interval_sec": 0,
            "stop_after_first_eligible_decision": True,
        },
    )()


def test_build_live_input_observe_mode_logs_decision_without_clob_calls(tmp_path: Path, monkeypatch):
    cfg = _config(tmp_path, execution_mode="observe", live_trading_enabled=False)
    FakeLiveInputBuilder.next_payload = _input_payload()
    monkeypatch.setattr(live_runner.ExecutionConfig, "from_env", classmethod(lambda cls: cfg))
    monkeypatch.setattr(live_runner, "BTC5MCanaryLiveInputBuilder", FakeLiveInputBuilder)
    monkeypatch.setattr(live_runner, "PyClobClientAdapter", lambda: (_ for _ in ()).throw(AssertionError("no clob in observe")))
    result = live_runner.run(_live_args(tmp_path))
    assert result["event_type"] == "execution_skipped"
    assert result["skip_reason"] == "not_live_mode"
    assert list((tmp_path / "live").glob("*/*/live_input_state.jsonl"))
    assert list((tmp_path / "live").glob("*/*/decision_state.jsonl"))


def test_build_live_input_live_one_shot_mocked_buy_calls_once(tmp_path: Path, monkeypatch):
    cfg = _config(tmp_path, execution_mode="live", live_trading_enabled=True)
    adapter = FakeAdapter()
    FakeLiveInputBuilder.next_payload = _input_payload()
    monkeypatch.setattr(live_runner.ExecutionConfig, "from_env", classmethod(lambda cls: cfg))
    monkeypatch.setattr(live_runner, "PyClobClientAdapter", lambda: adapter)
    monkeypatch.setattr(live_runner, "BTC5MCanaryLiveInputBuilder", FakeLiveInputBuilder)
    monkeypatch.setattr(live_runner, "live_builder_startup_errors", lambda builder: [])
    result = live_runner.run(_live_args(tmp_path))
    assert result["event_type"] == "order_filled"
    assert len(adapter.submits) == 1


def test_build_live_input_wrong_hmm_blocks_execution(tmp_path: Path, monkeypatch):
    cfg = _config(tmp_path, execution_mode="live", live_trading_enabled=True)
    adapter = FakeAdapter()
    payload = _input_payload()
    payload["hmm_state"] = {"hmm_model_id": "laplace_1m__gaussian_hmm__k4", "hmm_state": 1}
    FakeLiveInputBuilder.next_payload = payload
    monkeypatch.setattr(live_runner.ExecutionConfig, "from_env", classmethod(lambda cls: cfg))
    monkeypatch.setattr(live_runner, "PyClobClientAdapter", lambda: adapter)
    monkeypatch.setattr(live_runner, "BTC5MCanaryLiveInputBuilder", FakeLiveInputBuilder)
    monkeypatch.setattr(live_runner, "live_builder_startup_errors", lambda builder: [])
    result = live_runner.run(_live_args(tmp_path))
    assert result["event_type"] == "execution_skipped"
    assert result["skip_reason"] == "decision_component_mismatch"
    assert adapter.submits == []


def test_build_live_input_market_age_outside_window_blocks_execution(tmp_path: Path, monkeypatch):
    cfg = _config(tmp_path, execution_mode="live", live_trading_enabled=True)
    adapter = FakeAdapter()
    payload = _input_payload()
    payload["market"]["market_age_sec"] = 30.0
    FakeLiveInputBuilder.next_payload = payload
    monkeypatch.setattr(live_runner.ExecutionConfig, "from_env", classmethod(lambda cls: cfg))
    monkeypatch.setattr(live_runner, "PyClobClientAdapter", lambda: adapter)
    monkeypatch.setattr(live_runner, "BTC5MCanaryLiveInputBuilder", FakeLiveInputBuilder)
    monkeypatch.setattr(live_runner, "live_builder_startup_errors", lambda builder: [])
    result = live_runner.run(_live_args(tmp_path))
    assert result["skip_reason"] == "not_buy_decision"
    assert adapter.submits == []


def test_build_live_input_final_minute_logs_shadow_but_does_not_execute(tmp_path: Path, monkeypatch):
    cfg = _config(tmp_path, execution_mode="live", live_trading_enabled=True)
    adapter = FakeAdapter()
    payload = _input_payload()
    payload["market"]["market_age_sec"] = 260.0
    FakeLiveInputBuilder.next_payload = payload
    monkeypatch.setattr(live_runner.ExecutionConfig, "from_env", classmethod(lambda cls: cfg))
    monkeypatch.setattr(live_runner, "PyClobClientAdapter", lambda: adapter)
    monkeypatch.setattr(live_runner, "BTC5MCanaryLiveInputBuilder", FakeLiveInputBuilder)
    monkeypatch.setattr(live_runner, "live_builder_startup_errors", lambda builder: [])
    result = live_runner.run(_live_args(tmp_path))
    assert result["skip_reason"] == "not_buy_decision"
    assert adapter.submits == []
    decision_log = next((tmp_path / "live").glob("*/*/decision_state.jsonl"))
    assert '"final_decision": "SHADOW_ONLY"' in decision_log.read_text(encoding="utf-8")


def test_live_mode_refuses_missing_live_state_files(tmp_path: Path, monkeypatch):
    cfg = _config(tmp_path, execution_mode="live", live_trading_enabled=True)
    FakeLiveInputBuilder.next_payload = _input_payload()
    monkeypatch.setattr(live_runner.ExecutionConfig, "from_env", classmethod(lambda cls: cfg))
    monkeypatch.setattr(live_runner, "PyClobClientAdapter", lambda: FakeAdapter())
    monkeypatch.setattr(live_runner, "BTC5MCanaryLiveInputBuilder", FakeLiveInputBuilder)
    monkeypatch.setattr(
        FakeLiveInputBuilder,
        "__init__",
        lambda self, *args, **kwargs: setattr(
            self,
            "config",
            type("BuilderConfig", (), {"hmm_state_path": tmp_path / "missing_hmm.json", "brownian_state_path": tmp_path / "missing_brownian.json", "max_state_age_sec": 15})(),
        ),
    )
    with pytest.raises(RuntimeError, match="hmm_artifact_unavailable"):
        live_runner.run(_live_args(tmp_path))


def test_capital_preflight_trace_emits_skip_reason_without_secrets(tmp_path: Path, monkeypatch):
    captured: list[tuple[str, dict]] = []

    monkeypatch.setattr(canary_execution, "trace_event", lambda event_type, **fields: captured.append((event_type, fields)))
    monkeypatch.setattr(
        canary_execution,
        "trace_stage_done",
        lambda event_type, **fields: captured.append((event_type, fields)) or 0.0,
    )

    class Adapter(FakeAdapter):
        def capital_state(self):
            return {"pusd_balance": 1.0, "POLY_API_SECRET": "dont-log"}

    class Ledger:
        def open_reserved_pusd(self):
            return 0.0

        def unredeemed_winning_estimate(self):
            return 0.0

    executor = CanaryExecutor(
        _config(tmp_path),
        Adapter(),
        ExecutionJournal(tmp_path),
        ledger=Ledger(),
    )
    intent, _ = _create_intent(_decision(), _config(tmp_path))
    assert intent is not None

    result = executor._preflight_order(intent)

    assert result is not None
    assert result.get("skip_reason") == "insufficient_pusd_balance"
    done_events = [fields for event, fields in captured if event == "preflight_order_done"]
    assert done_events
    assert done_events[-1].get("skip_reason") == "insufficient_pusd_balance"
    encoded = json.dumps(captured)
    assert "dont-log" not in encoded
