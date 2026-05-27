from __future__ import annotations

from pathlib import Path

from scripts.update_btc5m_market_resolutions import update_once
from src.runtime.btc5m_resolution_source import GammaCtfResolutionSource
from src.runtime.btc5m_live_ledger import LiveLedger


class Source:
    def __init__(self, resolved=True, side="YES"):
        self.resolved = resolved
        self.side = side

    def resolve(self, lot):
        return {"resolved": self.resolved, "winning_side": self.side, "source": "mock"}


def _ledger(tmp_path: Path) -> LiveLedger:
    ledger = LiveLedger(tmp_path / "ledger.db")
    ledger.record_fill_from_event(
        {
            "event_type": "order_filled",
            "order_id": "ord1",
            "market_id": "m1",
            "condition_id": "c1",
            "token_id": "tok",
            "selected_side": "YES",
            "filled_size": 1,
            "avg_fill_price": 0.4,
            "raw_response": {"trade_id": "trade1"},
        }
    )
    return ledger


def test_updates_win_lots_from_mocked_resolution_source(tmp_path: Path):
    ledger = _ledger(tmp_path)

    summary = update_once(ledger, source=Source(resolved=True, side="YES"))

    assert summary["resolved_wins"] == 1
    with ledger.connect() as conn:
        status = conn.execute("SELECT status FROM outcome_lots").fetchone()[0]
    assert status == "resolved_win"


def test_unresolved_markets_do_not_become_redeemable(tmp_path: Path):
    ledger = _ledger(tmp_path)

    summary = update_once(ledger, source=Source(resolved=False, side="UNKNOWN"))

    assert summary["unresolved"] == 1
    assert ledger.redeemable_lots() == []


class FakeFunctions:
    def __init__(self, denominator, nums):
        self.denominator = denominator
        self.nums = nums

    def payoutDenominator(self, condition):
        return Call(self.denominator)

    def payoutNumerators(self, condition, idx):
        return Call(self.nums[idx])


class Call:
    def __init__(self, value):
        self.value = value

    def call(self):
        return self.value


class FakeWeb3:
    def __init__(self, denominator, nums):
        self.eth = self.Eth(denominator, nums)

    def to_checksum_address(self, address):
        return address

    class Eth:
        chain_id = 137

        def __init__(self, denominator, nums):
            self.denominator = denominator
            self.nums = nums

        def contract(self, address, abi):
            return type("Contract", (), {"functions": FakeFunctions(self.denominator, self.nums)})()


def test_real_source_resolves_yes_only_when_gamma_and_ctf_agree():
    source = GammaCtfResolutionSource(
        env={"POLYGON_RPC": "mock"},
        web3=FakeWeb3(denominator=1, nums=[1, 0]),
        gamma_fetcher=lambda lot: {"closed": True, "outcomes": '["Yes","No"]', "outcomePrices": '["1","0"]'},
    )

    result = source.resolve({"condition_id": "0x" + "11" * 32})

    assert result["resolved"] is True
    assert result["winning_side"] == "YES"


def test_real_source_resolves_no_only_when_gamma_and_ctf_agree():
    source = GammaCtfResolutionSource(
        env={"POLYGON_RPC": "mock"},
        web3=FakeWeb3(denominator=1, nums=[0, 1]),
        gamma_fetcher=lambda lot: {"closed": True, "outcomes": '["Yes","No"]', "outcomePrices": '["0","1"]'},
    )

    result = source.resolve({"condition_id": "0x" + "11" * 32})

    assert result["resolved"] is True
    assert result["winning_side"] == "NO"


def test_gamma_resolved_but_onchain_unresolved_fails_closed():
    source = GammaCtfResolutionSource(
        env={"POLYGON_RPC": "mock"},
        web3=FakeWeb3(denominator=0, nums=[0, 0]),
        gamma_fetcher=lambda lot: {"closed": True, "outcomes": '["Yes","No"]', "outcomePrices": '["1","0"]'},
    )

    result = source.resolve({"condition_id": "0x" + "11" * 32})

    assert result["resolved"] is False
    assert result["error"] == "ctf_unresolved"


def test_onchain_resolved_but_gamma_ambiguous_fails_closed():
    source = GammaCtfResolutionSource(
        env={"POLYGON_RPC": "mock"},
        web3=FakeWeb3(denominator=1, nums=[1, 0]),
        gamma_fetcher=lambda lot: {"closed": True, "outcomes": '["Yes","No"]'},
    )

    result = source.resolve({"condition_id": "0x" + "11" * 32})

    assert result["resolved"] is False
    assert result["error"] == "gamma_unresolved_or_ambiguous"


def test_weak_gamma_label_fallback_fails_closed_by_default():
    source = GammaCtfResolutionSource(
        env={"POLYGON_RPC": "mock"},
        web3=FakeWeb3(denominator=1, nums=[1, 0]),
        gamma_fetcher=lambda lot: {"closed": True, "outcomes": '["Bitcoin Up","Bitcoin Down"]', "outcomePrices": '["1","0"]'},
    )

    result = source.resolve({"condition_id": "0x" + "11" * 32})

    assert result["resolved"] is False
    assert result["error"] == "gamma_outcome_label_mapping_weak"


def test_weak_gamma_label_fallback_can_be_allowed_explicitly():
    source = GammaCtfResolutionSource(
        env={"POLYGON_RPC": "mock", "BTC5M_ALLOW_WEAK_GAMMA_OUTCOME_INDEX_MAPPING": "true"},
        web3=FakeWeb3(denominator=1, nums=[1, 0]),
        gamma_fetcher=lambda lot: {"closed": True, "outcomes": '["Bitcoin Up","Bitcoin Down"]', "outcomePrices": '["1","0"]'},
    )

    result = source.resolve({"condition_id": "0x" + "11" * 32})

    assert result["resolved"] is True
    assert result["winning_side"] == "YES"


def test_resolution_diagnostics_marks_fail_open_ignored():
    source = GammaCtfResolutionSource(env={"POLYGON_RPC": "mock", "BTC5M_RESOLUTION_FAIL_OPEN": "true"}, web3=FakeWeb3(0, [0, 0]))
    assert source.diagnostics()["fail_open_ignored_for_safety"] is True
