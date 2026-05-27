from __future__ import annotations

import argparse

from scripts.diagnose_btc5m_resolution_source import diagnose, main


class Source:
    allow_weak_gamma_mapping = False

    def diagnostics(self):
        return {"resolution_source": "gamma_ctf"}

    def gamma_fetcher(self, lot):
        return {"closed": True, "outcomes": '["Yes","No"]', "outcomePrices": '["1","0"]'}

    def read_ctf_payout(self, condition_id):
        return {"resolved": True, "denominator": 1, "payout_vector": [1, 0], "winning_index": 0}

    def resolve(self, lot):
        return {"resolved": True, "winning_side": "YES", "source": "gamma_ctf", "onchain_confirmed": True, "error": None}


def test_diagnostic_resolves_mocked_known_market():
    args = argparse.Namespace(condition_id="0x" + "11" * 32, market_id="m1", raw_gamma=False)

    row = diagnose(args, source=Source())

    assert row["resolved"] is True
    assert row["winning_side"] == "YES"
    assert row["mapped_ctf_winning_side"] == "YES"


def test_diagnostic_exits_nonzero_on_expected_side_mismatch(monkeypatch):
    monkeypatch.setattr("scripts.diagnose_btc5m_resolution_source.GammaCtfResolutionSource", lambda: Source())

    rc = main(["--condition-id", "0x" + "11" * 32, "--expected-side", "NO"])

    assert rc == 1
