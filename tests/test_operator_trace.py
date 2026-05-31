from __future__ import annotations

import json

from src.runtime.operator_trace import trace_event


def test_trace_redacts_secret_like_keys_and_truncates(monkeypatch, capsys):
    monkeypatch.setenv("BTC5M_OPERATOR_TRACE", "true")
    trace_event(
        "unit_test",
        POLY_API_SECRET="super-secret",
        wallet_private_key="0xdeadbeef",
        nested={"api_key": "abc", "safe": "ok"},
        large_value="x" * 1000,
    )
    lines = [line for line in capsys.readouterr().err.splitlines() if line.strip()]
    assert lines
    payload = json.loads(lines[-1])
    assert payload["event_type"] == "unit_test"
    assert payload["POLY_API_SECRET"] == "<redacted>"
    assert payload["wallet_private_key"] == "<redacted>"
    assert payload["nested"]["api_key"] == "<redacted>"
    assert payload["nested"]["safe"] == "ok"
    assert payload["large_value"].endswith("...<truncated>")
    encoded = json.dumps(payload)
    assert "super-secret" not in encoded
    assert "0xdeadbeef" not in encoded
    assert "\"api_key\": \"abc\"" not in encoded
