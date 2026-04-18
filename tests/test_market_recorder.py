from src.runtime.market_recorder import _market_identity


def test_market_identity_ignores_raw_market_jitter():
    base_market = {
        "market_id": "m1",
        "slug": "btc-up",
        "condition_id": "c1",
        "token_yes": "yes",
        "token_no": "no",
        "start_time": "2026-04-18T20:00:00Z",
        "end_time": "2026-04-18T20:05:00Z",
        "routing_score": 10,
        "raw_market": {"foo": "bar"},
    }
    jittered_market = {
        **base_market,
        "routing_score": 4,
        "raw_market": {"foo": "baz", "changed": True},
    }

    assert _market_identity(base_market) == _market_identity(jittered_market)


def test_market_identity_changes_when_market_identity_changes():
    first_market = {
        "market_id": "m1",
        "slug": "btc-up",
        "condition_id": "c1",
        "token_yes": "yes",
        "token_no": "no",
        "start_time": "2026-04-18T20:00:00Z",
        "end_time": "2026-04-18T20:05:00Z",
    }
    second_market = {
        **first_market,
        "market_id": "m2",
    }

    assert _market_identity(first_market) != _market_identity(second_market)
