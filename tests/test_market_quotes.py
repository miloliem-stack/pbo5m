import pytest

from src.market_quotes import build_market_quote_row, get_quote_snapshot


def test_get_quote_snapshot_uses_orderbook_only(monkeypatch):
    def fake_clob_get_diagnostic(path, *, params=None, timeout=None):
        assert path == "/book"
        assert params == {"token_id": "token-yes"}
        return {
            "ok": True,
            "payload": {
                "bids": [{"price": "0.41", "size": "12"}],
                "asks": [{"price": "0.43", "size": "9"}],
            },
            "http_status": 200,
            "transport": "requests_session",
        }

    monkeypatch.setattr("src.market_quotes.clob_get_diagnostic", fake_clob_get_diagnostic)

    snapshot = get_quote_snapshot("token-yes", force_refresh=True)

    assert snapshot["fetch_ok"] is True
    assert snapshot["error_kind"] is None
    assert snapshot["http_status"] == 200
    assert snapshot["transport"] == "requests_session"
    assert snapshot["best_bid"] == 0.41
    assert snapshot["best_ask"] == 0.43
    assert snapshot["mid"] == 0.42
    assert snapshot["spread"] == pytest.approx(0.02)
    assert snapshot["raw"]["book"]["bids"][0]["price"] == "0.41"
    assert snapshot["raw"]["book_diagnostic"]["ok"] is True


def test_get_quote_snapshot_selects_best_prices_from_unsorted_book(monkeypatch):
    def fake_clob_get_diagnostic(path, *, params=None, timeout=None):
        return {
            "ok": True,
            "payload": {
                "bids": [
                    {"price": "0.20", "size": "10"},
                    {"price": "0.37", "size": "0"},
                    {"price": "0.31", "size": "5"},
                ],
                "asks": [
                    {"price": "0.99", "size": "7"},
                    {"price": "0.44", "size": "0"},
                    {"price": "0.46", "size": "8"},
                ],
            },
            "http_status": 200,
            "transport": "requests_session",
        }

    monkeypatch.setattr("src.market_quotes.clob_get_diagnostic", fake_clob_get_diagnostic)

    snapshot = get_quote_snapshot("token-unsorted", force_refresh=True)

    assert snapshot["fetch_ok"] is True
    assert snapshot["best_bid"] == pytest.approx(0.31)
    assert snapshot["bid_size"] == pytest.approx(5.0)
    assert snapshot["best_ask"] == pytest.approx(0.46)
    assert snapshot["ask_size"] == pytest.approx(8.0)


def test_build_market_quote_row_marks_failed_quote_capture():
    market = {
        "market_id": "m1",
        "slug": "btc-up",
        "condition_id": "c1",
        "token_yes": "yes",
        "token_no": "no",
        "start_time": "2026-04-18T20:00:00Z",
        "end_time": "2026-04-18T20:05:00Z",
    }
    failed_quote = {
        "fetch_ok": False,
        "error_kind": "http_failure",
        "http_status": 403,
        "transport": "requests_session",
        "best_bid": None,
        "best_ask": None,
        "mid": None,
        "spread": None,
        "age_seconds": 0.0,
        "error": "HTTP 403",
        "raw": {"book_diagnostic": {"ok": False}},
    }

    row = build_market_quote_row(
        market,
        failed_quote,
        failed_quote,
        source="clob_poll",
        raw_payload_fragment={"yes_raw": failed_quote["raw"], "no_raw": failed_quote["raw"]},
    )

    assert row["record_type"] == "quote_snapshot"
    assert row["quote_capture_ok"] is False
    assert row["quote_capture_status"] == "failed"
    assert row["yes"]["fetch_ok"] is False
    assert row["yes"]["error_kind"] == "http_failure"
    assert row["yes"]["http_status"] == 403
    assert row["no"]["fetch_ok"] is False
