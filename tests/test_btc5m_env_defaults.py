from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_brownian_wrappers_default_to_complete_dotenv():
    live = (ROOT / "scripts/run_btc5m_brownian_live_oneshot.sh").read_text(encoding="utf-8")
    paper = (ROOT / "scripts/run_btc5m_brownian_paper.sh").read_text(encoding="utf-8")

    assert 'ENV_FILE="${BTC5M_ENV_FILE:-.env}"' in live
    assert 'ENV_FILE="${BTC5M_ENV_FILE:-.env}"' in paper
    assert '.env.btc5m.brownian.live.local"' not in live
    assert '.env.btc5m.brownian.paper.local"' not in paper


def test_live_wrapper_refuses_missing_required_complete_env_values():
    live = (ROOT / "scripts/run_btc5m_brownian_live_oneshot.sh").read_text(encoding="utf-8")

    for required in [
        "POLYGON_RPC missing",
        "POLY_API_KEY missing",
        "POLY_API_SECRET missing",
        "POLY_API_PASSPHRASE missing",
        "BTC5M_STRATEGY_ID must be brownian_no_hmm_conservative_v1",
        "BTC5M_ALLOW_CONTINUOUS_LIVE must not be true",
    ]:
        assert required in live


def test_live_wrapper_does_not_echo_secret_values():
    live = (ROOT / "scripts/run_btc5m_brownian_live_oneshot.sh").read_text(encoding="utf-8")

    assert "echo \"$POLY_WALLET_PRIVATE_KEY" not in live
    assert "echo \"$POLY_API_SECRET" not in live
    assert "echo \"$POLY_API_PASSPHRASE" not in live


def test_live_wrapper_preserves_operator_runtime_over_env_file_values():
    live = (ROOT / "scripts/run_btc5m_brownian_live_oneshot.sh").read_text(encoding="utf-8")

    assert 'OPERATOR_MAX_RUNTIME_SEC="${MAX_RUNTIME_SEC:-}"' in live
    assert 'if [[ -n "$OPERATOR_MAX_RUNTIME_SEC" ]]; then' in live
    assert 'MAX_RUNTIME_SEC="$OPERATOR_MAX_RUNTIME_SEC"' in live
    assert 'OPERATOR_CANARY_TICK_SEC="${BTC5M_CANARY_TICK_SEC:-}"' in live
    assert 'if [[ -n "$OPERATOR_CANARY_TICK_SEC" ]]; then' in live
    assert 'BTC5M_CANARY_TICK_SEC="$OPERATOR_CANARY_TICK_SEC"' in live
    assert 'export BTC5M_OPERATOR_TRACE="${BTC5M_OPERATOR_TRACE:-true}"' in live
    assert "export PYTHONUNBUFFERED=1" in live


def test_brownian_env_examples_use_single_one_dollar_minimum():
    for filename in [".env.btc5m.brownian.paper.example", ".env.btc5m.brownian.live.example"]:
        text = (ROOT / filename).read_text(encoding="utf-8")

        assert "BTC5M_BROWNIAN_MIN_ORDER_NOTIONAL=1" in text
        assert "BTC5M_BROWNIAN_MIN_ORDER_NOTIONAL=5" not in text
        assert "BTC5M_BROWNIAN_MIN_MARKET_BUY_NOTIONAL_USD" not in text
        assert "BTC5M_BROWNIAN_MIN_LIMIT_BUY_SIZE_SHARES" not in text
        assert "POLY_MARKET_BUY_MIN_SPEND" not in text
