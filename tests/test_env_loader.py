import os
from pathlib import Path

from scripts import run_btc5m_canary_live
from src.runtime.btc5m_brownian_conservative import validate_brownian_runtime_env
from src.runtime.env_loader import load_env_file, redacted_summary


def test_env_loader_parses_comments_blanks_and_key_values(tmp_path: Path, monkeypatch):
    monkeypatch.delenv("BTC5M_TEST_A", raising=False)
    path = tmp_path / ".env"
    path.write_text(
        """
        # comment
        BTC5M_TEST_A=one

        export BTC5M_TEST_B="two"
        """,
        encoding="utf-8",
    )
    loaded = load_env_file(path)
    assert loaded == {"BTC5M_TEST_A": "one", "BTC5M_TEST_B": "two"}
    assert os.environ["BTC5M_TEST_A"] == "one"
    assert os.environ["BTC5M_TEST_B"] == "two"


def test_env_loader_does_not_override_by_default_and_override_true_works(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("BTC5M_TEST_OVERRIDE", "shell")
    path = tmp_path / ".env"
    path.write_text("BTC5M_TEST_OVERRIDE=file\n", encoding="utf-8")
    assert load_env_file(path) == {}
    assert os.environ["BTC5M_TEST_OVERRIDE"] == "shell"
    assert load_env_file(path, override=True) == {"BTC5M_TEST_OVERRIDE": "file"}
    assert os.environ["BTC5M_TEST_OVERRIDE"] == "file"


def test_secret_keys_are_redacted_in_summary():
    summary = redacted_summary({"POLY_WALLET_PRIVATE_KEY": "abc", "BTC5M_BROWNIAN_BANKROLL_USD": "2000"})
    assert summary["POLY_WALLET_PRIVATE_KEY"] == "<redacted>"
    assert summary["BTC5M_BROWNIAN_BANKROLL_USD"] == "2000"


def test_env_file_missing_path_fails_clearly(tmp_path: Path, capsys):
    code = run_btc5m_canary_live.main(["--env-file", str(tmp_path / "missing.env"), "--build-live-input"])
    captured = capsys.readouterr()
    assert code == 2
    assert "env file does not exist" in captured.err


def paper_env(**overrides):
    env = {
        "BTC5M_STRATEGY_ID": "brownian_no_hmm_conservative_v1",
        "BTC5M_BROWNIAN_ENABLED": "true",
        "BTC5M_BROWNIAN_PAPER_ONLY": "true",
        "BTC5M_BROWNIAN_LIVE_ENABLED": "false",
        "BTC5M_BROWNIAN_BANKROLL_USD": "2000",
        "BTC5M_BROWNIAN_MAX_STAKE_FRACTION": "0.0025",
        "BTC5M_BROWNIAN_SMALL_WALLET_THRESHOLD": "2000",
        "BTC5M_BROWNIAN_MIN_ORDER_NOTIONAL": "5",
    }
    env.update(overrides)
    return env


def live_env(**overrides):
    env = paper_env(
        BTC5M_BROWNIAN_PAPER_ONLY="false",
        BTC5M_BROWNIAN_LIVE_ENABLED="true",
        BTC5M_EXECUTION_MODE="live",
        BTC5M_LIVE_ONE_SHOT="true",
        POLY_WALLET_PRIVATE_KEY="0xabc",
        BTC5M_EXPECTED_WALLET_ADDRESS="0x123",
    )
    env.update(overrides)
    return env


def test_paper_env_validation_does_not_require_secrets():
    assert validate_brownian_runtime_env(paper_env()) == []


def test_live_env_rejects_placeholder_private_key_and_missing_wallet():
    errors = validate_brownian_runtime_env(live_env(POLY_WALLET_PRIVATE_KEY="REPLACE_ME_DO_NOT_COMMIT", BTC5M_EXPECTED_WALLET_ADDRESS=""))
    assert "polymarket_private_key_missing_or_placeholder" in errors
    assert "expected_wallet_missing_or_placeholder" in errors


def test_live_env_rejects_paper_false_live_false():
    errors = validate_brownian_runtime_env(paper_env(BTC5M_BROWNIAN_PAPER_ONLY="false", BTC5M_BROWNIAN_LIVE_ENABLED="false"))
    assert "live_not_enabled" in errors


def test_live_env_rejects_continuous_live_unless_explicitly_allowed():
    errors = validate_brownian_runtime_env(live_env(BTC5M_LIVE_ONE_SHOT="false"))
    assert "continuous_live_blocked" in errors
    assert "continuous_live_blocked" not in validate_brownian_runtime_env(live_env(BTC5M_LIVE_ONE_SHOT="false", BTC5M_ALLOW_CONTINUOUS_LIVE="true"))


def test_live_oneshot_env_passes_validation():
    assert validate_brownian_runtime_env(live_env()) == []
