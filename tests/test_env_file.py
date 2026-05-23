from __future__ import annotations

from pathlib import Path

from src.runtime.env_file import load_env_file


def test_load_env_file_parses_common_dotenv_forms(tmp_path: Path, monkeypatch):
    path = tmp_path / ".env"
    path.write_text(
        """
# comment
export BTC5M_MIN_EDGE=0.02
BTC5M_HMM_ARTIFACT_DIR="/opt/btc5m_models/laplace_1m_gaussian_hmm_k4"
BTC5M_LIVE_RV30='0.01'
BTC5M_CANARY_STAKE_USD=5 # inline comment
""",
        encoding="utf-8",
    )
    for key in ["BTC5M_MIN_EDGE", "BTC5M_HMM_ARTIFACT_DIR", "BTC5M_LIVE_RV30", "BTC5M_CANARY_STAKE_USD"]:
        monkeypatch.delenv(key, raising=False)

    loaded = load_env_file(path)

    assert loaded["BTC5M_MIN_EDGE"] == "0.02"
    assert loaded["BTC5M_HMM_ARTIFACT_DIR"] == "/opt/btc5m_models/laplace_1m_gaussian_hmm_k4"
    assert loaded["BTC5M_LIVE_RV30"] == "0.01"
    assert loaded["BTC5M_CANARY_STAKE_USD"] == "5"


def test_load_env_file_does_not_override_existing_env_by_default(tmp_path: Path, monkeypatch):
    path = tmp_path / ".env"
    path.write_text("BTC5M_MIN_EDGE=0.02\n", encoding="utf-8")
    monkeypatch.setenv("BTC5M_MIN_EDGE", "0.05")

    loaded = load_env_file(path)

    assert "BTC5M_MIN_EDGE" not in loaded
    assert loaded == {}
