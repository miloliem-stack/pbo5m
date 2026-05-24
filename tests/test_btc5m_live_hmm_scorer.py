from __future__ import annotations

import json
from pathlib import Path

import joblib
import pytest

from scripts import fit_export_btc5m_frozen_hmm as fit_export
from scripts import run_btc5m_live_hmm_scorer as scorer


def _bundle(tmp_path: Path) -> Path:
    root = tmp_path / "bundle"
    root.mkdir()
    model = root / "model.joblib"
    scaler = root / "scaler.joblib"
    joblib.dump({"model": "x"}, model)
    joblib.dump({"scaler": "x"}, scaler)
    manifest = {
        "hmm_model_id": "laplace_1m__gaussian_hmm__k4_frozen_v1",
        "model_artifact": "model.joblib",
        "scaler_artifact": "scaler.joblib",
        "model_hash": fit_export.file_sha256(model),
        "scaler_hash": fit_export.file_sha256(scaler),
        "feature_config_hash": "feat",
    }
    (root / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    (root / "deploy_policy.json").write_text(
        json.dumps(
            {
                "policy_hmm_model_id": "laplace_1m__gaussian_hmm__k4_frozen_v1",
                "allowed_states": [1],
                "model_artifact_hash": manifest["model_hash"],
            }
        ),
        encoding="utf-8",
    )
    return root


def test_live_scorer_accepts_matching_bundle_policy(tmp_path: Path):
    manifest, policy = scorer.validate_bundle_against_deploy_policy(_bundle(tmp_path))
    assert manifest["hmm_model_id"] == policy["policy_hmm_model_id"]


def test_live_scorer_refuses_manifest_model_id_mismatch(tmp_path: Path):
    root = _bundle(tmp_path)
    policy = json.loads((root / "deploy_policy.json").read_text(encoding="utf-8"))
    policy["policy_hmm_model_id"] = "wrong"
    (root / "deploy_policy.json").write_text(json.dumps(policy), encoding="utf-8")
    with pytest.raises(ValueError, match="model id"):
        scorer.validate_bundle_against_deploy_policy(root)


def test_live_scorer_refuses_model_hash_mismatch(tmp_path: Path):
    root = _bundle(tmp_path)
    policy = json.loads((root / "deploy_policy.json").read_text(encoding="utf-8"))
    policy["model_artifact_hash"] = "wrong"
    (root / "deploy_policy.json").write_text(json.dumps(policy), encoding="utf-8")
    with pytest.raises(ValueError, match="model hash"):
        scorer.validate_bundle_against_deploy_policy(root)
