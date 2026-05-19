from pathlib import Path

import pandas as pd
import pytest

from scripts import build_strategy_research_report as report


def _write_fixture(root: Path) -> pd.DataFrame:
    root.mkdir(parents=True, exist_ok=True)
    trades = pd.DataFrame(
        {
            "market_key": [11, 22, 22],
            "gross_cost": [1.0, 2.0, 3.0],
            "gross_pnl": [0.2, -0.4, 0.6],
            "gross_payout": [1.2, 1.6, 3.6],
            "win": [True, False, True],
            "side": ["YES", "NO", "YES"],
            "ask_bin": ["0.45_0.47", "0.49_0.50", "0.50_0.55"],
            "chronological_slice": ["early", "main", "fresh"],
            "regime_state": [0, 1, 1],
        }
    )
    trades.to_parquet(root / "trade_level_results.parquet", index=False)
    return trades


def test_report_file_created_and_sections_present(tmp_path):
    replay_root = tmp_path / "replay"
    out = tmp_path / "report.md"
    _write_fixture(replay_root)

    manifest = report.run(
        report.build_parser().parse_args(
            ["--input", str(replay_root), "--output", str(out), "--strategy-name", "MeanRevertV1"]
        )
    )

    assert out.exists()
    text = out.read_text(encoding="utf-8")
    for i, section in enumerate(report.REQUIRED_SECTIONS, start=1):
        assert f"## {i}. {section}" in text
    assert manifest["missing_required_columns"] == []


def test_missing_attribution_columns_reported(tmp_path):
    replay_root = tmp_path / "replay"
    out = tmp_path / "report.md"
    trades = _write_fixture(replay_root).drop(columns=["ask_bin", "regime_state", "chronological_slice", "side"])
    trades.to_parquet(replay_root / "trade_level_results.parquet", index=False)

    manifest = report.run(
        report.build_parser().parse_args(
            ["--input", str(replay_root), "--output", str(out), "--strategy-name", "Sparse"]
        )
    )
    text = out.read_text(encoding="utf-8")
    assert "Attribution dimensions missing" in text
    assert "ask bin" in manifest["missing_attribution_dimensions"]
    assert "regime/HMM state" in manifest["missing_attribution_dimensions"]
    assert "chronological slice" in manifest["missing_attribution_dimensions"]


def test_aggregate_metrics_are_read_correctly(tmp_path):
    replay_root = tmp_path / "replay"
    out = tmp_path / "report.md"
    trades = _write_fixture(replay_root)
    manifest = report.run(
        report.build_parser().parse_args(
            ["--input", str(replay_root), "--output", str(out), "--strategy-name", "Agg"]
        )
    )
    text = out.read_text(encoding="utf-8")
    assert f"- Gross cost: {trades['gross_cost'].sum():.6f}" in text
    assert f"- Gross PnL: {trades['gross_pnl'].sum():.6f}" in text
    roi = trades["gross_pnl"].sum() / trades["gross_cost"].sum()
    assert f"- ROI on filled cost: {roi:.6f}" in text
    assert manifest["rows"] == len(trades)


def test_replay_assumptions_are_explicit_and_non_leaky(tmp_path):
    replay_root = tmp_path / "replay"
    out = tmp_path / "report.md"
    _write_fixture(replay_root)
    report.run(
        report.build_parser().parse_args(
            ["--input", str(replay_root), "--output", str(out), "--strategy-name", "LeakageGuard"]
        )
    )
    text = out.read_text(encoding="utf-8")
    assert "no future information" in text
    assert "does not call any sweep or simulation routines" in text


def test_missing_required_columns_are_detected(tmp_path):
    replay_root = tmp_path / "replay"
    out = tmp_path / "report.md"
    trades = _write_fixture(replay_root).drop(columns=["gross_pnl"])
    trades.to_parquet(replay_root / "trade_level_results.parquet", index=False)

    manifest = report.run(
        report.build_parser().parse_args(
            ["--input", str(replay_root), "--output", str(out), "--strategy-name", "MissingReq"]
        )
    )
    assert manifest["missing_required_columns"] == ["gross_pnl"]
    assert "Missing required aggregate columns: gross_pnl" in out.read_text(encoding="utf-8")
