#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import shutil
import sys
from pathlib import Path
from typing import Any, Optional

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts import check_btc5m_canary_parity as parity
from src.runtime.btc5m_canary_policy import (
    DEFAULT_HMM_MODEL_ID,
    POLICY_ID,
    REQUIRED_PROBABILITY_MODEL_ID,
    CanaryConfig,
    evaluate_canary_policy,
)


DEFAULT_COMPACT_ROOT = Path("artifacts/compact_market_recorder/2026-04-23_to_2026-05-11")
DEFAULT_PREDICTIONS_ROOT = Path("artifacts/probability_models_5m/compact_overlap_20260423_20260511_predictions")
DEFAULT_HMM_STATE_PATH = Path("artifacts/hmm_regime_veto_attribution/compact_20260423_20260511_phase1_v2/trade_level_with_hmm.parquet")
DEFAULT_REPLAY_PATH = Path(
    "artifacts/market_age_policy_replay/compact_20260423_20260511_state3_ask_age_v1/trade_level_policy_results.parquet"
)
DEFAULT_OUTPUT_DIR = Path("artifacts/btc5m_canary_rebuilt_inputs/state3_ask_brownian_age60_v0")


def read_frame(path: Path) -> pd.DataFrame:
    return parity.read_frame(path)


def load_market_windows(compact_root: Path) -> pd.DataFrame:
    path = compact_root / "market_windows.parquet"
    if not path.exists():
        raise FileNotFoundError(f"missing compact market windows artifact: {path}")
    frame = pd.read_parquet(path)
    required = {"market_key", "market_id", "condition_id", "market_start_ts"}
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f"market windows missing required columns: {sorted(missing)}")
    out = frame.copy()
    out["market_key"] = normalize_market_key(out["market_key"])
    out["market_start_ts"] = pd.to_datetime(out["market_start_ts"], utc=True, errors="coerce")
    out = out.dropna(subset=["market_start_ts"])
    if out.empty:
        raise ValueError("market windows contain no usable market_start_ts values")
    return out


def load_book_ticks(compact_root: Path) -> pd.DataFrame:
    path = compact_root / "book_ticks.parquet"
    if not path.exists():
        raise FileNotFoundError(f"missing compact quote artifact: {path}")
    frame = pd.read_parquet(path)
    required = {"market_key", "ts", "side", "ask_px_1"}
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f"book ticks missing required quote columns: {sorted(missing)}")
    out = frame.copy()
    out["market_key"] = normalize_market_key(out["market_key"])
    out["ts"] = pd.to_datetime(out["ts"], utc=True, errors="coerce")
    out["side"] = out["side"].astype(str).str.upper()
    if "is_valid_topbook" in out.columns:
        out = out[out["is_valid_topbook"].astype(bool)].copy()
    out = out[out["side"].isin(["YES", "NO"])].dropna(subset=["market_key", "ts", "ask_px_1"])
    if out.empty:
        raise ValueError("book ticks contain no usable valid YES/NO ask rows")
    return out.sort_values(["market_key", "side", "ts"], kind="mergesort").reset_index(drop=True)


def load_brownian_predictions(predictions_root: Path) -> pd.DataFrame:
    if not predictions_root.exists():
        raise FileNotFoundError(f"missing probability artifact root: {predictions_root}")
    frames = []
    for path in sorted(predictions_root.glob("*.parquet")) + sorted(predictions_root.glob("*.csv")):
        try:
            frame = read_frame(path)
        except Exception:
            continue
        if "model_id" not in frame.columns:
            continue
        subset = frame[frame["model_id"].astype(str).eq(REQUIRED_PROBABILITY_MODEL_ID)].copy()
        if subset.empty:
            continue
        subset["artifact_path"] = str(path)
        frames.append(subset)
    if not frames:
        raise FileNotFoundError(
            f"missing brownian probability artifact for {REQUIRED_PROBABILITY_MODEL_ID} under {predictions_root}"
        )
    raw = pd.concat(frames, ignore_index=True)
    ts_col = first_existing(raw, ["timestamp", "prediction_ts", "ts"])
    start_col = first_existing(raw, ["market_window_start", "market_start_ts", "market_start_time"])
    p_col = first_existing(raw, ["p_up", "p_yes", "model_p_yes", "prob_yes", "probability"])
    if not ts_col or not start_col or not p_col:
        raise ValueError("brownian probability artifact missing timestamp/start/probability convention columns")
    out = raw.rename(columns={ts_col: "prediction_ts", start_col: "market_start_ts", p_col: "model_p_yes"}).copy()
    out["prediction_ts"] = pd.to_datetime(out["prediction_ts"], utc=True, errors="coerce")
    out["market_start_ts"] = pd.to_datetime(out["market_start_ts"], utc=True, errors="coerce")
    out["model_p_yes"] = pd.to_numeric(out["model_p_yes"], errors="coerce")
    out["model_p_no"] = 1.0 - out["model_p_yes"]
    out = out.dropna(subset=["prediction_ts", "market_start_ts", "model_p_yes"])
    if out.empty:
        raise ValueError("brownian probability artifact has no usable rows after normalization")
    return out.sort_values(["market_start_ts", "prediction_ts"], kind="mergesort").reset_index(drop=True)


def load_hmm_states(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"missing HMM artifact: {path}")
    frame = read_frame(path)
    ts_col = first_existing(frame, ["timestamp", "hmm_state_ts", "entry_ts", "ts", "decision_ts"])
    required = {"hmm_model_id", "hmm_state"}
    missing = required - set(frame.columns)
    if missing or not ts_col:
        raise ValueError(f"HMM artifact missing required state columns: {sorted(missing)} timestamp_col={ts_col}")
    p_col = first_existing(frame, ["hmm_pmax", "pmax"])
    out = frame[["hmm_model_id", "hmm_state", ts_col] + ([p_col] if p_col else [])].copy()
    out = out.rename(columns={ts_col: "hmm_state_ts", p_col: "hmm_pmax"} if p_col else {ts_col: "hmm_state_ts"})
    out["hmm_model_id"] = out["hmm_model_id"].astype(str)
    out = out[out["hmm_model_id"].eq(DEFAULT_HMM_MODEL_ID)].copy()
    out["hmm_state_ts"] = pd.to_datetime(out["hmm_state_ts"], utc=True, errors="coerce")
    out["hmm_state"] = pd.to_numeric(out["hmm_state"], errors="coerce")
    if "hmm_pmax" not in out.columns:
        out["hmm_pmax"] = pd.NA
    out["hmm_pmax"] = pd.to_numeric(out["hmm_pmax"], errors="coerce")
    out = out.dropna(subset=["hmm_state_ts", "hmm_state"]).drop_duplicates(["hmm_state_ts"], keep="last")
    if out.empty:
        raise ValueError(f"HMM artifact has no usable rows for {DEFAULT_HMM_MODEL_ID}")
    out["hmm_artifact_path"] = str(path)
    return out.sort_values("hmm_state_ts", kind="mergesort").reset_index(drop=True)


def sampled_targets(replay_path: Path, policy_names: list[str], sample_size: int, seed: int) -> pd.DataFrame:
    replay = read_frame(replay_path)
    focused = parity.sample_focused_rows(replay, policy_names=policy_names, sample_size=sample_size, seed=seed)
    required = {"row_id", "decision_ts"}
    missing = required - set(focused.columns)
    if missing:
        raise ValueError(f"sampled replay rows missing targeting columns: {sorted(missing)}")
    focused["decision_ts"] = pd.to_datetime(focused["decision_ts"], utc=True, errors="coerce")
    if focused["decision_ts"].isna().any():
        raise ValueError("sampled replay rows contain ambiguous decision_ts values")
    return focused


def attach_market_identity(targets: pd.DataFrame, windows: pd.DataFrame) -> pd.DataFrame:
    window_cols = ["market_key", "market_id", "condition_id", "market_start_ts", "yes_token_id", "no_token_id"]
    source = targets.drop(columns=["market_start_ts"], errors="ignore").copy()
    if "market_key" in source.columns and source["market_key"].notna().any():
        source["market_key"] = normalize_market_key(source["market_key"])
        windows = windows.copy()
        windows["market_key"] = normalize_market_key(windows["market_key"])
        merged = source.merge(windows[window_cols], on="market_key", how="left", suffixes=("_replay", ""))
        for col in ["market_id", "condition_id"]:
            replay_col = f"{col}_replay"
            if replay_col in merged.columns:
                merged[col] = merged[col].combine_first(merged[replay_col])
                merged = merged.drop(columns=[replay_col])
    elif {"market_id", "condition_id"}.issubset(source.columns) and source[["market_id", "condition_id"]].notna().all(axis=1).any():
        merged = source.merge(windows[window_cols], on=["market_id", "condition_id"], how="left", suffixes=("", "_window"))
    else:
        numeric_key = pd.to_numeric(source.get("market_id"), errors="coerce") if "market_id" in source.columns else pd.Series(dtype=float)
        source = source.assign(market_key=normalize_market_key(numeric_key))
        windows = windows.copy()
        windows["market_key"] = normalize_market_key(windows["market_key"])
        merged = source.merge(windows[window_cols], on="market_key", how="left", suffixes=("_replay", ""))
        for col in ["market_id", "condition_id"]:
            replay_col = f"{col}_replay"
            if replay_col in merged.columns:
                merged[col] = merged[col].combine_first(merged[replay_col])
                merged = merged.drop(columns=[replay_col])
    if merged["market_start_ts"].isna().any():
        missing = merged.loc[merged["market_start_ts"].isna(), ["row_id", "market_id", "condition_id"]].to_dict("records")
        raise ValueError(f"market timestamp reconstruction is ambiguous for sampled rows: {missing[:10]}")
    dupes = merged.duplicated("row_id", keep=False)
    if dupes.any():
        raise ValueError(f"market timestamp reconstruction produced duplicate row ids: {merged.loc[dupes, 'row_id'].tolist()[:10]}")
    merged["market_age_sec"] = (merged["decision_ts"] - merged["market_start_ts"]).dt.total_seconds()
    return merged


def attach_quotes(targets: pd.DataFrame, ticks: pd.DataFrame) -> pd.DataFrame:
    out = targets.drop(columns=["yes_ask", "no_ask", "yes_quote_ts", "no_quote_ts", "quote_ts", "quote_age_ms"], errors="ignore").copy()
    out["market_key"] = normalize_market_key(out["market_key"])
    ticks = ticks.copy()
    ticks["market_key"] = normalize_market_key(ticks["market_key"])
    for side in ["YES", "NO"]:
        side_ticks = ticks[ticks["side"].eq(side)].copy()
        lookup = previous_asof_by_group(
            out[["row_id", "market_key", "decision_ts"]],
            side_ticks[["market_key", "ts", "ask_px_1"]],
            by="market_key",
            left_on="decision_ts",
            right_on="ts",
        ).rename(columns={"ts": f"{side.lower()}_quote_ts", "ask_px_1": f"{side.lower()}_ask"})
        out = out.merge(lookup[["row_id", f"{side.lower()}_quote_ts", f"{side.lower()}_ask"]], on="row_id", how="left")
    if out[["yes_ask", "no_ask"]].isna().any().any():
        missing = out.loc[out[["yes_ask", "no_ask"]].isna().any(axis=1), "row_id"].tolist()
        raise ValueError(f"quote fields are unavailable for sampled rows: {missing[:10]}")
    out["quote_ts"] = out[["yes_quote_ts", "no_quote_ts"]].max(axis=1)
    out["quote_age_ms"] = (out["decision_ts"] - out["quote_ts"]).dt.total_seconds() * 1000.0
    return out


def attach_predictions(targets: pd.DataFrame, predictions: pd.DataFrame) -> pd.DataFrame:
    targets = targets.drop(columns=["model_p_yes", "model_p_no", "prediction_ts", "probability_model_artifact_path"], errors="ignore").copy()
    lookup = previous_asof_by_group(
        targets[["row_id", "market_start_ts", "decision_ts"]],
        predictions[["market_start_ts", "prediction_ts", "model_p_yes", "model_p_no", "artifact_path"]],
        by="market_start_ts",
        left_on="decision_ts",
        right_on="prediction_ts",
    )
    out = targets.merge(
        lookup[["row_id", "prediction_ts", "model_p_yes", "model_p_no", "artifact_path"]].rename(
            columns={"artifact_path": "probability_model_artifact_path"}
        ),
        on="row_id",
        how="left",
    )
    if out[["model_p_yes", "model_p_no"]].isna().any().any():
        missing = out.loc[out[["model_p_yes", "model_p_no"]].isna().any(axis=1), "row_id"].tolist()
        raise ValueError(f"brownian predictions are unavailable for sampled rows: {missing[:10]}")
    out["model_id"] = REQUIRED_PROBABILITY_MODEL_ID
    out["probability_replay_convention"] = "model_p_yes"
    return out


def attach_hmm(targets: pd.DataFrame, states: pd.DataFrame) -> pd.DataFrame:
    targets = targets.drop(columns=["hmm_model_id", "hmm_state", "hmm_pmax", "hmm_state_ts", "hmm_artifact_path"], errors="ignore").copy()
    lookup = pd.merge_asof(
        targets[["row_id", "decision_ts"]].sort_values("decision_ts", kind="mergesort"),
        states[["hmm_state_ts", "hmm_state", "hmm_pmax", "hmm_artifact_path"]].sort_values("hmm_state_ts", kind="mergesort"),
        left_on="decision_ts",
        right_on="hmm_state_ts",
        direction="backward",
        allow_exact_matches=True,
    )
    out = targets.merge(lookup[["row_id", "hmm_state_ts", "hmm_state", "hmm_pmax", "hmm_artifact_path"]], on="row_id", how="left")
    if out["hmm_state"].isna().any():
        missing = out.loc[out["hmm_state"].isna(), "row_id"].tolist()
        raise ValueError(f"previous-only HMM join produced missing state for sampled rows: {missing[:10]}")
    out["hmm_model_id"] = DEFAULT_HMM_MODEL_ID
    out["hmm_state"] = out["hmm_state"].astype(int)
    return out


def add_policy_decisions(frame: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for _, row in frame.iterrows():
        selected_side, selected_ask, selected_edge = selected_from_row(row)
        decision = evaluate_canary_policy(
            market={
                "market_id": row["market_id"],
                "condition_id": row["condition_id"],
                "market_start_ts": row["market_start_ts"],
                "market_age_sec": row["market_age_sec"],
            },
            quote={
                "valid_topbook": True,
                "quote_ts": row["quote_ts"],
                "quote_age_ms": row["quote_age_ms"],
                "yes_ask": row["yes_ask"],
                "no_ask": row["no_ask"],
            },
            predictions={
                "model_id": REQUIRED_PROBABILITY_MODEL_ID,
                "model_p_yes": row["model_p_yes"],
                "model_p_no": row["model_p_no"],
                "artifact_path": row.get("probability_model_artifact_path"),
                "probability_replay_convention": "model_p_yes",
            },
            hmm_state={
                "hmm_model_id": DEFAULT_HMM_MODEL_ID,
                "hmm_state": row["hmm_state"],
                "hmm_pmax": row.get("hmm_pmax"),
                "hmm_state_ts": row.get("hmm_state_ts"),
                "hmm_artifact_path": row.get("hmm_artifact_path"),
            },
            risk_state={"open_positions": 0, "daily_loss_usd": 0.0},
            config=CanaryConfig(min_edge=float(row.get("edge_threshold") or 0.0), canary_stake_usd=1.0),
            decision_ts=row["decision_ts"],
        )
        enriched = row.to_dict()
        enriched.update(
            {
                "selected_side": selected_side,
                "selected_ask": selected_ask,
                "selected_edge": selected_edge,
                "final_decision": decision["final_decision"],
                "abstain_reason": decision["abstain_reason"],
            }
        )
        rows.append(enriched)
    return pd.DataFrame(rows)


def selected_from_row(row: pd.Series) -> tuple[str, float, float]:
    yes_edge = float(row["model_p_yes"]) - float(row["yes_ask"])
    no_edge = float(row["model_p_no"]) - float(row["no_ask"])
    if yes_edge >= no_edge:
        return "YES", float(row["yes_ask"]), yes_edge
    return "NO", float(row["no_ask"]), no_edge


def build_rebuilt_inputs(args: argparse.Namespace) -> tuple[pd.DataFrame, dict[str, Any]]:
    targets = sampled_targets(Path(args.replay_path), parity.parse_csv(args.policy_names), int(args.sample_size), int(args.seed))
    windows = load_market_windows(Path(args.compact_root))
    ticks = load_book_ticks(Path(args.compact_root))
    predictions = load_brownian_predictions(Path(args.predictions_root))
    states = load_hmm_states(Path(args.hmm_state_path))
    rebuilt = attach_market_identity(targets, windows)
    rebuilt = attach_quotes(rebuilt, ticks)
    rebuilt = attach_predictions(rebuilt, predictions)
    rebuilt = attach_hmm(rebuilt, states)
    rebuilt = add_policy_decisions(rebuilt)
    output_cols = [
        "row_id",
        "policy_id",
        "policy_name",
        "market_id",
        "condition_id",
        "market_key",
        "market_start_ts",
        "decision_ts",
        "market_age_sec",
        "model_id",
        "model_p_yes",
        "model_p_no",
        "probability_model_artifact_path",
        "probability_replay_convention",
        "hmm_model_id",
        "hmm_state",
        "hmm_pmax",
        "hmm_state_ts",
        "hmm_artifact_path",
        "yes_ask",
        "no_ask",
        "quote_ts",
        "quote_age_ms",
        "selected_side",
        "selected_ask",
        "selected_edge",
        "final_decision",
        "abstain_reason",
        "edge_threshold",
        "valid_topbook",
    ]
    rebuilt["policy_id"] = POLICY_ID
    rebuilt["valid_topbook"] = True
    rebuilt = rebuilt[[col for col in output_cols if col in rebuilt.columns]].copy()
    manifest = {
        "policy_id": POLICY_ID,
        "replay_path": str(args.replay_path),
        "compact_root": str(args.compact_root),
        "predictions_root": str(args.predictions_root),
        "hmm_state_path": str(args.hmm_state_path),
        "sample_size": int(args.sample_size),
        "seed": int(args.seed),
        "rebuilt_rows": int(len(rebuilt)),
        "previous_only_joins": True,
        "copied_from_replay": ["row_id", "policy_name", "decision_ts", "edge_threshold"],
        "not_copied_from_replay": [
            "market_start_ts",
            "market_age_sec",
            "yes_ask",
            "no_ask",
            "model_p_yes",
            "model_p_no",
            "hmm_state",
            "hmm_pmax",
        ],
    }
    return rebuilt, manifest


def write_outputs(output_dir: Path, rebuilt: pd.DataFrame, manifest: dict[str, Any], overwrite: bool) -> None:
    if output_dir.exists():
        if not overwrite:
            raise FileExistsError(f"{output_dir} exists; pass --overwrite")
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    rebuilt.to_parquet(output_dir / "rebuilt_inputs.parquet", index=False)
    (output_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
    (output_dir / "README.txt").write_text(render_readme(manifest), encoding="utf-8")


def render_readme(manifest: dict[str, Any]) -> str:
    return f"""BTC-5M canary rebuilt inputs

Policy: {POLICY_ID}

This artifact rebuilds live/shadow canary policy inputs from compact quote, probability, and HMM artifacts. Replay rows are used only to sample row ids and decision timestamps. Model probabilities, HMM state, executable asks, market start, and market age are reattached or recomputed from source artifacts using previous-only joins.

Inputs:
- replay_path={manifest.get('replay_path')}
- compact_root={manifest.get('compact_root')}
- predictions_root={manifest.get('predictions_root')}
- hmm_state_path={manifest.get('hmm_state_path')}

Output:
- rebuilt_inputs.parquet rows={manifest.get('rebuilt_rows')}

Use this with:
.venv/bin/python scripts/check_btc5m_canary_parity.py --replay-path {manifest.get('replay_path')} --rebuilt-input-path <this_dir>/rebuilt_inputs.parquet --output-dir artifacts/btc5m_canary_parity/state3_ask_brownian_age60_v0
"""


def first_existing(frame: pd.DataFrame, names: list[str]) -> Optional[str]:
    for name in names:
        if name in frame.columns:
            return name
    return None


def normalize_market_key(series: Any) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").astype("Int64")


def previous_asof_by_group(
    left: pd.DataFrame,
    right: pd.DataFrame,
    *,
    by: str,
    left_on: str,
    right_on: str,
) -> pd.DataFrame:
    parts = []
    right_groups = {key: group for key, group in right.groupby(by, dropna=False, sort=False)}
    for _, left_group in left.groupby(by, dropna=False, sort=False):
        group_key = left_group[by].iloc[0]
        right_group = right_groups.get(group_key)
        if right_group is None or right_group.empty:
            part = left_group.copy()
            for col in right.columns:
                if col not in part.columns:
                    part[col] = pd.NA
            parts.append(part)
            continue
        left_sorted = left_group.sort_values(left_on, kind="mergesort")
        right_sorted = right_group.sort_values(right_on, kind="mergesort")
        joined = pd.merge_asof(
            left_sorted,
            right_sorted.drop(columns=[by], errors="ignore"),
            left_on=left_on,
            right_on=right_on,
            direction="backward",
            allow_exact_matches=True,
        )
        parts.append(joined)
    if not parts:
        return left.copy()
    return pd.concat(parts, ignore_index=True)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build replay-backed live-input rows for BTC-5M canary parity checks.")
    parser.add_argument("--replay-path", type=Path, default=DEFAULT_REPLAY_PATH)
    parser.add_argument("--compact-root", type=Path, default=DEFAULT_COMPACT_ROOT)
    parser.add_argument("--predictions-root", type=Path, default=DEFAULT_PREDICTIONS_ROOT)
    parser.add_argument("--hmm-state-path", type=Path, default=DEFAULT_HMM_STATE_PATH)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--policy-names", default=parity.DEFAULT_POLICY_NAMES)
    parser.add_argument("--sample-size", type=int, default=1000, help="0 means all focused replay rows.")
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--overwrite", action="store_true")
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    try:
        args = build_parser().parse_args(argv)
        rebuilt, manifest = build_rebuilt_inputs(args)
        write_outputs(Path(args.output_dir), rebuilt, manifest, bool(args.overwrite))
    except Exception as exc:
        print(f"rebuilt input build failed: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(manifest, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
