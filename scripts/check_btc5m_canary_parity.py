#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Iterable, Optional

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.runtime.btc5m_canary_policy import (
    DEFAULT_HMM_MODEL_ID,
    POLICY_ID,
    REQUIRED_PROBABILITY_MODEL_ID,
    CanaryConfig,
    evaluate_canary_policy,
)


DEFAULT_REPLAY_PATH = Path(
    "artifacts/market_age_policy_replay/compact_20260423_20260511_state3_ask_age_v1/trade_level_policy_results.parquet"
)
DEFAULT_OUTPUT_DIR = Path("artifacts/btc5m_canary_parity/state3_ask_brownian_age60_v0")
DEFAULT_POLICY_NAMES = "state3_ask_0.30_0.47,state3_ask_0.30_0.47_simple_models,state3_ask_0.30_0.47_excl_logistic"
NUMERIC_FIELDS = {
    "market_age_sec": "age_tol_sec",
    "model_p_yes": "prob_tol",
    "model_p_no": "prob_tol",
    "hmm_pmax": "prob_tol",
    "yes_ask": "ask_tol",
    "no_ask": "ask_tol",
    "selected_ask": "ask_tol",
    "selected_edge": "edge_tol",
}
EXACT_FIELDS = [
    "market_id",
    "condition_id",
    "market_start_ts",
    "decision_ts",
    "model_id",
    "hmm_model_id",
    "hmm_state",
    "selected_side",
    "final_decision",
    "abstain_reason",
]
COMPARE_FIELDS = [
    "market_id",
    "condition_id",
    "market_start_ts",
    "decision_ts",
    "market_age_sec",
    "model_id",
    "model_p_yes",
    "model_p_no",
    "hmm_model_id",
    "hmm_state",
    "hmm_pmax",
    "yes_ask",
    "no_ask",
    "selected_side",
    "selected_ask",
    "selected_edge",
    "final_decision",
    "abstain_reason",
]
FATAL_FIELDS = {
    "hmm_state",
    "model_p_yes",
    "model_p_no",
    "selected_side",
    "final_decision",
}
OPTIONAL_REPLAY_FIELDS = {
    "market_id",
    "condition_id",
    "yes_ask",
    "no_ask",
}


def parse_csv(value: str) -> list[str]:
    return [part.strip() for part in str(value or "").split(",") if part.strip()]


def read_frame(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"required artifact does not exist: {path}")
    suffix = path.suffix.lower()
    if suffix == ".parquet":
        return pd.read_parquet(path)
    if suffix == ".csv":
        return pd.read_csv(path)
    if suffix in {".jsonl", ".ndjson"}:
        return pd.read_json(path, lines=True)
    raise ValueError(f"unsupported artifact format for {path}; expected parquet, csv, or jsonl")


def coalesce(row: pd.Series | dict[str, Any], names: Iterable[str], default: Any = None) -> Any:
    for name in names:
        if name in row:
            value = row[name]
            if not _is_missing(value):
                return value
    return default


def pick_series(frame: pd.DataFrame, names: Iterable[str]) -> pd.Series:
    for name in names:
        if name in frame.columns:
            return frame[name]
    return pd.Series([pd.NA] * len(frame), index=frame.index)


def canonicalize_rows(frame: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for idx, row in frame.reset_index(drop=True).iterrows():
        raw_market_key = coalesce(row, ["market_key"])
        raw_market_id = coalesce(row, ["market_id"])
        raw_condition_id = coalesce(row, ["condition_id"])
        if _looks_like_market_key_alias(raw_market_id, raw_market_key, raw_condition_id):
            raw_market_id = None
        p_yes = to_float(coalesce(row, ["model_p_yes", "p_yes", "prob_yes", "probability"]))
        p_no = to_float(coalesce(row, ["model_p_no", "p_no"]))
        if p_no is None and p_yes is not None:
            p_no = 1.0 - p_yes
        selected_side = normalize_side(coalesce(row, ["selected_side", "side"]))
        selected_ask = to_float(coalesce(row, ["selected_ask", "entry_ask", "ask_price", "executable_ask"]))
        selected_edge = to_float(coalesce(row, ["selected_edge", "model_edge", "edge"]))
        final_decision = coalesce(row, ["final_decision"])
        if _is_missing(final_decision) and selected_side in {"YES", "NO"}:
            final_decision = f"BUY_{selected_side}"
        canonical = {
            "row_id": str(coalesce(row, ["row_id", "parity_row_id"], idx)),
            "policy_id": coalesce(row, ["policy_id"], POLICY_ID),
            "policy_name": coalesce(row, ["policy_name"], ""),
            "market_key": raw_market_key,
            "market_id": raw_market_id,
            "condition_id": raw_condition_id,
            "market_start_ts": iso_text(coalesce(row, ["market_start_ts", "market_start_time"])),
            "decision_ts": iso_text(coalesce(row, ["decision_ts", "entry_ts", "ts", "timestamp"])),
            "market_age_sec": to_float(coalesce(row, ["market_age_sec", "market_age_seconds", "entry_age_sec", "entry_age_seconds"])),
            "model_id": coalesce(row, ["model_id", "model_name"]),
            "model_p_yes": p_yes,
            "model_p_no": p_no,
            "model_version": coalesce(row, ["model_version", "probability_model_version"]),
            "probability_model_artifact_path": coalesce(row, ["probability_model_artifact_path", "model_artifact_path", "artifact_path"]),
            "probability_formula": coalesce(row, ["probability_formula"]),
            "probability_replay_convention": coalesce(row, ["probability_replay_convention"]),
            "hmm_model_id": coalesce(row, ["hmm_model_id"]),
            "hmm_state": to_int(coalesce(row, ["hmm_state", "state"])),
            "hmm_pmax": to_float(coalesce(row, ["hmm_pmax", "pmax"])),
            "hmm_model_version": coalesce(row, ["hmm_model_version"]),
            "hmm_artifact_path": coalesce(row, ["hmm_artifact_path"]),
            "yes_ask": to_float(coalesce(row, ["yes_ask", "executable_yes_ask"])),
            "no_ask": to_float(coalesce(row, ["no_ask", "executable_no_ask"])),
            "selected_side": selected_side,
            "selected_ask": selected_ask,
            "selected_edge": selected_edge,
            "final_decision": None if _is_missing(final_decision) else str(final_decision),
            "abstain_reason": normalize_reason(coalesce(row, ["abstain_reason"])),
            "edge_threshold": to_float(coalesce(row, ["edge_threshold", "min_edge"], 0.0)),
            "valid_topbook": bool(coalesce(row, ["valid_topbook"], True)),
            "quote_ts": iso_text(coalesce(row, ["quote_ts", "decision_ts", "entry_ts", "ts", "timestamp"])),
            "quote_age_ms": to_float(coalesce(row, ["quote_age_ms"], 0.0)),
        }
        rows.append(canonical)
    return pd.DataFrame(rows)


def sample_focused_rows(frame: pd.DataFrame, *, policy_names: list[str], sample_size: int, seed: int) -> pd.DataFrame:
    mask = pd.Series(True, index=frame.index)
    if "policy_name" in frame.columns and policy_names:
        names = frame["policy_name"].astype(str)
        mask &= names.eq("") | names.isin(policy_names)
    model_id = pick_series(frame, ["model_id", "model_name"]).astype(str)
    hmm_model_id = pick_series(frame, ["hmm_model_id"]).astype(str)
    hmm_state = pd.to_numeric(pick_series(frame, ["hmm_state", "state"]), errors="coerce")
    age = pd.to_numeric(pick_series(frame, ["market_age_sec", "market_age_seconds", "entry_age_sec", "entry_age_seconds"]), errors="coerce")
    selected_ask = pd.to_numeric(pick_series(frame, ["selected_ask", "entry_ask", "ask_price", "executable_ask"]), errors="coerce")
    mask &= model_id.eq(REQUIRED_PROBABILITY_MODEL_ID)
    mask &= hmm_model_id.eq(DEFAULT_HMM_MODEL_ID)
    mask &= hmm_state.eq(3)
    mask &= age.ge(60.0) & age.le(240.0)
    mask &= selected_ask.gt(0.30) & selected_ask.lt(0.47)
    focused_raw = frame[mask].copy()
    if "row_id" not in focused_raw.columns and "parity_row_id" not in focused_raw.columns:
        focused_raw["parity_row_id"] = focused_raw.index.astype(str)
    if focused_raw.empty:
        raise ValueError("no focused canary replay rows found after applying state/model/age/ask filters")
    if sample_size > 0 and len(focused_raw) > sample_size:
        focused_raw = focused_raw.sample(n=sample_size, random_state=seed).sort_index(kind="mergesort")
    return canonicalize_rows(focused_raw).reset_index(drop=True)


def align_rebuilt_rows(replay: pd.DataFrame, rebuilt_source: Optional[pd.DataFrame]) -> pd.DataFrame:
    if rebuilt_source is None:
        return replay.copy()
    rebuilt = canonicalize_rows(rebuilt_source)
    if "row_id" not in rebuilt.columns:
        raise ValueError("rebuilt input rows must include row_id or parity_row_id")
    deduped = rebuilt.drop_duplicates("row_id", keep="last")
    joined = replay[["row_id"]].merge(deduped, on="row_id", how="left", suffixes=("", "_rebuilt"))
    if joined.isna().all(axis=1).any():
        missing = joined.loc[joined.isna().all(axis=1), "row_id"].astype(str).tolist()
        raise ValueError(f"rebuilt input missing sampled row_id values: {missing[:10]}")
    missing_mask = joined["market_id"].isna()
    if missing_mask.any():
        missing = joined.loc[missing_mask, "row_id"].astype(str).tolist()
        raise ValueError(f"rebuilt input missing sampled row_id values: {missing[:10]}")
    return joined


def decision_from_canonical(row: pd.Series) -> dict[str, Any]:
    config = CanaryConfig(
        min_edge=float(row.get("edge_threshold") or 0.0),
        canary_stake_usd=1.0,
        hmm_model_version=none_if_nan(row.get("hmm_model_version")),
        hmm_artifact_path=none_if_nan(row.get("hmm_artifact_path")),
        probability_model_version=none_if_nan(row.get("model_version")),
        probability_model_artifact_path=none_if_nan(row.get("probability_model_artifact_path")),
    )
    market = {
        "market_id": none_if_nan(row.get("market_id")),
        "condition_id": none_if_nan(row.get("condition_id")),
        "market_start_ts": none_if_nan(row.get("market_start_ts")),
        "market_age_sec": row.get("market_age_sec"),
    }
    quote = {
        "valid_topbook": bool(row.get("valid_topbook", True)),
        "quote_ts": none_if_nan(row.get("quote_ts")) or none_if_nan(row.get("decision_ts")),
        "quote_age_ms": row.get("quote_age_ms"),
        "yes_ask": row.get("yes_ask"),
        "no_ask": row.get("no_ask"),
    }
    prediction = {
        "model_id": none_if_nan(row.get("model_id")),
        "model_p_yes": row.get("model_p_yes"),
        "model_p_no": row.get("model_p_no"),
        "model_version": none_if_nan(row.get("model_version")),
        "artifact_path": none_if_nan(row.get("probability_model_artifact_path")),
        "probability_formula": none_if_nan(row.get("probability_formula")),
        "probability_replay_convention": none_if_nan(row.get("probability_replay_convention")),
    }
    hmm_state = None
    if not _is_missing(row.get("hmm_model_id")) or not _is_missing(row.get("hmm_state")):
        hmm_state = {
            "hmm_model_id": none_if_nan(row.get("hmm_model_id")),
            "hmm_state": row.get("hmm_state"),
            "hmm_pmax": row.get("hmm_pmax"),
            "hmm_model_version": none_if_nan(row.get("hmm_model_version")),
            "hmm_artifact_path": none_if_nan(row.get("hmm_artifact_path")),
        }
    return evaluate_canary_policy(
        market=market,
        quote=quote,
        predictions=prediction,
        hmm_state=hmm_state,
        risk_state={"open_positions": 0, "daily_loss_usd": 0.0},
        config=config,
        decision_ts=none_if_nan(row.get("decision_ts")),
    )


def expected_from_replay(row: pd.Series) -> dict[str, Any]:
    out = {field: none_if_nan(row.get(field)) for field in COMPARE_FIELDS}
    if _is_missing(out.get("final_decision")) and out.get("selected_side") in {"YES", "NO"}:
        out["final_decision"] = f"BUY_{out['selected_side']}"
    return out


def rebuilt_from_decision(row: pd.Series) -> dict[str, Any]:
    decision = decision_from_canonical(row)
    return {field: none_if_nan(decision.get(field)) for field in COMPARE_FIELDS}


def compare_rows(replay: pd.DataFrame, rebuilt: pd.DataFrame, tolerances: dict[str, float]) -> pd.DataFrame:
    diagnostics = []
    for idx, replay_row in replay.iterrows():
        rebuilt_row = rebuilt.iloc[idx]
        expected = expected_from_replay(replay_row)
        actual = rebuilt_from_decision(rebuilt_row)
        for field in COMPARE_FIELDS:
            replay_value = expected.get(field)
            rebuilt_value = actual.get(field)
            if field in OPTIONAL_REPLAY_FIELDS and _is_missing(replay_value):
                continue
            ok, abs_diff = values_match(field, replay_value, rebuilt_value, tolerances)
            if ok:
                continue
            diagnostics.append(
                {
                    "row_id": replay_row["row_id"],
                    "market_id": replay_row.get("market_id"),
                    "field_name": field,
                    "replay_value": replay_value,
                    "rebuilt_value": rebuilt_value,
                    "absolute_difference": abs_diff,
                    "mismatch_category": mismatch_category(field),
                    "fatal": field in FATAL_FIELDS or field in NUMERIC_FIELDS,
                }
            )
    return pd.DataFrame(diagnostics)


def values_match(field: str, left: Any, right: Any, tolerances: dict[str, float]) -> tuple[bool, Optional[float]]:
    if field in NUMERIC_FIELDS:
        lval = to_float(left)
        rval = to_float(right)
        if lval is None and rval is None:
            return True, None
        if lval is None or rval is None:
            return False, None
        diff = abs(lval - rval)
        return diff <= tolerances[NUMERIC_FIELDS[field]], diff
    if field in {"market_start_ts", "decision_ts"}:
        return normalize_ts(left) == normalize_ts(right), None
    return normalize_scalar(left) == normalize_scalar(right), None


def mismatch_category(field: str) -> str:
    if field.startswith("hmm_"):
        return "hmm_convention"
    if field.startswith("model_"):
        return "probability_convention"
    if field == "market_age_sec":
        return "market_age_convention"
    if field in {"yes_ask", "no_ask", "selected_ask"}:
        return "quote_convention"
    if field in {"selected_side", "selected_edge", "final_decision", "abstain_reason"}:
        return "policy_decision"
    return "identity"


def write_diagnostics(output_dir: Path, diagnostics: pd.DataFrame, summary: dict[str, Any]) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "parity_summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    diagnostics.to_csv(output_dir / "parity_diagnostics.csv", index=False)
    with (output_dir / "parity_diagnostics.jsonl").open("w", encoding="utf-8") as handle:
        for row in diagnostics.to_dict("records"):
            handle.write(json.dumps(row, sort_keys=True, default=str) + "\n")
    (output_dir / "README.txt").write_text(render_readme(summary), encoding="utf-8")


def render_readme(summary: dict[str, Any]) -> str:
    return f"""BTC-5M canary model-parity check

Policy: {POLICY_ID}

Required components:
- HMM model id: {DEFAULT_HMM_MODEL_ID}
- HMM allowed state: 3
- probability model id: {REQUIRED_PROBABILITY_MODEL_ID}
- market age: 60 <= market_age_sec <= 240
- ask filter: 0.30 < ask < 0.47

Inputs:
- replay_path={summary.get('replay_path')}
- rebuilt_input_path={summary.get('rebuilt_input_path')}

Tolerances:
- probability tolerance={summary.get('prob_tol')}
- market age tolerance seconds={summary.get('age_tol_sec')}
- ask tolerance={summary.get('ask_tol')}
- edge tolerance={summary.get('edge_tol')}

Result:
- sampled_rows={summary.get('sampled_rows')}
- mismatch_count={summary.get('mismatch_count')}
- fatal_mismatch_count={summary.get('fatal_mismatch_count')}
- passed={summary.get('passed')}

Live shadow mode should not be trusted unless this check passes with zero fatal mismatches.
"""


def run(args: argparse.Namespace) -> dict[str, Any]:
    replay_raw = read_frame(Path(args.replay_path))
    rebuilt_raw = read_frame(Path(args.rebuilt_input_path)) if args.rebuilt_input_path else None
    replay = sample_focused_rows(
        replay_raw,
        policy_names=parse_csv(args.policy_names),
        sample_size=int(args.sample_size),
        seed=int(args.seed),
    )
    rebuilt = align_rebuilt_rows(replay, rebuilt_raw)
    tolerances = {
        "prob_tol": float(args.prob_tol),
        "age_tol_sec": float(args.age_tol_sec),
        "ask_tol": float(args.ask_tol),
        "edge_tol": float(args.edge_tol),
    }
    diagnostics = compare_rows(replay, rebuilt, tolerances)
    fatal_count = int(diagnostics["fatal"].sum()) if not diagnostics.empty else 0
    summary = {
        "policy_id": POLICY_ID,
        "replay_path": str(args.replay_path),
        "rebuilt_input_path": str(args.rebuilt_input_path) if args.rebuilt_input_path else None,
        "sampled_rows": int(len(replay)),
        "mismatch_count": int(len(diagnostics)),
        "fatal_mismatch_count": fatal_count,
        "passed": fatal_count == 0 and diagnostics.empty,
        **tolerances,
    }
    write_diagnostics(Path(args.output_dir), diagnostics, summary)
    if diagnostics.empty:
        print(f"parity passed sampled_rows={len(replay)} output_dir={args.output_dir}")
    else:
        print(
            f"parity failed sampled_rows={len(replay)} mismatches={len(diagnostics)} fatal={fatal_count} "
            f"output_dir={args.output_dir}",
            file=sys.stderr,
        )
    return summary


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Check BTC-5M canary live-input parity against historical replay rows.")
    parser.add_argument("--replay-path", default=str(DEFAULT_REPLAY_PATH), help="Replay/reference trade rows with canary input columns.")
    parser.add_argument("--rebuilt-input-path", default=None, help="Optional live-builder output rows keyed by row_id/parity_row_id.")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--policy-names", default=DEFAULT_POLICY_NAMES)
    parser.add_argument("--sample-size", type=int, default=1000, help="0 means use all focused rows.")
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--prob-tol", type=float, default=1e-9)
    parser.add_argument("--age-tol-sec", type=float, default=1.0)
    parser.add_argument("--ask-tol", type=float, default=1e-9)
    parser.add_argument("--edge-tol", type=float, default=1e-9)
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    try:
        summary = run(build_parser().parse_args(argv))
    except Exception as exc:
        print(f"parity check failed: {exc}", file=sys.stderr)
        return 2
    return 0 if summary["passed"] else 1


def to_float(value: Any) -> Optional[float]:
    if _is_missing(value):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def to_int(value: Any) -> Optional[int]:
    if _is_missing(value):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def none_if_nan(value: Any) -> Any:
    return None if _is_missing(value) else value


def _is_missing(value: Any) -> bool:
    if value is None:
        return True
    try:
        return bool(pd.isna(value))
    except (TypeError, ValueError):
        return False


def normalize_side(value: Any) -> Optional[str]:
    if _is_missing(value):
        return None
    text = str(value).strip().upper()
    if text in {"BUY_YES", "YES"}:
        return "YES"
    if text in {"BUY_NO", "NO"}:
        return "NO"
    return text or None


def normalize_reason(value: Any) -> Optional[str]:
    if _is_missing(value) or str(value) in {"", "None", "nan"}:
        return None
    return str(value)


def _looks_like_market_key_alias(market_id: Any, market_key: Any, condition_id: Any) -> bool:
    if not _is_missing(condition_id):
        return False
    left = to_float(market_id)
    right = to_float(market_key)
    if left is None or right is None:
        return False
    return left == right


def normalize_scalar(value: Any) -> Optional[str]:
    if _is_missing(value):
        return None
    if isinstance(value, (np.integer, int)):
        return str(int(value))
    if isinstance(value, (np.floating, float)):
        return str(float(value))
    return str(value)


def normalize_ts(value: Any) -> Optional[str]:
    if _is_missing(value):
        return None
    ts = pd.to_datetime(value, utc=True, errors="coerce")
    if pd.isna(ts):
        return str(value)
    return ts.isoformat()


def iso_text(value: Any) -> Any:
    if _is_missing(value):
        return None
    ts = pd.to_datetime(value, utc=True, errors="coerce")
    if pd.isna(ts):
        return str(value)
    return ts.isoformat()


if __name__ == "__main__":
    raise SystemExit(main())
