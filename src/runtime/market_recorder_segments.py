from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

from ..time_utils import isoformat_utc

RECORDER_VERSION = "hourly-segment-rotation-v1"
SEGMENT_MANIFEST = "segment_manifest.json"
SEGMENT_FILES = {
    "chainlink": "chainlink_prices.jsonl",
    "binance": "binance_prices.jsonl",
    "quotes": "market_quotes.jsonl",
    "meta": "market_meta.jsonl",
    "heartbeat": "recorder_heartbeat.jsonl",
}


def segment_start(value: datetime) -> datetime:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    value = value.astimezone(timezone.utc)
    return value.replace(minute=0, second=0, microsecond=0)


def segment_dir(root: str | Path, value: datetime) -> Path:
    normalized = segment_start(value)
    return Path(root) / normalized.strftime("%Y-%m-%d") / normalized.strftime("%H")


def build_segment_manifest(
    *,
    output_dir: str | Path,
    segment_start_utc: datetime,
    segment_end_utc: datetime,
    closed_cleanly: bool,
    row_counts: dict[str, int],
    asset: str | None = None,
) -> dict[str, Any]:
    return {
        "segment_start_utc": isoformat_utc(segment_start_utc),
        "segment_end_utc": isoformat_utc(segment_end_utc),
        "closed_cleanly": closed_cleanly,
        "row_counts": {
            SEGMENT_FILES[key]: count
            for key, count in row_counts.items()
        },
        "recorder_version": RECORDER_VERSION,
        "asset": asset,
        "output_root": str(output_dir),
    }


def list_market_recorder_segments(output_dir: str | Path) -> dict[str, Any]:
    root = Path(output_dir)
    active_segment = None
    closed_segments: list[dict[str, Any]] = []
    if not root.exists():
        return {"output_dir": str(root), "active_segment": None, "closed_segments": []}

    for current_segment_dir in sorted(iter_segment_dirs(root)):
        manifest_path = current_segment_dir / SEGMENT_MANIFEST
        file_paths = {
            filename: str(current_segment_dir / filename)
            for filename in SEGMENT_FILES.values()
        }
        segment_info = {
            "segment_dir": str(current_segment_dir),
            "manifest_path": str(manifest_path),
            "files": file_paths,
        }
        if manifest_path.exists():
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            segment_info.update(manifest)
            closed_segments.append(segment_info)
        else:
            active_segment = segment_info
    return {
        "output_dir": str(root),
        "active_segment": active_segment,
        "closed_segments": closed_segments,
    }


def iter_segment_dirs(root: Path) -> Iterator[Path]:
    for day_dir in root.iterdir():
        if not day_dir.is_dir():
            continue
        for hour_dir in day_dir.iterdir():
            if hour_dir.is_dir():
                yield hour_dir
