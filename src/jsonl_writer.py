from __future__ import annotations

import json
from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Any

from .time_utils import isoformat_utc


def _json_default(value: Any) -> Any:
    if is_dataclass(value):
        return asdict(value)
    iso = isoformat_utc(value)
    if iso is not None and not isinstance(value, (dict, list, tuple, set)):
        return iso
    return str(value)


class JsonlWriter:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._handle = self.path.open("a", encoding="utf-8")

    def write(self, row: dict[str, Any]) -> None:
        self._handle.write(json.dumps(row, default=_json_default, sort_keys=False) + "\n")
        self._handle.flush()

    def close(self) -> None:
        self._handle.close()
