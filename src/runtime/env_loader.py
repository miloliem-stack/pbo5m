from __future__ import annotations

from pathlib import Path

from .env_file import loaded_env_summary, load_env_file as _load_env_file


def load_env_file(path: str | Path, *, override: bool = False, required: bool = False) -> dict[str, str]:
    return _load_env_file(path, override=override, required=required)


def redacted_summary(loaded: dict[str, str]) -> dict[str, str]:
    return loaded_env_summary(loaded)
