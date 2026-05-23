from __future__ import annotations

import os
from pathlib import Path
from typing import Optional


def load_env_file(path: str | Path = ".env", *, override: bool = False) -> dict[str, str]:
    target = Path(path)
    loaded: dict[str, str] = {}
    if not target.exists():
        return loaded
    for raw_line in target.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :].strip()
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if not key:
            continue
        parsed = _parse_env_value(value.strip())
        if override or key not in os.environ:
            os.environ[key] = parsed
            loaded[key] = parsed
    return loaded


def load_default_env_file(env_var: str = "BTC5M_ENV_FILE") -> dict[str, str]:
    return load_env_file(os.environ.get(env_var, ".env"), override=False)


def _parse_env_value(value: str) -> str:
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        value = value[1:-1]
    if " #" in value:
        value = value.split(" #", 1)[0].rstrip()
    return value
