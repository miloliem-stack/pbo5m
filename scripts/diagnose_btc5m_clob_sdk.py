#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.runtime.btc5m_canary_execution import PyClobClientAdapter, clob_sdk_metadata, import_clob_v2_sdk  # noqa: E402
from src.runtime.env_file import load_env_file  # noqa: E402


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Print redacted BTC-5m CLOB V2 SDK diagnostics.")
    parser.add_argument("--env-file", type=Path, help="Optional env file to load before diagnostics.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    loaded: dict[str, str] = {}
    if args.env_file:
        loaded = load_env_file(args.env_file, required=True)
    out: dict[str, object] = {"loaded_env_keys": sorted(loaded), **clob_sdk_metadata()}
    try:
        import_clob_v2_sdk()
        out["clob_v2_import_ok"] = True
    except Exception as exc:
        out["clob_v2_import_ok"] = False
        out["error"] = str(exc)
        print(json.dumps(out, indent=2, sort_keys=True))
        return 1
    try:
        adapter = PyClobClientAdapter()
        out["adapter_config"] = adapter.redacted_adapter_config()
    except Exception as exc:
        out["adapter_init_ok"] = False
        out["adapter_error"] = str(exc)
        print(json.dumps(out, indent=2, sort_keys=True))
        return 1
    out["adapter_init_ok"] = True
    print(json.dumps(out, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
