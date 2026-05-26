#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import stat
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.runtime.btc5m_canary_execution import PyClobClientAdapter, clob_sdk_metadata, first_non_placeholder  # noqa: E402
from src.runtime.env_file import load_env_file  # noqa: E402


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Diagnose or one-time bootstrap Polymarket CLOB L2 API credentials.")
    parser.add_argument("--env-file", type=Path, help="Env file to load first.")
    parser.add_argument("--bootstrap-api-key", action="store_true", help="Explicitly create/derive a CLOB API key once.")
    parser.add_argument("--output-file", type=Path, help="Write bootstrapped credentials to this local file with 0600 permissions.")
    parser.add_argument("--print-generated", action="store_true", help="Print generated credentials to stdout. Dangerous; prefer --output-file.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    loaded: dict[str, str] = {}
    if args.env_file:
        loaded = load_env_file(args.env_file, required=True)
    adapter = PyClobClientAdapter()
    config = adapter.redacted_adapter_config()
    present = {
        "POLY_API_KEY": bool(first_non_placeholder(os.environ, "POLY_API_KEY", "CLOB_API_KEY")),
        "POLY_API_SECRET": bool(first_non_placeholder(os.environ, "POLY_API_SECRET", "CLOB_SECRET")),
        "POLY_API_PASSPHRASE": bool(first_non_placeholder(os.environ, "POLY_API_PASSPHRASE", "CLOB_PASS_PHRASE", "CLOB_PASSPHRASE")),
    }
    out: dict[str, Any] = {
        "loaded_env_keys": sorted(loaded),
        **clob_sdk_metadata(),
        "adapter_config": config,
        "l2_credentials_present": all(present.values()),
        "l2_credential_fields_present": present,
    }
    if not args.bootstrap_api_key:
        print(json.dumps(out, indent=2, sort_keys=True))
        return 0 if out["l2_credentials_present"] else 1

    creds = adapter.bootstrap_api_creds_once()
    payload = credentials_to_env_payload(creds)
    out["bootstrap_attempted"] = True
    out["generated_fields"] = sorted(payload)
    if args.output_file:
        write_secret_env(args.output_file, payload)
        out["output_file"] = str(args.output_file)
        out["output_file_mode"] = "0600"
    if args.print_generated:
        out["generated_credentials"] = payload
    elif not args.output_file:
        out["warning"] = "Credentials generated but not printed or written. Use --output-file or --print-generated explicitly."
    print(json.dumps(out, indent=2, sort_keys=True))
    return 0


def credentials_to_env_payload(creds: Any) -> dict[str, str]:
    values = {
        "POLY_API_KEY": get_cred_value(creds, "api_key", "apiKey", "key"),
        "POLY_API_SECRET": get_cred_value(creds, "api_secret", "secret"),
        "POLY_API_PASSPHRASE": get_cred_value(creds, "api_passphrase", "passphrase", "pass_phrase"),
    }
    missing = [key for key, value in values.items() if not value]
    if missing:
        raise RuntimeError("bootstrap_missing_credential_fields:" + ",".join(missing))
    return {key: str(value) for key, value in values.items() if value is not None}


def get_cred_value(creds: Any, *names: str) -> Any:
    if isinstance(creds, dict):
        for name in names:
            if creds.get(name):
                return creds[name]
    for name in names:
        if hasattr(creds, name):
            value = getattr(creds, name)
            if value:
                return value
    return None


def write_secret_env(path: Path, payload: dict[str, str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = ["# Generated CLOB L2 credentials. Do not commit.\n"]
    lines.extend(f"{key}={value}\n" for key, value in payload.items())
    flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC
    fd = os.open(path, flags, 0o600)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.writelines(lines)
    finally:
        try:
            os.chmod(path, stat.S_IRUSR | stat.S_IWUSR)
        except OSError:
            pass


if __name__ == "__main__":
    raise SystemExit(main())
