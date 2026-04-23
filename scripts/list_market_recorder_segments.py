from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.runtime.market_recorder_segments import list_market_recorder_segments


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="List active and closed market recorder segments")
    parser.add_argument("--output-dir", default="artifacts/market_recorder")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report = list_market_recorder_segments(args.output_dir)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
