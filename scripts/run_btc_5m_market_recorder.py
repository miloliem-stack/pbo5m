from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.runtime.market_recorder import MarketRecorderRuntime


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the BTC-5m Polymarket market recorder")
    parser.add_argument("--output-dir", default="artifacts/market_recorder")
    parser.add_argument("--router-poll-seconds", type=float, default=10.0)
    parser.add_argument("--quote-poll-seconds", type=float, default=2.0)
    parser.add_argument("--heartbeat-seconds", type=float, default=5.0)
    parser.add_argument("--console-log-seconds", type=float, default=5.0)
    parser.add_argument("--duration-seconds", type=float)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    runtime = MarketRecorderRuntime(
        output_dir=args.output_dir,
        router_poll_seconds=args.router_poll_seconds,
        quote_poll_seconds=args.quote_poll_seconds,
        heartbeat_seconds=args.heartbeat_seconds,
        console_log_seconds=args.console_log_seconds,
    )
    report = runtime.run(duration_seconds=args.duration_seconds)
    print(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
