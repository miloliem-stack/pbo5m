#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.runtime.btc5m_brownian_conservative import BrownianConservativeConfig
from src.runtime.env_file import load_env_file


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Diagnose BTC-5m Brownian venue minimum sizing config without sending orders.")
    parser.add_argument("--env-file", type=Path)
    parser.add_argument("--bankroll", type=float, default=None)
    args = parser.parse_args(argv)
    if args.env_file:
        load_env_file(args.env_file, required=True)
    cfg = BrownianConservativeConfig.from_env()
    bankroll = args.bankroll if args.bankroll is not None else float(os.environ.get("BTC5M_BROWNIAN_BANKROLL_USD", "0") or 0)
    max_fraction = cfg.normal_max_stake_fraction
    print(
        json.dumps(
            {
                "strategy_id": cfg.strategy_id,
                "venue_min_discovery_mode": cfg.venue_min_discovery_mode,
                "min_market_buy_notional_usd": cfg.min_market_buy_notional_usd,
                "min_limit_buy_size_shares": cfg.min_limit_buy_size_shares,
                "max_stake_fraction": max_fraction,
                "computed_small_wallet_threshold": cfg.min_market_buy_notional_usd / max_fraction if max_fraction else None,
                "configured_bankroll": bankroll,
                "max_risk_capped_stake_for_bankroll": bankroll * max_fraction,
                "bankroll_can_naturally_place_min_market_buy": bankroll * max_fraction >= cfg.min_market_buy_notional_usd,
                "sends_live_order": False,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
