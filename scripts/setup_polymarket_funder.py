#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.runtime.btc5m_canary_execution import PyClobClientAdapter  # noqa: E402
from src.runtime.env_file import load_env_file  # noqa: E402
from src.runtime.polymarket_funder_setup import (  # noqa: E402
    PolymarketFunderConfig,
    approve_ctf_redeem_adapter,
    approve_erc20,
    diagnose_funder,
    make_web3,
    read_erc1155_approval,
    to_units,
    update_clob_collateral_allowance,
    validate_mode,
    wrap_usdce_to_pusd,
    write_setup_log,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Manual pUSD/funder setup and diagnostics for BTC-5m Polymarket canary.")
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    parser.add_argument("--diagnose-only", action="store_true", default=False)
    parser.add_argument("--wrap-usdce", type=str, metavar="AMOUNT")
    parser.add_argument("--approve-onramp", type=str, metavar="AMOUNT")
    parser.add_argument("--sync-clob-collateral-allowance", action="store_true")
    parser.add_argument("--approve-trading-collateral", action="store_true")
    parser.add_argument("--approve-ctf-redeem-adapter", action="store_true")
    parser.add_argument("--check-ctf-redeem-adapter-approval", action="store_true")
    parser.add_argument("--deposit-wallet-mode", action="store_true")
    parser.add_argument("--eoa-mode", action="store_true")
    parser.add_argument("--yes-i-understand-this-sends-transactions", action="store_true")
    parser.add_argument("--setup-log", type=Path, default=Path("artifacts/polymarket_funder_setup/setup_events.jsonl"))
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    load_env_file(args.env_file, required=True)
    config = PolymarketFunderConfig.from_env()
    errors = validate_mode(config, deposit_wallet_mode=args.deposit_wallet_mode, eoa_mode=args.eoa_mode)
    if errors:
        print(json.dumps({"ok": False, "errors": errors, "config": config.redacted()}, indent=2, sort_keys=True))
        return 2
    adapter = maybe_adapter()
    mutation_requested = bool(
        args.wrap_usdce
        or args.approve_onramp
        or args.sync_clob_collateral_allowance
        or args.approve_trading_collateral
        or args.approve_ctf_redeem_adapter
    )
    if args.deposit_wallet_mode and args.approve_ctf_redeem_adapter:
        print(
            json.dumps(
                {
                    "ok": False,
                    "errors": ["deposit_wallet_ctf_approval_requires_relayer"],
                    "config": config.redacted(),
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 2
    if args.deposit_wallet_mode and mutation_requested:
        print(
            json.dumps(
                {
                    "ok": False,
                    "errors": ["deposit_wallet_mutation_requires_relayer_wallet_batch_not_implemented"],
                    "config": config.redacted(),
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 2
    if mutation_requested and not args.yes_i_understand_this_sends_transactions:
        print(json.dumps({"ok": False, "errors": ["mutation_requires_confirmation_flag"], "config": config.redacted()}, indent=2, sort_keys=True))
        return 2
    diagnostic = diagnose_funder(config, clob_adapter=adapter)
    events: list[dict] = []
    if mutation_requested:
        web3 = make_web3(config)
        if args.approve_onramp:
            tx = approve_erc20(
                web3,
                config,
                token=config.usdc_e_token_address,
                spender=config.collateral_onramp_address,
                amount_units=to_units(args.approve_onramp),
            )
            events.append({"event_type": "approve_onramp_submitted", "tx_hash": tx})
        if args.wrap_usdce:
            recipient = config.effective_funder
            if not recipient:
                raise SystemExit("missing funder/owner recipient")
            tx = wrap_usdce_to_pusd(web3, config, recipient=recipient, amount_units=to_units(args.wrap_usdce))
            events.append({"event_type": "wrap_usdce_submitted", "tx_hash": tx, "recipient": recipient})
        if args.approve_trading_collateral:
            if not config.exchange_address:
                raise SystemExit("POLY_EXCHANGE_ADDRESS is required for direct EOA pUSD trading approval")
            tx = approve_erc20(
                web3,
                config,
                token=config.pusd_token_address,
                spender=config.exchange_address,
                amount_units=to_units("1000000000"),
            )
            events.append({"event_type": "approve_trading_collateral_submitted", "tx_hash": tx, "spender": config.exchange_address})
        if args.approve_ctf_redeem_adapter:
            if args.deposit_wallet_mode or config.signature_type == 3:
                raise SystemExit("deposit_wallet_ctf_approval_requires_relayer")
            tx = approve_ctf_redeem_adapter(web3, config)
            events.append(
                {
                    "event_type": "approve_ctf_redeem_adapter_submitted",
                    "tx_hash": tx,
                    "operator": config.ctf_collateral_adapter_address,
                    "ctf_contract_address": config.ctf_contract_address,
                }
            )
        if args.sync_clob_collateral_allowance:
            if adapter is None:
                raise SystemExit("CLOB adapter unavailable for update_balance_allowance")
            response = update_clob_collateral_allowance(adapter)
            events.append({"event_type": "sync_clob_collateral_allowance", "response": response})
    if args.check_ctf_redeem_adapter_approval:
        web3 = make_web3(config)
        events.append(
            {
                "event_type": "check_ctf_redeem_adapter_approval",
                "ctf_redeem_adapter_approved": read_erc1155_approval(
                    web3,
                    config.ctf_contract_address,
                    config.effective_funder,
                    config.ctf_collateral_adapter_address,
                ),
                "funder_address": config.effective_funder,
                "ctf_contract_address": config.ctf_contract_address,
                "ctf_collateral_adapter_address": config.ctf_collateral_adapter_address,
            }
        )
    out = {"ok": not diagnostic.get("errors"), "diagnostic": diagnostic, "events": events}
    write_setup_log(args.setup_log, {"config": config.redacted(), **out})
    print(json.dumps(out, indent=2, sort_keys=True, default=str))
    return 0 if out["ok"] else 1


def maybe_adapter():
    try:
        return PyClobClientAdapter()
    except Exception:
        return None


if __name__ == "__main__":
    raise SystemExit(main())
