from __future__ import annotations

import json
import os
from dataclasses import dataclass
from decimal import Decimal, ROUND_DOWN
from pathlib import Path
from typing import Any, Optional


USDC_E_TOKEN_ADDRESS = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
PUSD_TOKEN_ADDRESS = "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB"
COLLATERAL_ONRAMP_ADDRESS = "0x93070a847efEf7F70739046A929D47a521F5B8ee"
COLLATERAL_OFFRAMP_ADDRESS = "0x0000000000000000000000000000000000000000"
DECIMALS = 6
MAX_UINT256 = (1 << 256) - 1

ERC20_ABI = [
    {
        "constant": True,
        "inputs": [{"name": "owner", "type": "address"}],
        "name": "balanceOf",
        "outputs": [{"name": "", "type": "uint256"}],
        "type": "function",
    },
    {
        "constant": True,
        "inputs": [{"name": "owner", "type": "address"}, {"name": "spender", "type": "address"}],
        "name": "allowance",
        "outputs": [{"name": "", "type": "uint256"}],
        "type": "function",
    },
    {
        "constant": False,
        "inputs": [{"name": "spender", "type": "address"}, {"name": "amount", "type": "uint256"}],
        "name": "approve",
        "outputs": [{"name": "", "type": "bool"}],
        "type": "function",
    },
]

ONRAMP_ABI = [
    {
        "inputs": [
            {"name": "_collateral", "type": "address"},
            {"name": "_to", "type": "address"},
            {"name": "_amount", "type": "uint256"},
        ],
        "name": "wrap",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function",
    }
]


@dataclass(frozen=True)
class PolymarketFunderConfig:
    polygon_chain_id: int = 137
    polygon_rpc: Optional[str] = None
    clob_base: str = "https://clob.polymarket.com"
    signature_type: int = 0
    owner_private_key: Optional[str] = None
    owner_address: Optional[str] = None
    funder: Optional[str] = None
    usdc_e_token_address: str = USDC_E_TOKEN_ADDRESS
    pusd_token_address: str = PUSD_TOKEN_ADDRESS
    collateral_onramp_address: str = COLLATERAL_ONRAMP_ADDRESS
    collateral_offramp_address: str = COLLATERAL_OFFRAMP_ADDRESS
    exchange_address: Optional[str] = None
    neg_risk_exchange_address: Optional[str] = None
    relayer_url: Optional[str] = None

    @classmethod
    def from_env(cls, env: Optional[dict[str, str]] = None) -> "PolymarketFunderConfig":
        source = env if env is not None else os.environ
        return cls(
            polygon_chain_id=int(source.get("POLYGON_CHAIN_ID", "137")),
            polygon_rpc=source.get("POLYGON_RPC"),
            clob_base=source.get("POLY_CLOB_BASE", "https://clob.polymarket.com"),
            signature_type=int(source.get("POLY_SIGNATURE_TYPE", "0")),
            owner_private_key=source.get("POLY_WALLET_PRIVATE_KEY"),
            owner_address=source.get("POLY_WALLET_ADDRESS") or source.get("POLY_ADDRESS"),
            funder=source.get("POLY_FUNDER"),
            usdc_e_token_address=source.get("USDC_E_TOKEN_ADDRESS", USDC_E_TOKEN_ADDRESS),
            pusd_token_address=source.get("PUSD_TOKEN_ADDRESS", PUSD_TOKEN_ADDRESS),
            collateral_onramp_address=source.get("POLY_COLLATERAL_ONRAMP_ADDRESS", COLLATERAL_ONRAMP_ADDRESS),
            collateral_offramp_address=source.get("POLY_COLLATERAL_OFFRAMP_ADDRESS", COLLATERAL_OFFRAMP_ADDRESS),
            exchange_address=source.get("POLY_EXCHANGE_ADDRESS"),
            neg_risk_exchange_address=source.get("POLY_NEG_RISK_EXCHANGE_ADDRESS"),
            relayer_url=source.get("POLY_RELAYER_URL"),
        )

    @property
    def mode(self) -> str:
        return "deposit_wallet" if self.signature_type == 3 else "eoa"

    @property
    def effective_funder(self) -> Optional[str]:
        return self.funder or self.owner_address

    def redacted(self) -> dict[str, Any]:
        return {
            "polygon_chain_id": self.polygon_chain_id,
            "polygon_rpc_set": bool(self.polygon_rpc),
            "clob_base": self.clob_base,
            "signature_type": self.signature_type,
            "mode": self.mode,
            "owner_address": self.owner_address,
            "funder": self.effective_funder,
            "funder_source": "POLY_FUNDER" if self.funder else "owner_wallet",
            "usdc_e_token_address": self.usdc_e_token_address,
            "pusd_token_address": self.pusd_token_address,
            "collateral_onramp_address": self.collateral_onramp_address,
            "collateral_offramp_address": self.collateral_offramp_address,
            "exchange_address_set": bool(self.exchange_address),
            "neg_risk_exchange_address_set": bool(self.neg_risk_exchange_address),
            "relayer_url_set": bool(self.relayer_url),
        }


def to_units(amount: str | float | Decimal) -> int:
    value = Decimal(str(amount)).quantize(Decimal("0.000001"), rounding=ROUND_DOWN)
    if value < 0:
        raise ValueError("amount must be non-negative")
    return int(value * Decimal(10**DECIMALS))


def from_units(amount: int) -> float:
    return float(Decimal(int(amount)) / Decimal(10**DECIMALS))


def require_web3():
    try:
        from web3 import Web3
    except Exception as exc:  # pragma: no cover - optional runtime dependency
        raise RuntimeError("web3 is required for on-chain pUSD setup diagnostics") from exc
    return Web3


def _load_poa_middleware() -> Any:
    candidates = (
        ("web3.middleware", "ExtraDataToPOAMiddleware"),
        ("web3.middleware.proof_of_authority", "ExtraDataToPOAMiddleware"),
        ("web3.middleware", "geth_poa_middleware"),
    )
    for module_name, attr_name in candidates:
        try:
            module = __import__(module_name, fromlist=[attr_name])
            return getattr(module, attr_name)
        except Exception:
            continue
    return None


def inject_polygon_poa_middleware(web3: Any) -> bool:
    """Install Web3's Polygon/POA extraData middleware when available."""
    middleware = _load_poa_middleware()
    onion = getattr(web3, "middleware_onion", None)
    if middleware is None or onion is None:
        return False
    try:
        onion.inject(middleware, layer=0)
        return True
    except ValueError:
        # Already installed in some Web3 versions/configurations.
        return True
    except TypeError:
        if hasattr(onion, "add"):
            onion.add(middleware)
            return True
    return False


def make_web3(config: PolymarketFunderConfig):
    if not config.polygon_rpc:
        raise RuntimeError("POLYGON_RPC is required for on-chain pUSD setup diagnostics")
    Web3 = require_web3()
    web3 = Web3(Web3.HTTPProvider(config.polygon_rpc))
    inject_polygon_poa_middleware(web3)
    if hasattr(web3, "is_connected") and not web3.is_connected():
        raise RuntimeError("polygon_rpc_not_connected")
    return web3


def checksum(web3: Any, address: str) -> str:
    if hasattr(web3, "to_checksum_address"):
        return web3.to_checksum_address(address)
    return web3.toChecksumAddress(address)


def erc20_contract(web3: Any, address: str):
    return web3.eth.contract(address=checksum(web3, address), abi=ERC20_ABI)


def onramp_contract(web3: Any, address: str):
    return web3.eth.contract(address=checksum(web3, address), abi=ONRAMP_ABI)


def read_erc20_balance(web3: Any, token: str, owner: Optional[str]) -> Optional[int]:
    if not owner:
        return None
    return int(erc20_contract(web3, token).functions.balanceOf(checksum(web3, owner)).call())


def read_erc20_allowance(web3: Any, token: str, owner: Optional[str], spender: Optional[str]) -> Optional[int]:
    if not owner or not spender:
        return None
    return int(erc20_contract(web3, token).functions.allowance(checksum(web3, owner), checksum(web3, spender)).call())


def validate_mode(config: PolymarketFunderConfig, *, deposit_wallet_mode: bool = False, eoa_mode: bool = False) -> list[str]:
    errors: list[str] = []
    if deposit_wallet_mode and config.signature_type != 3:
        errors.append("deposit_wallet_mode_requires_signature_type_3")
    if deposit_wallet_mode and not config.funder:
        errors.append("missing_deposit_wallet_funder")
    if eoa_mode and config.signature_type == 3:
        errors.append("eoa_mode_rejects_signature_type_3")
    return errors


def diagnose_funder(config: PolymarketFunderConfig, *, clob_adapter: Any = None, web3: Any = None) -> dict[str, Any]:
    row = {"config": config.redacted(), "errors": []}
    try:
        web3 = web3 or make_web3(config)
        row.update(
            {
                "owner_wallet_address": config.owner_address,
                "funder_address": config.effective_funder,
                "signature_type": config.signature_type,
                "detected_mode": config.mode,
                "usdce_balance_owner": from_units(read_erc20_balance(web3, config.usdc_e_token_address, config.owner_address) or 0),
                "pusd_balance_owner": from_units(read_erc20_balance(web3, config.pusd_token_address, config.owner_address) or 0),
                "pusd_balance_funder": from_units(read_erc20_balance(web3, config.pusd_token_address, config.effective_funder) or 0),
                "usdce_allowance_owner_to_onramp": from_units(
                    read_erc20_allowance(web3, config.usdc_e_token_address, config.owner_address, config.collateral_onramp_address) or 0
                ),
                "pusd_allowance_funder_to_exchange": (
                    from_units(read_erc20_allowance(web3, config.pusd_token_address, config.effective_funder, config.exchange_address) or 0)
                    if config.exchange_address
                    else None
                ),
                "pusd_allowance_funder_to_neg_risk_exchange": (
                    from_units(read_erc20_allowance(web3, config.pusd_token_address, config.effective_funder, config.neg_risk_exchange_address) or 0)
                    if config.neg_risk_exchange_address
                    else None
                ),
            }
        )
    except Exception as exc:
        row["errors"].append(str(exc))
    if clob_adapter is not None:
        row["clob_adapter_config"] = clob_adapter.redacted_adapter_config()
        row["clob_l2_credentials_present"] = bool(row["clob_adapter_config"].get("l2_credentials_present"))
        row["clob_update_balance_allowance_available"] = hasattr(getattr(clob_adapter, "client", None), "update_balance_allowance")
    return row


def send_contract_tx(web3: Any, config: PolymarketFunderConfig, fn: Any) -> str:
    if not config.owner_private_key:
        raise RuntimeError("POLY_WALLET_PRIVATE_KEY is required to send setup transactions")
    account = web3.eth.account.from_key(config.owner_private_key)
    tx = fn.build_transaction(
        {
            "from": account.address,
            "nonce": web3.eth.get_transaction_count(account.address),
            "chainId": config.polygon_chain_id,
        }
    )
    signed = account.sign_transaction(tx)
    tx_hash = web3.eth.send_raw_transaction(signed.rawTransaction if hasattr(signed, "rawTransaction") else signed.raw_transaction)
    return web3.to_hex(tx_hash) if hasattr(web3, "to_hex") else web3.toHex(tx_hash)


def approve_erc20(web3: Any, config: PolymarketFunderConfig, *, token: str, spender: str, amount_units: int) -> str:
    fn = erc20_contract(web3, token).functions.approve(checksum(web3, spender), int(amount_units))
    return send_contract_tx(web3, config, fn)


def wrap_usdce_to_pusd(web3: Any, config: PolymarketFunderConfig, *, recipient: str, amount_units: int) -> str:
    fn = onramp_contract(web3, config.collateral_onramp_address).functions.wrap(
        checksum(web3, config.usdc_e_token_address),
        checksum(web3, recipient),
        int(amount_units),
    )
    return send_contract_tx(web3, config, fn)


def update_clob_collateral_allowance(clob_adapter: Any) -> Any:
    client = getattr(clob_adapter, "client", clob_adapter)
    if not hasattr(client, "update_balance_allowance"):
        raise RuntimeError("clob_update_balance_allowance_unavailable")
    try:
        from py_clob_client_v2 import AssetType, BalanceAllowanceParams, SignatureTypeV2
    except ImportError:  # pragma: no cover - optional runtime dependency
        from py_clob_client_v2 import AssetType, BalanceAllowanceParams

        SignatureTypeV2 = None

    kwargs = {"asset_type": getattr(AssetType, "COLLATERAL", "COLLATERAL")}
    signature_type = getattr(getattr(clob_adapter, "adapter_config", {}), "get", lambda _key, _default=None: _default)("signature_type", None)
    mapped_signature_type = map_signature_type_for_clob_params(signature_type, SignatureTypeV2)
    if mapped_signature_type is not None:
        kwargs["signature_type"] = mapped_signature_type
    try:
        params = BalanceAllowanceParams(**kwargs)
    except TypeError:
        kwargs.pop("signature_type", None)
        params = BalanceAllowanceParams(**kwargs)
    return client.update_balance_allowance(params)


def map_signature_type_for_clob_params(value: Any, signature_type_enum: Any) -> Any:
    if value is None:
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return value
    if signature_type_enum is None:
        return parsed
    names = {0: "EOA", 1: "POLY_PROXY", 2: "GNOSIS_SAFE", 3: "POLY_1271"}
    name = names.get(parsed)
    if name and hasattr(signature_type_enum, name):
        return getattr(signature_type_enum, name)
    return parsed


def write_setup_log(path: str | Path, row: dict[str, Any]) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(redact_mapping(row), sort_keys=True, default=str) + "\n")


def redact_mapping(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: ("<redacted>" if is_secret_key(key) else redact_mapping(item)) for key, item in value.items()}
    if isinstance(value, list):
        return [redact_mapping(item) for item in value]
    return value


def is_secret_key(key: str) -> bool:
    upper = str(key).upper()
    return any(part in upper for part in ("PRIVATE_KEY", "SECRET", "PASSPHRASE", "PASSWORD"))
