from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Optional

from .polymarket_funder_setup import (
    DECIMALS,
    PUSD_TOKEN_ADDRESS,
    PolymarketFunderConfig,
    checksum,
    from_units,
    make_web3,
    read_erc1155_approval,
    read_erc20_balance,
)


POLY_CTF_CONTRACT_ADDRESS = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
CTF_COLLATERAL_ADAPTER_ADDRESS = "0xAdA100Db00Ca00073811820692005400218FcE1f"
ZERO_PARENT_COLLECTION_ID = "0x" + "00" * 32
REDEEM_POSITIONS_SIGNATURE = "redeemPositions(address,bytes32,bytes32,uint256[])"
REDEEM_POSITIONS_SELECTOR = "0x01b7037c"

ERC1155_ABI = [
    {
        "inputs": [{"name": "account", "type": "address"}, {"name": "id", "type": "uint256"}],
        "name": "balanceOf",
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    }
]

CTF_COLLATERAL_ADAPTER_ABI = [
    {
        "inputs": [
            {"name": "collateralToken", "type": "address"},
            {"name": "parentCollectionId", "type": "bytes32"},
            {"name": "conditionId", "type": "bytes32"},
            {"name": "indexSets", "type": "uint256[]"},
        ],
        "name": "redeemPositions",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function",
    }
]


@dataclass(frozen=True)
class RedeemConfig:
    polygon_chain_id: int = 137
    ctf_contract_address: str = POLY_CTF_CONTRACT_ADDRESS
    ctf_collateral_adapter_address: str = CTF_COLLATERAL_ADAPTER_ADDRESS
    pusd_token_address: str = PUSD_TOKEN_ADDRESS
    wait_timeout_sec: int = 120
    poll_latency_sec: int = 2

    @classmethod
    def from_env(cls, env: Optional[dict[str, str]] = None) -> "RedeemConfig":
        import os

        source = env if env is not None else os.environ
        return cls(
            polygon_chain_id=int(source.get("POLYGON_CHAIN_ID", "137")),
            ctf_contract_address=source.get("POLY_CTF_CONTRACT_ADDRESS", POLY_CTF_CONTRACT_ADDRESS),
            ctf_collateral_adapter_address=source.get("POLY_CTF_COLLATERAL_ADAPTER_ADDRESS", CTF_COLLATERAL_ADAPTER_ADDRESS),
            pusd_token_address=source.get("PUSD_TOKEN_ADDRESS", PUSD_TOKEN_ADDRESS),
            wait_timeout_sec=int(source.get("BTC5M_REDEEMER_RECEIPT_TIMEOUT_SEC", source.get("REDEEM_RECEIPT_TIMEOUT_SEC", "120"))),
            poll_latency_sec=int(source.get("BTC5M_REDEEMER_RECEIPT_POLL_SEC", source.get("REDEEM_RECEIPT_POLL_SEC", "2"))),
        )

    def redacted(self) -> dict[str, Any]:
        return {
            "polygon_chain_id": self.polygon_chain_id,
            "ctf_contract_address": self.ctf_contract_address,
            "ctf_collateral_adapter_address": self.ctf_collateral_adapter_address,
            "pusd_token_address": self.pusd_token_address,
            "wait_timeout_sec": self.wait_timeout_sec,
            "poll_latency_sec": self.poll_latency_sec,
        }


class PusdCtfRedeemAdapter:
    def __init__(self, *, funder_config: Optional[PolymarketFunderConfig] = None, redeem_config: Optional[RedeemConfig] = None, web3: Any = None) -> None:
        self.funder_config = funder_config or PolymarketFunderConfig.from_env()
        self.redeem_config = redeem_config or RedeemConfig.from_env()
        self._web3_override = web3
        self._web3_instance: Any = None

    @property
    def web3(self) -> Any:
        if self._web3_override is not None:
            return self._web3_override
        if self._web3_instance is None:
            self._web3_instance = make_web3(self.funder_config)
        return self._web3_instance
        verify_adapter_abi()
        self._verify_chain()

    def redeem_condition(self, *, condition_id: str, token_ids: list[str], index_sets: Optional[list[int]] = None) -> dict[str, Any]:
        if not condition_id:
            raise RuntimeError("condition_id_missing")
        if self.funder_config.signature_type == 3:
            raise RuntimeError("deposit_wallet_redeem_requires_relayer_not_implemented")
        if not self.funder_config.owner_private_key:
            raise RuntimeError("POLY_WALLET_PRIVATE_KEY is required for redemption")
        owner = self._owner_address()
        if self.read_ctf_redeem_adapter_approval(owner) is not True:
            raise RuntimeError("missing_ctf_redeem_adapter_approval")
        balances = self.read_outcome_balances(owner, token_ids)
        if sum(balances.values()) <= 0:
            raise RuntimeError("zero_token_balance")
        before = read_erc20_balance(self.web3, self.redeem_config.pusd_token_address, owner)
        tx_hash = self._send_redeem_tx(condition_id=condition_id, index_sets=index_sets or [1, 2])
        receipt = self.web3.eth.wait_for_transaction_receipt(
            tx_hash,
            timeout=self.redeem_config.wait_timeout_sec,
            poll_latency=self.redeem_config.poll_latency_sec,
        )
        receipt_summary = summarize_receipt(self.web3, receipt)
        if int(receipt_summary.get("status") or 0) != 1:
            return {
                "tx_hash": receipt_summary.get("transactionHash") or _to_hex(self.web3, tx_hash),
                "receipt": receipt_summary,
                "status": "failed_retryable",
                "error_code": "redeem_tx_failed",
                "burned_token_balances": balances,
                "redeemed_pusd_delta": None,
            }
        after = read_erc20_balance(self.web3, self.redeem_config.pusd_token_address, owner)
        delta = from_units((after or 0) - (before or 0)) if before is not None and after is not None else None
        return {
            "tx_hash": receipt_summary.get("transactionHash") or _to_hex(self.web3, tx_hash),
            "receipt": receipt_summary,
            "status": "confirmed",
            "burned_token_balances": balances,
            "redeemed_pusd_delta": delta,
        }

    def read_outcome_balances(self, owner: str, token_ids: list[str]) -> dict[str, float]:
        contract = self.web3.eth.contract(address=checksum(self.web3, self.redeem_config.ctf_contract_address), abi=ERC1155_ABI)
        out: dict[str, float] = {}
        for token_id in token_ids:
            if token_id in (None, ""):
                continue
            raw = int(contract.functions.balanceOf(checksum(self.web3, owner), int(token_id)).call())
            out[str(token_id)] = float(Decimal(raw) / Decimal(10**DECIMALS))
        return out

    def read_ctf_redeem_adapter_approval(self, owner: str) -> bool:
        return bool(
            read_erc1155_approval(
                self.web3,
                self.redeem_config.ctf_contract_address,
                owner,
                self.redeem_config.ctf_collateral_adapter_address,
            )
        )

    def build_redeem_function(self, *, condition_id: str, index_sets: Optional[list[int]] = None) -> Any:
        adapter = self.web3.eth.contract(address=checksum(self.web3, self.redeem_config.ctf_collateral_adapter_address), abi=CTF_COLLATERAL_ADAPTER_ABI)
        return adapter.functions.redeemPositions(
            checksum(self.web3, self.redeem_config.pusd_token_address),
            bytes.fromhex("00" * 32),
            condition_id_to_bytes32(condition_id),
            index_sets or [1, 2],
        )

    def _send_redeem_tx(self, *, condition_id: str, index_sets: list[int]) -> Any:
        account = self.web3.eth.account.from_key(self.funder_config.owner_private_key)
        fn = self.build_redeem_function(condition_id=condition_id, index_sets=index_sets)
        tx = fn.build_transaction(
            {
                "from": account.address,
                "nonce": self.web3.eth.get_transaction_count(account.address),
                "chainId": self.redeem_config.polygon_chain_id,
            }
        )
        signed = account.sign_transaction(tx)
        return self.web3.eth.send_raw_transaction(signed.rawTransaction if hasattr(signed, "rawTransaction") else signed.raw_transaction)

    def _owner_address(self) -> str:
        account = self.web3.eth.account.from_key(self.funder_config.owner_private_key)
        return account.address

    def _verify_chain(self) -> None:
        chain_id = getattr(self.web3.eth, "chain_id", None)
        if chain_id is not None and int(chain_id) != int(self.redeem_config.polygon_chain_id):
            raise RuntimeError("polygon_chain_id_mismatch")


def verify_adapter_abi() -> None:
    if REDEEM_POSITIONS_SIGNATURE != "redeemPositions(address,bytes32,bytes32,uint256[])":
        raise RuntimeError("adapter_abi_unverified")
    if REDEEM_POSITIONS_SELECTOR != "0x01b7037c":
        raise RuntimeError("adapter_abi_unverified")


def condition_id_to_bytes32(condition_id: str) -> bytes:
    value = str(condition_id or "").strip()
    if value.startswith("0x"):
        value = value[2:]
    if len(value) != 64:
        raise RuntimeError("condition_id_invalid")
    try:
        return bytes.fromhex(value)
    except ValueError as exc:
        raise RuntimeError("condition_id_invalid") from exc


def summarize_receipt(web3: Any, receipt: Any) -> dict[str, Any]:
    getter = receipt.get if isinstance(receipt, dict) else lambda key, default=None: getattr(receipt, key, default)
    tx_hash = getter("transactionHash")
    return {
        "transactionHash": _to_hex(web3, tx_hash) if tx_hash is not None else None,
        "status": getter("status"),
        "blockNumber": getter("blockNumber"),
        "gasUsed": getter("gasUsed"),
    }


def _to_hex(web3: Any, value: Any) -> str:
    if hasattr(web3, "to_hex"):
        return web3.to_hex(value)
    return web3.toHex(value)
