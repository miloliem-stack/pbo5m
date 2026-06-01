from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Optional

from src.polymarket_api import coerce_json_list, gamma_get_diagnostic

from .btc5m_pusd_redeem_adapter import POLY_CTF_CONTRACT_ADDRESS, condition_id_to_bytes32
from .polymarket_funder_setup import PolymarketFunderConfig, checksum, make_web3


def _web3_importable() -> bool:
    try:
        import web3  # noqa: F401
        return True
    except Exception:
        return False


CTF_RESOLUTION_ABI = [
    {
        "inputs": [{"name": "conditionId", "type": "bytes32"}],
        "name": "payoutDenominator",
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [{"name": "conditionId", "type": "bytes32"}, {"name": "index", "type": "uint256"}],
        "name": "payoutNumerators",
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
]


@dataclass(frozen=True)
class ResolutionResult:
    resolved: bool
    winning_side: str = "UNKNOWN"
    source: str = "gamma_ctf"
    payout_vector: Optional[list[int]] = None
    gamma_status: Optional[str] = None
    onchain_confirmed: bool = False
    error: Optional[str] = None
    diagnostics: Optional[dict[str, Any]] = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "resolved": self.resolved,
            "winning_side": self.winning_side,
            "source": self.source,
            "payout_vector": self.payout_vector,
            "gamma_status": self.gamma_status,
            "onchain_confirmed": self.onchain_confirmed,
            "error": self.error,
            "diagnostics": self.diagnostics or {},
        }


class GammaCtfResolutionSource:
    def __init__(
        self,
        *,
        env: Optional[dict[str, str]] = None,
        web3: Any = None,
        gamma_fetcher: Any = None,
        require_onchain_confirmation: Optional[bool] = None,
    ) -> None:
        self.env = env if env is not None else os.environ
        self.timeout_sec = float(self.env.get("BTC5M_RESOLUTION_GAMMA_TIMEOUT_SEC", "6"))
        self.require_onchain_confirmation = (
            _env_bool(self.env.get("BTC5M_RESOLUTION_REQUIRE_ONCHAIN_CONFIRMATION", "true"))
            if require_onchain_confirmation is None
            else bool(require_onchain_confirmation)
        )
        self.fail_open = _env_bool(self.env.get("BTC5M_RESOLUTION_FAIL_OPEN", "false"))
        self.allow_weak_gamma_mapping = _env_bool(self.env.get("BTC5M_ALLOW_WEAK_GAMMA_OUTCOME_INDEX_MAPPING", "false"))
        self.funder_config = PolymarketFunderConfig.from_env(self.env)
        self._web3_override = web3  # explicit injection (tests / callers)
        self._web3_instance: Any = None  # lazy-connected
        self.gamma_fetcher = gamma_fetcher or self._fetch_gamma_market
        self.ctf_contract_address = self.env.get("POLY_CTF_CONTRACT_ADDRESS", POLY_CTF_CONTRACT_ADDRESS)

    @property
    def web3(self) -> Any:
        if self._web3_override is not None:
            return self._web3_override
        if self._web3_instance is None:
            self._web3_instance = make_web3(self.funder_config)
        return self._web3_instance

    def diagnostics(self) -> dict[str, Any]:
        return {
            "resolution_source": "gamma_ctf",
            "require_onchain_confirmation": self.require_onchain_confirmation,
            "fail_open": self.fail_open,
            "fail_open_ignored_for_safety": True,
            "allow_weak_gamma_mapping": self.allow_weak_gamma_mapping,
            "ctf_contract_address": self.ctf_contract_address,
        }

    def resolve(self, lot: dict[str, Any]) -> dict[str, Any]:
        condition_id = str(lot.get("condition_id") or "")
        if not condition_id:
            return ResolutionResult(False, error="condition_id_missing").as_dict()
        try:
            market = self.gamma_fetcher(lot)
        except Exception as exc:
            return ResolutionResult(False, error=f"gamma_fetch_failed:{exc}").as_dict()
        gamma = infer_gamma_resolution(market or {}, allow_weak_mapping=self.allow_weak_gamma_mapping)
        if not gamma.get("resolved"):
            return ResolutionResult(False, gamma_status=gamma.get("status"), error=gamma.get("error") or "gamma_unresolved_or_ambiguous", diagnostics=gamma).as_dict()
        # Skip on-chain confirmation if POLYGON_RPC is not configured or the web3
        # package is not installed — fall back to Gamma-only resolution.
        # Operators can get full on-chain confirmation by installing web3 and setting POLYGON_RPC.
        _web3_available = bool(self.funder_config.polygon_rpc) and _web3_importable()
        if self.require_onchain_confirmation and not _web3_available:
            winning_side = gamma.get("winning_side")
            if winning_side not in {"YES", "NO"}:
                return ResolutionResult(False, gamma_status=gamma.get("status"), error="gamma_winning_side_ambiguous", diagnostics=gamma).as_dict()
            return ResolutionResult(
                True,
                winning_side=winning_side,
                gamma_status=gamma.get("status"),
                onchain_confirmed=False,
                source="gamma_only_no_rpc",
                diagnostics={"gamma": gamma},
            ).as_dict()
        try:
            onchain = self.read_ctf_payout(condition_id)
        except Exception as exc:
            return ResolutionResult(False, gamma_status=gamma.get("status"), error=f"ctf_payout_read_failed:{exc}", diagnostics=gamma).as_dict()
        if not onchain.get("resolved"):
            return ResolutionResult(
                False,
                gamma_status=gamma.get("status"),
                payout_vector=onchain.get("payout_vector"),
                onchain_confirmed=False,
                error=onchain.get("error") or "ctf_unresolved",
                diagnostics={"gamma": gamma, "ctf": onchain},
            ).as_dict()
        onchain_side = side_from_winning_index(int(onchain["winning_index"]), gamma.get("side_by_index") or {})
        if onchain_side not in {"YES", "NO"}:
            return ResolutionResult(False, gamma_status=gamma.get("status"), payout_vector=onchain.get("payout_vector"), error="ctf_gamma_side_mapping_ambiguous", diagnostics={"gamma": gamma, "ctf": onchain}).as_dict()
        if onchain_side != gamma.get("winning_side"):
            return ResolutionResult(False, gamma_status=gamma.get("status"), payout_vector=onchain.get("payout_vector"), error="gamma_ctf_winner_disagreement", diagnostics={"gamma": gamma, "ctf": onchain}).as_dict()
        return ResolutionResult(
            True,
            winning_side=onchain_side,
            gamma_status=gamma.get("status"),
            payout_vector=onchain.get("payout_vector"),
            onchain_confirmed=True,
            diagnostics={"gamma": gamma, "ctf": onchain},
        ).as_dict()

    def read_ctf_payout(self, condition_id: str) -> dict[str, Any]:
        contract = self.web3.eth.contract(address=checksum(self.web3, self.ctf_contract_address), abi=CTF_RESOLUTION_ABI)
        cid = condition_id_to_bytes32(condition_id)
        denominator = int(contract.functions.payoutDenominator(cid).call())
        nums = [int(contract.functions.payoutNumerators(cid, idx).call()) for idx in (0, 1)]
        if denominator <= 0:
            return {"resolved": False, "denominator": denominator, "payout_vector": nums, "error": "ctf_unresolved"}
        winning = [idx for idx, value in enumerate(nums) if value == denominator]
        losing = [idx for idx, value in enumerate(nums) if value == 0]
        if len(winning) != 1 or len(losing) != 1:
            return {"resolved": False, "denominator": denominator, "payout_vector": nums, "error": "ctf_payout_ambiguous"}
        return {"resolved": True, "denominator": denominator, "payout_vector": nums, "winning_index": winning[0]}

    def _fetch_gamma_market(self, lot: dict[str, Any]) -> dict[str, Any]:
        market_id = lot.get("market_id")
        if market_id not in (None, ""):
            diag = gamma_get_diagnostic(f"/markets/{market_id}", timeout=self.timeout_sec)
            if diag.get("ok"):
                payload = diag.get("payload")
                if isinstance(payload, dict):
                    return payload
        condition_id = lot.get("condition_id")
        if condition_id not in (None, ""):
            diag = gamma_get_diagnostic("/markets", params={"condition_ids": condition_id}, timeout=self.timeout_sec)
            if diag.get("ok"):
                payload = diag.get("payload")
                if isinstance(payload, list) and payload and isinstance(payload[0], dict):
                    return payload[0]
                if isinstance(payload, dict):
                    items = payload.get("markets") or payload.get("data")
                    if isinstance(items, list) and items and isinstance(items[0], dict):
                        return items[0]
        raise RuntimeError("gamma_market_not_found")


class UnavailableResolutionSource:
    def diagnostics(self) -> dict[str, Any]:
        return {"resolution_source": "unavailable"}

    def resolve(self, lot: dict[str, Any]) -> dict[str, Any]:
        return ResolutionResult(False, source="unresolved_source_unavailable", error="unresolved_source_unavailable").as_dict()


def build_resolution_source(*, env: Optional[dict[str, str]] = None, allow_unavailable: bool = False) -> Any:
    source = (env or os.environ).get("BTC5M_RESOLUTION_SOURCE", "gamma_ctf").strip().lower()
    if source == "gamma_ctf":
        return GammaCtfResolutionSource(env=env)
    if source == "unavailable" and allow_unavailable:
        return UnavailableResolutionSource()
    raise RuntimeError("resolution_source_unavailable")


def infer_gamma_resolution(market: dict[str, Any], *, allow_weak_mapping: bool = False) -> dict[str, Any]:
    status = normalize_status(market)
    mapping = side_by_outcome_index(market, allow_weak_mapping=allow_weak_mapping)
    side_by_index = mapping["side_by_index"]
    winning_side = infer_gamma_winning_side(market, side_by_index)
    weak_mapping = bool(mapping["weak"])
    error = None
    if weak_mapping and not allow_weak_mapping:
        error = "gamma_outcome_label_mapping_weak"
    elif not (status in {"closed", "resolved", "settled", "finalized", "redeemed"} and winning_side in {"YES", "NO"}):
        error = "gamma_unresolved_or_ambiguous"
    resolved = error is None
    return {
        "resolved": resolved,
        "winning_side": winning_side,
        "status": status,
        "side_by_index": side_by_index,
        "weak_outcome_mapping": weak_mapping,
        "warnings": ["gamma_outcome_label_mapping_weak"] if weak_mapping else [],
        "error": error,
    }


def normalize_status(market: dict[str, Any]) -> str:
    raw = str(market.get("status") or market.get("state") or market.get("marketStatus") or "").strip().lower()
    if market.get("closed") is True:
        return "closed"
    if market.get("resolved") is True:
        return "resolved"
    return raw


def side_by_outcome_index(market: dict[str, Any], *, allow_weak_mapping: bool = False) -> dict[str, Any]:
    outcomes = coerce_json_list(market.get("outcomes")) or coerce_json_list(market.get("shortOutcomes")) or []
    mapping: dict[int, str] = {}
    for idx, label in enumerate(outcomes[:2]):
        side = label_to_side(label)
        if side:
            mapping[idx] = side
    weak = False
    if len(mapping) != len(outcomes[:2]) and len(outcomes) == 2:
        mapping = {0: "YES", 1: "NO"}
        weak = True
        if not allow_weak_mapping:
            return {"side_by_index": mapping, "weak": weak}
    return {"side_by_index": mapping, "weak": weak}


def infer_gamma_winning_side(market: dict[str, Any], side_by_index: dict[int, str]) -> str:
    for key in ("winningOutcome", "winner", "result", "winning_outcome"):
        side = label_to_side(market.get(key))
        if side:
            return side
    prices = coerce_json_list(market.get("outcomePrices")) or coerce_json_list(market.get("outcome_prices")) or coerce_json_list(market.get("prices")) or []
    if len(prices) >= 2:
        parsed = []
        for value in prices[:2]:
            try:
                parsed.append(float(value))
            except (TypeError, ValueError):
                parsed.append(float("nan"))
        winners = [idx for idx, value in enumerate(parsed) if value >= 0.99]
        losers = [idx for idx, value in enumerate(parsed) if value <= 0.01]
        if len(winners) == 1 and len(losers) == 1:
            return side_by_index.get(winners[0], "UNKNOWN")
    return "UNKNOWN"


def side_from_winning_index(index: int, side_by_index: dict[int, str]) -> str:
    return side_by_index.get(index, "UNKNOWN")


def label_to_side(value: Any) -> Optional[str]:
    text = str(value or "").strip().lower()
    if text in {"yes", "up", "above", "higher", "1"}:
        return "YES"
    if text in {"no", "down", "below", "lower", "0"}:
        return "NO"
    return None


def _env_bool(value: str) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}
