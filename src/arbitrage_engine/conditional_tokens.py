from __future__ import annotations

from typing import Any

from .connectors.web3_base import BaseWeb3Client
from .models import RedemptionIntentStatus, RedemptionReport, SettlementRequest, SettlementStatus

CONDITIONAL_TOKENS_ABI: list[dict[str, Any]] = [
    {
        "inputs": [{"name": "conditionId", "type": "bytes32"}],
        "name": "payoutDenominator",
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [
            {"name": "parentCollectionId", "type": "bytes32"},
            {"name": "conditionId", "type": "bytes32"},
            {"name": "indexSet", "type": "uint256"},
        ],
        "name": "getCollectionId",
        "outputs": [{"name": "", "type": "bytes32"}],
        "stateMutability": "pure",
        "type": "function",
    },
    {
        "inputs": [
            {"name": "collateralToken", "type": "address"},
            {"name": "collectionId", "type": "bytes32"},
        ],
        "name": "getPositionId",
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "pure",
        "type": "function",
    },
    {
        "inputs": [
            {"name": "account", "type": "address"},
            {"name": "id", "type": "uint256"},
        ],
        "name": "balanceOf",
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [
            {"name": "conditionId", "type": "bytes32"},
            {"name": "index", "type": "uint256"},
        ],
        "name": "payoutNumerators",
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
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
    },
]


class ConditionalTokensRedemption:
    def __init__(self, web3: BaseWeb3Client, contract_address: str, gas_limit: int) -> None:
        self._web3 = web3
        self._contract_address = contract_address
        self._gas_limit = gas_limit

    @property
    def signer_address(self) -> str | None:
        return str(self._web3.account.address) if self._web3.account is not None else None

    def checksum_address(self, value: str) -> str:
        return str(self._web3.w3.to_checksum_address(value))

    async def get_settlement_status(self, request: SettlementRequest) -> SettlementStatus:
        condition_id = _bytes32(request.condition_id)
        denominator = int(
            await self._web3.call_contract(
                self._contract_address,
                CONDITIONAL_TOKENS_ABI,
                "payoutDenominator",
                condition_id,
            )
        )
        if denominator == 0:
            return SettlementStatus.OPEN
        numerators = [
            int(
                await self._web3.call_contract(
                    self._contract_address,
                    CONDITIONAL_TOKENS_ABI,
                    "payoutNumerators",
                    condition_id,
                    index,
                )
            )
            for index in range(2)
        ]
        if all(value == 0 for value in numerators) or sum(value > 0 for value in numerators) != 1:
            return SettlementStatus.VOID
        return SettlementStatus.RESOLVED

    async def redeem_position(self, request: SettlementRequest, redemption_id: str) -> RedemptionReport:
        del redemption_id
        if self._web3.account is None:
            return RedemptionReport(RedemptionIntentStatus.MANUAL_REVIEW, error="signing key is unavailable")
        try:
            transaction = await self._web3.build_contract_transaction(
                self._contract_address,
                CONDITIONAL_TOKENS_ABI,
                "redeemPositions",
                (
                    self._web3.w3.to_checksum_address(request.collateral_token),
                    bytes(32),
                    _bytes32(request.condition_id),
                    list(request.index_sets),
                ),
                {"from": self._web3.account.address, "gas": self._gas_limit},
            )
            tx_hash = await self._web3.send_transaction(transaction)
        except Exception as exc:
            return RedemptionReport(RedemptionIntentStatus.UNKNOWN, error=str(exc))
        return RedemptionReport(RedemptionIntentStatus.SUBMITTED, tx_hash=tx_hash)

    async def reconcile(self, request: SettlementRequest, report: RedemptionReport) -> RedemptionReport:
        if not report.tx_hash:
            return RedemptionReport(
                RedemptionIntentStatus.UNKNOWN,
                error=report.error or "transaction hash unavailable",
            )
        try:
            status = await self._web3.transaction_status(report.tx_hash)
        except Exception as exc:
            return RedemptionReport(RedemptionIntentStatus.UNKNOWN, tx_hash=report.tx_hash, error=str(exc))
        if status is None:
            return RedemptionReport(RedemptionIntentStatus.UNKNOWN, tx_hash=report.tx_hash)
        if not status:
            return RedemptionReport(RedemptionIntentStatus.FAILED, tx_hash=report.tx_hash, error="transaction reverted")
        if await self._has_exposure(request):
            return RedemptionReport(
                RedemptionIntentStatus.UNKNOWN,
                tx_hash=report.tx_hash,
                error="redemption receipt confirmed but Conditional Tokens exposure remains non-zero",
            )
        return RedemptionReport(RedemptionIntentStatus.CONFIRMED, tx_hash=report.tx_hash)

    async def native_balance(self) -> float:
        return float(await self._web3.native_balance())

    async def _has_exposure(self, request: SettlementRequest) -> bool:
        if self._web3.account is None:
            return True
        collateral = self._web3.w3.to_checksum_address(request.collateral_token)
        condition_id = _bytes32(request.condition_id)
        for index_set in request.index_sets:
            collection_id = await self._web3.call_contract(
                self._contract_address,
                CONDITIONAL_TOKENS_ABI,
                "getCollectionId",
                bytes(32),
                condition_id,
                index_set,
            )
            position_id = await self._web3.call_contract(
                self._contract_address,
                CONDITIONAL_TOKENS_ABI,
                "getPositionId",
                collateral,
                collection_id,
            )
            balance = await self._web3.call_contract(
                self._contract_address,
                CONDITIONAL_TOKENS_ABI,
                "balanceOf",
                self._web3.account.address,
                position_id,
            )
            if int(balance) > 0:
                return True
        return False


def _bytes32(value: str) -> bytes:
    normalized = value.removeprefix("0x")
    if len(normalized) != 64:
        raise ValueError("condition_id must be a 32-byte hex value")
    try:
        return bytes.fromhex(normalized)
    except ValueError as exc:
        raise ValueError("condition_id must be hexadecimal") from exc
