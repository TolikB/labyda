from __future__ import annotations

import asyncio
import logging
from dataclasses import replace
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from .connectors.base import BinaryMarketClient
from .models import OpenPosition, SettlementStatus, position_key
from .positions import PositionLedger
from .risk import GlobalRiskController
from .telegram import TelegramNotifier

if TYPE_CHECKING:
    from .database import ProductionRepository

LOGGER = logging.getLogger(__name__)


class SettlementService:
    def __init__(
        self,
        ledger: PositionLedger,
        clients: dict[str, BinaryMarketClient],
        risk: GlobalRiskController,
        telegram: TelegramNotifier,
        repository: ProductionRepository | None = None,
    ) -> None:
        self._ledger = ledger
        self._clients = clients
        self._risk = risk
        self._telegram = telegram
        self._repository = repository

    async def run_once(self) -> None:
        now = datetime.now(timezone.utc)
        for position in self._ledger.all():
            expires_at = position.market.expires_at
            if expires_at is None:
                continue
            if expires_at.tzinfo is None:
                expires_at = expires_at.replace(tzinfo=timezone.utc)
            if expires_at > now or position.status in {"closed", "settled", "manual_review"}:
                continue
            await self._reconcile_position(position)

    async def _reconcile_position(self, position: OpenPosition) -> None:
        first_label = position.market.venue_a_label
        second_label = position.market.venue_b_label
        first = self._clients.get(first_label)
        second = self._clients.get(second_label)
        if first is None or second is None:
            await self._manual_review(position, "settlement connector unavailable")
            return
        first_id = _market_id(position, first_label)
        second_id = _market_id(position, second_label)
        if not first_id or not second_id:
            await self._manual_review(position, "settlement market id unavailable")
            return
        try:
            first_status, second_status = await asyncio.gather(
                first.get_settlement_status(first_id),
                second.get_settlement_status(second_id),
            )
        except Exception as exc:
            await self._manual_review(position, f"settlement status lookup failed: {exc}")
            return
        if SettlementStatus.MANUAL_REVIEW in {first_status, second_status}:
            await self._manual_review(position, "automatic settlement is unsupported for this venue route")
            return
        if SettlementStatus.OPEN in {first_status, second_status}:
            return
        if first_status is SettlementStatus.SETTLED and second_status is SettlementStatus.SETTLED:
            await self._remove(position)
            return
        try:
            await asyncio.gather(
                first.redeem_position(first_id),
                second.redeem_position(second_id),
            )
        except Exception as exc:
            await self._manual_review(position, f"automatic redemption failed: {exc}")
            return
        await self._remove(position)

    async def _manual_review(self, position: OpenPosition, reason: str) -> None:
        updated = replace(position, status="manual_review")
        key = position_key(position.market)
        if self._repository is not None:
            await self._repository.save_position(key, updated)
            await self._repository.audit(
                "settlement_manual_review",
                {"position_key": key, "symbol": position.market.symbol, "reason": reason},
            )
        self._ledger.add(updated)
        await self._risk.pause(f"settlement manual review required for {position.market.symbol}: {reason}")
        await self._telegram.send_html(
            "🚨 <b>SETTLEMENT MANUAL REVIEW REQUIRED</b>\n"
            f"Market: {position.market.symbol}\nReason: {reason}\nExecution remains paused."
        )

    async def _remove(self, position: OpenPosition) -> None:
        key = position_key(position.market)
        if self._repository is not None:
            await self._repository.remove_position(key)
        self._ledger.remove(key)


def _market_id(position: OpenPosition, venue: str) -> str | None:
    if venue == "Polymarket":
        return position.market.polymarket_market_id or position.market.condition_id
    if venue == "Predict.fun":
        return position.market.predict_fun_market_id
    if venue == "Myriad":
        return position.market.myriad_market_id
    return None
