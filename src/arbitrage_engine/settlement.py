from __future__ import annotations

import logging
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING

from .connectors.base import BinaryMarketClient
from .models import (
    OpenPosition,
    RedemptionIntent,
    RedemptionIntentStatus,
    RedemptionReport,
    SettlementRequest,
    SettlementStatus,
    position_key,
)
from .positions import PositionLedger
from .risk import GlobalRiskController
from .telegram import TelegramNotifier
from .utils.ids import uuid7

if TYPE_CHECKING:
    from .database import ProductionRepository

LOGGER = logging.getLogger(__name__)
REDEMPTION_PENDING_TIMEOUT_SECONDS = 180


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
        now = datetime.now(UTC)
        for position in self._ledger.all():
            expires_at = position.market.expires_at
            if expires_at is None:
                continue
            if expires_at.tzinfo is None:
                expires_at = expires_at.replace(tzinfo=UTC)
            if expires_at > now or position.status in {"closed", "settled", "manual_review"}:
                continue
            await self._reconcile_position(position)

    async def _reconcile_position(self, position: OpenPosition) -> None:
        if self._repository is None:
            await self._manual_review(position, "durable redemption storage is unavailable")
            return
        key = position_key(position.market)
        requests: list[tuple[BinaryMarketClient, SettlementRequest]] = []
        for venue, expected_contracts in (
            (position.market.venue_a_label, Decimal(str(position.polymarket_contracts))),
            (position.market.venue_b_label, Decimal(str(position.predict_fun_contracts))),
        ):
            client = self._clients.get(venue)
            request = _settlement_request(position, venue, expected_contracts)
            if client is None or request is None:
                await self._manual_review(position, f"settlement metadata or connector unavailable for {venue}")
                return
            try:
                requests.append((client, client.prepare_settlement_request(request)))
            except Exception as exc:
                await self._manual_review(position, f"invalid settlement configuration for {venue}: {exc}")
                return

        statuses: list[SettlementStatus] = []
        try:
            for client, request in requests:
                statuses.append(await client.get_settlement_status(request))
        except Exception as exc:
            await self._manual_review(position, f"settlement status lookup failed: {exc}")
            return
        if SettlementStatus.OPEN in statuses:
            return
        if any(status in {SettlementStatus.VOID, SettlementStatus.MANUAL_REVIEW} for status in statuses):
            await self._manual_review(
                position,
                f"unsupported or conflicting payout: {[item.value for item in statuses]}",
            )
            return

        confirmed: list[bool] = []
        for client, request in requests:
            confirmed.append(await self._process_redemption(position, client, request))
            if self._risk.is_paused():
                return
        if all(confirmed):
            await self._repository.audit(
                "position_redemption_confirmed",
                {"position_key": key, "venues": [request.venue for _, request in requests]},
            )
            await self._remove(position)

    async def _process_redemption(
        self,
        position: OpenPosition,
        client: BinaryMarketClient,
        request: SettlementRequest,
    ) -> bool:
        assert self._repository is not None
        intent = await self._repository.get_redemption_intent(
            request.position_key,
            request.venue,
            request.condition_id,
        )
        if intent is None:
            candidate = RedemptionIntent(
                redemption_id=str(uuid7()),
                position_key=request.position_key,
                venue=request.venue,
                market_id=request.market_id,
                condition_id=request.condition_id,
                collateral_token=request.collateral_token,
                expected_contracts=request.expected_contracts,
            )
            await self._repository.create_redemption_intent(candidate)
            intent = await self._repository.get_redemption_intent(
                request.position_key,
                request.venue,
                request.condition_id,
            )
            if intent is None:
                await self._manual_review(position, f"could not persist redemption intent for {request.venue}")
                return False

        if intent.status is RedemptionIntentStatus.CONFIRMED:
            return True
        if intent.status in {RedemptionIntentStatus.FAILED, RedemptionIntentStatus.MANUAL_REVIEW}:
            await self._manual_review(position, f"redemption is {intent.status.value} for {request.venue}")
            return False
        if intent.status in {RedemptionIntentStatus.SUBMITTED, RedemptionIntentStatus.UNKNOWN}:
            if not intent.tx_hash:
                await self._mark_unknown(position, intent, "transaction hash unavailable; blind retry forbidden")
                return False
            try:
                report = await client.reconcile_redemption(
                    request,
                    RedemptionReport(intent.status, tx_hash=intent.tx_hash, error=intent.last_error)
                )
            except Exception as exc:
                await self._mark_unknown(position, intent, f"receipt reconciliation failed: {exc}")
                return False
            if report.status is RedemptionIntentStatus.CONFIRMED:
                await self._repository.update_redemption_intent(
                    intent.redemption_id,
                    RedemptionIntentStatus.CONFIRMED,
                    tx_hash=report.tx_hash,
                )
                return True
            age_seconds = (datetime.now(UTC) - _aware(intent.updated_at)).total_seconds()
            if (
                report.status is RedemptionIntentStatus.UNKNOWN
                and not report.error
                and age_seconds < REDEMPTION_PENDING_TIMEOUT_SECONDS
            ):
                return False
            await self._mark_unknown(position, intent, report.error or "redemption receipt unresolved after timeout")
            return False

        intent = await self._repository.update_redemption_intent(
            intent.redemption_id,
            RedemptionIntentStatus.UNKNOWN,
            error="submission entered ambiguity window; blind retry is forbidden",
        )
        try:
            report = await client.redeem_position(request, intent.redemption_id)
        except Exception as exc:
            await self._mark_unknown(position, intent, f"redemption submission failed ambiguously: {exc}")
            return False
        await self._repository.update_redemption_intent(
            intent.redemption_id,
            report.status,
            tx_hash=report.tx_hash,
            error=report.error,
        )
        if report.status is RedemptionIntentStatus.CONFIRMED:
            return True
        if report.status is RedemptionIntentStatus.SUBMITTED and report.tx_hash:
            return False
        if report.status in {RedemptionIntentStatus.FAILED, RedemptionIntentStatus.MANUAL_REVIEW}:
            await self._manual_review(
                position,
                f"redemption submission is {report.status.value} for {request.venue}: {report.error or 'unknown'}",
            )
            return False
        await self._mark_unknown(
            position,
            replace(intent, tx_hash=report.tx_hash or intent.tx_hash),
            report.error or "redemption submission result is ambiguous",
        )
        return False

    async def _mark_unknown(self, position: OpenPosition, intent: RedemptionIntent, reason: str) -> None:
        assert self._repository is not None
        await self._repository.update_redemption_intent(
            intent.redemption_id,
            RedemptionIntentStatus.UNKNOWN,
            tx_hash=intent.tx_hash,
            error=reason,
        )
        await self._manual_review(position, f"UNKNOWN redemption for {intent.venue}: {reason}")

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


def _settlement_request(
    position: OpenPosition,
    venue: str,
    expected_contracts: Decimal,
) -> SettlementRequest | None:
    market = position.market
    if venue == "Polymarket":
        market_id = market.polymarket_market_id or market.condition_id
        condition_id = market.condition_id
        collateral = ""
    elif venue == "Myriad":
        market_id = market.myriad_market_id
        condition_id = market.myriad_condition_id
        collateral = market.myriad_collateral_token or ""
    else:
        return None
    if not market_id or not condition_id:
        return None
    return SettlementRequest(
        position_key=position_key(market),
        venue=venue,
        market_id=market_id,
        condition_id=condition_id,
        collateral_token=collateral,
        expected_contracts=expected_contracts,
    )


def _aware(value: datetime) -> datetime:
    return value if value.tzinfo is not None else value.replace(tzinfo=UTC)
