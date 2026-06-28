from __future__ import annotations

import asyncio
import logging
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING

from .connectors.base import BinaryMarketClient, ReconciliationUnsupported
from .models import (
    ExecutionStatus,
    OpenPosition,
    OrderIntentStatus,
    ReconciliationResult,
    VenueOrder,
)
from .risk import GlobalRiskController

if TYPE_CHECKING:
    from .database import ProductionRepository

LOGGER = logging.getLogger(__name__)
_SYNTHETIC_MARKET_KEY_PREFIXES = ("integration:", "restart:")
_SYNTHETIC_TOKEN_IDS = {"integration-token", "restart-token"}


class ReconciliationService:
    def __init__(
        self,
        repository: ProductionRepository,
        clients: dict[str, BinaryMarketClient],
        risk: GlobalRiskController,
        *,
        orders_interval_seconds: float = 5.0,
        full_interval_seconds: float = 30.0,
    ) -> None:
        self._repository = repository
        self._clients = clients
        self._risk = risk
        self._orders_interval_seconds = orders_interval_seconds
        self._full_interval_seconds = full_interval_seconds
        self._task: asyncio.Task[None] | None = None
        self._ready = False
        self._last_success_at: datetime | None = None
        self._last_error: str | None = None
        self._last_full_at = 0.0

    @property
    def ready(self) -> bool:
        return self._ready

    @property
    def last_error(self) -> str | None:
        return self._last_error

    async def startup_reconcile(self) -> bool:
        if any(not client.supports_full_reconciliation() for client in self._clients.values()):
            unsupported = [name for name, client in self._clients.items() if not client.supports_full_reconciliation()]
            self._last_error = f"full reconciliation unsupported: {', '.join(unsupported)}"
            await self._risk.pause(self._last_error)
            return False
        results = await asyncio.gather(
            *(self._reconcile_venue(name, client, full=True) for name, client in self._clients.items()),
            return_exceptions=True,
        )
        failures = [result for result in results if isinstance(result, BaseException) or not result.success]
        self._ready = not failures
        if failures:
            self._last_error = "; ".join(str(item) for item in failures)
            await self._risk.pause(f"startup reconciliation failed: {self._last_error}")
            return False
        self._last_error = None
        self._last_success_at = datetime.now(UTC)
        return True

    async def start(self) -> None:
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._run(), name="continuous-reconciliation")

    async def close(self) -> None:
        if self._task is not None:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
            self._task = None

    async def run_once(self, *, full: bool = True) -> list[ReconciliationResult]:
        results = await asyncio.gather(
            *(self._reconcile_venue(name, client, full=full) for name, client in self._clients.items())
        )
        self._ready = all(result.success and result.drift_count == 0 for result in results)
        if self._ready:
            self._last_success_at = datetime.now(UTC)
            self._last_error = None
        else:
            self._last_error = "; ".join(
                result.error or f"{result.venue}: drift"
                for result in results
                if not result.success or result.drift_count
            )
        return results

    async def _run(self) -> None:
        loop = asyncio.get_running_loop()
        while True:
            started = loop.time()
            full = started - self._last_full_at >= self._full_interval_seconds
            try:
                results = await self.run_once(full=full)
                if full:
                    self._last_full_at = started
                if any(not result.success or result.drift_count for result in results):
                    await self._risk.pause("continuous reconciliation detected drift")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._ready = False
                self._last_error = str(exc)
                LOGGER.exception("continuous_reconciliation_failed")
                await self._risk.pause(f"continuous reconciliation failed: {exc}")
            elapsed = loop.time() - started
            await asyncio.sleep(max(0.0, self._orders_interval_seconds - elapsed))

    async def _reconcile_venue(self, venue: str, client: BinaryMarketClient, *, full: bool) -> ReconciliationResult:
        started_at = datetime.now(UTC)
        checked = 0
        fills_recorded = 0
        drift = 0
        untracked_open_order_ids: list[str] = []
        untracked_fill_refs: list[str] = []
        error: str | None = None
        success = True
        try:
            unresolved = [row for row in await self._repository.unresolved_order_intents() if row.venue == venue]
            for row in unresolved:
                checked += 1
                if not row.venue_order_id:
                    if _is_synthetic_order_intent(row):
                        await self._repository.update_order_intent(
                            row.client_order_id,
                            OrderIntentStatus.CANCELLED,
                            error="retired synthetic startup artifact without venue order id",
                        )
                        continue
                    drift += 1
                    await self._repository.update_order_intent(
                        row.client_order_id,
                        OrderIntentStatus.MANUAL_REVIEW,
                        error="submission outcome unknown and venue order id is unavailable",
                    )
                    continue
                try:
                    report = await client.get_order(row.venue_order_id)
                except Exception as exc:
                    if _is_synthetic_order_intent(row) and _is_http_not_found(exc):
                        await self._repository.update_order_intent(
                            row.client_order_id,
                            OrderIntentStatus.CANCELLED,
                            venue_order_id=row.venue_order_id,
                            error="retired synthetic startup artifact missing on venue",
                        )
                        continue
                    raise
                status = _intent_status(report.status)
                await self._repository.update_order_intent(
                    row.client_order_id,
                    status,
                    venue_order_id=row.venue_order_id,
                )
                await self._repository.upsert_venue_order(
                    VenueOrder(
                        client_order_id=row.client_order_id,
                        venue_order_id=row.venue_order_id,
                        venue=venue,
                        status=status,
                        quantity=Decimal(str(report.amount_requested)),
                        cumulative_filled=Decimal(str(report.amount_filled)),
                        average_price=Decimal(str(report.avg_price)),
                        updated_at=report.updated_at,
                    )
                )
                if status in {OrderIntentStatus.UNKNOWN, OrderIntentStatus.MANUAL_REVIEW}:
                    drift += 1

            open_orders = await client.list_open_orders()
            for order in open_orders:
                checked += 1
                client_order_id = order.client_order_id or await self._repository.client_order_id_for_venue_order(
                    venue, order.venue_order_id
                )
                if client_order_id is None:
                    untracked_open_order_ids.append(order.venue_order_id)
                    continue
                order = replace(order, client_order_id=client_order_id)
                await self._repository.upsert_venue_order(order)
                await self._repository.update_order_intent(
                    client_order_id,
                    OrderIntentStatus.CANCEL_PENDING,
                    venue_order_id=order.venue_order_id,
                )
                try:
                    await client.cancel_order(order.venue_order_id)
                    report = await client.get_order(order.venue_order_id)
                    reconciled_status = _intent_status(report.status)
                    if reconciled_status is OrderIntentStatus.ACKNOWLEDGED:
                        reconciled_status = OrderIntentStatus.UNKNOWN
                    await self._repository.update_order_intent(
                        client_order_id,
                        reconciled_status,
                        venue_order_id=order.venue_order_id,
                    )
                    if reconciled_status not in {
                        OrderIntentStatus.CANCELLED,
                        OrderIntentStatus.FILLED,
                    }:
                        drift += 1
                except Exception as exc:
                    drift += 1
                    await self._repository.update_order_intent(
                        client_order_id,
                        OrderIntentStatus.UNKNOWN,
                        venue_order_id=order.venue_order_id,
                        error=f"reconciliation cancel failed: {exc}",
                    )

            fills = await client.list_fills(self._last_success_at)
            for fill in fills:
                client_order_id = fill.client_order_id or await self._repository.client_order_id_for_venue_order(
                    venue, fill.venue_order_id
                )
                if client_order_id is None:
                    untracked_fill_refs.append(fill.fill_id or fill.venue_order_id)
                    continue
                fill = replace(fill, client_order_id=client_order_id)
                fills_recorded += int(await self._repository.insert_fill(fill))

            if untracked_open_order_ids:
                await self._repository.audit(
                    "untracked_open_orders",
                    {
                        "venue": venue,
                        "count": len(untracked_open_order_ids),
                        "sample_venue_order_ids": untracked_open_order_ids[:10],
                    },
                )
            if untracked_fill_refs:
                await self._repository.audit(
                    "untracked_fills",
                    {
                        "venue": venue,
                        "count": len(untracked_fill_refs),
                        "sample_fill_refs": untracked_fill_refs[:10],
                    },
                )

            if full:
                balances, positions = await asyncio.gather(client.get_balances(), client.get_positions())
                await self._repository.record_balances(venue, balances)
                expected_positions = _expected_positions(venue, await self._repository.load_positions())
                mismatches = {
                    token_id: {
                        "expected": str(expected_positions.get(token_id, Decimal(0))),
                        "actual": str(positions.get(token_id, Decimal(0))),
                    }
                    for token_id in expected_positions
                    if abs(expected_positions.get(token_id, Decimal(0)) - positions.get(token_id, Decimal(0)))
                    > Decimal("0.00000001")
                }
                drift += len(mismatches)
                await self._repository.audit(
                    "venue_positions_snapshot",
                    {
                        "venue": venue,
                        "positions": {key: str(value) for key, value in positions.items()},
                        "mismatches": mismatches,
                    },
                )
        except ReconciliationUnsupported as exc:
            success = False
            error = str(exc)
        except Exception as exc:
            success = False
            error = str(exc)
            LOGGER.exception("venue_reconciliation_failed", extra={"_venue": venue})

        result = ReconciliationResult(
            venue=venue,
            started_at=started_at,
            completed_at=datetime.now(UTC),
            orders_checked=checked,
            fills_recorded=fills_recorded,
            drift_count=drift,
            success=success,
            error=error,
        )
        await self._repository.record_reconciliation(result)
        return result


def _intent_status(status: ExecutionStatus) -> OrderIntentStatus:
    return {
        ExecutionStatus.OPEN: OrderIntentStatus.ACKNOWLEDGED,
        ExecutionStatus.PARTIAL: OrderIntentStatus.PARTIAL,
        ExecutionStatus.FILLED: OrderIntentStatus.FILLED,
        ExecutionStatus.CANCELLED: OrderIntentStatus.CANCELLED,
        ExecutionStatus.EXPIRED: OrderIntentStatus.CANCELLED,
    }[status]


def _expected_positions(venue: str, local_positions: list[OpenPosition]) -> dict[str, Decimal]:
    expected: dict[str, Decimal] = {}
    for item in local_positions:
        # Kept local to reconciliation to avoid coupling the repository to
        # connector-specific token naming.
        position = item
        market = position.market
        if market.venue_a_label == venue:
            token_id = market.polymarket_token_id
            quantity = Decimal(str(position.polymarket_contracts - position.polymarket_closed_contracts))
            expected[token_id] = expected.get(token_id, Decimal(0)) + quantity
        if market.venue_b_label == venue:
            token_id = market.predict_fun_token_id
            quantity = Decimal(str(position.predict_fun_contracts - position.predict_fun_closed_contracts))
            expected[token_id] = expected.get(token_id, Decimal(0)) + quantity
    return expected


def _is_synthetic_order_intent(row: object) -> bool:
    market_key = str(getattr(row, "market_key", "") or "")
    token_id = str(getattr(row, "token_id", "") or "")
    return market_key.startswith(_SYNTHETIC_MARKET_KEY_PREFIXES) and token_id in _SYNTHETIC_TOKEN_IDS


def _is_http_not_found(exc: Exception) -> bool:
    status = getattr(exc, "status", None)
    return status == 404 or "404" in str(exc)
