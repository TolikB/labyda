from __future__ import annotations

import asyncio
import logging
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from .connectors.base import (
    BinaryMarketClient,
    OrderResidualExposure,
    OrderResidualExposureBatch,
    ReconciliationUnsupported,
)
from .models import (
    BinarySide,
    ExecutionStatus,
    OpenPosition,
    OrderIntent,
    OrderIntentStatus,
    ReconciliationResult,
    VenueOrder,
    execution_route_for_market,
    first_leg_token_for_route,
    second_leg_token_for_route,
)
from .risk import GlobalRiskController

if TYPE_CHECKING:
    from .database import ProductionRepository

LOGGER = logging.getLogger(__name__)
_SYNTHETIC_MARKET_KEY_PREFIXES = ("integration:", "restart:")
_SYNTHETIC_TOKEN_IDS = {"integration-token", "restart-token"}
_INFLIGHT_SUBMISSION_GRACE_SECONDS = 30.0
_FILL_RECONCILIATION_LOOKBACK = timedelta(days=7)


class ReconciliationService:
    def __init__(
        self,
        repository: ProductionRepository,
        clients: dict[str, BinaryMarketClient],
        risk: GlobalRiskController,
        *,
        orders_interval_seconds: float = 5.0,
        full_interval_seconds: float = 30.0,
        startup_retry_attempts: int = 2,
        startup_retry_delay_seconds: float = 1.0,
        transient_failure_pause_threshold: int = 3,
    ) -> None:
        self._repository = repository
        self._clients = clients
        self._risk = risk
        self._orders_interval_seconds = orders_interval_seconds
        self._full_interval_seconds = full_interval_seconds
        self._startup_retry_attempts = max(1, startup_retry_attempts)
        self._startup_retry_delay_seconds = max(0.0, startup_retry_delay_seconds)
        self._transient_failure_pause_threshold = max(1, transient_failure_pause_threshold)
        self._task: asyncio.Task[None] | None = None
        self._ready = False
        self._last_success_at: datetime | None = None
        self._last_error: str | None = None
        self._last_full_at = 0.0
        self._consecutive_transient_failures = 0

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
        pending_clients = dict(self._clients)
        failures: list[BaseException | ReconciliationResult] = []
        for attempt in range(self._startup_retry_attempts):
            pending_names = tuple(pending_clients)
            results = await asyncio.gather(
                *(
                    self._reconcile_venue(name, client, full=True, allow_inflight_grace=False)
                    for name, client in pending_clients.items()
                ),
                return_exceptions=True,
            )
            failures = []
            next_pending: dict[str, BinaryMarketClient] = {}
            for name, result in zip(pending_names, results, strict=True):
                if isinstance(result, BaseException) or not result.success or result.drift_count > 0:
                    failures.append(result)
                    next_pending[name] = pending_clients[name]
            if not failures:
                break
            if attempt + 1 < self._startup_retry_attempts:
                await asyncio.sleep(self._startup_retry_delay_seconds)
                pending_clients = next_pending
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
            *(
                self._reconcile_venue(name, client, full=full, allow_inflight_grace=True)
                for name, client in self._clients.items()
            )
        )
        hard_failures, transient_failures = _partition_reconciliation_failures(results)
        if not hard_failures and not transient_failures:
            self._consecutive_transient_failures = 0
            self._ready = True
            self._last_success_at = datetime.now(UTC)
            self._last_error = None
        elif hard_failures:
            self._consecutive_transient_failures = 0
            self._ready = False
            self._last_error = "; ".join(
                result.error or f"{result.venue}: drift"
                for result in hard_failures
            )
        else:
            self._consecutive_transient_failures += 1
            self._last_error = "; ".join(
                result.error or f"{result.venue}: transient reconciliation failure"
                for result in transient_failures
            )
            self._ready = (
                self._last_success_at is not None
                and self._consecutive_transient_failures < self._transient_failure_pause_threshold
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
                hard_failures, transient_failures = _partition_reconciliation_failures(results)
                if hard_failures:
                    await self._risk.pause("continuous reconciliation detected drift")
                elif transient_failures and (
                    self._consecutive_transient_failures >= self._transient_failure_pause_threshold
                ):
                    await self._risk.pause("continuous reconciliation transient failures exceeded threshold")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._ready = False
                self._last_error = str(exc)
                LOGGER.exception("continuous_reconciliation_failed")
                await self._risk.pause(f"continuous reconciliation failed: {exc}")
            elapsed = loop.time() - started
            await asyncio.sleep(max(0.0, self._orders_interval_seconds - elapsed))

    async def _reconcile_venue(
        self,
        venue: str,
        client: BinaryMarketClient,
        *,
        full: bool,
        allow_inflight_grace: bool,
    ) -> ReconciliationResult:
        started_at = datetime.now(UTC)
        checked = 0
        fills_recorded = 0
        drift = 0
        untracked_open_order_ids: set[str] = set()
        untracked_fill_refs: set[str] = set()
        error: str | None = None
        success = True
        transient_failure = False
        try:
            unresolved = [row for row in await self._repository.unresolved_order_intents() if row.venue == venue]
            unresolved_client_order_ids = {row.client_order_id for row in unresolved}
            inflight_client_order_ids = {
                row.client_order_id
                for row in unresolved
                if allow_inflight_grace and _is_recent_submitting_intent(row, started_at)
            }
            graced_venue_order_ids: set[str] = set()
            handled_residual_order_ids: set[str] = set()

            def can_defer_untracked(venue_order_id: str) -> bool:
                if venue_order_id in graced_venue_order_ids:
                    return True
                if len(graced_venue_order_ids) >= len(inflight_client_order_ids):
                    return False
                graced_venue_order_ids.add(venue_order_id)
                return True

            for row in unresolved:
                checked += 1
                if row.client_order_id in inflight_client_order_ids:
                    continue
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
                    await client.restore_order_context(
                        row.venue_order_id,
                        _order_intent_from_row(row),
                    )
                    report = await client.get_order(row.venue_order_id)
                except OrderResidualExposure as exc:
                    await self._persist_residual_exit_exposure(row, venue, exc)
                    handled_residual_order_ids.add(exc.order_id.lower())
                    drift += 1
                    continue
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
                persisted_client_order_id = await self._repository.client_order_id_for_venue_order(
                    venue, order.venue_order_id
                )
                remote_client_order_id = order.client_order_id or None
                if (
                    persisted_client_order_id in inflight_client_order_ids
                    or remote_client_order_id in inflight_client_order_ids
                ) and can_defer_untracked(order.venue_order_id):
                    continue
                client_order_id = persisted_client_order_id or (
                    remote_client_order_id
                    if remote_client_order_id in unresolved_client_order_ids
                    else None
                )
                if client_order_id is None:
                    if can_defer_untracked(order.venue_order_id):
                        continue
                    untracked_open_order_ids.add(order.venue_order_id)
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

            fill_since = self._last_success_at or started_at - _FILL_RECONCILIATION_LOOKBACK
            fill_context_rows = await self._repository.order_intents_for_fill_reconciliation(
                venue,
                fill_since,
            )
            for row in fill_context_rows:
                if row.venue_order_id:
                    await client.restore_fill_context(
                        row.venue_order_id,
                        _order_intent_from_row(row),
                    )
            try:
                fills = await client.list_fills(fill_since)
            except OrderResidualExposureBatch as batch:
                for exposure in batch.exposures:
                    normalized_order_id = exposure.order_id.lower()
                    context = next(
                        (
                            row
                            for row in fill_context_rows
                            if str(row.venue_order_id or "").lower() == normalized_order_id
                        ),
                        None,
                    )
                    if context is None:
                        raise RuntimeError(
                            f"residual SX Bet exposure has no durable order context: {exposure.order_id}"
                        ) from exposure
                    if normalized_order_id not in handled_residual_order_ids:
                        await self._persist_residual_exit_exposure(context, venue, exposure)
                        handled_residual_order_ids.add(normalized_order_id)
                        drift += 1
                fills = list(batch.fills)
            except OrderResidualExposure as exc:
                normalized_order_id = exc.order_id.lower()
                context = next(
                    (
                        row
                        for row in fill_context_rows
                        if str(row.venue_order_id or "").lower() == normalized_order_id
                    ),
                    None,
                )
                if context is None:
                    raise RuntimeError(
                        f"residual SX Bet exposure has no durable order context: {exc.order_id}"
                    ) from exc
                if normalized_order_id not in handled_residual_order_ids:
                    await self._persist_residual_exit_exposure(context, venue, exc)
                    handled_residual_order_ids.add(normalized_order_id)
                    drift += 1
                fills = []
            for fill in fills:
                persisted_client_order_id = await self._repository.client_order_id_for_venue_order(
                    venue, fill.venue_order_id
                )
                remote_client_order_id = fill.client_order_id or None
                if (
                    persisted_client_order_id in inflight_client_order_ids
                    or remote_client_order_id in inflight_client_order_ids
                ) and can_defer_untracked(fill.venue_order_id):
                    continue
                client_order_id = persisted_client_order_id or (
                    remote_client_order_id
                    if remote_client_order_id in unresolved_client_order_ids
                    else None
                )
                if client_order_id is None:
                    if can_defer_untracked(fill.venue_order_id):
                        continue
                    untracked_fill_refs.add(fill.fill_id or fill.venue_order_id)
                    continue
                fill = replace(fill, client_order_id=client_order_id)
                fills_recorded += int(await self._repository.insert_fill(fill))

            if graced_venue_order_ids:
                await self._repository.audit(
                    "inflight_submission_reconciliation_deferred",
                    {
                        "venue": venue,
                        "count": len(graced_venue_order_ids),
                        "sample_venue_order_ids": sorted(graced_venue_order_ids)[:10],
                    },
                )
            if untracked_open_order_ids:
                drift += len(untracked_open_order_ids)
                await self._repository.audit(
                    "untracked_open_orders",
                    {
                        "venue": venue,
                        "count": len(untracked_open_order_ids),
                        "sample_venue_order_ids": sorted(untracked_open_order_ids)[:10],
                    },
                )
            if untracked_fill_refs:
                drift += len(untracked_fill_refs)
                await self._repository.audit(
                    "untracked_fills",
                    {
                        "venue": venue,
                        "count": len(untracked_fill_refs),
                        "sample_fill_refs": sorted(untracked_fill_refs)[:10],
                    },
                )

            if full:
                balances, positions = await asyncio.gather(client.get_balances(), client.get_positions())
                await self._repository.record_balances(venue, balances)
                expected_positions = _expected_positions(venue, await self._repository.load_positions())
                position_token_ids = expected_positions.keys() | positions.keys()
                mismatches = {
                    token_id: {
                        "expected": str(expected_positions.get(token_id, Decimal(0))),
                        "actual": str(positions.get(token_id, Decimal(0))),
                    }
                    for token_id in position_token_ids
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
            transient_failure = _is_transient_reconciliation_exception(exc)
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
            transient_failure=transient_failure,
        )
        await self._repository.record_reconciliation(result)
        return result

    async def _persist_residual_exit_exposure(
        self,
        row: Any,
        venue: str,
        exc: OrderResidualExposure,
    ) -> None:
        if str(row.action).upper() != "SELL":
            raise RuntimeError("residual exit exposure is linked to a non-SELL order intent")
        await self._repository.record_residual_exit_exposure(
            market_key=row.market_key,
            venue=venue,
            requested_contracts=Decimal(str(row.quantity)),
            report=exc.report,
            residual_contracts=exc.residual_contracts,
        )
        await self._repository.update_order_intent(
            row.client_order_id,
            OrderIntentStatus.MANUAL_REVIEW,
            venue_order_id=exc.order_id,
            error=str(exc),
        )
        await self._repository.audit(
            "residual_exit_exposure_detected",
            {
                "venue": venue,
                "client_order_id": row.client_order_id,
                "venue_order_id": exc.order_id,
                "closed_contracts": str(exc.report.amount_filled),
                "residual_contracts": str(exc.residual_contracts),
            },
        )
        await self._risk.pause(
            f"residual opposite exposure: {venue} client_order_id={row.client_order_id}"
        )


def _order_intent_from_row(row: Any) -> OrderIntent:
    return OrderIntent(
        client_order_id=row.client_order_id,
        route=row.route,
        market_key=row.market_key,
        venue=row.venue,
        token_id=row.token_id,
        binary_side=BinarySide(row.binary_side),
        action=row.action,
        quantity=Decimal(str(row.quantity)),
        limit_price=Decimal(str(row.limit_price)),
        status=OrderIntentStatus(row.status),
        venue_order_id=row.venue_order_id,
        created_at=row.created_at,
        updated_at=row.updated_at,
    )


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
        position = item
        market = position.market
        route = execution_route_for_market(market)
        if market.first_venue_label == venue:
            token_id = first_leg_token_for_route(market, route)
            quantity = Decimal(str(position.polymarket_contracts - position.polymarket_closed_contracts))
            if token_id:
                expected[token_id] = expected.get(token_id, Decimal(0)) + quantity
        if market.second_venue_label == venue:
            token_id = second_leg_token_for_route(market, route)
            quantity = Decimal(str(position.predict_fun_contracts - position.predict_fun_closed_contracts))
            if token_id:
                expected[token_id] = expected.get(token_id, Decimal(0)) + quantity
    return expected


def _is_synthetic_order_intent(row: object) -> bool:
    market_key = str(getattr(row, "market_key", "") or "")
    token_id = str(getattr(row, "token_id", "") or "")
    return market_key.startswith(_SYNTHETIC_MARKET_KEY_PREFIXES) and token_id in _SYNTHETIC_TOKEN_IDS


def _is_recent_submitting_intent(row: object, now: datetime) -> bool:
    status = getattr(row, "status", None)
    if status not in {OrderIntentStatus.SUBMITTING, OrderIntentStatus.SUBMITTING.value}:
        return False
    updated_at = getattr(row, "updated_at", None)
    if not isinstance(updated_at, datetime):
        return False
    if updated_at.tzinfo is None:
        updated_at = updated_at.replace(tzinfo=UTC)
    age_seconds = max(0.0, (now - updated_at.astimezone(UTC)).total_seconds())
    return age_seconds <= _INFLIGHT_SUBMISSION_GRACE_SECONDS


def _is_http_not_found(exc: Exception) -> bool:
    status = getattr(exc, "status", None)
    return status == 404 or "404" in str(exc)


def _partition_reconciliation_failures(
    results: list[ReconciliationResult],
) -> tuple[list[ReconciliationResult], list[ReconciliationResult]]:
    hard_failures: list[ReconciliationResult] = []
    transient_failures: list[ReconciliationResult] = []
    for result in results:
        if result.drift_count > 0:
            hard_failures.append(result)
        elif not result.success:
            if result.transient_failure:
                transient_failures.append(result)
            else:
                hard_failures.append(result)
    return hard_failures, transient_failures


def _is_transient_reconciliation_exception(exc: BaseException) -> bool:
    transient_type_names = {
        "ClientConnectionError",
        "ClientConnectorError",
        "ClientConnectorSSLError",
        "ClientOSError",
        "ServerDisconnectedError",
    }
    current: BaseException | None = exc
    while current is not None:
        if isinstance(current, (TimeoutError, OSError, ConnectionError)):
            return True
        if getattr(current, "status", None) in {429, 500, 502, 503, 504}:
            return True
        if current.__class__.__name__ in transient_type_names:
            return True
        if current.__class__.__name__ == "CancelledError":
            current = current.__cause__ or current.__context__
            continue
        current = current.__cause__ or current.__context__
    text = str(exc).lower()
    return any(
        marker in text
        for marker in (
            "timeout",
            "temporarily unavailable",
            "temporary failure",
            "too many requests",
            "rate limit",
            "service unavailable",
            "name or service not known",
            "cannot connect",
            "connection reset",
            "closing transport",
        )
    )
