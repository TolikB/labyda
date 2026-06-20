from __future__ import annotations

import hashlib
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import JSON, Boolean, DateTime, ForeignKey, Index, Integer, Numeric, String, Text, func, select, text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from .market_mapping import rules_fingerprint as build_rules_fingerprint
from .models import (
    FillRecord,
    MappingStatus,
    MarketMapping,
    MarketSpec,
    OpenPosition,
    OrderIntent,
    OrderIntentStatus,
    ReconciliationResult,
    VenueOrder,
)
from .positions import _position_from_json, _position_to_json

MONEY = Numeric(38, 18)
_TRADER_LOCK_NAME = "arbitrage-engine-production-trader"


class Base(DeclarativeBase):
    pass


class CanonicalMarketRow(Base):
    __tablename__ = "canonical_markets"

    canonical_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    title: Mapped[str] = mapped_column(Text)
    category: Mapped[str] = mapped_column(String(64), index=True)
    resolution_source: Mapped[str] = mapped_column(Text)
    cutoff_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    timezone_name: Mapped[str] = mapped_column(String(64), default="UTC")
    outcome_semantics: Mapped[str] = mapped_column(Text)
    rules_fingerprint: Mapped[str] = mapped_column(String(64), unique=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(UTC))


class VenueInstrumentRow(Base):
    __tablename__ = "venue_instruments"

    instrument_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    canonical_id: Mapped[str | None] = mapped_column(ForeignKey("canonical_markets.canonical_id"), nullable=True)
    venue: Mapped[str] = mapped_column(String(32), index=True)
    market_id: Mapped[str] = mapped_column(String(256))
    yes_token_id: Mapped[str] = mapped_column(Text)
    no_token_id: Mapped[str] = mapped_column(Text)
    closes_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    resolution_source: Mapped[str | None] = mapped_column(Text, nullable=True)
    rules_fingerprint: Mapped[str | None] = mapped_column(String(64), nullable=True)
    category: Mapped[str | None] = mapped_column(String(64), nullable=True)
    metadata_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(UTC))

    __table_args__ = (Index("uq_venue_market", "venue", "market_id", unique=True),)


class MarketMappingRow(Base):
    __tablename__ = "market_mappings"

    mapping_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    canonical_market_id: Mapped[str] = mapped_column(ForeignKey("canonical_markets.canonical_id"))
    left_venue: Mapped[str] = mapped_column(String(32))
    left_market_id: Mapped[str] = mapped_column(String(256))
    right_venue: Mapped[str] = mapped_column(String(32))
    right_market_id: Mapped[str] = mapped_column(String(256))
    status: Mapped[str] = mapped_column(String(24), index=True)
    rules_fingerprint: Mapped[str] = mapped_column(String(64))
    verified_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    verified_by: Mapped[str | None] = mapped_column(String(128), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(UTC))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(UTC))

    __table_args__ = (
        Index(
            "uq_market_mapping_pair",
            "left_venue",
            "left_market_id",
            "right_venue",
            "right_market_id",
            unique=True,
        ),
    )


class OrderIntentRow(Base):
    __tablename__ = "order_intents"

    client_order_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    route: Mapped[str] = mapped_column(String(64), index=True)
    market_key: Mapped[str] = mapped_column(String(512), index=True)
    venue: Mapped[str] = mapped_column(String(32), index=True)
    token_id: Mapped[str] = mapped_column(Text)
    binary_side: Mapped[str] = mapped_column(String(8))
    action: Mapped[str] = mapped_column(String(8))
    quantity: Mapped[Decimal] = mapped_column(MONEY)
    limit_price: Mapped[Decimal] = mapped_column(MONEY)
    status: Mapped[str] = mapped_column(String(32), index=True)
    venue_order_id: Mapped[str | None] = mapped_column(String(256), nullable=True, index=True)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class VenueOrderRow(Base):
    __tablename__ = "venue_orders"

    venue_order_id: Mapped[str] = mapped_column(String(256), primary_key=True)
    client_order_id: Mapped[str] = mapped_column(ForeignKey("order_intents.client_order_id"), unique=True)
    venue: Mapped[str] = mapped_column(String(32), index=True)
    status: Mapped[str] = mapped_column(String(32), index=True)
    quantity: Mapped[Decimal] = mapped_column(MONEY)
    cumulative_filled: Mapped[Decimal] = mapped_column(MONEY, default=Decimal(0))
    average_price: Mapped[Decimal] = mapped_column(MONEY, default=Decimal(0))
    raw_payload: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class FillRow(Base):
    __tablename__ = "fills"

    fill_id: Mapped[str] = mapped_column(String(256), primary_key=True)
    client_order_id: Mapped[str] = mapped_column(ForeignKey("order_intents.client_order_id"), index=True)
    venue_order_id: Mapped[str] = mapped_column(String(256), index=True)
    venue: Mapped[str] = mapped_column(String(32), index=True)
    quantity: Mapped[Decimal] = mapped_column(MONEY)
    price: Mapped[Decimal] = mapped_column(MONEY)
    fee: Mapped[Decimal] = mapped_column(MONEY, default=Decimal(0))
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)


class PositionRow(Base):
    __tablename__ = "positions"

    position_key: Mapped[str] = mapped_column(String(768), primary_key=True)
    symbol: Mapped[str] = mapped_column(String(512), index=True)
    status: Mapped[str] = mapped_column(String(32), index=True)
    first_venue: Mapped[str] = mapped_column(String(32))
    second_venue: Mapped[str] = mapped_column(String(32))
    first_quantity: Mapped[Decimal] = mapped_column(MONEY)
    second_quantity: Mapped[Decimal] = mapped_column(MONEY)
    first_entry_price: Mapped[Decimal] = mapped_column(MONEY)
    second_entry_price: Mapped[Decimal] = mapped_column(MONEY)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON)
    opened_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(UTC))


class RiskStateRow(Base):
    __tablename__ = "risk_state"

    state_id: Mapped[str] = mapped_column(String(32), primary_key=True, default="global")
    loss_day: Mapped[str] = mapped_column(String(10))
    daily_loss_usd: Mapped[Decimal] = mapped_column(MONEY, default=Decimal(0))
    consecutive_api_errors: Mapped[int] = mapped_column(Integer, default=0)
    paused: Mapped[bool] = mapped_column(Boolean, default=False)
    pause_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(UTC))


class BalanceSnapshotRow(Base):
    __tablename__ = "balance_snapshots"

    snapshot_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    venue: Mapped[str] = mapped_column(String(32), index=True)
    asset: Mapped[str] = mapped_column(String(64))
    balance: Mapped[Decimal] = mapped_column(MONEY)
    captured_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)


class ReconciliationRunRow(Base):
    __tablename__ = "reconciliation_runs"

    run_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    venue: Mapped[str] = mapped_column(String(32), index=True)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    completed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    orders_checked: Mapped[int] = mapped_column(Integer)
    fills_recorded: Mapped[int] = mapped_column(Integer)
    drift_count: Mapped[int] = mapped_column(Integer)
    success: Mapped[bool] = mapped_column(Boolean)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)


class AuditEventRow(Base):
    __tablename__ = "audit_events"

    event_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    event_type: Mapped[str] = mapped_column(String(64), index=True)
    correlation_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(UTC))


class ProductionRepository:
    def __init__(self, database_url: str, *, echo: bool = False) -> None:
        self.engine: AsyncEngine = create_async_engine(database_url, echo=echo, pool_pre_ping=True)
        self.sessions = async_sessionmaker(self.engine, expire_on_commit=False)
        self._lock_connection: AsyncConnection | None = None

    async def close(self) -> None:
        await self.release_trader_lock()
        await self.engine.dispose()

    async def create_schema(self) -> None:
        async with self.engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)

    async def ping(self) -> bool:
        try:
            async with self.engine.connect() as connection:
                await connection.execute(text("SELECT 1"))
            return True
        except Exception:
            return False

    async def acquire_trader_lock(self) -> bool:
        if self.engine.dialect.name != "postgresql":
            return False
        if self._lock_connection is not None:
            return True
        connection = await self.engine.connect()
        lock_id = _advisory_lock_id(_TRADER_LOCK_NAME)
        acquired = bool(await connection.scalar(text("SELECT pg_try_advisory_lock(:lock_id)"), {"lock_id": lock_id}))
        if not acquired:
            await connection.close()
            return False
        self._lock_connection = connection
        return True

    async def release_trader_lock(self) -> None:
        if self._lock_connection is None:
            return
        lock_id = _advisory_lock_id(_TRADER_LOCK_NAME)
        await self._lock_connection.execute(text("SELECT pg_advisory_unlock(:lock_id)"), {"lock_id": lock_id})
        await self._lock_connection.close()
        self._lock_connection = None

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[AsyncSession]:
        async with self.sessions() as session, session.begin():
            yield session

    async def create_order_intent(self, intent: OrderIntent) -> None:
        async with self.transaction() as session:
            session.add(
                OrderIntentRow(
                    client_order_id=intent.client_order_id,
                    route=intent.route,
                    market_key=intent.market_key,
                    venue=intent.venue,
                    token_id=intent.token_id,
                    binary_side=intent.binary_side.value,
                    action=intent.action,
                    quantity=intent.quantity,
                    limit_price=intent.limit_price,
                    status=intent.status.value,
                    venue_order_id=intent.venue_order_id,
                    created_at=intent.created_at,
                    updated_at=intent.updated_at,
                )
            )

    async def update_order_intent(
        self,
        client_order_id: str,
        status: OrderIntentStatus,
        *,
        venue_order_id: str | None = None,
        error: str | None = None,
    ) -> None:
        async with self.transaction() as session:
            row = await session.get(OrderIntentRow, client_order_id, with_for_update=True)
            if row is None:
                raise KeyError(f"Unknown client order id: {client_order_id}")
            row.status = status.value
            if venue_order_id is not None:
                row.venue_order_id = venue_order_id
            row.last_error = error
            row.updated_at = datetime.now(UTC)

    async def unresolved_order_intents(self) -> list[OrderIntentRow]:
        terminal = {
            OrderIntentStatus.FILLED.value,
            OrderIntentStatus.CANCELLED.value,
        }
        async with self.sessions() as session:
            result = await session.scalars(select(OrderIntentRow).where(OrderIntentRow.status.not_in(terminal)))
            return list(result)

    async def client_order_id_for_venue_order(self, venue: str, venue_order_id: str) -> str | None:
        async with self.sessions() as session:
            value = await session.scalar(
                select(OrderIntentRow.client_order_id).where(
                    OrderIntentRow.venue == venue,
                    OrderIntentRow.venue_order_id == venue_order_id,
                )
            )
            return str(value) if value is not None else None

    async def upsert_venue_order(self, order: VenueOrder) -> None:
        async with self.transaction() as session:
            row = await session.get(VenueOrderRow, order.venue_order_id, with_for_update=True)
            values = {
                "client_order_id": order.client_order_id,
                "venue": order.venue,
                "status": order.status.value,
                "quantity": order.quantity,
                "cumulative_filled": order.cumulative_filled,
                "average_price": order.average_price,
                "updated_at": order.updated_at,
            }
            if row is None:
                session.add(VenueOrderRow(venue_order_id=order.venue_order_id, raw_payload={}, **values))
            else:
                for name, value in values.items():
                    setattr(row, name, value)

    async def insert_fill(self, fill: FillRecord) -> bool:
        async with self.transaction() as session:
            if await session.get(FillRow, fill.fill_id) is not None:
                return False
            session.add(
                FillRow(
                    fill_id=fill.fill_id,
                    client_order_id=fill.client_order_id,
                    venue_order_id=fill.venue_order_id,
                    venue=fill.venue,
                    quantity=fill.quantity,
                    price=fill.price,
                    fee=fill.fee,
                    occurred_at=fill.occurred_at,
                )
            )
            return True

    async def save_position(self, key: str, position: OpenPosition) -> None:
        payload = _position_to_json(position)
        async with self.transaction() as session:
            row = await session.get(PositionRow, key, with_for_update=True)
            values = {
                "symbol": position.market.symbol,
                "status": position.status,
                "first_venue": position.market.venue_a_label,
                "second_venue": position.market.venue_b_label,
                "first_quantity": Decimal(str(position.polymarket_contracts)),
                "second_quantity": Decimal(str(position.predict_fun_contracts)),
                "first_entry_price": Decimal(str(position.polymarket_entry_price)),
                "second_entry_price": Decimal(str(position.predict_fun_entry_price)),
                "payload": payload,
                "opened_at": position.opened_at,
                "updated_at": datetime.now(UTC),
            }
            if row is None:
                session.add(PositionRow(position_key=key, **values))
            else:
                for name, value in values.items():
                    setattr(row, name, value)

    async def remove_position(self, key: str) -> None:
        async with self.transaction() as session:
            row = await session.get(PositionRow, key, with_for_update=True)
            if row is not None:
                await session.delete(row)

    async def load_positions(self) -> list[OpenPosition]:
        async with self.sessions() as session:
            rows = await session.scalars(select(PositionRow))
            return [_position_from_json(row.payload) for row in rows]

    async def save_risk_state(self, state: dict[str, Any]) -> None:
        async with self.transaction() as session:
            row = await session.get(RiskStateRow, "global", with_for_update=True)
            values = {
                "loss_day": str(state["loss_day"]),
                "daily_loss_usd": Decimal(str(state["daily_loss_usd"])),
                "consecutive_api_errors": int(state["consecutive_api_errors"]),
                "paused": bool(state["paused"]),
                "pause_reason": state.get("pause_reason"),
                "updated_at": datetime.now(UTC),
            }
            if row is None:
                session.add(RiskStateRow(state_id="global", **values))
            else:
                for name, value in values.items():
                    setattr(row, name, value)

    async def load_risk_state(self) -> dict[str, Any] | None:
        async with self.sessions() as session:
            row = await session.get(RiskStateRow, "global")
            if row is None:
                return None
            return {
                "loss_day": row.loss_day,
                "daily_loss_usd": row.daily_loss_usd,
                "consecutive_api_errors": row.consecutive_api_errors,
                "paused": row.paused,
                "pause_reason": row.pause_reason,
            }

    async def list_mappings(self, status: MappingStatus | None = None) -> list[MarketMapping]:
        statement = select(MarketMappingRow)
        if status is not None:
            statement = statement.where(MarketMappingRow.status == status.value)
        async with self.sessions() as session:
            rows = await session.scalars(statement.order_by(MarketMappingRow.created_at))
            return [_mapping_from_row(row) for row in rows]

    async def upsert_market_candidates(self, markets: Sequence[MarketSpec]) -> None:
        now = datetime.now(UTC)
        async with self.transaction() as session:
            for market in markets:
                cutoff = market.cutoff_at or market.expires_at
                if cutoff is None:
                    continue
                if cutoff.tzinfo is None:
                    cutoff = cutoff.replace(tzinfo=UTC)
                fingerprint = market.rules_fingerprint or build_rules_fingerprint(
                    title=market.target_label or market.symbol,
                    resolution_source=market.resolution_source or "unknown",
                    cutoff_at=cutoff,
                    outcome_semantics=market.outcome_semantics or "unknown",
                    timezone_name=market.timezone_name,
                )
                canonical_id = _stable_id("canonical", fingerprint)
                canonical = await session.get(CanonicalMarketRow, canonical_id)
                if canonical is None:
                    session.add(
                        CanonicalMarketRow(
                            canonical_id=canonical_id,
                            title=market.target_label or market.symbol,
                            category=market.category or "unknown",
                            resolution_source=market.resolution_source or "unknown",
                            cutoff_at=cutoff,
                            timezone_name=market.timezone_name,
                            outcome_semantics=market.outcome_semantics or "unknown",
                            rules_fingerprint=fingerprint,
                        )
                    )
                identities = _market_identities(market)
                for venue, market_id in identities.items():
                    instrument_id = _stable_id(venue, market_id)
                    instrument = await session.get(VenueInstrumentRow, instrument_id)
                    if instrument is None:
                        yes_token, no_token = _venue_tokens(market, venue)
                        session.add(
                            VenueInstrumentRow(
                                instrument_id=instrument_id,
                                canonical_id=canonical_id,
                                venue=venue,
                                market_id=market_id,
                                yes_token_id=yes_token,
                                no_token_id=no_token,
                                closes_at=cutoff,
                                resolution_source=market.resolution_source,
                                rules_fingerprint=market.rules_fingerprint,
                                category=market.category,
                                metadata_json={},
                                updated_at=now,
                            )
                        )
                pairs = (("Polymarket", "Predict.fun"), ("Polymarket", "Myriad"), ("Predict.fun", "Myriad"))
                for left_venue, right_venue in pairs:
                    left_id, right_id = identities.get(left_venue), identities.get(right_venue)
                    if not left_id or not right_id:
                        continue
                    mapping_id = _stable_id(left_venue, left_id, right_venue, right_id)
                    mapping = await session.get(MarketMappingRow, mapping_id)
                    if mapping is None:
                        session.add(
                            MarketMappingRow(
                                mapping_id=mapping_id,
                                canonical_market_id=canonical_id,
                                left_venue=left_venue,
                                left_market_id=left_id,
                                right_venue=right_venue,
                                right_market_id=right_id,
                                status=MappingStatus.CANDIDATE.value,
                                rules_fingerprint=fingerprint,
                                created_at=now,
                                updated_at=now,
                            )
                        )
                    elif mapping.rules_fingerprint != fingerprint:
                        mapping.rules_fingerprint = fingerprint
                        mapping.status = MappingStatus.STALE.value
                        mapping.verified_at = None
                        mapping.verified_by = None
                        mapping.updated_at = now

    async def set_mapping_status(self, mapping_id: str, status: MappingStatus, *, operator: str | None = None) -> None:
        async with self.transaction() as session:
            row = await session.get(MarketMappingRow, mapping_id, with_for_update=True)
            if row is None:
                raise KeyError(f"Unknown mapping id: {mapping_id}")
            row.status = status.value
            row.verified_at = datetime.now(UTC) if status is MappingStatus.VERIFIED else None
            row.verified_by = operator if status is MappingStatus.VERIFIED else None
            row.updated_at = datetime.now(UTC)

    async def apply_verified_mappings(self, markets: Sequence[MarketSpec]) -> list[MarketSpec]:
        mappings = await self.list_mappings(MappingStatus.VERIFIED)
        route_pairs: dict[tuple[str, str], dict[str, str]] = {}
        for mapping in mappings:
            route = _route_name(mapping.left_venue, mapping.right_venue)
            route_pairs.setdefault((mapping.left_market_id, mapping.right_market_id), {})[route] = (
                mapping.rules_fingerprint
            )
            route_pairs.setdefault((mapping.right_market_id, mapping.left_market_id), {})[route] = (
                mapping.rules_fingerprint
            )
        result: list[MarketSpec] = []
        for market in markets:
            routes: set[str] = set(market.verified_routes)
            verified_fingerprint = market.rules_fingerprint
            identities = {
                "polymarket": market.polymarket_market_id or market.condition_id or "",
                "predict": market.predict_fun_market_id or "",
                "myriad": market.myriad_market_id or "",
            }
            for left_name, right_name in (
                ("polymarket", "predict"),
                ("polymarket", "myriad"),
                ("predict", "myriad"),
            ):
                left_id, right_id = identities[left_name], identities[right_name]
                if left_id and right_id:
                    matched = route_pairs.get((left_id, right_id), {})
                    routes.update(matched)
                    if matched and verified_fingerprint is None:
                        verified_fingerprint = next(iter(matched.values()))

            result.append(
                replace(
                    market,
                    verified_routes=frozenset(routes),
                    mapping_status=MappingStatus.VERIFIED if routes else market.mapping_status,
                    rules_fingerprint=verified_fingerprint,
                )
            )
        return result

    async def record_balances(self, venue: str, balances: dict[str, Decimal]) -> None:
        captured_at = datetime.now(UTC)
        async with self.transaction() as session:
            session.add_all(
                BalanceSnapshotRow(venue=venue, asset=asset, balance=balance, captured_at=captured_at)
                for asset, balance in balances.items()
            )

    async def record_reconciliation(self, result: ReconciliationResult) -> None:
        async with self.transaction() as session:
            session.add(
                ReconciliationRunRow(
                    venue=result.venue,
                    started_at=result.started_at,
                    completed_at=result.completed_at,
                    orders_checked=result.orders_checked,
                    fills_recorded=result.fills_recorded,
                    drift_count=result.drift_count,
                    success=result.success,
                    error=result.error,
                )
            )

    async def latest_reconciliation_failures(self) -> list[str]:
        """Return venues whose most recent reconciliation is failed or drifted."""
        async with self.sessions() as session:
            rows = await session.execute(
                select(
                    ReconciliationRunRow.venue,
                    ReconciliationRunRow.success,
                    ReconciliationRunRow.drift_count,
                    ReconciliationRunRow.error,
                ).order_by(ReconciliationRunRow.venue, ReconciliationRunRow.run_id.desc())
            )
            latest: dict[str, tuple[bool, int, str | None]] = {}
            for venue, success, drift_count, error in rows.all():
                latest.setdefault(str(venue), (bool(success), int(drift_count), str(error) if error else None))
            return [
                f"{venue}: {error or 'reconciliation drift'}"
                for venue, (success, drift_count, error) in latest.items()
                if not success or drift_count > 0
            ]

    async def audit(self, event_type: str, payload: dict[str, Any], correlation_id: str | None = None) -> None:
        async with self.transaction() as session:
            session.add(AuditEventRow(event_type=event_type, correlation_id=correlation_id, payload=payload))

    async def metrics_snapshot(self) -> dict[str, Any]:
        async with self.sessions() as session:
            canonical_count = int(await session.scalar(select(func.count()).select_from(CanonicalMarketRow)) or 0)
            mapping_rows = await session.execute(
                select(MarketMappingRow.status, func.count()).group_by(MarketMappingRow.status)
            )
            intent_rows = await session.execute(
                select(OrderIntentRow.status, func.count()).group_by(OrderIntentRow.status)
            )
            drift_total = int(
                await session.scalar(select(func.coalesce(func.sum(ReconciliationRunRow.drift_count), 0))) or 0
            )
            exposure = await session.scalar(
                select(
                    func.coalesce(
                        func.sum(
                            PositionRow.first_quantity * PositionRow.first_entry_price
                            + PositionRow.second_quantity * PositionRow.second_entry_price
                        ),
                        0,
                    )
                )
            )
            return {
                "canonical_markets": canonical_count,
                "mappings": {str(status): int(count) for status, count in mapping_rows.all()},
                "order_intents": {str(status): int(count) for status, count in intent_rows.all()},
                "reconciliation_drift_total": drift_total,
                "exposure_usd": Decimal(exposure or 0),
            }

    async def has_stale_mappings(self) -> bool:
        async with self.sessions() as session:
            count = await session.scalar(
                select(func.count())
                .select_from(MarketMappingRow)
                .where(MarketMappingRow.status == MappingStatus.STALE.value)
            )
            return bool(count)


def _advisory_lock_id(value: str) -> int:
    digest = hashlib.sha256(value.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], byteorder="big", signed=True)


def _mapping_from_row(row: MarketMappingRow) -> MarketMapping:
    return MarketMapping(
        mapping_id=row.mapping_id,
        canonical_market_id=row.canonical_market_id,
        left_venue=row.left_venue,
        left_market_id=row.left_market_id,
        right_venue=row.right_venue,
        right_market_id=row.right_market_id,
        status=MappingStatus(row.status),
        rules_fingerprint=row.rules_fingerprint,
        verified_at=row.verified_at,
        verified_by=row.verified_by,
    )


def _route_name(left_venue: str, right_venue: str) -> str:
    aliases = {"Polymarket": "polymarket", "Predict.fun": "predict", "Myriad": "myriad"}
    left = aliases.get(left_venue, left_venue.lower())
    right = aliases.get(right_venue, right_venue.lower())
    preferred = {
        frozenset(("polymarket", "predict")): "polymarket_predict",
        frozenset(("polymarket", "myriad")): "polymarket_myriad",
        frozenset(("predict", "myriad")): "predict_myriad",
    }
    return preferred.get(frozenset((left, right)), f"{left}_{right}")


def _stable_id(*parts: str) -> str:
    return hashlib.sha256("\x1f".join(parts).encode("utf-8")).hexdigest()


def _market_identities(market: MarketSpec) -> dict[str, str]:
    result: dict[str, str] = {}
    polymarket_id = market.polymarket_market_id or market.condition_id
    if polymarket_id:
        result["Polymarket"] = polymarket_id
    if market.predict_fun_market_id:
        result["Predict.fun"] = market.predict_fun_market_id
    if market.myriad_market_id:
        result["Myriad"] = market.myriad_market_id
    return result


def _venue_tokens(market: MarketSpec, venue: str) -> tuple[str, str]:
    if venue == "Polymarket":
        token = market.polymarket_token_id
        return (token, "") if market.polymarket_side.value == "YES" else ("", token)
    if venue == "Predict.fun":
        token = market.predict_fun_token_id
        return (token, "") if market.predict_fun_side.value == "YES" else ("", token)
    token = f"{market.myriad_market_id}:{market.myriad_side.value}" if market.myriad_market_id else ""
    return (token, "") if market.myriad_side.value == "YES" else ("", token)
