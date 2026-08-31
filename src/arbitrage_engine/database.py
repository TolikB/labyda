from __future__ import annotations

import asyncio
import hashlib
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import (
    JSON,
    Boolean,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    and_,
    exists,
    func,
    or_,
    select,
    text,
    tuple_,
)
from sqlalchemy.engine import make_url
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from .market_mapping import route_key
from .market_mapping import rules_fingerprint as build_rules_fingerprint
from .models import (
    ExecutionReport,
    FillRecord,
    MappingStatus,
    MarketMapping,
    MarketSpec,
    OpenPosition,
    OrderIntent,
    OrderIntentStatus,
    ReconciliationResult,
    RedemptionIntent,
    RedemptionIntentStatus,
    ResidualExitSnapshot,
    VenueOrder,
    apply_residual_exit_snapshot,
)
from .positions import _position_from_json, _position_to_json

MONEY = Numeric(38, 18)
_TRADER_LOCK_NAME = "arbitrage-engine-production-trader"
_SYNTHETIC_MARKET_KEY_PREFIXES = ("integration:", "restart:")
_SYNTHETIC_TOKEN_IDS = {"integration-token", "restart-token"}
_LEGACY_ORDER_INTENT_ROUTE_ALIASES = {
    "polymarket_predict": "Polymarket:Predict.fun",
    "polymarket_sx": "Polymarket:SX Bet",
    "polymarket_myriad": "Polymarket:Myriad",
    "predict_myriad": "Predict.fun:Myriad",
    "predict_sx": "Predict.fun:SX Bet",
    "sx_myriad": "SX Bet:Myriad",
}
_MARKET_CANDIDATE_UPSERT_CHUNK_SIZE = 128
_MAPPING_REVIEW_QUERY_CHUNK_SIZE = 256
_SUPPORTED_VENUES = ("Myriad", "Polymarket", "Predict.fun", "SX Bet")


@dataclass(frozen=True)
class _PreparedMarketCandidate:
    market: MarketSpec
    cutoff: datetime
    canonical_title: str
    canonical_fingerprint: str
    canonical_id: str
    identities: dict[str, str]
    cache_key: str
    persistence_signature: str


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
    match_strategy: Mapped[str | None] = mapped_column(String(24), nullable=True)
    verified_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    verified_by: Mapped[str | None] = mapped_column(String(128), nullable=True)
    last_discovered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
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


class RedemptionIntentRow(Base):
    __tablename__ = "redemption_intents"

    redemption_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    position_key: Mapped[str] = mapped_column(String(768), index=True)
    venue: Mapped[str] = mapped_column(String(32), index=True)
    market_id: Mapped[str] = mapped_column(String(256))
    condition_id: Mapped[str] = mapped_column(String(256))
    collateral_token: Mapped[str] = mapped_column(String(128))
    expected_contracts: Mapped[Decimal] = mapped_column(MONEY)
    status: Mapped[str] = mapped_column(String(32), index=True)
    tx_hash: Mapped[str | None] = mapped_column(String(256), nullable=True, index=True)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))

    __table_args__ = (
        Index(
            "uq_redemption_position_venue_condition",
            "position_key",
            "venue",
            "condition_id",
            unique=True,
        ),
    )


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
    def __init__(
        self,
        database_url: str,
        *,
        echo: bool = False,
        runtime_instance_id: str = "global",
        enabled_routes: Sequence[str] | None = None,
    ) -> None:
        engine_options: dict[str, Any] = {"echo": echo, "pool_pre_ping": True}
        url = make_url(database_url)
        if url.get_backend_name() != "sqlite":
            engine_options.update(
                pool_size=10,
                max_overflow=10,
                pool_timeout=30,
                pool_recycle=1800,
                pool_use_lifo=True,
            )
        if url.drivername.endswith("+asyncpg"):
            engine_options["connect_args"] = {"timeout": 15.0, "command_timeout": 30.0}
        self.engine = create_async_engine(database_url, **engine_options)
        self.sessions = async_sessionmaker(self.engine, expire_on_commit=False)
        self._lock_connection: AsyncConnection | None = None
        self.runtime_instance_id = runtime_instance_id or "global"
        self.enabled_routes = tuple(str(route) for route in (enabled_routes or ()))
        self.order_intent_routes = tuple(
            dict.fromkeys(
                value
                for route in self.enabled_routes
                for value in (route, _LEGACY_ORDER_INTENT_ROUTE_ALIASES.get(route, route))
            )
        )
        self.active_venues = _active_venues_for_routes(self.enabled_routes)
        self._market_candidate_signatures: dict[str, str] = {}

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

    async def schema_revision(self) -> str | None:
        try:
            async with self.engine.connect() as connection:
                value = await connection.scalar(text("SELECT version_num FROM alembic_version LIMIT 1"))
            return str(value) if value not in (None, "") else None
        except Exception:
            return None

    async def acquire_trader_lock(self) -> bool:
        if self.engine.dialect.name != "postgresql":
            return False
        if self._lock_connection is not None:
            return True
        connection = await self.engine.connect()
        lock_id = _advisory_lock_id(f"{_TRADER_LOCK_NAME}:{self.runtime_instance_id}")
        acquired = bool(await connection.scalar(text("SELECT pg_try_advisory_lock(:lock_id)"), {"lock_id": lock_id}))
        if not acquired:
            await connection.close()
            return False
        self._lock_connection = connection
        return True

    async def release_trader_lock(self) -> None:
        if self._lock_connection is None:
            return
        lock_id = _advisory_lock_id(f"{_TRADER_LOCK_NAME}:{self.runtime_instance_id}")
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
            query = select(OrderIntentRow).where(OrderIntentRow.status.not_in(terminal))
            if self.order_intent_routes:
                query = query.where(OrderIntentRow.route.in_(self.order_intent_routes))
            result = await session.scalars(query)
            return list(result)

    async def order_intents_for_fill_reconciliation(
        self,
        venue: str,
        since: datetime | None,
    ) -> list[OrderIntentRow]:
        async with self.sessions() as session:
            query = select(OrderIntentRow).where(
                OrderIntentRow.venue == venue,
                OrderIntentRow.venue_order_id.is_not(None),
            )
            if self.order_intent_routes:
                query = query.where(OrderIntentRow.route.in_(self.order_intent_routes))
            if since is not None:
                normalized_since = since if since.tzinfo is not None else since.replace(tzinfo=UTC)
                query = query.where(
                    or_(
                        OrderIntentRow.created_at >= normalized_since,
                        OrderIntentRow.updated_at >= normalized_since,
                    )
                )
            result = await session.scalars(query.order_by(OrderIntentRow.created_at.asc()))
            return list(result)

    async def client_order_id_for_venue_order(self, venue: str, venue_order_id: str) -> str | None:
        async with self.sessions() as session:
            value = await session.scalar(
                select(OrderIntentRow.client_order_id).where(
                    OrderIntentRow.venue == venue,
                    OrderIntentRow.venue_order_id == venue_order_id,
                )
            )
            if value is not None:
                return str(value)
            if ":" not in venue_order_id:
                suffix_match = await session.scalar(
                    select(OrderIntentRow.client_order_id).where(
                        OrderIntentRow.venue == venue,
                        OrderIntentRow.venue_order_id.like(f"%:{venue_order_id}"),
                    )
                )
                if suffix_match is not None:
                    return str(suffix_match)
            return None

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

    async def record_residual_exit_exposure(
        self,
        *,
        market_key: str,
        venue: str,
        requested_contracts: Decimal,
        report: ExecutionReport,
        residual_contracts: Decimal,
    ) -> bool:
        """Persist terminal CE unwind accounting without double-applying retries."""
        async with self.transaction() as session:
            row = await session.get(PositionRow, market_key, with_for_update=True)
            if row is None:
                raise RuntimeError(f"position {market_key} is missing for residual exit reconciliation")
            position = _position_from_json(row.payload)
            updated = _position_after_residual_exit(
                position,
                venue=venue,
                venue_order_id=report.order_id,
                requested_contracts=requested_contracts,
                closed_contracts=report.amount_filled,
                average_exit_price=report.avg_price,
                residual_contracts=residual_contracts,
            )
            changed = updated != position
            row.status = updated.status
            row.payload = _position_to_json(updated)
            row.updated_at = datetime.now(UTC)
            return changed

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
        return [position for _, position in await self.load_position_entries()]

    async def load_position_entries(self) -> list[tuple[str, OpenPosition]]:
        async with self.sessions() as session:
            rows = await session.scalars(select(PositionRow))
            positions = [(row.position_key, _position_from_json(row.payload)) for row in rows]
            if not self.enabled_routes:
                return positions
            return [
                (key, position)
                for key, position in positions
                if _position_route(position) in self.enabled_routes
            ]

    async def recent_fills(self, *, since: datetime | None = None, limit: int = 50) -> list[dict[str, Any]]:
        async with self.sessions() as session:
            query = select(FillRow).order_by(FillRow.occurred_at.desc()).limit(limit)
            if since is not None:
                query = query.where(FillRow.occurred_at >= since)
            rows = (await session.scalars(query)).all()
            intent_metadata_by_id: dict[str, dict[str, str]] = {}
            if rows:
                route_rows = (
                    await session.execute(
                        select(
                            OrderIntentRow.client_order_id,
                            OrderIntentRow.route,
                            OrderIntentRow.market_key,
                            OrderIntentRow.token_id,
                        ).where(OrderIntentRow.client_order_id.in_(tuple(row.client_order_id for row in rows)))
                    )
                ).all()
                intent_metadata_by_id = {
                    str(client_order_id): {
                        "route": str(route),
                        "market_key": str(market_key),
                        "token_id": str(token_id),
                    }
                    for client_order_id, route, market_key, token_id in route_rows
                }
            if self.enabled_routes and rows:
                allowed_ids = {
                    client_order_id
                    for client_order_id, metadata in intent_metadata_by_id.items()
                    if metadata["route"] in self.enabled_routes
                }
                rows = [row for row in rows if row.client_order_id in allowed_ids]
            return [
                {
                    "fill_id": row.fill_id,
                    "client_order_id": row.client_order_id,
                    "route": intent_metadata_by_id.get(row.client_order_id, {}).get("route"),
                    "market_key": intent_metadata_by_id.get(row.client_order_id, {}).get("market_key"),
                    "token_id": intent_metadata_by_id.get(row.client_order_id, {}).get("token_id"),
                    "synthetic": _is_synthetic_market_artifact(
                        intent_metadata_by_id.get(row.client_order_id, {}).get("market_key"),
                        intent_metadata_by_id.get(row.client_order_id, {}).get("token_id"),
                    ),
                    "venue_order_id": row.venue_order_id,
                    "venue": row.venue,
                    "quantity": str(row.quantity),
                    "price": str(row.price),
                    "fee": str(row.fee),
                    "occurred_at": row.occurred_at.isoformat(),
                }
                for row in rows
            ]

    async def fills_for_client_order_ids(self, client_order_ids: Sequence[str]) -> dict[str, list[dict[str, Any]]]:
        if not client_order_ids:
            return {}
        async with self.sessions() as session:
            rows = (
                await session.scalars(
                    select(FillRow)
                    .where(FillRow.client_order_id.in_(tuple(client_order_ids)))
                    .order_by(FillRow.occurred_at.desc())
                )
            ).all()
        result: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            result.setdefault(row.client_order_id, []).append(
                {
                    "fill_id": row.fill_id,
                    "client_order_id": row.client_order_id,
                    "venue_order_id": row.venue_order_id,
                    "venue": row.venue,
                    "quantity": str(row.quantity),
                    "price": str(row.price),
                    "fee": str(row.fee),
                    "occurred_at": row.occurred_at.isoformat(),
                }
            )
        return result

    async def create_redemption_intent(self, intent: RedemptionIntent) -> bool:
        try:
            async with self.transaction() as session:
                session.add(
                    RedemptionIntentRow(
                        redemption_id=intent.redemption_id,
                        position_key=intent.position_key,
                        venue=intent.venue,
                        market_id=intent.market_id,
                        condition_id=intent.condition_id,
                        collateral_token=intent.collateral_token,
                        expected_contracts=intent.expected_contracts,
                        status=intent.status.value,
                        tx_hash=intent.tx_hash,
                        last_error=intent.last_error,
                        created_at=intent.created_at,
                        updated_at=intent.updated_at,
                    )
                )
            return True
        except IntegrityError:
            return False

    async def get_redemption_intent(
        self,
        position_key: str,
        venue: str,
        condition_id: str,
    ) -> RedemptionIntent | None:
        async with self.sessions() as session:
            row = await session.scalar(
                select(RedemptionIntentRow).where(
                    RedemptionIntentRow.position_key == position_key,
                    RedemptionIntentRow.venue == venue,
                    RedemptionIntentRow.condition_id == condition_id,
                )
            )
            return _redemption_intent_from_row(row) if row is not None else None

    async def update_redemption_intent(
        self,
        redemption_id: str,
        status: RedemptionIntentStatus,
        *,
        tx_hash: str | None = None,
        error: str | None = None,
    ) -> RedemptionIntent:
        async with self.transaction() as session:
            row = await session.get(RedemptionIntentRow, redemption_id, with_for_update=True)
            if row is None:
                raise KeyError(f"Unknown redemption id: {redemption_id}")
            row.status = status.value
            if tx_hash is not None:
                row.tx_hash = tx_hash
            row.last_error = error
            row.updated_at = datetime.now(UTC)
            await session.flush()
            return _redemption_intent_from_row(row)

    async def unresolved_redemption_intents(self) -> list[RedemptionIntent]:
        terminal = {RedemptionIntentStatus.CONFIRMED.value, RedemptionIntentStatus.MANUAL_REVIEW.value}
        async with self.sessions() as session:
            rows = list(
                await session.scalars(
                select(RedemptionIntentRow)
                .where(RedemptionIntentRow.status.not_in(terminal))
                .order_by(RedemptionIntentRow.created_at)
            )
            )
            if self.enabled_routes and rows:
                allowed_position_keys = await self._allowed_position_keys(session)
                rows = [row for row in rows if row.position_key in allowed_position_keys]
            return [_redemption_intent_from_row(row) for row in rows]

    async def save_risk_state(self, state: dict[str, Any]) -> None:
        async with self.transaction() as session:
            row = await session.get(RiskStateRow, self.runtime_instance_id, with_for_update=True)
            values = {
                "loss_day": str(state["loss_day"]),
                "daily_loss_usd": Decimal(str(state["daily_loss_usd"])),
                "consecutive_api_errors": int(state["consecutive_api_errors"]),
                "paused": bool(state["paused"]),
                "pause_reason": state.get("pause_reason"),
                "updated_at": datetime.now(UTC),
            }
            if row is None:
                session.add(RiskStateRow(state_id=self.runtime_instance_id, **values))
            else:
                for name, value in values.items():
                    setattr(row, name, value)

    async def load_risk_state(self) -> dict[str, Any] | None:
        async with self.sessions() as session:
            row = await session.get(RiskStateRow, self.runtime_instance_id)
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

    async def mapping_review_snapshot(self, mappings: Sequence[MarketMapping]) -> dict[str, dict[str, dict[str, Any]]]:
        canonical_ids = sorted({mapping.canonical_market_id for mapping in mappings})
        identities = sorted(
            {
                (mapping.left_venue, mapping.left_market_id)
                for mapping in mappings
            }
            | {
                (mapping.right_venue, mapping.right_market_id)
                for mapping in mappings
            }
        )
        if not canonical_ids and not identities:
            return {"canonical_markets": {}, "venue_instruments": {}}
        async with self.sessions() as session:
            canonical_rows: list[CanonicalMarketRow] = []
            for offset in range(0, len(canonical_ids), _MAPPING_REVIEW_QUERY_CHUNK_SIZE):
                canonical_chunk = canonical_ids[offset : offset + _MAPPING_REVIEW_QUERY_CHUNK_SIZE]
                canonical_result = await session.scalars(
                    select(CanonicalMarketRow).where(CanonicalMarketRow.canonical_id.in_(canonical_chunk))
                )
                canonical_rows.extend(canonical_result)

            instrument_rows: list[VenueInstrumentRow] = []
            for offset in range(0, len(identities), _MAPPING_REVIEW_QUERY_CHUNK_SIZE):
                identity_chunk = identities[offset : offset + _MAPPING_REVIEW_QUERY_CHUNK_SIZE]
                instrument_result = await session.scalars(
                    select(VenueInstrumentRow).where(
                        tuple_(VenueInstrumentRow.venue, VenueInstrumentRow.market_id).in_(identity_chunk)
                    )
                )
                instrument_rows.extend(instrument_result)
            return {
                "canonical_markets": {
                    row.canonical_id: {
                        "canonical_market_id": row.canonical_id,
                        "title": row.title,
                        "category": row.category,
                        "resolution_source": row.resolution_source,
                        "cutoff_at": row.cutoff_at.isoformat(),
                        "timezone_name": row.timezone_name,
                        "outcome_semantics": row.outcome_semantics,
                        "rules_fingerprint": row.rules_fingerprint,
                    }
                    for row in canonical_rows
                },
                "venue_instruments": {
                    f"{row.venue}:{row.market_id}": {
                        "canonical_market_id": row.canonical_id,
                        "venue": row.venue,
                        "market_id": row.market_id,
                        "yes_token_id": row.yes_token_id,
                        "no_token_id": row.no_token_id,
                        "closes_at": row.closes_at.isoformat(),
                        "resolution_source": row.resolution_source,
                        "rules_fingerprint": row.rules_fingerprint,
                        "category": row.category,
                    }
                    for row in instrument_rows
                },
            }

    async def upsert_market_candidates(
        self,
        markets: Sequence[MarketSpec],
        *,
        mark_seen: bool = False,
    ) -> None:
        if len(markets) > _MARKET_CANDIDATE_UPSERT_CHUNK_SIZE:
            for offset in range(0, len(markets), _MARKET_CANDIDATE_UPSERT_CHUNK_SIZE):
                await self.upsert_market_candidates(
                    markets[offset : offset + _MARKET_CANDIDATE_UPSERT_CHUNK_SIZE],
                    mark_seen=mark_seen,
                )
                await asyncio.sleep(0)
            return

        now = datetime.now(UTC)
        prepared: list[_PreparedMarketCandidate] = []
        canonical_ids: set[str] = set()
        instrument_ids: set[str] = set()
        mapping_ids: set[str] = set()
        for market in markets:
            cutoff = market.cutoff_at or market.expires_at
            if cutoff is None:
                continue
            if cutoff.tzinfo is None:
                cutoff = cutoff.replace(tzinfo=UTC)
            canonical_title = _canonical_market_title(market)
            canonical_fingerprint = _normalized_rules_fingerprint(
                build_rules_fingerprint(
                    title=canonical_title,
                    resolution_source=market.resolution_source or "unknown",
                    cutoff_at=cutoff,
                    outcome_semantics=market.outcome_semantics or "unknown",
                    timezone_name=market.timezone_name,
                )
            )
            canonical_id = _stable_id("canonical", canonical_fingerprint)
            identities = _market_identities(market)
            cache_key = _market_candidate_cache_key(market, identities)
            persistence_signature = _market_candidate_persistence_signature(
                market=market,
                cutoff=cutoff,
                canonical_title=canonical_title,
                canonical_fingerprint=canonical_fingerprint,
                canonical_id=canonical_id,
                identities=identities,
            )
            if not mark_seen and self._market_candidate_signatures.get(cache_key) == persistence_signature:
                continue
            prepared.append(
                _PreparedMarketCandidate(
                    market=market,
                    cutoff=cutoff,
                    canonical_title=canonical_title,
                    canonical_fingerprint=canonical_fingerprint,
                    canonical_id=canonical_id,
                    identities=identities,
                    cache_key=cache_key,
                    persistence_signature=persistence_signature,
                )
            )
            canonical_ids.add(canonical_id)
            instrument_ids.update(_stable_id(venue, market_id) for venue, market_id in identities.items())
            venues = list(identities)
            for index, left_venue in enumerate(venues):
                left_id = identities.get(left_venue)
                if not left_id:
                    continue
                for right_venue in venues[index + 1 :]:
                    right_id = identities.get(right_venue)
                    if right_id:
                        mapping_ids.add(_stable_id(left_venue, left_id, right_venue, right_id))

        if not prepared:
            return

        async with self.transaction() as session:
            canonical_rows = await session.scalars(
                select(CanonicalMarketRow).where(CanonicalMarketRow.canonical_id.in_(canonical_ids))
            )
            instrument_rows = await session.scalars(
                select(VenueInstrumentRow).where(VenueInstrumentRow.instrument_id.in_(instrument_ids))
            )
            mapping_rows = await session.scalars(
                select(MarketMappingRow).where(MarketMappingRow.mapping_id.in_(mapping_ids))
            )
            canonical_by_id = {row.canonical_id: row for row in canonical_rows}
            instrument_by_id = {row.instrument_id: row for row in instrument_rows}
            mapping_by_id = {row.mapping_id: row for row in mapping_rows}

            for item in prepared:
                market = item.market
                canonical = canonical_by_id.get(item.canonical_id)
                if canonical is None:
                    canonical = CanonicalMarketRow(
                        canonical_id=item.canonical_id,
                        title=item.canonical_title,
                        category=market.category or "unknown",
                        resolution_source=market.resolution_source or "unknown",
                        cutoff_at=item.cutoff,
                        timezone_name=market.timezone_name,
                        outcome_semantics=market.outcome_semantics or "unknown",
                        rules_fingerprint=item.canonical_fingerprint,
                    )
                    session.add(canonical)
                    canonical_by_id[item.canonical_id] = canonical
                for venue, market_id in item.identities.items():
                    instrument_id = _stable_id(venue, market_id)
                    instrument = instrument_by_id.get(instrument_id)
                    yes_token, no_token = _venue_tokens(market, venue)
                    if instrument is None:
                        instrument = VenueInstrumentRow(
                            instrument_id=instrument_id,
                            canonical_id=item.canonical_id,
                            venue=venue,
                            market_id=market_id,
                            yes_token_id=yes_token,
                            no_token_id=no_token,
                            closes_at=item.cutoff,
                            resolution_source=market.resolution_source,
                            rules_fingerprint=item.canonical_fingerprint,
                            category=market.category,
                            metadata_json={},
                            updated_at=now,
                        )
                        session.add(instrument)
                        instrument_by_id[instrument_id] = instrument
                    else:
                        instrument_changed = False
                        instrument_updates = {
                            "canonical_id": item.canonical_id,
                            "closes_at": item.cutoff,
                            "resolution_source": market.resolution_source,
                            "rules_fingerprint": item.canonical_fingerprint,
                            "category": market.category,
                        }
                        if yes_token:
                            instrument_updates["yes_token_id"] = yes_token
                        if no_token:
                            instrument_updates["no_token_id"] = no_token
                        for attribute, value in instrument_updates.items():
                            if getattr(instrument, attribute) != value:
                                setattr(instrument, attribute, value)
                                instrument_changed = True
                        if instrument_changed:
                            instrument.updated_at = now
                venues = list(item.identities)
                for index, left_venue in enumerate(venues):
                    left_id = item.identities.get(left_venue)
                    if not left_id:
                        continue
                    for right_venue in venues[index + 1 :]:
                        right_id = item.identities.get(right_venue)
                        if not right_id:
                            continue
                        mapping_id = _stable_id(left_venue, left_id, right_venue, right_id)
                        mapping = mapping_by_id.get(mapping_id)
                        if mapping is None:
                            mapping = MarketMappingRow(
                                mapping_id=mapping_id,
                                canonical_market_id=item.canonical_id,
                                left_venue=left_venue,
                                left_market_id=left_id,
                                right_venue=right_venue,
                                right_market_id=right_id,
                                status=MappingStatus.CANDIDATE.value,
                                rules_fingerprint=item.canonical_fingerprint,
                                match_strategy=market.mapping_strategy,
                                last_discovered_at=now if mark_seen else None,
                                created_at=now,
                                updated_at=now,
                            )
                            session.add(mapping)
                            mapping_by_id[mapping_id] = mapping
                        else:
                            mapping_changed = False
                            if mapping.rules_fingerprint != item.canonical_fingerprint:
                                mapping.canonical_market_id = item.canonical_id
                                mapping.rules_fingerprint = item.canonical_fingerprint
                                mapping.status = MappingStatus.STALE.value
                                mapping.verified_at = None
                                mapping.verified_by = None
                                mapping_changed = True
                            if (
                                market.mapping_strategy is not None
                                and mapping.match_strategy != market.mapping_strategy
                            ):
                                if (
                                    mapping.status == MappingStatus.VERIFIED.value
                                    and (
                                        (mapping.match_strategy is None and market.mapping_strategy != "exact_id")
                                        or (
                                            mapping.match_strategy is not None
                                            and mapping.match_strategy != market.mapping_strategy
                                        )
                                    )
                                ):
                                    mapping.status = MappingStatus.STALE.value
                                    mapping.verified_at = None
                                    mapping.verified_by = None
                                mapping.match_strategy = market.mapping_strategy
                                mapping_changed = True
                            if mark_seen:
                                mapping.last_discovered_at = now
                            if mapping_changed:
                                mapping.updated_at = now

        self._market_candidate_signatures.update(
            {item.cache_key: item.persistence_signature for item in prepared}
        )

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
        canonical_ids = {mapping.canonical_market_id for mapping in mappings}
        canonical_metadata: dict[str, tuple[str, str, str, datetime]] = {}
        if canonical_ids:
            async with self.sessions() as session:
                rows = await session.scalars(
                    select(CanonicalMarketRow).where(CanonicalMarketRow.canonical_id.in_(canonical_ids))
                )
                canonical_metadata = {
                    row.canonical_id: (row.resolution_source, row.outcome_semantics, row.category, row.cutoff_at)
                    for row in rows
                }

        route_pairs: dict[tuple[str, str], dict[str, tuple[str, str, str, str, datetime]]] = {}
        metadata_by_fingerprint: dict[str, tuple[str, str, str, datetime]] = {}
        for mapping in mappings:
            route = _route_name(mapping.left_venue, mapping.right_venue)
            source, semantics, category, cutoff = canonical_metadata.get(
                mapping.canonical_market_id,
                ("unknown", "unknown", "unknown", datetime.now(UTC)),
            )
            metadata = (mapping.rules_fingerprint, source, semantics, category, cutoff)
            metadata_by_fingerprint.setdefault(mapping.rules_fingerprint, (source, semantics, category, cutoff))
            route_pairs.setdefault((mapping.left_market_id, mapping.right_market_id), {})[route] = metadata
            route_pairs.setdefault((mapping.right_market_id, mapping.left_market_id), {})[route] = metadata
        result: list[MarketSpec] = []
        for market in markets:
            routes: set[str] = set(market.verified_routes)
            verified_fingerprint = market.rules_fingerprint
            verified_source = market.resolution_source
            verified_semantics = market.outcome_semantics
            verified_category = market.category
            verified_cutoff = market.cutoff_at
            identities = _market_identities(market)
            venues = list(identities)
            for index, left_name in enumerate(venues):
                left_id = identities[left_name]
                for right_name in venues[index + 1 :]:
                    right_id = identities[right_name]
                    matched = route_pairs.get((left_id, right_id), {})
                    routes.update(matched)
                    if matched:
                        fingerprint, source, semantics, category, cutoff = next(iter(matched.values()))
                        if verified_fingerprint is None:
                            verified_fingerprint = fingerprint
                        if _missing_verified_metadata(verified_source):
                            verified_source = _known_verified_metadata(source)
                        if _missing_verified_metadata(verified_semantics):
                            verified_semantics = _known_verified_metadata(semantics)
                        if _missing_verified_metadata(verified_category):
                            verified_category = _known_verified_metadata(category)
                        if verified_cutoff is None:
                            verified_cutoff = cutoff

            # Route verification can be supplied by a discovery snapshot even
            # when its venue IDs differ from the pair persisted in the mapping.
            # The verified rules fingerprint still provides a durable, exact
            # canonical identity for recovering its metadata.
            if verified_fingerprint:
                fingerprint_metadata = metadata_by_fingerprint.get(verified_fingerprint)
                if fingerprint_metadata is not None:
                    source, semantics, category, cutoff = fingerprint_metadata
                    if _missing_verified_metadata(verified_source):
                        verified_source = _known_verified_metadata(source)
                    if _missing_verified_metadata(verified_semantics):
                        verified_semantics = _known_verified_metadata(semantics)
                    if _missing_verified_metadata(verified_category):
                        verified_category = _known_verified_metadata(category)
                    if verified_cutoff is None:
                        verified_cutoff = cutoff

            result.append(
                replace(
                    market,
                    verified_routes=frozenset(routes),
                    mapping_status=MappingStatus.VERIFIED if routes else market.mapping_status,
                    rules_fingerprint=verified_fingerprint,
                    resolution_source=verified_source,
                    outcome_semantics=verified_semantics,
                    category=verified_category,
                    cutoff_at=verified_cutoff,
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

    async def latest_balance_snapshots(self) -> dict[str, dict[str, dict[str, Any]]]:
        async with self.sessions() as session:
            rows = await session.scalars(
                select(BalanceSnapshotRow).order_by(
                    BalanceSnapshotRow.captured_at.desc(),
                    BalanceSnapshotRow.snapshot_id.desc(),
                )
            )
            result: dict[str, dict[str, dict[str, Any]]] = {}
            seen: set[tuple[str, str]] = set()
            for row in rows:
                key = (row.venue, row.asset)
                if key in seen:
                    continue
                seen.add(key)
                result.setdefault(row.venue, {})[row.asset] = {
                    "balance": str(row.balance),
                    "captured_at": row.captured_at.isoformat(),
                }
            if not self.active_venues:
                return result
            return {venue: balances for venue, balances in result.items() if venue in self.active_venues}

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
            latest_run_ids = select(
                ReconciliationRunRow.venue,
                func.max(ReconciliationRunRow.run_id).label("run_id"),
            )
            if self.active_venues:
                latest_run_ids = latest_run_ids.where(ReconciliationRunRow.venue.in_(self.active_venues))
            latest_runs = latest_run_ids.group_by(ReconciliationRunRow.venue).subquery()
            query = select(
                ReconciliationRunRow.venue,
                ReconciliationRunRow.success,
                ReconciliationRunRow.drift_count,
                ReconciliationRunRow.error,
            ).join(latest_runs, ReconciliationRunRow.run_id == latest_runs.c.run_id)
            rows = await session.execute(query)
            return [
                f"{venue}: {error or 'reconciliation drift'}"
                for venue, success, drift_count, error in rows.all()
                if not success or drift_count > 0
            ]

    async def audit(self, event_type: str, payload: dict[str, Any], correlation_id: str | None = None) -> None:
        payload = dict(payload)
        payload.setdefault("runtime_instance_id", self.runtime_instance_id)
        async with self.transaction() as session:
            session.add(AuditEventRow(event_type=event_type, correlation_id=correlation_id, payload=payload))

    async def record_runtime_balance_state(self, payload: dict[str, Any]) -> None:
        runtime_payload = dict(payload)
        runtime_payload.setdefault("runtime_instance_id", self.runtime_instance_id)
        await self.audit("runtime_balance_state", runtime_payload)

    async def record_shadow_preflight_evidence(self, payload: dict[str, Any]) -> None:
        evidence = dict(payload)
        route = str(evidence.get("route") or "").strip()
        market_key = str(evidence.get("market_key") or "").strip()
        release_sha = str(evidence.get("release_sha") or "").strip()
        if not route or not market_key or not release_sha:
            raise ValueError("shadow preflight evidence requires route, market_key, and release_sha")
        evidence["runtime_instance_id"] = self.runtime_instance_id
        await self.audit("shadow_preflight_evidence", evidence, correlation_id=route)

    async def latest_shadow_preflight_evidence_by_route(self) -> dict[str, dict[str, Any]]:
        async with self.sessions() as session:
            rows = (
                await session.scalars(
                    select(AuditEventRow)
                    .where(AuditEventRow.event_type == "shadow_preflight_evidence")
                    .order_by(AuditEventRow.created_at.desc(), AuditEventRow.event_id.desc())
                    .limit(1000)
                )
            ).all()
            result: dict[str, dict[str, Any]] = {}
            for row in rows:
                payload = dict(row.payload or {})
                if str(payload.get("runtime_instance_id") or "global") != self.runtime_instance_id:
                    continue
                route = str(payload.get("route") or "").strip()
                if not route or route in result:
                    continue
                if self.enabled_routes and route not in self.enabled_routes:
                    continue
                payload.setdefault("recorded_at", row.created_at.isoformat())
                result[route] = payload
                if self.enabled_routes and len(result) >= len(self.enabled_routes):
                    break
            return result

    async def latest_runtime_balance_state(self) -> dict[str, Any] | None:
        async with self.sessions() as session:
            rows = (
                await session.scalars(
                    select(AuditEventRow)
                    .where(AuditEventRow.event_type == "runtime_balance_state")
                    .order_by(AuditEventRow.created_at.desc(), AuditEventRow.event_id.desc())
                    .limit(100)
                )
            ).all()
            for row in rows:
                payload = dict(row.payload or {})
                payload_instance_id = str(payload.get("runtime_instance_id") or "global")
                if payload_instance_id != self.runtime_instance_id:
                    continue
                payload.setdefault("recorded_at", row.created_at.isoformat())
                return payload
            return None

    async def metrics_snapshot(self) -> dict[str, Any]:
        async with self.sessions() as session:
            canonical_count = int(await session.scalar(select(func.count()).select_from(CanonicalMarketRow)) or 0)
            mapping_rows = await session.execute(
                select(MarketMappingRow.status, func.count()).group_by(MarketMappingRow.status)
            )
            intent_query = select(OrderIntentRow.status, func.count()).group_by(OrderIntentRow.status)
            if self.order_intent_routes:
                intent_query = intent_query.where(OrderIntentRow.route.in_(self.order_intent_routes))
            intent_rows = await session.execute(intent_query)
            drift_total = 0
            for venue in self.active_venues or _SUPPORTED_VENUES:
                latest_drift = await session.scalar(
                    select(ReconciliationRunRow.drift_count)
                    .where(ReconciliationRunRow.venue == venue)
                    .order_by(ReconciliationRunRow.run_id.desc())
                    .limit(1)
                )
                drift_total += int(latest_drift or 0)
            exposure = Decimal(0)
            pending_unhedged_exposure = Decimal(0)
            for position in await self.load_positions():
                first_exposure = Decimal(str(position.polymarket_contracts)) * Decimal(
                    str(position.polymarket_entry_price)
                )
                second_exposure = Decimal(str(position.predict_fun_contracts)) * Decimal(
                    str(position.predict_fun_entry_price)
                )
                exposure += first_exposure + second_exposure
                if position.status in {"entry_pending", "unwind_pending", "partial_exit_pending", "manual_review"}:
                    pending_unhedged_exposure += first_exposure + second_exposure
            return {
                "canonical_markets": canonical_count,
                "mappings": {str(status): int(count) for status, count in mapping_rows.all()},
                "order_intents": {str(status): int(count) for status, count in intent_rows.all()},
                "reconciliation_drift_total": drift_total,
                "exposure_usd": Decimal(exposure or 0),
                "pending_unhedged_exposure_usd": Decimal(pending_unhedged_exposure or 0),
            }

    async def runtime_audit_snapshot(self) -> dict[str, Any]:
        unresolved_orders = await self.unresolved_order_intents()
        unresolved_redemptions = await self.unresolved_redemption_intents()
        positions = await self.load_positions()
        latest_balances = await self.latest_balance_snapshots()
        reconciliation_failures = await self.latest_reconciliation_failures()
        metrics = await self.metrics_snapshot()
        risk_state = await self.load_risk_state()
        runtime_balance_state = await self.latest_runtime_balance_state()
        shadow_preflight_evidence = await self.latest_shadow_preflight_evidence_by_route()

        order_intents_by_venue: dict[str, dict[str, Any]] = {}
        for intent_row in unresolved_orders:
            venue = order_intents_by_venue.setdefault(intent_row.venue, {"count": 0, "by_status": {}})
            venue["count"] = int(venue["count"]) + 1
            by_status = venue["by_status"]
            assert isinstance(by_status, dict)
            by_status[intent_row.status] = int(by_status.get(intent_row.status, 0)) + 1

        redemptions_by_venue: dict[str, dict[str, Any]] = {}
        for redemption in unresolved_redemptions:
            venue = redemptions_by_venue.setdefault(redemption.venue, {"count": 0, "by_status": {}})
            venue["count"] = int(venue["count"]) + 1
            by_status = venue["by_status"]
            assert isinstance(by_status, dict)
            status = redemption.status.value
            by_status[status] = int(by_status.get(status, 0)) + 1

        positions_by_status: dict[str, int] = {}
        exposure_by_venue: dict[str, Decimal] = {}
        for position in positions:
            positions_by_status[position.status] = positions_by_status.get(position.status, 0) + 1
            first_exposure = Decimal(str(position.polymarket_contracts)) * Decimal(
                str(position.polymarket_entry_price)
            )
            second_exposure = Decimal(str(position.predict_fun_contracts)) * Decimal(
                str(position.predict_fun_entry_price)
            )
            exposure_by_venue[position.market.venue_a_label] = exposure_by_venue.get(
                position.market.venue_a_label,
                Decimal(0),
            ) + first_exposure
            exposure_by_venue[position.market.venue_b_label] = exposure_by_venue.get(
                position.market.venue_b_label,
                Decimal(0),
            ) + second_exposure

        operator_resume_blocking_statuses = {
            "entry_pending",
            "unwind_pending",
            "partial_exit_pending",
            "manual_review",
        }
        blocking_position_count = sum(
            positions_by_status.get(status, 0) for status in operator_resume_blocking_statuses
        )
        operator_resume_blockers: list[str] = []
        if blocking_position_count:
            operator_resume_blockers.append(f"positions_require_manual_review:{blocking_position_count}")
        if unresolved_orders:
            operator_resume_blockers.append(f"unresolved_order_intents:{len(unresolved_orders)}")
        if unresolved_redemptions:
            operator_resume_blockers.append(f"unresolved_redemptions:{len(unresolved_redemptions)}")
        if reconciliation_failures:
            operator_resume_blockers.append(f"reconciliation_failures:{len(reconciliation_failures)}")

        risk_state_payload: dict[str, Any] | None = None
        if risk_state is not None:
            paused = bool(risk_state["paused"])
            risk_state_payload = {
                "daily_loss_usd": str(risk_state["daily_loss_usd"]),
                "consecutive_api_errors": int(risk_state["consecutive_api_errors"]),
                "paused": paused,
                "pause_reason": risk_state.get("pause_reason"),
                "operator_resume_gate": {
                    "applies": paused,
                    "eligible": paused and not operator_resume_blockers,
                    "blocking_reasons": operator_resume_blockers,
                },
            }

        return {
            "runtime_instance_id": self.runtime_instance_id,
            "enabled_routes": list(self.enabled_routes),
            "latest_balance_snapshots": latest_balances,
            "latest_runtime_balance_state": runtime_balance_state,
            "latest_shadow_preflight_evidence_by_route": shadow_preflight_evidence,
            "unresolved_order_intents": {
                "count": len(unresolved_orders),
                "by_venue": order_intents_by_venue,
            },
            "unresolved_redemptions": {
                "count": len(unresolved_redemptions),
                "by_venue": redemptions_by_venue,
            },
            "positions": {
                "count": len(positions),
                "by_status": positions_by_status,
                "estimated_entry_notional_by_venue_usd": {
                    venue: str(amount) for venue, amount in exposure_by_venue.items()
                },
            },
            "reconciliation_failures": reconciliation_failures,
            "risk_state": risk_state_payload,
            "metrics": {
                **metrics,
                "exposure_usd": str(metrics["exposure_usd"]),
            },
        }

    async def has_stale_mappings(self) -> bool:
        stale_row = MarketMappingRow.__table__
        verified_row = MarketMappingRow.__table__.alias("verified_market_mappings")
        verified_for_same_route = exists(
            select(1).where(
                verified_row.c.status == MappingStatus.VERIFIED.value,
                verified_row.c.canonical_market_id == stale_row.c.canonical_market_id,
                or_(
                    and_(
                        verified_row.c.left_venue == stale_row.c.left_venue,
                        verified_row.c.right_venue == stale_row.c.right_venue,
                    ),
                    and_(
                        verified_row.c.left_venue == stale_row.c.right_venue,
                        verified_row.c.right_venue == stale_row.c.left_venue,
                    ),
                ),
            )
        )
        statement = select(stale_row.c.mapping_id).where(
            stale_row.c.status == MappingStatus.STALE.value,
            ~verified_for_same_route,
        )
        if self.enabled_routes:
            allowed_pairs = sorted(_mapping_route_pairs(self.enabled_routes))
            if not allowed_pairs:
                return False
            statement = statement.where(tuple_(stale_row.c.left_venue, stale_row.c.right_venue).in_(allowed_pairs))
        statement = statement.limit(1)
        async with self.sessions() as session:
            mapping_id = await session.scalar(statement)
            return mapping_id is not None

    async def _allowed_position_keys(self, session: AsyncSession) -> set[str]:
        rows = (await session.scalars(select(PositionRow))).all()
        return {
            row.position_key
            for row in rows
            if route_key(row.first_venue, row.second_venue) in self.enabled_routes
        }


def _advisory_lock_id(value: str) -> int:
    digest = hashlib.sha256(value.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], byteorder="big", signed=True)


def _canonical_market_title(market: MarketSpec) -> str:
    return (market.symbol or market.target_label or "").strip()


def _is_synthetic_market_artifact(market_key: str | None, token_id: str | None) -> bool:
    return (
        str(market_key or "").startswith(_SYNTHETIC_MARKET_KEY_PREFIXES)
        and str(token_id or "") in _SYNTHETIC_TOKEN_IDS
    )


def _active_venues_for_routes(routes: Sequence[str]) -> tuple[str, ...]:
    venues: set[str] = set()
    for route in routes:
        if route == "polymarket_myriad":
            venues.update(("Polymarket", "Myriad"))
        elif route == "polymarket_predict":
            venues.update(("Polymarket", "Predict.fun"))
        elif route == "predict_myriad":
            venues.update(("Predict.fun", "Myriad"))
        elif route == "predict_sx":
            venues.update(("Predict.fun", "SX Bet"))
        elif route == "polymarket_sx":
            venues.update(("Polymarket", "SX Bet"))
        elif route == "sx_myriad":
            venues.update(("SX Bet", "Myriad"))
    return tuple(sorted(venues))


def _mapping_route_pairs(routes: Sequence[str]) -> set[tuple[str, str]]:
    pairs: set[tuple[str, str]] = set()
    for route in routes:
        if route == "polymarket_myriad":
            base = ("Polymarket", "Myriad")
        elif route == "polymarket_predict":
            base = ("Polymarket", "Predict.fun")
        elif route == "predict_myriad":
            base = ("Predict.fun", "Myriad")
        elif route == "predict_sx":
            base = ("Predict.fun", "SX Bet")
        elif route == "polymarket_sx":
            base = ("Polymarket", "SX Bet")
        elif route == "sx_myriad":
            base = ("SX Bet", "Myriad")
        else:
            continue
        pairs.add(base)
        pairs.add((base[1], base[0]))
    return pairs


def _position_route(position: OpenPosition) -> str:
    return route_key(position.market.venue_a_label, position.market.venue_b_label)


def _position_after_residual_exit(
    position: OpenPosition,
    *,
    venue: str,
    venue_order_id: str,
    requested_contracts: Decimal,
    closed_contracts: Decimal,
    average_exit_price: Decimal,
    residual_contracts: Decimal,
) -> OpenPosition:
    snapshot = ResidualExitSnapshot(
        venue_order_id=venue_order_id,
        requested_contracts=requested_contracts,
        closed_contracts=closed_contracts,
        exit_proceeds_usd=closed_contracts * average_exit_price,
        residual_contracts=residual_contracts,
    )
    return apply_residual_exit_snapshot(position, venue=venue, snapshot=snapshot)


def _normalized_rules_fingerprint(value: str) -> str:
    if len(value) <= 64:
        return value
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _missing_verified_metadata(value: str | None) -> bool:
    return not value or value.strip().lower() == "unknown"


def _known_verified_metadata(value: str) -> str | None:
    return None if _missing_verified_metadata(value) else value


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
        match_strategy=row.match_strategy,
        verified_at=row.verified_at,
        verified_by=row.verified_by,
        last_discovered_at=row.last_discovered_at,
        updated_at=row.updated_at,
    )


def _redemption_intent_from_row(row: RedemptionIntentRow) -> RedemptionIntent:
    return RedemptionIntent(
        redemption_id=row.redemption_id,
        position_key=row.position_key,
        venue=row.venue,
        market_id=row.market_id,
        condition_id=row.condition_id,
        collateral_token=row.collateral_token,
        expected_contracts=row.expected_contracts,
        status=RedemptionIntentStatus(row.status),
        tx_hash=row.tx_hash,
        last_error=row.last_error,
        created_at=row.created_at,
        updated_at=row.updated_at,
    )


def _route_name(left_venue: str, right_venue: str) -> str:
    aliases = {"Polymarket": "polymarket", "Predict.fun": "predict", "SX Bet": "sx", "Myriad": "myriad"}
    left = aliases.get(left_venue, left_venue.lower())
    right = aliases.get(right_venue, right_venue.lower())
    preferred = {
        frozenset(("polymarket", "predict")): "polymarket_predict",
        frozenset(("polymarket", "myriad")): "polymarket_myriad",
        frozenset(("predict", "myriad")): "predict_myriad",
        frozenset(("predict", "sx")): "predict_sx",
        frozenset(("polymarket", "sx")): "polymarket_sx",
        frozenset(("sx", "myriad")): "sx_myriad",
    }
    return preferred.get(frozenset((left, right)), f"{left}_{right}")


def _stable_id(*parts: str) -> str:
    return hashlib.sha256("\x1f".join(parts).encode("utf-8")).hexdigest()


def _market_candidate_cache_key(market: MarketSpec, identities: dict[str, str]) -> str:
    token_identity: list[str] = []
    for venue, market_id in sorted(identities.items()):
        yes_token, no_token = _venue_tokens(market, venue)
        token_identity.extend((venue, market_id, yes_token, no_token))
    return _stable_id("market-candidate", market.target_label, *token_identity)


def _market_candidate_persistence_signature(
    *,
    market: MarketSpec,
    cutoff: datetime,
    canonical_title: str,
    canonical_fingerprint: str,
    canonical_id: str,
    identities: dict[str, str],
) -> str:
    persisted_identity: list[str] = []
    for venue, market_id in sorted(identities.items()):
        yes_token, no_token = _venue_tokens(market, venue)
        persisted_identity.extend((venue, market_id, yes_token, no_token))
    return _stable_id(
        "market-candidate-signature",
        canonical_id,
        canonical_title,
        canonical_fingerprint,
        cutoff.isoformat(),
        market.category or "",
        market.resolution_source or "",
        market.outcome_semantics or "",
        market.timezone_name,
        market.mapping_strategy or "",
        *persisted_identity,
    )


def _market_identities(market: MarketSpec) -> dict[str, str]:
    result: dict[str, str] = {}
    first_market_id = _first_leg_market_id(market)
    if first_market_id:
        result[market.venue_a_label] = first_market_id
    if market.predict_fun_market_id and market.venue_b_label != "Myriad":
        result[market.venue_b_label] = market.predict_fun_market_id
    if market.myriad_market_id:
        result["Myriad"] = market.myriad_market_id
    return result


def _venue_tokens(market: MarketSpec, venue: str) -> tuple[str, str]:
    if venue == market.venue_a_label:
        token = market.polymarket_token_id
        return (token, "") if market.polymarket_side.value == "YES" else ("", token)
    if venue == market.venue_b_label and venue != "Myriad":
        token = market.predict_fun_token_id
        return (token, "") if market.predict_fun_side.value == "YES" else ("", token)
    if not market.myriad_market_id:
        return "", ""
    return f"{market.myriad_market_id}:YES", f"{market.myriad_market_id}:NO"


def _first_leg_market_id(market: MarketSpec) -> str | None:
    if market.venue_a_label == "Polymarket":
        return market.polymarket_market_id or market.condition_id
    return market.polymarket_market_id or market.predict_fun_market_id or market.condition_id
