import os
from collections.abc import AsyncIterator
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal

import pytest
from sqlalchemy import event, select, text

pytest.importorskip("sqlalchemy")

from arbitrage_engine.database import (
    Base,
    CanonicalMarketRow,
    MarketMappingRow,
    ProductionRepository,
    VenueInstrumentRow,
)
from arbitrage_engine.models import (
    BinarySide,
    FillRecord,
    MappingStatus,
    MarketSpec,
    OpenPosition,
    OrderIntent,
    OrderIntentStatus,
    ReconciliationResult,
    RedemptionIntent,
    RedemptionIntentStatus,
)
from arbitrage_engine.risk import GlobalRiskController
from arbitrage_engine.utils.ids import uuid7


@pytest.fixture
async def repository() -> AsyncIterator[ProductionRepository]:
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        if os.getenv("CI"):
            pytest.fail("DATABASE_URL must be configured in CI")
        pytest.skip("DATABASE_URL is required for PostgreSQL integration tests")
    repo = ProductionRepository(database_url)
    await repo.create_schema()
    table_names = ", ".join(f'"{table.name}"' for table in reversed(Base.metadata.sorted_tables))
    async with repo.engine.begin() as connection:
        await connection.execute(text(f"TRUNCATE TABLE {table_names} RESTART IDENTITY CASCADE"))
    yield repo
    await repo.close()


@pytest.mark.asyncio
async def test_order_intent_is_durable_before_status_transition(
    repository: ProductionRepository,
) -> None:
    client_order_id = str(uuid7())
    intent = OrderIntent(
        client_order_id=client_order_id,
        route="polymarket_myriad",
        market_key=f"integration:{client_order_id}",
        venue="Polymarket",
        token_id="integration-token",
        binary_side=BinarySide.YES,
        action="BUY",
        quantity=Decimal("1.000000000000000001"),
        limit_price=Decimal("0.123456789012345678"),
        created_at=datetime.now(UTC),
        updated_at=datetime.now(UTC),
    )

    await repository.create_order_intent(intent)
    await repository.update_order_intent(client_order_id, OrderIntentStatus.UNKNOWN)

    unresolved = await repository.unresolved_order_intents()
    row = next(item for item in unresolved if item.client_order_id == client_order_id)
    assert row.status == OrderIntentStatus.UNKNOWN.value
    assert row.quantity == intent.quantity
    assert row.limit_price == intent.limit_price


@pytest.mark.asyncio
async def test_only_one_repository_can_hold_trader_lock(
    repository: ProductionRepository,
) -> None:
    contender = ProductionRepository(repository.engine.url.render_as_string(hide_password=False))
    try:
        assert await repository.acquire_trader_lock()
        assert not await contender.acquire_trader_lock()
        await repository.release_trader_lock()
        assert await contender.acquire_trader_lock()
    finally:
        await contender.close()


@pytest.mark.asyncio
async def test_global_risk_pause_survives_repository_restart(repository: ProductionRepository) -> None:
    clean_state = {
        "loss_day": datetime.now(UTC).date().isoformat(),
        "daily_loss_usd": Decimal(0),
        "consecutive_api_errors": 0,
        "paused": False,
        "pause_reason": None,
    }
    await repository.save_risk_state(clean_state)
    controller = GlobalRiskController(100, 3, state_store=repository)
    await controller.initialize()
    await controller.pause("integration restart pause")

    restarted = ProductionRepository(repository.engine.url.render_as_string(hide_password=False))
    try:
        restored = GlobalRiskController(100, 3, state_store=restarted)
        await restored.initialize()
        assert restored.is_paused()
        assert restored.pause_reason == "integration restart pause"
    finally:
        await restarted.save_risk_state(clean_state)
        await restarted.close()


@pytest.mark.asyncio
async def test_runtime_instance_ids_partition_trader_lock_and_risk_state(repository: ProductionRepository) -> None:
    database_url = repository.engine.url.render_as_string(hide_password=False)
    clob = ProductionRepository(database_url, runtime_instance_id="clob_hft", enabled_routes=("polymarket_sx",))
    quote = ProductionRepository(
        database_url,
        runtime_instance_id="quote_arb",
        enabled_routes=("polymarket_predict", "polymarket_myriad"),
    )
    try:
        assert await clob.acquire_trader_lock()
        assert await quote.acquire_trader_lock()
        await clob.save_risk_state(
            {
                "loss_day": datetime.now(UTC).date().isoformat(),
                "daily_loss_usd": Decimal("1.25"),
                "consecutive_api_errors": 1,
                "paused": True,
                "pause_reason": "clob pause",
            }
        )
        await quote.save_risk_state(
            {
                "loss_day": datetime.now(UTC).date().isoformat(),
                "daily_loss_usd": Decimal("0"),
                "consecutive_api_errors": 0,
                "paused": False,
                "pause_reason": None,
            }
        )

        clob_state = await clob.load_risk_state()
        quote_state = await quote.load_risk_state()
        assert clob_state is not None
        assert quote_state is not None
        assert clob_state["pause_reason"] == "clob pause"
        assert quote_state["pause_reason"] is None
    finally:
        await clob.close()
        await quote.close()


@pytest.mark.asyncio
async def test_restart_recovery_and_duplicate_fill_are_idempotent(
    repository: ProductionRepository,
) -> None:
    client_order_id = str(uuid7())
    now = datetime.now(UTC)
    await repository.create_order_intent(
        OrderIntent(
            client_order_id=client_order_id,
            route="polymarket_myriad",
            market_key=f"restart:{client_order_id}",
            venue="Myriad",
            token_id="restart-token",
            binary_side=BinarySide.NO,
            action="BUY",
            quantity=Decimal("2.5"),
            limit_price=Decimal("0.4"),
            status=OrderIntentStatus.ACKNOWLEDGED,
            venue_order_id=f"venue-{client_order_id}",
            created_at=now,
            updated_at=now,
        )
    )
    fill = FillRecord(
        fill_id=f"fill-{client_order_id}",
        client_order_id=client_order_id,
        venue_order_id=f"venue-{client_order_id}",
        venue="Myriad",
        quantity=Decimal("1.25"),
        price=Decimal("0.4"),
        fee=Decimal("0"),
        occurred_at=now,
    )

    assert await repository.insert_fill(fill)
    assert not await repository.insert_fill(fill)

    restarted = ProductionRepository(repository.engine.url.render_as_string(hide_password=False))
    try:
        recovered = await restarted.unresolved_order_intents()
        row = next(item for item in recovered if item.client_order_id == client_order_id)
        assert row.status == OrderIntentStatus.ACKNOWLEDGED.value
        assert row.venue_order_id == fill.venue_order_id
    finally:
        await restarted.close()

    fills = await repository.fills_for_client_order_ids([client_order_id, "missing"])
    assert list(fills) == [client_order_id]
    assert fills[client_order_id][0]["fill_id"] == fill.fill_id


@pytest.mark.asyncio
async def test_redemption_intent_is_unique_and_restart_safe(repository: ProductionRepository) -> None:
    suffix = str(uuid7())
    intent = RedemptionIntent(
        redemption_id=suffix,
        position_key=f"position:{suffix}",
        venue="Polymarket",
        market_id=f"market:{suffix}",
        condition_id=f"0x{uuid7().hex}{uuid7().hex}",
        collateral_token="0x" + "1" * 40,
        expected_contracts=Decimal("1.000000000000000001"),
    )
    assert await repository.create_redemption_intent(intent)
    assert not await repository.create_redemption_intent(replace(intent, redemption_id=str(uuid7())))
    await repository.update_redemption_intent(
        intent.redemption_id,
        RedemptionIntentStatus.SUBMITTED,
        tx_hash="0x" + "2" * 64,
    )

    restarted = ProductionRepository(repository.engine.url.render_as_string(hide_password=False))
    try:
        restored = await restarted.get_redemption_intent(intent.position_key, intent.venue, intent.condition_id)
        assert restored is not None
        assert restored.status is RedemptionIntentStatus.SUBMITTED
        assert restored.expected_contracts == intent.expected_contracts
    finally:
        await restarted.close()


@pytest.mark.asyncio
async def test_only_latest_reconciliation_result_blocks_risk_resume(
    repository: ProductionRepository,
) -> None:
    venue = f"test-{uuid7().hex[:16]}"
    now = datetime.now(UTC)
    await repository.record_reconciliation(
        ReconciliationResult(
            venue=venue,
            started_at=now,
            completed_at=now,
            orders_checked=1,
            fills_recorded=0,
            drift_count=1,
            success=True,
        )
    )
    assert any(item.startswith(f"{venue}:") for item in await repository.latest_reconciliation_failures())

    await repository.record_reconciliation(
        ReconciliationResult(
            venue=venue,
            started_at=now,
            completed_at=now,
            orders_checked=1,
            fills_recorded=1,
            drift_count=0,
            success=True,
        )
    )
    assert not any(item.startswith(f"{venue}:") for item in await repository.latest_reconciliation_failures())


@pytest.mark.asyncio
async def test_metrics_snapshot_uses_latest_drift_for_enabled_route_venues(
    repository: ProductionRepository,
) -> None:
    database_url = repository.engine.url.render_as_string(hide_password=False)
    quote = ProductionRepository(
        database_url,
        runtime_instance_id="quote_arb",
        enabled_routes=("polymarket_predict", "polymarket_myriad"),
    )
    now = datetime.now(UTC)
    try:
        for venue in ("Polymarket", "Predict.fun", "Myriad"):
            await quote.record_reconciliation(
                ReconciliationResult(
                    venue=venue,
                    started_at=now,
                    completed_at=now,
                    orders_checked=1,
                    fills_recorded=0,
                    drift_count=7,
                    success=False,
                )
            )
            await quote.record_reconciliation(
                ReconciliationResult(
                    venue=venue,
                    started_at=now,
                    completed_at=now,
                    orders_checked=1,
                    fills_recorded=1,
                    drift_count=0,
                    success=True,
                )
            )

        snapshot = await quote.metrics_snapshot()

        assert snapshot["reconciliation_drift_total"] == 0
    finally:
        await quote.close()


@pytest.mark.asyncio
async def test_runtime_audit_snapshot_summarizes_balances_and_positions(repository: ProductionRepository) -> None:
    await repository.record_balances("Predict.fun", {"cash": Decimal("350")})
    await repository.record_balances("Myriad", {"USD1": Decimal("120")})
    await repository.record_runtime_balance_state(
        {
            "venues": {
                "Predict.fun": {
                    "balance_cache_usd": "350",
                    "optimistic_debits_usd": "25",
                    "capital_reservations_usd": "10",
                    "effective_balance_usd": "325",
                    "available_after_reservations_usd": "315",
                }
            }
        }
    )
    await repository.save_risk_state(
        {
            "loss_day": datetime.now(UTC).date().isoformat(),
            "daily_loss_usd": Decimal("0"),
            "consecutive_api_errors": 0,
            "paused": False,
            "pause_reason": None,
        }
    )
    await repository.save_position(
        "predict-position",
        OpenPosition(
            market=MarketSpec(
                symbol="BTC-USD",
                target_label=">$75,000",
                polymarket_token_id="poly-token",
                polymarket_side=BinarySide.YES,
                predict_fun_token_id="predict-token",
                predict_fun_side=BinarySide.NO,
                venue_b_label="Predict.fun",
                predict_fun_market_id="predict-market",
                mapping_status=MappingStatus.VERIFIED,
                verified_routes=frozenset({"polymarket_predict"}),
            ),
            polymarket_contracts=Decimal("10"),
            polymarket_entry_price=Decimal("0.40"),
            predict_fun_contracts=Decimal("10"),
            predict_fun_entry_price=Decimal("0.45"),
            opened_at=datetime.now(UTC),
            polymarket_order_id="poly-order",
            predict_fun_order_id="predict-order",
        ),
    )

    snapshot = await repository.runtime_audit_snapshot()

    assert Decimal(snapshot["latest_balance_snapshots"]["Predict.fun"]["cash"]["balance"]) == Decimal("350")
    assert snapshot["latest_runtime_balance_state"]["venues"]["Predict.fun"]["effective_balance_usd"] == "325"
    assert snapshot["positions"]["count"] == 1
    assert snapshot["positions"]["estimated_entry_notional_by_venue_usd"]["Predict.fun"] == "4.50"
    assert snapshot["risk_state"]["paused"] is False
    assert snapshot["risk_state"]["operator_resume_gate"] == {
        "applies": False,
        "eligible": False,
        "blocking_reasons": [],
    }


@pytest.mark.asyncio
async def test_runtime_audit_snapshot_marks_operator_resume_candidate_when_pause_is_clean(
    repository: ProductionRepository,
) -> None:
    await repository.save_risk_state(
        {
            "loss_day": datetime.now(UTC).date().isoformat(),
            "daily_loss_usd": Decimal("0"),
            "consecutive_api_errors": 0,
            "paused": True,
            "pause_reason": "continuous reconciliation detected drift",
        }
    )
    now = datetime.now(UTC)
    await repository.record_reconciliation(
        ReconciliationResult(
            venue="Polymarket",
            started_at=now,
            completed_at=now,
            orders_checked=1,
            fills_recorded=1,
            drift_count=0,
            success=True,
        )
    )

    snapshot = await repository.runtime_audit_snapshot()

    assert snapshot["risk_state"]["paused"] is True
    assert snapshot["risk_state"]["operator_resume_gate"] == {
        "applies": True,
        "eligible": True,
        "blocking_reasons": [],
    }


@pytest.mark.asyncio
async def test_runtime_audit_snapshot_scopes_to_enabled_routes_and_instance_state(
    repository: ProductionRepository,
) -> None:
    database_url = repository.engine.url.render_as_string(hide_password=False)
    clob = ProductionRepository(database_url, runtime_instance_id="clob_hft", enabled_routes=("polymarket_sx",))
    quote = ProductionRepository(
        database_url,
        runtime_instance_id="quote_arb",
        enabled_routes=("polymarket_predict", "polymarket_myriad"),
    )
    now = datetime.now(UTC)
    try:
        await repository.create_order_intent(
            OrderIntent(
                client_order_id=str(uuid7()),
                route="polymarket_sx",
                market_key="integration:clob",
                venue="Polymarket",
                token_id="poly-sx",
                binary_side=BinarySide.YES,
                action="BUY",
                quantity=Decimal("5"),
                limit_price=Decimal("0.40"),
                created_at=now,
                updated_at=now,
            )
        )
        await repository.create_order_intent(
            OrderIntent(
                client_order_id=str(uuid7()),
                route="polymarket_myriad",
                market_key="integration:quote",
                venue="Polymarket",
                token_id="poly-myriad",
                binary_side=BinarySide.YES,
                action="BUY",
                quantity=Decimal("7"),
                limit_price=Decimal("0.45"),
                created_at=now,
                updated_at=now,
            )
        )
        await repository.save_position(
            "clob-position",
            OpenPosition(
                market=MarketSpec(
                    symbol="SX Match",
                    target_label="YES",
                    polymarket_token_id="poly-sx",
                    polymarket_side=BinarySide.YES,
                    predict_fun_token_id="sx-token",
                    predict_fun_side=BinarySide.NO,
                    venue_b_label="SX Bet",
                    predict_fun_market_id="sx-market",
                    mapping_status=MappingStatus.VERIFIED,
                    verified_routes=frozenset({"polymarket_sx"}),
                ),
                polymarket_contracts=Decimal("10"),
                polymarket_entry_price=Decimal("0.40"),
                predict_fun_contracts=Decimal("10"),
                predict_fun_entry_price=Decimal("0.41"),
                opened_at=now,
                polymarket_order_id="poly-sx-order",
                predict_fun_order_id="sx-order",
            ),
        )
        await repository.save_position(
            "quote-position",
            OpenPosition(
                market=MarketSpec(
                    symbol="Myriad Match",
                    target_label="YES",
                    polymarket_token_id="poly-myriad",
                    polymarket_side=BinarySide.YES,
                    predict_fun_token_id="myriad-token",
                    predict_fun_side=BinarySide.NO,
                    venue_b_label="Myriad",
                    myriad_market_id="1335",
                    myriad_side=BinarySide.NO,
                    mapping_status=MappingStatus.VERIFIED,
                    verified_routes=frozenset({"polymarket_myriad"}),
                ),
                polymarket_contracts=Decimal("12"),
                polymarket_entry_price=Decimal("0.50"),
                predict_fun_contracts=Decimal("12"),
                predict_fun_entry_price=Decimal("0.52"),
                opened_at=now,
                polymarket_order_id="poly-myriad-order",
                predict_fun_order_id="myriad-order",
            ),
        )
        await clob.record_runtime_balance_state(
            {
                "venues": {
                    "Polymarket": {"effective_balance_usd": "90"},
                    "SX Bet": {"effective_balance_usd": "95"},
                }
            }
        )
        await quote.record_runtime_balance_state(
            {
                "venues": {
                    "Polymarket": {"effective_balance_usd": "80"},
                    "Myriad": {"effective_balance_usd": "70"},
                }
            }
        )
        await clob.save_risk_state(
            {
                "loss_day": now.date().isoformat(),
                "daily_loss_usd": Decimal("0"),
                "consecutive_api_errors": 0,
                "paused": True,
                "pause_reason": "clob pause",
            }
        )
        await quote.save_risk_state(
            {
                "loss_day": now.date().isoformat(),
                "daily_loss_usd": Decimal("0"),
                "consecutive_api_errors": 0,
                "paused": False,
                "pause_reason": None,
            }
        )

        clob_snapshot = await clob.runtime_audit_snapshot()
        quote_snapshot = await quote.runtime_audit_snapshot()

        assert clob_snapshot["runtime_instance_id"] == "clob_hft"
        assert clob_snapshot["enabled_routes"] == ["polymarket_sx"]
        assert clob_snapshot["unresolved_order_intents"]["count"] == 1
        assert clob_snapshot["positions"]["count"] == 1
        assert clob_snapshot["risk_state"]["pause_reason"] == "clob pause"
        assert clob_snapshot["latest_runtime_balance_state"]["runtime_instance_id"] == "clob_hft"

        assert quote_snapshot["runtime_instance_id"] == "quote_arb"
        assert set(quote_snapshot["enabled_routes"]) == {"polymarket_predict", "polymarket_myriad"}
        assert quote_snapshot["unresolved_order_intents"]["count"] == 1
        assert quote_snapshot["positions"]["count"] == 1
        assert quote_snapshot["risk_state"]["paused"] is False
        assert quote_snapshot["latest_runtime_balance_state"]["runtime_instance_id"] == "quote_arb"
    finally:
        await clob.close()
        await quote.close()


@pytest.mark.asyncio
async def test_has_stale_mappings_ignores_stale_rows_with_verified_alternative_for_same_route(
    repository: ProductionRepository,
) -> None:
    database_url = repository.engine.url.render_as_string(hide_password=False)
    quote = ProductionRepository(database_url, enabled_routes=("polymarket_predict",))
    now = datetime.now(UTC)
    try:
        async with quote.transaction() as session:
            session.add(
                CanonicalMarketRow(
                    canonical_id="predict-canonical",
                    title="Predict duplicate mapping",
                    category="Sports",
                    resolution_source="test",
                    cutoff_at=now,
                    timezone_name="UTC",
                    outcome_semantics="binary",
                    rules_fingerprint="predict-duplicate",
                )
            )
            session.add(
                MarketMappingRow(
                    mapping_id="predict-stale",
                    canonical_market_id="predict-canonical",
                    left_venue="Polymarket",
                    left_market_id="poly-stale",
                    right_venue="Predict.fun",
                    right_market_id="predict-stale",
                    status=MappingStatus.STALE.value,
                    rules_fingerprint="predict-stale",
                    created_at=now,
                    updated_at=now,
                )
            )
            session.add(
                MarketMappingRow(
                    mapping_id="predict-verified",
                    canonical_market_id="predict-canonical",
                    left_venue="Polymarket",
                    left_market_id="poly-verified",
                    right_venue="Predict.fun",
                    right_market_id="predict-verified",
                    status=MappingStatus.VERIFIED.value,
                    rules_fingerprint="predict-verified",
                    verified_at=now,
                    verified_by="test",
                    created_at=now,
                    updated_at=now,
                )
            )

        assert not await quote.has_stale_mappings()
    finally:
        await quote.close()


@pytest.mark.asyncio
async def test_has_stale_mappings_flags_stale_rows_without_verified_alternative(
    repository: ProductionRepository,
) -> None:
    database_url = repository.engine.url.render_as_string(hide_password=False)
    quote = ProductionRepository(database_url, enabled_routes=("polymarket_predict",))
    now = datetime.now(UTC)
    try:
        async with quote.transaction() as session:
            session.add(
                CanonicalMarketRow(
                    canonical_id="predict-canonical-blocking",
                    title="Predict blocking stale mapping",
                    category="Sports",
                    resolution_source="test",
                    cutoff_at=now,
                    timezone_name="UTC",
                    outcome_semantics="binary",
                    rules_fingerprint="predict-blocking",
                )
            )
            session.add(
                MarketMappingRow(
                    mapping_id="predict-stale-blocking",
                    canonical_market_id="predict-canonical-blocking",
                    left_venue="Polymarket",
                    left_market_id="poly-only-stale",
                    right_venue="Predict.fun",
                    right_market_id="predict-only-stale",
                    status=MappingStatus.STALE.value,
                    rules_fingerprint="predict-only-stale",
                    created_at=now,
                    updated_at=now,
                )
            )

        assert await quote.has_stale_mappings()
    finally:
        await quote.close()


@pytest.mark.asyncio
async def test_upsert_market_candidates_persists_both_myriad_binary_tokens(repository: ProductionRepository) -> None:
    market = MarketSpec(
        symbol="Will France win the World Cup?",
        target_label="France",
        polymarket_token_id="poly-token",
        polymarket_side=BinarySide.YES,
        predict_fun_token_id="0xsx:NO",
        predict_fun_side=BinarySide.NO,
        venue_b_label="SX Bet",
        predict_fun_market_id="0xsxmarket",
        myriad_market_id="1335",
        myriad_side=BinarySide.NO,
        rules_fingerprint=f"fp-{uuid7()}",
        resolution_source="SX/Polymarket aligned sports market",
        outcome_semantics="YES if France wins the tournament",
        category="sports",
        expires_at=datetime(2026, 7, 22, tzinfo=UTC),
        cutoff_at=datetime(2026, 7, 22, tzinfo=UTC),
    )

    await repository.upsert_market_candidates([market])
    async with repository.sessions() as session:
        row = await session.scalar(
            select(VenueInstrumentRow).where(
                VenueInstrumentRow.venue == "Myriad",
                VenueInstrumentRow.market_id == "1335",
            )
        )

    assert row is not None
    assert row.yes_token_id == "1335:YES"
    assert row.no_token_id == "1335:NO"


@pytest.mark.asyncio
async def test_upsert_market_candidates_uses_bounded_query_count(repository: ProductionRepository) -> None:
    expires_at = datetime(2026, 7, 22, tzinfo=UTC)
    markets = [
        MarketSpec(
            symbol=f"Predict candidate {index}",
            target_label="YES",
            polymarket_token_id=f"poly-token-{index}",
            polymarket_side=BinarySide.YES,
            condition_id=f"condition-{index}",
            predict_fun_token_id=f"predict-token-{index}",
            predict_fun_side=BinarySide.NO,
            predict_fun_market_id=f"predict-market-{index}",
            mapping_strategy="exact_id",
            resolution_source="Shared exact-id market",
            outcome_semantics="YES if the named event occurs",
            category="crypto",
            expires_at=expires_at,
            cutoff_at=expires_at,
        )
        for index in range(25)
    ]
    statements: list[str] = []

    def record_statement(
        _connection: object,
        _cursor: object,
        statement: str,
        _parameters: object,
        _context: object,
        _executemany: bool,
    ) -> None:
        statements.append(statement)

    event.listen(repository.engine.sync_engine, "before_cursor_execute", record_statement)
    try:
        await repository.upsert_market_candidates(markets)
    finally:
        event.remove(repository.engine.sync_engine, "before_cursor_execute", record_statement)

    assert len(statements) <= 8
    async with repository.sessions() as session:
        canonicals = list(await session.scalars(select(CanonicalMarketRow)))
        instruments = list(await session.scalars(select(VenueInstrumentRow)))
        mappings = list(await session.scalars(select(MarketMappingRow)))
    assert len(canonicals) == 25
    assert len(instruments) == 50
    assert len(mappings) == 25


@pytest.mark.asyncio
async def test_upsert_market_candidates_chunks_large_discovery_snapshots(repository: ProductionRepository) -> None:
    expires_at = datetime(2026, 7, 22, tzinfo=UTC)
    markets = [
        MarketSpec(
            symbol=f"Chunked candidate {index}",
            target_label="YES",
            polymarket_token_id=f"chunked-poly-token-{index}",
            polymarket_side=BinarySide.YES,
            condition_id=f"chunked-condition-{index}",
            predict_fun_token_id=f"chunked-predict-token-{index}",
            predict_fun_side=BinarySide.NO,
            predict_fun_market_id=f"chunked-predict-market-{index}",
            mapping_strategy="exact_id",
            resolution_source="Shared exact-id market",
            outcome_semantics="YES if the named event occurs",
            category="crypto",
            expires_at=expires_at,
            cutoff_at=expires_at,
        )
        for index in range(129)
    ]
    statements: list[str] = []

    def record_statement(
        _connection: object,
        _cursor: object,
        statement: str,
        _parameters: object,
        _context: object,
        _executemany: bool,
    ) -> None:
        statements.append(statement)

    event.listen(repository.engine.sync_engine, "before_cursor_execute", record_statement)
    try:
        await repository.upsert_market_candidates(markets)
    finally:
        event.remove(repository.engine.sync_engine, "before_cursor_execute", record_statement)

    canonical_reads = [
        statement
        for statement in statements
        if statement.lstrip().upper().startswith("SELECT") and "FROM canonical_markets" in statement
    ]
    assert len(canonical_reads) == 2

    mappings = await repository.list_mappings()
    snapshot = await repository.mapping_review_snapshot(mappings)
    assert len(snapshot["canonical_markets"]) == 129
    assert len(snapshot["venue_instruments"]) == 258


@pytest.mark.asyncio
async def test_upsert_market_candidates_does_not_rewrite_unchanged_rows(repository: ProductionRepository) -> None:
    expires_at = datetime(2026, 7, 22, tzinfo=UTC)
    market = MarketSpec(
        symbol="Stable Predict candidate",
        target_label="YES",
        polymarket_token_id="stable-poly-token",
        polymarket_side=BinarySide.YES,
        condition_id="stable-condition",
        predict_fun_token_id="stable-predict-token",
        predict_fun_side=BinarySide.NO,
        predict_fun_market_id="stable-predict-market",
        mapping_strategy="exact_id",
        resolution_source="Shared exact-id market",
        outcome_semantics="YES if the named event occurs",
        category="crypto",
        expires_at=expires_at,
        cutoff_at=expires_at,
    )
    await repository.upsert_market_candidates([market])
    statements: list[str] = []

    def record_statement(
        _connection: object,
        _cursor: object,
        statement: str,
        _parameters: object,
        _context: object,
        _executemany: bool,
    ) -> None:
        statements.append(statement)

    event.listen(repository.engine.sync_engine, "before_cursor_execute", record_statement)
    try:
        await repository.upsert_market_candidates([market])
    finally:
        event.remove(repository.engine.sync_engine, "before_cursor_execute", record_statement)

    assert statements == []


@pytest.mark.asyncio
async def test_upsert_market_candidates_keeps_two_sides_of_same_sx_market_pair_stable(
    repository: ProductionRepository,
) -> None:
    expires_at = datetime(2026, 7, 22, tzinfo=UTC)
    france = MarketSpec(
        symbol="Will France win the World Cup?",
        target_label="France",
        polymarket_token_id="poly-france",
        polymarket_side=BinarySide.YES,
        condition_id="poly-world-cup",
        predict_fun_token_id="sx-france:NO",
        predict_fun_side=BinarySide.NO,
        venue_b_label="SX Bet",
        predict_fun_market_id="sx-world-cup",
        rules_fingerprint=f"fp-{uuid7()}-france",
        mapping_strategy="exact_id",
        resolution_source="Official World Cup result",
        outcome_semantics="Outcome one=France; outcome two=The Field; type=274",
        category="sports",
        expires_at=expires_at,
        cutoff_at=expires_at,
    )
    field = replace(
        france,
        target_label="The Field",
        polymarket_token_id="poly-field",
        polymarket_side=BinarySide.NO,
        predict_fun_token_id="sx-france:YES",
        predict_fun_side=BinarySide.YES,
        rules_fingerprint=f"fp-{uuid7()}-field",
    )

    await repository.upsert_market_candidates([france, field])

    mappings = await repository.list_mappings()
    sx_rows = [
        mapping
        for mapping in mappings
        if mapping.left_venue == "Polymarket" and mapping.right_venue == "SX Bet"
    ]
    assert len(sx_rows) == 1
    assert sx_rows[0].status is MappingStatus.CANDIDATE
    assert sx_rows[0].match_strategy == "exact_id"
    async with repository.sessions() as session:
        instruments = list(
            await session.scalars(
                select(VenueInstrumentRow).where(
                    VenueInstrumentRow.market_id.in_(("poly-world-cup", "sx-world-cup"))
                )
            )
        )
    by_venue = {instrument.venue: instrument for instrument in instruments}
    assert by_venue["Polymarket"].yes_token_id == "poly-france"
    assert by_venue["Polymarket"].no_token_id == "poly-field"
    assert by_venue["SX Bet"].yes_token_id == "sx-france:YES"
    assert by_venue["SX Bet"].no_token_id == "sx-france:NO"


@pytest.mark.asyncio
async def test_verified_mapping_only_becomes_stale_when_match_provenance_changes(
    repository: ProductionRepository,
) -> None:
    expires_at = datetime(2026, 7, 22, tzinfo=UTC)
    market = MarketSpec(
        symbol="Will France win the World Cup?",
        target_label="France",
        polymarket_token_id="poly-france",
        polymarket_side=BinarySide.YES,
        condition_id="poly-world-cup",
        predict_fun_token_id="sx-france:NO",
        predict_fun_side=BinarySide.NO,
        venue_b_label="SX Bet",
        predict_fun_market_id="sx-world-cup-provenance",
        mapping_strategy="exact_title",
        resolution_source="Official World Cup result",
        outcome_semantics="Outcome one=France; outcome two=The Field; type=274",
        category="sports",
        expires_at=expires_at,
        cutoff_at=expires_at,
    )
    await repository.upsert_market_candidates([market])
    mapping = (await repository.list_mappings())[0]
    await repository.set_mapping_status(mapping.mapping_id, MappingStatus.VERIFIED, operator="test")

    await repository.upsert_market_candidates([market])
    same_strategy = (await repository.list_mappings())[0]
    assert same_strategy.status is MappingStatus.VERIFIED

    await repository.upsert_market_candidates([replace(market, mapping_strategy="semantic")])
    changed_strategy = (await repository.list_mappings())[0]
    assert changed_strategy.status is MappingStatus.STALE
    assert changed_strategy.match_strategy == "semantic"


@pytest.mark.asyncio
async def test_verified_mapping_repoints_to_new_canonical_metadata_before_reapproval(
    repository: ProductionRepository,
) -> None:
    expires_at = datetime(2026, 7, 22, tzinfo=UTC)
    market = MarketSpec(
        symbol="Will Spain win?",
        target_label="Spain",
        polymarket_token_id="poly-spain",
        polymarket_side=BinarySide.YES,
        condition_id="poly-spain-condition",
        predict_fun_token_id="predict-spain-no",
        predict_fun_side=BinarySide.NO,
        predict_fun_market_id="predict-spain",
        mapping_strategy="exact_id",
        resolution_source="unknown",
        outcome_semantics="YES if Spain wins",
        category="sports",
        expires_at=expires_at,
        cutoff_at=expires_at,
    )
    await repository.upsert_market_candidates([market])
    mapping = (await repository.list_mappings())[0]
    old_canonical_id = mapping.canonical_market_id
    await repository.set_mapping_status(mapping.mapping_id, MappingStatus.VERIFIED, operator="test")

    enriched = replace(market, resolution_source="resolver:0xresolver;oracle_question:0xoracle")
    await repository.upsert_market_candidates([enriched])
    stale = (await repository.list_mappings())[0]

    assert stale.status is MappingStatus.STALE
    assert stale.canonical_market_id != old_canonical_id
    await repository.set_mapping_status(stale.mapping_id, MappingStatus.VERIFIED, operator="test")
    applied = await repository.apply_verified_mappings([replace(enriched, resolution_source=None)])
    assert applied[0].resolution_source == "resolver:0xresolver;oracle_question:0xoracle"


@pytest.mark.asyncio
async def test_upsert_market_candidates_normalizes_long_rules_fingerprint(repository: ProductionRepository) -> None:
    long_fingerprint = "sx:" + "a" * 100
    market = MarketSpec(
        symbol="France",
        target_label="France",
        polymarket_token_id="poly-token",
        polymarket_side=BinarySide.YES,
        predict_fun_token_id="sx-token",
        predict_fun_side=BinarySide.NO,
        venue_b_label="SX Bet",
        predict_fun_market_id="sx-market",
        rules_fingerprint=long_fingerprint,
        resolution_source="Official Outrights - World Cup result",
        outcome_semantics="Outcome one=France; outcome two=The Field; type=274",
        category="sports",
        expires_at=datetime(2026, 7, 22, tzinfo=UTC),
        cutoff_at=datetime(2026, 7, 22, tzinfo=UTC),
    )

    await repository.upsert_market_candidates([market])
    async with repository.sessions() as session:
        canonical = await session.scalar(
            select(VenueInstrumentRow.rules_fingerprint).where(
                VenueInstrumentRow.venue == "SX Bet",
                VenueInstrumentRow.market_id == "sx-market",
            )
        )

    assert canonical is not None
    assert len(canonical) == 64


@pytest.mark.asyncio
async def test_upsert_market_candidates_preserves_sx_identity_for_sx_myriad_shape(
    repository: ProductionRepository,
) -> None:
    market = MarketSpec(
        symbol="SX-Myriad",
        target_label="YES",
        polymarket_token_id="sx-token",
        polymarket_side=BinarySide.YES,
        predict_fun_token_id="1335:YES",
        predict_fun_side=BinarySide.YES,
        predict_fun_market_id="0xsxmarket",
        myriad_market_id="1335",
        myriad_side=BinarySide.NO,
        venue_a_label="SX Bet",
        venue_b_label="Myriad",
        rules_fingerprint=f"fp-{uuid7()}",
        resolution_source="SX/Myriad aligned sports market",
        outcome_semantics="YES if the quoted team wins",
        category="sports",
        expires_at=datetime(2026, 7, 22, tzinfo=UTC),
        cutoff_at=datetime(2026, 7, 22, tzinfo=UTC),
    )

    await repository.upsert_market_candidates([market])
    async with repository.sessions() as session:
        sx_row = await session.scalar(
            select(VenueInstrumentRow).where(
                VenueInstrumentRow.venue == "SX Bet",
                VenueInstrumentRow.market_id == "0xsxmarket",
            )
        )
        myriad_row = await session.scalar(
            select(VenueInstrumentRow).where(
                VenueInstrumentRow.venue == "Myriad",
                VenueInstrumentRow.market_id == "1335",
            )
        )

    assert sx_row is not None
    assert sx_row.yes_token_id == "sx-token"
    assert sx_row.no_token_id == ""
    assert myriad_row is not None
    assert myriad_row.yes_token_id == "1335:YES"
    assert myriad_row.no_token_id == "1335:NO"


@pytest.mark.asyncio
async def test_upsert_market_candidates_preserves_predict_identity_for_predict_myriad_shape(
    repository: ProductionRepository,
) -> None:
    market = MarketSpec(
        symbol="Predict-Myriad",
        target_label="YES",
        polymarket_token_id="predict-token",
        polymarket_side=BinarySide.NO,
        predict_fun_token_id="1335:YES",
        predict_fun_side=BinarySide.YES,
        predict_fun_market_id="predict-market",
        myriad_market_id="1335",
        myriad_side=BinarySide.NO,
        venue_a_label="Predict.fun",
        venue_b_label="Myriad",
        rules_fingerprint=f"fp-{uuid7()}",
        resolution_source="Predict/Myriad aligned market",
        outcome_semantics="YES if the quoted event resolves true",
        category="politics",
        expires_at=datetime(2026, 7, 22, tzinfo=UTC),
        cutoff_at=datetime(2026, 7, 22, tzinfo=UTC),
    )

    await repository.upsert_market_candidates([market])
    async with repository.sessions() as session:
        predict_row = await session.scalar(
            select(VenueInstrumentRow).where(
                VenueInstrumentRow.venue == "Predict.fun",
                VenueInstrumentRow.market_id == "predict-market",
            )
        )
        myriad_row = await session.scalar(
            select(VenueInstrumentRow).where(
                VenueInstrumentRow.venue == "Myriad",
                VenueInstrumentRow.market_id == "1335",
            )
        )

    assert predict_row is not None
    assert predict_row.yes_token_id == ""
    assert predict_row.no_token_id == "predict-token"
    assert myriad_row is not None
    assert myriad_row.yes_token_id == "1335:YES"
    assert myriad_row.no_token_id == "1335:NO"


@pytest.mark.asyncio
async def test_client_order_id_lookup_supports_sx_fillhash_suffix(repository: ProductionRepository) -> None:
    intent = OrderIntent(
        client_order_id="intent-1",
        route="polymarket_sx",
        market_key="market",
        venue="SX Bet",
        token_id="0xmarket:NO",
        binary_side=BinarySide.NO,
        action="BUY",
        quantity=Decimal("10"),
        limit_price=Decimal("0.45"),
        venue_order_id="sx:BUY:NO:0xmarket:1E+1:0.45:0xfill",
    )
    await repository.create_order_intent(intent)

    assert await repository.client_order_id_for_venue_order("SX Bet", "0xfill") == "intent-1"
