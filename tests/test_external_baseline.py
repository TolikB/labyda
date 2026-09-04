from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
from sqlalchemy import delete, update

from arbitrage_engine.database import ExternalAccountBaselineItemRow, ProductionRepository
from arbitrage_engine.external_baseline import (
    account_fingerprint,
    canonical_external_baseline_payload,
    external_baseline_manifest_sha256,
)
from arbitrage_engine.models import (
    BinarySide,
    ExternalAccountBaseline,
    OrderIntent,
    OrderIntentStatus,
    ReconciliationResult,
)


def _baseline(runtime_instance_id: str = "quote_arb") -> ExternalAccountBaseline:
    fingerprint = account_fingerprint("Polymarket", "0x" + "1" * 40)
    payload = canonical_external_baseline_payload(
        runtime_instance_id=runtime_instance_id,
        venue="Polymarket",
        account_fingerprint_value=fingerprint,
        positions={"personal-token": Decimal("12.5")},
        fill_refs=("personal-fill",),
    )
    return ExternalAccountBaseline(
        manifest_sha256=external_baseline_manifest_sha256(payload),
        runtime_instance_id=runtime_instance_id,
        venue="Polymarket",
        account_fingerprint=fingerprint,
        positions=(("personal-token", Decimal("12.5")),),
        fill_refs=("personal-fill",),
        captured_at=datetime.now(UTC),
        operator="test-operator",
    )


@pytest.mark.asyncio
async def test_external_baseline_is_runtime_scoped_and_digest_verified(tmp_path: Path) -> None:
    database_url = f"sqlite+aiosqlite:///{(tmp_path / 'baseline.sqlite3').as_posix()}"
    quote = ProductionRepository(database_url, runtime_instance_id="quote_arb")
    clob = ProductionRepository(database_url, runtime_instance_id="clob_hft")
    try:
        await quote.create_schema()
        baseline = _baseline()

        assert await quote.activate_external_account_baseline(baseline)
        assert not await quote.activate_external_account_baseline(baseline)
        assert await quote.active_external_account_baseline("Polymarket") == baseline
        assert await clob.active_external_account_baseline("Polymarket") is None

        invalid = ExternalAccountBaseline(
            **{**baseline.__dict__, "manifest_sha256": "0" * 64}
        )
        with pytest.raises(ValueError, match="digest"):
            await quote.activate_external_account_baseline(invalid)
    finally:
        await clob.close()
        await quote.close()


@pytest.mark.asyncio
async def test_reconciliation_evidence_is_bound_to_active_baseline_and_latest_run(tmp_path: Path) -> None:
    database_url = f"sqlite+aiosqlite:///{(tmp_path / 'evidence.sqlite3').as_posix()}"
    repository = ProductionRepository(database_url, runtime_instance_id="quote_arb")
    try:
        await repository.create_schema()
        baseline = _baseline()
        await repository.activate_external_account_baseline(baseline)
        now = datetime.now(UTC)
        await repository.record_reconciliation(
            ReconciliationResult(
                venue="Polymarket",
                started_at=now,
                completed_at=now,
                orders_checked=0,
                fills_recorded=0,
                drift_count=0,
                success=True,
                full=True,
                account_fingerprint=baseline.account_fingerprint,
                external_baseline_manifest_sha256=baseline.manifest_sha256,
            )
        )
        assert await repository.latest_reconciliation_failures() == []
        evidence = await repository.external_account_baseline_evidence()
        assert len(evidence) == 1
        assert evidence[0]["manifest_sha256"] == baseline.manifest_sha256
        assert evidence[0]["account_fingerprint_sha256"] == baseline.account_fingerprint
        latest_full = evidence[0]["latest_full_reconciliation"]
        assert isinstance(latest_full, dict)
        assert latest_full["fresh"]
        assert latest_full["fingerprint_matches_active_baseline"]
        assert latest_full["manifest_matches_active_baseline"]

        await repository.record_reconciliation(
            ReconciliationResult(
                venue="Polymarket",
                started_at=now,
                completed_at=now,
                orders_checked=1,
                fills_recorded=0,
                drift_count=1,
                success=True,
                full=False,
                account_fingerprint=baseline.account_fingerprint,
                external_baseline_manifest_sha256=baseline.manifest_sha256,
            )
        )
        failures = await repository.latest_reconciliation_failures()
        assert any("latest reconciliation drift" in item for item in failures)

        with pytest.raises(ValueError, match="changed"):
            await repository.revoke_external_account_baseline(
                "Polymarket",
                operator="test-operator",
                manifest_sha256="f" * 64,
            )
        assert await repository.revoke_external_account_baseline(
            "Polymarket",
            operator="test-operator",
            manifest_sha256=baseline.manifest_sha256,
        )
        failures = await repository.latest_reconciliation_failures()
        assert any("predates external baseline state" in item for item in failures)
    finally:
        await repository.close()


@pytest.mark.parametrize("mutation", ["tamper", "delete"])
@pytest.mark.asyncio
async def test_reconciliation_readiness_detects_baseline_item_corruption(
    tmp_path: Path,
    mutation: str,
) -> None:
    database_url = f"sqlite+aiosqlite:///{(tmp_path / f'corrupt-{mutation}.sqlite3').as_posix()}"
    repository = ProductionRepository(database_url, runtime_instance_id="quote_arb")
    try:
        await repository.create_schema()
        baseline = _baseline()
        await repository.activate_external_account_baseline(baseline)
        now = datetime.now(UTC)
        await repository.record_reconciliation(
            ReconciliationResult(
                venue="Polymarket",
                started_at=now,
                completed_at=now,
                orders_checked=0,
                fills_recorded=0,
                drift_count=0,
                success=True,
                full=True,
                account_fingerprint=baseline.account_fingerprint,
                external_baseline_manifest_sha256=baseline.manifest_sha256,
            )
        )
        assert await repository.latest_reconciliation_failures() == []

        async with repository.transaction() as session:
            item_filter = (
                ExternalAccountBaselineItemRow.manifest_sha256 == baseline.manifest_sha256,
                ExternalAccountBaselineItemRow.item_type == "position",
            )
            statement = (
                update(ExternalAccountBaselineItemRow)
                .where(*item_filter)
                .values(quantity=Decimal("99"))
                if mutation == "tamper"
                else delete(ExternalAccountBaselineItemRow).where(*item_filter)
            )
            await session.execute(statement)

        failures = await repository.latest_reconciliation_failures()
        assert failures == ["Polymarket: active external baseline failed its integrity check"]
    finally:
        await repository.close()


@pytest.mark.asyncio
async def test_discovery_only_venue_is_nonblocking_until_it_has_managed_state(tmp_path: Path) -> None:
    database_url = f"sqlite+aiosqlite:///{(tmp_path / 'scope.sqlite3').as_posix()}"
    repository = ProductionRepository(
        database_url,
        runtime_instance_id="quote_arb",
        enabled_routes=("polymarket_predict", "polymarket_sx"),
    )
    try:
        await repository.create_schema()
        now = datetime.now(UTC)
        for venue in ("Polymarket", "Predict.fun"):
            await repository.record_reconciliation(
                ReconciliationResult(
                    venue=venue,
                    started_at=now,
                    completed_at=now,
                    orders_checked=0,
                    fills_recorded=0,
                    drift_count=0,
                    success=True,
                    full=True,
                )
            )
        await repository.record_reconciliation(
            ReconciliationResult(
                venue="SX Bet",
                started_at=now,
                completed_at=now,
                orders_checked=1,
                fills_recorded=0,
                drift_count=1,
                success=False,
                error="discovery-only venue unavailable",
                full=True,
            )
        )

        assert await repository.configure_managed_reconciliation_venues(("polymarket_predict",)) == (
            "Polymarket",
            "Predict.fun",
        )
        assert await repository.latest_reconciliation_failures() == []

        await repository.create_order_intent(
            OrderIntent(
                client_order_id="sx-managed-state",
                route="polymarket_sx",
                market_key="managed-sx-market",
                venue="SX Bet",
                token_id="sx-token",
                binary_side=BinarySide.NO,
                action="BUY",
                quantity=Decimal("1"),
                limit_price=Decimal("0.4"),
                status=OrderIntentStatus.SUBMITTING,
                created_at=now,
                updated_at=now,
            )
        )
        managed = await repository.configure_managed_reconciliation_venues(("polymarket_predict",))

        assert "SX Bet" in managed
        assert any("SX Bet" in item for item in await repository.latest_reconciliation_failures())
    finally:
        await repository.close()


@pytest.mark.asyncio
async def test_stale_full_reconciliation_evidence_blocks_resume(tmp_path: Path) -> None:
    database_url = f"sqlite+aiosqlite:///{(tmp_path / 'stale.sqlite3').as_posix()}"
    repository = ProductionRepository(database_url, runtime_instance_id="quote_arb")
    try:
        await repository.create_schema()
        old = datetime.now(UTC) - timedelta(minutes=6)
        await repository.record_reconciliation(
            ReconciliationResult(
                venue="Polymarket",
                started_at=old,
                completed_at=old,
                orders_checked=0,
                fills_recorded=0,
                drift_count=0,
                success=True,
                full=True,
            )
        )

        assert any(
            "full reconciliation evidence is stale" in item
            for item in await repository.latest_reconciliation_failures()
        )
    finally:
        await repository.close()
