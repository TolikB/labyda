import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from arbitrage_engine.main import (
    _SUBSCRIPTION_BUDGET_EVENT,
    _restored_subscription_budgets,
    _subscription_budget_recorder,
)

NOW = datetime(2026, 10, 7, 13, 0, tzinfo=UTC)


def test_a_recent_width_is_restored_at_three_quarters() -> None:
    record = (NOW - timedelta(hours=3), {"budgets": {"Polymarket": 4000, "Predict.fun": 3000}})

    assert _restored_subscription_budgets(record, NOW) == {"Polymarket": 3000, "Predict.fun": 2250}


def test_an_old_missing_or_malformed_record_restores_nothing() -> None:
    assert _restored_subscription_budgets(None, NOW) is None
    assert _restored_subscription_budgets((NOW - timedelta(hours=25), {"budgets": {"Polymarket": 4000}}), NOW) is None
    assert _restored_subscription_budgets((NOW, {"budgets": "4000"}), NOW) is None
    assert _restored_subscription_budgets((NOW, {"budgets": {"Polymarket": True, "Predict.fun": -5}}), NOW) is None


@pytest.mark.asyncio
async def test_each_change_is_written_as_an_audit_event() -> None:
    written: list[tuple[str, dict[str, Any]]] = []

    class _Repository:
        async def audit(self, event_type: str, payload: dict[str, Any]) -> None:
            written.append((event_type, payload))

    record = _subscription_budget_recorder(_Repository())  # type: ignore[arg-type]
    record({"Polymarket": 1500, "Predict.fun": 1500})
    await asyncio.sleep(0)

    assert written == [(_SUBSCRIPTION_BUDGET_EVENT, {"budgets": {"Polymarket": 1500, "Predict.fun": 1500}})]


@pytest.mark.asyncio
async def test_a_failed_write_is_logged_not_raised(caplog: pytest.LogCaptureFixture) -> None:
    class _Repository:
        async def audit(self, event_type: str, payload: dict[str, Any]) -> None:
            raise RuntimeError("database away")

    record = _subscription_budget_recorder(_Repository())  # type: ignore[arg-type]
    record({"Polymarket": 1500})
    await asyncio.sleep(0)
    await asyncio.sleep(0)

    assert any("market_data_subscription_budget_record_failed" in message for message in caplog.messages)
