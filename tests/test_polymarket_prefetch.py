"""Bulk audits prefetch Polymarket market constraints instead of pacing through them one by one."""

from __future__ import annotations

import asyncio
from decimal import Decimal
from typing import Any

import pytest

from arbitrage_engine.config import PolymarketConfig
from arbitrage_engine.connectors import polymarket as polymarket_module
from arbitrage_engine.connectors.polymarket import PolymarketClobClient


def _info(condition: str, *, fee: dict[str, Any] | None = None) -> dict[str, Any]:
    return {
        "t": [{"t": f"{condition}-yes"}, {"t": f"{condition}-no"}],
        "mts": 0.01,
        "mos": 5,
        "nr": False,
        "fd": {"r": 0.05, "e": 1} if fee is None else fee,
    }


class _Response:
    def __init__(self, status: int, payload: Any) -> None:
        self.status = status
        self._payload = payload

    async def __aenter__(self) -> _Response:
        return self

    async def __aexit__(self, *args: object) -> bool:
        return False

    async def json(self) -> Any:
        return self._payload


class _Session:
    def __init__(self, answers: dict[str, tuple[int, Any]]) -> None:
        self.answers = answers
        self.requested: list[str] = []
        self.in_flight = 0
        self.most_in_flight = 0

    def get(self, url: str, *, timeout: float) -> Any:
        del timeout
        condition = url.rsplit("/", 1)[-1]
        self.requested.append(condition)
        session = self

        class _Tracked(_Response):
            async def __aenter__(self) -> _Response:
                session.in_flight += 1
                session.most_in_flight = max(session.most_in_flight, session.in_flight)
                await asyncio.sleep(0.01)
                return self

            async def __aexit__(self, *args: object) -> bool:
                session.in_flight -= 1
                return False

        status, payload = session.answers.get(condition, (404, {}))
        return _Tracked(status, payload)


def _client(session: _Session) -> PolymarketClobClient:
    client = PolymarketClobClient(PolymarketConfig(None, "https://clob.polymarket.com", 137, 0, None))
    client._get_rest_session = lambda: session  # type: ignore[method-assign]  # noqa: SLF001
    return client


@pytest.mark.asyncio
async def test_a_window_is_fetched_a_few_at_a_time_and_served_from_the_cache() -> None:
    conditions = [f"c{index}" for index in range(20)]
    session = _Session({condition: (200, _info(condition)) for condition in conditions})
    client = _client(session)

    fetched = await client.prefetch_market_constraints(
        [(f"{condition}-yes", condition) for condition in conditions] + [("c0-no", "c0")], concurrency=4
    )

    assert fetched == 20
    assert sorted(session.requested) == sorted(conditions)  # one request per market, not per token
    assert session.most_in_flight <= 4

    def paced_path(*args: object) -> None:
        raise AssertionError("the prefetched market went through the paced SDK path")

    client._get_market_constraints = paced_path  # type: ignore[method-assign,assignment]  # noqa: SLF001
    constraints = await client.get_market_constraints("c7-no", "c7")
    assert constraints is not None
    assert constraints.fee_rate_bps == 500
    assert constraints.tick_size == Decimal("0.01")
    assert client._market_options_cache["c7"] == ("0.01", False)  # noqa: SLF001
    # Already cached: nothing new is asked for.
    assert await client.prefetch_market_constraints([("c3-yes", "c3")]) == 0
    assert len(session.requested) == 20


@pytest.mark.asyncio
async def test_what_cannot_be_parsed_is_left_to_the_regular_path() -> None:
    session = _Session(
        {
            "good": (200, _info("good")),
            "no-fee": (200, _info("no-fee", fee={})),
            "gone": (404, {}),
        }
    )
    client = _client(session)

    fetched = await client.prefetch_market_constraints(
        [("good-yes", "good"), ("no-fee-yes", "no-fee"), ("gone-yes", "gone"), ("wrong-token", "good")]
    )

    assert fetched == 1
    assert "no-fee:no-fee-yes" not in client._constraints_cache  # noqa: SLF001
    assert "gone:gone-yes" not in client._constraints_cache  # noqa: SLF001


@pytest.mark.asyncio
async def test_a_rate_limit_stops_the_prefetch_and_starts_the_cooldown() -> None:
    session = _Session({"c0": (429, {}), **{f"c{index}": (200, _info(f"c{index}")) for index in range(1, 30)}})
    client = _client(session)

    await client.prefetch_market_constraints([(f"c{index}-yes", f"c{index}") for index in range(30)], concurrency=1)

    assert session.requested == ["c0"]
    assert client._market_info_cooldown_until > 0  # noqa: SLF001
    # During the cooldown nothing is asked for at all.
    assert await client.prefetch_market_constraints([("c5-yes", "c5")]) == 0
    assert session.requested == ["c0"]
    assert polymarket_module._MARKET_INFO_PREFETCH_CONCURRENCY >= 1  # noqa: SLF001
