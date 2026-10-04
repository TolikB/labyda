"""Discovery's heavy work runs in a worker process; these tests pin how it gets there and back.

The trading process sits at about one core through a funded window, and the
catalog rebuild used to take the other half of that one core in a thread for
two minutes every ten. These cases are about the move to a second process being
invisible to everything except the event loop: the same results, the same
snapshot, and a thread to fall back on if the worker is lost.
"""

from __future__ import annotations

import asyncio
import os
import pickle
from collections.abc import Iterator
from concurrent.futures.process import BrokenProcessPool
from datetime import UTC, datetime, timedelta
from types import MappingProxyType
from typing import Any

import pytest

from arbitrage_engine import discovery_cpu, market_discovery
from arbitrage_engine.models import BinarySide, MarketSpec

NOW = datetime(2026, 10, 4, 9, 0, tzinfo=UTC)
EXPIRY = NOW + timedelta(days=2)


@pytest.fixture(autouse=True)
def _isolation_off_afterwards() -> Iterator[None]:
    yield
    discovery_cpu.configure_discovery_process_isolation(False)


def _payload(market_id: str, title: str) -> dict[str, Any]:
    return {
        "id": market_id,
        "question": title,
        "conditionId": f"condition-{market_id}",
        "endDate": EXPIRY.isoformat(),
        "outcomes": '["No", "Yes"]',
        "clobTokenIds": f'["no-{market_id}", "yes-{market_id}"]',
        "active": True,
        "closed": False,
        "archived": False,
        "acceptingOrders": True,
        "enableOrderBook": True,
    }


def _seed(market_id: str | None, title: str) -> MarketSpec:
    return MarketSpec(
        symbol=title,
        target_label=title,
        polymarket_token_id="",
        polymarket_side=BinarySide.YES,
        predict_fun_token_id="",
        predict_fun_side=BinarySide.NO,
        expires_at=EXPIRY,
        polymarket_market_id=market_id,
        venue_b_label="Predict.fun",
    )


def _catalog() -> tuple[list[dict[str, Any]], list[MarketSpec]]:
    payloads = [_payload(str(1000 + index), f"Will team {index} win on October 6?") for index in range(60)]
    # A duplicate id and a second id on the same condition: the builder keeps one
    # and aliases the other, so the snapshot has shared objects to preserve.
    payloads.append(_payload("1000", "Will team 0 win on October 6?"))
    payloads.append({**_payload("2000", "Will team 1 win on October 6?"), "conditionId": "condition-1001"})
    seeds = [_seed(str(1000 + index), f"Will team {index} win on October 6?") for index in range(0, 60, 3)]
    seeds.append(_seed("9999", "A market Polymarket does not list"))
    seeds.append(_seed(None, "Will team 7 win on October 6?"))
    return payloads, seeds


@pytest.mark.asyncio
async def test_with_isolation_off_the_work_stays_in_this_process() -> None:
    # Tests, the CLI and the operator scripts never switch it on, and must keep
    # exactly the behaviour they had.
    assert await discovery_cpu.run_discovery_process(os.getpid) == os.getpid()


@pytest.mark.asyncio
async def test_with_isolation_on_the_work_runs_in_another_process() -> None:
    discovery_cpu.configure_discovery_process_isolation(True)

    worker_pid = await asyncio.wait_for(discovery_cpu.run_discovery_process(os.getpid), timeout=120)

    assert worker_pid != os.getpid()


def test_a_snapshot_survives_the_process_boundary_with_its_shape_intact() -> None:
    # Every payload and index is a mappingproxy, which does not pickle on its
    # own. The registered reducer carries it as a dict and restores the proxy,
    # and pickle's memo keeps one object per payload across all four indexes.
    payloads, _ = _catalog()
    snapshot = market_discovery._build_gamma_snapshot(payloads, generation=3, now=NOW)  # noqa: SLF001

    restored = pickle.loads(pickle.dumps(snapshot, protocol=pickle.HIGHEST_PROTOCOL))

    assert isinstance(restored.by_id, MappingProxyType)
    assert all(isinstance(item, MappingProxyType) for item in restored.markets)
    assert len(restored.markets) == len(snapshot.markets)
    assert set(restored.by_id) == set(snapshot.by_id)
    assert restored.generation == 3
    for market_id, payload in restored.by_id.items():
        assert dict(payload) == dict(snapshot.by_id[market_id])
    # Shared, not copied: the alias and the market it points at are one object,
    # and the market list holds that same object.
    assert restored.by_id["2000"] is restored.by_id["1001"]
    assert any(item is restored.by_id["1001"] for item in restored.markets)
    # Still read-only on arrival.
    with pytest.raises(TypeError):
        restored.by_id["1000"]["question"] = "changed"


@pytest.mark.asyncio
async def test_the_worker_matches_exactly_what_the_thread_matches() -> None:
    payloads, seeds = _catalog()

    thread_snapshot = await discovery_cpu.run_discovery_process(
        market_discovery._build_gamma_snapshot, payloads, generation=1, now=NOW  # noqa: SLF001
    )
    thread_results = await discovery_cpu.run_discovery_process(
        market_discovery._resolve_scan_all_against, thread_snapshot, list(seeds)  # noqa: SLF001
    )

    discovery_cpu.configure_discovery_process_isolation(True)
    worker_snapshot = await asyncio.wait_for(
        discovery_cpu.run_discovery_process(
            market_discovery._build_gamma_snapshot, payloads, generation=1, now=NOW  # noqa: SLF001
        ),
        timeout=120,
    )
    worker_results = await asyncio.wait_for(
        discovery_cpu.run_discovery_process(
            market_discovery._resolve_scan_all_against, worker_snapshot, list(seeds)  # noqa: SLF001
        ),
        timeout=120,
    )

    assert [dict(item) for item in worker_snapshot.markets] == [dict(item) for item in thread_snapshot.markets]
    assert worker_results == thread_results
    resolved, stats = worker_results
    assert stats.requested == len(seeds)
    assert stats.unresolved >= 1  # the market Polymarket does not list
    assert any(market.polymarket_token_id == "yes-1003" for market in resolved)


@pytest.mark.asyncio
async def test_a_lost_worker_costs_the_call_its_isolation_not_its_result(monkeypatch: pytest.MonkeyPatch) -> None:
    # A worker killed for memory surfaces as BrokenProcessPool. The call is run
    # again in the thread, and the dead pool is dropped so the next call starts
    # a fresh worker rather than failing on the broken one forever.
    class _BrokenPool:
        def submit(self, *args: object, **kwargs: object) -> object:
            raise BrokenProcessPool("worker was killed")

        def shutdown(self, wait: bool = True, *, cancel_futures: bool = False) -> None:
            del wait, cancel_futures

    discovery_cpu.configure_discovery_process_isolation(True)
    monkeypatch.setattr(discovery_cpu, "_PROCESS_EXECUTOR", _BrokenPool())

    result = await discovery_cpu.run_discovery_process(os.getpid)

    assert result == os.getpid()  # answered by the thread, in this process
    assert _current_executor() is None


def test_switching_isolation_off_shuts_the_worker_down() -> None:
    discovery_cpu.configure_discovery_process_isolation(True)
    discovery_cpu._process_executor()  # noqa: SLF001
    assert _current_executor() is not None

    discovery_cpu.configure_discovery_process_isolation(False)

    assert _current_executor() is None
    assert not discovery_cpu.discovery_process_isolation_enabled()


def _current_executor() -> object:
    return discovery_cpu._PROCESS_EXECUTOR  # noqa: SLF001
