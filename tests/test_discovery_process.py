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

from arbitrage_engine import discovery_cpu, market_discovery, myriad_discovery, predict_fun_discovery
from arbitrage_engine.matcher import MarketText
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
async def test_the_worker_resolves_myriad_exactly_as_the_thread_does() -> None:
    # The Myriad pass compares every seed with every Myriad title; on 2026-10-05
    # it held the trading process's GIL for 2.5 minutes. Across the boundary it
    # must give the same markets, and hand back its log records as data.
    payloads, _ = _catalog()
    snapshot = market_discovery._build_gamma_snapshot(payloads, generation=1, now=NOW)  # noqa: SLF001
    seeds = [_seed(None, f"Will team {index} win on October 6?") for index in range(30)]
    gamma_resolved, _ = market_discovery._resolve_scan_all_against(snapshot, seeds)  # noqa: SLF001
    # Half the seeds get a Myriad market under the very title the resolver
    # compares, so the semantic path is exercised, not just the misses.
    myriad_catalog = [
        MarketText(
            platform="myriad",
            market_id=f"myriad-{index}",
            title=myriad_discovery._source_market_text(market).title,  # noqa: SLF001
            expires_at=EXPIRY + timedelta(minutes=5),
            condition_id=f"0xcondition{index}",
        )
        for index, market in enumerate(gamma_resolved[::2])
    ]

    in_thread = await discovery_cpu.run_discovery_process(
        myriad_discovery._resolve_market_specs, gamma_resolved, myriad_catalog  # noqa: SLF001
    )
    discovery_cpu.configure_discovery_process_isolation(True)
    in_worker = await asyncio.wait_for(
        discovery_cpu.run_discovery_process(
            myriad_discovery._resolve_market_specs, gamma_resolved, myriad_catalog  # noqa: SLF001
        ),
        timeout=120,
    )

    assert in_worker == in_thread
    resolved, discoveries = in_worker
    assert len(resolved) == len(gamma_resolved)
    assert discoveries  # some seeds did match a Myriad title
    assert {item["_myriad_market_id"] for item in discoveries} <= {item.market_id for item in myriad_catalog}
    assert sum(market.myriad_market_id is not None for market in resolved) == len(discoveries)


@pytest.mark.asyncio
async def test_the_worker_parses_the_predict_catalog_exactly_as_the_thread_does() -> None:
    # About seven seconds of interpreter time per cycle in production (12k raw
    # markets). The worker must hand back the same specs, poisoned and filtered
    # rows included.
    def predict_payload(index: int, **extra: Any) -> dict[str, Any]:
        return {
            "id": f"market-{index}",
            "question": f"Will team {index} win on October 6?",
            "expiresAt": EXPIRY.isoformat(),
            "tradingStatus": "OPEN",
            "categorySlug": "sports" if index % 2 else "crypto",
            "outcomes": [
                {"name": "Yes", "onChainId": f"yes-{index}"},
                {"name": "No", "onChainId": f"no-{index}"},
            ],
            **extra,
        }

    payloads = [predict_payload(index) for index in range(40)]
    payloads.append(predict_payload(3, question="A second row claiming market-3"))  # poisons market-3
    payloads.append(predict_payload(41, tradingStatus="CLOSED"))

    in_thread = await discovery_cpu.run_discovery_process(
        predict_fun_discovery._parse_scan_all_catalog, payloads, set()  # noqa: SLF001
    )
    discovery_cpu.configure_discovery_process_isolation(True)
    in_worker = await asyncio.wait_for(
        discovery_cpu.run_discovery_process(
            predict_fun_discovery._parse_scan_all_catalog, payloads, set()  # noqa: SLF001
        ),
        timeout=120,
    )

    assert in_worker == in_thread
    market_ids = {market.predict_fun_market_id for market in in_worker}
    assert "market-3" not in market_ids
    assert "market-41" not in market_ids
    assert len(market_ids) == 39


def _many_seeds(count: int) -> list[MarketSpec]:
    # Enough seeds to cross in several slices; every third names a listed market.
    return [
        _seed(str(1000 + index % 60) if index % 3 == 0 else None, f"Will team {index % 60} win on October 6?")
        for index in range(count)
    ]


@pytest.mark.asyncio
async def test_sliced_transfers_give_the_worker_and_the_thread_the_same_answer() -> None:
    # The production path: the snapshot comes back as an opaque handle that is
    # never unpacked here, goes out again for the match, and the seeds and the
    # results cross in slices. It must agree with the thread to the last field.
    payloads, _ = _catalog()
    seeds = _many_seeds(3 * discovery_cpu.TRANSFER_SLICE_ITEMS + 7)

    thread_snapshot = market_discovery._build_gamma_snapshot(payloads, generation=1, now=NOW)  # noqa: SLF001
    thread_results = market_discovery._resolve_scan_all_against(thread_snapshot, list(seeds))  # noqa: SLF001

    discovery_cpu.configure_discovery_process_isolation(True)
    handle, summary = await asyncio.wait_for(
        discovery_cpu.run_discovery_process_sliced(
            market_discovery._build_gamma_snapshot_with_summary,  # noqa: SLF001
            payloads,
            generation=1,
            now=NOW,
            opaque_results=(0,),
        ),
        timeout=120,
    )
    assert isinstance(handle, discovery_cpu.WorkerHandle)
    assert not handle._has_value  # noqa: SLF001 - the trading process never unpickled it
    assert summary.market_count == len(thread_snapshot.markets)
    assert summary.usable

    worker_results = await asyncio.wait_for(
        discovery_cpu.run_discovery_process_sliced(
            market_discovery._resolve_scan_all_against, handle, list(seeds)  # noqa: SLF001
        ),
        timeout=120,
    )

    assert not handle._has_value  # noqa: SLF001 - still only bytes after going out again
    assert worker_results == thread_results
    assert len(worker_results[0]) == len(seeds)


@pytest.mark.asyncio
async def test_with_isolation_off_sliced_calls_run_unchanged_in_the_thread() -> None:
    payloads, _ = _catalog()

    handle, summary = await discovery_cpu.run_discovery_process_sliced(
        market_discovery._build_gamma_snapshot_with_summary,  # noqa: SLF001
        payloads,
        generation=2,
        now=NOW,
        opaque_results=(0,),
    )

    assert handle._has_value  # noqa: SLF001 - nothing was pickled
    assert handle.value().generation == 2
    assert summary.market_count == len(handle.value().markets)


def test_slices_round_trip_lists_and_tuples_in_order() -> None:
    count = 2 * discovery_cpu.TRANSFER_SLICE_ITEMS + 1
    items = [{"index": index, "nested": [index, str(index)]} for index in range(count)]

    sliced = discovery_cpu._slice_sync(items)  # noqa: SLF001
    assert isinstance(sliced, discovery_cpu._Sliced)  # noqa: SLF001
    assert len(sliced.blobs) == 3
    assert discovery_cpu._unslice(sliced) == items  # noqa: SLF001
    assert discovery_cpu._unslice(discovery_cpu._slice_sync(tuple(items))) == tuple(items)  # noqa: SLF001
    # Small values are passed as they are.
    assert discovery_cpu._slice_sync(items[:10]) == items[:10]  # noqa: SLF001
    assert asyncio.run(discovery_cpu._slice_in(sliced)) == items  # noqa: SLF001


def test_a_handle_pickles_as_its_bytes_and_unpickles_once() -> None:
    value = {"markets": list(range(5))}
    handle = discovery_cpu.WorkerHandle(value=value, has_value=True)

    arrived = pickle.loads(pickle.dumps(handle))

    assert not arrived._has_value  # noqa: SLF001
    assert arrived.value() == value
    assert arrived.value() is arrived.value()


@pytest.mark.asyncio
async def test_the_resolver_keeps_the_snapshot_packed_on_the_scan_all_path() -> None:
    payloads, _ = _catalog()
    seeds = _many_seeds(discovery_cpu.TRANSFER_SLICE_ITEMS * 2 + 3)

    class _Resolver(market_discovery.GammaMarketResolver):
        async def _fetch_all_markets(self) -> list[dict[str, Any]]:
            return payloads

    in_thread = _Resolver(scan_all=True, now=lambda: NOW)
    await in_thread.bootstrap(seeds)
    thread_markets = await in_thread.resolve(list(seeds))

    discovery_cpu.configure_discovery_process_isolation(True)
    in_worker = _Resolver(scan_all=True, now=lambda: NOW)
    await asyncio.wait_for(in_worker.bootstrap(seeds), timeout=120)
    worker_markets = await asyncio.wait_for(in_worker.resolve(list(seeds)), timeout=120)

    assert not in_worker._snapshot_handle._has_value  # noqa: SLF001
    assert in_worker.catalog_size == in_thread.catalog_size > 0
    assert worker_markets == thread_markets
    assert in_worker.last_resolution_stats == in_thread.last_resolution_stats
    # A single-market reader in this process still gets the object, with the
    # freshness this process recorded.
    assert in_worker._snapshot.usable  # noqa: SLF001
    assert in_worker._snapshot.fetched_at == NOW  # noqa: SLF001


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
