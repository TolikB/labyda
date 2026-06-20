import asyncio
import unittest
from datetime import UTC, datetime, timedelta
from typing import Any

from arbitrage_engine.market_discovery import (
    GammaCacheUnavailable,
    GammaMarketResolver,
    _best_candidate,
    _token_id_for_side,
)
from arbitrage_engine.models import BinarySide, MarketSpec

EXPIRY = datetime(2026, 6, 28, 21, tzinfo=UTC)


def _market(*, external_id: str | None = None, title: str = "Will England defeat Panama?") -> MarketSpec:
    return MarketSpec(
        symbol=title,
        target_label=title,
        polymarket_token_id="",
        polymarket_side=BinarySide.YES,
        predict_fun_token_id="",
        predict_fun_side=BinarySide.NO,
        expires_at=EXPIRY,
        polymarket_market_id=external_id,
    )


def _candidate(
    market_id: str = "1897417",
    *,
    title: str = "Will England defeat Panama?",
    expiry: str = "2026-06-28T21:00:00Z",
) -> dict[str, Any]:
    return {
        "id": market_id,
        "question": title,
        "conditionId": f"condition-{market_id}",
        "endDate": expiry,
        "outcomes": '["No", "Yes"]',
        "clobTokenIds": f'["no-{market_id}", "yes-{market_id}"]',
        "active": True,
        "closed": False,
        "archived": False,
        "acceptingOrders": True,
        "enableOrderBook": True,
    }


class FakeGammaResolver(GammaMarketResolver):
    def __init__(self, pages: list[list[dict[str, Any]]], **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.pages = pages
        self.fetch_count = 0
        self.fail = False

    async def _fetch_all_markets(self) -> list[dict[str, Any]]:
        if self.fail:
            raise RuntimeError("synthetic Gamma failure")
        result: list[dict[str, Any]] = []
        seen: set[tuple[str, ...]] = set()
        for page in self.pages:
            self.fetch_count += 1
            self._refresh_http_requests += 1
            ids = tuple(str(item.get("id")) for item in page)
            if page and ids in seen:
                raise RuntimeError("repeated page")
            seen.add(ids)
            result.extend(page)
            if len(page) < 100:
                break
        return result


class GammaMatchingTests(unittest.TestCase):
    def test_search_fallback_rejects_unrelated_gamma_results(self) -> None:
        self.assertIsNone(_best_candidate([_candidate(title="New Rihanna Album before GTA VI?")], _market()))

    def test_external_market_id_has_priority_but_still_checks_expiry(self) -> None:
        candidates = [
            _candidate("other"),
            _candidate("1897417", title="Different title"),
        ]
        selected = _best_candidate(candidates, _market(external_id="1897417"))
        self.assertEqual(selected and selected["id"], "1897417")

        candidates[1]["endDate"] = "2026-07-01T21:00:00Z"
        self.assertIsNone(_best_candidate(candidates, _market(external_id="1897417")))

    def test_missing_expiry_and_invalid_trading_flags_fail_closed(self) -> None:
        missing_expiry = _candidate()
        missing_expiry.pop("endDate")
        self.assertIsNone(_best_candidate([missing_expiry], _market()))
        not_accepting = _candidate()
        not_accepting["acceptingOrders"] = False
        self.assertIsNone(_best_candidate([not_accepting], _market()))

    def test_expiry_window_is_inclusive_at_thirty_minutes(self) -> None:
        at_limit = _candidate(expiry=(EXPIRY + timedelta(minutes=30)).isoformat())
        outside = _candidate(expiry=(EXPIRY + timedelta(minutes=30, seconds=1)).isoformat())
        self.assertIsNotNone(_best_candidate([at_limit], _market()))
        self.assertIsNone(_best_candidate([outside], _market()))

    def test_ambiguous_normalized_title_is_rejected(self) -> None:
        self.assertIsNone(_best_candidate([_candidate("1"), _candidate("2")], _market()))

    def test_material_title_qualifiers_are_not_ignored(self) -> None:
        candidate = _candidate(title="Will England defeat Panama in the final?")
        self.assertIsNone(_best_candidate([candidate], _market()))

    def test_token_mapping_uses_labels_and_rejects_invalid_arrays(self) -> None:
        candidate = _candidate()
        self.assertEqual(_token_id_for_side(candidate, BinarySide.YES), "yes-1897417")
        self.assertEqual(_token_id_for_side(candidate, BinarySide.NO), "no-1897417")
        candidate["clobTokenIds"] = '["only-one"]'
        self.assertIsNone(_token_id_for_side(candidate, BinarySide.YES))


class GammaCacheLifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def test_resolve_is_local_and_request_count_is_independent_of_inputs(self) -> None:
        first_page = [_candidate(str(index), title=f"Market {index}") for index in range(100)]
        resolver = FakeGammaResolver([first_page, []], scan_all=True)
        await resolver.bootstrap()
        bootstrap_requests = resolver.fetch_count
        self.assertEqual(bootstrap_requests, 2)

        for count in (1, 100, 10_000):
            markets = [_market(external_id="0", title=f"Source {index}") for index in range(count)]
            resolved = await resolver.resolve(markets)
            self.assertEqual(len(resolved), count)
            self.assertTrue(all(item.polymarket_token_id == "yes-0" for item in resolved))
            self.assertEqual(resolver.fetch_count, bootstrap_requests)
        await resolver.close()

    async def test_short_page_ends_pagination(self) -> None:
        resolver = FakeGammaResolver([[_candidate("1")]])
        await resolver.bootstrap()
        self.assertEqual(resolver.fetch_count, 1)
        await resolver.close()

    async def test_repeated_page_fails_closed(self) -> None:
        page = [_candidate(str(index), title=f"Market {index}") for index in range(100)]
        resolver = FakeGammaResolver([page, page])
        with self.assertRaises(GammaCacheUnavailable):
            await resolver.bootstrap()
        with self.assertRaises(GammaCacheUnavailable):
            await resolver.resolve([_market(external_id="0")])
        await resolver.close()

    async def test_refresh_failure_keeps_recent_snapshot_then_expires_it(self) -> None:
        current = datetime(2026, 6, 20, 12, tzinfo=UTC)

        def now() -> datetime:
            return current

        resolver = FakeGammaResolver([[_candidate()]], now=now, max_stale_seconds=900)
        await resolver.bootstrap()
        self.assertEqual(
            (await resolver.resolve([_market(external_id="1897417")]))[0].polymarket_token_id, "yes-1897417"
        )

        resolver.fail = True
        with self.assertRaises(GammaCacheUnavailable):
            await resolver.refresh()
        self.assertEqual(
            (await resolver.resolve([_market(external_id="1897417")]))[0].polymarket_token_id, "yes-1897417"
        )

        current += timedelta(seconds=901)
        with self.assertRaises(GammaCacheUnavailable):
            await resolver.refresh()
        with self.assertRaises(GammaCacheUnavailable):
            await resolver.resolve([_market(external_id="1897417")])

        resolver.fail = False
        resolver.pages = [[_candidate("new")]]
        await resolver.refresh()
        resolved = await resolver.resolve([_market(external_id="new")])
        self.assertEqual(resolved[0].polymarket_token_id, "yes-new")
        await resolver.close()

    async def test_refresh_swap_is_atomic(self) -> None:
        resolver = FakeGammaResolver([[_candidate("old")]])
        await resolver.bootstrap()
        gate = asyncio.Event()
        original_fetch = resolver._fetch_all_markets

        async def blocked_fetch() -> list[dict[str, Any]]:
            await gate.wait()
            return await original_fetch()

        resolver.pages = [[_candidate("new")]]
        resolver._fetch_all_markets = blocked_fetch  # type: ignore[method-assign]
        refresh_task = asyncio.create_task(resolver.refresh())
        await asyncio.sleep(0)
        old = await resolver.resolve([_market(external_id="old")])
        self.assertEqual(old[0].polymarket_token_id, "yes-old")
        gate.set()
        await refresh_task
        new = await resolver.resolve([_market(external_id="new")])
        self.assertEqual(new[0].polymarket_token_id, "yes-new")
        await resolver.close()

    async def test_background_refresh_runs_and_close_cancels_it(self) -> None:
        resolver = FakeGammaResolver([[_candidate()]], refresh_interval_seconds=0.01)
        await resolver.bootstrap()
        resolver.start_background_refresh()
        await asyncio.sleep(0.035)
        self.assertGreaterEqual(resolver.fetch_count, 2)
        await resolver.close()
        count_after_close = resolver.fetch_count
        await asyncio.sleep(0.02)
        self.assertEqual(resolver.fetch_count, count_after_close)


if __name__ == "__main__":
    unittest.main()
