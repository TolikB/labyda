import asyncio
import time
import unittest
from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import AsyncMock, patch

from arbitrage_engine.market_discovery import (
    GammaCacheUnavailable,
    GammaMarketResolver,
    _allow_semantic_scan,
    _best_candidate,
    _bounded_retry_after,
    _candidate_expiry,
    _gamma_seed_market_id,
    _token_id_for_market,
    _token_id_for_side,
)
from arbitrage_engine.models import BinarySide, MarketSpec

EXPIRY = datetime.now(UTC) + timedelta(days=30)


def _market(
    *,
    external_id: str | None = None,
    title: str = "Will England defeat Panama?",
    category: str | None = None,
    venue_b_label: str = "Predict.fun",
) -> MarketSpec:
    return MarketSpec(
        symbol=title,
        target_label=title,
        polymarket_token_id="",
        polymarket_side=BinarySide.YES,
        predict_fun_token_id="",
        predict_fun_side=BinarySide.NO,
        expires_at=EXPIRY,
        polymarket_market_id=external_id,
        category=category,
        venue_b_label=venue_b_label,
    )


def _candidate(
    market_id: str = "1897417",
    *,
    title: str = "Will England defeat Panama?",
    expiry: str | None = None,
) -> dict[str, Any]:
    return {
        "id": market_id,
        "question": title,
        "conditionId": f"condition-{market_id}",
        "endDate": expiry or EXPIRY.isoformat(),
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


class _Response:
    def __init__(self, status: int, payload: Any, headers: dict[str, str] | None = None) -> None:
        self.status = status
        self._payload = payload
        self.headers = headers or {}

    async def __aenter__(self) -> "_Response":
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None

    def raise_for_status(self) -> None:
        if self.status >= 400:
            raise RuntimeError(f"HTTP {self.status}")

    async def json(self) -> Any:
        return self._payload


class _Session:
    def __init__(self, responses: list[_Response]) -> None:
        self.responses = responses
        self.calls: list[tuple[str, Any, float]] = []
        self.closed = False

    def get(self, url: str, *, params: Any, timeout: float) -> _Response:
        self.calls.append((url, params, timeout))
        return self.responses.pop(0)


class GammaMatchingTests(unittest.TestCase):
    def test_precise_gamma_end_date_precedes_clob_date_only_expiry(self) -> None:
        candidate = _candidate(expiry="2026-07-15T16:00:00Z")
        candidate["endDateIso"] = "2026-07-15"

        self.assertEqual(_candidate_expiry(candidate), datetime(2026, 7, 15, 16, tzinfo=UTC))

    def test_predict_fun_market_without_external_id_skips_semantic_scan(self) -> None:
        predict_market = _market(external_id=None, venue_b_label="Predict.fun")

        self.assertFalse(_allow_semantic_scan(predict_market))

    def test_gamma_seed_market_id_skips_condition_hashes(self) -> None:
        numeric = _market(external_id="2707658")
        condition_hash = _market(external_id="0x094725e5691f06b1895496ab0823b3f843c3be680f870cb721958d4e4d280a55")
        opaque = _market(external_id="poly-market")

        self.assertEqual(_gamma_seed_market_id(numeric), "2707658")
        self.assertIsNone(_gamma_seed_market_id(condition_hash))
        self.assertIsNone(_gamma_seed_market_id(opaque))

    def test_search_fallback_rejects_unrelated_gamma_results(self) -> None:
        self.assertIsNone(_best_candidate([_candidate(title="New Rihanna Album before GTA VI?")], _market()))

    def test_external_market_id_has_priority_but_still_checks_expiry(self) -> None:
        candidates = [
            _candidate("other"),
            _candidate("1897417", title="Different title"),
        ]
        selected = _best_candidate(candidates, _market(external_id="1897417"))
        self.assertEqual(selected and selected["id"], "1897417")

        candidates[1]["endDate"] = (EXPIRY + timedelta(days=2)).isoformat()
        self.assertIsNone(_best_candidate(candidates, _market(external_id="1897417")))

    def test_immutable_id_allows_date_only_close_time_drift(self) -> None:
        candidate = _candidate("1897417", expiry=(EXPIRY + timedelta(hours=1)).isoformat())

        selected = _best_candidate([candidate], _market(external_id="1897417"))

        self.assertIsNotNone(selected)

    def test_external_condition_id_is_an_immutable_lookup_key(self) -> None:
        candidate = _candidate("gamma-id")
        candidate["conditionId"] = "condition-external"

        selected = _best_candidate([candidate], _market(external_id="condition-external"))

        self.assertEqual(selected and selected["id"], "gamma-id")

    def test_unique_semantic_title_variant_is_accepted(self) -> None:
        selected = _best_candidate(
            [_candidate(title="Will England defeat Panama")],
            _market(title="Will England defeat Panama?"),
        )

        self.assertIsNotNone(selected)

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

    def test_sx_sports_expiry_window_allows_multi_day_outright_drift(self) -> None:
        candidate = _candidate(
            title="Will France win the World Cup?",
            expiry=(EXPIRY + timedelta(days=3)).isoformat(),
        )

        selected = _best_candidate(
            [candidate],
            _market(
                title="Will France win the World Cup?",
                category="sports",
                venue_b_label="SX Bet",
            ),
        )

        self.assertIsNotNone(selected)

    def test_sx_sports_semantic_match_accepts_fifa_world_cup_title_variant(self) -> None:
        candidate = _candidate(
            title="Will Brazil win the 2026 FIFA World Cup?",
            expiry=EXPIRY.isoformat(),
        )

        selected = _best_candidate(
            [candidate],
            MarketSpec(
                symbol="Will Brazil win the World Cup?",
                target_label="Brazil",
                polymarket_token_id="",
                polymarket_side=BinarySide.YES,
                predict_fun_token_id="",
                predict_fun_side=BinarySide.NO,
                category="sports",
                venue_b_label="SX Bet",
                expires_at=EXPIRY,
            ),
        )

        self.assertIsNotNone(selected)

    def test_sx_sports_semantic_match_rejects_wrong_subject_even_when_title_shape_is_similar(self) -> None:
        candidate = _candidate(
            title="Will Spain win the World Cup?",
            expiry=EXPIRY.isoformat(),
        )

        selected = _best_candidate(
            [candidate],
            MarketSpec(
                symbol="Will Brazil win the World Cup?",
                target_label="Brazil",
                polymarket_token_id="",
                polymarket_side=BinarySide.YES,
                predict_fun_token_id="",
                predict_fun_side=BinarySide.NO,
                category="sports",
                venue_b_label="SX Bet",
                expires_at=EXPIRY,
            ),
        )

        self.assertIsNone(selected)

    def test_sx_sports_semantic_match_ignores_metadata_blob_drift(self) -> None:
        candidate = _candidate(
            title="Will Brazil win the 2026 FIFA World Cup?",
            expiry=EXPIRY.isoformat(),
        )
        candidate["description"] = (
            "This market resolves according to the national team that wins the 2026 FIFA World Cup."
        )

        selected = _best_candidate(
            [candidate],
            MarketSpec(
                symbol="Will Brazil win the World Cup?",
                target_label="Brazil",
                polymarket_token_id="",
                polymarket_side=BinarySide.YES,
                predict_fun_token_id="",
                predict_fun_side=BinarySide.NO,
                category="sports",
                venue_b_label="SX Bet",
                expires_at=EXPIRY,
                outcome_semantics="Outcome one=Brazil; outcome two=The Field; type=274",
                resolution_source="Official Outrights - World Cup result",
            ),
        )

        self.assertIsNotNone(selected)

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

    def test_named_outcome_market_uses_target_label_when_yes_no_labels_are_absent(self) -> None:
        candidate = {
            "id": "world-cup",
            "conditionId": "condition-world-cup",
            "question": "Will France win the World Cup?",
            "endDate": EXPIRY.isoformat(),
            "outcomes": '["The Field", "France"]',
            "clobTokenIds": '["poly-field", "poly-france"]',
            "active": True,
            "closed": False,
            "archived": False,
            "acceptingOrders": True,
            "enableOrderBook": True,
        }

        france = MarketSpec(
            symbol="Will France win the World Cup?",
            target_label="France",
            polymarket_token_id="",
            polymarket_side=BinarySide.YES,
            predict_fun_token_id="",
            predict_fun_side=BinarySide.NO,
            expires_at=EXPIRY,
            category="sports",
            venue_b_label="SX Bet",
        )
        field = MarketSpec(
            symbol="Will France win the World Cup?",
            target_label="The Field",
            polymarket_token_id="",
            polymarket_side=BinarySide.NO,
            predict_fun_token_id="",
            predict_fun_side=BinarySide.YES,
            expires_at=EXPIRY,
            category="sports",
            venue_b_label="SX Bet",
        )

        self.assertEqual(_token_id_for_market(candidate, france), "poly-france")
        self.assertEqual(_token_id_for_market(candidate, field), "poly-field")


class GammaCacheLifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def test_condition_dedup_keeps_numeric_market_id_as_exact_lookup_alias(self) -> None:
        clob = _candidate("0xcondition")
        clob["conditionId"] = "condition-shared"
        clob["url"] = "https://polymarket.com/event/canonical"
        clob["description"] = "Canonical CLOB payload"
        gamma = _candidate("2846318")
        gamma["conditionId"] = "condition-shared"
        resolver = FakeGammaResolver([[clob, gamma]], scan_all=True)

        await resolver.bootstrap()
        resolved = await resolver.resolve([_market(external_id="2846318")])

        self.assertEqual(len(resolved), 1)
        self.assertEqual(resolver.last_resolution_stats.exact_id_matches, 1)
        self.assertEqual(resolved[0].condition_id, "condition-shared")
        self.assertEqual(resolved[0].mapping_strategy, "exact_id")
        self.assertEqual(resolved[0].polymarket_url, "https://polymarket.com/event/canonical")
        await resolver.close()

    async def test_duplicate_market_id_is_deduplicated_without_failing_refresh(self) -> None:
        primary = _candidate("duplicate")
        richer_duplicate = _candidate("duplicate")
        richer_duplicate["url"] = "https://polymarket.com/event/duplicate"
        richer_duplicate["description"] = "More complete duplicate payload"

        resolver = FakeGammaResolver([[primary, richer_duplicate]])

        await resolver.bootstrap()

        self.assertEqual(resolver.catalog_size, 1)
        resolved = await resolver.resolve([_market(external_id="duplicate")])
        self.assertEqual(resolved[0].polymarket_token_id, "yes-duplicate")
        self.assertEqual(resolved[0].polymarket_url, "https://polymarket.com/event/duplicate")
        await resolver.close()

    async def test_scan_all_exposes_aggregated_resolution_stats(self) -> None:
        resolver = FakeGammaResolver(
            [
                [
                    _candidate("exact", title="Different exact-id title"),
                    _candidate("semantic", title="England defeated Panama?"),
                ]
            ],
            scan_all=True,
        )
        await resolver.bootstrap()

        resolved = await resolver.resolve(
            [
                _market(external_id="exact"),
                _market(title="England defeat Panama?", venue_b_label="Myriad"),
                _market(title="Unrelated market"),
            ]
        )

        self.assertEqual(len(resolved), 2)
        self.assertEqual(resolver.last_resolution_stats.exact_id_matches, 1)
        self.assertEqual(resolver.last_resolution_stats.semantic_matches, 1)
        self.assertEqual(resolver.last_resolution_stats.unresolved, 1)
        self.assertEqual(dict(resolver.last_resolution_stats.rejection_reasons), {"no_safe_match": 1})
        self.assertEqual({market.mapping_strategy for market in resolved}, {"exact_id", "semantic"})
        await resolver.close()

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
            self.assertIsNone(resolver._session)
        await resolver.close()

    async def test_clob_429_retries_using_bounded_retry_after(self) -> None:
        resolver = GammaMarketResolver()
        session = _Session(
            [
                _Response(429, None, {"Retry-After": "60"}),
                _Response(200, {"data": [], "next_cursor": "LTE="}),
            ]
        )
        resolver._session = session
        with (
            patch.object(resolver, "_pace_request", AsyncMock()),
            patch("arbitrage_engine.market_discovery.asyncio.sleep", AsyncMock()) as sleep,
        ):
            page, cursor = await resolver._fetch_clob_page("MA==")

        self.assertEqual((page, cursor), ([], "LTE="))
        self.assertEqual(len(session.calls), 2)
        sleep.assert_awaited_once_with(30.0)

    async def test_clob_403_uses_urllib_fallback(self) -> None:
        resolver = GammaMarketResolver()

        class ForbiddenError(RuntimeError):
            status = 403

        with patch.object(resolver, "_get_json_with_retries", AsyncMock(side_effect=ForbiddenError("403"))), patch(
            "arbitrage_engine.market_discovery._load_json_via_urllib",
            AsyncMock(return_value={"data": [], "next_cursor": "LTE="}),
        ) as fallback:
            page, cursor = await resolver._fetch_clob_page("MA==")

        self.assertEqual((page, cursor), ([], "LTE="))
        fallback.assert_awaited_once()

    def test_gamma_session_uses_browser_like_headers_for_polymarket_feed(self) -> None:
        resolver = GammaMarketResolver()
        session = _Session([])

        with patch("arbitrage_engine.market_discovery.client_session", return_value=session) as factory:
            self.assertIs(resolver._get_session(), session)  # noqa: SLF001

        factory.assert_called_once_with({"User-Agent": "Mozilla/5.0", "Accept": "application/json"})

    async def test_clob_cursor_pagination_is_sequential(self) -> None:
        class Resolver(GammaMarketResolver):
            def __init__(self) -> None:
                super().__init__()
                self.active = 0
                self.max_active = 0
                self.cursors: list[str] = []

            async def _fetch_clob_page(self, cursor: str) -> tuple[list[dict[str, Any]], str | None]:
                self.active += 1
                self.max_active = max(self.max_active, self.active)
                self.cursors.append(cursor)
                await asyncio.sleep(0)
                self.active -= 1
                next_cursor = "one" if cursor == "MA==" else "LTE="
                return [], next_cursor

        resolver = Resolver()
        self.assertEqual(await resolver._fetch_clob_markets(), [])
        self.assertEqual(resolver.cursors, ["MA==", "one"])
        self.assertEqual(resolver.max_active, 1)

    async def test_clob_pagination_fails_before_two_hundred_pages(self) -> None:
        class Resolver(GammaMarketResolver):
            def __init__(self) -> None:
                super().__init__()
                self.calls = 0

            async def _fetch_clob_page(self, cursor: str) -> tuple[list[dict[str, Any]], str | None]:
                del cursor
                self.calls += 1
                return [], f"cursor-{self.calls}"

        resolver = Resolver()
        with self.assertRaisesRegex(RuntimeError, "exceeded 199 pages"):
            await resolver._fetch_clob_markets()
        self.assertEqual(resolver.calls, 199)

    def test_retry_after_accepts_seconds_and_http_dates(self) -> None:
        self.assertEqual(_bounded_retry_after("60"), 30.0)
        self.assertEqual(_bounded_retry_after("Wed, 21 Oct 2050 07:28:00 GMT"), 30.0)

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

    async def test_refresh_offloads_snapshot_build_from_event_loop(self) -> None:
        class SlowBuildResolver(FakeGammaResolver):
            def _build_snapshot(self, payloads: list[dict[str, Any]], *, generation: int) -> Any:
                time.sleep(0.05)
                return super()._build_snapshot(payloads, generation=generation)

        resolver = SlowBuildResolver([[_candidate()]])
        refresh_task = asyncio.create_task(resolver.refresh())
        probe = asyncio.create_task(asyncio.sleep(0.005))

        await probe

        self.assertFalse(refresh_task.done())
        await refresh_task
        await resolver.close()


if __name__ == "__main__":
    unittest.main()
