import unittest
from dataclasses import replace
from datetime import UTC, datetime
from types import TracebackType
from unittest.mock import AsyncMock, patch

from arbitrage_engine.config import SxBetConfig
from arbitrage_engine.models import BinarySide, MarketSpec
from arbitrage_engine.sx_bet_discovery import (
    SxBetMarketResolver,
    _fetch_market_page,
    _next_pagination_key,
    _sx_market_text,
)


def _sx_config() -> SxBetConfig:
    return SxBetConfig(
        enabled=True,
        api_base_url="https://api.sx.bet",
        api_key=None,
        private_key=None,
        rpc_url="https://rpc-rollup.sx.technology",
        rpc_urls=["https://rpc-rollup.sx.technology"],
        chain_id=4162,
    )


def _payload() -> dict[str, object]:
    return {
        "marketHash": "0xmarket",
        "eventName": "Arsenal vs Chelsea",
        "type": "moneyline",
        "startsAt": "2026-07-01T12:00:00Z",
        "outcomeOneName": "Arsenal",
        "outcomeTwoName": "Chelsea",
        "leagueLabel": "Premier League",
        "volumeUsd": 125000,
    }


def _live_payload() -> dict[str, object]:
    return {
        "marketHash": "0xlive-market",
        "group1": "Outrights - World Cup",
        "gameTime": 1784721600,
        "sportLabel": "Soccer",
        "leagueLabel": "Outrights - World Cup",
        "sportXeventId": "S1781204400:france:the-field",
        "teamOneName": "France",
        "teamTwoName": "The Field",
        "outcomeOneName": "France",
        "outcomeTwoName": "The Field",
        "type": 274,
    }


class SxBetDiscoveryTests(unittest.IsolatedAsyncioTestCase):
    async def test_public_discovery_does_not_send_api_key(self) -> None:
        captured_headers: dict[str, str] = {}

        class Session:
            closed = False

            async def close(self) -> None:
                self.closed = True

        def session_factory(headers: dict[str, str] | None = None) -> Session:
            captured_headers.update(headers or {})
            return Session()

        resolver = SxBetMarketResolver(replace(_sx_config(), api_key="sensitive-key", api_version="v3"))
        with patch("arbitrage_engine.sx_bet_discovery.client_session", side_effect=session_factory):
            resolver._get_session()  # noqa: SLF001

        self.assertEqual(
            captured_headers,
            {
                "Accept": "application/json",
                "User-Agent": "labyda-arbitrage/1.0 (+https://docs.sx.bet/)",
            },
        )
        await resolver.close()

    async def test_next_pagination_key_reads_live_shape(self) -> None:
        payload = {"data": {"markets": [_payload()], "nextKey": "cursor-2"}}

        self.assertEqual(_next_pagination_key(payload), "cursor-2")

    async def test_live_market_shape_is_parsed(self) -> None:
        market = _sx_market_text(_live_payload())

        assert market is not None
        self.assertEqual(market.market_id, "0xlive-market")
        self.assertEqual(market.category, "sports")
        self.assertEqual(market.yes_label, "France")
        self.assertEqual(market.no_label, "The Field")
        self.assertIn("Outrights - World Cup", market.title)
        self.assertIn("France vs The Field", market.title)
        self.assertNotIn("274", market.title)

    async def test_resolve_uses_exact_market_hash_when_configured(self) -> None:
        resolver = SxBetMarketResolver(_sx_config())
        resolver._fetch_markets = AsyncMock(return_value=[_payload()])  # type: ignore[method-assign]
        market = MarketSpec(
            symbol="Arsenal",
            target_label="Arsenal win",
            polymarket_token_id="poly",
            polymarket_side=BinarySide.YES,
            predict_fun_token_id="",
            predict_fun_side=BinarySide.NO,
            venue_b_label="SX Bet",
            predict_fun_market_id="0xmarket",
            expires_at=datetime(2026, 7, 1, 14, tzinfo=UTC),
            cutoff_at=datetime(2026, 7, 1, 13, tzinfo=UTC),
        )

        resolved = await resolver.resolve([market])

        self.assertEqual(resolved[0].predict_fun_token_id, "0xmarket:NO")
        self.assertEqual(resolved[0].venue_b_label, "SX Bet")
        self.assertEqual(resolved[0].predict_fun_market_id, "0xmarket")
        self.assertEqual(resolved[0].mapping_strategy, "exact_id")
        self.assertEqual(resolved[0].cutoff_at, datetime(2026, 7, 1, 12, tzinfo=UTC))

    async def test_resolve_uses_strict_structured_sports_match(self) -> None:
        resolver = SxBetMarketResolver(_sx_config())
        resolver._fetch_markets = AsyncMock(return_value=[_payload()])  # type: ignore[method-assign]
        market = MarketSpec(
            symbol="Will Arsenal beat Chelsea?",
            target_label="Arsenal",
            polymarket_token_id="poly",
            polymarket_side=BinarySide.YES,
            predict_fun_token_id="",
            predict_fun_side=BinarySide.NO,
            venue_b_label="SX Bet",
            expires_at=datetime(2026, 7, 1, 14, tzinfo=UTC),
            cutoff_at=datetime(2026, 7, 1, 13, tzinfo=UTC),
            category="sports",
        )

        resolved = await resolver.resolve([market])

        self.assertEqual(resolved[0].predict_fun_market_id, "0xmarket")
        self.assertEqual(resolved[0].predict_fun_token_id, "0xmarket:NO")
        self.assertEqual(resolved[0].mapping_strategy, "structured_sports")
        self.assertEqual(resolved[0].cutoff_at, datetime(2026, 7, 1, 12, tzinfo=UTC))

    async def test_resolve_rejects_ambiguous_structured_sports_match(self) -> None:
        duplicate = {**_payload(), "marketHash": "0xduplicate"}
        resolver = SxBetMarketResolver(_sx_config())
        resolver._fetch_markets = AsyncMock(return_value=[_payload(), duplicate])  # type: ignore[method-assign]
        market = MarketSpec(
            symbol="Will Arsenal beat Chelsea?",
            target_label="Arsenal",
            polymarket_token_id="poly",
            polymarket_side=BinarySide.YES,
            predict_fun_token_id="",
            predict_fun_side=BinarySide.NO,
            venue_b_label="SX Bet",
            expires_at=datetime(2026, 7, 1, 12, tzinfo=UTC),
            category="sports",
        )

        resolved = await resolver.resolve([market])

        self.assertEqual(resolved, [market])

    async def test_scan_all_generates_two_side_specific_specs(self) -> None:
        resolver = SxBetMarketResolver(_sx_config(), scan_all=True)
        resolver._fetch_markets = AsyncMock(return_value=[_payload()])  # type: ignore[method-assign]

        resolved = await resolver.resolve([])

        self.assertEqual(len(resolved), 2)
        self.assertEqual({market.predict_fun_token_id for market in resolved}, {"0xmarket:YES", "0xmarket:NO"})
        self.assertEqual({market.polymarket_side for market in resolved}, {BinarySide.YES, BinarySide.NO})
        self.assertEqual({market.symbol for market in resolved}, {"Will Arsenal beat Chelsea?"})
        self.assertEqual({market.target_label for market in resolved}, {"Arsenal", "Chelsea"})
        self.assertTrue(all(market.venue_b_label == "SX Bet" for market in resolved))

    async def test_scan_all_outright_market_uses_question_like_symbol(self) -> None:
        resolver = SxBetMarketResolver(_sx_config(), scan_all=True)
        resolver._fetch_markets = AsyncMock(return_value=[_live_payload()])  # type: ignore[method-assign]

        resolved = await resolver.resolve([])

        self.assertEqual(len(resolved), 2)
        self.assertEqual({market.symbol for market in resolved}, {"Will France win the World Cup?"})
        self.assertEqual({market.target_label for market in resolved}, {"France", "The Field"})
        self.assertEqual({market.polymarket_side for market in resolved}, {BinarySide.YES, BinarySide.NO})

    async def test_fetch_market_page_retries_canonical_cursor_request_after_timeout(self) -> None:
        calls: list[tuple[dict[str, int | str], int]] = []

        class FakeResponse:
            def __init__(self, payload: dict[str, object]) -> None:
                self._payload = payload

            async def __aenter__(self) -> "FakeResponse":
                return self

            async def __aexit__(
                self,
                exc_type: type[BaseException] | None,
                exc: BaseException | None,
                tb: TracebackType | None,
            ) -> bool:
                return False

            def raise_for_status(self) -> None:
                return None

            async def json(self) -> dict[str, object]:
                return self._payload

        class FakeSession:
            def get(self, url: str, *, params: dict[str, int | str], timeout: int) -> FakeResponse:
                del url
                calls.append((params, timeout))
                if len(calls) == 1:
                    raise TimeoutError
                return FakeResponse({"data": {"markets": [_payload()]}})

        with patch("arbitrage_engine.sx_bet_discovery.asyncio.sleep", new=AsyncMock()) as sleep:
            payload = await _fetch_market_page(
                FakeSession(),
                "https://api.sx.bet/markets/active",
                pagination_key=None,
                page=1,
            )

        self.assertEqual(calls[0], ({"pageSize": 100}, 30))
        self.assertEqual(calls[1], ({"pageSize": 100}, 30))
        sleep.assert_awaited_once_with(0.5)
        self.assertIn("data", payload)

    async def test_catalog_pagination_follows_cursor_even_after_empty_page(self) -> None:
        resolver = SxBetMarketResolver(_sx_config(), scan_all=True)
        resolver._get_session = lambda: object()  # type: ignore[method-assign]
        pages = AsyncMock(
            side_effect=[
                {"data": {"markets": [_payload()], "nextKey": "cursor-2"}},
                {"data": {"markets": [], "nextKey": "cursor-3"}},
                {"data": {"markets": []}},
            ]
        )

        with patch("arbitrage_engine.sx_bet_discovery._fetch_market_page", new=pages):
            markets = await resolver._fetch_markets()  # noqa: SLF001

        self.assertEqual(markets, [_payload()])
        self.assertEqual(
            [call.kwargs["pagination_key"] for call in pages.await_args_list],
            [None, "cursor-2", "cursor-3"],
        )

    async def test_catalog_fails_closed_when_next_page_has_no_cursor(self) -> None:
        resolver = SxBetMarketResolver(_sx_config(), scan_all=True)
        resolver._get_session = lambda: object()  # type: ignore[method-assign]
        pages = AsyncMock(
            return_value={
                "data": {"markets": [_payload()]},
                "pagination": {"hasNext": True, "nextPage": 2},
            }
        )

        with (
            patch("arbitrage_engine.sx_bet_discovery._fetch_market_page", new=pages),
            self.assertRaisesRegex(RuntimeError, "without the required nextKey cursor"),
        ):
            await resolver._fetch_markets()  # noqa: SLF001

        pages.assert_awaited_once()

    async def test_scan_all_uses_recent_last_good_catalog_after_transient_failure(self) -> None:
        resolver = SxBetMarketResolver(_sx_config(), scan_all=True)
        resolver._last_good_market_payloads = (_payload(),)  # noqa: SLF001
        resolver._last_good_market_payloads_at = 100.0  # noqa: SLF001
        resolver._fetch_markets = AsyncMock(side_effect=RuntimeError("transient 403"))  # type: ignore[method-assign]

        with patch("arbitrage_engine.sx_bet_discovery.time.monotonic", return_value=200.0):
            resolved = await resolver.resolve([])

        self.assertEqual(len(resolved), 2)
        self.assertEqual({market.predict_fun_token_id for market in resolved}, {"0xmarket:YES", "0xmarket:NO"})

    async def test_scan_all_rejects_expired_last_good_catalog(self) -> None:
        resolver = SxBetMarketResolver(_sx_config(), scan_all=True)
        resolver._last_good_market_payloads = (_payload(),)  # noqa: SLF001
        resolver._last_good_market_payloads_at = 100.0  # noqa: SLF001
        resolver._fetch_markets = AsyncMock(side_effect=RuntimeError("persistent 403"))  # type: ignore[method-assign]

        with patch("arbitrage_engine.sx_bet_discovery.time.monotonic", return_value=1_001.0):
            with self.assertRaisesRegex(RuntimeError, "SX Bet discovery failed"):
                await resolver.resolve([])


if __name__ == "__main__":
    unittest.main()
