import asyncio
import time
import unittest
from dataclasses import replace
from typing import cast
from unittest.mock import AsyncMock, MagicMock, patch

from arbitrage_engine.config import MyriadMarketsConfig
from arbitrage_engine.connectors.base import OrderBookUnavailableException
from arbitrage_engine.connectors.myriad import (
    MyriadClient,
    _apply_orderbook_changes,
    _normalize_order_amount,
    _order_book_from_payload,
    _orderbook_query_params,
    _to_units,
)
from arbitrage_engine.models import BinarySide, MarketDataStatus, OrderBook, OrderBookLevel
from arbitrage_engine.myriad_discovery import _has_next_page


class MyriadTests(unittest.TestCase):
    def test_discovery_supports_total_pages_pagination(self) -> None:
        self.assertTrue(_has_next_page({"pagination": {"totalPages": 3}}, 1))
        self.assertFalse(_has_next_page({"pagination": {"totalPages": 3}}, 3))

    def test_to_units_uses_expected_decimals(self) -> None:
        self.assertEqual(_to_units(1.0, 6), 1_000_000)
        self.assertEqual(_to_units(0.4, 18), 400_000_000_000_000_000)

    def test_normalize_order_amount_supports_wei_and_human_units(self) -> None:
        self.assertEqual(_normalize_order_amount(40.0, 100.0), 40.0)
        self.assertEqual(_normalize_order_amount(40 * 10**18, 100.0), 40.0)

    def test_orderbook_query_includes_network_outcome_and_clob_model(self) -> None:
        self.assertEqual(
            _orderbook_query_params(56, 1),
            {"network_id": 56, "outcome": 1, "trading_model": "ob"},
        )

    def test_websocket_delta_updates_local_orderbook(self) -> None:
        book = OrderBook(
            bids=[OrderBookLevel(0.40, 10)],
            asks=[OrderBookLevel(0.42, 10)],
        )

        updated = _apply_orderbook_changes(
            book,
            [
                {"outcome": 0, "side": "BUY", "price": "0.41", "size": "5"},
                {"outcome": 0, "side": "SELL", "price": "0.42", "size": "0"},
            ],
            BinarySide.YES,
        )

        self.assertEqual(updated.best_bid.price, 0.41)
        self.assertEqual(updated.asks, [])

    def test_websocket_payload_cannot_cross_market_cache_boundary(self) -> None:
        client = MyriadClient(_config())
        token_id = "553:NO"
        channel = "orderbook:56:553"
        original = OrderBook(bids=[OrderBookLevel(0.23, 10)], asks=[OrderBookLevel(0.24, 10)])
        client._channel_tokens[channel] = {token_id}
        client._books[token_id] = original

        client._handle_ws_payload(
            {
                "push": {
                    "channel": channel,
                    "pub": {
                        "data": {
                            "networkId": 56,
                            "marketId": 999,
                            "changes": [
                                {"outcome": 1, "side": "ask", "price": "0.99", "amount": "1000000000000000000"}
                            ],
                        }
                    },
                }
            }
        )

        self.assertIs(client._books[token_id], original)

    def test_websocket_delta_updates_only_matching_market_and_outcome(self) -> None:
        client = MyriadClient(_config())
        token_id = "553:NO"
        channel = "orderbook:56:553"
        client._channel_tokens[channel] = {token_id}
        client._books[token_id] = OrderBook(bids=[OrderBookLevel(0.23, 10)], asks=[OrderBookLevel(0.24, 10)])

        client._handle_ws_payload(
            {
                "push": {
                    "channel": channel,
                    "pub": {
                        "data": {
                            "networkId": 56,
                            "marketId": 553,
                            "changes": [
                                {"outcome": 0, "side": "ask", "price": "0.01", "amount": "1000000000000000000"},
                                {
                                    "outcome": 1,
                                    "side": "ask",
                                    "price": "240000000000000000",
                                    "amount": "2000000000000000000",
                                },
                            ],
                        }
                    },
                }
            }
        )

        self.assertEqual(client._books[token_id].best_ask, OrderBookLevel(0.24, 2.0))

    def test_sign_order_builds_eip712_payload(self) -> None:
        client = MyriadClient(_config())

        signed = asyncio.run(client.sign_order(market_id=123, outcome_id=1, side=0, contracts=10, price=0.4))

        self.assertEqual(signed.order["marketId"], "123")
        self.assertEqual(signed.order["outcomeId"], 1)
        self.assertEqual(signed.order["amount"], str(10 * 10**18))
        self.assertEqual(signed.order["price"], "400000000000000000")
        self.assertTrue(signed.signature.startswith("0x") or len(signed.signature) >= 128)

    def test_sign_order_uses_unique_nonce_under_concurrency(self) -> None:
        async def run() -> list[str]:
            client = MyriadClient(_config())
            signed = await asyncio.gather(
                *[client.sign_order(market_id=123, outcome_id=1, side=0, contracts=1, price=0.4) for _ in range(10)]
            )
            return [str(item.order["nonce"]) for item in signed]

        nonces = asyncio.run(run())

        self.assertEqual(len(set(nonces)), 10)

    def test_order_book_uses_requested_outcome_book(self) -> None:
        payload = {
            "orderbook": {
                "YES": {"bids": [{"price": "0.40", "size": "10"}], "asks": [{"price": "0.41", "size": "12"}]},
                "NO": {"bids": [{"price": "0.58", "size": "20"}], "asks": [{"price": "0.59", "size": "22"}]},
            }
        }

        book = _order_book_from_payload(payload, BinarySide.NO)

        self.assertEqual(book.best_bid.price, 0.58)
        self.assertEqual(book.best_ask.price, 0.59)

    def test_order_book_normalizes_api_integer_scales(self) -> None:
        book = _order_book_from_payload(
            {
                "bids": [["500000000000000000", "3000000000000000000"]],
                "asks": [["510000000000000000", "2000000000000000000"]],
            }
        )

        self.assertEqual(book.best_bid, OrderBookLevel(0.5, 3.0))
        self.assertEqual(book.best_ask, OrderBookLevel(0.51, 2.0))

    def test_sign_order_quantizes_off_tick_price_down(self) -> None:
        client = MyriadClient(_config())

        signed = asyncio.run(client.sign_order(market_id=123, outcome_id=1, side=0, contracts=10, price=0.405))

        self.assertEqual(signed.order["price"], "400000000000000000")

    def test_orderbook_rest_requests_share_one_client_session(self) -> None:
        client = MyriadClient(_config())
        session = MagicMock()
        session.closed = False

        with patch("arbitrage_engine.connectors.myriad.client_session", return_value=session) as factory:
            first = client._get_rest_session()
            second = client._get_rest_session()

        self.assertIs(first, session)
        self.assertIs(second, session)
        factory.assert_called_once()

    def test_websocket_session_is_reused_without_rest_headers(self) -> None:
        client = MyriadClient(_config())
        session = MagicMock()
        session.closed = False

        with patch("arbitrage_engine.connectors.myriad.client_session", return_value=session) as factory:
            first = client._get_ws_session()
            second = client._get_ws_session()

        self.assertIs(first, session)
        self.assertIs(second, session)
        factory.assert_called_once_with()

    def test_stream_health_tracks_latest_venue_event_and_requires_all_tokens(self) -> None:
        client = MyriadClient(_config())
        client._channel_tokens["orderbook:56:1"] = {"1:YES", "1:NO"}
        client._ws_connected = True
        client._books["1:YES"] = OrderBook([], [])
        client._book_timestamps["1:YES"] = time.monotonic() - 0.1

        self.assertFalse(client.market_data_ready())

        client._books["1:NO"] = OrderBook([], [])
        client._book_timestamps["1:NO"] = time.monotonic() - 30
        self.assertTrue(client.market_data_ready())
        self.assertLess(client.market_data_age_seconds() or 1.0, 0.5)


class MyriadHttpTests(unittest.IsolatedAsyncioTestCase):
    async def test_stale_book_is_rebootstrapped_after_reconnect(self) -> None:
        client = MyriadClient(_config())
        token_id = "553:NO"
        client._books[token_id] = OrderBook([], [], status=MarketDataStatus.STALE)
        client._bootstrap_order_book = AsyncMock(return_value=OrderBook([], []))  # type: ignore[method-assign]

        await client.watch_order_book(token_id)

        client._bootstrap_order_book.assert_awaited_once_with(token_id, 553, BinarySide.NO, force=True)

    async def test_rest_timeout_is_classified_as_orderbook_unavailable(self) -> None:
        client = MyriadClient(_config())
        response_context = MagicMock()
        response_context.__aenter__ = AsyncMock(side_effect=TimeoutError("synthetic timeout"))
        response_context.__aexit__ = AsyncMock(return_value=False)
        session = MagicMock()
        session.closed = False
        session.get.return_value = response_context

        with (
            patch("arbitrage_engine.connectors.myriad.client_session", return_value=session),
            self.assertRaisesRegex(OrderBookUnavailableException, "unavailable"),
        ):
            await client.get_orderbook(553, 1)

    async def test_configured_ttl_does_not_reject_execution_fresh_quiet_book(self) -> None:
        client = MyriadClient(replace(_config(), order_book_ttl_ms=10, websocket_stale_after_ms=20))
        client._ensure_ws_task = MagicMock()  # type: ignore[method-assign]
        token_id = "553:NO"
        expected = OrderBook(
            bids=[OrderBookLevel(0.23, 1.0)],
            asks=[OrderBookLevel(0.24, 1.0)],
            timestamp=time.time() - 0.03,
        )
        client._books[token_id] = expected
        client._book_timestamps[token_id] = time.monotonic() - 0.03
        client._book_events[token_id] = asyncio.Event()

        book = await client.watch_order_book(token_id)

        self.assertIs(book, expected)

    async def test_passively_fresh_cached_book_is_reused_after_ttl(self) -> None:
        client = MyriadClient(replace(_config(), order_book_ttl_ms=10, websocket_stale_after_ms=1500))
        client._ensure_ws_task = MagicMock()  # type: ignore[method-assign]
        token_id = "553:NO"
        expected = OrderBook(
            bids=[OrderBookLevel(0.23, 1.0)],
            asks=[OrderBookLevel(0.24, 1.0)],
            timestamp=time.time() - 0.5,
        )
        client._books[token_id] = expected
        client._book_timestamps[token_id] = time.monotonic() - 0.5
        client._book_events[token_id] = asyncio.Event()

        book = await client.watch_order_book(token_id)

        self.assertIs(book, expected)

    async def test_close_releases_rest_and_websocket_sessions(self) -> None:
        client = MyriadClient(_config())
        rest_session = MagicMock()
        rest_session.closed = False
        rest_session.close = AsyncMock()
        ws_session = MagicMock()
        ws_session.closed = False
        ws_session.close = AsyncMock()
        client._rest_session = rest_session
        client._ws_session = ws_session

        await client.close()

        rest_session.close.assert_awaited_once()
        ws_session.close.assert_awaited_once()
        self.assertIsNone(client._rest_session)
        self.assertIsNone(client._ws_session)

    async def test_list_fills_tolerates_missing_trades_endpoint(self) -> None:
        client = MyriadClient(_config())
        not_found = RuntimeError("404 missing")
        not_found.status = 404  # type: ignore[attr-defined]

        with patch.object(client, "_request_json", AsyncMock(side_effect=not_found)):
            fills = await client.list_fills()

        self.assertEqual(fills, [])

    async def test_get_positions_tolerates_missing_trades_endpoint(self) -> None:
        client = MyriadClient(_config())
        not_found = RuntimeError("404 missing")
        not_found.status = 404  # type: ignore[attr-defined]

        with patch.object(client, "_request_json", AsyncMock(side_effect=not_found)):
            positions = await client.get_positions()

        self.assertEqual(positions, {})

    async def test_sync_market_data_targets_prunes_stale_history_and_restores_readiness(self) -> None:
        client = MyriadClient(_config())
        stale_task = asyncio.create_task(asyncio.sleep(60))
        client._channel_tokens["orderbook:56:1"] = {"1:YES"}
        client._channel_tokens["orderbook:56:2"] = {"2:NO"}
        client._desired_channels.update({"orderbook:56:1", "orderbook:56:2"})
        client._bootstrap_tasks["2:NO"] = cast(asyncio.Task[OrderBook], stale_task)
        client._ws_connected = True
        client._books["1:YES"] = OrderBook([], [])
        client._books["2:NO"] = OrderBook([], [], status=MarketDataStatus.STALE)
        client._book_timestamps["1:YES"] = time.monotonic() - 0.1
        client._book_timestamps["2:NO"] = time.monotonic() - 30

        client.sync_market_data_targets({"1:YES"})
        await asyncio.gather(stale_task, return_exceptions=True)

        self.assertEqual(client._desired_channels, {"orderbook:56:1"})
        self.assertEqual(client._channel_tokens, {"orderbook:56:1": {"1:YES"}})
        self.assertNotIn("2:NO", client._books)
        self.assertTrue(stale_task.cancelled())
        self.assertTrue(client.market_data_ready())

    async def test_sync_market_data_targets_bootstraps_added_tokens(self) -> None:
        client = BootstrapTrackingClient(_config())
        client._ws_connected = True

        client.sync_market_data_targets({"553:NO", "554:YES"})
        tasks = list(client._bootstrap_tasks.values())
        await asyncio.gather(*tasks)

        self.assertEqual(client.calls, 2)
        self.assertEqual(set(client._books), {"553:NO", "554:YES"})
        self.assertTrue(client.market_data_ready())

    async def test_reconnect_failure_recycles_ws_session(self) -> None:
        client = MyriadClient(_config())
        session = MagicMock()
        session.closed = False
        session.close = AsyncMock()
        session.ws_connect.side_effect = RuntimeError("boom")
        client._ws_session = session

        with patch.object(client, "_get_ws_session", return_value=session):
            task = asyncio.create_task(client._run_orderbook_ws())
            for _ in range(20):
                if session.close.await_count:
                    break
                await asyncio.sleep(0.01)
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

        session.close.assert_awaited()
        self.assertIsNone(client._ws_session)

    async def test_stale_cached_book_uses_rest_refresh_fallback(self) -> None:
        client = BootstrapTrackingClient(_config())
        expected = OrderBook(
            bids=[OrderBookLevel(0.23, 1)],
            asks=[OrderBookLevel(0.24, 1)],
            timestamp=time.time() - 60,
        )
        client._books["553:NO"] = expected
        client._book_timestamps["553:NO"] = 0.0

        book = await client.watch_order_book("553:NO")

        self.assertEqual(book.best_bid.price, 0.23)
        self.assertEqual(client.calls, 1)

    async def test_failed_stale_refresh_is_cooldown_bounded(self) -> None:
        client = FailingBootstrapTrackingClient(_config())
        client._books["553:NO"] = OrderBook(
            bids=[OrderBookLevel(0.23, 1)],
            asks=[OrderBookLevel(0.24, 1)],
            timestamp=time.time() - 60,
        )
        client._book_timestamps["553:NO"] = 0.0

        with self.assertRaisesRegex(RuntimeError, "boom"):
            await client.watch_order_book("553:NO")
        with self.assertRaisesRegex(RuntimeError, "cooling down"):
            await client.watch_order_book("553:NO")

        self.assertEqual(client.calls, 1)

    async def test_bootstrap_snapshots_are_limited_to_five_concurrent_requests(self) -> None:
        client = BootstrapTrackingClient(_config())

        books = await asyncio.gather(*(client.watch_order_book(f"{market_id}:YES") for market_id in range(100, 112)))

        self.assertEqual(len(books), 12)
        self.assertEqual(client.calls, 12)
        self.assertLessEqual(client.max_active, 5)

    async def test_concurrent_watchers_share_one_bootstrap_request(self) -> None:
        client = BootstrapTrackingClient(_config())

        books = await asyncio.gather(*(client.watch_order_book("553:NO") for _ in range(10)))

        self.assertEqual(client.calls, 1)
        self.assertTrue(all(book is books[0] for book in books))

    async def test_place_uses_fak_and_cancel_sends_original_signature(self) -> None:
        client = MyriadClient(_config())
        signed = await client.sign_order(market_id=123, outcome_id=0, side=0, contracts=1, price=0.4)
        response = MagicMock()
        response.json = AsyncMock(return_value={"orderHash": "0xorder", "status": "open"})
        response.raise_for_status.return_value = None
        response_context = MagicMock()
        response_context.__aenter__.return_value = response
        response_context.__aexit__.return_value = False
        session = MagicMock()
        session.closed = False
        session.post.return_value = response_context
        session.delete.return_value = response_context

        with patch("arbitrage_engine.connectors.myriad.client_session", return_value=session):
            order_id = await client.place_order(signed)
            await client.cancel_order(order_id)

        place_payload = session.post.call_args.kwargs["json"]
        self.assertEqual(place_payload["time_in_force"], "FAK")
        cancel_payload = session.delete.call_args.kwargs["json"]
        self.assertEqual(cancel_payload["order"], signed.order)
        self.assertEqual(cancel_payload["signature"], signed.signature)


class BootstrapTrackingClient(MyriadClient):
    def __init__(self, config: MyriadMarketsConfig) -> None:
        super().__init__(config)
        self.calls = 0
        self.active = 0
        self.max_active = 0

    def _ensure_ws_task(self) -> None:
        return

    async def get_orderbook(self, market_id: int, outcome_id: int) -> dict[str, object]:
        self.calls += 1
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        try:
            await asyncio.sleep(0.01)
            return {
                "marketId": market_id,
                "outcomeId": outcome_id,
                "bids": [["230000000000000000", "1000000000000000000"]],
                "asks": [["240000000000000000", "1000000000000000000"]],
            }
        finally:
            self.active -= 1


class FailingBootstrapTrackingClient(BootstrapTrackingClient):
    async def get_orderbook(self, market_id: int, outcome_id: int) -> dict[str, object]:
        self.calls += 1
        raise RuntimeError("boom")


def _config() -> MyriadMarketsConfig:
    return MyriadMarketsConfig(
        api_url="https://api-v2.myriadprotocol.com",
        ws_url="wss://ws.myriadprotocol.com/ws",
        api_key="key",
        private_key="0x" + "1" * 64,
        rpc_url="https://bsc-dataseed.binance.org",
        rpc_urls=["https://bsc-dataseed.binance.org"],
        chain_id=56,
        exchange_address="0xa0b6f8ef8EdB64f395018D1933f2273Ce9f0f16A",
        conditional_tokens_address="0x6413734f92248D4B29ae35883290BD93212654Dc",
        collateral_tokens={
            "USD1": "0x8d0D000Ee44948FC98c9B98A4FA4921476f08B0d",
            "USDT": "0x55d398326f99059fF775485246999027B3197955",
        },
        collateral_symbol="USDT",
        trading_fee_pct=0.0,
        max_slippage_pct=0.015,
        enabled=True,
    )


if __name__ == "__main__":
    unittest.main()
