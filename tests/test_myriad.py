import asyncio
import time
import unittest
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from types import TracebackType
from typing import cast
from unittest.mock import AsyncMock, MagicMock, patch

from arbitrage_engine.config import MyriadMarketsConfig
from arbitrage_engine.connectors.base import OrderBookUnavailableException, OrderSubmissionRejected
from arbitrage_engine.connectors.myriad import (
    FUNDED_ORDER_BOOK_REFRESH_CONCURRENCY,
    FUNDED_REFRESH_DEADLINE_MARGIN_FRACTION,
    FUNDED_REFRESH_HEDGE_CONCURRENCY,
    FUNDED_REFRESH_HEDGE_DELAY_FRACTION,
    FUNDED_REFRESH_START_INTERVAL_SECONDS,
    FUNDED_REFRESH_STEADY_TRIGGER_FRACTION,
    ORDER_BOOK_BOOTSTRAP_CONCURRENCY,
    ORDER_BOOK_REQUEST_CONCURRENCY,
    MyriadClient,
    _apply_orderbook_changes,
    _loop_deadline_from_monotonic,
    _myriad_claim_transaction,
    _myriad_peak_fee_bps,
    _myriad_settlement_status,
    _normalize_order_amount,
    _order_book_from_payload,
    _orderbook_query_params,
    _proactive_refresh_timeout_seconds,
    _to_units,
)
from arbitrage_engine.models import (
    BinarySide,
    MarketDataStatus,
    OrderBook,
    OrderBookLevel,
    RedemptionIntentStatus,
    SettlementRequest,
    SettlementStatus,
)
from arbitrage_engine.myriad_discovery import _has_next_page


class MyriadTests(unittest.TestCase):
    def test_monotonic_deadline_is_translated_to_event_loop_clock(self) -> None:
        loop = MagicMock()
        loop.time.return_value = 500.0
        with (
            patch(
                "arbitrage_engine.connectors.myriad.asyncio.get_running_loop",
                return_value=loop,
            ),
            patch(
                "arbitrage_engine.connectors.myriad.time.monotonic",
                return_value=100.0,
            ),
        ):
            converted = _loop_deadline_from_monotonic(102.5)

        self.assertEqual(converted, 502.5)

    def test_discovery_supports_total_pages_pagination(self) -> None:
        self.assertTrue(_has_next_page({"pagination": {"totalPages": 3}}, 1))
        self.assertFalse(_has_next_page({"pagination": {"totalPages": 3}}, 3))

    def test_to_units_uses_expected_decimals(self) -> None:
        self.assertEqual(_to_units(1.0, 6), 1_000_000)
        self.assertEqual(_to_units(0.4, 18), 400_000_000_000_000_000)

    def test_normalize_order_amount_supports_wei_and_human_units(self) -> None:
        self.assertEqual(_normalize_order_amount(40.0, 100.0), 40.0)
        self.assertEqual(_normalize_order_amount(40 * 10**18, 100.0), 40.0)

    def test_peak_fee_supports_live_order_book_scalar_and_array_schema(self) -> None:
        self.assertEqual(_myriad_peak_fee_bps({"fees": {"taker_fee_bps": 150}}), 150)
        self.assertEqual(
            _myriad_peak_fee_bps({"fees": {"taker_fee_bps_array": [0, 30, 75, 150, 75, 30, 0]}}),
            150,
        )

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
    async def test_market_constraints_use_paginated_order_book_fee_metadata_and_cache(self) -> None:
        client = MyriadClient(_config())
        request = AsyncMock(
            return_value={
                "data": [
                    {
                        "id": 397,
                        "tradingModel": "ob",
                        "fees": {"taker_fee_bps": 150},
                    },
                    {
                        "id": 398,
                        "tradingModel": "ob",
                        "fees": {"taker_fee_bps": 125},
                    },
                ],
                "pagination": {"hasNext": False},
            }
        )

        with patch.object(client, "_request_json", request):
            first = await client.get_market_constraints("397:YES")
            second = await client.get_market_constraints("397:NO")
            third = await client.get_market_constraints("398:YES")

        assert first is not None
        assert third is not None
        self.assertEqual(first.fee_rate_bps, 150)
        self.assertEqual(third.fee_rate_bps, 125)
        self.assertIs(first, second)
        request.assert_awaited_once_with(
            "GET",
            "/markets",
            query_params={
                "network_id": "56",
                "trading_model": "ob",
                "state": "open",
                "page": "1",
                "limit": "100",
            },
        )

    async def test_market_constraints_follow_fee_catalog_pagination(self) -> None:
        client = MyriadClient(_config())
        request = AsyncMock(
            side_effect=[
                {
                    "data": [{"id": 397, "fees": {"taker_fee_bps": 150}}],
                    "pagination": {"hasNext": True},
                },
                {
                    "data": [{"id": 450, "fees": {"taker_fee_bps": 75}}],
                    "pagination": {"hasNext": False},
                },
            ]
        )

        with patch.object(client, "_request_json", request):
            constraints = await client.get_market_constraints("450:YES")

        assert constraints is not None
        self.assertEqual(constraints.fee_rate_bps, 75)
        self.assertEqual(request.await_count, 2)
        self.assertEqual(request.await_args_list[1].kwargs["query_params"]["page"], "2")

    async def test_market_constraints_fail_closed_when_market_is_missing_from_fee_catalog(self) -> None:
        client = MyriadClient(_config())
        request = AsyncMock(return_value={"data": [], "pagination": {"hasNext": False}})

        with patch.object(client, "_request_json", request):
            with self.assertRaisesRegex(RuntimeError, "fee metadata is unavailable for market 999"):
                await client.get_market_constraints("999:YES")

    async def test_request_json_retries_with_fresh_session_after_timeout(self) -> None:
        client = MyriadClient(_config())

        class _FailingRequest:
            async def __aenter__(self) -> None:
                raise TimeoutError("timed out")

            async def __aexit__(
                self,
                exc_type: type[BaseException] | None,
                exc: BaseException | None,
                tb: TracebackType | None,
            ) -> bool:
                return False

        class _Response:
            def raise_for_status(self) -> None:
                return None

            async def json(self) -> dict[str, object]:
                return {"data": []}

        class _SuccessfulRequest:
            async def __aenter__(self) -> _Response:
                return _Response()

            async def __aexit__(
                self,
                exc_type: type[BaseException] | None,
                exc: BaseException | None,
                tb: TracebackType | None,
            ) -> bool:
                return False

        first_session = MagicMock()
        first_session.closed = False
        first_session.close = AsyncMock()
        first_session.request.return_value = _FailingRequest()

        second_session = MagicMock()
        second_session.closed = False
        second_session.close = AsyncMock()
        second_session.request.return_value = _SuccessfulRequest()

        client._rest_session = first_session

        def _next_session() -> object:
            if client._rest_session is None:
                client._rest_session = second_session
            return client._rest_session

        with patch.object(client, "_get_rest_session", side_effect=_next_session):
            payload = await client._request_json("GET", "/orders", query_params={"status": "open"})

        self.assertEqual(payload, {"data": []})
        first_session.close.assert_awaited_once()
        second_session.request.assert_called_once()

    async def test_redeem_uses_market_id_api_calldata_and_signs_locally(self) -> None:
        client = MyriadClient(_config())
        web3_client = MagicMock()
        web3_client.account.address = "0x" + "2" * 40
        web3_client.send_transaction = AsyncMock(return_value="0xtx")
        request = SettlementRequest(
            position_key="position",
            venue="Myriad",
            market_id="123",
            condition_id="123",
            collateral_token="",
            expected_contracts=Decimal("1"),
        )

        with (
            patch.object(client, "_get_web3_client", return_value=web3_client),
            patch.object(
                client,
                "_request_json",
                AsyncMock(return_value={"to": "0x" + "1" * 40, "calldata": "0x1234", "value": "0"}),
            ) as request_json,
        ):
            report = await client.redeem_position(request, "redemption")

        self.assertEqual(report.status, RedemptionIntentStatus.SUBMITTED)
        self.assertEqual(report.tx_hash, "0xtx")
        request_json.assert_awaited_once_with(
            "POST",
            "/positions/redeem",
            json_body={"market_id": 123, "network_id": 56},
        )
        self.assertEqual(web3_client.send_transaction.await_args.args[0]["to"], "0x" + "1" * 40)

    async def test_reconcile_redemption_requires_portfolio_to_be_claimed(self) -> None:
        client = MyriadClient(_config())
        web3_client = MagicMock()
        web3_client.transaction_status = AsyncMock(return_value=True)
        request = SettlementRequest(
            position_key="position",
            venue="Myriad",
            market_id="123",
            condition_id="123",
            collateral_token="",
            expected_contracts=Decimal("1"),
        )

        with (
            patch.object(client, "_get_web3_client", return_value=web3_client),
            patch.object(client, "_has_redeemable_position", AsyncMock(return_value=True)),
        ):
            report = await client.reconcile_redemption(
                request,
                type("Report", (), {"tx_hash": "0xtx", "error": None})(),
            )

        self.assertEqual(report.status, RedemptionIntentStatus.UNKNOWN)
        self.assertIn("winnings to claim", report.error or "")

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

    def test_orderbook_settlement_status_uses_current_market_state_not_condition_id(self) -> None:
        self.assertEqual(_myriad_settlement_status({"status": "open"}), SettlementStatus.OPEN)
        self.assertEqual(_myriad_settlement_status({"status": "resolved"}), SettlementStatus.RESOLVED)
        self.assertEqual(_myriad_settlement_status({"state": "voided"}), SettlementStatus.VOID)

    async def test_market_detail_is_scoped_to_orderbook_trading_model(self) -> None:
        client = MyriadClient(_config())

        with patch.object(client, "_request_json", AsyncMock(return_value={"data": {"id": 1731}})) as request_json:
            payload = await client._market_payload("1731")

        self.assertEqual(payload, {"id": 1731})
        request_json.assert_awaited_once_with(
            "GET",
            "/markets/1731",
            query_params={"network_id": "56", "trading_model": "ob"},
        )

    def test_claim_transaction_validates_documented_api_calldata(self) -> None:
        transaction = _myriad_claim_transaction(
            {"data": {"to": "0x" + "1" * 40, "calldata": "0x1234", "value": "0x0"}},
            "0x" + "2" * 40,
            350_000,
        )
        self.assertEqual(transaction["to"], "0x" + "1" * 40)
        self.assertEqual(transaction["data"], "0x1234")
        self.assertEqual(transaction["value"], 0)
        self.assertEqual(transaction["gas"], 350_000)

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

    async def test_proactive_refresh_confirms_active_quiet_book_before_stale(self) -> None:
        client = MyriadClient(replace(_config(), order_book_ttl_ms=300, websocket_stale_after_ms=1500))
        client.set_market_data_execution_freshness(2.0)
        token_id = "553:NO"
        client._ensure_token_subscription(token_id, 553)
        client._books[token_id] = OrderBook(  # noqa: SLF001
            bids=[OrderBookLevel(0.23, 1.0)],
            asks=[OrderBookLevel(0.24, 1.0)],
            timestamp=time.time() - 1.1,
        )
        client._book_timestamps[token_id] = time.monotonic() - 1.1  # noqa: SLF001
        client._stale_refresh_attempted_at[token_id] = time.monotonic() - 1.1  # noqa: SLF001
        get_orderbook = AsyncMock(
            return_value={
                "marketId": 553,
                "outcomeId": 1,
                "bids": [["230000000000000000", "1000000000000000000"]],
                "asks": [["240000000000000000", "1000000000000000000"]],
            }
        )

        with patch.object(client, "get_orderbook", get_orderbook):
            refreshed = await client.refresh_market_data_target(token_id)
            cooling_down = await client.refresh_market_data_target(token_id)
            client._stale_refresh_attempted_at[token_id] = time.monotonic() - 0.25  # noqa: SLF001
            client._book_timestamps[token_id] = time.monotonic() - 0.25  # noqa: SLF001
            refreshed_after_production_cooldown = await client.refresh_market_data_target(token_id)
            inactive = await client.refresh_market_data_target("554:NO")

        self.assertTrue(refreshed)
        self.assertFalse(cooling_down)
        self.assertTrue(refreshed_after_production_cooldown)
        self.assertFalse(inactive)
        self.assertEqual(get_orderbook.await_count, 2)
        get_orderbook.assert_awaited_with(553, 1)
        refreshed_age = client.market_data_target_age_seconds(token_id)
        self.assertIsNotNone(refreshed_age)
        self.assertLess(cast(float, refreshed_age), 0.2)
        self.assertEqual(client.telemetry_snapshot()["proactive_refreshes"], 2.0)
        self.assertEqual(client.telemetry_snapshot()["proactive_refresh_failures"], 0.0)

        client._stale_refresh_attempted_at[token_id] = time.monotonic() - 0.25  # noqa: SLF001
        failed_get = AsyncMock(side_effect=OrderBookUnavailableException("synthetic refresh failure"))
        with (
            patch.object(client, "get_orderbook", failed_get),
            self.assertRaises(OrderBookUnavailableException),
        ):
            await client.refresh_market_data_target(token_id)

        failed_get.assert_awaited_once()
        self.assertEqual(client.telemetry_snapshot()["proactive_refreshes"], 2.0)
        self.assertEqual(client.telemetry_snapshot()["proactive_refresh_failures"], 1.0)

    async def test_proactive_refresh_concurrency_is_bounded_for_production_window(self) -> None:
        self.assertEqual(FUNDED_ORDER_BOOK_REFRESH_CONCURRENCY, 12)
        self.assertEqual(FUNDED_REFRESH_HEDGE_CONCURRENCY, 2)
        client = MyriadClient(replace(_config(), order_book_ttl_ms=300, websocket_stale_after_ms=1500))
        client.set_market_data_execution_freshness(2.0)
        release_requests = asyncio.Event()
        concurrency_reached = asyncio.Event()
        current_requests = 0
        peak_requests = 0

        async def blocked_orderbook(market_id: int, outcome_id: int) -> dict[str, object]:
            nonlocal current_requests, peak_requests
            self.assertEqual(outcome_id, 1)
            current_requests += 1
            peak_requests = max(peak_requests, current_requests)
            if current_requests == FUNDED_ORDER_BOOK_REFRESH_CONCURRENCY:
                concurrency_reached.set()
            try:
                await release_requests.wait()
            finally:
                current_requests -= 1
            return {
                "marketId": market_id,
                "outcomeId": outcome_id,
                "bids": [["230000000000000000", "1000000000000000000"]],
                "asks": [["240000000000000000", "1000000000000000000"]],
            }

        token_ids = [f"{600 + index}:NO" for index in range(FUNDED_ORDER_BOOK_REFRESH_CONCURRENCY)]
        for token_id in token_ids:
            market_id = int(token_id.split(":", 1)[0])
            client._ensure_token_subscription(token_id, market_id)  # noqa: SLF001

        with patch.object(client, "get_orderbook", side_effect=blocked_orderbook):
            refreshes = [asyncio.create_task(client.refresh_market_data_target(token_id)) for token_id in token_ids]
            await asyncio.wait_for(concurrency_reached.wait(), timeout=2.0)
            await asyncio.sleep(0)
            self.assertEqual(peak_requests, FUNDED_ORDER_BOOK_REFRESH_CONCURRENCY)
            self.assertEqual(current_requests, FUNDED_ORDER_BOOK_REFRESH_CONCURRENCY)
            release_requests.set()
            self.assertTrue(all(await asyncio.gather(*refreshes)))

        self.assertEqual(peak_requests, FUNDED_ORDER_BOOK_REFRESH_CONCURRENCY)
        self.assertEqual(client.telemetry_snapshot()["proactive_refreshes"], float(len(token_ids)))
        self.assertEqual(client.telemetry_snapshot()["proactive_refresh_failures"], 0.0)
        self.assertEqual(
            client.telemetry_snapshot()["funded_refresh_requests"],
            float(len(token_ids)),
        )

    async def test_funded_refresh_request_starts_are_paced(self) -> None:
        client = MyriadClient(replace(_config(), order_book_ttl_ms=300, websocket_stale_after_ms=1500))
        client.set_market_data_execution_freshness(2.0)
        started_at: list[float] = []

        async def orderbook(market_id: int, outcome_id: int) -> dict[str, object]:
            self.assertEqual(outcome_id, 1)
            started_at.append(time.monotonic())
            return {
                "marketId": market_id,
                "outcomeId": outcome_id,
                "bids": [["230000000000000000", "1000000000000000000"]],
                "asks": [["240000000000000000", "1000000000000000000"]],
            }

        token_ids = [f"{600 + index}:NO" for index in range(FUNDED_ORDER_BOOK_REFRESH_CONCURRENCY)]
        for token_id in token_ids:
            client._ensure_token_subscription(  # noqa: SLF001
                token_id,
                int(token_id.split(":", 1)[0]),
            )

        with patch.object(client, "get_orderbook", side_effect=orderbook):
            await asyncio.gather(*(client.prime_funded_market_data_target(token_id) for token_id in token_ids))

        self.assertEqual(len(started_at), len(token_ids))
        self.assertTrue(
            all(
                later - earlier >= FUNDED_REFRESH_START_INTERVAL_SECONDS - 0.01
                for earlier, later in zip(started_at, started_at[1:], strict=False)
            )
        )
        self.assertTrue(all(client.market_data_target_ready(token_id, 2.0) for token_id in token_ids))
        self.assertEqual(
            client.telemetry_snapshot()["funded_refresh_requests"],
            float(len(token_ids)),
        )

    def test_paced_supported_batches_fit_hard_freshness_deadline(self) -> None:
        client = MyriadClient(_config())
        token_id = "553:NO"
        target_count = FUNDED_ORDER_BOOK_REFRESH_CONCURRENCY
        target_token_ids = tuple(f"{553 + index}:NO" for index in range(target_count))
        paced_span_seconds = (target_count - 1) * FUNDED_REFRESH_START_INTERVAL_SECONDS
        for max_age_seconds, expected_trigger in ((2.0, 0.3), (1.5, 0.075)):
            default_trigger_age_seconds = max_age_seconds * 17 / 40
            trigger_age_seconds = client.funded_market_data_refresh_trigger_age_seconds(
                token_id,
                max_age_seconds,
                default_trigger_age_seconds,
                0.05,
                target_token_ids,
            )
            request_timeout_seconds = _proactive_refresh_timeout_seconds(max_age_seconds)

            self.assertAlmostEqual(trigger_age_seconds, expected_trigger)
            self.assertAlmostEqual(request_timeout_seconds, max_age_seconds / 2)
            self.assertLessEqual(
                trigger_age_seconds + 0.05 + paced_span_seconds + request_timeout_seconds,
                max_age_seconds * (1 - FUNDED_REFRESH_DEADLINE_MARGIN_FRACTION) + 1e-9,
            )
        self.assertEqual(FUNDED_REFRESH_DEADLINE_MARGIN_FRACTION, 1 / 20)

    def test_global_schedule_handles_synchronized_and_compressed_receipts(self) -> None:
        client = MyriadClient(_config())
        max_age_seconds = 2.0
        poll_seconds = 0.05
        default_trigger_age_seconds = 0.85
        healthy_tail_seconds = 0.68
        target_token_ids = tuple(f"{553 + index}:NO" for index in range(FUNDED_ORDER_BOOK_REFRESH_CONCURRENCY))
        synchronized_receipt = 100.0
        client._book_timestamps.update(  # noqa: SLF001
            dict.fromkeys(target_token_ids, synchronized_receipt)
        )

        trigger_ages = [
            client.funded_market_data_refresh_trigger_age_seconds(
                token_id,
                max_age_seconds,
                default_trigger_age_seconds,
                poll_seconds,
                target_token_ids,
            )
            for token_id in target_token_ids
        ]

        self.assertEqual(len(set(trigger_ages)), len(target_token_ids))
        self.assertAlmostEqual(trigger_ages[0], 0.15)
        self.assertAlmostEqual(
            trigger_ages[-1],
            max_age_seconds * FUNDED_REFRESH_STEADY_TRIGGER_FRACTION,
        )
        worst_case_completions = [
            synchronized_receipt + trigger_age + poll_seconds + 1.0 for trigger_age in trigger_ages
        ]
        self.assertTrue(
            all(
                completion
                <= synchronized_receipt + max_age_seconds * (1 - FUNDED_REFRESH_DEADLINE_MARGIN_FRACTION) + 1e-9
                for completion in worst_case_completions
            )
        )

        # Adversarial 40 ms receipt spacing is globally compressed just enough
        # to retain the full HTTP budget without reverting every token to the
        # 0.30-second cold-batch trigger.
        client._book_timestamps.update(  # noqa: SLF001
            {token_id: synchronized_receipt + index * 0.04 for index, token_id in enumerate(target_token_ids)}
        )
        compressed_trigger_ages = [
            client.funded_market_data_refresh_trigger_age_seconds(
                token_id,
                max_age_seconds,
                default_trigger_age_seconds,
                poll_seconds,
                target_token_ids,
            )
            for token_id in target_token_ids
        ]
        compressed_starts = [
            client._book_timestamps[token_id] + trigger_age  # noqa: SLF001
            for token_id, trigger_age in zip(
                target_token_ids,
                compressed_trigger_ages,
                strict=True,
            )
        ]
        self.assertTrue(
            all(
                later - earlier >= FUNDED_REFRESH_START_INTERVAL_SECONDS - 1e-9
                for earlier, later in zip(
                    compressed_starts,
                    compressed_starts[1:],
                    strict=False,
                )
            )
        )
        self.assertGreaterEqual(min(compressed_trigger_ages), 0.59 - 1e-9)
        self.assertLessEqual(
            max(compressed_trigger_ages),
            max_age_seconds * FUNDED_REFRESH_STEADY_TRIGGER_FRACTION + 1e-9,
        )

        # Once a real paced healthy-tail batch separates receipts by 50 ms,
        # the next cycle stays at the hedge-aware trigger. Even the conservative
        # case where all twelve primaries reach the hedge delay uses no more
        # than the shared twenty-starts-per-second venue pacer.
        client._book_timestamps.update(  # noqa: SLF001
            {
                token_id: synchronized_receipt + index * FUNDED_REFRESH_START_INTERVAL_SECONDS
                for index, token_id in enumerate(target_token_ids)
            }
        )
        steady_trigger_ages = [
            client.funded_market_data_refresh_trigger_age_seconds(
                token_id,
                max_age_seconds,
                default_trigger_age_seconds,
                poll_seconds,
                target_token_ids,
            )
            for token_id in target_token_ids
        ]
        self.assertTrue(
            all(
                abs(trigger_age - max_age_seconds * FUNDED_REFRESH_STEADY_TRIGGER_FRACTION) <= 1e-9
                for trigger_age in steady_trigger_ages
            )
        )
        hedge_delay_seconds = max(
            0.05,
            max_age_seconds * FUNDED_REFRESH_HEDGE_DELAY_FRACTION,
        )
        self.assertGreater(healthy_tail_seconds, hedge_delay_seconds)
        fastest_hedged_cycle_seconds = steady_trigger_ages[0] + hedge_delay_seconds
        maximum_funded_request_start_rate = (
            len(target_token_ids) * 2 / fastest_hedged_cycle_seconds
        )
        self.assertLessEqual(
            maximum_funded_request_start_rate,
            1 / FUNDED_REFRESH_START_INTERVAL_SECONDS + 1e-9,
        )

    async def test_semaphore_backlog_cannot_start_after_bounded_recovery_deadline(
        self,
    ) -> None:
        client = MyriadClient(replace(_config(), order_book_ttl_ms=50, websocket_stale_after_ms=300))
        client.set_market_data_execution_freshness(0.3)
        token_id = "553:NO"
        client._ensure_token_subscription(token_id, 553)  # noqa: SLF001
        client._store_book(  # noqa: SLF001
            token_id,
            OrderBook(
                bids=[OrderBookLevel(0.23, 1.0)],
                asks=[OrderBookLevel(0.24, 1.0)],
            ),
        )
        client._book_timestamps[token_id] = time.monotonic() - 0.2  # noqa: SLF001
        client._stale_refresh_attempted_at[token_id] = 0.0  # noqa: SLF001
        client._funded_refresh_semaphore = asyncio.Semaphore(0)  # noqa: SLF001
        get_orderbook = AsyncMock()

        with patch.object(client, "get_orderbook", get_orderbook):
            self.assertFalse(await client.refresh_market_data_target(token_id))

        get_orderbook.assert_not_awaited()
        telemetry = client.telemetry_snapshot()
        self.assertEqual(telemetry["funded_refresh_requests"], 0.0)
        self.assertEqual(telemetry["funded_refresh_deadline_misses"], 1.0)
        self.assertEqual(telemetry["proactive_refresh_timeouts"], 1.0)

    async def test_queued_refresh_can_recover_after_old_book_becomes_stale(self) -> None:
        client = MyriadClient(replace(_config(), order_book_ttl_ms=50, websocket_stale_after_ms=500))
        client.set_market_data_execution_freshness(0.5)
        token_id = "553:NO"
        client._ensure_token_subscription(token_id, 553)  # noqa: SLF001
        client._store_book(  # noqa: SLF001
            token_id,
            OrderBook(
                bids=[OrderBookLevel(0.20, 1.0)],
                asks=[OrderBookLevel(0.30, 1.0)],
                timestamp=time.time() - 0.4,
            ),
        )
        client._book_timestamps[token_id] = time.monotonic() - 0.35  # noqa: SLF001
        client._stale_refresh_attempted_at[token_id] = 0.0  # noqa: SLF001
        client._funded_refresh_semaphore = asyncio.Semaphore(0)  # noqa: SLF001
        request_started = asyncio.Event()

        async def orderbook(market_id: int, outcome_id: int) -> dict[str, object]:
            self.assertEqual((market_id, outcome_id), (553, 1))
            request_started.set()
            return {
                "marketId": market_id,
                "outcomeId": outcome_id,
                "bids": [["230000000000000000", "1000000000000000000"]],
                "asks": [["240000000000000000", "1000000000000000000"]],
            }

        with patch.object(client, "get_orderbook", side_effect=orderbook):
            refresh = asyncio.create_task(client.refresh_market_data_target(token_id))
            await asyncio.sleep(0.15)
            self.assertFalse(client.market_data_target_ready(token_id, 0.5))
            self.assertFalse(request_started.is_set())
            client._funded_refresh_semaphore.release()  # noqa: SLF001
            self.assertTrue(await asyncio.wait_for(refresh, timeout=0.2))

        telemetry = client.telemetry_snapshot()
        self.assertEqual(telemetry["funded_refresh_requests"], 1.0)
        self.assertEqual(telemetry["funded_refresh_deadline_misses"], 0.0)
        self.assertEqual(telemetry["proactive_refresh_timeouts"], 0.0)
        self.assertTrue(client.market_data_target_ready(token_id, 0.5))

    async def test_request_started_before_freshness_deadline_can_finish_under_http_timeout(
        self,
    ) -> None:
        client = MyriadClient(replace(_config(), order_book_ttl_ms=50, websocket_stale_after_ms=1000))
        client.set_market_data_execution_freshness(1.0)
        token_id = "553:NO"
        client._ensure_token_subscription(token_id, 553)  # noqa: SLF001
        client._store_book(  # noqa: SLF001
            token_id,
            OrderBook(
                bids=[OrderBookLevel(0.20, 1.0)],
                asks=[OrderBookLevel(0.30, 1.0)],
                timestamp=time.time() - 0.9,
            ),
        )
        client._book_timestamps[token_id] = time.monotonic() - 0.9  # noqa: SLF001
        client._stale_refresh_attempted_at[token_id] = 0.0  # noqa: SLF001
        request_started = asyncio.Event()
        release_request = asyncio.Event()

        async def delayed_orderbook(market_id: int, outcome_id: int) -> dict[str, object]:
            self.assertEqual((market_id, outcome_id), (553, 1))
            request_started.set()
            await release_request.wait()
            return {
                "marketId": market_id,
                "outcomeId": outcome_id,
                "bids": [["230000000000000000", "1000000000000000000"]],
                "asks": [["240000000000000000", "1000000000000000000"]],
            }

        with patch.object(client, "get_orderbook", side_effect=delayed_orderbook):
            refresh = asyncio.create_task(client.refresh_market_data_target(token_id))
            await asyncio.wait_for(request_started.wait(), timeout=0.2)
            await asyncio.sleep(0.15)
            self.assertFalse(client.market_data_target_ready(token_id, 1.0))
            release_request.set()
            self.assertTrue(await asyncio.wait_for(refresh, timeout=0.3))

        telemetry = client.telemetry_snapshot()
        self.assertEqual(telemetry["funded_refresh_requests"], 1.0)
        self.assertEqual(telemetry["funded_refresh_deadline_misses"], 0.0)
        self.assertEqual(telemetry["proactive_refresh_timeouts"], 0.0)
        self.assertTrue(client.market_data_target_ready(token_id, 1.0))

    async def test_newer_book_while_pacing_skips_redundant_get(self) -> None:
        client = MyriadClient(replace(_config(), order_book_ttl_ms=50, websocket_stale_after_ms=300))
        client.set_market_data_execution_freshness(2.0)
        token_id = "553:NO"
        client._ensure_token_subscription(token_id, 553)  # noqa: SLF001
        client._store_book(  # noqa: SLF001
            token_id,
            OrderBook(
                bids=[OrderBookLevel(0.20, 1.0)],
                asks=[OrderBookLevel(0.30, 1.0)],
            ),
        )
        initial_receipt = client._book_timestamps[token_id]  # noqa: SLF001
        client._stale_refresh_attempted_at[token_id] = 0.0  # noqa: SLF001
        client._next_funded_refresh_start_at = time.monotonic() + 0.12  # noqa: SLF001
        get_orderbook = AsyncMock()

        with patch.object(client, "get_orderbook", get_orderbook):
            refresh = asyncio.create_task(client.refresh_market_data_target(token_id))
            for _ in range(30):
                if client._funded_refresh_start_lock.locked():  # noqa: SLF001
                    break
                await asyncio.sleep(0.005)
            self.assertTrue(client._funded_refresh_start_lock.locked())  # noqa: SLF001
            client._store_book(  # noqa: SLF001
                token_id,
                OrderBook(
                    bids=[OrderBookLevel(0.25, 1.0)],
                    asks=[OrderBookLevel(0.26, 1.0)],
                ),
            )
            # Preserve the receipt value to prove that captured book identity,
            # not only clock resolution, suppresses the obsolete REST request.
            client._book_timestamps[token_id] = initial_receipt  # noqa: SLF001
            self.assertFalse(await asyncio.wait_for(refresh, timeout=0.5))

        get_orderbook.assert_not_awaited()
        telemetry = client.telemetry_snapshot()
        self.assertEqual(telemetry["funded_refresh_requests"], 0.0)
        self.assertEqual(telemetry["proactive_refreshes"], 0.0)
        self.assertEqual(telemetry["proactive_refresh_no_receipt"], 1.0)

    async def test_already_stale_target_gets_one_bounded_recovery(self) -> None:
        client = MyriadClient(replace(_config(), order_book_ttl_ms=50, websocket_stale_after_ms=300))
        client.set_market_data_execution_freshness(0.3)
        token_id = "553:NO"
        client._ensure_token_subscription(token_id, 553)  # noqa: SLF001
        client._store_book(  # noqa: SLF001
            token_id,
            OrderBook(
                bids=[OrderBookLevel(0.20, 1.0)],
                asks=[OrderBookLevel(0.30, 1.0)],
                timestamp=time.time() - 1.0,
            ),
        )
        client._book_timestamps[token_id] = time.monotonic() - 1.0  # noqa: SLF001
        client._stale_refresh_attempted_at[token_id] = 0.0  # noqa: SLF001
        get_orderbook = AsyncMock(
            return_value={
                "marketId": 553,
                "outcomeId": 1,
                "bids": [["230000000000000000", "1000000000000000000"]],
                "asks": [["240000000000000000", "1000000000000000000"]],
            }
        )

        with patch.object(client, "get_orderbook", get_orderbook):
            self.assertTrue(await client.refresh_market_data_target(token_id))

        get_orderbook.assert_awaited_once_with(553, 1)
        self.assertTrue(client.market_data_target_ready(token_id, 0.3))
        telemetry = client.telemetry_snapshot()
        self.assertEqual(telemetry["funded_refresh_requests"], 1.0)
        self.assertEqual(telemetry["funded_refresh_deadline_misses"], 0.0)

    async def test_funded_refresh_reserves_capacity_under_saturated_discovery_load(self) -> None:
        self.assertEqual(ORDER_BOOK_REQUEST_CONCURRENCY, 16)
        self.assertEqual(ORDER_BOOK_BOOTSTRAP_CONCURRENCY, 4)
        self.assertEqual(FUNDED_ORDER_BOOK_REFRESH_CONCURRENCY, 12)
        client = MyriadClient(replace(_config(), order_book_ttl_ms=50, websocket_stale_after_ms=300))
        client.set_market_data_execution_freshness(2.0)
        discovery_release = asyncio.Event()
        funded_release = asyncio.Event()
        discovery_saturated = asyncio.Event()
        funded_saturated = asyncio.Event()
        active_discovery_requests = 0
        active_funded_requests = 0
        active_requests = 0
        peak_requests = 0

        async def orderbook(market_id: int, outcome_id: int) -> dict[str, object]:
            nonlocal active_discovery_requests, active_funded_requests, active_requests, peak_requests
            self.assertEqual(outcome_id, 1)
            active_requests += 1
            peak_requests = max(peak_requests, active_requests)
            try:
                if market_id >= 900:
                    active_funded_requests += 1
                    if active_funded_requests == FUNDED_ORDER_BOOK_REFRESH_CONCURRENCY:
                        funded_saturated.set()
                    try:
                        await funded_release.wait()
                    finally:
                        active_funded_requests -= 1
                else:
                    active_discovery_requests += 1
                    if active_discovery_requests == ORDER_BOOK_BOOTSTRAP_CONCURRENCY:
                        discovery_saturated.set()
                    try:
                        await discovery_release.wait()
                    finally:
                        active_discovery_requests -= 1
            finally:
                active_requests -= 1
            return {
                "marketId": market_id,
                "outcomeId": outcome_id,
                "bids": [["230000000000000000", "1000000000000000000"]],
                "asks": [["240000000000000000", "1000000000000000000"]],
            }

        discovery_tokens = [f"{700 + index}:NO" for index in range(ORDER_BOOK_BOOTSTRAP_CONCURRENCY)]
        funded_tokens = [f"{900 + index}:NO" for index in range(FUNDED_ORDER_BOOK_REFRESH_CONCURRENCY)]
        for token_id in [*discovery_tokens, *funded_tokens]:
            market_id = int(token_id.split(":", 1)[0])
            client._ensure_token_subscription(token_id, market_id)  # noqa: SLF001

        with patch.object(client, "get_orderbook", side_effect=orderbook):
            discovery_tasks = [
                asyncio.create_task(
                    client._bootstrap_order_book(  # noqa: SLF001
                        token_id,
                        int(token_id.split(":", 1)[0]),
                        BinarySide.NO,
                        force=True,
                    )
                )
                for token_id in discovery_tokens
            ]
            funded_tasks: list[asyncio.Task[OrderBook]] = []
            try:
                await asyncio.wait_for(discovery_saturated.wait(), timeout=1.0)
                funded_tasks = [
                    asyncio.create_task(client.prime_funded_market_data_target(token_id)) for token_id in funded_tokens
                ]
                await asyncio.wait_for(funded_saturated.wait(), timeout=2.0)
                self.assertEqual(active_discovery_requests, ORDER_BOOK_BOOTSTRAP_CONCURRENCY)
                self.assertEqual(active_funded_requests, FUNDED_ORDER_BOOK_REFRESH_CONCURRENCY)
                self.assertEqual(active_requests, ORDER_BOOK_REQUEST_CONCURRENCY)
                self.assertEqual(peak_requests, ORDER_BOOK_REQUEST_CONCURRENCY)
                funded_release.set()
                self.assertEqual(len(await asyncio.gather(*funded_tasks)), len(funded_tokens))
            finally:
                funded_release.set()
                discovery_release.set()
                if funded_tasks:
                    await asyncio.gather(*funded_tasks, return_exceptions=True)
                await asyncio.gather(*discovery_tasks, return_exceptions=True)

        self.assertEqual(active_requests, 0)
        self.assertEqual(peak_requests, ORDER_BOOK_REQUEST_CONCURRENCY)

    async def test_saturated_discovery_and_funded_requests_admit_queued_hedge(
        self,
    ) -> None:
        client = MyriadClient(replace(_config(), order_book_ttl_ms=50, websocket_stale_after_ms=300))
        client.set_market_data_execution_freshness(2.0)
        discovery_tokens = tuple(
            f"{700 + index}:NO" for index in range(ORDER_BOOK_BOOTSTRAP_CONCURRENCY)
        )
        funded_tokens = tuple(
            f"{900 + index}:NO" for index in range(FUNDED_ORDER_BOOK_REFRESH_CONCURRENCY)
        )
        release_by_market = {
            int(token_id.split(":", 1)[0]): asyncio.Event()
            for token_id in (*discovery_tokens, *funded_tokens)
        }
        all_requests_saturated = asyncio.Event()
        hedge_started = asyncio.Event()
        calls_by_market: dict[int, int] = {}
        active_requests = 0
        peak_requests = 0
        first_hedge_market: int | None = None

        async def orderbook(market_id: int, outcome_id: int) -> dict[str, object]:
            nonlocal active_requests, peak_requests, first_hedge_market
            self.assertEqual(outcome_id, 1)
            calls_by_market[market_id] = calls_by_market.get(market_id, 0) + 1
            active_requests += 1
            peak_requests = max(peak_requests, active_requests)
            if active_requests == ORDER_BOOK_REQUEST_CONCURRENCY:
                all_requests_saturated.set()
            try:
                if calls_by_market[market_id] == 2:
                    if first_hedge_market is None:
                        first_hedge_market = market_id
                    if market_id == 900:
                        hedge_started.set()
                    else:
                        await release_by_market[market_id].wait()
                else:
                    await release_by_market[market_id].wait()
            finally:
                active_requests -= 1
            return {
                "marketId": market_id,
                "outcomeId": outcome_id,
                "bids": [["230000000000000000", "1000000000000000000"]],
                "asks": [["240000000000000000", "1000000000000000000"]],
            }

        for token_id in (*discovery_tokens, *funded_tokens):
            market_id = int(token_id.split(":", 1)[0])
            client._ensure_token_subscription(token_id, market_id)  # noqa: SLF001

        discovery_tasks: list[asyncio.Task[OrderBook]] = []
        funded_tasks: list[asyncio.Task[OrderBook]] = []
        with patch.object(client, "get_orderbook", side_effect=orderbook):
            try:
                discovery_tasks = [
                    asyncio.create_task(
                        client._bootstrap_order_book(  # noqa: SLF001
                            token_id,
                            int(token_id.split(":", 1)[0]),
                            BinarySide.NO,
                            force=True,
                        )
                    )
                    for token_id in discovery_tokens
                ]
                funded_tasks = [
                    asyncio.create_task(client.prime_funded_market_data_target(token_id))
                    for token_id in funded_tokens
                ]
                await asyncio.wait_for(all_requests_saturated.wait(), timeout=2.0)
                self.assertEqual(active_requests, ORDER_BOOK_REQUEST_CONCURRENCY)

                # The first slow primary has reached its 500 ms hedge delay,
                # acquired a hedge slot, and is waiting only for a global HTTP
                # permit. Releasing one unrelated primary must admit that hedge
                # while the original request and all discovery calls stay open.
                for _ in range(50):
                    if client._funded_refresh_hedge_semaphore._value < (  # noqa: SLF001
                        FUNDED_REFRESH_HEDGE_CONCURRENCY
                    ):
                        break
                    await asyncio.sleep(0.01)
                self.assertLess(  # noqa: SLF001
                    client._funded_refresh_hedge_semaphore._value,
                    FUNDED_REFRESH_HEDGE_CONCURRENCY,
                )
                release_by_market[901].set()
                await asyncio.wait_for(hedge_started.wait(), timeout=0.5)
                self.assertFalse(release_by_market[900].is_set())
                self.assertTrue(
                    all(
                        not release_by_market[int(token_id.split(":", 1)[0])].is_set()
                        for token_id in discovery_tokens
                    )
                )
                self.assertEqual(first_hedge_market, 900)
                self.assertLessEqual(peak_requests, ORDER_BOOK_REQUEST_CONCURRENCY)
            finally:
                for release in release_by_market.values():
                    release.set()
                await client.close()
                for task in (*discovery_tasks, *funded_tasks):
                    if not task.done():
                        task.cancel()
                await asyncio.gather(
                    *discovery_tasks,
                    *funded_tasks,
                    return_exceptions=True,
                )

        self.assertEqual(active_requests, 0)
        self.assertEqual(peak_requests, ORDER_BOOK_REQUEST_CONCURRENCY)
        self.assertEqual(  # noqa: SLF001
            client._funded_refresh_semaphore._value,
            FUNDED_ORDER_BOOK_REFRESH_CONCURRENCY,
        )
        self.assertEqual(  # noqa: SLF001
            client._funded_refresh_hedge_semaphore._value,
            FUNDED_REFRESH_HEDGE_CONCURRENCY,
        )
        self.assertEqual(  # noqa: SLF001
            client._order_book_request_semaphore._value,
            ORDER_BOOK_REQUEST_CONCURRENCY,
        )

    async def test_funded_prime_bypasses_queued_background_bootstrap_after_sync(self) -> None:
        client = MyriadClient(replace(_config(), order_book_ttl_ms=50, websocket_stale_after_ms=300))
        client.set_market_data_execution_freshness(2.0)
        discovery_release = asyncio.Event()
        discovery_saturated = asyncio.Event()
        active_discovery_requests = 0
        funded_market_id = 900

        async def orderbook(market_id: int, outcome_id: int) -> dict[str, object]:
            nonlocal active_discovery_requests
            self.assertEqual(outcome_id, 1)
            if market_id != funded_market_id:
                active_discovery_requests += 1
                if active_discovery_requests == ORDER_BOOK_BOOTSTRAP_CONCURRENCY:
                    discovery_saturated.set()
                try:
                    await discovery_release.wait()
                finally:
                    active_discovery_requests -= 1
            return {
                "marketId": market_id,
                "outcomeId": outcome_id,
                "bids": [["230000000000000000", "1000000000000000000"]],
                "asks": [["240000000000000000", "1000000000000000000"]],
            }

        discovery_tokens = {f"{700 + index}:NO" for index in range(ORDER_BOOK_BOOTSTRAP_CONCURRENCY)}
        funded_token = f"{funded_market_id}:NO"
        with (
            patch.object(client, "get_orderbook", side_effect=orderbook),
            patch.object(client, "_ensure_ws_task"),
        ):
            client.sync_market_data_targets(discovery_tokens)
            try:
                await asyncio.wait_for(discovery_saturated.wait(), timeout=1.0)
                client.sync_market_data_targets(discovery_tokens | {funded_token})
                queued_background = client._bootstrap_tasks[funded_token]  # noqa: SLF001
                await asyncio.sleep(0)
                self.assertFalse(queued_background.done())

                book = await asyncio.wait_for(
                    client.prime_funded_market_data_target(funded_token),
                    timeout=0.5,
                )

                self.assertEqual(book.best_bid, OrderBookLevel(0.23, 1.0))
                self.assertTrue(client.market_data_target_ready(funded_token, 2.0))
                self.assertFalse(queued_background.done())
            finally:
                discovery_release.set()
                await asyncio.gather(
                    *tuple(client._bootstrap_tasks.values()),  # noqa: SLF001
                    return_exceptions=True,
                )

    async def test_concurrent_funded_primes_share_one_shielded_request(self) -> None:
        client = MyriadClient(replace(_config(), order_book_ttl_ms=50, websocket_stale_after_ms=300))
        client.set_market_data_execution_freshness(2.0)
        token_id = "553:NO"
        client._ensure_token_subscription(token_id, 553)  # noqa: SLF001
        request_started = asyncio.Event()
        release_request = asyncio.Event()
        request_count = 0
        active_requests = 0

        async def delayed_orderbook(market_id: int, outcome_id: int) -> dict[str, object]:
            nonlocal request_count, active_requests
            self.assertEqual((market_id, outcome_id), (553, 1))
            request_count += 1
            active_requests += 1
            request_started.set()
            try:
                await release_request.wait()
                return {
                    "marketId": market_id,
                    "outcomeId": outcome_id,
                    "bids": [["230000000000000000", "1000000000000000000"]],
                    "asks": [["240000000000000000", "1000000000000000000"]],
                }
            finally:
                active_requests -= 1

        with patch.object(client, "get_orderbook", side_effect=delayed_orderbook):
            first = asyncio.create_task(client.prime_funded_market_data_target(token_id))
            await asyncio.wait_for(request_started.wait(), timeout=1.0)
            second = asyncio.create_task(client.prime_funded_market_data_target(token_id))
            await asyncio.sleep(0)

            self.assertEqual(request_count, 1)
            self.assertEqual(active_requests, 1)
            self.assertEqual(client.telemetry_snapshot()["funded_refresh_coalesced"], 1.0)

            first.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await first
            self.assertEqual(active_requests, 1)

            release_request.set()
            book = await asyncio.wait_for(second, timeout=1.0)

        self.assertEqual(book.best_bid, OrderBookLevel(0.23, 1.0))
        self.assertEqual(request_count, 1)
        self.assertEqual(active_requests, 0)
        self.assertNotIn(token_id, client._funded_refresh_tasks)  # noqa: SLF001

    async def test_proactive_and_priority_prime_share_one_request_in_both_orders(self) -> None:
        async def run_order(first: str) -> None:
            client = MyriadClient(replace(_config(), order_book_ttl_ms=50, websocket_stale_after_ms=300))
            client.set_market_data_execution_freshness(2.0)
            token_id = "553:NO"
            client._ensure_token_subscription(token_id, 553)  # noqa: SLF001
            request_started = asyncio.Event()
            release_request = asyncio.Event()
            request_count = 0

            async def delayed_orderbook(
                market_id: int,
                outcome_id: int,
            ) -> dict[str, object]:
                nonlocal request_count
                self.assertEqual((market_id, outcome_id), (553, 1))
                request_count += 1
                request_started.set()
                await release_request.wait()
                return {
                    "marketId": market_id,
                    "outcomeId": outcome_id,
                    "bids": [["230000000000000000", "1000000000000000000"]],
                    "asks": [["240000000000000000", "1000000000000000000"]],
                }

            async def proactive() -> object:
                return await client.refresh_market_data_target(token_id)

            async def priority_prime() -> object:
                return await client.prime_funded_market_data_target(token_id)

            with patch.object(client, "get_orderbook", side_effect=delayed_orderbook):
                if first == "proactive":
                    first_waiter = asyncio.create_task(proactive())
                else:
                    first_waiter = asyncio.create_task(priority_prime())
                await asyncio.wait_for(request_started.wait(), timeout=1.0)
                if first == "proactive":
                    second_waiter = asyncio.create_task(priority_prime())
                else:
                    second_waiter = asyncio.create_task(proactive())
                await asyncio.sleep(0)
                self.assertEqual(request_count, 1)
                release_request.set()
                await asyncio.wait_for(
                    asyncio.gather(first_waiter, second_waiter),
                    timeout=1.0,
                )

            self.assertEqual(request_count, 1)
            self.assertEqual(
                client.telemetry_snapshot()["funded_refresh_requests"],
                1.0,
            )

        await run_order("proactive")
        await run_order("prime")

    async def test_failed_first_refresh_retries_behind_existing_fifo_waiters(self) -> None:
        client = MyriadClient(replace(_config(), order_book_ttl_ms=50, websocket_stale_after_ms=300))
        client.set_market_data_execution_freshness(2.0)
        token_ids = [f"{600 + index}:NO" for index in range(FUNDED_ORDER_BOOK_REFRESH_CONCURRENCY)]
        for token_id in token_ids:
            client._ensure_token_subscription(  # noqa: SLF001
                token_id,
                int(token_id.split(":", 1)[0]),
            )
        starts: list[int] = []
        first_failed = False

        async def orderbook(market_id: int, outcome_id: int) -> dict[str, object]:
            nonlocal first_failed
            self.assertEqual(outcome_id, 1)
            starts.append(market_id)
            if market_id == 600 and not first_failed:
                first_failed = True
                raise OrderBookUnavailableException("synthetic first request failure")
            return {
                "marketId": market_id,
                "outcomeId": outcome_id,
                "bids": [["230000000000000000", "1000000000000000000"]],
                "asks": [["240000000000000000", "1000000000000000000"]],
            }

        with patch.object(client, "get_orderbook", side_effect=orderbook):
            first = asyncio.create_task(client.prime_funded_market_data_target(token_ids[0]))
            queued = [
                asyncio.create_task(client.prime_funded_market_data_target(token_id)) for token_id in token_ids[1:]
            ]
            with self.assertRaises(OrderBookUnavailableException):
                await first
            retry = asyncio.create_task(
                client.prime_funded_market_data_target(token_ids[0])
            )
            await asyncio.wait_for(asyncio.gather(*queued, retry), timeout=2.0)

        self.assertEqual(starts[0], 600)
        self.assertEqual(starts[1:-1], list(range(601, 612)))
        self.assertEqual(starts[-1], 600)
        self.assertEqual(len(starts), FUNDED_ORDER_BOOK_REFRESH_CONCURRENCY + 1)
        telemetry = client.telemetry_snapshot()
        self.assertEqual(telemetry["funded_refresh_retry_requests"], 0.0)
        self.assertEqual(telemetry["funded_refresh_hedge_requests"], 0.0)

    async def test_remove_and_readd_cancels_queued_refresh_without_permit_leak(self) -> None:
        client = MyriadClient(replace(_config(), order_book_ttl_ms=50, websocket_stale_after_ms=300))
        client.set_market_data_execution_freshness(2.0)
        token_id = "553:NO"
        client._ensure_token_subscription(token_id, 553)  # noqa: SLF001
        client._next_funded_refresh_start_at = time.monotonic() + 0.15  # noqa: SLF001
        get_orderbook = AsyncMock(
            return_value={
                "marketId": 553,
                "outcomeId": 1,
                "bids": [["230000000000000000", "1000000000000000000"]],
                "asks": [["240000000000000000", "1000000000000000000"]],
            }
        )

        with patch.object(client, "get_orderbook", get_orderbook):
            queued = asyncio.create_task(client.prime_funded_market_data_target(token_id))
            for _ in range(30):
                if client._funded_refresh_start_lock.locked():  # noqa: SLF001
                    break
                await asyncio.sleep(0.005)
            self.assertTrue(client._funded_refresh_start_lock.locked())  # noqa: SLF001
            client._remove_token_subscription(token_id)  # noqa: SLF001
            with self.assertRaises(asyncio.CancelledError):
                await queued
            await asyncio.sleep(0)
            self.assertFalse(client._funded_refresh_tasks)  # noqa: SLF001
            get_orderbook.assert_not_awaited()

            client._ensure_token_subscription(token_id, 553)  # noqa: SLF001
            book = await asyncio.wait_for(
                client.prime_funded_market_data_target(token_id),
                timeout=0.5,
            )

        self.assertEqual(book.best_bid, OrderBookLevel(0.23, 1.0))
        get_orderbook.assert_awaited_once_with(553, 1)
        self.assertEqual(
            client._funded_refresh_semaphore._value,  # noqa: SLF001
            FUNDED_ORDER_BOOK_REFRESH_CONCURRENCY,
        )
        self.assertEqual(  # noqa: SLF001
            client._order_book_request_semaphore._value,
            ORDER_BOOK_REQUEST_CONCURRENCY,
        )
        self.assertEqual(  # noqa: SLF001
            client._funded_refresh_hedge_semaphore._value,
            FUNDED_REFRESH_HEDGE_CONCURRENCY,
        )

    async def test_close_cancels_single_flight_funded_refresh(self) -> None:
        client = MyriadClient(replace(_config(), order_book_ttl_ms=50, websocket_stale_after_ms=300))
        client.set_market_data_execution_freshness(2.0)
        token_id = "553:NO"
        client._ensure_token_subscription(token_id, 553)  # noqa: SLF001
        request_started = asyncio.Event()
        request_cancelled = asyncio.Event()

        async def blocked_orderbook(market_id: int, outcome_id: int) -> dict[str, object]:
            self.assertEqual((market_id, outcome_id), (553, 1))
            request_started.set()
            try:
                await asyncio.Event().wait()
            finally:
                request_cancelled.set()
            raise AssertionError("cancelled request unexpectedly resumed")

        with patch.object(client, "get_orderbook", side_effect=blocked_orderbook):
            waiter = asyncio.create_task(client.prime_funded_market_data_target(token_id))
            await asyncio.wait_for(request_started.wait(), timeout=1.0)
            await client.close()
            await asyncio.wait_for(request_cancelled.wait(), timeout=1.0)
            with self.assertRaises(asyncio.CancelledError):
                await waiter

        self.assertFalse(client._funded_refresh_tasks)  # noqa: SLF001

    async def test_close_cancels_active_primary_and_hedge_without_permit_leak(self) -> None:
        client = MyriadClient(replace(_config(), order_book_ttl_ms=50, websocket_stale_after_ms=300))
        client.set_market_data_execution_freshness(0.3)
        token_id = "553:NO"
        client._ensure_token_subscription(token_id, 553)  # noqa: SLF001
        both_started = asyncio.Event()
        started_requests = 0
        cancelled_requests = 0

        async def blocked_orderbook(market_id: int, outcome_id: int) -> dict[str, object]:
            nonlocal started_requests, cancelled_requests
            self.assertEqual((market_id, outcome_id), (553, 1))
            started_requests += 1
            if started_requests == 2:
                both_started.set()
            try:
                await asyncio.Event().wait()
            finally:
                cancelled_requests += 1
            raise AssertionError("cancelled request unexpectedly resumed")

        with patch.object(client, "get_orderbook", side_effect=blocked_orderbook):
            waiter = asyncio.create_task(client.prime_funded_market_data_target(token_id))
            await asyncio.wait_for(both_started.wait(), timeout=1.0)
            telemetry = client.telemetry_snapshot()
            self.assertEqual(telemetry["funded_refresh_hedge_requests"], 1.0)
            self.assertEqual(telemetry["funded_refresh_inflight_requests"], 2.0)
            await client.close()
            with self.assertRaises(asyncio.CancelledError):
                await waiter

        self.assertEqual(started_requests, 2)
        self.assertEqual(cancelled_requests, 2)
        self.assertFalse(client._funded_refresh_tasks)  # noqa: SLF001
        self.assertEqual(  # noqa: SLF001
            client._funded_refresh_semaphore._value,
            FUNDED_ORDER_BOOK_REFRESH_CONCURRENCY,
        )
        self.assertEqual(  # noqa: SLF001
            client._funded_refresh_hedge_semaphore._value,
            FUNDED_REFRESH_HEDGE_CONCURRENCY,
        )
        self.assertEqual(  # noqa: SLF001
            client._order_book_request_semaphore._value,
            ORDER_BOOK_REQUEST_CONCURRENCY,
        )
        self.assertEqual(
            client.telemetry_snapshot()["funded_refresh_inflight_requests"],
            0.0,
        )

    async def test_double_timeout_is_bounded_and_next_cycle_remains_retryable(self) -> None:
        client = MyriadClient(replace(_config(), order_book_ttl_ms=50, websocket_stale_after_ms=300))
        client.set_market_data_execution_freshness(0.3)
        token_id = "553:NO"
        client._ensure_token_subscription(token_id, 553)  # noqa: SLF001
        client._store_book(  # noqa: SLF001
            token_id,
            OrderBook(
                bids=[OrderBookLevel(0.23, 1.0)],
                asks=[OrderBookLevel(0.24, 1.0)],
            ),
        )
        client._book_timestamps[token_id] = time.monotonic() - 0.2  # noqa: SLF001
        started_requests = 0
        cancelled_requests = 0

        async def slow_orderbook(market_id: int, outcome_id: int) -> dict[str, object]:
            nonlocal started_requests, cancelled_requests
            self.assertEqual((market_id, outcome_id), (553, 1))
            started_requests += 1
            try:
                await asyncio.Event().wait()
            finally:
                cancelled_requests += 1
            raise AssertionError("cancelled request unexpectedly resumed")

        with patch.object(client, "get_orderbook", side_effect=slow_orderbook):
            self.assertFalse(await client.refresh_market_data_target(token_id))

        self.assertEqual(started_requests, 2)
        self.assertEqual(cancelled_requests, 2)
        self.assertEqual(client.telemetry_snapshot()["proactive_refreshes"], 0.0)
        self.assertEqual(client.telemetry_snapshot()["proactive_refresh_failures"], 0.0)
        self.assertEqual(client.telemetry_snapshot()["proactive_refresh_timeouts"], 1.0)
        self.assertEqual(
            client.telemetry_snapshot()["funded_refresh_request_timeouts"],
            2.0,
        )
        self.assertEqual(
            client.telemetry_snapshot()["funded_refresh_retry_requests"],
            0.0,
        )
        self.assertEqual(
            client.telemetry_snapshot()["funded_refresh_hedge_requests"],
            1.0,
        )
        self.assertEqual(
            client.telemetry_snapshot()["funded_refresh_hedge_failures"],
            1.0,
        )
        self.assertEqual(client.telemetry_snapshot()["funded_refresh_requests"], 2.0)

        client._stale_refresh_attempted_at[token_id] = time.monotonic() - 0.2  # noqa: SLF001
        with patch.object(
            client,
            "get_orderbook",
            AsyncMock(
                return_value={
                    "marketId": 553,
                    "outcomeId": 1,
                    "bids": [["230000000000000000", "1000000000000000000"]],
                    "asks": [["240000000000000000", "1000000000000000000"]],
                }
            ),
        ):
            self.assertTrue(await client.refresh_market_data_target(token_id))

        self.assertEqual(client.telemetry_snapshot()["proactive_refreshes"], 1.0)
        self.assertEqual(client.telemetry_snapshot()["proactive_refresh_failures"], 0.0)
        self.assertEqual(client.telemetry_snapshot()["proactive_refresh_timeouts"], 1.0)
        self.assertEqual(
            client.telemetry_snapshot()["funded_refresh_request_timeouts"],
            2.0,
        )
        self.assertEqual(
            client.telemetry_snapshot()["funded_refresh_retry_requests"],
            0.0,
        )
        self.assertEqual(client.telemetry_snapshot()["funded_refresh_requests"], 3.0)

    async def test_mixed_timeout_and_contract_error_preserves_actionable_error(
        self,
    ) -> None:
        client = MyriadClient(replace(_config(), order_book_ttl_ms=50, websocket_stale_after_ms=300))
        client.set_market_data_execution_freshness(0.4)
        token_id = "553:NO"
        client._ensure_token_subscription(token_id, 553)  # noqa: SLF001
        client._store_book(  # noqa: SLF001
            token_id,
            OrderBook(
                bids=[OrderBookLevel(0.20, 1.0)],
                asks=[OrderBookLevel(0.30, 1.0)],
            ),
        )
        client._book_timestamps[token_id] = time.monotonic() - 0.05  # noqa: SLF001
        client._stale_refresh_attempted_at[token_id] = time.monotonic() - 1.0  # noqa: SLF001
        started_requests = 0
        primary_cancelled = asyncio.Event()

        async def timeout_then_contract_error(
            market_id: int,
            outcome_id: int,
        ) -> dict[str, object]:
            nonlocal started_requests
            self.assertEqual((market_id, outcome_id), (553, 1))
            started_requests += 1
            if started_requests == 1:
                try:
                    await asyncio.Event().wait()
                finally:
                    primary_cancelled.set()
                raise AssertionError("cancelled primary unexpectedly resumed")
            raise OrderBookUnavailableException("synthetic hedge contract failure")

        with patch.object(client, "get_orderbook", side_effect=timeout_then_contract_error):
            with self.assertRaisesRegex(
                OrderBookUnavailableException,
                "synthetic hedge contract failure",
            ):
                await client.refresh_market_data_target(token_id)

        self.assertTrue(primary_cancelled.is_set())
        self.assertEqual(started_requests, 2)
        telemetry = client.telemetry_snapshot()
        self.assertEqual(telemetry["proactive_refreshes"], 0.0)
        self.assertEqual(telemetry["proactive_refresh_failures"], 1.0)
        self.assertEqual(telemetry["proactive_refresh_timeouts"], 0.0)
        self.assertEqual(telemetry["funded_refresh_requests"], 2.0)
        self.assertEqual(telemetry["funded_refresh_request_timeouts"], 1.0)
        self.assertEqual(telemetry["funded_refresh_hedge_requests"], 1.0)
        self.assertEqual(telemetry["funded_refresh_hedge_failures"], 1.0)

    async def test_slow_primary_is_hedged_inside_same_bounded_refresh(self) -> None:
        client = MyriadClient(replace(_config(), order_book_ttl_ms=50, websocket_stale_after_ms=300))
        client.set_market_data_execution_freshness(0.4)
        token_id = "553:NO"
        client._ensure_token_subscription(token_id, 553)  # noqa: SLF001
        client._store_book(  # noqa: SLF001
            token_id,
            OrderBook(
                bids=[OrderBookLevel(0.20, 1.0)],
                asks=[OrderBookLevel(0.30, 1.0)],
            ),
        )
        client._book_timestamps[token_id] = time.monotonic() - 0.05  # noqa: SLF001
        client._stale_refresh_attempted_at[token_id] = time.monotonic() - 1.0  # noqa: SLF001
        started_requests = 0
        cancelled_requests = 0

        async def timeout_then_recover(
            market_id: int,
            outcome_id: int,
        ) -> dict[str, object]:
            nonlocal started_requests, cancelled_requests
            self.assertEqual((market_id, outcome_id), (553, 1))
            started_requests += 1
            if started_requests == 1:
                try:
                    await asyncio.Event().wait()
                finally:
                    cancelled_requests += 1
                raise AssertionError("cancelled request unexpectedly resumed")
            return {
                "marketId": market_id,
                "outcomeId": outcome_id,
                "bids": [["230000000000000000", "1000000000000000000"]],
                "asks": [["240000000000000000", "1000000000000000000"]],
            }

        with patch.object(client, "get_orderbook", side_effect=timeout_then_recover):
            self.assertTrue(await client.refresh_market_data_target(token_id))

        telemetry = client.telemetry_snapshot()
        self.assertEqual(started_requests, 2)
        self.assertEqual(cancelled_requests, 1)
        self.assertEqual(telemetry["funded_refresh_requests"], 2.0)
        self.assertEqual(telemetry["funded_refresh_request_timeouts"], 0.0)
        self.assertEqual(telemetry["funded_refresh_retry_requests"], 0.0)
        self.assertEqual(telemetry["funded_refresh_hedge_requests"], 1.0)
        self.assertEqual(telemetry["funded_refresh_hedge_wins"], 1.0)
        self.assertEqual(telemetry["funded_refresh_hedge_failures"], 0.0)
        self.assertEqual(telemetry["proactive_refreshes"], 1.0)
        self.assertEqual(telemetry["proactive_refresh_timeouts"], 0.0)
        self.assertEqual(telemetry["funded_refresh_deadline_misses"], 0.0)
        self.assertTrue(client.market_data_target_ready(token_id, 0.4))

    async def test_websocket_replacement_after_timeout_avoids_retry_request(self) -> None:
        client = MyriadClient(replace(_config(), order_book_ttl_ms=50, websocket_stale_after_ms=300))
        client.set_market_data_execution_freshness(0.4)
        token_id = "553:NO"
        channel = "orderbook:56:553"
        client._ensure_token_subscription(token_id, 553)  # noqa: SLF001
        client._store_book(  # noqa: SLF001
            token_id,
            OrderBook(
                bids=[OrderBookLevel(0.20, 1.0)],
                asks=[OrderBookLevel(0.30, 1.0)],
            ),
        )
        client._book_timestamps[token_id] = time.monotonic() - 0.05  # noqa: SLF001
        client._stale_refresh_attempted_at[token_id] = time.monotonic() - 1.0  # noqa: SLF001
        started_requests = 0
        websocket_stored = asyncio.Event()

        async def timeout_after_websocket_update(
            market_id: int,
            outcome_id: int,
        ) -> dict[str, object]:
            nonlocal started_requests
            self.assertEqual((market_id, outcome_id), (553, 1))
            started_requests += 1
            await asyncio.sleep(0.05)
            client._handle_ws_payload(  # noqa: SLF001
                {
                    "push": {
                        "channel": channel,
                        "pub": {
                            "data": {
                                "networkId": 56,
                                "marketId": market_id,
                                "outcomeId": outcome_id,
                                "bids": [["250000000000000000", "1000000000000000000"]],
                                "asks": [["260000000000000000", "1000000000000000000"]],
                            }
                        },
                    }
                }
            )
            websocket_stored.set()
            await asyncio.Event().wait()
            raise AssertionError("cancelled request unexpectedly resumed")

        with patch.object(
            client,
            "get_orderbook",
            side_effect=timeout_after_websocket_update,
        ):
            self.assertTrue(await client.refresh_market_data_target(token_id))

        self.assertTrue(websocket_stored.is_set())
        self.assertEqual(started_requests, 1)
        self.assertEqual(client._books[token_id].best_bid, OrderBookLevel(0.25, 1.0))  # noqa: SLF001
        telemetry = client.telemetry_snapshot()
        self.assertEqual(telemetry["funded_refresh_requests"], 1.0)
        self.assertEqual(telemetry["funded_refresh_request_timeouts"], 0.0)
        self.assertEqual(telemetry["funded_refresh_retry_requests"], 0.0)
        self.assertEqual(telemetry["funded_refresh_hedge_requests"], 0.0)
        self.assertEqual(telemetry["proactive_refreshes"], 1.0)
        self.assertEqual(telemetry["proactive_refresh_timeouts"], 0.0)

    async def test_primary_can_win_after_tail_hedge_without_timeout(self) -> None:
        client = MyriadClient(replace(_config(), order_book_ttl_ms=50, websocket_stale_after_ms=300))
        client.set_market_data_execution_freshness(2.0)
        token_id = "553:NO"
        client._ensure_token_subscription(token_id, 553)  # noqa: SLF001
        request_count = 0

        async def healthy_tail(market_id: int, outcome_id: int) -> dict[str, object]:
            nonlocal request_count
            self.assertEqual((market_id, outcome_id), (553, 1))
            request_count += 1
            # The measured healthy tail remains inside the primary's full
            # one-second budget. The hedge must not cancel that primary merely
            # because the secondary request has started.
            await asyncio.sleep(0.68)
            return {
                "marketId": market_id,
                "outcomeId": outcome_id,
                "bids": [["230000000000000000", "1000000000000000000"]],
                "asks": [["240000000000000000", "1000000000000000000"]],
            }

        with patch.object(client, "get_orderbook", side_effect=healthy_tail):
            self.assertTrue(await client.refresh_market_data_target(token_id))

        self.assertEqual(request_count, 2)
        self.assertEqual(client.telemetry_snapshot()["proactive_refreshes"], 1.0)
        self.assertEqual(client.telemetry_snapshot()["proactive_refresh_failures"], 0.0)
        self.assertEqual(client.telemetry_snapshot()["proactive_refresh_timeouts"], 0.0)
        self.assertEqual(
            client.telemetry_snapshot()["funded_refresh_primary_wins_after_hedge"],
            1.0,
        )

    async def test_proactive_rest_refresh_cannot_overwrite_newer_websocket_book(self) -> None:
        client = MyriadClient(replace(_config(), order_book_ttl_ms=300, websocket_stale_after_ms=1500))
        client.set_market_data_execution_freshness(2.0)
        token_id = "553:NO"
        channel = "orderbook:56:553"
        client._ensure_token_subscription(token_id, 553)  # noqa: SLF001
        client._store_book(  # noqa: SLF001
            token_id,
            OrderBook(
                bids=[OrderBookLevel(0.20, 1.0)],
                asks=[OrderBookLevel(0.30, 1.0)],
            ),
        )
        initial_receipt = client._book_timestamps[token_id]  # noqa: SLF001
        client._stale_refresh_attempted_at[token_id] = time.monotonic() - 1.1  # noqa: SLF001
        rest_started = asyncio.Event()
        release_rest = asyncio.Event()

        async def delayed_older_rest_book(market_id: int, outcome_id: int) -> dict[str, object]:
            self.assertEqual((market_id, outcome_id), (553, 1))
            rest_started.set()
            await release_rest.wait()
            return {
                "marketId": market_id,
                "outcomeId": outcome_id,
                "bids": [["210000000000000000", "1000000000000000000"]],
                "asks": [["310000000000000000", "1000000000000000000"]],
            }

        with patch.object(client, "get_orderbook", side_effect=delayed_older_rest_book):
            refresh = asyncio.create_task(client.refresh_market_data_target(token_id))
            await asyncio.wait_for(rest_started.wait(), timeout=1.0)
            client._handle_ws_payload(  # noqa: SLF001
                {
                    "push": {
                        "channel": channel,
                        "pub": {
                            "data": {
                                "networkId": 56,
                                "marketId": 553,
                                "outcomeId": 1,
                                "bids": [["250000000000000000", "1000000000000000000"]],
                                "asks": [["260000000000000000", "1000000000000000000"]],
                            }
                        },
                    }
                }
            )
            websocket_book = client._books[token_id]  # noqa: SLF001
            # Windows monotonic clocks may return the same value within one
            # scheduler tick. Force that case so book ordering never relies on
            # timestamp resolution.
            client._book_timestamps[token_id] = initial_receipt  # noqa: SLF001
            release_rest.set()
            refreshed = await asyncio.wait_for(refresh, timeout=1.0)

        self.assertFalse(refreshed)
        self.assertIs(client._books[token_id], websocket_book)  # noqa: SLF001
        self.assertEqual(client._books[token_id].best_bid, OrderBookLevel(0.25, 1.0))  # noqa: SLF001
        self.assertEqual(client._books[token_id].best_ask, OrderBookLevel(0.26, 1.0))  # noqa: SLF001
        self.assertEqual(client._book_timestamps[token_id], initial_receipt)  # noqa: SLF001
        self.assertEqual(
            client.telemetry_snapshot()["proactive_refresh_no_receipt"],
            1.0,
        )

    async def test_racing_normal_and_proactive_rest_refreshes_keep_first_completed_book(self) -> None:
        async def run_race(first_completion: str) -> None:
            client = MyriadClient(replace(_config(), order_book_ttl_ms=300, websocket_stale_after_ms=1500))
            client.set_market_data_execution_freshness(2.0)
            token_id = "553:NO"
            client._ensure_token_subscription(token_id, 553)  # noqa: SLF001
            client._store_book(  # noqa: SLF001
                token_id,
                OrderBook(
                    bids=[OrderBookLevel(0.20, 1.0)],
                    asks=[OrderBookLevel(0.30, 1.0)],
                ),
            )
            client._book_timestamps[token_id] = time.monotonic() - 1.0  # noqa: SLF001
            normal_started = asyncio.Event()
            proactive_started = asyncio.Event()
            release_normal = asyncio.Event()
            release_proactive = asyncio.Event()
            request_count = 0

            async def racing_orderbook(market_id: int, outcome_id: int) -> dict[str, object]:
                nonlocal request_count
                self.assertEqual((market_id, outcome_id), (553, 1))
                request_count += 1
                if request_count == 1:
                    normal_started.set()
                    await release_normal.wait()
                    bid, ask = "210000000000000000", "310000000000000000"
                else:
                    self.assertEqual(request_count, 2)
                    proactive_started.set()
                    await release_proactive.wait()
                    bid, ask = "250000000000000000", "260000000000000000"
                return {
                    "marketId": market_id,
                    "outcomeId": outcome_id,
                    "bids": [[bid, "1000000000000000000"]],
                    "asks": [[ask, "1000000000000000000"]],
                }

            with patch.object(client, "get_orderbook", side_effect=racing_orderbook):
                normal_task, started = client._ensure_bootstrap_task(  # noqa: SLF001
                    token_id,
                    553,
                    BinarySide.NO,
                    force=True,
                    min_refresh_interval_seconds=0.0,
                )
                self.assertTrue(started)
                self.assertIsNotNone(normal_task)
                normal_waiter = asyncio.create_task(  # noqa: SLF001
                    client._await_bootstrap_task(token_id, normal_task)
                )
                await asyncio.wait_for(normal_started.wait(), timeout=1.0)
                client._stale_refresh_attempted_at[token_id] = time.monotonic() - 1.0  # noqa: SLF001
                proactive_waiter = asyncio.create_task(client.refresh_market_data_target(token_id))
                await asyncio.wait_for(proactive_started.wait(), timeout=1.0)

                if first_completion == "normal":
                    release_normal.set()
                    await asyncio.wait_for(normal_waiter, timeout=1.0)
                    release_proactive.set()
                    self.assertTrue(await asyncio.wait_for(proactive_waiter, timeout=1.0))
                    expected_bid, expected_ask = 0.21, 0.31
                else:
                    release_proactive.set()
                    self.assertTrue(await asyncio.wait_for(proactive_waiter, timeout=1.0))
                    release_normal.set()
                    await asyncio.wait_for(normal_waiter, timeout=1.0)
                    expected_bid, expected_ask = 0.25, 0.26

            self.assertEqual(request_count, 2)
            self.assertNotIn(token_id, client._bootstrap_tasks)  # noqa: SLF001
            self.assertEqual(client._books[token_id].best_bid, OrderBookLevel(expected_bid, 1.0))  # noqa: SLF001
            self.assertEqual(client._books[token_id].best_ask, OrderBookLevel(expected_ask, 1.0))  # noqa: SLF001
            self.assertEqual(client.telemetry_snapshot()["proactive_refreshes"], 1.0)
            self.assertEqual(client.telemetry_snapshot()["proactive_refresh_failures"], 0.0)
            self.assertEqual(client.telemetry_snapshot()["proactive_refresh_timeouts"], 0.0)

        await run_race("normal")
        await run_race("proactive")

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
        web3_client = MagicMock()
        web3_client.close = AsyncMock()
        client._web3_client = web3_client

        await client.close()

        rest_session.close.assert_awaited_once()
        ws_session.close.assert_awaited_once()
        web3_client.close.assert_awaited_once()
        self.assertIsNone(client._rest_session)
        self.assertIsNone(client._ws_session)
        self.assertIsNone(client._web3_client)

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

    async def test_list_open_orders_filters_by_trader_account(self) -> None:
        client = MyriadClient(_config())

        with (
            patch.object(client, "_account_address", return_value="0xabc"),
            patch.object(client, "_request_json", AsyncMock(return_value={"data": []})) as request_json,
        ):
            orders = await client.list_open_orders()

        self.assertEqual(orders, [])
        request_json.assert_awaited_once_with(
            "GET",
            "/orders",
            query_params={
                "network_id": "56",
                "status": "open",
                "page": "1",
                "limit": "100",
                "trader": "0xabc",
            },
        )

    async def test_list_fills_uses_user_events_with_unix_since(self) -> None:
        client = MyriadClient(_config())
        since = datetime(2026, 1, 1, tzinfo=UTC)

        with (
            patch.object(client, "_account_address", return_value="0xabc"),
            patch.object(
                client,
                "_request_json",
                AsyncMock(
                    return_value={
                        "data": [
                            {
                                "id": "fill-1",
                                "orderHash": "ord-1",
                                "shares": "1.5",
                                "value": "0.6",
                                "timestamp": 1_700_000_000,
                            }
                        ]
                    }
                ),
            ) as request_json,
        ):
            fills = await client.list_fills(since)

        self.assertEqual(len(fills), 1)
        self.assertEqual(fills[0].venue_order_id, "ord-1")
        self.assertEqual(fills[0].quantity, Decimal("1.5"))
        self.assertEqual(fills[0].price, Decimal("0.4"))
        request_json.assert_awaited_once_with(
            "GET",
            "/users/0xabc/events",
            query_params={
                "network_id": "56",
                "page": "1",
                "limit": "100",
                "since": str(int(since.timestamp())),
            },
        )

    async def test_get_positions_uses_user_markets_snapshot(self) -> None:
        client = MyriadClient(replace(_config(), collateral_symbol="USD1"))

        with (
            patch.object(client, "_account_address", return_value="0xabc"),
            patch.object(
                client,
                "_request_json",
                AsyncMock(
                    return_value={
                        "data": [
                            {"marketId": 123, "outcomeId": 0, "shares": "2.5"},
                            {"marketId": 123, "outcomeId": 1, "shares": "-1.0"},
                        ]
                    }
                ),
            ) as request_json,
        ):
            positions = await client.get_positions()

        self.assertEqual(positions, {"123:YES": Decimal("2.5"), "123:NO": Decimal("-1.0")})
        request_json.assert_awaited_once_with(
            "GET",
            "/users/0xabc/markets",
            query_params={
                "network_id": "56",
                "page": "1",
                "limit": "100",
                "state": "open",
                "min_shares": "0",
                "status": "all",
                "token_address": "0x8d0D000Ee44948FC98c9B98A4FA4921476f08B0d",
            },
        )

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

    async def test_bootstrap_snapshots_use_bounded_concurrency(self) -> None:
        client = BootstrapTrackingClient(_config())

        books = await asyncio.gather(*(client.watch_order_book(f"{market_id}:YES") for market_id in range(100, 112)))

        self.assertEqual(len(books), 12)
        self.assertEqual(client.calls, 12)
        self.assertLessEqual(client.max_active, ORDER_BOOK_BOOTSTRAP_CONCURRENCY)

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

    async def test_entry_guard_runs_after_myriad_nonce_wait_before_post(self) -> None:
        client = MyriadClient(_config())
        request_count = 0

        class Session:
            closed = False

            def post(self, *args: object, **kwargs: object) -> object:
                nonlocal request_count
                del args, kwargs
                request_count += 1
                raise AssertionError("Myriad transport must remain blocked")

        allowed = True

        def pre_transport_guard() -> None:
            if not allowed:
                raise OrderSubmissionRejected("generation replaced")

        client._rest_session = Session()
        await client._nonce_lock.acquire()
        persist_order_id = AsyncMock()
        submission = asyncio.create_task(
            client.buy_with_order_id_persistence(
                "123:YES",
                BinarySide.YES,
                1.0,
                0.4,
                persist_order_id=persist_order_id,
                pre_transport_guard=pre_transport_guard,
            )
        )
        await asyncio.sleep(0)
        self.assertFalse(submission.done())
        allowed = False
        client._nonce_lock.release()

        with self.assertRaisesRegex(OrderSubmissionRejected, "generation replaced"):
            await submission

        self.assertEqual(request_count, 0)
        persist_order_id.assert_not_awaited()


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
