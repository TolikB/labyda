import tempfile
import time
import unittest
from dataclasses import dataclass, replace
from pathlib import Path
from types import SimpleNamespace, TracebackType
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

from predict_sdk.constants import Side, SignatureType
from predict_sdk.types import SignedOrder

from arbitrage_engine.config import PredictFunConfig
from arbitrage_engine.connectors.predict_fun import (
    PredictFunApiClient,
    _extract_first_nested,
    _extract_position_amount,
    _fill_from_trade,
    _invert_binary_order_book,
    _load_abi,
    _normalize_order_amount,
    _order_book_from_payload,
    _order_book_from_reserves,
    _parse_reserves,
    _to_precision_units,
    _venue_order_from_payload,
)
from arbitrage_engine.models import BinarySide, MarketDataStatus, OrderBook, OrderBookLevel


class PredictFunTests(unittest.TestCase):
    def test_live_orderbook_response_wrapper_is_parsed(self) -> None:
        book = _order_book_from_payload({"success": True, "data": {"bids": [[0.40, 10]], "asks": [[0.45, 12]]}})

        self.assertEqual(book.best_bid, OrderBookLevel(0.40, 10))
        self.assertEqual(book.best_ask, OrderBookLevel(0.45, 12))

    def test_no_orderbook_is_complement_of_yes_orderbook(self) -> None:
        yes_book = OrderBook(
            bids=[OrderBookLevel(0.40, 10)],
            asks=[OrderBookLevel(0.45, 12)],
        )

        no_book = _invert_binary_order_book(yes_book)

        self.assertAlmostEqual(no_book.best_bid.price, 0.55)
        self.assertAlmostEqual(no_book.best_ask.price, 0.60)

    def test_provider_update_timestamp_survives_outcome_inversion(self) -> None:
        source_timestamp = time.time() - 8
        yes_book = _order_book_from_payload(
            {
                "data": {
                    "bids": [[0.40, 10]],
                    "asks": [[0.45, 12]],
                    "updateTimestampMs": int(source_timestamp * 1000),
                }
            }
        )

        no_book = _invert_binary_order_book(yes_book)

        self.assertAlmostEqual(no_book.timestamp, source_timestamp, places=2)

    def test_reserve_books_are_isolated_by_outcome(self) -> None:
        yes_book = _order_book_from_reserves((10**18, 3 * 10**18), BinarySide.YES)
        no_book = _order_book_from_reserves((10**18, 3 * 10**18), BinarySide.NO)

        self.assertEqual(yes_book.best_ask.price, 0.75)
        self.assertEqual(no_book.best_ask.price, 0.25)
        self.assertEqual(len(yes_book.asks), 1)
        self.assertEqual(len(no_book.asks), 1)

    def test_load_abi_supports_plain_list(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "abi.json"
            path.write_text('[{"type":"function","name":"buy"}]', encoding="utf-8")

            abi = _load_abi(str(path))

            self.assertEqual(abi[0]["name"], "buy")

    def test_load_abi_supports_artifact_object(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "artifact.json"
            path.write_text('{"abi":[{"type":"function","name":"sell"}]}', encoding="utf-8")

            abi = _load_abi(str(path))

            self.assertEqual(abi[0]["name"], "sell")

    def test_parse_reserves_scales_wei(self) -> None:
        yes, no = _parse_reserves((10**21, 2 * 10**21))

        self.assertEqual(yes, 1000)
        self.assertEqual(no, 2000)

    def test_to_precision_units_uses_decimal_math(self) -> None:
        self.assertEqual(_to_precision_units(0.42, 18), 420_000_000_000_000_000)
        self.assertEqual(_to_precision_units(5.0, 18), 5 * 10**18)

    def test_normalize_order_amount_supports_wei_and_human_units(self) -> None:
        self.assertEqual(_normalize_order_amount(40.0, 100.0, 18), 40.0)
        self.assertEqual(_normalize_order_amount(40 * 10**18, 100.0, 18), 40.0)

    def test_extract_first_nested_supports_wrapped_order_responses(self) -> None:
        payload = {"data": {"order": {"orderId": "abc", "status": "filled"}}}

        self.assertEqual(_extract_first_nested(payload, ("orderId",)), "abc")
        self.assertEqual(_extract_first_nested(payload, ("status",)), "filled")

    def test_build_signed_order_payload_uses_predict_sdk_limit_order(self) -> None:
        calls: dict[str, Any] = {}

        class FakeBuilder:
            def get_limit_order_amounts(self, data: Any) -> Any:
                calls["limit"] = data
                return _Amounts(maker_amount=2500000000000000000, taker_amount=10000000000000000000)

            def build_order(self, strategy: str, data: Any) -> Any:
                calls["strategy"] = strategy
                calls["order_input"] = data
                return object()

            def build_typed_data(self, order: Any, *, is_neg_risk: bool, is_yield_bearing: bool) -> Any:
                calls["typed"] = (is_neg_risk, is_yield_bearing)
                return object()

            def sign_typed_data_order(self, typed_data: Any) -> SignedOrder:
                return SignedOrder(
                    salt="1",
                    maker="0xmaker",
                    signer="0xsigner",
                    taker="0x0000000000000000000000000000000000000000",
                    token_id="123",
                    maker_amount="2500000000000000000",
                    taker_amount="10000000000000000000",
                    expiration="4102444800",
                    nonce="0",
                    fee_rate_bps="0",
                    side=Side.BUY,
                    signature_type=SignatureType.EOA,
                    signature="0xsig",
                )

        client = PredictFunApiClient(_predict_config(), order_builder_factory=FakeBuilder)

        payload = client._build_signed_order_payload(
            token_id="123",
            contracts=10.0,
            limit_price=0.25,
            sdk_side_name="BUY",
            neg_risk=True,
            fee_rate_bps=125,
        )

        self.assertEqual(calls["strategy"], "MARKET")
        self.assertEqual(calls["order_input"].fee_rate_bps, "125")
        self.assertEqual(calls["typed"], (True, False))
        self.assertEqual(payload["tokenId"], "123")
        self.assertEqual(payload["makerAmount"], "2500000000000000000")
        self.assertEqual(payload["takerAmount"], "10000000000000000000")
        self.assertEqual(payload["side"], 0)
        self.assertEqual(payload["signature"], "0xsig")

    def test_build_signed_order_payload_uses_predict_account_for_maker_and_signer(self) -> None:
        calls: dict[str, Any] = {}

        class FakeBuilder:
            def get_limit_order_amounts(self, data: Any) -> Any:
                return _Amounts(maker_amount=2500000000000000000, taker_amount=10000000000000000000)

            def build_order(self, strategy: str, data: Any) -> Any:
                calls["strategy"] = strategy
                calls["order_input"] = data
                return object()

            def build_typed_data(self, order: Any, *, is_neg_risk: bool, is_yield_bearing: bool) -> Any:
                return object()

            def sign_typed_data_order(self, typed_data: Any) -> SignedOrder:
                return SignedOrder(
                    salt="1",
                    maker="0xpredict",
                    signer="0xpredict",
                    taker="0x0000000000000000000000000000000000000000",
                    token_id="123",
                    maker_amount="2500000000000000000",
                    taker_amount="10000000000000000000",
                    expiration="4102444800",
                    nonce="0",
                    fee_rate_bps="0",
                    side=Side.BUY,
                    signature_type=SignatureType.EOA,
                    signature="0xsig",
                )

        client = PredictFunApiClient(
            replace(_predict_config(), account_address="0x0000000000000000000000000000000000000abc"),
            order_builder_factory=FakeBuilder,
        )

        client._build_signed_order_payload(
            token_id="123",
            contracts=10.0,
            limit_price=0.25,
            sdk_side_name="BUY",
            neg_risk=True,
            fee_rate_bps=125,
        )

        self.assertEqual(calls["strategy"], "MARKET")
        self.assertEqual(calls["order_input"].maker, "0x0000000000000000000000000000000000000abc")
        self.assertEqual(calls["order_input"].signer, "0x0000000000000000000000000000000000000abc")

    def test_rest_session_is_reused(self) -> None:
        client = PredictFunApiClient(_predict_config())
        session = MagicMock()
        session.closed = False

        with patch("arbitrage_engine.connectors.predict_fun.client_session", return_value=session) as factory:
            self.assertIs(client._get_rest_session(), session)
            self.assertIs(client._get_rest_session(), session)

        factory.assert_called_once()


class PredictFunLifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def test_websocket_orderbook_requires_supported_version_and_open_status(self) -> None:
        client = PredictFunApiClient(_predict_config())
        client.register_market("token-1", "147609", BinarySide.YES)
        client.sync_market_data_targets({"token-1"})
        ws = SimpleNamespace(send_json=AsyncMock())

        await client._handle_ws_message(  # noqa: SLF001
            ws,
            {
                "type": "M",
                "topic": "predictTradingStatus/147609",
                "data": {"tsMs": 10, "tradingStatus": "OPEN"},
            },
        )
        await client._handle_ws_message(  # noqa: SLF001
            ws,
            {
                "type": "M",
                "topic": "predictOrderbook/147609",
                "data": {
                    "version": 1,
                    "updateTimestampMs": 11,
                    "bids": [[0.40, 10]],
                    "asks": [[0.45, 12]],
                },
            },
        )

        self.assertEqual(client._books["token-1"].status, MarketDataStatus.VALID)
        self.assertEqual(client._books["token-1"].best_ask, OrderBookLevel(0.45, 12))
        with self.assertRaisesRegex(RuntimeError, "version is unsupported"):
            await client._handle_ws_message(  # noqa: SLF001
                ws,
                {
                    "type": "M",
                    "topic": "predictOrderbook/147609",
                    "data": {"version": 2, "updateTimestampMs": 12},
                },
            )

        await client._handle_ws_message(  # noqa: SLF001
            ws,
            {
                "type": "M",
                "topic": "predictTradingStatus/147609",
                "data": {"tsMs": 13, "tradingStatus": "CLOSED"},
            },
        )
        self.assertEqual(client._books["token-1"].status, MarketDataStatus.INVALID)

    async def test_websocket_ignores_out_of_order_updates_and_echoes_heartbeat(self) -> None:
        client = PredictFunApiClient(_predict_config())
        client.register_market("token-1", "147609", BinarySide.YES)
        client.sync_market_data_targets({"token-1"})
        ws = SimpleNamespace(send_json=AsyncMock())
        await client._handle_ws_message(  # noqa: SLF001
            ws,
            {
                "type": "M",
                "topic": "predictOrderbook/147609",
                "data": {
                    "version": 1,
                    "updateTimestampMs": 20,
                    "bids": [[0.40, 10]],
                    "asks": [[0.45, 12]],
                },
            },
        )
        await client._handle_ws_message(  # noqa: SLF001
            ws,
            {
                "type": "M",
                "topic": "predictOrderbook/147609",
                "data": {
                    "version": 1,
                    "updateTimestampMs": 19,
                    "bids": [[0.20, 10]],
                    "asks": [[0.80, 12]],
                },
            },
        )
        await client._handle_ws_message(  # noqa: SLF001
            ws,
            {"type": "M", "topic": "heartbeat", "data": {"tsMs": 21}},
        )

        self.assertEqual(client._books["token-1"].best_ask, OrderBookLevel(0.45, 12))
        self.assertEqual(client.telemetry_snapshot()["sequence_gaps"], 1.0)
        ws.send_json.assert_awaited_once_with({"method": "heartbeat", "data": {"tsMs": 21}})

    async def test_market_data_age_tracks_latest_event_not_stalest_token(self) -> None:
        client = PredictFunApiClient(_predict_config())
        client.sync_market_data_targets({"stale", "fresh"})
        client._books["stale"] = OrderBook([], [])
        client._books["fresh"] = OrderBook([], [])
        client._book_timestamps["stale"] = time.monotonic() - 30
        client._book_timestamps["fresh"] = time.monotonic() - 0.1

        self.assertLess(client.market_data_age_seconds() or 1.0, 0.5)
        self.assertTrue(client.market_data_ready())

    async def test_sync_market_data_targets_primes_background_loops_for_active_tokens(self) -> None:
        client = PredictFunApiClient(_predict_config())
        client._ensure_multicall_task = MagicMock()  # type: ignore[method-assign]
        client._ensure_rest_books_task = MagicMock()  # type: ignore[method-assign]

        client.sync_market_data_targets({"token-1"})

        client._ensure_multicall_task.assert_called_once()
        client._ensure_rest_books_task.assert_called_once()

    async def test_websocket_subscriptions_are_scoped_to_active_market_targets(self) -> None:
        client = PredictFunApiClient(replace(_predict_config(), ws_url="wss://ws.predict.fun/ws"))
        client._ws_connected = True
        client._ensure_ws_task = MagicMock()  # type: ignore[method-assign]
        client._ensure_multicall_task = MagicMock()  # type: ignore[method-assign]
        client._ensure_rest_books_task = MagicMock()  # type: ignore[method-assign]
        client.register_market("inactive", "market-inactive", BinarySide.YES)

        self.assertTrue(client._ws_subscription_queue.empty())

        client.sync_market_data_targets({"inactive"})

        queued = [client._ws_subscription_queue.get_nowait() for _ in range(2)]
        self.assertEqual(
            queued,
            [
                ("subscribe", "predictOrderbook/market-inactive"),
                ("subscribe", "predictTradingStatus/market-inactive"),
            ],
        )
        client.register_market("still-inactive", "market-other", BinarySide.YES)
        self.assertTrue(client._ws_subscription_queue.empty())

    async def test_removing_one_outcome_keeps_shared_market_subscription(self) -> None:
        client = PredictFunApiClient(replace(_predict_config(), ws_url="wss://ws.predict.fun/ws"))
        client._ws_connected = True
        client._ensure_ws_task = MagicMock()  # type: ignore[method-assign]
        client._ensure_multicall_task = MagicMock()  # type: ignore[method-assign]
        client._ensure_rest_books_task = MagicMock()  # type: ignore[method-assign]
        client.register_market("yes-token", "market-1", BinarySide.YES)
        client.register_market("no-token", "market-1", BinarySide.NO)
        client.sync_market_data_targets({"yes-token", "no-token"})
        while not client._ws_subscription_queue.empty():
            client._ws_subscription_queue.get_nowait()

        client.sync_market_data_targets({"yes-token"})

        self.assertTrue(client._ws_subscription_queue.empty())

        client.sync_market_data_targets(set())
        queued = [client._ws_subscription_queue.get_nowait() for _ in range(2)]
        self.assertEqual(
            queued,
            [
                ("unsubscribe", "predictOrderbook/market-1"),
                ("unsubscribe", "predictTradingStatus/market-1"),
            ],
        )

    async def test_rest_recovery_marks_snapshot_fresh_at_receipt(self) -> None:
        client = PredictFunApiClient(_predict_config())
        provider_timestamp = time.time() - 30
        book = OrderBook(
            bids=[OrderBookLevel(0.4, 10)],
            asks=[OrderBookLevel(0.5, 10)],
            timestamp=provider_timestamp,
        )

        client._store_book("token", book, confirmed_at_receipt=True)  # noqa: SLF001

        self.assertGreater(client._books["token"].timestamp, provider_timestamp + 20)

    async def test_order_submission_uses_current_fok_api_envelope_and_hash(self) -> None:
        client = PredictFunApiClient(_predict_config())
        client._build_signed_order_payload = MagicMock(return_value={"tokenId": "123", "expiration": 1})  # type: ignore[method-assign]
        client._request_json = AsyncMock(  # type: ignore[method-assign]
            return_value={"success": True, "data": {"orderId": "cancel-id", "orderHash": "0xhash"}}
        )

        order_id = await client.buy("123", BinarySide.YES, 10.0, 0.25)

        self.assertEqual(order_id, "0xhash")
        request_call = client._request_json.await_args
        assert request_call is not None
        payload = request_call.kwargs["json_body"]
        self.assertEqual(payload["data"]["strategy"], "MARKET")
        self.assertTrue(payload["data"]["isFillOrKill"])
        self.assertEqual(payload["data"]["pricePerShare"], "250000000000000000")
        self.assertEqual(payload["data"]["order"]["tokenId"], "123")

        await client.cancel_order(order_id)
        cancel_call = client._request_json.await_args
        assert cancel_call is not None
        self.assertEqual(cancel_call.args[:2], ("POST", "/v1/orders/remove"))
        self.assertEqual(cancel_call.kwargs["json_body"], {"data": {"ids": ["cancel-id"]}})

    async def test_rpc_reserves_use_registered_amm_address_not_token(self) -> None:
        client = PredictFunApiClient(replace(_predict_config(), market_abi_path="unused.json"))
        called_addresses: list[str] = []

        class ReserveCall:
            async def call(self) -> tuple[int, int]:
                return (10**18, 3 * 10**18)

        class Functions:
            def getPoolReserves(self) -> ReserveCall:
                return ReserveCall()

        class Contract:
            functions = Functions()

        def build_contract(address: str, abi: Any) -> Contract:
            del abi
            called_addresses.append(address)
            return Contract()

        web3_client = MagicMock()
        web3_client.contract.side_effect = build_contract
        client._web3_client = web3_client
        client._market_abi = [{"type": "function", "name": "getPoolReserves", "outputs": []}]
        amm_address = "0x" + "1" * 40
        client.register_market("yes-token", amm_address, BinarySide.YES)

        book = await client._watch_order_book_rpc("yes-token")

        self.assertEqual(called_addresses, [amm_address])
        self.assertEqual(book.best_ask.price, 0.75)

    async def test_close_releases_rest_session(self) -> None:
        client = PredictFunApiClient(_predict_config())
        session = MagicMock()
        session.closed = False
        session.close = AsyncMock()
        client._rest_session = session

        await client.close()

        session.close.assert_awaited_once()
        self.assertIsNone(client._rest_session)

    async def test_cash_balance_uses_configured_balance_function(self) -> None:
        client = PredictFunApiClient(
            replace(
                _predict_config(),
                balance_function="customBalanceOf",
                collateral_token_address="0x" + "2" * 40,
            )
        )
        called_addresses: list[str] = []

        class BalanceCall:
            async def call(self) -> int:
                return 375_000_000

        class DecimalsCall:
            async def call(self) -> int:
                return 6

        class Functions:
            def customBalanceOf(self, address: str) -> BalanceCall:
                called_addresses.append(address)
                return BalanceCall()

            def decimals(self) -> DecimalsCall:
                return DecimalsCall()

        web3_client = MagicMock()
        web3_client.account = SimpleNamespace(address="0xabc")
        web3_client.contract.return_value = SimpleNamespace(functions=Functions())
        client._web3_client = web3_client

        details = await client.get_cash_balance_details()

        self.assertEqual(called_addresses, ["0xabc"])
        self.assertEqual(details["collateral_token_address"], "0x" + "2" * 40)
        self.assertEqual(details["balance_function"], "customBalanceOf")
        self.assertEqual(details["balance_raw"], "375000000")
        self.assertEqual(details["decimals"], 6)
        self.assertEqual(details["balance"], 375.0)
        self.assertEqual(details["signer_wallet_address"], "0xabc")
        self.assertEqual(await client.get_cash_balance(), 375.0)

    async def test_cash_balance_uses_predict_account_when_configured(self) -> None:
        client = PredictFunApiClient(
            replace(
                _predict_config(),
                account_address="0x0000000000000000000000000000000000000abc",
                collateral_token_address="0x" + "2" * 40,
            )
        )
        called_addresses: list[str] = []

        class BalanceCall:
            async def call(self) -> int:
                return 375_000_000

        class DecimalsCall:
            async def call(self) -> int:
                return 6

        class Functions:
            def balanceOf(self, address: str) -> BalanceCall:
                called_addresses.append(address)
                return BalanceCall()

            def decimals(self) -> DecimalsCall:
                return DecimalsCall()

        web3_client = MagicMock()
        web3_client.account = SimpleNamespace(address="0xsigner")
        web3_client.contract.return_value = SimpleNamespace(functions=Functions())
        client._web3_client = web3_client

        details = await client.get_cash_balance_details()

        self.assertEqual(called_addresses, ["0x0000000000000000000000000000000000000abc"])
        self.assertEqual(details["wallet_address"], "0x0000000000000000000000000000000000000abc")
        self.assertEqual(details["signer_wallet_address"], "0xsigner")

    async def test_list_open_orders_parses_wrapped_response(self) -> None:
        client = PredictFunApiClient(_predict_config())
        client._request_json = AsyncMock(  # type: ignore[method-assign]
            return_value={
                "data": {
                    "orders": [
                        {
                            "orderHash": "0xhash",
                            "amount": str(5 * 10**18),
                            "filledAmount": str(2 * 10**18),
                            "avgPrice": str(25 * 10**16),
                        }
                    ]
                }
            }
        )

        orders = await client.list_open_orders()

        self.assertEqual(len(orders), 1)
        self.assertEqual(orders[0].venue_order_id, "0xhash")
        self.assertEqual(str(orders[0].quantity), "5.0")
        self.assertEqual(str(orders[0].cumulative_filled), "2.0")
        self.assertEqual(str(orders[0].average_price), "0.25")

    async def test_get_order_parses_wrapped_status_response_by_order_hash(self) -> None:
        client = PredictFunApiClient(_predict_config())
        client._order_amounts["0xhash"] = 5.0
        client._order_prices["0xhash"] = 0.25
        client._request_json = AsyncMock(  # type: ignore[method-assign]
            return_value={
                "data": {
                    "order": {
                        "orderHash": "0xhash",
                        "status": "filled",
                        "filledAmount": str(5 * 10**18),
                        "avgPrice": str(25 * 10**16),
                    }
                }
            }
        )

        report = await client.get_order("0xhash")

        self.assertEqual(report.order_id, "0xhash")
        self.assertEqual(report.status.value, "FILLED")
        self.assertEqual(str(report.amount_requested), "5.0")
        self.assertEqual(str(report.amount_filled), "5.0")
        self.assertEqual(str(report.avg_price), "0.25")

    async def test_list_fills_and_positions_parse_nested_payloads(self) -> None:
        client = PredictFunApiClient(_predict_config())
        fills_payload = {
            "data": {
                "trades": [
                    {
                        "id": "fill-1",
                        "orderHash": "0xbuy",
                        "tokenId": "token-1",
                        "matchedAmount": str(3 * 10**18),
                        "avgPrice": str(40 * 10**16),
                        "side": "BUY",
                    },
                    {
                        "id": "fill-2",
                        "orderHash": "0xsell",
                        "tokenId": "token-1",
                        "matchedAmount": str(1 * 10**18),
                        "avgPrice": str(45 * 10**16),
                        "side": "SELL",
                    },
                ]
            }
        }
        positions_payload = {
            "data": {
                "positions": [
                    {"tokenId": "token-1", "size": str(2 * 10**18)},
                    {"onChainId": "token-2", "shares": "3.5"},
                ]
            }
        }
        client._request_json = AsyncMock(side_effect=[fills_payload, positions_payload])  # type: ignore[method-assign]

        fills = await client.list_fills(None)
        positions = await client.get_positions()

        self.assertEqual(len(fills), 2)
        self.assertEqual(fills[0].fill_id, "fill-1")
        self.assertEqual(str(fills[0].quantity), "3.0")
        self.assertEqual(str(fills[0].price), "0.4")
        self.assertEqual(positions, {"token-1": 2, "token-2": 3.5})

    async def test_list_fills_gracefully_handles_missing_trades_endpoint(self) -> None:
        client = PredictFunApiClient(_predict_config())
        client._request_json = AsyncMock(side_effect=Exception("404, message='Not Found'"))  # type: ignore[method-assign]

        fills = await client.list_fills(None)

        self.assertEqual(fills, [])

    async def test_market_orderbook_request_retries_with_jwt_after_public_auth_failure(self) -> None:
        class FakeResponse:
            def __init__(self, status: int, payload: dict[str, Any]) -> None:
                self.status = status
                self._payload = payload

            async def __aenter__(self) -> Any:
                return self

            async def __aexit__(
                self,
                exc_type: type[BaseException] | None,
                exc: BaseException | None,
                tb: TracebackType | None,
            ) -> bool:
                del exc_type, exc, tb
                return False

            async def read(self) -> bytes:
                return b""

            async def json(self) -> dict[str, Any]:
                return self._payload

            def raise_for_status(self) -> None:
                if self.status >= 400:
                    raise RuntimeError(f"http {self.status}")

        class FakeSession:
            def __init__(self, responses: list[FakeResponse]) -> None:
                self._responses = responses
                self.closed = False
                self.headers: list[dict[str, str]] = []

            def request(self, method: str, url: str, **kwargs: Any) -> FakeResponse:
                del method, url
                self.headers.append(dict(kwargs["headers"]))
                return self._responses.pop(0)

        session = FakeSession(
            [
                FakeResponse(403, {"error": "forbidden"}),
                FakeResponse(200, {"data": {"bids": [], "asks": []}}),
            ]
        )
        client = PredictFunApiClient(_predict_config())
        client._get_jwt_token = AsyncMock(return_value="jwt-token")  # type: ignore[method-assign]

        with patch("arbitrage_engine.connectors.predict_fun.client_session", return_value=session):
            payload = await client._request_json("GET", "/v1/markets/147609/orderbook")  # noqa: SLF001

        self.assertEqual(payload["data"], {"bids": [], "asks": []})
        self.assertEqual(len(session.headers), 2)
        self.assertNotIn("Authorization", session.headers[0])
        self.assertEqual(session.headers[1]["Authorization"], "Bearer jwt-token")

    async def test_batch_orderbook_refresh_uses_unified_request_json_path(self) -> None:
        client = PredictFunApiClient(_predict_config())
        client.register_market("token-1", "147609", BinarySide.YES)
        client.sync_market_data_targets({"token-1"})
        client._request_json = AsyncMock(  # type: ignore[method-assign]
            return_value={
                "data": [
                    {
                        "marketId": "147609",
                        "bids": [[0.40, 10]],
                        "asks": [[0.45, 12]],
                    }
                ]
            }
        )

        await client._refresh_rest_books_batch()  # noqa: SLF001

        self.assertEqual(client._books["token-1"].best_bid, OrderBookLevel(0.40, 10))
        request_call = client._request_json.await_args
        assert request_call is not None
        self.assertEqual(request_call.args[:2], ("GET", "/v1/markets/orderbooks"))
        self.assertEqual(request_call.kwargs["query_params"], [("ids", "147609")])

    async def test_get_jwt_token_uses_auth_message_flow_and_caches_token(self) -> None:
        client = PredictFunApiClient(
            replace(
                _predict_config(),
                account_address="0x0000000000000000000000000000000000000abc",
            )
        )
        client._request_json = AsyncMock(  # type: ignore[method-assign]
            side_effect=[
                {"data": {"message": "Sign this challenge"}},
                {"data": {"token": "jwt-token"}},
            ]
        )

        first = await client._get_jwt_token()  # noqa: SLF001
        second = await client._get_jwt_token()  # noqa: SLF001

        self.assertEqual(first, "jwt-token")
        self.assertEqual(second, "jwt-token")
        self.assertEqual(client._request_json.await_count, 2)
        auth_call = client._request_json.await_args_list[1]
        self.assertEqual(auth_call.args[:2], ("POST", "/v1/auth"))
        self.assertEqual(
            auth_call.kwargs["json_body"]["signer"].lower(),
            "0x19e7e376e7c213b7e7e7e46cc70a5dd086daff2a",
        )
        self.assertTrue(auth_call.kwargs["json_body"]["signature"].startswith("0x"))

    async def test_request_headers_add_jwt_only_for_personal_requests(self) -> None:
        client = PredictFunApiClient(_predict_config())
        client._get_jwt_token = AsyncMock(return_value="jwt-token")  # type: ignore[method-assign]

        public_headers = await client._request_headers(require_jwt=False)  # noqa: SLF001
        private_headers = await client._request_headers(require_jwt=True)  # noqa: SLF001

        self.assertEqual(public_headers["x-api-key"], "key")
        self.assertNotIn("Authorization", public_headers)
        self.assertEqual(private_headers["Authorization"], "Bearer jwt-token")


class PredictFunParserTests(unittest.TestCase):
    def test_venue_order_from_payload_accepts_hash_and_scaled_values(self) -> None:
        order = _venue_order_from_payload(
            {
                "data": {
                    "orderHash": "0xhash",
                    "amount": str(5 * 10**18),
                    "filledAmount": str(2 * 10**18),
                    "avgPrice": str(25 * 10**16),
                }
            },
            18,
        )

        self.assertEqual(order.venue_order_id, "0xhash")
        self.assertEqual(str(order.quantity), "5.0")
        self.assertEqual(str(order.cumulative_filled), "2.0")
        self.assertEqual(str(order.average_price), "0.25")

    def test_fill_from_trade_accepts_wrapped_scaled_values(self) -> None:
        fill = _fill_from_trade(
            {
                "data": {
                    "id": "fill-1",
                    "orderHash": "0xhash",
                    "matchedAmount": str(3 * 10**18),
                    "avgPrice": str(40 * 10**16),
                    "feeAmount": "17",
                }
            },
            18,
        )

        self.assertEqual(fill.fill_id, "fill-1")
        self.assertEqual(fill.venue_order_id, "0xhash")
        self.assertEqual(str(fill.quantity), "3.0")
        self.assertEqual(str(fill.price), "0.4")
        self.assertEqual(str(fill.fee), "17")

    def test_extract_position_amount_supports_scaled_and_human_units(self) -> None:
        self.assertEqual(_extract_position_amount({"size": str(2 * 10**18)}, 18), 2)
        self.assertEqual(_extract_position_amount({"shares": "3.5"}, 18), 3.5)


if __name__ == "__main__":
    unittest.main()


@dataclass(frozen=True)
class _Amounts:
    maker_amount: int
    taker_amount: int


def _predict_config() -> PredictFunConfig:
    return PredictFunConfig(
        enabled=True,
        private_key="0x" + "1" * 64,
        rpc_url="https://bsc-dataseed.binance.org",
        rpc_urls=["https://bsc-dataseed.binance.org"],
        chain_id=56,
        network="mainnet",
        api_base_url="https://api.predict.fun/",
        api_key="key",
        ws_url=None,
        market_abi_path=None,
        collateral_token_address=None,
        fee_rate_bps=0,
        precision=18,
        reserves_function="getPoolReserves",
        balance_function="balanceOf",
        max_priority_fee_gwei=3.0,
        confirmations=1,
        max_slippage_pct=0.015,
    )
