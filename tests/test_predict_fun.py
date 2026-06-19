import tempfile
import time
import unittest
from unittest.mock import AsyncMock, MagicMock, patch
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

from predict_sdk.constants import Side, SignatureType
from predict_sdk.types import SignedOrder

from arbitrage_engine.config import PredictFunConfig
from arbitrage_engine.connectors.predict_fun import (
    PredictFunApiClient,
    _extract_first_nested,
    _invert_binary_order_book,
    _load_abi,
    _normalize_order_amount,
    _order_book_from_payload,
    _order_book_from_reserves,
    _parse_reserves,
    _to_precision_units,
)
from arbitrage_engine.models import BinarySide
from arbitrage_engine.models import OrderBook, OrderBookLevel


class PredictFunTests(unittest.TestCase):
    def test_live_orderbook_response_wrapper_is_parsed(self) -> None:
        book = _order_book_from_payload(
            {"success": True, "data": {"bids": [[0.40, 10]], "asks": [[0.45, 12]]}}
        )

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

    def test_rest_session_is_reused(self) -> None:
        client = PredictFunApiClient(_predict_config())
        session = MagicMock()
        session.closed = False

        with patch("arbitrage_engine.connectors.predict_fun.client_session", return_value=session) as factory:
            self.assertIs(client._get_rest_session({"x-api-key": "key"}), session)
            self.assertIs(client._get_rest_session({"x-api-key": "key"}), session)

        factory.assert_called_once()


class PredictFunLifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def test_order_submission_uses_current_fok_api_envelope_and_hash(self) -> None:
        client = PredictFunApiClient(_predict_config())
        client._build_signed_order_payload = MagicMock(return_value={"tokenId": "123", "expiration": 1})  # type: ignore[method-assign]
        client._request_json = AsyncMock(  # type: ignore[method-assign]
            return_value={"success": True, "data": {"orderId": "cancel-id", "orderHash": "0xhash"}}
        )

        order_id = await client.buy("123", BinarySide.YES, 10.0, 0.25)

        self.assertEqual(order_id, "0xhash")
        payload = client._request_json.await_args.kwargs["json_body"]
        self.assertEqual(payload["data"]["strategy"], "MARKET")
        self.assertTrue(payload["data"]["isFillOrKill"])
        self.assertEqual(payload["data"]["pricePerShare"], "250000000000000000")
        self.assertEqual(payload["data"]["order"]["tokenId"], "123")

        await client.cancel_order(order_id)
        self.assertEqual(client._request_json.await_args.args[:2], ("POST", "/v1/orders/remove"))
        self.assertEqual(client._request_json.await_args.kwargs["json_body"], {"data": {"ids": ["cancel-id"]}})

    async def test_rpc_reserves_use_registered_amm_address_not_token(self) -> None:
        client = PredictFunApiClient(replace(_predict_config(), market_abi_path="unused.json"))
        called_addresses: list[str] = []

        class ReserveCall:
            async def call(self):
                return (10**18, 3 * 10**18)

        class Functions:
            def getPoolReserves(self):
                return ReserveCall()

        class Contract:
            functions = Functions()

        web3_client = MagicMock()
        web3_client.contract.side_effect = lambda address, abi: called_addresses.append(address) or Contract()
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
