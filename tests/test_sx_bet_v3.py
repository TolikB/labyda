from __future__ import annotations

import time
import unittest
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import TracebackType
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

from arbitrage_engine.config import SxBetConfig
from arbitrage_engine.connectors.sx_bet import SxBetApiClient, create_sx_bet_client
from arbitrage_engine.connectors.sx_bet_v3 import (
    SxBetV3ApiClient,
    SxBetV3SubmissionUnknown,
    _order_book_from_v3_maker_snapshot,
    _report_from_v3_outcome,
    _sign_v3_order,
    _V3SubmittedOrder,
)
from arbitrage_engine.models import BinarySide, ExecutionStatus, MarketDataStatus, OrderIntent, VenueFeeQuote

PRIVATE_KEY = "0x" + ("1" * 64)
MARKET_HASH = "0x" + ("2" * 64)
BASE_TOKEN = "0x1BC6326EA6aF2aB8E4b6Bc83418044B1923b2956"
ESCROW = "0x007D30a86366EdA2a410a176329f991565d8CfA4"
PROXY = "0x4361123dbdc1D812fdf7D27045aF358C9C8AA70A"


def _v3_config(*, environment: str = "toronto", allow_v3_mainnet: bool = False) -> SxBetConfig:
    return SxBetConfig(
        enabled=True,
        api_base_url="https://api.toronto.sx.bet" if environment == "toronto" else "https://api.sx.bet",
        api_key="v3-key",
        private_key=PRIVATE_KEY,
        rpc_url="https://rpc-rollup.sx.technology",
        rpc_urls=["https://rpc-rollup.sx.technology"],
        chain_id=4162,
        ws_url=(
            "wss://realtime.toronto.sx.bet/connection/websocket"
            if environment == "toronto"
            else "wss://realtime.sx.bet/connection/websocket"
        ),
        api_version="v3",
        environment=environment,
        time_in_force="FOK",
        allow_v3_mainnet=allow_v3_mainnet,
    )


def _v2_config() -> SxBetConfig:
    return SxBetConfig(
        enabled=True,
        api_base_url="https://api.sx.bet",
        api_key="v2-key",
        private_key=PRIVATE_KEY,
        rpc_url="https://rpc-rollup.sx.technology",
        rpc_urls=["https://rpc-rollup.sx.technology"],
        chain_id=4162,
    )


def _metadata() -> dict[str, Any]:
    return {
        "chainId": 79479957,
        "domain": {
            "name": "OBv3 Escrow",
            "version": "1",
            "chainId": 79479957,
            "verifyingContract": ESCROW,
        },
        "activeAsset": {
            "symbol": "USDC",
            "baseToken": BASE_TOKEN,
            "escrowAddress": ESCROW,
            "decimals": 6,
        },
        "oddsLadderStepSize": 125,
        "bettingDelay": {
            "pregameMsDefault": 0,
            "liveMsDefault": 10000,
            "leagueIdToMs": {},
            "sportIdToMs": {},
        },
        "limits": {
            "orderSizeMinimumBaseUnits": "1000000",
            "minRestingOrderSizeBaseUnits": "100000",
            "maxCreateOrders": 10,
            "maxCancelOrders": 100,
        },
    }


def _book_payload(*, version: str = "00100000000000000000001") -> dict[str, Any]:
    return {
        "marketHash": MARKET_HASH,
        "outcomeOne": [{"percentageOdds": "40000000000000000000", "size": "4000000"}],
        "outcomeTwo": [{"percentageOdds": "50000000000000000000", "size": "5000000"}],
        "version": version,
    }


class SxBetV3PureTests(unittest.TestCase):
    def test_factory_keeps_v2_and_selects_v3_explicitly(self) -> None:
        with patch(
            "arbitrage_engine.connectors.sx_bet._utc_now",
            return_value=datetime(2026, 8, 20, tzinfo=UTC),
        ):
            self.assertIsInstance(create_sx_bet_client(_v2_config()), SxBetApiClient)
        self.assertIsInstance(create_sx_bet_client(_v3_config()), SxBetV3ApiClient)

    def test_factory_rejects_v2_mainnet_after_official_cutover(self) -> None:
        with patch(
            "arbitrage_engine.connectors.sx_bet._utc_now",
            return_value=datetime(2026, 8, 25, 15, tzinfo=UTC) + timedelta(seconds=1),
        ):
            with self.assertRaisesRegex(RuntimeError, "V2 mainnet is disabled"):
                create_sx_bet_client(_v2_config())

    def test_mainnet_v3_is_fail_closed_without_operator_cutover(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "operator cutover"):
            SxBetV3ApiClient(_v3_config(environment="mainnet"))
        with patch(
            "arbitrage_engine.connectors.sx_bet_v3._utc_now",
            return_value=datetime(2026, 8, 20, tzinfo=UTC),
        ):
            with self.assertRaisesRegex(RuntimeError, "blocked before"):
                SxBetV3ApiClient(_v3_config(environment="mainnet", allow_v3_mainnet=True))
        with patch(
            "arbitrage_engine.connectors.sx_bet_v3._utc_now",
            return_value=datetime(2026, 8, 25, 15, tzinfo=UTC) + timedelta(seconds=1),
        ):
            self.assertIsInstance(
                SxBetV3ApiClient(_v3_config(environment="mainnet", allow_v3_mainnet=True)),
                SxBetV3ApiClient,
            )

    def test_v3_client_rejects_non_official_authenticated_hosts(self) -> None:
        with self.assertRaisesRegex(ValueError, "official API host"):
            SxBetV3ApiClient(replace(_v3_config(), api_base_url="https://api.toronto.sx.bet.evil.example"))
        with self.assertRaisesRegex(ValueError, "official realtime host"):
            SxBetV3ApiClient(replace(_v3_config(), ws_url="wss://realtime.toronto.sx.bet.evil.example/ws"))

    def test_connected_stream_does_not_make_stale_book_executable(self) -> None:
        client = SxBetV3ApiClient(_v3_config())
        client.register_market("yes-token", MARKET_HASH, BinarySide.YES)
        book = _order_book_from_v3_maker_snapshot(_book_payload(), BinarySide.YES)
        client._books["yes-token"] = book  # noqa: SLF001
        client._book_timestamps["yes-token"] = time.monotonic() - 30  # noqa: SLF001
        client._ws_connected = True  # noqa: SLF001
        client._subscribed_markets.add(MARKET_HASH)  # noqa: SLF001

        self.assertFalse(client._cached_book_is_fresh("yes-token", book))  # noqa: SLF001
        self.assertFalse(client.is_order_book_execution_fresh("yes-token", book, 2.0))

    def test_open_heartbeat_stream_keeps_quiet_book_without_rest_polling(self) -> None:
        client = SxBetV3ApiClient(_v3_config())
        client.register_market("yes-token", MARKET_HASH, BinarySide.YES)
        book = _order_book_from_v3_maker_snapshot(_book_payload(), BinarySide.YES)
        client._books["yes-token"] = book  # noqa: SLF001
        client._book_timestamps["yes-token"] = time.monotonic() - 30  # noqa: SLF001
        client._ws_connected = True  # noqa: SLF001
        client._ws = MagicMock(closed=False)  # noqa: SLF001
        client._subscribed_markets.add(MARKET_HASH)  # noqa: SLF001

        self.assertTrue(client._cached_book_is_fresh("yes-token", book))  # noqa: SLF001
        self.assertTrue(client.is_order_book_execution_fresh("yes-token", book, 2.0))

        client._ws.closed = True  # noqa: SLF001
        self.assertFalse(client._cached_book_is_fresh("yes-token", book))  # noqa: SLF001
        self.assertFalse(client.is_order_book_execution_fresh("yes-token", book, 2.0))

    def test_aggregated_maker_book_maps_both_binary_sides(self) -> None:
        yes = _order_book_from_v3_maker_snapshot(_book_payload(), BinarySide.YES)
        no = _order_book_from_v3_maker_snapshot(_book_payload(), BinarySide.NO)

        self.assertEqual(yes.best_bid.price, 0.4)
        self.assertEqual(yes.best_bid.size, 10.0)
        self.assertEqual(yes.best_ask.price, 0.5)
        self.assertEqual(yes.best_ask.size, 10.0)
        self.assertEqual(no.best_bid.price, 0.5)
        self.assertEqual(no.best_ask.price, 0.6)

    def test_sx_v3_fee_is_charged_on_winning_profit_not_notional(self) -> None:
        quote = VenueFeeQuote(
            venue="SX Bet",
            fee_rate_bps=500,
            model="sx_payout_profit",
            source="sx_user_fees_v3",
            verified=True,
            fee_rate_fraction=Decimal("0.05"),
        )

        self.assertEqual(quote.fee_for_fill(Decimal("10"), Decimal("0.4")), Decimal("0.30"))

    def test_unknown_or_missing_inline_outcome_remains_open(self) -> None:
        submitted = _V3SubmittedOrder(
            order_id="0x" + ("8" * 64),
            market_hash=MARKET_HASH,
            token_id="yes-token",
            action="BUY",
            synthetic_side=BinarySide.YES,
            actual_side=BinarySide.YES,
            requested_contracts=Decimal("10"),
            requested_price=Decimal("0.5"),
            submitted_stake=Decimal("5"),
            submitted_at=datetime.now(UTC),
        )

        unknown = _report_from_v3_outcome(
            submitted,
            {
                "state": "FUTURE_STATE",
                "fillAmount": "1000000",
                "blendedOdds": "50000000000000000000",
            },
        )
        missing = _report_from_v3_outcome(submitted, {})
        partial = _report_from_v3_outcome(
            submitted,
            {
                "state": "PARTIAL_FILL_DONE",
                "fillAmount": "1000000",
                "blendedOdds": "50000000000000000000",
            },
        )

        self.assertEqual(unknown.status, ExecutionStatus.OPEN)
        self.assertEqual(unknown.amount_filled, Decimal("2"))
        self.assertEqual(missing.status, ExecutionStatus.OPEN)
        self.assertEqual(missing.amount_filled, Decimal(0))
        self.assertEqual(partial.status, ExecutionStatus.PARTIAL)
        self.assertEqual(partial.amount_filled, Decimal("2"))
        with self.assertRaisesRegex(RuntimeError, "PARTIAL_FILL_DONE but has no fill"):
            _report_from_v3_outcome(submitted, {"state": "PARTIAL_FILL_DONE"})

    def test_eip712_digest_is_stable_and_time_in_force_is_not_signed(self) -> None:
        try:
            from eth_account import Account
        except ImportError as exc:  # pragma: no cover - dependency gate
            self.skipTest(str(exc))
        account = Account.from_key(PRIVATE_KEY)
        order = {
            "marketHash": MARKET_HASH,
            "baseToken": BASE_TOKEN,
            "totalBetSize": "5000000",
            "percentageOdds": "50000000000000000000",
            "salt": "123",
            "expiry": 0,
            "maker": account.address,
            "isMakerBettingOutcomeOne": True,
            "timeInForce": "FOK",
        }

        signature, digest = _sign_v3_order(account, _metadata()["domain"], order)
        changed = {**order, "timeInForce": "IOC"}
        _, changed_digest = _sign_v3_order(account, _metadata()["domain"], changed)

        self.assertEqual(len(signature), 132)
        self.assertEqual(len(digest), 66)
        self.assertEqual(changed_digest, digest)


class SxBetV3ClientTests(unittest.IsolatedAsyncioTestCase):
    def _client_with_book(self) -> SxBetV3ApiClient:
        client = SxBetV3ApiClient(_v3_config())
        client.register_market("yes-token", MARKET_HASH, BinarySide.YES)
        client.register_market("no-token", MARKET_HASH, BinarySide.NO)
        client._metadata_cache = _metadata()  # noqa: SLF001
        client._apply_book_snapshot(MARKET_HASH, _book_payload())  # noqa: SLF001
        return client

    async def test_snapshot_version_strictly_replaces_and_recovers_complete_gap(self) -> None:
        client = self._client_with_book()
        client._tracked_tokens.add("yes-token")  # noqa: SLF001
        channel = f"orderbook_v3:{MARKET_HASH}"
        client._subscription_positions[channel] = ("epoch", 1)  # noqa: SLF001
        original = client._books["yes-token"]  # noqa: SLF001

        duplicate = _book_payload(version="00100000000000000000001")
        duplicate["outcomeTwo"] = [{"percentageOdds": "60000000000000000000", "size": "5000000"}]
        client._apply_publication(MARKET_HASH, channel, {"offset": 2, "data": duplicate})  # noqa: SLF001
        self.assertIs(client._books["yes-token"], original)  # noqa: SLF001

        newest = _book_payload(version="00100000000000000000003")
        newest["outcomeTwo"] = [{"percentageOdds": "55000000000000000000", "size": "5500000"}]
        client._apply_publication(MARKET_HASH, channel, {"offset": 5, "data": newest})  # noqa: SLF001

        self.assertEqual(client._books["yes-token"].best_ask.price, 0.45)  # noqa: SLF001
        self.assertEqual(client._books["yes-token"].status, MarketDataStatus.VALID)  # noqa: SLF001
        self.assertEqual(client.telemetry_snapshot()["sequence_gaps"], 1.0)
        await client.close()

    async def test_same_version_snapshot_revalidates_a_stale_quiet_book(self) -> None:
        client = self._client_with_book()
        client._mark_market_books_stale(MARKET_HASH)  # noqa: SLF001

        applied = client._apply_book_snapshot(MARKET_HASH, _book_payload())  # noqa: SLF001

        self.assertTrue(applied)
        self.assertEqual(client._books["yes-token"].status, MarketDataStatus.VALID)  # noqa: SLF001
        await client.close()

    async def test_unknown_remote_lifecycle_does_not_finalize_order(self) -> None:
        client = self._client_with_book()
        order_id = "0x" + ("9" * 64)
        client._submitted_orders[order_id] = _V3SubmittedOrder(  # noqa: SLF001
            order_id=order_id,
            market_hash=MARKET_HASH,
            token_id="yes-token",
            action="BUY",
            synthetic_side=BinarySide.YES,
            actual_side=BinarySide.YES,
            requested_contracts=Decimal("10"),
            requested_price=Decimal("0.5"),
            submitted_stake=Decimal("5"),
            submitted_at=datetime.now(UTC),
        )
        base_order = {
            "id": order_id,
            "marketHash": MARKET_HASH,
            "isBettingOutcomeOne": True,
        }

        for lifecycle, expected_error in (
            ({}, "unsupported status MISSING"),
            ({"status": "FUTURE_STATUS"}, "unsupported status FUTURE_STATUS"),
            ({"status": "INACTIVE"}, "unsupported inactiveReason MISSING"),
            (
                {"status": "INACTIVE", "inactiveReason": "FUTURE_REASON"},
                "unsupported inactiveReason FUTURE_REASON",
            ),
        ):
            with self.subTest(lifecycle=lifecycle):
                client._request_json = AsyncMock(  # type: ignore[method-assign]
                    return_value={"data": {**base_order, **lifecycle}}
                )
                with self.assertRaisesRegex(RuntimeError, expected_error):
                    await client.get_order(order_id)
                self.assertNotIn(order_id, client._reports)  # noqa: SLF001

        await client.close()

    async def test_fok_submission_uses_v3_endpoint_and_terminal_outcome(self) -> None:
        client = self._client_with_book()
        seen_body: dict[str, Any] = {}

        async def request(method: str, path: str, **kwargs: Any) -> Any:
            if path == "/user/proxy":
                return {"data": {"deployed": True, "obv3ProxyWalletAddress": ESCROW}}
            if path == "/user/fees-v3":
                return {"data": {"takerPayoutFee": "0.025", "refundFee": "0.01"}}
            if method == "POST" and path == "/orders-v3":
                nonlocal seen_body
                seen_body = kwargs["json_body"]
                order = seen_body["orders"][0]
                account = __import__("eth_account").Account.from_key(PRIVATE_KEY)
                _, order_id = _sign_v3_order(account, _metadata()["domain"], order)
                return {
                    "data": {
                        "orders": [
                            {
                                "orderId": order_id,
                                "status": "SUBMITTED",
                                "outcome": {
                                    "state": "FULLY_FILLED",
                                    "remainingAmount": "0",
                                    "fillAmount": "5000000",
                                    "blendedOdds": "50000000000000000000",
                                },
                            }
                        ]
                    }
                }
            raise AssertionError((method, path, kwargs))

        client._request_json = AsyncMock(side_effect=request)  # type: ignore[method-assign]
        order_id = await client.buy("yes-token", BinarySide.YES, 10.0, 0.5)
        report = await client.wait_filled(order_id, 100)

        order = seen_body["orders"][0]
        self.assertTrue(seen_body["waitForOutcome"])
        self.assertEqual(seen_body["maxWaitTime"], 15_000)
        self.assertEqual(order["timeInForce"], "FOK")
        self.assertRegex(order["salt"], r"^0x[0-9a-f]{64}$")
        self.assertGreater(order["expiry"], int(datetime.now(UTC).timestamp()))
        self.assertEqual(order["totalBetSize"], "5000000")
        self.assertEqual(order["percentageOdds"], "50000000000000000000")
        self.assertEqual(order["clientOrderId"], order_id.removeprefix("0x"))
        self.assertNotIn("desiredOdds", order)
        self.assertNotIn("oddsSlippage", order)
        self.assertEqual(report.status, ExecutionStatus.FILLED)
        self.assertEqual(report.amount_filled, Decimal("10"))
        self.assertEqual(report.remaining_amount, Decimal(0))
        await client.close()

    async def test_inline_timeout_remains_open_for_venue_reconciliation(self) -> None:
        client = self._client_with_book()

        async def request(method: str, path: str, **kwargs: Any) -> Any:
            if path == "/user/proxy":
                return {"data": {"deployed": True, "obv3ProxyWalletAddress": PROXY}}
            if path == "/user/fees-v3":
                return {"data": {"takerPayoutFee": "0.025", "refundFee": "0.01"}}
            if method == "POST" and path == "/orders-v3":
                order = kwargs["json_body"]["orders"][0]
                account = __import__("eth_account").Account.from_key(PRIVATE_KEY)
                _, order_id = _sign_v3_order(account, _metadata()["domain"], order)
                return {
                    "data": {
                        "orders": [
                            {
                                "orderId": order_id,
                                "status": "SUBMITTED",
                                "outcome": {
                                    "state": "TIMEOUT",
                                    "remainingAmount": order["totalBetSize"],
                                    "fillAmount": "0",
                                    "blendedOdds": "0",
                                },
                            }
                        ]
                    }
                }
            if method == "GET" and path.startswith("/orders-v3/"):
                order_id = path.rsplit("/", 1)[-1]
                return {
                    "data": {
                        "id": order_id,
                        "marketHash": MARKET_HASH,
                        "isBettingOutcomeOne": True,
                        "status": "PENDING",
                    }
                }
            raise AssertionError((method, path, kwargs))

        client._request_json = AsyncMock(side_effect=request)  # type: ignore[method-assign]
        order_id = await client.buy("yes-token", BinarySide.YES, 10.0, 0.5)

        self.assertEqual(client._reports[order_id].status, ExecutionStatus.OPEN)  # noqa: SLF001
        report = await client.wait_filled(order_id, 0)
        self.assertEqual(report.status, ExecutionStatus.OPEN)
        await client.close()

    async def test_post_timeout_preserves_digest_for_fail_closed_reconciliation(self) -> None:
        client = self._client_with_book()

        async def request(method: str, path: str, **kwargs: Any) -> Any:
            del kwargs
            if path == "/user/proxy":
                return {"data": {"deployed": True, "obv3ProxyWalletAddress": ESCROW}}
            if path == "/user/fees-v3":
                return {"data": {"takerPayoutFee": "0.025", "refundFee": "0.01"}}
            if method == "POST" and path == "/orders-v3":
                raise TimeoutError("ack timeout")
            if method == "GET" and path.startswith("/orders-v3/"):
                raise RuntimeError("not found")
            raise AssertionError((method, path))

        client._request_json = AsyncMock(side_effect=request)  # type: ignore[method-assign]
        with patch("arbitrage_engine.connectors.sx_bet_v3.asyncio.sleep", new=AsyncMock()):
            with self.assertRaises(SxBetV3SubmissionUnknown) as raised:
                await client.buy("yes-token", BinarySide.YES, 10.0, 0.5)

        self.assertEqual(raised.exception.order_id, next(iter(client._submitted_orders)))  # noqa: SLF001
        self.assertIn(raised.exception.order_id, str(raised.exception))
        await client.close()

    async def test_terminal_fok_no_liquidity_is_cancelled_not_left_open(self) -> None:
        client = self._client_with_book()

        async def request(method: str, path: str, **kwargs: Any) -> Any:
            if path == "/user/proxy":
                return {"data": {"deployed": True, "obv3ProxyWalletAddress": ESCROW}}
            if path == "/user/fees-v3":
                return {"data": {"takerPayoutFee": None, "refundFee": None}}
            if method == "POST" and path == "/orders-v3":
                order = kwargs["json_body"]["orders"][0]
                account = __import__("eth_account").Account.from_key(PRIVATE_KEY)
                _, order_id = _sign_v3_order(account, _metadata()["domain"], order)
                return {
                    "data": {
                        "orders": [
                            {
                                "orderId": order_id,
                                "status": "SUBMITTED",
                                "outcome": {
                                    "state": "NO_LIQUIDITY",
                                    "remainingAmount": order["totalBetSize"],
                                    "fillAmount": "0",
                                    "blendedOdds": "0",
                                },
                            }
                        ]
                    }
                }
            raise AssertionError((method, path))

        client._request_json = AsyncMock(side_effect=request)  # type: ignore[method-assign]
        order_id = await client.buy("yes-token", BinarySide.YES, 10.0, 0.5)
        report = await client.wait_filled(order_id, 100)

        self.assertEqual(report.status, ExecutionStatus.CANCELLED)
        self.assertEqual(report.amount_filled, Decimal(0))
        await client.close()

    async def test_sell_preview_automatically_uses_opposite_outcome_book(self) -> None:
        client = SxBetV3ApiClient(_v3_config())
        client.register_market("yes-token", MARKET_HASH, BinarySide.YES)
        client._metadata_cache = _metadata()  # noqa: SLF001
        client._apply_book_snapshot(MARKET_HASH, _book_payload())  # noqa: SLF001
        async def request(method: str, path: str, **kwargs: Any) -> Any:
            del method, kwargs
            if path == "/user/proxy":
                return {"data": {"deployed": True, "obv3ProxyWalletAddress": PROXY}}
            if path == "/user/fees-v3":
                return {"data": {"takerPayoutFee": "0.025", "refundFee": "0.01"}}
            raise AssertionError(path)

        client._request_json = AsyncMock(side_effect=request)  # type: ignore[method-assign]

        preview = await client.build_order_preview(
            token_id="yes-token",
            side=BinarySide.YES,
            contracts=10,
            limit_price=0.4,
            action="SELL",
        )

        self.assertEqual(preview["actual_order_side"], "NO")
        self.assertEqual(preview["request_payload"]["totalBetSize"], "6000000")
        self.assertEqual(preview["request_payload"]["percentageOdds"], "60000000000000000000")
        self.assertNotIn("salt", preview["request_payload"])
        self.assertNotIn("orderSignature", preview["request_payload"])
        self.assertTrue(preview["signature_prefix"].startswith("sha256:"))
        self.assertEqual(preview["refund_fee_rate"], "0.01")
        self.assertIn((MARKET_HASH, BinarySide.NO), client._token_by_market_side)  # noqa: SLF001
        await client.close()

    async def test_proxy_balance_and_account_fee_are_required_live_metadata(self) -> None:
        client = self._client_with_book()

        async def request(method: str, path: str, **kwargs: Any) -> Any:
            del method, kwargs
            if path == "/user/proxy":
                return {"data": {"deployed": True, "obv3ProxyWalletAddress": PROXY}}
            if path == "/user/balance-v3":
                account = __import__("eth_account").Account.from_key(PRIVATE_KEY)
                return {
                    "data": {
                        "balances": [
                            {
                                "userAddress": account.address,
                                "wallet": PROXY,
                                "tokenAddress": BASE_TOKEN,
                                "escrowAddress": ESCROW,
                                "availableAmount": "25000000",
                                "pendingAvailableAmount": "-1000000",
                                "escrowedAmount": "5000000",
                                "pendingEscrowAmount": "1000000",
                            }
                        ]
                    }
                }
            if path == "/user/fees-v3":
                return {"data": {"takerPayoutFee": "0.025", "refundFee": "0.01"}}
            raise AssertionError(path)

        client._request_json = AsyncMock(side_effect=request)  # type: ignore[method-assign]
        details = await client.get_cash_balance_details()
        constraints = await client.get_market_constraints("yes-token")
        quote = await client.get_fee_quote("yes-token", Decimal("0.5"), constraints)

        self.assertEqual(details["balance"], 24.0)
        self.assertEqual(details["escrowed"], "6")
        self.assertIsNotNone(constraints)
        assert constraints is not None and quote is not None
        self.assertEqual(constraints.minimum_notional, Decimal("1"))
        self.assertEqual(constraints.fee_rate_bps, 250)
        self.assertEqual(quote.fee_for_fill(Decimal("10"), Decimal("0.5")), Decimal("0.1250"))
        self.assertEqual(quote.source, "sx_user_fees_v3")
        await client.close()

    async def test_balance_rejects_mismatched_authenticated_account(self) -> None:
        client = self._client_with_book()

        async def request(method: str, path: str, **kwargs: Any) -> Any:
            del method, kwargs
            if path == "/user/proxy":
                return {"data": {"deployed": True, "obv3ProxyWalletAddress": PROXY}}
            if path == "/user/balance-v3":
                return {
                    "data": {
                        "balances": [
                            {
                                "userAddress": "0x0000000000000000000000000000000000000001",
                                "wallet": PROXY,
                                "tokenAddress": BASE_TOKEN,
                                "escrowAddress": ESCROW,
                                "availableAmount": "25000000",
                                "pendingAvailableAmount": "0",
                                "escrowedAmount": "0",
                                "pendingEscrowAmount": "0",
                            }
                        ]
                    }
                }
            raise AssertionError(path)

        client._request_json = AsyncMock(side_effect=request)  # type: ignore[method-assign]
        with self.assertRaisesRegex(RuntimeError, "do not match the signer"):
            await client.get_cash_balance_details()
        await client.close()

    async def test_missing_proxy_fails_signed_preview_closed(self) -> None:
        client = self._client_with_book()
        client._request_json = AsyncMock(  # type: ignore[method-assign]
            return_value={"data": {"deployed": False, "obv3ProxyWalletAddress": ESCROW}}
        )

        with self.assertRaisesRegex(RuntimeError, "not deployed"):
            await client.build_order_preview(
                token_id="yes-token",
                side=BinarySide.YES,
                contracts=10,
                limit_price=0.5,
                action="BUY",
            )
        await client.close()

    async def test_reconciliation_uses_v3_order_fill_and_position_shapes(self) -> None:
        client = self._client_with_book()
        order_id = "0x" + ("3" * 64)
        fills_queries: list[dict[str, Any]] = []
        submitted = _V3SubmittedOrder(
            order_id=order_id,
            market_hash=MARKET_HASH,
            token_id="yes-token",
            action="BUY",
            synthetic_side=BinarySide.YES,
            actual_side=BinarySide.YES,
            requested_contracts=Decimal("10"),
            requested_price=Decimal("0.5"),
            submitted_stake=Decimal("5"),
            submitted_at=datetime.now(UTC),
        )
        client._submitted_orders[order_id] = submitted  # noqa: SLF001

        async def request(method: str, path: str, **kwargs: Any) -> Any:
            del method
            if path == f"/orders-v3/{order_id}":
                return {
                    "data": {
                        "id": order_id,
                        "marketHash": MARKET_HASH,
                        "isBettingOutcomeOne": True,
                        "status": "INACTIVE",
                        "inactiveReason": "FILLED",
                    }
                }
            if path == "/fills-v3":
                fills_queries.append(kwargs.get("query_params") or {})
                return {
                    "data": {
                        "fills": [
                            {
                                "id": "fill-1",
                                "orderId": order_id,
                                "marketHash": MARKET_HASH,
                                "fillAmount": "5000000",
                                "fillOdds": "50000000000000000000",
                                "isBettingOutcomeOne": True,
                                "createdAt": "2026-08-20T10:00:00Z",
                            }
                        ]
                    }
                }
            if path == "/positions-v3":
                return {
                    "data": {
                        "positions": [
                            {
                                "marketHash": MARKET_HASH,
                                "maxWin": "5000000",
                                "maxLoss": "-5000000",
                                "isOutcomeOneMaxWin": True,
                            }
                        ]
                    }
                }
            raise AssertionError(path)

        client._request_json = AsyncMock(side_effect=request)  # type: ignore[method-assign]
        report = await client.get_order(order_id)
        fills = await client.list_fills()
        positions = await client.get_positions()

        self.assertEqual(report.status, ExecutionStatus.FILLED)
        self.assertEqual(report.amount_filled, Decimal("10"))
        self.assertEqual(fills[0].quantity, Decimal("10"))
        self.assertEqual(positions, {"yes-token": Decimal("10")})
        self.assertEqual(fills_queries[0]["orderId"], order_id)
        await client.close()

    async def test_order_reconciliation_reconstructs_remote_state_after_restart(self) -> None:
        client = self._client_with_book()
        order_id = "0x" + ("6" * 64)

        async def request(method: str, path: str, **kwargs: Any) -> Any:
            del method, kwargs
            if path == f"/orders-v3/{order_id}":
                return {
                    "data": {
                        "id": order_id,
                        "marketHash": MARKET_HASH,
                        "totalBetSize": "5000000",
                        "percentageOdds": "50000000000000000000",
                        "isBettingOutcomeOne": True,
                        "createdAt": "2026-08-20T10:00:00Z",
                        "status": "INACTIVE",
                        "inactiveReason": "FILLED",
                    }
                }
            if path == "/fills-v3":
                return {
                    "data": {
                        "fills": [
                            {
                                "id": "fill-after-restart",
                                "orderId": order_id,
                                "fillAmount": "5000000",
                                "fillOdds": "50000000000000000000",
                            }
                        ]
                    }
                }
            raise AssertionError(path)

        client._request_json = AsyncMock(side_effect=request)  # type: ignore[method-assign]
        self.assertNotIn(order_id, client._submitted_orders)  # noqa: SLF001
        await client.restore_order_context(
            order_id,
            OrderIntent(
                client_order_id="restart-buy",
                route="Polymarket:SX Bet",
                market_key="restart-market",
                venue="SX Bet",
                token_id="yes-token",
                binary_side=BinarySide.YES,
                action="BUY",
                quantity=Decimal("10"),
                limit_price=Decimal("0.5"),
                venue_order_id=order_id,
                created_at=datetime(2026, 8, 20, 10, tzinfo=UTC),
            ),
        )

        report = await client.get_order(order_id)

        self.assertEqual(report.status, ExecutionStatus.FILLED)
        self.assertEqual(report.amount_requested, Decimal("10"))
        self.assertEqual(report.amount_filled, Decimal("10"))
        self.assertIn(order_id, client._submitted_orders)  # noqa: SLF001
        await client.close()

    async def test_restart_reconciliation_restores_sell_action_and_price(self) -> None:
        client = self._client_with_book()
        order_id = "0x" + ("8" * 64)

        async def request(method: str, path: str, **kwargs: Any) -> Any:
            del method, kwargs
            if path == "/user/fees-v3":
                return {"data": {"takerPayoutFee": "0.025", "refundFee": "0.01"}}
            if path == f"/orders-v3/{order_id}":
                return {
                    "data": {
                        "id": order_id,
                        "marketHash": MARKET_HASH,
                        "isBettingOutcomeOne": False,
                        "status": "INACTIVE",
                        "inactiveReason": "FILLED",
                    }
                }
            if path == "/fills-v3":
                return {
                    "data": {
                        "fills": [
                            {
                                "id": "fill-sell-after-restart",
                                "orderId": order_id,
                                "fillAmount": "6000000",
                                "fillOdds": "60000000000000000000",
                            }
                        ]
                    }
                }
            raise AssertionError(path)

        client._request_json = AsyncMock(side_effect=request)  # type: ignore[method-assign]
        await client.restore_order_context(
            order_id,
            OrderIntent(
                client_order_id="restart-sell",
                route="Polymarket:SX Bet",
                market_key="restart-market",
                venue="SX Bet",
                token_id="yes-token",
                binary_side=BinarySide.YES,
                action="SELL",
                quantity=Decimal("10"),
                limit_price=Decimal("0.4"),
                venue_order_id=order_id,
                created_at=datetime(2026, 8, 20, 10, tzinfo=UTC),
            ),
        )

        report = await client.get_order(order_id)

        self.assertEqual(report.status, ExecutionStatus.FILLED)
        self.assertEqual(report.amount_filled, Decimal("10"))
        self.assertEqual(report.avg_price, Decimal("0.39"))
        self.assertEqual(client._submitted_orders[order_id].action, "SELL")  # noqa: SLF001
        await client.close()

    async def test_filled_order_without_indexed_fills_remains_unknown(self) -> None:
        client = self._client_with_book()
        order_id = "0x" + ("7" * 64)
        client._submitted_orders[order_id] = _V3SubmittedOrder(  # noqa: SLF001
            order_id=order_id,
            market_hash=MARKET_HASH,
            token_id="yes-token",
            action="BUY",
            synthetic_side=BinarySide.YES,
            actual_side=BinarySide.YES,
            requested_contracts=Decimal("10"),
            requested_price=Decimal("0.5"),
            submitted_stake=Decimal("5"),
            submitted_at=datetime.now(UTC),
        )

        async def request(method: str, path: str, **kwargs: Any) -> Any:
            del method, kwargs
            if path == f"/orders-v3/{order_id}":
                return {
                    "data": {
                        "id": order_id,
                        "marketHash": MARKET_HASH,
                        "isBettingOutcomeOne": True,
                        "status": "INACTIVE",
                        "inactiveReason": "FILLED",
                    }
                }
            if path == "/fills-v3":
                return {"data": {"fills": []}}
            raise AssertionError(path)

        client._request_json = AsyncMock(side_effect=request)  # type: ignore[method-assign]
        with patch("arbitrage_engine.connectors.sx_bet_v3.asyncio.sleep", new=AsyncMock()):
            with self.assertRaisesRegex(RuntimeError, "fills are not indexed yet"):
                await client.get_order(order_id)
        self.assertEqual(client._request_json.await_count, 4)
        await client.close()

    async def test_fill_reconciliation_falls_back_when_order_id_filter_is_rejected(self) -> None:
        client = self._client_with_book()
        order_id = "0x" + ("4" * 64)
        submitted = _V3SubmittedOrder(
            order_id=order_id,
            market_hash=MARKET_HASH,
            token_id="yes-token",
            action="BUY",
            synthetic_side=BinarySide.YES,
            actual_side=BinarySide.YES,
            requested_contracts=Decimal("10"),
            requested_price=Decimal("0.5"),
            submitted_stake=Decimal("5"),
            submitted_at=datetime.now(UTC),
        )
        seen_queries: list[dict[str, Any]] = []

        async def request(method: str, path: str, **kwargs: Any) -> Any:
            del method
            if path == f"/orders-v3/{order_id}":
                return {
                    "data": {
                        "order": {
                            "id": order_id,
                            "marketHash": MARKET_HASH,
                            "isBettingOutcomeOne": True,
                            "status": "INACTIVE",
                            "inactiveReason": "FILLED",
                        }
                    }
                }
            if path == "/fills-v3":
                query = kwargs.get("query_params") or {}
                seen_queries.append(query)
                if "orderId" in query:
                    raise RuntimeError("SX Bet V3 GET /fills-v3 failed with 400: unsupported query")
                return {
                    "data": {
                        "fills": [
                            {
                                "id": "fill-fallback",
                                "orderId": order_id,
                                "fillAmount": "5000000",
                                "fillOdds": "50000000000000000000",
                            },
                            {
                                "id": "other-order",
                                "orderId": "0x" + ("5" * 64),
                                "fillAmount": "5000000",
                                "fillOdds": "50000000000000000000",
                            },
                        ]
                    }
                }
            raise AssertionError(path)

        client._submitted_orders[order_id] = submitted  # noqa: SLF001
        client._request_json = AsyncMock(side_effect=request)  # type: ignore[method-assign]
        report = await client.get_order(order_id)

        self.assertEqual(report.status, ExecutionStatus.FILLED)
        self.assertEqual(report.amount_filled, Decimal("10"))
        self.assertEqual(seen_queries[0]["orderId"], order_id)
        self.assertIn("startDate", seen_queries[1])
        await client.close()

    async def test_rest_requests_scope_v3_api_key_header_by_route(self) -> None:
        client = SxBetV3ApiClient(_v3_config())
        session_headers: dict[str, str] = {}
        request_headers: list[dict[str, str] | None] = []

        class Response:
            status = 200

            async def __aenter__(self) -> Response:
                return self

            async def __aexit__(
                self,
                exc_type: type[BaseException] | None,
                exc: BaseException | None,
                tb: TracebackType | None,
            ) -> bool:
                return False

            async def json(self, content_type: str | None = None) -> dict[str, Any]:
                del content_type
                return {"data": {}}

        class Session:
            closed = False

            def request(self, *args: Any, **kwargs: Any) -> Response:
                del args
                request_headers.append(kwargs.get("headers"))
                return Response()

            async def close(self) -> None:
                self.closed = True

        def session_factory(headers: dict[str, str] | None = None) -> Session:
            session_headers.update(headers or {})
            return Session()

        with patch("arbitrage_engine.connectors.sx_bet_v3.client_session", side_effect=session_factory):
            await client._request_json("GET", "/metadata/obv3")  # noqa: SLF001
            await client._request_json("GET", "/user/realtime-token-v3/api-key")  # noqa: SLF001
            await client._request_json("GET", "/user/balance-v3")  # noqa: SLF001
            await client._request_json("GET", "/trades-v3/public")  # noqa: SLF001
            await client._request_json("POST", "/heartbeat/v3")  # noqa: SLF001
            await client._request_json("GET", "/orderbook-v3/snapshot/event")  # noqa: SLF001

        self.assertEqual(
            session_headers,
            {"Accept": "application/json", "Content-Type": "application/json"},
        )
        self.assertEqual(
            request_headers,
            [
                None,
                {"x-sx-api-key": "v3-key"},
                {"x-sx-api-key": "v3-key"},
                None,
                {"x-sx-api-key": "v3-key"},
                {"x-sx-api-key": "v3-key"},
            ],
        )
        await client.close()

    async def test_metadata_domain_mismatch_fails_closed(self) -> None:
        client = SxBetV3ApiClient(_v3_config())
        malformed = _metadata()
        malformed["domain"] = {**malformed["domain"], "chainId": 1}
        client._request_json = AsyncMock(return_value={"data": malformed})  # type: ignore[method-assign]

        with self.assertRaisesRegex(RuntimeError, "domain chainId"):
            await client._metadata()  # noqa: SLF001
        await client.close()
