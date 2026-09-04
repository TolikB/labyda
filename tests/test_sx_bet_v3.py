from __future__ import annotations

import asyncio
import time
import unittest
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import TracebackType
from typing import Any
from unittest.mock import AsyncMock, MagicMock, call, patch

from arbitrage_engine.config import SxBetConfig
from arbitrage_engine.connectors.base import (
    OrderResidualExposure,
    OrderResidualExposureBatch,
    OrderSubmissionRejected,
)
from arbitrage_engine.connectors.sx_bet import SxBetApiClient, create_sx_bet_client
from arbitrage_engine.connectors.sx_bet_v3 import (
    SxBetV3ApiClient,
    SxBetV3HttpError,
    SxBetV3SubmissionUnknown,
    _order_book_from_v3_maker_snapshot,
    _report_from_v3_fills,
    _report_from_v3_outcome,
    _sign_v3_order,
    _V3SubmittedOrder,
)
from arbitrage_engine.models import (
    BinarySide,
    ExecutionReport,
    ExecutionStatus,
    MarketDataStatus,
    OrderIntent,
    OrderIntentStatus,
    VenueFeeQuote,
)

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


def _heartbeat_response(method: str, path: str, kwargs: dict[str, Any]) -> dict[str, Any] | None:
    if method != "POST" or path != "/heartbeat/v3":
        return None
    timeout_seconds = int(kwargs["json_body"]["timeoutSeconds"])
    return {
        "data": {
            "expiresAt": None if timeout_seconds == 0 else "2026-08-26T12:00:00Z",
        }
    }


class SxBetV3PureTests(unittest.TestCase):
    def test_v3_declares_pre_submit_order_id_persistence(self) -> None:
        self.assertTrue(SxBetV3ApiClient(_v3_config()).persists_order_id_before_submission())

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
            return_value=datetime(2026, 8, 26, 15, tzinfo=UTC) + timedelta(seconds=1),
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
            return_value=datetime(2026, 8, 26, 15, tzinfo=UTC) + timedelta(seconds=1),
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

    def test_target_readiness_uses_each_sx_token_timestamp(self) -> None:
        client = SxBetV3ApiClient(_v3_config())
        client.register_market("fresh-token", MARKET_HASH, BinarySide.YES)
        client.register_market("stale-token", MARKET_HASH, BinarySide.NO)
        client._tracked_tokens = {"fresh-token", "stale-token"}  # noqa: SLF001
        client._books = {  # noqa: SLF001
            "fresh-token": _order_book_from_v3_maker_snapshot(_book_payload(), BinarySide.YES),
            "stale-token": _order_book_from_v3_maker_snapshot(_book_payload(), BinarySide.NO),
        }
        now = time.monotonic()
        client._book_timestamps = {  # noqa: SLF001
            "fresh-token": now,
            "stale-token": now - 30,
        }

        aggregate_age = client.market_data_age_seconds()
        self.assertIsNotNone(aggregate_age)
        assert aggregate_age is not None
        self.assertLess(aggregate_age, 1.0)
        self.assertTrue(client.market_data_target_ready("fresh-token", 2.0))
        self.assertFalse(client.market_data_target_ready("stale-token", 2.0))

    def test_aggregated_maker_book_maps_both_binary_sides(self) -> None:
        yes = _order_book_from_v3_maker_snapshot(_book_payload(), BinarySide.YES)
        no = _order_book_from_v3_maker_snapshot(_book_payload(), BinarySide.NO)

        self.assertEqual(yes.best_bid.price, 0.4)
        self.assertEqual(yes.best_bid.size, 10.0)
        self.assertEqual(yes.best_ask.price, 0.5)
        self.assertEqual(yes.best_ask.size, 10.0)
        self.assertEqual(no.best_bid.price, 0.5)
        self.assertEqual(no.best_ask.price, 0.6)

    def test_orderbook_and_fill_units_follow_active_asset_decimals(self) -> None:
        book = _order_book_from_v3_maker_snapshot(
            _book_payload(),
            BinarySide.YES,
            asset_decimals=8,
        )
        submitted = _V3SubmittedOrder(
            order_id="0x" + ("9" * 64),
            market_hash=MARKET_HASH,
            token_id="yes-token",
            action="BUY",
            synthetic_side=BinarySide.YES,
            actual_side=BinarySide.YES,
            requested_contracts=Decimal("1"),
            requested_price=Decimal("0.5"),
            submitted_stake=Decimal("0.5"),
            submitted_at=datetime.now(UTC),
            asset_decimals=8,
        )
        report = _report_from_v3_fills(
            submitted,
            [
                {
                    "status": "LOCKED",
                    "fillAmount": "50000000",
                    "fillOdds": "50000000000000000000",
                }
            ],
            inactive_reason="FILLED",
        )

        self.assertEqual(book.best_bid.size, 0.1)
        self.assertEqual(book.best_ask.size, 0.1)
        self.assertEqual(report.amount_filled, Decimal("1"))

    def test_sell_report_uses_exact_net_ce_refund(self) -> None:
        submitted = _V3SubmittedOrder(
            order_id="0x" + ("a" * 64),
            market_hash=MARKET_HASH,
            token_id="yes-token",
            action="SELL",
            synthetic_side=BinarySide.YES,
            actual_side=BinarySide.NO,
            requested_contracts=Decimal("10"),
            requested_price=Decimal("0.4"),
            submitted_stake=Decimal("6"),
            submitted_at=datetime.now(UTC),
        )
        fill = {
            "id": "sell-fill",
            "status": "LOCKED",
            "fillAmount": "6000000",
            "fillOdds": "60000000000000000000",
            "ceRefundAmount": "9900000",
            "ceRefundFeeAmount": "100000",
        }

        report = _report_from_v3_fills(submitted, [fill], inactive_reason="FILLED")

        self.assertEqual(report.amount_filled, Decimal("10"))
        self.assertEqual(report.avg_price, Decimal("0.39"))
        with self.assertRaisesRegex(RuntimeError, "missing exact CE refund"):
            _report_from_v3_fills(
                submitted,
                [{key: value for key, value in fill.items() if not key.startswith("ceRefund")}],
                inactive_reason="FILLED",
            )

        partial_refund = {
            **fill,
            "ceRefundAmount": "4950000",
            "ceRefundFeeAmount": "50000",
        }
        with self.assertRaisesRegex(OrderResidualExposure, "residual opposite exposure") as raised:
            _report_from_v3_fills(submitted, [partial_refund], inactive_reason="FILLED")
        self.assertEqual(raised.exception.report.amount_filled, Decimal("5"))
        self.assertEqual(raised.exception.report.avg_price, Decimal("0.39"))
        self.assertEqual(raised.exception.residual_contracts, Decimal("5"))
        self.assertEqual(raised.exception.residual_side, BinarySide.NO)

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
        self.assertEqual(unknown.amount_filled, Decimal(0))
        self.assertEqual(missing.status, ExecutionStatus.OPEN)
        self.assertEqual(missing.amount_filled, Decimal(0))
        self.assertEqual(partial.status, ExecutionStatus.OPEN)
        self.assertEqual(partial.amount_filled, Decimal(0))
        with self.assertRaisesRegex(RuntimeError, "PARTIAL_FILL_DONE but has no fill"):
            _report_from_v3_outcome(submitted, {"state": "PARTIAL_FILL_DONE"})

    def test_only_locked_or_settled_fills_are_irreversible(self) -> None:
        submitted = _V3SubmittedOrder(
            order_id="0x" + ("7" * 64),
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
        matched = _report_from_v3_fills(
            submitted,
            [{"status": "MATCHED", "fillAmount": "5000000", "fillOdds": "50000000000000000000"}],
            inactive_reason="FILLED",
        )
        locked = _report_from_v3_fills(
            submitted,
            [{"status": "LOCKED", "fillAmount": "5000000", "fillOdds": "50000000000000000000"}],
            inactive_reason="FILLED",
        )
        failed = _report_from_v3_fills(
            submitted,
            [{"status": "FAILED", "fillAmount": "5000000", "fillOdds": "50000000000000000000"}],
            inactive_reason="FILLED",
        )

        self.assertEqual(matched.status, ExecutionStatus.OPEN)
        self.assertEqual(matched.amount_filled, Decimal(0))
        self.assertEqual(locked.status, ExecutionStatus.FILLED)
        self.assertEqual(locked.amount_filled, Decimal("10"))
        self.assertEqual(failed.status, ExecutionStatus.CANCELLED)
        self.assertEqual(failed.amount_filled, Decimal(0))
        with self.assertRaisesRegex(RuntimeError, "unsupported status MISSING"):
            _report_from_v3_fills(
                submitted,
                [{"fillAmount": "5000000", "fillOdds": "50000000000000000000"}],
                inactive_reason="FILLED",
            )

    def test_filled_order_waits_for_all_locked_stake_to_be_indexed(self) -> None:
        submitted = _V3SubmittedOrder(
            order_id="0x" + ("6" * 64),
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
        first_fill = {
            "status": "LOCKED",
            "fillAmount": "2000000",
            "fillOdds": "50000000000000000000",
        }
        second_fill = {
            "status": "LOCKED",
            "fillAmount": "3000000",
            "fillOdds": "50000000000000000000",
        }

        incomplete = _report_from_v3_fills(submitted, [first_fill], inactive_reason="FILLED")
        complete = _report_from_v3_fills(submitted, [first_fill, second_fill], inactive_reason="FILLED")

        self.assertEqual(incomplete.status, ExecutionStatus.OPEN)
        self.assertEqual(incomplete.amount_filled, Decimal("4"))
        self.assertGreater(incomplete.remaining_amount, 0)
        self.assertEqual(complete.status, ExecutionStatus.FILLED)
        self.assertEqual(complete.amount_filled, Decimal("10"))

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

    async def test_signed_order_depth_uses_rounded_ladder_bound(self) -> None:
        client = self._client_with_book()
        payload = _book_payload(version="00100000000000000000002")
        payload["outcomeTwo"] = [{"percentageOdds": "49950000000000000000", "size": "5000000"}]
        client._apply_book_snapshot(MARKET_HASH, payload)  # noqa: SLF001

        with self.assertRaisesRegex(RuntimeError, "insufficient executable depth"):
            await client._build_signed_order(  # noqa: SLF001
                token_id="yes-token",
                synthetic_side=BinarySide.YES,
                actual_side=BinarySide.YES,
                requested_contracts=Decimal("1"),
                requested_price=Decimal("0.5009"),
                action="BUY",
                book=client._books["yes-token"],  # noqa: SLF001
            )
        await client.close()

    async def test_preview_guarantees_only_fixed_stake_payout_at_limit_odds(self) -> None:
        client = self._client_with_book()
        payload = _book_payload(version="00100000000000000000004")
        payload["outcomeTwo"] = [{"percentageOdds": "60000000000000000000", "size": "6000000"}]
        client._apply_book_snapshot(MARKET_HASH, payload)  # noqa: SLF001

        async def request(method: str, path: str, **kwargs: Any) -> Any:
            del method, kwargs
            if path == "/user/proxy":
                return {"data": {"deployed": True, "obv3ProxyWalletAddress": ESCROW}}
            if path == "/user/fees-v3":
                return {"data": {"takerPayoutFee": "0.025", "refundFee": "0.01"}}
            raise AssertionError(path)

        client._request_json = AsyncMock(side_effect=request)  # type: ignore[method-assign]
        preview = await client.preview_buy(
            "yes-token",
            BinarySide.YES,
            Decimal("10"),
            Decimal("0.5"),
        )

        assert preview.payload_fingerprint is not None
        prepared = client._prepared_orders[preview.payload_fingerprint]  # noqa: SLF001
        self.assertEqual(preview.notional_usd, Decimal("4.0"))
        self.assertEqual(preview.maximum_notional_usd, Decimal("4"))
        self.assertEqual(preview.guaranteed_contracts, Decimal("8"))
        self.assertEqual(preview.maximum_fee_usd, Decimal("0.150"))
        at_limit = _report_from_v3_fills(
            prepared.submitted,
            [
                {
                    "status": "LOCKED",
                    "fillAmount": "4000000",
                    "fillOdds": "50000000000000000000",
                }
            ],
            inactive_reason="FILLED",
        )
        self.assertEqual(at_limit.amount_filled, preview.guaranteed_contracts)
        await client.close()

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
                response_payload = {"data": {**base_order, **lifecycle}}

                async def request(
                    method: str,
                    path: str,
                    response: dict[str, Any] = response_payload,
                    **kwargs: Any,
                ) -> Any:
                    heartbeat = _heartbeat_response(method, path, kwargs)
                    if heartbeat is not None:
                        return heartbeat
                    return response

                client._request_json = AsyncMock(side_effect=request)  # type: ignore[method-assign]
                with self.assertRaisesRegex(RuntimeError, expected_error):
                    await client.get_order(order_id)
                self.assertNotIn(order_id, client._reports)  # noqa: SLF001

        await client.close()

    async def test_fok_submission_uses_v3_endpoint_and_terminal_outcome(self) -> None:
        client = self._client_with_book()
        seen_body: dict[str, Any] = {}
        fill_statuses = iter(("MATCHED", "LOCKED"))
        heartbeat_timeouts: list[int] = []

        async def request(method: str, path: str, **kwargs: Any) -> Any:
            if method == "POST" and path == "/heartbeat/v3":
                timeout_seconds = int(kwargs["json_body"]["timeoutSeconds"])
                heartbeat_timeouts.append(timeout_seconds)
                return {
                    "data": {
                        "expiresAt": None if timeout_seconds == 0 else "2026-08-26T12:00:00Z",
                    }
                }
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
                                "clientOrderId": order["clientOrderId"],
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
            if method == "GET" and path.startswith("/orders-v3/"):
                return {
                    "data": {
                        "id": path.rsplit("/", 1)[-1],
                        "marketHash": MARKET_HASH,
                        "isBettingOutcomeOne": True,
                        "status": "INACTIVE",
                        "inactiveReason": "FILLED",
                    }
                }
            if method == "GET" and path == "/fills-v3":
                return {
                    "data": {
                        "fills": [
                            {
                                "id": "fill-locked-after-match",
                                "orderId": kwargs["query_params"]["orderId"],
                                "fillAmount": "5000000",
                                "fillOdds": "50000000000000000000",
                                "status": next(fill_statuses),
                            }
                        ]
                    }
                }
            raise AssertionError((method, path, kwargs))

        client._request_json = AsyncMock(side_effect=request)  # type: ignore[method-assign]
        order_id = await client.buy("yes-token", BinarySide.YES, 10.0, 0.5)
        self.assertEqual(client._reports[order_id].status, ExecutionStatus.OPEN)  # noqa: SLF001
        report = await client.wait_filled(order_id, 1_000)

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
        self.assertEqual(heartbeat_timeouts, [60, 0])
        await client.close()

    async def test_signed_order_id_is_persisted_before_post(self) -> None:
        client = self._client_with_book()
        events: list[tuple[str, str]] = []

        def pre_transport_guard() -> None:
            events.append(("guard", "ready"))

        async def persist_order_id(order_id: str) -> None:
            events.append(("persist", order_id))

        async def request(method: str, path: str, **kwargs: Any) -> Any:
            heartbeat = _heartbeat_response(method, path, kwargs)
            if heartbeat is not None:
                return heartbeat
            if path == "/user/proxy":
                return {"data": {"deployed": True, "obv3ProxyWalletAddress": ESCROW}}
            if path == "/user/fees-v3":
                return {"data": {"takerPayoutFee": "0.025", "refundFee": "0.01"}}
            if method == "POST" and path == "/orders-v3":
                order = kwargs["json_body"]["orders"][0]
                account = __import__("eth_account").Account.from_key(PRIVATE_KEY)
                _, order_id = _sign_v3_order(account, _metadata()["domain"], order)
                kwargs["before_request"]()
                events.append(("post", order_id))
                self.assertEqual(
                    events,
                    [("persist", order_id), ("guard", "ready"), ("post", order_id)],
                )
                return {
                    "data": {
                        "orders": [
                            {
                                "orderId": order_id,
                                "clientOrderId": order["clientOrderId"],
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
            raise AssertionError((method, path, kwargs))

        client._request_json = AsyncMock(side_effect=request)  # type: ignore[method-assign]
        order_id = await client.buy_with_order_id_persistence(
            "yes-token",
            BinarySide.YES,
            10.0,
            0.5,
            persist_order_id=persist_order_id,
            pre_transport_guard=pre_transport_guard,
            client_order_id="durable-client-id",
        )

        self.assertEqual(
            events,
            [("persist", order_id), ("guard", "ready"), ("post", order_id)],
        )
        post_call = next(
            item
            for item in client._request_json.await_args_list  # noqa: SLF001
            if item.args[:2] == ("POST", "/orders-v3")
        )
        posted_order = post_call.kwargs["json_body"]["orders"][0]
        self.assertEqual(posted_order["clientOrderId"], "durable-client-id")
        await client.close()

    async def test_execution_submits_the_exact_one_time_signed_preview(self) -> None:
        client = self._client_with_book()
        posted_order: dict[str, Any] = {}

        async def request(method: str, path: str, **kwargs: Any) -> Any:
            heartbeat = _heartbeat_response(method, path, kwargs)
            if heartbeat is not None:
                return heartbeat
            if path == "/user/proxy":
                return {"data": {"deployed": True, "obv3ProxyWalletAddress": ESCROW}}
            if path == "/user/fees-v3":
                return {"data": {"takerPayoutFee": "0.025", "refundFee": "0.01"}}
            if method == "POST" and path == "/orders-v3":
                posted_order.update(kwargs["json_body"]["orders"][0])
                account = __import__("eth_account").Account.from_key(PRIVATE_KEY)
                _, order_id = _sign_v3_order(account, _metadata()["domain"], posted_order)
                return {
                    "data": {
                        "orders": [
                            {
                                "orderId": order_id,
                                "clientOrderId": posted_order["clientOrderId"],
                                "status": "SUBMITTED",
                                "outcome": {"state": "NO_LIQUIDITY", "fillAmount": "0"},
                            }
                        ]
                    }
                }
            raise AssertionError((method, path, kwargs))

        client._request_json = AsyncMock(side_effect=request)  # type: ignore[method-assign]
        preview = await client.preview_buy(
            "yes-token",
            BinarySide.YES,
            Decimal("10"),
            Decimal("0.5"),
        )
        assert preview.payload_fingerprint is not None
        prepared = client._prepared_orders[preview.payload_fingerprint]  # noqa: SLF001
        expected_payload = dict(prepared.payload)
        claimed = client.claim_prepared_order(
            preview.payload_fingerprint,
            token_id="yes-token",
            side=BinarySide.YES,
            contracts=Decimal("10"),
            limit_price=Decimal("0.5"),
            action="BUY",
            submission_deadline_unix=time.time() + 60,
        )
        self.assertEqual(claimed, preview.payload_fingerprint)
        client._claimed_prepared_orders[preview.payload_fingerprint] = replace(  # noqa: SLF001
            prepared,
            prepared_at_monotonic=time.monotonic() - 100,
        )

        client._books["yes-token"] = _order_book_from_v3_maker_snapshot(  # noqa: SLF001
            {**_book_payload(), "outcomeTwo": []},
            BinarySide.YES,
        )
        order_id = await client.buy_with_order_id_persistence(
            "yes-token",
            BinarySide.YES,
            10.0,
            0.5,
            persist_order_id=AsyncMock(),
            client_order_id="durable-client-id",
            prepared_order_fingerprint=preview.payload_fingerprint,
            submission_deadline_unix=time.time() + 60,
        )

        self.assertEqual(order_id, prepared.submitted.order_id)
        self.assertEqual(posted_order["salt"], expected_payload["salt"])
        self.assertEqual(posted_order["orderSignature"], expected_payload["orderSignature"])
        self.assertEqual(posted_order["totalBetSize"], expected_payload["totalBetSize"])
        self.assertEqual(posted_order["percentageOdds"], expected_payload["percentageOdds"])
        self.assertEqual(posted_order["clientOrderId"], "durable-client-id")
        self.assertNotIn(preview.payload_fingerprint, client._prepared_orders)  # noqa: SLF001
        with self.assertRaisesRegex(OrderSubmissionRejected, "already consumed"):
            await client.buy_with_order_id_persistence(
                "yes-token",
                BinarySide.YES,
                10.0,
                0.5,
                persist_order_id=AsyncMock(),
                prepared_order_fingerprint=preview.payload_fingerprint,
                submission_deadline_unix=time.time() + 60,
            )
        await client.close()

    async def test_cutoff_crossed_during_persistence_blocks_order_post(self) -> None:
        client = self._client_with_book()
        post_called = False
        clock = [1_000.0]

        async def request(method: str, path: str, **kwargs: Any) -> Any:
            nonlocal post_called
            heartbeat = _heartbeat_response(method, path, kwargs)
            if heartbeat is not None:
                return heartbeat
            if path == "/user/proxy":
                return {"data": {"deployed": True, "obv3ProxyWalletAddress": ESCROW}}
            if path == "/user/fees-v3":
                return {"data": {"takerPayoutFee": "0.025", "refundFee": "0.01"}}
            if method == "POST" and path == "/orders-v3":
                post_called = True
            raise AssertionError((method, path, kwargs))

        async def persist_order_id(order_id: str) -> None:
            self.assertTrue(order_id.startswith("0x"))
            clock[0] = 1_006.0

        client._request_json = AsyncMock(side_effect=request)  # type: ignore[method-assign]
        preview = await client.preview_buy(
            "yes-token",
            BinarySide.YES,
            Decimal("10"),
            Decimal("0.5"),
        )
        assert preview.payload_fingerprint is not None

        with (
            patch("arbitrage_engine.connectors.sx_bet_v3.time.time", side_effect=lambda: clock[0]),
            self.assertRaisesRegex(OrderSubmissionRejected, "transport-start allowance"),
        ):
            await client.buy_with_order_id_persistence(
                "yes-token",
                BinarySide.YES,
                10.0,
                0.5,
                persist_order_id=persist_order_id,
                prepared_order_fingerprint=preview.payload_fingerprint,
                submission_deadline_unix=1_030.0,
            )

        self.assertFalse(post_called)
        self.assertEqual(client._submitted_orders, {})  # noqa: SLF001
        await client.close()

    def test_submission_window_reserves_connection_start_allowance(self) -> None:
        order_id = "0x" + ("b" * 64)
        with patch("arbitrage_engine.connectors.sx_bet_v3.time.time", return_value=1_000.0):
            with self.assertRaisesRegex(OrderSubmissionRejected, "transport-start allowance"):
                SxBetV3ApiClient._assert_submission_window(order_id, 1_025.0)  # noqa: SLF001
            SxBetV3ApiClient._assert_submission_window(order_id, 1_026.0)  # noqa: SLF001

    async def test_validation_http_failure_is_definitive_non_acceptance(self) -> None:
        client = self._client_with_book()
        recovered_order_reads = 0

        async def request(method: str, path: str, **kwargs: Any) -> Any:
            nonlocal recovered_order_reads
            heartbeat = _heartbeat_response(method, path, kwargs)
            if heartbeat is not None:
                return heartbeat
            if path == "/user/proxy":
                return {"data": {"deployed": True, "obv3ProxyWalletAddress": ESCROW}}
            if path == "/user/fees-v3":
                return {"data": {"takerPayoutFee": "0.025", "refundFee": "0.01"}}
            if method == "POST" and path == "/orders-v3":
                raise SxBetV3HttpError(method, path, 422)
            if method == "GET" and path.startswith("/orders-v3/"):
                recovered_order_reads += 1
            raise AssertionError((method, path, kwargs))

        client._request_json = AsyncMock(side_effect=request)  # type: ignore[method-assign]
        with self.assertRaisesRegex(OrderSubmissionRejected, "HTTP 422"):
            await client.buy("yes-token", BinarySide.YES, 10.0, 0.5)

        self.assertEqual(recovered_order_reads, 0)
        self.assertEqual(client._submitted_orders, {})  # noqa: SLF001
        await client.close()

    async def test_failed_order_id_persistence_prevents_post(self) -> None:
        client = self._client_with_book()
        post_called = False

        async def persist_order_id(order_id: str) -> None:
            del order_id
            raise RuntimeError("database unavailable")

        async def request(method: str, path: str, **kwargs: Any) -> Any:
            nonlocal post_called
            heartbeat = _heartbeat_response(method, path, kwargs)
            if heartbeat is not None:
                return heartbeat
            if path == "/user/proxy":
                return {"data": {"deployed": True, "obv3ProxyWalletAddress": ESCROW}}
            if path == "/user/fees-v3":
                return {"data": {"takerPayoutFee": "0.025", "refundFee": "0.01"}}
            if method == "POST" and path == "/orders-v3":
                post_called = True
            raise AssertionError((method, path))

        client._request_json = AsyncMock(side_effect=request)  # type: ignore[method-assign]
        with self.assertRaisesRegex(RuntimeError, "database unavailable"):
            await client.buy_with_order_id_persistence(
                "yes-token",
                BinarySide.YES,
                10.0,
                0.5,
                persist_order_id=persist_order_id,
            )

        self.assertFalse(post_called)
        self.assertEqual(client._submitted_orders, {})  # noqa: SLF001
        await client.close()

    async def test_malformed_heartbeat_blocks_order_submission(self) -> None:
        client = self._client_with_book()
        post_called = False

        async def request(method: str, path: str, **kwargs: Any) -> Any:
            nonlocal post_called
            if path == "/user/proxy":
                return {"data": {"deployed": True, "obv3ProxyWalletAddress": ESCROW}}
            if path == "/user/fees-v3":
                return {"data": {"takerPayoutFee": "0.025", "refundFee": "0.01"}}
            if method == "POST" and path == "/heartbeat/v3":
                return {"data": {}}
            if method == "POST" and path == "/orders-v3":
                post_called = True
            raise AssertionError((method, path, kwargs))

        client._request_json = AsyncMock(side_effect=request)  # type: ignore[method-assign]
        with self.assertRaisesRegex(RuntimeError, "missing expiresAt"):
            await client.buy("yes-token", BinarySide.YES, 10.0, 0.5)

        self.assertFalse(post_called)
        self.assertFalse(client._heartbeat_armed)  # noqa: SLF001
        self.assertEqual(client._submitted_orders, {})  # noqa: SLF001
        await client.close()

    async def test_heartbeat_transitions_are_serialized_across_concurrent_orders(self) -> None:
        client = self._client_with_book()
        old_order_id = "0x" + ("a" * 64)
        new_order_id = "0x" + ("b" * 64)
        old_submitted = _V3SubmittedOrder(
            order_id=old_order_id,
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
        new_submitted = replace(old_submitted, order_id=new_order_id)
        client._submitted_orders[old_order_id] = old_submitted  # noqa: SLF001
        client._reports[old_order_id] = ExecutionReport.from_amounts(  # noqa: SLF001
            old_order_id,
            Decimal("10"),
            Decimal(0),
            "cancelled",
        )
        client._heartbeat_armed = True  # noqa: SLF001
        zero_started = asyncio.Event()
        finish_zero = asyncio.Event()
        remote_timeout = 60
        heartbeat_timeouts: list[int] = []

        async def request(method: str, path: str, **kwargs: Any) -> Any:
            nonlocal remote_timeout
            self.assertEqual((method, path), ("POST", "/heartbeat/v3"))
            timeout_seconds = int(kwargs["json_body"]["timeoutSeconds"])
            heartbeat_timeouts.append(timeout_seconds)
            if timeout_seconds == 0:
                zero_started.set()
                await finish_zero.wait()
            remote_timeout = timeout_seconds
            return {
                "data": {
                    "expiresAt": None if timeout_seconds == 0 else "2026-08-26T12:00:00Z",
                }
            }

        client._request_json = AsyncMock(side_effect=request)  # type: ignore[method-assign]
        disarm_task = asyncio.create_task(client._disarm_account_heartbeat_if_safe())  # noqa: SLF001
        await zero_started.wait()
        track_task = asyncio.create_task(client._track_submitted_order(new_submitted))  # noqa: SLF001
        await asyncio.sleep(0)
        self.assertFalse(track_task.done())
        finish_zero.set()
        await asyncio.gather(disarm_task, track_task)

        self.assertEqual(heartbeat_timeouts, [0, 60])
        self.assertEqual(remote_timeout, 60)
        self.assertTrue(client._heartbeat_armed)  # noqa: SLF001
        self.assertIn(new_order_id, client._submitted_orders)  # noqa: SLF001
        await client.close()

    async def test_mismatched_client_order_id_is_an_unknown_submission(self) -> None:
        client = self._client_with_book()

        async def request(method: str, path: str, **kwargs: Any) -> Any:
            heartbeat = _heartbeat_response(method, path, kwargs)
            if heartbeat is not None:
                return heartbeat
            if path == "/user/proxy":
                return {"data": {"deployed": True, "obv3ProxyWalletAddress": ESCROW}}
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
                                "clientOrderId": "different-durable-id",
                                "status": "SUBMITTED",
                            }
                        ]
                    }
                }
            if method == "GET" and path.startswith("/orders-v3/"):
                raise RuntimeError("not found")
            raise AssertionError((method, path, kwargs))

        client._request_json = AsyncMock(side_effect=request)  # type: ignore[method-assign]
        with self.assertRaisesRegex(SxBetV3SubmissionUnknown, "clientOrderId") as raised:
            await client.buy_with_order_id_persistence(
                "yes-token",
                BinarySide.YES,
                10.0,
                0.5,
                persist_order_id=AsyncMock(),
                client_order_id="durable-client-id",
            )

        self.assertIn(raised.exception.order_id, client._submitted_orders)  # noqa: SLF001
        self.assertTrue(client._heartbeat_armed)  # noqa: SLF001
        await client.close()

    async def test_ambiguous_cancel_is_confirmed_by_order_read(self) -> None:
        client = self._client_with_book()
        order_id = "0x" + ("a" * 64)

        async def request(method: str, path: str, **kwargs: Any) -> Any:
            del kwargs
            if method == "DELETE" and path == "/orders-v3":
                return {"data": {"cancelled": [], "notCancelled": [{"orderId": order_id, "reason": "NOT_FOUND"}]}}
            if method == "GET" and path == f"/orders-v3/{order_id}":
                return {
                    "data": {
                        "id": order_id,
                        "status": "INACTIVE",
                        "inactiveReason": "USER_REQUESTED",
                    }
                }
            if method == "GET" and path == "/fills-v3":
                return {"data": {"fills": []}}
            raise AssertionError((method, path))

        client._request_json = AsyncMock(side_effect=request)  # type: ignore[method-assign]
        await client.cancel_order(order_id)
        self.assertEqual(client._request_json.await_count, 3)
        await client.close()

    async def test_ambiguous_cancel_fails_closed_when_order_filled(self) -> None:
        client = self._client_with_book()
        order_id = "0x" + ("b" * 64)

        async def request(method: str, path: str, **kwargs: Any) -> Any:
            del kwargs
            if method == "DELETE" and path == "/orders-v3":
                return {"data": {"cancelled": [], "unconfirmed": [{"orderId": order_id}]}}
            if method == "GET" and path == f"/orders-v3/{order_id}":
                return {"data": {"id": order_id, "status": "INACTIVE", "inactiveReason": "FILLED"}}
            if method == "GET" and path == "/fills-v3":
                return {
                    "data": {
                        "fills": [
                            {
                                "orderId": order_id,
                                "fillAmount": "5000000",
                                "fillOdds": "50000000000000000000",
                                "status": "LOCKED",
                            }
                        ]
                    }
                }
            raise AssertionError((method, path))

        client._request_json = AsyncMock(side_effect=request)  # type: ignore[method-assign]
        with self.assertRaisesRegex(RuntimeError, "fill reconciliation is required"):
            await client.cancel_order(order_id)
        await client.close()

    async def test_inline_timeout_remains_open_for_venue_reconciliation(self) -> None:
        client = self._client_with_book()

        async def request(method: str, path: str, **kwargs: Any) -> Any:
            heartbeat = _heartbeat_response(method, path, kwargs)
            if heartbeat is not None:
                return heartbeat
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
                                "clientOrderId": order["clientOrderId"],
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
            heartbeat = _heartbeat_response(method, path, kwargs)
            if heartbeat is not None:
                return heartbeat
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
            heartbeat = _heartbeat_response(method, path, kwargs)
            if heartbeat is not None:
                return heartbeat
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
                                "clientOrderId": order["clientOrderId"],
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
        pending_available = "-1000000"

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
                                "pendingAvailableAmount": pending_available,
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

        self.assertEqual(details["balance_raw"], "24000000")
        self.assertEqual(details["balance"], 24.0)
        self.assertEqual(details["pending_available"], "-1")
        self.assertEqual(details["escrowed"], "5")
        self.assertEqual(details["pending_escrow"], "1")
        pending_available = "2000000"
        positive_pending_details = await client.get_cash_balance_details()
        self.assertEqual(positive_pending_details["balance"], 25.0)
        self.assertEqual(positive_pending_details["pending_available"], "2")
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

    async def test_balance_rejects_non_finite_available_amount(self) -> None:
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
                                "availableAmount": "Infinity",
                                "pendingAvailableAmount": "0",
                                "escrowedAmount": "0",
                                "pendingEscrowAmount": "0",
                            }
                        ]
                    }
                }
            raise AssertionError(path)

        client._request_json = AsyncMock(side_effect=request)  # type: ignore[method-assign]
        with self.assertRaisesRegex(RuntimeError, "invalid availableAmount"):
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
        position_queries: list[dict[str, Any]] = []
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
            heartbeat = _heartbeat_response(method, path, kwargs)
            if heartbeat is not None:
                return heartbeat
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
                                "status": "LOCKED",
                                "isBettingOutcomeOne": True,
                                "createdAt": "2026-08-20T10:00:00Z",
                            }
                        ]
                    }
                }
            if path == "/positions-v3":
                position_queries.append(kwargs.get("query_params") or {})
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
        self.assertEqual(position_queries, [{"status": "MATCHED,LOCKED", "perPage": 100}])
        await client.close()

    async def test_historical_ce_fill_accounting_is_independent_of_transient_context(self) -> None:
        client = self._client_with_book()
        client._request_json = AsyncMock(  # type: ignore[method-assign]
            return_value={
                "data": {
                    "fills": [
                        {
                            "id": "ce-fill-without-context",
                            "orderId": "0x" + ("c" * 64),
                            "fillAmount": "6000000",
                            "fillOdds": "60000000000000000000",
                            "ceRefundAmount": "9900000",
                            "ceRefundFeeAmount": "100000",
                            "isBettingOutcomeOne": False,
                            "status": "LOCKED",
                            "createdAt": "2026-08-20T10:00:00Z",
                        }
                    ]
                }
            }
        )

        fills = await client.list_fills()
        self.assertEqual(fills[0].quantity, Decimal("10"))
        self.assertEqual(fills[0].price, Decimal("0.39"))
        await client.close()

    async def test_historical_ce_fill_without_outcome_side_fails_closed(self) -> None:
        client = self._client_with_book()
        client._request_json = AsyncMock(  # type: ignore[method-assign]
            return_value={
                "data": {
                    "fills": [
                        {
                            "id": "ce-fill-without-side",
                            "orderId": "0x" + ("f" * 64),
                            "fillAmount": "6000000",
                            "fillOdds": "60000000000000000000",
                            "ceRefundAmount": "9900000",
                            "ceRefundFeeAmount": "100000",
                            "status": "LOCKED",
                            "createdAt": "2026-08-20T10:00:00Z",
                        }
                    ]
                }
            }
        )

        with self.assertRaisesRegex(RuntimeError, "missing isBettingOutcomeOne"):
            await client.list_fills()
        await client.close()

    async def test_restored_terminal_sell_context_detects_zero_refund_residual_without_heartbeat(self) -> None:
        client = self._client_with_book()
        order_id = "0x" + ("d" * 64)
        second_order_id = "0x" + ("e" * 64)

        async def request(method: str, path: str, **kwargs: Any) -> Any:
            del method, kwargs
            if path == "/user/fees-v3":
                return {"data": {"takerPayoutFee": "0.025", "refundFee": "0.01"}}
            if path == "/fills-v3":
                return {
                    "data": {
                        "fills": [
                            {
                                "id": "zero-refund-sell",
                                "orderId": order_id,
                                "fillAmount": "6000000",
                                "fillOdds": "60000000000000000000",
                                "ceRefundAmount": "0",
                                "ceRefundFeeAmount": "0",
                                "isBettingOutcomeOne": False,
                                "status": "LOCKED",
                                "createdAt": "2026-08-20T10:00:00Z",
                            },
                            {
                                "id": "partial-refund-sell",
                                "orderId": second_order_id,
                                "fillAmount": "6000000",
                                "fillOdds": "60000000000000000000",
                                "ceRefundAmount": "4950000",
                                "ceRefundFeeAmount": "50000",
                                "isBettingOutcomeOne": False,
                                "status": "LOCKED",
                                "createdAt": "2026-08-20T10:00:01Z",
                            }
                        ]
                    }
                }
            raise AssertionError(path)

        client._request_json = AsyncMock(side_effect=request)  # type: ignore[method-assign]
        await client.restore_fill_context(
            order_id,
            OrderIntent(
                client_order_id="terminal-sell",
                route="polymarket_sx",
                market_key="market",
                venue="SX Bet",
                token_id="yes-token",
                binary_side=BinarySide.YES,
                action="SELL",
                quantity=Decimal("10"),
                limit_price=Decimal("0.4"),
                status=OrderIntentStatus.FILLED,
                venue_order_id=order_id,
            ),
        )
        await client.restore_fill_context(
            second_order_id,
            OrderIntent(
                client_order_id="terminal-partial-sell",
                route="polymarket_sx",
                market_key="market-two",
                venue="SX Bet",
                token_id="yes-token",
                binary_side=BinarySide.YES,
                action="SELL",
                quantity=Decimal("10"),
                limit_price=Decimal("0.4"),
                status=OrderIntentStatus.FILLED,
                venue_order_id=second_order_id,
            ),
        )

        with self.assertRaises(OrderResidualExposureBatch) as raised:
            await client.list_fills()
        exposures = {item.order_id: item for item in raised.exception.exposures}
        self.assertEqual(exposures[order_id].report.amount_filled, Decimal(0))
        self.assertEqual(exposures[order_id].residual_contracts, Decimal("10"))
        self.assertEqual(exposures[second_order_id].report.amount_filled, Decimal("5"))
        self.assertEqual(exposures[second_order_id].residual_contracts, Decimal("5"))
        self.assertEqual(len(raised.exception.fills), 2)
        self.assertIn(order_id, client._historical_order_contexts)  # noqa: SLF001
        self.assertIn(second_order_id, client._historical_order_contexts)  # noqa: SLF001
        self.assertNotIn(order_id, client._submitted_orders)  # noqa: SLF001
        self.assertFalse(client._heartbeat_armed)  # noqa: SLF001
        await client.close()

    async def test_v3_record_pagination_fails_closed_on_repeated_cursor(self) -> None:
        client = self._client_with_book()
        client._request_json = AsyncMock(  # type: ignore[method-assign]
            return_value={"data": {"orders": [], "nextKey": "repeated-cursor"}}
        )

        with self.assertRaisesRegex(RuntimeError, "pagination repeated a cursor"):
            await client._list_v3_records("/orders-v3", "orders")  # noqa: SLF001

        self.assertEqual(client._request_json.await_count, 2)
        await client.close()

    async def test_order_reconciliation_reconstructs_remote_state_after_restart(self) -> None:
        client = self._client_with_book()
        order_id = "0x" + ("6" * 64)

        async def request(method: str, path: str, **kwargs: Any) -> Any:
            heartbeat = _heartbeat_response(method, path, kwargs)
            if heartbeat is not None:
                return heartbeat
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
                                "status": "LOCKED",
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
                limit_price=Decimal("0.6"),
                venue_order_id=order_id,
                created_at=datetime(2026, 8, 20, 10, tzinfo=UTC),
            ),
        )
        self.assertEqual(client._submitted_orders[order_id].submitted_stake, Decimal("6"))  # noqa: SLF001
        self.assertFalse(client._submitted_orders[order_id].submitted_stake_verified)  # noqa: SLF001

        report = await client.get_order(order_id)

        self.assertEqual(report.status, ExecutionStatus.FILLED)
        self.assertEqual(report.amount_requested, Decimal("10"))
        self.assertEqual(report.amount_filled, Decimal("10"))
        self.assertIn(order_id, client._submitted_orders)  # noqa: SLF001
        self.assertEqual(client._submitted_orders[order_id].submitted_stake, Decimal("5"))  # noqa: SLF001
        self.assertTrue(client._submitted_orders[order_id].submitted_stake_verified)  # noqa: SLF001
        await client.close()

    async def test_restart_reconciliation_restores_sell_action_and_price(self) -> None:
        client = self._client_with_book()
        order_id = "0x" + ("8" * 64)

        async def request(method: str, path: str, **kwargs: Any) -> Any:
            heartbeat = _heartbeat_response(method, path, kwargs)
            if heartbeat is not None:
                return heartbeat
            if path == "/user/fees-v3":
                return {"data": {"takerPayoutFee": "0.025", "refundFee": "0.01"}}
            if path == f"/orders-v3/{order_id}":
                return {
                    "data": {
                        "id": order_id,
                        "marketHash": MARKET_HASH,
                        "totalBetSize": "6000000",
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
                                "ceRefundAmount": "9900000",
                                "ceRefundFeeAmount": "100000",
                                "status": "LOCKED",
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

    async def test_restart_reconciliation_requires_remote_total_bet_size(self) -> None:
        client = self._client_with_book()
        order_id = "0x" + ("a" * 64)

        async def request(method: str, path: str, **kwargs: Any) -> Any:
            heartbeat = _heartbeat_response(method, path, kwargs)
            if heartbeat is not None:
                return heartbeat
            return {
                "data": {
                    "id": order_id,
                    "marketHash": MARKET_HASH,
                    "isBettingOutcomeOne": True,
                    "status": "INACTIVE",
                    "inactiveReason": "FILLED",
                }
            }

        client._request_json = AsyncMock(side_effect=request)  # type: ignore[method-assign]
        await client.restore_order_context(
            order_id,
            OrderIntent(
                client_order_id="restart-missing-stake",
                route="Polymarket:SX Bet",
                market_key="restart-market",
                venue="SX Bet",
                token_id="yes-token",
                binary_side=BinarySide.YES,
                action="BUY",
                quantity=Decimal("10"),
                limit_price=Decimal("0.6"),
                venue_order_id=order_id,
                created_at=datetime(2026, 8, 20, 10, tzinfo=UTC),
            ),
        )

        with self.assertRaisesRegex(RuntimeError, "totalBetSize required after restart"):
            await client.get_order(order_id)
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
            heartbeat = _heartbeat_response(method, path, kwargs)
            if heartbeat is not None:
                return heartbeat
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
        self.assertEqual(client._request_json.await_count, 5)
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
            heartbeat = _heartbeat_response(method, path, kwargs)
            if heartbeat is not None:
                return heartbeat
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
                                "status": "LOCKED",
                            },
                            {
                                "id": "other-order",
                                "orderId": "0x" + ("5" * 64),
                                "fillAmount": "5000000",
                                "fillOdds": "50000000000000000000",
                                "status": "LOCKED",
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
        redirect_flags: list[bool | None] = []
        request_timeouts: list[Any] = []

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
                redirect_flags.append(kwargs.get("allow_redirects"))
                request_timeouts.append(kwargs.get("timeout"))
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
        self.assertEqual(redirect_flags, [False] * 6)
        self.assertTrue(all(timeout.total == 35 for timeout in request_timeouts))
        self.assertTrue(all(timeout.sock_read == 20 for timeout in request_timeouts))
        await client.close()

    async def test_http_start_guard_runs_inside_semaphore_before_request(self) -> None:
        client = SxBetV3ApiClient(_v3_config())
        request_count = 0

        class Session:
            closed = False

            def request(self, *args: Any, **kwargs: Any) -> Any:
                nonlocal request_count
                del args, kwargs
                request_count += 1
                raise AssertionError("request must remain blocked")

            async def close(self) -> None:
                self.closed = True

        def reject() -> None:
            raise OrderSubmissionRejected("deadline elapsed")

        with patch("arbitrage_engine.connectors.sx_bet_v3.client_session", return_value=Session()):
            with self.assertRaisesRegex(OrderSubmissionRejected, "deadline elapsed"):
                await client._request_json(  # noqa: SLF001
                    "POST",
                    "/orders-v3",
                    before_request=reject,
                )

        self.assertEqual(request_count, 0)
        await client.close()

    async def test_get_retries_rate_limit_and_transient_server_errors(self) -> None:
        client = SxBetV3ApiClient(_v3_config())
        responses: list[tuple[int, dict[str, Any], dict[str, str]]] = [
            (429, {"error": "rate limited"}, {"Retry-After": "1.25"}),
            (503, {"error": "unavailable"}, {}),
            (200, {"data": {"ok": True}}, {}),
        ]
        request_count = 0

        class Response:
            def __init__(self, status: int, payload: dict[str, Any], headers: dict[str, str]) -> None:
                self.status = status
                self._payload = payload
                self.headers = headers

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
                return self._payload

        class Session:
            closed = False

            def request(self, *args: Any, **kwargs: Any) -> Response:
                nonlocal request_count
                del args, kwargs
                status, payload, headers = responses[request_count]
                request_count += 1
                return Response(status, payload, headers)

            async def close(self) -> None:
                self.closed = True

        sleep = AsyncMock()
        with (
            patch("arbitrage_engine.connectors.sx_bet_v3.client_session", return_value=Session()),
            patch("arbitrage_engine.connectors.sx_bet_v3.asyncio.sleep", new=sleep),
        ):
            payload = await client._request_json("GET", "/orders-v3")  # noqa: SLF001

        self.assertEqual(payload, {"data": {"ok": True}})
        self.assertEqual(request_count, 3)
        self.assertEqual(sleep.await_args_list, [call(1.25), call(0.4)])
        await client.close()

    async def test_get_does_not_retry_when_retry_after_exceeds_operation_budget(self) -> None:
        client = SxBetV3ApiClient(_v3_config())
        request_count = 0

        class Response:
            status = 429
            headers = {"Retry-After": "30"}

            async def __aenter__(self) -> Response:
                return self

            async def __aexit__(
                self,
                exc_type: type[BaseException] | None,
                exc: BaseException | None,
                tb: TracebackType | None,
            ) -> bool:
                return False

        class Session:
            closed = False

            def request(self, *args: Any, **kwargs: Any) -> Response:
                nonlocal request_count
                del args, kwargs
                request_count += 1
                return Response()

            async def close(self) -> None:
                self.closed = True

        sleep = AsyncMock()
        with (
            patch("arbitrage_engine.connectors.sx_bet_v3.client_session", return_value=Session()),
            patch("arbitrage_engine.connectors.sx_bet_v3.asyncio.sleep", new=sleep),
        ):
            with self.assertRaises(SxBetV3HttpError) as raised:
                await client._request_json("GET", "/orders-v3")  # noqa: SLF001

        self.assertEqual(raised.exception.status, 429)
        self.assertEqual(request_count, 1)
        sleep.assert_not_awaited()
        await client.close()

    async def test_post_does_not_retry_transient_http_status(self) -> None:
        client = SxBetV3ApiClient(_v3_config())
        request_count = 0

        class Response:
            status = 503
            headers: dict[str, str] = {}

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
                return {"error": "unavailable"}

        class Session:
            closed = False

            def request(self, *args: Any, **kwargs: Any) -> Response:
                nonlocal request_count
                del args, kwargs
                request_count += 1
                return Response()

            async def close(self) -> None:
                self.closed = True

        sleep = AsyncMock()
        with (
            patch("arbitrage_engine.connectors.sx_bet_v3.client_session", return_value=Session()),
            patch("arbitrage_engine.connectors.sx_bet_v3.asyncio.sleep", new=sleep),
        ):
            with self.assertRaises(SxBetV3HttpError) as raised:
                await client._request_json("POST", "/heartbeat/v3")  # noqa: SLF001

        self.assertEqual(raised.exception.status, 503)
        self.assertEqual(request_count, 1)
        sleep.assert_not_awaited()
        await client.close()

    async def test_metadata_domain_mismatch_fails_closed(self) -> None:
        client = SxBetV3ApiClient(_v3_config())
        malformed = _metadata()
        malformed["domain"] = {**malformed["domain"], "chainId": 1}
        client._request_json = AsyncMock(return_value={"data": malformed})  # type: ignore[method-assign]

        with self.assertRaisesRegex(RuntimeError, "domain chainId"):
            await client._metadata()  # noqa: SLF001
        await client.close()
