import asyncio
import unittest
from datetime import UTC, datetime
from decimal import Decimal

from arbitrage_engine.config import OpinionConfig
from arbitrage_engine.connectors.base import OrderSubmissionRejected
from arbitrage_engine.connectors.opinion import (
    OpinionClient,
    apply_depth_diff,
    build_order_payload,
    execution_token,
    order_book_from_payload,
    outcome_side_code,
    parse_execution_token,
    side_from_outcome_code,
    unwrap_envelope,
)
from arbitrage_engine.models import (
    BinarySide,
    MarketDataStatus,
    OrderBook,
    OrderBookLevel,
    OrderIntent,
    OrderIntentStatus,
    VenueFeeQuote,
)

YES_TOKEN = "33095770954068818933468604332582424490740136703838404213332258128147961949614"
NO_TOKEN = "88015770954068818933468604332582424490740136703838404213332258128147961949611"


def make_config(**overrides: object) -> OpinionConfig:
    base = {
        "enabled": True,
        "api_base_url": "https://openapi.opinion.trade/openapi",
        "ws_url": "wss://ws.opinion.trade",
        "api_key": "test-api-key",
        "private_key": None,
        "rpc_url": "https://bsc-dataseed.binance.org",
        "rpc_urls": ["https://bsc-dataseed.binance.org"],
        "chain_id": 56,
        "clob_host": "https://proxy.opinion.trade:8443",
        "multi_sig_address": None,
        "conditional_tokens_address": None,
        "multisend_address": None,
        "collateral_token_address": None,
        "collateral_symbol": "USDT",
        "taker_fee_rate_bps": 400,
        "minimum_fee_usd": 0.25,
        "minimum_notional_usd": 5.0,
        "max_slippage_pct": 0.015,
    }
    base.update(overrides)
    return OpinionConfig(**base)  # type: ignore[arg-type]


class OpinionTokenTests(unittest.TestCase):
    def test_execution_token_round_trips_market_and_outcome_token(self) -> None:
        token = execution_token(813, YES_TOKEN)

        self.assertEqual(token, f"813:{YES_TOKEN}")
        self.assertEqual(parse_execution_token(token), (813, YES_TOKEN))

    def test_bare_outcome_token_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            parse_execution_token(YES_TOKEN)

    def test_non_numeric_market_id_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            parse_execution_token(f"market:{YES_TOKEN}")

    def test_outcome_side_codes_match_venue_semantics(self) -> None:
        self.assertEqual(outcome_side_code(BinarySide.YES), 1)
        self.assertEqual(outcome_side_code(BinarySide.NO), 2)
        self.assertIs(side_from_outcome_code(1), BinarySide.YES)
        self.assertIs(side_from_outcome_code("2"), BinarySide.NO)
        self.assertIsNone(side_from_outcome_code(3))


class OpinionEnvelopeTests(unittest.TestCase):
    def test_successful_envelope_returns_result(self) -> None:
        self.assertEqual(unwrap_envelope({"code": 0, "msg": "success", "result": {"a": 1}}, "/market"), {"a": 1})

    def test_error_envelope_raises_with_venue_message(self) -> None:
        with self.assertRaises(RuntimeError) as caught:
            unwrap_envelope({"code": 40001, "msg": "invalid token"}, "/token/orderbook")

        self.assertIn("code=40001", str(caught.exception))
        self.assertIn("invalid token", str(caught.exception))

    def test_non_dict_payload_is_rejected(self) -> None:
        with self.assertRaises(RuntimeError):
            unwrap_envelope([1, 2, 3], "/market")


class OpinionOrderBookTests(unittest.TestCase):
    def test_snapshot_orders_levels_and_keeps_venue_timestamp(self) -> None:
        book = order_book_from_payload(
            {
                "market": "0xcondition",
                "tokenId": YES_TOKEN,
                "timestamp": 1_700_000_000_000,
                "bids": [{"price": "0.41", "size": "80"}, {"price": "0.44", "size": "10"}],
                "asks": [{"price": "0.49", "size": "12"}, {"price": "0.46", "size": "5"}],
            }
        )

        self.assertEqual([level.price for level in book.bids], [0.44, 0.41])
        self.assertEqual([level.price for level in book.asks], [0.46, 0.49])
        self.assertEqual(book.timestamp, 1_700_000_000.0)
        self.assertIs(book.status, MarketDataStatus.VALID)

    def test_levels_outside_the_probability_range_are_dropped(self) -> None:
        book = order_book_from_payload(
            {"bids": [{"price": "0", "size": "5"}, {"price": "1.4", "size": "5"}], "asks": []}
        )

        self.assertEqual(list(book.bids), [])

    def test_depth_diff_replaces_and_removes_levels(self) -> None:
        cached = OrderBook(
            bids=[OrderBookLevel(0.44, 10.0)],
            asks=[OrderBookLevel(0.46, 5.0), OrderBookLevel(0.47, 9.0)],
        )

        updated = apply_depth_diff(
            cached,
            {
                "marketId": 813,
                "changes": [
                    {"side": "bids", "price": "0.45", "size": "7"},
                    {"side": "asks", "price": "0.46", "size": "0"},
                ],
            },
        )

        self.assertEqual([(level.price, level.size) for level in updated.bids], [(0.45, 7.0), (0.44, 10.0)])
        self.assertEqual([(level.price, level.size) for level in updated.asks], [(0.47, 9.0)])

    def test_depth_diff_without_a_side_invalidates_the_cached_book(self) -> None:
        cached = OrderBook(bids=[OrderBookLevel(0.44, 10.0)], asks=[OrderBookLevel(0.46, 5.0)])

        updated = apply_depth_diff(cached, {"changes": [{"price": "0.45", "size": "7"}]})

        self.assertIs(updated.status, MarketDataStatus.INVALID)

    def test_depth_diff_without_usable_entries_keeps_the_cached_book(self) -> None:
        cached = OrderBook(bids=[OrderBookLevel(0.44, 10.0)], asks=[OrderBookLevel(0.46, 5.0)])

        self.assertIs(apply_depth_diff(cached, {"changes": []}), cached)


class OpinionFeeTests(unittest.TestCase):
    def test_taker_curve_peaks_at_even_odds(self) -> None:
        quote = VenueFeeQuote("Opinion", 400, "opinion_curve", verified=True)

        even_odds = quote.fee_for_fill(Decimal(1000), Decimal("0.5"))
        long_shot = quote.fee_for_fill(Decimal(1000), Decimal("0.1"))

        self.assertEqual(even_odds, Decimal("10.0000"))
        self.assertEqual(long_shot, Decimal("3.600"))

    def test_minimum_trade_fee_floor_is_applied(self) -> None:
        quote = VenueFeeQuote(
            "Opinion",
            400,
            "opinion_curve",
            verified=True,
            minimum_fee_usd=Decimal("0.25"),
        )

        self.assertEqual(quote.fee_for_fill(Decimal(1), Decimal("0.5")), Decimal("0.25"))
        self.assertEqual(quote.fee_for_fill(Decimal(0), Decimal("0.5")), Decimal(0))


class OpinionOrderPayloadTests(unittest.TestCase):
    def test_payload_quantizes_price_and_derives_quote_notional(self) -> None:
        payload = build_order_payload(
            market_id=813,
            outcome_token=YES_TOKEN,
            action="buy",
            contracts=Decimal("10"),
            limit_price=Decimal("0.5537"),
            price_precision=2,
        )

        self.assertEqual(payload["side"], "BUY")
        self.assertEqual(payload["price"], "0.55")
        self.assertEqual(payload["makerAmountInQuoteToken"], "5.500000")
        self.assertEqual(payload["marketId"], 813)
        self.assertEqual(payload["tokenId"], YES_TOKEN)

    def test_out_of_range_price_and_size_are_rejected(self) -> None:
        for price, contracts in ((Decimal("1.2"), Decimal(1)), (Decimal("0.5"), Decimal(0))):
            with self.assertRaises(ValueError):
                build_order_payload(
                    market_id=813,
                    outcome_token=YES_TOKEN,
                    action="BUY",
                    contracts=contracts,
                    limit_price=price,
                    price_precision=2,
                )

    def test_unsupported_action_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            build_order_payload(
                market_id=813,
                outcome_token=YES_TOKEN,
                action="SPLIT",
                contracts=Decimal(1),
                limit_price=Decimal("0.5"),
                price_precision=2,
            )


class OpinionClientTests(unittest.IsolatedAsyncioTestCase):
    async def test_submission_without_a_signing_key_fails_closed(self) -> None:
        client = OpinionClient(make_config())

        with self.assertRaises(OrderSubmissionRejected):
            await client.buy(execution_token(813, YES_TOKEN), BinarySide.YES, 10.0, 0.55)

    async def test_submission_without_an_api_key_fails_closed(self) -> None:
        client = OpinionClient(make_config(api_key=None, private_key="11" * 32))

        with self.assertRaises(OrderSubmissionRejected):
            await client.buy(execution_token(813, YES_TOKEN), BinarySide.YES, 10.0, 0.55)

    async def test_preview_signature_is_unavailable_without_credentials(self) -> None:
        client = OpinionClient(make_config(api_key=None))

        signature = await client._preview_buy_signature(  # noqa: SLF001
            execution_token(813, YES_TOKEN),
            BinarySide.YES,
            Decimal(10),
            Decimal("0.55"),
            condition_id=None,
            tick_size=None,
            neg_risk=None,
        )

        self.assertIsNone(signature)

    async def test_prepared_order_claim_requires_the_matching_payload(self) -> None:
        client = OpinionClient(make_config(private_key="11" * 32))
        token = execution_token(813, YES_TOKEN)
        fingerprint = await client._preview_buy_signature(  # noqa: SLF001
            token,
            BinarySide.YES,
            Decimal(10),
            Decimal("0.55"),
            condition_id=None,
            tick_size=None,
            neg_risk=None,
        )
        assert fingerprint is not None

        claimed = client.claim_prepared_order(
            fingerprint,
            token_id=token,
            side=BinarySide.YES,
            contracts=Decimal(10),
            limit_price=Decimal("0.55"),
            action="BUY",
        )
        rejected = client.claim_prepared_order(
            fingerprint,
            token_id=token,
            side=BinarySide.YES,
            contracts=Decimal(11),
            limit_price=Decimal("0.55"),
            action="BUY",
        )

        self.assertEqual(claimed, fingerprint)
        self.assertIsNone(rejected)

    async def test_market_constraints_use_the_configured_venue_minimums(self) -> None:
        client = OpinionClient(make_config(price_precision=3, minimum_notional_usd=7.5))

        constraints = await client.get_market_constraints(execution_token(813, YES_TOKEN))

        assert constraints is not None
        self.assertEqual(constraints.tick_size, Decimal("0.001"))
        self.assertEqual(constraints.minimum_notional, Decimal("7.5"))
        self.assertEqual(constraints.fee_rate_bps, 400)

    async def test_fee_quote_is_verified_and_uses_the_opinion_curve(self) -> None:
        client = OpinionClient(make_config())

        quote = await client.get_fee_quote(execution_token(813, YES_TOKEN), Decimal("0.5"))

        assert quote is not None
        self.assertEqual(quote.model, "opinion_curve")
        self.assertTrue(quote.verified)
        self.assertEqual(quote.minimum_fee_usd, Decimal("0.25"))

    async def test_target_sync_drops_books_and_subscriptions_for_removed_tokens(self) -> None:
        client = OpinionClient(make_config())
        first = execution_token(813, YES_TOKEN)
        second = execution_token(900, NO_TOKEN)
        client.sync_market_data_targets({first, second})
        client._store_book(first, order_book_from_payload({"bids": [], "asks": []}))  # noqa: SLF001

        client.sync_market_data_targets({second})

        self.assertEqual(client._active_tokens(), {second})  # noqa: SLF001
        self.assertNotIn(first, client._books)  # noqa: SLF001
        self.assertEqual(client.active_market_data_target_count(), 1)

    async def test_depth_push_updates_only_the_subscribed_outcome_token(self) -> None:
        client = OpinionClient(make_config())
        yes_target = execution_token(813, YES_TOKEN)
        no_target = execution_token(813, NO_TOKEN)
        client.sync_market_data_targets({yes_target, no_target})

        client._handle_ws_payload(  # noqa: SLF001
            {
                "channel": "market.depth.diff",
                "data": {
                    "marketId": 813,
                    "tokenId": YES_TOKEN,
                    "bids": [{"price": "0.44", "size": "10"}],
                    "asks": [{"price": "0.46", "size": "5"}],
                },
            }
        )

        self.assertIn(yes_target, client._books)  # noqa: SLF001
        self.assertNotIn(no_target, client._books)  # noqa: SLF001

    async def test_market_data_is_not_ready_before_the_first_book_arrives(self) -> None:
        client = OpinionClient(make_config())
        target = execution_token(813, YES_TOKEN)
        client.sync_market_data_targets({target})

        self.assertFalse(client.market_data_ready())
        self.assertFalse(client.market_data_target_ready(target, 2.0))

        client._store_book(  # noqa: SLF001
            target,
            order_book_from_payload({"bids": [{"price": "0.4", "size": "1"}], "asks": []}),
        )

        self.assertTrue(client.market_data_ready())
        self.assertTrue(client.market_data_target_ready(target, 2.0))

    async def test_disconnect_marks_cached_books_stale(self) -> None:
        client = OpinionClient(make_config())
        target = execution_token(813, YES_TOKEN)
        client.sync_market_data_targets({target})
        client._store_book(  # noqa: SLF001
            target,
            order_book_from_payload({"bids": [{"price": "0.4", "size": "1"}], "asks": []}),
        )

        client._mark_books_stale()  # noqa: SLF001

        self.assertIs(client._books[target].status, MarketDataStatus.STALE)  # noqa: SLF001
        self.assertFalse(client.market_data_target_ready(target, 2.0))

    async def test_websocket_endpoint_carries_the_api_key(self) -> None:
        client = OpinionClient(make_config(ws_url="wss://ws.opinion.trade"))

        self.assertEqual(client._ws_endpoint(), "wss://ws.opinion.trade?apikey=test-api-key")  # noqa: SLF001

    async def test_open_orders_and_fills_are_mapped_from_paginated_results(self) -> None:
        client = OpinionClient(make_config(account_address="0xabc"))
        pages = {
            "/order": [
                {
                    "orderId": "order-1",
                    "shares": "10",
                    "filledShares": "4",
                    "price": "0.55",
                    "status": 1,
                    "createdAt": 1_700_000_000,
                }
            ],
            "/trade/user/0xabc": [
                {
                    "txHash": "0xfeed",
                    "orderId": "order-1",
                    "shares": "4",
                    "price": "0.55",
                    "fee": "0.25",
                    "status": 2,
                    "createdAt": 1_700_000_000,
                },
                {"txHash": "0xdead", "shares": "1", "price": "0.5", "status": 3},
            ],
        }

        async def fake_paginate(path: str, **_: object) -> list[dict[str, object]]:
            return pages.get(path, [])

        client._paginate = fake_paginate  # type: ignore[method-assign]  # noqa: SLF001

        orders = await client.list_open_orders()
        fills = await client.list_fills()

        self.assertEqual(len(orders), 1)
        self.assertEqual(orders[0].venue, "Opinion")
        self.assertIs(orders[0].status, OrderIntentStatus.ACKNOWLEDGED)
        self.assertEqual(orders[0].cumulative_filled, Decimal("4"))
        self.assertEqual([fill.fill_id for fill in fills], ["0xfeed"])
        self.assertEqual(fills[0].fee, Decimal("0.25"))

    async def test_fills_before_the_reconciliation_cursor_are_skipped(self) -> None:
        client = OpinionClient(make_config(account_address="0xabc"))

        async def fake_paginate(path: str, **_: object) -> list[dict[str, object]]:
            del path
            return [
                {"txHash": "0xold", "shares": "1", "price": "0.5", "status": 2, "createdAt": 1_600_000_000},
                {"txHash": "0xnew", "shares": "1", "price": "0.5", "status": 2, "createdAt": 1_700_000_000},
            ]

        client._paginate = fake_paginate  # type: ignore[method-assign]  # noqa: SLF001

        fills = await client.list_fills(since=datetime.fromtimestamp(1_650_000_000, tz=UTC))

        self.assertEqual([fill.fill_id for fill in fills], ["0xnew"])

    async def test_positions_are_keyed_by_the_execution_token(self) -> None:
        client = OpinionClient(make_config(account_address="0xabc"))

        async def fake_paginate(path: str, **_: object) -> list[dict[str, object]]:
            del path
            return [
                {"marketId": 813, "tokenId": YES_TOKEN, "sharesOwned": "12"},
                {"marketId": 813, "tokenId": YES_TOKEN, "sharesOwned": "3"},
                {"marketId": 813, "sharesOwned": "5"},
            ]

        client._paginate = fake_paginate  # type: ignore[method-assign]  # noqa: SLF001

        positions = await client.get_positions()

        self.assertEqual(positions, {execution_token(813, YES_TOKEN): Decimal("15")})

    async def test_reconciliation_requires_an_account_and_api_key(self) -> None:
        self.assertFalse(OpinionClient(make_config()).supports_full_reconciliation())
        self.assertTrue(
            OpinionClient(make_config(account_address="0xabc")).supports_full_reconciliation()
        )

    async def test_account_fingerprint_is_stable_and_non_reversible(self) -> None:
        client = OpinionClient(make_config(account_address="0xAbC"))

        fingerprint = client.reconciliation_account_fingerprint()

        self.assertIsNotNone(fingerprint)
        assert fingerprint is not None
        self.assertEqual(len(fingerprint), 32)
        self.assertNotIn("0xabc", fingerprint)
        self.assertEqual(fingerprint, client.reconciliation_account_fingerprint())

    async def test_depth_push_by_outcome_side_needs_cached_market_tokens(self) -> None:
        client = OpinionClient(make_config())
        yes_target = execution_token(813, YES_TOKEN)
        client.sync_market_data_targets({yes_target})
        push = {
            "channel": "market.depth.diff",
            "data": {
                "marketId": 813,
                "outcomeSide": 1,
                "bids": [{"price": "0.44", "size": "10"}],
                "asks": [{"price": "0.46", "size": "5"}],
            },
        }

        client._handle_ws_payload(push)  # noqa: SLF001
        self.assertNotIn(yes_target, client._books)  # noqa: SLF001

        client._remember_market_metadata(813, YES_TOKEN, BinarySide.YES)  # noqa: SLF001
        client._handle_ws_payload(push)  # noqa: SLF001

        self.assertIn(yes_target, client._books)  # noqa: SLF001

    async def test_restoring_an_intent_rebuilds_order_and_market_context(self) -> None:
        client = OpinionClient(make_config())
        intent = OrderIntent(
            client_order_id="intent-1",
            route="polymarket_opinion",
            market_key="market-key",
            venue="Opinion",
            token_id=execution_token(813, NO_TOKEN),
            binary_side=BinarySide.NO,
            action="BUY",
            quantity=Decimal("10"),
            limit_price=Decimal("0.55"),
        )

        await client.restore_order_context("order-1", intent)

        self.assertEqual(client.order_action("order-1"), "BUY")
        self.assertEqual(client.order_token("order-1"), execution_token(813, NO_TOKEN))
        metadata = client._market_metadata[813]  # noqa: SLF001
        self.assertEqual(metadata.no_token_id, NO_TOKEN)
        self.assertIs(metadata.side_for_token(NO_TOKEN), BinarySide.NO)

    async def test_forget_order_clears_restored_context(self) -> None:
        client = OpinionClient(make_config())
        intent = OrderIntent(
            client_order_id="intent-1",
            route="polymarket_opinion",
            market_key="market-key",
            venue="Opinion",
            token_id=execution_token(813, NO_TOKEN),
            binary_side=BinarySide.NO,
            action="BUY",
            quantity=Decimal("10"),
            limit_price=Decimal("0.55"),
        )
        await client.restore_fill_context("order-1", intent)

        client.forget_order("order-1")

        self.assertIsNone(client.order_action("order-1"))
        self.assertIsNone(client.order_token("order-1"))

    async def test_positions_resolve_the_token_from_the_numeric_outcome(self) -> None:
        client = OpinionClient(make_config(account_address="0xabc"))
        client._remember_market_metadata(813, YES_TOKEN, BinarySide.YES)  # noqa: SLF001

        async def fake_paginate(path: str, **_: object) -> list[dict[str, object]]:
            del path
            return [{"marketId": 813, "outcomeSide": 1, "sharesOwned": "4"}]

        client._paginate = fake_paginate  # type: ignore[method-assign]  # noqa: SLF001

        positions = await client.get_positions()

        self.assertEqual(positions, {execution_token(813, YES_TOKEN): Decimal("4")})

    async def test_positions_without_resolvable_outcome_are_skipped(self) -> None:
        client = OpinionClient(make_config(account_address="0xabc"))

        async def fake_paginate(path: str, **_: object) -> list[dict[str, object]]:
            del path
            return [{"marketId": 813, "outcomeSide": 1, "sharesOwned": "4"}]

        client._paginate = fake_paginate  # type: ignore[method-assign]  # noqa: SLF001

        self.assertEqual(await client.get_positions(), {})

    async def test_rate_limiter_paces_public_requests(self) -> None:
        from arbitrage_engine.connectors.opinion import _RateLimiter

        limiter = _RateLimiter(50.0)
        loop = asyncio.get_running_loop()
        started = loop.time()

        for _ in range(3):
            await limiter.acquire()

        self.assertGreaterEqual(loop.time() - started, 0.04)


if __name__ == "__main__":
    unittest.main()
