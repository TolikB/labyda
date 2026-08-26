import asyncio
import hashlib
import time
import unittest
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace, TracebackType
from unittest.mock import AsyncMock, MagicMock, patch

from arbitrage_engine.config import SxBetConfig
from arbitrage_engine.connectors.sx_bet import (
    SxBetApiClient,
    _order_book_from_orders,
    _submitted_from_trade,
    _trade_datetime,
    _trade_query_start,
)
from arbitrage_engine.models import (
    ArbitrageSignal,
    BinarySide,
    ExitSignal,
    MarketDataStatus,
    MarketSpec,
    OpenPosition,
    OrderBook,
    OrderBookLevel,
    PositionPlan,
    SettlementRequest,
    SpreadMetrics,
    market_supports_execution_route,
    myriad_execution_side_for_route,
    myriad_execution_token_for_route,
    route_execution_sides_are_complementary,
)


def _market() -> MarketSpec:
    return MarketSpec(
        symbol="market",
        target_label="target",
        polymarket_token_id="poly-token",
        polymarket_side=BinarySide.YES,
        predict_fun_token_id="second-token",
        predict_fun_side=BinarySide.NO,
        venue_a_label="Polymarket",
        venue_b_label="Predict.fun",
    )


def _sx_config() -> SxBetConfig:
    return SxBetConfig(
        enabled=True,
        api_base_url="https://api.sx.bet",
        api_key="api-key",
        private_key="0x" + ("1" * 64),
        rpc_url="https://rpc-rollup.sx.technology",
        rpc_urls=["https://rpc-rollup.sx.technology"],
        chain_id=4162,
        base_token_address="0x6629Ce1Cf35Cc1329ebB4F63202F3f197b3F050B",
        domain_version="6.0",
        odds_slippage=0,
    )


class ModelAliasTests(unittest.TestCase):
    def test_market_spec_exposes_neutral_second_leg_aliases(self) -> None:
        market = _market()

        self.assertEqual(market.first_venue_label, "Polymarket")
        self.assertEqual(market.second_venue_label, "Predict.fun")
        self.assertEqual(market.first_leg_token_id, "poly-token")
        self.assertEqual(market.second_leg_token_id, "second-token")
        self.assertEqual(market.first_leg_side, BinarySide.YES)
        self.assertEqual(market.second_leg_side, BinarySide.NO)

    def test_myriad_route_helpers_derive_execution_side_from_second_leg_routes(self) -> None:
        market = MarketSpec(
            symbol="Will France win the World Cup?",
            target_label="France",
            polymarket_token_id="poly-token",
            polymarket_side=BinarySide.YES,
            predict_fun_token_id="0xsx:NO",
            predict_fun_side=BinarySide.NO,
            venue_b_label="SX Bet",
            myriad_market_id="1335",
            myriad_side=BinarySide.NO,
        )

        self.assertEqual(myriad_execution_side_for_route(market, "polymarket_myriad"), BinarySide.NO)
        self.assertEqual(myriad_execution_side_for_route(market, "sx_myriad"), BinarySide.YES)
        self.assertEqual(myriad_execution_token_for_route(market, "sx_myriad"), "1335:YES")

    def test_route_shape_matrix_rejects_inconsistent_outcome_orientations(self) -> None:
        predict = MarketSpec(
            symbol="Shared event",
            target_label="YES",
            polymarket_token_id="poly-yes",
            polymarket_side=BinarySide.YES,
            predict_fun_token_id="predict-no",
            predict_fun_side=BinarySide.NO,
            venue_b_label="Predict.fun",
            myriad_market_id="myriad-market",
            myriad_side=BinarySide.NO,
        )
        sx = MarketSpec(
            symbol="Shared event",
            target_label="YES",
            polymarket_token_id="poly-yes",
            polymarket_side=BinarySide.YES,
            predict_fun_token_id="sx-no",
            predict_fun_side=BinarySide.NO,
            venue_b_label="SX Bet",
            myriad_market_id="myriad-market",
            myriad_side=BinarySide.NO,
        )
        predict_sx = MarketSpec(
            symbol="Shared event",
            target_label="YES",
            polymarket_token_id="predict-no",
            polymarket_side=BinarySide.NO,
            predict_fun_token_id="sx-yes",
            predict_fun_side=BinarySide.YES,
            venue_a_label="Predict.fun",
            venue_b_label="SX Bet",
        )
        valid = {
            "polymarket_myriad": predict,
            "polymarket_predict": predict,
            "predict_myriad": predict,
            "predict_sx": predict_sx,
            "polymarket_sx": sx,
            "sx_myriad": sx,
        }
        for route, market in valid.items():
            with self.subTest(route=route):
                self.assertTrue(market_supports_execution_route(market, route))
                self.assertTrue(route_execution_sides_are_complementary(market, route))

        inconsistent_myriad = replace(predict, myriad_side=BinarySide.YES)
        self.assertFalse(
            route_execution_sides_are_complementary(inconsistent_myriad, "polymarket_myriad")
        )
        self.assertFalse(
            route_execution_sides_are_complementary(inconsistent_myriad, "predict_myriad")
        )
        self.assertFalse(
            route_execution_sides_are_complementary(
                replace(predict, predict_fun_side=BinarySide.YES),
                "polymarket_predict",
            )
        )

    def test_position_and_signal_aliases_match_existing_predict_fun_fields(self) -> None:
        market = _market()
        plan = PositionPlan(
            polymarket_contracts=Decimal("12"),
            polymarket_capital_usd=Decimal("5.1"),
            predict_fun_contracts=Decimal("12"),
            predict_fun_capital_usd=Decimal("4.9"),
            payout_contracts=Decimal("12"),
            total_cost_usd=Decimal("10"),
            polymarket_fee_usd=Decimal("0.1"),
            predict_fun_fee_usd=Decimal("0.2"),
        )
        metrics = SpreadMetrics(
            gross_spread=0.08,
            net_spread=0.05,
            expected_net_profit_usd=0.6,
            polymarket_slippage=0.01,
            predict_fun_slippage=0.02,
            combined_cost_per_payout=0.95,
        )
        signal = ArbitrageSignal(
            market=market,
            plan=plan,
            metrics=metrics,
            polymarket_price=0.51,
            predict_fun_price=0.49,
        )
        position = OpenPosition(
            market=market,
            polymarket_contracts=Decimal("12"),
            polymarket_entry_price=Decimal("0.51"),
            predict_fun_contracts=Decimal("12"),
            predict_fun_entry_price=Decimal("0.49"),
            opened_at=datetime.now(UTC),
            polymarket_order_id="poly-order",
            predict_fun_order_id="second-order",
        )
        close_signal = ExitSignal(
            position=position,
            polymarket_exit_price=Decimal("0.55"),
            predict_fun_exit_price=Decimal("0.47"),
            profit_pct=0.03,
            profit_usd=Decimal("0.4"),
        )

        self.assertEqual(plan.second_leg_contracts, plan.predict_fun_contracts)
        self.assertEqual(plan.second_leg_capital_usd, plan.predict_fun_capital_usd)
        self.assertEqual(metrics.second_leg_slippage, metrics.predict_fun_slippage)
        self.assertEqual(signal.second_leg_price, signal.predict_fun_price)
        self.assertEqual(position.second_leg_order_id, position.predict_fun_order_id)
        self.assertEqual(position.second_leg_entry_price, position.predict_fun_entry_price)
        self.assertEqual(close_signal.second_leg_exit_price, close_signal.predict_fun_exit_price)


class SxBetOrderBookTests(unittest.TestCase):
    def test_order_book_maps_maker_orders_to_two_sided_binary_book(self) -> None:
        orders = [
            {
                "orderHash": "0xoutcome-one",
                "totalBetSize": "282680000",
                "fillAmount": "0",
                "pendingFillAmount": "0",
                "percentageOdds": "43125000000000000000",
                "isMakerBettingOutcomeOne": True,
            },
            {
                "orderHash": "0xoutcome-two",
                "totalBetSize": "294820000",
                "fillAmount": "0",
                "pendingFillAmount": "0",
                "percentageOdds": "46250000000000000000",
                "isMakerBettingOutcomeOne": False,
            },
        ]

        outcome_one_book = _order_book_from_orders(orders, BinarySide.YES)
        outcome_two_book = _order_book_from_orders(orders, BinarySide.NO)

        self.assertEqual(len(outcome_one_book.asks), 1)
        self.assertEqual(len(outcome_two_book.asks), 1)
        self.assertEqual(len(outcome_one_book.bids), 1)
        self.assertEqual(len(outcome_two_book.bids), 1)
        self.assertAlmostEqual(outcome_one_book.best_ask.price, 0.5375, places=6)
        self.assertAlmostEqual(outcome_one_book.best_bid.price, 0.43125, places=6)
        self.assertAlmostEqual(outcome_two_book.best_ask.price, 0.56875, places=6)
        self.assertAlmostEqual(outcome_two_book.best_bid.price, 0.4625, places=6)
        self.assertGreater(outcome_one_book.best_ask.size, 600)
        self.assertGreater(outcome_two_book.best_ask.size, 600)
        raw_payload = outcome_one_book.raw_payload
        self.assertIsInstance(raw_payload, dict)
        assert isinstance(raw_payload, dict)
        self.assertEqual(raw_payload["venue"], "SX Bet")
        self.assertEqual(raw_payload["synthetic_side"], "YES")
        self.assertEqual(len(raw_payload["orders"]), 2)


class SxBetClientTests(unittest.IsolatedAsyncioTestCase):
    async def test_websocket_subscriptions_follow_only_active_targets(self) -> None:
        client = SxBetApiClient(_sx_config())
        client._ws_connected = True
        client._ensure_ws_task = MagicMock()  # type: ignore[method-assign]
        client.register_market("inactive", "0xinactive", BinarySide.YES)

        self.assertTrue(client._subscription_queue.empty())

        client.sync_market_data_targets({"inactive"})

        self.assertEqual(client._subscription_queue.get_nowait(), ("subscribe", "0xinactive"))
        client.register_market("still-inactive", "0xother", BinarySide.YES)
        self.assertTrue(client._subscription_queue.empty())

    async def test_target_rotation_has_bounded_readiness_transition_after_healthy_window(self) -> None:
        client = SxBetApiClient(_sx_config())
        client._ensure_ws_task = MagicMock()  # type: ignore[method-assign]
        client.register_market("old-token", "0xold", BinarySide.YES)
        client.register_market("new-token", "0xnew", BinarySide.YES)
        client._ws_connected = True
        client._subscribed_markets.add("0xold")
        client.sync_market_data_targets({"old-token"})
        client._books["old-token"] = OrderBook(
            bids=[OrderBookLevel(0.40, 20)],
            asks=[OrderBookLevel(0.42, 20)],
        )
        client._book_timestamps["old-token"] = time.monotonic()

        self.assertTrue(client.market_data_ready())
        self.assertFalse(client.market_data_transitioning())

        client.sync_market_data_targets({"new-token"})

        self.assertFalse(client.market_data_ready())
        self.assertTrue(client.market_data_transitioning())
        client._target_transition_deadline = time.monotonic() - 1.0
        self.assertFalse(client.market_data_transitioning())

    async def test_target_rotation_uses_operational_quiet_window_for_bounded_transition(self) -> None:
        client = SxBetApiClient(_sx_config())
        client._ensure_ws_task = MagicMock()  # type: ignore[method-assign]
        client.register_market("old-liquid", "0xold-liquid", BinarySide.YES)
        client.register_market("old-quiet", "0xold-quiet", BinarySide.YES)
        client.register_market("new-token", "0xnew", BinarySide.YES)
        client._ws_connected = True
        client._subscribed_markets.update({"0xold-liquid", "0xold-quiet"})
        client.sync_market_data_targets({"old-liquid", "old-quiet"})
        client._books["old-liquid"] = OrderBook(
            bids=[OrderBookLevel(0.40, 20)],
            asks=[OrderBookLevel(0.42, 20)],
        )
        client._book_timestamps["old-liquid"] = time.monotonic()

        self.assertFalse(client.market_data_ready())
        self.assertIsNotNone(client.market_data_age_seconds())

        client.sync_market_data_targets({"new-token"})

        self.assertTrue(client.market_data_transitioning())

    async def test_target_rotation_without_prior_market_data_remains_fail_closed(self) -> None:
        client = SxBetApiClient(_sx_config())
        client._ensure_ws_task = MagicMock()  # type: ignore[method-assign]
        client.register_market("old-token", "0xold", BinarySide.YES)
        client.register_market("new-token", "0xnew", BinarySide.YES)
        client._ws_connected = True
        client._subscribed_markets.add("0xold")
        client.sync_market_data_targets({"old-token"})

        client.sync_market_data_targets({"new-token"})

        self.assertFalse(client.market_data_transitioning())

    async def test_target_transition_never_masks_initial_startup_or_disconnect(self) -> None:
        client = SxBetApiClient(_sx_config())
        client._ensure_ws_task = MagicMock()  # type: ignore[method-assign]
        client.register_market("token", "0xmarket", BinarySide.YES)
        client._ws_connected = True

        client.sync_market_data_targets({"token"})

        self.assertFalse(client.market_data_transitioning())
        client._target_transition_deadline = time.monotonic() + 60.0
        client._ws_connected = False
        self.assertFalse(client.market_data_transitioning())

    async def test_prime_market_data_targets_waits_for_first_book_event(self) -> None:
        client = SxBetApiClient(_sx_config())
        client._ensure_ws_task = MagicMock()  # type: ignore[method-assign]
        client.register_market("token", "0xmarket", BinarySide.YES)
        client._ws_connected = True
        client.sync_market_data_targets({"token"})

        async def publish_book() -> None:
            await asyncio.sleep(0)
            client._books["token"] = OrderBook(
                bids=[OrderBookLevel(0.40, 20)],
                asks=[OrderBookLevel(0.42, 20)],
            )
            client._book_timestamps["token"] = time.monotonic()
            client._book_events["token"].set()

        publisher = asyncio.create_task(publish_book())
        await client.prime_market_data_targets()
        await publisher

        self.assertIn("token", client._books)

    async def test_removing_one_outcome_keeps_shared_sx_subscription(self) -> None:
        client = SxBetApiClient(_sx_config())
        client._ws_connected = True
        client._ensure_ws_task = MagicMock()  # type: ignore[method-assign]
        client.register_market("yes-token", "0xmarket", BinarySide.YES)
        client.register_market("no-token", "0xmarket", BinarySide.NO)
        client.sync_market_data_targets({"yes-token", "no-token"})
        self.assertEqual(client._subscription_queue.get_nowait(), ("subscribe", "0xmarket"))

        client.sync_market_data_targets({"yes-token"})

        self.assertTrue(client._subscription_queue.empty())

        client.sync_market_data_targets(set())
        self.assertEqual(client._subscription_queue.get_nowait(), ("unsubscribe", "0xmarket"))

    async def test_removing_last_market_target_prunes_ephemeral_order_book_state(self) -> None:
        client = SxBetApiClient(_sx_config())
        client._ensure_ws_task = MagicMock()  # type: ignore[method-assign]
        client.register_market("yes-token", "0xmarket", BinarySide.YES)
        client.register_market("no-token", "0xmarket", BinarySide.NO)
        client.register_market("next-token", "0xnext", BinarySide.YES)
        client.sync_market_data_targets({"yes-token", "no-token"})
        order = {
            "orderHash": "0xmaker",
            "updateTime": 100,
            "status": "ACTIVE",
            "totalBetSize": "282680000",
            "fillAmount": "0",
            "pendingFillAmount": "0",
            "percentageOdds": "43125000000000000000",
            "isMakerBettingOutcomeOne": False,
        }
        client._orders_by_market["0xmarket"] = {"0xmaker": order}
        client._order_update_times["0xmaker"] = 100
        client._order_markets["0xmaker"] = "0xmarket"
        client._subscription_positions["order_book:market_0xmarket"] = ("epoch-1", 5)
        client._bootstrap_locks["0xmarket"] = asyncio.Lock()
        client._rebuild_market_books("0xmarket")

        client.sync_market_data_targets({"yes-token"})

        self.assertIn("0xmarket", client._orders_by_market)
        self.assertIn("no-token", client._books)

        client.sync_market_data_targets({"next-token"})

        self.assertNotIn("0xmarket", client._orders_by_market)
        self.assertNotIn("0xmaker", client._order_update_times)
        self.assertNotIn("0xmaker", client._order_markets)
        self.assertNotIn("order_book:market_0xmarket", client._subscription_positions)
        self.assertNotIn("0xmarket", client._bootstrap_locks)
        self.assertNotIn("yes-token", client._books)
        self.assertNotIn("no-token", client._books)
        self.assertNotIn("yes-token", client._book_events)
        self.assertNotIn("no-token", client._book_events)
        self.assertEqual(client._market_identifiers["yes-token"], ("0xmarket", BinarySide.YES))
        self.assertEqual(client._token_by_market_side[("0xmarket", BinarySide.NO)], "no-token")

    async def test_late_publication_cannot_repopulate_inactive_market_cache(self) -> None:
        client = SxBetApiClient(_sx_config())
        client.register_market("yes-token", "0xmarket", BinarySide.YES)
        publication = {
            "offset": 5,
            "data": [
                {
                    "orderHash": "0xmaker",
                    "updateTime": 100,
                    "status": "ACTIVE",
                    "totalBetSize": "282680000",
                    "fillAmount": "0",
                    "pendingFillAmount": "0",
                    "percentageOdds": "43125000000000000000",
                    "isMakerBettingOutcomeOne": False,
                }
            ],
        }

        client._apply_sx_publication(  # noqa: SLF001
            "0xmarket",
            "order_book:market_0xmarket",
            publication,
        )

        self.assertNotIn("0xmarket", client._orders_by_market)
        self.assertNotIn("yes-token", client._books)
        self.assertNotIn("0xmaker", client._order_update_times)

    async def test_quiet_subscribed_websocket_book_does_not_use_rest_recovery(self) -> None:
        client = SxBetApiClient(_sx_config())
        token_id = "sx-outcome-one"
        market_hash = "0xmarket"
        client.register_market(token_id, market_hash, BinarySide.YES)
        stale = OrderBook(
            bids=[OrderBookLevel(0.40, 20)],
            asks=[OrderBookLevel(0.42, 20)],
        )
        client._books[token_id] = stale
        client._book_timestamps[token_id] = time.monotonic() - 10
        client._ws_connected = True
        client._subscribed_markets.add(market_hash)
        client._bootstrap_market = AsyncMock()  # type: ignore[method-assign]

        with patch.object(client, "_ensure_ws_task"):
            first, second = await asyncio.gather(
                client.watch_order_book(token_id),
                client.watch_order_book(token_id),
            )

        self.assertIs(first, stale)
        self.assertIs(second, stale)
        client._bootstrap_market.assert_not_awaited()
        self.assertTrue(client.is_order_book_execution_fresh(token_id, stale, 1.5))

    async def test_disconnected_quiet_book_uses_single_flight_rest_recovery(self) -> None:
        client = SxBetApiClient(_sx_config())
        token_id = "sx-outcome-one"
        market_hash = "0xmarket"
        client.register_market(token_id, market_hash, BinarySide.YES)
        stale = OrderBook(
            bids=[OrderBookLevel(0.40, 20)],
            asks=[OrderBookLevel(0.42, 20)],
        )
        fresh = OrderBook(
            bids=[OrderBookLevel(0.41, 20)],
            asks=[OrderBookLevel(0.43, 20)],
        )
        client._books[token_id] = stale
        client._book_timestamps[token_id] = time.monotonic() - 10

        async def bootstrap(_market_hash: str, _side: BinarySide) -> OrderBook:
            client._books[token_id] = fresh
            client._book_timestamps[token_id] = time.monotonic()
            return fresh

        client._bootstrap_market = AsyncMock(side_effect=bootstrap)  # type: ignore[method-assign]

        with patch.object(client, "_ensure_ws_task"):
            first, second = await asyncio.gather(
                client.watch_order_book(token_id),
                client.watch_order_book(token_id),
            )

        self.assertIs(first, fresh)
        self.assertIs(second, fresh)
        client._bootstrap_market.assert_awaited_once_with(market_hash, BinarySide.YES)

    async def test_positioned_recovery_replays_publications_without_rest_bootstrap(self) -> None:
        client = SxBetApiClient(_sx_config())
        client.register_market("sx-outcome-one", "0xmarket", BinarySide.YES)
        client._tracked_tokens.add("sx-outcome-one")
        client._subscription_positions["order_book:market_0xmarket"] = ("epoch-1", 4)
        client._bootstrap_market = AsyncMock()  # type: ignore[method-assign]
        pending = {2: ("0xmarket", True)}
        order = {
            "orderHash": "0xmaker",
            "updateTime": 100,
            "status": "ACTIVE",
            "totalBetSize": "282680000",
            "fillAmount": "0",
            "pendingFillAmount": "0",
            "percentageOdds": "43125000000000000000",
            "isMakerBettingOutcomeOne": False,
        }

        await client._handle_centrifugo_message(  # noqa: SLF001
            {
                "id": 2,
                "subscribe": {
                    "epoch": "epoch-1",
                    "offset": 5,
                    "recovered": True,
                    "publications": [{"offset": 5, "data": [order]}],
                },
            },
            pending,
        )

        client._bootstrap_market.assert_not_awaited()
        self.assertIn("0xmarket", client._subscribed_markets)
        self.assertEqual(client._subscription_positions["order_book:market_0xmarket"], ("epoch-1", 5))
        self.assertEqual(client._books["sx-outcome-one"].status, MarketDataStatus.VALID)
        self.assertTrue(client._books["sx-outcome-one"].asks)

    async def test_initial_subscription_bootstrap_does_not_block_pong_or_lose_publications(self) -> None:
        client = SxBetApiClient(_sx_config())
        market_hash = "0xmarket"
        token_id = "sx-outcome-one"
        client.register_market(token_id, market_hash, BinarySide.YES)
        client._tracked_tokens.add(token_id)
        bootstrap_started = asyncio.Event()
        release_bootstrap = asyncio.Event()

        async def bootstrap(_market_hash: str, _side: BinarySide | None = None) -> OrderBook:
            del _side
            self.assertEqual(_market_hash, market_hash)
            bootstrap_started.set()
            await release_bootstrap.wait()
            client._orders_by_market[market_hash] = {}
            client._rebuild_market_books(market_hash)
            return client._books[token_id]

        client._bootstrap_market = AsyncMock(side_effect=bootstrap)  # type: ignore[method-assign]
        client._ws = SimpleNamespace(closed=False, send_json=AsyncMock())
        pending = {2: (market_hash, False)}

        await client._handle_centrifugo_message(  # noqa: SLF001
            {"id": 2, "subscribe": {"epoch": "epoch-1", "offset": 4, "recovered": False}},
            pending,
        )
        await asyncio.wait_for(bootstrap_started.wait(), timeout=1)
        order = {
            "orderHash": "0xmaker",
            "updateTime": 100,
            "status": "ACTIVE",
            "totalBetSize": "282680000",
            "fillAmount": "0",
            "pendingFillAmount": "0",
            "percentageOdds": "43125000000000000000",
            "isMakerBettingOutcomeOne": False,
        }
        await client._handle_centrifugo_message(  # noqa: SLF001
            {
                "push": {
                    "channel": f"order_book:market_{market_hash}",
                    "pub": {"offset": 5, "data": [order]},
                }
            },
            pending,
        )
        await client._handle_centrifugo_message({}, pending)  # noqa: SLF001

        client._ws.send_json.assert_awaited_once_with({})
        self.assertNotIn("0xmaker", client._orders_by_market.get(market_hash, {}))
        release_bootstrap.set()
        await asyncio.gather(*list(client._bootstrap_tasks.values()))

        self.assertIn("0xmaker", client._orders_by_market[market_hash])
        self.assertEqual(client._subscription_positions[f"order_book:market_{market_hash}"], ("epoch-1", 5))
        self.assertTrue(client._books[token_id].asks)

    async def test_realtime_publications_deduplicate_and_mark_sequence_gap_stale(self) -> None:
        client = SxBetApiClient(_sx_config())
        client.register_market("sx-outcome-one", "0xmarket", BinarySide.YES)
        client._tracked_tokens.add("sx-outcome-one")
        channel = "order_book:market_0xmarket"
        client._subscription_positions[channel] = ("epoch-1", 4)
        order = {
            "orderHash": "0xmaker",
            "updateTime": 100,
            "status": "ACTIVE",
            "totalBetSize": "282680000",
            "fillAmount": "0",
            "pendingFillAmount": "0",
            "percentageOdds": "43125000000000000000",
            "isMakerBettingOutcomeOne": False,
        }

        client._apply_sx_publication("0xmarket", channel, {"offset": 5, "data": [order]})  # noqa: SLF001
        original_price = client._books["sx-outcome-one"].best_ask.price
        duplicate = {**order, "percentageOdds": "60000000000000000000"}
        client._apply_sx_publication("0xmarket", channel, {"offset": 6, "data": [duplicate]})  # noqa: SLF001

        self.assertEqual(client._books["sx-outcome-one"].best_ask.price, original_price)
        client._apply_sx_publication("0xmarket", channel, {"offset": 8, "data": []})  # noqa: SLF001
        self.assertEqual(client._books["sx-outcome-one"].status, MarketDataStatus.STALE)
        self.assertEqual(client.telemetry_snapshot()["sequence_gaps"], 1.0)

    async def test_request_json_retries_get_after_timeout(self) -> None:
        client = SxBetApiClient(_sx_config())
        calls = 0

        class FakeResponse:
            status = 200

            async def __aenter__(self) -> "FakeResponse":
                return self

            async def __aexit__(
                self,
                exc_type: type[BaseException] | None,
                exc: BaseException | None,
                tb: TracebackType | None,
            ) -> bool:
                return False

            async def json(self, content_type: str | None = None) -> dict[str, object]:
                del content_type
                return {"data": [{"orderHash": "0xmaker"}]}

        class TimeoutResponse:
            async def __aenter__(self) -> "TimeoutResponse":
                raise TimeoutError("timeout")

            async def __aexit__(
                self,
                exc_type: type[BaseException] | None,
                exc: BaseException | None,
                tb: TracebackType | None,
            ) -> bool:
                return False

        class FakeSession:
            closed = False

            def request(self, method: str, url: str, **kwargs: object) -> FakeResponse | TimeoutResponse:
                del method, url, kwargs
                nonlocal calls
                calls += 1
                return TimeoutResponse() if calls == 1 else FakeResponse()

        client._rest_session = FakeSession()

        payload = await client._request_json("GET", "/orders", query_params={"marketHashes": "0xmarket"})  # noqa: SLF001

        self.assertEqual(calls, 2)
        self.assertEqual(payload, {"data": [{"orderHash": "0xmaker"}]})

    async def test_watch_order_book_uses_registered_market_side(self) -> None:
        client = SxBetApiClient(_sx_config())
        client.register_market("sx-outcome-one", "0xmarket", BinarySide.YES)
        client._request_json = AsyncMock(  # type: ignore[method-assign]
            return_value={
                "data": [
                    {
                        "orderHash": "0xmaker",
                        "totalBetSize": "282680000",
                        "fillAmount": "0",
                        "pendingFillAmount": "0",
                        "percentageOdds": "43125000000000000000",
                        "isMakerBettingOutcomeOne": False,
                    }
                ]
            }
        )

        book = await client.watch_order_book("sx-outcome-one")

        self.assertAlmostEqual(book.best_ask.price, 0.56875, places=6)
        self.assertGreater(book.best_ask.size, 400)

    async def test_empty_taker_book_refreshes_liveness_but_remains_unavailable_for_execution(self) -> None:
        client = SxBetApiClient(_sx_config())
        client.register_market("sx-outcome-one", "0xmarket", BinarySide.YES)
        client._request_json = AsyncMock(return_value={"data": []})  # type: ignore[method-assign]

        book = await client.watch_order_book("sx-outcome-one")

        self.assertEqual(book.asks, [])
        self.assertEqual(book.status, MarketDataStatus.VALID)
        self.assertIsNotNone(client.market_data_age_seconds())

    async def test_buy_translates_payout_contracts_into_sx_fill_payload(self) -> None:
        client = SxBetApiClient(_sx_config())
        client.register_market("sx-outcome-one", "0xmarket", BinarySide.YES)
        client._metadata = AsyncMock(return_value={"domainVersion": "6.0", "EIP712FillHasher": "0xhasher"})  # type: ignore[method-assign]
        client._base_token_address = AsyncMock(return_value="0xbase")  # type: ignore[method-assign]
        client._sign_fill_payload = MagicMock(return_value="0xsig")  # type: ignore[method-assign]
        client._get_web3_client = MagicMock(return_value=SimpleNamespace(account=SimpleNamespace(address="0xtrader")))  # type: ignore[method-assign]
        client._request_json = AsyncMock(  # type: ignore[method-assign]
            return_value={
                "data": {
                    "fillHash": "0xfill",
                    "isPartialFill": False,
                    "totalFilled": "10000000",
                    "averageOdds": "50000000000000000000",
                }
            }
        )

        order_id = await client.buy(
            token_id="sx-outcome-one",
            side=BinarySide.YES,
            contracts=20.0,
            max_price=0.5,
        )
        client._find_submitted_trade = AsyncMock(  # type: ignore[method-assign]
            return_value={
                "fillHash": "0xfill",
                "marketHash": "0xmarket",
                "bettor": "0xtrader",
                "maker": False,
                "bettingOutcomeOne": True,
                "normalizedStake": "10",
                "odds": "50000000000000000000",
                "tradeStatus": "SUCCESS",
                "valid": True,
                "updatedAt": "2026-06-30T12:00:00Z",
            }
        )
        report = await client.wait_filled(order_id, 1)
        await_args = client._request_json.await_args
        self.assertIsNotNone(await_args)
        assert await_args is not None
        request_payload = await_args.kwargs["json_body"]

        self.assertTrue(order_id.startswith("sx:BUY:YES:0xmarket:"))
        self.assertTrue(order_id.endswith(":0xfill"))
        self.assertEqual(request_payload["market"], "0xmarket")
        self.assertEqual(request_payload["baseToken"], "0xbase")
        self.assertTrue(request_payload["isTakerBettingOutcomeOne"])
        self.assertEqual(request_payload["stakeWei"], "10000000")
        self.assertEqual(request_payload["desiredOdds"], "50000000000000000000")
        self.assertEqual(request_payload["takerSig"], "0xsig")
        self.assertEqual(str(report.amount_requested), "20.0")
        self.assertEqual(report.amount_filled, Decimal("20"))
        self.assertEqual(report.avg_price, Decimal("0.5"))

    async def test_order_preview_builds_signed_fill_without_submit(self) -> None:
        client = SxBetApiClient(_sx_config())
        client.register_market("sx-outcome-one", "0xmarket", BinarySide.YES)
        client._metadata = AsyncMock(return_value={"domainVersion": "6.0", "EIP712FillHasher": "0xhasher"})  # type: ignore[method-assign]
        client._base_token_address = AsyncMock(return_value="0xbase")  # type: ignore[method-assign]
        client._sign_fill_payload = MagicMock(return_value="0xsig")  # type: ignore[method-assign]
        client._get_web3_client = MagicMock(return_value=SimpleNamespace(account=SimpleNamespace(address="0xtrader")))  # type: ignore[method-assign]

        preview = await client.build_order_preview(
            token_id="sx-outcome-one",
            side=BinarySide.YES,
            contracts=20.0,
            limit_price=0.55,
            action="SELL",
        )

        request_payload = preview["request_payload"]
        self.assertTrue(str(preview["order_id"]).startswith("sx:SELL:YES:0xmarket:"))
        self.assertEqual(preview["synthetic_side"], "YES")
        self.assertEqual(preview["actual_fill_side"], "NO")
        self.assertEqual(request_payload["market"], "0xmarket")
        self.assertEqual(request_payload["baseToken"], "0xbase")
        self.assertFalse(request_payload["isTakerBettingOutcomeOne"])
        self.assertEqual(request_payload["stakeWei"], "9000000")
        self.assertEqual(request_payload["desiredOdds"], "45000000000000000000")
        self.assertNotIn("takerSig", request_payload)
        self.assertTrue(preview["signature_present"])
        self.assertEqual(
            preview["signature_sha256"],
            hashlib.sha256(b"0xsig").hexdigest(),
        )

    async def test_sell_uses_opposite_outcome_fill_and_reports_same_side_exit_price(self) -> None:
        client = SxBetApiClient(_sx_config())
        client.register_market("sx-outcome-one", "0xmarket", BinarySide.YES)
        client._metadata = AsyncMock(return_value={"domainVersion": "6.0", "EIP712FillHasher": "0xhasher"})  # type: ignore[method-assign]
        client._base_token_address = AsyncMock(return_value="0xbase")  # type: ignore[method-assign]
        client._sign_fill_payload = MagicMock(return_value="0xsig")  # type: ignore[method-assign]
        client._get_web3_client = MagicMock(return_value=SimpleNamespace(account=SimpleNamespace(address="0xtrader")))  # type: ignore[method-assign]
        client._request_json = AsyncMock(  # type: ignore[method-assign]
            return_value={
                "data": {
                    "fillHash": "0xclose",
                    "isPartialFill": False,
                    "totalFilled": "9000000",
                    "averageOdds": "45000000000000000000",
                }
            }
        )

        order_id = await client.sell("sx-outcome-one", BinarySide.YES, 20.0, 0.55)
        client._find_submitted_trade = AsyncMock(  # type: ignore[method-assign]
            return_value={
                "fillHash": "0xclose",
                "marketHash": "0xmarket",
                "bettor": "0xtrader",
                "maker": False,
                "bettingOutcomeOne": False,
                "normalizedStake": "9",
                "odds": "45000000000000000000",
                "tradeStatus": "SUCCESS",
                "valid": True,
                "updatedAt": "2026-06-30T12:00:00Z",
            }
        )

        report = await client.wait_filled(order_id, 1)
        await_args = client._request_json.await_args
        self.assertIsNotNone(await_args)
        assert await_args is not None
        request_payload = await_args.kwargs["json_body"]

        self.assertTrue(order_id.startswith("sx:SELL:YES:0xmarket:"))
        self.assertTrue(request_payload["market"], "0xmarket")
        self.assertFalse(request_payload["isTakerBettingOutcomeOne"])
        self.assertEqual(request_payload["stakeWei"], "9000000")
        self.assertEqual(request_payload["desiredOdds"], "45000000000000000000")
        self.assertEqual(str(report.amount_requested), "20.0")
        self.assertEqual(report.amount_filled, Decimal("20"))
        self.assertEqual(report.avg_price, Decimal("0.55"))

    async def test_cash_balance_details_report_scaled_usdc_balance(self) -> None:
        client = SxBetApiClient(_sx_config())

        class BalanceCall:
            async def call(self) -> int:
                return 375_000_000

        class DecimalsCall:
            async def call(self) -> int:
                return 6

        class Functions:
            def balanceOf(self, address: str) -> BalanceCall:
                self.address = address
                return BalanceCall()

            def decimals(self) -> DecimalsCall:
                return DecimalsCall()

        functions = Functions()
        web3_client = MagicMock()
        web3_client.account = SimpleNamespace(address="0xtrader")
        web3_client.contract.return_value = SimpleNamespace(functions=functions)
        client._web3_client = web3_client
        client._base_token_address = AsyncMock(return_value="0xbase")  # type: ignore[method-assign]

        details = await client.get_cash_balance_details()

        self.assertEqual(functions.address, "0xtrader")
        self.assertEqual(details["wallet_address"], "0xtrader")
        self.assertEqual(details["base_token_address"], "0xbase")
        self.assertEqual(details["balance_raw"], "375000000")
        self.assertEqual(details["decimals"], 6)
        self.assertEqual(details["balance"], 375.0)
        self.assertEqual(await client.get_cash_balance(), 375.0)

    async def test_get_positions_nets_out_opposite_sx_bets(self) -> None:
        client = SxBetApiClient(_sx_config())
        client.register_market("sx-outcome-one", "0xmarket", BinarySide.YES)
        client.register_market("sx-outcome-two", "0xmarket", BinarySide.NO)
        client._get_web3_client = MagicMock(return_value=SimpleNamespace(account=SimpleNamespace(address="0xtrader")))  # type: ignore[method-assign]
        client._list_trades = AsyncMock(  # type: ignore[method-assign]
            return_value=[
                {
                    "fillHash": "0xyes",
                    "marketHash": "0xmarket",
                    "bettor": "0xtrader",
                    "maker": False,
                    "bettingOutcomeOne": True,
                    "normalizedStake": "10",
                    "odds": "50000000000000000000",
                    "tradeStatus": "SUCCESS",
                    "valid": True,
                    "settled": False,
                },
                {
                    "fillHash": "0xno",
                    "marketHash": "0xmarket",
                    "bettor": "0xtrader",
                    "maker": False,
                    "bettingOutcomeOne": False,
                    "normalizedStake": "2",
                    "odds": "40000000000000000000",
                    "tradeStatus": "SUCCESS",
                    "valid": True,
                    "settled": False,
                },
            ]
        )

        positions = await client.get_positions()

        self.assertEqual(positions, {"sx-outcome-one": Decimal("15")})

    async def test_settlement_status_reads_sx_trade_settlement(self) -> None:
        client = SxBetApiClient(_sx_config())
        client._get_web3_client = MagicMock(return_value=SimpleNamespace(account=SimpleNamespace(address="0xtrader")))  # type: ignore[method-assign]
        client._list_trades = AsyncMock(  # type: ignore[method-assign]
            side_effect=[
                [
                    {
                        "fillHash": "0xopen",
                        "marketHash": "0xmarket",
                        "bettor": "0xtrader",
                        "maker": False,
                        "bettingOutcomeOne": True,
                        "normalizedStake": "10",
                        "odds": "50000000000000000000",
                        "tradeStatus": "SUCCESS",
                        "valid": True,
                        "settled": False,
                    }
                ],
                [
                    {
                        "fillHash": "0xsettled",
                        "marketHash": "0xmarket",
                        "bettor": "0xtrader",
                        "maker": False,
                        "bettingOutcomeOne": True,
                        "normalizedStake": "10",
                        "odds": "50000000000000000000",
                        "tradeStatus": "SUCCESS",
                        "valid": True,
                        "settled": True,
                    }
                ],
            ]
        )

        open_status = await client.get_settlement_status(
            SettlementRequest(
                position_key="k",
                venue="SX Bet",
                market_id="0xmarket",
                condition_id="0xmarket",
                collateral_token="",
                expected_contracts=Decimal("0"),
            )
        )
        settled_status = await client.get_settlement_status(
            SettlementRequest(
                position_key="k",
                venue="SX Bet",
                market_id="0xmarket",
                condition_id="0xmarket",
                collateral_token="",
                expected_contracts=Decimal("0"),
            )
        )

        self.assertEqual(open_status.value, "OPEN")
        self.assertEqual(settled_status.value, "SETTLED")

    async def test_list_fills_reconstructs_order_id_from_trade_without_submitted_cache(self) -> None:
        client = SxBetApiClient(_sx_config())
        client._get_web3_client = MagicMock(return_value=SimpleNamespace(account=SimpleNamespace(address="0xtrader")))  # type: ignore[method-assign]
        client._list_trades = AsyncMock(  # type: ignore[method-assign]
            return_value=[
                {
                    "id": "trade-1",
                    "fillHash": "0xfill",
                    "marketHash": "0xmarket",
                    "bettor": "0xtrader",
                    "maker": False,
                    "bettingOutcomeOne": True,
                    "normalizedStake": "10",
                    "odds": "50000000000000000000",
                    "tradeStatus": "SUCCESS",
                    "valid": True,
                    "updatedAt": "2026-06-30T12:00:00Z",
                }
            ]
        )

        fills = await client.list_fills(None)

        self.assertEqual(len(fills), 1)
        self.assertEqual(fills[0].venue_order_id, "0xfill")
        self.assertEqual(fills[0].quantity, Decimal("20"))
        self.assertEqual(fills[0].price, Decimal("0.5"))

    async def test_find_submitted_trade_queries_with_lookback_buffer(self) -> None:
        client = SxBetApiClient(_sx_config())
        client._get_web3_client = MagicMock(return_value=SimpleNamespace(account=SimpleNamespace(address="0xtrader")))  # type: ignore[method-assign]
        client._list_trades = AsyncMock(return_value=[])  # type: ignore[method-assign]
        submitted_at = datetime(2026, 6, 30, 12, 0, tzinfo=UTC)

        submitted = _submitted_from_trade(
            {
                "fillHash": "0xfill",
                "marketHash": "0xmarket",
                "bettingOutcomeOne": True,
                "normalizedStake": "10",
                "odds": "50000000000000000000",
                "updatedAt": submitted_at.isoformat().replace("+00:00", "Z"),
            }
        )
        self.assertIsNotNone(submitted)
        assert submitted is not None
        await client._find_submitted_trade(submitted)  # noqa: SLF001

        await_args = client._list_trades.await_args
        self.assertIsNotNone(await_args)
        assert await_args is not None
        self.assertEqual(await_args.kwargs["bettor"], "0xtrader")
        self.assertEqual(await_args.kwargs["market_hashes"], ["0xmarket"])
        self.assertEqual(await_args.kwargs["start_date"], submitted_at - timedelta(minutes=2))

    async def test_find_submitted_trade_falls_back_when_fill_response_lacks_fill_hash(self) -> None:
        client = SxBetApiClient(_sx_config())
        client._get_web3_client = MagicMock(return_value=SimpleNamespace(account=SimpleNamespace(address="0xtrader")))  # type: ignore[method-assign]
        client._list_trades = AsyncMock(  # type: ignore[method-assign]
            return_value=[
                {
                    "fillHash": "0xvenuefill",
                    "marketHash": "0xmarket",
                    "bettor": "0xtrader",
                    "maker": False,
                    "bettingOutcomeOne": True,
                    "normalizedStake": "10",
                    "odds": "50000000000000000000",
                    "tradeStatus": "SUCCESS",
                    "valid": True,
                    "updatedAt": "2026-06-30T12:00:30Z",
                }
            ]
        )

        trade = await client._find_submitted_trade(  # noqa: SLF001
            client._submitted_fills.setdefault(  # noqa: SLF001
                "synthetic-order",
                SimpleNamespace(  # type: ignore[arg-type]
                    order_id="synthetic-order",
                    fill_hash="sx-fill:123",
                    market_hash="0xmarket",
                    token_id="sx-outcome-one",
                    action="BUY",
                    synthetic_side=BinarySide.YES,
                    actual_side=BinarySide.YES,
                    requested_contracts=Decimal("20"),
                    requested_price=Decimal("0.5"),
                    submitted_at=datetime(2026, 6, 30, 12, 0, tzinfo=UTC),
                ),
            )
        )

        assert trade is not None
        self.assertEqual(trade["fillHash"], "0xvenuefill")


class SxBetTradeFallbackTests(unittest.TestCase):
    def test_submitted_from_trade_reconstructs_full_order_identity(self) -> None:
        submitted = _submitted_from_trade(
            {
                "fillHash": "0xfill",
                "marketHash": "0xmarket",
                "bettingOutcomeOne": True,
                "normalizedStake": "10",
                "odds": "50000000000000000000",
                "updatedAt": "2026-06-30T12:00:00Z",
            }
        )

        assert submitted is not None
        self.assertEqual(submitted.order_id, "0xfill")
        self.assertEqual(submitted.market_hash, "0xmarket")
        self.assertEqual(submitted.token_id, "0xmarket:YES")
        self.assertEqual(submitted.synthetic_side, BinarySide.YES)
        self.assertEqual(submitted.requested_contracts, Decimal("20"))
        self.assertEqual(submitted.requested_price, Decimal("0.5"))

    def test_trade_query_start_keeps_epoch_fallback_unbuffered(self) -> None:
        epoch = datetime.fromtimestamp(0, tz=UTC)

        self.assertEqual(_trade_query_start(epoch), epoch)

    def test_trade_datetime_accepts_millisecond_epoch_payload(self) -> None:
        trade_time = _trade_datetime({"betTime": 1_719_843_600_000})

        self.assertEqual(trade_time, datetime.fromtimestamp(1_719_843_600, tz=UTC))


if __name__ == "__main__":
    unittest.main()
