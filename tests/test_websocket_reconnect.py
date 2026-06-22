import asyncio
import unittest
from unittest.mock import patch

from arbitrage_engine.config import MyriadMarketsConfig, PolymarketConfig
from arbitrage_engine.connectors.myriad import MyriadClient
from arbitrage_engine.connectors.polymarket import PolymarketClobClient


class _FailingConnection:
    async def __aenter__(self) -> None:
        raise RuntimeError("synthetic gateway rejection")

    async def __aexit__(self, *args: object) -> None:
        del args


class _FailingSession:
    def ws_connect(self, *args: object, **kwargs: object) -> _FailingConnection:
        del args, kwargs
        return _FailingConnection()


class WebSocketReconnectLoadTests(unittest.IsolatedAsyncioTestCase):
    async def test_one_hundred_polymarket_failures_are_backed_off(self) -> None:
        client = PolymarketClobClient(PolymarketConfig(None, "https://clob.polymarket.com", 137, 0, None))
        delays: list[float] = []

        async def controlled_sleep(delay: float) -> None:
            delays.append(delay)
            if len(delays) == 100:
                raise asyncio.CancelledError

        with (
            patch.object(client, "_get_ws_session", return_value=_FailingSession()),
            patch("arbitrage_engine.connectors.polymarket.asyncio.sleep", side_effect=controlled_sleep),
            patch("arbitrage_engine.connectors.base.random.uniform", side_effect=lambda _low, high: high),
        ):
            with self.assertRaises(asyncio.CancelledError):
                await client._run_order_book_ws()

        self.assertEqual(len(delays), 100)
        self.assertEqual(delays[:6], [1.0, 2.0, 4.0, 8.0, 16.0, 30.0])
        self.assertTrue(all(delay == 30.0 for delay in delays[5:]))

    async def test_one_hundred_myriad_failures_are_backed_off(self) -> None:
        client = MyriadClient(
            MyriadMarketsConfig(
                api_url="https://api-v2.myriad.markets",
                ws_url="wss://ws-v2.myriad.markets/connection/websocket",
                api_key=None,
                private_key=None,
                rpc_url="https://bsc-dataseed.binance.org",
                rpc_urls=["https://bsc-dataseed.binance.org"],
                chain_id=56,
                exchange_address="0x0000000000000000000000000000000000000001",
                conditional_tokens_address="0x0000000000000000000000000000000000000002",
                collateral_tokens={"USDT": "0x0000000000000000000000000000000000000003"},
                collateral_symbol="USDT",
                trading_fee_pct=0.0,
                max_slippage_pct=0.01,
                enabled=True,
            )
        )
        delays: list[float] = []

        async def controlled_sleep(delay: float) -> None:
            delays.append(delay)
            if len(delays) == 100:
                raise asyncio.CancelledError

        with (
            patch.object(client, "_get_ws_session", return_value=_FailingSession()),
            patch("arbitrage_engine.connectors.myriad.asyncio.sleep", side_effect=controlled_sleep),
            patch("arbitrage_engine.connectors.base.random.uniform", side_effect=lambda _low, high: high),
        ):
            with self.assertRaises(asyncio.CancelledError):
                await client._run_orderbook_ws()

        self.assertEqual(len(delays), 100)
        self.assertEqual(delays[:6], [1.0, 2.0, 4.0, 8.0, 16.0, 30.0])
        self.assertTrue(all(delay == 30.0 for delay in delays[5:]))


if __name__ == "__main__":
    unittest.main()
