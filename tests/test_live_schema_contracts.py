from __future__ import annotations

import os
import unittest

from arbitrage_engine.config import MyriadMarketsConfig, PredictFunConfig
from arbitrage_engine.market_discovery import GammaMarketResolver
from arbitrage_engine.myriad_discovery import MyriadMarketResolver, _market_text
from arbitrage_engine.predict_fun_discovery import PredictFunMarketResolver, _market_spec_from_payload


def _live_contracts_enabled() -> bool:
    return os.getenv("ARB_RUN_LIVE_SCHEMA_CONTRACTS") == "1"


class LiveSchemaContractTests(unittest.IsolatedAsyncioTestCase):
    async def test_polymarket_gamma_payload_contract(self) -> None:
        if not _live_contracts_enabled():
            self.skipTest("set ARB_RUN_LIVE_SCHEMA_CONTRACTS=1 to run live schema checks")

        resolver = GammaMarketResolver(scan_all=True)
        try:
            payloads = await resolver._fetch_all_markets()
        finally:
            await resolver.close()

        self.assertTrue(payloads)
        sample = payloads[0]
        self.assertIn("id", sample)
        self.assertIn("conditionId", sample)
        self.assertIn("clobTokenIds", sample)
        self.assertIn("outcomes", sample)

    async def test_myriad_market_payload_contract(self) -> None:
        if not _live_contracts_enabled():
            self.skipTest("set ARB_RUN_LIVE_SCHEMA_CONTRACTS=1 to run live schema checks")

        resolver = MyriadMarketResolver(
            MyriadMarketsConfig(
                api_url="https://api-v2.myriadprotocol.com",
                ws_url="wss://ws.myriadprotocol.com/ws",
                api_key=os.getenv("MYRIAD_API_KEY"),
                private_key=None,
                rpc_url="https://bsc-dataseed.binance.org",
                rpc_urls=["https://bsc-dataseed.binance.org"],
                chain_id=56,
                exchange_address="0xa0b6f8ef8EdB64f395018D1933f2273Ce9f0f16A",
                conditional_tokens_address="0x6413734f92248D4B29ae35883290BD93212654Dc",
                collateral_tokens={},
                collateral_symbol="USDT",
                trading_fee_pct=0.0,
                max_slippage_pct=0.015,
                enabled=True,
            ),
            scan_all=True,
        )
        try:
            payloads = await resolver._fetch_markets()
        finally:
            await resolver.close()

        self.assertTrue(payloads)
        parsed = [item for payload in payloads[:25] if (item := _market_text(payload)) is not None]
        self.assertTrue(parsed)

    async def test_predict_fun_market_payload_contract(self) -> None:
        if not _live_contracts_enabled():
            self.skipTest("set ARB_RUN_LIVE_SCHEMA_CONTRACTS=1 to run live schema checks")
        api_key = os.getenv("PREDICT_FUN_API_KEY")
        if not api_key:
            self.skipTest("PREDICT_FUN_API_KEY is required for live Predict.fun schema checks")

        resolver = PredictFunMarketResolver(
            PredictFunConfig(
                enabled=True,
                private_key=None,
                rpc_url="https://bsc-dataseed.binance.org",
                rpc_urls=["https://bsc-dataseed.binance.org"],
                chain_id=56,
                network="mainnet",
                api_base_url="https://api.predict.fun/",
                api_key=api_key,
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
            ),
            scan_all=True,
        )
        try:
            payloads = await resolver._fetch_markets()
        finally:
            await resolver.close()

        self.assertTrue(payloads)
        parsed = [item for payload in payloads[:25] if (item := _market_spec_from_payload(payload)) is not None]
        self.assertTrue(parsed)
