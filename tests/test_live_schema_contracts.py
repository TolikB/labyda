from __future__ import annotations

import os
import unittest
from dataclasses import replace
from decimal import Decimal
from typing import Any

from arbitrage_engine.conditional_tokens import CONDITIONAL_TOKENS_ABI, ConditionalTokensRedemption
from arbitrage_engine.config import MyriadMarketsConfig, OpinionConfig, PredictFunConfig, SxBetConfig
from arbitrage_engine.connectors.opinion import ERC20_BALANCE_ABI as OPINION_ERC20_ABI
from arbitrage_engine.connectors.opinion import OpinionClient
from arbitrage_engine.connectors.opinion import execution_token as opinion_execution_token
from arbitrage_engine.connectors.opinion import parse_execution_token as opinion_parse_execution_token
from arbitrage_engine.connectors.predict_fun import (
    PredictFunApiClient,
    _extract_records,
    _order_book_from_payload,
)
from arbitrage_engine.connectors.sx_bet import (
    _extract_records as _extract_sx_records,
)
from arbitrage_engine.connectors.sx_bet_v3 import SxBetV3ApiClient, _order_book_from_v3_maker_snapshot
from arbitrage_engine.connectors.web3_base import BaseWeb3Client
from arbitrage_engine.http import client_session
from arbitrage_engine.market_discovery import GammaMarketResolver
from arbitrage_engine.models import BinarySide, MarketDataStatus, SettlementRequest, SettlementStatus
from arbitrage_engine.myriad_discovery import MyriadMarketResolver, _market_text
from arbitrage_engine.opinion_discovery import OpinionMarketResolver
from arbitrage_engine.opinion_discovery import _market_text as _opinion_market_text
from arbitrage_engine.predict_fun_discovery import PredictFunMarketResolver, _market_spec_from_payload
from arbitrage_engine.sx_bet_discovery import SxBetMarketResolver, _sx_market_text


def _live_contracts_enabled() -> bool:
    return os.getenv("ARB_RUN_LIVE_SCHEMA_CONTRACTS") == "1"


OPINION_CONDITIONAL_TOKENS = "0xAD1a38cEc043e70E83a3eC30443dB285ED10D774"
OPINION_COLLATERAL = "0x55d398326f99059fF775485246999027B3197955"
OPINION_RPC = os.getenv("BNB_RPC_URL") or "https://bsc-dataseed.binance.org"


def _opinion_web3() -> BaseWeb3Client:
    return BaseWeb3Client(rpc_url=[OPINION_RPC], chain_id=56, private_key=None)


def _opinion_settlement_request(detail: dict[str, Any]) -> SettlementRequest:
    return SettlementRequest(
        position_key="live-contract",
        venue="Opinion",
        market_id=str(detail["marketId"]),
        condition_id=f"0x{detail['conditionId']}",
        collateral_token=OPINION_COLLATERAL,
        expected_contracts=Decimal(0),
    )


async def _opinion_market_detail(status: str) -> dict[str, Any] | None:
    """First market with the given venue status that exposes a usable condition id."""
    session = client_session({"Accept": "application/json"})
    try:
        async with session.get(
            "https://openapi.opinion.trade/openapi/market",
            params={"page": 1, "limit": 5, "status": status, "marketType": 0, "chainId": "56"},
            timeout=20,
        ) as response:
            listing = await response.json()
        for market in ((listing.get("result") or {}).get("list") or []):
            async with session.get(
                f"https://openapi.opinion.trade/openapi/market/{market['marketId']}", timeout=20
            ) as response:
                payload = await response.json()
            detail = (payload.get("result") or {}).get("data") or {}
            if len(str(detail.get("conditionId") or "")) == 64:
                return detail
    finally:
        await session.close()
    return None


async def _opinion_resolved_market_detail() -> dict[str, Any] | None:
    return await _opinion_market_detail("resolved")


async def _opinion_activated_market_detail() -> dict[str, Any] | None:
    return await _opinion_market_detail("activated")


def _opinion_config(**overrides: object) -> OpinionConfig:
    """Read-only Opinion config; no signing key, so nothing here can submit."""
    settings: dict[str, object] = {
        "enabled": True,
        "api_base_url": "https://openapi.opinion.trade/openapi",
        "ws_url": "wss://ws.opinion.trade",
        "api_key": os.getenv("OPINION_API_KEY"),
        "private_key": None,
        "rpc_url": os.getenv("BNB_RPC_URL") or "https://bsc-dataseed.binance.org",
        "rpc_urls": [os.getenv("BNB_RPC_URL") or "https://bsc-dataseed.binance.org"],
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
    settings.update(overrides)
    return OpinionConfig(**settings)  # type: ignore[arg-type]


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

    async def test_predict_fun_runtime_endpoint_contracts(self) -> None:
        if not _live_contracts_enabled():
            self.skipTest("set ARB_RUN_LIVE_SCHEMA_CONTRACTS=1 to run live schema checks")
        api_key = os.getenv("PREDICT_FUN_API_KEY")
        if not api_key:
            self.skipTest("PREDICT_FUN_API_KEY is required for live Predict.fun schema checks")
        private_key = os.getenv("PREDICT_FUN_PRIVATE_KEY")
        if not private_key:
            self.skipTest("PREDICT_FUN_PRIVATE_KEY is required for private Predict.fun schema checks")

        config = PredictFunConfig(
            enabled=True,
            private_key=private_key,
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
        )
        resolver = PredictFunMarketResolver(config, scan_all=True)
        client = PredictFunApiClient(config)
        try:
            markets = await resolver._fetch_markets()  # noqa: SLF001
            self.assertTrue(markets)
            market_id = str(
                markets[0].get("id")
                or markets[0].get("marketId")
                or markets[0].get("market_id")
                or markets[0].get("conditionId")
                or markets[0].get("condition_id")
                or ""
            )
            self.assertTrue(market_id)

            orderbook_payload = await client._request_json("GET", f"/v1/markets/{market_id}/orderbook")  # noqa: SLF001
            orderbook = _order_book_from_payload(orderbook_payload)
            self.assertIsInstance(orderbook_payload, dict)
            self.assertTrue(isinstance(orderbook.raw_payload, dict))

            orders_payload = await client._request_json(  # noqa: SLF001
                "GET",
                "/v1/orders",
                query_params={"status": "OPEN"},
                require_jwt=True,
            )
            self.assertIsInstance(orders_payload, dict)
            self.assertIsInstance(_extract_records(orders_payload, ("orders", "items", "results")), list)

            positions_payload = await client._request_json(  # noqa: SLF001
                "GET",
                "/v1/positions",
                require_jwt=True,
            )
            self.assertIsInstance(positions_payload, dict)
            self.assertIsInstance(_extract_records(positions_payload, ("positions", "items", "results")), list)

            open_orders = _extract_records(orders_payload, ("orders", "items", "results"))
            if open_orders:
                order_id = str(
                    open_orders[0].get("orderHash")
                    or open_orders[0].get("hash")
                    or open_orders[0].get("orderId")
                    or open_orders[0].get("id")
                    or ""
                )
                if order_id:
                    order_payload = await client._request_json(  # noqa: SLF001
                        "GET",
                        f"/v1/orders/{order_id}",
                        require_jwt=True,
                    )
                    self.assertIsInstance(order_payload, dict)
        finally:
            await client.close()
            await resolver.close()

    async def test_opinion_market_payload_contract(self) -> None:
        """Public catalogue shape. Needs no API key: /market is unauthenticated."""
        if not _live_contracts_enabled():
            self.skipTest("set ARB_RUN_LIVE_SCHEMA_CONTRACTS=1 to run live schema checks")

        resolver = OpinionMarketResolver(_opinion_config(), scan_all=True, categories_to_scan=[])
        try:
            payloads = await resolver._fetch_markets()
        finally:
            await resolver.close()

        self.assertTrue(payloads, "Opinion returned no activated binary markets")
        usable = [item for item in (_opinion_market_text(payload) for payload in payloads) if item is not None]
        self.assertTrue(usable, "no Opinion market payload survived parsing")

        sample = payloads[0]
        for field in ("marketId", "marketTitle", "status", "marketType", "cutoffAt"):
            self.assertIn(field, sample, msg=f"Opinion market payload lost {field}")
        # Both outcome tokens and the condition id are load-bearing: the tokens
        # build the execution token, and the condition id is what Conditional
        # Tokens is keyed by at settlement.
        self.assertIn("yesTokenId", sample)
        self.assertIn("noTokenId", sample)
        self.assertIn("conditionId", sample)

    async def test_opinion_order_book_payload_contract(self) -> None:
        """Order book shape, and that it prices inside the probability range."""
        if not _live_contracts_enabled():
            self.skipTest("set ARB_RUN_LIVE_SCHEMA_CONTRACTS=1 to run live schema checks")

        resolver = OpinionMarketResolver(_opinion_config(), scan_all=True, categories_to_scan=[])
        try:
            payloads = await resolver._fetch_markets()
        finally:
            await resolver.close()
        self.assertTrue(payloads)

        market = next(item for item in payloads if item.get("yesTokenId"))
        token = opinion_execution_token(str(market["marketId"]), str(market["yesTokenId"]))
        client = OpinionClient(_opinion_config())
        try:
            book = await client._fetch_and_store_order_book(token, str(market["yesTokenId"]))
        finally:
            await client.close()

        self.assertIs(book.status, MarketDataStatus.VALID)
        for level in (*book.bids, *book.asks):
            self.assertGreater(level.price, 0.0)
            self.assertLessEqual(level.price, 1.0)
            self.assertGreaterEqual(level.size, 0.0)
        if book.bids and book.asks:
            self.assertLessEqual(book.bids[0].price, book.asks[0].price, "crossed Opinion book")

    async def test_opinion_condition_id_is_settlement_shaped(self) -> None:
        """The market id must map to a 32-byte condition id, or redemption cannot run."""
        if not _live_contracts_enabled():
            self.skipTest("set ARB_RUN_LIVE_SCHEMA_CONTRACTS=1 to run live schema checks")

        resolver = OpinionMarketResolver(_opinion_config(), scan_all=True, categories_to_scan=[])
        try:
            payloads = await resolver._fetch_markets()
        finally:
            await resolver.close()
        # The listing never carries conditionId; only /market/{id} does, which
        # is exactly the lookup settlement performs.
        self.assertTrue(payloads)
        self.assertEqual({str(item.get("conditionId", "")) for item in payloads}, {""})
        market = payloads[0]

        client = OpinionClient(_opinion_config())
        try:
            condition_id = await client._settlement_condition_id(str(market["marketId"]))
        finally:
            await client.close()

        self.assertTrue(condition_id.startswith("0x"))
        self.assertEqual(len(condition_id), 66)
        self.assertTrue(all(character in "0123456789abcdef" for character in condition_id[2:]))

    async def test_opinion_authenticated_read_only_contracts(self) -> None:
        """Account-scoped endpoints. Requires OPINION_API_KEY; never submits."""
        if not _live_contracts_enabled():
            self.skipTest("set ARB_RUN_LIVE_SCHEMA_CONTRACTS=1 to run live schema checks")

        api_key = os.getenv("OPINION_API_KEY")
        if not api_key and os.getenv("ARB_REQUIRE_OPINION_AUTH_CONTRACTS") == "1":
            self.fail("OPINION_API_KEY is required when ARB_REQUIRE_OPINION_AUTH_CONTRACTS=1")
        account = os.getenv("OPINION_ACCOUNT_ADDRESS") or os.getenv("OPINION_MULTI_SIG_ADDRESS")
        if not api_key or not account:
            self.skipTest("OPINION_API_KEY and an account address are required")

        client = OpinionClient(_opinion_config(api_key=api_key, account_address=account))
        try:
            orders = await client.list_open_orders()
            fills = await client.list_fills()
            positions = await client.get_positions()
        finally:
            await client.close()

        for order in orders:
            self.assertEqual(order.venue, "Opinion")
            self.assertGreaterEqual(order.quantity, Decimal(0))
        for fill in fills:
            self.assertGreaterEqual(fill.price, Decimal(0))
            self.assertLessEqual(fill.price, Decimal(1))
        for key in positions:
            # Positions must be keyed the way the engine addresses targets.
            self.assertEqual(len(key.split(":", 1)), 2, msg=f"unkeyed Opinion position {key}")

    async def test_opinion_discovery_pipeline_invariants(self) -> None:
        """The real resolver against the real catalogue, not a fixture."""
        if not _live_contracts_enabled():
            self.skipTest("set ARB_RUN_LIVE_SCHEMA_CONTRACTS=1 to run live schema checks")

        resolver = OpinionMarketResolver(_opinion_config(), scan_all=True, categories_to_scan=[])
        try:
            specs = await resolver.resolve([])
        finally:
            await resolver.close()

        self.assertTrue(specs, "Opinion discovery produced no seed markets")
        for spec in specs:
            self.assertEqual(spec.venue_b_label, "Opinion")
            self.assertTrue(spec.predict_fun_token_id)
            self.assertIsNotNone(spec.expires_at)
            # A hedge that is not the opposite outcome is not a hedge.
            self.assertNotEqual(spec.polymarket_side, spec.predict_fun_side)
            market_id, outcome_token = opinion_parse_execution_token(spec.predict_fun_token_id)
            self.assertGreater(market_id, 0)
            self.assertTrue(outcome_token)

    async def test_opinion_categorical_markets_are_rejected(self) -> None:
        """The engine hedges two-outcome books; categorical parents must not leak in."""
        if not _live_contracts_enabled():
            self.skipTest("set ARB_RUN_LIVE_SCHEMA_CONTRACTS=1 to run live schema checks")

        session = client_session({"Accept": "application/json"})
        try:
            async with session.get(
                "https://openapi.opinion.trade/openapi/market",
                params={"page": 1, "limit": 5, "marketType": 1, "chainId": "56"},
                timeout=20,
            ) as response:
                payload = await response.json()
        finally:
            await session.close()

        categorical = (payload.get("result") or {}).get("list") or []
        if not categorical:
            self.skipTest("venue exposed no categorical markets to test against")
        for market in categorical:
            self.assertIsNone(_opinion_market_text(market), msg=f"categorical {market.get('marketId')} accepted")

    async def test_opinion_outcome_tokens_are_conditional_token_positions(self) -> None:
        """The venue's tokenIds must be the Conditional Tokens position ids.

        This is what ties settlement together: redemption and the exposure check
        derive position ids from the condition id and index sets, and they only
        address the right balances if that derivation reproduces the tokens the
        venue trades. Verified on-chain, so it needs no credentials.
        """
        if not _live_contracts_enabled():
            self.skipTest("set ARB_RUN_LIVE_SCHEMA_CONTRACTS=1 to run live schema checks")

        detail = await _opinion_resolved_market_detail()
        if detail is None:
            self.skipTest("no resolved Opinion market exposed a usable condition id")

        web3 = _opinion_web3()
        try:
            derived = []
            for index_set in (1, 2):
                collection = await web3.call_contract(
                    OPINION_CONDITIONAL_TOKENS, CONDITIONAL_TOKENS_ABI, "getCollectionId",
                    bytes(32), bytes.fromhex(detail["conditionId"]), index_set,
                )
                position = await web3.call_contract(
                    OPINION_CONDITIONAL_TOKENS, CONDITIONAL_TOKENS_ABI, "getPositionId",
                    web3.w3.to_checksum_address(OPINION_COLLATERAL), collection,
                )
                derived.append(str(int(position)))
        finally:
            await web3.close()

        # index_set 1 is YES, index_set 2 is NO -- the ordering the connector and
        # SettlementRequest.index_sets both assume.
        self.assertEqual(derived[0], str(detail["yesTokenId"]))
        self.assertEqual(derived[1], str(detail["noTokenId"]))

    async def test_opinion_settlement_status_matches_the_chain(self) -> None:
        """A resolved market must read RESOLVED and an open one OPEN, from the payout vector."""
        if not _live_contracts_enabled():
            self.skipTest("set ARB_RUN_LIVE_SCHEMA_CONTRACTS=1 to run live schema checks")

        resolved = await _opinion_resolved_market_detail()
        activated = await _opinion_activated_market_detail()
        if resolved is None:
            self.skipTest("no resolved Opinion market exposed a usable condition id")

        web3 = _opinion_web3()
        try:
            settlement = ConditionalTokensRedemption(web3, OPINION_CONDITIONAL_TOKENS, 350_000)
            self.assertIs(
                await settlement.get_settlement_status(_opinion_settlement_request(resolved)),
                SettlementStatus.RESOLVED,
            )
            if activated is not None:
                self.assertIs(
                    await settlement.get_settlement_status(_opinion_settlement_request(activated)),
                    SettlementStatus.OPEN,
                )
        finally:
            await web3.close()

    async def test_opinion_collateral_decimals_match_the_venue_catalogue(self) -> None:
        """Never assume 6 decimals: BNB Chain USDT has 18."""
        if not _live_contracts_enabled():
            self.skipTest("set ARB_RUN_LIVE_SCHEMA_CONTRACTS=1 to run live schema checks")

        session = client_session({"Accept": "application/json"})
        try:
            async with session.get("https://openapi.opinion.trade/openapi/quoteToken", timeout=20) as response:
                payload = await response.json()
        finally:
            await session.close()
        quote_tokens = (payload.get("result") or {}).get("list") or []
        self.assertTrue(quote_tokens)
        configured = next(
            item for item in quote_tokens
            if str(item["quoteTokenAddress"]).lower() == OPINION_COLLATERAL.lower()
        )

        web3 = _opinion_web3()
        try:
            token = web3.contract(OPINION_COLLATERAL, OPINION_ERC20_ABI)
            on_chain = int(await token.functions.decimals().call())
        finally:
            await web3.close()

        self.assertEqual(on_chain, int(configured["decimal"]))

    async def test_sx_bet_market_payload_contract(self) -> None:
        if not _live_contracts_enabled():
            self.skipTest("set ARB_RUN_LIVE_SCHEMA_CONTRACTS=1 to run live schema checks")

        resolver = SxBetMarketResolver(_sx_bet_live_config(), scan_all=True)
        try:
            payloads = await resolver._fetch_markets()  # noqa: SLF001
        finally:
            await resolver.close()

        self.assertTrue(payloads)
        parsed = [item for payload in payloads[:25] if (item := _sx_market_text(payload)) is not None]
        self.assertTrue(parsed)

    async def test_sx_bet_v3_mainnet_read_only_contracts(self) -> None:
        if not _live_contracts_enabled():
            self.skipTest("set ARB_RUN_LIVE_SCHEMA_CONTRACTS=1 to run live schema checks")

        config = _sx_bet_v3_live_config()
        auth_required = os.getenv("ARB_REQUIRE_SX_V3_AUTH_CONTRACTS") == "1"
        if auth_required and not config.api_key:
            self.fail("SX_BET_API_KEY is required when ARB_REQUIRE_SX_V3_AUTH_CONTRACTS=1")
        client = SxBetV3ApiClient(config)
        try:
            metadata = await client._metadata()  # noqa: SLF001
            self.assertEqual(metadata["domain"]["version"], "1")
            self.assertEqual(metadata["activeAsset"]["decimals"], 6)
            self.assertGreater(int(metadata["limits"]["orderSizeMinimumBaseUnits"]), 0)

            market_payload = await client._request_json(  # noqa: SLF001
                "GET",
                "/markets/active",
                query_params={"pageSize": 1},
            )
            markets = _extract_sx_records(market_payload, ("markets",))
            market_hash = next(str(item.get("marketHash") or "") for item in markets if item.get("marketHash"))
            snapshot_payload = await client._request_json(  # noqa: SLF001
                "GET",
                "/orderbook-v3/snapshot",
                query_params={"marketHash": market_hash},
            )
            snapshot = snapshot_payload["data"]
            self.assertEqual(snapshot["marketHash"].lower(), market_hash.lower())
            self.assertTrue(str(snapshot["version"]))
            self.assertIsInstance(snapshot["outcomeOne"], list)
            self.assertIsInstance(snapshot["outcomeTwo"], list)
            _order_book_from_v3_maker_snapshot(snapshot, BinarySide.YES)
            _order_book_from_v3_maker_snapshot(snapshot, BinarySide.NO)

            if config.api_key:
                proxy = await client._request_json("GET", "/user/proxy")  # noqa: SLF001
                balances = await client._request_json("GET", "/user/balance-v3")  # noqa: SLF001
                fees = await client._request_json("GET", "/user/fees-v3")  # noqa: SLF001
                token = await client._request_json(  # noqa: SLF001
                    "GET",
                    "/user/realtime-token-v3/api-key",
                )
                orders = await client._request_json(  # noqa: SLF001
                    "GET",
                    "/orders-v3",
                    query_params={"perPage": 1},
                )
                fills = await client._request_json(  # noqa: SLF001
                    "GET",
                    "/fills-v3",
                    query_params={"perPage": 1},
                )
                positions = await client._request_json(  # noqa: SLF001
                    "GET",
                    "/positions-v3",
                    query_params={"status": "MATCHED,LOCKED", "perPage": 1},
                )
                self.assertIsInstance(proxy.get("data"), dict)
                self.assertIsInstance(balances.get("data", {}).get("balances"), list)
                self.assertIn("takerPayoutFee", fees.get("data", {}))
                self.assertIn("refundFee", fees.get("data", {}))
                self.assertTrue(token.get("data", {}).get("token") or token.get("token"))
                order_rows = _extract_sx_records(orders, ("orders",))
                fill_rows = _extract_sx_records(fills, ("fills",))
                self.assertIsInstance(order_rows, list)
                self.assertIsInstance(fill_rows, list)
                self.assertIsInstance(_extract_sx_records(positions, ("positions",)), list)
                self.assertTrue(
                    all(str(row.get("status") or "").upper() in {"PENDING", "ACTIVE", "INACTIVE"} for row in order_rows)
                )
                self.assertTrue(
                    all(
                        str(row.get("status") or "").upper() in {"MATCHED", "LOCKED", "SETTLED", "FAILED"}
                        for row in fill_rows
                    )
                )

                # secret-scan: allow-test-fixture
                invalid_client = SxBetV3ApiClient(replace(config, api_key="invalid-live-contract-key"))
                try:
                    with self.assertRaisesRegex(RuntimeError, "failed with 401"):
                        await invalid_client._request_json("GET", "/user/proxy")  # noqa: SLF001
                finally:
                    await invalid_client.close()
        finally:
            await client.close()


def _sx_bet_live_config() -> SxBetConfig:
    return SxBetConfig(
        enabled=True,
        api_base_url="https://api.sx.bet",
        api_key=os.getenv("SX_BET_API_KEY"),
        private_key=None,
        rpc_url="https://rpc-rollup.sx.technology",
        rpc_urls=["https://rpc-rollup.sx.technology"],
        chain_id=4162,
        base_token_address=None,
        domain_version="6.0",
        odds_slippage=0,
        taker_fee_bps=0,
        minimum_notional_usd=1.0,
        max_slippage_pct=0.015,
    )


def _sx_bet_v3_live_config() -> SxBetConfig:
    return SxBetConfig(
        enabled=True,
        api_base_url="https://api.sx.bet",
        api_key=os.getenv("SX_BET_API_KEY"),
        private_key=None,
        rpc_url="https://rpc-rollup.sx.technology",
        rpc_urls=["https://rpc-rollup.sx.technology"],
        chain_id=4162,
        ws_url="wss://realtime.sx.bet/connection/websocket",
        base_token_address=None,
        domain_version=None,
        odds_slippage=0,
        taker_fee_bps=0,
        minimum_notional_usd=1.0,
        max_slippage_pct=0.015,
        api_version="v3",
        environment="mainnet",
        time_in_force="FOK",
        allow_v3_mainnet=True,
    )
