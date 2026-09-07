import asyncio
import unittest
from datetime import UTC, datetime
from decimal import Decimal

from arbitrage_engine.config import OpinionConfig
from arbitrage_engine.connectors.base import OrderSubmissionRejected
from arbitrage_engine.connectors.opinion import (
    OpinionClient,
    OpinionMarketMetadata,
    OpinionSubmissionUnknown,
    _submission_is_definitively_rejected,
    apply_depth_diff,
    build_order_payload,
    execution_token,
    order_book_from_payload,
    outcome_side_code,
    parse_execution_token,
    side_from_outcome_code,
    unwrap_envelope,
)
from arbitrage_engine.external_baseline import account_fingerprint
from arbitrage_engine.models import (
    BinarySide,
    MarketDataStatus,
    OrderBook,
    OrderBookLevel,
    OrderIntent,
    OrderIntentStatus,
    RedemptionIntentStatus,
    RedemptionReport,
    SettlementRequest,
    SettlementStatus,
    VenueFeeQuote,
)


def depth_diff(*, token_id: str | None, side: str, price: str, size: str) -> dict[str, object]:
    """A market.depth.diff frame in the venue's real shape.

    Mirrors opinion_clob_sdk.websocket_models.MarketDepthDiffMessage.from_dict:
    one flat price level per message, tagged with msgType.
    """
    message: dict[str, object] = {
        "msgType": "market.depth.diff",
        "marketId": 813,
        "outcomeSide": 1,
        "side": side,
        "price": price,
        "size": size,
        "timestamp": 1_700_000_000_000,
    }
    if token_id is not None:
        message["tokenId"] = token_id
    return message


SAFE_ADDRESS = "0x1111111111111111111111111111111111111111"
SIGNER_ADDRESS = "0x2222222222222222222222222222222222222222"
COLLATERAL_TOKEN = "0x55d398326f99059fF775485246999027B3197955"
CONDITIONAL_TOKENS = "0x3333333333333333333333333333333333333333"

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
    """Envelope shapes captured from the live public API.

    The published documentation describes {code, msg, result}; the venue
    actually sends {errno, errmsg, result}. Both are accepted, but errno is the
    one that matters today.
    """

    def test_live_success_envelope_returns_result(self) -> None:
        payload = {"errmsg": "", "errno": 0, "result": {"list": [], "total": 160}}

        self.assertEqual(unwrap_envelope(payload, "/market"), {"list": [], "total": 160})

    def test_documented_success_envelope_is_still_accepted(self) -> None:
        self.assertEqual(unwrap_envelope({"code": 0, "msg": "success", "result": {"a": 1}}, "/market"), {"a": 1})

    def test_application_error_raises_even_though_http_was_200(self) -> None:
        # The venue answers application errors with HTTP 200, so
        # raise_for_status never fires and this is the only error gate. A
        # missing-parameter response must not read as "no data".
        payload = {"errmsg": "token_id is required", "errno": 10000, "result": None}

        with self.assertRaises(RuntimeError) as caught:
            unwrap_envelope(payload, "/token/orderbook")

        self.assertIn("errno=10000", str(caught.exception))
        self.assertIn("token_id is required", str(caught.exception))

    def test_documented_error_envelope_also_raises(self) -> None:
        with self.assertRaises(RuntimeError) as caught:
            unwrap_envelope({"code": 40001, "msg": "invalid token"}, "/token/orderbook")

        self.assertIn("code=40001", str(caught.exception))

    def test_envelope_without_any_status_code_is_rejected(self) -> None:
        # Silently returning the body would hand callers an un-vetted payload.
        with self.assertRaises(RuntimeError):
            unwrap_envelope({"result": {"list": []}}, "/market")

    def test_non_dict_payload_is_rejected(self) -> None:
        with self.assertRaises(RuntimeError):
            unwrap_envelope([1, 2, 3], "/market")


class OpinionLiveShapeTests(unittest.TestCase):
    """Payload shapes captured verbatim from the live public API."""

    # A real activated binary market, trimmed to the fields the engine reads.
    LIVE_LISTING_MARKET = {
        "chainId": "56",
        "conditionId": "",
        "createdAt": 1788797453,
        "cutoffAt": 1788961500,
        "marketId": 37077,
        "marketTitle": "Fire Flux Esports vs Noir Verse",
        "marketType": 0,
        "noLabel": "Noir Verse",
        "noTokenId": "5207295628457562866946373758972020469628696168733710716671077224146613",
        "quoteToken": "0x55d398326f99059fF775485246999027B3197955",
        "slug": "fire-flux-esports-vs-noir-verse",
        "status": 2,
        "statusEnum": "Activated",
        "volume": "0",
        "yesLabel": "Fire Flux Esports",
        "yesTokenId": "2544770676833454159806739227049689018852259818462714864303394663615424",
        "labels": ["Esports", "cs2", "CCT Europe"],
    }

    def test_live_listing_market_parses(self) -> None:
        from arbitrage_engine.opinion_discovery import _market_text

        text = _market_text(dict(self.LIVE_LISTING_MARKET))

        assert text is not None
        self.assertEqual(text.market_id, "37077")
        self.assertEqual(text.title, "Fire Flux Esports vs Noir Verse")
        self.assertEqual(text.yes_label, "Fire Flux Esports")
        self.assertEqual(text.no_label, "Noir Verse")
        # Numeric fields arrive as ints here and as strings elsewhere; both must
        # parse. conditionId is always empty in the listing.
        self.assertIsNone(text.condition_id)
        self.assertEqual(text.expires_at.timestamp(), 1788961500)

    def test_live_order_book_parses_and_is_uncrossed(self) -> None:
        payload = {
            "asks": [{"price": "0.08", "size": "3752"}],
            "bids": [{"price": "0.054", "size": "26006.88"}],
            "market": "62462204356933f373fb",
            "timestamp": 1788798519669,
            "tokenId": YES_TOKEN,
        }

        book = order_book_from_payload(payload)

        self.assertIs(book.status, MarketDataStatus.VALID)
        self.assertEqual(book.best_bid.price, 0.054)
        self.assertEqual(book.best_ask.price, 0.08)
        self.assertLess(book.best_bid.price, book.best_ask.price)
        # Millisecond venue timestamps must become seconds.
        self.assertAlmostEqual(book.timestamp, 1788798519.669, places=2)

    def test_market_detail_body_is_unwrapped_from_data(self) -> None:
        from arbitrage_engine.connectors.opinion import _market_detail_body

        detail = {"data": {"marketId": 36918, "conditionId": "ab" * 32}}

        body = _market_detail_body(detail)

        assert body is not None
        self.assertEqual(body["marketId"], 36918)

    def test_market_detail_body_tolerates_an_unnested_object(self) -> None:
        from arbitrage_engine.connectors.opinion import _market_detail_body

        self.assertIsNotNone(_market_detail_body({"marketId": 1}))
        self.assertIsNone(_market_detail_body({"unrelated": 1}))
        self.assertIsNone(_market_detail_body(None))


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

    def test_peak_matches_the_on_chain_fee_formula(self) -> None:
        """Our curve must peak at exactly the rate the FeeManager encodes.

        The contract stores takerFeeRateBps and the SDK derives the maximum rate
        as ``bps * 0.25 / 10000`` -- the 0.25 being ``price * (1 - price)`` at
        50c. Live reads return 400 bps, so the peak is 1% **per share**, which is
        what this model charges. Note that as a share of *notional* the same fee
        is 1/price times larger, so a 400 bps market costs 2% of notional at 50c
        and 3.7% at 8c: cheap-side books are expensive to take.
        """
        quote = VenueFeeQuote("Opinion", 400, "opinion_curve", verified=True)
        on_chain_peak_rate = Decimal(400) * Decimal("0.25") / Decimal(10_000)

        shares = Decimal(200)
        peak_fee = quote.fee_for_fill(shares, Decimal("0.5"))

        self.assertEqual(on_chain_peak_rate, Decimal("0.01"))
        self.assertEqual(peak_fee / shares, on_chain_peak_rate)
        # And the curve really is a peak: moving either way off 50c is cheaper.
        self.assertLess(quote.fee_for_fill(shares, Decimal("0.2")), peak_fee)
        self.assertLess(quote.fee_for_fill(shares, Decimal("0.8")), peak_fee)

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


class OpinionBalanceTests(unittest.IsolatedAsyncioTestCase):
    def _client_with_fake_chain(self, raw_balance: int, decimals: int) -> OpinionClient:
        client = OpinionClient(
            make_config(
                private_key="11" * 32,
                multi_sig_address=SAFE_ADDRESS,
                collateral_token_address=COLLATERAL_TOKEN,
            )
        )

        class FakeCall:
            def __init__(self, value: object) -> None:
                self._value = value

            async def call(self) -> object:
                return self._value

        class FakeFunctions:
            def balanceOf(self, address: str) -> FakeCall:  # noqa: N802 - ERC-20 ABI name
                assert address == SAFE_ADDRESS
                return FakeCall(raw_balance)

            def decimals(self) -> FakeCall:
                return FakeCall(decimals)

        class FakeToken:
            functions = FakeFunctions()

        class FakeAccount:
            address = SIGNER_ADDRESS

        class FakeWeb3:
            account = FakeAccount()

            class w3:  # noqa: N801 - mirrors the web3 attribute name
                @staticmethod
                def to_checksum_address(value: str) -> str:
                    return value

            def contract(self, address: str, abi: object) -> FakeToken:
                del abi
                assert address == COLLATERAL_TOKEN
                return FakeToken()

        client._web3_client = FakeWeb3()  # type: ignore[assignment]  # noqa: SLF001
        return client

    async def test_balance_is_read_on_chain_at_the_safe_address(self) -> None:
        client = self._client_with_fake_chain(raw_balance=125_500_000, decimals=6)

        details = await client.get_cash_balance_details()

        # Opinion names the Safe as order maker, so collateral sits there and
        # not at the signer EOA.
        self.assertEqual(details["wallet_address"], SAFE_ADDRESS)
        self.assertEqual(details["signer_wallet_address"], SIGNER_ADDRESS)
        self.assertEqual(details["balance"], 125.5)
        self.assertEqual(details["balance_raw"], "125500000")
        self.assertEqual(details["decimals"], 6)
        self.assertEqual(await client.get_cash_balance(), 125.5)

    async def test_decimals_are_read_from_the_token_not_assumed(self) -> None:
        client = self._client_with_fake_chain(raw_balance=10**19, decimals=18)

        self.assertEqual(await client.get_cash_balance(), 10.0)

    async def test_lower_case_safe_address_is_checksummed(self) -> None:
        # web3 rejects non-checksum addresses outright, and a Safe address copied
        # from the venue or an env file is routinely lower-case.
        seen: list[str] = []

        class FakeCall:
            def __init__(self, value: object) -> None:
                self._value = value

            async def call(self) -> object:
                return self._value

        class FakeFunctions:
            def balanceOf(self, address: str) -> FakeCall:  # noqa: N802 - ERC-20 ABI name
                seen.append(address)
                return FakeCall(5 * 10**18)

            def decimals(self) -> FakeCall:
                return FakeCall(18)

        class FakeWeb3:
            account = None

            class w3:  # noqa: N801 - mirrors the web3 attribute name
                @staticmethod
                def to_checksum_address(value: str) -> str:
                    return value.upper().replace("0X", "0x")

            def contract(self, address: str, abi: object) -> object:
                del address, abi
                return type("Token", (), {"functions": FakeFunctions()})()

        client = OpinionClient(
            make_config(
                private_key="11" * 32,
                multi_sig_address=SAFE_ADDRESS.lower(),
                collateral_token_address=COLLATERAL_TOKEN,
            )
        )
        client._web3_client = FakeWeb3()  # type: ignore[assignment]  # noqa: SLF001

        details = await client.get_cash_balance_details()

        self.assertEqual(seen, [SAFE_ADDRESS.upper().replace("0X", "0x")])
        self.assertEqual(details["balance"], 5.0)

    async def test_missing_collateral_or_wallet_config_fails_closed(self) -> None:
        for overrides in (
            {"collateral_token_address": None, "multi_sig_address": "0xSafe"},
            {"collateral_token_address": COLLATERAL_TOKEN, "multi_sig_address": None, "account_address": None},
        ):
            with self.assertRaises(RuntimeError):
                await OpinionClient(make_config(**overrides)).get_cash_balance_details()


class OpinionSettlementTests(unittest.IsolatedAsyncioTestCase):
    SETTLEMENT_CONFIG = {
        "private_key": "11" * 32,
        "multi_sig_address": SAFE_ADDRESS,
        "conditional_tokens_address": CONDITIONAL_TOKENS,
        "collateral_token_address": COLLATERAL_TOKEN,
    }
    CONDITION_ID = "ab" * 32

    def _client(self, **overrides: object) -> OpinionClient:
        settings = {**self.SETTLEMENT_CONFIG, **overrides}
        return OpinionClient(make_config(**settings))

    def _with_metadata(self, condition_id: str | None) -> OpinionClient:
        client = self._client()
        client._market_metadata[813] = OpinionMarketMetadata(  # noqa: SLF001
            market_id=813, condition_id=condition_id, yes_token_id=YES_TOKEN, no_token_id=NO_TOKEN
        )
        return client

    def _request(self, market_id: str = "813") -> SettlementRequest:
        return SettlementRequest(
            position_key="key",
            venue="Opinion",
            market_id=market_id,
            condition_id=market_id,
            collateral_token="",
            expected_contracts=Decimal("10"),
        )

    async def test_redemption_requires_the_full_safe_topology(self) -> None:
        self.assertTrue(self._client().supports_automatic_redemption())
        for missing in self.SETTLEMENT_CONFIG:
            self.assertFalse(
                self._client(**{missing: None}).supports_automatic_redemption(),
                msg=f"{missing} must be required for automatic redemption",
            )

    async def test_prepare_fills_collateral_from_config(self) -> None:
        prepared = self._client().prepare_settlement_request(self._request())

        self.assertEqual(prepared.collateral_token, COLLATERAL_TOKEN)
        self.assertEqual(prepared.index_sets, (1, 2))

    async def test_prepare_fails_closed_without_redemption_config(self) -> None:
        with self.assertRaises(RuntimeError):
            self._client(multi_sig_address=None).prepare_settlement_request(self._request())

    async def test_market_id_is_swapped_for_the_on_chain_condition_id(self) -> None:
        client = self._with_metadata(self.CONDITION_ID)

        resolved = await client._resolved_settlement_request(self._request())  # noqa: SLF001

        # Conditional Tokens is keyed by the 32-byte condition id, never by the
        # venue's numeric market id.
        self.assertEqual(resolved.condition_id, f"0x{self.CONDITION_ID}")
        self.assertEqual(resolved.collateral_token, COLLATERAL_TOKEN)

    async def test_condition_id_is_normalised_when_the_venue_omits_the_prefix(self) -> None:
        client = self._with_metadata(f"0X{self.CONDITION_ID.upper()}")

        resolved = await client._resolved_settlement_request(self._request())  # noqa: SLF001

        self.assertEqual(resolved.condition_id, f"0x{self.CONDITION_ID}")

    async def test_missing_or_malformed_condition_id_fails_closed(self) -> None:
        for condition_id in (None, "", "0xdeadbeef"):
            client = self._with_metadata(condition_id)
            with self.assertRaises(RuntimeError):
                await client._resolved_settlement_request(self._request())  # noqa: SLF001

    async def test_non_numeric_market_id_fails_closed(self) -> None:
        client = self._with_metadata(self.CONDITION_ID)

        with self.assertRaises(RuntimeError):
            await client._resolved_settlement_request(self._request(market_id="not-a-market"))  # noqa: SLF001

    async def test_settlement_calls_delegate_with_the_resolved_request(self) -> None:
        client = self._with_metadata(self.CONDITION_ID)
        seen: list[SettlementRequest] = []

        class FakeSettlement:
            async def get_settlement_status(self, request: SettlementRequest) -> SettlementStatus:
                seen.append(request)
                return SettlementStatus.RESOLVED

            async def redeem_position(self, request: SettlementRequest, redemption_id: str) -> RedemptionReport:
                del redemption_id
                seen.append(request)
                return RedemptionReport(RedemptionIntentStatus.SUBMITTED, tx_hash="0xtx")

            async def reconcile(
                self, request: SettlementRequest, report: RedemptionReport
            ) -> RedemptionReport:
                del report
                seen.append(request)
                return RedemptionReport(RedemptionIntentStatus.CONFIRMED, tx_hash="0xtx")

        client._settlement = FakeSettlement()  # type: ignore[assignment]  # noqa: SLF001
        request = self._request()

        self.assertIs(await client.get_settlement_status(request), SettlementStatus.RESOLVED)
        submitted = await client.redeem_position(request, "redemption-1")
        confirmed = await client.reconcile_redemption(request, submitted)

        self.assertIs(submitted.status, RedemptionIntentStatus.SUBMITTED)
        self.assertIs(confirmed.status, RedemptionIntentStatus.CONFIRMED)
        self.assertEqual({item.condition_id for item in seen}, {f"0x{self.CONDITION_ID}"})
        self.assertEqual({item.collateral_token for item in seen}, {COLLATERAL_TOKEN})

    async def test_settlement_client_targets_the_safe_not_the_signer(self) -> None:
        client = self._client()

        settlement = client._get_settlement_client()  # noqa: SLF001

        # Outcome tokens are held by the Safe; redeeming from the signer would
        # redeem the wrong account.
        self.assertEqual(settlement._safe_address.lower(), SAFE_ADDRESS.lower())  # noqa: SLF001


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
        for target in (yes_target, no_target):
            client._store_book(  # noqa: SLF001
                target,
                order_book_from_payload(
                    {"bids": [{"price": "0.40", "size": "1"}], "asks": [{"price": "0.60", "size": "1"}]}
                ),
            )

        client._handle_ws_payload(depth_diff(token_id=YES_TOKEN, side="bids", price="0.44", size="10"))  # noqa: SLF001

        self.assertEqual(
            [(level.price, level.size) for level in client._books[yes_target].bids],  # noqa: SLF001
            [(0.44, 10.0), (0.40, 1.0)],
        )
        self.assertEqual(
            [(level.price, level.size) for level in client._books[no_target].bids],  # noqa: SLF001
            [(0.40, 1.0)],
        )

    async def test_depth_push_for_an_unbootstrapped_target_is_dropped(self) -> None:
        client = OpinionClient(make_config())
        target = execution_token(813, YES_TOKEN)
        client.sync_market_data_targets({target})

        client._handle_ws_payload(depth_diff(token_id=YES_TOKEN, side="bids", price="0.44", size="10"))  # noqa: SLF001

        # A single level is not a book; only a REST snapshot may seed one.
        self.assertNotIn(target, client._books)  # noqa: SLF001

    async def test_non_depth_message_never_touches_the_cached_book(self) -> None:
        client = OpinionClient(make_config())
        target = execution_token(813, YES_TOKEN)
        client.sync_market_data_targets({target})
        client._store_book(  # noqa: SLF001
            target,
            order_book_from_payload({"bids": [{"price": "0.40", "size": "1"}], "asks": []}),
        )

        # market.last.price carries a price but no book side; parsing it as a
        # depth diff would invalidate the book.
        client._handle_ws_payload(  # noqa: SLF001
            {
                "msgType": "market.last.price",
                "marketId": 813,
                "tokenId": YES_TOKEN,
                "outcomeSide": 1,
                "price": "0.52",
                "timestamp": 1_700_000_000_000,
            }
        )

        book = client._books[target]  # noqa: SLF001
        self.assertIs(book.status, MarketDataStatus.VALID)
        self.assertEqual([(level.price, level.size) for level in book.bids], [(0.40, 1.0)])

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

    async def test_full_reconciliation_is_unconditionally_supported(self) -> None:
        # Every proven venue reports True unconditionally; config validation is
        # what rejects a funded route with missing credentials, so a conditional
        # False here would only turn a precise config error into a generic
        # permanent risk pause.
        self.assertTrue(OpinionClient(make_config()).supports_full_reconciliation())

    async def test_account_fingerprint_matches_the_external_baseline_contract(self) -> None:
        client = OpinionClient(make_config(account_address="0xAbC"))

        fingerprint = client.reconciliation_account_fingerprint()

        assert fingerprint is not None
        # canonical_external_baseline_payload rejects anything but a full
        # SHA-256 digest, so a truncated fingerprint makes an Opinion external
        # baseline impossible to capture.
        self.assertEqual(len(fingerprint), 64)
        self.assertTrue(all(character in "0123456789abcdef" for character in fingerprint))
        self.assertEqual(fingerprint, account_fingerprint("Opinion", "0xAbC"))
        self.assertNotEqual(fingerprint, account_fingerprint("Polymarket", "0xAbC"))
        self.assertIsNone(OpinionClient(make_config()).reconciliation_account_fingerprint())

    async def test_depth_push_by_outcome_side_needs_cached_market_tokens(self) -> None:
        client = OpinionClient(make_config())
        yes_target = execution_token(813, YES_TOKEN)
        client.sync_market_data_targets({yes_target})
        client._store_book(  # noqa: SLF001
            yes_target,
            order_book_from_payload({"bids": [{"price": "0.40", "size": "1"}], "asks": []}),
        )
        push = depth_diff(token_id=None, side="bids", price="0.44", size="10")

        client._handle_ws_payload(push)  # noqa: SLF001
        self.assertEqual(len(client._books[yes_target].bids), 1)  # noqa: SLF001

        client._remember_market_metadata(813, YES_TOKEN, BinarySide.YES)  # noqa: SLF001
        client._handle_ws_payload(push)  # noqa: SLF001

        self.assertEqual(len(client._books[yes_target].bids), 2)  # noqa: SLF001

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

    async def test_ambiguous_submission_failures_are_not_proven_rejections(self) -> None:
        # The SDK routes every failure at or after its POST through one wrapper,
        # transport timeouts included. Treating those as proven rejections would
        # book a terminal CANCELLED for a possibly live, unhedged leg.
        class OpenApiError(Exception):
            pass

        class InvalidParamError(Exception):
            pass

        post_transport = OpenApiError("Failed to place order: HTTPSConnectionPool read timed out")
        pre_transport = OpenApiError("Cannot place order on different chain")

        self.assertFalse(_submission_is_definitively_rejected(post_transport))
        self.assertTrue(_submission_is_definitively_rejected(pre_transport))
        self.assertTrue(_submission_is_definitively_rejected(InvalidParamError("price must be positive")))
        self.assertFalse(_submission_is_definitively_rejected(TimeoutError("read timeout")))

    async def test_unparseable_submission_response_is_unknown_not_rejected(self) -> None:
        client = OpinionClient(make_config(private_key="11" * 32, multi_sig_address="0xsafe"))

        class FakeClob:
            def place_order(self, _data: object) -> dict[str, str]:
                return {"unexpected": "shape"}

        client._clob_client = FakeClob()  # noqa: SLF001

        with self.assertRaises(OpinionSubmissionUnknown):
            await client.buy(execution_token(813, YES_TOKEN), BinarySide.YES, 10.0, 0.55)

    async def test_order_payload_matches_the_vendored_sdk_contract(self) -> None:
        # Pins our locally built payload against the real SDK model so an SDK
        # upgrade that renames or re-types a field fails here rather than at the
        # first funded submission.
        from arbitrage_engine.connectors.opinion import _place_order_input

        payload = build_order_payload(
            market_id=813,
            outcome_token=YES_TOKEN,
            action="BUY",
            contracts=Decimal("20"),
            limit_price=Decimal("0.55"),
            price_precision=2,
        )

        data = _place_order_input(payload)

        self.assertEqual(data.marketId, 813)
        self.assertEqual(data.tokenId, YES_TOKEN)
        self.assertEqual(data.price, "0.55")
        # BUY spends quote token: contracts * price.
        self.assertEqual(data.makerAmountInQuoteToken, "11.000000")
        self.assertGreaterEqual(float(data.makerAmountInQuoteToken), 1.0)

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
