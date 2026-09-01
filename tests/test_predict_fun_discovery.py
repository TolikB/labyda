import unittest
from datetime import UTC, datetime
from types import SimpleNamespace, TracebackType
from typing import Any
from unittest.mock import AsyncMock, patch

from arbitrage_engine.models import BinarySide, MappingStatus, MarketSpec
from arbitrage_engine.predict_fun_discovery import (
    PREDICT_MARKETS_PATH,
    PredictFunMarketResolver,
    _best_candidate,
    _extract_market_list,
    _fetch_market_page,
    _market_spec_from_payload,
    _market_specs_from_payload,
    _market_volume,
    _next_cursor,
    _optional_bool,
    _parse_datetime,
    _token_id_for_side,
)


class PredictFunDiscoveryTests(unittest.TestCase):
    def test_timezone_less_expiry_is_normalized_to_utc(self) -> None:
        parsed = _parse_datetime("2026-06-30T12:00:00")
        self.assertEqual(parsed and parsed.tzinfo, UTC)

    def test_nested_page_info_cursor_is_supported(self) -> None:
        payload = {"data": {"pageInfo": {"hasNextPage": True, "endCursor": "next-page"}}}

        self.assertEqual(_next_cursor(payload, None), "next-page")

    def test_pagination_rejects_repeated_current_cursor(self) -> None:
        payload = {"data": {"pageInfo": {"hasNextPage": True, "endCursor": "page-a"}}}

        with self.assertRaisesRegex(RuntimeError, "repeated the current cursor"):
            _next_cursor(payload, "page-a")

    def test_pagination_rejects_missing_required_next_cursor(self) -> None:
        payload = {"data": {"pageInfo": {"hasNextPage": True}}}

        with self.assertRaisesRegex(RuntimeError, "omitted the next cursor"):
            _next_cursor(payload, "page-a")

    def test_pagination_accepts_explicit_last_page(self) -> None:
        payload = {
            "data": {
                "cursor": "unused-sibling-cursor",
                "pageInfo": {"hasNextPage": False, "endCursor": "page-a"},
            }
        }

        self.assertIsNone(_next_cursor(payload, "page-a"))

    def test_pagination_rejects_conflicting_page_state(self) -> None:
        payload = {
            "hasNextPage": False,
            "data": {"pageInfo": {"hasNextPage": True, "endCursor": "page-b"}},
        }

        with self.assertRaisesRegex(RuntimeError, "conflicting page state"):
            _next_cursor(payload, "page-a")

    def test_outcome_mapping_rejects_unlabelled_index_order(self) -> None:
        candidate = {"tokenIds": ["first", "second"]}

        self.assertIsNone(_token_id_for_side(candidate, BinarySide.YES))
        self.assertIsNone(_token_id_for_side(candidate, BinarySide.NO))

    def test_token_mapping_supports_live_api_on_chain_id(self) -> None:
        payload = {
            "outcomes": [
                {"name": "Yes", "onChainId": "101"},
                {"name": "No", "onChainId": "202"},
            ]
        }

        self.assertEqual(_token_id_for_side(payload, BinarySide.YES), "101")
        self.assertEqual(_token_id_for_side(payload, BinarySide.NO), "202")

    def test_discovery_uses_current_v1_markets_endpoint(self) -> None:
        self.assertEqual(PREDICT_MARKETS_PATH, "/v1/markets")

    def test_extract_market_list_supports_wrapped_data(self) -> None:
        payload = {"data": {"markets": [{"id": "one"}, {"id": "two"}]}}

        self.assertEqual([item["id"] for item in _extract_market_list(payload)], ["one", "two"])

    def test_best_candidate_scores_symbol_and_target(self) -> None:
        market = MarketSpec(
            symbol="BTC-USD",
            target_label=">$75,000",
            polymarket_token_id="",
            polymarket_side=BinarySide.YES,
            predict_fun_token_id="",
            predict_fun_side=BinarySide.NO,
        )
        candidates: list[dict[str, Any]] = [
            {"question": "Will ETH be above 5000?"},
            {"question": "Will BTC USD be above $75,000?", "tokens": []},
        ]

        self.assertEqual(_best_candidate(candidates, market), candidates[1])

    def test_best_candidate_rejects_more_specific_superset_market(self) -> None:
        market = MarketSpec(
            symbol="Will Turkiye win the 2026 FIFA World Cup?",
            target_label="Will Turkiye win the 2026 FIFA World Cup?",
            polymarket_token_id="",
            polymarket_side=BinarySide.YES,
            predict_fun_token_id="",
            predict_fun_side=BinarySide.NO,
            expires_at=datetime(2026, 7, 19, tzinfo=UTC),
        )
        candidates = [
            {
                "id": "opening-match",
                "question": "Will Turkiye win the 2026 FIFA World Cup opening match?",
                "expiresAt": "2026-07-19T00:00:00Z",
            },
            {
                "id": "group",
                "question": "Will Turkiye win their group in the 2026 FIFA World Cup?",
                "expiresAt": "2026-07-19T00:00:00Z",
            },
        ]

        self.assertIsNone(_best_candidate(candidates, market))

    def test_best_candidate_rejects_ambiguous_equal_titles(self) -> None:
        market = MarketSpec(
            symbol="Will Turkiye win the 2026 FIFA World Cup?",
            target_label="Will Turkiye win the 2026 FIFA World Cup?",
            polymarket_token_id="",
            polymarket_side=BinarySide.YES,
            predict_fun_token_id="",
            predict_fun_side=BinarySide.NO,
        )
        candidates = [
            {"id": "one", "question": market.symbol},
            {"id": "two", "question": market.symbol},
        ]

        self.assertIsNone(_best_candidate(candidates, market))

    def test_best_candidate_requires_expiry_when_source_has_expiry(self) -> None:
        market = MarketSpec(
            symbol="Will Turkiye win the 2026 FIFA World Cup?",
            target_label="Will Turkiye win the 2026 FIFA World Cup?",
            polymarket_token_id="",
            polymarket_side=BinarySide.YES,
            predict_fun_token_id="",
            predict_fun_side=BinarySide.NO,
            expires_at=datetime(2026, 7, 19, tzinfo=UTC),
        )

        self.assertIsNone(_best_candidate([{"id": "missing-expiry", "question": market.symbol}], market))

    def test_token_id_for_side_supports_outcome_objects(self) -> None:
        candidate = {
            "tokens": [
                {"side": "YES", "tokenId": "yes-token"},
                {"side": "NO", "tokenId": "no-token"},
            ]
        }

        self.assertEqual(_token_id_for_side(candidate, BinarySide.YES), "yes-token")
        self.assertEqual(_token_id_for_side(candidate, BinarySide.NO), "no-token")

    def test_optional_bool_supports_predict_fun_neg_risk_fields(self) -> None:
        self.assertTrue(_optional_bool({"isNegRisk": "true"}, ("isNegRisk",)))
        self.assertFalse(_optional_bool({"negRisk": False}, ("negRisk",)))

    def test_market_volume_supports_live_stats_container(self) -> None:
        payload = {"stats": {"totalLiquidityUsd": "12345.67"}}

        self.assertEqual(_market_volume(payload), 12345.67)

    def test_live_style_payload_without_expiry_still_builds_shadow_candidate(self) -> None:
        payload = {
            "id": "123",
            "question": "Will Arsenal win?",
            "categorySlug": "sports",
            "status": "REGISTERED",
            "tradingStatus": "OPEN",
            "stats": {"totalLiquidityUsd": "25000"},
            "outcomes": [
                {"name": "Yes", "onChainId": "101"},
                {"name": "No", "onChainId": "202"},
            ],
        }

        market = _market_spec_from_payload(payload)

        self.assertIsNotNone(market)
        assert market is not None
        self.assertEqual(market.predict_fun_market_id, "123")
        self.assertEqual(market.predict_fun_token_id, "202")
        self.assertIsNone(market.expires_at)
        self.assertEqual(market.predict_fun_volume_usd, 25000.0)
        self.assertEqual(market.category, "sports")

    def test_official_variant_metadata_classifies_crypto_and_sports(self) -> None:
        crypto = _market_spec_from_payload(
            {
                "id": "crypto-market",
                "question": "Will BTC be up in the next hour?",
                "categorySlug": "btc-up-or-down-july-18",
                "variantData": {"type": "CRYPTO_UP_DOWN"},
                "outcomes": [
                    {"name": "Yes", "onChainId": "crypto-yes"},
                    {"name": "No", "onChainId": "crypto-no"},
                ],
            }
        )
        sports = _market_spec_from_payload(
            {
                "id": "sports-market",
                "question": "Will Team A win?",
                "categorySlug": "world-cup-final",
                "marketType": "SPORTS_MONEYLINE",
                "outcomes": [
                    {"name": "Yes", "onChainId": "sports-yes"},
                    {"name": "No", "onChainId": "sports-no"},
                ],
            }
        )

        assert crypto is not None
        assert sports is not None
        self.assertEqual(crypto.category, "crypto")
        self.assertEqual(sports.category, "sports")

    def test_sports_taxonomy_category_slugs_are_classified_as_sports(self) -> None:
        for slug in ("football", "soccer", "esports"):
            market = _market_spec_from_payload(
                {
                    "id": f"{slug}-market",
                    "question": "Will Team A win?",
                    "categorySlug": slug,
                    "outcomes": [
                        {"name": "Yes", "onChainId": f"{slug}-yes"},
                        {"name": "No", "onChainId": f"{slug}-no"},
                    ],
                }
            )

            assert market is not None
            self.assertEqual(market.category, "sports")

    def test_standard_binary_outcomes_expand_into_both_route_orientations(self) -> None:
        payload = {
            "id": "btc-market",
            "question": "Will BTC exceed 100000?",
            "outcomes": [
                {"name": "Yes", "onChainId": "btc-yes"},
                {"name": "No", "onChainId": "btc-no"},
            ],
        }

        markets = _market_specs_from_payload(payload)

        self.assertEqual(len(markets), 2)
        self.assertEqual(
            [(market.polymarket_side, market.predict_fun_side) for market in markets],
            [(BinarySide.YES, BinarySide.NO), (BinarySide.NO, BinarySide.YES)],
        )
        self.assertEqual([market.predict_fun_token_id for market in markets], ["btc-no", "btc-yes"])

    def test_market_id_aliases_must_agree_and_condition_id_is_not_api_id(self) -> None:
        outcomes = [
            {"name": "Yes", "onChainId": "btc-yes"},
            {"name": "No", "onChainId": "btc-no"},
        ]
        agreeing = {
            "id": 123,
            "marketId": "123",
            "question": "Will BTC exceed 100000?",
            "outcomes": outcomes,
        }
        conflicting = {**agreeing, "marketId": "456"}
        condition_only = {
            "conditionId": "condition-is-not-api-id",
            "question": "Will BTC exceed 100000?",
            "outcomes": outcomes,
        }

        self.assertEqual(len(_market_specs_from_payload(agreeing)), 2)
        self.assertEqual(_market_specs_from_payload(conflicting), [])
        self.assertEqual(_market_specs_from_payload(condition_only), [])

    def test_numeric_market_and_token_ids_are_canonicalized(self) -> None:
        canonical = {
            "id": "00123",
            "question": "Will BTC exceed 100000?",
            "outcomes": [
                {"name": "Yes", "onChainId": "000456"},
                {"name": "No", "onChainId": "789"},
            ],
        }
        numerically_duplicate_tokens = {
            **canonical,
            "id": "124",
            "outcomes": [
                {"name": "Yes", "onChainId": "123"},
                {"name": "No", "onChainId": "0123"},
            ],
        }

        markets = _market_specs_from_payload(canonical)

        self.assertEqual([market.predict_fun_market_id for market in markets], ["123", "123"])
        self.assertEqual([market.predict_fun_token_id for market in markets], ["789", "456"])
        self.assertEqual(_market_specs_from_payload(numerically_duplicate_tokens), [])

    def test_present_empty_or_null_identity_aliases_fail_closed(self) -> None:
        invalid_payloads: tuple[dict[str, Any], ...] = (
            {
                "id": "empty-official-token",
                "question": "Will BTC exceed 100000?",
                "outcomes": [
                    {"name": "Yes", "onChainId": "", "tokenId": "fallback-yes"},
                    {"name": "No", "onChainId": "no-token"},
                ],
            },
            {
                "id": "null-direct-token",
                "question": "Will BTC exceed 100000?",
                "yesTokenId": None,
                "yes_token_id": "yes-token",
                "noTokenId": "no-token",
            },
            {
                "id": None,
                "marketId": 123,
                "question": "Will BTC exceed 100000?",
                "outcomes": [
                    {"name": "Yes", "onChainId": "yes-token"},
                    {"name": "No", "onChainId": "no-token"},
                ],
            },
        )

        for index, payload in enumerate(invalid_payloads):
            with self.subTest(index=index):
                self.assertEqual(_market_specs_from_payload(payload), [])

    def test_generic_outcome_id_is_not_an_execution_token(self) -> None:
        payload = {
            "id": "market-id",
            "question": "Will BTC exceed 100000?",
            "outcomes": [
                {"name": "Yes", "id": "yes-row-id"},
                {"name": "No", "id": "no-row-id"},
            ],
        }

        self.assertEqual(_market_specs_from_payload(payload), [])

    def test_standard_binary_outcomes_validate_supplied_index_sets(self) -> None:
        valid = {
            "id": "valid-index-sets",
            "question": "Will BTC exceed 100000?",
            "outcomes": [
                {"name": "Yes", "onChainId": "btc-yes", "indexSet": 1},
                {"name": "No", "onChainId": "btc-no", "indexSet": 2},
            ],
        }
        invalid_outcome_sets = (
            [
                {"name": "Yes", "onChainId": "btc-yes", "indexSet": 2},
                {"name": "No", "onChainId": "btc-no", "indexSet": 1},
            ],
            [
                {"name": "Yes", "onChainId": "btc-yes", "indexSet": 1},
                {"name": "No", "onChainId": "btc-no", "indexSet": 1},
            ],
            [
                {"name": "Yes", "onChainId": "btc-yes", "indexSet": 1},
                {"name": "No", "onChainId": "btc-no"},
            ],
        )

        self.assertEqual(len(_market_specs_from_payload(valid)), 2)
        for index, outcomes in enumerate(invalid_outcome_sets):
            with self.subTest(index=index):
                self.assertEqual(
                    _market_specs_from_payload(
                        {
                            "id": f"invalid-index-sets-{index}",
                            "question": "Will BTC exceed 100000?",
                            "outcomes": outcomes,
                        }
                    ),
                    [],
                )

    def test_redundant_direct_and_outcome_token_ids_must_agree(self) -> None:
        consistent = {
            "id": "consistent-token-identities",
            "question": "Will BTC exceed 100000?",
            "yesTokenId": "btc-yes",
            "noTokenId": "btc-no",
            "outcomes": [
                {"name": "Yes", "onChainId": "btc-yes", "indexSet": 1},
                {"name": "No", "onChainId": "btc-no", "indexSet": 2},
            ],
        }
        conflicting = {
            **consistent,
            "id": "conflicting-token-identities",
            "yesTokenId": "different-yes-token",
        }

        self.assertEqual(len(_market_specs_from_payload(consistent)), 2)
        self.assertEqual(_market_specs_from_payload(conflicting), [])

    def test_conflicting_direct_token_aliases_fail_closed(self) -> None:
        payload = {
            "id": "conflicting-direct-aliases",
            "question": "Will BTC exceed 100000?",
            "yesTokenId": "btc-yes",
            "yes_token_id": "different-yes-token",
            "noTokenId": "btc-no",
        }

        self.assertEqual(_market_specs_from_payload(payload), [])

    def test_direct_yes_and_no_token_ids_must_be_distinct(self) -> None:
        payload = {
            "id": "duplicate-direct-tokens",
            "question": "Will BTC exceed 100000?",
            "yesTokenId": "same-token",
            "noTokenId": "same-token",
        }

        self.assertEqual(_market_specs_from_payload(payload), [])

    def test_direct_only_payload_requires_both_token_sides(self) -> None:
        payload = {
            "id": "one-sided-direct-token",
            "question": "Will BTC exceed 100000?",
            "noTokenId": "btc-no",
        }

        self.assertEqual(_market_specs_from_payload(payload), [])

    def test_direct_token_ids_are_string_only_trimmed_and_nonempty(self) -> None:
        trimmed = {
            "id": "trimmed-direct-tokens",
            "question": "Will BTC exceed 100000?",
            "yesTokenId": " btc-yes ",
            "noTokenId": " btc-no ",
        }
        malformed_values = ("   ", {"token": "btc-yes"})

        self.assertEqual(
            [market.predict_fun_token_id for market in _market_specs_from_payload(trimmed)],
            ["btc-no", "btc-yes"],
        )
        for value in malformed_values:
            with self.subTest(value=value):
                self.assertEqual(
                    _market_specs_from_payload(
                        {
                            "id": "malformed-direct-token",
                            "question": "Will BTC exceed 100000?",
                            "yesTokenId": value,
                            "noTokenId": "btc-no",
                        }
                    ),
                    [],
                )

    def test_conflicting_outcome_name_and_side_fail_closed(self) -> None:
        payload = {
            "id": "conflicting-name-and-side",
            "question": "Will BTC exceed 100000?",
            "outcomes": [
                {"name": "Yes", "side": "NO", "onChainId": "btc-yes", "indexSet": 1},
                {"name": "No", "side": "NO", "onChainId": "btc-no", "indexSet": 2},
            ],
        }

        self.assertEqual(_market_specs_from_payload(payload), [])

    def test_named_binary_outcomes_expand_into_two_scan_all_specs(self) -> None:
        payload = {
            "id": "world-cup-market",
            "question": "Will France win the World Cup?",
            "conditionId": "predict-condition",
            "polymarketConditionIds": ["poly-condition"],
            "oracleQuestionId": "oracle-question",
            "resolverAddress": "0xresolver",
            "categorySlug": "sports",
            "outcomes": [
                {"name": "France", "onChainId": "france-token", "indexSet": 1},
                {"name": "The Field", "onChainId": "field-token", "indexSet": 2},
            ],
        }

        markets = _market_specs_from_payload(payload)

        self.assertEqual(len(markets), 2)
        self.assertEqual([market.target_label for market in markets], ["France", "The Field"])
        self.assertEqual([market.predict_fun_token_id for market in markets], ["field-token", "france-token"])
        self.assertEqual(
            [market.predict_fun_side for market in markets],
            [BinarySide.NO, BinarySide.YES],
        )
        self.assertEqual([market.polymarket_market_id for market in markets], ["poly-condition", "poly-condition"])
        self.assertEqual(
            [market.resolution_source for market in markets],
            [
                "resolver:0xresolver;oracle_question:oracle-question",
                "resolver:0xresolver;oracle_question:oracle-question",
            ],
        )

    def test_named_binary_outcomes_use_index_set_not_api_order(self) -> None:
        payload = {
            "id": "world-cup-market",
            "question": "Will France win the World Cup?",
            "outcomes": [
                {"name": "The Field", "onChainId": "field-token", "indexSet": 2},
                {"name": "France", "onChainId": "france-token", "indexSet": 1},
            ],
        }

        markets = _market_specs_from_payload(payload)

        self.assertEqual([market.target_label for market in markets], ["France", "The Field"])
        self.assertEqual([market.predict_fun_token_id for market in markets], ["field-token", "france-token"])

    def test_named_binary_outcomes_fail_closed_without_unambiguous_index_sets(self) -> None:
        missing_index_set = {
            "id": "missing-index-set",
            "question": "Will France win the World Cup?",
            "outcomes": [
                {"name": "France", "onChainId": "france-token"},
                {"name": "The Field", "onChainId": "field-token"},
            ],
        }
        duplicate_index_set = {
            "id": "duplicate-index-set",
            "question": "Will France win the World Cup?",
            "outcomes": [
                {"name": "France", "onChainId": "france-token", "indexSet": 1},
                {"name": "The Field", "onChainId": "field-token", "indexSet": 1},
            ],
        }
        direct_ids_do_not_make_missing_indexes_safe = {
            **missing_index_set,
            "id": "missing-index-set-with-direct-ids",
            "yesTokenId": "france-token",
            "noTokenId": "field-token",
        }

        self.assertEqual(_market_specs_from_payload(missing_index_set), [])
        self.assertEqual(_market_specs_from_payload(duplicate_index_set), [])
        self.assertEqual(_market_specs_from_payload(direct_ids_do_not_make_missing_indexes_safe), [])

    def test_binary_outcomes_fail_closed_on_duplicate_identity_or_extra_rows(self) -> None:
        invalid_outcome_sets = (
            [
                {"name": "France", "onChainId": "same-token", "indexSet": 1},
                {"name": "The Field", "onChainId": "same-token", "indexSet": 2},
            ],
            [
                {"name": "France", "onChainId": "france-token", "indexSet": 1},
                {"name": " france ", "onChainId": "field-token", "indexSet": 2},
            ],
            [
                {"name": "France", "onChainId": "france-token", "indexSet": 1},
                {"name": "The Field", "onChainId": "field-token", "indexSet": 2},
                {"name": "", "onChainId": "", "indexSet": 4},
            ],
            [
                {"name": "France", "onChainId": "france-token", "indexSet": 1},
                {"name": "The Field", "onChainId": "field-token", "indexSet": 2},
                "junk",
            ],
            [
                {"name": "---", "onChainId": "france-token", "indexSet": 1},
                {"name": "The Field", "onChainId": "field-token", "indexSet": 2},
            ],
            [
                {"name": "France", "onChainId": {"token": "france-token"}, "indexSet": 1},
                {"name": "The Field", "onChainId": "field-token", "indexSet": 2},
            ],
        )

        for index, outcomes in enumerate(invalid_outcome_sets):
            with self.subTest(index=index):
                self.assertEqual(
                    _market_specs_from_payload(
                        {
                            "id": f"invalid-outcomes-{index}",
                            "question": "Will France win the World Cup?",
                            "outcomes": outcomes,
                        }
                    ),
                    [],
                )

    def test_conflicting_outcome_containers_fail_closed(self) -> None:
        payload = {
            "id": "conflicting-outcome-containers",
            "question": "Will BTC exceed 100000?",
            "outcomes": [
                {"name": "Yes", "onChainId": "outcomes-yes"},
                {"name": "No", "onChainId": "outcomes-no"},
            ],
            "tokens": [
                {"side": "YES", "tokenId": "tokens-yes"},
                {"side": "NO", "tokenId": "tokens-no"},
            ],
        }

        self.assertEqual(_market_specs_from_payload(payload), [])


class PredictFunScanAllTests(unittest.IsolatedAsyncioTestCase):
    async def test_fetch_market_page_retries_timeout_with_same_cursor(self) -> None:
        calls: list[tuple[dict[str, str | int], int]] = []

        class FakeResponse:
            status = 200
            headers: dict[str, str] = {}

            async def __aenter__(self) -> "FakeResponse":
                return self

            async def __aexit__(
                self,
                exc_type: type[BaseException] | None,
                exc: BaseException | None,
                tb: TracebackType | None,
            ) -> bool:
                return False

            def raise_for_status(self) -> None:
                return None

            async def json(self) -> dict[str, object]:
                return {"data": {"markets": [{"id": "market-1"}]}}

        class FakeSession:
            def get(
                self,
                url: str,
                *,
                params: dict[str, str | int],
                timeout: int,
            ) -> FakeResponse:
                del url
                calls.append((dict(params), timeout))
                if len(calls) == 1:
                    raise TimeoutError
                return FakeResponse()

        with patch("arbitrage_engine.predict_fun_discovery.asyncio.sleep", new=AsyncMock()) as sleep:
            payload = await _fetch_market_page(
                FakeSession(),
                "https://api.predict.fun/v1/markets",
                after="cursor-1",
                page=2,
            )

        expected_params: dict[str, str | int] = {
            "status": "OPEN",
            "includeStats": "true",
            "first": 100,
            "after": "cursor-1",
        }
        self.assertEqual(calls, [(expected_params, 30), (expected_params, 30)])
        sleep.assert_awaited_once_with(0.5)
        self.assertEqual(_extract_market_list(payload), [{"id": "market-1"}])

    async def test_fetch_market_page_does_not_retry_authentication_failure(self) -> None:
        calls = 0

        class FakeResponse:
            status = 401
            headers: dict[str, str] = {}

            async def __aenter__(self) -> "FakeResponse":
                return self

            async def __aexit__(
                self,
                exc_type: type[BaseException] | None,
                exc: BaseException | None,
                tb: TracebackType | None,
            ) -> bool:
                return False

            def raise_for_status(self) -> None:
                raise AssertionError("authentication is rejected before generic status handling")

            async def json(self) -> dict[str, object]:
                raise AssertionError("authentication failure has no JSON contract")

        class FakeSession:
            def get(
                self,
                url: str,
                *,
                params: dict[str, str | int],
                timeout: int,
            ) -> FakeResponse:
                nonlocal calls
                del url, params, timeout
                calls += 1
                return FakeResponse()

        with (
            patch("arbitrage_engine.predict_fun_discovery.asyncio.sleep", new=AsyncMock()) as sleep,
            self.assertRaisesRegex(RuntimeError, "rejected authentication \\(401\\)"),
        ):
            await _fetch_market_page(
                FakeSession(),
                "https://api.predict.fun/v1/markets",
                after=None,
                page=1,
            )

        self.assertEqual(calls, 1)
        sleep.assert_not_awaited()

    async def test_malformed_raw_token_claim_poison_colliding_valid_market(self) -> None:
        payloads: list[dict[str, Any]] = [
            {
                "id": None,
                "question": "Will the malformed market happen?",
                "outcomes": [
                    {"name": "Yes", "onChainId": "100", "tokenId": "200"},
                    {"name": "No", "onChainId": "300"},
                ],
            },
            {
                "id": "otherwise-valid-market",
                "question": "Will the otherwise valid market happen?",
                "outcomes": [
                    {"name": "Yes", "onChainId": "100"},
                    {"name": "No", "onChainId": "400"},
                ],
            },
            {
                "id": "safe-market",
                "question": "Will the safe market happen?",
                "outcomes": [
                    {"name": "Yes", "onChainId": "500"},
                    {"name": "No", "onChainId": "600"},
                ],
            },
        ]

        class Resolver(PredictFunMarketResolver):
            async def _fetch_markets(self) -> list[dict[str, Any]]:
                return payloads

        config = SimpleNamespace(api_base_url="https://example.invalid", api_key=None)

        markets = await Resolver(config, scan_all=True).resolve([])  # type: ignore[arg-type]

        self.assertEqual([market.predict_fun_market_id for market in markets], ["safe-market", "safe-market"])

    async def test_raw_catalog_conflicts_poison_markets_before_open_filtering(self) -> None:
        payloads: list[dict[str, Any]] = [
            {
                "id": "open-closed-duplicate",
                "question": "Will the open duplicate happen?",
                "tradingStatus": "OPEN",
                "outcomes": [
                    {"name": "Yes", "onChainId": "open-yes"},
                    {"name": "No", "onChainId": "open-no"},
                ],
            },
            {
                "id": "open-closed-duplicate",
                "question": "Will the closed duplicate happen?",
                "tradingStatus": "CLOSED",
                "outcomes": [
                    {"name": "Yes", "onChainId": "closed-yes"},
                    {"name": "No", "onChainId": "closed-no"},
                ],
            },
            {
                "id": 123,
                "marketId": 456,
                "question": "Will the conflicting alias row happen?",
                "tradingStatus": "OPEN",
                "outcomes": [
                    {"name": "Yes", "onChainId": "conflict-yes"},
                    {"name": "No", "onChainId": "conflict-no"},
                ],
            },
            {
                "id": "00123",
                "question": "Will the valid colliding row happen?",
                "tradingStatus": "OPEN",
                "outcomes": [
                    {"name": "Yes", "onChainId": "valid-yes"},
                    {"name": "No", "onChainId": "valid-no"},
                ],
            },
            {
                "id": "safe-market",
                "question": "Will the safe market happen?",
                "tradingStatus": "OPEN",
                "outcomes": [
                    {"name": "Yes", "onChainId": "safe-yes"},
                    {"name": "No", "onChainId": "safe-no"},
                ],
            },
        ]

        class Resolver(PredictFunMarketResolver):
            async def _fetch_markets(self) -> list[dict[str, Any]]:
                return payloads

        config = SimpleNamespace(api_base_url="https://example.invalid", api_key=None)

        markets = await Resolver(config, scan_all=True).resolve([])  # type: ignore[arg-type]

        self.assertEqual([market.predict_fun_market_id for market in markets], ["safe-market", "safe-market"])

    async def test_scan_all_rejects_duplicate_market_ids_and_cross_market_token_reuse(self) -> None:
        payloads = [
            {
                "id": "duplicate-market",
                "question": "Will duplicate A happen?",
                "outcomes": [
                    {"name": "Yes", "onChainId": "duplicate-a-yes"},
                    {"name": "No", "onChainId": "duplicate-a-no"},
                ],
            },
            {
                "id": "duplicate-market",
                "question": "Will duplicate B happen?",
                "outcomes": [
                    {"name": "Yes", "onChainId": "duplicate-b-yes"},
                    {"name": "No", "onChainId": "duplicate-b-no"},
                ],
            },
            {
                "id": "shared-token-a",
                "question": "Will shared token A happen?",
                "outcomes": [
                    {"name": "Yes", "onChainId": "shared-token"},
                    {"name": "No", "onChainId": "shared-a-no"},
                ],
            },
            {
                "id": "shared-token-b",
                "question": "Will shared token B happen?",
                "outcomes": [
                    {"name": "Yes", "onChainId": "shared-token"},
                    {"name": "No", "onChainId": "shared-b-no"},
                ],
            },
            {
                "id": "safe-market",
                "question": "Will the safe market happen?",
                "outcomes": [
                    {"name": "Yes", "onChainId": "safe-yes"},
                    {"name": "No", "onChainId": "safe-no"},
                ],
            },
        ]

        class Resolver(PredictFunMarketResolver):
            async def _fetch_markets(self) -> list[dict[str, Any]]:
                return payloads

        config = SimpleNamespace(api_base_url="https://example.invalid", api_key=None)

        markets = await Resolver(config, scan_all=True).resolve([])  # type: ignore[arg-type]

        self.assertEqual([market.predict_fun_market_id for market in markets], ["safe-market", "safe-market"])
        self.assertEqual([market.predict_fun_token_id for market in markets], ["safe-no", "safe-yes"])

    async def test_refresh_rejects_duplicate_catalog_market_id(self) -> None:
        payloads = [
            {
                "id": "duplicate-market",
                "question": "Will duplicate A happen?",
                "outcomes": [
                    {"name": "Yes", "onChainId": "duplicate-a-yes"},
                    {"name": "No", "onChainId": "duplicate-a-no"},
                ],
            },
            {
                "id": "duplicate-market",
                "question": "Will duplicate B happen?",
                "outcomes": [
                    {"name": "Yes", "onChainId": "duplicate-b-yes"},
                    {"name": "No", "onChainId": "duplicate-b-no"},
                ],
            },
        ]

        class Resolver(PredictFunMarketResolver):
            async def _fetch_markets(self) -> list[dict[str, Any]]:
                return payloads

        market = MarketSpec(
            symbol="Will duplicate B happen?",
            target_label="Will duplicate B happen?",
            polymarket_token_id="poly-token",
            polymarket_side=BinarySide.YES,
            predict_fun_token_id="duplicate-b-no",
            predict_fun_side=BinarySide.NO,
            predict_fun_market_id="duplicate-market",
        )
        config = SimpleNamespace(api_base_url="https://example.invalid", api_key=None)

        resolved = await Resolver(config, scan_all=False).resolve([market])  # type: ignore[arg-type]

        self.assertEqual(resolved[0].predict_fun_token_id, "")

    async def test_refresh_normalizes_direct_token_whitespace(self) -> None:
        payload = {
            "id": "btc-market",
            "question": "Will BTC exceed 100000?",
            "tradingStatus": "OPEN",
            "yesTokenId": " current-yes ",
            "noTokenId": " current-no ",
        }

        class Resolver(PredictFunMarketResolver):
            async def _fetch_markets(self) -> list[dict[str, Any]]:
                return [payload]

        market = MarketSpec(
            symbol="Will BTC exceed 100000?",
            target_label="Will BTC exceed 100000?",
            polymarket_token_id="poly-token",
            polymarket_side=BinarySide.YES,
            predict_fun_token_id="stale-no",
            predict_fun_side=BinarySide.NO,
            predict_fun_market_id="btc-market",
        )
        config = SimpleNamespace(api_base_url="https://example.invalid", api_key=None)

        resolved = await Resolver(config, scan_all=False).resolve([market])  # type: ignore[arg-type]

        self.assertEqual(resolved[0].predict_fun_token_id, "current-no")

    async def test_named_binary_refresh_preserves_opposite_execution_token(self) -> None:
        payload = {
            "id": "world-cup-market",
            "question": "Will France win the World Cup?",
            "tradingStatus": "OPEN",
            "outcomes": [
                {"name": "France", "onChainId": "france-token", "indexSet": 1},
                {"name": "The Field", "onChainId": "field-token", "indexSet": 2},
            ],
        }

        class Resolver(PredictFunMarketResolver):
            async def _fetch_markets(self) -> list[dict[str, Any]]:
                return [payload]

        discovered = _market_specs_from_payload(payload)
        config = SimpleNamespace(api_base_url="https://example.invalid", api_key=None)

        refreshed = await Resolver(config, scan_all=False).resolve(discovered)  # type: ignore[arg-type]

        self.assertEqual([market.target_label for market in refreshed], ["France", "The Field"])
        self.assertEqual([market.predict_fun_token_id for market in refreshed], ["field-token", "france-token"])
        self.assertEqual(
            [market.predict_fun_side for market in refreshed],
            [BinarySide.NO, BinarySide.YES],
        )

    async def test_existing_execution_metadata_is_refreshed_from_open_catalog(self) -> None:
        payloads = [
            {
                "id": "btc-market",
                "question": "Will BTC exceed 100000?",
                "tradingStatus": "OPEN",
                "feeRateBps": 125,
                "decimalPrecision": 2,
                "tokens": [
                    {"side": "YES", "tokenId": "current-yes"},
                    {"side": "NO", "tokenId": "current-no"},
                ],
            }
        ]

        class Resolver(PredictFunMarketResolver):
            async def _fetch_markets(self) -> list[dict[str, Any]]:
                return payloads

        market = MarketSpec(
            symbol="Will BTC exceed 100000?",
            target_label="Will BTC exceed 100000?",
            polymarket_token_id="poly-token",
            polymarket_side=BinarySide.YES,
            predict_fun_token_id="stale-no",
            predict_fun_side=BinarySide.NO,
            predict_fun_market_id="btc-market",
            predict_fun_fee_rate_bps=7,
            predict_fun_price_precision=6,
        )
        config = SimpleNamespace(api_base_url="https://example.invalid", api_key=None)

        resolved = await Resolver(config, scan_all=False).resolve([market])  # type: ignore[arg-type]

        self.assertEqual(resolved[0].predict_fun_token_id, "current-no")
        self.assertEqual(resolved[0].predict_fun_fee_rate_bps, 125)
        self.assertEqual(resolved[0].predict_fun_price_precision, 2)

    async def test_missing_open_catalog_market_clears_stale_execution_metadata(self) -> None:
        class Resolver(PredictFunMarketResolver):
            async def _fetch_markets(self) -> list[dict[str, Any]]:
                return [
                    {
                        "id": "different-market",
                        "question": "Will ETH exceed 10000?",
                        "tradingStatus": "OPEN",
                        "tokens": [
                            {"side": "YES", "tokenId": "eth-yes"},
                            {"side": "NO", "tokenId": "eth-no"},
                        ],
                    }
                ]

        market = MarketSpec(
            symbol="Will BTC exceed 100000?",
            target_label="Will BTC exceed 100000?",
            polymarket_token_id="poly-token",
            polymarket_side=BinarySide.YES,
            predict_fun_token_id="stale-no",
            predict_fun_side=BinarySide.NO,
            predict_fun_market_id="btc-market",
            predict_fun_fee_rate_bps=125,
            predict_fun_price_precision=2,
        )
        config = SimpleNamespace(api_base_url="https://example.invalid", api_key=None)

        resolved = await Resolver(config, scan_all=False).resolve([market])  # type: ignore[arg-type]

        self.assertEqual(resolved[0].predict_fun_market_id, "btc-market")
        self.assertEqual(resolved[0].predict_fun_token_id, "")
        self.assertIsNone(resolved[0].predict_fun_fee_rate_bps)
        self.assertIsNone(resolved[0].predict_fun_price_precision)

    async def test_verified_market_id_cannot_migrate_to_same_title_catalog_market(self) -> None:
        class Resolver(PredictFunMarketResolver):
            async def _fetch_markets(self) -> list[dict[str, Any]]:
                return [
                    {
                        "id": "replacement-market",
                        "question": "Will BTC exceed 100000?",
                        "tradingStatus": "OPEN",
                        "tokens": [
                            {"side": "YES", "tokenId": "replacement-yes"},
                            {"side": "NO", "tokenId": "replacement-no"},
                        ],
                    }
                ]

        market = MarketSpec(
            symbol="Will BTC exceed 100000?",
            target_label="Will BTC exceed 100000?",
            polymarket_token_id="poly-token",
            polymarket_side=BinarySide.YES,
            predict_fun_token_id="approved-no",
            predict_fun_side=BinarySide.NO,
            predict_fun_market_id="approved-market",
            mapping_status=MappingStatus.VERIFIED,
            verified_routes=frozenset({"polymarket_predict"}),
        )
        config = SimpleNamespace(api_base_url="https://example.invalid", api_key=None)

        resolved = await Resolver(config, scan_all=False).resolve([market])  # type: ignore[arg-type]

        self.assertEqual(resolved[0].predict_fun_market_id, "approved-market")
        self.assertEqual(resolved[0].predict_fun_token_id, "")
        self.assertEqual(resolved[0].mapping_status, MappingStatus.STALE)
        self.assertEqual(resolved[0].verified_routes, frozenset({"polymarket_predict"}))

    async def test_closed_catalog_payload_cannot_refresh_execution_metadata(self) -> None:
        class Resolver(PredictFunMarketResolver):
            async def _fetch_markets(self) -> list[dict[str, Any]]:
                return [
                    {
                        "id": "btc-market",
                        "question": "Will BTC exceed 100000?",
                        "tradingStatus": "CLOSED",
                        "tokens": [
                            {"side": "YES", "tokenId": "closed-yes"},
                            {"side": "NO", "tokenId": "closed-no"},
                        ],
                    }
                ]

        market = MarketSpec(
            symbol="Will BTC exceed 100000?",
            target_label="Will BTC exceed 100000?",
            polymarket_token_id="poly-token",
            polymarket_side=BinarySide.YES,
            predict_fun_token_id="stale-no",
            predict_fun_side=BinarySide.NO,
            predict_fun_market_id="btc-market",
        )
        config = SimpleNamespace(api_base_url="https://example.invalid", api_key=None)

        resolved = await Resolver(config, scan_all=False).resolve([market])  # type: ignore[arg-type]

        self.assertEqual(resolved[0].predict_fun_token_id, "")

    async def test_resolver_does_not_mutate_non_predict_route_tokens(self) -> None:
        class Resolver(PredictFunMarketResolver):
            async def _fetch_markets(self) -> list[dict[str, Any]]:
                return [
                    {
                        "id": "same-id",
                        "question": "Will Team A win?",
                        "tradingStatus": "OPEN",
                        "tokens": [
                            {"side": "YES", "tokenId": "predict-yes"},
                            {"side": "NO", "tokenId": "predict-no"},
                        ],
                    }
                ]

        sx_market = MarketSpec(
            symbol="Will Team A win?",
            target_label="Will Team A win?",
            polymarket_token_id="poly-token",
            polymarket_side=BinarySide.YES,
            predict_fun_token_id="sx-market:NO",
            predict_fun_side=BinarySide.NO,
            predict_fun_market_id="same-id",
            venue_b_label="SX Bet",
        )
        config = SimpleNamespace(api_base_url="https://example.invalid", api_key=None)

        resolved = await Resolver(config, scan_all=False).resolve([sx_market])  # type: ignore[arg-type]

        self.assertEqual(resolved, [sx_market])

    async def test_cross_catalog_resolution_uses_cpu_executor(self) -> None:
        payloads = [
            {
                "id": "btc-market",
                "question": "Will BTC exceed 100000?",
                "expiresAt": "2026-12-31T00:00:00Z",
                "outcomes": [
                    {"name": "Yes", "onChainId": "btc-yes"},
                    {"name": "No", "onChainId": "btc-no"},
                ],
            }
        ]

        class Resolver(PredictFunMarketResolver):
            async def _fetch_markets(self) -> list[dict[str, Any]]:
                return payloads

        calls: list[str] = []

        async def run_in_test_executor(function: Any, *args: Any, **kwargs: Any) -> Any:
            calls.append(function.__name__)
            return function(*args, **kwargs)

        market = MarketSpec(
            symbol="Will BTC exceed 100000?",
            target_label="Will BTC exceed 100000?",
            polymarket_token_id="poly-token",
            polymarket_side=BinarySide.YES,
            predict_fun_token_id="",
            predict_fun_side=BinarySide.NO,
            expires_at=datetime(2026, 12, 31, tzinfo=UTC),
        )
        config = SimpleNamespace(api_base_url="https://example.invalid", api_key=None)
        with patch(
            "arbitrage_engine.predict_fun_discovery.run_discovery_cpu",
            new=run_in_test_executor,
        ):
            resolved = await Resolver(config, scan_all=True).resolve([market])  # type: ignore[arg-type]

        self.assertIn("_resolve_market_specs", calls)
        self.assertEqual(resolved[0].predict_fun_token_id, "btc-no")

    async def test_scan_all_does_not_hide_discovery_api_failure(self) -> None:
        class Resolver(PredictFunMarketResolver):
            async def _fetch_markets(self) -> list[dict[str, Any]]:
                raise RuntimeError("authentication rejected")

        config = SimpleNamespace(api_base_url="https://api.predict.fun", api_key=None)

        with self.assertRaisesRegex(RuntimeError, "Predict.fun discovery failed"):
            await Resolver(config, scan_all=True).resolve([])  # type: ignore[arg-type]

    async def test_scan_all_returns_every_valid_api_market_without_text_filter(self) -> None:
        payloads: list[dict[str, Any]] = [
            {
                "id": "btc",
                "question": "Will BTC exceed 100000?",
                "expiresAt": "2026-12-31T00:00:00Z",
                "feeRateBps": 125,
                "decimalPrecision": 2,
                "tokens": [{"side": "YES", "tokenId": "btc-yes"}, {"side": "NO", "tokenId": "btc-no"}],
            },
            {
                "id": "election",
                "question": "Will candidate X win?",
                "expiresAt": "2026-11-01T00:00:00Z",
                "tokens": [{"side": "YES", "tokenId": "x-yes"}, {"side": "NO", "tokenId": "x-no"}],
            },
        ]

        class Resolver(PredictFunMarketResolver):
            async def _fetch_markets(self) -> list[dict[str, Any]]:
                return payloads

        config = SimpleNamespace(api_base_url="https://example.invalid", api_key=None)
        markets = await Resolver(config, scan_all=True).resolve([])  # type: ignore[arg-type]

        self.assertEqual([market.predict_fun_market_id for market in markets], ["btc", "btc", "election", "election"])
        self.assertEqual(
            [market.predict_fun_token_id for market in markets],
            ["btc-no", "btc-yes", "x-no", "x-yes"],
        )
        self.assertEqual(markets[0].predict_fun_fee_rate_bps, 125)
        self.assertEqual(markets[0].predict_fun_price_precision, 2)

    async def test_scan_all_filters_to_allowed_categories(self) -> None:
        payloads: list[dict[str, Any]] = [
            {
                "id": "match",
                "question": "Will Arsenal win?",
                "expiresAt": "2026-12-31T00:00:00Z",
                "category": "sports",
                "tokens": [{"side": "YES", "tokenId": "match-yes"}, {"side": "NO", "tokenId": "match-no"}],
            },
            {
                "id": "btc",
                "question": "Will BTC exceed 100000?",
                "expiresAt": "2026-12-31T00:00:00Z",
                "category": "finance",
                "tokens": [{"side": "YES", "tokenId": "btc-yes"}, {"side": "NO", "tokenId": "btc-no"}],
            },
        ]

        class Resolver(PredictFunMarketResolver):
            async def _fetch_markets(self) -> list[dict[str, Any]]:
                return payloads

        config = SimpleNamespace(api_base_url="https://example.invalid", api_key=None)
        markets = await Resolver(config, scan_all=True, categories_to_scan=["sport"]).resolve([])  # type: ignore[arg-type]

        self.assertEqual([market.predict_fun_market_id for market in markets], ["match", "match"])
        self.assertEqual(
            [(market.polymarket_side, market.predict_fun_side) for market in markets],
            [(BinarySide.YES, BinarySide.NO), (BinarySide.NO, BinarySide.YES)],
        )

    async def test_scan_all_does_not_drop_custom_category_slug_without_true_category(self) -> None:
        payloads: list[dict[str, Any]] = [
            {
                "id": "cz-tweets",
                "question": "Will CZ tweet between 0 and 5 times?",
                "categorySlug": "number-of-cz-tweets-jun-29th-jul-6th-2026",
                "tokens": [{"side": "YES", "tokenId": "cz-yes"}, {"side": "NO", "tokenId": "cz-no"}],
            }
        ]

        class Resolver(PredictFunMarketResolver):
            async def _fetch_markets(self) -> list[dict[str, Any]]:
                return payloads

        config = SimpleNamespace(api_base_url="https://example.invalid", api_key=None)
        markets = await Resolver(config, scan_all=True, categories_to_scan=["sports"]).resolve([])  # type: ignore[arg-type]

        self.assertEqual([market.predict_fun_market_id for market in markets], ["cz-tweets", "cz-tweets"])
        self.assertEqual(
            [(market.polymarket_side, market.predict_fun_side) for market in markets],
            [(BinarySide.YES, BinarySide.NO), (BinarySide.NO, BinarySide.YES)],
        )
        self.assertIsNone(markets[0].category)


if __name__ == "__main__":
    unittest.main()
