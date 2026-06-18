import unittest

from arbitrage_engine.myriad_discovery import _extract_market_list, _market_text


class MyriadDiscoveryTests(unittest.TestCase):
    def test_extract_market_list_supports_wrapped_data(self) -> None:
        payload = {"data": {"markets": [{"id": 1}, {"id": 2}]}}

        self.assertEqual([item["id"] for item in _extract_market_list(payload)], [1, 2])

    def test_market_text_reads_common_myriad_fields(self) -> None:
        market = _market_text(
            {
                "marketId": 123,
                "question": "Will BTC close above $75,000?",
                "expiresAt": "2026-06-30T12:00:00Z",
                "outcomes": [{"name": "YES"}, {"name": "NO"}],
            }
        )

        self.assertIsNotNone(market)
        assert market is not None
        self.assertEqual(market.market_id, "123")
        self.assertEqual(market.yes_label, "YES")
        self.assertEqual(market.no_label, "NO")


if __name__ == "__main__":
    unittest.main()
