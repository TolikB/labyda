import tempfile
import unittest
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from predict_sdk.constants import Side, SignatureType
from predict_sdk.types import SignedOrder

from arbitrage_engine.config import PredictFunConfig
from arbitrage_engine.connectors.predict_fun import (
    PredictFunApiClient,
    _extract_first_nested,
    _load_abi,
    _normalize_order_amount,
    _parse_reserves,
    _to_precision_units,
)


class PredictFunTests(unittest.TestCase):
    def test_load_abi_supports_plain_list(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "abi.json"
            path.write_text('[{"type":"function","name":"buy"}]', encoding="utf-8")

            abi = _load_abi(str(path))

            self.assertEqual(abi[0]["name"], "buy")

    def test_load_abi_supports_artifact_object(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "artifact.json"
            path.write_text('{"abi":[{"type":"function","name":"sell"}]}', encoding="utf-8")

            abi = _load_abi(str(path))

            self.assertEqual(abi[0]["name"], "sell")

    def test_parse_reserves_scales_wei(self) -> None:
        yes, no = _parse_reserves((10**21, 2 * 10**21))

        self.assertEqual(yes, 1000)
        self.assertEqual(no, 2000)

    def test_to_precision_units_uses_decimal_math(self) -> None:
        self.assertEqual(_to_precision_units(0.42, 18), 420_000_000_000_000_000)
        self.assertEqual(_to_precision_units(5.0, 18), 5 * 10**18)

    def test_normalize_order_amount_supports_wei_and_human_units(self) -> None:
        self.assertEqual(_normalize_order_amount(40.0, 100.0, 18), 40.0)
        self.assertEqual(_normalize_order_amount(40 * 10**18, 100.0, 18), 40.0)

    def test_extract_first_nested_supports_wrapped_order_responses(self) -> None:
        payload = {"data": {"order": {"orderId": "abc", "status": "filled"}}}

        self.assertEqual(_extract_first_nested(payload, ("orderId",)), "abc")
        self.assertEqual(_extract_first_nested(payload, ("status",)), "filled")

    def test_build_signed_order_payload_uses_predict_sdk_limit_order(self) -> None:
        calls: dict[str, Any] = {}

        class FakeBuilder:
            def get_limit_order_amounts(self, data: Any) -> Any:
                calls["limit"] = data
                return _Amounts(maker_amount=2500000000000000000, taker_amount=10000000000000000000)

            def build_order(self, strategy: str, data: Any) -> Any:
                calls["strategy"] = strategy
                calls["order_input"] = data
                return object()

            def build_typed_data(self, order: Any, *, is_neg_risk: bool, is_yield_bearing: bool) -> Any:
                calls["typed"] = (is_neg_risk, is_yield_bearing)
                return object()

            def sign_typed_data_order(self, typed_data: Any) -> SignedOrder:
                return SignedOrder(
                    salt="1",
                    maker="0xmaker",
                    signer="0xsigner",
                    taker="0x0000000000000000000000000000000000000000",
                    token_id="123",
                    maker_amount="2500000000000000000",
                    taker_amount="10000000000000000000",
                    expiration="4102444800",
                    nonce="0",
                    fee_rate_bps="0",
                    side=Side.BUY,
                    signature_type=SignatureType.EOA,
                    signature="0xsig",
                )

        client = PredictFunApiClient(_predict_config(), order_builder_factory=FakeBuilder)

        payload = client._build_signed_order_payload(
            token_id="123",
            contracts=10.0,
            limit_price=0.25,
            sdk_side_name="BUY",
            neg_risk=True,
        )

        self.assertEqual(calls["strategy"], "MARKET")
        self.assertEqual(calls["typed"], (True, False))
        self.assertEqual(payload["tokenId"], "123")
        self.assertEqual(payload["makerAmount"], "2500000000000000000")
        self.assertEqual(payload["takerAmount"], "10000000000000000000")
        self.assertEqual(payload["side"], 0)
        self.assertEqual(payload["signature"], "0xsig")


if __name__ == "__main__":
    unittest.main()


@dataclass(frozen=True)
class _Amounts:
    maker_amount: int
    taker_amount: int


def _predict_config() -> PredictFunConfig:
    return PredictFunConfig(
        enabled=True,
        private_key="0x" + "1" * 64,
        rpc_url="https://bsc-dataseed.binance.org",
        rpc_urls=["https://bsc-dataseed.binance.org"],
        chain_id=56,
        network="mainnet",
        api_base_url="https://api.predict.fun/",
        api_key="key",
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
