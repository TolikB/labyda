from __future__ import annotations

import asyncio
from dataclasses import asdict
from decimal import Decimal
import json
import logging
from pathlib import Path
import secrets
from typing import Any, Callable

from arbitrage_engine.config import PredictFunConfig
from arbitrage_engine.connectors.base import PredictFunClient
from arbitrage_engine.connectors.web3_base import BaseWeb3Client
from arbitrage_engine.models import BinarySide, ExecutionReport, OrderBook, OrderBookLevel

LOGGER = logging.getLogger(__name__)
ERC20_BALANCE_ABI: list[dict[str, Any]] = [
    {
        "constant": True,
        "inputs": [{"name": "account", "type": "address"}],
        "name": "balanceOf",
        "outputs": [{"name": "", "type": "uint256"}],
        "type": "function",
    },
    {
        "constant": True,
        "inputs": [],
        "name": "decimals",
        "outputs": [{"name": "", "type": "uint8"}],
        "type": "function",
    },
]


class PredictFunApiClient(PredictFunClient):
    def __init__(self, config: PredictFunConfig, order_builder_factory: Callable[[], Any] | None = None) -> None:
        self._config = config
        self._web3_client: BaseWeb3Client | None = None
        self._order_builder_factory = order_builder_factory
        self._order_builder: Any | None = None
        self._market_abi: list[dict[str, Any]] | None = None
        self._collateral_decimals: int | None = None
        self._order_amounts: dict[str, float] = {}

    async def watch_order_book(self, token_id: str) -> OrderBook:
        if self._config.api_base_url:
            try:
                return await self._watch_order_book_rest(token_id)
            except Exception:
                if self._config.market_abi_path:
                    LOGGER.exception("predict_fun_rest_orderbook_failed_using_rpc", extra={"_token_id": token_id})
                    return await self._watch_order_book_rpc(token_id)
                raise
        return await self._watch_order_book_rpc(token_id)

    async def buy(
        self,
        token_id: str,
        side: BinarySide,
        contracts: float,
        max_price: float,
        *,
        condition_id: str | None = None,
        tick_size: str | None = None,
        neg_risk: bool | None = None,
    ) -> str:
        del condition_id, tick_size
        return await self._submit_sdk_order(
            token_id,
            side,
            contracts,
            max_price,
            sdk_side_name="BUY",
            neg_risk=bool(neg_risk),
        )

    async def sell(
        self,
        token_id: str,
        side: BinarySide,
        contracts: float,
        min_price: float,
        *,
        condition_id: str | None = None,
        tick_size: str | None = None,
        neg_risk: bool | None = None,
    ) -> str:
        del condition_id, tick_size
        return await self._submit_sdk_order(
            token_id,
            side,
            contracts,
            min_price,
            sdk_side_name="SELL",
            neg_risk=bool(neg_risk),
        )

    async def wait_filled(self, order_id: str, timeout_ms: int) -> ExecutionReport:
        if not self._config.api_base_url:
            raise RuntimeError("predict_fun.api_base_url is required to poll Predict.fun orders")
        deadline = asyncio.get_running_loop().time() + timeout_ms / 1000
        requested = self._order_amounts.get(order_id, 0.0)
        last_filled = 0.0
        last_status = "pending"
        while asyncio.get_running_loop().time() < deadline:
            payload = await self._request_json("GET", f"/v1/orders/{order_id}")
            status = str(_extract_first_nested(payload, ("status", "state", "orderStatus", "order_status")) or "").lower()
            last_status = status or last_status
            parsed_filled = _extract_filled_amount(payload)
            if parsed_filled is not None:
                parsed_filled = _normalize_order_amount(parsed_filled, requested, self._config.precision)
                last_filled = max(last_filled, parsed_filled)
            if status in {"filled", "matched", "executed", "complete", "completed"}:
                return ExecutionReport.from_amounts(order_id, requested, parsed_filled or requested, status)
            if status in {"cancelled", "canceled", "expired", "rejected", "failed"}:
                return ExecutionReport.from_amounts(order_id, requested, last_filled, status)
            await asyncio.sleep(0.25)
        return ExecutionReport.from_amounts(order_id, requested, last_filled, last_status)

    async def cancel_order(self, order_id: str) -> None:
        if not self._config.api_base_url:
            raise RuntimeError("predict_fun.api_base_url is required to cancel Predict.fun orders")
        await self._request_json("POST", f"/v1/orders/{order_id}/cancel")

    async def get_cash_balance(self) -> float:
        return await self._get_collateral_balance()

    async def _watch_order_book_rest(self, token_id: str) -> OrderBook:
        if not self._config.api_base_url:
            raise RuntimeError("predict_fun.api_base_url is required for REST orderbook access")
        last_error: Exception | None = None
        for path in (
            f"/v1/orderbook/{token_id}",
            f"/v1/orderbooks/{token_id}",
            f"/v1/markets/{token_id}/orderbook",
            f"/v1/markets/{token_id}",
        ):
            try:
                payload = await self._request_json("GET", path)
                book = _order_book_from_payload(payload)
                if book.bids and book.asks:
                    return book
            except Exception as exc:
                last_error = exc
        if last_error is not None:
            raise last_error
        raise RuntimeError(f"Predict.fun REST API did not return an order book for token {token_id}")

    async def _watch_order_book_rpc(self, token_id: str) -> OrderBook:
        if not self._config.market_abi_path:
            raise RuntimeError("predict_fun.market_abi_path is required for direct RPC price reads")
        contract = self._get_web3_client().contract(token_id, self._get_market_abi())
        reserves = await getattr(contract.functions, self._config.reserves_function)().call()
        yes_reserve, no_reserve = _parse_reserves(reserves)
        yes_price = no_reserve / (yes_reserve + no_reserve)
        no_price = yes_reserve / (yes_reserve + no_reserve)
        synthetic_size = min(yes_reserve, no_reserve)
        return OrderBook(
            bids=[
                OrderBookLevel(price=max(0.0, yes_price - 0.001), size=synthetic_size),
                OrderBookLevel(price=max(0.0, no_price - 0.001), size=synthetic_size),
            ],
            asks=[
                OrderBookLevel(price=yes_price, size=synthetic_size),
                OrderBookLevel(price=no_price, size=synthetic_size),
            ],
        )

    async def _submit_sdk_order(
        self, token_id: str, side: BinarySide, contracts: float, limit_price: float, *, sdk_side_name: str, neg_risk: bool
    ) -> str:
        if not self._config.private_key:
            raise RuntimeError("PREDICT_FUN_PRIVATE_KEY is required for Predict.fun production orders")
        if not self._config.api_base_url:
            raise RuntimeError("predict_fun.api_base_url is required for Predict.fun order submission")
        payload = self._build_signed_order_payload(
            token_id=token_id,
            contracts=contracts,
            limit_price=limit_price,
            sdk_side_name=sdk_side_name,
            neg_risk=neg_risk,
        )
        payload["outcomeSide"] = side.value
        response = await self._request_json("POST", "/v1/orders", json_body=payload)
        order_id = _extract_first_nested(response, ("order_id", "orderId", "id", "hash"))
        if not order_id:
            raise RuntimeError(f"Predict.fun order response does not include an order id: {response!r}")
        normalized_order_id = str(order_id)
        self._order_amounts[normalized_order_id] = contracts
        return normalized_order_id

    def _build_signed_order_payload(
        self, *, token_id: str, contracts: float, limit_price: float, sdk_side_name: str, neg_risk: bool
    ) -> dict[str, Any]:
        builder = self._get_order_builder()
        sdk_side = _sdk_side(sdk_side_name)
        amounts = builder.get_limit_order_amounts(
            _sdk_limit_helper_input(
                side=sdk_side,
                price_per_share_wei=_to_precision_units(limit_price, self._config.precision),
                quantity_wei=_to_precision_units(contracts, self._config.precision),
            )
        )
        order = builder.build_order(
            "MARKET",
            _sdk_build_order_input(
                side=sdk_side,
                token_id=token_id,
                maker_amount=str(amounts.maker_amount),
                taker_amount=str(amounts.taker_amount),
                fee_rate_bps=str(self._config.fee_rate_bps),
            ),
        )
        typed_data = builder.build_typed_data(order, is_neg_risk=neg_risk, is_yield_bearing=False)
        signed_order = builder.sign_typed_data_order(typed_data)
        return _signed_order_to_payload(signed_order)

    def _get_order_builder(self) -> Any:
        if self._order_builder is not None:
            return self._order_builder
        if self._order_builder_factory is not None:
            self._order_builder = self._order_builder_factory()
            return self._order_builder
        try:
            from eth_account import Account
            from predict_sdk.constants import ADDRESSES_BY_CHAIN_ID
            from predict_sdk.logger import Logger
            from predict_sdk.order_builder import OrderBuilder
        except ImportError as exc:
            raise RuntimeError("predict-sdk is required for Predict.fun order signing") from exc
        chain_id = _sdk_chain_id(self._config.chain_id)
        self._order_builder = OrderBuilder(
            chain_id=chain_id,
            precision=self._config.precision,
            addresses=ADDRESSES_BY_CHAIN_ID[chain_id],
            generate_salt_fn=_generate_order_salt,
            logger=Logger("INFO"),
            signer=Account.from_key(self._config.private_key),
        )
        return self._order_builder

    async def _get_collateral_balance(self) -> float:
        collateral = self._config.collateral_token_address or _sdk_collateral_token(self._config.chain_id)
        account = self._get_web3_client().account
        if account is None:
            raise RuntimeError("PREDICT_FUN_PRIVATE_KEY is required for balance checks")
        token = self._get_web3_client().contract(collateral, ERC20_BALANCE_ABI)
        raw_balance = await token.functions.balanceOf(account.address).call()
        decimals = await self._get_collateral_decimals(token)
        return float(raw_balance) / float(10**decimals)

    async def _get_collateral_decimals(self, token: Any) -> int:
        if self._collateral_decimals is None:
            raw_decimals = await token.functions.decimals().call()
            self._collateral_decimals = int(raw_decimals)
        return self._collateral_decimals

    async def _request_json(self, method: str, path: str, json_body: dict[str, Any] | None = None) -> dict[str, Any]:
        if not self._config.api_base_url:
            raise RuntimeError("predict_fun.api_base_url is required")
        try:
            import aiohttp
        except ImportError as exc:
            raise RuntimeError("aiohttp is required for Predict.fun REST connectivity") from exc

        url = f"{self._config.api_base_url.rstrip('/')}/{path.lstrip('/')}"
        headers = {"Content-Type": "application/json"}
        if self._config.api_key:
            headers["x-api-key"] = self._config.api_key
            headers["X-API-Key"] = self._config.api_key
            headers["Authorization"] = f"Bearer {self._config.api_key}"
        async with aiohttp.ClientSession(headers=headers) as session:
            async with session.request(method, url, json=json_body, timeout=10) as response:
                response.raise_for_status()
                payload = await response.json()
        if not isinstance(payload, dict):
            raise RuntimeError(f"Predict.fun API returned unsupported payload: {payload!r}")
        return payload

    def _get_web3_client(self) -> BaseWeb3Client:
        if self._web3_client is None:
            self._web3_client = BaseWeb3Client(
                rpc_url=self._config.rpc_urls or self._config.rpc_url,
                chain_id=self._config.chain_id,
                private_key=self._config.private_key,
                max_priority_fee_gwei=self._config.max_priority_fee_gwei,
                confirmations=self._config.confirmations,
            )
        return self._web3_client

    def _get_market_abi(self) -> list[dict[str, Any]]:
        if self._market_abi is None:
            if not self._config.market_abi_path:
                raise RuntimeError("predict_fun.market_abi_path is required")
            self._market_abi = _load_abi(self._config.market_abi_path)
        return self._market_abi


def _load_abi(path: str) -> list[dict[str, Any]]:
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    if isinstance(raw, dict) and isinstance(raw.get("abi"), list):
        return list(raw["abi"])
    if isinstance(raw, list):
        return raw
    raise ValueError(f"ABI file has unsupported format: {path}")


def _to_wei(value: float) -> int:
    return int(value * 10**18)


def _to_precision_units(value: float, precision: int) -> int:
    return int(Decimal(str(value)) * (Decimal(10) ** precision))


def _sdk_chain_id(chain_id: int) -> Any:
    try:
        from predict_sdk.constants import ChainId
    except ImportError as exc:
        raise RuntimeError("predict-sdk is required for Predict.fun chain metadata") from exc
    if chain_id == 56:
        return ChainId.BNB_MAINNET
    if chain_id == 97:
        return ChainId.BNB_TESTNET
    raise ValueError("Predict.fun supports BNB mainnet chain_id=56 and BNB testnet chain_id=97")


def _sdk_side(side_name: str) -> Any:
    try:
        from predict_sdk.constants import Side
    except ImportError as exc:
        raise RuntimeError("predict-sdk is required for Predict.fun order sides") from exc
    return Side[side_name]


def _sdk_limit_helper_input(*, side: Any, price_per_share_wei: int, quantity_wei: int) -> Any:
    try:
        from predict_sdk.types import LimitHelperInput
    except ImportError as exc:
        raise RuntimeError("predict-sdk is required for Predict.fun order sizing") from exc
    return LimitHelperInput(side=side, price_per_share_wei=price_per_share_wei, quantity_wei=quantity_wei)


def _sdk_build_order_input(
    *, side: Any, token_id: str, maker_amount: str, taker_amount: str, fee_rate_bps: str
) -> Any:
    try:
        from predict_sdk.types import BuildOrderInput
    except ImportError as exc:
        raise RuntimeError("predict-sdk is required for Predict.fun order building") from exc
    return BuildOrderInput(
        side=side,
        token_id=token_id,
        maker_amount=maker_amount,
        taker_amount=taker_amount,
        fee_rate_bps=fee_rate_bps,
    )


def _sdk_collateral_token(chain_id: int) -> str:
    try:
        from predict_sdk.constants import ADDRESSES_BY_CHAIN_ID
    except ImportError as exc:
        raise RuntimeError("predict-sdk is required for Predict.fun contract addresses") from exc
    return str(ADDRESSES_BY_CHAIN_ID[_sdk_chain_id(chain_id)].USDT)


def _generate_order_salt() -> str:
    return str(secrets.randbits(256))


def _signed_order_to_payload(signed_order: Any) -> dict[str, Any]:
    raw = asdict(signed_order)
    return {
        "salt": str(raw["salt"]),
        "maker": raw["maker"],
        "signer": raw["signer"],
        "taker": raw["taker"],
        "tokenId": str(raw["token_id"]),
        "makerAmount": str(raw["maker_amount"]),
        "takerAmount": str(raw["taker_amount"]),
        "expiration": str(raw["expiration"]),
        "nonce": str(raw["nonce"]),
        "feeRateBps": str(raw["fee_rate_bps"]),
        "side": _required_int(raw, "side"),
        "signatureType": _required_int(raw, "signature_type"),
        "signature": raw["signature"],
        "hash": raw.get("hash"),
    }


def _required_int(payload: dict[str, Any], key: str) -> int:
    value = payload.get(key)
    if value is None:
        raise ValueError(f"Signed Predict.fun order is missing {key}")
    return int(getattr(value, "value", value))


def _parse_reserves(raw: Any) -> tuple[float, float]:
    if isinstance(raw, dict):
        yes = raw.get("yesReserve") or raw.get("yes_reserve") or raw.get("0")
        no = raw.get("noReserve") or raw.get("no_reserve") or raw.get("1")
    elif isinstance(raw, (list, tuple)) and len(raw) >= 2:
        yes, no = raw[0], raw[1]
    else:
        raise ValueError(f"Unsupported reserves response: {raw!r}")
    if yes is None or no is None:
        raise ValueError(f"Unsupported reserves response: {raw!r}")
    return float(yes) / 10**18, float(no) / 10**18


def _order_book_from_payload(payload: dict[str, Any]) -> OrderBook:
    book_payload = payload.get("orderbook") or payload.get("orderBook") or payload.get("book") or payload
    if not isinstance(book_payload, dict):
        book_payload = payload
    bids = [_level(item) for item in book_payload.get("bids", [])]
    asks = [_level(item) for item in book_payload.get("asks", [])]
    if not bids and not asks:
        synthetic = _synthetic_level_from_price_payload(payload)
        if synthetic is not None:
            bid, ask = synthetic
            bids = [bid]
            asks = [ask]
    return OrderBook(
        bids=sorted([level for level in bids if level is not None], key=lambda item: item.price, reverse=True),
        asks=sorted([level for level in asks if level is not None], key=lambda item: item.price),
    )


def _synthetic_level_from_price_payload(payload: dict[str, Any]) -> tuple[OrderBookLevel, OrderBookLevel] | None:
    price = _first_numeric(payload, ("ask", "bestAsk", "best_ask", "price", "lastPrice", "last_price", "probability"))
    bid_price = _first_numeric(payload, ("bid", "bestBid", "best_bid"))
    size = _first_numeric(payload, ("size", "liquidity", "available", "volume")) or 1.0
    if price is None:
        return None
    bid = bid_price if bid_price is not None else max(0.0, price - 0.001)
    return OrderBookLevel(price=bid, size=size), OrderBookLevel(price=price, size=size)


def _first_numeric(payload: dict[str, Any], keys: tuple[str, ...]) -> float | None:
    for key in keys:
        value = payload.get(key)
        if value in (None, ""):
            continue
        try:
            return float(str(value))
        except (TypeError, ValueError):
            continue
    nested = payload.get("data")
    if isinstance(nested, dict):
        return _first_numeric(nested, keys)
    return None


def _level(payload: Any) -> OrderBookLevel | None:
    if isinstance(payload, dict):
        price = payload.get("price")
        size = payload.get("size") or payload.get("quantity")
    elif isinstance(payload, (list, tuple)) and len(payload) >= 2:
        price, size = payload[0], payload[1]
    else:
        return None
    if price is None or size is None:
        return None
    return OrderBookLevel(float(price), float(size))


def _extract_first_nested(payload: Any, keys: tuple[str, ...]) -> Any:
    if isinstance(payload, dict):
        for key in keys:
            if key in payload and payload[key] not in (None, ""):
                return payload[key]
        for nested_key in ("data", "order", "result"):
            nested = payload.get(nested_key)
            found = _extract_first_nested(nested, keys)
            if found not in (None, ""):
                return found
    return None


def _extract_filled_amount(payload: Any) -> float | None:
    value = _extract_first_nested(
        payload,
        ("filledAmount", "filled_amount", "amountFilled", "executedAmount", "matchedAmount", "sizeMatched"),
    )
    if value in (None, ""):
        return None
    try:
        return float(str(value))
    except (TypeError, ValueError):
        return None


def _normalize_order_amount(value: float, requested: float, precision: int) -> float:
    if requested > 0 and value > requested * 1_000:
        return value / float(10**precision)
    return value
