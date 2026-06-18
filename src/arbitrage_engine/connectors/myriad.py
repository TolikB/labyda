from __future__ import annotations

import time
import asyncio
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, cast

from arbitrage_engine.config import MyriadMarketsConfig
from arbitrage_engine.connectors.base import PredictFunClient
from arbitrage_engine.connectors.web3_base import BaseWeb3Client
from arbitrage_engine.models import BinarySide, OrderBook, OrderBookLevel

SHARE_DECIMALS = 18
PRICE_DECIMALS = 18
COLLATERAL_DECIMALS = 6
ERC20_BALANCE_ABI: list[dict[str, Any]] = [
    {
        "constant": True,
        "inputs": [{"name": "account", "type": "address"}],
        "name": "balanceOf",
        "outputs": [{"name": "", "type": "uint256"}],
        "type": "function",
    }
]


@dataclass(frozen=True)
class MyriadSignedOrder:
    order: dict[str, Any]
    signature: str


class MyriadClient(PredictFunClient):
    def __init__(self, config: MyriadMarketsConfig) -> None:
        self._config = config
        self._nonce = int(time.time() * 1000)
        self._nonce_lock = asyncio.Lock()
        self._web3_client: BaseWeb3Client | None = None

    async def watch_order_book(self, token_id: str) -> OrderBook:
        market_id, side = _parse_token_id(token_id)
        raw = await self.get_orderbook(market_id)
        return _order_book_from_payload(raw, side)

    async def get_orderbook(self, market_id: int) -> dict[str, Any]:
        try:
            import aiohttp
        except ImportError as exc:
            raise RuntimeError("aiohttp is required for Myriad connectivity") from exc

        headers = self._headers()
        url = f"{self._config.api_url.rstrip('/')}/markets/{market_id}/orderbook"
        async with aiohttp.ClientSession(headers=headers) as session:
            async with session.get(url, timeout=10) as response:
                response.raise_for_status()
                payload = await response.json()
        if not isinstance(payload, dict):
            raise RuntimeError(f"Myriad orderbook payload has unsupported format: {payload!r}")
        return payload

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
        del condition_id, tick_size, neg_risk
        market_id, _ = _parse_token_id(token_id)
        signed = await self.sign_order(market_id, _outcome_id(side), 0, contracts, max_price)
        return await self.place_order(signed)

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
        del condition_id, tick_size, neg_risk
        market_id, _ = _parse_token_id(token_id)
        signed = await self.sign_order(market_id, _outcome_id(side), 1, contracts, min_price)
        return await self.place_order(signed)

    async def wait_filled(self, order_id: str, timeout_ms: int) -> bool:
        try:
            import aiohttp
        except ImportError as exc:
            raise RuntimeError("aiohttp is required for Myriad connectivity") from exc

        effective_timeout_ms = min(timeout_ms, 200)
        deadline = time.monotonic() + effective_timeout_ms / 1000
        url = f"{self._config.api_url.rstrip('/')}/orders/{order_id}"
        async with aiohttp.ClientSession(headers=self._headers()) as session:
            while time.monotonic() < deadline:
                async with session.get(url, timeout=5) as response:
                    response.raise_for_status()
                    payload = await response.json()
                status = str(_extract_first_nested(payload, ("status", "state", "orderStatus")) or "").lower()
                if status in {"filled", "matched", "executed", "complete", "completed"}:
                    return True
                if status in {"cancelled", "canceled", "expired", "rejected", "failed"}:
                    return False
                await asyncio.sleep(0.2)
        return False

    async def cancel_order(self, order_id: str) -> None:
        try:
            import aiohttp
        except ImportError as exc:
            raise RuntimeError("aiohttp is required for Myriad connectivity") from exc

        url = f"{self._config.api_url.rstrip('/')}/orders/{order_id}/cancel"
        async with aiohttp.ClientSession(headers=self._headers()) as session:
            async with session.post(url, timeout=10) as response:
                response.raise_for_status()

    async def get_cash_balance(self) -> float:
        token_address = self._config.collateral_tokens.get(self._config.collateral_symbol)
        if not token_address:
            raise RuntimeError(f"Myriad collateral token is not configured: {self._config.collateral_symbol}")
        web3_client = self._get_web3_client()
        account = web3_client.account
        if account is None:
            raise RuntimeError("MYRIAD_PRIVATE_KEY is required for Myriad balance checks")
        token = web3_client.contract(token_address, ERC20_BALANCE_ABI)
        raw_balance = cast(int | str, await token.functions.balanceOf(account.address).call())
        balance: float = float(int(raw_balance)) / float(10**COLLATERAL_DECIMALS)
        return balance

    async def place_order(self, signed_order: MyriadSignedOrder) -> str:
        try:
            import aiohttp
        except ImportError as exc:
            raise RuntimeError("aiohttp is required for Myriad connectivity") from exc

        payload = {
            "order": signed_order.order,
            "signature": signed_order.signature,
            "network_id": self._config.chain_id,
            "time_in_force": "IOC",
        }
        url = f"{self._config.api_url.rstrip('/')}/orders"
        async with aiohttp.ClientSession(headers=self._headers()) as session:
            async with session.post(url, json=payload, timeout=10) as response:
                response.raise_for_status()
                raw = await response.json()
        order_id = _extract_first_nested(raw, ("order_id", "orderId", "id", "hash"))
        if not order_id:
            raise RuntimeError(f"Myriad order response does not include an order id: {raw!r}")
        return str(order_id)

    async def sign_order(self, market_id: int, outcome_id: int, side: int, contracts: float, price: float) -> MyriadSignedOrder:
        if not self._config.private_key:
            raise RuntimeError("MYRIAD_PRIVATE_KEY is required for Myriad order signing")
        try:
            from eth_account import Account
            from eth_account.messages import encode_typed_data
        except ImportError as exc:
            raise RuntimeError("eth-account is required for Myriad order signing") from exc

        account = Account.from_key(self._config.private_key)
        eip712_order = {
            "trader": account.address,
            "marketId": market_id,
            "outcomeId": outcome_id,
            "side": side,
            "amount": _to_units(contracts, SHARE_DECIMALS),
            "price": _to_units(price, PRICE_DECIMALS),
            "minFillAmount": 0,
            "nonce": await self._next_nonce(),
            "expiration": 0,
        }
        typed_data = {
            "types": {
                "EIP712Domain": [
                    {"name": "name", "type": "string"},
                    {"name": "version", "type": "string"},
                    {"name": "chainId", "type": "uint256"},
                    {"name": "verifyingContract", "type": "address"},
                ],
                "Order": [
                    {"name": "trader", "type": "address"},
                    {"name": "marketId", "type": "uint256"},
                    {"name": "outcomeId", "type": "uint8"},
                    {"name": "side", "type": "uint8"},
                    {"name": "amount", "type": "uint256"},
                    {"name": "price", "type": "uint256"},
                    {"name": "minFillAmount", "type": "uint256"},
                    {"name": "nonce", "type": "uint256"},
                    {"name": "expiration", "type": "uint256"},
                ],
            },
            "primaryType": "Order",
            "domain": {
                "name": "MyriadCTFExchange",
                "version": "1",
                "chainId": self._config.chain_id,
                "verifyingContract": self._config.exchange_address,
            },
            "message": eip712_order,
        }
        signable = encode_typed_data(full_message=typed_data)
        signed = account.sign_message(signable)
        signature = str(signed.signature.hex())
        if not signature.startswith("0x"):
            signature = f"0x{signature}"
        return MyriadSignedOrder(order=_api_order_payload(eip712_order), signature=signature)

    def _headers(self) -> dict[str, str]:
        headers = {"Content-Type": "application/json"}
        if self._config.api_key:
            headers["x-api-key"] = self._config.api_key
        return headers

    def _get_web3_client(self) -> BaseWeb3Client:
        if self._web3_client is None:
            self._web3_client = BaseWeb3Client(
                rpc_url=self._config.rpc_url,
                chain_id=self._config.chain_id,
                private_key=self._config.private_key,
            )
        return self._web3_client

    async def _next_nonce(self) -> int:
        async with self._nonce_lock:
            self._nonce += 1
            return self._nonce


def _order_book_from_payload(payload: dict[str, Any], side: BinarySide | None = None) -> OrderBook:
    book = payload.get("orderbook") or payload.get("orderBook") or payload
    if side is not None and isinstance(book, dict):
        side_book = (
            book.get(side.value)
            or book.get(side.value.lower())
            or book.get(f"{side.value.lower()}Orderbook")
            or book.get(f"{side.value.lower()}_orderbook")
        )
        if isinstance(side_book, dict):
            book = side_book
    if not isinstance(book, dict):
        book = payload
    bids = [_level(item) for item in book.get("bids", [])]
    asks = [_level(item) for item in book.get("asks", [])]
    return OrderBook(
        bids=sorted([level for level in bids if level is not None], key=lambda item: item.price, reverse=True),
        asks=sorted([level for level in asks if level is not None], key=lambda item: item.price),
    )


def _level(payload: Any) -> OrderBookLevel | None:
    if isinstance(payload, dict):
        price = payload.get("price")
        size = payload.get("size") or payload.get("quantity") or payload.get("amount")
    elif isinstance(payload, (list, tuple)) and len(payload) >= 2:
        price, size = payload[0], payload[1]
    else:
        return None
    if price is None or size is None:
        return None
    return OrderBookLevel(float(price), float(size))


def _outcome_id(side: BinarySide) -> int:
    return 0 if side is BinarySide.YES else 1


def _parse_token_id(token_id: str) -> tuple[int, BinarySide | None]:
    if ":" not in token_id:
        return int(token_id), None
    market_id, raw_side = token_id.split(":", 1)
    return int(market_id), BinarySide(raw_side)


def _to_units(value: float, decimals: int) -> int:
    scale = Decimal(10) ** decimals
    return int(Decimal(str(value)) * scale)


def _api_order_payload(order: dict[str, Any]) -> dict[str, Any]:
    uint_fields = {"marketId", "amount", "price", "minFillAmount", "nonce", "expiration"}
    return {key: str(value) if key in uint_fields else value for key, value in order.items()}


def _extract_first_nested(payload: Any, keys: tuple[str, ...]) -> Any:
    if isinstance(payload, dict):
        for key in keys:
            if key in payload and payload[key] not in (None, ""):
                return payload[key]
        for nested_key in ("data", "order", "result"):
            found = _extract_first_nested(payload.get(nested_key), keys)
            if found not in (None, ""):
                return found
    return None
