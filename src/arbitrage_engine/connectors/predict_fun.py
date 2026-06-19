from __future__ import annotations

import asyncio
from dataclasses import asdict, replace
from decimal import Decimal
import json
import logging
from pathlib import Path
import secrets
import time
from typing import Any, Callable

from arbitrage_engine.config import PredictFunConfig
from arbitrage_engine.connectors.base import (
    OrderBookStaleException,
    OrderBookUnavailableException,
    PredictFunClient,
    event_timestamp,
)
from arbitrage_engine.connectors.web3_base import BaseWeb3Client
from arbitrage_engine.http import client_session
from arbitrage_engine.models import BinarySide, ExecutionReport, OrderBook, OrderBookLevel

LOGGER = logging.getLogger(__name__)
ORDER_BOOK_MAX_AGE_SECONDS = 1.0
MULTICALL3_ADDRESS = "0xcA11bde05977b3631167028862bE2a173976CA11"
MULTICALL3_ABI: list[dict[str, Any]] = [
    {
        "type": "function",
        "name": "aggregate3",
        "stateMutability": "payable",
        "inputs": [
            {
                "name": "calls",
                "type": "tuple[]",
                "components": [
                    {"name": "target", "type": "address"},
                    {"name": "allowFailure", "type": "bool"},
                    {"name": "callData", "type": "bytes"},
                ],
            }
        ],
        "outputs": [
            {
                "name": "returnData",
                "type": "tuple[]",
                "components": [
                    {"name": "success", "type": "bool"},
                    {"name": "returnData", "type": "bytes"},
                ],
            }
        ],
    }
]
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
        self._rest_session: Any | None = None
        self._http_semaphore = asyncio.Semaphore(20)
        self._order_amounts: dict[str, float] = {}
        self._order_prices: dict[str, float] = {}
        self._order_cancel_ids: dict[str, str] = {}
        self._books: dict[str, OrderBook] = {}
        self._book_timestamps: dict[str, float] = {}
        self._book_events: dict[str, asyncio.Event] = {}
        self._tracked_tokens: set[str] = set()
        self._market_identifiers: dict[str, tuple[str, BinarySide]] = {}
        self._rpc_markets: dict[str, tuple[str, BinarySide]] = {}
        self._token_fee_rate_bps: dict[str, int] = {}
        self._multicall_task: asyncio.Task[None] | None = None
        self._rest_books_task: asyncio.Task[None] | None = None

    def register_market(
        self,
        token_id: str,
        market_id: str | None,
        side: BinarySide,
        fee_rate_bps: int | None = None,
    ) -> None:
        if not token_id or not market_id:
            return
        self._market_identifiers[token_id] = (market_id, side)
        self._token_fee_rate_bps[token_id] = self._config.fee_rate_bps if fee_rate_bps is None else fee_rate_bps
        if _is_evm_address(market_id):
            self._rpc_markets[token_id] = (market_id, side)

    async def watch_order_book(self, token_id: str) -> OrderBook:
        self._tracked_tokens.add(token_id)
        self._ensure_multicall_task()
        self._ensure_rest_books_task()
        event = self._book_events.setdefault(token_id, asyncio.Event())
        cached = self._books.get(token_id)
        if cached is not None and time.monotonic() - self._book_timestamps.get(token_id, 0.0) <= ORDER_BOOK_MAX_AGE_SECONDS:
            return cached
        if self._config.api_base_url and token_id in self._market_identifiers:
            event.clear()
            try:
                await asyncio.wait_for(event.wait(), timeout=1.5)
                cached = self._books.get(token_id)
                if cached is not None:
                    return cached
            except asyncio.TimeoutError:
                pass
        try:
            if self._config.api_base_url:
                try:
                    book = await self._watch_order_book_rest(token_id)
                except Exception:
                    if self._config.market_abi_path:
                        LOGGER.exception("predict_fun_rest_orderbook_failed_using_rpc", extra={"_token_id": token_id})
                        book = await self._watch_order_book_rpc(token_id)
                    else:
                        raise
            else:
                book = await self._watch_order_book_rpc(token_id)
        except Exception as exc:
            if cached is not None:
                raise OrderBookStaleException(f"Predict.fun order book is stale for token {token_id}") from exc
            raise
        self._store_book(token_id, book)
        return book

    def _ensure_multicall_task(self) -> None:
        if not self._config.market_abi_path:
            return
        if self._multicall_task is None or self._multicall_task.done():
            self._multicall_task = asyncio.create_task(self._run_multicall_loop())

    def _ensure_rest_books_task(self) -> None:
        if not self._config.api_base_url:
            return
        if self._rest_books_task is None or self._rest_books_task.done():
            self._rest_books_task = asyncio.create_task(self._run_rest_books_loop())

    async def _run_rest_books_loop(self) -> None:
        while True:
            await asyncio.sleep(0.5)
            try:
                await self._refresh_rest_books_batch()
            except asyncio.CancelledError:
                raise
            except Exception:
                LOGGER.exception("predict_fun_batch_orderbooks_failed")

    async def _refresh_rest_books_batch(self) -> None:
        if not self._config.api_base_url:
            return
        by_market: dict[str, list[tuple[str, BinarySide]]] = {}
        for token_id in self._tracked_tokens:
            identity = self._market_identifiers.get(token_id)
            if identity is not None:
                market_id, side = identity
                by_market.setdefault(market_id, []).append((token_id, side))
        market_ids = list(by_market)
        if not market_ids:
            return
        headers = {"Content-Type": "application/json"}
        if self._config.api_key:
            headers["x-api-key"] = self._config.api_key
            headers["X-API-Key"] = self._config.api_key
            headers["Authorization"] = f"Bearer {self._config.api_key}"
        session = self._get_rest_session(headers)
        url = f"{self._config.api_base_url.rstrip('/')}/v1/markets/orderbooks"
        for start in range(0, len(market_ids), 100):
            chunk = market_ids[start : start + 100]
            params = [("ids", market_id) for market_id in chunk]
            async with self._http_semaphore:
                async with session.get(url, params=params, timeout=10) as response:
                    response.raise_for_status()
                    payload = await response.json()
            data = payload.get("data") if isinstance(payload, dict) else None
            if not isinstance(data, list):
                continue
            returned_market_ids: set[str] = set()
            for item in data:
                if not isinstance(item, dict):
                    continue
                market_id = str(item.get("marketId") or "")
                if market_id not in by_market:
                    continue
                returned_market_ids.add(market_id)
                yes_book = _order_book_from_payload({"data": item})
                for token_id, side in by_market[market_id]:
                    self._store_book(
                        token_id,
                        yes_book if side is BinarySide.YES else _invert_binary_order_book(yes_book),
                    )
            for market_id in set(chunk) - returned_market_ids:
                for token_id, side in by_market[market_id]:
                    empty = OrderBook(
                        bids=[],
                        asks=[],
                        raw_payload={"marketId": market_id, "reason": "omitted_from_batch_orderbooks"},
                    )
                    self._store_book(
                        token_id,
                        empty if side is BinarySide.YES else _invert_binary_order_book(empty),
                    )

    async def _run_multicall_loop(self) -> None:
        while True:
            await asyncio.sleep(3.0)
            try:
                await self._refresh_books_multicall()
            except asyncio.CancelledError:
                raise
            except Exception:
                LOGGER.exception("predict_fun_multicall_refresh_failed")

    async def _refresh_books_multicall(self) -> None:
        token_ids = sorted(self._tracked_tokens)
        if not token_ids:
            return
        web3_client = self._get_web3_client()
        market_abi = self._get_market_abi()
        calls: list[tuple[str, bool, bytes]] = []
        registered: list[tuple[str, BinarySide]] = []
        for token_id in token_ids:
            market_identity = self._rpc_markets.get(token_id)
            if market_identity is None:
                continue
            amm_address, side = market_identity
            market = web3_client.contract(amm_address, market_abi)
            function = getattr(market.functions, self._config.reserves_function)()
            calls.append((market.address, True, bytes.fromhex(function._encode_transaction_data()[2:])))
            registered.append((token_id, side))
        if not calls:
            return
        multicall = web3_client.contract(MULTICALL3_ADDRESS, MULTICALL3_ABI)
        results = await multicall.functions.aggregate3(calls).call()
        output_types = _function_output_types(market_abi, self._config.reserves_function)
        for (token_id, side), result in zip(registered, results):
            success, return_data = result
            if not success:
                continue
            reserves = web3_client.w3.codec.decode(output_types, bytes(return_data))
            self._store_book(
                token_id,
                _order_book_from_reserves(
                    reserves,
                    side,
                    float(Decimal(self._config.fee_rate_bps) / Decimal(10_000)),
                ),
            )

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
        last_avg_price = self._order_prices.get(order_id, 0.0)
        while asyncio.get_running_loop().time() < deadline:
            payload = await self._request_json("GET", f"/v1/orders/{order_id}")
            status = str(_extract_first_nested(payload, ("status", "state", "orderStatus", "order_status")) or "").lower()
            last_status = status or last_status
            parsed_filled = _extract_filled_amount(payload)
            if parsed_filled is not None:
                parsed_filled = _normalize_order_amount(parsed_filled, requested, self._config.precision)
                last_filled = max(last_filled, parsed_filled)
            parsed_avg_price = _extract_avg_price(payload)
            if parsed_avg_price is not None:
                last_avg_price = _normalize_price(parsed_avg_price, self._config.precision)
            if status in {"filled", "matched", "executed", "complete", "completed"}:
                return ExecutionReport.from_amounts(
                    order_id, requested, parsed_filled or requested, status, last_avg_price
                )
            if status in {"cancelled", "canceled", "expired", "rejected", "failed"}:
                return ExecutionReport.from_amounts(order_id, requested, last_filled, status, last_avg_price)
            await asyncio.sleep(0.25)
        return ExecutionReport.from_amounts(order_id, requested, last_filled, last_status, last_avg_price)

    async def cancel_order(self, order_id: str) -> None:
        if not self._config.api_base_url:
            raise RuntimeError("predict_fun.api_base_url is required to cancel Predict.fun orders")
        cancel_id = self._order_cancel_ids.get(order_id, order_id)
        await self._request_json("POST", "/v1/orders/remove", json_body={"data": {"ids": [cancel_id]}})

    async def get_cash_balance(self) -> float:
        return await self._get_collateral_balance()

    async def _watch_order_book_rest(self, token_id: str) -> OrderBook:
        if not self._config.api_base_url:
            raise RuntimeError("predict_fun.api_base_url is required for REST orderbook access")
        market_identity = self._market_identifiers.get(token_id)
        if market_identity is None:
            raise RuntimeError(f"Predict.fun market id and side are not registered for token {token_id}")
        market_id, side = market_identity
        payload = await self._request_json("GET", f"/v1/markets/{market_id}/orderbook")
        book = _order_book_from_payload(payload)
        if book.bids and book.asks:
            return book if side is BinarySide.YES else _invert_binary_order_book(book)
        raise OrderBookUnavailableException(
            f"Predict.fun REST API did not return a two-sided order book for token {token_id}"
        )

    async def _watch_order_book_rpc(self, token_id: str) -> OrderBook:
        if not self._config.market_abi_path:
            raise RuntimeError("predict_fun.market_abi_path is required for direct RPC price reads")
        market_identity = self._rpc_markets.get(token_id)
        if market_identity is None:
            raise RuntimeError(f"Predict.fun AMM address and side are not registered for token {token_id}")
        amm_address, side = market_identity
        contract = self._get_web3_client().contract(amm_address, self._get_market_abi())
        reserves = await getattr(contract.functions, self._config.reserves_function)().call()
        return _order_book_from_reserves(
            reserves,
            side,
            float(Decimal(self._config.fee_rate_bps) / Decimal(10_000)),
        )

    def _store_book(self, token_id: str, book: OrderBook) -> None:
        self._books[token_id] = replace(book, timestamp=min(book.timestamp, time.time()))
        self._book_timestamps[token_id] = time.monotonic()
        self._book_events.setdefault(token_id, asyncio.Event()).set()

    async def _submit_sdk_order(
        self, token_id: str, side: BinarySide, contracts: float, limit_price: float, *, sdk_side_name: str, neg_risk: bool
    ) -> str:
        if not self._config.private_key:
            raise RuntimeError("PREDICT_FUN_PRIVATE_KEY is required for Predict.fun production orders")
        if not self._config.api_base_url:
            raise RuntimeError("predict_fun.api_base_url is required for Predict.fun order submission")
        contract_order = self._build_signed_order_payload(
            token_id=token_id,
            contracts=contracts,
            limit_price=limit_price,
            sdk_side_name=sdk_side_name,
            neg_risk=neg_risk,
            fee_rate_bps=self._token_fee_rate_bps.get(token_id, self._config.fee_rate_bps),
        )
        del side
        payload = {
            "data": {
                "pricePerShare": str(_to_precision_units(limit_price, self._config.precision)),
                "strategy": "MARKET",
                "slippageBps": str(
                    int(min(Decimal(str(self._config.max_slippage_pct)), Decimal("0.015")) * Decimal(10_000))
                ),
                "isFillOrKill": True,
                "isPostOnly": False,
                "reservedBalancePolicy": "REJECT_MARKET_ORDER",
                "order": contract_order,
            }
        }
        response = await self._request_json("POST", "/v1/orders", json_body=payload)
        if response.get("success") is False:
            raise RuntimeError(f"Predict.fun rejected order creation: {response!r}")
        order_hash = _extract_first_nested(response, ("orderHash", "order_hash", "hash"))
        cancel_id = _extract_first_nested(response, ("orderId", "order_id", "id"))
        if not order_hash:
            raise RuntimeError(f"Predict.fun order response does not include an order id: {response!r}")
        normalized_order_id = str(order_hash)
        self._order_amounts[normalized_order_id] = contracts
        self._order_prices[normalized_order_id] = limit_price
        self._order_cancel_ids[normalized_order_id] = str(cancel_id or order_hash)
        return normalized_order_id

    def forget_order(self, order_id: str) -> None:
        self._order_amounts.pop(order_id, None)
        self._order_prices.pop(order_id, None)
        self._order_cancel_ids.pop(order_id, None)

    def _build_signed_order_payload(
        self,
        *,
        token_id: str,
        contracts: float,
        limit_price: float,
        sdk_side_name: str,
        neg_risk: bool,
        fee_rate_bps: int | None = None,
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
                fee_rate_bps=str(self._config.fee_rate_bps if fee_rate_bps is None else fee_rate_bps),
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
        session = self._get_rest_session(headers)
        async with self._http_semaphore:
            async with session.request(method, url, json=json_body, timeout=10) as response:
                response.raise_for_status()
                payload = await response.json()
        if not isinstance(payload, dict):
            raise RuntimeError(f"Predict.fun API returned unsupported payload: {payload!r}")
        return payload

    def _get_rest_session(self, headers: dict[str, str]) -> Any:
        if self._rest_session is None or self._rest_session.closed:
            self._rest_session = client_session(headers)
        return self._rest_session

    async def close(self) -> None:
        if self._rest_books_task is not None:
            self._rest_books_task.cancel()
            await asyncio.gather(self._rest_books_task, return_exceptions=True)
            self._rest_books_task = None
        if self._multicall_task is not None:
            self._multicall_task.cancel()
            await asyncio.gather(self._multicall_task, return_exceptions=True)
            self._multicall_task = None
        if self._rest_session is not None and not self._rest_session.closed:
            await self._rest_session.close()
        self._rest_session = None

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


def _is_evm_address(value: str) -> bool:
    raw = value[2:] if value.startswith("0x") else value
    return len(raw) == 40 and all(char in "0123456789abcdefABCDEF" for char in raw)


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
    payload = {
        "salt": str(raw["salt"]),
        "maker": raw["maker"],
        "signer": raw["signer"],
        "taker": raw["taker"],
        "tokenId": str(raw["token_id"]),
        "makerAmount": str(raw["maker_amount"]),
        "takerAmount": str(raw["taker_amount"]),
        "expiration": int(raw["expiration"]),
        "nonce": str(raw["nonce"]),
        "feeRateBps": str(raw["fee_rate_bps"]),
        "side": _required_int(raw, "side"),
        "signatureType": _required_int(raw, "signature_type"),
        "signature": raw["signature"],
    }
    if raw.get("hash"):
        payload["hash"] = raw["hash"]
    return payload


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


def _order_book_from_reserves(raw: Any, side: BinarySide, fee_pct: float = 0.0) -> OrderBook:
    yes_reserve, no_reserve = _parse_reserves(raw)
    total = yes_reserve + no_reserve
    if total <= 0:
        raise ValueError("Predict.fun pool reserves must be positive")
    yes_price = no_reserve / total
    no_price = yes_reserve / total
    target_price = yes_price if side is BinarySide.YES else no_price
    synthetic_size = min(yes_reserve, no_reserve)
    return OrderBook(
        bids=[OrderBookLevel(price=max(0.0, target_price - 0.001), size=synthetic_size)],
        asks=[OrderBookLevel(price=target_price, size=synthetic_size)],
        raw_payload={
            "reserves": raw,
            "side": side.value,
            "amm_pool": {
                "yes_reserve": yes_reserve,
                "no_reserve": no_reserve,
                "fee_pct": fee_pct,
            },
        },
    )


def _function_output_types(abi: list[dict[str, Any]], function_name: str) -> list[str]:
    for item in abi:
        if item.get("type") == "function" and item.get("name") == function_name:
            outputs = item.get("outputs")
            if isinstance(outputs, list):
                return [str(output["type"]) for output in outputs if isinstance(output, dict) and "type" in output]
    raise ValueError(f"ABI does not define outputs for {function_name}")


def _order_book_from_payload(payload: dict[str, Any]) -> OrderBook:
    book_payload = (
        payload.get("orderbook")
        or payload.get("orderBook")
        or payload.get("book")
        or payload.get("data")
        or payload
    )
    if not isinstance(book_payload, dict):
        book_payload = payload
    bids = [_level(item) for item in book_payload.get("bids", [])]
    asks = [_level(item) for item in book_payload.get("asks", [])]
    return OrderBook(
        bids=sorted([level for level in bids if level is not None], key=lambda item: item.price, reverse=True),
        asks=sorted([level for level in asks if level is not None], key=lambda item: item.price),
        raw_payload=payload,
        timestamp=event_timestamp(payload),
    )


def _invert_binary_order_book(book: OrderBook) -> OrderBook:
    bids = [OrderBookLevel(price=max(0.0, 1.0 - level.price), size=level.size) for level in book.asks]
    asks = [OrderBookLevel(price=max(0.0, 1.0 - level.price), size=level.size) for level in book.bids]
    return OrderBook(
        bids=sorted(bids, key=lambda level: level.price, reverse=True),
        asks=sorted(asks, key=lambda level: level.price),
        raw_payload={"source": book.raw_payload, "inverted_from": BinarySide.YES.value},
        timestamp=book.timestamp,
    )


def _level(payload: Any) -> OrderBookLevel | None:
    if isinstance(payload, dict):
        price = payload.get("price")
        size = payload.get("size")
        if size is None:
            size = payload.get("quantity")
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


def _extract_avg_price(payload: Any) -> float | None:
    value = _extract_first_nested(payload, ("avgPrice", "averagePrice", "avg_price", "average_price"))
    if value in (None, ""):
        return None
    try:
        return float(str(value))
    except (TypeError, ValueError):
        return None


def _normalize_price(value: float, precision: int) -> float:
    return value / float(10**precision) if value > 1.0 else value
