from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import secrets
import time
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import asdict, dataclass, replace
from datetime import UTC, datetime
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal
from pathlib import Path
from typing import Any

from arbitrage_engine.config import PredictFunConfig
from arbitrage_engine.connectors.base import (
    OrderBookStaleException,
    OrderBookUnavailableException,
    PredictFunClient,
    WebSocketReconnectBackoff,
    event_timestamp,
)
from arbitrage_engine.connectors.web3_base import BaseWeb3Client
from arbitrage_engine.http import client_session
from arbitrage_engine.models import (
    BinarySide,
    ExecutionReport,
    FillRecord,
    MarketConstraints,
    MarketDataStatus,
    OrderBook,
    OrderBookLevel,
    OrderIntentStatus,
    OrderPreview,
    VenueFeeQuote,
    VenueOrder,
)

LOGGER = logging.getLogger(__name__)
ORDER_BOOK_MAX_AGE_SECONDS = 1.0
_WS_HEARTBEAT_SECONDS = 5.0
_APPLICATION_HEARTBEAT_MAX_AGE_SECONDS = 30.0
_WS_SUBSCRIPTION_ACK_TIMEOUT_SECONDS = 10.0
_WS_SUBSCRIPTION_WATCHDOG_INTERVAL_SECONDS = 1.0
_MIN_PLAUSIBLE_EPOCH_MS = 946_684_800_000
_MAX_FUTURE_CLOCK_SKEW_MS = 300_000
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


@dataclass(frozen=True)
class _SignedPredictMarketOrder:
    signed_order: dict[str, Any]
    amount_wei: int
    price_per_share_wei: int
    slippage_bps: int
    is_min_amount_out: bool


class PredictFunApiClient(PredictFunClient):
    venue_name = "Predict.fun"

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
        self._last_market_data_at: float | None = None
        self._book_events: dict[str, asyncio.Event] = {}
        self._tracked_tokens: set[str] = set()
        self._market_identifiers: dict[str, tuple[str, BinarySide]] = {}
        self._rpc_markets: dict[str, tuple[str, BinarySide]] = {}
        self._token_fee_rate_bps: dict[str, int] = {}
        self._token_price_precision: dict[str, int] = {}
        self._multicall_task: asyncio.Task[None] | None = None
        self._rest_books_task: asyncio.Task[None] | None = None
        self._ws_task: asyncio.Task[None] | None = None
        self._ws_session: Any | None = None
        self._ws: Any | None = None
        self._ws_subscription_queue: asyncio.Queue[tuple[str, str]] = asyncio.Queue()
        self._ws_subscribed_topics: set[str] = set()
        self._ws_pending_requests: dict[int, tuple[str, str]] = {}
        self._ws_pending_request_started_at: dict[int, float] = {}
        self._ws_session_orderbook_markets: set[str] = set()
        self._ws_session_status_markets: set[str] = set()
        self._last_application_heartbeat_at: float | None = None
        self._ws_connected = False
        self._reconnect_backoff = WebSocketReconnectBackoff()
        self._reconnect_count = 0
        self._sequence_gap_count = 0
        self._market_update_timestamps_ms: dict[str, int] = {}
        self._market_update_fingerprints: dict[str, str] = {}
        self._trading_status: dict[str, str] = {}
        self._trading_status_timestamps_ms: dict[str, int] = {}
        self._jwt_token: str | None = None
        self._jwt_lock = asyncio.Lock()
        self._rest_refresh_lock = asyncio.Lock()

    def register_market(
        self,
        token_id: str,
        market_id: str | None,
        side: BinarySide,
        fee_rate_bps: int | None = None,
        price_precision: int | None = None,
    ) -> None:
        if not token_id or not market_id:
            return
        self._market_identifiers[token_id] = (market_id, side)
        if fee_rate_bps is None:
            self._token_fee_rate_bps.pop(token_id, None)
        else:
            self._token_fee_rate_bps[token_id] = fee_rate_bps
        if price_precision is None or not 0 <= price_precision <= 18:
            self._token_price_precision.pop(token_id, None)
        else:
            self._token_price_precision[token_id] = price_precision
        if _is_evm_address(market_id):
            self._rpc_markets[token_id] = (market_id, side)
        if self._ws_connected and token_id in self._tracked_tokens:
            self._queue_market_topics("subscribe", market_id)

    def _required_fee_rate_bps(self, token_id: str) -> int:
        fee_rate_bps = self._token_fee_rate_bps.get(token_id)
        if fee_rate_bps is None:
            raise RuntimeError(f"Predict.fun fee metadata is unavailable for token {token_id}")
        return fee_rate_bps

    def _required_price_precision(self, token_id: str) -> int:
        price_precision = self._token_price_precision.get(token_id)
        if price_precision is None:
            raise RuntimeError(f"Predict.fun price precision metadata is unavailable for token {token_id}")
        return price_precision

    async def watch_order_book(self, token_id: str) -> OrderBook:
        self._tracked_tokens.add(token_id)
        self._ensure_ws_task()
        self._ensure_multicall_task()
        self._ensure_rest_books_task()
        event = self._book_events.setdefault(token_id, asyncio.Event())
        cached = self._books.get(token_id)
        market_identity = self._market_identifiers.get(token_id)
        if market_identity is not None:
            status = self._trading_status.get(market_identity[0])
            if status is not None and status != "OPEN":
                raise OrderBookUnavailableException(
                    f"Predict.fun market {market_identity[0]} trading status is {status}"
                )
        if (
            cached is not None
            and self._cached_book_is_passively_fresh(token_id, cached)
        ):
            return self._execution_book_from_cache(token_id, cached)
        if self._config.api_base_url and token_id in self._market_identifiers:
            event.clear()
            try:
                await asyncio.wait_for(event.wait(), timeout=1.5)
                cached = self._books.get(token_id)
                if cached is not None and self._cached_book_is_passively_fresh(token_id, cached):
                    return self._execution_book_from_cache(token_id, cached)
            except TimeoutError:
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
        self._store_book(token_id, book, confirmed_at_receipt=True)
        return book

    def _ensure_multicall_task(self) -> None:
        if not self._config.market_abi_path:
            return
        if self._multicall_task is None or self._multicall_task.done():
            self._multicall_task = asyncio.create_task(self._run_multicall_loop())

    def _ensure_rest_books_task(self) -> None:
        if self._config.ws_url and self._config.api_key:
            return
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
        async with self._rest_refresh_lock:
            by_market: dict[str, list[tuple[str, BinarySide]]] = {}
            for token_id in self._tracked_tokens:
                identity = self._market_identifiers.get(token_id)
                if identity is not None:
                    market_id, side = identity
                    by_market.setdefault(market_id, []).append((token_id, side))
            market_ids = list(by_market)
            if not market_ids:
                return
            for start in range(0, len(market_ids), 100):
                chunk = market_ids[start : start + 100]
                params = [("ids", market_id) for market_id in chunk]
                payload = await self._request_json(
                    "GET",
                    "/v1/markets/orderbooks",
                    query_params=params,
                )
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
                    try:
                        yes_book = _order_book_from_payload({"data": item})
                    except (TypeError, ValueError) as exc:
                        LOGGER.warning(
                            "predict_fun_batch_orderbook_invalid",
                            extra={"_market_id": market_id, "_error_type": type(exc).__name__},
                        )
                        continue
                    returned_market_ids.add(market_id)
                    for token_id, side in by_market[market_id]:
                        try:
                            price_precision = self._required_price_precision(token_id)
                            validated_yes_book = _validate_order_book_price_precision(
                                yes_book,
                                price_precision,
                            )
                            self._store_book(
                                token_id,
                                validated_yes_book
                                if side is BinarySide.YES
                                else _invert_binary_order_book(
                                    validated_yes_book,
                                    price_precision=price_precision,
                                ),
                                confirmed_at_receipt=True,
                            )
                        except (RuntimeError, ValueError) as exc:
                            LOGGER.warning(
                                "predict_fun_batch_token_orderbook_invalid",
                                extra={
                                    "_market_id": market_id,
                                    "_token_id": token_id,
                                    "_error_type": type(exc).__name__,
                                },
                            )
                omitted_market_ids = set(chunk) - returned_market_ids
                if omitted_market_ids:
                    # Omission is not an authoritative empty-book snapshot. Leave
                    # these tokens uncached so watch_order_book performs the
                    # single-market REST recovery before declaring them unavailable.
                    LOGGER.debug(
                        "predict_fun_batch_orderbooks_omitted_markets",
                        extra={"_omitted_market_count": len(omitted_market_ids)},
                    )

    async def prime_market_data_targets(self) -> None:
        if not self._config.api_base_url or not self._tracked_tokens:
            return
        if all(
            token_id in self._books
            and self._cached_book_is_passively_fresh(token_id, self._books[token_id])
            for token_id in self._tracked_tokens
        ):
            return
        await self._refresh_rest_books_batch()

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
        for (token_id, side), result in zip(registered, results, strict=True):
            success, return_data = result
            if not success:
                continue
            reserves = web3_client.w3.codec.decode(output_types, bytes(return_data))
            self._store_book(
                token_id,
                _order_book_from_reserves(
                    reserves,
                    side,
                    float(Decimal(self._required_fee_rate_bps(token_id)) / Decimal(10_000)),
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

    async def buy_with_order_id_persistence(
        self,
        token_id: str,
        side: BinarySide,
        contracts: float,
        max_price: float,
        *,
        persist_order_id: Callable[[str], Awaitable[None]],
        pre_transport_guard: Callable[[], None] | None = None,
        client_order_id: str | None = None,
        prepared_order_fingerprint: str | None = None,
        submission_deadline_unix: float | None = None,
        condition_id: str | None = None,
        tick_size: str | None = None,
        neg_risk: bool | None = None,
    ) -> str:
        del client_order_id, prepared_order_fingerprint, submission_deadline_unix, condition_id, tick_size
        order_id = await self._submit_sdk_order(
            token_id,
            side,
            contracts,
            max_price,
            sdk_side_name="BUY",
            neg_risk=bool(neg_risk),
            pre_transport_guard=pre_transport_guard,
        )
        await persist_order_id(order_id)
        return order_id

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
            payload = await self._request_json("GET", f"/v1/orders/{order_id}", require_jwt=True)
            status = str(
                _extract_first_nested(payload, ("status", "state", "orderStatus", "order_status")) or ""
            ).lower()
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
        await self._request_json(
            "POST",
            "/v1/orders/remove",
            json_body={"data": {"ids": [cancel_id]}},
            require_jwt=True,
        )

    async def get_cash_balance(self) -> float:
        return float((await self.get_cash_balance_details())["balance"])

    async def get_cash_balance_details(self) -> dict[str, Any]:
        return await self._get_collateral_balance_details()

    async def get_order(self, order_id: str) -> ExecutionReport:
        payload = await self._request_json("GET", f"/v1/orders/{order_id}", require_jwt=True)
        requested = self._order_amounts.get(order_id, _extract_requested_amount(payload, self._config.precision))
        filled = _extract_filled_amount(payload) or 0.0
        filled = _normalize_order_amount(filled, requested, self._config.precision)
        status = str(_extract_first_nested(payload, ("status", "state", "orderStatus")) or "open")
        price = _normalize_price(
            _extract_avg_price(payload) or self._order_prices.get(order_id, 0.0), self._config.precision
        )
        return ExecutionReport.from_amounts(order_id, requested, filled, status, price)

    async def list_open_orders(self) -> list[VenueOrder]:
        payload = await self._request_json("GET", "/v1/orders", query_params={"status": "OPEN"}, require_jwt=True)
        return [
            _venue_order_from_payload(item, self._config.precision)
            for item in _extract_records(payload, ("orders", "items", "results"))
        ]

    async def list_fills(self, since: datetime | None = None) -> list[FillRecord]:
        params = {"since": since.isoformat()} if since is not None else None
        try:
            payload = await self._request_json("GET", "/v1/trades", query_params=params, require_jwt=True)
        except Exception as exc:
            if _is_http_not_found(exc):
                LOGGER.info("predict_fun_trades_endpoint_unavailable", extra={"_path": "/v1/trades"})
                return []
            raise
        return [
            _fill_from_trade(item, self._config.precision)
            for item in _extract_records(payload, ("trades", "fills", "items", "results"))
        ]

    async def get_positions(self) -> dict[str, Decimal]:
        try:
            payload = await self._request_json("GET", "/v1/positions", require_jwt=True)
        except Exception as exc:
            if _is_http_not_found(exc):
                LOGGER.info("predict_fun_positions_endpoint_unavailable", extra={"_path": "/v1/positions"})
                return {}
            raise
        positions: dict[str, Decimal] = {}
        for item in _extract_records(payload, ("positions", "items", "results")):
            token_id = str(
                _extract_first_nested(
                    item,
                    (
                        "tokenId",
                        "token_id",
                        "outcomeTokenId",
                        "outcome_token_id",
                        "onChainId",
                        "on_chain_id",
                        "assetId",
                        "asset_id",
                    ),
                )
                or ""
            )
            if not token_id:
                continue
            amount = _extract_position_amount(item, self._config.precision)
            if amount is None:
                continue
            positions[token_id] = positions.get(token_id, Decimal(0)) + amount
        return positions

    def supports_full_reconciliation(self) -> bool:
        return True

    async def get_market_constraints(self, token_id: str, condition_id: str | None = None) -> MarketConstraints | None:
        del condition_id
        if token_id not in self._token_fee_rate_bps or token_id not in self._token_price_precision:
            return None
        return MarketConstraints(
            fee_rate_bps=self._token_fee_rate_bps[token_id],
            tick_size=Decimal(1) / (Decimal(10) ** self._token_price_precision[token_id]),
            lot_size=Decimal(1) / (Decimal(10) ** self._config.precision),
            minimum_notional=Decimal("1"),
        )

    async def get_fee_quote(
        self,
        token_id: str,
        average_price: Decimal,
        constraints: MarketConstraints | None = None,
    ) -> VenueFeeQuote | None:
        del average_price
        resolved = constraints or await self.get_market_constraints(token_id)
        if resolved is None:
            return None
        return VenueFeeQuote(
            "Predict.fun",
            resolved.fee_rate_bps,
            "notional_bps",
            source="predict_market_fee_rate",
            verified=True,
        )

    async def preview_buy(
        self,
        token_id: str,
        side: BinarySide,
        contracts: Decimal,
        max_price: Decimal,
        *,
        condition_id: str | None = None,
        tick_size: str | None = None,
        neg_risk: bool | None = None,
    ) -> OrderPreview:
        del tick_size
        normalized_price = self._normalized_limit_price(token_id, max_price, is_buy=True)
        constraints = await self.get_market_constraints(token_id, condition_id)
        book = await self.watch_order_book(token_id)
        return await self._preview_buy_from_book(
            token_id,
            side,
            contracts,
            normalized_price,
            book,
            condition_id=condition_id,
            tick_size=str(constraints.tick_size) if constraints is not None else None,
            neg_risk=neg_risk,
        )

    def _normalized_limit_price(
        self,
        token_id: str,
        price: Decimal | float,
        *,
        is_buy: bool,
    ) -> Decimal:
        precision = self._required_price_precision(token_id)
        tick_size = Decimal(1).scaleb(-precision)
        value = Decimal(str(price))
        # A caller-provided max/min price is a hard execution bound.
        rounding = ROUND_FLOOR if is_buy else ROUND_CEILING
        normalized = (value / tick_size).to_integral_value(rounding=rounding) * tick_size
        if normalized <= 0 or normalized > 1:
            raise ValueError(f"Predict.fun limit price is outside the executable range for token {token_id}")
        return normalized

    async def _preview_buy_signature_for_book(
        self,
        token_id: str,
        side: BinarySide,
        contracts: Decimal,
        max_price: Decimal,
        *,
        book: OrderBook,
        condition_id: str | None,
        tick_size: str | None,
        neg_risk: bool | None,
    ) -> str | None:
        del condition_id, tick_size
        if not self._config.private_key:
            return None
        normalized_price = self._normalized_limit_price(token_id, max_price, is_buy=True)
        built = self._build_signed_order_payload(
            token_id=token_id,
            contracts=float(contracts),
            limit_price=float(normalized_price),
            sdk_side_name="BUY",
            neg_risk=bool(neg_risk),
            fee_rate_bps=self._required_fee_rate_bps(token_id),
            book=book,
        )
        return hashlib.sha256(
            json.dumps(built.signed_order, sort_keys=True, default=str).encode("utf-8")
        ).hexdigest()

    async def _watch_order_book_rest(self, token_id: str) -> OrderBook:
        if not self._config.api_base_url:
            raise RuntimeError("predict_fun.api_base_url is required for REST orderbook access")
        market_identity = self._market_identifiers.get(token_id)
        if market_identity is None:
            raise RuntimeError(f"Predict.fun market id and side are not registered for token {token_id}")
        market_id, side = market_identity
        payload = await self._request_json("GET", f"/v1/markets/{market_id}/orderbook")
        yes_book = _validate_order_book_price_precision(
            _order_book_from_payload(payload),
            self._required_price_precision(token_id),
        )
        execution_book = (
            yes_book
            if side is BinarySide.YES
            else _invert_binary_order_book(
                yes_book,
                price_precision=self._required_price_precision(token_id),
            )
        )
        if execution_book.asks:
            return execution_book
        raise OrderBookUnavailableException(
            f"Predict.fun REST API did not return executable asks for token {token_id}"
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
            float(Decimal(self._required_fee_rate_bps(token_id)) / Decimal(10_000)),
        )

    def _store_book(self, token_id: str, book: OrderBook, *, confirmed_at_receipt: bool = False) -> None:
        received_at = time.time()
        received_at_monotonic = time.monotonic()
        timestamp = received_at if confirmed_at_receipt else min(book.timestamp, received_at)
        self._books[token_id] = replace(book, timestamp=timestamp)
        self._book_timestamps[token_id] = received_at_monotonic
        self._last_market_data_at = received_at_monotonic
        self._book_events.setdefault(token_id, asyncio.Event()).set()

    def _cached_book_is_passively_fresh(self, token_id: str, book: OrderBook) -> bool:
        if book.status is not MarketDataStatus.VALID:
            return False
        return self._healthy_stream_confirms_book(token_id) or (
            time.monotonic() - self._book_timestamps.get(token_id, 0.0) <= ORDER_BOOK_MAX_AGE_SECONDS
        )

    def _healthy_stream_confirms_book(self, token_id: str) -> bool:
        market_identity = self._market_identifiers.get(token_id)
        if market_identity is None:
            return False
        market_id = market_identity[0]
        required_topics = {
            f"predictOrderbook/{market_id}",
            f"predictTradingStatus/{market_id}",
        }
        heartbeat_age = (
            None
            if self._last_application_heartbeat_at is None
            else time.monotonic() - self._last_application_heartbeat_at
        )
        return bool(
            self._ws_connected
            and self._ws is not None
            and not self._ws.closed
            and token_id in self._tracked_tokens
            and required_topics.issubset(self._ws_subscribed_topics)
            and market_id in self._ws_session_orderbook_markets
            and market_id in self._ws_session_status_markets
            and heartbeat_age is not None
            and heartbeat_age <= _APPLICATION_HEARTBEAT_MAX_AGE_SECONDS
            and self._trading_status.get(market_id) == "OPEN"
        )

    def _execution_book_from_cache(self, token_id: str, book: OrderBook) -> OrderBook:
        if self._healthy_stream_confirms_book(token_id):
            return replace(book, timestamp=time.time())
        return book

    def market_data_age_seconds(self) -> float | None:
        if not self._tracked_tokens:
            return None
        timestamps = [
            self._book_timestamps[token_id]
            for token_id in self._tracked_tokens
            if token_id in self._book_timestamps
        ]
        latest_timestamp = max(timestamps, default=self._last_market_data_at)
        if latest_timestamp is None:
            return None
        now = time.monotonic()
        return now - latest_timestamp

    def market_data_target_age_seconds(self, token_id: str) -> float | None:
        timestamp = self._book_timestamps.get(token_id)
        if timestamp is None:
            return None
        return max(0.0, time.monotonic() - timestamp)

    def market_data_target_ready(self, token_id: str, max_age_seconds: float) -> bool:
        book = self._books.get(token_id)
        return book is not None and self.is_order_book_execution_fresh(token_id, book, max_age_seconds)

    def market_data_ready(self) -> bool:
        if self._config.ws_url and self._config.api_key:
            if not all(self._healthy_stream_confirms_book(token_id) for token_id in self._tracked_tokens):
                return False
        statuses_open = all(
            self._trading_status.get(market_id) == "OPEN"
            for market_id, _ in {
                self._market_identifiers[token_id]
                for token_id in self._tracked_tokens
                if token_id in self._market_identifiers
            }
        )
        return bool(self._tracked_tokens) and statuses_open and all(
            token_id in self._books and self._books[token_id].status.value == "VALID"
            for token_id in self._tracked_tokens
        )

    def is_order_book_execution_fresh(
        self,
        token_id: str,
        book: OrderBook,
        max_age_seconds: float,
    ) -> bool:
        return self._cached_book_is_passively_fresh(token_id, book) or super().is_order_book_execution_fresh(
            token_id,
            book,
            max_age_seconds,
        )

    def telemetry_snapshot(self) -> dict[str, float]:
        return {
            "connected": float(self._ws_connected),
            "reconnects": float(self._reconnect_count),
            "sequence_gaps": float(self._sequence_gap_count),
            "reconnect_backoff_seconds": self._reconnect_backoff.current_delay_seconds,
        }

    def sync_market_data_targets(self, token_ids: set[str]) -> None:
        normalized = {token_id for token_id in token_ids if token_id}
        removed = self._tracked_tokens - normalized
        added = normalized - self._tracked_tokens
        self._tracked_tokens = set(normalized)
        for token_id in removed:
            market_identity = self._market_identifiers.get(token_id)
            if market_identity is not None and not self._market_has_tracked_token(market_identity[0]):
                self._queue_market_topics("unsubscribe", market_identity[0])
            self._books.pop(token_id, None)
            self._book_timestamps.pop(token_id, None)
            self._book_events.pop(token_id, None)
        if self._ws_connected:
            for market_id in sorted(
                {
                    self._market_identifiers[token_id][0]
                    for token_id in added
                    if token_id in self._market_identifiers
                }
            ):
                self._queue_market_topics("subscribe", market_id)
        if self._tracked_tokens:
            self._ensure_ws_task()
            self._ensure_multicall_task()
            self._ensure_rest_books_task()

    def _market_has_tracked_token(self, market_id: str) -> bool:
        return any(
            token_id in self._market_identifiers and self._market_identifiers[token_id][0] == market_id
            for token_id in self._tracked_tokens
        )

    def _desired_ws_topics(self) -> set[str]:
        market_ids = {
            self._market_identifiers[token_id][0]
            for token_id in self._tracked_tokens
            if token_id in self._market_identifiers
        }
        return {
            topic
            for market_id in market_ids
            for topic in (
                f"predictOrderbook/{market_id}",
                f"predictTradingStatus/{market_id}",
            )
        }

    def _reconcile_ws_topic(self, topic: str) -> None:
        desired = topic in self._desired_ws_topics()
        subscribed = topic in self._ws_subscribed_topics
        pending = set(self._ws_pending_requests.values())
        action = "subscribe" if desired else "unsubscribe"
        if desired == subscribed or (action, topic) in pending:
            return
        self._ws_subscription_queue.put_nowait((action, topic))

    def _ensure_ws_task(self) -> None:
        if not self._config.ws_url or not self._config.api_key:
            return
        if self._ws_task is None or self._ws_task.done():
            self._ws_task = asyncio.create_task(self._run_order_book_ws())

    def _queue_market_topics(self, action: str, market_id: str) -> None:
        for topic in (f"predictOrderbook/{market_id}", f"predictTradingStatus/{market_id}"):
            self._ws_subscription_queue.put_nowait((action, topic))

    async def _run_order_book_ws(self) -> None:
        try:
            import aiohttp
        except ImportError:
            return
        while True:
            connected_at: float | None = None
            sender: asyncio.Task[None] | None = None
            receiver: asyncio.Task[None] | None = None
            try:
                session = self._get_ws_session()
                headers = {"x-api-key": str(self._config.api_key)}
                async with session.ws_connect(
                    str(self._config.ws_url),
                    headers=headers,
                    heartbeat=_WS_HEARTBEAT_SECONDS,
                ) as ws:
                    self._ws = ws
                    self._ws_connected = True
                    connected_at = time.monotonic()
                    self._ws_subscribed_topics.clear()
                    self._ws_pending_requests.clear()
                    self._ws_pending_request_started_at.clear()
                    self._ws_session_orderbook_markets.clear()
                    self._ws_session_status_markets.clear()
                    self._last_application_heartbeat_at = None
                    request_id = 1
                    active_market_ids = {
                        self._market_identifiers[token_id][0]
                        for token_id in self._tracked_tokens
                        if token_id in self._market_identifiers
                    }
                    for market_id in sorted(active_market_ids):
                        for topic in (f"predictOrderbook/{market_id}", f"predictTradingStatus/{market_id}"):
                            self._record_ws_pending_request(request_id, "subscribe", topic)
                            await ws.send_json({"method": "subscribe", "requestId": request_id, "params": [topic]})
                            request_id += 1
                    sender = asyncio.create_task(self._send_ws_subscriptions(ws, request_id))
                    receiver = asyncio.create_task(self._receive_ws_messages(ws, aiohttp.WSMsgType.TEXT))
                    done, _ = await asyncio.wait(
                        (sender, receiver),
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    for completed in done:
                        await completed
            except asyncio.CancelledError:
                raise
            except (aiohttp.ClientConnectionError, ConnectionResetError) as exc:
                LOGGER.info("predict_fun_ws_disconnected", extra={"reason": type(exc).__name__})
            except Exception:
                LOGGER.exception("predict_fun_ws_failed")
            finally:
                active_tasks = [task for task in (sender, receiver) if task is not None]
                for task in active_tasks:
                    task.cancel()
                if active_tasks:
                    await asyncio.gather(*active_tasks, return_exceptions=True)
                self._ws_connected = False
                self._mark_ws_books_stale()
                self._ws_subscribed_topics.clear()
                self._ws_pending_requests.clear()
                self._ws_pending_request_started_at.clear()
                self._ws_session_orderbook_markets.clear()
                self._ws_session_status_markets.clear()
                self._last_application_heartbeat_at = None
                self._ws = None
                await self._close_ws_session()
            if connected_at is not None and time.monotonic() - connected_at >= 60:
                self._reconnect_backoff.reset()
            self._reconnect_count += 1
            await asyncio.sleep(self._reconnect_backoff.next_delay())

    async def _receive_ws_messages(self, ws: Any, text_message_type: Any) -> None:
        async for message in ws:
            if message.type != text_message_type:
                continue
            payload = json.loads(str(message.data))
            if isinstance(payload, dict):
                await self._handle_ws_message(ws, payload)

    async def _send_ws_subscriptions(self, ws: Any, request_id: int) -> None:
        while True:
            expired = self._expired_ws_pending_request()
            if expired is not None:
                expired_request_id, expired_topic = expired
                LOGGER.warning(
                    "predict_fun_ws_subscription_ack_timeout",
                    extra={"_request_id": expired_request_id, "_topic": expired_topic},
                )
                raise RuntimeError(f"Predict.fun subscription ACK timed out for {expired_topic}")
            try:
                action, topic = await asyncio.wait_for(
                    self._ws_subscription_queue.get(),
                    timeout=_WS_SUBSCRIPTION_WATCHDOG_INTERVAL_SECONDS,
                )
            except TimeoutError:
                continue
            desired_action = "subscribe" if topic in self._desired_ws_topics() else "unsubscribe"
            if action != desired_action:
                continue
            if any(pending_topic == topic for _, pending_topic in self._ws_pending_requests.values()):
                continue
            if action == "subscribe" and topic in self._ws_subscribed_topics:
                continue
            if action == "unsubscribe" and topic not in self._ws_subscribed_topics:
                continue
            self._record_ws_pending_request(request_id, action, topic)
            await ws.send_json({"method": action, "requestId": request_id, "params": [topic]})
            request_id += 1

    def _record_ws_pending_request(self, request_id: int, action: str, topic: str) -> None:
        self._ws_pending_requests[request_id] = (action, topic)
        self._ws_pending_request_started_at[request_id] = time.monotonic()

    def _expired_ws_pending_request(self) -> tuple[int, str] | None:
        now = time.monotonic()
        for request_id, (_, topic) in self._ws_pending_requests.items():
            started_at = self._ws_pending_request_started_at.get(request_id)
            if started_at is not None and now - started_at >= _WS_SUBSCRIPTION_ACK_TIMEOUT_SECONDS:
                return request_id, topic
        return None

    async def _handle_ws_message(self, ws: Any, payload: dict[str, Any]) -> None:
        if payload.get("type") == "R":
            raw_request_id = payload.get("requestId")
            if not isinstance(raw_request_id, (int, str)):
                return
            try:
                request_id = int(raw_request_id)
            except ValueError:
                return
            pending = self._ws_pending_requests.pop(request_id, None)
            self._ws_pending_request_started_at.pop(request_id, None)
            if pending is None:
                return
            if payload.get("success") is not True:
                raise RuntimeError(f"Predict.fun subscription rejected: {payload.get('error')!r}")
            action, topic = pending
            if action == "subscribe":
                self._ws_subscribed_topics.add(topic)
            else:
                self._ws_subscribed_topics.discard(topic)
                market_id = topic.rsplit("/", 1)[-1]
                if topic.startswith("predictOrderbook/"):
                    self._ws_session_orderbook_markets.discard(market_id)
                elif topic.startswith("predictTradingStatus/"):
                    self._ws_session_status_markets.discard(market_id)
            self._reconcile_ws_topic(topic)
            return
        if payload.get("type") != "M":
            return
        topic = str(payload.get("topic") or "")
        data = payload.get("data")
        if topic == "heartbeat":
            heartbeat_timestamp_ms = _validated_epoch_milliseconds(data, field_name="heartbeat.data")
            if heartbeat_timestamp_ms < int(time.time() * 1000) - 30_000:
                raise RuntimeError("Predict.fun heartbeat timestamp is stale")
            self._last_application_heartbeat_at = time.monotonic()
            await ws.send_json({"method": "heartbeat", "data": data})
            return
        if not isinstance(data, dict):
            return
        market_id = topic.rsplit("/", 1)[-1]
        if topic.startswith("predictTradingStatus/"):
            timestamp_ms = _validated_epoch_milliseconds(
                data.get("tsMs"),
                field_name="predictTradingStatus.tsMs",
            )
            if timestamp_ms < self._trading_status_timestamps_ms.get(market_id, 0):
                return
            self._trading_status_timestamps_ms[market_id] = timestamp_ms
            self._trading_status[market_id] = str(data.get("tradingStatus") or "UNKNOWN").upper()
            self._ws_session_status_markets.add(market_id)
            if self._trading_status[market_id] != "OPEN":
                self._mark_market_books_invalid(market_id)
            return
        if not topic.startswith("predictOrderbook/"):
            return
        if int(data.get("version") or 0) != 1:
            raise RuntimeError(f"Predict.fun orderbook version is unsupported: {data.get('version')!r}")
        payload_fingerprint = hashlib.sha256(
            json.dumps(data, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
        ).hexdigest()
        raw_timestamp_ms = data.get("updateTimestampMs")
        zero_timestamp = raw_timestamp_ms == "0" or (
            isinstance(raw_timestamp_ms, int)
            and not isinstance(raw_timestamp_ms, bool)
            and raw_timestamp_ms == 0
        )
        zero_timestamp_empty_snapshot = (
            zero_timestamp
            and isinstance(data.get("bids"), list)
            and not data["bids"]
            and isinstance(data.get("asks"), list)
            and not data["asks"]
        )
        if zero_timestamp_empty_snapshot:
            # Predict uses zero for a never-populated initial book. It is not an
            # ordering value, so accept it only as the current session snapshot
            # and retain the last real update timestamp across reconnects.
            if (
                market_id in self._ws_session_orderbook_markets
                and self._market_update_fingerprints.get(market_id) == payload_fingerprint
            ):
                return
        else:
            timestamp_ms = _validated_epoch_milliseconds(
                raw_timestamp_ms,
                field_name="predictOrderbook.updateTimestampMs",
            )
            previous = self._market_update_timestamps_ms.get(market_id, 0)
            if timestamp_ms < previous:
                self._sequence_gap_count += 1
                return
            if (
                timestamp_ms == previous
                and market_id in self._ws_session_orderbook_markets
                and self._market_update_fingerprints.get(market_id) == payload_fingerprint
            ):
                return
            self._market_update_timestamps_ms[market_id] = timestamp_ms
        self._market_update_fingerprints[market_id] = payload_fingerprint
        yes_book = _order_book_from_payload({"data": data})
        stored_current_session_book = False
        for token_id, (registered_market, side) in self._market_identifiers.items():
            if registered_market != market_id or token_id not in self._tracked_tokens:
                continue
            validated_yes_book = _validate_order_book_price_precision(
                yes_book,
                self._required_price_precision(token_id),
            )
            book = (
                validated_yes_book
                if side is BinarySide.YES
                else _invert_binary_order_book(
                    validated_yes_book,
                    price_precision=self._required_price_precision(token_id),
                )
            )
            if self._trading_status.get(market_id, "OPEN") != "OPEN":
                book = replace(book, status=MarketDataStatus.INVALID)
            self._store_book(token_id, book)
            stored_current_session_book = True
        if stored_current_session_book:
            self._ws_session_orderbook_markets.add(market_id)

    def _mark_market_books_invalid(self, market_id: str) -> None:
        for token_id, (registered_market, _) in self._market_identifiers.items():
            if registered_market == market_id and token_id in self._books:
                self._books[token_id] = replace(self._books[token_id], status=MarketDataStatus.INVALID)

    def _mark_ws_books_stale(self) -> None:
        for token_id in self._tracked_tokens & self._books.keys():
            self._books[token_id] = replace(self._books[token_id], status=MarketDataStatus.STALE)

    def _get_ws_session(self) -> Any:
        if self._ws_session is None or self._ws_session.closed:
            self._ws_session = client_session()
        return self._ws_session

    async def _close_ws_session(self) -> None:
        if self._ws_session is not None and not self._ws_session.closed:
            await self._ws_session.close()
        self._ws_session = None

    def has_active_market_data_targets(self) -> bool:
        return bool(self._tracked_tokens)

    def active_market_data_target_count(self) -> int:
        return len(self._tracked_tokens)

    async def _submit_sdk_order(
        self,
        token_id: str,
        side: BinarySide,
        contracts: float,
        limit_price: float,
        *,
        sdk_side_name: str,
        neg_risk: bool,
        pre_transport_guard: Callable[[], None] | None = None,
    ) -> str:
        if not self._config.private_key:
            raise RuntimeError("PREDICT_FUN_PRIVATE_KEY is required for Predict.fun production orders")
        if not self._config.api_base_url:
            raise RuntimeError("predict_fun.api_base_url is required for Predict.fun order submission")
        normalized_price = self._normalized_limit_price(
            token_id,
            limit_price,
            is_buy=sdk_side_name == "BUY",
        )
        normalized_price_float = float(normalized_price)
        fee_rate_bps = self._required_fee_rate_bps(token_id)
        book = await self.watch_order_book(token_id)
        built = self._build_signed_order_payload(
            token_id=token_id,
            contracts=contracts,
            limit_price=normalized_price_float,
            sdk_side_name=sdk_side_name,
            neg_risk=neg_risk,
            fee_rate_bps=fee_rate_bps,
            book=book,
        )
        del side
        payload = {
            "data": {
                "pricePerShare": str(built.price_per_share_wei),
                "amount": str(built.amount_wei),
                "strategy": "MARKET",
                "slippageBps": str(built.slippage_bps),
                "feeRateBps": str(fee_rate_bps),
                "isMinAmountOut": built.is_min_amount_out,
                "isFillOrKill": True,
                "isPostOnly": False,
                "reservedBalancePolicy": "REJECT_MARKET_ORDER",
                "order": built.signed_order,
            }
        }
        response = await self._request_json(
            "POST",
            "/v1/orders",
            json_body=payload,
            require_jwt=True,
            before_request=pre_transport_guard,
        )
        if response.get("success") is False:
            raise RuntimeError(f"Predict.fun rejected order creation: {response!r}")
        order_hash = _extract_first_nested(response, ("orderHash", "order_hash", "hash"))
        cancel_id = _extract_first_nested(response, ("orderId", "order_id", "id"))
        if not order_hash:
            raise RuntimeError(f"Predict.fun order response does not include an order id: {response!r}")
        normalized_order_id = str(order_hash)
        scale = Decimal(10) ** self._config.precision
        self._order_amounts[normalized_order_id] = float(Decimal(built.amount_wei) / scale)
        self._order_prices[normalized_order_id] = float(Decimal(built.price_per_share_wei) / scale)
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
        book: OrderBook,
        fee_rate_bps: int | None = None,
    ) -> _SignedPredictMarketOrder:
        builder = self._get_order_builder()
        sdk_side = _sdk_side(sdk_side_name)
        limit = Decimal(str(limit_price))
        is_buy = sdk_side_name == "BUY"
        _require_executable_limit_depth(book, Decimal(str(contracts)), limit, is_buy=is_buy)
        configured_slippage_bps = int(
            min(Decimal(str(self._config.max_slippage_pct)), Decimal("0.015")) * Decimal(10_000)
        )
        sdk_book = _sdk_order_book(self._required_market_id(token_id), book)

        def calculate_amounts(slippage_bps: int) -> Any:
            return builder.get_market_order_amounts(
                _sdk_market_helper_input(
                    side=sdk_side,
                    quantity_wei=_to_precision_units(contracts, self._config.precision),
                    slippage_bps=slippage_bps,
                    is_min_amount_out=is_buy,
                ),
                sdk_book,
            )

        amounts = calculate_amounts(configured_slippage_bps)
        if not _market_order_respects_limit(
            amounts,
            limit,
            is_buy=is_buy,
        ):
            amounts = calculate_amounts(0)
        if not _market_order_respects_limit(
            amounts,
            limit,
            is_buy=is_buy,
        ):
            raise OrderBookUnavailableException("Predict.fun market order exceeds the hard limit price")
        if int(amounts.amount) <= 0 or int(amounts.price_per_share) <= 0:
            raise OrderBookUnavailableException("Predict.fun market order calculation returned no executable amount")
        order = builder.build_order(
            "MARKET",
            _sdk_build_order_input(
                side=sdk_side,
                token_id=token_id,
                maker_amount=str(amounts.maker_amount),
                taker_amount=str(amounts.taker_amount),
                fee_rate_bps=str(self._config.fee_rate_bps if fee_rate_bps is None else fee_rate_bps),
                maker=self._trading_account_address(),
                signer=self._trading_account_address(),
            ),
        )
        typed_data = builder.build_typed_data(order, is_neg_risk=neg_risk, is_yield_bearing=False)
        order_hash = str(builder.build_typed_data_hash(typed_data))
        if not order_hash:
            raise RuntimeError("Predict.fun SDK returned an empty order hash")
        signed_order = builder.sign_typed_data_order(typed_data)
        return _SignedPredictMarketOrder(
            signed_order=_signed_order_to_payload(signed_order, order_hash=order_hash),
            amount_wei=int(amounts.amount),
            price_per_share_wei=int(amounts.price_per_share),
            slippage_bps=int(amounts.slippage_bps),
            is_min_amount_out=bool(amounts.is_min_amount_out),
        )

    def _required_market_id(self, token_id: str) -> str:
        identity = self._market_identifiers.get(token_id)
        if identity is None:
            raise RuntimeError(f"Predict.fun market id and side are not registered for token {token_id}")
        return identity[0]

    def _get_order_builder(self) -> Any:
        if self._order_builder is not None:
            return self._order_builder
        if self._order_builder_factory is not None:
            self._order_builder = self._order_builder_factory()
            return self._order_builder
        try:
            from predict_sdk import order_builder
        except ImportError as exc:
            raise RuntimeError("predict-sdk is required for Predict.fun order signing") from exc
        options_type_name = "OrderBuilderOptions"
        order_builder_options = getattr(order_builder, options_type_name)
        self._order_builder = order_builder.OrderBuilder.make(
            _sdk_chain_id(self._config.chain_id),
            signer=self._config.private_key,
            options=order_builder_options(
                precision=self._config.precision,
                predict_account=self._config.account_address,
                generate_salt=_generate_order_salt,
                log_level="INFO",
            ),
        )
        return self._order_builder

    async def _get_collateral_balance(self) -> float:
        return float((await self._get_collateral_balance_details())["balance"])

    async def _get_collateral_balance_details(self) -> dict[str, Any]:
        collateral = self._config.collateral_token_address or _sdk_collateral_token(self._config.chain_id)
        account_address = self._trading_account_address()
        if not account_address:
            raise RuntimeError("PREDICT_FUN_PRIVATE_KEY is required for balance checks")
        token = self._get_web3_client().contract(collateral, ERC20_BALANCE_ABI)
        try:
            balance_call = getattr(token.functions, self._config.balance_function)
        except AttributeError as exc:
            raise RuntimeError(
                f"Predict.fun collateral token does not expose {self._config.balance_function}(address)"
            ) from exc
        raw_balance = await balance_call(account_address).call()
        decimals = await self._get_collateral_decimals(token)
        balance = float(raw_balance) / float(10**decimals)
        return {
            "wallet_address": account_address,
            "signer_wallet_address": self._signer_address(),
            "collateral_token_address": collateral,
            "balance_function": self._config.balance_function,
            "balance_raw": str(raw_balance),
            "decimals": decimals,
            "balance": balance,
        }

    async def _get_collateral_decimals(self, token: Any) -> int:
        if self._collateral_decimals is None:
            raw_decimals = await token.functions.decimals().call()
            self._collateral_decimals = int(raw_decimals)
        return self._collateral_decimals

    async def _request_json(
        self,
        method: str,
        path: str,
        json_body: dict[str, Any] | None = None,
        query_params: Mapping[str, str] | Sequence[tuple[str, str]] | None = None,
        *,
        require_jwt: bool = False,
        before_request: Callable[[], None] | None = None,
    ) -> dict[str, Any]:
        if not self._config.api_base_url:
            raise RuntimeError("predict_fun.api_base_url is required")
        try:
            import aiohttp

            _ = aiohttp
        except ImportError as exc:
            raise RuntimeError("aiohttp is required for Predict.fun REST connectivity") from exc

        url = f"{self._config.api_base_url.rstrip('/')}/{path.lstrip('/')}"
        session = self._get_rest_session()
        auth_failures_remaining = 1 if require_jwt else 0
        allow_public_jwt_retry = (
            not require_jwt
            and path.startswith("/v1/markets/")
            and bool(self._config.api_key and self._config.private_key)
        )
        use_jwt = require_jwt
        while True:
            headers = await self._request_headers(require_jwt=use_jwt)
            async with self._http_semaphore:
                if before_request is not None:
                    before_request()
                async with session.request(
                    method,
                    url,
                    json=json_body,
                    params=query_params,
                    headers=headers,
                    timeout=10,
                ) as response:
                    if response.status in (401, 403) and use_jwt and auth_failures_remaining > 0:
                        await response.read()
                        auth_failures_remaining -= 1
                        self._jwt_token = None
                        continue
                    if response.status in (401, 403) and not use_jwt and allow_public_jwt_retry:
                        await response.read()
                        LOGGER.info("predict_fun_market_data_requires_jwt", extra={"_path": path})
                        use_jwt = True
                        continue
                    response.raise_for_status()
                    payload = await response.json()
            if payload is not None:
                break
        if not isinstance(payload, dict):
            raise RuntimeError(f"Predict.fun API returned unsupported payload: {payload!r}")
        return payload

    async def _request_headers(self, *, require_jwt: bool) -> dict[str, str]:
        headers = {"Content-Type": "application/json"}
        if self._config.api_key:
            headers["x-api-key"] = self._config.api_key
            headers["X-API-Key"] = self._config.api_key
        if require_jwt:
            headers["Authorization"] = f"Bearer {await self._get_jwt_token()}"
        return headers

    async def _get_jwt_token(self) -> str:
        if self._jwt_token:
            return self._jwt_token
        async with self._jwt_lock:
            if not self._config.api_base_url:
                raise RuntimeError("predict_fun.api_base_url is required for Predict.fun authentication")
            if not self._config.api_key:
                raise RuntimeError("PREDICT_FUN_API_KEY is required for Predict.fun authentication")
            if not self._config.private_key:
                raise RuntimeError("PREDICT_FUN_PRIVATE_KEY is required for Predict.fun JWT authentication")
            message_payload = await self._request_json("GET", "/v1/auth/message")
            message = _extract_first_nested(message_payload, ("message", "raw", "text"))
            if not message:
                raise RuntimeError(
                    "Predict.fun auth message response is missing a signable message: "
                    f"{message_payload!r}"
                )
            signer = self._signer_address()
            if not signer:
                raise RuntimeError("PREDICT_FUN_PRIVATE_KEY is required for Predict.fun JWT authentication")
            signature = _sign_auth_message(self._config.private_key, str(message))
            token_payload = await self._request_json(
                "POST",
                "/v1/auth",
                json_body={
                    "signer": signer,
                    "signature": signature,
                    "message": str(message),
                },
            )
            token = _extract_first_nested(token_payload, ("token", "jwt", "accessToken", "access_token"))
            if not token:
                raise RuntimeError(f"Predict.fun auth response does not include a JWT token: {token_payload!r}")
            self._jwt_token = str(token)
            return self._jwt_token

    def _signer_address(self) -> str | None:
        account = self._get_web3_client().account
        if account is None:
            return None
        return str(account.address)

    def _trading_account_address(self) -> str | None:
        return self._config.account_address or self._signer_address()

    def _get_rest_session(self) -> Any:
        if self._rest_session is None or self._rest_session.closed:
            self._rest_session = client_session()
        return self._rest_session

    async def close(self) -> None:
        if self._ws_task is not None:
            self._ws_task.cancel()
            await asyncio.gather(self._ws_task, return_exceptions=True)
            self._ws_task = None
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
        await self._close_ws_session()
        if self._web3_client is not None:
            await self._web3_client.close()
        self._web3_client = None

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


def _sdk_market_helper_input(
    *,
    side: Any,
    quantity_wei: int,
    slippage_bps: int,
    is_min_amount_out: bool,
) -> Any:
    try:
        from predict_sdk.types import MarketHelperInput
    except ImportError as exc:
        raise RuntimeError("predict-sdk is required for Predict.fun order sizing") from exc
    return MarketHelperInput(
        side=side,
        quantity_wei=quantity_wei,
        slippage_bps=slippage_bps,
        is_min_amount_out=is_min_amount_out,
    )


def _sdk_order_book(market_id: str, book: OrderBook) -> Any:
    try:
        from predict_sdk.types import Book
    except ImportError as exc:
        raise RuntimeError("predict-sdk is required for Predict.fun order sizing") from exc
    try:
        numeric_market_id = int(market_id)
    except ValueError as exc:
        raise RuntimeError(f"Predict.fun market id is not numeric: {market_id}") from exc
    return Book(
        market_id=numeric_market_id,
        update_timestamp_ms=int(book.timestamp * 1000),
        asks=sorted(
            ((float(level.price), float(level.size)) for level in book.asks),
            key=lambda level: level[0],
        ),
        bids=sorted(
            ((float(level.price), float(level.size)) for level in book.bids),
            key=lambda level: level[0],
            reverse=True,
        ),
    )


def _require_executable_limit_depth(
    book: OrderBook,
    contracts: Decimal,
    limit_price: Decimal,
    *,
    is_buy: bool,
) -> None:
    if contracts <= 0:
        raise ValueError("Predict.fun order contracts must be positive")
    if book.status is not MarketDataStatus.VALID:
        raise OrderBookUnavailableException("Predict.fun market order requires a valid orderbook")
    remaining = contracts
    levels = book.asks if is_buy else book.bids
    for level in levels:
        price = Decimal(str(level.price))
        size = Decimal(str(level.size))
        within_limit = price <= limit_price if is_buy else price >= limit_price
        if price <= 0 or size <= 0 or not within_limit:
            continue
        remaining -= min(remaining, size)
        if remaining <= Decimal("1e-18"):
            return
    raise OrderBookUnavailableException("Predict.fun orderbook has insufficient depth inside the hard limit price")


def _market_order_respects_limit(
    amounts: Any,
    limit_price: Decimal,
    *,
    is_buy: bool,
) -> bool:
    maker_amount = Decimal(int(amounts.maker_amount))
    taker_amount = Decimal(int(amounts.taker_amount))
    if maker_amount <= 0 or taker_amount <= 0:
        return False
    effective_price = maker_amount / taker_amount if is_buy else taker_amount / maker_amount
    return effective_price <= limit_price if is_buy else effective_price >= limit_price


def _validated_epoch_milliseconds(value: Any, *, field_name: str) -> int:
    if isinstance(value, str):
        if not (value.isascii() and value.isdecimal() and len(value) <= 16):
            raise RuntimeError(f"Predict.fun {field_name} must be an epoch-millisecond integer")
        value = int(value)
    if isinstance(value, bool) or not isinstance(value, int):
        raise RuntimeError(f"Predict.fun {field_name} must be an epoch-millisecond integer")
    now_ms = int(time.time() * 1000)
    if value < _MIN_PLAUSIBLE_EPOCH_MS or value > now_ms + _MAX_FUTURE_CLOCK_SKEW_MS:
        raise RuntimeError(f"Predict.fun {field_name} is outside the plausible epoch range")
    return value


def _sdk_build_order_input(
    *,
    side: Any,
    token_id: str,
    maker_amount: str,
    taker_amount: str,
    fee_rate_bps: str,
    maker: str | None = None,
    signer: str | None = None,
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
        maker=maker,
        signer=signer,
    )


def _sdk_collateral_token(chain_id: int) -> str:
    try:
        from predict_sdk.constants import ADDRESSES_BY_CHAIN_ID
    except ImportError as exc:
        raise RuntimeError("predict-sdk is required for Predict.fun contract addresses") from exc
    return str(ADDRESSES_BY_CHAIN_ID[_sdk_chain_id(chain_id)].USDT)


def _generate_order_salt() -> str:
    return str(secrets.randbits(256))


def _sign_auth_message(private_key: str, message: str) -> str:
    try:
        from eth_account import Account
        from eth_account.messages import encode_defunct
    except ImportError as exc:
        raise RuntimeError("eth-account is required for Predict.fun JWT authentication") from exc
    signed = Account.from_key(private_key).sign_message(encode_defunct(text=message))
    return str(signed.signature.hex() if signed.signature.hex().startswith("0x") else f"0x{signed.signature.hex()}")


def _signed_order_to_payload(signed_order: Any, *, order_hash: str) -> dict[str, Any]:
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
        "hash": order_hash,
    }
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
        payload.get("orderbook") or payload.get("orderBook") or payload.get("book") or payload.get("data") or payload
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


def _invert_binary_order_book(book: OrderBook, *, price_precision: int | None = None) -> OrderBook:
    def complement(price: float) -> float:
        source = Decimal(str(price))
        if price_precision is not None:
            _require_tick_aligned_price(source, price_precision)
        value = max(Decimal(0), Decimal(1) - source)
        return float(value)

    bids = [OrderBookLevel(price=complement(level.price), size=level.size) for level in book.asks]
    asks = [OrderBookLevel(price=complement(level.price), size=level.size) for level in book.bids]
    return OrderBook(
        bids=sorted(bids, key=lambda level: level.price, reverse=True),
        asks=sorted(asks, key=lambda level: level.price),
        raw_payload={"source": book.raw_payload, "inverted_from": BinarySide.YES.value},
        timestamp=book.timestamp,
        sequence=book.sequence,
        checksum=book.checksum,
        status=book.status,
    )


def _validate_order_book_price_precision(book: OrderBook, price_precision: int) -> OrderBook:
    for level in (*book.bids, *book.asks):
        _require_tick_aligned_price(Decimal(str(level.price)), price_precision)
    return book


def _require_tick_aligned_price(price: Decimal, price_precision: int) -> None:
    tick_size = Decimal(1).scaleb(-price_precision)
    if price.quantize(tick_size) != price:
        raise ValueError("Predict.fun orderbook contains an off-tick price")


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
    if (requested > 0 and value > requested * 1_000) or (requested <= 0 and abs(value) >= 10**12):
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


def _extract_requested_amount(payload: Any, precision: int) -> float:
    value = _extract_first_nested(payload, ("amount", "quantity", "originalAmount", "original_amount"))
    if value in (None, ""):
        return 0.0
    return _normalize_order_amount(float(str(value)), 0.0, precision)


def _extract_records(payload: Any, keys: tuple[str, ...]) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        for key in keys:
            value = payload.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
        data = payload.get("data")
        if data is not payload:
            return _extract_records(data, keys)
    return []


def _venue_order_from_payload(payload: dict[str, Any], precision: int) -> VenueOrder:
    order_id = str(_extract_first_nested(payload, ("orderHash", "hash", "orderId", "id")) or "")
    quantity = Decimal(str(_extract_requested_amount(payload, precision)))
    filled = _extract_filled_amount(payload) or 0.0
    normalized_filled = Decimal(str(_normalize_order_amount(filled, float(quantity), precision)))
    return VenueOrder(
        client_order_id="",
        venue_order_id=order_id,
        venue="Predict.fun",
        status=OrderIntentStatus.PARTIAL if normalized_filled > 0 else OrderIntentStatus.ACKNOWLEDGED,
        quantity=quantity,
        cumulative_filled=normalized_filled,
        average_price=Decimal(str(_normalize_price(_extract_avg_price(payload) or 0.0, precision))),
        updated_at=datetime.now(UTC),
    )


def _fill_from_trade(payload: dict[str, Any], precision: int) -> FillRecord:
    fill_id = str(_extract_first_nested(payload, ("id", "tradeId", "trade_id", "fillId", "fill_id")) or "")
    order_id = str(_extract_first_nested(payload, ("orderHash", "orderId", "order_id", "hash")) or fill_id)
    occurred_at = datetime.fromtimestamp(event_timestamp(payload), tz=UTC)
    raw_quantity = _extract_filled_amount(payload) or 0.0
    return FillRecord(
        fill_id=fill_id,
        client_order_id="",
        venue_order_id=order_id,
        venue="Predict.fun",
        quantity=Decimal(str(_normalize_order_amount(raw_quantity, 0.0, precision))),
        price=Decimal(str(_normalize_price(_extract_avg_price(payload) or 0.0, precision))),
        fee=Decimal(str(_extract_first_nested(payload, ("fee", "feeAmount", "fee_amount")) or 0)),
        occurred_at=occurred_at,
    )


def _extract_position_amount(payload: dict[str, Any], precision: int) -> Decimal | None:
    raw_amount = _extract_first_nested(
        payload,
        (
            "size",
            "quantity",
            "shares",
            "amount",
            "balance",
            "positionSize",
            "position_size",
            "contracts",
        ),
    )
    if raw_amount in (None, ""):
        return None
    try:
        amount = float(str(raw_amount))
    except (TypeError, ValueError):
        return None
    return Decimal(str(_normalize_order_amount(amount, 0.0, precision)))


def _is_http_not_found(exc: Exception) -> bool:
    status = getattr(exc, "status", None)
    return status == 404 or "404" in str(exc)
