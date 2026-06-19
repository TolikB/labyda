from __future__ import annotations

import asyncio
import logging
import threading
import time
from dataclasses import replace
from typing import Any

from arbitrage_engine.config import PolymarketConfig
from arbitrage_engine.connectors.base import OrderBookStaleException, PolymarketClient, event_timestamp
from arbitrage_engine.http import client_session
from arbitrage_engine.models import BinarySide, ExecutionReport, OrderBook, OrderBookLevel
from arbitrage_engine.utils.math import quantize_down, quantize_up

LOGGER = logging.getLogger(__name__)
ORDER_BOOK_MAX_AGE_SECONDS = 0.3


class PolymarketClobClient(PolymarketClient):
    def __init__(self, config: PolymarketConfig) -> None:
        self._config = config
        self._sdk_client: Any | None = None
        self._sdk_client_lock = threading.Lock()
        self._books: dict[str, OrderBook] = {}
        self._book_timestamps: dict[str, float] = {}
        self._book_events: dict[str, asyncio.Event] = {}
        self._ws_task: asyncio.Task[None] | None = None
        self._ws_session: Any | None = None
        self._desired_tokens: set[str] = set()
        self._subscription_queue: asyncio.Queue[str] = asyncio.Queue()
        self._bootstrap_tasks: dict[str, asyncio.Task[OrderBook]] = {}
        self._bootstrap_attempted: set[str] = set()
        self._order_amounts: dict[str, float] = {}
        self._order_prices: dict[str, float] = {}
        self._rest_session: Any | None = None
        self._http_semaphore = asyncio.Semaphore(20)

    async def watch_order_book(self, token_id: str) -> OrderBook:
        self._register_token(token_id)
        if token_id in self._books and time.monotonic() - self._book_timestamps.get(token_id, 0.0) <= ORDER_BOOK_MAX_AGE_SECONDS:
            return self._books[token_id]

        if token_id not in self._books:
            task = self._bootstrap_tasks.get(token_id)
            if task is None and token_id not in self._bootstrap_attempted:
                self._bootstrap_attempted.add(token_id)
                task = asyncio.create_task(self._fetch_order_book_http(token_id))
                self._bootstrap_tasks[token_id] = task
            if task is None:
                raise OrderBookStaleException(f"Polymarket order book bootstrap unavailable for token {token_id}")
            try:
                return await asyncio.shield(task)
            except Exception:
                self._bootstrap_attempted.discard(token_id)
                raise
            finally:
                if task.done():
                    self._bootstrap_tasks.pop(token_id, None)

        event = self._book_events[token_id]
        event.clear()
        try:
            await asyncio.wait_for(event.wait(), timeout=ORDER_BOOK_MAX_AGE_SECONDS)
            return self._books[token_id]
        except asyncio.TimeoutError as exc:
            LOGGER.warning("polymarket_ws_snapshot_timeout", extra={"_token_id": token_id})
            raise OrderBookStaleException(f"Polymarket order book is stale for token {token_id}") from exc

    async def _fetch_order_book_http(self, token_id: str) -> OrderBook:
        try:
            import aiohttp
        except ImportError as exc:
            raise RuntimeError("aiohttp is required for Polymarket connectivity") from exc

        url = f"{self._config.api_base_url}/book"
        session = self._get_rest_session()
        async with self._http_semaphore:
            async with session.get(url, params={"token_id": token_id}, timeout=10) as response:
                response.raise_for_status()
                raw: dict[str, Any] = await response.json()
        bids = [OrderBookLevel(float(item["price"]), float(item["size"])) for item in raw.get("bids", [])]
        asks = [OrderBookLevel(float(item["price"]), float(item["size"])) for item in raw.get("asks", [])]
        book = OrderBook(bids=_sorted_bids(bids)[:10], asks=_sorted_asks(asks)[:10], raw_payload=raw)
        self._update_book(token_id, book)
        return book

    def _get_rest_session(self) -> Any:
        if self._rest_session is None or self._rest_session.closed:
            self._rest_session = client_session()
        return self._rest_session

    def _get_ws_session(self) -> Any:
        if self._ws_session is None or self._ws_session.closed:
            self._ws_session = client_session()
        return self._ws_session

    async def close(self) -> None:
        tasks: list[asyncio.Task[Any]] = list(self._bootstrap_tasks.values())
        if self._ws_task is not None:
            tasks.append(self._ws_task)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._bootstrap_tasks.clear()
        self._ws_task = None
        if self._rest_session is not None and not self._rest_session.closed:
            await self._rest_session.close()
        self._rest_session = None
        if self._ws_session is not None and not self._ws_session.closed:
            await self._ws_session.close()
        self._ws_session = None

    def _register_token(self, token_id: str) -> None:
        self._book_events.setdefault(token_id, asyncio.Event())
        if token_id not in self._desired_tokens:
            self._desired_tokens.add(token_id)
            self._subscription_queue.put_nowait(token_id)
        if self._ws_task is None or self._ws_task.done():
            self._ws_task = asyncio.create_task(self._run_order_book_ws())

    async def _run_order_book_ws(self) -> None:
        try:
            import aiohttp
        except ImportError as exc:
            LOGGER.warning("polymarket_ws_unavailable", extra={"_error": str(exc)})
            return

        ws_url = _clob_ws_url(self._config.api_base_url)
        while True:
            try:
                session = self._get_ws_session()
                async with session.ws_connect(ws_url, heartbeat=10) as ws:
                    subscribed = set(self._desired_tokens)
                    if subscribed:
                        await ws.send_json(_subscription_payload(sorted(subscribed)))
                    ping_task = asyncio.create_task(_send_market_channel_pings(ws))
                    subscription_task = asyncio.create_task(self._send_subscriptions(ws, subscribed))
                    try:
                        async for message in ws:
                            if message.type != aiohttp.WSMsgType.TEXT:
                                continue
                            if message.data == "PONG":
                                continue
                            try:
                                payload = _json_loads(message.data)
                            except (TypeError, ValueError):
                                continue
                            self._handle_ws_payload(payload)
                    finally:
                        ping_task.cancel()
                        subscription_task.cancel()
                        await asyncio.gather(ping_task, subscription_task, return_exceptions=True)
            except asyncio.CancelledError:
                raise
            except Exception:
                LOGGER.exception("polymarket_ws_failed", extra={"_ws_url": ws_url})
                await asyncio.sleep(1.0)

    async def _send_subscriptions(self, ws: Any, subscribed: set[str]) -> None:
        while True:
            token_id = await self._subscription_queue.get()
            if token_id in subscribed:
                continue
            await ws.send_json(_subscription_payload([token_id]))
            subscribed.add(token_id)

    def _handle_ws_payload(self, payload: Any) -> None:
        for item in _iter_payload_items(payload):
            item_token = _asset_id(item)
            book = _order_book_from_payload(item)
            if book is not None and item_token in self._desired_tokens:
                self._update_book(item_token, book)
                continue

            changes = item.get("changes") or item.get("price_changes") or item.get("priceChanges")
            if not isinstance(changes, list):
                continue
            tokens = {token for change in changes if isinstance(change, dict) and (token := _asset_id(change))}
            if item_token:
                tokens.add(item_token)
            for token_id in tokens & self._desired_tokens:
                if token_id in self._books:
                    self._update_book(token_id, _apply_price_changes(self._books[token_id], changes, token_id))

    def _update_book(self, token_id: str, book: OrderBook) -> None:
        self._books[token_id] = replace(book, timestamp=min(book.timestamp, time.time()))
        self._book_timestamps[token_id] = time.monotonic()
        self._book_events.setdefault(token_id, asyncio.Event()).set()

    def market_data_age_seconds(self) -> float | None:
        if not self._desired_tokens:
            return None
        now = time.monotonic()
        return max(now - self._book_timestamps.get(token_id, 0.0) for token_id in self._desired_tokens)

    async def reconnect_market_data(self) -> None:
        task = self._ws_task
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        self._ws_task = None
        if self._desired_tokens:
            self._ws_task = asyncio.create_task(self._run_order_book_ws())

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
        if not self._config.private_key:
            raise RuntimeError("POLYMARKET_PRIVATE_KEY is required for production orders")
        order_id = await asyncio.to_thread(
            self._post_limit_order,
            token_id,
            "BUY",
            contracts,
            max_price,
            condition_id,
            tick_size,
            neg_risk,
        )
        self._order_amounts[order_id] = contracts
        self._order_prices[order_id] = max_price
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
        if not self._config.private_key:
            raise RuntimeError("POLYMARKET_PRIVATE_KEY is required for production exits")
        order_id = await asyncio.to_thread(
            self._post_limit_order,
            token_id,
            "SELL",
            contracts,
            min_price,
            condition_id,
            tick_size,
            neg_risk,
        )
        self._order_amounts[order_id] = contracts
        self._order_prices[order_id] = min_price
        return order_id

    async def wait_filled(self, order_id: str, timeout_ms: int) -> ExecutionReport:
        deadline = asyncio.get_running_loop().time() + timeout_ms / 1000
        requested = self._order_amounts.get(order_id, 0.0)
        last_filled = 0.0
        last_status = "pending"
        last_avg_price = self._order_prices.get(order_id, 0.0)
        while asyncio.get_running_loop().time() < deadline:
            payload = await asyncio.to_thread(self._get_order_payload, order_id)
            status = str(_extract_first(payload, ("status", "state", "orderStatus")) or "")
            last_status = status or last_status
            parsed_filled = _extract_filled_amount(payload)
            if parsed_filled is not None:
                last_filled = max(last_filled, parsed_filled)
            parsed_avg_price = _extract_avg_price(payload)
            if parsed_avg_price is not None:
                last_avg_price = parsed_avg_price
            if status in {"FILLED", "filled", "MATCHED", "matched"}:
                return ExecutionReport.from_amounts(
                    order_id, requested, parsed_filled or requested, status, last_avg_price
                )
            if status in {"CANCELED", "cancelled", "CANCELLED", "EXPIRED", "expired"}:
                return ExecutionReport.from_amounts(order_id, requested, last_filled, status, last_avg_price)
            await asyncio.sleep(0.1)
        return ExecutionReport.from_amounts(order_id, requested, last_filled, last_status, last_avg_price)

    async def cancel_order(self, order_id: str) -> None:
        await asyncio.to_thread(self._cancel_order, order_id)

    async def get_cash_balance(self) -> float:
        return await asyncio.to_thread(self._get_cash_balance)

    def forget_order(self, order_id: str) -> None:
        self._order_amounts.pop(order_id, None)
        self._order_prices.pop(order_id, None)

    def _get_sdk_client(self) -> Any:
        with self._sdk_client_lock:
            if self._sdk_client is not None:
                return self._sdk_client
            try:
                from py_clob_client_v2 import ClobClient  # type: ignore[import-untyped]
            except ImportError as exc:
                raise RuntimeError("py-clob-client-v2 is required for Polymarket production trading") from exc

            temp_client = ClobClient(
                self._config.api_base_url,
                key=self._config.private_key,
                chain_id=self._config.chain_id,
            )
            creds = temp_client.create_or_derive_api_key()
            self._sdk_client = ClobClient(
                self._config.api_base_url,
                key=self._config.private_key,
                chain_id=self._config.chain_id,
                creds=creds,
                signature_type=self._config.signature_type,
                funder=self._config.funder,
            )
            return self._sdk_client

    def _post_limit_order(
        self,
        token_id: str,
        side_name: str,
        size: float,
        price: float,
        condition_id: str | None,
        tick_size: str | None,
        neg_risk: bool | None,
    ) -> str:
        try:
            from py_clob_client_v2 import OrderArgs, OrderType, PartialCreateOrderOptions
            from py_clob_client_v2.order_builder.constants import BUY, SELL  # type: ignore[import-untyped]
        except ImportError as exc:
            raise RuntimeError("py-clob-client-v2 is required for Polymarket production trading") from exc

        client = self._get_sdk_client()
        order_tick_size, order_neg_risk = self._resolve_order_options(client, condition_id, tick_size, neg_risk)
        normalized_price = float(
            quantize_down(price, order_tick_size) if side_name == "BUY" else quantize_up(price, order_tick_size)
        )
        side = BUY if side_name == "BUY" else SELL
        response = client.create_and_post_order(
            OrderArgs(token_id=token_id, price=normalized_price, size=size, side=side),
            options=PartialCreateOrderOptions(tick_size=order_tick_size, neg_risk=order_neg_risk),
            order_type=OrderType.FOK,
        )
        order_id = _extract_first(response, ("orderID", "order_id", "id", "hash"))
        if not order_id:
            raise RuntimeError(f"Polymarket order response did not include an order id: {response!r}")
        return str(order_id)

    def _resolve_order_options(
        self,
        client: Any,
        condition_id: str | None,
        tick_size: str | None,
        neg_risk: bool | None,
    ) -> tuple[str, bool]:
        if tick_size is not None and neg_risk is not None:
            return tick_size, neg_risk
        if not condition_id:
            raise RuntimeError("condition_id or explicit tick_size/neg_risk is required for Polymarket orders")
        market = client.get_market(condition_id)
        resolved_tick_size = tick_size or str(market["minimum_tick_size"])
        resolved_neg_risk = neg_risk if neg_risk is not None else bool(market["neg_risk"])
        return resolved_tick_size, resolved_neg_risk

    def _get_order_status(self, order_id: str) -> str:
        order = self._get_order_payload(order_id)
        return str(_extract_first(order, ("status", "state", "orderStatus")) or "")

    def _get_order_payload(self, order_id: str) -> dict[str, Any]:
        client = self._get_sdk_client()
        order = client.get_order(order_id)
        if not isinstance(order, dict):
            raise RuntimeError(f"Polymarket returned unsupported order payload: {order!r}")
        return order

    def _cancel_order(self, order_id: str) -> None:
        try:
            from py_clob_client_v2 import OrderPayload
        except ImportError as exc:
            raise RuntimeError("py-clob-client-v2 is required for Polymarket production trading") from exc

        client = self._get_sdk_client()
        client.cancel_order(OrderPayload(orderID=order_id))

    def _get_cash_balance(self) -> float:
        try:
            from py_clob_client_v2 import AssetType, BalanceAllowanceParams
        except ImportError as exc:
            raise RuntimeError("py-clob-client-v2 is required for Polymarket production trading") from exc

        client = self._get_sdk_client()
        result = client.get_balance_allowance(
            BalanceAllowanceParams(
                asset_type=AssetType.COLLATERAL,
                signature_type=self._config.signature_type,
            )
        )
        balance = _find_numeric_balance(result, ("pusd", "pUSD", "USDC", "cash", "balance", "available"))
        if balance is None:
            raise RuntimeError(f"Could not parse Polymarket collateral balance from response: {result!r}")
        return balance


def _extract_first(payload: Any, keys: tuple[str, ...]) -> Any:
    if isinstance(payload, dict):
        for key in keys:
            if key in payload:
                return payload[key]
    return None


def _clob_ws_url(api_base_url: str) -> str:
    del api_base_url
    return "wss://ws-subscriptions-clob.polymarket.com/ws/market"


def _subscription_payload(token_ids: list[str]) -> dict[str, Any]:
    return {
        "assets_ids": token_ids,
        "type": "market",
        "custom_feature_enabled": True,
    }


def _asset_id(payload: dict[str, Any]) -> str:
    value = payload.get("asset_id") or payload.get("assetId") or payload.get("token_id") or payload.get("tokenId")
    return str(value) if value not in (None, "") else ""


def _json_loads(payload: str | bytes) -> Any:
    try:
        import orjson  # type: ignore[import-not-found]
    except ImportError:
        import json

        return json.loads(payload)
    return orjson.loads(payload)


async def _send_market_channel_pings(ws: Any) -> None:
    while True:
        await asyncio.sleep(10)
        await ws.send_str("PING")


def _iter_payload_items(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, dict):
        data = payload.get("data")
        if isinstance(data, list):
            return [item for item in data if isinstance(item, dict)]
        if isinstance(data, dict):
            return [data]
        return [payload]
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    return []


def _order_book_from_payload(payload: dict[str, Any]) -> OrderBook | None:
    raw_bids = payload.get("bids") or payload.get("buys")
    raw_asks = payload.get("asks") or payload.get("sells")
    if not isinstance(raw_bids, list) or not isinstance(raw_asks, list):
        return None
    bids = [_level_from_payload(item) for item in raw_bids]
    asks = [_level_from_payload(item) for item in raw_asks]
    return OrderBook(
        bids=_sorted_bids([level for level in bids if level is not None])[:10],
        asks=_sorted_asks([level for level in asks if level is not None])[:10],
        raw_payload=payload,
        timestamp=event_timestamp(payload),
    )


def _apply_price_changes(book: OrderBook, changes: list[Any], token_id: str | None = None) -> OrderBook:
    bids = {level.price: level.size for level in book.bids}
    asks = {level.price: level.size for level in book.asks}
    for raw_change in changes:
        if not isinstance(raw_change, dict):
            continue
        change_token = raw_change.get("asset_id") or raw_change.get("assetId") or raw_change.get("token_id")
        if token_id is not None and change_token is not None and str(change_token) != token_id:
            continue
        level = _level_from_payload(raw_change)
        if level is None:
            continue
        side = str(raw_change.get("side") or raw_change.get("book_side") or "").upper()
        target = bids if side in {"BUY", "BID", "BIDS"} else asks if side in {"SELL", "ASK", "ASKS"} else None
        if target is None:
            continue
        if level.size <= 0:
            target.pop(level.price, None)
        else:
            target[level.price] = level.size
    return OrderBook(
        bids=_sorted_bids([OrderBookLevel(price, size) for price, size in bids.items()])[:10],
        asks=_sorted_asks([OrderBookLevel(price, size) for price, size in asks.items()])[:10],
        raw_payload={"changes": changes},
    )


def _level_from_payload(payload: Any) -> OrderBookLevel | None:
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
    try:
        return OrderBookLevel(float(price), float(size))
    except (TypeError, ValueError):
        return None


def _sorted_bids(levels: list[OrderBookLevel]) -> list[OrderBookLevel]:
    return sorted(levels, key=lambda level: level.price, reverse=True)


def _sorted_asks(levels: list[OrderBookLevel]) -> list[OrderBookLevel]:
    return sorted(levels, key=lambda level: level.price)


def _find_numeric_balance(payload: Any, keys: tuple[str, ...]) -> float | None:
    if isinstance(payload, (int, float, str)):
        try:
            return float(payload)
        except ValueError:
            return None
    if isinstance(payload, dict):
        for key in keys:
            if key in payload:
                nested = _find_numeric_balance(payload[key], keys)
                if nested is not None:
                    return nested
        for value in payload.values():
            nested = _find_numeric_balance(value, keys)
            if nested is not None:
                return nested
    if isinstance(payload, list):
        for item in payload:
            nested = _find_numeric_balance(item, keys)
            if nested is not None:
                return nested
    return None


def _extract_filled_amount(payload: dict[str, Any]) -> float | None:
    value = _extract_first(
        payload,
        ("size_matched", "sizeMatched", "filledAmount", "filled_amount", "amountFilled", "executedAmount"),
    )
    if value in (None, ""):
        return None
    try:
        return float(str(value))
    except (TypeError, ValueError):
        return None


def _extract_avg_price(payload: dict[str, Any]) -> float | None:
    value = _extract_first(payload, ("avg_price", "average_price", "avgPrice", "averagePrice"))
    if value in (None, ""):
        return None
    try:
        return float(str(value))
    except (TypeError, ValueError):
        return None
