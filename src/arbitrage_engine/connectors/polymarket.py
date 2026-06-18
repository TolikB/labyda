from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

from arbitrage_engine.config import PolymarketConfig
from arbitrage_engine.connectors.base import PolymarketClient
from arbitrage_engine.models import BinarySide, OrderBook, OrderBookLevel

LOGGER = logging.getLogger(__name__)


class PolymarketClobClient(PolymarketClient):
    def __init__(self, config: PolymarketConfig) -> None:
        self._config = config
        self._sdk_client: Any | None = None
        self._books: dict[str, OrderBook] = {}
        self._book_timestamps: dict[str, float] = {}
        self._book_events: dict[str, asyncio.Event] = {}
        self._ws_tasks: dict[str, asyncio.Task[None]] = {}

    async def watch_order_book(self, token_id: str) -> OrderBook:
        if token_id in self._books and time.monotonic() - self._book_timestamps.get(token_id, 0.0) <= 2.0:
            return self._books[token_id]

        self._ensure_ws_task(token_id)
        event = self._book_events[token_id]
        event.clear()
        try:
            await asyncio.wait_for(event.wait(), timeout=0.5 if token_id in self._books else 2.0)
            return self._books[token_id]
        except asyncio.TimeoutError:
            LOGGER.warning("polymarket_ws_snapshot_timeout", extra={"_token_id": token_id})
            return await self._fetch_order_book_http(token_id)

    async def _fetch_order_book_http(self, token_id: str) -> OrderBook:
        try:
            import aiohttp
        except ImportError as exc:
            raise RuntimeError("aiohttp is required for Polymarket connectivity") from exc

        url = f"{self._config.api_base_url}/book"
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params={"token_id": token_id}, timeout=10) as response:
                response.raise_for_status()
                raw: dict[str, Any] = await response.json()
        bids = [OrderBookLevel(float(item["price"]), float(item["size"])) for item in raw.get("bids", [])[:10]]
        asks = [OrderBookLevel(float(item["price"]), float(item["size"])) for item in raw.get("asks", [])[:10]]
        book = OrderBook(bids=_sorted_bids(bids), asks=_sorted_asks(asks))
        self._books[token_id] = book
        self._book_timestamps[token_id] = time.monotonic()
        self._book_events.setdefault(token_id, asyncio.Event()).set()
        return book

    def _ensure_ws_task(self, token_id: str) -> None:
        self._book_events.setdefault(token_id, asyncio.Event())
        task = self._ws_tasks.get(token_id)
        if task is None or task.done():
            self._ws_tasks[token_id] = asyncio.create_task(self._run_order_book_ws(token_id))

    async def _run_order_book_ws(self, token_id: str) -> None:
        try:
            import aiohttp
        except ImportError as exc:
            LOGGER.warning("polymarket_ws_unavailable", extra={"_error": str(exc)})
            return

        ws_url = _clob_ws_url(self._config.api_base_url)
        subscribe_payload = {
            "assets_ids": [token_id],
            "type": "market",
            "custom_feature_enabled": True,
        }
        while True:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(ws_url, heartbeat=10) as ws:
                        await ws.send_json(subscribe_payload)
                        ping_task = asyncio.create_task(_send_market_channel_pings(ws))
                        try:
                            async for message in ws:
                                if message.type != aiohttp.WSMsgType.TEXT:
                                    continue
                                if message.data == "PONG":
                                    continue
                                try:
                                    payload = message.json()
                                except ValueError:
                                    continue
                                self._handle_ws_payload(token_id, payload)
                        finally:
                            ping_task.cancel()
            except asyncio.CancelledError:
                raise
            except Exception:
                LOGGER.exception("polymarket_ws_failed", extra={"_token_id": token_id, "_ws_url": ws_url})
                await asyncio.sleep(1.0)

    def _handle_ws_payload(self, token_id: str, payload: Any) -> None:
        for item in _iter_payload_items(payload):
            item_token = str(
                item.get("asset_id")
                or item.get("assetId")
                or item.get("token_id")
                or item.get("tokenId")
                or token_id
            )
            if item_token != token_id:
                continue

            book = _order_book_from_payload(item)
            if book is not None:
                self._books[token_id] = book
                self._book_timestamps[token_id] = time.monotonic()
                self._book_events[token_id].set()
                continue

            changes = item.get("changes") or item.get("price_changes") or item.get("priceChanges")
            if changes and token_id in self._books:
                self._books[token_id] = _apply_price_changes(self._books[token_id], changes, token_id)
                self._book_timestamps[token_id] = time.monotonic()
                self._book_events[token_id].set()

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
        return order_id

    async def wait_filled(self, order_id: str, timeout_ms: int) -> bool:
        deadline = asyncio.get_running_loop().time() + timeout_ms / 1000
        while asyncio.get_running_loop().time() < deadline:
            status = await asyncio.to_thread(self._get_order_status, order_id)
            if status in {"FILLED", "filled", "MATCHED", "matched"}:
                return True
            if status in {"CANCELED", "cancelled", "CANCELLED", "EXPIRED", "expired"}:
                return False
            await asyncio.sleep(0.1)
        return False

    async def cancel_order(self, order_id: str) -> None:
        await asyncio.to_thread(self._cancel_order, order_id)

    async def get_cash_balance(self) -> float:
        return await asyncio.to_thread(self._get_cash_balance)

    def _get_sdk_client(self) -> Any:
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
        side = BUY if side_name == "BUY" else SELL
        response = client.create_and_post_order(
            OrderArgs(token_id=token_id, price=price, size=size, side=side),
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
        client = self._get_sdk_client()
        order = client.get_order(order_id)
        return str(_extract_first(order, ("status", "state", "orderStatus")) or "")

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
    )


def _level_from_payload(payload: Any) -> OrderBookLevel | None:
    if isinstance(payload, dict):
        price = payload.get("price")
        size = payload.get("size") or payload.get("quantity")
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
