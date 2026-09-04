from __future__ import annotations

import asyncio
import hashlib
import logging
import threading
import time
from collections.abc import Callable
from dataclasses import replace
from datetime import UTC, datetime
from decimal import ROUND_CEILING, Decimal
from typing import Any

from arbitrage_engine.conditional_tokens import ConditionalTokensRedemption, SafeConditionalTokensRedemption
from arbitrage_engine.config import PolymarketConfig
from arbitrage_engine.connectors.base import (
    OrderBookStaleException,
    OrderBookUnavailableException,
    PolymarketClient,
    WebSocketReconnectBackoff,
    event_checksum,
    event_sequence,
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
    RedemptionReport,
    SettlementRequest,
    SettlementStatus,
    VenueFeeQuote,
    VenueOrder,
)
from arbitrage_engine.utils.math import quantize_down, quantize_up

LOGGER = logging.getLogger(__name__)
ORDER_BOOK_MAX_AGE_SECONDS = 0.3
PASSIVE_BOOK_MAX_AGE_SECONDS = 2.0
_WS_SNAPSHOT_PRIME_TIMEOUT_SECONDS = 0.5
_TARGET_TRANSITION_GRACE_SECONDS = _WS_SNAPSHOT_PRIME_TIMEOUT_SECONDS + 0.25
_BOOK_REST_REQUEST_INTERVAL_SECONDS = 0.05
_BOOK_REST_INITIAL_RATE_LIMIT_BACKOFF_SECONDS = 2.0
_BOOK_REST_MAX_RATE_LIMIT_BACKOFF_SECONDS = 30.0
_MARKET_INFO_CACHE_TTL_SECONDS = 300.0
_MARKET_INFO_MIN_INTERVAL_SECONDS = 0.2
_MARKET_INFO_RATE_LIMIT_COOLDOWN_SECONDS = 30.0
_POSITIONS_API_URL = "https://data-api.polymarket.com/positions"
_POSITIONS_PAGE_LIMIT = 500
_POSITIONS_MAX_OFFSET = 10_000
_MAX_UINT256 = (1 << 256) - 1
_SDK_TICK_CONFIG_LOCK = threading.Lock()


class PolymarketClobClient(PolymarketClient):
    venue_name = "Polymarket"

    def __init__(self, config: PolymarketConfig) -> None:
        self._config = config
        self._sdk_client: Any | None = None
        self._sdk_client_lock = threading.Lock()
        # py-clob-client-v2 keeps a mutable synchronous HTTP/2 client. Several
        # asyncio.to_thread callers may otherwise corrupt its stream state.
        self._sdk_call_lock = threading.RLock()
        self._sdk_client_forced_derived_creds = False
        self._sdk_client_uses_configured_creds = False
        self._books: dict[str, OrderBook] = {}
        self._book_timestamps: dict[str, float] = {}
        self._snapshot_timestamps: dict[str, float] = {}
        self._book_events: dict[str, asyncio.Event] = {}
        self._ws_task: asyncio.Task[None] | None = None
        self._ws_session: Any | None = None
        self._ws: Any | None = None
        self._reconnect_lock = asyncio.Lock()
        self._reconnecting = False
        self._ws_connected = False
        self._reconnect_backoff = WebSocketReconnectBackoff()
        self._desired_tokens: set[str] = set()
        self._subscription_queue: asyncio.Queue[tuple[str, str]] = asyncio.Queue()
        self._bootstrap_tasks: dict[str, asyncio.Task[OrderBook]] = {}
        self._bootstrap_attempted: set[str] = set()
        self._order_amounts: dict[str, float] = {}
        self._order_prices: dict[str, float] = {}
        self._rest_session: Any | None = None
        self._http_semaphore = asyncio.Semaphore(20)
        self._book_rest_rate_lock = asyncio.Lock()
        self._last_book_rest_request_at = 0.0
        self._book_rest_cooldown_until = 0.0
        self._book_rest_last_rate_limit_at = 0.0
        self._book_rest_backoff_seconds = _BOOK_REST_INITIAL_RATE_LIMIT_BACKOFF_SECONDS
        self._book_rest_rate_limit_count = 0
        self._constraints_cache: dict[str, tuple[float, MarketConstraints]] = {}
        self._constraints_locks: dict[str, asyncio.Lock] = {}
        self._market_token_ids_by_condition: dict[str, frozenset[str]] = {}
        self._market_options_cache: dict[str, tuple[str, bool]] = {}
        self._last_market_info_request_at = 0.0
        self._market_info_cooldown_until = 0.0
        self._snapshot_interval_seconds = 30.0
        self._execution_freshness_seconds = PASSIVE_BOOK_MAX_AGE_SECONDS
        self._reconnect_count = 0
        self._sequence_gap_count = 0
        self._snapshot_timeout_count = 0
        self._stale_refresh_attempted_at: dict[str, float] = {}
        self._market_data_ever_ready = False
        self._target_transition_deadline = 0.0
        self._settlement: ConditionalTokensRedemption | None = None
        self._safe_settlement: SafeConditionalTokensRedemption | None = None

    async def watch_order_book(self, token_id: str) -> OrderBook:
        self._register_token(token_id)
        cached = self._books.get(token_id)
        if cached is not None and (
            cached.status in {MarketDataStatus.INVALID, MarketDataStatus.STALE}
            or (
                cached.sequence is None
                and time.monotonic() - self._snapshot_timestamps.get(token_id, 0.0) >= self._snapshot_interval_seconds
            )
        ):
            task, _ = self._ensure_refresh_task(token_id, force=True)
            if task is None:
                raise OrderBookStaleException(f"Polymarket order book refresh is cooling down for token {token_id}")
            return await self._await_refresh_task(token_id, task)
        if (
            token_id in self._books
            and time.monotonic() - self._book_timestamps.get(token_id, 0.0) <= ORDER_BOOK_MAX_AGE_SECONDS
        ):
            return self._books[token_id]
        if self._cached_book_is_passively_fresh(token_id):
            return self._execution_book_from_cache(token_id)

        if token_id not in self._books:
            task, _ = self._ensure_refresh_task(token_id, force=False)
            if task is None:
                raise OrderBookStaleException(f"Polymarket order book bootstrap unavailable for token {token_id}")
            return await self._await_refresh_task(token_id, task)

        task, _ = self._ensure_refresh_task(token_id, force=True)
        if task is not None:
            return await self._await_refresh_task(token_id, task)
        raise OrderBookStaleException(f"Polymarket order book is stale for token {token_id}")

    async def _fetch_order_book_http(self, token_id: str) -> OrderBook:
        try:
            import aiohttp

            _ = aiohttp
        except ImportError as exc:
            raise RuntimeError("aiohttp is required for Polymarket connectivity") from exc

        url = f"{self._config.api_base_url}/book"
        session = self._get_rest_session()
        await self._wait_for_book_rest_slot()
        try:
            async with self._http_semaphore:
                async with session.get(url, params={"token_id": token_id}, timeout=10) as response:
                    if getattr(response, "status", None) == 404:
                        raise OrderBookUnavailableException(
                            f"Polymarket has no CLOB order book for token {token_id}"
                        )
                    response.raise_for_status()
                    raw: dict[str, Any] = await response.json()
        except Exception as exc:
            if _is_rate_limit_error(exc):
                await self._record_book_rest_rate_limit()
            raise
        bids = [OrderBookLevel(float(item["price"]), float(item["size"])) for item in raw.get("bids", [])]
        asks = [OrderBookLevel(float(item["price"]), float(item["size"])) for item in raw.get("asks", [])]
        book = OrderBook(bids=_sorted_bids(bids)[:10], asks=_sorted_asks(asks)[:10], raw_payload=raw)
        self._update_book(token_id, book)
        self._snapshot_timestamps[token_id] = time.monotonic()
        return book

    async def _wait_for_book_rest_slot(self) -> None:
        async with self._book_rest_rate_lock:
            now = time.monotonic()
            delay = max(
                0.0,
                self._book_rest_cooldown_until - now,
                _BOOK_REST_REQUEST_INTERVAL_SECONDS - (now - self._last_book_rest_request_at),
            )
            if delay > 0:
                await asyncio.sleep(delay)
            self._last_book_rest_request_at = time.monotonic()

    async def _record_book_rest_rate_limit(self) -> None:
        async with self._book_rest_rate_lock:
            now = time.monotonic()
            if now - self._book_rest_last_rate_limit_at > 60.0:
                self._book_rest_backoff_seconds = _BOOK_REST_INITIAL_RATE_LIMIT_BACKOFF_SECONDS
            else:
                self._book_rest_backoff_seconds = min(
                    _BOOK_REST_MAX_RATE_LIMIT_BACKOFF_SECONDS,
                    self._book_rest_backoff_seconds * 2,
                )
            self._book_rest_last_rate_limit_at = now
            self._book_rest_cooldown_until = max(
                self._book_rest_cooldown_until,
                now + self._book_rest_backoff_seconds,
            )
            self._book_rest_rate_limit_count += 1

    def _ensure_refresh_task(self, token_id: str, *, force: bool) -> tuple[asyncio.Task[OrderBook] | None, bool]:
        task = self._bootstrap_tasks.get(token_id)
        if task is not None and not task.done():
            return task, False
        now = time.monotonic()
        cooldown_seconds = self._execution_freshness_seconds
        if force and now - self._stale_refresh_attempted_at.get(token_id, 0.0) < cooldown_seconds:
            return None, False
        if not force and token_id in self._bootstrap_attempted:
            return None, False
        if not force:
            self._bootstrap_attempted.add(token_id)
        self._stale_refresh_attempted_at[token_id] = now
        task = asyncio.create_task(self._fetch_order_book_http(token_id))
        self._bootstrap_tasks[token_id] = task
        return task, True

    async def _await_refresh_task(self, token_id: str, task: asyncio.Task[OrderBook]) -> OrderBook:
        try:
            return await asyncio.shield(task)
        except OrderBookUnavailableException:
            # A confirmed 404 cannot recover through repeated REST bootstraps.
            # Target rotation clears this marker if the token becomes eligible again.
            raise
        except Exception:
            self._bootstrap_attempted.discard(token_id)
            raise
        finally:
            if self._bootstrap_tasks.get(token_id) is task and task.done():
                self._bootstrap_tasks.pop(token_id, None)

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
        await self._close_ws_session()
        if self._settlement is not None:
            await self._settlement.web3_client.close()
        self._settlement = None
        self._safe_settlement = None

    def _register_token(self, token_id: str) -> None:
        self._book_events.setdefault(token_id, asyncio.Event())
        if token_id not in self._desired_tokens:
            self._desired_tokens.add(token_id)
            self._subscription_queue.put_nowait(("subscribe", token_id))
        if self._ws_task is None or self._ws_task.done():
            self._ws_task = asyncio.create_task(self._run_order_book_ws())

    def sync_market_data_targets(self, token_ids: set[str]) -> None:
        normalized = {token_id for token_id in token_ids if token_id}
        removed = self._desired_tokens - normalized
        added = normalized - self._desired_tokens
        self._desired_tokens = set(normalized)
        for token_id in removed:
            self._prune_token(token_id)
            self._subscription_queue.put_nowait(("unsubscribe", token_id))
        for token_id in added:
            self._book_events.setdefault(token_id, asyncio.Event())
            self._subscription_queue.put_nowait(("subscribe", token_id))
        if added and self._ws_connected and self._market_data_ever_ready:
            self._target_transition_deadline = time.monotonic() + _TARGET_TRANSITION_GRACE_SECONDS
        elif not self._desired_tokens:
            self._target_transition_deadline = 0.0
        if self._desired_tokens and (self._ws_task is None or self._ws_task.done()):
            self._ws_task = asyncio.create_task(self._run_order_book_ws())

    async def prime_market_data_targets(self) -> None:
        if not self._ws_connected:
            return
        waiters = [
            asyncio.create_task(self._book_events.setdefault(token_id, asyncio.Event()).wait())
            for token_id in self._desired_tokens
            if token_id not in self._books
        ]
        if not waiters:
            return
        _, pending = await asyncio.wait(waiters, timeout=_WS_SNAPSHOT_PRIME_TIMEOUT_SECONDS)
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)

    async def _run_order_book_ws(self) -> None:
        try:
            import aiohttp
        except ImportError as exc:
            LOGGER.warning("polymarket_ws_unavailable", extra={"_error": str(exc)})
            return

        ws_url = _clob_ws_url(self._config.api_base_url)
        while True:
            connected_at: float | None = None
            try:
                session = self._get_ws_session()
                async with session.ws_connect(ws_url, heartbeat=10) as ws:
                    self._ws = ws
                    connected_at = time.monotonic()
                    self._ws_connected = True
                    self._reconnecting = False
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
            finally:
                self._ws_connected = False
                self._reconnecting = True
                self._mark_books_stale()
                ws = self._ws
                self._ws = None
                if ws is not None and not ws.closed:
                    await ws.close()
                await self._close_ws_session()
            if connected_at is not None and time.monotonic() - connected_at >= 60.0:
                self._reconnect_backoff.reset()
            self._reconnect_count += 1
            await asyncio.sleep(self._reconnect_backoff.next_delay())

    async def _send_subscriptions(self, ws: Any, subscribed: set[str]) -> None:
        while True:
            operation, token_id = await self._subscription_queue.get()
            if operation == "subscribe":
                if token_id in subscribed or token_id not in self._desired_tokens:
                    continue
                await ws.send_json(_subscription_payload([token_id], operation="subscribe"))
                subscribed.add(token_id)
                continue
            if token_id not in subscribed:
                continue
            await ws.send_json(_subscription_payload([token_id], operation="unsubscribe"))
            subscribed.discard(token_id)

    def _handle_ws_payload(self, payload: Any) -> None:
        for item in _iter_payload_items(payload):
            item_token = _asset_id(item)
            book = _order_book_from_payload(item)
            if book is not None and item_token in self._desired_tokens:
                self._update_book(item_token, book)
                self._snapshot_timestamps[item_token] = time.monotonic()
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
        if book.status is MarketDataStatus.INVALID:
            self._sequence_gap_count += 1
        self._books[token_id] = replace(book, timestamp=min(book.timestamp, time.time()))
        self._book_timestamps[token_id] = time.monotonic()
        self._book_events.setdefault(token_id, asyncio.Event()).set()
        if self._desired_tokens and all(
            desired_token in self._books
            and self._books[desired_token].status is MarketDataStatus.VALID
            for desired_token in self._desired_tokens
        ):
            self._market_data_ever_ready = True
            self._target_transition_deadline = 0.0

    def market_data_age_seconds(self) -> float | None:
        active_tokens = self._active_tokens()
        if not active_tokens:
            return None
        timestamps = [
            self._book_timestamps[token_id]
            for token_id in active_tokens
            if token_id in self._book_timestamps
        ]
        if not timestamps:
            return None
        now = time.monotonic()
        return now - max(timestamps)

    def set_market_data_snapshot_interval(self, seconds: float) -> None:
        self._snapshot_interval_seconds = seconds

    def set_market_data_execution_freshness(self, seconds: float) -> None:
        self._execution_freshness_seconds = max(ORDER_BOOK_MAX_AGE_SECONDS, seconds)

    def market_data_ready(self) -> bool:
        active_tokens = self._active_tokens()
        return self._ws_connected and bool(active_tokens) and all(
            token_id in self._books and self._books[token_id].status is MarketDataStatus.VALID
            for token_id in active_tokens
        )

    def market_data_transitioning(self) -> bool:
        if (
            not self._market_data_ever_ready
            or not self._ws_connected
            or not self._desired_tokens
            or time.monotonic() > self._target_transition_deadline
        ):
            return False
        return any(
            token_id not in self._books or self._books[token_id].status is not MarketDataStatus.VALID
            for token_id in self._desired_tokens
        )

    def has_active_market_data_targets(self) -> bool:
        return bool(self._desired_tokens)

    def active_market_data_target_count(self) -> int:
        return len(self._desired_tokens)

    async def reconnect_market_data(self) -> None:
        async with self._reconnect_lock:
            if self._reconnecting:
                return
            self._reconnecting = True
            self._ws_connected = False
            self._mark_books_stale()
            if self._ws is not None and not self._ws.closed:
                await self._ws.close()
            if self._desired_tokens and (self._ws_task is None or self._ws_task.done()):
                self._ws_task = asyncio.create_task(self._run_order_book_ws())

    def _mark_books_stale(self) -> None:
        for token_id in self._active_tokens() & self._books.keys():
            self._books[token_id] = replace(self._books[token_id], status=MarketDataStatus.STALE)

    async def _close_ws_session(self) -> None:
        if self._ws_session is not None and not self._ws_session.closed:
            await self._ws_session.close()
        self._ws_session = None

    def _prune_token(self, token_id: str) -> None:
        task = self._bootstrap_tasks.pop(token_id, None)
        if task is not None:
            task.cancel()
        self._bootstrap_attempted.discard(token_id)
        self._stale_refresh_attempted_at.pop(token_id, None)
        self._books.pop(token_id, None)
        self._book_timestamps.pop(token_id, None)
        self._snapshot_timestamps.pop(token_id, None)
        self._book_events.pop(token_id, None)

    def _active_tokens(self) -> set[str]:
        return set(self._desired_tokens)

    def _cached_book_is_passively_fresh(self, token_id: str) -> bool:
        book = self._books.get(token_id)
        if book is None or book.status is not MarketDataStatus.VALID:
            return False
        if self._ws_connected and token_id in self._desired_tokens:
            return True
        return max(0.0, time.time() - book.timestamp) <= self._execution_freshness_seconds

    def _execution_book_from_cache(self, token_id: str) -> OrderBook:
        book = self._books[token_id]
        if self._ws_connected and token_id in self._desired_tokens:
            # Stream continuity means no update was missed; a quiet book is still current.
            return replace(book, timestamp=time.time())
        return book

    def telemetry_snapshot(self) -> dict[str, float]:
        return {
            "reconnects": float(self._reconnect_count),
            "sequence_gaps": float(self._sequence_gap_count),
            "snapshot_timeouts": float(self._snapshot_timeout_count),
            "rest_rate_limits": float(self._book_rest_rate_limit_count),
            "rest_cooldown_seconds": max(0.0, self._book_rest_cooldown_until - time.monotonic()),
            "connected": float(self._ws_connected),
            "reconnecting": float(self._reconnecting),
            "reconnect_backoff_seconds": self._reconnect_backoff.current_delay_seconds,
        }

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

    async def get_order(self, order_id: str) -> ExecutionReport:
        payload = await asyncio.to_thread(self._get_order_payload, order_id)
        requested = self._order_amounts.get(order_id, _extract_requested_amount(payload) or 0.0)
        filled = _extract_filled_amount(payload) or 0.0
        status = str(_extract_first(payload, ("status", "state", "orderStatus")) or "open")
        price = _extract_avg_price(payload) or self._order_prices.get(order_id, 0.0)
        return ExecutionReport.from_amounts(order_id, requested, filled, status, price)

    async def list_open_orders(self) -> list[VenueOrder]:
        payloads = await asyncio.to_thread(self._sdk_call, lambda client: client.get_open_orders(None, True))
        return [_venue_order_from_payload(item) for item in payloads if isinstance(item, dict)]

    async def list_fills(self, since: datetime | None = None) -> list[FillRecord]:
        payloads = await asyncio.to_thread(self._sdk_call, lambda client: client.get_trades())
        fills = [_fill_from_trade(item) for item in payloads if isinstance(item, dict)]
        return [fill for fill in fills if since is None or fill.occurred_at >= since]

    async def get_positions(self) -> dict[str, Decimal]:
        profile_address = self._positions_profile_address()
        positions: dict[str, Decimal] = {}
        seen_token_ids: set[str] = set()
        offset = 0
        while True:
            page = await self._fetch_positions_page(profile_address, offset)
            for row_index, item in enumerate(page):
                token_id, size = _position_from_payload(item, profile_address, offset, row_index)
                if token_id in seen_token_ids:
                    raise RuntimeError(
                        f"Polymarket current positions contain duplicate asset at offset {offset}, row {row_index}"
                    )
                seen_token_ids.add(token_id)
                if size > 0:
                    positions[token_id] = size
            if len(page) < _POSITIONS_PAGE_LIMIT:
                break
            if offset >= _POSITIONS_MAX_OFFSET:
                raise RuntimeError("Polymarket current positions exceed the supported pagination window")
            offset += _POSITIONS_PAGE_LIMIT
        return positions

    def _positions_profile_address(self) -> str:
        address = (self._config.funder or "").strip()
        if not address:
            if not self._config.private_key:
                raise RuntimeError("Polymarket profile address is unavailable for position reconciliation")
            try:
                from eth_account import Account

                address = str(Account.from_key(self._config.private_key).address)
            except Exception:
                raise RuntimeError(
                    "Polymarket signer address could not be derived for position reconciliation"
                ) from None
        if not _is_evm_address(address):
            raise RuntimeError("Polymarket profile address is invalid for position reconciliation")
        return address

    async def _fetch_positions_page(self, profile_address: str, offset: int) -> list[Any]:
        params = {
            "user": profile_address,
            "sizeThreshold": "0",
            "includeArchived": "true",
            "limit": str(_POSITIONS_PAGE_LIMIT),
            "offset": str(offset),
            "sortBy": "TOKENS",
            "sortDirection": "ASC",
        }
        try:
            session = self._get_rest_session()
            async with self._http_semaphore:
                async with session.get(_POSITIONS_API_URL, params=params, timeout=10) as response:
                    response.raise_for_status()
                    payload: Any = await response.json()
        except Exception as exc:
            LOGGER.warning(
                "polymarket_current_positions_request_failed",
                extra={"_offset": offset, "_error_type": type(exc).__name__},
            )
            raise RuntimeError(f"Polymarket current positions request failed at offset {offset}") from None
        if not isinstance(payload, list):
            raise RuntimeError(f"Polymarket current positions response is invalid at offset {offset}")
        return payload

    def supports_full_reconciliation(self) -> bool:
        return True

    def supports_automatic_redemption(self) -> bool:
        return self._supports_direct_redemption() or self._supports_safe_redemption()

    async def get_settlement_status(self, request: SettlementRequest) -> SettlementStatus:
        return await self._get_execution_settlement_client().get_settlement_status(self._settlement_request(request))

    def prepare_settlement_request(self, request: SettlementRequest) -> SettlementRequest:
        if not self.supports_automatic_redemption():
            raise RuntimeError("Polymarket funder requires an unsupported automatic redemption topology")
        return self._settlement_request(request)

    async def redeem_position(self, request: SettlementRequest, redemption_id: str) -> RedemptionReport:
        return await self._get_execution_settlement_client().redeem_position(
            self._settlement_request(request),
            redemption_id,
        )

    async def reconcile_redemption(
        self,
        request: SettlementRequest,
        report: RedemptionReport,
    ) -> RedemptionReport:
        return await self._get_execution_settlement_client().reconcile(self._settlement_request(request), report)

    async def get_native_gas_balance(self) -> float:
        return await self._get_settlement_client().native_balance()

    def _get_settlement_client(self) -> ConditionalTokensRedemption:
        if self._settlement is None:
            web3 = BaseWeb3Client(
                rpc_url=self._config.rpc_urls or self._config.rpc_url,
                chain_id=self._config.chain_id,
                private_key=self._config.private_key,
                max_priority_fee_gwei=self._config.max_priority_fee_gwei,
                confirmations=self._config.confirmations,
            )
            self._settlement = ConditionalTokensRedemption(
                web3,
                self._config.conditional_tokens_address,
                self._config.redemption_gas_limit,
            )
        return self._settlement

    def _get_safe_settlement_client(self) -> SafeConditionalTokensRedemption:
        if not self._config.funder:
            raise RuntimeError("Polymarket Safe funder is not configured")
        if self._safe_settlement is None:
            settlement = self._get_settlement_client()
            self._safe_settlement = SafeConditionalTokensRedemption(
                settlement.web3_client,
                self._config.funder,
                self._config.conditional_tokens_address,
                self._config.redemption_gas_limit,
            )
        return self._safe_settlement

    def _get_execution_settlement_client(self) -> ConditionalTokensRedemption | SafeConditionalTokensRedemption:
        if self._supports_direct_redemption():
            return self._get_settlement_client()
        if self._supports_safe_redemption():
            return self._get_safe_settlement_client()
        return self._get_settlement_client()

    def _supports_direct_redemption(self) -> bool:
        if not self._config.funder:
            return True
        settlement = self._get_settlement_client()
        if settlement.signer_address is None:
            return True
        funder = settlement.checksum_address(self._config.funder)
        return funder == settlement.signer_address

    def _supports_safe_redemption(self) -> bool:
        return bool(self._config.private_key and self._config.funder and self._config.signature_type == 2)

    def _settlement_request(self, request: SettlementRequest) -> SettlementRequest:
        return replace(request, collateral_token=request.collateral_token or self._config.collateral_token_address)

    async def get_market_constraints(self, token_id: str, condition_id: str | None = None) -> MarketConstraints | None:
        if not condition_id:
            return None
        cache_key = f"{condition_id}:{token_id}"
        cached = self._constraints_cache.get(cache_key)
        if cached is not None and time.monotonic() - cached[0] < _MARKET_INFO_CACHE_TTL_SECONDS:
            return cached[1]
        lock = self._constraints_locks.setdefault(condition_id, asyncio.Lock())
        async with lock:
            cached = self._constraints_cache.get(cache_key)
            if cached is not None and time.monotonic() - cached[0] < _MARKET_INFO_CACHE_TTL_SECONDS:
                return cached[1]
            constraints = await asyncio.to_thread(self._get_market_constraints, token_id, condition_id)
            fetched_at = time.monotonic()
            market_tokens = self._market_token_ids_by_condition.get(condition_id, frozenset({token_id}))
            for market_token in market_tokens:
                self._constraints_cache[f"{condition_id}:{market_token}"] = (fetched_at, constraints)
            return constraints

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
            "Polymarket",
            resolved.fee_rate_bps,
            "polymarket_dynamic",
            source="polymarket_clob_market_info_v2",
            verified=True,
            fee_exponent=resolved.fee_exponent,
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
        resolved_tick_size = tick_size
        if resolved_tick_size is None:
            constraints = await self.get_market_constraints(token_id, condition_id)
            if constraints is not None:
                resolved_tick_size = str(constraints.tick_size)
        if resolved_tick_size is not None:
            resolved_tick_size = _sdk_compatible_tick_size(resolved_tick_size)
        normalized_max_price = max_price
        if resolved_tick_size is not None and 0 < max_price <= 1:
            normalized_max_price = _normalize_binary_order_price(
                max_price,
                resolved_tick_size,
                round_up=False,
            )
        return await super().preview_buy(
            token_id,
            side,
            contracts,
            normalized_max_price,
            condition_id=condition_id,
            tick_size=resolved_tick_size,
            neg_risk=neg_risk,
        )

    async def _preview_buy_signature(
        self,
        token_id: str,
        side: BinarySide,
        contracts: Decimal,
        max_price: Decimal,
        *,
        condition_id: str | None,
        tick_size: str | None,
        neg_risk: bool | None,
    ) -> str | None:
        del side
        if not self._config.private_key:
            return None
        return await asyncio.to_thread(
            self._create_limit_order_preview,
            token_id,
            float(contracts),
            float(max_price),
            condition_id,
            tick_size,
            neg_risk,
        )

    def forget_order(self, order_id: str) -> None:
        self._order_amounts.pop(order_id, None)
        self._order_prices.pop(order_id, None)

    def _get_sdk_client(self) -> Any:
        with self._sdk_client_lock:
            if self._sdk_client is not None:
                return self._sdk_client
            try:
                from py_clob_client_v2 import ClobClient  # type: ignore[import-untyped]
                from py_clob_client_v2.clob_types import ApiCreds  # type: ignore[import-untyped]
            except ImportError as exc:
                raise RuntimeError("py-clob-client-v2 is required for Polymarket production trading") from exc

            creds = None
            temp_client = ClobClient(
                self._config.api_base_url,
                key=self._config.private_key,
                chain_id=self._config.chain_id,
            )
            if not self._sdk_client_forced_derived_creds:
                try:
                    creds = temp_client.derive_api_key()
                    self._sdk_client_uses_configured_creds = False
                except Exception:
                    if (
                        self._config.api_key
                        and self._config.api_secret
                        and self._config.api_passphrase
                    ):
                        creds = ApiCreds(
                            api_key=self._config.api_key,
                            api_secret=self._config.api_secret,
                            api_passphrase=self._config.api_passphrase,
                        )
                        self._sdk_client_uses_configured_creds = True
                    else:
                        creds = temp_client.create_or_derive_api_key()
                        self._sdk_client_uses_configured_creds = False
            else:
                try:
                    creds = temp_client.derive_api_key()
                except Exception:
                    creds = temp_client.create_or_derive_api_key()
                self._sdk_client_uses_configured_creds = False
            self._sdk_client = ClobClient(
                self._config.api_base_url,
                key=self._config.private_key,
                chain_id=self._config.chain_id,
                creds=creds,
                signature_type=self._config.signature_type,
                funder=self._config.funder,
            )
            return self._sdk_client

    def _reset_sdk_client(self) -> None:
        with self._sdk_client_lock:
            self._sdk_client = None
            self._sdk_client_uses_configured_creds = False

    def _fallback_to_derived_sdk_client(self) -> None:
        with self._sdk_client_lock:
            self._sdk_client = None
            self._sdk_client_forced_derived_creds = True
            self._sdk_client_uses_configured_creds = False

    def _sdk_call(self, operation: Callable[[Any], Any]) -> Any:
        with self._sdk_call_lock:
            last_error: Exception | None = None
            for attempt in range(2):
                client = self._get_sdk_client()
                try:
                    return operation(client)
                except Exception as exc:
                    last_error = exc
                    if self._sdk_client_uses_configured_creds and _is_auth_sdk_error(exc):
                        LOGGER.warning(
                            "polymarket_sdk_call_falling_back_to_derived_api_key",
                            extra={"_error": str(exc), "_attempt": attempt + 1},
                        )
                        self._fallback_to_derived_sdk_client()
                        continue
                    if attempt == 1 or not _is_transient_sdk_error(exc):
                        raise
                    LOGGER.warning(
                        "polymarket_sdk_call_retrying",
                        extra={"_error": str(exc), "_attempt": attempt + 1},
                    )
                    self._reset_sdk_client()
        if last_error is not None:
            raise last_error

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

        # Submission is serialized with metadata/signing calls, but deliberately
        # not retried: a transport failure can leave the order outcome unknown.
        with self._sdk_call_lock:
            client = self._get_sdk_client()
            order_tick_size, order_neg_risk = self._resolve_order_options(
                client,
                condition_id,
                tick_size,
                neg_risk,
            )
            normalized_price = float(
                _normalize_binary_order_price(price, order_tick_size, round_up=side_name != "BUY")
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

    def _create_limit_order_preview(
        self,
        token_id: str,
        size: float,
        price: float,
        condition_id: str | None,
        tick_size: str | None,
        neg_risk: bool | None,
    ) -> str:
        try:
            from py_clob_client_v2 import OrderArgs, PartialCreateOrderOptions
            from py_clob_client_v2.order_builder.constants import BUY
        except ImportError as exc:
            raise RuntimeError("py-clob-client-v2 is required for Polymarket production previews") from exc
        with self._sdk_call_lock:
            client = self._get_sdk_client()
            order_tick_size, order_neg_risk = self._resolve_order_options(
                client,
                condition_id,
                tick_size,
                neg_risk,
            )
            normalized_price = float(_normalize_binary_order_price(price, order_tick_size, round_up=False))
            signed = client.create_order(
                OrderArgs(token_id=token_id, price=normalized_price, size=size, side=BUY),
                options=PartialCreateOrderOptions(tick_size=order_tick_size, neg_risk=order_neg_risk),
            )
        return hashlib.sha256(repr(signed).encode("utf-8")).hexdigest()

    def _resolve_order_options(
        self,
        client: Any,
        condition_id: str | None,
        tick_size: str | None,
        neg_risk: bool | None,
    ) -> tuple[str, bool]:
        if tick_size is not None and neg_risk is not None:
            return _sdk_compatible_tick_size(tick_size), neg_risk
        if not condition_id:
            raise RuntimeError("condition_id or explicit tick_size/neg_risk is required for Polymarket orders")
        cached = self._market_options_cache.get(condition_id)
        if cached is not None:
            cached_tick_size, cached_neg_risk = cached
            return (
                _sdk_compatible_tick_size(tick_size or cached_tick_size),
                neg_risk if neg_risk is not None else cached_neg_risk,
            )
        market = self._sdk_call(lambda current: current.get_market(condition_id))
        resolved_tick_size = _sdk_compatible_tick_size(tick_size or str(market["minimum_tick_size"]))
        resolved_neg_risk = neg_risk if neg_risk is not None else bool(market["neg_risk"])
        self._market_options_cache[condition_id] = (resolved_tick_size, resolved_neg_risk)
        return resolved_tick_size, resolved_neg_risk

    def _get_order_status(self, order_id: str) -> str:
        order = self._get_order_payload(order_id)
        return str(_extract_first(order, ("status", "state", "orderStatus")) or "")

    def _get_order_payload(self, order_id: str) -> dict[str, Any]:
        order = self._sdk_call(lambda client: client.get_order(order_id))
        if not isinstance(order, dict):
            raise RuntimeError(f"Polymarket returned unsupported order payload: {order!r}")
        return order

    def _cancel_order(self, order_id: str) -> None:
        try:
            from py_clob_client_v2 import OrderPayload
        except ImportError as exc:
            raise RuntimeError("py-clob-client-v2 is required for Polymarket production trading") from exc

        self._sdk_call(lambda client: client.cancel_order(OrderPayload(orderID=order_id)))

    def _get_cash_balance(self) -> float:
        try:
            from py_clob_client_v2 import AssetType, BalanceAllowanceParams
        except ImportError as exc:
            raise RuntimeError("py-clob-client-v2 is required for Polymarket production trading") from exc

        result = self._sdk_call(
            lambda client: client.get_balance_allowance(
                BalanceAllowanceParams(
                    asset_type=AssetType.COLLATERAL,
                    signature_type=self._config.signature_type,
                )
            )
        )
        raw_balance = _find_balance_value(result, ("pusd", "pUSD", "USDC", "cash", "balance", "available"))
        balance = _normalize_collateral_balance(raw_balance)
        if balance is None:
            raise RuntimeError(f"Could not parse Polymarket collateral balance from response: {result!r}")
        return balance

    def _get_market_constraints(self, token_id: str, condition_id: str) -> MarketConstraints:
        market = self._sdk_call(
            lambda current: self._fetch_clob_market_info(current, condition_id)
        )
        market_tokens = frozenset(
            str(item.get("t"))
            for item in market.get("t", ())
            if isinstance(item, dict) and item.get("t") not in (None, "")
        )
        if token_id not in market_tokens:
            raise RuntimeError(
                f"Polymarket token {token_id} is not part of condition {condition_id}"
            )
        tick = Decimal(str(market.get("mts") or ""))
        minimum_order = Decimal(str(market.get("mos") or ""))
        neg_risk = bool(market.get("nr", False))
        fee_details = market.get("fd")
        if not isinstance(fee_details, dict) or fee_details.get("r") is None or fee_details.get("e") is None:
            raise RuntimeError(f"Polymarket V2 fee metadata is unavailable for condition {condition_id}")
        fee_rate = Decimal(str(fee_details["r"]))
        fee_exponent = Decimal(str(fee_details["e"]))
        if not fee_rate.is_finite() or fee_rate < 0:
            raise RuntimeError(f"Polymarket V2 fee rate is invalid for condition {condition_id}")
        if not fee_exponent.is_finite() or fee_exponent < 0:
            raise RuntimeError(f"Polymarket V2 fee exponent is invalid for condition {condition_id}")
        dynamic_fee_bps = int(
            (fee_rate * Decimal(10_000)).to_integral_value(rounding=ROUND_CEILING)
        )
        self._market_token_ids_by_condition[condition_id] = market_tokens
        self._market_options_cache[condition_id] = (_sdk_compatible_tick_size(str(tick)), neg_risk)
        return MarketConstraints(
            fee_rate_bps=dynamic_fee_bps,
            tick_size=tick,
            lot_size=minimum_order,
            minimum_notional=Decimal("1"),
            fee_exponent=fee_exponent,
        )

    def _fetch_clob_market_info(self, client: Any, condition_id: str) -> dict[str, Any]:
        getter = getattr(client, "get_clob_market_info", None)
        if not callable(getter):
            raise RuntimeError("py-clob-client-v2 does not support get_clob_market_info")
        now = time.monotonic()
        if now < self._market_info_cooldown_until:
            remaining = self._market_info_cooldown_until - now
            raise RuntimeError(
                f"Polymarket V2 market metadata is cooling down for {remaining:.1f}s"
            )
        delay = _MARKET_INFO_MIN_INTERVAL_SECONDS - (now - self._last_market_info_request_at)
        if delay > 0:
            time.sleep(delay)
        try:
            payload = getter(condition_id)
        except Exception as exc:
            if _is_rate_limit_error(exc):
                self._market_info_cooldown_until = (
                    time.monotonic() + _MARKET_INFO_RATE_LIMIT_COOLDOWN_SECONDS
                )
            raise
        finally:
            self._last_market_info_request_at = time.monotonic()
        if not isinstance(payload, dict):
            raise RuntimeError(
                f"Polymarket V2 market metadata has unsupported format for {condition_id}"
            )
        return payload


def _extract_first(payload: Any, keys: tuple[str, ...]) -> Any:
    if isinstance(payload, dict):
        for key in keys:
            if key in payload:
                return payload[key]
    return None


def _position_from_payload(
    payload: Any,
    profile_address: str,
    offset: int,
    row_index: int,
) -> tuple[str, Decimal]:
    location = f"offset {offset}, row {row_index}"
    if not isinstance(payload, dict):
        raise RuntimeError(f"Polymarket current position is invalid at {location}")
    proxy_wallet = payload.get("proxyWallet")
    if not isinstance(proxy_wallet, str) or proxy_wallet.lower() != profile_address.lower():
        raise RuntimeError(f"Polymarket current position has an unexpected profile at {location}")
    token_id = payload.get("asset")
    if not isinstance(token_id, str) or not token_id.isdecimal() or len(token_id) > 78:
        raise RuntimeError(f"Polymarket current position has an invalid asset at {location}")
    token_number = int(token_id)
    if token_number <= 0 or token_number > _MAX_UINT256:
        raise RuntimeError(f"Polymarket current position has an invalid asset at {location}")
    if "size" not in payload or isinstance(payload["size"], bool):
        raise RuntimeError(f"Polymarket current position has an invalid size at {location}")
    try:
        size = Decimal(str(payload["size"]))
    except Exception:
        raise RuntimeError(f"Polymarket current position has an invalid size at {location}") from None
    if not size.is_finite() or size < 0:
        raise RuntimeError(f"Polymarket current position has an invalid size at {location}")
    return token_id, size


def _is_evm_address(value: str) -> bool:
    return (
        len(value) == 42
        and value.startswith("0x")
        and all(character in "0123456789abcdefABCDEF" for character in value[2:])
    )


def _sdk_compatible_tick_size(tick_size: str | Decimal) -> str:
    actual_tick = Decimal(str(tick_size))
    if not actual_tick.is_finite() or actual_tick <= 0 or actual_tick >= 1:
        raise ValueError(f"invalid Polymarket tick size: {tick_size}")
    normalized_tick = format(actual_tick.normalize(), "f")
    price_decimals = max(0, -int(actual_tick.normalize().as_tuple().exponent))
    if price_decimals > 4:
        raise ValueError(f"Polymarket tick size {tick_size} exceeds the supported 4-decimal price precision")

    try:
        from py_clob_client_v2.clob_types import RoundConfig
        from py_clob_client_v2.order_builder.builder import ROUNDING_CONFIG  # type: ignore[import-untyped]
    except ImportError as exc:
        raise RuntimeError("py-clob-client-v2 is required for Polymarket production trading") from exc

    with _SDK_TICK_CONFIG_LOCK:
        ROUNDING_CONFIG.setdefault(
            normalized_tick,
            RoundConfig(price=price_decimals, size=2, amount=price_decimals + 2),
        )
    return normalized_tick


def _normalize_binary_order_price(price: float | Decimal, tick_size: str, *, round_up: bool) -> Decimal:
    tick = Decimal(str(tick_size))
    if tick <= 0 or tick >= 1:
        raise ValueError(f"invalid binary-market tick size: {tick_size}")
    lower_bound = tick
    upper_bound = Decimal("1") - tick
    if lower_bound > upper_bound:
        raise ValueError(f"binary-market tick size has no executable price range: {tick_size}")
    quantized = quantize_up(price, tick) if round_up else quantize_down(price, tick)
    return min(max(quantized, lower_bound), upper_bound)


def _is_transient_sdk_error(exc: BaseException) -> bool:
    current: BaseException | None = exc
    while current is not None:
        text = f"{type(current).__name__}: {current}".lower()
        if any(
            needle in text
            for needle in (
                "server disconnected",
                "remoteprotocolerror",
                "readtimeout",
                "timed out",
                "timeout",
                "connection reset",
                "connection aborted",
                "request exception!",
            )
        ):
            return True
        current = current.__cause__ or current.__context__
    return False


def _is_rate_limit_error(exc: BaseException) -> bool:
    current: BaseException | None = exc
    while current is not None:
        text = f"{type(current).__name__}: {current}".lower()
        if any(
            needle in text
            for needle in (
                "status=429",
                "status code 429",
                "too many requests",
                "rate limit",
                "error 1015",
            )
        ):
            return True
        current = current.__cause__ or current.__context__
    return False


def _is_auth_sdk_error(exc: BaseException) -> bool:
    current: BaseException | None = exc
    while current is not None:
        text = f"{type(current).__name__}: {current}".lower()
        if any(
            needle in text
            for needle in (
                "unauthorized/invalid api key",
                "invalid api key",
                "unauthorized",
                "forbidden",
            )
        ):
            return True
        current = current.__cause__ or current.__context__
    return False


def _clob_ws_url(api_base_url: str) -> str:
    del api_base_url
    return "wss://ws-subscriptions-clob.polymarket.com/ws/market"


def _subscription_payload(token_ids: list[str], *, operation: str | None = None) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "assets_ids": token_ids,
        "custom_feature_enabled": True,
    }
    if operation is None:
        payload["type"] = "market"
    else:
        payload["operation"] = operation
    return payload


def _asset_id(payload: dict[str, Any]) -> str:
    value = payload.get("asset_id") or payload.get("assetId") or payload.get("token_id") or payload.get("tokenId")
    return str(value) if value not in (None, "") else ""


def _json_loads(payload: str | bytes) -> Any:
    try:
        import orjson
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
        sequence=event_sequence(payload),
        checksum=event_checksum(payload),
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
    sequences = [
        sequence for change in changes if isinstance(change, dict) and (sequence := event_sequence(change)) is not None
    ]
    next_sequence = max(sequences) if sequences else None
    valid_sequence = book.sequence is None or next_sequence is None or next_sequence == book.sequence + 1
    return OrderBook(
        bids=_sorted_bids([OrderBookLevel(price, size) for price, size in bids.items()])[:10],
        asks=_sorted_asks([OrderBookLevel(price, size) for price, size in asks.items()])[:10],
        raw_payload={"changes": changes},
        sequence=next_sequence if next_sequence is not None else book.sequence,
        status=MarketDataStatus.VALID if valid_sequence else MarketDataStatus.INVALID,
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


def _find_balance_value(payload: Any, keys: tuple[str, ...]) -> Any | None:
    if isinstance(payload, (int, float, str)):
        return payload
    if isinstance(payload, dict):
        for key in keys:
            if key in payload:
                nested = _find_balance_value(payload[key], keys)
                if nested is not None:
                    return nested
        for value in payload.values():
            nested = _find_balance_value(value, keys)
            if nested is not None:
                return nested
    if isinstance(payload, list):
        for item in payload:
            nested = _find_balance_value(item, keys)
            if nested is not None:
                return nested
    return None


def _normalize_collateral_balance(value: Any) -> float | None:
    if value in (None, ""):
        return None
    raw = str(value).strip()
    try:
        numeric = float(raw)
    except (TypeError, ValueError):
        return None
    # Polymarket balance-allowance can return collateral base units as an
    # integer string. Collateral is 6-decimal pUSD/USDC, so normalize it.
    if raw.isdigit() and "." not in raw and "e" not in raw.lower():
        return numeric / 1_000_000.0
    return numeric


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


def _extract_requested_amount(payload: dict[str, Any]) -> float | None:
    value = _extract_first(payload, ("original_size", "originalSize", "size", "amount"))
    try:
        return float(str(value)) if value not in (None, "") else None
    except (TypeError, ValueError):
        return None


def _venue_order_from_payload(payload: dict[str, Any]) -> VenueOrder:
    order_id = str(_extract_first(payload, ("id", "orderID", "order_id", "hash")) or "")
    requested = Decimal(str(_extract_requested_amount(payload) or 0.0))
    filled = Decimal(str(_extract_filled_amount(payload) or 0.0))
    status = str(_extract_first(payload, ("status", "state")) or "open").lower()
    normalized = OrderIntentStatus.PARTIAL if filled > 0 else OrderIntentStatus.ACKNOWLEDGED
    if status in {"filled", "matched"}:
        normalized = OrderIntentStatus.FILLED
    return VenueOrder(
        client_order_id="",
        venue_order_id=order_id,
        venue="Polymarket",
        status=normalized,
        quantity=requested,
        cumulative_filled=filled,
        average_price=Decimal(str(_extract_avg_price(payload) or _extract_first(payload, ("price",)) or 0)),
        updated_at=datetime.now(UTC),
    )


def _fill_from_trade(payload: dict[str, Any]) -> FillRecord:
    fill_id = str(_extract_first(payload, ("id", "trade_id", "tradeId", "match_id", "matchId")) or "")
    order_id = str(_extract_first(payload, ("order_id", "orderId", "maker_order_id", "taker_order_id")) or fill_id)
    raw_time = _extract_first(payload, ("timestamp", "created_at", "createdAt", "match_time"))
    occurred_at = datetime.fromtimestamp(event_timestamp({"timestamp": raw_time}), tz=UTC)
    return FillRecord(
        fill_id=fill_id,
        client_order_id="",
        venue_order_id=order_id,
        venue="Polymarket",
        quantity=Decimal(str(_extract_first(payload, ("size", "amount", "quantity")) or 0)),
        price=Decimal(str(_extract_first(payload, ("price", "avg_price")) or 0)),
        fee=Decimal(str(_extract_first(payload, ("fee", "fee_amount", "feeAmount")) or 0)),
        occurred_at=occurred_at,
    )
