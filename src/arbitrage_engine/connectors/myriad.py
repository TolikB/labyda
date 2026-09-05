from __future__ import annotations

import asyncio
import hashlib
import logging
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, cast

from arbitrage_engine.config import MyriadMarketsConfig
from arbitrage_engine.connectors.base import (
    OrderBookStaleException,
    OrderBookUnavailableException,
    PredictFunClient,
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
    RedemptionIntentStatus,
    RedemptionReport,
    SettlementRequest,
    SettlementStatus,
    VenueFeeQuote,
    VenueOrder,
)
from arbitrage_engine.utils.math import quantize_down, quantize_up

LOGGER = logging.getLogger(__name__)
PASSIVE_BOOK_MAX_AGE_SECONDS = 2.0
ORDER_BOOK_BOOTSTRAP_CONCURRENCY = 16
# Funded refreshes start after one quarter of the hard freshness window and the
# coordinator polls every one eighth of it.  A 23/40 timeout therefore leaves a
# nominal 1/20 fail-closed margin (100 ms at the production two-second gate).
# Myriad's authenticated order-book endpoint has a healthy tail above the
# former 1/3-window cutoff, so cancelling those requests early created
# avoidable stale gaps without reducing venue pressure.
PROACTIVE_REFRESH_TIMEOUT_FRACTION = 23 / 40
PROACTIVE_REFRESH_MIN_TIMEOUT_SECONDS = 0.05
MARKET_CONSTRAINTS_TTL_SECONDS = 30.0
SHARE_DECIMALS = 18
PRICE_DECIMALS = 18
PRICE_TICK_UNITS = 10**16
COLLATERAL_DECIMALS = 6


def _proactive_refresh_timeout_seconds(freshness_seconds: float) -> float:
    return max(
        PROACTIVE_REFRESH_MIN_TIMEOUT_SECONDS,
        freshness_seconds * PROACTIVE_REFRESH_TIMEOUT_FRACTION,
    )


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
class MyriadSignedOrder:
    order: dict[str, Any]
    signature: str


class MyriadClient(PredictFunClient):
    venue_name = "Myriad"

    def __init__(self, config: MyriadMarketsConfig) -> None:
        self._config = config
        self._nonce = int(time.time() * 1000)
        self._nonce_lock = asyncio.Lock()
        self._web3_client: BaseWeb3Client | None = None
        self._collateral_decimals: int | None = None
        self._order_amounts: dict[str, float] = {}
        self._order_prices: dict[str, float] = {}
        self._signed_orders: dict[str, MyriadSignedOrder] = {}
        self._books: dict[str, OrderBook] = {}
        self._book_timestamps: dict[str, float] = {}
        self._snapshot_timestamps: dict[str, float] = {}
        self._book_events: dict[str, asyncio.Event] = {}
        self._bootstrap_tasks: dict[str, asyncio.Task[OrderBook]] = {}
        # The production evaluation window can contain thirteen funded Myriad
        # targets plus discovery traffic. Sixteen bounded requests let every
        # funded target start without timing out behind the semaphore while
        # retaining a hard cap on venue pressure.
        self._bootstrap_semaphore = asyncio.Semaphore(ORDER_BOOK_BOOTSTRAP_CONCURRENCY)
        self._rest_session: Any | None = None
        self._ws_session: Any | None = None
        self._desired_channels: set[str] = set()
        self._channel_tokens: dict[str, set[str]] = {}
        self._subscription_queue: asyncio.Queue[tuple[str, str]] = asyncio.Queue()
        self._ws_task: asyncio.Task[None] | None = None
        self._ws: Any | None = None
        self._reconnect_lock = asyncio.Lock()
        self._reconnecting = False
        self._ws_connected = False
        self._reconnect_backoff = WebSocketReconnectBackoff()
        self._snapshot_interval_seconds = 30.0
        self._execution_freshness_seconds = PASSIVE_BOOK_MAX_AGE_SECONDS
        self._reconnect_count = 0
        self._sequence_gap_count = 0
        self._proactive_refresh_count = 0
        self._proactive_refresh_failure_count = 0
        self._proactive_refresh_timeout_count = 0
        self._stale_refresh_attempted_at: dict[str, float] = {}
        self._market_constraints_cache: dict[int, tuple[float, MarketConstraints]] = {}
        self._market_fee_payload_cache: dict[int, dict[str, Any]] = {}
        self._market_fee_catalog_cached_at = 0.0
        self._market_fee_catalog_lock = asyncio.Lock()

    async def watch_order_book(self, token_id: str) -> OrderBook:
        market_id, side = _parse_token_id(token_id)
        self._ensure_token_subscription(token_id, market_id)
        self._ensure_ws_task()
        cached = self._books.get(token_id)
        if cached is not None and cached.status in {MarketDataStatus.INVALID, MarketDataStatus.STALE}:
            task, _ = self._ensure_bootstrap_task(token_id, market_id, side, force=True)
            return await self._await_bootstrap_task(token_id, task)
        ttl_seconds = self._config.order_book_ttl_ms / 1_000.0
        stale_after_seconds = self._config.websocket_stale_after_ms / 1_000.0
        passive_age_seconds = max(ttl_seconds, stale_after_seconds, self._execution_freshness_seconds)
        if token_id in self._books:
            age = time.monotonic() - self._book_timestamps.get(token_id, 0.0)
            if age <= ttl_seconds:
                snapshot_at = self._snapshot_timestamps.get(token_id)
                if (
                    cached is not None
                    and cached.sequence is None
                    and snapshot_at is not None
                    and time.monotonic() - snapshot_at >= self._snapshot_interval_seconds
                ):
                    task, _ = self._ensure_bootstrap_task(token_id, market_id, side, force=True)
                    return await self._await_bootstrap_task(token_id, task)
                return self._books[token_id]
            if self._cached_book_is_passively_fresh(token_id, passive_age_seconds):
                return self._books[token_id]
            task, _ = self._ensure_bootstrap_task(token_id, market_id, side, force=True)
            if task is not None:
                return await self._await_bootstrap_task(token_id, task)
            age = time.monotonic() - self._book_timestamps.get(token_id, 0.0)
            refresh_age = time.monotonic() - self._stale_refresh_attempted_at.get(token_id, 0.0)
            if refresh_age < self._execution_freshness_seconds:
                raise OrderBookStaleException(
                    f"Myriad order book refresh is cooling down for token {token_id}, age={age:.3f}s"
                )
            reason = "websocket stalled" if age >= stale_after_seconds else "TTL exceeded"
            raise OrderBookStaleException(f"Myriad order book is stale for token {token_id}: {reason}, age={age:.3f}s")

        task, _ = self._ensure_bootstrap_task(token_id, market_id, side, force=False)
        return await self._await_bootstrap_task(token_id, task)

    async def _bootstrap_order_book(
        self,
        token_id: str,
        market_id: int,
        side: BinarySide | None,
        force: bool = False,
    ) -> OrderBook:
        async with self._bootstrap_semaphore:
            if token_id in self._books and not force:
                return self._books[token_id]
            book_before_request = self._books.get(token_id)
            resolved_side = side or BinarySide.YES
            raw = await self.get_orderbook(market_id, _outcome_id(resolved_side))
            book = _order_book_from_payload(raw, side)
            current_book = self._books.get(token_id)
            if token_id not in self._active_tokens():
                return current_book or book
            if current_book is not book_before_request and current_book is not None:
                return current_book
            self._store_book(token_id, book)
            self._snapshot_timestamps[token_id] = time.monotonic()
            return book

    def _ensure_bootstrap_task(
        self,
        token_id: str,
        market_id: int,
        side: BinarySide | None,
        *,
        force: bool,
        min_refresh_interval_seconds: float | None = None,
    ) -> tuple[asyncio.Task[OrderBook] | None, bool]:
        task = self._bootstrap_tasks.get(token_id)
        if task is not None and not task.done():
            return task, False
        now = time.monotonic()
        refresh_interval = (
            self._execution_freshness_seconds
            if min_refresh_interval_seconds is None
            else max(0.0, min_refresh_interval_seconds)
        )
        if force and now - self._stale_refresh_attempted_at.get(token_id, 0.0) < refresh_interval:
            return None, False
        self._stale_refresh_attempted_at[token_id] = now
        task = asyncio.create_task(self._bootstrap_order_book(token_id, market_id, side, force=force))
        self._bootstrap_tasks[token_id] = task
        return task, True

    async def refresh_market_data_target(self, token_id: str) -> bool:
        """Confirm a quiet active book before the execution-age deadline expires."""
        if token_id not in self._active_tokens():
            return False
        market_id, side = _parse_token_id(token_id)
        previous_receipt = self._book_timestamps.get(token_id)
        now = time.monotonic()
        refresh_interval = max(0.1, self._execution_freshness_seconds / 10.0)
        if now - self._stale_refresh_attempted_at.get(token_id, 0.0) < refresh_interval:
            return False
        self._stale_refresh_attempted_at[token_id] = now
        timeout_seconds = _proactive_refresh_timeout_seconds(self._execution_freshness_seconds)
        try:
            # Proactive refreshes deliberately do not share the normal bootstrap
            # registry. A slow discovery/evaluation request must not prevent a
            # bounded independent confirmation of an exact funded target.
            async with asyncio.timeout(timeout_seconds):
                await self._bootstrap_order_book(token_id, market_id, side, force=True)
        except asyncio.CancelledError:
            raise
        except TimeoutError:
            self._proactive_refresh_timeout_count += 1
            return False
        except Exception:
            self._proactive_refresh_failure_count += 1
            raise
        self._proactive_refresh_count += 1
        refreshed_receipt = self._book_timestamps.get(token_id)
        return refreshed_receipt is not None and (
            previous_receipt is None or refreshed_receipt > previous_receipt
        )

    async def _await_bootstrap_task(self, token_id: str, task: asyncio.Task[OrderBook] | None) -> OrderBook:
        if task is None:
            raise OrderBookStaleException(f"Myriad order book refresh is cooling down for token {token_id}")
        try:
            return await asyncio.shield(task)
        finally:
            if self._bootstrap_tasks.get(token_id) is task and task.done():
                self._bootstrap_tasks.pop(token_id, None)

    def _ensure_ws_task(self) -> None:
        if self._ws_task is None or self._ws_task.done():
            self._ws_task = asyncio.create_task(self._run_orderbook_ws())

    async def _run_orderbook_ws(self) -> None:
        try:
            import aiohttp
        except ImportError:
            return
        while True:
            connected_at: float | None = None
            try:
                session = self._get_ws_session()
                async with session.ws_connect(self._config.ws_url, heartbeat=15) as ws:
                    self._ws = ws
                    await ws.send_json({"connect": {}, "id": 1})
                    first = await ws.receive_json(timeout=10)
                    if first.get("error"):
                        raise RuntimeError(f"Myriad Centrifugo handshake failed: {first!r}")
                    connected_at = time.monotonic()
                    self._ws_connected = True
                    self._reconnecting = False
                    command_id = 2
                    subscribed = set(self._desired_channels)
                    for channel in subscribed:
                        await ws.send_json({"subscribe": {"channel": channel}, "id": command_id})
                        command_id += 1
                    sender = asyncio.create_task(self._send_subscriptions(ws, command_id, subscribed))
                    try:
                        async for message in ws:
                            if message.type != aiohttp.WSMsgType.TEXT:
                                continue
                            for raw_message in str(message.data).splitlines():
                                if not raw_message:
                                    continue
                                payload = _json_loads(raw_message)
                                if payload == {}:
                                    await ws.send_json({})
                                    continue
                                if isinstance(payload, dict):
                                    self._handle_ws_payload(payload)
                    finally:
                        sender.cancel()
                        await asyncio.gather(sender, return_exceptions=True)
            except asyncio.CancelledError:
                raise
            except (aiohttp.ClientConnectionError, ConnectionResetError) as exc:
                LOGGER.info("myriad_ws_disconnected", extra={"reason": type(exc).__name__})
            except Exception:
                LOGGER.exception("myriad_ws_failed")
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

    async def _send_subscriptions(self, ws: Any, command_id: int, subscribed: set[str]) -> None:
        while True:
            operation, channel = await self._subscription_queue.get()
            if operation == "subscribe":
                if channel in subscribed or channel not in self._desired_channels:
                    continue
                await ws.send_json({"subscribe": {"channel": channel}, "id": command_id})
                command_id += 1
                subscribed.add(channel)
                continue
            if channel not in subscribed:
                continue
            await ws.send_json({"unsubscribe": {"channel": channel}, "id": command_id})
            command_id += 1
            subscribed.discard(channel)

    def _handle_ws_payload(self, payload: dict[str, Any]) -> None:
        push = payload.get("push")
        if not isinstance(push, dict):
            return
        channel = str(push.get("channel") or "")
        channel_identity = _parse_orderbook_channel(channel)
        if channel_identity is None:
            return
        expected_network_id, expected_market_id = channel_identity
        publication = push.get("pub")
        data = publication.get("data") if isinstance(publication, dict) else None
        if not isinstance(data, dict):
            return
        if not _payload_matches_channel(data, expected_network_id, expected_market_id):
            LOGGER.error(
                "myriad_ws_payload_identity_mismatch",
                extra={"_channel": channel, "_payload": data},
            )
            return
        for token_id in self._channel_tokens.get(channel, set()):
            token_market_id, side = _parse_token_id(token_id)
            if token_market_id != expected_market_id:
                continue
            book = _order_book_from_payload(data, side)
            if book.bids or book.asks:
                self._snapshot_timestamps.setdefault(token_id, time.monotonic())
                self._store_book(token_id, book)
                continue
            changes = data.get("changes") or data.get("price_changes") or data.get("priceChanges")
            if isinstance(changes, list) and token_id in self._books:
                self._store_book(token_id, _apply_orderbook_changes(self._books[token_id], changes, side))

    def _store_book(self, token_id: str, book: OrderBook) -> None:
        if book.status is MarketDataStatus.INVALID:
            self._sequence_gap_count += 1
        self._books[token_id] = replace(book, timestamp=min(book.timestamp, time.time()))
        self._book_timestamps[token_id] = time.monotonic()
        self._book_events.setdefault(token_id, asyncio.Event()).set()

    def market_data_age_seconds(self) -> float | None:
        active_tokens = self._active_tokens()
        if not active_tokens:
            return None
        timestamps = [
            self._book_timestamps[token_id] for token_id in active_tokens if token_id in self._book_timestamps
        ]
        if not timestamps:
            return None
        now = time.monotonic()
        return now - max(timestamps)

    def market_data_target_age_seconds(self, token_id: str) -> float | None:
        timestamp = self._book_timestamps.get(token_id)
        if timestamp is None:
            return None
        return max(0.0, time.monotonic() - timestamp)

    def market_data_target_ready(self, token_id: str, max_age_seconds: float) -> bool:
        book = self._books.get(token_id)
        return book is not None and self.is_order_book_execution_fresh(token_id, book, max_age_seconds)

    def set_market_data_snapshot_interval(self, seconds: float) -> None:
        self._snapshot_interval_seconds = seconds

    def set_market_data_execution_freshness(self, seconds: float) -> None:
        ttl_seconds = self._config.order_book_ttl_ms / 1_000.0
        self._execution_freshness_seconds = max(ttl_seconds, seconds)

    def market_data_ready(self) -> bool:
        active_tokens = self._active_tokens()
        return self._ws_connected and bool(active_tokens) and all(
            token_id in self._books and self._books[token_id].status is MarketDataStatus.VALID
            for token_id in active_tokens
        )

    def has_active_market_data_targets(self) -> bool:
        return bool(self._active_tokens())

    def active_market_data_target_count(self) -> int:
        return len(self._active_tokens())

    def sync_market_data_targets(self, token_ids: set[str]) -> None:
        normalized = {token_id for token_id in token_ids if token_id}
        current_tokens = self._active_tokens()
        removed = current_tokens - normalized
        added = normalized - current_tokens
        for token_id in removed:
            self._remove_token_subscription(token_id)
        for token_id in added:
            market_id, _ = _parse_token_id(token_id)
            self._ensure_token_subscription(token_id, market_id)
            self._start_background_bootstrap(token_id, market_id)
        if self._desired_channels:
            self._ensure_ws_task()

    def _start_background_bootstrap(self, token_id: str, market_id: int) -> None:
        _, side = _parse_token_id(token_id)
        task, started = self._ensure_bootstrap_task(token_id, market_id, side, force=False)
        if task is None or not started:
            return
        task.add_done_callback(lambda done: self._finalize_background_bootstrap(token_id, done))

    def _finalize_background_bootstrap(self, token_id: str, task: asyncio.Task[OrderBook]) -> None:
        if self._bootstrap_tasks.get(token_id) is task and task.done():
            self._bootstrap_tasks.pop(token_id, None)
        if task.cancelled():
            return
        exc = task.exception()
        if exc is None:
            return
        LOGGER.debug(
            "myriad_background_bootstrap_failed",
            extra={"_token_id": token_id, "_error": str(exc)},
        )

    async def reconnect_market_data(self) -> None:
        async with self._reconnect_lock:
            if self._reconnecting:
                return
            self._reconnecting = True
            self._ws_connected = False
            self._mark_books_stale()
            if self._ws is not None and not self._ws.closed:
                await self._ws.close()
            if self._desired_channels:
                self._ensure_ws_task()

    def _mark_books_stale(self) -> None:
        active_tokens = self._active_tokens()
        for token_id in active_tokens & self._books.keys():
            self._books[token_id] = replace(self._books[token_id], status=MarketDataStatus.STALE)

    def telemetry_snapshot(self) -> dict[str, float]:
        return {
            "reconnects": float(self._reconnect_count),
            "sequence_gaps": float(self._sequence_gap_count),
            "proactive_refreshes": float(self._proactive_refresh_count),
            "proactive_refresh_failures": float(self._proactive_refresh_failure_count),
            "proactive_refresh_timeouts": float(self._proactive_refresh_timeout_count),
            "connected": float(self._ws_connected),
            "reconnecting": float(self._reconnecting),
            "reconnect_backoff_seconds": self._reconnect_backoff.current_delay_seconds,
        }

    def _ensure_token_subscription(self, token_id: str, market_id: int) -> None:
        channel = f"orderbook:{self._config.chain_id}:{market_id}"
        self._channel_tokens.setdefault(channel, set()).add(token_id)
        if channel not in self._desired_channels:
            self._desired_channels.add(channel)
            self._subscription_queue.put_nowait(("subscribe", channel))
        self._book_events.setdefault(token_id, asyncio.Event())

    def _remove_token_subscription(self, token_id: str) -> None:
        market_id, _ = _parse_token_id(token_id)
        channel = f"orderbook:{self._config.chain_id}:{market_id}"
        tokens = self._channel_tokens.get(channel)
        if tokens is not None:
            tokens.discard(token_id)
            if not tokens:
                self._channel_tokens.pop(channel, None)
                if channel in self._desired_channels:
                    self._desired_channels.discard(channel)
                    self._subscription_queue.put_nowait(("unsubscribe", channel))
        task = self._bootstrap_tasks.pop(token_id, None)
        if task is not None:
            task.cancel()
        self._books.pop(token_id, None)
        self._book_timestamps.pop(token_id, None)
        self._snapshot_timestamps.pop(token_id, None)
        self._book_events.pop(token_id, None)
        self._stale_refresh_attempted_at.pop(token_id, None)

    def _active_tokens(self) -> set[str]:
        return {token for tokens in self._channel_tokens.values() for token in tokens}

    def _cached_book_is_passively_fresh(self, token_id: str, max_age_seconds: float) -> bool:
        book = self._books.get(token_id)
        if book is None or book.status is not MarketDataStatus.VALID:
            return False
        return max(0.0, time.time() - book.timestamp) <= max_age_seconds

    async def get_orderbook(self, market_id: int, outcome_id: int) -> dict[str, Any]:
        try:
            import aiohttp

            _ = aiohttp
        except ImportError as exc:
            raise RuntimeError("aiohttp is required for Myriad connectivity") from exc

        url = f"{self._config.api_url.rstrip('/')}/markets/{market_id}/orderbook"
        params = _orderbook_query_params(self._config.chain_id, outcome_id)
        session = self._get_rest_session()
        timeout = aiohttp.ClientTimeout(total=20, connect=10, sock_read=10)
        try:
            async with session.get(url, params=params, timeout=timeout) as response:
                response.raise_for_status()
                payload = await response.json()
        except asyncio.CancelledError:
            raise
        except (TimeoutError, aiohttp.ClientError) as exc:
            raise OrderBookUnavailableException(f"Myriad order book is unavailable for market {market_id}") from exc
        if not isinstance(payload, dict):
            raise RuntimeError(f"Myriad orderbook payload has unsupported format: {payload!r}")
        return payload

    def _get_rest_session(self) -> Any:
        if self._rest_session is None or self._rest_session.closed:
            self._rest_session = client_session(self._headers())
        return self._rest_session

    def _get_ws_session(self) -> Any:
        if self._ws_session is None or self._ws_session.closed:
            self._ws_session = client_session()
        return self._ws_session

    async def _close_ws_session(self) -> None:
        if self._ws_session is not None and not self._ws_session.closed:
            await self._ws_session.close()
        self._ws_session = None

    async def close(self) -> None:
        if self._ws_task is not None:
            self._ws_task.cancel()
            await asyncio.gather(self._ws_task, return_exceptions=True)
            self._ws_task = None
        bootstrap_tasks = list(self._bootstrap_tasks.values())
        for task in bootstrap_tasks:
            task.cancel()
        if bootstrap_tasks:
            await asyncio.gather(*bootstrap_tasks, return_exceptions=True)
        self._bootstrap_tasks.clear()
        if self._rest_session is not None and not self._rest_session.closed:
            await self._rest_session.close()
        self._rest_session = None
        await self._close_ws_session()
        if self._web3_client is not None:
            await self._web3_client.close()
        self._web3_client = None

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
        order_id = await self.place_order(signed, time_in_force="FOK")
        self._order_amounts[order_id] = contracts
        self._order_prices[order_id] = max_price
        return order_id

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
        del (
            client_order_id,
            prepared_order_fingerprint,
            submission_deadline_unix,
            condition_id,
            tick_size,
            neg_risk,
        )
        market_id, _ = _parse_token_id(token_id)
        signed = await self.sign_order(market_id, _outcome_id(side), 0, contracts, max_price)
        order_id = await self.place_order(
            signed,
            time_in_force="FOK",
            pre_transport_guard=pre_transport_guard,
        )
        self._order_amounts[order_id] = contracts
        self._order_prices[order_id] = max_price
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
        del condition_id, tick_size, neg_risk
        market_id, _ = _parse_token_id(token_id)
        signed = await self.sign_order(market_id, _outcome_id(side), 1, contracts, min_price)
        order_id = await self.place_order(signed)
        self._order_amounts[order_id] = contracts
        self._order_prices[order_id] = min_price
        return order_id

    async def wait_filled(self, order_id: str, timeout_ms: int) -> ExecutionReport:
        try:
            import aiohttp

            _ = aiohttp
        except ImportError as exc:
            raise RuntimeError("aiohttp is required for Myriad connectivity") from exc

        deadline = time.monotonic() + timeout_ms / 1000
        requested = self._order_amounts.get(order_id, 0.0)
        last_filled = 0.0
        last_status = "pending"
        last_avg_price = self._order_prices.get(order_id, 0.0)
        url = f"{self._config.api_url.rstrip('/')}/orders/{order_id}"
        session = self._get_rest_session()
        while time.monotonic() < deadline:
            async with session.get(url, timeout=5) as response:
                response.raise_for_status()
                payload = await response.json()
            status = str(_extract_first_nested(payload, ("status", "state", "orderStatus")) or "").lower()
            last_status = status or last_status
            parsed_filled = _extract_filled_amount(payload)
            if parsed_filled is not None:
                parsed_filled = _normalize_order_amount(parsed_filled, requested)
                last_filled = max(last_filled, parsed_filled)
            parsed_avg_price = _extract_avg_price(payload)
            if parsed_avg_price is not None:
                last_avg_price = _normalize_price(parsed_avg_price)
            if status in {"filled", "matched", "executed", "complete", "completed"}:
                return ExecutionReport.from_amounts(
                    order_id, requested, parsed_filled or requested, status, last_avg_price
                )
            if status in {"cancelled", "canceled", "expired", "rejected", "failed"}:
                return ExecutionReport.from_amounts(order_id, requested, last_filled, status, last_avg_price)
            await asyncio.sleep(0.2)
        return ExecutionReport.from_amounts(order_id, requested, last_filled, last_status, last_avg_price)

    async def cancel_order(self, order_id: str) -> None:
        try:
            import aiohttp

            _ = aiohttp
        except ImportError as exc:
            raise RuntimeError("aiohttp is required for Myriad connectivity") from exc

        signed_order = self._signed_orders.get(order_id)
        if signed_order is None:
            raise RuntimeError(f"Cannot cancel Myriad order without its original signed payload: {order_id}")
        payload = {
            "order": signed_order.order,
            "signature": signed_order.signature,
            "network_id": self._config.chain_id,
        }
        base_url = self._config.api_url.rstrip("/")
        session = self._get_rest_session()
        async with session.delete(f"{base_url}/orders/{order_id}", json=payload, timeout=10) as response:
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
        decimals = await self._get_collateral_decimals(token)
        balance: float = float(int(raw_balance)) / float(10**decimals)
        return balance

    async def get_order(self, order_id: str) -> ExecutionReport:
        payload = await self._request_json("GET", f"/orders/{order_id}")
        requested = self._order_amounts.get(order_id, _extract_requested_amount(payload))
        filled = _normalize_order_amount(_extract_filled_amount(payload) or 0.0, requested)
        status = str(_extract_first_nested(payload, ("status", "state", "orderStatus")) or "open")
        price = _normalize_price(_extract_avg_price(payload) or self._order_prices.get(order_id, 0.0))
        return ExecutionReport.from_amounts(order_id, requested, filled, status, price)

    async def list_open_orders(self) -> list[VenueOrder]:
        params = {
            "network_id": str(self._config.chain_id),
            "status": "open",
            "page": "1",
            "limit": "100",
        }
        account = self._account_address()
        if account:
            params["trader"] = account
        payload = await self._request_json("GET", "/orders", query_params=params)
        return [_venue_order_from_payload(item) for item in _extract_records(payload, ("orders", "items", "results"))]

    async def list_fills(self, since: datetime | None = None) -> list[FillRecord]:
        account = self._account_address()
        if not account:
            return []
        params = {"network_id": str(self._config.chain_id), "page": "1", "limit": "100"}
        if since is not None:
            params["since"] = str(int(since.timestamp()))
        try:
            payload = await self._request_json("GET", f"/users/{account}/events", query_params=params)
        except Exception as exc:
            if _is_not_found_error(exc):
                LOGGER.info("myriad_trades_endpoint_unavailable", extra={"_path": "/users/:address/events"})
                return []
            raise
        return [_fill_from_trade(item) for item in _extract_records(payload, ("events", "fills", "items", "results"))]

    async def get_positions(self) -> dict[str, Decimal]:
        account = self._account_address()
        if not account:
            return {}
        token_address = self._config.collateral_tokens.get(self._config.collateral_symbol)
        params = {
            "network_id": str(self._config.chain_id),
            "page": "1",
            "limit": "100",
            "state": "open",
            "min_shares": "0",
            "status": "all",
        }
        if token_address:
            params["token_address"] = token_address
        try:
            payload = await self._request_json("GET", f"/users/{account}/markets", query_params=params)
        except Exception as exc:
            if _is_not_found_error(exc):
                LOGGER.info("myriad_trades_endpoint_unavailable", extra={"_path": "/users/:address/markets"})
                return {}
            raise
        positions: dict[str, Decimal] = {}
        for item in _extract_records(payload, ("markets", "positions", "items", "results")):
            market_id_value = _extract_first_nested(item, ("marketId", "market_id"))
            outcome_value = _extract_first_nested(item, ("outcomeId", "outcome_id", "outcome"))
            market_id = "" if market_id_value in (None, "") else str(market_id_value)
            outcome = "" if outcome_value in (None, "") else str(outcome_value)
            if not market_id or outcome == "":
                continue
            normalized_outcome = "YES" if outcome in {"0", "YES", "yes"} else "NO"
            key = f"{market_id}:{normalized_outcome}"
            shares = _extract_decimal(item, ("shares", "amount", "quantity", "positionSize", "position_size"))
            if shares is None:
                continue
            positions[key] = positions.get(key, Decimal(0)) + shares
        return positions

    def supports_full_reconciliation(self) -> bool:
        return True

    def supports_automatic_redemption(self) -> bool:
        # Myriad OB redemption is API-calldata based. The API resolves the
        # market contract details from market_id, then the local key signs the
        # returned transaction. A Conditional Tokens conditionId is not part of
        # the current public OB settlement contract.
        return bool(self._config.private_key)

    async def get_settlement_status(self, request: SettlementRequest) -> SettlementStatus:
        market = await self._market_payload(request.market_id)
        return _myriad_settlement_status(market)

    def prepare_settlement_request(self, request: SettlementRequest) -> SettlementRequest:
        return self._settlement_request(request)

    async def redeem_position(self, request: SettlementRequest, redemption_id: str) -> RedemptionReport:
        del redemption_id
        web3_client = self._get_web3_client()
        if web3_client.account is None:
            return RedemptionReport(RedemptionIntentStatus.MANUAL_REVIEW, error="signing key is unavailable")
        try:
            payload = await self._request_json(
                "POST",
                "/positions/redeem",
                json_body={
                    "market_id": int(request.market_id),
                    "network_id": self._config.chain_id,
                },
            )
            transaction = _myriad_claim_transaction(
                payload,
                web3_client.account.address,
                self._config.redemption_gas_limit,
            )
            tx_hash = await web3_client.send_transaction(transaction)
        except Exception as exc:
            return RedemptionReport(RedemptionIntentStatus.UNKNOWN, error=str(exc))
        return RedemptionReport(RedemptionIntentStatus.SUBMITTED, tx_hash=tx_hash)

    async def reconcile_redemption(
        self,
        request: SettlementRequest,
        report: RedemptionReport,
    ) -> RedemptionReport:
        if not report.tx_hash:
            return RedemptionReport(
                RedemptionIntentStatus.UNKNOWN,
                error=report.error or "transaction hash unavailable",
            )
        try:
            status = await self._get_web3_client().transaction_status(report.tx_hash)
        except Exception as exc:
            return RedemptionReport(RedemptionIntentStatus.UNKNOWN, tx_hash=report.tx_hash, error=str(exc))
        if status is None:
            return RedemptionReport(RedemptionIntentStatus.UNKNOWN, tx_hash=report.tx_hash)
        if not status:
            return RedemptionReport(RedemptionIntentStatus.FAILED, tx_hash=report.tx_hash, error="transaction reverted")
        try:
            if await self._has_redeemable_position(request):
                return RedemptionReport(
                    RedemptionIntentStatus.UNKNOWN,
                    tx_hash=report.tx_hash,
                    error="redemption receipt confirmed but Myriad portfolio still reports winnings to claim",
                )
        except Exception as exc:
            return RedemptionReport(
                RedemptionIntentStatus.UNKNOWN,
                tx_hash=report.tx_hash,
                error=f"redemption receipt confirmed but portfolio verification failed: {exc}",
            )
        return RedemptionReport(RedemptionIntentStatus.CONFIRMED, tx_hash=report.tx_hash)

    async def get_native_gas_balance(self) -> float:
        return float(await self._get_web3_client().native_balance())

    def _settlement_request(self, request: SettlementRequest) -> SettlementRequest:
        collateral = request.collateral_token
        collateral = self._config.collateral_tokens.get(collateral, collateral)
        if not collateral:
            collateral = self._config.collateral_tokens[self._config.collateral_symbol]
        return replace(request, collateral_token=collateral)

    async def _market_payload(self, market_id: str) -> dict[str, Any]:
        payload = await self._request_json(
            "GET",
            f"/markets/{market_id}",
            query_params={
                "network_id": str(self._config.chain_id),
                "trading_model": "ob",
            },
        )
        return _myriad_data_mapping(payload)

    async def _order_book_fee_payload(self, market_id: int) -> dict[str, Any]:
        now = time.monotonic()
        cached = self._market_fee_payload_cache.get(market_id)
        if cached is not None and now - self._market_fee_catalog_cached_at <= MARKET_CONSTRAINTS_TTL_SECONDS:
            return cached

        async with self._market_fee_catalog_lock:
            now = time.monotonic()
            cached = self._market_fee_payload_cache.get(market_id)
            if cached is not None and now - self._market_fee_catalog_cached_at <= MARKET_CONSTRAINTS_TTL_SECONDS:
                return cached

            fee_payloads: dict[int, dict[str, Any]] = {}
            page = 1
            while True:
                payload = await self._request_json(
                    "GET",
                    "/markets",
                    query_params={
                        "network_id": str(self._config.chain_id),
                        "trading_model": "ob",
                        "state": "open",
                        "page": str(page),
                        "limit": "100",
                    },
                )
                items = _myriad_market_items(payload)
                for market in items:
                    raw_market_id = market.get("id")
                    try:
                        parsed_market_id = int(str(raw_market_id))
                    except (TypeError, ValueError):
                        continue
                    fee_payloads[parsed_market_id] = market
                if not _myriad_has_next_page(payload, page, len(items)):
                    break
                page += 1

            self._market_fee_payload_cache = fee_payloads
            self._market_fee_catalog_cached_at = time.monotonic()
            cached = fee_payloads.get(market_id)
            if cached is not None:
                return cached
        raise RuntimeError(f"Myriad order-book fee metadata is unavailable for market {market_id}")

    async def _has_redeemable_position(self, request: SettlementRequest) -> bool:
        account = self._account_address()
        if not account:
            raise RuntimeError("signing key is unavailable")
        payload = await self._request_json(
            "GET",
            f"/users/{account}/portfolio",
            query_params={
                "network_id": str(self._config.chain_id),
                "market_id": request.market_id,
                "trading_model": "ob",
                "status": "all",
                "exclude_history": "false",
                "page": "1",
                "limit": "100",
            },
        )
        return _payload_has_claimable_winnings(payload)

    async def get_market_constraints(self, token_id: str, condition_id: str | None = None) -> MarketConstraints | None:
        del condition_id
        market_id, _ = _parse_token_id(token_id)
        cached = self._market_constraints_cache.get(market_id)
        now = time.monotonic()
        if cached is not None and now - cached[0] <= MARKET_CONSTRAINTS_TTL_SECONDS:
            return cached[1]
        payload = await self._order_book_fee_payload(market_id)
        peak_fee_bps = _myriad_peak_fee_bps(payload)
        constraints = MarketConstraints(
            fee_rate_bps=peak_fee_bps,
            tick_size=Decimal("0.01"),
            lot_size=Decimal(1) / (Decimal(10) ** SHARE_DECIMALS),
            minimum_notional=Decimal("1"),
        )
        self._market_constraints_cache[market_id] = (now, constraints)
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
            "Myriad",
            resolved.fee_rate_bps,
            "myriad_curve",
            source="myriad_order_book_market_fee",
            verified=True,
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
        del condition_id, tick_size, neg_risk
        if not self._config.private_key:
            return None
        market_id, _ = _parse_token_id(token_id)
        signed = await self.sign_order(market_id, _outcome_id(side), 0, float(contracts), float(max_price))
        return hashlib.sha256(repr(signed).encode("utf-8")).hexdigest()

    def forget_order(self, order_id: str) -> None:
        self._order_amounts.pop(order_id, None)
        self._order_prices.pop(order_id, None)
        self._signed_orders.pop(order_id, None)

    async def place_order(
        self,
        signed_order: MyriadSignedOrder,
        *,
        time_in_force: str = "FAK",
        pre_transport_guard: Callable[[], None] | None = None,
    ) -> str:
        try:
            import aiohttp

            _ = aiohttp
        except ImportError as exc:
            raise RuntimeError("aiohttp is required for Myriad connectivity") from exc

        if time_in_force not in {"GTC", "GTD", "FOK", "FAK", "PO"}:
            raise ValueError("time_in_force must be GTC, GTD, FOK, FAK, or PO")
        payload = {
            "order": signed_order.order,
            "signature": signed_order.signature,
            "network_id": self._config.chain_id,
            "time_in_force": time_in_force,
        }
        url = f"{self._config.api_url.rstrip('/')}/orders"
        session = self._get_rest_session()
        if pre_transport_guard is not None:
            pre_transport_guard()
        async with session.post(url, json=payload, timeout=10) as response:
            response.raise_for_status()
            raw = await response.json()
        order_id = _extract_first_nested(raw, ("orderHash", "order_id", "orderId", "id", "hash"))
        if not order_id:
            raise RuntimeError(f"Myriad order response does not include an order id: {raw!r}")
        normalized_order_id = str(order_id)
        self._signed_orders[normalized_order_id] = signed_order
        self._order_amounts.setdefault(
            normalized_order_id,
            float(int(signed_order.order["amount"])) / float(10**SHARE_DECIMALS),
        )
        self._order_prices.setdefault(
            normalized_order_id,
            float(int(signed_order.order["price"])) / float(10**PRICE_DECIMALS),
        )
        return normalized_order_id

    async def sign_order(
        self, market_id: int, outcome_id: int, side: int, contracts: float, price: float
    ) -> MyriadSignedOrder:
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
            "price": _to_units(
                float(quantize_down(price, "0.01") if side == 0 else quantize_up(price, "0.01")),
                PRICE_DECIMALS,
            ),
            "minFillAmount": 0,
            "nonce": await self._next_nonce(),
            "expiration": 0,
        }
        if not 1 <= eip712_order["price"] <= 10**PRICE_DECIMALS:
            raise ValueError("Myriad order price must be between 0 and 1")
        if eip712_order["price"] % PRICE_TICK_UNITS != 0:
            raise ValueError("Myriad order price must use the 0.01 tick size")
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
                rpc_url=self._config.rpc_urls or self._config.rpc_url,
                chain_id=self._config.chain_id,
                private_key=self._config.private_key,
                max_priority_fee_gwei=self._config.max_priority_fee_gwei,
                confirmations=self._config.confirmations,
            )
        return self._web3_client

    async def _next_nonce(self) -> int:
        async with self._nonce_lock:
            self._nonce += 1
            return self._nonce

    async def _get_collateral_decimals(self, token: Any) -> int:
        if self._collateral_decimals is None:
            raw_decimals = await token.functions.decimals().call()
            self._collateral_decimals = int(raw_decimals)
        return self._collateral_decimals

    def _account_address(self) -> str | None:
        account = self._get_web3_client().account
        return account.address if account is not None else None

    async def _reset_rest_session(self) -> None:
        if self._rest_session is not None and not self._rest_session.closed:
            await self._rest_session.close()
        self._rest_session = None

    async def _request_json(
        self,
        method: str,
        path: str,
        *,
        query_params: dict[str, str] | None = None,
        json_body: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        try:
            import aiohttp
        except ImportError as exc:
            raise RuntimeError("aiohttp is required for Myriad connectivity") from exc
        url = f"{self._config.api_url.rstrip('/')}/{path.lstrip('/')}"
        timeout = aiohttp.ClientTimeout(total=20, connect=10, sock_read=15)
        for attempt in range(2):
            session = self._get_rest_session()
            try:
                request_kwargs: dict[str, Any] = {"params": query_params, "timeout": timeout}
                if json_body is not None:
                    request_kwargs["json"] = json_body
                async with session.request(method, url, **request_kwargs) as response:
                    response.raise_for_status()
                    payload = await response.json()
                break
            except asyncio.CancelledError:
                raise
            except (TimeoutError, aiohttp.ClientError):
                await self._reset_rest_session()
                if attempt == 1:
                    raise
        if not isinstance(payload, dict):
            raise RuntimeError(f"Myriad API returned unsupported payload: {payload!r}")
        return payload


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
        raw_payload=payload,
        timestamp=event_timestamp(payload),
        sequence=event_sequence(payload),
        checksum=event_checksum(payload),
    )


def _parse_orderbook_channel(channel: str) -> tuple[int, int] | None:
    parts = channel.split(":")
    if len(parts) != 3 or parts[0] != "orderbook":
        return None
    try:
        return int(parts[1]), int(parts[2])
    except ValueError:
        return None


def _payload_matches_channel(data: dict[str, Any], network_id: int, market_id: int) -> bool:
    raw_network_id = data.get("networkId") if data.get("networkId") is not None else data.get("network_id")
    raw_market_id = data.get("marketId") if data.get("marketId") is not None else data.get("market_id")
    if raw_network_id is None or raw_market_id is None:
        return False
    try:
        return int(raw_network_id) == network_id and int(raw_market_id) == market_id
    except (TypeError, ValueError):
        return False


def _is_not_found_error(exc: Exception) -> bool:
    status = getattr(exc, "status", None)
    return status == 404 or "404" in str(exc)


def _myriad_data_mapping(payload: dict[str, Any]) -> dict[str, Any]:
    data = payload.get("data")
    if isinstance(data, dict):
        return data
    result = payload.get("result")
    if isinstance(result, dict):
        return result
    return payload


def _myriad_market_items(payload: dict[str, Any]) -> list[dict[str, Any]]:
    for key in ("data", "markets", "items", "results"):
        value = payload.get(key)
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
        if isinstance(value, dict):
            nested = _myriad_market_items(value)
            if nested:
                return nested
    return []


def _myriad_has_next_page(payload: dict[str, Any], current_page: int, item_count: int) -> bool:
    pagination = payload.get("pagination")
    if isinstance(pagination, dict):
        has_next = pagination.get("hasNext")
        if isinstance(has_next, bool):
            return has_next
        total_pages = pagination.get("totalPages")
        if total_pages is not None:
            try:
                return current_page < int(str(total_pages))
            except (TypeError, ValueError):
                pass
    return item_count >= 100


def _myriad_settlement_status(payload: dict[str, Any]) -> SettlementStatus:
    """Map documented OB market state without relying on legacy CTF condition IDs."""
    values = {
        str(payload.get(key) or "").strip().lower()
        for key in ("state", "status", "marketStatus", "market_status")
    }
    if values & {"void", "voided", "cancelled", "canceled", "invalid"}:
        return SettlementStatus.VOID
    if values & {"resolved", "closed", "finalized", "settled"}:
        return SettlementStatus.RESOLVED
    resolution_keys = ("resolvedAt", "resolved_at", "winningOutcome", "winning_outcome")
    if any(payload.get(key) not in (None, "") for key in resolution_keys):
        return SettlementStatus.RESOLVED
    return SettlementStatus.OPEN


def _myriad_claim_transaction(payload: dict[str, Any], sender: str, gas_limit: int) -> dict[str, Any]:
    claim = _myriad_data_mapping(payload)
    target = claim.get("to") or claim.get("target") or claim.get("contractAddress")
    calldata = claim.get("calldata") or claim.get("data")
    if not isinstance(target, str) or not target.startswith("0x"):
        raise RuntimeError("Myriad redeem response does not include a transaction target")
    if not isinstance(calldata, str) or not calldata.startswith("0x"):
        raise RuntimeError("Myriad redeem response does not include calldata")
    raw_value = claim.get("value", 0)
    try:
        value = int(str(raw_value), 0)
    except (TypeError, ValueError) as exc:
        raise RuntimeError("Myriad redeem response includes an invalid transaction value") from exc
    if value < 0:
        raise RuntimeError("Myriad redeem response includes a negative transaction value")
    return {"from": sender, "to": target, "data": calldata, "value": value, "gas": gas_limit}


def _payload_has_claimable_winnings(payload: Any) -> bool:
    if isinstance(payload, dict):
        for key in ("winningsToClaim", "voidedWinningsToClaim"):
            if payload.get(key) is True:
                return True
        return any(_payload_has_claimable_winnings(value) for value in payload.values())
    if isinstance(payload, list):
        return any(_payload_has_claimable_winnings(value) for value in payload)
    return False


def _apply_orderbook_changes(
    book: OrderBook,
    changes: list[Any],
    outcome_side: BinarySide | None,
) -> OrderBook:
    bids = {level.price: level.size for level in book.bids}
    asks = {level.price: level.size for level in book.asks}
    expected_outcome = _outcome_id(outcome_side) if outcome_side is not None else None
    for raw in changes:
        if not isinstance(raw, dict):
            continue
        raw_outcome = raw.get("outcome") if raw.get("outcome") is not None else raw.get("outcomeId")
        if expected_outcome is not None and raw_outcome is not None and int(raw_outcome) != expected_outcome:
            continue
        level = _level(raw)
        if level is None:
            continue
        side = str(raw.get("side") or raw.get("book_side") or "").upper()
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
        bids=sorted(
            (OrderBookLevel(price, size) for price, size in bids.items()), key=lambda item: item.price, reverse=True
        ),
        asks=sorted((OrderBookLevel(price, size) for price, size in asks.items()), key=lambda item: item.price),
        raw_payload={"changes": changes},
        sequence=next_sequence if next_sequence is not None else book.sequence,
        status=MarketDataStatus.VALID if valid_sequence else MarketDataStatus.INVALID,
    )


def _level(payload: Any) -> OrderBookLevel | None:
    if isinstance(payload, dict):
        price = payload.get("price")
        size = payload.get("size")
        if size is None:
            size = payload.get("quantity")
        if size is None:
            size = payload.get("amount")
    elif isinstance(payload, (list, tuple)) and len(payload) >= 2:
        price, size = payload[0], payload[1]
    else:
        return None
    if price is None or size is None:
        return None
    normalized_price = _normalize_price(float(str(price)))
    normalized_size = _normalize_share_amount(float(str(size)))
    return OrderBookLevel(normalized_price, normalized_size)


def _outcome_id(side: BinarySide) -> int:
    return 0 if side is BinarySide.YES else 1


def _orderbook_query_params(chain_id: int, outcome_id: int) -> dict[str, int | str]:
    return {"network_id": chain_id, "outcome": outcome_id, "trading_model": "ob"}


def _parse_token_id(token_id: str) -> tuple[int, BinarySide | None]:
    if ":" not in token_id:
        return int(token_id), None
    market_id, raw_side = token_id.split(":", 1)
    return int(market_id), BinarySide(raw_side)


def _to_units(value: float, decimals: int) -> int:
    scale = Decimal(10) ** decimals
    return int(Decimal(str(value)) * scale)


def _json_loads(payload: str | bytes) -> Any:
    try:
        import orjson
    except ImportError:
        import json

        return json.loads(payload)
    return orjson.loads(payload)


def _api_order_payload(order: dict[str, Any]) -> dict[str, Any]:
    uint_fields = {"marketId", "amount", "price", "minFillAmount", "nonce", "expiration"}
    return {key: str(value) if key in uint_fields else value for key, value in order.items()}


def _myriad_peak_fee_bps(payload: dict[str, Any]) -> int:
    candidates: list[dict[str, Any]] = [payload]
    for key in ("fees", "fee", "feeSchedule", "fee_schedule"):
        nested = payload.get(key)
        if isinstance(nested, dict):
            candidates.append(nested)
        elif isinstance(nested, list):
            candidates.extend(item for item in nested if isinstance(item, dict))
    for candidate in candidates:
        for key in (
            "peakBPS",
            "peakBps",
            "peak_bps",
            "takerFeeBPS",
            "takerFeeBps",
            "taker_fee_bps",
        ):
            value = candidate.get(key)
            if value is None:
                continue
            bps = int(Decimal(str(value)))
            if 0 <= bps < 10_000:
                return bps
        for key in ("takerFeeBpsArray", "taker_fee_bps_array"):
            values = candidate.get(key)
            if not isinstance(values, list) or not values:
                continue
            bps_values = [int(Decimal(str(value))) for value in values]
            if any(value < 0 or value >= 10_000 for value in bps_values):
                raise RuntimeError("Myriad taker fee schedule contains an invalid BPS value")
            return max(bps_values)
    raise RuntimeError("Myriad market metadata does not include the taker peak fee schedule")


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


def _normalize_order_amount(value: float, requested: float) -> float:
    if requested > 0 and value > requested * 1_000:
        return value / float(10**SHARE_DECIMALS)
    return value


def _normalize_share_amount(value: float) -> float:
    return value / float(10**SHARE_DECIMALS) if abs(value) >= 10**12 else value


def _extract_avg_price(payload: Any) -> float | None:
    value = _extract_first_nested(payload, ("avgPrice", "averagePrice", "avg_price", "average_price", "price"))
    if value in (None, ""):
        shares = _extract_decimal(payload, ("shares",))
        total_value = _extract_decimal(payload, ("value",))
        if shares is None or shares == Decimal(0) or total_value is None:
            return None
        return float(abs(total_value / shares))
    try:
        return float(str(value))
    except (TypeError, ValueError):
        return None


def _normalize_price(value: float) -> float:
    return value / float(10**PRICE_DECIMALS) if value > 1.0 else value


def _extract_requested_amount(payload: Any) -> float:
    value = _extract_first_nested(payload, ("amount", "quantity", "originalAmount", "original_amount"))
    if value in (None, ""):
        return 0.0
    return _normalize_share_amount(float(str(value)))


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


def _extract_decimal(payload: Any, keys: tuple[str, ...]) -> Decimal | None:
    value = _extract_first_nested(payload, keys)
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None


def _venue_order_from_payload(payload: dict[str, Any]) -> VenueOrder:
    order_id = str(_extract_first_nested(payload, ("orderHash", "hash", "orderId", "id")) or "")
    quantity = Decimal(str(_extract_requested_amount(payload)))
    filled = Decimal(str(_normalize_share_amount(_extract_filled_amount(payload) or 0.0)))
    status = str(_extract_first_nested(payload, ("status", "state")) or "open").lower()
    normalized = OrderIntentStatus.PARTIAL if filled > 0 else OrderIntentStatus.ACKNOWLEDGED
    if status in {"filled", "matched", "completed"}:
        normalized = OrderIntentStatus.FILLED
    return VenueOrder(
        client_order_id="",
        venue_order_id=order_id,
        venue="Myriad",
        status=normalized,
        quantity=quantity,
        cumulative_filled=filled,
        average_price=Decimal(str(_normalize_price(_extract_avg_price(payload) or 0.0))),
        updated_at=datetime.now(UTC),
    )


def _fill_from_trade(payload: dict[str, Any]) -> FillRecord:
    fill_id = str(_extract_first_nested(payload, ("id", "tradeId", "trade_id", "fillId", "fill_id")) or "")
    order_id = str(_extract_first_nested(payload, ("orderHash", "orderId", "order_id", "hash")) or fill_id)
    quantity = _extract_decimal(payload, ("shares",))
    if quantity is None:
        quantity = Decimal(str(_normalize_share_amount(_extract_filled_amount(payload) or 0.0)))
    fee = _extract_decimal(payload, ("fee", "feeAmount", "fee_amount")) or Decimal(0)
    return FillRecord(
        fill_id=fill_id,
        client_order_id="",
        venue_order_id=order_id,
        venue="Myriad",
        quantity=quantity,
        price=Decimal(str(_normalize_price(_extract_avg_price(payload) or 0.0))),
        fee=fee,
        occurred_at=datetime.fromtimestamp(event_timestamp(payload), tz=UTC),
    )
