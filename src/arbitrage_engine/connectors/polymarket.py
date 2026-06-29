from __future__ import annotations

import asyncio
import logging
import threading
import time
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, Callable

from arbitrage_engine.conditional_tokens import ConditionalTokensRedemption
from arbitrage_engine.config import PolymarketConfig
from arbitrage_engine.connectors.base import (
    OrderBookStaleException,
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
    RedemptionReport,
    SettlementRequest,
    SettlementStatus,
    VenueOrder,
)
from arbitrage_engine.utils.math import quantize_down, quantize_up

LOGGER = logging.getLogger(__name__)
ORDER_BOOK_MAX_AGE_SECONDS = 0.3
PASSIVE_BOOK_MAX_AGE_SECONDS = 2.0


class PolymarketClobClient(PolymarketClient):
    def __init__(self, config: PolymarketConfig) -> None:
        self._config = config
        self._sdk_client: Any | None = None
        self._sdk_client_lock = threading.Lock()
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
        self._constraints_cache: dict[str, tuple[float, MarketConstraints]] = {}
        self._snapshot_interval_seconds = 30.0
        self._execution_freshness_seconds = PASSIVE_BOOK_MAX_AGE_SECONDS
        self._reconnect_count = 0
        self._sequence_gap_count = 0
        self._snapshot_timeout_count = 0
        self._stale_refresh_attempted_at: dict[str, float] = {}
        self._settlement: ConditionalTokensRedemption | None = None

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
            return self._books[token_id]

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
        async with self._http_semaphore:
            async with session.get(url, params={"token_id": token_id}, timeout=10) as response:
                response.raise_for_status()
                raw: dict[str, Any] = await response.json()
        bids = [OrderBookLevel(float(item["price"]), float(item["size"])) for item in raw.get("bids", [])]
        asks = [OrderBookLevel(float(item["price"]), float(item["size"])) for item in raw.get("asks", [])]
        book = OrderBook(bids=_sorted_bids(bids)[:10], asks=_sorted_asks(asks)[:10], raw_payload=raw)
        self._update_book(token_id, book)
        self._snapshot_timestamps[token_id] = time.monotonic()
        return book

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
            self._start_background_refresh(token_id)
        if self._desired_tokens and (self._ws_task is None or self._ws_task.done()):
            self._ws_task = asyncio.create_task(self._run_order_book_ws())

    def _start_background_refresh(self, token_id: str) -> None:
        task, started = self._ensure_refresh_task(token_id, force=False)
        if task is None or not started:
            return
        task.add_done_callback(lambda done: self._finalize_background_refresh(token_id, done))

    def _finalize_background_refresh(self, token_id: str, task: asyncio.Task[OrderBook]) -> None:
        if self._bootstrap_tasks.get(token_id) is task and task.done():
            self._bootstrap_tasks.pop(token_id, None)
        if task.cancelled():
            return
        exc = task.exception()
        if exc is None:
            return
        self._bootstrap_attempted.discard(token_id)
        LOGGER.debug(
            "polymarket_background_snapshot_failed",
            extra={"_token_id": token_id, "_error": str(exc)},
        )

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
        return max(0.0, time.time() - book.timestamp) <= self._execution_freshness_seconds

    def telemetry_snapshot(self) -> dict[str, float]:
        return {
            "reconnects": float(self._reconnect_count),
            "sequence_gaps": float(self._sequence_gap_count),
            "snapshot_timeouts": float(self._snapshot_timeout_count),
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
        payloads = await asyncio.to_thread(self._sdk_call, lambda client: client.get_trades())
        positions: dict[str, Decimal] = {}
        for item in payloads:
            if not isinstance(item, dict):
                continue
            token_id = str(_extract_first(item, ("asset_id", "assetId", "token_id", "tokenId")) or "")
            if not token_id:
                continue
            amount = Decimal(str(_extract_first(item, ("size", "amount", "quantity")) or "0"))
            side = str(_extract_first(item, ("side", "type")) or "BUY").upper()
            positions[token_id] = positions.get(token_id, Decimal(0)) + (amount if side == "BUY" else -amount)
        return positions

    def supports_full_reconciliation(self) -> bool:
        return True

    async def get_settlement_status(self, request: SettlementRequest) -> SettlementStatus:
        return await self._get_settlement_client().get_settlement_status(self._settlement_request(request))

    def prepare_settlement_request(self, request: SettlementRequest) -> SettlementRequest:
        settlement = self._get_settlement_client()
        if self._config.funder and settlement.signer_address is not None:
            funder = settlement.checksum_address(self._config.funder)
            if funder != settlement.signer_address:
                raise RuntimeError("direct redemption is unsafe when Polymarket funder differs from signer")
        return self._settlement_request(request)

    async def redeem_position(self, request: SettlementRequest, redemption_id: str) -> RedemptionReport:
        return await self._get_settlement_client().redeem_position(self._settlement_request(request), redemption_id)

    async def reconcile_redemption(
        self,
        request: SettlementRequest,
        report: RedemptionReport,
    ) -> RedemptionReport:
        return await self._get_settlement_client().reconcile(self._settlement_request(request), report)

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

    def _settlement_request(self, request: SettlementRequest) -> SettlementRequest:
        return replace(request, collateral_token=request.collateral_token or self._config.collateral_token_address)

    async def get_market_constraints(self, token_id: str, condition_id: str | None = None) -> MarketConstraints | None:
        if not condition_id:
            return None
        cache_key = f"{condition_id}:{token_id}"
        cached = self._constraints_cache.get(cache_key)
        if cached is not None and time.monotonic() - cached[0] < 30.0:
            return cached[1]
        constraints = await asyncio.to_thread(self._get_market_constraints, token_id, condition_id)
        self._constraints_cache[cache_key] = (time.monotonic(), constraints)
        return constraints

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
            else:
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

    def _reset_sdk_client(self) -> None:
        with self._sdk_client_lock:
            self._sdk_client = None

    def _sdk_call(self, operation: Callable[[Any], Any]) -> Any:
        last_error: Exception | None = None
        for attempt in range(2):
            client = self._get_sdk_client()
            try:
                return operation(client)
            except Exception as exc:
                last_error = exc
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
        market = self._sdk_call(lambda current: current.get_market(condition_id))
        resolved_tick_size = tick_size or str(market["minimum_tick_size"])
        resolved_neg_risk = neg_risk if neg_risk is not None else bool(market["neg_risk"])
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
        balance = _find_numeric_balance(result, ("pusd", "pUSD", "USDC", "cash", "balance", "available"))
        if balance is None:
            raise RuntimeError(f"Could not parse Polymarket collateral balance from response: {result!r}")
        return balance

    def _get_market_constraints(self, token_id: str, condition_id: str) -> MarketConstraints:
        client = self._get_sdk_client()
        market = self._sdk_call(lambda current: current.get_market(condition_id))
        tick = Decimal(str(market.get("minimum_tick_size") or market.get("minimumTickSize") or ""))
        minimum_order = Decimal(str(market.get("minimum_order_size") or market.get("minimumOrderSize") or "1"))
        fee_bps = int(round(self._config.trading_fee_pct * 10_000))
        get_fee_rate = getattr(client, "get_fee_rate_bps", None)
        if callable(get_fee_rate):
            try:
                fee_bps = int(get_fee_rate(token_id))
            except Exception:
                LOGGER.exception("polymarket_dynamic_fee_lookup_failed", extra={"_token_id": token_id})
        return MarketConstraints(
            fee_rate_bps=fee_bps,
            tick_size=tick,
            lot_size=minimum_order,
            minimum_notional=Decimal("1"),
        )


def _extract_first(payload: Any, keys: tuple[str, ...]) -> Any:
    if isinstance(payload, dict):
        for key in keys:
            if key in payload:
                return payload[key]
    return None


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
