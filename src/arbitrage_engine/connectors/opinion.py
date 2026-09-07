"""Opinion.trade (https://app.opinion.trade) binary CLOB connector.

Market data comes from the public Opinion OpenAPI plus the ``market.depth.diff``
WebSocket channel. Order submission goes through the Opinion CLOB SDK, which
signs locally; when the SDK or a signing key is missing every submission path
fails closed instead of degrading to an unproven request.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import time
from collections.abc import Awaitable, Callable
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from arbitrage_engine.config import OpinionConfig
from arbitrage_engine.connectors.base import (
    BinaryMarketClient,
    OrderBookStaleException,
    OrderBookUnavailableException,
    OrderSubmissionRejected,
    WebSocketReconnectBackoff,
    event_sequence,
    event_timestamp,
)
from arbitrage_engine.connectors.web3_base import BaseWeb3Client
from arbitrage_engine.external_baseline import account_fingerprint
from arbitrage_engine.http import client_session
from arbitrage_engine.models import (
    BinarySide,
    ExecutionReport,
    FillRecord,
    MarketConstraints,
    MarketDataStatus,
    OrderBook,
    OrderBookLevel,
    OrderIntent,
    OrderIntentStatus,
    VenueFeeQuote,
    VenueOrder,
)

LOGGER = logging.getLogger(__name__)

VENUE_NAME = "Opinion"

_ORDER_BOOK_REQUEST_CONCURRENCY = 8
_MARKET_CONSTRAINTS_TTL_SECONDS = 300.0
_MARKET_METADATA_TTL_SECONDS = 300.0
_PASSIVE_BOOK_MAX_AGE_SECONDS = 2.0
_HEARTBEAT_INTERVAL_SECONDS = 20.0
_DEPTH_CHANNEL = "market.depth.diff"

# Opinion order status codes shared by /order and /trade responses.
_ORDER_STATUS_PENDING = 1
_ORDER_STATUS_FILLED = 2
_ORDER_STATUS_CANCELED = 3
_ORDER_STATUS_EXPIRED = 4
_ORDER_STATUS_FAILED = 5

_TERMINAL_ORDER_STATUSES = {
    _ORDER_STATUS_FILLED,
    _ORDER_STATUS_CANCELED,
    _ORDER_STATUS_EXPIRED,
    _ORDER_STATUS_FAILED,
}

_INTENT_STATUS_BY_CODE = {
    _ORDER_STATUS_PENDING: OrderIntentStatus.ACKNOWLEDGED,
    _ORDER_STATUS_FILLED: OrderIntentStatus.FILLED,
    _ORDER_STATUS_CANCELED: OrderIntentStatus.CANCELLED,
    _ORDER_STATUS_EXPIRED: OrderIntentStatus.CANCELLED,
    _ORDER_STATUS_FAILED: OrderIntentStatus.MANUAL_REVIEW,
}

_EXECUTION_STATUS_BY_CODE = {
    _ORDER_STATUS_PENDING: "open",
    _ORDER_STATUS_FILLED: "filled",
    _ORDER_STATUS_CANCELED: "cancelled",
    _ORDER_STATUS_EXPIRED: "expired",
    _ORDER_STATUS_FAILED: "cancelled",
}

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

# Opinion tags outcomes numerically on every trading surface.
_OUTCOME_SIDE_YES = 1
_OUTCOME_SIDE_NO = 2

# Sentinel for a depth push whose outcome cannot be resolved yet.
_UNRESOLVED_OUTCOME = "\x00unresolved"


class OpinionSubmissionUnknown(RuntimeError):
    """An Opinion submission may or may not have reached the venue.

    Never raise :class:`OrderSubmissionRejected` for these: that type asserts
    proof the venue did not accept the order, and the engine books a terminal
    ``CANCELLED`` with zero fill on it. An ambiguous outcome must instead reach
    the engine as an ordinary failure so the intent settles as ``UNKNOWN`` and
    risk pauses until reconciliation resolves it.
    """


# The SDK validates locally before it signs or sends. These failures happen with
# certainty before any transport, so they are provable rejections; the message
# prefix its own catch-all wrapper adds around the POST is not.
_SDK_PRE_TRANSPORT_ERRORS = frozenset({"InvalidParamError", "BalanceNotEnough", "InsufficientGasBalance"})
_SDK_POST_TRANSPORT_WRAPPER_PREFIX = "Failed to place order:"


def _submission_is_definitively_rejected(exc: BaseException) -> bool:
    """Return whether the venue provably never accepted the order."""
    if isinstance(exc, OrderSubmissionRejected):
        return True
    if type(exc).__name__ in _SDK_PRE_TRANSPORT_ERRORS:
        return True
    if type(exc).__name__ == "OpenApiError":
        # Raised both for pre-flight refusals (wrong chain, unknown quote token)
        # and, behind this prefix, for anything that went wrong at or after the
        # POST. Only the former is proof.
        return _SDK_POST_TRANSPORT_WRAPPER_PREFIX not in str(exc)
    return False


class OpinionMarketMetadata:
    """Cached market descriptor needed to route an execution token."""

    __slots__ = ("market_id", "condition_id", "yes_token_id", "no_token_id", "fetched_at")

    def __init__(
        self,
        market_id: int,
        condition_id: str | None,
        yes_token_id: str | None,
        no_token_id: str | None,
    ) -> None:
        self.market_id = market_id
        self.condition_id = condition_id
        self.yes_token_id = yes_token_id
        self.no_token_id = no_token_id
        self.fetched_at = time.monotonic()

    def side_for_token(self, token_id: str) -> BinarySide | None:
        if self.yes_token_id and token_id == self.yes_token_id:
            return BinarySide.YES
        if self.no_token_id and token_id == self.no_token_id:
            return BinarySide.NO
        return None


class OpinionClient(BinaryMarketClient):
    """Binary market client for Opinion.trade.

    Execution tokens use the ``"<marketId>:<tokenId>"`` composite form so a
    single identifier carries both the WebSocket subscription key and the
    ERC-1155 outcome token the CLOB settles against.
    """

    venue_name = VENUE_NAME

    def __init__(self, config: OpinionConfig) -> None:
        self._config = config
        self._books: dict[str, OrderBook] = {}
        self._book_timestamps: dict[str, float] = {}
        self._snapshot_timestamps: dict[str, float] = {}
        self._bootstrap_tasks: dict[str, asyncio.Task[OrderBook]] = {}
        self._order_book_request_semaphore = asyncio.Semaphore(_ORDER_BOOK_REQUEST_CONCURRENCY)
        self._rest_session: Any | None = None
        self._ws_session: Any | None = None
        self._ws: Any | None = None
        self._ws_task: asyncio.Task[None] | None = None
        self._heartbeat_task: asyncio.Task[None] | None = None
        self._desired_markets: set[int] = set()
        self._market_tokens: dict[int, set[str]] = {}
        self._subscription_queue: asyncio.Queue[tuple[str, int]] = asyncio.Queue()
        self._ws_connected = False
        self._reconnecting = False
        self._reconnect_backoff = WebSocketReconnectBackoff()
        self._reconnect_count = 0
        self._sequence_gap_count = 0
        self._proactive_refresh_count = 0
        self._proactive_refresh_failure_count = 0
        self._rest_request_count = 0
        self._rest_error_count = 0
        self._snapshot_interval_seconds = 30.0
        self._execution_freshness_seconds = _PASSIVE_BOOK_MAX_AGE_SECONDS
        self._market_metadata: dict[int, OpinionMarketMetadata] = {}
        self._market_constraints: dict[int, tuple[float, MarketConstraints]] = {}
        self._order_amounts: dict[str, Decimal] = {}
        self._order_prices: dict[str, Decimal] = {}
        self._order_tokens: dict[str, str] = {}
        self._order_actions: dict[str, str] = {}
        self._prepared_orders: dict[str, dict[str, Any]] = {}
        self._clob_client: Any | None = None
        self._clob_lock = asyncio.Lock()
        self._web3_client: BaseWeb3Client | None = None
        self._collateral_decimals: int | None = None
        self._public_rate_limiter = _RateLimiter(config.public_request_rate_per_second)
        self._authenticated_rate_limiter = _RateLimiter(config.authenticated_request_rate_per_second)

    # ------------------------------------------------------------------
    # Market data
    # ------------------------------------------------------------------
    async def watch_order_book(self, token_id: str) -> OrderBook:
        market_id, outcome_token = parse_execution_token(token_id)
        self._ensure_token_subscription(token_id, market_id)
        self._ensure_ws_task()
        cached = self._books.get(token_id)
        if cached is not None and cached.status in {MarketDataStatus.INVALID, MarketDataStatus.STALE}:
            return await self._await_bootstrap(token_id, outcome_token, force=True)

        ttl_seconds = self._config.order_book_ttl_ms / 1_000.0
        stale_after_seconds = self._config.websocket_stale_after_ms / 1_000.0
        passive_age_seconds = max(ttl_seconds, stale_after_seconds, self._execution_freshness_seconds)
        if cached is not None:
            age = time.monotonic() - self._book_timestamps.get(token_id, 0.0)
            if age <= ttl_seconds:
                snapshot_at = self._snapshot_timestamps.get(token_id)
                if (
                    cached.sequence is None
                    and snapshot_at is not None
                    and time.monotonic() - snapshot_at >= self._snapshot_interval_seconds
                ):
                    return await self._await_bootstrap(token_id, outcome_token, force=True)
                return cached
            if age <= passive_age_seconds:
                return cached
            reason = "websocket stalled" if age >= stale_after_seconds else "TTL exceeded"
            refreshed = await self._await_bootstrap(token_id, outcome_token, force=True)
            if refreshed.status is MarketDataStatus.VALID:
                return refreshed
            raise OrderBookStaleException(
                f"Opinion order book is stale for token {token_id}: {reason}, age={age:.3f}s"
            )
        return await self._await_bootstrap(token_id, outcome_token, force=False)

    async def prime_funded_market_data_target(self, token_id: str) -> OrderBook:
        market_id, outcome_token = parse_execution_token(token_id)
        self._ensure_token_subscription(token_id, market_id)
        self._ensure_ws_task()
        return await self._await_bootstrap(token_id, outcome_token, force=True)

    async def prime_market_data_targets(self) -> None:
        tokens = sorted(self._active_tokens())
        if not tokens:
            return
        self._ensure_ws_task()
        await asyncio.gather(
            *(self.prime_funded_market_data_target(token_id) for token_id in tokens),
            return_exceptions=True,
        )

    async def refresh_market_data_target(self, token_id: str) -> bool:
        if token_id not in self._active_tokens():
            return False
        _, outcome_token = parse_execution_token(token_id)
        self._proactive_refresh_count += 1
        try:
            book = await self._await_bootstrap(token_id, outcome_token, force=True)
        except (OrderBookStaleException, OrderBookUnavailableException, RuntimeError):
            self._proactive_refresh_failure_count += 1
            return False
        return book.status is MarketDataStatus.VALID

    async def _await_bootstrap(self, token_id: str, outcome_token: str, *, force: bool) -> OrderBook:
        existing = self._bootstrap_tasks.get(token_id)
        if existing is not None and not existing.done() and not force:
            return await asyncio.shield(existing)
        if existing is not None and not existing.done():
            return await asyncio.shield(existing)
        task = asyncio.ensure_future(self._fetch_and_store_order_book(token_id, outcome_token))
        self._bootstrap_tasks[token_id] = task
        try:
            return await asyncio.shield(task)
        finally:
            if self._bootstrap_tasks.get(token_id) is task and task.done():
                self._bootstrap_tasks.pop(token_id, None)

    async def _fetch_and_store_order_book(self, token_id: str, outcome_token: str) -> OrderBook:
        async with self._order_book_request_semaphore:
            payload = await self._request_result(
                "GET",
                "/token/orderbook",
                query_params={"token_id": outcome_token},
                authenticated=False,
            )
        book = order_book_from_payload(payload)
        self._snapshot_timestamps[token_id] = time.monotonic()
        self._store_book(token_id, book)
        return book

    def _store_book(self, token_id: str, book: OrderBook) -> None:
        if book.status is MarketDataStatus.INVALID:
            self._sequence_gap_count += 1
        self._books[token_id] = replace(book, timestamp=min(book.timestamp, time.time()))
        self._book_timestamps[token_id] = time.monotonic()

    # ------------------------------------------------------------------
    # WebSocket transport
    # ------------------------------------------------------------------
    def _ensure_ws_task(self) -> None:
        if not self._config.ws_url:
            return
        if self._ws_task is None or self._ws_task.done():
            self._ws_task = asyncio.create_task(self._run_depth_ws())

    async def _run_depth_ws(self) -> None:
        try:
            import aiohttp
        except ImportError:
            return
        while True:
            connected_at: float | None = None
            try:
                session = self._get_ws_session()
                async with session.ws_connect(self._ws_endpoint(), heartbeat=15) as ws:
                    self._ws = ws
                    connected_at = time.monotonic()
                    self._ws_connected = True
                    self._reconnecting = False
                    subscribed: set[int] = set()
                    for market_id in set(self._desired_markets):
                        await ws.send_json(_subscribe_message(market_id))
                        subscribed.add(market_id)
                    sender = asyncio.create_task(self._send_subscriptions(ws, subscribed))
                    heartbeat = asyncio.create_task(self._send_heartbeats(ws))
                    try:
                        async for message in ws:
                            if message.type != aiohttp.WSMsgType.TEXT:
                                continue
                            payload = _json_loads(str(message.data))
                            if isinstance(payload, dict):
                                self._handle_ws_payload(payload)
                    finally:
                        for task in (sender, heartbeat):
                            task.cancel()
                        await asyncio.gather(sender, heartbeat, return_exceptions=True)
            except asyncio.CancelledError:
                raise
            except (aiohttp.ClientConnectionError, ConnectionResetError) as exc:
                LOGGER.info("opinion_ws_disconnected", extra={"_reason": type(exc).__name__})
            except Exception:
                LOGGER.exception("opinion_ws_failed")
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

    async def _send_subscriptions(self, ws: Any, subscribed: set[int]) -> None:
        while True:
            operation, market_id = await self._subscription_queue.get()
            if operation == "subscribe":
                if market_id in subscribed or market_id not in self._desired_markets:
                    continue
                await ws.send_json(_subscribe_message(market_id))
                subscribed.add(market_id)
                continue
            if market_id not in subscribed:
                continue
            await ws.send_json(_unsubscribe_message(market_id))
            subscribed.discard(market_id)

    async def _send_heartbeats(self, ws: Any) -> None:
        while True:
            await asyncio.sleep(_HEARTBEAT_INTERVAL_SECONDS)
            if ws.closed:
                return
            await ws.send_json({"action": "HEARTBEAT"})

    def _handle_ws_payload(self, payload: dict[str, Any]) -> None:
        data = payload.get("data")
        body = data if isinstance(data, dict) else payload
        # Opinion tags the message kind as `msgType`; `channel` only ever appears
        # on our own subscribe frames. Anything that is not a depth update must
        # be ignored rather than parsed as one: a `market.last.price` message
        # carries a price with no book side and would otherwise invalidate the
        # cached book.
        message_type = _optional_str(body.get("msgType")) or _optional_str(payload.get("channel"))
        if message_type is not None and message_type != _DEPTH_CHANNEL:
            return
        market_id = _optional_int(body.get("marketId") or payload.get("marketId"))
        if market_id is None:
            return
        outcome_token = self._push_outcome_token(market_id, body)
        if outcome_token is _UNRESOLVED_OUTCOME:
            return
        for token_id in set(self._market_tokens.get(market_id, set())):
            token_market_id, token_outcome = parse_execution_token(token_id)
            if token_market_id != market_id:
                continue
            if outcome_token is not None and outcome_token != token_outcome:
                continue
            # A depth push carries exactly one level. Only a REST snapshot can
            # seed a book, so an update for a target we have not bootstrapped is
            # dropped rather than mistaken for a complete book.
            cached = self._books.get(token_id)
            if cached is not None:
                self._store_book(token_id, apply_depth_diff(cached, body))

    def _push_outcome_token(self, market_id: int, body: dict[str, Any]) -> str | None:
        """Resolve which outcome token a depth push describes.

        Opinion identifies the outcome by ``tokenId`` and/or the numeric
        ``outcomeSide``. A push that names ``outcomeSide`` without a token is
        only usable once the market's token ids are cached; without them the
        update is dropped rather than applied to a possibly wrong side.
        """
        outcome_token = _optional_str(body.get("tokenId"))
        if outcome_token is not None:
            return outcome_token
        side = side_from_outcome_code(body.get("outcomeSide"))
        if side is None:
            return None
        metadata = self._market_metadata.get(market_id)
        if metadata is None:
            return _UNRESOLVED_OUTCOME
        resolved = metadata.yes_token_id if side is BinarySide.YES else metadata.no_token_id
        return resolved if resolved is not None else _UNRESOLVED_OUTCOME

    def _remember_market_metadata(self, market_id: int, outcome_token: str, side: BinarySide) -> None:
        """Record which side an execution token represents for this market."""
        metadata = self._market_metadata.get(market_id)
        yes_token = metadata.yes_token_id if metadata is not None else None
        no_token = metadata.no_token_id if metadata is not None else None
        if side is BinarySide.YES:
            yes_token = outcome_token
        else:
            no_token = outcome_token
        self._market_metadata[market_id] = OpinionMarketMetadata(
            market_id=market_id,
            condition_id=metadata.condition_id if metadata is not None else None,
            yes_token_id=yes_token,
            no_token_id=no_token,
        )

    def _ws_endpoint(self) -> str:
        base = self._config.ws_url
        if not self._config.api_key:
            return base
        separator = "&" if "?" in base else "?"
        return f"{base}{separator}apikey={self._config.api_key}"

    def _mark_books_stale(self) -> None:
        for token_id, book in list(self._books.items()):
            if book.status is MarketDataStatus.VALID:
                self._books[token_id] = replace(book, status=MarketDataStatus.STALE)

    async def reconnect_market_data(self) -> None:
        task = self._ws_task
        self._ws_task = None
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        ws = self._ws
        self._ws = None
        if ws is not None and not ws.closed:
            await ws.close()
        await self._close_ws_session()
        self._mark_books_stale()
        self._ensure_ws_task()

    # ------------------------------------------------------------------
    # Target bookkeeping
    # ------------------------------------------------------------------
    def _ensure_token_subscription(self, token_id: str, market_id: int) -> None:
        tokens = self._market_tokens.setdefault(market_id, set())
        tokens.add(token_id)
        if market_id not in self._desired_markets:
            self._desired_markets.add(market_id)
            self._subscription_queue.put_nowait(("subscribe", market_id))

    def _remove_token_subscription(self, token_id: str) -> None:
        market_id, _ = parse_execution_token(token_id)
        tokens = self._market_tokens.get(market_id)
        if tokens is None:
            return
        tokens.discard(token_id)
        if tokens:
            return
        self._market_tokens.pop(market_id, None)
        if market_id in self._desired_markets:
            self._desired_markets.discard(market_id)
            self._subscription_queue.put_nowait(("unsubscribe", market_id))

    def sync_market_data_targets(self, token_ids: set[str]) -> None:
        current = self._active_tokens()
        for token_id in current - token_ids:
            self._remove_token_subscription(token_id)
            self._books.pop(token_id, None)
            self._book_timestamps.pop(token_id, None)
            self._snapshot_timestamps.pop(token_id, None)
            task = self._bootstrap_tasks.pop(token_id, None)
            if task is not None and not task.done():
                task.cancel()
        for token_id in token_ids - current:
            market_id, _ = parse_execution_token(token_id)
            self._ensure_token_subscription(token_id, market_id)

    def _active_tokens(self) -> set[str]:
        return {token_id for tokens in self._market_tokens.values() for token_id in tokens}

    def has_active_market_data_targets(self) -> bool:
        return bool(self._active_tokens())

    def active_market_data_target_count(self) -> int:
        return len(self._active_tokens())

    def market_data_age_seconds(self) -> float | None:
        active = self._active_tokens()
        timestamps = [self._book_timestamps[token] for token in active if token in self._book_timestamps]
        if not timestamps:
            return None
        return time.monotonic() - max(timestamps)

    def market_data_target_age_seconds(self, token_id: str) -> float | None:
        stamped = self._book_timestamps.get(token_id)
        if stamped is None:
            return None
        return time.monotonic() - stamped

    def market_data_target_ready(self, token_id: str, max_age_seconds: float) -> bool:
        book = self._books.get(token_id)
        if book is None or book.status is not MarketDataStatus.VALID:
            return False
        age = self.market_data_target_age_seconds(token_id)
        return age is not None and age <= max_age_seconds

    def market_data_ready(self) -> bool:
        active = self._active_tokens()
        if not active:
            return True
        return any(
            (book := self._books.get(token_id)) is not None and book.status is MarketDataStatus.VALID
            for token_id in active
        )

    def set_market_data_snapshot_interval(self, seconds: float) -> None:
        self._snapshot_interval_seconds = max(0.0, seconds)

    def set_market_data_execution_freshness(self, seconds: float) -> None:
        self._execution_freshness_seconds = max(0.0, seconds)

    def telemetry_snapshot(self) -> dict[str, float]:
        return {
            "reconnects": float(self._reconnect_count),
            "sequence_gaps": float(self._sequence_gap_count),
            "proactive_refreshes": float(self._proactive_refresh_count),
            "proactive_refresh_failures": float(self._proactive_refresh_failure_count),
            "rest_requests": float(self._rest_request_count),
            "rest_errors": float(self._rest_error_count),
            "active_targets": float(len(self._active_tokens())),
            "connected": float(self._ws_connected),
            "reconnecting": float(self._reconnecting),
            "reconnect_backoff_seconds": self._reconnect_backoff.current_delay_seconds,
        }

    # ------------------------------------------------------------------
    # Market metadata, constraints and fees
    # ------------------------------------------------------------------
    async def get_market_metadata(self, market_id: int) -> OpinionMarketMetadata | None:
        cached = self._market_metadata.get(market_id)
        if cached is not None and time.monotonic() - cached.fetched_at <= _MARKET_METADATA_TTL_SECONDS:
            return cached
        payload = await self._request_result("GET", f"/market/{market_id}", authenticated=False)
        if not isinstance(payload, dict):
            return None
        metadata = OpinionMarketMetadata(
            market_id=market_id,
            condition_id=_optional_str(payload.get("conditionId")),
            yes_token_id=_optional_str(payload.get("yesTokenId")),
            no_token_id=_optional_str(payload.get("noTokenId")),
        )
        self._market_metadata[market_id] = metadata
        return metadata

    async def get_market_constraints(
        self,
        token_id: str,
        condition_id: str | None = None,
    ) -> MarketConstraints | None:
        del condition_id
        market_id, _ = parse_execution_token(token_id)
        cached = self._market_constraints.get(market_id)
        now = time.monotonic()
        if cached is not None and now - cached[0] <= _MARKET_CONSTRAINTS_TTL_SECONDS:
            return cached[1]
        precision = max(1, self._config.price_precision)
        constraints = MarketConstraints(
            fee_rate_bps=self._config.taker_fee_rate_bps,
            tick_size=Decimal(1).scaleb(-precision),
            lot_size=Decimal("0.000001"),
            minimum_notional=Decimal(str(self._config.minimum_notional_usd)),
        )
        self._market_constraints[market_id] = (now, constraints)
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
            VENUE_NAME,
            resolved.fee_rate_bps,
            "opinion_curve",
            source="opinion_config_taker_curve",
            verified=True,
            minimum_fee_usd=Decimal(str(self._config.minimum_fee_usd)),
        )

    # ------------------------------------------------------------------
    # Order submission
    # ------------------------------------------------------------------
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
        return await self._submit_order(token_id, side, Decimal(str(contracts)), Decimal(str(max_price)), "BUY")

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
        return await self._submit_order(token_id, side, Decimal(str(contracts)), Decimal(str(min_price)), "SELL")

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
        del client_order_id, condition_id, tick_size, neg_risk
        if submission_deadline_unix is not None and time.time() > submission_deadline_unix:
            self.release_prepared_order(prepared_order_fingerprint)
            raise OrderSubmissionRejected("Opinion submission deadline elapsed before transport")
        if pre_transport_guard is not None:
            pre_transport_guard()
        order_id = await self._submit_order(
            token_id,
            side,
            Decimal(str(contracts)),
            Decimal(str(max_price)),
            "BUY",
            prepared_order_fingerprint=prepared_order_fingerprint,
        )
        await persist_order_id(order_id)
        return order_id

    async def _submit_order(
        self,
        token_id: str,
        side: BinarySide,
        contracts: Decimal,
        limit_price: Decimal,
        action: str,
        *,
        prepared_order_fingerprint: str | None = None,
    ) -> str:
        market_id, outcome_token = parse_execution_token(token_id)
        if not self._config.private_key:
            self.release_prepared_order(prepared_order_fingerprint)
            raise OrderSubmissionRejected("OPINION_PRIVATE_KEY is required to submit Opinion orders")
        if not self._config.api_key:
            self.release_prepared_order(prepared_order_fingerprint)
            raise OrderSubmissionRejected("OPINION_API_KEY is required to submit Opinion orders")
        client = await self._get_clob_client()
        payload = build_order_payload(
            market_id=market_id,
            outcome_token=outcome_token,
            action=action,
            contracts=contracts,
            limit_price=limit_price,
            price_precision=self._config.price_precision,
        )
        try:
            result = await asyncio.to_thread(client.place_order, _place_order_input(payload))
        except Exception as exc:  # noqa: BLE001 - the venue SDK raises bare errors
            self.release_prepared_order(prepared_order_fingerprint)
            if _submission_is_definitively_rejected(exc):
                raise OrderSubmissionRejected(f"Opinion order submission rejected: {exc}") from exc
            # The SDK funnels every failure past its own POST — transport
            # timeouts included — through one wrapper. Claiming proof of
            # rejection here would book a CANCELLED leg for an order that may be
            # live and unhedged, so surface it as unknown and let the engine
            # pause risk for reconciliation.
            raise OpinionSubmissionUnknown(f"Opinion order submission outcome is unknown: {exc}") from exc
        order_id = _extract_order_id(result)
        if not order_id:
            self.release_prepared_order(prepared_order_fingerprint)
            raise OpinionSubmissionUnknown(
                f"Opinion accepted the order but returned no usable order id: {result!r}"
            )
        self._order_amounts[order_id] = contracts
        self._order_prices[order_id] = limit_price
        self._order_tokens[order_id] = token_id
        self._order_actions[order_id] = action.upper()
        self._remember_market_metadata(market_id, outcome_token, side)
        self._prepared_orders.pop(prepared_order_fingerprint or "", None)
        return order_id

    def persists_order_id_before_submission(self) -> bool:
        """Accepted residual risk: the order id is only known after the POST.

        SX V3 can return ``True`` because it derives the order id as the EIP-712
        digest of the order it signed locally, before any network call. The
        Opinion SDK signs inside ``_place_order`` and never exposes that digest,
        returning only the parsed API response, so there is no venue-agreed id
        to persist beforehand without reimplementing its signing.

        The consequence is a narrow window: if the process dies between the POST
        leaving and the id being persisted, the durable intent has no
        ``venue_order_id`` and reconciliation escalates it to MANUAL_REVIEW with
        a global risk pause. Reporting ``True`` here would be a lie that also
        skips the post-persist re-validation in ExecutionRouter, so it stays
        ``False`` until the SDK exposes the signed digest.
        """
        return False

    async def _get_clob_client(self) -> Any:
        async with self._clob_lock:
            if self._clob_client is not None:
                return self._clob_client
            try:
                from opinion_clob_sdk import Client
            except ImportError as exc:
                raise OrderSubmissionRejected(
                    "opinion_clob_sdk is required to submit Opinion orders"
                ) from exc
            client = Client(
                host=self._config.clob_host,
                apikey=self._config.api_key,
                chain_id=self._config.chain_id,
                rpc_url=self._config.rpc_url,
                private_key=self._config.private_key,
                multi_sig_addr=self._config.multi_sig_address,
                conditional_tokens_addr=self._config.conditional_tokens_address,
                multisend_addr=self._config.multisend_address,
            )
            await asyncio.to_thread(client.enable_trading)
            self._clob_client = client
            return client

    def claim_prepared_order(
        self,
        fingerprint: str | None,
        *,
        token_id: str,
        side: BinarySide,
        contracts: Decimal,
        limit_price: Decimal,
        action: str,
        submission_deadline_unix: float | None = None,
    ) -> str | None:
        del side, submission_deadline_unix
        if fingerprint is None:
            return None
        market_id, outcome_token = parse_execution_token(token_id)
        expected = _payload_fingerprint(
            build_order_payload(
                market_id=market_id,
                outcome_token=outcome_token,
                action=action,
                contracts=contracts,
                limit_price=limit_price,
                price_precision=self._config.price_precision,
            )
        )
        if expected != fingerprint:
            return None
        self._prepared_orders[fingerprint] = {"token_id": token_id, "action": action.upper()}
        return fingerprint

    def release_prepared_order(self, fingerprint: str | None) -> None:
        if fingerprint is not None:
            self._prepared_orders.pop(fingerprint, None)

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
        del side, condition_id, tick_size, neg_risk
        if not self._config.private_key or not self._config.api_key:
            return None
        market_id, outcome_token = parse_execution_token(token_id)
        return _payload_fingerprint(
            build_order_payload(
                market_id=market_id,
                outcome_token=outcome_token,
                action="BUY",
                contracts=contracts,
                limit_price=max_price,
                price_precision=self._config.price_precision,
            )
        )

    # ------------------------------------------------------------------
    # Order lifecycle
    # ------------------------------------------------------------------
    async def wait_filled(self, order_id: str, timeout_ms: int) -> ExecutionReport:
        deadline = time.monotonic() + timeout_ms / 1_000.0
        requested = self._order_amounts.get(order_id, Decimal(0))
        last_filled = Decimal(0)
        last_price = self._order_prices.get(order_id, Decimal(0))
        last_status = "open"
        while True:
            payload = await self._order_payload(order_id)
            status_code = _optional_int(payload.get("status"))
            filled = _decimal_field(payload, ("filledShares", "shares", "filledAmount")) or Decimal(0)
            price = _decimal_field(payload, ("avgPrice", "price"))
            if price is not None:
                last_price = price
            last_filled = max(last_filled, filled)
            if status_code is not None:
                last_status = _EXECUTION_STATUS_BY_CODE.get(status_code, last_status)
                if status_code in _TERMINAL_ORDER_STATUSES:
                    resolved = requested if status_code == _ORDER_STATUS_FILLED and not last_filled else last_filled
                    return ExecutionReport.from_amounts(order_id, requested, resolved, last_status, last_price)
            if time.monotonic() >= deadline:
                return ExecutionReport.from_amounts(order_id, requested, last_filled, last_status, last_price)
            await asyncio.sleep(0.2)

    async def get_order(self, order_id: str) -> ExecutionReport:
        payload = await self._order_payload(order_id)
        requested = self._order_amounts.get(order_id) or _decimal_field(payload, ("shares", "amount")) or Decimal(0)
        filled = _decimal_field(payload, ("filledShares", "filledAmount")) or Decimal(0)
        status_code = _optional_int(payload.get("status"))
        status = _EXECUTION_STATUS_BY_CODE.get(status_code or _ORDER_STATUS_PENDING, "open")
        price = _decimal_field(payload, ("avgPrice", "price")) or self._order_prices.get(order_id, Decimal(0))
        return ExecutionReport.from_amounts(order_id, requested, filled, status, price)

    async def _order_payload(self, order_id: str) -> dict[str, Any]:
        payload = await self._request_result("GET", f"/order/{order_id}", authenticated=True)
        if isinstance(payload, dict):
            nested = payload.get("order")
            if isinstance(nested, dict):
                return nested
            return payload
        return {}

    async def cancel_order(self, order_id: str) -> None:
        client = await self._get_clob_client()
        cancel = getattr(client, "cancel_order", None)
        if cancel is None:
            raise RuntimeError("opinion_clob_sdk does not expose cancel_order")
        await asyncio.to_thread(cancel, order_id)

    async def restore_order_context(self, order_id: str, intent: OrderIntent) -> None:
        self._remember_order_context(order_id, intent)

    async def restore_fill_context(self, order_id: str, intent: OrderIntent) -> None:
        self._remember_order_context(order_id, intent)

    def _remember_order_context(self, order_id: str, intent: OrderIntent) -> None:
        """Rebuild the local order bookkeeping a durable intent already proves."""
        self._order_amounts.setdefault(order_id, intent.quantity)
        self._order_prices.setdefault(order_id, intent.limit_price)
        self._order_tokens.setdefault(order_id, intent.token_id)
        self._order_actions.setdefault(order_id, intent.action.upper())
        try:
            market_id, outcome_token = parse_execution_token(intent.token_id)
        except ValueError:
            return
        self._remember_market_metadata(market_id, outcome_token, intent.binary_side)

    def order_action(self, order_id: str) -> str | None:
        """Return the submitted action for an order the connector still tracks."""
        return self._order_actions.get(order_id)

    def order_token(self, order_id: str) -> str | None:
        """Return the execution token an order was submitted against."""
        return self._order_tokens.get(order_id)

    def forget_order(self, order_id: str) -> None:
        self._order_amounts.pop(order_id, None)
        self._order_prices.pop(order_id, None)
        self._order_tokens.pop(order_id, None)
        self._order_actions.pop(order_id, None)

    # ------------------------------------------------------------------
    # Account state
    # ------------------------------------------------------------------
    async def get_cash_balance(self) -> float:
        return float((await self.get_cash_balance_details())["balance"])

    async def get_cash_balance_details(self) -> dict[str, Any]:
        """Read the tradable collateral balance directly from the chain.

        Opinion settles through a Safe: the CLOB signs with the EOA derived from
        ``OPINION_PRIVATE_KEY`` but names the Safe as order maker, so the
        spendable collateral sits at the Safe address, not at the signer. Both
        are reported here so an operator can prove the pair belongs together.
        """
        token_address = self._config.collateral_token_address
        if not token_address:
            raise RuntimeError("opinion.collateral_token_address is required for Opinion balance checks")
        wallet_address = self._config.multi_sig_address or self._config.account_address
        if not wallet_address:
            raise RuntimeError("opinion.multi_sig_address is required for Opinion balance checks")
        web3_client = self._get_web3_client()
        token = web3_client.contract(token_address, ERC20_BALANCE_ABI)
        raw_balance = int(await token.functions.balanceOf(wallet_address).call())
        decimals = await self._get_collateral_decimals(token)
        signer = web3_client.account
        return {
            "balance": float(raw_balance) / float(10**decimals),
            "balance_raw": str(raw_balance),
            "decimals": decimals,
            "wallet_address": wallet_address,
            "signer_wallet_address": signer.address if signer is not None else None,
            "collateral_token_address": token_address,
            "collateral_symbol": self._config.collateral_symbol,
        }

    async def get_native_gas_balance(self) -> float:
        """Signer gas balance; redemption and Safe transactions spend from it."""
        return float(await self._get_web3_client().native_balance())

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

    async def _get_collateral_decimals(self, token: Any) -> int:
        if self._collateral_decimals is None:
            self._collateral_decimals = int(await token.functions.decimals().call())
        return self._collateral_decimals

    async def get_positions(self) -> dict[str, Decimal]:
        account = self._account_address()
        if not account:
            return {}
        positions: dict[str, Decimal] = {}
        for item in await self._paginate(f"/positions/user/{account}"):
            market_id = _optional_int(item.get("marketId"))
            shares = _decimal_field(item, ("sharesOwned", "shares"))
            if market_id is None or shares is None:
                continue
            token = _optional_str(item.get("tokenId"))
            if token is None:
                token = self._cached_outcome_token(market_id, item.get("outcomeSide"))
            if token is None:
                continue
            self._remember_position_metadata(market_id, token, item.get("outcomeSide"))
            key = execution_token(market_id, token)
            positions[key] = positions.get(key, Decimal(0)) + shares
        return positions

    def _cached_outcome_token(self, market_id: int, outcome_side: Any) -> str | None:
        side = side_from_outcome_code(outcome_side)
        metadata = self._market_metadata.get(market_id)
        if side is None or metadata is None:
            return None
        return metadata.yes_token_id if side is BinarySide.YES else metadata.no_token_id

    def _remember_position_metadata(self, market_id: int, token: str, outcome_side: Any) -> None:
        side = side_from_outcome_code(outcome_side)
        if side is None:
            metadata = self._market_metadata.get(market_id)
            side = metadata.side_for_token(token) if metadata is not None else None
        if side is not None:
            self._remember_market_metadata(market_id, token, side)

    async def list_open_orders(self) -> list[VenueOrder]:
        orders: list[VenueOrder] = []
        for item in await self._paginate("/order", extra_params={"status": str(_ORDER_STATUS_PENDING)}):
            order_id = _optional_str(item.get("orderId") or item.get("id"))
            if order_id is None:
                continue
            quantity = _decimal_field(item, ("shares", "amount")) or Decimal(0)
            filled = _decimal_field(item, ("filledShares", "filledAmount")) or Decimal(0)
            price = _decimal_field(item, ("avgPrice", "price")) or Decimal(0)
            status_code = _optional_int(item.get("status")) or _ORDER_STATUS_PENDING
            orders.append(
                VenueOrder(
                    client_order_id=order_id,
                    venue_order_id=order_id,
                    venue=VENUE_NAME,
                    status=_INTENT_STATUS_BY_CODE.get(status_code, OrderIntentStatus.UNKNOWN),
                    quantity=quantity,
                    cumulative_filled=filled,
                    average_price=price,
                    updated_at=_timestamp_field(item) or datetime.now(UTC),
                )
            )
        return orders

    async def list_fills(self, since: datetime | None = None) -> list[FillRecord]:
        account = self._account_address()
        if not account:
            return []
        fills: list[FillRecord] = []
        for item in await self._paginate(f"/trade/user/{account}"):
            if _optional_int(item.get("status")) != _ORDER_STATUS_FILLED:
                continue
            occurred_at = _timestamp_field(item) or datetime.now(UTC)
            if since is not None and occurred_at < since:
                continue
            fill_id = _optional_str(item.get("txHash")) or f"{item.get('marketId')}:{occurred_at.timestamp()}"
            order_id = _optional_str(item.get("orderId")) or fill_id
            fills.append(
                FillRecord(
                    fill_id=fill_id,
                    client_order_id=order_id,
                    venue_order_id=order_id,
                    venue=VENUE_NAME,
                    quantity=_decimal_field(item, ("shares",)) or Decimal(0),
                    price=_decimal_field(item, ("price",)) or Decimal(0),
                    fee=_decimal_field(item, ("fee",)) or Decimal(0),
                    occurred_at=occurred_at,
                )
            )
        return fills

    def supports_full_reconciliation(self) -> bool:
        return True

    def reconciliation_account_fingerprint(self) -> str | None:
        account = self._account_address()
        if not account:
            return None
        return account_fingerprint(self.venue_name, account)

    def _account_address(self) -> str | None:
        return self._config.account_address or self._config.multi_sig_address

    def _account_query_params(self) -> dict[str, str]:
        return {"chainId": str(self._config.chain_id)}

    async def _paginate(
        self,
        path: str,
        *,
        extra_params: dict[str, str] | None = None,
        max_pages: int = 25,
    ) -> list[dict[str, Any]]:
        limit = max(1, min(self._config.market_page_limit, 20))
        records: list[dict[str, Any]] = []
        page = 1
        while page <= max_pages:
            params = {**self._account_query_params(), "page": str(page), "limit": str(limit)}
            if extra_params:
                params.update(extra_params)
            payload = await self._request_result("GET", path, query_params=params, authenticated=True)
            if not isinstance(payload, dict):
                break
            batch = payload.get("list")
            if not isinstance(batch, list) or not batch:
                break
            records.extend(item for item in batch if isinstance(item, dict))
            total = _optional_int(payload.get("total"))
            if total is not None and len(records) >= total:
                break
            if len(batch) < limit:
                break
            page += 1
        return records

    # ------------------------------------------------------------------
    # Transport
    # ------------------------------------------------------------------
    def _headers(self) -> dict[str, str]:
        headers = {"Content-Type": "application/json"}
        if self._config.api_key:
            headers["apikey"] = self._config.api_key
        return headers

    def _get_rest_session(self) -> Any:
        if self._rest_session is None or self._rest_session.closed:
            self._rest_session = client_session(self._headers())
        return self._rest_session

    def _get_ws_session(self) -> Any:
        if self._ws_session is None or self._ws_session.closed:
            self._ws_session = client_session(self._headers())
        return self._ws_session

    async def _close_ws_session(self) -> None:
        if self._ws_session is not None and not self._ws_session.closed:
            await self._ws_session.close()
        self._ws_session = None

    async def _reset_rest_session(self) -> None:
        if self._rest_session is not None and not self._rest_session.closed:
            await self._rest_session.close()
        self._rest_session = None

    async def _request_result(
        self,
        method: str,
        path: str,
        *,
        query_params: dict[str, str] | None = None,
        json_body: dict[str, Any] | None = None,
        authenticated: bool,
    ) -> Any:
        try:
            import aiohttp
        except ImportError as exc:
            raise RuntimeError("aiohttp is required for Opinion connectivity") from exc
        if authenticated and not self._config.api_key:
            raise RuntimeError(f"OPINION_API_KEY is required for {path}")
        limiter = self._authenticated_rate_limiter if authenticated else self._public_rate_limiter
        url = f"{self._config.api_base_url.rstrip('/')}/{path.lstrip('/')}"
        timeout = aiohttp.ClientTimeout(total=20, connect=10, sock_read=15)
        payload: Any = None
        for attempt in range(2):
            await limiter.acquire()
            session = self._get_rest_session()
            self._rest_request_count += 1
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
                self._rest_error_count += 1
                await self._reset_rest_session()
                if attempt == 1:
                    raise
        return unwrap_envelope(payload, path)

    async def close(self) -> None:
        for stream_task in (self._ws_task, self._heartbeat_task):
            if stream_task is not None and not stream_task.done():
                stream_task.cancel()
                await asyncio.gather(stream_task, return_exceptions=True)
        self._ws_task = None
        self._heartbeat_task = None
        for bootstrap_task in list(self._bootstrap_tasks.values()):
            if not bootstrap_task.done():
                bootstrap_task.cancel()
        await asyncio.gather(*self._bootstrap_tasks.values(), return_exceptions=True)
        self._bootstrap_tasks.clear()
        ws = self._ws
        self._ws = None
        if ws is not None and not ws.closed:
            await ws.close()
        await self._close_ws_session()
        await self._reset_rest_session()


class _RateLimiter:
    """Minimum-interval limiter matching the documented Opinion request budget."""

    def __init__(self, requests_per_second: float) -> None:
        self._min_interval = 1.0 / requests_per_second if requests_per_second > 0 else 0.0
        self._next_allowed_at = 0.0
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        if self._min_interval <= 0:
            return
        async with self._lock:
            now = time.monotonic()
            wait_for = self._next_allowed_at - now
            if wait_for > 0:
                await asyncio.sleep(wait_for)
                now = time.monotonic()
            self._next_allowed_at = now + self._min_interval


def execution_token(market_id: int | str, outcome_token_id: str) -> str:
    """Build the composite ``marketId:tokenId`` execution token."""
    return f"{market_id}:{outcome_token_id}"


def parse_execution_token(token_id: str) -> tuple[int, str]:
    """Split a composite execution token into its market id and outcome token."""
    raw = str(token_id).strip()
    market_part, separator, outcome_part = raw.partition(":")
    if not separator or not outcome_part:
        raise ValueError(f"Opinion token id must use 'marketId:tokenId' form: {token_id!r}")
    try:
        market_id = int(market_part)
    except ValueError as exc:
        raise ValueError(f"Opinion token id has a non-numeric market id: {token_id!r}") from exc
    return market_id, outcome_part


def outcome_side_code(side: BinarySide) -> int:
    return _OUTCOME_SIDE_YES if side is BinarySide.YES else _OUTCOME_SIDE_NO


def side_from_outcome_code(value: Any) -> BinarySide | None:
    code = _optional_int(value)
    if code == _OUTCOME_SIDE_YES:
        return BinarySide.YES
    if code == _OUTCOME_SIDE_NO:
        return BinarySide.NO
    return None


def unwrap_envelope(payload: Any, path: str) -> Any:
    """Unwrap the ``{code, msg, result}`` envelope every Opinion endpoint returns."""
    if not isinstance(payload, dict):
        raise RuntimeError(f"Opinion API returned unsupported payload for {path}: {payload!r}")
    if "code" not in payload:
        return payload
    code = _optional_int(payload.get("code"))
    if code not in (0, None):
        message = payload.get("msg") or "unknown error"
        raise RuntimeError(f"Opinion API error for {path}: code={code} msg={message}")
    return payload.get("result")


def order_book_from_payload(payload: Any) -> OrderBook:
    """Build an order book from an Opinion orderbook snapshot or depth push."""
    if not isinstance(payload, dict):
        return OrderBook(bids=(), asks=(), status=MarketDataStatus.INVALID)
    bids = [level for item in payload.get("bids") or [] if (level := _level(item)) is not None]
    asks = [level for item in payload.get("asks") or [] if (level := _level(item)) is not None]
    return OrderBook(
        bids=sorted(bids, key=lambda item: item.price, reverse=True),
        asks=sorted(asks, key=lambda item: item.price),
        raw_payload=payload,
        timestamp=event_timestamp(payload),
        sequence=event_sequence(payload),
    )


def apply_depth_diff(book: OrderBook, payload: dict[str, Any]) -> OrderBook:
    """Apply an Opinion ``market.depth.diff`` push onto a cached order book.

    A level whose size is zero is removed; any other size replaces the level at
    that price. A diff without a usable ``side`` cannot be trusted to describe
    the whole book, so the cached snapshot is invalidated rather than merged.
    """
    changes = payload.get("changes")
    entries = changes if isinstance(changes, list) else [payload]
    bids = {level.price: level.size for level in book.bids}
    asks = {level.price: level.size for level in book.asks}
    applied = False
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        level = _level(entry)
        if level is None:
            continue
        target = _diff_side(entry)
        if target is None:
            return replace(book, status=MarketDataStatus.INVALID)
        book_side = bids if target == "bids" else asks
        if level.size <= 0:
            book_side.pop(level.price, None)
        else:
            book_side[level.price] = level.size
        applied = True
    if not applied:
        return book
    return OrderBook(
        bids=[OrderBookLevel(price, size) for price, size in sorted(bids.items(), reverse=True)],
        asks=[OrderBookLevel(price, size) for price, size in sorted(asks.items())],
        raw_payload=payload,
        timestamp=event_timestamp(payload),
        sequence=event_sequence(payload),
    )


def build_order_payload(
    *,
    market_id: int,
    outcome_token: str,
    action: str,
    contracts: Decimal,
    limit_price: Decimal,
    price_precision: int,
) -> dict[str, Any]:
    """Build the deterministic CLOB order payload used for signing and preview."""
    normalized_action = action.upper()
    if normalized_action not in {"BUY", "SELL"}:
        raise ValueError(f"Unsupported Opinion order action: {action}")
    if contracts <= 0:
        raise ValueError("Opinion order size must be positive")
    if not 0 < limit_price <= 1:
        raise ValueError("Opinion order price must be between 0 and 1")
    quantized_price = limit_price.quantize(Decimal(1).scaleb(-max(1, price_precision)))
    return {
        "marketId": market_id,
        "tokenId": outcome_token,
        "side": normalized_action,
        "orderType": "LIMIT",
        "price": format(quantized_price, "f"),
        "makerAmountInQuoteToken": format((contracts * quantized_price).quantize(Decimal("0.000001")), "f"),
        "shares": format(contracts.quantize(Decimal("0.000001")), "f"),
    }


def _place_order_input(payload: dict[str, Any]) -> Any:
    from opinion_clob_sdk.chain.py_order_utils.model.order import (
        PlaceOrderDataInput,
    )
    from opinion_clob_sdk.chain.py_order_utils.model.order_type import (
        LIMIT_ORDER,
    )
    from opinion_clob_sdk.chain.py_order_utils.model.sides import (
        OrderSide,
    )

    side = OrderSide.BUY if payload["side"] == "BUY" else OrderSide.SELL
    return PlaceOrderDataInput(
        marketId=payload["marketId"],
        tokenId=payload["tokenId"],
        side=side,
        orderType=LIMIT_ORDER,
        price=payload["price"],
        makerAmountInQuoteToken=payload["makerAmountInQuoteToken"],
    )


def _payload_fingerprint(payload: dict[str, Any]) -> str:
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _extract_order_id(result: Any) -> str | None:
    if isinstance(result, str):
        return result or None
    if isinstance(result, dict):
        for key in ("orderId", "order_id", "id", "orderHash"):
            value = result.get(key)
            if value not in (None, ""):
                return str(value)
        nested = result.get("result") or result.get("data")
        if isinstance(nested, dict):
            return _extract_order_id(nested)
    return _optional_str(getattr(result, "order_id", None) or getattr(result, "orderId", None))


def _subscribe_message(market_id: int) -> dict[str, Any]:
    return {"action": "SUBSCRIBE", "channel": _DEPTH_CHANNEL, "marketId": market_id}


def _unsubscribe_message(market_id: int) -> dict[str, Any]:
    return {"action": "UNSUBSCRIBE", "channel": _DEPTH_CHANNEL, "marketId": market_id}


def _diff_side(entry: dict[str, Any]) -> str | None:
    raw = str(entry.get("side") or entry.get("bookSide") or "").strip().lower()
    if raw in {"bid", "bids", "buy"}:
        return "bids"
    if raw in {"ask", "asks", "sell"}:
        return "asks"
    return None


def _level(item: Any) -> OrderBookLevel | None:
    if isinstance(item, dict):
        price = _optional_float(item.get("price"))
        size = _optional_float(item.get("size") if item.get("size") is not None else item.get("shares"))
    elif isinstance(item, list | tuple) and len(item) >= 2:
        price = _optional_float(item[0])
        size = _optional_float(item[1])
    else:
        return None
    if price is None or size is None or price <= 0 or price > 1 or size < 0:
        return None
    return OrderBookLevel(price=price, size=size)


def _json_loads(raw: str) -> Any:
    try:
        return json.loads(raw)
    except ValueError:
        return None


def _optional_str(value: Any) -> str | None:
    if value in (None, ""):
        return None
    text = str(value).strip()
    return text or None


def _optional_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


def _optional_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _decimal_field(payload: dict[str, Any], keys: tuple[str, ...]) -> Decimal | None:
    for key in keys:
        value = payload.get(key)
        if value in (None, ""):
            continue
        try:
            return Decimal(str(value))
        except ArithmeticError:
            continue
    return None


def _timestamp_field(payload: dict[str, Any]) -> datetime | None:
    for key in ("createdAt", "updatedAt", "timestamp", "filledAt"):
        seconds = _optional_int(payload.get(key))
        if seconds is None:
            continue
        if seconds > 10_000_000_000:
            seconds //= 1_000
        return datetime.fromtimestamp(seconds, tz=UTC)
    return None
