from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import re
import secrets
import time
from collections import OrderedDict
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal
from typing import Any

from arbitrage_engine.config import SxBetConfig
from arbitrage_engine.connectors.base import (
    BinaryMarketClient,
    OrderResidualExposure,
    OrderResidualExposureBatch,
    OrderSubmissionRejected,
    WebSocketReconnectBackoff,
)
from arbitrage_engine.http import client_session
from arbitrage_engine.models import (
    BinarySide,
    ExecutionReport,
    ExecutionStatus,
    FillRecord,
    MarketConstraints,
    MarketDataStatus,
    OrderBook,
    OrderBookLevel,
    OrderIntent,
    OrderIntentStatus,
    OrderPreview,
    RedemptionIntentStatus,
    RedemptionReport,
    SettlementRequest,
    SettlementStatus,
    VenueFeeQuote,
    VenueOrder,
    opposite_binary_side,
)

LOGGER = logging.getLogger(__name__)

ODDS_DECIMALS = Decimal("1e20")
_REST_RECOVERY_AFTER_SECONDS = 2.0
_WS_HEARTBEAT_SECONDS = 5.0
_WS_SNAPSHOT_PRIME_TIMEOUT_SECONDS = 2.0
_TARGET_TRANSITION_GRACE_SECONDS = 2.25
_FEE_CACHE_SECONDS = 30.0
_INTERNAL_TOKEN_PREFIX = "__sx_v3_outcome__"
_V3_MAX_WAIT_TIME_MS = 15_000
_V3_HTTP_TOTAL_TIMEOUT_SECONDS = 35
_V3_HTTP_CONNECT_TIMEOUT_SECONDS = 10
_V3_HTTP_READ_TIMEOUT_SECONDS = 20
_V3_TRANSPORT_START_ALLOWANCE_SECONDS = _V3_HTTP_CONNECT_TIMEOUT_SECONDS
_V3_ORDER_TTL_SECONDS = 60
_V3_PREPARED_ORDER_TTL_SECONDS = 10.0
_V3_PREPARED_ORDER_CACHE_LIMIT = 512
_V3_HISTORICAL_ORDER_CONTEXT_LIMIT = 10_000
_V3_SUBMISSION_CUTOFF_BUFFER_SECONDS = 15.0
_V3_FILL_INDEX_RETRIES = 3
_V3_MAX_RECORD_PAGES = 500
_V3_HEARTBEAT_TIMEOUT_SECONDS = 60
_V3_HEARTBEAT_REFRESH_SECONDS = 20.0
_V3_RETRYABLE_GET_STATUSES = frozenset({429, 500, 502, 503, 504})
_V3_DEFINITE_POST_REJECTION_STATUSES = frozenset({400, 401, 403, 404, 405, 413, 415, 422})
_V3_MAX_RETRY_AFTER_SECONDS = 5.0
_REALTIME_TOKEN_PATH = "/user/realtime-token-v3/api-key"
_ACCOUNT_AUTHENTICATED_PREFIXES = (
    "/user/",
    "/orders-v3",
    "/fills-v3",
    "/positions-v3",
    "/trades-v3",
    "/heartbeat/v3",
    "/orderbook-v3/snapshot/event",
)
_V3_INACTIVE_REASONS = frozenset(
    {
        "FILLED",
        "USER_REQUESTED",
        "EXPIRED",
        "NO_LIQUIDITY",
        "INSUFFICIENT_BALANCE",
        "EVENT_LIFECYCLE",
        "MARKET_HALTED",
        "HEARTBEAT_TIMEOUT",
        "SYSTEM",
    }
)
_V3_BET_STATES = frozenset({"MATCHED", "LOCKED", "SETTLED", "FAILED"})
_V3_IRREVERSIBLE_BET_STATES = frozenset({"LOCKED", "SETTLED"})
_V3_STAKE_INDEX_TOLERANCE = Decimal("0.000001")
_V3_CLIENT_ORDER_ID_PATTERN = re.compile(r"^[A-Za-z0-9_-]{1,64}$")
# The migration guide says 10:00 AM EST. Using 15:00 UTC is the conservative
# literal conversion; an operator flag is still required after this timestamp.
SX_V3_MAINNET_CUTOVER_AT = datetime(2026, 8, 26, 15, 0, tzinfo=UTC)


@dataclass(frozen=True)
class _V3SubmittedOrder:
    order_id: str
    market_hash: str
    token_id: str
    action: str
    synthetic_side: BinarySide
    actual_side: BinarySide
    requested_contracts: Decimal
    requested_price: Decimal
    submitted_stake: Decimal
    submitted_at: datetime
    asset_decimals: int = 6
    refund_fee_rate: Decimal = Decimal(0)
    submitted_stake_verified: bool = True


@dataclass(frozen=True)
class _V3FeeSchedule:
    taker_payout_fee: Decimal
    refund_fee: Decimal


@dataclass(frozen=True)
class _V3PreparedOrder:
    payload: dict[str, Any]
    submitted: _V3SubmittedOrder
    fingerprint: str
    prepared_at_monotonic: float


class SxBetV3SubmissionUnknown(RuntimeError):
    """Carries the locally computed order id when POST acknowledgement is unknown."""

    def __init__(self, order_id: str, reason: BaseException | str) -> None:
        self.order_id = order_id
        super().__init__(f"SX Bet V3 submission outcome is unknown for {order_id}: {reason}")


class SxBetV3HttpError(RuntimeError):
    """HTTP failure with a status that reconciliation can classify safely."""

    def __init__(self, method: str, path: str, status: int) -> None:
        self.status = status
        super().__init__(f"SX Bet V3 {method} {path} failed with {status}")


class SxBetV3ApiClient(BinaryMarketClient):
    """SX Bet OBv3 taker connector.

    V3 uses proxy-held balances, aggregated versioned books, and one signed
    order endpoint for both makers and takers. This client intentionally only
    submits immediate IOC/FOK taker orders; it never leaves a GTC quote.
    """

    venue_name = "SX Bet"

    def persists_order_id_before_submission(self) -> bool:
        """SX V3 derives and persists its signed digest before POST /orders-v3."""
        return True

    def __init__(self, config: SxBetConfig) -> None:
        if config.api_version != "v3":
            raise ValueError("SxBetV3ApiClient requires sx_bet.api_version=v3")
        expected_api_url = "https://api.toronto.sx.bet" if config.environment == "toronto" else "https://api.sx.bet"
        expected_ws_url = (
            "wss://realtime.toronto.sx.bet/connection/websocket"
            if config.environment == "toronto"
            else "wss://realtime.sx.bet/connection/websocket"
        )
        if config.api_base_url.rstrip("/") != expected_api_url:
            raise ValueError(f"SX Bet V3 {config.environment} must use the official API host")
        if config.ws_url.rstrip("/") != expected_ws_url:
            raise ValueError(f"SX Bet V3 {config.environment} must use the official realtime host")
        if config.environment == "mainnet" and not config.allow_v3_mainnet:
            raise RuntimeError("SX Bet V3 mainnet is blocked until operator cutover is explicitly enabled")
        if config.environment == "mainnet" and _utc_now() < SX_V3_MAINNET_CUTOVER_AT:
            raise RuntimeError(f"SX Bet V3 mainnet is blocked before {SX_V3_MAINNET_CUTOVER_AT.isoformat()}")
        if config.time_in_force not in {"IOC", "FOK"}:
            raise ValueError("SX Bet V3 taker time_in_force must be IOC or FOK")
        self._config = config
        self._rest_session: Any | None = None
        self._ws_session: Any | None = None
        self._ws: Any | None = None
        self._ws_task: asyncio.Task[None] | None = None
        self._http_semaphore = asyncio.Semaphore(20)
        self._metadata_cache: dict[str, Any] | None = None
        self._fee_cache: tuple[float, _V3FeeSchedule] | None = None
        self._proxy_cache: tuple[float, dict[str, Any]] | None = None
        self._market_identifiers: dict[str, tuple[str, BinarySide]] = {}
        self._token_by_market_side: dict[tuple[str, BinarySide], str] = {}
        self._tracked_tokens: set[str] = set()
        self._books: dict[str, OrderBook] = {}
        self._book_timestamps: dict[str, float] = {}
        self._book_events: dict[str, asyncio.Event] = {}
        self._book_versions: dict[str, str] = {}
        self._bootstrap_locks: dict[str, asyncio.Lock] = {}
        self._bootstrap_tasks: dict[str, asyncio.Task[None]] = {}
        self._subscription_queue: asyncio.Queue[tuple[str, str]] = asyncio.Queue()
        self._subscription_positions: dict[str, tuple[str, int]] = {}
        self._subscribed_markets: set[str] = set()
        self._ws_connected = False
        self._reconnect_backoff = WebSocketReconnectBackoff()
        self._reconnect_count = 0
        self._sequence_gap_count = 0
        self._target_transition_deadline = 0.0
        self._reports: dict[str, ExecutionReport] = {}
        self._submitted_orders: dict[str, _V3SubmittedOrder] = {}
        self._historical_order_contexts: OrderedDict[str, _V3SubmittedOrder] = OrderedDict()
        self._prepared_orders: dict[str, _V3PreparedOrder] = {}
        self._claimed_prepared_orders: dict[str, _V3PreparedOrder] = {}
        self._heartbeat_lock = asyncio.Lock()
        self._heartbeat_armed = False
        self._heartbeat_last_refresh_at = 0.0

    def register_market(self, token_id: str, market_hash: str | None, side: BinarySide) -> None:
        if not token_id or not market_hash:
            return
        previous_token = self._token_by_market_side.get((market_hash, side))
        self._market_identifiers[token_id] = (market_hash, side)
        self._token_by_market_side[(market_hash, side)] = token_id
        self._book_events.setdefault(token_id, asyncio.Event())
        if previous_token and previous_token != token_id and previous_token.startswith(_INTERNAL_TOKEN_PREFIX):
            if previous_token in self._books:
                self._books[token_id] = self._books[previous_token]
            if previous_token in self._book_timestamps:
                self._book_timestamps[token_id] = self._book_timestamps[previous_token]
            if previous_token not in self._tracked_tokens:
                self._market_identifiers.pop(previous_token, None)
                self._books.pop(previous_token, None)
                self._book_timestamps.pop(previous_token, None)
                self._book_events.pop(previous_token, None)
        opposite_side = opposite_binary_side(side)
        if (market_hash, opposite_side) not in self._token_by_market_side:
            internal_token = f"{_INTERNAL_TOKEN_PREFIX}:{market_hash}:{opposite_side.value}"
            self._market_identifiers[internal_token] = (market_hash, opposite_side)
            self._token_by_market_side[(market_hash, opposite_side)] = internal_token
            self._book_events.setdefault(internal_token, asyncio.Event())
        if self._ws_connected and token_id in self._tracked_tokens:
            self._subscription_queue.put_nowait(("subscribe", market_hash))

    async def watch_order_book(self, token_id: str) -> OrderBook:
        identity = self._market_identifiers.get(token_id)
        if identity is None:
            raise RuntimeError(f"SX Bet V3 market hash and side are not registered for token {token_id}")
        market_hash, side = identity
        self._tracked_tokens.add(token_id)
        self._ensure_ws_task()
        cached = self._books.get(token_id)
        if self._cached_book_is_fresh(token_id, cached):
            assert cached is not None
            return cached
        lock = self._bootstrap_locks.setdefault(market_hash, asyncio.Lock())
        async with lock:
            cached = self._books.get(token_id)
            if self._cached_book_is_fresh(token_id, cached):
                assert cached is not None
                return cached
            await self._bootstrap_market(market_hash)
            return self._books.get(token_id) or _empty_v3_book(market_hash, side)

    def _cached_book_is_fresh(self, token_id: str, book: OrderBook | None) -> bool:
        if book is None or book.status is not MarketDataStatus.VALID:
            return False
        if self._healthy_stream_confirms_book(token_id):
            return True
        updated = self._book_timestamps.get(token_id)
        return updated is not None and time.monotonic() - updated <= _REST_RECOVERY_AFTER_SECONDS

    def _healthy_stream_confirms_book(self, token_id: str) -> bool:
        identity = self._market_identifiers.get(token_id)
        return bool(
            identity is not None
            and self._ws_connected
            and self._ws is not None
            and not self._ws.closed
            and identity[0] in self._subscribed_markets
        )

    async def _bootstrap_market(self, market_hash: str) -> None:
        await self._metadata()
        payload = await self._request_json(
            "GET",
            "/orderbook-v3/snapshot",
            query_params={"marketHash": market_hash},
        )
        data = _response_data(payload)
        if not isinstance(data, dict):
            raise RuntimeError("SX Bet V3 orderbook snapshot is malformed")
        self._apply_book_snapshot(market_hash, data)

    def _apply_book_snapshot(self, market_hash: str, payload: dict[str, Any]) -> bool:
        payload_market = str(payload.get("marketHash") or market_hash)
        if payload_market.lower() != market_hash.lower():
            raise RuntimeError("SX Bet V3 orderbook market hash mismatch")
        version = str(payload.get("version") or "")
        if not version:
            raise RuntimeError("SX Bet V3 orderbook snapshot is missing version")
        current = self._book_versions.get(market_hash)
        if current is not None:
            if version < current:
                return False
            if version == current and not self._market_books_are_stale(market_hash):
                return False
        if not isinstance(payload.get("outcomeOne"), list) or not isinstance(payload.get("outcomeTwo"), list):
            raise RuntimeError("SX Bet V3 orderbook snapshot is missing outcome levels")
        self._book_versions[market_hash] = version
        for (registered_market, side), token_id in self._token_by_market_side.items():
            if registered_market != market_hash:
                continue
            self._books[token_id] = _order_book_from_v3_maker_snapshot(
                payload,
                side,
                asset_decimals=self._cached_asset_decimals(),
            )
            self._book_timestamps[token_id] = time.monotonic()
            self._book_events.setdefault(token_id, asyncio.Event()).set()
        if self.market_data_ready():
            self._target_transition_deadline = 0.0
        return True

    def _market_books_are_stale(self, market_hash: str) -> bool:
        return any(
            registered_market == market_hash
            and token_id in self._books
            and self._books[token_id].status is MarketDataStatus.STALE
            for (registered_market, _), token_id in self._token_by_market_side.items()
        )

    def _ensure_ws_task(self) -> None:
        if not self._config.api_key or not self._config.ws_url:
            return
        if self._ws_task is None or self._ws_task.done():
            self._ws_task = asyncio.create_task(self._run_order_book_ws())

    async def _run_order_book_ws(self) -> None:
        try:
            import aiohttp
        except ImportError:
            return
        while True:
            connected_at: float | None = None
            sender: asyncio.Task[None] | None = None
            try:
                await self._metadata()
                token_payload = await self._request_json("GET", _REALTIME_TOKEN_PATH)
                token = _extract_realtime_token(token_payload)
                if not token:
                    raise RuntimeError("SX Bet V3 realtime token response is missing token")
                session = self._get_ws_session()
                async with session.ws_connect(self._config.ws_url, heartbeat=_WS_HEARTBEAT_SECONDS) as ws:
                    self._ws = ws
                    await ws.send_json({"connect": {"token": token}, "id": 1})
                    pending: dict[int, tuple[str, bool]] = {}
                    command_id = 2
                    for market_hash in sorted(self._active_market_hashes()):
                        command_id = await self._send_market_subscription(ws, market_hash, command_id, pending)
                    self._ws_connected = True
                    connected_at = time.monotonic()
                    sender = asyncio.create_task(self._send_subscriptions(ws, command_id, pending))
                    async for message in ws:
                        if message.type != aiohttp.WSMsgType.TEXT:
                            continue
                        for raw_message in str(message.data).splitlines():
                            if raw_message:
                                payload = json.loads(raw_message)
                                if isinstance(payload, dict):
                                    await self._handle_centrifugo_message(payload, pending)
            except asyncio.CancelledError:
                raise
            except (aiohttp.ClientConnectionError, ConnectionResetError) as exc:
                LOGGER.info("sx_bet_v3_ws_disconnected", extra={"reason": type(exc).__name__})
            except Exception:
                LOGGER.exception("sx_bet_v3_ws_failed")
            finally:
                if sender is not None:
                    sender.cancel()
                    await asyncio.gather(sender, return_exceptions=True)
                self._ws_connected = False
                self._subscribed_markets.clear()
                self._mark_books_stale()
                self._ws = None
                await self._close_ws_session()
            if connected_at is not None and time.monotonic() - connected_at >= 60:
                self._reconnect_backoff.reset()
            self._reconnect_count += 1
            await asyncio.sleep(self._reconnect_backoff.next_delay())

    async def _send_market_subscription(
        self,
        ws: Any,
        market_hash: str,
        command_id: int,
        pending: dict[int, tuple[str, bool]],
    ) -> int:
        channel = f"orderbook_v3:{market_hash}"
        subscribe: dict[str, Any] = {"channel": channel, "positioned": True, "recoverable": True}
        position = self._subscription_positions.get(channel)
        if position:
            subscribe.update({"epoch": position[0], "offset": position[1]})
        await ws.send_json({"subscribe": subscribe, "id": command_id})
        pending[command_id] = (market_hash, position is not None)
        return command_id + 1

    async def _send_subscriptions(
        self,
        ws: Any,
        command_id: int,
        pending: dict[int, tuple[str, bool]],
    ) -> None:
        subscribed = set(self._subscribed_markets)
        subscribed.update(market_hash for market_hash, _ in pending.values())
        while True:
            action, market_hash = await self._subscription_queue.get()
            channel = f"orderbook_v3:{market_hash}"
            if action == "subscribe" and market_hash not in subscribed:
                command_id = await self._send_market_subscription(ws, market_hash, command_id, pending)
                subscribed.add(market_hash)
            elif action == "unsubscribe" and market_hash in subscribed:
                await ws.send_json({"unsubscribe": {"channel": channel}, "id": command_id})
                command_id += 1
                subscribed.remove(market_hash)
                self._subscribed_markets.discard(market_hash)

    async def _handle_centrifugo_message(
        self,
        payload: dict[str, Any],
        pending: dict[int, tuple[str, bool]],
    ) -> None:
        if payload == {}:
            if self._ws is not None and not getattr(self._ws, "closed", False):
                await self._ws.send_json({})
            return
        if payload.get("error"):
            raise RuntimeError(f"SX Bet V3 Centrifugo error: {payload['error']!r}")
        command_id = payload.get("id")
        subscribed = payload.get("subscribe")
        if subscribed is None and isinstance(payload.get("result"), dict):
            subscribed = payload["result"].get("subscribe", payload["result"])
        if isinstance(command_id, int) and command_id in pending and isinstance(subscribed, dict):
            market_hash, was_recovering = pending.pop(command_id)
            self._subscribed_markets.add(market_hash)
            channel = f"orderbook_v3:{market_hash}"
            epoch = str(subscribed.get("epoch") or "")
            offset = int(subscribed.get("offset") or 0)
            if epoch:
                old_epoch, old_offset = self._subscription_positions.get(channel, ("", 0))
                self._subscription_positions[channel] = (epoch, old_offset if old_epoch == epoch else 0)
            recovered = was_recovering and bool(subscribed.get("recovered"))
            if recovered:
                for publication in subscribed.get("publications") or []:
                    if isinstance(publication, dict):
                        self._apply_publication(market_hash, channel, publication)
            else:
                current_epoch, current_offset = self._subscription_positions.get(channel, (epoch, 0))
                self._subscription_positions[channel] = (current_epoch or epoch, max(current_offset, offset))
                if market_hash in self._active_market_hashes():
                    self._schedule_bootstrap(market_hash)
            return
        push = payload.get("push")
        if not isinstance(push, dict):
            return
        channel = str(push.get("channel") or "")
        publication = push.get("pub")
        if channel.startswith("orderbook_v3:") and isinstance(publication, dict):
            self._apply_publication(channel.removeprefix("orderbook_v3:"), channel, publication)

    def _apply_publication(self, market_hash: str, channel: str, publication: dict[str, Any]) -> None:
        if market_hash not in self._active_market_hashes():
            return
        offset = int(publication.get("offset") or 0)
        epoch, previous_offset = self._subscription_positions.get(channel, ("", 0))
        if previous_offset and offset > previous_offset + 1:
            self._sequence_gap_count += 1
            # Every V3 publication is a complete book, so the newest valid
            # version safely recovers an offset gap without replaying deltas.
        if offset and offset <= previous_offset:
            return
        data = publication.get("data")
        if not isinstance(data, dict):
            self._mark_market_books_stale(market_hash)
            return
        self._apply_book_snapshot(market_hash, data)
        if offset:
            self._subscription_positions[channel] = (epoch, offset)

    def _schedule_bootstrap(self, market_hash: str) -> None:
        current = self._bootstrap_tasks.get(market_hash)
        if current is not None and not current.done():
            return
        task = asyncio.create_task(self._bootstrap_market(market_hash))
        self._bootstrap_tasks[market_hash] = task

        def _done(completed: asyncio.Task[None]) -> None:
            if self._bootstrap_tasks.get(market_hash) is completed:
                self._bootstrap_tasks.pop(market_hash, None)
            try:
                completed.result()
            except asyncio.CancelledError:
                return
            except Exception:
                self._mark_market_books_stale(market_hash)
                LOGGER.exception("sx_bet_v3_subscription_bootstrap_failed", extra={"_market_hash": market_hash})

        task.add_done_callback(_done)

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
        return await self._submit_taker_order(
            token_id,
            side,
            side,
            Decimal(str(contracts)),
            Decimal(str(max_price)),
            "BUY",
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
        del condition_id, tick_size, neg_risk
        return await self._submit_taker_order(
            token_id,
            side,
            side,
            Decimal(str(contracts)),
            Decimal(str(max_price)),
            "BUY",
            persist_order_id=persist_order_id,
            pre_transport_guard=pre_transport_guard,
            client_order_id=client_order_id,
            prepared_order_fingerprint=prepared_order_fingerprint,
            submission_deadline_unix=submission_deadline_unix,
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
        del condition_id, tick_size, neg_risk
        price = Decimal(str(min_price))
        if price <= 0 or price >= 1:
            raise ValueError("SX Bet V3 sell min_price must be between 0 and 1")
        return await self._submit_taker_order(
            token_id,
            side,
            opposite_binary_side(side),
            Decimal(str(contracts)),
            price,
            "SELL",
        )

    async def sell_with_order_id_persistence(
        self,
        token_id: str,
        side: BinarySide,
        contracts: float,
        min_price: float,
        *,
        persist_order_id: Callable[[str], Awaitable[None]],
        client_order_id: str | None = None,
        condition_id: str | None = None,
        tick_size: str | None = None,
        neg_risk: bool | None = None,
    ) -> str:
        del condition_id, tick_size, neg_risk
        price = Decimal(str(min_price))
        if price <= 0 or price >= 1:
            raise ValueError("SX Bet V3 sell min_price must be between 0 and 1")
        return await self._submit_taker_order(
            token_id,
            side,
            opposite_binary_side(side),
            Decimal(str(contracts)),
            price,
            "SELL",
            persist_order_id=persist_order_id,
            client_order_id=client_order_id,
        )

    async def _submit_taker_order(
        self,
        token_id: str,
        synthetic_side: BinarySide,
        actual_side: BinarySide,
        requested_contracts: Decimal,
        requested_price: Decimal,
        action: str,
        *,
        persist_order_id: Callable[[str], Awaitable[None]] | None = None,
        pre_transport_guard: Callable[[], None] | None = None,
        client_order_id: str | None = None,
        prepared_order_fingerprint: str | None = None,
        submission_deadline_unix: float | None = None,
    ) -> str:
        prepared = self._consume_prepared_order(
            prepared_order_fingerprint,
            token_id=token_id,
            synthetic_side=synthetic_side,
            actual_side=actual_side,
            requested_contracts=requested_contracts,
            requested_price=requested_price,
            action=action,
        )
        if prepared_order_fingerprint is not None and submission_deadline_unix is None:
            raise OrderSubmissionRejected("SX Bet V3 prepared entry is missing a market submission deadline")
        if prepared is not None:
            payload = dict(prepared.payload)
            order_id = prepared.submitted.order_id
            resolved_client_order_id = client_order_id or order_id.removeprefix("0x")
            if _V3_CLIENT_ORDER_ID_PATTERN.fullmatch(resolved_client_order_id) is None:
                raise ValueError(
                    "SX Bet V3 client_order_id must be 1-64 ASCII letters, digits, underscores, or hyphens"
                )
            payload["clientOrderId"] = resolved_client_order_id
            submitted = replace(prepared.submitted, submitted_at=datetime.now(UTC))
        else:
            await self._ensure_proxy_ready()
            fee_schedule = await self._fee_schedule()
            book = await self._execution_book(token_id, actual_side)
            payload, order_id, market_hash, stake = await self._build_signed_order(
                token_id=token_id,
                synthetic_side=synthetic_side,
                actual_side=actual_side,
                requested_contracts=requested_contracts,
                requested_price=requested_price,
                action=action,
                book=book,
                client_order_id=client_order_id,
            )
            submitted = _V3SubmittedOrder(
                order_id=order_id,
                market_hash=market_hash,
                token_id=token_id,
                action=action,
                synthetic_side=synthetic_side,
                actual_side=actual_side,
                requested_contracts=requested_contracts,
                requested_price=requested_price,
                submitted_stake=stake,
                submitted_at=datetime.now(UTC),
                asset_decimals=await self._asset_decimals(),
                refund_fee_rate=fee_schedule.refund_fee,
            )
        self._assert_submission_window(order_id, submission_deadline_unix)
        self._assert_signed_order_expiry(order_id, payload)
        await self._track_submitted_order(submitted)
        if persist_order_id is not None:
            try:
                await persist_order_id(order_id)
            except BaseException:
                await self._drop_submitted_order(order_id)
                raise
        try:
            self._assert_submission_window(order_id, submission_deadline_unix)
            self._assert_signed_order_expiry(order_id, payload)

            def assert_transport_window() -> None:
                self._assert_submission_window(order_id, submission_deadline_unix)
                self._assert_signed_order_expiry(order_id, payload)
                if pre_transport_guard is not None:
                    pre_transport_guard()

            response = await self._request_json(
                "POST",
                "/orders-v3",
                json_body={
                    "orders": [payload],
                    "waitForOutcome": True,
                    "maxWaitTime": _V3_MAX_WAIT_TIME_MS,
                },
                before_request=assert_transport_window,
            )
        except OrderSubmissionRejected:
            await self._drop_submitted_order(order_id)
            raise
        except SxBetV3HttpError as exc:
            if exc.status in _V3_DEFINITE_POST_REJECTION_STATUSES:
                await self._drop_submitted_order(order_id)
                raise OrderSubmissionRejected(
                    f"SX Bet V3 order was not accepted (HTTP {exc.status})",
                    order_id=order_id,
                ) from exc
            if await self._recover_unknown_submission(order_id):
                return order_id
            raise SxBetV3SubmissionUnknown(order_id, exc) from exc
        except Exception as exc:
            if await self._recover_unknown_submission(order_id):
                return order_id
            raise SxBetV3SubmissionUnknown(order_id, exc) from exc
        entries = _extract_records(response, ("orders",))
        if len(entries) != 1:
            raise SxBetV3SubmissionUnknown(order_id, "create-order response did not contain one order")
        entry = entries[0]
        if str(entry.get("status") or "").upper() != "SUBMITTED":
            await self._drop_submitted_order(order_id)
            raise OrderSubmissionRejected(
                f"SX Bet V3 order rejected: {entry.get('error') or entry.get('reason') or 'unknown'}",
                order_id=order_id,
            )
        returned_id = str(entry.get("orderId") or "").lower()
        if not returned_id or returned_id != order_id.lower():
            raise SxBetV3SubmissionUnknown(order_id, "returned orderId did not match signed digest")
        if str(entry.get("clientOrderId") or "") != str(payload["clientOrderId"]):
            raise SxBetV3SubmissionUnknown(order_id, "returned clientOrderId did not match durable intent")
        outcome = entry.get("outcome")
        if isinstance(outcome, dict):
            await self._store_report(order_id, _report_from_v3_outcome(submitted, outcome))
        else:
            await self._store_report(
                order_id,
                ExecutionReport.from_amounts(order_id, requested_contracts, Decimal(0), "open"),
            )
        return order_id

    async def _execution_book(self, token_id: str, actual_side: BinarySide) -> OrderBook:
        identity = self._market_identifiers.get(token_id)
        if identity is None:
            raise RuntimeError(f"SX Bet V3 market hash and side are not registered for token {token_id}")
        market_hash, _ = identity
        execution_token = self._token_by_market_side.get((market_hash, actual_side))
        if execution_token is None:
            raise RuntimeError(f"SX Bet V3 {actual_side.value} outcome is not registered for market {market_hash}")
        return await self.watch_order_book(execution_token)

    async def _recover_unknown_submission(self, order_id: str) -> bool:
        for attempt in range(3):
            try:
                payload = await self._request_json("GET", f"/orders-v3/{order_id}")
                data = _response_data(payload)
                order = data.get("order", data) if isinstance(data, dict) else None
                if isinstance(order, dict) and str(order.get("id") or order.get("orderId") or "").lower() == order_id:
                    await self._store_report(
                        order_id,
                        ExecutionReport.from_amounts(
                            order_id,
                            self._submitted_orders[order_id].requested_contracts,
                            Decimal(0),
                            "open",
                        ),
                    )
                    return True
            except Exception:
                if attempt == 2:
                    return False
            await asyncio.sleep(0.25 * (attempt + 1))
        return False

    async def _build_signed_order(
        self,
        *,
        token_id: str,
        synthetic_side: BinarySide,
        actual_side: BinarySide,
        requested_contracts: Decimal,
        requested_price: Decimal,
        action: str,
        book: OrderBook,
        client_order_id: str | None = None,
    ) -> tuple[dict[str, Any], str, str, Decimal]:
        if not self._config.private_key:
            raise RuntimeError("SX Bet private_key is required for V3 order signing")
        identity = self._market_identifiers.get(token_id)
        if identity is None:
            raise RuntimeError(f"SX Bet V3 market hash and side are not registered for token {token_id}")
        market_hash, registered_side = identity
        if registered_side is not synthetic_side:
            raise RuntimeError(
                f"SX Bet V3 token {token_id} is registered for {registered_side.value}, not {synthetic_side.value}"
            )
        actual_price_bound = requested_price if action == "BUY" else Decimal(1) - requested_price
        if requested_contracts <= 0:
            raise ValueError(f"SX Bet V3 {action.lower()} contracts must be positive")
        if actual_price_bound <= 0 or actual_price_bound >= 1:
            raise ValueError(f"SX Bet V3 {action.lower()} price must be between 0 and 1")
        metadata = await self._metadata()
        active_asset = metadata.get("activeAsset")
        domain = metadata.get("domain")
        if not isinstance(active_asset, dict) or not isinstance(domain, dict):
            raise RuntimeError("SX Bet V3 metadata is missing activeAsset or domain")
        decimals = _asset_decimals_from_metadata(metadata)
        exact_odds = _round_v3_probability(actual_price_bound, metadata)
        stake = _stake_for_contracts(book, requested_contracts, exact_odds)
        stake_units = _to_base_units(stake, decimals)
        limits = metadata.get("limits")
        minimum_units = (
            Decimal(str(limits.get("orderSizeMinimumBaseUnits", "0"))) if isinstance(limits, dict) else Decimal(0)
        )
        if stake_units < minimum_units:
            raise ValueError("SX Bet V3 order is below the venue minimum")
        account = _account_from_private_key(self._config.private_key)
        order = {
            "marketHash": market_hash,
            "maker": account.address,
            "baseToken": str(active_asset.get("baseToken") or ""),
            "totalBetSize": str(stake_units),
            "percentageOdds": str(int(_probability_to_odds_units(exact_odds))),
            "salt": "0x" + secrets.token_hex(32),
            "expiry": _v3_order_expiry(metadata),
            "isMakerBettingOutcomeOne": actual_side is BinarySide.YES,
            "timeInForce": self._config.time_in_force,
        }
        signature, order_id = _sign_v3_order(account, domain, order)
        resolved_client_order_id = client_order_id or order_id.removeprefix("0x")
        if _V3_CLIENT_ORDER_ID_PATTERN.fullmatch(resolved_client_order_id) is None:
            raise ValueError("SX Bet V3 client_order_id must be 1-64 ASCII letters, digits, underscores, or hyphens")
        return (
            {**order, "clientOrderId": resolved_client_order_id, "orderSignature": signature},
            order_id,
            market_hash,
            _from_base_units(stake_units, decimals),
        )

    async def build_order_preview(
        self,
        *,
        token_id: str,
        side: BinarySide,
        contracts: float,
        limit_price: float,
        action: str,
    ) -> dict[str, Any]:
        if action not in {"BUY", "SELL"}:
            raise ValueError("SX Bet V3 preview action must be BUY or SELL")
        await self._ensure_proxy_ready()
        actual_side = side if action == "BUY" else opposite_binary_side(side)
        book = await self._execution_book(token_id, actual_side)
        payload, order_id, market_hash, stake = await self._build_signed_order(
            token_id=token_id,
            synthetic_side=side,
            actual_side=actual_side,
            requested_contracts=Decimal(str(contracts)),
            requested_price=Decimal(str(limit_price)),
            action=action,
            book=book,
        )
        signature_fingerprint = hashlib.sha256(str(payload["orderSignature"]).encode()).hexdigest()
        safe_payload = {key: value for key, value in payload.items() if key not in {"salt", "orderSignature"}}
        return {
            "order_id": order_id,
            "market_hash": market_hash,
            "synthetic_side": side.value,
            "actual_order_side": actual_side.value,
            "requested_contracts": contracts,
            "requested_price": limit_price,
            "submitted_stake": str(stake),
            "refund_fee_rate": str((await self._fee_schedule()).refund_fee),
            "request_payload": safe_payload,
            "signature_fingerprint": signature_fingerprint,
            "signature_prefix": f"sha256:{signature_fingerprint[:16]}",
        }

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
        del condition_id, tick_size, neg_risk
        await self._ensure_proxy_ready()
        payload, order_id, market_hash, stake = await self._build_signed_order(
            token_id=token_id,
            synthetic_side=side,
            actual_side=side,
            requested_contracts=contracts,
            requested_price=max_price,
            action="BUY",
            book=book,
        )
        fingerprint = _v3_order_payload_fingerprint(order_id, payload)
        fee_schedule = await self._fee_schedule()
        submitted = _V3SubmittedOrder(
            order_id=order_id,
            market_hash=market_hash,
            token_id=token_id,
            action="BUY",
            synthetic_side=side,
            actual_side=side,
            requested_contracts=contracts,
            requested_price=max_price,
            submitted_stake=stake,
            submitted_at=datetime.now(UTC),
            asset_decimals=await self._asset_decimals(),
            refund_fee_rate=fee_schedule.refund_fee,
        )
        self._store_prepared_order(
            _V3PreparedOrder(
                payload=dict(payload),
                submitted=submitted,
                fingerprint=fingerprint,
                prepared_at_monotonic=time.monotonic(),
            )
        )
        return fingerprint

    async def _preview_buy_from_book(
        self,
        token_id: str,
        side: BinarySide,
        contracts: Decimal,
        max_price: Decimal,
        book: OrderBook,
        *,
        condition_id: str | None = None,
        tick_size: str | None = None,
        neg_risk: bool | None = None,
    ) -> OrderPreview:
        preview = await super()._preview_buy_from_book(
            token_id,
            side,
            contracts,
            max_price,
            book,
            condition_id=condition_id,
            tick_size=tick_size,
            neg_risk=neg_risk,
        )
        if not preview.payload_fingerprint:
            return preview
        prepared = self._prepared_orders.get(preview.payload_fingerprint)
        if prepared is None or preview.fee_quote is None:
            raise RuntimeError("SX Bet V3 signed preview accounting context is unavailable")
        signed_odds = _odds_units_to_probability(prepared.payload.get("percentageOdds"))
        if signed_odds <= 0:
            raise RuntimeError("SX Bet V3 signed preview has invalid limit odds")
        signed_stake = prepared.submitted.submitted_stake
        guaranteed_contracts = signed_stake / signed_odds
        executable_prices = [
            Decimal(str(level.price))
            for level in book.asks
            if Decimal(str(level.price)) > 0 and Decimal(str(level.price)) <= max_price
        ]
        if not executable_prices:
            raise RuntimeError("SX Bet V3 signed preview has no executable ask prices")
        lowest_fill_price = min(executable_prices)
        maximum_fill_contracts = signed_stake / lowest_fill_price
        maximum_fee = preview.fee_quote.fee_for_fill(maximum_fill_contracts, lowest_fill_price)
        return replace(
            preview,
            guaranteed_contracts=guaranteed_contracts,
            maximum_notional_usd=signed_stake,
            maximum_fee_usd=maximum_fee,
        )

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
        if fingerprint is not None and submission_deadline_unix is None:
            raise OrderSubmissionRejected("SX Bet V3 prepared entry is missing a market submission deadline")
        prepared = self._consume_prepared_order(
            fingerprint,
            token_id=token_id,
            synthetic_side=side,
            actual_side=side if action == "BUY" else opposite_binary_side(side),
            requested_contracts=contracts,
            requested_price=limit_price,
            action=action,
        )
        if prepared is None or fingerprint is None:
            return None
        self._assert_submission_window(prepared.submitted.order_id, submission_deadline_unix)
        self._assert_signed_order_expiry(prepared.submitted.order_id, prepared.payload)
        self._claimed_prepared_orders[fingerprint] = prepared
        return fingerprint

    def release_prepared_order(self, fingerprint: str | None) -> None:
        if fingerprint is not None:
            self._claimed_prepared_orders.pop(fingerprint, None)

    def _store_prepared_order(self, prepared: _V3PreparedOrder) -> None:
        self._prune_prepared_orders()
        if len(self._prepared_orders) >= _V3_PREPARED_ORDER_CACHE_LIMIT:
            oldest = min(
                self._prepared_orders.values(),
                key=lambda candidate: candidate.prepared_at_monotonic,
            )
            self._prepared_orders.pop(oldest.fingerprint, None)
        self._prepared_orders[prepared.fingerprint] = prepared

    def _prune_prepared_orders(self) -> None:
        stale_before = time.monotonic() - _V3_PREPARED_ORDER_TTL_SECONDS
        for fingerprint, prepared in tuple(self._prepared_orders.items()):
            if prepared.prepared_at_monotonic < stale_before:
                self._prepared_orders.pop(fingerprint, None)

    def _consume_prepared_order(
        self,
        fingerprint: str | None,
        *,
        token_id: str,
        synthetic_side: BinarySide,
        actual_side: BinarySide,
        requested_contracts: Decimal,
        requested_price: Decimal,
        action: str,
    ) -> _V3PreparedOrder | None:
        if fingerprint is None:
            return None
        self._prune_prepared_orders()
        prepared = self._claimed_prepared_orders.pop(fingerprint, None)
        if prepared is None:
            prepared = self._prepared_orders.pop(fingerprint, None)
        if prepared is None:
            raise OrderSubmissionRejected("SX Bet V3 prepared order is missing, expired, or already consumed")
        submitted = prepared.submitted
        if (
            submitted.token_id != token_id
            or submitted.synthetic_side is not synthetic_side
            or submitted.actual_side is not actual_side
            or submitted.requested_contracts != requested_contracts
            or submitted.requested_price != requested_price
            or submitted.action != action
        ):
            raise OrderSubmissionRejected("SX Bet V3 prepared order does not match the authorized entry")
        if _v3_order_payload_fingerprint(submitted.order_id, prepared.payload) != fingerprint:
            raise OrderSubmissionRejected("SX Bet V3 prepared order fingerprint is invalid")
        return prepared

    @staticmethod
    def _assert_submission_window(order_id: str, submission_deadline_unix: float | None) -> None:
        if submission_deadline_unix is None:
            return
        deadline = Decimal(str(submission_deadline_unix))
        if not deadline.is_finite() or deadline <= 0:
            raise OrderSubmissionRejected("SX Bet V3 market submission deadline is invalid", order_id=order_id)
        required_margin = Decimal(str(_V3_SUBMISSION_CUTOFF_BUFFER_SECONDS + _V3_TRANSPORT_START_ALLOWANCE_SECONDS))
        if Decimal(str(time.time())) + required_margin >= deadline:
            raise OrderSubmissionRejected(
                "SX Bet V3 entry lacks the final 15-second cutoff buffer plus transport-start allowance",
                order_id=order_id,
            )

    @staticmethod
    def _assert_signed_order_expiry(order_id: str, payload: dict[str, Any]) -> None:
        try:
            expiry = Decimal(str(payload["expiry"]))
        except (KeyError, ArithmeticError, TypeError, ValueError) as exc:
            raise OrderSubmissionRejected(
                "SX Bet V3 signed order expiry is invalid",
                order_id=order_id,
            ) from exc
        if (
            not expiry.is_finite()
            or Decimal(str(time.time())) + Decimal(str(_V3_TRANSPORT_START_ALLOWANCE_SECONDS)) >= expiry
        ):
            raise OrderSubmissionRejected(
                "SX Bet V3 signed order may expire before transport starts",
                order_id=order_id,
            )

    async def wait_filled(self, order_id: str, timeout_ms: int) -> ExecutionReport:
        deadline = asyncio.get_running_loop().time() + timeout_ms / 1000
        while True:
            report = await self.get_order(order_id)
            if report.status is not ExecutionStatus.OPEN:
                await self._disarm_account_heartbeat_if_safe()
                return report
            if asyncio.get_running_loop().time() >= deadline:
                return report
            await asyncio.sleep(0.25)

    async def get_order(self, order_id: str) -> ExecutionReport:
        normalized_order_id = order_id.lower()
        cached = self._reports.get(normalized_order_id)
        if cached is not None and cached.status is not ExecutionStatus.OPEN:
            await self._disarm_account_heartbeat_if_safe()
            return cached
        if normalized_order_id in self._submitted_orders:
            await self._refresh_account_heartbeat_if_due()
        payload = await self._request_json("GET", f"/orders-v3/{order_id}")
        data = _response_data(payload)
        order = data.get("order", data) if isinstance(data, dict) else None
        if not isinstance(order, dict):
            raise RuntimeError("SX Bet V3 order response is malformed")
        remote_order_id = str(order.get("id") or order.get("orderId") or "").lower()
        if remote_order_id != order_id.lower():
            raise RuntimeError(f"SX Bet V3 order response id does not match {order_id}")
        submitted = self._submitted_orders.get(normalized_order_id)
        if submitted is None:
            raise RuntimeError(f"SX Bet V3 durable order context must be restored before reconciling {order_id}")
        remote_market_hash = str(order.get("marketHash") or "")
        remote_side = _remote_order_side(order)
        if remote_market_hash.lower() != submitted.market_hash.lower():
            raise RuntimeError("SX Bet V3 remote order market does not match durable intent")
        if remote_side is not submitted.actual_side:
            raise RuntimeError("SX Bet V3 remote order side does not match durable intent")
        submitted = await self._with_remote_submitted_stake(submitted, order)
        async with self._heartbeat_lock:
            self._submitted_orders[normalized_order_id] = submitted
        status = str(order.get("status") or "").upper()
        if status in {"PENDING", "ACTIVE"}:
            return cached or ExecutionReport.from_amounts(order_id, submitted.requested_contracts, Decimal(0), "open")
        if status != "INACTIVE":
            raise RuntimeError(f"SX Bet V3 order {order_id} has unsupported status {status or 'MISSING'}")
        inactive_reason = str(order.get("inactiveReason") or "").upper()
        if inactive_reason not in _V3_INACTIVE_REASONS:
            raise RuntimeError(
                f"SX Bet V3 order {order_id} has unsupported inactiveReason {inactive_reason or 'MISSING'}"
            )
        fills = await self._fills_for_terminal_order(submitted, inactive_reason)
        report = _report_from_v3_fills(submitted, fills, inactive_reason=inactive_reason)
        await self._store_report(normalized_order_id, report)
        return report

    async def _track_submitted_order(self, submitted: _V3SubmittedOrder) -> None:
        async with self._heartbeat_lock:
            self._submitted_orders[submitted.order_id] = submitted
            try:
                await self._arm_account_heartbeat_locked()
            except BaseException:
                self._submitted_orders.pop(submitted.order_id, None)
                raise

    async def _drop_submitted_order(self, order_id: str) -> None:
        async with self._heartbeat_lock:
            self._submitted_orders.pop(order_id.lower(), None)
            await self._disarm_account_heartbeat_if_safe_locked()

    async def _store_report(self, order_id: str, report: ExecutionReport) -> None:
        async with self._heartbeat_lock:
            self._reports[order_id.lower()] = report
            if report.status is not ExecutionStatus.OPEN:
                await self._disarm_account_heartbeat_if_safe_locked()

    async def _arm_account_heartbeat(self) -> None:
        async with self._heartbeat_lock:
            await self._arm_account_heartbeat_locked()

    async def _arm_account_heartbeat_locked(self) -> None:
        await self._set_account_heartbeat(_V3_HEARTBEAT_TIMEOUT_SECONDS)
        self._heartbeat_armed = True
        self._heartbeat_last_refresh_at = time.monotonic()

    async def _refresh_account_heartbeat_if_due(self) -> None:
        async with self._heartbeat_lock:
            if not self._heartbeat_armed or (
                time.monotonic() - self._heartbeat_last_refresh_at >= _V3_HEARTBEAT_REFRESH_SECONDS
            ):
                await self._arm_account_heartbeat_locked()

    async def _set_account_heartbeat(self, timeout_seconds: int) -> None:
        payload = await self._request_json(
            "POST",
            "/heartbeat/v3",
            json_body={"timeoutSeconds": timeout_seconds},
        )
        data = _response_data(payload)
        if not isinstance(data, dict):
            raise RuntimeError("SX Bet V3 heartbeat response is malformed")
        if timeout_seconds > 0 and not data.get("expiresAt"):
            raise RuntimeError("SX Bet V3 heartbeat response is missing expiresAt")

    def _has_possibly_open_orders(self) -> bool:
        return any(
            (report := self._reports.get(order_id)) is None or report.status is ExecutionStatus.OPEN
            for order_id in self._submitted_orders
        )

    async def _disarm_account_heartbeat_if_safe(self) -> None:
        async with self._heartbeat_lock:
            await self._disarm_account_heartbeat_if_safe_locked()

    async def _disarm_account_heartbeat_if_safe_locked(self) -> None:
        if not self._heartbeat_armed or self._has_possibly_open_orders():
            return
        try:
            await self._set_account_heartbeat(0)
        except Exception:
            LOGGER.exception("sx_bet_v3_heartbeat_disarm_failed")
            return
        if self._has_possibly_open_orders():
            await self._arm_account_heartbeat_locked()
            return
        self._heartbeat_armed = False
        self._heartbeat_last_refresh_at = 0.0

    async def restore_order_context(self, order_id: str, intent: OrderIntent) -> None:
        normalized_order_id = order_id.lower()
        if normalized_order_id in self._submitted_orders:
            return
        submitted = await self._submitted_order_from_intent(normalized_order_id, intent)
        self._historical_order_contexts.pop(normalized_order_id, None)
        await self._track_submitted_order(submitted)

    async def restore_fill_context(self, order_id: str, intent: OrderIntent) -> None:
        normalized_order_id = order_id.lower()
        if (
            normalized_order_id in self._submitted_orders
            or normalized_order_id in self._historical_order_contexts
        ):
            return
        self._remember_historical_order_context(
            await self._submitted_order_from_intent(normalized_order_id, intent)
        )

    async def _submitted_order_from_intent(
        self,
        normalized_order_id: str,
        intent: OrderIntent,
    ) -> _V3SubmittedOrder:
        action = intent.action.upper()
        if action not in {"BUY", "SELL"}:
            raise RuntimeError(f"SX Bet V3 durable order action is invalid: {intent.action}")
        identity = self._market_identifiers.get(intent.token_id)
        if identity is None:
            market_hash, separator, raw_side = intent.token_id.rpartition(":")
            if not separator:
                raise RuntimeError("SX Bet V3 durable token id does not contain market identity")
            try:
                registered_side = BinarySide(raw_side)
            except ValueError as exc:
                raise RuntimeError("SX Bet V3 durable token id has an invalid outcome side") from exc
            self.register_market(intent.token_id, market_hash, registered_side)
            identity = self._market_identifiers.get(intent.token_id)
        if identity is None:
            raise RuntimeError("SX Bet V3 durable token identity could not be restored")
        market_hash, registered_side = identity
        if registered_side is not intent.binary_side:
            raise RuntimeError("SX Bet V3 durable token side does not match the order intent")
        actual_side = registered_side if action == "BUY" else opposite_binary_side(registered_side)
        actual_price = intent.limit_price if action == "BUY" else Decimal(1) - intent.limit_price
        if intent.quantity <= 0 or actual_price <= 0 or actual_price >= 1:
            raise RuntimeError("SX Bet V3 durable order economics are invalid")
        refund_fee_rate = (await self._fee_schedule()).refund_fee if action == "SELL" else Decimal(0)
        submitted_at = intent.created_at if intent.created_at.tzinfo else intent.created_at.replace(tzinfo=UTC)
        return _V3SubmittedOrder(
            order_id=normalized_order_id,
            market_hash=market_hash,
            token_id=intent.token_id,
            action=action,
            synthetic_side=registered_side,
            actual_side=actual_side,
            requested_contracts=intent.quantity,
            requested_price=intent.limit_price,
            submitted_stake=intent.quantity * actual_price,
            submitted_at=submitted_at.astimezone(UTC),
            asset_decimals=await self._asset_decimals(),
            refund_fee_rate=refund_fee_rate,
            submitted_stake_verified=False,
        )

    def _remember_historical_order_context(self, submitted: _V3SubmittedOrder) -> None:
        self._historical_order_contexts.pop(submitted.order_id, None)
        self._historical_order_contexts[submitted.order_id] = submitted
        while len(self._historical_order_contexts) > _V3_HISTORICAL_ORDER_CONTEXT_LIMIT:
            self._historical_order_contexts.popitem(last=False)

    async def _with_remote_submitted_stake(
        self,
        submitted: _V3SubmittedOrder,
        remote_order: dict[str, Any],
    ) -> _V3SubmittedOrder:
        raw_stake = remote_order.get("totalBetSize")
        if raw_stake is None:
            if submitted.submitted_stake_verified:
                return submitted
            raise RuntimeError("SX Bet V3 remote order is missing totalBetSize required after restart")
        try:
            stake_units = Decimal(str(raw_stake))
            decimals = await self._asset_decimals()
            if decimals != submitted.asset_decimals:
                raise RuntimeError("SX Bet V3 active asset decimals changed after order submission")
            submitted_stake = _from_base_units(stake_units, decimals)
        except (ArithmeticError, TypeError, ValueError) as exc:
            raise RuntimeError("SX Bet V3 remote order has invalid totalBetSize") from exc
        if not submitted_stake.is_finite() or submitted_stake <= 0:
            raise RuntimeError("SX Bet V3 remote order has invalid totalBetSize")
        return replace(submitted, submitted_stake=submitted_stake, submitted_stake_verified=True)

    async def _fills_for_terminal_order(
        self,
        submitted: _V3SubmittedOrder,
        inactive_reason: str,
    ) -> list[dict[str, Any]]:
        attempts = _V3_FILL_INDEX_RETRIES if inactive_reason == "FILLED" or submitted.action == "SELL" else 1
        for attempt in range(attempts):
            fills = await self._fills_for_order(submitted)
            refund_accounting_pending = submitted.action == "SELL" and any(
                _v3_fill_status(fill) in _V3_IRREVERSIBLE_BET_STATES and not _sell_refund_accounting_is_indexed(fill)
                for fill in fills
            )
            if (fills or inactive_reason != "FILLED") and not refund_accounting_pending:
                return fills
            if attempt + 1 < attempts:
                await asyncio.sleep(0.25 * (attempt + 1))
        raise RuntimeError(f"SX Bet V3 order {submitted.order_id} is FILLED but fills are not indexed yet")

    async def _fills_for_order(self, submitted: _V3SubmittedOrder) -> list[dict[str, Any]]:
        try:
            rows = await self._list_v3_records(
                "/fills-v3",
                "fills",
                {"orderId": submitted.order_id},
            )
        except RuntimeError as exc:
            if "failed with 400" not in str(exc):
                raise
            # The current taker guide documents orderId while one generated
            # API reference omits it. Keep a bounded compatibility fallback.
            rows = await self._list_v3_records(
                "/fills-v3",
                "fills",
                {"startDate": submitted.submitted_at.astimezone(UTC).isoformat().replace("+00:00", "Z")},
            )
        return [row for row in rows if str(row.get("orderId") or "").lower() == submitted.order_id.lower()]

    async def cancel_order(self, order_id: str) -> None:
        payload = await self._request_json("DELETE", "/orders-v3", json_body={"orders": [{"orderId": order_id}]})
        data = _response_data(payload)
        if not isinstance(data, dict):
            raise RuntimeError("SX Bet V3 cancel response is malformed")
        cancelled = _extract_records(data, ("cancelled",))
        if any(str(item.get("orderId") or "").lower() == order_id.lower() for item in cancelled):
            return
        ambiguous = _extract_records(data, ("notCancelled",)) + _extract_records(data, ("unconfirmed",))
        row = next(
            (item for item in ambiguous if str(item.get("orderId") or "").lower() == order_id.lower()),
            None,
        )
        if row is None:
            raise RuntimeError("SX Bet V3 order cancellation response omitted the requested order")
        await self._confirm_order_inactive_after_cancel(order_id, str(row.get("reason") or "unknown"))

    async def _confirm_order_inactive_after_cancel(self, order_id: str, cancel_reason: str) -> None:
        for attempt in range(3):
            payload = await self._request_json("GET", f"/orders-v3/{order_id}")
            data = _response_data(payload)
            order = data.get("order", data) if isinstance(data, dict) else None
            if not isinstance(order, dict):
                raise RuntimeError("SX Bet V3 cancel confirmation order response is malformed")
            remote_order_id = str(order.get("id") or order.get("orderId") or "").lower()
            if remote_order_id != order_id.lower():
                raise RuntimeError("SX Bet V3 cancel confirmation returned a different order")
            status = str(order.get("status") or "").upper()
            if status in {"PENDING", "ACTIVE"}:
                if attempt < 2:
                    await asyncio.sleep(0.25 * (attempt + 1))
                    continue
                raise RuntimeError(
                    f"SX Bet V3 order cancellation remains unconfirmed ({cancel_reason}): order is still {status}"
                )
            if status != "INACTIVE":
                raise RuntimeError(f"SX Bet V3 cancel confirmation has unsupported order status {status or 'MISSING'}")
            inactive_reason = str(order.get("inactiveReason") or "").upper()
            if inactive_reason not in _V3_INACTIVE_REASONS:
                raise RuntimeError(
                    f"SX Bet V3 cancel confirmation has unsupported inactiveReason {inactive_reason or 'MISSING'}"
                )
            fills = await self._list_v3_records("/fills-v3", "fills", {"orderId": order_id})
            non_failed_states = {state for state in (_v3_fill_status(fill) for fill in fills) if state != "FAILED"}
            if inactive_reason == "FILLED" or non_failed_states:
                raise RuntimeError(
                    "SX Bet V3 order became matched or filled while cancellation was attempted; "
                    "fill reconciliation is required"
                )
            return
        raise AssertionError("unreachable")

    async def get_cash_balance(self) -> float:
        return float((await self.get_cash_balance_details())["balance"])

    async def get_cash_balance_details(self) -> dict[str, Any]:
        proxy = await self._ensure_proxy_ready()
        metadata = await self._metadata()
        active_asset = metadata.get("activeAsset")
        if not isinstance(active_asset, dict):
            raise RuntimeError("SX Bet V3 metadata is missing activeAsset")
        token_address = str(active_asset.get("baseToken") or "")
        escrow_address = str(active_asset.get("escrowAddress") or "")
        decimals = _asset_decimals_from_metadata(metadata)
        if not self._config.private_key:
            raise RuntimeError("SX Bet private_key is required to validate the V3 account balance")
        account_address = _account_from_private_key(self._config.private_key).address
        proxy_address = str(proxy.get("obv3ProxyWalletAddress") or "")
        payload = await self._request_json("GET", "/user/balance-v3")
        balances = _extract_records(payload, ("balances",))
        row = next(
            (
                item
                for item in balances
                if str(item.get("tokenAddress") or "").lower() == token_address.lower()
                and str(item.get("escrowAddress") or "").lower() == escrow_address.lower()
                and str(item.get("userAddress") or "").lower() == account_address.lower()
                and str(item.get("wallet") or "").lower() == proxy_address.lower()
            ),
            None,
        )
        if balances and row is None:
            raise RuntimeError("SX Bet V3 balance rows do not match the signer, proxy, token, and escrow")
        if row is None:
            available = pending_available = escrowed = pending_escrow = Decimal(0)
        else:
            available = _parse_balance_units(row.get("availableAmount"), "availableAmount", allow_negative=False)
            pending_available = _parse_balance_units(
                row.get("pendingAvailableAmount"),
                "pendingAvailableAmount",
                allow_negative=True,
            )
            escrowed = _parse_balance_units(row.get("escrowedAmount"), "escrowedAmount", allow_negative=False)
            pending_escrow = _parse_balance_units(
                row.get("pendingEscrowAmount"),
                "pendingEscrowAmount",
                allow_negative=True,
            )
        spendable_units = available + min(pending_available, Decimal(0))
        return {
            "wallet_address": proxy_address,
            "user_address": account_address,
            "escrow_address": escrow_address,
            "base_token_address": token_address,
            "balance_raw": str(spendable_units),
            "decimals": decimals,
            "balance": float(_from_base_units(spendable_units, decimals)),
            "pending_available": str(_from_base_units(pending_available, decimals)),
            "escrowed": str(_from_base_units(escrowed, decimals)),
            "pending_escrow": str(_from_base_units(pending_escrow, decimals)),
            "proxy_deployed": True,
        }

    async def _ensure_proxy_ready(self) -> dict[str, Any]:
        now = time.monotonic()
        if self._proxy_cache is not None and now - self._proxy_cache[0] <= _FEE_CACHE_SECONDS:
            proxy = self._proxy_cache[1]
        else:
            payload = await self._request_json("GET", "/user/proxy")
            data = _response_data(payload)
            if not isinstance(data, dict):
                raise RuntimeError("SX Bet V3 proxy response is malformed")
            proxy = data
            self._proxy_cache = (now, proxy)
        if not bool(proxy.get("deployed")):
            raise RuntimeError("SX Bet V3 proxy wallet is not deployed")
        if not proxy.get("obv3ProxyWalletAddress"):
            raise RuntimeError("SX Bet V3 proxy response is missing wallet address")
        return proxy

    async def get_market_constraints(self, token_id: str, condition_id: str | None = None) -> MarketConstraints | None:
        del condition_id
        if token_id not in self._market_identifiers:
            return None
        metadata = await self._metadata()
        limits = metadata.get("limits")
        active_asset = metadata.get("activeAsset")
        if not isinstance(limits, dict) or not isinstance(active_asset, dict):
            return None
        decimals = _asset_decimals_from_metadata(metadata)
        fee_rate = (await self._fee_schedule()).taker_payout_fee
        return MarketConstraints(
            fee_rate_bps=int((fee_rate * Decimal(10_000)).to_integral_value(rounding=ROUND_CEILING)),
            tick_size=Decimal(str(metadata.get("oddsLadderStepSize", 0))) / Decimal(100_000),
            lot_size=Decimal(1) / (Decimal(10) ** decimals),
            minimum_notional=_from_base_units(Decimal(str(limits.get("orderSizeMinimumBaseUnits", "0"))), decimals),
        )

    async def get_fee_quote(
        self,
        token_id: str,
        average_price: Decimal,
        constraints: MarketConstraints | None = None,
    ) -> VenueFeeQuote | None:
        del average_price
        if constraints is None and await self.get_market_constraints(token_id) is None:
            return None
        rate = (await self._fee_schedule()).taker_payout_fee
        return VenueFeeQuote(
            venue="SX Bet",
            fee_rate_bps=int((rate * Decimal(10_000)).to_integral_value(rounding=ROUND_CEILING)),
            model="sx_payout_profit",
            source="sx_user_fees_v3",
            verified=True,
            fee_rate_fraction=rate,
        )

    async def _fee_schedule(self) -> _V3FeeSchedule:
        now = time.monotonic()
        if self._fee_cache is not None and now - self._fee_cache[0] <= _FEE_CACHE_SECONDS:
            return self._fee_cache[1]
        payload = await self._request_json("GET", "/user/fees-v3")
        data = _response_data(payload)
        if not isinstance(data, dict) or "takerPayoutFee" not in data or "refundFee" not in data:
            raise RuntimeError("SX Bet V3 fee response is missing takerPayoutFee or refundFee")
        taker_rate = _nullable_fee_rate(data.get("takerPayoutFee"), "takerPayoutFee")
        refund_rate = _nullable_fee_rate(data.get("refundFee"), "refundFee")
        schedule = _V3FeeSchedule(taker_payout_fee=taker_rate, refund_fee=refund_rate)
        self._fee_cache = (now, schedule)
        return schedule

    async def list_open_orders(self) -> list[VenueOrder]:
        rows = await self._list_v3_records("/orders-v3", "orders")
        asset_decimals = await self._asset_decimals()
        orders: list[VenueOrder] = []
        for row in rows:
            odds = _odds_units_to_probability(row.get("percentageOdds"))
            if odds <= 0:
                continue
            remaining_stake = _from_base_units(
                Decimal(str(row.get("remainingSize") or "0")),
                asset_decimals,
            )
            orders.append(
                VenueOrder(
                    client_order_id=str(row.get("clientOrderId") or ""),
                    venue_order_id=str(row.get("id") or ""),
                    venue="SX Bet",
                    status=OrderIntentStatus.ACKNOWLEDGED,
                    quantity=remaining_stake / odds,
                    cumulative_filled=Decimal(0),
                    average_price=odds,
                    updated_at=_parse_datetime(row.get("updatedAt")),
                )
            )
        return orders

    async def list_fills(self, since: datetime | None = None) -> list[FillRecord]:
        params: dict[str, Any] = {}
        if since is not None:
            params["startDate"] = since.astimezone(UTC).isoformat().replace("+00:00", "Z")
        rows = await self._list_v3_records("/fills-v3", "fills", params)
        asset_decimals = await self._asset_decimals()
        fills: list[FillRecord] = []
        residual_accounting: dict[
            str,
            tuple[Decimal, Decimal, Decimal, Decimal, BinarySide],
        ] = {}
        for row in rows:
            if _v3_fill_status(row) not in _V3_IRREVERSIBLE_BET_STATES:
                continue
            odds = _odds_units_to_probability(row.get("fillOdds"))
            if odds <= 0:
                continue
            stake = _from_base_units(Decimal(str(row.get("fillAmount") or "0")), asset_decimals)
            normalized_order_id = str(row.get("orderId") or "").lower()
            submitted = self._submitted_orders.get(normalized_order_id) or self._historical_order_contexts.get(
                normalized_order_id
            )
            reported_price = odds
            reported_quantity = stake / odds
            refund_fee = _ce_refund_fee(row, asset_decimals)
            has_ce_refund = _has_positive_ce_refund(row, asset_decimals)
            if has_ce_refund or (submitted is not None and submitted.action == "SELL"):
                reported_quantity, net_proceeds, refund_fee, residual_opposite = _sell_fill_accounting(
                    row,
                    stake=stake,
                    odds=odds,
                    asset_decimals=asset_decimals,
                )
                reported_price = (
                    net_proceeds / reported_quantity if reported_quantity > _V3_STAKE_INDEX_TOLERANCE else Decimal(0)
                )
                if not normalized_order_id:
                    raise RuntimeError("SX Bet V3 CE fill is missing orderId")
                requested = (
                    submitted.requested_contracts
                    if submitted is not None
                    else reported_quantity + residual_opposite
                )
                residual_side = submitted.actual_side if submitted is not None else _remote_order_side(row)
                previous = residual_accounting.get(normalized_order_id)
                if previous is not None and previous[4] is not residual_side:
                    raise RuntimeError("SX Bet V3 CE fills for one order disagree on outcome side")
                residual_accounting[normalized_order_id] = (
                    max(previous[0] if previous is not None else Decimal(0), requested),
                    (previous[1] if previous is not None else Decimal(0)) + reported_quantity,
                    (previous[2] if previous is not None else Decimal(0)) + net_proceeds,
                    (previous[3] if previous is not None else Decimal(0)) + residual_opposite,
                    residual_side,
                )
            fills.append(
                FillRecord(
                    fill_id=str(row.get("id") or row.get("matchId") or ""),
                    client_order_id="",
                    venue_order_id=str(row.get("orderId") or ""),
                    venue="SX Bet",
                    quantity=reported_quantity,
                    price=reported_price,
                    fee=refund_fee,
                    occurred_at=_parse_datetime(row.get("createdAt")),
                )
            )
        residual_exposures: list[OrderResidualExposure] = []
        for order_id, (requested, closed, proceeds, residual, residual_side) in residual_accounting.items():
            if residual <= _V3_STAKE_INDEX_TOLERANCE:
                continue
            average_price = proceeds / closed if closed > _V3_STAKE_INDEX_TOLERANCE else Decimal(0)
            residual_exposures.append(
                OrderResidualExposure(
                    "SX Bet V3 historical fills created residual opposite exposure "
                    f"({residual} contracts)",
                    report=ExecutionReport.from_amounts(
                        order_id,
                        max(requested, closed),
                        closed,
                        ExecutionStatus.PARTIAL,
                        average_price,
                    ),
                    residual_contracts=residual,
                    residual_side=residual_side,
                )
            )
        if residual_exposures:
            raise OrderResidualExposureBatch(residual_exposures, fills=fills)
        return fills

    async def get_positions(self) -> dict[str, Decimal]:
        rows = await self._list_v3_records(
            "/positions-v3",
            "positions",
            {"status": "MATCHED,LOCKED"},
        )
        asset_decimals = await self._asset_decimals()
        positions: dict[str, Decimal] = {}
        for row in rows:
            market_hash = str(row.get("marketHash") or "")
            max_win = _from_base_units(Decimal(str(row.get("maxWin") or "0")), asset_decimals)
            max_loss = _from_base_units(Decimal(str(row.get("maxLoss") or "0")), asset_decimals)
            contracts = abs(max_win - max_loss)
            if not market_hash or contracts <= 0:
                continue
            side = BinarySide.YES if bool(row.get("isOutcomeOneMaxWin")) else BinarySide.NO
            token_id = self._token_by_market_side.get((market_hash, side), f"{market_hash}:{side.value}")
            positions[token_id] = contracts
        return positions

    async def _list_v3_records(
        self,
        path: str,
        key: str,
        query: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        next_key: str | None = None
        seen_next_keys: set[str] = set()
        for _page in range(_V3_MAX_RECORD_PAGES):
            params = dict(query or {})
            params.setdefault("perPage", 100)
            if next_key:
                params["nextKey"] = next_key
            payload = await self._request_json("GET", path, query_params=params)
            data = _response_data(payload)
            if not isinstance(data, dict):
                raise RuntimeError(f"SX Bet V3 {path} response is malformed")
            page = data.get(key, [])
            if not isinstance(page, list):
                raise RuntimeError(f"SX Bet V3 {path} response is missing {key}")
            records.extend(item for item in page if isinstance(item, dict))
            next_key = str(data.get("nextKey") or "") or None
            if not next_key:
                return records
            if next_key in seen_next_keys:
                raise RuntimeError(f"SX Bet V3 {path} pagination repeated a cursor")
            seen_next_keys.add(next_key)
        raise RuntimeError(f"SX Bet V3 {path} exceeded {_V3_MAX_RECORD_PAGES} pages")

    def supports_full_reconciliation(self) -> bool:
        return True

    def supports_automatic_redemption(self) -> bool:
        return True

    def prepare_settlement_request(self, request: SettlementRequest) -> SettlementRequest:
        return request

    async def get_settlement_status(self, request: SettlementRequest) -> SettlementStatus:
        rows = await self._list_v3_records("/trades-v3", "trades", {"marketHash": request.market_id})
        relevant = [row for row in rows if str(row.get("marketHash") or "") == request.market_id]
        if not relevant:
            return SettlementStatus.MANUAL_REVIEW
        statuses = {str(row.get("status") or "").upper() for row in relevant}
        if statuses & {"MATCHED", "LOCKED"}:
            return SettlementStatus.OPEN
        if statuses == {"SETTLED"}:
            return SettlementStatus.SETTLED
        return SettlementStatus.MANUAL_REVIEW

    async def redeem_position(self, request: SettlementRequest, redemption_id: str) -> RedemptionReport:
        del redemption_id
        status = await self.get_settlement_status(request)
        if status is SettlementStatus.SETTLED:
            return RedemptionReport(RedemptionIntentStatus.CONFIRMED)
        if status is SettlementStatus.OPEN:
            raise RuntimeError("SX Bet V3 positions settle on venue and cannot be force-redeemed early")
        return RedemptionReport(RedemptionIntentStatus.MANUAL_REVIEW, error=f"SX Bet V3 settlement is {status.value}")

    def sync_market_data_targets(self, token_ids: set[str]) -> None:
        normalized = {token_id for token_id in token_ids if token_id}
        was_operational = self._market_data_window_operational()
        previous_markets = self._active_market_hashes()
        self._tracked_tokens = normalized
        current_markets = self._active_market_hashes()
        for market_hash in sorted(previous_markets - current_markets):
            self._prune_market(market_hash)
            self._subscription_queue.put_nowait(("unsubscribe", market_hash))
        if self._ws_connected:
            for market_hash in sorted(current_markets - previous_markets):
                self._subscription_queue.put_nowait(("subscribe", market_hash))
        if current_markets - previous_markets and was_operational:
            self._target_transition_deadline = time.monotonic() + _TARGET_TRANSITION_GRACE_SECONDS
        elif not normalized or self.market_data_ready():
            self._target_transition_deadline = 0.0
        if normalized:
            self._ensure_ws_task()

    async def prime_market_data_targets(self) -> None:
        if not self._ws_connected:
            return
        waiters = [
            asyncio.create_task(self._book_events.setdefault(token_id, asyncio.Event()).wait())
            for token_id in self._tracked_tokens
            if token_id not in self._books
        ]
        if not waiters:
            return
        _, pending = await asyncio.wait(waiters, timeout=_WS_SNAPSHOT_PRIME_TIMEOUT_SECONDS)
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)

    def has_active_market_data_targets(self) -> bool:
        return bool(self._tracked_tokens)

    def active_market_data_target_count(self) -> int:
        return len(self._tracked_tokens)

    def market_data_ready(self) -> bool:
        if self._config.api_key and self._config.ws_url and not self._ws_connected:
            return False
        if (
            self._config.api_key
            and self._config.ws_url
            and not self._active_market_hashes().issubset(self._subscribed_markets)
        ):
            return False
        return bool(self._tracked_tokens) and all(
            token_id in self._books
            and self._books[token_id].status is MarketDataStatus.VALID
            and bool(self._books[token_id].asks)
            for token_id in self._tracked_tokens
        )

    def _market_data_window_operational(self) -> bool:
        return (
            self._ws_connected
            and bool(self._tracked_tokens)
            and any(token_id in self._book_timestamps for token_id in self._tracked_tokens)
        )

    def market_data_transitioning(self) -> bool:
        return (
            self._ws_connected
            and bool(self._tracked_tokens)
            and time.monotonic() <= self._target_transition_deadline
            and not self.market_data_ready()
        )

    def is_order_book_execution_fresh(self, token_id: str, book: OrderBook, max_age_seconds: float) -> bool:
        if book.status is MarketDataStatus.VALID and self._healthy_stream_confirms_book(token_id):
            return True
        updated = self._book_timestamps.get(token_id)
        return (
            book.status is MarketDataStatus.VALID
            and updated is not None
            and max(0.0, time.monotonic() - updated) <= max_age_seconds
        )

    async def reconnect_market_data(self) -> None:
        self._mark_books_stale()
        if self._ws is not None and not self._ws.closed:
            await self._ws.close()
        if self._tracked_tokens and (self._ws_task is None or self._ws_task.done()):
            self._ensure_ws_task()

    def telemetry_snapshot(self) -> dict[str, float]:
        return {
            "connected": float(self._ws_connected),
            "reconnects": float(self._reconnect_count),
            "sequence_gaps": float(self._sequence_gap_count),
            "reconnect_backoff_seconds": self._reconnect_backoff.current_delay_seconds,
            "account_heartbeat_armed": float(self._heartbeat_armed),
        }

    def market_data_age_seconds(self) -> float | None:
        timestamps = [self._book_timestamps[token] for token in self._tracked_tokens if token in self._book_timestamps]
        return None if not timestamps else max(0.0, time.monotonic() - max(timestamps))

    def market_data_target_age_seconds(self, token_id: str) -> float | None:
        timestamp = self._book_timestamps.get(token_id)
        if timestamp is None:
            return None
        return max(0.0, time.monotonic() - timestamp)

    def market_data_target_ready(self, token_id: str, max_age_seconds: float) -> bool:
        book = self._books.get(token_id)
        return (
            token_id in self._tracked_tokens
            and book is not None
            and bool(book.asks)
            and self.market_data_target_age_seconds(token_id) is not None
            and self.is_order_book_execution_fresh(token_id, book, max_age_seconds)
        )

    def forget_order(self, order_id: str) -> None:
        normalized_order_id = order_id.lower()
        submitted = self._submitted_orders.pop(normalized_order_id, None)
        if submitted is not None:
            self._remember_historical_order_context(submitted)
        self._reports.pop(normalized_order_id, None)

    async def close(self) -> None:
        await self._disarm_account_heartbeat_if_safe()
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
        await self._close_ws_session()
        if self._rest_session is not None:
            await self._rest_session.close()
            self._rest_session = None
        self._prepared_orders.clear()
        self._claimed_prepared_orders.clear()
        self._historical_order_contexts.clear()

    async def _metadata(self) -> dict[str, Any]:
        if self._metadata_cache is None:
            payload = await self._request_json("GET", "/metadata/obv3")
            data = _response_data(payload)
            if not isinstance(data, dict):
                raise RuntimeError("SX Bet V3 metadata response is malformed")
            _validate_v3_metadata(data)
            self._metadata_cache = data
        return self._metadata_cache

    async def _asset_decimals(self) -> int:
        return _asset_decimals_from_metadata(await self._metadata())

    def _cached_asset_decimals(self) -> int:
        if self._metadata_cache is None:
            raise RuntimeError("SX Bet V3 metadata must be loaded before applying an order book")
        return _asset_decimals_from_metadata(self._metadata_cache)

    def _active_market_hashes(self) -> set[str]:
        return {
            self._market_identifiers[token_id][0]
            for token_id in self._tracked_tokens
            if token_id in self._market_identifiers
        }

    def _prune_market(self, market_hash: str) -> None:
        task = self._bootstrap_tasks.pop(market_hash, None)
        if task is not None:
            task.cancel()
        self._book_versions.pop(market_hash, None)
        self._bootstrap_locks.pop(market_hash, None)
        self._subscription_positions.pop(f"orderbook_v3:{market_hash}", None)
        for (registered_market, _), token_id in self._token_by_market_side.items():
            if registered_market == market_hash:
                self._books.pop(token_id, None)
                self._book_timestamps.pop(token_id, None)
                self._book_events.pop(token_id, None)

    def _mark_market_books_stale(self, market_hash: str) -> None:
        for (registered_market, _), token_id in self._token_by_market_side.items():
            if registered_market == market_hash and token_id in self._books:
                self._books[token_id] = replace(self._books[token_id], status=MarketDataStatus.STALE)

    def _mark_books_stale(self) -> None:
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

    async def _request_json(
        self,
        method: str,
        path: str,
        *,
        query_params: dict[str, Any] | None = None,
        json_body: Any | None = None,
        before_request: Callable[[], None] | None = None,
    ) -> Any:
        try:
            import aiohttp
        except ImportError as exc:
            raise RuntimeError("aiohttp is required for SX Bet V3 REST connectivity") from exc
        if self._rest_session is None:
            headers = {"Accept": "application/json", "Content-Type": "application/json"}
            self._rest_session = client_session(headers)
        url = f"{self._config.api_base_url.rstrip('/')}{path}"
        request_headers = self._request_auth_headers(path)
        timeout = aiohttp.ClientTimeout(
            total=_V3_HTTP_TOTAL_TIMEOUT_SECONDS,
            connect=_V3_HTTP_CONNECT_TIMEOUT_SECONDS,
            sock_read=_V3_HTTP_READ_TIMEOUT_SECONDS,
        )
        normalized_method = method.upper()
        attempts = 3 if normalized_method == "GET" else 1
        last_error: BaseException | None = None
        for attempt in range(1, attempts + 1):
            retry_delay: float | None = None
            try:
                async with self._http_semaphore:
                    if before_request is not None:
                        before_request()
                    async with self._rest_session.request(
                        normalized_method,
                        url,
                        params=query_params,
                        json=json_body,
                        headers=request_headers or None,
                        timeout=timeout,
                        allow_redirects=False,
                    ) as response:
                        if response.status >= 300:
                            error = SxBetV3HttpError(normalized_method, path, response.status)
                            if response.status in _V3_RETRYABLE_GET_STATUSES and attempt < attempts:
                                candidate_delay = _v3_retry_delay_seconds(response, attempt)
                                if candidate_delay is None:
                                    raise error
                                last_error = error
                                retry_delay = candidate_delay
                            else:
                                raise error
                        else:
                            payload = await response.json(content_type=None)
                            return payload
                if retry_delay is not None:
                    await asyncio.sleep(retry_delay)
                    continue
            except (TimeoutError, aiohttp.ClientError) as exc:
                last_error = exc
                if attempt >= attempts:
                    raise
                await asyncio.sleep(0.2 * attempt)
        assert last_error is not None
        raise last_error

    def _request_auth_headers(self, path: str) -> dict[str, str]:
        if not self._config.api_key:
            return {}
        if path == "/trades-v3/public":
            return {}
        if path.startswith(_ACCOUNT_AUTHENTICATED_PREFIXES):
            return {"x-sx-api-key": self._config.api_key}
        return {}


def _asset_decimals_from_metadata(metadata: dict[str, Any]) -> int:
    active_asset = metadata.get("activeAsset")
    if not isinstance(active_asset, dict):
        raise RuntimeError("SX Bet V3 metadata is missing activeAsset")
    try:
        decimals = int(str(active_asset.get("decimals")))
    except (TypeError, ValueError) as exc:
        raise RuntimeError("SX Bet V3 active asset decimals are invalid") from exc
    if decimals < 0 or decimals > 36:
        raise RuntimeError("SX Bet V3 active asset decimals are outside the supported range")
    return decimals


def _response_data(payload: Any) -> Any:
    return payload.get("data") if isinstance(payload, dict) and "data" in payload else payload


def _v3_retry_delay_seconds(response: Any, attempt: int) -> float | None:
    headers = getattr(response, "headers", None)
    retry_after = headers.get("Retry-After") if headers is not None else None
    if retry_after is not None:
        try:
            parsed = Decimal(str(retry_after).strip())
        except (ArithmeticError, ValueError):
            parsed = Decimal("NaN")
        if parsed.is_finite() and 0 <= parsed <= Decimal(str(_V3_MAX_RETRY_AFTER_SECONDS)):
            return float(parsed)
        return None
    return 0.2 * attempt


def _validate_v3_metadata(metadata: dict[str, Any]) -> None:
    domain = metadata.get("domain")
    asset = metadata.get("activeAsset")
    limits = metadata.get("limits")
    if not isinstance(domain, dict) or not isinstance(asset, dict) or not isinstance(limits, dict):
        raise RuntimeError("SX Bet V3 metadata is missing domain, activeAsset, or limits")
    if str(domain.get("version") or "") != "1":
        raise RuntimeError("SX Bet V3 metadata has an unsupported EIP-712 domain version")
    try:
        chain_id = int(str(metadata.get("chainId")))
        domain_chain_id = int(str(domain.get("chainId")))
        decimals = int(str(asset.get("decimals")))
        ladder_step = Decimal(str(metadata.get("oddsLadderStepSize")))
        minimum_size = Decimal(str(limits.get("orderSizeMinimumBaseUnits")))
    except (ArithmeticError, TypeError, ValueError) as exc:
        raise RuntimeError("SX Bet V3 metadata contains invalid numeric fields") from exc
    if chain_id <= 0 or domain_chain_id != chain_id:
        raise RuntimeError("SX Bet V3 metadata domain chainId does not match chainId")
    if not 0 <= decimals <= 18 or ladder_step <= 0 or minimum_size <= 0:
        raise RuntimeError("SX Bet V3 metadata contains invalid asset or order limits")
    base_token = str(asset.get("baseToken") or "")
    escrow = str(asset.get("escrowAddress") or "")
    verifying_contract = str(domain.get("verifyingContract") or "")
    if not base_token or not escrow or not verifying_contract:
        raise RuntimeError("SX Bet V3 metadata is missing token or escrow addresses")
    if escrow.lower() != verifying_contract.lower():
        raise RuntimeError("SX Bet V3 metadata escrow does not match the EIP-712 verifying contract")


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


def _extract_realtime_token(payload: Any) -> str | None:
    data = _response_data(payload)
    if not isinstance(data, dict):
        return None
    token = data.get("token") or data.get("connectionToken")
    return str(token) if token else None


def _order_book_from_v3_maker_snapshot(
    payload: dict[str, Any],
    side: BinarySide,
    *,
    asset_decimals: int = 6,
) -> OrderBook:
    desired_key = "outcomeOne" if side is BinarySide.YES else "outcomeTwo"
    opposite_key = "outcomeTwo" if side is BinarySide.YES else "outcomeOne"
    bids = _v3_maker_levels(payload.get(desired_key), as_ask=False, asset_decimals=asset_decimals)
    asks = _v3_maker_levels(payload.get(opposite_key), as_ask=True, asset_decimals=asset_decimals)
    bids.sort(key=lambda level: level.price, reverse=True)
    asks.sort(key=lambda level: level.price)
    return OrderBook(
        bids=bids,
        asks=asks,
        raw_payload={"venue": "SX Bet", "api_version": "v3", "synthetic_side": side.value, "book": payload},
        sequence=_version_as_sequence(payload.get("version")),
        timestamp=time.time(),
    )


def _v3_maker_levels(
    raw_levels: Any,
    *,
    as_ask: bool,
    asset_decimals: int,
) -> list[OrderBookLevel]:
    if not isinstance(raw_levels, list):
        return []
    levels: list[OrderBookLevel] = []
    for raw in raw_levels:
        if not isinstance(raw, dict):
            continue
        maker_probability = _odds_units_to_probability(raw.get("percentageOdds"))
        maker_stake = _from_base_units(
            Decimal(str(raw.get("size") or "0")),
            asset_decimals,
        )
        if maker_probability <= 0 or maker_probability >= 1 or maker_stake <= 0:
            continue
        price = Decimal(1) - maker_probability if as_ask else maker_probability
        payout_contracts = maker_stake / maker_probability
        levels.append(OrderBookLevel(float(price), float(payout_contracts)))
    return levels


def _empty_v3_book(market_hash: str, side: BinarySide) -> OrderBook:
    return OrderBook(
        bids=[],
        asks=[],
        raw_payload={"venue": "SX Bet", "api_version": "v3", "marketHash": market_hash, "side": side.value},
        status=MarketDataStatus.INVALID,
    )


def _round_v3_probability(probability: Decimal, metadata: dict[str, Any]) -> Decimal:
    raw_step = Decimal(str(metadata.get("oddsLadderStepSize") or "0"))
    step = raw_step / Decimal(100_000)
    if step <= 0:
        raise RuntimeError("SX Bet V3 metadata odds ladder step is invalid")
    rounded = (probability / step).to_integral_value(rounding=ROUND_FLOOR) * step
    if rounded <= 0 or rounded >= 1:
        raise ValueError("SX Bet V3 rounded probability must be between 0 and 1")
    return rounded


def _stake_for_contracts(book: OrderBook, contracts: Decimal, limit_price: Decimal) -> Decimal:
    remaining = contracts
    spent = Decimal(0)
    for level in book.asks:
        price = Decimal(str(level.price))
        available = Decimal(str(level.size))
        if price <= 0 or price > limit_price or available <= 0:
            continue
        take = min(remaining, available)
        spent += take * price
        remaining -= take
        if remaining <= Decimal("1e-18"):
            break
    if remaining > Decimal("1e-18"):
        raise RuntimeError("SX Bet V3 orderbook has insufficient executable depth")
    return spent


def _account_from_private_key(private_key: str) -> Any:
    try:
        from eth_account import Account
    except ImportError as exc:
        raise RuntimeError("eth-account is required for SX Bet V3 order signing") from exc
    return Account.from_key(private_key)


def _v3_order_payload_fingerprint(order_id: str, payload: dict[str, Any]) -> str:
    signed_payload = {key: value for key, value in payload.items() if key != "clientOrderId"}
    canonical = json.dumps(signed_payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(f"{order_id}:{canonical}".encode()).hexdigest()


def _sign_v3_order(account: Any, domain: dict[str, Any], order: dict[str, Any]) -> tuple[str, str]:
    try:
        import eth_utils
        from eth_account.messages import encode_typed_data
    except ImportError as exc:
        raise RuntimeError("eth-account and eth-utils are required for SX Bet V3 order signing") from exc
    required_domain = {key: domain.get(key) for key in ("name", "version", "chainId", "verifyingContract")}
    if required_domain["version"] != "1" or not all(required_domain.values()):
        raise RuntimeError("SX Bet V3 EIP-712 domain is incomplete or has an unsupported version")
    types = {
        "Order": [
            {"name": "marketHash", "type": "bytes32"},
            {"name": "baseToken", "type": "address"},
            {"name": "totalBetSize", "type": "uint256"},
            {"name": "percentageOdds", "type": "uint256"},
            {"name": "salt", "type": "uint256"},
            {"name": "expiry", "type": "uint256"},
            {"name": "maker", "type": "address"},
            {"name": "isMakerBettingOutcomeOne", "type": "bool"},
        ]
    }
    message = {
        "marketHash": order["marketHash"],
        "baseToken": order["baseToken"],
        "totalBetSize": int(order["totalBetSize"]),
        "percentageOdds": int(order["percentageOdds"]),
        "salt": _uint256(order["salt"]),
        "expiry": int(order["expiry"]),
        "maker": order["maker"],
        "isMakerBettingOutcomeOne": bool(order["isMakerBettingOutcomeOne"]),
    }
    signable = encode_typed_data(domain_data=required_domain, message_types=types, message_data=message)
    signed = account.sign_message(signable)
    signature = signed.signature.hex()
    signature = signature if signature.startswith("0x") else f"0x{signature}"
    keccak: Any = eth_utils.keccak  # type: ignore[attr-defined]
    order_id = "0x" + keccak(b"\x19" + signable.version + signable.header + signable.body).hex()
    return signature, order_id.lower()


def _report_from_v3_outcome(submitted: _V3SubmittedOrder, outcome: dict[str, Any]) -> ExecutionReport:
    fill_stake = _from_base_units(
        Decimal(str(outcome.get("fillAmount") or "0")),
        submitted.asset_decimals,
    )
    odds = _odds_units_to_probability(outcome.get("blendedOdds"))
    matched_contracts = fill_stake / odds if odds > 0 else Decimal(0)
    state = str(outcome.get("state") or "").upper()
    if state == "PARTIAL_FILL_DONE" and matched_contracts <= 0:
        raise RuntimeError(f"SX Bet V3 order {submitted.order_id} is PARTIAL_FILL_DONE but has no fill")
    if state in {"TIMEOUT", "FULLY_FILLED", "PARTIAL_FILL_DONE"} or matched_contracts > 0:
        # Matching-engine outcomes are reversible until every fill reaches LOCKED.
        return ExecutionReport.from_amounts(
            submitted.order_id,
            max(submitted.requested_contracts, matched_contracts),
            Decimal(0),
            ExecutionStatus.OPEN,
            Decimal(0),
        )
    if state == "EXPIRED":
        status = ExecutionStatus.EXPIRED
    elif state in _V3_INACTIVE_REASONS:
        status = ExecutionStatus.CANCELLED
    else:
        # An unrecognized matching outcome is not proof that the order is gone.
        status = ExecutionStatus.OPEN
    return ExecutionReport.from_amounts(
        submitted.order_id,
        submitted.requested_contracts,
        Decimal(0),
        status,
        Decimal(0),
    )


def _v3_fill_status(fill: dict[str, Any]) -> str:
    status = str(fill.get("status") or "").upper()
    if status not in _V3_BET_STATES:
        raise RuntimeError(f"SX Bet V3 fill has unsupported status {status or 'MISSING'}")
    return status


def _ce_refund_fee(fill: dict[str, Any], asset_decimals: int) -> Decimal:
    raw = Decimal(str(fill.get("ceRefundFeeAmount") or "0"))
    if not raw.is_finite() or raw < 0:
        raise RuntimeError("SX Bet V3 fill has invalid ceRefundFeeAmount")
    return _from_base_units(raw, asset_decimals)


def _has_positive_ce_refund(fill: dict[str, Any], asset_decimals: int) -> bool:
    refund_raw = Decimal(str(fill.get("ceRefundAmount") or "0"))
    if not refund_raw.is_finite() or refund_raw < 0:
        raise RuntimeError("SX Bet V3 fill has invalid ceRefundAmount")
    return _from_base_units(refund_raw, asset_decimals) + _ce_refund_fee(fill, asset_decimals) > 0


def _sell_refund_accounting_is_indexed(fill: dict[str, Any]) -> bool:
    if "ceRefundAmount" not in fill or "ceRefundFeeAmount" not in fill:
        return False
    try:
        refund = Decimal(str(fill.get("ceRefundAmount") or "0"))
        fee = Decimal(str(fill.get("ceRefundFeeAmount") or "0"))
    except (ArithmeticError, TypeError, ValueError):
        return False
    return refund.is_finite() and fee.is_finite() and refund >= 0 and fee >= 0


def _sell_fill_accounting(
    fill: dict[str, Any],
    *,
    stake: Decimal,
    odds: Decimal,
    asset_decimals: int,
) -> tuple[Decimal, Decimal, Decimal, Decimal]:
    if not _sell_refund_accounting_is_indexed(fill):
        raise RuntimeError(f"SX Bet V3 sell fill {fill.get('id') or 'UNKNOWN'} is missing exact CE refund accounting")
    net_refund_raw = Decimal(str(fill.get("ceRefundAmount") or "0"))
    refund_fee = _ce_refund_fee(fill, asset_decimals)
    net_refund = _from_base_units(net_refund_raw, asset_decimals)
    closed_contracts = net_refund + refund_fee
    opposite_contracts = stake / odds
    if closed_contracts > opposite_contracts + _V3_STAKE_INDEX_TOLERANCE:
        raise RuntimeError("SX Bet V3 CE refund exceeds the opposite contracts created by the fill")
    residual_opposite = max(Decimal(0), opposite_contracts - closed_contracts)
    stake_used_to_close = closed_contracts * odds
    net_proceeds = net_refund - stake_used_to_close
    if closed_contracts > _V3_STAKE_INDEX_TOLERANCE and net_proceeds <= 0:
        raise RuntimeError("SX Bet V3 sell fill produced no positive net unwind proceeds")
    return closed_contracts, net_proceeds, refund_fee, residual_opposite


def _report_from_v3_fills(
    submitted: _V3SubmittedOrder,
    fills: list[dict[str, Any]],
    *,
    inactive_reason: str,
) -> ExecutionReport:
    if inactive_reason not in _V3_INACTIVE_REASONS:
        raise RuntimeError(
            f"SX Bet V3 order {submitted.order_id} has unsupported inactiveReason {inactive_reason or 'MISSING'}"
        )
    confirmed_contracts = Decimal(0)
    confirmed_order_stake = Decimal(0)
    confirmed_buy_stake = Decimal(0)
    confirmed_sell_proceeds = Decimal(0)
    confirmed_residual_opposite = Decimal(0)
    matched_contracts = Decimal(0)
    failed_contracts = Decimal(0)
    states: set[str] = set()
    for fill in fills:
        status = _v3_fill_status(fill)
        states.add(status)
        odds = _odds_units_to_probability(fill.get("fillOdds"))
        stake = _from_base_units(
            Decimal(str(fill.get("fillAmount") or "0")),
            submitted.asset_decimals,
        )
        if odds <= 0 or stake <= 0:
            continue
        contracts = stake / odds
        if status in _V3_IRREVERSIBLE_BET_STATES:
            confirmed_order_stake += stake
            if submitted.action == "SELL":
                closed_contracts, net_proceeds, _, residual_opposite = _sell_fill_accounting(
                    fill,
                    stake=stake,
                    odds=odds,
                    asset_decimals=submitted.asset_decimals,
                )
                confirmed_contracts += closed_contracts
                confirmed_sell_proceeds += net_proceeds
                confirmed_residual_opposite += residual_opposite
            else:
                if _has_positive_ce_refund(fill, submitted.asset_decimals):
                    raise RuntimeError("SX Bet V3 BUY fill unexpectedly created CE unwind accounting")
                confirmed_contracts += contracts
                confirmed_buy_stake += stake
        elif status == "MATCHED":
            matched_contracts += contracts
        else:
            failed_contracts += contracts
    avg_price = (
        confirmed_buy_stake / confirmed_contracts
        if submitted.action == "BUY" and confirmed_contracts > 0
        else confirmed_sell_proceeds / confirmed_contracts
        if confirmed_contracts > 0
        else Decimal(0)
    )
    if submitted.action == "SELL" and confirmed_residual_opposite > _V3_STAKE_INDEX_TOLERANCE:
        residual_report = ExecutionReport.from_amounts(
            submitted.order_id,
            max(submitted.requested_contracts, confirmed_contracts),
            confirmed_contracts,
            ExecutionStatus.PARTIAL,
            avg_price,
        )
        raise OrderResidualExposure(
            "SX Bet V3 sell fill created residual opposite exposure "
            f"({confirmed_residual_opposite} contracts)",
            report=residual_report,
            residual_contracts=confirmed_residual_opposite,
            residual_side=submitted.actual_side,
        )
    if "MATCHED" in states:
        requested = max(submitted.requested_contracts, confirmed_contracts + matched_contracts)
        return ExecutionReport(
            order_id=submitted.order_id,
            status=ExecutionStatus.OPEN,
            amount_requested=requested,
            amount_filled=confirmed_contracts,
            remaining_amount=max(Decimal(0), requested - confirmed_contracts),
            avg_price=avg_price,
            venue_order_id=submitted.order_id,
            cumulative_filled=confirmed_contracts,
        )
    if (
        inactive_reason == "FILLED"
        and confirmed_contracts <= 0
        and (not fills or bool(states & _V3_IRREVERSIBLE_BET_STATES))
    ):
        raise RuntimeError(f"SX Bet V3 order {submitted.order_id} is FILLED but has no valid locked fills")
    if (
        inactive_reason == "FILLED"
        and confirmed_contracts > 0
        and "FAILED" not in states
        and confirmed_order_stake + _V3_STAKE_INDEX_TOLERANCE < submitted.submitted_stake
    ):
        requested = max(submitted.requested_contracts, confirmed_contracts)
        return ExecutionReport(
            order_id=submitted.order_id,
            status=ExecutionStatus.OPEN,
            amount_requested=requested,
            amount_filled=confirmed_contracts,
            remaining_amount=max(_V3_STAKE_INDEX_TOLERANCE, requested - confirmed_contracts),
            avg_price=avg_price,
            venue_order_id=submitted.order_id,
            cumulative_filled=confirmed_contracts,
        )
    if inactive_reason == "FILLED" and confirmed_contracts > 0 and "FAILED" not in states:
        status = ExecutionStatus.FILLED
        requested_contracts = confirmed_contracts
    elif confirmed_contracts > 0:
        status = ExecutionStatus.PARTIAL
        requested_contracts = max(submitted.requested_contracts, confirmed_contracts + failed_contracts)
    elif inactive_reason == "EXPIRED":
        status = ExecutionStatus.EXPIRED
        requested_contracts = submitted.requested_contracts
    else:
        status = ExecutionStatus.CANCELLED
        requested_contracts = submitted.requested_contracts
    return ExecutionReport.from_amounts(
        submitted.order_id,
        requested_contracts,
        confirmed_contracts,
        status,
        avg_price,
    )


def _parse_balance_units(value: Any, field: str, *, allow_negative: bool) -> Decimal:
    try:
        parsed = Decimal("0") if value is None else Decimal(str(value))
    except (ArithmeticError, TypeError, ValueError) as exc:
        raise RuntimeError(f"SX Bet V3 balance has invalid {field}") from exc
    if not parsed.is_finite() or (not allow_negative and parsed < 0):
        raise RuntimeError(f"SX Bet V3 balance has invalid {field}")
    return parsed


def _to_base_units(value: Decimal, decimals: int) -> Decimal:
    return (value * (Decimal(10) ** decimals)).to_integral_value(rounding=ROUND_FLOOR)


def _from_base_units(value: Decimal, decimals: int) -> Decimal:
    return value / (Decimal(10) ** decimals)


def _probability_to_odds_units(value: Decimal) -> Decimal:
    return (value * ODDS_DECIMALS).to_integral_value(rounding=ROUND_FLOOR)


def _odds_units_to_probability(value: Any) -> Decimal:
    return Decimal(str(value or "0")) / ODDS_DECIMALS


def _uint256(value: Any) -> int:
    raw = str(value)
    return int(raw, 16) if raw.lower().startswith("0x") else int(raw)


def _remote_order_side(order: dict[str, Any]) -> BinarySide:
    if "isBettingOutcomeOne" not in order:
        raise RuntimeError("SX Bet V3 remote order is missing isBettingOutcomeOne")
    value = order["isBettingOutcomeOne"]
    if not isinstance(value, bool):
        raise RuntimeError("SX Bet V3 remote order has an invalid outcome side")
    return BinarySide.YES if value else BinarySide.NO


def _v3_order_expiry(metadata: dict[str, Any]) -> int:
    delay_values: list[int] = []

    def collect(value: Any) -> None:
        if isinstance(value, dict):
            for item in value.values():
                collect(item)
        elif isinstance(value, (int, float, str)):
            try:
                parsed = int(str(value))
            except ValueError:
                return
            if parsed >= 0:
                delay_values.append(parsed)

    collect(metadata.get("bettingDelay"))
    maximum_delay_seconds = ((max(delay_values, default=0) + 999) // 1000) + 2
    ttl_seconds = max(_V3_ORDER_TTL_SECONDS, maximum_delay_seconds + 5)
    return int(_utc_now().timestamp()) + ttl_seconds


def _version_as_sequence(value: Any) -> int | None:
    raw = str(value or "")
    return int(raw) if raw.isdigit() else None


def _parse_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    raw = str(value or "")
    if raw:
        try:
            parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
        except ValueError:
            pass
    return datetime.now(UTC)


def _nullable_fee_rate(value: Any, name: str) -> Decimal:
    rate = Decimal(0) if value is None else Decimal(str(value))
    if not rate.is_finite() or rate < 0 or rate >= 1:
        raise RuntimeError(f"SX Bet V3 {name} is invalid")
    return rate


def _utc_now() -> datetime:
    return datetime.now(UTC)
