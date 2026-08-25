from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import secrets
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal
from typing import Any

from arbitrage_engine.config import SxBetConfig
from arbitrage_engine.connectors.base import BinaryMarketClient, WebSocketReconnectBackoff
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
_V3_ORDER_TTL_SECONDS = 60
_V3_FILL_INDEX_RETRIES = 3
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
    refund_fee_rate: Decimal = Decimal(0)


@dataclass(frozen=True)
class _V3FeeSchedule:
    taker_payout_fee: Decimal
    refund_fee: Decimal


class SxBetV3SubmissionUnknown(RuntimeError):
    """Carries the locally computed order id when POST acknowledgement is unknown."""

    def __init__(self, order_id: str, reason: BaseException | str) -> None:
        self.order_id = order_id
        super().__init__(f"SX Bet V3 submission outcome is unknown for {order_id}: {reason}")


class SxBetV3ApiClient(BinaryMarketClient):
    """SX Bet OBv3 taker connector.

    V3 uses proxy-held balances, aggregated versioned books, and one signed
    order endpoint for both makers and takers. This client intentionally only
    submits immediate IOC/FOK taker orders; it never leaves a GTC quote.
    """

    venue_name = "SX Bet"

    def __init__(self, config: SxBetConfig) -> None:
        if config.api_version != "v3":
            raise ValueError("SxBetV3ApiClient requires sx_bet.api_version=v3")
        expected_api_url = (
            "https://api.toronto.sx.bet" if config.environment == "toronto" else "https://api.sx.bet"
        )
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
            raise RuntimeError(
                f"SX Bet V3 mainnet is blocked before {SX_V3_MAINNET_CUTOVER_AT.isoformat()}"
            )
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
            self._books[token_id] = _order_book_from_v3_maker_snapshot(payload, side)
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
    ) -> str:
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
            refund_fee_rate=fee_schedule.refund_fee,
        )
        self._submitted_orders[order_id] = submitted
        if persist_order_id is not None:
            try:
                await persist_order_id(order_id)
            except BaseException:
                self._submitted_orders.pop(order_id, None)
                raise
        try:
            response = await self._request_json(
                "POST",
                "/orders-v3",
                json_body={
                    "orders": [payload],
                    "waitForOutcome": True,
                    "maxWaitTime": _V3_MAX_WAIT_TIME_MS,
                },
            )
        except Exception as exc:
            if await self._recover_unknown_submission(order_id):
                return order_id
            raise SxBetV3SubmissionUnknown(order_id, exc) from exc
        entries = _extract_records(response, ("orders",))
        if len(entries) != 1:
            raise SxBetV3SubmissionUnknown(order_id, "create-order response did not contain one order")
        entry = entries[0]
        if str(entry.get("status") or "").upper() != "SUBMITTED":
            self._submitted_orders.pop(order_id, None)
            raise RuntimeError(f"SX Bet V3 order rejected: {entry.get('error') or entry.get('reason') or 'unknown'}")
        returned_id = str(entry.get("orderId") or "").lower()
        if not returned_id or returned_id != order_id.lower():
            raise SxBetV3SubmissionUnknown(order_id, "returned orderId did not match signed digest")
        outcome = entry.get("outcome")
        if isinstance(outcome, dict):
            self._reports[order_id] = _report_from_v3_outcome(submitted, outcome)
        else:
            self._reports[order_id] = ExecutionReport.from_amounts(order_id, requested_contracts, Decimal(0), "open")
        return order_id

    async def _execution_book(self, token_id: str, actual_side: BinarySide) -> OrderBook:
        identity = self._market_identifiers.get(token_id)
        if identity is None:
            raise RuntimeError(f"SX Bet V3 market hash and side are not registered for token {token_id}")
        market_hash, _ = identity
        execution_token = self._token_by_market_side.get((market_hash, actual_side))
        if execution_token is None:
            raise RuntimeError(
                f"SX Bet V3 {actual_side.value} outcome is not registered for market {market_hash}"
            )
        return await self.watch_order_book(execution_token)

    async def _recover_unknown_submission(self, order_id: str) -> bool:
        for attempt in range(3):
            try:
                payload = await self._request_json("GET", f"/orders-v3/{order_id}")
                data = _response_data(payload)
                order = data.get("order", data) if isinstance(data, dict) else None
                if isinstance(order, dict) and str(order.get("id") or order.get("orderId") or "").lower() == order_id:
                    self._reports[order_id] = ExecutionReport.from_amounts(
                        order_id,
                        self._submitted_orders[order_id].requested_contracts,
                        Decimal(0),
                        "open",
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
        decimals = int(active_asset.get("decimals", 6))
        exact_odds = _round_v3_probability(actual_price_bound, metadata)
        stake = _stake_for_contracts(book, requested_contracts, actual_price_bound)
        stake_units = _to_base_units(stake, decimals)
        limits = metadata.get("limits")
        minimum_units = (
            Decimal(str(limits.get("orderSizeMinimumBaseUnits", "0")))
            if isinstance(limits, dict)
            else Decimal(0)
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
        return (
            {**order, "clientOrderId": order_id.removeprefix("0x"), "orderSignature": signature},
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
        safe_payload = {
            key: value
            for key, value in payload.items()
            if key not in {"salt", "orderSignature"}
        }
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
        payload, order_id, _, _ = await self._build_signed_order(
            token_id=token_id,
            synthetic_side=side,
            actual_side=side,
            requested_contracts=contracts,
            requested_price=max_price,
            action="BUY",
            book=book,
        )
        canonical = {key: value for key, value in payload.items() if key not in {"salt", "orderSignature"}}
        return hashlib.sha256((order_id + json.dumps(canonical, sort_keys=True)).encode()).hexdigest()

    async def wait_filled(self, order_id: str, timeout_ms: int) -> ExecutionReport:
        deadline = asyncio.get_running_loop().time() + timeout_ms / 1000
        while True:
            report = await self.get_order(order_id)
            if report.status is not ExecutionStatus.OPEN:
                return report
            if asyncio.get_running_loop().time() >= deadline:
                return report
            await asyncio.sleep(0.25)

    async def get_order(self, order_id: str) -> ExecutionReport:
        cached = self._reports.get(order_id)
        if cached is not None and cached.status is not ExecutionStatus.OPEN:
            return cached
        payload = await self._request_json("GET", f"/orders-v3/{order_id}")
        data = _response_data(payload)
        order = data.get("order", data) if isinstance(data, dict) else None
        if not isinstance(order, dict):
            raise RuntimeError("SX Bet V3 order response is malformed")
        remote_order_id = str(order.get("id") or order.get("orderId") or "").lower()
        if remote_order_id != order_id.lower():
            raise RuntimeError(f"SX Bet V3 order response id does not match {order_id}")
        submitted = self._submitted_orders.get(order_id.lower())
        if submitted is None:
            raise RuntimeError(
                f"SX Bet V3 durable order context must be restored before reconciling {order_id}"
            )
        remote_market_hash = str(order.get("marketHash") or "")
        remote_side = _remote_order_side(order)
        if remote_market_hash.lower() != submitted.market_hash.lower():
            raise RuntimeError("SX Bet V3 remote order market does not match durable intent")
        if remote_side is not submitted.actual_side:
            raise RuntimeError("SX Bet V3 remote order side does not match durable intent")
        status = str(order.get("status") or "").upper()
        if status in {"PENDING", "ACTIVE"}:
            return cached or ExecutionReport.from_amounts(order_id, submitted.requested_contracts, Decimal(0), "open")
        if status != "INACTIVE":
            raise RuntimeError(f"SX Bet V3 order {order_id} has unsupported status {status or 'MISSING'}")
        inactive_reason = str(order.get("inactiveReason") or "").upper()
        if inactive_reason not in _V3_INACTIVE_REASONS:
            raise RuntimeError(
                f"SX Bet V3 order {order_id} has unsupported inactiveReason "
                f"{inactive_reason or 'MISSING'}"
            )
        fills = await self._fills_for_terminal_order(submitted, inactive_reason)
        report = _report_from_v3_fills(submitted, fills, inactive_reason=inactive_reason)
        self._reports[order_id] = report
        return report

    async def restore_order_context(self, order_id: str, intent: OrderIntent) -> None:
        normalized_order_id = order_id.lower()
        if normalized_order_id in self._submitted_orders:
            return
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
        self._submitted_orders[normalized_order_id] = _V3SubmittedOrder(
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
            refund_fee_rate=refund_fee_rate,
        )

    async def _fills_for_terminal_order(
        self,
        submitted: _V3SubmittedOrder,
        inactive_reason: str,
    ) -> list[dict[str, Any]]:
        attempts = _V3_FILL_INDEX_RETRIES if inactive_reason == "FILLED" else 1
        for attempt in range(attempts):
            fills = await self._fills_for_order(submitted)
            if fills or inactive_reason != "FILLED":
                return fills
            if attempt + 1 < attempts:
                await asyncio.sleep(0.25 * (attempt + 1))
        raise RuntimeError(
            f"SX Bet V3 order {submitted.order_id} is FILLED but fills are not indexed yet"
        )

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
        return [
            row
            for row in rows
            if str(row.get("orderId") or "").lower() == submitted.order_id.lower()
        ]

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
                raise RuntimeError(
                    f"SX Bet V3 cancel confirmation has unsupported order status {status or 'MISSING'}"
                )
            inactive_reason = str(order.get("inactiveReason") or "").upper()
            if inactive_reason not in _V3_INACTIVE_REASONS:
                raise RuntimeError(
                    "SX Bet V3 cancel confirmation has unsupported inactiveReason "
                    f"{inactive_reason or 'MISSING'}"
                )
            fills = await self._list_v3_records("/fills-v3", "fills", {"orderId": order_id})
            non_failed_states = {
                state
                for state in (_v3_fill_status(fill) for fill in fills)
                if state != "FAILED"
            }
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
        decimals = int(active_asset.get("decimals", 6))
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
            available = Decimal(str(row.get("availableAmount") or "0"))
            pending_available = Decimal(str(row.get("pendingAvailableAmount") or "0"))
            escrowed = Decimal(str(row.get("escrowedAmount") or "0"))
            pending_escrow = Decimal(str(row.get("pendingEscrowAmount") or "0"))
        spendable_units = available + pending_available
        return {
            "wallet_address": proxy_address,
            "user_address": account_address,
            "escrow_address": escrow_address,
            "base_token_address": token_address,
            "balance_raw": str(spendable_units),
            "decimals": decimals,
            "balance": float(_from_base_units(spendable_units, decimals)),
            "escrowed": str(_from_base_units(escrowed + pending_escrow, decimals)),
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
        decimals = int(active_asset.get("decimals", 6))
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
        orders: list[VenueOrder] = []
        for row in rows:
            odds = _odds_units_to_probability(row.get("percentageOdds"))
            if odds <= 0:
                continue
            remaining_stake = _from_base_units(Decimal(str(row.get("remainingSize") or "0")), 6)
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
        fills: list[FillRecord] = []
        for row in rows:
            if _v3_fill_status(row) not in _V3_IRREVERSIBLE_BET_STATES:
                continue
            odds = _odds_units_to_probability(row.get("fillOdds"))
            if odds <= 0:
                continue
            stake = _from_base_units(Decimal(str(row.get("fillAmount") or "0")), 6)
            submitted = self._submitted_orders.get(str(row.get("orderId") or "").lower())
            reported_price = odds
            if submitted is not None and submitted.action == "SELL":
                reported_price = max(Decimal(0), Decimal(1) - odds - submitted.refund_fee_rate)
            fills.append(
                FillRecord(
                    fill_id=str(row.get("id") or row.get("matchId") or ""),
                    client_order_id="",
                    venue_order_id=str(row.get("orderId") or ""),
                    venue="SX Bet",
                    quantity=stake / odds,
                    price=reported_price,
                    fee=_from_base_units(Decimal(str(row.get("ceRefundFeeAmount") or "0")), 6),
                    occurred_at=_parse_datetime(row.get("createdAt")),
                )
            )
        return fills

    async def get_positions(self) -> dict[str, Decimal]:
        rows = await self._list_v3_records(
            "/positions-v3",
            "positions",
            {"status": "MATCHED,LOCKED"},
        )
        positions: dict[str, Decimal] = {}
        for row in rows:
            market_hash = str(row.get("marketHash") or "")
            max_win = _from_base_units(Decimal(str(row.get("maxWin") or "0")), 6)
            max_loss = _from_base_units(Decimal(str(row.get("maxLoss") or "0")), 6)
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
        while True:
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
        return self._ws_connected and bool(self._tracked_tokens) and any(
            token_id in self._book_timestamps for token_id in self._tracked_tokens
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
        }

    def market_data_age_seconds(self) -> float | None:
        timestamps = [self._book_timestamps[token] for token in self._tracked_tokens if token in self._book_timestamps]
        return None if not timestamps else max(0.0, time.monotonic() - max(timestamps))

    def forget_order(self, order_id: str) -> None:
        self._submitted_orders.pop(order_id, None)
        self._reports.pop(order_id, None)

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
        await self._close_ws_session()
        if self._rest_session is not None:
            await self._rest_session.close()
            self._rest_session = None

    async def _metadata(self) -> dict[str, Any]:
        if self._metadata_cache is None:
            payload = await self._request_json("GET", "/metadata/obv3")
            data = _response_data(payload)
            if not isinstance(data, dict):
                raise RuntimeError("SX Bet V3 metadata response is malformed")
            _validate_v3_metadata(data)
            self._metadata_cache = data
        return self._metadata_cache

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
        timeout = aiohttp.ClientTimeout(total=20, connect=10, sock_read=10)
        normalized_method = method.upper()
        attempts = 3 if normalized_method == "GET" else 1
        last_error: BaseException | None = None
        for attempt in range(1, attempts + 1):
            try:
                async with self._http_semaphore:
                    async with self._rest_session.request(
                        normalized_method,
                        url,
                        params=query_params,
                        json=json_body,
                        headers=request_headers or None,
                        timeout=timeout,
                    ) as response:
                        payload = await response.json(content_type=None)
                        if response.status >= 400:
                            raise RuntimeError(
                                f"SX Bet V3 {normalized_method} {path} failed with {response.status}: {payload}"
                            )
                        return payload
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


def _response_data(payload: Any) -> Any:
    return payload.get("data") if isinstance(payload, dict) and "data" in payload else payload


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


def _order_book_from_v3_maker_snapshot(payload: dict[str, Any], side: BinarySide) -> OrderBook:
    desired_key = "outcomeOne" if side is BinarySide.YES else "outcomeTwo"
    opposite_key = "outcomeTwo" if side is BinarySide.YES else "outcomeOne"
    bids = _v3_maker_levels(payload.get(desired_key), as_ask=False)
    asks = _v3_maker_levels(payload.get(opposite_key), as_ask=True)
    bids.sort(key=lambda level: level.price, reverse=True)
    asks.sort(key=lambda level: level.price)
    return OrderBook(
        bids=bids,
        asks=asks,
        raw_payload={"venue": "SX Bet", "api_version": "v3", "synthetic_side": side.value, "book": payload},
        sequence=_version_as_sequence(payload.get("version")),
        timestamp=time.time(),
    )


def _v3_maker_levels(raw_levels: Any, *, as_ask: bool) -> list[OrderBookLevel]:
    if not isinstance(raw_levels, list):
        return []
    levels: list[OrderBookLevel] = []
    for raw in raw_levels:
        if not isinstance(raw, dict):
            continue
        maker_probability = _odds_units_to_probability(raw.get("percentageOdds"))
        maker_stake = _from_base_units(Decimal(str(raw.get("size") or "0")), 6)
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
    fill_stake = _from_base_units(Decimal(str(outcome.get("fillAmount") or "0")), 6)
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


def _report_from_v3_fills(
    submitted: _V3SubmittedOrder,
    fills: list[dict[str, Any]],
    *,
    inactive_reason: str,
) -> ExecutionReport:
    if inactive_reason not in _V3_INACTIVE_REASONS:
        raise RuntimeError(
            f"SX Bet V3 order {submitted.order_id} has unsupported inactiveReason "
            f"{inactive_reason or 'MISSING'}"
        )
    confirmed_contracts = Decimal(0)
    confirmed_stake = Decimal(0)
    matched_contracts = Decimal(0)
    failed_contracts = Decimal(0)
    states: set[str] = set()
    for fill in fills:
        status = _v3_fill_status(fill)
        states.add(status)
        odds = _odds_units_to_probability(fill.get("fillOdds"))
        stake = _from_base_units(Decimal(str(fill.get("fillAmount") or "0")), 6)
        if odds <= 0 or stake <= 0:
            continue
        contracts = stake / odds
        if status in _V3_IRREVERSIBLE_BET_STATES:
            confirmed_contracts += contracts
            confirmed_stake += stake
        elif status == "MATCHED":
            matched_contracts += contracts
        else:
            failed_contracts += contracts
    actual_avg = confirmed_stake / confirmed_contracts if confirmed_contracts > 0 else Decimal(0)
    avg_price = (
        actual_avg
        if submitted.action == "BUY"
        else max(Decimal(0), Decimal(1) - actual_avg - submitted.refund_fee_rate)
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
    if inactive_reason == "FILLED" and confirmed_contracts <= 0 and (
        not fills or bool(states & _V3_IRREVERSIBLE_BET_STATES)
    ):
        raise RuntimeError(
            f"SX Bet V3 order {submitted.order_id} is FILLED but has no valid locked fills"
        )
    if (
        inactive_reason == "FILLED"
        and confirmed_contracts > 0
        and "FAILED" not in states
        and confirmed_stake + _V3_STAKE_INDEX_TOLERANCE < submitted.submitted_stake
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
