from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import logging
import secrets
import time
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from decimal import ROUND_FLOOR, Decimal
from typing import Any

from arbitrage_engine.config import SxBetConfig
from arbitrage_engine.connectors.base import (
    BinaryMarketClient,
    ReconciliationUnsupported,
    WebSocketReconnectBackoff,
    event_timestamp,
)
from arbitrage_engine.connectors.web3_base import BaseWeb3Client
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
    RedemptionIntentStatus,
    RedemptionReport,
    SettlementRequest,
    SettlementStatus,
    VenueFeeQuote,
    VenueOrder,
    opposite_binary_side,
)

LOGGER = logging.getLogger(__name__)

USDC_DECIMALS = Decimal("1e6")
ODDS_DECIMALS = Decimal("1e20")
ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"
ZERO_HASH = "0x" + ("0" * 64)
_TRADE_LOOKBACK_BUFFER = timedelta(minutes=2)
_REST_RECOVERY_AFTER_SECONDS = 2.0
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
class _SubmittedFill:
    order_id: str
    fill_hash: str
    market_hash: str
    token_id: str
    action: str
    synthetic_side: BinarySide
    actual_side: BinarySide
    requested_contracts: Decimal
    requested_price: Decimal
    submitted_at: datetime

class SxBetApiClient(BinaryMarketClient):
    venue_name = "SX Bet"

    def __init__(self, config: SxBetConfig) -> None:
        self._config = config
        self._rest_session: Any | None = None
        self._http_semaphore = asyncio.Semaphore(20)
        self._metadata_cache: dict[str, Any] | None = None
        self._web3_client: BaseWeb3Client | None = None
        self._market_identifiers: dict[str, tuple[str, BinarySide]] = {}
        self._token_by_market_side: dict[tuple[str, BinarySide], str] = {}
        self._tracked_tokens: set[str] = set()
        self._book_timestamps: dict[str, float] = {}
        self._books: dict[str, OrderBook] = {}
        self._book_events: dict[str, asyncio.Event] = {}
        self._bootstrap_locks: dict[str, asyncio.Lock] = {}
        self._orders_by_market: dict[str, dict[str, dict[str, Any]]] = {}
        self._order_update_times: dict[str, int] = {}
        self._order_markets: dict[str, str] = {}
        self._ws_session: Any | None = None
        self._ws: Any | None = None
        self._ws_task: asyncio.Task[None] | None = None
        self._ws_connected = False
        self._subscription_queue: asyncio.Queue[tuple[str, str]] = asyncio.Queue()
        self._subscription_positions: dict[str, tuple[str, int]] = {}
        self._subscribed_markets: set[str] = set()
        self._bootstrap_tasks: dict[str, asyncio.Task[None]] = {}
        self._bootstrapping_markets: set[str] = set()
        self._buffered_publications: dict[str, list[tuple[str, dict[str, Any]]]] = {}
        self._reconnect_backoff = WebSocketReconnectBackoff()
        self._reconnect_count = 0
        self._sequence_gap_count = 0
        self._reports: dict[str, ExecutionReport] = {}
        self._submitted_fills: dict[str, _SubmittedFill] = {}

    def register_market(self, token_id: str, market_hash: str | None, side: BinarySide) -> None:
        if token_id and market_hash:
            self._market_identifiers[token_id] = (market_hash, side)
            self._token_by_market_side[(market_hash, side)] = token_id
            self._book_events.setdefault(token_id, asyncio.Event())
            if self._ws_connected and token_id in self._tracked_tokens:
                self._subscription_queue.put_nowait(("subscribe", market_hash))

    async def watch_order_book(self, token_id: str) -> OrderBook:
        market_identity = self._market_identifiers.get(token_id)
        if market_identity is None:
            raise RuntimeError(f"SX Bet market hash and side are not registered for token {token_id}")
        market_hash, side = market_identity
        self._tracked_tokens.add(token_id)
        self._ensure_ws_task()
        cached = self._books.get(token_id)
        if self._cached_book_is_fresh(token_id, cached):
            assert cached is not None
            return cached
        event = self._book_events.setdefault(token_id, asyncio.Event())
        event.clear()
        if cached is None and self._config.api_key:
            try:
                await asyncio.wait_for(event.wait(), timeout=1.5)
                cached = self._books.get(token_id)
                if self._cached_book_is_fresh(token_id, cached):
                    assert cached is not None
                    return cached
            except TimeoutError:
                pass
        book = await self._recover_market_book(token_id, market_hash, side)
        return book

    def _cached_book_is_fresh(self, token_id: str, book: OrderBook | None) -> bool:
        if book is None or book.status is not MarketDataStatus.VALID:
            return False
        market_identity = self._market_identifiers.get(token_id)
        if (
            market_identity is not None
            and self._ws_connected
            and market_identity[0] in self._subscribed_markets
        ):
            return True
        updated_at = self._book_timestamps.get(token_id)
        return updated_at is not None and time.monotonic() - updated_at <= _REST_RECOVERY_AFTER_SECONDS

    async def _recover_market_book(self, token_id: str, market_hash: str, side: BinarySide) -> OrderBook:
        lock = self._bootstrap_locks.setdefault(market_hash, asyncio.Lock())
        async with lock:
            cached = self._books.get(token_id)
            if self._cached_book_is_fresh(token_id, cached):
                assert cached is not None
                return cached
            return await self._bootstrap_market(market_hash, side)

    async def _bootstrap_market(self, market_hash: str, side: BinarySide | None = None) -> OrderBook:
        payload = await self._request_json("GET", "/orders", query_params={"marketHashes": market_hash})
        orders = _extract_records(payload, ("data", "orders"))
        market_orders = {
            str(order.get("orderHash")): order
            for order in orders
            if order.get("orderHash") and str(order.get("status") or "ACTIVE").upper() == "ACTIVE"
        }
        self._prune_market_order_versions(market_hash)
        self._orders_by_market[market_hash] = market_orders
        for order in orders:
            order_hash = str(order.get("orderHash") or "")
            if order_hash:
                self._order_update_times[order_hash] = _sx_update_time(order)
                self._order_markets[order_hash] = market_hash
        self._rebuild_market_books(market_hash)
        resolved_side = side or BinarySide.YES
        token_id = self._token_by_market_side.get((market_hash, resolved_side))
        if token_id and token_id in self._books:
            return self._books[token_id]
        return _order_book_from_orders(orders, resolved_side)

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
                token_payload = await self._request_json("GET", "/user/realtime-token/api-key")
                token = _extract_realtime_token(token_payload)
                if not token:
                    raise RuntimeError("SX Bet realtime token response is missing token")
                session = self._get_ws_session()
                async with session.ws_connect(self._config.ws_url) as ws:
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
                            if not raw_message:
                                continue
                            payload = json.loads(raw_message)
                            if isinstance(payload, dict):
                                await self._handle_centrifugo_message(payload, pending)
            except asyncio.CancelledError:
                raise
            except (aiohttp.ClientConnectionError, ConnectionResetError) as exc:
                LOGGER.info("sx_bet_ws_disconnected", extra={"reason": type(exc).__name__})
            except Exception:
                LOGGER.exception("sx_bet_ws_failed")
            finally:
                if sender is not None:
                    sender.cancel()
                    await asyncio.gather(sender, return_exceptions=True)
                await self._cancel_bootstrap_tasks()
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
        channel = f"order_book:market_{market_hash}"
        subscribe: dict[str, Any] = {"channel": channel, "positioned": True, "recoverable": True}
        position = self._subscription_positions.get(channel)
        if position is not None:
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
            if action == "subscribe" and market_hash not in subscribed:
                command_id = await self._send_market_subscription(ws, market_hash, command_id, pending)
                subscribed.add(market_hash)
            elif action == "unsubscribe" and market_hash in subscribed:
                await ws.send_json(
                    {
                        "unsubscribe": {"channel": f"order_book:market_{market_hash}"},
                        "id": command_id,
                    }
                )
                command_id += 1
                subscribed.remove(market_hash)
                self._subscribed_markets.discard(market_hash)

    async def _handle_centrifugo_message(
        self,
        payload: dict[str, Any],
        pending: dict[int, tuple[str, bool]],
    ) -> None:
        if payload == {}:
            ws = self._ws
            if ws is not None and not getattr(ws, "closed", False):
                await ws.send_json({})
            return
        if payload.get("error"):
            raise RuntimeError(f"SX Bet Centrifugo error: {payload['error']!r}")
        command_id = payload.get("id")
        subscribe_result = payload.get("subscribe")
        if subscribe_result is None and isinstance(payload.get("result"), dict):
            subscribe_result = payload["result"].get("subscribe", payload["result"])
        if isinstance(command_id, int) and command_id in pending and isinstance(subscribe_result, dict):
            market_hash, was_recovering = pending.pop(command_id)
            self._subscribed_markets.add(market_hash)
            channel = f"order_book:market_{market_hash}"
            epoch = str(subscribe_result.get("epoch") or "")
            offset = int(subscribe_result.get("offset") or 0)
            if epoch:
                previous_epoch, previous_offset = self._subscription_positions.get(channel, ("", 0))
                self._subscription_positions[channel] = (
                    epoch,
                    previous_offset if previous_epoch == epoch else 0,
                )
            recovered = was_recovering and bool(subscribe_result.get("recovered"))
            if recovered:
                publications = subscribe_result.get("publications") or []
                for publication in publications:
                    if isinstance(publication, dict):
                        self._apply_sx_publication(market_hash, channel, publication)
            else:
                current_epoch, current_offset = self._subscription_positions.get(channel, (epoch, 0))
                if offset > current_offset:
                    self._subscription_positions[channel] = (current_epoch or epoch, offset)
                if market_hash in self._active_market_hashes():
                    self._schedule_market_bootstrap(market_hash)
            return
        push = payload.get("push")
        if not isinstance(push, dict):
            return
        channel = str(push.get("channel") or "")
        publication = push.get("pub")
        if not channel.startswith("order_book:market_") or not isinstance(publication, dict):
            return
        market_hash = channel.removeprefix("order_book:market_")
        self._apply_sx_publication(market_hash, channel, publication)

    def _apply_sx_publication(self, market_hash: str, channel: str, publication: dict[str, Any]) -> None:
        if market_hash not in self._active_market_hashes():
            return
        if market_hash in self._bootstrapping_markets:
            self._buffered_publications.setdefault(market_hash, []).append((channel, publication))
            return
        self._apply_sx_publication_now(market_hash, channel, publication)

    def _apply_sx_publication_now(self, market_hash: str, channel: str, publication: dict[str, Any]) -> None:
        offset = int(publication.get("offset") or 0)
        epoch, previous_offset = self._subscription_positions.get(channel, ("", 0))
        if previous_offset and offset > previous_offset + 1:
            self._sequence_gap_count += 1
            self._mark_market_books_stale(market_hash)
            return
        if offset and offset <= previous_offset:
            return
        data = publication.get("data")
        updates = data if isinstance(data, list) else [data] if isinstance(data, dict) else []
        orders = self._orders_by_market.setdefault(market_hash, {})
        changed = False
        for order in updates:
            order_hash = str(order.get("orderHash") or "")
            if not order_hash:
                continue
            update_time = _sx_update_time(order)
            if update_time <= self._order_update_times.get(order_hash, -1):
                continue
            self._order_update_times[order_hash] = update_time
            self._order_markets[order_hash] = market_hash
            if str(order.get("status") or "ACTIVE").upper() == "ACTIVE":
                orders[order_hash] = order
            else:
                orders.pop(order_hash, None)
            changed = True
        if offset:
            self._subscription_positions[channel] = (epoch, offset)
        if changed:
            self._rebuild_market_books(market_hash)

    def _schedule_market_bootstrap(self, market_hash: str) -> None:
        existing = self._bootstrap_tasks.get(market_hash)
        if existing is not None and not existing.done():
            return
        self._bootstrapping_markets.add(market_hash)
        task = asyncio.create_task(self._bootstrap_subscribed_market(market_hash))
        self._bootstrap_tasks[market_hash] = task

        def _cleanup(completed: asyncio.Task[None]) -> None:
            if self._bootstrap_tasks.get(market_hash) is completed:
                self._bootstrap_tasks.pop(market_hash, None)
            try:
                completed.result()
            except asyncio.CancelledError:
                return
            except Exception:
                LOGGER.exception("sx_bet_subscription_bootstrap_task_failed", extra={"_market_hash": market_hash})

        task.add_done_callback(_cleanup)

    async def _bootstrap_subscribed_market(self, market_hash: str) -> None:
        try:
            await self._bootstrap_market(market_hash)
        except asyncio.CancelledError:
            self._buffered_publications.pop(market_hash, None)
            self._bootstrapping_markets.discard(market_hash)
            raise
        except Exception:
            self._buffered_publications.pop(market_hash, None)
            self._bootstrapping_markets.discard(market_hash)
            self._mark_market_books_stale(market_hash)
            LOGGER.exception("sx_bet_subscription_bootstrap_failed", extra={"_market_hash": market_hash})
            return
        buffered = self._buffered_publications.pop(market_hash, [])
        self._bootstrapping_markets.discard(market_hash)
        for channel, publication in buffered:
            self._apply_sx_publication_now(market_hash, channel, publication)

    def _cancel_market_bootstrap(self, market_hash: str) -> None:
        task = self._bootstrap_tasks.pop(market_hash, None)
        if task is not None:
            task.cancel()
        self._bootstrapping_markets.discard(market_hash)
        self._buffered_publications.pop(market_hash, None)

    async def _cancel_bootstrap_tasks(self) -> None:
        tasks = list(self._bootstrap_tasks.values())
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._bootstrap_tasks.clear()
        self._bootstrapping_markets.clear()
        self._buffered_publications.clear()

    def _rebuild_market_books(self, market_hash: str) -> None:
        orders = list(self._orders_by_market.get(market_hash, {}).values())
        for (registered_market, side), token_id in self._token_by_market_side.items():
            if registered_market != market_hash:
                continue
            book = _order_book_from_orders(orders, side)
            self._books[token_id] = book
            self._book_timestamps[token_id] = time.monotonic()
            self._book_events.setdefault(token_id, asyncio.Event()).set()

    def _mark_market_books_stale(self, market_hash: str) -> None:
        for (registered_market, _), token_id in self._token_by_market_side.items():
            if registered_market == market_hash and token_id in self._books:
                self._books[token_id] = replace(self._books[token_id], status=MarketDataStatus.STALE)

    def _mark_books_stale(self) -> None:
        for token_id in self._tracked_tokens & self._books.keys():
            self._books[token_id] = replace(self._books[token_id], status=MarketDataStatus.STALE)

    def _active_market_hashes(self) -> set[str]:
        return {
            self._market_identifiers[token_id][0]
            for token_id in self._tracked_tokens
            if token_id in self._market_identifiers
        }

    def _prune_market_order_versions(self, market_hash: str) -> None:
        stale_order_hashes = [
            order_hash
            for order_hash, registered_market in self._order_markets.items()
            if registered_market == market_hash
        ]
        for order_hash in stale_order_hashes:
            self._order_update_times.pop(order_hash, None)
            self._order_markets.pop(order_hash, None)

    def _prune_inactive_market(self, market_hash: str) -> None:
        self._cancel_market_bootstrap(market_hash)
        self._orders_by_market.pop(market_hash, None)
        self._prune_market_order_versions(market_hash)
        self._bootstrap_locks.pop(market_hash, None)
        self._subscription_positions.pop(f"order_book:market_{market_hash}", None)
        for (registered_market, _), token_id in self._token_by_market_side.items():
            if registered_market != market_hash:
                continue
            self._books.pop(token_id, None)
            self._book_timestamps.pop(token_id, None)
            self._book_events.pop(token_id, None)

    def _get_ws_session(self) -> Any:
        if self._ws_session is None or self._ws_session.closed:
            self._ws_session = client_session()
        return self._ws_session

    async def _close_ws_session(self) -> None:
        if self._ws_session is not None and not self._ws_session.closed:
            await self._ws_session.close()
        self._ws_session = None

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
        return await self._submit_fill(
            token_id=token_id,
            synthetic_side=side,
            actual_side=side,
            requested_contracts=_d(contracts),
            requested_price=_d(max_price),
            action="BUY",
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
        requested_price = _d(min_price)
        if requested_price <= 0 or requested_price >= 1:
            raise ValueError("SX Bet sell min_price must be between 0 and 1")
        return await self._submit_fill(
            token_id=token_id,
            synthetic_side=side,
            actual_side=opposite_binary_side(side),
            requested_contracts=_d(contracts),
            requested_price=requested_price,
            action="SELL",
        )

    async def _submit_fill(
        self,
        *,
        token_id: str,
        synthetic_side: BinarySide,
        actual_side: BinarySide,
        requested_contracts: Decimal,
        requested_price: Decimal,
        action: str,
    ) -> str:
        request_payload, market_hash, fill_salt = await self._build_fill_request(
            token_id=token_id,
            synthetic_side=synthetic_side,
            actual_side=actual_side,
            requested_contracts=requested_contracts,
            requested_price=requested_price,
            action=action,
        )
        response = await self._request_json("POST", "/orders/fill/v2", json_body=request_payload)
        data = response.get("data") if isinstance(response, dict) else None
        fill_hash = str((data or {}).get("fillHash") or "")
        order_id = _compose_order_id(
            fill_hash or f"sx-fill:{fill_salt}",
            action,
            synthetic_side,
            market_hash,
            requested_contracts,
            requested_price,
        )
        self._store_submitted_fill(
            _SubmittedFill(
            order_id=order_id,
            fill_hash=fill_hash or f"sx-fill:{fill_salt}",
            market_hash=market_hash,
            token_id=token_id,
            action=action,
            synthetic_side=synthetic_side,
            actual_side=actual_side,
            requested_contracts=requested_contracts,
            requested_price=requested_price,
            submitted_at=datetime.now(UTC),
            )
        )
        self._reports.pop(order_id, None)
        return order_id

    async def _build_fill_request(
        self,
        *,
        token_id: str,
        synthetic_side: BinarySide,
        actual_side: BinarySide,
        requested_contracts: Decimal,
        requested_price: Decimal,
        action: str,
    ) -> tuple[dict[str, Any], str, str]:
        if not self._config.private_key:
            raise RuntimeError("SX Bet private_key is required for taker fills")
        market_identity = self._market_identifiers.get(token_id)
        if market_identity is None:
            raise RuntimeError(f"SX Bet market hash and side are not registered for token {token_id}")
        market_hash, registered_side = market_identity
        if registered_side is not synthetic_side:
            raise RuntimeError(
                f"SX Bet token {token_id} is registered for {registered_side.value}, not {synthetic_side.value}"
            )
        metadata = await self._metadata()
        account = self._get_web3_client().account
        if account is None:
            raise RuntimeError("SX Bet private_key is required for taker fills")
        actual_price = requested_price if action == "BUY" else Decimal(1) - requested_price
        if requested_contracts <= 0:
            raise ValueError(f"SX Bet {action.lower()} contracts must be positive")
        if actual_price <= 0 or actual_price >= 1:
            raise ValueError(f"SX Bet {action.lower()} price must be between 0 and 1")
        stake_usd = requested_contracts * actual_price
        if stake_usd <= 0:
            raise ValueError(f"SX Bet {action.lower()} stake must be positive")
        stake_wei = _usd_to_usdc_units(stake_usd)
        desired_odds = _probability_to_odds_units(actual_price)
        fill_salt = str(int.from_bytes(secrets.token_bytes(32), "big"))
        request_payload = {
            "market": market_hash,
            "baseToken": await self._base_token_address(),
            "isTakerBettingOutcomeOne": actual_side is BinarySide.YES,
            "stakeWei": str(stake_wei),
            "desiredOdds": str(desired_odds),
            "oddsSlippage": self._config.odds_slippage,
            "fillSalt": fill_salt,
            "taker": account.address,
        }
        request_payload["takerSig"] = self._sign_fill_payload(
            request_payload,
            chain_id=self._config.chain_id,
            domain_version=str(self._config.domain_version or metadata.get("domainVersion") or "6.0"),
            eip712_fill_hasher=str(metadata.get("EIP712FillHasher") or ""),
        )
        return request_payload, market_hash, fill_salt

    async def wait_filled(self, order_id: str, timeout_ms: int) -> ExecutionReport:
        deadline = asyncio.get_running_loop().time() + timeout_ms / 1000
        while True:
            report = await self.get_order(order_id)
            if report.status is not ExecutionStatus.OPEN:
                return report
            if asyncio.get_running_loop().time() >= deadline:
                return report
            await asyncio.sleep(0.25)

    async def cancel_order(self, order_id: str) -> None:
        del order_id
        return None

    async def get_cash_balance(self) -> float:
        return float((await self.get_cash_balance_details())["balance"])

    async def get_cash_balance_details(self) -> dict[str, Any]:
        account = self._get_web3_client().account
        if account is None:
            raise RuntimeError("SX Bet private_key is required for balance checks")
        base_token_address = await self._base_token_address()
        token = self._get_web3_client().contract(base_token_address, ERC20_BALANCE_ABI)
        raw_balance, decimals = await asyncio.gather(
            _call_contract_method(token.functions.balanceOf(account.address).call),
            _call_contract_method(token.functions.decimals().call),
        )
        balance = float(Decimal(int(raw_balance)) / (Decimal(10) ** int(decimals)))
        return {
            "wallet_address": account.address,
            "base_token_address": base_token_address,
            "balance_raw": str(raw_balance),
            "decimals": int(decimals),
            "balance": balance,
        }

    async def build_order_preview(
        self,
        *,
        token_id: str,
        side: BinarySide,
        contracts: float,
        limit_price: float,
        action: str,
    ) -> dict[str, Any]:
        requested_contracts = _d(contracts)
        requested_price = _d(limit_price)
        if action not in {"BUY", "SELL"}:
            raise ValueError("SX Bet preview action must be BUY or SELL")
        actual_side = side if action == "BUY" else opposite_binary_side(side)
        request_payload, market_hash, fill_salt = await self._build_fill_request(
            token_id=token_id,
            synthetic_side=side,
            actual_side=actual_side,
            requested_contracts=requested_contracts,
            requested_price=requested_price,
            action=action,
        )
        preview_order_id = _compose_order_id(
            f"sx-preview:{fill_salt}",
            action,
            side,
            market_hash,
            requested_contracts,
            requested_price,
        )
        return {
            "order_id": preview_order_id,
            "market_hash": market_hash,
            "synthetic_side": side.value,
            "actual_fill_side": actual_side.value,
            "requested_contracts": float(requested_contracts),
            "requested_price": float(requested_price),
            "request_payload": request_payload,
            "signature_prefix": str(request_payload.get("takerSig") or "")[:18],
        }

    async def get_order(self, order_id: str) -> ExecutionReport:
        cached = self._reports.get(order_id)
        if cached is not None and cached.status is not cached.status.OPEN:
            return cached
        submitted = self._submitted_fills.get(order_id) or _submitted_from_order_id(order_id)
        if submitted is None:
            raise RuntimeError(f"SX Bet fill report is unavailable for {order_id}")
        trade = await self._find_submitted_trade(submitted)
        if trade is None:
            report = ExecutionReport.from_amounts(
                order_id,
                submitted.requested_contracts,
                Decimal(0),
                "open",
                Decimal(0),
            )
        else:
            report = _execution_report_from_trade(order_id, submitted, trade)
        self._reports[order_id] = report
        return report

    async def list_open_orders(self) -> list[VenueOrder]:
        return []

    async def list_fills(self, since: datetime | None = None) -> list[FillRecord]:
        account = self._get_web3_client().account
        if account is None:
            raise RuntimeError("SX Bet private_key is required for trade reconciliation")
        trades = await self._list_trades(
            bettor=account.address,
            start_date=since,
        )
        fills: list[FillRecord] = []
        for trade in trades:
            if not _is_successful_taker_trade(trade, bettor=account.address):
                continue
            fill_hash = str(trade.get("fillHash") or "")
            if not fill_hash:
                continue
            submitted = self._submitted_fill_for_hash(fill_hash) or _submitted_from_trade(trade)
            order_id = submitted.order_id if submitted is not None else fill_hash
            occurred_at = _trade_datetime(trade)
            report = _execution_report_from_trade(order_id, submitted, trade)
            fills.append(
                FillRecord(
                    fill_id=str(trade.get("id") or fill_hash),
                    client_order_id="",
                    venue_order_id=order_id,
                    venue="SX Bet",
                    quantity=report.amount_filled,
                    price=report.avg_price,
                    fee=Decimal(0),
                    occurred_at=occurred_at,
                )
            )
        return fills

    async def get_positions(self) -> dict[str, Decimal]:
        account = self._get_web3_client().account
        if account is None:
            raise RuntimeError("SX Bet private_key is required for trade reconciliation")
        trades = await self._list_trades(
            bettor=account.address,
            settled=False,
            trade_status="SUCCESS",
        )
        by_market: dict[str, dict[BinarySide, Decimal]] = {}
        for trade in trades:
            if not _is_successful_taker_trade(trade, bettor=account.address):
                continue
            market_hash = str(trade.get("marketHash") or "")
            side = BinarySide.YES if bool(trade.get("bettingOutcomeOne")) else BinarySide.NO
            contracts = _trade_contracts(trade)
            market_exposure = by_market.setdefault(market_hash, {BinarySide.YES: Decimal(0), BinarySide.NO: Decimal(0)})
            market_exposure[side] = market_exposure.get(side, Decimal(0)) + contracts
        positions: dict[str, Decimal] = {}
        for market_hash, exposure in by_market.items():
            yes = exposure.get(BinarySide.YES, Decimal(0))
            no = exposure.get(BinarySide.NO, Decimal(0))
            if yes > no:
                token_id = self._token_by_market_side.get((market_hash, BinarySide.YES)) or _fallback_token_id(
                    market_hash,
                    BinarySide.YES,
                )
                positions[token_id] = yes - no
            elif no > yes:
                token_id = self._token_by_market_side.get((market_hash, BinarySide.NO)) or _fallback_token_id(
                    market_hash,
                    BinarySide.NO,
                )
                positions[token_id] = no - yes
        return positions

    def supports_full_reconciliation(self) -> bool:
        return True

    async def get_market_constraints(self, token_id: str, condition_id: str | None = None) -> MarketConstraints | None:
        del condition_id
        if token_id not in self._market_identifiers:
            return None
        metadata = await self._metadata()
        taker_minimums = metadata.get("takerMinimums") if isinstance(metadata, dict) else None
        minimum_raw = "0"
        if isinstance(taker_minimums, dict):
            minimum_raw = taker_minimums.get(await self._base_token_address(), "0")
        tick_size = Decimal(str(metadata.get("oddsLadderStepSize", 125))) / Decimal("100000")
        return MarketConstraints(
            fee_rate_bps=self._config.taker_fee_bps,
            tick_size=tick_size,
            lot_size=Decimal("0.01"),
            minimum_notional=max(Decimal(str(self._config.minimum_notional_usd)), _usdc_units_to_decimal(minimum_raw)),
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
        model = "zero_fee" if resolved.fee_rate_bps == 0 else "notional_bps"
        return VenueFeeQuote(
            "SX Bet",
            resolved.fee_rate_bps,
            model,
            source="sx_single_bet_schedule",
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
        payload, _, _ = await self._build_fill_request(
            token_id=token_id,
            synthetic_side=side,
            actual_side=side,
            requested_contracts=contracts,
            requested_price=max_price,
            action="BUY",
        )
        return hashlib.sha256(repr(sorted(payload.items())).encode("utf-8")).hexdigest()

    def sync_market_data_targets(self, token_ids: set[str]) -> None:
        normalized = {token_id for token_id in token_ids if token_id}
        added_markets = {
            self._market_identifiers[token_id][0]
            for token_id in normalized - self._tracked_tokens
            if token_id in self._market_identifiers
        }
        removed_markets = {
            self._market_identifiers[token_id][0]
            for token_id in self._tracked_tokens - normalized
            if token_id in self._market_identifiers
        }
        self._tracked_tokens = normalized
        for market_hash in removed_markets - self._active_market_hashes():
            self._prune_inactive_market(market_hash)
            self._subscription_queue.put_nowait(("unsubscribe", market_hash))
        if self._ws_connected:
            for market_hash in sorted(added_markets & self._active_market_hashes()):
                self._subscription_queue.put_nowait(("subscribe", market_hash))
        if self._tracked_tokens:
            self._ensure_ws_task()

    def has_active_market_data_targets(self) -> bool:
        return bool(self._tracked_tokens)

    def active_market_data_target_count(self) -> int:
        return len(self._tracked_tokens)

    def market_data_ready(self) -> bool:
        if self._config.api_key and self._config.ws_url and not self._ws_connected:
            return False
        if self._config.api_key and self._config.ws_url:
            if not self._active_market_hashes().issubset(self._subscribed_markets):
                return False
        return bool(self._tracked_tokens) and all(
            token_id in self._books
            and self._books[token_id].status is MarketDataStatus.VALID
            and bool(self._books[token_id].asks)
            for token_id in self._tracked_tokens
        )

    def is_order_book_execution_fresh(
        self,
        token_id: str,
        book: OrderBook,
        max_age_seconds: float,
    ) -> bool:
        market_identity = self._market_identifiers.get(token_id)
        stream_confirms_book = (
            market_identity is not None
            and self._ws_connected
            and market_identity[0] in self._subscribed_markets
        )
        return book.status is MarketDataStatus.VALID and (
            stream_confirms_book
            or super().is_order_book_execution_fresh(token_id, book, max_age_seconds)
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

    def supports_automatic_redemption(self) -> bool:
        return True

    def market_data_age_seconds(self) -> float | None:
        if not self._book_timestamps:
            return None
        latest = max(self._book_timestamps.values())
        return max(0.0, time.monotonic() - latest)

    def prepare_settlement_request(self, request: SettlementRequest) -> SettlementRequest:
        return request

    async def get_settlement_status(self, request: SettlementRequest) -> SettlementStatus:
        account = self._get_web3_client().account
        if account is None:
            raise RuntimeError("SX Bet private_key is required for settlement checks")
        trades = await self._list_trades(
            bettor=account.address,
            market_hashes=[request.market_id],
        )
        relevant = [trade for trade in trades if str(trade.get("marketHash") or "") == request.market_id]
        if not relevant:
            return SettlementStatus.MANUAL_REVIEW
        if any(not bool(trade.get("settled")) and _is_trade_confirmed(trade) for trade in relevant):
            return SettlementStatus.OPEN
        return SettlementStatus.SETTLED

    async def redeem_position(self, request: SettlementRequest, redemption_id: str) -> RedemptionReport:
        del redemption_id
        status = await self.get_settlement_status(request)
        if status is SettlementStatus.SETTLED:
            return RedemptionReport(RedemptionIntentStatus.CONFIRMED)
        if status is SettlementStatus.OPEN:
            raise ReconciliationUnsupported("SX Bet positions settle on-venue and cannot be force-redeemed early")
        return RedemptionReport(
            RedemptionIntentStatus.MANUAL_REVIEW,
            error=f"SX Bet settlement status is {status.value}",
        )

    def forget_order(self, order_id: str) -> None:
        submitted = self._submitted_fills.get(order_id) or _submitted_from_order_id(order_id)
        fill_hash = submitted.fill_hash if submitted is not None else None
        self._submitted_fills.pop(order_id, None)
        if fill_hash:
            for key, value in list(self._submitted_fills.items()):
                if value.fill_hash == fill_hash:
                    self._submitted_fills.pop(key, None)
        self._reports.pop(order_id, None)

    async def close(self) -> None:
        if self._ws_task is not None:
            self._ws_task.cancel()
            await asyncio.gather(self._ws_task, return_exceptions=True)
            self._ws_task = None
        await self._cancel_bootstrap_tasks()
        await self._close_ws_session()
        if self._rest_session is not None:
            await self._rest_session.close()
            self._rest_session = None
        if self._web3_client is not None:
            await self._web3_client.close()
        self._web3_client = None

    async def _metadata(self) -> dict[str, Any]:
        if self._metadata_cache is None:
            payload = await self._request_json("GET", "/metadata")
            if isinstance(payload, dict) and isinstance(payload.get("data"), dict):
                self._metadata_cache = payload["data"]
            elif isinstance(payload, dict):
                self._metadata_cache = payload
            else:
                raise RuntimeError("SX Bet metadata payload is malformed")
        return self._metadata_cache

    async def _base_token_address(self) -> str:
        if self._config.base_token_address:
            return self._config.base_token_address
        metadata = await self._metadata()
        addresses = metadata.get("addresses")
        if not isinstance(addresses, dict):
            raise RuntimeError("SX Bet metadata is missing addresses")
        chain_addresses = addresses.get(str(self._config.chain_id))
        if not isinstance(chain_addresses, dict):
            raise RuntimeError(f"SX Bet metadata is missing addresses for chain {self._config.chain_id}")
        token_address = chain_addresses.get("USDC")
        if not token_address:
            raise RuntimeError(f"SX Bet metadata is missing USDC for chain {self._config.chain_id}")
        return str(token_address)

    async def _find_submitted_trade(self, submitted: _SubmittedFill) -> dict[str, Any] | None:
        account = self._get_web3_client().account
        if account is None:
            raise RuntimeError("SX Bet private_key is required for trade reconciliation")
        trades = await self._list_trades(
            bettor=account.address,
            market_hashes=[submitted.market_hash] if submitted.market_hash else None,
            start_date=_trade_query_start(submitted.submitted_at),
        )
        matches = [
            trade
            for trade in trades
            if str(trade.get("fillHash") or "") == submitted.fill_hash
            and str(trade.get("bettor") or "").lower() == account.address.lower()
        ]
        if not matches and submitted.fill_hash.startswith("sx-fill:"):
            matches = [
                trade
                for trade in trades
                if _trade_matches_submitted_fill(
                    trade,
                    submitted,
                    bettor=account.address,
                )
            ]
        if not matches:
            return None
        matches.sort(key=_trade_datetime, reverse=True)
        return matches[0]

    async def _list_trades(
        self,
        *,
        bettor: str,
        market_hashes: list[str] | None = None,
        start_date: datetime | None = None,
        settled: bool | None = None,
        trade_status: str | None = None,
    ) -> list[dict[str, Any]]:
        query_params: dict[str, Any] = {"bettor": bettor, "pageSize": 200}
        if market_hashes:
            query_params["marketHashes"] = ",".join(market_hashes)
        if start_date is not None:
            query_params["startDate"] = int(start_date.timestamp())
        if settled is not None:
            query_params["settled"] = str(settled).lower()
        if trade_status:
            query_params["tradeStatus"] = trade_status
        trades: list[dict[str, Any]] = []
        next_key: str | None = None
        while True:
            page_params = dict(query_params)
            if next_key:
                page_params["paginationKey"] = next_key
            payload = await self._request_json("GET", "/trades", query_params=page_params)
            data = payload.get("data") if isinstance(payload, dict) else None
            page_trades = []
            if isinstance(data, dict):
                page_trades = [item for item in data.get("trades", []) if isinstance(item, dict)]
                next_key = str(data.get("nextKey") or "") or None
            trades.extend(page_trades)
            if not next_key:
                break
        return trades

    def _store_submitted_fill(self, submitted: _SubmittedFill) -> None:
        self._submitted_fills[submitted.order_id] = submitted
        self._submitted_fills[submitted.fill_hash] = submitted

    def _submitted_fill_for_hash(self, fill_hash: str) -> _SubmittedFill | None:
        direct = self._submitted_fills.get(fill_hash)
        if direct is not None:
            return direct
        for submitted in self._submitted_fills.values():
            if submitted.fill_hash == fill_hash:
                return submitted
        return None

    def _get_web3_client(self) -> BaseWeb3Client:
        if self._web3_client is None:
            rpc_urls = self._config.rpc_urls or [self._config.rpc_url]
            self._web3_client = BaseWeb3Client(
                rpc_url=rpc_urls,
                chain_id=self._config.chain_id,
                private_key=self._config.private_key,
            )
        return self._web3_client

    def _sign_fill_payload(
        self,
        payload: dict[str, Any],
        *,
        chain_id: int,
        domain_version: str,
        eip712_fill_hasher: str,
    ) -> str:
        if not self._config.private_key:
            raise RuntimeError("SX Bet private_key is required for taker fills")
        if not eip712_fill_hasher:
            raise RuntimeError("SX Bet metadata is missing EIP712FillHasher")
        try:
            from eth_account import Account
            from eth_account.messages import encode_typed_data
        except ImportError as exc:
            raise RuntimeError("eth-account is required for SX Bet order signing") from exc

        typed_data = {
            "types": {
                "EIP712Domain": [
                    {"name": "name", "type": "string"},
                    {"name": "version", "type": "string"},
                    {"name": "chainId", "type": "uint256"},
                    {"name": "verifyingContract", "type": "address"},
                ],
                "Details": [
                    {"name": "action", "type": "string"},
                    {"name": "market", "type": "string"},
                    {"name": "betting", "type": "string"},
                    {"name": "stake", "type": "string"},
                    {"name": "worstOdds", "type": "string"},
                    {"name": "worstReturning", "type": "string"},
                    {"name": "fills", "type": "FillObject"},
                ],
                "FillObject": [
                    {"name": "stakeWei", "type": "string"},
                    {"name": "marketHash", "type": "string"},
                    {"name": "baseToken", "type": "string"},
                    {"name": "desiredOdds", "type": "string"},
                    {"name": "oddsSlippage", "type": "uint256"},
                    {"name": "isTakerBettingOutcomeOne", "type": "bool"},
                    {"name": "fillSalt", "type": "uint256"},
                    {"name": "beneficiary", "type": "address"},
                    {"name": "beneficiaryType", "type": "uint8"},
                    {"name": "cashOutTarget", "type": "bytes32"},
                ],
            },
            "primaryType": "Details",
            "domain": {
                "name": "SX Bet",
                "version": domain_version,
                "chainId": chain_id,
                "verifyingContract": eip712_fill_hasher,
            },
            "message": {
                "action": "N/A",
                "market": payload["market"],
                "betting": "N/A",
                "stake": "N/A",
                "worstOdds": "N/A",
                "worstReturning": "N/A",
                "fills": {
                    "stakeWei": payload["stakeWei"],
                    "marketHash": payload["market"],
                    "baseToken": payload["baseToken"],
                    "desiredOdds": payload["desiredOdds"],
                    "oddsSlippage": payload["oddsSlippage"],
                    "isTakerBettingOutcomeOne": payload["isTakerBettingOutcomeOne"],
                    "fillSalt": payload["fillSalt"],
                    "beneficiary": ZERO_ADDRESS,
                    "beneficiaryType": 0,
                    "cashOutTarget": ZERO_HASH,
                },
            },
        }
        signable = encode_typed_data(full_message=typed_data)
        signed = Account.from_key(self._config.private_key).sign_message(signable)
        signature = signed.signature.hex()
        return signature if signature.startswith("0x") else f"0x{signature}"

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
            raise RuntimeError("aiohttp is required for SX Bet REST connectivity") from exc
        if self._rest_session is None:
            headers = {"Accept": "application/json", "Content-Type": "application/json"}
            if self._config.api_key:
                headers["x-api-key"] = self._config.api_key
            self._rest_session = client_session(headers)
        url = f"{self._config.api_base_url.rstrip('/')}{path}"
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
                        timeout=timeout,
                    ) as response:
                        payload = await response.json(content_type=None)
                        if response.status >= 400:
                            raise RuntimeError(
                                f"SX Bet {normalized_method} {path} failed with {response.status}: {payload}"
                            )
                        return payload
            except (TimeoutError, aiohttp.ClientError) as exc:
                last_error = exc
                if attempt >= attempts:
                    raise
                await asyncio.sleep(0.2 * attempt)
        assert last_error is not None
        raise last_error


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
    if not isinstance(payload, dict):
        return None
    token = payload.get("token")
    if token:
        return str(token)
    data = payload.get("data")
    if isinstance(data, dict) and data.get("token"):
        return str(data["token"])
    return None


def _sx_update_time(order: dict[str, Any]) -> int:
    raw = order.get("updateTime") or order.get("updatedAt") or order.get("updated_at") or 0
    if isinstance(raw, (int, float, Decimal)):
        return int(raw)
    text = str(raw)
    if text.isdigit():
        return int(text)
    try:
        return int(datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp() * 1000)
    except ValueError:
        return 0


async def _call_contract_method(method: Any) -> Any:
    if inspect.iscoroutinefunction(method):
        return await method()
    result = await asyncio.to_thread(method)
    if inspect.isawaitable(result):
        return await result
    return result


def _order_book_from_orders(orders: list[dict[str, Any]], side: BinarySide) -> OrderBook:
    bids: list[OrderBookLevel] = []
    asks: list[OrderBookLevel] = []
    wants_outcome_one = side is BinarySide.YES
    for order in orders:
        maker_betting_outcome_one = bool(order.get("isMakerBettingOutcomeOne"))
        maker_probability = _maker_implied_probability(order)
        taker_probability = max(Decimal(0), Decimal(1) - maker_probability)
        stake_capacity = _remaining_taker_stake_usd(order)
        if taker_probability <= 0 or stake_capacity <= 0:
            continue
        payout_size = stake_capacity / taker_probability
        if maker_betting_outcome_one == wants_outcome_one:
            bids.append(OrderBookLevel(price=float(maker_probability), size=float(payout_size)))
        else:
            asks.append(OrderBookLevel(price=float(taker_probability), size=float(payout_size)))
    bids.sort(key=lambda level: level.price, reverse=True)
    asks.sort(key=lambda level: level.price)
    return OrderBook(
        bids=bids,
        asks=asks,
        raw_payload={
            "venue": "SX Bet",
            "synthetic_side": side.value,
            "orders": orders,
        },
        timestamp=max((event_timestamp(order) for order in orders), default=datetime.now(UTC).timestamp()),
    )


def _maker_implied_probability(order: dict[str, Any]) -> Decimal:
    return _d(order.get("percentageOdds", "0")) / ODDS_DECIMALS


def _remaining_taker_stake_usd(order: dict[str, Any]) -> Decimal:
    remaining_maker = _remaining_maker_stake_raw(order)
    maker_odds_raw = _d(order.get("percentageOdds", "0"))
    if remaining_maker <= 0 or maker_odds_raw <= 0:
        return Decimal(0)
    taker_raw = ((remaining_maker * ODDS_DECIMALS) / maker_odds_raw) - remaining_maker
    taker_raw = taker_raw.quantize(Decimal("1"), rounding=ROUND_FLOOR)
    return taker_raw / USDC_DECIMALS


def _remaining_maker_stake_raw(order: dict[str, Any]) -> Decimal:
    total_bet_size = _d(order.get("totalBetSize", "0"))
    fill_amount = _d(order.get("fillAmount", "0"))
    pending_fill_amount = _d(order.get("pendingFillAmount", "0"))
    return max(Decimal(0), total_bet_size - fill_amount - pending_fill_amount)


def _usd_to_usdc_units(value: Decimal) -> Decimal:
    return (value * USDC_DECIMALS).quantize(Decimal("1"), rounding=ROUND_FLOOR)


def _usdc_units_to_decimal(value: Any) -> Decimal:
    return _d(value) / USDC_DECIMALS


def _probability_to_odds_units(value: Decimal) -> Decimal:
    return (value * ODDS_DECIMALS).quantize(Decimal("1"), rounding=ROUND_FLOOR)


def _odds_units_to_probability(value: Any) -> Decimal:
    return _d(value) / ODDS_DECIMALS


def _d(value: Any) -> Decimal:
    return value if isinstance(value, Decimal) else Decimal(str(value))


def _decimal_id_component(value: Decimal) -> str:
    normalized = format(value.normalize(), "f")
    if "." in normalized:
        normalized = normalized.rstrip("0").rstrip(".")
    return normalized or "0"


def _fallback_token_id(market_hash: str, side: BinarySide) -> str:
    return f"{market_hash}:{side.value}"


def _compose_order_id(
    fill_hash: str,
    action: str,
    side: BinarySide,
    market_hash: str,
    requested_contracts: Decimal,
    requested_price: Decimal,
) -> str:
    return (
        f"sx:{action}:{side.value}:{market_hash}:"
        f"{_decimal_id_component(requested_contracts)}:{_decimal_id_component(requested_price)}:{fill_hash}"
    )


def _parse_order_id(order_id: str) -> tuple[str, BinarySide, str, Decimal, Decimal, str] | None:
    parts = order_id.split(":", 6)
    if len(parts) != 7:
        return None
    prefix, action, side, market_hash, requested_contracts, requested_price, fill_hash = parts
    if prefix != "sx" or action not in {"BUY", "SELL"}:
        return None
    try:
        return action, BinarySide(side), market_hash, _d(requested_contracts), _d(requested_price), fill_hash
    except (ValueError, ArithmeticError):
        return None


def _submitted_from_order_id(order_id: str) -> _SubmittedFill | None:
    parsed = _parse_order_id(order_id)
    if parsed is None:
        return None
    action, side, market_hash, requested_contracts, requested_price, fill_hash = parsed
    actual_side = side if action == "BUY" else opposite_binary_side(side)
    return _SubmittedFill(
        order_id=order_id,
        fill_hash=fill_hash,
        market_hash=market_hash,
        token_id="",
        action=action,
        synthetic_side=side,
        actual_side=actual_side,
        requested_contracts=requested_contracts,
        requested_price=requested_price,
        submitted_at=datetime.fromtimestamp(0, tz=UTC),
    )


def _submitted_from_trade(trade: dict[str, Any]) -> _SubmittedFill | None:
    fill_hash = str(trade.get("fillHash") or "")
    if not fill_hash:
        return None
    market_hash = str(trade.get("marketHash") or "")
    side = BinarySide.YES if bool(trade.get("bettingOutcomeOne")) else BinarySide.NO
    requested_contracts = _trade_contracts(trade)
    requested_price = _trade_probability(trade)
    return _SubmittedFill(
        order_id=fill_hash,
        fill_hash=fill_hash,
        market_hash=market_hash,
        token_id=_fallback_token_id(market_hash, side),
        action="BUY",
        synthetic_side=side,
        actual_side=side,
        requested_contracts=requested_contracts,
        requested_price=requested_price,
        submitted_at=_trade_datetime(trade),
    )


def _trade_datetime(trade: dict[str, Any]) -> datetime:
    raw = trade.get("updatedAt") or trade.get("createdAt") or trade.get("betTime")
    if isinstance(raw, str):
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).astimezone(UTC)
    if raw not in (None, ""):
        return datetime.fromtimestamp(_normalize_epoch_seconds(raw), tz=UTC)
    return datetime.now(UTC)


def _trade_query_start(submitted_at: datetime) -> datetime:
    if submitted_at <= datetime.fromtimestamp(0, tz=UTC):
        return submitted_at
    return submitted_at - _TRADE_LOOKBACK_BUFFER


def _normalize_epoch_seconds(raw: Any) -> float:
    value = float(str(raw))
    if value > 10_000_000_000:
        return value / 1000.0
    return value


def _trade_probability(trade: dict[str, Any]) -> Decimal:
    return _odds_units_to_probability(trade.get("odds") or "0")


def _trade_stake_usd(trade: dict[str, Any]) -> Decimal:
    normalized = trade.get("normalizedStake")
    if normalized not in (None, ""):
        return _d(normalized)
    return _usdc_units_to_decimal(trade.get("stake") or "0")


def _trade_contracts(trade: dict[str, Any]) -> Decimal:
    probability = _trade_probability(trade)
    if probability <= 0:
        return Decimal(0)
    return _trade_stake_usd(trade) / probability


def _trade_matches_submitted_fill(
    trade: dict[str, Any],
    submitted: _SubmittedFill,
    *,
    bettor: str,
) -> bool:
    if not _is_successful_taker_trade(trade, bettor=bettor):
        return False
    if str(trade.get("marketHash") or "") != submitted.market_hash:
        return False
    trade_side = BinarySide.YES if bool(trade.get("bettingOutcomeOne")) else BinarySide.NO
    if trade_side is not submitted.actual_side:
        return False
    trade_contracts = _trade_contracts(trade)
    trade_probability = _trade_probability(trade)
    if abs(trade_contracts - submitted.requested_contracts) > Decimal("0.000001"):
        return False
    if abs(trade_probability - _submitted_actual_probability(submitted)) > Decimal("0.000001"):
        return False
    return _trade_datetime(trade) >= _trade_query_start(submitted.submitted_at)


def _submitted_actual_probability(submitted: _SubmittedFill) -> Decimal:
    if submitted.action == "SELL":
        return max(Decimal(0), Decimal(1) - submitted.requested_price)
    return submitted.requested_price


def _is_trade_confirmed(trade: dict[str, Any]) -> bool:
    return str(trade.get("tradeStatus") or "").upper() == "SUCCESS" and bool(trade.get("valid", True))


def _is_successful_taker_trade(trade: dict[str, Any], *, bettor: str) -> bool:
    return (
        str(trade.get("bettor") or "").lower() == bettor.lower()
        and not bool(trade.get("maker"))
        and _is_trade_confirmed(trade)
    )


def _execution_report_from_trade(
    order_id: str,
    submitted: _SubmittedFill | None,
    trade: dict[str, Any],
) -> ExecutionReport:
    probability = _trade_probability(trade)
    trade_status = str(trade.get("tradeStatus") or "").upper()
    if submitted is not None and submitted.action == "SELL":
        avg_price = max(Decimal(0), Decimal(1) - probability)
        requested = submitted.requested_contracts
    else:
        avg_price = probability
        requested = (
            submitted.requested_contracts
            if submitted is not None and submitted.requested_contracts > 0
            else _trade_contracts(trade)
        )
    filled = _trade_contracts(trade) if trade_status == "SUCCESS" and bool(trade.get("valid", True)) else Decimal(0)
    if trade_status == "SUCCESS" and filled < requested:
        status = "partial"
    elif trade_status == "SUCCESS":
        status = "filled"
    elif trade_status == "FAILED":
        status = "failed"
    else:
        status = "open"
    return ExecutionReport.from_amounts(order_id, requested, filled, status, avg_price)
