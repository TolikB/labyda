from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, cast

from arbitrage_engine.conditional_tokens import ConditionalTokensRedemption
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
    RedemptionReport,
    SettlementRequest,
    SettlementStatus,
    VenueOrder,
)
from arbitrage_engine.utils.math import quantize_down, quantize_up

LOGGER = logging.getLogger(__name__)
SHARE_DECIMALS = 18
PRICE_DECIMALS = 18
PRICE_TICK_UNITS = 10**16
COLLATERAL_DECIMALS = 6
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
        self._bootstrap_semaphore = asyncio.Semaphore(5)
        self._rest_session: Any | None = None
        self._ws_session: Any | None = None
        self._desired_channels: set[str] = set()
        self._channel_tokens: dict[str, set[str]] = {}
        self._subscription_queue: asyncio.Queue[str] = asyncio.Queue()
        self._ws_task: asyncio.Task[None] | None = None
        self._ws: Any | None = None
        self._reconnect_lock = asyncio.Lock()
        self._reconnecting = False
        self._ws_connected = False
        self._reconnect_backoff = WebSocketReconnectBackoff()
        self._snapshot_interval_seconds = 30.0
        self._reconnect_count = 0
        self._sequence_gap_count = 0
        self._settlement: ConditionalTokensRedemption | None = None

    async def watch_order_book(self, token_id: str) -> OrderBook:
        market_id, side = _parse_token_id(token_id)
        channel = f"orderbook:{self._config.chain_id}:{market_id}"
        self._channel_tokens.setdefault(channel, set()).add(token_id)
        if channel not in self._desired_channels:
            self._desired_channels.add(channel)
            await self._subscription_queue.put(channel)
        self._book_events.setdefault(token_id, asyncio.Event())
        self._ensure_ws_task()
        cached = self._books.get(token_id)
        if cached is not None and cached.status in {MarketDataStatus.INVALID, MarketDataStatus.STALE}:
            return await self._bootstrap_order_book(token_id, market_id, side, force=True)
        ttl_seconds = self._config.order_book_ttl_ms / 1_000.0
        stale_after_seconds = self._config.websocket_stale_after_ms / 1_000.0
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
                    return await self._bootstrap_order_book(token_id, market_id, side, force=True)
                return self._books[token_id]
            event = self._book_events[token_id]
            event.clear()
            try:
                await asyncio.wait_for(event.wait(), timeout=min(ttl_seconds, stale_after_seconds))
                if time.monotonic() - self._book_timestamps.get(token_id, 0.0) <= ttl_seconds:
                    return self._books[token_id]
            except TimeoutError:
                pass
            age = time.monotonic() - self._book_timestamps.get(token_id, 0.0)
            reason = "websocket stalled" if age >= stale_after_seconds else "TTL exceeded"
            raise OrderBookStaleException(f"Myriad order book is stale for token {token_id}: {reason}, age={age:.3f}s")

        task = self._bootstrap_tasks.get(token_id)
        if task is None or task.done():
            task = asyncio.create_task(self._bootstrap_order_book(token_id, market_id, side))
            self._bootstrap_tasks[token_id] = task
        try:
            return await task
        finally:
            if self._bootstrap_tasks.get(token_id) is task and task.done():
                self._bootstrap_tasks.pop(token_id, None)

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
            resolved_side = side or BinarySide.YES
            raw = await self.get_orderbook(market_id, _outcome_id(resolved_side))
            book = _order_book_from_payload(raw, side)
            self._store_book(token_id, book)
            self._snapshot_timestamps[token_id] = time.monotonic()
            return book

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
            if connected_at is not None and time.monotonic() - connected_at >= 60.0:
                self._reconnect_backoff.reset()
            self._reconnect_count += 1
            await asyncio.sleep(self._reconnect_backoff.next_delay())

    async def _send_subscriptions(self, ws: Any, command_id: int, subscribed: set[str]) -> None:
        while True:
            channel = await self._subscription_queue.get()
            if channel in subscribed:
                continue
            await ws.send_json({"subscribe": {"channel": channel}, "id": command_id})
            command_id += 1
            subscribed.add(channel)

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
        active_tokens = {token for tokens in self._channel_tokens.values() for token in tokens}
        if not active_tokens:
            return None
        timestamps = [
            self._book_timestamps[token_id] for token_id in active_tokens if token_id in self._book_timestamps
        ]
        if not timestamps:
            return None
        now = time.monotonic()
        return now - max(timestamps)

    def set_market_data_snapshot_interval(self, seconds: float) -> None:
        self._snapshot_interval_seconds = seconds

    def market_data_ready(self) -> bool:
        active_tokens = {token for tokens in self._channel_tokens.values() for token in tokens}
        return self._ws_connected and bool(active_tokens) and all(
            token_id in self._books and self._books[token_id].status is MarketDataStatus.VALID
            for token_id in active_tokens
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
        active_tokens = {token for tokens in self._channel_tokens.values() for token in tokens}
        for token_id in active_tokens & self._books.keys():
            self._books[token_id] = replace(self._books[token_id], status=MarketDataStatus.STALE)

    def telemetry_snapshot(self) -> dict[str, float]:
        return {
            "reconnects": float(self._reconnect_count),
            "sequence_gaps": float(self._sequence_gap_count),
            "connected": float(self._ws_connected),
            "reconnecting": float(self._reconnecting),
            "reconnect_backoff_seconds": self._reconnect_backoff.current_delay_seconds,
        }

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
        market_id, _ = _parse_token_id(token_id)
        signed = await self.sign_order(market_id, _outcome_id(side), 0, contracts, max_price)
        order_id = await self.place_order(signed)
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
        payload = await self._request_json(
            "GET", "/orders", query_params={"network_id": str(self._config.chain_id), "status": "open"}
        )
        return [_venue_order_from_payload(item) for item in _extract_records(payload, ("orders", "items", "results"))]

    async def list_fills(self, since: datetime | None = None) -> list[FillRecord]:
        params = {"network_id": str(self._config.chain_id)}
        if since is not None:
            params["since"] = since.isoformat()
        payload = await self._request_json("GET", "/trades", query_params=params)
        return [_fill_from_trade(item) for item in _extract_records(payload, ("trades", "fills", "items", "results"))]

    async def get_positions(self) -> dict[str, Decimal]:
        payload = await self._request_json("GET", "/trades", query_params={"network_id": str(self._config.chain_id)})
        positions: dict[str, Decimal] = {}
        for item in _extract_records(payload, ("trades", "fills", "items", "results")):
            market_id = str(_extract_first_nested(item, ("marketId", "market_id")) or "")
            outcome = str(_extract_first_nested(item, ("outcomeId", "outcome_id", "outcome")) or "")
            if not market_id or outcome == "":
                continue
            normalized_outcome = "YES" if outcome in {"0", "YES", "yes"} else "NO"
            key = f"{market_id}:{normalized_outcome}"
            amount = Decimal(str(_normalize_share_amount(_extract_filled_amount(item) or 0.0)))
            side = str(_extract_first_nested(item, ("side", "action")) or "BUY").upper()
            positions[key] = positions.get(key, Decimal(0)) + (amount if side in {"BUY", "0"} else -amount)
        return positions

    def supports_full_reconciliation(self) -> bool:
        return True

    async def get_settlement_status(self, request: SettlementRequest) -> SettlementStatus:
        return await self._get_settlement_client().get_settlement_status(self._settlement_request(request))

    def prepare_settlement_request(self, request: SettlementRequest) -> SettlementRequest:
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
            self._settlement = ConditionalTokensRedemption(
                self._get_web3_client(),
                self._config.conditional_tokens_address,
                self._config.redemption_gas_limit,
            )
        return self._settlement

    def _settlement_request(self, request: SettlementRequest) -> SettlementRequest:
        collateral = request.collateral_token
        collateral = self._config.collateral_tokens.get(collateral, collateral)
        if not collateral:
            collateral = self._config.collateral_tokens[self._config.collateral_symbol]
        return replace(request, collateral_token=collateral)

    async def get_market_constraints(self, token_id: str, condition_id: str | None = None) -> MarketConstraints | None:
        del token_id, condition_id
        return MarketConstraints(
            fee_rate_bps=int(round(self._config.trading_fee_pct * 10_000)),
            tick_size=Decimal("0.01"),
            lot_size=Decimal(1) / (Decimal(10) ** SHARE_DECIMALS),
            minimum_notional=Decimal("1"),
        )

    def forget_order(self, order_id: str) -> None:
        self._order_amounts.pop(order_id, None)
        self._order_prices.pop(order_id, None)
        self._signed_orders.pop(order_id, None)

    async def place_order(self, signed_order: MyriadSignedOrder, *, time_in_force: str = "FAK") -> str:
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

    async def _request_json(
        self,
        method: str,
        path: str,
        *,
        query_params: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        session = self._get_rest_session()
        url = f"{self._config.api_url.rstrip('/')}/{path.lstrip('/')}"
        async with session.request(method, url, params=query_params, timeout=10) as response:
            response.raise_for_status()
            payload = await response.json()
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
    value = _extract_first_nested(payload, ("avgPrice", "averagePrice", "avg_price", "average_price"))
    if value in (None, ""):
        return None
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
    return FillRecord(
        fill_id=fill_id,
        client_order_id="",
        venue_order_id=order_id,
        venue="Myriad",
        quantity=Decimal(str(_normalize_share_amount(_extract_filled_amount(payload) or 0.0))),
        price=Decimal(str(_normalize_price(_extract_avg_price(payload) or 0.0))),
        fee=Decimal(str(_extract_first_nested(payload, ("fee", "feeAmount", "fee_amount")) or 0)),
        occurred_at=datetime.fromtimestamp(event_timestamp(payload), tz=UTC),
    )
