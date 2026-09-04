from __future__ import annotations

import asyncio
import logging
import math
import os
import time
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING, Any

from .chain_cost import LiveChainCostEstimator, LiveChainCostUnavailable
from .config import AppConfig, effective_funded_routes
from .connectors.base import BinaryMarketClient, OrderResidualExposure, OrderSubmissionRejected
from .connectors.web3_base import TransactionTimeoutException
from .market_mapping import is_live_mapping_eligible, route_key
from .models import (
    ArbitrageSignal,
    BinarySide,
    ExecutionMode,
    ExecutionReport,
    ExecutionStatus,
    ExitSignal,
    MarketDataStatus,
    MarketSpec,
    OpenPosition,
    OrderIntent,
    OrderIntentStatus,
    OrderPreview,
    PositionPlan,
    ResidualExitSnapshot,
    SpreadMetrics,
    VenueFeeQuote,
    apply_residual_exit_snapshot,
    first_leg_side_for_route,
    first_leg_token_for_route,
    position_key,
    second_leg_side_for_route,
    second_leg_token_for_route,
)
from .positions import PositionLedger
from .quant import (
    FillQuote,
    calculate_realized_position_profit_decimal,
    calculate_spread_metrics,
    executable_depth_usd,
    is_binary_signal_allowed,
    orderbook_buy_quote,
    top_of_book_ask_depth_usd,
)
from .risk import GlobalRiskController
from .telegram import TelegramNotifier, format_exit_message
from .utils.ids import uuid7

if TYPE_CHECKING:
    from .database import ProductionRepository

LOGGER = logging.getLogger(__name__)
ZERO = Decimal(0)
EPSILON = Decimal("1e-18")
_RISK_USD_EPSILON = Decimal("1e-9")
_SX_SUBMISSION_CUTOFF_BUFFER_SECONDS = 25.0

_KNOWN_PREVIEW_BLOCKERS = frozenset(
    {
        "asks_unavailable",
        "constraints_unavailable",
        "contracts_not_positive",
        "fee_metadata_unavailable",
        "fee_metadata_unverified",
        "insufficient_executable_depth",
        "limit_price_out_of_range",
        "minimum_notional_not_met",
        "signature_preview_unavailable",
    }
)
_KNOWN_ORDERBOOK_STATUSES = frozenset({"disconnected", "invalid", "stale", "unavailable", "valid"})


def _d(value: Decimal | float | int) -> Decimal:
    return value if isinstance(value, Decimal) else Decimal(str(value))


def _deployed_release_sha() -> str:
    configured = str(os.getenv("CI_VERIFIED_COMMIT_SHA") or "").strip()
    if configured:
        return configured
    for path in (Path("/run/release/release-sha"), Path(".runtime/release-sha")):
        try:
            value = path.read_text(encoding="utf-8").strip()
        except OSError:
            continue
        if value:
            return value
    return ""


def _safe_preview_blockers(preview: OrderPreview) -> tuple[str, ...]:
    """Return bounded blocker codes without exposing connector or signing payloads."""
    blockers: list[str] = []
    for raw_blocker in preview.blockers:
        blocker = str(raw_blocker).strip().lower()
        if blocker in _KNOWN_PREVIEW_BLOCKERS:
            blockers.append(blocker)
            continue
        if blocker.startswith("orderbook_status:"):
            status = blocker.partition(":")[2]
            if status in _KNOWN_ORDERBOOK_STATUSES:
                blockers.append(blocker)
                continue
        blockers.append("unknown_preview_blocker")
    if not preview.signing_validated and not preview.blockers:
        blockers.append("signature_preview_unavailable")
    fee_quote = preview.fee_quote
    if fee_quote is None:
        if "fee_metadata_unavailable" not in blockers:
            blockers.append("fee_metadata_unavailable")
    elif not fee_quote.verified and "fee_metadata_unverified" not in blockers:
        blockers.append("fee_metadata_unverified")
    if not preview.executable and not blockers:
        blockers.append("non_executable_without_blocker")
    return tuple(dict.fromkeys(blockers))


@dataclass(frozen=True)
class ExitLegResult:
    order_id: str
    report: ExecutionReport | None
    error: Exception | None = None


@dataclass(frozen=True)
class EntryLegResult:
    order_id: str
    report: ExecutionReport | None
    error: Exception | None = None
    submit_started_ns: int | None = None
    acknowledged_ns: int | None = None


@dataclass(frozen=True)
class PreparedEntry:
    """Exact signed-preview payload and economics authorized for submission."""

    signal: ArbitrageSignal
    all_in_cost_usd: Decimal
    evidence: dict[str, Any]
    first_order_fingerprint: str
    second_order_fingerprint: str


class EntrySubmissionCoordinator:
    """Serialize funded entries and enforce one runtime-wide entry-order budget."""

    def __init__(self, clock: Callable[[], float] = time.monotonic) -> None:
        self.entry_lock = asyncio.Lock()
        self._rate_lock = asyncio.Lock()
        self._order_timestamps: deque[float] = deque()
        self._clock = clock

    async def reserve_order_attempts(self, count: int, max_orders_per_minute: int) -> bool:
        if count <= 0:
            raise ValueError("count must be positive")
        if max_orders_per_minute <= 0:
            raise ValueError("max_orders_per_minute must be positive")
        now = self._clock()
        async with self._rate_lock:
            while self._order_timestamps and now - self._order_timestamps[0] >= 60.0:
                self._order_timestamps.popleft()
            if len(self._order_timestamps) + count > max_orders_per_minute:
                return False
            self._order_timestamps.extend(now for _ in range(count))
            return True


class ExecutionRouter:
    def __init__(
        self,
        config: AppConfig,
        polymarket: BinaryMarketClient,
        predict_fun: BinaryMarketClient,
        telegram: TelegramNotifier,
        ledger: PositionLedger | None = None,
        first_leg_label: str = "Polymarket",
        second_leg_label: str = "Predict.fun",
        first_leg_fill_timeout_ms: int | None = None,
        second_leg_fill_timeout_ms: int | None = None,
        market_locks: dict[str, asyncio.Lock] | None = None,
        capacity_lock: asyncio.Lock | None = None,
        pending_markets: set[str] | None = None,
        balance_cache: dict[str, Decimal | float] | None = None,
        capital_reservations: dict[str, Decimal | float] | None = None,
        optimistic_debits: dict[str, Decimal | float] | None = None,
        state_path: str | Path | None = None,
        risk_controller: GlobalRiskController | None = None,
        repository: ProductionRepository | None = None,
        preflight_observer: Callable[[str, dict[str, float]], None] | None = None,
        shadow_preflight_observer: Callable[[str, str], None] | None = None,
        accepted_preflight_observer: Callable[[str], None] | None = None,
        chain_cost_estimator: LiveChainCostEstimator | None = None,
        entry_submission_coordinator: EntrySubmissionCoordinator | None = None,
    ) -> None:
        self._config = config
        self._first_leg = polymarket
        self._second_leg = predict_fun
        self._telegram = telegram
        self._ledger = ledger or PositionLedger()
        self._last_signal_alert_at: dict[str, datetime] = {}
        self._last_exit_alert_at: dict[str, datetime] = {}
        self._first_leg_label = first_leg_label
        self._second_leg_label = second_leg_label
        self._first_leg_fill_timeout_ms = first_leg_fill_timeout_ms or config.polymarket_fill_timeout_ms
        self._second_leg_fill_timeout_ms = second_leg_fill_timeout_ms or config.predict_fun_fill_timeout_ms
        self._balance_cache = balance_cache if balance_cache is not None else {}
        self._capital_reservations = capital_reservations if capital_reservations is not None else {}
        self._optimistic_debits = optimistic_debits if optimistic_debits is not None else {}
        self._balance_updater_task: asyncio.Task[None] | None = None
        self._last_low_balance_alert_at = 0.0
        self._consecutive_api_errors = 0
        self._market_locks = market_locks if market_locks is not None else {}
        self._capacity_lock = capacity_lock or asyncio.Lock()
        self._pending_markets = pending_markets if pending_markets is not None else set()
        self._risk = risk_controller or GlobalRiskController(
            config.max_daily_loss_usd,
            config.max_consecutive_api_errors,
            state_path,
        )
        self._repository = repository
        self._preflight_observer = preflight_observer
        self._shadow_preflight_observer = shadow_preflight_observer
        self._accepted_preflight_observer = accepted_preflight_observer
        self._last_shadow_preflight_at: dict[str, float] = {}
        self._release_sha = _deployed_release_sha()
        self._funded_canary_deadline_unix: float | None = None
        self._chain_cost_estimator = chain_cost_estimator or LiveChainCostEstimator(config)
        self._active_orders: dict[tuple[int, str], BinaryMarketClient] = {}
        self._entry_submission_coordinator = entry_submission_coordinator or EntrySubmissionCoordinator()
        self._entry_readiness: Callable[[], bool] = lambda: True
        self._entry_snapshot_readiness: Callable[[int | None], bool] = lambda _generation: True
        self._risk.register_pause_callback(self._cancel_active_orders_and_clear_pending)

    @property
    def ledger(self) -> PositionLedger:
        return self._ledger

    @property
    def is_paused(self) -> bool:
        return self._risk.is_paused()

    def _route_name(self) -> str:
        return route_key(self._first_leg_label, self._second_leg_label)

    def set_preflight_observer(self, observer: Callable[[str, dict[str, float]], None] | None) -> None:
        self._preflight_observer = observer

    def set_shadow_preflight_observer(self, observer: Callable[[str, str], None] | None) -> None:
        self._shadow_preflight_observer = observer

    def set_accepted_preflight_observer(self, observer: Callable[[str], None] | None) -> None:
        self._accepted_preflight_observer = observer

    def set_entry_readiness(self, readiness: Callable[[], bool]) -> None:
        """Install the service-wide reconciliation gate for new entries."""
        self._entry_readiness = readiness

    def set_entry_snapshot_readiness(
        self,
        readiness: Callable[[int | None], bool],
    ) -> None:
        """Install the discovery-generation gate for in-flight entries."""
        self._entry_snapshot_readiness = readiness

    def _first_leg_token_id(self, market: MarketSpec) -> str:
        return first_leg_token_for_route(market, self._route_name()) or ""

    def _second_leg_token_id(self, market: MarketSpec) -> str:
        return second_leg_token_for_route(market, self._route_name()) or ""

    def _first_leg_side(self, market: MarketSpec) -> BinarySide:
        side = first_leg_side_for_route(market, self._route_name())
        if side is None:
            raise ValueError(f"First-leg side is unavailable for route {self._route_name()}")
        return side

    def _second_leg_side(self, market: MarketSpec) -> BinarySide:
        side = second_leg_side_for_route(market, self._route_name())
        if side is None:
            raise ValueError(f"Second-leg side is unavailable for route {self._route_name()}")
        return side

    def net_exit_values(
        self,
        market: MarketSpec,
        first_price: Decimal | float,
        second_price: Decimal | float,
    ) -> tuple[Decimal, Decimal]:
        return (
            _d(first_price) * (Decimal(1) - _d(self._venue_fee_pct(self._first_leg_label, market))),
            _d(second_price) * (Decimal(1) - _d(self._venue_fee_pct(self._second_leg_label, market))),
        )

    def gross_entry_values(
        self,
        market: MarketSpec,
        first_price: Decimal | float,
        second_price: Decimal | float,
    ) -> tuple[Decimal, Decimal]:
        return (
            _d(first_price) * (Decimal(1) + _d(self._venue_fee_pct(self._first_leg_label, market))),
            _d(second_price) * (Decimal(1) + _d(self._venue_fee_pct(self._second_leg_label, market))),
        )

    async def start(self) -> None:
        await self._refresh_balances()
        if not self._config.execution_mode.submits_orders:
            return
        if self._balance_updater_task is None or self._balance_updater_task.done():
            self._balance_updater_task = asyncio.create_task(self._run_balance_updater())

    async def close(self) -> None:
        await self._cancel_active_orders_and_clear_pending()
        if self._balance_updater_task is not None:
            self._balance_updater_task.cancel()
            await asyncio.gather(self._balance_updater_task, return_exceptions=True)
            self._balance_updater_task = None
        await self._chain_cost_estimator.close()
        await self._telegram.close()

    async def ensure_balances(self) -> bool:
        if not self._balance_cache:
            await self.start()
        first_balance = self._effective_balance(self._first_leg_label)
        second_balance = self._effective_balance(self._second_leg_label)
        required = _d(self._config.position_size_usd) / Decimal(2)
        ok = first_balance >= required and second_balance >= required
        now = time.monotonic()
        if not ok and now - self._last_low_balance_alert_at >= 600:
            self._last_low_balance_alert_at = now
            await self._telegram.send_html(
                "⚠️ <b>ARBITRAGE ENGINE STOPPED</b>\n"
                f"Недостатній баланс: {self._first_leg_label} ${first_balance:.2f}, "
                f"{self._second_leg_label} ${second_balance:.2f}. Required per leg: ${required:.2f}."
            )
        return ok

    async def handle_signal(self, signal: ArbitrageSignal) -> None:
        signal_received_ns = time.perf_counter_ns()
        if (
            self._config.execution_mode.submits_orders
            and self._route_name() not in effective_funded_routes(self._config)
        ):
            LOGGER.error(
                "entry_route_not_funded",
                extra={"_route": self._route_name(), "_symbol": signal.market.symbol},
            )
            return
        if self._risk.is_paused() and self._config.execution_mode.submits_orders:
            LOGGER.error(
                "execution_circuit_open",
                extra={"_symbol": signal.market.symbol, "_reason": self._risk.pause_reason},
            )
            return
        if self._config.execution_mode.submits_orders and not await self._funded_canary_window_guard(signal):
            return
        entry_threshold = max(
            self._config.min_net_spread,
            self._config.spread_policy.threshold_for(route_key(self._first_leg_label, self._second_leg_label)),
        )
        if not is_binary_signal_allowed(signal.metrics, entry_threshold):
            LOGGER.info(
                "binary_signal_rejected",
                extra={
                    "_combined_cost": signal.metrics.combined_cost_per_payout,
                    "_net_spread": signal.metrics.net_spread,
                    "_entry_threshold": entry_threshold,
                },
            )
            return
        market_key = signal.market.symbol
        market_lock = self._market_locks.setdefault(market_key, asyncio.Lock())
        async with market_lock:
            if self._config.execution_mode.submits_orders:
                async with self._entry_submission_coordinator.entry_lock:
                    await self._handle_signal_locked(signal, market_key, signal_received_ns)
            else:
                await self._handle_signal_locked(signal, market_key, signal_received_ns)

    async def _handle_signal_locked(self, signal: ArbitrageSignal, market_key: str, signal_received_ns: int) -> None:
        reserved = False
        capital_reserved = False
        execution_signal = signal
        if not self._entry_cutoff_guard(signal):
            return
        async with self._capacity_lock:
            if self._risk.is_paused() and self._config.execution_mode.submits_orders:
                return
            if self._ledger.has(position_key(signal.market)) or self._has_open_market(market_key):
                LOGGER.info("signal_skipped_existing_position", extra={"_symbol": signal.market.symbol})
                return
            active_markets = {position.market.symbol for position in self._ledger.all()} | self._pending_markets
            active_count = len(active_markets)
            if active_count >= self._config.max_open_positions:
                LOGGER.warning(
                    "signal_skipped_max_open_positions",
                    extra={"_symbol": signal.market.symbol, "_limit": self._config.max_open_positions},
                )
                return
            if market_key in self._pending_markets:
                LOGGER.info("signal_skipped_pending_market", extra={"_symbol": signal.market.symbol})
                return
            if not self._risk_limits_allow(signal):
                return
            self._pending_markets.add(market_key)
            reserved = True
        try:
            if not self._config.execution_mode.submits_orders:
                if not await self._shadow_preflight_guard(signal):
                    LOGGER.info(
                        "dry_run_signal_preflight_rejected",
                        extra={"_symbol": signal.market.symbol, "_route": self._route_name()},
                    )
                    return
                if self._should_send_signal_alert(signal):
                    await self._telegram.send_signal(
                        signal,
                        is_test=True,
                        min_net_spread=self._config.min_net_spread,
                    )
                LOGGER.info(
                    "dry_run_signal",
                    extra={
                        "_symbol": signal.market.symbol,
                        "_net_spread": signal.metrics.net_spread,
                        "_shadow_preflight_samples": self._config.shadow_preflight_samples,
                    },
                )
                return
            if not await self._market_constraints_guard(signal):
                return
            prepared_entry = await self._preflight_price_guard(signal)
            if prepared_entry is None:
                return
            execution_signal = prepared_entry.signal
            if not self._risk_limits_allow(
                execution_signal,
                all_in_cost_usd=prepared_entry.all_in_cost_usd,
            ):
                LOGGER.warning(
                    "pre_submit_risk_limits_rejected",
                    extra={
                        "_symbol": execution_signal.market.symbol,
                        "_route": self._route_name(),
                        "_all_in_cost_usd": str(prepared_entry.all_in_cost_usd),
                    },
                )
                return
            if not await self._reserve_signal_capital(execution_signal):
                return
            capital_reserved = True
            reserved_ns = time.perf_counter_ns()
            if self._risk.is_paused():
                LOGGER.warning(
                    "signal_aborted_global_risk_pause",
                    extra={"_symbol": execution_signal.market.symbol},
                )
                return
            if not self._entry_cutoff_guard(execution_signal):
                return
            if self._should_send_signal_alert(execution_signal):
                await self._telegram.send_signal(
                    execution_signal,
                    is_test=False,
                    min_net_spread=self._config.min_net_spread,
                )

            errors_before_execution = self._consecutive_api_errors
            try:
                if not await self._funded_canary_window_guard(execution_signal):
                    return
                if not self._entry_runtime_ready(execution_signal):
                    return
                if not await self._entry_submission_coordinator.reserve_order_attempts(
                    2,
                    self._config.max_orders_per_minute,
                ):
                    LOGGER.warning(
                        "risk_order_rate_rejected",
                        extra={"_symbol": execution_signal.market.symbol, "_route": self._route_name()},
                    )
                    return
                if not self._entry_runtime_ready(execution_signal):
                    return
                await self._execute_production(
                    execution_signal,
                    signal_received_ns,
                    reserved_ns,
                    first_order_fingerprint=prepared_entry.first_order_fingerprint,
                    second_order_fingerprint=prepared_entry.second_order_fingerprint,
                )
            except Exception:
                await self._record_api_error()
                raise
            else:
                if self._consecutive_api_errors == errors_before_execution:
                    self._consecutive_api_errors = 0
                    await self._risk.reset_api_errors()
        finally:
            if reserved or capital_reserved:
                async with self._capacity_lock:
                    if capital_reserved:
                        self._release_signal_capital(execution_signal)
                        await self._persist_runtime_balance_state()
                    self._pending_markets.discard(market_key)

    def _entry_cutoff_guard(self, signal: ArbitrageSignal) -> bool:
        allowed = _entry_submission_window_open(
            signal.market,
            includes_sx="SX Bet" in {self._first_leg_label, self._second_leg_label},
            now=datetime.now(UTC),
        )
        if not allowed:
            LOGGER.warning(
                "entry_cutoff_rejected",
                extra={
                    "_symbol": signal.market.symbol,
                    "_route": self._route_name(),
                    "_cutoff_at": (
                        signal.market.cutoff_at.isoformat()
                        if signal.market.cutoff_at is not None
                        else signal.market.expires_at.isoformat()
                        if signal.market.expires_at is not None
                        else None
                    ),
                },
            )
        return allowed

    async def _funded_canary_window_guard(self, signal: ArbitrageSignal) -> bool:
        return await self._funded_canary_window_guard_for_market(signal.market)

    def _funded_canary_window_open_for_market(self, market: MarketSpec) -> bool:
        deadline = self._funded_canary_deadline_unix
        if deadline is None:
            deadline = _funded_canary_deadline_unix(self._config)
            self._funded_canary_deadline_unix = deadline
        if deadline is None or time.time() < deadline - 1.0:
            return True
        LOGGER.warning(
            "funded_canary_entry_deadline_rejected",
            extra={"_symbol": market.symbol, "_route": self._route_name(), "_deadline_unix": deadline},
        )
        return False

    async def _funded_canary_window_guard_for_market(self, market: MarketSpec) -> bool:
        if self._funded_canary_window_open_for_market(market):
            return True
        await self._risk.pause("funded_canary_window_complete")
        return False

    def _has_open_market(self, market_key: str) -> bool:
        return any(position.market.symbol == market_key for position in self._ledger.all())

    def _risk_limits_allow(
        self,
        signal: ArbitrageSignal,
        *,
        all_in_cost_usd: Decimal | None = None,
    ) -> bool:
        positions = self._ledger.all()
        principal_cost = Decimal(str(signal.plan.polymarket_capital_usd)) + Decimal(
            str(signal.plan.predict_fun_capital_usd)
        )
        if all_in_cost_usd is None:
            chain_cost = Decimal(str(signal.metrics.fixed_chain_cost_usd))
            if not chain_cost.is_finite() or chain_cost < 0:
                LOGGER.error("risk_chain_cost_invalid", extra={"_symbol": signal.market.symbol})
                return False
            all_in_cost = Decimal(str(signal.plan.total_cost_usd)) + chain_cost
        else:
            all_in_cost = all_in_cost_usd
        if (
            not principal_cost.is_finite()
            or principal_cost < 0
            or not all_in_cost.is_finite()
            or all_in_cost < principal_cost
        ):
            LOGGER.error("risk_all_in_cost_invalid", extra={"_symbol": signal.market.symbol})
            return False
        total_notional = sum(
            (
                Decimal(str(position.polymarket_contracts)) * Decimal(str(position.polymarket_entry_price))
                + Decimal(str(position.predict_fun_contracts)) * Decimal(str(position.predict_fun_entry_price))
                for position in positions
            ),
            Decimal(0),
        )
        if (
            total_notional + all_in_cost
            > Decimal(str(self._config.max_total_notional_usd)) + _RISK_USD_EPSILON
        ):
            LOGGER.warning("risk_total_notional_rejected", extra={"_symbol": signal.market.symbol})
            return False
        if principal_cost > Decimal(str(self._config.max_market_exposure_usd)) + _RISK_USD_EPSILON:
            LOGGER.warning("risk_market_exposure_rejected", extra={"_symbol": signal.market.symbol})
            return False
        venue_exposure: dict[str, Decimal] = {}
        for position in positions:
            venue_exposure[position.market.venue_a_label] = venue_exposure.get(
                position.market.venue_a_label, Decimal(0)
            ) + Decimal(str(position.polymarket_contracts)) * Decimal(str(position.polymarket_entry_price))
            venue_exposure[position.market.venue_b_label] = venue_exposure.get(
                position.market.venue_b_label, Decimal(0)
            ) + Decimal(str(position.predict_fun_contracts)) * Decimal(str(position.predict_fun_entry_price))
        required = {
            self._first_leg_label: Decimal(str(signal.plan.polymarket_capital_usd)),
            self._second_leg_label: Decimal(str(signal.plan.predict_fun_capital_usd)),
        }
        if any(
            venue_exposure.get(venue, Decimal(0)) + amount
            > Decimal(str(self._config.max_venue_exposure_usd)) + _RISK_USD_EPSILON
            for venue, amount in required.items()
        ):
            LOGGER.warning("risk_venue_exposure_rejected", extra={"_symbol": signal.market.symbol})
            return False
        unresolved = sum(
            (
                Decimal(str(position.polymarket_contracts)) * Decimal(str(position.polymarket_entry_price))
                + Decimal(str(position.predict_fun_contracts)) * Decimal(str(position.predict_fun_entry_price))
                for position in positions
                if position.status in {"entry_pending", "unwind_pending", "partial_exit_pending", "manual_review"}
            ),
            Decimal(0),
        )
        if unresolved > Decimal(str(self._config.max_unresolved_exposure_usd)):
            LOGGER.error("risk_unresolved_exposure_rejected", extra={"_exposure_usd": str(unresolved)})
            return False
        return True

    async def _execute_production(
        self,
        signal: ArbitrageSignal,
        signal_received_ns: int | None = None,
        reserved_ns: int | None = None,
        *,
        first_order_fingerprint: str | None = None,
        second_order_fingerprint: str | None = None,
    ) -> None:
        if not await self._funded_canary_window_guard(signal):
            return
        if not self._entry_runtime_ready(signal):
            return
        claimed_first = self._first_leg.claim_prepared_order(
            first_order_fingerprint,
            token_id=self._first_leg_token_id(signal.market),
            side=self._first_leg_side(signal.market),
            contracts=signal.plan.polymarket_contracts,
            limit_price=Decimal(str(signal.polymarket_price)),
            action="BUY",
            submission_deadline_unix=(
                _entry_submission_deadline_unix(signal.market)
                if self._first_leg_label == "SX Bet"
                else None
            ),
        )
        claimed_second: str | None = None
        try:
            claimed_second = self._second_leg.claim_prepared_order(
                second_order_fingerprint,
                token_id=self._second_leg_token_id(signal.market),
                side=self._second_leg_side(signal.market),
                contracts=signal.plan.predict_fun_contracts,
                limit_price=Decimal(str(signal.predict_fun_price)),
                action="BUY",
                submission_deadline_unix=(
                    _entry_submission_deadline_unix(signal.market)
                    if self._second_leg_label == "SX Bet"
                    else None
                ),
            )
        except BaseException:
            self._first_leg.release_prepared_order(claimed_first)
            raise
        try:
            await self._save_entry_pending(signal)
            if not await self._funded_canary_window_guard(signal):
                await self._remove_position(position_key(signal.market))
                return
            if not self._entry_runtime_ready(signal):
                await self._remove_position(position_key(signal.market))
                return
            raw_first, raw_second = await asyncio.gather(
                self._submit_entry_leg(
                    client=self._first_leg,
                    market=signal.market,
                    venue_label=self._first_leg_label,
                    token_id=self._first_leg_token_id(signal.market),
                    side=self._first_leg_side(signal.market),
                    contracts=signal.plan.polymarket_contracts,
                    max_price=signal.polymarket_price,
                    capital_usd=signal.plan.polymarket_capital_usd + signal.plan.polymarket_fee_usd,
                    timeout_ms=self._first_leg_fill_timeout_ms,
                    prepared_order_fingerprint=claimed_first,
                    submission_deadline_unix=(
                        _entry_submission_deadline_unix(signal.market) if self._first_leg_label == "SX Bet" else None
                    ),
                    condition_id=signal.market.condition_id if self._first_leg_label == "Polymarket" else None,
                    tick_size=signal.market.tick_size if self._first_leg_label == "Polymarket" else None,
                    neg_risk=(
                        signal.market.neg_risk
                        if self._first_leg_label == "Polymarket"
                        else signal.market.predict_fun_neg_risk
                        if self._first_leg_label == "Predict.fun"
                        else None
                    ),
                    discovery_generation=signal.discovery_generation,
                ),
                self._submit_entry_leg(
                    client=self._second_leg,
                    market=signal.market,
                    venue_label=self._second_leg_label,
                    token_id=self._second_leg_token_id(signal.market),
                    side=self._second_leg_side(signal.market),
                    contracts=signal.plan.predict_fun_contracts,
                    max_price=signal.predict_fun_price,
                    capital_usd=signal.plan.predict_fun_capital_usd + signal.plan.predict_fun_fee_usd,
                    timeout_ms=self._second_leg_fill_timeout_ms,
                    prepared_order_fingerprint=claimed_second,
                    submission_deadline_unix=(
                        _entry_submission_deadline_unix(signal.market) if self._second_leg_label == "SX Bet" else None
                    ),
                    tick_size=signal.market.tick_size if self._second_leg_label == "Polymarket" else None,
                    neg_risk=(
                        signal.market.neg_risk
                        if self._second_leg_label == "Polymarket"
                        else signal.market.predict_fun_neg_risk
                        if self._second_leg_label == "Predict.fun"
                        else None
                    ),
                    discovery_generation=signal.discovery_generation,
                ),
                return_exceptions=True,
            )
        finally:
            self._first_leg.release_prepared_order(claimed_first)
            self._second_leg.release_prepared_order(claimed_second)
        first = self._normalize_entry_result(raw_first, self._first_leg_label)
        second = self._normalize_entry_result(raw_second, self._second_leg_label)
        self._log_pipeline_latency(signal, first, second, signal_received_ns, reserved_ns)
        if first.error is not None or second.error is not None:
            await self._record_api_error()

        first_filled = first.report.amount_filled if first.report is not None else ZERO
        second_filled = second.report.amount_filled if second.report is not None else ZERO
        missing_price_venues = [
            venue
            for venue, filled, result in (
                (self._first_leg_label, first_filled, first),
                (self._second_leg_label, second_filled, second),
            )
            if filled > EPSILON and (result.report is None or result.report.avg_price <= 0)
        ]
        if missing_price_venues:
            await self._save_unpriced_entry_pending(
                signal,
                first.order_id,
                second.order_id,
                first_filled,
                second_filled,
                first.report.avg_price if first.report is not None and first.report.avg_price > 0 else ZERO,
                second.report.avg_price if second.report is not None and second.report.avg_price > 0 else ZERO,
            )
            reason = f"filled execution report missing avg_price: {', '.join(missing_price_venues)}"
            LOGGER.critical(
                "filled_execution_price_missing_pausing",
                extra={"_symbol": signal.market.symbol, "_venues": missing_price_venues},
            )
            await self._risk.pause(reason)
            await self._telegram.send_html(
                "🚨 <b>EXECUTION PAUSED: MISSING FILL PRICE</b>\n"
                f"Market: {signal.market.symbol}; venues: {', '.join(missing_price_venues)}. "
                "The entry remains pending for reconciliation and manual review."
            )
            return
        first_entry_price = first.report.avg_price if first_filled > EPSILON and first.report is not None else ZERO
        second_entry_price = second.report.avg_price if second_filled > EPSILON and second.report is not None else ZERO
        matched = min(first_filled, second_filled)
        unmatched_first = max(ZERO, first_filled - matched)
        unmatched_second = max(ZERO, second_filled - matched)
        first_unwound, second_unwound = await asyncio.gather(
            self._try_unwind_first_leg(signal, unmatched_first) if unmatched_first > EPSILON else _zero_async(),
            self._try_unwind_second_leg(signal, unmatched_second) if unmatched_second > EPSILON else _zero_async(),
        )
        pending_first = max(ZERO, unmatched_first - first_unwound)
        pending_second = max(ZERO, unmatched_second - second_unwound)

        if unmatched_first > EPSILON or unmatched_second > EPSILON:
            await self._telegram.send_html(
                "⚠️ <b>PARALLEL ENTRY IMBALANCE</b>\n"
                f"{self._first_leg_label} unmatched: {unmatched_first:.6f}; unwound: {first_unwound:.6f}.\n"
                f"{self._second_leg_label} unmatched: {unmatched_second:.6f}; unwound: {second_unwound:.6f}."
            )

        if pending_first > EPSILON or pending_second > EPSILON:
            await self._save_unwind_pending(
                signal,
                first.order_id,
                second.order_id,
                matched,
                pending_first,
                pending_second,
                first_entry_price,
                second_entry_price,
            )
            return
        if matched <= EPSILON:
            await self._remove_position(position_key(signal.market))
            LOGGER.warning("parallel_entry_no_matched_fill", extra={"_symbol": signal.market.symbol})
            return

        position = self._open_position_from_amounts(
            signal,
            first.order_id,
            second.order_id,
            matched,
            first_entry_price,
            second_entry_price,
        )
        await self._add_position(position)
        LOGGER.info(
            "binary_signal_executed",
            extra={"_first_order_id": first.order_id, "_second_order_id": second.order_id},
        )
        await self._telegram.send_position_opened(signal, position)

    def _entry_runtime_ready(self, signal: ArbitrageSignal) -> bool:
        return self._entry_runtime_ready_for_market(
            signal.market,
            signal.discovery_generation,
        )

    def _entry_runtime_ready_for_market(
        self,
        market: MarketSpec,
        discovery_generation: int | None = None,
    ) -> bool:
        if self._risk.is_paused():
            LOGGER.warning(
                "entry_rejected_global_risk_pause",
                extra={"_symbol": market.symbol, "_route": self._route_name()},
            )
            return False
        try:
            ready = bool(self._entry_readiness())
        except Exception:
            LOGGER.exception(
                "entry_readiness_check_failed",
                extra={"_symbol": market.symbol, "_route": self._route_name()},
            )
            return False
        if not ready:
            LOGGER.warning(
                "entry_rejected_runtime_not_ready",
                extra={"_symbol": market.symbol, "_route": self._route_name()},
            )
            return False
        try:
            snapshot_ready = bool(self._entry_snapshot_readiness(discovery_generation))
        except Exception:
            LOGGER.exception(
                "entry_snapshot_readiness_check_failed",
                extra={"_symbol": market.symbol, "_route": self._route_name()},
            )
            return False
        if not snapshot_ready:
            LOGGER.warning(
                "entry_rejected_discovery_snapshot_replaced",
                extra={
                    "_symbol": market.symbol,
                    "_route": self._route_name(),
                    "_discovery_generation": discovery_generation,
                },
            )
        return snapshot_ready

    async def _submit_entry_leg(
        self,
        *,
        client: BinaryMarketClient,
        market: MarketSpec,
        venue_label: str,
        token_id: str,
        side: BinarySide,
        contracts: Decimal,
        max_price: float,
        capital_usd: Decimal,
        timeout_ms: int,
        prepared_order_fingerprint: str | None = None,
        submission_deadline_unix: float | None = None,
        condition_id: str | None = None,
        tick_size: str | None = None,
        neg_risk: bool | None = None,
        discovery_generation: int | None = None,
    ) -> EntryLegResult:
        order_id = "failed-before-order"
        client_order_id = str(uuid7())
        submit_started_ns = time.perf_counter_ns()
        acknowledged_ns: int | None = None
        final_intent_status: OrderIntentStatus | None = None
        transport_deadline_rejected = False

        if not await self._funded_canary_window_guard_for_market(market):
            return EntryLegResult(
                order_id,
                ExecutionReport.from_amounts(
                    order_id,
                    contracts,
                    Decimal(0),
                    ExecutionStatus.CANCELLED,
                ),
                OrderSubmissionRejected("funded canary entry deadline elapsed"),
                submit_started_ns,
                acknowledged_ns,
            )

        async def persist_order_id(prepared_order_id: str) -> None:
            nonlocal order_id
            if not prepared_order_id:
                raise RuntimeError(f"{venue_label} returned an empty venue order id")
            # Generic connectors learn the id only after submission. Keep it in
            # memory before the durable write so DB failure still permits cancel.
            order_id = prepared_order_id
            if self._repository is not None:
                await self._repository.update_order_intent(
                    client_order_id,
                    OrderIntentStatus.SUBMITTING,
                    venue_order_id=prepared_order_id,
                )
            if client.persists_order_id_before_submission():
                if not await self._funded_canary_window_guard_for_market(market):
                    raise OrderSubmissionRejected(
                        "funded canary entry deadline elapsed during order-id persistence",
                        order_id=prepared_order_id,
                    )
                if not self._entry_runtime_ready_for_market(market, discovery_generation):
                    raise OrderSubmissionRejected(
                        "entry readiness lost during pre-submit order-id persistence",
                        order_id=prepared_order_id,
                    )

        def assert_pre_transport_ready() -> None:
            nonlocal transport_deadline_rejected
            if not self._funded_canary_window_open_for_market(market):
                transport_deadline_rejected = True
                raise OrderSubmissionRejected(
                    "funded canary entry deadline elapsed immediately before transport",
                    order_id=None if order_id == "failed-before-order" else order_id,
                )
            if not self._entry_runtime_ready_for_market(market, discovery_generation):
                raise OrderSubmissionRejected(
                    "entry readiness lost immediately before transport",
                    order_id=None if order_id == "failed-before-order" else order_id,
                )

        async def reject_before_venue_submit(reason: str) -> EntryLegResult:
            error = OrderSubmissionRejected(reason)
            report = ExecutionReport.from_amounts(
                order_id,
                contracts,
                Decimal(0),
                ExecutionStatus.CANCELLED,
            )
            if self._repository is not None:
                await self._repository.update_order_intent(
                    client_order_id,
                    OrderIntentStatus.CANCELLED,
                    error=reason,
                )
            return EntryLegResult(
                order_id,
                report,
                error,
                submit_started_ns,
                acknowledged_ns,
            )

        if self._repository is not None:
            await self._repository.create_order_intent(
                OrderIntent(
                    client_order_id=client_order_id,
                    route=self._route_name(),
                    market_key=position_key(market),
                    venue=venue_label,
                    token_id=token_id,
                    binary_side=side,
                    action="BUY",
                    quantity=Decimal(str(contracts)),
                    limit_price=Decimal(str(max_price)),
                )
            )
            if not self._entry_runtime_ready_for_market(market, discovery_generation):
                return await reject_before_venue_submit(
                    "entry readiness lost after durable intent creation"
                )
            await self._repository.update_order_intent(client_order_id, OrderIntentStatus.SUBMITTING)
            if not self._entry_runtime_ready_for_market(market, discovery_generation):
                return await reject_before_venue_submit(
                    "entry readiness lost before venue submission"
                )
        try:
            if not await self._funded_canary_window_guard_for_market(market):
                raise OrderSubmissionRejected("funded canary entry deadline elapsed")
            if not self._entry_runtime_ready_for_market(market, discovery_generation):
                return await reject_before_venue_submit(
                    "entry readiness lost immediately before venue submission"
                )
            returned_order_id = await client.buy_with_order_id_persistence(
                token_id=token_id,
                side=side,
                contracts=float(contracts),
                max_price=max_price,
                persist_order_id=persist_order_id,
                pre_transport_guard=assert_pre_transport_ready,
                client_order_id=client_order_id,
                prepared_order_fingerprint=prepared_order_fingerprint,
                submission_deadline_unix=submission_deadline_unix,
                condition_id=condition_id,
                tick_size=tick_size,
                neg_risk=neg_risk,
            )
            if order_id == "failed-before-order" or returned_order_id.lower() != order_id.lower():
                raise RuntimeError(f"{venue_label} did not persist the submitted venue order id")
            order_id = returned_order_id
            acknowledged_ns = time.perf_counter_ns()
            if self._repository is not None:
                await self._repository.update_order_intent(
                    client_order_id,
                    OrderIntentStatus.ACKNOWLEDGED,
                    venue_order_id=order_id,
                )
            active_key = (id(client), order_id)
            self._active_orders[active_key] = client
            report = await client.wait_filled(order_id, timeout_ms)
            if not report.is_filled:
                report = await self._cancel_and_reconcile(client, order_id, report)
            report = replace(
                report,
                client_order_id=client_order_id,
                venue_order_id=order_id,
            )
            if self._repository is not None:
                final_intent_status = _intent_status_from_report(report)
                await self._repository.update_order_intent(
                    client_order_id,
                    final_intent_status,
                    venue_order_id=order_id,
                )
                if final_intent_status is OrderIntentStatus.UNKNOWN:
                    await self._risk.pause(f"unknown order outcome: {venue_label} client_order_id={client_order_id}")
            self._debit_reported_fill(venue_label, market, report, capital_usd)
            await self._persist_runtime_balance_state()
            return EntryLegResult(order_id, report, None, submit_started_ns, acknowledged_ns)
        except asyncio.CancelledError:
            if self._repository is not None:
                final_intent_status = OrderIntentStatus.UNKNOWN
                await self._repository.update_order_intent(
                    client_order_id,
                    OrderIntentStatus.UNKNOWN,
                    venue_order_id=None if order_id == "failed-before-order" else order_id,
                    error="entry submission cancelled during shutdown",
                )
            if order_id != "failed-before-order":
                try:
                    await client.cancel_order(order_id)
                except Exception:
                    LOGGER.exception("entry_cancel_during_shutdown_failed", extra={"_order_id": order_id})
            raise
        except Exception as exc:
            if transport_deadline_rejected:
                await self._risk.pause("funded_canary_window_complete")
            exception_order_id = getattr(exc, "order_id", None)
            if order_id == "failed-before-order" and isinstance(exception_order_id, str) and exception_order_id:
                order_id = exception_order_id
            if isinstance(exc, TransactionTimeoutException):
                await self._telegram.send_html(
                    "🚨 <b>NONCE/TRANSACTION TIMEOUT</b>\n"
                    f"Venue: {venue_label}; order: {order_id}; timeout: {timeout_ms}ms; reason: {exc}."
                )
            reconciled_report: ExecutionReport | None = None
            definitively_rejected = isinstance(exc, OrderSubmissionRejected)
            if definitively_rejected:
                reconciled_report = ExecutionReport.from_amounts(
                    order_id,
                    contracts,
                    Decimal(0),
                    ExecutionStatus.CANCELLED,
                )
            elif order_id != "failed-before-order":
                try:
                    await client.cancel_order(order_id)
                except Exception:
                    LOGGER.exception("entry_cancel_after_error_failed", extra={"_order_id": order_id})
                try:
                    reconciled_report = await client.wait_filled(order_id, self._config.cancel_reconcile_timeout_ms)
                except Exception:
                    LOGGER.exception("entry_reconcile_after_error_failed", extra={"_order_id": order_id})
            if reconciled_report is not None:
                reconciled_report = replace(
                    reconciled_report,
                    client_order_id=client_order_id,
                    venue_order_id=order_id,
                )
                self._debit_reported_fill(venue_label, market, reconciled_report, capital_usd)
                await self._persist_runtime_balance_state()
            if self._repository is not None:
                final_intent_status = (
                    OrderIntentStatus.CANCELLED
                    if definitively_rejected
                    else _intent_status_from_report(reconciled_report)
                    if reconciled_report is not None
                    else OrderIntentStatus.UNKNOWN
                )
                await self._repository.update_order_intent(
                    client_order_id,
                    final_intent_status,
                    venue_order_id=None if order_id == "failed-before-order" else order_id,
                    error=str(exc),
                )
                if final_intent_status is OrderIntentStatus.UNKNOWN:
                    await self._risk.pause(f"unknown order outcome: {venue_label} client_order_id={client_order_id}")
            return EntryLegResult(order_id, reconciled_report, exc, submit_started_ns, acknowledged_ns)
        finally:
            if order_id != "failed-before-order":
                self._active_orders.pop((id(client), order_id), None)
                forget_order = getattr(client, "forget_order", None)
                if callable(forget_order) and (
                    self._repository is None
                    or final_intent_status in {OrderIntentStatus.FILLED, OrderIntentStatus.CANCELLED}
                ):
                    forget_order(order_id)

    async def _cancel_and_reconcile(
        self,
        client: BinaryMarketClient,
        order_id: str,
        previous: ExecutionReport,
    ) -> ExecutionReport:
        try:
            await client.cancel_order(order_id)
        except Exception:
            LOGGER.exception("entry_cancel_failed_reconciling", extra={"_order_id": order_id})
        try:
            current = await client.wait_filled(order_id, self._config.cancel_reconcile_timeout_ms)
        except Exception:
            LOGGER.exception("entry_post_cancel_reconcile_failed", extra={"_order_id": order_id})
            return previous
        return current if current.amount_filled >= previous.amount_filled else previous

    def _debit_reported_fill(
        self,
        venue_label: str,
        market: MarketSpec,
        report: ExecutionReport,
        reserved_capital_usd: Decimal,
    ) -> None:
        if not report.has_fill:
            return
        actual = reserved_capital_usd
        if report.avg_price > 0:
            actual = min(
                reserved_capital_usd,
                report.amount_filled * report.avg_price * (Decimal(1) + _d(self._venue_fee_pct(venue_label, market))),
            )
        self._debit_cached_balance(venue_label, actual)

    @staticmethod
    def _normalize_entry_result(result: EntryLegResult | BaseException, venue_label: str) -> EntryLegResult:
        if isinstance(result, BaseException):
            if isinstance(result, asyncio.CancelledError):
                raise result
            LOGGER.error("entry_leg_failed", extra={"_venue": venue_label, "_error": str(result)})
            return EntryLegResult("failed-before-order", None, result if isinstance(result, Exception) else None)
        if result.error is not None:
            LOGGER.error("entry_leg_failed", extra={"_venue": venue_label, "_error": str(result.error)})
        return result

    async def handle_exit_signal(self, signal: ExitSignal) -> None:
        if not self._config.execution_mode.submits_orders:
            key = position_key(signal.position.market)
            now = datetime.now(UTC)
            last_sent = self._last_exit_alert_at.get(key)
            if last_sent is None or (now - last_sent).total_seconds() >= self._config.signal_alert_cooldown_seconds:
                self._last_exit_alert_at[key] = now
                await self._telegram.send_html(format_exit_message(signal, is_test=True))
            return

        await self._close_position_legs(
            signal.position,
            polymarket_exit_price=signal.polymarket_exit_price,
            predict_fun_exit_price=signal.predict_fun_exit_price,
        )

    async def retry_partial_exit(self, position: OpenPosition) -> None:
        if position.status != "partial_exit_pending":
            return
        poly_price = position.polymarket_exit_price
        predict_price = position.predict_fun_exit_price
        if not position.polymarket_closed:
            poly_price = _d(
                (await self._first_leg.watch_order_book(self._first_leg_token_id(position.market))).best_bid.price
            )
        if not position.predict_fun_closed:
            predict_price = _d(
                (await self._second_leg.watch_order_book(self._second_leg_token_id(position.market))).best_bid.price
            )
        await self._close_position_legs(
            position,
            polymarket_exit_price=poly_price or Decimal("0.01"),
            predict_fun_exit_price=predict_price or Decimal("0.01"),
        )

    async def retry_pending_unwind(self, position: OpenPosition) -> None:
        if position.status != "unwind_pending":
            return
        signal = _signal_from_unwind_position(position)
        first_pending = position.unmatched_first_contracts
        second_pending = position.unmatched_second_contracts
        first_filled, second_filled = await asyncio.gather(
            self._try_unwind_first_leg(signal, first_pending) if first_pending > EPSILON else _zero_async(),
            self._try_unwind_second_leg(signal, second_pending) if second_pending > EPSILON else _zero_async(),
        )
        attempts = position.polymarket_unwind_attempts + 1
        remaining_first = max(ZERO, first_pending - first_filled)
        remaining_second = max(ZERO, second_pending - second_filled)
        polymarket_contracts = max(ZERO, position.polymarket_contracts - first_filled)
        predict_fun_contracts = max(ZERO, position.predict_fun_contracts - second_filled)
        if remaining_first <= EPSILON and remaining_second <= EPSILON:
            matched = min(polymarket_contracts, predict_fun_contracts)
            if matched > EPSILON:
                await self._add_position(
                    replace(
                        position,
                        polymarket_contracts=matched,
                        predict_fun_contracts=matched,
                        status="open",
                        polymarket_unwind_attempts=attempts,
                        unmatched_first_contracts=ZERO,
                        unmatched_second_contracts=ZERO,
                    )
                )
            else:
                await self._remove_position(position_key(position.market))
            await self._telegram.send_html(
                "✅ <b>[AUTO-UNWIND COMPLETED]</b>\n"
                f"Пара: {position.market.symbol}\n"
                f"Attempts: {attempts}\n"
                "Unhedged exposure was closed automatically."
            )
            return
        await self._add_position(
            replace(
                position,
                polymarket_contracts=polymarket_contracts,
                predict_fun_contracts=predict_fun_contracts,
                polymarket_unwind_attempts=attempts,
                unmatched_first_contracts=remaining_first,
                unmatched_second_contracts=remaining_second,
            )
        )

    async def _close_position_legs(
        self,
        position: OpenPosition,
        *,
        polymarket_exit_price: Decimal | float,
        predict_fun_exit_price: Decimal | float,
    ) -> None:
        polymarket_exit_price = _d(polymarket_exit_price)
        predict_fun_exit_price = _d(predict_fun_exit_price)
        poly_task = self._submit_exit_leg(
            client=self._first_leg,
            market=position.market,
            venue_label=self._first_leg_label,
            already_closed=position.polymarket_closed,
            token_id=self._first_leg_token_id(position.market),
            side=self._first_leg_side(position.market),
            contracts=max(ZERO, position.polymarket_contracts - position.polymarket_closed_contracts),
            min_price=polymarket_exit_price,
            timeout_ms=self._first_leg_fill_timeout_ms,
            condition_id=position.market.condition_id if self._first_leg_label == "Polymarket" else None,
            tick_size=position.market.tick_size if self._first_leg_label == "Polymarket" else None,
            neg_risk=position.market.neg_risk if self._first_leg_label == "Polymarket" else None,
        )
        predict_task = self._submit_exit_leg(
            client=self._second_leg,
            market=position.market,
            venue_label=self._second_leg_label,
            already_closed=position.predict_fun_closed,
            token_id=self._second_leg_token_id(position.market),
            side=self._second_leg_side(position.market),
            contracts=max(ZERO, position.predict_fun_contracts - position.predict_fun_closed_contracts),
            min_price=predict_fun_exit_price,
            timeout_ms=self._second_leg_fill_timeout_ms,
            neg_risk=position.market.predict_fun_neg_risk if self._second_leg_label == "Predict.fun" else None,
        )
        raw_poly_result, raw_predict_result = await asyncio.gather(
            poly_task,
            predict_task,
            return_exceptions=True,
        )
        poly_result = self._normalize_exit_result(
            raw_poly_result,
            self._first_leg_label,
        )
        predict_result = self._normalize_exit_result(
            raw_predict_result,
            self._second_leg_label,
        )
        poly_exit_order_id, poly_report = poly_result.order_id, poly_result.report
        predict_exit_order_id, predict_report = predict_result.order_id, predict_result.report
        poly_residual_error = (
            poly_result.error if isinstance(poly_result.error, OrderResidualExposure) else None
        )
        predict_residual_error = (
            predict_result.error if isinstance(predict_result.error, OrderResidualExposure) else None
        )
        poly_new_fill = (
            poly_report.amount_filled if poly_report is not None and poly_residual_error is None else ZERO
        )
        predict_new_fill = (
            predict_report.amount_filled
            if predict_report is not None and predict_residual_error is None
            else ZERO
        )
        poly_closed_contracts = min(position.polymarket_contracts, position.polymarket_closed_contracts + poly_new_fill)
        predict_closed_contracts = min(
            position.predict_fun_contracts, position.predict_fun_closed_contracts + predict_new_fill
        )
        poly_fill_price = poly_report.avg_price if poly_report and poly_report.avg_price > 0 else polymarket_exit_price
        predict_fill_price = (
            predict_report.avg_price if predict_report and predict_report.avg_price > 0 else predict_fun_exit_price
        )
        poly_proceeds = position.polymarket_exit_proceeds_usd + poly_new_fill * poly_fill_price * (
            Decimal(1) - _d(self._venue_fee_pct(self._first_leg_label, position.market))
        )
        predict_proceeds = position.predict_fun_exit_proceeds_usd + predict_new_fill * predict_fill_price * (
            Decimal(1) - _d(self._venue_fee_pct(self._second_leg_label, position.market))
        )
        poly_filled = position.polymarket_closed or poly_closed_contracts >= position.polymarket_contracts - EPSILON
        predict_filled = (
            position.predict_fun_closed or predict_closed_contracts >= position.predict_fun_contracts - EPSILON
        )
        manual_review_required = any(
            isinstance(result.error, OrderResidualExposure)
            or (
                result.error is not None
                and result.order_id != "failed-before-order"
                and (
                    result.report is None
                    or result.report.status in {ExecutionStatus.OPEN, ExecutionStatus.PARTIAL}
                )
            )
            for result in (poly_result, predict_result)
        )

        updated = replace(
            position,
            status=(
                "manual_review"
                if manual_review_required
                else "closed"
                if poly_filled and predict_filled
                else "partial_exit_pending"
            ),
            polymarket_closed=poly_filled,
            predict_fun_closed=predict_filled,
            polymarket_exit_price=(poly_proceeds / poly_closed_contracts if poly_closed_contracts > EPSILON else None),
            predict_fun_exit_price=(
                predict_proceeds / predict_closed_contracts if predict_closed_contracts > EPSILON else None
            ),
            polymarket_closed_contracts=poly_closed_contracts,
            predict_fun_closed_contracts=predict_closed_contracts,
            polymarket_exit_proceeds_usd=poly_proceeds,
            predict_fun_exit_proceeds_usd=predict_proceeds,
        )
        if poly_residual_error is not None:
            residual_report = poly_residual_error.report
            updated = apply_residual_exit_snapshot(
                updated,
                venue=self._first_leg_label,
                snapshot=ResidualExitSnapshot(
                    venue_order_id=poly_residual_error.order_id,
                    requested_contracts=residual_report.amount_requested,
                    closed_contracts=residual_report.amount_filled,
                    exit_proceeds_usd=residual_report.amount_filled * residual_report.avg_price,
                    residual_contracts=poly_residual_error.residual_contracts,
                ),
            )
        if predict_residual_error is not None:
            residual_report = predict_residual_error.report
            updated = apply_residual_exit_snapshot(
                updated,
                venue=self._second_leg_label,
                snapshot=ResidualExitSnapshot(
                    venue_order_id=predict_residual_error.order_id,
                    requested_contracts=residual_report.amount_requested,
                    closed_contracts=residual_report.amount_filled,
                    exit_proceeds_usd=residual_report.amount_filled * residual_report.avg_price,
                    residual_contracts=predict_residual_error.residual_contracts,
                ),
            )
        poly_closed_contracts = updated.polymarket_closed_contracts
        predict_closed_contracts = updated.predict_fun_closed_contracts
        poly_residual = updated.polymarket_residual_exposure_contracts
        predict_residual = updated.predict_fun_residual_exposure_contracts
        if manual_review_required:
            await self._add_position(updated)
            await self._telegram.send_html(
                "🚨 <b>EXIT REQUIRES MANUAL REVIEW</b>\n"
                f"{self._first_leg_label} closed: {poly_closed_contracts}; residual opposite: {poly_residual}.\n"
                f"{self._second_leg_label} closed: {predict_closed_contracts}; residual opposite: "
                f"{predict_residual}.\nAutomatic exit retries are disabled."
            )
            return
        if not poly_filled or not predict_filled:
            await self._add_position(updated)
            await self._telegram.send_html(
                "🚨 <b>AUTO-CLOSE PARTIAL/FAILED</b>\n"
                f"{self._first_leg_label} exit filled: {poly_filled} ({poly_exit_order_id}).\n"
                f"{self._second_leg_label} exit filled: {predict_filled} ({predict_exit_order_id}).\n"
                "Only the remaining open leg will be retried automatically."
            )
            return

        await self._remove_position(position_key(position.market))
        first_entry_value, second_entry_value = self.gross_entry_values(
            position.market,
            position.polymarket_entry_price,
            position.predict_fun_entry_price,
        )
        entry_cost = (
            position.polymarket_contracts * first_entry_value + position.predict_fun_contracts * second_entry_value
        )
        profit_pct_decimal, profit_usd_decimal = calculate_realized_position_profit_decimal(
            entry_cost,
            updated.polymarket_exit_proceeds_usd + updated.predict_fun_exit_proceeds_usd,
        )
        profit_pct = float(profit_pct_decimal)
        close_signal = ExitSignal(
            position=updated,
            polymarket_exit_price=updated.polymarket_exit_price or polymarket_exit_price,
            predict_fun_exit_price=updated.predict_fun_exit_price or predict_fun_exit_price,
            profit_pct=profit_pct,
            profit_usd=profit_usd_decimal,
        )
        LOGGER.info(
            "binary_position_auto_closed",
            extra={"_poly_exit_order_id": poly_exit_order_id, "_predict_exit_order_id": predict_exit_order_id},
        )
        if profit_usd_decimal < 0 and await self._risk.record_realized_result(profit_usd_decimal):
            await self._telegram.send_html(
                "🚨 <b>GLOBAL DAILY LOSS HARD STOP</b>\n"
                f"Realized daily loss: ${self._risk.daily_loss_usd:.2f}; "
                f"limit: ${self._config.max_daily_loss_usd:.2f}. Manual resume required."
            )
        await self._telegram.send_html(format_exit_message(close_signal, is_test=False))

    async def _submit_exit_leg(
        self,
        *,
        client: BinaryMarketClient,
        market: MarketSpec,
        venue_label: str,
        already_closed: bool,
        token_id: str,
        side: BinarySide,
        contracts: Decimal,
        min_price: Decimal,
        timeout_ms: int,
        condition_id: str | None = None,
        tick_size: str | None = None,
        neg_risk: bool | None = None,
    ) -> ExitLegResult:
        if already_closed:
            return ExitLegResult("already-closed", None)
        order_id = "failed-before-order"
        client_order_id = str(uuid7())
        final_intent_status: OrderIntentStatus | None = None

        async def persist_order_id(prepared_order_id: str) -> None:
            nonlocal order_id
            if not prepared_order_id:
                raise RuntimeError(f"{venue_label} returned an empty venue order id")
            order_id = prepared_order_id
            if self._repository is not None:
                await self._repository.update_order_intent(
                    client_order_id,
                    OrderIntentStatus.SUBMITTING,
                    venue_order_id=prepared_order_id,
                )

        if self._repository is not None:
            await self._repository.create_order_intent(
                OrderIntent(
                    client_order_id=client_order_id,
                    route=self._route_name(),
                    market_key=position_key(market),
                    venue=venue_label,
                    token_id=token_id,
                    binary_side=side,
                    action="SELL",
                    quantity=Decimal(str(contracts)),
                    limit_price=Decimal(str(min_price)),
                )
            )
            await self._repository.update_order_intent(client_order_id, OrderIntentStatus.SUBMITTING)
        try:
            returned_order_id = await client.sell_with_order_id_persistence(
                token_id=token_id,
                side=side,
                contracts=float(contracts),
                min_price=float(min_price),
                persist_order_id=persist_order_id,
                client_order_id=client_order_id,
                condition_id=condition_id,
                tick_size=tick_size,
                neg_risk=neg_risk,
            )
            if order_id == "failed-before-order" or returned_order_id.lower() != order_id.lower():
                raise RuntimeError(f"{venue_label} did not persist the submitted venue order id")
            order_id = returned_order_id
            if self._repository is not None:
                await self._repository.update_order_intent(
                    client_order_id,
                    OrderIntentStatus.ACKNOWLEDGED,
                    venue_order_id=order_id,
                )
            self._active_orders[(id(client), order_id)] = client
            report = await client.wait_filled(order_id, timeout_ms)
            if not report.is_filled:
                report = await self._cancel_and_reconcile(client, order_id, report)
            report = replace(report, client_order_id=client_order_id, venue_order_id=order_id)
            if self._repository is not None:
                final_intent_status = _intent_status_from_report(report)
                await self._repository.update_order_intent(
                    client_order_id,
                    final_intent_status,
                    venue_order_id=order_id,
                )
                if final_intent_status is OrderIntentStatus.UNKNOWN:
                    await self._risk.pause(f"unknown order outcome: {venue_label} client_order_id={client_order_id}")
            return ExitLegResult(order_id, report)
        except OrderResidualExposure as exc:
            order_id = exc.order_id
            report = replace(
                exc.report,
                client_order_id=client_order_id,
                venue_order_id=order_id,
            )
            final_intent_status = OrderIntentStatus.MANUAL_REVIEW
            if self._repository is not None:
                try:
                    await self._repository.update_order_intent(
                        client_order_id,
                        final_intent_status,
                        venue_order_id=order_id,
                        error=str(exc),
                    )
                except Exception:
                    LOGGER.exception(
                        "residual_exit_intent_persistence_failed",
                        extra={"_client_order_id": client_order_id, "_order_id": order_id},
                    )
            await self._risk.pause(
                f"residual opposite exposure: {venue_label} client_order_id={client_order_id}"
            )
            return ExitLegResult(order_id, report, exc)
        except asyncio.CancelledError:
            if self._repository is not None:
                final_intent_status = OrderIntentStatus.UNKNOWN
                await self._repository.update_order_intent(
                    client_order_id,
                    OrderIntentStatus.UNKNOWN,
                    venue_order_id=None if order_id == "failed-before-order" else order_id,
                    error="exit submission cancelled during shutdown",
                )
            if order_id != "failed-before-order":
                try:
                    await client.cancel_order(order_id)
                except Exception:
                    LOGGER.exception("exit_cancel_during_shutdown_failed", extra={"_order_id": order_id})
            raise
        except Exception as exc:
            reconciled_error: Exception = exc
            exception_order_id = getattr(exc, "order_id", None)
            if order_id == "failed-before-order" and isinstance(exception_order_id, str) and exception_order_id:
                order_id = exception_order_id
            if isinstance(exc, TransactionTimeoutException):
                await self._telegram.send_html(
                    f"🚨 <b>NONCE/TRANSACTION TIMEOUT</b>\nOrder: {order_id}; timeout: {timeout_ms}ms; reason: {exc}."
                )
            reconciled_report: ExecutionReport | None = None
            if order_id != "failed-before-order":
                try:
                    await client.cancel_order(order_id)
                except Exception:
                    LOGGER.exception("exit_cancel_after_error_failed", extra={"_order_id": order_id})
                try:
                    reconciled_report = await client.wait_filled(
                        order_id,
                        self._config.cancel_reconcile_timeout_ms,
                    )
                except OrderResidualExposure as residual_exc:
                    reconciled_report = residual_exc.report
                    reconciled_error = residual_exc
                except Exception:
                    LOGGER.exception("exit_reconcile_after_error_failed", extra={"_order_id": order_id})
            if reconciled_report is not None:
                reconciled_report = replace(
                    reconciled_report,
                    client_order_id=client_order_id,
                    venue_order_id=order_id,
                )
            if self._repository is not None:
                final_intent_status = (
                    OrderIntentStatus.MANUAL_REVIEW
                    if isinstance(reconciled_error, OrderResidualExposure)
                    else _intent_status_from_report(reconciled_report)
                    if reconciled_report is not None
                    else OrderIntentStatus.UNKNOWN
                )
                try:
                    await self._repository.update_order_intent(
                        client_order_id,
                        final_intent_status,
                        venue_order_id=None if order_id == "failed-before-order" else order_id,
                        error=str(reconciled_error),
                    )
                except Exception:
                    if not isinstance(reconciled_error, OrderResidualExposure):
                        raise
                    LOGGER.exception(
                        "residual_exit_intent_persistence_failed",
                        extra={"_client_order_id": client_order_id, "_order_id": order_id},
                    )
                if final_intent_status is OrderIntentStatus.UNKNOWN:
                    await self._risk.pause(f"unknown order outcome: {venue_label} client_order_id={client_order_id}")
            if isinstance(reconciled_error, OrderResidualExposure):
                await self._risk.pause(
                    f"residual opposite exposure: {venue_label} client_order_id={client_order_id}"
                )
            return ExitLegResult(order_id, reconciled_report, reconciled_error)
        finally:
            if order_id != "failed-before-order":
                self._active_orders.pop((id(client), order_id), None)
                forget_order = getattr(client, "forget_order", None)
                if callable(forget_order) and (
                    self._repository is None
                    or final_intent_status in {OrderIntentStatus.FILLED, OrderIntentStatus.CANCELLED}
                ):
                    forget_order(order_id)

    @staticmethod
    def _normalize_exit_result(
        result: ExitLegResult | BaseException,
        venue_label: str,
    ) -> ExitLegResult:
        if isinstance(result, BaseException):
            if isinstance(result, asyncio.CancelledError):
                raise result
            LOGGER.error("exit_leg_failed", extra={"_venue": venue_label, "_error": str(result)})
            return ExitLegResult("failed-before-order", None, result if isinstance(result, Exception) else None)
        if result.error is not None:
            LOGGER.error("exit_leg_failed", extra={"_venue": venue_label, "_error": str(result.error)})
        return result

    def _should_send_signal_alert(self, signal: ArbitrageSignal) -> bool:
        key = _signal_key(signal)
        now = datetime.now(UTC)
        last_sent = self._last_signal_alert_at.get(key)
        if last_sent is not None:
            elapsed = (now - last_sent).total_seconds()
            if elapsed < self._config.signal_alert_cooldown_seconds:
                return False
        self._last_signal_alert_at[key] = now
        return True

    async def _reserve_signal_capital(self, signal: ArbitrageSignal) -> bool:
        if not self._balance_cache:
            await self.start()
        required_first = signal.plan.polymarket_capital_usd + signal.plan.polymarket_fee_usd
        required_second = signal.plan.predict_fun_capital_usd + signal.plan.predict_fun_fee_usd
        async with self._capacity_lock:
            first_available = self._effective_balance(self._first_leg_label) - _d(
                self._capital_reservations.get(self._first_leg_label, ZERO)
            )
            second_available = self._effective_balance(self._second_leg_label) - _d(
                self._capital_reservations.get(self._second_leg_label, ZERO)
            )
            if first_available < required_first or second_available < required_second:
                LOGGER.info(
                    "signal_skipped_insufficient_balance",
                    extra={
                        "_symbol": signal.market.symbol,
                        "_first_available": first_available,
                        "_first_required": required_first,
                        "_second_available": second_available,
                        "_second_required": required_second,
                    },
                )
                return False
            self._capital_reservations[self._first_leg_label] = (
                _d(self._capital_reservations.get(self._first_leg_label, ZERO)) + required_first
            )
            self._capital_reservations[self._second_leg_label] = (
                _d(self._capital_reservations.get(self._second_leg_label, ZERO)) + required_second
            )
            await self._persist_runtime_balance_state()
            return True

    def _release_signal_capital(self, signal: ArbitrageSignal) -> None:
        releases = {
            self._first_leg_label: signal.plan.polymarket_capital_usd + signal.plan.polymarket_fee_usd,
            self._second_leg_label: signal.plan.predict_fun_capital_usd + signal.plan.predict_fun_fee_usd,
        }
        for label, amount in releases.items():
            remaining = max(ZERO, _d(self._capital_reservations.get(label, ZERO)) - amount)
            if remaining <= EPSILON:
                self._capital_reservations.pop(label, None)
            else:
                self._capital_reservations[label] = remaining

    async def _run_balance_updater(self) -> None:
        while True:
            await asyncio.sleep(self._config.balance_refresh_interval_seconds)
            await self._refresh_balances()

    def _runtime_balance_state_snapshot(self) -> dict[str, object]:
        venue_labels = sorted(
            {
                self._first_leg_label,
                self._second_leg_label,
                *(str(label) for label in self._balance_cache),
                *(str(label) for label in self._optimistic_debits),
                *(str(label) for label in self._capital_reservations),
            }
        )
        venues: dict[str, dict[str, str]] = {}
        balance_cache: dict[str, str] = {}
        optimistic_debits: dict[str, str] = {}
        capital_reservations: dict[str, str] = {}
        effective_balances: dict[str, str] = {}
        available_after_reservations: dict[str, str] = {}
        for venue in venue_labels:
            cached = _d(self._balance_cache.get(venue, ZERO))
            debits = _d(self._optimistic_debits.get(venue, ZERO))
            reservations = _d(self._capital_reservations.get(venue, ZERO))
            effective = max(ZERO, cached - debits)
            available = max(ZERO, effective - reservations)
            balance_cache[venue] = str(cached)
            optimistic_debits[venue] = str(debits)
            capital_reservations[venue] = str(reservations)
            effective_balances[venue] = str(effective)
            available_after_reservations[venue] = str(available)
            venues[venue] = {
                "balance_cache_usd": str(cached),
                "optimistic_debits_usd": str(debits),
                "capital_reservations_usd": str(reservations),
                "effective_balance_usd": str(effective),
                "available_after_reservations_usd": str(available),
            }
        return {
            "captured_at": datetime.now(UTC).isoformat(),
            "runtime_instance_id": self._config.runtime_instance_id,
            "route": route_key(self._first_leg_label, self._second_leg_label),
            "balance_cache_usd": balance_cache,
            "optimistic_debits_usd": optimistic_debits,
            "capital_reservations_usd": capital_reservations,
            "effective_balances_usd": effective_balances,
            "available_after_reservations_usd": available_after_reservations,
            "venues": venues,
        }

    async def _persist_runtime_balance_state(self) -> None:
        if self._repository is None:
            return
        await self._repository.record_runtime_balance_state(self._runtime_balance_state_snapshot())

    async def _refresh_balances(self) -> None:
        try:
            first_balance, second_balance = await asyncio.gather(
                self._first_leg.get_cash_balance(),
                self._second_leg.get_cash_balance(),
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            LOGGER.exception("balance_cache_refresh_failed")
            return
        self._apply_balance_refresh(self._first_leg_label, first_balance)
        self._apply_balance_refresh(self._second_leg_label, second_balance)
        await self._persist_runtime_balance_state()
        minimum = _d(self._config.min_venue_balance_usd)
        now = time.monotonic()
        effective_first = self._effective_balance(self._first_leg_label)
        effective_second = self._effective_balance(self._second_leg_label)
        if min(effective_first, effective_second) < minimum and now - self._last_low_balance_alert_at >= 600:
            self._last_low_balance_alert_at = now
            await self._telegram.send_html(
                "⚠️ <b>LOW VENUE BALANCE</b>\n"
                f"{self._first_leg_label}: ${effective_first:.2f}; "
                f"{self._second_leg_label}: ${effective_second:.2f}; minimum: ${minimum:.2f}."
            )

    async def _record_api_error(self) -> None:
        self._consecutive_api_errors += 1
        if await self._risk.record_api_error():
            await self._telegram.send_html(
                "🚨 <b>GLOBAL EXECUTION CIRCUIT BREAKER OPEN</b>\n"
                f"Consecutive API errors: {self._risk.consecutive_api_errors}; "
                f"reason: {self._risk.pause_reason}. Manual resume required."
            )

    def _record_shadow_preflight(self, outcome: str) -> None:
        if self._shadow_preflight_observer is not None:
            self._shadow_preflight_observer(self._route_name(), outcome)

    async def _shadow_preflight_guard(self, signal: ArbitrageSignal) -> bool:
        route = self._route_name()
        market_key = position_key(signal.market)
        now = time.monotonic()
        last_attempt = self._last_shadow_preflight_at.get(market_key)
        if last_attempt is not None and now - last_attempt < self._config.shadow_preflight_cooldown_seconds:
            self._record_shadow_preflight("cooldown_skipped")
            LOGGER.debug(
                "shadow_preflight_cooldown_skipped",
                extra={"_symbol": signal.market.symbol, "_route": route},
            )
            return False
        self._last_shadow_preflight_at[market_key] = now

        if not await self._market_constraints_guard(signal):
            self._record_shadow_preflight("constraints_rejected")
            LOGGER.info(
                "shadow_preflight_evidence_rejected",
                extra={
                    "_symbol": signal.market.symbol,
                    "_route": route,
                    "_completed_samples": 0,
                    "_required_samples": self._config.shadow_preflight_samples,
                    "_reason": "constraints_rejected",
                },
            )
            return False

        required_samples = self._config.shadow_preflight_samples
        samples: list[dict[str, Any]] = []
        for sample_index in range(required_samples):
            prepared_entry = await self._preflight_price_guard(signal)
            if prepared_entry is None:
                self._record_shadow_preflight("sample_rejected")
                LOGGER.info(
                    "shadow_preflight_evidence_rejected",
                    extra={
                        "_symbol": signal.market.symbol,
                        "_route": route,
                        "_completed_samples": sample_index,
                        "_required_samples": required_samples,
                        "_reason": "signed_preflight_rejected",
                    },
                )
                return False
            evidence = dict(prepared_entry.evidence)
            evidence["sample"] = sample_index + 1
            samples.append(evidence)
            if sample_index + 1 < required_samples:
                await asyncio.sleep(self._config.shadow_preflight_sample_interval_seconds)

        evidence_payload = {
            "schema_version": 1,
            "captured_at": datetime.now(UTC).isoformat(),
            "runtime_instance_id": self._config.runtime_instance_id,
            "release_sha": self._release_sha,
            "route": route,
            "market_key": market_key,
            "market": {
                "symbol": signal.market.symbol,
                "target_label": signal.market.target_label,
                "rules_fingerprint": signal.market.rules_fingerprint,
                "expires_at": signal.market.expires_at.isoformat() if signal.market.expires_at else None,
                "cutoff_at": signal.market.cutoff_at.isoformat() if signal.market.cutoff_at else None,
            },
            "completed_samples": len(samples),
            "required_samples": required_samples,
            "samples": samples,
        }
        if not is_live_mapping_eligible(signal.market, ExecutionMode.CANARY, route):
            self._record_shadow_preflight("route_not_verified")
            LOGGER.info(
                "shadow_preflight_evidence_rejected",
                extra={
                    "_symbol": signal.market.symbol,
                    "_route": route,
                    "_completed_samples": required_samples,
                    "_required_samples": required_samples,
                    "_reason": "route_not_verified",
                },
            )
            # Shadow exploration may still emit its normal test alert, but an
            # unverified mapping must never become durable launch evidence.
            return True
        if self._repository is not None:
            if not self._release_sha:
                self._record_shadow_preflight("evidence_persist_rejected")
                LOGGER.error(
                    "shadow_preflight_evidence_persist_rejected",
                    extra={"_symbol": signal.market.symbol, "_route": route, "_reason": "release_sha_missing"},
                )
                return False
            try:
                await self._repository.record_shadow_preflight_evidence(evidence_payload)
            except Exception:
                self._record_shadow_preflight("evidence_persist_rejected")
                LOGGER.exception(
                    "shadow_preflight_evidence_persist_failed",
                    extra={"_symbol": signal.market.symbol, "_route": route},
                )
                return False

        self._record_shadow_preflight("evidence_passed")
        LOGGER.info(
            "shadow_preflight_evidence",
            extra={
                "_symbol": signal.market.symbol,
                "_route": route,
                "_completed_samples": required_samples,
                "_required_samples": required_samples,
                "_net_spread": signal.metrics.net_spread,
            },
        )
        return True

    async def _preflight_price_guard(self, signal: ArbitrageSignal) -> PreparedEntry | None:
        preflight_started = time.perf_counter()
        target_notional = self._config.position_size_usd / 2.0
        required_depth = target_notional * self._config.spread_policy.depth_buffer
        try:
            first_book, second_book = await asyncio.gather(
                self._first_leg.watch_order_book(self._first_leg_token_id(signal.market)),
                self._second_leg.watch_order_book(self._second_leg_token_id(signal.market)),
            )
        except Exception:
            LOGGER.exception("preflight_orderbook_check_failed", extra={"_symbol": signal.market.symbol})
            LOGGER.warning(
                "preflight_liquidity_rejected",
                extra=self._preflight_liquidity_log_extra(
                    signal,
                    None,
                    None,
                    target_notional=target_notional,
                    reason="orderbook_fetch_failed",
                ),
            )
            return None

        if not first_book.asks or not second_book.asks:
            LOGGER.warning("preflight_price_guard_empty_book", extra={"_symbol": signal.market.symbol})
            LOGGER.warning(
                "preflight_liquidity_rejected",
                extra=self._preflight_liquidity_log_extra(
                    signal,
                    first_book,
                    second_book,
                    target_notional=target_notional,
                    reason="empty_orderbook",
                ),
            )
            return None
        if first_book.status is not MarketDataStatus.VALID or second_book.status is not MarketDataStatus.VALID:
            LOGGER.error("preflight_price_guard_invalid_book_rejected", extra={"_symbol": signal.market.symbol})
            LOGGER.warning(
                "preflight_liquidity_rejected",
                extra=self._preflight_liquidity_log_extra(
                    signal,
                    first_book,
                    second_book,
                    target_notional=target_notional,
                    reason="invalid_market_data_status",
                ),
            )
            return None
        now = time.time()
        first_age = max(0.0, now - first_book.timestamp)
        second_age = max(0.0, now - second_book.timestamp)
        first_fresh = self._first_leg.is_order_book_execution_fresh(
            self._first_leg_token_id(signal.market),
            first_book,
            self._config.max_orderbook_age_seconds,
        )
        second_fresh = self._second_leg.is_order_book_execution_fresh(
            self._second_leg_token_id(signal.market),
            second_book,
            self._config.max_orderbook_age_seconds,
        )
        if not first_fresh or not second_fresh:
            LOGGER.error(
                "preflight_price_guard_stale_book_rejected",
                extra={
                    "_symbol": signal.market.symbol,
                    "_first_age_sec": first_age,
                    "_second_age_sec": second_age,
                    "_max_allowed": self._config.max_orderbook_age_seconds,
                },
            )
            LOGGER.warning(
                "preflight_liquidity_rejected",
                extra=self._preflight_liquidity_log_extra(
                    signal,
                    first_book,
                    second_book,
                    target_notional=target_notional,
                    reason="stale_orderbook",
                ),
            )
            return None
        first_top_depth = top_of_book_ask_depth_usd(first_book)
        second_top_depth = top_of_book_ask_depth_usd(second_book)
        required_depth_decimal = Decimal(str(required_depth))
        if first_top_depth < required_depth_decimal or second_top_depth < required_depth_decimal:
            LOGGER.warning(
                "preflight_top_of_book_depth_rejected",
                extra={
                    "_symbol": signal.market.symbol,
                    "_route": self._route_name(),
                    "_first_top_depth_usd": str(first_top_depth),
                    "_second_top_depth_usd": str(second_top_depth),
                    "_required_top_depth_usd": str(required_depth_decimal),
                },
            )
            LOGGER.warning(
                "preflight_liquidity_rejected",
                extra=self._preflight_liquidity_log_extra(
                    signal,
                    first_book,
                    second_book,
                    target_notional=target_notional,
                    reason="top_of_book_depth_below_no_impact_buffer",
                ),
            )
            return None
        first_quote: FillQuote | None = None
        second_quote: FillQuote | None = None
        try:
            first_quote = orderbook_buy_quote(first_book, target_notional)
            second_quote = orderbook_buy_quote(second_book, target_notional)
        except ValueError as exc:
            LOGGER.warning(
                "preflight_full_depth_quote_rejected",
                extra={"_symbol": signal.market.symbol, "_reason": str(exc)},
            )
            LOGGER.warning(
                "preflight_liquidity_rejected",
                extra=self._preflight_liquidity_log_extra(
                    signal,
                    first_book,
                    second_book,
                    target_notional=target_notional,
                    first_quote=first_quote,
                    second_quote=second_quote,
                    reason=str(exc),
                ),
            )
            return None
        first_limit = signal.polymarket_price * (1.0 + self._venue_slippage_cap(self._first_leg_label))
        second_limit = signal.predict_fun_price * (1.0 + self._venue_slippage_cap(self._second_leg_label))
        first_submit_limit = min(
            Decimal("0.999999"),
            Decimal(str(first_book.best_ask.price))
            * (Decimal(1) + Decimal(str(self._venue_slippage_cap(self._first_leg_label)))),
        )
        second_submit_limit = min(
            Decimal("0.999999"),
            Decimal(str(second_book.best_ask.price))
            * (Decimal(1) + Decimal(str(self._venue_slippage_cap(self._second_leg_label)))),
        )
        route = self._route_name()
        dynamic_threshold = max(
            self._config.min_net_spread,
            self._config.spread_policy.threshold_for(route),
        )
        try:
            chain_cost_quote = await self._chain_cost_estimator.estimate(
                route,
                require_live=(self._config.spread_policy.require_live_gas_estimate and not self._config.is_test),
            )
        except LiveChainCostUnavailable as exc:
            LOGGER.error(
                "preflight_live_chain_cost_unavailable",
                extra={"_symbol": signal.market.symbol, "_route": route, "_reason": str(exc)},
            )
            return None
        try:
            first_constraints, second_constraints = await asyncio.gather(
                self._first_leg.get_market_constraints(
                    self._first_leg_token_id(signal.market),
                    signal.market.condition_id if self._first_leg_label == "Polymarket" else None,
                ),
                self._second_leg.get_market_constraints(
                    self._second_leg_token_id(signal.market),
                    signal.market.condition_id if self._second_leg_label == "Polymarket" else None,
                ),
            )
            if first_constraints is None or second_constraints is None:
                raise ValueError("live market constraints are unavailable")
            first_fee_quote, second_fee_quote = await asyncio.gather(
                self._first_leg.get_fee_quote(
                    self._first_leg_token_id(signal.market),
                    Decimal(str(first_quote.avg_price)),
                    first_constraints,
                ),
                self._second_leg.get_fee_quote(
                    self._second_leg_token_id(signal.market),
                    Decimal(str(second_quote.avg_price)),
                    second_constraints,
                ),
            )
            if (
                first_fee_quote is None
                or second_fee_quote is None
                or not first_fee_quote.verified
                or not second_fee_quote.verified
            ):
                raise ValueError("live fee metadata is unavailable")
            refreshed_metrics = calculate_spread_metrics(
                polymarket_book=first_book,
                predict_fun_book=second_book,
                max_order_size_usd=target_notional,
                min_net_spread=dynamic_threshold,
                max_slippage_pct=min(
                    self._venue_slippage_cap(self._first_leg_label),
                    self._venue_slippage_cap(self._second_leg_label),
                ),
                polymarket_side=self._first_leg_side(signal.market),
                predict_fun_side=self._second_leg_side(signal.market),
                polymarket_fee_quote=first_fee_quote,
                predict_fun_fee_quote=second_fee_quote,
                required_executable_depth_usd=required_depth,
                fixed_chain_cost_usd=float(chain_cost_quote.reserved_cost_usd),
                max_price_impact=self._config.max_production_price_impact,
            )
        except ValueError as exc:
            LOGGER.warning(
                "preflight_full_depth_quote_rejected",
                extra={"_symbol": signal.market.symbol, "_reason": str(exc)},
            )
            LOGGER.warning(
                "preflight_liquidity_rejected",
                extra=self._preflight_liquidity_log_extra(
                    signal,
                    first_book,
                    second_book,
                    target_notional=target_notional,
                    first_quote=first_quote,
                    second_quote=second_quote,
                    reason=str(exc),
                ),
            )
            return None
        current_spread = refreshed_metrics.net_spread
        rejection_reasons: list[str] = []
        first_preview: OrderPreview | None = None
        second_preview: OrderPreview | None = None
        refreshed_plan: PositionPlan | None = None
        refreshed_all_in_cost: Decimal | None = None
        first_preview_blockers: tuple[str, ...] = ()
        second_preview_blockers: tuple[str, ...] = ()
        if first_book.best_ask.price > first_limit:
            rejection_reasons.append("first_leg_best_ask_above_limit")
        if second_book.best_ask.price > second_limit:
            rejection_reasons.append("second_leg_best_ask_above_limit")
        if current_spread < dynamic_threshold:
            rejection_reasons.append("net_spread_below_dynamic_threshold")
        target_notional_decimal = Decimal(str(target_notional))
        payout_contracts = min(
            Decimal(str(first_quote.contracts)),
            Decimal(str(second_quote.contracts)),
            target_notional_decimal / first_submit_limit,
            target_notional_decimal / second_submit_limit,
        )
        variable_cost = first_fee_quote.fee_for_fill(
            payout_contracts,
            Decimal(str(first_quote.avg_price)),
        ) + second_fee_quote.fee_for_fill(
            payout_contracts,
            Decimal(str(second_quote.avg_price)),
        )
        minimum_profit = max(
            Decimal(str(self._config.spread_policy.min_expected_profit_usd)),
            variable_cost * Decimal(2),
        )
        if Decimal(str(refreshed_metrics.expected_net_profit_usd)) < minimum_profit:
            rejection_reasons.append("expected_profit_below_minimum")
        if not rejection_reasons:
            preview_contracts = payout_contracts
            try:
                first_preview, second_preview = await asyncio.gather(
                    self._first_leg.preview_buy(
                        self._first_leg_token_id(signal.market),
                        self._first_leg_side(signal.market),
                        preview_contracts,
                        first_submit_limit,
                        condition_id=signal.market.condition_id if self._first_leg_label == "Polymarket" else None,
                        tick_size=(str(first_constraints.tick_size) if self._first_leg_label == "Polymarket" else None),
                        neg_risk=(
                            signal.market.neg_risk
                            if self._first_leg_label == "Polymarket"
                            else signal.market.predict_fun_neg_risk
                            if self._first_leg_label == "Predict.fun"
                            else None
                        ),
                    ),
                    self._second_leg.preview_buy(
                        self._second_leg_token_id(signal.market),
                        self._second_leg_side(signal.market),
                        preview_contracts,
                        second_submit_limit,
                        condition_id=signal.market.condition_id if self._second_leg_label == "Polymarket" else None,
                        tick_size=(
                            str(second_constraints.tick_size) if self._second_leg_label == "Polymarket" else None
                        ),
                        neg_risk=(
                            signal.market.neg_risk
                            if self._second_leg_label == "Polymarket"
                            else signal.market.predict_fun_neg_risk
                            if self._second_leg_label == "Predict.fun"
                            else None
                        ),
                    ),
                )
            except Exception:
                LOGGER.exception("signed_pre_submit_preview_failed", extra={"_symbol": signal.market.symbol})
                return None
            assert first_preview is not None and second_preview is not None
            if not first_preview.executable or not second_preview.executable:
                rejection_reasons.append("signed_pre_submit_preview_rejected")
                if not first_preview.executable:
                    first_preview_blockers = _safe_preview_blockers(first_preview)
                    rejection_reasons.extend(f"first_leg_preview:{item}" for item in first_preview_blockers)
                if not second_preview.executable:
                    second_preview_blockers = _safe_preview_blockers(second_preview)
                    rejection_reasons.extend(f"second_leg_preview:{item}" for item in second_preview_blockers)
            if first_preview.executable and second_preview.executable:
                if not first_preview.price_impact_pct.is_finite() or first_preview.price_impact_pct != ZERO:
                    rejection_reasons.append("first_leg_signed_preview_price_impact_nonzero")
                if not second_preview.price_impact_pct.is_finite() or second_preview.price_impact_pct != ZERO:
                    rejection_reasons.append("second_leg_signed_preview_price_impact_nonzero")
                if first_preview.requested_contracts != second_preview.requested_contracts:
                    rejection_reasons.append("signed_preview_quantity_mismatch")
                first_guaranteed_contracts = (
                    first_preview.guaranteed_contracts
                    if first_preview.guaranteed_contracts is not None
                    else first_preview.requested_contracts
                )
                second_guaranteed_contracts = (
                    second_preview.guaranteed_contracts
                    if second_preview.guaranteed_contracts is not None
                    else second_preview.requested_contracts
                )
                if first_guaranteed_contracts <= 0 or second_guaranteed_contracts <= 0:
                    LOGGER.error(
                        "signed_preview_guaranteed_payout_invalid",
                        extra={"_symbol": signal.market.symbol, "_route": route},
                    )
                    return None
                guaranteed_payout_contracts = min(
                    first_guaranteed_contracts,
                    second_guaranteed_contracts,
                )
                assert first_preview.fee_quote is not None and second_preview.fee_quote is not None
                first_worst_capital = (
                    first_preview.maximum_notional_usd
                    if first_preview.maximum_notional_usd is not None
                    else first_preview.requested_contracts * first_preview.limit_price
                )
                second_worst_capital = (
                    second_preview.maximum_notional_usd
                    if second_preview.maximum_notional_usd is not None
                    else second_preview.requested_contracts * second_preview.limit_price
                )
                if first_worst_capital > target_notional_decimal or second_worst_capital > target_notional_decimal:
                    rejection_reasons.append("signed_preview_leg_notional_above_limit")
                first_worst_fee = (
                    first_preview.maximum_fee_usd
                    if first_preview.maximum_fee_usd is not None
                    else _worst_case_fee_between(
                        first_preview.fee_quote,
                        first_preview.requested_contracts,
                        first_preview.average_price,
                        first_preview.limit_price,
                    )
                )
                second_worst_fee = (
                    second_preview.maximum_fee_usd
                    if second_preview.maximum_fee_usd is not None
                    else _worst_case_fee_between(
                        second_preview.fee_quote,
                        second_preview.requested_contracts,
                        second_preview.average_price,
                        second_preview.limit_price,
                    )
                )
                worst_variable_cost = first_worst_fee + second_worst_fee
                worst_minimum_profit = max(
                    Decimal(str(self._config.spread_policy.min_expected_profit_usd)),
                    worst_variable_cost * Decimal(2),
                )
                worst_all_in_cost = (
                    first_worst_capital
                    + second_worst_capital
                    + worst_variable_cost
                    + chain_cost_quote.reserved_cost_usd
                )
                worst_profit = guaranteed_payout_contracts - worst_all_in_cost
                worst_net_spread = worst_profit / guaranteed_payout_contracts
                if worst_net_spread < Decimal(str(dynamic_threshold)):
                    rejection_reasons.append("signed_preview_worst_case_net_spread_below_dynamic_threshold")
                if worst_profit < worst_minimum_profit:
                    rejection_reasons.append("signed_preview_worst_case_profit_below_minimum")
                refreshed_plan = PositionPlan(
                    polymarket_contracts=first_preview.requested_contracts,
                    polymarket_capital_usd=first_worst_capital,
                    predict_fun_contracts=second_preview.requested_contracts,
                    predict_fun_capital_usd=second_worst_capital,
                    payout_contracts=guaranteed_payout_contracts,
                    total_cost_usd=(first_worst_capital + second_worst_capital + first_worst_fee + second_worst_fee),
                    polymarket_fee_usd=first_worst_fee,
                    predict_fun_fee_usd=second_worst_fee,
                )
                refreshed_all_in_cost = worst_all_in_cost
                refreshed_metrics = SpreadMetrics(
                    gross_spread=float(
                        (guaranteed_payout_contracts - first_worst_capital - second_worst_capital)
                        / guaranteed_payout_contracts
                    ),
                    net_spread=float(worst_net_spread),
                    expected_net_profit_usd=float(worst_profit),
                    polymarket_slippage=float(first_preview.price_impact_pct),
                    predict_fun_slippage=float(second_preview.price_impact_pct),
                    combined_cost_per_payout=float(worst_all_in_cost / guaranteed_payout_contracts),
                    fixed_chain_cost_usd=float(chain_cost_quote.reserved_cost_usd),
                )
                variable_cost = worst_variable_cost
                minimum_profit = worst_minimum_profit
                current_spread = float(worst_net_spread)
        if rejection_reasons:
            LOGGER.warning(
                "preflight_price_guard_rejected",
                extra={
                    "_symbol": signal.market.symbol,
                    "_route": route,
                    "_reason": ",".join(rejection_reasons),
                    "_first_price": first_book.best_ask.price,
                    "_first_limit": first_limit,
                    "_first_preview_executable": (first_preview.executable if first_preview is not None else None),
                    "_first_preview_signing_validated": (
                        first_preview.signing_validated if first_preview is not None else None
                    ),
                    "_first_preview_depth_usd": (
                        str(first_preview.available_depth_usd) if first_preview is not None else None
                    ),
                    "_first_preview_fee_verified": (
                        first_preview.fee_quote.verified
                        if first_preview is not None and first_preview.fee_quote is not None
                        else None
                    ),
                    "_first_preview_blockers": ",".join(first_preview_blockers),
                    "_second_price": second_book.best_ask.price,
                    "_second_limit": second_limit,
                    "_second_preview_executable": (second_preview.executable if second_preview is not None else None),
                    "_second_preview_signing_validated": (
                        second_preview.signing_validated if second_preview is not None else None
                    ),
                    "_second_preview_depth_usd": (
                        str(second_preview.available_depth_usd) if second_preview is not None else None
                    ),
                    "_second_preview_fee_verified": (
                        second_preview.fee_quote.verified
                        if second_preview is not None and second_preview.fee_quote is not None
                        else None
                    ),
                    "_second_preview_blockers": ",".join(second_preview_blockers),
                    "_current_spread": current_spread,
                    "_spread_floor": dynamic_threshold,
                    "_expected_profit_usd": refreshed_metrics.expected_net_profit_usd,
                    "_minimum_profit_usd": float(minimum_profit),
                },
            )
            LOGGER.warning(
                "preflight_liquidity_rejected",
                extra=self._preflight_liquidity_log_extra(
                    signal,
                    first_book,
                    second_book,
                    target_notional=target_notional,
                    current_spread=current_spread,
                    first_quote=first_quote,
                    second_quote=second_quote,
                    reason=",".join(rejection_reasons),
                ),
            )
            if self._config.execution_mode.submits_orders:
                await self._telegram.send_html(
                    "⚠️ <b>SPREAD GUARD REJECTED</b>\n"
                    f"Market: {signal.market.symbol}\n"
                    f"{self._first_leg_label}: {first_book.best_ask.price:.6f} / limit {first_limit:.6f}\n"
                    f"{self._second_leg_label}: {second_book.best_ask.price:.6f} / limit {second_limit:.6f}\n"
                    f"Spread: {current_spread:.4%} / floor {dynamic_threshold:.4%}."
                )
            return None
        LOGGER.info(
            "preflight_liquidity_analysis",
            extra=self._preflight_liquidity_log_extra(
                signal,
                first_book,
                second_book,
                target_notional=target_notional,
                current_spread=current_spread,
                first_quote=first_quote,
                second_quote=second_quote,
            ),
        )
        adverse_move = self._config.spread_policy.adverse_move_p95_pct_by_route.get(
            route,
            self._config.spread_policy.adverse_move_p95_pct,
        )
        preflight_latency = time.perf_counter() - preflight_started
        first_depth = executable_depth_usd(first_book)
        second_depth = executable_depth_usd(second_book)
        if self._preflight_observer is not None:
            self._preflight_observer(
                route,
                {
                    "first_executable_depth_usd": float(first_depth),
                    "second_executable_depth_usd": float(second_depth),
                    "fee_cost_usd": float(variable_cost),
                    "chain_cost_usd": float(chain_cost_quote.reserved_cost_usd),
                    "expected_profit_usd": refreshed_metrics.expected_net_profit_usd,
                    "dynamic_threshold": dynamic_threshold,
                    "adverse_move_reserve": adverse_move + self._config.spread_policy.safety_buffer_pct,
                    "preflight_latency_seconds": preflight_latency,
                },
            )
        assert first_preview is not None and second_preview is not None
        assert refreshed_plan is not None and refreshed_all_in_cost is not None
        assert first_preview.fee_quote is not None and second_preview.fee_quote is not None
        assert first_preview.payload_fingerprint and second_preview.payload_fingerprint
        if self._accepted_preflight_observer is not None:
            self._accepted_preflight_observer(route)
        prepared_market = signal.market
        if self._first_leg_label == "Polymarket":
            prepared_market = replace(
                prepared_market,
                tick_size=str(first_constraints.tick_size),
                neg_risk=signal.market.neg_risk,
            )
        elif self._first_leg_label == "Predict.fun":
            prepared_market = replace(
                prepared_market,
                predict_fun_neg_risk=signal.market.predict_fun_neg_risk,
            )
        if self._second_leg_label == "Polymarket":
            prepared_market = replace(
                prepared_market,
                tick_size=str(second_constraints.tick_size),
                neg_risk=signal.market.neg_risk,
            )
        elif self._second_leg_label == "Predict.fun":
            prepared_market = replace(
                prepared_market,
                predict_fun_neg_risk=signal.market.predict_fun_neg_risk,
            )
        prepared_signal = replace(
            signal,
            market=prepared_market,
            plan=refreshed_plan,
            metrics=refreshed_metrics,
            polymarket_price=float(first_preview.limit_price),
            predict_fun_price=float(second_preview.limit_price),
        )
        evidence = {
            "captured_at": datetime.now(UTC).isoformat(),
            "signed_preview_validated": bool(first_preview.signing_validated and second_preview.signing_validated),
            "first_leg": {
                "venue": self._first_leg_label,
                "token_id": self._first_leg_token_id(signal.market),
                "side": self._first_leg_side(signal.market).value,
                "contracts": str(first_preview.requested_contracts),
                "guaranteed_contracts": str(
                    first_preview.guaranteed_contracts
                    if first_preview.guaranteed_contracts is not None
                    else first_preview.requested_contracts
                ),
                "limit_price": str(first_preview.limit_price),
                "vwap": str(first_preview.average_price),
                "executable_depth_usd": str(first_depth),
                "top_of_book_depth_usd": str(first_top_depth),
                "signed_preview_depth_usd": str(first_preview.available_depth_usd),
                "signed_preview_price_impact_pct": str(first_preview.price_impact_pct),
                "maximum_notional_usd": str(
                    first_preview.maximum_notional_usd
                    if first_preview.maximum_notional_usd is not None
                    else first_preview.requested_contracts * first_preview.limit_price
                ),
                "fee_model": first_preview.fee_quote.model,
                "fee_rate_bps": first_preview.fee_quote.fee_rate_bps,
                "fee_source": first_preview.fee_quote.source,
                "fee_verified": first_preview.fee_quote.verified,
                "payload_fingerprint": first_preview.payload_fingerprint,
            },
            "second_leg": {
                "venue": self._second_leg_label,
                "token_id": self._second_leg_token_id(signal.market),
                "side": self._second_leg_side(signal.market).value,
                "contracts": str(second_preview.requested_contracts),
                "guaranteed_contracts": str(
                    second_preview.guaranteed_contracts
                    if second_preview.guaranteed_contracts is not None
                    else second_preview.requested_contracts
                ),
                "limit_price": str(second_preview.limit_price),
                "vwap": str(second_preview.average_price),
                "executable_depth_usd": str(second_depth),
                "top_of_book_depth_usd": str(second_top_depth),
                "signed_preview_depth_usd": str(second_preview.available_depth_usd),
                "signed_preview_price_impact_pct": str(second_preview.price_impact_pct),
                "maximum_notional_usd": str(
                    second_preview.maximum_notional_usd
                    if second_preview.maximum_notional_usd is not None
                    else second_preview.requested_contracts * second_preview.limit_price
                ),
                "fee_model": second_preview.fee_quote.model,
                "fee_rate_bps": second_preview.fee_quote.fee_rate_bps,
                "fee_source": second_preview.fee_quote.source,
                "fee_verified": second_preview.fee_quote.verified,
                "payload_fingerprint": second_preview.payload_fingerprint,
            },
            "economics": {
                "variable_fee_cost_usd": str(variable_cost),
                "fixed_chain_cost_usd": str(chain_cost_quote.reserved_cost_usd),
                "all_in_cost_usd": str(refreshed_all_in_cost),
                "expected_profit_usd": str(refreshed_metrics.expected_net_profit_usd),
                "minimum_profit_usd": str(minimum_profit),
                "net_edge": str(current_spread),
                "dynamic_threshold": str(dynamic_threshold),
                "required_depth_usd": str(required_depth),
                "target_notional_usd": str(target_notional),
                "adverse_move_reserve": str(adverse_move + self._config.spread_policy.safety_buffer_pct),
                "preflight_latency_seconds": str(preflight_latency),
            },
        }
        return PreparedEntry(
            signal=prepared_signal,
            all_in_cost_usd=refreshed_all_in_cost,
            evidence=evidence,
            first_order_fingerprint=first_preview.payload_fingerprint,
            second_order_fingerprint=second_preview.payload_fingerprint,
        )

    async def _market_constraints_guard(self, signal: ArbitrageSignal) -> bool:
        try:
            first_constraints, second_constraints = await asyncio.gather(
                self._first_leg.get_market_constraints(
                    self._first_leg_token_id(signal.market),
                    signal.market.condition_id if self._first_leg_label == "Polymarket" else None,
                ),
                self._second_leg.get_market_constraints(self._second_leg_token_id(signal.market)),
            )
        except Exception:
            LOGGER.exception("market_constraints_lookup_failed", extra={"_symbol": signal.market.symbol})
            return False
        if first_constraints is None or second_constraints is None:
            LOGGER.error(
                "market_constraints_unknown_live_order_blocked",
                extra={"_symbol": signal.market.symbol},
            )
            return False
        checks = (
            (
                self._first_leg_label,
                first_constraints,
                signal.plan.polymarket_contracts,
                signal.plan.polymarket_capital_usd,
            ),
            (
                self._second_leg_label,
                second_constraints,
                signal.plan.predict_fun_contracts,
                signal.plan.predict_fun_capital_usd,
            ),
        )
        for venue, constraints, quantity, notional in checks:
            if Decimal(str(quantity)) < constraints.lot_size or Decimal(str(notional)) < constraints.minimum_notional:
                LOGGER.warning(
                    "market_constraints_minimum_rejected",
                    extra={"_symbol": signal.market.symbol, "_venue": venue},
                )
                return False
        return True

    async def _cancel_active_orders_and_clear_pending(self) -> None:
        active = list(self._active_orders.items())
        if active:
            await asyncio.gather(
                *(client.cancel_order(key[1]) for key, client in active),
                return_exceptions=True,
            )
        async with self._capacity_lock:
            self._active_orders.clear()
            self._pending_markets.clear()
            self._capital_reservations.clear()
        await self._persist_runtime_balance_state()

    def _log_pipeline_latency(
        self,
        signal: ArbitrageSignal,
        first: EntryLegResult,
        second: EntryLegResult,
        signal_received_ns: int | None,
        reserved_ns: int | None,
    ) -> None:
        submit_times = [value for value in (first.submit_started_ns, second.submit_started_ns) if value is not None]
        if not submit_times:
            return
        first_submit = min(submit_times)
        submit_delta_ns = abs((first.submit_started_ns or first_submit) - (second.submit_started_ns or first_submit))
        extra: dict[str, object] = {
            "_symbol": signal.market.symbol,
            "_entry_submit_delta_us": submit_delta_ns / 1_000.0,
        }
        if signal_received_ns is not None and reserved_ns is not None:
            extra["_signal_to_reservation_us"] = (reserved_ns - signal_received_ns) / 1_000.0
            extra["_reservation_to_submit_us"] = (first_submit - reserved_ns) / 1_000.0
        if first.acknowledged_ns is not None and first.submit_started_ns is not None:
            extra["_first_exchange_ack_us"] = (first.acknowledged_ns - first.submit_started_ns) / 1_000.0
        if second.acknowledged_ns is not None and second.submit_started_ns is not None:
            extra["_second_exchange_ack_us"] = (second.acknowledged_ns - second.submit_started_ns) / 1_000.0
        LOGGER.info("execution_pipeline_latency", extra=extra)

    def _preflight_liquidity_log_extra(
        self,
        signal: ArbitrageSignal,
        first_book: Any,
        second_book: Any,
        *,
        target_notional: float,
        current_spread: float | None = None,
        first_quote: FillQuote | None = None,
        second_quote: FillQuote | None = None,
        reason: str | None = None,
    ) -> dict[str, object]:
        now = time.time()
        first_age = max(0.0, now - first_book.timestamp) if first_book is not None else None
        second_age = max(0.0, now - second_book.timestamp) if second_book is not None else None
        extra: dict[str, object] = {
            "_symbol": signal.market.symbol,
            "_route": route_key(self._first_leg_label, self._second_leg_label),
            "_target_notional_per_leg_usd": target_notional,
            "_current_net_spread": current_spread,
            "_spread_floor": self._config.min_retry_spread_pct,
            "_first_venue": self._first_leg_label,
            "_first_best_ask": first_book.best_ask.price if first_book is not None and first_book.asks else None,
            "_first_avg_fill": first_quote.avg_price if first_quote is not None else None,
            "_first_slippage_pct": first_quote.slippage_pct if first_quote is not None else None,
            "_first_book_age_sec": first_age,
            "_second_venue": self._second_leg_label,
            "_second_best_ask": second_book.best_ask.price if second_book is not None and second_book.asks else None,
            "_second_avg_fill": second_quote.avg_price if second_quote is not None else None,
            "_second_slippage_pct": second_quote.slippage_pct if second_quote is not None else None,
            "_second_book_age_sec": second_age,
            "_max_production_price_impact": self._config.max_production_price_impact,
        }
        if reason is not None:
            extra["_reason"] = reason
        return extra

    def _venue_slippage_cap(self, venue_label: str) -> float:
        if venue_label == "Polymarket":
            configured = self._config.polymarket.max_slippage_pct
        elif venue_label == "Predict.fun":
            configured = self._config.predict_fun.max_slippage_pct
        elif venue_label == "SX Bet":
            configured = self._config.sx_bet.max_slippage_pct
        elif venue_label == "Myriad":
            configured = self._config.myriad_markets.max_slippage_pct
        else:
            raise ValueError(f"Unsupported venue label: {venue_label}")
        return min(configured, self._config.max_production_price_impact)

    def _venue_fee_pct(self, venue_label: str, market: MarketSpec) -> float:
        if venue_label == "Polymarket":
            return self._config.polymarket.trading_fee_pct
        if venue_label == "Predict.fun":
            fee_rate_bps = (
                market.predict_fun_fee_rate_bps
                if market.predict_fun_fee_rate_bps is not None
                else self._config.predict_fun.fee_rate_bps
            )
            return float(Decimal(fee_rate_bps) / Decimal(10_000))
        if venue_label == "SX Bet":
            fee_rate_bps = self._config.sx_bet.taker_fee_bps
            if market.venue_a_label != "Predict.fun" and market.predict_fun_fee_rate_bps is not None:
                fee_rate_bps = market.predict_fun_fee_rate_bps
            return float(Decimal(fee_rate_bps) / Decimal(10_000))
        if venue_label == "Myriad":
            return self._config.myriad_markets.trading_fee_pct
        raise ValueError(f"Unsupported venue label: {venue_label}")

    def _debit_cached_balance(self, venue_label: str, amount_usd: Decimal) -> None:
        self._optimistic_debits[venue_label] = _d(self._optimistic_debits.get(venue_label, ZERO)) + amount_usd

    def _effective_balance(self, venue_label: str) -> Decimal:
        return max(
            ZERO,
            _d(self._balance_cache.get(venue_label, ZERO)) - _d(self._optimistic_debits.get(venue_label, ZERO)),
        )

    def _apply_balance_refresh(self, venue_label: str, fetched_balance: float) -> None:
        previous = self._balance_cache.get(venue_label)
        fetched = _d(fetched_balance)
        if previous is not None and fetched < _d(previous) - EPSILON:
            observed_debit = _d(previous) - fetched
            remaining = max(ZERO, _d(self._optimistic_debits.get(venue_label, ZERO)) - observed_debit)
            if remaining <= EPSILON:
                self._optimistic_debits.pop(venue_label, None)
            else:
                self._optimistic_debits[venue_label] = remaining
        self._balance_cache[venue_label] = fetched

    async def _add_position(self, position: OpenPosition) -> None:
        key = position_key(position.market)
        if self._repository is not None:
            await self._repository.save_position(key, position)
        self._ledger.add(position)

    async def _remove_position(self, key: str) -> None:
        if self._repository is not None:
            await self._repository.remove_position(key)
        self._ledger.remove(key)

    async def _save_unwind_pending(
        self,
        signal: ArbitrageSignal,
        first_order_id: str,
        second_order_id: str,
        matched_amount: Decimal,
        unmatched_first: Decimal,
        unmatched_second: Decimal,
        first_entry_price: Decimal,
        second_entry_price: Decimal,
    ) -> None:
        await self._add_position(
            OpenPosition(
                market=signal.market,
                polymarket_contracts=matched_amount + unmatched_first,
                polymarket_entry_price=first_entry_price,
                predict_fun_contracts=matched_amount + unmatched_second,
                predict_fun_entry_price=second_entry_price,
                opened_at=datetime.now(UTC),
                polymarket_order_id=first_order_id,
                predict_fun_order_id=second_order_id,
                status="unwind_pending",
                polymarket_unwind_attempts=1,
                unmatched_first_contracts=unmatched_first,
                unmatched_second_contracts=unmatched_second,
            )
        )

    async def _save_entry_pending(self, signal: ArbitrageSignal) -> None:
        await self._add_position(
            OpenPosition(
                market=signal.market,
                polymarket_contracts=signal.plan.polymarket_contracts,
                polymarket_entry_price=ZERO,
                predict_fun_contracts=signal.plan.predict_fun_contracts,
                predict_fun_entry_price=ZERO,
                opened_at=datetime.now(UTC),
                polymarket_order_id="pending",
                predict_fun_order_id="pending",
                status="entry_pending",
            )
        )

    async def _save_unpriced_entry_pending(
        self,
        signal: ArbitrageSignal,
        first_order_id: str,
        second_order_id: str,
        first_filled: Decimal,
        second_filled: Decimal,
        first_entry_price: Decimal,
        second_entry_price: Decimal,
    ) -> None:
        await self._add_position(
            OpenPosition(
                market=signal.market,
                polymarket_contracts=first_filled,
                polymarket_entry_price=first_entry_price,
                predict_fun_contracts=second_filled,
                predict_fun_entry_price=second_entry_price,
                opened_at=datetime.now(UTC),
                polymarket_order_id=first_order_id,
                predict_fun_order_id=second_order_id,
                status="entry_pending",
            )
        )

    def _open_position_from_amounts(
        self,
        signal: ArbitrageSignal,
        first_order_id: str,
        second_order_id: str,
        matched_amount: Decimal,
        first_entry_price: Decimal,
        second_entry_price: Decimal,
    ) -> OpenPosition:
        return OpenPosition(
            market=signal.market,
            polymarket_contracts=matched_amount,
            polymarket_entry_price=first_entry_price,
            predict_fun_contracts=matched_amount,
            predict_fun_entry_price=second_entry_price,
            opened_at=datetime.now(UTC),
            polymarket_order_id=first_order_id,
            predict_fun_order_id=second_order_id,
        )

    async def _try_unwind_first_leg(self, signal: ArbitrageSignal, contracts: Decimal | None = None) -> Decimal:
        requested = contracts if contracts is not None else signal.plan.polymarket_contracts
        try:
            book = await self._first_leg.watch_order_book(self._first_leg_token_id(signal.market))
            if not book.bids:
                return ZERO
            target_unwind_price = max(0.01, book.best_bid.price - 0.01)
            result = await self._submit_exit_leg(
                client=self._first_leg,
                market=signal.market,
                venue_label=self._first_leg_label,
                already_closed=False,
                token_id=self._first_leg_token_id(signal.market),
                side=self._first_leg_side(signal.market),
                contracts=requested,
                min_price=_d(target_unwind_price),
                timeout_ms=self._first_leg_fill_timeout_ms,
                condition_id=signal.market.condition_id if self._first_leg_label == "Polymarket" else None,
                tick_size=signal.market.tick_size if self._first_leg_label == "Polymarket" else None,
                neg_risk=signal.market.neg_risk if self._first_leg_label == "Polymarket" else None,
            )
            if result.report is None:
                return ZERO
            unwind_report = result.report
            unwound = min(requested, unwind_report.amount_filled)
            await self._record_unwind_pnl(
                self._first_leg_label,
                signal.market,
                signal.polymarket_price,
                unwind_report.avg_price or target_unwind_price,
                unwound,
            )
            return unwound
        except Exception:
            LOGGER.exception("instant_unwind_failed", extra={"_symbol": signal.market.symbol})
            return ZERO

    async def _try_unwind_second_leg(self, signal: ArbitrageSignal, contracts: Decimal) -> Decimal:
        try:
            book = await self._second_leg.watch_order_book(self._second_leg_token_id(signal.market))
            if not book.bids:
                return ZERO
            target_unwind_price = max(0.01, book.best_bid.price - 0.01)
            result = await self._submit_exit_leg(
                client=self._second_leg,
                market=signal.market,
                venue_label=self._second_leg_label,
                already_closed=False,
                token_id=self._second_leg_token_id(signal.market),
                side=self._second_leg_side(signal.market),
                contracts=contracts,
                min_price=_d(target_unwind_price),
                timeout_ms=self._second_leg_fill_timeout_ms,
                neg_risk=(signal.market.predict_fun_neg_risk if self._second_leg_label == "Predict.fun" else None),
            )
            if result.report is None:
                return ZERO
            unwind_report = result.report
            unwound = min(contracts, unwind_report.amount_filled)
            await self._record_unwind_pnl(
                self._second_leg_label,
                signal.market,
                signal.predict_fun_price,
                unwind_report.avg_price or target_unwind_price,
                unwound,
            )
            return unwound
        except Exception:
            LOGGER.exception("instant_second_leg_unwind_failed", extra={"_symbol": signal.market.symbol})
            return ZERO

    async def _record_unwind_pnl(
        self,
        venue_label: str,
        market: MarketSpec,
        entry_price: Decimal | float,
        exit_price: Decimal | float,
        contracts: Decimal,
    ) -> None:
        if contracts <= 0:
            return
        fee = self._venue_fee_pct(venue_label, market)
        fee_decimal = Decimal(str(fee))
        profit_usd = Decimal(str(contracts)) * (
            Decimal(str(exit_price)) * (Decimal(1) - fee_decimal)
            - Decimal(str(entry_price)) * (Decimal(1) + fee_decimal)
        )
        if profit_usd < 0 and await self._risk.record_realized_result(profit_usd):
            await self._telegram.send_html(
                "🚨 <b>GLOBAL DAILY LOSS HARD STOP</b>\n"
                f"Emergency unwind opened hard stop; realized daily loss: ${self._risk.daily_loss_usd:.2f}."
            )


def _signal_key(signal: ArbitrageSignal) -> str:
    if signal.market.rules_fingerprint:
        return signal.market.rules_fingerprint
    if signal.market.polymarket_token_id and signal.market.predict_fun_token_id:
        return f"{signal.market.polymarket_token_id}:{signal.market.predict_fun_token_id}"
    return f"{signal.market.symbol}:{signal.market.target_label}"


def _worst_case_fee_between(
    quote: VenueFeeQuote,
    contracts: Decimal,
    average_price: Decimal,
    limit_price: Decimal,
) -> Decimal:
    low, high = sorted((average_price, limit_price))
    prices = {low, high}
    if low <= Decimal("0.5") <= high:
        prices.add(Decimal("0.5"))
    return max(quote.fee_for_fill(contracts, price) for price in prices)


def _entry_submission_deadline_unix(market: MarketSpec) -> float | None:
    cutoffs = [value for value in (market.cutoff_at, market.expires_at) if value is not None]
    if not cutoffs:
        return None
    normalized = [value if value.tzinfo is not None else value.replace(tzinfo=UTC) for value in cutoffs]
    return min(value.astimezone(UTC) for value in normalized).timestamp()


def _funded_canary_deadline_unix(config: AppConfig) -> float | None:
    if config.execution_mode is not ExecutionMode.CANARY:
        return None
    deadline_file = os.getenv("FUNDED_CANARY_DEADLINE_FILE")
    raw: str | None
    if deadline_file:
        try:
            raw = Path(deadline_file).read_text(encoding="utf-8").strip()
        except OSError:
            return 0.0
    else:
        raw = os.getenv("FUNDED_CANARY_DEADLINE_UNIX")
    if raw is None or raw == "":
        return 0.0
    try:
        deadline = float(raw)
    except (TypeError, ValueError):
        return 0.0
    return deadline if math.isfinite(deadline) and deadline > 0 else 0.0


def _entry_submission_window_open(
    market: MarketSpec,
    *,
    includes_sx: bool,
    now: datetime,
) -> bool:
    deadline_unix = _entry_submission_deadline_unix(market)
    if deadline_unix is None:
        return not includes_sx
    normalized_now = now if now.tzinfo is not None else now.replace(tzinfo=UTC)
    buffer_seconds = _SX_SUBMISSION_CUTOFF_BUFFER_SECONDS if includes_sx else 0.0
    return deadline_unix > normalized_now.astimezone(UTC).timestamp() + buffer_seconds


def _intent_status_from_report(report: ExecutionReport) -> OrderIntentStatus:
    if report.status is ExecutionStatus.OPEN:
        # MATCHED V3 fills can still fail before funds are irreversibly LOCKED.
        # Keep the intent unresolved even when the venue reports provisional size.
        return OrderIntentStatus.UNKNOWN
    if report.is_filled:
        return OrderIntentStatus.FILLED
    if report.has_fill:
        return OrderIntentStatus.PARTIAL
    if report.status.value in {"CANCELLED", "EXPIRED"}:
        return OrderIntentStatus.CANCELLED
    # An order still reported OPEN after cancel/reconcile has an unresolved
    # outcome. It must not be retried until account-level reconciliation.
    return OrderIntentStatus.UNKNOWN


def _signal_from_unwind_position(position: OpenPosition) -> ArbitrageSignal:
    plan = PositionPlan(
        polymarket_contracts=position.polymarket_contracts,
        polymarket_capital_usd=position.polymarket_contracts * position.polymarket_entry_price,
        predict_fun_contracts=ZERO,
        predict_fun_capital_usd=ZERO,
        payout_contracts=position.polymarket_contracts,
        total_cost_usd=position.polymarket_contracts * position.polymarket_entry_price,
    )
    metrics = SpreadMetrics(0.0, 0.0, 0.0, 0.0, 0.0, float(position.polymarket_entry_price))
    return ArbitrageSignal(
        market=position.market,
        plan=plan,
        metrics=metrics,
        polymarket_price=float(position.polymarket_entry_price),
        predict_fun_price=0.0,
    )


async def _zero_async() -> Decimal:
    return ZERO
