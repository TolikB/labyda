from __future__ import annotations

import argparse
import asyncio
import gc
import logging
import random
import signal
import sys
from collections.abc import Awaitable
from dataclasses import dataclass, replace
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING

from .config import AppConfig, effective_funded_routes, load_config, load_operator_env, validate_config
from .connectors.base import BinaryMarketClient
from .connectors.myriad import MyriadClient
from .connectors.polymarket import PolymarketClobClient
from .connectors.predict_fun import PredictFunApiClient
from .connectors.sx_bet import create_sx_bet_client
from .discovery_cpu import run_discovery_cpu
from .discovery_lifecycle import ActiveMarketRegistry, DiscoveryCoordinator, DiscoveryDiagnostics, DiscoveryResult
from .engine import ArbitrageEngine
from .execution import EntrySubmissionCoordinator, ExecutionRouter
from .logging_config import configure_logging
from .market_discovery import GammaCacheUnavailable, GammaMarketResolver, GammaResolutionStats
from .market_mapping import (
    filter_markets_for_categories,
    filter_markets_for_launch_horizon,
    is_live_mapping_eligible,
)
from .matcher import normalize_text
from .models import (
    ExecutionMode,
    MarketSpec,
    market_supports_execution_route,
    opposite_binary_side,
    route_execution_sides_are_complementary,
)
from .myriad_discovery import MyriadMarketResolver
from .position_manager import PositionManager
from .positions import JsonPositionLedger, PositionLedger
from .predict_fun_discovery import PredictFunMarketResolver
from .risk import GlobalRiskController
from .settlement import SettlementService
from .sx_bet_discovery import SxBetMarketResolver
from .telegram import TelegramNotifier

LOGGER = logging.getLogger(__name__)

_DISCOVERY_RETRY_INITIAL_SECONDS = 5.0
_DISCOVERY_RETRY_MAX_SECONDS = 300.0
_DISCOVERY_RETRY_JITTER = 0.20


@dataclass(frozen=True)
class _DiscoveryCandidateCounts:
    raw: int
    safe: int
    horizon: int
    category: int
    volume: int
    execution_shape_rejected: int

if TYPE_CHECKING:
    from .database import ProductionRepository
    from .reconciliation import ReconciliationService


def _install_shutdown_handlers(
    loop: asyncio.AbstractEventLoop,
    shutdown_event: asyncio.Event,
    *,
    platform: str | None = None,
) -> None:
    def request_shutdown() -> None:
        if not shutdown_event.is_set():
            LOGGER.warning("graceful_shutdown_requested")
            shutdown_event.set()

    if (platform or sys.platform) != "win32":
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, request_shutdown)
        return

    def windows_handler(signum: int, frame: object) -> None:
        del signum, frame
        loop.call_soon_threadsafe(request_shutdown)

    signal.signal(signal.SIGINT, windows_handler)
    signal.signal(signal.SIGTERM, windows_handler)


async def async_main() -> None:
    from .database import ProductionRepository, _active_venues_for_routes
    from .observability import ObservabilityServer
    from .reconciliation import ReconciliationService

    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="config.json")
    parser.add_argument("--once", action="store_true", help="run a single engine cycle and exit")
    parser.add_argument("--resume-risk-only", action="store_true", help="clear the durable global risk pause and exit")
    args = parser.parse_args()

    configure_logging()
    load_operator_env(args.config)
    config = load_config(args.config)
    validate_config(config, require_verified_mappings=False)
    repository: ProductionRepository | None = None
    if config.database_url:
        repository = ProductionRepository(
            config.database_url,
            runtime_instance_id=config.runtime_instance_id,
            enabled_routes=_enabled_routes(config),
        )
        if not await repository.ping():
            await repository.close()
            raise RuntimeError("PostgreSQL is unavailable; execution remains disabled")
        await repository.configure_managed_reconciliation_venues(
            effective_funded_routes(config)
        )
        reconciliation_venues = set(repository.reconciliation_venues)
    else:
        reconciliation_venues = set(
            _active_venues_for_routes(effective_funded_routes(config))
        )
    if config.execution_mode.submits_orders:
        if repository is None:
            raise RuntimeError("PostgreSQL repository is required for canary/live execution")
        if _legacy_state_is_nonempty(Path("data/open_positions.json")):
            await repository.close()
            raise RuntimeError("Legacy JSON state is non-empty; run `arbitrage-admin state import-json` first")
        if not await repository.acquire_trader_lock():
            await repository.close()
            raise RuntimeError("Another production execution process already holds the PostgreSQL trader lock")
        ledger = PositionLedger()
        for position in await repository.load_positions():
            ledger.add(position)
    else:
        ledger = JsonPositionLedger("data/open_positions.json")
    risk_state_path, risk_state_store = _risk_state_backend(repository)
    risk_controller = GlobalRiskController(
        config.max_daily_loss_usd,
        config.max_consecutive_api_errors,
        risk_state_path,
        state_store=risk_state_store,
    )
    await risk_controller.initialize()
    unresolved_entries = [position for position in ledger.all() if position.status == "entry_pending"]
    unresolved_redemptions = await repository.unresolved_redemption_intents() if repository is not None else []
    if args.resume_risk_only:
        blocking_positions = [
            position
            for position in ledger.all()
            if position.status in {"entry_pending", "unwind_pending", "partial_exit_pending", "manual_review"}
        ]
        unresolved_intents = await repository.unresolved_order_intents() if repository is not None else []
        unresolved_redemption_intents = (
            await repository.unresolved_redemption_intents() if repository is not None else []
        )
        reconciliation_failures = await repository.latest_reconciliation_failures() if repository is not None else []
        if blocking_positions or unresolved_intents or unresolved_redemption_intents or reconciliation_failures:
            if repository is not None:
                await repository.close()
            raise RuntimeError(
                "Cannot resume risk: unresolved positions/intents or reconciliation failures require manual review"
            )
        await risk_controller.resume()
        LOGGER.warning("global_risk_pause_cleared_by_operator")
        if repository is not None:
            await repository.close()
        return
    if unresolved_entries:
        await risk_controller.pause(f"{len(unresolved_entries)} unresolved entry intent(s) found after restart")
        LOGGER.critical(
            "startup_paused_unresolved_entry_intents",
            extra={"_count": len(unresolved_entries)},
        )
    if unresolved_redemptions:
        await risk_controller.pause(
            f"{len(unresolved_redemptions)} unresolved redemption intent(s) found after restart"
        )
        LOGGER.critical(
            "startup_paused_unresolved_redemptions",
            extra={"_count": len(unresolved_redemptions)},
        )
    predict_route_enabled = (
        config.routes.polymarket_predict or config.routes.predict_myriad or config.routes.predict_sx
    )
    sx_route_enabled = config.routes.polymarket_sx or config.routes.sx_myriad or config.routes.predict_sx
    myriad_route_enabled = config.routes.polymarket_myriad or config.routes.predict_myriad
    myriad_route_enabled = myriad_route_enabled or config.routes.sx_myriad
    predict_enabled = (
        predict_route_enabled
        and config.enable_predict_fun
        and config.predict_fun.enabled
        and bool(config.predict_fun.api_key)
    )
    sx_enabled = sx_route_enabled and config.enable_sx_bet and config.sx_bet.enabled
    myriad_enabled = myriad_route_enabled and config.myriad_markets.enabled
    if not predict_enabled:
        LOGGER.info("predict_fun_disabled", extra={"_reason": "disabled or PREDICT_FUN_API_KEY is missing"})
    if not sx_enabled:
        LOGGER.info("sx_bet_disabled", extra={"_reason": "disabled or SX routes are inactive"})
    gamma_resolver = GammaMarketResolver(
        scan_all=config.scan_all,
        sports_horizon_hours=config.max_sports_market_horizon_hours,
    )
    myriad_resolver = MyriadMarketResolver(
        config.myriad_markets,
        scan_all=config.scan_all,
        categories_to_scan=config.categories_to_scan,
    )
    predict_resolver = PredictFunMarketResolver(config.predict_fun)
    predict_catalog = PredictFunMarketResolver(
        config.predict_fun,
        scan_all=True,
        categories_to_scan=config.categories_to_scan,
    )
    sx_resolver = SxBetMarketResolver(config.sx_bet)
    sx_catalog = SxBetMarketResolver(
        config.sx_bet,
        scan_all=True,
        categories_to_scan=config.categories_to_scan,
    )
    bootstrap_observability: ObservabilityServer | None = None
    if config.scan_all and not args.once:
        bootstrap_observability = ObservabilityServer(
            config.observability_host,
            config.observability_port,
            config.runtime_instance_id,
            risk_controller,
            {},
            repository=repository,
            discovery_ready=lambda: False,
            max_market_data_age_seconds=config.max_orderbook_age_seconds,
            max_stream_silence_seconds=config.websocket_stale_after_seconds,
            execution_mode=config.execution_mode.value,
        )
        try:
            await bootstrap_observability.start()
        except BaseException:
            if repository is not None:
                await repository.close()
            raise
    discovery_succeeded = False
    try:
        if config.scan_all:
            try:
                initial_discovery = await _resolve_scan_all_snapshot(
                    config,
                    gamma_resolver,
                    myriad_resolver,
                    predict_catalog,
                    sx_catalog,
                    repository,
                    predict_enabled=predict_enabled,
                    sx_enabled=sx_enabled,
                    myriad_enabled=myriad_enabled,
                )
            except Exception as exc:
                LOGGER.exception("initial_discovery_unavailable_starting_not_ready")
                initial_discovery = DiscoveryResult(
                    (),
                    tuple(effective_funded_routes(config)),
                    route_statuses=tuple((route, "failed") for route in _enabled_routes(config)),
                )
                initial_discovery_error: BaseException | None = exc
            else:
                initial_discovery_error = None
            config = replace(config, markets=list(initial_discovery.markets))
        else:
            markets = list(config.markets)
            if any(
                not market.polymarket_token_id or market.polymarket_token_id == "replace-with-token-id"
                for market in markets
            ):
                await gamma_resolver.bootstrap(markets)
            markets = await gamma_resolver.resolve(markets)
            if predict_enabled:
                markets = await predict_resolver.resolve(markets)
            if sx_enabled:
                markets = await sx_resolver.resolve(markets)
            if myriad_enabled:
                markets = await myriad_resolver.resolve(markets)
            candidate_markets = _build_route_market_snapshot(markets)
            markets = filter_markets_for_categories(
                candidate_markets, config.categories_to_scan, config.execution_mode
            )
            config = replace(config, markets=markets)
            if repository is not None:
                await repository.upsert_market_candidates(candidate_markets)
                config = replace(config, markets=await repository.apply_verified_mappings(config.markets))
            if _requires_verified_runtime_mappings(config):
                config = replace(config, markets=_verified_active_markets(config))
            route_statuses = _discovery_route_statuses(config, DiscoveryDiagnostics())
            initial_discovery = DiscoveryResult(
                tuple(config.markets),
                _required_missing_routes(config, route_statuses),
                route_statuses=route_statuses,
            )
            initial_discovery_error = None

        validate_config(config, require_resolved_markets=not config.scan_all)
        if args.once:
            _assert_once_discovery_ready(initial_discovery)
        discovery_succeeded = True
    finally:
        if not discovery_succeeded:
            await asyncio.gather(
                myriad_resolver.close(),
                predict_resolver.close(),
                predict_catalog.close(),
                sx_resolver.close(),
                sx_catalog.close(),
                return_exceptions=True,
            )
            await gamma_resolver.close()
            if bootstrap_observability is not None:
                await bootstrap_observability.close()
            if repository is not None:
                await repository.close()
    if bootstrap_observability is not None:
        await bootstrap_observability.close()
    market_registry = ActiveMarketRegistry(
        initial_discovery.markets,
        missing_routes=initial_discovery.missing_routes,
        diagnostics=initial_discovery.diagnostics,
        route_statuses=initial_discovery.route_statuses,
        max_stale_seconds=config.discovery_max_stale_seconds,
    )
    if initial_discovery_error is not None:
        market_registry.record_failure(initial_discovery_error)
    polymarket = PolymarketClobClient(config.polymarket)
    # Discovery/economic evaluation stays active for every configured route.  The
    # funded allowlist only narrows entry submission and reconciliation scope.
    predict_fun = PredictFunApiClient(config.predict_fun) if predict_enabled else None
    sx_bet = create_sx_bet_client(config.sx_bet) if sx_enabled else None

    def register_second_leg_markets(markets: tuple[MarketSpec, ...]) -> None:
        for market in markets:
            if predict_fun is not None:
                register_market = getattr(predict_fun, "register_market", None)
                if callable(register_market):
                    if market.venue_a_label == "Predict.fun":
                        register_market(
                            market.polymarket_token_id,
                            market.polymarket_market_id,
                            market.polymarket_side,
                            market.predict_fun_fee_rate_bps,
                            market.predict_fun_price_precision,
                        )
                    if market.venue_b_label == "Predict.fun":
                        register_market(
                            market.predict_fun_token_id,
                            market.predict_fun_market_id,
                            market.predict_fun_side,
                            market.predict_fun_fee_rate_bps,
                            market.predict_fun_price_precision,
                        )
            if market.venue_b_label == "SX Bet" and sx_bet is not None:
                register_market = getattr(sx_bet, "register_market", None)
                if callable(register_market):
                    register_market(
                        market.predict_fun_token_id,
                        market.predict_fun_market_id,
                        market.predict_fun_side,
                    )

    register_second_leg_markets(tuple(config.markets))
    discovery_coordinator: DiscoveryCoordinator | None = None
    if config.scan_all and not args.once:

        async def refresh_discovery() -> DiscoveryResult:
            return await _resolve_scan_all_snapshot(
                config,
                gamma_resolver,
                myriad_resolver,
                predict_catalog,
                sx_catalog,
                repository,
                predict_enabled=predict_enabled,
                sx_enabled=sx_enabled,
                myriad_enabled=myriad_enabled,
            )

        discovery_coordinator = DiscoveryCoordinator(
            market_registry,
            refresh_discovery,
            on_publish=register_second_leg_markets,
            refresh_interval_seconds=300.0,
            retry_initial_seconds=_DISCOVERY_RETRY_INITIAL_SECONDS,
            retry_max_seconds=_DISCOVERY_RETRY_MAX_SECONDS,
            jitter=_DISCOVERY_RETRY_JITTER,
        )
    myriad = MyriadClient(config.myriad_markets) if myriad_enabled else None
    telegram = TelegramNotifier(config.telegram)
    if unresolved_entries:
        await telegram.send_html(
            "🚨 <b>STARTUP PAUSED: UNRESOLVED ENTRY INTENT</b>\n"
            f"Count: {len(unresolved_entries)}. Reconcile venue orders before using --resume-risk-only."
        )
    if unresolved_redemptions:
        await telegram.send_html(
            "🚨 <b>STARTUP PAUSED: UNRESOLVED REDEMPTION</b>\n"
            f"Count: {len(unresolved_redemptions)}. Receipt reconciliation and manual risk resume are required."
        )
    market_locks: dict[str, asyncio.Lock] = {}
    capacity_lock = asyncio.Lock()
    entry_submission_coordinator = EntrySubmissionCoordinator()
    pending_markets: set[str] = set()
    balance_cache: dict[str, Decimal | float] = {}
    capital_reservations: dict[str, Decimal | float] = {}
    optimistic_debits: dict[str, Decimal | float] = {}
    execution = (
        ExecutionRouter(
            config,
            polymarket,
            predict_fun,
            telegram,
            ledger,
            market_locks=market_locks,
            capacity_lock=capacity_lock,
            pending_markets=pending_markets,
            balance_cache=balance_cache,
            capital_reservations=capital_reservations,
            optimistic_debits=optimistic_debits,
            state_path="data/state.json",
            risk_controller=risk_controller,
            repository=repository,
            entry_submission_coordinator=entry_submission_coordinator,
        )
        if predict_fun is not None and config.routes.polymarket_predict
        else None
    )
    sx_execution = (
        ExecutionRouter(
            config,
            polymarket,
            sx_bet,
            telegram,
            ledger,
            second_leg_label="SX Bet",
            second_leg_fill_timeout_ms=config.sx_bet_fill_timeout_ms,
            market_locks=market_locks,
            capacity_lock=capacity_lock,
            pending_markets=pending_markets,
            balance_cache=balance_cache,
            capital_reservations=capital_reservations,
            optimistic_debits=optimistic_debits,
            state_path="data/state.json",
            risk_controller=risk_controller,
            repository=repository,
            entry_submission_coordinator=entry_submission_coordinator,
        )
        if sx_bet is not None and config.routes.polymarket_sx
        else None
    )
    myriad_execution = (
        ExecutionRouter(
            config,
            polymarket,
            myriad,
            telegram,
            ledger,
            second_leg_label="Myriad",
            second_leg_fill_timeout_ms=config.myriad_fill_timeout_ms,
            market_locks=market_locks,
            capacity_lock=capacity_lock,
            pending_markets=pending_markets,
            balance_cache=balance_cache,
            capital_reservations=capital_reservations,
            optimistic_debits=optimistic_debits,
            state_path="data/state.json",
            risk_controller=risk_controller,
            repository=repository,
            entry_submission_coordinator=entry_submission_coordinator,
        )
        if myriad is not None and config.routes.polymarket_myriad
        else None
    )
    predict_myriad_execution = None
    if myriad is not None and predict_fun is not None and config.routes.predict_myriad:
        predict_myriad_execution = ExecutionRouter(
            config,
            predict_fun,
            myriad,
            telegram,
            ledger,
            first_leg_label="Predict.fun",
            second_leg_label="Myriad",
            first_leg_fill_timeout_ms=config.predict_fun_fill_timeout_ms,
            second_leg_fill_timeout_ms=config.myriad_fill_timeout_ms,
            market_locks=market_locks,
            capacity_lock=capacity_lock,
            pending_markets=pending_markets,
            balance_cache=balance_cache,
            capital_reservations=capital_reservations,
            optimistic_debits=optimistic_debits,
            state_path="data/state.json",
            risk_controller=risk_controller,
            repository=repository,
            entry_submission_coordinator=entry_submission_coordinator,
        )
    predict_sx_execution = None
    if predict_fun is not None and sx_bet is not None and config.routes.predict_sx:
        predict_sx_execution = ExecutionRouter(
            config,
            predict_fun,
            sx_bet,
            telegram,
            ledger,
            first_leg_label="Predict.fun",
            second_leg_label="SX Bet",
            first_leg_fill_timeout_ms=config.predict_fun_fill_timeout_ms,
            second_leg_fill_timeout_ms=config.sx_bet_fill_timeout_ms,
            market_locks=market_locks,
            capacity_lock=capacity_lock,
            pending_markets=pending_markets,
            balance_cache=balance_cache,
            capital_reservations=capital_reservations,
            optimistic_debits=optimistic_debits,
            state_path="data/state.json",
            risk_controller=risk_controller,
            repository=repository,
            entry_submission_coordinator=entry_submission_coordinator,
        )
    sx_myriad_execution = None
    if myriad is not None and sx_bet is not None and config.routes.sx_myriad:
        sx_myriad_execution = ExecutionRouter(
            config,
            sx_bet,
            myriad,
            telegram,
            ledger,
            first_leg_label="SX Bet",
            second_leg_label="Myriad",
            first_leg_fill_timeout_ms=config.sx_bet_fill_timeout_ms,
            second_leg_fill_timeout_ms=config.myriad_fill_timeout_ms,
            market_locks=market_locks,
            capacity_lock=capacity_lock,
            pending_markets=pending_markets,
            balance_cache=balance_cache,
            capital_reservations=capital_reservations,
            optimistic_debits=optimistic_debits,
            state_path="data/state.json",
            risk_controller=risk_controller,
            repository=repository,
            entry_submission_coordinator=entry_submission_coordinator,
        )
    settlement_clients: dict[str, BinaryMarketClient] = {"Polymarket": polymarket}
    if predict_fun is not None:
        settlement_clients["Predict.fun"] = predict_fun
    if sx_bet is not None:
        settlement_clients["SX Bet"] = sx_bet
    if myriad is not None:
        settlement_clients["Myriad"] = myriad
    for client in settlement_clients.values():
        client.set_market_data_snapshot_interval(config.market_data_snapshot_interval_seconds)
        client.set_market_data_execution_freshness(config.max_orderbook_age_seconds)
    settlement_service = SettlementService(
        ledger,
        settlement_clients,
        risk_controller,
        telegram,
        repository,
    )
    position_manager = PositionManager(
        config=config,
        polymarket=polymarket,
        predict_fun=predict_fun,
        execution=execution,
        sx_bet=sx_bet,
        sx_execution=sx_execution,
        myriad=myriad,
        myriad_execution=myriad_execution,
        predict_myriad_execution=predict_myriad_execution,
        predict_sx_execution=predict_sx_execution,
        sx_myriad_execution=sx_myriad_execution,
        ledger=ledger,
        settlement_service=settlement_service,
    )
    engine = ArbitrageEngine(
        config,
        polymarket,
        predict_fun,
        execution,
        sx_bet=sx_bet,
        sx_execution=sx_execution,
        myriad=myriad,
        myriad_execution=myriad_execution,
        predict_myriad_execution=predict_myriad_execution,
        predict_sx_execution=predict_sx_execution,
        sx_myriad_execution=sx_myriad_execution,
        position_manager=position_manager,
        market_locks=market_locks,
        telegram=telegram,
        market_provider=lambda: market_registry.tradable_snapshot(config.execution_mode),
        market_generation_provider=lambda: market_registry.generation,
    )
    reconciliation: ReconciliationService | None = None
    if config.execution_mode.submits_orders:
        assert repository is not None
        reconciliation_clients: dict[str, BinaryMarketClient] = {}
        if "Polymarket" in reconciliation_venues:
            reconciliation_clients["Polymarket"] = polymarket
        if predict_fun is not None and "Predict.fun" in reconciliation_venues:
            reconciliation_clients["Predict.fun"] = predict_fun
        if sx_bet is not None and "SX Bet" in reconciliation_venues:
            reconciliation_clients["SX Bet"] = sx_bet
        if myriad is not None and "Myriad" in reconciliation_venues:
            reconciliation_clients["Myriad"] = myriad
        reconciliation_service = ReconciliationService(
            repository,
            reconciliation_clients,
            risk_controller,
            orders_interval_seconds=config.reconciliation_orders_interval_seconds,
            full_interval_seconds=config.reconciliation_full_interval_seconds,
        )
        reconciliation = reconciliation_service

        def funded_entry_is_ready() -> bool:
            return (
                reconciliation_service.ready
                and market_registry.ready
                and engine.funded_market_data_ready()
            )

        def funded_entry_snapshot_is_current(discovery_generation: int | None) -> bool:
            return (
                discovery_generation is not None
                and market_registry.ready
                and discovery_generation == market_registry.generation
            )

        engine.set_entry_readiness(funded_entry_is_ready)
        for router in (
            execution,
            sx_execution,
            myriad_execution,
            predict_myriad_execution,
            predict_sx_execution,
            sx_myriad_execution,
        ):
            if router is not None:
                router.set_entry_readiness(funded_entry_is_ready)
                router.set_entry_snapshot_readiness(funded_entry_snapshot_is_current)
        if not await reconciliation.startup_reconcile():
            LOGGER.critical(
                "startup_reconciliation_failed_paused",
                extra={"_error": reconciliation.last_error or "unknown"},
            )
            await telegram.send_html(
                "🚨 <b>STARTUP RECONCILIATION PAUSED</b>\n"
                f"{reconciliation.last_error or 'unknown reconciliation failure'}"
            )
        await reconciliation.start()

        async def reconcile_after_pause() -> None:
            assert reconciliation is not None
            await reconciliation.run_once(full=True)

        risk_controller.register_pause_callback(reconcile_after_pause)

    def runtime_discovery_status() -> dict[str, object]:
        route_statuses = _operational_route_statuses(
            market_registry.route_statuses,
            engine.funded_market_data_route_readiness(),
        )
        return {
            "missing_routes": market_registry.missing_routes,
            "idle_routes": tuple(
                route for route, status in route_statuses if status == "idle_no_verified_overlap"
            ),
            "failed_routes": tuple(route for route, status in route_statuses if status == "failed"),
            "route_statuses": dict(route_statuses),
            "last_error": market_registry.last_error,
            "stale": market_registry.is_stale,
            "diagnostics": market_registry.diagnostics.as_dict(),
        }

    observability = ObservabilityServer(
        config.observability_host,
        config.observability_port,
        config.runtime_instance_id,
        risk_controller,
        settlement_clients,
        repository=repository,
        reconciliation=reconciliation,
        discovery_ready=lambda: market_registry.ready,
        discovery_status=runtime_discovery_status,
        max_market_data_age_seconds=config.max_orderbook_age_seconds,
        max_stream_silence_seconds=config.websocket_stale_after_seconds,
        execution_mode=config.execution_mode.value,
        entry_submission_in_progress=entry_submission_coordinator.entry_lock.locked,
        funded_market_data_targets=engine.funded_market_data_targets,
    )
    await observability.start()
    engine.set_signal_evaluation_observer(observability.record_signal_evaluation)
    engine.set_market_economics_observer(observability.record_market_economics)
    engine.set_calibration_observer(observability.record_route_calibration)
    for router in (
        execution,
        sx_execution,
        myriad_execution,
        predict_myriad_execution,
        predict_sx_execution,
        sx_myriad_execution,
    ):
        if router is not None:
            router.set_preflight_observer(observability.record_market_economics)
            router.set_shadow_preflight_observer(observability.record_shadow_preflight)
            router.set_accepted_preflight_observer(observability.record_accepted_entry_preflight)
    risk_controller.start_external_monitor()
    try:
        if discovery_coordinator is not None:
            discovery_coordinator.start()
        for router in (
            execution,
            sx_execution,
            myriad_execution,
            predict_myriad_execution,
            predict_sx_execution,
            sx_myriad_execution,
        ):
            if router is not None:
                await router.start()
        if args.once:
            await engine.run_once()
        else:
            shutdown_event = asyncio.Event()
            loop = asyncio.get_running_loop()
            _install_shutdown_handlers(loop, shutdown_event)
            await engine.run_forever(shutdown_event=shutdown_event)
    finally:
        LOGGER.info("shutdown_draining_inflight_orders")
        # run_forever drains its active cycle; router teardown cancels any
        # residual tracked venue orders before clients are disconnected.
        if discovery_coordinator is not None:
            await discovery_coordinator.close()
        await observability.close()
        if reconciliation is not None:
            await reconciliation.close()
        for router in (
            execution,
            sx_execution,
            myriad_execution,
            predict_myriad_execution,
            predict_sx_execution,
            sx_myriad_execution,
        ):
            if router is not None:
                await router.close()
        await engine.close()
        await polymarket.close()
        if predict_fun is not None:
            await predict_fun.close()
        if sx_bet is not None:
            await sx_bet.close()
        if myriad is not None:
            await myriad.close()
        await telegram.close()
        await risk_controller.close()
        await asyncio.gather(
            gamma_resolver.close(),
            myriad_resolver.close(),
            predict_resolver.close(),
            predict_catalog.close(),
            sx_resolver.close(),
            sx_catalog.close(),
            return_exceptions=True,
        )
        if repository is not None:
            await repository.close()


def main() -> None:
    try:
        import uvloop  # type: ignore[import-not-found]

        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    except (ImportError, RuntimeError):
        pass
    asyncio.run(async_main())


async def _resolve_scan_all_snapshot(
    config: AppConfig,
    gamma_resolver: GammaMarketResolver,
    myriad_catalog: MyriadMarketResolver,
    predict_catalog: PredictFunMarketResolver,
    sx_catalog: SxBetMarketResolver,
    repository: ProductionRepository | None,
    *,
    predict_enabled: bool,
    sx_enabled: bool,
    myriad_enabled: bool,
) -> DiscoveryResult:
    try:
        return await _resolve_scan_all_snapshot_with_caches(
            config,
            gamma_resolver,
            myriad_catalog,
            predict_catalog,
            sx_catalog,
            repository,
            predict_enabled=predict_enabled,
            sx_enabled=sx_enabled,
            myriad_enabled=myriad_enabled,
        )
    finally:
        # ActiveMarketRegistry owns the compact published snapshot. Retaining the
        # full venue payloads and Gamma indexes between refreshes only raises the
        # next discovery peak and can OOM the long-running service. Limit the
        # explicit sweep to generation zero: a full-heap collection is a
        # stop-the-world latency spike for funded market-data refreshes.
        gamma_resolver.invalidate_cache()
        myriad_catalog.invalidate_cache()
        predict_catalog.invalidate_cache()
        sx_catalog.invalidate_cache()
        gc.collect(0)


async def _resolve_scan_all_snapshot_with_caches(
    config: AppConfig,
    gamma_resolver: GammaMarketResolver,
    myriad_catalog: MyriadMarketResolver,
    predict_catalog: PredictFunMarketResolver,
    sx_catalog: SxBetMarketResolver,
    repository: ProductionRepository | None,
    *,
    predict_enabled: bool,
    sx_enabled: bool,
    myriad_enabled: bool,
) -> DiscoveryResult:
    myriad_catalog.invalidate_cache()
    predict_catalog.invalidate_cache()
    sx_catalog.invalidate_cache()
    catalog_calls: list[tuple[str, Awaitable[list[MarketSpec]]]] = []
    if myriad_enabled:
        catalog_calls.append(("Myriad", myriad_catalog.resolve([])))
    if predict_enabled:
        catalog_calls.append(("Predict.fun", predict_catalog.resolve([])))
    if sx_enabled:
        catalog_calls.append(("SX Bet", sx_catalog.resolve([])))
    results = await asyncio.gather(*(call for _, call in catalog_calls), return_exceptions=True)
    markets: list[MarketSpec] = []
    available: set[str] = set()
    result: list[MarketSpec] | BaseException | None = None
    for (venue, _), result in zip(catalog_calls, results, strict=True):
        if isinstance(result, BaseException):
            LOGGER.error(
                "venue_catalog_unavailable",
                extra={
                    "_venue": venue,
                    "_authentication_failure": venue == "Predict.fun" and _is_predict_auth_failure(result),
                    "_error": str(result),
                },
            )
            continue
        available.add(venue)
        markets.extend(result)
    # Do not let completed coroutine/result containers retain the original
    # source catalogs after Gamma replaces them with the matched subset.
    result = None
    results.clear()
    catalog_calls.clear()

    # Predict scan_all already returned collision-safe, fully enriched
    # MarketSpecs. Its raw JSON catalog is not needed again in this cycle and
    # must not overlap the large Gamma snapshot. SX keeps only a compact parsed
    # catalog for later cross-venue enrichment.
    if predict_enabled:
        predict_catalog.invalidate_cache()
        gc.collect(0)

    try:
        await gamma_resolver.bootstrap(markets)
    except GammaCacheUnavailable:
        # refresh() marks the previous immutable snapshot usable for at most
        # 15 minutes. resolve() succeeds only while that fallback is valid.
        LOGGER.warning("polymarket_catalog_refresh_using_stale_snapshot")
    markets = await gamma_resolver.resolve(markets)
    gamma_stats = gamma_resolver.last_resolution_stats
    gamma_catalog_size = gamma_resolver.catalog_size
    gamma_resolver.invalidate_cache()
    gc.collect(0)
    if "SX Bet" in available:
        markets = await sx_catalog.resolve(markets)
        sx_catalog.invalidate_cache()
        gc.collect(0)
    if "Myriad" in available:
        markets = await myriad_catalog.resolve(markets)
        myriad_catalog.invalidate_cache()
        gc.collect(0)

    enabled_routes = _enabled_routes(config)
    active, persistence_candidates, candidate_counts = await run_discovery_cpu(
        _prepare_discovery_candidate_batch,
        markets,
        config,
        enabled_routes,
    )
    if repository is not None:
        await repository.upsert_market_candidates(persistence_candidates)
        del persistence_candidates
        active = await repository.apply_verified_mappings(active)
    else:
        del persistence_candidates
    myriad_raw, myriad_parsed = myriad_catalog.last_catalog_counts
    predict_raw, predict_parsed = predict_catalog.last_catalog_counts
    sx_raw, sx_parsed = sx_catalog.last_catalog_counts
    discovery_result = await run_discovery_cpu(
        _finalize_discovery_result,
        config,
        active,
        enabled_routes,
        candidate_counts,
        frozenset(available),
        gamma_stats,
        gamma_catalog_size,
        (myriad_raw, myriad_parsed),
        (predict_raw, predict_parsed),
        (sx_raw, sx_parsed),
    )
    diagnostic_payload = discovery_result.diagnostics.as_dict()
    LOGGER.info(
        "discovery_pipeline_summary",
        extra={
            "_stages": diagnostic_payload["stages"],
            "_rejection_reasons": diagnostic_payload["rejection_reasons"],
            "_missing_routes": discovery_result.missing_routes,
        },
    )
    return discovery_result


def _prepare_discovery_candidate_batch(
    markets: list[MarketSpec],
    config: AppConfig,
    enabled_routes: tuple[str, ...],
) -> tuple[list[MarketSpec], list[MarketSpec], _DiscoveryCandidateCounts]:
    """Apply the full-catalog filters outside the latency-sensitive event loop."""
    raw_candidates = _build_route_market_snapshot(markets)
    candidates = _execution_safe_route_candidates(raw_candidates, enabled_routes)
    persistence_candidates = _route_scoped_persistence_candidates(raw_candidates, enabled_routes)
    execution_shape_rejected = _execution_unsafe_route_count(raw_candidates, enabled_routes)
    horizon_active = (
        filter_markets_for_launch_horizon(
            candidates,
            config.categories_to_scan,
            sports_horizon_hours=config.max_sports_market_horizon_hours,
            crypto_horizon_hours=config.max_crypto_market_horizon_hours,
            category_horizon_hours=config.max_market_horizon_hours_by_category,
        )
        if config.market_horizon_filter_enabled
        else candidates
    )
    category_active = filter_markets_for_categories(
        horizon_active,
        config.categories_to_scan,
        config.execution_mode,
    )
    active = _filter_markets_by_volume(category_active, config)
    counts = _DiscoveryCandidateCounts(
        raw=len(raw_candidates),
        safe=len(candidates),
        horizon=len(horizon_active),
        category=len(category_active),
        volume=len(active),
        execution_shape_rejected=execution_shape_rejected,
    )
    return active, persistence_candidates, counts


def _finalize_discovery_result(
    config: AppConfig,
    active: list[MarketSpec],
    enabled_routes: tuple[str, ...],
    counts: _DiscoveryCandidateCounts,
    available: frozenset[str],
    gamma_stats: GammaResolutionStats,
    gamma_catalog_size: int,
    myriad_counts: tuple[int, int],
    predict_counts: tuple[int, int],
    sx_counts: tuple[int, int],
) -> DiscoveryResult:
    """Build the immutable published snapshot outside the market-data event loop."""
    verified_count = sum(
        any(
            _market_supports_route(market, route, require_verified=True)
            and route_execution_sides_are_complementary(market, route)
            and is_live_mapping_eligible(market, ExecutionMode.CANARY, route)
            for route in enabled_routes
        )
        for market in active
    )
    snapshot_config = replace(config, markets=active)
    if _requires_verified_runtime_mappings(config):
        active = _verified_active_markets(snapshot_config)
        snapshot_config = replace(snapshot_config, markets=active)

    myriad_raw, myriad_parsed = myriad_counts
    predict_raw, predict_parsed = predict_counts
    sx_raw, sx_parsed = sx_counts
    stages = {
        "myriad_catalog_available": int("Myriad" in available),
        "myriad_catalog_raw": myriad_raw,
        "myriad_catalog_parsed": myriad_parsed,
        "predict_catalog_available": int("Predict.fun" in available),
        "predict_catalog_raw": predict_raw,
        "predict_catalog_parsed": predict_parsed,
        "sx_catalog_available": int("SX Bet" in available),
        "sx_catalog_raw": sx_raw,
        "sx_catalog_parsed": sx_parsed,
        "seed_catalog": myriad_parsed + predict_parsed + sx_parsed,
        "polymarket_catalog": gamma_catalog_size,
        "exact_id_matches": gamma_stats.exact_id_matches,
        "exact_title_matches": gamma_stats.exact_title_matches,
        "structured_sports_matches": gamma_stats.structured_sports_matches,
        "semantic_matches": gamma_stats.semantic_matches,
        "raw_cross_venue_candidates": counts.raw,
        "cross_venue_candidates": counts.safe,
        "horizon_accepted": counts.horizon,
        "category_accepted": counts.category,
        "volume_accepted": counts.volume,
        "verified_mapping_markets": verified_count,
        "tradable": len(active),
    }
    rejection_reasons = dict(gamma_stats.rejection_reasons)
    rejection_reasons["execution_shape_rejected"] = counts.execution_shape_rejected
    rejection_reasons["horizon_rejected"] = max(0, counts.safe - counts.horizon)
    rejection_reasons["category_rejected"] = max(0, counts.horizon - counts.category)
    rejection_reasons["volume_rejected"] = max(0, counts.category - counts.volume)
    diagnostics = DiscoveryDiagnostics(
        stages=tuple(stages.items()),
        rejection_reasons=tuple((key, value) for key, value in sorted(rejection_reasons.items()) if value),
    )
    route_statuses = _discovery_route_statuses(snapshot_config, diagnostics)
    missing_routes = _required_missing_routes(snapshot_config, route_statuses)
    return DiscoveryResult(tuple(active), missing_routes, diagnostics, route_statuses)


def _assert_once_discovery_ready(result: DiscoveryResult) -> None:
    if result.markets and not result.missing_routes:
        return
    diagnostics = result.diagnostics.as_dict()
    raise RuntimeError(
        "One-shot discovery produced no complete tradable route set: "
        f"markets={len(result.markets)} missing_routes={list(result.missing_routes)} diagnostics={diagnostics}"
    )


def _verified_active_markets(
    config: AppConfig,
    routes: tuple[str, ...] | None = None,
) -> list[MarketSpec]:
    required_routes = routes or _enabled_routes(config)
    return [
        market
        for market in config.markets
        if any(
            _market_supports_route(market, route, require_verified=True)
            and route_execution_sides_are_complementary(market, route)
            and is_live_mapping_eligible(market, ExecutionMode.CANARY, route)
            for route in required_routes
        )
    ]


def _requires_verified_runtime_mappings(config: AppConfig) -> bool:
    return config.execution_mode.submits_orders or (
        config.execution_mode is ExecutionMode.SHADOW
        and config.shadow_require_verified_mappings
    )


def _missing_discovery_routes(
    config: AppConfig,
    routes: tuple[str, ...] | None = None,
) -> list[str]:
    require_verified = _requires_verified_runtime_mappings(config)
    return [
        route
        for route in (routes or effective_funded_routes(config))
        if not any(
            _market_supports_route(market, route, require_verified=require_verified)
            and route_execution_sides_are_complementary(market, route)
            and (not require_verified or is_live_mapping_eligible(market, ExecutionMode.CANARY, route))
            for market in config.markets
        )
    ]


def _enabled_routes(config: AppConfig) -> tuple[str, ...]:
    return config.routes.enabled_names()


def _required_missing_routes(
    config: AppConfig,
    route_statuses: tuple[tuple[str, str], ...],
) -> tuple[str, ...]:
    statuses = dict(route_statuses)
    return tuple(
        route
        for route in effective_funded_routes(config)
        if statuses.get(route) != "ready_verified"
    )


def _discovery_route_statuses(
    config: AppConfig,
    diagnostics: DiscoveryDiagnostics,
    routes: tuple[str, ...] | None = None,
) -> tuple[tuple[str, str], ...]:
    configured_routes = routes or _enabled_routes(config)
    missing = set(_missing_discovery_routes(config, configured_routes))
    return tuple(
        (
            route,
            "ready_verified"
            if route not in missing
            else "failed"
            if _route_catalog_failed(route, diagnostics)
            else "idle_no_verified_overlap",
        )
        for route in configured_routes
    )


def _operational_route_statuses(
    route_statuses: tuple[tuple[str, str], ...],
    funded_market_data_readiness: dict[str, bool],
) -> tuple[tuple[str, str], ...]:
    """Overlay current funded data health without penalizing discovery-only routes."""
    return tuple(
        (
            route,
            "failed"
            if status == "ready_verified" and funded_market_data_readiness.get(route) is False
            else status,
        )
        for route, status in route_statuses
    )


def _route_catalog_failed(route: str, diagnostics: DiscoveryDiagnostics) -> bool:
    stages = diagnostics.as_dict().get("stages", {})
    if not stages:
        return False
    route_venues = {
        "polymarket_myriad": ("Polymarket", "Myriad"),
        "polymarket_predict": ("Polymarket", "Predict.fun"),
        "predict_myriad": ("Predict.fun", "Myriad"),
        "predict_sx": ("Predict.fun", "SX Bet"),
        "polymarket_sx": ("Polymarket", "SX Bet"),
        "sx_myriad": ("SX Bet", "Myriad"),
    }.get(route, ())
    for venue in route_venues:
        if venue == "Polymarket":
            if "polymarket_catalog" in stages and stages.get("polymarket_catalog", 0) <= 0:
                return True
            continue
        prefix = {"Predict.fun": "predict", "SX Bet": "sx", "Myriad": "myriad"}[venue]
        available_key = f"{prefix}_catalog_available"
        if available_key in stages and stages.get(available_key, 0) <= 0:
            return True
        raw = stages.get(f"{prefix}_catalog_raw", 0)
        parsed = stages.get(f"{prefix}_catalog_parsed", 0)
        if raw > 0 and parsed <= 0:
            return True
    return False


def _risk_state_backend(
    repository: ProductionRepository | None,
) -> tuple[str | None, ProductionRepository | None]:
    if repository is not None:
        return None, repository
    return "data/state.json", None


def _market_supports_route(
    market: MarketSpec,
    route: str,
    *,
    require_verified: bool,
) -> bool:
    if require_verified and route not in market.verified_routes:
        return False
    return market_supports_execution_route(market, route)


def _execution_safe_route_candidates(
    markets: list[MarketSpec],
    routes: tuple[str, ...],
) -> list[MarketSpec]:
    safe_markets: list[MarketSpec] = []
    for market in markets:
        supported_routes = [
            route
            for route in routes
            if _market_supports_route(market, route, require_verified=False)
        ]
        if supported_routes and any(
            route_execution_sides_are_complementary(market, route)
            for route in supported_routes
        ):
            safe_markets.append(market)
    return safe_markets


def _execution_unsafe_route_count(
    markets: list[MarketSpec],
    routes: tuple[str, ...],
) -> int:
    return sum(
        1
        for market in markets
        for route in routes
        if _market_supports_route(market, route, require_verified=False)
        and not route_execution_sides_are_complementary(market, route)
    )


def _route_scoped_persistence_candidates(
    markets: list[MarketSpec],
    routes: tuple[str, ...],
) -> list[MarketSpec]:
    projections: list[MarketSpec] = []
    for market in markets:
        for route in routes:
            if not _market_supports_route(market, route, require_verified=False):
                continue
            if not route_execution_sides_are_complementary(market, route):
                continue
            verified_routes = frozenset({route}) if route in market.verified_routes else frozenset()
            if route in {"polymarket_predict", "polymarket_sx", "predict_sx"}:
                projections.append(
                    replace(
                        market,
                        myriad_market_id=None,
                        myriad_condition_id=None,
                        myriad_collateral_token=None,
                        myriad_url=None,
                        myriad_volume_usd=None,
                        verified_routes=verified_routes,
                    )
                )
                continue
            first_label = "Polymarket"
            first_token = market.polymarket_token_id
            first_side = market.polymarket_side
            first_market_id = market.polymarket_market_id
            if route == "predict_myriad":
                first_label = "Predict.fun"
                first_token = market.predict_fun_token_id
                first_side = market.predict_fun_side
                first_market_id = market.predict_fun_market_id
            elif route == "sx_myriad":
                first_label = "SX Bet"
                first_token = market.predict_fun_token_id
                first_side = market.predict_fun_side
                first_market_id = market.predict_fun_market_id
            projections.append(
                replace(
                    market,
                    venue_a_label=first_label,
                    venue_b_label="Myriad",
                    polymarket_token_id=first_token,
                    polymarket_side=first_side,
                    polymarket_market_id=first_market_id,
                    condition_id=market.condition_id if route == "polymarket_myriad" else None,
                    predict_fun_token_id="",
                    predict_fun_market_id=None,
                    predict_fun_amm_pool=None,
                    verified_routes=verified_routes,
                )
            )
    return _deduplicate_route_markets(projections)


def _should_retry_discovery(config: AppConfig, once: bool, missing_routes: list[str]) -> bool:
    return config.scan_all and not once and bool(missing_routes)


def _next_discovery_retry_delay(current: float) -> float:
    if current < 40.0:
        return min(current * 2.0, 40.0)
    if current < 60.0:
        return 60.0
    return min(current * 2.0, _DISCOVERY_RETRY_MAX_SECONDS)


def _jittered_retry_delay(base: float, random_value: float | None = None) -> float:
    sample = random.random() if random_value is None else random_value
    if not 0.0 <= sample <= 1.0:
        raise ValueError("random_value must be between 0 and 1")
    return base * (1.0 - _DISCOVERY_RETRY_JITTER + 2.0 * _DISCOVERY_RETRY_JITTER * sample)


def _filter_markets_by_volume(markets: list[MarketSpec], config: AppConfig) -> list[MarketSpec]:
    threshold = config.min_market_volume_usd
    filtered = [market for market in markets if _volume_filter_accepts(market, threshold)]
    LOGGER.info(
        "market_volume_filter_applied",
        extra={"_before": len(markets), "_after": len(filtered), "_minimum_volume_usd": threshold},
    )
    return filtered


def _maximum_market_volume(market: MarketSpec) -> float:
    volumes = (market.polymarket_volume_usd, market.second_leg_volume_usd, market.myriad_volume_usd)
    return max((volume for volume in volumes if volume is not None), default=0.0)


def _volume_filter_accepts(market: MarketSpec, threshold: float) -> bool:
    if _maximum_market_volume(market) >= threshold:
        return True
    if market.venue_b_label != "SX Bet":
        return False
    # Polymarket sampling-markets currently omits volume metadata on the route-discovery feed,
    # so SX scan_all must not fail closed purely because both venues report unknown volume.
    known_volumes = (
        market.polymarket_volume_usd,
        market.predict_fun_volume_usd,
        market.myriad_volume_usd,
    )
    if any(volume is not None for volume in known_volumes):
        return False
    return bool(market.polymarket_token_id and market.predict_fun_token_id)


def _build_route_market_snapshot(markets: list[MarketSpec]) -> list[MarketSpec]:
    polymarket_family = [market for market in markets if market.venue_a_label == "Polymarket"]
    passthrough = [market for market in markets if market.venue_a_label != "Polymarket"]
    predict_family = _deduplicate_markets(
        [market for market in polymarket_family if market.venue_b_label == "Predict.fun"]
    )
    sx_family = _deduplicate_markets(
        [market for market in polymarket_family if market.venue_b_label == "SX Bet"]
    )
    myriad_family = _deduplicate_markets(
        [market for market in polymarket_family if market.venue_b_label == "Myriad"]
    )
    predict_sx = _synthesize_predict_sx_markets(predict_family, sx_family)
    return _deduplicate_route_markets(
        [*passthrough, *predict_family, *sx_family, *myriad_family, *predict_sx]
    )


def _synthesize_predict_sx_markets(
    predict_family: list[MarketSpec],
    sx_family: list[MarketSpec],
) -> list[MarketSpec]:
    sx_by_key: dict[tuple[str, str], list[MarketSpec]] = {}
    for market in sx_family:
        if market.venue_b_label != "SX Bet" or not market.predict_fun_token_id:
            continue
        match_key = _cross_route_match_key(market)
        if match_key is None:
            continue
        sx_by_key.setdefault((match_key, market.predict_fun_side.value), []).append(market)

    synthesized: list[MarketSpec] = []
    for predict_market in predict_family:
        if predict_market.venue_b_label != "Predict.fun" or not predict_market.predict_fun_token_id:
            continue
        match_key = _cross_route_match_key(predict_market)
        if match_key is None:
            continue
        desired_side = opposite_binary_side(predict_market.predict_fun_side).value
        matches = sx_by_key.get((match_key, desired_side), [])
        if not matches:
            continue
        if len(matches) > 1:
            LOGGER.error(
                "ambiguous_predict_sx_route_rejected",
                extra={"_symbol": predict_market.symbol, "_match_key": match_key},
            )
            continue
        sx_market = matches[0]
        synthesized.append(
            replace(
                predict_market,
                target_label=sx_market.target_label or predict_market.target_label,
                polymarket_token_id=predict_market.predict_fun_token_id,
                polymarket_side=predict_market.predict_fun_side,
                venue_a_label="Predict.fun",
                venue_b_label="SX Bet",
                condition_id=None,
                polymarket_market_id=predict_market.predict_fun_market_id,
                polymarket_url=predict_market.predict_fun_url,
                tick_size=None,
                neg_risk=predict_market.predict_fun_neg_risk,
                predict_fun_token_id=sx_market.predict_fun_token_id,
                predict_fun_side=sx_market.predict_fun_side,
                predict_fun_neg_risk=sx_market.predict_fun_neg_risk,
                predict_fun_fee_rate_bps=predict_market.predict_fun_fee_rate_bps,
                predict_fun_price_precision=predict_market.predict_fun_price_precision,
                predict_fun_market_id=sx_market.predict_fun_market_id,
                predict_fun_url=sx_market.predict_fun_url,
                predict_fun_amm_pool=predict_market.predict_fun_amm_pool,
                myriad_market_id=None,
                myriad_url=None,
                polymarket_volume_usd=predict_market.predict_fun_volume_usd,
                predict_fun_volume_usd=sx_market.predict_fun_volume_usd,
                myriad_volume_usd=None,
                verified_routes=frozenset(
                    route for route in predict_market.verified_routes if route == "predict_sx"
                ),
            )
        )
    return synthesized


def _cross_route_match_key(market: MarketSpec) -> str | None:
    if market.polymarket_market_id:
        return market.polymarket_market_id
    if market.condition_id:
        return market.condition_id
    expiry = market.expires_at.isoformat() if market.expires_at else ""
    title = normalize_text(market.symbol)
    return f"{title}:{expiry}" if title else None


def _deduplicate_route_markets(markets: list[MarketSpec]) -> list[MarketSpec]:
    result: dict[str, MarketSpec] = {}
    for market in markets:
        expiry = market.expires_at.isoformat() if market.expires_at else ""
        route = _route_identity(market)
        key = ":".join(
            (
                route,
                market.venue_a_label,
                market.venue_b_label,
                market.polymarket_token_id or "",
                market.predict_fun_token_id or "",
                market.myriad_market_id or "",
                market.polymarket_market_id or market.condition_id or "",
                market.predict_fun_market_id or "",
                expiry,
            )
        )
        if key not in result:
            result[key] = market
    return list(result.values())


def _route_identity(market: MarketSpec) -> str:
    if market.venue_a_label == "Predict.fun" and market.venue_b_label == "SX Bet":
        return "predict_sx"
    if market.venue_a_label == "Predict.fun" and market.venue_b_label == "Myriad":
        return "predict_myriad"
    if market.venue_a_label == "SX Bet" and market.venue_b_label == "Myriad":
        return "sx_myriad"
    if market.venue_b_label == "Predict.fun":
        return "polymarket_predict"
    if market.venue_b_label == "SX Bet":
        return "polymarket_sx"
    return "polymarket_myriad"


def _deduplicate_markets(markets: list[MarketSpec]) -> list[MarketSpec]:
    merged: dict[str, MarketSpec] = {}
    ambiguous: set[str] = set()
    for market in markets:
        expiry = market.expires_at.isoformat() if market.expires_at else ""
        if market.polymarket_token_id:
            key = market.polymarket_token_id
        else:
            unresolved_identity = (
                market.predict_fun_market_id
                or market.predict_fun_token_id
                or market.myriad_market_id
                or market.condition_id
                or ""
            )
            key = (
                f"{normalize_text(market.symbol)}:"
                f"{normalize_text(market.target_label or market.symbol)}:"
                f"{unresolved_identity}:{expiry}"
            )
        existing = merged.get(key)
        if existing is None:
            merged[key] = market
            continue
        predict_conflict = (
            existing.predict_fun_token_id
            and market.predict_fun_token_id
            and (
                existing.predict_fun_token_id != market.predict_fun_token_id
                or existing.venue_b_label != market.venue_b_label
            )
        )
        myriad_conflict = (
            existing.myriad_market_id
            and market.myriad_market_id
            and existing.myriad_market_id != market.myriad_market_id
        )
        if predict_conflict or myriad_conflict:
            ambiguous.add(key)
            LOGGER.warning(
                "ambiguous_cross_venue_mapping_rejected",
                extra={"_symbol": market.symbol, "_key": key},
            )
            continue
        merged[key] = replace(
            existing,
            predict_fun_token_id=existing.predict_fun_token_id or market.predict_fun_token_id,
            predict_fun_side=existing.predict_fun_side if existing.predict_fun_token_id else market.predict_fun_side,
            venue_b_label=existing.venue_b_label if existing.predict_fun_token_id else market.venue_b_label,
            predict_fun_market_id=existing.predict_fun_market_id or market.predict_fun_market_id,
            predict_fun_url=existing.predict_fun_url or market.predict_fun_url,
            predict_fun_neg_risk=(
                existing.predict_fun_neg_risk
                if existing.predict_fun_neg_risk is not None
                else market.predict_fun_neg_risk
            ),
            predict_fun_fee_rate_bps=(
                existing.predict_fun_fee_rate_bps
                if existing.predict_fun_fee_rate_bps is not None
                else market.predict_fun_fee_rate_bps
            ),
            predict_fun_price_precision=(
                existing.predict_fun_price_precision
                if existing.predict_fun_price_precision is not None
                else market.predict_fun_price_precision
            ),
            myriad_market_id=existing.myriad_market_id or market.myriad_market_id,
            myriad_condition_id=existing.myriad_condition_id or market.myriad_condition_id,
            myriad_collateral_token=existing.myriad_collateral_token or market.myriad_collateral_token,
            myriad_url=existing.myriad_url or market.myriad_url,
            myriad_side=existing.myriad_side if existing.myriad_market_id else market.myriad_side,
            polymarket_url=existing.polymarket_url or market.polymarket_url,
            polymarket_volume_usd=max(
                (
                    value
                    for value in (existing.polymarket_volume_usd, market.polymarket_volume_usd)
                    if value is not None
                ),
                default=None,
            ),
            predict_fun_volume_usd=max(
                (
                    value
                    for value in (existing.predict_fun_volume_usd, market.predict_fun_volume_usd)
                    if value is not None
                ),
                default=None,
            ),
            myriad_volume_usd=max(
                (value for value in (existing.myriad_volume_usd, market.myriad_volume_usd) if value is not None),
                default=None,
            ),
        )
    return [market for key, market in merged.items() if key not in ambiguous]


def _legacy_state_is_nonempty(path: Path) -> bool:
    if not path.exists():
        return False
    try:
        import json

        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return True
    return bool(payload)


def _is_predict_auth_failure(exc: BaseException) -> bool:
    return getattr(exc, "status", None) == 401 or "401" in str(exc)


if __name__ == "__main__":
    main()
