from __future__ import annotations

import argparse
import asyncio
import logging
import random
from collections.abc import Awaitable
from dataclasses import replace
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING

from .config import AppConfig, load_config, load_operator_env, validate_config
from .connectors.base import BinaryMarketClient
from .connectors.myriad import MyriadClient
from .connectors.polymarket import PolymarketClobClient
from .connectors.predict_fun import PredictFunApiClient
from .connectors.sx_bet import SxBetApiClient
from .discovery_lifecycle import ActiveMarketRegistry, DiscoveryCoordinator, DiscoveryDiagnostics, DiscoveryResult
from .engine import ArbitrageEngine
from .execution import ExecutionRouter
from .logging_config import configure_logging
from .market_discovery import GammaCacheUnavailable, GammaMarketResolver
from .market_mapping import (
    filter_markets_for_categories,
    filter_markets_for_launch_horizon,
    is_live_mapping_eligible,
)
from .matcher import normalize_text
from .models import MarketSpec, opposite_binary_side
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

if TYPE_CHECKING:
    from .database import ProductionRepository
    from .reconciliation import ReconciliationService


async def async_main() -> None:
    from .database import ProductionRepository
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
    gamma_resolver = GammaMarketResolver(scan_all=config.scan_all)
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
                initial_discovery = DiscoveryResult((), tuple(_enabled_routes(config)))
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
            if config.execution_mode.submits_orders:
                config = replace(config, markets=_verified_active_markets(config))
            initial_discovery = DiscoveryResult(tuple(config.markets), tuple(_missing_discovery_routes(config)))
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
        max_stale_seconds=900.0,
    )
    if initial_discovery_error is not None:
        market_registry.record_failure(initial_discovery_error)
    polymarket = PolymarketClobClient(config.polymarket)
    predict_fun = PredictFunApiClient(config.predict_fun) if predict_enabled else None
    sx_bet = SxBetApiClient(config.sx_bet) if sx_enabled else None

    def register_second_leg_markets(markets: tuple[MarketSpec, ...]) -> None:
        for market in markets:
            if market.venue_b_label == "Predict.fun" and predict_fun is not None:
                register_market = getattr(predict_fun, "register_market", None)
                if callable(register_market):
                    register_market(
                        market.predict_fun_token_id,
                        market.predict_fun_market_id,
                        market.predict_fun_side,
                        market.predict_fun_fee_rate_bps,
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
    )
    reconciliation: ReconciliationService | None = None
    if config.execution_mode.submits_orders:
        assert repository is not None
        reconciliation_clients: dict[str, BinaryMarketClient] = {"Polymarket": polymarket}
        if predict_fun is not None:
            reconciliation_clients["Predict.fun"] = predict_fun
        if sx_bet is not None:
            reconciliation_clients["SX Bet"] = sx_bet
        if myriad is not None:
            reconciliation_clients["Myriad"] = myriad
        reconciliation = ReconciliationService(
            repository,
            reconciliation_clients,
            risk_controller,
            orders_interval_seconds=config.reconciliation_orders_interval_seconds,
            full_interval_seconds=config.reconciliation_full_interval_seconds,
        )
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
    observability = ObservabilityServer(
        config.observability_host,
        config.observability_port,
        config.runtime_instance_id,
        risk_controller,
        settlement_clients,
        repository=repository,
        reconciliation=reconciliation,
        discovery_ready=lambda: market_registry.ready,
        discovery_status=lambda: {
            "missing_routes": market_registry.missing_routes,
            "last_error": market_registry.last_error,
            "stale": market_registry.is_stale,
            "diagnostics": market_registry.diagnostics.as_dict(),
        },
        max_market_data_age_seconds=config.max_orderbook_age_seconds,
        max_stream_silence_seconds=config.websocket_stale_after_seconds,
        execution_mode=config.execution_mode.value,
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
            await engine.run_forever()
    finally:
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

    try:
        await gamma_resolver.bootstrap(markets)
    except GammaCacheUnavailable:
        # refresh() marks the previous immutable snapshot usable for at most
        # 15 minutes. resolve() succeeds only while that fallback is valid.
        LOGGER.warning("polymarket_catalog_refresh_using_stale_snapshot")
    markets = await gamma_resolver.resolve(markets)
    if "Predict.fun" in available:
        markets = await predict_catalog.resolve(markets)
    if "SX Bet" in available:
        markets = await sx_catalog.resolve(markets)
    if "Myriad" in available:
        markets = await myriad_catalog.resolve(markets)

    candidates = _build_route_market_snapshot(markets)
    horizon_active = (
        filter_markets_for_launch_horizon(
            candidates,
            config.categories_to_scan,
            sports_horizon_hours=config.max_sports_market_horizon_hours,
            crypto_horizon_hours=config.max_crypto_market_horizon_hours,
        )
        if config.market_horizon_filter_enabled
        else candidates
    )
    category_active = filter_markets_for_categories(horizon_active, config.categories_to_scan, config.execution_mode)
    active = _filter_markets_by_volume(category_active, config)
    volume_active_count = len(active)
    if repository is not None:
        await repository.upsert_market_candidates(candidates)
        active = await repository.apply_verified_mappings(active)
    verified_count = sum(bool(market.verified_routes) for market in active)
    snapshot_config = replace(config, markets=active)
    if config.execution_mode.submits_orders:
        active = _verified_active_markets(snapshot_config)
        snapshot_config = replace(snapshot_config, markets=active)
    missing_routes = tuple(_missing_discovery_routes(snapshot_config))
    gamma_stats = gamma_resolver.last_resolution_stats
    myriad_raw, myriad_parsed = myriad_catalog.last_catalog_counts
    predict_raw, predict_parsed = predict_catalog.last_catalog_counts
    sx_raw, sx_parsed = sx_catalog.last_catalog_counts
    stages = {
        "myriad_catalog_raw": myriad_raw,
        "myriad_catalog_parsed": myriad_parsed,
        "predict_catalog_raw": predict_raw,
        "predict_catalog_parsed": predict_parsed,
        "sx_catalog_raw": sx_raw,
        "sx_catalog_parsed": sx_parsed,
        "seed_catalog": myriad_parsed + predict_parsed + sx_parsed,
        "polymarket_catalog": gamma_resolver.catalog_size,
        "exact_id_matches": gamma_stats.exact_id_matches,
        "exact_title_matches": gamma_stats.exact_title_matches,
        "structured_sports_matches": getattr(gamma_stats, "structured_sports_matches", 0),
        "semantic_matches": gamma_stats.semantic_matches,
        "cross_venue_candidates": len(candidates),
        "horizon_accepted": len(horizon_active),
        "category_accepted": len(category_active),
        "volume_accepted": volume_active_count,
        "verified_mapping_markets": verified_count,
        "tradable": len(active),
    }
    rejection_reasons = dict(gamma_stats.rejection_reasons)
    rejection_reasons["horizon_rejected"] = max(0, len(candidates) - len(horizon_active))
    rejection_reasons["category_rejected"] = max(0, len(horizon_active) - len(category_active))
    rejection_reasons["volume_rejected"] = max(0, len(category_active) - volume_active_count)
    diagnostics = DiscoveryDiagnostics(
        stages=tuple(stages.items()),
        rejection_reasons=tuple((key, value) for key, value in sorted(rejection_reasons.items()) if value),
    )
    LOGGER.info(
        "discovery_pipeline_summary",
        extra={
            "_stages": stages,
            "_rejection_reasons": dict(diagnostics.rejection_reasons),
            "_missing_routes": missing_routes,
        },
    )
    return DiscoveryResult(tuple(active), missing_routes, diagnostics)


def _assert_once_discovery_ready(result: DiscoveryResult) -> None:
    if result.markets and not result.missing_routes:
        return
    diagnostics = result.diagnostics.as_dict()
    raise RuntimeError(
        "One-shot discovery produced no complete tradable route set: "
        f"markets={len(result.markets)} missing_routes={list(result.missing_routes)} diagnostics={diagnostics}"
    )


def _verified_active_markets(config: AppConfig) -> list[MarketSpec]:
    return [
        market
        for market in config.markets
        if any(
            _market_supports_route(market, route, require_verified=True)
            and is_live_mapping_eligible(market, config.execution_mode, route)
            for route in _enabled_routes(config)
        )
    ]


def _missing_discovery_routes(config: AppConfig) -> list[str]:
    require_verified = config.execution_mode.submits_orders
    return [
        route
        for route in _enabled_routes(config)
        if not any(
            _market_supports_route(market, route, require_verified=require_verified) for market in config.markets
        )
    ]


def _enabled_routes(config: AppConfig) -> tuple[str, ...]:
    routes: list[str] = []
    if getattr(config.routes, "polymarket_myriad", False):
        routes.append("polymarket_myriad")
    if getattr(config.routes, "polymarket_predict", False):
        routes.append("polymarket_predict")
    if getattr(config.routes, "predict_myriad", False):
        routes.append("predict_myriad")
    if getattr(config.routes, "predict_sx", False):
        routes.append("predict_sx")
    if getattr(config.routes, "polymarket_sx", False):
        routes.append("polymarket_sx")
    if getattr(config.routes, "sx_myriad", False):
        routes.append("sx_myriad")
    return tuple(routes)


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
    if route == "polymarket_myriad":
        return bool(market.polymarket_token_id and market.myriad_market_id)
    if route == "polymarket_predict":
        return bool(
            market.venue_b_label == "Predict.fun" and market.polymarket_token_id and market.predict_fun_token_id
        )
    if route == "predict_myriad":
        return bool(market.venue_b_label == "Predict.fun" and market.predict_fun_token_id and market.myriad_market_id)
    if route == "predict_sx":
        return bool(
            market.venue_a_label == "Predict.fun"
            and market.venue_b_label == "SX Bet"
            and market.polymarket_token_id
            and market.predict_fun_token_id
        )
    if route == "polymarket_sx":
        return bool(market.venue_b_label == "SX Bet" and market.polymarket_token_id and market.predict_fun_token_id)
    if route == "sx_myriad":
        return bool(market.venue_b_label == "SX Bet" and market.predict_fun_token_id and market.myriad_market_id)
    return False


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
        [market for market in polymarket_family if market.venue_b_label in {"Predict.fun", "Myriad"}]
    )
    sx_family = _deduplicate_markets(
        [market for market in polymarket_family if market.venue_b_label in {"SX Bet", "Myriad"}]
    )
    predict_sx = _synthesize_predict_sx_markets(predict_family, sx_family)
    return _deduplicate_route_markets([*passthrough, *predict_family, *sx_family, *predict_sx])


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
            LOGGER.error(
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
