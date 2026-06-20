from __future__ import annotations

import argparse
import asyncio
import logging
import random
from collections.abc import Awaitable
from dataclasses import replace
from pathlib import Path
from typing import TYPE_CHECKING

from dotenv import load_dotenv

from .config import AppConfig, load_config, validate_config
from .connectors.base import BinaryMarketClient
from .connectors.myriad import MyriadClient
from .connectors.polymarket import PolymarketClobClient
from .connectors.predict_fun import PredictFunApiClient
from .discovery_lifecycle import ActiveMarketRegistry, DiscoveryCoordinator, DiscoveryDiagnostics, DiscoveryResult
from .engine import ArbitrageEngine
from .execution import ExecutionRouter
from .logging_config import configure_logging
from .market_discovery import GammaCacheUnavailable, GammaMarketResolver
from .market_mapping import filter_markets_for_categories
from .matcher import normalize_text
from .models import MarketSpec
from .myriad_discovery import MyriadMarketResolver
from .position_manager import PositionManager
from .positions import JsonPositionLedger, PositionLedger
from .predict_fun_discovery import PredictFunMarketResolver
from .risk import GlobalRiskController
from .settlement import SettlementService
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

    load_dotenv()
    configure_logging()
    config = load_config(args.config)
    validate_config(config, require_verified_mappings=False)
    repository: ProductionRepository | None = None
    if config.database_url:
        repository = ProductionRepository(config.database_url)
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
    risk_controller = GlobalRiskController(
        config.max_daily_loss_usd,
        config.max_consecutive_api_errors,
        None if config.execution_mode.submits_orders else "data/state.json",
        state_store=repository if config.execution_mode.submits_orders else None,
    )
    await risk_controller.initialize()
    unresolved_entries = [position for position in ledger.all() if position.status == "entry_pending"]
    if args.resume_risk_only:
        blocking_positions = [
            position
            for position in ledger.all()
            if position.status in {"entry_pending", "unwind_pending", "partial_exit_pending", "manual_review"}
        ]
        unresolved_intents = await repository.unresolved_order_intents() if repository is not None else []
        reconciliation_failures = await repository.latest_reconciliation_failures() if repository is not None else []
        if blocking_positions or unresolved_intents or reconciliation_failures:
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
    predict_route_enabled = config.routes.polymarket_predict or config.routes.predict_myriad
    myriad_route_enabled = config.routes.polymarket_myriad or config.routes.predict_myriad
    predict_enabled = (
        predict_route_enabled
        and config.enable_predict_fun
        and config.predict_fun.enabled
        and bool(config.predict_fun.api_key)
    )
    myriad_enabled = myriad_route_enabled and config.myriad_markets.enabled
    if not predict_enabled:
        LOGGER.info("predict_fun_disabled", extra={"_reason": "disabled or PREDICT_FUN_API_KEY is missing"})
    gamma_resolver = GammaMarketResolver(scan_all=config.scan_all)
    myriad_resolver = MyriadMarketResolver(config.myriad_markets)
    myriad_catalog = MyriadMarketResolver(config.myriad_markets, scan_all=True)
    predict_resolver = PredictFunMarketResolver(config.predict_fun)
    predict_catalog = PredictFunMarketResolver(config.predict_fun, scan_all=True)
    bootstrap_observability: ObservabilityServer | None = None
    if config.scan_all and not args.once:
        bootstrap_observability = ObservabilityServer(
            config.observability_host,
            config.observability_port,
            risk_controller,
            {},
            repository=repository,
            discovery_ready=lambda: False,
            max_market_data_age_seconds=config.max_orderbook_age_seconds,
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
                    myriad_catalog,
                    predict_catalog,
                    repository,
                    predict_enabled=predict_enabled,
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
            if myriad_enabled:
                markets = await myriad_resolver.resolve(markets)
            candidate_markets = _deduplicate_markets(markets)
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
                myriad_catalog.close(),
                predict_resolver.close(),
                predict_catalog.close(),
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
    def register_predict_markets(markets: tuple[MarketSpec, ...]) -> None:
        if predict_fun is None:
            return
        for market in markets:
            predict_fun.register_market(
                market.predict_fun_token_id,
                market.predict_fun_market_id,
                market.predict_fun_side,
                market.predict_fun_fee_rate_bps,
            )

    register_predict_markets(tuple(config.markets))
    discovery_coordinator: DiscoveryCoordinator | None = None
    if config.scan_all and not args.once:

        async def refresh_discovery() -> DiscoveryResult:
            return await _resolve_scan_all_snapshot(
                config,
                gamma_resolver,
                myriad_resolver,
                myriad_catalog,
                predict_catalog,
                repository,
                predict_enabled=predict_enabled,
                myriad_enabled=myriad_enabled,
            )

        discovery_coordinator = DiscoveryCoordinator(
            market_registry,
            refresh_discovery,
            on_publish=register_predict_markets,
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
    market_locks: dict[str, asyncio.Lock] = {}
    capacity_lock = asyncio.Lock()
    pending_markets: set[str] = set()
    balance_cache: dict[str, float] = {}
    capital_reservations: dict[str, float] = {}
    optimistic_debits: dict[str, float] = {}
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
    settlement_clients: dict[str, BinaryMarketClient] = {"Polymarket": polymarket}
    if predict_fun is not None:
        settlement_clients["Predict.fun"] = predict_fun
    if myriad is not None:
        settlement_clients["Myriad"] = myriad
    for client in settlement_clients.values():
        client.set_market_data_snapshot_interval(config.market_data_snapshot_interval_seconds)
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
        myriad=myriad,
        myriad_execution=myriad_execution,
        predict_myriad_execution=predict_myriad_execution,
        ledger=ledger,
        settlement_service=settlement_service,
    )
    engine = ArbitrageEngine(
        config,
        polymarket,
        predict_fun,
        execution,
        myriad=myriad,
        myriad_execution=myriad_execution,
        predict_myriad_execution=predict_myriad_execution,
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
            await reconciliation.close()
            await asyncio.gather(
                *(client.close() for client in reconciliation_clients.values()),
                return_exceptions=True,
            )
            await telegram.close()
            await gamma_resolver.close()
            await repository.close()
            raise RuntimeError(f"Startup reconciliation failed: {reconciliation.last_error}")
        await reconciliation.start()

        async def reconcile_after_pause() -> None:
            assert reconciliation is not None
            await reconciliation.run_once(full=True)

        risk_controller.register_pause_callback(reconcile_after_pause)
    observability = ObservabilityServer(
        config.observability_host,
        config.observability_port,
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
    )
    await observability.start()
    risk_controller.start_external_monitor()
    try:
        if discovery_coordinator is not None:
            discovery_coordinator.start()
        for router in (execution, myriad_execution, predict_myriad_execution):
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
        for router in (execution, myriad_execution, predict_myriad_execution):
            if router is not None:
                await router.close()
        await polymarket.close()
        if predict_fun is not None:
            await predict_fun.close()
        if myriad is not None:
            await myriad.close()
        await telegram.close()
        await risk_controller.close()
        await asyncio.gather(
            gamma_resolver.close(),
            myriad_resolver.close(),
            myriad_catalog.close(),
            predict_resolver.close(),
            predict_catalog.close(),
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
    myriad_resolver: MyriadMarketResolver,
    myriad_catalog: MyriadMarketResolver,
    predict_catalog: PredictFunMarketResolver,
    repository: ProductionRepository | None,
    *,
    predict_enabled: bool,
    myriad_enabled: bool,
) -> DiscoveryResult:
    predict_catalog.invalidate_cache()
    catalog_calls: list[tuple[str, Awaitable[list[MarketSpec]]]] = []
    if myriad_enabled:
        catalog_calls.append(("Myriad", myriad_catalog.resolve([])))
    if predict_enabled:
        catalog_calls.append(("Predict.fun", predict_catalog.resolve([])))
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
    if "Myriad" in available:
        markets = await myriad_resolver.resolve(markets)

    candidates = _deduplicate_markets(markets)
    category_active = filter_markets_for_categories(candidates, config.categories_to_scan, config.execution_mode)
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
    stages = {
        "myriad_catalog_raw": myriad_raw,
        "myriad_catalog_parsed": myriad_parsed,
        "predict_catalog_raw": predict_raw,
        "predict_catalog_parsed": predict_parsed,
        "seed_catalog": myriad_parsed + predict_parsed,
        "polymarket_catalog": gamma_resolver.catalog_size,
        "exact_id_matches": gamma_stats.exact_id_matches,
        "exact_title_matches": gamma_stats.exact_title_matches,
        "semantic_matches": gamma_stats.semantic_matches,
        "cross_venue_candidates": len(candidates),
        "category_accepted": len(category_active),
        "volume_accepted": volume_active_count,
        "verified_mapping_markets": verified_count,
        "tradable": len(active),
    }
    rejection_reasons = dict(gamma_stats.rejection_reasons)
    rejection_reasons["category_rejected"] = max(0, len(candidates) - len(category_active))
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
        if any(_market_supports_route(market, route, require_verified=True) for route in _enabled_routes(config))
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
    if config.routes.polymarket_myriad:
        routes.append("polymarket_myriad")
    if config.routes.polymarket_predict:
        routes.append("polymarket_predict")
    if config.routes.predict_myriad:
        routes.append("predict_myriad")
    return tuple(routes)


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
        return bool(market.polymarket_token_id and market.predict_fun_token_id)
    if route == "predict_myriad":
        return bool(market.predict_fun_token_id and market.myriad_market_id)
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
    filtered = [market for market in markets if _maximum_market_volume(market) >= threshold]
    LOGGER.info(
        "market_volume_filter_applied",
        extra={"_before": len(markets), "_after": len(filtered), "_minimum_volume_usd": threshold},
    )
    return filtered


def _maximum_market_volume(market: MarketSpec) -> float:
    volumes = (market.polymarket_volume_usd, market.predict_fun_volume_usd, market.myriad_volume_usd)
    return max((volume for volume in volumes if volume is not None), default=0.0)


def _deduplicate_markets(markets: list[MarketSpec]) -> list[MarketSpec]:
    merged: dict[str, MarketSpec] = {}
    ambiguous: set[str] = set()
    for market in markets:
        expiry = market.expires_at.isoformat() if market.expires_at else ""
        key = market.polymarket_token_id or f"{normalize_text(market.target_label or market.symbol)}:{expiry}"
        existing = merged.get(key)
        if existing is None:
            merged[key] = market
            continue
        predict_conflict = (
            existing.predict_fun_token_id
            and market.predict_fun_token_id
            and existing.predict_fun_token_id != market.predict_fun_token_id
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
