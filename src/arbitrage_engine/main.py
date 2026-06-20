from __future__ import annotations

import argparse
import asyncio
import logging
import random
from dataclasses import replace
from pathlib import Path
from typing import TYPE_CHECKING

from dotenv import load_dotenv

from .config import AppConfig, load_config, validate_config
from .connectors.base import BinaryMarketClient
from .connectors.myriad import MyriadClient
from .connectors.polymarket import PolymarketClobClient
from .connectors.predict_fun import PredictFunApiClient
from .engine import ArbitrageEngine
from .execution import ExecutionRouter
from .logging_config import configure_logging
from .market_discovery import GammaMarketResolver
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
        if unresolved_entries:
            if repository is not None:
                await repository.close()
            raise RuntimeError("Cannot resume risk: unresolved entry intents require manual venue reconciliation")
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
    gamma_bootstrapped = False
    candidate_markets: list[MarketSpec] = []
    discovery_succeeded = False
    try:
        retry_delay = _DISCOVERY_RETRY_INITIAL_SECONDS
        while True:
            if config.scan_all:
                catalog_tasks = [myriad_catalog.resolve([])] if myriad_enabled else []
                predict_catalog_available = predict_enabled
                if predict_enabled:
                    catalog_tasks.append(predict_catalog.resolve([]))
                catalog_results = await asyncio.gather(*catalog_tasks, return_exceptions=True)
                result_index = 0
                markets: list[MarketSpec] = []
                if myriad_enabled:
                    myriad_result = catalog_results[result_index]
                    result_index += 1
                    if not isinstance(myriad_result, BaseException):
                        markets.extend(myriad_result)
                if predict_enabled:
                    predict_result = catalog_results[result_index]
                    if isinstance(predict_result, BaseException):
                        predict_catalog_available = False
                        if config.routes.polymarket_myriad:
                            LOGGER.error(
                                "predict_fun_catalog_unavailable_continuing_with_myriad",
                                extra={"_error": str(predict_result)},
                            )
                            predict_enabled = False
                            config = replace(
                                config,
                                enable_predict_fun=False,
                                routes=replace(
                                    config.routes,
                                    polymarket_predict=False,
                                    predict_myriad=False,
                                ),
                            )
                        else:
                            LOGGER.error(
                                "predict_fun_required_catalog_unavailable",
                                extra={
                                    "_authentication_failure": _is_predict_auth_failure(predict_result),
                                    "_error": str(predict_result),
                                },
                            )
                    else:
                        markets.extend(predict_result)
                await gamma_resolver.bootstrap(markets)
                gamma_bootstrapped = True
                markets = await gamma_resolver.resolve(markets)
                if predict_catalog_available:
                    markets = await predict_catalog.resolve(markets)
                if myriad_enabled:
                    markets = await myriad_resolver.resolve(markets)
                candidate_markets = _deduplicate_markets(markets)
                markets = filter_markets_for_categories(
                    candidate_markets, config.categories_to_scan, config.execution_mode
                )
                markets = _filter_markets_by_volume(markets, config)
            else:
                if any(
                    not market.polymarket_token_id or market.polymarket_token_id == "replace-with-token-id"
                    for market in config.markets
                ):
                    await gamma_resolver.bootstrap(config.markets)
                    gamma_bootstrapped = True
                markets = await gamma_resolver.resolve(config.markets)
                if predict_enabled:
                    try:
                        markets = await predict_resolver.resolve(markets)
                    except Exception as exc:
                        if _is_predict_auth_failure(exc) or not config.routes.polymarket_myriad:
                            raise
                        LOGGER.exception("predict_fun_discovery_unavailable_continuing_with_myriad")
                        predict_enabled = False
                        config = replace(
                            config,
                            enable_predict_fun=False,
                            routes=replace(
                                config.routes,
                                polymarket_predict=False,
                                predict_myriad=False,
                            ),
                        )
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

            missing_routes = _missing_discovery_routes(config)
            if not _should_retry_discovery(config, args.once, missing_routes):
                break
            sleep_seconds = _jittered_retry_delay(retry_delay)
            LOGGER.warning(
                "discovery_not_ready_retrying",
                extra={
                    "_candidate_count": len(candidate_markets),
                    "_active_market_count": len(config.markets),
                    "_missing_routes": missing_routes,
                    "_retry_seconds": sleep_seconds,
                },
            )
            await asyncio.sleep(sleep_seconds)
            retry_delay = _next_discovery_retry_delay(retry_delay)

        validate_config(config, require_resolved_markets=not config.scan_all)
        discovery_succeeded = True
    finally:
        await asyncio.gather(
            myriad_resolver.close(),
            myriad_catalog.close(),
            predict_resolver.close(),
            predict_catalog.close(),
            return_exceptions=True,
        )
        if not discovery_succeeded:
            await gamma_resolver.close()
            if bootstrap_observability is not None:
                await bootstrap_observability.close()
            if repository is not None:
                await repository.close()
    if bootstrap_observability is not None:
        await bootstrap_observability.close()
    polymarket = PolymarketClobClient(config.polymarket)
    predict_fun = PredictFunApiClient(config.predict_fun) if predict_enabled else None
    if predict_fun is not None:
        for market in config.markets:
            predict_fun.register_market(
                market.predict_fun_token_id,
                market.predict_fun_market_id,
                market.predict_fun_side,
                market.predict_fun_fee_rate_bps,
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
    )
    if gamma_bootstrapped:
        gamma_resolver.start_background_refresh()
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
        discovery_ready=lambda: bool(config.markets),
        max_market_data_age_seconds=config.max_orderbook_age_seconds,
    )
    await observability.start()
    try:
        for router in (execution, myriad_execution, predict_myriad_execution):
            if router is not None:
                await router.start()
        if args.once:
            await engine.run_once()
        else:
            await engine.run_forever()
    finally:
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
        await gamma_resolver.close()
        if repository is not None:
            await repository.close()


def main() -> None:
    try:
        import uvloop  # type: ignore[import-not-found]

        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    except (ImportError, RuntimeError):
        pass
    asyncio.run(async_main())


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
