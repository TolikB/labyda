from __future__ import annotations

import argparse
import asyncio
import logging
from dataclasses import replace

from dotenv import load_dotenv

from .config import AppConfig, load_config, validate_config
from .connectors.myriad import MyriadClient
from .connectors.polymarket import PolymarketClobClient
from .connectors.predict_fun import PredictFunApiClient
from .engine import ArbitrageEngine
from .execution import ExecutionRouter
from .logging_config import configure_logging
from .market_discovery import GammaMarketResolver
from .matcher import normalize_text
from .myriad_discovery import MyriadMarketResolver
from .models import MarketSpec
from .position_manager import PositionManager
from .predict_fun_discovery import PredictFunMarketResolver
from .positions import JsonPositionLedger
from .risk import GlobalRiskController
from .telegram import TelegramNotifier

LOGGER = logging.getLogger(__name__)


async def async_main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="config.json")
    parser.add_argument("--once", action="store_true", help="run a single engine cycle and exit")
    parser.add_argument("--resume-risk-only", action="store_true", help="clear the durable global risk pause and exit")
    args = parser.parse_args()

    load_dotenv()
    configure_logging()
    config = load_config(args.config)
    validate_config(config)
    risk_controller = GlobalRiskController(
        config.max_daily_loss_usd,
        config.max_consecutive_api_errors,
        "data/state.json",
    )
    ledger = JsonPositionLedger("data/open_positions.json")
    unresolved_entries = [position for position in ledger.all() if position.status == "entry_pending"]
    if args.resume_risk_only:
        if unresolved_entries:
            raise RuntimeError("Cannot resume risk: unresolved entry intents require manual venue reconciliation")
        await risk_controller.resume()
        LOGGER.warning("global_risk_pause_cleared_by_operator")
        return
    if unresolved_entries:
        await risk_controller.pause(
            f"{len(unresolved_entries)} unresolved entry intent(s) found after restart"
        )
        LOGGER.critical(
            "startup_paused_unresolved_entry_intents",
            extra={"_count": len(unresolved_entries)},
        )
    predict_enabled = config.enable_predict_fun and config.predict_fun.enabled and bool(config.predict_fun.api_key)
    if not predict_enabled:
        LOGGER.info("predict_fun_disabled", extra={"_reason": "disabled or PREDICT_FUN_API_KEY is missing"})
    gamma_resolver = GammaMarketResolver(scan_all=config.scan_all)
    myriad_resolver = MyriadMarketResolver(config.myriad_markets)
    myriad_catalog = MyriadMarketResolver(config.myriad_markets, scan_all=True)
    predict_resolver = PredictFunMarketResolver(config.predict_fun)
    predict_catalog = PredictFunMarketResolver(config.predict_fun, scan_all=True)
    try:
        if config.scan_all:
            catalog_tasks = [myriad_catalog.resolve([])]
            if predict_enabled:
                catalog_tasks.append(predict_catalog.resolve([]))
            catalog_results = await asyncio.gather(*catalog_tasks, return_exceptions=True)
            myriad_result = catalog_results[0]
            markets = [] if isinstance(myriad_result, BaseException) else myriad_result
            if predict_enabled:
                predict_result = catalog_results[1]
                if isinstance(predict_result, BaseException):
                    if not config.myriad_markets.enabled:
                        raise predict_result
                    LOGGER.error(
                        "predict_fun_catalog_unavailable_continuing_with_myriad",
                        extra={"_error": str(predict_result)},
                    )
                    predict_enabled = False
                    config = replace(config, enable_predict_fun=False)
                else:
                    markets.extend(predict_result)
            markets = await gamma_resolver.resolve(markets)
            if predict_enabled:
                markets = await predict_catalog.resolve(markets)
            markets = await myriad_resolver.resolve(markets)
            markets = _deduplicate_markets(markets)
            markets = _filter_markets_by_volume(markets, config)
        else:
            markets = await gamma_resolver.resolve(config.markets)
            if predict_enabled:
                try:
                    markets = await predict_resolver.resolve(markets)
                except Exception:
                    if not config.myriad_markets.enabled:
                        raise
                    LOGGER.exception("predict_fun_discovery_unavailable_continuing_with_myriad")
                    predict_enabled = False
                    config = replace(config, enable_predict_fun=False)
            markets = await myriad_resolver.resolve(markets)
            markets = _deduplicate_markets(markets)
    finally:
        await asyncio.gather(
            gamma_resolver.close(),
            myriad_resolver.close(),
            myriad_catalog.close(),
            predict_resolver.close(),
            predict_catalog.close(),
            return_exceptions=True,
        )
    config = replace(config, markets=markets)
    validate_config(config, require_resolved_markets=True)
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
    myriad = MyriadClient(config.myriad_markets) if config.myriad_markets.enabled else None
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
        )
        if predict_fun is not None
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
        )
        if myriad is not None
        else None
    )
    predict_myriad_execution = None
    if myriad is not None and predict_fun is not None:
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
    try:
        for router in (execution, myriad_execution, predict_myriad_execution):
            if router is not None:
                await router.start()
        if args.once:
            await engine.run_once()
        else:
            await engine.run_forever()
    finally:
        for router in (execution, myriad_execution, predict_myriad_execution):
            if router is not None:
                await router.close()
        await polymarket.close()
        if predict_fun is not None:
            await predict_fun.close()
        if myriad is not None:
            await myriad.close()
        await telegram.close()


def main() -> None:
    try:
        import uvloop  # type: ignore[import-not-found]

        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    except (ImportError, RuntimeError):
        pass
    asyncio.run(async_main())


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
                (value for value in (existing.polymarket_volume_usd, market.polymarket_volume_usd) if value is not None),
                default=None,
            ),
            predict_fun_volume_usd=max(
                (value for value in (existing.predict_fun_volume_usd, market.predict_fun_volume_usd) if value is not None),
                default=None,
            ),
            myriad_volume_usd=max(
                (value for value in (existing.myriad_volume_usd, market.myriad_volume_usd) if value is not None),
                default=None,
            ),
        )
    return [market for key, market in merged.items() if key not in ambiguous]


if __name__ == "__main__":
    main()
