from __future__ import annotations

import argparse
import asyncio
from dataclasses import replace

from dotenv import load_dotenv

from .config import load_config, validate_config
from .connectors.myriad import MyriadClient
from .connectors.polymarket import PolymarketClobClient
from .connectors.predict_fun import PredictFunApiClient
from .engine import ArbitrageEngine
from .execution import ExecutionRouter
from .logging_config import configure_logging
from .market_discovery import GammaMarketResolver
from .myriad_discovery import MyriadMarketResolver
from .position_manager import PositionManager
from .predict_fun_discovery import PredictFunMarketResolver
from .positions import JsonPositionLedger
from .telegram import TelegramNotifier


async def async_main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="config.json")
    parser.add_argument("--once", action="store_true", help="run a single engine cycle and exit")
    args = parser.parse_args()

    load_dotenv()
    configure_logging()
    config = load_config(args.config)
    validate_config(config)
    if config.scan_all:
        predict_catalog, myriad_catalog = await asyncio.gather(
            PredictFunMarketResolver(config.predict_fun, scan_all=True).resolve([]),
            MyriadMarketResolver(config.myriad_markets, scan_all=True).resolve([]),
        )
        markets = predict_catalog + myriad_catalog
        markets = await GammaMarketResolver(scan_all=True).resolve(markets)
        markets = await PredictFunMarketResolver(config.predict_fun).resolve(markets)
        markets = await MyriadMarketResolver(config.myriad_markets).resolve(markets)
    else:
        markets = await GammaMarketResolver().resolve(config.markets)
        markets = await PredictFunMarketResolver(config.predict_fun).resolve(markets)
        markets = await MyriadMarketResolver(config.myriad_markets).resolve(markets)
    config = replace(config, markets=markets)
    validate_config(config, require_resolved_markets=True)
    polymarket = PolymarketClobClient(config.polymarket)
    predict_fun = PredictFunApiClient(config.predict_fun)
    myriad = MyriadClient(config.myriad_markets) if config.myriad_markets.enabled else None
    telegram = TelegramNotifier(config.telegram)
    ledger = JsonPositionLedger("data/open_positions.json")
    execution = ExecutionRouter(config, polymarket, predict_fun, telegram, ledger)
    myriad_execution = (
        ExecutionRouter(
            config,
            polymarket,
            myriad,
            telegram,
            ledger,
            second_leg_label="Myriad",
            second_leg_fill_timeout_ms=config.myriad_fill_timeout_ms,
        )
        if myriad is not None
        else None
    )
    predict_myriad_execution = (
        ExecutionRouter(
            config,
            predict_fun,
            myriad,
            telegram,
            ledger,
            first_leg_label="Predict.fun",
            second_leg_label="Myriad",
            first_leg_fill_timeout_ms=config.predict_fun_fill_timeout_ms,
            second_leg_fill_timeout_ms=config.myriad_fill_timeout_ms,
        )
        if myriad is not None
        else None
    )
    position_manager = PositionManager(
        config=config,
        polymarket=polymarket,
        predict_fun=predict_fun,
        execution=execution,
        myriad=myriad,
        myriad_execution=myriad_execution,
        predict_myriad_execution=predict_myriad_execution,
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
    )
    if args.once:
        await engine.run_once()
    else:
        await engine.run_forever()


def main() -> None:
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
