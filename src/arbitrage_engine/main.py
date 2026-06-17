from __future__ import annotations

import argparse
import asyncio
from dataclasses import replace

from dotenv import load_dotenv

from .config import load_config, validate_config
from .connectors.cefi import CcxtProBinanceFuturesClient
from .connectors.polymarket import PolymarketClobClient
from .engine import ArbitrageEngine
from .execution import ExecutionRouter
from .logging_config import configure_logging
from .market_discovery import GammaMarketResolver
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
    config = replace(config, markets=await GammaMarketResolver().resolve(config.markets))
    validate_config(config, require_resolved_markets=True)
    polymarket = PolymarketClobClient(config.polymarket)
    cefi = CcxtProBinanceFuturesClient(config.binance)
    telegram = TelegramNotifier(config.telegram)
    execution = ExecutionRouter(config, polymarket, cefi, telegram, JsonPositionLedger("data/open_positions.json"))
    engine = ArbitrageEngine(config, polymarket, cefi, execution)
    if args.once:
        await engine.run_once()
    else:
        await engine.run_forever()


def main() -> None:
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
