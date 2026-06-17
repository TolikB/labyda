from __future__ import annotations

import argparse
import asyncio

from .config import load_config
from .connectors.cefi import CcxtProBinanceFuturesClient
from .connectors.polymarket import PolymarketClobClient
from .engine import ArbitrageEngine
from .execution import ExecutionRouter
from .logging_config import configure_logging
from .positions import PositionLedger
from .telegram import TelegramNotifier


async def async_main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="config.json")
    args = parser.parse_args()

    configure_logging()
    config = load_config(args.config)
    polymarket = PolymarketClobClient(config.polymarket)
    cefi = CcxtProBinanceFuturesClient(config.binance)
    telegram = TelegramNotifier(config.telegram)
    execution = ExecutionRouter(config, polymarket, cefi, telegram, PositionLedger())
    engine = ArbitrageEngine(config, polymarket, cefi, execution)
    await engine.run_forever()


def main() -> None:
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
