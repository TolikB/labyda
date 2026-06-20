from __future__ import annotations

import argparse
import asyncio
import json
import os
from datetime import UTC, datetime
from pathlib import Path

from alembic import command
from alembic.config import Config
from dotenv import load_dotenv

from .config import AppConfig, load_config
from .connectors.base import BinaryMarketClient
from .connectors.myriad import MyriadClient
from .connectors.polymarket import PolymarketClobClient
from .connectors.predict_fun import PredictFunApiClient
from .database import ProductionRepository
from .market_discovery import GammaMarketResolver
from .models import MappingStatus, MarketMapping, position_key
from .myriad_discovery import MyriadMarketResolver
from .positions import JsonPositionLedger
from .predict_fun_discovery import PredictFunMarketResolver
from .reconciliation import ReconciliationService
from .risk import GlobalRiskController


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="arbitrage-admin")
    parser.add_argument("--config", default="config.json")
    commands = parser.add_subparsers(dest="command", required=True)

    db = commands.add_parser("db")
    db_commands = db.add_subparsers(dest="db_command", required=True)
    db_commands.add_parser("migrate")

    mappings = commands.add_parser("mappings")
    mapping_commands = mappings.add_subparsers(dest="mapping_command", required=True)
    list_command = mapping_commands.add_parser("list")
    list_command.add_argument("--status", choices=[status.value for status in MappingStatus])
    for name in ("approve", "reject"):
        action = mapping_commands.add_parser(name)
        action.add_argument("mapping_id")
        action.add_argument("--operator", default=os.getenv("USER") or os.getenv("USERNAME") or "operator")

    discovery = commands.add_parser("discovery")
    discovery_commands = discovery.add_subparsers(dest="discovery_command", required=True)
    discovery_commands.add_parser("audit")

    state = commands.add_parser("state")
    state_commands = state.add_subparsers(dest="state_command", required=True)
    import_json = state_commands.add_parser("import-json")
    import_json.add_argument("--path", default="data/open_positions.json")

    risk = commands.add_parser("risk")
    risk_commands = risk.add_subparsers(dest="risk_command", required=True)
    risk_commands.add_parser("status")
    risk_commands.add_parser("resume")

    commands.add_parser("reconcile")
    return parser


def main() -> None:
    load_dotenv()
    args = build_parser().parse_args()
    if args.command == "db" and args.db_command == "migrate":
        _migrate(args.config)
        return
    asyncio.run(_async_command(args))


def _migrate(config_path: str) -> None:
    app_config = load_config(config_path)
    if not app_config.database_url:
        raise SystemExit("DATABASE_URL/database_url is required")
    alembic_config = Config("alembic.ini")
    alembic_config.set_main_option("sqlalchemy.url", app_config.database_url)
    command.upgrade(alembic_config, "head")


async def _async_command(args: argparse.Namespace) -> None:
    config = load_config(args.config)
    if args.command == "discovery":
        await _discovery_audit(config)
        return
    if not config.database_url:
        raise SystemExit("DATABASE_URL/database_url is required")
    repository = ProductionRepository(config.database_url)
    try:
        if args.command == "mappings":
            if args.mapping_command == "list":
                status = MappingStatus(args.status) if args.status else None
                mappings = await repository.list_mappings(status)
                print(json.dumps([_mapping_json(mapping) for mapping in mappings], indent=2, ensure_ascii=False))
            else:
                status = MappingStatus.VERIFIED if args.mapping_command == "approve" else MappingStatus.REJECTED
                await repository.set_mapping_status(args.mapping_id, status, operator=args.operator)
                print(f"{args.mapping_id} -> {status.value}")
        elif args.command == "state":
            source_path = Path(args.path)
            ledger = JsonPositionLedger(source_path)
            for position in ledger.all():
                await repository.save_position(position_key(position.market), position)
            archive_path: Path | None = None
            if await asyncio.to_thread(source_path.exists) and ledger.all():
                timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
                archive_path = source_path.with_name(f"{source_path.stem}.imported-{timestamp}{source_path.suffix}")
                await asyncio.to_thread(source_path.replace, archive_path)
            print(
                f"imported_positions={len(ledger.all())}"
                + (f" archived_to={archive_path}" if archive_path is not None else "")
            )
        elif args.command == "risk":
            risk = GlobalRiskController(
                config.max_daily_loss_usd,
                config.max_consecutive_api_errors,
                state_store=repository,
            )
            await risk.initialize()
            if args.risk_command == "resume":
                if await repository.unresolved_order_intents():
                    raise SystemExit("Cannot resume: unresolved order intents remain")
                blocking_positions = [
                    position
                    for position in await repository.load_positions()
                    if position.status in {"entry_pending", "unwind_pending", "partial_exit_pending", "manual_review"}
                ]
                if blocking_positions:
                    raise SystemExit("Cannot resume: unresolved or manual-review positions remain")
                await risk.resume()
            print(
                json.dumps(
                    {
                        "paused": risk.paused,
                        "pause_reason": risk.pause_reason,
                        "daily_loss_usd": risk.daily_loss_usd,
                        "consecutive_api_errors": risk.consecutive_api_errors,
                    },
                    indent=2,
                )
            )
        elif args.command == "reconcile":
            await _reconcile(config, repository)
    finally:
        await repository.close()


async def _discovery_audit(app_config: AppConfig) -> None:
    gamma = GammaMarketResolver(scan_all=True)
    myriad = MyriadMarketResolver(app_config.myriad_markets, scan_all=True)
    predict = PredictFunMarketResolver(app_config.predict_fun, scan_all=True)
    try:
        myriad_markets, predict_markets = await asyncio.gather(
            myriad.resolve([]),
            predict.resolve([]),
            return_exceptions=True,
        )
        seeds = []
        counts: dict[str, object] = {}
        if isinstance(myriad_markets, BaseException):
            counts["myriad_error"] = str(myriad_markets)
        else:
            counts["myriad_catalog"] = len(myriad_markets)
            seeds.extend(myriad_markets)
        if isinstance(predict_markets, BaseException):
            counts["predict_error"] = str(predict_markets)
        else:
            counts["predict_catalog"] = len(predict_markets)
            seeds.extend(predict_markets)
        await gamma.bootstrap(seeds)
        resolved = await gamma.resolve(seeds)
        counts["cross_venue_candidates"] = len(resolved)
        print(json.dumps(counts, indent=2, ensure_ascii=False))
    finally:
        await asyncio.gather(gamma.close(), myriad.close(), predict.close(), return_exceptions=True)


async def _reconcile(app_config: AppConfig, repository: ProductionRepository) -> None:
    clients: dict[str, BinaryMarketClient] = {"Polymarket": PolymarketClobClient(app_config.polymarket)}
    if app_config.predict_fun.enabled and app_config.predict_fun.api_key:
        clients["Predict.fun"] = PredictFunApiClient(app_config.predict_fun)
    if app_config.myriad_markets.enabled:
        clients["Myriad"] = MyriadClient(app_config.myriad_markets)
    risk = GlobalRiskController(
        app_config.max_daily_loss_usd,
        app_config.max_consecutive_api_errors,
        state_store=repository,
    )
    await risk.initialize()
    service = ReconciliationService(repository, clients, risk)
    try:
        results = await service.run_once(full=True)
        print(json.dumps([result.__dict__ for result in results], default=str, indent=2))
    finally:
        await asyncio.gather(*(client.close() for client in clients.values()), return_exceptions=True)


def _mapping_json(mapping: MarketMapping) -> dict[str, object]:
    return {
        "mapping_id": mapping.mapping_id,
        "canonical_market_id": mapping.canonical_market_id,
        "left": f"{mapping.left_venue}:{mapping.left_market_id}",
        "right": f"{mapping.right_venue}:{mapping.right_market_id}",
        "status": mapping.status.value,
        "rules_fingerprint": mapping.rules_fingerprint,
        "verified_at": mapping.verified_at.isoformat() if mapping.verified_at else None,
        "verified_by": mapping.verified_by,
    }


if __name__ == "__main__":
    main()
