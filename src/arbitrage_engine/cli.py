from __future__ import annotations

import argparse
import asyncio
import gzip
import hashlib
import json
import os
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

from alembic import command
from alembic.config import Config
from dotenv import load_dotenv

from .config import AppConfig, load_config, validate_config
from .connectors.base import BinaryMarketClient
from .connectors.myriad import MyriadClient
from .connectors.polymarket import PolymarketClobClient
from .connectors.predict_fun import PredictFunApiClient
from .database import ProductionRepository
from .market_discovery import GammaMarketResolver
from .models import ExecutionMode, MappingStatus, MarketMapping, MarketSpec, SettlementRequest, position_key
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

    production = commands.add_parser("production")
    production_commands = production.add_subparsers(dest="production_command", required=True)
    verify = production_commands.add_parser("verify")
    verify.add_argument("--backup-dir", default="/var/backups/arbitrage")
    verify.add_argument("--restore-marker", default="/var/lib/arbitrage/restore-drill.json")
    verify.add_argument("--release-sha-file", default="/etc/arbitrage/release-sha")
    verify.add_argument("--drain-marker", default="/var/lib/arbitrage/drain-ready.json")
    drain = production_commands.add_parser("drain")
    drain.add_argument("--reason", required=True)
    drain.add_argument("--marker", default="/var/lib/arbitrage/drain-ready.json")

    state = commands.add_parser("state")
    state_commands = state.add_subparsers(dest="state_command", required=True)
    import_json = state_commands.add_parser("import-json")
    import_json.add_argument("--path", default="data/open_positions.json")

    risk = commands.add_parser("risk")
    risk_commands = risk.add_subparsers(dest="risk_command", required=True)
    risk_commands.add_parser("status")
    risk_commands.add_parser("resume")
    pause = risk_commands.add_parser("pause")
    pause.add_argument("--reason", required=True)

    orders = commands.add_parser("orders")
    order_commands = orders.add_subparsers(dest="order_command", required=True)
    cancel_all = order_commands.add_parser("cancel-all")
    cancel_all.add_argument("--confirm", choices=["YES"], required=True)

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
            if args.risk_command == "pause":
                await risk.pause(args.reason)
            elif args.risk_command == "resume":
                if await repository.unresolved_order_intents():
                    raise SystemExit("Cannot resume: unresolved order intents remain")
                if await repository.unresolved_redemption_intents():
                    raise SystemExit("Cannot resume: unresolved redemption intents remain")
                blocking_positions = [
                    position
                    for position in await repository.load_positions()
                    if position.status in {"entry_pending", "unwind_pending", "partial_exit_pending", "manual_review"}
                ]
                if blocking_positions:
                    raise SystemExit("Cannot resume: unresolved or manual-review positions remain")
                reconciliation_failures = await repository.latest_reconciliation_failures()
                if reconciliation_failures:
                    raise SystemExit(
                        "Cannot resume: latest reconciliation is not clean: " + "; ".join(reconciliation_failures)
                    )
                await risk.resume()
            print(
                json.dumps(
                    {
                        "paused": risk.paused,
                        "pause_reason": risk.pause_reason,
                        "daily_loss_usd": str(risk.daily_loss_usd),
                        "consecutive_api_errors": risk.consecutive_api_errors,
                    },
                    indent=2,
                )
            )
        elif args.command == "reconcile":
            await _reconcile(config, repository)
        elif args.command == "orders":
            await _cancel_all_orders(config)
        elif args.command == "production":
            if args.production_command == "drain":
                await _production_drain(config, repository, args.reason, Path(args.marker))
            else:
                passed, report = await _production_verify(
                    config,
                    repository,
                    Path(args.backup_dir),
                    Path(args.restore_marker),
                    Path(args.release_sha_file),
                    Path(args.drain_marker),
                )
                print(json.dumps(report, default=str, indent=2, ensure_ascii=False))
                if not passed:
                    raise SystemExit(1)
    finally:
        await repository.close()


async def _discovery_audit(app_config: AppConfig) -> None:
    from .main import _resolve_scan_all_snapshot

    gamma = GammaMarketResolver(scan_all=True)
    myriad_resolver = MyriadMarketResolver(app_config.myriad_markets)
    myriad_catalog = MyriadMarketResolver(
        app_config.myriad_markets,
        scan_all=True,
        categories_to_scan=app_config.categories_to_scan,
    )
    predict_catalog = PredictFunMarketResolver(
        app_config.predict_fun,
        scan_all=True,
        categories_to_scan=app_config.categories_to_scan,
    )
    repository: ProductionRepository | None = None
    if app_config.database_url:
        candidate = ProductionRepository(app_config.database_url)
        if await candidate.ping():
            repository = candidate
        else:
            await candidate.close()
    try:
        predict_enabled = (
            app_config.enable_predict_fun
            and app_config.predict_fun.enabled
            and bool(app_config.predict_fun.api_key)
            and (app_config.routes.polymarket_predict or app_config.routes.predict_myriad)
        )
        result = await _resolve_scan_all_snapshot(
            app_config,
            gamma,
            myriad_resolver,
            myriad_catalog,
            predict_catalog,
            repository,
            predict_enabled=predict_enabled,
            myriad_enabled=app_config.myriad_markets.enabled
            and (app_config.routes.polymarket_myriad or app_config.routes.predict_myriad),
        )
        print(
            json.dumps(
                {
                    **result.diagnostics.as_dict(),
                    "missing_routes": result.missing_routes,
                    "tradable_market_count": len(result.markets),
                },
                indent=2,
                ensure_ascii=False,
            )
        )
    finally:
        await asyncio.gather(
            gamma.close(),
            myriad_resolver.close(),
            myriad_catalog.close(),
            predict_catalog.close(),
            return_exceptions=True,
        )
        if repository is not None:
            await repository.close()


async def _production_drain(
    app_config: AppConfig,
    repository: ProductionRepository,
    reason: str,
    marker_path: Path,
) -> None:
    risk = GlobalRiskController(
        app_config.max_daily_loss_usd,
        app_config.max_consecutive_api_errors,
        state_store=repository,
    )
    await risk.initialize()
    await risk.pause(f"production drain: {reason}")
    await repository.audit("production_drain_started", {"reason": reason})
    await _cancel_all_orders(app_config)
    await _reconcile(app_config, repository)
    unresolved_orders = await repository.unresolved_order_intents()
    unresolved_redemptions = await repository.unresolved_redemption_intents()
    reconciliation_failures = await repository.latest_reconciliation_failures()
    if unresolved_orders or unresolved_redemptions or reconciliation_failures:
        raise SystemExit(
            "Drain remains fail-closed: unresolved_orders="
            f"{len(unresolved_orders)} unresolved_redemptions={len(unresolved_redemptions)} "
            f"reconciliation_failures={reconciliation_failures}"
        )
    payload = {
        "ready": True,
        "reason": reason,
        "completed_at": datetime.now(UTC).isoformat(),
    }
    marker_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = marker_path.with_suffix(f"{marker_path.suffix}.tmp")
    temporary.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    os.replace(temporary, marker_path)
    await repository.audit("production_drain_completed", payload)
    print(json.dumps(payload, indent=2))


async def _production_verify(
    app_config: AppConfig,
    repository: ProductionRepository,
    backup_dir: Path,
    restore_marker: Path,
    release_sha_file: Path,
    drain_marker: Path,
) -> tuple[bool, dict[str, object]]:
    from .main import _resolve_scan_all_snapshot

    checks: list[dict[str, object]] = []

    def record(name: str, passed: bool, detail: object) -> None:
        checks.append({"name": name, "passed": passed, "detail": detail})

    try:
        validate_config(app_config, require_verified_mappings=False)
    except ValueError as exc:
        record("configuration", False, str(exc))
    else:
        record("configuration", True, "valid")
    predict_required = app_config.predict_fun.enabled and (
        app_config.routes.polymarket_predict or app_config.routes.predict_myriad
    )
    myriad_required = app_config.myriad_markets.enabled and (
        app_config.routes.polymarket_myriad or app_config.routes.predict_myriad
    )
    credential_checks = {
        "POLYMARKET_PRIVATE_KEY": bool(app_config.polymarket.private_key),
        "POLYMARKET_FUNDER_ADDRESS": app_config.polymarket.signature_type == 0 or bool(app_config.polymarket.funder),
        "MYRIAD_PRIVATE_KEY": not myriad_required or bool(app_config.myriad_markets.private_key),
        "PREDICT_FUN_PRIVATE_KEY": not predict_required or bool(app_config.predict_fun.private_key),
        "PREDICT_FUN_API_KEY": not predict_required or bool(app_config.predict_fun.api_key),
    }
    record(
        "credentials",
        all(credential_checks.values()),
        {name: "configured" if present else "missing" for name, present in credential_checks.items()},
    )
    record("execution_mode", app_config.execution_mode is ExecutionMode.CANARY, app_config.execution_mode.value)
    record("database", await repository.ping(), "reachable")
    revision = await repository.schema_revision()
    record("database_migration", revision == "0002_redemption_intents", revision or "alembic_version unavailable")
    lock_acquired = await repository.acquire_trader_lock()
    record("trader_lock", lock_acquired, "acquired" if lock_acquired else "held by another process")
    if lock_acquired:
        await repository.release_trader_lock()
    release_sha = await asyncio.to_thread(_read_text, release_sha_file)
    verified_sha = os.getenv("CI_VERIFIED_COMMIT_SHA")
    record(
        "verified_commit_sha",
        bool(release_sha and verified_sha and release_sha == verified_sha),
        {"deployed": release_sha, "ci_verified": verified_sha},
    )
    backup = await asyncio.to_thread(_latest_valid_backup, backup_dir)
    backup_fresh = backup is not None and _age_seconds(backup) <= 8 * 60 * 60
    record(
        "backup",
        backup_fresh,
        str(backup) if backup else f"no valid .sql.gz with checksum in {backup_dir}",
    )
    restore_fresh = _marker_is_fresh(restore_marker, max_age_seconds=30 * 24 * 60 * 60)
    record("restore_drill", restore_fresh, str(restore_marker))
    drain_ready = _marker_is_fresh(drain_marker, max_age_seconds=30 * 24 * 60 * 60, require_ready=True)
    record("spot_drain_readiness", drain_ready, str(drain_marker))

    gamma = GammaMarketResolver(scan_all=True)
    myriad_resolver = MyriadMarketResolver(app_config.myriad_markets)
    myriad_catalog = MyriadMarketResolver(
        app_config.myriad_markets,
        scan_all=True,
        categories_to_scan=app_config.categories_to_scan,
    )
    predict_catalog = PredictFunMarketResolver(
        app_config.predict_fun,
        scan_all=True,
        categories_to_scan=app_config.categories_to_scan,
    )
    clients: dict[str, BinaryMarketClient] = {}
    markets: tuple[MarketSpec, ...] = ()
    try:
        predict_enabled = (
            app_config.enable_predict_fun
            and app_config.predict_fun.enabled
            and bool(app_config.predict_fun.api_key)
            and (app_config.routes.polymarket_predict or app_config.routes.predict_myriad)
        )
        myriad_enabled = app_config.myriad_markets.enabled and (
            app_config.routes.polymarket_myriad or app_config.routes.predict_myriad
        )
        discovery = await _resolve_scan_all_snapshot(
            app_config,
            gamma,
            myriad_resolver,
            myriad_catalog,
            predict_catalog,
            repository,
            predict_enabled=predict_enabled,
            myriad_enabled=myriad_enabled,
        )
        markets = discovery.markets
        record(
            "discovery",
            bool(markets) and not discovery.missing_routes,
            {
                **discovery.diagnostics.as_dict(),
                "missing_routes": discovery.missing_routes,
            },
        )
    except Exception as exc:
        record("discovery", False, str(exc))

    if markets:
        clients["Polymarket"] = PolymarketClobClient(app_config.polymarket)
        if app_config.routes.polymarket_myriad or app_config.routes.predict_myriad:
            clients["Myriad"] = MyriadClient(app_config.myriad_markets)
        if app_config.routes.polymarket_predict or app_config.routes.predict_myriad:
            clients["Predict.fun"] = PredictFunApiClient(app_config.predict_fun)
        for venue, client in clients.items():
            try:
                balance = await client.get_cash_balance()
                record(
                    f"balance:{venue}",
                    balance >= app_config.min_venue_balance_usd,
                    {"balance_usd": balance, "minimum_usd": app_config.min_venue_balance_usd},
                )
                record(f"reconciliation_contract:{venue}", client.supports_full_reconciliation(), "supported")
                settlement_supported = type(client).redeem_position is not BinaryMarketClient.redeem_position
                record(
                    f"redemption_support:{venue}",
                    settlement_supported,
                    "supported" if settlement_supported else "missing",
                )
                gas_balance_method = getattr(client, "get_native_gas_balance", None)
                if callable(gas_balance_method):
                    gas_balance = await gas_balance_method()
                    record(f"gas_balance:{venue}", gas_balance > 0, gas_balance)
                open_orders = await client.list_open_orders()
                record(f"open_orders:{venue}", not open_orders, len(open_orders))
                await client.list_fills(None)
                positions = await client.get_positions()
                record(f"position_snapshot:{venue}", True, {"position_count": len(positions)})
            except Exception as exc:
                record(f"venue:{venue}", False, str(exc))
        first = markets[0]
        market_tokens = {
            "Polymarket": first.polymarket_token_id,
            "Myriad": first.myriad_market_id or "",
            "Predict.fun": first.predict_fun_token_id,
        }
        for venue, token in market_tokens.items():
            market_client = clients.get(venue)
            if market_client is None or not token:
                continue
            try:
                book = await asyncio.wait_for(market_client.watch_order_book(token), timeout=15.0)
                record(f"market_data:{venue}", bool(book.bids and book.asks), "two-sided book")
            except Exception as exc:
                record(f"market_data:{venue}", False, str(exc))
            settlement_request = _settlement_request_for_market(first, venue)
            if settlement_request is None:
                record(f"settlement_metadata:{venue}", False, "condition/collateral metadata missing")
                continue
            try:
                prepared = market_client.prepare_settlement_request(settlement_request)
                settlement_status = await market_client.get_settlement_status(prepared)
                record(f"settlement_status:{venue}", True, settlement_status.value)
            except Exception as exc:
                record(f"settlement_status:{venue}", False, str(exc))

    unresolved = await repository.unresolved_order_intents()
    unresolved_redemptions = await repository.unresolved_redemption_intents()
    failures = await repository.latest_reconciliation_failures()
    stale_mappings = await repository.has_stale_mappings()
    record("unresolved_intents", not unresolved, len(unresolved))
    record("unresolved_redemptions", not unresolved_redemptions, len(unresolved_redemptions))
    record("reconciliation_history", not failures, failures)
    record("stale_mappings", not stale_mappings, stale_mappings)
    metrics = await repository.metrics_snapshot()
    record("zero_unresolved_exposure", metrics["exposure_usd"] == 0, str(metrics["exposure_usd"]))

    await asyncio.gather(
        *(client.close() for client in clients.values()),
        gamma.close(),
        myriad_resolver.close(),
        myriad_catalog.close(),
        predict_catalog.close(),
        return_exceptions=True,
    )
    passed = all(bool(check["passed"]) for check in checks)
    return passed, {"passed": passed, "checks": checks}


def _latest_valid_backup(backup_dir: Path) -> Path | None:
    if not backup_dir.is_dir():
        return None
    for path in sorted(backup_dir.glob("*.sql.gz"), key=lambda item: item.stat().st_mtime, reverse=True):
        try:
            with gzip.open(path, "rb") as handle:
                while handle.read(1024 * 1024):
                    pass
            checksum_path = path.with_name(f"{path.name}.sha256")
            expected = checksum_path.read_text(encoding="utf-8").split()[0]
            digest = _sha256_file(path)
            if digest != expected:
                continue
        except (OSError, EOFError, IndexError):
            continue
        return path
    return None


def _read_text(path: Path) -> str | None:
    try:
        return path.read_text(encoding="utf-8").strip() or None
    except OSError:
        return None


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _age_seconds(path: Path) -> float:
    return max(0.0, datetime.now(UTC).timestamp() - path.stat().st_mtime)


def _marker_is_fresh(path: Path, *, max_age_seconds: float, require_ready: bool = False) -> bool:
    try:
        if _age_seconds(path) > max_age_seconds:
            return False
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        return False
    return not require_ready or bool(payload.get("ready"))


def _settlement_request_for_market(market: MarketSpec, venue: str) -> SettlementRequest | None:
    if venue == "Polymarket":
        market_id = market.polymarket_market_id or market.condition_id
        condition_id = market.condition_id
        collateral = ""
    elif venue == "Myriad":
        market_id = market.myriad_market_id
        condition_id = market.myriad_condition_id
        collateral = market.myriad_collateral_token or ""
    else:
        return None
    if not market_id or not condition_id:
        return None
    return SettlementRequest(
        position_key=position_key(market),
        venue=venue,
        market_id=market_id,
        condition_id=condition_id,
        collateral_token=collateral,
        expected_contracts=Decimal(0),
    )


async def _cancel_all_orders(app_config: AppConfig) -> None:
    clients: dict[str, BinaryMarketClient] = {"Polymarket": PolymarketClobClient(app_config.polymarket)}
    if app_config.predict_fun.enabled and app_config.predict_fun.api_key:
        clients["Predict.fun"] = PredictFunApiClient(app_config.predict_fun)
    if app_config.myriad_markets.enabled:
        clients["Myriad"] = MyriadClient(app_config.myriad_markets)
    results: dict[str, dict[str, object]] = {}
    try:
        for venue, client in clients.items():
            cancelled: list[str] = []
            failures: dict[str, str] = {}
            try:
                orders = await client.list_open_orders()
            except Exception as exc:
                results[venue] = {"cancelled": cancelled, "failures": {"list_open_orders": str(exc)}}
                continue
            for order in orders:
                try:
                    await client.cancel_order(order.venue_order_id)
                    cancelled.append(order.venue_order_id)
                except Exception as exc:
                    failures[order.venue_order_id] = str(exc)
            results[venue] = {"cancelled": cancelled, "failures": failures}
        print(json.dumps(results, indent=2, ensure_ascii=False))
        if any(result["failures"] for result in results.values()):
            raise SystemExit(1)
    finally:
        await asyncio.gather(*(client.close() for client in clients.values()), return_exceptions=True)


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
