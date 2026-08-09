from __future__ import annotations

import argparse
import asyncio
import gzip
import hashlib
import json
import os
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, cast

from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory

from .config import AppConfig, load_config, load_operator_env, validate_config
from .connectors.base import BinaryMarketClient
from .connectors.myriad import MyriadClient
from .connectors.polymarket import PolymarketClobClient
from .connectors.predict_fun import PredictFunApiClient
from .connectors.sx_bet import SxBetApiClient
from .database import ProductionRepository
from .market_mapping import normalize_launch_category, route_key
from .models import (
    BinarySide,
    ExecutionMode,
    MappingStatus,
    MarketDataStatus,
    MarketMapping,
    MarketSpec,
    OrderIntentStatus,
    SettlementRequest,
    VenueOrder,
    myriad_execution_token_for_route,
    position_key,
)
from .positions import JsonPositionLedger
from .production_audit import (
    build_route_overlap_report,
    collect_all_market_audit,
    enabled_routes,
    live_window_has_real_order_evidence,
    resolve_route_discovery_snapshot,
)
from .reconciliation import ReconciliationService
from .risk import GlobalRiskController
from .sports_matching import sports_market_identity

_SYNTHETIC_MARKET_KEY_PREFIXES = ("integration:", "restart:")
_SYNTHETIC_TOKEN_IDS = {"integration-token", "restart-token"}
_DEFAULT_PRODUCTION_BACKUP_DIR = os.getenv("ARBITRAGE_BACKUP_DIR", "/mnt/arbitrage-backups")
_DEFAULT_PRODUCTION_RESTORE_MARKER = os.getenv(
    "ARBITRAGE_RESTORE_MARKER",
    "/mnt/arbitrage-backups/restore-drill.json",
)
_DEFAULT_PRODUCTION_RELEASE_SHA_FILE = os.getenv(
    "ARBITRAGE_RELEASE_SHA_FILE",
    ".runtime/release-sha",
)
_DEFAULT_PRODUCTION_DRAIN_MARKER = os.getenv(
    "ARBITRAGE_DRAIN_MARKER",
    "/mnt/arbitrage-backups/drain-ready.json",
)
_MAPPING_ROUTE_CHOICES = (
    "polymarket_myriad",
    "polymarket_predict",
    "predict_myriad",
    "predict_sx",
    "polymarket_sx",
    "sx_myriad",
)


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
    list_command.add_argument(
        "--route",
        choices=_MAPPING_ROUTE_CHOICES,
    )
    list_command.add_argument("--canonical-market-id")
    review_command = mapping_commands.add_parser("review")
    review_command.add_argument("--status", choices=[status.value for status in MappingStatus])
    review_command.add_argument("--route", choices=_MAPPING_ROUTE_CHOICES)
    review_command.add_argument("--canonical-market-id")
    review_command.add_argument("--operator", default=os.getenv("USER") or os.getenv("USERNAME") or "operator")
    approve_safe = mapping_commands.add_parser("approve-safe-candidates")
    approve_safe.add_argument("--operator", default=os.getenv("USER") or os.getenv("USERNAME") or "operator")
    approve_safe.add_argument("--route", choices=_MAPPING_ROUTE_CHOICES)
    approve_safe.add_argument("--allow-structured-sports", action="store_true")
    approve_safe.add_argument("--confirm", choices=["YES"])
    for name in ("approve", "reject"):
        action = mapping_commands.add_parser(name)
        action.add_argument("mapping_id")
        action.add_argument("--operator", default=os.getenv("USER") or os.getenv("USERNAME") or "operator")

    discovery = commands.add_parser("discovery")
    discovery_commands = discovery.add_subparsers(dest="discovery_command", required=True)
    discovery_commands.add_parser("audit")
    discovery_commands.add_parser("overlap")

    production = commands.add_parser("production")
    production_commands = production.add_subparsers(dest="production_command", required=True)
    verify = production_commands.add_parser("verify")
    _add_production_check_arguments(verify)
    audit = production_commands.add_parser("audit")
    _add_production_check_arguments(audit)
    drain = production_commands.add_parser("drain")
    drain.add_argument("--reason", required=True)
    drain.add_argument("--marker", default=_DEFAULT_PRODUCTION_DRAIN_MARKER)

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
    review_unresolved = order_commands.add_parser("review-unresolved")
    review_unresolved.add_argument("--older-than-minutes", type=float, default=60.0)
    retire_safe = order_commands.add_parser("retire-safe-unresolved")
    retire_safe.add_argument("--older-than-minutes", type=float, default=60.0)
    retire_safe.add_argument("--confirm", choices=["YES"])

    commands.add_parser("reconcile")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    load_operator_env(args.config)
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
        if args.discovery_command == "audit":
            await _discovery_audit(config)
        else:
            await _discovery_overlap(config)
        return
    if not config.database_url:
        raise SystemExit("DATABASE_URL/database_url is required")
    repository = ProductionRepository(
        config.database_url,
        runtime_instance_id=config.runtime_instance_id,
        enabled_routes=enabled_routes(config),
    )
    try:
        if args.command == "mappings":
            if args.mapping_command == "list":
                status = MappingStatus(args.status) if args.status else None
                mappings = await repository.list_mappings(status)
                if args.route:
                    mappings = [mapping for mapping in mappings if _mapping_route(mapping) == args.route]
                if args.canonical_market_id:
                    mappings = [
                        mapping for mapping in mappings if mapping.canonical_market_id == args.canonical_market_id
                    ]
                print(json.dumps([_mapping_json(mapping) for mapping in mappings], indent=2, ensure_ascii=False))
            elif args.mapping_command == "review":
                status = MappingStatus(args.status) if args.status else None
                mappings = await repository.list_mappings(status)
                if args.route:
                    mappings = [mapping for mapping in mappings if _mapping_route(mapping) == args.route]
                if args.canonical_market_id:
                    mappings = [
                        mapping
                        for mapping in mappings
                        if mapping.canonical_market_id == args.canonical_market_id
                    ]
                snapshot = await repository.mapping_review_snapshot(mappings)
                print(
                    json.dumps(
                        _mapping_review_report(
                            mappings,
                            _enabled_route_names(config),
                            config=config,
                            config_path=args.config,
                            operator=args.operator,
                            canonical_markets=cast(
                                dict[str, dict[str, object]],
                                snapshot["canonical_markets"],
                            ),
                            venue_instruments=cast(
                                dict[str, dict[str, object]],
                                snapshot["venue_instruments"],
                            ),
                        ),
                        indent=2,
                        ensure_ascii=False,
                    )
                )
            elif args.mapping_command == "approve-safe-candidates":
                mappings = await repository.list_mappings(None)
                snapshot = await repository.mapping_review_snapshot(mappings)
                report = _mapping_review_report(
                    mappings,
                    _enabled_route_names(config),
                    config=config,
                    config_path=args.config,
                    operator=args.operator,
                    canonical_markets=cast(
                        dict[str, dict[str, object]],
                        snapshot["canonical_markets"],
                    ),
                    venue_instruments=cast(
                        dict[str, dict[str, object]],
                        snapshot["venue_instruments"],
                    ),
                    allow_structured_sports=args.allow_structured_sports,
                )
                candidates = _approval_candidates_from_report(report, route=args.route)
                route_option = f" --route {args.route}" if args.route else ""
                structured_option = " --allow-structured-sports" if args.allow_structured_sports else ""
                if args.confirm == "YES":
                    approved: list[str] = []
                    for candidate in candidates:
                        mapping_id = str(candidate["mapping_id"])
                        await repository.set_mapping_status(
                            mapping_id,
                            MappingStatus.VERIFIED,
                            operator=args.operator,
                        )
                        approved.append(mapping_id)
                    print(
                        json.dumps(
                            {
                                "applied": True,
                                "approved_mapping_ids": approved,
                                "operator": args.operator,
                                "route": args.route,
                                "allow_structured_sports": args.allow_structured_sports,
                            },
                            indent=2,
                            ensure_ascii=False,
                        )
                    )
                else:
                    print(
                        json.dumps(
                            {
                                "applied": False,
                                "operator": args.operator,
                                "route": args.route,
                                "allow_structured_sports": args.allow_structured_sports,
                                "approval_candidates": candidates,
                                "confirm_hint": (
                                    f"arbitrage-admin --config {args.config} mappings approve-safe-candidates "
                                    f"--operator {args.operator}{route_option}{structured_option} --confirm YES"
                                ),
                            },
                            indent=2,
                            ensure_ascii=False,
                        )
                    )
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
            if args.order_command == "cancel-all":
                await _cancel_all_orders(config)
            elif args.order_command == "review-unresolved":
                report = await _review_unresolved_orders(config, repository, older_than_minutes=args.older_than_minutes)
                print(json.dumps(report, indent=2, ensure_ascii=False))
            else:
                report = await _retire_safe_unresolved_orders(
                    config,
                    repository,
                    older_than_minutes=args.older_than_minutes,
                    apply=args.confirm == "YES",
                    config_path=args.config,
                )
                print(json.dumps(report, indent=2, ensure_ascii=False))
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
                    include_runtime_snapshot=args.production_command == "audit",
                    all_markets=getattr(args, "all_markets", False),
                    defer_backup_gates=getattr(args, "defer_backup_gates", False),
                    require_live_order_evidence=getattr(args, "require_live_order_evidence", False),
                    live_window_report_paths=list(args.live_window_report or ()),
                )
                print(json.dumps(report, default=str, indent=2, ensure_ascii=False))
                if not passed:
                    raise SystemExit(1)
    finally:
        await repository.close()


async def _discovery_audit(app_config: AppConfig) -> None:
    repository: ProductionRepository | None = None
    if app_config.database_url:
        candidate = ProductionRepository(
            app_config.database_url,
            runtime_instance_id=app_config.runtime_instance_id,
            enabled_routes=enabled_routes(app_config),
        )
        if await candidate.ping():
            repository = candidate
        else:
            await candidate.close()
    try:
        result = await resolve_route_discovery_snapshot(app_config, repository)
        print(
            json.dumps(
                {
                    **result.diagnostics.as_dict(),
                    "missing_routes": result.missing_routes,
                    "tradable_market_count": len(result.tradable_markets),
                },
                indent=2,
                ensure_ascii=False,
            )
        )
    finally:
        if repository is not None:
            await repository.close()


async def _discovery_overlap(app_config: AppConfig) -> None:
    repository: ProductionRepository | None = None
    if app_config.database_url:
        candidate = ProductionRepository(
            app_config.database_url,
            runtime_instance_id=app_config.runtime_instance_id,
            enabled_routes=enabled_routes(app_config),
        )
        if await candidate.ping():
            repository = candidate
        else:
            await candidate.close()
    try:
        snapshot = await resolve_route_discovery_snapshot(app_config, repository)
        print(json.dumps(build_route_overlap_report(snapshot), indent=2, ensure_ascii=False))
    finally:
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
    os.chmod(temporary, 0o644)
    os.replace(temporary, marker_path)
    os.chmod(marker_path, 0o644)
    await repository.audit("production_drain_completed", payload)
    print(json.dumps(payload, indent=2))


async def _production_verify(
    app_config: AppConfig,
    repository: ProductionRepository,
    backup_dir: Path,
    restore_marker: Path,
    release_sha_file: Path,
    drain_marker: Path,
    *,
    include_runtime_snapshot: bool = False,
    all_markets: bool = False,
    defer_backup_gates: bool = False,
    require_live_order_evidence: bool = False,
    live_window_report_paths: list[str] | None = None,
) -> tuple[bool, dict[str, object]]:
    checks: list[dict[str, object]] = []
    overlap_report: dict[str, Any] | None = None
    all_market_report: dict[str, Any] | None = None
    live_window_reports: dict[str, dict[str, Any]] = {}
    runtime_snapshot: dict[str, Any] | None = None

    def record(name: str, passed: bool, detail: object) -> None:
        checks.append({"name": name, "passed": passed, "detail": detail})

    try:
        validate_config(app_config, require_verified_mappings=False)
    except ValueError as exc:
        record("configuration", False, str(exc))
    else:
        record("configuration", True, "valid")
    predict_required = (
        app_config.enable_predict_fun
        and app_config.predict_fun.enabled
        and (
            app_config.routes.polymarket_predict
            or app_config.routes.predict_myriad
            or app_config.routes.predict_sx
        )
    )
    sx_required = (
        app_config.enable_sx_bet
        and app_config.sx_bet.enabled
        and (app_config.routes.polymarket_sx or app_config.routes.sx_myriad or app_config.routes.predict_sx)
    )
    myriad_required = app_config.myriad_markets.enabled and (
        app_config.routes.polymarket_myriad
        or app_config.routes.predict_myriad
        or app_config.routes.sx_myriad
    )
    credential_checks = {
        "POLYMARKET_PRIVATE_KEY": bool(app_config.polymarket.private_key),
        "POLYMARKET_FUNDER_ADDRESS": app_config.polymarket.signature_type == 0 or bool(app_config.polymarket.funder),
        "MYRIAD_PRIVATE_KEY": not myriad_required or bool(app_config.myriad_markets.private_key),
        "PREDICT_FUN_PRIVATE_KEY": not predict_required or bool(app_config.predict_fun.private_key),
        "PREDICT_FUN_API_KEY": not predict_required or bool(app_config.predict_fun.api_key),
        "SX_BET_PRIVATE_KEY": not sx_required or bool(app_config.sx_bet.private_key),
    }
    record(
        "credentials",
        all(credential_checks.values()),
        {name: "configured" if present else "missing" for name, present in credential_checks.items()},
    )
    record("execution_mode", app_config.execution_mode is ExecutionMode.CANARY, app_config.execution_mode.value)
    record("database", await repository.ping(), "reachable")
    revision = await repository.schema_revision()
    expected_revision = _migration_head_revision()
    record(
        "database_migration",
        expected_revision is not None and revision == expected_revision,
        {
            "current": revision or "alembic_version unavailable",
            "expected": expected_revision or "migration head unavailable",
        },
    )
    lock_acquired = await repository.acquire_trader_lock()
    trader_lock_passed = lock_acquired or include_runtime_snapshot
    trader_lock_detail = "acquired"
    if not lock_acquired:
        trader_lock_detail = (
            "held by another process (accepted during live runtime audit)"
            if include_runtime_snapshot
            else "held by another process"
        )
    record("trader_lock", trader_lock_passed, trader_lock_detail)
    if lock_acquired:
        await repository.release_trader_lock()
    release_sha = await asyncio.to_thread(_read_text, release_sha_file)
    verified_sha = os.getenv("CI_VERIFIED_COMMIT_SHA")
    record(
        "verified_commit_sha",
        bool(release_sha and verified_sha and release_sha == verified_sha),
        {"deployed": release_sha, "ci_verified": verified_sha},
    )
    if defer_backup_gates:
        deferred_detail = {
            "deferred": True,
            "reason": "initial funded launch explicitly defers backup/restore/drain gates",
        }
        record("backup", True, deferred_detail)
        record("restore_drill", True, deferred_detail)
        record("spot_drain_readiness", True, deferred_detail)
    else:
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

    clients: dict[str, BinaryMarketClient] = {}
    markets: tuple[MarketSpec, ...] = ()
    discovery_snapshot = None
    try:
        discovery_snapshot = await resolve_route_discovery_snapshot(app_config, repository)
        markets = discovery_snapshot.tradable_markets
        record(
            "discovery",
            bool(markets) and not discovery_snapshot.missing_routes,
            {
                **discovery_snapshot.diagnostics.as_dict(),
                "missing_routes": discovery_snapshot.missing_routes,
            },
        )
    except Exception as exc:
        record("discovery", False, str(exc))

    if markets:
        clients["Polymarket"] = PolymarketClobClient(app_config.polymarket)
        if (
            app_config.routes.polymarket_myriad
            or app_config.routes.predict_myriad
            or app_config.routes.sx_myriad
        ):
            clients["Myriad"] = MyriadClient(app_config.myriad_markets)
        if app_config.routes.polymarket_predict or app_config.routes.predict_myriad or app_config.routes.predict_sx:
            clients["Predict.fun"] = PredictFunApiClient(app_config.predict_fun)
        if app_config.routes.polymarket_sx or app_config.routes.sx_myriad or app_config.routes.predict_sx:
            clients["SX Bet"] = SxBetApiClient(app_config.sx_bet)
        _register_second_leg_market_clients(markets, clients)
        for venue, client in clients.items():
            try:
                balance = await client.get_cash_balance()
                record(
                    f"balance:{venue}",
                    balance >= app_config.min_venue_balance_usd,
                    {"balance_usd": balance, "minimum_usd": app_config.min_venue_balance_usd},
                )
                record(f"reconciliation_contract:{venue}", client.supports_full_reconciliation(), "supported")
                settlement_supported, settlement_detail = _automatic_redemption_status(client, venue)
                record(
                    f"automatic_redemption_support:{venue}",
                    settlement_supported,
                    settlement_detail,
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
        for venue, venue_markets in _candidate_markets_by_venue(markets).items():
            market_client = clients.get(venue)
            if market_client is None or not venue_markets:
                continue
            market_data_detail: Any = "no eligible market token"
            market_data_passed = False
            for market in venue_markets:
                token = _market_token_for_venue(market, venue)
                if not token:
                    continue
                try:
                    book = await asyncio.wait_for(market_client.watch_order_book(token), timeout=15.0)
                    market_data_detail = _market_data_probe_detail(book)
                    if _market_data_probe_passed(book):
                        market_data_passed = True
                        break
                except Exception as exc:
                    market_data_detail = str(exc)
            record(f"market_data:{venue}", market_data_passed, market_data_detail)

            settlement_status_detail: Any = "condition/collateral metadata missing"
            settlement_status_passed = False
            settlement_metadata_found = False
            for market in venue_markets:
                settlement_request = _settlement_request_for_market(market, venue)
                if settlement_request is None:
                    continue
                settlement_metadata_found = True
                try:
                    prepared = market_client.prepare_settlement_request(settlement_request)
                    settlement_status = await market_client.get_settlement_status(prepared)
                    settlement_status_detail = settlement_status.value
                    settlement_status_passed = True
                    break
                except Exception as exc:
                    settlement_status_detail = str(exc)
            if not settlement_metadata_found:
                record(f"settlement_metadata:{venue}", False, settlement_status_detail)
                continue
            record(f"settlement_status:{venue}", settlement_status_passed, settlement_status_detail)

    unresolved = await repository.unresolved_order_intents()
    unresolved_redemptions = await repository.unresolved_redemption_intents()
    failures = await repository.latest_reconciliation_failures()
    repository_has_stale_mappings = await repository.has_stale_mappings()
    if discovery_snapshot is not None:
        stale_rows = await repository.list_mappings(MappingStatus.STALE)
        stale_mappings = repository_has_stale_mappings and _has_active_stale_mappings(
            stale_rows,
            discovery_snapshot.tradable_markets,
        )
    else:
        stale_mappings = repository_has_stale_mappings
    record("unresolved_intents", not unresolved, len(unresolved))
    record("unresolved_redemptions", not unresolved_redemptions, len(unresolved_redemptions))
    record("reconciliation_history", not failures, failures)
    record("stale_mappings", not stale_mappings, stale_mappings)
    metrics = await repository.metrics_snapshot()
    pending_unhedged_exposure = metrics.get("pending_unhedged_exposure_usd", metrics["exposure_usd"])
    record(
        "zero_pending_unhedged_exposure",
        pending_unhedged_exposure == 0,
        str(pending_unhedged_exposure),
    )

    if include_runtime_snapshot or all_markets:
        runtime_snapshot = await repository.runtime_audit_snapshot()

    if all_markets and discovery_snapshot is not None:
        overlap_report = build_route_overlap_report(discovery_snapshot)
        all_market_report = await collect_all_market_audit(app_config, discovery_snapshot, runtime_snapshot)
        for route in enabled_routes(app_config):
            route_overlap = overlap_report["routes"].get(route, {})
            record(
                f"verified_tradable_markets:{route}",
                int(route_overlap.get("verified_tradable_count", 0)) > 0,
                route_overlap,
            )
            route_audit = all_market_report["route_summary"].get(route, {})
            technical_openable_count = int(
                route_audit.get("technical_openable_count", route_audit.get("openable_count", 0))
            )
            canary_openable_count = int(
                route_audit.get("canary_openable_count", route_audit.get("openable_count", 0))
            )
            record(
                f"technical_openable_markets:{route}",
                technical_openable_count > 0,
                route_audit,
            )
            record(
                f"canary_openable_markets:{route}",
                canary_openable_count > 0,
                route_audit,
            )
        for venue, venue_report in all_market_report["venue_balances"].items():
            gate = venue_report.get("canary_gate", {})
            record(f"balance_gate:{venue}", bool(gate.get("passed", False)), gate)

    if require_live_order_evidence:
        report_paths = _parse_live_window_report_paths(live_window_report_paths or (), enabled_routes(app_config))
        for route in enabled_routes(app_config):
            report_path = report_paths.get(route)
            live_window_report = _read_json_report(report_path)
            if live_window_report is not None:
                live_window_reports[route] = live_window_report
            report_runtime_instance_id = (
                str(live_window_report.get("runtime_instance_id", "")) if live_window_report is not None else ""
            )
            runtime_instance_matches = (
                live_window_report is not None and report_runtime_instance_id == app_config.runtime_instance_id
            )
            record(
                f"live_canary_evidence:{route}",
                runtime_instance_matches
                and (
                    live_window_has_real_order_evidence(live_window_report, route)
                    if live_window_report is not None
                    else False
                ),
                (
                    {
                        **live_window_report,
                        "expected_runtime_instance_id": app_config.runtime_instance_id,
                        "runtime_instance_match": runtime_instance_matches,
                        "required_route": route,
                    }
                    if live_window_report is not None
                    else {"route": route, "path": str(report_path or "")}
                ),
            )

    await asyncio.gather(
        *(client.close() for client in clients.values()),
        return_exceptions=True,
    )
    passed = all(bool(check["passed"]) for check in checks)
    report: dict[str, object] = {"passed": passed, "checks": checks}
    if overlap_report is not None:
        report["route_overlap"] = overlap_report
    if all_market_report is not None:
        report["all_market_audit"] = all_market_report
    if live_window_reports:
        report["live_window_reports"] = live_window_reports
    if include_runtime_snapshot and runtime_snapshot is not None:
        report["runtime_audit"] = runtime_snapshot
    return passed, report


def _add_production_check_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--backup-dir", default=_DEFAULT_PRODUCTION_BACKUP_DIR)
    parser.add_argument("--restore-marker", default=_DEFAULT_PRODUCTION_RESTORE_MARKER)
    parser.add_argument("--release-sha-file", default=_DEFAULT_PRODUCTION_RELEASE_SHA_FILE)
    parser.add_argument("--drain-marker", default=_DEFAULT_PRODUCTION_DRAIN_MARKER)
    parser.add_argument("--all-markets", action="store_true")
    parser.add_argument("--defer-backup-gates", action="store_true")
    parser.add_argument("--require-live-order-evidence", action="store_true")
    parser.add_argument(
        "--live-window-report",
        action="append",
        metavar="ROUTE=PATH",
        help="Repeat once per enabled route, for example polymarket_sx=artifacts/sx/report.json.",
    )


def _parse_live_window_report_paths(
    values: tuple[str, ...] | list[str],
    routes: tuple[str, ...],
) -> dict[str, Path]:
    result: dict[str, Path] = {}
    for value in values:
        route, separator, path = value.partition("=")
        if not separator or not route or not path:
            raise SystemExit("--live-window-report must use ROUTE=PATH")
        if route not in routes:
            raise SystemExit(f"--live-window-report route is not enabled: {route}")
        if route in result:
            raise SystemExit(f"duplicate --live-window-report for route: {route}")
        result[route] = Path(path)
    return result


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


def _read_json_report(path: Path | None) -> dict[str, Any] | None:
    if path is None:
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


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
    if venue == market.venue_a_label:
        market_id = market.polymarket_market_id or market.condition_id
        condition_id = market.condition_id if venue == "Polymarket" else market.polymarket_market_id
        collateral = ""
    elif venue == "Predict.fun":
        market_id = market.predict_fun_market_id
        condition_id = market.predict_fun_market_id
        collateral = ""
    elif venue == "SX Bet":
        market_id = market.predict_fun_market_id
        condition_id = market.predict_fun_market_id
        collateral = ""
    elif venue == "Myriad":
        market_id = market.myriad_market_id
        condition_id = market.myriad_market_id
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


def _market_data_probe_detail(book: Any) -> dict[str, Any]:
    bids = getattr(book, "bids", [])
    asks = getattr(book, "asks", [])
    status = getattr(book, "status", None)
    return {
        "status": status.value if isinstance(status, MarketDataStatus) else str(status),
        "has_bids": bool(bids),
        "has_asks": bool(asks),
        "bid_levels": len(bids),
        "ask_levels": len(asks),
    }


def _market_data_probe_passed(book: Any) -> bool:
    status = getattr(book, "status", MarketDataStatus.VALID)
    if status in {MarketDataStatus.INVALID, MarketDataStatus.STALE}:
        return False
    return True


async def _cancel_all_orders(app_config: AppConfig) -> None:
    predict_enabled = (
        app_config.enable_predict_fun
        and app_config.predict_fun.enabled
        and bool(app_config.predict_fun.api_key)
        and (
            app_config.routes.polymarket_predict
            or app_config.routes.predict_myriad
            or app_config.routes.predict_sx
        )
    )
    sx_enabled = app_config.enable_sx_bet and app_config.sx_bet.enabled and (
        app_config.routes.polymarket_sx or app_config.routes.sx_myriad or app_config.routes.predict_sx
    )
    myriad_enabled = app_config.myriad_markets.enabled and (
        app_config.routes.polymarket_myriad
        or app_config.routes.predict_myriad
        or app_config.routes.sx_myriad
    )
    clients: dict[str, BinaryMarketClient] = {"Polymarket": PolymarketClobClient(app_config.polymarket)}
    if predict_enabled:
        clients["Predict.fun"] = PredictFunApiClient(app_config.predict_fun)
    if sx_enabled:
        clients["SX Bet"] = SxBetApiClient(app_config.sx_bet)
    if myriad_enabled:
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


async def _review_unresolved_orders(
    app_config: AppConfig,
    repository: ProductionRepository,
    *,
    older_than_minutes: float,
) -> dict[str, Any]:
    unresolved = await repository.unresolved_order_intents()
    positions = await repository.load_positions()
    fills_by_client_order_id = await repository.fills_for_client_order_ids([row.client_order_id for row in unresolved])
    venues = sorted({row.venue for row in unresolved})
    clients = _build_order_review_clients(app_config, venues)
    open_orders_by_venue: dict[str, dict[str, VenueOrder]] = {}
    fills_by_venue_order: dict[str, dict[str, list[dict[str, Any]]]] = {}
    try:
        for venue, client in clients.items():
            if client is None:
                continue
            try:
                open_orders = await client.list_open_orders()
            except Exception:
                open_orders = []
            open_orders_by_venue[venue] = {order.venue_order_id: order for order in open_orders}
            try:
                venue_fills = await client.list_fills(None)
            except Exception:
                venue_fills = []
            grouped: dict[str, list[dict[str, Any]]] = {}
            for fill in venue_fills:
                grouped.setdefault(fill.venue_order_id, []).append(
                    {
                        "fill_id": fill.fill_id,
                        "venue_order_id": fill.venue_order_id,
                        "quantity": str(fill.quantity),
                        "price": str(fill.price),
                        "occurred_at": fill.occurred_at.isoformat(),
                    }
                )
            fills_by_venue_order[venue] = grouped

        rows: list[dict[str, Any]] = []
        safe_candidates: list[dict[str, Any]] = []
        now = datetime.now(UTC)
        for row in unresolved:
            age_minutes = max(0.0, (now - row.updated_at).total_seconds() / 60.0)
            synthetic = _is_synthetic_order_row(row)
            linked_positions = _linked_positions_for_intent(row, positions)
            db_fills = fills_by_client_order_id.get(row.client_order_id, [])
            open_order = None
            if row.venue_order_id:
                open_order = open_orders_by_venue.get(row.venue, {}).get(row.venue_order_id)
            venue_fill_rows: list[Any] = (
                fills_by_venue_order.get(row.venue, {}).get(row.venue_order_id or "", []) if row.venue_order_id else []
            )
            venue_status: str | None = None
            venue_error: str | None = None
            if row.venue_order_id and open_order is not None:
                venue_status = open_order.status.value
            elif row.venue_order_id and (client := clients.get(row.venue)) is not None:
                try:
                    report = await client.get_order(row.venue_order_id)
                    venue_status = report.status.value
                except Exception as exc:
                    venue_error = str(exc)
            safe_retire_reason = _safe_retire_reason(
                row=row,
                age_minutes=age_minutes,
                older_than_minutes=older_than_minutes,
                linked_position_count=len(linked_positions),
                db_fill_count=len(db_fills),
                venue_fill_count=len(venue_fill_rows),
                open_order_present=open_order is not None,
                venue_status=venue_status,
                venue_error=venue_error,
                synthetic=synthetic,
            )
            payload = {
                "client_order_id": row.client_order_id,
                "venue": row.venue,
                "route": row.route,
                "market_key": row.market_key,
                "token_id": row.token_id,
                "status": row.status,
                "venue_order_id": row.venue_order_id,
                "last_error": row.last_error,
                "created_at": row.created_at.isoformat(),
                "updated_at": row.updated_at.isoformat(),
                "age_minutes": round(age_minutes, 3),
                "synthetic": synthetic,
                "db_fill_count": len(db_fills),
                "venue_fill_count": len(venue_fill_rows),
                "open_order_present": open_order is not None,
                "linked_position_keys": linked_positions,
                "venue_status": venue_status,
                "venue_error": venue_error,
                "safe_retire_reason": safe_retire_reason,
                "safe_retire_candidate": safe_retire_reason is not None,
            }
            rows.append(payload)
            if safe_retire_reason is not None:
                safe_candidates.append(payload)
        return {
            "older_than_minutes": older_than_minutes,
            "unresolved_count": len(rows),
            "safe_retire_candidate_count": len(safe_candidates),
            "safe_retire_candidates": safe_candidates,
            "unresolved_orders": rows,
        }
    finally:
        await asyncio.gather(
            *(client.close() for client in clients.values() if client is not None),
            return_exceptions=True,
        )


async def _retire_safe_unresolved_orders(
    app_config: AppConfig,
    repository: ProductionRepository,
    *,
    older_than_minutes: float,
    apply: bool,
    config_path: str,
) -> dict[str, Any]:
    report = await _review_unresolved_orders(app_config, repository, older_than_minutes=older_than_minutes)
    candidates = list(report["safe_retire_candidates"])
    if not apply:
        report["applied"] = False
        report["confirm_hint"] = (
            f"arbitrage-admin --config {config_path} orders retire-safe-unresolved "
            f"--older-than-minutes {older_than_minutes:g} --confirm YES"
        )
        return report
    retired: list[dict[str, Any]] = []
    for item in candidates:
        await repository.update_order_intent(
            str(item["client_order_id"]),
            OrderIntentStatus.CANCELLED,
            venue_order_id=str(item["venue_order_id"]) if item["venue_order_id"] else None,
            error=f"operator safe retirement: {item['safe_retire_reason']}",
        )
        await repository.audit(
            "order_intent_retired_safe",
            {
                "client_order_id": item["client_order_id"],
                "venue": item["venue"],
                "route": item["route"],
                "venue_order_id": item["venue_order_id"],
                "safe_retire_reason": item["safe_retire_reason"],
            },
            correlation_id=str(item["client_order_id"]),
        )
        retired.append(
            {
                "client_order_id": item["client_order_id"],
                "venue": item["venue"],
                "venue_order_id": item["venue_order_id"],
                "safe_retire_reason": item["safe_retire_reason"],
            }
        )
    return {
        **report,
        "applied": True,
        "retired_count": len(retired),
        "retired_orders": retired,
    }


async def _reconcile(app_config: AppConfig, repository: ProductionRepository) -> None:
    predict_enabled = (
        app_config.enable_predict_fun
        and app_config.predict_fun.enabled
        and bool(app_config.predict_fun.api_key)
        and (
            app_config.routes.polymarket_predict
            or app_config.routes.predict_myriad
            or app_config.routes.predict_sx
        )
    )
    sx_enabled = app_config.enable_sx_bet and app_config.sx_bet.enabled and (
        app_config.routes.polymarket_sx or app_config.routes.sx_myriad or app_config.routes.predict_sx
    )
    myriad_enabled = app_config.myriad_markets.enabled and (
        app_config.routes.polymarket_myriad
        or app_config.routes.predict_myriad
        or app_config.routes.sx_myriad
    )
    clients: dict[str, BinaryMarketClient] = {"Polymarket": PolymarketClobClient(app_config.polymarket)}
    if predict_enabled:
        clients["Predict.fun"] = PredictFunApiClient(app_config.predict_fun)
    if sx_enabled:
        clients["SX Bet"] = SxBetApiClient(app_config.sx_bet)
    if myriad_enabled:
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


def _build_order_review_clients(
    app_config: AppConfig,
    venues: list[str] | tuple[str, ...],
) -> dict[str, BinaryMarketClient | None]:
    clients: dict[str, BinaryMarketClient | None] = {}
    for venue in venues:
        if venue == "Polymarket":
            clients[venue] = PolymarketClobClient(app_config.polymarket)
        elif venue == "Predict.fun":
            clients[venue] = PredictFunApiClient(app_config.predict_fun)
        elif venue == "SX Bet":
            clients[venue] = SxBetApiClient(app_config.sx_bet)
        elif venue == "Myriad":
            clients[venue] = MyriadClient(app_config.myriad_markets)
        else:
            clients[venue] = None
    return clients


def _is_synthetic_order_row(row: object) -> bool:
    market_key = str(getattr(row, "market_key", "") or "")
    token_id = str(getattr(row, "token_id", "") or "")
    return market_key.startswith(_SYNTHETIC_MARKET_KEY_PREFIXES) and token_id in _SYNTHETIC_TOKEN_IDS


def _linked_positions_for_intent(row: object, positions: Sequence[object]) -> list[str]:
    venue = str(getattr(row, "venue", "") or "")
    venue_order_id = str(getattr(row, "venue_order_id", "") or "")
    if not venue_order_id:
        return []
    linked: list[str] = []
    for position in positions:
        market = getattr(position, "market", None)
        if market is None:
            continue
        first_leg_matches = (
            venue == getattr(market, "venue_a_label", None)
            and getattr(position, "polymarket_order_id", None) == venue_order_id
        )
        second_leg_matches = (
            venue == getattr(market, "venue_b_label", None)
            and getattr(position, "predict_fun_order_id", None) == venue_order_id
        )
        if first_leg_matches:
            linked.append(position_key(market))
        elif second_leg_matches:
            linked.append(position_key(market))
    return linked


def _safe_retire_reason(
    *,
    row: object,
    age_minutes: float,
    older_than_minutes: float,
    linked_position_count: int,
    db_fill_count: int,
    venue_fill_count: int,
    open_order_present: bool,
    venue_status: str | None,
    venue_error: str | None,
    synthetic: bool,
) -> str | None:
    if age_minutes < older_than_minutes:
        return None
    if linked_position_count or db_fill_count or venue_fill_count or open_order_present:
        return None
    if venue_error is not None and "404" not in venue_error:
        return None
    status = str(getattr(row, "status", "") or "")
    venue_order_id = str(getattr(row, "venue_order_id", "") or "")
    safe_statuses = {
        OrderIntentStatus.PREPARED.value,
        OrderIntentStatus.SUBMITTING.value,
        OrderIntentStatus.ACKNOWLEDGED.value,
        OrderIntentStatus.CANCEL_PENDING.value,
        OrderIntentStatus.UNKNOWN.value,
        OrderIntentStatus.MANUAL_REVIEW.value,
    }
    if status not in safe_statuses:
        return None
    if synthetic:
        return "synthetic_artifact_without_fill_or_position_evidence"
    if not venue_order_id:
        return "missing_venue_order_id_without_fill_or_position_evidence"
    if venue_status == OrderIntentStatus.CANCELLED.value:
        return "venue_cancelled_without_fill_or_position_evidence"
    if venue_status in {"OPEN", "PARTIAL", "FILLED"}:
        return None
    if venue_error is not None and "404" in venue_error:
        return "venue_order_missing_without_fill_or_position_evidence"
    return None


def _mapping_json(mapping: MarketMapping) -> dict[str, object]:
    return {
        "mapping_id": mapping.mapping_id,
        "canonical_market_id": mapping.canonical_market_id,
        "left": f"{mapping.left_venue}:{mapping.left_market_id}",
        "right": f"{mapping.right_venue}:{mapping.right_market_id}",
        "route": _mapping_route(mapping),
        "status": mapping.status.value,
        "rules_fingerprint": mapping.rules_fingerprint,
        "match_strategy": mapping.match_strategy,
        "verified_at": mapping.verified_at.isoformat() if mapping.verified_at else None,
        "verified_by": mapping.verified_by,
    }


def _mapping_route(mapping: MarketMapping) -> str:
    return route_key(mapping.left_venue, mapping.right_venue)


def _enabled_route_names(config: AppConfig) -> tuple[str, ...]:
    routes: list[str] = []
    if config.routes.polymarket_myriad:
        routes.append("polymarket_myriad")
    if config.routes.polymarket_predict:
        routes.append("polymarket_predict")
    if config.routes.predict_myriad:
        routes.append("predict_myriad")
    if config.routes.predict_sx:
        routes.append("predict_sx")
    if config.routes.polymarket_sx:
        routes.append("polymarket_sx")
    if config.routes.sx_myriad:
        routes.append("sx_myriad")
    return tuple(routes)


def _register_second_leg_market_clients(
    markets: tuple[MarketSpec, ...] | list[MarketSpec],
    clients: dict[str, BinaryMarketClient],
) -> None:
    predict_client = clients.get("Predict.fun")
    sx_client = clients.get("SX Bet")
    for market in markets:
        if predict_client is not None:
            register_market = getattr(predict_client, "register_market", None)
            predict_token = _market_token_for_venue(market, "Predict.fun")
            predict_market_id = _market_market_id_for_venue(market, "Predict.fun")
            if callable(register_market) and predict_market_id and predict_token:
                register_market(
                    predict_token,
                    predict_market_id,
                    _market_side_for_venue(market, "Predict.fun"),
                    market.predict_fun_fee_rate_bps,
                )
        if sx_client is not None:
            register_market = getattr(sx_client, "register_market", None)
            sx_token = _market_token_for_venue(market, "SX Bet")
            sx_market_id = _market_market_id_for_venue(market, "SX Bet")
            if callable(register_market) and sx_market_id and sx_token:
                register_market(
                    sx_token,
                    sx_market_id,
                    _market_side_for_venue(market, "SX Bet"),
                )


def _candidate_markets_by_venue(markets: tuple[MarketSpec, ...] | list[MarketSpec]) -> dict[str, list[MarketSpec]]:
    candidates: dict[str, list[MarketSpec]] = {
        "Polymarket": [],
        "Myriad": [],
        "Predict.fun": [],
        "SX Bet": [],
    }
    for market in markets:
        if market.polymarket_token_id:
            candidates["Polymarket"].append(market)
        if market.myriad_market_id:
            candidates["Myriad"].append(market)
        if _market_token_for_venue(market, "Predict.fun"):
            candidates["Predict.fun"].append(market)
        if _market_token_for_venue(market, "SX Bet"):
            candidates["SX Bet"].append(market)
    ranked: dict[str, list[MarketSpec]] = {}
    for venue, venue_markets in candidates.items():
        if venue_markets:
            ranked[venue] = [
                item[1]
                for item in sorted(
                    enumerate(venue_markets),
                    key=lambda item: (_representative_market_score(item[1], venue), -item[0]),
                    reverse=True,
                )
            ]
    return ranked


def _representative_markets_by_venue(markets: tuple[MarketSpec, ...] | list[MarketSpec]) -> dict[str, MarketSpec]:
    return {
        venue: venue_markets[0]
        for venue, venue_markets in _candidate_markets_by_venue(markets).items()
        if venue_markets
    }


def _active_route_market_pairs(markets: tuple[MarketSpec, ...] | list[MarketSpec]) -> set[tuple[str, str, str, str]]:
    active_pairs: set[tuple[str, str, str, str]] = set()
    for market in markets:
        if (
            "polymarket_predict" in market.verified_routes
            and market.polymarket_market_id
            and market.predict_fun_market_id
        ):
            active_pairs.add(("Polymarket", market.polymarket_market_id, "Predict.fun", market.predict_fun_market_id))
            active_pairs.add(("Predict.fun", market.predict_fun_market_id, "Polymarket", market.polymarket_market_id))
        if "polymarket_myriad" in market.verified_routes and market.polymarket_market_id and market.myriad_market_id:
            active_pairs.add(("Polymarket", market.polymarket_market_id, "Myriad", market.myriad_market_id))
            active_pairs.add(("Myriad", market.myriad_market_id, "Polymarket", market.polymarket_market_id))
        if "predict_myriad" in market.verified_routes and market.predict_fun_market_id and market.myriad_market_id:
            active_pairs.add(("Predict.fun", market.predict_fun_market_id, "Myriad", market.myriad_market_id))
            active_pairs.add(("Myriad", market.myriad_market_id, "Predict.fun", market.predict_fun_market_id))
        if "predict_sx" in market.verified_routes and market.predict_fun_market_id:
            active_pairs.add(("Predict.fun", market.predict_fun_market_id, "SX Bet", market.predict_fun_market_id))
            active_pairs.add(("SX Bet", market.predict_fun_market_id, "Predict.fun", market.predict_fun_market_id))
        if "polymarket_sx" in market.verified_routes and market.polymarket_market_id and market.predict_fun_market_id:
            active_pairs.add(("Polymarket", market.polymarket_market_id, "SX Bet", market.predict_fun_market_id))
            active_pairs.add(("SX Bet", market.predict_fun_market_id, "Polymarket", market.polymarket_market_id))
        if "sx_myriad" in market.verified_routes and market.predict_fun_market_id and market.myriad_market_id:
            active_pairs.add(("SX Bet", market.predict_fun_market_id, "Myriad", market.myriad_market_id))
            active_pairs.add(("Myriad", market.myriad_market_id, "SX Bet", market.predict_fun_market_id))
    return active_pairs


def _has_active_stale_mappings(
    mappings: list[MarketMapping],
    markets: tuple[MarketSpec, ...] | list[MarketSpec],
) -> bool:
    active_pairs = _active_route_market_pairs(markets)
    if not active_pairs:
        return False
    for mapping in mappings:
        if (
            mapping.left_venue,
            mapping.left_market_id,
            mapping.right_venue,
            mapping.right_market_id,
        ) in active_pairs:
            return True
    return False


def _representative_market_score(market: MarketSpec, venue: str) -> tuple[int, int, int, int]:
    verified = int(bool(market.verified_routes))
    if venue == "Polymarket":
        return (
            verified,
            int(bool(market.polymarket_market_id or market.condition_id)),
            int(bool(market.condition_id)),
            int(bool(market.polymarket_token_id)),
        )
    if venue == "Myriad":
        return (
            verified,
            int(bool(market.myriad_condition_id and market.myriad_collateral_token)),
            int(bool(market.myriad_market_id)),
            int(bool(_market_token_for_venue(market, "Myriad"))),
        )
    if venue == "Predict.fun":
        return (
            verified,
            int(bool(market.predict_fun_market_id or market.polymarket_market_id)),
            int(bool(_market_token_for_venue(market, "Predict.fun"))),
            int(bool(market.predict_fun_fee_rate_bps is not None)),
        )
    if venue == "SX Bet":
        return (
            verified,
            int(bool(market.predict_fun_market_id)),
            int(bool(_market_token_for_venue(market, "SX Bet"))),
            0,
        )
    return (verified, 0, 0, 0)


def _market_token_for_venue(market: MarketSpec, venue: str) -> str:
    if venue == "Polymarket":
        return market.polymarket_token_id
    if venue == "Myriad":
        if market.venue_b_label == "Predict.fun":
            return myriad_execution_token_for_route(market, "predict_myriad") or ""
        if market.venue_b_label == "SX Bet":
            return myriad_execution_token_for_route(market, "sx_myriad") or ""
        return myriad_execution_token_for_route(market, "polymarket_myriad") or ""
    if venue in {"Predict.fun", "SX Bet"}:
        if venue == market.venue_a_label:
            return market.polymarket_token_id
        if venue == market.venue_b_label:
            return market.predict_fun_token_id
    return ""


def _market_market_id_for_venue(market: MarketSpec, venue: str) -> str:
    if venue == market.venue_a_label:
        if venue == "Polymarket":
            return market.polymarket_market_id or market.condition_id or ""
        return market.polymarket_market_id or market.predict_fun_market_id or market.condition_id or ""
    if venue == market.venue_b_label and venue != "Myriad":
        return market.predict_fun_market_id or ""
    if venue == "Myriad":
        return market.myriad_market_id or ""
    return ""


def _market_side_for_venue(market: MarketSpec, venue: str) -> BinarySide:
    if venue == "Polymarket":
        return market.polymarket_side
    if venue == "Myriad":
        token = ""
        if market.venue_b_label == "Predict.fun":
            token = myriad_execution_token_for_route(market, "predict_myriad") or ""
        if market.venue_b_label == "SX Bet":
            token = myriad_execution_token_for_route(market, "sx_myriad") or ""
        if token:
            return BinarySide(token.rsplit(":", 1)[1])
        return market.myriad_side
    if venue in {"Predict.fun", "SX Bet"}:
        if venue == market.venue_a_label:
            return market.polymarket_side
        if venue == market.venue_b_label:
            return market.predict_fun_side
    return market.predict_fun_side


def _automatic_redemption_status(client: BinaryMarketClient, venue: str) -> tuple[bool, str]:
    if client.supports_automatic_redemption():
        return True, "supported"
    if venue == "Predict.fun":
        return True, "not required for this venue"
    return False, "missing"


def _mapping_review_report(
    mappings: list[MarketMapping],
    enabled_routes: tuple[str, ...] = (),
    *,
    config_path: str = "config.production.json",
    operator: str = "operator",
    config: AppConfig | None = None,
    now: datetime | None = None,
    canonical_markets: dict[str, dict[str, object]] | None = None,
    venue_instruments: dict[str, dict[str, object]] | None = None,
    allow_structured_sports: bool = False,
) -> dict[str, object]:
    canonical_markets = canonical_markets or {}
    venue_instruments = venue_instruments or {}
    status_summary: dict[str, int] = {}
    route_summary: dict[str, dict[str, int]] = {}
    markets: dict[str, dict[str, object]] = {}
    approval_candidates: list[dict[str, object]] = []

    for mapping in mappings:
        route = _mapping_route(mapping)
        status_key = mapping.status.value
        status_summary[status_key] = status_summary.get(status_key, 0) + 1
        route_status = route_summary.setdefault(route, {})
        route_status[status_key] = route_status.get(status_key, 0) + 1
        market_entry = markets.setdefault(
            mapping.canonical_market_id,
            {
                "canonical_market_id": mapping.canonical_market_id,
                "by_status": {},
                "routes": set(),
                "live_ready_routes": set(),
                "mappings": [],
                "canonical": canonical_markets.get(mapping.canonical_market_id),
            },
        )
        market_status = market_entry["by_status"]
        assert isinstance(market_status, dict)
        market_status[status_key] = market_status.get(status_key, 0) + 1
        market_routes = market_entry["routes"]
        assert isinstance(market_routes, set)
        market_routes.add(route)
        if mapping.status is MappingStatus.VERIFIED:
            market_live_routes = market_entry["live_ready_routes"]
            assert isinstance(market_live_routes, set)
            market_live_routes.add(route)
        market_items = market_entry["mappings"]
        assert isinstance(market_items, list)
        market_items.append(
            {
                **_mapping_json(mapping),
                "left_instrument": venue_instruments.get(f"{mapping.left_venue}:{mapping.left_market_id}"),
                "right_instrument": venue_instruments.get(f"{mapping.right_venue}:{mapping.right_market_id}"),
            }
        )

    enabled_coverage: dict[str, dict[str, object]] = {}
    for route in enabled_routes:
        counts = route_summary.get(route, {})
        enabled_coverage[route] = {
            "has_verified": bool(counts.get(MappingStatus.VERIFIED.value, 0)),
            "verified": counts.get(MappingStatus.VERIFIED.value, 0),
            "candidate": counts.get(MappingStatus.CANDIDATE.value, 0),
            "stale": counts.get(MappingStatus.STALE.value, 0),
            "rejected": counts.get(MappingStatus.REJECTED.value, 0),
        }

    market_rows: list[dict[str, object]] = []
    for entry in markets.values():
        routes = sorted(cast(set[str], entry["routes"]))
        live_ready_routes = sorted(cast(set[str], entry["live_ready_routes"]))
        missing_enabled_routes = [route for route in enabled_routes if route not in live_ready_routes]
        mappings_json = sorted(
            cast(list[dict[str, object]], entry["mappings"]),
            key=lambda item: (
                str(item["route"]),
                str(item["status"]),
                str(item["mapping_id"]),
            ),
        )
        market_rows.append(
            {
                "canonical_market_id": entry["canonical_market_id"],
                "canonical": entry["canonical"],
                "by_status": entry["by_status"],
                "routes": routes,
                "live_ready_routes": live_ready_routes,
                "missing_enabled_routes": missing_enabled_routes,
                "ready_for_live": bool(live_ready_routes),
                "mappings": mappings_json,
            }
        )
        if missing_enabled_routes:
            route_candidates: dict[str, list[dict[str, object]]] = {}
            for item in mappings_json:
                route_candidates.setdefault(str(item["route"]), []).append(item)
            for route_name in missing_enabled_routes:
                items = route_candidates.get(route_name, [])
                pending_items = [
                    item
                    for item in items
                    if item["status"] in {MappingStatus.CANDIDATE.value, MappingStatus.STALE.value}
                ]
                rejected_items = [item for item in items if item["status"] == MappingStatus.REJECTED.value]
                exact_id_candidate = pending_items[0]["match_strategy"] == "exact_id" if pending_items else False
                structured_sports_candidate = bool(
                    pending_items
                    and allow_structured_sports
                    and _structured_sports_candidate_is_safe(entry["canonical"], pending_items[0])
                )
                if (
                    len(pending_items) == 1
                    and not rejected_items
                    and (exact_id_candidate or structured_sports_candidate)
                    and _mapping_candidate_within_auto_approval_scope(entry["canonical"], config, now=now)
                ):
                    pending = pending_items[0]
                    if structured_sports_candidate:
                        reason = "single_strict_structured_sports_candidate_for_polymarket_sx"
                    else:
                        reason = (
                            "single_exact_id_candidate_for_enabled_route"
                            if pending["status"] == MappingStatus.CANDIDATE.value
                            else "single_enriched_exact_id_stale_mapping_for_enabled_route"
                        )
                    approval_candidates.append(
                        {
                            "canonical_market_id": entry["canonical_market_id"],
                            "canonical": entry["canonical"],
                            "route": route_name,
                            "mapping_id": pending["mapping_id"],
                            "left": pending["left"],
                            "right": pending["right"],
                            "reason": reason,
                            "approve_command": (
                                f"arbitrage-admin --config {config_path} mappings approve "
                                f"{pending['mapping_id']} --operator {operator}"
                            ),
                        }
                    )
    market_rows.sort(
        key=lambda item: (
            not bool(item["live_ready_routes"]),
            str(item["canonical_market_id"]),
        )
    )

    return {
        "summary": {
            "total": len(mappings),
            "by_status": status_summary,
            "by_route": route_summary,
            "enabled_route_coverage": enabled_coverage,
            "approval_candidates": approval_candidates,
        },
        "markets": market_rows,
    }


def _structured_sports_candidate_is_safe(canonical: object, mapping: dict[str, object]) -> bool:
    if not isinstance(canonical, dict):
        return False
    if (
        mapping.get("route") != "polymarket_sx"
        or mapping.get("status") != MappingStatus.CANDIDATE.value
        or mapping.get("match_strategy") != "structured_sports"
    ):
        return False
    title = canonical.get("title")
    semantics = canonical.get("outcome_semantics")
    source = canonical.get("resolution_source")
    cutoff = canonical.get("cutoff_at")
    fingerprint = canonical.get("rules_fingerprint")
    if not all(isinstance(value, str) and value.strip() for value in (title, semantics, cutoff, fingerprint)):
        return False
    if not isinstance(source, str) or not source.startswith("Official "):
        return False
    assert isinstance(title, str)
    assert isinstance(semantics, str)
    if not all(marker in semantics for marker in ("Outcome one=", "; outcome two=", "; type=")):
        return False
    if sports_market_identity(title, outcome_semantics=semantics) is None:
        return False
    if mapping.get("rules_fingerprint") != fingerprint:
        return False
    left = mapping.get("left_instrument")
    right = mapping.get("right_instrument")
    if not isinstance(left, dict) or not isinstance(right, dict):
        return False
    if left.get("venue") != "Polymarket" or right.get("venue") != "SX Bet":
        return False
    if left.get("market_id") != str(mapping.get("left", "")).removeprefix("Polymarket:"):
        return False
    if right.get("market_id") != str(mapping.get("right", "")).removeprefix("SX Bet:"):
        return False
    if left.get("closes_at") != cutoff or right.get("closes_at") != cutoff:
        return False
    if left.get("rules_fingerprint") != fingerprint or right.get("rules_fingerprint") != fingerprint:
        return False
    token_fields = ("yes_token_id", "no_token_id")
    if any(
        not isinstance(instrument.get(field), str) or not instrument[field]
        for instrument in (left, right)
        for field in token_fields
    ):
        return False
    right_market_id = right["market_id"]
    return bool(
        right["yes_token_id"] == f"{right_market_id}:YES"
        and right["no_token_id"] == f"{right_market_id}:NO"
    )


def _mapping_candidate_within_auto_approval_scope(
    canonical: object,
    config: AppConfig | None,
    *,
    now: datetime | None = None,
) -> bool:
    if config is None:
        return True
    if not isinstance(canonical, dict):
        return False
    for field in ("resolution_source", "outcome_semantics"):
        value = canonical.get(field)
        if not isinstance(value, str) or not value.strip() or value.strip().lower() == "unknown":
            return False
    category = normalize_launch_category(str(canonical.get("category") or ""))
    allowed_categories = {
        normalized
        for value in config.categories_to_scan
        if (normalized := normalize_launch_category(value)) is not None
    }
    if category is None or (allowed_categories and category not in allowed_categories):
        return False
    cutoff_raw = canonical.get("cutoff_at")
    if not isinstance(cutoff_raw, str):
        return False
    try:
        cutoff = datetime.fromisoformat(cutoff_raw.replace("Z", "+00:00"))
    except ValueError:
        return False
    if cutoff.tzinfo is None:
        cutoff = cutoff.replace(tzinfo=UTC)
    reference = now or datetime.now(UTC)
    if reference.tzinfo is None:
        reference = reference.replace(tzinfo=UTC)
    remaining = cutoff.astimezone(UTC) - reference.astimezone(UTC)
    if remaining <= timedelta(0):
        return False
    if not config.market_horizon_filter_enabled:
        return True
    if category == "sports":
        return remaining <= timedelta(hours=config.max_sports_market_horizon_hours)
    if category == "crypto":
        return remaining <= timedelta(hours=config.max_crypto_market_horizon_hours)
    horizon_by_category = getattr(config, "max_market_horizon_hours_by_category", {})
    normalized_horizons = {
        normalized: float(hours)
        for raw_category, hours in horizon_by_category.items()
        if (normalized := normalize_launch_category(raw_category)) is not None
    }
    horizon_hours = normalized_horizons.get(category)
    return horizon_hours is not None and remaining <= timedelta(hours=horizon_hours)


def _migration_head_revision(config_path: str = "alembic.ini") -> str | None:
    try:
        return ScriptDirectory.from_config(Config(config_path)).get_current_head()
    except Exception:
        return None


def _approval_candidates_from_report(
    report: dict[str, object],
    *,
    route: str | None = None,
) -> list[dict[str, object]]:
    summary = report.get("summary")
    if not isinstance(summary, dict):
        return []
    candidates = summary.get("approval_candidates")
    if not isinstance(candidates, list):
        return []
    return [
        candidate
        for candidate in candidates
        if isinstance(candidate, dict) and (route is None or candidate.get("route") == route)
    ]


if __name__ == "__main__":
    main()
