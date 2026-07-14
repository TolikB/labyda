from __future__ import annotations

import argparse
import asyncio
import json
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from arbitrage_engine.config import load_config, load_operator_env
from arbitrage_engine.database import ProductionRepository
from arbitrage_engine.market_mapping import route_key
from arbitrage_engine.models import position_key
from arbitrage_engine.production_audit import _http_probe, enabled_routes, probe_observability

_SYNTHETIC_MARKET_KEY_PREFIXES = ("integration:", "restart:")
_SYNTHETIC_TOKEN_IDS = {"integration-token", "restart-token"}


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _timestamp(value: datetime | None = None) -> str:
    return (value or _utc_now()).strftime("%Y%m%dT%H%M%SZ")


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def _write_text(path: Path, body: str) -> None:
    path.write_text(body, encoding="utf-8")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Observe a live canary window until the first real fill/open position")
    parser.add_argument("--config", default="config.production.json")
    parser.add_argument("--database-url")
    parser.add_argument("--duration-seconds", type=int, default=7200)
    parser.add_argument("--poll-seconds", type=int, default=15)
    parser.add_argument(
        "--stop-on",
        choices=("timeout", "first_fill_or_open_position"),
        default="timeout",
    )
    parser.add_argument(
        "--required-route",
        action="append",
        default=None,
        help="Only stop early when evidence is observed for this enabled route; repeat for multiple routes.",
    )
    parser.add_argument("--artifact-dir", required=True)
    parser.add_argument("--compose-cwd", default=".")
    parser.add_argument("--compose-service", action="append", default=None)
    return parser


def _run_command(command: list[str], cwd: Path | None = None) -> dict[str, Any]:
    try:
        completed = subprocess.run(
            command,
            cwd=str(cwd) if cwd is not None else None,
            capture_output=True,
            text=True,
            check=False,
        )
    except OSError as exc:
        return {"ok": False, "error": str(exc), "command": command}
    return {
        "ok": completed.returncode == 0,
        "returncode": completed.returncode,
        "stdout": completed.stdout,
        "stderr": completed.stderr,
        "command": command,
    }


def _default_compose_service(runtime_instance_id: str) -> str:
    mapping = {
        "clob_hft": "bot-clob-hft",
        "quote_arb": "bot-quote-arb",
    }
    return mapping.get(runtime_instance_id, "bot-quote-arb")


def _normalize_compose_services(runtime_instance_id: str, requested: list[str] | None) -> list[str]:
    selected = requested[:] if requested else [_default_compose_service(runtime_instance_id)]
    deduped: list[str] = []
    seen: set[str] = set()
    for service in selected:
        if service in seen:
            continue
        deduped.append(service)
        seen.add(service)
    return deduped


def _is_synthetic_order_payload(payload: dict[str, Any]) -> bool:
    if bool(payload.get("synthetic")):
        return True
    market_key = str(payload.get("market_key") or "")
    token_id = str(payload.get("token_id") or "")
    return market_key.startswith(_SYNTHETIC_MARKET_KEY_PREFIXES) and token_id in _SYNTHETIC_TOKEN_IDS


def _is_synthetic_position_key(value: str) -> bool:
    return str(value or "").startswith(_SYNTHETIC_MARKET_KEY_PREFIXES)


def _serialize_unresolved_intent(row: object) -> dict[str, Any]:
    market_key = str(getattr(row, "market_key", "") or "")
    token_id = str(getattr(row, "token_id", "") or "")
    return {
        "client_order_id": str(getattr(row, "client_order_id", "") or ""),
        "route": str(getattr(row, "route", "") or ""),
        "market_key": market_key,
        "venue": str(getattr(row, "venue", "") or ""),
        "token_id": token_id,
        "status": str(getattr(row, "status", "") or ""),
        "venue_order_id": str(getattr(row, "venue_order_id", "") or "") or None,
        "synthetic": market_key.startswith(_SYNTHETIC_MARKET_KEY_PREFIXES) and token_id in _SYNTHETIC_TOKEN_IDS,
    }


def _position_route(position: Any) -> str | None:
    try:
        return str(route_key(str(position.market.venue_a_label), str(position.market.venue_b_label)))
    except ValueError:
        return None


def _serialize_position_entry(
    entry_key: str,
    position: Any,
    *,
    started_at: datetime | None = None,
    baseline_keys: set[str] | None = None,
) -> dict[str, Any]:
    derived_market_key = position_key(position.market)
    opened_at = getattr(position, "opened_at", None)
    opened_after_start = bool(
        isinstance(opened_at, datetime)
        and started_at is not None
        and opened_at >= started_at
    )
    new_position = opened_after_start and entry_key not in (baseline_keys or set())
    return {
        "position_key": entry_key,
        "derived_market_key": derived_market_key,
        "symbol": position.market.symbol,
        "status": position.status,
        "opened_at": opened_at.isoformat() if isinstance(opened_at, datetime) else None,
        "first_venue": position.market.venue_a_label,
        "second_venue": position.market.venue_b_label,
        "route": _position_route(position),
        "synthetic": _is_synthetic_position_key(entry_key) or _is_synthetic_position_key(derived_market_key),
        "opened_after_started_at": opened_after_start,
        "new_in_window": new_position,
    }


def _route_evidence(
    routes: tuple[str, ...],
    fills: list[dict[str, Any]],
    positions: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    evidence = {
        route: {
            "real_fill_count": 0,
            "real_open_position_count": 0,
            "has_live_evidence": False,
        }
        for route in routes
    }
    for fill in fills:
        route = str(fill.get("route") or "")
        if route in evidence:
            evidence[route]["real_fill_count"] += 1
    for position in positions:
        route = str(position.get("route") or "")
        if route in evidence:
            evidence[route]["real_open_position_count"] += 1
    for item in evidence.values():
        item["has_live_evidence"] = bool(item["real_fill_count"] or item["real_open_position_count"])
    return evidence


def _validated_required_routes(enabled: tuple[str, ...], requested: list[str] | None) -> tuple[str, ...]:
    if not requested:
        return ()
    selected = tuple(dict.fromkeys(requested))
    unknown = sorted(set(selected).difference(enabled))
    if unknown:
        raise SystemExit(f"--required-route is not enabled in this config: {', '.join(unknown)}")
    return selected


async def _sample_http(base_url: str, run_dir: Path, stamp: str) -> dict[str, Any]:
    live = await asyncio.to_thread(_http_probe, f"{base_url}/health/live")
    ready = await asyncio.to_thread(_http_probe, f"{base_url}/health/ready")
    metrics = await asyncio.to_thread(_http_probe, f"{base_url}/metrics")
    for name, payload in (("live", live), ("ready", ready), ("metrics", metrics)):
        body = str(payload.get("body") or "")
        payload_path = run_dir / name / f"{stamp}.txt"
        await asyncio.to_thread(_write_text, payload_path, body)
    return {
        "live": {key: value for key, value in live.items() if key != "body"},
        "ready": {key: value for key, value in ready.items() if key != "body"},
        "metrics": {key: value for key, value in metrics.items() if key != "body"},
    }


async def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    load_operator_env(args.config)
    app_config = load_config(args.config)
    configured_routes = enabled_routes(app_config)
    required_routes = _validated_required_routes(configured_routes, args.required_route)
    database_url = args.database_url or app_config.database_url
    if not database_url:
        raise SystemExit("DATABASE_URL/database_url is required")

    repository = ProductionRepository(
        database_url,
        runtime_instance_id=app_config.runtime_instance_id,
        enabled_routes=configured_routes,
    )
    if not await repository.ping():
        await repository.close()
        raise SystemExit("database is unreachable")

    started_at = _utc_now()
    baseline_entries = await repository.load_position_entries()
    baseline_position_keys = {entry_key for entry_key, _ in baseline_entries}
    baseline_positions = [
        _serialize_position_entry(entry_key, position)
        for entry_key, position in baseline_entries
    ]
    run_dir = Path(args.artifact_dir) / _timestamp(started_at)
    (run_dir / "live").mkdir(parents=True, exist_ok=True)
    (run_dir / "ready").mkdir(parents=True, exist_ok=True)
    (run_dir / "metrics").mkdir(parents=True, exist_ok=True)
    base_url = f"http://127.0.0.1:{app_config.observability_port}"
    samples_path = run_dir / "samples.jsonl"
    compose_cwd = Path(args.compose_cwd)
    compose_services = _normalize_compose_services(app_config.runtime_instance_id, args.compose_service)

    observed_real_fill_or_open_position = False
    stop_reason = "timeout"
    latest_sample: dict[str, Any] | None = None
    recent_fills: list[dict[str, Any]] = []
    real_recent_fills: list[dict[str, Any]] = []
    positions: list[dict[str, Any]] = []
    real_positions: list[dict[str, Any]] = []
    unresolved_order_intents: list[dict[str, Any]] = []
    route_evidence: dict[str, dict[str, Any]] = _route_evidence(configured_routes, [], [])
    poll_count = 0

    try:
        deadline = started_at.timestamp() + args.duration_seconds
        while _utc_now().timestamp() < deadline:
            poll_count += 1
            stamp = _timestamp()
            http_snapshot = await _sample_http(base_url, run_dir, stamp)
            observability = await probe_observability("127.0.0.1", app_config.observability_port)
            runtime_audit = await repository.runtime_audit_snapshot()
            unresolved_order_intents = [
                _serialize_unresolved_intent(row) for row in await repository.unresolved_order_intents()
            ]
            recent_fills = await repository.recent_fills(since=started_at, limit=100)
            real_recent_fills = [fill for fill in recent_fills if not _is_synthetic_order_payload(fill)]
            position_entries = await repository.load_position_entries()
            positions = [
                _serialize_position_entry(
                    entry_key,
                    position,
                    started_at=started_at,
                    baseline_keys=baseline_position_keys,
                )
                for entry_key, position in position_entries
            ]
            real_positions = [
                item
                for item in positions
                if not bool(item["synthetic"]) and bool(item["new_in_window"])
            ]
            route_evidence = _route_evidence(configured_routes, real_recent_fills, real_positions)
            observed_routes = tuple(route for route, item in route_evidence.items() if item["has_live_evidence"])
            observed_real_fill_or_open_position = bool(observed_routes)
            required_route_evidence_observed = bool(observed_routes) and (
                set(required_routes).issubset(observed_routes) if required_routes else True
            )
            sample = {
                "timestamp": _utc_now().isoformat(),
                "observability": observability,
                "http_snapshot": http_snapshot,
                "runtime_audit": runtime_audit,
                "unresolved_order_intents": unresolved_order_intents,
                "unresolved_order_intent_count": len(unresolved_order_intents),
                "recent_fills": recent_fills,
                "recent_fill_count": len(recent_fills),
                "real_recent_fills": real_recent_fills,
                "real_recent_fill_count": len(real_recent_fills),
                "open_positions_count": len(positions),
                "open_positions": positions,
                "real_open_positions": real_positions,
                "real_open_position_count": len(real_positions),
                "route_evidence": route_evidence,
                "required_routes": list(required_routes),
                "required_route_evidence_observed": required_route_evidence_observed,
                "risk_paused": bool(((runtime_audit.get("risk_state") or {})).get("paused", False)),
                "reconciliation_failures": runtime_audit.get("reconciliation_failures", []),
            }
            with samples_path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(sample, ensure_ascii=False))
                handle.write("\n")
            latest_sample = sample
            if args.stop_on == "first_fill_or_open_position" and required_route_evidence_observed:
                observed_real_fill_or_open_position = True
                stop_reason = "first_fill_or_open_position"
                break
            await asyncio.sleep(max(1, args.poll_seconds))

        started_iso = started_at.isoformat()
        log_capture: dict[str, Any] = {}
        for compose_service in compose_services:
            logs = await asyncio.to_thread(
                _run_command,
                ["docker", "compose", "logs", "--since", started_iso, "--no-color", compose_service],
                compose_cwd,
            )
            _write_text(run_dir / f"{compose_service}.log", str(logs.get("stdout") or ""))
            _write_text(run_dir / f"{compose_service}.stderr.log", str(logs.get("stderr") or ""))
            log_capture[compose_service] = {key: value for key, value in logs.items() if key != "stdout"}

        report = {
            "config_path": args.config,
            "database_url_source": "override" if args.database_url else "config",
            "duration_seconds": args.duration_seconds,
            "poll_seconds": args.poll_seconds,
            "stop_on": args.stop_on,
            "started_at": started_iso,
            "stopped_at": _utc_now().isoformat(),
            "artifact_dir": str(run_dir),
            "compose_cwd": str(compose_cwd),
            "compose_service": compose_services[0],
            "compose_services": compose_services,
            "runtime_instance_id": app_config.runtime_instance_id,
            "enabled_routes": list(configured_routes),
            "required_routes": list(required_routes),
            "baseline_position_count": len(baseline_positions),
            "baseline_positions": baseline_positions,
            "poll_count": poll_count,
            "observed_real_fill_or_open_position": observed_real_fill_or_open_position,
            "stop_reason": stop_reason,
            "recent_fill_count": len(recent_fills),
            "real_recent_fill_count": len(real_recent_fills),
            "recent_fills": recent_fills,
            "real_recent_fills": real_recent_fills,
            "open_position_count": len(positions),
            "real_open_position_count": len(real_positions),
            "open_positions": positions,
            "real_open_positions": real_positions,
            "route_evidence": route_evidence,
            "unresolved_order_intent_count": len(unresolved_order_intents),
            "unresolved_order_intents": unresolved_order_intents,
            "latest_runtime_audit": latest_sample["runtime_audit"] if latest_sample is not None else None,
            "latest_observability": latest_sample["observability"] if latest_sample is not None else None,
            "latest_http_snapshot": latest_sample["http_snapshot"] if latest_sample is not None else None,
            "log_capture": log_capture,
            "result": "observed_real_fill_or_open_position" if observed_real_fill_or_open_position else "timeout",
        }
        _write_json(run_dir / "report.json", report)
        print(json.dumps(report, indent=2, ensure_ascii=False))
    finally:
        await repository.close()


if __name__ == "__main__":
    asyncio.run(main())
