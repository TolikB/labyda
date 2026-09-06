from __future__ import annotations

import argparse
import asyncio
import hashlib
import http.client
import json
import math
import os
import socket
import subprocess
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

from arbitrage_engine.config import load_config, load_operator_env
from arbitrage_engine.database import ProductionRepository
from arbitrage_engine.market_mapping import route_key
from arbitrage_engine.models import position_key
from arbitrage_engine.production_audit import _http_probe, enabled_routes, funded_routes, probe_observability
from arbitrage_engine.risk import GlobalRiskController

_SYNTHETIC_MARKET_KEY_PREFIXES = ("integration:", "restart:")
_SYNTHETIC_TOKEN_IDS = {"integration-token", "restart-token"}
_MAX_CONSECUTIVE_MONITORING_FAILURES = 2
_MAX_CONSECUTIVE_DATABASE_POLL_ERRORS = 2


def _json_default(value: Any) -> str:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _timestamp(value: datetime | None = None) -> str:
    return (value or _utc_now()).strftime("%Y%m%dT%H%M%SZ")


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False, default=_json_default),
        encoding="utf-8",
    )


def _write_text(path: Path, body: str) -> None:
    path.write_text(body, encoding="utf-8")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Observe a live canary window until the first real fill/open position")
    parser.add_argument("--config", default="config.production.json")
    parser.add_argument("--database-url")
    parser.add_argument("--duration-seconds", type=int, default=14400)
    parser.add_argument("--poll-seconds", type=int, default=15)
    parser.add_argument(
        "--database-poll-seconds",
        type=int,
        default=60,
        help="Refresh PostgreSQL evidence at this cadence while HTTP health is sampled at --poll-seconds.",
    )
    parser.add_argument(
        "--database-timeout-seconds",
        type=float,
        default=45.0,
        help="Bound PostgreSQL evidence refreshes without terminating the observer on a transient timeout.",
    )
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
    parser.add_argument(
        "--await-risk-resume",
        action="store_true",
        help="Arm and capture counter baselines while paused, then start the window after durable risk resume.",
    )
    parser.add_argument("--armed-file", help="Write an observer-armed marker after all baselines are captured.")
    parser.add_argument("--risk-resume-timeout-seconds", type=float, default=300.0)
    parser.add_argument(
        "--deadline-unix",
        type=float,
        help="Shared hard deadline established by the wrapper before durable risk resume.",
    )
    parser.add_argument(
        "--deadline-file",
        help="Shared hard-deadline file populated before durable risk resume.",
    )
    parser.add_argument("--pause-confirmation-timeout-seconds", type=float, default=120.0)
    parser.add_argument("--expected-config-sha256")
    parser.add_argument(
        "--expected-funded-route",
        action="append",
        default=None,
        help="Expected immutable funded allowlist; repeat for every route.",
    )
    return parser


def _config_integrity_snapshot(
    config_path: Path,
    *,
    expected_sha256: str,
    expected_funded_routes: tuple[str, ...],
) -> dict[str, Any]:
    try:
        actual_sha256 = hashlib.sha256(config_path.read_bytes()).hexdigest()
        current_routes = funded_routes(load_config(config_path))
        error = None
    except Exception as exc:
        actual_sha256 = None
        current_routes = ()
        error = f"{type(exc).__name__}: {exc}"
    return {
        "passed": bool(
            error is None
            and actual_sha256 == expected_sha256
            and current_routes == expected_funded_routes
        ),
        "expected_config_sha256": expected_sha256,
        "actual_config_sha256": actual_sha256,
        "expected_funded_routes": list(expected_funded_routes),
        "actual_funded_routes": list(current_routes),
        "error": error,
    }


def _accepted_preflight_counters(
    observability: dict[str, Any],
    routes: tuple[str, ...],
) -> dict[str, float]:
    metrics = observability.get("metrics") or {}
    counters = metrics.get("arbitrage_entry_preflight_accepted_total") or {}
    if not isinstance(counters, dict):
        counters = {}
    result: dict[str, float] = {}
    for route in routes:
        try:
            value = float(counters.get(route, 0.0))
        except (TypeError, ValueError):
            value = float("nan")
        result[route] = value
    return result


def _next_monitoring_failure_streak(
    current: int,
    *,
    http_snapshot: dict[str, Any],
    observability: dict[str, Any],
) -> int:
    http_ok = all(
        bool((http_snapshot.get(name) or {}).get("ok"))
        for name in ("live", "ready", "metrics")
    )
    observability_metrics = observability.get("metrics") or {}
    observability_ok = bool((observability.get("live") or {}).get("ok")) and bool(
        (observability_metrics.get("probe") or {}).get("ok")
    )
    return 0 if http_ok and observability_ok else current + 1


async def _await_durable_risk_resume(
    repository: ProductionRepository,
    *,
    timeout_seconds: float,
) -> datetime:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout_seconds
    while loop.time() < deadline:
        state = await repository.load_risk_state()
        if state is not None and state.get("paused") is False:
            return _utc_now()
        await asyncio.sleep(0.1)
    raise TimeoutError("durable risk resume was not observed before the observer timeout")


async def _wait_for_paused_entry_quiescence(
    *,
    host: str,
    port: int,
    timeout_seconds: float,
) -> tuple[dict[str, Any], bool]:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout_seconds
    latest: dict[str, Any] = {}
    consecutive_quiet_samples = 0
    while loop.time() < deadline:
        latest = await probe_observability(host, port)
        metrics = latest.get("metrics") or {}
        quiet = (
            metrics.get("arbitrage_risk_paused") == 1.0
            and metrics.get("arbitrage_ready") == 0.0
            and metrics.get("arbitrage_entry_submission_in_progress") == 0.0
        )
        consecutive_quiet_samples = consecutive_quiet_samples + 1 if quiet else 0
        if consecutive_quiet_samples >= 2:
            return latest, True
        await asyncio.sleep(0.25)
    return latest, False


async def _wait_for_unpaused_ready(
    *,
    host: str,
    port: int,
    runtime_start_time_seconds: float,
    timeout_seconds: float,
) -> datetime:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout_seconds
    while loop.time() < deadline:
        observability = await probe_observability(host, port)
        metrics = observability.get("metrics") or {}
        if (
            metrics.get("arbitrage_risk_paused") == 0.0
            and metrics.get("arbitrage_ready") == 1.0
            and metrics.get("arbitrage_entry_submission_in_progress") == 0.0
            and metrics.get("arbitrage_runtime_start_time_seconds") == runtime_start_time_seconds
        ):
            return _utc_now()
        await asyncio.sleep(0.25)
    raise TimeoutError("funded runtime did not become unpaused and ready before observer timeout")


async def _wait_for_shared_deadline_file(
    path: str,
    *,
    duration_seconds: int,
    timeout_seconds: float,
) -> float:
    loop = asyncio.get_running_loop()
    timeout_at = loop.time() + timeout_seconds
    while loop.time() < timeout_at:
        try:
            raw = await asyncio.to_thread(Path(path).read_text, encoding="utf-8")
            candidate = float(raw.strip())
        except (OSError, TypeError, ValueError):
            candidate = 0.0
        now = _utc_now().timestamp()
        # The wrapper first writes a fail-closed zero sentinel so the paused
        # canary can boot safely, then replaces it before durable risk resume.
        # Ignore the sentinel until the exact bounded window is published.
        if math.isfinite(candidate) and now < candidate <= now + duration_seconds + 5:
            return candidate
        await asyncio.sleep(0.1)
    raise TimeoutError("shared funded-canary deadline was not published before observer timeout")


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


class _UnixSocketHTTPConnection(http.client.HTTPConnection):
    def __init__(self, socket_path: Path) -> None:
        super().__init__("localhost")
        self._socket_path = str(socket_path)

    def connect(self) -> None:
        af_unix = getattr(socket, "AF_UNIX", None)
        if af_unix is None:
            raise OSError("Unix domain sockets are unavailable on this platform")
        self.sock = socket.socket(af_unix, socket.SOCK_STREAM)
        self.sock.connect(self._socket_path)


def _docker_api_get(socket_path: Path, path: str) -> tuple[int, bytes]:
    connection = _UnixSocketHTTPConnection(socket_path)
    try:
        connection.request("GET", path)
        response = connection.getresponse()
        return response.status, response.read()
    finally:
        connection.close()


def _decode_docker_log_stream(payload: bytes) -> str:
    if not payload:
        return ""
    offset = 0
    chunks: list[str] = []
    while offset + 8 <= len(payload):
        header = payload[offset : offset + 8]
        stream_type = header[0]
        frame_length = int.from_bytes(header[4:8], "big")
        frame_end = offset + 8 + frame_length
        if stream_type not in (0, 1, 2) or header[1:4] != b"\x00\x00\x00" or frame_end > len(payload):
            return payload.decode("utf-8", errors="replace")
        chunks.append(payload[offset + 8 : frame_end].decode("utf-8", errors="replace"))
        offset = frame_end
    if offset != len(payload):
        return payload.decode("utf-8", errors="replace")
    return "".join(chunks)


def _docker_socket_logs(
    *,
    socket_path: Path,
    compose_project: str,
    compose_service: str,
    started_at: datetime,
) -> dict[str, Any]:
    command = ["docker-engine-api", compose_project, compose_service]
    try:
        filters = json.dumps(
            {
                "label": [
                    f"com.docker.compose.project={compose_project}",
                    f"com.docker.compose.service={compose_service}",
                ]
            },
            separators=(",", ":"),
        )
        status, payload = _docker_api_get(
            socket_path,
            f"/containers/json?{urlencode({'all': '1', 'filters': filters})}",
        )
        if status != 200:
            raise RuntimeError(f"Docker container lookup returned HTTP {status}")
        containers = json.loads(payload)
        if not isinstance(containers, list) or not containers:
            raise RuntimeError(f"No Compose container found for service {compose_service}")
        selected = max(
            containers,
            key=lambda item: (
                str(item.get("State") or "") == "running",
                int(item.get("Created") or 0),
            ),
        )
        container_id = str(selected.get("Id") or "")
        if not container_id:
            raise RuntimeError(f"Docker container id is missing for service {compose_service}")
        query = urlencode(
            {
                "stdout": "1",
                "stderr": "1",
                "timestamps": "1",
                "since": str(int(started_at.timestamp())),
            }
        )
        status, payload = _docker_api_get(socket_path, f"/containers/{container_id}/logs?{query}")
        if status != 200:
            raise RuntimeError(f"Docker logs returned HTTP {status}")
    except (OSError, ValueError, RuntimeError, http.client.HTTPException) as exc:
        return {
            "ok": False,
            "error": str(exc),
            "command": command,
            "source": "docker_engine_api",
        }
    return {
        "ok": True,
        "returncode": 0,
        "stdout": _decode_docker_log_stream(payload),
        "stderr": "",
        "command": command,
        "source": "docker_engine_api",
        "container_id": container_id,
    }


def _capture_compose_logs(
    *,
    compose_cwd: Path,
    compose_service: str,
    started_at: datetime,
) -> dict[str, Any]:
    started_iso = started_at.isoformat()
    compose_result = _run_command(
        ["docker", "compose", "logs", "--since", started_iso, "--no-color", compose_service],
        compose_cwd,
    )
    if bool(compose_result.get("ok")):
        compose_result["source"] = "docker_compose_cli"
        return compose_result

    compose_project = os.getenv("COMPOSE_PROJECT_NAME") or compose_cwd.resolve().name
    socket_result = _docker_socket_logs(
        socket_path=Path(os.getenv("DOCKER_SOCKET_PATH") or "/var/run/docker.sock"),
        compose_project=compose_project,
        compose_service=compose_service,
        started_at=started_at,
    )
    socket_result["compose_cli_error"] = {
        key: value
        for key, value in compose_result.items()
        if key in {"error", "returncode", "stderr", "command"}
    }
    if bool(socket_result.get("ok")):
        return socket_result
    compose_result["docker_socket_fallback"] = {
        key: value for key, value in socket_result.items() if key != "stdout"
    }
    return compose_result


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


def _log_capture_summary(
    log_capture: dict[str, Any],
    expected_services: list[str],
) -> dict[str, Any]:
    failed_services = [
        service
        for service in expected_services
        if not bool((log_capture.get(service) or {}).get("ok"))
    ]
    return {
        "passed": not failed_services,
        "failure_count": len(failed_services),
        "failed_services": failed_services,
    }


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


def _route_statuses_from_ready_probe(probe: dict[str, Any]) -> dict[str, str]:
    try:
        payload = json.loads(str(probe.get("body") or ""))
    except (TypeError, json.JSONDecodeError):
        return {}
    if not isinstance(payload, dict):
        return {}
    discovery = payload.get("discovery")
    if not isinstance(discovery, dict):
        return {}
    route_statuses = discovery.get("route_statuses")
    if not isinstance(route_statuses, dict):
        return {}
    return {
        str(route): str(status)
        for route, status in route_statuses.items()
        if isinstance(route, str) and route and isinstance(status, str) and status
    }


async def _collect_database_state(
    repository: ProductionRepository,
    *,
    started_at: datetime,
    baseline_position_keys: set[str],
) -> dict[str, Any]:
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
        item for item in positions if not bool(item["synthetic"]) and bool(item["new_in_window"])
    ]
    return {
        "runtime_audit": runtime_audit,
        "unresolved_order_intents": unresolved_order_intents,
        "recent_fills": recent_fills,
        "real_recent_fills": real_recent_fills,
        "positions": positions,
        "real_positions": real_positions,
    }


def _database_error_payload(exc: Exception, *, stage: str, timestamp: datetime) -> dict[str, str]:
    return {
        "timestamp": timestamp.isoformat(),
        "stage": stage,
        "type": type(exc).__name__,
        "message": str(exc)[:500],
    }


async def _poll_database_state(
    repository: ProductionRepository,
    *,
    started_at: datetime,
    baseline_position_keys: set[str],
    timeout_seconds: float,
    stage: str,
) -> tuple[dict[str, Any] | None, dict[str, str] | None]:
    try:
        state = await asyncio.wait_for(
            _collect_database_state(
                repository,
                started_at=started_at,
                baseline_position_keys=baseline_position_keys,
            ),
            timeout=timeout_seconds,
        )
    except Exception as exc:
        return None, _database_error_payload(exc, stage=stage, timestamp=_utc_now())
    return state, None


async def _sample_http(base_url: str, run_dir: Path, stamp: str) -> dict[str, Any]:
    live = await asyncio.to_thread(_http_probe, f"{base_url}/health/live")
    ready = await asyncio.to_thread(_http_probe, f"{base_url}/health/ready")
    metrics = await asyncio.to_thread(_http_probe, f"{base_url}/metrics")
    for name, payload in (("live", live), ("ready", ready), ("metrics", metrics)):
        body = str(payload.get("body") or "")
        payload_path = run_dir / name / f"{stamp}.txt"
        await asyncio.to_thread(_write_text, payload_path, body)
    ready_summary = {key: value for key, value in ready.items() if key != "body"}
    ready_summary["route_statuses"] = _route_statuses_from_ready_probe(ready)
    return {
        "live": {key: value for key, value in live.items() if key != "body"},
        "ready": ready_summary,
        "metrics": {key: value for key, value in metrics.items() if key != "body"},
    }


async def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    if (
        args.duration_seconds <= 0
        or args.poll_seconds <= 0
        or args.database_poll_seconds <= 0
        or args.database_timeout_seconds <= 0
        or args.risk_resume_timeout_seconds <= 0
        or args.pause_confirmation_timeout_seconds <= 0
    ):
        raise SystemExit("duration, poll intervals, and database timeout must be positive")
    if args.armed_file and not args.await_risk_resume:
        raise SystemExit("--armed-file requires --await-risk-resume")
    if args.await_risk_resume and not args.armed_file:
        raise SystemExit("--await-risk-resume requires --armed-file")
    if args.deadline_unix is not None and args.deadline_file:
        raise SystemExit("--deadline-unix and --deadline-file are mutually exclusive")

    load_operator_env(args.config)
    expected_config_sha256 = str(args.expected_config_sha256 or "").strip().lower()
    expected_funded_routes = tuple(dict.fromkeys(args.expected_funded_route or ()))
    if bool(expected_config_sha256) != bool(expected_funded_routes):
        raise SystemExit(
            "--expected-config-sha256 and at least one --expected-funded-route must be supplied together"
        )
    if expected_config_sha256 and (
        len(expected_config_sha256) != 64
        or any(character not in "0123456789abcdef" for character in expected_config_sha256)
    ):
        raise SystemExit("--expected-config-sha256 must be a lowercase SHA-256 digest")
    app_config = load_config(args.config)
    configured_routes = funded_routes(app_config)
    config_path = await asyncio.to_thread(Path(args.config).resolve)
    config_integrity = (
        _config_integrity_snapshot(
            config_path,
            expected_sha256=expected_config_sha256,
            expected_funded_routes=expected_funded_routes,
        )
        if expected_config_sha256
        else None
    )
    if config_integrity is not None and not config_integrity["passed"]:
        raise SystemExit("funded canary configuration does not match its approved immutable digest/allowlist")
    required_routes = _validated_required_routes(configured_routes, args.required_route)
    database_url = args.database_url or app_config.database_url
    if not database_url:
        raise SystemExit("DATABASE_URL/database_url is required")

    repository = ProductionRepository(
        database_url,
        runtime_instance_id=app_config.runtime_instance_id,
        enabled_routes=enabled_routes(app_config),
    )
    if not await repository.ping():
        await repository.close()
        raise SystemExit("database is unreachable")
    await repository.configure_managed_reconciliation_venues(configured_routes)
    risk_controller = GlobalRiskController(
        app_config.max_daily_loss_usd,
        app_config.max_consecutive_api_errors,
        state_store=repository,
    )
    await risk_controller.initialize()

    armed_at = _utc_now()
    baseline_entries = await repository.load_position_entries()
    baseline_position_keys = {entry_key for entry_key, _ in baseline_entries}
    baseline_positions = [
        _serialize_position_entry(entry_key, position)
        for entry_key, position in baseline_entries
    ]
    base_url = f"http://127.0.0.1:{app_config.observability_port}"
    baseline_observability = await probe_observability("127.0.0.1", app_config.observability_port)
    baseline_metrics = baseline_observability.get("metrics") or {}
    if not bool((baseline_observability.get("live") or {}).get("ok")) or not bool(
        (baseline_metrics.get("probe") or {}).get("ok")
    ):
        await repository.close()
        raise SystemExit("observer could not capture a live metrics baseline")
    if args.await_risk_resume and baseline_metrics.get("arbitrage_risk_paused") != 1.0:
        await repository.close()
        raise SystemExit("observer must be armed while durable risk is paused")
    baseline_runtime_start = baseline_metrics.get("arbitrage_runtime_start_time_seconds")
    if baseline_runtime_start is None:
        await repository.close()
        raise SystemExit("runtime start metric is unavailable")
    baseline_accepted_preflights = _accepted_preflight_counters(
        baseline_observability,
        configured_routes,
    )
    if not all(math.isfinite(value) and value >= 0 for value in baseline_accepted_preflights.values()):
        await repository.close()
        raise SystemExit("accepted-preflight counter baseline is invalid")
    if args.armed_file:
        armed_path = Path(args.armed_file)
        armed_path.parent.mkdir(parents=True, exist_ok=True)
        _write_json(
            armed_path,
            {
                "armed_at": armed_at.isoformat(),
                "runtime_instance_id": app_config.runtime_instance_id,
                "runtime_start_time_seconds": baseline_runtime_start,
                "accepted_entry_preflight_baseline": baseline_accepted_preflights,
            },
        )
    if args.await_risk_resume:
        await _await_durable_risk_resume(
            repository,
            timeout_seconds=args.risk_resume_timeout_seconds,
        )
        started_at = await _wait_for_unpaused_ready(
            host="127.0.0.1",
            port=app_config.observability_port,
            runtime_start_time_seconds=float(baseline_runtime_start),
            timeout_seconds=args.risk_resume_timeout_seconds,
        )
    else:
        started_at = armed_at
    if args.deadline_file:
        deadline = await _wait_for_shared_deadline_file(
            args.deadline_file,
            duration_seconds=args.duration_seconds,
            timeout_seconds=args.risk_resume_timeout_seconds,
        )
    else:
        deadline = (
            float(args.deadline_unix)
            if args.deadline_unix is not None
            else started_at.timestamp() + args.duration_seconds
        )
    if deadline <= started_at.timestamp():
        await repository.close()
        raise SystemExit("hard deadline elapsed before the live window started")
    run_dir = Path(args.artifact_dir) / _timestamp(started_at)
    (run_dir / "live").mkdir(parents=True, exist_ok=True)
    (run_dir / "ready").mkdir(parents=True, exist_ok=True)
    (run_dir / "metrics").mkdir(parents=True, exist_ok=True)
    samples_path = run_dir / "samples.jsonl"
    compose_cwd = Path(args.compose_cwd)
    compose_services = _normalize_compose_services(app_config.runtime_instance_id, args.compose_service)

    observed_real_fill_or_open_position = False
    stop_reason = "timeout"
    latest_sample: dict[str, Any] | None = None
    runtime_audit: dict[str, Any] = {}
    recent_fills: list[dict[str, Any]] = []
    real_recent_fills: list[dict[str, Any]] = []
    positions: list[dict[str, Any]] = []
    real_positions: list[dict[str, Any]] = []
    unresolved_order_intents: list[dict[str, Any]] = []
    route_evidence: dict[str, dict[str, Any]] = _route_evidence(configured_routes, [], [])
    poll_count = 0
    database_poll_attempt_count = 0
    database_poll_error_count = 0
    consecutive_database_poll_errors = 0
    max_consecutive_database_poll_errors = 0
    database_poll_errors: list[dict[str, str]] = []
    last_database_poll_at: datetime | None = None
    next_database_poll_at = 0.0
    http_probe_failure_count = 0
    readiness_failure_count = 0
    required_route_readiness_failure_counts = {route: 0 for route in required_routes}
    consecutive_monitoring_failures = 0
    final_database_snapshot_ok = False
    accepted_preflight_last = dict(baseline_accepted_preflights)
    accepted_preflight_max = dict(baseline_accepted_preflights)
    accepted_preflight_counter_monotonic = True
    runtime_start_stable = True
    pause_confirmation_observability: dict[str, Any] = {}
    has_shared_hard_deadline = args.deadline_unix is not None or bool(args.deadline_file)
    pause_confirmation_passed = not has_shared_hard_deadline

    try:
        while _utc_now().timestamp() < deadline:
            poll_count += 1
            if expected_config_sha256:
                config_integrity = _config_integrity_snapshot(
                    config_path,
                    expected_sha256=expected_config_sha256,
                    expected_funded_routes=expected_funded_routes,
                )
                if not config_integrity["passed"]:
                    await risk_controller.pause("funded_canary_config_integrity_violation")
                    raise RuntimeError("funded canary configuration changed during the live window")
            stamp = _timestamp()
            http_snapshot = await _sample_http(base_url, run_dir, stamp)
            observability = await probe_observability("127.0.0.1", app_config.observability_port)
            consecutive_monitoring_failures = _next_monitoring_failure_streak(
                consecutive_monitoring_failures,
                http_snapshot=http_snapshot,
                observability=observability,
            )
            current_metrics = observability.get("metrics") or {}
            current_runtime_start = current_metrics.get("arbitrage_runtime_start_time_seconds")
            if current_runtime_start != baseline_runtime_start:
                runtime_start_stable = False
            if bool((current_metrics.get("probe") or {}).get("ok")):
                current_accepted = _accepted_preflight_counters(observability, configured_routes)
                for route, value in current_accepted.items():
                    if not math.isfinite(value) or value < accepted_preflight_last[route]:
                        accepted_preflight_counter_monotonic = False
                    accepted_preflight_max[route] = max(accepted_preflight_max[route], value)
                    accepted_preflight_last[route] = value
            if not bool((http_snapshot.get("live") or {}).get("ok")) or not bool(
                (http_snapshot.get("metrics") or {}).get("ok")
            ):
                http_probe_failure_count += 1
            if not bool((http_snapshot.get("ready") or {}).get("ok")):
                readiness_failure_count += 1
            ready_route_statuses = (http_snapshot.get("ready") or {}).get("route_statuses")
            if not isinstance(ready_route_statuses, dict):
                ready_route_statuses = {}
            required_route_statuses = {
                route: str(ready_route_statuses.get(route) or "missing")
                for route in required_routes
            }
            for route, status in required_route_statuses.items():
                if status != "ready_verified":
                    required_route_readiness_failure_counts[route] += 1
            if consecutive_monitoring_failures >= _MAX_CONSECUTIVE_MONITORING_FAILURES:
                raise RuntimeError("funded canary lost consecutive local health/metrics monitoring")

            loop_time = asyncio.get_running_loop().time()
            database_poll_attempted = loop_time >= next_database_poll_at
            database_poll_ok: bool | None = None
            database_poll_error: dict[str, str] | None = None
            if database_poll_attempted:
                next_database_poll_at = loop_time + args.database_poll_seconds
                database_poll_attempt_count += 1
                database_state, database_poll_error = await _poll_database_state(
                    repository,
                    started_at=started_at,
                    baseline_position_keys=baseline_position_keys,
                    timeout_seconds=args.database_timeout_seconds,
                    stage="poll",
                )
                if database_poll_error is not None:
                    database_poll_ok = False
                    database_poll_error_count += 1
                    consecutive_database_poll_errors += 1
                    max_consecutive_database_poll_errors = max(
                        max_consecutive_database_poll_errors,
                        consecutive_database_poll_errors,
                    )
                    database_poll_errors.append(database_poll_error)
                    if consecutive_database_poll_errors >= _MAX_CONSECUTIVE_DATABASE_POLL_ERRORS:
                        raise RuntimeError("funded canary lost consecutive PostgreSQL monitoring")
                else:
                    assert database_state is not None
                    database_poll_ok = True
                    consecutive_database_poll_errors = 0
                    last_database_poll_at = _utc_now()
                    runtime_audit = database_state["runtime_audit"]
                    unresolved_order_intents = database_state["unresolved_order_intents"]
                    recent_fills = database_state["recent_fills"]
                    real_recent_fills = database_state["real_recent_fills"]
                    positions = database_state["positions"]
                    real_positions = database_state["real_positions"]
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
                "required_route_statuses": required_route_statuses,
                "required_route_evidence_observed": required_route_evidence_observed,
                "database_poll": {
                    "attempted": database_poll_attempted,
                    "ok": database_poll_ok,
                    "error": database_poll_error,
                    "last_successful_at": last_database_poll_at.isoformat() if last_database_poll_at else None,
                    "state_age_seconds": (
                        max(0.0, (_utc_now() - last_database_poll_at).total_seconds())
                        if last_database_poll_at is not None
                        else None
                    ),
                },
                "risk_paused": bool(((runtime_audit.get("risk_state") or {})).get("paused", False)),
                "reconciliation_failures": runtime_audit.get("reconciliation_failures", []),
            }
            with samples_path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(sample, ensure_ascii=False, default=_json_default))
                handle.write("\n")
            latest_sample = sample
            if args.stop_on == "first_fill_or_open_position" and required_route_evidence_observed:
                observed_real_fill_or_open_position = True
                stop_reason = "first_fill_or_open_position"
                break
            await asyncio.sleep(max(1, args.poll_seconds))

        if expected_config_sha256:
            config_integrity = _config_integrity_snapshot(
                config_path,
                expected_sha256=expected_config_sha256,
                expected_funded_routes=expected_funded_routes,
            )
            if not config_integrity["passed"]:
                await risk_controller.pause("funded_canary_config_integrity_violation")
                raise RuntimeError("funded canary configuration changed before final evidence capture")

        if has_shared_hard_deadline:
            pause_confirmation_observability, pause_confirmation_passed = (
                await _wait_for_paused_entry_quiescence(
                    host="127.0.0.1",
                    port=app_config.observability_port,
                    timeout_seconds=args.pause_confirmation_timeout_seconds,
                )
            )
            if not pause_confirmation_passed:
                raise RuntimeError("hard-deadline risk pause did not reach entry quiescence")
            pause_metrics = pause_confirmation_observability.get("metrics") or {}
            if pause_metrics.get("arbitrage_runtime_start_time_seconds") != baseline_runtime_start:
                runtime_start_stable = False
            current_accepted = _accepted_preflight_counters(
                pause_confirmation_observability,
                configured_routes,
            )
            for route, value in current_accepted.items():
                if not math.isfinite(value) or value < accepted_preflight_last[route]:
                    accepted_preflight_counter_monotonic = False
                accepted_preflight_max[route] = max(accepted_preflight_max[route], value)
                accepted_preflight_last[route] = value

        for final_attempt in range(1, 4):
            database_poll_attempt_count += 1
            database_state, final_database_error = await _poll_database_state(
                repository,
                started_at=started_at,
                baseline_position_keys=baseline_position_keys,
                timeout_seconds=args.database_timeout_seconds,
                stage=f"final_{final_attempt}",
            )
            if final_database_error is not None:
                database_poll_error_count += 1
                database_poll_errors.append(final_database_error)
                if final_attempt < 3:
                    await asyncio.sleep(2)
            else:
                assert database_state is not None
                final_database_snapshot_ok = True
                last_database_poll_at = _utc_now()
                runtime_audit = database_state["runtime_audit"]
                unresolved_order_intents = database_state["unresolved_order_intents"]
                recent_fills = database_state["recent_fills"]
                real_recent_fills = database_state["real_recent_fills"]
                positions = database_state["positions"]
                real_positions = database_state["real_positions"]
                route_evidence = _route_evidence(configured_routes, real_recent_fills, real_positions)
                observed_real_fill_or_open_position = any(
                    item["has_live_evidence"] for item in route_evidence.values()
                )
                break

        started_iso = started_at.isoformat()
        log_capture: dict[str, Any] = {}
        for compose_service in compose_services:
            logs = await asyncio.to_thread(
                _capture_compose_logs,
                compose_cwd=compose_cwd,
                compose_service=compose_service,
                started_at=started_at,
            )
            _write_text(run_dir / f"{compose_service}.log", str(logs.get("stdout") or ""))
            _write_text(run_dir / f"{compose_service}.stderr.log", str(logs.get("stderr") or ""))
            log_capture[compose_service] = {key: value for key, value in logs.items() if key != "stdout"}
        log_capture_summary = _log_capture_summary(log_capture, compose_services)

        stopped_at = _utc_now()
        observed_duration_seconds = max(0.0, (stopped_at - started_at).total_seconds())
        window_completed = bool(stop_reason == "timeout" and stopped_at.timestamp() >= deadline)
        accepted_entry_preflights = {
            route: {
                "baseline": baseline_accepted_preflights[route],
                "final": accepted_preflight_last[route],
                "maximum_observed": accepted_preflight_max[route],
                "delta": accepted_preflight_max[route] - baseline_accepted_preflights[route],
                "monotonic": accepted_preflight_counter_monotonic,
            }
            for route in configured_routes
        }
        report = {
            "config_path": args.config,
            "database_url_source": "override" if args.database_url else "config",
            "duration_seconds": args.duration_seconds,
            "poll_seconds": args.poll_seconds,
            "database_poll_seconds": args.database_poll_seconds,
            "database_timeout_seconds": args.database_timeout_seconds,
            "stop_on": args.stop_on,
            "armed_at": armed_at.isoformat(),
            "started_at": started_iso,
            "stopped_at": stopped_at.isoformat(),
            "observed_duration_seconds": observed_duration_seconds,
            "scheduled_live_window_seconds": max(0.0, deadline - started_at.timestamp()),
            "hard_deadline_unix": deadline if has_shared_hard_deadline else None,
            "hard_deadline_source": (
                "file" if args.deadline_file else "argument" if args.deadline_unix is not None else None
            ),
            "window_completed": window_completed,
            "artifact_dir": str(run_dir),
            "compose_cwd": str(compose_cwd),
            "compose_service": compose_services[0],
            "compose_services": compose_services,
            "runtime_instance_id": app_config.runtime_instance_id,
            "enabled_routes": list(enabled_routes(app_config)),
            "funded_routes": list(configured_routes),
            "config_integrity": config_integrity,
            "required_routes": list(required_routes),
            "baseline_position_count": len(baseline_positions),
            "baseline_positions": baseline_positions,
            "poll_count": poll_count,
            "database_poll_attempt_count": database_poll_attempt_count,
            "database_poll_error_count": database_poll_error_count,
            "database_poll_errors": database_poll_errors,
            "max_consecutive_database_poll_errors": max_consecutive_database_poll_errors,
            "final_database_snapshot_ok": final_database_snapshot_ok,
            "monitoring_continuity": {
                "passed": bool(
                    final_database_snapshot_ok
                    and window_completed
                    and database_poll_error_count == 0
                    and http_probe_failure_count == 0
                    and readiness_failure_count == 0
                    and all(
                        count == 0
                        for count in required_route_readiness_failure_counts.values()
                    )
                    and accepted_preflight_counter_monotonic
                    and runtime_start_stable
                    and pause_confirmation_passed
                    and bool(log_capture_summary["passed"])
                ),
                "http_probe_failure_count": http_probe_failure_count,
                "readiness_failure_count": readiness_failure_count,
                "required_route_readiness_failure_counts": required_route_readiness_failure_counts,
                "database_poll_error_count": database_poll_error_count,
                "final_database_snapshot_ok": final_database_snapshot_ok,
                "window_completed": window_completed,
                "accepted_preflight_counter_monotonic": accepted_preflight_counter_monotonic,
                "runtime_start_stable": runtime_start_stable,
                "pause_confirmation_passed": pause_confirmation_passed,
                "log_capture_ok": log_capture_summary["passed"],
                "log_capture_failure_count": log_capture_summary["failure_count"],
                "log_capture_failed_services": log_capture_summary["failed_services"],
            },
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
            "accepted_entry_preflights": accepted_entry_preflights,
            "unresolved_order_intent_count": len(unresolved_order_intents),
            "unresolved_order_intents": unresolved_order_intents,
            "latest_runtime_audit": runtime_audit or None,
            "latest_observability": (
                pause_confirmation_observability
                if pause_confirmation_observability
                else latest_sample["observability"]
                if latest_sample is not None
                else None
            ),
            "pause_confirmation_observability": pause_confirmation_observability or None,
            "latest_http_snapshot": latest_sample["http_snapshot"] if latest_sample is not None else None,
            "log_capture": log_capture,
            "log_capture_summary": log_capture_summary,
            "result": "observed_real_fill_or_open_position" if observed_real_fill_or_open_position else "timeout",
        }
        _write_json(run_dir / "report.json", report)
        print(json.dumps(report, indent=2, ensure_ascii=False, default=_json_default))
    finally:
        await repository.close()


if __name__ == "__main__":
    asyncio.run(main())
