from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import aiohttp

from arbitrage_engine.config import AppConfig, load_config, load_operator_env
from arbitrage_engine.database import ProductionRepository
from arbitrage_engine.production_audit import enabled_routes, funded_routes

_METRIC_RE = re.compile(
    r"^(?P<name>[a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{(?P<labels>[^}]*)\})?\s+(?P<value>\S+)$"
)
_LABEL_RE = re.compile(r'(?P<key>[a-zA-Z_][a-zA-Z0-9_]*)="(?P<value>(?:\\.|[^"\\])*)"')


@dataclass(frozen=True)
class _Service:
    config_path: str
    config: AppConfig
    repository: ProductionRepository
    routes: tuple[str, ...]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Capture route-specific signed technical-openability evidence in paused shadow mode"
    )
    parser.add_argument(
        "--config",
        action="append",
        required=True,
        help="Production config to observe; repeat for each runtime instance.",
    )
    parser.add_argument("--duration-seconds", type=int, default=7200)
    parser.add_argument("--poll-seconds", type=int, default=15)
    parser.add_argument("--database-timeout-seconds", type=float, default=45.0)
    parser.add_argument(
        "--stop-on",
        choices=("timeout", "all_routes_technical_openable"),
        default="all_routes_technical_openable",
    )
    parser.add_argument("--window-start", help="Resume a UTC window from this ISO-8601 timestamp.")
    parser.add_argument("--expected-release-sha")
    parser.add_argument("--artifact-dir", required=True)
    return parser


def _parse_time(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _decimal(value: Any) -> Decimal | None:
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None


def _mapping(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _parse_metrics(body: str) -> list[tuple[str, dict[str, str], float]]:
    result: list[tuple[str, dict[str, str], float]] = []
    for raw_line in body.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        match = _METRIC_RE.match(line)
        if match is None:
            continue
        try:
            value = float(match.group("value"))
        except ValueError:
            continue
        labels = {
            item.group("key"): item.group("value").replace(r'\"', '"')
            for item in _LABEL_RE.finditer(match.group("labels") or "")
        }
        result.append((match.group("name"), labels, value))
    return result


def _metric_value(
    metrics: list[tuple[str, dict[str, str], float]],
    name: str,
    **required_labels: str,
) -> float | None:
    for metric_name, labels, value in metrics:
        if metric_name == name and all(labels.get(key) == expected for key, expected in required_labels.items()):
            return value
    return None


def _execution_mode(metrics: list[tuple[str, dict[str, str], float]]) -> str | None:
    return next(
        (
            labels.get("mode")
            for name, labels, value in metrics
            if name == "arbitrage_execution_mode_info" and value == 1
        ),
        None,
    )


def _validate_evidence(
    evidence: dict[str, Any] | None,
    *,
    route: str,
    config: AppConfig,
    expected_release_sha: str,
    window_start: datetime,
    observed_at: datetime,
) -> dict[str, Any]:
    blockers: list[str] = []
    if evidence is None:
        return {"accepted": False, "blockers": ["evidence_missing"]}
    if str(evidence.get("route") or "") != route:
        blockers.append("route_mismatch")
    if str(evidence.get("runtime_instance_id") or "") != config.runtime_instance_id:
        blockers.append("runtime_instance_mismatch")
    if str(evidence.get("release_sha") or "") != expected_release_sha:
        blockers.append("release_sha_mismatch")

    captured_at = _parse_time(evidence.get("captured_at"))
    if captured_at is None:
        blockers.append("captured_at_invalid")
    else:
        if captured_at < window_start:
            blockers.append("evidence_predates_window")
        if captured_at > observed_at:
            blockers.append("evidence_from_future")

    market = _mapping(evidence.get("market"))
    cutoff = _parse_time(market.get("cutoff_at")) or _parse_time(market.get("expires_at"))
    if captured_at is None or cutoff is None or cutoff <= captured_at:
        blockers.append("market_not_current_at_capture")

    required_samples = config.shadow_preflight_samples
    samples = _list(evidence.get("samples"))
    try:
        completed_samples = int(evidence.get("completed_samples") or 0)
        recorded_required_samples = int(evidence.get("required_samples") or 0)
    except (TypeError, ValueError):
        completed_samples = 0
        recorded_required_samples = 0
    if (
        completed_samples < required_samples
        or recorded_required_samples < required_samples
        or len(samples) < required_samples
    ):
        blockers.append("insufficient_samples")
    required_depth = (
        Decimal(str(config.position_size_usd / 2.0))
        * Decimal(str(config.spread_policy.depth_buffer))
    )
    configured_threshold = Decimal(str(config.spread_policy.threshold_for(route)))
    configured_minimum_profit = Decimal(str(config.spread_policy.min_expected_profit_usd))
    for index, raw_sample in enumerate(samples[:required_samples], start=1):
        if not isinstance(raw_sample, dict):
            blockers.append(f"sample_{index}:invalid")
            continue
        if raw_sample.get("signed_preview_validated") is not True:
            blockers.append(f"sample_{index}:signature_missing")
        for leg_name in ("first_leg", "second_leg"):
            leg = _mapping(raw_sample.get(leg_name))
            executable_depth = _decimal(leg.get("executable_depth_usd"))
            signed_depth = _decimal(leg.get("signed_preview_depth_usd"))
            if executable_depth is None or executable_depth < required_depth:
                blockers.append(f"sample_{index}:{leg_name}:executable_depth")
            if signed_depth is None or signed_depth < required_depth:
                blockers.append(f"sample_{index}:{leg_name}:signed_depth")
            if leg.get("fee_verified") is not True:
                blockers.append(f"sample_{index}:{leg_name}:fee_unverified")
            if not str(leg.get("payload_fingerprint") or ""):
                blockers.append(f"sample_{index}:{leg_name}:payload_fingerprint_missing")
        economics = _mapping(raw_sample.get("economics"))
        expected_profit = _decimal(economics.get("expected_profit_usd"))
        evidence_minimum = _decimal(economics.get("minimum_profit_usd"))
        net_edge = _decimal(economics.get("net_edge"))
        evidence_threshold = _decimal(economics.get("dynamic_threshold"))
        chain_cost = _decimal(economics.get("fixed_chain_cost_usd"))
        required_profit = max(configured_minimum_profit, evidence_minimum or Decimal(0))
        required_edge = max(configured_threshold, evidence_threshold or Decimal(0))
        if expected_profit is None or expected_profit < required_profit:
            blockers.append(f"sample_{index}:profit_floor")
        if net_edge is None or net_edge < required_edge:
            blockers.append(f"sample_{index}:edge_floor")
        if chain_cost is None or chain_cost <= 0:
            blockers.append(f"sample_{index}:chain_cost_missing")
    return {
        "accepted": not blockers,
        "blockers": sorted(set(blockers)),
        "captured_at": evidence.get("captured_at"),
        "market_key": evidence.get("market_key"),
        "symbol": market.get("symbol"),
        "completed_samples": evidence.get("completed_samples"),
        "required_samples": required_samples,
    }


def _latch_route_state(
    current: dict[str, Any],
    candidate: dict[str, Any],
    *,
    observed_at: datetime,
) -> tuple[dict[str, Any], bool]:
    if current.get("accepted") is True:
        return current, False
    if candidate.get("accepted") is not True:
        return candidate, False
    return {**candidate, "first_observed_at": observed_at.isoformat()}, True


def _require_safe_runtime(
    candidate: dict[str, Any],
    runtime_status: dict[str, Any],
) -> dict[str, Any]:
    if runtime_status.get("safe_paused_shadow") is True:
        return candidate
    return {
        **candidate,
        "accepted": False,
        "blockers": sorted({*candidate.get("blockers", []), "runtime_not_safe_paused_shadow"}),
    }


async def _http_get(session: aiohttp.ClientSession, url: str) -> tuple[int | None, str]:
    try:
        async with session.get(url) as response:
            return int(response.status), await response.text(errors="replace")
    except (TimeoutError, aiohttp.ClientError, OSError) as exc:
        return None, str(exc)


async def _service_runtime_status(
    session: aiohttp.ClientSession,
    service: _Service,
) -> dict[str, Any]:
    port = service.config.observability_port
    live, ready, metrics_response = await asyncio.gather(
        _http_get(session, f"http://127.0.0.1:{port}/health/live"),
        _http_get(session, f"http://127.0.0.1:{port}/health/ready"),
        _http_get(session, f"http://127.0.0.1:{port}/metrics"),
    )
    metrics = _parse_metrics(metrics_response[1])
    mode = _execution_mode(metrics)
    risk_paused = _metric_value(metrics, "arbitrage_risk_paused")
    ready_metric = _metric_value(metrics, "arbitrage_ready")
    return {
        "runtime_instance_id": service.config.runtime_instance_id,
        "live_status": live[0],
        "ready_status": ready[0],
        "metrics_status": metrics_response[0],
        "mode": mode,
        "risk_paused": risk_paused,
        "ready_metric": ready_metric,
        "safe_paused_shadow": (
            live[0] == 200
            and ready[0] == 503
            and metrics_response[0] == 200
            and mode == "shadow"
            and risk_paused == 1
            and ready_metric == 0
        ),
        "best_net_spread_by_route": {
            route: _metric_value(metrics, "arbitrage_signal_best_net_spread", route=route)
            for route in service.routes
        },
    }


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def _append_json_line(path: Path, payload: Any) -> None:
    with path.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(payload, ensure_ascii=False) + "\n")


def _prepare_artifact_paths(value: str) -> tuple[Path, Path]:
    artifact_dir = Path(value).resolve()
    artifact_dir.mkdir(parents=True, exist_ok=True)
    return artifact_dir, artifact_dir / "status.jsonl"


async def main() -> int:
    args = build_parser().parse_args()
    if args.duration_seconds <= 0 or args.poll_seconds <= 0 or args.database_timeout_seconds <= 0:
        raise SystemExit("duration, poll interval, and database timeout must be positive")
    expected_release_sha = str(
        args.expected_release_sha or os.getenv("CI_VERIFIED_COMMIT_SHA") or ""
    ).strip()
    if not expected_release_sha:
        raise SystemExit("expected release SHA is required")
    injected_release_sha = str(os.getenv("CI_VERIFIED_COMMIT_SHA") or "").strip()
    if injected_release_sha and injected_release_sha != expected_release_sha:
        raise SystemExit("expected release SHA does not match CI_VERIFIED_COMMIT_SHA")
    window_start = _parse_time(args.window_start) if args.window_start else datetime.now(UTC)
    if window_start is None:
        raise SystemExit("window start must be a valid ISO-8601 timestamp")
    started_at = datetime.now(UTC)
    started_monotonic = time.monotonic()
    artifact_dir, status_path = _prepare_artifact_paths(args.artifact_dir)

    services: list[_Service] = []
    claimed_routes: set[str] = set()
    claimed_instances: set[str] = set()
    try:
        for config_path in args.config:
            load_operator_env(config_path)
            config = load_config(config_path)
            routes = funded_routes(config)
            duplicate_routes = claimed_routes.intersection(routes)
            if duplicate_routes:
                raise SystemExit(f"routes configured more than once: {sorted(duplicate_routes)}")
            if config.runtime_instance_id in claimed_instances:
                raise SystemExit(
                    f"runtime instance configured more than once: {config.runtime_instance_id}"
                )
            if not config.database_url:
                raise SystemExit(f"database URL missing for {config.runtime_instance_id}")
            claimed_routes.update(routes)
            claimed_instances.add(config.runtime_instance_id)
            repository = ProductionRepository(
                config.database_url,
                runtime_instance_id=config.runtime_instance_id,
                enabled_routes=enabled_routes(config),
            )
            if not await repository.ping():
                await repository.close()
                raise SystemExit(f"database unavailable for {config.runtime_instance_id}")
            services.append(_Service(config_path, config, repository, routes))
    except BaseException:
        await asyncio.gather(
            *(service.repository.close() for service in services),
            return_exceptions=True,
        )
        raise
    if not claimed_routes:
        raise SystemExit("at least one enabled route is required")

    route_states = {
        route: {"accepted": False, "blockers": ["evidence_missing"]}
        for route in sorted(claimed_routes)
    }
    continuity_failures: list[dict[str, Any]] = []
    safety_violation: str | None = None
    sample_count = 0
    timeout = aiohttp.ClientTimeout(total=10)
    try:
        async with aiohttp.ClientSession(
            timeout=timeout,
            connector=aiohttp.TCPConnector(limit=max(6, len(services) * 3)),
            trust_env=False,
        ) as session:
            deadline = started_monotonic + args.duration_seconds
            while time.monotonic() < deadline:
                sample_count += 1
                observed_at = datetime.now(UTC)
                runtime_statuses = await asyncio.gather(
                    *(_service_runtime_status(session, service) for service in services)
                )
                evidence_results = await asyncio.gather(
                    *(
                        asyncio.wait_for(
                            service.repository.latest_shadow_preflight_evidence_by_route(),
                            timeout=args.database_timeout_seconds,
                        )
                        for service in services
                    ),
                    return_exceptions=True,
                )
                sample_ok = all(status["safe_paused_shadow"] for status in runtime_statuses)
                database_errors: dict[str, str] = {}
                for status in runtime_statuses:
                    if status["mode"] is not None and status["mode"] != "shadow":
                        safety_violation = f"unsafe_execution_mode:{status['runtime_instance_id']}"
                    if status["risk_paused"] is not None and status["risk_paused"] != 1:
                        safety_violation = f"risk_not_paused:{status['runtime_instance_id']}"
                for service, evidence_result, runtime_status in zip(
                    services,
                    evidence_results,
                    runtime_statuses,
                    strict=True,
                ):
                    if isinstance(evidence_result, BaseException):
                        sample_ok = False
                        database_errors[service.config.runtime_instance_id] = (
                            f"{type(evidence_result).__name__}: {evidence_result}"
                        )[:500]
                        continue
                    for route in service.routes:
                        evidence = evidence_result.get(route)
                        candidate = _validate_evidence(
                            evidence,
                            route=route,
                            config=service.config,
                            expected_release_sha=expected_release_sha,
                            window_start=window_start,
                            observed_at=observed_at,
                        )
                        candidate = _require_safe_runtime(candidate, runtime_status)
                        route_states[route], newly_accepted = _latch_route_state(
                            route_states[route],
                            candidate,
                            observed_at=observed_at,
                        )
                        if newly_accepted and evidence is not None:
                            evidence_path = artifact_dir / f"evidence-{route}.json"
                            await asyncio.to_thread(_write_json, evidence_path, evidence)
                            route_states[route]["evidence_artifact"] = evidence_path.name
                if not sample_ok and len(continuity_failures) < 100:
                    continuity_failures.append(
                        {
                            "timestamp": observed_at.isoformat(),
                            "services": runtime_statuses,
                            "database_errors": database_errors,
                        }
                    )
                await asyncio.to_thread(
                    _append_json_line,
                    status_path,
                    {
                        "timestamp": observed_at.isoformat(),
                        "services": runtime_statuses,
                        "database_errors": database_errors,
                        "routes": route_states,
                        "safety_violation": safety_violation,
                    },
                )
                all_routes_accepted = all(state.get("accepted") is True for state in route_states.values())
                if safety_violation or (
                    args.stop_on == "all_routes_technical_openable" and all_routes_accepted
                ):
                    break
                await asyncio.sleep(min(args.poll_seconds, max(0.0, deadline - time.monotonic())))
    finally:
        await asyncio.gather(
            *(service.repository.close() for service in services),
            return_exceptions=True,
        )

    stopped_at = datetime.now(UTC)
    passed = safety_violation is None and all(
        state.get("accepted") is True for state in route_states.values()
    )
    report = {
        "schema_version": 1,
        "expected_release_sha": expected_release_sha,
        "window_start": window_start.isoformat(),
        "observer_started_at": started_at.isoformat(),
        "stopped_at": stopped_at.isoformat(),
        "requested_duration_seconds": args.duration_seconds,
        "observed_duration_seconds": time.monotonic() - started_monotonic,
        "poll_seconds": args.poll_seconds,
        "sample_count": sample_count,
        "stop_on": args.stop_on,
        "passed": passed,
        "timed_out": not passed and safety_violation is None,
        "safety_violation": safety_violation,
        "continuity_failure_count": len(continuity_failures),
        "continuity_failure_samples": continuity_failures,
        "routes": route_states,
    }
    await asyncio.to_thread(_write_json, artifact_dir / "report.json", report)
    print(json.dumps(report, indent=2, ensure_ascii=False))
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
