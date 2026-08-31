from __future__ import annotations

import argparse
import asyncio
import json
import math
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import aiohttp

from arbitrage_engine.config import load_config, load_operator_env
from arbitrage_engine.production_audit import enabled_routes

_SAMPLE_BUCKETS = (0.0, 0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.02, 0.05, 0.1)
_METRIC_RE = re.compile(r"^(?P<name>[a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{(?P<labels>[^}]*)\})?\s+(?P<value>\S+)$")
_LABEL_RE = re.compile(r'(?P<key>[a-zA-Z_][a-zA-Z0-9_]*)="(?P<value>(?:\\.|[^"\\])*)"')


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Calibrate route adverse-move p95 from a fail-closed shadow window")
    parser.add_argument("--config", required=True)
    parser.add_argument("--duration-seconds", type=int, default=3600)
    parser.add_argument("--poll-seconds", type=int, default=15)
    parser.add_argument("--min-valid-evaluations", type=int, default=10_000)
    parser.add_argument("--artifact-dir", required=True)
    parser.add_argument("--write-config", action="store_true")
    parser.add_argument(
        "--require-configured-reserve",
        action="store_true",
        help="Fail if the CI-tracked route reserve is missing or below the observed p95.",
    )
    return parser


async def _http_get(session: aiohttp.ClientSession, url: str) -> tuple[int | None, str]:
    try:
        async with session.get(url) as response:
            return int(response.status), await response.text(errors="replace")
    except (TimeoutError, aiohttp.ClientError, OSError) as exc:
        return None, str(exc)


def _parse_labels(raw: str | None) -> dict[str, str]:
    if not raw:
        return {}
    return {match.group("key"): match.group("value").replace(r'\"', '"') for match in _LABEL_RE.finditer(raw)}


def parse_prometheus(body: str) -> list[tuple[str, dict[str, str], float]]:
    parsed: list[tuple[str, dict[str, str], float]] = []
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
        parsed.append((match.group("name"), _parse_labels(match.group("labels")), value))
    return parsed


def _route_counters(metrics: list[tuple[str, dict[str, str], float]]) -> dict[str, float]:
    return {
        labels["route"]: value
        for name, labels, value in metrics
        if name == "arbitrage_calibration_valid_evaluations_total" and "route" in labels
    }


def _route_buckets(metrics: list[tuple[str, dict[str, str], float]]) -> dict[str, dict[float, float]]:
    buckets: dict[str, dict[float, float]] = {}
    for name, labels, value in metrics:
        if name != "arbitrage_calibration_adverse_move_pct_bucket" or "route" not in labels:
            continue
        raw_bound = labels.get("le")
        if raw_bound is None:
            continue
        bound = math.inf if raw_bound == "+Inf" else float(raw_bound)
        buckets.setdefault(labels["route"], {})[bound] = value
    return buckets


def effective_execution_mode(metrics: list[tuple[str, dict[str, str], float]]) -> str | None:
    for name, labels, value in metrics:
        if name == "arbitrage_execution_mode_info" and value == 1 and labels.get("mode"):
            return labels["mode"]
    return None


def _metric_scalar(
    metrics: list[tuple[str, dict[str, str], float]],
    metric_name: str,
) -> float | None:
    return next(
        (value for name, labels, value in metrics if name == metric_name and not labels),
        None,
    )


def runtime_health_sample(
    live: tuple[int | None, str],
    ready: tuple[int | None, str],
    metrics: tuple[int | None, str],
    *,
    expected_runtime_instance_id: str | None = None,
    expected_runtime_start_time_seconds: float | None = None,
) -> dict[str, Any]:
    parsed_metrics = parse_prometheus(metrics[1])
    sample_mode = effective_execution_mode(parsed_metrics)
    risk_paused = _metric_scalar(parsed_metrics, "arbitrage_risk_paused")
    ready_metric = _metric_scalar(parsed_metrics, "arbitrage_ready")
    runtime_start_time_seconds = _metric_scalar(
        parsed_metrics,
        "arbitrage_runtime_start_time_seconds",
    )
    ready_payload: dict[str, Any] | None = None
    try:
        candidate = json.loads(ready[1])
        if isinstance(candidate, dict):
            ready_payload = candidate
    except (TypeError, ValueError, json.JSONDecodeError):
        pass
    raw_reasons = ready_payload.get("reasons", []) if ready_payload is not None else []
    ready_reasons = [str(reason) for reason in raw_reasons] if isinstance(raw_reasons, list) else []
    ready_runtime_instance_id = (
        str(ready_payload.get("runtime_instance_id"))
        if ready_payload is not None and ready_payload.get("runtime_instance_id") is not None
        else None
    )
    instance_matches = (
        expected_runtime_instance_id is None or ready_runtime_instance_id == expected_runtime_instance_id
    )
    runtime_start_matches = (
        expected_runtime_start_time_seconds is None
        or runtime_start_time_seconds == expected_runtime_start_time_seconds
    )
    ready_shadow = (
        ready[0] == 200
        and ready_payload is not None
        and ready_payload.get("status") == "ready"
        and not ready_reasons
        and risk_paused == 0
        and ready_metric == 1
    )
    paused_shadow = (
        ready[0] == 503
        and ready_payload is not None
        and ready_payload.get("status") == "not_ready"
        and bool(ready_reasons)
        and all(reason.startswith("risk_paused:") for reason in ready_reasons)
        and risk_paused == 1
        and ready_metric == 0
    )
    sample_ok = (
        live[0] == 200
        and instance_matches
        and runtime_start_matches
        and metrics[0] == 200
        and sample_mode == "shadow"
        and (ready_shadow or paused_shadow)
    )
    return {
        "live_status": live[0],
        "ready_status": ready[0],
        "ready_runtime_instance_id": ready_runtime_instance_id,
        "ready_runtime_instance_matches": instance_matches,
        "runtime_start_time_seconds": runtime_start_time_seconds,
        "runtime_start_time_matches": runtime_start_matches,
        "ready_reasons": ready_reasons,
        "ready_payload_valid": ready_payload is not None,
        "metrics_status": metrics[0],
        "execution_mode": sample_mode,
        "risk_paused": risk_paused,
        "ready_metric": ready_metric,
        "safe_paused_shadow": paused_shadow,
        "ok": sample_ok,
    }


def calibration_result(
    routes: tuple[str, ...],
    start_metrics: list[tuple[str, dict[str, str], float]],
    end_metrics: list[tuple[str, dict[str, str], float]],
    minimum_evaluations: int,
) -> dict[str, Any]:
    start_counts = _route_counters(start_metrics)
    end_counts = _route_counters(end_metrics)
    start_buckets = _route_buckets(start_metrics)
    end_buckets = _route_buckets(end_metrics)
    route_results: dict[str, dict[str, Any]] = {}
    passed = True
    for route in routes:
        valid_count = end_counts.get(route, 0.0) - start_counts.get(route, 0.0)
        cumulative_deltas = {
            bound: count - start_buckets.get(route, {}).get(bound, 0.0)
            for bound, count in end_buckets.get(route, {}).items()
        }
        observation_count = cumulative_deltas.get(math.inf, 0.0)
        p95: float | None = None
        if observation_count > 0:
            target = observation_count * 0.95
            for bound in sorted(cumulative_deltas):
                if bound != math.inf and cumulative_deltas[bound] >= target:
                    p95 = max(_SAMPLE_BUCKETS[1], bound)
                    break
        blockers: list[str] = []
        if valid_count < minimum_evaluations:
            blockers.append(f"valid_evaluations_below_{minimum_evaluations}")
        if observation_count <= 0:
            blockers.append("no_latency_horizon_observations")
        if p95 is None:
            blockers.append("p95_exceeds_calibration_histogram")
        if valid_count < 0 or observation_count < 0:
            blockers.append("runtime_metrics_reset_during_window")
        route_results[route] = {
            "valid_evaluation_count": int(max(0.0, valid_count)),
            "adverse_move_observation_count": int(max(0.0, observation_count)),
            "adverse_move_p95_pct": p95,
            "blockers": blockers,
            "passed": not blockers,
        }
        passed = passed and not blockers
    return {"passed": passed, "routes": route_results}


def validate_configured_reserves(
    result: dict[str, Any],
    configured_by_route: dict[str, float],
) -> dict[str, Any]:
    """Attach the release reserve comparison and fail closed on under-reserved routes."""
    passed = bool(result["passed"])
    routes = result["routes"]
    assert isinstance(routes, dict)
    for route, raw_details in routes.items():
        assert isinstance(raw_details, dict)
        configured = float(configured_by_route.get(route, 0.0))
        observed = raw_details.get("adverse_move_p95_pct")
        blockers = raw_details["blockers"]
        assert isinstance(blockers, list)
        if configured <= 0:
            blockers.append("route_specific_adverse_move_reserve_missing")
        elif isinstance(observed, (int, float)) and configured < float(observed):
            blockers.append("configured_adverse_move_reserve_below_observed_p95")
        raw_details["configured_adverse_move_reserve_pct"] = configured
        raw_details["passed"] = not blockers
        passed = passed and not blockers
    result["passed"] = passed
    return result


def window_continuity_blockers(
    *,
    final_sample: dict[str, Any],
    expected_runtime_start_time_seconds: float,
    continuity_ok: bool,
) -> list[str]:
    blockers: list[str] = []
    runtime_identity_blocked = False
    metrics_status = final_sample.get("metrics_status")
    runtime_start_time_seconds = final_sample.get("runtime_start_time_seconds")
    if metrics_status != 200:
        blockers.append("final_metrics_unavailable")
    elif (
        not isinstance(runtime_start_time_seconds, (int, float))
        or not math.isfinite(runtime_start_time_seconds)
        or runtime_start_time_seconds <= 0
    ):
        blockers.append("runtime_start_metric_unavailable")
        runtime_identity_blocked = True
    elif runtime_start_time_seconds != expected_runtime_start_time_seconds:
        blockers.append("runtime_restarted_during_window")
        runtime_identity_blocked = True
    if not continuity_ok or (not bool(final_sample.get("ok")) and not runtime_identity_blocked):
        blockers.append("runtime_health_or_shadow_mode_interrupted")
    return blockers


def _write_calibration(config_path: Path, result: dict[str, Any]) -> None:
    payload = json.loads(config_path.read_text(encoding="utf-8"))
    spread_policy = payload.setdefault("spread_policy", {})
    spread_policy["adverse_move_p95_pct_by_route"] = {
        route: details["adverse_move_p95_pct"]
        for route, details in result["routes"].items()
        if details["passed"]
    }
    temporary = config_path.with_suffix(config_path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    temporary.replace(config_path)


async def main() -> None:
    args = build_parser().parse_args()
    if args.duration_seconds <= 0 or args.poll_seconds <= 0 or args.min_valid_evaluations <= 0:
        raise SystemExit("duration, poll interval, and minimum evaluations must be positive")
    config_path = await asyncio.to_thread(lambda: Path(args.config).resolve())
    load_operator_env(config_path)
    config = load_config(config_path)
    routes = enabled_routes(config)
    base_url = f"http://127.0.0.1:{config.observability_port}"
    artifact_dir = await asyncio.to_thread(lambda: Path(args.artifact_dir).resolve())
    await asyncio.to_thread(artifact_dir.mkdir, parents=True, exist_ok=True)
    started_at = datetime.now(UTC)
    timeout = aiohttp.ClientTimeout(total=10)
    connector = aiohttp.TCPConnector(limit=3)
    async with aiohttp.ClientSession(timeout=timeout, connector=connector, trust_env=False) as session:
        start_status, start_body = await _http_get(session, f"{base_url}/metrics")
        start_metrics = parse_prometheus(start_body)
        mode = effective_execution_mode(start_metrics)
        if start_status != 200 or mode != "shadow":
            raise SystemExit(f"calibration requires healthy shadow metrics; status={start_status}, mode={mode}")
        runtime_start_time_seconds = _metric_scalar(
            start_metrics,
            "arbitrage_runtime_start_time_seconds",
        )
        if (
            runtime_start_time_seconds is None
            or not math.isfinite(runtime_start_time_seconds)
            or runtime_start_time_seconds <= 0
        ):
            raise SystemExit("calibration requires a valid runtime start-time metric")

        samples: list[dict[str, Any]] = []
        deadline = asyncio.get_running_loop().time() + args.duration_seconds
        continuity_ok = True
        while asyncio.get_running_loop().time() < deadline:
            # /metrics performs its own readiness snapshot. Sequential probes avoid
            # creating duplicate concurrent DB pings from the observer itself.
            live = await _http_get(session, f"{base_url}/health/live")
            ready = await _http_get(session, f"{base_url}/health/ready")
            metrics = await _http_get(session, f"{base_url}/metrics")
            sample = runtime_health_sample(
                live,
                ready,
                metrics,
                expected_runtime_instance_id=config.runtime_instance_id,
                expected_runtime_start_time_seconds=runtime_start_time_seconds,
            )
            continuity_ok = continuity_ok and bool(sample["ok"])
            samples.append({"timestamp": datetime.now(UTC).isoformat(), **sample})
            await asyncio.sleep(min(args.poll_seconds, max(0.0, deadline - asyncio.get_running_loop().time())))

        final_live = await _http_get(session, f"{base_url}/health/live")
        final_ready = await _http_get(session, f"{base_url}/health/ready")
        final_metrics_response = await _http_get(session, f"{base_url}/metrics")
        final_sample = runtime_health_sample(
            final_live,
            final_ready,
            final_metrics_response,
            expected_runtime_instance_id=config.runtime_instance_id,
            expected_runtime_start_time_seconds=runtime_start_time_seconds,
        )
        samples.append({"timestamp": datetime.now(UTC).isoformat(), "phase": "final", **final_sample})
        end_body = final_metrics_response[1]
    end_metrics = parse_prometheus(end_body)
    result = calibration_result(routes, start_metrics, end_metrics, args.min_valid_evaluations)
    if args.require_configured_reserve:
        result = validate_configured_reserves(
            result,
            config.spread_policy.adverse_move_p95_pct_by_route,
        )
    blockers = window_continuity_blockers(
        final_sample=final_sample,
        expected_runtime_start_time_seconds=runtime_start_time_seconds,
        continuity_ok=continuity_ok,
    )
    result["passed"] = bool(result["passed"] and not blockers)
    report = {
        "config_path": str(config_path),
        "runtime_instance_id": config.runtime_instance_id,
        "runtime_start_time_seconds": runtime_start_time_seconds,
        "enabled_routes": list(routes),
        "duration_seconds": args.duration_seconds,
        "minimum_valid_evaluations": args.min_valid_evaluations,
        "configured_reserve_required": args.require_configured_reserve,
        "started_at": started_at.isoformat(),
        "stopped_at": datetime.now(UTC).isoformat(),
        "continuity_samples": samples,
        "blockers": blockers,
        **result,
    }
    report_path = artifact_dir / f"shadow-calibration-{config.runtime_instance_id}.json"
    await asyncio.to_thread(
        report_path.write_text,
        json.dumps(report, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    if report["passed"] and args.write_config:
        await asyncio.to_thread(_write_calibration, config_path, result)
        report["config_updated"] = True
        await asyncio.to_thread(
            report_path.write_text,
            json.dumps(report, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
    print(json.dumps(report, indent=2, ensure_ascii=False))
    if not report["passed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    asyncio.run(main())
