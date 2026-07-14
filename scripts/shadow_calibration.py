from __future__ import annotations

import argparse
import asyncio
import json
import math
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import urlopen

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
    return parser


def _http_get(url: str) -> tuple[int | None, str]:
    try:
        with urlopen(url, timeout=10) as response:  # noqa: S310
            return int(response.status), response.read().decode("utf-8", errors="replace")
    except HTTPError as exc:
        return int(exc.code), exc.read().decode("utf-8", errors="replace")
    except (TimeoutError, URLError, OSError) as exc:
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
    start_status, start_body = await asyncio.to_thread(_http_get, f"{base_url}/metrics")
    start_metrics = parse_prometheus(start_body)
    mode = effective_execution_mode(start_metrics)
    if start_status != 200 or mode != "shadow":
        raise SystemExit(f"calibration requires healthy shadow metrics; status={start_status}, mode={mode}")

    samples: list[dict[str, Any]] = []
    deadline = asyncio.get_running_loop().time() + args.duration_seconds
    continuity_ok = True
    while asyncio.get_running_loop().time() < deadline:
        live, ready, metrics = await asyncio.gather(
            asyncio.to_thread(_http_get, f"{base_url}/health/live"),
            asyncio.to_thread(_http_get, f"{base_url}/health/ready"),
            asyncio.to_thread(_http_get, f"{base_url}/metrics"),
        )
        sample_mode = effective_execution_mode(parse_prometheus(metrics[1]))
        sample_ok = live[0] == 200 and ready[0] == 200 and metrics[0] == 200 and sample_mode == "shadow"
        continuity_ok = continuity_ok and sample_ok
        samples.append(
            {
                "timestamp": datetime.now(UTC).isoformat(),
                "live_status": live[0],
                "ready_status": ready[0],
                "metrics_status": metrics[0],
                "execution_mode": sample_mode,
                "ok": sample_ok,
            }
        )
        await asyncio.sleep(min(args.poll_seconds, max(0.0, deadline - asyncio.get_running_loop().time())))

    end_status, end_body = await asyncio.to_thread(_http_get, f"{base_url}/metrics")
    result = calibration_result(routes, start_metrics, parse_prometheus(end_body), args.min_valid_evaluations)
    blockers: list[str] = []
    if end_status != 200:
        blockers.append("final_metrics_unavailable")
    if not continuity_ok:
        blockers.append("runtime_health_or_shadow_mode_interrupted")
    result["passed"] = bool(result["passed"] and not blockers)
    report = {
        "config_path": str(config_path),
        "runtime_instance_id": config.runtime_instance_id,
        "enabled_routes": list(routes),
        "duration_seconds": args.duration_seconds,
        "minimum_valid_evaluations": args.min_valid_evaluations,
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
