from __future__ import annotations

import argparse
import json
import re
import urllib.error
import urllib.request
from typing import Any

_METRIC_RE = re.compile(
    r"^(?P<name>[a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{(?P<labels>[^}]*)\})?\s+(?P<value>\S+)$"
)
_LABEL_RE = re.compile(r'(?P<key>[a-zA-Z_][a-zA-Z0-9_]*)="(?P<value>(?:\\.|[^"\\])*)"')


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Fail-closed runtime health gate for Compose deployment")
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--expected-runtime-instance-id", required=True)
    parser.add_argument("--expected-mode", required=True, choices=("shadow", "canary", "live"))
    parser.add_argument(
        "--accept",
        required=True,
        choices=("ready", "safe_paused_shadow", "safe_paused_shadow_bootstrap"),
    )
    parser.add_argument("--timeout-seconds", type=float, default=3.0)
    return parser


def _parse_metrics(body: str) -> list[tuple[str, dict[str, str], float]]:
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
        labels = {
            label.group("key"): label.group("value").replace(r'\"', '"')
            for label in _LABEL_RE.finditer(match.group("labels") or "")
        }
        parsed.append((match.group("name"), labels, value))
    return parsed


def _metric_scalar(metrics: list[tuple[str, dict[str, str], float]], name: str) -> float | None:
    return next((value for metric, labels, value in metrics if metric == name and not labels), None)


def _execution_mode(metrics: list[tuple[str, dict[str, str], float]]) -> str | None:
    return next(
        (
            labels["mode"]
            for name, labels, value in metrics
            if name == "arbitrage_execution_mode_info" and value == 1 and labels.get("mode")
        ),
        None,
    )


def evaluate_runtime_health(
    live: tuple[int | None, str],
    ready: tuple[int | None, str],
    metrics_response: tuple[int | None, str],
    *,
    expected_runtime_instance_id: str,
    expected_mode: str,
    accepted_state: str,
) -> dict[str, Any]:
    try:
        ready_payload = json.loads(ready[1])
    except (TypeError, json.JSONDecodeError):
        ready_payload = None
    if not isinstance(ready_payload, dict):
        ready_payload = None

    metrics = _parse_metrics(metrics_response[1])
    risk_paused = _metric_scalar(metrics, "arbitrage_risk_paused")
    ready_metric = _metric_scalar(metrics, "arbitrage_ready")
    execution_mode = _execution_mode(metrics)
    reasons = ready_payload.get("reasons", []) if ready_payload is not None else []
    if not isinstance(reasons, list):
        reasons = []
    discovery = ready_payload.get("discovery") if ready_payload is not None else None
    if not isinstance(discovery, dict):
        discovery = None
    runtime_instance_id = (
        str(ready_payload.get("runtime_instance_id") or "") if ready_payload is not None else ""
    )
    common = (
        live[0] == 200
        and metrics_response[0] == 200
        and runtime_instance_id == expected_runtime_instance_id
        and execution_mode == expected_mode
    )
    ready_state = (
        common
        and ready[0] == 200
        and ready_payload is not None
        and ready_payload.get("status") == "ready"
        and not reasons
        and risk_paused == 0
        and ready_metric == 1
    )
    safe_paused_shadow = (
        common
        and expected_mode == "shadow"
        and ready[0] == 503
        and ready_payload is not None
        and ready_payload.get("status") == "not_ready"
        and bool(reasons)
        and all(isinstance(reason, str) and reason.startswith("risk_paused:") for reason in reasons)
        and risk_paused == 1
        and ready_metric == 0
    )
    bootstrap_reason_set = {
        reason
        for reason in reasons
        if isinstance(reason, str) and (reason.startswith("risk_paused:") or reason == "discovery_not_ready")
    }
    bootstrap_missing_routes = discovery.get("missing_routes", []) if discovery is not None else []
    safe_paused_shadow_bootstrap = (
        common
        and expected_mode == "shadow"
        and ready[0] == 503
        and ready_payload is not None
        and ready_payload.get("status") == "not_ready"
        and len(bootstrap_reason_set) == len(reasons)
        and any(isinstance(reason, str) and reason.startswith("risk_paused:") for reason in reasons)
        and "discovery_not_ready" in bootstrap_reason_set
        and discovery is not None
        and isinstance(bootstrap_missing_routes, list)
        and bool(bootstrap_missing_routes)
        and all(isinstance(route, str) and route for route in bootstrap_missing_routes)
        and discovery.get("last_error") in (None, "")
        and discovery.get("stale") is False
        and risk_paused == 1
        and ready_metric == 0
    )
    accepted = {
        "ready": ready_state,
        "safe_paused_shadow": safe_paused_shadow,
        "safe_paused_shadow_bootstrap": safe_paused_shadow_bootstrap,
    }.get(accepted_state, False)
    return {
        "accepted": accepted,
        "accepted_state": accepted_state,
        "live_status": live[0],
        "ready_status": ready[0],
        "metrics_status": metrics_response[0],
        "runtime_instance_id": runtime_instance_id,
        "expected_runtime_instance_id": expected_runtime_instance_id,
        "execution_mode": execution_mode,
        "expected_mode": expected_mode,
        "risk_paused": risk_paused,
        "ready_metric": ready_metric,
        "readiness_reasons": reasons,
        "ready_state": ready_state,
        "safe_paused_shadow": safe_paused_shadow,
        "safe_paused_shadow_bootstrap": safe_paused_shadow_bootstrap,
    }


def _get(url: str, timeout_seconds: float) -> tuple[int | None, str]:
    try:
        with urllib.request.urlopen(url, timeout=timeout_seconds) as response:
            return int(response.status), response.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        return int(exc.code), exc.read().decode("utf-8", errors="replace")
    except (OSError, TimeoutError, urllib.error.URLError) as exc:
        return None, str(exc)


def main() -> None:
    args = build_parser().parse_args()
    if args.timeout_seconds <= 0:
        raise SystemExit("--timeout-seconds must be positive")
    if args.accept in {"safe_paused_shadow", "safe_paused_shadow_bootstrap"} and args.expected_mode != "shadow":
        raise SystemExit(f"{args.accept} requires --expected-mode shadow")

    base_url = str(args.base_url).rstrip("/")
    result = evaluate_runtime_health(
        _get(f"{base_url}/health/live", args.timeout_seconds),
        _get(f"{base_url}/health/ready", args.timeout_seconds),
        _get(f"{base_url}/metrics", args.timeout_seconds),
        expected_runtime_instance_id=args.expected_runtime_instance_id,
        expected_mode=args.expected_mode,
        accepted_state=args.accept,
    )
    print(json.dumps(result, sort_keys=True))
    if not result["accepted"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
