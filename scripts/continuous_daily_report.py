"""Write the daily evidence a continuously running funded canary owes.

A bounded run ends with SUMMARY.txt, which is where an operator looks to see
what happened. Continuous operation has no such end, so the equivalent is
written once per UTC day and refreshed after every window: which windows ran,
whether each one completed on its own deadline, which routes produced live
evidence, what the day has cost so far, and what state the runtime is in now.

Everything here is read back from evidence that already exists -- the per-window
observer reports on disk and the runtime audit snapshot in PostgreSQL. Nothing
is recomputed or re-derived, so the daily report cannot disagree with the
artifacts it summarises.
"""

from __future__ import annotations

import argparse
import asyncio
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from arbitrage_engine.config import load_config, load_operator_env
from arbitrage_engine.database import ProductionRepository
from arbitrage_engine.production_audit import enabled_routes


def window_summaries(target_dir: Path, day: str) -> list[dict[str, Any]]:
    """Summarise every observer report whose window started on `day` (UTC)."""
    summaries: list[dict[str, Any]] = []
    for report_path in sorted(target_dir.glob("canary-artifacts/*/*/report.json")):
        try:
            report = json.loads(report_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            # A window still in flight has no readable report yet. Skipping it
            # keeps the day's report honest rather than failing the run.
            continue
        if not isinstance(report, dict):
            continue
        started_at = str(report.get("started_at") or "")
        if not started_at.startswith(day):
            continue
        route_evidence = report.get("route_evidence") or {}
        summaries.append(
            {
                "report_path": str(report_path),
                "required_routes": report.get("required_routes"),
                "started_at": started_at,
                "stopped_at": report.get("stopped_at"),
                "observed_duration_seconds": report.get("observed_duration_seconds"),
                "window_completed": report.get("window_completed"),
                "stop_reason": report.get("stop_reason"),
                "result": report.get("result"),
                "monitoring_continuity_passed": (report.get("monitoring_continuity") or {}).get("passed"),
                "final_database_snapshot_ok": report.get("final_database_snapshot_ok"),
                "unresolved_order_intent_count": report.get("unresolved_order_intent_count"),
                "routes_with_live_evidence": sorted(
                    route
                    for route, evidence in route_evidence.items()
                    if isinstance(evidence, dict) and evidence.get("has_live_evidence") is True
                ),
            }
        )
    return summaries


def build_payload(
    snapshot: dict[str, Any],
    windows: list[dict[str, Any]],
    *,
    now: datetime,
    window_label: str,
) -> dict[str, Any]:
    risk_state = snapshot.get("risk_state") or {}
    metrics = snapshot.get("metrics") or {}
    return {
        "date": now.date().isoformat(),
        "generated_at": now.isoformat(),
        "generated_after_window": window_label,
        "runtime_instance_id": snapshot.get("runtime_instance_id"),
        "windows_started_today": len(windows),
        "windows_completed_today": sum(1 for window in windows if window.get("window_completed") is True),
        "windows": windows,
        "routes_with_live_evidence_today": sorted(
            {route for window in windows for route in (window.get("routes_with_live_evidence") or [])}
        ),
        # The risk controller's durable UTC-day accumulator: the number the daily
        # loss limit pauses on. It counts losses only and is not a full P&L.
        "daily_realized_loss_usd": risk_state.get("daily_loss_usd"),
        "risk_state": risk_state,
        "positions": snapshot.get("positions"),
        "unresolved_order_intents": snapshot.get("unresolved_order_intents"),
        "unresolved_redemptions": snapshot.get("unresolved_redemptions"),
        "reconciliation_failures": snapshot.get("reconciliation_failures"),
        "order_intents_by_status": metrics.get("order_intents"),
        "exposure_usd": metrics.get("exposure_usd"),
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True)
    parser.add_argument("--target-dir", required=True, help="closeout-artifacts/<run>/<target>")
    parser.add_argument("--daily-dir", required=True, help="closeout-artifacts/<run>/daily")
    parser.add_argument("--window-label", required=True)
    return parser


async def _snapshot(config_path: str) -> dict[str, Any]:
    load_operator_env(config_path)
    config = load_config(config_path)
    repository = ProductionRepository(
        config.database_url,
        runtime_instance_id=config.runtime_instance_id,
        enabled_routes=enabled_routes(config),
    )
    try:
        return await repository.runtime_audit_snapshot()
    finally:
        await repository.close()


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    now = datetime.now(UTC)
    snapshot = asyncio.run(_snapshot(args.config))
    windows = window_summaries(Path(args.target_dir), now.date().isoformat())
    payload = build_payload(snapshot, windows, now=now, window_label=args.window_label)

    daily_dir = Path(args.daily_dir)
    daily_dir.mkdir(parents=True, exist_ok=True)
    destination = daily_dir / f"{payload['date']}.json"
    destination.write_text(
        json.dumps(payload, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )
    print(
        json.dumps(
            {
                "daily_report": str(destination),
                "date": payload["date"],
                "windows_started_today": payload["windows_started_today"],
                "windows_completed_today": payload["windows_completed_today"],
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
