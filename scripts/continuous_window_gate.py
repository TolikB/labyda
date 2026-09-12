"""Decide what happens after a bounded funded window ends.

Continuous funded operation is a sequence of bounded canary windows rather than
one unbounded one: the evidence contract every window produces -- a completed
window that stopped on its own deadline -- only exists because the window ends.
Between two windows the runtime is durably paused, and this gate reads that
pause to choose between three outcomes.

`repeat`
    The window ended and the runtime is in the state an operator resume would
    require anyway. Start the next window.

`hold`
    The runtime stopped for a reason that clears itself: the daily loss limit,
    which is a stop for that UTC day and not for good, or a run of API errors,
    which is usually a venue having a bad few minutes. Wait it out, then
    re-evaluate. A hold is only ever offered when the runtime is otherwise
    clean -- unresolved money outranks any recoverable reason.

`stop`
    Everything else: an UNKNOWN order outcome, reconciliation drift, a position
    in manual review, an operator stop, a reason this gate does not recognise.
    Those exist to keep trading stopped until a human looks, and no amount of
    waiting changes them.

Exit status carries the verdict: 0 repeat, 10 hold, anything else stop. A crash
therefore reads as stop, which is the fail-closed direction.
"""

from __future__ import annotations

import argparse
import asyncio
import json
from datetime import UTC, date, datetime, timedelta
from typing import Any

from arbitrage_engine.config import load_config, load_operator_env
from arbitrage_engine.database import ProductionRepository
from arbitrage_engine.production_audit import enabled_routes
from arbitrage_engine.reconciliation import RECONCILIATION_TRANSIENT_PAUSE_REASON
from arbitrage_engine.risk import API_ERROR_PAUSE_SUFFIX, DAILY_LOSS_PAUSE_PREFIX

WINDOW_COMPLETE_PAUSE_REASON = "funded_canary_window_complete"

REPEAT = "repeat"
HOLD = "hold"
STOP = "stop"

EXIT_REPEAT = 0
EXIT_HOLD = 10
EXIT_STOP = 1

# A daily-loss hold ends just after midnight UTC, when `GlobalRiskController`
# rolls the day forward and `risk resume` starts succeeding again. The margin
# keeps the wrapper from racing a clock a second behind the runtime's.
DAILY_LOSS_HOLD_MARGIN_SECONDS = 60
DEFAULT_API_ERROR_HOLD_SECONDS = 900


def _next_utc_midnight(now: datetime) -> datetime:
    return datetime.combine(now.date() + timedelta(days=1), datetime.min.time(), tzinfo=UTC)


def _parse_loss_day(value: Any) -> date | None:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    try:
        return date.fromisoformat(str(value))
    except (TypeError, ValueError):
        return None


def evaluate(
    snapshot: dict[str, Any],
    *,
    now: datetime | None = None,
    api_error_hold_seconds: int = DEFAULT_API_ERROR_HOLD_SECONDS,
) -> dict[str, Any]:
    """Turn a runtime audit snapshot into a repeat/hold/stop decision."""
    moment = now or datetime.now(UTC)
    risk_state = snapshot.get("risk_state") or {}
    resume_gate = risk_state.get("operator_resume_gate") or {}
    pause_reason = risk_state.get("pause_reason")

    def decision(
        verdict: str,
        reasons: list[str],
        *,
        hold_until: datetime | None = None,
        hold_kind: str | None = None,
    ) -> dict[str, Any]:
        return {
            "verdict": verdict,
            "may_repeat": verdict == REPEAT,
            "blocking_reasons": reasons,
            "hold_until_unix": hold_until.timestamp() if hold_until else None,
            "hold_until": hold_until.isoformat() if hold_until else None,
            "hold_kind": hold_kind,
            "pause_reason": pause_reason,
            "risk_state": risk_state,
            "positions": snapshot.get("positions"),
            "unresolved_order_intents": snapshot.get("unresolved_order_intents"),
            "unresolved_redemptions": snapshot.get("unresolved_redemptions"),
            "reconciliation_failures": snapshot.get("reconciliation_failures"),
        }

    # Unresolved money outranks every recoverable reason. Waiting out a hold
    # would not resolve an UNKNOWN intent or a drifted reconciliation, and the
    # next window's `risk resume` would refuse anyway -- better to stop here,
    # where the decision is recorded, than to discover it half a window later.
    if resume_gate.get("eligible") is not True:
        return decision(
            STOP,
            [
                str(reason)
                for reason in (resume_gate.get("blocking_reasons") or ["operator_resume_gate_not_eligible"])
            ],
        )

    if risk_state.get("paused") is not True:
        # Either the previous window never came to a stop, or something resumed
        # trading behind the wrapper's back. Neither is a state to build on.
        return decision(STOP, ["runtime_is_not_durably_paused"])

    if pause_reason == WINDOW_COMPLETE_PAUSE_REASON:
        return decision(REPEAT, [])

    if isinstance(pause_reason, str) and pause_reason.startswith(DAILY_LOSS_PAUSE_PREFIX):
        loss_day = _parse_loss_day(risk_state.get("loss_day"))
        if loss_day is None:
            # Without the day, there is no way to know when the limit expires.
            return decision(STOP, [f"daily_loss_pause_without_loss_day:{pause_reason}"])
        if loss_day < moment.date():
            # The day already rolled over; resume will zero the accumulator.
            return decision(REPEAT, [])
        return decision(
            HOLD,
            [f"pause_reason:{pause_reason}"],
            hold_until=_next_utc_midnight(moment) + timedelta(seconds=DAILY_LOSS_HOLD_MARGIN_SECONDS),
            hold_kind="daily_loss",
        )

    # A venue that kept failing reconciliation is the same kind of trouble as
    # one that kept failing orders: usually a bad few minutes, occasionally a
    # real outage. Same wait, same budget.
    if isinstance(pause_reason, str) and (
        pause_reason.endswith(API_ERROR_PAUSE_SUFFIX) or pause_reason == RECONCILIATION_TRANSIENT_PAUSE_REASON
    ):
        return decision(
            HOLD,
            [f"pause_reason:{pause_reason}"],
            hold_until=moment + timedelta(seconds=api_error_hold_seconds),
            hold_kind="api_errors",
        )

    return decision(STOP, [f"pause_reason:{pause_reason}"])


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Decide repeat/hold/stop after a bounded funded window")
    parser.add_argument("--config", required=True)
    parser.add_argument("--api-error-hold-seconds", type=int, default=DEFAULT_API_ERROR_HOLD_SECONDS)
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
    decision = evaluate(
        asyncio.run(_snapshot(args.config)),
        api_error_hold_seconds=args.api_error_hold_seconds,
    )
    print(json.dumps(decision, indent=2, sort_keys=True, default=str))
    if decision["verdict"] == REPEAT:
        return EXIT_REPEAT
    if decision["verdict"] == HOLD:
        return EXIT_HOLD
    return EXIT_STOP


if __name__ == "__main__":
    raise SystemExit(main())
