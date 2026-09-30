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
from collections.abc import Awaitable, Callable
from datetime import UTC, date, datetime, timedelta
from typing import Any

from arbitrage_engine.cli import _reconciliation_failures_for_resume
from arbitrage_engine.config import load_config, load_operator_env
from arbitrage_engine.database import ProductionRepository
from arbitrage_engine.production_audit import enabled_routes
from arbitrage_engine.reconciliation import RECONCILIATION_TRANSIENT_PAUSE_REASON
from arbitrage_engine.risk import API_ERROR_PAUSE_SUFFIX, DAILY_LOSS_PAUSE_PREFIX

WINDOW_COMPLETE_PAUSE_REASON = "funded_canary_window_complete"
OBSERVER_FAILED_PAUSE_REASON = "funded_canary_observer_failed"

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


def effective_pause_reason(risk_state: dict[str, Any], window_close_report: dict[str, Any] | None) -> Any:
    """The reason the gate judges: the runtime's own, if the wrapper's pause covered it.

    The wrapper closes a window with `risk pause --reason funded_canary_window_complete`
    and hands the gate that command's report. When the runtime was already
    paused for a reason of its own -- drift, a venue that stayed broken, the
    daily loss limit -- that reason is the one that decides repeat/hold/stop;
    the wrapper's overwrite is bookkeeping. On 2026-09-15 a drift pause was
    covered this way and the loop repeated instead of stopping.
    """
    pause_reason = risk_state.get("pause_reason")
    if pause_reason != WINDOW_COMPLETE_PAUSE_REASON or not window_close_report:
        return pause_reason
    previous = window_close_report.get("previous_pause_reason")
    covered_own_reason = previous not in (None, "", WINDOW_COMPLETE_PAUSE_REASON)
    if window_close_report.get("previously_paused") is True and covered_own_reason:
        return previous
    return pause_reason


def evaluate(
    snapshot: dict[str, Any],
    *,
    now: datetime | None = None,
    api_error_hold_seconds: int = DEFAULT_API_ERROR_HOLD_SECONDS,
    window_close_report: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Turn a runtime audit snapshot into a repeat/hold/stop decision."""
    moment = now or datetime.now(UTC)
    risk_state = snapshot.get("risk_state") or {}
    resume_gate = risk_state.get("operator_resume_gate") or {}
    pause_reason = effective_pause_reason(risk_state, window_close_report)

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
            "window_close_pause_reason": risk_state.get("pause_reason"),
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
    # Reconciliation evidence reaches this point already re-taken the way
    # resume takes it (`refreshed_snapshot`), so a blocker here is one resume
    # would also refuse on, not a request that timed out during a stall.
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
    # An observer that gave up on a slow runtime is the same again: the
    # runtime was paused fail-closed, nothing is open, and a fresh window
    # after a wait is the right next step. The hold budget still bounds it.
    if isinstance(pause_reason, str) and (
        pause_reason.endswith(API_ERROR_PAUSE_SUFFIX)
        or pause_reason == RECONCILIATION_TRANSIENT_PAUSE_REASON
        or pause_reason == OBSERVER_FAILED_PAUSE_REASON
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
    parser.add_argument(
        "--window-close-report",
        help="JSON printed by the wrapper's window-close `risk pause`; carries the reason it overwrote",
    )
    return parser


def _load_window_close_report(path: str | None) -> dict[str, Any] | None:
    if not path:
        return None
    try:
        with open(path, encoding="utf-8") as handle:
            loaded = json.load(handle)
    except (OSError, ValueError):
        # A missing or unreadable report must not hide a reason: without it
        # the gate falls back to the snapshot, which is the pre-existing
        # behaviour, and the artifact set shows the report failed.
        return None
    return loaded if isinstance(loaded, dict) else None


RECONCILIATION_BLOCKER_PREFIX = "reconciliation_failures:"


def needs_reconciliation_recheck(snapshot: dict[str, Any]) -> bool:
    """Whether the resume gate is refusing on reconciliation evidence the gate should re-take.

    The latest reconciliation row per venue is whatever the runtime wrote last,
    and the moment a window ends early is exactly when those rows are worst: on
    2026-09-30 a discovery rebuild starved the runtime's event loop for long
    enough that the observers gave up at 13:31, and the reconciliation cycles
    that ran through the same stall recorded a Myriad timeout and a failed
    Predict.fun pass with zero drift. The gate read those rows and stopped the
    run -- although the pause reason, an observer that gave up, is one it holds
    for, and `risk resume` would have re-reconciled and gone through.
    """
    risk_state = snapshot.get("risk_state") or {}
    resume_gate = risk_state.get("operator_resume_gate") or {}
    if resume_gate.get("eligible") is True:
        return False
    return any(
        str(reason).startswith(RECONCILIATION_BLOCKER_PREFIX)
        for reason in resume_gate.get("blocking_reasons") or ()
    )


async def refreshed_snapshot(
    read: Callable[[], Awaitable[dict[str, Any]]],
    recheck: Callable[[], Awaitable[object]],
) -> dict[str, Any]:
    """The runtime snapshot, re-taken after a fresh reconciliation when the old one is unclean.

    Only the reconciliation evidence is re-taken, and with the same retries
    `risk resume` uses, so the gate judges what resume would see. Drift or a
    venue that stays broken still reads as a blocker and still stops; an
    UNKNOWN intent or an open position was never a reconciliation blocker and
    is untouched by this.
    """
    snapshot = await read()
    if not needs_reconciliation_recheck(snapshot):
        return snapshot
    await recheck()
    return await read()


async def _snapshot(config_path: str) -> dict[str, Any]:
    load_operator_env(config_path)
    config = load_config(config_path)
    repository = ProductionRepository(
        config.database_url,
        runtime_instance_id=config.runtime_instance_id,
        enabled_routes=enabled_routes(config),
    )
    try:
        return await refreshed_snapshot(
            repository.runtime_audit_snapshot,
            lambda: _reconciliation_failures_for_resume(config, repository),
        )
    finally:
        await repository.close()


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    decision = evaluate(
        asyncio.run(_snapshot(args.config)),
        api_error_hold_seconds=args.api_error_hold_seconds,
        window_close_report=_load_window_close_report(args.window_close_report),
    )
    print(json.dumps(decision, indent=2, sort_keys=True, default=str))
    if decision["verdict"] == REPEAT:
        return EXIT_REPEAT
    if decision["verdict"] == HOLD:
        return EXIT_HOLD
    return EXIT_STOP


if __name__ == "__main__":
    raise SystemExit(main())
