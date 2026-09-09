"""Decide whether another bounded funded window may start without an operator.

Continuous funded operation is a sequence of bounded canary windows rather than
one unbounded one: the evidence contract every window produces -- a completed
window that stopped on its own deadline -- only exists because the window ends.
Between two windows the runtime is durably paused, and this gate answers the one
question that decides whether the wrapper starts another: is the runtime paused
*because the window ended*, and is it in the state an operator resume would
require anyway?

Any other pause reason -- the daily loss limit, an UNKNOWN order outcome,
reconciliation drift, consecutive API errors, an operator stop -- exists to keep
trading stopped until a human looks at it. Starting another window would
silently overrule that, so this gate refuses and the wrapper stops.

Exit status is the answer: 0 to repeat, 1 to stop. The decision is printed as
JSON either way, so a stopped run says why.
"""

from __future__ import annotations

import argparse
import asyncio
import json
from typing import Any

from arbitrage_engine.config import load_config, load_operator_env
from arbitrage_engine.database import ProductionRepository
from arbitrage_engine.production_audit import enabled_routes

WINDOW_COMPLETE_PAUSE_REASON = "funded_canary_window_complete"


def evaluate(snapshot: dict[str, Any]) -> dict[str, Any]:
    """Turn a runtime audit snapshot into a repeat decision."""
    risk_state = snapshot.get("risk_state") or {}
    resume_gate = risk_state.get("operator_resume_gate") or {}
    blocking_reasons: list[str] = []

    if risk_state.get("paused") is not True:
        # Either the previous window never came to a stop or something resumed
        # trading behind the wrapper's back. Neither is a state to build on.
        blocking_reasons.append("runtime_is_not_durably_paused")

    pause_reason = risk_state.get("pause_reason")
    if pause_reason != WINDOW_COMPLETE_PAUSE_REASON:
        blocking_reasons.append(f"pause_reason:{pause_reason}")

    if resume_gate.get("eligible") is not True:
        # The same gate `risk resume` enforces: unresolved order intents,
        # unresolved redemptions, manual-review positions, reconciliation drift.
        # Checking it here means a stop is recorded as a decision rather than
        # discovered as a failed resume half a window later.
        blocking_reasons.extend(
            str(reason)
            for reason in (resume_gate.get("blocking_reasons") or ["operator_resume_gate_not_eligible"])
        )

    return {
        "may_repeat": not blocking_reasons,
        "blocking_reasons": blocking_reasons,
        "pause_reason": pause_reason,
        "risk_state": risk_state,
        "positions": snapshot.get("positions"),
        "unresolved_order_intents": snapshot.get("unresolved_order_intents"),
        "unresolved_redemptions": snapshot.get("unresolved_redemptions"),
        "reconciliation_failures": snapshot.get("reconciliation_failures"),
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True)
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
    decision = evaluate(asyncio.run(_snapshot(args.config)))
    print(json.dumps(decision, indent=2, sort_keys=True, default=str))
    return 0 if decision["may_repeat"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
