"""Indefinite funded trading is repeated bounded windows, not an unbounded one.

Removing the hard deadline would have been the small change, and it would have
taken the evidence contract with it: `live_canary_window.py` is built around a
deadline, its report only claims `window_completed` when the window stopped on
that deadline, and the final audit refuses anything whose `stop_reason` is not
`timeout`. So continuous operation keeps every window exactly as it was and
repeats it. What has to hold is that the wrapper starts another window only from
the one state that means "the previous window ended", and never after a pause
that means "stop".

The runtime half of this -- re-reading a deadline it used to latch forever --
lives in `test_execution.py`, beside the other funded-canary deadline tests.
"""

from __future__ import annotations

import importlib.util
import json
import os
import shutil
import subprocess
import sys
import unittest
from datetime import UTC, datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from types import ModuleType
from typing import Any, cast

from arbitrage_engine.risk import GlobalRiskController

REPO_ROOT = Path(__file__).resolve().parents[1]
CLOSEOUT_SCRIPT = REPO_ROOT / "ops" / "production_closeout.sh"


def _load_script_module(name: str, filename: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, REPO_ROOT / "scripts" / filename)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


window_gate = _load_script_module("continuous_window_gate_module", "continuous_window_gate.py")
daily_report = _load_script_module("continuous_daily_report_module", "continuous_daily_report.py")


class ContinuousWindowGateTests(unittest.TestCase):
    """Only one pause reason means `the window ended`; every other means `stop`."""

    @staticmethod
    def snapshot(
        *,
        paused: bool = True,
        pause_reason: str | None = "funded_canary_window_complete",
        eligible: bool = True,
        blocking_reasons: list[str] | None = None,
    ) -> dict[str, Any]:
        return {
            "risk_state": {
                "paused": paused,
                "pause_reason": pause_reason,
                "daily_loss_usd": "0",
                "operator_resume_gate": {
                    "applies": paused,
                    "eligible": eligible,
                    "blocking_reasons": blocking_reasons or [],
                },
            },
            "positions": {"count": 0},
            "unresolved_order_intents": {"count": 0},
            "unresolved_redemptions": {"count": 0},
            "reconciliation_failures": [],
        }

    def evaluate(self, **kwargs: Any) -> dict[str, Any]:
        # The gate is loaded by path, so its return type is opaque to mypy here.
        return cast("dict[str, Any]", window_gate.evaluate(self.snapshot(**kwargs)))

    def test_a_completed_window_may_be_followed_by_another(self) -> None:
        decision = self.evaluate()

        self.assertTrue(decision["may_repeat"])
        self.assertEqual(decision["blocking_reasons"], [])

    def test_the_daily_loss_limit_stops_the_loop(self) -> None:
        # The limit exists so an unattended run stops for the day. Starting the
        # next window would be the loop quietly overruling it.
        reason = "daily realized loss $12.00 reached limit $10.00"
        decision = self.evaluate(pause_reason=reason)

        self.assertFalse(decision["may_repeat"])
        self.assertIn(f"pause_reason:{reason}", decision["blocking_reasons"])

    def test_an_unknown_order_outcome_stops_the_loop(self) -> None:
        decision = self.evaluate(pause_reason="unknown order outcome: Polymarket client_order_id=abc")

        self.assertFalse(decision["may_repeat"])

    def test_an_operator_pause_stops_the_loop(self) -> None:
        decision = self.evaluate(pause_reason="operator_requested_stop")

        self.assertFalse(decision["may_repeat"])

    def test_a_running_runtime_is_not_a_state_to_start_from(self) -> None:
        decision = self.evaluate(paused=False, pause_reason=None)

        self.assertFalse(decision["may_repeat"])
        self.assertIn("runtime_is_not_durably_paused", decision["blocking_reasons"])

    def test_unresolved_state_blocks_the_next_window(self) -> None:
        # The same gate `risk resume` enforces, checked before the window starts
        # so the stop is recorded as a decision instead of a failed resume.
        decision = self.evaluate(
            eligible=False,
            blocking_reasons=["unresolved_order_intents:2", "reconciliation_failures:1"],
        )

        self.assertFalse(decision["may_repeat"])
        self.assertIn("unresolved_order_intents:2", decision["blocking_reasons"])
        self.assertIn("reconciliation_failures:1", decision["blocking_reasons"])

    def test_a_missing_resume_gate_is_treated_as_ineligible(self) -> None:
        decision = window_gate.evaluate(
            {"risk_state": {"paused": True, "pause_reason": "funded_canary_window_complete"}}
        )

        self.assertFalse(decision["may_repeat"])
        self.assertIn("operator_resume_gate_not_eligible", decision["blocking_reasons"])

    def test_an_empty_snapshot_never_repeats(self) -> None:
        self.assertFalse(window_gate.evaluate({})["may_repeat"])


class ContinuousDailyReportTests(unittest.TestCase):
    """A continuous run has no SUMMARY.txt, so the day is the unit of evidence."""

    def setUp(self) -> None:
        temporary = TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.target_dir = Path(temporary.name) / "quote_arb"

    def write_window(self, route: str, started_at: str, **overrides: Any) -> Path:
        report_dir = self.target_dir / "canary-artifacts" / route / started_at.replace(":", "")
        report_dir.mkdir(parents=True, exist_ok=True)
        report: dict[str, Any] = {
            "started_at": started_at,
            "stopped_at": started_at,
            "window_completed": True,
            "stop_reason": "timeout",
            "result": "timeout",
            "required_routes": [route],
            "monitoring_continuity": {"passed": True},
            "final_database_snapshot_ok": True,
            "unresolved_order_intent_count": 0,
            "route_evidence": {route: {"has_live_evidence": False}},
        }
        report.update(overrides)
        path = report_dir / "report.json"
        path.write_text(json.dumps(report), encoding="utf-8")
        return path

    def test_only_windows_that_started_today_are_counted(self) -> None:
        self.write_window("polymarket_predict", "2026-09-09T00:00:00+00:00")
        self.write_window("polymarket_predict", "2026-09-09T04:00:00+00:00")
        self.write_window("polymarket_predict", "2026-09-08T20:00:00+00:00")

        summaries = daily_report.window_summaries(self.target_dir, "2026-09-09")

        self.assertEqual(len(summaries), 2)

    def test_a_window_still_in_flight_does_not_break_the_report(self) -> None:
        self.write_window("polymarket_predict", "2026-09-09T00:00:00+00:00")
        partial = self.target_dir / "canary-artifacts" / "polymarket_sx" / "inflight"
        partial.mkdir(parents=True)
        (partial / "report.json").write_text("{ truncated", encoding="utf-8")

        summaries = daily_report.window_summaries(self.target_dir, "2026-09-09")

        self.assertEqual(len(summaries), 1)

    def test_routes_with_live_evidence_are_rolled_up_across_the_day(self) -> None:
        self.write_window(
            "polymarket_predict",
            "2026-09-09T00:00:00+00:00",
            route_evidence={"polymarket_predict": {"has_live_evidence": True}},
        )
        self.write_window("polymarket_sx", "2026-09-09T04:00:00+00:00")
        windows = daily_report.window_summaries(self.target_dir, "2026-09-09")

        payload = daily_report.build_payload(
            {"risk_state": {"daily_loss_usd": "4.25"}, "metrics": {"exposure_usd": "50"}},
            windows,
            now=datetime(2026, 9, 9, 8, tzinfo=UTC),
            window_label="window-002",
        )

        self.assertEqual(payload["windows_started_today"], 2)
        self.assertEqual(payload["windows_completed_today"], 2)
        self.assertEqual(payload["routes_with_live_evidence_today"], ["polymarket_predict"])
        self.assertEqual(payload["daily_realized_loss_usd"], "4.25")
        self.assertEqual(payload["generated_after_window"], "window-002")

    def test_an_incomplete_window_is_reported_as_incomplete(self) -> None:
        self.write_window(
            "polymarket_predict",
            "2026-09-09T00:00:00+00:00",
            window_completed=False,
            stop_reason="first_fill_or_open_position",
        )
        windows = daily_report.window_summaries(self.target_dir, "2026-09-09")

        payload = daily_report.build_payload(
            {},
            windows,
            now=datetime(2026, 9, 9, 8, tzinfo=UTC),
            window_label="window-001",
        )

        self.assertEqual(payload["windows_started_today"], 1)
        self.assertEqual(payload["windows_completed_today"], 0)


@unittest.skipIf(shutil.which("bash") is None, "bash is required for ops script contracts")
class ContinuousModeGateTests(unittest.TestCase):
    """`CONTINUOUS_TRADING_CONFIRMED` is a second confirmation, not a relaxation."""

    harness: str

    @classmethod
    def setUpClass(cls) -> None:
        body = CLOSEOUT_SCRIPT.read_text(encoding="utf-8")
        defaults = body[body.index("CREDENTIAL_ROTATION_CONFIRMED=") : body.index("CLOSEOUT_OPERATOR=")]
        gates = body[body.index("resolve_credential_decision()") : body.index('if [[ -n "${ADMIN_BIN}"')]
        cls.harness = f"set -Eeuo pipefail\n{defaults}\n{gates}\necho gates_passed\n"

    def run_gates(self, **environment: str) -> subprocess.CompletedProcess[str]:
        env = {
            **os.environ,
            "ENABLE_FUNDED_CANARY": "YES",
            "FUNDED_CANARY_TARGET": "quote_arb",
            "CREDENTIAL_REUSE_CONFIRMED": "YES",
            "DURATION_SECONDS": "14400",
            "CALIBRATION_DURATION_SECONDS": "3600",
        }
        for key in ("CONTINUOUS_TRADING_CONFIRMED", "CONTINUOUS_MAX_WINDOWS", "CONTINUOUS_STOP_FILE"):
            env.pop(key, None)
        env.update(environment)
        return subprocess.run(
            ["bash", "-c", self.harness],
            cwd=REPO_ROOT,
            env=env,
            capture_output=True,
            text=True,
            check=False,
            timeout=30,
        )

    def test_the_default_is_a_single_bounded_window(self) -> None:
        result = self.run_gates()

        self.assertEqual(result.returncode, 0, msg=result.stderr)
        self.assertIn("gates_passed", result.stdout)

    def test_continuous_mode_is_accepted_alongside_the_funded_canary(self) -> None:
        result = self.run_gates(CONTINUOUS_TRADING_CONFIRMED="YES")

        self.assertEqual(result.returncode, 0, msg=result.stderr)

    def test_continuous_mode_cannot_be_confirmed_on_its_own(self) -> None:
        result = self.run_gates(CONTINUOUS_TRADING_CONFIRMED="YES", ENABLE_FUNDED_CANARY="NO")

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("requires ENABLE_FUNDED_CANARY=YES", result.stderr)

    def test_continuous_mode_does_not_lengthen_the_window(self) -> None:
        # Each window stays exactly 14400s; only the number of them is unbounded.
        result = self.run_gates(CONTINUOUS_TRADING_CONFIRMED="YES", DURATION_SECONDS="86400")

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("funded canary requires DURATION_SECONDS=14400", result.stderr)

    def test_continuous_mode_does_not_shorten_calibration(self) -> None:
        result = self.run_gates(CONTINUOUS_TRADING_CONFIRMED="YES", CALIBRATION_DURATION_SECONDS="60")

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("funded canary requires CALIBRATION_DURATION_SECONDS=3600", result.stderr)

    def test_continuous_mode_still_needs_a_credential_decision(self) -> None:
        result = self.run_gates(CONTINUOUS_TRADING_CONFIRMED="YES", CREDENTIAL_REUSE_CONFIRMED="NO")

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("funded canary requires CREDENTIAL_REUSE", result.stderr)

    def test_a_typo_in_the_confirmation_is_not_read_as_no(self) -> None:
        result = self.run_gates(CONTINUOUS_TRADING_CONFIRMED="yes")

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("CONTINUOUS_TRADING_CONFIRMED must be YES or NO", result.stderr)

    def test_a_window_budget_must_be_a_number(self) -> None:
        result = self.run_gates(CONTINUOUS_TRADING_CONFIRMED="YES", CONTINUOUS_MAX_WINDOWS="lots")

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("CONTINUOUS_MAX_WINDOWS must be a non-negative integer", result.stderr)

    def test_a_window_budget_of_zero_means_unbounded(self) -> None:
        result = self.run_gates(CONTINUOUS_TRADING_CONFIRMED="YES", CONTINUOUS_MAX_WINDOWS="0")

        self.assertEqual(result.returncode, 0, msg=result.stderr)


class ContinuousLoopStructureTests(unittest.TestCase):
    """The loop's stop conditions are the safety property; keep them together."""

    body: str

    @classmethod
    def setUpClass(cls) -> None:
        cls.body = CLOSEOUT_SCRIPT.read_text(encoding="utf-8")

    def test_the_window_is_a_function_the_loop_calls(self) -> None:
        self.assertIn("run_funded_canary_window() {", self.body)
        self.assertIn('run_funded_canary_window "${funded_window_label}"', self.body)

    def test_every_stop_condition_is_checked_after_the_window(self) -> None:
        window_call = self.body.index('run_funded_canary_window "${funded_window_label}"')
        for condition in (
            'if [[ "${CONTINUOUS_TRADING_CONFIRMED}" != "YES" ]]; then',
            'if [[ -e "${CONTINUOUS_STOP_FILE}" ]]; then',
            'if [[ "${CONTINUOUS_MAX_WINDOWS}" != "0" \\',
            "if ! assert_release_integrity; then",
            "if ! continuous_window_may_repeat",
        ):
            self.assertGreater(self.body.index(condition, window_call), window_call, condition)

    def test_each_window_re_arms_the_fail_closed_sentinel_before_its_observers(self) -> None:
        window = self.body[
            self.body.index("run_funded_canary_window() {") : self.body.index("funded_window_index=0")
        ]
        sentinel = window.index("printf '%s\\n' 0 >\"${canary_deadline_file}\"")
        observers = window.index("scripts/live_canary_window.py")
        publish = window.index('printf \'%s\\n\' "${canary_deadline_unix}" >"${canary_deadline_file}"')
        resume = window.index("risk-resume-canary-")
        self.assertLess(sentinel, observers)
        self.assertLess(observers, publish)
        self.assertLess(publish, resume)

    def test_a_stale_stop_file_cannot_shorten_a_new_run(self) -> None:
        self.assertIn('rm -f "${CONTINUOUS_STOP_FILE}"', self.body)
        self.assertLess(
            self.body.index('rm -f "${CONTINUOUS_STOP_FILE}"'),
            self.body.index("funded_window_index=0"),
        )

    def test_per_window_artifacts_do_not_overwrite_each_other(self) -> None:
        window = self.body[self.body.index("run_funded_canary_window() {") :]
        self.assertIn('window_dir="${run_dir}/${FUNDED_CANARY_TARGET}/windows/${window_label}"', window)
        self.assertIn('"risk-resume-canary-${window_label}"', window)
        self.assertIn('"risk-pause-canary-window-complete-${window_label}"', window)

    def test_the_daily_report_is_written_after_every_window(self) -> None:
        window_call = self.body.index('run_funded_canary_window "${funded_window_label}"')
        report = self.body.index("write_continuous_daily_report", window_call)
        stop_checks = self.body.index('if [[ "${CONTINUOUS_TRADING_CONFIRMED}" != "YES" ]]; then', window_call)
        # Written before the loop can break, so a stopped run still has the day.
        self.assertLess(report, stop_checks)


class RiskResumeCallbackTests(unittest.IsolatedAsyncioTestCase):
    """A resume is the boundary between two windows, so it has to be observable.

    The trading process does not perform the resume -- `arbitrage-admin` does, in
    another process -- so the only way the engine learns that a new window has
    begun is the durable-state refresh it already runs every second.
    """

    class RecordingStore:
        def __init__(self, state: dict[str, Any]) -> None:
            self.state = state

        async def load_risk_state(self) -> dict[str, Any]:
            return self.state

        async def save_risk_state(self, state: dict[str, Any]) -> None:
            self.state = dict(state)

    @staticmethod
    def controller(store: Any = None) -> GlobalRiskController:
        return GlobalRiskController(10.0, 3, state_store=store)

    async def test_resume_announces_that_a_window_started(self) -> None:
        risk = self.controller()
        resumed: list[str] = []
        risk.register_resume_callback(lambda: _append(resumed, "resumed"))
        await risk.pause("funded_canary_window_complete")

        await risk.resume()

        self.assertEqual(resumed, ["resumed"])

    async def test_resuming_a_running_controller_announces_nothing(self) -> None:
        risk = self.controller()
        resumed: list[str] = []
        risk.register_resume_callback(lambda: _append(resumed, "resumed"))

        await risk.resume()

        self.assertEqual(resumed, [])

    async def test_a_failing_callback_cannot_leave_trading_paused(self) -> None:
        risk = self.controller()

        async def explode() -> None:
            raise RuntimeError("callback is broken")

        risk.register_resume_callback(explode)
        await risk.pause("funded_canary_window_complete")

        await risk.resume()

        self.assertFalse(risk.is_paused())

    async def test_an_external_resume_reaches_this_process(self) -> None:
        # `arbitrage-admin risk resume` writes the durable state; the engine
        # picks it up here. Without this the next window would never open.
        store = self.RecordingStore({"paused": True, "pause_reason": "funded_canary_window_complete"})
        risk = self.controller(store)
        resumed: list[str] = []
        risk.register_resume_callback(lambda: _append(resumed, "resumed"))
        await risk.refresh_from_store()
        self.assertTrue(risk.is_paused())

        store.state = {"paused": False, "pause_reason": None}
        await risk.refresh_from_store()

        self.assertFalse(risk.is_paused())
        self.assertEqual(resumed, ["resumed"])

    async def test_an_external_pause_does_not_announce_a_resume(self) -> None:
        store = self.RecordingStore({"paused": False, "pause_reason": None})
        risk = self.controller(store)
        resumed: list[str] = []
        paused: list[str] = []
        risk.register_resume_callback(lambda: _append(resumed, "resumed"))
        risk.register_pause_callback(lambda: _append(paused, "paused"))
        await risk.refresh_from_store()

        store.state = {"paused": True, "pause_reason": "operator_requested_stop"}
        await risk.refresh_from_store()

        self.assertEqual(paused, ["paused"])
        self.assertEqual(resumed, [])


async def _append(sink: list[str], value: str) -> None:
    sink.append(value)


if __name__ == "__main__":
    unittest.main()
