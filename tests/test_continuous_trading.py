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
import re
import shutil
import subprocess
import sys
import unittest
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
from types import ModuleType
from typing import Any, cast

from arbitrage_engine.risk import (
    API_ERROR_PAUSE_SUFFIX,
    DAILY_LOSS_PAUSE_PREFIX,
    GlobalRiskController,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
NEWLINE = chr(10)
CLOSEOUT_SCRIPT = REPO_ROOT / "ops" / "production_closeout.sh"


def _load_script_module(name: str, filename: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, REPO_ROOT / "scripts" / filename)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def _bash_path(path: Path) -> str:
    resolved = str(path.resolve())
    if os.name != "nt":
        return resolved
    drive, tail = os.path.splitdrive(resolved)
    return f"/{drive[0].lower()}{tail.replace(chr(92), '/')}"


window_gate = _load_script_module("continuous_window_gate_module", "continuous_window_gate.py")
daily_report = _load_script_module("continuous_daily_report_module", "continuous_daily_report.py")
pruner = _load_script_module("prune_closeout_artifacts_module", "prune_closeout_artifacts.py")


class ContinuousWindowGateTests(unittest.TestCase):
    """Three outcomes, and the line between "wait" and "somebody has to look"."""

    NOW = datetime(2026, 9, 9, 14, 30, tzinfo=UTC)
    DAILY_LOSS_REASON = f"{DAILY_LOSS_PAUSE_PREFIX}$12.00 reached limit $10.00"
    API_ERROR_REASON = f"3{API_ERROR_PAUSE_SUFFIX}"

    @staticmethod
    def snapshot(
        *,
        paused: bool = True,
        pause_reason: str | None = "funded_canary_window_complete",
        eligible: bool = True,
        blocking_reasons: list[str] | None = None,
        loss_day: str | None = "2026-09-09",
    ) -> dict[str, Any]:
        return {
            "risk_state": {
                "paused": paused,
                "pause_reason": pause_reason,
                "daily_loss_usd": "0",
                "loss_day": loss_day,
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
        return cast(
            "dict[str, Any]",
            window_gate.evaluate(self.snapshot(**kwargs), now=self.NOW, api_error_hold_seconds=900),
        )

    def test_a_completed_window_may_be_followed_by_another(self) -> None:
        decision = self.evaluate()

        self.assertEqual(decision["verdict"], "repeat")
        self.assertEqual(decision["blocking_reasons"], [])

    def test_the_daily_loss_limit_holds_until_the_utc_day_rolls(self) -> None:
        # The limit is a stop for the day, not for good: the controller itself
        # lets `resume` through on a new UTC day and zeroes the accumulator.
        decision = self.evaluate(pause_reason=self.DAILY_LOSS_REASON, loss_day="2026-09-09")

        self.assertEqual(decision["verdict"], "hold")
        self.assertEqual(decision["hold_kind"], "daily_loss")
        self.assertEqual(decision["hold_until"], "2026-09-10T00:01:00+00:00")

    def test_yesterdays_daily_loss_pause_no_longer_holds(self) -> None:
        decision = self.evaluate(pause_reason=self.DAILY_LOSS_REASON, loss_day="2026-09-08")

        self.assertEqual(decision["verdict"], "repeat")

    def test_a_daily_loss_pause_without_a_day_is_not_guessed_at(self) -> None:
        # Without the day there is no way to know when the limit expires, and
        # inventing one would resume trading early.
        decision = self.evaluate(pause_reason=self.DAILY_LOSS_REASON, loss_day=None)

        self.assertEqual(decision["verdict"], "stop")

    def test_api_errors_hold_for_the_configured_cooldown(self) -> None:
        decision = self.evaluate(pause_reason=self.API_ERROR_REASON)

        self.assertEqual(decision["verdict"], "hold")
        self.assertEqual(decision["hold_kind"], "api_errors")
        self.assertEqual(decision["hold_until_unix"], (self.NOW + timedelta(seconds=900)).timestamp())

    def test_an_unknown_order_outcome_stops_the_loop(self) -> None:
        decision = self.evaluate(pause_reason="unknown order outcome: Polymarket client_order_id=abc")

        self.assertEqual(decision["verdict"], "stop")

    def test_an_operator_pause_stops_the_loop(self) -> None:
        decision = self.evaluate(pause_reason="operator_requested_stop")

        self.assertEqual(decision["verdict"], "stop")

    def test_an_unrecognised_reason_stops_the_loop(self) -> None:
        decision = self.evaluate(pause_reason="something nobody has written yet")

        self.assertEqual(decision["verdict"], "stop")

    def test_a_running_runtime_is_not_a_state_to_start_from(self) -> None:
        decision = self.evaluate(paused=False, pause_reason=None)

        self.assertEqual(decision["verdict"], "stop")
        self.assertIn("runtime_is_not_durably_paused", decision["blocking_reasons"])

    def test_unresolved_money_outranks_a_recoverable_reason(self) -> None:
        # Waiting out a hold would not resolve an UNKNOWN intent, and the next
        # window's `risk resume` would refuse anyway.
        decision = self.evaluate(
            pause_reason=self.DAILY_LOSS_REASON,
            eligible=False,
            blocking_reasons=["unresolved_order_intents:2"],
        )

        self.assertEqual(decision["verdict"], "stop")
        self.assertIn("unresolved_order_intents:2", decision["blocking_reasons"])

    def test_unresolved_money_outranks_even_a_completed_window(self) -> None:
        decision = self.evaluate(
            eligible=False,
            blocking_reasons=["reconciliation_failures:1"],
        )

        self.assertEqual(decision["verdict"], "stop")

    def test_a_missing_resume_gate_is_treated_as_ineligible(self) -> None:
        decision = window_gate.evaluate(
            {"risk_state": {"paused": True, "pause_reason": "funded_canary_window_complete"}}
        )

        self.assertEqual(decision["verdict"], "stop")
        self.assertIn("operator_resume_gate_not_eligible", decision["blocking_reasons"])

    def test_an_empty_snapshot_never_repeats(self) -> None:
        self.assertEqual(window_gate.evaluate({})["verdict"], "stop")

    def test_exit_status_carries_the_verdict(self) -> None:
        # The wrapper reads the status, so a crashing gate must read as stop.
        self.assertEqual(window_gate.EXIT_REPEAT, 0)
        self.assertEqual(window_gate.EXIT_HOLD, 10)
        self.assertNotIn(window_gate.EXIT_STOP, (0, 10))


class PauseReasonContractTests(unittest.IsolatedAsyncioTestCase):
    """The gate classifies reason strings, so the strings are a contract.

    Both sides import the same constants; these tests prove the controller
    actually produces something the constants match, which a shared constant
    alone does not guarantee.
    """

    async def test_the_daily_loss_pause_reason_starts_with_its_prefix(self) -> None:
        risk = GlobalRiskController(10.0, 3)

        await risk.record_realized_result(Decimal("-12"))

        self.assertTrue(risk.is_paused())
        assert risk.pause_reason is not None
        self.assertTrue(risk.pause_reason.startswith(DAILY_LOSS_PAUSE_PREFIX))

    async def test_the_api_error_pause_reason_ends_with_its_suffix(self) -> None:
        risk = GlobalRiskController(10.0, 3)

        for _ in range(3):
            await risk.record_api_error()

        self.assertTrue(risk.is_paused())
        assert risk.pause_reason is not None
        self.assertTrue(risk.pause_reason.endswith(API_ERROR_PAUSE_SUFFIX))

    async def test_the_two_reasons_cannot_be_confused_for_each_other(self) -> None:
        loss = GlobalRiskController(10.0, 3)
        await loss.record_realized_result(Decimal("-12"))
        errors = GlobalRiskController(10.0, 3)
        for _ in range(3):
            await errors.record_api_error()

        assert loss.pause_reason is not None
        assert errors.pause_reason is not None
        self.assertFalse(loss.pause_reason.endswith(API_ERROR_PAUSE_SUFFIX))
        self.assertFalse(errors.pause_reason.startswith(DAILY_LOSS_PAUSE_PREFIX))


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
            "continuous_window_verdict",
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


class ArtifactRetentionTests(unittest.TestCase):
    """A continuous run keeps one run directory for weeks; it has to be bounded."""

    def setUp(self) -> None:
        temporary = TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.artifact_root = Path(temporary.name)
        self.run_dir = self.artifact_root / "20260909T000000Z"

    def write_observer_window(self, route: str, stamp: str, *, heavy_bytes: int = 4096) -> Path:
        window = self.run_dir / "quote_arb" / "canary-artifacts" / route / stamp
        (window / "live").mkdir(parents=True, exist_ok=True)
        (window / "ready").mkdir(parents=True, exist_ok=True)
        (window / "metrics").mkdir(parents=True, exist_ok=True)
        (window / "report.json").write_text(json.dumps({"stop_reason": "timeout"}), encoding="utf-8")
        (window / "samples.jsonl").write_text("x" * heavy_bytes, encoding="utf-8")
        (window / "live" / "probe.json").write_text("x" * heavy_bytes, encoding="utf-8")
        return window

    def test_every_report_survives_however_old(self) -> None:
        windows = [self.write_observer_window("polymarket_predict", f"2026090{n}T000000Z") for n in range(1, 6)]

        pruner.prune_run_directory(self.run_dir, keep_windows=2)

        for window in windows:
            self.assertTrue((window / "report.json").is_file(), window)

    def test_heavy_capture_is_dropped_outside_the_keep_window(self) -> None:
        windows = [self.write_observer_window("polymarket_predict", f"2026090{n}T000000Z") for n in range(1, 6)]

        result = pruner.prune_run_directory(self.run_dir, keep_windows=2)

        for window in windows[:-2]:
            self.assertFalse((window / "samples.jsonl").exists(), window)
            self.assertFalse((window / "live").exists(), window)
        for window in windows[-2:]:
            self.assertTrue((window / "samples.jsonl").is_file(), window)
            self.assertTrue((window / "live").is_dir(), window)
        self.assertGreater(result["reclaimed_bytes"], 0)

    def test_the_keep_window_is_counted_per_route(self) -> None:
        # Four routes run in parallel; keeping "the last N windows" globally
        # would keep barely one window's worth of any of them.
        for route in ("polymarket_predict", "polymarket_sx"):
            for day in range(1, 4):
                self.write_observer_window(route, f"2026090{day}T000000Z")

        pruner.prune_run_directory(self.run_dir, keep_windows=2)

        for route in ("polymarket_predict", "polymarket_sx"):
            base = self.run_dir / "quote_arb" / "canary-artifacts" / route
            kept = [d for d in sorted(base.iterdir()) if (d / "samples.jsonl").exists()]
            self.assertEqual([d.name for d in kept], ["20260902T000000Z", "20260903T000000Z"])

    def test_dry_run_changes_nothing(self) -> None:
        window = self.write_observer_window("polymarket_predict", "20260901T000000Z")
        self.write_observer_window("polymarket_predict", "20260902T000000Z")

        pruner.prune_run_directory(self.run_dir, keep_windows=1, dry_run=True)

        self.assertTrue((window / "samples.jsonl").is_file())

    def test_cross_run_retention_is_off_by_default(self) -> None:
        old = self.artifact_root / "20250101T000000Z"
        old.mkdir(parents=True)
        self.run_dir.mkdir(parents=True, exist_ok=True)

        result = pruner.prune_old_runs(self.artifact_root, current_run_dir=self.run_dir, retention_days=0)

        self.assertFalse(result["enabled"])
        self.assertTrue(old.is_dir())

    def test_operator_named_directories_are_never_deleted(self) -> None:
        # closeout-artifacts holds directories a person created by hand --
        # release-*, readiness-*, safe-launch-*. Deleting somebody's evidence to
        # save space is not this script's call.
        self.run_dir.mkdir(parents=True, exist_ok=True)
        operator_dirs = []
        for name in ("release-51b1d371", "readiness-096ecbf7", "goal-opportunity-analysis"):
            path = self.artifact_root / name
            path.mkdir()
            os.utime(path, (0, 0))
            operator_dirs.append(path)

        pruner.prune_old_runs(self.artifact_root, current_run_dir=self.run_dir, retention_days=1)

        for path in operator_dirs:
            self.assertTrue(path.is_dir(), path)

    def test_an_expired_wrapper_run_is_deleted_but_never_the_current_one(self) -> None:
        self.run_dir.mkdir(parents=True, exist_ok=True)
        os.utime(self.run_dir, (0, 0))
        expired = self.artifact_root / "20250101T000000Z"
        expired.mkdir()
        os.utime(expired, (0, 0))

        result = pruner.prune_old_runs(self.artifact_root, current_run_dir=self.run_dir, retention_days=1)

        self.assertFalse(expired.exists())
        self.assertTrue(self.run_dir.is_dir())
        self.assertEqual(result["removed"], [str(expired)])


class ContinuousHoldLoopStructureTests(unittest.TestCase):
    """Waiting out a recoverable pause is what makes a run survive a bad day."""

    body: str
    hold: str

    @classmethod
    def setUpClass(cls) -> None:
        cls.body = CLOSEOUT_SCRIPT.read_text(encoding="utf-8")
        cls.hold = cls.body[
            cls.body.index("continuous_hold_and_recover() {") : cls.body.index("funded_window_index=0")
        ]

    def test_a_hold_verdict_is_distinct_from_a_stop(self) -> None:
        # Exit 10 is the hold; anything unrecognised falls through to stop.
        self.assertIn("    10)" + NEWLINE, self.body)
        self.assertIn("continuous_hold_and_recover", self.body)
        self.assertIn('continuous_stop_reason="window_state_not_repeatable"', self.body)

    def test_the_stop_file_still_works_during_a_hold(self) -> None:
        # A daily-loss hold lasts most of a day; one long sleep would ignore an
        # operator asking it to stop for all of that time.
        self.assertIn('if [[ -e "${CONTINUOUS_STOP_FILE}" ]]; then', self.hold)
        self.assertIn("((remaining > 60)) && remaining=60", self.hold)

    def test_funding_and_release_are_rechecked_before_the_next_window(self) -> None:
        # A losing day can drain the account, and the release must still be the
        # one CI verified.
        integrity = self.hold.index("assert_release_integrity")
        readiness = self.hold.index("all-market-readiness-after-hold")
        funding = self.hold.index("require_full_capacity_funding_ready")
        self.assertLess(integrity, readiness)
        self.assertLess(readiness, funding)

    def test_hold_budgets_are_checked_before_waiting(self) -> None:
        daily = self.hold.index("CONTINUOUS_MAX_DAILY_LOSS_HOLDS")
        api = self.hold.index("CONTINUOUS_MAX_API_ERROR_HOLDS")
        wait = self.hold.index("while :; do")
        self.assertLess(daily, wait)
        self.assertLess(api, wait)

    def test_an_unbounded_daily_loss_budget_is_expressed_as_zero(self) -> None:
        self.assertIn("CONTINUOUS_MAX_DAILY_LOSS_HOLDS=${CONTINUOUS_MAX_DAILY_LOSS_HOLDS:-0}", self.body)
        self.assertIn("((CONTINUOUS_MAX_DAILY_LOSS_HOLDS > 0))", self.hold)

    def test_a_hold_without_a_deadline_stops_rather_than_guessing(self) -> None:
        self.assertIn('continuous_stop_reason="hold_without_deadline"', self.hold)

    def test_hold_counters_reset_after_a_completed_window(self) -> None:
        window_call = self.body.index('run_funded_canary_window "${funded_window_label}"')
        reset = self.body.index("continuous_daily_loss_holds=0", window_call)
        stop_checks = self.body.index('if [[ "${CONTINUOUS_TRADING_CONFIRMED}" != "YES" ]]; then', window_call)
        self.assertLess(reset, stop_checks)

    def test_the_disk_gate_runs_before_a_window_not_after(self) -> None:
        loop = self.body[self.body.index("funded_window_index=0") :]
        disk = loop.index("continuous_disk_has_headroom")
        window = loop.index('run_funded_canary_window "${funded_window_label}"')
        self.assertLess(disk, window)
        self.assertIn('continuous_stop_reason="low_disk"', loop)

    def test_retention_runs_after_every_window(self) -> None:
        loop = self.body[self.body.index("funded_window_index=0") :]
        self.assertIn("prune_continuous_artifacts", loop)
        self.assertLess(
            loop.index("prune_continuous_artifacts"),
            loop.index('if [[ "${CONTINUOUS_TRADING_CONFIRMED}" != "YES" ]]; then'),
        )

    def test_the_operator_is_told_when_the_run_starts_and_stops(self) -> None:
        self.assertIn("Continuous funded trading started", self.body)
        self.assertIn("Continuous funded trading stopped", self.body)
        self.assertIn("${continuous_stop_reason}", self.body)


@unittest.skipIf(shutil.which("bash") is None, "bash is required for ops script contracts")
class SignalSafetyTests(unittest.TestCase):
    """`systemctl stop` must pause the runtime, not orphan it mid-window.

    bash does not run an EXIT trap when it is terminated by an untrapped signal.
    With only `trap ... EXIT`, a SIGTERM killed the wrapper while the runtime
    kept trading with no observer watching it -- the worst state this system can
    be left in.
    """

    body: str

    @classmethod
    def setUpClass(cls) -> None:
        cls.body = CLOSEOUT_SCRIPT.read_text(encoding="utf-8")

    def test_the_trap_covers_the_signals_systemd_sends(self) -> None:
        self.assertIn("trap pause_targets_on_exit EXIT INT TERM", self.body)
        self.assertIn("trap - EXIT INT TERM", self.body)

    def test_sigterm_pauses_every_managed_runtime(self) -> None:
        temporary = TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        marker = Path(temporary.name) / "pauses.log"

        handler = self.body[
            self.body.index("pause_targets_on_exit() {") : self.body.index("run_id=$(date -u")
        ]
        script = NEWLINE.join(
            [
                "set -Eeuo pipefail",
                'record_pause() { printf "%s\\n" "$*" >>"${MARKER}"; }',
                "TARGETS=(quote_arb clob_hft)",
                "admin_cmd=(record_pause)",
                'target_config_path() { printf "config.production.%s.json" "$1"; }',
                handler,
                "pause_on_exit=1",
                "trap pause_targets_on_exit EXIT INT TERM",
                "kill -TERM $$",
                "sleep 10",
            ]
        )

        subprocess.run(
            ["bash", "-c", script],
            cwd=REPO_ROOT,
            env={**os.environ, "MARKER": _bash_path(marker)},
            capture_output=True,
            text=True,
            check=False,
            timeout=30,
        )

        recorded = marker.read_text(encoding="utf-8")
        self.assertIn("production_closeout_exit_fail_closed", recorded)
        self.assertIn("config.production.quote_arb.json", recorded)
        self.assertIn("config.production.clob_hft.json", recorded)


class ContinuousUnitFileTests(unittest.TestCase):
    """The unit is what turns a foreground script into something you can forget."""

    unit: str

    @classmethod
    def setUpClass(cls) -> None:
        cls.unit = (REPO_ROOT / "ops" / "systemd" / "labyda-continuous.service").read_text(encoding="utf-8")

    def test_a_fail_closed_exit_is_not_restarted(self) -> None:
        # Restarting would paper over the conditions the wrapper stops for.
        self.assertIn("Restart=no", self.unit)

    def test_stopping_gives_the_wrapper_time_to_pause_first(self) -> None:
        # KillMode=control-group would tear down the observers alongside the
        # wrapper, before it could pause anything.
        self.assertIn("KillMode=mixed", self.unit)
        self.assertIn("KillSignal=SIGTERM", self.unit)
        self.assertIn("TimeoutStopSec=600", self.unit)

    def test_it_survives_a_reboot(self) -> None:
        self.assertIn("WantedBy=multi-user.target", self.unit)
        self.assertIn("Requires=docker.service", self.unit)

    def test_the_watchdog_timer_runs_often_enough_to_be_useful(self) -> None:
        timer = (REPO_ROOT / "ops" / "systemd" / "labyda-watchdog.timer").read_text(encoding="utf-8")
        self.assertIn("OnUnitActiveSec=5m", timer)
        self.assertIn("WantedBy=timers.target", timer)


class WatchdogScriptTests(unittest.TestCase):
    """It reports what nothing else can: the wrapper being gone."""

    script: str

    @classmethod
    def setUpClass(cls) -> None:
        cls.script = (REPO_ROOT / "ops" / "continuous_watchdog.sh").read_text(encoding="utf-8")

    def test_a_unit_that_was_never_started_does_not_page_anybody(self) -> None:
        # Alerting on "inactive" outright would fire every five minutes forever
        # on a box where the operator has not started trading yet.
        self.assertIn('if [[ "${previous_service_state}" == "active" && "${service_state}" != "active" ]]', self.script)

    def test_it_watches_the_things_the_engine_cannot_report(self) -> None:
        for signal in ("metrics_unreachable", "disk_low", "daily_report_stale"):
            self.assertIn(signal, self.script)

    def test_repeated_problems_are_not_repeated_alerts(self) -> None:
        self.assertIn('"${current_problems}" != "${previous_problems}"', self.script)

    def test_recovery_is_announced_too(self) -> None:
        self.assertIn("watchdog clear", self.script)

    def test_notification_failure_cannot_break_the_watchdog(self) -> None:
        self.assertIn("|| true", self.script)

    def test_alerting_does_not_depend_on_the_engine_package(self) -> None:
        # scripts/notify_operator.py imports arbitrage_engine, which on the
        # compose deployment only exists inside the operator container. A
        # watchdog whose alerting depends on what it is watching is not one.
        executable_lines = [
            line for line in self.script.splitlines() if line.strip() and not line.lstrip().startswith("#")
        ]
        self.assertFalse([line for line in executable_lines if re.search(r"python3?", line)])
        self.assertIn("api.telegram.org", self.script)

    def test_the_bot_token_never_reaches_the_process_list(self) -> None:
        self.assertIn("curl -fsS --max-time 10 -K", self.script)
        self.assertIn("chmod 0600", self.script)


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
