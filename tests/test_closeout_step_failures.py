"""A failing closeout step must leave enough behind to diagnose it.

`run_and_capture` pipes a step's stdout through `tee`. Under `set -o pipefail`
a failing step aborts the run, but `tee` has already created the artifact — so
the only trace used to be a zero-byte `.json`, with the reason written to a
terminal nobody kept. That is exactly what happened to the
`discovery-overlap-post-approval` step, and it left the failure undiagnosable
after the fact.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
CLOSEOUT_SCRIPT = REPO_ROOT / "ops" / "production_closeout.sh"

# Source just the helper, supplying the globals it closes over.
_HARNESS = """
set -Eeuo pipefail
run_dir="$1"
shift
script_python=("$1")
shift
eval "$(cat "$1")"
shift
mkdir -p "${run_dir}/quote_arb"
run_and_capture quote_arb "$@"
"""


def _bash_path(path: Path) -> str:
    resolved = str(path.resolve())
    if os.name != "nt":
        return resolved
    drive, tail = os.path.splitdrive(resolved)
    return f"/{drive[0].lower()}{tail.replace(chr(92), '/')}"


def _extract_helper(destination: Path) -> Path:
    source = CLOSEOUT_SCRIPT.read_text(encoding="utf-8")
    start = source.index("run_and_capture() {")
    end = source.index("require_full_capacity_funding_ready() {")
    destination.write_text(source[start:end], encoding="utf-8")
    return destination


@unittest.skipIf(shutil.which("bash") is None, "bash is required for ops script contracts")
class CloseoutStepFailureTests(unittest.TestCase):
    helper: Path
    run_dir: Path

    @classmethod
    def setUpClass(cls) -> None:
        base = Path(os.environ.get("TEMP", "/tmp")) / "arbitrage-closeout-step"
        base.mkdir(parents=True, exist_ok=True)
        cls.helper = _extract_helper(base / "run_and_capture.sh")
        cls.run_dir = base / "run"

    def setUp(self) -> None:
        if self.run_dir.exists():
            shutil.rmtree(self.run_dir)
        self.run_dir.mkdir(parents=True)

    def _run(self, name: str, *command: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [
                "bash",
                "-c",
                _HARNESS,
                "_",
                _bash_path(self.run_dir),
                _bash_path(Path(sys.executable)),
                _bash_path(self.helper),
                name,
                *command,
            ],
            capture_output=True,
            text=True,
            timeout=90,
        )

    def _artifact(self, name: str) -> Path:
        return self.run_dir / "quote_arb" / name

    def test_successful_step_still_captures_stdout_as_json(self) -> None:
        result = self._run("overlap", "printf", '{"routes": {}}')

        self.assertEqual(result.returncode, 0, msg=result.stderr)
        self.assertEqual(json.loads(self._artifact("overlap.json").read_text()), {"routes": {}})
        self.assertFalse(self._artifact("overlap.failure.json").exists())

    def test_failing_step_still_aborts_the_run(self) -> None:
        # The fail-closed behaviour is the point; diagnosability must not soften it.
        result = self._run("overlap", "sh", "-c", "echo boom >&2; exit 3")

        self.assertEqual(result.returncode, 3)

    def test_failing_step_records_the_reason_it_used_to_lose(self) -> None:
        self._run("overlap", "sh", "-c", "echo 'MemoryError: catalogue' >&2; exit 3")

        failure = json.loads(self._artifact("overlap.failure.json").read_text())
        self.assertEqual(failure["exit_status"], 3)
        self.assertEqual(failure["step"], "overlap")
        self.assertEqual(failure["target"], "quote_arb")
        self.assertIn("MemoryError: catalogue", failure["stderr_tail"])
        # The zero-byte stdout is recorded rather than being the only evidence.
        self.assertEqual(failure["stdout_bytes"], 0)

    def test_stderr_is_written_beside_the_artifact(self) -> None:
        self._run("overlap", "sh", "-c", "echo 'connection reset' >&2; exit 1")

        self.assertIn("connection reset", self._artifact("overlap.stderr.log").read_text())

    def test_failure_tail_reaches_the_console_too(self) -> None:
        result = self._run("overlap", "sh", "-c", "echo 'OOM killed' >&2; exit 137")

        self.assertIn("failed with exit 137", result.stderr)
        self.assertIn("OOM killed", result.stderr)

    def test_step_that_emits_output_then_fails_keeps_both(self) -> None:
        self._run("overlap", "sh", "-c", """echo '{"partial": true}'; echo late >&2; exit 2""")

        self.assertIn("partial", self._artifact("overlap.json").read_text())
        failure = json.loads(self._artifact("overlap.failure.json").read_text())
        self.assertEqual(failure["exit_status"], 2)
        self.assertGreater(failure["stdout_bytes"], 0)


if __name__ == "__main__":
    unittest.main()
