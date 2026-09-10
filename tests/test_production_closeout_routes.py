"""Contract tests for the funded-route allowlist in ops/production_closeout.sh.

The allowlist is the release-side counterpart to ``funded_routes`` in the
runtime config: it decides which routes this release is permitted to fund with
real money. A hole in it is a hole in the money guard, so the accept/reject
behaviour is pinned here rather than left to review.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
CLOSEOUT_SCRIPT = REPO_ROOT / "ops" / "production_closeout.sh"

_HARNESS = """
set -uo pipefail
source "$1"
shift
target=$1
shift
STUB_ROUTES=("$@")
target_routes() {{ printf '%s\\n' ${{STUB_ROUTES+"${{STUB_ROUTES[@]}}"}}; }}
declare -a resolved=()
read_target_routes "${{target}}" resolved || exit $?
printf '%s\\n' ${{resolved+"${{resolved[@]}}"}}
"""


def _bash_path(path: Path) -> str:
    resolved = str(path.resolve())
    if os.name != "nt":
        return resolved
    drive, tail = os.path.splitdrive(resolved)
    return f"/{drive[0].lower()}{tail.replace(chr(92), '/')}"


def _declared_funded_routes(target: str) -> list[str]:
    """The funded set this release declares, read from the script itself.

    Restating it here would make these tests fail whenever the set changes,
    which is exactly when their accept/reject behaviour needs to keep working.
    """
    source = CLOSEOUT_SCRIPT.read_text(encoding="utf-8")
    marker = f"{target.upper()}_EXPECTED_FUNDED_ROUTES=(\n"
    start = source.index(marker) + len(marker)
    end = source.index(")", start)
    return [line.strip() for line in source[start:end].splitlines() if line.strip()]


def _extract_allowlist(destination: Path) -> Path:
    """Copy the allowlist block out of the closeout script so it can be sourced."""
    source = CLOSEOUT_SCRIPT.read_text(encoding="utf-8")
    start = source.index("# The exact funded route set each release target is proven for")
    end = source.index("resolve_targets() {")
    destination.write_text(source[start:end], encoding="utf-8")
    return destination


@unittest.skipIf(shutil.which("bash") is None, "bash is required for ops script contracts")
class FundedRouteAllowlistTests(unittest.TestCase):
    _tmp: Path
    declared: list[str]

    @classmethod
    def setUpClass(cls) -> None:
        cls._tmp = Path(os.environ.get("TEMP", "/tmp")) / "arbitrage-closeout-allowlist.sh"
        _extract_allowlist(cls._tmp)
        cls.declared = _declared_funded_routes("quote_arb")
        assert cls.declared, "the release must declare a funded set for quote_arb"

    def _run(self, target: str, routes: list[str]) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["bash", "-c", _HARNESS.format(), "_", _bash_path(self._tmp), target, *routes],
            capture_output=True,
            text=True,
            timeout=60,
        )

    def assert_accepted(self, target: str, routes: list[str]) -> None:
        result = self._run(target, routes)
        self.assertEqual(result.returncode, 0, msg=result.stderr)
        self.assertEqual(sorted(result.stdout.split()), sorted(routes))

    def assert_rejected(self, target: str, routes: list[str], *, because: str) -> None:
        result = self._run(target, routes)
        self.assertNotEqual(result.returncode, 0, msg=f"expected rejection: {because}")
        self.assertIn(because, result.stderr)

    def test_declared_funded_set_is_accepted_in_any_order(self) -> None:
        self.assert_accepted("quote_arb", list(self.declared))
        self.assert_accepted("quote_arb", list(reversed(self.declared)))

    def test_route_the_release_does_not_declare_cannot_be_funded(self) -> None:
        # The whole point of the guard: enabling a route in config must not be
        # enough to fund it. Promotion requires a tracked change to the release.
        self.assert_rejected(
            "quote_arb",
            [*self.declared, "polymarket_opinion"],
            because="unexpected funded route",
        )

    def test_no_trade_routes_cannot_be_funded(self) -> None:
        for route in ("predict_myriad", "sx_myriad"):
            self.assert_rejected(
                "quote_arb",
                [*self.declared[:-1], route],
                because="unexpected funded route",
            )

    def test_every_opinion_route_is_unfunded_in_this_release(self) -> None:
        for route in ("polymarket_opinion", "predict_opinion", "sx_opinion", "opinion_myriad"):
            self.assert_rejected(
                "quote_arb",
                [*self.declared[:-1], route],
                because="unexpected funded route",
            )

    def test_missing_declared_route_is_rejected(self) -> None:
        self.assert_rejected(
            "quote_arb",
            list(self.declared[:-1]),
            because="missing funded route",
        )

    def test_duplicate_route_is_rejected(self) -> None:
        self.assert_rejected(
            "quote_arb",
            [self.declared[0], *self.declared],
            because="duplicate funded route",
        )

    def test_unknown_route_name_is_rejected(self) -> None:
        self.assert_rejected(
            "quote_arb",
            [*self.declared[:-1], "not_a_route"],
            because="unexpected funded route",
        )

    def test_empty_funded_set_is_rejected_for_quote_arb(self) -> None:
        self.assert_rejected("quote_arb", [], because="missing funded route")

    def test_clob_hft_must_never_fund_a_route(self) -> None:
        self.assert_accepted("clob_hft", [])
        self.assert_rejected("clob_hft", ["polymarket_myriad"], because="unexpected funded route")

    def test_unknown_release_target_is_rejected(self) -> None:
        self.assert_rejected("bogus_target", ["polymarket_myriad"], because="unknown release target")


if __name__ == "__main__":
    unittest.main()
