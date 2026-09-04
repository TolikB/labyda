from __future__ import annotations

import os
import shutil
import stat
import subprocess
import tempfile
import unittest
from collections.abc import Callable
from pathlib import Path

_RELEASE_SHA = "a" * 40


def _write_executable(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")
    path.chmod(path.stat().st_mode | stat.S_IXUSR)


def _deploy_harness(
    tmp_path: Path,
    *,
    policy: str,
    clob_mode: str,
    quote_mode: str,
    live_confirm: str,
    health_retries: str | None = "1",
    bootstrap_health_retries: str | None = None,
    fail_second_pause: bool = False,
    replace_script_on_first_pull: bool = False,
) -> tuple[subprocess.CompletedProcess[str], list[str], list[str], list[str], list[str]]:
    source_root = Path(__file__).resolve().parents[1]
    repo = tmp_path / "repo"
    fake_bin = tmp_path / "bin"
    (repo / ".git").mkdir(parents=True)
    (repo / "ops").mkdir()
    (repo / "scripts").mkdir()
    fake_bin.mkdir()
    shutil.copy2(source_root / "ops" / "deploy_compose.sh", repo / "ops" / "deploy_compose.sh")
    (repo / "docker-compose.yml").write_text("services: {}\n", encoding="utf-8")
    (repo / ".env.production").write_text("# fake deploy environment\n", encoding="utf-8")

    docker_log = tmp_path / "docker.log"
    operator_log = tmp_path / "operator.log"
    operator_count = tmp_path / "operator.count"
    health_log = tmp_path / "health.log"
    seq_log = tmp_path / "seq.log"
    pull_count = tmp_path / "pull.count"
    reexec_marker = tmp_path / "reexec.marker"

    replacement_script = tmp_path / "replacement-deploy.sh"
    replacement = (source_root / "ops" / "deploy_compose.sh").read_text(encoding="utf-8")
    replacement = replacement.replace(
        "# Read only the allowlisted runtime controls",
        'printf "verified-checkout\\n" >>"${FAKE_REEXEC_MARKER:-/dev/null}"\n\n'
        "# Read only the allowlisted runtime controls",
        1,
    )
    replacement_script.write_text(replacement, encoding="utf-8")

    _write_executable(
        fake_bin / "git",
        f"""#!/usr/bin/env python3
import os
import shutil
import sys
from pathlib import Path

if sys.argv[1:3] == ["rev-parse", "HEAD"]:
    print("{_RELEASE_SHA}")
elif sys.argv[1:2] == ["pull"] and os.environ.get("FAKE_REPLACE_SCRIPT") == "YES":
    count_path = Path(os.environ["FAKE_PULL_COUNT"])
    count = int(count_path.read_text(encoding="utf-8")) + 1 if count_path.exists() else 1
    count_path.write_text(str(count), encoding="utf-8")
    if count == 1:
        shutil.copyfile(os.environ["FAKE_REPLACEMENT_SCRIPT"], os.environ["FAKE_DEPLOY_SCRIPT"])
""",
    )
    _write_executable(
        fake_bin / "docker",
        """#!/usr/bin/env python3
import json
import os
import sys
from pathlib import Path

Path(os.environ["FAKE_DOCKER_LOG"]).open("a", encoding="utf-8").write(" ".join(sys.argv[1:]) + "\\n")
if sys.argv[-3:] == ["config", "--format", "json"]:
    print(json.dumps({"services": {
        "bot-clob-hft": {"environment": {
            "ARBITRAGE_EXECUTION_MODE_OVERRIDE": os.environ["FAKE_CLOB_MODE"],
            "LIVE_TRADING_CONFIRM": os.environ["FAKE_LIVE_CONFIRM"],
        }},
        "bot-quote-arb": {"environment": {
            "ARBITRAGE_EXECUTION_MODE_OVERRIDE": os.environ["FAKE_QUOTE_MODE"],
            "LIVE_TRADING_CONFIRM": os.environ["FAKE_LIVE_CONFIRM"],
        }},
    }}))
""",
    )
    _write_executable(
        repo / "ops" / "operator_python.sh",
        """#!/usr/bin/env python3
import json
import os
import sys
from pathlib import Path

log = Path(os.environ["FAKE_OPERATOR_LOG"])
log.open("a", encoding="utf-8").write(" ".join(sys.argv[1:]) + "\\n")
count_path = Path(os.environ["FAKE_OPERATOR_COUNT"])
count = int(count_path.read_text(encoding="utf-8")) + 1 if count_path.exists() else 1
count_path.write_text(str(count), encoding="utf-8")
paused = not (os.environ.get("FAKE_FAIL_SECOND_PAUSE") == "YES" and count == 2)
print(json.dumps({"paused": paused}))
""",
    )
    _write_executable(
        repo / "scripts" / "runtime_health_gate.py",
        """#!/usr/bin/env python3
import os
import sys
from pathlib import Path

Path(os.environ["FAKE_HEALTH_LOG"]).open("a", encoding="utf-8").write(" ".join(sys.argv[1:]) + "\\n")
""",
    )
    _write_executable(
        fake_bin / "seq",
        """#!/usr/bin/env python3
import os
import sys
from pathlib import Path

Path(os.environ["FAKE_SEQ_LOG"]).open("a", encoding="utf-8").write(" ".join(sys.argv[1:]) + "\\n")
print("1")
""",
    )

    env = os.environ.copy()
    env.update(
        {
            "PATH": f"{fake_bin}{os.pathsep}{env['PATH']}",
            "REPO_DIR": str(repo),
            "BRANCH": "verified",
            "CI_VERIFIED_COMMIT_SHA": _RELEASE_SHA,
            "HEALTH_SLEEP_SECONDS": "0",
            "DEPLOY_HEALTH_POLICY": policy,
            "FAKE_DOCKER_LOG": str(docker_log),
            "FAKE_OPERATOR_LOG": str(operator_log),
            "FAKE_OPERATOR_COUNT": str(operator_count),
            "FAKE_HEALTH_LOG": str(health_log),
            "FAKE_SEQ_LOG": str(seq_log),
            "FAKE_CLOB_MODE": clob_mode,
            "FAKE_QUOTE_MODE": quote_mode,
            "FAKE_LIVE_CONFIRM": live_confirm,
            "FAKE_FAIL_SECOND_PAUSE": "YES" if fail_second_pause else "NO",
            "FAKE_REPLACE_SCRIPT": "YES" if replace_script_on_first_pull else "NO",
            "FAKE_PULL_COUNT": str(pull_count),
            "FAKE_REEXEC_MARKER": str(reexec_marker),
            "FAKE_REPLACEMENT_SCRIPT": str(replacement_script),
            "FAKE_DEPLOY_SCRIPT": str(repo / "ops" / "deploy_compose.sh"),
        }
    )
    if health_retries is None:
        env.pop("HEALTH_RETRIES", None)
    else:
        env["HEALTH_RETRIES"] = health_retries
    if bootstrap_health_retries is None:
        env.pop("BOOTSTRAP_HEALTH_RETRIES", None)
    else:
        env["BOOTSTRAP_HEALTH_RETRIES"] = bootstrap_health_retries
    result = subprocess.run(
        ["bash", "ops/deploy_compose.sh"],
        cwd=repo,
        env=env,
        check=False,
        capture_output=True,
        text=True,
    )
    docker_lines = docker_log.read_text(encoding="utf-8").splitlines() if docker_log.exists() else []
    operator_lines = operator_log.read_text(encoding="utf-8").splitlines() if operator_log.exists() else []
    health_lines = health_log.read_text(encoding="utf-8").splitlines() if health_log.exists() else []
    seq_lines = seq_log.read_text(encoding="utf-8").splitlines() if seq_log.exists() else []
    return result, docker_lines, operator_lines, health_lines, seq_lines


def _assert_safe_paused_deploy_fences_and_verifies_both_runtimes(tmp_path: Path) -> None:
    result, docker_lines, operator_lines, health_lines, _ = _deploy_harness(
        tmp_path,
        policy="safe_paused_shadow",
        clob_mode="shadow",
        quote_mode="shadow",
        live_confirm="NO",
    )

    assert result.returncode == 0, result.stderr
    migrate = next(index for index, line in enumerate(docker_lines) if "run --rm migrate" in line)
    stop = next(index for index, line in enumerate(docker_lines) if "stop bot-clob-hft bot-quote-arb" in line)
    recreate = next(index for index, line in enumerate(docker_lines) if "up -d --build" in line)
    assert stop < migrate < recreate
    assert len(operator_lines) == 2
    assert "config.production.clob_hft.json risk pause" in operator_lines[0]
    assert "config.production.quote_arb.json risk pause" in operator_lines[1]
    assert all("--expected-mode shadow" in line for line in health_lines)


def _assert_bootstrap_paused_deploy_uses_same_fences_with_explicit_policy(tmp_path: Path) -> None:
    result, docker_lines, operator_lines, health_lines, _ = _deploy_harness(
        tmp_path,
        policy="safe_paused_shadow_bootstrap",
        clob_mode="shadow",
        quote_mode="shadow",
        live_confirm="NO",
    )

    assert result.returncode == 0, result.stderr
    stop = next(index for index, line in enumerate(docker_lines) if "stop bot-clob-hft bot-quote-arb" in line)
    migrate = next(index for index, line in enumerate(docker_lines) if "run --rm migrate" in line)
    recreate = next(index for index, line in enumerate(docker_lines) if "up -d --build" in line)
    assert stop < migrate < recreate
    assert len(operator_lines) == 2
    assert all("safe_paused_shadow_bootstrap_deploy:" in line for line in operator_lines)
    assert all("--accept safe_paused_shadow_bootstrap" in line for line in health_lines)


def _assert_safe_paused_deploy_rejects_resolved_live_controls_before_migration(tmp_path: Path) -> None:
    result, docker_lines, operator_lines, _, _ = _deploy_harness(
        tmp_path,
        policy="safe_paused_shadow",
        clob_mode="canary",
        quote_mode="shadow",
        live_confirm="YES",
    )

    assert result.returncode != 0
    assert not any("run --rm migrate" in line for line in docker_lines)
    assert not any("up -d --build" in line for line in docker_lines)
    assert operator_lines == []


def _assert_safe_paused_deploy_leaves_bots_stopped_when_second_pause_is_not_verified(tmp_path: Path) -> None:
    result, docker_lines, operator_lines, _, _ = _deploy_harness(
        tmp_path,
        policy="safe_paused_shadow",
        clob_mode="shadow",
        quote_mode="shadow",
        live_confirm="NO",
        fail_second_pause=True,
    )

    assert result.returncode != 0
    assert any("stop bot-clob-hft bot-quote-arb" in line for line in docker_lines)
    assert not any("up -d --build" in line for line in docker_lines)
    assert len(operator_lines) == 2


def _assert_ready_policy_keeps_resolved_canary_mode_without_pause_flow(tmp_path: Path) -> None:
    result, docker_lines, operator_lines, health_lines, _ = _deploy_harness(
        tmp_path,
        policy="ready",
        clob_mode="canary",
        quote_mode="canary",
        live_confirm="YES",
    )

    assert result.returncode == 0, result.stderr
    stop = next(index for index, line in enumerate(docker_lines) if "stop bot-clob-hft bot-quote-arb" in line)
    migrate = next(index for index, line in enumerate(docker_lines) if "run --rm migrate" in line)
    recreate = next(index for index, line in enumerate(docker_lines) if "up -d --build" in line)
    assert stop < migrate < recreate
    assert any("up -d --build" in line for line in docker_lines)
    assert operator_lines == []
    assert all("--expected-mode canary" in line for line in health_lines)


def _assert_deploy_reexecutes_script_replaced_by_pull(tmp_path: Path) -> None:
    result, _, _, _, _ = _deploy_harness(
        tmp_path,
        policy="safe_paused_shadow",
        clob_mode="shadow",
        quote_mode="shadow",
        live_confirm="NO",
        replace_script_on_first_pull=True,
    )

    assert result.returncode == 0, result.stderr
    assert (tmp_path / "reexec.marker").read_text(encoding="utf-8").splitlines() == ["verified-checkout"]


def _assert_health_retry_defaults_and_overrides(tmp_path: Path) -> None:
    cases = (
        ("ready-default", "ready", None, None, False, "1 120"),
        ("paused-default", "safe_paused_shadow", None, None, False, "1 120"),
        ("bootstrap-default", "safe_paused_shadow_bootstrap", None, None, False, "1 600"),
        ("bootstrap-override", "safe_paused_shadow_bootstrap", None, "9", False, "1 9"),
        ("explicit-wins", "safe_paused_shadow_bootstrap", "7", "9", False, "1 7"),
        ("bootstrap-reexec", "safe_paused_shadow_bootstrap", None, None, True, "1 600"),
        ("explicit-reexec", "safe_paused_shadow_bootstrap", "7", "9", True, "1 7"),
    )
    for name, policy, retries, bootstrap_retries, reexec, expected_seq in cases:
        result, _, _, _, seq_lines = _deploy_harness(
            tmp_path / name,
            policy=policy,
            clob_mode="shadow" if policy != "ready" else "canary",
            quote_mode="shadow" if policy != "ready" else "canary",
            live_confirm="NO" if policy != "ready" else "YES",
            health_retries=retries,
            bootstrap_health_retries=bootstrap_retries,
            replace_script_on_first_pull=reexec,
        )

        assert result.returncode == 0, result.stderr
        assert seq_lines == [expected_seq]


@unittest.skipUnless(shutil.which("bash") is not None, "bash is required")
class DeployComposeScriptTests(unittest.TestCase):
    def _run_assertion(self, assertion: Callable[[Path], None]) -> None:
        with tempfile.TemporaryDirectory() as directory:
            assertion(Path(directory))

    def test_safe_paused_deploy_fences_and_verifies_both_runtimes(self) -> None:
        self._run_assertion(_assert_safe_paused_deploy_fences_and_verifies_both_runtimes)

    def test_bootstrap_paused_deploy_uses_same_fences_with_explicit_policy(self) -> None:
        self._run_assertion(_assert_bootstrap_paused_deploy_uses_same_fences_with_explicit_policy)

    def test_safe_paused_deploy_rejects_resolved_live_controls_before_migration(self) -> None:
        self._run_assertion(_assert_safe_paused_deploy_rejects_resolved_live_controls_before_migration)

    def test_safe_paused_deploy_leaves_bots_stopped_when_second_pause_is_not_verified(self) -> None:
        self._run_assertion(_assert_safe_paused_deploy_leaves_bots_stopped_when_second_pause_is_not_verified)

    def test_ready_policy_keeps_resolved_canary_mode_without_pause_flow(self) -> None:
        self._run_assertion(_assert_ready_policy_keeps_resolved_canary_mode_without_pause_flow)

    def test_deploy_reexecutes_script_replaced_by_pull(self) -> None:
        self._run_assertion(_assert_deploy_reexecutes_script_replaced_by_pull)

    def test_health_retry_defaults_and_overrides_survive_reexec(self) -> None:
        self._run_assertion(_assert_health_retry_defaults_and_overrides)


if __name__ == "__main__":
    unittest.main()
