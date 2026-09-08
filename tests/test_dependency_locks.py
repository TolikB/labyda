import unittest
from pathlib import Path

from packaging.requirements import Requirement


class DependencyLockPlatformTests(unittest.TestCase):
    def test_pywin32_is_installed_only_on_windows(self) -> None:
        repo_root = Path(__file__).resolve().parents[1]
        for filename in ("requirements.lock", "requirements-dev.lock"):
            with self.subTest(lockfile=filename):
                declaration = next(
                    line.removesuffix("\\").strip()
                    for line in (repo_root / filename).read_text(encoding="utf-8").splitlines()
                    if line.startswith("pywin32==")
                )
                requirement = Requirement(declaration)
                self.assertIsNotNone(requirement.marker, "Windows-only dependency must be platform-gated")
                assert requirement.marker is not None
                for platform in ("linux", "darwin", "win32"):
                    with self.subTest(platform=platform):
                        self.assertEqual(
                            requirement.marker.evaluate({"sys_platform": platform}),
                            platform == "win32",
                        )
