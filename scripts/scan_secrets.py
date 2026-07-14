from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path

SENSITIVE_ASSIGNMENT = re.compile(
    r"""(?ix)
    ["']?
    (?P<name>[A-Z0-9_]*(?:PRIVATE_KEY|API_KEY|ACCESS_TOKEN|BOT_TOKEN|SECRET|PASSWORD)[A-Z0-9_]*)
    ["']?\s*[:=]\s*
    (?P<value>[^\s,;#]+)
    """
)
TELEGRAM_TOKEN = re.compile(r"\bbot[0-9]{6,}:[A-Za-z0-9_-]{20,}\b")
SAFE_VALUE_MARKERS = (
    "${",
    "example",
    "placeholder",
    "redacted",
    "replace",
    "test-only",
    "stale-",
    "your_",
    "changeme",
    "<",
)


def _candidate_files(root: Path, *, include_untracked: bool) -> list[Path]:
    arguments = ["git", "-c", f"safe.directory={root.as_posix()}", "ls-files", "-z"]
    if include_untracked:
        arguments.extend(["--cached", "--others", "--exclude-standard"])
    result = subprocess.run(
        arguments,
        cwd=root,
        check=True,
        capture_output=True,
    )
    return [root / item.decode("utf-8") for item in result.stdout.split(b"\0") if item]


def _safe_assignment_value(raw_value: str) -> bool:
    raw = raw_value.strip()
    value = raw.strip("\"'[]{}()").lower()
    if not value or any(marker in value for marker in SAFE_VALUE_MARKERS):
        return True
    if raw[:1] not in {"\"", "'"} and ("(" in raw or "." in raw):
        return True
    if value.startswith(("os.getenv(", "getenv(", "env[", "secretref:")):
        return True
    return len(value) < 16


def scan(root: Path, *, include_untracked: bool = False) -> list[tuple[Path, int, str]]:
    findings: list[tuple[Path, int, str]] = []
    for path in _candidate_files(root, include_untracked=include_untracked):
        if path.suffix == ".lock" or not path.is_file():
            continue
        try:
            lines = path.read_text(encoding="utf-8").splitlines()
        except (UnicodeDecodeError, OSError):
            continue
        allow_next_fixture = False
        for line_number, line in enumerate(lines, start=1):
            if "secret-scan: allow-test-fixture" in line:
                allow_next_fixture = True
                continue
            if allow_next_fixture:
                allow_next_fixture = False
                continue
            if TELEGRAM_TOKEN.search(line):
                findings.append((path, line_number, "telegram_bot_token"))
            for match in SENSITIVE_ASSIGNMENT.finditer(line):
                if not _safe_assignment_value(match.group("value")):
                    findings.append((path, line_number, match.group("name")))
    return findings


def main() -> int:
    parser = argparse.ArgumentParser(description="Scan release source files for hard-coded credentials")
    parser.add_argument("--include-untracked", action="store_true")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    findings = scan(root, include_untracked=args.include_untracked)
    for path, line_number, secret_kind in findings:
        print(f"{path.relative_to(root)}:{line_number}: possible hard-coded {secret_kind}")
    if findings:
        print(f"Secret scan failed with {len(findings)} potential finding(s).")
        return 1
    scope = "tracked and untracked release files" if args.include_untracked else "tracked files"
    print(f"Secret scan passed for {scope}.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
