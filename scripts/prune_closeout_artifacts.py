"""Bound what a continuously running funded canary writes to disk.

A bounded run writes its artifacts once and stops. A continuous run keeps the
same `run_id` for as long as it lives, so its directory grows for weeks: every
window leaves a per-route observer directory, and the bulk of that is
`samples.jsonl` plus the `live/`, `ready/` and `metrics/` probe captures. The
completed runs already on the box are ~50 MB each, and six windows a day on a
disk that is 92% full is not a background concern.

What is kept, always and regardless of age:

* every `report.json` -- it is the window's evidence, the final audit reads it,
  and it is kilobytes;
* every per-window artifact the wrapper itself writes (`windows/window-NNN/`,
  repeat decisions, daily reports) -- also small, also evidence.

What is dropped, for windows older than the keep count: the heavy probe capture
that only matters while diagnosing a window in flight.

Old *run* directories are left alone by default. `closeout-artifacts/` on the VM
holds directories an operator created by hand -- `release-*`, `readiness-*`,
`safe-launch-*` -- and deleting somebody's evidence to save space is not this
script's call. Cross-run pruning is opt-in, and even then only touches
directories whose name is a wrapper-generated `run_id` timestamp.
"""

from __future__ import annotations

import argparse
import json
import re
import shutil
import time
from pathlib import Path
from typing import Any

# The wrapper's own `date -u +%Y%m%dT%H%M%SZ`. Anything else in the artifact
# root was named by a person and is never a candidate for deletion.
RUN_ID_PATTERN = re.compile(r"^\d{8}T\d{6}Z$")

HEAVY_FILES = ("samples.jsonl",)
HEAVY_DIRECTORIES = ("live", "ready", "metrics")


def _directory_size(path: Path) -> int:
    return sum(item.stat().st_size for item in path.rglob("*") if item.is_file())


def observer_window_directories(run_dir: Path) -> list[Path]:
    """Every `<target>/canary-artifacts/<route>/<timestamp>` directory, oldest first.

    The observer names these with the window's start timestamp, so sorting by
    name is chronological within a route.
    """
    return sorted(
        (path for path in run_dir.glob("*/canary-artifacts/*/*") if path.is_dir()),
        key=lambda path: (path.parent.name, path.name),
    )


def prune_run_directory(run_dir: Path, *, keep_windows: int, dry_run: bool = False) -> dict[str, Any]:
    """Drop heavy probe captures from all but the newest `keep_windows` per route."""
    by_route: dict[str, list[Path]] = {}
    for window in observer_window_directories(run_dir):
        by_route.setdefault(str(window.parent), []).append(window)

    removed: list[str] = []
    reclaimed_bytes = 0
    for windows in by_route.values():
        for window in windows[: max(0, len(windows) - keep_windows)]:
            for name in HEAVY_FILES:
                target = window / name
                if target.is_file():
                    reclaimed_bytes += target.stat().st_size
                    removed.append(str(target))
                    if not dry_run:
                        target.unlink()
            for name in HEAVY_DIRECTORIES:
                target = window / name
                if target.is_dir():
                    reclaimed_bytes += _directory_size(target)
                    removed.append(str(target))
                    if not dry_run:
                        shutil.rmtree(target)

    return {"removed": removed, "reclaimed_bytes": reclaimed_bytes}


def prune_old_runs(
    artifact_root: Path,
    *,
    current_run_dir: Path,
    retention_days: int,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Remove wrapper-generated run directories older than the retention window."""
    if retention_days <= 0:
        return {"removed": [], "reclaimed_bytes": 0, "enabled": False}

    cutoff = time.time() - retention_days * 86400
    current = current_run_dir.resolve()
    removed: list[str] = []
    reclaimed_bytes = 0
    for candidate in sorted(artifact_root.iterdir()):
        if not candidate.is_dir() or not RUN_ID_PATTERN.match(candidate.name):
            continue
        if candidate.resolve() == current:
            continue
        if candidate.stat().st_mtime >= cutoff:
            continue
        reclaimed_bytes += _directory_size(candidate)
        removed.append(str(candidate))
        if not dry_run:
            shutil.rmtree(candidate)

    return {"removed": removed, "reclaimed_bytes": reclaimed_bytes, "enabled": True}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Bound continuous funded-canary artifact growth")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--artifact-root", required=True)
    parser.add_argument(
        "--keep-windows",
        type=int,
        default=12,
        help="Windows per route whose heavy probe capture is retained (12 is two days).",
    )
    parser.add_argument(
        "--retention-days",
        type=int,
        default=0,
        help="Delete wrapper-generated run directories older than this. 0 disables it.",
    )
    parser.add_argument("--dry-run", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    run_dir = Path(args.run_dir)
    artifact_root = Path(args.artifact_root)

    report = {
        "run_dir": str(run_dir),
        "keep_windows": args.keep_windows,
        "dry_run": args.dry_run,
        "current_run": prune_run_directory(run_dir, keep_windows=args.keep_windows, dry_run=args.dry_run),
        "old_runs": prune_old_runs(
            artifact_root,
            current_run_dir=run_dir,
            retention_days=args.retention_days,
            dry_run=args.dry_run,
        ),
    }
    report["reclaimed_bytes"] = report["current_run"]["reclaimed_bytes"] + report["old_runs"]["reclaimed_bytes"]
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
