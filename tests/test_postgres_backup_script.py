"""The backup sidecar's retention: both copies age out, hand-placed files stay."""

from __future__ import annotations

import os
import shutil
import stat
import subprocess
import time
from pathlib import Path

import pytest

_SCRIPT = Path(__file__).resolve().parents[1] / "ops" / "postgres_backup.sh"
_DAY = 86400

# The script runs in the Linux backup container; WSL's launcher on the dev
# machine cannot run it, so the contract is checked where bash is native.
pytestmark = pytest.mark.skipif(
    shutil.which("bash") is None or os.name == "nt", reason="bash is required for the backup script contract"
)


def _aged(path: Path, days: float) -> Path:
    path.write_text("x", encoding="utf-8")
    stamp = time.time() - days * _DAY
    os.utime(path, (stamp, stamp))
    return path


def _seed(directory: Path) -> dict[str, Path]:
    directory.mkdir()
    files = {
        "old": _aged(directory / "arbitrage-20260901T000000Z.sql.gz", 20),
        "old_sum": _aged(directory / "arbitrage-20260901T000000Z.sql.gz.sha256", 20),
        "five_days": _aged(directory / "arbitrage-20261008T000000Z.sql.gz", 5),
        "five_days_sum": _aged(directory / "arbitrage-20261008T000000Z.sql.gz.sha256", 5),
        "yesterday": _aged(directory / "arbitrage-20261012T000000Z.sql.gz", 1),
        "migration": _aged(directory / "arbitrage-migration-20260913T190311Z.sql.gz", 30),
        "drill": _aged(directory / "restore-drill.json", 30),
    }
    return files


def _run_backup(tmp_path: Path, **overrides: str) -> tuple[dict[str, Path], dict[str, Path]]:
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    pg_dump = fake_bin / "pg_dump"
    pg_dump.write_text("#!/bin/sh\necho '-- stub dump'\n", encoding="utf-8")
    pg_dump.chmod(pg_dump.stat().st_mode | stat.S_IXUSR)
    primary = _seed(tmp_path / "primary")
    offsite = _seed(tmp_path / "offsite")
    env = {
        **os.environ,
        "PATH": f"{fake_bin}{os.pathsep}{os.environ['PATH']}",
        "DATABASE_URL": "postgresql+asyncpg://arbitrage:secret@postgres:5432/arbitrage",
        "BACKUP_DIR": str(tmp_path / "primary"),
        "BACKUP_METRICS_DIR": str(tmp_path / "metrics"),
        "OFFSITE_BACKUP_DIR": str(tmp_path / "offsite"),
        "BACKUP_RETENTION_DAYS": "14",
        **overrides,
    }
    env.pop("RCLONE_REMOTE", None)
    result = subprocess.run(["bash", str(_SCRIPT)], env=env, capture_output=True, text=True, check=False)
    assert result.returncode == 0, result.stderr
    return primary, offsite


def test_the_offsite_copy_ages_out_on_its_own_clock(tmp_path: Path) -> None:
    primary, offsite = _run_backup(tmp_path, OFFSITE_BACKUP_RETENTION_DAYS="2")

    # The primary keeps two weeks.
    assert not primary["old"].exists() and not primary["old_sum"].exists()
    assert primary["five_days"].exists() and primary["yesterday"].exists()
    # The offsite copy keeps two days, checksums included.
    assert not offsite["old"].exists() and not offsite["five_days"].exists()
    assert not offsite["five_days_sum"].exists()
    assert offsite["yesterday"].exists()
    # Today's dump reached both.
    assert len(list((tmp_path / "primary").glob("arbitrage-2*.sql.gz"))) == 3
    assert len(list((tmp_path / "offsite").glob("arbitrage-2*.sql.gz"))) == 2


def test_hand_placed_files_are_never_pruned(tmp_path: Path) -> None:
    primary, offsite = _run_backup(tmp_path, OFFSITE_BACKUP_RETENTION_DAYS="2")

    for files in (primary, offsite):
        assert files["migration"].exists()
        assert files["drill"].exists()


def test_the_offsite_copy_follows_the_primary_when_unset(tmp_path: Path) -> None:
    _primary, offsite = _run_backup(tmp_path)

    assert not offsite["old"].exists()
    assert offsite["five_days"].exists()
