import gzip
import tempfile
from pathlib import Path

from arbitrage_engine.cli import _latest_valid_backup, build_parser


def test_production_verify_parser_accepts_backup_directory() -> None:
    args = build_parser().parse_args(["production", "verify", "--backup-dir", "/mnt/offsite"])

    assert args.command == "production"
    assert args.production_command == "verify"
    assert args.backup_dir == "/mnt/offsite"


def test_cancel_all_requires_explicit_confirmation() -> None:
    args = build_parser().parse_args(["orders", "cancel-all", "--confirm", "YES"])

    assert args.order_command == "cancel-all"
    assert args.confirm == "YES"


def test_latest_valid_backup_skips_corrupt_newest_file() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        directory = Path(tmp)
        valid = directory / "arbitrage-20260620T000000Z.sql.gz"
        with gzip.open(valid, "wb") as handle:
            handle.write(b"postgres dump")
        corrupt = directory / "arbitrage-20260621T000000Z.sql.gz"
        corrupt.write_bytes(b"not gzip")

        assert _latest_valid_backup(directory) == valid
