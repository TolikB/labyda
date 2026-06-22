import gzip
import hashlib
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


def test_production_drain_requires_reason() -> None:
    args = build_parser().parse_args(["production", "drain", "--reason", "spot drill"])

    assert args.production_command == "drain"
    assert args.reason == "spot drill"


def test_latest_valid_backup_skips_corrupt_newest_file() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        directory = Path(tmp)
        valid = directory / "arbitrage-20260620T000000Z.sql.gz"
        with gzip.open(valid, "wb") as handle:
            handle.write(b"postgres dump")
        digest = hashlib.sha256(valid.read_bytes()).hexdigest()
        valid.with_name(f"{valid.name}.sha256").write_text(f"{digest}  {valid.name}\n", encoding="utf-8")
        corrupt = directory / "arbitrage-20260621T000000Z.sql.gz"
        corrupt.write_bytes(b"not gzip")

        assert _latest_valid_backup(directory) == valid
