import gzip
import hashlib
import tempfile
from datetime import UTC, datetime
from pathlib import Path

from arbitrage_engine.cli import (
    _approval_candidates_from_report,
    _latest_valid_backup,
    _mapping_review_report,
    build_parser,
)
from arbitrage_engine.models import MappingStatus, MarketMapping


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


def test_mappings_review_parser_is_available() -> None:
    args = build_parser().parse_args(
        ["mappings", "review", "--status", "CANDIDATE", "--operator", "tolik"]
    )

    assert args.command == "mappings"
    assert args.mapping_command == "review"
    assert args.status == "CANDIDATE"
    assert args.operator == "tolik"


def test_mappings_approve_safe_candidates_parser_is_available() -> None:
    args = build_parser().parse_args(
        ["mappings", "approve-safe-candidates", "--operator", "tolik", "--confirm", "YES"]
    )

    assert args.command == "mappings"
    assert args.mapping_command == "approve-safe-candidates"
    assert args.operator == "tolik"
    assert args.confirm == "YES"


def test_mappings_list_supports_route_filter() -> None:
    args = build_parser().parse_args(["mappings", "list", "--route", "polymarket_myriad"])

    assert args.mapping_command == "list"
    assert args.route == "polymarket_myriad"


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


def test_mapping_review_report_summarizes_route_coverage() -> None:
    mappings = [
        MarketMapping(
            mapping_id="a",
            canonical_market_id="canon-1",
            left_venue="Polymarket",
            left_market_id="poly-1",
            right_venue="Myriad",
            right_market_id="myriad-1",
            status=MappingStatus.CANDIDATE,
            rules_fingerprint="fp-1",
        ),
        MarketMapping(
            mapping_id="b",
            canonical_market_id="canon-1",
            left_venue="Polymarket",
            left_market_id="poly-1",
            right_venue="Predict.fun",
            right_market_id="predict-1",
            status=MappingStatus.VERIFIED,
            rules_fingerprint="fp-1",
            verified_at=datetime(2026, 6, 28, tzinfo=UTC),
            verified_by="operator",
        ),
        MarketMapping(
            mapping_id="c",
            canonical_market_id="canon-2",
            left_venue="Predict.fun",
            left_market_id="predict-2",
            right_venue="Myriad",
            right_market_id="myriad-2",
            status=MappingStatus.CANDIDATE,
            rules_fingerprint="fp-2",
        ),
    ]

    report = _mapping_review_report(
        mappings,
        ("polymarket_myriad", "polymarket_predict", "predict_myriad"),
        config_path="config.runtime.json",
        operator="tolik",
        canonical_markets={
            "canon-1": {
                "canonical_market_id": "canon-1",
                "title": "Will BTC exceed 100000?",
                "category": "finance",
            },
            "canon-2": {
                "canonical_market_id": "canon-2",
                "title": "Will Arsenal win?",
                "category": "sports",
            }
        },
        venue_instruments={
            "Polymarket:poly-1": {"yes_token_id": "poly-yes", "no_token_id": "poly-no"},
            "Myriad:myriad-1": {"yes_token_id": "", "no_token_id": "553:NO"},
            "Predict.fun:predict-1": {"yes_token_id": "101", "no_token_id": "202"},
            "Predict.fun:predict-2": {"yes_token_id": "303", "no_token_id": "404"},
            "Myriad:myriad-2": {"yes_token_id": "", "no_token_id": "777:NO"},
        },
    )
    summary = report["summary"]
    assert isinstance(summary, dict)
    coverage = summary["enabled_route_coverage"]
    assert isinstance(coverage, dict)
    assert coverage["polymarket_myriad"]["has_verified"] is False
    assert coverage["polymarket_predict"]["has_verified"] is True
    assert coverage["predict_myriad"]["has_verified"] is False
    approval_candidates = summary["approval_candidates"]
    assert isinstance(approval_candidates, list)
    assert {item["mapping_id"] for item in approval_candidates} == {"a", "c"}
    assert all("--config config.runtime.json" in str(item["approve_command"]) for item in approval_candidates)
    assert all("--operator tolik" in str(item["approve_command"]) for item in approval_candidates)
    extracted = _approval_candidates_from_report(report)
    assert {item["mapping_id"] for item in extracted} == {"a", "c"}
    markets = report["markets"]
    assert isinstance(markets, list)
    assert markets[0]["canonical_market_id"] == "canon-1"
    assert markets[0]["canonical"]["title"] == "Will BTC exceed 100000?"
    assert markets[0]["missing_enabled_routes"] == ["polymarket_myriad", "predict_myriad"]
    assert markets[0]["mappings"][0]["left_instrument"]["yes_token_id"] == "poly-yes"
