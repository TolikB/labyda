import gzip
import hashlib
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from arbitrage_engine import cli
from arbitrage_engine.cli import (
    _approval_candidates_from_report,
    _automatic_redemption_status,
    _has_active_stale_mappings,
    _latest_valid_backup,
    _linked_positions_for_intent,
    _mapping_candidate_within_auto_approval_scope,
    _mapping_review_report,
    _market_data_probe_detail,
    _market_data_probe_passed,
    _market_token_for_venue,
    _register_second_leg_market_clients,
    _representative_markets_by_venue,
    _safe_retire_reason,
    build_parser,
)
from arbitrage_engine.connectors.base import BinaryMarketClient
from arbitrage_engine.database import _market_identities, _venue_tokens
from arbitrage_engine.models import (
    BinarySide,
    ExecutionReport,
    MappingStatus,
    MarketDataStatus,
    MarketMapping,
    MarketSpec,
    OrderBook,
)


def test_production_verify_parser_accepts_backup_directory() -> None:
    args = build_parser().parse_args(["production", "verify", "--backup-dir", "/mnt/offsite"])

    assert args.command == "production"
    assert args.production_command == "verify"
    assert args.backup_dir == "/mnt/offsite"


def test_production_verify_parser_defaults_to_compose_vm_paths() -> None:
    args = build_parser().parse_args(["production", "verify"])

    assert args.backup_dir == "/mnt/arbitrage-backups"
    assert args.restore_marker == "/mnt/arbitrage-backups/restore-drill.json"
    assert args.release_sha_file == ".runtime/release-sha"
    assert args.drain_marker == "/mnt/arbitrage-backups/drain-ready.json"


def test_production_audit_parser_accepts_backup_directory() -> None:
    args = build_parser().parse_args(["production", "audit", "--backup-dir", "/mnt/offsite"])

    assert args.command == "production"
    assert args.production_command == "audit"
    assert args.backup_dir == "/mnt/offsite"


def test_production_audit_parser_accepts_all_market_and_live_evidence_flags() -> None:
    args = build_parser().parse_args(
        [
            "production",
            "audit",
            "--all-markets",
            "--require-live-order-evidence",
            "--live-window-report",
            "polymarket_sx=artifacts/report.json",
        ]
    )

    assert args.production_command == "audit"
    assert args.all_markets is True
    assert args.require_live_order_evidence is True
    assert args.live_window_report == ["polymarket_sx=artifacts/report.json"]


def test_production_audit_parser_accepts_deferred_backup_gate_flag() -> None:
    args = build_parser().parse_args(["production", "audit", "--defer-backup-gates"])

    assert args.production_command == "audit"
    assert args.defer_backup_gates is True


def test_discovery_overlap_parser_is_available() -> None:
    args = build_parser().parse_args(["discovery", "overlap"])

    assert args.command == "discovery"
    assert args.discovery_command == "overlap"


def test_main_loads_operator_env_for_selected_config() -> None:
    parser = MagicMock()
    parser.parse_args.return_value = SimpleNamespace(
        command="orders",
        order_command="review-unresolved",
        config="config.production.json",
    )

    with (
        patch("arbitrage_engine.cli.build_parser", return_value=parser),
        patch("arbitrage_engine.cli.load_operator_env") as load_operator_env,
        patch("arbitrage_engine.cli.asyncio.run") as asyncio_run,
    ):
        asyncio_run.side_effect = lambda coro: coro.close()
        cli.main()

    load_operator_env.assert_called_once_with("config.production.json")
    asyncio_run.assert_called_once()


def test_cancel_all_requires_explicit_confirmation() -> None:
    args = build_parser().parse_args(["orders", "cancel-all", "--confirm", "YES"])

    assert args.order_command == "cancel-all"
    assert args.confirm == "YES"


def test_orders_review_unresolved_parser_accepts_age_threshold() -> None:
    args = build_parser().parse_args(["orders", "review-unresolved", "--older-than-minutes", "90"])

    assert args.order_command == "review-unresolved"
    assert args.older_than_minutes == 90.0


def test_orders_retire_safe_unresolved_parser_accepts_confirmation() -> None:
    args = build_parser().parse_args(
        ["orders", "retire-safe-unresolved", "--older-than-minutes", "120", "--confirm", "YES"]
    )

    assert args.order_command == "retire-safe-unresolved"
    assert args.older_than_minutes == 120.0
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


def test_mappings_list_supports_sx_route_filter() -> None:
    args = build_parser().parse_args(["mappings", "list", "--route", "polymarket_sx"])

    assert args.mapping_command == "list"
    assert args.route == "polymarket_sx"


def test_mappings_list_supports_predict_sx_route_filter() -> None:
    args = build_parser().parse_args(["mappings", "list", "--route", "predict_sx"])

    assert args.mapping_command == "list"
    assert args.route == "predict_sx"


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
            match_strategy="exact_id",
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
            match_strategy="exact_id",
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
            "Myriad:myriad-1": {"yes_token_id": "553:YES", "no_token_id": "553:NO"},
            "Predict.fun:predict-1": {"yes_token_id": "101", "no_token_id": "202"},
            "Predict.fun:predict-2": {"yes_token_id": "303", "no_token_id": "404"},
            "Myriad:myriad-2": {"yes_token_id": "777:YES", "no_token_id": "777:NO"},
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


def test_mapping_review_report_does_not_auto_approve_title_or_legacy_matches() -> None:
    mappings = [
        MarketMapping(
            mapping_id="title",
            canonical_market_id="canon-title",
            left_venue="Polymarket",
            left_market_id="poly-title",
            right_venue="SX Bet",
            right_market_id="sx-title",
            status=MappingStatus.CANDIDATE,
            rules_fingerprint="fp-title",
            match_strategy="exact_title",
        ),
        MarketMapping(
            mapping_id="legacy",
            canonical_market_id="canon-legacy",
            left_venue="Polymarket",
            left_market_id="poly-legacy",
            right_venue="Predict.fun",
            right_market_id="predict-legacy",
            status=MappingStatus.CANDIDATE,
            rules_fingerprint="fp-legacy",
        ),
    ]

    report = _mapping_review_report(mappings, ("polymarket_sx", "polymarket_predict"))

    assert _approval_candidates_from_report(report) == []


def test_mapping_auto_approval_scope_enforces_category_and_launch_horizon() -> None:
    config = MagicMock()
    config.categories_to_scan = ["crypto", "sports"]
    config.market_horizon_filter_enabled = True
    config.max_sports_market_horizon_hours = 48
    config.max_crypto_market_horizon_hours = 24
    now = datetime(2026, 7, 15, 8, tzinfo=UTC)

    assert _mapping_candidate_within_auto_approval_scope(
        {"category": "Crypto", "cutoff_at": "2026-07-16T07:00:00Z"}, config, now=now
    )
    assert not _mapping_candidate_within_auto_approval_scope(
        {"category": "Crypto", "cutoff_at": "2026-07-16T09:00:00Z"}, config, now=now
    )
    assert _mapping_candidate_within_auto_approval_scope(
        {"category": "Sports", "cutoff_at": "2026-07-17T08:00:00Z"}, config, now=now
    )
    assert not _mapping_candidate_within_auto_approval_scope(
        {"category": "Politics", "cutoff_at": "2026-07-15T09:00:00Z"}, config, now=now
    )


def test_register_second_leg_market_clients_registers_predict_fun_and_sx_markets() -> None:
    predict_client = MagicMock()
    sx_client = MagicMock()
    markets = [
        MarketSpec(
            symbol="Predict market",
            target_label="YES",
            polymarket_token_id="poly-predict",
            polymarket_side=BinarySide.YES,
            predict_fun_token_id="predict-token",
            predict_fun_side=BinarySide.NO,
            predict_fun_market_id="predict-market",
            predict_fun_fee_rate_bps=17,
            venue_b_label="Predict.fun",
        ),
        MarketSpec(
            symbol="SX market",
            target_label="YES",
            polymarket_token_id="poly-sx",
            polymarket_side=BinarySide.YES,
            predict_fun_token_id="sx-token",
            predict_fun_side=BinarySide.YES,
            predict_fun_market_id="0xsxmarket",
            venue_b_label="SX Bet",
        ),
    ]

    _register_second_leg_market_clients(markets, {"Predict.fun": predict_client, "SX Bet": sx_client})

    predict_client.register_market.assert_called_once_with("predict-token", "predict-market", BinarySide.NO, 17)
    sx_client.register_market.assert_called_once_with("sx-token", "0xsxmarket", BinarySide.YES)


def test_register_second_leg_market_clients_handles_transformed_predict_and_sx_routes() -> None:
    predict_client = MagicMock()
    sx_client = MagicMock()
    markets = [
        MarketSpec(
            symbol="Predict-Myriad",
            target_label="YES",
            polymarket_token_id="predict-token",
            polymarket_side=BinarySide.NO,
            predict_fun_token_id="1335:YES",
            predict_fun_side=BinarySide.YES,
            predict_fun_market_id="predict-market",
            myriad_market_id="1335",
            myriad_side=BinarySide.NO,
            venue_a_label="Predict.fun",
            venue_b_label="Myriad",
        ),
        MarketSpec(
            symbol="SX-Myriad",
            target_label="YES",
            polymarket_token_id="sx-token",
            polymarket_side=BinarySide.YES,
            predict_fun_token_id="1440:NO",
            predict_fun_side=BinarySide.NO,
            predict_fun_market_id="0xsxmarket",
            myriad_market_id="1440",
            myriad_side=BinarySide.YES,
            venue_a_label="SX Bet",
            venue_b_label="Myriad",
        ),
    ]

    _register_second_leg_market_clients(markets, {"Predict.fun": predict_client, "SX Bet": sx_client})

    predict_client.register_market.assert_called_once_with("predict-token", "predict-market", BinarySide.NO, None)
    sx_client.register_market.assert_called_once_with("sx-token", "0xsxmarket", BinarySide.YES)


def test_register_second_leg_market_clients_handles_predict_sx_route_shape() -> None:
    predict_client = MagicMock()
    sx_client = MagicMock()
    market = MarketSpec(
        symbol="Predict-SX",
        target_label="YES",
        polymarket_token_id="predict-token",
        polymarket_side=BinarySide.NO,
        polymarket_market_id="predict-market",
        predict_fun_token_id="sx-token",
        predict_fun_side=BinarySide.YES,
        predict_fun_market_id="0xsxmarket",
        predict_fun_fee_rate_bps=19,
        venue_a_label="Predict.fun",
        venue_b_label="SX Bet",
    )

    _register_second_leg_market_clients([market], {"Predict.fun": predict_client, "SX Bet": sx_client})

    predict_client.register_market.assert_called_once_with("predict-token", "predict-market", BinarySide.NO, 19)
    sx_client.register_market.assert_called_once_with("sx-token", "0xsxmarket", BinarySide.YES)


def test_representative_markets_by_venue_covers_each_enabled_venue() -> None:
    predict_market = MarketSpec(
        symbol="Predict market",
        target_label="YES",
        polymarket_token_id="poly-predict",
        polymarket_side=BinarySide.YES,
        predict_fun_token_id="predict-token",
        predict_fun_side=BinarySide.NO,
        predict_fun_market_id="predict-market",
        venue_b_label="Predict.fun",
    )
    sx_market = MarketSpec(
        symbol="SX market",
        target_label="YES",
        polymarket_token_id="poly-sx",
        polymarket_side=BinarySide.YES,
        predict_fun_token_id="sx-token",
        predict_fun_side=BinarySide.YES,
        predict_fun_market_id="0xsxmarket",
        myriad_market_id="myriad-sx",
        venue_b_label="SX Bet",
    )

    representatives = _representative_markets_by_venue([predict_market, sx_market])

    assert representatives["Polymarket"] is predict_market
    assert representatives["Predict.fun"] is predict_market
    assert representatives["SX Bet"] is sx_market
    assert representatives["Myriad"] is sx_market
    assert _market_token_for_venue(sx_market, "SX Bet") == "sx-token"
    assert _market_token_for_venue(sx_market, "Myriad") == "myriad-sx:NO"


def test_representative_markets_by_venue_handles_transformed_routes() -> None:
    predict_market = MarketSpec(
        symbol="Predict-Myriad",
        target_label="YES",
        polymarket_token_id="predict-token",
        polymarket_side=BinarySide.NO,
        predict_fun_token_id="1335:YES",
        predict_fun_side=BinarySide.YES,
        predict_fun_market_id="predict-market",
        myriad_market_id="1335",
        myriad_side=BinarySide.NO,
        venue_a_label="Predict.fun",
        venue_b_label="Myriad",
    )
    sx_market = MarketSpec(
        symbol="SX-Myriad",
        target_label="YES",
        polymarket_token_id="sx-token",
        polymarket_side=BinarySide.YES,
        predict_fun_token_id="1440:NO",
        predict_fun_side=BinarySide.NO,
        predict_fun_market_id="0xsxmarket",
        myriad_market_id="1440",
        myriad_side=BinarySide.YES,
        venue_a_label="SX Bet",
        venue_b_label="Myriad",
    )

    representatives = _representative_markets_by_venue([predict_market, sx_market])

    assert representatives["Predict.fun"] is predict_market
    assert representatives["SX Bet"] is sx_market


def test_market_data_probe_accepts_fresh_single_sided_book() -> None:
    book = SimpleNamespace(bids=[SimpleNamespace(price=0.4, size=5.0)], asks=[], status=MarketDataStatus.VALID)

    assert _market_data_probe_passed(book) is True
    assert _market_data_probe_detail(book) == {
        "status": "VALID",
        "has_bids": True,
        "has_asks": False,
        "bid_levels": 1,
        "ask_levels": 0,
    }


def test_representative_markets_by_venue_prefers_myriad_market_with_settlement_metadata() -> None:
    bare_myriad = MarketSpec(
        symbol="Bare Myriad",
        target_label="YES",
        polymarket_token_id="poly-bare",
        polymarket_side=BinarySide.YES,
        predict_fun_token_id="",
        predict_fun_side=BinarySide.NO,
        myriad_market_id="1335",
        myriad_side=BinarySide.NO,
        venue_b_label="Myriad",
        mapping_status=MappingStatus.VERIFIED,
        verified_routes=frozenset({"polymarket_myriad"}),
    )
    rich_myriad = MarketSpec(
        symbol="Rich Myriad",
        target_label="YES",
        polymarket_token_id="poly-rich",
        polymarket_side=BinarySide.YES,
        predict_fun_token_id="",
        predict_fun_side=BinarySide.NO,
        myriad_market_id="1440",
        myriad_condition_id="condition-1440",
        myriad_collateral_token="USD1",
        myriad_side=BinarySide.NO,
        venue_b_label="Myriad",
        mapping_status=MappingStatus.VERIFIED,
        verified_routes=frozenset({"polymarket_myriad"}),
    )

    representatives = _representative_markets_by_venue([bare_myriad, rich_myriad])

    assert representatives["Myriad"] is rich_myriad


def test_has_active_stale_mappings_ignores_historical_rows_outside_current_tradable_routes() -> None:
    active_market = MarketSpec(
        symbol="Current Predict",
        target_label="YES",
        polymarket_token_id="poly-token",
        polymarket_side=BinarySide.YES,
        polymarket_market_id="poly-current",
        predict_fun_token_id="predict-token",
        predict_fun_side=BinarySide.NO,
        predict_fun_market_id="predict-current",
        venue_b_label="Predict.fun",
        mapping_status=MappingStatus.VERIFIED,
        verified_routes=frozenset({"polymarket_predict"}),
    )
    historical_stale = MarketMapping(
        mapping_id="stale-old",
        canonical_market_id="canon-old",
        left_venue="Polymarket",
        left_market_id="poly-old",
        right_venue="Predict.fun",
        right_market_id="predict-old",
        status=MappingStatus.STALE,
        rules_fingerprint="rfp-old",
    )
    current_stale = MarketMapping(
        mapping_id="stale-current",
        canonical_market_id="canon-current",
        left_venue="Polymarket",
        left_market_id="poly-current",
        right_venue="Predict.fun",
        right_market_id="predict-current",
        status=MappingStatus.STALE,
        rules_fingerprint="rfp-current",
    )

    assert _has_active_stale_mappings([historical_stale], [active_market]) is False
    assert _has_active_stale_mappings([historical_stale, current_stale], [active_market]) is True


def test_deduplicate_markets_preserves_myriad_settlement_metadata_from_merged_route_shapes() -> None:
    from arbitrage_engine.main import _deduplicate_markets

    predict_shape = MarketSpec(
        symbol="Will Switzerland win the 2026 FIFA World Cup?",
        target_label="Will Switzerland win the 2026 FIFA World Cup?",
        polymarket_token_id="poly-yes",
        polymarket_market_id="558974",
        condition_id="0xcondition",
        polymarket_side=BinarySide.YES,
        predict_fun_token_id="predict-no",
        predict_fun_side=BinarySide.NO,
        predict_fun_market_id="1536",
        myriad_market_id="410",
        myriad_side=BinarySide.NO,
        venue_b_label="Predict.fun",
        mapping_status=MappingStatus.VERIFIED,
        verified_routes=frozenset({"polymarket_predict", "polymarket_myriad"}),
    )
    myriad_shape = MarketSpec(
        symbol=predict_shape.symbol,
        target_label=predict_shape.target_label,
        polymarket_token_id="poly-yes",
        polymarket_market_id="558974",
        condition_id="0xcondition",
        polymarket_side=BinarySide.YES,
        predict_fun_token_id="",
        predict_fun_side=BinarySide.NO,
        myriad_market_id="410",
        myriad_condition_id="myriad-condition-410",
        myriad_collateral_token="0x8d0D000Ee44948FC98c9B98A4FA4921476f08B0d",
        myriad_side=BinarySide.NO,
        venue_b_label="Myriad",
        mapping_status=MappingStatus.VERIFIED,
        verified_routes=frozenset({"polymarket_myriad"}),
    )

    merged = _deduplicate_markets([predict_shape, myriad_shape])

    assert len(merged) == 1
    assert merged[0].myriad_market_id == "410"
    assert merged[0].myriad_condition_id == "myriad-condition-410"
    assert merged[0].myriad_collateral_token == "0x8d0D000Ee44948FC98c9B98A4FA4921476f08B0d"


def test_venue_tokens_exposes_both_myriad_binary_tokens() -> None:
    market = MarketSpec(
        symbol="SX market",
        target_label="YES",
        polymarket_token_id="poly-sx",
        polymarket_side=BinarySide.YES,
        predict_fun_token_id="sx-token",
        predict_fun_side=BinarySide.NO,
        predict_fun_market_id="0xsxmarket",
        myriad_market_id="1335",
        myriad_side=BinarySide.NO,
        venue_b_label="SX Bet",
    )

    assert _venue_tokens(market, "Myriad") == ("1335:YES", "1335:NO")


def test_sx_myriad_shape_preserves_sx_identity_and_tokens() -> None:
    market = MarketSpec(
        symbol="SX market",
        target_label="YES",
        polymarket_token_id="sx-token",
        polymarket_side=BinarySide.YES,
        predict_fun_token_id="1335:YES",
        predict_fun_side=BinarySide.YES,
        predict_fun_market_id="0xsxmarket",
        myriad_market_id="1335",
        myriad_side=BinarySide.NO,
        venue_a_label="SX Bet",
        venue_b_label="Myriad",
    )

    assert _market_identities(market) == {"SX Bet": "0xsxmarket", "Myriad": "1335"}
    assert _venue_tokens(market, "SX Bet") == ("sx-token", "")
    assert _venue_tokens(market, "Myriad") == ("1335:YES", "1335:NO")


def test_predict_myriad_shape_preserves_predict_identity_and_tokens() -> None:
    market = MarketSpec(
        symbol="Predict-Myriad",
        target_label="YES",
        polymarket_token_id="predict-token",
        polymarket_side=BinarySide.NO,
        predict_fun_token_id="1335:YES",
        predict_fun_side=BinarySide.YES,
        predict_fun_market_id="predict-market",
        myriad_market_id="1335",
        myriad_side=BinarySide.NO,
        venue_a_label="Predict.fun",
        venue_b_label="Myriad",
    )

    assert _market_identities(market) == {"Predict.fun": "predict-market", "Myriad": "1335"}
    assert _venue_tokens(market, "Predict.fun") == ("", "predict-token")
    assert _venue_tokens(market, "Myriad") == ("1335:YES", "1335:NO")


def test_predict_sx_shape_preserves_predict_and_sx_identity_and_tokens() -> None:
    market = MarketSpec(
        symbol="Predict-SX",
        target_label="YES",
        polymarket_token_id="predict-token",
        polymarket_side=BinarySide.NO,
        polymarket_market_id="predict-market",
        predict_fun_token_id="sx-token",
        predict_fun_side=BinarySide.YES,
        predict_fun_market_id="0xsxmarket",
        venue_a_label="Predict.fun",
        venue_b_label="SX Bet",
    )

    assert _market_identities(market) == {"Predict.fun": "predict-market", "SX Bet": "0xsxmarket"}
    assert _venue_tokens(market, "Predict.fun") == ("", "predict-token")
    assert _venue_tokens(market, "SX Bet") == ("sx-token", "")


def test_market_token_for_venue_uses_route_aware_myriad_execution_token() -> None:
    predict_market = MarketSpec(
        symbol="Predict market",
        target_label="YES",
        polymarket_token_id="poly-predict",
        polymarket_side=BinarySide.YES,
        predict_fun_token_id="predict-token",
        predict_fun_side=BinarySide.NO,
        predict_fun_market_id="predict-market",
        myriad_market_id="predict-myriad",
        myriad_side=BinarySide.NO,
        venue_b_label="Predict.fun",
    )
    sx_market = MarketSpec(
        symbol="SX market",
        target_label="YES",
        polymarket_token_id="poly-sx",
        polymarket_side=BinarySide.YES,
        predict_fun_token_id="sx-token",
        predict_fun_side=BinarySide.YES,
        predict_fun_market_id="0xsxmarket",
        myriad_market_id="sx-myriad",
        myriad_side=BinarySide.YES,
        venue_b_label="SX Bet",
    )

    assert _market_token_for_venue(predict_market, "Myriad") == "predict-myriad:YES"
    assert _market_token_for_venue(sx_market, "Myriad") == "sx-myriad:NO"


def test_safe_retire_reason_accepts_old_missing_order_without_fill_or_position_evidence() -> None:
    row = MagicMock(status="ACKNOWLEDGED", venue_order_id="venue-1")

    reason = _safe_retire_reason(
        row=row,
        age_minutes=180.0,
        older_than_minutes=60.0,
        linked_position_count=0,
        db_fill_count=0,
        venue_fill_count=0,
        open_order_present=False,
        venue_status=None,
        venue_error="404 missing",
        synthetic=False,
    )

    assert reason == "venue_order_missing_without_fill_or_position_evidence"


def test_safe_retire_reason_rejects_recent_or_evidenced_order() -> None:
    row = MagicMock(status="ACKNOWLEDGED", venue_order_id="venue-1")

    reason = _safe_retire_reason(
        row=row,
        age_minutes=10.0,
        older_than_minutes=60.0,
        linked_position_count=0,
        db_fill_count=0,
        venue_fill_count=0,
        open_order_present=False,
        venue_status=None,
        venue_error="404 missing",
        synthetic=False,
    )
    assert reason is None

    reason = _safe_retire_reason(
        row=row,
        age_minutes=180.0,
        older_than_minutes=60.0,
        linked_position_count=1,
        db_fill_count=0,
        venue_fill_count=0,
        open_order_present=False,
        venue_status=None,
        venue_error="404 missing",
        synthetic=False,
    )
    assert reason is None


def test_linked_positions_for_intent_matches_both_route_legs() -> None:
    predict_market = MarketSpec(
        symbol="Predict market",
        target_label="YES",
        polymarket_token_id="predict-token",
        polymarket_side=BinarySide.NO,
        predict_fun_token_id="myriad-token",
        predict_fun_side=BinarySide.YES,
        predict_fun_market_id="predict-market",
        venue_a_label="Predict.fun",
        venue_b_label="Myriad",
        myriad_market_id="1335",
    )
    sx_market = MarketSpec(
        symbol="SX market",
        target_label="YES",
        polymarket_token_id="sx-token",
        polymarket_side=BinarySide.YES,
        predict_fun_token_id="1335:NO",
        predict_fun_side=BinarySide.NO,
        predict_fun_market_id="sx-market",
        venue_a_label="SX Bet",
        venue_b_label="Myriad",
        myriad_market_id="1335",
    )
    predict_position = MagicMock(
        market=predict_market,
        polymarket_order_id="predict-order",
        predict_fun_order_id="myriad-order-1",
    )
    sx_position = MagicMock(
        market=sx_market,
        polymarket_order_id="sx-order",
        predict_fun_order_id="myriad-order-2",
    )

    predict_linked = _linked_positions_for_intent(
        MagicMock(venue="Predict.fun", venue_order_id="predict-order"),
        [predict_position, sx_position],
    )
    myriad_linked = _linked_positions_for_intent(
        MagicMock(venue="Myriad", venue_order_id="myriad-order-2"),
        [predict_position, sx_position],
    )

    assert len(predict_linked) == 1
    assert len(myriad_linked) == 1


def test_automatic_redemption_status_uses_explicit_client_capability() -> None:
    class ManualClient(BinaryMarketClient):
        async def watch_order_book(self, token_id: str) -> OrderBook:
            raise NotImplementedError

        async def buy(
            self,
            token_id: str,
            side: BinarySide,
            contracts: float,
            max_price: float,
            *,
            condition_id: str | None = None,
            tick_size: str | None = None,
            neg_risk: bool | None = None,
        ) -> str:
            raise NotImplementedError

        async def sell(
            self,
            token_id: str,
            side: BinarySide,
            contracts: float,
            min_price: float,
            *,
            condition_id: str | None = None,
            tick_size: str | None = None,
            neg_risk: bool | None = None,
        ) -> str:
            raise NotImplementedError

        async def wait_filled(self, order_id: str, timeout_ms: int) -> ExecutionReport:
            raise NotImplementedError

        async def cancel_order(self, order_id: str) -> None:
            raise NotImplementedError

        async def get_cash_balance(self) -> float:
            raise NotImplementedError

    class AutoClient(ManualClient):
        def supports_automatic_redemption(self) -> bool:
            return True

    assert _automatic_redemption_status(AutoClient(), "SX Bet") == (True, "supported")
    assert _automatic_redemption_status(ManualClient(), "Predict.fun") == (True, "not required for this venue")
    assert _automatic_redemption_status(ManualClient(), "Myriad") == (False, "missing")
