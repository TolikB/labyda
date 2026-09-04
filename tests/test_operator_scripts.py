from __future__ import annotations

import hashlib
import importlib.util
import io
import json
import os
import subprocess
import sys
from copy import deepcopy
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any

import pytest

from arbitrage_engine.redaction import redact_signing_material


def _load_script_module(name: str, filename: str) -> ModuleType:
    root = Path(__file__).resolve().parents[1]
    path = root / "scripts" / filename
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


predict_preview = _load_script_module(
    "predict_fun_balance_and_order_preview_module",
    "predict_fun_balance_and_order_preview.py",
)
live_readiness = _load_script_module(
    "live_balance_and_order_readiness_module",
    "live_balance_and_order_readiness.py",
)
sx_preview = _load_script_module(
    "sx_bet_balance_and_order_preview_module",
    "sx_bet_balance_and_order_preview.py",
)
sx_probe = _load_script_module(
    "sx_bet_probe_module",
    "sx_bet_probe.py",
)
sx_match_probe = _load_script_module(
    "sx_polymarket_match_probe_module",
    "sx_polymarket_match_probe.py",
)
live_canary = _load_script_module(
    "live_canary_window_module",
    "live_canary_window.py",
)
shadow_openability = _load_script_module(
    "shadow_openability_window_module",
    "shadow_openability_window.py",
)
runtime_health_gate = _load_script_module(
    "runtime_health_gate_module",
    "runtime_health_gate.py",
)
polymarket_wallet_probe = _load_script_module(
    "polymarket_deposit_wallet_probe_module",
    "polymarket_deposit_wallet_probe.py",
)
predict_approvals = _load_script_module(
    "predict_fun_approvals_module",
    "predict_fun_approvals.py",
)
myriad_preview = _load_script_module(
    "myriad_balance_and_order_preview_module",
    "myriad_balance_and_order_preview.py",
)


def test_live_readiness_json_transport_serializes_decimal_without_losing_precision() -> None:
    payload = json.dumps({"fee": Decimal("0.123456789012345678")}, default=live_readiness._json_default)  # noqa: SLF001

    assert json.loads(payload) == {"fee": "0.123456789012345678"}


def test_predict_preview_redacts_replayable_signed_order() -> None:
    signature = "0x" + "a" * 130
    preview = predict_preview._redacted_signed_preview(  # noqa: SLF001
        SimpleNamespace(
            signed_order={"tokenId": "123", "signature": signature, "salt": "secret-order-value"},
            amount_wei=10,
            price_per_share_wei=20,
            slippage_bps=30,
            is_min_amount_out=True,
        )
    )
    serialized = json.dumps(preview)

    assert preview["signed_preview_created"] is True
    assert preview["signature_present"] is True
    assert len(preview["signed_order_sha256"]) == 64
    assert "signed_order" not in preview
    assert signature not in serialized
    assert "secret-order-value" not in serialized


def test_predict_preview_report_serializes_nested_decimal_without_losing_precision() -> None:
    payload = predict_preview._report_json(  # noqa: SLF001
        {
            "order_preview": {
                "maximum_fee_usd": Decimal("0.123456789012345678"),
                "constraints": {"minimum_notional": Decimal("1.000000000000000001")},
            }
        }
    )

    assert json.loads(payload) == {
        "order_preview": {
            "maximum_fee_usd": "0.123456789012345678",
            "constraints": {"minimum_notional": "1.000000000000000001"},
        }
    }


def test_live_readiness_redacts_embedded_and_detached_signatures() -> None:
    signature = "0x" + "b" * 130
    preview = live_readiness._redacted_signed_payload(  # noqa: SLF001
        {"order": "replayable", "signature": signature},
        detached_signature="detached-secret",
    )
    serialized = json.dumps(preview)

    assert preview["signed_preview_created"] is True
    assert preview["signature_present"] is True
    assert len(preview["signed_payload_sha256"]) == 64
    assert "replayable" not in serialized
    assert signature not in serialized
    assert "detached-secret" not in serialized


def test_myriad_preview_redacts_replayable_signed_order() -> None:
    signature = "0x" + "c" * 130
    preview = myriad_preview._redacted_signed_order(  # noqa: SLF001
        {"marketId": 123, "salt": "secret-order-value"},
        signature,
    )
    serialized = json.dumps(preview)

    assert preview["signature_present"] is True
    assert len(preview["signed_order_sha256"]) == 64
    assert "secret-order-value" not in serialized
    assert signature not in serialized


def test_operator_redaction_removes_nested_sx_signature_material() -> None:
    signature = "0x" + "d" * 130
    preview = redact_signing_material(
        {
            "request_payload": {
                "market": "0xmarket",
                "takerSig": signature,
                "nested": {"orderSignature": "replayable-order-signature"},
            },
            "signature_prefix": signature[:18],
        }
    )
    serialized = json.dumps(preview)

    assert preview["signature_present"] is True
    assert preview["request_payload"] == {"market": "0xmarket", "nested": {}}
    assert signature not in serialized
    assert "replayable-order-signature" not in serialized


def _shadow_evidence(*, captured_at: str, cutoff_at: str) -> dict[str, Any]:
    sample = {
        "signed_preview_validated": True,
        "first_leg": {
            "executable_depth_usd": "25",
            "signed_preview_depth_usd": "25",
            "fee_verified": True,
            "payload_fingerprint": "first",
        },
        "second_leg": {
            "executable_depth_usd": "30",
            "signed_preview_depth_usd": "30",
            "fee_verified": True,
            "payload_fingerprint": "second",
        },
        "economics": {
            "expected_profit_usd": "0.75",
            "minimum_profit_usd": "0.50",
            "net_edge": "0.04",
            "dynamic_threshold": "0.025",
            "fixed_chain_cost_usd": "0.10",
        },
    }
    return {
        "route": "polymarket_predict",
        "runtime_instance_id": "quote_arb",
        "release_sha": "abc123",
        "captured_at": captured_at,
        "market_key": "predict:1",
        "market": {"symbol": "BTC target", "cutoff_at": cutoff_at},
        "completed_samples": 3,
        "required_samples": 3,
        "samples": [deepcopy(sample), deepcopy(sample), deepcopy(sample)],
    }


def test_shadow_openability_parser_accepts_multiple_configs() -> None:
    args = shadow_openability.build_parser().parse_args(  # noqa: SLF001
        [
            "--config",
            "config.production.clob_hft.json",
            "--config",
            "config.production.quote_arb.json",
            "--artifact-dir",
            "artifacts",
        ]
    )

    assert args.config == [
        "config.production.clob_hft.json",
        "config.production.quote_arb.json",
    ]
    assert args.stop_on == "all_routes_technical_openable"


def test_shadow_openability_accepts_evidence_that_was_valid_at_capture() -> None:
    config = SimpleNamespace(
        runtime_instance_id="quote_arb",
        position_size_usd=20.0,
        shadow_preflight_samples=3,
        spread_policy=SimpleNamespace(
            depth_buffer=1.25,
            min_expected_profit_usd=0.50,
            threshold_for=lambda route: 0.025,
        ),
    )
    window_start = shadow_openability._parse_time("2026-08-14T00:00:00Z")  # noqa: SLF001
    observed_at = shadow_openability._parse_time("2026-08-14T03:00:00Z")  # noqa: SLF001
    assert window_start is not None
    assert observed_at is not None

    result = shadow_openability._validate_evidence(  # noqa: SLF001
        _shadow_evidence(
            captured_at="2026-08-14T01:00:00Z",
            cutoff_at="2026-08-14T02:00:00Z",
        ),
        route="polymarket_predict",
        config=config,
        expected_release_sha="abc123",
        window_start=window_start,
        observed_at=observed_at,
    )

    assert result["accepted"] is True
    assert result["blockers"] == []


def test_shadow_openability_rejects_incomplete_or_uneconomic_evidence() -> None:
    config = SimpleNamespace(
        runtime_instance_id="quote_arb",
        position_size_usd=20.0,
        shadow_preflight_samples=3,
        spread_policy=SimpleNamespace(
            depth_buffer=1.25,
            min_expected_profit_usd=0.50,
            threshold_for=lambda route: 0.025,
        ),
    )
    evidence = _shadow_evidence(
        captured_at="2026-08-14T01:00:00Z",
        cutoff_at="2026-08-14T02:00:00Z",
    )
    evidence["samples"][0]["signed_preview_validated"] = False
    evidence["samples"][1]["second_leg"]["signed_preview_depth_usd"] = "10"
    evidence["samples"][2]["economics"]["expected_profit_usd"] = "0.10"
    window_start = shadow_openability._parse_time("2026-08-14T00:00:00Z")  # noqa: SLF001
    observed_at = shadow_openability._parse_time("2026-08-14T01:30:00Z")  # noqa: SLF001
    assert window_start is not None
    assert observed_at is not None

    result = shadow_openability._validate_evidence(  # noqa: SLF001
        evidence,
        route="polymarket_predict",
        config=config,
        expected_release_sha="abc123",
        window_start=window_start,
        observed_at=observed_at,
    )

    assert result["accepted"] is False
    assert "sample_1:signature_missing" in result["blockers"]
    assert "sample_2:second_leg:signed_depth" in result["blockers"]
    assert "sample_3:profit_floor" in result["blockers"]


def test_shadow_openability_latches_first_accepted_route_state() -> None:
    observed_at = shadow_openability._parse_time("2026-08-14T01:00:00Z")  # noqa: SLF001
    assert observed_at is not None
    accepted, newly_accepted = shadow_openability._latch_route_state(  # noqa: SLF001
        {"accepted": False, "blockers": ["evidence_missing"]},
        {"accepted": True, "blockers": [], "market_key": "predict:1"},
        observed_at=observed_at,
    )
    retained, accepted_again = shadow_openability._latch_route_state(  # noqa: SLF001
        accepted,
        {"accepted": False, "blockers": ["evidence_expired"]},
        observed_at=observed_at,
    )

    assert newly_accepted is True
    assert accepted_again is False
    assert retained == accepted


def test_shadow_openability_requires_safe_paused_shadow_at_first_observation() -> None:
    candidate = {"accepted": True, "blockers": [], "market_key": "predict:1"}

    rejected = shadow_openability._require_safe_runtime(  # noqa: SLF001
        candidate,
        {"safe_paused_shadow": False},
    )
    accepted = shadow_openability._require_safe_runtime(  # noqa: SLF001
        candidate,
        {"safe_paused_shadow": True},
    )

    assert rejected["accepted"] is False
    assert rejected["blockers"] == ["runtime_not_safe_paused_shadow"]
    assert accepted is candidate


def _runtime_metrics(*, mode: str, risk_paused: int, ready: int) -> str:
    return (
        f'arbitrage_execution_mode_info{{mode="{mode}"}} 1\n'
        f"arbitrage_risk_paused {risk_paused}\n"
        f"arbitrage_ready {ready}\n"
    )


def test_runtime_health_gate_accepts_only_operator_pause_in_shadow() -> None:
    result = runtime_health_gate.evaluate_runtime_health(
        (200, '{"status":"live"}'),
        (
            503,
            '{"status":"not_ready","runtime_instance_id":"clob_hft",'
            '"reasons":["risk_paused:operator closeout"]}',
        ),
        (200, _runtime_metrics(mode="shadow", risk_paused=1, ready=0)),
        expected_runtime_instance_id="clob_hft",
        expected_mode="shadow",
        accepted_state="safe_paused_shadow",
    )

    assert result["accepted"] is True
    assert result["safe_paused_shadow"] is True


def test_runtime_health_gate_rejects_paused_shadow_with_additional_blocker() -> None:
    result = runtime_health_gate.evaluate_runtime_health(
        (200, '{"status":"live"}'),
        (
            503,
            '{"status":"not_ready","runtime_instance_id":"quote_arb",'
            '"reasons":["risk_paused:operator closeout","market_data_invalid:Predict.fun"]}',
        ),
        (200, _runtime_metrics(mode="shadow", risk_paused=1, ready=0)),
        expected_runtime_instance_id="quote_arb",
        expected_mode="shadow",
        accepted_state="safe_paused_shadow",
    )

    assert result["accepted"] is False
    assert result["safe_paused_shadow"] is False


def test_runtime_health_gate_accepts_bootstrap_with_only_fresh_missing_routes() -> None:
    result = runtime_health_gate.evaluate_runtime_health(
        (200, '{"status":"live"}'),
        (
            503,
            '{"status":"not_ready","runtime_instance_id":"clob_hft",'
            '"reasons":["risk_paused:safe bootstrap","discovery_not_ready"],'
            '"discovery":{"missing_routes":["sx_myriad"],"last_error":null,"stale":false}}',
        ),
        (200, _runtime_metrics(mode="shadow", risk_paused=1, ready=0)),
        expected_runtime_instance_id="clob_hft",
        expected_mode="shadow",
        accepted_state="safe_paused_shadow_bootstrap",
    )

    assert result["accepted"] is True
    assert result["safe_paused_shadow"] is False
    assert result["safe_paused_shadow_bootstrap"] is True


def test_runtime_health_gate_bootstrap_rejects_other_readiness_blockers() -> None:
    result = runtime_health_gate.evaluate_runtime_health(
        (200, '{"status":"live"}'),
        (
            503,
            '{"status":"not_ready","runtime_instance_id":"quote_arb",'
            '"reasons":["risk_paused:safe bootstrap","discovery_not_ready",'
            '"market_data_invalid:Predict.fun"],'
            '"discovery":{"missing_routes":["predict_myriad"],"last_error":null,"stale":false}}',
        ),
        (200, _runtime_metrics(mode="shadow", risk_paused=1, ready=0)),
        expected_runtime_instance_id="quote_arb",
        expected_mode="shadow",
        accepted_state="safe_paused_shadow_bootstrap",
    )

    assert result["accepted"] is False
    assert result["safe_paused_shadow_bootstrap"] is False


def test_runtime_health_gate_ready_policy_rejects_wrong_mode() -> None:
    result = runtime_health_gate.evaluate_runtime_health(
        (200, '{"status":"live"}'),
        (200, '{"status":"ready","runtime_instance_id":"quote_arb","reasons":[]}'),
        (200, _runtime_metrics(mode="shadow", risk_paused=0, ready=1)),
        expected_runtime_instance_id="quote_arb",
        expected_mode="canary",
        accepted_state="ready",
    )

    assert result["accepted"] is False


def test_live_readiness_json_transport_rejects_unknown_types() -> None:
    with pytest.raises(TypeError, match="SimpleNamespace"):
        json.dumps(SimpleNamespace(), default=live_readiness._json_default)  # noqa: SLF001


def test_live_readiness_streams_json_report_with_decimal_precision() -> None:
    stream = io.StringIO()

    live_readiness._write_json_report(  # noqa: SLF001
        {"fee": Decimal("0.123456789012345678")},
        stream,
    )

    assert stream.getvalue().endswith("\n")
    assert json.loads(stream.getvalue()) == {"fee": "0.123456789012345678"}


def test_live_readiness_route_scope_rejects_disabled_routes() -> None:
    with pytest.raises(ValueError, match="not enabled"):
        live_readiness._select_audit_routes(  # noqa: SLF001
            ("polymarket_predict", "polymarket_myriad"),
            ["polymarket_sx"],
        )


def test_live_readiness_route_scope_deduplicates_and_limits_venue_gates() -> None:
    selected = live_readiness._select_audit_routes(  # noqa: SLF001
        ("polymarket_predict", "polymarket_myriad"),
        ["polymarket_myriad", "polymarket_myriad"],
    )

    assert selected == ("polymarket_myriad",)
    assert live_readiness._route_venues(selected) == {"Polymarket", "Myriad"}  # noqa: SLF001


def test_live_readiness_route_scope_disables_unneeded_discovery_connectors() -> None:
    @dataclass(frozen=True)
    class FakeRoutes:
        polymarket_myriad: bool = True
        polymarket_predict: bool = True
        predict_myriad: bool = False
        predict_sx: bool = False
        polymarket_sx: bool = False
        sx_myriad: bool = False

    @dataclass(frozen=True)
    class FakeMyriad:
        enabled: bool = True

    @dataclass(frozen=True)
    class FakeConfig:
        routes: FakeRoutes
        enable_predict_fun: bool
        enable_sx_bet: bool
        myriad_markets: FakeMyriad

    scoped = live_readiness._scope_app_config(  # noqa: SLF001
        FakeConfig(
            routes=FakeRoutes(),
            enable_predict_fun=True,
            enable_sx_bet=False,
            myriad_markets=FakeMyriad(),
        ),
        ("polymarket_myriad",),
    )

    assert scoped.routes.polymarket_myriad is True
    assert scoped.routes.polymarket_predict is False
    assert scoped.enable_predict_fun is False
    assert scoped.myriad_markets.enabled is True


def test_all_market_go_no_go_requires_current_verified_and_openable_route() -> None:
    report = live_readiness._go_no_go_report(  # noqa: SLF001
        enabled_routes=("polymarket_predict",),
        mapping_coverage={
            "enabled_routes": {"polymarket_predict": {"has_verified": True, "verified_count": 610}}
        },
        observability={
            "live": {"ok": True},
            "ready": {"ok": True},
            "metrics": {"arbitrage_ready": 1.0, "arbitrage_risk_paused": 0.0},
        },
        venue_gates={"Polymarket": {"passed": True}, "Predict.fun": {"passed": True}},
        route_overlap={
            "routes": {"polymarket_predict": {"verified_tradable_count": 0}}
        },
        route_summary={
            "polymarket_predict": {
                "technical_openable_count": 0,
                "canary_openable_count": 0,
                "openable_count": 0,
            }
        },
    )

    assert report == {
        "technical_routes_ready": False,
        "technical_blocking_reasons": [
            "no_verified_tradable_market:polymarket_predict",
            "no_mechanically_openable_market:polymarket_predict",
        ],
        "ready_for_canary": False,
        "blocking_reasons": [
            "no_verified_tradable_market:polymarket_predict",
            "no_mechanically_openable_market:polymarket_predict",
            "no_natural_positive_openable_market_for_target",
        ],
        "non_blocking_waiting_reasons": [
            "no_natural_positive_openable_market:polymarket_predict"
        ],
    }


def test_all_market_go_no_go_passes_current_verified_and_openable_route() -> None:
    report = live_readiness._go_no_go_report(  # noqa: SLF001
        enabled_routes=("polymarket_predict",),
        mapping_coverage={
            "enabled_routes": {"polymarket_predict": {"has_verified": True, "verified_count": 1}}
        },
        observability={
            "live": {"ok": True},
            "ready": {"ok": True},
            "metrics": {"arbitrage_ready": 1.0, "arbitrage_risk_paused": 0.0},
        },
        venue_gates={"Polymarket": {"passed": True}, "Predict.fun": {"passed": True}},
        route_overlap={
            "routes": {"polymarket_predict": {"verified_tradable_count": 1}}
        },
        route_summary={
            "polymarket_predict": {
                "technical_openable_count": 1,
                "canary_openable_count": 1,
                "openable_count": 1,
            }
        },
    )

    assert report == {
        "technical_routes_ready": True,
        "technical_blocking_reasons": [],
        "ready_for_canary": True,
        "blocking_reasons": [],
        "non_blocking_waiting_reasons": [],
    }


def test_all_market_go_no_go_reports_technical_readiness_while_risk_is_paused() -> None:
    report = live_readiness._go_no_go_report(  # noqa: SLF001
        enabled_routes=("polymarket_predict",),
        mapping_coverage={
            "enabled_routes": {"polymarket_predict": {"has_verified": True, "verified_count": 1}}
        },
        observability={
            "live": {"ok": True},
            "ready": {"ok": False},
            "metrics": {"arbitrage_ready": 0.0, "arbitrage_risk_paused": 1.0},
        },
        venue_gates={
            "Polymarket": {"passed": False, "blocking_reasons": ["risk_paused"]},
            "Predict.fun": {"passed": False, "blocking_reasons": ["risk_paused"]},
        },
        route_overlap={
            "routes": {"polymarket_predict": {"verified_tradable_count": 1}}
        },
        route_summary={
            "polymarket_predict": {
                "technical_openable_count": 1,
                "economically_openable_count": 1,
                "canary_openable_count": 0,
                "openable_count": 0,
            }
        },
    )

    assert report["technical_routes_ready"] is True
    assert report["technical_blocking_reasons"] == []
    assert report["ready_for_canary"] is False
    assert report["blocking_reasons"] == [
        "canary_route_gate_failed:polymarket_predict",
        "health_ready_failed",
        "arbitrage_ready_not_1",
        "arbitrage_risk_paused_not_0",
        "venue_gate_failed:Polymarket",
        "venue_gate_failed:Predict.fun",
    ]
    assert report["non_blocking_waiting_reasons"] == []


def test_all_market_go_no_go_treats_legacy_unprofitable_report_as_canary_waiting() -> None:
    report = live_readiness._go_no_go_report(  # noqa: SLF001
        enabled_routes=("polymarket_predict",),
        mapping_coverage={
            "enabled_routes": {"polymarket_predict": {"has_verified": True, "verified_count": 1}}
        },
        observability={
            "live": {"ok": True},
            "ready": {"ok": True},
            "metrics": {"arbitrage_ready": 1.0, "arbitrage_risk_paused": 0.0},
        },
        venue_gates={"Polymarket": {"passed": True}, "Predict.fun": {"passed": True}},
        route_overlap={
            "routes": {"polymarket_predict": {"verified_tradable_count": 1}}
        },
        route_summary={
            "polymarket_predict": {
                "technical_openable_count": 1,
                "economically_openable_count": 0,
                "canary_openable_count": 0,
                "openable_count": 0,
            }
        },
    )

    assert report["technical_routes_ready"] is True
    assert report["technical_blocking_reasons"] == []
    assert report["ready_for_canary"] is False
    assert report["blocking_reasons"] == [
        "no_natural_positive_openable_market_for_target"
    ]
    assert report["non_blocking_waiting_reasons"] == [
        "no_natural_positive_openable_market:polymarket_predict"
    ]


def test_all_market_go_no_go_rejects_failed_canary_route_gate() -> None:
    report = live_readiness._go_no_go_report(  # noqa: SLF001
        enabled_routes=("polymarket_predict",),
        mapping_coverage={
            "enabled_routes": {"polymarket_predict": {"has_verified": True, "verified_count": 1}}
        },
        observability={
            "live": {"ok": True},
            "ready": {"ok": True},
            "metrics": {"arbitrage_ready": 1.0, "arbitrage_risk_paused": 0.0},
        },
        venue_gates={"Polymarket": {"passed": True}, "Predict.fun": {"passed": True}},
        route_overlap={
            "routes": {"polymarket_predict": {"verified_tradable_count": 1}}
        },
        route_summary={
            "polymarket_predict": {
                "technical_openable_count": 1,
                "economically_openable_count": 1,
                "canary_openable_count": 0,
                "openable_count": 0,
            }
        },
    )

    assert report["technical_routes_ready"] is True
    assert report["ready_for_canary"] is False
    assert report["blocking_reasons"] == [
        "canary_route_gate_failed:polymarket_predict"
    ]
    assert report["non_blocking_waiting_reasons"] == []


def test_all_market_go_no_go_keeps_legacy_canary_zero_fail_closed() -> None:
    report = live_readiness._go_no_go_report(  # noqa: SLF001
        enabled_routes=("polymarket_predict",),
        mapping_coverage={
            "enabled_routes": {"polymarket_predict": {"has_verified": True, "verified_count": 1}}
        },
        observability={
            "live": {"ok": True},
            "ready": {"ok": True},
            "metrics": {"arbitrage_ready": 1.0, "arbitrage_risk_paused": 0.0},
        },
        venue_gates={"Polymarket": {"passed": True}, "Predict.fun": {"passed": True}},
        route_overlap={
            "routes": {"polymarket_predict": {"verified_tradable_count": 1}}
        },
        route_summary={
            "polymarket_predict": {
                "technical_openable_count": 1,
                "canary_openable_count": 0,
                "openable_count": 0,
            }
        },
    )

    assert report["ready_for_canary"] is False
    assert report["blocking_reasons"] == [
        "canary_route_gate_failed:polymarket_predict"
    ]
    assert report["non_blocking_waiting_reasons"] == []


def test_predict_fun_preview_failure_report_blocks_missing_key() -> None:
    app_config = SimpleNamespace(
        min_venue_balance_usd=50.0,
        predict_fun=SimpleNamespace(
            collateral_token_address="0x" + "2" * 40,
            balance_function="balanceOf",
        ),
    )

    report = predict_preview._predict_failure_report(  # noqa: SLF001
        app_config=app_config,
        runtime_audit={"database_reachable": True},
        error="PREDICT_FUN_PRIVATE_KEY is not configured",
        blocking_reason="predict_fun_private_key_missing",
    )

    assert report["balance_probe_error"] == "PREDICT_FUN_PRIVATE_KEY is not configured"
    assert report["collateral_token_address"] == "0x" + "2" * 40
    assert report["balance_function"] == "balanceOf"
    assert report["canary_gate"]["passed"] is False
    assert report["canary_gate"]["blocking_reasons"] == ["predict_fun_private_key_missing"]
    assert report["effective_balance"]["runtime_audit"] == {"database_reachable": True}


def test_predict_fun_preview_readiness_requires_canary_gate() -> None:
    readiness = predict_preview._predict_order_preview_readiness(  # noqa: SLF001
        requested=True,
        private_key_configured=True,
        metadata_found=True,
        canary_gate_passed=False,
    )

    assert readiness["ready"] is False
    assert readiness["blocking_reasons"] == ["predict_fun_balance_or_runtime_gate_failed"]


def test_predict_fun_preview_canary_gate_uses_runtime_effective_balance() -> None:
    gate = predict_preview._predict_canary_gate(  # noqa: SLF001
        minimum_balance_usd=50.0,
        connector_balance=350.0,
        direct_balance=350.0,
        runtime_audit={
            "latest_runtime_balance_state": {
                "venues": {
                    "Predict.fun": {
                        "balance_cache_usd": "350",
                        "optimistic_debits_usd": "350",
                        "capital_reservations_usd": "0",
                        "effective_balance_usd": "0",
                        "available_after_reservations_usd": "0",
                    }
                }
            }
        },
    )

    assert gate["passed"] is False
    assert "runtime_effective_balance_below_minimum" in gate["blocking_reasons"]


def test_predict_fun_approvals_trade_scope_covers_standard_and_neg_risk_tracks() -> None:
    class FakeBuilder:
        def get_approval_steps(self, scope: Any) -> list[SimpleNamespace]:
            is_neg_risk = bool(scope.is_neg_risk)
            is_yield_bearing = bool(scope.is_yield_bearing)
            suffix = "yield" if is_yield_bearing else "standard"
            if is_neg_risk:
                return [SimpleNamespace(id=f"neg-{suffix}", type="ERC1155_APPROVAL", spender="0x2", token="0x3")]
            return [SimpleNamespace(id=f"std-{suffix}", type="ERC20_ALLOWANCE", spender="0x4", token="0x5")]

    steps = predict_approvals._select_approval_steps(  # noqa: SLF001
        FakeBuilder(),
        scope="trade",
        yield_bearing="both",
    )

    assert [step.id for step in steps] == ["std-standard", "neg-standard", "std-yield", "neg-yield"]


def test_predict_fun_approvals_report_counts_missing_steps() -> None:
    checks = [
        SimpleNamespace(
            step=SimpleNamespace(
                id="ok",
                type="ERC20_ALLOWANCE",
                spender="0x1",
                token="0x2",
                label="ok",
                description="ok",
            ),
            satisfied=True,
        ),
        SimpleNamespace(
            step=SimpleNamespace(
                id="missing",
                type="ERC1155_APPROVAL",
                spender="0x3",
                token="0x4",
                label="missing",
                description="missing",
            ),
            satisfied=False,
        ),
    ]
    args = SimpleNamespace(scope="trade", yield_bearing="both", apply=False)
    app_config = SimpleNamespace(
        predict_fun=SimpleNamespace(
            # secret-scan: allow-test-fixture
            private_key="0x59c6995e998f97a5a0044976f7d6f16b0f6c3b2f7d662d1e6f4c2d6a1dbeef01",
            account_address="0x0000000000000000000000000000000000000abc",
            chain_id=56,
            network="mainnet",
            api_base_url="https://api.predict.fun/",
        )
    )

    report = predict_approvals._report(  # noqa: SLF001
        app_config=app_config,
        args=args,
        builder=None,
        checks=checks,
        run_report=None,
    )

    assert report["mode"] == "predict_account"
    assert report["missing_step_count"] == 1
    assert report["missing_step_ids"] == ["missing"]
    assert report["submitted"] is False


def test_live_readiness_failed_venue_report_is_structured() -> None:
    report = live_readiness._failed_venue_report(  # noqa: SLF001
        venue="Predict.fun",
        minimum_balance_usd=50.0,
        runtime_audit={"database_reachable": True},
        error="boom",
        blocking_reason="predict_fun_balance_probe_failed",
        payload={"wallet_address": None},
    )

    assert report["balance_probe_error"] == "boom"
    assert report["wallet_address"] is None
    assert report["canary_gate"]["venue"] == "Predict.fun"
    assert report["canary_gate"]["passed"] is False
    assert report["canary_gate"]["blocking_reasons"] == ["predict_fun_balance_probe_failed"]
    assert report["effective_balance"]["runtime_audit"] == {"database_reachable": True}


def test_live_readiness_effective_balance_payload_prefers_runtime_effective_balance() -> None:
    payload = live_readiness._effective_balance_payload(  # noqa: SLF001
        "Predict.fun",
        350.0,
        direct_balance=350.0,
        runtime_audit={
            "latest_runtime_balance_state": {
                "venues": {
                    "Predict.fun": {
                        "balance_cache_usd": "350",
                        "optimistic_debits_usd": "25",
                        "capital_reservations_usd": "10",
                        "effective_balance_usd": "325",
                        "available_after_reservations_usd": "315",
                    }
                }
            }
        },
    )

    assert payload["effective_balance_usd"] == 325.0
    assert payload["available_after_reservations_usd"] == 315.0
    assert payload["runtime_balance_cache_vs_connector_delta_usd"] == 0.0


def test_live_readiness_order_preview_requires_canary_gate() -> None:
    readiness = live_readiness._order_preview_readiness(  # noqa: SLF001
        requested=True,
        private_key_configured=True,
        market_metadata_found=True,
        canary_gate_passed=False,
    )

    assert readiness["ready"] is False
    assert readiness["blocking_reasons"] == ["balance_or_runtime_gate_failed"]


def test_live_canary_window_defaults_compose_service_by_runtime_instance() -> None:
    assert live_canary._normalize_compose_services("clob_hft", None) == ["bot-clob-hft"]  # noqa: SLF001
    assert live_canary._normalize_compose_services("quote_arb", None) == ["bot-quote-arb"]  # noqa: SLF001


def test_live_canary_window_serializes_runtime_decimal_without_losing_precision() -> None:
    payload = json.dumps(
        {"pending_unhedged_exposure_usd": Decimal("0.123456789012345678")},
        default=live_canary._json_default,  # noqa: SLF001
    )

    assert json.loads(payload) == {"pending_unhedged_exposure_usd": "0.123456789012345678"}


def test_live_canary_window_parser_accepts_multiple_compose_services() -> None:
    args = live_canary.build_parser().parse_args(  # noqa: SLF001
        [
            "--artifact-dir",
            "artifacts",
            "--database-url",
            "postgresql+asyncpg://user:pass@127.0.0.1:5432/arbitrage",
            "--compose-service",
            "bot-clob-hft",
            "--compose-service",
            "bot-quote-arb",
        ]
    )

    assert args.database_url == "postgresql+asyncpg://user:pass@127.0.0.1:5432/arbitrage"
    assert args.compose_service == ["bot-clob-hft", "bot-quote-arb"]
    assert args.duration_seconds == 14400
    assert args.database_poll_seconds == 60
    assert args.database_timeout_seconds == 45.0


def test_live_canary_config_integrity_detects_allowlist_mutation(tmp_path: Path) -> None:
    config_path = tmp_path / "config.production.quote_arb.json"
    payload = {
        "routes": {
            "polymarket_myriad": True,
            "polymarket_predict": True,
            "predict_myriad": True,
        },
        "funded_routes": {
            "polymarket_myriad": True,
            "polymarket_predict": True,
            "predict_myriad": False,
        },
    }
    config_path.write_text(json.dumps(payload), encoding="utf-8")
    expected_sha256 = hashlib.sha256(config_path.read_bytes()).hexdigest()
    expected_routes = ("polymarket_myriad", "polymarket_predict")

    original = live_canary._config_integrity_snapshot(  # noqa: SLF001
        config_path,
        expected_sha256=expected_sha256,
        expected_funded_routes=expected_routes,
    )
    assert original["passed"]

    payload["funded_routes"]["predict_myriad"] = True
    config_path.write_text(json.dumps(payload), encoding="utf-8")
    mutated = live_canary._config_integrity_snapshot(  # noqa: SLF001
        config_path,
        expected_sha256=expected_sha256,
        expected_funded_routes=expected_routes,
    )

    assert not mutated["passed"]
    assert mutated["actual_config_sha256"] != expected_sha256
    assert "predict_myriad" in mutated["actual_funded_routes"]


def test_live_canary_window_parser_accepts_database_sampling_controls() -> None:
    args = live_canary.build_parser().parse_args(  # noqa: SLF001
        [
            "--artifact-dir",
            "artifacts",
            "--database-poll-seconds",
            "120",
            "--database-timeout-seconds",
            "20",
        ]
    )

    assert args.database_poll_seconds == 120
    assert args.database_timeout_seconds == 20.0


def test_live_canary_window_parser_accepts_armed_shared_deadline_controls() -> None:
    args = live_canary.build_parser().parse_args(  # noqa: SLF001
        [
            "--artifact-dir",
            "artifacts",
            "--await-risk-resume",
            "--armed-file",
            "artifacts/armed.json",
            "--deadline-unix",
            "2000000000",
        ]
    )

    assert args.await_risk_resume is True
    assert args.armed_file == "artifacts/armed.json"
    assert args.deadline_unix == 2_000_000_000.0


def test_live_canary_window_parser_accepts_shared_deadline_file() -> None:
    args = live_canary.build_parser().parse_args(  # noqa: SLF001
        [
            "--artifact-dir",
            "artifacts",
            "--await-risk-resume",
            "--armed-file",
            "artifacts/armed.json",
            "--deadline-file",
            ".runtime/canary-control/deadline",
        ]
    )

    assert args.deadline_file == ".runtime/canary-control/deadline"


def test_live_canary_counter_snapshot_defaults_uninitialized_route_to_zero() -> None:
    counters = live_canary._accepted_preflight_counters(  # noqa: SLF001
        {
            "metrics": {
                "arbitrage_entry_preflight_accepted_total": {
                    "polymarket_sx": 2.0,
                }
            }
        },
        ("polymarket_sx", "predict_sx"),
    )

    assert counters == {"polymarket_sx": 2.0, "predict_sx": 0.0}


def test_live_canary_monitoring_streak_requires_all_local_probes_healthy() -> None:
    healthy_http = {
        "live": {"ok": True},
        "ready": {"ok": True},
        "metrics": {"ok": True},
    }
    healthy_observability = {
        "live": {"ok": True},
        "metrics": {"probe": {"ok": True}},
    }

    assert (
        live_canary._next_monitoring_failure_streak(  # noqa: SLF001
            1,
            http_snapshot=healthy_http,
            observability=healthy_observability,
        )
        == 0
    )
    unhealthy_http = {**healthy_http, "ready": {"ok": False}}
    assert (
        live_canary._next_monitoring_failure_streak(  # noqa: SLF001
            1,
            http_snapshot=unhealthy_http,
            observability=healthy_observability,
        )
        == 2
    )


@pytest.mark.asyncio
async def test_live_canary_pause_confirmation_waits_for_entry_quiescence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    snapshots = iter(
        (
            {"arbitrage_risk_paused": 1.0, "arbitrage_ready": 0.0, "arbitrage_entry_submission_in_progress": 1.0},
            {"arbitrage_risk_paused": 1.0, "arbitrage_ready": 0.0, "arbitrage_entry_submission_in_progress": 0.0},
            {"arbitrage_risk_paused": 1.0, "arbitrage_ready": 0.0, "arbitrage_entry_submission_in_progress": 0.0},
        )
    )

    async def _probe(host: str, port: int) -> dict[str, Any]:
        del host, port
        return {"metrics": next(snapshots)}

    async def _no_sleep(delay: float) -> None:
        del delay

    monkeypatch.setattr(live_canary, "probe_observability", _probe)
    monkeypatch.setattr(live_canary.asyncio, "sleep", _no_sleep)

    latest, passed = await live_canary._wait_for_paused_entry_quiescence(  # noqa: SLF001
        host="127.0.0.1",
        port=9108,
        timeout_seconds=1,
    )

    assert passed is True
    assert latest["metrics"]["arbitrage_entry_submission_in_progress"] == 0.0


def test_live_canary_window_log_capture_requires_every_requested_service() -> None:
    summary = live_canary._log_capture_summary(  # noqa: SLF001
        {
            "bot-clob-hft": {"ok": True, "returncode": 0},
            "bot-quote-arb": {"ok": True, "returncode": 0},
        },
        ["bot-clob-hft", "bot-quote-arb"],
    )

    assert summary == {
        "passed": True,
        "failure_count": 0,
        "failed_services": [],
    }


def test_live_canary_window_log_capture_fails_closed_on_missing_or_failed_service() -> None:
    summary = live_canary._log_capture_summary(  # noqa: SLF001
        {
            "bot-clob-hft": {"ok": False, "returncode": 1},
        },
        ["bot-clob-hft", "bot-quote-arb"],
    )

    assert summary == {
        "passed": False,
        "failure_count": 2,
        "failed_services": ["bot-clob-hft", "bot-quote-arb"],
    }


def test_live_canary_window_decodes_multiplexed_docker_logs() -> None:
    def _frame(stream_type: int, body: bytes) -> bytes:
        return bytes([stream_type, 0, 0, 0]) + len(body).to_bytes(4, "big") + body

    payload = _frame(1, b"stdout line\n") + _frame(2, b"stderr line\n")

    assert live_canary._decode_docker_log_stream(payload) == "stdout line\nstderr line\n"  # noqa: SLF001
    assert live_canary._decode_docker_log_stream(b"plain log\n") == "plain log\n"  # noqa: SLF001


def test_live_canary_window_uses_docker_socket_when_compose_cli_is_missing(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("COMPOSE_PROJECT_NAME", "labyda_next")
    monkeypatch.setattr(
        live_canary,
        "_run_command",
        lambda *args, **kwargs: {"ok": False, "error": "docker executable missing"},
    )
    captured: dict[str, Any] = {}

    def _socket_logs(**kwargs: Any) -> dict[str, Any]:
        captured.update(kwargs)
        return {"ok": True, "stdout": "captured\n", "stderr": "", "source": "docker_engine_api"}

    monkeypatch.setattr(live_canary, "_docker_socket_logs", _socket_logs)
    started_at = live_canary._utc_now()  # noqa: SLF001

    result = live_canary._capture_compose_logs(  # noqa: SLF001
        compose_cwd=tmp_path,
        compose_service="bot-quote-arb",
        started_at=started_at,
    )

    assert result["ok"] is True
    assert result["source"] == "docker_engine_api"
    assert result["compose_cli_error"] == {"error": "docker executable missing"}
    assert captured["compose_project"] == "labyda_next"
    assert captured["compose_service"] == "bot-quote-arb"
    assert captured["started_at"] == started_at


def test_live_canary_window_reads_latest_running_service_logs_from_docker_api(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []
    framed_logs = bytes([1, 0, 0, 0]) + len(b"service log\n").to_bytes(4, "big") + b"service log\n"

    def _api_get(socket_path: Path, path: str) -> tuple[int, bytes]:
        assert socket_path == Path("/var/run/docker.sock")
        calls.append(path)
        if path.startswith("/containers/json?"):
            return 200, json.dumps(
                [
                    {"Id": "old-container", "State": "exited", "Created": 100},
                    {"Id": "running-container", "State": "running", "Created": 90},
                ]
            ).encode()
        assert path.startswith("/containers/running-container/logs?")
        return 200, framed_logs

    monkeypatch.setattr(live_canary, "_docker_api_get", _api_get)

    result = live_canary._docker_socket_logs(  # noqa: SLF001
        socket_path=Path("/var/run/docker.sock"),
        compose_project="labyda_next",
        compose_service="bot-clob-hft",
        started_at=live_canary._utc_now(),  # noqa: SLF001
    )

    assert result["ok"] is True
    assert result["container_id"] == "running-container"
    assert result["stdout"] == "service log\n"
    assert len(calls) == 2


@pytest.mark.asyncio
async def test_live_canary_window_records_transient_database_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _timeout(*args: Any, **kwargs: Any) -> dict[str, Any]:
        del args, kwargs
        raise TimeoutError("synthetic database timeout")

    monkeypatch.setattr(live_canary, "_collect_database_state", _timeout)

    state, error = await live_canary._poll_database_state(  # noqa: SLF001
        SimpleNamespace(),
        started_at=live_canary._utc_now(),  # noqa: SLF001
        baseline_position_keys=set(),
        timeout_seconds=1,
        stage="poll",
    )

    assert state is None
    assert error is not None
    assert error["type"] == "TimeoutError"
    assert error["stage"] == "poll"


def test_live_canary_window_detects_synthetic_artifacts() -> None:
    assert live_canary._is_synthetic_order_payload(  # noqa: SLF001
        {"market_key": "integration:test", "token_id": "integration-token"}
    )
    assert live_canary._is_synthetic_position_key("restart:test-position")  # noqa: SLF001
    assert not live_canary._is_synthetic_order_payload(  # noqa: SLF001
        {"market_key": "real:test", "token_id": "real-token"}
    )


def test_live_canary_window_attributes_evidence_to_enabled_routes() -> None:
    evidence = live_canary._route_evidence(  # noqa: SLF001
        ("polymarket_predict", "polymarket_myriad"),
        [{"route": "polymarket_predict"}],
        [{"route": "polymarket_myriad"}],
    )

    assert evidence["polymarket_predict"] == {
        "real_fill_count": 1,
        "real_open_position_count": 0,
        "has_live_evidence": True,
    }
    assert evidence["polymarket_myriad"] == {
        "real_fill_count": 0,
        "real_open_position_count": 1,
        "has_live_evidence": True,
    }


def test_live_canary_window_rejects_route_outside_service_scope() -> None:
    with pytest.raises(SystemExit, match="not enabled"):
        live_canary._validated_required_routes(  # noqa: SLF001
            ("polymarket_predict",),
            ["polymarket_sx"],
        )


def test_polymarket_probe_candidate_rpc_urls_prefer_explicit_then_fallbacks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("POLYGON_RPC_URL", "https://env-rpc.example")

    candidates = polymarket_wallet_probe._candidate_rpc_urls("https://arg-rpc.example")  # noqa: SLF001

    assert candidates[0] == "https://arg-rpc.example"
    assert candidates[1] == "https://env-rpc.example"
    assert "https://polygon-bor-rpc.publicnode.com" in candidates


def test_live_readiness_balance_gate_uses_runtime_effective_balance_and_risk_state() -> None:
    gate = live_readiness._venue_balance_gate(  # noqa: SLF001
        venue="Predict.fun",
        minimum_balance_usd=50.0,
        connector_balance=350.0,
        direct_balance=350.0,
        runtime_audit={
            "latest_runtime_balance_state": {
                "venues": {
                    "Predict.fun": {
                        "balance_cache_usd": "350",
                        "optimistic_debits_usd": "320",
                        "capital_reservations_usd": "10",
                        "effective_balance_usd": "30",
                        "available_after_reservations_usd": "20",
                    }
                }
            },
            "risk_state": {"paused": True},
        },
    )

    assert gate["passed"] is False
    assert "runtime_effective_balance_below_minimum" in gate["blocking_reasons"]
    assert "runtime_available_balance_below_minimum" in gate["blocking_reasons"]
    assert "risk_paused" in gate["blocking_reasons"]


def test_live_readiness_requires_125_principal_plus_five_signed_preview_fees() -> None:
    audit = {
        "markets": [
            {
                "paired_preview_validated": True,
                "first_leg": {
                    "venue": "Polymarket",
                    "preview": {
                        "signing_validated": True,
                        "fee_metadata_verified": True,
                        "expected_fee_usd": "0.40",
                    },
                },
                "second_leg": {
                    "venue": "SX Bet",
                    "preview": {
                        "signing_validated": True,
                        "fee_metadata_verified": True,
                        "expected_fee_usd": "0.15",
                        "maximum_fee_usd": "0.25",
                    },
                },
            },
            {
                "paired_preview_validated": True,
                "first_leg": {
                    "venue": "Polymarket",
                    "preview": {
                        "signing_validated": True,
                        "fee_metadata_verified": True,
                        "expected_fee_usd": "0.50",
                    },
                },
                "second_leg": {
                    "venue": "SX Bet",
                    "preview": {
                        "signing_validated": True,
                        "fee_metadata_verified": True,
                        "expected_fee_usd": "0.10",
                    },
                },
            },
        ]
    }
    headroom = live_readiness._full_capacity_fee_headroom_by_venue(  # noqa: SLF001
        audit,
        venues={"Polymarket", "SX Bet"},
        max_positions=5,
    )
    assert headroom["Polymarket"]["fee_headroom_usd"] == Decimal("2.50")
    assert headroom["SX Bet"]["fee_headroom_usd"] == Decimal("1.25")

    polymarket: dict[str, Any] = {
        "effective_balance": {
            "connector_visible_balance_usd": 127.5,
            "direct_balance_usd": 127.5,
            "effective_balance_usd": 127.5,
            "available_after_reservations_usd": 127.5,
        },
        "canary_gate": {
            "venue": "Polymarket",
            "passed": False,
            "blocking_reasons": ["risk_paused"],
        },
    }
    sx_bet: dict[str, Any] = {
        "effective_balance": {
            "connector_visible_balance_usd": 126.25,
            "direct_balance_usd": 126.25,
            "effective_balance_usd": 126.25,
            "available_after_reservations_usd": 126.25,
        },
        "canary_gate": {
            "venue": "SX Bet",
            "passed": False,
            "blocking_reasons": ["risk_paused"],
        },
    }
    for venue, details in (("Polymarket", polymarket), ("SX Bet", sx_bet)):
        live_readiness._apply_full_capacity_balance_gate(  # noqa: SLF001
            details,
            principal_required_usd=Decimal("125"),
            fee_headroom=headroom[venue],
            max_positions=5,
        )

    readiness = live_readiness._full_capacity_funding_readiness(  # noqa: SLF001
        enabled_routes=("polymarket_sx",),
        venue_reports={"Polymarket": polymarket, "SX Bet": sx_bet},
        route_summary={
            "polymarket_sx": {
                "mechanically_openable_count": 1,
                "technical_openable_count": 1,
                "economically_openable_count": 1,
            }
        },
        max_positions=5,
    )
    assert readiness["ready"] is True
    assert polymarket["canary_gate"]["required_balance_usd"] == Decimal("127.50")

    polymarket["effective_balance"]["connector_visible_balance_usd"] = 127.49
    polymarket["canary_gate"]["blocking_reasons"] = ["risk_paused"]
    live_readiness._apply_full_capacity_balance_gate(  # noqa: SLF001
        polymarket,
        principal_required_usd=Decimal("125"),
        fee_headroom=headroom["Polymarket"],
        max_positions=5,
    )
    assert "connector_visible_balance_below_full_capacity" in polymarket["canary_gate"]["blocking_reasons"]


def test_live_readiness_fails_closed_without_verified_signed_fee_preview() -> None:
    headroom = live_readiness._full_capacity_fee_headroom_by_venue(  # noqa: SLF001
        {"markets": []},
        venues={"Myriad"},
        max_positions=5,
    )
    details: dict[str, Any] = {
        "effective_balance": {"connector_visible_balance_usd": 1000.0},
        "canary_gate": {"venue": "Myriad", "passed": True, "blocking_reasons": []},
    }

    live_readiness._apply_full_capacity_balance_gate(  # noqa: SLF001
        details,
        principal_required_usd=Decimal("125"),
        fee_headroom=headroom["Myriad"],
        max_positions=5,
    )

    assert details["canary_gate"]["passed"] is False
    assert details["canary_gate"]["blocking_reasons"] == ["full_capacity_fee_headroom_unverified"]


def test_full_capacity_requires_mechanical_proof_for_every_route_but_not_edge_on_every_route() -> None:
    ready_gate = {
        "passed": False,
        "blocking_reasons": ["risk_paused"],
    }
    readiness = live_readiness._full_capacity_funding_readiness(  # noqa: SLF001
        enabled_routes=("polymarket_sx", "predict_sx"),
        venue_reports={
            "Polymarket": {"canary_gate": dict(ready_gate)},
            "Predict.fun": {"canary_gate": dict(ready_gate)},
            "SX Bet": {"canary_gate": dict(ready_gate)},
        },
        route_summary={
            "polymarket_sx": {
                "mechanically_openable_count": 1,
                "technical_openable_count": 1,
                "economically_openable_count": 1,
            },
            "predict_sx": {
                "mechanically_openable_count": 1,
                "technical_openable_count": 0,
                "economically_openable_count": 0,
            },
        },
        max_positions=5,
    )

    assert readiness["ready"] is True
    assert readiness["blocking_reasons"] == []
    assert readiness["non_blocking_waiting_reasons"] == [
        "no_natural_positive_openable_market:predict_sx"
    ]


def test_sx_preview_failure_report_blocks_balance_probe_error() -> None:
    app_config = SimpleNamespace(
        min_venue_balance_usd=50.0,
        sx_bet=SimpleNamespace(
            base_token_address="0x" + "3" * 40,
        ),
    )

    report = sx_preview._sx_failure_report(  # noqa: SLF001
        app_config=app_config,
        runtime_audit={"database_reachable": True},
        error="boom",
        blocking_reason="sx_bet_balance_probe_failed",
    )

    assert report["balance_probe_error"] == "boom"
    assert report["base_token_address"] == "0x" + "3" * 40
    assert report["canary_gate"]["passed"] is False
    assert report["canary_gate"]["blocking_reasons"] == ["sx_bet_balance_probe_failed"]
    assert report["effective_balance"]["runtime_audit"] == {"database_reachable": True}


def test_sx_preview_effective_balance_payload_prefers_runtime_effective_balance() -> None:
    payload = sx_preview._effective_balance_payload(  # noqa: SLF001
        "SX Bet",
        350.0,
        direct_balance=350.0,
        runtime_audit={
            "latest_runtime_balance_state": {
                "venues": {
                    "SX Bet": {
                        "balance_cache_usd": "350",
                        "optimistic_debits_usd": "40",
                        "capital_reservations_usd": "15",
                        "effective_balance_usd": "310",
                        "available_after_reservations_usd": "295",
                    }
                }
            }
        },
    )

    assert payload["effective_balance_usd"] == 310.0
    assert payload["available_after_reservations_usd"] == 295.0
    assert payload["runtime_balance_cache_vs_connector_delta_usd"] == 0.0


def test_sx_preview_canary_gate_uses_runtime_effective_balance_and_risk_state() -> None:
    gate = sx_preview._sx_canary_gate(  # noqa: SLF001
        minimum_balance_usd=50.0,
        connector_balance=350.0,
        direct_balance=350.0,
        explorer_balance={"ok": True, "balance_usd": 350.0},
        runtime_audit={
            "latest_runtime_balance_state": {
                "venues": {
                    "SX Bet": {
                        "balance_cache_usd": "350",
                        "optimistic_debits_usd": "320",
                        "capital_reservations_usd": "10",
                        "effective_balance_usd": "30",
                        "available_after_reservations_usd": "20",
                    }
                }
            },
            "risk_state": {"paused": True},
        },
    )

    assert gate["passed"] is False
    assert "runtime_effective_balance_below_minimum" in gate["blocking_reasons"]
    assert "runtime_available_balance_below_minimum" in gate["blocking_reasons"]
    assert "risk_paused" in gate["blocking_reasons"]


def test_sx_match_probe_selects_filtered_subset() -> None:
    markets = [
        SimpleNamespace(symbol="Will France win the World Cup?", target_label="France"),
        SimpleNamespace(symbol="Will England win the World Cup?", target_label="England"),
        SimpleNamespace(symbol="Will BTC exceed 100000?", target_label="BTC"),
    ]

    selected = sx_match_probe._select_probe_markets(markets, "World Cup", 1)  # noqa: SLF001

    assert len(selected) == 1
    assert selected[0].symbol == "Will France win the World Cup?"


def test_sx_match_probe_selected_rows_include_sx_identifiers() -> None:
    rows = sx_match_probe._selected_market_rows(  # noqa: SLF001
        [
            SimpleNamespace(
                symbol="Will Los Angeles Rams beat San Francisco 49ers?",
                target_label="Los Angeles Rams",
                predict_fun_market_id="0xmarket",
                predict_fun_token_id="0xmarket:NO",
                predict_fun_side=SimpleNamespace(value="NO"),
            )
        ]
    )

    assert rows == [
        {
            "symbol": "Will Los Angeles Rams beat San Francisco 49ers?",
            "target_label": "Los Angeles Rams",
            "sx_market_hash": "0xmarket",
            "sx_token_id": "0xmarket:NO",
            "sx_side": "NO",
        }
    ]


def test_sx_match_probe_identity_uses_market_hash_and_token_id() -> None:
    market = SimpleNamespace(predict_fun_market_id="0xmarket", predict_fun_token_id="0xmarket:YES")

    assert sx_match_probe._sx_identity(market) == ("0xmarket", "0xmarket:YES")  # noqa: SLF001


def test_sx_probe_env_alias_prefers_sx_bet_names(monkeypatch: object) -> None:
    monkeypatch.setenv("SX_BET_API_KEY", "new-key")  # type: ignore[attr-defined]
    monkeypatch.setenv("SX_API_KEY", "old-key")  # type: ignore[attr-defined]

    assert sx_probe._env_first("SX_BET_API_KEY", "SX_API_KEY") == "new-key"  # noqa: SLF001


def test_sx_probe_env_alias_falls_back_to_legacy_name(monkeypatch: object) -> None:
    monkeypatch.delenv("SX_BET_API_KEY", raising=False)  # type: ignore[attr-defined]
    monkeypatch.setenv("SX_API_KEY", "old-key")  # type: ignore[attr-defined]

    assert sx_probe._env_first("SX_BET_API_KEY", "SX_API_KEY") == "old-key"  # noqa: SLF001


def test_sx_probe_active_markets_uses_cursor_pagination(monkeypatch: object) -> None:
    urls: list[str] = []

    def fake_http_json(url: str, **_kwargs: object) -> dict[str, object]:
        urls.append(url)
        if len(urls) == 1:
            return {"data": {"markets": [{"marketHash": "first"}], "nextKey": "cursor-2"}}
        return {"data": {"markets": [{"marketHash": "second"}]}}

    monkeypatch.setattr(sx_probe, "_http_json", fake_http_json)  # type: ignore[attr-defined]

    markets = sx_probe._active_markets("https://api.sx.bet")  # noqa: SLF001

    assert [market["marketHash"] for market in markets] == ["first", "second"]
    assert urls == [
        "https://api.sx.bet/markets/active?pageSize=100",
        "https://api.sx.bet/markets/active?pageSize=100&paginationKey=cursor-2",
    ]


def test_sx_probe_does_not_accept_api_key_on_command_line() -> None:
    source = (Path(__file__).resolve().parents[1] / "scripts" / "sx_bet_probe.py").read_text(encoding="utf-8")

    assert 'parser.add_argument("--api-key"' not in source


def test_sx_probe_never_exposes_realtime_token_prefix(monkeypatch: object) -> None:
    captured: dict[str, object] = {}

    def fake_http_json(url: str, *, headers: dict[str, str]) -> dict[str, object]:
        captured.update({"url": url, "headers": headers})
        return {"data": {"token": "sensitive-realtime-token"}}

    monkeypatch.setattr(  # type: ignore[attr-defined]
        sx_probe,
        "_http_json",
        fake_http_json,
    )

    result = sx_probe._fetch_realtime_token("https://api.sx.bet", "api-key", "v3")  # noqa: SLF001

    assert result == {"token_present": True}
    assert captured == {
        "url": "https://api.sx.bet/user/realtime-token-v3/api-key",
        "headers": {"x-sx-api-key": "api-key"},
    }


def test_sx_probe_disables_redirects_for_authenticated_requests(monkeypatch: object) -> None:
    authenticated_open_called = False

    class Response:
        def __enter__(self) -> Response:
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def read(self) -> bytes:
            return b'{"data": {"ok": true}}'

    def authenticated_open(request: object, *, timeout: int) -> Response:
        nonlocal authenticated_open_called
        del request
        authenticated_open_called = True
        assert timeout == 20
        return Response()

    def public_open(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("authenticated request followed the redirect-capable opener")

    monkeypatch.setattr(  # type: ignore[attr-defined]  # noqa: SLF001
        sx_probe._AUTHENTICATED_OPENER,
        "open",
        authenticated_open,
    )
    monkeypatch.setattr(  # type: ignore[attr-defined]
        sx_probe.urllib.request,
        "urlopen",
        public_open,
    )

    payload = sx_probe._http_json(  # noqa: SLF001
        "https://api.sx.bet/user/balance-v3",
        headers={"x-sx-api-key": "api-key"},
    )

    assert authenticated_open_called
    assert payload == {"data": {"ok": True}}


def test_sx_probe_v3_account_contracts_are_read_only_and_redacted(monkeypatch: object) -> None:
    calls: list[tuple[str, dict[str, str]]] = []

    def fake_http_json(url: str, *, headers: dict[str, str]) -> dict[str, object]:
        calls.append((url, headers))
        if "/realtime-token-v3/" in url:
            return {"data": {"token": "sensitive-realtime-token"}}
        if "/user/proxy" in url:
            return {"data": {"deployed": True, "obv3ProxyWalletAddress": "0xproxy"}}
        if "/user/balance-v3" in url:
            return {"data": {"balances": [{"availableAmount": "25000000"}]}}
        if "/user/fees-v3" in url:
            return {"data": {"takerPayoutFee": "0.025", "refundFee": "0.01"}}
        if "/orders-v3" in url:
            return {"data": {"orders": []}}
        if "/fills-v3" in url:
            return {"data": {"fills": []}}
        if "/positions-v3" in url:
            return {"data": {"positions": []}}
        raise AssertionError(url)

    monkeypatch.setattr(sx_probe, "_http_json", fake_http_json)  # type: ignore[attr-defined]

    result = sx_probe._fetch_v3_account_contracts(  # noqa: SLF001
        "https://api.sx.bet",
        "api-key",
    )

    assert result["realtime_token"] == {"token_present": True}
    assert result["proxy"] == {
        "response_valid": True,
        "deployed": True,
        "proxy_address_present": True,
    }
    assert result["balance"] == {"records": 1, "available_amount_present": True}
    assert result["fees"] == {
        "taker_payout_fee_present": True,
        "refund_fee_present": True,
    }
    assert "sensitive-realtime-token" not in json.dumps(result)
    assert len(calls) == 7
    assert all(headers == {"x-sx-api-key": "api-key"} for _, headers in calls)


def test_sx_probe_sorts_taker_levels_by_lowest_cost_first(monkeypatch: object) -> None:
    monkeypatch.setattr(  # type: ignore[attr-defined]
        sx_probe,
        "_http_json",
        lambda *_args, **_kwargs: {
            "data": [
                {
                    "orderHash": "expensive",
                    "percentageOdds": "60000000000000000000",
                    "isMakerBettingOutcomeOne": True,
                    "totalBetSize": "6000000",
                    "fillAmount": "0",
                    "pendingFillAmount": "0",
                },
                {
                    "orderHash": "cheap",
                    "percentageOdds": "70000000000000000000",
                    "isMakerBettingOutcomeOne": True,
                    "totalBetSize": "7000000",
                    "fillAmount": "0",
                    "pendingFillAmount": "0",
                },
            ]
        },
    )

    book = sx_probe._fetch_best_levels("https://api.sx.bet", "0xmarket")  # noqa: SLF001

    assert [level["order_hash"] for level in book["outcome_two"]] == ["cheap", "expensive"]
    assert [level["taker_implied"] for level in book["outcome_two"]] == [0.3, 0.4]


def test_sx_probe_never_sends_api_key_to_non_official_host(monkeypatch: object) -> None:
    called = False

    def fake_http_json(*args: object, **kwargs: object) -> dict[str, object]:
        nonlocal called
        called = True
        return {"data": {"token": "sensitive-realtime-token"}}

    monkeypatch.setattr(sx_probe, "_http_json", fake_http_json)  # type: ignore[attr-defined]

    with pytest.raises(ValueError, match="official SX Bet API host"):
        sx_probe._fetch_realtime_token(  # noqa: SLF001
            "https://api.sx.bet.evil.example",
            "api-key",
            "v3",
        )

    assert called is False


def test_sx_match_probe_formats_myriad_matches() -> None:
    rows = sx_match_probe._matched_market_rows(  # noqa: SLF001
        [
            SimpleNamespace(
                symbol="Will France win the World Cup?",
                target_label="France",
                myriad_market_id="1335",
                myriad_side=SimpleNamespace(value="NO"),
                myriad_condition_id="condition-1335",
                predict_fun_market_id="0xmarket",
                predict_fun_token_id="0xmarket:NO",
                predict_fun_side=SimpleNamespace(value="NO"),
                myriad_volume_usd=120000.0,
                predict_fun_volume_usd=80000.0,
            )
        ],
        route="myriad",
    )

    assert rows == [
        {
            "symbol": "Will France win the World Cup?",
            "target_label": "France",
            "myriad_market_id": "1335",
            "myriad_side": "NO",
            "myriad_execution_side_for_sx_myriad": "YES",
            "myriad_execution_token_for_sx_myriad": "1335:YES",
            "myriad_condition_id": "condition-1335",
            "sx_market_hash": "0xmarket",
            "sx_token_id": "0xmarket:NO",
            "sx_side": "NO",
            "myriad_volume_usd": 120000.0,
            "sx_volume_usd": 80000.0,
        }
    ]


def test_production_closeout_targets_split_services_and_deferred_backup_gates() -> None:
    script = Path(__file__).resolve().parents[1] / "ops" / "production_closeout.sh"
    body = script.read_text(encoding="utf-8")

    assert "config.production.clob_hft.json" in body
    assert "config.production.quote_arb.json" in body
    target_routes = body[body.index("target_routes()") : body.index("resolve_targets()")]
    assert 'config_path=$(target_config_path "$1")' in target_routes
    assert "funded_routes(load_config(sys.argv[1]))" in target_routes
    assert "printf '%s\\n' \"polymarket_sx\"" not in target_routes
    assert "--defer-backup-gates" in body
    assert "--compose-service" in body
    assert "./ops/operator_python.sh" in body
    assert "--profile operator build operator" in body
    assert "CREDENTIAL_ROTATION_CONFIRMED" in body
    assert "CREDENTIAL_ROTATION_RISK_ACCEPTED" not in body
    assert "funded canary requires CREDENTIAL_ROTATION_CONFIRMED=YES" in body
    assert "only FUNDED_CANARY_TARGET=quote_arb" in body
    assert 'funded_target_matches' in body
    assert 'clob_target_matches' in body
    assert 'quote_target_matches' in body
    assert 'CLOSEOUT_TARGETS subsets are forbidden' in body
    assert 'wait_for_paused_shadow "${target}"' in body
    assert 'funded_canary_target=${FUNDED_CANARY_TARGET}' in body
    assert 'export CLOB_HFT_EXECUTION_MODE=shadow' in body
    assert 'export QUOTE_ARB_EXECUTION_MODE=shadow' in body
    assert 'FORMAL_TARGETS=("quote_arb")' in body
    assert "assert_release_tree_clean" in body
    assert "assert_release_integrity" in body
    assert "expected_config_sha256" in body
    assert "--expected-funded-route" in body
    assert "env ARBITRAGE_EXECUTION_MODE_OVERRIDE=canary" in body
    assert "funded_canary_config_integrity_violation" not in body
    assert "CLOB_HFT_CONFIG_PATH" in body
    assert "QUOTE_ARB_CONFIG_PATH" in body
    assert 'for target in "${FUNDED_CANARY_TARGET}"' in body
    assert "--require-configured-reserve" in body
    assert "--write-config" not in body

    pre_approval = body.index("discovery-overlap-pre-approval")
    safe_approval = body.index("mappings approve-safe-candidates")
    calibration = body.index("scripts/shadow_calibration.py")
    post_calibration_reconcile = body.index("full-reconciliation-post-calibration")
    technical_audit = body.index("--technical-only")
    risk_resume = body.index("risk-resume-canary")
    deadline_publish = body.index(
        'printf \'%s\\n\' "${canary_deadline_unix}" >"${canary_deadline_file}"'
    )
    assert pre_approval < safe_approval < calibration < post_calibration_reconcile < technical_audit < risk_resume
    assert 'actual_commit_sha=$(git rev-parse HEAD)' in body
    assert "RESUME_RISK_FOR_SHADOW_CALIBRATION" not in body
    assert '--operator "${CLOSEOUT_OPERATOR}" --confirm YES' in body
    assert "production_closeout_exit_fail_closed" in body
    assert "pause_on_exit=0" in body
    assert "READY_WAIT_ATTEMPTS=${READY_WAIT_ATTEMPTS:-450}" in body
    assert "READY_WAIT_SLEEP_SECONDS=${READY_WAIT_SLEEP_SECONDS:-2}" in body
    assert "AUTO_APPROVE_SAFE_MAPPINGS=${AUTO_APPROVE_SAFE_MAPPINGS:-NO}" in body
    assert "AUTO_APPROVE_SAFE_MAPPINGS must be YES or NO" in body
    assert 'seq 1 "${READY_WAIT_ATTEMPTS}"' in body
    assert "DURATION_SECONDS=${DURATION_SECONDS:-14400}" in body
    assert "funded canary requires DURATION_SECONDS=14400" in body
    assert "funded canary requires CALIBRATION_DURATION_SECONDS=3600" in body
    assert "require_full_capacity_funding_ready" in body
    assert "require_shadow_transition_quiescent" in body
    assert "pre-shadow-transition-quiescence" in body
    assert "refusing shadow transition while PostgreSQL still contains managed state" in body
    assert "funded_canary_window_complete" in body
    assert "flock -n 9" in body
    assert "funded_canary_observer_failed" in body
    assert "funded_observer_failed_early" in body
    assert "--await-risk-resume" in body
    assert "--armed-file" in body
    assert "--deadline-file" in body
    assert 'printf \'%s\\n\' "${canary_deadline_unix}" >"${canary_deadline_file}"' in body
    assert 'wait_for_paused_canary "${FUNDED_CANARY_TARGET}"' in body
    assert "--post-window-paused" in body
    assert "final_audit_is_clean_for_shadow" in body
    assert "paused_shadow_clean" in body

    observer_wait = body.index('for pid in "${canary_pids[@]}"', body.index("canary_failed=0"))
    observer_liveness_monitor = body.index('while kill -0 "${deadline_watchdog_pid}"')
    window_pause = body.index("risk-pause-canary-window-complete")
    final_audit = body.index("production-audit-final")
    assert deadline_publish < risk_resume < window_pause < observer_wait < final_audit
    final_audit_block = body[final_audit : body.index("final_audit_is_clean_for_shadow", final_audit)]
    assert "env ARBITRAGE_EXECUTION_MODE_OVERRIDE=canary" in final_audit_block
    post_window_reconcile = body.index("full-reconciliation-post-window", final_audit)
    post_reconciliation_audit = body.index("production-audit-final-post-reconciliation", final_audit)
    post_window_quiescence = body.index("post-window-quiescence", final_audit)
    final_shadow_recreate = body.index('docker compose up -d --force-recreate "${all_services[@]}"', final_audit)
    assert final_audit < post_window_reconcile < post_reconciliation_audit
    assert post_reconciliation_audit < post_window_quiescence < final_shadow_recreate
    post_reconciliation_block = body[post_window_reconcile:final_shadow_recreate]
    assert post_reconciliation_block.count("env ARBITRAGE_EXECUTION_MODE_OVERRIDE=canary") >= 2
    assert 'require_shadow_transition_quiescent "${config_path}"' in post_reconciliation_block
    assert observer_liveness_monitor < observer_wait
    assert body.index("flock -n 9") < body.index("docker compose up -d")
    persistence_boot = body.index('docker compose up -d postgres migrate')
    shadow_quiescence = body.index('pre-shadow-transition-quiescence')
    bot_shadow_boot = body.index('docker compose up -d "${all_services[@]}"')
    assert persistence_boot < shadow_quiescence < bot_shadow_boot


@pytest.mark.skipif(os.name == "nt", reason="fcntl/flock regression runs in Linux CI")
def test_production_closeout_rejects_a_second_project_scoped_run(tmp_path: Path) -> None:
    import fcntl

    root = Path(__file__).resolve().parents[1]
    lock_path = tmp_path / "production-closeout.lock"
    with lock_path.open("w", encoding="utf-8") as lock_handle:
        fcntl_api: Any = fcntl
        fcntl_api.flock(lock_handle.fileno(), fcntl_api.LOCK_EX | fcntl_api.LOCK_NB)
        result = subprocess.run(
            ["bash", "ops/production_closeout.sh"],
            cwd=root,
            env={**os.environ, "CLOSEOUT_LOCK_FILE": str(lock_path)},
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )

    assert result.returncode != 0
    assert "another production_closeout.sh run already owns" in result.stderr


def test_operator_python_uses_one_off_compose_service_and_docker_socket() -> None:
    root = Path(__file__).resolve().parents[1]
    script = (root / "ops" / "operator_python.sh").read_text(encoding="utf-8")
    compose = (root / "docker-compose.yml").read_text(encoding="utf-8")

    assert "run --rm --no-deps operator" in script
    assert "DOCKER_GID" in script
    assert "OPERATOR_WORKSPACE" in script
    assert 'profiles: ["operator"]' in compose
    assert "working_dir: ${OPERATOR_WORKSPACE:-/workspace}" in compose
    assert "/var/run/docker.sock:/var/run/docker.sock" in compose
    assert "network_mode: host" in compose
    operator_block = compose.split("  operator:", 1)[1].split("  bot-clob-hft:", 1)[0]
    assert "ARBITRAGE_EXECUTION_MODE_OVERRIDE: ${ARBITRAGE_EXECUTION_MODE_OVERRIDE:-shadow}" in operator_block
    assert "LIVE_TRADING_CONFIRM: ${LIVE_TRADING_CONFIRM:-NO}" in operator_block
    assert "CI_VERIFIED_COMMIT_SHA: ${CI_VERIFIED_COMMIT_SHA:-}" in operator_block
    assert "ARBITRAGE_RUNTIME_ROLE: operator" in operator_block
    assert "mem_limit: 768m" in operator_block

    clob_block = compose.split("  bot-clob-hft:", 1)[1].split("  bot-quote-arb:", 1)[0]
    quote_block = compose.split("  bot-quote-arb:", 1)[1].split("  prometheus:", 1)[0]
    assert "ARBITRAGE_RUNTIME_ROLE: bot" in clob_block
    assert "ARBITRAGE_RUNTIME_ROLE: bot" in quote_block
    assert "FUNDED_CANARY_DEADLINE_UNIX: ${FUNDED_CANARY_DEADLINE_UNIX:-}" in clob_block
    assert "FUNDED_CANARY_DEADLINE_UNIX: ${FUNDED_CANARY_DEADLINE_UNIX:-}" in quote_block
    assert "FUNDED_CANARY_DEADLINE_FILE: /run/canary-control/deadline" in clob_block
    assert "FUNDED_CANARY_DEADLINE_FILE: /run/canary-control/deadline" in quote_block
    assert "./.runtime/canary-control:/run/canary-control:ro" in clob_block
    assert "./.runtime/canary-control:/run/canary-control:ro" in quote_block


def test_market_data_alert_uses_stream_liveness_not_quiet_book_age() -> None:
    root = Path(__file__).resolve().parents[1]
    alerts = (root / "ops" / "prometheus-alerts.yml").read_text(encoding="utf-8")
    expression = alerts.split("- alert: ArbitrageBookStale", 1)[1].split("- alert:", 1)[0]

    assert 'event="connected"' in expression
    assert 'event="reconnecting"' in expression
    assert "arbitrage_market_data_age_seconds" not in expression
