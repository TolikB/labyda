from __future__ import annotations

import importlib.util
import json
import sys
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any

import pytest


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
polymarket_wallet_probe = _load_script_module(
    "polymarket_deposit_wallet_probe_module",
    "polymarket_deposit_wallet_probe.py",
)
predict_approvals = _load_script_module(
    "predict_fun_approvals_module",
    "predict_fun_approvals.py",
)


def test_live_readiness_json_transport_serializes_decimal_without_losing_precision() -> None:
    payload = json.dumps({"fee": Decimal("0.123456789012345678")}, default=live_readiness._json_default)  # noqa: SLF001

    assert json.loads(payload) == {"fee": "0.123456789012345678"}


def test_live_readiness_json_transport_rejects_unknown_types() -> None:
    with pytest.raises(TypeError, match="SimpleNamespace"):
        json.dumps(SimpleNamespace(), default=live_readiness._json_default)  # noqa: SLF001


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
            "no_technical_openable_market:polymarket_predict",
        ],
        "ready_for_canary": False,
        "blocking_reasons": [
            "no_verified_tradable_market:polymarket_predict",
            "no_technical_openable_market:polymarket_predict",
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
                "canary_openable_count": 0,
                "openable_count": 0,
            }
        },
    )

    assert report["technical_routes_ready"] is True
    assert report["technical_blocking_reasons"] == []
    assert report["ready_for_canary"] is False
    assert report["blocking_reasons"] == [
        "no_canary_openable_market:polymarket_predict",
        "health_ready_failed",
        "arbitrage_ready_not_1",
        "arbitrage_risk_paused_not_0",
        "venue_gate_failed:Polymarket",
        "venue_gate_failed:Predict.fun",
    ]


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
    assert args.database_poll_seconds == 60
    assert args.database_timeout_seconds == 45.0


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
    assert "--defer-backup-gates" in body
    assert "--compose-service" in body
    assert "./ops/operator_python.sh" in body
    assert "--profile operator build operator" in body
    assert "CREDENTIAL_ROTATION_CONFIRMED" in body
    assert "CREDENTIAL_ROTATION_RISK_ACCEPTED" in body
    assert "explicit credential risk acceptance" in body
    assert "--require-configured-reserve" in body
    assert "--write-config" not in body

    pre_approval = body.index("discovery-overlap-pre-approval")
    safe_approval = body.index("mappings approve-safe-candidates")
    calibration = body.index("scripts/shadow_calibration.py")
    assert pre_approval < safe_approval < calibration
    assert '--operator "${CLOSEOUT_OPERATOR}" --confirm YES' in body
    assert "production_closeout_exit_fail_closed" in body
    assert "pause_on_exit=0" in body
    assert "READY_WAIT_ATTEMPTS=${READY_WAIT_ATTEMPTS:-450}" in body
    assert "READY_WAIT_SLEEP_SECONDS=${READY_WAIT_SLEEP_SECONDS:-2}" in body
    assert 'seq 1 "${READY_WAIT_ATTEMPTS}"' in body


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
    assert "mem_limit: 512m" in operator_block


def test_market_data_alert_uses_stream_liveness_not_quiet_book_age() -> None:
    root = Path(__file__).resolve().parents[1]
    alerts = (root / "ops" / "prometheus-alerts.yml").read_text(encoding="utf-8")
    expression = alerts.split("- alert: ArbitrageBookStale", 1)[1].split("- alert:", 1)[0]

    assert 'event="connected"' in expression
    assert 'event="reconnecting"' in expression
    assert "arbitrage_market_data_age_seconds" not in expression
