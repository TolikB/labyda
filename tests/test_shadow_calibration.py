from __future__ import annotations

import importlib.util
from pathlib import Path
from types import ModuleType
from typing import Any


def _load_module() -> ModuleType:
    path = Path(__file__).resolve().parents[1] / "scripts" / "shadow_calibration.py"
    spec = importlib.util.spec_from_file_location("shadow_calibration_module", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


calibration = _load_module()


class _FakeResponse:
    def __init__(self, status: int, body: str) -> None:
        self.status = status
        self._body = body

    async def __aenter__(self) -> _FakeResponse:
        return self

    async def __aexit__(self, *args: object) -> None:
        del args

    async def text(self, *, errors: str) -> str:
        assert errors == "replace"
        return self._body


class _FakeSession:
    def __init__(self, response: _FakeResponse | BaseException) -> None:
        self._response = response

    def get(self, url: str) -> Any:
        assert url.startswith("http://127.0.0.1:")
        if isinstance(self._response, BaseException):
            raise self._response
        return self._response


async def test_http_get_uses_async_session_without_worker_threads() -> None:
    assert await calibration._http_get(_FakeSession(_FakeResponse(503, "not ready")), "http://127.0.0.1:9108/x") == (
        503,
        "not ready",
    )


async def test_http_get_normalizes_transport_failure() -> None:
    status, body = await calibration._http_get(
        _FakeSession(OSError("connection failed")),
        "http://127.0.0.1:9108/x",
    )

    assert status is None
    assert body == "connection failed"


def _metrics(
    valid: int,
    observations: int,
    *,
    below_001: int,
    mode: str = "shadow",
    risk_paused: int = 0,
    ready: int = 1,
) -> str:
    return f"""
arbitrage_execution_mode_info{{mode="{mode}"}} 1
arbitrage_risk_paused {risk_paused}
arbitrage_ready {ready}
arbitrage_calibration_valid_evaluations_total{{route="polymarket_sx"}} {valid}
arbitrage_calibration_adverse_move_pct_bucket{{le="0.0",route="polymarket_sx"}} 0
arbitrage_calibration_adverse_move_pct_bucket{{le="0.001",route="polymarket_sx"}} {below_001}
arbitrage_calibration_adverse_move_pct_bucket{{le="0.0025",route="polymarket_sx"}} {observations}
arbitrage_calibration_adverse_move_pct_bucket{{le="+Inf",route="polymarket_sx"}} {observations}
"""


def test_calibration_computes_conservative_p95_from_window_delta() -> None:
    start = calibration.parse_prometheus(_metrics(100, 10, below_001=9))
    end = calibration.parse_prometheus(_metrics(10_100, 110, below_001=104))

    result = calibration.calibration_result(("polymarket_sx",), start, end, 10_000)

    assert result["passed"] is True
    assert result["routes"]["polymarket_sx"]["valid_evaluation_count"] == 10_000
    assert result["routes"]["polymarket_sx"]["adverse_move_observation_count"] == 100
    assert result["routes"]["polymarket_sx"]["adverse_move_p95_pct"] == 0.001


def test_calibration_fails_closed_on_insufficient_samples_or_metric_reset() -> None:
    start = calibration.parse_prometheus(_metrics(100, 10, below_001=9))
    end = calibration.parse_prometheus(_metrics(50, 5, below_001=4))

    result = calibration.calibration_result(("polymarket_sx",), start, end, 10_000)

    assert result["passed"] is False
    blockers = result["routes"]["polymarket_sx"]["blockers"]
    assert "valid_evaluations_below_10000" in blockers
    assert "runtime_metrics_reset_during_window" in blockers


def test_configured_reserve_must_cover_observed_route_p95() -> None:
    start = calibration.parse_prometheus(_metrics(100, 10, below_001=9))
    end = calibration.parse_prometheus(_metrics(10_100, 110, below_001=104))
    result = calibration.calibration_result(("polymarket_sx",), start, end, 10_000)

    result = calibration.validate_configured_reserves(result, {"polymarket_sx": 0.0005})

    route = result["routes"]["polymarket_sx"]
    assert result["passed"] is False
    assert route["configured_adverse_move_reserve_pct"] == 0.0005
    assert "configured_adverse_move_reserve_below_observed_p95" in route["blockers"]


def test_configured_reserve_requires_route_specific_value() -> None:
    start = calibration.parse_prometheus(_metrics(100, 10, below_001=9))
    end = calibration.parse_prometheus(_metrics(10_100, 110, below_001=104))
    result = calibration.calibration_result(("polymarket_sx",), start, end, 10_000)

    result = calibration.validate_configured_reserves(result, {})

    assert result["passed"] is False
    assert "route_specific_adverse_move_reserve_missing" in result["routes"]["polymarket_sx"]["blockers"]


def test_configured_reserve_passes_when_it_is_conservative() -> None:
    start = calibration.parse_prometheus(_metrics(100, 10, below_001=9))
    end = calibration.parse_prometheus(_metrics(10_100, 110, below_001=104))
    result = calibration.calibration_result(("polymarket_sx",), start, end, 10_000)

    result = calibration.validate_configured_reserves(result, {"polymarket_sx": 0.0025})

    assert result["passed"] is True
    assert result["routes"]["polymarket_sx"]["blockers"] == []


def test_effective_mode_is_read_from_runtime_metrics() -> None:
    parsed = calibration.parse_prometheus(_metrics(0, 0, below_001=0, mode="shadow"))

    assert calibration.effective_execution_mode(parsed) == "shadow"


def test_runtime_health_sample_records_readiness_reasons() -> None:
    metrics = _metrics(0, 0, below_001=0, mode="shadow")
    sample = calibration.runtime_health_sample(
        (200, '{"status":"live"}'),
        (
            503,
            '{"status":"not_ready","runtime_instance_id":"clob_hft",'
            '"reasons":["market_data_invalid:SX Bet"]}',
        ),
        (200, metrics),
        expected_runtime_instance_id="clob_hft",
    )

    assert sample == {
        "live_status": 200,
        "ready_status": 503,
        "ready_runtime_instance_id": "clob_hft",
        "ready_runtime_instance_matches": True,
        "ready_reasons": ["market_data_invalid:SX Bet"],
        "ready_payload_valid": True,
        "metrics_status": 200,
        "execution_mode": "shadow",
        "risk_paused": 0.0,
        "ready_metric": 1.0,
        "safe_paused_shadow": False,
        "ok": False,
    }


def test_runtime_health_sample_fails_closed_on_invalid_ready_payload() -> None:
    sample = calibration.runtime_health_sample(
        (200, '{"status":"live"}'),
        (200, "not-json"),
        (200, _metrics(0, 0, below_001=0, mode="shadow")),
    )

    assert sample["ready_payload_valid"] is False
    assert sample["ready_reasons"] == []
    assert sample["ok"] is False


def test_runtime_health_sample_accepts_matching_ready_shadow_runtime() -> None:
    sample = calibration.runtime_health_sample(
        (200, '{"status":"live"}'),
        (200, '{"status":"ready","runtime_instance_id":"quote_arb","reasons":[]}'),
        (200, _metrics(0, 0, below_001=0, mode="shadow")),
        expected_runtime_instance_id="quote_arb",
    )

    assert sample["ready_runtime_instance_matches"] is True
    assert sample["ok"] is True


def test_runtime_health_sample_accepts_risk_pause_as_only_shadow_blocker() -> None:
    sample = calibration.runtime_health_sample(
        (200, '{"status":"live"}'),
        (
            503,
            '{"status":"not_ready","runtime_instance_id":"quote_arb",'
            '"reasons":["risk_paused:operator closeout"]}',
        ),
        (200, _metrics(0, 0, below_001=0, risk_paused=1, ready=0)),
        expected_runtime_instance_id="quote_arb",
    )

    assert sample["safe_paused_shadow"] is True
    assert sample["ok"] is True


def test_runtime_health_sample_rejects_pause_with_another_readiness_blocker() -> None:
    sample = calibration.runtime_health_sample(
        (200, '{"status":"live"}'),
        (
            503,
            '{"status":"not_ready","runtime_instance_id":"quote_arb",'
            '"reasons":["risk_paused:operator closeout","market_data_invalid:Predict.fun"]}',
        ),
        (200, _metrics(0, 0, below_001=0, risk_paused=1, ready=0)),
        expected_runtime_instance_id="quote_arb",
    )

    assert sample["safe_paused_shadow"] is False
    assert sample["ok"] is False


def test_runtime_health_sample_rejects_pause_reason_without_paused_metrics() -> None:
    sample = calibration.runtime_health_sample(
        (200, '{"status":"live"}'),
        (
            503,
            '{"status":"not_ready","runtime_instance_id":"quote_arb",'
            '"reasons":["risk_paused:operator closeout"]}',
        ),
        (200, _metrics(0, 0, below_001=0, risk_paused=0, ready=0)),
        expected_runtime_instance_id="quote_arb",
    )

    assert sample["safe_paused_shadow"] is False
    assert sample["ok"] is False


def test_runtime_health_sample_rejects_wrong_runtime_instance() -> None:
    sample = calibration.runtime_health_sample(
        (200, '{"status":"live"}'),
        (200, '{"status":"ready","runtime_instance_id":"clob_hft","reasons":[]}'),
        (200, _metrics(0, 0, below_001=0, mode="shadow")),
        expected_runtime_instance_id="quote_arb",
    )

    assert sample["ready_runtime_instance_matches"] is False
    assert sample["ok"] is False
