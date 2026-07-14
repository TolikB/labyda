from __future__ import annotations

import importlib.util
from pathlib import Path
from types import ModuleType


def _load_module() -> ModuleType:
    path = Path(__file__).resolve().parents[1] / "scripts" / "shadow_calibration.py"
    spec = importlib.util.spec_from_file_location("shadow_calibration_module", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


calibration = _load_module()


def _metrics(valid: int, observations: int, *, below_001: int, mode: str = "shadow") -> str:
    return f"""
arbitrage_execution_mode_info{{mode="{mode}"}} 1
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


def test_effective_mode_is_read_from_runtime_metrics() -> None:
    parsed = calibration.parse_prometheus(_metrics(0, 0, below_001=0, mode="shadow"))

    assert calibration.effective_execution_mode(parsed) == "shadow"
