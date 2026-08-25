from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock

import pytest

from arbitrage_engine.config import load_config
from arbitrage_engine.database import ProductionRepository
from arbitrage_engine.discovery_lifecycle import DiscoveryDiagnostics
from arbitrage_engine.market_discovery import GammaCacheUnavailable
from arbitrage_engine.models import (
    BinarySide,
    ExecutionMode,
    MappingStatus,
    MarketConstraints,
    MarketSpec,
    OrderPreview,
    VenueFeeQuote,
    position_key,
)
from arbitrage_engine.production_audit import (
    RouteDiscoverySnapshot,
    _recent_shadow_preflight_evidence,
    _route_preview_economics,
    build_route_overlap_report,
    collect_all_market_audit,
    live_window_has_real_order_evidence,
    resolve_route_discovery_snapshot,
)


def _sx_market(symbol: str, token: str, market_id: str, *, verified_routes: frozenset[str]) -> MarketSpec:
    return MarketSpec(
        symbol=symbol,
        target_label="YES",
        polymarket_token_id=f"poly-{token}",
        polymarket_side=BinarySide.YES,
        polymarket_market_id=f"poly-{market_id}",
        condition_id=f"condition-{market_id}",
        predict_fun_token_id=token,
        predict_fun_side=BinarySide.NO,
        predict_fun_market_id=market_id,
        venue_b_label="SX Bet",
        myriad_market_id=f"myriad-{market_id}",
        myriad_side=BinarySide.YES,
        mapping_status=MappingStatus.VERIFIED if verified_routes else MappingStatus.CANDIDATE,
        verified_routes=verified_routes,
        rules_fingerprint=f"rules-{market_id}",
        resolution_source="Official event result",
        outcome_semantics="YES is the stated outcome",
        category="sports",
        polymarket_volume_usd=120_000,
        predict_fun_volume_usd=90_000,
        myriad_volume_usd=70_000,
    )


def test_recent_shadow_preflight_evidence_requires_exact_sha_freshness_and_three_signed_samples() -> None:
    base_config = load_config(Path(__file__).parents[1] / "config.example.json")
    config = replace(
        base_config,
        runtime_instance_id="clob_hft",
        position_size_usd=20,
        shadow_preflight_samples=3,
        shadow_preflight_evidence_ttl_seconds=900,
        spread_policy=replace(
            base_config.spread_policy,
            route_floors={"polymarket_sx": 0.015},
            min_expected_profit_usd=0.5,
            depth_buffer=1.25,
            require_live_gas_estimate=True,
        ),
    )
    market = _sx_market(
        "Evidence market",
        "sx-evidence-token",
        "sx-evidence-market",
        verified_routes=frozenset({"polymarket_sx"}),
    )
    now = datetime.now(UTC)
    sample = {
        "signed_preview_validated": True,
        "first_leg": {
            "fee_verified": True,
            "executable_depth_usd": "30",
            "signed_preview_depth_usd": "30",
        },
        "second_leg": {
            "fee_verified": True,
            "executable_depth_usd": "25",
            "signed_preview_depth_usd": "25",
        },
        "economics": {
            "expected_profit_usd": "1.25",
            "minimum_profit_usd": "0.50",
            "net_edge": "0.04",
            "fixed_chain_cost_usd": "0.10",
        },
    }
    evidence = {
        "route": "polymarket_sx",
        "market_key": position_key(market),
        "runtime_instance_id": "clob_hft",
        "release_sha": "a" * 40,
        "recorded_at": now.isoformat(),
        "completed_samples": 3,
        "required_samples": 3,
        "samples": [dict(sample) for _ in range(3)],
    }
    runtime_snapshot = {"latest_shadow_preflight_evidence_by_route": {"polymarket_sx": evidence}}

    result = _recent_shadow_preflight_evidence(
        route="polymarket_sx",
        app_config=config,
        runtime_snapshot=runtime_snapshot,
        eligible_markets_by_key={position_key(market): market},
        now=now,
        expected_release_sha="a" * 40,
    )

    assert result["accepted"] is True
    assert result["technical_accepted"] is True
    assert result["economically_openable"] is True
    assert result["blockers"] == []

    low_profit_sample = dict(sample)
    low_profit_economics = dict(cast(dict[str, Any], sample["economics"]))
    low_profit_economics.update(
        {
            "expected_profit_usd": "0.10",
            "net_edge": "0.005",
        }
    )
    low_profit_sample["economics"] = low_profit_economics
    evidence["samples"] = [dict(low_profit_sample) for _ in range(3)]
    waiting = _recent_shadow_preflight_evidence(
        route="polymarket_sx",
        app_config=config,
        runtime_snapshot=runtime_snapshot,
        eligible_markets_by_key={position_key(market): market},
        now=now,
        expected_release_sha="a" * 40,
    )
    assert waiting["accepted"] is False
    assert waiting["technical_accepted"] is False
    assert waiting["mechanical_preflight_accepted"] is True
    assert waiting["economically_openable"] is False
    assert waiting["technical_blockers"] == []
    assert "sample_1:expected_profit_below_minimum" in waiting["economic_blockers"]

    evidence["samples"] = [dict(sample) for _ in range(3)]

    wrong_sha = _recent_shadow_preflight_evidence(
        route="polymarket_sx",
        app_config=config,
        runtime_snapshot=runtime_snapshot,
        eligible_markets_by_key={position_key(market): market},
        now=now,
        expected_release_sha="b" * 40,
    )
    assert wrong_sha["accepted"] is False
    assert "release_sha_mismatch" in wrong_sha["blockers"]

    evidence["recorded_at"] = (now - timedelta(seconds=901)).isoformat()
    stale = _recent_shadow_preflight_evidence(
        route="polymarket_sx",
        app_config=config,
        runtime_snapshot=runtime_snapshot,
        eligible_markets_by_key={position_key(market): market},
        now=now,
        expected_release_sha="a" * 40,
    )
    assert stale["accepted"] is False
    assert "recent_shadow_evidence_expired" in stale["blockers"]

    evidence["recorded_at"] = (now + timedelta(seconds=6)).isoformat()
    future = _recent_shadow_preflight_evidence(
        route="polymarket_sx",
        app_config=config,
        runtime_snapshot=runtime_snapshot,
        eligible_markets_by_key={position_key(market): market},
        now=now,
        expected_release_sha="a" * 40,
    )
    assert future["accepted"] is False
    assert "recorded_at_in_future" in future["blockers"]

    evidence["recorded_at"] = now.isoformat()
    invalid_depth_sample = dict(sample)
    invalid_first_leg = dict(cast(dict[str, Any], sample["first_leg"]))
    invalid_first_leg["executable_depth_usd"] = "Infinity"
    invalid_depth_sample["first_leg"] = invalid_first_leg
    evidence["samples"] = [dict(invalid_depth_sample) for _ in range(3)]
    invalid_depth = _recent_shadow_preflight_evidence(
        route="polymarket_sx",
        app_config=config,
        runtime_snapshot=runtime_snapshot,
        eligible_markets_by_key={position_key(market): market},
        now=now,
        expected_release_sha="a" * 40,
    )
    assert invalid_depth["accepted"] is False
    assert "sample_1:first_leg_depth_invalid" in invalid_depth["blockers"]

    invalid_economics_sample = dict(sample)
    invalid_economics = dict(cast(dict[str, Any], sample["economics"]))
    invalid_economics["expected_profit_usd"] = "Infinity"
    invalid_economics_sample["economics"] = invalid_economics
    evidence["samples"] = [dict(invalid_economics_sample) for _ in range(3)]
    invalid_profit = _recent_shadow_preflight_evidence(
        route="polymarket_sx",
        app_config=config,
        runtime_snapshot=runtime_snapshot,
        eligible_markets_by_key={position_key(market): market},
        now=now,
        expected_release_sha="a" * 40,
    )
    assert invalid_profit["accepted"] is False
    assert "sample_1:economics_invalid" in invalid_profit["blockers"]


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("requested_contracts", "Infinity"),
        ("average_price", "NaN"),
        ("expected_fee_usd", "-0.01"),
    ),
)
def test_route_preview_economics_rejects_invalid_numeric_values(field: str, value: str) -> None:
    config = load_config(Path(__file__).parents[1] / "config.example.json")
    preview = {
        "requested_contracts": "20",
        "average_price": "0.45",
        "expected_fee_usd": "0.10",
    }
    invalid_preview = dict(preview)
    invalid_preview[field] = value

    economics, blockers = _route_preview_economics(
        "polymarket_sx",
        [{"preview": invalid_preview}, {"preview": preview}],
        config,
        Decimal("0.10"),
    )

    assert blockers == ("route_economics_unavailable",)
    assert economics is not None
    assert "error" in economics


@pytest.mark.asyncio
async def test_exact_paired_preview_rejects_minimum_notional_false_positive() -> None:
    import arbitrage_engine.production_audit as audit_module

    class _PreviewClient:
        def __init__(self, venue: str, minimum_notional: Decimal) -> None:
            self._venue = venue
            self._minimum_notional = minimum_notional

        async def preview_buy(
            self,
            token_id: str,
            side: BinarySide,
            contracts: Decimal,
            max_price: Decimal,
            **kwargs: object,
        ) -> OrderPreview:
            del kwargs
            notional = contracts * max_price
            blockers = ("minimum_notional_not_met",) if notional < self._minimum_notional else ()
            return OrderPreview(
                venue=self._venue,
                token_id=token_id,
                side=side,
                requested_contracts=contracts,
                limit_price=max_price,
                average_price=max_price,
                notional_usd=notional,
                available_depth_usd=Decimal("20"),
                price_impact_pct=Decimal(0),
                expected_fee_usd=Decimal(0),
                fee_quote=VenueFeeQuote(
                    self._venue,
                    0,
                    "zero_fee",
                    source="test_fixture",
                    verified=True,
                ),
                constraints=MarketConstraints(0, Decimal("0.01"), Decimal("0.01"), self._minimum_notional),
                signing_validated=True,
                payload_fingerprint=f"preview-{self._venue}",
                blockers=blockers,
            )

    screening_rows: list[dict[str, Any]] = [
        {
            "venue": "Polymarket",
            "token_id": "poly-token",
            "condition_id": "poly-condition",
            "constraints": {"tick_size": "0.01"},
            "preview": {"requested_contracts": "100", "limit_price": "0.10"},
        },
        {
            "venue": "SX Bet",
            "token_id": "sx-token",
            "condition_id": None,
            "constraints": {},
            "preview": {"requested_contracts": "12.5", "limit_price": "0.80"},
        },
    ]
    market = _sx_market(
        "Minimum notional regression",
        "sx-token",
        "sx-market",
        verified_routes=frozenset({"polymarket_sx"}),
    )

    rows, blockers = await audit_module._collect_exact_paired_previews(  # noqa: SLF001
        market=market,
        route="polymarket_sx",
        leg_rows=screening_rows,
        clients=cast(
            Any,
            {
                "Polymarket": _PreviewClient("Polymarket", Decimal("5")),
                "SX Bet": _PreviewClient("SX Bet", Decimal("1")),
            },
        ),
        leg_notional_usd=Decimal("10"),
        required_depth_usd=Decimal("12.50"),
        timeout_seconds=1.0,
    )

    assert "paired_preview:minimum_notional_not_met:Polymarket" in blockers
    assert rows[0]["paired_preview"]["requested_contracts"] == "12.5"
    assert rows[1]["paired_preview"]["requested_contracts"] == "12.5"


@pytest.mark.asyncio
async def test_exact_paired_preview_scopes_targets_and_uses_first_leg_predict_neg_risk() -> None:
    import arbitrage_engine.production_audit as audit_module

    class _PreviewClient:
        def __init__(self, venue: str, *, fail_prime: bool = False) -> None:
            self._venue = venue
            self._fail_prime = fail_prime
            self.target_history: list[set[str]] = []
            self.preview_kwargs: list[dict[str, object]] = []
            self.prime_count = 0

        def sync_market_data_targets(self, token_ids: set[str]) -> None:
            self.target_history.append(set(token_ids))

        async def prime_market_data_targets(self) -> None:
            self.prime_count += 1
            if self._fail_prime:
                raise RuntimeError("test prime failure")

        async def preview_buy(
            self,
            token_id: str,
            side: BinarySide,
            contracts: Decimal,
            max_price: Decimal,
            **kwargs: object,
        ) -> OrderPreview:
            self.preview_kwargs.append(dict(kwargs))
            return OrderPreview(
                venue=self._venue,
                token_id=token_id,
                side=side,
                requested_contracts=contracts,
                limit_price=max_price,
                average_price=Decimal("0.4"),
                notional_usd=contracts * Decimal("0.4"),
                available_depth_usd=Decimal("20"),
                price_impact_pct=Decimal(0),
                expected_fee_usd=Decimal(0),
                fee_quote=VenueFeeQuote(
                    self._venue,
                    0,
                    "zero_fee",
                    source="test_fixture",
                    verified=True,
                ),
                constraints=MarketConstraints(0, Decimal("0.01"), Decimal("0.01"), Decimal("1")),
                signing_validated=True,
                payload_fingerprint=f"preview-{self._venue}",
            )

    predict_client = _PreviewClient("Predict.fun")
    sx_client = _PreviewClient("SX Bet")
    market = replace(_predict_sx_market(), neg_risk=True, predict_fun_neg_risk=False)
    screening_rows = [
        {
            "venue": "Predict.fun",
            "token_id": market.polymarket_token_id,
            "condition_id": None,
            "constraints": {},
            "preview": {"requested_contracts": "20", "limit_price": "0.5"},
        },
        {
            "venue": "SX Bet",
            "token_id": market.predict_fun_token_id,
            "condition_id": None,
            "constraints": {},
            "preview": {"requested_contracts": "20", "limit_price": "0.5"},
        },
    ]

    _, blockers = await audit_module._collect_exact_paired_previews(  # noqa: SLF001
        market=market,
        route="predict_sx",
        leg_rows=screening_rows,
        clients=cast(Any, {"Predict.fun": predict_client, "SX Bet": sx_client}),
        leg_notional_usd=Decimal("10"),
        required_depth_usd=Decimal("12.50"),
        timeout_seconds=1.0,
    )

    assert blockers == ()
    assert predict_client.target_history == [{market.polymarket_token_id}, set()]
    assert sx_client.target_history == [{market.predict_fun_token_id}, set()]
    assert predict_client.prime_count == 1
    assert sx_client.prime_count == 1
    assert predict_client.preview_kwargs[0]["neg_risk"] is True
    assert sx_client.preview_kwargs[0]["neg_risk"] is None

    failing_predict = _PreviewClient("Predict.fun")
    failing_sx = _PreviewClient("SX Bet", fail_prime=True)
    _, prime_blockers = await audit_module._collect_exact_paired_previews(  # noqa: SLF001
        market=market,
        route="predict_sx",
        leg_rows=screening_rows,
        clients=cast(Any, {"Predict.fun": failing_predict, "SX Bet": failing_sx}),
        leg_notional_usd=Decimal("10"),
        required_depth_usd=Decimal("12.50"),
        timeout_seconds=1.0,
    )

    assert prime_blockers == ("paired_preview:prime_failed:SX Bet",)
    assert failing_predict.target_history[-1] == set()
    assert failing_sx.target_history[-1] == set()


@pytest.mark.asyncio
async def test_collect_all_market_audit_uses_exact_economics_after_negative_screening(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import arbitrage_engine.production_audit as audit_module

    base_config = load_config(Path(__file__).parents[1] / "config.example.json")
    config = replace(
        base_config,
        execution_mode=ExecutionMode.SHADOW,
        live_trading_confirmed=False,
        categories_to_scan=[],
        markets=[],
        enable_sx_bet=True,
        sx_bet=replace(base_config.sx_bet, enabled=True),
        routes=replace(
            base_config.routes,
            polymarket_myriad=False,
            polymarket_predict=False,
            predict_myriad=False,
            predict_sx=False,
            polymarket_sx=True,
            sx_myriad=False,
        ),
        spread_policy=replace(
            base_config.spread_policy,
            adverse_move_p95_pct_by_route={"polymarket_sx": 0.001},
            require_live_gas_estimate=False,
        ),
    )
    market = _sx_market(
        "Exact economics",
        "sx-exact-token",
        "sx-exact-market",
        verified_routes=frozenset({"polymarket_sx"}),
    )
    snapshot = RouteDiscoverySnapshot(
        enabled_routes=("polymarket_sx",),
        source_catalogs={},
        raw_route_candidates=(market,),
        route_candidates=(market,),
        category_markets=(market,),
        volume_markets=(market,),
        verified_markets=(market,),
        tradable_markets=(market,),
        missing_routes=(),
        diagnostics=DiscoveryDiagnostics(stages=(("tradable", 1),), rejection_reasons=()),
    )

    async def _fake_venue_balances(app_config: Any, runtime_snapshot: Any) -> dict[str, Any]:
        del app_config, runtime_snapshot
        return {
            "Polymarket": {"canary_gate": {"venue": "Polymarket", "passed": True}},
            "SX Bet": {"canary_gate": {"venue": "SX Bet", "passed": True}},
        }

    class _FakeClient:
        def __init__(self, *args: object, **kwargs: object) -> None:
            del args, kwargs

        def register_market(self, *args: object, **kwargs: object) -> None:
            del args, kwargs

        async def close(self) -> None:
            return None

    async def _screening_preview(**kwargs: Any) -> tuple[dict[str, Any], tuple[str, ...]]:
        del kwargs
        return (
            {
                "constraints": {},
                "samples": [{"ok": True}] * 3,
                "preview": {
                    "requested_contracts": "10",
                    "limit_price": "0.6",
                    "average_price": "0.6",
                    "expected_fee_usd": "0",
                },
            },
            (),
        )

    exact_calls = 0

    async def _exact_preview(**kwargs: Any) -> tuple[list[dict[str, Any]], tuple[str, ...]]:
        nonlocal exact_calls
        exact_calls += 1
        rows = [dict(row) for row in cast(list[dict[str, Any]], kwargs["leg_rows"])]
        for row in rows:
            row["preview"] = {
                "requested_contracts": "10",
                "limit_price": "0.4",
                "average_price": "0.4",
                "expected_fee_usd": "0",
            }
        return rows, ()

    monkeypatch.setattr(audit_module, "collect_venue_balance_audit", _fake_venue_balances)
    monkeypatch.setattr(audit_module, "PolymarketClobClient", _FakeClient)
    monkeypatch.setattr(audit_module, "create_sx_bet_client", _FakeClient)
    monkeypatch.setattr(audit_module, "_collect_leg_preview", _screening_preview)
    monkeypatch.setattr(audit_module, "_collect_exact_paired_previews", _exact_preview)

    report = await collect_all_market_audit(config, snapshot, runtime_snapshot={})

    assert exact_calls == 1
    assert report["route_summary"]["polymarket_sx"]["technical_openable_count"] == 1
    assert report["markets"][0]["paired_preview_status"] == "validated"
    assert report["markets"][0]["route_economics_basis"] == "exact_paired_preview"
    assert report["markets"][0]["route_economics"]["expected_profit_usd"] == "2.0"


@pytest.mark.asyncio
async def test_recent_shadow_evidence_is_reported_separately_from_current_openability(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import arbitrage_engine.production_audit as audit_module

    base_config = load_config(Path(__file__).parents[1] / "config.example.json")
    config = replace(
        base_config,
        execution_mode=ExecutionMode.SHADOW,
        live_trading_confirmed=False,
        categories_to_scan=[],
        markets=[],
        enable_sx_bet=True,
        sx_bet=replace(base_config.sx_bet, enabled=True),
        routes=replace(
            base_config.routes,
            polymarket_myriad=False,
            polymarket_sx=True,
        ),
        spread_policy=replace(
            base_config.spread_policy,
            adverse_move_p95_pct_by_route={"polymarket_sx": 0.001},
        ),
    )
    market = _sx_market(
        "Recent evidence",
        "sx-recent-token",
        "sx-recent-market",
        verified_routes=frozenset({"polymarket_sx"}),
    )
    snapshot = RouteDiscoverySnapshot(
        enabled_routes=("polymarket_sx",),
        source_catalogs={},
        raw_route_candidates=(market,),
        route_candidates=(market,),
        category_markets=(market,),
        volume_markets=(market,),
        verified_markets=(market,),
        tradable_markets=(market,),
        missing_routes=(),
        diagnostics=DiscoveryDiagnostics(stages=(("tradable", 1),), rejection_reasons=()),
    )

    async def _fake_balances(app_config: Any, runtime_snapshot: Any) -> dict[str, Any]:
        del app_config, runtime_snapshot
        return {
            venue: {
                "canary_gate": {
                    "venue": venue,
                    "passed": True,
                    "blocking_reasons": [],
                }
            }
            for venue in ("Polymarket", "SX Bet")
        }

    class _FakeClient:
        def __init__(self, *args: object, **kwargs: object) -> None:
            del args, kwargs

        def register_market(self, *args: object, **kwargs: object) -> None:
            del args, kwargs

        async def close(self) -> None:
            return None

    async def _blocked_preview(**kwargs: Any) -> tuple[dict[str, Any], tuple[str, ...]]:
        venue = str(kwargs["venue"])
        return {"samples": [], "preview": None}, (f"sample_1:asks_unavailable:{venue}",)

    monkeypatch.setattr(audit_module, "collect_venue_balance_audit", _fake_balances)
    monkeypatch.setattr(audit_module, "PolymarketClobClient", _FakeClient)
    monkeypatch.setattr(audit_module, "create_sx_bet_client", _FakeClient)
    monkeypatch.setattr(audit_module, "_collect_leg_preview", _blocked_preview)
    monkeypatch.setattr(
        audit_module,
        "_recent_shadow_preflight_evidence",
        lambda **kwargs: {
            "accepted": True,
            "blockers": [],
            "market_key": position_key(market),
        },
    )

    report = await collect_all_market_audit(config, snapshot, runtime_snapshot={})
    route = report["route_summary"]["polymarket_sx"]

    assert route["current_technical_openable_count"] == 0
    assert route["current_canary_openable_count"] == 0
    assert route["recent_technical_evidence_count"] == 1
    assert route["recent_canary_evidence_count"] == 0
    assert route["technical_openable_count"] == 0
    assert route["canary_openable_count"] == 0
    assert route["openable_count"] == 0


def _predict_myriad_market() -> MarketSpec:
    return MarketSpec(
        symbol="Predict-Myriad",
        target_label="YES",
        polymarket_token_id="predict-token",
        polymarket_side=BinarySide.NO,
        polymarket_market_id="predict-market",
        predict_fun_token_id="myriad-1335:YES",
        predict_fun_side=BinarySide.YES,
        predict_fun_market_id="predict-market",
        myriad_market_id="1335",
        myriad_side=BinarySide.NO,
        venue_b_label="Predict.fun",
        mapping_status=MappingStatus.VERIFIED,
        verified_routes=frozenset({"predict_myriad"}),
        rules_fingerprint="rules-predict-myriad",
        resolution_source="Official market resolution",
        outcome_semantics="YES is the stated outcome",
        category="sports",
    )


def _predict_sx_market() -> MarketSpec:
    return MarketSpec(
        symbol="Predict-SX",
        target_label="YES",
        polymarket_token_id="predict-sx-token",
        polymarket_side=BinarySide.NO,
        polymarket_market_id="predict-sx-market",
        predict_fun_token_id="sx-second-token",
        predict_fun_side=BinarySide.YES,
        predict_fun_market_id="sx-second-market",
        venue_a_label="Predict.fun",
        venue_b_label="SX Bet",
        mapping_status=MappingStatus.VERIFIED,
        verified_routes=frozenset({"predict_sx"}),
        rules_fingerprint="rules-predict-sx",
        resolution_source="Official event result",
        outcome_semantics="YES is the stated outcome",
        category="sports",
    )


def test_build_route_overlap_report_scopes_to_enabled_routes_and_unmatched_samples() -> None:
    matched = _sx_market(
        "Matched SX",
        "sx-token",
        "sx-market",
        verified_routes=frozenset({"polymarket_sx", "sx_myriad"}),
    )
    unmatched_source = MarketSpec(
        symbol="Unmatched SX",
        target_label="NO",
        polymarket_token_id="",
        polymarket_side=BinarySide.YES,
        predict_fun_token_id="sx-unmatched-token",
        predict_fun_side=BinarySide.NO,
        predict_fun_market_id="sx-unmatched-market",
        venue_b_label="SX Bet",
    )
    snapshot = RouteDiscoverySnapshot(
        enabled_routes=("polymarket_sx", "sx_myriad"),
        source_catalogs={"SX Bet": (matched, unmatched_source)},
        raw_route_candidates=(matched,),
        route_candidates=(matched,),
        category_markets=(matched,),
        volume_markets=(matched,),
        verified_markets=(matched,),
        tradable_markets=(matched,),
        missing_routes=(),
        diagnostics=DiscoveryDiagnostics(stages=(("tradable", 1),), rejection_reasons=()),
    )

    report = build_route_overlap_report(snapshot, unmatched_limit=5)

    assert set(report["routes"]) == {"polymarket_sx", "sx_myriad"}
    assert report["routes"]["polymarket_sx"]["verified_tradable_count"] == 1
    assert report["routes"]["sx_myriad"]["verified_tradable_count"] == 1
    assert report["routes"]["polymarket_sx"]["unmatched_samples"][0]["source_market_id"] == "sx-unmatched-market"
    assert report["routes"]["polymarket_sx"]["category_coverage"] == {
        "source_catalog": {"sports": 1, "unknown": 1},
        "discovered_candidates": {"sports": 1},
        "engine_safe_matched": {"sports": 1},
        "post_horizon_filter": {"sports": 1},
        "post_volume_filter": {"sports": 1},
        "verified_tradable": {"sports": 1},
    }
    volume_coverage = report["routes"]["polymarket_sx"]["volume_coverage"]
    assert volume_coverage["first_venue"] == "Polymarket"
    assert volume_coverage["second_venue"] == "SX Bet"
    sports_volume = volume_coverage["engine_safe_matched"]["sports"]
    assert sports_volume["market_pair_count"] == 1
    assert sports_volume["both_legs_reported_count"] == 1
    assert sports_volume["minimum_leg_volume_usd"]["median_usd"] == 90_000
    assert len(report["discovery_snapshot_id"]) == 64


def test_build_route_overlap_report_requires_route_specific_verification_for_verified_count() -> None:
    predict_only_candidate = MarketSpec(
        symbol="Shared market",
        target_label="YES",
        polymarket_token_id="poly-token",
        polymarket_side=BinarySide.YES,
        polymarket_market_id="poly-market",
        predict_fun_token_id="predict-token",
        predict_fun_side=BinarySide.NO,
        predict_fun_market_id="predict-market",
        myriad_market_id="1335",
        myriad_side=BinarySide.YES,
        venue_b_label="Predict.fun",
        mapping_status=MappingStatus.VERIFIED,
        verified_routes=frozenset({"predict_myriad"}),
        rules_fingerprint="rules-shared-market",
        resolution_source="Official market resolution",
        outcome_semantics="YES is the stated outcome",
        category="sports",
    )
    snapshot = RouteDiscoverySnapshot(
        enabled_routes=("polymarket_predict", "predict_myriad"),
        source_catalogs={"Predict.fun": (predict_only_candidate,)},
        raw_route_candidates=(predict_only_candidate,),
        route_candidates=(predict_only_candidate,),
        category_markets=(predict_only_candidate,),
        volume_markets=(predict_only_candidate,),
        verified_markets=(predict_only_candidate,),
        tradable_markets=(predict_only_candidate,),
        missing_routes=("polymarket_predict",),
        diagnostics=DiscoveryDiagnostics(stages=(("tradable", 1),), rejection_reasons=()),
    )

    report = build_route_overlap_report(snapshot)

    assert report["routes"]["polymarket_predict"]["engine_safe_matched_count"] == 1
    assert report["routes"]["polymarket_predict"]["verified_tradable_count"] == 0
    assert report["routes"]["polymarket_predict"]["missing_route"] is True
    assert report["routes"]["predict_myriad"]["verified_tradable_count"] == 1


def test_build_route_overlap_report_rejects_incomplete_verified_mapping() -> None:
    incomplete = replace(
        _sx_market(
            "Incomplete SX mapping",
            "sx-incomplete-token",
            "sx-incomplete-market",
            verified_routes=frozenset({"polymarket_sx"}),
        ),
        rules_fingerprint="",
    )
    snapshot = RouteDiscoverySnapshot(
        enabled_routes=("polymarket_sx",),
        source_catalogs={"SX Bet": (incomplete,)},
        raw_route_candidates=(incomplete,),
        route_candidates=(incomplete,),
        category_markets=(incomplete,),
        volume_markets=(incomplete,),
        verified_markets=(incomplete,),
        tradable_markets=(incomplete,),
        missing_routes=("polymarket_sx",),
        diagnostics=DiscoveryDiagnostics(stages=(("tradable", 1),), rejection_reasons=()),
    )

    report = build_route_overlap_report(snapshot)

    assert report["routes"]["polymarket_sx"]["engine_safe_matched_count"] == 1
    assert report["routes"]["polymarket_sx"]["verified_tradable_count"] == 0
    assert report["routes"]["polymarket_sx"]["category_coverage"]["verified_tradable"] == {}


def test_build_route_overlap_volume_coverage_deduplicates_complementary_specs() -> None:
    yes_market = _sx_market(
        "Shared event",
        "sx-yes-token",
        "sx-market",
        verified_routes=frozenset({"polymarket_sx"}),
    )
    no_market = replace(
        yes_market,
        target_label="NO",
        polymarket_token_id="poly-no-token",
        polymarket_side=BinarySide.NO,
        predict_fun_token_id="sx-no-token",
        predict_fun_side=BinarySide.YES,
    )
    markets = (yes_market, no_market)
    snapshot = RouteDiscoverySnapshot(
        enabled_routes=("polymarket_sx",),
        source_catalogs={"SX Bet": markets},
        raw_route_candidates=markets,
        route_candidates=markets,
        category_markets=markets,
        volume_markets=markets,
        verified_markets=markets,
        tradable_markets=markets,
        missing_routes=(),
        diagnostics=DiscoveryDiagnostics(stages=(("tradable", 2),), rejection_reasons=()),
    )

    report = build_route_overlap_report(snapshot)

    sports = report["routes"]["polymarket_sx"]["volume_coverage"]["verified_tradable"]["sports"]
    assert sports["market_pair_count"] == 1
    assert sports["first_leg_volume_usd"]["sum_usd"] == 120_000
    assert sports["second_leg_volume_usd"]["sum_usd"] == 90_000


@pytest.mark.asyncio
async def test_collect_all_market_audit_summarizes_openable_and_blocked_routes(monkeypatch: pytest.MonkeyPatch) -> None:
    import arbitrage_engine.production_audit as audit_module

    config = replace(
        load_config(Path(__file__).parents[1] / "config.example.json"),
        execution_mode=ExecutionMode.CANARY,
        categories_to_scan=[],
        markets=[],
        enable_predict_fun=True,
        enable_sx_bet=True,
        predict_fun=replace(
            load_config(Path(__file__).parents[1] / "config.example.json").predict_fun,
            enabled=True,
            api_key="predict-key",
        ),
        sx_bet=replace(load_config(Path(__file__).parents[1] / "config.example.json").sx_bet, enabled=True),
        myriad_markets=replace(
            load_config(Path(__file__).parents[1] / "config.example.json").myriad_markets,
            enabled=True,
        ),
        routes=replace(
            load_config(Path(__file__).parents[1] / "config.example.json").routes,
            polymarket_myriad=False,
            polymarket_predict=False,
            predict_myriad=True,
            predict_sx=True,
            polymarket_sx=True,
            sx_myriad=True,
        ),
    )
    config = replace(
        config,
        spread_policy=replace(
            config.spread_policy,
            adverse_move_p95_pct_by_route={route: 0.001 for route in (
                "polymarket_sx",
                "sx_myriad",
                "predict_myriad",
                "predict_sx",
            )},
        ),
    )
    sx_market = _sx_market(
        "SX + Myriad",
        "sx-openable-token",
        "sx-openable-market",
        verified_routes=frozenset({"polymarket_sx", "sx_myriad"}),
    )
    predict_myriad = _predict_myriad_market()
    predict_sx = _predict_sx_market()
    snapshot = RouteDiscoverySnapshot(
        enabled_routes=("polymarket_sx", "sx_myriad", "predict_myriad", "predict_sx"),
        source_catalogs={},
        raw_route_candidates=(sx_market, predict_myriad, predict_sx),
        route_candidates=(sx_market, predict_myriad, predict_sx),
        category_markets=(sx_market, predict_myriad, predict_sx),
        volume_markets=(sx_market, predict_myriad, predict_sx),
        verified_markets=(sx_market, predict_myriad, predict_sx),
        tradable_markets=(sx_market, predict_myriad, predict_sx),
        missing_routes=(),
        diagnostics=DiscoveryDiagnostics(stages=(("tradable", 3),), rejection_reasons=()),
    )

    async def _fake_venue_balances(app_config: Any, runtime_snapshot: Any) -> dict[str, Any]:
        del app_config, runtime_snapshot
        return {
            "Polymarket": {
                "canary_gate": {"venue": "Polymarket", "passed": False, "blocking_reasons": ["risk_paused"]}
            },
            "Predict.fun": {
                "canary_gate": {"venue": "Predict.fun", "passed": False, "blocking_reasons": ["risk_paused"]}
            },
            "SX Bet": {
                "canary_gate": {"venue": "SX Bet", "passed": False, "blocking_reasons": ["risk_paused"]}
            },
            "Myriad": {
                "canary_gate": {"venue": "Myriad", "passed": False, "blocking_reasons": ["risk_paused"]}
            },
        }

    class _FakeClient:
        def __init__(self, *args: object, **kwargs: object) -> None:  # noqa: D401
            del args, kwargs

        async def watch_order_book(self, token_id: str) -> SimpleNamespace:
            if token_id == "1335:NO":
                raise RuntimeError("myriad book unavailable")
            level = SimpleNamespace(price=0.45, size=100.0)
            return SimpleNamespace(bids=[level], asks=[level])

        async def get_market_constraints(
            self,
            token_id: str,
            condition_id: str | None = None,
        ) -> MarketConstraints:
            del token_id, condition_id
            return MarketConstraints(0, Decimal("0.01"), Decimal("0.01"), Decimal("1"))

        def is_order_book_execution_fresh(
            self,
            token_id: str,
            book: object,
            max_age_seconds: float,
        ) -> bool:
            del token_id, book, max_age_seconds
            return True

        async def preview_buy(
            self,
            token_id: str,
            side: BinarySide,
            contracts: Decimal,
            max_price: Decimal,
            **kwargs: object,
        ) -> OrderPreview:
            del kwargs
            fee_quote = VenueFeeQuote("Test", 0, "zero_fee", source="test_fixture", verified=True)
            return OrderPreview(
                venue="Test",
                token_id=token_id,
                side=side,
                requested_contracts=contracts,
                limit_price=max_price,
                average_price=Decimal("0.45"),
                notional_usd=contracts * Decimal("0.45"),
                available_depth_usd=Decimal("45"),
                price_impact_pct=Decimal(0),
                expected_fee_usd=Decimal(0),
                fee_quote=fee_quote,
                constraints=MarketConstraints(0, Decimal("0.01"), Decimal("0.01"), Decimal("1")),
                signing_validated=True,
                payload_fingerprint="test-preview",
            )

        async def close(self) -> None:
            return None

        def register_market(self, *args: object, **kwargs: object) -> None:
            del args, kwargs

    monkeypatch.setattr(audit_module, "collect_venue_balance_audit", _fake_venue_balances)
    monkeypatch.setattr(audit_module, "PolymarketClobClient", _FakeClient)
    monkeypatch.setattr(audit_module, "PredictFunApiClient", _FakeClient)
    monkeypatch.setattr(audit_module, "create_sx_bet_client", _FakeClient)
    monkeypatch.setattr(audit_module, "MyriadClient", _FakeClient)

    report = await collect_all_market_audit(config, snapshot, runtime_snapshot={})

    assert report["discovery_snapshot_id"] == build_route_overlap_report(snapshot)["discovery_snapshot_id"]
    assert report["openability_model"] == "technical_and_canary_v4"
    for route in ("polymarket_sx", "sx_myriad", "predict_sx"):
        assert report["route_summary"][route]["technical_openable_count"] == 1
        assert report["route_summary"][route]["canary_openable_count"] == 0
        assert report["route_summary"][route]["openable_count"] == 0
    assert report["route_summary"]["predict_myriad"]["technical_openable_count"] == 0
    assert report["route_summary"]["predict_myriad"]["canary_openable_count"] == 0
    assert report["route_summary"]["predict_myriad"]["openable_count"] == 0
    assert report["route_summary"]["polymarket_sx"]["category_summary"] == {
        "sports": {
            "market_count": 2,
            "verified_count": 1,
            "technical_openable_count": 1,
            "economically_openable_count": 1,
            "canary_openable_count": 0,
            "openable_count": 0,
            "recent_technical_evidence_count": 0,
        }
    }
    assert any(
        "orderbook_unavailable:Myriad" in item["blocker"]
        for item in report["route_summary"]["predict_myriad"]["technical_blocker_samples"]
    )
    technically_openable = next(
        row
        for row in report["markets"]
        if row["route"] == "polymarket_sx" and row["technical_preview_feasible"]
    )
    assert technically_openable["canary_preview_feasible"] is False
    assert "live_trading_confirmation_missing" in technically_openable["canary_preview_blockers"]
    assert technically_openable["technical_preview_blockers"] == []
    assert technically_openable["economically_openable"] is True
    assert technically_openable["economic_preview_blockers"] == []


@pytest.mark.asyncio
async def test_collect_all_market_audit_uses_verified_route_state_from_verified_snapshot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import arbitrage_engine.production_audit as audit_module

    base_config = load_config(Path(__file__).parents[1] / "config.example.json")
    config = replace(
        base_config,
        execution_mode=ExecutionMode.CANARY,
        live_trading_confirmed=True,
        categories_to_scan=[],
        markets=[],
        myriad_markets=replace(base_config.myriad_markets, enabled=True),
        routes=replace(
            base_config.routes,
            polymarket_myriad=True,
            polymarket_predict=False,
            predict_myriad=False,
            predict_sx=False,
            polymarket_sx=False,
            sx_myriad=False,
        ),
    )
    config = replace(
        config,
        spread_policy=replace(
            config.spread_policy,
            adverse_move_p95_pct_by_route={"polymarket_myriad": 0.001},
        ),
    )
    candidate = MarketSpec(
        symbol="Poly-Myriad Candidate",
        target_label="YES",
        polymarket_token_id="poly-token",
        polymarket_side=BinarySide.YES,
        polymarket_market_id="poly-market",
        condition_id="condition-poly",
        predict_fun_token_id="",
        predict_fun_side=BinarySide.NO,
        myriad_market_id="410",
        myriad_side=BinarySide.NO,
        venue_b_label="Myriad",
        mapping_status=MappingStatus.CANDIDATE,
        verified_routes=frozenset(),
        rules_fingerprint="rules-poly-myriad",
        resolution_source="Official market resolution",
        outcome_semantics="YES is the stated outcome",
        category="sports",
    )
    verified = replace(
        candidate,
        mapping_status=MappingStatus.VERIFIED,
        verified_routes=frozenset({"polymarket_myriad"}),
    )
    snapshot = RouteDiscoverySnapshot(
        enabled_routes=("polymarket_myriad",),
        source_catalogs={},
        raw_route_candidates=(candidate,),
        route_candidates=(candidate,),
        category_markets=(candidate,),
        volume_markets=(candidate,),
        verified_markets=(verified,),
        tradable_markets=(verified,),
        missing_routes=(),
        diagnostics=DiscoveryDiagnostics(stages=(("tradable", 1),), rejection_reasons=()),
    )

    async def _fake_venue_balances(app_config: Any, runtime_snapshot: Any) -> dict[str, Any]:
        del app_config, runtime_snapshot
        return {
            "Polymarket": {"canary_gate": {"venue": "Polymarket", "passed": True, "blocking_reasons": []}},
            "Myriad": {"canary_gate": {"venue": "Myriad", "passed": True, "blocking_reasons": []}},
        }

    class _FakeClient:
        def __init__(self, *args: object, **kwargs: object) -> None:
            del args, kwargs

        async def watch_order_book(self, token_id: str) -> SimpleNamespace:
            del token_id
            level = SimpleNamespace(price=0.45, size=100.0)
            return SimpleNamespace(bids=[level], asks=[level])

        async def get_market_constraints(
            self,
            token_id: str,
            condition_id: str | None = None,
        ) -> MarketConstraints:
            del token_id, condition_id
            return MarketConstraints(0, Decimal("0.01"), Decimal("0.01"), Decimal("1"))

        def is_order_book_execution_fresh(
            self,
            token_id: str,
            book: object,
            max_age_seconds: float,
        ) -> bool:
            del token_id, book, max_age_seconds
            return True

        async def preview_buy(
            self,
            token_id: str,
            side: BinarySide,
            contracts: Decimal,
            max_price: Decimal,
            **kwargs: object,
        ) -> OrderPreview:
            del kwargs
            fee_quote = VenueFeeQuote("Test", 0, "zero_fee", source="test_fixture", verified=True)
            return OrderPreview(
                venue="Test",
                token_id=token_id,
                side=side,
                requested_contracts=contracts,
                limit_price=max_price,
                average_price=Decimal("0.45"),
                notional_usd=contracts * Decimal("0.45"),
                available_depth_usd=Decimal("45"),
                price_impact_pct=Decimal(0),
                expected_fee_usd=Decimal(0),
                fee_quote=fee_quote,
                constraints=MarketConstraints(0, Decimal("0.01"), Decimal("0.01"), Decimal("1")),
                signing_validated=True,
                payload_fingerprint="test-preview",
            )

        async def close(self) -> None:
            return None

    monkeypatch.setattr(audit_module, "collect_venue_balance_audit", _fake_venue_balances)
    monkeypatch.setattr(audit_module, "PolymarketClobClient", _FakeClient)
    monkeypatch.setattr(audit_module, "MyriadClient", _FakeClient)

    report = await collect_all_market_audit(config, snapshot, runtime_snapshot={})

    assert report["route_summary"]["polymarket_myriad"]["verified_count"] == 1
    assert report["route_summary"]["polymarket_myriad"]["technical_openable_count"] == 1
    assert report["route_summary"]["polymarket_myriad"]["canary_openable_count"] == 1
    assert report["route_summary"]["polymarket_myriad"]["openable_count"] == 1
    assert report["markets"][0]["technical_preview_feasible"] is True
    assert report["markets"][0]["canary_preview_feasible"] is True
    assert report["markets"][0]["preview_feasible"] is True
    assert report["markets"][0]["verified_routes"] == ["polymarket_myriad"]
    assert report["markets"][0]["canonical_identity"]["category"] == "sports"


@pytest.mark.asyncio
async def test_collect_all_market_audit_bounds_preview_concurrency_and_skips_unverified_signing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import arbitrage_engine.production_audit as audit_module

    base_config = load_config(Path(__file__).parents[1] / "config.example.json")
    config = replace(
        base_config,
        execution_mode=ExecutionMode.SHADOW,
        categories_to_scan=[],
        markets=[],
        enable_sx_bet=True,
        sx_bet=replace(base_config.sx_bet, enabled=True),
        routes=replace(
            base_config.routes,
            polymarket_myriad=False,
            polymarket_predict=False,
            predict_myriad=False,
            predict_sx=False,
            polymarket_sx=True,
            sx_myriad=False,
        ),
        spread_policy=replace(
            base_config.spread_policy,
            adverse_move_p95_pct_by_route={"polymarket_sx": 0.001},
        ),
    )
    verified = tuple(
        _sx_market(
            f"Verified {index}",
            f"sx-token-{index}",
            f"sx-market-{index}",
            verified_routes=frozenset({"polymarket_sx"}),
        )
        for index in range(100)
    )
    candidate = _sx_market(
        "Candidate",
        "sx-token-candidate",
        "sx-market-candidate",
        verified_routes=frozenset(),
    )
    incomplete_verified = replace(
        _sx_market(
            "Incomplete verified",
            "sx-token-incomplete",
            "sx-market-incomplete",
            verified_routes=frozenset({"polymarket_sx"}),
        ),
        outcome_semantics="",
    )
    all_markets = (*verified, candidate, incomplete_verified)
    snapshot = RouteDiscoverySnapshot(
        enabled_routes=("polymarket_sx",),
        source_catalogs={},
        raw_route_candidates=all_markets,
        route_candidates=all_markets,
        category_markets=all_markets,
        volume_markets=all_markets,
        verified_markets=(*verified, incomplete_verified),
        tradable_markets=(*verified, incomplete_verified),
        missing_routes=(),
        diagnostics=DiscoveryDiagnostics(stages=(("tradable", len(verified)),), rejection_reasons=()),
    )

    async def _fake_venue_balances(app_config: Any, runtime_snapshot: Any) -> dict[str, Any]:
        del app_config, runtime_snapshot
        return {
            "Polymarket": {"canary_gate": {"venue": "Polymarket", "passed": True}},
            "SX Bet": {"canary_gate": {"venue": "SX Bet", "passed": True}},
        }

    class _FakeClient:
        def __init__(self, *args: object, **kwargs: object) -> None:
            del args, kwargs
            self._target_count = 0

        def register_market(self, *args: object, **kwargs: object) -> None:
            del args, kwargs

        def sync_market_data_targets(self, token_ids: set[str]) -> None:
            synced_targets.update(token_ids)
            synced_windows.append(set(token_ids))
            self._target_count = len(token_ids)

        async def prime_market_data_targets(self) -> None:
            prime_window_sizes.append(self._target_count)

        async def preview_buy(
            self,
            token_id: str,
            side: BinarySide,
            contracts: Decimal,
            max_price: Decimal,
            **kwargs: object,
        ) -> OrderPreview:
            del kwargs
            fee_quote = VenueFeeQuote("Test", 0, "zero_fee", source="test_fixture", verified=True)
            return OrderPreview(
                venue="Test",
                token_id=token_id,
                side=side,
                requested_contracts=contracts,
                limit_price=max_price,
                average_price=Decimal("0.4"),
                notional_usd=contracts * Decimal("0.4"),
                available_depth_usd=Decimal("50"),
                price_impact_pct=Decimal(0),
                expected_fee_usd=Decimal(0),
                fee_quote=fee_quote,
                constraints=MarketConstraints(0, Decimal("0.01"), Decimal("0.01"), Decimal("1")),
                signing_validated=True,
                payload_fingerprint=f"preview-{token_id}",
            )

        async def close(self) -> None:
            return None

    in_flight = 0
    max_in_flight = 0
    signed_tokens: list[str] = []
    synced_targets: set[str] = set()
    synced_windows: list[set[str]] = []
    prime_window_sizes: list[int] = []

    async def _fake_collect_leg_preview(**kwargs: Any) -> tuple[dict[str, Any], tuple[str, ...]]:
        nonlocal in_flight, max_in_flight
        token_id = str(kwargs["token_id"])
        signed_tokens.append(token_id)
        in_flight += 1
        max_in_flight = max(max_in_flight, in_flight)
        await asyncio.sleep(0.01)
        in_flight -= 1
        return (
            {
                "constraints": {},
                "samples": [{"sample": index, "ok": True} for index in range(1, 4)],
                "preview": {
                    "requested_contracts": "25",
                    "limit_price": "0.4",
                    "average_price": "0.4",
                    "expected_fee_usd": "0",
                },
            },
            (),
        )

    monkeypatch.setattr(audit_module, "collect_venue_balance_audit", _fake_venue_balances)
    monkeypatch.setattr(audit_module, "PolymarketClobClient", _FakeClient)
    monkeypatch.setattr(audit_module, "create_sx_bet_client", _FakeClient)
    monkeypatch.setattr(audit_module, "_collect_leg_preview", _fake_collect_leg_preview)

    report = await collect_all_market_audit(
        config,
        snapshot,
        runtime_snapshot={},
        max_preview_concurrency=2,
        max_preview_concurrency_per_venue=2,
    )

    assert max_in_flight == 2
    assert report["preview_policy"] == {
        "global_concurrency": 2,
        "per_venue_concurrency": 2,
        "target_window_size": 100,
        "worker_count": 2,
        "unique_preview_count": 200,
        "consecutive_samples_required": 3,
        "exact_paired_preview_required_for_openable": True,
    }
    non_empty_windows = [window for window in synced_windows if window]
    screening_windows = [window for window in non_empty_windows if len(window) > 1]
    exact_windows = [window for window in non_empty_windows if len(window) == 1]
    assert max(len(window) for window in screening_windows) == 50
    assert len(screening_windows) == 4
    assert len(exact_windows) == 200
    assert all(
        sum(len(window) for window in screening_windows[start : start + 2]) == 100
        for start in range(0, len(screening_windows), 2)
    )
    assert prime_window_sizes
    assert 0 < max(prime_window_sizes) <= 100
    assert len(signed_tokens) == 200
    assert all("candidate" not in token for token in signed_tokens)
    assert synced_targets == set(signed_tokens)
    assert synced_windows[-2:] == [set(), set()]
    assert report["route_summary"]["polymarket_sx"]["market_count"] == 102
    assert report["route_summary"]["polymarket_sx"]["verified_count"] == 100
    assert report["route_summary"]["polymarket_sx"]["technical_openable_count"] == 100
    assert report["route_summary"]["polymarket_sx"]["canary_openable_count"] == 0
    assert report["route_summary"]["polymarket_sx"]["openable_count"] == 0
    candidate_row = next(row for row in report["markets"] if row["mapping_status"] == "CANDIDATE")
    assert "route_not_execution_eligible" in candidate_row["technical_preview_blockers"]
    assert candidate_row["first_leg"]["samples"] == []
    incomplete_row = next(
        row for row in report["markets"] if row["canonical_identity"]["symbol"] == "Incomplete verified"
    )
    assert "route_not_execution_eligible" in incomplete_row["technical_preview_blockers"]
    assert incomplete_row["first_leg"]["samples"] == []


@pytest.mark.asyncio
async def test_collect_leg_preview_does_not_misclassify_preflight_reject_as_signing_failure() -> None:
    import arbitrage_engine.production_audit as audit_module

    constraints = MarketConstraints(0, Decimal("0.01"), Decimal("0.01"), Decimal("1"))
    preview_blockers: tuple[str, ...] = ("insufficient_executable_depth",)
    preview_depth = Decimal("0.50")
    level = SimpleNamespace(price=Decimal("0.50"), size=Decimal("1"))
    book = SimpleNamespace(
        bids=[level],
        asks=[level],
        status=SimpleNamespace(value="VALID"),
    )

    class _BlockedClient:
        async def get_market_constraints(
            self,
            token_id: str,
            condition_id: str | None = None,
        ) -> MarketConstraints:
            del token_id, condition_id
            return constraints

        async def watch_order_book(self, token_id: str) -> SimpleNamespace:
            del token_id
            return book

        def is_order_book_execution_fresh(
            self,
            token_id: str,
            watched_book: object,
            max_age_seconds: float,
        ) -> bool:
            del token_id, watched_book, max_age_seconds
            return True

        async def preview_buy(
            self,
            token_id: str,
            side: BinarySide,
            contracts: Decimal,
            max_price: Decimal,
            **kwargs: object,
        ) -> OrderPreview:
            del kwargs
            return OrderPreview(
                venue="Polymarket",
                token_id=token_id,
                side=side,
                requested_contracts=contracts,
                limit_price=max_price,
                average_price=Decimal("0.50"),
                notional_usd=Decimal("0.50"),
                available_depth_usd=preview_depth,
                price_impact_pct=Decimal(0),
                expected_fee_usd=Decimal(0),
                fee_quote=VenueFeeQuote(
                    "Polymarket",
                    0,
                    "zero_fee",
                    source="test_fixture",
                    verified=True,
                ),
                constraints=constraints,
                signing_validated=False,
                payload_fingerprint=None,
                blockers=preview_blockers,
            )

    _, blockers = await audit_module._collect_leg_preview(
        client=cast(Any, _BlockedClient()),
        venue="Polymarket",
        token_id="poly-token",
        side=BinarySide.YES,
        condition_id="condition-poly",
        leg_notional_usd=Decimal("10"),
        required_depth_usd=Decimal("12.50"),
        max_price_impact=Decimal("0.01"),
        max_orderbook_age_seconds=5.0,
    )

    assert "preview:insufficient_executable_depth:Polymarket" in blockers
    assert "signature_preview_unavailable:Polymarket" not in blockers

    preview_blockers = ()
    preview_depth = Decimal("50")
    deep_level = SimpleNamespace(price=Decimal("0.50"), size=Decimal("100"))
    book.bids = [deep_level]
    book.asks = [deep_level]
    _, signing_blockers = await audit_module._collect_leg_preview(
        client=cast(Any, _BlockedClient()),
        venue="Polymarket",
        token_id="poly-token",
        side=BinarySide.YES,
        condition_id="condition-poly",
        leg_notional_usd=Decimal("10"),
        required_depth_usd=Decimal("12.50"),
        max_price_impact=Decimal("0.01"),
        max_orderbook_age_seconds=5.0,
    )

    assert "signature_preview_unavailable:Polymarket" in signing_blockers


@pytest.mark.asyncio
async def test_resolve_route_discovery_snapshot_preserves_myriad_settlement_metadata_from_resolver(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import arbitrage_engine.production_audit as audit_module

    expiry = datetime.now(UTC) + timedelta(days=14)
    base_config = load_config(Path(__file__).parents[1] / "config.example.json")
    config = replace(
        base_config,
        scan_all=True,
        execution_mode=ExecutionMode.CANARY,
        categories_to_scan=[],
        markets=[],
        myriad_markets=replace(base_config.myriad_markets, enabled=True),
        routes=replace(
            base_config.routes,
            polymarket_myriad=True,
            polymarket_predict=False,
            predict_myriad=False,
            predict_sx=False,
            polymarket_sx=False,
            sx_myriad=False,
        ),
    )

    myriad_seed = MarketSpec(
        symbol="Poly-Myriad Rich",
        target_label="YES",
        polymarket_token_id="",
        polymarket_side=BinarySide.YES,
        polymarket_market_id="",
        condition_id=None,
        predict_fun_token_id="",
        predict_fun_side=BinarySide.NO,
        venue_b_label="Myriad",
        expires_at=expiry,
        category="finance",
    )

    class _FakeGamma:
        def __init__(self, scan_all: bool = True, sports_horizon_hours: float = 200.0) -> None:
            del scan_all
            assert sports_horizon_hours == config.max_sports_market_horizon_hours
            self.catalog_size = 1
            self.last_resolution_stats = SimpleNamespace(
                exact_id_matches=1,
                exact_title_matches=0,
                semantic_matches=0,
                rejection_reasons={},
            )

        async def bootstrap(self, markets) -> None:  # type: ignore[no-untyped-def]
            del markets

        async def resolve(self, markets):  # type: ignore[no-untyped-def]
            return [
                replace(
                    market,
                    polymarket_token_id="poly-token",
                    polymarket_market_id="poly-market",
                    condition_id="condition-poly",
                    venue_a_label="Polymarket",
                    venue_b_label="Myriad",
                    polymarket_volume_usd=120_000,
                )
                for market in markets
            ]

        async def close(self) -> None:
            return None

    class _FakeCatalog:
        def __init__(self, markets) -> None:  # type: ignore[no-untyped-def]
            self._markets = list(markets)
            self.last_catalog_counts = (len(self._markets), len(self._markets))

        def invalidate_cache(self) -> None:
            return None

        async def resolve(self, markets):  # type: ignore[no-untyped-def]
            if not markets:
                return list(self._markets)
            return list(markets)

        async def close(self) -> None:
            return None

    class _FakeMyriadResolver(_FakeCatalog):
        async def resolve(self, markets):  # type: ignore[no-untyped-def]
            if not markets:
                return list(self._markets)
            return [
                replace(
                    market,
                    myriad_market_id="410",
                    myriad_condition_id="condition-410",
                    myriad_collateral_token="USD1",
                    myriad_side=BinarySide.NO,
                    myriad_volume_usd=90_000,
                )
                for market in markets
            ]

    class _FakeRepository:
        async def upsert_market_candidates(self, markets) -> None:  # type: ignore[no-untyped-def]
            del markets

        async def apply_verified_mappings(self, markets):  # type: ignore[no-untyped-def]
            return [
                replace(
                    market,
                    myriad_condition_id=None,
                    myriad_collateral_token=None,
                    mapping_status=MappingStatus.VERIFIED,
                    verified_routes=frozenset({"polymarket_myriad"}),
                    rules_fingerprint="rules-poly-myriad-rich",
                    resolution_source="Official market resolution",
                    outcome_semantics="YES is the stated outcome",
                )
                for market in markets
            ]

    monkeypatch.setattr(audit_module, "GammaMarketResolver", _FakeGamma)
    monkeypatch.setattr(audit_module, "PredictFunMarketResolver", lambda *args, **kwargs: _FakeCatalog([]))
    monkeypatch.setattr(audit_module, "SxBetMarketResolver", lambda *args, **kwargs: _FakeCatalog([]))
    myriad_instances: list[_FakeMyriadResolver] = []

    def _myriad_factory(*args: object, **kwargs: object) -> _FakeMyriadResolver:
        del args, kwargs
        instance = _FakeMyriadResolver([myriad_seed])
        myriad_instances.append(instance)
        return instance

    monkeypatch.setattr(audit_module, "MyriadMarketResolver", _myriad_factory)

    snapshot = await resolve_route_discovery_snapshot(
        config,
        cast(ProductionRepository, _FakeRepository()),
    )

    assert snapshot.tradable_markets[0].myriad_condition_id == "condition-410"
    assert snapshot.tradable_markets[0].myriad_collateral_token == "USD1"
    assert len(myriad_instances) == 1


def test_route_candidates_preserve_predict_and_myriad_for_same_polymarket_token() -> None:
    import arbitrage_engine.production_audit as audit_module

    expiry = datetime.now(UTC) + timedelta(hours=2)
    predict = MarketSpec(
        symbol="Will BTC finish above 100000?",
        target_label="YES",
        polymarket_token_id="poly-yes",
        polymarket_side=BinarySide.YES,
        polymarket_market_id="poly-market",
        expires_at=expiry,
        predict_fun_token_id="predict-no",
        predict_fun_side=BinarySide.NO,
        predict_fun_market_id="predict-market",
        venue_b_label="Predict.fun",
    )
    myriad = replace(
        predict,
        predict_fun_token_id="",
        predict_fun_market_id=None,
        myriad_market_id="myriad-market",
        myriad_side=BinarySide.NO,
        venue_b_label="Myriad",
    )

    raw, deduplicated = audit_module._build_route_candidates([predict, myriad])

    assert len(raw) == 2
    assert {market.venue_b_label for market in deduplicated} == {"Predict.fun", "Myriad"}


@pytest.mark.asyncio
async def test_gamma_audit_bootstrap_retries_transient_full_catalog_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import arbitrage_engine.production_audit as audit_module

    class _FlakyGamma:
        def __init__(self) -> None:
            self.attempts = 0

        async def bootstrap(self, markets: list[MarketSpec]) -> None:
            del markets
            self.attempts += 1
            if self.attempts < 3:
                raise GammaCacheUnavailable("transient")

    sleep = AsyncMock()
    monkeypatch.setattr("arbitrage_engine.production_audit.asyncio.sleep", sleep)
    resolver = _FlakyGamma()

    await audit_module._bootstrap_gamma_for_audit(  # noqa: SLF001
        cast(Any, resolver),
        [],
    )

    assert resolver.attempts == 3
    assert [call.args[0] for call in sleep.await_args_list] == [10.0, 30.0]


@pytest.mark.asyncio
async def test_gamma_audit_bootstrap_fails_closed_after_three_attempts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import arbitrage_engine.production_audit as audit_module

    class _FailingGamma:
        def __init__(self) -> None:
            self.attempts = 0

        async def bootstrap(self, markets: list[MarketSpec]) -> None:
            del markets
            self.attempts += 1
            raise GammaCacheUnavailable("persistent")

    monkeypatch.setattr("arbitrage_engine.production_audit.asyncio.sleep", AsyncMock())
    resolver = _FailingGamma()

    with pytest.raises(GammaCacheUnavailable, match="persistent"):
        await audit_module._bootstrap_gamma_for_audit(  # noqa: SLF001
            cast(Any, resolver),
            [],
        )

    assert resolver.attempts == 3


@pytest.mark.asyncio
async def test_resolve_route_discovery_snapshot_aligns_overlap_with_engine_pipeline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import arbitrage_engine.production_audit as audit_module

    expiry = datetime.now(UTC) + timedelta(hours=23)
    far_expiry = datetime.now(UTC) + timedelta(days=30)
    base_config = load_config(Path(__file__).parents[1] / "config.example.json")
    config = replace(
        base_config,
        scan_all=True,
        categories_to_scan=["crypto"],
        market_horizon_filter_enabled=True,
        execution_mode=ExecutionMode.CANARY,
        markets=[],
        enable_predict_fun=True,
        predict_fun=replace(base_config.predict_fun, enabled=True, api_key="predict-key"),
        myriad_markets=replace(base_config.myriad_markets, enabled=True),
        routes=replace(
            base_config.routes,
            polymarket_myriad=False,
            polymarket_predict=True,
            predict_myriad=True,
            predict_sx=False,
            polymarket_sx=False,
            sx_myriad=False,
        ),
    )

    predict_seed = MarketSpec(
        symbol="Will BTC exceed 100000?",
        target_label="YES",
        polymarket_token_id="",
        polymarket_side=BinarySide.YES,
        predict_fun_token_id="predict-token",
        predict_fun_side=BinarySide.NO,
        predict_fun_market_id="predict-market",
        venue_b_label="Predict.fun",
        expires_at=expiry,
        category="finance",
    )
    far_predict_seed = replace(
        predict_seed,
        symbol="Will BTC exceed 200000?",
        predict_fun_token_id="far-predict-token",
        predict_fun_market_id="far-predict-market",
        expires_at=far_expiry,
    )

    class _FakeGamma:
        def __init__(self, scan_all: bool = True, sports_horizon_hours: float = 200.0) -> None:
            del scan_all
            assert sports_horizon_hours == config.max_sports_market_horizon_hours
            self.catalog_size = 1
            self.last_resolution_stats = SimpleNamespace(
                exact_id_matches=1,
                exact_title_matches=0,
                semantic_matches=0,
                rejection_reasons={},
            )

        async def bootstrap(self, markets) -> None:  # type: ignore[no-untyped-def]
            del markets

        async def resolve(self, markets):  # type: ignore[no-untyped-def]
            return [
                replace(
                    market,
                    polymarket_token_id=f"poly-{market.predict_fun_token_id}",
                    polymarket_market_id=f"poly-{market.predict_fun_market_id}",
                    condition_id=f"condition-{market.predict_fun_market_id}",
                    polymarket_side=BinarySide.YES,
                    venue_a_label="Polymarket",
                    venue_b_label="Predict.fun",
                    predict_fun_volume_usd=100_000,
                    polymarket_volume_usd=120_000,
                )
                for market in markets
            ]

        async def close(self) -> None:
            return None

    class _FakeCatalog:
        def __init__(self, markets) -> None:  # type: ignore[no-untyped-def]
            self._markets = list(markets)
            self.last_catalog_counts = (len(self._markets), len(self._markets))

        def invalidate_cache(self) -> None:
            return None

        async def resolve(self, markets):  # type: ignore[no-untyped-def]
            if not markets:
                return list(self._markets)
            return list(markets)

        async def close(self) -> None:
            return None

    class _FakeMyriadResolver(_FakeCatalog):
        async def resolve(self, markets):  # type: ignore[no-untyped-def]
            if not markets:
                return list(self._markets)
            return [
                replace(
                    market,
                    myriad_market_id=f"myriad-{market.predict_fun_market_id}",
                    myriad_side=BinarySide.NO,
                    myriad_volume_usd=90_000,
                )
                for market in markets
            ]

    class _FakeRepository:
        async def upsert_market_candidates(self, markets) -> None:  # type: ignore[no-untyped-def]
            del markets

        async def apply_verified_mappings(self, markets):  # type: ignore[no-untyped-def]
            verified = [
                replace(
                    market,
                    mapping_status=MappingStatus.VERIFIED,
                    verified_routes=frozenset({"polymarket_predict", "predict_myriad"}),
                    rules_fingerprint="rules-btc",
                    resolution_source="Official market resolution",
                    outcome_semantics="YES is the stated outcome",
                )
                for market in markets
            ]
            assert len(verified) == 1
            return [
                verified[0],
                replace(
                    verified[0],
                    symbol="Incomplete verified mapping",
                    rules_fingerprint="",
                ),
                replace(
                    verified[0],
                    symbol="Disabled-route verified mapping",
                    venue_b_label="SX Bet",
                    verified_routes=frozenset({"polymarket_sx"}),
                ),
            ]

    monkeypatch.setattr(audit_module, "GammaMarketResolver", _FakeGamma)
    monkeypatch.setattr(
        audit_module,
        "PredictFunMarketResolver",
        lambda *args, **kwargs: _FakeCatalog([predict_seed, far_predict_seed]),
    )
    monkeypatch.setattr(audit_module, "SxBetMarketResolver", lambda *args, **kwargs: _FakeCatalog([]))
    monkeypatch.setattr(audit_module, "MyriadMarketResolver", lambda *args, **kwargs: _FakeMyriadResolver([]))

    snapshot = await resolve_route_discovery_snapshot(
        config,
        cast(ProductionRepository, _FakeRepository()),
    )
    report = build_route_overlap_report(snapshot)

    assert report["routes"]["polymarket_predict"]["engine_safe_matched_count"] == 2
    assert report["routes"]["polymarket_predict"]["post_horizon_filter_count"] == 1
    assert report["routes"]["polymarket_predict"]["verified_tradable_count"] == 1
    assert report["routes"]["predict_myriad"]["engine_safe_matched_count"] == 2
    assert report["routes"]["predict_myriad"]["post_horizon_filter_count"] == 1
    assert report["routes"]["predict_myriad"]["verified_tradable_count"] == 1
    assert report["diagnostics"]["stages"]["cross_venue_candidates"] == 2
    assert report["diagnostics"]["stages"]["horizon_accepted"] == 1
    assert report["diagnostics"]["stages"]["verified_mapping_markets"] == 1
    assert report["diagnostics"]["rejection_reasons"]["horizon_rejected"] == 1


def test_live_window_has_real_order_evidence_requires_true_marker() -> None:
    assert live_window_has_real_order_evidence({"observed_real_fill_or_open_position": True}) is True
    assert live_window_has_real_order_evidence({"observed_real_fill_or_open_position": False}) is False


def test_live_window_has_real_order_evidence_accepts_real_fill_or_position_counts() -> None:
    assert live_window_has_real_order_evidence({"real_recent_fill_count": 1}) is True
    assert live_window_has_real_order_evidence({"real_open_position_count": 1}) is True


def test_live_window_evidence_is_route_specific() -> None:
    report = {
        "route_evidence": {
            "polymarket_predict": {"has_live_evidence": True},
            "polymarket_myriad": {"has_live_evidence": False},
        }
    }

    assert live_window_has_real_order_evidence(report, "polymarket_predict") is True
    assert live_window_has_real_order_evidence(report, "polymarket_myriad") is False


def test_live_window_rejects_evidence_from_incomplete_monitoring_window() -> None:
    report = {
        "route_evidence": {"polymarket_predict": {"has_live_evidence": True}},
        "monitoring_continuity": {"passed": False},
        "final_database_snapshot_ok": True,
    }

    assert live_window_has_real_order_evidence(report, "polymarket_predict") is False


def test_live_window_rejects_evidence_from_early_window_exit() -> None:
    report = {
        "route_evidence": {"polymarket_predict": {"has_live_evidence": True}},
        "monitoring_continuity": {"passed": True},
        "final_database_snapshot_ok": True,
        "window_completed": False,
    }

    assert live_window_has_real_order_evidence(report, "polymarket_predict") is False
