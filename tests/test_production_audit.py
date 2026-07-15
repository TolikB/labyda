from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest

from arbitrage_engine.config import load_config
from arbitrage_engine.database import ProductionRepository
from arbitrage_engine.discovery_lifecycle import DiscoveryDiagnostics
from arbitrage_engine.models import (
    BinarySide,
    ExecutionMode,
    MappingStatus,
    MarketConstraints,
    MarketSpec,
    OrderPreview,
    VenueFeeQuote,
)
from arbitrage_engine.production_audit import (
    RouteDiscoverySnapshot,
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
    )


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
            "Polymarket": {"canary_gate": {"venue": "Polymarket", "passed": True, "blocking_reasons": []}},
            "Predict.fun": {"canary_gate": {"venue": "Predict.fun", "passed": True, "blocking_reasons": []}},
            "SX Bet": {"canary_gate": {"venue": "SX Bet", "passed": True, "blocking_reasons": []}},
            "Myriad": {"canary_gate": {"venue": "Myriad", "passed": True, "blocking_reasons": []}},
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

        async def preview_buy(
            self,
            token_id: str,
            side: BinarySide,
            contracts: Decimal,
            max_price: Decimal,
            **kwargs: object,
        ) -> OrderPreview:
            del kwargs
            fee_quote = VenueFeeQuote("Test", 0, "zero_fee")
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
    monkeypatch.setattr(audit_module, "SxBetApiClient", _FakeClient)
    monkeypatch.setattr(audit_module, "MyriadClient", _FakeClient)

    report = await collect_all_market_audit(config, snapshot, runtime_snapshot={})

    assert report["discovery_snapshot_id"] == build_route_overlap_report(snapshot)["discovery_snapshot_id"]
    assert report["route_summary"]["polymarket_sx"]["openable_count"] == 1
    assert report["route_summary"]["sx_myriad"]["openable_count"] == 1
    assert report["route_summary"]["predict_sx"]["openable_count"] == 1
    assert report["route_summary"]["predict_myriad"]["openable_count"] == 0
    assert any(
        "orderbook_unavailable:Myriad" in item["blocker"]
        for item in report["route_summary"]["predict_myriad"]["blocker_samples"]
    )


@pytest.mark.asyncio
async def test_collect_all_market_audit_uses_verified_route_state_from_verified_snapshot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import arbitrage_engine.production_audit as audit_module

    base_config = load_config(Path(__file__).parents[1] / "config.example.json")
    config = replace(
        base_config,
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

        async def preview_buy(
            self,
            token_id: str,
            side: BinarySide,
            contracts: Decimal,
            max_price: Decimal,
            **kwargs: object,
        ) -> OrderPreview:
            del kwargs
            fee_quote = VenueFeeQuote("Test", 0, "zero_fee")
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
    assert report["route_summary"]["polymarket_myriad"]["openable_count"] == 1
    assert report["markets"][0]["preview_feasible"] is True
    assert report["markets"][0]["verified_routes"] == ["polymarket_myriad"]


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
        def __init__(self, scan_all: bool = True) -> None:
            del scan_all
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
        def __init__(self, scan_all: bool = True) -> None:
            del scan_all
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
            return [
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

    assert report["routes"]["polymarket_predict"]["engine_safe_matched_count"] == 1
    assert report["routes"]["polymarket_predict"]["verified_tradable_count"] == 1
    assert report["routes"]["predict_myriad"]["engine_safe_matched_count"] == 1
    assert report["routes"]["predict_myriad"]["verified_tradable_count"] == 1
    assert report["diagnostics"]["stages"]["cross_venue_candidates"] == 2
    assert report["diagnostics"]["stages"]["horizon_accepted"] == 1
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
