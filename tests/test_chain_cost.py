from dataclasses import replace
from decimal import Decimal
from pathlib import Path

import pytest

import arbitrage_engine.chain_cost as chain_cost_module
from arbitrage_engine.chain_cost import LiveChainCostEstimator, LiveChainCostUnavailable
from arbitrage_engine.config import load_config


@pytest.mark.asyncio
async def test_live_chain_cost_uses_rpc_gas_price_and_conservative_floor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = load_config(Path(__file__).parents[1] / "config.example.json")
    config = replace(
        config,
        spread_policy=replace(
            config.spread_policy,
            fixed_chain_cost_usd_by_route={"polymarket_predict": 0.25},
            gas_units_by_route={"polymarket_predict": {"56": 500_000}},
            native_token_usd_ceiling_by_chain={"56": 1_000.0},
            gas_price_multiplier=1.5,
            require_live_gas_estimate=True,
        ),
    )
    gas_calls = 0

    class _FakeWeb3Client:
        def __init__(self, *args: object, **kwargs: object) -> None:
            del args, kwargs

        async def gas_price_wei(self) -> int:
            nonlocal gas_calls
            gas_calls += 1
            return 1_000_000_000

        async def close(self) -> None:
            return None

    monkeypatch.setattr(chain_cost_module, "BaseWeb3Client", _FakeWeb3Client)
    estimator = LiveChainCostEstimator(config)

    first = await estimator.estimate("polymarket_predict", require_live=True)
    second = await estimator.estimate("polymarket_predict", require_live=True)

    assert first.live is True
    assert first.live_estimate_usd == Decimal("0.7500000000")
    assert first.reserved_cost_usd == Decimal("0.7500000000")
    assert second == first
    assert gas_calls == 1
    await estimator.close()


@pytest.mark.asyncio
async def test_live_chain_cost_fails_closed_when_rpc_quote_is_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = load_config(Path(__file__).parents[1] / "config.example.json")
    config = replace(
        config,
        spread_policy=replace(
            config.spread_policy,
            gas_units_by_route={"polymarket_predict": {"56": 500_000}},
            native_token_usd_ceiling_by_chain={"56": 1_000.0},
            require_live_gas_estimate=True,
        ),
    )

    class _FailingWeb3Client:
        def __init__(self, *args: object, **kwargs: object) -> None:
            del args, kwargs

        async def gas_price_wei(self) -> int:
            raise TimeoutError("RPC timeout")

        async def close(self) -> None:
            return None

    monkeypatch.setattr(chain_cost_module, "BaseWeb3Client", _FailingWeb3Client)
    estimator = LiveChainCostEstimator(config)

    with pytest.raises(LiveChainCostUnavailable, match="gas quote failed for chain 56"):
        await estimator.estimate("polymarket_predict", require_live=True)
    await estimator.close()
