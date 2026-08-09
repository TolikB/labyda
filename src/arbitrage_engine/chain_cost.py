from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from decimal import Decimal

from .config import AppConfig
from .connectors.web3_base import BaseWeb3Client


class LiveChainCostUnavailable(RuntimeError):
    """Raised when a production route cannot obtain a current gas quote."""


@dataclass(frozen=True)
class ChainGasComponent:
    chain_id: int
    gas_units: int
    gas_price_wei: int
    native_token_usd_ceiling: Decimal
    estimated_cost_usd: Decimal

    def as_dict(self) -> dict[str, str | int]:
        return {
            "chain_id": self.chain_id,
            "gas_units": self.gas_units,
            "gas_price_wei": self.gas_price_wei,
            "native_token_usd_ceiling": str(self.native_token_usd_ceiling),
            "estimated_cost_usd": str(self.estimated_cost_usd),
        }


@dataclass(frozen=True)
class RouteChainCostQuote:
    route: str
    configured_floor_usd: Decimal
    live_estimate_usd: Decimal
    reserved_cost_usd: Decimal
    multiplier: Decimal
    live: bool
    components: tuple[ChainGasComponent, ...]

    def as_dict(self) -> dict[str, object]:
        return {
            "route": self.route,
            "configured_floor_usd": str(self.configured_floor_usd),
            "live_estimate_usd": str(self.live_estimate_usd),
            "reserved_cost_usd": str(self.reserved_cost_usd),
            "multiplier": str(self.multiplier),
            "live": self.live,
            "components": [component.as_dict() for component in self.components],
        }


class LiveChainCostEstimator:
    def __init__(self, config: AppConfig) -> None:
        self._config = config
        self._clients: dict[int, BaseWeb3Client] = {}
        self._cache: dict[str, tuple[float, RouteChainCostQuote]] = {}

    async def estimate(self, route: str, *, require_live: bool) -> RouteChainCostQuote:
        cached = self._cache.get(route)
        if cached is not None and time.monotonic() - cached[0] <= self._config.spread_policy.gas_quote_ttl_seconds:
            return cached[1]

        floor = Decimal(str(self._config.spread_policy.fixed_chain_cost_for(route)))
        route_units = self._config.spread_policy.gas_units_by_route.get(route, {})
        if not route_units:
            if require_live:
                raise LiveChainCostUnavailable(f"live gas policy is not configured for route {route}")
            return RouteChainCostQuote(route, floor, Decimal(0), floor, Decimal(1), False, ())

        multiplier = Decimal(str(self._config.spread_policy.gas_price_multiplier))
        components = await asyncio.gather(
            *(self._estimate_chain(int(chain_id), gas_units, multiplier) for chain_id, gas_units in route_units.items())
        )
        live_estimate = sum((component.estimated_cost_usd for component in components), Decimal(0))
        quote = RouteChainCostQuote(
            route=route,
            configured_floor_usd=floor,
            live_estimate_usd=live_estimate,
            reserved_cost_usd=max(floor, live_estimate),
            multiplier=multiplier,
            live=True,
            components=tuple(components),
        )
        self._cache[route] = (time.monotonic(), quote)
        return quote

    async def close(self) -> None:
        clients = tuple(self._clients.values())
        self._clients.clear()
        await asyncio.gather(*(client.close() for client in clients), return_exceptions=True)

    async def _estimate_chain(
        self,
        chain_id: int,
        gas_units: int,
        multiplier: Decimal,
    ) -> ChainGasComponent:
        native_usd = self._config.spread_policy.native_token_usd_ceiling_by_chain.get(str(chain_id))
        if native_usd is None or native_usd <= 0:
            raise LiveChainCostUnavailable(f"native-token USD ceiling is missing for chain {chain_id}")
        client = self._clients.get(chain_id)
        if client is None:
            rpc_urls = _rpc_urls_for_chain(self._config, chain_id)
            if not rpc_urls:
                raise LiveChainCostUnavailable(f"RPC URL is missing for chain {chain_id}")
            client = BaseWeb3Client(rpc_url=rpc_urls, chain_id=chain_id)
            self._clients[chain_id] = client
        try:
            gas_price_wei = await client.gas_price_wei()
        except Exception as exc:
            raise LiveChainCostUnavailable(f"gas quote failed for chain {chain_id}: {exc}") from exc
        if gas_price_wei <= 0:
            raise LiveChainCostUnavailable(f"gas quote is not positive for chain {chain_id}")
        native_ceiling = Decimal(str(native_usd))
        estimated = (
            Decimal(gas_price_wei)
            * Decimal(gas_units)
            * native_ceiling
            * multiplier
            / Decimal(10**18)
        )
        return ChainGasComponent(chain_id, gas_units, gas_price_wei, native_ceiling, estimated)


def _rpc_urls_for_chain(config: AppConfig, chain_id: int) -> list[str]:
    candidates: list[str] = []
    venue_configs = (config.polymarket, config.predict_fun, config.sx_bet, config.myriad_markets)
    for venue_config in venue_configs:
        if venue_config.chain_id != chain_id:
            continue
        candidates.extend(venue_config.rpc_urls)
        candidates.append(venue_config.rpc_url)
    for network in config.web3_networks.values():
        if network.chain_id != chain_id:
            continue
        candidates.extend(network.rpc_urls)
        candidates.append(network.rpc_url)
    return list(dict.fromkeys(url for url in candidates if url))
