from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from .models import AmmPool, BinarySide, MarketSpec


@dataclass(frozen=True)
class TelegramConfig:
    bot_token: str | None
    chat_id: str | None


@dataclass(frozen=True)
class PolymarketConfig:
    private_key: str | None
    api_base_url: str
    chain_id: int
    signature_type: int
    funder: str | None


@dataclass(frozen=True)
class PredictFunConfig:
    private_key: str | None
    rpc_url: str
    rpc_urls: list[str]
    chain_id: int
    network: str
    api_base_url: str | None
    api_key: str | None
    ws_url: str | None
    market_abi_path: str | None
    collateral_token_address: str | None
    fee_rate_bps: int
    precision: int
    reserves_function: str
    balance_function: str
    max_priority_fee_gwei: float
    confirmations: int
    max_slippage_pct: float


@dataclass(frozen=True)
class MyriadMarketsConfig:
    api_url: str
    api_key: str | None
    private_key: str | None
    rpc_url: str
    rpc_urls: list[str]
    chain_id: int
    exchange_address: str
    conditional_tokens_address: str
    collateral_tokens: dict[str, str]
    collateral_symbol: str
    trading_fee_pct: float
    max_slippage_pct: float
    enabled: bool


@dataclass(frozen=True)
class Web3NetworkConfig:
    rpc_url: str
    rpc_urls: list[str]
    chain_id: int
    max_slippage_pct: float
    max_priority_fee_gwei: float
    confirmations: int


@dataclass(frozen=True)
class AutoCloseConfig:
    enabled: bool
    exit_spread_pct: float


@dataclass(frozen=True)
class AppConfig:
    is_test: bool
    position_size_usd: float
    max_order_size_usd: float
    min_net_spread: float
    poll_interval_ms: int
    polymarket_fill_timeout_ms: int
    predict_fun_fill_timeout_ms: int
    myriad_fill_timeout_ms: int
    signal_alert_cooldown_seconds: int
    categories_to_scan: list[str]
    telegram: TelegramConfig
    polymarket: PolymarketConfig
    predict_fun: PredictFunConfig
    myriad_markets: MyriadMarketsConfig
    web3_networks: dict[str, Web3NetworkConfig]
    auto_close: AutoCloseConfig
    markets: list[MarketSpec]


def _expand_env(value: Any) -> Any:
    if isinstance(value, str) and value.startswith("${") and value.endswith("}"):
        return os.getenv(value[2:-1])
    if isinstance(value, dict):
        return {key: _expand_env(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_expand_env(item) for item in value]
    return value


def _optional_str(value: Any) -> str | None:
    if value in (None, ""):
        return None
    return str(value)


def _str_or_default(value: Any, default: str) -> str:
    if value in (None, ""):
        return default
    return str(value)


def _parse_rpc_urls(value: Any, fallback: str | None = None) -> list[str]:
    if isinstance(value, list):
        urls = [str(item) for item in value if item not in (None, "")]
    elif value not in (None, ""):
        urls = [str(value)]
    else:
        urls = []
    if not urls and fallback:
        urls = [fallback]
    return urls


def _first_rpc_url(value: Any) -> str | None:
    urls = _parse_rpc_urls(value)
    return urls[0] if urls else None


def _parse_datetime(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    if not isinstance(value, str):
        raise ValueError("expires_at must be an ISO-8601 string")
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _parse_amm_pool(value: Any) -> AmmPool | None:
    if not isinstance(value, dict):
        return None
    return AmmPool(
        yes_reserve=float(value["yes_reserve"]),
        no_reserve=float(value["no_reserve"]),
        fee_pct=float(value.get("fee_pct", 0.0)),
    )


def load_config(path: str | Path) -> AppConfig:
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    data = _expand_env(raw)

    markets = [
        MarketSpec(
            symbol=str(item["symbol"]),
            target_label=str(item["target_label"]),
            polymarket_token_id=str(item.get("polymarket_token_id") or ""),
            polymarket_side=BinarySide(str(item["polymarket_side"])),
            predict_fun_token_id=str(item.get("predict_fun_token_id") or ""),
            predict_fun_side=BinarySide(str(item.get("predict_fun_side") or "NO")),
            venue_a_label=str(item.get("venue_a_label") or "Polymarket"),
            venue_b_label=str(item.get("venue_b_label") or "Predict.fun"),
            expires_at=_parse_datetime(item.get("expires_at")),
            condition_id=item.get("condition_id"),
            tick_size=item.get("tick_size"),
            neg_risk=item.get("neg_risk"),
            predict_fun_market_id=item.get("predict_fun_market_id"),
            predict_fun_amm_pool=_parse_amm_pool(item.get("predict_fun_amm_pool")),
            myriad_market_id=_optional_str(item.get("myriad_market_id")),
            myriad_side=BinarySide(str(item.get("myriad_side") or "NO")),
            rules_fingerprint=item.get("rules_fingerprint"),
        )
        for item in data.get("markets", [])
    ]
    auto_close = data.get("auto_close", {})
    predict_fun = data.get("predict_fun", {})
    myriad = data.get("myriad_markets", {})
    web3_networks_raw = data.get("web3_networks", {})
    web3_networks = {
        name: Web3NetworkConfig(
            rpc_url=_str_or_default(item.get("rpc_url") or _first_rpc_url(item.get("rpc_urls")), ""),
            rpc_urls=_parse_rpc_urls(item.get("rpc_urls"), _optional_str(item.get("rpc_url"))),
            chain_id=int(item["chain_id"]),
            max_slippage_pct=float(item.get("max_slippage_pct", 0.015)),
            max_priority_fee_gwei=float(item.get("max_priority_fee_gwei", _default_priority_fee_gwei(int(item["chain_id"])))),
            confirmations=int(item.get("confirmations", 1)),
        )
        for name, item in web3_networks_raw.items()
    }
    bnb_network = web3_networks.get("bnb")

    return AppConfig(
        is_test=bool(data.get("isTest", True)),
        position_size_usd=float(data.get("position_size_usd", data.get("max_order_size_usd", 100.0))),
        max_order_size_usd=float(data.get("max_order_size_usd", 100.0)),
        min_net_spread=float(data.get("min_net_spread", 0.10)),
        poll_interval_ms=int(data.get("poll_interval_ms", 250)),
        polymarket_fill_timeout_ms=int(data.get("polymarket_fill_timeout_ms", 500)),
        predict_fun_fill_timeout_ms=int(data.get("predict_fun_fill_timeout_ms", 4_000)),
        myriad_fill_timeout_ms=int(data.get("myriad_fill_timeout_ms", data.get("predict_fun_fill_timeout_ms", 4_000))),
        signal_alert_cooldown_seconds=int(data.get("signal_alert_cooldown_seconds", 900)),
        categories_to_scan=[str(item) for item in data.get("categories_to_scan", ["sports", "esports", "finance"])],
        telegram=TelegramConfig(
            bot_token=_optional_str(data.get("telegram", {}).get("bot_token")),
            chat_id=_optional_str(data.get("telegram", {}).get("chat_id")),
        ),
        polymarket=PolymarketConfig(
            private_key=_optional_str(data.get("polymarket", {}).get("private_key")),
            api_base_url=_str_or_default(data.get("polymarket", {}).get("api_base_url"), "https://clob.polymarket.com"),
            chain_id=int(data.get("polymarket", {}).get("chain_id", 137)),
            signature_type=int(data.get("polymarket", {}).get("signature_type", 0)),
            funder=_optional_str(data.get("polymarket", {}).get("funder")),
        ),
        predict_fun=PredictFunConfig(
            private_key=_optional_str(predict_fun.get("private_key")),
            rpc_url=_str_or_default(
                predict_fun.get("rpc_url") or _first_rpc_url(predict_fun.get("rpc_urls")) or (bnb_network.rpc_url if bnb_network else None),
                "https://bsc-dataseed.binance.org",
            ),
            rpc_urls=_parse_rpc_urls(
                predict_fun.get("rpc_urls"),
                _optional_str(predict_fun.get("rpc_url")) or (bnb_network.rpc_url if bnb_network else "https://bsc-dataseed.binance.org"),
            ),
            chain_id=int(predict_fun.get("chain_id") or (bnb_network.chain_id if bnb_network else 56)),
            network=str(predict_fun.get("network", "mainnet")),
            api_base_url=_optional_str(predict_fun.get("api_base_url")),
            api_key=_optional_str(predict_fun.get("api_key")),
            ws_url=_optional_str(predict_fun.get("ws_url")),
            market_abi_path=_optional_str(predict_fun.get("market_abi_path")),
            collateral_token_address=_optional_str(predict_fun.get("collateral_token_address")),
            fee_rate_bps=int(predict_fun.get("fee_rate_bps", 0)),
            precision=int(predict_fun.get("precision", 18)),
            reserves_function=str(predict_fun.get("reserves_function", "getPoolReserves")),
            balance_function=str(predict_fun.get("balance_function", "balanceOf")),
            max_priority_fee_gwei=float(
                predict_fun.get("max_priority_fee_gwei")
                or (bnb_network.max_priority_fee_gwei if bnb_network else _default_priority_fee_gwei(int(predict_fun.get("chain_id", 56))))
            ),
            confirmations=int(predict_fun.get("confirmations") or (bnb_network.confirmations if bnb_network else 1)),
            max_slippage_pct=float(predict_fun.get("max_slippage_pct", 0.015)),
        ),
        myriad_markets=MyriadMarketsConfig(
            api_url=_str_or_default(myriad.get("api_url"), "https://api-v2.myriadprotocol.com"),
            api_key=_optional_str(myriad.get("api_key")),
            private_key=_optional_str(myriad.get("private_key")),
            rpc_url=_str_or_default(
                myriad.get("rpc_url") or _first_rpc_url(myriad.get("rpc_urls")) or (bnb_network.rpc_url if bnb_network else None),
                "https://bsc-dataseed.binance.org",
            ),
            rpc_urls=_parse_rpc_urls(
                myriad.get("rpc_urls"),
                _optional_str(myriad.get("rpc_url")) or (bnb_network.rpc_url if bnb_network else "https://bsc-dataseed.binance.org"),
            ),
            chain_id=int(myriad.get("chain_id", 56)),
            exchange_address=_str_or_default(myriad.get("exchange_address"), "0xa0b6f8ef8EdB64f395018D1933f2273Ce9f0f16A"),
            conditional_tokens_address=_str_or_default(
                myriad.get("conditional_tokens_address"),
                "0x6413734f92248D4B29ae35883290BD93212654Dc",
            ),
            collateral_tokens={str(key): str(value) for key, value in myriad.get("collateral_tokens", {}).items()},
            collateral_symbol=str(myriad.get("collateral_symbol", "USDT")),
            trading_fee_pct=float(myriad.get("trading_fee_pct", 0.0)),
            max_slippage_pct=float(myriad.get("max_slippage_pct", 0.015)),
            enabled=bool(myriad.get("enabled", False)),
        ),
        web3_networks=web3_networks,
        auto_close=AutoCloseConfig(
            enabled=bool(auto_close.get("enabled", True)),
            exit_spread_pct=float(auto_close.get("exit_spread_pct", 0.02)),
        ),
        markets=markets,
    )


def validate_config(config: AppConfig, *, require_resolved_markets: bool = False) -> None:
    errors: list[str] = []
    if not config.markets:
        errors.append("markets must contain at least one market")
    if config.position_size_usd <= 0:
        errors.append("position_size_usd must be positive")
    if config.max_order_size_usd <= 0:
        errors.append("max_order_size_usd must be positive")
    if config.min_net_spread < 0.10:
        errors.append("min_net_spread must be at least 0.10 for binary arbitrage")
    if config.predict_fun.max_slippage_pct <= 0:
        errors.append("predict_fun.max_slippage_pct must be positive")
    if config.polymarket_fill_timeout_ms < 300:
        errors.append("polymarket_fill_timeout_ms must be at least 300 for production-safe CLOB fills")
    if config.predict_fun_fill_timeout_ms < 3_600:
        errors.append("predict_fun_fill_timeout_ms must be at least 3600 for Predict.fun/Web3 fills")
    if config.myriad_fill_timeout_ms < 3_600:
        errors.append("myriad_fill_timeout_ms must be at least 3600 for Myriad/Web3 fills")
    if config.signal_alert_cooldown_seconds < 0:
        errors.append("signal_alert_cooldown_seconds must be non-negative")
    if config.auto_close.exit_spread_pct < 0:
        errors.append("auto_close.exit_spread_pct must be non-negative")
    if config.predict_fun.chain_id not in (56, 97):
        errors.append("predict_fun.chain_id must be 56 for BNB mainnet or 97 for BNB testnet")
    if config.predict_fun.network not in ("mainnet", "testnet"):
        errors.append("predict_fun.network must be mainnet or testnet")
    if config.predict_fun.precision <= 0:
        errors.append("predict_fun.precision must be positive")
    if config.myriad_markets.enabled:
        if not config.myriad_markets.api_key:
            errors.append("MYRIAD_API_KEY is required when myriad_markets.enabled=true")
        if not config.is_test and not config.myriad_markets.private_key:
            errors.append("MYRIAD_PRIVATE_KEY is required when myriad_markets.enabled=true")
        if config.myriad_markets.chain_id != 56:
            errors.append("myriad_markets.chain_id must be 56")
        if config.myriad_markets.max_slippage_pct > 0.015:
            errors.append("myriad_markets.max_slippage_pct must be <= 0.015")
        if config.myriad_markets.collateral_symbol not in config.myriad_markets.collateral_tokens:
            errors.append("myriad_markets.collateral_symbol must exist in myriad_markets.collateral_tokens")
    for name, network in config.web3_networks.items():
        if not network.rpc_url and not config.is_test:
            errors.append(f"web3_networks.{name}.rpc_url is required")
        if network.chain_id <= 0:
            errors.append(f"web3_networks.{name}.chain_id must be positive")
        if network.confirmations < 0:
            errors.append(f"web3_networks.{name}.confirmations must be non-negative")

    for index, market in enumerate(config.markets):
        prefix = f"markets[{index}]"
        has_discovery_terms = bool(market.symbol and market.target_label)
        if market.predict_fun_side == market.polymarket_side:
            errors.append(f"{prefix}.predict_fun_side must be opposite to polymarket_side")
        if (
            (require_resolved_markets or not has_discovery_terms)
            and (not market.polymarket_token_id or market.polymarket_token_id.startswith("replace-with"))
        ):
            errors.append(f"{prefix}.polymarket_token_id or discovery fields symbol/target_label are required")
        if (
            (require_resolved_markets or not has_discovery_terms)
            and (not market.predict_fun_token_id or market.predict_fun_token_id.startswith("replace-with"))
            and market.predict_fun_amm_pool is None
        ):
            errors.append(f"{prefix}.predict_fun_token_id or predict_fun_amm_pool is required")
        if config.auto_close.enabled and market.expires_at is None and (require_resolved_markets or not has_discovery_terms):
            errors.append(f"{prefix}.expires_at is required when auto_close.enabled=true")
        if (
            config.myriad_markets.enabled
            and require_resolved_markets
            and (not market.myriad_market_id or market.myriad_market_id.startswith("replace-with"))
        ):
            errors.append(f"{prefix}.myriad_market_id or discovery fields symbol/target_label are required")

    if not config.is_test:
        if not config.polymarket.private_key:
            errors.append("POLYMARKET_PRIVATE_KEY is required when isTest=false")
        elif not _is_private_key(config.polymarket.private_key):
            errors.append("POLYMARKET_PRIVATE_KEY must be a 64 hex character ECDSA key, with optional 0x prefix")
        if not config.predict_fun.private_key:
            errors.append("PREDICT_FUN_PRIVATE_KEY is required when isTest=false")
        elif not _is_private_key(config.predict_fun.private_key):
            errors.append("PREDICT_FUN_PRIVATE_KEY must be a 64 hex character ECDSA key, with optional 0x prefix")
        if not config.predict_fun.rpc_url:
            errors.append("BNB_RPC_URL or predict_fun.rpc_url is required when isTest=false")
        if not config.predict_fun.api_base_url:
            errors.append("predict_fun.api_base_url is required when isTest=false")
        if config.predict_fun.network == "mainnet" and not config.predict_fun.api_key:
            errors.append("PREDICT_FUN_API_KEY is required for Predict.fun mainnet")
        if not config.predict_fun.market_abi_path and not config.predict_fun.api_base_url:
            errors.append("predict_fun.market_abi_path or api_base_url is required for price reads when isTest=false")
        if config.polymarket.signature_type != 0 and not config.polymarket.funder:
            errors.append("POLYMARKET_FUNDER_ADDRESS is required for non-EOA signature types")
        if config.myriad_markets.enabled and config.myriad_markets.private_key and not _is_private_key(config.myriad_markets.private_key):
            errors.append("MYRIAD_PRIVATE_KEY must be a 64 hex character ECDSA key, with optional 0x prefix")

    if errors:
        joined = "\n - ".join(errors)
        raise ValueError(f"Invalid configuration:\n - {joined}")


def _default_priority_fee_gwei(chain_id: int) -> float:
    if chain_id in (56, 97):
        return 2.0
    if chain_id in (137, 80002):
        return 20.0
    return 3.0


def _is_private_key(value: str) -> bool:
    raw = value[2:] if value.startswith("0x") else value
    if len(raw) != 64:
        return False
    return all(char in "0123456789abcdefABCDEF" for char in raw)
