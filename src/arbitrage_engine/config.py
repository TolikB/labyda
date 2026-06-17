from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from .models import HedgeSide, MarketSpec, PolymarketSide


@dataclass(frozen=True)
class TelegramConfig:
    bot_token: str | None
    chat_id: str | None


@dataclass(frozen=True)
class BinanceConfig:
    api_key: str | None
    api_secret: str | None


@dataclass(frozen=True)
class PolymarketConfig:
    private_key: str | None
    api_base_url: str
    chain_id: int
    signature_type: int
    funder: str | None


@dataclass(frozen=True)
class AutoCloseConfig:
    enabled: bool
    take_profit_pct: float
    close_before_expiry_seconds: int


@dataclass(frozen=True)
class AppConfig:
    is_test: bool
    max_order_size_usd: float
    min_net_spread: float
    cefi_taker_fee: float
    cefi_leverage: float
    poll_interval_ms: int
    polymarket_fill_timeout_ms: int
    telegram: TelegramConfig
    binance: BinanceConfig
    polymarket: PolymarketConfig
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


def _parse_datetime(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    if not isinstance(value, str):
        raise ValueError("expires_at must be an ISO-8601 string")
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def load_config(path: str | Path) -> AppConfig:
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    data = _expand_env(raw)

    markets = [
        MarketSpec(
            symbol=str(item["symbol"]),
            target_label=str(item["target_label"]),
            polymarket_token_id=str(item.get("polymarket_token_id") or ""),
            polymarket_side=PolymarketSide(str(item["polymarket_side"])),
            cefi_symbol=str(item["cefi_symbol"]),
            cefi_hedge_side=HedgeSide(str(item["cefi_hedge_side"])),
            expires_at=_parse_datetime(item.get("expires_at")),
            condition_id=item.get("condition_id"),
            tick_size=item.get("tick_size"),
            neg_risk=item.get("neg_risk"),
        )
        for item in data.get("markets", [])
    ]
    auto_close = data.get("auto_close", {})

    return AppConfig(
        is_test=bool(data.get("isTest", True)),
        max_order_size_usd=float(data.get("max_order_size_usd", 100.0)),
        min_net_spread=float(data.get("min_net_spread", 0.05)),
        cefi_taker_fee=float(data.get("cefi_taker_fee", 0.0005)),
        cefi_leverage=float(data.get("cefi_leverage", 10.0)),
        poll_interval_ms=int(data.get("poll_interval_ms", 250)),
        polymarket_fill_timeout_ms=int(data.get("polymarket_fill_timeout_ms", 300)),
        telegram=TelegramConfig(
            bot_token=data.get("telegram", {}).get("bot_token"),
            chat_id=data.get("telegram", {}).get("chat_id"),
        ),
        binance=BinanceConfig(
            api_key=data.get("binance", {}).get("api_key"),
            api_secret=data.get("binance", {}).get("api_secret"),
        ),
        polymarket=PolymarketConfig(
            private_key=data.get("polymarket", {}).get("private_key"),
            api_base_url=str(data.get("polymarket", {}).get("api_base_url", "https://clob.polymarket.com")),
            chain_id=int(data.get("polymarket", {}).get("chain_id", 137)),
            signature_type=int(data.get("polymarket", {}).get("signature_type", 0)),
            funder=data.get("polymarket", {}).get("funder"),
        ),
        auto_close=AutoCloseConfig(
            enabled=bool(auto_close.get("enabled", True)),
            take_profit_pct=float(auto_close.get("take_profit_pct", 0.10)),
            close_before_expiry_seconds=int(auto_close.get("close_before_expiry_seconds", 3600)),
        ),
        markets=markets,
    )


def validate_config(config: AppConfig, *, require_resolved_markets: bool = False) -> None:
    errors: list[str] = []
    if not config.markets:
        errors.append("markets must contain at least one market")

    for index, market in enumerate(config.markets):
        prefix = f"markets[{index}]"
        has_discovery_terms = bool(market.symbol and market.target_label)
        if (
            (require_resolved_markets or not has_discovery_terms)
            and (not market.polymarket_token_id or market.polymarket_token_id == "replace-with-token-id")
        ):
            errors.append(f"{prefix}.polymarket_token_id or discovery fields symbol/target_label are required")
        if (
            (require_resolved_markets or not has_discovery_terms)
            and market.condition_id in (None, "", "replace-with-condition-id")
            and (
            market.tick_size is None or market.neg_risk is None
            )
        ):
            errors.append(f"{prefix}.condition_id or both tick_size and neg_risk are required")
        if config.auto_close.enabled and market.expires_at is None and (require_resolved_markets or not has_discovery_terms):
            errors.append(f"{prefix}.expires_at is required when auto_close.enabled=true")

    if not config.is_test:
        if not config.binance.api_key:
            errors.append("BINANCE_API_KEY is required when isTest=false")
        if not config.binance.api_secret:
            errors.append("BINANCE_API_SECRET is required when isTest=false")
        if not config.polymarket.private_key:
            errors.append("POLYMARKET_PRIVATE_KEY is required when isTest=false")
        if config.polymarket.signature_type != 0 and not config.polymarket.funder:
            errors.append("POLYMARKET_FUNDER_ADDRESS is required for non-EOA signature types")

    if errors:
        joined = "\n - ".join(errors)
        raise ValueError(f"Invalid configuration:\n - {joined}")
