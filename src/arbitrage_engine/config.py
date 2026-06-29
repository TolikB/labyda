from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .models import AmmPool, BinarySide, ExecutionMode, MappingStatus, MarketSpec

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class TelegramConfig:
    bot_token: str | None
    chat_id: str | None
    min_interval_seconds: float = 1.0
    log_raw_signal_books: bool = False


@dataclass(frozen=True)
class PolymarketConfig:
    private_key: str | None
    api_base_url: str
    chain_id: int
    signature_type: int
    funder: str | None
    api_key: str | None = None
    api_secret: str | None = None
    api_passphrase: str | None = None
    max_slippage_pct: float = 0.015
    trading_fee_pct: float = 0.0
    rpc_url: str = "https://polygon-rpc.com"
    rpc_urls: list[str] = field(default_factory=list)
    conditional_tokens_address: str = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
    collateral_token_address: str = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
    confirmations: int = 2
    max_priority_fee_gwei: float = 30.0
    redemption_gas_limit: int = 350_000


@dataclass(frozen=True)
class PredictFunConfig:
    enabled: bool
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
    ws_url: str
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
    order_book_ttl_ms: int = 300
    websocket_stale_after_ms: int = 1_500
    confirmations: int = 3
    max_priority_fee_gwei: float = 2.0
    redemption_gas_limit: int = 350_000


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
class RouteConfig:
    polymarket_myriad: bool = True
    polymarket_predict: bool = True
    predict_myriad: bool = True

    def any_enabled(self) -> bool:
        return self.polymarket_myriad or self.polymarket_predict or self.predict_myriad


@dataclass(frozen=True)
class AppConfig:
    is_test: bool
    scan_all: bool
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
    enable_predict_fun: bool = False
    min_market_volume_usd: float = 25_000.0
    min_entry_spread_pct: float = 0.05
    min_retry_spread_pct: float = 0.05
    shadow_mode: bool = True
    min_venue_balance_usd: float = 50.0
    auto_rebalance_ratio_threshold: float = 0.80
    enable_auto_rebalance: bool = False
    max_consecutive_api_errors: int = 3
    max_daily_loss_usd: float = 100.0
    max_open_positions: int = 5
    spread_guard_floor: float = 0.05
    balance_refresh_interval_seconds: float = 5.0
    max_concurrent_market_evaluations: int = 100
    cancel_reconcile_timeout_ms: int = 1_000
    max_orderbook_age_seconds: float = 2.0
    max_production_price_impact: float = 0.015
    websocket_heartbeat_interval_seconds: float = 30.0
    websocket_stale_after_seconds: float = 10.0
    execution_mode: ExecutionMode = ExecutionMode.PAPER
    database_url: str | None = None
    routes: RouteConfig = field(default_factory=RouteConfig)
    reconciliation_orders_interval_seconds: float = 5.0
    reconciliation_full_interval_seconds: float = 30.0
    market_data_snapshot_interval_seconds: float = 30.0
    max_total_notional_usd: float = 500.0
    max_venue_exposure_usd: float = 300.0
    max_market_exposure_usd: float = 100.0
    max_orders_per_minute: int = 30
    max_unresolved_exposure_usd: float = 25.0
    observability_host: str = "0.0.0.0"
    observability_port: int = 9108
    live_trading_confirmed: bool = False
    _execution_mode_explicit: bool = False

    def __post_init__(self) -> None:
        # One-release compatibility for callers constructing AppConfig directly.
        # Config files always set the normalized execution mode explicitly.
        if not self._execution_mode_explicit:
            legacy_mode = (
                ExecutionMode.PAPER
                if self.is_test
                else ExecutionMode.SHADOW
                if self.shadow_mode
                else ExecutionMode.LIVE
            )
            object.__setattr__(
                self,
                "execution_mode",
                legacy_mode,
            )


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


def _optional_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _fraction(value: Any, field_name: str) -> float:
    parsed = float(value)
    if not 0.0 <= parsed <= 1.0:
        raise ValueError(f"{field_name} must be a decimal fraction between 0 and 1")
    return parsed


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
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


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
    configured_markets = data.get("markets", [])
    scan_all = bool(data.get("scan_all", False)) or _is_scan_all_filter(configured_markets)

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
            polymarket_market_id=_optional_str(item.get("polymarket_market_id")),
            polymarket_url=_optional_str(item.get("polymarket_url")),
            tick_size=item.get("tick_size"),
            neg_risk=item.get("neg_risk"),
            predict_fun_neg_risk=item.get("predict_fun_neg_risk"),
            predict_fun_fee_rate_bps=(
                int(item["predict_fun_fee_rate_bps"]) if item.get("predict_fun_fee_rate_bps") is not None else None
            ),
            predict_fun_market_id=item.get("predict_fun_market_id"),
            predict_fun_url=_optional_str(item.get("predict_fun_url")),
            predict_fun_amm_pool=_parse_amm_pool(item.get("predict_fun_amm_pool")),
            myriad_market_id=_optional_str(item.get("myriad_market_id")),
            myriad_url=_optional_str(item.get("myriad_url")),
            myriad_side=BinarySide(str(item.get("myriad_side") or "NO")),
            rules_fingerprint=item.get("rules_fingerprint"),
            polymarket_volume_usd=_optional_float(item.get("polymarket_volume_usd")),
            predict_fun_volume_usd=_optional_float(item.get("predict_fun_volume_usd")),
            myriad_volume_usd=_optional_float(item.get("myriad_volume_usd")),
            category=_optional_str(item.get("category")),
            mapping_status=MappingStatus(str(item.get("mapping_status") or "CANDIDATE").upper()),
            resolution_source=_optional_str(item.get("resolution_source")),
            outcome_semantics=_optional_str(item.get("outcome_semantics")),
            cutoff_at=_parse_datetime(item.get("cutoff_at")),
            timezone_name=str(item.get("timezone_name") or "UTC"),
            verified_routes=frozenset(str(value) for value in item.get("verified_routes", [])),
        )
        for item in ([] if scan_all else configured_markets)
    ]
    auto_close = data.get("auto_close", {})
    predict_fun = data.get("predict_fun", {})
    myriad = data.get("myriad_markets", {})
    web3_networks_raw = data.get("web3_networks", {})
    routes_raw = data.get("routes", {})
    execution_mode = _parse_execution_mode(data)
    web3_networks = {
        name: Web3NetworkConfig(
            rpc_url=_str_or_default(item.get("rpc_url") or _first_rpc_url(item.get("rpc_urls")), ""),
            rpc_urls=_parse_rpc_urls(item.get("rpc_urls"), _optional_str(item.get("rpc_url"))),
            chain_id=int(item["chain_id"]),
            max_slippage_pct=float(item.get("max_slippage_pct", 0.015)),
            max_priority_fee_gwei=float(
                item.get("max_priority_fee_gwei", _default_priority_fee_gwei(int(item["chain_id"])))
            ),
            confirmations=int(item.get("confirmations", 1)),
        )
        for name, item in web3_networks_raw.items()
    }
    bnb_network = web3_networks.get("bnb")

    return AppConfig(
        is_test=bool(data.get("isTest", True)),
        scan_all=scan_all,
        position_size_usd=float(data.get("position_size_usd", data.get("max_order_size_usd", 100.0))),
        max_order_size_usd=float(data.get("max_order_size_usd", 100.0)),
        min_net_spread=_fraction(
            data.get("min_net_spread", data.get("min_entry_spread_pct", 0.05)),
            "min_net_spread",
        ),
        poll_interval_ms=int(data.get("poll_interval_ms", 250)),
        polymarket_fill_timeout_ms=int(data.get("polymarket_fill_timeout_ms", 500)),
        predict_fun_fill_timeout_ms=int(data.get("predict_fun_fill_timeout_ms", 4_000)),
        myriad_fill_timeout_ms=int(data.get("myriad_fill_timeout_ms", data.get("predict_fun_fill_timeout_ms", 4_000))),
        signal_alert_cooldown_seconds=int(data.get("signal_alert_cooldown_seconds", 900)),
        categories_to_scan=[str(item) for item in data.get("categories_to_scan", ["sport"])],
        telegram=TelegramConfig(
            bot_token=_optional_str(data.get("telegram", {}).get("bot_token")),
            chat_id=_optional_str(data.get("telegram", {}).get("chat_id")),
            min_interval_seconds=float(data.get("telegram", {}).get("min_interval_seconds", 1.0)),
            log_raw_signal_books=bool(data.get("telegram", {}).get("log_raw_signal_books", False)),
        ),
        polymarket=PolymarketConfig(
            private_key=_optional_str(data.get("polymarket", {}).get("private_key")),
            api_base_url=_str_or_default(data.get("polymarket", {}).get("api_base_url"), "https://clob.polymarket.com"),
            chain_id=int(data.get("polymarket", {}).get("chain_id", 137)),
            signature_type=int(data.get("polymarket", {}).get("signature_type", 0)),
            funder=_optional_str(data.get("polymarket", {}).get("funder")),
            api_key=_optional_str(data.get("polymarket", {}).get("api_key")),
            api_secret=_optional_str(data.get("polymarket", {}).get("api_secret")),
            api_passphrase=_optional_str(data.get("polymarket", {}).get("api_passphrase")),
            max_slippage_pct=_fraction(
                data.get("polymarket", {}).get("max_slippage_pct", 0.015),
                "polymarket.max_slippage_pct",
            ),
            trading_fee_pct=_fraction(
                data.get("polymarket", {}).get("trading_fee_pct", 0.0),
                "polymarket.trading_fee_pct",
            ),
            rpc_url=_str_or_default(data.get("polymarket", {}).get("rpc_url"), "https://polygon-rpc.com"),
            rpc_urls=_parse_rpc_urls(
                data.get("polymarket", {}).get("rpc_urls"),
                _optional_str(data.get("polymarket", {}).get("rpc_url")) or "https://polygon-rpc.com",
            ),
            conditional_tokens_address=_str_or_default(
                data.get("polymarket", {}).get("conditional_tokens_address"),
                "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045",
            ),
            collateral_token_address=_str_or_default(
                data.get("polymarket", {}).get("collateral_token_address"),
                "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174",
            ),
            confirmations=int(data.get("polymarket", {}).get("confirmations", 2)),
            max_priority_fee_gwei=float(data.get("polymarket", {}).get("max_priority_fee_gwei", 30.0)),
            redemption_gas_limit=int(data.get("polymarket", {}).get("redemption_gas_limit", 350_000)),
        ),
        predict_fun=PredictFunConfig(
            enabled=bool(predict_fun.get("enabled", True)),
            private_key=_optional_str(predict_fun.get("private_key")),
            rpc_url=_str_or_default(
                predict_fun.get("rpc_url")
                or _first_rpc_url(predict_fun.get("rpc_urls"))
                or (bnb_network.rpc_url if bnb_network else None),
                "https://bsc-dataseed.binance.org",
            ),
            rpc_urls=_parse_rpc_urls(
                predict_fun.get("rpc_urls"),
                _optional_str(predict_fun.get("rpc_url"))
                or (bnb_network.rpc_url if bnb_network else "https://bsc-dataseed.binance.org"),
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
                or (
                    bnb_network.max_priority_fee_gwei
                    if bnb_network
                    else _default_priority_fee_gwei(int(predict_fun.get("chain_id", 56)))
                )
            ),
            confirmations=int(predict_fun.get("confirmations") or (bnb_network.confirmations if bnb_network else 1)),
            max_slippage_pct=float(predict_fun.get("max_slippage_pct", 0.015)),
        ),
        myriad_markets=MyriadMarketsConfig(
            api_url=_str_or_default(myriad.get("api_url"), "https://api-v2.myriadprotocol.com"),
            ws_url=_str_or_default(myriad.get("ws_url"), "wss://ws.myriadprotocol.com/ws"),
            api_key=_optional_str(myriad.get("api_key")),
            private_key=_optional_str(myriad.get("private_key")),
            rpc_url=_str_or_default(
                myriad.get("rpc_url")
                or _first_rpc_url(myriad.get("rpc_urls"))
                or (bnb_network.rpc_url if bnb_network else None),
                "https://bsc-dataseed.binance.org",
            ),
            rpc_urls=_parse_rpc_urls(
                myriad.get("rpc_urls"),
                _optional_str(myriad.get("rpc_url"))
                or (bnb_network.rpc_url if bnb_network else "https://bsc-dataseed.binance.org"),
            ),
            chain_id=int(myriad.get("chain_id", 56)),
            exchange_address=_str_or_default(
                myriad.get("exchange_address"), "0xa0b6f8ef8EdB64f395018D1933f2273Ce9f0f16A"
            ),
            conditional_tokens_address=_str_or_default(
                myriad.get("conditional_tokens_address"),
                "0x6413734f92248D4B29ae35883290BD93212654Dc",
            ),
            collateral_tokens={str(key): str(value) for key, value in myriad.get("collateral_tokens", {}).items()},
            collateral_symbol=str(myriad.get("collateral_symbol", "USDT")),
            trading_fee_pct=float(myriad.get("trading_fee_pct", 0.0)),
            max_slippage_pct=float(myriad.get("max_slippage_pct", 0.015)),
            enabled=bool(myriad.get("enabled", False)),
            order_book_ttl_ms=int(myriad.get("order_book_ttl_ms", 300)),
            websocket_stale_after_ms=int(myriad.get("websocket_stale_after_ms", 1_500)),
            confirmations=int(myriad.get("confirmations", bnb_network.confirmations if bnb_network else 3)),
            max_priority_fee_gwei=float(
                myriad.get("max_priority_fee_gwei", bnb_network.max_priority_fee_gwei if bnb_network else 2.0)
            ),
            redemption_gas_limit=int(myriad.get("redemption_gas_limit", 350_000)),
        ),
        web3_networks=web3_networks,
        auto_close=AutoCloseConfig(
            enabled=bool(auto_close.get("enabled", True)),
            exit_spread_pct=_fraction(
                auto_close.get("exit_spread_pct", data.get("early_exit_spread_threshold_pct", 0.015)),
                "auto_close.exit_spread_pct",
            ),
        ),
        markets=markets,
        enable_predict_fun=bool(data.get("enable_predict_fun", True)),
        min_market_volume_usd=float(data.get("min_market_volume_usd", 25_000.0)),
        min_entry_spread_pct=_fraction(
            data.get("min_net_spread", data.get("min_entry_spread_pct", 0.05)),
            "min_entry_spread_pct",
        ),
        min_retry_spread_pct=_fraction(data.get("min_retry_spread_pct", 0.05), "min_retry_spread_pct"),
        shadow_mode=bool(data.get("shadow_mode", True)),
        min_venue_balance_usd=float(data.get("min_venue_balance_usd", 50.0)),
        auto_rebalance_ratio_threshold=_fraction(
            data.get("auto_rebalance_ratio_threshold", 0.80),
            "auto_rebalance_ratio_threshold",
        ),
        enable_auto_rebalance=bool(data.get("enable_auto_rebalance", False)),
        max_consecutive_api_errors=int(data.get("max_consecutive_api_errors", 3)),
        max_daily_loss_usd=float(data.get("max_daily_loss_usd", 100.0)),
        max_open_positions=int(data.get("max_open_positions", 5)),
        spread_guard_floor=_fraction(data.get("spread_guard_floor", 0.05), "spread_guard_floor"),
        balance_refresh_interval_seconds=float(data.get("balance_refresh_interval_seconds", 5.0)),
        max_concurrent_market_evaluations=int(data.get("max_concurrent_market_evaluations", 100)),
        cancel_reconcile_timeout_ms=int(data.get("cancel_reconcile_timeout_ms", 1_000)),
        max_orderbook_age_seconds=float(data.get("max_orderbook_age_seconds", 2.0)),
        max_production_price_impact=_fraction(
            data.get("max_production_price_impact", 0.015),
            "max_production_price_impact",
        ),
        websocket_heartbeat_interval_seconds=float(data.get("websocket_heartbeat_interval_seconds", 30.0)),
        websocket_stale_after_seconds=float(data.get("websocket_stale_after_seconds", 10.0)),
        execution_mode=execution_mode,
        database_url=_optional_str(data.get("database_url") or os.getenv("DATABASE_URL")),
        routes=RouteConfig(
            polymarket_myriad=bool(routes_raw.get("polymarket_myriad", True)),
            polymarket_predict=bool(routes_raw.get("polymarket_predict", True)),
            predict_myriad=bool(routes_raw.get("predict_myriad", True)),
        ),
        reconciliation_orders_interval_seconds=float(data.get("reconciliation_orders_interval_seconds", 5.0)),
        reconciliation_full_interval_seconds=float(data.get("reconciliation_full_interval_seconds", 30.0)),
        market_data_snapshot_interval_seconds=float(data.get("market_data_snapshot_interval_seconds", 30.0)),
        max_total_notional_usd=float(data.get("max_total_notional_usd", 500.0)),
        max_venue_exposure_usd=float(data.get("max_venue_exposure_usd", 300.0)),
        max_market_exposure_usd=float(data.get("max_market_exposure_usd", 100.0)),
        max_orders_per_minute=int(data.get("max_orders_per_minute", 30)),
        max_unresolved_exposure_usd=float(data.get("max_unresolved_exposure_usd", 25.0)),
        observability_host=str(data.get("observability_host", "0.0.0.0")),
        observability_port=int(data.get("observability_port", 9108)),
        live_trading_confirmed=os.getenv("LIVE_TRADING_CONFIRM") == "YES",
        _execution_mode_explicit=True,
    )


def validate_config(
    config: AppConfig,
    *,
    require_resolved_markets: bool = False,
    require_verified_mappings: bool = True,
) -> None:
    errors: list[str] = []
    predict_active = config.enable_predict_fun and config.predict_fun.enabled and bool(config.predict_fun.api_key)
    live_execution = config.execution_mode.submits_orders
    if not config.routes.any_enabled():
        errors.append("at least one route must be enabled")
    if live_execution and not config.database_url:
        errors.append("DATABASE_URL is required for canary/live execution")
    if live_execution and not config.live_trading_confirmed:
        errors.append("LIVE_TRADING_CONFIRM=YES is required for canary/live execution")
    if config.execution_mode is ExecutionMode.CANARY:
        if config.position_size_usd > 20.0:
            errors.append("canary position_size_usd must not exceed $20 total ($10 per leg)")
        if config.max_open_positions > 1:
            errors.append("canary max_open_positions must be 1")
        if config.max_daily_loss_usd > 10.0:
            errors.append("canary max_daily_loss_usd must not exceed $10")
    if config.reconciliation_orders_interval_seconds <= 0:
        errors.append("reconciliation_orders_interval_seconds must be positive")
    if config.reconciliation_full_interval_seconds < config.reconciliation_orders_interval_seconds:
        errors.append("reconciliation_full_interval_seconds must be >= orders interval")
    if config.market_data_snapshot_interval_seconds <= 0:
        errors.append("market_data_snapshot_interval_seconds must be positive")
    if (
        min(
            config.max_total_notional_usd,
            config.max_venue_exposure_usd,
            config.max_market_exposure_usd,
            config.max_unresolved_exposure_usd,
        )
        <= 0
    ):
        errors.append("all production exposure limits must be positive")
    if config.max_orders_per_minute <= 0:
        errors.append("max_orders_per_minute must be positive")
    if not 1 <= config.observability_port <= 65535:
        errors.append("observability_port must be between 1 and 65535")
    if not config.markets and (not config.scan_all or require_resolved_markets):
        errors.append("markets must contain at least one market")
    if config.position_size_usd <= 0:
        errors.append("position_size_usd must be positive")
    if config.max_order_size_usd <= 0:
        errors.append("max_order_size_usd must be positive")
    if config.position_size_usd > config.max_order_size_usd:
        errors.append("position_size_usd must not exceed max_order_size_usd")
    if config.min_net_spread <= 0:
        errors.append("min_net_spread must be positive")
    if config.min_retry_spread_pct <= 0 or config.min_retry_spread_pct > config.min_entry_spread_pct:
        errors.append("min_retry_spread_pct must be positive and no greater than min_entry_spread_pct")
    if config.min_market_volume_usd < 0:
        errors.append("min_market_volume_usd must be non-negative")
    if config.max_consecutive_api_errors <= 0:
        errors.append("max_consecutive_api_errors must be positive")
    if config.enable_auto_rebalance:
        errors.append("enable_auto_rebalance=true is unsupported; bridge execution is intentionally disabled")
    if config.max_daily_loss_usd <= 0:
        errors.append("max_daily_loss_usd must be positive")
    if config.max_open_positions <= 0:
        errors.append("max_open_positions must be positive")
    if config.balance_refresh_interval_seconds <= 0:
        errors.append("balance_refresh_interval_seconds must be positive")
    if config.max_concurrent_market_evaluations <= 0:
        errors.append("max_concurrent_market_evaluations must be positive")
    if config.cancel_reconcile_timeout_ms < 100:
        errors.append("cancel_reconcile_timeout_ms must be at least 100")
    if not 1.5 <= config.max_orderbook_age_seconds <= 2.0:
        errors.append("max_orderbook_age_seconds must be between 1.5 and 2.0")
    if not 0 < config.max_production_price_impact <= 0.05:
        errors.append("max_production_price_impact must be between 0 and 0.05")
    if config.websocket_heartbeat_interval_seconds <= 0:
        errors.append("websocket_heartbeat_interval_seconds must be positive")
    if config.websocket_stale_after_seconds <= 0:
        errors.append("websocket_stale_after_seconds must be positive")
    if config.predict_fun.max_slippage_pct <= 0:
        errors.append("predict_fun.max_slippage_pct must be positive")
    if config.polymarket.max_slippage_pct <= 0:
        errors.append("polymarket.max_slippage_pct must be positive")
    if config.polymarket.confirmations < 1:
        errors.append("polymarket.confirmations must be at least 1")
    if config.polymarket.redemption_gas_limit <= 0:
        errors.append("polymarket.redemption_gas_limit must be positive")
    if config.myriad_markets.max_slippage_pct <= 0:
        errors.append("myriad_markets.max_slippage_pct must be positive")
    configured_slippages = {
        "polymarket": config.polymarket.max_slippage_pct,
        "predict_fun": config.predict_fun.max_slippage_pct,
        "myriad_markets": config.myriad_markets.max_slippage_pct,
    }
    for venue, configured_slippage in configured_slippages.items():
        if configured_slippage > config.max_production_price_impact:
            LOGGER.warning(
                "configured_slippage_capped_by_safety_limit",
                extra={
                    "_venue": venue,
                    "_configured": configured_slippage,
                    "_effective": config.max_production_price_impact,
                },
            )
    if not 0 <= config.predict_fun.fee_rate_bps < 10_000:
        errors.append("predict_fun.fee_rate_bps must be between 0 and 9999")
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
    if not predict_active and not config.myriad_markets.enabled:
        errors.append("at least one hedge venue must be active: Predict.fun with API key or Myriad")
    if config.myriad_markets.enabled:
        if not 50 <= config.myriad_markets.order_book_ttl_ms <= 1_500:
            errors.append("myriad_markets.order_book_ttl_ms must be between 50 and 1500")
        if config.myriad_markets.websocket_stale_after_ms < config.myriad_markets.order_book_ttl_ms:
            errors.append("myriad_markets.websocket_stale_after_ms must be >= order_book_ttl_ms")
        if live_execution and not config.myriad_markets.private_key:
            errors.append("MYRIAD_PRIVATE_KEY is required when myriad_markets.enabled=true")
        if config.myriad_markets.chain_id != 56:
            errors.append("myriad_markets.chain_id must be 56")
        if not 0 <= config.myriad_markets.trading_fee_pct < 1:
            errors.append("myriad_markets.trading_fee_pct must be between 0 and 1")
        if config.myriad_markets.collateral_symbol not in config.myriad_markets.collateral_tokens:
            errors.append("myriad_markets.collateral_symbol must exist in myriad_markets.collateral_tokens")
        if config.myriad_markets.confirmations < 1:
            errors.append("myriad_markets.confirmations must be at least 1")
        if config.myriad_markets.redemption_gas_limit <= 0:
            errors.append("myriad_markets.redemption_gas_limit must be positive")
    for name, network in config.web3_networks.items():
        if not network.rpc_url and config.execution_mode.submits_orders:
            errors.append(f"web3_networks.{name}.rpc_url is required")
        if network.chain_id <= 0:
            errors.append(f"web3_networks.{name}.chain_id must be positive")
        if network.confirmations < 0:
            errors.append(f"web3_networks.{name}.confirmations must be non-negative")

    for index, market in enumerate(config.markets):
        prefix = f"markets[{index}]"
        if live_execution and require_verified_mappings and market.mapping_status is not MappingStatus.VERIFIED:
            errors.append(f"{prefix}.mapping_status must be VERIFIED for canary/live execution")
        if live_execution and require_verified_mappings and not market.verified_routes:
            errors.append(f"{prefix}.verified_routes must contain at least one approved route")
        has_discovery_terms = bool(market.symbol and market.target_label)
        if market.predict_fun_side == market.polymarket_side:
            errors.append(f"{prefix}.predict_fun_side must be opposite to polymarket_side")
        if market.myriad_market_id and market.myriad_side == market.polymarket_side:
            errors.append(f"{prefix}.myriad_side must be opposite to polymarket_side")
        if not config.scan_all and (
            (require_resolved_markets or not has_discovery_terms)
            and (not market.polymarket_token_id or market.polymarket_token_id.startswith("replace-with"))
        ):
            errors.append(f"{prefix}.polymarket_token_id or discovery fields symbol/target_label are required")
        if (
            predict_active
            and not config.scan_all
            and (
                (require_resolved_markets or not has_discovery_terms)
                and (not market.predict_fun_token_id or market.predict_fun_token_id.startswith("replace-with"))
                and market.predict_fun_amm_pool is None
            )
        ):
            errors.append(f"{prefix}.predict_fun_token_id or predict_fun_amm_pool is required")
        if (
            config.auto_close.enabled
            and market.expires_at is None
            and (require_resolved_markets or not has_discovery_terms)
        ):
            errors.append(f"{prefix}.expires_at is required when auto_close.enabled=true")
        if (
            config.myriad_markets.enabled
            and require_resolved_markets
            and (not market.myriad_market_id or market.myriad_market_id.startswith("replace-with"))
        ):
            errors.append(f"{prefix}.myriad_market_id or discovery fields symbol/target_label are required")

    if live_execution and require_verified_mappings:
        route_coverage = {
            route
            for market in config.markets
            for route in market.verified_routes
            if market.mapping_status is MappingStatus.VERIFIED
        }
        enabled_routes = {
            "polymarket_myriad": config.routes.polymarket_myriad,
            "polymarket_predict": config.routes.polymarket_predict,
            "predict_myriad": config.routes.predict_myriad,
        }
        for route, enabled in enabled_routes.items():
            if enabled and route not in route_coverage:
                errors.append(f"enabled route {route} has no VERIFIED market mapping")

    if live_execution:
        if not config.polymarket.private_key:
            errors.append("POLYMARKET_PRIVATE_KEY is required when isTest=false")
        elif not _is_private_key(config.polymarket.private_key):
            errors.append("POLYMARKET_PRIVATE_KEY must be a 64 hex character ECDSA key, with optional 0x prefix")
        if not config.polymarket.rpc_url:
            errors.append("POLYGON_RPC_URL or polymarket.rpc_url is required when isTest=false")
        if not config.polymarket.conditional_tokens_address or not config.polymarket.collateral_token_address:
            errors.append("Polymarket Conditional Tokens and collateral addresses are required")
        if predict_active and not config.predict_fun.private_key:
            errors.append("PREDICT_FUN_PRIVATE_KEY is required when isTest=false")
        elif predict_active and config.predict_fun.private_key and not _is_private_key(config.predict_fun.private_key):
            errors.append("PREDICT_FUN_PRIVATE_KEY must be a 64 hex character ECDSA key, with optional 0x prefix")
        if predict_active and not config.predict_fun.rpc_url:
            errors.append("BNB_RPC_URL or predict_fun.rpc_url is required when isTest=false")
        if predict_active and not config.predict_fun.api_base_url:
            errors.append("predict_fun.api_base_url is required when isTest=false")
        if predict_active and not config.predict_fun.market_abi_path and not config.predict_fun.api_base_url:
            errors.append("predict_fun.market_abi_path or api_base_url is required for price reads when isTest=false")
        if config.polymarket.signature_type != 0 and not config.polymarket.funder:
            errors.append("POLYMARKET_FUNDER_ADDRESS is required for non-EOA signature types")
        polymarket_api_creds = (
            config.polymarket.api_key,
            config.polymarket.api_secret,
            config.polymarket.api_passphrase,
        )
        if any(polymarket_api_creds) and not all(polymarket_api_creds):
            errors.append(
                "Polymarket API credentials must include api_key, api_secret, and api_passphrase together"
            )
        if (
            config.myriad_markets.enabled
            and config.myriad_markets.private_key
            and not _is_private_key(config.myriad_markets.private_key)
        ):
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


def _is_scan_all_filter(value: Any) -> bool:
    if not isinstance(value, list) or not value:
        return True
    return any(isinstance(item, dict) and str(item.get("symbol", "")).strip() in {"", "*"} for item in value)


def _parse_execution_mode(data: dict[str, Any]) -> ExecutionMode:
    raw = data.get("execution_mode")
    if raw not in (None, ""):
        try:
            return ExecutionMode(str(raw).lower())
        except ValueError as exc:
            raise ValueError("execution_mode must be paper, shadow, canary, or live") from exc
    if bool(data.get("isTest", True)):
        return ExecutionMode.PAPER
    if bool(data.get("shadow_mode", True)):
        return ExecutionMode.SHADOW
    return ExecutionMode.LIVE
