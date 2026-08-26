from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlsplit, urlunsplit

from dotenv import load_dotenv

from .market_mapping import normalize_launch_category
from .models import (
    AmmPool,
    BinarySide,
    ExecutionMode,
    MappingStatus,
    MarketSpec,
    execution_route_for_market,
    market_supports_execution_route,
    route_execution_sides_are_complementary,
)

LOGGER = logging.getLogger(__name__)

_ENV_FALLBACKS: dict[str, tuple[str, ...]] = {
    "SX_BET_API_KEY": ("SX_API_KEY",),
    "SX_BET_PRIVATE_KEY": ("SX_PRIVATE_KEY",),
    "SX_BET_BASE_TOKEN_ADDRESS": ("SX_BASE_TOKEN_ADDRESS",),
}

_DATABASE_HOST_OVERRIDE_ENV = "ARBITRAGE_DATABASE_HOST_OVERRIDE"
_DATABASE_PORT_OVERRIDE_ENV = "ARBITRAGE_DATABASE_PORT_OVERRIDE"
_EXECUTION_MODE_OVERRIDE_ENV = "ARBITRAGE_EXECUTION_MODE_OVERRIDE"


@dataclass(frozen=True)
class TelegramConfig:
    bot_token: str | None = field(repr=False)
    chat_id: str | None
    min_interval_seconds: float = 1.0
    log_raw_signal_books: bool = False


@dataclass(frozen=True)
class PolymarketConfig:
    private_key: str | None = field(repr=False)
    api_base_url: str
    chain_id: int
    signature_type: int
    funder: str | None
    api_key: str | None = field(default=None, repr=False)
    api_secret: str | None = field(default=None, repr=False)
    api_passphrase: str | None = field(default=None, repr=False)
    max_slippage_pct: float = 0.015
    trading_fee_pct: float = 0.0
    rpc_url: str = field(default="https://polygon-rpc.com", repr=False)
    rpc_urls: list[str] = field(default_factory=list, repr=False)
    conditional_tokens_address: str = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
    collateral_token_address: str = "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB"
    confirmations: int = 2
    max_priority_fee_gwei: float = 30.0
    redemption_gas_limit: int = 350_000


@dataclass(frozen=True)
class PredictFunConfig:
    enabled: bool
    private_key: str | None = field(repr=False)
    rpc_url: str = field(repr=False)
    rpc_urls: list[str] = field(repr=False)
    chain_id: int
    network: str
    api_base_url: str | None
    api_key: str | None = field(repr=False)
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
    account_address: str | None = None


@dataclass(frozen=True)
class SxBetConfig:
    enabled: bool
    api_base_url: str
    api_key: str | None = field(repr=False)
    private_key: str | None = field(repr=False)
    rpc_url: str = field(repr=False)
    rpc_urls: list[str] = field(repr=False)
    chain_id: int
    ws_url: str = "wss://realtime.sx.bet/connection/websocket"
    base_token_address: str | None = None
    domain_version: str | None = None
    odds_slippage: int = 0
    taker_fee_bps: int = 0
    minimum_notional_usd: float = 1.0
    max_slippage_pct: float = 0.015
    api_version: str = "v2"
    environment: str = "mainnet"
    time_in_force: str = "FOK"
    allow_v3_mainnet: bool = False


@dataclass(frozen=True)
class MyriadMarketsConfig:
    api_url: str
    ws_url: str
    api_key: str | None = field(repr=False)
    private_key: str | None = field(repr=False)
    rpc_url: str = field(repr=False)
    rpc_urls: list[str] = field(repr=False)
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
    rpc_url: str = field(repr=False)
    rpc_urls: list[str] = field(repr=False)
    chain_id: int
    max_slippage_pct: float
    max_priority_fee_gwei: float
    confirmations: int


@dataclass(frozen=True)
class AutoCloseConfig:
    enabled: bool
    exit_spread_pct: float


@dataclass(frozen=True)
class SpreadPolicy:
    """Route-aware entry economics shared by discovery, signal, and preflight paths."""

    route_floors: dict[str, float] = field(
        default_factory=lambda: {
            "polymarket_sx": 0.015,
            "polymarket_predict": 0.025,
            "polymarket_myriad": 0.025,
        }
    )
    min_expected_profit_usd: float = 0.50
    depth_buffer: float = 1.25
    adverse_move_p95_pct: float = 0.0
    adverse_move_p95_pct_by_route: dict[str, float] = field(default_factory=dict)
    safety_buffer_pct: float = 0.0025
    fixed_chain_cost_usd: float = 0.0
    fixed_chain_cost_usd_by_route: dict[str, float] = field(default_factory=dict)
    gas_units_by_route: dict[str, dict[str, int]] = field(default_factory=dict)
    native_token_usd_ceiling_by_chain: dict[str, float] = field(default_factory=dict)
    gas_price_multiplier: float = 1.5
    gas_quote_ttl_seconds: float = 15.0
    require_live_gas_estimate: bool = False

    def threshold_for(self, route: str) -> float:
        floor = self.route_floors.get(route, 0.0)
        adverse_move = self.adverse_move_p95_pct_by_route.get(route, self.adverse_move_p95_pct)
        return max(floor, adverse_move + self.safety_buffer_pct)

    def fixed_chain_cost_for(self, route: str) -> float:
        return self.fixed_chain_cost_usd_by_route.get(route, self.fixed_chain_cost_usd)

    def has_route_calibration(self, route: str) -> bool:
        return self.adverse_move_p95_pct_by_route.get(route, 0.0) > 0


@dataclass(frozen=True)
class RouteConfig:
    polymarket_myriad: bool = True
    polymarket_predict: bool = True
    predict_myriad: bool = True
    predict_sx: bool = False
    polymarket_sx: bool = False
    sx_myriad: bool = False

    def any_enabled(self) -> bool:
        return any(
            (
                self.polymarket_myriad,
                self.polymarket_predict,
                self.predict_myriad,
                self.predict_sx,
                self.polymarket_sx,
                self.sx_myriad,
            )
        )


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
    sx_bet_fill_timeout_ms: int
    myriad_fill_timeout_ms: int
    signal_alert_cooldown_seconds: int
    categories_to_scan: list[str]
    telegram: TelegramConfig
    polymarket: PolymarketConfig
    predict_fun: PredictFunConfig
    sx_bet: SxBetConfig
    myriad_markets: MyriadMarketsConfig
    web3_networks: dict[str, Web3NetworkConfig]
    auto_close: AutoCloseConfig
    markets: list[MarketSpec]
    market_horizon_filter_enabled: bool = False
    max_sports_market_horizon_hours: float = 48.0
    max_crypto_market_horizon_hours: float = 24.0
    max_market_horizon_hours_by_category: dict[str, float] = field(default_factory=dict)
    spread_policy: SpreadPolicy = field(default_factory=SpreadPolicy)
    enable_predict_fun: bool = False
    enable_sx_bet: bool = False
    min_market_volume_usd: float = 25_000.0
    min_entry_spread_pct: float = 0.05
    min_retry_spread_pct: float = 0.05
    shadow_mode: bool = True
    shadow_require_verified_mappings: bool = False
    min_venue_balance_usd: float = 50.0
    auto_rebalance_ratio_threshold: float = 0.80
    enable_auto_rebalance: bool = False
    max_consecutive_api_errors: int = 3
    max_daily_loss_usd: float = 100.0
    max_open_positions: int = 5
    spread_guard_floor: float = 0.05
    balance_refresh_interval_seconds: float = 5.0
    max_concurrent_market_evaluations: int = 100
    shadow_preflight_samples: int = 1
    shadow_preflight_sample_interval_seconds: float = 0.15
    shadow_preflight_cooldown_seconds: float = 30.0
    shadow_preflight_evidence_ttl_seconds: float = 900.0
    market_data_target_hold_seconds: float = 0.0
    market_data_target_hold_seconds_by_route: dict[str, float] = field(default_factory=dict)
    market_data_executable_priority_seconds: float = 0.0
    market_data_executable_priority_seconds_by_route: dict[str, float] = field(default_factory=dict)
    market_data_exploration_fraction: float = 0.25
    market_data_exploration_fraction_by_route: dict[str, float] = field(default_factory=dict)
    market_data_prefetch_multiplier_by_route: dict[str, int] = field(default_factory=dict)
    market_evaluation_weight_by_route: dict[str, int] = field(default_factory=dict)
    discovery_max_stale_seconds: float = 900.0
    cancel_reconcile_timeout_ms: int = 1_000
    max_orderbook_age_seconds: float = 2.0
    max_production_price_impact: float = 0.015
    websocket_heartbeat_interval_seconds: float = 30.0
    websocket_stale_after_seconds: float = 10.0
    execution_mode: ExecutionMode = ExecutionMode.PAPER
    database_url: str | None = field(default=None, repr=False)
    runtime_instance_id: str = "global"
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

    def market_data_target_hold_for(self, route: str) -> float:
        return self.market_data_target_hold_seconds_by_route.get(route, self.market_data_target_hold_seconds)

    def market_data_executable_priority_for(self, route: str) -> float:
        configured = self.market_data_executable_priority_seconds_by_route.get(route)
        if configured is not None:
            return configured
        if self.market_data_executable_priority_seconds > 0:
            return self.market_data_executable_priority_seconds
        return self.market_data_target_hold_for(route)

    def market_data_exploration_fraction_for(self, route: str) -> float:
        return self.market_data_exploration_fraction_by_route.get(
            route,
            self.market_data_exploration_fraction,
        )

    def market_data_prefetch_multiplier_for(self, route: str) -> int:
        return self.market_data_prefetch_multiplier_by_route.get(route, 1)

    def market_evaluation_weight_for(self, route: str) -> int:
        return self.market_evaluation_weight_by_route.get(route, 1)


def _expand_env(value: Any) -> Any:
    if isinstance(value, str) and value.startswith("${") and value.endswith("}"):
        key = value[2:-1]
        direct = os.getenv(key)
        if direct not in (None, ""):
            return direct
        for alias in _ENV_FALLBACKS.get(key, ()):
            fallback = os.getenv(alias)
            if fallback not in (None, ""):
                return fallback
        return direct
    if isinstance(value, dict):
        return {key: _expand_env(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_expand_env(item) for item in value]
    return value


def _optional_str(value: Any) -> str | None:
    if value in (None, ""):
        return None
    normalized = str(value).strip()
    return normalized or None


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


def _database_url_with_overrides(value: str | None) -> str | None:
    if not value:
        return value
    host_override = _optional_str(os.getenv(_DATABASE_HOST_OVERRIDE_ENV))
    port_override = _optional_str(os.getenv(_DATABASE_PORT_OVERRIDE_ENV))
    if host_override is None and port_override is None:
        return value
    parsed = urlsplit(value)
    hostname = host_override or parsed.hostname
    if hostname is None:
        return value
    port = port_override or (str(parsed.port) if parsed.port is not None else None)
    userinfo = ""
    if parsed.username is not None:
        userinfo = quote(parsed.username, safe="")
        if parsed.password is not None:
            userinfo += f":{quote(parsed.password, safe='')}"
        userinfo += "@"
    host = hostname if ":" not in hostname or hostname.startswith("[") else f"[{hostname}]"
    netloc = f"{userinfo}{host}"
    if port is not None:
        netloc += f":{port}"
    return urlunsplit((parsed.scheme, netloc, parsed.path, parsed.query, parsed.fragment))


def load_operator_env(config_path: str | Path) -> None:
    config_file = Path(config_path)
    config_dir = config_file.parent
    config_name = config_file.name.lower()
    candidates: list[Path]
    if config_name == "config.production.json" or ".production." in config_name:
        candidates = [
            config_dir / ".env.production",
            config_dir / ".env",
            Path.cwd() / ".env.production",
            Path.cwd() / ".env",
        ]
    else:
        candidates = [
            config_dir / ".env",
            Path.cwd() / ".env",
        ]
    seen: set[Path] = set()
    for candidate in candidates:
        resolved = candidate.resolve()
        if resolved in seen or not resolved.is_file():
            continue
        try:
            load_dotenv(resolved, override=False)
        except PermissionError:
            # Compose already injects env_file values into operator containers.
            # Keep the host file root-only instead of requiring weaker permissions.
            LOGGER.warning(
                "operator_env_file_unreadable_using_process_environment",
                extra={"_path": str(resolved)},
            )
        seen.add(resolved)


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
            predict_fun_token_id=str(item.get("second_leg_token_id") or item.get("predict_fun_token_id") or ""),
            predict_fun_side=BinarySide(str(item.get("second_leg_side") or item.get("predict_fun_side") or "NO")),
            venue_a_label=str(item.get("venue_a_label") or "Polymarket"),
            venue_b_label=str(item.get("second_venue_label") or item.get("venue_b_label") or "Predict.fun"),
            expires_at=_parse_datetime(item.get("expires_at")),
            condition_id=item.get("condition_id"),
            polymarket_market_id=_optional_str(item.get("polymarket_market_id")),
            polymarket_url=_optional_str(item.get("polymarket_url")),
            tick_size=item.get("tick_size"),
            neg_risk=item.get("neg_risk"),
            predict_fun_neg_risk=item.get("second_leg_neg_risk", item.get("predict_fun_neg_risk")),
            predict_fun_fee_rate_bps=(
                int(item["second_leg_fee_rate_bps"])
                if item.get("second_leg_fee_rate_bps") is not None
                else int(item["predict_fun_fee_rate_bps"])
                if item.get("predict_fun_fee_rate_bps") is not None
                else None
            ),
            predict_fun_price_precision=(
                int(item["second_leg_price_precision"])
                if item.get("second_leg_price_precision") is not None
                else int(item["predict_fun_price_precision"])
                if item.get("predict_fun_price_precision") is not None
                else None
            ),
            predict_fun_market_id=item.get("second_leg_market_id", item.get("predict_fun_market_id")),
            predict_fun_url=_optional_str(item.get("second_leg_url", item.get("predict_fun_url"))),
            predict_fun_amm_pool=_parse_amm_pool(item.get("second_leg_amm_pool", item.get("predict_fun_amm_pool"))),
            myriad_market_id=_optional_str(item.get("myriad_market_id")),
            myriad_url=_optional_str(item.get("myriad_url")),
            myriad_side=BinarySide(str(item.get("myriad_side") or "NO")),
            rules_fingerprint=item.get("rules_fingerprint"),
            polymarket_volume_usd=_optional_float(item.get("polymarket_volume_usd")),
            predict_fun_volume_usd=_optional_float(
                item.get("second_leg_volume_usd", item.get("predict_fun_volume_usd"))
            ),
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
    spread_policy = data.get("spread_policy", {})
    predict_fun = data.get("predict_fun", {})
    sx_bet = data.get("sx_bet", {})
    myriad = data.get("myriad_markets", {})
    web3_networks_raw = data.get("web3_networks", {})
    routes_raw = data.get("routes", {})
    execution_mode = _parse_execution_mode(data)
    database_url = _database_url_with_overrides(_optional_str(data.get("database_url") or os.getenv("DATABASE_URL")))
    runtime_instance_id = _optional_str(data.get("runtime_instance_id") or os.getenv("ARBITRAGE_RUNTIME_INSTANCE_ID"))
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
        sx_bet_fill_timeout_ms=int(data.get("sx_bet_fill_timeout_ms", data.get("predict_fun_fill_timeout_ms", 4_000))),
        myriad_fill_timeout_ms=int(data.get("myriad_fill_timeout_ms", data.get("predict_fun_fill_timeout_ms", 4_000))),
        signal_alert_cooldown_seconds=int(data.get("signal_alert_cooldown_seconds", 900)),
        categories_to_scan=[str(item) for item in data.get("categories_to_scan", ["sport"])],
        market_horizon_filter_enabled=bool(data.get("market_horizon_filter_enabled", False)),
        max_sports_market_horizon_hours=float(data.get("max_sports_market_horizon_hours", 48.0)),
        max_crypto_market_horizon_hours=float(data.get("max_crypto_market_horizon_hours", 24.0)),
        max_market_horizon_hours_by_category={
            str(category): float(hours)
            for category, hours in dict(data.get("max_market_horizon_hours_by_category", {})).items()
        },
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
                "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB",
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
            account_address=_optional_str(predict_fun.get("account_address")),
        ),
        sx_bet=SxBetConfig(
            enabled=bool(sx_bet.get("enabled", False)),
            api_base_url=_str_or_default(sx_bet.get("api_base_url"), "https://api.sx.bet"),
            api_key=_optional_str(sx_bet.get("api_key")),
            private_key=_optional_str(sx_bet.get("private_key")),
            rpc_url=_str_or_default(
                sx_bet.get("rpc_url") or _first_rpc_url(sx_bet.get("rpc_urls")),
                "https://rpc-rollup.sx.technology",
            ),
            rpc_urls=_parse_rpc_urls(
                sx_bet.get("rpc_urls"),
                _optional_str(sx_bet.get("rpc_url")) or "https://rpc-rollup.sx.technology",
            ),
            chain_id=int(sx_bet.get("chain_id", 4162)),
            ws_url=_str_or_default(
                sx_bet.get("ws_url"),
                "wss://realtime.sx.bet/connection/websocket",
            ),
            base_token_address=_optional_str(sx_bet.get("base_token_address")),
            domain_version=_optional_str(sx_bet.get("domain_version")),
            odds_slippage=int(sx_bet.get("odds_slippage", 0)),
            taker_fee_bps=int(sx_bet.get("taker_fee_bps", 0)),
            minimum_notional_usd=float(sx_bet.get("minimum_notional_usd", 1.0)),
            max_slippage_pct=_fraction(sx_bet.get("max_slippage_pct", 0.015), "sx_bet.max_slippage_pct"),
            api_version=str(sx_bet.get("api_version", "v2")).lower(),
            environment=str(sx_bet.get("environment", "mainnet")).lower(),
            time_in_force=str(sx_bet.get("time_in_force", "FOK")).upper(),
            allow_v3_mainnet=_strict_bool(
                sx_bet.get("allow_v3_mainnet", False),
                "sx_bet.allow_v3_mainnet",
            ),
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
        spread_policy=SpreadPolicy(
            route_floors={
                str(route): _fraction(value, f"spread_policy.route_floors.{route}")
                for route, value in dict(spread_policy.get("route_floors", {})).items()
            }
            or SpreadPolicy().route_floors,
            min_expected_profit_usd=float(spread_policy.get("min_expected_profit_usd", 0.50)),
            depth_buffer=float(spread_policy.get("depth_buffer", 1.25)),
            adverse_move_p95_pct=_fraction(
                spread_policy.get("adverse_move_p95_pct", 0.0),
                "spread_policy.adverse_move_p95_pct",
            ),
            adverse_move_p95_pct_by_route={
                str(route): _fraction(value, f"spread_policy.adverse_move_p95_pct_by_route.{route}")
                for route, value in dict(spread_policy.get("adverse_move_p95_pct_by_route", {})).items()
            },
            safety_buffer_pct=_fraction(
                spread_policy.get("safety_buffer_pct", 0.0025),
                "spread_policy.safety_buffer_pct",
            ),
            fixed_chain_cost_usd=float(spread_policy.get("fixed_chain_cost_usd", 0.0)),
            fixed_chain_cost_usd_by_route={
                str(route): float(value)
                for route, value in dict(spread_policy.get("fixed_chain_cost_usd_by_route", {})).items()
            },
            gas_units_by_route={
                str(route): {str(chain_id): int(units) for chain_id, units in dict(chains).items()}
                for route, chains in dict(spread_policy.get("gas_units_by_route", {})).items()
            },
            native_token_usd_ceiling_by_chain={
                str(chain_id): float(value)
                for chain_id, value in dict(
                    spread_policy.get("native_token_usd_ceiling_by_chain", {})
                ).items()
            },
            gas_price_multiplier=float(spread_policy.get("gas_price_multiplier", 1.5)),
            gas_quote_ttl_seconds=float(spread_policy.get("gas_quote_ttl_seconds", 15.0)),
            require_live_gas_estimate=bool(spread_policy.get("require_live_gas_estimate", False)),
        ),
        enable_predict_fun=bool(data.get("enable_predict_fun", True)),
        enable_sx_bet=bool(data.get("enable_sx_bet", False)),
        min_market_volume_usd=float(data.get("min_market_volume_usd", 25_000.0)),
        min_entry_spread_pct=_fraction(
            data.get("min_net_spread", data.get("min_entry_spread_pct", 0.05)),
            "min_entry_spread_pct",
        ),
        min_retry_spread_pct=_fraction(data.get("min_retry_spread_pct", 0.05), "min_retry_spread_pct"),
        shadow_mode=bool(data.get("shadow_mode", True)),
        shadow_require_verified_mappings=_json_bool(
            data.get("shadow_require_verified_mappings", False),
            "shadow_require_verified_mappings",
        ),
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
        shadow_preflight_samples=int(data.get("shadow_preflight_samples", 1)),
        shadow_preflight_sample_interval_seconds=float(
            data.get("shadow_preflight_sample_interval_seconds", 0.15)
        ),
        shadow_preflight_cooldown_seconds=float(
            data.get("shadow_preflight_cooldown_seconds", 30.0)
        ),
        shadow_preflight_evidence_ttl_seconds=float(
            data.get("shadow_preflight_evidence_ttl_seconds", 900.0)
        ),
        market_data_target_hold_seconds=float(data.get("market_data_target_hold_seconds", 0.0)),
        market_data_target_hold_seconds_by_route={
            str(route): float(seconds)
            for route, seconds in dict(data.get("market_data_target_hold_seconds_by_route", {})).items()
        },
        market_data_executable_priority_seconds=float(
            data.get("market_data_executable_priority_seconds", 0.0)
        ),
        market_data_executable_priority_seconds_by_route={
            str(route): float(seconds)
            for route, seconds in dict(
                data.get("market_data_executable_priority_seconds_by_route", {})
            ).items()
        },
        market_data_exploration_fraction=_fraction(
            data.get("market_data_exploration_fraction", 0.25),
            "market_data_exploration_fraction",
        ),
        market_data_exploration_fraction_by_route={
            str(route): _fraction(
                fraction,
                f"market_data_exploration_fraction_by_route.{route}",
            )
            for route, fraction in dict(data.get("market_data_exploration_fraction_by_route", {})).items()
        },
        market_data_prefetch_multiplier_by_route={
            str(route): int(multiplier)
            for route, multiplier in dict(data.get("market_data_prefetch_multiplier_by_route", {})).items()
        },
        market_evaluation_weight_by_route={
            str(route): int(weight)
            for route, weight in dict(data.get("market_evaluation_weight_by_route", {})).items()
        },
        discovery_max_stale_seconds=float(data.get("discovery_max_stale_seconds", 900.0)),
        cancel_reconcile_timeout_ms=int(data.get("cancel_reconcile_timeout_ms", 1_000)),
        max_orderbook_age_seconds=float(data.get("max_orderbook_age_seconds", 2.0)),
        max_production_price_impact=_fraction(
            data.get("max_production_price_impact", 0.015),
            "max_production_price_impact",
        ),
        websocket_heartbeat_interval_seconds=float(data.get("websocket_heartbeat_interval_seconds", 30.0)),
        websocket_stale_after_seconds=float(data.get("websocket_stale_after_seconds", 10.0)),
        execution_mode=execution_mode,
        database_url=database_url,
        runtime_instance_id=runtime_instance_id or "global",
        routes=RouteConfig(
            polymarket_myriad=bool(routes_raw.get("polymarket_myriad", True)),
            polymarket_predict=bool(routes_raw.get("polymarket_predict", True)),
            predict_myriad=bool(routes_raw.get("predict_myriad", True)),
            predict_sx=bool(routes_raw.get("predict_sx", False)),
            polymarket_sx=bool(routes_raw.get("polymarket_sx", False)),
            sx_myriad=bool(routes_raw.get("sx_myriad", False)),
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
        live_trading_confirmed=bool(data.get("live_trading_confirmed", data.get("live_trading_confirm", False)))
        or os.getenv("LIVE_TRADING_CONFIRM") == "YES",
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
    sx_active = config.enable_sx_bet and config.sx_bet.enabled
    live_execution = config.execution_mode.submits_orders
    predict_routes_enabled = (
        config.routes.polymarket_predict or config.routes.predict_myriad or config.routes.predict_sx
    )
    sx_routes_enabled = config.routes.polymarket_sx or config.routes.sx_myriad or config.routes.predict_sx
    predict_required = predict_active and predict_routes_enabled
    sx_required = sx_active and sx_routes_enabled
    myriad_required = config.myriad_markets.enabled and (
        config.routes.polymarket_myriad or config.routes.predict_myriad or config.routes.sx_myriad
    )
    second_routes_enabled = predict_routes_enabled or sx_routes_enabled
    if not config.routes.any_enabled():
        errors.append("at least one route must be enabled")
    if live_execution and not config.database_url:
        errors.append("DATABASE_URL is required for canary/live execution")
    if live_execution and not str(config.runtime_instance_id).strip():
        errors.append("runtime_instance_id is required for canary/live execution")
    if live_execution and not config.live_trading_confirmed:
        errors.append("LIVE_TRADING_CONFIRM=YES is required for canary/live execution")
    if config.execution_mode is ExecutionMode.CANARY:
        if config.position_size_usd > 50.0:
            errors.append("canary position_size_usd must not exceed $50 total ($25 per leg)")
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
    if config.spread_policy.min_expected_profit_usd <= 0:
        errors.append("spread_policy.min_expected_profit_usd must be positive")
    if config.spread_policy.depth_buffer < 1.0:
        errors.append("spread_policy.depth_buffer must be at least 1")
    if not 0 <= config.spread_policy.adverse_move_p95_pct < 1:
        errors.append("spread_policy.adverse_move_p95_pct must be between 0 and 1")
    for route, adverse_move in config.spread_policy.adverse_move_p95_pct_by_route.items():
        if not route.strip() or not 0 < adverse_move < 1:
            errors.append("spread_policy.adverse_move_p95_pct_by_route values must be between 0 and 1")
    if not 0 <= config.spread_policy.safety_buffer_pct < 1:
        errors.append("spread_policy.safety_buffer_pct must be between 0 and 1")
    if config.spread_policy.fixed_chain_cost_usd < 0:
        errors.append("spread_policy.fixed_chain_cost_usd must be non-negative")
    for route, fixed_cost in config.spread_policy.fixed_chain_cost_usd_by_route.items():
        if not route.strip() or fixed_cost < 0:
            errors.append("spread_policy.fixed_chain_cost_usd_by_route values must be non-negative")
    for route, chain_units in config.spread_policy.gas_units_by_route.items():
        if not route.strip() or not chain_units:
            errors.append("spread_policy.gas_units_by_route entries must contain at least one chain")
        for chain_id, gas_units in chain_units.items():
            if not chain_id.isdigit() or int(chain_id) <= 0 or gas_units <= 0:
                errors.append("spread_policy.gas_units_by_route requires positive numeric chains and gas units")
    for chain_id, native_usd in config.spread_policy.native_token_usd_ceiling_by_chain.items():
        if not chain_id.isdigit() or int(chain_id) <= 0 or native_usd <= 0:
            errors.append(
                "spread_policy.native_token_usd_ceiling_by_chain requires positive numeric chains and values"
            )
    if config.spread_policy.gas_price_multiplier < 1:
        errors.append("spread_policy.gas_price_multiplier must be at least 1")
    if config.spread_policy.gas_quote_ttl_seconds <= 0:
        errors.append("spread_policy.gas_quote_ttl_seconds must be positive")
    if live_execution and not config.is_test:
        enabled_route_names = tuple(
            route
            for route in (
                "polymarket_predict",
                "polymarket_sx",
                "polymarket_myriad",
                "predict_myriad",
                "predict_sx",
                "sx_myriad",
            )
            if getattr(config.routes, route)
        )
        routes_without_chain_cost = [
            route for route in enabled_route_names if config.spread_policy.fixed_chain_cost_for(route) <= 0
        ]
        if routes_without_chain_cost:
            errors.append(
                "canary/live requires positive spread_policy fixed chain cost for: "
                + ", ".join(routes_without_chain_cost)
            )
        if config.spread_policy.require_live_gas_estimate:
            routes_without_live_gas = [
                route for route in enabled_route_names if not config.spread_policy.gas_units_by_route.get(route)
            ]
            if routes_without_live_gas:
                errors.append(
                    "canary/live requires spread_policy.gas_units_by_route for: "
                    + ", ".join(routes_without_live_gas)
                )
            missing_native_price_chains = sorted(
                {
                    chain_id
                    for route in enabled_route_names
                    for chain_id in config.spread_policy.gas_units_by_route.get(route, {})
                    if config.spread_policy.native_token_usd_ceiling_by_chain.get(chain_id, 0) <= 0
                }
            )
            if missing_native_price_chains:
                errors.append(
                    "canary/live requires native-token USD ceilings for chains: "
                    + ", ".join(missing_native_price_chains)
                )
    for route, floor in config.spread_policy.route_floors.items():
        if not route.strip() or not 0 < floor < 1:
            errors.append("spread_policy.route_floors values must be between 0 and 1")
    if config.min_retry_spread_pct <= 0 or config.min_retry_spread_pct > config.min_entry_spread_pct:
        errors.append("min_retry_spread_pct must be positive and no greater than min_entry_spread_pct")
    if config.min_market_volume_usd < 0:
        errors.append("min_market_volume_usd must be non-negative")
    if config.max_sports_market_horizon_hours <= 0:
        errors.append("max_sports_market_horizon_hours must be positive")
    if config.max_crypto_market_horizon_hours <= 0:
        errors.append("max_crypto_market_horizon_hours must be positive")
    normalized_category_horizons: set[str] = set()
    for category, hours in config.max_market_horizon_hours_by_category.items():
        normalized = normalize_launch_category(category)
        if normalized is None:
            errors.append("max_market_horizon_hours_by_category keys must be non-empty")
        else:
            normalized_category_horizons.add(normalized)
        if hours <= 0:
            errors.append("max_market_horizon_hours_by_category values must be positive")
    if config.execution_mode.submits_orders and config.scan_all and not config.market_horizon_filter_enabled:
        errors.append("scan_all canary/live requires market_horizon_filter_enabled=true")
    if config.execution_mode.submits_orders and config.scan_all:
        configured_categories = {
            normalized_category
            for value in config.categories_to_scan
            if (normalized_category := normalize_launch_category(value)) is not None
        }
        if not configured_categories:
            errors.append("scan_all canary/live requires at least one configured category")
        missing_category_horizons = configured_categories - {
            "sports",
            "crypto",
            *normalized_category_horizons,
        }
        if missing_category_horizons:
            errors.append(
                "scan_all canary/live requires max_market_horizon_hours_by_category for: "
                + ", ".join(sorted(missing_category_horizons))
            )
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
    if not 1 <= config.shadow_preflight_samples <= 5:
        errors.append("shadow_preflight_samples must be between 1 and 5")
    if not 0 <= config.shadow_preflight_sample_interval_seconds <= 1:
        errors.append("shadow_preflight_sample_interval_seconds must be between 0 and 1")
    if config.shadow_preflight_cooldown_seconds < 0:
        errors.append("shadow_preflight_cooldown_seconds must be non-negative")
    if not 0 < config.shadow_preflight_evidence_ttl_seconds <= 3600:
        errors.append("shadow_preflight_evidence_ttl_seconds must be between 0 and 3600")
    if config.market_data_target_hold_seconds < 0:
        errors.append("market_data_target_hold_seconds must be non-negative")
    route_names = set(RouteConfig.__dataclass_fields__)
    if any(
        route not in route_names or seconds < 0
        for route, seconds in config.market_data_target_hold_seconds_by_route.items()
    ):
        errors.append("market_data_target_hold_seconds_by_route requires known routes and non-negative values")
    if config.market_data_executable_priority_seconds < 0:
        errors.append("market_data_executable_priority_seconds must be non-negative")
    if any(
        route not in route_names or seconds < 0
        for route, seconds in config.market_data_executable_priority_seconds_by_route.items()
    ):
        errors.append(
            "market_data_executable_priority_seconds_by_route requires known routes "
            "and non-negative values"
        )
    if not 0 < config.market_data_exploration_fraction <= 1:
        errors.append("market_data_exploration_fraction must be between 0 and 1")
    if any(
        route not in route_names or not 0 < fraction <= 1
        for route, fraction in config.market_data_exploration_fraction_by_route.items()
    ):
        errors.append(
            "market_data_exploration_fraction_by_route requires known routes and values between 0 and 1"
        )
    if any(
        route not in route_names or not 1 <= multiplier <= 4
        for route, multiplier in config.market_data_prefetch_multiplier_by_route.items()
    ):
        errors.append("market_data_prefetch_multiplier_by_route requires known routes and values between 1 and 4")
    if any(
        route not in route_names or not 1 <= weight <= 4
        for route, weight in config.market_evaluation_weight_by_route.items()
    ):
        errors.append("market_evaluation_weight_by_route requires known routes and values between 1 and 4")
    if config.discovery_max_stale_seconds < 900:
        errors.append("discovery_max_stale_seconds must be at least 900")
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
        "sx_bet": config.sx_bet.max_slippage_pct,
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
    if config.sx_bet_fill_timeout_ms < 3_600:
        errors.append("sx_bet_fill_timeout_ms must be at least 3600 for SX Bet/Web3 fills")
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
    if config.sx_bet.api_version not in {"v2", "v3"}:
        errors.append("sx_bet.api_version must be v2 or v3")
    if config.sx_bet.environment not in {"mainnet", "toronto"}:
        errors.append("sx_bet.environment must be mainnet or toronto")
    if config.sx_bet.time_in_force not in {"IOC", "FOK"}:
        errors.append("sx_bet.time_in_force must be IOC or FOK")
    if live_execution and sx_required and config.sx_bet.api_version == "v3" and config.sx_bet.time_in_force != "FOK":
        errors.append("SX Bet V3 funded execution requires sx_bet.time_in_force=FOK")
    if config.sx_bet.api_version == "v2" and config.sx_bet.chain_id != 4162:
        errors.append("sx_bet.chain_id must be 4162 for V2")
    if config.sx_bet.api_version == "v3":
        expected_api_url = (
            "https://api.toronto.sx.bet" if config.sx_bet.environment == "toronto" else "https://api.sx.bet"
        )
        expected_ws_url = (
            "wss://realtime.toronto.sx.bet/connection/websocket"
            if config.sx_bet.environment == "toronto"
            else "wss://realtime.sx.bet/connection/websocket"
        )
        if config.sx_bet.api_base_url.rstrip("/") != expected_api_url:
            errors.append(f"SX Bet V3 {config.sx_bet.environment} must use the official API host")
        if config.sx_bet.ws_url.rstrip("/") != expected_ws_url:
            errors.append(f"SX Bet V3 {config.sx_bet.environment} must use the official realtime host")
        if config.sx_bet.environment == "mainnet" and not config.sx_bet.allow_v3_mainnet:
            errors.append("SX Bet V3 mainnet requires sx_bet.allow_v3_mainnet=true after operator cutover")
    if not predict_required and not sx_required and not myriad_required:
        errors.append("at least one hedge venue must be active: Predict.fun, SX Bet, or Myriad")
    if config.myriad_markets.enabled:
        if not 50 <= config.myriad_markets.order_book_ttl_ms <= 1_500:
            errors.append("myriad_markets.order_book_ttl_ms must be between 50 and 1500")
        if config.myriad_markets.websocket_stale_after_ms < config.myriad_markets.order_book_ttl_ms:
            errors.append("myriad_markets.websocket_stale_after_ms must be >= order_book_ttl_ms")
        if live_execution and myriad_required and not config.myriad_markets.private_key:
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
        enabled_market_routes = (
            ("polymarket_myriad", config.routes.polymarket_myriad),
            ("polymarket_predict", config.routes.polymarket_predict),
            ("predict_myriad", config.routes.predict_myriad),
            ("predict_sx", config.routes.predict_sx),
            ("polymarket_sx", config.routes.polymarket_sx),
            ("sx_myriad", config.routes.sx_myriad),
        )
        validated_routes = set(market.verified_routes)
        try:
            validated_routes.add(execution_route_for_market(market))
        except ValueError:
            pass
        for route, enabled in enabled_market_routes:
            if (
                enabled
                and route in validated_routes
                and market_supports_execution_route(market, route)
                and not route_execution_sides_are_complementary(market, route)
            ):
                errors.append(f"{prefix} execution orientation is inconsistent for route {route}")
        if market.predict_fun_price_precision is not None and not 0 <= market.predict_fun_price_precision <= 18:
            errors.append(f"{prefix}.predict_fun_price_precision must be between 0 and 18")
        if not config.scan_all and (
            (require_resolved_markets or not has_discovery_terms)
            and (not market.polymarket_token_id or market.polymarket_token_id.startswith("replace-with"))
        ):
            errors.append(f"{prefix}.polymarket_token_id or discovery fields symbol/target_label are required")
        if (
            second_routes_enabled
            and not config.scan_all
            and (
                (require_resolved_markets or not has_discovery_terms)
                and (not market.predict_fun_token_id or market.predict_fun_token_id.startswith("replace-with"))
                and market.predict_fun_amm_pool is None
            )
        ):
            errors.append(f"{prefix}.second_leg_token_id or second_leg_amm_pool is required")
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
            "predict_sx": config.routes.predict_sx,
            "polymarket_sx": config.routes.polymarket_sx,
            "sx_myriad": config.routes.sx_myriad,
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
        if predict_required and not config.predict_fun.private_key:
            errors.append("PREDICT_FUN_PRIVATE_KEY is required when isTest=false")
        elif (
            predict_required
            and config.predict_fun.private_key
            and not _is_private_key(config.predict_fun.private_key)
        ):
            errors.append("PREDICT_FUN_PRIVATE_KEY must be a 64 hex character ECDSA key, with optional 0x prefix")
        if predict_required and not config.predict_fun.rpc_url:
            errors.append("BNB_RPC_URL or predict_fun.rpc_url is required when isTest=false")
        if predict_required and not config.predict_fun.api_base_url:
            errors.append("predict_fun.api_base_url is required when isTest=false")
        if predict_required and not config.predict_fun.market_abi_path and not config.predict_fun.api_base_url:
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
        if sx_required and not config.sx_bet.private_key:
            errors.append("SX_BET_PRIVATE_KEY is required when SX Bet routes are enabled")
        elif sx_required and not _is_private_key(config.sx_bet.private_key):
            errors.append("SX_BET_PRIVATE_KEY must be a 64 hex character ECDSA key, with optional 0x prefix")
        if sx_required and config.sx_bet.api_version == "v3" and not config.sx_bet.api_key:
            errors.append("SX_BET_API_KEY is required for funded SX Bet V3 execution")
        if sx_required and config.sx_bet.api_version == "v2" and not config.sx_bet.rpc_url:
            errors.append("SX_BET_RPC_URL or sx_bet.rpc_url is required when SX Bet is enabled")

    if errors:
        joined = "\n - ".join(errors)
        raise ValueError(f"Invalid configuration:\n - {joined}")


def _default_priority_fee_gwei(chain_id: int) -> float:
    if chain_id in (56, 97):
        return 2.0
    if chain_id in (137, 80002):
        return 20.0
    return 3.0


def _strict_bool(value: Any, field: str) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized == "true":
            return True
        if normalized == "false":
            return False
    if value in (0, 1):
        return bool(value)
    raise ValueError(f"{field} must be a boolean")


def _json_bool(value: Any, field: str) -> bool:
    if isinstance(value, bool):
        return value
    raise ValueError(f"{field} must be a JSON boolean")


def _is_private_key(value: str | None) -> bool:
    if not value:
        return False
    raw = value[2:] if value.startswith("0x") else value
    if len(raw) != 64:
        return False
    return all(char in "0123456789abcdefABCDEF" for char in raw)


def _is_scan_all_filter(value: Any) -> bool:
    if not isinstance(value, list) or not value:
        return True
    return any(isinstance(item, dict) and str(item.get("symbol", "")).strip() in {"", "*"} for item in value)


def _parse_execution_mode(data: dict[str, Any]) -> ExecutionMode:
    raw = os.getenv(_EXECUTION_MODE_OVERRIDE_ENV) or data.get("execution_mode")
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
