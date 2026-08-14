import json
import re
from pathlib import Path


def test_env_example_covers_all_config_example_placeholders() -> None:
    root = Path(__file__).resolve().parents[1]
    config_example = (root / "config.example.json").read_text(encoding="utf-8")
    env_example = (root / ".env.example").read_text(encoding="utf-8")

    placeholders = {match.group(1) for match in re.finditer(r"\$\{([A-Z0-9_]+)\}", config_example)}
    env_keys = {match.group(1) for match in re.finditer(r"^([A-Z0-9_]+)=", env_example, flags=re.MULTILINE)}

    missing = sorted(placeholders - env_keys)
    assert missing == []


def test_default_configs_keep_current_sx_and_route_schema() -> None:
    root = Path(__file__).resolve().parents[1]
    for name in ("config.example.json", "config.shadow_sports.json"):
        payload = json.loads((root / name).read_text(encoding="utf-8"))

        assert payload["execution_mode"] == "shadow"
        assert payload["database_url"] == "${DATABASE_URL}"
        assert payload["enable_sx_bet"] is False
        assert payload["sx_bet_fill_timeout_ms"] == 4000

        routes = payload["routes"]
        assert routes["polymarket_myriad"] is True
        assert routes["predict_sx"] is False
        assert routes["polymarket_sx"] is False
        assert routes["sx_myriad"] is False

        sx_bet = payload["sx_bet"]
        assert sx_bet["api_base_url"] == "https://api.sx.bet"
        assert sx_bet["api_key"] == "${SX_BET_API_KEY}"
        assert sx_bet["private_key"] == "${SX_BET_PRIVATE_KEY}"
        assert sx_bet["base_token_address"] == "${SX_BET_BASE_TOKEN_ADDRESS}"
        assert sx_bet["chain_id"] == 4162


def test_compose_deploy_uses_authoritative_production_env_file() -> None:
    root = Path(__file__).resolve().parents[1]
    script = (root / "ops" / "deploy_compose.sh").read_text(encoding="utf-8")
    compose = (root / "docker-compose.yml").read_text(encoding="utf-8")

    assert "COMPOSE_ENV_FILE=${COMPOSE_ENV_FILE:-.env.production}" in script
    assert 'docker compose --env-file "${COMPOSE_ENV_FILE}"' in script
    assert 'test -f "${COMPOSE_ENV_FILE}"' in script
    assert 'test -n "${CI_VERIFIED_COMMIT_SHA:-}"' in script
    assert "HEALTH_RETRIES=${HEALTH_RETRIES:-120}" in script
    assert "DEPLOY_HEALTH_POLICY=${DEPLOY_HEALTH_POLICY:-ready}" in script
    assert "scripts/runtime_health_gate.py" in script
    assert 'test "${LIVE_TRADING_CONFIRM:-NO}" = "NO"' in script
    assert "http://127.0.0.1:9108/health/live" in compose
    assert "http://127.0.0.1:9109/health/live" in compose
    assert "http://127.0.0.1:9108/health/ready" not in compose
    assert "http://127.0.0.1:9109/health/ready" not in compose
    assert compose.count("CI_VERIFIED_COMMIT_SHA: ${CI_VERIFIED_COMMIT_SHA:-}") == 3


def test_production_services_use_bounded_concurrency_and_safe_exit_policy() -> None:
    root = Path(__file__).resolve().parents[1]
    clob = json.loads((root / "config.production.clob_hft.json").read_text(encoding="utf-8"))
    quote = json.loads((root / "config.production.quote_arb.json").read_text(encoding="utf-8"))

    assert clob["max_concurrent_market_evaluations"] == 16
    assert quote["max_concurrent_market_evaluations"] == 16
    assert clob["shadow_preflight_samples"] == 3
    assert quote["shadow_preflight_samples"] == 3
    assert clob["shadow_preflight_sample_interval_seconds"] == 0.15
    assert quote["shadow_preflight_sample_interval_seconds"] == 0.15
    assert clob["shadow_preflight_cooldown_seconds"] == 30.0
    assert quote["shadow_preflight_cooldown_seconds"] == 30.0
    assert clob["shadow_preflight_evidence_ttl_seconds"] == 900.0
    assert quote["shadow_preflight_evidence_ttl_seconds"] == 900.0
    assert quote["market_data_target_hold_seconds"] == 3.0
    assert quote["market_data_target_hold_seconds_by_route"] == {
        "polymarket_predict": 3.0,
        "polymarket_myriad": 60.0,
    }
    assert clob["market_data_executable_priority_seconds_by_route"] == {
        "polymarket_sx": 60.0,
    }
    assert quote["market_data_executable_priority_seconds_by_route"] == {
        "polymarket_predict": 120.0,
        "polymarket_myriad": 300.0,
    }
    assert clob["market_data_exploration_fraction_by_route"] == {"polymarket_sx": 0.75}
    assert quote["market_data_exploration_fraction_by_route"] == {
        "polymarket_predict": 0.75,
        "polymarket_myriad": 0.5,
    }
    assert quote["market_data_prefetch_multiplier_by_route"] == {
        "polymarket_predict": 1,
        "polymarket_myriad": 3,
    }
    assert quote["market_evaluation_weight_by_route"] == {
        "polymarket_predict": 3,
        "polymarket_myriad": 1,
    }
    assert quote["poll_interval_ms"] == 50
    assert clob["market_data_target_hold_seconds"] == 2.0
    assert clob["market_data_prefetch_multiplier_by_route"] == {"polymarket_sx": 2}
    assert clob["auto_close"]["enabled"] is False
    assert quote["auto_close"]["enabled"] is False
    assert clob["spread_policy"]["fixed_chain_cost_usd_by_route"]["polymarket_sx"] > 0
    assert quote["spread_policy"]["fixed_chain_cost_usd_by_route"]["polymarket_predict"] > 0
    assert quote["spread_policy"]["fixed_chain_cost_usd_by_route"]["polymarket_myriad"] > 0
    assert clob["spread_policy"]["require_live_gas_estimate"] is True
    assert quote["spread_policy"]["require_live_gas_estimate"] is True
    assert clob["spread_policy"]["gas_units_by_route"]["polymarket_sx"]
    assert quote["spread_policy"]["gas_units_by_route"]["polymarket_predict"]
    assert quote["spread_policy"]["gas_units_by_route"]["polymarket_myriad"]
    assert clob["discovery_max_stale_seconds"] == 1800.0
    assert quote["discovery_max_stale_seconds"] == 1800.0
    assert clob["spread_policy"]["adverse_move_p95_pct_by_route"] == {
        "polymarket_sx": 0.00025,
    }
    assert quote["spread_policy"]["adverse_move_p95_pct_by_route"] == {
        "polymarket_predict": 0.0001,
        "polymarket_myriad": 0.001,
    }
