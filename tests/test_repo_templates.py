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
    assert 'docker compose --env-file "${COMPOSE_ENV_FILE}" -f docker-compose.yml' in script
    assert 'test -f "${COMPOSE_ENV_FILE}"' in script
    assert 'test -n "${CI_VERIFIED_COMMIT_SHA:-}"' in script
    assert 'if [[ -z "${HEALTH_RETRIES:-}" ]]; then' in script
    assert '[[ "${DEPLOY_HEALTH_POLICY}" == "safe_paused_shadow_bootstrap" ]]' in script
    assert "HEALTH_RETRIES=${BOOTSTRAP_HEALTH_RETRIES:-600}" in script
    assert "HEALTH_RETRIES=120" in script
    assert "HEALTH_WAIT_TIMEOUT_SECONDS=1200" in script
    assert "HEALTH_WAIT_TIMEOUT_SECONDS=240" in script
    assert 'timeout --foreground --kill-after=1s "${process_timeout_seconds}s"' in script
    assert '((SECONDS < health_wait_deadline)) || break' in script
    assert "DEPLOY_HEALTH_POLICY=${DEPLOY_HEALTH_POLICY:-ready}" in script
    assert "safe_paused_shadow_bootstrap" in script
    assert "scripts/runtime_health_gate.py" in script
    assert "compose config --format json" in script
    assert 'environment.get("ARBITRAGE_EXECUTION_MODE_OVERRIDE", "")' in script
    assert 'environment.get("LIVE_TRADING_CONFIRM", "")' in script
    reexec = script.index('exec bash "${BASH_SOURCE[0]}"')
    assert script.index("git pull --ff-only") < reexec
    assert reexec < script.index("compose config --format json")
    migrate_build = script.index("compose build migrate")
    stop = script.index("compose stop bot-clob-hft bot-quote-arb")
    migrate = script.index("compose run --rm migrate")
    assert script.index("compose config --format json") < migrate_build < stop < migrate
    pause_block = script.index("if is_safe_paused_deploy; then", script.index("compose run --rm migrate"))
    compose_up = script.index("compose up -d --build bot-clob-hft bot-quote-arb")
    assert pause_block < compose_up
    pause_section = script[pause_block:compose_up]
    assert stop < migrate < pause_block
    assert "persist_and_verify_pause config.production.clob_hft.json" in pause_section
    assert "json.load(sys.stdin).get(\"paused\") is not True" in pause_section
    assert pause_section.count("persist_and_verify_pause config.production.clob_hft.json") == 1
    assert pause_section.count("persist_and_verify_pause config.production.quote_arb.json") == 1
    assert '-m arbitrage_engine.cli --config "${config_path}" risk pause' in pause_section
    assert '"${DEPLOY_HEALTH_POLICY}_deploy:${revision}"' in script[pause_block:compose_up]
    assert "http://127.0.0.1:9108/health/live" in compose
    assert "http://127.0.0.1:9109/health/live" in compose
    assert "http://127.0.0.1:9108/health/ready" not in compose
    assert "http://127.0.0.1:9109/health/ready" not in compose
    assert compose.count("CI_VERIFIED_COMMIT_SHA: ${CI_VERIFIED_COMMIT_SHA:-}") == 3


def test_database_integration_tests_cannot_use_runtime_database_url() -> None:
    root = Path(__file__).resolve().parents[1]
    compose = (root / "docker-compose.yml").read_text(encoding="utf-8")
    workflow = (root / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")
    integration_test = (root / "tests" / "test_database_integration.py").read_text(encoding="utf-8")

    postgres_test_service = compose[
        compose.index("\n  postgres-test:\n") : compose.index("\n  migrate:\n")
    ]
    test_service = compose[compose.index("\n  test:\n") : compose.index("\n  operator:\n")]
    assert 'profiles: ["test"]' in postgres_test_service
    assert "POSTGRES_DB: arbitrage_test" in postgres_test_service
    assert "tmpfs:" in postgres_test_service
    assert "/var/lib/postgresql/data" in postgres_test_service
    assert ".env.production" not in test_service
    assert 'ARBITRAGE_ALLOW_DESTRUCTIVE_DB_TESTS: "YES"' in test_service
    assert 'DATABASE_URL: ""' in test_service
    assert "TEST_DATABASE_URL: postgresql+asyncpg://arbitrage_test:" in test_service
    assert "@postgres-test:5432/arbitrage_test" in test_service
    assert "\n      postgres-test:\n" in test_service
    assert 'os.getenv("ARBITRAGE_ALLOW_DESTRUCTIVE_DB_TESTS") != "YES"' in integration_test
    assert 'os.getenv("TEST_DATABASE_URL")' in integration_test
    assert 'os.getenv("DATABASE_URL")' in integration_test
    assert "TEST_DATABASE_URL must use the isolated PostgreSQL test service" in integration_test
    assert "TEST_DATABASE_URL database name must be exactly 'arbitrage_test'" in integration_test
    assert "TEST_DATABASE_URL must not match DATABASE_URL" in integration_test
    assert workflow.count("TEST_DATABASE_URL: postgresql+asyncpg://") == 1
    pytest_step = workflow[workflow.index("      - run: python -m pytest -q") :]
    assert 'ARBITRAGE_ALLOW_DESTRUCTIVE_DB_TESTS: "YES"' in pytest_step[:200]
    assert 'DATABASE_URL: ""' in pytest_step[:200]

    dockerfile = (root / "Dockerfile").read_text(encoding="utf-8")
    assert (
        "COPY Dockerfile docker-compose.yml config.production.clob_hft.json config.production.quote_arb.json ./"
        in dockerfile
    )
    assert "COPY .github/workflows/ci.yml ./.github/workflows/ci.yml" in dockerfile


def test_production_services_use_bounded_concurrency_and_safe_exit_policy() -> None:
    root = Path(__file__).resolve().parents[1]
    clob = json.loads((root / "config.production.clob_hft.json").read_text(encoding="utf-8"))
    quote = json.loads((root / "config.production.quote_arb.json").read_text(encoding="utf-8"))

    assert clob["max_concurrent_market_evaluations"] == 16
    assert quote["max_concurrent_market_evaluations"] == 18
    assert quote["max_concurrent_market_evaluations_by_route"] == {
        "polymarket_myriad": 10,
    }
    assert clob["sx_bet"]["api_version"] == "v3"
    assert clob["sx_bet"]["environment"] == "mainnet"
    assert clob["sx_bet"]["time_in_force"] == "FOK"
    assert clob["sx_bet"]["allow_v3_mainnet"] is True
    assert quote["sx_bet"]["api_version"] == "v3"
    assert quote["sx_bet"]["environment"] == "mainnet"
    assert quote["sx_bet"]["time_in_force"] == "FOK"
    assert quote["sx_bet"]["allow_v3_mainnet"] is True
    assert clob["enable_predict_fun"] is True
    assert clob["predict_fun"]["enabled"] is True
    assert clob["myriad_markets"]["enabled"] is True
    assert {
        route for route, enabled in clob["routes"].items() if enabled
    } == {"predict_sx", "polymarket_sx", "sx_myriad"}
    assert {
        route for route, enabled in quote["routes"].items() if enabled
    } == {
        "polymarket_predict",
        "polymarket_myriad",
        "predict_myriad",
        "predict_sx",
        "polymarket_sx",
        "sx_myriad",
    }
    assert clob["execution_mode"] == "shadow"
    assert not any(clob["funded_routes"].values())
    assert {
        route for route, enabled in quote["funded_routes"].items() if enabled
    } == {
        "polymarket_predict",
        "polymarket_myriad",
        "predict_myriad",
        "predict_sx",
        "polymarket_sx",
        "sx_myriad",
    }
    assert quote["enable_sx_bet"] is True
    assert quote["sx_bet"]["enabled"] is True
    expected_quote_categories = {
        "ai",
        "airdrops",
        "apple",
        "box office",
        "business",
        "canada",
        "china",
        "crypto",
        "culture",
        "economy",
        "fed",
        "federal reserve",
        "finance",
        "gdp",
        "gta 6",
        "iran",
        "politics",
        "prediction markets",
        "science",
        "spacex",
        "sports",
        "trump",
        "video games",
        "weather",
    }
    assert set(quote["categories_to_scan"]) == expected_quote_categories
    assert clob["categories_to_scan"] == ["sports"]
    assert quote["max_market_horizon_hours_by_category"] == {
        category: 200 for category in expected_quote_categories - {"crypto", "sports"}
    }
    for config in (clob, quote):
        assert config["shadow_require_verified_mappings"] is True
        assert config["position_size_usd"] == 50.0
        assert config["max_order_size_usd"] == 50.0
        assert config["max_total_notional_usd"] == 252
        assert config["max_venue_exposure_usd"] == 125
        assert config["max_market_exposure_usd"] == 52
        assert config["min_venue_balance_usd"] == 125
        assert config["max_open_positions"] == 5
        assert config["max_daily_loss_usd"] == 10
        assert config["max_unresolved_exposure_usd"] == 5
        assert config["max_orders_per_minute"] == 10
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
        "polymarket_myriad": 20.0,
        "predict_myriad": 60.0,
        "predict_sx": 3.0,
        "polymarket_sx": 2.0,
        "sx_myriad": 60.0,
    }
    assert clob["market_data_target_hold_seconds_by_route"] == {
        "predict_sx": 3.0,
        "polymarket_sx": 2.0,
        "sx_myriad": 60.0,
    }
    assert clob["market_data_executable_priority_seconds_by_route"] == {
        "predict_sx": 120.0,
        "polymarket_sx": 60.0,
        "sx_myriad": 300.0,
    }
    assert quote["market_data_executable_priority_seconds_by_route"] == {
        "polymarket_predict": 120.0,
        "polymarket_myriad": 300.0,
        "predict_myriad": 300.0,
        "predict_sx": 120.0,
        "polymarket_sx": 60.0,
        "sx_myriad": 300.0,
    }
    assert clob["market_data_exploration_fraction_by_route"] == {
        "predict_sx": 0.75,
        "polymarket_sx": 0.75,
        "sx_myriad": 0.5,
    }
    assert quote["market_data_exploration_fraction_by_route"] == {
        "polymarket_predict": 0.75,
        "polymarket_myriad": 0.5,
        "predict_myriad": 0.5,
        "predict_sx": 0.75,
        "polymarket_sx": 0.75,
        "sx_myriad": 0.5,
    }
    assert quote["market_data_prefetch_multiplier_by_route"] == {
        "polymarket_predict": 1,
        "polymarket_myriad": 1,
        "predict_myriad": 3,
        "predict_sx": 1,
        "polymarket_sx": 2,
        "sx_myriad": 3,
    }
    assert quote["market_evaluation_weight_by_route"] == {
        "polymarket_predict": 1,
        "polymarket_myriad": 4,
        "predict_myriad": 1,
        "predict_sx": 1,
        "polymarket_sx": 1,
        "sx_myriad": 1,
    }
    assert quote["poll_interval_ms"] == 300
    quote_evaluation_slots_per_second = (
        quote["max_concurrent_market_evaluations"] * 1_000 / quote["poll_interval_ms"]
    )
    assert quote_evaluation_slots_per_second <= 64
    # The formal one-hour calibration requires 10,000 valid evaluations per
    # funded route. Keep 20% cadence headroom before run-time work is included.
    quote_theoretical_route_cycles_per_hour = 3_600_000 / quote["poll_interval_ms"]
    assert quote_theoretical_route_cycles_per_hour >= 12_000
    assert clob["market_data_target_hold_seconds"] == 2.0
    assert clob["market_data_prefetch_multiplier_by_route"] == {
        "predict_sx": 1,
        "polymarket_sx": 2,
        "sx_myriad": 3,
    }
    assert clob["market_evaluation_weight_by_route"] == {
        "predict_sx": 1,
        "polymarket_sx": 1,
        "sx_myriad": 1,
    }
    assert clob["auto_close"]["enabled"] is False
    assert quote["auto_close"]["enabled"] is False
    assert clob["spread_policy"]["fixed_chain_cost_usd_by_route"]["polymarket_sx"] > 0
    assert clob["spread_policy"]["fixed_chain_cost_usd_by_route"]["predict_sx"] > 0
    assert clob["spread_policy"]["fixed_chain_cost_usd_by_route"]["sx_myriad"] > 0
    assert quote["spread_policy"]["fixed_chain_cost_usd_by_route"]["polymarket_predict"] > 0
    assert quote["spread_policy"]["fixed_chain_cost_usd_by_route"]["polymarket_myriad"] > 0
    assert quote["spread_policy"]["fixed_chain_cost_usd_by_route"]["predict_myriad"] > 0
    assert quote["spread_policy"]["fixed_chain_cost_usd_by_route"]["predict_sx"] > 0
    assert quote["spread_policy"]["fixed_chain_cost_usd_by_route"]["polymarket_sx"] > 0
    assert quote["spread_policy"]["fixed_chain_cost_usd_by_route"]["sx_myriad"] > 0
    assert clob["spread_policy"]["require_live_gas_estimate"] is True
    assert quote["spread_policy"]["require_live_gas_estimate"] is True
    assert clob["spread_policy"]["gas_units_by_route"]["polymarket_sx"]
    assert clob["spread_policy"]["gas_units_by_route"]["predict_sx"]
    assert clob["spread_policy"]["gas_units_by_route"]["sx_myriad"]
    assert quote["spread_policy"]["gas_units_by_route"]["polymarket_predict"]
    assert quote["spread_policy"]["gas_units_by_route"]["polymarket_myriad"]
    assert quote["spread_policy"]["gas_units_by_route"]["predict_myriad"]
    assert quote["spread_policy"]["gas_units_by_route"]["predict_sx"]
    assert quote["spread_policy"]["gas_units_by_route"]["polymarket_sx"]
    assert quote["spread_policy"]["gas_units_by_route"]["sx_myriad"]
    assert clob["discovery_max_stale_seconds"] == 1800.0
    assert quote["discovery_max_stale_seconds"] == 1800.0
    assert clob["spread_policy"]["adverse_move_p95_pct_by_route"] == {
        "predict_sx": 0.01,
        "polymarket_sx": 0.0005,
        "sx_myriad": 0.0005,
    }
    assert quote["spread_policy"]["adverse_move_p95_pct_by_route"] == {
        "polymarket_predict": 0.01,
        "polymarket_myriad": 0.02,
        "predict_myriad": 0.01,
        "predict_sx": 0.01,
        "polymarket_sx": 0.0005,
        "sx_myriad": 0.0005,
    }
    for route in ("polymarket_predict", "polymarket_myriad", "predict_myriad"):
        assert max(
            quote["spread_policy"]["route_floors"][route],
            quote["spread_policy"]["adverse_move_p95_pct_by_route"][route]
            + quote["spread_policy"]["safety_buffer_pct"],
        ) == 0.025
    for route in ("predict_sx", "sx_myriad"):
        assert max(
            clob["spread_policy"]["route_floors"][route],
            clob["spread_policy"]["adverse_move_p95_pct_by_route"][route]
            + clob["spread_policy"]["safety_buffer_pct"],
        ) == 0.025
