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
    stop = script.index("compose stop bot-quote-arb")
    migrate = script.index("compose run --rm migrate")
    assert script.index("compose config --format json") < migrate_build < stop < migrate
    pause_block = script.index("if is_safe_paused_deploy; then", script.index("compose run --rm migrate"))
    compose_up = script.index("compose up -d --build bot-quote-arb")
    assert pause_block < compose_up
    pause_section = script[pause_block:compose_up]
    assert stop < migrate < pause_block
    assert "json.load(sys.stdin).get(\"paused\") is not True" in pause_section
    assert pause_section.count("persist_and_verify_pause config.production.quote_arb.json") == 1
    assert '-m arbitrage_engine.cli --config "${config_path}" risk pause' in pause_section
    assert '"${DEPLOY_HEALTH_POLICY}_deploy:${revision}"' in script[pause_block:compose_up]
    assert "http://127.0.0.1:9109/health/live" in compose
    assert "http://127.0.0.1:9109/health/ready" not in compose
    assert compose.count("CI_VERIFIED_COMMIT_SHA: ${CI_VERIFIED_COMMIT_SHA:-}") == 2
    # SX Bet is retired from the release: no second runtime, no second port.
    assert "bot-clob-hft" not in script
    assert "bot-clob-hft" not in compose
    assert "9108" not in compose


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
        "COPY Dockerfile docker-compose.yml config.production.quote_arb.json ./"
        in dockerfile
    )
    assert "COPY .github/workflows/ci.yml ./.github/workflows/ci.yml" in dockerfile


def test_production_services_use_bounded_concurrency_and_safe_exit_policy() -> None:
    root = Path(__file__).resolve().parents[1]
    quote = json.loads((root / "config.production.quote_arb.json").read_text(encoding="utf-8"))
    # SX Bet is retired from the release: there is no SX shadow runtime and
    # no second production config. The SX connector stays in src/ for
    # discovery, and the SX routes stay disabled in the one runtime that ships.
    assert not (root / "config.production.clob_hft.json").exists()

    # 2026-09-25: 57 hours, 3.25M evaluations, zero entries. The window held
    # 18 pairs, split evenly across three routes, while the Polymarket <->
    # Predict.fun universe is ~1,180 live verified pairs and the Myriad ones
    # are ~24 each. Each Predict.fun pair was therefore watched for three
    # seconds once every ten minutes -- 0.5% of the time -- on the one route
    # whose best observed spread (+2.06%) came anywhere near the 2.5% floor.
    # predict_myriad, which cannot submit an entry at all, was taking a third
    # of the slots and, at a prefetch multiplier of 3, most of the Myriad and
    # Predict.fun subscriptions.
    # 24 of them, and the first try at this said 36. A cycle has to prime the
    # books its new window just rotated in, and Predict.fun's stream is the
    # slow one: 24 slots on that route rotating every three seconds produced
    # 201 stale primes in an hour and stretched the cycle from 1.1 to 4.9
    # seconds. Every route is then starved in proportion to its slots, and
    # polymarket_myriad came in at 7,124 calibration evaluations against a
    # 10,000 minimum. Twelve slots keep a real gain -- a full sweep of the
    # ~1,180 Predict.fun pairs in ~5 minutes instead of ~10 -- at a prime rate
    # the venue keeps up with.
    assert quote["max_concurrent_market_evaluations"] == 24
    assert quote["max_concurrent_market_evaluations_by_route"] == {
        "polymarket_predict": 12,
        "polymarket_myriad": 10,
        "predict_myriad": 2,
    }
    # The slot budget is exactly the sum of the per-route caps: nothing is
    # allocated to a route the config did not name.
    assert sum(quote["max_concurrent_market_evaluations_by_route"].values()) == (
        quote["max_concurrent_market_evaluations"]
    )
    assert quote["sx_bet"]["api_version"] == "v3"
    assert quote["sx_bet"]["environment"] == "mainnet"
    assert quote["sx_bet"]["time_in_force"] == "FOK"
    assert quote["sx_bet"]["allow_v3_mainnet"] is True
    # SX Bet is off in the funded runtime. Its overlap with the other venues is
    # two markets against Predict.fun, none against Myriad, and a handful of
    # short-lived handicap lines against Polymarket -- and three routes that
    # cannot trade still take evaluation slots, market-data subscriptions and
    # CPU from the two that can.
    assert {
        route for route, enabled in quote["routes"].items() if enabled
    } == {
        "polymarket_predict",
        "polymarket_myriad",
        "predict_myriad",
    }
    # predict_sx and polymarket_sx stay enabled for discovery but are not
    # funded: two and four tradable markets cannot sustain a calibration
    # window, and the gate passes only if every funded route does.
    assert {
        route for route, enabled in quote["funded_routes"].items() if enabled
    } == {
        "polymarket_predict",
        "polymarket_myriad",
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
        "brazil",
        "gaming",
        "jobs",
        "sam altman",
        "south korea",
        "trump",
        "unknown",
        "video games",
        "weather",
    }
    assert set(quote["categories_to_scan"]) == expected_quote_categories
    # Every scanned category needs a horizon: one that is missing from this map
    # is rejected outright, not defaulted (`horizon_hours is not None and ...`
    # in cli.py). crypto and sports carry their own dedicated settings.
    horizons = quote["max_market_horizon_hours_by_category"]
    assert set(horizons) == expected_quote_categories - {"crypto", "sports"}
    # `unknown` is not a category, it is markets the classifier could not label.
    # It gets a tighter bound than the rest so capital does not sit for a week
    # in something nobody has classified; the rest share the standard 200.
    assert horizons["unknown"] == 48
    # A category nobody listed is in scope on this bound rather than dropped by
    # the approval scope and left unbounded at runtime, which is what used to
    # happen to each one a venue invented.
    assert quote["default_market_horizon_hours"] == 48.0
    assert {c: h for c, h in horizons.items() if c != "unknown"} == {
        category: 200 for category in expected_quote_categories - {"crypto", "sports", "unknown"}
    }
    assert quote["shadow_require_verified_mappings"] is True
    assert quote["position_size_usd"] == 50.0
    assert quote["max_order_size_usd"] == 50.0
    assert quote["max_total_notional_usd"] == 252
    assert quote["max_venue_exposure_usd"] == 125
    assert quote["max_market_exposure_usd"] == 52
    assert quote["min_venue_balance_usd"] == 125
    assert quote["max_open_positions"] == 5
    assert quote["max_daily_loss_usd"] == 10
    assert quote["max_unresolved_exposure_usd"] == 5
    assert quote["max_orders_per_minute"] == 10
    assert quote["shadow_preflight_samples"] == 3
    assert quote["shadow_preflight_sample_interval_seconds"] == 0.15
    assert quote["shadow_preflight_cooldown_seconds"] == 30.0
    assert quote["shadow_preflight_evidence_ttl_seconds"] == 900.0
    assert quote["market_data_executable_priority_seconds_by_route"] == {
        "polymarket_predict": 120.0,
        "polymarket_myriad": 300.0,
        "predict_myriad": 300.0,
        "predict_sx": 120.0,
        "polymarket_sx": 60.0,
        "sx_myriad": 300.0,
    }
    # The engine no longer rotates a window of books on a timer, so these are
    # the width of what it can see. The caps differ per venue because the
    # venues do: Polymarket's stream carried 34 books at 0.35s event age,
    # Predict.fun's was already ~2s behind at 26 against a 2s staleness bar,
    # and Myriad's whole live universe is two dozen pairs.
    assert quote["max_market_data_subscriptions"] == 120
    assert quote["max_market_data_subscriptions_by_venue"] == {
        "Polymarket": 160,
        "Predict.fun": 120,
        "Myriad": 48,
    }
    # Polymarket's cap covers both funded routes at once: 12 predict pairs and
    # 10 myriad pairs need 22 of it per cycle, and the rest is what the
    # scheduler can react to without paying a snapshot first.
    assert (
        quote["max_market_data_subscriptions_by_venue"]["Polymarket"]
        >= sum(quote["max_concurrent_market_evaluations_by_route"].values())
    )
    # Five minutes of stability per rebuild. The old design paid a snapshot per
    # book every three seconds, which is what stretched the cycle to 4.9s and
    # starved calibration on 2026-09-25.
    assert quote["market_data_subscription_rotation_seconds"] == 300.0
    # And a pair whose books sit still is still recomputed inside a minute:
    # fees, chain cost and the other leg's quote move without the book.
    assert quote["evaluation_max_staleness_seconds"] == 45.0
    assert quote["poll_interval_ms"] == 300
    # A ceiling on evaluation work per second, not a measurement: a cycle on
    # this 2 vCPU host takes about a second of real work, so 18 slots at a
    # 300 ms sleep achieved 16 evaluations/second, not the 60 this arithmetic
    # implies. The sleep stays at 300 ms because it is also how long a filled
    # leg waits to be noticed.
    quote_evaluation_slots_per_second = (
        quote["max_concurrent_market_evaluations"] * 1_000 / quote["poll_interval_ms"]
    )
    assert quote_evaluation_slots_per_second <= 128
    # The formal one-hour calibration requires 10,000 valid evaluations per
    # funded route. Keep 20% cadence headroom before run-time work is included.
    quote_theoretical_route_cycles_per_hour = 3_600_000 / quote["poll_interval_ms"]
    assert quote_theoretical_route_cycles_per_hour >= 12_000
    assert quote["auto_close"]["enabled"] is False
    assert quote["spread_policy"]["fixed_chain_cost_usd_by_route"]["polymarket_predict"] > 0
    assert quote["spread_policy"]["fixed_chain_cost_usd_by_route"]["polymarket_myriad"] > 0
    assert quote["spread_policy"]["fixed_chain_cost_usd_by_route"]["predict_myriad"] > 0
    assert quote["spread_policy"]["fixed_chain_cost_usd_by_route"]["predict_sx"] > 0
    assert quote["spread_policy"]["fixed_chain_cost_usd_by_route"]["polymarket_sx"] > 0
    assert quote["spread_policy"]["fixed_chain_cost_usd_by_route"]["sx_myriad"] > 0
    assert quote["spread_policy"]["require_live_gas_estimate"] is True
    assert quote["spread_policy"]["gas_units_by_route"]["polymarket_predict"]
    assert quote["spread_policy"]["gas_units_by_route"]["polymarket_myriad"]
    assert quote["spread_policy"]["gas_units_by_route"]["predict_myriad"]
    assert quote["spread_policy"]["gas_units_by_route"]["predict_sx"]
    assert quote["spread_policy"]["gas_units_by_route"]["polymarket_sx"]
    assert quote["spread_policy"]["gas_units_by_route"]["sx_myriad"]
    assert quote["discovery_max_stale_seconds"] == 1800.0
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
