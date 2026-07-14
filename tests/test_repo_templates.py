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
    for name in ("config.json", "config.shadow_sports.json"):
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
