"""The rewards shadow's simulated quote: scoring, queue position, fills, markouts."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from typing import Any

import pytest

_PATH = Path(__file__).resolve().parents[1] / "scripts" / "rewards_shadow.py"
_SPEC = importlib.util.spec_from_file_location("rewards_shadow", _PATH)
assert _SPEC is not None and _SPEC.loader is not None
shadow = importlib.util.module_from_spec(_SPEC)
sys.modules["rewards_shadow"] = shadow
_SPEC.loader.exec_module(shadow)

NOW = 1_800_000_000.0


def _market(**overrides: object) -> Any:
    fields: dict[str, object] = {
        "condition_id": "0xc",
        "question": "Will it rain?",
        "yes_token": "yes",
        "no_token": "no",
        "daily": 144.0,
        "max_spread": 0.035,
        "min_size": 20.0,
        "tick": 0.01,
        "end": NOW + 7 * 86400,
    }
    fields.update(overrides)
    return shadow.RewardMarket(**fields)


def _state(bids: dict[float, float], asks: dict[float, float], **market: object) -> Any:
    state = shadow.MarketState(_market(**market), dict(bids), dict(asks))
    shadow.requote(state, NOW)
    return state


def test_the_documented_score_and_the_single_sided_third() -> None:
    assert shadow.order_score(0.035, 0.0) == 1.0
    assert shadow.order_score(0.035, 0.035) == 0.0
    assert shadow.order_score(0.04, 0.02) == pytest.approx(0.25)
    # Inside 0.10-0.90 one side alone still counts a third; outside it, it does not count.
    assert shadow.q_min(9.0, 0.0, 0.5) == pytest.approx(3.0)
    assert shadow.q_min(9.0, 0.0, 0.05) == 0.0
    assert shadow.q_min(4.0, 6.0, 0.5) == 4.0


def test_a_narrow_book_is_joined_and_a_wide_one_quoted_inside() -> None:
    joined = _state({0.48: 100.0}, {0.50: 50.0})
    assert (joined.bid.price, joined.ask.price) == (0.48, 0.50)
    # Joining puts us behind everything already at the price.
    assert joined.bid.queue_ahead == 100.0
    wide = _state({0.30: 10.0}, {0.70: 10.0})
    assert wide.bid.price == pytest.approx(0.48) and wide.ask.price == pytest.approx(0.52)
    assert wide.bid.queue_ahead == 0.0


def test_no_quotes_near_the_end_or_past_the_inventory_limit() -> None:
    assert _state({0.48: 1.0}, {0.50: 1.0}, end=NOW + 3600).bid is None
    state = _state({0.48: 1.0}, {0.50: 1.0})
    state.position = 3 * 20.0
    shadow.requote(state, NOW)
    assert state.bid is None and state.ask is not None


def test_trades_eat_the_queue_before_us_and_a_trade_through_fills_us_whole() -> None:
    state = _state({0.48: 30.0}, {0.50: 50.0})
    assert shadow.apply_trade(state, 0.48, "SELL", 25.0, NOW) == 0.0
    assert state.bid.queue_ahead == 5.0
    assert shadow.apply_trade(state, 0.48, "SELL", 15.0, NOW) == 10.0
    assert state.position == 10.0 and state.cash == pytest.approx(-4.8)
    # A sell printing below our price took our level whole.
    assert shadow.apply_trade(state, 0.47, "SELL", 1.0, NOW) == 10.0
    assert state.bid is None and state.position == 20.0
    # A buy lifts our ask the same way.
    assert shadow.apply_trade(state, 0.51, "BUY", 1.0, NOW) == 20.0
    assert state.position == 0.0


def test_size_cancelled_ahead_moves_us_up() -> None:
    state = _state({0.48: 30.0}, {0.50: 50.0})
    state.bids[0.48] = 12.0
    shadow.requote(state, NOW)
    assert state.bid.queue_ahead == 12.0


def test_fills_are_marked_against_the_later_mid() -> None:
    state = _state({0.48: 0.0}, {0.50: 50.0})
    shadow.apply_trade(state, 0.48, "SELL", 20.0, NOW)
    state.bids, state.asks = {0.40: 10.0}, {0.42: 10.0}
    shadow.settle_markouts(state, NOW + 30)
    assert state.markout[30] == pytest.approx((0.41 - 0.48) * 20)
    assert state.markout[1800] == 0.0
    shadow.settle_markouts(state, NOW + 1800)
    assert state.markout[1800] == pytest.approx((0.41 - 0.48) * 20)


def test_rewards_accrue_by_our_share_of_the_score() -> None:
    # Alone in the band, we take the whole minute of the pool.
    alone = _state({0.30: 100.0}, {0.70: 100.0})
    shadow.accrue_rewards(alone, 60)
    assert alone.reward_opt == pytest.approx(144.0 / 1440)
    # Against an equal rival on both sides at the same prices, half.
    shared = _state({0.48: 20.0}, {0.50: 20.0})
    shadow.accrue_rewards(shared, 60)
    assert shared.reward_opt == pytest.approx(144.0 / 1440 / 2)
    assert shared.reward_pes == pytest.approx(shared.reward_opt)


def test_a_no_token_trade_is_read_on_the_yes_book() -> None:
    sim = shadow.Shadow.__new__(shadow.Shadow)
    state = _state({0.48: 0.0}, {0.50: 50.0})
    sim.states = {"0xc": state}
    sim.by_token = {"yes": (state, True), "no": (state, False)}
    # Buying NO at 0.52 is selling YES at 0.48: it hits our bid.
    sim.handle({"event_type": "last_trade_price", "asset_id": "no", "price": "0.52", "size": "20",
                "side": "BUY", "transaction_hash": "0x1"}, NOW)
    assert state.position == 20.0
    # The same print delivered again is not a second fill.
    sim.handle({"event_type": "last_trade_price", "asset_id": "no", "price": "0.52", "size": "20",
                "side": "BUY", "transaction_hash": "0x1"}, NOW)
    assert state.position == 20.0
