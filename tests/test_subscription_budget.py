"""The subscription width follows the trading process's load, between a proven floor and a ceiling."""

from __future__ import annotations

from arbitrage_engine.subscription_budget import (
    GROW_STEP_BOOKS,
    SubscriptionBudget,
)


def _budget() -> SubscriptionBudget:
    return SubscriptionBudget({"Polymarket": 250, "Predict.fun": 250}, {"Polymarket": 800, "Predict.fun": 800})


def test_it_starts_at_the_floor_and_grows_a_step_while_the_process_is_idle() -> None:
    budget = _budget()
    assert budget.budget_for("Polymarket", 250) == 250

    decision = budget.observe(cpu_fraction=0.30, lag_seconds=0.01)

    assert decision.action == "grow"
    assert decision.budgets == {"Polymarket": 250 + GROW_STEP_BOOKS, "Predict.fun": 250 + GROW_STEP_BOOKS}
    assert budget.budget_for("Polymarket", 250) == 300


def test_it_stops_at_the_ceiling() -> None:
    budget = _budget()
    for _ in range(50):
        budget.observe(cpu_fraction=0.10, lag_seconds=0.0)

    assert budget.budget_for("Polymarket", 250) == 800
    assert budget.budget_for("Predict.fun", 250) == 800


def test_load_or_a_stall_gives_a_quarter_back_but_never_below_the_floor() -> None:
    budget = _budget()
    for _ in range(4):
        budget.observe(cpu_fraction=0.10, lag_seconds=0.0)
    assert budget.budget_for("Polymarket", 250) == 450

    assert budget.observe(cpu_fraction=0.85, lag_seconds=0.0).action == "shrink"
    assert budget.budget_for("Polymarket", 250) == 337
    assert budget.observe(cpu_fraction=0.20, lag_seconds=1.5).action == "shrink"
    assert budget.budget_for("Polymarket", 250) == 252
    budget.observe(cpu_fraction=2.0, lag_seconds=9.0)
    assert budget.budget_for("Polymarket", 250) == 250


def test_between_the_bars_it_holds() -> None:
    budget = _budget()
    budget.observe(cpu_fraction=0.10, lag_seconds=0.0)

    # Busy enough not to add, not so busy as to give back.
    assert budget.observe(cpu_fraction=0.60, lag_seconds=0.0).action == "hold"
    assert budget.observe(cpu_fraction=0.20, lag_seconds=0.5).action == "hold"
    assert budget.budget_for("Polymarket", 250) == 300


def test_a_polled_venue_without_a_ceiling_keeps_its_cap() -> None:
    budget = SubscriptionBudget({"Polymarket": 250}, {"Polymarket": 800})
    budget.observe(cpu_fraction=0.10, lag_seconds=0.0)

    assert budget.budget_for("Myriad", 18) == 18
    assert budget.adaptive


def test_no_ceilings_means_no_adaptation() -> None:
    budget = SubscriptionBudget({}, {})

    assert not budget.adaptive
    assert budget.budget_for("Polymarket", 250) == 250


def test_a_venue_reconnect_gives_width_back_however_light_the_load() -> None:
    # CPU and loop lag cannot see a gateway's own limit; a dropped stream can.
    budget = _budget()
    budget.observe(cpu_fraction=0.10, lag_seconds=0.0)
    grown = dict(budget.observe(cpu_fraction=0.10, lag_seconds=0.0).budgets)

    decision = budget.observe(cpu_fraction=0.10, lag_seconds=0.0, venue_reconnected=True)

    assert decision.action == "shrink"
    assert all(decision.budgets[venue] < grown[venue] for venue in grown)
    assert budget.venues == ("Polymarket", "Predict.fun")
