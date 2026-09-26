"""The scheduler decides what the engine sees, so its rules are pinned here.

The window it replaces gave each of ~1,180 Polymarket <-> Predict.fun pairs
three seconds of attention every five minutes. These tests are about the
property that replaces it: a pair is evaluated because its book moved.
"""

from __future__ import annotations

from dataclasses import dataclass

from arbitrage_engine.evaluation_scheduler import EvaluationScheduler


@dataclass(frozen=True)
class FakeEvaluation:
    route: str
    targets: tuple[tuple[str, str], ...]


def pair(route: str, first: str, second: str) -> FakeEvaluation:
    return FakeEvaluation(route, (("Polymarket", first), ("Predict.fun", second)))


class Receipts:
    """A venue's per-target receipt clock, driven by the test."""

    def __init__(self) -> None:
        self.values: dict[tuple[str, str], float | None] = {}

    def set(self, venue: str, token_id: str, receipt: float | None) -> None:
        self.values[(venue, token_id)] = receipt

    def __call__(self, venue: str, token_id: str) -> float | None:
        return self.values.get((venue, token_id))


def test_a_pair_is_evaluated_when_its_book_moves_and_not_otherwise() -> None:
    receipts = Receipts()
    evaluations = [pair("polymarket_predict", "a1", "b1"), pair("polymarket_predict", "a2", "b2")]
    for token, venue in (("a1", "Polymarket"), ("a2", "Polymarket")):
        receipts.set(venue, token, 100.0)
    receipts.set("Predict.fun", "b1", 100.0)
    receipts.set("Predict.fun", "b2", 100.0)
    scheduler: EvaluationScheduler[FakeEvaluation] = EvaluationScheduler(
        max_per_cycle=10,
        max_staleness_seconds=60.0,
    )

    # First sight of a pair counts as movement: a new market is looked at at
    # once rather than waiting out the staleness bound.
    first = scheduler.decide(evaluations, receipts, now=1_000.0)
    assert set(first.batch) == set(evaluations)
    assert first.moved == 2

    # Nothing moved and nothing is stale: no work at all. This is the whole
    # point -- the spread cannot have changed if neither book did.
    quiet = scheduler.decide(evaluations, receipts, now=1_001.0)
    assert quiet.batch == ()
    assert quiet.moved == 0

    # One venue sends a new book for one pair: exactly that pair is evaluated.
    receipts.set("Predict.fun", "b2", 101.5)
    moved = scheduler.decide(evaluations, receipts, now=1_002.0)
    assert moved.batch == (evaluations[1],)
    assert moved.moved == 1


def test_the_freshest_book_goes_first_and_recent_edge_goes_ahead_of_it() -> None:
    receipts = Receipts()
    stale_but_promising = pair("polymarket_predict", "a1", "b1")
    freshest = pair("polymarket_predict", "a2", "b2")
    middle = pair("polymarket_predict", "a3", "b3")
    evaluations = [stale_but_promising, freshest, middle]
    for index, evaluation in enumerate(evaluations, start=1):
        for venue, token in evaluation.targets:
            receipts.set(venue, token, 100.0 + index)
    scheduler: EvaluationScheduler[FakeEvaluation] = EvaluationScheduler(
        max_per_cycle=10,
        max_staleness_seconds=60.0,
    )
    scheduler.decide(evaluations, receipts, now=1_000.0)

    receipts.set("Predict.fun", "b1", 200.0)
    receipts.set("Predict.fun", "b2", 202.0)
    receipts.set("Predict.fun", "b3", 201.0)

    # No priority: freshest receipt first.
    plain = scheduler.decide(evaluations, receipts, now=1_001.0)
    assert plain.batch == (freshest, middle, stale_but_promising)

    receipts.set("Predict.fun", "b1", 300.0)
    receipts.set("Predict.fun", "b2", 302.0)
    receipts.set("Predict.fun", "b3", 301.0)
    # With a recent executable observation on the oldest tick, that pair leads.
    prioritised = scheduler.decide(
        evaluations,
        receipts,
        now=1_002.0,
        priority_targets=frozenset({("polymarket_predict", ("a1", "b1"))}),
    )
    assert prioritised.batch[0] is stale_but_promising


def test_a_deferred_pair_stays_queued_until_the_budget_reaches_it() -> None:
    # The cycle budget is the protection for a 2 vCPU host; a pair it cannot
    # reach must not lose its movement, or a busy market would be skipped
    # exactly when it is busiest.
    receipts = Receipts()
    evaluations = [pair("polymarket_predict", f"a{index}", f"b{index}") for index in range(5)]
    for index, evaluation in enumerate(evaluations):
        for venue, token in evaluation.targets:
            receipts.set(venue, token, 100.0 + index)
    scheduler: EvaluationScheduler[FakeEvaluation] = EvaluationScheduler(
        max_per_cycle=2,
        max_staleness_seconds=60.0,
    )
    first = scheduler.decide(evaluations, receipts, now=1_000.0)
    assert len(first.batch) == 2
    assert first.deferred == 3

    second = scheduler.decide(evaluations, receipts, now=1_000.5)
    assert len(second.batch) == 2
    assert set(second.batch).isdisjoint(first.batch)

    third = scheduler.decide(evaluations, receipts, now=1_001.0)
    assert len(third.batch) == 1
    assert set(first.batch) | set(second.batch) | set(third.batch) == set(evaluations)


def test_a_quiet_book_is_refreshed_before_the_staleness_bound_passes_it_by() -> None:
    # Myriad's books can sit unchanged for minutes. Those pairs still need a
    # periodic recompute: fees, chain cost and the other leg's fee quote move
    # even when the book does not.
    receipts = Receipts()
    evaluations = [pair("polymarket_myriad", "a1", "b1")]
    for venue, token in evaluations[0].targets:
        receipts.set(venue, token, 100.0)
    scheduler: EvaluationScheduler[FakeEvaluation] = EvaluationScheduler(
        max_per_cycle=10,
        max_staleness_seconds=30.0,
    )
    scheduler.decide(evaluations, receipts, now=1_000.0)

    assert scheduler.decide(evaluations, receipts, now=1_029.0).batch == ()
    refreshed = scheduler.decide(evaluations, receipts, now=1_030.0)
    assert refreshed.batch == (evaluations[0],)
    assert refreshed.refreshed == 1
    assert refreshed.moved == 0


def test_a_venue_without_receipts_is_still_evaluated_on_the_staleness_cadence() -> None:
    # SX Bet and Opinion report no per-target receipt. Treating "no receipt" as
    # "never moved" would silently stop evaluating those routes.
    receipts = Receipts()
    evaluations = [FakeEvaluation("polymarket_sx", (("Polymarket", "a1"), ("SX Bet", "s1")))]
    receipts.set("Polymarket", "a1", 100.0)
    receipts.set("SX Bet", "s1", None)
    scheduler: EvaluationScheduler[FakeEvaluation] = EvaluationScheduler(
        max_per_cycle=10,
        max_staleness_seconds=15.0,
    )
    assert scheduler.decide(evaluations, receipts, now=1_000.0).batch == (evaluations[0],)
    assert scheduler.decide(evaluations, receipts, now=1_005.0).batch == ()
    assert scheduler.decide(evaluations, receipts, now=1_015.0).batch == (evaluations[0],)


def test_per_route_budgets_keep_one_busy_route_from_taking_the_cycle() -> None:
    # Predict.fun has ~1,180 live pairs against Myriad's ~24. Without a per-route
    # cap the busy route would take every slot and the funded Myriad route would
    # starve -- which is how calibration failed on 2026-09-25.
    receipts = Receipts()
    busy = [pair("polymarket_predict", f"a{index}", f"b{index}") for index in range(10)]
    quiet_route = [
        FakeEvaluation("polymarket_myriad", (("Polymarket", f"m{index}"), ("Myriad", f"y{index}")))
        for index in range(4)
    ]
    evaluations = [*busy, *quiet_route]
    for index, evaluation in enumerate(evaluations):
        for venue, token in evaluation.targets:
            receipts.set(venue, token, 100.0 + index)
    budgets = {"polymarket_predict": 3, "polymarket_myriad": 2}
    scheduler: EvaluationScheduler[FakeEvaluation] = EvaluationScheduler(
        max_per_cycle=10,
        max_staleness_seconds=60.0,
        budget_for=lambda route: budgets[route],
    )
    decision = scheduler.decide(evaluations, receipts, now=1_000.0)
    by_route = {route: len([item for item in decision.batch if item.route == route]) for route in budgets}
    assert by_route == {"polymarket_predict": 3, "polymarket_myriad": 2}


def test_state_for_pairs_discovery_dropped_does_not_leak() -> None:
    receipts = Receipts()
    evaluations = [pair("polymarket_predict", "a1", "b1"), pair("polymarket_predict", "a2", "b2")]
    for venue, token in (("Polymarket", "a1"), ("Polymarket", "a2")):
        receipts.set(venue, token, 100.0)
    receipts.set("Predict.fun", "b1", 100.0)
    receipts.set("Predict.fun", "b2", 100.0)
    scheduler: EvaluationScheduler[FakeEvaluation] = EvaluationScheduler(
        max_per_cycle=10,
        max_staleness_seconds=60.0,
    )
    scheduler.decide(evaluations, receipts, now=1_000.0)
    scheduler.forget_missing(evaluations[:1])
    assert len(scheduler._state) == 1  # noqa: SLF001

    # An empty plan clears everything rather than holding a stale universe.
    assert scheduler.decide([], receipts, now=1_001.0).batch == ()
    assert scheduler._state == {}  # noqa: SLF001
