"""Decide which market pairs to evaluate now, from what the venues just sent.

The engine used to hold a small window of pairs and rotate it on a timer: 24
slots over ~1,180 live Polymarket <-> Predict.fun pairs, rotated every three
seconds, which meant each pair was looked at for three seconds once every five
minutes. Whatever happened to a pair in the other 99% of the time was invisible
-- and in the 57 hours to 2026-09-25 only 8 of 648,000 measured spreads on that
route were positive at all, which is a statement about the 1% we watched, not
about the market.

This schedules on evidence instead of on a clock. Every subscribed book carries
a receipt time; a pair whose leg receipt advanced since we last looked at it has
moved and is worth recomputing, and a pair whose books have not moved cannot
have changed its spread. The cycle budget then goes to the pairs that moved,
newest first, with the pairs that recently showed executable edge ahead of them,
and whatever budget is left goes to the pairs nobody has looked at in a while so
a quiet book cannot starve.

Nothing here talks to a venue or to the engine: it takes the planned pairs, a
receipt lookup and a clock, and returns the batch to run. That is what makes it
testable, and this is the component whose behaviour decides whether the engine
sees the market or a slice of it.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, field
from typing import Protocol


class _Schedulable(Protocol):
    """The part of a planned evaluation the scheduler needs to see.

    Read-only members, so a frozen dataclass satisfies it -- which the engine's
    planned evaluation is.
    """

    @property
    def route(self) -> str: ...

    @property
    def targets(self) -> tuple[tuple[str, str], ...]: ...


# A receipt lookup returns the venue's monotonic receipt time for one target,
# or None when that connector cannot report one.
ReceiptLookup = Callable[[str, str], float | None]


@dataclass(frozen=True)
class SchedulerDecision[EvaluationT: _Schedulable]:
    """What to evaluate this cycle, and what the queue looked like."""

    batch: tuple[EvaluationT, ...]
    moved: int
    """Pairs whose books advanced since the scheduler last looked at them."""
    deferred: int
    """Moved pairs the cycle budget could not reach; they stay queued."""
    refreshed: int
    """Pairs in the batch that had not moved but were past the staleness bound."""
    oldest_evaluation_age_seconds: float | None
    """Age of the least recently evaluated pair, after this batch is applied."""


@dataclass
class _PairState:
    receipts: tuple[float | None, ...]
    last_evaluated_at: float
    moved_at: float | None = None


@dataclass
class EvaluationScheduler[EvaluationT: _Schedulable]:
    """Pick the pairs to evaluate from the books that moved.

    ``budget_for`` and ``priority_targets`` are supplied by the engine so the
    per-route caps and the "recently executable" memory stay where they are
    configured and measured.
    """

    max_per_cycle: int
    max_staleness_seconds: float
    budget_for: Callable[[str], int] = lambda route: 1_000_000
    _state: dict[tuple[str, tuple[tuple[str, str], ...]], _PairState] = field(default_factory=dict)

    def forget_missing(self, evaluations: Iterable[EvaluationT]) -> None:
        """Drop state for pairs discovery no longer plans, so it cannot leak."""
        live = {(evaluation.route, evaluation.targets) for evaluation in evaluations}
        for key in tuple(self._state):
            if key not in live:
                del self._state[key]

    def decide(
        self,
        evaluations: Sequence[EvaluationT],
        receipt: ReceiptLookup,
        now: float,
        *,
        priority_targets: frozenset[tuple[str, tuple[str, ...]]] | None = None,
    ) -> SchedulerDecision[EvaluationT]:
        if not evaluations:
            self._state.clear()
            return SchedulerDecision((), 0, 0, 0, None)

        priority = priority_targets or frozenset()
        moved: list[tuple[float, EvaluationT]] = []
        quiet: list[tuple[float, EvaluationT]] = []
        receipts_by_key: dict[tuple[str, tuple[tuple[str, str], ...]], tuple[float | None, ...]] = {}

        for evaluation in evaluations:
            key = (evaluation.route, evaluation.targets)
            current = tuple(receipt(venue, token_id) for venue, token_id in evaluation.targets)
            receipts_by_key[key] = current
            state = self._state.get(key)
            if state is None:
                # Never looked at: treat as moved so a new pair is seen at once
                # rather than waiting out the staleness bound.
                moved.append((now, evaluation))
                continue
            if _advanced(state.receipts, current):
                moved.append((_newest(current) or now, evaluation))
            elif now - state.last_evaluated_at >= self.max_staleness_seconds:
                quiet.append((state.last_evaluated_at, evaluation))

        # Freshest first, and a pair that recently showed executable edge ahead
        # of pairs that merely ticked: when the budget binds, that is the one
        # whose next tick is worth spending it on.
        moved.sort(
            key=lambda item: (
                0 if (item[1].route, tuple(token for _, token in item[1].targets)) in priority else 1,
                -item[0],
            )
        )
        quiet.sort(key=lambda item: item[0])

        batch: list[EvaluationT] = []
        per_route = dict.fromkeys({evaluation.route for evaluation in evaluations}, 0)
        refreshed = 0
        for source, is_quiet in ((moved, False), (quiet, True)):
            for _, evaluation in source:
                if len(batch) >= self.max_per_cycle:
                    break
                if per_route[evaluation.route] >= self.budget_for(evaluation.route):
                    continue
                batch.append(evaluation)
                per_route[evaluation.route] += 1
                refreshed += int(is_quiet)

        # Only the batch records what it saw. A moved pair the budget could not
        # reach keeps its old receipts and is therefore still moved next cycle:
        # the queue is this state, not a list, so a busy market cannot be
        # skipped precisely because it is busy.
        for evaluation in batch:
            key = (evaluation.route, evaluation.targets)
            self._state[key] = _PairState(receipts=receipts_by_key[key], last_evaluated_at=now)

        oldest = min((state.last_evaluated_at for state in self._state.values()), default=None)
        return SchedulerDecision(
            batch=tuple(batch),
            moved=len(moved),
            deferred=max(0, len(moved) - (len(batch) - refreshed)),
            refreshed=refreshed,
            oldest_evaluation_age_seconds=None if oldest is None else max(0.0, now - oldest),
        )


def _advanced(previous: tuple[float | None, ...], current: tuple[float | None, ...]) -> bool:
    if len(previous) != len(current):
        return True
    for before, after in zip(previous, current, strict=True):
        if after is None:
            continue
        if before is None or after > before:
            return True
    return False


def _newest(receipts: tuple[float | None, ...]) -> float | None:
    known = [receipt for receipt in receipts if receipt is not None]
    return max(known) if known else None
