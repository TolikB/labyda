"""How many books the trading process can keep subscribed right now.

Every subscribed book streams updates that the one trading process has to
parse, and that process runs on one core. On 2026-09-28, 515 books a venue
took 106% of it: the event loop fell 320 ms behind, evaluations dropped from 22
to 4.5 a second, the observers lost the runtime and the run died. So the width
was fixed at 250 -- which on 2026-10-06, with discovery moved to its own
process, left the trading process at 26-32% of a core while 14 400 planned
pairs waited their turn in a five-minute rotation.

A fixed number is wrong at one end of the day or the other: an evening of
football sends many times the updates of a quiet morning. So the width follows
the load instead. At every rotation the process's own CPU share and the worst
event-loop lag since the last one decide: comfortably idle, add a step of books;
loaded, give a quarter back at once; in between, hold. It never goes below the
configured cap, which is the width already proven safe, nor above the ceiling.

Lag is judged by the 95th percentile of the once-a-second probe over the
interval, not by its single worst second. On the night of 2026-10-06 the worst
second came from discovery hand-offs and garbage collection, not from books:
49 of 122 decisions gave a quarter back over one such second while the
process sat at a quarter of a core, so the width never got past 362. A
stall that lasts is still what shrinks it.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

# Below both, there is room for more books. Lag is the interval's 95th
# percentile (see the module docstring). The CPU bar sits at half a core so
# that one step's worth of extra updates, an evening surge and a discovery
# rebuild's post-processing can all land on top without reaching the core.
GROW_BELOW_CPU_FRACTION = 0.50
GROW_BELOW_LAG_SECONDS = 0.25
# Above either, give books back. 70% of a core is well short of where the
# 2026-09-28 run broke; a one-second stall is what the observers start to see.
SHRINK_ABOVE_CPU_FRACTION = 0.70
SHRINK_ABOVE_LAG_SECONDS = 1.0
GROW_STEP_BOOKS = 50
SHRINK_FACTOR = 0.75


@dataclass(frozen=True)
class BudgetDecision:
    action: str
    cpu_fraction: float
    lag_seconds: float
    budgets: Mapping[str, int]


class SubscriptionBudget:
    """Per-venue subscription widths that grow under light load and shrink under heavy load.

    Only venues with a ceiling above their floor adapt. A venue whose books are
    polled rather than streamed costs requests, not parsing, and keeps its fixed
    cap by having no ceiling.
    """

    def __init__(self, floors: Mapping[str, int], ceilings: Mapping[str, int]) -> None:
        self._floors = {venue: floors[venue] for venue, ceiling in ceilings.items() if ceiling > floors[venue]}
        self._ceilings = {venue: ceilings[venue] for venue in self._floors}
        self._budgets = dict(self._floors)

    @property
    def adaptive(self) -> bool:
        return bool(self._budgets)

    def budget_for(self, venue: str, fixed: int) -> int:
        return self._budgets.get(venue, fixed)

    def observe(self, cpu_fraction: float, lag_seconds: float) -> BudgetDecision:
        if cpu_fraction > SHRINK_ABOVE_CPU_FRACTION or lag_seconds > SHRINK_ABOVE_LAG_SECONDS:
            action = "shrink"
            self._budgets = {
                venue: max(self._floors[venue], int(budget * SHRINK_FACTOR)) for venue, budget in self._budgets.items()
            }
        elif cpu_fraction < GROW_BELOW_CPU_FRACTION and lag_seconds < GROW_BELOW_LAG_SECONDS:
            action = "grow"
            self._budgets = {
                venue: min(self._ceilings[venue], budget + GROW_STEP_BOOKS) for venue, budget in self._budgets.items()
            }
        else:
            action = "hold"
        return BudgetDecision(action, cpu_fraction, lag_seconds, dict(self._budgets))
