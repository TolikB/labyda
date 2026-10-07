"""Garbage-collector pauses in the trading process: timed per generation, the long ones kept for the log.

On 2026-10-06 the event loop stood still for 0.4-0.6 s every one to three minutes
outside any discovery work, each time at a line that allocates small tuples. A
full (generation 2) collection over the ~1.4 million objects the process holds
takes about that long, and the allocation that triggers it is wherever the loop
happens to be. This makes that visible instead of inferred.

The callback runs inside the collector, in whichever thread triggered it, so it
only does arithmetic; the observability loop reads the totals and drains the
long pauses once a second.
"""

from __future__ import annotations

import gc
import time
from collections import deque
from typing import Any

LONG_PAUSE_SECONDS = 0.1
_KEPT_LONG_PAUSES = 256


class GcPauseWatch:
    def __init__(self) -> None:
        self.collections = [0, 0, 0]
        self.seconds = [0.0, 0.0, 0.0]
        self.longest = [0.0, 0.0, 0.0]
        self._started = 0.0
        self._long_pauses: deque[tuple[int, float, int]] = deque(maxlen=_KEPT_LONG_PAUSES)
        self._installed = False

    def install(self) -> None:
        if not self._installed:
            gc.callbacks.append(self._callback)
            self._installed = True

    def uninstall(self) -> None:
        if self._installed:
            gc.callbacks.remove(self._callback)
            self._installed = False

    def drain_long_pauses(self) -> list[tuple[int, float, int]]:
        """(generation, seconds, objects collected) for each pause of LONG_PAUSE_SECONDS or more since the last call."""
        drained = []
        while self._long_pauses:
            drained.append(self._long_pauses.popleft())
        return drained

    def _callback(self, phase: str, info: dict[str, Any]) -> None:
        if phase == "start":
            self._started = time.perf_counter()
            return
        duration = time.perf_counter() - self._started
        generation = int(info.get("generation", 0))
        if not 0 <= generation <= 2:
            return
        self.collections[generation] += 1
        self.seconds[generation] += duration
        if duration > self.longest[generation]:
            self.longest[generation] = duration
        if duration >= LONG_PAUSE_SECONDS:
            self._long_pauses.append((generation, duration, int(info.get("collected", 0))))


def freeze_startup_objects() -> int:
    """Move everything alive now into the collector's permanent generation and return how many objects that was.

    Called once, after the imports and the long-lived setup and before any
    market data arrives: modules, classes, SDKs and config are never garbage,
    and a full collection no longer walks them. Objects frozen here that do
    become garbage later are simply never collected, which is why this runs
    before discovery fills the process with data that is replaced every cycle.
    """
    gc.collect()
    gc.freeze()
    return gc.get_freeze_count()
