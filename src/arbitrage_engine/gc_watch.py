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
import os
import time
from collections import deque
from collections.abc import Callable
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


SETTLE_SECONDS = 60.0
REFREEZE_INTERVAL_SECONDS = 60.0
FULL_COLLECTION_INTERVAL_SECONDS = 86400.0
# Growth of the resident set since the last full collection that brings the
# next one forward, and the least time between two such collections.
FULL_COLLECTION_RSS_GROWTH_MB = 600.0
MIN_FULL_COLLECTION_SPACING_SECONDS = 3600.0


def _resident_set_mb() -> float | None:
    try:
        with open("/proc/self/statm") as handle:
            pages = int(handle.read().split()[1])
    except (OSError, ValueError, IndexError):
        return None
    sysconf = getattr(os, "sysconf", None)  # absent on Windows, where /proc is too
    page_size = float(sysconf("SC_PAGE_SIZE")) if sysconf is not None else 4096.0
    return pages * page_size / (1024 * 1024)


class GcFreezePolicy:
    """Keep almost all of the heap out of full collections, and sweep it in full only rarely.

    Measured on 2026-10-07 with startup objects frozen: a full collection ran
    about once a minute, paused the trading loop 0.1-0.6 s each time, and found
    0-750 objects to free -- almost all of the heap is discovery data that
    lives until the next cycle replaces it, and replaced data is freed by
    reference counting, not by the collector.

    Freezing once a discovery cycle had settled was not enough: a cycle runs
    every eight minutes and builds hundreds of thousands of objects on the way,
    so the unfrozen part was large again within minutes (35 pauses of 0.1-0.35 s
    in the 70 minutes after release 2 went live). So everything alive is
    frozen every REFREEZE_INTERVAL_SECONDS, and SETTLE_SECONDS after a publish
    as before; a full collection then walks only the objects promoted since.
    Freezing is a constant-time list splice -- its object count is not -- so
    nothing is counted on this path.

    For thirteen hours on 2026-10-10 the interval was five seconds, to shorten
    those walks at 9,000 books a venue -- and it did (the collector's share of
    wall time went from 6.7% to 3.5%). But it froze every object that lived a
    few seconds, the requests and tasks among them, and the cycles those leave
    behind were never collected: the trading process grew from 1.2 GB to
    2.85 GB in nine hours and was killed for memory at 02:53 UTC on
    2026-10-11, taking the funded window and the daily report with it. A
    minute is back, and a full collection also runs early once the resident
    set has grown FULL_COLLECTION_RSS_GROWTH_MB since the last one (at most
    once per MIN_FULL_COLLECTION_SPACING_SECONDS), so a leak of this kind is
    bounded whatever its source.

    What freezing gives up is collecting reference cycles among frozen objects
    (an exception with its traceback, a finished task). Every
    FULL_COLLECTION_INTERVAL_SECONDS everything is unfrozen, collected once and
    frozen again, so such garbage lives at most that long, at the price of one
    full pause per interval.

    That interval was two hours until 2026-10-08, when the pause had grown with
    the subscription width to 3.5-4.3 s at 7,500 books a venue -- most of the
    five seconds after which Polymarket drops a silent stream -- and each sweep
    found only 17,000-24,000 objects. It is a day now: a day of such cycles is
    tens of megabytes against a three-gigabyte limit, and a run rarely lasts
    that long before the operator restarts it.
    """

    def __init__(
        self,
        *,
        settle_seconds: float = SETTLE_SECONDS,
        refreeze_interval_seconds: float = REFREEZE_INTERVAL_SECONDS,
        full_collection_interval_seconds: float = FULL_COLLECTION_INTERVAL_SECONDS,
        clock: Callable[[], float] = time.monotonic,
        resident_set_mb: Callable[[], float | None] = _resident_set_mb,
        rss_growth_mb: float = FULL_COLLECTION_RSS_GROWTH_MB,
        min_full_collection_spacing_seconds: float = MIN_FULL_COLLECTION_SPACING_SECONDS,
    ) -> None:
        self._resident_set_mb = resident_set_mb
        self._rss_growth_mb = rss_growth_mb
        self._min_spacing = min_full_collection_spacing_seconds
        self._settle_seconds = settle_seconds
        self._refreeze_interval_seconds = refreeze_interval_seconds
        self._full_collection_interval_seconds = full_collection_interval_seconds
        self._clock = clock
        now = clock()
        # Startup data (the first discovery, the first plan) settles like any other.
        self._pending_since: float | None = now
        self._last_freeze = now
        self._next_full_collection = now + full_collection_interval_seconds
        self._last_full_collection = now
        # The baseline is taken once startup data has settled and been frozen:
        # measured at construction, before the first discovery loaded, the
        # catalogues alone read as 600 MB of growth and forced a 4.7 s sweep an
        # hour into the run of 2026-10-11 with nothing leaking.
        self._rss_after_full_collection: float | None = None

    def long_lived_data_replaced(self) -> None:
        """A discovery publish: freeze again once the new data has settled."""
        self._pending_since = self._clock()

    def tick(self) -> tuple[str, int, float] | None:
        """Called about once a second. Returns (action, objects collected, seconds) when it did something.

        The periodic freeze is reported as "refrozen"; it happens every minute
        and the caller may leave it out of the log.
        """
        now = self._clock()
        if now >= self._next_full_collection or self._memory_grew(now):
            self._next_full_collection = now + self._full_collection_interval_seconds
            started = time.perf_counter()
            gc.unfreeze()
            collected = gc.collect()
            gc.freeze()
            self._pending_since = None
            self._last_freeze = now
            self._last_full_collection = now
            self._rss_after_full_collection = self._resident_set_mb()
            return "full_collection", collected, time.perf_counter() - started
        settled = self._pending_since is not None and now - self._pending_since >= self._settle_seconds
        if settled or now - self._last_freeze >= self._refreeze_interval_seconds:
            started = time.perf_counter()
            gc.freeze()
            self._pending_since = None
            self._last_freeze = now
            if settled and self._rss_after_full_collection is None:
                self._rss_after_full_collection = self._resident_set_mb()
                self._last_full_collection = now
            return ("frozen" if settled else "refrozen"), 0, time.perf_counter() - started
        return None

    def _memory_grew(self, now: float) -> bool:
        if now - self._last_full_collection < self._min_spacing:
            return False
        baseline = self._rss_after_full_collection
        if baseline is None:
            return False
        current = self._resident_set_mb()
        return current is not None and current - baseline >= self._rss_growth_mb


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
