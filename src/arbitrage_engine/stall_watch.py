"""What the trading loop was running when it stood still.

The once-a-second lag probe says how long the loop stalled, not where. On
2026-10-08 at 7,000-7,500 books a venue it recorded stalls of 4.75 s and
5.85 s that matched neither a garbage collection nor a rotation, and
Polymarket drops a stream it has not heard from in five seconds -- so each
one is a reconnect and a quarter of the subscription width given back.
Catching them with a profiler means sampling for hours.

A watchdog thread does it instead. The loop marks a heartbeat four times a
second; when the heartbeat is older than the threshold the thread reads the
loop thread's current stack and keeps it, again every further threshold while
the stall lasts. A stall inside Python code is caught in the act, because the
interpreter hands the GIL to other threads every few milliseconds. One inside a
C call that holds the GIL (a collection, a pickle) is only seen once it returns,
and those are already timed elsewhere.

When the loop is waiting for the GIL rather than running, its own stack only
says where it was parked; whoever holds the GIL is another thread. So each
capture also keeps the innermost frames of every other thread that is in
Python code -- on 2026-10-10 an 8 s stall caught the loop at a one-line lambda.

The thread only reads frames and appends to a deque; the loop drains and logs.
"""

from __future__ import annotations

import sys
import threading
import time
import traceback
from collections import deque
from collections.abc import Callable
from typing import Any

STALL_THRESHOLD_SECONDS = 1.5
POLL_SECONDS = 0.25
BEAT_SECONDS = 0.25
_STACK_FRAMES = 14
_OTHER_THREAD_FRAMES = 6
_CAPTURES_PER_STALL = 4
_KEPT_STALLS = 64


class LoopStallWatch:
    def __init__(
        self,
        *,
        threshold_seconds: float = STALL_THRESHOLD_SECONDS,
        poll_seconds: float = POLL_SECONDS,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._threshold = threshold_seconds
        self._poll = poll_seconds
        self._clock = clock
        self._last_beat = clock()
        self._loop_thread_id: int | None = None
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()
        # The stall in progress: when it was first seen and the stacks taken so far.
        self._stall_started: float | None = None
        self._stall_stacks: list[tuple[float, list[str]]] = []
        self._finished: deque[tuple[float, list[tuple[float, list[str]]]]] = deque(maxlen=_KEPT_STALLS)
        self._lock = threading.Lock()

    def beat(self) -> None:
        """Called on the loop, every BEAT_SECONDS."""
        self._last_beat = self._clock()

    def start(self, loop_thread_id: int) -> None:
        if self._thread is not None:
            return
        self._loop_thread_id = loop_thread_id
        self._last_beat = self._clock()
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="loop-stall-watch", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=2.0)
            self._thread = None

    def drain(self) -> list[tuple[float, list[tuple[float, list[str]]]]]:
        """(stall seconds, [(seconds into the stall, stack lines)]) for each stall that has ended."""
        with self._lock:
            drained = list(self._finished)
            self._finished.clear()
        return drained

    def _run(self) -> None:
        while not self._stop.wait(self._poll):
            self.check()

    def check(self) -> None:
        """One look at the heartbeat. Public so tests can drive it without the thread."""
        now = self._clock()
        last_beat = self._last_beat
        idle = now - last_beat
        if idle < self._threshold:
            if self._stall_started is not None:
                # The loop is back: the stall lasted from the last beat before it
                # to the first beat after it.
                with self._lock:
                    self._finished.append((last_beat - self._stall_started, self._stall_stacks))
                self._stall_started = None
                self._stall_stacks = []
            return
        if self._stall_started is None:
            self._stall_started = now - idle
        captures = len(self._stall_stacks)
        if captures < _CAPTURES_PER_STALL and idle >= self._threshold * (captures + 1):
            self._stall_stacks.append((round(idle, 2), self._loop_stack()))

    def _loop_stack(self) -> list[str]:
        frames = sys._current_frames()
        frame = frames.get(self._loop_thread_id or 0)
        lines = [] if frame is None else _format(frame, _STACK_FRAMES)
        names = {thread.ident: thread.name for thread in threading.enumerate()}
        own = threading.get_ident()
        for ident, other in frames.items():
            if ident in (own, self._loop_thread_id):
                continue
            stack = _format(other, _OTHER_THREAD_FRAMES)
            # Threads parked in a lock or a selector are not who holds the GIL.
            if stack and not stack[-1].startswith(("wait ", "select ", "_worker ", "get ")):
                lines.append(f"--- thread {names.get(ident, ident)}")
                lines.extend(stack)
        return lines


def _format(frame: Any, limit: int) -> list[str]:
    summary = traceback.extract_stack(frame)[-limit:]
    return [f"{entry.name} ({_short(entry.filename)}:{entry.lineno})" for entry in summary]


def _short(filename: str) -> str:
    for marker in ("site-packages/", "arbitrage_engine/", "lib/python"):
        index = filename.find(marker)
        if index >= 0:
            return filename[index:]
    return filename
