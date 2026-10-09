from __future__ import annotations

import threading

import pytest

from arbitrage_engine.stall_watch import LoopStallWatch


class _Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


def _watch(clock: _Clock) -> LoopStallWatch:
    watch = LoopStallWatch(threshold_seconds=1.5, clock=clock)
    # The test thread stands in for the loop, so the captured stack is ours.
    watch._loop_thread_id = threading.get_ident()  # noqa: SLF001
    return watch


def test_a_loop_that_keeps_beating_reports_nothing() -> None:
    clock = _Clock()
    watch = _watch(clock)
    for _ in range(20):
        clock.now += 0.25
        watch.beat()
        watch.check()

    assert watch.drain() == []


def test_a_stall_is_reported_once_it_ends_with_the_stack_it_was_caught_in() -> None:
    clock = _Clock()
    watch = _watch(clock)
    watch.beat()

    clock.now += 1.0
    watch.check()  # a second without a beat is not a stall yet
    clock.now += 0.6
    watch.check()  # 1.6 s: first capture
    clock.now += 1.5
    watch.check()  # 3.1 s: second capture
    assert watch.drain() == []  # still stalled: nothing to report yet

    clock.now += 0.2
    watch.beat()  # the loop is back after 3.3 s
    clock.now += 0.1
    watch.check()

    [(seconds, stacks)] = watch.drain()
    assert seconds == pytest.approx(3.3)
    assert [at for at, _ in stacks] == [1.6, 3.1]
    assert any("test_a_stall_is_reported_once_it_ends" in line for line in stacks[0][1])
    assert watch.drain() == []


def test_a_long_stall_keeps_only_the_first_few_stacks() -> None:
    clock = _Clock()
    watch = _watch(clock)
    watch.beat()
    for _ in range(60):
        clock.now += 0.25
        watch.check()
    watch.beat()
    clock.now += 0.25
    watch.check()

    [(seconds, stacks)] = watch.drain()
    assert seconds == pytest.approx(15.0)
    assert len(stacks) == 4


def test_the_thread_starts_and_stops() -> None:
    watch = LoopStallWatch(poll_seconds=0.01)
    watch.start(threading.get_ident())
    watch.beat()
    watch.stop()

    assert watch.drain() == []
