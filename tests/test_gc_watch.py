import gc

import pytest

from arbitrage_engine import gc_watch
from arbitrage_engine.gc_watch import GcPauseWatch


def test_a_full_collection_is_counted_and_timed() -> None:
    watch = GcPauseWatch()
    watch.install()
    try:
        gc.collect(2)
    finally:
        watch.uninstall()

    assert watch.collections[2] >= 1
    assert watch.seconds[2] >= 0.0
    assert watch.longest[2] >= 0.0
    assert watch._callback not in gc.callbacks  # noqa: SLF001


def test_long_pauses_are_kept_for_the_log_and_drained_once(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(gc_watch, "LONG_PAUSE_SECONDS", 0.0)
    watch = GcPauseWatch()
    watch.install()
    try:
        gc.collect(2)
    finally:
        watch.uninstall()

    drained = watch.drain_long_pauses()
    assert any(generation == 2 for generation, _seconds, _collected in drained)
    assert watch.drain_long_pauses() == []


def test_installing_twice_registers_one_callback() -> None:
    watch = GcPauseWatch()
    watch.install()
    watch.install()
    try:
        assert sum(callback == watch._callback for callback in gc.callbacks) == 1  # noqa: SLF001
    finally:
        watch.uninstall()
    watch.uninstall()
    assert watch._callback not in gc.callbacks  # noqa: SLF001


class _Clock:
    def __init__(self) -> None:
        self.now = 1_000.0

    def __call__(self) -> float:
        return self.now


def test_the_policy_freezes_once_a_publish_has_settled() -> None:
    clock = _Clock()
    policy = gc_watch.GcFreezePolicy(
        settle_seconds=60.0, refreeze_interval_seconds=600.0, full_collection_interval_seconds=7200.0, clock=clock
    )
    try:
        # Startup data settles like a publish.
        clock.now += 59.0
        assert policy.tick() is None
        clock.now += 1.0
        action = policy.tick()
        assert action is not None and action[0] == "frozen"
        assert gc.get_freeze_count() > 0
        assert policy.tick() is None  # nothing new to freeze

        # A publish restarts the wait; a second one before it settles restarts it again.
        policy.long_lived_data_replaced()
        clock.now += 30.0
        policy.long_lived_data_replaced()
        clock.now += 59.0
        assert policy.tick() is None
        clock.now += 1.0
        action = policy.tick()
        assert action is not None and action[0] == "frozen"
    finally:
        gc.unfreeze()


def test_the_policy_refreezes_every_interval_without_a_publish() -> None:
    clock = _Clock()
    policy = gc_watch.GcFreezePolicy(
        settle_seconds=60.0, refreeze_interval_seconds=60.0, full_collection_interval_seconds=7200.0, clock=clock
    )
    try:
        clock.now += 60.0
        action = policy.tick()
        assert action is not None and action[0] == "frozen"  # the startup settle
        # A minute of new objects is frozen too, publish or not.
        clock.now += 30.0
        assert policy.tick() is None
        clock.now += 30.0
        action = policy.tick()
        assert action is not None and action[0] == "refrozen"
        assert action[1] == 0  # nothing is counted on this path
    finally:
        gc.unfreeze()


def test_the_policy_sweeps_everything_once_per_interval_and_freezes_again() -> None:
    clock = _Clock()
    policy = gc_watch.GcFreezePolicy(
        settle_seconds=60.0, refreeze_interval_seconds=600.0, full_collection_interval_seconds=7200.0, clock=clock
    )
    try:
        clock.now += 7200.0
        action = policy.tick()
        assert action is not None and action[0] == "full_collection"
        assert gc.get_freeze_count() > 0  # frozen again straight after the sweep
        # The pending startup freeze was covered by the sweep.
        clock.now += 60.0
        assert policy.tick() is None
        clock.now += 7140.0
        action = policy.tick()
        assert action is not None and action[0] == "full_collection"
    finally:
        gc.unfreeze()


def test_freezing_moves_live_objects_out_of_full_collections() -> None:
    try:
        frozen = gc_watch.freeze_startup_objects()
        assert frozen > 0
        assert gc.get_freeze_count() == frozen
    finally:
        gc.unfreeze()
