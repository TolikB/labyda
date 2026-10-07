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


def test_freezing_moves_live_objects_out_of_full_collections() -> None:
    try:
        frozen = gc_watch.freeze_startup_objects()
        assert frozen > 0
        assert gc.get_freeze_count() == frozen
    finally:
        gc.unfreeze()
