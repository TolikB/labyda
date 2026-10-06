from __future__ import annotations

import asyncio
import contextlib
import functools
import logging
import multiprocessing
import os
import threading
from collections.abc import Callable
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from concurrent.futures.process import BrokenProcessPool
from typing import Any, TypeVar

_T = TypeVar("_T")

LOGGER = logging.getLogger(__name__)

_DISCOVERY_EXECUTOR = ThreadPoolExecutor(max_workers=1, thread_name_prefix="discovery-cpu")

# The thread above keeps discovery off the event loop, but not off the GIL. A
# trading runtime sits at about one core in steady state -- 101-103% in
# `docker stats` through a funded window, which is the ceiling for one Python
# process -- and the catalog rebuild adds ~70 s of snapshot building and ~50 s of
# scan-all matching every ten minutes. Sharing one GIL, the event loop got
# roughly half the interpreter for those two minutes: on 2026-09-30 at 13:28 the
# observers lost /metrics and ended window-004, and on 2026-10-03 at 18:49 the
# database probes, the gas quotes and two venues' reconciliation all stalled
# together. With the bot paused and idle, the same rebuild costs the loop 17 ms
# on average, so it is the sharing that hurts, not the work. A separate process
# has its own interpreter and runs on the host's second core.
#
# Opt-in per call site (`run_discovery_process`) and per runtime
# (`configure_discovery_process_isolation`): only work that is known to be heavy
# and known to pickle goes across, and tests, operator scripts and anything else
# that never switches it on keep the thread.
_PROCESS_ISOLATION_ENABLED = False
_PROCESS_EXECUTOR: ProcessPoolExecutor | None = None
_PROCESS_EXECUTOR_LOCK = threading.Lock()
# A worker is replaced after this many tasks. Python rarely hands a peak back to
# the operating system, and a rebuild peaks at a few hundred megabytes inside a
# container whose limit is shared with the trading process.
_MAX_TASKS_PER_WORKER = 4
# Imported once into the forkserver, so a replacement worker forks with it
# already loaded instead of importing it again on the second core.
_FORKSERVER_PRELOAD = (
    "arbitrage_engine.market_discovery",
    "arbitrage_engine.myriad_discovery",
    "arbitrage_engine.predict_fun_discovery",
)
# Lower than the trading process, so the scheduler favours the event loop
# whenever the two want the same core.
_WORKER_NICE_INCREMENT = 10
# The highest the kernel allows: inside the container, the worker is always
# the process that goes first when memory runs out.
_WORKER_OOM_SCORE_ADJ = 1000


async def run_discovery_cpu(fn: Callable[..., _T], /, *args: Any, **kwargs: Any) -> _T:  # noqa: UP047
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(_DISCOVERY_EXECUTOR, lambda: fn(*args, **kwargs))


def configure_discovery_process_isolation(enabled: bool) -> None:
    """Switch the heavy discovery work between a worker process and the thread."""
    global _PROCESS_ISOLATION_ENABLED
    _PROCESS_ISOLATION_ENABLED = enabled
    if not enabled:
        shutdown_discovery_process()


def discovery_process_isolation_enabled() -> bool:
    return _PROCESS_ISOLATION_ENABLED


async def run_discovery_process(fn: Callable[..., _T], /, *args: Any, **kwargs: Any) -> _T:  # noqa: UP047
    """Run module-level CPU work in the discovery worker process, or in the thread when isolation is off.

    `fn` must be importable by name and its arguments and result must pickle.
    A worker that dies -- killed for memory, say -- costs this call its
    isolation, not its result: it is run again in the thread, and the next call
    starts a fresh worker.
    """
    if not _PROCESS_ISOLATION_ENABLED:
        return await run_discovery_cpu(fn, *args, **kwargs)
    loop = asyncio.get_running_loop()
    call = functools.partial(fn, *args, **kwargs)
    try:
        return await loop.run_in_executor(_process_executor(), call)
    except BrokenProcessPool:
        LOGGER.error("discovery_worker_lost_running_in_thread", extra={"_function": _function_name(fn)})
        _discard_process_executor()
        return await run_discovery_cpu(fn, *args, **kwargs)


def shutdown_discovery_process() -> None:
    _discard_process_executor()


def _process_executor() -> ProcessPoolExecutor:
    global _PROCESS_EXECUTOR
    with _PROCESS_EXECUTOR_LOCK:
        if _PROCESS_EXECUTOR is None:
            context = _worker_context()
            _PROCESS_EXECUTOR = ProcessPoolExecutor(
                max_workers=1,
                mp_context=context,
                initializer=_lower_worker_priority,
                max_tasks_per_child=_MAX_TASKS_PER_WORKER,
            )
            LOGGER.info(
                "discovery_worker_pool_started",
                extra={"_start_method": context.get_start_method(), "_max_tasks_per_worker": _MAX_TASKS_PER_WORKER},
            )
        return _PROCESS_EXECUTOR


def _worker_context() -> Any:
    # Never fork: the trading process has threads (the discovery thread, the
    # SDK's) and an event loop, and a forked child inherits whatever locks
    # they held. The forkserver is a clean interpreter to fork from; spawn is
    # the fallback where there is no forkserver.
    if "forkserver" in multiprocessing.get_all_start_methods():
        context = multiprocessing.get_context("forkserver")
        context.set_forkserver_preload(list(_FORKSERVER_PRELOAD))
        return context
    return multiprocessing.get_context("spawn")


def _discard_process_executor() -> None:
    global _PROCESS_EXECUTOR
    with _PROCESS_EXECUTOR_LOCK:
        executor, _PROCESS_EXECUTOR = _PROCESS_EXECUTOR, None
    if executor is not None:
        executor.shutdown(wait=False, cancel_futures=True)


def _lower_worker_priority() -> None:
    nice = getattr(os, "nice", None)  # POSIX only; the worker simply keeps its priority elsewhere
    if nice is not None:
        with contextlib.suppress(OSError):
            nice(_WORKER_NICE_INCREMENT)
    # The container protects itself with a strongly negative oom_score_adj,
    # which this process inherits -- so if the container's own limit is ever
    # reached, the kernel would pick the bigger process inside it, the trading
    # runtime. A process may always raise its own score, and at the maximum
    # the worker is killed first: that costs one rebuild, which then reruns in
    # the thread, never the runtime. Linux only; elsewhere there is no file.
    with contextlib.suppress(OSError):
        with open("/proc/self/oom_score_adj", "w", encoding="ascii") as handle:
            handle.write(str(_WORKER_OOM_SCORE_ADJ))


def _function_name(fn: Callable[..., Any]) -> str:
    return f"{getattr(fn, '__module__', '?')}.{getattr(fn, '__qualname__', repr(fn))}"
