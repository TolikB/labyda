from __future__ import annotations

import asyncio
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from typing import Any, TypeVar

_T = TypeVar("_T")

_DISCOVERY_EXECUTOR = ThreadPoolExecutor(max_workers=1, thread_name_prefix="discovery-cpu")


async def run_discovery_cpu(fn: Callable[..., _T], /, *args: Any, **kwargs: Any) -> _T:  # noqa: UP047
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(_DISCOVERY_EXECUTOR, lambda: fn(*args, **kwargs))
