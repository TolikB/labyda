from __future__ import annotations

import asyncio
from typing import Any

import pytest

from arbitrage_engine.connectors.web3_base import read_with_rpc_rotation


class _Client:
    def __init__(self, urls: list[str]) -> None:
        self.rpc_urls = urls
        self.index = 0

    def _rotate_rpc(self) -> None:
        self.index = (self.index + 1) % len(self.rpc_urls)


@pytest.mark.asyncio
async def test_a_hung_read_moves_on_to_the_next_rpc() -> None:
    # 2026-10-10: a bare balance read on the first BNB endpoint hung for good
    # while the others answered at once.
    client = _Client(["hung", "healthy"])
    seen: list[str] = []

    async def read() -> Any:
        url = client.rpc_urls[client.index]
        seen.append(url)
        if url == "hung":
            await asyncio.sleep(60)
        return 42

    assert await read_with_rpc_rotation(client, read, timeout_seconds=0.05) == 42
    assert seen == ["hung", "healthy"]


@pytest.mark.asyncio
async def test_every_rpc_failing_raises_the_last_error() -> None:
    client = _Client(["a", "b"])
    calls = 0

    async def read() -> Any:
        nonlocal calls
        calls += 1
        raise ConnectionError(client.rpc_urls[client.index])

    with pytest.raises(ConnectionError, match="b"):
        await read_with_rpc_rotation(client, read, timeout_seconds=0.05)
    assert calls == 2


@pytest.mark.asyncio
async def test_a_client_without_rpc_urls_gets_one_bounded_attempt() -> None:
    async def read() -> Any:
        await asyncio.sleep(60)

    with pytest.raises(TimeoutError):
        await read_with_rpc_rotation(object(), read, timeout_seconds=0.05)
