from __future__ import annotations

from typing import Any


def client_session(headers: dict[str, str] | None = None) -> Any:
    import aiohttp

    connector = aiohttp.TCPConnector(resolver=aiohttp.ThreadedResolver())
    return aiohttp.ClientSession(headers=headers, connector=connector)
