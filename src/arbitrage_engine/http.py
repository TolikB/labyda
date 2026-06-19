from __future__ import annotations

from typing import Any


def client_session(headers: dict[str, str] | None = None) -> Any:
    import aiohttp

    connector = aiohttp.TCPConnector(
        limit=100,
        use_dns_cache=True,
        ttl_dns_cache=300,
        enable_cleanup_closed=True,
        resolver=aiohttp.ThreadedResolver(),
    )
    return aiohttp.ClientSession(headers=headers, connector=connector)
