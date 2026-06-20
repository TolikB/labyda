from __future__ import annotations

import secrets
import time
import uuid


def uuid7() -> uuid.UUID:
    """Generate an RFC 9562 UUIDv7 without requiring Python 3.14."""
    unix_ms = time.time_ns() // 1_000_000
    random_bits = secrets.randbits(74)
    value = (unix_ms & ((1 << 48) - 1)) << 80
    value |= 0x7 << 76
    value |= ((random_bits >> 62) & 0xFFF) << 64
    value |= 0b10 << 62
    value |= random_bits & ((1 << 62) - 1)
    return uuid.UUID(int=value)
