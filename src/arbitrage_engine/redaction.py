from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

_SIGNING_FIELDS = frozenset(
    {
        "ordersignature",
        "signature",
        "signatureprefix",
        "takersig",
    }
)


def redact_signing_material(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Remove replayable signature material from an operator-facing payload."""
    redacted, removed_count = _redact_value(payload)
    assert isinstance(redacted, dict)
    if removed_count:
        redacted["signature_present"] = True
    return redacted


def _redact_value(value: Any) -> tuple[Any, int]:
    if isinstance(value, Mapping):
        redacted: dict[str, Any] = {}
        removed_count = 0
        for raw_key, item in value.items():
            key = str(raw_key)
            normalized_key = "".join(character for character in key.lower() if character.isalnum())
            if normalized_key in _SIGNING_FIELDS:
                removed_count += int(bool(item))
                continue
            redacted_item, nested_removed = _redact_value(item)
            redacted[key] = redacted_item
            removed_count += nested_removed
        return redacted, removed_count
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        redacted_items: list[Any] = []
        removed_count = 0
        for item in value:
            redacted_item, nested_removed = _redact_value(item)
            redacted_items.append(redacted_item)
            removed_count += nested_removed
        return redacted_items, removed_count
    return value, 0
