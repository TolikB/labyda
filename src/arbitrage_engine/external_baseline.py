from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from decimal import Decimal


def account_fingerprint(venue: str, account_identifier: str) -> str:
    normalized_venue = venue.strip()
    normalized_account = account_identifier.strip().lower()
    if not normalized_venue or not normalized_account:
        raise ValueError("venue and account identifier are required")
    payload = f"external-account-baseline:v1\0{normalized_venue}\0{normalized_account}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def canonical_external_baseline_payload(
    *,
    runtime_instance_id: str,
    venue: str,
    account_fingerprint_value: str,
    positions: Mapping[str, Decimal] | Sequence[tuple[str, Decimal]],
    fill_refs: Sequence[str],
) -> dict[str, object]:
    runtime = runtime_instance_id.strip()
    normalized_venue = venue.strip()
    fingerprint = account_fingerprint_value.strip().lower()
    if not runtime or not normalized_venue:
        raise ValueError("runtime instance and venue are required")
    if len(fingerprint) != 64 or any(character not in "0123456789abcdef" for character in fingerprint):
        raise ValueError("account fingerprint must be a SHA-256 digest")

    position_items = positions.items() if isinstance(positions, Mapping) else positions
    normalized_positions: list[dict[str, str]] = []
    seen_tokens: set[str] = set()
    for raw_token_id, raw_quantity in position_items:
        token_id = str(raw_token_id).strip()
        quantity = Decimal(str(raw_quantity))
        if not token_id or len(token_id) > 256:
            raise ValueError("external position token id is invalid")
        if token_id in seen_tokens:
            raise ValueError(f"duplicate external position token id: {token_id}")
        if not quantity.is_finite() or quantity <= 0:
            raise ValueError(f"external position quantity must be positive for token {token_id}")
        seen_tokens.add(token_id)
        normalized_positions.append(
            {"token_id": token_id, "quantity": _canonical_decimal(quantity)}
        )

    normalized_fill_refs: list[str] = []
    seen_fill_refs: set[str] = set()
    for raw_fill_ref in fill_refs:
        fill_ref = str(raw_fill_ref).strip()
        if not fill_ref or len(fill_ref) > 256:
            raise ValueError("external fill reference is invalid")
        if fill_ref in seen_fill_refs:
            raise ValueError(f"duplicate external fill reference: {fill_ref}")
        seen_fill_refs.add(fill_ref)
        normalized_fill_refs.append(fill_ref)

    normalized_positions.sort(key=lambda item: item["token_id"])
    normalized_fill_refs.sort()
    return {
        "schema_version": 1,
        "runtime_instance_id": runtime,
        "venue": normalized_venue,
        "account_fingerprint": fingerprint,
        "positions": normalized_positions,
        "fill_refs": normalized_fill_refs,
    }


def external_baseline_manifest_sha256(payload: Mapping[str, object]) -> str:
    canonical = json.dumps(
        dict(payload),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _canonical_decimal(value: Decimal) -> str:
    normalized = value.normalize()
    rendered = format(normalized, "f")
    return rendered.rstrip("0").rstrip(".") if "." in rendered else rendered
