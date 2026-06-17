from __future__ import annotations

from dataclasses import dataclass, field

from .models import OpenPosition


@dataclass
class PositionLedger:
    _positions: dict[str, OpenPosition] = field(default_factory=dict)

    def add(self, position: OpenPosition) -> None:
        self._positions[position.market.polymarket_token_id] = position

    def remove(self, token_id: str) -> None:
        self._positions.pop(token_id, None)

    def all(self) -> list[OpenPosition]:
        return list(self._positions.values())
