from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

from .models import HedgeSide, MarketSpec, OpenPosition, PolymarketSide


@dataclass
class PositionLedger:
    _positions: dict[str, OpenPosition] = field(default_factory=dict)

    def add(self, position: OpenPosition) -> None:
        self._positions[position.market.polymarket_token_id] = position

    def remove(self, token_id: str) -> None:
        self._positions.pop(token_id, None)

    def all(self) -> list[OpenPosition]:
        return list(self._positions.values())


class JsonPositionLedger(PositionLedger):
    def __init__(self, path: str | Path) -> None:
        super().__init__()
        self._path = Path(path)
        self._load()

    def add(self, position: OpenPosition) -> None:
        super().add(position)
        self._save()

    def remove(self, token_id: str) -> None:
        super().remove(token_id)
        self._save()

    def _load(self) -> None:
        if not self._path.exists():
            return
        raw = json.loads(self._path.read_text(encoding="utf-8"))
        for item in raw:
            position = _position_from_json(item)
            self._positions[position.market.polymarket_token_id] = position

    def _save(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        payload = [_position_to_json(position) for position in self.all()]
        self._path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def _position_to_json(position: OpenPosition) -> dict[str, Any]:
    market = position.market
    return {
        "market": {
            "symbol": market.symbol,
            "target_label": market.target_label,
            "polymarket_token_id": market.polymarket_token_id,
            "polymarket_side": market.polymarket_side.value,
            "cefi_symbol": market.cefi_symbol,
            "cefi_hedge_side": market.cefi_hedge_side.value,
            "expires_at": market.expires_at.isoformat() if market.expires_at else None,
            "condition_id": market.condition_id,
            "tick_size": market.tick_size,
            "neg_risk": market.neg_risk,
        },
        "polymarket_contracts": position.polymarket_contracts,
        "polymarket_entry_price": position.polymarket_entry_price,
        "cefi_quantity": position.cefi_quantity,
        "cefi_entry_side": position.cefi_entry_side.value,
        "opened_at": position.opened_at.isoformat(),
        "polymarket_order_id": position.polymarket_order_id,
        "cefi_order_id": position.cefi_order_id,
    }


def _position_from_json(item: dict[str, Any]) -> OpenPosition:
    market_data = item["market"]
    expires_at = market_data.get("expires_at")
    market = MarketSpec(
        symbol=market_data["symbol"],
        target_label=market_data["target_label"],
        polymarket_token_id=market_data["polymarket_token_id"],
        polymarket_side=PolymarketSide(market_data["polymarket_side"]),
        cefi_symbol=market_data["cefi_symbol"],
        cefi_hedge_side=HedgeSide(market_data["cefi_hedge_side"]),
        expires_at=datetime.fromisoformat(expires_at) if expires_at else None,
        condition_id=market_data.get("condition_id"),
        tick_size=market_data.get("tick_size"),
        neg_risk=market_data.get("neg_risk"),
    )
    return OpenPosition(
        market=market,
        polymarket_contracts=float(item["polymarket_contracts"]),
        polymarket_entry_price=float(item["polymarket_entry_price"]),
        cefi_quantity=float(item["cefi_quantity"]),
        cefi_entry_side=HedgeSide(item["cefi_entry_side"]),
        opened_at=datetime.fromisoformat(item["opened_at"]),
        polymarket_order_id=item["polymarket_order_id"],
        cefi_order_id=item["cefi_order_id"],
    )
