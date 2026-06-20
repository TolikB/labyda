from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

from .models import AmmPool, BinarySide, MappingStatus, MarketSpec, OpenPosition, position_key


@dataclass
class PositionLedger:
    _positions: dict[str, OpenPosition] = field(default_factory=dict)

    def add(self, position: OpenPosition) -> None:
        self._positions[position_key(position.market)] = position

    def remove(self, token_id: str) -> None:
        self._positions.pop(token_id, None)

    def has(self, token_id: str) -> bool:
        return token_id in self._positions

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
            self._positions[position_key(position.market)] = position

    def _save(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        payload = [_position_to_json(position) for position in self.all()]
        temporary_path = self._path.with_name(f"{self._path.name}.tmp")
        with temporary_path.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, ensure_ascii=False)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_path, self._path)


def _amm_to_json(pool: AmmPool | None) -> dict[str, float] | None:
    if pool is None:
        return None
    return {"yes_reserve": pool.yes_reserve, "no_reserve": pool.no_reserve, "fee_pct": pool.fee_pct}


def _amm_from_json(raw: Any) -> AmmPool | None:
    if not isinstance(raw, dict):
        return None
    return AmmPool(float(raw["yes_reserve"]), float(raw["no_reserve"]), float(raw.get("fee_pct", 0.0)))


def _position_to_json(position: OpenPosition) -> dict[str, Any]:
    market = position.market
    return {
        "market": {
            "symbol": market.symbol,
            "target_label": market.target_label,
            "polymarket_token_id": market.polymarket_token_id,
            "polymarket_side": market.polymarket_side.value,
            "predict_fun_token_id": market.predict_fun_token_id,
            "predict_fun_side": market.predict_fun_side.value,
            "venue_a_label": market.venue_a_label,
            "venue_b_label": market.venue_b_label,
            "expires_at": market.expires_at.isoformat() if market.expires_at else None,
            "condition_id": market.condition_id,
            "polymarket_market_id": market.polymarket_market_id,
            "polymarket_url": market.polymarket_url,
            "tick_size": market.tick_size,
            "neg_risk": market.neg_risk,
            "predict_fun_neg_risk": market.predict_fun_neg_risk,
            "predict_fun_fee_rate_bps": market.predict_fun_fee_rate_bps,
            "predict_fun_market_id": market.predict_fun_market_id,
            "predict_fun_url": market.predict_fun_url,
            "predict_fun_amm_pool": _amm_to_json(market.predict_fun_amm_pool),
            "myriad_market_id": market.myriad_market_id,
            "myriad_url": market.myriad_url,
            "myriad_side": market.myriad_side.value,
            "rules_fingerprint": market.rules_fingerprint,
            "category": market.category,
            "mapping_status": market.mapping_status.value,
            "resolution_source": market.resolution_source,
            "outcome_semantics": market.outcome_semantics,
            "cutoff_at": market.cutoff_at.isoformat() if market.cutoff_at else None,
            "timezone_name": market.timezone_name,
            "verified_routes": sorted(market.verified_routes),
        },
        "polymarket_contracts": position.polymarket_contracts,
        "polymarket_entry_price": position.polymarket_entry_price,
        "predict_fun_contracts": position.predict_fun_contracts,
        "predict_fun_entry_price": position.predict_fun_entry_price,
        "opened_at": position.opened_at.isoformat(),
        "polymarket_order_id": position.polymarket_order_id,
        "predict_fun_order_id": position.predict_fun_order_id,
        "status": position.status,
        "polymarket_unwind_attempts": position.polymarket_unwind_attempts,
        "polymarket_closed": position.polymarket_closed,
        "predict_fun_closed": position.predict_fun_closed,
        "polymarket_exit_price": position.polymarket_exit_price,
        "predict_fun_exit_price": position.predict_fun_exit_price,
        "unmatched_first_contracts": position.unmatched_first_contracts,
        "unmatched_second_contracts": position.unmatched_second_contracts,
        "polymarket_closed_contracts": position.polymarket_closed_contracts,
        "predict_fun_closed_contracts": position.predict_fun_closed_contracts,
        "polymarket_exit_proceeds_usd": position.polymarket_exit_proceeds_usd,
        "predict_fun_exit_proceeds_usd": position.predict_fun_exit_proceeds_usd,
    }


def _position_from_json(item: dict[str, Any]) -> OpenPosition:
    market_data = item["market"]
    expires_at = market_data.get("expires_at")
    market = MarketSpec(
        symbol=market_data["symbol"],
        target_label=market_data["target_label"],
        polymarket_token_id=market_data["polymarket_token_id"],
        polymarket_side=BinarySide(market_data["polymarket_side"]),
        predict_fun_token_id=market_data["predict_fun_token_id"],
        predict_fun_side=BinarySide(market_data["predict_fun_side"]),
        venue_a_label=str(market_data.get("venue_a_label", "Polymarket")),
        venue_b_label=str(market_data.get("venue_b_label", "Predict.fun")),
        expires_at=datetime.fromisoformat(expires_at) if expires_at else None,
        condition_id=market_data.get("condition_id"),
        polymarket_market_id=market_data.get("polymarket_market_id"),
        polymarket_url=market_data.get("polymarket_url"),
        tick_size=market_data.get("tick_size"),
        neg_risk=market_data.get("neg_risk"),
        predict_fun_neg_risk=market_data.get("predict_fun_neg_risk"),
        predict_fun_fee_rate_bps=(
            int(market_data["predict_fun_fee_rate_bps"])
            if market_data.get("predict_fun_fee_rate_bps") is not None
            else None
        ),
        predict_fun_market_id=market_data.get("predict_fun_market_id"),
        predict_fun_url=market_data.get("predict_fun_url"),
        predict_fun_amm_pool=_amm_from_json(market_data.get("predict_fun_amm_pool")),
        myriad_market_id=market_data.get("myriad_market_id"),
        myriad_url=market_data.get("myriad_url"),
        myriad_side=BinarySide(str(market_data.get("myriad_side") or "NO")),
        rules_fingerprint=market_data.get("rules_fingerprint"),
        category=market_data.get("category"),
        mapping_status=MappingStatus(str(market_data.get("mapping_status") or "CANDIDATE")),
        resolution_source=market_data.get("resolution_source"),
        outcome_semantics=market_data.get("outcome_semantics"),
        cutoff_at=(
            datetime.fromisoformat(market_data["cutoff_at"])
            if market_data.get("cutoff_at")
            else None
        ),
        timezone_name=str(market_data.get("timezone_name") or "UTC"),
        verified_routes=frozenset(str(value) for value in market_data.get("verified_routes", [])),
    )
    return OpenPosition(
        market=market,
        polymarket_contracts=float(item["polymarket_contracts"]),
        polymarket_entry_price=float(item["polymarket_entry_price"]),
        predict_fun_contracts=float(item["predict_fun_contracts"]),
        predict_fun_entry_price=float(item["predict_fun_entry_price"]),
        opened_at=datetime.fromisoformat(item["opened_at"]),
        polymarket_order_id=item["polymarket_order_id"],
        predict_fun_order_id=item["predict_fun_order_id"],
        status=str(item.get("status", "open")),
        polymarket_unwind_attempts=int(item.get("polymarket_unwind_attempts", 0)),
        polymarket_closed=bool(item.get("polymarket_closed", False)),
        predict_fun_closed=bool(item.get("predict_fun_closed", False)),
        polymarket_exit_price=(
            float(item["polymarket_exit_price"]) if item.get("polymarket_exit_price") is not None else None
        ),
        predict_fun_exit_price=(
            float(item["predict_fun_exit_price"]) if item.get("predict_fun_exit_price") is not None else None
        ),
        unmatched_first_contracts=float(item.get("unmatched_first_contracts", 0.0)),
        unmatched_second_contracts=float(item.get("unmatched_second_contracts", 0.0)),
        polymarket_closed_contracts=float(item.get("polymarket_closed_contracts", 0.0)),
        predict_fun_closed_contracts=float(item.get("predict_fun_closed_contracts", 0.0)),
        polymarket_exit_proceeds_usd=float(item.get("polymarket_exit_proceeds_usd", 0.0)),
        predict_fun_exit_proceeds_usd=float(item.get("predict_fun_exit_proceeds_usd", 0.0)),
    )
