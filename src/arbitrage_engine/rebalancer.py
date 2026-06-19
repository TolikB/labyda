from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class RebalanceRecommendation:
    source_venue: str
    destination_venue: str
    amount_usd: float


class BalanceRebalancer:
    """Disabled-by-default planning stub; it never submits bridge transactions."""

    def __init__(self, *, enabled: bool, ratio_threshold: float = 0.80) -> None:
        self.enabled = enabled
        self.ratio_threshold = ratio_threshold

    def recommend(self, balances: dict[str, float]) -> RebalanceRecommendation | None:
        if not self.enabled or len(balances) < 2:
            return None
        richest, richest_balance = max(balances.items(), key=lambda item: item[1])
        poorest, poorest_balance = min(balances.items(), key=lambda item: item[1])
        total = richest_balance + poorest_balance
        if total <= 0 or richest_balance / total < self.ratio_threshold:
            return None
        return RebalanceRecommendation(richest, poorest, (richest_balance - poorest_balance) / 2.0)

    async def execute(self, recommendation: RebalanceRecommendation) -> None:
        del recommendation
        raise RuntimeError("Automatic bridge execution is intentionally not implemented")
