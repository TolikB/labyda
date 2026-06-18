from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone
from difflib import SequenceMatcher

from .models import BinarySide

STOP_WORDS = {
    "will",
    "be",
    "the",
    "a",
    "an",
    "on",
    "in",
    "at",
    "by",
    "to",
    "or",
    "price",
    "volume",
}


@dataclass(frozen=True)
class MarketText:
    platform: str
    market_id: str
    title: str
    expires_at: datetime
    yes_label: str = "YES"
    no_label: str = "NO"


@dataclass(frozen=True)
class MatchedMarketPair:
    left: MarketText
    right: MarketText
    left_side: BinarySide
    right_side: BinarySide
    similarity: float


class SemanticMarketMatcher:
    def __init__(self, *, min_similarity: float = 0.85, expiry_window_seconds: int = 1800) -> None:
        self._min_similarity = min_similarity
        self._expiry_window_seconds = expiry_window_seconds

    def match(self, left_markets: list[MarketText], right_markets: list[MarketText]) -> list[MatchedMarketPair]:
        matches: list[MatchedMarketPair] = []
        for left in left_markets:
            best: MatchedMarketPair | None = None
            for right in right_markets:
                if not self._within_expiry_window(left.expires_at, right.expires_at):
                    continue
                similarity = text_similarity(left.title, right.title)
                if similarity < self._min_similarity:
                    continue
                pair = MatchedMarketPair(
                    left=left,
                    right=right,
                    left_side=BinarySide.YES,
                    right_side=_opposite_or_same_side(left.yes_label, right.yes_label),
                    similarity=similarity,
                )
                if best is None or pair.similarity > best.similarity:
                    best = pair
            if best is not None:
                matches.append(best)
        return matches

    def _within_expiry_window(self, left: datetime, right: datetime) -> bool:
        left_aware = _as_aware_utc(left)
        right_aware = _as_aware_utc(right)
        return abs((left_aware - right_aware).total_seconds()) <= self._expiry_window_seconds


def normalize_text(value: str) -> str:
    tokens = re.findall(r"[a-z0-9]+", value.lower().replace("$", ""))
    return " ".join(token for token in tokens if token not in STOP_WORDS)


def text_similarity(left: str, right: str) -> float:
    return SequenceMatcher(None, normalize_text(left), normalize_text(right)).ratio()


def _opposite_or_same_side(left_yes_label: str, right_yes_label: str) -> BinarySide:
    similarity = text_similarity(left_yes_label, right_yes_label)
    return BinarySide.NO if similarity >= 0.85 else BinarySide.YES


def _as_aware_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
