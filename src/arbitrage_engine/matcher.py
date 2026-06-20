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
    "to",
    "or",
    "price",
    "volume",
}

_ALIASES: tuple[tuple[re.Pattern[str], str], ...] = tuple(
    (re.compile(pattern, re.IGNORECASE), replacement)
    for pattern, replacement in (
        (r"\bbinance\s+coin\b", "bnb"),
        (r"\busd\s+coin\b", "usdc"),
        (r"\bgreater\s+than\b", "above"),
        (r"\bless\s+than\b", "below"),
        (r"\bbitcoin\b", "btc"),
        (r"\bxbt\b", "btc"),
        (r"\bethereum\b", "eth"),
        (r"\bether\b", "eth"),
        (r"\bsolana\b", "sol"),
        (r"\bdogecoin\b", "doge"),
        (r"\btether\b", "usdt"),
        (r"\bturkey\b", "turkiye"),
        (r"\btürkiye\b", "turkiye"),
        (r"\bversus\b", "vs"),
        (r"\bover\b", "above"),
        (r"\bunder\b", "below"),
    )
)

_MONTH = (
    r"(?:jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|"
    r"jul(?:y)?|aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)"
)
_DATE = rf"(?:{_MONTH}\s+\d{{1,2}}(?:st|nd|rd|th)?(?:,?\s+\d{{4}})?|\d{{4}}-\d{{1,2}}-\d{{1,2}}|\d{{1,2}}[/-]\d{{1,2}}[/-]\d{{2,4}})"
_TIME = r"(?:\d{1,2}(?::\d{2})?(?:\s*[ap]\.?m\.?)?)"
_TIMEZONE = r"(?:utc|gmt|et|est|edt|ct|cst|cdt|mt|mst|mdt|pt|pst|pdt)"
_DATE_TIME_NOISE = rf"{_DATE}(?:[\s,]+(?:at\s+)?{_TIME}(?:\s*{_TIMEZONE})?)?"
_TIME_NOISE = rf"{_TIME}\s*{_TIMEZONE}"
_NOISE_LABEL = r"(?:closes?|closing|expires?|expiry|ends?|ending|settles?|settlement|cutoff)"
_PLATFORM_NOISE_SUFFIXES: tuple[re.Pattern[str], ...] = (
    re.compile(
        rf"\s*[\[(]\s*(?:{_NOISE_LABEL}\s*:?\s*)?(?:{_DATE_TIME_NOISE}|{_TIME_NOISE})\s*[\])]\s*$",
        re.IGNORECASE,
    ),
    re.compile(
        rf"\s*(?:[-–—|•]\s*|{_NOISE_LABEL}\s*:?\s*)(?:{_DATE_TIME_NOISE}|{_TIME_NOISE})\s*$",
        re.IGNORECASE,
    ),
)


@dataclass(frozen=True)
class MarketText:
    platform: str
    market_id: str
    title: str
    expires_at: datetime
    yes_label: str = "YES"
    no_label: str = "NO"
    external_market_id: str | None = None
    volume_usd: float | None = None
    public_url: str | None = None
    category: str | None = None
    resolution_source: str | None = None
    outcome_semantics: str | None = None


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
    normalized = value.casefold().replace("$", "")
    for pattern in _PLATFORM_NOISE_SUFFIXES:
        normalized = pattern.sub("", normalized)
    normalized = re.sub(r"(?<=\d),(?=\d)", "", normalized)
    for pattern, replacement in _ALIASES:
        normalized = pattern.sub(replacement, normalized)
    tokens = re.findall(r"[a-z0-9]+", normalized)
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
