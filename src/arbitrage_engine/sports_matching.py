from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation

from .matcher import normalize_text

_NUMBER = r"[+-]?\d+(?:\.\d+)?"
_PERIOD_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"\b(?:map|game|set|round)\s+\d+\b", re.IGNORECASE),
    re.compile(r"\b(?:first|second|third|fourth|1st|2nd|3rd|4th)\s+(?:half|quarter|period|inning)\b", re.IGNORECASE),
)
_TYPE_PATTERN = re.compile(r"\btype\s*=\s*([^;]+)", re.IGNORECASE)
_LINE_PATTERN = re.compile(rf"\bline\s*=\s*({_NUMBER})", re.IGNORECASE)
_STANDARD_CUTOFF_WINDOW_SECONDS = 12 * 60 * 60
_OUTRIGHT_CUTOFF_WINDOW_SECONDS = 72 * 60 * 60


@dataclass(frozen=True)
class SportsMarketIdentity:
    kind: str
    participants: tuple[str, ...]
    subject: str
    line: Decimal | None
    period: str | None
    competition: str | None


def sports_market_identity(
    title: str,
    *,
    yes_label: str | None = None,
    no_label: str | None = None,
    outcome_semantics: str | None = None,
) -> SportsMarketIdentity | None:
    raw_title = title.strip().rstrip("?.!")
    semantics = outcome_semantics or ""
    period = _period_identity(f"{raw_title} {semantics}")

    total = re.match(
        rf"^will\s+(.+?)\s+(?:vs\.?|versus)\s+(.+?)\s+total\s+(?:go\s+)?(?:over|above)\s+({_NUMBER})$",
        raw_title,
        re.IGNORECASE,
    )
    if total:
        return _identity("total", (total.group(1), total.group(2)), total.group(1), total.group(3), period)

    spread = re.match(
        rf"^will\s+(.+?)\s+cover\s+({_NUMBER})\s+(?:vs\.?|versus)\s+(.+?)$",
        raw_title,
        re.IGNORECASE,
    )
    if spread:
        participants = (spread.group(1), spread.group(3))
        subject = _named_participant(yes_label, participants) or spread.group(1)
        line = _decimal(spread.group(2))
        if line is not None and _participant_identity(subject) == _participant_identity(spread.group(3)):
            line = -line
        return _identity("spread", participants, subject, line, period)

    head_to_head = re.match(
        r"^will\s+(.+?)\s+(?:beat|defeat|win\s+(?:against|vs\.?))\s+(.+?)$",
        raw_title,
        re.IGNORECASE,
    )
    if head_to_head:
        participants = (head_to_head.group(1), head_to_head.group(2))
        subject = _named_participant(yes_label, participants) or head_to_head.group(1)
        return _identity("moneyline", participants, subject, None, period)

    outright = re.match(r"^will\s+(.+?)\s+win\s+(?:the\s+)?(.+?)$", raw_title, re.IGNORECASE)
    if outright:
        outright_competition = _competition_identity(outright.group(2))
        if not outright_competition:
            return None
        return _identity(
            "outright",
            (outright.group(1),),
            outright.group(1),
            None,
            period,
            competition=outright_competition,
        )

    matchup = _matchup_from_structured_title(raw_title, yes_label=yes_label, no_label=no_label)
    if matchup is None:
        return None
    left, right = matchup
    kind = _kind_from_metadata(
        yes_label,
        no_label,
        semantics,
        participants=(left, right),
        title=raw_title,
    )
    if kind is None:
        return None
    subject = _named_participant(yes_label, (left, right)) or _strip_line(yes_label or left)
    line = _line_from_metadata(kind, yes_label, no_label, semantics, title=raw_title, subject=subject)
    if kind in {"spread", "total"} and line is None:
        return None
    structured_competition = _competition_from_structured_title(raw_title)
    return _identity(kind, (left, right), subject, line, period, competition=structured_competition)


def structured_sports_match(
    left: SportsMarketIdentity | None,
    right: SportsMarketIdentity | None,
    *,
    left_cutoff: datetime | None,
    right_cutoff: datetime | None,
    cutoff_window_seconds: int | None = None,
) -> bool:
    if left is None or right is None:
        return False
    if left.kind != right.kind or left.participants != right.participants:
        return False
    if left.subject != right.subject or left.line != right.line or left.period != right.period:
        return False
    if left.kind == "outright" and left.competition != right.competition:
        return False
    if left.competition and right.competition and left.competition != right.competition:
        return False
    if left_cutoff is None or right_cutoff is None:
        return False
    if cutoff_window_seconds is None:
        cutoff_window_seconds = (
            _OUTRIGHT_CUTOFF_WINDOW_SECONDS
            if left.kind == "outright"
            else _STANDARD_CUTOFF_WINDOW_SECONDS
        )
    return abs((_as_utc(left_cutoff) - _as_utc(right_cutoff)).total_seconds()) <= cutoff_window_seconds


def _identity(
    kind: str,
    participants: tuple[str, ...],
    subject: str,
    line: str | Decimal | None,
    period: str | None,
    *,
    competition: str | None = None,
) -> SportsMarketIdentity | None:
    normalized_participants = tuple(sorted({_participant_identity(value) for value in participants if value.strip()}))
    normalized_subject = _participant_identity(subject)
    if not normalized_participants or not normalized_subject or normalized_subject not in normalized_participants:
        return None
    return SportsMarketIdentity(
        kind=kind,
        participants=normalized_participants,
        subject=normalized_subject,
        line=_normalized_line(kind, line),
        period=period,
        competition=competition,
    )


def _matchup_from_structured_title(
    title: str,
    *,
    yes_label: str | None,
    no_label: str | None,
) -> tuple[str, str] | None:
    named = _named_matchup_from_outcomes(title, yes_label, no_label)
    if named is not None:
        return named
    for part in (item.strip() for item in title.split("|")):
        match = re.fullmatch(r"(.+?)\s+(?:vs\.?|versus)\s+(.+)", part, re.IGNORECASE)
        if match:
            return match.group(1), match.group(2)
    match = re.search(
        r"(?:^|[:|]\s*)(.+?)\s+(?:vs\.?|versus)\s+(.+?)(?:\s+\(bo\d+\)|\s+-\s+|$)",
        title,
        re.IGNORECASE,
    )
    if match:
        return _strip_participant_annotation(match.group(1)), _strip_participant_annotation(match.group(2))
    return None


def _kind_from_metadata(
    yes_label: str | None,
    no_label: str | None,
    semantics: str,
    *,
    participants: tuple[str, str] | None = None,
    title: str = "",
) -> str | None:
    labels = f"{yes_label or ''} {no_label or ''}".casefold()
    normalized_title = normalize_text(title)
    type_match = _TYPE_PATTERN.search(semantics)
    market_type = normalize_text(type_match.group(1)) if type_match else ""
    if "over" in labels and "under" in labels:
        return "total"
    if re.search(r"[+-]\d", labels) or "handicap" in normalized_title:
        return "spread"
    if "total" in normalized_title and ("above" in labels or "below" in labels):
        return "total"
    if any(value in market_type for value in ("moneyline", "match winner", "winner")):
        return "moneyline"
    if any(value in market_type for value in ("spread", "handicap")):
        return "spread"
    if any(value in market_type for value in ("total", "over under")):
        return "total"
    if participants is not None and yes_label and no_label:
        normalized_participants = {_participant_identity(value) for value in participants}
        normalized_outcomes = {_participant_identity(yes_label), _participant_identity(no_label)}
        if (
            len(normalized_participants) == 2
            and normalized_outcomes == normalized_participants
            and ("winner" in normalized_title or _named_matchup_from_outcomes(title, yes_label, no_label))
        ):
            return "moneyline"
    return None


def _line_from_metadata(
    kind: str,
    yes_label: str | None,
    no_label: str | None,
    semantics: str,
    *,
    title: str = "",
    subject: str | None = None,
) -> Decimal | None:
    if kind == "spread" and yes_label:
        label_match = re.search(rf"({_NUMBER})\s*$", yes_label)
        if label_match:
            return _decimal(label_match.group(1))
    line_match = _LINE_PATTERN.search(semantics)
    if line_match:
        return _decimal(line_match.group(1))
    if kind == "spread":
        title_line = _spread_line_from_title(title, subject)
        if title_line is not None:
            return title_line
    for label in (yes_label, no_label):
        if not label:
            continue
        label_match = re.search(rf"({_NUMBER})\s*$", label)
        if label_match:
            return _decimal(label_match.group(1))
    return None


def _strip_line(value: str) -> str:
    return re.sub(rf"\s+{_NUMBER}\s*$", "", value.strip())


def _strip_participant_annotation(value: str) -> str:
    stripped = re.sub(rf"\s*\(\s*{_NUMBER}\s*\)\s*$", "", value.strip())
    return re.sub(r"\s+\(bo\d+\)\s*$", "", stripped, flags=re.IGNORECASE)


def _named_matchup_from_outcomes(
    title: str,
    yes_label: str | None,
    no_label: str | None,
) -> tuple[str, str] | None:
    if not yes_label or not no_label:
        return None
    participants = (
        _strip_participant_annotation(_strip_line(yes_label)),
        _strip_participant_annotation(_strip_line(no_label)),
    )
    identities = tuple(_participant_identity(value) for value in participants)
    if any(_is_generic_outcome(value) for value in identities) or len(set(identities)) != 2:
        return None
    title_tokens = set(normalize_text(title).split())
    if not all(set(identity.split()).issubset(title_tokens) for identity in identities):
        return None
    return participants


def _named_participant(label: str | None, participants: tuple[str, ...]) -> str | None:
    if not label:
        return None
    candidate = _participant_identity(label)
    if _is_generic_outcome(candidate):
        return None
    for participant in participants:
        if candidate == _participant_identity(participant):
            return _strip_participant_annotation(_strip_line(label))
    return None


def _is_generic_outcome(value: str) -> bool:
    return value in {
        "yes",
        "no",
        "draw",
        "tie",
        "field",
        "other",
        "outcome one",
        "outcome two",
    } or value.startswith(("not ", "above ", "below "))


def _spread_line_from_title(title: str, subject: str | None) -> Decimal | None:
    if not title or not subject:
        return None
    match = re.search(
        rf"(.+?)\s*\(\s*({_NUMBER})\s*\)\s+(?:vs\.?|versus)\s+(.+?)\s*\(\s*({_NUMBER})\s*\)",
        title,
        re.IGNORECASE,
    )
    if match is None:
        return None
    subject_identity = _participant_identity(subject)
    left = _strip_participant_annotation(match.group(1).rsplit(":", 1)[-1])
    right = _strip_participant_annotation(match.group(3))
    if subject_identity == _participant_identity(left):
        return _decimal(match.group(2))
    if subject_identity == _participant_identity(right):
        return _decimal(match.group(4))
    return None


def _participant_identity(value: str) -> str:
    without_period = re.sub(
        r"\s+(?:on|in)?\s*(?:map|game|set|round)\s+\d+\s*$",
        "",
        _strip_line(value),
        flags=re.IGNORECASE,
    )
    normalized = normalize_text(without_period)
    return re.sub(r"\s+(?:esports|gaming)$", "", normalized)


def _competition_identity(value: str) -> str:
    normalized = normalize_text(value)
    normalized = re.sub(r"\b20\d{2}\b", "", normalized)
    normalized = re.sub(r"\bfifa\s+world\s+cup\b", "world cup", normalized)
    return " ".join(normalized.split())


def _competition_from_structured_title(title: str) -> str | None:
    parts = [part.strip() for part in title.split("|") if part.strip()]
    if len(parts) < 2:
        return None
    competition = parts[0].removeprefix("Outrights - ").strip()
    return _competition_identity(competition) or None


def _period_identity(value: str) -> str | None:
    matches = [normalize_text(match.group(0)) for pattern in _PERIOD_PATTERNS for match in pattern.finditer(value)]
    unique = tuple(dict.fromkeys(matches))
    if len(unique) > 1:
        return "ambiguous"
    return unique[0] if unique else None


def _decimal(value: str | Decimal | None) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value)).normalize()
    except InvalidOperation:
        return None


def _normalized_line(kind: str, value: str | Decimal | None) -> Decimal | None:
    if kind not in {"spread", "total"}:
        return None
    parsed = _decimal(value)
    if parsed is None:
        return None
    return parsed if kind == "spread" else parsed.copy_abs()


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)
