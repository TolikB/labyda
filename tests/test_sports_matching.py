from __future__ import annotations

from datetime import UTC, datetime, timedelta

from arbitrage_engine.sports_matching import (
    sports_market_identity,
    structured_sports_match,
)

CUTOFF = datetime(2026, 7, 16, 18, tzinfo=UTC)


def _matches(left: str, right: str, *, right_offset: timedelta = timedelta()) -> bool:
    return structured_sports_match(
        sports_market_identity(left),
        sports_market_identity(right),
        left_cutoff=CUTOFF,
        right_cutoff=CUTOFF + right_offset,
    )


def test_structured_sports_match_accepts_canonical_outright_aliases() -> None:
    assert _matches(
        "Will Turkiye win the World Cup?",
        "Will Turkey win the 2026 FIFA World Cup?",
    )


def test_structured_sports_match_requires_participants_and_market_kind() -> None:
    assert _matches("Will Arsenal beat Chelsea?", "Will Arsenal beat Liverpool?") is False
    assert _matches("Will Arsenal beat Chelsea?", "Will Arsenal win the Premier League?") is False


def test_structured_sports_match_requires_line_period_and_cutoff() -> None:
    assert _matches(
        "Will Team Liquid cover -1.5 vs Dragon Ranger in map 1?",
        "Will Team Liquid cover +1.5 vs Dragon Ranger in map 1?",
    ) is False
    assert _matches(
        "Will Team Liquid beat Dragon Ranger in map 1?",
        "Will Team Liquid beat Dragon Ranger in map 2?",
    ) is False
    assert _matches(
        "Will Team Liquid beat Dragon Ranger?",
        "Will Team Liquid beat Dragon Ranger?",
        right_offset=timedelta(hours=12, seconds=1),
    ) is False


def test_structured_sports_identity_rejects_untyped_matchup() -> None:
    assert sports_market_identity("Premier League | Arsenal vs Chelsea") is None


def test_structured_sports_identity_accepts_named_moneyline_outcomes() -> None:
    identity = sports_market_identity(
        "Premier League | Arsenal vs Chelsea",
        yes_label="Chelsea",
        no_label="Arsenal",
    )

    assert identity is not None
    assert identity.kind == "moneyline"
    assert identity.participants == ("arsenal", "chelsea")


def test_structured_sports_identity_rejects_generic_or_non_participant_outcomes() -> None:
    assert (
        sports_market_identity(
            "Premier League | Arsenal vs Chelsea",
            yes_label="YES",
            no_label="NO",
        )
        is None
    )
    assert (
        sports_market_identity(
            "Premier League | Arsenal vs Chelsea",
            yes_label="Arsenal",
            no_label="Draw",
        )
        is None
    )
