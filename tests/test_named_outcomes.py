"""Which Polymarket outcome a Predict.fun label names -- every case here is a real label from 2026-10-05.

A wrong answer is not a worse trade but an unhedged bet, so the cases that must
be refused matter as much as the ones that must be read.
"""

from __future__ import annotations

import pytest

from arbitrage_engine.named_outcomes import named_outcome_index, same_rules_text


@pytest.mark.parametrize(
    ("label", "outcomes", "title", "expected"),
    [
        # A prefix of a word.
        ("HOU", ["Houston Dynamo", "Minnesota United FC"], "Spread: Houston Dynamo (-5.5)", 0),
        ("ENG", ["Czechia", "England"], "Spread: Czechia (-5.5)", 1),
        ("BELA", ["Finland", "Belarus"], "Spread: Finland (-4.5)", 1),
        ("SWIATEK", ["Iva Jovic", "Iga Swiatek"], "China Open: Iva Jovic vs Iga Swiatek", 1),
        # A whole word, a full name.
        ("LG", ["Doosan Bears", "LG Twins"], "KBO: Doosan Bears vs. LG Twins", 1),
        ("Natus Vincere", ["HANJIN BRION", "Natus Vincere"], "LoL: HANJIN BRION vs Natus Vincere", 1),
        # Initials, and the starts of consecutive words.
        ("SJE", ["Colorado Rapids SC", "San Jose Earthquakes"], "Spread: Colorado Rapids SC (-5.5)", 1),
        ("AJA", ["Stade Rennais FC 1901", "AJ Auxerre"], "Spread: Stade Rennais FC 1901 (-1.5)", 1),
        ("ARKST", ["South Alabama", "Arkansas State"], "South Alabama vs. Arkansas State", 1),
        ("NAVI", ["HANJIN BRION", "Natus Vincere"], "First Blood in Game 1?", 1),
        # Digits that are part of the name, and digits Predict.fun appended.
        ("B04", ["1. FSV Mainz 05", "Bayer 04 Leverkusen"], "Spread: 1. FSV Mainz 05 (-1.5)", 1),
        ("AUR1", ["Aurora", "1win"], "Dota 2: Aurora vs 1win - Game 1 Winner", 0),
        ("100T1", ["100 Thieves", "G2 Esports"], "Valorant: 100 Thieves vs G2 Esports - Map 1 Winner", 0),
        # Known codes: a city for a nickname, a country's ISO or FIFA code.
        ("PHI", ["Knicks", "76ers"], "Knicks vs. 76ers", 1),
        ("TEN", ["Texans", "Titans"], "Texans vs. Titans", 1),
        ("LAS", ["Maple Leafs", "Golden Knights"], "Maple Leafs vs. Golden Knights", 1),
        ("CHE", ["Switzerland", "North Macedonia"], "Spread: Switzerland (-2.5)", 0),
        ("CHN", ["China PR", "Tajikistan"], "Spread: China PR (-1.5)", 0),
        # Over/under, with the line both titles carry.
        ("Under 3.5", ["Over", "Under"], "Al Kholood Saudi Club vs. Al Qadisiyah Saudi Club: O/U 3.5", 1),
        ("Over 21.5", ["Over", "Under"], "Map 1 Total Rounds: Over/Under 21.5", 0),
    ],
)
def test_a_label_names_the_outcome_it_stands_for(label: str, outcomes: list[str], title: str, expected: int) -> None:
    assert named_outcome_index(label, outcomes, (title,)) == expected


@pytest.mark.parametrize(
    ("label", "outcomes", "title"),
    [
        # Reads like both: the initials of either country, the one code that
        # Carolina's NHL and NFL teams share. A code that fits both sides at the
        # first reading that fits at all is refused, not resolved by a looser one.
        ("SA", ["Saudi Arabia", "South Africa"], "Saudi Arabia vs. South Africa"),
        ("CAR", ["Hurricanes", "Panthers"], "Hurricanes vs. Panthers"),
        # Reads like neither -- a nickname with no table entry, a stray code.
        ("BIL", ["Rayo Vallecano de Madrid", "Athletic Club"], "Spread: Rayo Vallecano de Madrid (-1.5)"),
        ("XYZ", ["Houston Dynamo", "Minnesota United FC"], "Spread: Houston Dynamo (-5.5)"),
        # Letters in order inside a word are not a reading: this is what put
        # "TEN" in "Texans" before the table said Titans.
        ("TNS", ["Texans", "Seahawks"], "Texans vs. Seahawks"),
        # Yes and No are sides, never a team code, even where a team is called "YeS".
        ("YES", ["Yellow Submarine", "Team Synapse"], "Game Handicap: YeS (-1.5) vs Team Synapse (+1.5)"),
        ("NO", ["Falcons", "Saints"], "Falcons vs. Saints"),
        # An over/under whose line is not the market's line.
        ("Over 7.5", ["Over", "Under"], "Estonia vs. Iceland: O/U 9.5 Total Corners"),
        ("Over 7.5", ["Higher", "Lower"], "Estonia vs. Iceland: O/U 7.5 Total Corners"),
        # Not a two-outcome market.
        ("HOU", ["Houston Dynamo", "Draw", "Minnesota United FC"], "Houston Dynamo vs. Minnesota United FC"),
    ],
)
def test_a_label_that_names_neither_or_both_outcomes_is_refused(label: str, outcomes: list[str], title: str) -> None:
    assert named_outcome_index(label, outcomes, (title,)) is None


def test_an_over_under_line_must_agree_across_both_titles() -> None:
    outcomes = ["Over", "Under"]

    assert named_outcome_index("Under 2.5", outcomes, ("A vs. B: O/U 2.5", "A vs. B: O/U 2.5")) == 1
    assert named_outcome_index("Under 2.5", outcomes, ("A vs. B: O/U 2.5", "A vs. B: O/U 3.5")) is None


def test_rules_texts_must_match_apart_from_case_and_spacing() -> None:
    rules = "This market will resolve to Houston Dynamo if they win by 6 or more.\nOtherwise Minnesota."

    assert same_rules_text(rules, " ".join(rules.split()).upper())
    assert not same_rules_text(rules, rules.replace("6 or more", "5 or more"))
    assert not same_rules_text(rules, None)
    assert not same_rules_text("", "")
