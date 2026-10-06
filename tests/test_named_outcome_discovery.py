"""Scan-all pairs a Predict.fun seed whose outcome is named differently -- and only when it is certain.

Predict.fun lists Polymarket's sports markets under Polymarket's condition id and
copies the rules, but names outcomes its own way. Each case builds the real
Gamma snapshot and the seeds exactly as the Predict.fun parser emits them: one
per outcome, its side being its contract slot, the hedge being the other slot.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from arbitrage_engine import market_discovery
from arbitrage_engine.models import BinarySide, MarketSpec
from arbitrage_engine.named_outcomes import NAMED_OUTCOME_STRATEGY

NOW = datetime(2026, 10, 5, 12, 0, tzinfo=UTC)
KICKOFF = NOW + timedelta(days=1)
TITLE = "Spread: Houston Dynamo (-5.5)"
RULES = (
    "In the upcoming MLS game, scheduled for October 6, Houston Dynamo will face Minnesota United FC.\n"
    "If Houston Dynamo wins by over 5.5 goals, this market will resolve to Houston Dynamo. Otherwise, "
    "Minnesota United FC. If the game is postponed, this market will resolve 50-50."
)


def _polymarket(
    *,
    market_id: str = "1001",
    condition_id: str = "0xcondition-hou",
    question: str = TITLE,
    outcomes: str = '["Houston Dynamo", "Minnesota United FC"]',
    tokens: str = '["poly-hou", "poly-min"]',
    rules: str = RULES,
) -> dict[str, Any]:
    return {
        "id": market_id,
        "question": question,
        "conditionId": condition_id,
        "endDate": KICKOFF.isoformat(),
        "outcomes": outcomes,
        "clobTokenIds": tokens,
        "description": rules,
        "active": True,
        "closed": False,
        "archived": False,
        "acceptingOrders": True,
        "enableOrderBook": True,
    }


def _predict_seeds(
    labels: tuple[str, str] = ("HOU", "MIN"),
    *,
    title: str = TITLE,
    condition_id: str = "0xcondition-hou",
    rules: str = RULES,
    venue: str = "Predict.fun",
) -> list[MarketSpec]:
    # As `_market_specs_from_payload` builds them: indexSet 1 is the YES side
    # and hedges with the indexSet 2 token, and the other way round.
    first, second = labels
    common: dict[str, Any] = {
        "symbol": title,
        "polymarket_token_id": "",
        "expires_at": KICKOFF,
        "predict_fun_market_id": "predict-7",
        "category": "sports",
        "resolution_source": "MLS",
        "outcome_semantics": rules,
        "cutoff_at": KICKOFF,
        "polymarket_market_id": condition_id,
        "condition_id": condition_id,
        "venue_b_label": venue,
    }
    return [
        MarketSpec(
            target_label=first,
            polymarket_side=BinarySide.YES,
            predict_fun_token_id="predict-slot-2",
            predict_fun_side=BinarySide.NO,
            rules_fingerprint=f"predict:predict-7:{first}",
            **common,
        ),
        MarketSpec(
            target_label=second,
            polymarket_side=BinarySide.NO,
            predict_fun_token_id="predict-slot-1",
            predict_fun_side=BinarySide.YES,
            rules_fingerprint=f"predict:predict-7:{second}",
            **common,
        ),
    ]


def _resolve(payloads: list[dict[str, Any]], seeds: list[MarketSpec]):  # type: ignore[no-untyped-def]
    snapshot = market_discovery._build_gamma_snapshot(payloads, generation=1, now=NOW)  # noqa: SLF001
    return market_discovery._resolve_scan_all_against(snapshot, seeds)  # noqa: SLF001


def test_both_outcomes_named_in_their_slots_pair_each_seed_with_its_own_slot_token() -> None:
    resolved, stats = _resolve([_polymarket()], _predict_seeds())

    assert [(market.target_label, market.polymarket_token_id) for market in resolved] == [
        ("HOU", "poly-hou"),
        ("MIN", "poly-min"),
    ]
    # Each pairing hedges with the other outcome on Predict.fun.
    assert [market.predict_fun_token_id for market in resolved] == ["predict-slot-2", "predict-slot-1"]
    assert {market.mapping_strategy for market in resolved} == {NAMED_OUTCOME_STRATEGY}
    assert {market.polymarket_market_id for market in resolved} == {"1001"}
    assert stats.named_outcome_matches == 2
    assert stats.exact_id_matches == 0
    assert stats.unresolved == 0


def test_over_under_outcomes_pair_on_the_line_both_titles_carry() -> None:
    title = "Torino FC vs. Udinese Calcio: O/U 2.5"
    payload = _polymarket(question=title, outcomes='["Over", "Under"]', tokens='["poly-over", "poly-under"]')

    resolved, stats = _resolve([payload], _predict_seeds(("Over 2.5", "Under 2.5"), title=title))

    assert [market.polymarket_token_id for market in resolved] == ["poly-over", "poly-under"]
    assert stats.named_outcome_matches == 2


def test_one_unreadable_side_leaves_the_whole_market_unpaired() -> None:
    # "HOU" reads as Houston, but "ZZZ" reads as nothing: the confirmed side
    # alone is not trusted, because the other label is what would catch a
    # first label that only looks right.
    resolved, stats = _resolve([_polymarket()], _predict_seeds(("HOU", "ZZZ")))

    assert resolved == []
    assert stats.named_outcome_matches == 0
    assert stats.unresolved == 2
    assert dict(stats.rejection_reasons) == {
        "ambiguous_outcomes": 1,
        "named_outcome_one_side_unconfirmed": 1,
    }


@pytest.mark.parametrize(
    ("labels", "exact_label", "exact_token"),
    [
        (("Houston Dynamo", "MIN"), "Houston Dynamo", "poly-hou"),
        (("HOU", "Minnesota United FC"), "Minnesota United FC", "poly-min"),
    ],
)
def test_a_market_whose_other_outcome_paired_by_exact_name_stays_exactly_as_before(
    labels: tuple[str, str], exact_label: str, exact_token: str
) -> None:
    # Both seeds share one mapping row, which records whichever strategy was
    # written last. Mixed, it would either demote an approved exact-id mapping
    # or carry the coded side past its switch as exact_id -- in whichever order
    # the two seeds arrive. So the exact-name side keeps the market as it was.
    resolved, stats = _resolve([_polymarket()], _predict_seeds(labels))

    assert [(market.target_label, market.polymarket_token_id, market.mapping_strategy) for market in resolved] == [
        (exact_label, exact_token, "exact_id")
    ]
    assert stats.exact_id_matches == 1
    assert stats.named_outcome_matches == 0
    assert dict(stats.rejection_reasons) == {"named_outcome_beside_exact_name": 1}


def test_labels_that_name_the_other_slot_are_a_contradiction_and_pair_nothing() -> None:
    # Predict.fun's slot 1 is labelled Minnesota while Polymarket's slot 1 is
    # Houston: the venues disagree about the order, so nothing here is safe.
    resolved, stats = _resolve([_polymarket()], _predict_seeds(("MIN", "HOU")))

    assert resolved == []
    assert dict(stats.rejection_reasons) == {"named_outcome_contradiction": 2}


def test_rules_that_differ_between_the_venues_pair_nothing() -> None:
    altered = RULES.replace("over 5.5 goals", "over 4.5 goals")

    resolved, stats = _resolve([_polymarket()], _predict_seeds(rules=altered))

    assert resolved == []
    assert dict(stats.rejection_reasons) == {"named_outcome_rules_differ": 2}


def test_only_predict_fun_seeds_are_read_this_way() -> None:
    resolved, stats = _resolve([_polymarket()], _predict_seeds(venue="Myriad"))

    assert resolved == []
    assert dict(stats.rejection_reasons) == {"ambiguous_outcomes": 2}


def test_yes_no_markets_and_exact_labels_resolve_exactly_as_before() -> None:
    # Nothing that paired before changes: a Yes/No market still pairs by side
    # under plain exact_id, and so does a label equal to an outcome.
    yes_no = _polymarket(
        market_id="2002", condition_id="0xcondition-yn", outcomes='["Yes", "No"]', tokens='["poly-yes", "poly-no"]'
    )
    named_exactly = _polymarket(market_id="3003", condition_id="0xcondition-exact")
    seeds = [
        *_predict_seeds(("Yes-side", "No-side"), condition_id="0xcondition-yn"),
        *_predict_seeds(("Houston Dynamo", "Minnesota United FC"), condition_id="0xcondition-exact"),
    ]

    resolved, stats = _resolve([yes_no, named_exactly], seeds)

    assert [(market.polymarket_token_id, market.mapping_strategy) for market in resolved] == [
        ("poly-yes", "exact_id"),
        ("poly-no", "exact_id"),
        ("poly-hou", "exact_id"),
        ("poly-min", "exact_id"),
    ]
    assert stats.exact_id_matches == 4
    assert stats.named_outcome_matches == 0


def test_outside_scan_all_named_outcomes_stay_unread() -> None:
    # Resolving one market at a time cannot see its other outcome, so it cannot
    # require both to confirm; it keeps refusing, as it always has.
    snapshot = market_discovery._build_gamma_snapshot([_polymarket()], generation=1, now=NOW)  # noqa: SLF001

    with pytest.raises(RuntimeError, match="no unambiguous"):
        market_discovery._resolve_market_from_snapshot(  # noqa: SLF001
            snapshot, _predict_seeds()[0], log_discovery=False
        )
