"""The dispersion probe decides whether a venue is worth five thousand lines.

Its arithmetic is therefore worth pinning: a mirrored book that is off by one
side, or a maker quote that quietly crosses, would manufacture edge that is not
there and send us building a connector against a number we invented.
"""

from __future__ import annotations

import importlib.util
import sys
from datetime import UTC, datetime
from pathlib import Path

import pytest

from arbitrage_engine.models import BinarySide, MarketDataStatus, OrderBook, OrderBookLevel

_SPEC = importlib.util.spec_from_file_location(
    "cross_venue_dispersion",
    Path(__file__).resolve().parents[1] / "scripts" / "cross_venue_dispersion.py",
)
assert _SPEC is not None and _SPEC.loader is not None
dispersion = importlib.util.module_from_spec(_SPEC)
sys.modules["cross_venue_dispersion"] = dispersion
_SPEC.loader.exec_module(dispersion)


def limitless_payload(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "slug": "hubert-hurkacz-vs-alejandro-davidovich-fokina-1790604005511",
        "title": "Hubert Hurkacz vs Alejandro Davidovich Fokina",
        "tradeType": "clob",
        "marketType": "single",
        "expired": False,
        "hidden": False,
        "expirationTimestamp": 1790604005511,
        "conditionId": "0xabc",
        "description": "Resolves to the match winner.",
        "categories": ["Sports"],
        "volumeFormatted": "3645.633000",
        "metadata": {"homeTeam": "Hubert Hurkacz", "awayTeam": "Alejandro Davidovich Fokina"},
        "collateralToken": {"address": "0x83", "decimals": 6, "symbol": "USDC"},
        "tokens": {"yes": "103071926785918447837", "no": "889912233445566778899"},
        "settings": {"minSize": "100000000", "maxSpread": 0.035, "takerDelayMs": 250},
    }
    payload.update(overrides)
    return payload


def test_only_markets_our_two_leg_engine_can_trade_enter_the_catalog() -> None:
    # 483 active Limitless markets on 2026-09-29 were 454 clob and 29 AMM, and
    # 176 of them grouped more than two outcomes. Pricing either kind with a
    # binary two-leg model would report an edge the engine could never take.
    assert dispersion.limitless_market_text(limitless_payload()) is not None
    assert dispersion.limitless_market_text(limitless_payload(tradeType="amm")) is None
    assert dispersion.limitless_market_text(limitless_payload(marketType="group")) is None
    assert dispersion.limitless_market_text(limitless_payload(expired=True)) is None
    assert dispersion.limitless_market_text(limitless_payload(tokens={"yes": "1"})) is None


def test_the_catalog_carries_what_the_size_decision_needs() -> None:
    market = dispersion.limitless_market_text(limitless_payload())
    assert market is not None
    # $100, not 100000000: the minimum is quoted in collateral units, and the
    # difference between those two readings is the difference between "we can
    # afford this venue" and "we cannot".
    assert market.minimum_notional_usd == pytest.approx(100.0)
    assert market.taker_delay_ms == 250
    assert market.text.expires_at == datetime.fromtimestamp(1790604005.511, tz=UTC)
    assert market.text.collateral_token == "103071926785918447837|889912233445566778899"
    # Polymarket titles the same match "Chengdu Open: Hubert Hurkacz vs ...".
    # Only the participants bridge that, and they arrive in metadata.
    assert market.text.yes_label == "Hubert Hurkacz"
    assert market.text.no_label == "Alejandro Davidovich Fokina"


def test_both_sides_of_a_market_become_matchable_seeds() -> None:
    market = dispersion.limitless_market_text(limitless_payload())
    assert market is not None
    specs = dispersion.venue_market_specs(market, venue_label="Limitless")
    assert [spec.polymarket_side for spec in specs] == [BinarySide.YES, BinarySide.NO]
    # The hedge leg is always the opposite outcome: the engine buys YES on one
    # venue and NO on the other, and a seed that pairs a side with itself would
    # price a position that pays nothing.
    assert [spec.predict_fun_side for spec in specs] == [BinarySide.NO, BinarySide.YES]
    assert {spec.venue_b_label for spec in specs} == {"Limitless"}
    assert specs[0].predict_fun_token_id == "889912233445566778899"
    assert specs[1].predict_fun_token_id == "103071926785918447837"


def test_the_no_book_is_the_mirror_of_the_yes_book() -> None:
    # A live book on 2026-09-29: bid 0.295, ask 0.35, 1000 contracts a side.
    payload = {
        "bids": [{"price": 0.295, "size": 1_000_000_000}],
        "asks": [{"price": 0.35, "size": 1_000_000_000}],
    }
    yes = dispersion.order_book_from_limitless(payload, BinarySide.YES)
    assert yes.best_ask.price == pytest.approx(0.35)
    assert yes.best_ask.size == pytest.approx(1000.0)

    # Buying NO is selling YES: the NO ask is 1 - the YES bid. Getting this
    # backwards turns a 5.5-point spread into a 5.5-point edge.
    no = dispersion.order_book_from_limitless(payload, BinarySide.NO)
    assert no.best_ask.price == pytest.approx(1 - 0.295)
    assert no.best_bid.price == pytest.approx(1 - 0.35)
    assert no.best_ask.size == pytest.approx(1000.0)


def test_a_maker_quote_improves_the_bid_without_crossing() -> None:
    book = OrderBook(
        bids=[OrderBookLevel(0.295, 1000.0)],
        asks=[OrderBookLevel(0.35, 1000.0)],
        status=MarketDataStatus.VALID,
    )
    posted = dispersion.maker_book(book, tick=0.001, leg_usd=100.0, max_spread=0.10)
    assert posted is not None
    assert posted.best_ask.price == pytest.approx(0.296)
    # The leg still has to buy $100 worth at that price.
    assert posted.best_ask.size == pytest.approx(100.0 / 0.296)


def test_a_book_one_tick_wide_leaves_a_maker_nothing_to_earn() -> None:
    book = OrderBook(
        bids=[OrderBookLevel(0.349, 1000.0)],
        asks=[OrderBookLevel(0.350, 1000.0)],
        status=MarketDataStatus.VALID,
    )
    posted = dispersion.maker_book(book, tick=0.001, leg_usd=25.0, max_spread=0.10)
    assert posted is not None
    # Improving the bid would cross, so the quote sits a tick inside the ask
    # instead -- it must never become a taker order wearing a maker's fee.
    assert posted.best_ask.price == pytest.approx(0.349)
    assert posted.best_ask.price < book.asks[0].price


def test_an_empty_book_yields_no_maker_quote() -> None:
    empty = OrderBook(bids=[], asks=[], status=MarketDataStatus.VALID)
    assert dispersion.maker_book(empty, tick=0.001, leg_usd=25.0, max_spread=0.10) is None


def test_posting_instead_of_crossing_is_what_separates_the_two_strategies() -> None:
    # The venue quotes 0.295/0.35 and Polymarket's other side is 0.66. Crossing
    # both books costs 0.35 + 0.66 = 1.01 and loses money; posting at 0.296
    # costs 0.956 and does not. This is the whole maker argument in one row.
    market = dispersion.limitless_market_text(limitless_payload())
    assert market is not None
    spec = dispersion.venue_market_specs(market, venue_label="Limitless")[0]
    spec = spec.__class__(**{**spec.__dict__, "polymarket_token_id": "poly-token"})

    venue_book = OrderBook(
        bids=[OrderBookLevel(0.295, 10_000.0)],
        asks=[OrderBookLevel(0.35, 10_000.0)],
        status=MarketDataStatus.VALID,
    )
    poly_book = OrderBook(
        bids=[OrderBookLevel(0.65, 10_000.0)],
        asks=[OrderBookLevel(0.66, 10_000.0)],
        status=MarketDataStatus.VALID,
    )

    taker = dispersion.price_pair(
        spec,
        polymarket_book=poly_book,
        venue_book=venue_book,
        venue_market=market,
        leg_usd=25.0,
        strategy="taker",
        polymarket_fee_pct=0.0,
        venue_fee_pct=0.0,
        chain_cost_usd=0.0,
    )
    assert taker.net_spread is not None
    assert taker.net_spread == pytest.approx(1 - (0.35 + 0.66), abs=1e-9)
    assert taker.net_spread < 0

    posted = dispersion.maker_book(venue_book, tick=0.001, leg_usd=25.0, max_spread=0.10)
    assert posted is not None
    maker = dispersion.price_pair(
        spec,
        polymarket_book=poly_book,
        venue_book=posted,
        venue_market=market,
        leg_usd=25.0,
        strategy="maker",
        polymarket_fee_pct=0.0,
        venue_fee_pct=0.0,
        chain_cost_usd=0.0,
    )
    assert maker.net_spread is not None
    assert maker.net_spread == pytest.approx(1 - (0.296 + 0.66), abs=1e-9)
    assert maker.net_spread > taker.net_spread


def test_fees_and_chain_cost_are_charged_against_the_edge() -> None:
    market = dispersion.limitless_market_text(limitless_payload())
    assert market is not None
    spec = dispersion.venue_market_specs(market, venue_label="Limitless")[0]
    spec = spec.__class__(**{**spec.__dict__, "polymarket_token_id": "poly-token"})
    venue_book = OrderBook(
        bids=[OrderBookLevel(0.29, 10_000.0)],
        asks=[OrderBookLevel(0.30, 10_000.0)],
        status=MarketDataStatus.VALID,
    )
    poly_book = OrderBook(
        bids=[OrderBookLevel(0.64, 10_000.0)],
        asks=[OrderBookLevel(0.65, 10_000.0)],
        status=MarketDataStatus.VALID,
    )
    free = dispersion.price_pair(
        spec,
        polymarket_book=poly_book,
        venue_book=venue_book,
        venue_market=market,
        leg_usd=25.0,
        strategy="taker",
        polymarket_fee_pct=0.0,
        venue_fee_pct=0.0,
        chain_cost_usd=0.0,
    )
    charged = dispersion.price_pair(
        spec,
        polymarket_book=poly_book,
        venue_book=venue_book,
        venue_market=market,
        leg_usd=25.0,
        strategy="taker",
        polymarket_fee_pct=0.0,
        venue_fee_pct=0.03,
        chain_cost_usd=0.10,
    )
    assert free.net_spread is not None and charged.net_spread is not None
    assert charged.net_spread < free.net_spread
    # 5% gross survives a 3% taker fee on a 0.30 leg; it is the kind of margin
    # the venue's own 3.5-point maxSpread makes rare.
    assert free.net_spread == pytest.approx(0.05, abs=1e-9)


def test_an_unpriceable_pair_is_reported_rather_than_dropped() -> None:
    market = dispersion.limitless_market_text(limitless_payload())
    assert market is not None
    spec = dispersion.venue_market_specs(market, venue_label="Limitless")[0]
    spec = spec.__class__(**{**spec.__dict__, "polymarket_token_id": "poly-token"})
    empty = OrderBook(bids=[], asks=[], status=MarketDataStatus.VALID)
    poly_book = OrderBook(
        bids=[OrderBookLevel(0.64, 10.0)],
        asks=[OrderBookLevel(0.65, 10.0)],
        status=MarketDataStatus.VALID,
    )
    # A venue whose books are empty is a finding about the venue, so the row
    # carries a blocker instead of vanishing from the denominator.
    row = dispersion.price_pair(
        spec,
        polymarket_book=poly_book,
        venue_book=empty,
        venue_market=market,
        leg_usd=25.0,
        strategy="taker",
        polymarket_fee_pct=0.0,
        venue_fee_pct=0.0,
        chain_cost_usd=0.0,
    )
    assert row.net_spread is None
    assert row.blocker == "venue_no_asks"

    summary = dispersion.summarise([row])
    assert summary["taker@25"]["pairs"] == 1
    assert summary["taker@25"]["priced"] == 0
    assert summary["taker@25"]["blockers"] == {"venue_no_asks": 1}


def test_the_summary_counts_the_only_number_that_decides_anything() -> None:
    rows = [
        dispersion.DispersionRow(
            symbol=f"m{index}",
            target_label="Yes",
            category="sports",
            expires_at="",
            venue_market_id=f"m{index}",
            polymarket_token_id="t",
            mapping_strategy="semantic_title",
            leg_usd=25.0,
            strategy="taker",
            gross_spread=spread,
            net_spread=spread,
            combined_cost_per_payout=1 - spread,
            polymarket_book_walk=0.0,
            venue_book_walk=0.0,
            polymarket_best_ask=0.5,
            venue_best_bid=0.4,
            venue_best_ask=0.45,
            venue_minimum_notional_usd=100.0,
            blocker=None,
        )
        for index, spread in enumerate((-0.04, -0.01, 0.002))
    ]
    summary = dispersion.summarise(rows)["taker@25"]
    assert summary["positive_net_spread"] == 1
    assert summary["best_net_spread"] == pytest.approx(0.002)
    assert summary["worst_net_spread"] == pytest.approx(-0.04)


def test_a_stub_bid_earns_no_maker_quote() -> None:
    # Limitless books carry parked levels at 0.001 and 0.999. Improving a 0.001
    # bid to 0.002 against a 0.35 ask models a fill nobody would ever give us,
    # and it read as 35 of 43 pairs profitable at up to +95% before this rule.
    book = OrderBook(
        bids=[OrderBookLevel(0.001, 1000.0)],
        asks=[OrderBookLevel(0.35, 1000.0)],
        status=MarketDataStatus.VALID,
    )
    assert dispersion.maker_book(book, tick=0.001, leg_usd=25.0, max_spread=0.035) is None
    # A book inside the venue's own maxSpread is a real quote and does earn one.
    tight = OrderBook(
        bids=[OrderBookLevel(0.32, 1000.0)],
        asks=[OrderBookLevel(0.35, 1000.0)],
        status=MarketDataStatus.VALID,
    )
    posted = dispersion.maker_book(tight, tick=0.001, leg_usd=25.0, max_spread=0.035)
    assert posted is not None
    assert posted.best_ask.price == pytest.approx(0.321)


def test_a_one_sided_book_earns_no_maker_quote() -> None:
    # Several of the busiest Limitless markets had asks and no bids at all.
    # Posting alone in an empty market is not a measurement of edge.
    one_sided = OrderBook(
        bids=[],
        asks=[OrderBookLevel(0.36, 1000.0)],
        status=MarketDataStatus.VALID,
    )
    assert dispersion.maker_book(one_sided, tick=0.001, leg_usd=25.0, max_spread=0.035) is None


def test_the_book_walk_is_reported_without_the_fee_folded_into_it() -> None:
    # SpreadMetrics prices its second leg after fees and chain cost, so reading
    # its slippage as a book walk reports the fee as depth. Fee and depth are
    # different levers -- one is venue choice, the other is leg size -- and the
    # decomposition exists to tell them apart.
    market = dispersion.limitless_market_text(limitless_payload())
    assert market is not None
    spec = dispersion.venue_market_specs(market, venue_label="SX Bet")[0]
    spec = spec.__class__(**{**spec.__dict__, "polymarket_token_id": "poly-token"})
    flat = OrderBook(
        bids=[OrderBookLevel(0.29, 10_000.0)],
        asks=[OrderBookLevel(0.30, 10_000.0)],
        status=MarketDataStatus.VALID,
    )
    poly = OrderBook(
        bids=[OrderBookLevel(0.64, 10_000.0)],
        asks=[OrderBookLevel(0.65, 10_000.0)],
        status=MarketDataStatus.VALID,
    )
    row = dispersion.price_pair(
        spec,
        polymarket_book=flat,
        venue_book=poly,
        venue_market=market,
        leg_usd=25.0,
        strategy="taker",
        polymarket_fee_pct=0.0,
        venue_fee_pct=0.03,
        chain_cost_usd=0.0,
    )
    # One level deep enough to absorb the leg: the walk is zero whatever the fee.
    assert row.venue_book_walk == pytest.approx(0.0)
    assert row.polymarket_book_walk == pytest.approx(0.0)


def test_a_sports_market_is_seeded_as_a_proposition_the_matcher_can_read() -> None:
    # Polymarket titles the match "Chengdu Open: Hurkacz vs Davidovich Fokina";
    # Limitless titles it "Hubert Hurkacz vs Alejandro Davidovich Fokina". No
    # title similarity bridges that, so the structured sports path has to carry
    # it -- and that path reads a proposition, not a fixture line. Across 275
    # markets the bare title produced 0 structured matches.
    from arbitrage_engine.sports_matching import sports_market_identity

    market = dispersion.limitless_market_text(limitless_payload())
    assert market is not None
    assert market.participants == ("Hubert Hurkacz", "Alejandro Davidovich Fokina")
    spec = dispersion.venue_market_specs(market, venue_label="SX Bet")[0]
    assert spec.symbol == "Will Hubert Hurkacz beat Alejandro Davidovich Fokina"

    # The resolver passes the YES label only, so this must resolve without the
    # opposite one.
    identity = sports_market_identity(spec.symbol, yes_label=spec.target_label)
    assert identity is not None
    assert identity.kind == "moneyline"
    assert identity.subject == "hubert hurkacz"


def test_a_market_without_participants_keeps_the_title_that_matches_exactly() -> None:
    # Pre-TGE and politics markets carry identical titles on both venues and
    # match on the title alone; rewriting those would lose matches, not win them.
    payload = limitless_payload(
        title="Will Pacifica launch a token by September 30, 2026?",
        categories=["Pre-TGE"],
    )
    payload.pop("metadata")
    market = dispersion.limitless_market_text(payload)
    assert market is not None
    assert market.participants is None
    spec = dispersion.venue_market_specs(market, venue_label="SX Bet")[0]
    assert spec.symbol == "Will Pacifica launch a token by September 30, 2026?"
