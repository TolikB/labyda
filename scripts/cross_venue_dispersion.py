"""Measure whether a candidate venue disagrees with Polymarket by more than our costs.

The engine measured 348,000 spreads across three funded venues in 57 hours and
not one of them was positive: the best it ever saw was -1.06% on
polymarket_predict. That is not a scheduling failure any more -- since the
event-driven scheduler landed, the engine sees every subscribed book move -- it
is a statement that the cost stack (fees, the 2.5% route floor, chain cost)
exceeds how much those three venues disagree about price.

Adding a venue is the obvious lever, and a venue costs four to six thousand
lines to integrate. This measures the lever before we pull it: it takes a
candidate venue's public catalog, matches it to Polymarket with the same
resolver discovery uses, pulls both public books, and prices the pair with
``calculate_spread_metrics`` -- the same arithmetic the engine runs. If a
candidate shows no positive net spread over a day of sampling, we have saved
five thousand lines; if it shows some, we know which markets and how big before
writing a connector.

It prices two strategies:

* **taker** -- what the engine does today: cross both books at once.
* **maker** -- post on the candidate venue instead of crossing it, and hedge on
  Polymarket at the touch. Limitless quotes 0% maker fees against books 3.5
  points wide, so the difference between these two numbers is the entire case
  for a maker strategy.

Read-only: it holds no keys, signs nothing, and places no orders. Public
endpoints only.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import statistics
import sys
import time
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from arbitrage_engine.http import client_session
from arbitrage_engine.market_discovery import GammaMarketResolver
from arbitrage_engine.market_mapping import normalize_category
from arbitrage_engine.matcher import MarketText
from arbitrage_engine.models import (
    BinarySide,
    MarketDataStatus,
    MarketSpec,
    OrderBook,
    OrderBookLevel,
)
from arbitrage_engine.quant import calculate_spread_metrics, orderbook_buy_quote

LIMITLESS_API = "https://api.limitless.exchange"
POLYMARKET_CLOB_API = "https://clob.polymarket.com"

# The research run wants the measurement, not the engine's refusals: the guards
# inside calculate_spread_metrics raise once slippage passes a cap, and a raise
# tells us nothing about how far past the cap the market was. Slippage is
# reported per row instead.
_UNCAPPED = 1.0


@dataclass(frozen=True)
class VenueMarket:
    """One candidate market: what to match on, and what to price."""

    text: MarketText
    book_ref: str
    minimum_notional_usd: float | None = None
    max_spread: float | None = None
    taker_delay_ms: int | None = None
    participants: tuple[str, str] | None = None


@dataclass(frozen=True)
class DispersionRow:
    symbol: str
    target_label: str
    category: str | None
    expires_at: str
    venue_market_id: str
    polymarket_token_id: str
    mapping_strategy: str | None
    leg_usd: float
    strategy: str
    gross_spread: float | None
    net_spread: float | None
    combined_cost_per_payout: float | None
    polymarket_book_walk: float | None
    venue_book_walk: float | None
    polymarket_best_ask: float | None
    venue_best_bid: float | None
    venue_best_ask: float | None
    venue_minimum_notional_usd: float | None
    blocker: str | None


# --------------------------------------------------------------------------
# Limitless catalog
# --------------------------------------------------------------------------


def limitless_market_text(payload: dict[str, Any]) -> VenueMarket | None:
    """Adapt one Limitless market to the shared MarketText contract.

    Skips what our two-leg binary engine cannot trade: AMM markets (no book),
    grouped markets (more than two outcomes), and anything already expired. Of
    483 active markets on 2026-09-29 that left 278.
    """
    if payload.get("tradeType") != "clob" or payload.get("marketType") != "single":
        return None
    if payload.get("expired") or payload.get("hidden"):
        return None
    slug = _text(payload.get("slug"))
    title = _text(payload.get("title"))
    expires_at = _epoch_ms(payload.get("expirationTimestamp"))
    tokens = payload.get("tokens")
    if not slug or not title or expires_at is None or not isinstance(tokens, dict):
        return None
    yes_token = _text(tokens.get("yes"))
    no_token = _text(tokens.get("no"))
    if not yes_token or not no_token:
        return None

    raw_settings = payload.get("settings")
    settings: dict[str, Any] = raw_settings if isinstance(raw_settings, dict) else {}
    decimals = _collateral_decimals(payload)
    categories = payload.get("categories")
    category = None
    if isinstance(categories, list):
        for candidate in categories:
            category = normalize_category(_text(candidate))
            if category:
                break

    # The structured sports matcher identifies a market by its participants, not
    # by its title -- Polymarket writes "Chengdu Open: Hurkacz vs Davidovich
    # Fokina" where Limitless writes "Hubert Hurkacz vs Alejandro Davidovich
    # Fokina", and no title similarity bridges that. Limitless publishes the two
    # sides in metadata, and without them every sports market resolves against
    # the literal string "Yes".
    metadata = payload.get("metadata")
    home = away = None
    if isinstance(metadata, dict):
        home = _text(metadata.get("homeTeam"))
        away = _text(metadata.get("awayTeam"))

    text = MarketText(
        platform="limitless",
        market_id=slug,
        title=title,
        expires_at=expires_at,
        yes_label=home or "Yes",
        no_label=away or "No",
        volume_usd=_number(payload.get("volumeFormatted")),
        public_url=f"https://limitless.exchange/markets/{slug}",
        category=category,
        resolution_source=_text(payload.get("description")),
        outcome_semantics=_text(payload.get("description")),
        condition_id=_text(payload.get("conditionId")),
        # The outcome token ids ride in collateral_token, the way Opinion does
        # it, so the shared MarketText contract does not grow a field per venue.
        collateral_token=f"{yes_token}|{no_token}",
    )
    return VenueMarket(
        text=text,
        book_ref=slug,
        minimum_notional_usd=_scaled(settings.get("minSize"), decimals),
        max_spread=_number(settings.get("maxSpread")),
        taker_delay_ms=_int(settings.get("takerDelayMs")),
        participants=(home, away) if home and away else None,
    )


def venue_market_specs(market: VenueMarket, *, venue_label: str) -> list[MarketSpec]:
    """Two seeds per market -- one per side -- for the Gamma resolver to match.

    Slot B of MarketSpec is the generic hedge slot Predict.fun, SX Bet and
    Opinion already share; a candidate venue borrows it rather than earning
    fields of its own.

    ``venue_label`` is not cosmetic: the resolver keys its matching rules off
    it. "SX Bet" matches on the market title, runs the structured sports path
    over participants and competition, and allows a seven-day cutoff window;
    any other label matches on the outcome label instead, which for a venue
    whose outcomes are named "Yes" and "No" compares the string "Yes" against
    35,000 Polymarket titles and matches nothing. A candidate venue that is not
    yet in EXECUTION_ROUTES therefore borrows the SX profile to be measured at
    all -- see --match-as.
    """
    text = market.text
    # sports_market_identity reads a proposition, and the resolver hands it only
    # the YES label -- on a bare "X vs Y" title that is not enough to decide
    # whether the market is a moneyline, a spread or a total, and it returns
    # nothing. The same title written as a proposition parses on the title
    # alone, which is why SX Bet's own catalog matches and this one did not:
    # 0 structured matches across 275 markets before this. Only sports markets
    # are rewritten; everything else keeps the title that matches Polymarket's
    # exactly.
    title = text.title
    if market.participants is not None:
        title = f"Will {market.participants[0]} beat {market.participants[1]}"
    tokens = (text.collateral_token or "").split("|")
    if len(tokens) != 2 or not all(tokens):
        return []
    yes_token, no_token = tokens
    specs: list[MarketSpec] = []
    for target_label, polymarket_side, hedge_side, hedge_token in (
        (text.yes_label, BinarySide.YES, BinarySide.NO, no_token),
        (text.no_label, BinarySide.NO, BinarySide.YES, yes_token),
    ):
        specs.append(
            MarketSpec(
                symbol=title,
                target_label=target_label,
                polymarket_token_id="",
                polymarket_side=polymarket_side,
                predict_fun_token_id=hedge_token,
                predict_fun_side=hedge_side,
                venue_b_label=venue_label,
                expires_at=text.expires_at,
                predict_fun_market_id=text.market_id,
                predict_fun_url=text.public_url,
                rules_fingerprint=f"{text.platform}:{text.market_id}:{polymarket_side.value.lower()}",
                predict_fun_volume_usd=text.volume_usd,
                category=text.category,
                resolution_source=text.resolution_source,
                outcome_semantics=text.outcome_semantics,
                cutoff_at=text.expires_at,
            )
        )
    return specs


# --------------------------------------------------------------------------
# Books
# --------------------------------------------------------------------------


def order_book_from_limitless(
    payload: dict[str, Any],
    side: BinarySide,
    *,
    decimals: int = 6,
) -> OrderBook:
    """Build the book for one side from Limitless's single YES-quoted book.

    Limitless publishes one book per market, quoted in the YES token, the way
    every binary CLOB does. Buying NO at ``p`` is selling YES at ``1 - p``, so
    the NO book is the mirror of the YES book: the asks we can lift for NO are
    the bids standing on YES. Sizes arrive in collateral units.
    """
    yes_bids = _levels(payload.get("bids"), decimals=decimals)
    yes_asks = _levels(payload.get("asks"), decimals=decimals)
    if side is BinarySide.YES:
        bids, asks = yes_bids, yes_asks
    else:
        bids = [OrderBookLevel(1.0 - level.price, level.size) for level in yes_asks]
        asks = [OrderBookLevel(1.0 - level.price, level.size) for level in yes_bids]
    return OrderBook(
        bids=sorted((level for level in bids if 0 < level.price < 1), key=lambda entry: -entry.price),
        asks=sorted((level for level in asks if 0 < level.price < 1), key=lambda entry: entry.price),
        status=MarketDataStatus.VALID,
    )


def order_book_from_polymarket(payload: dict[str, Any]) -> OrderBook:
    bids = _levels(payload.get("bids"), decimals=0)
    asks = _levels(payload.get("asks"), decimals=0)
    return OrderBook(
        bids=sorted((level for level in bids if 0 < level.price < 1), key=lambda entry: -entry.price),
        asks=sorted((level for level in asks if 0 < level.price < 1), key=lambda entry: entry.price),
        status=MarketDataStatus.VALID,
    )


def maker_book(
    book: OrderBook,
    *,
    tick: float,
    leg_usd: float,
    max_spread: float,
) -> OrderBook | None:
    """The book we would face by posting instead of crossing.

    A maker order fills at the price we name, so the ask side collapses to one
    level at that price, one tick in front of the best bid.

    The quote is only offered against a genuinely two-sided book. Limitless
    books carry stub levels at 0.001 and 0.999, and improving a 0.001 bid to
    0.002 in a market whose ask is 0.35 models a fill that will never happen --
    it reported 35 of 43 pairs profitable at up to +95%, which is not edge, it
    is a quote nobody would ever lift. A book wider than ``max_spread`` has no
    bid worth joining, so it yields no maker row at all.
    """
    best_bid = book.bids[0].price if book.bids else None
    best_ask = book.asks[0].price if book.asks else None
    if best_bid is None or best_ask is None:
        return None
    if best_ask - best_bid > max_spread:
        return None
    price = best_bid + tick
    if price >= best_ask:
        # The book is already a tick wide: there is nothing for a maker to earn
        # and the quote must not become a taker order wearing a maker's fee.
        price = best_ask - tick
    if not 0 < price < 1:
        return None
    return OrderBook(
        bids=book.bids,
        asks=[OrderBookLevel(price, leg_usd / price)],
        status=MarketDataStatus.VALID,
    )


# --------------------------------------------------------------------------
# Pricing
# --------------------------------------------------------------------------


def price_pair(
    market: MarketSpec,
    *,
    polymarket_book: OrderBook,
    venue_book: OrderBook,
    venue_market: VenueMarket | None,
    leg_usd: float,
    strategy: str,
    polymarket_fee_pct: float,
    venue_fee_pct: float,
    chain_cost_usd: float,
) -> DispersionRow:
    def _row(
        *,
        gross: float | None = None,
        net: float | None = None,
        combined: float | None = None,
        poly_walk: float | None = None,
        venue_walk: float | None = None,
        blocker: str | None = None,
    ) -> DispersionRow:
        return DispersionRow(
            symbol=market.symbol,
            target_label=market.target_label,
            category=market.category,
            expires_at=market.expires_at.isoformat() if market.expires_at else "",
            venue_market_id=market.predict_fun_market_id or "",
            polymarket_token_id=market.polymarket_token_id,
            mapping_strategy=market.mapping_strategy,
            leg_usd=leg_usd,
            strategy=strategy,
            gross_spread=gross,
            net_spread=net,
            combined_cost_per_payout=combined,
            polymarket_book_walk=poly_walk,
            venue_book_walk=venue_walk,
            polymarket_best_ask=polymarket_book.asks[0].price if polymarket_book.asks else None,
            venue_best_bid=venue_book.bids[0].price if venue_book.bids else None,
            venue_best_ask=venue_book.asks[0].price if venue_book.asks else None,
            venue_minimum_notional_usd=venue_market.minimum_notional_usd if venue_market else None,
            blocker=blocker,
        )

    if not polymarket_book.asks:
        return _row(blocker="polymarket_no_asks")
    if not venue_book.asks:
        return _row(blocker="venue_no_asks")

    try:
        metrics = calculate_spread_metrics(
            polymarket_book,
            venue_book,
            max_order_size_usd=leg_usd,
            min_net_spread=0.0,
            max_slippage_pct=_UNCAPPED,
            max_price_impact=_UNCAPPED,
            polymarket_side=market.polymarket_side,
            predict_fun_side=market.predict_fun_side,
            polymarket_fee_pct=polymarket_fee_pct,
            predict_fun_fee_pct=venue_fee_pct,
            fixed_chain_cost_usd=chain_cost_usd,
        )
    except ValueError as exc:
        return _row(blocker=str(exc).replace(" ", "_"))

    # SpreadMetrics' slippage fields price the leg after fees and chain cost,
    # so reading them as a book walk charges the fee twice and hides which of
    # the two levers -- the fee or the depth -- is actually costing us. The walk
    # is measured straight off the books instead.
    return _row(
        gross=metrics.gross_spread,
        net=metrics.net_spread,
        combined=metrics.combined_cost_per_payout,
        poly_walk=_book_walk(polymarket_book, leg_usd),
        venue_walk=_book_walk(venue_book, leg_usd),
    )


def _book_walk(book: OrderBook, leg_usd: float) -> float | None:
    """How far the leg moves the price, fees excluded."""
    try:
        return orderbook_buy_quote(book, leg_usd).slippage_pct
    except (ValueError, IndexError):
        return None


def summarise(rows: Sequence[DispersionRow]) -> dict[str, Any]:
    """Per (strategy, leg size): how often the market paid, and how much."""
    summary: dict[str, Any] = {}
    keys = sorted({(row.strategy, row.leg_usd) for row in rows})
    for strategy, leg_usd in keys:
        scoped = [row for row in rows if row.strategy == strategy and row.leg_usd == leg_usd]
        priced = [row for row in scoped if row.net_spread is not None]
        spreads = sorted(row.net_spread for row in priced if row.net_spread is not None)
        blockers: dict[str, int] = {}
        for row in scoped:
            if row.blocker:
                blockers[row.blocker] = blockers.get(row.blocker, 0) + 1
        summary[f"{strategy}@{leg_usd:g}"] = {
            "pairs": len(scoped),
            "priced": len(priced),
            "positive_net_spread": sum(1 for spread in spreads if spread > 0),
            "best_net_spread": spreads[-1] if spreads else None,
            "median_net_spread": statistics.median(spreads) if spreads else None,
            "worst_net_spread": spreads[0] if spreads else None,
            "blockers": dict(sorted(blockers.items(), key=lambda item: -item[1])),
        }
    return summary


def cost_decomposition(rows: Sequence[DispersionRow]) -> dict[str, Any]:
    """Split the cost of a leg into the spread we cross and the fee we pay.

    The 6.97% we measured on a $25 Polymarket leg is a single number that hides
    two different levers: a fee is cut by negotiating or by venue choice, a book
    walk is cut by leg size or by posting instead of crossing. They are not the
    same problem and only one of them is fixed by adding a venue.
    """
    out: dict[str, Any] = {}
    for leg_usd in sorted({row.leg_usd for row in rows}):
        scoped = [
            row
            for row in rows
            if row.leg_usd == leg_usd and row.strategy == "taker" and row.polymarket_book_walk is not None
        ]
        if not scoped:
            continue
        poly = sorted(row.polymarket_book_walk for row in scoped if row.polymarket_book_walk is not None)
        venue = sorted(row.venue_book_walk for row in scoped if row.venue_book_walk is not None)
        out[f"{leg_usd:g}"] = {
            "samples": len(scoped),
            "polymarket_book_walk_median": statistics.median(poly) if poly else None,
            "polymarket_book_walk_p90": poly[int(len(poly) * 0.9) - 1] if poly else None,
            "venue_book_walk_median": statistics.median(venue) if venue else None,
            "venue_book_walk_p90": venue[int(len(venue) * 0.9) - 1] if venue else None,
        }
    return out


# --------------------------------------------------------------------------
# HTTP
# --------------------------------------------------------------------------


class _Http:
    """A counted GET. A research tool that reports zero matches because its own
    HTTP layer broke is worse than one that reports nothing at all, so every
    failure is tallied and printed beside the result."""

    def __init__(self, *, concurrency: int = 4) -> None:
        # client_session pins aiohttp's ThreadedResolver. The default async
        # resolver times out contacting DNS on some hosts, and it fails as an
        # empty catalog rather than as an error.
        self._session = client_session({"User-Agent": "labyda-dispersion-research/1.0"})
        self._semaphore = asyncio.Semaphore(concurrency)
        self.requests = 0
        self.failures: dict[str, int] = {}

    def _fail(self, reason: str) -> None:
        self.failures[reason] = self.failures.get(reason, 0) + 1

    async def json(self, url: str, params: dict[str, Any] | None = None) -> Any | None:
        # aiohttp rejects non-string query values outright, and swallowing that
        # TypeError once cost a whole run that read as "the venue has no
        # tradeable markets".
        encoded = {key: str(value) for key, value in (params or {}).items()}
        async with self._semaphore:
            self.requests += 1
            try:
                async with self._session.get(url, params=encoded, timeout=25) as response:
                    if response.status != 200:
                        self._fail(f"http_{response.status}")
                        return None
                    return await response.json(content_type=None)
            except Exception as exc:
                self._fail(type(exc).__name__)
                return None

    async def close(self) -> None:
        await self._session.close()


async def fetch_limitless_catalog(http: _Http, *, max_pages: int = 40) -> list[VenueMarket]:
    markets: list[VenueMarket] = []
    seen = 0
    for page in range(1, max_pages + 1):
        payload = await http.json(f"{LIMITLESS_API}/markets/active", {"limit": 25, "page": page})
        rows = (payload or {}).get("data") if isinstance(payload, dict) else None
        if not rows:
            break
        seen += len(rows)
        for row in rows:
            market = limitless_market_text(row)
            if market is not None:
                markets.append(market)
        total = (payload or {}).get("totalMarketsCount") if isinstance(payload, dict) else None
        if isinstance(total, int) and seen >= total:
            break
    return markets


# --------------------------------------------------------------------------
# Run
# --------------------------------------------------------------------------


async def run(args: argparse.Namespace) -> dict[str, Any]:
    http = _Http(concurrency=args.concurrency)
    gamma = GammaMarketResolver(scan_all=True, include_sports_catalog=True)
    try:
        _progress("fetching the candidate venue catalog")
        catalog = await fetch_limitless_catalog(http)
        _progress(f"tradeable candidate markets: {len(catalog)}")
        by_id = {candidate.text.market_id: candidate for candidate in catalog}
        seeds: list[MarketSpec] = []
        for candidate in catalog:
            seeds.extend(venue_market_specs(candidate, venue_label=args.match_as))

        _progress(f"loading the Polymarket catalog for {len(seeds)} seeds (this is the slow part)")
        await gamma.bootstrap(seeds)
        _progress(f"Polymarket catalog: {gamma.catalog_size} markets; matching")
        resolved = await gamma.resolve(seeds)
        matched = [market for market in resolved if market.polymarket_token_id]
        stats = gamma.last_resolution_stats

        # One pair per market, not two: both sides of the same market price the
        # same trade from opposite ends, and pricing both doubles the row count
        # without adding an observation.
        unique: dict[str, MarketSpec] = {}
        for market in matched:
            key = market.predict_fun_market_id or ""
            if key and key not in unique:
                unique[key] = market
        pairs = sorted(unique.values(), key=lambda market: market.symbol)[: args.limit]

        _progress(f"matched {len(unique)} markets; pricing {len(pairs)}")
        rows: list[DispersionRow] = []
        for index, market in enumerate(pairs, start=1):
            if index % 25 == 0:
                _progress(f"  priced {index}/{len(pairs)}")
            venue_market = by_id.get(market.predict_fun_market_id or "")
            if venue_market is None:
                continue
            venue_payload, poly_payload = await asyncio.gather(
                http.json(f"{LIMITLESS_API}/markets/{venue_market.book_ref}/orderbook"),
                http.json(
                    f"{POLYMARKET_CLOB_API}/book",
                    {"token_id": market.polymarket_token_id},
                ),
            )
            if not isinstance(venue_payload, dict) or not isinstance(poly_payload, dict):
                continue
            venue_book = order_book_from_limitless(venue_payload, market.predict_fun_side)
            poly_book = order_book_from_polymarket(poly_payload)

            for leg_usd in args.leg_usd:
                rows.append(
                    price_pair(
                        market,
                        polymarket_book=poly_book,
                        venue_book=venue_book,
                        venue_market=venue_market,
                        leg_usd=leg_usd,
                        strategy="taker",
                        polymarket_fee_pct=args.polymarket_fee_pct,
                        venue_fee_pct=args.venue_taker_fee_pct,
                        chain_cost_usd=args.chain_cost_usd,
                    )
                )
                posted = maker_book(
                    venue_book,
                    tick=args.tick,
                    leg_usd=leg_usd,
                    max_spread=venue_market.max_spread or args.max_maker_spread,
                )
                if posted is not None:
                    rows.append(
                        price_pair(
                            market,
                            polymarket_book=poly_book,
                            venue_book=posted,
                            venue_market=venue_market,
                            leg_usd=leg_usd,
                            strategy="maker",
                            polymarket_fee_pct=args.polymarket_fee_pct,
                            venue_fee_pct=args.venue_maker_fee_pct,
                            chain_cost_usd=args.chain_cost_usd,
                        )
                    )

        return {
            "captured_at": datetime.now(UTC).isoformat(),
            "venue": args.venue_label,
            "match_profile": args.match_as,
            "venue_catalog_tradeable": len(catalog),
            "seeds": len(seeds),
            "matched_sides": len(matched),
            "matched_markets": len(unique),
            "priced_markets": len(pairs),
            "polymarket_catalog_size": gamma.catalog_size,
            "resolution_stats": {
                "requested": stats.requested,
                "exact_id_matches": stats.exact_id_matches,
                "exact_title_matches": stats.exact_title_matches,
                "structured_sports_matches": stats.structured_sports_matches,
                "semantic_matches": stats.semantic_matches,
                "unresolved": stats.unresolved,
                "rejection_reasons": dict(stats.rejection_reasons),
            },
            "fees": {
                "polymarket_fee_pct": args.polymarket_fee_pct,
                "venue_taker_fee_pct": args.venue_taker_fee_pct,
                "venue_maker_fee_pct": args.venue_maker_fee_pct,
                "chain_cost_usd": args.chain_cost_usd,
            },
            "http": {"requests": http.requests, "failures": dict(http.failures)},
            "summary": summarise(rows),
            "cost_decomposition": cost_decomposition(rows),
            "rows": [row.__dict__ for row in rows],
        }
    finally:
        await asyncio.gather(http.close(), gamma.close(), return_exceptions=True)


def _print_human(report: dict[str, Any]) -> None:
    print(f"venue                     {report['venue']}")
    print(f"tradeable venue markets   {report['venue_catalog_tradeable']}")
    print(f"polymarket catalog        {report['polymarket_catalog_size']}")
    print(f"matched markets           {report['matched_markets']}")
    print(f"priced markets            {report['priced_markets']}")
    http = report.get("http", {})
    print(f"http requests             {http.get('requests')}  failures={http.get('failures') or '{}'}")
    stats = report["resolution_stats"]
    print(
        "  exact_id={exact_id_matches} exact_title={exact_title_matches} "
        "structured_sports={structured_sports_matches} semantic={semantic_matches} "
        "unresolved={unresolved}".format(**stats)
    )
    print()
    print(f"{'scenario':>16}  {'priced':>6}  {'positive':>8}  {'best':>9}  {'median':>9}")
    for scenario, values in report["summary"].items():
        best = values["best_net_spread"]
        median = values["median_net_spread"]
        print(
            f"{scenario:>16}  {values['priced']:>6}  {values['positive_net_spread']:>8}  "
            f"{(f'{best:+.2%}' if best is not None else '-'):>9}  "
            f"{(f'{median:+.2%}' if median is not None else '-'):>9}"
        )
    if report["cost_decomposition"]:
        print()
        print("book walk (taker), median / p90:")
        for leg, values in report["cost_decomposition"].items():
            poly_median = values["polymarket_book_walk_median"]
            poly_p90 = values["polymarket_book_walk_p90"]
            venue_median = values["venue_book_walk_median"]
            venue_p90 = values["venue_book_walk_p90"]
            print(
                f"  ${leg:>5}  polymarket {_pct(poly_median)} / {_pct(poly_p90)}"
                f"   venue {_pct(venue_median)} / {_pct(venue_p90)}"
            )


def _progress(message: str) -> None:
    """Phase markers on stderr: the Polymarket catalog load runs for minutes and
    a silent tool is one nobody can tell apart from a hung one."""
    print(f"[{time.strftime('%H:%M:%S')}] {message}", file=sys.stderr, flush=True)


def _pct(value: float | None) -> str:
    return f"{value:.2%}" if value is not None else "   -  "


# --------------------------------------------------------------------------
# Parsing helpers
# --------------------------------------------------------------------------


def _text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _number(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _scaled(value: Any, decimals: int) -> float | None:
    raw = _number(value)
    return None if raw is None else raw / (10**decimals)


def _epoch_ms(value: Any) -> datetime | None:
    raw = _number(value)
    if raw is None or raw <= 0:
        return None
    try:
        return datetime.fromtimestamp(raw / 1000.0, tz=UTC)
    except (OverflowError, OSError, ValueError):
        return None


def _collateral_decimals(payload: dict[str, Any]) -> int:
    collateral = payload.get("collateralToken")
    if isinstance(collateral, dict):
        decimals = _int(collateral.get("decimals"))
        if decimals is not None:
            return decimals
    return 6


def _levels(raw: Any, *, decimals: int) -> list[OrderBookLevel]:
    if not isinstance(raw, list):
        return []
    scale = 10**decimals
    levels: list[OrderBookLevel] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        price = _number(item.get("price"))
        size = _number(item.get("size"))
        if price is None or size is None or price <= 0 or size <= 0:
            continue
        levels.append(OrderBookLevel(price, size / scale))
    return levels


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Measure a candidate venue's price dispersion against Polymarket (read-only)",
    )
    parser.add_argument("--venue-label", default="Limitless", help="Name for the report")
    parser.add_argument(
        "--match-as",
        default="SX Bet",
        help=(
            "Matching profile the Gamma resolver applies to the seeds. \"SX Bet\" matches on "
            "market title with the structured sports path; anything else matches on the "
            "outcome label and will not match a Yes/No venue."
        ),
    )
    parser.add_argument(
        "--leg-usd",
        type=float,
        action="append",
        help="Leg notional to price; repeatable. Defaults to 25 and 100.",
    )
    parser.add_argument("--limit", type=int, default=200, help="Maximum matched markets to price")
    parser.add_argument("--concurrency", type=int, default=4)
    parser.add_argument("--tick", type=float, default=0.001, help="Price tick for the maker quote")
    parser.add_argument(
        "--polymarket-fee-pct",
        type=float,
        default=0.0,
        help="Explicit Polymarket trading fee; the book walk is measured separately",
    )
    parser.add_argument(
        "--venue-taker-fee-pct",
        type=float,
        default=0.03,
        help="Candidate venue taker fee. Limitless quotes 0.40-3.00%%; this defaults to the top.",
    )
    parser.add_argument("--venue-maker-fee-pct", type=float, default=0.0)
    parser.add_argument(
        "--max-maker-spread",
        type=float,
        default=0.10,
        help="Widest book a maker quote is credible against, when the venue states no cap",
    )
    parser.add_argument("--chain-cost-usd", type=float, default=0.0)
    parser.add_argument("--json", help="Write the full report, rows included, to this path")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if not args.leg_usd:
        args.leg_usd = [25.0, 100.0]
    started = time.time()
    report = asyncio.run(run(args))
    report["elapsed_seconds"] = round(time.time() - started, 1)
    if args.json:
        with open(args.json, "w", encoding="utf-8") as handle:
            json.dump(report, handle, indent=2)
    _print_human(report)
    return 0


if __name__ == "__main__":
    sys.exit(main())
