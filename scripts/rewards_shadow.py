"""Shadow market-making for Polymarket's liquidity rewards: no orders, every quote simulated on the live book.

A day of books (2026-10-09/10) said a minimum-size two-sided quote would take
a large share of many reward pools that nobody else is quoting for. A day of
snapshots cannot say what a quote that is actually standing in the book would
live through: being repriced as the mid moves, waiting behind the size that
was there first, being filled by the trade that knows something. This runs
that quote continuously instead, against Polymarket's market WebSocket, and
keeps the books it would have kept:

* Selection. Every few hours the rewarded markets are ranked by expected
  reward per dollar locked at their current books, and the best are taken
  until the capital budget is spent. Markets that hold a position stay.
* Quoting. Min-size bid and ask at the best bid and ask when the book is
  narrower than the market's max spread, otherwise half the max spread from
  the mid; nothing within STOP_BEFORE_END_SECONDS of the market's end; one side
  dropped once the position is MAX_INVENTORY_LOTS lots long or short.
* Queue. A quote that moves to a new price joins behind everything resting
  there; size cancelled ahead of it moves it up, trades at its price eat the
  queue first, a trade through its price fills it.
* Rewards. Once a minute each market's pool accrues by our share of the
  score Polymarket documents: ((v - s) / v)^2 * size per order, the smaller
  side counting (a single side counts a third while the mid is within
  0.10-0.90). Rivals are bounded both ways because the book does not say
  which order is whose.
* Fills are marked to the mid 30 s, 5 min and 30 min later.

Read-only: public endpoints, no keys, no orders. State is kept in OUT so a
restart continues the books.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import os
import time
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

CLOB = "https://clob.polymarket.com"
WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
SINGLE_SIDED_DIVISOR = 3.0
STOP_BEFORE_END_SECONDS = 6 * 3600
MAX_INVENTORY_LOTS = 3
MARKOUT_DELAYS = (30, 300, 1800)
REWARD_SAMPLE_SECONDS = 60
SELECTION_SECONDS = 6 * 3600
STATUS_SECONDS = 60
MAX_MARKETS = 120
MIN_DAILY_POOL = 5.0


# ----------------------------------------------------------------- the model


@dataclass(frozen=True)
class RewardMarket:
    condition_id: str
    question: str
    yes_token: str
    no_token: str
    daily: float
    max_spread: float
    min_size: float
    tick: float
    end: float | None


@dataclass
class Quote:
    price: float
    size: float
    queue_ahead: float


@dataclass
class MarketState:
    market: RewardMarket
    bids: dict[float, float] = field(default_factory=dict)
    asks: dict[float, float] = field(default_factory=dict)
    bid: Quote | None = None
    ask: Quote | None = None
    position: float = 0.0
    cash: float = 0.0
    reward_opt: float = 0.0
    reward_pes: float = 0.0
    quoted_minutes: int = 0
    fills: int = 0
    filled_size: float = 0.0
    markout: dict[int, float] = field(default_factory=lambda: dict.fromkeys(MARKOUT_DELAYS, 0.0))
    pending: list[tuple[float, str, float, float, int]] = field(default_factory=list)
    seen_trades: dict[str, None] = field(default_factory=dict)

    def best_bid(self) -> float | None:
        return max(self.bids) if self.bids else None

    def best_ask(self) -> float | None:
        return min(self.asks) if self.asks else None

    def mid(self) -> float | None:
        bid, ask = self.best_bid(), self.best_ask()
        if bid is None or ask is None or ask <= bid:
            return None
        return (bid + ask) / 2

    def capital(self) -> float:
        locked = 0.0
        if self.bid is not None:
            locked += self.bid.price * self.bid.size
        if self.ask is not None:
            locked += (1 - self.ask.price) * self.ask.size
        mid = self.mid() or 0.5
        return locked + abs(self.position) * (mid if self.position > 0 else 1 - mid)

    def mark_to_market(self) -> float:
        mid = self.mid()
        return self.cash + self.position * (mid if mid is not None else 0.5)


def order_score(max_spread: float, distance: float) -> float:
    if distance >= max_spread:
        return 0.0
    return ((max_spread - distance) / max_spread) ** 2


def q_min(q_bid: float, q_ask: float, mid: float) -> float:
    if 0.10 <= mid <= 0.90:
        return max(min(q_bid, q_ask), max(q_bid, q_ask) / SINGLE_SIDED_DIVISOR)
    return min(q_bid, q_ask)


def rival_scores(levels: dict[float, float], mid: float, max_spread: float) -> float:
    return sum(order_score(max_spread, abs(price - mid)) * size for price, size in levels.items())


def _floor_tick(price: float, tick: float) -> float:
    return round(math.floor(price / tick + 1e-9) * tick, 6)


def _ceil_tick(price: float, tick: float) -> float:
    return round(math.ceil(price / tick - 1e-9) * tick, 6)


def plan_quotes(state: MarketState, now: float) -> tuple[float | None, float | None]:
    m = state.market
    bid, ask = state.best_bid(), state.best_ask()
    if bid is None or ask is None or ask <= bid:
        return None, None
    if m.end is not None and m.end - now < STOP_BEFORE_END_SECONDS:
        return None, None
    mid = (bid + ask) / 2
    if (ask - bid) / 2 < m.max_spread:
        our_bid, our_ask = bid, ask
    else:
        our_bid = _floor_tick(mid - m.max_spread / 2, m.tick)
        our_ask = _ceil_tick(mid + m.max_spread / 2, m.tick)
    planned_bid: float | None = our_bid if 0 < our_bid < 1 else None
    planned_ask: float | None = our_ask if 0 < our_ask < 1 else None
    if state.position >= MAX_INVENTORY_LOTS * m.min_size:
        planned_bid = None
    if state.position <= -MAX_INVENTORY_LOTS * m.min_size:
        planned_ask = None
    return planned_bid, planned_ask


def requote(state: MarketState, now: float) -> None:
    """Move our quotes to the plan; a moved quote joins the back of its new level."""
    planned_bid, planned_ask = plan_quotes(state, now)
    state.bid = _requote_side(state.bid, planned_bid, state.bids, state.market.min_size)
    state.ask = _requote_side(state.ask, planned_ask, state.asks, state.market.min_size)


def _requote_side(quote: Quote | None, price: float | None, levels: dict[float, float], size: float) -> Quote | None:
    if price is None:
        return None
    if quote is None or quote.price != price:
        return Quote(price, size, levels.get(price, 0.0))
    # Same price: anything cancelled ahead of us moves us up.
    quote.queue_ahead = min(quote.queue_ahead, levels.get(price, 0.0))
    return quote


def apply_trade(state: MarketState, yes_price: float, taker_side: str, size: float, now: float) -> float:
    """A trade on the YES book; returns how much of our quote it filled."""
    mid = state.mid()
    if taker_side == "SELL" and state.bid is not None and yes_price <= state.bid.price:
        quote, side = state.bid, "bid"
    elif taker_side == "BUY" and state.ask is not None and yes_price >= state.ask.price:
        quote, side = state.ask, "ask"
    else:
        return 0.0
    if (side == "bid" and yes_price < quote.price) or (side == "ask" and yes_price > quote.price):
        filled = quote.size  # the trade went through our price: our level was taken whole
    else:
        filled = min(quote.size, max(0.0, size - quote.queue_ahead))
        quote.queue_ahead = max(0.0, quote.queue_ahead - size)
    if filled <= 0:
        return 0.0
    quote.size -= filled
    if side == "bid":
        state.position += filled
        state.cash -= quote.price * filled
    else:
        state.position -= filled
        state.cash += quote.price * filled
    state.fills += 1
    state.filled_size += filled
    for delay in MARKOUT_DELAYS:
        state.pending.append((now + delay, side, quote.price, filled, delay))
    if quote.size <= 1e-9:
        if side == "bid":
            state.bid = None
        else:
            state.ask = None
    del mid
    return filled


def settle_markouts(state: MarketState, now: float) -> None:
    mid = state.mid()
    if mid is None:
        return
    keep = []
    for due, side, price, filled, delay in state.pending:
        if due > now:
            keep.append((due, side, price, filled, delay))
            continue
        state.markout[delay] += (mid - price) * filled if side == "bid" else (price - mid) * filled
    state.pending = keep


def accrue_rewards(state: MarketState, seconds: float) -> None:
    mid = state.mid()
    m = state.market
    if mid is None or (state.bid is None and state.ask is None):
        return
    ours_bid = order_score(m.max_spread, mid - state.bid.price) * state.bid.size if state.bid else 0.0
    ours_ask = order_score(m.max_spread, state.ask.price - mid) * state.ask.size if state.ask else 0.0
    ours = q_min(ours_bid, ours_ask, mid)
    if ours <= 0:
        return
    q_bid = rival_scores(state.bids, mid, m.max_spread)
    q_ask = rival_scores(state.asks, mid, m.max_spread)
    optimistic = min(q_bid, q_ask)
    pessimistic = optimistic + (abs(q_bid - q_ask) / SINGLE_SIDED_DIVISOR if 0.10 <= mid <= 0.90 else 0.0)
    pool = m.daily * seconds / 86400
    state.reward_opt += pool * ours / (optimistic + ours)
    state.reward_pes += pool * ours / (pessimistic + ours)
    state.quoted_minutes += 1


# ----------------------------------------------------------------- the feed


def _get(url: str, body: Any = None) -> Any:
    data = None if body is None else json.dumps(body).encode()
    request = urllib.request.Request(
        url,
        data=data,
        headers={"accept": "application/json", "content-type": "application/json", "user-agent": "labyda-research/1"},
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)


def load_reward_markets() -> list[RewardMarket]:
    markets, cursor = [], ""
    for _ in range(80):
        page = _get(f"{CLOB}/sampling-markets?next_cursor={cursor}")
        for raw in page.get("data") or []:
            reward = raw.get("rewards") or {}
            daily = sum(float(r.get("rewards_daily_rate") or 0) for r in reward.get("rates") or [])
            tokens = raw.get("tokens") or []
            if daily < MIN_DAILY_POOL or len(tokens) != 2 or not reward.get("max_spread") or not reward.get("min_size"):
                continue
            end = raw.get("end_date_iso")
            markets.append(RewardMarket(
                condition_id=raw["condition_id"],
                question=(raw.get("question") or "")[:100],
                yes_token=str(tokens[0]["token_id"]),
                no_token=str(tokens[1]["token_id"]),
                daily=daily,
                max_spread=float(reward["max_spread"]) / 100.0,
                min_size=float(reward["min_size"]),
                tick=float(raw.get("minimum_tick_size") or 0.01),
                end=datetime.fromisoformat(end.replace("Z", "+00:00")).timestamp() if end else None,
            ))
        cursor = page.get("next_cursor") or ""
        if not cursor or cursor == "LTE=":
            break
        time.sleep(0.3)
    return markets


def _levels(raw: Any) -> dict[float, float]:
    levels = {}
    for item in raw or []:
        try:
            price, size = float(item["price"]), float(item["size"])
        except (KeyError, TypeError, ValueError):
            continue
        if size > 0:
            levels[price] = size
    return levels


def select_markets(
    candidates: list[RewardMarket], budget: float, now: float, keep: dict[str, float]
) -> list[RewardMarket]:
    """The markets whose current books promise the most reward per dollar locked, until the budget is spent.

    `keep` maps the markets that hold a position to the capital they already
    tie up; they stay, and that capital counts against the budget.
    """
    books: dict[str, dict[str, Any]] = {}
    tokens = [m.yes_token for m in candidates]
    for start in range(0, len(tokens), 400):
        try:
            for book in _get(f"{CLOB}/books", [{"token_id": t} for t in tokens[start:start + 400]]):
                books[str(book.get("asset_id"))] = book
        except Exception:  # noqa: BLE001 - a missing batch only narrows the choice
            pass
        time.sleep(0.5)
    ranked = []
    for m in candidates:
        book = books.get(m.yes_token)
        if not book:
            continue
        state = MarketState(m, _levels(book.get("bids")), _levels(book.get("asks")))
        requote(state, now)
        if state.bid is None or state.ask is None:
            continue
        mid = state.mid() or 0.5
        ours = q_min(
            order_score(m.max_spread, mid - state.bid.price) * m.min_size,
            order_score(m.max_spread, state.ask.price - mid) * m.min_size,
            mid,
        )
        rivals = min(rival_scores(state.bids, mid, m.max_spread), rival_scores(state.asks, mid, m.max_spread))
        reward = m.daily * ours / (rivals + ours) if ours > 0 else 0.0
        capital = state.capital()
        if capital > 0:
            ranked.append((reward / capital, capital, m))
    chosen, spent = [], sum(keep.values())
    by_id = {m.condition_id: m for m in candidates}
    for cond in keep:
        if cond in by_id:
            chosen.append(by_id[cond])
    for _, capital, m in sorted(ranked, key=lambda r: r[0], reverse=True):
        if len(chosen) >= MAX_MARKETS:
            break
        if m.condition_id in keep or spent + capital > budget:
            continue
        chosen.append(m)
        spent += capital
    return chosen


class Shadow:
    def __init__(self, out: str, budget: float) -> None:
        self.out = out
        self.budget = budget
        self.states: dict[str, MarketState] = {}
        self.by_token: dict[str, tuple[MarketState, bool]] = {}
        self.started = time.time()
        self.carried: dict[str, dict[str, Any]] = {}
        os.makedirs(out, exist_ok=True)
        self._load()

    # -- persistence
    def _load(self) -> None:
        path = os.path.join(self.out, "state.json")
        if os.path.exists(path):
            with open(path) as handle:
                saved = json.load(handle)
            self.started = saved.get("started", self.started)
            self.carried = saved.get("markets", {})

    def save(self) -> None:
        markets = dict(self.carried)
        for cond, s in self.states.items():
            markets[cond] = {
                "q": s.market.question, "daily": s.market.daily, "position": s.position, "cash": s.cash,
                "reward_opt": s.reward_opt, "reward_pes": s.reward_pes, "fills": s.fills,
                "filled_size": s.filled_size, "quoted_minutes": s.quoted_minutes,
                "markout": {str(k): v for k, v in s.markout.items()}, "mtm": s.mark_to_market(),
                "capital": s.capital(), "mid": s.mid(),
            }
        tmp = os.path.join(self.out, "state.json.tmp")
        with open(tmp, "w") as handle:
            json.dump({"started": self.started, "saved": time.time(), "markets": markets}, handle)
        os.replace(tmp, os.path.join(self.out, "state.json"))

    def adopt(self, markets: list[RewardMarket]) -> None:
        fresh = {}
        for m in markets:
            state = self.states.get(m.condition_id)
            if state is None:
                state = MarketState(m)
                carried = self.carried.pop(m.condition_id, None)
                if carried:
                    state.position, state.cash = carried["position"], carried["cash"]
                    state.reward_opt, state.reward_pes = carried["reward_opt"], carried["reward_pes"]
                    state.fills, state.filled_size = carried["fills"], carried["filled_size"]
                    state.quoted_minutes = carried["quoted_minutes"]
                    state.markout = {int(k): v for k, v in carried["markout"].items()}
            fresh[m.condition_id] = state
        for cond, state in self.states.items():
            if cond not in fresh:
                self.carried[cond] = {
                    "q": state.market.question, "daily": state.market.daily, "position": state.position,
                    "cash": state.cash, "reward_opt": state.reward_opt, "reward_pes": state.reward_pes,
                    "fills": state.fills, "filled_size": state.filled_size, "quoted_minutes": state.quoted_minutes,
                    "markout": {str(k): v for k, v in state.markout.items()}, "mtm": state.mark_to_market(),
                    "capital": 0.0, "mid": state.mid(),
                }
        self.states = fresh
        self.by_token = {}
        for state in fresh.values():
            self.by_token[state.market.yes_token] = (state, True)
            self.by_token[state.market.no_token] = (state, False)

    # -- feed handling
    def handle(self, item: dict[str, Any], now: float) -> None:
        kind = item.get("event_type")
        if kind == "book":
            entry = self.by_token.get(str(item.get("asset_id")))
            if entry and entry[1]:
                state = entry[0]
                state.bids, state.asks = _levels(item.get("bids")), _levels(item.get("asks"))
                requote(state, now)
        elif kind == "price_change":
            touched = set()
            for change in item.get("price_changes") or []:
                entry = self.by_token.get(str(change.get("asset_id")))
                if not entry or not entry[1]:
                    continue
                state = entry[0]
                try:
                    price, size = float(change["price"]), float(change["size"])
                except (KeyError, TypeError, ValueError):
                    continue
                levels = state.bids if str(change.get("side")).upper() == "BUY" else state.asks
                if size <= 0:
                    levels.pop(price, None)
                else:
                    levels[price] = size
                touched.add(state.market.condition_id)
            for cond in touched:
                requote(self.states[cond], now)
        elif kind == "last_trade_price":
            entry = self.by_token.get(str(item.get("asset_id")))
            if not entry:
                return
            state, is_yes = entry
            try:
                price, size = float(item["price"]), float(item["size"])
            except (KeyError, TypeError, ValueError):
                return
            side = str(item.get("side")).upper()
            yes_price, yes_side = (price, side) if is_yes else (1 - price, "SELL" if side == "BUY" else "BUY")
            key = f"{item.get('transaction_hash')}:{round(yes_price, 6)}:{size}"
            if key in state.seen_trades:
                return
            state.seen_trades[key] = None
            if len(state.seen_trades) > 2000:
                state.seen_trades = dict.fromkeys(list(state.seen_trades)[-1000:])
            apply_trade(state, yes_price, yes_side, size, now)

    def tick(self, seconds: float, now: float) -> None:
        for state in self.states.values():
            accrue_rewards(state, seconds)
            settle_markouts(state, now)
            requote(state, now)

    def status(self) -> dict[str, Any]:
        rows = list(self.carried.values()) + [
            {"reward_opt": s.reward_opt, "reward_pes": s.reward_pes, "mtm": s.mark_to_market(), "fills": s.fills,
             "capital": s.capital(), "markout": {str(k): v for k, v in s.markout.items()}, "position": s.position}
            for s in self.states.values()
        ]
        hours = max((time.time() - self.started) / 3600, 1e-9)
        return {
            "hours": round(hours, 2),
            "markets_live": len(self.states),
            "quoting_now": sum(1 for s in self.states.values() if s.bid or s.ask),
            "capital_now": round(sum(s.capital() for s in self.states.values()), 2),
            "reward_opt": round(sum(r["reward_opt"] for r in rows), 2),
            "reward_pes": round(sum(r["reward_pes"] for r in rows), 2),
            "mtm": round(sum(r["mtm"] for r in rows), 2),
            "fills": sum(r["fills"] for r in rows),
            "markout_30m": round(sum(r["markout"].get("1800", 0.0) for r in rows), 2),
        }


def _append_status(out: str, now: int, status: dict[str, Any]) -> None:
    with open(os.path.join(out, "status.jsonl"), "a") as handle:
        handle.write(json.dumps({"t": now, **status}) + "\n")
    print(f"{time.strftime('%H:%M')} {json.dumps(status)}", flush=True)


async def run(out: str, budget: float) -> None:
    import aiohttp

    shadow = Shadow(out, budget)
    while True:
        now = time.time()
        candidates = await asyncio.to_thread(load_reward_markets)
        keep = {c: s.capital() for c, s in shadow.states.items() if abs(s.position) > 1e-9}
        for c, saved in shadow.carried.items():
            if abs(saved.get("position", 0.0)) > 1e-9:
                keep.setdefault(c, abs(saved["position"]) * 0.5)
        chosen = await asyncio.to_thread(select_markets, candidates, budget, now, keep)
        shadow.adopt(chosen)
        tokens = [t for s in shadow.states.values() for t in (s.market.yes_token, s.market.no_token)]
        print(f"{time.strftime('%H:%M')} selected {len(chosen)} markets, {len(tokens)} tokens", flush=True)
        selection_ends = time.time() + SELECTION_SECONDS
        try:
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(WS_URL, heartbeat=10) as ws:
                    for start in range(0, len(tokens), 400):
                        payload: dict[str, Any] = {
                            "assets_ids": tokens[start:start + 400],
                            "custom_feature_enabled": True,
                        }
                        payload["type" if start == 0 else "operation"] = "market" if start == 0 else "subscribe"
                        await ws.send_json(payload)
                    last_tick = last_status = time.time()
                    while time.time() < selection_ends:
                        try:
                            msg = await asyncio.wait_for(ws.receive(), timeout=5)
                        except TimeoutError:
                            await ws.send_str("PING")
                            msg = None
                        now = time.time()
                        if msg is not None:
                            if msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                break
                            if msg.type == aiohttp.WSMsgType.TEXT and msg.data != "PONG":
                                try:
                                    data = json.loads(msg.data)
                                except ValueError:
                                    data = []
                                for item in data if isinstance(data, list) else [data]:
                                    if isinstance(item, dict):
                                        shadow.handle(item, now)
                        if now - last_tick >= REWARD_SAMPLE_SECONDS:
                            shadow.tick(now - last_tick, now)
                            last_tick = now
                        if now - last_status >= STATUS_SECONDS:
                            last_status = now
                            shadow.save()
                            _append_status(out, int(now), shadow.status())
        except Exception as exc:  # noqa: BLE001 - reconnect and carry on
            print(f"{time.strftime('%H:%M')} feed error {type(exc).__name__}: {exc}", flush=True)
            await asyncio.sleep(5)
        shadow.save()


def report(out: str) -> None:
    with open(os.path.join(out, "state.json")) as handle:
        saved = json.load(handle)
    rows = list(saved["markets"].values())
    hours = (saved["saved"] - saved["started"]) / 3600
    days = max(hours / 24, 1e-9)
    rew_o = sum(r["reward_opt"] for r in rows)
    rew_p = sum(r["reward_pes"] for r in rows)
    mtm = sum(r["mtm"] for r in rows)
    print(f"{hours:.1f} h, {len(rows)} markets ever quoted")
    print(f"rewards ${rew_p:,.2f}..${rew_o:,.2f} (${rew_p / days:,.2f}..${rew_o / days:,.2f}/day)")
    print(f"inventory mark-to-market ${mtm:+,.2f}; fills {sum(r['fills'] for r in rows)}; "
          f"markout 30 s/5 min/30 min ${sum(r['markout']['30'] for r in rows):+,.2f}/"
          f"${sum(r['markout']['300'] for r in rows):+,.2f}/${sum(r['markout']['1800'] for r in rows):+,.2f}")
    print(f"net (pessimistic rewards + mark-to-market) ${(rew_p + mtm) / days:,.2f}/day")
    for r in sorted(rows, key=lambda r: r["reward_pes"] + r["mtm"], reverse=True)[:12]:
        print(f"  {r['reward_pes'] + r['mtm']:+8.2f} reward {r['reward_pes']:7.2f} mtm {r['mtm']:+7.2f} "
              f"fills {r['fills']:3d} pos {r['position']:+7.1f}  {r['q'][:60]}")
    for r in sorted(rows, key=lambda r: r["mtm"])[:5]:
        print(f"  worst mtm {r['mtm']:+8.2f} pos {r['position']:+7.1f} fills {r['fills']:3d}  {r['q'][:60]}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", default="/probe/shadow")
    parser.add_argument("--budget", type=float, default=1500.0)
    parser.add_argument("--report", action="store_true")
    args = parser.parse_args()
    if args.report:
        report(args.out)
    else:
        asyncio.run(run(args.out, args.budget))


if __name__ == "__main__":
    main()
