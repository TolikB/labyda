# SX Bet Integration Plan

> This document records the V2 implementation and historical evidence. The
> dual-stack V3 implementation and mandatory cutover procedure are documented
> in `ops/SX_BET_V3_CUTOVER.md`. Repository production configs stage V3, but
> deployment remains blocked until the documented cutover timestamp and until
> authenticated mainnet account and runtime gates pass without order submission.

## Current conclusion

SX Bet is now wired into the repo as a real second-leg route family, not only as a probe.
The connector supports:

- discovery and market-data translation from SX maker liquidity into binary books
- taker entry fills through `POST /orders/fill/v2`
- venue-aware early exit through an opposite-outcome fill
- trade-based fills/positions reconciliation
- venue-side settlement tracking without manual redemption transactions

The remaining blockers are production proof and operator parity, not basic connector viability.
The runtime can now host both second-leg route families in one process, but that still
needs evidence-backed rollout and route-by-route canary proof on the live VM.

The repo's live engine is built around tokenized binary positions that:

- open with `buy`
- can be monitored as venue orders
- can be cancelled if not fully filled
- can later be exited with `sell`
- reconcile as inventory-like positions plus open orders/fills

SX Bet's taker flow is different:

- the runtime object is a sports market identified by `marketHash`
- the trading side is `outcome one` vs `outcome two`, not shared YES/NO claim tokens
- a taker fill is an immediate bet submission, not a reusable token position
- post-fill lifecycle is settlement or separate hedge, not the same `sell` path used by the current engine
- open-order management mostly applies to maker orders, while taker fills are signed and matched immediately

The current branch closes that gap by adapting SX Bet to the shared two-leg engine
contract instead of pretending it is a tokenized CLOB venue. The remaining work is
production proof, route coverage, and VM evidence, not basic route wiring.

## Evidence

Official SX Bet docs and live API show:

- mainnet chain is `4162` with base URL `https://api.sx.bet`
- market objects are sports fixtures with `marketHash`, `outcomeOneName`, `outcomeTwoName`, `sportXeventId`, `type`, `line`
- odds are fixed-point implied probabilities in `percentageOdds / 10^20`
- taker odds are derived from the opposite maker side: `taker_implied = 1 - maker_implied`
- taker liquidity must be derived from maker space:
  - `remainingTakerSpace = (remainingMaker * 10^20 / percentageOdds) - remainingMaker`
- fill submission is `POST /orders/fill/v2` with `market`, `isTakerBettingOutcomeOne`, `stakeWei`, `desiredOdds`, `oddsSlippage`, `taker`, `takerSig`
- cancel endpoints apply to open maker orders, not already-filled taker bets

Live contract evidence captured on 2026-07-01:

- `scripts/sx_bet_probe.py` reached `https://api.sx.bet` with the API key supplied only through the environment
- `/metadata` returned chain `4162`, `domainVersion=6.0`, `EIP712FillHasher`,
  USDC base token `0x6629Ce1Cf35Cc1329ebB4F63202F3f197b3F050B`, and taker minimums
- `/markets/active` returned active sports markets with live fields such as
  `marketHash`, `gameTime`, `group1`, `teamOneName`, `teamTwoName`, numeric `type`,
  `line`, `outcomeOneName`, and `outcomeTwoName`
- `/orders?marketHashes=...` returned maker orders from which the probe derived
  both `OUTCOME_ONE` and `OUTCOME_TWO` taker liquidity
- `/user/realtime-token/api-key` returned an authenticated realtime token when
  `SX_BET_API_KEY` was supplied
- `ARB_RUN_LIVE_SCHEMA_CONTRACTS=1` with `tests/test_live_schema_contracts.py -k sx_bet`
  passed against the live API

Relevant official pages:

- [Authentication](https://docs.sx.bet/developers/authentication.md)
- [Markets](https://docs.sx.bet/developers/markets-and-sports.md)
- [Odds & Tokens](https://docs.sx.bet/developers/odds-and-tokens.md)
- [Orderbook](https://docs.sx.bet/developers/orderbook-core.md)
- [Filling Orders](https://docs.sx.bet/developers/filling-orders.md)
- [References](https://docs.sx.bet/api-reference/references.md)

## Repo drift still visible in code shape

The repo still carries legacy Predict.fun-shaped field names in several layers even
though SX Bet routes are now runnable:

- `src/arbitrage_engine/models.py`
  - `MarketSpec.predict_fun_token_id`, `predict_fun_side`, `venue_b_label`
  - `OpenPosition.predict_fun_*`
  - `PositionPlan.predict_fun_*`
- `src/arbitrage_engine/config.py`
  - `PredictFunConfig`
  - route flags: `polymarket_predict`, `predict_myriad`
- `src/arbitrage_engine/main.py`
  - Predict.fun-specific startup and resolver wiring
- `src/arbitrage_engine/engine.py`
  - route evaluation assumes the second venue is the opposite binary side
- `src/arbitrage_engine/quant.py`
  - position math assumes a shared binary payout model where both legs can later be exited through venue-side `sell`
- `src/arbitrage_engine/execution.py`
  - entry uses `buy`
  - exit requires `sell`
  - partial-fill recovery requires `cancel_order`
  - reconciliation assumes order/position state compatible with reusable inventory

## What is already reusable in runtime

- SX binary market prices can be mapped to the repo's `price-per-payout` math.
- SX orderbook depth can be converted into taker-side liquidity.
- SX reconciliation can ingest fills and balances.
- SX now fits the live engine as a market-data venue plus a taker execution venue
  through adapter logic in the connector, engine wiring, and reconciliation path.

## What had to change for real runtime support

### Phase 1: Generalize the third venue slot

Status: implemented with backward-compatible aliases, though the stored field names
still use the historical `predict_fun_*` schema.

- stop using Predict.fun field names as the generic second venue model
- introduce neutral second-leg naming in models, router state, config, and persistence
- keep backward-compatible config parsing during migration

### Phase 2: Separate outcome-side semantics from YES/NO semantics

Status: implemented in discovery and execution adapters.

- current discovery assumes shared YES/NO propositions
- SX needs `OUTCOME_ONE` / `OUTCOME_TWO` semantics with venue-specific labels
- mapping logic must understand sports fixture identity, market type, line, and side

### Phase 3: Build an SX-specific connector contract

Status: implemented in `src/arbitrage_engine/connectors/sx_bet.py`.

- market data:
  - `GET /markets/active`
  - `GET /orders`
  - optional realtime token + Centrifugo orderbook channels
- account state:
  - on-chain USDC balance on SX chain
  - optional API-key realtime auth verification
- execution:
  - sign `POST /orders/fill/v2`
  - model maker-order posting separately if needed later

### Phase 4: Introduce an SX execution lifecycle

This phase is now implemented in the current branch:

- `sell` on SX is modeled as an opposite-outcome taker fill
- orderbook bids are synthesized from same-market opposite-side hedgeability
- reconciliation uses SX trade history rather than token balances
- settlement is modeled as venue settlement status, not on-chain redemption

That is enough for SX canary/live route validation inside the existing two-leg engine.

### Phase 5: Add a sports-aware discovery and mapping pipeline

Status: implemented in `src/arbitrage_engine/sx_bet_discovery.py`, but still needs
live VM canary proof for the enabled route set.

- current universe is sourced from Gamma + Predict.fun + Myriad proposition markets
- SX needs fixture-based matching:
  - sport
  - league
  - event id / kickoff
  - market type
  - line
  - side/outcome
- only after this can routes such as `polymarket_sx` or `sx_myriad` be evaluated honestly

## Current operator tooling

This repo now has SX-specific operator tooling:

- `scripts/sx_bet_probe.py`
- `scripts/sx_bet_balance_and_order_preview.py`
- `scripts/live_balance_and_order_readiness.py`

The probe fetches:

- SX metadata
- a live market payload
- live order payloads for that market
- derived taker-side liquidity for both outcomes
- optional realtime token validation when `SX_BET_API_KEY` is set, with legacy `SX_API_KEY` fallback

The balance/preview and unified readiness scripts add:

- direct base-token balance probe from the same wallet runtime
- connector-visible versus direct balance parity
- durable runtime audit blockers such as unresolved intents or reconciliation failures
- optional signed fill preview for a concrete `market_hash/token_id/outcome_side/order_side/price/size`
- combined canary go/no-go reporting across Polymarket, Predict.fun, SX Bet, and Myriad

These scripts are safe to run without submitting live SX fills.

## Remaining no-go

Do not claim production-closeout for SX yet.

What still remains:

- live funded-wallet proof through `SX_BET_PRIVATE_KEY` and the active base token
- live VM evidence for natural `polymarket_sx` and `sx_myriad` open-order paths
- verified mapping coverage for the enabled SX routes on the active production database
- production-closeout verification on the active VM with `/health/live`, `/health/ready`,
  metrics, logs, and canary evidence for the SX family
