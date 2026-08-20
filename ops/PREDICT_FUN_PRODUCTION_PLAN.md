# Predict.fun Production Plan

## Current conclusion

Predict.fun is already a real second-leg venue in this repo, not a placeholder.
The current codebase already supports:

- `polymarket_predict`
- `predict_myriad`
- staged coexistence with the control route `polymarket_myriad`

The remaining gap is not basic architecture. The remaining gap is production
proof: balance parity, verified route coverage, VM rollout discipline, and
natural canary open-order evidence on the live compose deployment.

For this plan, `all three markets` means the three supported execution routes:

- `polymarket_myriad`
- `polymarket_predict`
- `predict_myriad`

The goal is not complete until all three routes have evidence-backed canary
open-order capability without false balance alerts, reconciliation drift, or
health/log instability.

## What the code already proves

### Runtime wiring

- `src/arbitrage_engine/main.py` already wires Predict.fun into startup,
  route selection, reconciliation clients, and the dedicated
  `predict_myriad` execution router.
- `src/arbitrage_engine/position_manager.py` already knows how to supervise
  exits for both `polymarket_predict` and `predict_myriad`.
- `src/arbitrage_engine/database.py`, `src/arbitrage_engine/reconciliation.py`,
  and `src/arbitrage_engine/market_mapping.py` already persist and audit
  Predict.fun route state inside the same production model used by the other
  venues.

### Connector/runtime behavior

- `src/arbitrage_engine/connectors/predict_fun.py` already supports:
  - authenticated REST discovery/orderbook access
  - signed order submission through `predict-sdk`
  - order status polling and cancel via current API envelopes
  - direct collateral balance reads through the configured
    `predict_fun.balance_function`
  - latest-event market-data age tracking
  - REST-first books with optional RPC fallback
- `src/arbitrage_engine/predict_fun_discovery.py` already resolves scan-all
  and explicit-market modes against the authenticated `GET /v1/markets`
  catalog.

### Operator proof tooling already present

- `scripts/predict_fun_balance_and_order_preview.py`
  - derived wallet
  - collateral token
  - raw `balanceOf`
  - decimals-scaled balance
  - connector-visible balance
  - app runtime balance state and blockers
  - optional signed order preview without submit
- `scripts/live_balance_and_order_readiness.py`
  - unified direct-balance versus connector/effective-balance parity
  - verified-mapping coverage
  - `/health/live`, `/health/ready`, `/metrics`
  - final canary go/no-go verdict
- `ops/PRODUCTION_RUNBOOK.md`
  - already stages Predict.fun rollout as
    `polymarket_myriad -> polymarket_predict -> predict_myriad`

### Existing tests already covering core Predict.fun behavior

- `tests/test_predict_fun.py`
  - wrapped REST payload parsing
  - signed-order payload construction
  - `balance_function` usage
  - open-order parsing
  - latest-event age semantics
- `tests/test_live_schema_contracts.py`
  - live market payload checks
  - live orderbook/orders/trades contract checks
- shared route/config/execution coverage in:
  - `tests/test_config.py`
  - `tests/test_execution.py`
  - `tests/test_market_mapping.py`
  - `tests/test_database_integration.py`

## Remaining production gaps

Do not call Predict.fun production-closeout done yet. These are still open:

- no live VM proof that Predict.fun direct balances, connector balances, and
  app-effective balances stay aligned under the actual compose runtime
- no evidence-backed natural canary open on `polymarket_predict`
- no evidence-backed natural canary open on `predict_myriad`
- no final proof that `LOW VENUE BALANCE` stays silent when Predict.fun direct
  collateral and app-effective balance are both above the configured minimum
- no live proof that enabled Predict.fun routes keep:
  - `/health/live=200`
  - `/health/ready=200`
  - `missing_routes=[]`
  - `arbitrage_ready=1`
  - `arbitrage_risk_paused=0`
  during a full canary observation window

## Plan of record

### Phase 1: local code and contract audit

- Re-run the local verification bundle before any VM action:
  - `pytest -q`
  - focused Predict.fun/shared execution bundle
  - `mypy src tests`
  - `ruff check src tests .github`
  - `python -m compileall -q src tests`
- Fail the phase if Predict.fun contract tests, balance parsing, or shared
  router tests regress.

### Phase 2: Predict.fun balance-proof gate

- Use `scripts/predict_fun_balance_and_order_preview.py` from the real runtime
  config that the compose container uses.
- Capture, in one evidence set:
  - derived wallet address
  - collateral token address
  - configured `balance_function`
  - raw `balanceOf`
  - decimals
  - scaled direct balance
  - connector-visible `get_cash_balance()`
  - runtime `balance_cache`, `optimistic_debits`, `capital_reservations`
  - resulting effective balance
- Treat any of these as a hard blocker before canary:
  - wrong wallet/key/account
  - wrong collateral token
  - connector/direct mismatch
  - direct/effective mismatch explained by stale reservations or unresolved
    intents
  - reconciliation failures or risk pause

### Phase 3: unified readiness gate

- Run `scripts/live_balance_and_order_readiness.py --config config.production.json`
  on the active compose VM checkout.
- Require all of the following before real orders:
  - `polymarket_myriad` remains healthy as the control route
  - enabled Predict.fun route has at least one `VERIFIED` mapping
  - direct Predict.fun balance is above `min_venue_balance_usd`
  - connector-visible Predict.fun balance is above `min_venue_balance_usd`
  - app-effective Predict.fun balance is above `min_venue_balance_usd`
  - `/health/live=200`
  - `/health/ready=200`
  - `arbitrage_ready=1`
  - `arbitrage_risk_paused=0`
  - no unresolved order intents, unresolved redemptions, or reconciliation
    failures

### Phase 4: rollout route `polymarket_predict`

- Enable only:
  - `polymarket_myriad=true`
  - `polymarket_predict=true`
  - `predict_myriad=false`
- Keep canary limits unchanged:
  - `execution_mode=canary`
  - `position_size_usd=50`
  - `$25` per leg
  - `max_open_positions=1`
  - `max_daily_loss_usd=10`
- On the active VM:
  - run `mappings review --operator <name>`
  - approve only safe candidates for enabled routes
  - deploy via `./ops/deploy_compose.sh`
  - run `DURATION_SECONDS=900 ./ops/shadow_smoke.sh`
  - extend to `60` minutes maximum only if no natural opportunity appears
- Success for this phase means:
  - at least one natural canary entry on `polymarket_predict`
  - preceding `preflight_liquidity_analysis`
  - no `LOW VENUE BALANCE` false alert
  - no `UNKNOWN` intent
  - no reconciliation drift
  - no `ERROR`, `CRITICAL`, `Traceback`, repeated `balance_cache_refresh_failed`,
    or repeated `market_route_evaluation_failed`

### Phase 5: rollout route `predict_myriad`

- Only start after `polymarket_predict` has clean evidence.
- Enable:
  - `polymarket_myriad=true`
  - `polymarket_predict=true`
  - `predict_myriad=true`
- Repeat the same gate sequence:
  - mappings review
  - safe approvals only
  - unified readiness proof
  - compose deploy
  - 15-minute canary window, extend to 60 minutes maximum only for lack of
    opportunity
- Success for this phase means:
  - at least one natural canary entry on `predict_myriad`
  - preceding `preflight_liquidity_analysis`
  - stable Myriad hedge token selection
  - no false low-balance alerts
  - no unresolved intent, reconciliation drift, or readiness regression

## Acceptance gates

This goal is done only when all of the following are true:

- `polymarket_myriad` has a clean control-route canary baseline
- `polymarket_predict` has a natural canary open-order proof
- `predict_myriad` has a natural canary open-order proof
- all enabled routes report `/health/live=200`
- all enabled routes report `/health/ready=200`
- `missing_routes=[]` for the enabled route set
- `arbitrage_ready=1`
- `arbitrage_risk_paused=0`
- no false `LOW VENUE BALANCE` alerts while direct and effective balances stay
  above the configured minimum
- no error-class log noise, reconnect storm, or unresolved reconciliation drift

If a route stays healthy but no natural opportunity appears for 60 minutes, mark
that route `unexercised` and keep the goal open. Do not force a trade and do
not close the goal on synthetic evidence.

## Final done definition

Predict.fun reaches the same production/canary level as the control route only
when the repo and VM together provide repeatable evidence that:

- all three supported routes can run in staged canary mode
- Predict.fun balance visibility is factually correct
- every real entry is protected by the existing liquidity and mapping gates
- the live compose runtime stays healthy throughout the observation windows

Until then, the correct outcome is a blocker report tied to the exact failing
phase, not a claim of rollout completion.
