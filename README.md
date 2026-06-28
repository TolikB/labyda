# Binary Prediction Arbitrage Engine

Async Python engine for binary arbitrage between Polymarket, Predict.fun, and Myriad Markets. The active strategy buys opposite outcomes for the same event across every supported pair: `Polymarket ↔ Predict.fun`, `Polymarket ↔ Myriad`, and `Predict.fun ↔ Myriad`.

Default mode is safe paper trading:

```json
{
  "execution_mode": "paper",
  "position_size_usd": 100.0,
  "min_entry_spread_pct": 0.08,
  "signal_alert_cooldown_seconds": 900
}
```

`execution_mode` supports `paper`, `shadow`, `canary`, and `live`. `canary` and
`live` require PostgreSQL plus `LIVE_TRADING_CONFIRM=YES`. The legacy `isTest`
and `shadow_mode` fields remain accepted for one compatibility release.

## Production control plane

The Windows VM deployment and final acceptance procedure is documented in
[`ops/PRODUCTION_RUNBOOK.md`](ops/PRODUCTION_RUNBOOK.md).

Canary/live execution is fail-closed:

- PostgreSQL is the durable source of truth for mappings, intents, venue orders,
  fills, positions, balances, risk state, reconciliation runs, and audit events.
- Every submitted leg gets an immutable UUIDv7 order intent before the venue
  request. An ambiguous submission or cancel outcome becomes `UNKNOWN` and
  blocks further risk until reconciliation.
- Only `VERIFIED` route mappings with canonical rules metadata are tradable.
  Fuzzy matches and unknown categories remain discovery candidates only.
- Startup reconciliation and the PostgreSQL advisory trader lock must succeed
  before order submission. Reconciliation runs every 5 seconds for orders/fills
  and every 30 seconds for balances/positions by default.
- Global risk pause cancels tracked orders, runs reconciliation, and can only be
  cleared by an explicit operator command. Same-day resume preserves accrued
  loss and is rejected while the daily-loss limit remains exceeded.

Administrative commands:

```bash
arbitrage-admin --config config.production.json db migrate
arbitrage-admin --config config.production.json discovery audit
arbitrage-admin --config config.production.json production verify --backup-dir /var/backups/offsite
arbitrage-admin --config config.production.json mappings list
arbitrage-admin --config config.production.json mappings review
arbitrage-admin --config config.production.json mappings approve-safe-candidates --operator NAME
arbitrage-admin --config config.production.json mappings approve MAPPING_ID --operator NAME
arbitrage-admin --config config.production.json mappings reject MAPPING_ID --operator NAME
arbitrage-admin --config config.production.json reconcile
arbitrage-admin --config config.production.json risk status
arbitrage-admin --config config.production.json risk pause --reason "operator emergency stop"
arbitrage-admin --config config.production.json risk resume
arbitrage-admin --config config.production.json orders cancel-all --confirm YES
arbitrage-admin --config config.production.json state import-json --path data/open_positions.json
```

Legacy JSON state is never imported automatically. A non-empty legacy ledger
blocks canary/live startup until `state import-json` is run and the old file is
archived by the operator.

`mappings review --operator NAME` groups candidate, verified, stale, and
rejected pairs by canonical market and route coverage. It also emits
`approval_candidates` with ready-to-run `mappings approve` commands using the
current `--config` path. Use it before canary to confirm that each enabled
route has at least one `VERIFIED` mapping and to identify the remaining
candidate approvals. `mappings approve-safe-candidates --operator NAME` applies
only the `single_clean_candidate_for_enabled_route` approvals from that report;
omit `--confirm YES` to preview without changing the database.

The service exposes `/health/live`, `/health/ready`, and `/metrics` on port
`9108`. Readiness is false for a risk pause, failed reconciliation, unavailable
database, invalid/stale market data, or incomplete discovery.

## Docker Compose deployment

Use a Linux host and keep the checkout and database volumes outside OneDrive.
Create an ignored `.env.production`, `config.production.json`, and external
Alertmanager configuration based on `ops/alertmanager.example.yml`, then run:

```bash
docker compose build
docker compose run --rm migrate
ALERTMANAGER_CONFIG_FILE=/etc/arbitrage/alertmanager.yml docker compose up -d
curl --fail http://127.0.0.1:9108/health/ready
```

For an existing Compose deployment that already runs from a git checkout, use:

```bash
./ops/deploy_compose.sh
```

That path fast-forwards `origin/master`, runs Alembic, rebuilds `bot`, and
waits for `/health/ready`. Keep deployment-only files such as
`.env.production`, `config.production.json`, and environment-specific
Alertmanager config ignored and local to that checkout.

The Compose stack pins Python 3.12, PostgreSQL 16, Prometheus, Alertmanager,
node-exporter, and six-hour PostgreSQL backups. Copy backups off the VM and run
the restore drill in `ops/POSTGRES_BACKUP_RESTORE.md`. Use trading keys without
withdrawal permission. Do not place private keys or tokens in the repository;
use the protected external env file or Docker secrets in the target environment.

Initial canary limits are `$5` per leg, one open position, and `$10` daily loss.
Enable routes sequentially in this order: Polymarket–Myriad,
Polymarket–Predict.fun, Predict.fun–Myriad. Any `UNKNOWN` intent, residual
exposure, or settlement mismatch requires returning to `shadow`.

## Pure systemd deployment

The production layout is `/opt/arbitrage` for immutable releases,
`/etc/arbitrage` for the `0600` environment/config files, and
`/var/lib/arbitrage` for runtime state. Install the hardened unit from
`ops/systemd/arbitrage-engine.service`, then deploy with:

```bash
sudo /opt/arbitrage/repo/ops/deploy_systemd.sh
```

The deployment creates a release from `origin/master`, installs only hashed
Python 3.12 dependencies, takes a PostgreSQL dump when `DATABASE_URL` is
available, runs forward migrations, restarts the unit, and rolls the application
symlink back if readiness does not become healthy. Database migrations remain
forward-only during application rollback.

Set `scan_all=true` to build the candidate catalog from every valid Predict.fun market returned by the API. An empty `markets` array, an empty market symbol, or `symbol: "*"` also enables this mode. In scan-all mode `markets` is not used as a text filter; Polymarket and Myriad discovery then resolve matching markets from the full catalog. Set `scan_all=false` with explicit market symbols to use the filtered list.

When enabled, Predict.fun discovery uses the authenticated Mainnet endpoint `GET /v1/markets`; the deprecated unauthenticated `/markets` fallback is not used.

The repository includes an opt-in live schema contract suite in
`tests/test_live_schema_contracts.py`. Local runs require
`ARB_RUN_LIVE_SCHEMA_CONTRACTS=1`; GitHub Actions runs the same suite nightly in
`.github/workflows/live-schema-contracts.yml`.

Predict.fun is optional. Set `predict_fun.enabled=false`, or leave `PREDICT_FUN_API_KEY` empty, to run the Polymarket/Myriad-only route. In that mode Predict.fun discovery, clients, balance checks, execution routes, and position-manager routes are not created. Myriad must be enabled and configured so at least one hedge venue remains active.

## Core Rule

Entry is allowed only when:

```text
P_first_venue + P_second_venue + slippage + fees < 1.0 - min_net_spread
```

With the example `min_entry_spread_pct=0.08`, entry requires spread strictly above `8%`. Any signal with combined cost at or above `$0.92` per `$1.00` payout is rejected.

## Layout

- `src/arbitrage_engine/quant.py` - binary spread, orderbook/AMM fills, slippage cap, sizing.
- `src/arbitrage_engine/execution.py` - dry-run and production two-leg router.
- `src/arbitrage_engine/position_manager.py` - open position supervisor, exit checks, partial-close retries, unwind retries.
- `src/arbitrage_engine/connectors/polymarket.py` - Polymarket CLOB SDK + WebSocket orderbook.
- `src/arbitrage_engine/connectors/predict_fun.py` - Predict.fun API boundary.
- `src/arbitrage_engine/market_discovery.py` - Polymarket Gamma resolver.
- `src/arbitrage_engine/matcher.py` - semantic matcher with 30-minute expiry hard-stop.
- `src/arbitrage_engine/database.py` - PostgreSQL production repository and durable execution state.
- `src/arbitrage_engine/positions.py` - legacy JSON/paper ledger serialization.
- `src/arbitrage_engine/reconciliation.py` - startup and continuous venue reconciliation.
- `src/arbitrage_engine/observability.py` - health and Prometheus endpoints.
- `src/arbitrage_engine/myriad_discovery.py` - Myriad market resolver.
- `src/arbitrage_engine/connectors/myriad.py` - Myriad EIP-712 CLOB connector.

## Run

Install dependencies, copy `config.example.json` to `config.json`, then fill `.env`.

```powershell
python -m pip install -e ".[dev]"
copy config.example.json config.json
copy .env.example .env
```

```powershell
python -m arbitrage_engine.main --config config.json --once
python -m arbitrage_engine.main --config config.json
python -m arbitrage_engine.main --config config.json --resume-risk-only
```

Required live secrets:

- `POLYMARKET_PRIVATE_KEY`
- `POLYGON_RPC_URL` for payout checks, redemption receipts, and POL gas validation
- `PREDICT_FUN_PRIVATE_KEY`
- `PREDICT_FUN_API_KEY` for Predict.fun mainnet REST order submission
- `MYRIAD_API_KEY` (optional; raises the public API rate limit)
- `MYRIAD_PRIVATE_KEY`
- Optional `TELEGRAM_BOT_TOKEN` / `TELEGRAM_CHAT_ID` for notifications

`config.example.json` uses public defaults for Polymarket CLOB, Predict.fun mainnet REST, Myriad API, and BNB RPC. Override `predict_fun.rpc_urls`, `predict_fun.api_base_url`, `myriad_markets.rpc_urls`, `myriad_markets.api_url`, or `web3_networks.bnb.rpc_urls` in `config.json` if you use private infrastructure. The legacy singular `rpc_url` key is still accepted; `rpc_urls` enables failover across multiple nodes.

Resolved positions use restart-safe Conditional Tokens redemption. A unique `redemption_intents` row is committed before
broadcast; submitted or unknown transactions are reconciled by receipt and are never blindly retried. Both venue
redemptions must reach `CONFIRMED` before the active position is removed. Missing condition/collateral metadata,
conflicting payout vectors, reverted transactions, or an unresolved receipt open the durable circuit breaker and require
manual review.

Polymarket metadata is resolved from an immutable in-memory snapshot when
`polymarket_token_id` is empty. Scan-all discovery refreshes all enabled venue
catalogs in the background and atomically publishes the active market set. API
failures preserve the previous snapshot for no more than 15 minutes; stale data
then clears the entry set while position exits and reconciliation continue.
Canary/live entries remain blocked until every enabled route has a verified
mapping. Predict.fun metadata is resolved from the authenticated markets API and
Myriad metadata from `/markets`. Matchers require compatible expiry windows and
strict title/outcome semantics.

Predict.fun execution uses `predict-sdk`: the connector builds a marketable SDK order, signs it locally as EIP-712 with `PREDICT_FUN_PRIVATE_KEY`, then submits the signed order to the Predict.fun REST API. The private key is never sent to the API. Balance checks use the Predict.fun USDT collateral address from the SDK unless `predict_fun.collateral_token_address` is explicitly set.

Order creation uses the current `{data: ...}` API envelope with fill-or-kill market orders. Status is polled by the returned order hash, while cancellation uses the returned order id through `POST /v1/orders/remove`. Market-specific `feeRateBps` discovered from Predict.fun is used for signing and profitability calculations; `predict_fun.fee_rate_bps` is the fallback for explicitly configured markets.

Myriad execution is a BNB Chain CLOB flow: the connector builds a Myriad order, signs the EIP-712 payload locally with `MYRIAD_PRIVATE_KEY`, and submits `{ order, signature, network_id, time_in_force }` to the Myriad API. `MYRIAD_API_KEY` is optional and increases rate limits. Arbitrage orders use FAK so unfilled quantity does not rest in the book. Prices are quantized down to Myriad's 0.01 tick grid. The configured collateral token balance is checked on-chain through `myriad_markets.rpc_url`.

Predict.fun and Myriad are treated as hybrid CLOB venues, not AMMs. Order placement and cancellation are off-chain REST calls with locally signed EIP-712 orders; balance and collateral operations are on-chain through BNB Chain RPC. Myriad order books are WebSocket-first with one semaphore-limited REST bootstrap per market. `myriad_markets.order_book_ttl_ms` defaults to 300 ms and `websocket_stale_after_ms` defaults to 1500 ms.

If Predict.fun REST orderbook reads fail and `predict_fun.market_abi_path` is configured, the Predict.fun connector falls back to direct RPC reserve reads. Discovery is also optional when token ids are provided explicitly in `config.json`; in that mode stale discovery endpoints do not block startup.

Financial domain values (`ExecutionReport`, `PositionPlan`, `OpenPosition`, fees, exposure and realized PnL) use
`Decimal` and serialize as strings. `float` is limited to orderbook/market-data adapters and converted once at the domain
boundary. PostgreSQL stores all monetary values as `Numeric(38,18)`.

Decimals are handled explicitly:

- Polymarket USDC collateral uses 6 decimals;
- BNB collateral balances read `decimals()` dynamically from the ERC-20 contract before scaling;
- order `amount` uses 18 decimals;
- order `price` uses 18 decimals;
- large integer order fields are serialized as strings in REST payloads where the API expects JSON.

## Auto Close

When enabled, auto-close compares the combined exit bids of both binary legs. A position is closed when the remaining market spread is below `auto_close.exit_spread_pct`, which defaults to `1.5%`. In `isTest=true` or `shadow_mode=true`, it only sends a throttled Telegram report. Persisted positions are never deleted without confirmed live exit fills.

Open positions are checked by `PositionManager`, separate from new signal scanning. It walks the persisted ledger each cycle, selects the correct venue route for each position, retries pending unwind/partial exits, and closes positions when the exit rule is met.

In production, close handling is leg-aware. If one exit leg fills and the other does not, PostgreSQL marks only the filled leg as closed and retries only the remaining leg on later cycles. A full close notification is sent only after both legs are confirmed closed.

Order fill polling returns an `ExecutionReport` containing `requested_amount`, `amount_filled`, `remaining_amount`, and status. If the second entry leg fills partially, the matched quantity remains as the hedged position and emergency unwind sells only the unmatched first-leg delta.

## Notifications

Signal notifications are throttled per pair by `signal_alert_cooldown_seconds` and default to one alert per 15 minutes. Open and close notifications are separate:

- signal detected: spread currently passes filters;
- position opened: both entry legs filled, with approximate expected profit;
- position closed: both exit legs filled, with realized profit based on confirmed exit prices.

Signal notifications include clickable links for the two venues in the active route. URLs are captured during startup discovery or read from explicit `*_url` market configuration; formatting a signal performs no network requests and does not delay order submission.

Set `telegram.log_raw_signal_books=true` only for short diagnostic windows. It records both triggering books and venue payloads; the production default is `false` to avoid oversized logs.

## Timeouts And Recovery

`polymarket_fill_timeout_ms` defaults to 500 ms. `predict_fun_fill_timeout_ms` and `myriad_fill_timeout_ms` default to 4000 ms for BNB Chain backed CLOB execution. Config validation allows Polymarket down to 300 ms and BNB-backed venues down to 3600 ms. Limit prices still protect against fills worse than the submitted price.

If the second entry leg fails after the first leg is already filled, the bot attempts an automatic first-leg unwind using the current best bid from the live order book. If immediate unwind does not fill, the position is saved as `unwind_pending` and retried automatically on later cycles.

Before either live entry leg is submitted, separate UUIDv7 `OrderIntent` rows are committed to PostgreSQL. If the process restarts with an unresolved intent, global risk starts paused and requires operator reconciliation before `risk resume` can clear the stop. Non-filled orders are cancelled and polled again for `cancel_reconcile_timeout_ms`; an unconfirmed result remains `UNKNOWN` and cannot be resubmitted.

## Liquidity Guard

Position sizing is controlled by `position_size_usd`. The bot splits that target across the two legs, walks the full order book, and uses weighted average fill price for spread calculations. If the full target size cannot be filled, or price impact exceeds `1.5%`, the signal is rejected instead of shrinking the order size.

Before a production entry, the router checks available balance for both venues and subtracts capital already reserved by open positions in the local ledger. Multiple positions can be opened across markets/routes as long as the venue balances cover the next position.

## Global Risk Stop

All execution routes share one durable risk controller. Capital is reserved atomically before either leg is submitted; entry legs are then submitted concurrently. Reaching `max_daily_loss_usd` or `max_consecutive_api_errors` pauses every route, cancels tracked active orders, clears pending reservations, and requires the explicit `--resume-risk-only` operator command before trading can continue. Guard and transaction-timeout events include market metrics in Telegram notifications. Execution latency is emitted as structured `execution_pipeline_latency` records through a non-blocking logging queue.

`max_concurrent_market_evaluations` bounds scan-all work. Polymarket, Predict.fun, and Myriad bootstrap HTTP traffic is bounded and all clients reuse long-lived `aiohttp` sessions. Polymarket discovery uses sequential 1,000-market CLOB pages plus Gamma ID batches of up to 50, eliminating individual Gamma lookups. Set `shadow_mode=true` to exercise discovery, books, matching, sizing, and alerts with order submission and production balance gates disabled.

Every parsed order book carries its venue update timestamp when available, otherwise its local receipt timestamp. Signal evaluation and production preflight reject either leg older than `max_orderbook_age_seconds`; configuration validation enforces the production-safe range `1.5`–`2.0` seconds (default `2.0`). Readiness and reconnect control use stream liveness instead: only venues with active subscription targets are evaluated, quiet markets can keep a passively cached `VALID` book until `max_orderbook_age_seconds`, and a venue is considered stale only after `websocket_stale_after_seconds` without a real market-data event. Socket PONG/heartbeat frames never refresh either timestamp. Streams without any actual market-data update for `websocket_stale_after_seconds` are reconnected and reported to Telegram.

`arbitrage_market_data_age_seconds` remains the “latest real venue event age” metric for observability. Prometheus stale-feed alerts should gate on venues with `arbitrage_market_data_active_targets > 0` and align their silence threshold with `websocket_stale_after_seconds`, rather than alerting on the per-book execution freshness ceiling.

`max_production_price_impact` is the global safety ceiling applied to every venue-specific slippage setting. Looser venue settings are accepted but produce a startup warning and are capped explicitly. Persisted entry prices remain raw exchange fill prices; entry and exit fees are applied exactly once when profitability and realized PnL are calculated.
