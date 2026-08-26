# Binary Prediction Arbitrage Engine

Async Python engine for binary arbitrage between Polymarket, Predict.fun, SX Bet, and Myriad Markets. The runtime supports two route families:

- Predict.fun family: `Polymarket ↔ Predict.fun`, `Polymarket ↔ Myriad`, and `Predict.fun ↔ Myriad`
- SX Bet family: `Polymarket ↔ SX Bet`, `Polymarket ↔ Myriad`, and `SX Bet ↔ Myriad`

The process can host both second-leg families at once, but production rollout should still stay staged and evidence-backed per route family until each canary path has its own clean proof window.

Default mode is safe paper trading:

```json
{
  "execution_mode": "paper",
  "position_size_usd": 100.0,
  "min_entry_spread_pct": 0.05,
  "signal_alert_cooldown_seconds": 900
}
```

`execution_mode` supports `paper`, `shadow`, `canary`, and `live`. `canary` and
`live` require PostgreSQL plus `LIVE_TRADING_CONFIRM=YES`. The legacy `isTest`
and `shadow_mode` fields remain accepted for one compatibility release.

## Production control plane

The Contabo VPS deployment and final acceptance procedure is documented in
[`ops/PRODUCTION_RUNBOOK.md`](ops/PRODUCTION_RUNBOOK.md). The dedicated
Predict.fun staged closeout plan is tracked in
[`ops/PREDICT_FUN_PRODUCTION_PLAN.md`](ops/PREDICT_FUN_PRODUCTION_PLAN.md).
Current cost scope is locked to the existing single Compose VM footprint. Do
not add disks, backup services, load balancers, or larger VM capacity without
separate approval.

Canary/live execution is fail-closed:

- PostgreSQL is the durable source of truth for mappings, intents, venue orders,
  fills, positions, balances, risk state, reconciliation runs, and audit events.
- Every submitted leg gets an immutable UUIDv7 order intent before the venue
  request. An ambiguous submission or cancel outcome becomes `UNKNOWN` and
  blocks further risk until reconciliation.
- Only `VERIFIED` route mappings with canonical rules metadata are tradable.
  Fuzzy matches and unknown categories remain discovery candidates only.
- Funded `scan_all` requires `market_horizon_filter_enabled=true`. The initial
  launch universe is limited to sports and crypto settling within 200 hours;
  longer futures are rejected before mapping and audit.
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
arbitrage-admin --config config.production.json mappings approve-safe-candidates --operator NAME --route polymarket_myriad
arbitrage-admin --config config.production.json mappings approve MAPPING_ID --operator NAME
arbitrage-admin --config config.production.json mappings reject MAPPING_ID --operator NAME
arbitrage-admin --config config.production.json reconcile
arbitrage-admin --config config.production.json risk status
arbitrage-admin --config config.production.json risk pause --reason "operator emergency stop"
arbitrage-admin --config config.production.json risk resume
arbitrage-admin --config config.production.json orders cancel-all --confirm YES
arbitrage-admin --config config.production.json orders review-unresolved --older-than-minutes 60
arbitrage-admin --config config.production.json orders retire-safe-unresolved --older-than-minutes 60 --confirm YES
arbitrage-admin --config config.production.json state import-json --path data/open_positions.json
python scripts/polymarket_deposit_wallet_probe.py --config config.production.json
python scripts/polymarket_deposit_wallet_probe.py --config config.production.json --wallet-address 0x... --relayer-api-key 019f... --relayer-api-address 0x4816...
python scripts/polymarket_safe_order_preview.py --config config.production.json --condition-id 0x... --token-id ... --side BUY --price 0.03 --size 5
python scripts/predict_fun_approvals.py --config config.production.quote_arb.json
python scripts/predict_fun_approvals.py --config config.production.quote_arb.json --scope trade --yield-bearing both --apply
python scripts/predict_fun_balance_and_order_preview.py --config config.production.json
python scripts/predict_fun_balance_and_order_preview.py --config config.production.json --market-id ... --token-id ... --side BUY --price 0.40 --size 5
python scripts/sx_bet_probe.py --api-version v3
python scripts/sx_bet_balance_and_order_preview.py --config config.production.json
python scripts/sx_bet_balance_and_order_preview.py --config config.production.json --market-hash 0x... --token-id ... --outcome-side YES --order-side BUY --price 0.40 --size 5
python scripts/sx_polymarket_match_probe.py --config config.production.json --route polymarket --contains "World Cup" --limit 12 --require-match
python scripts/sx_polymarket_match_probe.py --config config.production.json --route myriad --contains "World Cup" --limit 12
python scripts/myriad_balance_and_order_preview.py --config config.production.json
python scripts/myriad_balance_and_order_preview.py --config config.production.json --market-id 1335 --side YES --order-side BUY --price 0.40 --size 5
python scripts/live_balance_and_order_readiness.py --config config.production.quote_arb.json
python scripts/live_balance_and_order_readiness.py --config config.production.quote_arb.json --all-markets
python scripts/live_balance_and_order_readiness.py --config config.production.quote_arb.json --polymarket-condition-id 0x... --polymarket-token-id ... --polymarket-side BUY --polymarket-price 0.03 --polymarket-size 5 --predict-market-id ... --predict-token-id ... --predict-order-side BUY --predict-price 0.40 --predict-size 5 --sx-market-hash 0x... --sx-token-id ... --sx-outcome-side YES --sx-order-side BUY --sx-price 0.40 --sx-size 5 --myriad-market-id 1335 --myriad-outcome-side YES --myriad-order-side BUY --myriad-price 0.40 --myriad-size 5
CI_VERIFIED_COMMIT_SHA=<verified-sha> CALIBRATION_REQUIRE_CONFIGURED_RESERVE=NO ./ops/production_closeout.sh
./ops/operator_python.sh scripts/live_canary_window.py --config config.production.clob_hft.json --duration-seconds 7200 --poll-seconds 15 --database-poll-seconds 60 --database-timeout-seconds 45 --stop-on timeout --required-route polymarket_sx --artifact-dir canary-artifacts/clob_hft/polymarket_sx --compose-service bot-clob-hft --compose-service bot-quote-arb
./ops/operator_python.sh scripts/live_canary_window.py --config config.production.quote_arb.json --duration-seconds 7200 --poll-seconds 15 --database-poll-seconds 60 --database-timeout-seconds 45 --stop-on timeout --required-route polymarket_predict --artifact-dir canary-artifacts/quote_arb/polymarket_predict --compose-service bot-quote-arb --compose-service bot-clob-hft
./ops/operator_python.sh scripts/live_canary_window.py --config config.production.quote_arb.json --duration-seconds 7200 --poll-seconds 15 --database-poll-seconds 60 --database-timeout-seconds 45 --stop-on timeout --required-route polymarket_myriad --artifact-dir canary-artifacts/quote_arb/polymarket_myriad --compose-service bot-quote-arb --compose-service bot-clob-hft
arbitrage-admin --config config.production.clob_hft.json discovery overlap
arbitrage-admin --config config.production.quote_arb.json production audit --all-markets --defer-backup-gates
arbitrage-admin --config config.production.quote_arb.json production audit --all-markets --defer-backup-gates --require-live-order-evidence --live-window-report canary-artifacts/quote_arb/<timestamp>/report.json
./ops/production_closeout.sh
POLYMARKET_WALLET_CREATE_CONFIRM=YES python scripts/polymarket_deposit_wallet_create.py --relayer-api-key 019f... --relayer-api-address 0x4816... --confirm-wallet-create
POLYMARKET_SAFE_TRANSFER_CONFIRM=YES python scripts/polymarket_safe_to_deposit_transfer.py --relayer-api-key 019f... --relayer-api-address 0x4816... --amount-usd 50 --confirm-safe-transfer
POLYMARKET_DEPOSIT_APPROVE_CONFIRM=YES python scripts/polymarket_deposit_wallet_approve.py --relayer-api-key 019f... --relayer-api-address 0x4816... --confirm-deposit-approve
```

Legacy JSON state is never imported automatically. A non-empty legacy ledger
blocks canary/live startup until `state import-json` is run and the old file is
archived by the operator.

`mappings review --operator NAME` groups candidate, verified, stale, and
rejected pairs by canonical market and route coverage. It also emits
`approval_candidates` with ready-to-run `mappings approve` commands using the
current `--config` path. Use it before canary to confirm that each enabled
route has at least one `VERIFIED` mapping and to identify the remaining
candidate approvals. `mappings approve-safe-candidates --operator NAME --route ROUTE` applies
only single-candidate mappings whose discovery provenance is `exact_id` and
whose category/cutoff remain inside the configured production launch horizon;
title, semantic, and legacy candidates without persisted provenance always require
individual operator review and `mappings approve MAPPING_ID`;
omit `--confirm YES` to preview without changing the database.
Use repeatable `--category crypto|sports` and `--mapping-id ID` filters for a
scoped canary approval. Requested IDs are revalidated and the command fails
before writing if any selected mapping is not currently safe.
Omit `--route` only when the operator intentionally wants to process every
enabled route in the selected config. Football, soccer, and esports category
labels are normalized into the sports universe and use its configured horizon.
For Myriad instruments, the stored review metadata now exposes both
`market_id:YES` and `market_id:NO` token ids, so `predict_myriad` and `sx_myriad`
coverage can be audited against the actual binary hedge token that runtime uses.
The production verification path also probes Myriad books through the same
route-aware execution token (`market_id:SIDE`) rather than the bare market id,
so SX and Predict-to-Myriad checks do not fail on a malformed token selector.

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
COMPOSE_ENV_FILE=.env.production ./ops/deploy_compose.sh
```

That path fast-forwards the configured verified branch, runs Alembic, rebuilds
both bot services, and waits for both readiness endpoints. Keep deployment-only files such as
`.env.production` and environment-specific
Alertmanager config ignored and local to that checkout.

For the current live VPS rollout shape, the authoritative checkout is
`/opt/labyda_next` on Contabo host `169.58.161.34`. Treat that Compose checkout and its
`config.production.clob_hft.json` and `config.production.quote_arb.json` as the
production source of truth. Run
`COMPOSE_ENV_FILE=.env.production ./ops/deploy_compose.sh` there, then capture
one 120-minute report per enabled route with the commands in the production
runbook. The full safe flow is:

```bash
CI_VERIFIED_COMMIT_SHA=<verified-sha> ./ops/production_closeout.sh
```

The closeout wrapper discovers and safely approves only exact-ID mappings before
shadow calibration. It never edits the deployed production configs. Apply measured
route p95 reserves in a new commit and redeploy the resulting CI-verified SHA.
While risk remains paused, `scripts/shadow_openability_window.py` can latch exact-SHA,
three-sample signed technical-openability evidence across both production configs;
this evidence does not replace shadow calibration or funded route evidence.
Funded execution additionally requires `ENABLE_FUNDED_CANARY=YES` and an explicit
credential decision. Use `CREDENTIAL_ROTATION_CONFIRMED=YES` after rotation, or
`CREDENTIAL_ROTATION_RISK_ACCEPTED=YES` only when the owner accepts continued use
of previously exposed credentials. Any failed or shadow-only run restores the
durable risk pause before exit.

The observer stores `/health/live`, `/health/ready`, `/metrics`, Docker Compose
logs, unresolved intents, fills, open positions, reconciliation failures, risk
pause state, and a machine-readable `report.json` under
`canary-artifacts/<timestamp>/`.

The Compose stack pins Python 3.12, PostgreSQL 16, Prometheus, Alertmanager,
node-exporter, and six-hour PostgreSQL backups. For the current approved budget
profile, keep those backup artifacts on the same VM under the configured local
backup path; do not add a separate backup disk or paid offsite service. Run the
restore drill in `ops/POSTGRES_BACKUP_RESTORE.md`. Use trading keys without
withdrawal permission. Do not place private keys or tokens in the repository;
use the protected external env file or Docker secrets in the target environment.

Before any order-submitting rollout, run `mappings review --operator NAME` and
approve only safe candidates for the enabled route set. Canary/live startup
fails closed until at least one `VERIFIED` mapping exists for each enabled
route.

Initial canary limits are `$10` per leg (`$20` total), one open position, and
`$10` realized daily-loss breaker. Because the two runtime instances have
independent risk state, run only one funded-canary service at a time; the other
must remain risk-paused in shadow. A failed cross-venue hedge can still lose up
to the funded single-leg notional. Enable routes sequentially inside one family at a time:
Polymarket–Myriad, then either Polymarket–Predict.fun and Predict.fun–Myriad,
or Polymarket–SX Bet and SX Bet–Myriad. Any `UNKNOWN` intent, residual
exposure, or settlement mismatch requires returning to `shadow`.

Recommended stage order is:

- control route: `polymarket_myriad`
- Predict.fun family: `polymarket_predict`, then `predict_myriad`
- SX Bet family: `polymarket_sx`, then `sx_myriad`

Do not enable the next route in a family until the previous route has clean
health, clean reconciliation, and either a natural canary open-order proof or
an explicitly documented `unexercised` window caused only by lack of market
opportunity.

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

Predict.fun and SX Bet are optional. Set `predict_fun.enabled=false`, or leave
`PREDICT_FUN_API_KEY` empty, to disable the Predict.fun family. Set
`sx_bet.enabled=false`, or keep `enable_sx_bet=false`, to disable the SX family.
Enable only the venues and routes you intend to trade in the current rollout;
the validation gates now key off the active route set rather than unrelated
disabled venues.

## Core Rule

Entry is allowed only when:

```text
P_first_venue + P_second_venue + slippage + fees < 1.0 - min_net_spread
```

With the example `min_entry_spread_pct=0.05`, entry requires spread strictly above `5%`. Any signal with combined cost at or above `$0.95` per `$1.00` payout is rejected.

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
- `SX_BET_PRIVATE_KEY`
- `SX_BET_API_KEY` for enabled SX Bet V3 realtime and order endpoints
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

Predict.fun execution uses `predict-sdk`: the connector builds a marketable SDK order, signs it locally as EIP-712 with `PREDICT_FUN_PRIVATE_KEY`, then submits the signed order to the Predict.fun REST API. The private key is never sent to the API. Balance checks use the Predict.fun USDT collateral address from the SDK unless `predict_fun.collateral_token_address` is explicitly set. If the funded venue balance lives on a Predict Account or deposit address rather than the signer EOA, set `predict_fun.account_address` or `PREDICT_FUN_ACCOUNT_ADDRESS`; JWT auth still uses the signer wallet, while balance checks and signed orders target the funded Predict Account.

Before the first funded Predict.fun order, confirm trading approvals on-chain for the funded EOA or Predict Account. `scripts/predict_fun_approvals.py` previews the required ERC-20 and ERC-1155 approval steps for standard and neg-risk markets, and only submits them when `PREDICT_FUN_APPROVE_CONFIRM=YES` is set together with `--apply`.

Order creation uses the current `{data: ...}` API envelope with fill-or-kill market orders. Status is polled by the returned order hash, while cancellation uses the returned order id through `POST /v1/orders/remove`. Market-specific `feeRateBps` discovered from Predict.fun is used for signing and profitability calculations; `predict_fun.fee_rate_bps` is the fallback for explicitly configured markets.

Myriad execution is a BNB Chain CLOB flow: the connector builds a Myriad order, signs the EIP-712 payload locally with `MYRIAD_PRIVATE_KEY`, and submits `{ order, signature, network_id, time_in_force }` to the Myriad API. `MYRIAD_API_KEY` is optional and increases rate limits. Arbitrage orders use FAK so unfilled quantity does not rest in the book. Prices are quantized down to Myriad's 0.01 tick grid. The configured collateral token balance is checked on-chain through `myriad_markets.rpc_url`.

Predict.fun and Myriad are treated as hybrid CLOB venues, not AMMs. Order placement and cancellation are off-chain REST calls with locally signed EIP-712 orders; balance and collateral operations are on-chain through BNB Chain RPC. Myriad order books are WebSocket-first with one semaphore-limited REST bootstrap per market. `myriad_markets.order_book_ttl_ms` defaults to 300 ms and `websocket_stale_after_ms` defaults to 1500 ms.

If Predict.fun REST orderbook reads fail and `predict_fun.market_abi_path` is configured, the Predict.fun connector falls back to direct RPC reserve reads. Discovery is also optional when token ids are provided explicitly in `config.json`; in that mode stale discovery endpoints do not block startup.

Financial domain values (`ExecutionReport`, `PositionPlan`, `OpenPosition`, fees, exposure and realized PnL) use
`Decimal` and serialize as strings. `float` is limited to orderbook/market-data adapters and converted once at the domain
boundary. PostgreSQL stores all monetary values as `Numeric(38,18)`.

Decimals are handled explicitly:

- Polymarket pUSD collateral uses 6 decimals and wraps USDC.e on Polygon;
- BNB collateral balances read `decimals()` dynamically from the ERC-20 contract before scaling;
- order `amount` uses 18 decimals;
- order `price` uses 18 decimals;
- large integer order fields are serialized as strings in REST payloads where the API expects JSON.

For deposit-wallet accounts on Polymarket, use `scripts/polymarket_deposit_wallet_probe.py` before any live rollout. It
checks the relayer deployment flag, on-chain pUSD balance, exchange allowances, and both CLOB balance paths
(`signature_type=2` and `signature_type=3`) so wallet-registration drift is visible before real orders are enabled. If
you also pass `--relayer-api-key` and `--relayer-api-address`, the probe lists authenticated relayer API keys and the
most recent relayer transactions for that owner without sending any transaction. If the funded wallet is the canonical
SAFE wallet and the canonical deposit wallet is still undeployed, use
`signature_type=2` with `funder=<safe_wallet>` for immediate SAFE-mode trading from the already funded wallet. Use
`scripts/polymarket_deposit_wallet_create.py` to submit a guarded `WALLET-CREATE` transaction before migrating pUSD
into the `POLY_1271` flow. After the wallet exists, use
`scripts/polymarket_safe_to_deposit_transfer.py` to move pUSD from the canonical SAFE wallet into the canonical deposit
wallet, then `scripts/polymarket_deposit_wallet_approve.py` to approve the trading contracts from the deposit wallet
before the first `signature_type=3` balance sync and live order attempt.
If you already have Polymarket L2 API credentials, set `polymarket.api_key`, `polymarket.api_secret`, and
`polymarket.api_passphrase` together so the runtime reuses them instead of attempting to create or derive keys on each
startup.
For SAFE-mode rollout from a funded API wallet, `scripts/polymarket_safe_order_preview.py` shows the exact signed order
shape (`maker`, `signer`, `signatureType`) before you allow a live `post_order`.
For Predict.fun, `scripts/predict_fun_balance_and_order_preview.py` shows the derived wallet, the active collateral token,
raw `balanceOf` data, connector-visible balance, persisted runtime `balance_cache` / `optimistic_debits` / `capital_reservations`,
the resulting app-effective balance, runtime-audit blockers, an explicit canary gate, and an optional signed-order
preview for a concrete `market_id/token_id/side/price/size` without submitting an order. Missing keys, balance-probe failures,
or metadata drift return a structured blocker report instead of crashing the script.
For Myriad, `scripts/myriad_balance_and_order_preview.py` shows the derived trader address, every configured collateral
token balance, the currently selected collateral symbol, and an optional signed-order preview before any live submit.
For SX Bet, `scripts/sx_bet_balance_and_order_preview.py` shows the derived wallet, active base token,
raw `balanceOf` data, connector-visible balance, explorer-visible balance, persisted runtime
`balance_cache` / `optimistic_debits` / `capital_reservations`, the resulting app-effective balance,
runtime-audit blockers, an explicit canary gate, and an optional signed fill preview for a concrete
`market_hash/token_id/outcome_side/order_side/price/size` without submitting a live fill. Missing keys,
balance-probe failures, or preview drift return a structured blocker report instead of aborting the script.
If you want one operator view across all four venues, `scripts/live_balance_and_order_readiness.py` reports enabled routes,
verified-mapping coverage, `/health/live`, `/health/ready`, key `/metrics` values, direct balance probes,
connector-visible balances, persisted runtime `balance_cache` / `optimistic_debits` / `capital_reservations`,
effective-balance math, SX explorer balance evidence, optional order-preview shapes, and a final
canary go/no-go report for Polymarket, Predict.fun, SX Bet, and Myriad together. Per-venue probe failures are returned as explicit
`balance_probe_error` blockers so one broken venue does not hide the rest of the evidence. Use it with `arbitrage-admin --config ... production audit`
before any canary rollout.
With `--all-markets`, the same script expands to every enabled-route market from
the real discovery pipeline and emits route-aware first/second-leg identities,
verified-route coverage, orderbook and constraint availability, preview
feasibility, and explicit blockers for non-openable markets. Reports expose
`technical_openable_count` only when executable depth, verified fees, signed
previews, VWAP, live chain cost, route edge, and minimum profit all pass without
considering operator pause. `canary_openable_count` adds runtime balance, risk,
and live-confirmation gates. Legacy `openable_count` aliases the canary value;
`economically_openable_count` remains a compatibility alias for technical.
For SX Bet contract probing and live orderbook shape checks, use `scripts/sx_bet_probe.py`.
The runtime supports explicit V2/V3 selection. V3 uses aggregated versioned books,
proxy balances, per-account payout fees, FOK taker orders, and V3 order/fill/position
reconciliation. Production config selects V3 after the official cutover, but
deployment remains fail-closed until the authenticated key, proxy, balance, fee,
preview, reconciliation, and risk checks in `ops/SX_BET_V3_CUTOVER.md` pass.
For production overlap on every enabled route family, use the split-service
configs directly:
`arbitrage-admin --config config.production.clob_hft.json discovery overlap`
and
`arbitrage-admin --config config.production.quote_arb.json discovery overlap`.
These report commands are read-only by default. Add `--persist-candidates` only
for an intentional mapping-bootstrap run before operator review.
They emit per-route discovered candidate counts, engine-safe matched counts,
post-volume-filter counts, verified/tradable counts, and unmatched sample rows
using the same discovery and mapping logic the bot relies on. Each route also
includes category-level first-leg, second-leg, and minimum-leg volume
distributions. Complementary YES/NO strategy rows for one binary market pair
are deduplicated so reported sums and percentiles are not doubled.
If runtime hygiene is blocked by durable unresolved intents from an older
restart or partial rollout, use
`arbitrage-admin --config config.production.quote_arb.json orders review-unresolved` first.
It reports safe retirement candidates only when the intent is older than the
chosen threshold and there is no venue open order, no fill evidence, and no
linked open position. Apply cleanup only with the explicit
`orders retire-safe-unresolved --confirm YES` path.
If you want the full funded-launch closeout sequence captured under one timestamped
artifact root, run `./ops/production_closeout.sh` on the authoritative Compose VM
checkout. By default it runs both services in shadow, captures 60-minute route
calibration, and executes per-service overlap, all-market readiness, and the
pre-live audit. `ENABLE_FUNDED_CANARY=YES` additionally requires exactly one
`FUNDED_CANARY_TARGET` and enables a 120-minute route-specific funded window plus
the final live-evidence-gated audit for that target. Run separate, reconciled
invocations for `quote_arb` and `clob_hft`; never fund both simultaneously.
On the Compose VM, host-side closeout tooling uses the loopback PostgreSQL port
and automatically loads `.env.production` from the authoritative checkout when
`--config config.production.clob_hft.json` or
`--config config.production.quote_arb.json` is used. The wrapper also exports
`ARBITRAGE_DATABASE_HOST_OVERRIDE=127.0.0.1`; use the same override for any direct
`arbitrage-admin` or `live_canary_window.py` host command against
`/opt/labyda_next`.

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

`polymarket_fill_timeout_ms` defaults to 500 ms. `predict_fun_fill_timeout_ms`, `sx_bet_fill_timeout_ms`, and `myriad_fill_timeout_ms` default to 4000 ms for venue-backed fill reconciliation. Config validation allows Polymarket down to 300 ms and the non-Polymarket execution venues down to 3600 ms. Limit prices still protect against fills worse than the submitted price.

If the second entry leg fails after the first leg is already filled, the bot attempts an automatic first-leg unwind using the current best bid from the live order book. If immediate unwind does not fill, the position is saved as `unwind_pending` and retried automatically on later cycles.

Before either live entry leg is submitted, separate UUIDv7 `OrderIntent` rows are committed to PostgreSQL. If the process restarts with an unresolved intent, global risk starts paused and requires operator reconciliation before `risk resume` can clear the stop. Non-filled orders are cancelled and polled again for `cancel_reconcile_timeout_ms`; an unconfirmed result remains `UNKNOWN` and cannot be resubmitted.

## Liquidity Guard

Position sizing is controlled by `position_size_usd`. The bot splits that target across the two legs, walks the full order book, and uses weighted average fill price for spread calculations. If the full target size cannot be filled, or price impact exceeds `1.5%`, the signal is rejected instead of shrinking the order size.

Before a production entry, the router checks available balance for both venues and subtracts capital already reserved by open positions in the local ledger. Multiple positions can be opened across markets/routes as long as the venue balances cover the next position.

Every order-submitting preflight now emits either `preflight_liquidity_analysis`
or `preflight_liquidity_rejected`. The payload records the route, target
notional per leg, best ask, average full-depth fill, slippage, book age, net
spread, and the exact reject reason for insufficient liquidity, stale/invalid
books, slippage-cap overflow, or spread floor failure.

## Global Risk Stop

All execution routes share one durable risk controller. Capital is reserved atomically before either leg is submitted; entry legs are then submitted concurrently. Reaching `max_daily_loss_usd` or `max_consecutive_api_errors` pauses every route, cancels tracked active orders, clears pending reservations, and requires the explicit `--resume-risk-only` operator command before trading can continue. Guard and transaction-timeout events include market metrics in Telegram notifications. Execution latency is emitted as structured `execution_pipeline_latency` records through a non-blocking logging queue.

`max_concurrent_market_evaluations` bounds the active market-data window. The engine rotates that window across the full eligible universe after `market_data_target_hold_seconds`, allowing WebSocket snapshots to warm without increasing concurrency. Polymarket, Predict.fun, and Myriad bootstrap HTTP traffic is bounded and all clients reuse long-lived `aiohttp` sessions. Polymarket discovery uses sequential 1,000-market CLOB pages plus Gamma ID batches of up to 50, eliminating individual Gamma lookups. Set `shadow_mode=true` to exercise discovery, books, matching, sizing, and alerts with order submission and production balance gates disabled.

Every parsed order book carries its venue update timestamp when available, otherwise its local receipt timestamp. Signal evaluation and production preflight reject either leg older than `max_orderbook_age_seconds`; configuration validation enforces the production-safe range `1.5`–`2.0` seconds (default `2.0`). Readiness and reconnect control use stream liveness instead: only venues with active subscription targets are evaluated, quiet markets can keep a passively cached `VALID` book until `max_orderbook_age_seconds`, and a venue is considered stale only after `websocket_stale_after_seconds` without a real market-data event. Socket PONG/heartbeat frames never refresh either timestamp. Streams without any actual market-data update for `websocket_stale_after_seconds` are reconnected and reported to Telegram.

`arbitrage_market_data_age_seconds` remains the “latest real venue event age” metric for observability. Prometheus stale-feed alerts should gate on venues with `arbitrage_market_data_active_targets > 0` and align their silence threshold with `websocket_stale_after_seconds`, rather than alerting on the per-book execution freshness ceiling.

`max_production_price_impact` is the global safety ceiling applied to every venue-specific slippage setting. Looser venue settings are accepted but produce a startup warning and are capped explicitly. Persisted entry prices remain raw exchange fill prices; entry and exit fees are applied exactly once when profitability and realized PnL are calculated.
