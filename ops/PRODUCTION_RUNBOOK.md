# Production Runbook - Docker Compose Split Services On Contabo

Authoritative runtime:

- VPS: Contabo `169.58.161.34`, SSH user `root`, port `22`
- SSH key: `C:\Users\tolik\.ssh\funding-bot-contabo-ed25519`
- authoritative checkout: `/opt/labyda_next`
- Compose project: `labyda_next`
- database: local PostgreSQL in the same Compose project
- protected co-tenant: `/opt/funding_arbitrage_paper` and all
  `funding_arbitrage_paper-*` containers; never stop, recreate, or reuse their ports
- launch configs:
  - `config.production.clob_hft.json`
  - `config.production.quote_arb.json`

The production pair universe contains all six unique routes between the four
supported venues, split without duplicates between the two services:

- `bot-clob-hft`: `predict_sx`, `polymarket_sx`, `sx_myriad`
- `bot-quote-arb`: `polymarket_predict`, `polymarket_myriad`, `predict_myriad`

The shared evaluation cap remains 16 per service, so adding routes widens the
rotating candidate universe without multiplying unbounded concurrent work.

## 1. Cost And Authorization Gate

- Do not add paid services, disks, larger VM shapes, or new backup infrastructure for the initial funded launch.
- Keep the current approved footprint fixed:
  - one shared Contabo VPS
  - one local `labyda_next` PostgreSQL volume
  - the existing VPS disk only
- Funded launch still requires explicit operator approval for live balances and live orders.

## 2. Active Deployment Shape

Docker Compose must run two bot services, not one:

- `bot-clob-hft`
  - config: `config.production.clob_hft.json`
  - observability: `http://127.0.0.1:9108`
  - runtime instance: `clob_hft`
  - enabled routes:
    - `predict_sx`
    - `polymarket_sx`
    - `sx_myriad`
- `bot-quote-arb`
  - config: `config.production.quote_arb.json`
  - observability: `http://127.0.0.1:9109`
  - runtime instance: `quote_arb`
  - enabled routes:
    - `polymarket_predict`
    - `polymarket_myriad`
    - `predict_myriad`

Prometheus must scrape both ports.

Quick checks:

```bash
curl --fail http://127.0.0.1:9108/health/live
curl --fail http://127.0.0.1:9108/health/ready
curl --fail http://127.0.0.1:9109/health/live
curl --fail http://127.0.0.1:9109/health/ready
```

## 3. Release Gate

Deploy only from the authoritative checkout:

```bash
cd /opt/labyda_next
CI_VERIFIED_COMMIT_SHA=<verified-sha> \
BRANCH=codex/production-closeout \
COMPOSE_ENV_FILE=.env.production \
DEPLOY_HEALTH_POLICY=safe_paused_shadow \
CLOB_HFT_EXECUTION_MODE=shadow \
QUOTE_ARB_EXECUTION_MODE=shadow \
LIVE_TRADING_CONFIRM=NO \
./ops/deploy_compose.sh
```

`deploy_compose.sh` must:

- require a clean tracked worktree
- fast-forward `origin/codex/production-closeout`
- run Alembic
- rebuild and start both services
- require both services to pass the selected fail-closed health policy

For pre-canary deployment, `safe_paused_shadow` requires `/health/live=200`,
`/health/ready=503`, `arbitrage_risk_paused=1`, `arbitrage_ready=0`, exact runtime
instance identity, shadow execution mode, and no readiness reason except
`risk_paused:*`. It rejects any market-data, discovery, reconciliation, or other
additional blocker. Normal funded deployments use the default `ready` policy.
The Compose container healthcheck uses `/health/live`; operational readiness is
never inferred from Docker's liveness state and remains enforced by the policy above.

Do not skip migrations after schema changes.

## 4. Runtime Config Gate

`config.production.clob_hft.json` must stay narrowed to:

```json
{
  "runtime_instance_id": "clob_hft",
  "execution_mode": "canary",
  "position_size_usd": 50.0,
  "max_order_size_usd": 50.0,
  "max_total_notional_usd": 252.0,
  "max_venue_exposure_usd": 125.0,
  "max_market_exposure_usd": 52.0,
  "min_venue_balance_usd": 125.0,
  "max_open_positions": 5,
  "max_daily_loss_usd": 10.0,
  "max_unresolved_exposure_usd": 5.0,
  "max_orders_per_minute": 10,
  "categories_to_scan": ["sports"],
  "market_horizon_filter_enabled": true,
  "max_sports_market_horizon_hours": 200,
  "max_crypto_market_horizon_hours": 200,
  "max_market_horizon_hours_by_category": {},
  "enable_sx_bet": true,
  "enable_predict_fun": true,
  "routes": {
    "predict_sx": true,
    "polymarket_sx": true,
    "sx_myriad": true
  }
}
```

`config.production.quote_arb.json` must stay narrowed to:

```json
{
  "runtime_instance_id": "quote_arb",
  "execution_mode": "canary",
  "position_size_usd": 50.0,
  "max_order_size_usd": 50.0,
  "max_total_notional_usd": 252.0,
  "max_venue_exposure_usd": 125.0,
  "max_market_exposure_usd": 52.0,
  "min_venue_balance_usd": 125.0,
  "max_open_positions": 5,
  "max_daily_loss_usd": 10.0,
  "max_unresolved_exposure_usd": 5.0,
  "max_orders_per_minute": 10,
  "categories_to_scan": ["crypto", "sports"],
  "market_horizon_filter_enabled": true,
  "max_sports_market_horizon_hours": 200,
  "max_crypto_market_horizon_hours": 200,
  "max_market_horizon_hours_by_category": {},
  "enable_predict_fun": true,
  "enable_sx_bet": false,
  "routes": {
    "polymarket_predict": true,
    "polymarket_myriad": true,
    "predict_myriad": true
  }
}
```

Any category added beyond `sports` and `crypto` must also have a positive entry in
`max_market_horizon_hours_by_category`. Canary/live validation fails closed otherwise.

Both services share one PostgreSQL, but must not share:

- trader lock
- risk pause state
- readiness state

That isolation is keyed by `runtime_instance_id`.

The `$25` leg size is conditional, not a blind stake. Immediately before submit,
each leg must have at least `$31.25` (`$25 × 1.25`) at the current best ask and
the exact signed preview must report zero price impact. Aggregate depth at worse
prices does not satisfy this gate. Stale books, any non-zero signed-preview impact,
insufficient best-level buffer, or a net edge below the configured threshold produce
`NO-TRADE` without lowering the stake or profitability floor.

## 5. Prelaunch Audit Per Service

Run the same three checks for each service config:

```bash
cd /opt/labyda_next
export ARBITRAGE_DATABASE_HOST_OVERRIDE=127.0.0.1

arbitrage-admin --config config.production.clob_hft.json discovery overlap
python scripts/live_balance_and_order_readiness.py --config config.production.clob_hft.json --all-markets
arbitrage-admin --config config.production.clob_hft.json production audit --all-markets --defer-backup-gates

arbitrage-admin --config config.production.quote_arb.json discovery overlap
python scripts/live_balance_and_order_readiness.py --config config.production.quote_arb.json --all-markets
arbitrage-admin --config config.production.quote_arb.json production audit --all-markets --defer-backup-gates
```

Fail closed if any enabled route has:

- `verified_tradable_count = 0`
- `mechanically_openable_count = 0`
- no launch-eligible sports or crypto market within the configured 200-hour horizon
- unhealthy venue balances
- unresolved intents/redemptions
- reconciliation failures
- risk pause

Every enabled route must pass the mechanical signed-preview contract, but the
funded target needs a natural positive opportunity on at least one of its routes,
not on all routes simultaneously. Routes with no current positive edge remain
non-blocking `NO-TRADE`; if the entire target has no positive edge, funded launch
does not start.

All-market reports intentionally separate two launch states:

- `mechanically_openable_count` validates mapping identity, three consecutive
  best-level depth samples, order constraints, exact paired signed previews,
  verified fee bounds, and zero signed-preview price impact without requiring a
  currently positive net edge.
- `technical_openable_count` validates mapping identity, three consecutive
  depth samples, constraints, verified fees, signed preview, VWAP, live chain
  cost, current route threshold, and minimum expected profit. It ignores only
  operator/runtime balance gates, risk pause, and live confirmation so it can
  be measured safely before risk resume.
- `canary_openable_count` additionally requires current venue/runtime balance
  gates, `risk_paused = 0`, and live-trading confirmation. It is the per-signal
  funded execution gate. Legacy `openable_count` aliases technical readiness
  and must never authorize funded execution.

`economically_openable_count` remains in JSON as a compatibility alias for
`technical_openable_count`; new consumers must not treat a loss-making preview
as technically openable.

The `technical_and_canary_v5` report also distinguishes
`current_technical_openable_count` from `recent_technical_evidence_count`.
After three consecutive signed shadow preflights pass, the bot stores a bounded
`shadow_preflight_evidence` event in PostgreSQL. The audit accepts it for at most
`shadow_preflight_evidence_ttl_seconds` (900 seconds in production), and only when
the runtime instance, exact CI-verified release SHA, route, and still-eligible
market match. Stored depth, fee, chain-cost, and profit observations are checked
against the current policy; the audit does not claim they are a fresh book
snapshot. Economic failures invalidate technical openability. A stale,
mismatched, incomplete, or unverified event is never counted.

Never submit or resume risk solely because `technical_openable_count > 0`.
Recent technical evidence only proves that the route can execute its current
preflight contract; every funded signal still repeats all pre-submit checks.

Before approving mappings, preview `mappings approve-safe-candidates` without
`--confirm YES`. Auto-approval is restricted to persisted `exact_id` provenance
inside the configured category and launch horizon;
exact-title, semantic, and legacy candidates require individual operator review.
The preview must follow a successful `discovery overlap --persist-candidates`.
Only mappings observed by that persisted run within `discovery_max_stale_seconds`
remain eligible; disappeared venue markets fail closed. Eligibility is based on
`last_discovered_at`, which only the explicit persisted discovery run updates;
mapping status or metadata changes do not count as current observation evidence.
Use `--route ROUTE` for route-specific closeout; omit it only when intentionally
processing every enabled route in the selected config.
Use repeatable `--category crypto|sports` and `--mapping-id ID` options to scope
the preview and confirmation to the intended canary universe. A requested ID
that is no longer a safe candidate fails the entire command before any mapping
is changed.

`discovery audit` and `discovery overlap` are read-only by default. Use
`discovery overlap --persist-candidates` only in the deliberate mapping-bootstrap
step immediately before review. Runtime discovery also persists candidates, so
ordinary readiness and production audits never need this flag, but it does not
refresh approval evidence unless the explicit operator flag is present.

`production_closeout.sh` defaults `AUTO_APPROVE_SAFE_MAPPINGS=NO`. Review the
preview artifact and approve only the mappings intentionally entering the canary.
Set the variable to `YES` only when accepting every safe candidate reported for
all enabled routes; this can be a large set and is not the default launch path.

For the first funded launch only, `--defer-backup-gates` skips:

- `backup`
- `restore_drill`
- `spot_drain_readiness`

It does not skip balances, gas, settlement metadata, mappings, reconciliation, readiness, or live evidence.

## 6. Shadow Calibration And Funded Canary

Compose defaults both bot services to `shadow`, even though the mounted production
configs describe the canary contract. Discovery and safe exact-ID approvals must run
before calibration. `production_closeout.sh` does this automatically and never
auto-approves fuzzy, semantic, exact-title, or structured-sports mappings.

Both tracked production configs set `shadow_require_verified_mappings=true`. Discovery
still persists every candidate for review, but the runtime publishes only markets that
pass the same route-specific mapping and metadata checks as canary execution. Do not
disable this gate for production calibration: candidate-wide evaluation both makes the
calibration evidence non-executable and can saturate the shared VM CPU.

Calibration is a two-release process. First collect 60 minutes and at least 10,000
valid executable evaluations per enabled route without modifying the deployed config.
Use the wrapper so mapping bootstrap, paused-shadow proof, technical-only audit, and
pause-on-exit remain enforced:

```bash
CI_VERIFIED_COMMIT_SHA=<verified-sha> \
CALIBRATION_REQUIRE_CONFIGURED_RESERVE=NO \
./ops/production_closeout.sh
```

The first run can fail at the later pre-live audit because route reserves are still
missing; that is expected. The two calibration JSON reports remain the input to the
next release and the EXIT trap restores risk pause.

Apply the reported route p95 values to the local production configs, commit them,
pass CI, and deploy that exact verified SHA. Never use `--write-config` on the
authoritative VM checkout because that creates an unverified post-deploy config.
On the final SHA, rerun the wrapper with its default configured-reserve check; the
window fails if a route reserve is missing or below the newly observed p95.

The paused-shadow calibration on release `4d0613ce` completed from
`2026-09-01T15:39:56Z` through `16:40:11Z` with uninterrupted health continuity.
It measured `polymarket_sx=0.0005` from 130,663 valid evaluations and
`polymarket_myriad=0.0001` from 15,336, so those values are the minimum tracked
reserves for the next release. Predict produced 12,918 valid evaluations, but its
p95 exceeded the histogram and the run exposed that a stale history sample could be
selected before retention pruning; treat that p95 as invalid and do not lower its
conservative `0.01` reserve until the corrected calibration completes on the next
exact CI-verified SHA. The authoritative reports
are under `.runtime/calibration-{clob,quote}-20260901T153337Z-4d0613ce` on the VM.
Those historical reports do not qualify the newly enabled `predict_sx`,
`sx_myriad`, or `predict_myriad` routes. Their configured reserves are conservative
cross-route starting reserves only; the fresh 3600-second run must produce at least
10000 valid evaluations for every enabled route and must not exceed those reserves.
Otherwise funded launch remains `NO-GO`.

Calibration and the technical-only audit run while both services remain risk-paused
in `shadow` with `LIVE_TRADING_CONFIRM=NO`. A paused sample is accepted only when
`risk_paused=1`, `arbitrage_ready=0`, and the readiness endpoint has no blocker other
than `risk_paused:*`. `risk resume` occurs only after technical pass and explicit
funded-canary authorization; it still rejects unresolved intents, redemptions,
manual-review positions, reconciliation drift, and exceeded daily loss.
After a service restart, the wrapper allows up to 15 minutes for route discovery to
restore `/health/ready`; override `READY_WAIT_ATTEMPTS` or
`READY_WAIT_SLEEP_SECONDS` only when the VM catalog benchmark justifies it.

At `$25` per leg, never run both runtime instances funded at the same time. One
funded service may hold at most five positions: `$250` aggregate principal, `$125`
per venue, and `$52` per market including its bounded fee/chain allowance. The
`$252` total-notional cap is `$250` principal plus the configured aggregate buffer.
The shared runtime entry lock permits only one two-leg entry in flight, and the
service-wide limiter reserves two of the ten entry-order slots before each submit.
Keep the non-target service risk-paused in `shadow`; complete and reconcile one
service window before switching to the other. The `$10` daily-loss setting is a
realized-loss breaker, not a mathematical guarantee that final losses cannot exceed
`$10`: positions already open when the stop trips may realize later.

Do not set either service mode to `canary` if calibration fails. After calibration,
the wrapper runs overlap, all-market readiness, and the pre-live audit. A funded run
requires exactly one `FUNDED_CANARY_TARGET`; only that service is recreated in
`canary`, while every non-target service remains risk-paused in `shadow`.

For rare opportunities, a point-in-time audit may miss otherwise valid signed
preflight evidence after its normal TTL. Keep both services risk-paused in shadow and
use the dedicated observer to latch the first valid exact-release sample set:

```bash
CI_VERIFIED_COMMIT_SHA=<verified-sha> ./ops/operator_python.sh \
  scripts/shadow_openability_window.py \
  --config config.production.clob_hft.json \
  --config config.production.quote_arb.json \
  --duration-seconds 7200 \
  --poll-seconds 15 \
  --stop-on all_routes_technical_openable \
  --artifact-dir closeout-artifacts/<verified-sha>/shadow-openability-window
```

The observer never resumes risk or submits orders. It accepts evidence only for the
exact release SHA, current market at capture, three signed samples, configured depth,
verified fees, live chain cost, route threshold, and minimum profit while the runtime
is confirmed paused shadow. Its report is diagnostic technical evidence only; it does
not satisfy the 60-minute calibration or route-specific funded canary gates.

The funded observer window is 240 minutes. It always runs to timeout; an early fill
does not shorten the test. Run one observer per required route:

The VM host Python is not part of the release runtime. Run operator Python commands
through `./ops/operator_python.sh`; it uses the locked Python 3.12 image, host
networking, and the Docker socket only for the lifetime of the one-off command.
`production_closeout.sh` selects this runner automatically.
Never run an all-market audit with `docker exec` inside either bot container: its
catalog workload competes with the live engine inside the bot memory cgroup.
Bot containers are explicitly tagged with `ARBITRAGE_RUNTIME_ROLE=bot`, and
catalog/all-market commands fail closed there. Use `./ops/operator_python.sh`.

Polymarket live order books use the market WebSocket as the source of truth. REST
`/book` is limited to bootstrap, recovery, and periodic integrity snapshots. During
shadow calibration, `arbitrage_market_data_events{venue="Polymarket",event="rest_rate_limits"}`
must remain `0`; any increase invalidates the window and requires rate/recovery review.

Run one observer per enabled route. `production_closeout.sh` is the authoritative
launcher: it arms all route observers while the target is durably paused, resumes
exactly one target, publishes one shared absolute deadline immediately after the
durable resume, and starts an independent watchdog. The runtime reads the same
deadline before every entry-submit boundary. Do not launch the route observers
manually because that would omit this coordination; the other service must remain
paused-shadow for the whole window.

The wrapper holds a non-blocking project-scoped `flock` for its entire lifetime;
a second closeout run exits before changing Compose or risk state. If any required
observer exits before the deadline, or loses local health/metrics or PostgreSQL
monitoring for two consecutive samples, the wrapper durably pauses the funded
runtime, waits for entry quiescence, terminates the remaining observers/watchdog,
and fails the run.

The observer captures:

- `/health/live`
- `/health/ready`
- `/metrics`
- Docker Compose logs for the requested services
- unresolved intents from PostgreSQL
- fills from PostgreSQL
- open positions from PostgreSQL
- risk pause state
- reconciliation failures
- the per-route accepted-entry-preflight counter baseline and delta
- post-deadline risk pause plus entry-lock quiescence

HTTP health is sampled every 15 seconds. PostgreSQL evidence uses the separate
60-second cadence to avoid three parallel route observers overloading the single production
database. A transient database timeout is recorded and the observer continues, but
`monitoring_continuity.passed` remains false and the final evidence audit fails closed.
Log capture is also fail-closed: every requested Compose service must return its full
`--since started_at` log window or `log_capture_ok=false` invalidates the observer report.

Synthetic integration/restart artifacts do not satisfy live proof.

## 7. Final Go/No-Go Audit

Each service needs its own final audit with its own `report.json`:

```bash
arbitrage-admin --config config.production.clob_hft.json production audit --all-markets --defer-backup-gates --require-live-order-evidence --post-window-paused --live-window-report predict_sx=canary-artifacts/clob_hft/predict_sx/<timestamp>/report.json --live-window-report polymarket_sx=canary-artifacts/clob_hft/polymarket_sx/<timestamp>/report.json --live-window-report sx_myriad=canary-artifacts/clob_hft/sx_myriad/<timestamp>/report.json
arbitrage-admin --config config.production.quote_arb.json production audit --all-markets --defer-backup-gates --require-live-order-evidence --post-window-paused --live-window-report polymarket_predict=canary-artifacts/quote_arb/polymarket_predict/<timestamp>/report.json --live-window-report polymarket_myriad=canary-artifacts/quote_arb/polymarket_myriad/<timestamp>/report.json --live-window-report predict_myriad=canary-artifacts/quote_arb/predict_myriad/<timestamp>/report.json
```

Acceptance:

- `bot-clob-hft`
  - `predict_sx` has `verified_tradable_count > 0`
  - `polymarket_sx` has `verified_tradable_count > 0`
  - `sx_myriad` has `verified_tradable_count > 0`
  - all three enabled routes passed pre-live full-capacity readiness
  - each completed report contains real order/fill evidence, or a clean `safe_no_trade`
    result with no current positive net edge
- `bot-quote-arb`
  - `polymarket_predict` has `verified_tradable_count > 0`
  - `polymarket_myriad` has `verified_tradable_count > 0`
  - `predict_myriad` has `verified_tradable_count > 0`
  - all three enabled routes passed pre-live full-capacity readiness
  - each completed route report contains real evidence, or a clean `safe_no_trade`
    result with no current positive net edge
- funded target service:
  - remained ready and unpaused throughout the 14400-second observation window
  - after the window, durable risk pause reason is `funded_canary_window_complete`
  - open positions remain in canary mode for reconciliation/exit/redemption
  - transition to paused-shadow occurs only at zero positions, zero unresolved
    intents/redemptions, and clean reconciliation
- non-target service:
  - `/health/live` = 200
  - execution mode `shadow`
  - `arbitrage_ready = 0`
  - `arbitrage_risk_paused = 1`
  - unresolved intents = 0
  - unresolved redemptions = 0

## 8. One-Command Closeout Wrapper

For the full two-service artifact bundle:

```bash
cd /opt/labyda_next
./ops/production_closeout.sh
```

The default wrapper run performs only shadow calibration and pre-live checks. Funded
execution requires a second explicit invocation after operator sign-off and confirmed
credential rotation:

```bash
CREDENTIAL_ROTATION_CONFIRMED=YES \
ENABLE_FUNDED_CANARY=YES \
FUNDED_CANARY_TARGET=quote_arb \
./ops/production_closeout.sh
```

Defaults:

- targets:
  - `clob_hft`
  - `quote_arb`
- shadow calibration:
  - `3600` seconds and `10000` valid evaluations per route
  - exact-ID safe approvals happen before the window
  - the final SHA must contain a route reserve at least as large as observed p95
- funded live window:
  - `14400` seconds per route
- backup gates:
  - deferred

Artifacts are written under one timestamped root with per-target subdirectories.

## 9. Current Blocking Conditions

Do not call funded launch `GO` until these are closed on the VM:

- `quote_arb`
  - no repeat `risk_paused` after resume
  - `market_data_invalid: Myriad` resolved under quiet-but-executable semantics
  - Myriad gas funded
  - settlement metadata complete
  - Predict.fun balance funded
  - `predict_myriad` verified mapping, calibration, signed preview, and natural openability proven
- `clob_hft`
  - `SX_BET_PRIVATE_KEY` configured
  - SX API version matches the official cutover state; follow `ops/SX_BET_V3_CUTOVER.md`
  - V3 requires a new API key, deployed/funded proxy, live account fee metadata, and `FOK`
  - `predict_sx`, `polymarket_sx`, and `sx_myriad` verified mappings present
  - all three SX-family route calibrations, signed previews, overlap, and natural openability proven on the VM
  - Predict.fun and Myriad balances satisfy the same full-capacity gate as SX and Polymarket
- Polymarket settlement
  - `funder != signer` topology resolved or explicitly supported

Without those closures, real-money launch remains `NO-GO`.
