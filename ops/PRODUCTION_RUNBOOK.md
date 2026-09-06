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

The production discovery universe contains all six unique routes between the four
supported venues. The one funded runtime owns the five routes with a current safe
mapping path; `sx_myriad` stays enabled as `NO-TRADE` discovery until a current
verified overlap exists. The second service retains overlapping SX discovery only
for paused-shadow continuity:

- `bot-clob-hft`: `predict_sx`, `polymarket_sx`, `sx_myriad`
- `bot-quote-arb`: all six supported routes

Evaluation remains bounded to 16 concurrent markets in `clob_hft` and 18 in
`quote_arb`, so adding routes widens the rotating candidate universe without
unbounded work. Five `quote_arb` routes are funded; `sx_myriad` and every
`clob_hft` route are discovery-only and their final execution gates reject entry.

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
  - discovery routes:
    - `predict_sx`
    - `polymarket_sx`
    - `sx_myriad`
  - funded routes: none; this service stays durably risk-paused in `shadow`
- `bot-quote-arb`
  - config: `config.production.quote_arb.json`
  - observability: `http://127.0.0.1:9109`
  - runtime instance: `quote_arb`
  - discovery routes:
    - `polymarket_predict`
    - `polymarket_myriad`
    - `predict_myriad`
    - `predict_sx`
    - `polymarket_sx`
    - `sx_myriad` (enabled `NO-TRADE`; not funded until a current verified overlap exists)
  - funded routes:
    - `polymarket_predict`
    - `polymarket_myriad`
    - `predict_myriad`
    - `predict_sx`
    - `polymarket_sx`

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
DEPLOY_HEALTH_POLICY=safe_paused_shadow_bootstrap \
CLOB_HFT_EXECUTION_MODE=shadow \
QUOTE_ARB_EXECUTION_MODE=shadow \
LIVE_TRADING_CONFIRM=NO \
./ops/deploy_compose.sh
```

`deploy_compose.sh` must:

- require a clean tracked worktree and no untracked runtime/build inputs
- fast-forward `origin/codex/production-closeout`
- build the migration image from the verified SHA before stopping either runtime
- run Alembic
- rebuild and start both services
- require both services to pass the selected fail-closed health policy

For the first deployment of newly enabled routes,
`safe_paused_shadow_bootstrap` requires `/health/live=200`, `/health/ready=503`,
`arbitrage_risk_paused=1`, `arbitrage_ready=0`, exact runtime identity, shadow mode,
and `LIVE_TRADING_CONFIRM=NO`. It accepts only `risk_paused:*` plus a fresh,
non-stale `discovery_not_ready` snapshot with explicit missing routes; market-data,
reconciliation, manual-review, and every other blocker still fail the deployment.
Once discovery completes, the same bootstrap policy also accepts the stricter
`safe_paused_shadow` state where `risk_paused:*` is the only readiness reason.
This keeps the bounded deployment gate monotonic as bootstrap finishes.
Its default health window has an absolute 1200-second wall-clock deadline so a
full scan-all catalog pass can finish before the wrapper reports failure. Normal
policies use a 240-second deadline. `HEALTH_RETRIES` remains a secondary attempt
cap (`600` for bootstrap, `120` otherwise), and every probe plus the final sleep
is constrained by the remaining deadline. Set `HEALTH_WAIT_TIMEOUT_SECONDS`
explicitly only when an operator needs a different bounded window.
After mapping bootstrap, use strict `safe_paused_shadow`, which accepts no readiness
reason except `risk_paused:*`. Normal funded deployments use the default `ready`
policy.
The Compose container healthcheck uses `/health/live`; operational readiness is
never inferred from Docker's liveness state and remains enforced by the policy above.

Do not skip migrations after schema changes.

## 4. Runtime Config Gate

`config.production.clob_hft.json` must stay narrowed to:

```json
{
  "runtime_instance_id": "clob_hft",
  "execution_mode": "shadow",
  "shadow_mode": true,
  "live_trading_confirmed": false,
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
  },
  "funded_routes": {
    "predict_sx": false,
    "polymarket_sx": false,
    "sx_myriad": false
  }
}
```

`config.production.quote_arb.json` must stay narrowed to:

```json
{
  "runtime_instance_id": "quote_arb",
  "execution_mode": "canary",
  "live_trading_confirmed": false,
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
  "categories_to_scan": [
    "ai", "airdrops", "apple", "box office", "business", "canada", "china",
    "crypto", "culture", "economy", "fed", "federal reserve", "finance", "gdp",
    "gta 6", "iran", "politics", "prediction markets", "science", "spacex",
    "sports", "trump", "video games", "weather"
  ],
  "market_horizon_filter_enabled": true,
  "max_sports_market_horizon_hours": 200,
  "max_crypto_market_horizon_hours": 200,
  "max_market_horizon_hours_by_category": {
    "ai": 200, "airdrops": 200, "apple": 200, "box office": 200,
    "business": 200, "canada": 200, "china": 200, "culture": 200,
    "economy": 200, "fed": 200, "federal reserve": 200, "finance": 200,
    "gdp": 200, "gta 6": 200, "iran": 200, "politics": 200,
    "prediction markets": 200, "science": 200, "spacex": 200, "trump": 200,
    "video games": 200, "weather": 200
  },
  "enable_predict_fun": true,
  "enable_sx_bet": true,
  "routes": {
    "polymarket_predict": true,
    "polymarket_myriad": true,
    "predict_myriad": true,
    "predict_sx": true,
    "polymarket_sx": true,
    "sx_myriad": true
  },
  "funded_routes": {
    "polymarket_predict": true,
    "polymarket_myriad": true,
    "predict_myriad": true,
    "predict_sx": true,
    "polymarket_sx": true,
    "sx_myriad": false
  }
}
```

The quote category list is generated from the latest exact-ID overlap inventory.
`gaming` is excluded because that inventory found no cross-venue match, and
`unknown` remains discovery-only. Every category beyond `sports` and `crypto`
must have a positive entry in `max_market_horizon_hours_by_category`; canary/live
validation fails closed otherwise.

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

The formal launch target in this release is `quote_arb`, with all six routes in
one runtime and five funded routes in one shared entry-lock and entry-rate-limit
domain. Run its reconciliation,
overlap, balance/signed-preview readiness, and production audit. Lack of a liquid
opportunity on one route does not prevent that route from starting; it remains a
per-entry `NO-TRADE`. `clob_hft` is checked only for discovery continuity and zero
managed PostgreSQL state because it is not a funded target:

```bash
cd /opt/labyda_next
export ARBITRAGE_DATABASE_HOST_OVERRIDE=127.0.0.1

ARBITRAGE_EXECUTION_MODE_OVERRIDE=shadow arbitrage-admin --config config.production.clob_hft.json discovery overlap

ARBITRAGE_EXECUTION_MODE_OVERRIDE=shadow arbitrage-admin --config config.production.quote_arb.json reconcile
arbitrage-admin --config config.production.quote_arb.json discovery overlap
python scripts/live_balance_and_order_readiness.py --config config.production.quote_arb.json --all-markets
arbitrage-admin --config config.production.quote_arb.json production audit --all-markets --defer-backup-gates
```

Fail closed before enabling submission if the funded runtime has:

- an enabled funded route has no verified tradable mapping
- no launch-eligible configured-category market within the 200-hour horizon
- unhealthy venue balances
- unresolved intents/redemptions
- reconciliation failures
- risk pause

Every submitted entry must pass the mechanical signed-preview, liquidity, price-
impact, and net-edge contracts. A route does not need a current liquid or positive-
edge opportunity merely to start scanning; such a route remains a non-blocking
`NO-TRADE` until an eligible opportunity appears.

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
processing every discovery route in the selected config.
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
all discovery routes; this can be a large set and is not the default launch path.

### 5.1 Existing personal Polymarket positions

Personal combo bets are not bot positions, but they must never be silently ignored.
Record one exact external-account baseline before calibration. Baseline management
requires the runtime in `shadow`, a durable risk pause, zero unresolved order and
redemption intents, zero pending/manual-review bot positions, zero venue open orders,
and exclusive ownership of the runtime trader advisory lock.

Preview only:

```bash
ARBITRAGE_EXECUTION_MODE_OVERRIDE=shadow LIVE_TRADING_CONFIRM=NO \
  arbitrage-admin --config config.production.quote_arb.json \
  reconciliation baseline-external --venue Polymarket --operator <operator>
```

Inspect the account fingerprint, residual position list, external fill references,
and `manifest_sha256`. Apply only by copying the exact digest printed by that fresh
preview; the command recaptures the account while holding the same trader lock and
rejects any changed manifest:

```bash
ARBITRAGE_EXECUTION_MODE_OVERRIDE=shadow LIVE_TRADING_CONFIRM=NO \
  arbitrage-admin --config config.production.quote_arb.json \
  reconciliation baseline-external --venue Polymarket --operator <operator> \
  --manifest-sha256 <exact-preview-digest> --confirm YES
```

Activation immediately runs full reconciliation. Readiness remains blocked unless
the latest full result is fresh, clean, and bound to the active baseline's exact
account fingerprint and manifest. Any later untracked fill, position delta, account
identity change, or baseline revocation fails closed. To revoke, first run the
following command without confirmation, then repeat its exact printed command with
the active digest and `--confirm YES`:

```bash
ARBITRAGE_EXECUTION_MODE_OVERRIDE=shadow LIVE_TRADING_CONFIRM=NO \
  arbitrage-admin --config config.production.quote_arb.json \
  reconciliation revoke-external-baseline --venue Polymarket --operator <operator>
```

For the first funded launch only, `--defer-backup-gates` skips:

- `backup`
- `restore_drill`
- `spot_drain_readiness`

It does not skip balances, gas, settlement metadata, mappings, reconciliation, readiness, or live evidence.

## 6. Shadow Calibration And Funded Canary

Compose defaults both bot services to `shadow`; `quote_arb`'s tracked config describes
the canary contract while `clob_hft` is explicitly discovery-only shadow. Discovery
and safe exact-ID approvals must run
before calibration. `production_closeout.sh` does this automatically and never
auto-approves fuzzy, semantic, exact-title, or structured-sports mappings.

Both tracked production configs set `shadow_require_verified_mappings=true`. Discovery
still persists every candidate for review, but the runtime publishes only markets that
pass the same route-specific mapping and metadata checks as canary execution. Do not
disable this gate for production calibration: candidate-wide evaluation both makes the
calibration evidence non-executable and can saturate the shared VM CPU.

Calibration is a two-release process. First collect 60 minutes and at least 10,000
valid executable evaluations for each funded `quote_arb` route without modifying the
deployed config. The wrapper performs a clean full reconciliation both before and
after this window. Before it changes either running bot to shadow, it also requires
zero open PostgreSQL positions and zero unresolved order/redemption intents for both
runtime instances; otherwise it aborts and leaves the existing canary process
risk-paused so reconciliation and exits can continue.
Use the wrapper so mapping bootstrap, paused-shadow proof, technical-only audit, and
pause-on-exit remain enforced:

```bash
CI_VERIFIED_COMMIT_SHA=<verified-sha> \
CALIBRATION_REQUIRE_CONFIGURED_RESERVE=NO \
./ops/production_closeout.sh
```

The first run can fail at the later pre-live audit because route reserves are still
missing; that is expected. The quote calibration JSON report remains the input to the
next release and the EXIT trap restores risk pause.

Apply the reported route p95 values to the local production configs, commit them,
pass CI, and deploy that exact verified SHA. Never use `--write-config` on the
authoritative VM checkout because that creates an unverified post-deploy config.
On the final SHA, rerun the wrapper with its default configured-reserve check; the
window fails if a route reserve is missing or below the newly observed p95.

Historical calibration used a smaller leg size and does not qualify this release.
The configured route-specific adverse-move reserves are conservative starting bounds
only. A fresh exact-SHA 3600-second run must produce the configured minimum of valid
evaluations for each of the five funded routes and must not exceed its configured reserve;
otherwise funded launch remains `NO-GO`. The separate `clob_hft` discovery routes
cannot submit orders.

Calibration and the technical-only audit run while both services remain risk-paused
in `shadow` with `LIVE_TRADING_CONFIRM=NO`. A paused sample is accepted only when
`risk_paused=1`, `arbitrage_ready=0`, and the readiness endpoint has no blocker other
than `risk_paused:*`. `risk resume` occurs only after technical pass and explicit
funded-canary authorization; it still rejects unresolved intents, redemptions,
manual-review positions, reconciliation drift, and exceeded daily loss.
The runtime proactively revalidates only the exact funded target window before the
hard order-book age deadline. A failed refresh still exposes the normal stale blocker;
discovery-only targets neither consume this refresh budget nor block funded readiness.
Myriad refresh attempts and failures are exported as bounded `event` labels on
`arbitrage_market_data_events_total`, without market or token identifiers.
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

Do not set `quote_arb` to `canary` if calibration fails; never set `clob_hft` to
`canary` in this release. After calibration,
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
  --duration-seconds 14400 \
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

Run one observer per funded route. `production_closeout.sh` is the authoritative
launcher: it arms all route observers while the target is durably paused, publishes
one shared absolute deadline, resumes exactly one target, and starts an independent
watchdog. The runtime reads the same
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
60-second cadence to avoid six parallel route observers overloading the single production
database. A transient database timeout is recorded and the observer continues, but
`monitoring_continuity.passed` remains false and the final evidence audit fails closed.
Log capture is also fail-closed: every requested Compose service must return its full
`--since started_at` log window or `log_capture_ok=false` invalidates the observer report.

Synthetic integration/restart artifacts do not satisfy live proof.

## 7. Final Go/No-Go Audit

Only the funded `quote_arb` service receives a final live audit in this release:

```bash
ARBITRAGE_EXECUTION_MODE_OVERRIDE=canary arbitrage-admin --config config.production.quote_arb.json production audit --all-markets --defer-backup-gates --require-live-order-evidence --post-window-paused --live-window-report polymarket_predict=canary-artifacts/quote_arb/polymarket_predict/<timestamp>/report.json --live-window-report polymarket_myriad=canary-artifacts/quote_arb/polymarket_myriad/<timestamp>/report.json --live-window-report predict_myriad=canary-artifacts/quote_arb/predict_myriad/<timestamp>/report.json --live-window-report predict_sx=canary-artifacts/quote_arb/predict_sx/<timestamp>/report.json --live-window-report polymarket_sx=canary-artifacts/quote_arb/polymarket_sx/<timestamp>/report.json
```

Acceptance:

- `bot-clob-hft`
  - stays durably risk-paused in `shadow`
  - has zero open PostgreSQL positions and zero unresolved intents before shadow transition
  - may continue discovery for all three routes, but cannot submit an entry
- `bot-quote-arb`
  - all five funded routes have `verified_tradable_count > 0`
  - `sx_myriad` remains enabled discovery with no funded entry path
  - all four venues passed pre-live full-capacity funding readiness
  - at least one route had a natural positive net edge before risk resume
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
CI_VERIFIED_COMMIT_SHA=<verified-sha> ./ops/production_closeout.sh
```

The default wrapper run performs only shadow calibration and pre-live checks. Funded
execution requires a second explicit invocation after operator sign-off and confirmed
credential rotation:

```bash
CREDENTIAL_ROTATION_CONFIRMED=YES \
ENABLE_FUNDED_CANARY=YES \
FUNDED_CANARY_TARGET=quote_arb \
CI_VERIFIED_COMMIT_SHA=<verified-sha> \
./ops/production_closeout.sh
```

Defaults:

- managed services: `clob_hft` and `quote_arb`
- only formal/funded target: `quote_arb`
- enabled routes: all six supported venue pairs in the one `quote_arb` runtime
- funded routes: five; `sx_myriad` remains enabled `NO-TRADE` discovery
- shadow calibration:
  - `3600` seconds and `10000` valid evaluations per route
  - exact-ID safe approvals happen before the window
  - the final SHA must contain a route reserve at least as large as observed p95
- funded live window:
  - one shared `14400`-second window, observed independently for every route
- backup gates:
  - deferred

Artifacts are written under one timestamped root with per-target subdirectories.

## 9. Current Blocking Conditions

Do not call funded launch `GO` until current exact-SHA evidence closes all of these
gates on the VM:

- `quote_arb`
  - active Polymarket external baseline exactly captures the user's pre-existing bets
  - latest full reconciliation is fresh and matches the active baseline fingerprint/manifest
  - zero unresolved intents, redemptions, manual-review state, or reconciliation drift
  - Polymarket, Predict.fun, SX Bet, and Myriad each satisfy the `$125` principal
    gate plus signed-preview fee/gas headroom required by their funded routes
  - verified mappings and the 60-minute calibration qualify all five funded routes
  - at least one route has a current natural positive net edge; routes without
    sufficient depth remain active `NO-TRADE`, and every later entry must pass its
    own current depth, settlement-metadata, signed-preview, and zero-impact gates
  - release SHA and both immutable config digests match the CI-verified manifest
- `clob_hft`
  - durable risk pause, `shadow` mode, and zero managed PostgreSQL state before any
    wrapper-driven shadow recreate

Independent funded qualification of `clob_hft` does not block this release because
its funded allowlist is empty. SX credentials and balance do block `quote_arb`, since
its funded allowlist includes `predict_sx` and `polymarket_sx`.

Without those closures, real-money launch remains `NO-GO`.
