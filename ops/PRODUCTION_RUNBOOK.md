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

The first funded launch is scoped to three routes only:

- `polymarket_sx` via `bot-clob-hft`
- `polymarket_predict` via `bot-quote-arb`
- `polymarket_myriad` via `bot-quote-arb`

Disabled for this launch:

- `predict_myriad`
- `predict_sx`
- `sx_myriad`

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
  - enabled route: `polymarket_sx`
- `bot-quote-arb`
  - config: `config.production.quote_arb.json`
  - observability: `http://127.0.0.1:9109`
  - runtime instance: `quote_arb`
  - enabled routes:
    - `polymarket_predict`
    - `polymarket_myriad`

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
  "position_size_usd": 20.0,
  "max_order_size_usd": 20.0,
  "max_open_positions": 1,
  "max_daily_loss_usd": 10.0,
  "categories_to_scan": ["sports"],
  "market_horizon_filter_enabled": true,
  "max_sports_market_horizon_hours": 200,
  "max_crypto_market_horizon_hours": 200,
  "max_market_horizon_hours_by_category": {},
  "enable_sx_bet": true,
  "enable_predict_fun": false,
  "routes": {
    "polymarket_sx": true
  }
}
```

`config.production.quote_arb.json` must stay narrowed to:

```json
{
  "runtime_instance_id": "quote_arb",
  "execution_mode": "canary",
  "position_size_usd": 20.0,
  "max_order_size_usd": 20.0,
  "max_open_positions": 1,
  "max_daily_loss_usd": 10.0,
  "categories_to_scan": ["crypto", "sports"],
  "market_horizon_filter_enabled": true,
  "max_sports_market_horizon_hours": 200,
  "max_crypto_market_horizon_hours": 200,
  "max_market_horizon_hours_by_category": {},
  "enable_predict_fun": true,
  "enable_sx_bet": false,
  "routes": {
    "polymarket_predict": true,
    "polymarket_myriad": true
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
- `technical_openable_count = 0`
- `canary_openable_count = 0` before funded execution
- no launch-eligible sports or crypto market within the configured 200-hour horizon
- unhealthy venue balances
- unresolved intents/redemptions
- reconciliation failures
- risk pause

All-market reports intentionally separate two states:

- `technical_openable_count` validates mapping identity, three consecutive
  depth samples, constraints, verified fees, signed preview, VWAP, live chain
  cost, route threshold, and minimum expected profit. It ignores operator
  pause and live confirmation so it can be measured safely before risk resume.
- `canary_openable_count` adds current venue/runtime balance gates,
  `risk_paused = 0`, and live-trading confirmation. It is the funded execution
  gate. Legacy `openable_count` remains a fail-closed alias for this value.

The `technical_and_canary_v2` report also distinguishes
`current_technical_openable_count` from `recent_technical_evidence_count`.
After three consecutive signed shadow preflights pass, the bot stores a bounded
`shadow_preflight_evidence` event in PostgreSQL. The audit accepts it for at most
`shadow_preflight_evidence_ttl_seconds` (900 seconds in production), and only when
the runtime instance, exact CI-verified release SHA, route, and still-eligible
market match. Every stored sample is revalidated against the current depth,
fee, chain-cost, profit, and route-floor policy. A stale, mismatched, incomplete,
or unverified event is never counted.

Never submit or resume risk solely because `technical_openable_count > 0`.
Recent technical evidence only proves that the route can execute its current
preflight contract; every funded signal still repeats all pre-submit checks.

Before approving mappings, preview `mappings approve-safe-candidates` without
`--confirm YES`. Auto-approval is restricted to persisted `exact_id` provenance
inside the configured category and launch horizon;
exact-title, semantic, and legacy candidates require individual operator review.
Use `--route ROUTE` for route-specific closeout; omit it only when intentionally
processing every enabled route in the selected config.

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

Calibration and the technical-only audit run while both services remain risk-paused
in `shadow` with `LIVE_TRADING_CONFIRM=NO`. A paused sample is accepted only when
`risk_paused=1`, `arbitrage_ready=0`, and the readiness endpoint has no blocker other
than `risk_paused:*`. `risk resume` occurs only after technical pass and explicit
funded-canary authorization; it still rejects unresolved intents, redemptions,
manual-review positions, reconciliation drift, and exceeded daily loss.
After a service restart, the wrapper allows up to 15 minutes for route discovery to
restore `/health/ready`; override `READY_WAIT_ATTEMPTS` or
`READY_WAIT_SLEEP_SECONDS` only when the VM catalog benchmark justifies it.

Do not set either service mode to `canary` if calibration fails. After calibration,
the wrapper runs overlap, all-market readiness, and the pre-live audit. Then it may
start both services together with `CLOB_HFT_EXECUTION_MODE=canary`,
`QUOTE_ARB_EXECUTION_MODE=canary`, and `LIVE_TRADING_CONFIRM=YES`.

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

The funded observer window is 120 minutes. It always runs to timeout; an early fill
does not shorten the test. Run one observer per required route:

The VM host Python is not part of the release runtime. Run operator Python commands
through `./ops/operator_python.sh`; it uses the locked Python 3.12 image, host
networking, and the Docker socket only for the lifetime of the one-off command.
`production_closeout.sh` selects this runner automatically.
Never run an all-market audit with `docker exec` inside either bot container: its
catalog workload competes with the live engine inside the bot memory cgroup.

Polymarket live order books use the market WebSocket as the source of truth. REST
`/book` is limited to bootstrap, recovery, and periodic integrity snapshots. During
shadow calibration, `arbitrage_market_data_events{venue="Polymarket",event="rest_rate_limits"}`
must remain `0`; any increase invalidates the window and requires rate/recovery review.

Run one observer per service:

```bash
cd /opt/labyda_next

./ops/operator_python.sh scripts/live_canary_window.py \
  --config config.production.clob_hft.json \
  --duration-seconds 7200 \
  --poll-seconds 15 \
  --database-poll-seconds 60 \
  --database-timeout-seconds 45 \
  --stop-on timeout \
  --required-route polymarket_sx \
  --artifact-dir canary-artifacts/clob_hft/polymarket_sx \
  --compose-service bot-clob-hft \
  --compose-service bot-quote-arb

./ops/operator_python.sh scripts/live_canary_window.py \
  --config config.production.quote_arb.json \
  --duration-seconds 7200 \
  --poll-seconds 15 \
  --database-poll-seconds 60 \
  --database-timeout-seconds 45 \
  --stop-on timeout \
  --required-route polymarket_predict \
  --artifact-dir canary-artifacts/quote_arb/polymarket_predict \
  --compose-service bot-quote-arb \
  --compose-service bot-clob-hft

./ops/operator_python.sh scripts/live_canary_window.py \
  --config config.production.quote_arb.json \
  --duration-seconds 7200 \
  --poll-seconds 15 \
  --database-poll-seconds 60 \
  --database-timeout-seconds 45 \
  --stop-on timeout \
  --required-route polymarket_myriad \
  --artifact-dir canary-artifacts/quote_arb/polymarket_myriad \
  --compose-service bot-quote-arb \
  --compose-service bot-clob-hft
```

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

HTTP health is sampled every 15 seconds. PostgreSQL evidence uses the separate
60-second cadence to avoid three route observers overloading the single production
database. A transient database timeout is recorded and the observer continues, but
`monitoring_continuity.passed` remains false and the final evidence audit fails closed.
Log capture is also fail-closed: every requested Compose service must return its full
`--since started_at` log window or `log_capture_ok=false` invalidates the observer report.

Synthetic integration/restart artifacts do not satisfy live proof.

## 7. Final Go/No-Go Audit

Each service needs its own final audit with its own `report.json`:

```bash
arbitrage-admin --config config.production.clob_hft.json production audit --all-markets --defer-backup-gates --require-live-order-evidence --live-window-report polymarket_sx=canary-artifacts/clob_hft/polymarket_sx/<timestamp>/report.json
arbitrage-admin --config config.production.quote_arb.json production audit --all-markets --defer-backup-gates --require-live-order-evidence --live-window-report polymarket_predict=canary-artifacts/quote_arb/polymarket_predict/<timestamp>/report.json --live-window-report polymarket_myriad=canary-artifacts/quote_arb/polymarket_myriad/<timestamp>/report.json
```

Acceptance:

- `bot-clob-hft`
  - `polymarket_sx` has `verified_tradable_count > 0`
  - `polymarket_sx` has `technical_openable_count > 0`
  - `polymarket_sx` has `canary_openable_count > 0`
  - `report.json` contains real fill/open-position evidence for runtime instance `clob_hft`
- `bot-quote-arb`
  - `polymarket_predict` has `verified_tradable_count > 0`
  - `polymarket_predict` has `technical_openable_count > 0`
  - `polymarket_predict` has `canary_openable_count > 0`
  - `polymarket_myriad` has `verified_tradable_count > 0`
  - `polymarket_myriad` has `technical_openable_count > 0`
  - `polymarket_myriad` has `canary_openable_count > 0`
  - `report.json` contains real fill/open-position evidence for runtime instance `quote_arb`
- both services:
  - `/health/live` = 200
  - `/health/ready` = 200
  - `arbitrage_ready = 1`
  - `arbitrage_risk_paused = 0`
  - unresolved intents = 0
  - unresolved redemptions = 0

## 8. One-Command Closeout Wrapper

For the full two-service artifact bundle:

```bash
cd /opt/labyda_next
./ops/production_closeout.sh
```

The default wrapper run performs only shadow calibration and pre-live checks. Funded
execution requires a second explicit invocation after operator sign-off and an
explicit credential decision. The preferred path is credential rotation:

```bash
CREDENTIAL_ROTATION_CONFIRMED=YES ENABLE_FUNDED_CANARY=YES ./ops/production_closeout.sh
```

If the owner deliberately keeps previously exposed credentials, record that accepted
risk without falsely claiming rotation:

```bash
CREDENTIAL_ROTATION_RISK_ACCEPTED=YES ENABLE_FUNDED_CANARY=YES ./ops/production_closeout.sh
```

This exception bypasses only the rotation acknowledgement. It does not bypass
balances, fees, previews, mappings, openability, risk, reconciliation, or live-evidence
gates.

Defaults:

- targets:
  - `clob_hft`
  - `quote_arb`
- shadow calibration:
  - `3600` seconds and `10000` valid evaluations per route
  - exact-ID safe approvals happen before the window
  - the final SHA must contain a route reserve at least as large as observed p95
- funded live window:
  - `7200` seconds per route
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
- `clob_hft`
  - `SX_BET_PRIVATE_KEY` configured
  - `polymarket_sx` verified mappings present
  - SX overlap and openability proven on the VM
- Polymarket settlement
  - `funder != signer` topology resolved or explicitly supported

Without those closures, real-money launch remains `NO-GO`.
