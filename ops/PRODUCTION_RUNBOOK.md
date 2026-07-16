# Production Runbook — Docker Compose Split Services On GCP Spot

Authoritative runtime:

- VM checkout: `/home/tolik1992s/labyda_next`
- database: local PostgreSQL on the same VM
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
  - one Spot VM
  - one local PostgreSQL
  - one boot disk
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
cd /home/tolik1992s/labyda_next
COMPOSE_ENV_FILE=.env.production ./ops/deploy_compose.sh
```

`deploy_compose.sh` must:

- require a clean tracked worktree
- fast-forward `origin/master`
- run Alembic
- rebuild and start both services
- wait for both readiness endpoints

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
  "max_sports_market_horizon_hours": 48,
  "max_crypto_market_horizon_hours": 24,
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
  "max_sports_market_horizon_hours": 48,
  "max_crypto_market_horizon_hours": 24,
  "enable_predict_fun": true,
  "enable_sx_bet": false,
  "routes": {
    "polymarket_predict": true,
    "polymarket_myriad": true
  }
}
```

Both services share one PostgreSQL, but must not share:

- trader lock
- risk pause state
- readiness state

That isolation is keyed by `runtime_instance_id`.

## 5. Prelaunch Audit Per Service

Run the same three checks for each service config:

```bash
cd /home/tolik1992s/labyda_next
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
- `openable_count = 0`
- no launch-eligible sports market within 48 hours or crypto market within 24 hours
- unhealthy venue balances
- unresolved intents/redemptions
- reconciliation failures
- risk pause

Before approving mappings, preview `mappings approve-safe-candidates` without
`--confirm YES`. Auto-approval is restricted to persisted `exact_id` provenance
inside the configured category and launch horizon;
exact-title, semantic, and legacy candidates require individual operator review.

For the first funded launch only, `--defer-backup-gates` skips:

- `backup`
- `restore_drill`
- `spot_drain_readiness`

It does not skip balances, gas, settlement metadata, mappings, reconciliation, readiness, or live evidence.

## 6. Shadow Calibration And Funded Canary

Compose defaults both bot services to `shadow`, even though the mounted production
configs describe the canary contract. Run 60 minutes of calibration first and require
at least 10,000 valid executable evaluations per enabled route:

```bash
python scripts/shadow_calibration.py \
  --config config.production.clob_hft.json \
  --duration-seconds 3600 \
  --min-valid-evaluations 10000 \
  --artifact-dir calibration-artifacts/clob_hft \
  --write-config

python scripts/shadow_calibration.py \
  --config config.production.quote_arb.json \
  --duration-seconds 3600 \
  --min-valid-evaluations 10000 \
  --artifact-dir calibration-artifacts/quote_arb \
  --write-config
```

Do not set either service mode to `canary` if calibration fails. After calibration,
run overlap, all-market readiness, and the pre-live audit. Then start both services
together with `CLOB_HFT_EXECUTION_MODE=canary`,
`QUOTE_ARB_EXECUTION_MODE=canary`, and `LIVE_TRADING_CONFIRM=YES`.

The funded observer window is 120 minutes. It always runs to timeout; an early fill
does not shorten the test. Run one observer per required route:

The VM host Python is not part of the release runtime. Run operator Python commands
through `./ops/operator_python.sh`; it uses the locked Python 3.12 image, host
networking, and the Docker socket only for the lifetime of the one-off command.
`production_closeout.sh` selects this runner automatically.

Run one observer per service:

```bash
cd /home/tolik1992s/labyda_next

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
  - `polymarket_sx` has `openable_count > 0`
  - `report.json` contains real fill/open-position evidence for runtime instance `clob_hft`
- `bot-quote-arb`
  - `polymarket_predict` has `verified_tradable_count > 0`
  - `polymarket_predict` has `openable_count > 0`
  - `polymarket_myriad` has `verified_tradable_count > 0`
  - `polymarket_myriad` has `openable_count > 0`
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
cd /home/tolik1992s/labyda_next
./ops/production_closeout.sh
```

The default wrapper run performs only shadow calibration and pre-live checks. Funded
execution requires a second explicit invocation after credential rotation and sign-off:

```bash
ENABLE_FUNDED_CANARY=YES ./ops/production_closeout.sh
```

Defaults:

- targets:
  - `clob_hft`
  - `quote_arb`
- shadow calibration:
  - `3600` seconds and `10000` valid evaluations per route
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
