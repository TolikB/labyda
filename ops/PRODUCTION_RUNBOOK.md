# Production runbook — Docker Compose on GCP Spot

This repo currently runs production-shadow from Docker Compose on a GCP Spot VM.
The authoritative live checkout is `/home/tolik1992s/labyda_next`; do not
assume the older `/opt/arbitrage` systemd layout is active unless the VM has
been explicitly rebuilt and re-verified.

## 1. Authorization and cost gate

- Do not run funded live orders, wallet funding, venue lifecycle smoke, or `terraform apply` without separate explicit approval.
- Refresh GCP inventory before every rollout. Previous instance names, IPs, and cost estimates are stale evidence.
- Keep the effective monthly footprint at or below the repo guardrails in `ops/PRODUCTION_READINESS_REPORT.md`; no new paid services or larger capacity without explicit approval.

## 2. Active deployment shape

- VM runtime: Docker Compose under `/home/tolik1992s/labyda_next`.
- Main service: `labyda_next-bot-1`.
- Observability endpoint: `http://127.0.0.1:9108`.
- Local health checks:

```bash
curl --fail http://127.0.0.1:9108/health/live
curl --fail http://127.0.0.1:9108/health/ready
curl --fail http://127.0.0.1:9108/metrics
```

- Deployment-only files that stay local and ignored in that checkout:
  - `.env.production`
  - `config.production.json`
  - environment-specific Alertmanager config

## 3. Release gate

- Roll forward only from a clean git checkout on `master`.
- The standard deployment command on the VM is:

```bash
cd /home/tolik1992s/labyda_next
./ops/deploy_compose.sh
```

- `ops/deploy_compose.sh` enforces a clean tracked worktree, fast-forwards `origin/master`, runs Alembic, rebuilds `bot`, and waits for `/health/ready`.
- After schema-affecting changes, do not bypass the migration step. The known failure mode is a crash loop caused by a repo/DB mismatch such as a missing `redemption_intents` table.

## 4. Runtime config gate

Keep production shadow narrowed to the intended route set unless explicitly changing the rollout:

```json
{
  "execution_mode": "shadow",
  "shadow_mode": true,
  "scan_all": true,
  "routes": {
    "polymarket_myriad": true,
    "polymarket_predict": false,
    "predict_myriad": false
  }
}
```

- Disabled routes and disabled venues must stay aligned.
- `Predict.fun` remains disabled in the current live shadow shape.
- Readiness is only valid when `missing_routes=[]` for the enabled route set.

For the current order-submitting canary drill, use this override shape instead:

```json
{
  "execution_mode": "canary",
  "position_size_usd": 20.0,
  "max_order_size_usd": 20.0,
  "min_entry_spread_pct": 0.05,
  "min_retry_spread_pct": 0.05,
  "max_open_positions": 1,
  "max_daily_loss_usd": 10.0,
  "routes": {
    "polymarket_myriad": true,
    "polymarket_predict": false,
    "predict_myriad": false
  }
}
```

- `LIVE_TRADING_CONFIRM=YES` is required for `canary`.
- Run `arbitrage-admin --config config.production.json mappings review --operator tolik` before changing to `canary`.
- Approve only `single_clean_candidate_for_enabled_route` candidates for `polymarket_myriad`.
- If no `VERIFIED` `polymarket_myriad` mapping remains after review, stop the rollout before live orders.

## 5. Passive verification window

Run this immediately after deploy on the active VM:

```bash
cd /home/tolik1992s/labyda_next
DURATION_SECONDS=900 ./ops/shadow_smoke.sh
```

The helper captures:

- `/health/live` every 15 seconds
- `/health/ready` every 15 seconds
- `/metrics` every 15 seconds
- The matching bot log window

Artifacts are written under `shadow-smoke-artifacts/<timestamp>/`.

Pass criteria:

- `/health/live` stays HTTP 200 for the full window.
- `/health/ready` stays HTTP 200, or any failure is an explicitly understood gate rather than disabled-venue noise.
- `arbitrage_market_data_age_seconds` stays below the stream-silence threshold for active venues except isolated recovery blips.
- `arbitrage_market_data_active_targets` is non-zero only for genuinely enabled active venues.
- `arbitrage_market_data_events_total{event="reconnecting"}` is transient rather than latched.
- Every allowed live entry is preceded by `preflight_liquidity_analysis` at the canary size of `$10` per leg.
- No quiet-market false alerts while `active_targets=0`.
- No reconnect storm, repeated snapshot-timeout churn, `ERROR`, `CRITICAL`, or `Traceback`.

## 6. Log audit

Inspect the same verification window:

```bash
cd /home/tolik1992s/labyda_next
docker compose logs --since 10m --no-color bot
```

Flag as failures:

- `ERROR`
- `CRITICAL`
- `Traceback`
- repeated `telegram_send_failed`
- repeated `polymarket_ws_snapshot_timeouts`
- repeated `websocket_market_data_stale_reconnecting`
- crash/restart loops

Separate hard failures from noisy-but-transient warnings, but do not call the rollout healthy if reconnect/staleness noise is continuous.

## 7. Backups and restore

- PostgreSQL backups remain part of the release gate.
- Keep the backup disk mounted and continue six-hour backup cadence plus periodic restore drills.
- Record restore-drill freshness before any funded-mode rollout.

## 8. Current interpretation

As of the latest closeout pass, the repo is in a repeatable shadow-deploy state on the compose VM. That is not equivalent to funded live-trading authorization. Funded rollout still requires separate approval plus venue lifecycle smoke and wallet/operator checks.
