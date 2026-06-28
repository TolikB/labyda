# Polymarket-Myriad production closeout report

Date: 2026-06-28

## Verdict

`NO-GO` for repeatable canary rollout on the live VM.

`GO` for local code changes, mapping approval, and Docker Compose deployment
mechanics.

`NO-GO` for any funded canary or live trading until startup reconciliation and
bot process stability are fixed on the VM.

## Confirmed live deployment shape

- VM runtime is Docker Compose under `/home/tolik1992s/labyda_next`.
- The active service is `labyda_next-bot-1`.
- The standard rollout command is `./ops/deploy_compose.sh` from that checkout.
- The standard passive verification command is
  `DURATION_SECONDS=900 ./ops/shadow_smoke.sh`.

## Closeout evidence

- Local `master` was committed and pushed with the canary/liquidity change set:
  - `79d39f1` `Tighten canary spread and liquidity preflight`
- The code and tracked docs now align on:
  - `5%` entry and retry thresholds
  - canary `position_size_usd=20.0` for `$10` per leg
  - structured `preflight_liquidity_analysis` and
    `preflight_liquidity_rejected` events in execution preflight
- VM mapping gate passed after operator approval:
  - `enabled_route_coverage.polymarket_myriad.has_verified=true`
  - `127` total `VERIFIED` mappings for `polymarket_myriad`
- VM config was switched to canary parameters on the authoritative checkout:
  - `execution_mode=canary`
  - `position_size_usd=20.0`
  - `max_order_size_usd=20.0`
  - `min_entry_spread_pct=0.05`
  - `min_retry_spread_pct=0.05`
  - `max_open_positions=1`
  - `max_daily_loss_usd=10.0`
- `deploy_compose.sh` rebuilt and restarted the stack, but readiness did not
  recover.
- The required 15-minute smoke artifacts were captured at:
  - `/home/tolik1992s/labyda_next/shadow-smoke-artifacts/20260628T125206Z`
- After the failed canary attempt, the VM was restored to the previous shadow
  config from `config.production.backup.20260628T124247Z.json`.
- Post-rollback checks returned:
  - `/health/live`: HTTP 200
  - `/health/ready`: HTTP 200
  - `missing_routes=[]`

## Blocking findings

- `/health/live` and `/health/ready` were unstable during the entire 15-minute
  window. `samples.tsv` stayed empty because repeated endpoint calls ended in
  `Recv failure`, `Empty reply`, or `Failed to connect`.
- `arbitrage_ready` stayed `0.0` for all `61` metric snapshots.
- `/health/ready` reported persistent `503` with:
  - `risk_paused:startup reconciliation failed`
  - `discovery_not_ready`
- The startup reconciliation failure is deterministic:
  - venue `Myriad`
  - `404` on `GET /orders/venue-019ee89e-21ff-79e5-b865-a2022f99e368`
  - unresolved durable intent:
    - `client_order_id=019ee89e-21ff-79e5-b865-a2022f99e368`
    - `market_key=restart:019ee89e-21ff-79e5-b865-a2022f99e368`
    - `status=ACKNOWLEDGED`
- Durable state is inconsistent even though live exposure is zero:
  - `positions` table: `0` rows
  - `order_intents` still contains:
    - one `ACKNOWLEDGED` Myriad restart intent
    - one `MANUAL_REVIEW` Polymarket integration intent without `venue_order_id`
- The bot entered a restart storm during verification:
  - `docker inspect` reported `RestartCount=103`
  - recent container logs repeatedly ended with
    `RuntimeError: Startup reconciliation failed`
- No execution path was exercised:
  - `preflight_liquidity_analysis=0`
  - `preflight_liquidity_rejected=0`
  - `execution_pipeline_latency=0`
  - no real canary entry was attempted because readiness never became healthy
- Log audit still found error-class noise in the same window:
  - repeated `Traceback` lines (`265` in captured `bot.log`)
  - repeated `venue_reconciliation_failed` for Myriad
  - repeated shutdown-time logging errors with unclosed aiohttp sessions
  - repeated Polymarket auth noise:
    - `POST https://clob.polymarket.com/auth/api-key` -> `400 Bad Request`

## Why the rollout was previously noisy

- The live VM path was ambiguous between an old systemd assumption and the actual Docker Compose stack.
- The compose directory was not initially a git checkout, which made fast-forward deploys non-repeatable.
- Gamma bulk refresh could fail on duplicate market IDs even when a safe deduplicated snapshot was possible.

Those conditions were addressed, but the current blocker moved to durable
reconciliation state and startup stability.

## Remaining follow-up goal

Keep driving the deployed `master` toward a durable production-closeout state
where every repeat 15-minute canary smoke can actually stay up and remain ready
without operator interpretation:

- Resolve stale restart/integration order intents so startup reconciliation can
  succeed without manual DB cleanup.
- Keep readiness free of disabled-route or disabled-venue pollution.
- Eliminate bot restart storms and shutdown-time aiohttp/logging noise.
- Reduce or explicitly explain recurrent Polymarket auth noise during startup
  reconciliation.
- Preserve the Docker Compose deploy path as the authoritative VM workflow in
  docs and operations.
