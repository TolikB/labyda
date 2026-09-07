# Funded Launch Plan — current four-route set

Plan of record for taking the **current** funded set to real money:
`polymarket_myriad`, `polymarket_predict`, `predict_sx`, `polymarket_sx`.

Supersedes [`PREDICT_FUN_PRODUCTION_PLAN.md`](PREDICT_FUN_PRODUCTION_PLAN.md) and the
"Remaining no-go" section of [`SX_BET_INTEGRATION_PLAN.md`](SX_BET_INTEGRATION_PLAN.md),
both of which describe a release that no longer exists — see *Why those plans were
replaced* below. They are kept for history; do not execute them.

Companion to [`PRODUCTION_RUNBOOK.md`](PRODUCTION_RUNBOOK.md). Where the two disagree,
the runbook wins.

## Current conclusion

**NO-GO**, and the blockers are evidence and infrastructure, not code.

The engine has never submitted a funded order. There is no canary artifact, no closeout
artifact, no commit and no runbook note recording a single real fill on any route. The
only closeout report on record — [`PRODUCTION_READINESS_REPORT.md`](PRODUCTION_READINESS_REPORT.md)
— is a `NO-GO` from 2026-06-28 on the retired GCP host, never superseded.
`config.production.quote_arb.json` still carries `"live_trading_confirmed": false`.

The nearest thing to a live-money path is `ops/LIVE_WALLET_ORDER_PATH.md`: two guarded
one-shot operator scripts, on two venues of four, predating the Contabo move and the
two-service split. They exercise a connector's submit call — not the engine's two-leg
entry, reservation accounting, hedge, unwind, reconciliation or settlement.

## Limits this release actually runs

Taken from `config.production.quote_arb.json`. The superseded plans quote roughly a
tenth of these; using their numbers would understate exposure by ~11x.

| Setting | Value |
|---|---|
| `execution_mode` | `canary` |
| `position_size_usd` / `max_order_size_usd` | `50.0` ($25 per leg) |
| `max_open_positions` | `5` |
| `max_total_notional_usd` | `252` |
| `max_venue_exposure_usd` | `125` |
| `max_market_exposure_usd` | `52` |
| `max_daily_loss_usd` | `10` |
| `min_venue_balance_usd` | `125` per venue |
| `min_entry_spread_pct` | `0.025` |
| funded routes | `polymarket_myriad`, `polymarket_predict`, `predict_sx`, `polymarket_sx` |
| enabled `NO-TRADE` | `predict_myriad`, `sx_myriad` |

The funded set must match `QUOTE_ARB_EXPECTED_FUNDED_ROUTES` in
`ops/production_closeout.sh` exactly; the wrapper aborts on any mismatch.

## Blockers

### B1 — No backups, no restore drill, no drain proof

`postgres-backup`, `prometheus`, `alertmanager` and `node-exporter` sit behind the
`hardening` Compose profile (`docker-compose.yml:207,228,243,270`), and **no tracked
script activates it** — `deploy_compose.sh` and `production_closeout.sh` only ever bring
up `operator`. As deployed there is no backup, no metrics scrape and no alerting.

Even once started, two mismatches remain:

- the audit reads `/mnt/arbitrage-backups` (`cli.py:67`) while the service writes to
  `/var/backups/arbitrage` (`docker-compose.yml:249`), and the `operator` container —
  which is how `production audit` runs — mounts neither;
- `spot_drain_readiness` reads a marker whose only producer, `ops/spot_preemption_watch.sh`,
  polls **GCP instance metadata**. Production is on Contabo. The marker cannot be written.

This is the highest-severity open item. PostgreSQL is the only durable record of
in-flight two-leg exposure, and `PRODUCTION_READINESS_REPORT.md:74-78` documents that
this state has already gone inconsistent once — a stuck `ACKNOWLEDGED` Myriad intent and
a `MANUAL_REVIEW` Polymarket intent producing a 103-restart storm. Losing or corrupting
it mid-window with open legs is unbounded.

`--defer-backup-gates` does not close this. It accepts the gates without running them;
the audit report now lists them under `deferred_gates` with `evaluated: false`.

### B2 — The four-route set has never been calibrated as a set

`predict_myriad` was demoted to `NO-TRADE` on 2026-09-06 (`cfbfc25c`), so the current set
is newer than the last calibration. `PRODUCTION_RUNBOOK.md:442-446` requires a fresh
exact-SHA 3600-second window with ≥10 000 valid evaluations **per funded route**, and no
such artifact exists for any SHA. Without it the audit reports
`adverse_move_calibration_missing`.

### B3 — Myriad market-data freshness never converged under load

Eighteen commits in three days (`241e8902` … `e666d8fa`) reworked the Myriad funded
refresh scheduler — on the route designated as the control. The resulting scheme
(12 reserved request slots, 50 ms pacing, deadline-ordered scheduling, one bounded
retry, coalescing) has never run under a funded window.

### B4 — Live schema coverage is thinnest where money moves

Predict.fun's authenticated endpoints skip even in the nightly workflow, because
`.github/workflows/live-schema-contracts.yml` does not supply `PREDICT_FUN_PRIVATE_KEY`.
Its websocket `predictTradingStatus` parser — which gates whether a market is executable —
broke twice within 24 hours (`cfbfc25c`, `c67eeb85`) and was diagnosed with an untracked
ad-hoc probe rather than a test. Polymarket and Myriad have no live contract for the
order or settlement paths at all.

### B5 — Capital rebalancing is unimplemented

`rebalancer.py:32` refuses by design. With a `$125` principal gate on each of four
venues, any drift between them is a manual bridge operation with no rehearsed procedure.

## Acceptance

A `GO` requires all of the following, on the same CI-verified SHA:

1. Every `PRODUCTION_RUNBOOK.md` §9 condition closed.
2. Backups running and verified, restore drill fresh, drain readiness satisfied — or an
   explicit, written, time-boxed acceptance of B1 signed off by the operator, recorded in
   the release artifact rather than implied by `--defer-backup-gates`.
3. Fresh exact-SHA calibration for all four funded routes.
4. `production-audit-final.json` with `passed: true`, `audit_scope: "post_window_paused"`,
   and a `live_canary_evidence:<route>` result for each funded route.
5. Durable pause reason `funded_canary_window_complete`;
   `SUMMARY.txt` → `post_window_state=paused_shadow_clean`.
6. No `UNKNOWN` intent, no reconciliation drift, no false `LOW VENUE BALANCE`, no
   reconnect storm, no `ERROR`/`CRITICAL`/`Traceback` noise.

### A `GO` is not proof the money path works

`cli.py:894-896` accepts `real_order_evidence` **or** `safe_no_trade`. The latter is
honest — it requires the window to have run to timeout with zero unresolved intents,
clean reconciliation, and `technical_openable_count == 0`, so a route that *was* openable
and did not fill still fails. But all four routes can pass with **zero fills** if nothing
was openable for four hours.

Record explicitly in the release artifact whether the engine submitted a real order. Until
it has, the submit → both-leg fill → reservation accounting → hedge or unwind →
reconciliation → settlement path remains exercised only by unit tests and shadow.

## Escape valve

If a route stays healthy but no natural opportunity appears for 60 minutes, mark it
`unexercised` and leave the goal open. Do not force a trade and do not close the goal on
synthetic evidence.

## Why those plans were replaced

`PREDICT_FUN_PRODUCTION_PLAN.md` (2026-08-25) targets `$10` per leg,
`max_open_positions=1`, `max_total_notional_usd=22`, and three routes including
`predict_myriad` as a funded rollout target. It also runs `--config config.production.json`,
a file that no longer exists after the two-service split. Its "final done definition" is
unreachable as written.

`SX_BET_INTEGRATION_PLAN.md:199-209` remains fully open for both SX funded routes, and
`SX_BET_V3_CUTOVER.md` instructed resuming `bot-clob-hft` for the funded canary — which
`production_closeout.sh` rejects outright, since only `quote_arb` may be funded and
`clob_hft` must carry an empty funded allowlist.
