# Polymarket–Myriad production readiness report

Date: 2026-06-21

## Verdict

`NO-GO` for funded production deployment. The code hardening is complete locally, but PostgreSQL/Docker/Terraform CI,
current GCP inventory, target stress/drills, 24-hour shadow, venue lifecycle smoke, and explicitly authorized redemption
remain mandatory external gates.

## Implemented

- Restart-safe Conditional Tokens settlement with durable unique redemption intents, receipt reconciliation, non-zero
  exposure checks, fail-closed UNKNOWN handling, and no blind retry ambiguity window.
- Alembic `0002_redemption_intents`; monetary ORM/migration columns remain `Numeric(38,18)`.
- `ExecutionReport`, `PositionPlan`, `OpenPosition`, fees and realized PnL use `Decimal`; persisted JSON uses strings.
- Polymarket–Myriad-only example configuration, Polygon/BNB settlement and gas settings, CI SHA deployment gate, and
  `arbitrage-admin production drain --reason ...`.
- Native Ubuntu/systemd e2-micro profile, PostgreSQL memory tuning, zram, Spot preemption drain, readiness watchdog,
  stateful-disk Terraform, six-hour checksummed backup, rclone offsite copy, and restore freshness marker.
- `production verify` covers exact migration head, advisory lock, CI SHA, backup checksum/freshness, restore/drain
  freshness, reconciliation, unresolved order/redemption intents, redemption support, settlement metadata/status,
  collateral and native gas balances.

## Local evidence

- `pytest`: 210 passed, 6 skipped; skips require PostgreSQL/external integration infrastructure.
- `mypy src tests`: passed.
- `ruff check src tests`: passed.
- `compileall src tests`: passed.
- `git diff --check`: passed.
- Deterministic accelerated `scan_all=true` five-minute simulation: passed; expected Gamma/discovery/snapshot events and
  no `ValueError`/traceback.
- Secret-pattern scan: no matches.

## Remaining release gates

| Severity | Gate | Required evidence |
| --- | --- | --- |
| Blocker | CI/tooling | PostgreSQL tests without skips, migration cycle, Docker build/Compose, Terraform validate, pip-audit, secret scan |
| Blocker | GCP inventory/cost | Fresh `gcloud` inventory and official estimate at or below USD 13/month |
| Blocker | Target runtime | 12-hour e2-micro stress with no OOM/crash-loop/advisory-lock loss |
| Blocker | Failure drills | Process kill, PostgreSQL restart, network loss and Spot preemption recover paused without duplicate orders |
| Blocker | Shadow | 24 hours with tradable routes, stable readiness, no UNKNOWN/drift/ERROR/CRITICAL/stale execution |
| Blocker | Venue lifecycle | Authorized <=USD 1 place/cancel smoke per venue and one idempotent minimum redemption |
| High | Polymarket custody | Direct redemption is intentionally blocked when configured funder differs from signer; validate wallet model |

Live orders, wallet funding and `terraform apply` were not executed and still require explicit operator authorization.
