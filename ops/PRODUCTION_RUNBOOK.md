# Production runbook — native Ubuntu 24.04 on GCP Spot e2-micro

The first release runs only Polymarket–Myriad. Predict.fun stays disabled. This is a best-effort single-instance
deployment: Spot downtime is accepted, but duplicate orders, unresolved redemption, stale restore evidence, and dirty
releases are not.

## 1. Authorization and cost gate

- Do not run live orders, fund wallets, or execute `terraform apply` without a separate explicit approval.
- Refresh GCP inventory and pricing before every rollout; old VM names, zones, IPs and estimates are invalid evidence.
- Keep the current calculator estimate at or below USD 13/month. The USD 15 budget alert is not a hard cap.
- Use `ops/gcp`; never run PostgreSQL, Prometheus, or Alertmanager in Docker on the 1 GB VM.

## 2. Provision and configure Ubuntu

After an authorized Terraform apply, connect through IAP and install Python 3.12, PostgreSQL, rclone, curl, git and the
PostgreSQL client tools. Mount the preserved disk under `/srv/arbitrage-state`; place PostgreSQL data, application state
and backups there. Then run:

```bash
sudo ./ops/configure_e2_micro.sh
sudo ./ops/install_systemd.sh
```

The e2-micro profile caps the bot at 420 MB, limits PostgreSQL memory/connections, and creates compressed zram. During
the 12-hour target stress test, reject the VM if journald shows OOM kills or sustained swap latency affects orderbook
freshness.

## 3. Secrets and wallet controls

`/etc/arbitrage/arbitrage.env` must be `root:root 0600`; `/etc/arbitrage/config.json` must be `root:arbitrage 0640` and
contain only environment placeholders for secrets. Use dedicated capped-balance trading wallets and API keys without
withdrawal permission. Configure Polygon and BNB RPC URLs, Conditional Tokens/collateral addresses, Telegram, rclone's
encrypted operator-provided remote, and `CI_VERIFIED_COMMIT_SHA`.

Keep these rollout defaults:

```json
{
  "execution_mode": "shadow",
  "scan_all": true,
  "routes": {
    "polymarket_myriad": true,
    "polymarket_predict": false,
    "predict_myriad": false
  },
  "position_size_usd": 10,
  "max_open_positions": 1,
  "max_daily_loss_usd": 10
}
```

## 4. Release and CI gate

Deploy only a clean commit whose SHA exactly equals `CI_VERIFIED_COMMIT_SHA`. CI must run PostgreSQL integration tests
without skips, Alembic `upgrade -> downgrade -> upgrade`, Docker build, pytest, mypy, ruff, compileall, pip-audit and a
secret scan. `ops/deploy_systemd.sh` rejects a dirty checkout or SHA mismatch and writes `/etc/arbitrage/release-sha`.

Docker Compose is only for CI/dev. The VM runs the Python virtualenv, local PostgreSQL and systemd directly.

## 5. Backup and recovery gate

`arbitrage-backup.timer` runs every six hours, writes SHA-256 sidecars, retains 14 days and copies to `RCLONE_REMOTE`.
Run and record an isolated restore at least every 30 days:

```bash
sudo systemctl start arbitrage-backup.service
sudo -u arbitrage /opt/arbitrage/current/ops/postgres_restore_drill.sh
```

The drill writes `/var/lib/arbitrage/restore-drill.json`. Canary is forbidden when this marker is missing/stale, the
newest backup is older than eight hours, or its checksum fails.

## 6. Drain, restart and preemption drills

The drain sequence persists the risk pause before cancelling orders, performs full reconciliation, refuses success
with unresolved order/redemption intents, and writes a readiness marker:

```bash
sudo -u arbitrage /opt/arbitrage/current/.venv/bin/arbitrage-admin \
  --config /etc/arbitrage/config.json production drain --reason "operator drill"
```

Test process kill, PostgreSQL restart, network loss and Spot preemption. Every case must restart paused, reconcile
before execution, retain the advisory lock invariant and create no duplicate order or redemption transaction.

## 7. Shadow, lifecycle and canary gates

Run `scan_all=true` in shadow for 24 hours. Require `tradable > 0`, `missing_routes=[]`, reviewed mappings, stable
readiness, zero UNKNOWN intents, zero reconciliation drift, no ERROR/CRITICAL logs, controlled 429 retries, no
sustained `ArbitrageBookStale` alert for venues with active targets, and no stale-book execution attempt.

Before canary, run the passive gate:

```bash
sudo -u arbitrage /opt/arbitrage/current/.venv/bin/arbitrage-admin \
  --config /etc/arbitrage/config.json production verify \
  --backup-dir /var/lib/arbitrage/backups
```

With separate authorization, execute at most USD 1 place/cancel/zero-fill smoke per venue, read-only settlement checks,
and one minimum redemption. Repeating the same redemption idempotency ID must not broadcast a second transaction.

Canary lasts 72 hours: USD 10 total, USD 5 per leg, one position, USD 10 daily loss. Wait for a natural profitable
opportunity. Keep these limits for the first seven live days. Any UNKNOWN order, settlement mismatch, restore failure,
Spot recovery failure, or reconciliation drift returns the system to shadow and requires manual review.
