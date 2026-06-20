# Production runbook — Windows VM with WSL2 and Docker

This runbook keeps order submission disabled until every preflight gate passes. The first production route is
Polymarket–Myriad only.

## 1. Prepare the VM

Run in an elevated PowerShell session, reboot when requested, and install Docker Desktop after Ubuntu is available:

```powershell
wsl --install -d Ubuntu-24.04
wsl --update
wsl --shutdown
```

Move the Ubuntu distribution to `F:\WSL\Ubuntu` with `wsl --manage Ubuntu-24.04 --move F:\WSL\Ubuntu` when supported by
the installed WSL version. In Docker Desktop, set **Settings → Resources → Advanced → Disk image location** to
`F:\DockerData`. Verify that both locations are on `F:` before creating database volumes.

Clone or copy the repository into the Linux filesystem, for example `~/arbitrage`; do not run production from the
OneDrive checkout. Keep at least 30 GB free for images, PostgreSQL, metrics, and backups.

## 2. Configure secrets and limits

Inside WSL:

```bash
cd ~/arbitrage
cp .env.example .env.production
cp config.example.json config.production.json
cp ops/alertmanager.example.yml /etc/arbitrage-alertmanager.yml
chmod 0600 .env.production config.production.json /etc/arbitrage-alertmanager.yml
```

Set unique PostgreSQL credentials, venue keys without withdrawal permission, Telegram values, and an external disk or
network path in `OFFSITE_BACKUP_DIR`. Keep `LIVE_TRADING_CONFIRM=NO` and `execution_mode=shadow` initially. Configure:

```json
{
  "execution_mode": "shadow",
  "scan_all": true,
  "routes": {
    "polymarket_myriad": true,
    "polymarket_predict": false,
    "predict_myriad": false
  },
  "position_size_usd": 10.0,
  "max_open_positions": 1,
  "max_daily_loss_usd": 10.0
}
```

`POLYMARKET_FUNDER_ADDRESS` is mandatory only for non-EOA signature types. Confirm that `signature_type=0` is correct
for an EOA account; otherwise set the actual funder address before any live test.

## 3. Build and validate

```bash
export ALERTMANAGER_CONFIG_FILE=/etc/arbitrage-alertmanager.yml
export OFFSITE_BACKUP_DIR=/mnt/offsite/arbitrage
docker compose config --quiet
docker compose build --pull
docker compose up -d postgres
docker compose run --rm migrate
docker compose --profile test build test
docker compose --profile test run --rm test -m pytest -q
docker compose --profile test run --rm test -m mypy src tests
docker compose --profile test run --rm test -m ruff check src tests
docker compose --profile test run --rm test -m compileall -q src tests
```

The `test` target uses `requirements-dev.lock`; the production runtime image does not contain test tools. CI must pass
all PostgreSQL integration tests and the Alembic upgrade → downgrade → upgrade cycle before deployment.

## 4. Discovery and 24-hour shadow soak

```bash
docker compose run --rm bot --config /run/config/config.json --once
docker compose run --rm --entrypoint arbitrage-admin bot \
  --config /run/config/config.json discovery audit
docker compose up -d
curl --fail http://127.0.0.1:9108/health/ready
```

The one-shot command must fail when no complete route exists. Do not proceed until `tradable > 0`,
`missing_routes=[]`, mappings are reviewed, and the 24-hour soak has no reconciliation drift, UNKNOWN intents, 429
bursts, stale books, or risk pause.

Approve reviewed mappings explicitly:

```bash
docker compose run --rm --entrypoint arbitrage-admin bot \
  --config /run/config/config.json mappings list
docker compose run --rm --entrypoint arbitrage-admin bot \
  --config /run/config/config.json mappings approve MAPPING_ID --operator OPERATOR
```

## 5. Backup and restore gate

Trigger a backup and verify both local and off-VM copies:

```bash
docker compose exec postgres-backup /bin/bash /opt/arbitrage/postgres_backup.sh
gzip -t "$(ls -1t "${OFFSITE_BACKUP_DIR}"/arbitrage-*.sql.gz | head -1)"
(cd "${OFFSITE_BACKUP_DIR}" && sha256sum -c "$(basename "$(ls -1t arbitrage-*.sha256 | head -1)")")
```

Complete the isolated restore procedure in `ops/POSTGRES_BACKUP_RESTORE.md` and record backup SHA-256, restore duration,
migration revision, and operator.

## 6. Passive smoke and capped canary

Run the preflight first. It never submits an order:

```bash
docker compose run --rm --entrypoint arbitrage-admin bot \
  --config /run/config/config.json production verify --backup-dir /var/backups/offsite
```

For the explicitly authorized $1 Myriad lifecycle test:

```bash
export LIVE_SMOKE_CONFIRM=YES
docker compose run --rm -e LIVE_SMOKE_CONFIRM=YES --entrypoint python bot \
  scripts/live_smoke_myriad.py --config /run/config/config.json --market-id MARKET_ID \
  --max-notional-usd 1 --confirm-live-smoke
```

After the passive order is confirmed cancelled with zero fill, change only:

```text
execution_mode=canary
LIVE_TRADING_CONFIRM=YES
```

Keep $10 total position size ($5 maximum per leg), one open position, and $10 daily loss. Allow up to 72 hours for one
natural valid opportunity; never force an unprofitable fill for acceptance testing.

## 7. Incident response and rollback

```bash
docker compose run --rm --entrypoint arbitrage-admin bot \
  --config /run/config/config.json risk pause --reason "operator emergency stop"
docker compose run --rm --entrypoint arbitrage-admin bot \
  --config /run/config/config.json orders cancel-all --confirm YES
docker compose run --rm --entrypoint arbitrage-admin bot \
  --config /run/config/config.json reconcile
```

Confirm zero venue orders, zero unresolved intents, zero unexpected exposure, and a clean reconciliation report. Set
`execution_mode=shadow`, set `LIVE_TRADING_CONFIRM=NO`, restart the bot, and keep reconciliation/position management
running. Rotate a venue key immediately after suspected disclosure; never reuse a key that had withdrawal permission.
