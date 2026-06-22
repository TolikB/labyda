# PostgreSQL backup and restore

Production backups are generated every six hours by `ops/postgres_backup.sh`, retained for 14 days, and accompanied by
SHA-256 sidecars. Configure `RCLONE_REMOTE` as an operator-owned encrypted OneDrive/rclone destination; local copies on
the preserved state disk are not sufficient.

## Verify a backup

```bash
gzip -t /var/lib/arbitrage/backups/arbitrage-YYYYMMDDTHHMMSSZ.sql.gz
```

## Restore drill

Never restore over the production database. The drill script creates an isolated timestamped database, restores the latest
backup, verifies the Alembic revision and public tables, then removes the temporary database even if validation fails.

```bash
sudo -u arbitrage /opt/arbitrage/current/ops/postgres_restore_drill.sh
```

Pass an explicit in-container backup path as the first argument when the latest backup is not the intended restore point.

The drill writes `/var/lib/arbitrage/restore-drill.json`. `production verify` rejects canary when this marker is older
than 30 days. Record the backup name, SHA-256, restore duration, migration revision, and operator in the deployment log.
