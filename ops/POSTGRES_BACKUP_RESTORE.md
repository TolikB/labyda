# PostgreSQL backup and restore

Production backups are generated every six hours by `ops/postgres_backup.sh`, retained for 14 days, and accompanied by
SHA-256 sidecars. Under the current approved budget profile, keep them on the
main VM in `/mnt/arbitrage-backups`; do not add a separate backup disk or paid
offsite service as part of the standard closeout path.

## Verify a backup

```bash
gzip -t /mnt/arbitrage-backups/arbitrage-YYYYMMDDTHHMMSSZ.sql.gz
```

## Restore drill

Never restore over the production database. The drill script creates an isolated timestamped database, restores the latest
backup, verifies the Alembic revision and public tables, then removes the temporary database even if validation fails.

```bash
cd /home/tolik1992s/labyda_next
docker compose exec postgres-backup /opt/arbitrage/postgres_restore_drill.sh
```

Pass an explicit in-container backup path as the first argument when the latest backup is not the intended restore point.

The drill writes `/mnt/arbitrage-backups/restore-drill.json` in the same local
backup directory.
`production verify`/`production audit` reject canary when this marker is older than 30 days. Record the backup name,
SHA-256, restore duration, migration revision, and operator in the deployment log.
