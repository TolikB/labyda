#!/usr/bin/env bash
set -Eeuo pipefail

umask 0077
BACKUP_DIR=${BACKUP_DIR:-/var/lib/arbitrage/backups}
BACKUP_METRICS_DIR=${BACKUP_METRICS_DIR:-/var/lib/node_exporter/textfile}
BACKUP_INTERVAL_SECONDS=${BACKUP_INTERVAL_SECONDS:-21600}
BACKUP_RETENTION_DAYS=${BACKUP_RETENTION_DAYS:-14}

backup_once() {
  test -n "${DATABASE_URL:-}"
  install -d -m 0700 "${BACKUP_DIR}"
  install -d -m 0750 "${BACKUP_METRICS_DIR}"
  local timestamp target temporary metric_tmp
  timestamp=$(date -u +%Y%m%dT%H%M%SZ)
  target="${BACKUP_DIR}/arbitrage-${timestamp}.sql.gz"
  temporary="${target}.tmp"
  metric_tmp="${BACKUP_METRICS_DIR}/arbitrage_backup.prom.tmp"
  local backup_url=${DATABASE_URL/postgresql+asyncpg:/postgresql:}
  pg_dump --no-owner --no-privileges "${backup_url}" | gzip -9 >"${temporary}"
  test -s "${temporary}"
  mv "${temporary}" "${target}"
  find "${BACKUP_DIR}" -type f -name 'arbitrage-*.sql.gz' -mtime "+${BACKUP_RETENTION_DAYS}" -delete
  printf 'arbitrage_postgres_backup_last_success_timestamp_seconds %s\n' "$(date +%s)" >"${metric_tmp}"
  mv "${metric_tmp}" "${BACKUP_METRICS_DIR}/arbitrage_backup.prom"
  printf 'backup=%s\n' "${target}"
}

if [[ ${1:-} == "--loop" ]]; then
  while true; do
    backup_once
    sleep "${BACKUP_INTERVAL_SECONDS}"
  done
else
  backup_once
fi
