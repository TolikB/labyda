#!/usr/bin/env bash
set -Eeuo pipefail

umask 0077
BACKUP_DIR=${BACKUP_DIR:-/var/lib/arbitrage/backups}
BACKUP_METRICS_DIR=${BACKUP_METRICS_DIR:-/var/lib/node_exporter/textfile}
BACKUP_INTERVAL_SECONDS=${BACKUP_INTERVAL_SECONDS:-21600}
BACKUP_RETENTION_DAYS=${BACKUP_RETENTION_DAYS:-14}
OFFSITE_BACKUP_DIR=${OFFSITE_BACKUP_DIR:-}
RCLONE_REMOTE=${RCLONE_REMOTE:-}

backup_once() {
  test -n "${DATABASE_URL:-}"
  install -d -m 0700 "${BACKUP_DIR}"
  install -d -m 0750 "${BACKUP_METRICS_DIR}"
  local timestamp target temporary metric_tmp checksum
  timestamp=$(date -u +%Y%m%dT%H%M%SZ)
  target="${BACKUP_DIR}/arbitrage-${timestamp}.sql.gz"
  temporary="${target}.tmp"
  metric_tmp="${BACKUP_METRICS_DIR}/arbitrage_backup.prom.tmp"
  local backup_url=${DATABASE_URL/postgresql+asyncpg:/postgresql:}
  pg_dump --no-owner --no-privileges "${backup_url}" | gzip -9 >"${temporary}"
  test -s "${temporary}"
  gzip -t "${temporary}"
  mv "${temporary}" "${target}"
  checksum=$(sha256sum "${target}" | awk '{print $1}')
  printf '%s  %s\n' "${checksum}" "$(basename "${target}")" >"${target}.sha256"
  if [[ -n ${OFFSITE_BACKUP_DIR} ]]; then
    install -d -m 0700 "${OFFSITE_BACKUP_DIR}"
    cp --preserve=timestamps "${target}" "${target}.sha256" "${OFFSITE_BACKUP_DIR}/"
  fi
  if [[ -n ${RCLONE_REMOTE} ]]; then
    command -v rclone >/dev/null
    rclone copyto "${target}" "${RCLONE_REMOTE}/$(basename "${target}")" --immutable
    rclone copyto "${target}.sha256" "${RCLONE_REMOTE}/$(basename "${target}.sha256")" --immutable
    rclone delete "${RCLONE_REMOTE}" --min-age "${BACKUP_RETENTION_DAYS}d" --include 'arbitrage-*.sql.gz*'
  fi
  find "${BACKUP_DIR}" -type f -name 'arbitrage-*.sql.gz' -mtime "+${BACKUP_RETENTION_DAYS}" -delete
  find "${BACKUP_DIR}" -type f -name 'arbitrage-*.sql.gz.sha256' -mtime "+${BACKUP_RETENTION_DAYS}" -delete
  printf 'arbitrage_postgres_backup_last_success_timestamp_seconds %s\n' "$(date +%s)" >"${metric_tmp}"
  mv "${metric_tmp}" "${BACKUP_METRICS_DIR}/arbitrage_backup.prom"
  printf 'backup=%s sha256=%s offsite=%s rclone=%s\n' \
    "${target}" "${checksum}" "${OFFSITE_BACKUP_DIR:-disabled}" "${RCLONE_REMOTE:-disabled}"
}

if [[ ${1:-} == "--loop" ]]; then
  while true; do
    backup_once
    sleep "${BACKUP_INTERVAL_SECONDS}"
  done
else
  backup_once
fi
