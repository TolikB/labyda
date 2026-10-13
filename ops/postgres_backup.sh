#!/usr/bin/env bash
set -Eeuo pipefail

umask 0077
BACKUP_DIR=${BACKUP_DIR:-/var/lib/arbitrage/backups}
BACKUP_METRICS_DIR=${BACKUP_METRICS_DIR:-/var/lib/node_exporter/textfile}
BACKUP_INTERVAL_SECONDS=${BACKUP_INTERVAL_SECONDS:-21600}
BACKUP_RETENTION_DAYS=${BACKUP_RETENTION_DAYS:-14}
OFFSITE_BACKUP_DIR=${OFFSITE_BACKUP_DIR:-}
# The offsite copy ages out on its own clock. On 2026-10-13 it had never been
# pruned at all: 123 dumps, 6.5 GB of a 38 GB disk it shared with the primary.
OFFSITE_BACKUP_RETENTION_DAYS=${OFFSITE_BACKUP_RETENTION_DAYS:-${BACKUP_RETENTION_DAYS}}
RCLONE_REMOTE=${RCLONE_REMOTE:-}

# Only this script's own timestamped dumps age out. Anything else in these
# directories -- a migration snapshot, a restore drill -- was put there by a
# person, and deleting it is not this script's call.
prune_backups() {
  local directory=$1 retention_days=$2
  find "${directory}" -maxdepth 1 -type f -regextype posix-extended \
    -regex '.*/arbitrage-[0-9]{8}T[0-9]{6}Z\.sql\.gz(\.sha256)?' \
    -mtime "+${retention_days}" -delete
}

backup_once() {
  test -n "${DATABASE_URL:-}"
  install -d -m 0755 "${BACKUP_DIR}"
  chmod 0755 "${BACKUP_DIR}"
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
  chmod 0644 "${target}"
  checksum=$(sha256sum "${target}" | awk '{print $1}')
  printf '%s  %s\n' "${checksum}" "$(basename "${target}")" >"${target}.sha256"
  chmod 0644 "${target}.sha256"
  if [[ -n ${OFFSITE_BACKUP_DIR} ]]; then
    install -d -m 0755 "${OFFSITE_BACKUP_DIR}"
    chmod 0755 "${OFFSITE_BACKUP_DIR}"
    cp --preserve=timestamps "${target}" "${target}.sha256" "${OFFSITE_BACKUP_DIR}/"
    chmod 0644 "${OFFSITE_BACKUP_DIR}/$(basename "${target}")"
    chmod 0644 "${OFFSITE_BACKUP_DIR}/$(basename "${target}.sha256")"
  fi
  if [[ -n ${RCLONE_REMOTE} ]]; then
    command -v rclone >/dev/null
    rclone copyto "${target}" "${RCLONE_REMOTE}/$(basename "${target}")" --immutable
    rclone copyto "${target}.sha256" "${RCLONE_REMOTE}/$(basename "${target}.sha256")" --immutable
    rclone delete "${RCLONE_REMOTE}" --min-age "${BACKUP_RETENTION_DAYS}d" --include 'arbitrage-*.sql.gz*'
  fi
  prune_backups "${BACKUP_DIR}" "${BACKUP_RETENTION_DAYS}"
  if [[ -n ${OFFSITE_BACKUP_DIR} ]]; then
    prune_backups "${OFFSITE_BACKUP_DIR}" "${OFFSITE_BACKUP_RETENTION_DAYS}"
  fi
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
