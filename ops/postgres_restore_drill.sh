#!/usr/bin/env bash
set -Eeuo pipefail

umask 0077
BACKUP_DIR=${BACKUP_DIR:-/var/lib/arbitrage/backups}
RESTORE_MARKER=${RESTORE_MARKER:-/var/lib/arbitrage/restore-drill.json}
backup_path=${1:-}

test -n "${DATABASE_URL:-}"
if [[ -z ${backup_path} ]]; then
  backup_path=$(find "${BACKUP_DIR}" -maxdepth 1 -type f -name 'arbitrage-*.sql.gz' -print | sort | tail -n 1)
fi
test -n "${backup_path}"
test -s "${backup_path}"
gzip -t "${backup_path}"

backup_url=${DATABASE_URL/postgresql+asyncpg:/postgresql:}
admin_url=${backup_url%/*}/postgres
restore_db="arbitrage_restore_$(date -u +%Y%m%dT%H%M%SZ)_$$"
restore_url=${backup_url%/*}/${restore_db}

cleanup() {
  dropdb --if-exists --force --maintenance-db="${admin_url}" "${restore_db}" >/dev/null
}
trap cleanup EXIT

createdb --maintenance-db="${admin_url}" "${restore_db}"
gzip -cd "${backup_path}" | psql --set=ON_ERROR_STOP=1 "${restore_url}" >/dev/null

revision=$(psql --no-align --tuples-only "${restore_url}" -c 'select version_num from alembic_version')
table_count=$(psql --no-align --tuples-only "${restore_url}" -c \
  "select count(*) from information_schema.tables where table_schema = 'public'")

test -n "${revision}"
test "${table_count}" -gt 0
checksum=$(sha256sum "${backup_path}" | awk '{print $1}')
install -d -m 0750 "$(dirname "${RESTORE_MARKER}")"
marker_tmp="${RESTORE_MARKER}.tmp"
printf '{"completed_at":"%s","backup":"%s","sha256":"%s","revision":"%s","table_count":%s}\n' \
  "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "${backup_path}" "${checksum}" "${revision}" "${table_count}" >"${marker_tmp}"
chmod 0640 "${marker_tmp}"
mv "${marker_tmp}" "${RESTORE_MARKER}"
printf 'restore_drill=ok backup=%s revision=%s tables=%s\n' \
  "${backup_path}" "${revision}" "${table_count}"
