#!/usr/bin/env bash
set -Eeuo pipefail

APP_ROOT=${APP_ROOT:-/opt/arbitrage}
REPO_DIR=${REPO_DIR:-${APP_ROOT}/repo}
RELEASES_DIR=${RELEASES_DIR:-${APP_ROOT}/releases}
CURRENT_LINK=${CURRENT_LINK:-${APP_ROOT}/current}
ENV_FILE=${ENV_FILE:-/etc/arbitrage/arbitrage.env}
SERVICE=${SERVICE:-arbitrage-engine.service}
HEALTH_URL=${HEALTH_URL:-http://127.0.0.1:9108/health/ready}
BACKUP_DIR=${BACKUP_DIR:-/var/lib/arbitrage/backups}
LOCK_FILE=${LOCK_FILE:-/run/lock/arbitrage-deploy.lock}

if [[ ${EUID} -ne 0 ]]; then
  echo "deploy_systemd.sh must run as root" >&2
  exit 1
fi

exec 9>"${LOCK_FILE}"
flock -n 9 || { echo "another deployment is already running" >&2; exit 1; }

test -d "${REPO_DIR}/.git"
test -f "${ENV_FILE}"
test -f /etc/arbitrage/config.json
test -z "$(git -C "${REPO_DIR}" status --porcelain)" || { echo "deployment requires a clean worktree" >&2; exit 1; }
set -a
# shellcheck disable=SC1090
source "${ENV_FILE}"
set +a
install -d -o arbitrage -g arbitrage -m 0750 "${RELEASES_DIR}" /var/lib/arbitrage "${BACKUP_DIR}"

git -C "${REPO_DIR}" fetch --prune origin master
git -C "${REPO_DIR}" checkout master
git -C "${REPO_DIR}" pull --ff-only origin master
revision=$(git -C "${REPO_DIR}" rev-parse HEAD)
test -n "${CI_VERIFIED_COMMIT_SHA:-}" || { echo "CI_VERIFIED_COMMIT_SHA is required" >&2; exit 1; }
test "${revision}" = "${CI_VERIFIED_COMMIT_SHA}" || {
  echo "refusing unverified revision ${revision}; CI verified ${CI_VERIFIED_COMMIT_SHA}" >&2
  exit 1
}
release="${RELEASES_DIR}/${revision}"
previous=$(readlink -f "${CURRENT_LINK}" || true)

if [[ ! -d "${release}" ]]; then
  install -d -o arbitrage -g arbitrage -m 0750 "${release}"
  git -C "${REPO_DIR}" archive "${revision}" | tar -x -C "${release}"
  python3.12 -m venv "${release}/.venv"
  "${release}/.venv/bin/python" -m pip install --require-hashes --no-deps -r "${release}/requirements.lock"
  "${release}/.venv/bin/python" -m pip install --no-deps "${release}"
fi

if [[ -n ${DATABASE_URL:-} ]] && command -v pg_dump >/dev/null 2>&1; then
  backup_url=${DATABASE_URL/postgresql+asyncpg:/postgresql:}
  pg_dump "${backup_url}" | gzip -9 >"${BACKUP_DIR}/predeploy-${revision}-$(date -u +%Y%m%dT%H%M%SZ).sql.gz"
fi

(
  cd "${release}"
  "${release}/.venv/bin/alembic" upgrade head
)

ln -sfn "${release}" "${CURRENT_LINK}.new"
mv -Tf "${CURRENT_LINK}.new" "${CURRENT_LINK}"
printf '%s\n' "${revision}" >/etc/arbitrage/release-sha
chown root:root /etc/arbitrage/release-sha
chmod 0644 /etc/arbitrage/release-sha
systemctl daemon-reload
systemctl restart "${SERVICE}"

for _ in $(seq 1 30); do
  if curl --silent --show-error --fail --max-time 2 "${HEALTH_URL}" >/dev/null; then
    echo "deployment ${revision} is ready"
    exit 0
  fi
  sleep 2
done

echo "readiness failed for ${revision}; rolling application symlink back" >&2
if [[ -n ${previous} && -d ${previous} ]]; then
  ln -sfn "${previous}" "${CURRENT_LINK}.new"
  mv -Tf "${CURRENT_LINK}.new" "${CURRENT_LINK}"
  systemctl restart "${SERVICE}"
fi
exit 1
