#!/usr/bin/env bash
set -Eeuo pipefail

REPO_DIR=${REPO_DIR:-$(pwd)}
BRANCH=${BRANCH:-master}
HEALTH_URLS=${HEALTH_URLS:-http://127.0.0.1:9108/health/ready http://127.0.0.1:9109/health/ready}
HEALTH_RETRIES=${HEALTH_RETRIES:-120}
HEALTH_SLEEP_SECONDS=${HEALTH_SLEEP_SECONDS:-2}
RELEASE_SHA_FILE=${RELEASE_SHA_FILE:-.runtime/release-sha}
COMPOSE_ENV_FILE=${COMPOSE_ENV_FILE:-.env.production}

cd "${REPO_DIR}"

test -d .git || { echo "deploy_compose.sh requires a git checkout" >&2; exit 1; }
test -f docker-compose.yml || { echo "docker-compose.yml is missing" >&2; exit 1; }
test -f "${COMPOSE_ENV_FILE}" || { echo "Compose env file is missing: ${COMPOSE_ENV_FILE}" >&2; exit 1; }

compose() {
  docker compose --env-file "${COMPOSE_ENV_FILE}" "$@"
}

tracked_changes=$(git status --porcelain --untracked-files=no)
test -z "${tracked_changes}" || { echo "deployment requires a clean tracked worktree" >&2; exit 1; }

git fetch --prune origin "${BRANCH}"
git checkout "${BRANCH}"
git pull --ff-only origin "${BRANCH}"

revision=$(git rev-parse HEAD)
test -n "${CI_VERIFIED_COMMIT_SHA:-}" || { echo "CI_VERIFIED_COMMIT_SHA is required" >&2; exit 1; }
test "${revision}" = "${CI_VERIFIED_COMMIT_SHA}" || {
  echo "refusing unverified revision ${revision}; CI verified ${CI_VERIFIED_COMMIT_SHA}" >&2
  exit 1
}
install -d -m 0755 "$(dirname "${RELEASE_SHA_FILE}")"
printf '%s\n' "${revision}" >"${RELEASE_SHA_FILE}"
chmod 0644 "${RELEASE_SHA_FILE}"

compose run --rm migrate
compose up -d --build bot-clob-hft bot-quote-arb

for _ in $(seq 1 "${HEALTH_RETRIES}"); do
  ready=1
  for url in ${HEALTH_URLS}; do
    if ! curl --silent --show-error --fail --max-time 3 "${url}" >/dev/null; then
      ready=0
      break
    fi
  done
  if [[ ${ready} -eq 1 ]]; then
    echo "compose deployment is ready on ${revision}"
    compose ps -a
    exit 0
  fi
  sleep "${HEALTH_SLEEP_SECONDS}"
done

echo "compose deployment failed readiness on $(git rev-parse HEAD)" >&2
compose ps -a >&2
compose logs --no-color --tail=200 bot-clob-hft bot-quote-arb >&2 || true
exit 1
