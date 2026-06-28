#!/usr/bin/env bash
set -Eeuo pipefail

REPO_DIR=${REPO_DIR:-$(pwd)}
BRANCH=${BRANCH:-master}
HEALTH_URL=${HEALTH_URL:-http://127.0.0.1:9108/health/ready}
HEALTH_RETRIES=${HEALTH_RETRIES:-30}
HEALTH_SLEEP_SECONDS=${HEALTH_SLEEP_SECONDS:-2}

cd "${REPO_DIR}"

test -d .git || { echo "deploy_compose.sh requires a git checkout" >&2; exit 1; }
test -f docker-compose.yml || { echo "docker-compose.yml is missing" >&2; exit 1; }

tracked_changes=$(git status --porcelain --untracked-files=no)
test -z "${tracked_changes}" || { echo "deployment requires a clean tracked worktree" >&2; exit 1; }

git fetch --prune origin "${BRANCH}"
git checkout "${BRANCH}"
git pull --ff-only origin "${BRANCH}"

docker compose run --rm migrate
docker compose up -d --build bot

for _ in $(seq 1 "${HEALTH_RETRIES}"); do
  if curl --silent --show-error --fail --max-time 3 "${HEALTH_URL}" >/dev/null; then
    echo "compose deployment is ready on $(git rev-parse HEAD)"
    docker compose ps -a
    exit 0
  fi
  sleep "${HEALTH_SLEEP_SECONDS}"
done

echo "compose deployment failed readiness on $(git rev-parse HEAD)" >&2
docker compose ps -a >&2
docker compose logs --no-color --tail=200 bot >&2 || true
exit 1
