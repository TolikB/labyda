#!/usr/bin/env bash
set -Eeuo pipefail

REPO_DIR=${REPO_DIR:-$(pwd)}
BRANCH=${BRANCH:-master}
HEALTH_RETRIES=${HEALTH_RETRIES:-120}
HEALTH_SLEEP_SECONDS=${HEALTH_SLEEP_SECONDS:-2}
DEPLOY_HEALTH_POLICY=${DEPLOY_HEALTH_POLICY:-ready}
RELEASE_SHA_FILE=${RELEASE_SHA_FILE:-.runtime/release-sha}
COMPOSE_ENV_FILE=${COMPOSE_ENV_FILE:-.env.production}

cd "${REPO_DIR}"

test -d .git || { echo "deploy_compose.sh requires a git checkout" >&2; exit 1; }
test -f docker-compose.yml || { echo "docker-compose.yml is missing" >&2; exit 1; }
test -f "${COMPOSE_ENV_FILE}" || { echo "Compose env file is missing: ${COMPOSE_ENV_FILE}" >&2; exit 1; }
test -f scripts/runtime_health_gate.py || { echo "runtime health gate is missing" >&2; exit 1; }
case "${DEPLOY_HEALTH_POLICY}" in
  ready|safe_paused_shadow) ;;
  *) echo "unsupported DEPLOY_HEALTH_POLICY: ${DEPLOY_HEALTH_POLICY}" >&2; exit 1 ;;
esac
if [[ "${DEPLOY_HEALTH_POLICY}" == "safe_paused_shadow" ]]; then
  test "${CLOB_HFT_EXECUTION_MODE:-shadow}" = "shadow" || {
    echo "safe paused deployment requires CLOB_HFT_EXECUTION_MODE=shadow" >&2
    exit 1
  }
  test "${QUOTE_ARB_EXECUTION_MODE:-shadow}" = "shadow" || {
    echo "safe paused deployment requires QUOTE_ARB_EXECUTION_MODE=shadow" >&2
    exit 1
  }
  test "${LIVE_TRADING_CONFIRM:-NO}" = "NO" || {
    echo "safe paused deployment requires LIVE_TRADING_CONFIRM=NO" >&2
    exit 1
  }
fi

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

health_target_ok() {
  local port=$1
  local runtime_instance_id=$2
  local expected_mode=$3
  python3 scripts/runtime_health_gate.py \
    --base-url "http://127.0.0.1:${port}" \
    --expected-runtime-instance-id "${runtime_instance_id}" \
    --expected-mode "${expected_mode}" \
    --accept "${DEPLOY_HEALTH_POLICY}" \
    --timeout-seconds 3
}

for _ in $(seq 1 "${HEALTH_RETRIES}"); do
  if health_target_ok 9108 clob_hft "${CLOB_HFT_EXECUTION_MODE:-shadow}" >/dev/null \
    && health_target_ok 9109 quote_arb "${QUOTE_ARB_EXECUTION_MODE:-shadow}" >/dev/null; then
    echo "compose deployment passed ${DEPLOY_HEALTH_POLICY} health policy on ${revision}"
    compose ps -a
    exit 0
  fi
  sleep "${HEALTH_SLEEP_SECONDS}"
done

echo "compose deployment failed ${DEPLOY_HEALTH_POLICY} health policy on $(git rev-parse HEAD)" >&2
health_target_ok 9108 clob_hft "${CLOB_HFT_EXECUTION_MODE:-shadow}" >&2 || true
health_target_ok 9109 quote_arb "${QUOTE_ARB_EXECUTION_MODE:-shadow}" >&2 || true
compose ps -a >&2
compose logs --no-color --tail=200 bot-clob-hft bot-quote-arb >&2 || true
exit 1
