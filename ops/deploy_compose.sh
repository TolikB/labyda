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

compose() {
  docker compose --env-file "${COMPOSE_ENV_FILE}" "$@"
}

case "${DEPLOY_HEALTH_POLICY}" in
  ready|safe_paused_shadow) ;;
  *) echo "unsupported DEPLOY_HEALTH_POLICY: ${DEPLOY_HEALTH_POLICY}" >&2; exit 1 ;;
esac

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

# Bash may continue executing the pre-pull file after git replaces this script.
# Re-exec once from the verified checkout so deployment policy always comes from
# the exact revision being deployed.
if [[ "${ARBITRAGE_DEPLOY_SCRIPT_REEXEC_SHA:-}" != "${revision}" ]]; then
  export ARBITRAGE_DEPLOY_SCRIPT_REEXEC_SHA=${revision}
  exec bash "${BASH_SOURCE[0]}"
fi

# Read only the allowlisted runtime controls from the exact verified checkout's
# fully resolved Compose model. Shell defaults alone do not account for values
# supplied by --env-file.
mapfile -t resolved_runtime_controls < <(
  compose config --format json | python3 -c '
import json
import sys

services = json.load(sys.stdin)["services"]
for service_name in ("bot-clob-hft", "bot-quote-arb"):
    environment = services[service_name].get("environment", {})
    print(environment.get("ARBITRAGE_EXECUTION_MODE_OVERRIDE", ""))
    print(environment.get("LIVE_TRADING_CONFIRM", ""))
'
)
test "${#resolved_runtime_controls[@]}" -eq 4 || {
  echo "could not resolve runtime safety controls from Compose" >&2
  exit 1
}
resolved_clob_mode=${resolved_runtime_controls[0]}
resolved_clob_confirm=${resolved_runtime_controls[1]}
resolved_quote_mode=${resolved_runtime_controls[2]}
resolved_quote_confirm=${resolved_runtime_controls[3]}

if [[ "${DEPLOY_HEALTH_POLICY}" == "safe_paused_shadow" ]]; then
  test "${resolved_clob_mode}" = "shadow" || {
    echo "safe paused deployment requires resolved clob_hft mode=shadow" >&2
    exit 1
  }
  test "${resolved_quote_mode}" = "shadow" || {
    echo "safe paused deployment requires resolved quote_arb mode=shadow" >&2
    exit 1
  }
  test "${resolved_clob_confirm}" = "NO" && test "${resolved_quote_confirm}" = "NO" || {
    echo "safe paused deployment requires resolved LIVE_TRADING_CONFIRM=NO" >&2
    exit 1
  }
fi

install -d -m 0755 "$(dirname "${RELEASE_SHA_FILE}")"
printf '%s\n' "${revision}" >"${RELEASE_SHA_FILE}"
chmod 0644 "${RELEASE_SHA_FILE}"

# Fence trading before schema changes. Any migration or pause failure leaves both
# runtimes stopped instead of trading against a partially migrated database.
compose stop bot-clob-hft bot-quote-arb
compose run --rm migrate
if [[ "${DEPLOY_HEALTH_POLICY}" == "safe_paused_shadow" ]]; then
  compose --profile operator build operator

  persist_and_verify_pause() {
    local config_path=$1
    local pause_output
    pause_output=$(
      ARBITRAGE_OPERATOR_SKIP_BUILD=YES ./ops/operator_python.sh \
        -m arbitrage_engine.cli --config "${config_path}" risk pause \
        --reason "safe_paused_shadow_deploy:${revision}"
    )
    python3 -c '
import json
import sys

if json.load(sys.stdin).get("paused") is not True:
    raise SystemExit("durable risk pause verification failed")
' <<<"${pause_output}"
  }

  persist_and_verify_pause config.production.clob_hft.json
  persist_and_verify_pause config.production.quote_arb.json
fi
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
  if health_target_ok 9108 clob_hft "${resolved_clob_mode}" >/dev/null \
    && health_target_ok 9109 quote_arb "${resolved_quote_mode}" >/dev/null; then
    echo "compose deployment passed ${DEPLOY_HEALTH_POLICY} health policy on ${revision}"
    compose ps -a
    exit 0
  fi
  sleep "${HEALTH_SLEEP_SECONDS}"
done

echo "compose deployment failed ${DEPLOY_HEALTH_POLICY} health policy on $(git rev-parse HEAD)" >&2
health_target_ok 9108 clob_hft "${resolved_clob_mode}" >&2 || true
health_target_ok 9109 quote_arb "${resolved_quote_mode}" >&2 || true
compose ps -a >&2
compose logs --no-color --tail=200 bot-clob-hft bot-quote-arb >&2 || true
exit 1
