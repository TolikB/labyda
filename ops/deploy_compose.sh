#!/usr/bin/env bash
set -Eeuo pipefail

REPO_DIR=${REPO_DIR:-$(pwd)}
BRANCH=${BRANCH:-master}
HEALTH_SLEEP_SECONDS=${HEALTH_SLEEP_SECONDS:-2}
HEALTH_DIAGNOSTIC_TIMEOUT_SECONDS=${HEALTH_DIAGNOSTIC_TIMEOUT_SECONDS:-10}
DEPLOY_HEALTH_POLICY=${DEPLOY_HEALTH_POLICY:-ready}
RELEASE_SHA_FILE=${RELEASE_SHA_FILE:-.runtime/release-sha}
COMPOSE_ENV_FILE=${COMPOSE_ENV_FILE:-.env.production}

cd "${REPO_DIR}"

test -d .git || { echo "deploy_compose.sh requires a git checkout" >&2; exit 1; }
test -f docker-compose.yml || { echo "docker-compose.yml is missing" >&2; exit 1; }
test -f "${COMPOSE_ENV_FILE}" || { echo "Compose env file is missing: ${COMPOSE_ENV_FILE}" >&2; exit 1; }
test -f scripts/runtime_health_gate.py || { echo "runtime health gate is missing" >&2; exit 1; }

compose() {
  docker compose --env-file "${COMPOSE_ENV_FILE}" -f docker-compose.yml "$@"
}

case "${DEPLOY_HEALTH_POLICY}" in
  ready|safe_paused_shadow|safe_paused_shadow_bootstrap) ;;
  *) echo "unsupported DEPLOY_HEALTH_POLICY: ${DEPLOY_HEALTH_POLICY}" >&2; exit 1 ;;
esac

# A full scan-all bootstrap can legitimately take several minutes before it can
# publish the first discovery snapshot. Keep the shorter fail-fast window for
# normal deploys, but give the bootstrap policy enough time to observe one full
# catalog pass. HEALTH_RETRIES remains a secondary attempt cap; the absolute
# wall-clock deadline prevents slow or wedged HTTP probes from stretching this
# wait to hours.
if [[ -z "${HEALTH_RETRIES:-}" ]]; then
  if [[ "${DEPLOY_HEALTH_POLICY}" == "safe_paused_shadow_bootstrap" ]]; then
    HEALTH_RETRIES=${BOOTSTRAP_HEALTH_RETRIES:-600}
  else
    HEALTH_RETRIES=120
  fi
fi
if [[ -z "${HEALTH_WAIT_TIMEOUT_SECONDS:-}" ]]; then
  if [[ "${DEPLOY_HEALTH_POLICY}" == "safe_paused_shadow_bootstrap" ]]; then
    HEALTH_WAIT_TIMEOUT_SECONDS=1200
  else
    HEALTH_WAIT_TIMEOUT_SECONDS=240
  fi
fi

[[ "${HEALTH_RETRIES}" =~ ^[1-9][0-9]*$ ]] || {
  echo "HEALTH_RETRIES must be a positive integer" >&2
  exit 1
}
[[ "${HEALTH_WAIT_TIMEOUT_SECONDS}" =~ ^[1-9][0-9]*$ ]] || {
  echo "HEALTH_WAIT_TIMEOUT_SECONDS must be a positive integer" >&2
  exit 1
}
[[ "${HEALTH_DIAGNOSTIC_TIMEOUT_SECONDS}" =~ ^[1-9][0-9]*$ ]] || {
  echo "HEALTH_DIAGNOSTIC_TIMEOUT_SECONDS must be a positive integer" >&2
  exit 1
}
command -v timeout >/dev/null 2>&1 || {
  echo "GNU timeout is required for bounded deployment health checks" >&2
  exit 1
}

is_safe_paused_deploy() {
  [[ "${DEPLOY_HEALTH_POLICY}" == "safe_paused_shadow" \
    || "${DEPLOY_HEALTH_POLICY}" == "safe_paused_shadow_bootstrap" ]]
}

tracked_changes=$(git status --porcelain --untracked-files=no)
test -z "${tracked_changes}" || { echo "deployment requires a clean tracked worktree" >&2; exit 1; }
untracked_runtime_input_count=$(
  git ls-files --others -z -- \
    Dockerfile .dockerignore docker-compose.yml \
    requirements.lock pyproject.toml README.md alembic.ini \
    config.production.clob_hft.json config.production.quote_arb.json \
    migrations ops scripts src | \
    tr -cd '\0' | \
    wc -c
)
if ((untracked_runtime_input_count > 0)); then
  echo "deployment refuses untracked runtime/build inputs" >&2
  exit 1
fi

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
' | tr -d '\r'
)
test "${#resolved_runtime_controls[@]}" -eq 4 || {
  echo "could not resolve runtime safety controls from Compose" >&2
  exit 1
}
resolved_clob_mode=${resolved_runtime_controls[0]}
resolved_clob_confirm=${resolved_runtime_controls[1]}
resolved_quote_mode=${resolved_runtime_controls[2]}
resolved_quote_confirm=${resolved_runtime_controls[3]}

if is_safe_paused_deploy; then
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

# Build the migration image from the exact verified checkout before fencing the
# runtimes. Otherwise `compose run migrate` can reuse the previous release image
# and report success without applying migrations that only exist in this SHA.
compose build migrate

install -d -m 0755 "$(dirname "${RELEASE_SHA_FILE}")"
printf '%s\n' "${revision}" >"${RELEASE_SHA_FILE}"
chmod 0644 "${RELEASE_SHA_FILE}"

# Fence trading before schema changes. Any migration or pause failure leaves both
# runtimes stopped instead of trading against a partially migrated database.
compose stop bot-clob-hft bot-quote-arb
compose run --rm migrate
if is_safe_paused_deploy; then
  compose --profile operator build operator

  persist_and_verify_pause() {
    local config_path=$1
    local pause_output
    pause_output=$(
      ARBITRAGE_OPERATOR_SKIP_BUILD=YES ./ops/operator_python.sh \
        -m arbitrage_engine.cli --config "${config_path}" risk pause \
        --reason "${DEPLOY_HEALTH_POLICY}_deploy:${revision}"
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

health_target_probe() {
  local port=$1
  local runtime_instance_id=$2
  local expected_mode=$3
  local process_timeout_seconds=$4
  timeout --foreground --kill-after=1s "${process_timeout_seconds}s" \
    python3 scripts/runtime_health_gate.py \
    --base-url "http://127.0.0.1:${port}" \
    --expected-runtime-instance-id "${runtime_instance_id}" \
    --expected-mode "${expected_mode}" \
    --accept "${DEPLOY_HEALTH_POLICY}" \
    --timeout-seconds 3
}

health_wait_deadline=$((SECONDS + HEALTH_WAIT_TIMEOUT_SECONDS))
health_target_ok() {
  local remaining_seconds=$((health_wait_deadline - SECONDS))
  ((remaining_seconds > 0)) || return 124
  health_target_probe "$1" "$2" "$3" "${remaining_seconds}"
}

health_attempts=0
for _ in $(seq 1 "${HEALTH_RETRIES}"); do
  ((SECONDS < health_wait_deadline)) || break
  ((health_attempts += 1))
  if health_target_ok 9108 clob_hft "${resolved_clob_mode}" >/dev/null \
    && health_target_ok 9109 quote_arb "${resolved_quote_mode}" >/dev/null; then
    echo "compose deployment passed ${DEPLOY_HEALTH_POLICY} health policy on ${revision}"
    compose ps -a
    exit 0
  fi
  remaining_seconds=$((health_wait_deadline - SECONDS))
  ((remaining_seconds > 0)) || break
  timeout --foreground --kill-after=1s "${remaining_seconds}s" \
    sleep "${HEALTH_SLEEP_SECONDS}" || true
done

echo "compose deployment failed ${DEPLOY_HEALTH_POLICY} health policy on $(git rev-parse HEAD) after ${health_attempts} attempts within ${HEALTH_WAIT_TIMEOUT_SECONDS}s" >&2
health_target_probe 9108 clob_hft "${resolved_clob_mode}" "${HEALTH_DIAGNOSTIC_TIMEOUT_SECONDS}" >&2 || true
health_target_probe 9109 quote_arb "${resolved_quote_mode}" "${HEALTH_DIAGNOSTIC_TIMEOUT_SECONDS}" >&2 || true
compose ps -a >&2
compose logs --no-color --tail=200 bot-clob-hft bot-quote-arb >&2 || true
exit 1
