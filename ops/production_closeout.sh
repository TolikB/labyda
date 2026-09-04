#!/usr/bin/env bash
set -Eeuo pipefail

ARTIFACT_ROOT=${ARTIFACT_ROOT:-$(pwd)/closeout-artifacts}
DURATION_SECONDS=${DURATION_SECONDS:-14400}
POLL_SECONDS=${POLL_SECONDS:-15}
DATABASE_POLL_SECONDS=${DATABASE_POLL_SECONDS:-60}
DATABASE_TIMEOUT_SECONDS=${DATABASE_TIMEOUT_SECONDS:-45}
CALIBRATION_DURATION_SECONDS=${CALIBRATION_DURATION_SECONDS:-3600}
CALIBRATION_MIN_EVALUATIONS=${CALIBRATION_MIN_EVALUATIONS:-10000}
CALIBRATION_REQUIRE_CONFIGURED_RESERVE=${CALIBRATION_REQUIRE_CONFIGURED_RESERVE:-YES}
READY_WAIT_ATTEMPTS=${READY_WAIT_ATTEMPTS:-450}
READY_WAIT_SLEEP_SECONDS=${READY_WAIT_SLEEP_SECONDS:-2}
AUTO_APPROVE_SAFE_MAPPINGS=${AUTO_APPROVE_SAFE_MAPPINGS:-NO}
ENABLE_FUNDED_CANARY=${ENABLE_FUNDED_CANARY:-NO}
FUNDED_CANARY_TARGET=${FUNDED_CANARY_TARGET:-}
CREDENTIAL_ROTATION_CONFIRMED=${CREDENTIAL_ROTATION_CONFIRMED:-NO}
CLOSEOUT_OPERATOR=${CLOSEOUT_OPERATOR:-production-closeout}
CLOSEOUT_LOCK_FILE=${CLOSEOUT_LOCK_FILE:-.runtime/production-closeout.lock}
PYTHON_BIN=${PYTHON_BIN:-}
ADMIN_BIN=${ADMIN_BIN:-}
DEFER_BACKUP_GATES=${DEFER_BACKUP_GATES:-1}
LEGACY_CONFIG_PATH=${CONFIG_PATH:-}
LEGACY_COMPOSE_SERVICE=${COMPOSE_SERVICE:-}
COMPOSE_ENV_FILE=${COMPOSE_ENV_FILE:-.env.production}

test -d .git || { echo "run production_closeout.sh from the repo checkout" >&2; exit 1; }
test -f docker-compose.yml || { echo "docker-compose.yml is missing" >&2; exit 1; }
test -f "${COMPOSE_ENV_FILE}" || { echo "Compose env file is missing: ${COMPOSE_ENV_FILE}" >&2; exit 1; }

compose() {
  docker compose --env-file "${COMPOSE_ENV_FILE}" -f docker-compose.yml "$@"
}

mkdir -p "$(dirname "${CLOSEOUT_LOCK_FILE}")"
command -v flock >/dev/null 2>&1 || {
  echo "flock is required for project-scoped closeout serialization" >&2
  exit 1
}
command -v sha256sum >/dev/null 2>&1 || {
  echo "sha256sum is required for release/config integrity binding" >&2
  exit 1
}
command -v install >/dev/null 2>&1 || {
  echo "install is required for immutable run-scoped configuration copies" >&2
  exit 1
}
exec 9>"${CLOSEOUT_LOCK_FILE}"
if ! flock -n 9; then
  echo "another production_closeout.sh run already owns ${CLOSEOUT_LOCK_FILE}" >&2
  exit 1
fi

using_operator_container=0
if [[ -z "${PYTHON_BIN}" ]]; then
  PYTHON_BIN=./ops/operator_python.sh
  using_operator_container=1
fi

export ARBITRAGE_DATABASE_HOST_OVERRIDE=${ARBITRAGE_DATABASE_HOST_OVERRIDE:-127.0.0.1}
export ARBITRAGE_DATABASE_PORT_OVERRIDE=${ARBITRAGE_DATABASE_PORT_OVERRIDE:-5432}
test -n "${CI_VERIFIED_COMMIT_SHA:-}" || {
  echo "CI_VERIFIED_COMMIT_SHA from the successful CI artifact is required" >&2
  exit 1
}
actual_commit_sha=$(git rev-parse HEAD)
if [[ "${actual_commit_sha}" != "${CI_VERIFIED_COMMIT_SHA}" ]]; then
  echo "checkout SHA ${actual_commit_sha} does not match CI_VERIFIED_COMMIT_SHA" >&2
  exit 1
fi
assert_release_tree_clean() {
  local tracked_changes
  local untracked_release_files
  if ! tracked_changes=$(git status --porcelain=v1 --untracked-files=no); then
    echo "could not verify tracked release state" >&2
    return 1
  fi
  if [[ -n "${tracked_changes}" ]]; then
    echo "tracked or staged checkout changes invalidate the CI-verified release" >&2
    return 1
  fi
  if ! untracked_release_files=$(git ls-files --others -- \
      Dockerfile .dockerignore docker-compose.yml \
      requirements.lock pyproject.toml README.md alembic.ini \
      config.production.clob_hft.json config.production.quote_arb.json \
      migrations ops scripts src); then
    echo "could not verify untracked release inputs" >&2
    return 1
  fi
  if [[ -n "${untracked_release_files}" ]]; then
    echo "untracked release source/config files invalidate the CI-verified release" >&2
    return 1
  fi
}
assert_release_tree_clean
case "${AUTO_APPROVE_SAFE_MAPPINGS}" in
  YES|NO) ;;
  *)
    echo "AUTO_APPROVE_SAFE_MAPPINGS must be YES or NO" >&2
    exit 1
    ;;
esac
if [[ "${ENABLE_FUNDED_CANARY}" == "YES" ]]; then
  case "${FUNDED_CANARY_TARGET}" in
    quote_arb) ;;
    *)
      echo "this release permits only FUNDED_CANARY_TARGET=quote_arb; clob_hft stays paused-shadow" >&2
      exit 1
      ;;
  esac
  if [[ "${CREDENTIAL_ROTATION_CONFIRMED}" != "YES" ]]; then
    echo "funded canary requires CREDENTIAL_ROTATION_CONFIRMED=YES" >&2
    exit 1
  fi
  if [[ "${DURATION_SECONDS}" != "14400" ]]; then
    echo "funded canary requires DURATION_SECONDS=14400" >&2
    exit 1
  fi
  if [[ "${CALIBRATION_DURATION_SECONDS}" != "3600" ]]; then
    echo "funded canary requires CALIBRATION_DURATION_SECONDS=3600" >&2
    exit 1
  fi
fi

if [[ -n "${ADMIN_BIN}" ]]; then
  read -r -a admin_cmd <<<"${ADMIN_BIN}"
else
  admin_cmd=(env PYTHONPATH=src "${PYTHON_BIN}" -m arbitrage_engine.cli)
fi
script_python=(env PYTHONPATH=src "${PYTHON_BIN}")

run_maybe_sudo() {
  if command -v sudo >/dev/null 2>&1 && sudo -n true >/dev/null 2>&1; then
    sudo -n "$@"
  else
    "$@"
  fi
}

normalize_closeout_artifacts() {
  local backup_dir=${ARBITRAGE_BACKUP_DIR:-/mnt/arbitrage-backups}
  local path
  if [[ -d "${backup_dir}" ]]; then
    run_maybe_sudo chmod 0755 "${backup_dir}" || true
  fi
  shopt -s nullglob
  for path in \
    "${backup_dir}"/arbitrage-*.sql.gz \
    "${backup_dir}"/arbitrage-*.sql.gz.sha256 \
    "${backup_dir}"/restore-drill.json \
    "${backup_dir}"/drain-ready.json; do
    run_maybe_sudo chmod 0644 "${path}" || true
  done
  shopt -u nullglob
}

target_config_path() {
  case "$1" in
    clob_hft) echo "${CLOB_HFT_CONFIG_PATH:-config.production.clob_hft.json}" ;;
    quote_arb) echo "${QUOTE_ARB_CONFIG_PATH:-config.production.quote_arb.json}" ;;
    custom) echo "${LEGACY_CONFIG_PATH}" ;;
    *) echo "unknown target: $1" >&2; exit 1 ;;
  esac
}

target_source_config_path() {
  case "$1" in
    clob_hft) echo "config.production.clob_hft.json" ;;
    quote_arb) echo "config.production.quote_arb.json" ;;
    *) echo "unknown release target: $1" >&2; exit 1 ;;
  esac
}

target_compose_service() {
  case "$1" in
    clob_hft) echo "bot-clob-hft" ;;
    quote_arb) echo "bot-quote-arb" ;;
    custom)
      if [[ -n "${LEGACY_COMPOSE_SERVICE}" ]]; then
        echo "${LEGACY_COMPOSE_SERVICE}"
      else
        echo "bot-quote-arb"
      fi
      ;;
    *) echo "unknown target: $1" >&2; exit 1 ;;
  esac
}

target_observability_port() {
  case "$1" in
    clob_hft) echo "9108" ;;
    quote_arb) echo "9109" ;;
    custom)
      "${script_python[@]}" - "${LEGACY_CONFIG_PATH}" <<'PY'
import sys
from arbitrage_engine.config import load_config
print(load_config(sys.argv[1]).observability_port)
PY
      ;;
    *) echo "unknown target: $1" >&2; exit 1 ;;
  esac
}

target_routes() {
  local config_path
  if ! config_path=$(target_config_path "$1"); then
    return 1
  fi
  "${script_python[@]}" - "${config_path}" <<'PY'
import sys
from arbitrage_engine.config import load_config
from arbitrage_engine.production_audit import funded_routes
for route in funded_routes(load_config(sys.argv[1])):
    print(route)
PY
}

read_target_routes() {
  local target=$1
  local destination_name=$2
  local output
  local route
  local myriad_count=0
  local predict_count=0
  local -a parsed_routes=()

  if ! output=$(target_routes "${target}"); then
    echo "could not resolve funded routes for ${target}" >&2
    return 1
  fi
  if [[ -n "${output}" ]]; then
    mapfile -t parsed_routes <<<"${output}"
  fi
  for route in "${parsed_routes[@]}"; do
    case "${route}" in
      polymarket_myriad) myriad_count=$((myriad_count + 1)) ;;
      polymarket_predict) predict_count=$((predict_count + 1)) ;;
      *)
        echo "unexpected funded route for ${target}: ${route}" >&2
        return 1
        ;;
    esac
  done
  case "${target}" in
    clob_hft)
      if ((${#parsed_routes[@]} != 0)); then
        echo "clob_hft must have no funded routes in this release" >&2
        return 1
      fi
      ;;
    quote_arb)
      if ((${#parsed_routes[@]} != 2 || myriad_count != 1 || predict_count != 1)); then
        echo "quote_arb must fund exactly polymarket_myriad and polymarket_predict" >&2
        return 1
      fi
      ;;
    *)
      echo "unknown release target while resolving funded routes: ${target}" >&2
      return 1
      ;;
  esac

  local -n destination=${destination_name}
  destination=("${parsed_routes[@]}")
}

resolve_targets() {
  if [[ -n "${CLOSEOUT_TARGETS:-}" ]]; then
    read -r -a targets <<<"${CLOSEOUT_TARGETS}"
    printf '%s\n' "${targets[@]}"
    return
  fi
  if [[ -n "${LEGACY_CONFIG_PATH}" ]]; then
    printf '%s\n' "custom"
    return
  fi
  printf '%s\n' "clob_hft" "quote_arb"
}

audit_args() {
  local config_path=$1
  shift
  local args=("${admin_cmd[@]}" --config "${config_path}" "$@")
  if [[ "${DEFER_BACKUP_GATES}" == "1" ]]; then
    args+=(--defer-backup-gates)
  fi
  printf '%s\n' "${args[@]}"
}

run_and_capture() {
  local target=$1
  local name=$2
  shift 2
  echo "==> ${target}:${name}"
  "$@" | tee "${run_dir}/${target}/${name}.json"
}

require_full_capacity_funding_ready() {
  local report_path=$1
  "${script_python[@]}" - "${report_path}" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    report = json.load(handle)
readiness = report.get("full_capacity_funding_readiness") or {}
if readiness.get("ready") is not True:
    blockers = readiness.get("blocking_reasons") or ["full_capacity_readiness_missing"]
    raise SystemExit("funded canary readiness blocked: " + ", ".join(map(str, blockers)))
PY
}

require_shadow_transition_quiescent() {
  local config_path=$1
  "${script_python[@]}" - "${config_path}" <<'PY'
import asyncio
import json
import sys

from arbitrage_engine.config import load_config, load_operator_env
from arbitrage_engine.database import ProductionRepository
from arbitrage_engine.production_audit import enabled_routes


async def main() -> None:
    load_operator_env(sys.argv[1])
    config = load_config(sys.argv[1])
    repository = ProductionRepository(
        config.database_url,
        runtime_instance_id=config.runtime_instance_id,
        enabled_routes=enabled_routes(config),
    )
    try:
        positions, unresolved_orders, unresolved_redemptions = await asyncio.gather(
            repository.load_positions(),
            repository.unresolved_order_intents(),
            repository.unresolved_redemption_intents(),
        )
        report = {
            "runtime_instance_id": config.runtime_instance_id,
            "open_position_count": len(positions),
            "unresolved_order_intent_count": len(unresolved_orders),
            "unresolved_redemption_intent_count": len(unresolved_redemptions),
        }
        report["safe_to_enter_shadow"] = all(
            report[key] == 0
            for key in (
                "open_position_count",
                "unresolved_order_intent_count",
                "unresolved_redemption_intent_count",
            )
        )
        print(json.dumps(report, indent=2, sort_keys=True))
        if not report["safe_to_enter_shadow"]:
            raise SystemExit(
                "refusing shadow transition while PostgreSQL still contains managed state"
            )
    finally:
        await repository.close()


asyncio.run(main())
PY
}

final_audit_is_clean_for_shadow() {
  local report_path=$1
  local config_path=$2
  "${script_python[@]}" - "${report_path}" "${config_path}" <<'PY'
import asyncio
import json
import sys

from arbitrage_engine.config import load_config, load_operator_env
from arbitrage_engine.database import ProductionRepository

with open(sys.argv[1], encoding="utf-8") as handle:
    report = json.load(handle)

def clean(runtime):
    risk = runtime.get("risk_state") or {}
    return (
        (runtime.get("positions") or {}).get("count") == 0
        and (runtime.get("unresolved_order_intents") or {}).get("count") == 0
        and (runtime.get("unresolved_redemptions") or {}).get("count") == 0
        and not runtime.get("reconciliation_failures")
        and risk.get("paused") is True
        and risk.get("pause_reason") == "funded_canary_window_complete"
    )

async def main():
    if not clean(report.get("runtime_audit") or {}):
        return False
    load_operator_env(sys.argv[2])
    config = load_config(sys.argv[2])
    repository = ProductionRepository(
        config.database_url,
        runtime_instance_id=config.runtime_instance_id,
    )
    try:
        first = await repository.runtime_audit_snapshot()
        await asyncio.sleep(2)
        second = await repository.runtime_audit_snapshot()
        return clean(first) and clean(second)
    finally:
        await repository.close()

raise SystemExit(0 if asyncio.run(main()) else 1)
PY
}

wait_for_shadow_mode() {
  local target=$1
  local port
  local attempt
  port=$(target_observability_port "${target}")
  for attempt in $(seq 1 60); do
    if curl -fsS --max-time 3 "http://127.0.0.1:${port}/metrics" 2>/dev/null \
      | grep -Eq '^arbitrage_execution_mode_info\{[^}]*mode="shadow"[^}]*\} 1(\.0)?$'; then
      return 0
    fi
    sleep 2
  done
  echo "${target} did not expose shadow-mode metrics on port ${port}" >&2
  return 1
}

wait_for_paused_shadow() {
  local target=$1
  local port
  local attempt
  local metrics
  port=$(target_observability_port "${target}")
  for attempt in $(seq 1 60); do
    metrics=$(curl -fsS --max-time 3 "http://127.0.0.1:${port}/metrics" 2>/dev/null || true)
    if grep -Eq '^arbitrage_execution_mode_info\{[^}]*mode="shadow"[^}]*\} 1(\.0)?$' <<<"${metrics}" \
      && grep -Eq '^arbitrage_risk_paused 1(\.0)?$' <<<"${metrics}" \
      && grep -Eq '^arbitrage_ready 0(\.0)?$' <<<"${metrics}"; then
      return 0
    fi
    sleep 2
  done
  echo "${target} did not expose paused-shadow metrics on port ${port}" >&2
  return 1
}

wait_for_paused_canary() {
  local target=$1
  local port
  local attempt
  local metrics
  local quiet_samples=0
  port=$(target_observability_port "${target}")
  for attempt in $(seq 1 "${READY_WAIT_ATTEMPTS}"); do
    metrics=$(curl -fsS --max-time 3 "http://127.0.0.1:${port}/metrics" 2>/dev/null || true)
    if grep -Eq '^arbitrage_execution_mode_info\{[^}]*mode="canary"[^}]*\} 1(\.0)?$' <<<"${metrics}" \
      && grep -Eq '^arbitrage_risk_paused 1(\.0)?$' <<<"${metrics}" \
      && grep -Eq '^arbitrage_ready 0(\.0)?$' <<<"${metrics}" \
      && grep -Eq '^arbitrage_entry_submission_in_progress 0(\.0)?$' <<<"${metrics}"; then
      quiet_samples=$((quiet_samples + 1))
      if [[ "${quiet_samples}" -ge 2 ]]; then
        return 0
      fi
    else
      quiet_samples=0
    fi
    sleep "${READY_WAIT_SLEEP_SECONDS}"
  done
  echo "${target} did not reach paused-canary entry quiescence on port ${port}" >&2
  return 1
}

wait_for_observer_armed() {
  local armed_file=$1
  local attempt
  for attempt in $(seq 1 120); do
    if [[ -s "${armed_file}" ]]; then
      return 0
    fi
    sleep 1
  done
  echo "live canary observer did not arm: ${armed_file}" >&2
  return 1
}

wait_for_ready() {
  local target=$1
  local port
  local attempt
  port=$(target_observability_port "${target}")
  for attempt in $(seq 1 "${READY_WAIT_ATTEMPTS}"); do
    if curl -fsS --max-time 3 "http://127.0.0.1:${port}/health/ready" >/dev/null 2>&1; then
      return 0
    fi
    sleep "${READY_WAIT_SLEEP_SECONDS}"
  done
  echo "${target} did not become ready on port ${port} after $((READY_WAIT_ATTEMPTS * READY_WAIT_SLEEP_SECONDS)) seconds" >&2
  return 1
}

pause_on_exit=0
pause_targets_on_exit() {
  local status=$?
  local target
  trap - EXIT
  if [[ "${pause_on_exit}" == "1" ]]; then
    set +e
    for target in "${TARGETS[@]}"; do
      "${admin_cmd[@]}" --config "$(target_config_path "${target}")" risk pause \
        --reason "production_closeout_exit_fail_closed" >/dev/null
    done
  fi
  exit "${status}"
}

run_id=$(date -u +%Y%m%dT%H%M%SZ)
run_dir="${ARTIFACT_ROOT}/${run_id}"
mkdir -p "${run_dir}"

normalize_closeout_artifacts

mapfile -t TARGETS < <(resolve_targets)
test "${#TARGETS[@]}" -gt 0 || { echo "no closeout targets resolved" >&2; exit 1; }
funded_target_matches=0
clob_target_matches=0
quote_target_matches=0
for target in "${TARGETS[@]}"; do
  if [[ "${target}" == "${FUNDED_CANARY_TARGET}" ]]; then
    funded_target_matches=$((funded_target_matches + 1))
  fi
  if [[ "${target}" == "clob_hft" ]]; then
    clob_target_matches=$((clob_target_matches + 1))
  fi
  if [[ "${target}" == "quote_arb" ]]; then
    quote_target_matches=$((quote_target_matches + 1))
  fi
done
if [[ "${#TARGETS[@]}" -ne 2 \
  || "${clob_target_matches}" -ne 1 \
  || "${quote_target_matches}" -ne 1 ]]; then
  echo "this release must manage exactly clob_hft and quote_arb; CLOSEOUT_TARGETS subsets are forbidden" >&2
  exit 1
fi
if [[ "${ENABLE_FUNDED_CANARY}" == "YES" && "${funded_target_matches}" -ne 1 ]]; then
  echo "funded canary target must be one of the two managed services" >&2
  exit 1
fi
FORMAL_TARGETS=("quote_arb")

all_services=()
declare -A expected_config_sha256=()
verified_config_dir=".runtime/verified-config/${CI_VERIFIED_COMMIT_SHA}"
mkdir -p "${verified_config_dir}"
for target in "${TARGETS[@]}"; do
  source_config_path=$(target_source_config_path "${target}")
  test -f "${source_config_path}" || {
    echo "missing config for ${target}: ${source_config_path}" >&2
    exit 1
  }
  verified_config_path="${verified_config_dir}/config.production.${target}.json"
  install -m 0444 "${source_config_path}" "${verified_config_path}"
  service=$(target_compose_service "${target}")
  all_services+=("${service}")
  mkdir -p "${run_dir}/${target}"
  expected_config_sha256["${target}"]=$(sha256sum "${verified_config_path}" | awk '{print $1}')
done
export CLOB_HFT_CONFIG_PATH="${verified_config_dir}/config.production.clob_hft.json"
export QUOTE_ARB_CONFIG_PATH="${verified_config_dir}/config.production.quote_arb.json"

assert_release_integrity() {
  local target
  local config_path
  local current_digest
  [[ "$(git rev-parse HEAD)" == "${CI_VERIFIED_COMMIT_SHA}" ]] || {
    echo "checkout SHA changed after preflight" >&2
    return 1
  }
  assert_release_tree_clean || return 1
  for target in "${TARGETS[@]}"; do
    config_path=$(target_config_path "${target}")
    current_digest=$(sha256sum "${config_path}" | awk '{print $1}')
    if [[ "${current_digest}" != "${expected_config_sha256[${target}]}" ]]; then
      echo "configuration changed after preflight: ${config_path}" >&2
      return 1
    fi
  done
}

integrity_manifest_path="${run_dir}/RELEASE_INTEGRITY.txt"
{
  echo "ci_verified_commit_sha=${CI_VERIFIED_COMMIT_SHA}"
  for target in "${TARGETS[@]}"; do
    config_path=$(target_config_path "${target}")
    integrity_routes=()
    read_target_routes "${target}" integrity_routes
    integrity_routes_csv=$(IFS=,; echo "${integrity_routes[*]}")
    echo "config_sha256_${target}=${expected_config_sha256[${target}]}"
    echo "funded_routes_${target}=${integrity_routes_csv}"
  done
} >"${integrity_manifest_path}"

# Bring up only persistence/operator dependencies first. Existing bot processes,
# if any, must remain in their current mode until they are durably paused and the
# database proves there is no position or unresolved lifecycle state to strand.
compose up -d postgres migrate
if [[ "${using_operator_container}" == "1" ]]; then
  compose --profile operator build operator
  export ARBITRAGE_OPERATOR_SKIP_BUILD=YES
fi

# Establish the durable stop before any discovery or mapping mutation. This also
# handles a future invocation that starts while a previous canary is still running.
pause_on_exit=1
trap pause_targets_on_exit EXIT
for target in "${TARGETS[@]}"; do
  config_path=$(target_config_path "${target}")
  run_and_capture \
    "${target}" \
    risk-pause-closeout-start \
    "${admin_cmd[@]}" --config "${config_path}" risk pause \
      --reason "production_closeout_shadow_setup"
done

# A shadow runtime intentionally has no PostgreSQL reconciliation/exit service.
# Never force-recreate either bot into shadow while durable managed state exists.
for target in "${TARGETS[@]}"; do
  config_path=$(target_config_path "${target}")
  run_and_capture \
    "${target}" \
    pre-shadow-transition-quiescence \
    require_shadow_transition_quiescent "${config_path}"
done

export LIVE_TRADING_CONFIRM=NO
export ARBITRAGE_EXECUTION_MODE_OVERRIDE=shadow
export CLOB_HFT_EXECUTION_MODE=shadow
export QUOTE_ARB_EXECUTION_MODE=shadow
compose up -d "${all_services[@]}"

# Candidate persistence is explicit. Approve only the CLI's exact-ID, single-candidate,
# launch-horizon-safe set before calibration so short-lived markets reach the engine.
for target in "${FORMAL_TARGETS[@]}"; do
  config_path=$(target_config_path "${target}")
  run_and_capture \
    "${target}" \
    discovery-overlap-pre-approval \
    "${admin_cmd[@]}" --config "${config_path}" discovery overlap --persist-candidates
  run_and_capture \
    "${target}" \
    safe-mapping-approval-preview \
    "${admin_cmd[@]}" --config "${config_path}" mappings approve-safe-candidates \
      --operator "${CLOSEOUT_OPERATOR}"
  if [[ "${AUTO_APPROVE_SAFE_MAPPINGS}" == "YES" ]]; then
    run_and_capture \
      "${target}" \
      safe-mapping-approval-applied \
      "${admin_cmd[@]}" --config "${config_path}" mappings approve-safe-candidates \
        --operator "${CLOSEOUT_OPERATOR}" --confirm YES
  fi
  run_and_capture \
    "${target}" \
    discovery-overlap-post-approval \
    "${admin_cmd[@]}" --config "${config_path}" discovery overlap
done

# Force a deterministic mapping reload while order submission remains impossible.
compose up -d --force-recreate "${all_services[@]}"
for target in "${TARGETS[@]}"; do
  wait_for_shadow_mode "${target}"
done

# Fail fast on wallet/baseline identity or account drift before spending an hour
# collecting calibration evidence. A second full pass runs after calibration so
# the eventual resume gate is fresh as well.
for target in "${FORMAL_TARGETS[@]}"; do
  config_path=$(target_config_path "${target}")
  run_and_capture \
    "${target}" \
    full-reconciliation-pre-calibration \
    "${admin_cmd[@]}" --config "${config_path}" reconcile
done

calibration_pids=()
for target in "${FORMAL_TARGETS[@]}"; do
  config_path=$(target_config_path "${target}")
  (
    calibration_args=(
      scripts/shadow_calibration.py
      --config "${config_path}"
      --duration-seconds "${CALIBRATION_DURATION_SECONDS}"
      --poll-seconds "${POLL_SECONDS}"
      --min-valid-evaluations "${CALIBRATION_MIN_EVALUATIONS}"
      --artifact-dir "${run_dir}/${target}"
    )
    if [[ "${CALIBRATION_REQUIRE_CONFIGURED_RESERVE}" == "YES" ]]; then
      calibration_args+=(--require-configured-reserve)
    fi
    "${script_python[@]}" "${calibration_args[@]}" \
      | tee "${run_dir}/${target}/shadow-calibration-console.json"
  ) &
  calibration_pids+=($!)
done

calibration_failed=0
for pid in "${calibration_pids[@]}"; do
  if ! wait "${pid}"; then
    calibration_failed=1
  fi
done
if [[ "${calibration_failed}" == "1" ]]; then
  echo "one or more shadow calibration windows failed" >&2
  exit 1
fi
assert_release_integrity

# Technical audit stays fail-closed in paused shadow. Canary gates are evaluated
# only after this phase passes and funded execution is explicitly enabled.

for target in "${FORMAL_TARGETS[@]}"; do
  config_path=$(target_config_path "${target}")
  run_and_capture \
    "${target}" \
    full-reconciliation-post-calibration \
    "${admin_cmd[@]}" --config "${config_path}" reconcile

  run_and_capture \
    "${target}" \
    unresolved-review \
    "${admin_cmd[@]}" --config "${config_path}" orders review-unresolved --older-than-minutes 60

  run_and_capture \
    "${target}" \
    discovery-overlap \
    "${admin_cmd[@]}" --config "${config_path}" discovery overlap

  run_and_capture \
    "${target}" \
    all-market-readiness \
    "${script_python[@]}" scripts/live_balance_and_order_readiness.py --config "${config_path}" --all-markets

  mapfile -t target_audit_cmd < <(
    audit_args "${config_path}" production audit --all-markets --technical-only
  )
  run_and_capture "${target}" production-audit-pre-live "${target_audit_cmd[@]}"
done

summary_quote_routes=()
read_target_routes quote_arb summary_quote_routes
summary_quote_routes_csv=$(IFS=,; echo "${summary_quote_routes[*]}")

if [[ "${ENABLE_FUNDED_CANARY}" != "YES" ]]; then
  {
    echo "artifact_root=${run_dir}"
    echo "ci_verified_commit_sha=${CI_VERIFIED_COMMIT_SHA}"
    echo "release_integrity_manifest=${integrity_manifest_path}"
    echo "config_sha256_quote_arb=${expected_config_sha256[quote_arb]}"
    echo "funded_routes_quote_arb=${summary_quote_routes_csv}"
    echo "result=shadow_calibration_and_preflight_complete"
    echo "funded_canary_started=false"
    echo "risk_state_after_exit=paused"
    echo "next_step=set ENABLE_FUNDED_CANARY=YES only after operator sign-off and credential decision acknowledgement"
  } >"${run_dir}/SUMMARY.txt"
  echo "shadow closeout complete; funded canary was not enabled"
  exit 0
fi

# Risk resume itself fails closed on unresolved intents, redemptions, manual-review
# positions, reconciliation drift, and the daily-loss limit. Arm every observer
# and the independent deadline watchdog before resuming exactly one runtime.
assert_release_integrity
require_full_capacity_funding_ready \
  "${run_dir}/${FUNDED_CANARY_TARGET}/all-market-readiness.json"
funded_config_path=$(target_config_path "${FUNDED_CANARY_TARGET}")
canary_deadline_file=.runtime/canary-control/deadline
mkdir -p "$(dirname "${canary_deadline_file}")"
# Zero keeps canary startup fail-closed until the exact deadline is published
# before durable resume.
printf '%s\n' 0 >"${canary_deadline_file}"
unset FUNDED_CANARY_DEADLINE_UNIX

export LIVE_TRADING_CONFIRM=YES
export ARBITRAGE_EXECUTION_MODE_OVERRIDE=shadow
export CLOB_HFT_EXECUTION_MODE=shadow
export QUOTE_ARB_EXECUTION_MODE=shadow
export QUOTE_ARB_EXECUTION_MODE=canary
compose up -d --force-recreate "${all_services[@]}"
for target in "${TARGETS[@]}"; do
  if [[ "${target}" == "${FUNDED_CANARY_TARGET}" ]]; then
    wait_for_paused_canary "${target}"
  else
    wait_for_paused_shadow "${target}"
  fi
done

canary_pids=()
observer_armed_files=()
observer_exit_files=()
funded_routes=()
read_target_routes "${FUNDED_CANARY_TARGET}" funded_routes
for route in "${funded_routes[@]}"; do
  canary_root="${run_dir}/${FUNDED_CANARY_TARGET}/canary-artifacts/${route}"
  armed_file="${run_dir}/${FUNDED_CANARY_TARGET}/observer-armed-${route}.json"
  observer_exit_file="${run_dir}/${FUNDED_CANARY_TARGET}/observer-exit-${route}.status"
  observer_cmd=(
    env ARBITRAGE_EXECUTION_MODE_OVERRIDE=canary
    "${script_python[@]}"
    scripts/live_canary_window.py
    --config "${funded_config_path}"
    --duration-seconds "${DURATION_SECONDS}"
    --poll-seconds "${POLL_SECONDS}"
    --database-poll-seconds "${DATABASE_POLL_SECONDS}"
    --database-timeout-seconds "${DATABASE_TIMEOUT_SECONDS}"
    --await-risk-resume
    --armed-file "${armed_file}"
    --deadline-file "${canary_deadline_file}"
    --stop-on timeout
    --required-route "${route}"
    --artifact-dir "${canary_root}"
    --compose-cwd .
    --expected-config-sha256 "${expected_config_sha256[${FUNDED_CANARY_TARGET}]}"
  )
  for expected_route in "${funded_routes[@]}"; do
    observer_cmd+=(--expected-funded-route "${expected_route}")
  done
  for service in "${all_services[@]}"; do
    observer_cmd+=(--compose-service "${service}")
  done
  (
    set +e
    "${observer_cmd[@]}" | tee "${run_dir}/${FUNDED_CANARY_TARGET}/live-canary-window-${route}.json"
    observer_status=${PIPESTATUS[0]}
    printf '%s\n' "${observer_status}" >"${observer_exit_file}"
    exit "${observer_status}"
  ) &
  canary_pids+=($!)
  observer_armed_files+=("${armed_file}")
  observer_exit_files+=("${observer_exit_file}")
done

funded_observer_failed_early() {
  local index
  for index in "${!canary_pids[@]}"; do
    if [[ -s "${observer_exit_files[${index}]}" ]] \
      || ! kill -0 "${canary_pids[${index}]}" 2>/dev/null; then
      return 0
    fi
  done
  return 1
}

for armed_file in "${observer_armed_files[@]}"; do
  wait_for_observer_armed "${armed_file}"
done
if funded_observer_failed_early; then
  echo "a required funded-canary observer exited before durable risk resume" >&2
  exit 1
fi

# Publish the hard deadline before resume. Starting the bounded window a few
# seconds early is safe; publishing after resume is not, because the runtime can
# observe/cache the fail-closed zero sentinel before the real value arrives.
canary_deadline_unix=$(( $(date -u +%s) + DURATION_SECONDS ))
printf '%s\n' "${canary_deadline_unix}" >"${canary_deadline_file}"
run_and_capture \
  "${FUNDED_CANARY_TARGET}" \
  risk-resume-canary \
  env ARBITRAGE_EXECUTION_MODE_OVERRIDE=canary \
    "${admin_cmd[@]}" --config "${funded_config_path}" risk resume
(
  watchdog_sleep_seconds=$(( canary_deadline_unix - $(date -u +%s) ))
  if [[ "${watchdog_sleep_seconds}" -gt 0 ]]; then
    sleep "${watchdog_sleep_seconds}"
  fi
  run_and_capture \
    "${FUNDED_CANARY_TARGET}" \
    risk-pause-canary-window-complete \
    env ARBITRAGE_EXECUTION_MODE_OVERRIDE=canary \
      "${admin_cmd[@]}" --config "${funded_config_path}" risk pause \
      --reason "funded_canary_window_complete"
  wait_for_paused_canary "${FUNDED_CANARY_TARGET}"
) &
deadline_watchdog_pid=$!

stop_failed_funded_canary() {
  local pid
  set +e
  env ARBITRAGE_EXECUTION_MODE_OVERRIDE=canary \
    "${admin_cmd[@]}" --config "${funded_config_path}" risk pause \
    --reason "funded_canary_observer_failed" \
    >"${run_dir}/${FUNDED_CANARY_TARGET}/risk-pause-observer-failed.json"
  wait_for_paused_canary "${FUNDED_CANARY_TARGET}"
  for pid in "${canary_pids[@]}" "${deadline_watchdog_pid}" "${funded_ready_pid:-}"; do
    if [[ -n "${pid}" ]] && kill -0 "${pid}" 2>/dev/null; then
      kill "${pid}" 2>/dev/null
    fi
  done
  for pid in "${canary_pids[@]}" "${deadline_watchdog_pid}" "${funded_ready_pid:-}"; do
    if [[ -n "${pid}" ]]; then
      wait "${pid}" 2>/dev/null
    fi
  done
  set -e
  echo "funded canary stopped because a required observer exited before the hard deadline" >&2
  return 0
}

wait_for_ready "${FUNDED_CANARY_TARGET}" &
funded_ready_pid=$!
while kill -0 "${funded_ready_pid}" 2>/dev/null; do
  if funded_observer_failed_early; then
    stop_failed_funded_canary
    exit 1
  fi
  if ! kill -0 "${deadline_watchdog_pid}" 2>/dev/null; then
    stop_failed_funded_canary
    exit 1
  fi
  sleep 1
done
if ! wait "${funded_ready_pid}"; then
  stop_failed_funded_canary
  exit 1
fi

while kill -0 "${deadline_watchdog_pid}" 2>/dev/null; do
  if funded_observer_failed_early \
    && [[ "$(date -u +%s)" -lt "${canary_deadline_unix}" ]]; then
    stop_failed_funded_canary
    exit 1
  fi
  sleep 1
done

canary_failed=0
if ! wait "${deadline_watchdog_pid}"; then
  canary_failed=1
fi
for pid in "${canary_pids[@]}"; do
  if ! wait "${pid}"; then
    canary_failed=1
  fi
done
if [[ "${canary_failed}" == "1" ]]; then
  echo "one or more funded canary observers failed" >&2
  exit 1
fi
assert_release_integrity

summary_path="${run_dir}/SUMMARY.txt"
: >"${summary_path}"
{
  echo "artifact_root=${run_dir}"
  echo "ci_verified_commit_sha=${CI_VERIFIED_COMMIT_SHA}"
  echo "release_integrity_manifest=${integrity_manifest_path}"
  echo "config_sha256_quote_arb=${expected_config_sha256[quote_arb]}"
  echo "funded_routes_quote_arb=${summary_quote_routes_csv}"
  echo "defer_backup_gates=${DEFER_BACKUP_GATES}"
  echo "duration_seconds=${DURATION_SECONDS}"
  echo "poll_seconds=${POLL_SECONDS}"
  echo "database_poll_seconds=${DATABASE_POLL_SECONDS}"
  echo "database_timeout_seconds=${DATABASE_TIMEOUT_SECONDS}"
  echo "calibration_duration_seconds=${CALIBRATION_DURATION_SECONDS}"
  echo "calibration_min_evaluations=${CALIBRATION_MIN_EVALUATIONS}"
  echo "calibration_require_configured_reserve=${CALIBRATION_REQUIRE_CONFIGURED_RESERVE}"
  echo "auto_approve_safe_mappings=${AUTO_APPROVE_SAFE_MAPPINGS}"
  echo "funded_canary_started=true"
  echo "funded_canary_target=${FUNDED_CANARY_TARGET}"
  echo "credential_rotation_confirmed=${CREDENTIAL_ROTATION_CONFIRMED}"
} >>"${summary_path}"

for target in "${FUNDED_CANARY_TARGET}"; do
  config_path=$(target_config_path "${target}")
  routes=()
  read_target_routes "${target}" routes
  final_audit_extra=(
    production audit --all-markets --require-live-order-evidence --post-window-paused
  )
  route_report_lines=()
  for route in "${routes[@]}"; do
    live_report_path=$(find "${run_dir}/${target}/canary-artifacts/${route}" -name report.json | sort | tail -n 1)
    test -n "${live_report_path}" || {
      echo "live canary report.json not found for ${target}:${route}" >&2
      exit 1
    }
    final_audit_extra+=(--live-window-report "${route}=${live_report_path}")
    route_report_lines+=("live_canary_report_${route}=${live_report_path}")
  done
  mapfile -t final_audit_cmd < <(
    audit_args "${config_path}" "${final_audit_extra[@]}"
  )
  # Operator containers inherit the global shadow override used for the non-target
  # service. Scope this audit to canary so it validates the actual funded contract.
  run_and_capture \
    "${target}" \
    production-audit-final \
    env ARBITRAGE_EXECUTION_MODE_OVERRIDE=canary "${final_audit_cmd[@]}"
  final_audit_path="${run_dir}/${target}/production-audit-final.json"
  if final_audit_is_clean_for_shadow "${final_audit_path}" "${config_path}"; then
    # A zero-row snapshot is not sufficient: an exit may have removed its
    # PositionRow after the last venue reconciliation. Reconcile once more in
    # paused-canary, then repeat the complete audit and zero-state gate before
    # removing the runtime that still owns exit/reconciliation responsibility.
    run_and_capture \
      "${target}" \
      full-reconciliation-post-window \
      env ARBITRAGE_EXECUTION_MODE_OVERRIDE=canary \
        "${admin_cmd[@]}" --config "${config_path}" reconcile
    run_and_capture \
      "${target}" \
      production-audit-final-post-reconciliation \
      env ARBITRAGE_EXECUTION_MODE_OVERRIDE=canary "${final_audit_cmd[@]}"
    post_reconciliation_audit_path="${run_dir}/${target}/production-audit-final-post-reconciliation.json"
    if final_audit_is_clean_for_shadow "${post_reconciliation_audit_path}" "${config_path}" \
      && run_and_capture \
        "${target}" \
        post-window-quiescence \
        require_shadow_transition_quiescent "${config_path}"; then
      export LIVE_TRADING_CONFIRM=NO
      export ARBITRAGE_EXECUTION_MODE_OVERRIDE=shadow
      export CLOB_HFT_EXECUTION_MODE=shadow
      export QUOTE_ARB_EXECUTION_MODE=shadow
      compose up -d --force-recreate "${all_services[@]}"
      for paused_target in "${TARGETS[@]}"; do
        wait_for_paused_shadow "${paused_target}"
      done
      post_window_state="paused_shadow_clean"
    else
      post_window_state="canary_risk_paused_post_reconciliation_state"
    fi
  else
    post_window_state="canary_risk_paused_open_or_unresolved_state"
  fi
  {
    echo ""
    echo "[${target}]"
    echo "config_path=${config_path}"
    echo "compose_service=$(target_compose_service "${target}")"
    echo "unresolved_review=${run_dir}/${target}/unresolved-review.json"
    echo "discovery_overlap=${run_dir}/${target}/discovery-overlap.json"
    echo "all_market_readiness=${run_dir}/${target}/all-market-readiness.json"
    echo "production_audit_pre_live=${run_dir}/${target}/production-audit-pre-live.json"
    echo "shadow_calibration=${run_dir}/${target}/shadow-calibration-$(target_config_path "${target}" | sed -E 's/^config\.production\.([^.]+)\.json$/\1/').json"
    printf '%s\n' "${route_report_lines[@]}"
    echo "production_audit_final=${run_dir}/${target}/production-audit-final.json"
    if [[ -n "${post_reconciliation_audit_path:-}" ]]; then
      echo "full_reconciliation_post_window=${run_dir}/${target}/full-reconciliation-post-window.json"
      echo "production_audit_final_post_reconciliation=${post_reconciliation_audit_path}"
      echo "post_window_quiescence=${run_dir}/${target}/post-window-quiescence.json"
    fi
    echo "post_window_state=${post_window_state}"
  } >>"${summary_path}"
done

pause_on_exit=0
echo "production closeout artifacts written to ${run_dir}"
