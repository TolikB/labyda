#!/usr/bin/env bash
set -Eeuo pipefail

ARTIFACT_ROOT=${ARTIFACT_ROOT:-$(pwd)/closeout-artifacts}
DURATION_SECONDS=${DURATION_SECONDS:-7200}
POLL_SECONDS=${POLL_SECONDS:-15}
DATABASE_POLL_SECONDS=${DATABASE_POLL_SECONDS:-60}
DATABASE_TIMEOUT_SECONDS=${DATABASE_TIMEOUT_SECONDS:-45}
CALIBRATION_DURATION_SECONDS=${CALIBRATION_DURATION_SECONDS:-3600}
CALIBRATION_MIN_EVALUATIONS=${CALIBRATION_MIN_EVALUATIONS:-10000}
CALIBRATION_REQUIRE_CONFIGURED_RESERVE=${CALIBRATION_REQUIRE_CONFIGURED_RESERVE:-YES}
READY_WAIT_ATTEMPTS=${READY_WAIT_ATTEMPTS:-450}
READY_WAIT_SLEEP_SECONDS=${READY_WAIT_SLEEP_SECONDS:-2}
AUTO_APPROVE_SAFE_MAPPINGS=${AUTO_APPROVE_SAFE_MAPPINGS:-YES}
ENABLE_FUNDED_CANARY=${ENABLE_FUNDED_CANARY:-NO}
CREDENTIAL_ROTATION_CONFIRMED=${CREDENTIAL_ROTATION_CONFIRMED:-NO}
CREDENTIAL_ROTATION_RISK_ACCEPTED=${CREDENTIAL_ROTATION_RISK_ACCEPTED:-NO}
CLOSEOUT_OPERATOR=${CLOSEOUT_OPERATOR:-production-closeout}
PYTHON_BIN=${PYTHON_BIN:-}
ADMIN_BIN=${ADMIN_BIN:-}
DEFER_BACKUP_GATES=${DEFER_BACKUP_GATES:-1}
LEGACY_CONFIG_PATH=${CONFIG_PATH:-}
LEGACY_COMPOSE_SERVICE=${COMPOSE_SERVICE:-}

test -d .git || { echo "run production_closeout.sh from the repo checkout" >&2; exit 1; }

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
if [[ "${ENABLE_FUNDED_CANARY}" == "YES" ]]; then
  if [[ "${CREDENTIAL_ROTATION_CONFIRMED}" != "YES" && "${CREDENTIAL_ROTATION_RISK_ACCEPTED}" != "YES" ]]; then
    echo "funded canary requires credential rotation or explicit credential risk acceptance" >&2
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
    clob_hft) echo "config.production.clob_hft.json" ;;
    quote_arb) echo "config.production.quote_arb.json" ;;
    custom) echo "${LEGACY_CONFIG_PATH}" ;;
    *) echo "unknown target: $1" >&2; exit 1 ;;
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
  case "$1" in
    clob_hft) printf '%s\n' "polymarket_sx" ;;
    quote_arb) printf '%s\n' "polymarket_predict" "polymarket_myriad" ;;
    custom)
      "${script_python[@]}" - "$LEGACY_CONFIG_PATH" <<'PY'
import sys
from arbitrage_engine.config import load_config
from arbitrage_engine.production_audit import enabled_routes
for route in enabled_routes(load_config(sys.argv[1])):
    print(route)
PY
      ;;
    *) echo "unknown target: $1" >&2; exit 1 ;;
  esac
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

all_services=()
for target in "${TARGETS[@]}"; do
  config_path=$(target_config_path "${target}")
  test -f "${config_path}" || { echo "missing config for ${target}: ${config_path}" >&2; exit 1; }
  service=$(target_compose_service "${target}")
  all_services+=("${service}")
  mkdir -p "${run_dir}/${target}"
done

export LIVE_TRADING_CONFIRM=NO
export ARBITRAGE_EXECUTION_MODE_OVERRIDE=shadow
export CLOB_HFT_EXECUTION_MODE=shadow
export QUOTE_ARB_EXECUTION_MODE=shadow
docker compose up -d postgres migrate "${all_services[@]}"
if [[ "${using_operator_container}" == "1" ]]; then
  docker compose --profile operator build operator
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

# Discovery persists candidates. Approve only the CLI's exact-ID, single-candidate,
# launch-horizon-safe set before calibration so short-lived markets reach the engine.
for target in "${TARGETS[@]}"; do
  config_path=$(target_config_path "${target}")
  run_and_capture \
    "${target}" \
    discovery-overlap-pre-approval \
    "${admin_cmd[@]}" --config "${config_path}" discovery overlap
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
docker compose up -d --force-recreate "${all_services[@]}"
for target in "${TARGETS[@]}"; do
  wait_for_shadow_mode "${target}"
done

calibration_pids=()
for target in "${TARGETS[@]}"; do
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

# Technical audit stays fail-closed in paused shadow. Canary gates are evaluated
# only after this phase passes and funded execution is explicitly enabled.

for target in "${TARGETS[@]}"; do
  config_path=$(target_config_path "${target}")
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

if [[ "${ENABLE_FUNDED_CANARY}" != "YES" ]]; then
  {
    echo "artifact_root=${run_dir}"
    echo "result=shadow_calibration_and_preflight_complete"
    echo "funded_canary_started=false"
    echo "risk_state_after_exit=paused"
    echo "next_step=set ENABLE_FUNDED_CANARY=YES only after operator sign-off and credential decision acknowledgement"
  } >"${run_dir}/SUMMARY.txt"
  echo "shadow closeout complete; funded canary was not enabled"
  exit 0
fi

# risk resume itself fails closed on unresolved intents, redemptions, manual-review
# positions, reconciliation drift, and the daily-loss limit. It happens only after
# calibration and technical openability pass and funded execution is authorized.
for target in "${TARGETS[@]}"; do
  config_path=$(target_config_path "${target}")
  run_and_capture \
    "${target}" \
    risk-resume-canary \
    "${admin_cmd[@]}" --config "${config_path}" risk resume
done

export LIVE_TRADING_CONFIRM=YES
export ARBITRAGE_EXECUTION_MODE_OVERRIDE=canary
export CLOB_HFT_EXECUTION_MODE=canary
export QUOTE_ARB_EXECUTION_MODE=canary
docker compose up -d --force-recreate "${all_services[@]}"
for target in "${TARGETS[@]}"; do
  wait_for_ready "${target}"
done

canary_pids=()
for target in "${TARGETS[@]}"; do
  config_path=$(target_config_path "${target}")
  mapfile -t routes < <(target_routes "${target}")
  for route in "${routes[@]}"; do
    canary_root="${run_dir}/${target}/canary-artifacts/${route}"
    observer_cmd=(
      "${script_python[@]}"
      scripts/live_canary_window.py
      --config "${config_path}"
      --duration-seconds "${DURATION_SECONDS}"
      --poll-seconds "${POLL_SECONDS}"
      --database-poll-seconds "${DATABASE_POLL_SECONDS}"
      --database-timeout-seconds "${DATABASE_TIMEOUT_SECONDS}"
      --stop-on timeout
      --required-route "${route}"
      --artifact-dir "${canary_root}"
      --compose-cwd .
    )
    for service in "${all_services[@]}"; do
      observer_cmd+=(--compose-service "${service}")
    done
    (
      "${observer_cmd[@]}" | tee "${run_dir}/${target}/live-canary-window-${route}.json"
    ) &
    canary_pids+=($!)
  done
done

canary_failed=0
for pid in "${canary_pids[@]}"; do
  if ! wait "${pid}"; then
    canary_failed=1
  fi
done
if [[ "${canary_failed}" == "1" ]]; then
  echo "one or more funded canary observers failed" >&2
  exit 1
fi

summary_path="${run_dir}/SUMMARY.txt"
: >"${summary_path}"
{
  echo "artifact_root=${run_dir}"
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
  echo "credential_rotation_confirmed=${CREDENTIAL_ROTATION_CONFIRMED}"
  echo "credential_rotation_risk_accepted=${CREDENTIAL_ROTATION_RISK_ACCEPTED}"
} >>"${summary_path}"

for target in "${TARGETS[@]}"; do
  config_path=$(target_config_path "${target}")
  mapfile -t routes < <(target_routes "${target}")
  final_audit_extra=(production audit --all-markets --require-live-order-evidence)
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
  run_and_capture "${target}" production-audit-final "${final_audit_cmd[@]}"
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
  } >>"${summary_path}"
done

pause_on_exit=0
echo "production closeout artifacts written to ${run_dir}"
