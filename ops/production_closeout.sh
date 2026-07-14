#!/usr/bin/env bash
set -Eeuo pipefail

ARTIFACT_ROOT=${ARTIFACT_ROOT:-$(pwd)/closeout-artifacts}
DURATION_SECONDS=${DURATION_SECONDS:-7200}
POLL_SECONDS=${POLL_SECONDS:-15}
CALIBRATION_DURATION_SECONDS=${CALIBRATION_DURATION_SECONDS:-3600}
CALIBRATION_MIN_EVALUATIONS=${CALIBRATION_MIN_EVALUATIONS:-10000}
ENABLE_FUNDED_CANARY=${ENABLE_FUNDED_CANARY:-NO}
PYTHON_BIN=${PYTHON_BIN:-python}
ADMIN_BIN=${ADMIN_BIN:-}
DEFER_BACKUP_GATES=${DEFER_BACKUP_GATES:-1}
LEGACY_CONFIG_PATH=${CONFIG_PATH:-}
LEGACY_COMPOSE_SERVICE=${COMPOSE_SERVICE:-}

test -d .git || { echo "run production_closeout.sh from the repo checkout" >&2; exit 1; }

export ARBITRAGE_DATABASE_HOST_OVERRIDE=${ARBITRAGE_DATABASE_HOST_OVERRIDE:-127.0.0.1}
export ARBITRAGE_DATABASE_PORT_OVERRIDE=${ARBITRAGE_DATABASE_PORT_OVERRIDE:-5432}
test -n "${CI_VERIFIED_COMMIT_SHA:-}" || {
  echo "CI_VERIFIED_COMMIT_SHA from the successful CI artifact is required" >&2
  exit 1
}

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

calibration_pids=()
for target in "${TARGETS[@]}"; do
  config_path=$(target_config_path "${target}")
  (
    "${script_python[@]}" scripts/shadow_calibration.py \
      --config "${config_path}" \
      --duration-seconds "${CALIBRATION_DURATION_SECONDS}" \
      --poll-seconds "${POLL_SECONDS}" \
      --min-valid-evaluations "${CALIBRATION_MIN_EVALUATIONS}" \
      --artifact-dir "${run_dir}/${target}" \
      --write-config \
      | tee "${run_dir}/${target}/shadow-calibration-console.json"
  ) &
  calibration_pids+=($!)
done

for pid in "${calibration_pids[@]}"; do
  wait "${pid}"
done

# Audit the exact canary contract while the running services remain in shadow mode.
export LIVE_TRADING_CONFIRM=YES
export ARBITRAGE_EXECUTION_MODE_OVERRIDE=canary

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

  mapfile -t target_audit_cmd < <(audit_args "${config_path}" production audit --all-markets)
  run_and_capture "${target}" production-audit-pre-live "${target_audit_cmd[@]}"
done

if [[ "${ENABLE_FUNDED_CANARY}" != "YES" ]]; then
  {
    echo "artifact_root=${run_dir}"
    echo "result=shadow_calibration_and_preflight_complete"
    echo "funded_canary_started=false"
    echo "next_step=set ENABLE_FUNDED_CANARY=YES only after credential rotation and operator sign-off"
  } >"${run_dir}/SUMMARY.txt"
  echo "shadow closeout complete; funded canary was not enabled"
  exit 0
fi

export LIVE_TRADING_CONFIRM=YES
export ARBITRAGE_EXECUTION_MODE_OVERRIDE=canary
export CLOB_HFT_EXECUTION_MODE=canary
export QUOTE_ARB_EXECUTION_MODE=canary
docker compose up -d --force-recreate "${all_services[@]}"

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

for pid in "${canary_pids[@]}"; do
  wait "${pid}"
done

summary_path="${run_dir}/SUMMARY.txt"
: >"${summary_path}"
{
  echo "artifact_root=${run_dir}"
  echo "defer_backup_gates=${DEFER_BACKUP_GATES}"
  echo "duration_seconds=${DURATION_SECONDS}"
  echo "poll_seconds=${POLL_SECONDS}"
  echo "calibration_duration_seconds=${CALIBRATION_DURATION_SECONDS}"
  echo "calibration_min_evaluations=${CALIBRATION_MIN_EVALUATIONS}"
  echo "funded_canary_started=true"
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

echo "production closeout artifacts written to ${run_dir}"
