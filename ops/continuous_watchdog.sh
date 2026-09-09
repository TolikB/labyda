#!/usr/bin/env bash
# Tell a human about the failures the engine cannot report itself.
#
# The engine announces its own risk pauses over Telegram, and the continuous
# wrapper announces its own holds and stops. Neither can announce being dead:
# an OOM kill, a wrapper that exited without running its trap, a box that filled
# its disk, a loop that is technically running but has not produced a window in
# hours. Unattended operation means nobody is looking at the console, so those
# are precisely the failures that would otherwise go unnoticed for days.
#
# Alerts fire on transitions, not on every check. A problem that is still there
# five minutes later is not news; a problem that has just appeared, or has just
# cleared, is.
set -Eeuo pipefail

CONFIG_PATH=${CONFIG_PATH:-/opt/labyda_next/config.production.quote_arb.json}
REPO_DIR=${REPO_DIR:-/opt/labyda_next}
WATCHDOG_SERVICE=${WATCHDOG_SERVICE:-labyda-continuous.service}
WATCHDOG_STATE_FILE=${WATCHDOG_STATE_FILE:-/run/labyda-watchdog.state}
WATCHDOG_MIN_FREE_DISK_GB=${WATCHDOG_MIN_FREE_DISK_GB:-10}
WATCHDOG_DAILY_REPORT_MAX_AGE_MINUTES=${WATCHDOG_DAILY_REPORT_MAX_AGE_MINUTES:-300}
WATCHDOG_METRICS_URLS=${WATCHDOG_METRICS_URLS:-http://127.0.0.1:9108/metrics http://127.0.0.1:9109/metrics}
PYTHON_BIN=${PYTHON_BIN:-python3}

problems=()

service_state=inactive
if systemctl is-active --quiet "${WATCHDOG_SERVICE}"; then
  service_state=active
fi

for url in ${WATCHDOG_METRICS_URLS}; do
  if ! curl -fsS --max-time 5 "${url}" >/dev/null 2>&1; then
    problems+=("metrics_unreachable:${url}")
  fi
done

free_gb=$(df -BG --output=avail "${REPO_DIR}" 2>/dev/null | tail -n 1 | tr -dc '0-9')
if [[ -z "${free_gb}" ]]; then
  problems+=("disk_unreadable")
elif ((free_gb < WATCHDOG_MIN_FREE_DISK_GB)); then
  problems+=("disk_low:${free_gb}GB")
fi

# A running wrapper writes the day's report after every window. If today's file
# has gone stale, the loop is alive but not turning over -- a stuck window, a
# hung observer -- which no other signal would surface.
if [[ "${service_state}" == "active" ]]; then
  daily_report=$(find "${REPO_DIR}/closeout-artifacts" -path '*/daily/*.json' -printf '%T@ %p\n' 2>/dev/null \
    | sort -n | tail -n 1 | cut -d' ' -f2- || true)
  if [[ -z "${daily_report}" ]]; then
    : # A run that has not completed its first window yet has nothing to report.
  elif [[ -z $(find "${daily_report}" -mmin "-${WATCHDOG_DAILY_REPORT_MAX_AGE_MINUTES}" 2>/dev/null) ]]; then
    problems+=("daily_report_stale:$(basename "${daily_report}")")
  fi
fi

previous_service_state=unknown
previous_problems=""
if [[ -r "${WATCHDOG_STATE_FILE}" ]]; then
  # shellcheck disable=SC1090
  source "${WATCHDOG_STATE_FILE}" || true
  previous_service_state=${SERVICE_STATE:-unknown}
  previous_problems=${PROBLEMS:-}
fi

current_problems=$(printf '%s\n' "${problems[@]+"${problems[@]}"}" | sort | tr '\n' ' ' | sed 's/ *$//')

notify() {
  "${PYTHON_BIN}" "${REPO_DIR}/scripts/notify_operator.py" \
    --config "${CONFIG_PATH}" --text "$1" >/dev/null 2>&1 || true
}

# Only a service that *was* running and has stopped is worth reporting. A unit
# the operator has deliberately not started must not page anybody every five
# minutes.
if [[ "${previous_service_state}" == "active" && "${service_state}" != "active" ]]; then
  notify "🔻 <b>${WATCHDOG_SERVICE} is no longer running</b>
Host: $(hostname)
Last exit: $(systemctl show -p Result --value "${WATCHDOG_SERVICE}" 2>/dev/null || echo unknown)
The runtime should be paused; check the closeout artifacts before restarting."
fi

if [[ -n "${current_problems}" && "${current_problems}" != "${previous_problems}" ]]; then
  notify "⚠️ <b>labyda watchdog</b>
Host: $(hostname)
Problems: ${current_problems}"
elif [[ -z "${current_problems}" && -n "${previous_problems}" ]]; then
  notify "✅ <b>labyda watchdog clear</b>
Host: $(hostname)
Resolved: ${previous_problems}"
fi

mkdir -p "$(dirname "${WATCHDOG_STATE_FILE}")"
{
  printf 'SERVICE_STATE=%q\n' "${service_state}"
  printf 'PROBLEMS=%q\n' "${current_problems}"
} >"${WATCHDOG_STATE_FILE}"
