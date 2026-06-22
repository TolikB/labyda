#!/usr/bin/env bash
set -Eeuo pipefail

HEALTH_URL=${HEALTH_URL:-http://127.0.0.1:9108/health/ready}
STATE_FILE=${STATE_FILE:-/run/arbitrage-watchdog.failures}
CONFIG_PATH=${CONFIG_PATH:-/etc/arbitrage/config.json}

if curl --silent --fail --max-time 3 "${HEALTH_URL}" >/dev/null; then
  printf '0\n' >"${STATE_FILE}"
  exit 0
fi

failures=0
if [[ -f ${STATE_FILE} ]]; then
  read -r failures <"${STATE_FILE}" || failures=0
fi
failures=$((failures + 1))
printf '%s\n' "${failures}" >"${STATE_FILE}"
if (( failures < 3 )); then
  exit 0
fi

/opt/arbitrage/current/.venv/bin/arbitrage-admin --config "${CONFIG_PATH}" \
  risk pause --reason "systemd health watchdog: readiness failed three times" || true
systemctl restart arbitrage-engine.service
printf '0\n' >"${STATE_FILE}"
