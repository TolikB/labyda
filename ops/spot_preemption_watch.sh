#!/usr/bin/env bash
set -Eeuo pipefail

METADATA_URL=${METADATA_URL:-http://metadata.google.internal/computeMetadata/v1/instance/preempted}
CONFIG_PATH=${CONFIG_PATH:-/etc/arbitrage/config.json}
MARKER_PATH=${MARKER_PATH:-/var/lib/arbitrage/drain-ready.json}

while sleep 5; do
  preempted=$(curl --silent --fail --max-time 2 -H 'Metadata-Flavor: Google' "${METADATA_URL}" || true)
  if [[ ${preempted^^} != TRUE ]]; then
    continue
  fi
  /opt/arbitrage/current/.venv/bin/arbitrage-admin --config "${CONFIG_PATH}" \
    production drain --reason "gcp-spot-preemption" --marker "${MARKER_PATH}"
  systemctl stop arbitrage-engine.service
  exit 0
done
