#!/usr/bin/env bash
set -Eeuo pipefail

BASE_URL=${BASE_URL:-http://127.0.0.1:9108}
OUTPUT_ROOT=${OUTPUT_ROOT:-$(pwd)/shadow-smoke-artifacts}
DURATION_SECONDS=${DURATION_SECONDS:-600}
INTERVAL_SECONDS=${INTERVAL_SECONDS:-15}
LOG_MODE=${LOG_MODE:-docker-compose}
COMPOSE_SERVICE=${COMPOSE_SERVICE:-bot}
JOURNAL_UNIT=${JOURNAL_UNIT:-arbitrage-engine.service}

run_id=$(date -u +%Y%m%dT%H%M%SZ)
started_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)
run_dir="${OUTPUT_ROOT}/${run_id}"
mkdir -p "${run_dir}/health/live" "${run_dir}/health/ready" "${run_dir}/metrics"

sample_endpoint() {
  local path=$1
  local target_dir=$2
  local stamp=$3
  local body_file="${target_dir}/${stamp}.body"
  local code

  code=$(curl --silent --show-error --output "${body_file}" --write-out "%{http_code}" --max-time 5 "${BASE_URL}${path}" || true)
  printf "%s\t%s\t%s\n" "${stamp}" "${path}" "${code}" >>"${run_dir}/samples.tsv"
}

start_epoch=$(date +%s)
next_epoch=${start_epoch}

while :; do
  now_epoch=$(date +%s)
  elapsed=$((now_epoch - start_epoch))
  if (( elapsed >= DURATION_SECONDS )); then
    break
  fi

  if (( now_epoch < next_epoch )); then
    sleep $((next_epoch - now_epoch))
  fi

  stamp=$(date -u +%Y%m%dT%H%M%SZ)
  sample_endpoint "/health/live" "${run_dir}/health/live" "${stamp}"
  sample_endpoint "/health/ready" "${run_dir}/health/ready" "${stamp}"
  curl --silent --show-error --max-time 10 "${BASE_URL}/metrics" >"${run_dir}/metrics/${stamp}.prom" || true
  next_epoch=$((next_epoch + INTERVAL_SECONDS))
done

case "${LOG_MODE}" in
  docker-compose)
    docker compose logs --since "${started_at}" --no-color "${COMPOSE_SERVICE}" >"${run_dir}/${COMPOSE_SERVICE}.log" || true
    ;;
  journal)
    journalctl -u "${JOURNAL_UNIT}" --since "${started_at}" --no-pager >"${run_dir}/${JOURNAL_UNIT}.log" || true
    ;;
  none)
    ;;
  *)
    echo "unknown LOG_MODE=${LOG_MODE}; expected docker-compose, journal, or none" >&2
    exit 1
    ;;
esac

cat >"${run_dir}/README.txt" <<EOF
started_at=${started_at}
base_url=${BASE_URL}
duration_seconds=${DURATION_SECONDS}
interval_seconds=${INTERVAL_SECONDS}
log_mode=${LOG_MODE}
EOF

echo "shadow smoke artifacts written to ${run_dir}"
