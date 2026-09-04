#!/usr/bin/env bash
set -Eeuo pipefail

COMPOSE_ENV_FILE=${COMPOSE_ENV_FILE:-.env.production}

test -f docker-compose.yml || { echo "operator_python.sh requires the Compose checkout cwd" >&2; exit 1; }
test -f "${COMPOSE_ENV_FILE}" || { echo "missing Compose env file: ${COMPOSE_ENV_FILE}" >&2; exit 1; }
test -S /var/run/docker.sock || { echo "Docker socket is unavailable" >&2; exit 1; }

export OPERATOR_UID=${OPERATOR_UID:-$(id -u)}
export OPERATOR_GID=${OPERATOR_GID:-$(id -g)}
export DOCKER_GID=${DOCKER_GID:-$(stat -c %g /var/run/docker.sock)}
export COMPOSE_PROJECT_NAME=${COMPOSE_PROJECT_NAME:-$(basename "$(pwd)")}
export OPERATOR_WORKSPACE=${OPERATOR_WORKSPACE:-$(pwd)}

compose=(docker compose --env-file "${COMPOSE_ENV_FILE}" -f docker-compose.yml --profile operator)
if [[ "${ARBITRAGE_OPERATOR_SKIP_BUILD:-NO}" != "YES" ]]; then
  "${compose[@]}" build operator >&2
fi
exec "${compose[@]}" run --rm --no-deps operator "$@"
