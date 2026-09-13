#!/bin/bash
set -euo pipefail

exec > >(tee /var/log/codex_startup_probe.log /dev/ttyS0) 2>&1

echo "CODEX_STARTUP_PROBE_BEGIN $(date -Is)"
hostname
id
echo "HOME=$HOME"
echo "PWD=$(pwd)"

if [ -d /opt/labyda_next ]; then
  echo "VM_CHECKOUT_OK /opt/labyda_next"
else
  echo "VM_CHECKOUT_MISSING /opt/labyda_next"
  exit 11
fi

cd /opt/labyda_next
echo "VM_REPO_PWD=$(pwd)"

if [ -f config.production.quote_arb.json ]; then
  echo "VM_CONFIG_OK config.production.quote_arb.json"
else
  echo "VM_CONFIG_MISSING production config"
  exit 12
fi

command -v docker
docker --version
docker compose version
docker compose --project-name labyda_next --env-file .env.production ps

echo "CODEX_STARTUP_PROBE_END $(date -Is)"
