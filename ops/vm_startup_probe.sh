#!/bin/bash
set -euo pipefail

exec > >(tee /var/log/codex_startup_probe.log /dev/ttyS0) 2>&1

echo "CODEX_STARTUP_PROBE_BEGIN $(date -Is)"
hostname
id
echo "HOME=$HOME"
echo "PWD=$(pwd)"

if [ -d /home/tolik1992s/labyda_next ]; then
  echo "VM_CHECKOUT_OK /home/tolik1992s/labyda_next"
else
  echo "VM_CHECKOUT_MISSING /home/tolik1992s/labyda_next"
  exit 11
fi

cd /home/tolik1992s/labyda_next
echo "VM_REPO_PWD=$(pwd)"

if [ -f config.production.json ]; then
  echo "VM_CONFIG_OK config.production.json"
else
  echo "VM_CONFIG_MISSING config.production.json"
  exit 12
fi

command -v docker
docker --version
docker compose version
docker compose ps

echo "CODEX_STARTUP_PROBE_END $(date -Is)"
