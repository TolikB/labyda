#!/usr/bin/env bash
set -Eeuo pipefail

if [[ ${EUID} -ne 0 ]]; then
  echo "install_systemd.sh must run as root" >&2
  exit 1
fi

if ! id arbitrage >/dev/null 2>&1; then
  useradd --system --home-dir /var/lib/arbitrage --shell /usr/sbin/nologin arbitrage
fi

install -d -o root -g arbitrage -m 0750 /etc/arbitrage
install -d -o arbitrage -g arbitrage -m 0750 \
  /var/lib/arbitrage /var/lib/arbitrage/backups /var/lib/node_exporter/textfile /opt/arbitrage/releases
install -o root -g root -m 0644 ops/systemd/arbitrage-engine.service /etc/systemd/system/arbitrage-engine.service
install -o root -g root -m 0644 ops/systemd/arbitrage-backup.service /etc/systemd/system/arbitrage-backup.service
install -o root -g root -m 0644 ops/systemd/arbitrage-backup.timer /etc/systemd/system/arbitrage-backup.timer

if [[ ! -f /etc/arbitrage/arbitrage.env ]]; then
  install -o root -g root -m 0600 ops/systemd/arbitrage.env.example /etc/arbitrage/arbitrage.env
fi
if [[ ! -f /etc/arbitrage/config.json ]]; then
  install -o root -g arbitrage -m 0640 config.example.json /etc/arbitrage/config.json
fi

systemctl daemon-reload
systemctl enable arbitrage-engine.service
systemctl enable arbitrage-backup.timer
echo "Review /etc/arbitrage/arbitrage.env and config.json before starting the service."
