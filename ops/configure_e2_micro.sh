#!/usr/bin/env bash
set -Eeuo pipefail

if [[ ${EUID} -ne 0 ]]; then
  echo "configure_e2_micro.sh must run as root" >&2
  exit 1
fi

apt-get update
apt-get install -y --no-install-recommends rsync systemd-zram-generator

cat >/etc/systemd/zram-generator.conf <<'EOF'
[zram0]
zram-size = min(ram / 2, 512)
compression-algorithm = zstd
swap-priority = 100
EOF

postgres_conf=$(find /etc/postgresql -type d -path '*/main/conf.d' | sort | tail -n 1)
test -n "${postgres_conf}"
source_data=$(runuser -u postgres -- psql --no-align --tuples-only -c 'show data_directory')
target_data=/srv/arbitrage-state/postgresql/main
systemctl stop postgresql.service
install -d -o postgres -g postgres -m 0700 "${target_data}"
if [[ ! -f ${target_data}/PG_VERSION ]]; then
  rsync -a --delete "${source_data}/" "${target_data}/"
  chown -R postgres:postgres "${target_data}"
fi
install -o postgres -g postgres -m 0644 ops/postgresql-e2-micro.conf \
  "${postgres_conf}/90-arbitrage-e2-micro.conf"

systemctl daemon-reload
systemctl restart systemd-zram-setup@zram0.service
systemctl start postgresql.service
swapon --show
