#!/usr/bin/env bash
set -Eeuo pipefail

device=/dev/disk/by-id/google-state
mount_point=/srv/arbitrage-state
for _ in $(seq 1 60); do
  [[ -e ${device} ]] && break
  sleep 1
done
test -e "${device}"
if ! blkid "${device}" >/dev/null 2>&1; then
  mkfs.ext4 -F -L arbitrage-state "${device}"
fi
install -d -m 0750 "${mount_point}"
grep -q "${mount_point}" /etc/fstab || printf 'LABEL=arbitrage-state %s ext4 defaults,nofail 0 2\n' "${mount_point}" >>/etc/fstab
mount "${mount_point}" || mount -a
install -d -m 0750 "${mount_point}/app" "${mount_point}/postgresql" "${mount_point}/backups"
