#!/bin/sh
# Installs the systemd units (run with sudo on the server). The unit file is
# written for an example user `alice` with uid/gid 1000; the paths and the
# container user are rewritten for the user running sudo, who owns the
# config and the data.
set -e
cd "$(dirname "$0")/systemd"
user=${SUDO_USER:-$(id -un)}
home=$(getent passwd "$user" | cut -d: -f6)
[ -n "$home" ] || { echo "cannot determine the home directory of $user" >&2; exit 1; }
uid=$(id -u "$user"); gid=$(id -g "$user")
sed "s|/home/alice|$home|g; s|--user 1000:1000|--user $uid:$gid|" gphoto-sync.service > /etc/systemd/system/gphoto-sync.service
chmod 0644 /etc/systemd/system/gphoto-sync.service
install -m 0644 gphoto-sync.timer /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now gphoto-sync.timer
systemctl list-timers gphoto-sync.timer --no-pager
