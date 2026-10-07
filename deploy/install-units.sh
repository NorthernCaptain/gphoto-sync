#!/bin/sh
# Installs the systemd units (run with sudo on the server).
set -e
cd "$(dirname "$0")/systemd"
install -m 0644 gphoto-sync.service gphoto-sync.timer /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now gphoto-sync.timer
systemctl list-timers gphoto-sync.timer --no-pager
