#!/usr/bin/env bash
# Ships bin/linux-amd64/beacon, the scripts and the systemd units to a
# server, then restarts the service. Usage: deploy.sh user@server
set -euo pipefail

host=${1:?usage: deploy.sh user@server}
cd "$(dirname "$0")/.."

log() { printf '==> %s\n' "$*"; }

log "deploying to $host"

log "checking linger for the deploy user"
if [ "$(ssh "$host" 'loginctl show-user "$(id -un)" --property=Linger')" != Linger=yes ]; then
	echo "$host: run 'loginctl enable-linger' for the deploy user first, or the service stops at logout" >&2
	exit 1
fi

log "creating remote directories"
ssh "$host" 'mkdir -p beacon/bin beacon/scripts beacon/geoip .config/systemd/user'

log "checking for ~/beacon/.env"
if ! ssh "$host" 'test -f beacon/.env'; then
	echo "$host: create ~/beacon/.env with BEACON_DATABASE_URL first" >&2
	exit 1
fi

log "uploading scripts"
scp deploy/backup.sh deploy/geoip-update.sh "$host:beacon/scripts/"
log "uploading systemd units"
scp deploy/*.service deploy/*.timer "$host:.config/systemd/user/"
log "uploading binary"
scp bin/linux-amd64/beacon "$host:beacon/bin/beacon.new"

ssh "$host" bash -s <<'REMOTE'
set -euo pipefail
log() { printf '==> %s\n' "$*"; }

cd ~/beacon/bin
# Only a healthy binary becomes the rollback target, so retrying a failed
# deploy keeps the last good one.
if curl -fsS http://127.0.0.1:8178/healthz >/dev/null 2>&1; then
	log "current binary is healthy, keeping it as the rollback target"
	ln -fv beacon beacon.prev
else
	log "current binary is not healthy, leaving beacon.prev alone"
fi
log "installing new binary"
mv -v beacon.new beacon

log "reloading systemd and enabling units"
systemctl --user daemon-reload
systemctl --user enable beacon.service beacon-backup.timer beacon-geoip.timer
log "starting timers"
systemctl --user start beacon-backup.timer beacon-geoip.timer
if [ ! -e ~/beacon/geoip/city.mmdb ]; then
	log "no geoip database, downloading one"
	systemctl --user start beacon-geoip.service
fi
log "restarting beacon.service"
systemctl --user restart beacon.service

log "waiting for /healthz"
for i in $(seq 30); do
	if curl -fsS http://127.0.0.1:8178/healthz >/dev/null 2>&1; then
		log "healthy on attempt $i/30"
		exit 0
	fi
	log "not ready, attempt $i/30"
	sleep 1
done
journalctl --user -u beacon.service -n 30 --no-pager || true
echo "not healthy after 30s; the previous binary is ~/beacon/bin/beacon.prev" >&2
exit 1
REMOTE
