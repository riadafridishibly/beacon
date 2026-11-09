#!/usr/bin/env bash
# Ships bin/linux-amd64/beacon, the scripts and the systemd units to a
# server, then restarts the service. Usage: deploy.sh user@server
set -euo pipefail

host=${1:?usage: deploy.sh user@server}
cd "$(dirname "$0")/.."

if [ "$(ssh "$host" 'loginctl show-user "$(id -un)" --property=Linger')" != Linger=yes ]; then
	echo "$host: run 'loginctl enable-linger' for the deploy user first, or the service stops at logout" >&2
	exit 1
fi

ssh "$host" 'mkdir -p beacon/bin beacon/scripts beacon/geoip .config/systemd/user'
if ! ssh "$host" 'test -f beacon/.env'; then
	echo "$host: create ~/beacon/.env with BEACON_DATABASE_URL first" >&2
	exit 1
fi

scp -q deploy/backup.sh deploy/geoip-update.sh "$host:beacon/scripts/"
scp -q deploy/*.service deploy/*.timer "$host:.config/systemd/user/"
scp -q bin/linux-amd64/beacon "$host:beacon/bin/beacon.new"

ssh "$host" bash -s <<'REMOTE'
set -euo pipefail
cd ~/beacon/bin
# Only a healthy binary becomes the rollback target, so retrying a failed
# deploy keeps the last good one.
if curl -fsS http://127.0.0.1:8178/healthz >/dev/null 2>&1; then ln -f beacon beacon.prev; fi
mv beacon.new beacon

systemctl --user daemon-reload
systemctl --user enable beacon.service beacon-backup.timer beacon-geoip.timer
systemctl --user start beacon-backup.timer beacon-geoip.timer
if [ ! -e ~/beacon/geoip/city.mmdb ]; then
	systemctl --user start beacon-geoip.service
fi
systemctl --user restart beacon.service

for _ in $(seq 30); do
	if curl -fsS http://127.0.0.1:8178/healthz >/dev/null 2>&1; then
		echo healthy
		exit 0
	fi
	sleep 1
done
journalctl --user -u beacon.service -n 30 --no-pager || true
echo "not healthy after 30s; the previous binary is ~/beacon/bin/beacon.prev" >&2
exit 1
REMOTE
