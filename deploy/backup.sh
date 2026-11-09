#!/usr/bin/env bash
# Dumps $BEACON_DATABASE_URL to ~/beacon/backups and keeps the newest 14.
set -euo pipefail

dir=${BEACON_BACKUP_DIR:-$HOME/beacon/backups}
keep=${BEACON_BACKUP_KEEP:-14}
mkdir -p "$dir"

tmp=$(mktemp "$dir/.partial.XXXXXX")
trap 'rm -f "$tmp"' EXIT
pg_dump --format=custom --no-owner --dbname="$BEACON_DATABASE_URL" --file="$tmp"
mv "$tmp" "$dir/beacon-$(date -u +%Y%m%dT%H%M%SZ).dump"

printf '%s\n' "$dir"/beacon-*.dump | sort -r | tail -n +$((keep + 1)) |
	while read -r old; do rm -f -- "$old"; done
