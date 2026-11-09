#!/usr/bin/env bash
# Installs this month's DB-IP City Lite at $BEACON_GEOIP_DB. The server
# won't start on an unreadable database, so the file is checked before the
# rename.
set -euo pipefail

target=${BEACON_GEOIP_DB:?BEACON_GEOIP_DB is not set}
url=https://download.db-ip.com/free/dbip-city-lite-$(date -u +%Y-%m).mmdb.gz

mkdir -p "$(dirname "$target")"
tmp=$(mktemp "$target.XXXXXX")
trap 'rm -f "$tmp"' EXIT
curl -fsSL --retry 3 "$url" | gunzip >"$tmp"
if ! LC_ALL=C grep -qa 'MaxMind.com' "$tmp"; then
	echo "$url: not an MMDB file" >&2
	exit 1
fi
mv "$tmp" "$target"
