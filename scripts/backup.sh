#!/bin/sh

set -eu

# The directory in which you want backups to be stored.
BASE_DIR="${HOME}/snarkOS/aleo_ledger_checkpoints/mainnet/"
# The node REST endpoint to trigger backup creation from.
ENDPOINT="http://localhost:3030/v2/mainnet/db_backup"
# The JWT to authenticate to the endpoint. You can either:
# 1. Run a node with --nojwt, in which case this value will be safely ignored.
# 2. Run a node with --jwt-secret and --jwt-timestamp, in which case the jwt will be stored in jwt_secret_{address}.txt
#    You can generate a jwt secret with: dd if=/dev/urandom bs=16 count=1 2>/dev/null | uuencode -m - | sed -n '2p'
#    You can generate a timestamp with: date +%s
JWT="eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJhbGVvMXJoZ2R1NzdoZ3lxZDN4amo4dWN1M2pqOXIya3J3ejZtbnp5ZDgwZ25jcjVmeGN3bGg1cnN2enA5cHgiLCJpYXQiOjE3NDkxMTYzNDUsImV4cCI6MjA2NDQ3NjM0NX0.LiqFGiQds3OGHGJ5K3xi359g-uTQBZCrAskGj9UWAbM"

create_backup() {
    slot="$1"
    slot_path="${BASE_DIR}/${slot}"
    rm -rf -- "$slot_path"
    curl -fsS -X POST -H "Authorization: Bearer ${JWT}" "${ENDPOINT}?path=${slot_path}"
}

# Refresh helper: if missing OR older than N minutes, refresh.
# Using +N-1 so "older than N minutes" triggers at >= N.
refresh_if_older_than() {
    slot="$1"
    mins="$2"          # e.g. 5, 60, 1440
    slot_path="${BASE_DIR}/${slot}"

    if [ ! -e "${slot_path}" ]; then
        create_backup "${slot}"
        return
    fi

    # Older than N minutes? (>= N) -> use -mmin +N-1
    nminus1=$((mins - 1))
    if find "${slot_path}" -prune -mmin +"${nminus1}" | grep -q .; then
        create_backup "${slot}"
    fi
}

# Ensure base dir exists
mkdir -p -- "${BASE_DIR}"

# Always overwrite latest
create_backup "latest"

# Overwrite only if older than thresholds
refresh_if_older_than "5min" 5
refresh_if_older_than "1hour" 60
refresh_if_older_than "1day" 1440
