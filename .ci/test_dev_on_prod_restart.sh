#!/usr/bin/env bash

set -euo pipefail

if [ "${BASH_VERSINFO[0]}" -lt 5 ]; then
  echo "Error: This script requires bash version 5.0 or higher."
  exit 1
fi

# shellcheck source=SCRIPTDIR/utils.sh
. ./.ci/utils.sh

network_id=0
NETWORK_NAME=$(get_network_name "$network_id")
REST_PORT=3030
NUM_DEV_NODES=4
SETUP_ADVANCE_BLOCKS=5
DEV_ADVANCE_BLOCKS=10
SETUP_MAX_WAIT=40
DEV_MAX_WAIT=100

BOOTSTRAP_PID=""
SNARKOS_SETUP_BIN="snarkos"

function wait_for_node_ready() {
  local port="$1"
  local timeout="$2"
  local start
  start=$(now)

  while (( $(elapsed_since "$start") < timeout )); do
    local height
    height=$(get_block_height_by_port "$port" "$NETWORK_NAME" 2)
    if [ -n "$height" ]; then
      log "Node on port ${port} is ready at height ${height}"
      return 0
    fi
    log "Sleeping for 2 seconds before retrying to get height"
    sleep 2
  done

  log "Timed out waiting for node on port ${port} to become ready"
  return 1
}

function wait_for_height_advance() {
  local port="$1"
  local advance_by="$2"
  local timeout="$3"
  local label="$4"

  wait_for_node_ready "$port" "$timeout"

  local start_height
  start_height=$(get_block_height_by_port "$port" "$NETWORK_NAME" 2)
  if [ -z "$start_height" ]; then
    log "${label}: failed to read initial block height from port ${port}"
    return 1
  fi

  local target_height=$((start_height + advance_by))
  local start_time
  start_time=$(now)
  local last_log_time=0

  log "${label}: waiting for height to advance by ${advance_by} blocks (${start_height} -> ${target_height})"
  while (( $(elapsed_since "$start_time") < timeout )); do
    local current_height
    current_height=$(get_block_height_by_port "$port" "$NETWORK_NAME" 2)
    if [ -n "$current_height" ] && (( current_height >= target_height )); then
      log "${label}: reached target height ${current_height} (>= ${target_height})"
      return 0
    fi

    local elapsed
    elapsed=$(elapsed_since "$start_time")
    if (( elapsed - last_log_time >= 15 )); then
      if [ -n "$current_height" ]; then
        log "${label}: current height ${current_height}, target ${target_height}"
      else
        log "${label}: waiting for REST endpoint on port ${port}"
      fi
      last_log_time=$elapsed
    fi
    log "Sleeping for 2 seconds before retrying to get height"
    sleep 2
  done

  local final_height
  final_height=$(get_block_height_by_port "$port" "$NETWORK_NAME" 2)
  log "${label}: timed out waiting for height advance (final height: ${final_height:-unavailable}, target: ${target_height})"
  return 1
}

function graceful_stop_all_dev_nodes() {
  for i in "${!PIDS[@]}"; do
    graceful_stop_pid "${PIDS[$i]}" "dev-node-${i}"
  done
  PIDS=()
}

function cleanup() {
  graceful_stop_all_dev_nodes
  graceful_stop_pid "$BOOTSTRAP_PID" "setup-node"
}

function start_setup_node() {
  mkdir -p dev_logs
  log "Starting production setup node: ${SNARKOS_SETUP_BIN} start --client --nodisplay"
  "$SNARKOS_SETUP_BIN" start --client --nodisplay > "dev_logs/setup-client.txt" 2>&1 &
  BOOTSTRAP_PID=$!
  log "Started setup node (pid=${BOOTSTRAP_PID})"
}

function copy_setup_ledger() {
  local source="${HOME}/.aleo/storage/ledger-0"
  if [ ! -d "$source" ]; then
    log "Missing source ledger at ${source}"
    exit 1
  fi

  log "Copying setup ledger into local ledgers"
  rm -rf ledger-0 ledger-1 ledger-2 ledger-3
  cp -r "$source" ledger-0
  cp -r "$source" ledger-1
  cp -r "$source" ledger-2
  cp -r "$source" ledger-3

  # Clear any state cached by a previous test run (proposal cache, dev committee state, etc.).
  # The ledger we just copied is fresh, so any persisted dev state must be regenerated from it.
  # Pass --node-data-storage so clean targets the same custom path start_dev_nodes uses.
  for i in $(seq 0 $((NUM_DEV_NODES - 1))); do
    snarkos clean "--dev=${i}" "--network=0" --node-data-storage "node-data-${i}"
  done
}

function start_dev_nodes() {
  mkdir -p dev_logs
  PIDS=()

  for i in $(seq 0 $((NUM_DEV_NODES - 1))); do
    log "Starting dev node ${i}"
    snarkos start --nodisplay --validator --ledger-storage "ledger-${i}" --node-data-storage "node-data-${i}" --dev "${i}" \
      --no-dev-txs --nocdn --dev-num-validators "${NUM_DEV_NODES}" --verbosity 2 \
      --allow-external-peers --logfile "dev_logs/val-${i}.txt" --dev-on-prod &
    PIDS[i]=$!
    sleep 1
  done
}

trap cleanup EXIT
trap 'log "Error at line $LINENO while running: $BASH_COMMAND"' ERR

init_log_dir
require_cmd snarkos
require_cmd curl
require_cmd cargo
require_cmd tar

SNARKOS_SETUP_BIN="snarkos"
log "Using setup binary: ${SNARKOS_SETUP_BIN}"

log "Step 1: Start production node and wait for +${SETUP_ADVANCE_BLOCKS} blocks"
start_setup_node
wait_for_height_advance "$REST_PORT" "$SETUP_ADVANCE_BLOCKS" "$SETUP_MAX_WAIT" "setup-network"

log "Step 2: Gracefully stop production node"
graceful_stop_pid "$BOOTSTRAP_PID" "setup-node"
BOOTSTRAP_PID=""

log "Step 3: Copy production ledger and start 4 dev nodes"
copy_setup_ledger
start_dev_nodes

log "Step 4: Wait until dev network advances by +${DEV_ADVANCE_BLOCKS} blocks"
wait_for_height_advance "$REST_PORT" "$DEV_ADVANCE_BLOCKS" "$DEV_MAX_WAIT" "dev-committee-first-run"

log "Step 5: Gracefully stop all dev nodes"
graceful_stop_all_dev_nodes

log "Step 6: Restart all dev nodes and wait for +${DEV_ADVANCE_BLOCKS} blocks"
start_dev_nodes
wait_for_height_advance "$REST_PORT" "$DEV_ADVANCE_BLOCKS" "$DEV_MAX_WAIT" "dev-committee-second-run"

log "SUCCESS: Completed dev-on-prod restart flow"
