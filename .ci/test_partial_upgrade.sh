#!/bin/bash

####################################################
# Partial network upgrade test:
#
# 1. Download latest snarkOS release source from GitHub and build it.
# 2. Probe stable consensus versions from both binaries by spawning temporary
#    devnets and waiting for consensus to stabilize.
# 3. Sanity check: If the consensus version is unchanged between the release
#    and current binaries, return early with "success".
# 4. Start the devnet with latest release binaries.
# 5. Wait for block height to be greater than that of the new consensus version.
# 6. Stop the penultimate node (but do not restart it yet).
# 7. Start the last node with the new binary and ensure it does NOT sync
#    beyond the new consensus height (because all other nodes are outdated).
# 8. Start the penultimate node with the new binary and ensure the last node
#    syncs up to the new consensus version's activation height.
####################################################

set -eo pipefail  # error on any command failure

# --- Parameters from CLI ---
total_validators=$1
network_id=$2

# Default values if not provided
: "${total_validators:=4}"
: "${network_id:=0}"

# Node verbosity
NODE_VERBOSITY=4
# Max logfile size (bytes) for regression checks.
MAX_VALIDATOR_LOG_SIZE_BYTES=$((4 * 1024 * 1024))

# How long to wait between upgrades (seconds); used for block-height window
WAIT_BETWEEN_UPGRADES="${WAIT_BETWEEN_UPGRADES:-60}"

# Load shared helpers (is_integer, get_network_name, wait_for_nodes, stop_nodes, ...)
# shellcheck source=SCRIPTDIR/utils.sh
. ./.ci/utils.sh

# Create the log directory
init_log_dir

# Reuse the same target dir for all builds (release + PR) to get incremental builds.
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$PWD/.ci/target}"
log "Using CARGO_TARGET_DIR=${CARGO_TARGET_DIR}"

########################################
# Consensus + height helpers
########################################

SNARKOS_CURRENT_BIN="${SNARKOS_CURRENT_BIN:-snarkos}"

network_name=$(get_network_name "$network_id")
log "Using network: $network_name (ID: $network_id)"

# Wait for a node to NOT sync beyond a certain height for a given duration.
# Returns 0 if the node stays below max_height for the entire wait_time.
# Returns 1 if the node syncs beyond max_height.
function wait_for_no_sync() {
  local node_index=$1
  local network_name=$2
  local max_height=$3
  local wait_time=$4  # seconds to wait

  local start
  start=$(now)
  
  while (( $(elapsed_since "$start") < wait_time )); do
    height=$(get_block_height "$node_index" "$network_name" 2>/dev/null || echo 0)
    if (is_integer "$height") && (( height > max_height )); then
      log "Node #$node_index unexpectedly synced to height $height (expected to stay below $max_height)"
      return 1
    fi
    log "Node #$node_index height is $height (expected to stay below $max_height)"
    sleep 10
  done

  log "Node #$node_index correctly did not sync beyond height $max_height for ${wait_time}s"
  return 0
}

declare -a PIDS

# Handler that stops all nodes on shutdown.
# shellcheck disable=SC2329
exit_handler() {
  stop_probe_nodes || true
  stop_nodes || true
}

# Install signal handlers.
trap exit_handler EXIT
trap 'echo "Error in $BASH_SOURCE at line $LINENO: \"$BASH_COMMAND\" failed (exit $?)"' ERR

common_flags=(
  --nodisplay "--network=$network_id" "--verbosity=$NODE_VERBOSITY"
  "--dev-num-validators=$total_validators"
)

function start_node() {
  local bin="$1"
  local node_index="$2"
  local role="$3"    # "validator" or "client"
  local log_file="$4"

  local flags=( "${common_flags[@]}" "--dev=$node_index" )

  if [ "$role" = "validator" ]; then
    # The set of other validators to connect to.
    # NOTE: In newever versions of snarkOS, the set of peers is populated automatically through `--dev-num-validators`
    # and this code can be removed evetually. 
    trusted_validators=""
    for ((peer_index = 0; peer_index < total_validators; peer_index++)); do
      if [ "$peer_index" -eq "$node_index" ]; then
        continue
      else
        # append "," if this is not the first trusted validator 
        if [ -n "$trusted_validators" ]; then
          trusted_validators+=","
        fi
        trusted_validators+="127.0.0.1:$((5000+peer_index))"
      fi
    done

    flags+=( --validator "--logfile=$log_file" "--validators=$trusted_validators" )
    if [ "$node_index" -eq 0 ]; then
      flags+=( --metrics --no-dev-txs )
    fi

    # TODO Remove once old nodes are no longer on v4.4.0!
    if [ "$bin" = "$SNARKOS_CURRENT_BIN" ]; then
      flags+=( --auto-migrate-node-data )
    fi
  else
    flags+=( --client "--logfile=$log_file" )
  fi

  run_with_prefix "$role-$node_index" "$bin" start "${flags[@]}"
  PIDS[node_index]=$!
  log "Started $role $node_index with PID ${PIDS[node_index]} using $(basename "$bin")"
}

function stop_node() {
  local node_index="$1"
  local pid="${PIDS[node_index]:-}"

  if [ -z "$pid" ]; then
    return 0
  fi

  if kill -0 "$pid" >/dev/null 2>&1; then
    log "Stopping node index $node_index (PID $pid)…"
    kill "$pid" || true
    local waited=0
    while kill -0 "$pid" >/dev/null 2>&1 && (( waited < 30 )); do
      sleep 1
      waited=$((waited + 1))
    done
    if kill -0 "$pid" >/dev/null 2>&1; then
      log "PID $pid did not exit in time, sending SIGKILL"
      kill -9 "$pid" || true
    fi
  fi
}
 
function wait_for_node_height() {
  local node_index=$1
  local network_name=$2
  local target_height=$3

  local start
  start=$(now)

  while (( $(elapsed_since "$start") < 300 )); do  #5 minutes max
      height=$(get_block_height "$node_index" "$network_name" || echo 0)
      if (is_integer "$height") && (( height >= target_height )); then
        return 0
      fi

      log "Node #$node_index has not reached height $target_height yet"
      sleep 10
  done

  log "Test failed! Node did not sync in time"
  return 1
}

########################################
# MAIN
########################################

log "Starting test_partial_upgrade.sh"
log "total_validators=${total_validators}, network_id=${network_id}"
log "WAIT_BETWEEN_UPGRADES=${WAIT_BETWEEN_UPGRADES}"

# 1. Build release snarkos once.
download_and_build_latest_snarkos 0

# 2. Probe stable consensus versions from both binaries.
# We spawn temporary devnets and wait for consensus to stabilize to determine
# the latest consensus version and its activation height.

log "Probing stable consensus version from current binary..."
current_probe_result=$(probe_stable_consensus_version "$SNARKOS_CURRENT_BIN")
current_consensus_version=$(echo "$current_probe_result" | awk '{print $1}')
new_consensus_height=$(echo "$current_probe_result" | awk '{print $2}')
log "Current binary: consensus_version=$current_consensus_version, activation_height=$new_consensus_height"

log "Probing stable consensus version from release binary..."
release_probe_result=$(probe_stable_consensus_version "$SNARKOS_RELEASE_BIN")
release_consensus_version=$(echo "$release_probe_result" | awk '{print $1}')
log "Release binary: consensus_version=$release_consensus_version"

# 3. Sanity check: Verify that the consensus version actually changes.
if [ "$release_consensus_version" = "$current_consensus_version" ]; then
  log "🎉 Consensus version unchanged at $release_consensus_version. Skipping test - success!"
  exit 0
fi

log "Consensus version changed from $release_consensus_version to $current_consensus_version. Proceeding with test..."

# 4. Clean up and start all nodes with the release binary.
log "Cleaning dev stores with release binary..."
for node_index in $(seq 0 $((total_validators-1)) ); do
  "$SNARKOS_RELEASE_BIN" clean "--dev=$node_index" "--network=$network_id"
done

log "Starting $total_validators validator nodes with release binary..."
for validator_index in $(seq 0 $((total_validators-1)) ); do
  log_file="$log_dir/validator-$validator_index.log"
  start_node "$SNARKOS_RELEASE_BIN" "$validator_index" "validator" "$log_file"
  sleep 1
done

# Ensure the network is up and running.
wait_for_nodes "$total_validators" 0 "$network_name" 120

# Block until the consensus version does not increase anymore.
if ! wait_for_stable_consensus_version 0 "$network_name"; then
  log "❌ Test failed! Consensus version did not stabilize within 5 minutes."
  exit 1
fi

# 5. Wait for block height to be greater than that of the new consensus version.
# Wait another 50 blocks so we are past the next consensus version activation height after upgrade.
latest_height=$(get_block_height 0 "$network_name")
wait_for_node_height 0 "$network_name" $((latest_height+50))

penultimate_index=$((total_validators-2))
last_node=$((total_validators-1))

# Record the current height of the last node before any changes.
pre_upgrade_height=$(get_block_height "$last_node" "$network_name")
log "Last node #$last_node current height: $pre_upgrade_height"

# 6. Stop the penultimate node (but do NOT restart it yet).
log "Stopping node #$penultimate_index (will restart later with new binary)..."
stop_node "$penultimate_index"

# 7. Start the last node with the new binary and verify it does NOT sync beyond
#    the new consensus version's activation height.
# The last node should not be able to sync past this height because all other
# running nodes are on the old version and cannot provide blocks for the new consensus.
log "Clearing ledger for node $last_node and starting with updated binary..."
stop_node "$last_node"
"$SNARKOS_RELEASE_BIN" clean "--dev=$last_node" "--network=$network_id"
log_file="$log_dir/validator-$last_node.log"
start_node "$SNARKOS_CURRENT_BIN" "$last_node" "validator" "$log_file"

# Wait and verify the node does NOT sync beyond the new consensus height.
log "Verifying that node #$last_node does NOT sync beyond height $new_consensus_height (all peers are outdated)..."
if ! wait_for_no_sync "$last_node" "$network_name" "$new_consensus_height" 60; then
  log "❌ Test failed! Node synced beyond new consensus height when it should not have (all peers are outdated)."
  exit 1
fi
log "✅ Confirmed: Node #$last_node correctly did not sync beyond new consensus height with outdated peers."

# 8. Start the penultimate node with the new binary and ensure the last node syncs
#    up to the new consensus version's activation height.
log "Starting node #$penultimate_index with updated binary..."
log_file="$log_dir/validator-$penultimate_index.log"
start_node "$SNARKOS_CURRENT_BIN" "$penultimate_index" "validator" "$log_file"

# Wait for the last node to sync up to the new consensus version's activation height.
# The node should be able to sync blocks up to (but not beyond) this height since
# those are the blocks created with the new consensus version.
log "Waiting for node #$last_node to sync to the new consensus height ($new_consensus_height)..."
if ! wait_for_node_height "$last_node" "$network_name" "$new_consensus_height"; then
  log "❌ Test failed! Node did not sync to the new consensus height after penultimate node was upgraded."
  exit 1
fi

log "🎉 Test passed! Node synced to new consensus height ($new_consensus_height) after another node was upgraded."

if check_logs "$log_dir" "$total_validators" 0 "$MAX_VALIDATOR_LOG_SIZE_BYTES"; then
  exit 0
else
  exit 1
fi
