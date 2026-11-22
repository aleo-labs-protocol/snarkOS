#!/bin/bash

####################################################
# Backwards-compatibility upgrade test:
#
# 1. Download latest snarkOS release source from GitHub.
# 2. Build it with its devnet feature via `ci_cargo_install_snarkos`
#    into a separate prefix (SNARKOS_RELEASE_DIR).
# 3. Query latest_consensus_version for latest release and PR binaries from the REST API of a single node of each.
# 4. Compute CONSENSUS_VERSION_HEIGHTS for latest release and PR.
# 5. Start the devnet with latest release binaries and pass CONSENSUS_VERSION_HEIGHTS to nodes.
# 6. Restart nodes one-by-one with the PR binary.
#    After upgrading the first validator, restart the last validator on the release binary and check
#    that it dials the upgraded validator. After upgrading the first client, do the same for the
#    last client.
# 7. If the PR binary has a newer consensus version, wait until the network passes its activation
#    height, which lies after every upgrade, and check that every node runs it.
####################################################

set -eo pipefail  # error on any command failure

# --- Parameters from CLI ---
total_validators=$1
total_clients=$2
network_id=$3

# Default values if not provided
: "${total_validators:=4}"
: "${total_clients:=2}"
# Testnet, where the latest release and this build share a dev genesis.
: "${network_id:=1}"

# Node verbosity
NODE_VERBOSITY=4
# Max logfile sizes (bytes) for regression checks.
MAX_VALIDATOR_LOG_SIZE_BYTES=$((8 * 1024 * 1024))
MAX_CLIENT_LOG_SIZE_BYTES=$((1 * 1024 * 1024))


# How long a restarted node may take to rejoin the network (seconds).
WAIT_BETWEEN_UPGRADES="${WAIT_BETWEEN_UPGRADES:-60}"

# How long an upgraded node may take to serve its REST API again (seconds).
MAX_RESTART_WAIT="${MAX_RESTART_WAIT:-300}"

# A validator stops REQUIRED_UPGRADE_SAFETY_MARGIN blocks before a newer consensus version that
# validators holding the availability threshold of stake schedule and it does not. With four
# validators, that is two of them.
REQUIRED_UPGRADE_SAFETY_MARGIN=100

# A devnet makes about one block every 2 s.
SECONDS_PER_BLOCK=2

# Load shared helpers (is_integer, get_network_name, wait_for_nodes, stop_nodes, ...)
# shellcheck source=SCRIPTDIR/utils.sh
. ./.ci/utils.sh

# Set up logging directory
init_log_dir

# Reuse the same target dir for all builds (release + PR) to get incremental builds.
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$PWD/.ci/target}"
log "Using CARGO_TARGET_DIR=${CARGO_TARGET_DIR}"

SNARKOS_CURRENT_BIN="${SNARKOS_CURRENT_BIN:-snarkos}"

network_name=$(get_network_name "$network_id")
log "Using network: $network_name (ID: $network_id)"

# Handler that stops all nodes on shutdown.
# shellcheck disable=SC2329
exit_handler() {
  stop_nodes || true
}

# Install signal handlers.
trap exit_handler EXIT
trap 'log "⛔️ Error in $BASH_SOURCE at line $LINENO: \"$BASH_COMMAND\" failed (exit $?)"' ERR

common_flags=(
  --nodisplay "--network=$network_id" "--verbosity=$NODE_VERBOSITY"
  "--dev-num-validators=$total_validators"
)

# The set of all clients passed to each validators, so that clients can connect to validators.
# NOTE: In newer versions of snarkOS, the set of trusted clients is populated automatically through `--dev-num-clients`
# and this code can be removed eventually.
trusted_peers=$(generate_trusted_clients "$total_validators" "$total_clients")

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
    for peer_index in $(seq 0 $((total_validators-1))); do
      if (( peer_index == node_index )); then
        continue
      else
        # append "," if this is not the first trusted validator 
        if [ -n "$trusted_validators" ]; then
          trusted_validators+=","
        fi
        trusted_validators+="127.0.0.1:$((5000+peer_index))"
      fi
    done

    # Validators trust the clients as peers
    flags+=( --validator "--logfile=$log_file" "--peers=$trusted_peers" "--validators=$trusted_validators" )
    if (( node_index == 0 )); then
      flags+=( --metrics --no-dev-txs )
    fi

    # TODO Remove once old nodes are no longer on v4.4.0!
    if [ "$bin" = "$SNARKOS_CURRENT_BIN" ]; then
      flags+=( --auto-migrate-node-data )
    fi
  else
    flags+=( --client "--logfile=$log_file" )
  fi

  # Set the appropriate CONSENSUS_VERSION_HEIGHTS based on binary type
  local heights_env
  if [ "$bin" = "$SNARKOS_RELEASE_BIN" ]; then
    heights_env="$CONSENSUS_VERSION_HEIGHTS_RELEASE"
  else
    heights_env="$CONSENSUS_VERSION_HEIGHTS_CURRENT"
  fi

  run_with_prefix "$role-$node_index" env CONSENSUS_VERSION_HEIGHTS="$heights_env" "$bin" start "${flags[@]}"
  PIDS[node_index]=$!
  log "Started $role $node_index with PID ${PIDS[node_index]} using $(basename "$bin") with heights=$heights_env"
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

# Build heights string: for L, we want exactly L entries: 0,5,6,...,L+3, the shape of snarkVM's
# TEST_CONSENSUS_VERSION_HEIGHTS.
function build_consensus_heights() {
  local lcv="$1"

  if ! is_integer "$lcv" || (( lcv <= 0 )); then
    echo ""
    return 0
  fi

  local heights=""
  local i=0
  while (( i < lcv )); do
    local h=$(( i == 0 ? 0 : i + 4 ))
    if [ -z "$heights" ]; then
      heights="$h"
    else
      heights="$heights,$h"
    fi
    i=$((i + 1))
  done

  echo "$heights"
}

# Build heights string for PR when its latest_consensus_version differs from release:
# same as build_consensus_heights, but the last version activates gap blocks after the previous one.
function build_consensus_heights_with_big_last() {
  local lcv="$1"
  local gap="$2"

  if ! is_integer "$lcv" || (( lcv <= 0)) ; then
    echo ""
    return 0
  fi

  # Degenerate case: only one consensus version → just "0"
  if (( lcv == 1 )); then
    echo "0"
    return 0
  fi

  local heights=""
  local i=0

  # Up to the penultimate entry
  while (( i < (lcv - 1) )); do
    local h=$(( i == 0 ? 0 : i + 4 ))
    if [ -z "$heights" ]; then
      heights="$h"
    else
      heights="$heights,$h"
    fi
    i=$((i + 1))
  done

  # Previous value is for i = lcv - 2
  local prev=$(( lcv == 2 ? 0 : lcv + 2 ))
  local last=$(( prev + gap ))
  heights="$heights,$last"

  echo "$heights"
}

# Query the latest consensus versions of the release + PR binaries, then compute heights.
function derive_consensus_env_from_version() {
  local lcv_release=""
  local lcv_current=""
  local schedule=""
  local new_version_height=""

  if ! schedule=$(query_default_consensus_version "$SNARKOS_CURRENT_BIN" current); then
    log "ERROR: Could not determine consensus version from current binary"
    exit 1
  fi
  read -r lcv_current _ <<< "$schedule"

  if ! schedule=$(query_default_consensus_version "$SNARKOS_RELEASE_BIN" release); then
    log "ERROR: Could not determine consensus version from release binary"
    exit 1
  fi
  read -r lcv_release _ <<< "$schedule"

  log "Computed latest_consensus_version (release) = $lcv_release"
  log "Computed latest_consensus_version (current) = $lcv_current"

  # Simplification: |lcv_current - lcv_release| <= 1.
  local heights_release
  local heights_current

  heights_release="$(build_consensus_heights "$lcv_release")"

  if [ "$lcv_current" = "$lcv_release" ]; then
    # Same consensus horizon → same pattern
    heights_current="$(build_consensus_heights "$lcv_current")"
  else
    # The new version activates after every upgrade, and the release validators must not reach
    # its stop height before they are upgraded. The gap counts one rejoin window of
    # WAIT_BETWEEN_UPGRADES seconds per restart, one more for the wait of up to 60 s for a
    # heartbeat of validator 0 before the release validator restarts, and 20 blocks for the stops
    # and restarts themselves.
    local windows=$(( total_validators + total_clients + 3 ))
    local gap=$(( REQUIRED_UPGRADE_SAFETY_MARGIN + windows * WAIT_BETWEEN_UPGRADES / SECONDS_PER_BLOCK + 20 ))
    heights_current="$(build_consensus_heights_with_big_last "$lcv_current" "$gap")"
    new_version_height="${heights_current##*,}"
  fi

  log "Release CONSENSUS_VERSION_HEIGHTS=${heights_release}"
  log "Current CONSENSUS_VERSION_HEIGHTS=${heights_current}"

  # Export separate heights for each binary type
  export CONSENSUS_VERSION_HEIGHTS_RELEASE="$heights_release"
  export CONSENSUS_VERSION_HEIGHTS_CURRENT="$heights_current"
  export EXPECTED_MAX_CONSENSUS_VERSION="$lcv_current"
  # Empty when both binaries have the same latest consensus version.
  export NEW_VERSION_HEIGHT="$new_version_height"

  log "Derived EXPECTED_MAX_CONSENSUS_VERSION=${EXPECTED_MAX_CONSENSUS_VERSION}"
  log "Exported CONSENSUS_VERSION_HEIGHTS_RELEASE=${CONSENSUS_VERSION_HEIGHTS_RELEASE}"
  log "Exported CONSENSUS_VERSION_HEIGHTS_CURRENT=${CONSENSUS_VERSION_HEIGHTS_CURRENT}"
}
 
# Blocks for up to duration seconds until the node with the given index reaches 5 blocks above
# previous_height and, if it is a validator, has a gateway connection to every other validator.
function wait_for_node_to_rejoin() {
  local node_index="$1"
  local previous_height="$2"
  local duration="${3:-60}"

  # The previous height may be slightly outdated, so require several blocks.
  local target_height=$(( previous_height + 5 ))
  local port=$(( 3030 + node_index ))
  local height=""
  local start next_report=10
  start=$(now)

  log "Waiting up to ${duration}s for node index ${node_index} to reach height ${target_height}..."
  while (( $(elapsed_since "$start") < duration )); do
    height=$(get_block_height_by_port "$port" "$network_name" 5)
    if (is_integer "$height") && (( height >= target_height )); then
      break
    fi
    if (( $(elapsed_since "$start") >= next_report )); then
      log "Node index ${node_index} is at height ${height:-<none>}"
      next_report=$((next_report + 10))
    fi
    sleep 1
  done

  if ! (is_integer "$height") || (( height < target_height )); then
    echo "❌ Timeout: node index ${node_index} is at height ${height:-<none>}, below ${target_height}, after ${duration}s." >&2
    return 1
  fi

  if (( node_index < total_validators )); then
    local remaining=$(( duration - $(elapsed_since "$start") ))
    if (( remaining < 1 )); then
      remaining=1
    fi
    if ! wait_for_bft_connections "$node_index" "$((total_validators - 1))" "$network_name" "$remaining"; then
      return 1
    fi
  fi

  log "✅ Node index ${node_index} rejoined at height ${height} after $(elapsed_since "$start")s."
  return 0
}

# Prints the given message and the tail of the given node log, then exits the test.
function fail_with_log() {
  local log_file="$1"
  local message="$2"

  echo "❌ $message"
  echo "Last 50 lines of $(basename "$log_file"):"
  tail -n 50 "$log_file" || true
  exit 1
}

# Blocks until a node that was just (re)started serves its REST API again. Exits the test otherwise.
# Without this, a node that fails to come back up is not distinguishable from a chain that stopped
# advancing.
function wait_for_restarted_node() {
  local description="$1"
  local log_file="$2"

  if ! wait_for_nodes "$total_validators" "$total_clients" "$network_name" "$MAX_RESTART_WAIT"; then
    fail_with_log "$log_file" "Restart failed: ${description} did not become ready within ${MAX_RESTART_WAIT}s."
  fi
}

# Blocks until the restarted node with the given index follows the advancing chain, for at most
# WAIT_BETWEEN_UPGRADES seconds. Exits the test otherwise. Call it only once the node serves its
# REST API again, so that the baseline is the height the network reached while it was down.
function wait_for_chain_to_advance() {
  local node_index="$1"
  local description="$2"
  local log_file="$3"

  local baseline_height
  baseline_height=$(get_network_block_height "$((total_validators + total_clients))" "$network_name")
  if ! (is_integer "$baseline_height"); then
    baseline_height=0
  fi

  if ! wait_for_node_to_rejoin "$node_index" "$baseline_height" "$WAIT_BETWEEN_UPGRADES"; then
    fail_with_log "$log_file" "Restart failed: ${description} did not rejoin the advancing chain."
  fi
}

# Blocks for up to max_wait seconds until the given log has a line at or after since_epoch that
# matches the given extended regex.
function wait_for_log_line_since() {
  local log_file="$1"
  local since_epoch="$2"
  local pattern="$3"
  local max_wait="$4"

  local start
  start=$(now)
  while (( $(elapsed_since "$start") < max_wait )); do
    local lines
    lines=$(log_lines_since "$log_file" "$since_epoch")
    if grep -qE "$pattern" <<< "$lines"; then
      return 0
    fi
    sleep 1
  done
  return 1
}

# Succeeds if the node with the given index is connected to peer_addr over its gateway ("bft") or
# router ("p2p"), and that peer opened the connection: since since_epoch, the node's log has an
# inbound connection request and no outbound handshake with peer_addr.
#
# Both lines are logged when a handshake starts. A failed inbound handshake does not pass: the node
# then connects only by dialing the peer itself, which logs the outbound handshake.
function check_connection_opened_by_peer() {
  local node_index="$1"
  local kind="$2"
  local peer_addr="$3"
  local log_file="$4"
  local since_epoch="$5"

  local target
  if [ "$kind" = "bft" ]; then
    target="snarkos_node_bft::gateway: \[MemoryPool\]"
  else
    target="snarkos_node_router::handshake:"
  fi

  local connected
  connected=$(curl -s "http://$localhost:$((3030 + node_index))/v2/$network_name/connections/$kind/all")
  if ! grep -qF "\"$peer_addr\"" <<< "$connected"; then
    return 1
  fi

  local lines
  lines=$(log_lines_since "$log_file" "$since_epoch")
  grep -qE "$target Received a connection request" <<< "$lines" &&
    ! grep -qE "$target Shaking hands with '?${peer_addr}'?\.\.\." <<< "$lines"
}

# Restarts a node that still runs the release binary on the release binary, with its flags and
# ledger unchanged. Its handshakes with the upgraded nodes then have the old node as the initiator.
# Checks that it reconnects, that it opened the connection to an upgraded node, and that the chain
# advances.
function restart_on_release_binary() {
  local node_index="$1"
  local role="$2"
  local log_file="$3"
  local description="release ${role} (node index ${node_index})"

  log "=============================="
  log "Restarting ${description} so that it dials upgraded nodes"
  log "=============================="

  local since_epoch
  since_epoch=$(epoch_now)
  stop_node "$node_index"

  if [ "$role" = "validator" ]; then
    # Validator 0 redials a missing validator on each gateway heartbeat, 15s apart; the restarted
    # validator dials its peers 1s after it starts listening. Starting it right after a heartbeat
    # of validator 0 lets it dial first.
    #
    # HAZARD: if the restarted validator takes more than about 13s to start listening, validator 0
    # may dial it first, and the direction check below fails.
    if ! wait_for_log_line_since "$log_dir/validator-0.log" "$since_epoch" \
        "snarkos_node_bft::gateway: Reconnecting to" 60; then
      fail_with_log "$log_dir/validator-0.log" "Validator 0 did not try to reconnect to the stopped ${description}."
    fi
  fi

  since_epoch=$(epoch_now)
  start_node "$SNARKOS_RELEASE_BIN" "$node_index" "$role" "$log_file"
  wait_for_restarted_node "$description" "$log_file"

  # Check the direction before the height window: from then on, peers may disconnect and redial for
  # unrelated reasons.
  if [ "$role" = "validator" ]; then
    if ! wait_for_bft_connections "$node_index" "$((total_validators - 1))" "$network_name" ||
       ! wait_for_bft_connections 0 "$((total_validators - 1))" "$network_name"; then
      fail_with_log "$log_file" "The ${description} did not reconnect to all validators."
    fi
    # Validator 0 is the only upgraded validator at this point.
    if ! check_connection_opened_by_peer 0 bft "127.0.0.1:$((5000 + node_index))" \
        "$log_dir/validator-0.log" "$since_epoch"; then
      fail_with_log "$log_dir/validator-0.log" "Validator 0 did not accept a gateway connection from the ${description}."
    fi
    log "✅ The ${description} opened a gateway connection to upgraded validator 0."
  else
    # A dev client dials two validators.
    if ! wait_for_peers "$node_index" 2 "$network_name"; then
      fail_with_log "$log_file" "The ${description} did not reconnect to its peers."
    fi
    # Every validator is upgraded at this point. A validator lists the client shortly after the
    # client lists the validator.
    local responder=""
    local validator_index
    for _ in $(seq 1 10); do
      for validator_index in $(seq 0 $((total_validators - 1))); do
        if check_connection_opened_by_peer "$validator_index" p2p "127.0.0.1:$((4130 + node_index))" \
            "$log_dir/validator-$validator_index.log" "$since_epoch"; then
          responder="$validator_index"
          break 2
        fi
      done
      sleep 1
    done
    if [ -z "$responder" ]; then
      fail_with_log "$log_file" "No upgraded validator accepted a router connection from the ${description}."
    fi
    log "✅ The ${description} opened a router connection to upgraded validator ${responder}."
  fi

  wait_for_chain_to_advance "$node_index" "$description" "$log_file"
}

########################################
# MAIN
########################################

log "Starting upgrade_nodes_ci.sh"
log "total_validators=${total_validators}, total_clients=${total_clients}, network_id=${network_id}"
log "EXPECTED_MAX_CONSENSUS_VERSION=${EXPECTED_MAX_CONSENSUS_VERSION:-<none>} WAIT_BETWEEN_UPGRADES=${WAIT_BETWEEN_UPGRADES}"

# 1. Build release snarkos once (without heights override yet).
download_and_build_latest_snarkos 0

# 2–4. Query versions, compute heights.
derive_consensus_env_from_version

# From here on, we use:
#   - SNARKOS_RELEASE_BIN  => rebuilt with release heights
#   - snarkos (PR)         => rebuilt with PR heights

log "Cleaning dev stores with release binary..."
for node_index in $(seq 0 $((total_validators+total_clients-1))); do
  "$SNARKOS_RELEASE_BIN" clean "--dev=$node_index" "--network=$network_id"
done

log "Starting $total_validators validator nodes with release binary..."
for validator_index in $(seq 0 $((total_validators-1))); do
  log_file="$log_dir/validator-$validator_index.log"
  start_node "$SNARKOS_RELEASE_BIN" "$validator_index" "validator" "$log_file"
  sleep 1
done

log "Starting $total_clients client nodes with release binary..."
for client_index in $(seq 0 $((total_clients-1))); do
  node_index=$((client_index + total_validators))
  log_file="$log_dir/client-$client_index.log"
  start_node "$SNARKOS_RELEASE_BIN" "$node_index" "client" "$log_file"
  if (( client_index < total_clients-1 )); then
    sleep 1
  fi
done

wait_for_nodes "$total_validators" "$total_clients" "$network_name" 120
wait_for_stable_consensus_version 0 "$network_name"

for node_index in $(seq 0 $((total_validators+total_clients-1))); do
  if (( node_index < total_validators )); then
    role="validator"
    idx_label="$node_index"
    log_file="$log_dir/validator-$node_index.log"
  else
    role="client"
    idx_label=$((node_index - total_validators))
    log_file="$log_dir/client-$idx_label.log"
  fi

  log "=============================="
  log "Upgrading ${role} ${idx_label} (node index ${node_index})"
  log "=============================="

  stop_node "$node_index"
  start_node "$SNARKOS_CURRENT_BIN" "$node_index" "$role" "$log_file"

  description="upgraded ${role} ${idx_label} (node index ${node_index})"
  wait_for_restarted_node "$description" "$log_file"
  wait_for_chain_to_advance "$node_index" "$description" "$log_file"

  # The last validator and the last client still run the release binary.
  if (( node_index == 0 )); then
    last_validator=$((total_validators - 1))
    restart_on_release_binary "$last_validator" validator "$log_dir/validator-$last_validator.log"
  elif (( node_index == total_validators && total_clients >= 2 )); then
    restart_on_release_binary "$((total_validators + total_clients - 1))" client "$log_dir/client-$((total_clients - 1)).log"
  fi
done


# The new consensus version activates after every upgrade. Wait until every node is 5 blocks past
# its activation height, and check that every node runs it.
if [ -n "$NEW_VERSION_HEIGHT" ]; then
  total_nodes=$((total_validators + total_clients))
  target_height=$((NEW_VERSION_HEIGHT + 5))
  current_height=$(get_network_block_height "$total_nodes" "$network_name")
  if ! (is_integer "$current_height"); then
    current_height=0
  fi
  max_wait=$(( (target_height > current_height ? target_height - current_height : 0) * 2 * SECONDS_PER_BLOCK + 120 ))
  log "Waiting up to ${max_wait}s for every node to reach height ${target_height}, past the activation of consensus version ${EXPECTED_MAX_CONSENSUS_VERSION} at height ${NEW_VERSION_HEIGHT}..."
  if ! wait_for_heights 0 "$total_nodes" "$target_height" "$network_name" "$max_wait" 5; then
    log "❌ Test failed! Not every node reached height ${target_height} within ${max_wait}s."
    exit 1
  fi
  for node_index in $(seq 0 $((total_nodes - 1))); do
    node_version=$(get_consensus_version "$node_index" "$network_name" || echo "")
    if [ "$node_version" != "$EXPECTED_MAX_CONSENSUS_VERSION" ]; then
      log "❌ Test failed! Node #${node_index} is at consensus version ${node_version:-<none>}, expected ${EXPECTED_MAX_CONSENSUS_VERSION}."
      exit 1
    fi
  done
  log "✅ Every node runs consensus version ${EXPECTED_MAX_CONSENSUS_VERSION} past its activation height ${NEW_VERSION_HEIGHT}."
fi

# Ensure that every upgraded node applies the configured consensus version schedule, i.e.
# that its consensus version is the one that its block height implies.
log "Checking that the consensus version matches the configured schedule..."
for node_index in $(seq 0 $((total_validators+total_clients-1))); do
  # Retry a few times, as the height and the consensus version are read separately and so
  # may straddle a block that crosses a consensus version boundary.
  for attempt in 1 2 3; do
    node_height=$(get_block_height "$node_index" "$network_name" || echo "")
    node_version=$(get_consensus_version "$node_index" "$network_name" || echo "")
    if ! (is_integer "$node_height") || ! (is_integer "$node_version"); then
      log "❌ Test failed! Could not read the height and consensus version of node #${node_index}."
      exit 1
    fi

    expected_version=$(consensus_version_at_height "$CONSENSUS_VERSION_HEIGHTS_CURRENT" "$node_height")
    if (( node_version == expected_version )); then
      log "✅ Node #${node_index} is at consensus version ${node_version} at height ${node_height}, as expected."
      break
    fi

    if (( attempt == 3 )); then
      log "❌ Test failed! Node #${node_index} is at consensus version ${node_version} at height ${node_height}, expected ${expected_version} for CONSENSUS_VERSION_HEIGHTS=${CONSENSUS_VERSION_HEIGHTS_CURRENT}."
      exit 1
    fi
    sleep 2
  done
done

log "Upgrade test passed: all nodes upgraded to PR snarkos, the network kept advancing, and every node's consensus version matches the configured schedule."

# A release validator logs this ERROR once two upgraded validators schedule the new version.
expected_errors=""
if [ -n "$NEW_VERSION_HEIGHT" ]; then
  expected_errors="This node stops before building block [0-9]+, [0-9]+ blocks before that height; upgrade snarkOS before then"
fi

if check_logs "$log_dir" "$total_validators" "$total_clients" "$MAX_VALIDATOR_LOG_SIZE_BYTES" "$MAX_CLIENT_LOG_SIZE_BYTES" "" "$expected_errors"; then
  exit 0
else
  exit 1
fi
