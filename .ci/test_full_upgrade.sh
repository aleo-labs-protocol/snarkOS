#!/bin/bash

####################################################
# Backwards-compatibility upgrade test:
#
# 1. Download latest snarkOS release source from GitHub.
# 2. Build it via `cargo install --locked --path . --features test_network`
#    into a separate prefix (SNARKOS_RELEASE_DIR).
# 3. Probe latest_consensus_version for latest release and PR binaries by spawning temporary devnets and waiting for consensus to stabilize.
# 4. Compute CONSENSUS_VERSION_HEIGHTS for latest release and PR.
# 5. Start the devnet with latest release binaries and pass CONSENSUS_VERSION_HEIGHTS to nodes.
# 6. Restart nodes one-by-one with the PR binary.
####################################################

set -eo pipefail  # error on any command failure

# --- Parameters from CLI ---
total_validators=$1
total_clients=$2
network_id=$3

# Default values if not provided
: "${total_validators:=4}"
: "${total_clients:=2}"
: "${network_id:=0}"

# Node verbosity
NODE_VERBOSITY=4
# Max logfile sizes (bytes) for regression checks.
MAX_VALIDATOR_LOG_SIZE_BYTES=$((8 * 1024 * 1024))
MAX_CLIENT_LOG_SIZE_BYTES=$((1 * 1024 * 1024))


# How long to wait between upgrades (seconds); used for block-height window
WAIT_BETWEEN_UPGRADES="${WAIT_BETWEEN_UPGRADES:-60}"

# How long an upgraded node may take to serve its REST API again (seconds).
MAX_RESTART_WAIT="${MAX_RESTART_WAIT:-300}"

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

# probe_stable_consensus_version is now in utils.sh

# Build heights string: for L, we want exactly L entries: 0,5,10,...,5*(L-1)
function build_consensus_heights() {
  local lcv="$1"
  local step=5

  if ! is_integer "$lcv" || (( lcv <= 0 )); then
    echo ""
    return 0
  fi

  local heights=""
  local i=0
  while (( i < lcv )); do
    local h=$(( i * step ))
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
# same as build_consensus_heights, but last step is +100 instead of +5.
function build_consensus_heights_with_big_last() {
  local lcv="$1"
  local step=5

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

  # Up to the penultimate entry with step 5
  while (( i < (lcv - 1) )); do
    local h=$(( i * step ))
    if [ -z "$heights" ]; then
      heights="$h"
    else
      heights="$heights,$h"
    fi
    i=$((i + 1))
  done

  # Previous value is for i = lcv - 2
  local prev=$(( (lcv - 2) * step ))
  local last=$(( prev + 100 ))
  heights="$heights,$last"

  echo "$heights"
}

function ci_cargo_install_snarkos() {
  CARGO_PROFILE_RELEASE_LTO=off \
  CARGO_PROFILE_RELEASE_CODEGEN_UNITS=16 \
  CARGO_PROFILE_RELEASE_OPT_LEVEL=2 \
  CARGO_PROFILE_RELEASE_DEBUG=0 \
    cargo install --locked --path . --features test_network "$@"
}

# Probe release + PR binaries to get stable consensus versions, then compute heights.
function derive_consensus_env_from_version() {
  local lcv_release=""
  local lcv_current=""
  local probe_result=""

  # 1) Probe current binary first (so we know the target consensus version)
  log "Probing stable consensus version from current binary..."
  probe_result=$(probe_stable_consensus_version "$SNARKOS_CURRENT_BIN")
  lcv_current=$(echo "$probe_result" | awk '{print $1}')
  if ! is_integer "$lcv_current"; then
    log "ERROR: Could not determine consensus version from current binary"
    exit 1
  fi
  log "Current binary stable consensus version: $lcv_current"

  # 2) Probe release binary
  log "Probing stable consensus version from release binary..."
  probe_result=$(probe_stable_consensus_version "$SNARKOS_RELEASE_BIN")
  lcv_release=$(echo "$probe_result" | awk '{print $1}')
  if ! is_integer "$lcv_release"; then
    log "ERROR: Could not determine consensus version from release binary"
    exit 1
  fi
  log "Release binary stable consensus version: $lcv_release"

  log "Computed latest_consensus_version (release) = $lcv_release"
  log "Computed latest_consensus_version (current) = $lcv_current"

  # Simplification: step is always 5, and |lcv_current - lcv_release| <= 1.
  local heights_release
  local heights_current

  heights_release="$(build_consensus_heights "$lcv_release")"

  if [ "$lcv_current" = "$lcv_release" ]; then
    # Same consensus horizon → same step-5 pattern
    heights_current="$(build_consensus_heights "$lcv_current")"
  else
    # Different latest consensus version → stretch the last step by +100
    heights_current="$(build_consensus_heights_with_big_last "$lcv_current")"
  fi

  log "Release CONSENSUS_VERSION_HEIGHTS=${heights_release}"
  log "Current CONSENSUS_VERSION_HEIGHTS=${heights_current}"

  # Export separate heights for each binary type
  export CONSENSUS_VERSION_HEIGHTS_RELEASE="$heights_release"
  export CONSENSUS_VERSION_HEIGHTS_CURRENT="$heights_current"
  export EXPECTED_MAX_CONSENSUS_VERSION="$lcv_current"

  log "Derived EXPECTED_MAX_CONSENSUS_VERSION=${EXPECTED_MAX_CONSENSUS_VERSION}"
  log "Exported CONSENSUS_VERSION_HEIGHTS_RELEASE=${CONSENSUS_VERSION_HEIGHTS_RELEASE}"
  log "Exported CONSENSUS_VERSION_HEIGHTS_CURRENT=${CONSENSUS_VERSION_HEIGHTS_CURRENT}"
}
 
function wait_for_height_increase_window() {
  local previous_height="$1"
  local duration="${2:-60}"
  local interval="${3:-5}"

  local elapsed=0
  local increased=0
  local current_height="$previous_height"

  log "Waiting ${duration}s window to see height increase above $previous_height..."

  while (( elapsed < duration )); do
    # Note: take the height across all nodes rather than from a single one. The node that
    # is being upgraded is unreachable while it restarts, so polling just that one would
    # report a stalled chain even while the rest of the network advances happily.
    local height
    height="$(get_network_block_height "$((total_validators + total_clients))" "$network_name")"
    if (is_integer "$height"); then
      current_height="$height"
      log "Current height=${current_height}"
      if (( current_height > previous_height )); then
        increased=$(( current_height - previous_height ))
      fi
    else
      log "WARN: no node responded to a height request"
    fi
    sleep "$interval"
    elapsed=$((elapsed + interval))
  done

  # Wait multiple blocks in case the previous height was slightly outdated.
  if (( increased >= 5 )); then
    log "✅ Height increased from $previous_height to $current_height during upgrade window."
    return 0
  else
    echo "❌ Timeout: height did not increase above $previous_height during ${duration}s." >&2
    return 1
  fi
}

########################################
# MAIN
########################################

log "Starting upgrade_nodes_ci.sh"
log "total_validators=${total_validators}, total_clients=${total_clients}, network_id=${network_id}"
log "EXPECTED_MAX_CONSENSUS_VERSION=${EXPECTED_MAX_CONSENSUS_VERSION:-<none>} WAIT_BETWEEN_UPGRADES=${WAIT_BETWEEN_UPGRADES}"

# 1. Build release snarkos once (without heights override yet).
download_and_build_latest_snarkos 0

# 2–4. Probe versions, compute heights, rebuild release & PR with those heights.
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

  # Block until the upgraded node serves its REST API again. Without this, a node that
  # fails to come back up is not distinguishable from a chain that stopped advancing.
  if ! wait_for_nodes "$total_validators" "$total_clients" "$network_name" "$MAX_RESTART_WAIT"; then
    echo "❌ Upgrade failed: ${role} ${idx_label} (node index ${node_index}) did not become ready within ${MAX_RESTART_WAIT}s."
    echo "Last 50 lines of ${role} ${idx_label} log:"
    tail -n 50 "$log_file" || true
    exit 1
  fi

  # Take the baseline only once the upgraded node is back, so that the window measures the
  # network advancing *with* it, rather than crediting progress made while it was down.
  baseline_height=$(get_network_block_height "$((total_validators + total_clients))" "$network_name")
  if ! (is_integer "$baseline_height"); then
    baseline_height=0
  fi

  if ! wait_for_height_increase_window "$baseline_height" "$WAIT_BETWEEN_UPGRADES" 5; then
    echo "❌ Upgrade failed: chain did not advance after restarting ${role} ${idx_label} (node index ${node_index})."
    echo "Last 50 lines of ${role} ${idx_label} log:"
    tail -n 50 "$log_file" || true
    exit 1
  fi
done


# Ensure that every upgraded node applies the configured consensus version schedule, i.e.
# that its consensus version is the one that its block height implies. Note that this is
# not necessarily `EXPECTED_MAX_CONSENSUS_VERSION`: the test does not run long enough to
# reach the last configured height, so the version is checked against the height reached.
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

if check_logs "$log_dir" "$total_validators" "$total_clients" "$MAX_VALIDATOR_LOG_SIZE_BYTES" "$MAX_CLIENT_LOG_SIZE_BYTES"; then
  exit 0
else
  exit 1
fi
