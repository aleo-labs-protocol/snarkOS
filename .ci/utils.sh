#!/bin/bash

######################################
# Utility functions for devnet scripts
######################################

# Ensures we use IPv4 localhost everywhere.
localhost="127.0.0.1"

# Tracked node PIDs (declared before any function so callers using `set -u` never see PIDS unbound).
declare -a PIDS=()

# How many cores should each node use?
# (Should be half of the number of (v)CPUs)
# NOTE: when you update this, update TASKSET1/2 as well.
# shellcheck disable=SC2034
CORES_PER_NODE=8

# Tasksets to pin processes to specific CPUs.
# This is a no-op on MacOS.
if [[ "$(uname)" == "Darwin" ]]; then
  # shellcheck disable=SC2034
  TASKSET1=""
  # shellcheck disable=SC2034
  TASKSET2=""
else
  # shellcheck disable=SC2034
  TASKSET1="taskset -c 0-7"
  # shellcheck disable=SC2034
  TASKSET2="taskset -c 8-15"
fi

# Check if any tracked node process has exited.
# Returns 0 if a node stopped, 1 otherwise.
function check_node_stopped() {
  for i in "${!PIDS[@]}"; do
    local pid="${PIDS[i]}"
    if ! kill -0 "$pid" 2>/dev/null; then
      log "Node #${i} (pid=$pid) has exited unexpectedly"
      return 0
    fi
  done
  return 1
}

########################################
# Log prefixing
########################################

# Runs a command in the background with stdout/stderr prefixed by "[label] ".
# Usage: run_with_prefix <label> <command...>
# After calling, $! holds the PID of the backgrounded command.
function run_with_prefix() {
  local label="$1"
  shift
  stdbuf -oL -eL "$@" > >(awk -v prefix="[$label] " '{print prefix $0}') 2>&1 &
}

########################################
# Basic utility functions
########################################

# Checks that the given command is available in the PATH.
function require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    log "ERROR: required command '$1' not found in PATH" >&2
    exit 1
  fi
}

# Get the current time in seconds since the node started.
function now() {
  echo "$SECONDS"
}

# Get the relative time since the given start time
function elapsed_since() {
  local start=$1
  echo $((SECONDS - start))
}

# Get the current Unix epoch timestamp in seconds.
function epoch_now() {
  date +%s
}

# Convert a Unix epoch timestamp to an ISO-8601 prefix used by snarkOS log lines.
function epoch_to_iso() {
  local epoch=$1
  if date -u -d "@${epoch}" +%Y-%m-%dT%H:%M:%S >/dev/null 2>&1; then
    date -u -d "@${epoch}" +%Y-%m-%dT%H:%M:%S
  else
    date -u -r "${epoch}" +%Y-%m-%dT%H:%M:%S
  fi
}

# Return log lines at or after the given Unix epoch timestamp.
# When since_epoch is empty, returns the full log file.
function log_lines_since() {
  local log_file=$1
  local since_epoch=$2

  if [ -z "$since_epoch" ]; then
    cat "$log_file"
    return 0
  fi

  local since_prefix
  since_prefix=$(epoch_to_iso "$since_epoch")

  awk -v since="$since_prefix" '
    BEGIN { from = 0 }
    /^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}/ {
      if (substr($1, 1, 19) >= since) {
        from = 1
      } else {
        from = 0
      }
    }
    from { print }
  ' "$log_file"
}

# Determine network name based on network_id
function get_network_name() {
  local network_id=$1

  case $network_id in
    0)
      echo "mainnet"
      ;;
    1)
      echo "testnet"
      ;;
    2)
      echo "canary"
      ;;
    *)
      >&2 echo "Unknown network ID: $network_id, defaulting to mainnet"
      echo "mainnet"
      ;;
  esac
}

# Generates the given number of random indices up to max_index.
function generate_random_indices() {
  local count=$1
  local max_index=$2

  # Check if count is greater than max_index + 1 (impossible request)
  if (( count > max_index + 1 )); then
    echo "Error: Cannot request more unique indices than exist." >&2
    return 1
  fi

  # shuf -i generates a range (0 to max), -n picks N items
  shuf -i 0-"$max_index" -n "$count"
}

# Stops select running processes from the PIDS list.
function stop_some_nodes() {
  local indices=("$@")
  local killed_pids=()

  echo "🚨 Stopping ${#indices[@]} selected node(s)..."

  for i in "${indices[@]}"; do
    # Get the PID from the global PIDS array using the index
    local pid="${PIDS[$i]}"

    # Check if PID exists (is not empty) and is currently running
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
      echo "Killing PIDS[$i] -> $pid"
      # Use SIGTERM to gracefully shut down the node.
      kill "$pid" 2>/dev/null || true
      # Add to list of PIDs to wait for specifically
      killed_pids+=("$pid")
    else
      echo "Skipping PIDS[$i] (PID: $pid) - Already dead or invalid."
    fi
  done

  # Wait up to 60 seconds for all selected nodes to shut down.
  elapsed=0
  while (( elapsed < 60 )); do
    still_running=false
    for pid in "${killed_pids[@]}"; do
      if kill -0 "$pid" 2>/dev/null; then
        still_running=true
        break
      fi
    done

    if ! $still_running; then
      return 0 
    else 
      sleep 1
      elapsed=$((elapsed + 1))
    fi
  done

  log "❌ Not all nodes shut down within 60 seconds."
  return 1
}

# Succeeds if the given string is an integer.
function is_integer() {
  if [[ $1 =~ ^[0-9]+$ ]]; then
    return 0
  else
    return 1
  fi
}

# Succeeds if the given string is a float.
function is_float() {  
  if [[ "$1" =~ ^[+-]?[0-9]+(\.[0-9]+)?([eE][+-]?[0-9]+)?$ ]]; then
    return 0
  else
    return 1
  fi
}

########################################
# Helper functions for logging
########################################

# The log directory variable. The directory will only be created by invoking `init_log_dir` 
log_dir="$PWD/.logs-$(date +"%Y%m%d%H%M%S")"

# Set up a logging directory that nodes and "ci-runner: logs are storeed in
function init_log_dir() {
  mkdir -p "$log_dir"
  chmod 755 "$log_dir"
  log "Created log directory: $log_dir"
}

# Write a log message to the console and "ci-runner.log".
function log() {
  msg="$(date -u +"%Y-%m-%dT%H:%M:%SZ") $*"
  echo "$msg" >> "$log_dir/ci-runner.log"
  # Print to message to stderr so it is always visible.
  >&2 echo "[ci-runner] $msg"
}

###########################################
# Helper functions to set up and stop nodes 
###########################################

# Wait until the given PID is no longer running, or until timeout seconds elapse.
# Returns 0 if the process exited, 1 on timeout.
function wait_for_pid_exit() {
  local pid="$1"
  local timeout="$2"
  local start
  start=$(now)
  while (( $(elapsed_since "$start") < timeout )); do
    if ! kill -0 "$pid" 2>/dev/null; then
      return 0
    fi
    sleep 1
  done
  return 1
}

# Stop a process with SIGINT, then SIGTERM, then SIGKILL if needed.
function graceful_stop_pid() {
  local pid="$1"
  local label="$2"

  if [ -z "$pid" ]; then
    return 0
  fi

  if ! kill -0 "$pid" 2>/dev/null; then
    return 0
  fi

  log "Stopping ${label} (pid=${pid}) with SIGINT"
  kill -INT "$pid" 2>/dev/null || true
  if wait_for_pid_exit "$pid" 60; then
    return 0
  fi

  log "${label} did not exit after SIGINT; sending SIGTERM"
  kill -TERM "$pid" 2>/dev/null || true
  if wait_for_pid_exit "$pid" 20; then
    return 0
  fi

  log "${label} did not exit after SIGTERM; sending SIGKILL"
  kill -KILL "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true
}

# Stops all running processes in the given list (graceful shutdown per PID).
function stop_nodes() {
  log "🚨 Cleaning up ${#PIDS[@]} process(es)…"
  for pid in "${PIDS[@]}"; do
    if kill -0 "$pid" 2>/dev/null; then
      kill -9 "$pid" 2>/dev/null || true
    fi
  done

  # block until all nodes have shut down
  wait
}

# Generate the trusted peers for the validators as they will not allow connections from unknown peers.
function generate_trusted_clients() {
  local total_validators=$1
  local total_clients=$2

  local result=""

  for ((client_index = 0; client_index < total_clients; client_index++)); do
    node_index=$((client_index + total_validators))
    if (( client_index == 0 )); then
      result+="127.0.0.1:$((4130+node_index))"
    else
      result+=",127.0.0.1:$((4130+node_index))"
    fi
  done

  echo "$result"
}

########################################
# Common network checkes needed through the scripts
########################################

# Function checking that each node in the given range [start_index, end_index)
# reached a minimum block height.
function check_heights() {
  local start_index=$1
  local end_index=$2
  local min_height=$3
  local network_name=$4
  local elapsed=$5

  local all_reached=true
  local highest_height=0

  if (( end_index <= start_index )); then
    log "❌ Invalid range: $end_index <= $start_index"
    exit 1 
  fi

  for node_index in $(seq "$start_index" $((end_index-1))); do
    port=$((3030 + node_index))
    # Note: this yields an empty string when the node is unreachable, so that an
    # unreachable node is not indistinguishable from one that is at height 0.
    height=$(get_block_height_by_port "$port" "$network_name")

    # Track highest height for reporting
    if (is_integer "$height") && (( height > highest_height )); then
      highest_height=$height
    fi

    if ! (is_integer "$height"); then
      log "Node #${node_index} (port=$port) did not respond to height request"
      all_reached=false
    elif (( height < min_height )); then
      log "Node #${node_index} (port=$port) only reached height $height, expected at least $min_height"
      all_reached=false
    fi
  done
  
  if $all_reached; then
    log "✅ SUCCESS: All nodes reached minimum height of $min_height"
    return 0
  else
    if (( elapsed > 0 && ((elapsed % 60) == 0) )); then
      elapsed_mins=$((elapsed / 60))
      log "⏳ WAITING: Not all nodes reached minimum height of $min_height (highest so far: $highest_height, elapsed: $elapsed_mins minutes)"
    fi

    return 1
  fi
}

# Function checking that nodes created logs on disk and they contain no errors.
function check_logs() {
  log "Checking logs exist for all nodes..."
  local log_dir=$1
  local total_validators=$2
  local total_clients=$3
  # Optional max logfile size in bytes.
  local max_validator_log_size_bytes=${4:-}
  local max_client_log_size_bytes=${5:-}
  # Optional Unix epoch timestamp; only log lines at or after this time are checked.
  local since_epoch=${6:-}

  if [ -n "$since_epoch" ]; then
    log "Only checking log lines at or after $(epoch_to_iso "$since_epoch")"
  fi

  local all_reached=true
  local highest_height=0

  # Don't use `seq` here as `total_validators` can be 0.
  for ((validator_index = 0; validator_index < total_validators; validator_index++)); do
    validator_log="$log_dir/validator-${validator_index}.log"
    if [ ! -s "$validator_log" ]; then
      log "❌ Test failed! Validator #${validator_index} did not create any logs in \"$log_dir\"."
      return 1
    fi

    if [ -n "$max_validator_log_size_bytes" ]; then
      validator_log_size_bytes=$(wc -c < "$validator_log")
      if (( validator_log_size_bytes > max_validator_log_size_bytes )); then
        log "❌ Test failed! Validator #${validator_index} logfile is too large (${validator_log_size_bytes}B > ${max_validator_log_size_bytes}B)."
        return 1
      fi
    fi

    validator_log_content=$(log_lines_since "$validator_log" "$since_epoch")

    if echo "$validator_log_content" | grep -q "ERROR"; then
      log "❌ Test failed! Validator #${validator_index} logs contain errors."
      # Print the errors to the console.
      echo "$validator_log_content" | grep "ERROR"
      return 1
    fi
  done

  # Don't use `seq` here as `total_clients` can be 0.
  for ((client_index = 0; client_index < total_clients; client_index++)); do
    client_log="$log_dir/client-${client_index}.log"
    if [ ! -s "$client_log" ]; then
      log "❌ Test failed! Client #${client_index} did not create any logs in \"$log_dir\"."
      return 1
    fi

    if [ -n "$max_client_log_size_bytes" ]; then
      client_log_size_bytes=$(wc -c < "$client_log")
      if (( client_log_size_bytes > max_client_log_size_bytes )); then
        log "❌ Test failed! Client #${client_index} logfile is too large (${client_log_size_bytes}B > ${max_client_log_size_bytes}B)."
        return 1
      fi
    fi

    client_log_content=$(log_lines_since "$client_log" "$since_epoch")

    if echo "$client_log_content" | grep -q "ERROR"; then
      log "❌ Test failed! Client #${client_index} logs contain errors."
      # Print the errors to the console.
      echo "$client_log_content" | grep "ERROR"
      return 1
    fi
  done

  return 0
}




# Succeeds if all nodes are available.
function check_nodes() {
  local total_validators=$1
  local total_clients=$2
  local network_name=$3

  for node_index in $(seq 0 $((total_validators+total_clients-1))); do
    port=$((3030 + node_index))
    status=$(curl -s -o /dev/null -w "%{http_code}" "http://$localhost:$port/v2/$network_name/version")
    # Fail if the HTTP response is not 2XX.
    if (( status < 200 || status > 300 )); then
      log "Node #${node_index} (port=$port) is not ready yet"
      return 1
    fi
  done

  return 0
}

# Succeeds if the node with the given index has the specified number of peers (or greater)
function wait_for_peers() {
  local node_index=$1
  local min_peers=$2
  local network_name=$3

  local max_wait=300
  local poll_interval=1
  local port=$((3030+node_index))

  local start
  start=$(now)
  
  while (( $(elapsed_since "$start") < max_wait )); do
    result=$(curl -s "http://$localhost:$port/v2/$network_name/peers/count")

    if (is_integer "$result"); then
      if (( result < min_peers )); then
        log "Node #${node_index} (port=$port) has $result peers, expected at least $min_peers. Will wait and retry..."
      else 
        return 0
      fi
    else
      log "Failed to get number of peers for node #${node_index} (port=$port). Will retry..."
      return 0
    fi

    # Continue waiting
    sleep "$poll_interval"
  done

  log "❌ Nodes did not connect within 5 minutes."
  return 1
}

# Succeeds if the node with the given index has the specified number of BFT connections (or greater)
function wait_for_bft_connections() {
  local node_index=$1
  local min_peers=$2
  local network_name=$3

  local max_wait=300
  local poll_interval=1
  local port=$((3030 + node_index))

  local start
  start=$(now)
  
  while (( $(elapsed_since "$start") < max_wait )); do
    result=$(curl -s "http://$localhost:$port/v2/$network_name/connections/bft/count")

    if ! (is_integer "$result"); then
      log "Failed to get number of BFT connections for node #${node_index} (port=$port). Will retry..."
    elif (( result < min_peers )); then
      log "Node #${node_index} (port=$port) has $result BFT connections, expected at least $min_peers. Will wait and retry..."
    else
      return 0
    fi

    # Continue waiting
    sleep "$poll_interval"
  done

  log "❌ BFT connections did not reach $min_peers within 5 minutes."
  return 1
}

# Blocks until the node with the given index has at least one peer to sync from (or times out).
function wait_for_sync_peers() {
  local node_index=$1

  local max_wait=300 
  local start
  start=$(now)

  while (( $(elapsed_since "$start") < max_wait )); do
    port=$((3030+node_index))
    result=$(curl -s "http://localhost:${port}/v2/$network_name/sync/peers")
    echo "$result"
    num_peers=$(echo "$result" | jq -r '. | length')

    # Height is set to zero without block locators. So wait for until it is greater than 0 for at least one peer.
    for ((idx=0; idx<num_peers; ++idx)); do
      count=$(echo "$result" | jq -r ".[keys[$idx]]")
      if ((count > 0)); then
        return 0
      fi
    done

    # Continue waiting
    sleep 1
  done
  
  return 1
}

# Blocks until the network is ready.
function wait_for_nodes() {
  log "Waiting for nodes to become ready"
  
  local total_validators=$1
  local total_clients=$2
  local network_name=$3
  # Default to 60s if not provided. 
  local max_wait=${4:-60}

  local poll_interval=1

  local start
  start=$(now)

  while (( $(elapsed_since "$start") < max_wait )); do
    if check_node_stopped; then
      log "ERROR: one or more nodes stopped unexpectedly"
      return 1
    fi
    
    if check_nodes "$total_validators" "$total_clients" "$network_name"; then
      log  "✅ All nodes are ready!"
      return 0
    fi

    # Pause to give the nodes time to start up.
    sleep "$poll_interval"
  done

  log "❌ Nodes did not become ready within $max_wait seconds."
  return 1
}


# Print the last 20 lines of logs for all nodes.
function print_validator_logs() {
  local log_dir=$1
  local total_validators=$2
  local total_clients=$3

  echo "Last 20 lines of node logs:"
  for ((validator_index = 0; validator_index < total_validators; validator_index++)); do
    echo "=== Validator $validator_index logs ==="
    tail -n 20 "$log_dir/validator-$validator_index.log"
  done
}

function wait_for_heights() {
  local start_index=$1
  local end_index=$2
  local min_height=$3
  local network_name=$4
  local max_wait=$5
  local poll_interval=$6

  # Defaultv values
  : "${max_wait:=300}"
  : "${poll_interval:=5}"

  local start
  start=$(now) 
  
  while (( $(elapsed_since "$start") < max_wait )); do
    if check_heights "$start_index" "$end_index" "$min_height" "$network_name" "$elapsed"; then
      return 0
    fi

    # Continue waiting
    sleep 5
  done
  return 1
}

function print_client_logs() {
  local log_dir=$1
  local total_validators=$2
  local total_clients=$3

  for ((client_index = 0; client_index < total_clients; client_index++)); do
    echo "=== Client $client_index logs ==="
    node_index=$((total_validators + client_index))
    tail -n 20 "$log_dir/client-$client_index.log"
  done
}


# Function checking that the first node reached the latest (unchanging) consensus version.
function wait_for_stable_consensus_version() {
  local node_index=$1
  local network_name=$2
  
  local last_seen_consensus_version=0
  local last_seen_height=0

  # Check consensus versions periodically with a timeout
  log "ℹ️ Waiting for consensus version to stabilize..."
  local start
  start=$(now)
  
  while (( $(elapsed_since "$start") < 300 )); do  # 5 minutes max
    consensus_version=$(get_consensus_version "$node_index" "$network_name" || echo "0")
    height=$(get_block_height "$node_index" "$network_name" || echo "0")

    # If the consensus version is greater than the last seen, we update it.
    if (( consensus_version > last_seen_consensus_version )); then
      log "✅ Consensus version updated to $consensus_version"
    # If the consensus version is the same whereas the block height is different and at least 10, we can assume that the consensus version is stable
    elif (( (height != last_seen_height) && (height >= 10) )); then
        log "✅ Consensus version is stable at $consensus_version with height $height"
        return 0
    fi

    last_seen_consensus_version=$consensus_version
    last_seen_height=$height

    # Continue waiting
    sleep 10
    log "Waited $(elapsed_since "$start") seconds so far..."
  done

  return 1
}

########################################
# Consensus version probing helpers
########################################

# Arrays to track temporary probe nodes (separate from main test nodes)
declare -a PROBE_PIDS

# Stop all probe nodes
function stop_probe_nodes() {
  log "Stopping ${#PROBE_PIDS[@]} probe node(s)..."
  for pid in "${PROBE_PIDS[@]}"; do
    if kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
    fi
  done
  for pid in "${PROBE_PIDS[@]}"; do
    wait "$pid" 2>/dev/null || true
  done
  PROBE_PIDS=()
}

# Start a temporary devnet with the given binary, wait for consensus to stabilize,
# and return the stable consensus version and block height.
# Output format: "consensus_version block_height"
#
# Required variables (must be set before calling):
#   - total_validators: number of validators to start
#   - network_id: network ID (0=mainnet, 1=testnet, 2=canary)
#   - network_name: network name string
#   - log_dir: directory for log files
function probe_stable_consensus_version() {
  local bin="$1"
  local probe_log_prefix
  probe_log_prefix="$log_dir/probe-$(basename "$bin")"

  log "Probing stable consensus version using $bin..."

  # Clean up any existing data first
  for node_index in $(seq 0 $((total_validators-1))); do
    "$bin" clean "--dev=$node_index" "--network=$network_id" >/dev/null 2>&1 || true
  done

  # Start all validator nodes with the probe binary
  PROBE_PIDS=()
  for node_index in $(seq 0 $((total_validators-1))); do
    local probe_log="${probe_log_prefix}-validator-${node_index}.log"
    
    # Build trusted validators list
    local trusted_validators=""
    for ((peer_index = 0; peer_index < total_validators; peer_index++)); do
      if [ "$peer_index" -ne "$node_index" ]; then
        if [ -n "$trusted_validators" ]; then
          trusted_validators+=","
        fi
        trusted_validators+="127.0.0.1:$((5000+peer_index))"
      fi
    done

    # Hide node output from stdout
    "$bin" start --nodisplay "--network=$network_id" "--verbosity=1" \
      "--dev=$node_index" "--dev-num-validators=$total_validators" \
      --validator "--logfile=$probe_log" "--validators=$trusted_validators" >/dev/null 2>&1 &
    PROBE_PIDS[node_index]=$!
    sleep 1
  done

  # Wait for nodes to become ready
  log "Waiting for probe nodes to become ready..."
  local max_wait=120
  local start
  start=$(now)
  
  while (( $(elapsed_since "$start") < max_wait )); do
    local all_ready=true
    for node_index in $(seq 0 $((total_validators-1))); do
      local port=$((3030 + node_index))
      if ! curl -s "http://$localhost:$port/v2/$network_name/block/height/latest" >/dev/null 2>&1; then
        all_ready=false
        break
      fi
    done
    if $all_ready; then
      log "All probe nodes ready after $(elapsed_since "$start")s"
      break
    fi
    sleep 2
  done

  if (( $(elapsed_since "$start") >= max_wait )); then
    log "ERROR: Probe nodes did not become ready within ${max_wait}s"
    stop_probe_nodes
    return 1
  fi

  # Wait for consensus version to stabilize
  log "Waiting for consensus version to stabilize..."
  if ! wait_for_stable_consensus_version 0 "$network_name"; then
    log "ERROR: Consensus version did not stabilize"
    stop_probe_nodes
    return 1
  fi

  # Get the stable consensus version and current height
  local stable_consensus_version
  local stable_height
  stable_consensus_version=$(get_consensus_version 0 "$network_name")
  stable_height=$(get_block_height 0 "$network_name")

  log "Probe complete: consensus_version=$stable_consensus_version, height=$stable_height"

  # Stop and clean up probe nodes
  stop_probe_nodes

  for node_index in $(seq 0 $((total_validators-1))); do
    "$bin" clean "--dev=$node_index" "--network=$network_id" >/dev/null 2>&1 || true
  done

  echo "$stable_consensus_version $stable_height"
}

########################################
# Helper functions for benchmarks 
########################################

# Compute the throughput for a number of operation over some time.
function compute_throughput {
  local num_ops=$1
  local duration=$2
  local decimal_points=2
  
  # Use floating point division
  result=$(bc <<< "scale=$decimal_points; $num_ops/$duration")

  echo "$result"
}

######################################################################
# Helper functions to download and build latest stable snarkOS release
######################################################################

# Release binary will be installed here via `cargo install --root`.
SNARKOS_RELEASE_DIR="${SNARKOS_RELEASE_DIR:-$PWD/.ci/release-snarkos}"
SNARKOS_RELEASE_BIN="${SNARKOS_RELEASE_BIN:-$SNARKOS_RELEASE_DIR/bin/snarkos}"
SNARKOS_RELEASE_VERSION_FILE="${SNARKOS_RELEASE_VERSION_FILE:-$SNARKOS_RELEASE_DIR/VERSION}"

# Build snarkos with optimized settings for CI (faster builds).
function ci_cargo_install_snarkos() {
  CARGO_PROFILE_RELEASE_LTO=off \
  CARGO_PROFILE_RELEASE_CODEGEN_UNITS=16 \
  CARGO_PROFILE_RELEASE_OPT_LEVEL=2 \
  CARGO_PROFILE_RELEASE_DEBUG=0 \
    cargo install --locked --path . --features test_network "$@"
}

# Download and build the latest snarkOS release from GitHub.
# Pass force_build=1 to rebuild even if cached binary exists.
function download_and_build_latest_snarkos() {
  require_cmd curl
  require_cmd tar
  require_cmd cargo

  local force_build="${1:-0}"

  mkdir -p "${SNARKOS_RELEASE_DIR}"

  local repo="ProvableHQ/snarkOS"
  local latest_url latest_tag tar_url
  local tmpdir srcdir
  local existing_tag=""

  log "Resolving latest snarkOS release tag via redirect…"
  latest_url="$(
    curl -fsSL -o /dev/null -w '%{url_effective}' \
      "https://github.com/${repo}/releases/latest"
  )" || {
    echo "ERROR: Failed to resolve latest release URL from GitHub." >&2
    exit 1
  }

  latest_tag="${latest_url##*/}"

  if [ -z "${latest_tag}" ] || [ "${latest_tag}" = "latest" ]; then
    echo "ERROR: Failed to determine latest tag from URL: ${latest_url}" >&2
    exit 1
  fi

  log "Latest stable tag resolved to: ${latest_tag}"

  ########################################
  # Cached binary exists & matches version
  #         AND force_build = 0 → return early
  ########################################
  if [ "$force_build" != "1" ] &&
     [ -x "${SNARKOS_RELEASE_BIN}" ] &&
     [ -f "${SNARKOS_RELEASE_VERSION_FILE}" ]; then

    existing_tag="$(cat "${SNARKOS_RELEASE_VERSION_FILE}" || true)"

    if [ "${existing_tag}" = "${latest_tag}" ]; then
      log "Reusing cached release snarkos for tag ${existing_tag} (no rebuild)."
      return 0
    fi
  fi

  ########################################
  # Cached source exists AND force_build=1
  # → Do NOT download again, just rebuild
  ########################################
  local cached_src_dir=".ci/release-snarkos-src"
  if [ "$force_build" = "1" ] && [ -d "$cached_src_dir" ]; then
    log "Force-rebuilding release snarkos using cached source in $cached_src_dir"
    srcdir="$cached_src_dir"

    (
      cd "$srcdir"
      ci_cargo_install_snarkos --root "${SNARKOS_RELEASE_DIR}"
    ) || {
      echo "ERROR: forced rebuild failed" >&2
      exit 1
    }

    echo "${latest_tag}" > "${SNARKOS_RELEASE_VERSION_FILE}"
    log "Rebuild complete."
    return 0
  fi

  ########################################
  # Need to download the source (first run or version mismatch)
  ########################################

  tar_url="https://github.com/${repo}/archive/refs/tags/${latest_tag}.tar.gz"
  log "Downloading release source tarball: ${tar_url}"

  tmpdir="$(mktemp -d)"
  curl -fL "${tar_url}" -o "${tmpdir}/snarkos-src.tar.gz" || {
    echo "ERROR: Failed to download tarball from ${tar_url}" >&2
    rm -rf "${tmpdir}"
    exit 1
  }

  log "Extracting source tarball…"
  rm -rf "$cached_src_dir"
  mkdir -p "$cached_src_dir"
  tar -xzf "${tmpdir}/snarkos-src.tar.gz" -o -C "$cached_src_dir" --strip-components=1

  srcdir="$cached_src_dir"

  log "Building release snarkos from fresh source at: ${srcdir}"

  (
    cd "${srcdir}"
    ci_cargo_install_snarkos --root "${SNARKOS_RELEASE_DIR}"
  ) || {
    echo "ERROR: cargo install failed for release snarkos" >&2
    rm -rf "${tmpdir}"
    exit 1
  }

  echo "${latest_tag}" > "${SNARKOS_RELEASE_VERSION_FILE}"
  rm -rf "${tmpdir}"

  log "snarkos release (${latest_tag}) built and installed at ${SNARKOS_RELEASE_BIN}"
}

########################################
# Helper functions for REST queries to individual nodes
########################################

# Get the consensus version of the specified node
function get_consensus_version {
  local node_index=$1
  local network_name=$2

  port=$((3030+node_index))
  result=$(curl -s "http://$localhost:$port/v2/$network_name/consensus_version")

  if ! is_integer "$result"; then
    log "❌ Failed to retrieve consensus version for node #${node_index}"
    return 1
  else
    echo "$result"
    return 0
  fi
}

# Latest block height from REST (port + network). Prints height or empty on failure; no logging.
# Optional third argument: curl --max-time seconds (omit for no per-request timeout).
function get_block_height_by_port() {
  local port="$1"
  local network_name="$2"
  local max_time="${3-}"
  local result
  if [[ -n "$max_time" ]]; then
    result=$(curl -s --max-time "$max_time" "http://$localhost:$port/v2/$network_name/block/height/latest" || true)
  else
    result=$(curl -s "http://$localhost:$port/v2/$network_name/block/height/latest" || true)
  fi
  if is_integer "$result"; then
    echo "$result"
  else
    echo ""
  fi
}

# Get the block height of the specified node
function get_block_height {
  local node_index=$1
  local network_name=$2
  local port
  local result

  port=$((3030+node_index))
  result=$(get_block_height_by_port "$port" "$network_name")

  if [[ -z "$result" ]]; then
    log "❌ Failed to retrieve block height for node #${node_index}"
    return 1
  fi
  echo "$result"
  return 0
}

# Get the greatest block height reported by any of the given nodes, i.e. the height that
# the network has reached. Prints an empty string if no node responded.
#
# Unlike `get_block_height`, this tolerates individual nodes being unreachable, which makes
# it suitable for checking that the network is advancing while a node is being restarted.
function get_network_block_height() {
  local total_nodes=$1
  local network_name=$2

  local greatest=""
  local height

  for ((node_index = 0; node_index < total_nodes; node_index++)); do
    height=$(get_block_height_by_port $((3030 + node_index)) "$network_name" 5)
    if (is_integer "$height") && { [[ -z "$greatest" ]] || (( height > greatest )); }; then
      greatest=$height
    fi
  done

  echo "$greatest"
}

# Get the consensus version that the given comma-separated list of consensus version
# heights implies for the given block height, i.e. how many of them have been reached.
function consensus_version_at_height() {
  local heights=$1
  local height=$2

  local count=0
  local entries
  IFS=',' read -ra entries <<< "$heights"

  for entry in "${entries[@]}"; do
    if (is_integer "$entry") && (( entry <= height )); then
      count=$((count + 1))
    fi
  done

  echo "$count"
}
