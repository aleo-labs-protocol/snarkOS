#!/bin/bash

####################################################
# Runs the simple_node example in each of its modes
####################################################

set -eo pipefail # error on any command failure

# The example's public balances only add up to the starting supply if the node count divides it
# evenly. For example, 7 or 9 nodes panic at genesis.
num_nodes=4

# Set parameters directly
min_round=$1
timeout_secs=$2

# Default values if not provided
# The target must stay below round 102. The example's ledger stays at genesis, so at that round the
# committee lookback asks for a committee the ledger does not have, and the nodes stop proposing.
: "${min_round:=40}"
: "${timeout_secs:=300}"

# shellcheck source=SCRIPTDIR/utils.sh
. ./.ci/utils.sh

init_log_dir

run_dir=$(mktemp -d)
PIDS=()

# Print the end of each node's log if the script fails, and clean up in any case.
# shellcheck disable=SC2329
function exit_handler() {
  local status=$?
  stop_nodes
  if [ "$status" -ne 0 ]; then
    for node_log in "$log_dir"/*-node-*.log; do
      [ -f "$node_log" ] || continue
      echo "--- $node_log"
      tail -n 20 "$node_log"
    done
  fi
  rm -rf "$run_dir"
}
trap exit_handler EXIT

# Exits if a process already listens on a node's port, as its responses would count as the node's.
function check_ports_free() {
  local port
  for ((i = 0; i < num_nodes; i++)); do
    for port in $((3000 + i)) $((5000 + i)); do
      if (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null; then
        log "⛔️ Another process listens on port $port"
        exit 1
      fi
    done
  done
}

# Starts the nodes for the given mode.
function start_nodes() {
  local mode="$1"
  check_ports_free
  PIDS=()
  for ((i = 0; i < num_nodes; i++)); do
    # A node keeps its data in its working directory.
    local node_dir="$run_dir/$mode-$i"
    mkdir -p "$node_dir"
    (
      cd "$node_dir"
      exec "$simple_node" --mode "$mode" --id "$i" --num-nodes "$num_nodes" --fire-transmissions
    ) > "$log_dir/$mode-node-$i.log" 2>&1 &
    PIDS+=($!)
  done
}

# Waits until every node reaches the target round. Exits if a node stops or the timeout elapses.
function wait_for_round() {
  local mode="$1"
  local start
  start=$(now)
  while true; do
    for pid in "${PIDS[@]}"; do
      if ! kill -0 "$pid" 2>/dev/null; then
        log "⛔️ A $mode node (pid=$pid) exited early"
        exit 1
      fi
    done

    local lowest_round=$min_round
    for ((i = 0; i < num_nodes; i++)); do
      local round
      round=$(curl -sf -m 2 "127.0.0.1:$((3000 + i))/round/current") || round=0
      # A longer or zero-padded number does not compare correctly below.
      [[ "$round" =~ ^(0|[1-9][0-9]{0,8})$ ]] || round=0
      if ((round < lowest_round)); then
        lowest_round=$round
      fi
    done

    if ((lowest_round >= min_round)); then
      log "✅ All $mode nodes reached round $min_round after $(elapsed_since "$start")s"
      return 0
    fi

    if (($(elapsed_since "$start") >= timeout_secs)); then
      log "⛔️ The lowest $mode node is at round $lowest_round after ${timeout_secs}s, below round $min_round"
      exit 1
    fi

    sleep 5
  done
}

log "Building the simple_node example"
CARGO_PROFILE_RELEASE_LTO=off \
CARGO_PROFILE_RELEASE_CODEGEN_UNITS=16 \
CARGO_PROFILE_RELEASE_OPT_LEVEL=2 \
CARGO_PROFILE_RELEASE_DEBUG=0 \
  cargo build --release --locked -p snarkos-node-bft --example simple_node
simple_node="$(realpath "${CARGO_TARGET_DIR:-target}")/release/examples/simple_node"

for mode in bft narwhal; do
  log "Starting $num_nodes $mode nodes"
  start_nodes "$mode"
  wait_for_round "$mode"

  # Nodes with different genesis blocks can still reach consensus, and report the fork in their logs.
  if grep -l "Detected fork" "$log_dir/$mode"-node-*.log; then
    log "⛔️ The $mode nodes do not share a genesis block"
    exit 1
  fi

  # Narwhal mode has no BFT layer, so its nodes do not commit.
  if [ "$mode" = bft ]; then
    for ((i = 0; i < num_nodes; i++)); do
      if ! grep -q "Committing a subDAG" "$log_dir/$mode-node-$i.log"; then
        log "⛔️ BFT node $i reached round $min_round without a commit"
        exit 1
      fi
    done
  fi

  stop_nodes
  PIDS=()
done

log "✅ simple_node ran in every mode"
