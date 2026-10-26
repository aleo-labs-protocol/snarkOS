#!/bin/bash

####################################################
# Runs a network up to a certain height and stores
# the first node's ledger in a zipfile.
####################################################

set -eo pipefail # error on any command failure

# Uncomment this to print commands before executing them for easier debugging.
#set -x

# Change this to increase/decrease logging
log_filter="info,snarkos_node_rest=warn,snarkos_node_bft::primary=error,snarkos_node_router=error,snarkos_node_tcp=off"

# Set parameters directly
total_validators=$1
min_height=$2
network_id=$3

# How often to poll the network height (in seconds)
poll_interval=10

# Default values if not provided
: "${total_validators:=40}"
: "${min_height:=250}"
: "${network_id:=1}"

#shellcheck source=SCRIPTDIR/utils.sh
. ./.ci/utils.sh

# Create log directory
init_log_dir

git_commit=$(git rev-parse --short=10 HEAD)
echo "On git commit ${git_commit}"

network_name=$(get_network_name "$network_id")
echo "Network set to $network_name with $total_validators validators"

# Define a trap handler that cleans up all processes on exit.
#shellcheck disable=SC2329
function exit_handler() {
  stop_nodes
}
trap exit_handler EXIT

# Define a trap handler that prints a message when an error occurs
trap 'echo "⛔️ Error in $BASH_SOURCE at line $LINENO: \"$BASH_COMMAND\" failed (exit $?)"' ERR

# Flags used by all ndoes
common_flags=(--nodisplay --nobanner --noupdater "--network=$network_id"
  "--log-filter=$log_filter" "--dev-num-validators=$total_validators")

# Start all validator nodes in the background
for ((validator_index = 0; validator_index < total_validators; validator_index++)); do
  snarkos clean --dev $validator_index "--network=${network_id}"

  log_file="$log_dir/validator-$validator_index.log"
  if [ $validator_index -eq 0 ]; then
    run_with_prefix "validator-$validator_index" snarkos start "${common_flags[@]}" --dev "$validator_index" \
      --validator --logfile "$log_file" --metrics --no-dev-txs
  else
    run_with_prefix "validator-$validator_index" snarkos start "${common_flags[@]}" --dev "$validator_index" \
      --validator --logfile "$log_file"
  fi
  PIDS[validator_index]=$!
  echo "Started validator $validator_index with PID ${PIDS[$validator_index]}"

  # Add 1-second delay between starting nodes to avoid hitting rate limits
  sleep 1
done

# Ensure all nodes are up and running.
wait_for_nodes "$total_validators" 0 "$network_name"

# Wait until the first node reaches the given height.
total_wait=0
while ! check_heights 0 1 "$min_height" "$network_name"; do
  # Continue waiting
  sleep "$poll_interval"
  total_wait=$((total_wait + poll_interval))
  echo "Waited $total_wait seconds so far..."
done

printf "num_validators=%i, git_commit=%s, snapshot_height=%i" "$total_validators" "$git_commit" "$min_height" > info.txt

zipname="sync-ledger-val${total_validators}-${min_height}-${git_commit}.zip"
echo "Done! Generating zipfile \"$zipname\""
zip -r "$zipname" ".ledger-${network_id}-0" info.txt

exit 0
