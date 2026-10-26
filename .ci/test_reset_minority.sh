#! /usr/bin/env bash

set -eo pipefail # error on any command failure

# Ensure that we run a recent version of bash
if [ "${BASH_VERSINFO[0]}" -lt 5 ]; then
  echo "Error: This script requires bash version 5.0 or higher."
  exit 1
fi

#shellcheck source=SCRIPTDIR/utils.sh
. ./.ci/utils.sh

# Set up the logging directory
init_log_dir

# Network parameters
total_validators=$1
network_id=$2
reset_interval=$3
final_height=$4
num_resets=$5

# Default values if not provided
: "${total_validators:=7}"
: "${network_id:=0}"
: "${reset_interval:=50}"
: "${final_height:=250}"
: "${num_resets:=3}"

minority=$(( (total_validators - 1) / 3 ))
network_name=$(get_network_name "$network_id")
verbosity=1
max_validator_log_size_bytes=$((6 * 1024 * 1024))

# The time that is used to determine the total timeout for the test
max_wait_per_block=10

# Define a trap handler that cleans up all processes on exit.
trap stop_nodes EXIT

# Define a trap handler that prints a message when an error occurs 
trap 'log "⛔️ Error in $BASH_SOURCE at line $LINENO: \"$BASH_COMMAND\" failed (exit $?)"' ERR

# Define flags used by all nodes.
common_flags=(
  --nodisplay --nobanner --noupdater "--network=$network_id" "--verbosity=$verbosity"
  "--dev-num-validators=$total_validators"
)

# Start all validator nodes in the background
for validator_index in $(seq 0 $((total_validators-1))); do
  snarkos clean "--dev=$validator_index" "--network=$network_id"
 
  run_with_prefix "validator-$validator_index" snarkos start "${common_flags[@]}" "--dev=$validator_index" --validator --logfile="$log_dir/validator-$validator_index.log"
  PIDS[validator_index]=$!

  log "Started validator $validator_index with PID ${PIDS[$validator_index]}"
  # Add 1-second delay between starting nodes to avoid hitting rate limits
  sleep 1
done

wait_for_nodes "$total_validators" 0 "$network_name" 180

# Wait longer if there are more blocks to reach.
max_wait=$((final_height * max_wait_per_block));

for iter in $(seq 1 "$num_resets"); do
  reset_height=$(( iter * reset_interval ));

  # Wait until all nodes reach the reset height.
  if ! wait_for_heights 0 "$total_validators" "$reset_height" "$network_name" $((reset_interval * max_wait_per_block)); then
    log "❌ Test failed! Not all nodes reached reset height of $reset_height within $((reset_interval * max_wait_per_block)) seconds."
    exit 1
  fi
  log "All nodes reached reset height."

  # Gracefully shut down a minority of the validators
  mapfile -t target_indices < <(generate_random_indices "$minority" $(( ${#PIDS[@]} - 1 )))
  stop_some_nodes "${target_indices[@]}"

  for target_index in "${target_indices[@]}"; do
    # Remove the original ledger
    snarkos clean "--network=$network_id" "--dev=$target_index"
    # Wait until the cleanup concludes
    sleep 1
    # Restart
    run_with_prefix "validator-$target_index" snarkos start "${common_flags[@]}" "--dev=$target_index" --validator --logfile="$log_dir/validator-$target_index.log"
    PIDS[target_index]=$!
    log "Restarted a fresh validator $target_index with PID ${PIDS[$target_index]}"
    # Add 1-second delay between starting nodes to avoid hitting rate limits
    sleep 1
  done
done

if wait_for_heights 0 "$total_validators" "$final_height" "$network_name" "$max_wait"; then
  log "All nodes reached the final height of $final_height"
else
  log "❌ Test failed! Not all nodes reached final height of $final_height within $max_wait seconds."
  exit 1
fi

if check_logs "$log_dir" "$total_validators" 0 "$max_validator_log_size_bytes"; then
  exit 0
else
  exit 1
fi
