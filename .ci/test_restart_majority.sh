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
num_resets=$4

# Default values if not provided
: "${total_validators:=7}"
: "${network_id:=0}"
: "${reset_interval:=20}"
: "${num_resets:=3}"

# Blocks to advance after all restarts complete.
final_height_advance=20

max_faulty=$(( (total_validators - 1) / 3 ))
# AleoBFT needs at least N-f for a quorum, not 2*f+1.
majority=$((total_validators - max_faulty))
network_name=$(get_network_name "$network_id")

verbosity=1
max_validator_log_size_bytes=$((3 * 1024 * 1024))

# The time that is used to determine the total timeout for the test.
# Set this higher than the interval for the minority test, as more nodes need to sync.
max_wait_per_block=20

# How long a restarted node may take to become ready again.
# Loading an existing ledger is dominated by deserializing the cached block tree,
# which has been observed to take over two minutes on CI.
max_restart_wait=300

# Define a trap handler that cleans up all processes on exit.
trap stop_nodes EXIT

# Define a trap handler that prints a message when an error occurs 
trap 'log "⛔️ Error in $BASH_SOURCE at line $LINENO: \"$BASH_COMMAND\" failed (exit $?)"' ERR

# Define flags used by all nodes.
common_flags=(
  --nodisplay --nobanner --noupdater "--network=$network_id" "--verbosity=$verbosity"
  "--dev-num-validators=$total_validators"
)

start=$(now)

# Start all validator nodes in the background
for validator_index in $(seq 0 $((total_validators-1))); do
  run_with_prefix "validator-$validator_index" snarkos start "${common_flags[@]}" "--dev=$validator_index" --validator --logfile="$log_dir/validator-$validator_index.log"
  PIDS[validator_index]=$!
  log "Started validator $validator_index with PID ${PIDS[$validator_index]}"
  # Add 1-second delay between starting nodes to avoid hitting rate limits
  sleep 1
done

wait_for_nodes "$total_validators" 0 "$network_name" 180

for iter in $(seq 1 "$num_resets"); do
  reset_height=$(( iter * reset_interval ));

  # Block until the reset height is reached.
  if ! wait_for_heights 0 "$total_validators" "$reset_height" "$network_name" $((reset_interval * max_wait_per_block)); then
    log "❌ Test failed! Not all nodes reached reset height of $reset_height within $((reset_interval * max_wait_per_block)) seconds."
    exit 1
  fi
  log "All nodes reached the next reset height."

  # Gracefully shut down a majority of the validators
  mapfile -t target_indices < <(generate_random_indices "$majority" $(( ${#PIDS[@]} - 1 )))
  stop_some_nodes "${target_indices[@]}"

  # wait for a non-trivial amount of time
  sleep 30

  for target_index in "${target_indices[@]}"; do
    # Restart
    run_with_prefix "validator-$target_index" snarkos start "${common_flags[@]}" "--dev=$target_index" --validator --logfile="$log_dir/validator-$target_index.log"
    PIDS[target_index]=$!
    log "Restarted a fresh validator $target_index with PID ${PIDS[$target_index]}"
    # Add 1-second delay between starting nodes to avoid hitting rate limits
    sleep 1
  done

  # Block until the restarted nodes serve their REST API again.
  # Note: loading an existing ledger can take a couple of minutes, so be generous here.
  # Doing this inside the loop also ensures that the startup time is not deducted from
  # the time budget that the next `wait_for_heights` gets.
  if ! wait_for_nodes "$total_validators" 0 "$network_name" "$max_restart_wait"; then
    log "❌ Test failed! Not all nodes became ready within $max_restart_wait seconds after restart #$iter."
    exit 1
  fi
done

log_check_since=$(epoch_now)
latest_height=$(get_block_height 0 "$network_name")
final_height=$((latest_height + final_height_advance))
max_wait=$((final_height_advance * max_wait_per_block))

log "All restarts complete at height ${latest_height}. Waiting for final height ${final_height}..."

if ! wait_for_heights 0 "$total_validators" "$final_height" "$network_name" "$max_wait"; then
  log "❌ Test failed! Not all nodes reached final height of $final_height within $max_wait seconds."
  exit 1
fi

log "SUCCESS! Network took $(elapsed_since "$start") seconds to reach final height of $final_height after $num_resets resets."

if check_logs "$log_dir" "$total_validators" 0 "$max_validator_log_size_bytes" "" "$log_check_since"; then
  exit 0
else
  exit 1
fi