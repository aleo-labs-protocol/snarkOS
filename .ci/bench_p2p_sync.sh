#!/bin/bash

###########################################################
# Measures a client syncing 1000 blocks from another client
###########################################################

set -eo pipefail # error on any command failure

network_id=1
min_height=250

# The total number of validators in the beacon committee.
# This must match the number of validators used when generating the snapshot. 
num_validators=40

# The number of clients that are syncing
# Note: Because the first indexes 0-39 are resevered for validators, the first client will have index 40.
# The script works around this by manually setting the storage, ports, and log files for the clients. 
num_clients=1

# Adjust this to show more/less log messages
log_filter="info,snarkos_node::client=trace,snarkos_node_sync=trace,snarkos_node_tcp=warn,snarkos_node_rest=warn"

max_wait=2400 # Wait for up to 40 minutes
poll_interval=1 # Check block heights every second

# shellcheck source=SCRIPTDIR/utils.sh
. ./.ci/utils.sh

# Create log directory
init_log_dir

# Running sums for variance: use sum and sumsq for unbiased sample variance
sum_speed=0
sumsq_speed=0
samples=0
max_speed=0.0

# Fetch sync speeds from clients via REST and accumulate stats
function sample_sync_speeds() {
  for ((client_index = 1; client_index <= num_clients; client_index++)); do
    port=$((3030 + client_index))
    resp=$(curl -s "http://127.0.0.1:$port/v2/$network_name/sync/status" || true)

    # Skip if response missing
    if [[ -z "$resp" ]]; then
      continue
    fi

    speed=$(echo "$resp" | jq -r '.sync_speed_bps')

    # Skip null or empty
    if [[ -z "$speed" ]] || [[ "$speed" == "null" ]]; then
      log "Invalid speed value $speed"
      continue
    fi

    # Validate numeric (allow exponent)
    if ! (is_float "$speed"); then
        log "Invalid speed value $speed"
       continue
    fi

    # Convert to fixed decimal for bc -l
    speed_dec=$(awk -v x="$speed" 'BEGIN{printf "%.12f", x}')
    if [[ -z "$speed_dec" ]]; then
      continue
    fi

    if (( $(echo "$speed > $max_speed" | bc -l) )); then
      max_speed=$speed
    fi

    # Accumulate using bc -l for floating point
    sum_speed=$(echo "$sum_speed + $speed_dec" | bc -l)
    sumsq_speed=$(echo "$sumsq_speed + ($speed_dec * $speed_dec)" | bc -l)
    samples=$((samples + 1))
  done
}

branch_name=$(git rev-parse --abbrev-ref HEAD)
log "On branch: ${branch_name}"

network_name=$(get_network_name $network_id)
log "Using network: $network_name (ID: $network_id)"

snapshot_info=$(<info.txt)
log "Snapshot_info: ${snapshot_info}"

# Define a trap handler that cleans up all processes on exit.
trap stop_nodes EXIT

# Define a trap handler that prints a message when an error occurs.
trap 'log "⛔️ Error in $BASH_SOURCE at line $LINENO: \"$BASH_COMMAND\" failed (exit $?)"' ERR

# Shared flags between all nodes
common_flags=(
  --nodisplay --nobanner --noupdater # reduce clutter in the output
  "--log-filter=$log_filter" # only show the logs we care about
  "--network=$network_id"
  --nocdn # don't sync from CDN, so we only benchmark p2p sync
  "--dev-num-validators=$num_validators"
  --no-dev-txs
)

# The client that has the ledger
# (runs on the first two cores)
# shellcheck disable=SC2086
run_with_prefix "client-0" $TASKSET1 snarkos start "--dev=$num_validators" --client "${common_flags[@]}" \
  "--logfile=$log_dir/client-0.log" "--storage=.ledger-$network_id-0" \
  "--node=127.0.0.1:4130" "--rest=127.0.0.1:3030"
PIDS[0]=$!

# Spawn the clients that will sync the ledger
# (running on the other two cores)
for client_index in $(seq 1 "$num_clients"); do
  node_index=$((num_validators + client_index))
  prev_port=$((4130+client_index-1))
  node_addr="127.0.0.1:$((4130+client_index))"
  name="client-$client_index"

  # Ensure there are no old ledger files and the node syncs from scratch
  snarkos clean "--dev=$node_index" "--network=$network_id" "--path=.ledger-$network_id-$client_index" || true

  # shellcheck disable=SC2086
  run_with_prefix "$name" $TASKSET2 snarkos start "--dev=$node_index" --client \
    "${common_flags[@]}" "--peers=127.0.0.1:$prev_port" "--node=$node_addr" \
    "--rest=127.0.0.1:$((3030+client_index))" \
    "--logfile=$log_dir/$name.log" "--storage=.ledger-$network_id-$client_index"
  PIDS[client_index]=$!

  # Add 1-second delay between starting nodes to avoid hitting rate limits
  sleep 1
done

# Block until nodes are running and connected to each other.
wait_for_nodes $((num_clients+1)) 0 "$network_name"

# It takes about 30s for nodes to connect. Do not measure this time.
SECONDS=0
for node_index in $(seq 0 "$num_clients"); do
  if ! (wait_for_peers "$node_index" $num_clients "$network_name"); then
    exit 1
  fi
done

connect_time=$SECONDS
log "ℹ️ Nodes are fully connected (took $connect_time secs). Starting block sync measurement."

# Ensure the first node actually has the ledger snapshot.
# This should succeed instantly in most cases
SECONDS=0
has_blocks=false
while (( SECONDS < 30 )); do
  if check_heights 0 1 $min_height "$network_name" "0"; then
    has_blocks=true
    break
  fi

  sleep "$poll_interval"
done

if ! $has_blocks; then
  log "Node #0 has not reached the expected height. Maybe the ledger snapshot is corrupted or outdated?"
  exit 1
fi

# Count the initial startup of node #0 as part of the benchmark as the other node
# might already start syncing.
# SECONDS=0 

# Check heights periodically with a timeout
while (( SECONDS < max_wait )); do
  # Sample sync speed(s) for variance calculation
  sample_sync_speeds

  if check_heights 1 $((num_clients+1)) $min_height "$network_name" "$SECONDS"; then
    total_wait=$SECONDS
    throughput=$(compute_throughput "$min_height" "$total_wait")

    # Compute unbiased sample variance of sync_speed_bps (in blocks^2/s^2)
    if (( samples > 1 )); then
      mean_speed=$(echo "scale=8; $sum_speed / $samples" | bc -l)
      variance=$(echo "scale=8; (($sumsq_speed / $samples) - ($mean_speed * $mean_speed)) * ($samples / ($samples - 1))" | bc -l)
    else
      mean_speed=$(echo "scale=8; 0" | bc -l)
      variance=$(echo "scale=8; 0" | bc -l)
    fi

    log "🎉 P2P sync benchmark done! Waited $total_wait seconds for $min_height blocks. Throughput was $throughput blocks/s."

    # Append data to results file.
    printf "{ \"name\": \"p2p-sync\", \"unit\": \"blocks/s\", \"value\": %.3f, \"extra\": \"total_wait=%is, target_height=%i, connect_time=%is, %s\" },\n" \
       "$throughput" "$total_wait" "$min_height" "$connect_time" "$snapshot_info" | tee -a results.json
    printf "{ \"name\": \"p2p-sync-speed-variance\", \"unit\": \"blocks^2/s^2\", \"value\": %.6f, \"extra\": \"samples=%d, mean_speed=%.6f, max_speed=%.6f, branch=%s, %s\" },\n" \
       "$variance" "$samples" "$mean_speed" "$max_speed" "$branch_name" "$snapshot_info" | tee -a results.json

    exit 0
  fi
  
  # Continue waiting
  sleep "$poll_interval"
done

log "❌ Benchmark failed! Clients did not sync within 40 minutes."

# Print logs for debugging
print_client_logs "$log_dir" "$num_validators" "$num_clients"

exit 1
