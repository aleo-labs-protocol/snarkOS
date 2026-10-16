#!/bin/bash

#shellcheck source=SCRIPTDIR/utils.sh
. ./.ci/utils.sh

# Change this to increase/decrease log verbosity
log_verbosity=4
max_validator_log_size_bytes=$((2 * 1024 * 1024))

# Network parameters
total_validators=4
network_id=0
network_name="mainnet"

# Ledgers, node data and checkpoints
tmp_dir="${TMPDIR:-/tmp}"

# Stopping conditions
checkpoint_height=3
rollback_height=10
num_checkpoints=0
remaining_checkpoints=2

# Create log directory
init_log_dir

# The ledger checkpoint with the given suffix.
function checkpoint_dir() {
  echo "$tmp_dir/checkpoint_$1"
}

# Use fixed JWT values in order to be able to create checkpoints
jwt_secret="ZGJjaGVja3BvaW50dGVzdA=="
jwt_ts=1749116345
jwt[0]="eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJhbGVvMXJoZ2R1NzdoZ3lxZDN4amo4dWN1M2pqOXIya3J3ejZtbnp5ZDgwZ25jcjVmeGN3bGg1cnN2enA5cHgiLCJpYXQiOjE3NDkxMTYzNDUsImV4cCI6MjA2NDQ3NjM0NX0.qm2idfIm4ZTFOsyT19lH9pcWzzAtP5mbymkN4oL6_sc"
jwt[1]="eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJhbGVvMXMzd3M1dHJhODdmanljbmpyd3NqY3JudzJxeHI4amZxcWR1Z25mMHh6cXF3MjlxOW01cHFlbTJ1NHQiLCJpYXQiOjE3NDkxMTYzNDUsImV4cCI6MjA2NDQ3NjM0NX0.4efs4qWJuG0Lm2CxrLMIKrrbJiGD-XNqHlk_AUaXOBo"
jwt[2]="eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJhbGVvMWFzaHl1OTZ0andlNjN1MGd0bm52OHo1bGhhcGR1NGw1cGpzbDJraGE3ZnY3aHZ6MmVxeHM1ZHowcmciLCJpYXQiOjE3NDkxMTYzNDUsImV4cCI6MjA2NDQ3NjM0NX0.zxO1ajmQ0Wqr1gg4NuRzH4i_hiUBt7_fP9WP3KHbp4c"
jwt[3]="eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJhbGVvMTJ1eDNnZGF1Y2swdjYwd2VzdGdjcHFqN3Y4cnJjcjN2MzQ2ZTRqdHEwNHE3a2t0MjJjenNoODA4djIiLCJpYXQiOjE3NDkxMTYzNDUsImV4cCI6MjA2NDQ3NjM0NX0.bJZ-fcrJwaI5YdPXDQ1nySV-jmxeABQCSvL1Ag9CSpo"

# Define a trap handler that cleans up all processes on exit.
trap stop_nodes EXIT

# Define a trap handler that prints a message when an error occurs 
trap 'log "⛔️ Error in $BASH_SOURCE at line $LINENO: \"$BASH_COMMAND\" failed (exit $?)"' ERR

# Start all validator nodes in the background
for ((validator_index = 0; validator_index < total_validators; validator_index++)); do
  snarkos clean --dev $validator_index --network=$network_id \
    "--node-data-storage=$tmp_dir/node_data_$validator_index" "--ledger-storage=$tmp_dir/ledger_$validator_index"
  # RocksDB refuses to write a checkpoint into an existing directory.
  for ((checkpoint_index = 0; checkpoint_index <= remaining_checkpoints; checkpoint_index++)); do
    snarkos clean --dev $validator_index --network=$network_id --keep-node-data \
      "--ledger-storage=$(checkpoint_dir "${validator_index}_$checkpoint_index")"
  done

  log_file="$log_dir/validator-$validator_index.log"
  run_with_prefix "validator-$validator_index" snarkos start --nodisplay --network $network_id --dev $validator_index --dev-num-validators $total_validators \
    --validator --jwt-secret $jwt_secret --jwt-timestamp $jwt_ts --verbosity $log_verbosity "--logfile=$log_file" \
    "--node-data-storage=$tmp_dir/node_data_$validator_index" "--ledger-storage=$tmp_dir/ledger_$validator_index"
  PIDS[validator_index]=$!
  log "Started validator $validator_index with PID ${PIDS[$validator_index]}"
  # Add 1-second delay between starting nodes to avoid hitting rate limits
  sleep 1
done

# The height of each node just before its most recent checkpoint.
declare -a checkpoint_floor=()

# Create database checkpoints
function create_checkpoints() {
  for ((node_index = 0; node_index < total_validators; node_index++)); do
    port=$((3030 + node_index))
    path=$(checkpoint_dir "${node_index}_$1")
    checkpoint_floor[node_index]=$(get_block_height_by_port "$port" "$network_name")
    if [[ -z "${checkpoint_floor[node_index]}" ]]; then
      log "❌ Node #${node_index} did not report its height before the checkpoint"
      return 1
    fi
    if ! curl -sf -X "POST" -H "Authorization: Bearer ${jwt[node_index]}" "http://127.0.0.1:$port/v2/$network_name/db_backup?path=$path" > /dev/null; then
      log "❌ Node #${node_index} failed to create a checkpoint at $path"
      return 1
    fi
    if [[ ! -d "$path" ]]; then
      log "❌ Node #${node_index} reported a checkpoint, but $path does not exist"
      return 1
    fi
  done

  log "All nodes created a checkpoint"
  return 0
}

wait_for_nodes "$total_validators" 0 "$network_name"

# Check heights periodically with a timeout
total_wait=0
checkpoint_created=false
while (( total_wait < 600 )); do  # 10 minutes max
  # Apply short-circuiting
  if [[ $checkpoint_created = true ]] || check_heights  0 "$total_validators" "$checkpoint_height" "$network_name"; then
    if [[ $checkpoint_created = false ]]; then
      # Create checkpoints at the specified height
      if ! create_checkpoints $num_checkpoints; then
        log "❌ Test failed!"
        exit 1
      fi
      checkpoint_created=true
      checkpoint_height=$((checkpoint_height+2))
      num_checkpoints=$((num_checkpoints+1))

      log "num_checkpoints: $num_checkpoints"
      sleep 2
    fi

    # Wait until the specified rollback height is reached
    if check_heights 0 "$total_validators" "$rollback_height" "$network_name"; then
      log "All nodes reached rollback height."

      checkpoint_created=false

      # Gracefully shut down the validators
      stop_nodes
      # Wait until the shutdown concludes.
      wait

      for ((validator_index = 0; validator_index < total_validators; validator_index++)); do
        # Remove the ledger storage. The node data is not backed up yet and will be kept. 
        if (( num_checkpoints == 1 )); then 
          # Remove the original ledger
          snarkos clean "--network=$network_id" "--dev=$validator_index" --keep-node-data \
              "--ledger-storage=$tmp_dir/ledger_$validator_index"
        else
          # Remove the checkpoint the node last ran from
          suffix="${validator_index}_$((num_checkpoints-2))"
          snarkos clean "--network=$network_id" "--dev=$validator_index" --keep-node-data \
              "--ledger-storage=$(checkpoint_dir "$suffix")"
        fi
       # Wait until the cleanup concludes
        sleep 1

        # Restart using the checkpoint
        suffix="${validator_index}_$((num_checkpoints-1))"
        log_file="$log_dir/validator-$validator_index.log"
        run_with_prefix "validator-$validator_index" snarkos start --nodisplay "--network=$network_id" "--dev=$validator_index" "--dev-num-validators=$total_validators" \
          --validator "--jwt-secret=$jwt_secret" "--jwt-timestamp=$jwt_ts" --verbosity $log_verbosity "--logfile=$log_file" \
          "--node-data-storage=$tmp_dir/node_data_$validator_index" "--ledger-storage=$(checkpoint_dir "$suffix")"
        PIDS[validator_index]=$!
        log "Restarted validator $validator_index with PID ${PIDS[$validator_index]}"
        # Add 1-second delay between starting nodes to avoid hitting rate limits
        sleep 1

        # The REST server starts after the ledger loads and before the node connects to peers,
        # so the first height it reports is the height of the ledger it loaded.
        port=$((3030 + validator_index))
        height=""
        for ((attempt = 0; attempt < 120; attempt++)); do
          height=$(get_block_height_by_port "$port" "$network_name" 2)
          if [[ -n "$height" ]]; then
            break
          fi
          if ! kill -0 "${PIDS[validator_index]}" 2>/dev/null; then
            log "❌ Node #${validator_index} exited after the restart"
            exit 1
          fi
          sleep 1
        done
        if [[ -z "$height" ]]; then
          log "❌ Node #${validator_index} did not report its height after the restart"
          exit 1
        fi
        log "Node #${validator_index} height after restart: $height"

        # A node that ignored the checkpoint starts either at genesis or at its pre-shutdown height.
        if (( height < checkpoint_floor[validator_index] || height >= rollback_height )); then
          log "❌ Node #${validator_index} restarted at height $height, expected at least ${checkpoint_floor[validator_index]} and below $rollback_height"
          log "❌ Test failed!"
          exit 1
        fi
      done

      if (( remaining_checkpoints == 0 )); then
        if check_logs "$log_dir" "$total_validators" 0 "$max_validator_log_size_bytes"; then
          exit 0
        else
          exit 1
        fi
      fi

      remaining_checkpoints=$((remaining_checkpoints-1))
    fi
  fi

  # Continue waiting
  sleep 3
  total_wait=$((total_wait + 3))
  log "Waited $total_wait seconds so far..."
done

# The main loop has expired by now
log "❌ Test failed!"
exit 1
