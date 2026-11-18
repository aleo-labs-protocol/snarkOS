#!/bin/bash

# devnet-topo.sh

# Like devnet.sh, but with these changes that stabilize the network topology:
#
# * Asks for the number N of validators, not the number of clients.
#   Asks how many core clients each validator should have.
#   Makes the appropriate number of clients, to reflect
#   the concept that each validator has 1 or 2 dedicated core clients.
# * For the Gateway (communication for BFT consensus)
#   sets --validators starting up each validator to all the other validators
#   (devnet.sh ends up setting it only to the first two validators).
# * For the Router (p2p communication), sets --peers (the trusted peers)
#   directly, rather than allowing them to be set by parse_development(),
#   * Each validator has 1 or 2 trusted peers: its core clients.
#     No longer allow validators to accept external peer connections
#     (removed the "--allow-external-peers" flag).
#   * Each client has either 
#     N trusted peers (when there is 1 core client per validator)
#     or 2N trusted peers (when there are 2 core clients per validator).
#     In the first case, the client's trusted peers are its validator and N-1 other clients.
#     In the second case, the client's trusted peers are its validator and 2N-1 other clients.

if [[ -n "$TMUX" ]]; then
   echo "Detected nested tmux session. Try again after unsetting \$TMUX, e.g., using \`unset TMUX\` in bash."
   exit 1
fi

# Read the total number of validators from the user or use a default value of 4
read -p "Enter the total number of validators (default: 4): " total_validators
total_validators=${total_validators:-4}

# Validate that total_validators is a positive integer
if ! [[ "$total_validators" =~ ^[1-9][0-9]*$ ]]; then
    echo "Error: Number of validators must be a positive integer"
    exit 1
fi

# Read the network ID from user or use a default value of 1
read -p "Enter the network ID (mainnet = 0, testnet = 1, canary = 2) (default: 1): " network_id
network_id=${network_id:-1}

# Validate that network_id is 0, 1, or 2
if ! [[ "$network_id" =~ ^[012]$ ]]; then
    echo "Error: Network ID must be 0 (mainnet), 1 (testnet), or 2 (canary)"
    exit 1
fi

# Read the number of core clients per validator from user or use a default value of 2
read -p "Enter the number of core clients for each validator (1 or 2; default: 2): " core_clients_per_validator
core_clients_per_validator=${core_clients_per_validator:-2}

# Validate that core_clients_per_validator is either 1 or 2
if [[ "$core_clients_per_validator" != "1" && "$core_clients_per_validator" != "2" ]]; then
    echo "Error: Number of core clients per validator must be 1 or 2"
    exit 1
fi

# The total number of clients is the number of validators times the number of core clients per validator.
total_clients=$((total_validators * core_clients_per_validator))

# Ask the user if they want to run 'cargo install --locked --path .' or use a pre-installed binary
read -p "Do you want to run 'cargo install --locked --path .' to build the binary? (y/n, default: y): " build_binary
build_binary=${build_binary:-y}

# Ask the user whether to clear the existing ledger history
read -p "Do you want to clear the existing ledger history? (y/n, default: n): " clear_ledger
clear_ledger=${clear_ledger:-n}

# Log verbosity is set to 1 (DEBUG) by default.
verbosity=1

if [[ $build_binary == "y" ]]; then
  # Ask the user if they want to enable validator telemetry
  read -p "Do you want to enable validator telemetry? (y/n, default: y): " enable_telemetry
  enable_telemetry=${enable_telemetry:-y}

  # Build command
  build_cmd="cargo install --locked --path ."

  # Add the telemetry feature if requested
  if [[ $enable_telemetry == "y" ]]; then
    build_cmd+=" --features telemetry"
  fi

  # Build command
  echo "Running build command: \"$build_cmd\""
  eval "$build_cmd" || exit 1
fi

# Clear the ledger logs for each validator if the user chooses to clear ledger
if [[ $clear_ledger == "y" ]]; then
  # Create an array to store background processes
  clean_processes=()

  for ((index = 0; index < $((total_validators + total_clients)); index++)); do
    # Run 'snarkos clean' for each node in the background
    snarkos clean --network $network_id --dev $index &

    # Store the process ID of the background task
    clean_processes+=($!)
  done

  # Wait for all 'snarkos clean' processes to finish
  for process_id in "${clean_processes[@]}"; do
    wait "$process_id"
  done
fi

# Create a timestamp-based directory for log files
log_dir=".logs-$(date +"%Y%m%d%H%M%S")"
mkdir -p "$log_dir"

# Create a new tmux session named "devnet"
tmux new-session -d -s "devnet" -n "validator-0"
if [[ $? -ne 0 ]]; then
   echo "Failed to create new TMUX session."
   exit 1
fi

# Get the tmux's base-index for windows
# we have to create all windows with index offset by this much
index_offset="$(tmux show-option -gv base-index)"
if [ -z "$index_offset" ]; then
  index_offset=0
fi

# Generate validator indices from 0 to (total_validators - 1).
validator_indices=($(seq 0 $((total_validators - 1))))

# Loop through the list of validator indices and create a new window for each
for validator_index in "${validator_indices[@]}"; do
  # Generate a unique and incrementing log file name based on the validator index
  name="validator-$validator_index"
  log_file="$log_dir/$name.log"
  window_index=$((validator_index + index_offset))
  metrics_port=$((validator_index + 9000))

  if [ "$validator_index" -ne 0 ]; then
    # Create a new window with a unique name
    tmux new-window -t "devnet:$window_index" -n $name
  fi

  # Create the list of trusted validators for the validator at validator_index
  trusted_validators=""
  for trusted_validator_index in "${validator_indices[@]}"; do
      if [ "$validator_index" -ne "$trusted_validator_index" ]; then
          if [ "$trusted_validators" = "" ]; then
              trusted_validators="127.0.0.1:$((5000 + trusted_validator_index))"
          else
              trusted_validators="${trusted_validators},127.0.0.1:$((5000 + trusted_validator_index))"
          fi
      fi
  done

  # Create the list of peers (i.e., trusted peers) for the validator at validator_index.
  # Each validator has 1 or 2 trusted peers: its core clients
  if [ "$core_clients_per_validator" -eq 1 ]; then
      # Each validator has 1 trusted peer: its core client
      client_offset=$((validator_index * core_clients_per_validator))
      validator_trusted_peers="127.0.0.1:$((4130 + total_validators + client_offset))"
  else
      # Each validator has 2 trusted peers: its two core clients
      client_offset_1=$((validator_index * core_clients_per_validator))
      client_offset_2=$((validator_index * core_clients_per_validator + 1))
      validator_trusted_peers="127.0.0.1:$((4130 + total_validators + client_offset_1)),127.0.0.1:$((4130 + total_validators + client_offset_2))"
  fi

  # Send the command to start the validator to the new window and capture output to the log file
  tmux send-keys -t "devnet:$window_index" "snarkos start --nodisplay --network $network_id --dev $validator_index --dev-num-validators $total_validators --validator --validators $trusted_validators --peers $validator_trusted_peers --logfile $log_file --verbosity $verbosity  --metrics --metrics-ip=0.0.0.0:$metrics_port" C-m
done

# Define a function that generates the string to be passed as --peers when starting a client node.
# The trusted peers consist of the one paired validator plus all the other clients.
# The string looks like "vvv${v},ccc0,ccc1,...,ccc${n}"
# excluding ccc${i} from the list,
# where vvv${v} is "127.0.0.1:$((4130 + (i / core_clients_per_validator)))"
# and ccc${i} is "127.0.0.1:$((4130 + total_validators + i))"
client_trusted_peers() {
    # n is the number of validators
    # i is the index of the client for which we want a trusted peers list
    local n=$1
    local i=$2

    # Calculate which validator this client belongs to
    local validator_index=$((i / core_clients_per_validator))
    
    # Start with the client's validator as the first trusted peer.
    local result="127.0.0.1:$((4130 + validator_index))"

    # Add all other clients as trusted peers
    for ((j=0; j<total_clients; j++)); do
        if [ $j -ne $i ]; then
            result="${result},127.0.0.1:$((4130 + total_validators + j))"
        fi
    done

    echo "$result"
}

# Generate client indices from 0 to (total_clients - 1)
client_indices=($(seq 0 $((total_clients - 1))))

# Loop through the list of client indices and create a new window for each
for client_index in "${client_indices[@]}"; do
  # Generate a unique and incrementing log file name based on the client index
  name="client-$client_index"
  log_file="$log_dir/$name.log"

  window_index=$((client_index + total_validators + index_offset))

  # Create a new window with a unique name
  tmux new-window -t "devnet:$window_index" -n $name

  # Send the command to start the client to the new window and capture output to the log file
  tmux send-keys -t "devnet:$window_index" "snarkos start --nodisplay --network $network_id --dev $((total_validators + client_index)) --dev-num-validators $total_validators --client --peers $(client_trusted_peers $total_validators $client_index) --logfile $log_file  --verbosity $verbosity" C-m
done

# Attach to the tmux session to view and interact with the windows
tmux attach-session -t "devnet"
