#!/usr/bin/env bash

if [[ -n "$TMUX" ]]; then
  echo "Detected nested tmux session. Try again after unsetting \$TMUX, e.g., using \`unset TMUX\` in bash."
  exit 1
fi

# Resolve the repo root from this script's own location, so the script works
# regardless of the caller's current working directory.
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Read the total number of validators from the user or use a default value of 4
read -r -p "Enter the total number of validators (default: 4): " total_validators
total_validators=${total_validators:-4}

# Read the total number of clients from the user or use a default value of 2
read -r -p "Enter the total number of clients (default: 2): " total_clients
total_clients=${total_clients:-2}

# Read the network ID from user or use a default value of 1
read -r -p "Enter the network ID (mainnet = 0, testnet = 1, canary = 2) (default: 1): " network_id
network_id=${network_id:-1}

case "$network_id" in
  0) network_name="mainnet" ;;
  1) network_name="testnet" ;;
  2) network_name="canary" ;;
  *) network_name="network-$network_id" ;;
esac

# Ask the user whether validator 0 should drive the network with transactions. Nothing else
# in a devnet creates them, so the dashboard's transmission panels stay at zero without it.
read -r -p "Do you want validator 0 to generate transactions? (y/n, default: n): " dev_txs
dev_txs=${dev_txs:-n}

if [[ $dev_txs == "y" ]]; then
  dev_txs_flag=""
else
  dev_txs_flag=" --no-dev-txs"
fi

# Regenerate the Prometheus scrape config so it targets exactly the validators
# this devnet will start (one target per validator's metrics port, 9000 + index).
#
# The target labels mirror the ones the production scrape config attaches, because the
# Grafana dashboard provisioned alongside this stack is the production dashboard and its
# queries select on them. Clients are started without `--metrics`, so every target here
# is a validator.
prometheus_yml="$repo_root/node/metrics/prometheus.yml"
{
  echo "global:"
  echo "  scrape_interval: 15s"
  echo "  scrape_timeout: 10s"
  echo "  evaluation_interval: 1m"
  echo "scrape_configs:"
  echo "- job_name: prometheus"
  echo "  honor_timestamps: true"
  echo "  scrape_interval: 15s"
  echo "  scrape_timeout: 10s"
  echo "  metrics_path: /metrics"
  echo "  scheme: http"
  echo "  follow_redirects: true"
  echo "  static_configs:"
  echo "  - targets:"
  echo "    - localhost:9090"
  echo "- job_name: snarkos"
  echo "  honor_timestamps: true"
  echo "  scrape_interval: 15s"
  echo "  scrape_timeout: 10s"
  echo "  metrics_path: /metrics"
  echo "  scheme: http"
  echo "  follow_redirects: true"
  echo "  static_configs:"
  for ((validator_index = 0; validator_index < total_validators; validator_index++)); do
    metrics_port=$((9000 + validator_index))
    echo "  - targets:"
    echo "    - host.docker.internal:$metrics_port"
    echo "    labels:"
    echo "      instance_name: validator-$validator_index"
    echo "      public_ip: 127.0.0.1:$metrics_port"
    echo "      snarkos_role: validator"
    echo "      snarkos_network: $network_name"
  done
} >"$prometheus_yml"

# Ask the user if they want to run 'cargo install --locked --path .' or use a pre-installed binary
read -r -p "Do you want to run 'cargo install --locked --path .' to build the binary? (y/n, default: y): " build_binary
build_binary=${build_binary:-y}

# Ask the user whether to clear the existing ledger history
read -r -p "Do you want to clear the existing ledger history? (y/n, default: y): " clear_ledger
clear_ledger=${clear_ledger:-y}

# Log verbosity is set to 1 (DEBUG) by default.
verbosity=1

# Binary path set to "" by default (using installed binary) 
binary_path=""

if [[ $build_binary == "y" ]]; then
  # Ask the user for additional crate features (comma-separated)
  read -r -p "Enter crate features to enable (comma separated, default: test_network): " crate_features
  crate_features=${crate_features:-test_network}

  # Build command
  build_cmd=(cargo install --locked --path "$repo_root")

  # Add any extra features if provided
  if [[ -n $crate_features ]]; then
    build_cmd+=(--features "$crate_features")
  fi

  # Build command
  echo "Running build command: \"${build_cmd[*]}\""
  "${build_cmd[@]}" || exit 1
else
  # Ask the user whether to use a custom relative path
  read -r -p "Do you want to run snarkos from a relative path? (e.g. ./target/debug/, defaults to the installed binary): " binary_path
  binary_path=${binary_path:-""}
fi

# Clear the ledger logs for each validator if the user chooses to clear ledger
if [[ $clear_ledger == "y" ]]; then
  # Create an array to store background processes
  clean_processes=()

  for ((index = 0; index < $((total_validators + total_clients)); index++)); do
    # Run 'snarkos clean' for each node in the background
    "${binary_path}snarkos" clean "--network=$network_id" "--dev=$index" &

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
if ! tmux new-session -d -s "devnet" -n "validator-0"; then
  echo "Failed to create new TMUX session."
  exit 1
fi

# Get the tmux's base-index for windows
# we have to create all windows with index offset by this much
index_offset="$(tmux show-option -gv base-index)"
if [ -z "$index_offset" ]; then
  index_offset=0
fi

# Generate validator indices from 0 to (total_validators - 1)
# (mapfile would be cleaner but is unavailable on the bash 3.2 shipped with macOS)
# shellcheck disable=SC2207
validator_indices=($(seq 0 $((total_validators - 1))))

# Loop through the list of validator indices and create a new window for each
for validator_index in "${validator_indices[@]}"; do
  # Generate a unique and incrementing log file name based on the validator indexi
  name="validator-$validator_index"
  log_file="$log_dir/$name.log"
  window_index=$((validator_index + index_offset))
  metrics_port=$((validator_index + 9000))

  if [ "$validator_index" -ne 0 ]; then
    # We don't need to create a window for the first validator because the tmux session already starts with one window.
    tmux new-window -t "devnet:$window_index" -n "$name"
  fi

  # Send the command to start the validator to the new window and capture output to the log file
  tmux send-keys -t "devnet:$window_index" "${binary_path}snarkos start --dev-num-clients $total_clients --nodisplay --network $network_id --dev $validator_index --dev-num-validators $total_validators --validator --logfile $log_file --verbosity $verbosity --metrics --metrics-ip=0.0.0.0:$metrics_port$dev_txs_flag" C-m
done

if [ "$total_clients" -ne 0 ]; then
  # Generate client indices from 0 to (total_clients - 1)
  # shellcheck disable=SC2207
  client_indices=($(seq 0 $((total_clients - 1))))

  # Loop through the list of client indices and create a new window for each
  for client_index in "${client_indices[@]}"; do
    # Generate a unique and incrementing log file name based on the client index
    name="client-$client_index"
    log_file="$log_dir/$name.log"

    window_index=$((client_index + total_validators + index_offset))

    # Create a new window with a unique name
    tmux new-window -t "devnet:$window_index" -n "$name"

    # Send the command to start the client to the new window and capture output to the log file
    tmux send-keys -t "devnet:$window_index" "${binary_path}snarkos start --nodisplay --network $network_id --dev $window_index --dev-num-validators $total_validators --client --logfile $log_file  --verbosity $verbosity" C-m
  done
fi

# Bring up the Prometheus/Grafana stack and open the dashboard, if Docker is available.
# This must never abort the devnet itself, so failures here only print a warning.
compose_cmd=""
if docker compose version >/dev/null 2>&1; then
  compose_cmd="docker compose"
elif command -v docker-compose >/dev/null 2>&1; then
  compose_cmd="docker-compose"
fi

if [ -n "$compose_cmd" ]; then
  metrics_compose_file="$repo_root/node/metrics/docker-compose.yml"
  # Compose won't restart an already-running container just because the
  # bind-mounted prometheus.yml changed underneath it, so force a restart to
  # pick up the scrape targets regenerated above. If either step fails, skip
  # opening the dashboard, since it could be backed by stale scrape targets.
  if ! $compose_cmd -f "$metrics_compose_file" up --detach; then
    echo "Warning: failed to start the Prometheus/Grafana stack; skipping dashboard setup. See node/metrics/README.md to start it manually."
  elif ! $compose_cmd -f "$metrics_compose_file" restart prometheus; then
    echo "Warning: failed to restart Prometheus with the new scrape targets; skipping dashboard setup. See node/metrics/README.md to start it manually."
  else
    grafana_url="http://localhost:3000/d/snarkos"
    grafana_ready=""
    for _ in $(seq 1 30); do
      if curl -sf http://localhost:3000/api/health >/dev/null 2>&1; then
        grafana_ready="1"
        break
      fi
      sleep 1
    done

    if [ -n "$grafana_ready" ]; then
      opener=""
      if command -v xdg-open >/dev/null 2>&1; then
        opener="xdg-open"
      elif command -v open >/dev/null 2>&1; then
        opener="open"
      fi

      if [ -n "$opener" ]; then
        "$opener" "$grafana_url" >/dev/null 2>&1 &
      else
        echo "Grafana is up at $grafana_url (no xdg-open/open found to launch a browser automatically)."
      fi
    else
      echo "Grafana didn't become ready in time; check it manually at $grafana_url."
    fi
  fi
else
  echo "Docker not found; skipping automatic Prometheus/Grafana setup. See node/metrics/README.md to start it manually."
fi

# Attach to the tmux session to view and interact with the windows
tmux attach-session -t "devnet"
