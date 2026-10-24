#!/bin/bash

####################################################
# Runs a devnet in which one validator lacks the next consensus version.
# Checks that the outdated validator warns as soon as it connects, keeps building blocks up to the
# activation height, and then exits with an error and without forking. Checks the same exit on
# every restart, with or without its record of the required upgrade. Then checks that it rejoins
# the network once upgraded, and that it discards a record no validator confirms.
####################################################

set -eo pipefail

network_id=0
total_validators=4
activation_height=150
# The outdated validator builds no block from REQUIRED_UPGRADE_SAFETY_MARGIN blocks before the activation.
safety_margin=100
stop_height=$((activation_height - safety_margin))
target_height=180
# The outdated validator is the last one, so that the upgraded ones form a contiguous range.
outdated_validator=$((total_validators-1))
NODE_VERBOSITY=2

# shellcheck source=SCRIPTDIR/utils.sh
. ./.ci/utils.sh

network_name=$(get_network_name "$network_id")

# Logs a failure and exits.
function fail() {
  log "⛔️ $*"
  exit 1
}
init_log_dir

trap stop_nodes EXIT

# The heights of V1..V21 match the test defaults; only V22 differs.
base_heights="0,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24"
upgraded_heights="$base_heights,$activation_height"
outdated_heights="$base_heights,4294967295"

common_flags=(
  --nodisplay --nobanner --noupdater "--network=$network_id" "--verbosity=$NODE_VERBOSITY"
  "--dev-num-validators=$total_validators" "--dev-num-clients=0"
)

upgraded_port=3030
outdated_port=$((3030+outdated_validator))
outdated_metrics_port=9000
record_file=".node-data-$network_id-$outdated_validator/required-consensus-upgrade"
# The activation height and ConsensusVersion::V22, in the little-endian encoding of the record.
expected_record="$(printf '%08x' "$activation_height" | sed -E 's/(..)(..)(..)(..)/\4\3\2\1/')1600"

# Starts validator $1 with the consensus version heights $2, logging to $3.
function start_validator() {
  local index=$1
  local heights=$2
  local log_file=$3
  local metrics_flags=()
  if (( index == outdated_validator )); then
    metrics_flags=(--metrics "--metrics-ip=$localhost:$outdated_metrics_port")
  fi
  CONSENSUS_VERSION_HEIGHTS=$heights run_with_prefix "validator-$index" snarkos start "${common_flags[@]}" \
    "--dev=$index" --validator "--logfile=$log_file" "--rest=$localhost:$((3030+index))" --no-dev-txs \
    "${metrics_flags[@]}"
  PIDS[index]=$!
}

# Prints the hash of the block at height $2 on the node with the REST port $1, or nothing.
function block_hash() {
  curl -s --max-time 5 "http://$localhost:$1/v2/$network_name/block/$2" | jq -r '.block_hash // empty' 2>/dev/null || true
}

# Fails unless the upgraded validators hold the block with hash $2 at height $1.
function check_no_fork() {
  local height=$1
  local outdated_hash=$2
  local upgraded_hash
  upgraded_hash=$(block_hash "$upgraded_port" "$height")
  if [[ -z "$outdated_hash" || "$outdated_hash" != "$upgraded_hash" ]]; then
    fail "At height $height, the outdated validator holds '$outdated_hash' and the upgraded ones '$upgraded_hash'"
  fi
}

# Waits for the outdated validator to exit, and checks that it exited with an error, recorded the
# required upgrade, and holds no block the upgraded validators lack. $1 names the run, $2 is its log
# file, and $3, if set, is the lowest height it must have reached; it must not exceed the activation
# height. Sets `warned_height` to its height when it first warned of the upgrade, if it did, and
# `reported_upgrade` if its metrics reported that it needs an upgrade.
function expect_outdated_exit() {
  local run=$1
  local log_file=$2
  local min_height=$3
  local pid=${PIDS[outdated_validator]}
  local height last_height="" last_hash="" hash start
  warned_height=""
  reported_upgrade=""
  start=$(now)

  while kill -0 "$pid" 2>/dev/null; do
    if (( $(elapsed_since "$start") > 600 )); then
      fail "The outdated validator did not exit ($run)"
    fi
    height=$(get_block_height_by_port "$outdated_port" "$network_name" 5)
    if [[ -n "$height" ]]; then
      hash=$(block_hash "$outdated_port" "$height")
      if [[ -n "$hash" ]]; then
        last_height=$height
        last_hash=$hash
      fi
      if [[ -z "$warned_height" ]] && grep -q "This node stops before building block" "$log_file"; then
        warned_height=$height
      fi
      if [[ -n "$warned_height" && -z "$reported_upgrade" ]]; then
        # Read the metrics first: with `pipefail`, `grep -q` exiting early would fail `curl`.
        metrics=$(curl -s --max-time 5 "http://$localhost:$outdated_metrics_port/metrics" || true)
        if grep -q "^snarkos_consensus_needs_upgrade 1$" <<< "$metrics"; then
          reported_upgrade=1
        fi
      fi
    fi
    sleep 1
  done

  local status=0
  wait "$pid" || status=$?
  log "The outdated validator exited with status $status at height '$last_height' ($run)"
  if (( status == 0 )); then
    fail "The outdated validator exited without an error ($run)"
  fi
  local record
  record=$(od -An -tx1 "$record_file" 2>/dev/null | tr -d ' \n' || true)
  if [[ "$record" != "$expected_record" ]]; then
    fail "The outdated validator recorded '$record' instead of '$expected_record' ($run)"
  fi
  if ! grep -q "must be upgraded" "$log_file"; then
    fail "The outdated validator did not log that it must be upgraded ($run)"
  fi
  if [[ -z "$last_height" ]]; then
    fail "The outdated validator never reported its height ($run)"
  fi
  if (( last_height >= stop_height )); then
    fail "The outdated validator reached height $last_height, at or past the stop height $stop_height ($run)"
  fi
  if [[ -n "$min_height" ]] && (( last_height < min_height )); then
    fail "The outdated validator stopped at height $last_height, before height $min_height ($run)"
  fi
  check_no_fork "$last_height" "$last_hash"
}

# Starts the outdated validator with the upgraded schedule, logging to $2, and checks that it catches
# up without forking and removes the record. $1 names the run.
function expect_rejoin() {
  local run=$1
  local log_file=$2
  # Not `height`, which `check_heights` in utils.sh assigns without declaring it local.
  local rejoin_height
  start_validator "$outdated_validator" "$upgraded_heights" "$log_file"
  rejoin_height=$(( $(get_block_height_by_port "$upgraded_port" "$network_name" 5) + 10 ))
  if ! wait_for_heights "$outdated_validator" "$total_validators" "$rejoin_height" "$network_name" 900 5; then
    fail "The validator did not reach height $rejoin_height ($run)"
  fi
  if [[ -f "$record_file" ]]; then
    fail "The validator kept the record ($run)"
  fi
  check_no_fork "$rejoin_height" "$(block_hash "$outdated_port" "$rejoin_height")"
}

for validator_index in $(seq 0 $((total_validators-1))); do
  snarkos clean "--dev=$validator_index" "--network=$network_id"
  if (( validator_index == outdated_validator )); then
    heights=$outdated_heights
  else
    heights=$upgraded_heights
  fi
  start_validator "$validator_index" "$heights" "$log_dir/validator-$validator_index.log"
  sleep 1
done

wait_for_nodes "$total_validators" 0 "$network_name" 180

# The outdated validator warns as soon as it connects, and builds blocks until the activation height.
expect_outdated_exit "first run" "$log_dir/validator-$outdated_validator.log" $((stop_height - 5))
if [[ -z "$warned_height" ]] || (( warned_height >= stop_height - 10 )); then
  fail "The outdated validator first warned at height '$warned_height', not well before $stop_height"
fi
log "The outdated validator first warned at height $warned_height"
if [[ -z "$reported_upgrade" ]]; then
  fail "The outdated validator did not report snarkos_consensus_needs_upgrade 1 before it exited"
fi
log "The outdated validator reported snarkos_consensus_needs_upgrade 1"

# The upgraded validators continue without it.
if ! wait_for_heights 0 "$outdated_validator" "$target_height" "$network_name" 1800 5; then
  fail "Upgraded validators did not reach height $target_height"
fi
log "Upgraded validators reached height $target_height"

# A restart with the same build and the record exits again.
log_file="$log_dir/validator-$outdated_validator-restart.log"
start_validator "$outdated_validator" "$outdated_heights" "$log_file"
expect_outdated_exit "restart with the record" "$log_file"
if ! grep -q "A previous run found" "$log_file"; then
  fail "The restarted validator did not read the record"
fi

# A start with the same build and no record exits too, as peers keep announcing the version they run.
rm "$record_file"
log_file="$log_dir/validator-$outdated_validator-fresh.log"
start_validator "$outdated_validator" "$outdated_heights" "$log_file"
expect_outdated_exit "start without the record" "$log_file"

# Once upgraded, the validator removes the record and rejoins.
expect_rejoin "upgraded" "$log_dir/validator-$outdated_validator-upgraded.log"

# A record of a version that no build schedules is cleared by a quorum of validators that are not ahead.
graceful_stop_pid "${PIDS[outdated_validator]}" "validator-$outdated_validator"
# Height 1000 and ConsensusVersion 65535, in the little-endian encoding of the record.
printf '\xe8\x03\x00\x00\xff\xff' > "$record_file"
log_file="$log_dir/validator-$outdated_validator-forged.log"
expect_rejoin "forged record" "$log_file"
if ! grep -q "A previous run found" "$log_file" || ! grep -q "Connected validators holding a quorum" "$log_file"; then
  fail "The validator did not read and then clear the forged record"
fi
if check_node_stopped; then
  exit 1
fi

log "✅ The outdated validator exited without forking on every run, rejoined once upgraded, and cleared a forged record"
