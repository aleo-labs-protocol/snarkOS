#!/bin/bash

####################################################
# Runs and tests a development network
####################################################

set -eo pipefail # error on any command failure

# Uncomment this to print commands before executing them for easier debugging.
#set -x

# Set parameters directly
total_validators=$1
total_clients=$2
network_id=$3
min_height=$4

# The verbosity of snarkos nodes.
NODE_VERBOSITY=4
# Max logfile sizes (bytes) for regression checks.
MAX_VALIDATOR_LOG_SIZE_BYTES=$((2 * 1024 * 1024))
MAX_CLIENT_LOG_SIZE_BYTES=$((1 * 1024 * 1024))

# Default values if not provided
: "${total_validators:=4}"
: "${total_clients:=4}" # need at least 4 clients, so each validator has at least one client connected to it.
: "${network_id:=0}"
: "${min_height:=60}" # To likely go past the 100 round garbage collection limit.

tx_file="${TMPDIR:-/tmp}/transfer_public_tx.txt"

# shellcheck source=SCRIPTDIR/utils.sh
. ./.ci/utils.sh

# Determine network name based on network_id
network_name=$(get_network_name "$network_id")
echo "Using network: $network_name (ID: $network_id)"

# Create log directory
init_log_dir

# Define a trap handler that cleans up all processes on exit.
# shellcheck disable=SC2329
function exit_handler() {
  stop_nodes

  # Remove all temporary files and folders
  rm program/program.json program/main.aleo || true
  rm program/txn_data.json program/invalid_txn_data.json || true
  rm "$tx_file" || true
  rmdir program || true
}
trap exit_handler EXIT

# Define a trap handler that prints a message when an error occurs 
trap 'log "⛔️ Error in $BASH_SOURCE at line $LINENO: \"$BASH_COMMAND\" failed (exit $?)"' ERR

# Flags used by all nodes.
common_flags=(
  --nodisplay --nobanner --noupdater "--network=$network_id" "--verbosity=$NODE_VERBOSITY"
  "--dev-num-validators=$total_validators"  "--dev-num-clients=$total_clients"
)

# Start all validator nodes in the background
for validator_index in $(seq 0 $((total_validators-1))); do
  snarkos clean "--dev=$validator_index" "--network=$network_id"

  log_file="$log_dir/validator-$validator_index.log"
  if (( validator_index == 0 )); then
    run_with_prefix "validator-$validator_index" snarkos start "${common_flags[@]}" "--dev=$validator_index" \
      --validator "--logfile=$log_file" "--rest=127.0.0.1:$((3030+validator_index))" \
      --metrics --no-dev-txs
  else
    run_with_prefix "validator-$validator_index" snarkos start "${common_flags[@]}" "--dev=$validator_index" \
      --validator "--logfile=$log_file" "--rest=127.0.0.1:$((3030+validator_index))"
  fi
  PIDS[validator_index]=$!
  log "Started validator $validator_index with PID ${PIDS[$validator_index]}"

  # Add 1-second delay between starting nodes to avoid hitting rate limits
  sleep 1
done

# Start all client nodes in the background.
for client_index in $(seq 0 $((total_clients-1))); do
  # compute the absolute index for this node.
  node_index=$((client_index + total_validators))

  snarkos clean "--dev=$node_index" "--network=$network_id"

  log_file="$log_dir/client-$client_index.log"
  run_with_prefix "client-$client_index" snarkos start "${common_flags[@]}" "--dev=$node_index" \
    --client "--logfile=$log_file" "--rest=127.0.0.1:$((3030+node_index))"
  PIDS[node_index]=$!
  log "Started client $client_index with PID ${PIDS[$node_index]}"
  # Add 1-second delay between starting nodes to avoid hitting rate limits
  if (( client_index < total_clients-1)); then
    sleep 1
  fi
done

# Ensure all nodes are up and running.
# Wait up to two minutes, as this can take long in CI.
wait_for_nodes "$total_validators" "$total_clients" "$network_name" 180

# Wait for validators to be fully connected.
log "ℹ️ Waiting for validators to be fully connected..." 
for validator_index in $(seq 0 $((total_validators-1))); do
  if ! (wait_for_bft_connections "$validator_index" $((total_validators-1)) "$network_name"); then
    exit 1
  fi
done
log "✅ All validators are fully connected"

if (( total_clients > 0 )); then
  log "ℹ️ Waiting for clients to have at least one peer..."
  # Wait for all clients to be connected to another client or a validator.
  for client_index in $(seq 0 $((total_clients-1))); do
    node_index=$((client_index + total_validators))
    if ! (wait_for_peers "$node_index" 1 "$network_name"); then
      exit 1
    fi
  done
  log "✅ All clients have at least one peer"
fi

if ! wait_for_stable_consensus_version 0 "$network_name"; then
  echo "❌ Test failed! Consensus version did not stabilize within 5 minutes."
  exit 1
fi

# Broadcast a transfer_public built from a static query.
# The state root is the fixed dev genesis root (Field::one()), which a test_network
# node stores at height 0. The height is the chain height after the consensus
# version has stabilized, so proving uses that same consensus version.
# Devnet consensus passes ConsensusVersion::V10 before the version stabilizes.
# The transaction is generated against the stable consensus version.
log "● Testing static-query transfer_public against the dev genesis state root..."
txs_target_state_root="sr1qyqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqquwxeur"
txs_target_consensus_version_height=$(get_block_height 0 "$network_name")
txs_target_consensus_version=$(get_consensus_version 0 "$network_name")
query=$(printf '{"state_root": "%s",\n"height": %s}' \
  "$txs_target_state_root" "$txs_target_consensus_version_height")
log "Static query at consensus version ${txs_target_consensus_version}, height ${txs_target_consensus_version_height}"

recipient=$(curl -s "http://$localhost:3030/v2/$network_name/committee/latest" | jq -r '.members | keys[1]')
if [ -z "$recipient" ] || [ "$recipient" = "null" ]; then
  log "❌ Test failed! Could not retrieve a recipient address from the committee."
  exit 1
fi

snarkos developer execute \
  --dev-key 0 \
  --query "$query" \
  credits.aleo transfer_public "$recipient" 100u64 \
  --network "$network_id" \
  --store "$tx_file" \
  --store-format string

broadcast_body=$(mktemp)
broadcast_status=$(curl -s -w "%{http_code}" -o "$broadcast_body" -X POST \
  -H "Content-Type: application/json" \
  --data-binary @"$tx_file" \
  "http://$localhost:3030/v2/$network_name/transaction/broadcast?check_transaction=true")

if (( broadcast_status != 200 )); then
  log "❌ Test failed! Static-query transfer_public failed verification (HTTP ${broadcast_status}): $(<"$broadcast_body")"
  rm -f "$broadcast_body"
  exit 1
fi

log "✅ Static-query transfer_public verified: $(<"$broadcast_body")"
rm -f "$broadcast_body"

# Creates a test program.
mkdir -p program
program_name="test_program.aleo"
echo "program ${program_name};

function main:
    input r0 as u32.public;
    input r1 as u32.private;
    add r0 r1 into r2;
    output r2 as u32.private;

view compute_sum:
    add 1u32 2u32 into r0;
    output r0 as u32.public;

constructor:
    assert.eq true true;
" > program/main.aleo

echo "{
  \"program\": \"${program_name}\",
  \"version\": \"0.1.0\",
  \"description\": \"\",
  \"license\": \"\",
  \"dependencies\": null,
  \"editions\": {}
}
" > program/program.json

# Deploy the test program and wait for the deployment to be processed.
log "● Testing program deployment..."
_deploy_result=$(cd program && snarkos developer deploy --dev-key 0 --network "$network_id" --endpoint=localhost:3030 --broadcast --wait --timeout 20 "$program_name")

# Ensure we are able to fetch the program from the node.
status_code=$(curl -s -o /dev/null -w "%{http_code}" "http://localhost:3030/v2/$network_name/program/${program_name}/0")
if (( status_code == 200 )); then
  log "✅ Program exists on the node"
else
  log "❌ Test failed! Failed to get program. Code was ${status_code}"
  exit 1
fi

# Ensure the latest edition is indeed 0.
log "● Testing retrieval of program editions..."
edition=$(curl -s -o /dev/null "http://localhost:3030/v2/$network_name/program/${program_name}/latest_edition")
if (( edition != 0 )); then
  log "❌ Test failed! Invalid latest edition {} for test program returned, not 0."
  exit 1
fi

# Also check that the latest edition for the default program (credits.aleo) is 0.
edition=$(curl -s -o /dev/null "http://localhost:3030/v2/$network_name/program/credits.aleo/latest_edition")
if (( edition != 0 )); then
  log "❌ Test failed! Invalid latest edition {} for credits.aleo returned, not 0."
  exit 1
fi

# Finally, check that we cannot fetch a non-existing edition of a program
status_code=$(curl -s -o /dev/null -w "%{http_code}" "http://localhost:3030/v2/$network_name/program/${program_name}/1")
if (( status_code == 404 )); then
  log "✅ Only program edition 0 exists on the node"
else
  log "❌ Test failed! Invalid edition returnd ${status_code}, not 404."
  exit 1
fi

# Query the view function at the latest height.
log "● Testing view function evaluation at latest height..."
view_response=$(curl -s -X POST \
  -H "Content-Type: application/json" \
  -d '[]' \
  "http://localhost:3030/v2/$network_name/program/${program_name}/view/compute_sum")
view_output=$(jq -r '.[0]' <<< "$view_response")
if [ "$view_output" = "3u32" ]; then
  log "✅ View function returned expected output at latest height: $view_output"
else
  log "❌ Test failed! View function returned unexpected output at latest height: $view_response"
  exit 1
fi

# Query the view function at a specific block height.
# Requires history feature.
# log "● Testing view function evaluation at specific block height..."
# current_height=$(curl -s "http://localhost:3030/v2/$network_name/block/height/latest")
# view_response=$(curl -s -X POST \
#   -H "Content-Type: application/json" \
#   -d '[]' \
#   "http://localhost:3030/v2/$network_name/program/${program_name}/view/compute_sum/${current_height}")
# view_output=$(jq -r '.[0]' <<< "$view_response")
# if [ "$view_output" = "3u32" ]; then
#   log "✅ View function returned expected output at height ${current_height}: $view_output"
# else
#   log "❌ Test failed! View function returned unexpected output at height ${current_height}: $view_response"
#   exit 1
# fi

# Execute a function in the deployed program and wait for the execution to be processed.
log "● Testing program execution with V2 API..."
execute_result=$(cd program && snarkos developer execute --dev-key 0 --network "$network_id" --broadcast --endpoint=http://localhost:3030 \
    "$program_name" main 1u32 1u32 --wait --timeout 10)

# Fail if the execution transaction does not exist.
tx=$(echo "$execute_result" | tail -n 1)
found=$(curl -s -o /dev/null -w "%{http_code}" "http://localhost:3030/v2/$network_name/transaction/$tx")
# Fail if the HTTP response is not 2XX.
if (( found < 200 || found >= 300 )); then
  printf "❌ Test failed! Transaction does not exist or contains an error: \nexecute_result: %s\nfound: %s\n" \
    "$execute_result" "$found"
  exit 1
else
  log "✅ Transaction executed successfully: $execute_result"
fi

# Use the old flags here `--query` and `--broadcast=URL` to test they still work.
# Also, use the v1 API to test it still works.
log "● Testing program execution with V1 API..."
execute_result=$(cd program && snarkos developer execute --dev-key 0 --network "$network_id" "--query=http://$localhost:3030/v1" \
    "--broadcast=http://$localhost:3030/v1/$network_name/transaction/broadcast" "$program_name" main 1u32 1u32 --wait --timeout 10)

# Fail if the execution transaction does not exist.
tx=$(echo "$execute_result" | tail -n 1)
found=$(curl -s -o /dev/null -w "%{http_code}" "http://$localhost:3030/v1/$network_name/transaction/$tx")
# Fail if the HTTP response is not 2X.
if (( found < 200 || found >= 300 )); then
  printf "❌ Test failed! Transaction does not exist or contains an error: \nexecute_result: %s\nfound: %s\n" \
    "$execute_result" "$found"
  exit 1
else
  log "✅ Transaction executed successfully: $execute_result"
fi

# Fail if status does not exist or is not set to "accepted".
log "● Testing confirmed transaction endpoint..."
rest_confirmed=$(curl -s "http://$localhost:3030/v2/$network_name/transaction/confirmed/$tx")

rest_status=$(jq --raw-output '.status' <<< "$rest_confirmed")
if [ "$rest_status" != "accepted" ]; then
  printf "❌ Test failed! Rest API did not mark the transaction as \"accepted\". Status was: \"%s\" \nFull JSON: %s\n" "$rest_status" "$rest_confirmed"
  exit 1
fi

log "✅ Confirmed transaction endpoint returned status \"accepted\""

# Broadcast a transfer_public with an impossibly large amount so the transaction is rejected on finalize.
log "● Testing transaction rejection reason endpoint..."
recipient=$(curl -s "http://$localhost:3030/v2/$network_name/committee/latest" | jq -r '.members | keys[1]')
if [ -z "$recipient" ] || [ "$recipient" = "null" ]; then
  log "❌ Test failed! Could not retrieve a recipient address from the committee."
  exit 1
fi

rejected_execute_result=$(snarkos developer execute --dev-key 0 --network "$network_id" \
  --broadcast --endpoint="http://$localhost:3030" --wait --timeout 60 \
  credits.aleo transfer_public "$recipient" 18446744073709551615u64)
rejected_tx=$(echo "$rejected_execute_result" | tail -n 1)

rest_rejected=$(curl -s "http://$localhost:3030/v2/$network_name/transaction/confirmed/$rejected_tx")
rest_status=$(jq --raw-output '.status' <<< "$rest_rejected")
if [ "$rest_status" != "rejected" ]; then
  printf "❌ Test failed! Expected transaction to be rejected, but status was: \"%s\"\nFull JSON: %s\n" \
    "$rest_status" "$rest_rejected"
  exit 1
fi

rejection_reason=$(curl -s "http://$localhost:3030/v2/$network_name/transaction/rejected/$rejected_tx/reason")
rejection_reason_status=$(curl -s -o /dev/null -w "%{http_code}" \
  "http://$localhost:3030/v2/$network_name/transaction/rejected/$rejected_tx/reason")
if (( rejection_reason_status != 200 )); then
  log "❌ Test failed! Rejection reason endpoint returned $rejection_reason_status instead of 200: $rejection_reason"
  exit 1
fi

rejection_type=$(jq --raw-output '.type' <<< "$rejection_reason")
rejection_program=$(jq --raw-output '.program_id' <<< "$rejection_reason")
rejection_resource=$(jq --raw-output '.resource' <<< "$rejection_reason")
if [ "$rejection_type" != "finalize" ] || [ "$rejection_program" != "credits.aleo" ] || [ "$rejection_resource" != "transfer_public" ]; then
  log "❌ Test failed! Unexpected rejection reason: $rejection_reason"
  exit 1
fi

log "✅ Rejection reason retrieved successfully for rejected transfer_public transaction"

log "ℹ️Testing REST API and REST Error Handling"

# Test invalid transaction data (JsonDataError) returns 422 Unprocessable Content
log "● Testing invalid transaction data returns 422 status code..."
(cd program && snarkos developer execute --dev-key 0 --network "$network_id" \
  "--endpoint=$localhost:3030"  --store txn_data.json --store-format=string \
  "$program_name" main 1u32 1u32)

# Modify the proof data
# This changes the last three characters in the hash but keeps the correct length.
# `printf %s` avoids a newline at the end.
(cd program && printf %s "$(jq -c '.id = (.id[0:-3] + "qpz")' txn_data.json)" > invalid_txn_data.json)

invalid_tx_status=$(curl -s -w "%{http_code}" -X POST \
  -H "Content-Type: application/json" \
  -d "$(< ./program/invalid_txn_data.json)" \
  "http://$localhost:3030/v2/$network_name/transaction/broadcast" \
  -o /dev/null)

if (( invalid_tx_status == 422 )); then
  log "✅ Invalid transaction correctly returned 422 Unprocessable Content"
else
  log "❌ Test failed! Invalid transaction returned $invalid_tx_status instead of 422"
  exit 1
fi

# Test that the returned error is valid JSON
json_error=$(curl -s -X POST \
  -H "Content-Type: application/json" \
  -d "$(< ./program/invalid_txn_data.json)" \
  "http://$localhost:3030/v2/$network_name/transaction/broadcast")

# Ensure the top-level error message is "Invalid transaction"
if ! jq -e '.message | test("Invalid transaction")' <<< "$json_error" > /dev/null ; then 
  log "❌ Test failed! Invalid JSON returned: \"$json_error\""
  exit 1
fi

log "✅ Invalid transaction return valid JSON error"

# Test malformed JSON syntax (JsonSyntaxError) returns 400 Bad Request
malformed_json_response=$(curl -s -w "%{http_code}" -X POST \
  -H "Content-Type: application/json" \
  -d '{"malformed": json}' \
  "http://$localhost:3030/v2/$network_name/transaction/broadcast" \
  -o /dev/null)

if (( malformed_json_response == 400 )); then
  log "✅ Malformed JSON correctly returned 400 Bad Request"
else
  echo "❌ Test failed! Malformed JSON returned $malformed_json_response instead of 400"
  exit 1
fi

# Test that malformed JSON returns a properly formatted RestError
malformed_json_error=$(curl -s -X POST \
  -H "Content-Type: application/json" \
  -d '{"malformed": json}' \
  "http://$localhost:3030/v2/$network_name/transaction/broadcast")

# Verify the message contains JSON-related error text
if ! jq -e '.message | test("Invalid JSON")' <<< "$malformed_json_error" > /dev/null; then
  log "❌ Test failed! Malformed JSON response message doesn't contain expected JSON error text: \"$malformed_json_error\""
  exit 1
fi

log "✅ Malformed JSON returns properly formatted RestError with JSON syntax error message"

# Test invalid Content-Type header returns 400 Bad Request
log "● Testing missing Content-Type header returns 400 status code..."
missing_content_type_response=$(curl -s -w "%{http_code}" -X POST \
  -d '{"valid": "json"}' \
  "http://$localhost:3030/v2/$network_name/transaction/broadcast" \
  -o /dev/null)

if (( missing_content_type_response == 400 )); then
  log "✅ Missing Content-Type correctly returned 400 Bad Request"
else
  log "❌ Test failed! Missing Content-Type returned $missing_content_type_response instead of 400"
  exit 1
fi

# Test that missing Content-Type returns a properly formatted RestError
log "● Testing missing Content-Type returns valid RestError format..."

missing_content_type_error=$(curl -s -X POST \
  -d '{"valid": "json"}' \
  "http://$localhost:3030/v2/$network_name/transaction/broadcast")

# Verify the response is valid JSON
if ! jq . <<< "$missing_content_type_error" > /dev/null 2>&1; then
  log "❌ Test failed! Missing Content-Type response is not valid JSON: \"$missing_content_type_error\""
  exit 1
fi

# Verify the message contains Content-Type related error text
if ! jq -e '.message | test("Content-Type|application/json")' <<< "$missing_content_type_error" > /dev/null; then
  log "❌ Test failed! Missing Content-Type response message doesn't contain expected error text: \"$missing_content_type_error\""
  exit 1
fi

log "✅ Missing Content-Type returns properly formatted RestError with Content-Type error message"

# Scan the network for records.
log "● Testing \`snarkos developer scan\`..."

scan_result=$(snarkos developer scan --dev-key 0 --network "$network_id" --start 0 "--endpoint=$localhost:3030")
num_records=$(echo "$scan_result" | grep -c "owner")
# Fail if the scan did not return 4 records.
if (( num_records != 4 )); then
  log "❌ Test failed! Expected 4 records, but found $num_records: $scan_result"
  exit 1
else
  log "✅ Scan returned 4 records correctly: $scan_result"
fi

log "ℹ️Testing network progress"

# Check heights periodically with a timeout
if wait_for_heights 0 $((total_validators+total_clients)) "$min_height" "$network_name" 600; then
  log "🎉 Test passed! All nodes reached minimum height."
else
  log "❌ Test failed! Not all nodes reached minimum height within 10 minutes."
  print_validator_logs "$log_dir" "$total_validators" "$total_clients"
  print_client_logs "$log_dir" "$total_validators" "$total_clients"
  exit 1
fi

# Ensure no errors are generated during the devnet run, as all nodes are
# expected to operate without failures or interruptions.
if check_logs "$log_dir" "$total_validators" "$total_clients" "$MAX_VALIDATOR_LOG_SIZE_BYTES" "$MAX_CLIENT_LOG_SIZE_BYTES"; then
  exit 0
else
  exit 1
fi
