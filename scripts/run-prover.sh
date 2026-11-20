#!/usr/bin/env bash
# USAGE examples:
  # CLI with env vars: PROVER_PRIVATE_KEY=APrivateKey1...  ./run-prover.sh
  # CLI with prompts for vars:  ./run-prover.sh

# If the env var PROVER_PRIVATE_KEY is not set, prompt for it
if [ -z "${PROVER_PRIVATE_KEY}" ]
then
  read -r -p "Enter the Aleo Prover account private key: "
  PROVER_PRIVATE_KEY=$REPLY
fi

if [ "${PROVER_PRIVATE_KEY}" == "" ]
then
  echo "Missing account private key. (run 'snarkos account new' and try again)"
  exit
fi

# Collect arguments
for word in "$@"; do
  ARGS+=("$word")
done

# Build the command
COMMAND="cargo run --release -- start --nodisplay --prover --private-key ${PROVER_PRIVATE_KEY}"

# Append the arguments
for arg in "${ARGS[@]}"; do
  COMMAND="${COMMAND} ${arg}"
done

function exit_node()
{
    echo "Exiting..."
    kill $!
    exit
}

trap exit_node SIGINT

echo "Running an Aleo Prover node..."
$COMMAND &
wait
