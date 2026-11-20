#!/usr/bin/env bash

# USAGE examples: 
  # CLI with env vars: PEERS=core_client_ip_1:4130,core_client_ip_2:4130,core_client_ip_3:4130,outer_client_ip_1:4130,... ./run-outer-client.sh
  # CLI with prompts for vars:  ./run-outer-client.sh

# If the env var PEERS is not set, prompt for it
if [ -z "${PEERS}" ]
then
  read -r -p "Enter the peers (comma-separated) (e.g., core_client_ip_1:4130,core_client_ip_2:4130,core_client_ip_3:4130,outer_client_ip_1:4130,...): "
  PEERS=$REPLY
fi

if [ -z "${PEERS}" ]; then
  COMMAND="cargo run --release -- start --nodisplay --client --node 0.0.0.0:4130 --verbosity 1 --rest 0.0.0.0:3030"
else
  COMMAND="cargo run --release -- start --nodisplay --client --node 0.0.0.0:4130 --peers ${PEERS} --verbosity 1 --rest 0.0.0.0:3030"
fi

for word in "$@";
do
  COMMAND="${COMMAND} ${word}"
done

function exit_node()
{
    echo "Exiting..."
    kill $!
    exit
}

trap exit_node SIGINT

echo "Running an Aleo Outer Client node..."
$COMMAND &
wait
