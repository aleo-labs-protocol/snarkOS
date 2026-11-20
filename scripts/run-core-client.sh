#!/usr/bin/env bash

# USAGE examples:
  # CLI with env vars: PEERS="validator_ip:4130,core_client_ip_1:4130,core_client_ip_2:4130,core_client_ip_3:4130,outer_client_ip_1:4130,..." ./run-core-client.sh
  # CLI with prompts for vars:  ./run-core-client.sh

# If the env var PEERS is not set, prompt for it
if [ -z "${PEERS}" ]
then
  read -r -p "Enter the peers (comma-separated) (e.g., \"validator_ip:4130,core_client_ip_1:4130,core_client_ip_2:4130,core_client_ip_3:4130,outer_client_ip_1:4130,...\"): "
  PEERS=$REPLY
fi

if [ "${PEERS}" == "" ]
then
  echo "Missing peers."
  exit 1
fi

COMMAND="cargo run --release -- start --nodisplay --client --node 0.0.0.0:4130 --peers ${PEERS} --verbosity 1 --norest"

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

echo "Running an Aleo Core Client node..."
$COMMAND &
wait
