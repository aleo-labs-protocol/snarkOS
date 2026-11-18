#!/usr/bin/env bash
# Check out the latest snarkOS release for a network.
# USAGE: ./scripts/checkout-latest-release.sh <mainnet|testnet>
set -euo pipefail

repo=aleo-labs-protocol/snarkOS
network=${1:-}
case "$network" in
  mainnet | testnet) ;;
  *) echo "usage: $0 <mainnet|testnet>" >&2; exit 1 ;;
esac

# mainnet runs the highest vX.Y.Z. testnet runs the highest vX.Y.Z-rc.N, until
# the same commit is tagged vX.Y.Z.
tag=$(curl -fsSL "https://api.github.com/repos/$repo/releases?per_page=100" | jq -r --arg network "$network" '
  [ .[] | select(.draft | not) | .tag_name as $tag
    | ($tag | capture("^v(?<major>[0-9]+)\\.(?<minor>[0-9]+)\\.(?<patch>[0-9]+)(-rc\\.(?<rc>[0-9]+))?$"))
    | select(if $network == "mainnet" then .rc == null else true end)
    | [(.major, .minor, .patch, .rc // "999999" | tonumber), $tag] ]
  | max | .[4] // empty')
[ -n "$tag" ] || { echo "error: no $network release found" >&2; exit 1; }

cd "$(dirname "$0")"
# Without --force, git refuses to replace a tag that was moved after it was fetched.
git fetch --no-tags "https://github.com/$repo.git" "refs/tags/$tag:refs/tags/$tag"
git checkout --detach "refs/tags/$tag"
echo "Checked out $tag ($(git rev-parse --short HEAD))"
