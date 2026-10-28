# snarkos-ledger-distribution

This is a simple tool that fetches chunks of 50 blocks from a node and uploads them to an S3-compatible bucket with a checksum file.
The bucket is the block CDN that `snarkos-node-cdn` syncs from.

The binary is named `ledger-distribution`; it was previously named `fastsync`.

## Setup

Create a `.env` file that contains the following variables:
```dotenv
BUCKETS_KEY=
BUCKETS_SECRET=
BUCKET_NAME=
EXPLORER_URL=
```

## Usage guide

```
cargo run --release -p snarkos-ledger-distribution --bin ledger-distribution -- <network ID> [--dry-run]
```

The network ID is `0` for mainnet, `1` for testnet, and `2` for canary.

## Runtime configuration

The binary reads its credentials and its targets from the environment:

| Variable | Required | Default | Purpose |
|---|---|---|---|
| `BUCKETS_KEY` | yes | — | HMAC access id |
| `BUCKETS_SECRET` | yes | — | HMAC secret |
| `BUCKET_NAME` | yes | — | Target bucket |
| `EXPLORER_URL` | yes | — | Base URL of the node REST API that blocks are fetched from |
| `BLOCKS_ENDPOINT` | no | `storage.googleapis.com` | S3-compatible endpoint |
| `BLOCKS_REGION` | no | `auto` | Bucket region |
| `GENESIS_PATH` | no | the network's genesis block | File holding the genesis block (as written by `snarkos start --dev`) that an empty bucket must start with |

There is also a `GET /health` endpoint on port `3123` that returns `200` without
touching the bucket.

## Procedure

1. Checks the latest block height of the node and of the stored blocks in the bucket.
    - If the node is ahead by more than 50 blocks, then continue.
2. Fetch 50 blocks at a time from the node at `EXPLORER_URL`.
3. Upload the 50 blocks and their checksum to the bucket.
    - e.g. 0.50.blocks and 0.50.blocks.sum
    - Checksum is sha256(block_bytes)
4. Fetch the newly uploaded blocks and checksum from the bucket and check that they are equivalent to the node's blocks.

When the bucket has caught up to the node, it checks every 30 seconds whether it needs to upload more block chunks.
