# snarkos-ledger-distribution

This is a simple tool that fetches chunks of 50 blocks from a node and uploads them to a Google Cloud Storage or S3-compatible bucket with a checksum file.
The bucket is the block CDN that `snarkos-node-cdn` syncs from.

The binary is named `ledger-distribution`; it was previously named `fastsync`.

## Setup

Create a `.env` file that contains the following variables:
```dotenv
BUCKET_URL=
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
| `BUCKET_URL` | yes | — | Target bucket: `gs://<bucket>` for Google Cloud Storage, or `s3://<bucket>` for an S3-compatible service. Objects go under `v0/` at the bucket's root; a path in the URL is rejected |
| `EXPLORER_URL` | yes | — | Base URL of the node REST API that blocks are fetched from |
| `GENESIS_PATH` | no | the network's genesis block | File holding the genesis block (as written by `snarkos start --dev`) that an empty bucket must start with |

The bucket's credentials and other settings come from the environment variables that the
[`object_store`](https://docs.rs/object_store) builders read:

- **`gs://`**: application default credentials, which on GKE include Workload Identity, or a
  service account key in `GOOGLE_SERVICE_ACCOUNT` or `GOOGLE_APPLICATION_CREDENTIALS`.
- **`s3://`**: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION` and `AWS_ENDPOINT`.
  To use Google Cloud Storage's S3-compatible API with HMAC keys, set
  `AWS_ENDPOINT=https://storage.googleapis.com` and `AWS_REGION=auto`.

Uploads set no per-object ACL. The bucket itself must grant public read access, for example
with uniform bucket-level access and `roles/storage.objectViewer` for `allUsers`, since nodes
download from it without credentials.

There is also a `GET /health` endpoint on port `3123` that returns `200` without
touching the bucket.

## Testing

```
cargo test -p snarkos-ledger-distribution
```

The test uploads a generated ledger from a mock node to an in-process S3 server, then loads it
back with `snarkos-node-cdn`, the client snarkOS nodes sync from the CDN with.

## Procedure

1. Checks the latest block height of the node and of the stored blocks in the bucket.
    - If the node is ahead by more than 50 blocks, then continue.
2. Fetch 50 blocks at a time from the node at `EXPLORER_URL`.
3. Upload the 50 blocks and their checksum to the bucket.
    - e.g. 0.50.blocks and 0.50.blocks.sum
    - Checksum is sha256(block_bytes)
4. Fetch the newly uploaded blocks and checksum from the bucket and check that they are equivalent to the node's blocks.

When the bucket has caught up to the node, it checks every 30 seconds whether it needs to upload more block chunks.
