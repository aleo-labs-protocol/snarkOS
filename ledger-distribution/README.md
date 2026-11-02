# snarkos-ledger-distribution

This is a simple tool that reads chunks of 50 blocks from a node's ledger database and uploads them to a Google Cloud Storage or S3-compatible bucket with a checksum file.
The bucket is the block CDN that `snarkos-node-cdn` syncs from.

The binary is named `ledger-distribution`; it was previously named `fastsync`.

## Setup

Create a `.env` file that contains the following variables:
```dotenv
BUCKET_URL=
LEDGER_PATH=
LEDGER_SECONDARY_PATH=
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
| `LEDGER_PATH` | yes | — | The node's ledger directory, e.g. `~/.aleo/storage/ledger-0` |
| `LEDGER_SECONDARY_PATH` | yes | — | A directory of its own, where RocksDB keeps the files of the read-only instance |
| `GENESIS_PATH` | no | the network's genesis block | File holding the genesis block (as written by `snarkos start --dev`) that an empty bucket must start with |

The bucket's credentials and other settings come from the environment variables that the
[`object_store`](https://docs.rs/object_store) builders read:

- **`gs://`**: application default credentials, which on GKE include Workload Identity, or a
  service account key in `GOOGLE_SERVICE_ACCOUNT` or `GOOGLE_APPLICATION_CREDENTIALS`.
- **`s3://`**: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION` and `AWS_ENDPOINT`.
  To use Google Cloud Storage's S3-compatible API with HMAC keys, set
  `AWS_ENDPOINT=https://storage.googleapis.com` and `AWS_REGION=auto`.

The ledger is opened read-only, as a RocksDB secondary instance, so it can run beside the node
on the same machine or volume while the node keeps writing. It reads the blocks the node has
written at the start of every pass. It refuses a ledger written by a newer snarkVM storage version,
so it must be built from the same or a newer snarkOS release than the node.

Uploads set no per-object ACL. The bucket itself must grant public read access, for example
with uniform bucket-level access and `roles/storage.objectViewer` for `allUsers`, since nodes
download from it without credentials.

There is also a `GET /health` endpoint on port `3123` that returns `200` without
touching the bucket.

## Testing

```
cargo test -p snarkos-ledger-distribution
```

`round_trip` reads a ledger that another process keeps writing, uploads it to an in-process S3
server, then loads it back with `snarkos-node-cdn`, the client snarkOS nodes sync from the CDN with.
`recovery` sets up the bucket states that an interrupted pass or a rollback of the ledger leaves
behind, and checks that one pass repairs each.

## Procedure

1. Catches up with the node's ledger, and checks its latest block height.
2. Repairs the bucket, so that it holds complete chunks from height 0 that match the ledger:
    - A chunk that is missing, or missing either of its files, below the bucket's highest file is uploaded again, as is a highest chunk that cannot be read back.
    - After a rollback of the ledger, `latest.json` is lowered to the last chunk that still matches, and the chunks above it are removed from the highest down.
3. If the ledger is ahead of the bucket by 50 blocks or more, reads 50 blocks at a time from the ledger.
4. Uploads the 50 blocks and their checksum to the bucket.
    - e.g. 0.50.blocks and 0.50.blocks.sum
    - Checksum is sha256(block_bytes)
5. Fetches the newly uploaded blocks and checksum from the bucket, checks that they are equivalent to the ledger's blocks, and updates `latest.json`.
6. Corrects `latest.json` if it does not describe the bucket's highest chunk, even when nothing was uploaded.

When the bucket has caught up to the ledger, it checks every 30 seconds whether it needs to upload more block chunks.
