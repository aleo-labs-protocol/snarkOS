// Copyright (c) 2019-2026 Provable Inc.
// This file is part of the snarkOS library.

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at:

// http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Repairs of the bucket states that an interrupted pass, or a rollback of the ledger, leaves
//! behind. Each case sets up a bucket the way the interruption would leave it, and checks that
//! one pass makes it match the ledger again.

mod common;
use common::*;

use snarkos_ledger_distribution::{BlocksBucket, Bucket, LatestState, LedgerReader, sync_with_ledger};
use snarkvm::{
    ledger::{block::Block, store::helpers::memory::ConsensusMemory},
    prelude::*,
};

use aleo_std::StorageMode;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

/// The objects of a bucket holding the ledger's first two chunks.
const SYNCED_PATHS: [&str; 5] =
    ["v0/0.50.blocks", "v0/0.50.blocks.sum", "v0/50.100.blocks", "v0/50.100.blocks.sum", "v0/latest.json"];

/// Returns the blocks of a chain up to height 149, which is past the node's ledger. With
/// `NODE_GENESIS_SEED` it is a fork of the node's chain from its genesis block.
fn chain_blocks(genesis_seed: u64) -> Vec<Block<CurrentNetwork>> {
    let mut chain = Chain::<ConsensusMemory<CurrentNetwork>>::new(StorageMode::new_test(None), genesis_seed);
    chain.advance_to(149);
    (0..150).map(|height| chain.ledger.get_block(height).unwrap()).collect()
}

/// The node's ledger, and the S3 server the cases' buckets live on.
struct Fixture {
    ledger: LedgerReader<CurrentNetwork>,
    /// The ledger's blocks up to height 99, which the bucket must hold after a pass.
    blocks: Vec<Block<CurrentNetwork>>,
    s3_root: tempfile::TempDir,
    s3_url: String,
    s3_faults: Arc<S3Faults>,
}

impl Fixture {
    /// Returns an empty bucket of its own for the case named `name`.
    fn bucket(&self, name: &str) -> (Bucket, BlocksBucket<CurrentNetwork>) {
        let (_, bucket) = new_bucket(self.s3_root.path(), &self.s3_url, name);
        (bucket.clone(), BlocksBucket::new(bucket))
    }

    /// Returns a bucket of its own for the case named `name`, after a pass from empty.
    async fn synced_bucket(&self, name: &str) -> (Bucket, BlocksBucket<CurrentNetwork>) {
        let (bucket, blocks_bucket) = self.bucket(name);
        self.sync(&blocks_bucket).await.unwrap();
        assert_eq!(object_paths(&bucket).await, SYNCED_PATHS);
        (bucket, blocks_bucket)
    }

    async fn sync(&self, blocks_bucket: &BlocksBucket<CurrentNetwork>) -> anyhow::Result<()> {
        self.sync_expecting_genesis(blocks_bucket, &self.blocks[0]).await
    }

    async fn sync_expecting_genesis(
        &self,
        blocks_bucket: &BlocksBucket<CurrentNetwork>,
        genesis: &Block<CurrentNetwork>,
    ) -> anyhow::Result<()> {
        sync_with_ledger(blocks_bucket, &self.ledger, genesis, Arc::new(AtomicBool::new(false)), false).await
    }

    /// Runs a pass that may not delete any chunk's files, and checks that the bucket then holds the
    /// ledger's first two chunks and a `latest.json` that describes them.
    async fn assert_sync_repairs(&self, case: &str, bucket: &Bucket, blocks_bucket: &BlocksBucket<CurrentNetwork>) {
        self.s3_faults.fail_chunk_deletes.store(true, Ordering::Relaxed);
        let result = self.sync(blocks_bucket).await;
        self.s3_faults.fail_chunk_deletes.store(false, Ordering::Relaxed);
        if let Err(error) = result {
            panic!("{case}: the pass failed: {error}");
        }
        assert_eq!(object_paths(bucket).await, SYNCED_PATHS, "{case}");
        assert_eq!(blocks_bucket.get_blocks(0).await.unwrap(), self.blocks[0..50], "{case}");
        assert_eq!(blocks_bucket.get_blocks(50).await.unwrap(), self.blocks[50..100], "{case}");
        let expected = LatestState::from_block(&self.blocks[99]);
        assert_eq!(blocks_bucket.get_latest_state().await.unwrap(), expected, "{case}");
    }
}

/// Uploads the given blocks to the bucket as chunks of 50.
async fn insert_chunks(blocks_bucket: &BlocksBucket<CurrentNetwork>, blocks: &[Block<CurrentNetwork>]) {
    for chunk in blocks.chunks(50) {
        blocks_bucket.insert_blocks(chunk).await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn sync_repairs_interrupted_and_stale_buckets() {
    let ledger_dir = tempfile::tempdir().unwrap();
    let secondary_dir = tempfile::tempdir().unwrap();
    let mut node = Node::spawn(ledger_dir.path());
    node.wait_for_height(NODE_HEIGHTS[0]).await;
    node.resume().await;
    node.wait_for_height(NODE_HEIGHTS[1]).await;
    let fork = tokio::task::spawn_blocking(|| chain_blocks(NODE_GENESIS_SEED)).await.unwrap();
    let other_chain = tokio::task::spawn_blocking(|| chain_blocks(NODE_GENESIS_SEED + 1)).await.unwrap();

    let ledger = LedgerReader::<CurrentNetwork>::open(ledger_dir.path(), secondary_dir.path()).unwrap();
    let blocks = ledger.get_blocks(0, 100).await.unwrap();
    assert_eq!(fork[0], blocks[0]);
    assert_ne!(fork[1], blocks[1]);
    assert_ne!(other_chain[0], blocks[0]);
    let s3_root = tempfile::tempdir().unwrap();
    let (s3_url, s3_faults) = serve_s3(s3_root.path()).await;
    let fixture = Fixture { ledger, blocks, s3_root, s3_url, s3_faults };

    // An upload that stopped between a chunk and its checksum, at the top and further down.
    for (case, path) in
        [("checksum missing at the top", "v0/50.100.blocks.sum"), ("checksum missing below", "v0/0.50.blocks.sum")]
    {
        let (bucket, blocks_bucket) = fixture.synced_bucket(&case.replace(' ', "-")).await;
        bucket.remove(path).await.unwrap();
        fixture.assert_sync_repairs(case, &bucket, &blocks_bucket).await;
    }

    // An upload whose chunk does not match its checksum.
    let case = "chunk corrupt at the top";
    let (bucket, blocks_bucket) = fixture.synced_bucket("chunk-corrupt").await;
    bucket.insert_or_update("v0/50.100.blocks", b"not a chunk").await.unwrap();
    fixture.assert_sync_repairs(case, &bucket, &blocks_bucket).await;

    // A chunk missing below the top one.
    let case = "chunk missing below";
    let (bucket, blocks_bucket) = fixture.synced_bucket("chunk-missing").await;
    bucket.remove("v0/0.50.blocks").await.unwrap();
    bucket.remove("v0/0.50.blocks.sum").await.unwrap();
    fixture.assert_sync_repairs(case, &bucket, &blocks_bucket).await;

    // A pass that stopped before `latest.json` was written, or before it was updated.
    let case = "latest.json missing";
    let (bucket, blocks_bucket) = fixture.synced_bucket("latest-missing").await;
    blocks_bucket.remove_latest_state().await.unwrap();
    fixture.assert_sync_repairs(case, &bucket, &blocks_bucket).await;

    let case = "latest.json behind";
    let (bucket, blocks_bucket) = fixture.synced_bucket("latest-behind").await;
    blocks_bucket.update_latest_state(&fixture.blocks[49]).await.unwrap();
    fixture.assert_sync_repairs(case, &bucket, &blocks_bucket).await;

    // Chunks of a chain the ledger has left, all the way back to the genesis block.
    let case = "rollback to genesis";
    let (bucket, blocks_bucket) = fixture.bucket("rollback-to-genesis");
    insert_chunks(&blocks_bucket, &fork[..100]).await;
    blocks_bucket.update_latest_state(&fork[99]).await.unwrap();
    fixture.assert_sync_repairs(case, &bucket, &blocks_bucket).await;

    // Chunks of a chain the ledger has left above its first chunk.
    let case = "rollback above the first chunk";
    let (bucket, blocks_bucket) = fixture.bucket("rollback-above-first");
    insert_chunks(&blocks_bucket, &fixture.blocks[..50]).await;
    insert_chunks(&blocks_bucket, &fork[50..100]).await;
    blocks_bucket.update_latest_state(&fork[99]).await.unwrap();
    fixture.assert_sync_repairs(case, &bucket, &blocks_bucket).await;

    // A stale chunk above a hole, and no chunk that matches the ledger.
    let case = "stale chunk above a hole";
    let (bucket, blocks_bucket) = fixture.bucket("stale-above-hole");
    insert_chunks(&blocks_bucket, &fork[50..100]).await;
    blocks_bucket.update_latest_state(&fork[99]).await.unwrap();
    fixture.assert_sync_repairs(case, &bucket, &blocks_bucket).await;

    // A stale chunk missing one of its files, above another stale chunk. Writing the incomplete
    // chunk from the ledger before comparing would make it match, and hide the stale chunk below.
    for (case, path) in
        [("stale chunk without blocks", "v0/50.100.blocks"), ("stale chunk without checksum", "v0/50.100.blocks.sum")]
    {
        let (bucket, blocks_bucket) = fixture.bucket(&case.replace(' ', "-"));
        insert_chunks(&blocks_bucket, &fork[..100]).await;
        blocks_bucket.update_latest_state(&fork[99]).await.unwrap();
        bucket.remove(path).await.unwrap();
        fixture.assert_sync_repairs(case, &bucket, &blocks_bucket).await;
    }

    // A `latest.json` left over from a removed chain, with none of its chunks.
    let case = "latest.json without chunks";
    let (bucket, blocks_bucket) = fixture.bucket("latest-without-chunks");
    blocks_bucket.update_latest_state(&fork[49]).await.unwrap();
    fixture.assert_sync_repairs(case, &bucket, &blocks_bucket).await;

    // Chunks past the ledger are left as they are until the ledger reaches them.
    let (bucket, blocks_bucket) = fixture.bucket("past-the-ledger");
    insert_chunks(&blocks_bucket, &fork).await;
    blocks_bucket.update_latest_state(&fork[149]).await.unwrap();
    let paths = object_paths(&bucket).await;
    fixture.sync(&blocks_bucket).await.unwrap();
    assert_eq!(object_paths(&bucket).await, paths);
    assert_eq!(blocks_bucket.get_blocks(100).await.unwrap(), fork[100..150]);
    assert_eq!(blocks_bucket.get_latest_state().await.unwrap(), LatestState::from_block(&fork[149]));

    // Failing to fetch the highest chunk fails the pass, and does not touch the bucket.
    let (bucket, blocks_bucket) = fixture.synced_bucket("fetch-fails").await;
    for key in ["v0/50.100.blocks", "v0/50.100.blocks.sum"] {
        *fixture.s3_faults.fail_get_of.lock().unwrap() = Some(key.to_string());
        assert!(fixture.sync(&blocks_bucket).await.is_err(), "{key}");
        *fixture.s3_faults.fail_get_of.lock().unwrap() = None;
        assert_eq!(object_paths(&bucket).await, SYNCED_PATHS, "{key}");
        assert_eq!(blocks_bucket.get_latest_state().await.unwrap(), LatestState::from_block(&fixture.blocks[99]));
        assert_eq!(blocks_bucket.get_blocks(50).await.unwrap(), fixture.blocks[50..100]);
    }

    // A ledger of another chain than the genesis block's fails the pass, and does not touch the bucket.
    let (bucket, blocks_bucket) = fixture.synced_bucket("other-ledger").await;
    assert!(fixture.sync_expecting_genesis(&blocks_bucket, &other_chain[0]).await.is_err());
    assert_eq!(object_paths(&bucket).await, SYNCED_PATHS);
    assert_eq!(blocks_bucket.get_blocks(0).await.unwrap(), fixture.blocks[0..50]);
    assert_eq!(blocks_bucket.get_latest_state().await.unwrap(), LatestState::from_block(&fixture.blocks[99]));

    // A bucket of another chain fails the pass, and is not touched.
    let (bucket, blocks_bucket) = fixture.bucket("other-bucket");
    insert_chunks(&blocks_bucket, &other_chain[..100]).await;
    blocks_bucket.update_latest_state(&other_chain[99]).await.unwrap();
    assert!(fixture.sync(&blocks_bucket).await.is_err());
    assert_eq!(object_paths(&bucket).await, SYNCED_PATHS);
    assert_eq!(blocks_bucket.get_blocks(0).await.unwrap(), other_chain[0..50]);
    assert_eq!(blocks_bucket.get_blocks(50).await.unwrap(), other_chain[50..100]);
    assert_eq!(blocks_bucket.get_latest_state().await.unwrap(), LatestState::from_block(&other_chain[99]));

    // A first chunk that cannot be read, under chunks that do not match, could be of another chain.
    let (bucket, blocks_bucket) = fixture.bucket("first-chunk-unreadable");
    insert_chunks(&blocks_bucket, &fork[..100]).await;
    blocks_bucket.update_latest_state(&fork[99]).await.unwrap();
    bucket.insert_or_update("v0/0.50.blocks", b"not a chunk").await.unwrap();
    assert!(fixture.sync(&blocks_bucket).await.is_err());
    assert_eq!(object_paths(&bucket).await, SYNCED_PATHS);
    assert_eq!(blocks_bucket.get_blocks(50).await.unwrap(), fork[50..100]);
    assert_eq!(blocks_bucket.get_latest_state().await.unwrap(), LatestState::from_block(&fork[99]));

    // A `.blocks` name that is not a chunk is reported, rather than panicking.
    let (bucket, blocks_bucket) = fixture.synced_bucket("unexpected-name").await;
    bucket.insert_or_update("v0/foo.blocks", b"").await.unwrap();
    assert!(blocks_bucket.latest_height().await.is_err());
    assert!(fixture.sync(&blocks_bucket).await.is_err());

    drop(node.child.stdin.take());
    assert!(node.child.wait().await.unwrap().success());
}
