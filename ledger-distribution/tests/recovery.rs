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
    ledger::{
        Ledger,
        block::Block,
        store::{ConsensusStore, helpers::memory::ConsensusMemory},
    },
    prelude::*,
    synthesizer::VM,
    utilities::TestRng,
};

use aleo_std::StorageMode;
use std::sync::{Arc, atomic::AtomicBool};

/// The objects of a bucket holding the ledger's first two chunks.
const SYNCED_PATHS: [&str; 5] =
    ["v0/0.50.blocks", "v0/0.50.blocks.sum", "v0/50.100.blocks", "v0/50.100.blocks.sum", "v0/latest.json"];

/// Returns the blocks of a chain other than the node's, from its own genesis block up to height 99.
fn fork_blocks() -> Vec<Block<CurrentNetwork>> {
    let rng = &mut TestRng::default();
    let private_key = PrivateKey::<CurrentNetwork>::new(rng).unwrap();
    let store = ConsensusStore::<CurrentNetwork, ConsensusMemory<CurrentNetwork>>::open(StorageMode::new_test(None));
    let genesis = VM::from(store.unwrap()).unwrap().genesis_beacon(&private_key, rng).unwrap();
    let ledger =
        Ledger::<CurrentNetwork, ConsensusMemory<CurrentNetwork>>::load(genesis.clone(), StorageMode::new_test(None))
            .unwrap();

    let mut blocks = vec![genesis];
    while ledger.latest_height() < 99 {
        let block = ledger.prepare_advance_to_next_beacon_block(&private_key, vec![], vec![], vec![], rng).unwrap();
        ledger.advance_to_next_block(&block).unwrap();
        blocks.push(block);
    }
    blocks
}

/// The node's ledger, and the S3 server the cases' buckets live on.
struct Fixture {
    ledger: LedgerReader<CurrentNetwork>,
    /// The ledger's blocks up to height 99, which the bucket must hold after a pass.
    blocks: Vec<Block<CurrentNetwork>>,
    s3_root: tempfile::TempDir,
    s3_url: String,
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
        sync_with_ledger(blocks_bucket, &self.ledger, &self.blocks[0], Arc::new(AtomicBool::new(false)), false).await
    }

    /// Runs a pass, and checks that the bucket then holds the ledger's first two chunks and a
    /// `latest.json` that describes them.
    async fn assert_sync_repairs(&self, case: &str, bucket: &Bucket, blocks_bucket: &BlocksBucket<CurrentNetwork>) {
        if let Err(error) = self.sync(blocks_bucket).await {
            panic!("{case}: the pass failed: {error}");
        }
        assert_eq!(object_paths(bucket).await, SYNCED_PATHS, "{case}");
        assert_eq!(blocks_bucket.get_blocks(0).await.unwrap(), self.blocks[0..50], "{case}");
        assert_eq!(blocks_bucket.get_blocks(50).await.unwrap(), self.blocks[50..100], "{case}");
        let expected =
            LatestState::<CurrentNetwork> { exclusive_height: 100, inclusive_height: 99, hash: self.blocks[99].hash() };
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
    for (index, height) in NODE_HEIGHTS.into_iter().enumerate() {
        if index > 0 {
            node.resume().await;
        }
        node.wait_for_height(height).await;
    }
    let fork = tokio::task::spawn_blocking(fork_blocks).await.unwrap();

    let ledger = LedgerReader::<CurrentNetwork>::open(ledger_dir.path(), secondary_dir.path()).unwrap();
    let blocks = ledger.get_blocks(0, 100).await.unwrap();
    let s3_root = tempfile::tempdir().unwrap();
    let s3_url = serve_s3(s3_root.path()).await;
    let fixture = Fixture { ledger, blocks, s3_root, s3_url };

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
    blocks_bucket.remove_blocks_chunk(0).await.unwrap();
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
    let case = "rollback";
    let (bucket, blocks_bucket) = fixture.bucket("rollback");
    insert_chunks(&blocks_bucket, &fork).await;
    blocks_bucket.update_latest_state(&fork[99]).await.unwrap();
    fixture.assert_sync_repairs(case, &bucket, &blocks_bucket).await;

    // The same rollback, stopped after removing the lowest stale chunk, as the removal used to run
    // from the lowest chunk up.
    let case = "rollback interrupted";
    let (bucket, blocks_bucket) = fixture.bucket("rollback-interrupted");
    insert_chunks(&blocks_bucket, &fork[50..100]).await;
    blocks_bucket.update_latest_state(&fork[99]).await.unwrap();
    fixture.assert_sync_repairs(case, &bucket, &blocks_bucket).await;

    // A `latest.json` left over from a removed chain, with none of its chunks.
    let case = "latest.json without chunks";
    let (bucket, blocks_bucket) = fixture.bucket("latest-without-chunks");
    blocks_bucket.update_latest_state(&fork[49]).await.unwrap();
    fixture.assert_sync_repairs(case, &bucket, &blocks_bucket).await;

    // A `.blocks` name that is not a chunk is reported, rather than panicking.
    let (bucket, blocks_bucket) = fixture.synced_bucket("unexpected-name").await;
    bucket.insert_or_update("v0/foo.blocks", b"").await.unwrap();
    assert!(blocks_bucket.latest_height().await.is_err());
    assert!(fixture.sync(&blocks_bucket).await.is_err());

    drop(node.child.stdin.take());
    assert!(node.child.wait().await.unwrap().success());
}
