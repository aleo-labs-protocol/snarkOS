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

//! Uploads the blocks of a ledger that another process is writing to an in-process S3 server,
//! reading the ledger as the binary does, and loads them back with the client snarkOS nodes sync
//! from the CDN with.

mod common;
use common::*;

use snarkos_ledger_distribution::{BlocksBucket, LedgerReader, sync_with_ledger};
use snarkos_node_cdn::load_blocks;
use snarkos_utilities::SimpleStoppable;
use snarkvm::ledger::store::{BlockStorage, helpers::rocksdb::BlockDB};

use aleo_std::StorageMode;
use object_store::{Attribute, ObjectStoreExt, path::Path as ObjectPath};
use std::sync::{Arc, Mutex, atomic::AtomicBool};

#[tokio::test(flavor = "multi_thread")]
async fn uploaded_blocks_load_through_the_cdn_client() {
    let ledger_dir = tempfile::tempdir().unwrap();
    let secondary_dir = tempfile::tempdir().unwrap();
    let mut node = Node::spawn(ledger_dir.path());
    node.wait_for_height(NODE_HEIGHTS[0]).await;

    let ledger = LedgerReader::<CurrentNetwork>::open(ledger_dir.path(), secondary_dir.path()).unwrap();
    let genesis = ledger.get_block(0).await.unwrap();

    let s3_root = tempfile::tempdir().unwrap();
    let s3_url = serve_s3(s3_root.path()).await;

    let (store, bucket) = new_bucket(s3_root.path(), &s3_url, BUCKET_NAME);
    let blocks_bucket = BlocksBucket::<CurrentNetwork>::new(bucket.clone());
    let sync = || sync_with_ledger(&blocks_bucket, &ledger, &genesis, Arc::new(AtomicBool::new(false)), false);

    // At height 60, only the first chunk is complete.
    sync().await.unwrap();
    assert_eq!(object_paths(&bucket).await, ["v0/0.50.blocks", "v0/0.50.blocks.sum", "v0/latest.json"]);
    assert_eq!(blocks_bucket.get_latest_state().await.unwrap().exclusive_height, 50);

    // The node keeps writing while the ledger is open.
    node.resume().await;
    node.wait_for_height(NODE_HEIGHTS[1]).await;

    // Until it catches up, the ledger has only the blocks the node had written when it was opened.
    assert!(ledger.get_block(NODE_HEIGHTS[0] + 1).await.is_err());

    let expected_paths =
        ["v0/0.50.blocks", "v0/0.50.blocks.sum", "v0/50.100.blocks", "v0/50.100.blocks.sum", "v0/latest.json"];
    // The first pass catches up with the node and uploads the second chunk; the second checks the
    // uploaded chunks against the ledger for a rollback, and finds nothing new to upload.
    for _ in 0..2 {
        sync().await.unwrap();

        assert_eq!(object_paths(&bucket).await, expected_paths);
        let latest_state = blocks_bucket.get_latest_state().await.unwrap();
        assert_eq!(latest_state.exclusive_height, 100);
        assert_eq!(latest_state.inclusive_height, 99);
        assert_eq!(latest_state.hash, ledger.get_block(99).await.unwrap().hash());
    }
    let blocks = ledger.get_blocks(0, 100).await.unwrap();

    let latest_json = store.get(&ObjectPath::from("v0/latest.json")).await.unwrap();
    assert_eq!(
        latest_json.attributes.get(&Attribute::CacheControl).map(|value| value.as_ref()),
        Some("no-cache, max-age=0")
    );

    // As a node that has only the genesis block does.
    let loaded = Arc::new(Mutex::new(Vec::new()));
    let base_url = format!("{s3_url}/{BUCKET_NAME}/v0").parse().unwrap();
    let process = {
        let loaded = loaded.clone();
        move |block| {
            loaded.lock().unwrap().push(block);
            Ok(())
        }
    };
    let height = load_blocks::<CurrentNetwork>(&base_url, 1, None, SimpleStoppable::new(), process)
        .await
        .map_err(|(height, error)| format!("Failed at height {height}: {error}"))
        .unwrap();

    assert_eq!(height, 99);
    assert_eq!(*loaded.lock().unwrap(), blocks[1..100]);

    // snarkVM keeps one handle per ledger database and storage mode, so this is the reader's own.
    let storage = BlockDB::<CurrentNetwork>::open(StorageMode::Custom(
        ledger_dir.path().into(),
        Some(secondary_dir.path().into()),
    ))
    .unwrap();
    let hash = storage.get_block_hash(99).unwrap().unwrap();
    assert!(storage.remove(&hash).is_err(), "The ledger was writable through the read-only handle");
    assert_eq!(ledger.get_block(99).await.unwrap(), blocks[99]);

    // Closing stdin lets the node process finish.
    drop(node.child.stdin.take());
    assert!(node.child.wait().await.unwrap().success());
}
