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

use snarkos_ledger_distribution::{BlocksBucket, Bucket, LedgerReader, sync_with_ledger};
use snarkos_node_cdn::load_blocks;
use snarkos_utilities::SimpleStoppable;
use snarkvm::{
    ledger::{
        Ledger,
        store::{
            BlockStorage,
            ConsensusStore,
            helpers::{
                memory::ConsensusMemory,
                rocksdb::{BlockDB, ConsensusDB},
            },
        },
    },
    prelude::*,
    synthesizer::VM,
    utilities::TestRng,
};

use aleo_std::StorageMode;
use hyper_util::{
    rt::{TokioExecutor, TokioIo},
    server::conn::auto::Builder as ConnBuilder,
};
use object_store::{Attribute, ObjectStoreExt, aws::AmazonS3Builder, path::Path as ObjectPath};
use s3s::{
    S3Result,
    access::{S3Access, S3AccessContext},
    auth::SimpleAuth,
    s3_error,
    service::S3ServiceBuilder,
};
use s3s_fs::FileSystem;
use std::{
    io::BufRead,
    path::Path,
    process::Stdio,
    sync::{Arc, Mutex, atomic::AtomicBool},
};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines},
    process::{Child, ChildStdout, Command},
};

type CurrentNetwork = MainnetV0;

/// The variable that gives the node process its ledger directory.
const LEDGER_DIR_VAR: &str = "LEDGER_DISTRIBUTION_TEST_LEDGER_DIR";
/// The prefix of the line the node process prints once it has written up to a height.
const NODE_HEIGHT_PREFIX: &str = "ledger-distribution-test node height";
/// The heights the node process writes up to, pausing after each: one full chunk of 50 blocks
/// and part of the next, then two full chunks and part of a third.
const NODE_HEIGHTS: [u32; 2] = [60, 110];

const BUCKET_NAME: &str = "blocks";
const ACCESS_KEY: &str = "access-key";
const SECRET_KEY: &str = "secret-key";

/// Plays the node: writes a ledger to the directory in `LEDGER_DIR_VAR`, up to each height in
/// `NODE_HEIGHTS` in turn. After each, it prints the height and waits for a line on stdin.
///
/// The ledger has to be written by another process, since snarkVM allows one open ledger
/// database per process and `LedgerReader` opens one of its own.
#[test]
#[ignore = "run as a child process by `uploaded_blocks_load_through_the_cdn_client`"]
fn node_writing_a_ledger() {
    let Ok(ledger_dir) = std::env::var(LEDGER_DIR_VAR) else {
        return;
    };

    let rng = &mut TestRng::default();
    let private_key = PrivateKey::<CurrentNetwork>::new(rng).unwrap();
    let store = ConsensusStore::<CurrentNetwork, ConsensusMemory<CurrentNetwork>>::open(StorageMode::new_test(None));
    let genesis = VM::from(store.unwrap()).unwrap().genesis_beacon(&private_key, rng).unwrap();
    let ledger = Ledger::<CurrentNetwork, ConsensusDB<CurrentNetwork>>::load(
        genesis,
        StorageMode::Custom(ledger_dir.into(), None),
    )
    .unwrap();

    let mut stdin = std::io::stdin().lock().lines();
    for height in NODE_HEIGHTS {
        while ledger.latest_height() < height {
            let block = ledger.prepare_advance_to_next_beacon_block(&private_key, vec![], vec![], vec![], rng).unwrap();
            ledger.advance_to_next_block(&block).unwrap();
        }
        println!("{NODE_HEIGHT_PREFIX} {height}");
        stdin.next();
    }
}

/// The node process, running `node_writing_a_ledger`.
struct Node {
    child: Child,
    stdout: Lines<BufReader<ChildStdout>>,
}

impl Node {
    fn spawn(ledger_dir: &Path) -> Self {
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args(["node_writing_a_ledger", "--exact", "--ignored", "--nocapture"])
            .env(LEDGER_DIR_VAR, ledger_dir)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let stdout = BufReader::new(child.stdout.take().unwrap()).lines();
        Self { child, stdout }
    }

    /// Waits until the node has written up to the given height of `NODE_HEIGHTS`.
    async fn wait_for_height(&mut self, height: u32) {
        let expected = format!("{NODE_HEIGHT_PREFIX} {height}");
        while let Some(line) = self.stdout.next_line().await.unwrap() {
            if line == expected {
                return;
            }
        }
        panic!("The node process exited before reaching height {height}: {:?}", self.child.wait().await);
    }

    /// Lets the node write up to its next height.
    async fn resume(&mut self) {
        self.child.stdin.as_mut().unwrap().write_all(b"\n").await.unwrap();
    }
}

/// Admits signed requests, and unsigned requests that only read an object, as a bucket whose
/// objects are public does.
struct PublicRead;

#[async_trait::async_trait]
impl S3Access for PublicRead {
    async fn check(&self, cx: &mut S3AccessContext<'_>) -> S3Result<()> {
        if cx.credentials().is_some() || matches!(cx.s3_op().name(), "GetObject" | "HeadObject") {
            Ok(())
        } else {
            Err(s3_error!(AccessDenied, "Signature is required"))
        }
    }
}

/// Serves an S3 API that stores objects under `root`, and returns its base URL.
async fn serve_s3(root: &Path) -> String {
    let mut builder = S3ServiceBuilder::new(FileSystem::new(root).unwrap());
    builder.set_auth(SimpleAuth::from_single(ACCESS_KEY, SECRET_KEY));
    builder.set_access(PublicRead);
    let service = builder.build();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let (socket, _) = listener.accept().await.unwrap();
            let service = service.clone();
            tokio::spawn(async move {
                let _ = ConnBuilder::new(TokioExecutor::new()).serve_connection(TokioIo::new(socket), service).await;
            });
        }
    });
    format!("http://{addr}")
}

/// Returns the paths of the objects in the bucket, sorted.
async fn object_paths(bucket: &Bucket) -> Vec<String> {
    let mut paths: Vec<_> = bucket.list_files().await.unwrap().into_iter().map(|file| file.file_path).collect();
    paths.sort();
    paths
}

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

    // s3s-fs stores each bucket as a directory under its root.
    std::fs::create_dir(s3_root.path().join(BUCKET_NAME)).unwrap();
    let store = AmazonS3Builder::new()
        .with_endpoint(&s3_url)
        .with_allow_http(true)
        .with_region("auto")
        .with_bucket_name(BUCKET_NAME)
        .with_access_key_id(ACCESS_KEY)
        .with_secret_access_key(SECRET_KEY)
        .build()
        .unwrap();
    let store = Arc::new(store);
    let bucket = Bucket::new(store.clone(), BUCKET_NAME);
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
