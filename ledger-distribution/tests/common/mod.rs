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

//! The node process, S3 server and bucket helpers that the integration tests share.

#![allow(dead_code)]

use snarkos_ledger_distribution::Bucket;
use snarkvm::{
    ledger::{
        Ledger,
        store::{
            ConsensusStore,
            helpers::{memory::ConsensusMemory, rocksdb::ConsensusDB},
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
use object_store::aws::{AmazonS3, AmazonS3Builder};
use s3s::{
    S3Result,
    access::{S3Access, S3AccessContext},
    auth::SimpleAuth,
    s3_error,
    service::S3ServiceBuilder,
};
use s3s_fs::FileSystem;
use std::{io::BufRead, path::Path, process::Stdio, sync::Arc};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines},
    process::{Child, ChildStdout, Command},
};

pub type CurrentNetwork = MainnetV0;

/// The variable that gives the node process its ledger directory.
const LEDGER_DIR_VAR: &str = "LEDGER_DISTRIBUTION_TEST_LEDGER_DIR";
/// The prefix of the line the node process prints once it has written up to a height.
const NODE_HEIGHT_PREFIX: &str = "ledger-distribution-test node height";
/// The heights the node process writes up to, pausing after each: one full chunk of 50 blocks
/// and part of the next, then two full chunks and part of a third.
pub const NODE_HEIGHTS: [u32; 2] = [60, 110];

pub const BUCKET_NAME: &str = "blocks";
pub const ACCESS_KEY: &str = "access-key";
pub const SECRET_KEY: &str = "secret-key";

/// Plays the node: writes a ledger to the directory in `LEDGER_DIR_VAR`, up to each height in
/// `NODE_HEIGHTS` in turn. After each, it prints the height and waits for a line on stdin.
///
/// The ledger has to be written by another process, since snarkVM allows one open ledger
/// database per process and `LedgerReader` opens one of its own.
#[test]
#[ignore = "run as a child process by the tests that spawn a `Node`"]
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
pub struct Node {
    pub child: Child,
    stdout: Lines<BufReader<ChildStdout>>,
}

impl Node {
    pub fn spawn(ledger_dir: &Path) -> Self {
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args(["common::node_writing_a_ledger", "--exact", "--ignored", "--nocapture"])
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
    pub async fn wait_for_height(&mut self, height: u32) {
        let expected = format!("{NODE_HEIGHT_PREFIX} {height}");
        while let Some(line) = self.stdout.next_line().await.unwrap() {
            if line == expected {
                return;
            }
        }
        panic!("The node process exited before reaching height {height}: {:?}", self.child.wait().await);
    }

    /// Lets the node write up to its next height.
    pub async fn resume(&mut self) {
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
pub async fn serve_s3(root: &Path) -> String {
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
pub async fn object_paths(bucket: &Bucket) -> Vec<String> {
    let mut paths: Vec<_> = bucket.list_files().await.unwrap().into_iter().map(|file| file.file_path).collect();
    paths.sort();
    paths
}

/// Returns an S3 bucket named `name` on the server at `s3_url`, which stores its buckets under
/// `s3_root`, through `object_store` as the binary reaches it.
pub fn new_bucket(s3_root: &Path, s3_url: &str, name: &str) -> (Arc<AmazonS3>, Bucket) {
    // s3s-fs stores each bucket as a directory under its root.
    std::fs::create_dir(s3_root.join(name)).unwrap();
    let store = AmazonS3Builder::new()
        .with_endpoint(s3_url)
        .with_allow_http(true)
        .with_region("auto")
        .with_bucket_name(name)
        .with_access_key_id(ACCESS_KEY)
        .with_secret_access_key(SECRET_KEY)
        .build()
        .unwrap();
    let store = Arc::new(store);
    (store.clone(), Bucket::new(store, name))
}
