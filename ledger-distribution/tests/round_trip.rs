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

//! Uploads a ledger's blocks from a mock node to an in-process S3 server, and loads them back
//! with the client snarkOS nodes sync from the CDN with.

use snarkos_ledger_distribution::{Beacon, BlocksBucket, Bucket, permissions::Permission, sync_with_beacon};
use snarkos_node_cdn::load_blocks;
use snarkos_utilities::SimpleStoppable;
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
use axum::{Json, Router, extract::Query, http::StatusCode, routing::get};
use hyper_util::{
    rt::{TokioExecutor, TokioIo},
    server::conn::auto::Builder as ConnBuilder,
};
use s3s::{
    S3Result,
    access::{S3Access, S3AccessContext},
    auth::SimpleAuth,
    s3_error,
    service::S3ServiceBuilder,
};
use s3s_fs::FileSystem;
use std::{
    collections::HashMap,
    path::Path,
    sync::{Arc, Mutex, atomic::AtomicBool},
};

type CurrentNetwork = MainnetV0;

/// The height of the generated ledger: two full chunks of 50 blocks and part of a third.
const LEDGER_HEIGHT: u32 = 110;
/// The most blocks the node's `/blocks` route serves in one request.
const MAX_BLOCK_RANGE: u32 = 50;

const BUCKET_NAME: &str = "blocks";
const ACCESS_KEY: &str = "access-key";
const SECRET_KEY: &str = "secret-key";

/// Returns the blocks of a new ledger up to `LEDGER_HEIGHT`, starting with its genesis block.
fn sample_blocks() -> Vec<Block<CurrentNetwork>> {
    let rng = &mut TestRng::default();
    let private_key = PrivateKey::<CurrentNetwork>::new(rng).unwrap();

    let store = ConsensusStore::<CurrentNetwork, ConsensusMemory<CurrentNetwork>>::open(StorageMode::new_test(None));
    let genesis = VM::from(store.unwrap()).unwrap().genesis_beacon(&private_key, rng).unwrap();
    let ledger =
        Ledger::<CurrentNetwork, ConsensusMemory<CurrentNetwork>>::load(genesis.clone(), StorageMode::new_test(None))
            .unwrap();

    let mut blocks = vec![genesis];
    for _ in 0..LEDGER_HEIGHT {
        let block = ledger.prepare_advance_to_next_beacon_block(&private_key, vec![], vec![], vec![], rng).unwrap();
        ledger.advance_to_next_block(&block).unwrap();
        blocks.push(block);
    }
    blocks
}

/// Serves the node REST routes the uploader reads, with the range checks of the node's own
/// `/blocks` route, and returns the base URL.
fn serve_beacon(blocks: Arc<Vec<Block<CurrentNetwork>>>) -> String {
    let latest_height = blocks.len() as u32 - 1;

    let get_blocks = move |Query(range): Query<HashMap<String, u32>>| {
        let blocks = blocks.clone();
        async move {
            let (Some(&start), Some(&end)) = (range.get("start"), range.get("end")) else {
                return Err(StatusCode::BAD_REQUEST);
            };
            if start > end || end - start > MAX_BLOCK_RANGE {
                return Err(StatusCode::BAD_REQUEST);
            }
            match blocks.get(start as usize..end as usize) {
                Some(range) => Ok(Json(range.to_vec())),
                None => Err(StatusCode::NOT_FOUND),
            }
        }
    };
    let app = Router::new()
        .route("/mainnet/block/height/latest", get(move || async move { Json(latest_height) }))
        .route("/mainnet/blocks", get(get_blocks));

    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(axum::Server::from_tcp(listener).unwrap().serve(app.into_make_service()));
    format!("http://{addr}")
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
    let blocks = Arc::new(tokio::task::spawn_blocking(sample_blocks).await.unwrap());
    let genesis = blocks[0].clone();

    let beacon_url = serve_beacon(blocks.clone());
    let s3_root = tempfile::tempdir().unwrap();
    let s3_url = serve_s3(s3_root.path()).await;

    let bucket = Bucket::new("auto", &s3_url, BUCKET_NAME, ACCESS_KEY.to_string(), SECRET_KEY.to_string());
    bucket.create_bucket(BUCKET_NAME.to_string(), Permission::Private).await.unwrap();
    let blocks_bucket = BlocksBucket::<CurrentNetwork>::new(bucket.clone());
    let beacon = Beacon::<CurrentNetwork>::new(beacon_url);

    let expected_paths =
        ["v0/0.50.blocks", "v0/0.50.blocks.sum", "v0/50.100.blocks", "v0/50.100.blocks.sum", "v0/latest.json"];

    // The first pass starts from an empty bucket; the second checks the uploaded chunks against
    // the node for a rollback, and finds nothing new to upload.
    for _ in 0..2 {
        sync_with_beacon(&blocks_bucket, &beacon, &genesis, Arc::new(AtomicBool::new(false)), false).await.unwrap();

        assert_eq!(object_paths(&bucket).await, expected_paths);
        let latest_state = blocks_bucket.get_latest_state().await.unwrap();
        assert_eq!(latest_state.exclusive_height, 100);
        assert_eq!(latest_state.inclusive_height, 99);
        assert_eq!(latest_state.hash, blocks[99].hash());
    }

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
}
