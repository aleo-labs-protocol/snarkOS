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

use crate::BlocksBucket;
use snarkvm::prelude::*;

use axum::{Json, Router, extract::Extension, http::StatusCode, response::IntoResponse, routing::get};
use std::net::SocketAddr;
use tower_http::trace::TraceLayer;

pub fn initialize_rest_server<N: Network>(
    blocks_bucket: BlocksBucket<N>,
) -> anyhow::Result<(tokio::runtime::Runtime, tokio::task::JoinHandle<()>)> {
    let num_threads = (num_cpus::get() / 2).max(2);
    info!("REST server is running on {num_threads} threads");

    // Initialize a runtime.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_stack_size(16 * 1024 * 1024)
        .worker_threads(num_threads)
        .build()?;

    let handle = runtime.spawn(async move {
        start_rest_server(blocks_bucket).await;
    });

    Ok((runtime, handle))
}

#[rustfmt::skip]
async fn start_rest_server<N: Network>(blocks_bucket: BlocksBucket<N>) {
    let network = match N::ID {
        MainnetV0::ID => "mainnet",
        TestnetV0::ID => "testnet",
        CanaryV0::ID => "canary",
        unknown_id => {
            eprintln!("Unknown network ID ({unknown_id})");
            return;
        }
    };

    // initialize server
    let app = Router::new()
        .route("/health", get(get_health))
        .route(&format!("/{network}/fastsync/latestState"), get(get_latest_state::<N>))
        .route(&format!("/{network}/fastsync/latestHeight"), get(get_latest_height::<N>))
        .layer(TraceLayer::new_for_http())
        .layer(Extension(blocks_bucket));

    // Serve on port 3123.
    let addr = SocketAddr::from(([0, 0, 0, 0], 3123));
    // Run server.
    debug!("Serving at {}", addr);
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

/// Liveness probe endpoint. Returns `200 OK` without touching the bucket, so it
/// reflects process health independently of the GCS-backed endpoints below.
async fn get_health() -> impl IntoResponse {
    StatusCode::OK
}

/// Returns the contents of `latest.json`.
async fn get_latest_state<N: Network>(Extension(blocks_bucket): Extension<BlocksBucket<N>>) -> impl IntoResponse {
    match blocks_bucket.get_latest_state().await {
        Ok(latest_state) => Ok::<_, (StatusCode, String)>(Json(latest_state)),
        Err(err) => Err((StatusCode::INTERNAL_SERVER_ERROR, err.to_string())),
    }
}

/// Returns the exclusive end height of the highest block chunk in the bucket.
async fn get_latest_height<N: Network>(Extension(blocks_bucket): Extension<BlocksBucket<N>>) -> impl IntoResponse {
    match blocks_bucket.latest_height().await {
        Ok(latest_height) => Ok::<_, (StatusCode, String)>(Json(latest_height)),
        Err(err) => Err((StatusCode::INTERNAL_SERVER_ERROR, err.to_string())),
    }
}
