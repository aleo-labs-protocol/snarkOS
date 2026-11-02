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

#![forbid(unsafe_code)]

#[macro_use]
extern crate tracing;

use snarkos_ledger_distribution::{Beacon, BlocksBucket, Bucket, initialize_rest_server, sync_with_beacon};
use snarkvm::{ledger::block::Block, prelude::*};

use anyhow::Result;
use dotenv::dotenv;
use std::{
    env,
    fs,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::Duration,
};

/// The bucket region, unless `BLOCKS_REGION` is set.
const DEFAULT_BLOCKS_REGION: &str = "auto";
/// The S3-compatible endpoint, unless `BLOCKS_ENDPOINT` is set.
const DEFAULT_BLOCKS_ENDPOINT: &str = "storage.googleapis.com";

/// The base URL of the node REST API that blocks are fetched from, read from `EXPLORER_URL`.
fn explorer_url() -> String {
    env::var("EXPLORER_URL").expect("EXPLORER_URL must be set")
}

/// The genesis block the bucket must start with: the block in the file at `GENESIS_PATH` if it
/// is set, which a development network needs, and the network's genesis block otherwise.
fn genesis_block<N: Network>() -> Result<Block<N>> {
    match env::var("GENESIS_PATH") {
        Ok(path) => Block::from_bytes_le(&fs::read(&path)?),
        Err(_) => Ok(Block::read_le(N::genesis_bytes())?),
    }
}

pub fn initialize_logger(verbosity: u8) {
    let level = match verbosity {
        0 => "info",
        1 => "debug",
        2 | 3 => "trace",
        _ => "info",
    };

    // Filter out undesirable logs.
    let filter = tracing_subscriber::EnvFilter::new(level)
        .add_directive("hyper::client=off".parse().unwrap())
        .add_directive("hyper::proto=off".parse().unwrap())
        .add_directive("jsonrpsee=off".parse().unwrap())
        .add_directive("mio=off".parse().unwrap())
        .add_directive("rusoto_core=off".parse().unwrap())
        .add_directive("tokio_util=off".parse().unwrap())
        .add_directive("want=off".parse().unwrap());

    // Initialize tracing.
    let _ = tracing_subscriber::fmt().with_env_filter(filter).with_target(verbosity == 3).with_ansi(false).try_init();
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize the logger.
    initialize_logger(2);

    // Verify that the requisite environment variables are present.
    dotenv().ok();

    // Determine the network to run the fast sync server on.
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        println!("Invalid number of arguments. Given: {} - Required: 1", args.len() - 1);
        println!("Usage: ledger-distribution <network_id> [--dry-run]");
        return Ok(());
    }
    let network = match args[1].parse::<u16>()? {
        MainnetV0::ID => "mainnet",
        TestnetV0::ID => "testnet",
        CanaryV0::ID => "canary",
        unknown_id => bail!("Unknown network ID ({unknown_id})"),
    };

    // Check for dry-run flag
    let dry_run = args.iter().any(|arg| arg == "--dry-run");
    if dry_run {
        info!("Running in DRY-RUN mode - no chunks will be removed from the bucket");
    }

    info!("Loading bucket keys...\n");

    let key = env::var("BUCKETS_KEY").expect("BUCKETS_KEY must be set");
    let secret = env::var("BUCKETS_SECRET").expect("BUCKETS_SECRET must be set");

    // Initialize the bucket.
    let region = env::var("BLOCKS_REGION").unwrap_or_else(|_| DEFAULT_BLOCKS_REGION.to_string());
    let endpoint = env::var("BLOCKS_ENDPOINT").unwrap_or_else(|_| DEFAULT_BLOCKS_ENDPOINT.to_string());
    let bucket_name = env::var("BUCKET_NAME").expect("BUCKET_NAME must be set");
    info!("Using bucket '{bucket_name}' at endpoint '{endpoint}' (region '{region}')");
    let bucket = Bucket::new(&region, &endpoint, &bucket_name, key, secret);

    // Run the fast sync server.
    match network {
        "mainnet" => run_fast_sync::<MainnetV0>(bucket, dry_run).await,
        "testnet" => run_fast_sync::<TestnetV0>(bucket, dry_run).await,
        "canary" => run_fast_sync::<CanaryV0>(bucket, dry_run).await,
        unknown_network => bail!("Unknown network ({unknown_network})"),
    }
}

/// Run the fast sync server.
async fn run_fast_sync<N: Network>(bucket: Bucket, dry_run: bool) -> Result<()> {
    // Initialize the blocks bucket.
    let blocks_bucket = BlocksBucket::<N>::new(bucket);
    let beacon = Beacon::<N>::new(explorer_url());
    let genesis = genesis_block::<N>()?;

    let (_rest_runtime, _rest_handle) = initialize_rest_server(blocks_bucket.clone()).unwrap();

    // Handle Ctrl-C to ensure that all blocks are dumped to the S3 bucket.
    let shutdown_started: Arc<AtomicBool> = Default::default();
    #[cfg(any(target_family = "unix", target_family = "windows"))]
    handle_ctrl_c(shutdown_started.clone());

    loop {
        // Sync with the beacon.
        if let Err(err) =
            sync_with_beacon::<N>(&blocks_bucket, &beacon, &genesis, shutdown_started.clone(), dry_run).await
        {
            error!("Failed to sync with the beacon: {}", err);
        }

        // Shut down the script if the shutdown signal has been received.
        if shutdown_started.load(Ordering::Relaxed) {
            return Ok(());
        }

        // Sleep for 30 seconds.
        thread::sleep(Duration::from_secs(30));
    }
}

#[cfg(any(target_family = "unix", target_family = "windows"))]
fn handle_ctrl_c(shutdown_started: Arc<AtomicBool>) {
    tokio::spawn(async move {
        // If the signal listener fails, don't rely on it to set the shutdown flag.
        if tokio::signal::ctrl_c().await.is_ok() {
            info!("Preparing to shut down...");
            shutdown_started.store(true, Ordering::Relaxed);
        }
    });
}
