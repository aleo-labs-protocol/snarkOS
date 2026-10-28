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

use snarkos_ledger_distribution::{BlocksBucket, Bucket, NUM_BLOCKS_PER_CHUNK, initialize_rest_server};
use snarkvm::{ledger::block::Block, prelude::*};

use anyhow::Result;
use dotenv::dotenv;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use std::{
    env,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

/// The bucket region, unless `BLOCKS_REGION` is set.
const DEFAULT_BLOCKS_REGION: &str = "auto";
/// The S3-compatible endpoint, unless `BLOCKS_ENDPOINT` is set.
const DEFAULT_BLOCKS_ENDPOINT: &str = "storage.googleapis.com";

/// The base URL of the node REST API that blocks are fetched from, read from `EXPLORER_URL`.
fn explorer_url() -> String {
    env::var("EXPLORER_URL").expect("EXPLORER_URL must be set")
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

/// Get the latest block height of the beacon.
async fn get_beacon_latest_height<N: Network>() -> Result<u32> {
    let network = match N::ID {
        MainnetV0::ID => "mainnet",
        TestnetV0::ID => "testnet",
        CanaryV0::ID => "canary",
        unknown_id => bail!("Unknown network ID ({unknown_id})"),
    };

    let client = reqwest::Client::new();
    let response = client.get(format!("{}/{network}/block/height/latest", explorer_url())).send().await?;

    Ok(response.text().await?.parse::<u32>()?)
}

/// Fetch a range of blocks from the beacon REST API endpoint.
async fn get_blocks_from_beacon<N: Network>(start_height: u32, end_height: u32) -> Result<Vec<Block<N>>> {
    ensure!(end_height - start_height <= 50, "Cannot fetch more than 50 blocks at a time");

    let network = match N::ID {
        MainnetV0::ID => "mainnet",
        TestnetV0::ID => "testnet",
        CanaryV0::ID => "canary",
        unknown_id => bail!("Unknown network ID ({unknown_id})"),
    };

    info!("Fetching blocks {} to {} from the beacon", start_height, end_height);

    let timer = Instant::now();
    let explorer_url = explorer_url();
    let client = reqwest::Client::new();
    let response =
        client.get(format!("{explorer_url}/{network}/blocks?start={start_height}&end={end_height}")).send().await?;

    debug!("\tFetched blocks {start_height} to {end_height} in {}ms", timer.elapsed().as_millis());

    let timer = Instant::now();
    let json_array: Vec<serde_json::Value> = serde_json::from_str(&response.text().await?)?;
    let blocks: Vec<Block<N>> =
        json_array.into_par_iter().map(serde_json::from_value::<Block<N>>).collect::<Result<Vec<_>, _>>()?;
    debug!("\tDeserialized blocks {start_height} to {end_height} in {}ms", timer.elapsed().as_millis());

    Ok(blocks)
}

/// Fetch a single block from the beacon REST API endpoint.
async fn get_block_from_beacon<N: Network>(height: u32) -> Result<Block<N>> {
    let blocks = get_blocks_from_beacon::<N>(height, height + 1).await?;
    ensure!(blocks.len() == 1, "Expected exactly 1 block, got {}", blocks.len());
    Ok(blocks[0].clone())
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
    };

    Ok(())
}

/// Run the fast sync server.
async fn run_fast_sync<N: Network>(bucket: Bucket, dry_run: bool) {
    // Initialize the blocks bucket.
    let blocks_bucket = BlocksBucket::<N>::new(bucket);

    let (_rest_runtime, _rest_handle) = initialize_rest_server(blocks_bucket.clone()).unwrap();

    // Handle Ctrl-C to ensure that all blocks are dumped to the S3 bucket.
    let shutdown_started: Arc<AtomicBool> = Default::default();
    #[cfg(any(target_family = "unix", target_family = "windows"))]
    handle_ctrl_c(shutdown_started.clone());

    loop {
        // Sync with the beacon.
        if let Err(err) = sync_with_beacon::<N>(&blocks_bucket, shutdown_started.clone(), dry_run).await {
            error!("Failed to sync with the beacon: {}", err);
        }

        // Shut down the script if the shutdown signal has been received.
        if shutdown_started.load(Ordering::Relaxed) {
            return;
        }

        // Sleep for 30 seconds.
        thread::sleep(Duration::from_secs(30));
    }
}

/// Detect and recover from a blockchain rollback.
async fn detect_and_recover_from_rollback<N: Network>(
    blocks_bucket: &BlocksBucket<N>,
    latest_bucket_height: u32,
    dry_run: bool,
) -> Result<u32> {
    if latest_bucket_height == 0 {
        return Ok(0);
    }

    info!("Checking for blockchain rollback...");

    let mut current_height = latest_bucket_height;
    let mut chunks_to_remove = Vec::new();

    loop {
        let chunk_start = current_height.saturating_sub(NUM_BLOCKS_PER_CHUNK);

        let blocks = blocks_bucket.get_blocks(chunk_start).await?;
        let bucket_block = blocks.last().unwrap().clone();
        let bucket_block_height = bucket_block.height();
        let beacon_block = get_block_from_beacon::<N>(bucket_block_height).await?;

        if bucket_block.hash() == beacon_block.hash() {
            info!("Found fork point at height {}", bucket_block_height);
            break;
        }

        warn!("Hash mismatch at height {}.", bucket_block_height);
        chunks_to_remove.push(chunk_start);
        if chunk_start == 0 {
            break;
        }
        current_height = chunk_start;
    }

    if chunks_to_remove.is_empty() {
        return Ok(latest_bucket_height);
    }

    let new_latest_height = if chunks_to_remove.contains(&0) { 0 } else { *chunks_to_remove.last().unwrap() };

    if dry_run {
        warn!("DRY-RUN: would remove {} stale chunk(s):", chunks_to_remove.len());
        for chunk_start in chunks_to_remove.iter().rev() {
            let chunk_end = chunk_start + NUM_BLOCKS_PER_CHUNK;
            warn!("  - v0/{}.{}.blocks", chunk_start, chunk_end);
        }
        warn!("DRY-RUN: latest bucket height would change from {} to {}", latest_bucket_height, new_latest_height);
        return Ok(latest_bucket_height);
    }

    info!("Removing {} stale chunk(s)...", chunks_to_remove.len());
    for chunk_start in chunks_to_remove.iter().rev() {
        blocks_bucket.remove_blocks_chunk(*chunk_start).await?;
    }

    if new_latest_height > 0 {
        info!("New latest bucket height: {}", new_latest_height);
    }

    if new_latest_height > 0 {
        let blocks = blocks_bucket.get_blocks(new_latest_height.saturating_sub(NUM_BLOCKS_PER_CHUNK)).await?;
        let latest_block = blocks[blocks.len() - 1].clone();
        blocks_bucket.update_latest_state(&latest_block).await?;
    }

    Ok(new_latest_height)
}

/// Sync the bucket blocks with the beacon.
async fn sync_with_beacon<N: Network>(
    blocks_bucket: &BlocksBucket<N>,
    shutdown_started: Arc<AtomicBool>,
    dry_run: bool,
) -> Result<()> {
    info!("Fetching latest beacon height...");
    // Get the latest height of the beacon.
    let latest_beacon_height = get_beacon_latest_height::<N>().await?;

    info!("Fetching latest bucket height...\n");
    // Get the latest height of the stored blocks.
    let latest_bucket_height = blocks_bucket.latest_height().await?;

    info!("Latest bucket height {}, latest beacon height {}\n", latest_bucket_height, latest_beacon_height);

    // Detect and recover from any blockchain rollback
    let latest_bucket_height =
        detect_and_recover_from_rollback::<N>(blocks_bucket, latest_bucket_height, dry_run).await?;

    info!("Latest bucket height after rollback check: {}\n", latest_bucket_height);

    // Update the blocks bucket with the latest blocks from the beacon.
    let mut start_height = latest_bucket_height;
    ensure!(
        start_height.is_multiple_of(NUM_BLOCKS_PER_CHUNK),
        "The latest block height must be a multiple of {NUM_BLOCKS_PER_CHUNK}"
    );

    // Fetch the latest chunk of blocks from the bucket.
    let mut latest_block_in_bucket = match start_height {
        0 => Block::<N>::read_le(N::genesis_bytes())?,
        height => {
            let blocks = blocks_bucket.get_blocks(height.saturating_sub(NUM_BLOCKS_PER_CHUNK)).await?;
            blocks[blocks.len() - 1].clone()
        }
    };

    info!("Uploading blocks to bucket...\n");
    // Upload the blocks chunks to the S3 bucket.
    while start_height <= latest_beacon_height {
        info!("Latest bucket height {}, latest beacon height {}", start_height, latest_beacon_height);
        // Fetch the end height of the block range.
        let end_height = start_height + NUM_BLOCKS_PER_CHUNK;

        if end_height > latest_beacon_height {
            info!(
                "Block bucket is up to date with the beacon. (To the closest block chunk of {})",
                NUM_BLOCKS_PER_CHUNK
            );
            break;
        }

        // Fetch the blocks from the beacon.
        let blocks = get_blocks_from_beacon::<N>(start_height, end_height).await?;
        ensure!(
            blocks.len() == NUM_BLOCKS_PER_CHUNK as usize,
            "The number of blocks fetched must be {NUM_BLOCKS_PER_CHUNK}"
        );

        // Check that the block chunks are continuous.
        // i.e. block chunks 0-50 and 50-100 are "connected" with the correct block hashes and previous block hashes.
        info!("Checking that the block chunks are continuous...");
        if start_height == 0 {
            ensure!(latest_block_in_bucket == blocks[0], "The starting block must be the genesis block");
        } else {
            ensure!(
                latest_block_in_bucket.height() + 1 == blocks[0].height(),
                "The starting block height must be the next block height - 1"
            );
            ensure!(
                latest_block_in_bucket.hash() == blocks[0].previous_hash(),
                "The starting block hash must be the next block previous hash"
            );
        }
        // Set the last block of this chunk (for comparison with the next chunk).
        latest_block_in_bucket = blocks[blocks.len() - 1].clone();

        // Insert the blocks into the bucket.
        blocks_bucket.insert_blocks(&blocks).await?;

        // Check that the inserted blocks are correct.
        let blocks_from_bucket = blocks_bucket.get_blocks(start_height).await?;
        debug!("Checking that the bucket and beacon blocks match...");
        ensure!(blocks == blocks_from_bucket, "The blocks from the bucket do not match the blocks from the beacon");

        // Update the latest state.
        blocks_bucket.update_latest_state(&latest_block_in_bucket).await?;
        let latest_state = blocks_bucket.get_latest_state().await?;
        debug!("Checking that the latest state is correct...\n");
        ensure!(
            latest_state.exclusive_height == end_height,
            "The latest state inclusive height does not match the latest block"
        );
        ensure!(
            latest_state.inclusive_height == latest_block_in_bucket.height(),
            "The latest state inclusive height does not match the latest block"
        );
        ensure!(
            latest_state.hash == latest_block_in_bucket.hash(),
            "The latest state hash does not match the latest block"
        );

        // Update the start_height for the next chunk.
        start_height = end_height;

        // If the Ctrl-C handler registered the signal, stop the node once the current upload is complete.
        if shutdown_started.load(Ordering::Relaxed) {
            info!("Shutting down the fast sync server.");
            break;
        }
    }

    Ok(())
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
