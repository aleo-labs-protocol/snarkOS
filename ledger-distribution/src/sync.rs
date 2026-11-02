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

use crate::{BlocksBucket, LedgerReader, NUM_BLOCKS_PER_CHUNK};
use snarkvm::{ledger::block::Block, prelude::*};

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

/// Detect and recover from a blockchain rollback.
async fn detect_and_recover_from_rollback<N: Network>(
    blocks_bucket: &BlocksBucket<N>,
    ledger: &LedgerReader<N>,
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
        let ledger_block = ledger.get_block(bucket_block_height).await?;

        if bucket_block.hash() == ledger_block.hash() {
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

/// Sync the bucket blocks with the ledger.
///
/// `genesis` is the block the bucket must start with when it is empty.
pub async fn sync_with_ledger<N: Network>(
    blocks_bucket: &BlocksBucket<N>,
    ledger: &LedgerReader<N>,
    genesis: &Block<N>,
    shutdown_started: Arc<AtomicBool>,
    dry_run: bool,
) -> Result<()> {
    // Read the blocks the node has written since the last pass.
    ledger.catch_up().await?;

    info!("Fetching latest ledger height...");
    // Get the latest height of the ledger.
    let latest_ledger_height = ledger.latest_height().await?;

    info!("Fetching latest bucket height...\n");
    // Get the latest height of the stored blocks.
    let latest_bucket_height = blocks_bucket.latest_height().await?;

    info!("Latest bucket height {}, latest ledger height {}\n", latest_bucket_height, latest_ledger_height);

    // Detect and recover from any blockchain rollback
    let latest_bucket_height =
        detect_and_recover_from_rollback::<N>(blocks_bucket, ledger, latest_bucket_height, dry_run).await?;

    info!("Latest bucket height after rollback check: {}\n", latest_bucket_height);

    // Update the blocks bucket with the latest blocks from the ledger.
    let mut start_height = latest_bucket_height;
    ensure!(
        start_height.is_multiple_of(NUM_BLOCKS_PER_CHUNK),
        "The latest block height must be a multiple of {NUM_BLOCKS_PER_CHUNK}"
    );

    // Fetch the latest chunk of blocks from the bucket.
    let mut latest_block_in_bucket = match start_height {
        0 => genesis.clone(),
        height => {
            let blocks = blocks_bucket.get_blocks(height.saturating_sub(NUM_BLOCKS_PER_CHUNK)).await?;
            blocks[blocks.len() - 1].clone()
        }
    };

    info!("Uploading blocks to bucket...\n");
    // Upload the blocks chunks to the S3 bucket.
    while start_height <= latest_ledger_height {
        info!("Latest bucket height {}, latest ledger height {}", start_height, latest_ledger_height);
        // Fetch the end height of the block range.
        let end_height = start_height + NUM_BLOCKS_PER_CHUNK;

        if end_height > latest_ledger_height {
            info!(
                "Block bucket is up to date with the ledger. (To the closest block chunk of {})",
                NUM_BLOCKS_PER_CHUNK
            );
            break;
        }

        // Fetch the blocks from the ledger.
        let blocks = ledger.get_blocks(start_height, end_height).await?;
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
        debug!("Checking that the bucket and ledger blocks match...");
        ensure!(blocks == blocks_from_bucket, "The blocks from the bucket do not match the blocks from the ledger");

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
