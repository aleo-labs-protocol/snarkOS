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

use crate::{BlocksBucket, LatestState, LedgerReader, NUM_BLOCKS_PER_CHUNK};
use snarkvm::{ledger::block::Block, prelude::*};

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

/// Replaces the chunk starting at `start_height` with the ledger's blocks.
async fn reupload_chunk<N: Network>(
    blocks_bucket: &BlocksBucket<N>,
    ledger: &LedgerReader<N>,
    start_height: u32,
) -> Result<()> {
    blocks_bucket.remove_blocks_chunk(start_height).await?;
    let blocks = ledger.get_blocks(start_height, start_height + NUM_BLOCKS_PER_CHUNK).await?;
    blocks_bucket.insert_blocks(&blocks).await?;
    ensure!(
        blocks_bucket.get_blocks(start_height).await? == blocks,
        "The blocks from the bucket do not match the blocks from the ledger"
    );
    Ok(())
}

/// Repairs what an interrupted pass or a rollback of the ledger leaves in the bucket, and returns
/// the exclusive height up to which the bucket then holds complete chunks that match the ledger.
///
/// The repairs, in order:
/// - A chunk that is missing, or missing either of its files, below the bucket's highest chunk
///   file is uploaded again if the ledger has its blocks, and has any remaining file removed
///   otherwise.
/// - The highest chunk is uploaded again if its blocks cannot be read back.
/// - Chunks that no longer match the ledger are removed, from the highest down, once `latest.json`
///   no longer points at them.
///
/// With `dry_run`, it reports the first repair it would make instead, and returns `None`.
async fn reconcile_bucket<N: Network>(
    blocks_bucket: &BlocksBucket<N>,
    ledger: &LedgerReader<N>,
    latest_ledger_height: u32,
    dry_run: bool,
) -> Result<Option<u32>> {
    let ledger_has_chunk = |start_height: u32| start_height + NUM_BLOCKS_PER_CHUNK <= latest_ledger_height + 1;

    let chunks = blocks_bucket.chunks().await?;
    let top = chunks.last_key_value().map(|(start_height, _)| start_height + NUM_BLOCKS_PER_CHUNK).unwrap_or(0);
    for start_height in (0..top).step_by(NUM_BLOCKS_PER_CHUNK as usize) {
        let files = chunks.get(&start_height).copied().unwrap_or_default();
        if files.is_complete() {
            continue;
        }
        warn!("The chunk at height {start_height} is incomplete ({files:?})");
        if dry_run {
            warn!("DRY-RUN: would replace it");
            return Ok(None);
        }
        match ledger_has_chunk(start_height) {
            true => reupload_chunk(blocks_bucket, ledger, start_height).await?,
            false => blocks_bucket.remove_blocks_chunk(start_height).await?,
        }
    }

    // Uploads are the only writes that can stop partway, and the highest chunk is the last one uploaded.
    let mut top = blocks_bucket.latest_height().await?;
    if top > 0 {
        let start_height = top - NUM_BLOCKS_PER_CHUNK;
        if let Err(error) = blocks_bucket.get_blocks(start_height).await {
            warn!("The chunk at height {start_height} cannot be read back: {error}");
            if dry_run {
                warn!("DRY-RUN: would replace it");
                return Ok(None);
            }
            match ledger_has_chunk(start_height) {
                true => reupload_chunk(blocks_bucket, ledger, start_height).await?,
                false => {
                    blocks_bucket.remove_blocks_chunk(start_height).await?;
                    top = start_height;
                }
            }
        }
    }

    info!("Checking for blockchain rollback...");

    // The start heights of the chunks that no longer match the ledger, highest first.
    let mut stale_chunks = Vec::new();
    // The last block of the highest chunk that matches the ledger.
    let mut fork_block = None;
    let mut fork_height = top;
    while fork_height > 0 {
        let start_height = fork_height - NUM_BLOCKS_PER_CHUNK;
        let blocks = blocks_bucket.get_blocks(start_height).await?;
        let bucket_block = blocks.last().unwrap();
        let ledger_block = ledger.get_block(bucket_block.height()).await?;

        if bucket_block.hash() == ledger_block.hash() {
            info!("Found fork point at height {}", bucket_block.height());
            fork_block = Some(bucket_block.clone());
            break;
        }

        warn!("Hash mismatch at height {}.", bucket_block.height());
        stale_chunks.push(start_height);
        fork_height = start_height;
    }

    if stale_chunks.is_empty() {
        return Ok(Some(top));
    }

    if dry_run {
        warn!("DRY-RUN: would remove {} stale chunk(s):", stale_chunks.len());
        for start_height in &stale_chunks {
            warn!("  - v0/{}.{}.blocks", start_height, start_height + NUM_BLOCKS_PER_CHUNK);
        }
        warn!("DRY-RUN: latest bucket height would change from {top} to {fork_height}");
        return Ok(None);
    }

    // Lowered first, so that `latest.json` never points at a removed chunk.
    match &fork_block {
        Some(block) => blocks_bucket.update_latest_state(block).await?,
        None => blocks_bucket.remove_latest_state().await?,
    }
    // From the highest down, so that an interruption leaves the remaining chunks unbroken.
    info!("Removing {} stale chunk(s)...", stale_chunks.len());
    for start_height in stale_chunks {
        blocks_bucket.remove_blocks_chunk(start_height).await?;
    }
    info!("New latest bucket height: {fork_height}");

    Ok(Some(fork_height))
}

/// Makes `latest.json` describe the bucket, whose chunks end at `exclusive_height` with
/// `latest_block`, or removes it if the bucket has no chunks.
async fn reconcile_latest_state<N: Network>(
    blocks_bucket: &BlocksBucket<N>,
    exclusive_height: u32,
    latest_block: &Block<N>,
    dry_run: bool,
) -> Result<()> {
    if exclusive_height == 0 {
        if !dry_run {
            blocks_bucket.remove_latest_state().await?;
        }
        return Ok(());
    }

    let expected =
        LatestState::<N> { exclusive_height, inclusive_height: latest_block.height(), hash: latest_block.hash() };
    let current = blocks_bucket.get_latest_state().await.ok();
    if current.as_ref() == Some(&expected) {
        return Ok(());
    }

    warn!("latest.json is {current:?}, but the bucket ends at height {exclusive_height}");
    if dry_run {
        warn!("DRY-RUN: would correct it");
        return Ok(());
    }
    blocks_bucket.update_latest_state(latest_block).await
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

    info!("Checking the bucket against the ledger...\n");
    let Some(latest_bucket_height) = reconcile_bucket(blocks_bucket, ledger, latest_ledger_height, dry_run).await?
    else {
        return Ok(());
    };

    info!("Latest bucket height {}, latest ledger height {}\n", latest_bucket_height, latest_ledger_height);

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

    // An interruption between uploading a chunk and updating `latest.json` leaves it behind the
    // bucket, and nothing else corrects it until another chunk is uploaded.
    reconcile_latest_state(blocks_bucket, start_height, &latest_block_in_bucket, dry_run).await
}
