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

use crate::{BlocksBucket, LatestState, LedgerReader, NUM_BLOCKS_PER_CHUNK, chunk_path};
use snarkvm::{ledger::block::Block, prelude::*};

use std::{
    ops::ControlFlow,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

/// Returns `true` if the ledger, whose latest block is at `latest_ledger_height`, is past the
/// chunk starting at `start_height`, which is when that chunk is uploaded.
fn ledger_has_chunk(start_height: u32, latest_ledger_height: u32) -> bool {
    start_height + NUM_BLOCKS_PER_CHUNK <= latest_ledger_height
}

/// Uploads a chunk of blocks, and checks that it reads back the same.
async fn upload_chunk<N: Network>(blocks_bucket: &BlocksBucket<N>, blocks: &[Block<N>]) -> Result<()> {
    blocks_bucket.insert_blocks(blocks).await?;
    debug!("Checking that the bucket and ledger blocks match...");
    ensure!(
        blocks_bucket.get_blocks(blocks[0].height()).await? == blocks,
        "The blocks from the bucket do not match the blocks from the ledger"
    );
    Ok(())
}

/// Replaces the chunk starting at `start_height` with the ledger's blocks, and returns them.
async fn reupload_chunk<N: Network>(
    blocks_bucket: &BlocksBucket<N>,
    ledger: &LedgerReader<N>,
    start_height: u32,
) -> Result<Vec<Block<N>>> {
    blocks_bucket.remove_blocks_chunk(start_height).await?;
    let blocks = ledger.get_blocks(start_height, start_height + NUM_BLOCKS_PER_CHUNK).await?;
    upload_chunk(blocks_bucket, &blocks).await?;
    Ok(blocks)
}

/// Writes `latest.json` for a bucket whose chunks end with `end_block`, or removes it if the
/// bucket has no chunks.
async fn write_latest_state<N: Network>(blocks_bucket: &BlocksBucket<N>, end_block: Option<&Block<N>>) -> Result<()> {
    match end_block {
        Some(block) => blocks_bucket.update_latest_state(block).await,
        None => blocks_bucket.remove_latest_state().await,
    }
}

/// Repairs what an interrupted pass or a rollback of the ledger leaves in the bucket.
///
/// The repairs, in order:
/// - A chunk that is missing, or missing either of its files, up to the bucket's highest chunk
///   file is uploaded again, as is a highest chunk that cannot be read back.
/// - Chunks that no longer match the ledger are removed, from the highest down, once `latest.json`
///   no longer points at them.
///
/// A bucket that goes past the ledger is left as it is until the ledger catches up. With
/// `dry_run`, the first repair needed is reported instead of made.
///
/// Returns the last block of the bucket's chunks, or `None` if it has none, or `Break` if the
/// pass stops here.
async fn reconcile_bucket<N: Network>(
    blocks_bucket: &BlocksBucket<N>,
    ledger: &LedgerReader<N>,
    latest_ledger_height: u32,
    dry_run: bool,
) -> Result<ControlFlow<(), Option<Block<N>>>> {
    let chunks = blocks_bucket.chunks().await?;
    let Some(top_start) = chunks.last_key_value().map(|(start_height, _)| *start_height) else {
        return Ok(ControlFlow::Continue(None));
    };
    if !ledger_has_chunk(top_start, latest_ledger_height) {
        warn!(
            "The bucket goes up to height {}, past the ledger at height {latest_ledger_height}; waiting for the ledger",
            top_start + NUM_BLOCKS_PER_CHUNK
        );
        return Ok(ControlFlow::Break(()));
    }

    let mut top_blocks = None;
    for start_height in (0..=top_start).step_by(NUM_BLOCKS_PER_CHUNK as usize) {
        let files = chunks.get(&start_height).copied().unwrap_or_default();
        if !files.is_complete() {
            warn!("The chunk at height {start_height} is incomplete ({files:?})");
        } else if start_height < top_start {
            continue;
        } else {
            // Checks that the highest chunk can be read back.
            match blocks_bucket.get_blocks(start_height).await {
                Ok(blocks) => {
                    top_blocks = Some(blocks);
                    continue;
                }
                Err(error) => warn!("The chunk at height {start_height} cannot be read back: {error}"),
            }
        }
        if dry_run {
            warn!("DRY-RUN: would upload it again");
            return Ok(ControlFlow::Break(()));
        }
        let blocks = reupload_chunk(blocks_bucket, ledger, start_height).await?;
        if start_height == top_start {
            top_blocks = Some(blocks);
        }
    }

    info!("Checking for blockchain rollback...");

    // The start heights of the chunks that no longer match the ledger, highest first.
    let mut stale_chunks = Vec::new();
    // The last block of the highest chunk that matches the ledger.
    let mut end_block = None;
    let mut next_blocks = top_blocks;
    let mut end_height = top_start + NUM_BLOCKS_PER_CHUNK;
    while end_height > 0 {
        let start_height = end_height - NUM_BLOCKS_PER_CHUNK;
        let blocks = match next_blocks.take() {
            Some(blocks) => blocks,
            None => blocks_bucket.get_blocks(start_height).await?,
        };
        let bucket_block = blocks.into_iter().last().unwrap();
        let ledger_block = ledger.get_block(bucket_block.height()).await?;

        if bucket_block.hash() == ledger_block.hash() {
            info!("Found fork point at height {}", bucket_block.height());
            end_block = Some(bucket_block);
            break;
        }

        warn!("Hash mismatch at height {}.", bucket_block.height());
        stale_chunks.push(start_height);
        end_height = start_height;
    }

    if !stale_chunks.is_empty() {
        if dry_run {
            warn!("DRY-RUN: would remove {} stale chunk(s):", stale_chunks.len());
            for start_height in &stale_chunks {
                warn!("  - {}", chunk_path(*start_height));
            }
            warn!(
                "DRY-RUN: latest bucket height would change from {} to {end_height}",
                top_start + NUM_BLOCKS_PER_CHUNK
            );
            return Ok(ControlFlow::Break(()));
        }

        // Lowered first, so that `latest.json` never points at a removed chunk.
        write_latest_state(blocks_bucket, end_block.as_ref()).await?;
        // From the highest down, so that an interruption leaves the remaining chunks unbroken.
        info!("Removing {} stale chunk(s)...", stale_chunks.len());
        for start_height in stale_chunks {
            blocks_bucket.remove_blocks_chunk(start_height).await?;
        }
        info!("New latest bucket height: {end_height}");
    }

    Ok(ControlFlow::Continue(end_block))
}

/// Makes `latest.json` describe a bucket whose chunks end with `end_block`, or removes it if the
/// bucket has no chunks.
async fn reconcile_latest_state<N: Network>(
    blocks_bucket: &BlocksBucket<N>,
    end_block: Option<&Block<N>>,
    dry_run: bool,
) -> Result<()> {
    let expected = end_block.map(LatestState::from_block);
    let current = blocks_bucket.get_latest_state().await.ok();
    if current == expected {
        return Ok(());
    }

    warn!("latest.json is {current:?}, but should be {expected:?}");
    if dry_run {
        warn!("DRY-RUN: would correct it");
        return Ok(());
    }
    write_latest_state(blocks_bucket, end_block).await
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
    let ControlFlow::Continue(end_block) =
        reconcile_bucket(blocks_bucket, ledger, latest_ledger_height, dry_run).await?
    else {
        return Ok(());
    };

    // Update the blocks bucket with the latest blocks from the ledger.
    let mut start_height = end_block.as_ref().map_or(0, |block| block.height() + 1);
    info!("Latest bucket height {}, latest ledger height {}\n", start_height, latest_ledger_height);

    // The last block in the bucket, or the genesis block the first chunk must start with.
    let mut latest_block_in_bucket = end_block.unwrap_or_else(|| genesis.clone());

    info!("Uploading blocks to bucket...\n");
    // Upload the blocks chunks to the S3 bucket.
    while start_height <= latest_ledger_height {
        info!("Latest bucket height {}, latest ledger height {}", start_height, latest_ledger_height);
        // Fetch the end height of the block range.
        let end_height = start_height + NUM_BLOCKS_PER_CHUNK;

        if !ledger_has_chunk(start_height, latest_ledger_height) {
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

        upload_chunk(blocks_bucket, &blocks).await?;

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

    reconcile_latest_state(blocks_bucket, (start_height > 0).then_some(&latest_block_in_bucket), dry_run).await
}
