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

use crate::{Bucket, permissions::Permission};
use snarkvm::{ledger::block::Block, prelude::*};

use colored::Colorize;
use core::marker::PhantomData;
use futures::StreamExt;
use sha2::{Digest, Sha256};
use std::time::Instant;

/// The name of the directory containing blocks in the bucket.
pub const DIRECTORY_NAME: &str = "v0/";

/// The number of blocks to store in each chunk.
pub const NUM_BLOCKS_PER_CHUNK: u32 = 50;

#[derive(Deserialize, Serialize, Debug)]
pub struct LatestState<N: Network> {
    pub exclusive_height: u32,
    pub inclusive_height: u32,
    pub hash: N::BlockHash,
}

#[derive(Clone)]
pub struct BlocksBucket<N: Network> {
    /// The storage bucket.
    bucket: Bucket,
    /// PhantomData.
    _phantom: PhantomData<N>,
}

impl<N: Network> BlocksBucket<N> {
    /// Initializes a new blocks bucket.
    pub fn new(bucket: Bucket) -> Self {
        Self { bucket, _phantom: PhantomData }
    }

    /// Returns the bucket name.
    pub fn name(&self) -> &str {
        self.bucket.name()
    }

    /// Fetches the latest block height.
    pub async fn latest_height(&self) -> Result<u32> {
        // List all of the files.
        let files = self.bucket.list_files().await?;

        // Parse the block heights.
        let block_heights = files
            .iter()
            .filter(|file| file.file_path.starts_with(DIRECTORY_NAME))
            .filter(|file| file.file_path.ends_with(".blocks"))
            .map(|file| {
                let sanitized_path = file.file_path.replace(DIRECTORY_NAME, "").replace(".blocks", "");
                let block_range: Vec<_> = sanitized_path.split('.').collect();

                block_range[1].parse::<u32>()
            })
            .collect::<Result<Vec<_>, _>>()?;

        // Find the latest height of the stored blocks.
        let latest_height = block_heights.iter().max().cloned().unwrap_or_default();

        // Return the latest height.
        Ok(latest_height)
    }

    /// Update the latest state of the bucket.
    pub async fn update_latest_state(&self, latest_block: &Block<N>) -> Result<()> {
        // Prepare the latest state file path.
        let latest_state_file_path = format!("{DIRECTORY_NAME}latest.json");

        // Prepare the latest state.
        let latest_state = LatestState::<N> {
            exclusive_height: latest_block.height().saturating_add(1),
            inclusive_height: latest_block.height(),
            hash: latest_block.hash(),
        };

        ensure!(
            latest_state.exclusive_height.is_multiple_of(NUM_BLOCKS_PER_CHUNK),
            "The exclusive_height must be a multiple of {}",
            NUM_BLOCKS_PER_CHUNK
        );

        // Convert the latest state to JSON.
        let json_object = serde_json::to_string(&latest_state)?;
        let json_bytes = bincode::serialize(&json_object)?;

        if self.bucket.exists(&latest_state_file_path).await {
            info!("Updating {latest_state_file_path} to the bucket.");
        } else {
            info!("Inserting {latest_state_file_path} to the bucket.");
        }

        self.bucket.insert_or_update(&latest_state_file_path, &json_bytes, true).await
    }

    /// Fetch the latest state of the bucket.
    pub async fn get_latest_state(&self) -> Result<LatestState<N>> {
        // Prepare the latest state file path.
        let latest_state_file_path = format!("{DIRECTORY_NAME}latest.json");

        debug!("Fetching {latest_state_file_path} from the bucket.");
        let latest_state_bytes = self.bucket.get(&latest_state_file_path).await?;
        let json_string: String = bincode::deserialize(&latest_state_bytes)?;

        // Return the result
        serde_json::from_str(&json_string).map_err(Into::into)
    }

    /// Returns `true` if the given block height exists in the bucket.
    pub async fn contains_block(&self, height: u32) -> bool {
        let height_start = height - height % NUM_BLOCKS_PER_CHUNK;
        let height_end = height_start + NUM_BLOCKS_PER_CHUNK;

        // Prepare the block file path.
        let file_path = format!("{DIRECTORY_NAME}{height_start}.{height_end}.blocks");

        // Return the result.
        self.bucket.exists(&file_path).await
    }

    /// Fetches the block chunk with the given starting block height from the bucket.
    pub async fn get_blocks(&self, start_height: u32) -> Result<Vec<Block<N>>> {
        let block_bytes = self.get_blocks_bytes(start_height).await?;
        let blocks: Vec<Block<N>> = bincode::deserialize(&block_bytes)?;

        ensure!(blocks.len() == NUM_BLOCKS_PER_CHUNK as usize, "Blocks must be in chunks of {NUM_BLOCKS_PER_CHUNK}");

        // Ensure that the blocks are sequential.
        let mut previous_block_height = blocks[0].height();
        let mut previous_block_hash = blocks[0].hash();
        for block in blocks.iter().skip(1) {
            ensure!(block.height() == previous_block_height.saturating_add(1), "Blocks must be sequential");
            ensure!(block.previous_hash() == previous_block_hash, "Block hashes must be sequential");
            previous_block_height = previous_block_height.saturating_add(1);
            previous_block_hash = block.hash();
        }

        Ok(blocks)
    }

    /// Fetches the block chunk's bytes with the given starting block height from the bucket.
    pub async fn get_blocks_bytes(&self, start_height: u32) -> Result<Vec<u8>> {
        ensure!(
            start_height.is_multiple_of(NUM_BLOCKS_PER_CHUNK),
            "Blocks must start at a multiple of {NUM_BLOCKS_PER_CHUNK}"
        );

        let end_height = start_height + NUM_BLOCKS_PER_CHUNK;

        // Prepare the blocks file path.
        let blocks_file_path = format!("{DIRECTORY_NAME}{}.{}.blocks", start_height, end_height);
        // Prepare the blocks checksum file path.
        let blocks_checksum_file_path = format!("{}.sum", blocks_file_path);

        debug!("Fetching {blocks_file_path} from the bucket.");
        let timer_1 = Instant::now();
        let block_bytes = self.bucket.get(&blocks_file_path).await?;
        debug!("\tFetching {blocks_checksum_file_path} from the bucket.");
        let timer_2 = Instant::now();
        let blocks_checksum = self.bucket.get(&blocks_checksum_file_path).await?;
        debug!(
            "\tFetched {blocks_file_path} in {}ms and {blocks_checksum_file_path} in {}ms",
            timer_1.elapsed().as_millis(),
            timer_2.elapsed().as_millis()
        );

        // Check that the checksum is accurate.
        let timer_3 = Instant::now();
        ensure!(sha256(&block_bytes).to_vec() == blocks_checksum, "Blocks checksum does not match");
        debug!("\tVerified checksum in {}ms", timer_3.elapsed().as_millis());

        Ok(block_bytes)
    }

    /// Inserts the given blocks into the bucket.
    pub async fn insert_blocks(&self, blocks: &[Block<N>]) -> Result<()> {
        ensure!(blocks.len() == NUM_BLOCKS_PER_CHUNK as usize, "Blocks must be in chunks of {NUM_BLOCKS_PER_CHUNK}");
        ensure!(
            blocks[0].height().is_multiple_of(NUM_BLOCKS_PER_CHUNK),
            "Blocks must start at a multiple of {NUM_BLOCKS_PER_CHUNK}"
        );

        // Ensure that the blocks are sequential.
        let mut previous_block_height = blocks[0].height();
        let mut previous_block_hash = blocks[0].hash();
        for block in blocks.iter().skip(1) {
            ensure!(block.height() == previous_block_height.saturating_add(1), "Blocks must be sequential");
            ensure!(block.previous_hash() == previous_block_hash, "Block hashes must be sequential");
            previous_block_height = previous_block_height.saturating_add(1);
            previous_block_hash = block.hash();
        }

        // Specify the inclusive start height.
        let start_height = blocks[0].height();
        // Specify the exclusive end height.
        let end_height = previous_block_height.saturating_add(1);

        // Prepare the blocks.
        let block_bytes = bincode::serialize(blocks)?;
        // Prepare the blocks checksum.
        let checksum = sha256(&block_bytes);

        // Prepare the blocks file path.
        let blocks_file_path = format!("{DIRECTORY_NAME}{}.{}.blocks", start_height, end_height);
        // Prepare the blocks checksum file path.
        let blocks_checksum_file_path = format!("{}.sum", blocks_file_path);

        // Upload the blocks.
        info!("Inserting {blocks_file_path} to the bucket.");
        let timer = Instant::now();
        self.bucket.insert(&blocks_file_path, &block_bytes, true).await?;
        // Upload the blocks_checksum.
        info!("Inserting {blocks_checksum_file_path} to the bucket.");
        if let Err(err) = self.bucket.insert_or_update(&blocks_checksum_file_path, &checksum, true).await {
            warn!("Error uploading {blocks_checksum_file_path}. Removing {blocks_file_path} from the bucket...");
            self.bucket.remove(&blocks_file_path).await?;

            return Err(err);
        }
        debug!("\tInserted objects to the bucket in {}ms", timer.elapsed().as_millis());

        // Return.
        Ok(())
    }

    /// Removes a single block chunk from the bucket.
    pub async fn remove_blocks_chunk(&self, start_height: u32) -> Result<()> {
        ensure!(
            start_height.is_multiple_of(NUM_BLOCKS_PER_CHUNK),
            "Blocks must start at a multiple of {NUM_BLOCKS_PER_CHUNK}"
        );

        let end_height = start_height + NUM_BLOCKS_PER_CHUNK;

        // Prepare the blocks file path.
        let blocks_file_path = format!("{DIRECTORY_NAME}{}.{}.blocks", start_height, end_height);
        // Prepare the blocks checksum file path.
        let blocks_checksum_file_path = format!("{}.sum", blocks_file_path);

        info!("Removing {blocks_file_path} from the bucket.");
        self.bucket.remove(&blocks_file_path).await?;
        info!("Removing {blocks_checksum_file_path} from the bucket.");
        self.bucket.remove(&blocks_checksum_file_path).await?;

        Ok(())
    }

    /// Updates the permission of each file in the bucket.
    pub async fn update_file_permissions(&self, permission: Permission) -> Result<()> {
        /// The number of concurrent requests with the network.
        const CONCURRENT_REQUESTS: usize = 100;

        // List all of the files.
        let files = self.bucket.list_files().await?;
        trace!("{:#?} block files", files.len());

        // Start a timer.
        let timer = std::time::Instant::now();

        let total = files.len();

        // Update the permission for each file.
        futures::stream::iter(files.iter().enumerate())
            .map(|(index, file)| {
                let bucket = &self.bucket;
                let bucket_name = self.bucket.name().to_string();
                async move {
                    // Update the file permissions.
                    (index, bucket.update_file_acl(bucket_name, file.file_path.clone(), permission).await)
                }
            })
            .buffered(CONCURRENT_REQUESTS)
            .for_each(|(index, _)| {
                let index = index + 1;
                // Compute the percentage completed.
                let percentage = index * 100 / total;
                // Compute the time remaining (in seconds).
                let time_remaining = (timer.elapsed().as_secs() + 1) * (total - index) as u64 / index as u64;
                // Prepare the estimate message.
                let estimate = format!("(est. {} minutes remaining)", time_remaining / 60u64);
                // Log the progress.
                info!(
                    "Updated permissions for {index} of {total} files - {percentage}% complete {}",
                    estimate.dimmed()
                );

                // Return.
                futures::future::ready(())
            })
            .await;

        Ok(())
    }

    // /// Returns a list of objects in the given bucket starting from the given key.
    // pub async fn list_blocks_after_height(
    //     &self,
    //     height: u32,
    //     continuation_token: Option<String>,
    // ) -> Result<ListObjectsV2Output> {
    //     self.bucket.list_objects_after_key(format!("testnet3/{height}"), continuation_token).await
    // }
}

pub fn sha256(data: &[u8]) -> [u8; 32] {
    let digest = Sha256::digest(data);
    let mut ret = [0u8; 32];
    ret.copy_from_slice(&digest);
    ret
}
