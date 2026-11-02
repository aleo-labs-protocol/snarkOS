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

use crate::Bucket;
use snarkvm::{ledger::block::Block, prelude::*};

use core::marker::PhantomData;
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, time::Instant};

/// The name of the directory containing blocks in the bucket.
pub const DIRECTORY_NAME: &str = "v0/";

/// The number of blocks to store in each chunk.
pub const NUM_BLOCKS_PER_CHUNK: u32 = 50;

#[derive(Deserialize, Serialize, Debug, PartialEq, Eq)]
pub struct LatestState<N: Network> {
    pub exclusive_height: u32,
    pub inclusive_height: u32,
    pub hash: N::BlockHash,
}

/// The files of one chunk that are present in the bucket.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub struct ChunkFiles {
    /// The `.blocks` file, holding the chunk's blocks.
    pub blocks: bool,
    /// The `.blocks.sum` file, holding the checksum of the `.blocks` file.
    pub checksum: bool,
}

impl ChunkFiles {
    /// Returns `true` if both of the chunk's files are present.
    pub fn is_complete(&self) -> bool {
        self.blocks && self.checksum
    }
}

/// Returns the start height of the chunk that a `.blocks` or `.blocks.sum` path belongs to, and
/// whether the path is the checksum, or `None` for any other path.
///
/// A chunk path that does not name a whole chunk, such as `v0/foo.blocks`, is an error.
fn parse_chunk_path(path: &str) -> Result<Option<(u32, bool)>> {
    let Some(name) = path.strip_prefix(DIRECTORY_NAME) else {
        return Ok(None);
    };
    let (range, is_checksum) = match (name.strip_suffix(".blocks.sum"), name.strip_suffix(".blocks")) {
        (Some(range), _) => (range, true),
        (None, Some(range)) => (range, false),
        (None, None) => return Ok(None),
    };

    let parsed =
        range.split_once('.').and_then(|(start, end)| Some((start.parse::<u32>().ok()?, end.parse::<u32>().ok()?)));
    match parsed {
        Some((start, end))
            if start.is_multiple_of(NUM_BLOCKS_PER_CHUNK) && Some(end) == start.checked_add(NUM_BLOCKS_PER_CHUNK) =>
        {
            Ok(Some((start, is_checksum)))
        }
        _ => bail!("Unexpected chunk file in the bucket: '{path}'"),
    }
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

    /// Returns the files present for each chunk in the bucket, by the chunk's start height.
    pub async fn chunks(&self) -> Result<BTreeMap<u32, ChunkFiles>> {
        let mut chunks = BTreeMap::<u32, ChunkFiles>::new();
        for file in self.bucket.list_files().await? {
            if let Some((start_height, is_checksum)) = parse_chunk_path(&file.file_path)? {
                let files = chunks.entry(start_height).or_default();
                match is_checksum {
                    true => files.checksum = true,
                    false => files.blocks = true,
                }
            }
        }
        Ok(chunks)
    }

    /// Fetches the latest block height: the exclusive end height of the highest `.blocks` file.
    pub async fn latest_height(&self) -> Result<u32> {
        let chunks = self.chunks().await?;
        Ok(chunks
            .iter()
            .rev()
            .find(|(_, files)| files.blocks)
            .map(|(start_height, _)| start_height + NUM_BLOCKS_PER_CHUNK)
            .unwrap_or_default())
    }

    /// Removes `latest.json`, if it exists.
    pub async fn remove_latest_state(&self) -> Result<()> {
        let latest_state_file_path = format!("{DIRECTORY_NAME}latest.json");
        info!("Removing {latest_state_file_path} from the bucket.");
        self.bucket.remove(&latest_state_file_path).await
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

        if self.bucket.exists(&latest_state_file_path).await? {
            info!("Updating {latest_state_file_path} to the bucket.");
        } else {
            info!("Inserting {latest_state_file_path} to the bucket.");
        }

        self.bucket.insert_or_update(&latest_state_file_path, &json_bytes).await
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
    pub async fn contains_block(&self, height: u32) -> Result<bool> {
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
        self.bucket.insert(&blocks_file_path, &block_bytes).await?;
        // Upload the blocks_checksum.
        info!("Inserting {blocks_checksum_file_path} to the bucket.");
        if let Err(err) = self.bucket.insert_or_update(&blocks_checksum_file_path, &checksum).await {
            warn!("Error uploading {blocks_checksum_file_path}. Removing {blocks_file_path} from the bucket...");
            self.bucket.remove(&blocks_file_path).await?;

            return Err(err);
        }
        debug!("\tInserted objects to the bucket in {}ms", timer.elapsed().as_millis());

        // Return.
        Ok(())
    }

    /// Removes a single block chunk from the bucket: whichever of its `.blocks` and `.blocks.sum`
    /// files exist, in that order, so that a removal interrupted between them can be retried.
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
}

pub fn sha256(data: &[u8]) -> [u8; 32] {
    let digest = Sha256::digest(data);
    let mut ret = [0u8; 32];
    ret.copy_from_slice(&digest);
    ret
}
