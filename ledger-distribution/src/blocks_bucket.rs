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

/// The path of the file that describes the bucket's highest chunk.
const LATEST_STATE_PATH: &str = "v0/latest.json";

#[derive(Deserialize, Serialize, Debug, PartialEq, Eq)]
pub struct LatestState<N: Network> {
    pub exclusive_height: u32,
    pub inclusive_height: u32,
    pub hash: N::BlockHash,
}

impl<N: Network> LatestState<N> {
    /// Returns the state of a bucket whose highest chunk ends with `latest_block`.
    pub fn from_block(latest_block: &Block<N>) -> Self {
        Self {
            exclusive_height: latest_block.height().saturating_add(1),
            inclusive_height: latest_block.height(),
            hash: latest_block.hash(),
        }
    }
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

/// Returns the path of the `.blocks` file of the chunk starting at `start_height`. Its checksum is
/// at the same path with `.sum` appended.
pub fn chunk_path(start_height: u32) -> String {
    format!("{DIRECTORY_NAME}{}.{}.blocks", start_height, start_height + NUM_BLOCKS_PER_CHUNK)
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

    let start = range.split_once('.').and_then(|(start, _)| start.parse::<u32>().ok());
    match start {
        Some(start)
            if start.is_multiple_of(NUM_BLOCKS_PER_CHUNK)
                && start.checked_add(NUM_BLOCKS_PER_CHUNK).is_some()
                && path.strip_suffix(".sum").unwrap_or(path) == chunk_path(start) =>
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
                if is_checksum {
                    files.checksum = true;
                } else {
                    files.blocks = true;
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
        info!("Removing {LATEST_STATE_PATH} from the bucket.");
        self.bucket.remove(LATEST_STATE_PATH).await
    }

    /// Update the latest state of the bucket.
    pub async fn update_latest_state(&self, latest_block: &Block<N>) -> Result<()> {
        let latest_state = LatestState::<N>::from_block(latest_block);

        ensure!(
            latest_state.exclusive_height.is_multiple_of(NUM_BLOCKS_PER_CHUNK),
            "The exclusive_height must be a multiple of {}",
            NUM_BLOCKS_PER_CHUNK
        );

        // Convert the latest state to JSON.
        let json_object = serde_json::to_string(&latest_state)?;
        let json_bytes = bincode::serialize(&json_object)?;

        if self.bucket.exists(LATEST_STATE_PATH).await? {
            info!("Updating {LATEST_STATE_PATH} to the bucket.");
        } else {
            info!("Inserting {LATEST_STATE_PATH} to the bucket.");
        }

        self.bucket.insert_or_update(LATEST_STATE_PATH, &json_bytes).await
    }

    /// Fetch the latest state of the bucket.
    pub async fn get_latest_state(&self) -> Result<LatestState<N>> {
        debug!("Fetching {LATEST_STATE_PATH} from the bucket.");
        let latest_state_bytes = self.bucket.get(LATEST_STATE_PATH).await?;
        Self::decode_latest_state(&latest_state_bytes)
    }

    /// Returns `true` if `latest.json` holds `expected`, or does not exist when `expected` is `None`.
    pub async fn latest_state_is(&self, expected: Option<&LatestState<N>>) -> Result<bool> {
        Ok(match self.bucket.get_if_exists(LATEST_STATE_PATH).await? {
            Some(bytes) => Self::decode_latest_state(&bytes).is_ok_and(|state| Some(&state) == expected),
            None => expected.is_none(),
        })
    }

    fn decode_latest_state(bytes: &[u8]) -> Result<LatestState<N>> {
        let json_string: String = bincode::deserialize(bytes)?;
        serde_json::from_str(&json_string).map_err(Into::into)
    }

    /// Returns `true` if the given block height exists in the bucket.
    pub async fn contains_block(&self, height: u32) -> Result<bool> {
        let height_start = height - height % NUM_BLOCKS_PER_CHUNK;
        let file_path = chunk_path(height_start);

        // Return the result.
        self.bucket.exists(&file_path).await
    }

    /// Fetches the block chunk with the given starting block height from the bucket.
    pub async fn get_blocks(&self, start_height: u32) -> Result<Vec<Block<N>>> {
        let block_bytes = self.get_blocks_bytes(start_height).await?;
        Self::decode_blocks(&block_bytes)
    }

    /// Fetches the block chunk with the given starting block height from the bucket, or returns
    /// `None` if what it holds does not match its checksum or does not decode. Failing to fetch
    /// the chunk is an error.
    pub async fn get_blocks_if_valid(&self, start_height: u32) -> Result<Option<Vec<Block<N>>>> {
        let (block_bytes, blocks_checksum) = self.fetch_chunk_files(start_height).await?;
        let blocks = check_checksum(&block_bytes, &blocks_checksum).and_then(|()| Self::decode_blocks(&block_bytes));
        Ok(blocks.inspect_err(|error| warn!("The chunk at height {start_height} cannot be read: {error}")).ok())
    }

    fn decode_blocks(block_bytes: &[u8]) -> Result<Vec<Block<N>>> {
        let blocks: Vec<Block<N>> = bincode::deserialize(block_bytes)?;

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
        let (block_bytes, blocks_checksum) = self.fetch_chunk_files(start_height).await?;

        // Check that the checksum is accurate.
        let timer = Instant::now();
        check_checksum(&block_bytes, &blocks_checksum)?;
        debug!("\tVerified checksum in {}ms", timer.elapsed().as_millis());

        Ok(block_bytes)
    }

    /// Fetches the contents of the `.blocks` and `.blocks.sum` files of the given chunk.
    async fn fetch_chunk_files(&self, start_height: u32) -> Result<(Vec<u8>, Vec<u8>)> {
        ensure!(
            start_height.is_multiple_of(NUM_BLOCKS_PER_CHUNK),
            "Blocks must start at a multiple of {NUM_BLOCKS_PER_CHUNK}"
        );

        let blocks_file_path = chunk_path(start_height);
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

        Ok((block_bytes, blocks_checksum))
    }

    /// Writes the given blocks into the bucket, replacing any files of their chunk already there.
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

        // Prepare the blocks.
        let block_bytes = bincode::serialize(blocks)?;
        // Prepare the blocks checksum.
        let checksum = sha256(&block_bytes);

        let blocks_file_path = chunk_path(start_height);
        // Prepare the blocks checksum file path.
        let blocks_checksum_file_path = format!("{}.sum", blocks_file_path);

        // Upload the blocks.
        info!("Inserting {blocks_file_path} to the bucket.");
        let timer = Instant::now();
        self.bucket.insert_or_update(&blocks_file_path, &block_bytes).await?;
        // Upload the blocks_checksum.
        info!("Inserting {blocks_checksum_file_path} to the bucket.");
        self.bucket.insert_or_update(&blocks_checksum_file_path, &checksum).await?;
        debug!("\tInserted objects to the bucket in {}ms", timer.elapsed().as_millis());

        // Return.
        Ok(())
    }
}

fn check_checksum(block_bytes: &[u8], blocks_checksum: &[u8]) -> Result<()> {
    ensure!(sha256(block_bytes).as_slice() == blocks_checksum, "Blocks checksum does not match");
    Ok(())
}

pub fn sha256(data: &[u8]) -> [u8; 32] {
    let digest = Sha256::digest(data);
    let mut ret = [0u8; 32];
    ret.copy_from_slice(&digest);
    ret
}

#[cfg(test)]
mod tests {
    use super::parse_chunk_path;

    #[test]
    fn parse_chunk_path_takes_only_whole_chunks() {
        assert_eq!(parse_chunk_path("v0/50.100.blocks").unwrap(), Some((50, false)));
        assert_eq!(parse_chunk_path("v0/50.100.blocks.sum").unwrap(), Some((50, true)));
        assert_eq!(parse_chunk_path("v0/latest.json").unwrap(), None);
        assert_eq!(parse_chunk_path("other/foo.blocks").unwrap(), None);

        for path in [
            "v0/foo.blocks",
            "v0/50.blocks",
            "v0/50.99.blocks",
            "v0/51.101.blocks",
            "v0/050.100.blocks",
            "v0/+50.100.blocks.sum",
            "v0/50.0100.blocks",
            "v0/4294967250.4294967300.blocks",
        ] {
            assert!(parse_chunk_path(path).is_err(), "{path} was accepted");
        }
    }
}
