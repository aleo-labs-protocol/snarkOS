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

use snarkvm::{
    ledger::{
        block::Block,
        store::{BlockStorage, helpers::rocksdb::BlockDB},
    },
    prelude::*,
};

use aleo_std::StorageMode;
use std::path::PathBuf;

/// A read-only view of a node's ledger.
///
/// The ledger database is opened as a RocksDB secondary instance, so it can be read while the
/// node keeps writing to it, and nothing here can write to it.
#[derive(Clone)]
pub struct LedgerReader<N: Network> {
    storage: BlockDB<N>,
}

impl<N: Network> LedgerReader<N> {
    /// Opens the ledger database at `ledger_path` read-only.
    ///
    /// RocksDB keeps the secondary instance's own files in `secondary_path`, which must not be
    /// the ledger directory itself.
    ///
    /// HAZARD: snarkVM allows one open ledger database per process, so this cannot be opened in a
    /// process that also opens a ledger of its own.
    pub fn open(ledger_path: impl Into<PathBuf>, secondary_path: impl Into<PathBuf>) -> Result<Self> {
        let storage = BlockDB::<N>::open(StorageMode::Custom(ledger_path.into(), Some(secondary_path.into())))?;
        Ok(Self { storage })
    }

    /// Makes the blocks the node has written since the last call visible.
    pub async fn catch_up(&self) -> Result<()> {
        self.blocking(|storage| storage.catch_up_with_primary()).await
    }

    /// Returns the height of the latest block in the ledger.
    pub async fn latest_height(&self) -> Result<u32> {
        self.blocking(|storage| {
            ensure!(storage.get_block_hash(0)?.is_some(), "The ledger has no genesis block");

            // The heights in the ledger are contiguous from the genesis block, so the latest
            // height is found by doubling past it and then bisecting.
            let (mut present, mut absent) = (0u32, 1u32);
            while storage.get_block_hash(absent)?.is_some() {
                present = absent;
                absent = absent.checked_mul(2).ok_or_else(|| anyhow!("The ledger height overflows"))?;
            }
            while absent - present > 1 {
                let middle = present + (absent - present) / 2;
                match storage.get_block_hash(middle)?.is_some() {
                    true => present = middle,
                    false => absent = middle,
                }
            }
            Ok(present)
        })
        .await
    }

    /// Returns the blocks from `start_height` (inclusive) to `end_height` (exclusive).
    pub async fn get_blocks(&self, start_height: u32, end_height: u32) -> Result<Vec<Block<N>>> {
        self.blocking(move |storage| (start_height..end_height).map(|height| get_block(storage, height)).collect())
            .await
    }

    /// Returns the block at the given height.
    pub async fn get_block(&self, height: u32) -> Result<Block<N>> {
        self.blocking(move |storage| get_block(storage, height)).await
    }

    /// Runs `f` on a blocking thread, since RocksDB reads block.
    async fn blocking<T: Send + 'static>(
        &self,
        f: impl FnOnce(&BlockDB<N>) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let storage = self.storage.clone();
        tokio::task::spawn_blocking(move || f(&storage)).await?
    }
}

/// Returns the block at the given height from the ledger database.
fn get_block<N: Network>(storage: &BlockDB<N>, height: u32) -> Result<Block<N>> {
    let hash = storage.get_block_hash(height)?.ok_or_else(|| anyhow!("The ledger has no block at height {height}"))?;
    storage.get_block(&hash)?.ok_or_else(|| anyhow!("The ledger has no block with hash {hash} (height {height})"))
}
