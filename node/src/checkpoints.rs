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

use aleo_std::aleo_ledger_dir;
use anyhow::{Result, bail};
use snarkvm::prelude::{Ledger, Network, store::helpers::rocksdb::ConsensusDB};
use std::{
    cmp,
    fs,
    path::{Path, PathBuf},
    str::FromStr,
    time::Duration,
};
use tokio::task;

const CHECKPOINT_BLOCK_FREQUENCY: u32 = 1000;
const MAX_AUTO_CHECKPOINTS: usize = 5;

fn existing_startup_checkpoint_height(auto_checkpoint_path: &Path, startup_height: u32) -> Option<u32> {
    let checkpoint_path = auto_checkpoint_path.join(format!("checkpoint_{startup_height}"));
    checkpoint_path.is_dir().then_some(startup_height)
}

pub(crate) fn spawn_auto_checkpoints<N: Network>(
    ledger: Ledger<N, ConsensusDB<N>>,
    auto_checkpoint_path: PathBuf,
) -> Result<task::JoinHandle<()>> {
    // Ensure that the target path exists as a folder or create it.
    if !auto_checkpoint_path.exists() {
        if let Err(e) = fs::create_dir_all(&auto_checkpoint_path) {
            bail!("Couldn't create the specified path for the automatic ledger checkpoints: {e}");
        }
    } else if auto_checkpoint_path.exists() && !auto_checkpoint_path.is_dir() {
        bail!("The specified path for automatic ledger checkpoints is not a directory");
    }

    let startup_height = ledger.vm().block_store().current_block_height();
    let mut initialized = false;
    let mut last_checkpoint_height = None;
    let mut existing_checkpoints = Vec::with_capacity(MAX_AUTO_CHECKPOINTS + 1);
    let mut block_tree_path = aleo_ledger_dir(N::ID, ledger.vm().block_store().storage_mode());
    block_tree_path.push("block_tree");

    Ok(spawn_checkpoint_loop(move || {
        if !initialized {
            last_checkpoint_height = existing_startup_checkpoint_height(&auto_checkpoint_path, startup_height);
            initialized = true;
        }
        // Skip if we've already created a checkpoint during this run, and the
        // number of blocks baked since then is lower than the configured threshold.
        let current_height = ledger.vm().block_store().current_block_height();
        if last_checkpoint_height.is_some_and(|checkpoint_height| {
            current_height.saturating_sub(checkpoint_height) < CHECKPOINT_BLOCK_FREQUENCY
        }) {
            return;
        }

        // Create a checkpoint.
        let mut checkpoint_path = auto_checkpoint_path.clone();
        checkpoint_path.push(format!("checkpoint_{current_height}"));
        if let Err(e) = ledger.backup_database(&checkpoint_path) {
            warn!("Couldn't automatically store a checkpoint at {}: {e}", checkpoint_path.display());
            return;
        }
        last_checkpoint_height = Some(current_height);

        if let Err(e) = ledger.cache_block_tree() {
            warn!("The block tree cache failed: {e}");
        } else if let Err(e) = fs::copy(&block_tree_path, checkpoint_path.join("block_tree")) {
            warn!("The checkpoint block tree copy failed: {e}");
        }

        prune_auto_checkpoints(&auto_checkpoint_path, &mut existing_checkpoints);
    }))
}

fn prune_auto_checkpoints(auto_checkpoint_path: &Path, existing_checkpoints: &mut Vec<(PathBuf, u32)>) {
    // Count the existing auto checkpoints.
    existing_checkpoints.clear();
    let checkpoint_dir = match auto_checkpoint_path.read_dir() {
        Ok(dir) => dir,
        Err(e) => {
            warn!("IO error while accessing the automatic checkpoints: {e}");
            return;
        }
    };
    for entry in checkpoint_dir {
        // Handle possible IO errors.
        let entry = match entry {
            Ok(entry) => entry,
            Err(e) => {
                warn!("IO error while counting the automatic checkpoints: {e}");
                continue;
            }
        };

        // Skip non-directories.
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }

        // Recognize checkpoints by the "checkpoint_height" name.
        let file_name = entry.file_name().into_string().unwrap(); // can't fail - we create Unicode filenames
        let mut name_iter = file_name.split("_");
        if name_iter.next() != Some("checkpoint") {
            continue;
        }
        let Some(height) = name_iter.next() else {
            continue;
        };
        let Ok(height) = u32::from_str(height) else {
            continue;
        };
        existing_checkpoints.push((path, height));
    }
    existing_checkpoints.sort_unstable_by_key(|(_, height)| cmp::Reverse(*height));

    // If we have a sufficient number of checkpoints, delete the oldest one(s).
    let surplus_checkpoints = existing_checkpoints.len().saturating_sub(MAX_AUTO_CHECKPOINTS);
    for _ in 0..surplus_checkpoints {
        if let Some((checkpoint_path, _)) = existing_checkpoints.pop()
            && let Err(e) = fs::remove_dir_all(checkpoint_path)
        {
            warn!("Couldn't remove an automatic ledger checkpoint: {e}");
        }
    }
}

fn spawn_checkpoint_loop(mut checkpoint: impl FnMut() + Send + 'static) -> task::JoinHandle<()> {
    tokio::spawn(async move {
        info!("The automatic ledger checkpoint routine started");
        loop {
            tokio::time::sleep(Duration::from_millis(500)).await;
            match task::spawn_blocking(move || {
                checkpoint();
                checkpoint
            })
            .await
            {
                Ok(next) => checkpoint = next,
                Err(error) => {
                    warn!("The automatic ledger checkpoint task failed: {error}");
                    return;
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::existing_startup_checkpoint_height;
    use std::{
        fs,
        time::{SystemTime, UNIX_EPOCH},
    };

    #[test]
    fn seeds_last_checkpoint_height_when_startup_checkpoint_directory_exists() {
        let startup_height = 42;
        let unique = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        let base_path = std::env::temp_dir().join(format!("snarkos_checkpoint_seed_test_{unique}"));
        let checkpoint_path = base_path.join(format!("checkpoint_{startup_height}"));
        fs::create_dir_all(&checkpoint_path).unwrap();

        let seeded_height = existing_startup_checkpoint_height(base_path.as_path(), startup_height);
        assert_eq!(seeded_height, Some(startup_height));

        fs::remove_dir_all(base_path).unwrap();
    }

    #[test]
    fn does_not_seed_last_checkpoint_height_when_startup_checkpoint_directory_missing() {
        let startup_height = 42;
        let unique = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        let base_path = std::env::temp_dir().join(format!("snarkos_checkpoint_seed_test_{unique}"));
        fs::create_dir_all(&base_path).unwrap();

        let seeded_height = existing_startup_checkpoint_height(base_path.as_path(), startup_height);
        assert_eq!(seeded_height, None);

        fs::remove_dir_all(base_path).unwrap();
    }
}
