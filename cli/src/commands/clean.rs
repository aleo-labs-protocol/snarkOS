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

use crate::helpers::args::parse_node_data_dir;

use snarkos_utilities::NodeDataDir;

use snarkvm::console::network::{CanaryV0, MainnetV0, Network, TestnetV0};

use aleo_std::StorageMode;
use anyhow::{Context, Result, bail};
use clap::Parser;
use colored::Colorize;
use std::path::PathBuf;

/// Cleans the snarkOS node storage.
#[derive(Debug, Parser)]
pub struct Clean {
    /// Specify the network to remove from storage (0 = mainnet, 1 = testnet, 2 = canary)
    #[clap(default_value_t=MainnetV0::ID, long = "network", value_parser = clap::value_parser!(u16).range((MainnetV0::ID as i64)..=(CanaryV0::ID as i64)))]
    pub network: u16,

    /// Enables development mode, specify the unique ID of the local node to clean.
    #[clap(long)]
    pub dev: Option<u16>,

    /// Specify the path to a directory containing the ledger. Overrides the default path (also for dev).
    #[clap(long, alias = "path")]
    pub ledger_storage: Option<PathBuf>,

    /// Keep the node data directory (disabled by default).
    #[clap(long)]
    pub keep_node_data: bool,

    /// Sets a custom path for the node configuration. Overrides the default path (also for dev).
    #[clap(long, alias = "node-data-path", conflicts_with = "keep_node_data")]
    pub node_data_storage: Option<PathBuf>,

    /// Remove the legacy RocksDB mapping-history prefixes and leave the ledger in place.
    ///
    /// Deletes `MappingUpdate`, `MappingUpdateHeights`, and `StakingRewards`. Node data and the
    /// rest of the ledger stay.
    #[clap(long)]
    pub history: bool,
}

impl Clean {
    /// Cleans the snarkOS node storage.
    pub fn parse(self) -> Result<String> {
        // Legacy mapping history is a prefix delete. The ledger and node data stay.
        if self.history {
            return self.delete_legacy_mapping_history();
        }

        // Remove the specified node configuration from storage.
        if !self.keep_node_data {
            let node_data_dir = parse_node_data_dir(&self.node_data_storage, self.network, self.dev)?;
            println!("{}", Self::remove_node_data(&node_data_dir)?);
        }

        // Remove the specified ledger from storage.
        let storage_mode = match self.ledger_storage {
            Some(path) => StorageMode::from(path),
            None => match self.dev {
                Some(id) => StorageMode::Development(id),
                None => StorageMode::Production,
            },
        };
        Self::remove_ledger(self.network, &storage_mode)
    }

    /// Removes the specified node configuration from storage.
    fn remove_node_data(node_data_dir: &NodeDataDir) -> Result<String> {
        // With the new layout, we can remove the entire folder.
        let data_path = node_data_dir.path();

        // Prepare the path string.
        let path_string = format!("(in \"{}\")", data_path.display()).dimmed();

        if data_path.exists() {
            std::fs::remove_dir_all(data_path).with_context(|| format!("Failed to remove node data {path_string}"))?;
            Ok(format!("✅ Cleaned up node data {path_string}"))
        } else {
            Ok(format!("✅ No node data was found {path_string}"))
        }
    }

    /// Removes the specified ledger from storage.
    pub(crate) fn remove_ledger(network: u16, mode: &StorageMode) -> Result<String> {
        // Construct the path to the ledger in storage.
        let path = aleo_std::aleo_ledger_dir(network, mode);

        // Prepare the path string.
        let path_string = format!("(in \"{}\")", path.display()).dimmed();

        // Check if the path to the ledger exists in storage.
        if path.exists() {
            // Remove the ledger files from storage.
            std::fs::remove_dir_all(&path)
                .with_context(|| format!("Failed to remove the snarkOS ledger {path_string}"))?;
            Ok(format!("✅ Cleaned the snarkOS ledger {path_string}"))
        } else {
            Ok(format!("✅ No snarkOS ledger was found {path_string}"))
        }
    }

    /// Deletes the legacy mapping-history prefixes from an existing ledger.
    fn delete_legacy_mapping_history(&self) -> Result<String> {
        let storage_mode = match &self.ledger_storage {
            Some(path) => StorageMode::from(path.clone()),
            None => match self.dev {
                Some(id) => StorageMode::Development(id),
                None => StorageMode::Production,
            },
        };
        let path = aleo_std::aleo_ledger_dir(self.network, &storage_mode);
        let path_string = format!("(in \"{}\")", path.display()).dimmed();
        if !path.exists() {
            return Ok(format!("✅ No snarkOS ledger was found {path_string}"));
        }

        match self.network {
            MainnetV0::ID => Self::delete_network::<MainnetV0>(&storage_mode)?,
            TestnetV0::ID => Self::delete_network::<TestnetV0>(&storage_mode)?,
            CanaryV0::ID => Self::delete_network::<CanaryV0>(&storage_mode)?,
            id => bail!("Unsupported network id {id}"),
        }
        Ok(format!("✅ Removed legacy mapping history {path_string}"))
    }

    /// Opens the ledger and deletes the three legacy mapping-history prefixes.
    fn delete_network<N: Network>(storage_mode: &StorageMode) -> Result<()> {
        use snarkvm::ledger::store::helpers::rocksdb::{Database, RocksDB};
        RocksDB::open(N::ID, storage_mode.clone())?.delete_legacy_mapping_history()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn history_flag_is_opt_in() {
        let config = Clean::try_parse_from(["snarkos"].iter()).unwrap();
        assert!(!config.history);

        let config = Clean::try_parse_from(["snarkos", "--history"].iter()).unwrap();
        assert!(config.history);
    }
}
