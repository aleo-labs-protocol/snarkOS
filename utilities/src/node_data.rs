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

use std::path::{Path, PathBuf};

/// The filename of the gateway peer cache.
pub const GATEWAY_PEER_CACHE_FILE: &str = "gateway-peer-cache";
/// The old filename of the gateway peer cache.
pub const LEGACY_GATEWAY_PEER_CACHE_FILE: &str = "cached_gateway_peers";

/// The filename of the router peer cache.
pub const ROUTER_PEER_CACHE_FILE: &str = "router-peer-cache";
/// The old filename of the router peer cache.
pub const LEGACY_ROUTER_PEER_CACHE_FILE: &str = "cached_router_peers";

/// The filename of the proposal cache.
pub const CURRENT_PROPOSAL_CACHE_FILE: &str = "current-proposal-cache";

/// The filename used to persist the hotswapped dev committee's starting round.
#[cfg(feature = "test_network")]
pub const DEV_COMMITTEE_STATE_FILE: &str = "dev-committee-state";

/// The filename of the JWT secret for a given address.
pub fn jwt_secret_file<D: std::fmt::Display>(address: &D) -> PathBuf {
    PathBuf::from(format!("jwt_secret_{address}.txt"))
}

/// The old filename of the current proposal cache.
pub fn legacy_current_proposal_cache_file(network: u16, dev: Option<u16>) -> PathBuf {
    if let Some(dev) = dev {
        PathBuf::from(format!(".current-proposal-cache-{network}-{dev}"))
    } else {
        PathBuf::from(format!("current-proposal-cache-{network}"))
    }
}

/// Tracks information about where the node-specfic configuration files are stored.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeDataDir {
    path: PathBuf,
}

impl NodeDataDir {
    /// Initializes the node data directory the given path.
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }

    /// Initializes the node data directory to a location suitable for unit/integration tests.
    pub fn new_test(dev: Option<u16>) -> Self {
        if let Some(dev) = dev {
            Self { path: PathBuf::from(format!(".node-data-test-{dev}")) }
        } else {
            Self { path: PathBuf::from(".node-data-test") }
        }
    }

    /// Initializes the node data directory path to the development path for the specified network and node index.
    pub fn new_development(network: u16, dev: u16) -> Self {
        // Use the current directory as the base path, and fall back to the
        // cargo manifest directory if the current directory is not available.
        let path = std::env::current_dir()
            .unwrap_or(PathBuf::from(env!("CARGO_MANIFEST_DIR")))
            .join(format!(".node-data-{network}-{dev}"));

        Self::new(path)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// The location to store the previous peer cache.
    pub fn router_peer_cache_path(&self) -> PathBuf {
        self.path.join(ROUTER_PEER_CACHE_FILE)
    }

    pub fn gateway_peer_cache_path(&self) -> PathBuf {
        self.path.join(GATEWAY_PEER_CACHE_FILE)
    }

    /// The location to store the current proposal cache.
    pub fn current_proposal_cache_path(&self) -> PathBuf {
        self.path.join(CURRENT_PROPOSAL_CACHE_FILE)
    }

    /// The location used to persist the hotswapped dev committee's starting round.
    #[cfg(feature = "test_network")]
    pub fn dev_committee_state_path(&self) -> PathBuf {
        self.path.join(DEV_COMMITTEE_STATE_FILE)
    }

    /// The location to store the JWT secret for a given address.
    pub fn jwt_secret_path<D: std::fmt::Display>(&self, address: &D) -> PathBuf {
        self.path.join(jwt_secret_file(address))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_cache_filenames_are_pinned() {
        // These are on-disk names. Renaming one does not fail anything at compile time; it just
        // orphans the file a running node already wrote, so the change should be deliberate.
        assert_eq!(ROUTER_PEER_CACHE_FILE, "router-peer-cache");
        assert_eq!(GATEWAY_PEER_CACHE_FILE, "gateway-peer-cache");
        assert_eq!(CURRENT_PROPOSAL_CACHE_FILE, "current-proposal-cache");
        assert_eq!(LEGACY_ROUTER_PEER_CACHE_FILE, "cached_router_peers");
        assert_eq!(LEGACY_GATEWAY_PEER_CACHE_FILE, "cached_gateway_peers");
    }

    #[test]
    fn the_legacy_filenames_are_distinct_from_the_current_ones() {
        assert_ne!(ROUTER_PEER_CACHE_FILE, LEGACY_ROUTER_PEER_CACHE_FILE);
        assert_ne!(GATEWAY_PEER_CACHE_FILE, LEGACY_GATEWAY_PEER_CACHE_FILE);
        assert_ne!(ROUTER_PEER_CACHE_FILE, GATEWAY_PEER_CACHE_FILE);
    }

    #[test]
    fn every_path_is_rooted_at_the_data_dir() {
        let dir = NodeDataDir::new(PathBuf::from("/var/lib/snarkos"));

        for path in [dir.router_peer_cache_path(), dir.gateway_peer_cache_path(), dir.current_proposal_cache_path()] {
            assert!(path.starts_with(dir.path()), "{path:?} escaped the data dir");
            assert_eq!(path.parent().unwrap(), dir.path());
        }
    }

    #[test]
    fn each_cache_path_is_a_distinct_file() {
        let dir = NodeDataDir::new(PathBuf::from("/var/lib/snarkos"));

        // The router and gateway caches hold different peer sets; sharing a filename would have
        // one silently overwrite the other.
        assert_ne!(dir.router_peer_cache_path(), dir.gateway_peer_cache_path());
        assert_ne!(dir.router_peer_cache_path(), dir.current_proposal_cache_path());
        assert_ne!(dir.gateway_peer_cache_path(), dir.current_proposal_cache_path());
    }

    #[test]
    fn the_jwt_secret_path_agrees_with_the_bare_filename_helper() {
        let dir = NodeDataDir::new(PathBuf::from("/var/lib/snarkos"));
        let address = "aleo1example";

        // `cli::commands::start` builds this path itself out of `path()` and `jwt_secret_file`
        // rather than calling `jwt_secret_path`, so the two constructions have to stay in step or
        // the node writes its JWT secret where the reader will not look.
        assert_eq!(dir.jwt_secret_path(&address), dir.path().join(jwt_secret_file(&address)));
    }

    #[test]
    fn the_jwt_secret_filename_is_scoped_to_the_address() {
        assert_eq!(jwt_secret_file(&"aleo1abc"), PathBuf::from("jwt_secret_aleo1abc.txt"));
        assert_ne!(jwt_secret_file(&"aleo1abc"), jwt_secret_file(&"aleo1def"));
    }

    #[test]
    fn the_legacy_proposal_cache_filename_switches_on_dev() {
        // note: the dev form is a hidden file and the non-dev form is not.
        assert_eq!(legacy_current_proposal_cache_file(1, Some(3)), PathBuf::from(".current-proposal-cache-1-3"));
        assert_eq!(legacy_current_proposal_cache_file(1, None), PathBuf::from("current-proposal-cache-1"));

        // Distinct dev indices must not collide.
        assert_ne!(legacy_current_proposal_cache_file(1, Some(0)), legacy_current_proposal_cache_file(1, Some(1)));
        // Neither must distinct networks.
        assert_ne!(legacy_current_proposal_cache_file(0, None), legacy_current_proposal_cache_file(1, None));
    }

    #[test]
    fn test_data_dirs_are_separated_by_dev_index() {
        assert_eq!(NodeDataDir::new_test(None).path(), Path::new(".node-data-test"));
        assert_eq!(NodeDataDir::new_test(Some(2)).path(), Path::new(".node-data-test-2"));

        // Two dev nodes running side by side must not share a data dir.
        assert_ne!(NodeDataDir::new_test(Some(0)), NodeDataDir::new_test(Some(1)));
        assert_ne!(NodeDataDir::new_test(None), NodeDataDir::new_test(Some(0)));
    }

    #[test]
    fn development_data_dirs_are_absolute_and_separated_by_network_and_index() {
        let first = NodeDataDir::new_development(1, 0);
        let second = NodeDataDir::new_development(1, 1);
        let other_network = NodeDataDir::new_development(2, 0);

        // The path is anchored at the current directory, so it must not be a bare relative name.
        assert!(first.path().is_absolute());
        assert_eq!(first.path().file_name().unwrap(), ".node-data-1-0");

        assert_ne!(first, second);
        assert_ne!(first, other_network);
    }
}
