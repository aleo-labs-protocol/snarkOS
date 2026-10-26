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

#![forbid(unsafe_code)]

pub mod node_type;
pub use node_type::*;

pub mod noise;

pub mod peer;
pub use peer::*;

pub mod peering;
pub use peering::*;

pub mod resolver;
pub use resolver::*;

use snarkvm::prelude::Network;

use smol_str::SmolStr;
use socket2::SockRef;
use std::{env::VarError, io, net::SocketAddr, str::FromStr, time::Duration};
use tokio::net::TcpStream;
use tracing::*;

// Include the generated build information.
pub mod built_info {
    include!(concat!(env!("OUT_DIR"), "/built.rs"));
}

/// Returns the list of bootstrap peers.
#[allow(clippy::if_same_then_else)]
pub fn bootstrap_peers<N: Network>(is_dev: bool) -> Vec<SocketAddr> {
    if cfg!(feature = "test") || is_dev {
        // Development testing contains optional bootstrap peers loaded from the environment.
        match std::env::var("TEST_BOOTSTRAP_PEERS") {
            Ok(peers) => peers.split(',').map(|peer| SocketAddr::from_str(peer).unwrap()).collect(),
            Err(VarError::NotPresent) => {
                // Return an empty list if the environment variable is not present.
                vec![]
            }
            Err(err) => {
                // Log other errors, e.g., invalid encoding.
                warn!("Failed to load bootstrap peers from environment: {err}");
                vec![]
            }
        }
    } else if N::ID == snarkvm::console::network::MainnetV0::ID {
        // Mainnet contains the following bootstrap peers.
        vec![
            SocketAddr::from_str("35.231.67.219:4130").unwrap(),
            SocketAddr::from_str("34.73.195.196:4130").unwrap(),
            SocketAddr::from_str("34.23.225.202:4130").unwrap(),
            SocketAddr::from_str("34.148.16.111:4130").unwrap(),
        ]
    } else if N::ID == snarkvm::console::network::TestnetV0::ID {
        // TestnetV0 contains the following bootstrap peers.
        vec![
            SocketAddr::from_str("34.138.104.159:4130").unwrap(),
            SocketAddr::from_str("35.231.46.237:4130").unwrap(),
            SocketAddr::from_str("34.148.251.155:4130").unwrap(),
            SocketAddr::from_str("35.190.141.234:4130").unwrap(),
        ]
    } else if N::ID == snarkvm::console::network::CanaryV0::ID {
        // CanaryV0 contains the following bootstrap peers.
        vec![
            SocketAddr::from_str("34.139.88.58:4130").unwrap(),
            SocketAddr::from_str("34.139.252.207:4130").unwrap(),
            SocketAddr::from_str("35.185.98.12:4130").unwrap(),
            SocketAddr::from_str("35.231.106.26:4130").unwrap(),
        ]
    } else {
        // Unrecognized networks contain no bootstrap peers.
        vec![]
    }
}

/// Get our SHA from the build information (or None if it is not set or does not 40 bytes long).
pub fn get_repo_commit_hash() -> Option<[u8; 40]> {
    built_info::GIT_COMMIT_HASH.and_then(|sha| sha.as_bytes().try_into().ok())
}

/// Logs the peer's snarkOS repo SHA and how it compares to ours.
pub fn log_repo_sha_comparison(peer_addr: SocketAddr, peer_sha: &Option<[u8; 40]>, ctx: &str) {
    let our_sha = get_repo_commit_hash();

    // Generate a string representation for the peers hash.
    let peer_sha_str: Option<&str> = peer_sha.as_ref().and_then(|h| str::from_utf8(h).ok());

    let sha_cmp = match (&our_sha, peer_sha, peer_sha_str) {
        // They sent no hash, or an invalid string.
        (_, _, None) | (_, None, _) => " with an unknown repo SHA".to_owned(),
        // Our hash cannot be retrieved.
        (None, _, Some(theirs_str)) => format!("@{theirs_str} (potentially different than us)"),
        // Both hashes are valid. Compare.
        (Some(ours), Some(theirs), Some(theirs_str)) => {
            if ours == theirs {
                format!("@{theirs_str} (same as us)")
            } else {
                format!("@{theirs_str} (different than us)")
            }
        }
    };

    debug!("{ctx} Peer '{peer_addr}' uses snarkOS{sha_cmp}");
}

/// Shortens the commit SHA.
pub fn shorten_snarkos_sha(sha: &Option<[u8; 40]>) -> SmolStr {
    if let Some(full_sha) = sha.as_ref().and_then(|s| str::from_utf8(s).ok()) {
        let end_idx = full_sha.char_indices()
            .nth(7) // GitHub commit SHA shorthand.
            .map(|(i, _)| i)
            .unwrap_or(full_sha.len()); // Can't really fail.

        SmolStr::from(&full_sha[..end_idx])
    } else {
        "unknown snarkOS SHA".into()
    }
}

/// Adjusts the low-level socket settings for extra robustness.
pub fn harden_socket(stream: &TcpStream) -> io::Result<()> {
    let socket = SockRef::from(stream);

    // Make OS-level disconnects immediate (no TIME_WAIT).
    socket.set_linger(Some(Duration::from_secs(0)))?;

    // Disable Nagle's algorithm for lower latency.
    socket.set_tcp_nodelay(true)?;

    // Disconnect if unacknowledged data stalls for 20s. This protects
    // the kernel's retransmission queue.
    #[cfg(target_os = "linux")]
    socket.set_tcp_user_timeout(Some(Duration::from_secs(20)))?;

    Ok(())
}
