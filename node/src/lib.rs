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
#![allow(clippy::too_many_arguments)]
#![recursion_limit = "256"]

#[macro_use]
extern crate async_trait;
#[macro_use]
extern crate tracing;

pub use snarkos_node_bft as bft;
pub use snarkos_node_cdn as cdn;
pub use snarkos_node_consensus as consensus;
pub use snarkos_node_network as network;
pub use snarkos_node_rest as rest;
pub use snarkos_node_router as router;
pub use snarkos_node_sync as sync;
pub use snarkos_node_tcp as tcp;
pub use snarkvm;

mod bootstrap_client;
pub use bootstrap_client::*;

mod client;
pub use client::*;

mod prover;
pub use prover::*;

mod validator;
pub use validator::*;

mod node;
pub use node::*;

mod traits;
pub use traits::*;

use aleo_std::StorageMode;
use snarkos_node_sync::STUCK_SYNC_SHUTDOWN_THRESHOLD;
use std::time::Duration;

/// A helper to log instructions to recover.
pub fn log_clean_error(storage_mode: &StorageMode) {
    match storage_mode {
        StorageMode::Production => error!("Storage corruption detected! Run `snarkos clean` to reset storage"),
        StorageMode::Development(id) => {
            error!("Storage corruption detected! Run `snarkos clean --dev {id}` to reset storage")
        }
        StorageMode::Custom(path, _) => {
            error!("Storage corruption detected! Run `snarkos clean --path {}` to reset storage", path.display())
        }
        StorageMode::Test(_) => {
            // Ephemeral location - no need for cleanups.
        }
    }
}

/// Whether a node whose needed blocks every peer has pruned can recover by restarting into the
/// startup CDN sync.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CdnFallback {
    /// The startup CDN sync completed, so a restart repeats it and fills the gap.
    Available,
    /// CDN sync is disabled (`--nocdn`, `--dev`, or a prover).
    Disabled,
    /// The startup CDN sync failed, so a restart may fail the same way.
    Failed,
}

/// Returns `true` if the node must shut down because no connected peer can serve the blocks it
/// needs (see `BlockSync::sync_stuck_below_peer_floors`).
///
/// With the CDN to fall back to, the condition must persist for `STUCK_SYNC_SHUTDOWN_THRESHOLD`
/// first, so that a transient set of peers does not restart the node. Without it, waiting changes
/// nothing, so the node fails at once.
pub fn is_stuck_sync_fatal(stuck_for: Option<Duration>, cdn_fallback: CdnFallback) -> bool {
    match stuck_for {
        None => false,
        Some(_) if cdn_fallback != CdnFallback::Available => true,
        Some(stuck_for) => stuck_for >= STUCK_SYNC_SHUTDOWN_THRESHOLD,
    }
}

/// Logs an error explaining that no connected peer can serve the blocks this node needs, and how to recover.
pub fn log_stuck_sync_error(cdn_fallback: CdnFallback) {
    const WHAT: &str = "No connected peer can serve the blocks this node needs — they have likely been pruned network-wide. Shutting down.";
    match cdn_fallback {
        CdnFallback::Available => error!(
            "{WHAT} On restart, the node will sync the missing blocks from the CDN. \
            Alternatively, restore a recent ledger snapshot."
        ),
        CdnFallback::Disabled => error!(
            "{WHAT} CDN sync is disabled: restart without --nocdn so the node syncs them from the CDN, \
            or restore a recent ledger snapshot."
        ),
        CdnFallback::Failed => error!(
            "{WHAT} The startup CDN sync failed (see above): restart the node to retry it, \
            or restore a recent ledger snapshot."
        ),
    }
}

/// Starts the notification message loop.
pub fn start_notification_message_loop() -> tokio::task::JoinHandle<()> {
    // let mut interval = tokio::time::interval(std::time::Duration::from_secs(180));
    tokio::spawn(async move {
        //     loop {
        //         interval.tick().await;
        //         // TODO (howardwu): Swap this with the official message for announcements.
        //         // info!("{}", notification_message());
        //     }
    })
}

/// Returns the notification message as a string.
pub fn notification_message() -> String {
    use colored::Colorize;

    let mut output = String::new();
    output += &r#"

 ==================================================================================================

                     🚧 Welcome to Aleo - Calibration Period 🚧

 ==================================================================================================

     During the calibration period, the network will be running in limited capacity.

     This calibration period is to ensure validators are stable and ready for mainnet launch.
     During this period, the objective is to assess, adjust, and align validators' performance,
     stability, and interoperability under varying network conditions.

     Please expect several network resets. With each network reset, software updates will
     be performed to address potential bottlenecks, vulnerabilities, and/or inefficiencies, which
     will ensure optimal performance for the ecosystem of validators, provers, and developers.

 ==================================================================================================

    Duration:
    - Start Date: September 27, 2023
    - End Date: October 18, 2023 (subject to change)

    Participation:
    - Node operators are NOT REQUIRED to participate during this calibration period.

    Network Resets:
    - IMPORTANT: EXPECT MULTIPLE NETWORK RESETS.
    - If participating, BE PREPARED TO RESET YOUR NODE AT ANY TIME.
    - When a reset occurs, RUN THE FOLLOWING TO RESET YOUR NODE:
        - git checkout mainnet && git pull
        - cargo install --locked --path .
        - snarkos clean
        - snarkos start --nodisplay --client

    Communication:
    - Stay ONLINE and MONITOR our Discord and Twitter for community updates.

    Purpose:
    - This period is STRICTLY FOR NETWORK CALIBRATION.
    - This period is NOT INTENDED for general-purpose usage by developers and provers.

    Incentives:
    - There are NO INCENTIVES during this calibration period.

 ==================================================================================================
"#
    .white()
    .bold();

    output
}
