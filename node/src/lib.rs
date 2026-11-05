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

use snarkos_node_cdn::{CdnBlockSync, CdnSyncOutcome};
use snarkos_node_router::messages::DataBlocks;
use snarkos_node_sync::{BlockSync, STUCK_SYNC_SHUTDOWN_THRESHOLD, blocks_or_empty};
use snarkos_utilities::SignalHandler;
use snarkvm::{
    prelude::{Ledger, Network, store::ConsensusStorage},
    utilities::flatten_error,
};

use aleo_std::StorageMode;
use anyhow::{Context, Result};
use std::{sync::Arc, time::Duration};

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
    /// The startup CDN sync completed, so a restart repeats it. A CDN that had nothing new then may
    /// have caught up since.
    Available,
    /// CDN sync is disabled (`--nocdn`, or `--dev` without `--cdn`).
    Disabled,
    /// The startup CDN sync failed, so a restart may fail the same way.
    Failed,
}

/// Waits for the startup CDN sync, if any, and reports whether a restart could repeat it.
pub async fn wait_for_cdn(cdn_sync: Option<Arc<CdnBlockSync>>) -> Result<CdnFallback> {
    let Some(cdn_sync) = cdn_sync else { return Ok(CdnFallback::Disabled) };
    Ok(match cdn_sync.wait().await.with_context(|| "Failed to synchronize from the CDN")? {
        CdnSyncOutcome::Synced => CdnFallback::Available,
        CdnSyncOutcome::Failed(height, error) => {
            warn!("{}", flatten_error(error.context(format!("The startup CDN sync failed after block {height}"))));
            CdnFallback::Failed
        }
    })
}

/// Returns `true` if a node that has been stuck for `stuck_for` must shut down.
///
/// With CDN sync disabled or failed, waiting changes nothing, so the node fails at once. Otherwise
/// the condition must persist for `STUCK_SYNC_SHUTDOWN_THRESHOLD`, so that a transient set of peers
/// does not stop the node.
fn is_stuck_sync_fatal(stuck_for: Option<Duration>, cdn_fallback: CdnFallback) -> bool {
    stuck_for.is_some_and(|stuck_for| {
        matches!(cdn_fallback, CdnFallback::Disabled | CdnFallback::Failed)
            || stuck_for >= STUCK_SYNC_SHUTDOWN_THRESHOLD
    })
}

/// Shuts the node down, as failed, if no connected peer can serve the blocks it needs (see
/// `BlockSync::sync_stuck_below_peer_floors` and [`is_stuck_sync_fatal`]), and returns whether it did.
pub fn shut_down_if_stuck_sync<N: Network>(
    sync: &BlockSync<N>,
    signal_handler: &SignalHandler,
    cdn_fallback: CdnFallback,
) -> bool {
    if !is_stuck_sync_fatal(sync.sync_stuck_below_peer_floors(), cdn_fallback) {
        return false;
    }

    let recovery = match cdn_fallback {
        CdnFallback::Available => {
            "Restart the node to sync the missing blocks from the CDN; if the CDN has nothing newer, restore a recent ledger snapshot."
        }
        CdnFallback::Disabled => {
            "CDN sync is disabled: restore a recent ledger snapshot, or restart with CDN sync enabled."
        }
        CdnFallback::Failed => {
            "The startup CDN sync failed (see above): restart the node to retry it, or restore a recent ledger snapshot."
        }
    };
    error!(
        "No connected peer can serve the blocks this node needs; they have likely been pruned network-wide. \
        Shutting down. {recovery}"
    );
    signal_handler
        .stop_with_error("No connected peer can serve the blocks this node needs (see the logs above)".to_string());
    true
}

/// Returns the blocks to answer a block request for `start_height..end_height` with (see
/// [`blocks_or_empty`]), or `None` after logging the error if the ledger fails to read them.
pub(crate) fn blocks_for_request<N: Network, C: ConsensusStorage<N>>(
    ledger: &Ledger<N, C>,
    start_height: u32,
    end_height: u32,
) -> Option<DataBlocks<N>> {
    let latest_height = || ledger.latest_height();
    match blocks_or_empty(start_height, end_height, latest_height, || ledger.get_blocks(start_height..end_height)) {
        Ok(blocks) => Some(blocks),
        Err(error) => {
            let err =
                error.context(format!("Failed to retrieve blocks {start_height} to {end_height} from the ledger"));
            error!("{}", flatten_error(&err));
            None
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stuck_sync_is_fatal_at_once_only_without_a_cdn_to_wait_for() {
        let just_stuck = Some(Duration::ZERO);
        let below_threshold = Some(STUCK_SYNC_SHUTDOWN_THRESHOLD - Duration::from_secs(1));
        let at_threshold = Some(STUCK_SYNC_SHUTDOWN_THRESHOLD);

        for cdn_fallback in [CdnFallback::Disabled, CdnFallback::Failed] {
            assert!(!is_stuck_sync_fatal(None, cdn_fallback));
            assert!(is_stuck_sync_fatal(just_stuck, cdn_fallback));
        }
        assert!(!is_stuck_sync_fatal(None, CdnFallback::Available));
        assert!(!is_stuck_sync_fatal(below_threshold, CdnFallback::Available));
        assert!(is_stuck_sync_fatal(at_threshold, CdnFallback::Available));
    }
}
