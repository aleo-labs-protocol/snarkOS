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

use snarkos_node_cdn::CdnBlockSync;
use snarkos_node_router::{
    Router,
    messages::{BlockRequest, Message},
};
use snarkos_node_sync::{BlockSync, STUCK_SYNC_SHUTDOWN_THRESHOLD, serve_block_request};
use snarkos_utilities::SignalHandler;
use snarkvm::{
    ledger::{
        Ledger,
        LoadOptions,
        store::{ConsensusStorage, StorageMode, check_authority_retention_blocks},
    },
    prelude::Network,
    utilities::flatten_error,
};

use anyhow::{Context, Result};
use std::{net::SocketAddr, sync::Arc, time::Duration};

/// How many recent blocks' authority data this node keeps (see `--prune-authority-data`). The
/// default keeps all of it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct AuthorityRetention(Option<u32>);

impl AuthorityRetention {
    /// Keeps the authority data of the last `blocks` blocks, which must be at least snarkVM's
    /// `min_authority_retention_blocks`.
    pub fn prune<N: Network>(blocks: u32) -> Result<Self> {
        check_authority_retention_blocks::<N>(blocks)?;
        Ok(Self(Some(blocks)))
    }

    /// Returns the number of recent blocks whose authority data this node keeps, or `None` if it
    /// keeps all of it.
    pub fn blocks(&self) -> Option<u32> {
        self.0
    }

    /// Returns the options to load the ledger with.
    fn load_options(self) -> LoadOptions {
        LoadOptions::default().with_authority_retention(self.0)
    }

    /// Returns the options to load the ledger with, installing `dev_committee` if it is set.
    #[cfg(feature = "test_network")]
    fn load_options_with_dev_committee(
        self,
        dev_committee: Option<snarkvm::ledger::DevCommitteeOptions>,
    ) -> LoadOptions {
        let options = self.load_options();
        match dev_committee {
            Some(dev_committee) => options.with_dev_committee(dev_committee),
            None => options,
        }
    }
}

/// Whether a node whose peers no longer serve the blocks it needs can recover by restarting into
/// the startup CDN sync.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CdnFallback {
    /// The startup CDN sync completed, so a restart repeats it.
    Available,
    /// CDN sync is disabled (`--nocdn`, or `--dev` without `--cdn`).
    Disabled,
    /// The startup CDN sync failed, so a restart retries it.
    Failed,
}

/// Waits for the startup CDN sync, if any, and reports whether a restart could repeat it.
pub async fn wait_for_cdn(cdn_sync: Option<Arc<CdnBlockSync>>) -> Result<CdnFallback> {
    let Some(cdn_sync) = cdn_sync else { return Ok(CdnFallback::Disabled) };
    // A failed sync was logged by the CDN sync task.
    Ok(match cdn_sync.wait().await.with_context(|| "Failed to synchronize from the CDN")? {
        Ok(_) => CdnFallback::Available,
        Err(_) => CdnFallback::Failed,
    })
}

/// Returns `true` if a node whose block sync has been stuck for `stuck_for` must shut down: after
/// `STUCK_SYNC_SHUTDOWN_THRESHOLD`, unless CDN sync is disabled, since a restart could not help.
fn is_stuck_sync_fatal(stuck_for: Option<Duration>, cdn_fallback: CdnFallback) -> bool {
    cdn_fallback != CdnFallback::Disabled
        && stuck_for.is_some_and(|stuck_for| stuck_for >= STUCK_SYNC_SHUTDOWN_THRESHOLD)
}

/// Shuts the node down, as failed, if its block sync has been stuck long enough (see
/// [`is_stuck_sync_fatal`]), and returns whether it did.
pub fn shut_down_if_stuck_sync<N: Network>(
    sync: &BlockSync<N>,
    signal_handler: &SignalHandler,
    cdn_fallback: CdnFallback,
) -> bool {
    if !is_stuck_sync_fatal(sync.stuck_for(), cdn_fallback) {
        return false;
    }
    let recovery = match cdn_fallback {
        CdnFallback::Failed => "The startup CDN sync failed (see above): restart the node to retry it",
        _ => "Restart the node to sync the missing blocks from the CDN",
    };
    error!(
        "No connected peer serves the blocks this node needs. Shutting down. {recovery}, or restore a recent \
        ledger snapshot."
    );
    signal_handler.stop_with_error("No connected peer serves the blocks this node needs (see the logs above)".into());
    true
}

/// Answers a `BlockRequest` from a router peer with blocks from `ledger`. Returns `false` if the
/// request cannot be served.
fn send_block_response<N: Network, C: ConsensusStorage<N>>(
    router: &Router<N>,
    ledger: &Ledger<N, C>,
    peer_ip: SocketAddr,
    request: BlockRequest,
) -> bool {
    let response = serve_block_request(
        request,
        router.supports_status_replies(peer_ip),
        || ledger.latest_height(),
        || ledger.vm().block_store().authority_pruned_below(),
        |heights| ledger.get_blocks(heights),
    );
    match response {
        Ok(response) => {
            router.send(peer_ip, Message::BlockResponse(response));
            true
        }
        Err(error) => {
            error!("{}", flatten_error(&error));
            false
        }
    }
}

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

    type CurrentNetwork = snarkvm::prelude::MainnetV0;

    #[test]
    fn authority_retention_load_options() {
        assert_eq!(AuthorityRetention::default().load_options().authority_retention, None);
        let min = snarkvm::ledger::store::min_authority_retention_blocks::<CurrentNetwork>();
        let pruning = AuthorityRetention::prune::<CurrentNetwork>(min).unwrap();
        assert_eq!(pruning.load_options().authority_retention, Some(min));
        assert!(AuthorityRetention::prune::<CurrentNetwork>(min - 1).is_err());
    }

    #[cfg(feature = "test_network")]
    #[test]
    fn dev_committee_keeps_the_retention_window() {
        let dev_committee = snarkvm::ledger::DevCommitteeOptions { start_round: None, dev_num_validators: 4, seed: 0 };
        let min = snarkvm::ledger::store::min_authority_retention_blocks::<CurrentNetwork>();
        let pruning = AuthorityRetention::prune::<CurrentNetwork>(min).unwrap();
        let options = pruning.load_options_with_dev_committee(Some(dev_committee));
        assert_eq!(options.authority_retention, Some(min));
        assert!(options.dev_committee.is_some());
    }

    /// Tests that a stuck node shuts down only after the threshold, and never with CDN sync disabled.
    #[test]
    fn stuck_sync_is_fatal_only_with_a_cdn_after_the_threshold() {
        let below = Some(STUCK_SYNC_SHUTDOWN_THRESHOLD - Duration::from_secs(1));
        let at = Some(STUCK_SYNC_SHUTDOWN_THRESHOLD);
        for cdn_fallback in [CdnFallback::Available, CdnFallback::Failed] {
            assert!(!is_stuck_sync_fatal(None, cdn_fallback));
            assert!(!is_stuck_sync_fatal(below, cdn_fallback));
            assert!(is_stuck_sync_fatal(at, cdn_fallback));
        }
        assert!(!is_stuck_sync_fatal(at, CdnFallback::Disabled));
        assert!(!is_stuck_sync_fatal(Some(Duration::MAX), CdnFallback::Disabled));
    }
}
