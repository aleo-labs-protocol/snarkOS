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

use crate::{
    CandidatePeer,
    ConnectedPeer,
    NodeType,
    Outbound,
    PeerPoolHandling,
    Router,
    bootstrap_peers,
    messages::{DisconnectReason, Message, PeerRequest},
};

use snarkos_node_tcp::{ConnectError, P2P};

use snarkvm::prelude::Network;

use futures::future::join_all;
use rand::{SeedableRng, prelude::IteratorRandom};
use rand_chacha::ChaChaRng;
use std::time::Duration;
use tokio::task::JoinError;

/// A helper function to compute the maximum of two numbers.
/// See Rust issue 92391: https://github.com/rust-lang/rust/issues/92391.
pub const fn max(a: usize, b: usize) -> usize {
    match a > b {
        true => a,
        false => b,
    }
}

#[async_trait]
pub trait Heartbeat<N: Network>: Outbound<N> {
    /// The duration in seconds to sleep in between heartbeat executions.
    const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(25);
    /// The minimum number of peers required to maintain connections with.
    const MINIMUM_NUMBER_OF_PEERS: usize = 3;
    /// The minimum time between connection attempts to a peer.
    const MINIMUM_TIME_BETWEEN_CONNECTION_ATTEMPTS: Duration = Duration::from_secs(10);
    /// The time we consider the node to be starting up and avoid certain warnings such as "No connected peers".
    const STARTUP_GRACE_PERIOD: Duration = Duration::from_secs(60);
    /// The median number of peers to maintain connections with.
    const MEDIAN_NUMBER_OF_PEERS: usize = max(Self::MAXIMUM_NUMBER_OF_PEERS / 2, Self::MINIMUM_NUMBER_OF_PEERS);
    /// The maximum number of peers permitted to maintain connections with.
    const MAXIMUM_NUMBER_OF_PEERS: usize = 21;
    /// The maximum number of provers to maintain connections with.
    const MAXIMUM_NUMBER_OF_PROVERS: usize = Self::MAXIMUM_NUMBER_OF_PEERS / 4;
    /// The amount of time an IP address is prohibited from connecting.
    const IP_BAN_TIME_IN_SECS: u64 = 300;

    /// Handles the heartbeat request.
    async fn heartbeat(&self) {
        self.safety_check_minimum_number_of_peers();
        self.log_connected_peers();

        // Remove the oldest connected peer.
        self.remove_oldest_connected_peer();
        // Keep the number of connected peers within the allowed range.
        self.handle_connected_peers().await;
        // Keep the bootstrap peers within the allowed range.
        self.handle_bootstrap_peers().await;
        // Keep the trusted peers connected.
        self.handle_trusted_peers().await;
        // Keep the puzzle request up to date.
        self.handle_puzzle_request();
        // Unban any addresses whose ban time has expired.
        self.handle_banned_ips();
        // Clear stale rate-limit cache entries.
        self.clear_stale_peers();
    }

    /// TODO (howardwu): Consider checking minimum number of validators, to exclude clients and provers.
    /// This function performs safety checks on the setting for the minimum number of peers.
    fn safety_check_minimum_number_of_peers(&self) {
        // Perform basic sanity checks on the configuration for the number of peers.
        assert!(Self::MINIMUM_NUMBER_OF_PEERS >= 1, "The minimum number of peers must be at least 1.");
        assert!(Self::MINIMUM_NUMBER_OF_PEERS <= Self::MAXIMUM_NUMBER_OF_PEERS);
        assert!(Self::MINIMUM_NUMBER_OF_PEERS <= Self::MEDIAN_NUMBER_OF_PEERS);
        assert!(Self::MEDIAN_NUMBER_OF_PEERS <= Self::MAXIMUM_NUMBER_OF_PEERS);
        assert!(Self::MAXIMUM_NUMBER_OF_PROVERS <= Self::MAXIMUM_NUMBER_OF_PEERS);
    }

    /// This function logs the connected peers.
    fn log_connected_peers(&self) {
        // Log the connected peers.
        let connected_peers = self.router().connected_peers();
        let connected_peers_fmt = format!("{connected_peers:?}");
        match connected_peers.len() {
            0 => {
                // Only log a warning if the node has been running for a while.
                if self.router().tcp().uptime() > Self::STARTUP_GRACE_PERIOD {
                    warn!("No connected peers")
                }
            }
            1 => debug!(dim = true, "Connected to 1 peer: {connected_peers_fmt}"),
            num_connected => debug!(dim = true, "Connected to {num_connected} peers {connected_peers_fmt}"),
        }
    }

    /// Returns a sorted vector of network addresses of all removable connected peers
    /// where the first entry has the lowest priority and the last one the highest.
    ///
    /// Rules:
    ///     - Trusted peers and bootstrap nodes are not removable.
    ///     - Peers that we are currently syncing with are not removable.
    ///     - Connections that have not been seen in a while are considered lower priority.
    fn get_removable_peers(&self) -> Vec<ConnectedPeer<N>> {
        // Are we synced already? (cache this here, so it does not need to be recomputed)
        let is_block_synced = self.is_block_synced();

        // Sort by priority, where lowest priority will be at the beginning
        // of the vector.
        // Note, that this gives equal priority to clients and provers, which
        // we might want to change in the future.
        let mut peers = self.router().filter_connected_peers(|peer| {
            !peer.trusted
                && peer.node_type != NodeType::BootstrapClient
                && !self.router().cache.contains_inbound_block_request(&peer.listener_addr) // This peer is currently syncing from us.
                && (is_block_synced || self.router().cache.num_outbound_block_requests(&peer.listener_addr) == 0) // We are currently syncing from this peer.
        });
        peers.sort_by_key(|peer| peer.last_seen);

        peers
    }

    /// This function removes the peer that we have not heard from the longest,
    /// to keep the connections fresh.
    /// It only triggers if the router is above the minimum number of connected peers.
    fn remove_oldest_connected_peer(&self) {
        // Skip if the node is not requesting peers.
        if self.router().trusted_peers_only() {
            return;
        }

        // Skip if the router is at or below the minimum number of connected peers.
        if self.router().number_of_connected_peers() <= Self::MINIMUM_NUMBER_OF_PEERS {
            return;
        }

        // Disconnect from the oldest connected peer, which is the first entry in the list
        // of removable peers.
        // Do nothing, if the list is empty.
        if let Some(oldest) = self.get_removable_peers().first().map(|peer| peer.listener_addr) {
            info!("Disconnecting from '{oldest}' (periodic refresh of peers)");
            let _ = self.router().send(oldest, Message::Disconnect(DisconnectReason::PeerRefresh.into()));
            self.router().disconnect(oldest);
        }
    }

    /// Logs a message with the error and `context` if the connection attempt failed,
    /// and sets the log level based on the severity of the error.
    #[inline]
    fn log_if_connect_error(result: Result<Result<(), ConnectError>, JoinError>, context: &str) {
        match result {
            // Success!
            Ok(Ok(())) => {}
            Ok(Err(err @ ConnectError::AlreadyConnecting { .. }))
            | Ok(Err(err @ ConnectError::AlreadyConnected { .. })) => {
                // Log benign errors at a lower level.
                debug!("{context}: {err}");
            }
            // Print regular connection errors (such as "connection refused" as warnings)
            Ok(Err(err)) => warn!("{context}: {err}"),
            // Print join errors as error, as they most likely indicate a crash.
            Err(err) => error!("{context}: {err}"),
        }
    }

    /// This function keeps the number of connected peers within the allowed range.
    async fn handle_connected_peers(&self) {
        // Initialize an RNG.
        let rng = &mut ChaChaRng::from_rng(&mut rand::rng());

        // Obtain the number of connected peers.
        let num_connected = self.router().number_of_connected_peers();
        // Obtain the number of connected provers.
        let num_connected_provers = self.router().filter_connected_peers(|peer| peer.node_type.is_prover()).len();

        // Determine the maximum number of peers and provers to keep.
        let (max_peers, max_provers) = (Self::MAXIMUM_NUMBER_OF_PEERS, Self::MAXIMUM_NUMBER_OF_PROVERS);

        // Compute the number of surplus peers.
        let num_surplus_peers = num_connected.saturating_sub(max_peers);
        // Compute the number of surplus provers.
        let num_surplus_provers = num_connected_provers.saturating_sub(max_provers);
        // Compute the number of provers remaining connected.
        let num_remaining_provers = num_connected_provers.saturating_sub(num_surplus_provers);
        // Compute the number of surplus clients and validators.
        let num_surplus_clients_validators = num_surplus_peers.saturating_sub(num_remaining_provers);

        if num_surplus_provers > 0 || num_surplus_clients_validators > 0 {
            debug!(
                "Exceeded maximum number of connected peers, disconnecting from ({num_surplus_provers} + {num_surplus_clients_validators}) peers"
            );

            // Determine the provers to disconnect from.
            let provers_to_disconnect = self
                .router()
                .filter_connected_peers(|peer| peer.node_type.is_prover() && !peer.trusted)
                .into_iter()
                .sample(rng, num_surplus_provers);

            // Determine the clients and validators to disconnect from.
            let peers_to_disconnect = self
                .get_removable_peers()
                .into_iter()
                .filter(|peer| !peer.node_type.is_prover()) // remove provers as those are handled separately
                .take(num_surplus_clients_validators);

            // Proceed to send disconnect requests to these peers.
            for peer in peers_to_disconnect.chain(provers_to_disconnect) {
                // TODO (howardwu): Remove this after specializing this function.
                if self.router().node_type().is_prover() {
                    continue;
                }

                let peer_addr = peer.listener_addr;
                info!("Disconnecting from '{peer_addr}' (exceeded maximum connections)");
                self.router().send(peer_addr, Message::Disconnect(DisconnectReason::TooManyPeers.into()));
                // Disconnect from this peer.
                self.router().disconnect(peer_addr);
            }
        }

        // Obtain the number of connected peers.
        let num_connected = self.router().number_of_connected_peers();
        // Compute the number of deficit peers.
        let num_deficient = Self::MEDIAN_NUMBER_OF_PEERS.saturating_sub(num_connected);

        if num_deficient > 0 {
            // Initialize an RNG.
            let rng = &mut ChaChaRng::from_rng(&mut rand::rng());

            // Attempt to connect to more peers, separately choosing from those at a greater block
            // height, and those whose height is lower or unknown to us.
            let own_height = self.router().ledger.latest_block_height();
            let (higher_peers, other_peers): (Vec<_>, Vec<_>) = self
                .router()
                .get_candidate_peers()
                .into_iter()
                .partition(|peer| peer.last_height_seen.map(|h| h > own_height).unwrap_or(false));
            // We may not know of half of `num_deficient` candidates; account for it using `min`.
            let num_higher_peers = num_deficient.div_ceil(2).min(higher_peers.len());

            let higher_peers = higher_peers.into_iter().sample(rng, num_higher_peers);
            let other_peers = other_peers.into_iter().sample(rng, num_deficient.saturating_sub(num_higher_peers));

            // Initiate connection attempts and wait for them to complete.
            self.try_connect_to_peers(higher_peers.into_iter().chain(other_peers)).await;

            if !self.router().trusted_peers_only() {
                // Request more peers from the connected peers.
                for peer_ip in self.router().connected_peers().into_iter().sample(rng, 3) {
                    self.router().send(peer_ip, Message::PeerRequest(PeerRequest));
                }
            }
        }
    }

    /// This function keeps the number of bootstrap peers within the allowed range.
    async fn handle_bootstrap_peers(&self) {
        // Return early if we are in trusted peers only mode.
        if self.router().trusted_peers_only() {
            return;
        }
        // Split the bootstrap peers into connected and candidate lists.
        let mut candidate_bootstrap = Vec::new();
        let connected_bootstrap =
            self.router().filter_connected_peers(|peer| peer.node_type == NodeType::BootstrapClient);
        for bootstrap_ip in bootstrap_peers::<N>(self.router().is_dev()) {
            if !connected_bootstrap.iter().any(|peer| peer.listener_addr == bootstrap_ip) {
                candidate_bootstrap.push(bootstrap_ip);
            }
        }
        // If there are not enough connected bootstrap peers, connect to more.
        if connected_bootstrap.is_empty() {
            // Initialize an RNG.
            let rng = &mut ChaChaRng::from_rng(&mut rand::rng());
            // Attempt to connect to a random bootstrap peer.
            if let Some(peer_ip) = candidate_bootstrap.into_iter().choose(rng) {
                match self.router().connect(peer_ip) {
                    Ok(hdl) => {
                        Self::log_if_connect_error(
                            hdl.await,
                            &format!("Could not connect to bootstrap peer at '{peer_ip:?}'"),
                        );
                    }
                    Err(ConnectError::AlreadyConnected { .. }) | Err(ConnectError::AlreadyConnecting { .. }) => {}
                    Err(err) => warn!("Could not initiate connection to bootstrap peer at '{peer_ip:?}' - {err}"),
                }
            }
        }
        // Determine if the node is connected to more bootstrap peers than allowed.
        let num_surplus = connected_bootstrap.len().saturating_sub(1);
        if num_surplus > 0 {
            // Initialize an RNG.
            let rng = &mut ChaChaRng::from_rng(&mut rand::rng());
            // Proceed to send disconnect requests to these bootstrap peers.
            for peer in connected_bootstrap.into_iter().sample(rng, num_surplus) {
                info!("Disconnecting from '{}' (exceeded maximum bootstrap)", peer.listener_addr);
                self.router().send(peer.listener_addr, Message::Disconnect(DisconnectReason::TooManyPeers.into()));
                // Disconnect from this peer.
                self.router().disconnect(peer.listener_addr);
            }
        }
    }

    /// Helper function that attempts to connect the given peers.
    ///
    /// Used by [`Self::handle_trusted_peers`] and [`Self::handle_connected_peers`].
    async fn try_connect_to_peers(&self, peers: impl Iterator<Item = CandidatePeer<N>> + Send + 'static) {
        let (peer_info, hdls): (Vec<_>, Vec<_>) = peers
            .filter_map(|peer| {
                let peer_type = if peer.trusted { "trusted peer" } else { "peer" };

                // Do not attempt to reconnect too frequently.
                // TODO (kaimast): Consider increasing the minimum time based on the number of failed attempts.
                if let Some(last_connection_attempt) = peer.last_connection_attempt
                    && last_connection_attempt.elapsed() < Self::MINIMUM_TIME_BETWEEN_CONNECTION_ATTEMPTS
                {
                    return None;
                }

                // Get the peers address.
                let addr = peer.listener_addr;
                let attempt_no = peer.total_connection_attempts + 1;

                // Start connection attempt.
                debug!("(Re-)connecting to {peer_type} '{addr}' (attempt #{attempt_no})");
                match self.router().connect(addr) {
                    Ok(hdl) => Some(((addr, attempt_no, peer_type), hdl)),
                    Err(ConnectError::AlreadyConnected { .. }) | Err(ConnectError::AlreadyConnecting { .. }) => None,
                    Err(err) => {
                        warn!("Could not initiate connection to {peer_type} at '{addr}' - {err}");
                        None
                    }
                }
            })
            .unzip();

        // Wait for all the connection attempts to complete.
        for ((peer_addr, attempt_no, peer_type), result) in peer_info.into_iter().zip(join_all(hdls).await) {
            Self::log_if_connect_error(
                result,
                &format!("Could not connect to {peer_type} at '{peer_addr}' (attempt #{attempt_no})"),
            );
        }
    }

    /// This function attempts to connect to any disconnected trusted peers.
    async fn handle_trusted_peers(&self) {
        self.try_connect_to_peers(self.router().get_trusted_candidate_peers().into_iter()).await;
    }

    /// This function updates the puzzle if network has updated.
    fn handle_puzzle_request(&self) {
        // No-op
    }

    // Remove addresses whose ban time has expired.
    fn handle_banned_ips(&self) {
        self.router().tcp().banned_peers().remove_old_bans(Self::IP_BAN_TIME_IN_SECS);
    }

    fn clear_stale_peers(&self) {
        self.router().cache().clear_stale_entries(
            Router::<N>::CONNECTION_ATTEMPTS_SINCE_SECS,
            Router::<N>::MESSAGE_LIMIT_TIME_FRAME_IN_SECS,
        );
    }
}
