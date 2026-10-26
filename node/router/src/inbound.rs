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
    Outbound,
    PeerPoolHandling,
    Router,
    messages::{
        BlockRequest,
        BlockResponse,
        DataBlocks,
        Message,
        PeerResponse,
        Ping,
        Pong,
        UnconfirmedSolution,
        UnconfirmedTransaction,
    },
};
use snarkos_node_tcp::protocols::Reading;
use snarkvm::prelude::{
    ConsensusVersion,
    Network,
    block::{Block, Header, Transaction},
    puzzle::Solution,
};

use anyhow::{Result, anyhow, bail};
use std::net::SocketAddr;
use tokio::task::spawn_blocking;

/// The max number of peers to send in a `PeerResponse` message.
pub const MAX_PEERS_TO_SEND: usize = u8::MAX as usize;

#[async_trait]
pub trait Inbound<N: Network>: Reading + Outbound<N> {
    /// The maximum number of puzzle requests per interval.
    const MAXIMUM_PUZZLE_REQUESTS_PER_INTERVAL: usize = 5;
    /// The maximum number of block requests per interval.
    const MAXIMUM_BLOCK_REQUESTS_PER_INTERVAL: usize = 256;
    /// The maximum number of unconfirmed solutions per interval (~3800 per hour at 60s interval).
    const MAXIMUM_UNCONFIRMED_SOLUTIONS_PER_INTERVAL: usize = 64;
    /// The duration in seconds to sleep in between ping requests with a connected peer.
    const PING_SLEEP_IN_SECS: u64 = 20; // 20 seconds
    /// The maximum number of messages accepted within `MESSAGE_LIMIT_TIME_FRAME_IN_SECS`.
    const MESSAGE_LIMIT: usize = 500;

    /// Returns `true` if the message version is valid.
    fn is_valid_message_version(&self, message_version: u32) -> bool;

    /// Is the node synced enough to process unconfirmed transactions and solutions?
    fn is_within_sync_leniency(&self) -> bool {
        // The maximum number of blocks the client can be behind it's latest peer before it skips
        // processing incoming transactions and solutions.
        const SYNC_LENIENCY: u32 = 10;

        if let Some(num) = self.num_blocks_behind() {
            num <= SYNC_LENIENCY
        } else {
            // We have not received block locators yet.
            true
        }
    }

    /// Handles the inbound message from the peer. The returned value indicates whether
    /// the connection is still active, and errors cause a disconnect once they are
    /// propagated to the caller.
    async fn inbound(&self, peer_addr: SocketAddr, message: Message<N>) -> Result<bool> {
        // Retrieve the listener IP for the peer.
        let peer_ip = match self.router().resolve_to_listener(peer_addr) {
            Some(peer_ip) => peer_ip,
            None => {
                // No longer connected to the peer.
                trace!("Dropping a {} from {peer_addr} - no longer connected.", message.name());
                return Ok(false);
            }
        };

        // Drop the peer, if they have sent more than `MESSAGE_LIMIT` messages
        // in the last `MESSAGE_LIMIT_TIME_FRAME_IN_SECS` seconds.
        let num_messages =
            self.router().cache.insert_inbound_message(peer_ip, Router::<N>::MESSAGE_LIMIT_TIME_FRAME_IN_SECS);
        if num_messages > Self::MESSAGE_LIMIT {
            bail!("Dropping '{peer_ip}' for spamming messages (num_messages = {num_messages})")
        }

        trace!("Received '{}' from '{peer_ip}'", message.name());

        #[cfg(feature = "metrics")]
        metrics::increment_counter_2(metrics::router::EVENTS, "direction", "inbound", "type", message.variant_name());

        // Update the last seen timestamp of the peer.
        self.router().update_last_seen_for_connected_peer(peer_ip);

        // This match statement handles the inbound message by deserializing the message,
        // checking that the message is valid, and then calling the appropriate (trait) handler.
        match message {
            Message::BlockRequest(message) => {
                let BlockRequest { start_height, end_height } = &message;
                // Insert the block request for the peer, and fetch the recent frequency.
                let frequency = self.router().cache.insert_inbound_block_request(peer_ip);
                // Check if the number of block requests is within the limit.
                if frequency > Self::MAXIMUM_BLOCK_REQUESTS_PER_INTERVAL {
                    bail!("Peer '{peer_ip}' is not following the protocol (excessive block requests)")
                }
                // Ensure the block request is well-formed.
                if start_height >= end_height {
                    bail!("Block request from '{peer_ip}' has an invalid range ({start_height}..{end_height})")
                }
                // Ensure that the block request is within the allowed bounds.
                if end_height - start_height > DataBlocks::<N>::MAXIMUM_NUMBER_OF_BLOCKS as u32 {
                    bail!("Block request from '{peer_ip}' has an excessive range ({start_height}..{end_height})")
                }

                let node = self.clone();
                match spawn_blocking(move || node.block_request(peer_ip, message)).await? {
                    true => Ok(true),
                    false => bail!("Peer '{peer_ip}' sent an invalid block request"),
                }
            }
            Message::BlockResponse(BlockResponse { request, latest_consensus_version, blocks, .. }) => {
                // Remove the block request, checking if this node previously sent a block request to this peer.
                if !self.router().cache.remove_outbound_block_request(peer_ip, &request) {
                    bail!("Peer '{peer_ip}' is not following the protocol (unexpected block response)")
                }

                // Perform the deferred non-blocking deserialization of the blocks.
                // The deserialization can take a long time (minutes). We should not be running
                // this on a blocking task, but on a rayon thread pool.
                let (send, recv) = tokio::sync::oneshot::channel();
                rayon::spawn_fifo(move || {
                    let blocks = blocks.deserialize_blocking().map_err(|error| anyhow!("[BlockResponse] {error}"));
                    let _ = send.send(blocks);
                });
                let blocks = match recv.await {
                    Ok(Ok(blocks)) => blocks,
                    Ok(Err(error)) => bail!("Peer '{peer_ip}' sent an invalid block response - {error}"),
                    Err(error) => bail!("Peer '{peer_ip}' sent an invalid block response - {error}"),
                };

                // Ensure the block response is well-formed.
                blocks.ensure_response_is_well_formed(peer_ip, request.start_height, request.end_height)?;

                // Process the block response.
                let node = self.clone();
                match spawn_blocking(move || node.block_response(peer_ip, blocks.0, latest_consensus_version)).await? {
                    true => Ok(true),
                    false => bail!("Peer '{peer_ip}' sent an invalid block response"),
                }
            }
            Message::ChallengeRequest(..) | Message::ChallengeResponse(..) => {
                // Disconnect as the peer is not following the protocol.
                bail!("Peer '{peer_ip}' is not following the protocol")
            }
            Message::Disconnect(message) => {
                // The peer informs us that they had disconnected. Disconnect from them too.
                debug!("Peer '{peer_ip}' decided to disconnect due to '{}'", message.reason);
                self.router().disconnect(peer_ip);
                Ok(false)
            }
            Message::PeerRequest(..) => match self.peer_request(peer_ip) {
                true => Ok(true),
                false => bail!("Peer '{peer_ip}' sent an invalid peer request"),
            },
            Message::PeerResponse(message) => {
                if !self.router().cache.contains_outbound_peer_request(peer_ip) {
                    bail!("Peer '{peer_ip}' is not following the protocol (unexpected peer response)")
                }
                self.router().cache.decrement_outbound_peer_requests(peer_ip);
                if self.router().trusted_peers_only() {
                    bail!("Not accepting peer response from '{peer_ip}' (trusted peers only)");
                }

                match self.peer_response(peer_ip, message.peers) {
                    true => Ok(true),
                    false => bail!("Peer '{peer_ip}' sent an invalid peer response"),
                }
            }
            Message::Ping(message) => {
                // Ensure the message protocol version is not outdated.
                if !self.is_valid_message_version(message.version) {
                    bail!("Dropping '{peer_ip}' on message version {} (outdated)", message.version);
                }

                // If the peer is a prover, ensure there are no block locators.
                if message.node_type.is_prover() && message.block_locators.is_some() {
                    bail!("Peer '{peer_ip}' is a prover, but block locators were provided");
                }

                // Process the ping message.
                match self.ping(peer_ip, message) {
                    true => Ok(true),
                    false => bail!("Peer '{peer_ip}' sent an invalid ping"),
                }
            }
            Message::Pong(message) => match self.pong(peer_ip, message) {
                true => Ok(true),
                false => bail!("Peer '{peer_ip}' sent an invalid pong"),
            },
            Message::PuzzleRequest(..) => {
                // Insert the puzzle request for the peer, and fetch the recent frequency.
                let frequency = self.router().cache.insert_inbound_puzzle_request(peer_ip);
                // Check if the number of puzzle requests is within the limit.
                if frequency > Self::MAXIMUM_PUZZLE_REQUESTS_PER_INTERVAL {
                    bail!("Peer '{peer_ip}' is not following the protocol (excessive puzzle requests)")
                }
                // Process the puzzle request.
                match self.puzzle_request(peer_ip) {
                    true => Ok(true),
                    false => bail!("Peer '{peer_ip}' sent an invalid puzzle request"),
                }
            }
            Message::PuzzleResponse(message) => {
                // Check that this node previously sent a puzzle request to this peer.
                if !self.router().cache.contains_outbound_puzzle_request(&peer_ip) {
                    bail!("Peer '{peer_ip}' is not following the protocol (unexpected puzzle response)")
                }
                // Decrement the number of puzzle requests.
                self.router().cache.decrement_outbound_puzzle_requests(peer_ip);

                // Perform the deferred non-blocking deserialization of the block header.
                let header = match message.block_header.deserialize().await {
                    Ok(header) => header,
                    Err(error) => bail!("[PuzzleResponse] {error}"),
                };
                // Process the puzzle response.
                match self.puzzle_response(peer_ip, message.epoch_hash, header) {
                    true => Ok(true),
                    false => bail!("Peer '{peer_ip}' sent an invalid puzzle response"),
                }
            }
            Message::UnconfirmedSolution(message) => {
                // Insert the unconfirmed solution for the peer, and fetch the recent frequency.
                let frequency = self.router().cache.insert_inbound_unconfirmed_solution(peer_ip);
                // Check if the number of unconfirmed solutions is within the limit.
                if frequency > Self::MAXIMUM_UNCONFIRMED_SOLUTIONS_PER_INTERVAL {
                    bail!("Peer '{peer_ip}' is not following the protocol (excessive unconfirmed solutions)")
                }

                // Do not process unconfirmed solutions if the node is too far behind.
                if !self.is_within_sync_leniency() {
                    trace!("Skipped processing unconfirmed solution '{}' (node is syncing)", message.solution_id);
                    return Ok(true);
                }

                // Update the timestamp for the unconfirmed solution.
                let seen_before = self.router().cache.insert_inbound_solution(peer_ip, message.solution_id).is_some();
                // Determine whether to propagate the solution.
                if seen_before {
                    trace!("Skipping 'UnconfirmedSolution' from '{peer_ip}'");
                    return Ok(true);
                }
                // Clone the serialized message.
                let serialized = message.clone();
                // Perform the deferred non-blocking deserialization of the solution.
                let solution = match message.solution.deserialize().await {
                    Ok(solution) => solution,
                    Err(error) => bail!("[UnconfirmedSolution] {error}"),
                };
                // Check that the solution parameters match.
                if message.solution_id != solution.id() {
                    bail!("Peer '{peer_ip}' is not following the 'UnconfirmedSolution' protocol")
                }
                // Handle the unconfirmed solution.
                match self.unconfirmed_solution(peer_ip, serialized, solution).await {
                    true => Ok(true),
                    false => bail!("Peer '{peer_ip}' sent an invalid unconfirmed solution"),
                }
            }
            Message::UnconfirmedTransaction(message) => {
                // Do not process unconfirmed solutions if the node is too far behind.
                if !self.is_within_sync_leniency() {
                    trace!("Skipped processing unconfirmed transaction '{}' (node is syncing)", message.transaction_id);
                    return Ok(true);
                }
                // Update the timestamp for the unconfirmed transaction.
                let seen_before =
                    self.router().cache.insert_inbound_transaction(peer_ip, message.transaction_id).is_some();
                // Determine whether to propagate the transaction.
                if seen_before {
                    trace!("Skipping 'UnconfirmedTransaction' from '{peer_ip}'");
                    return Ok(true);
                }
                // Clone the serialized message.
                let serialized = message.clone();
                // Perform the deferred non-blocking deserialization of the transaction.
                let transaction = match message.transaction.deserialize().await {
                    Ok(transaction) => transaction,
                    Err(error) => bail!("[UnconfirmedTransaction] {error}"),
                };
                // Check that the transaction parameters match.
                if message.transaction_id != transaction.id() {
                    bail!("Peer '{peer_ip}' is not following the 'UnconfirmedTransaction' protocol")
                }
                // Handle the unconfirmed transaction.
                match self.unconfirmed_transaction(peer_ip, serialized, transaction).await {
                    true => Ok(true),
                    false => bail!("Peer '{peer_ip}' sent an invalid unconfirmed transaction"),
                }
            }
        }
    }

    /// Handles a `BlockRequest` message.
    fn block_request(&self, peer_ip: SocketAddr, _message: BlockRequest) -> bool;

    /// Handles a `BlockResponse` message.
    fn block_response(
        &self,
        peer_ip: SocketAddr,
        blocks: Vec<Block<N>>,
        latest_consensus_version: Option<ConsensusVersion>,
    ) -> bool;

    /// Handles a `PeerRequest` message.
    fn peer_request(&self, peer_ip: SocketAddr) -> bool {
        let peers = self.router().get_best_connected_peers(Some(MAX_PEERS_TO_SEND));
        let peers = peers.into_iter().map(|peer| (peer.listener_addr, peer.last_height_seen)).collect();

        // Send a `PeerResponse` message to the peer.
        self.router().send(peer_ip, Message::PeerResponse(PeerResponse { peers }));
        true
    }

    /// Handles a `PeerResponse` message.
    fn peer_response(&self, _peer_ip: SocketAddr, peers: Vec<(SocketAddr, Option<u32>)>) -> bool {
        // Check if the number of peers received is less than MAX_PEERS_TO_SEND.
        if peers.len() > MAX_PEERS_TO_SEND {
            return false;
        }
        // Adds the given peer IPs to the list of candidate peers.
        if !peers.is_empty() {
            self.router().insert_candidate_peers(peers);
        }

        #[cfg(feature = "metrics")]
        self.router().update_metrics();

        true
    }

    /// Handles a `Ping` message.
    fn ping(&self, peer_ip: SocketAddr, message: Ping<N>) -> bool;

    /// Sleeps for a period and then sends a `Ping` message to the peer.
    fn pong(&self, peer_ip: SocketAddr, _message: Pong) -> bool;

    /// Handles a `PuzzleRequest` message.
    fn puzzle_request(&self, peer_ip: SocketAddr) -> bool;

    /// Handles a `PuzzleResponse` message.
    fn puzzle_response(&self, peer_ip: SocketAddr, _epoch_hash: N::BlockHash, _header: Header<N>) -> bool;

    /// Handles an `UnconfirmedSolution` message.
    async fn unconfirmed_solution(
        &self,
        peer_ip: SocketAddr,
        serialized: UnconfirmedSolution<N>,
        solution: Solution<N>,
    ) -> bool;

    /// Handles an `UnconfirmedTransaction` message.
    async fn unconfirmed_transaction(
        &self,
        peer_ip: SocketAddr,
        serialized: UnconfirmedTransaction<N>,
        _transaction: Transaction<N>,
    ) -> bool;
}
