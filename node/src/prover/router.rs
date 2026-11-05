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

use super::*;

use snarkos_node_network::harden_socket;
use snarkos_node_router::messages::{
    BlockRequest,
    DisconnectReason,
    Message,
    MessageCodec,
    Ping,
    Pong,
    PuzzleRequest,
    UnconfirmedTransaction,
};
use snarkos_node_tcp::{ConnectError, Connection, ConnectionSide, Tcp, connections::DisconnectOrigin};
use snarkvm::{
    console::network::{ConsensusVersion, Network},
    ledger::block::Transaction,
    prelude::{Field, Zero},
    utilities::into_io_error,
};

use std::{io, net::SocketAddr};

impl<N: Network, C: ConsensusStorage<N>> P2P for Prover<N, C> {
    /// Returns a reference to the TCP instance.
    fn tcp(&self) -> &Tcp {
        self.router.tcp()
    }
}

#[async_trait]
impl<N: Network, C: ConsensusStorage<N>> Handshake for Prover<N, C> {
    /// Performs the handshake protocol.
    async fn perform_handshake(&self, mut connection: Connection) -> Result<Connection, ConnectError> {
        // Perform the handshake.
        let peer_addr = connection.addr();
        let conn_side = connection.side();
        let stream = self.borrow_stream(&mut connection);
        // Make the socket more robust.
        harden_socket(stream)?;
        let genesis_header = *self.genesis.header();
        let restrictions_id = Field::zero(); // Provers may bypass restrictions, since they do not validate transactions.

        self.router
            .handshake(peer_addr, stream, conn_side, genesis_header, restrictions_id)
            .await
            .map_err(into_io_error)?;

        Ok(connection)
    }
}

#[async_trait]
impl<N: Network, C: ConsensusStorage<N>> OnConnect for Prover<N, C>
where
    Self: Outbound<N>,
{
    async fn on_connect(&self, peer_addr: SocketAddr) {
        // Resolve the peer address to the listener address.
        if let Some(listener_addr) = self.router().resolve_to_listener(peer_addr)
            && let Some(peer) = self.router().get_connected_peer(listener_addr)
            && peer.node_type != NodeType::BootstrapClient
        {
            // Send the first `Ping` message to the peer.
            self.ping.on_peer_connected(listener_addr);
        }
    }
}

#[async_trait]
impl<N: Network, C: ConsensusStorage<N>> Disconnect for Prover<N, C> {
    /// Any extra operations to be performed during a disconnect.
    async fn handle_disconnect(&self, peer_addr: SocketAddr, origin: DisconnectOrigin) {
        debug!("Physically disconnecting from {peer_addr}; origin: {origin:?}");

        if let Some(peer_ip) = self.router.resolve_to_listener(peer_addr) {
            let was_fully_connected = self.router.downgrade_peer_to_candidate(peer_ip);
            // Only remove the peer from sync if the handshake was successful.
            if was_fully_connected {
                self.sync.remove_peer(&peer_ip);
            }
            // Clear cached entries applicable to the peer.
            self.router.cache().clear_peer_entries(peer_ip);
            #[cfg(feature = "metrics")]
            self.router.update_metrics();
        } else {
            warn!("Got disconnect for a peer '{peer_addr}' that is not in the peer pool");
        }
    }
}

#[async_trait]
impl<N: Network, C: ConsensusStorage<N>> Reading for Prover<N, C> {
    type Codec = MessageCodec<N>;
    type Message = Message<N>;

    /// Creates a [`Decoder`] used to interpret messages from the network.
    /// The `side` param indicates the connection side **from the node's perspective**.
    fn codec(&self, _peer_addr: SocketAddr, _side: ConnectionSide) -> Self::Codec {
        Default::default()
    }

    /// Processes a message received from the network.
    async fn process_message(&self, peer_addr: SocketAddr, message: Self::Message) -> io::Result<()> {
        // Process the message. Disconnect if the peer violated the protocol.
        if let Err(error) = self.inbound(peer_addr, message).await
            && let Some(peer_ip) = self.router().resolve_to_listener(peer_addr)
        {
            warn!("Disconnecting from '{peer_addr}' - {error}");
            self.router().send(peer_ip, Message::Disconnect(DisconnectReason::ProtocolViolation.into()));
            // Disconnect from this peer.
            self.router().disconnect(peer_ip);
        }
        Ok(())
    }
}

#[async_trait]
impl<N: Network, C: ConsensusStorage<N>> Routing<N> for Prover<N, C> {}

impl<N: Network, C: ConsensusStorage<N>> Heartbeat<N> for Prover<N, C> {
    /// This function updates the puzzle if network has updated.
    fn handle_puzzle_request(&self) {
        // Find the sync peers.
        if let Some((sync_peers, _)) = self.sync.find_sync_peers() {
            // Choose the peer with the highest block height.
            if let Some((peer_ip, _)) = sync_peers.into_iter().max_by_key(|(_, height)| *height) {
                // Request the puzzle from the peer.
                self.router().send(peer_ip, Message::PuzzleRequest(PuzzleRequest));
            }
        }
    }
}

impl<N: Network, C: ConsensusStorage<N>> Outbound<N> for Prover<N, C> {
    /// Returns a reference to the router.
    fn router(&self) -> &Router<N> {
        &self.router
    }

    /// Returns `true` if the node is synced up to the latest block (within the given tolerance).
    fn is_block_synced(&self) -> bool {
        true
    }

    /// Returns the number of blocks this node is behind the greatest peer height,
    /// or `None` if not connected to peers yet.
    fn num_blocks_behind(&self) -> Option<u32> {
        //TODO(kaimast): should this return None instead?
        Some(0)
    }

    /// Returns the current sync speed in blocks per second.
    fn get_sync_speed(&self) -> f64 {
        0.0
    }
}

#[async_trait]
impl<N: Network, C: ConsensusStorage<N>> Inbound<N> for Prover<N, C> {
    /// Returns `true` if the message version is valid.
    fn is_valid_message_version(&self, message_version: u32) -> bool {
        self.router().is_valid_message_version(message_version)
    }

    /// Handles a `BlockRequest` message.
    fn block_request(&self, peer_ip: SocketAddr, _message: BlockRequest) -> bool {
        debug!("Disconnecting '{peer_ip}' for the following reason - {}", DisconnectReason::ProtocolViolation);
        false
    }

    /// Handles a `BlockResponse` message.
    fn block_response(
        &self,
        peer_ip: SocketAddr,
        _start_height: u32,
        _end_height: u32,
        _blocks: Vec<Block<N>>,
        _latest_consensus_version: Option<ConsensusVersion>,
    ) -> bool {
        debug!("Disconnecting '{peer_ip}' for the following reason - {}", DisconnectReason::ProtocolViolation);
        false
    }

    /// Processes the block locators and sends back a `Pong` message.
    fn ping(&self, peer_ip: SocketAddr, message: Ping<N>) -> bool {
        // If block locators were provided, then update the peer in the sync pool.
        if let Some(block_locators) = message.block_locators {
            // Check the block locators are valid, and update the peer in the sync pool.
            if let Err(error) = self.sync.update_peer_locators(peer_ip, &block_locators) {
                warn!("Peer '{peer_ip}' sent invalid block locators: {error}");
                return false;
            }
        }

        // Send a `Pong` message to the peer.
        self.router().send(peer_ip, Message::Pong(Pong { is_fork: Some(false) }));
        true
    }

    /// Sleeps for a period and then sends a `Ping` message to the peer.
    fn pong(&self, peer_ip: SocketAddr, _message: Pong) -> bool {
        self.ping.on_pong_received(peer_ip);
        true
    }

    /// Disconnects on receipt of a `PuzzleRequest` message.
    fn puzzle_request(&self, peer_ip: SocketAddr) -> bool {
        debug!("Disconnecting '{peer_ip}' for the following reason - {}", DisconnectReason::ProtocolViolation);
        false
    }

    /// Saves the latest epoch hash and latest block header in the node.
    fn puzzle_response(&self, peer_ip: SocketAddr, epoch_hash: N::BlockHash, header: Header<N>) -> bool {
        // Retrieve the block height.
        let block_height = header.height();

        info!(
            "Puzzle (Block {block_height}, Coinbase Target {}, Proof Target {})",
            header.coinbase_target(),
            header.proof_target()
        );

        // Save the latest epoch hash in the node.
        self.latest_epoch_hash.write().replace(epoch_hash);
        // Save the latest block header in the node.
        self.latest_block_header.write().replace(header);

        trace!("Received 'PuzzleResponse' from '{peer_ip}' (Block {block_height})");
        true
    }

    /// Propagates the unconfirmed solution to all connected validators.
    async fn unconfirmed_solution(
        &self,
        peer_ip: SocketAddr,
        serialized: UnconfirmedSolution<N>,
        solution: Solution<N>,
    ) -> bool {
        // Retrieve the latest epoch hash.
        let epoch_hash = *self.latest_epoch_hash.read();
        // Retrieve the latest proof target.
        let proof_target = self.latest_block_header.read().as_ref().map(|header| header.proof_target());

        if let (Some(epoch_hash), Some(proof_target)) = (epoch_hash, proof_target) {
            // Ensure that the solution is valid for the given epoch.
            let puzzle = self.puzzle.clone();
            let is_valid =
                tokio::task::spawn_blocking(move || puzzle.check_solution(&solution, epoch_hash, proof_target)).await;

            match is_valid {
                // If the solution is valid, propagate the `UnconfirmedSolution`.
                Ok(Ok(())) => {
                    let message = Message::UnconfirmedSolution(serialized);
                    // Propagate the "UnconfirmedSolution".
                    self.propagate(message, &[peer_ip]);
                }
                Ok(Err(_)) => {
                    trace!("Invalid solution '{}' for the proof target.", solution.id())
                }
                // If error occurs after the first 10 blocks of the epoch, log it as a warning, otherwise ignore.
                Err(error) => {
                    if let Some(height) = self.latest_block_header.read().as_ref().map(|header| header.height())
                        && height % N::NUM_BLOCKS_PER_EPOCH > 10
                    {
                        warn!("Failed to verify the solution - {error}")
                    }
                }
            }
        }
        true
    }

    /// Handles an `UnconfirmedTransaction` message.
    async fn unconfirmed_transaction(
        &self,
        _peer_ip: SocketAddr,
        _serialized: UnconfirmedTransaction<N>,
        _transaction: Transaction<N>,
    ) -> bool {
        true
    }
}
