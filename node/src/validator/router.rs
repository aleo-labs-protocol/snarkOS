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
use snarkos_node_network::{PeerPoolHandling, harden_socket};
use snarkos_node_router::messages::{
    BlockRequest,
    BlockResponse,
    DataBlocks,
    DisconnectReason,
    Message,
    MessageCodec,
    MessageId,
    Ping,
    Pong,
    UnconfirmedTransaction,
};
use snarkos_node_tcp::{ConnectError, Connection, ConnectionSide, Tcp, connections::DisconnectOrigin};
use snarkvm::{
    console::network::{ConsensusVersion, Network},
    ledger::{block::Transaction, narwhal::Data},
    utilities::flatten_error,
};

use std::{io, net::SocketAddr};

impl<N: Network, C: ConsensusStorage<N>> P2P for Validator<N, C> {
    /// Returns a reference to the TCP instance.
    fn tcp(&self) -> &Tcp {
        self.router.tcp()
    }
}

#[async_trait]
impl<N: Network, C: ConsensusStorage<N>> Handshake for Validator<N, C> {
    /// Performs the handshake protocol.
    async fn perform_handshake(&self, mut connection: Connection) -> Result<Connection, ConnectError> {
        // Perform the handshake.
        let peer_addr = connection.addr();
        let conn_side = connection.side();
        let stream = self.borrow_stream(&mut connection);
        // Make the socket more robust.
        harden_socket(stream)?;
        //TODO(kaimast): if this fails, the validator must be corrupted. Handle this with higher severity.
        let genesis_header = self.ledger.get_header(0).map_err(ConnectError::other)?;
        let restrictions_id = self.ledger.vm().restrictions().restrictions_id();
        self.router.handshake(peer_addr, stream, conn_side, genesis_header, restrictions_id).await?;

        Ok(connection)
    }
}

#[async_trait]
impl<N: Network, C: ConsensusStorage<N>> OnConnect for Validator<N, C>
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
impl<N: Network, C: ConsensusStorage<N>> Disconnect for Validator<N, C> {
    /// Any extra operations to be performed during a disconnect.
    async fn handle_disconnect(&self, peer_addr: SocketAddr, origin: DisconnectOrigin) {
        debug!("Physically disconnecting from {peer_addr}; origin: {origin:?}");

        if let Some(peer_ip) = self.router.resolve_to_listener(peer_addr) {
            self.router.downgrade_peer_to_candidate(peer_ip);

            // Validators do not sync from clients in the current design, so do not remove the peer from sync here.

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
impl<N: Network, C: ConsensusStorage<N>> Reading for Validator<N, C> {
    type Codec = MessageCodec<N>;
    type Message = Message<N>;

    /// Creates a [`Decoder`] used to interpret messages from the network.
    /// The `side` param indicates the connection side **from the node's perspective**.
    fn codec(&self, _peer_addr: SocketAddr, _side: ConnectionSide) -> Self::Codec {
        // A validator syncs blocks exclusively over the committee-gated BFT gateway
        // (`ConnectionMode::Gateway`); it never issues a `BlockRequest` over the router, so it
        // never has a legitimate reason to accept a `BlockResponse` there either. Excluding it
        // rejects one outright the moment its ID is visible, and shrinks this connection's frame
        // ceiling from the general 128 MiB down to `UnconfirmedTransaction`'s (the largest type
        // still allowed, and one a validator must accept) - so an untrusted router peer can no
        // longer use a claimed `BlockResponse` to force either a 128 MiB allocation or a
        // successful decode that `block_response` below only rejects afterward.
        MessageCodec::excluding(&[MessageId::BlockResponse])
    }

    /// Processes a message received from the network.
    async fn process_message(&self, peer_addr: SocketAddr, message: Self::Message) -> io::Result<()> {
        let clone = self.clone();
        if matches!(message, Message::BlockRequest(_) | Message::BlockResponse(_)) {
            // Handle BlockRequest and BlockResponse messages in a separate task to not block the
            // inbound queue.
            tokio::spawn(async move {
                clone.process_message_inner(peer_addr, message).await;
            });
        } else {
            self.process_message_inner(peer_addr, message).await;
        }
        Ok(())
    }
}

impl<N: Network, C: ConsensusStorage<N>> Validator<N, C> {
    async fn process_message_inner(
        &self,
        peer_addr: SocketAddr,
        message: <Validator<N, C> as snarkos_node_tcp::protocols::Reading>::Message,
    ) {
        // Process the message. Disconnect if the peer violated the protocol.
        if let Err(error) = self.inbound(peer_addr, message).await {
            warn!("Failed to process inbound message from '{peer_addr}' - {error}");
            if let Some(peer_ip) = self.router().resolve_to_listener(peer_addr) {
                warn!("Disconnecting from '{peer_ip}' for protocol violation");
                self.router().send(peer_ip, Message::Disconnect(DisconnectReason::ProtocolViolation.into()));
                // Disconnect from this peer.
                self.router().disconnect(peer_ip);
            }
        }
    }
}

#[async_trait]
impl<N: Network, C: ConsensusStorage<N>> Routing<N> for Validator<N, C> {}

impl<N: Network, C: ConsensusStorage<N>> Heartbeat<N> for Validator<N, C> {}

impl<N: Network, C: ConsensusStorage<N>> Outbound<N> for Validator<N, C> {
    /// Returns a reference to the router.
    fn router(&self) -> &Router<N> {
        &self.router
    }

    /// Returns `true` if the node is synced up to the latest block (within the given tolerance).
    fn is_block_synced(&self) -> bool {
        self.sync.is_block_synced()
    }

    /// Returns the number of blocks this node is behind the greatest peer height,
    /// or `None` if not connected to peers yet.
    fn num_blocks_behind(&self) -> Option<u32> {
        self.sync.num_blocks_behind()
    }

    /// Returns the current sync speed in blocks per second.
    fn get_sync_speed(&self) -> f64 {
        self.sync.get_sync_speed()
    }
}

#[async_trait]
impl<N: Network, C: ConsensusStorage<N>> Inbound<N> for Validator<N, C> {
    /// Returns `true` if the message version is valid.
    fn is_valid_message_version(&self, message_version: u32) -> bool {
        self.router().is_valid_message_version(message_version)
    }

    /// Retrieves the blocks within the block request range, and returns the block response to the peer.
    fn block_request(&self, peer_ip: SocketAddr, message: BlockRequest) -> bool {
        let BlockRequest { start_height, end_height } = &message;

        // Get the latest consensus version, i.e., the one for the last block's height.
        let latest_consensus_version = match N::CONSENSUS_VERSION(end_height.saturating_sub(1)) {
            Ok(version) => version,
            Err(err) => {
                error!("{}", flatten_error(err.context("Failed to retrieve consensus version")));
                return false;
            }
        };

        // Retrieve the blocks within the requested range.
        let blocks = match self.ledger.get_blocks(*start_height..*end_height) {
            Ok(blocks) => DataBlocks(blocks),
            Err(err) => {
                let err =
                    err.context(format!("Failed to retrieve blocks {start_height} to {end_height} from the ledger"));
                error!("{}", flatten_error(err));
                return false;
            }
        };
        // Send the `BlockResponse` message to the peer.
        self.router()
            .send(peer_ip, Message::BlockResponse(BlockResponse::new(message, blocks, latest_consensus_version)));
        true
    }

    /// Handles a `BlockResponse` message.
    fn block_response(
        &self,
        peer_ip: SocketAddr,
        _blocks: Vec<Block<N>>,
        _latest_consensus_version: Option<ConsensusVersion>,
    ) -> bool {
        warn!("Received a block response through P2P, not BFT, from {peer_ip}");
        false
    }

    /// Processes a ping message from a client (or prover) and sends back a `Pong` message.
    fn ping(&self, peer_ip: SocketAddr, _message: Ping<N>) -> bool {
        // In gateway/validator mode, we do not need to process client block locators.
        // Instead, locators are fetched from other validators in `Gateway` using `PrimaryPing` messages.

        // Send a `Pong` message to the peer.
        self.router().send(peer_ip, Message::Pong(Pong { is_fork: Some(false) }));
        true
    }

    /// Process a Pong message (response to a Ping).
    fn pong(&self, peer_ip: SocketAddr, _message: Pong) -> bool {
        self.ping.on_pong_received(peer_ip);
        true
    }

    /// Retrieves the latest epoch hash and latest block header, and returns the puzzle response to the peer.
    fn puzzle_request(&self, peer_ip: SocketAddr) -> bool {
        // Retrieve the latest epoch hash.
        let epoch_hash = match self.ledger.latest_epoch_hash() {
            Ok(epoch_hash) => epoch_hash,
            Err(error) => {
                error!("Failed to prepare a puzzle request for '{peer_ip}': {error}");
                return false;
            }
        };
        // Retrieve the latest block header.
        let block_header = Data::Object(self.ledger.latest_header());
        // Send the `PuzzleResponse` message to the peer.
        self.router().send(peer_ip, Message::PuzzleResponse(PuzzleResponse { epoch_hash, block_header }));
        true
    }

    /// Disconnects on receipt of a `PuzzleResponse` message.
    fn puzzle_response(&self, peer_ip: SocketAddr, _epoch_hash: N::BlockHash, _header: Header<N>) -> bool {
        debug!("Disconnecting '{peer_ip}' for the following reason - {}", DisconnectReason::ProtocolViolation);
        false
    }

    /// Propagates the unconfirmed solution to all connected validators.
    async fn unconfirmed_solution(
        &self,
        peer_ip: SocketAddr,
        serialized: UnconfirmedSolution<N>,
        solution: Solution<N>,
    ) -> bool {
        // Add the unconfirmed solution to the memory pool.
        if let Err(error) = self.consensus.add_unconfirmed_solution(solution).await {
            trace!("[UnconfirmedSolution] {error}");
            return true; // Maintain the connection.
        }
        let message = Message::UnconfirmedSolution(serialized);
        // Propagate the "UnconfirmedSolution" to the connected validators.
        self.propagate_to_validators(message, &[peer_ip]);
        #[cfg(feature = "metrics")]
        snarkos_node_metrics::increment_counter_1(
            snarkos_node_metrics::router::UNCONFIRMED_SOLUTIONS_PROPAGATED,
            "site",
            "validator",
        );
        true
    }

    /// Handles an `UnconfirmedTransaction` message.
    async fn unconfirmed_transaction(
        &self,
        peer_ip: SocketAddr,
        serialized: UnconfirmedTransaction<N>,
        transaction: Transaction<N>,
    ) -> bool {
        // Add the unconfirmed transaction to the memory pool.
        if let Err(error) = self.consensus.add_unconfirmed_transaction(transaction).await {
            trace!("[UnconfirmedTransaction] {error}");
            return true; // Maintain the connection.
        }
        let message = Message::UnconfirmedTransaction(serialized);
        // Propagate the "UnconfirmedTransaction" to the connected validators.
        self.propagate_to_validators(message, &[peer_ip]);
        #[cfg(feature = "metrics")]
        snarkos_node_metrics::increment_counter_1(
            snarkos_node_metrics::router::UNCONFIRMED_TRANSACTIONS_PROPAGATED,
            "site",
            "validator",
        );
        true
    }
}
