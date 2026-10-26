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
use snarkos_node_router::{
    Routing,
    messages::{
        BlockRequest,
        BlockResponse,
        DataBlocks,
        DisconnectReason,
        MessageCodec,
        PeerRequest,
        Ping,
        Pong,
        PuzzleResponse,
        UnconfirmedTransaction,
    },
};
use snarkos_node_tcp::{ConnectError, Connection, ConnectionSide, Tcp, connections::DisconnectOrigin};
use snarkvm::{
    console::network::{ConsensusVersion, Network},
    ledger::{block::Transaction, narwhal::Data},
    utilities::flatten_error,
};

use std::{io, net::SocketAddr};

impl<N: Network, C: ConsensusStorage<N>> P2P for Client<N, C> {
    /// Returns a reference to the TCP instance.
    fn tcp(&self) -> &Tcp {
        self.router.tcp()
    }
}

#[async_trait]
impl<N: Network, C: ConsensusStorage<N>> Handshake for Client<N, C> {
    /// Performs the handshake protocol.
    async fn perform_handshake(&self, mut connection: Connection) -> Result<Connection, ConnectError> {
        // Perform the handshake.
        let peer_addr = connection.addr();
        let conn_side = connection.side();
        let stream = self.borrow_stream(&mut connection);
        // Make the socket more robust.
        harden_socket(stream)?;
        let genesis_header = *self.genesis.header();
        let restrictions_id = self.ledger.vm().restrictions().restrictions_id();

        self.router.handshake(peer_addr, stream, conn_side, genesis_header, restrictions_id).await?;

        Ok(connection)
    }
}

#[async_trait]
impl<N: Network, C: ConsensusStorage<N>> OnConnect for Client<N, C> {
    async fn on_connect(&self, peer_addr: SocketAddr) {
        // Resolve the peer address to the listener address.
        if let Some(listener_addr) = self.router().resolve_to_listener(peer_addr)
            && let Some(peer) = self.router().get_connected_peer(listener_addr)
        {
            // If it's a bootstrap client, only request its peers.
            if peer.node_type == NodeType::BootstrapClient {
                self.router().send(listener_addr, Message::PeerRequest(PeerRequest));
            } else {
                // Send the first `Ping` message to the peer.
                self.ping.on_peer_connected(listener_addr);
            }
        }
    }
}

#[async_trait]
impl<N: Network, C: ConsensusStorage<N>> Disconnect for Client<N, C> {
    /// Any extra operations to be performed during a disconnect.
    async fn handle_disconnect(&self, peer_addr: SocketAddr, origin: DisconnectOrigin) {
        debug!("Physically disconnecting from {peer_addr}; origin: {origin:?}");

        if let Some(peer_ip) = self.router.resolve_to_listener(peer_addr) {
            let was_fully_connected = self.router.downgrade_peer_to_candidate(peer_ip);

            // Only remove the peer from sync if the handshake was successful.
            // This handles the cases where a client unsuccessfully tries to connect to another client using the router.
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
impl<N: Network, C: ConsensusStorage<N>> Reading for Client<N, C> {
    type Codec = MessageCodec<N>;
    type Message = Message<N>;

    /// Creates a [`Decoder`] used to interpret messages from the network.
    /// The `side` param indicates the connection side **from the node's perspective**.
    fn codec(&self, _peer_addr: SocketAddr, _side: ConnectionSide) -> Self::Codec {
        Default::default()
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

impl<N: Network, C: ConsensusStorage<N>> Client<N, C> {
    async fn process_message_inner(
        &self,
        peer_addr: SocketAddr,
        message: <Client<N, C> as snarkos_node_tcp::protocols::Reading>::Message,
    ) {
        // Process the message. Disconnect if the peer violated the protocol.
        if let Err(error) = self.inbound(peer_addr, message).await {
            warn!("Failed to process inbound message from '{peer_addr}' - {error}");

            //TODO(kaimast): set disconnect reason based on error
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
impl<N: Network, C: ConsensusStorage<N>> Routing<N> for Client<N, C> {}

impl<N: Network, C: ConsensusStorage<N>> Heartbeat<N> for Client<N, C> {}

impl<N: Network, C: ConsensusStorage<N>> Outbound<N> for Client<N, C> {
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
impl<N: Network, C: ConsensusStorage<N>> Inbound<N> for Client<N, C> {
    /// Returns `true` if the message version is valid.
    fn is_valid_message_version(&self, message_version: u32) -> bool {
        self.router().is_valid_message_version(message_version)
    }

    /// Handles a `BlockRequest` message.
    fn block_request(&self, peer_ip: SocketAddr, message: BlockRequest) -> bool {
        let BlockRequest { start_height, end_height } = &message;

        // Get the latest consensus version, i.e., the one for the last block's height.
        let latest_consensus_version = match N::CONSENSUS_VERSION(end_height.saturating_sub(1)) {
            Ok(version) => version,
            Err(err) => {
                let err = err.context("Failed to retrieve consensus version");
                error!("{}", flatten_error(&err));
                return false;
            }
        };

        // Reply empty rather than disconnect: the peer learns we cannot serve this range (e.g. pruned).
        let blocks = match self.ledger.get_blocks(*start_height..*end_height) {
            Ok(blocks) => DataBlocks(blocks),
            Err(error) => {
                let err =
                    error.context(format!("Cannot serve block request {start_height}..{end_height} from the ledger"));
                warn!("{}", flatten_error(&err));
                DataBlocks(vec![])
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
        blocks: Vec<Block<N>>,
        latest_consensus_version: Option<ConsensusVersion>,
    ) -> bool {
        // We do not need to explicitly sync here because insert_block_response, will wake up the sync task.
        match self.sync.insert_block_responses(peer_ip, blocks, latest_consensus_version) {
            Ok(_) => true,
            Err(err) if err.is_benign() => {
                let err: anyhow::Error = err.into();
                debug!("{}", flatten_error(err.context(format!("Ignoring block response from peer '{peer_ip}'"))));
                true
            }
            Err(err) if err.is_consensus_version_ahead() => {
                let err: anyhow::Error = err.into();
                let err = err.context(format!("Peer sent a block response with a newer consensus version '{peer_ip}'"));
                warn!("{}", flatten_error(&err));
                true
            }
            Err(err) if err.is_consensus_version_behind() => {
                // If the error indicates the peer missed an upgrade and forked, ban it.
                let err: anyhow::Error = err.into();
                let err = err.context(format!("Peer sent an invalid block response '{peer_ip}'"));

                let msg = flatten_error(&err);
                error!("{msg}");
                self.router().ip_ban_peer(peer_ip, Some(&err.to_string()));

                false
            }
            Err(err) => {
                let err: anyhow::Error = err.into();
                let err = err.context(format!("Failed to insert block response from '{peer_ip}'"));
                warn!("{}", flatten_error(err));

                // TODO(kaimast): This needs more testing to ensure disconnect is the correct action.
                true
            }
        }
    }

    /// Processes the block locators and sends back a `Pong` message.
    fn ping(&self, peer_ip: SocketAddr, message: Ping<N>) -> bool {
        // If block locators were provided, then update the peer in the sync pool.
        if let Some(block_locators) = message.block_locators {
            // Check the block locators are valid, and update the peer in the sync pool.
            if let Err(err) = self.sync.update_peer_locators(peer_ip, &block_locators) {
                warn!("{}", flatten_error(err.context(format!("Peer '{peer_ip}' sent invalid block locators"))));
                return false;
            }

            let last_peer_height = Some(block_locators.latest_locator_height());
            self.router().update_connected_peer(&peer_ip, |peer| peer.last_height_seen = last_peer_height);
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

    /// Retrieves the latest epoch hash and latest block header, and returns the puzzle response to the peer.
    fn puzzle_request(&self, peer_ip: SocketAddr) -> bool {
        // Retrieve the latest epoch hash.
        let epoch_hash = match self.ledger.latest_epoch_hash() {
            Ok(epoch_hash) => epoch_hash,
            Err(err) => {
                let err = err.context(format!("Failed to prepare a puzzle request for '{peer_ip}'"));
                error!("{}", flatten_error(err));
                return false;
            }
        };
        // Retrieve the latest block header.
        let block_header = Data::Object(self.ledger.latest_header());
        // Send the `PuzzleResponse` message to the peer.
        self.router().send(peer_ip, Message::PuzzleResponse(PuzzleResponse { epoch_hash, block_header }));
        true
    }

    /// Saves the latest epoch hash and latest block header in the node.
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
        // Try to add the solution to the verification queue, without changing LRU status of known solutions.
        let mut solution_queue = self.solution_queue.lock();
        if !solution_queue.contains(&solution.id()) {
            solution_queue.put(solution.id(), (peer_ip, serialized, solution));
        }

        true // Maintain the connection
    }

    /// Handles an `UnconfirmedTransaction` message.
    async fn unconfirmed_transaction(
        &self,
        peer_ip: SocketAddr,
        serialized: UnconfirmedTransaction<N>,
        transaction: Transaction<N>,
    ) -> bool {
        // Try to add the transaction to a verification queue, without changing LRU status of known transactions.
        match &transaction {
            Transaction::<N>::Fee(..) => (), // Fee Transactions are not valid.
            Transaction::<N>::Deploy(..) => {
                let mut deploy_queue = self.deploy_queue.lock();
                if !deploy_queue.contains(&transaction.id()) {
                    deploy_queue.put(transaction.id(), (peer_ip, serialized, transaction));
                }
            }
            Transaction::<N>::Execute(..) => {
                let mut execute_queue = self.execute_queue.lock();
                if !execute_queue.contains(&transaction.id()) {
                    execute_queue.put(transaction.id(), (peer_ip, serialized, transaction));
                }
            }
        }

        true // Maintain the connection
    }
}
