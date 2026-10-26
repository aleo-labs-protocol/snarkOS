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

//! Test helpers for exercising a `Router` and anything generic over [`Routing`].
//!
//! This module is gated behind the `test-helpers` feature so that other crates can build a node
//! component that requires `R: Routing<N>` without standing up a real node. It is the single
//! definition of `TestRouter`; the router's own integration tests re-export it from here.

use snarkos_account::Account;
use snarkos_node_bft_ledger_service::MockLedgerService;
use snarkos_utilities::NodeDataDir;
use snarkvm::{
    prelude::{FromBytes, MainnetV0 as CurrentNetwork, PrivateKey},
    utilities::TestRng,
};
use std::net::{IpAddr, Ipv4Addr};

use crate::{
    Heartbeat,
    Inbound,
    Outbound,
    Router,
    Routing,
    messages::{
        BlockRequest,
        DisconnectReason,
        Message,
        MessageCodec,
        Ping,
        Pong,
        UnconfirmedSolution,
        UnconfirmedTransaction,
    },
};
use snarkos_node_network::{NodeType, Peer, PeerPoolHandling, Resolver};
use snarkos_node_tcp::{
    ConnectError,
    Connection,
    ConnectionSide,
    P2P,
    Tcp,
    connections::DisconnectOrigin,
    protocols::{Disconnect, Handshake, OnConnect, Reading, Writing},
};
use snarkvm::{
    console::network::{ConsensusVersion, Network},
    ledger::{
        block::{Block, Header, Transaction},
        puzzle::Solution,
    },
    prelude::Field,
};

use async_trait::async_trait;
#[cfg(feature = "locktick")]
use locktick::parking_lot::RwLock;
#[cfg(not(feature = "locktick"))]
use parking_lot::RwLock;
use std::{collections::HashMap, io, net::SocketAddr, str::FromStr, sync::Arc};
use tracing::*;

#[derive(Clone)]
pub struct TestRouter<N: Network>(Router<N>);

impl<N: Network> From<Router<N>> for TestRouter<N> {
    fn from(router: Router<N>) -> Self {
        Self(router)
    }
}

impl<N: Network> core::ops::Deref for TestRouter<N> {
    type Target = Router<N>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<N: Network> P2P for TestRouter<N> {
    /// Returns a reference to the TCP instance.
    fn tcp(&self) -> &Tcp {
        self.router().tcp()
    }
}

impl<N: Network> PeerPoolHandling<N> for TestRouter<N> {
    const MAXIMUM_POOL_SIZE: usize = 1_000;
    const OWNER: &str = "[TestRouter]";
    const PEER_SLASHING_COUNT: usize = 0;

    fn peer_pool(&self) -> &RwLock<HashMap<SocketAddr, Peer<N>>> {
        self.router().peer_pool()
    }

    fn resolver(&self) -> &RwLock<Resolver<N>> {
        self.router().resolver()
    }

    fn is_dev(&self) -> bool {
        true
    }

    fn trusted_peers_only(&self) -> bool {
        false
    }

    fn node_type(&self) -> NodeType {
        self.router().node_type()
    }
}

#[async_trait]
impl<N: Network> Handshake for TestRouter<N> {
    /// Performs the handshake protocol.
    async fn perform_handshake(&self, mut connection: Connection) -> Result<Connection, ConnectError> {
        // Perform the handshake.
        let peer_addr = connection.addr();
        let conn_side = connection.side();
        let stream = self.borrow_stream(&mut connection);
        let genesis_header = *sample_genesis_block().header();
        let restrictions_id =
            Field::<N>::from_str("7562506206353711030068167991213732850758501012603348777370400520506564970105field")
                .unwrap();
        self.router().handshake(peer_addr, stream, conn_side, genesis_header, restrictions_id).await?;

        Ok(connection)
    }
}

#[async_trait]
impl<N: Network> OnConnect for TestRouter<N> {
    async fn on_connect(&self, _peer_addr: SocketAddr) {}
}

#[async_trait]
impl<N: Network> Disconnect for TestRouter<N> {
    /// Any extra operations to be performed during a disconnect.
    async fn handle_disconnect(&self, peer_addr: SocketAddr, _origin: DisconnectOrigin) {
        if let Some(peer_ip) = self.router().resolve_to_listener(peer_addr) {
            self.router().downgrade_peer_to_candidate(peer_ip);
        }
    }
}

#[async_trait]
impl<N: Network> Writing for TestRouter<N> {
    type Codec = MessageCodec<N>;
    type Message = Message<N>;

    /// Creates an [`Encoder`] used to write the outbound messages to the target stream.
    /// The `side` parameter indicates the connection side **from the node's perspective**.
    fn codec(&self, _addr: SocketAddr, _side: ConnectionSide) -> Self::Codec {
        Default::default()
    }
}

#[async_trait]
impl<N: Network> Reading for TestRouter<N> {
    type Codec = MessageCodec<N>;
    type Message = Message<N>;

    /// Creates a [`Decoder`] used to interpret messages from the network.
    /// The `side` param indicates the connection side **from the node's perspective**.
    fn codec(&self, _peer_addr: SocketAddr, _side: ConnectionSide) -> Self::Codec {
        Default::default()
    }

    /// Processes a message received from the network.
    async fn process_message(&self, peer_ip: SocketAddr, message: Self::Message) -> io::Result<()> {
        // Process the message. Disconnect if the peer violated the protocol.
        if let Err(error) = self.inbound(peer_ip, message).await {
            warn!("Disconnecting from '{peer_ip}' - {error}");
            self.send(peer_ip, Message::Disconnect(DisconnectReason::ProtocolViolation.into()));
            // Disconnect from this peer.
            self.router().disconnect(peer_ip);
        }
        Ok(())
    }
}

#[async_trait]
impl<N: Network> Routing<N> for TestRouter<N> {}

impl<N: Network> Heartbeat<N> for TestRouter<N> {}

impl<N: Network> Outbound<N> for TestRouter<N> {
    /// Returns a reference to the router.
    fn router(&self) -> &Router<N> {
        &self.0
    }

    /// Returns `true` if the node is synced up to the latest block (within the given tolerance).
    fn is_block_synced(&self) -> bool {
        true
    }

    /// Returns the number of blocks this node is behind the greatest peer height.
    fn num_blocks_behind(&self) -> Option<u32> {
        None
    }

    /// Returns the current sync speed in blocks per second.
    fn get_sync_speed(&self) -> f64 {
        0.0
    }
}

#[async_trait]
impl<N: Network> Inbound<N> for TestRouter<N> {
    /// Returns `true` if the message version is valid.
    fn is_valid_message_version(&self, _message_version: u32) -> bool {
        true
    }

    /// Handles a `BlockRequest` message.
    fn block_request(&self, _peer_ip: SocketAddr, _message: BlockRequest) -> bool {
        true
    }

    /// Handles a `BlockResponse` message.
    fn block_response(
        &self,
        _peer_ip: SocketAddr,
        _blocks: Vec<Block<N>>,
        _latest_consensus_version: Option<ConsensusVersion>,
    ) -> bool {
        true
    }

    /// Handles an `Ping` message.
    fn ping(&self, _peer_ip: SocketAddr, _message: Ping<N>) -> bool {
        true
    }

    /// Handles an `Pong` message.
    fn pong(&self, _peer_ip: SocketAddr, _message: Pong) -> bool {
        true
    }

    /// Handles an `PuzzleRequest` message.
    fn puzzle_request(&self, _peer_ip: SocketAddr) -> bool {
        true
    }

    /// Handles an `PuzzleResponse` message.
    fn puzzle_response(&self, _peer_ip: SocketAddr, _epoch_hash: N::BlockHash, _header: Header<N>) -> bool {
        true
    }

    /// Handles an `UnconfirmedSolution` message.
    async fn unconfirmed_solution(
        &self,
        _peer_ip: SocketAddr,
        _serialized: UnconfirmedSolution<N>,
        _solution: Solution<N>,
    ) -> bool {
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

/// Returns a fixed account.
pub fn sample_account(rng: &mut TestRng) -> Account<CurrentNetwork> {
    let private_key = PrivateKey::<CurrentNetwork>::new(rng).unwrap();
    Account::<CurrentNetwork>::try_from(&private_key).unwrap()
}

/// Loads the current network's genesis block.
pub fn sample_genesis_block<N: Network>() -> Block<N> {
    Block::<N>::from_bytes_le(N::genesis_bytes()).unwrap()
}

/// Initializes a router of the given node type.
///
/// Setting `listening_port = 0` results in a random port being assigned. No listener is bound
/// until `initialize_routing` is called, so a router built here does not touch the network.
pub async fn sample_router(
    node_type: NodeType,
    listening_port: u16,
    max_peers: u16,
    trusted_peers: &[SocketAddr],
    trusted_peers_only: bool,
    rng: &mut TestRng,
) -> TestRouter<CurrentNetwork> {
    let committee = snarkvm::ledger::committee::test_helpers::sample_committee(rng);
    let ledger_service = Arc::new(MockLedgerService::new(committee));
    Router::new(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), listening_port),
        node_type,
        sample_account(rng),
        ledger_service,
        trusted_peers,
        max_peers,
        trusted_peers_only,
        NodeDataDir::new_test(None),
        true,
    )
    .await
    .expect("couldn't create router")
    .into()
}

/// Initializes a client router. Setting the `listening_port = 0` will result in a random port being assigned.
pub async fn client(listening_port: u16, max_peers: u16, rng: &mut TestRng) -> TestRouter<CurrentNetwork> {
    sample_router(NodeType::Client, listening_port, max_peers, &[], false, rng).await
}

/// Initializes a prover router. Setting the `listening_port = 0` will result in a random port being assigned.
pub async fn prover(listening_port: u16, max_peers: u16, rng: &mut TestRng) -> TestRouter<CurrentNetwork> {
    sample_router(NodeType::Prover, listening_port, max_peers, &[], false, rng).await
}

/// Initializes a validator router. Setting the `listening_port = 0` will result in a random port being assigned.
pub async fn validator(
    listening_port: u16,
    max_peers: u16,
    trusted_peers: &[SocketAddr],
    trusted_peers_only: bool,
    rng: &mut TestRng,
) -> TestRouter<CurrentNetwork> {
    sample_router(NodeType::Validator, listening_port, max_peers, trusted_peers, trusted_peers_only, rng).await
}
