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

#[macro_use]
extern crate async_trait;
#[macro_use]
extern crate tracing;

#[cfg(feature = "metrics")]
extern crate snarkos_node_metrics as metrics;

pub use snarkos_node_router_messages as messages;
use snarkos_utilities::NodeDataDir;

mod handshake;

mod heartbeat;
pub use heartbeat::*;

mod helpers;
pub use helpers::*;

mod inbound;
pub use inbound::*;

mod outbound;
pub use outbound::*;

mod routing;
pub use routing::*;

#[cfg(feature = "test-helpers")]
pub mod test_helpers;

mod writing;

use crate::messages::{BlockRequest, Message, MessageCodec};

use snarkos_account::Account;
use snarkos_node_bft_ledger_service::LedgerService;
use snarkos_node_network::{
    CandidatePeer,
    ConnectedPeer,
    ConnectionMode,
    NodeType,
    Peer,
    PeerPoolHandling,
    Resolver,
    bootstrap_peers,
};
use snarkos_node_sync_communication_service::CommunicationService;
use snarkos_node_tcp::{Config, ConnectionSide, Tcp};

use snarkvm::prelude::{Address, Network, PrivateKey, ViewKey};

use anyhow::Result;
#[cfg(feature = "locktick")]
use locktick::parking_lot::{Mutex, RwLock};
#[cfg(not(feature = "locktick"))]
use parking_lot::{Mutex, RwLock};
use std::{collections::HashMap, future::Future, io, net::SocketAddr, ops::Deref, sync::Arc};
use tokio::task::JoinHandle;

/// The default port used by the router.
pub const DEFAULT_NODE_PORT: u16 = 4130;

/// The router keeps track of connected and connecting peers.
/// The actual network communication happens in Inbound/Outbound,
/// which is implemented by Validator, Prover, and Client.
#[derive(Clone)]
pub struct Router<N: Network>(Arc<InnerRouter<N>>);

impl<N: Network> Deref for Router<N> {
    type Target = Arc<InnerRouter<N>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<N: Network> PeerPoolHandling<N> for Router<N> {
    const MAXIMUM_POOL_SIZE: usize = 10_000;
    const OWNER: &str = "[Router]";
    const PEER_SLASHING_COUNT: usize = 200;

    fn peer_pool(&self) -> &RwLock<HashMap<SocketAddr, Peer<N>>> {
        &self.peer_pool
    }

    fn resolver(&self) -> &RwLock<Resolver<N>> {
        &self.resolver
    }

    fn is_dev(&self) -> bool {
        self.is_dev
    }

    fn trusted_peers_only(&self) -> bool {
        self.trusted_peers_only
    }

    fn node_type(&self) -> NodeType {
        self.node_type
    }
}

pub struct InnerRouter<N: Network> {
    /// The TCP stack.
    tcp: Tcp,
    /// The node type.
    node_type: NodeType,
    /// The account of the node.
    account: Account<N>,
    /// The ledger service.
    ledger: Arc<dyn LedgerService<N>>,
    /// The cache.
    cache: Cache<N>,
    /// The resolver.
    resolver: RwLock<Resolver<N>>,
    /// The collection of both candidate and connected peers.
    peer_pool: RwLock<HashMap<SocketAddr, Peer<N>>>,
    /// The spawned handles.
    handles: Mutex<Vec<JoinHandle<()>>>,
    /// If the flag is set, the node will only connect to trusted peers.
    trusted_peers_only: bool,
    /// The storage mode.
    node_data_dir: NodeDataDir,
    /// The boolean flag for the development mode.
    is_dev: bool,
}

impl<N: Network> Router<N> {
    /// The minimum permitted interval between connection attempts for an IP; anything shorter is considered malicious.
    const CONNECTION_ATTEMPTS_SINCE_SECS: i64 = 10;
    /// The maximum amount of connection attempts within a 10 second threshold.
    #[cfg(not(feature = "test"))]
    const MAX_CONNECTION_ATTEMPTS: usize = 10;
    /// The time frame to enforce the `MESSAGE_LIMIT`.
    const MESSAGE_LIMIT_TIME_FRAME_IN_SECS: i64 = 5;
}

impl<N: Network> Router<N> {
    /// Initializes a new `Router` instance.
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        node_ip: SocketAddr,
        node_type: NodeType,
        account: Account<N>,
        ledger: Arc<dyn LedgerService<N>>,
        trusted_peers: &[SocketAddr],
        max_peers: u16,
        trusted_peers_only: bool,
        node_data_dir: NodeDataDir,
        is_dev: bool,
    ) -> Result<Self> {
        // Initialize the TCP stack.
        let tcp = Tcp::new(Config::new(node_ip, max_peers));

        // Prepare the collection of the initial peers.
        let mut initial_peers = HashMap::new();

        // Load entries from the peer cache (if present and if we are not in trusted peers only mode).
        if !trusted_peers_only {
            let cached_peers = Self::load_cached_peers(&node_data_dir.router_peer_cache_path())?;
            for addr in cached_peers {
                initial_peers.insert(addr, Peer::new_candidate(addr, false));
            }
        }

        // Add the trusted peers to the list of the initial peers; this may promote
        // some of the cached peers to trusted ones.
        initial_peers.extend(trusted_peers.iter().copied().map(|addr| (addr, Peer::new_candidate(addr, true))));

        // Initialize the router.
        Ok(Self(Arc::new(InnerRouter {
            tcp,
            node_type,
            account,
            ledger,
            cache: Default::default(),
            resolver: Default::default(),
            peer_pool: RwLock::new(initial_peers),
            handles: Default::default(),
            trusted_peers_only,
            node_data_dir,
            is_dev,
        })))
    }
}

impl<N: Network> Router<N> {
    /// Returns `true` if the message version is valid.
    pub fn is_valid_message_version(&self, message_version: u32) -> bool {
        // Determine the minimum message version this node will accept, based on its role.
        // - Provers always operate at the latest message version.
        // - Validators and clients may accept older versions, depending on their current block height.
        let lowest_accepted_message_version = match self.node_type {
            // Provers should always use the latest version. The bootstrap clients are forced to
            // be strict, as they don't follow the current chain height.
            NodeType::Prover | NodeType::BootstrapClient => Message::<N>::latest_message_version(),
            // Validators and clients accept messages from lower version based on the migration height.
            NodeType::Validator | NodeType::Client => {
                Message::<N>::lowest_accepted_message_version(self.ledger.latest_block_height())
            }
        };

        // Check if the incoming message version is valid.
        message_version >= lowest_accepted_message_version
    }

    /// Returns the account private key of the node.
    pub fn private_key(&self) -> &PrivateKey<N> {
        self.account.private_key()
    }

    /// Returns the account view key of the node.
    pub fn view_key(&self) -> &ViewKey<N> {
        self.account.view_key()
    }

    /// Returns the account address of the node.
    pub fn address(&self) -> Address<N> {
        self.account.address()
    }

    /// Returns a reference to the cache.
    pub fn cache(&self) -> &Cache<N> {
        &self.cache
    }

    /// Returns a reference to the ledger.
    pub fn ledger(&self) -> &Arc<dyn LedgerService<N>> {
        &self.ledger
    }

    /// Returns `true` if the node is only engaging with trusted peers.
    pub fn trusted_peers_only(&self) -> bool {
        self.trusted_peers_only
    }

    /// Returns the listener IP address from the (ambiguous) peer address.
    pub fn resolve_to_listener(&self, connected_addr: SocketAddr) -> Option<SocketAddr> {
        self.resolver.read().get_listener(connected_addr)
    }

    /// Returns the list of metrics for the connected peers.
    pub fn connected_metrics(&self) -> Vec<(SocketAddr, NodeType)> {
        self.get_connected_peers().iter().map(|peer| (peer.listener_addr, peer.node_type)).collect()
    }

    #[cfg(feature = "metrics")]
    pub fn update_metrics(&self) {
        metrics::gauge(metrics::router::CONNECTED, self.number_of_connected_peers() as f64);
        metrics::gauge(metrics::router::CANDIDATE, self.number_of_candidate_peers() as f64);
    }

    pub fn update_last_seen_for_connected_peer(&self, peer_ip: SocketAddr) {
        if let Some(peer) = self.peer_pool.write().get_mut(&peer_ip) {
            peer.update_last_seen();
        }
    }

    /// Spawns a task with the given future; it should only be used for long-running tasks.
    pub fn spawn<T: Future<Output = ()> + Send + 'static>(&self, future: T) {
        self.handles.lock().push(tokio::spawn(future));
    }

    /// Shuts down the router.
    pub async fn shut_down(&self) {
        info!("Shutting down the router...");
        // Save the best peers for future use.
        if let Err(e) =
            self.save_best_peers(&self.node_data_dir.router_peer_cache_path(), Some(MAX_PEERS_TO_SEND), true)
        {
            warn!("Failed to persist best peers to disk: {e}");
        }
        // Abort the tasks.
        self.handles.lock().iter().for_each(|handle| handle.abort());
        // Close the listener.
        self.tcp.shut_down().await;
    }
}

#[async_trait]
impl<N: Network> CommunicationService for Router<N> {
    /// The message type.
    type Message = Message<N>;

    /// Prepares a block request to be sent.
    fn prepare_block_request(start_height: u32, end_height: u32) -> Self::Message {
        debug_assert!(start_height < end_height, "Invalid block request format");
        Message::BlockRequest(BlockRequest { start_height, end_height })
    }

    /// Sends the given message to specified peer.
    ///
    /// This function returns as soon as the message is queued to be sent,
    /// without waiting for the actual delivery; instead, the caller is provided with a [`oneshot::Receiver`]
    /// which can be used to determine when and whether the message has been delivered.
    async fn send(
        &self,
        peer_ip: SocketAddr,
        message: Self::Message,
    ) -> Option<tokio::sync::oneshot::Receiver<io::Result<()>>> {
        self.send(peer_ip, message)
    }
}
