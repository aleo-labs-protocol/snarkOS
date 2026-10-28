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
    BootstrapClient,
    Client,
    Prover,
    Validator,
    network::{NodeType, Peer, PeerPoolHandling},
    rest::RestVerificationLimits,
    router::Outbound,
    traits::NodeInterface,
};

use snarkos_account::Account;
use snarkos_utilities::{DevHotswapConfig, NodeDataDir, SignalHandler};

use snarkvm::prelude::{
    Address,
    Header,
    Ledger,
    Network,
    PrivateKey,
    ViewKey,
    block::Block,
    store::helpers::{memory::ConsensusMemory, rocksdb::ConsensusDB},
};

use aleo_std::StorageMode;
use anyhow::Result;

#[cfg(feature = "locktick")]
use locktick::parking_lot::RwLock;
#[cfg(not(feature = "locktick"))]
use parking_lot::RwLock;
#[cfg(feature = "metrics")]
use std::time::Duration;
use std::{collections::HashMap, net::SocketAddr, path::PathBuf, sync::Arc};
use tokio::task;

/// How often to publish RocksDB internal metrics to Prometheus.
#[cfg(feature = "metrics")]
const ROCKSDB_METRICS_INTERVAL: Duration = Duration::from_secs(15);

#[derive(Clone)]
pub enum Node<N: Network> {
    /// A validator is a full node, capable of validating blocks.
    Validator(Arc<Validator<N, ConsensusDB<N>>>),
    /// A prover is a light node, capable of producing proofs for consensus.
    Prover(Arc<Prover<N, ConsensusMemory<N>>>),
    /// A client node is a full node, capable of querying with the network.
    Client(Arc<Client<N, ConsensusDB<N>>>),
    /// A bootstrap client node is a light node dedicated to serving lists of peers.
    BootstrapClient(BootstrapClient<N>),
}

impl<N: Network> Node<N> {
    /// Initializes a new validator node.
    pub async fn new_validator(
        node_ip: SocketAddr,
        bft_ip: Option<SocketAddr>,
        rest_ip: Option<SocketAddr>,
        rest_rps: u32,
        rest_verification_limits: RestVerificationLimits,
        history_api_url: Option<String>,
        history_json: bool,
        account: Account<N>,
        trusted_peers: &[SocketAddr],
        trusted_validators: &[SocketAddr],
        genesis: Block<N>,
        cdn: Option<http::Uri>,
        storage_mode: StorageMode,
        node_data_dir: NodeDataDir,
        trusted_peers_only: bool,
        auto_db_checkpoints: Option<PathBuf>,
        dev_txs: bool,
        dev: Option<u16>,
        dev_hotswap_config: Option<DevHotswapConfig>,
        proposal_spent_limit_multiplier: u64,
        signal_handler: Arc<SignalHandler>,
    ) -> Result<Self> {
        let validator = Arc::new(
            Validator::new(
                node_ip,
                bft_ip,
                rest_ip,
                rest_rps,
                rest_verification_limits,
                history_api_url,
                history_json,
                account,
                trusted_peers,
                trusted_validators,
                genesis,
                cdn,
                storage_mode,
                node_data_dir,
                trusted_peers_only,
                dev_txs,
                dev,
                dev_hotswap_config,
                proposal_spent_limit_multiplier,
                signal_handler,
            )
            .await?,
        );

        let node = Self::Validator(validator.clone());

        // Perform automatic ledger checkpoints.
        if let Some(path) = auto_db_checkpoints
            && let Some(handle) = node.perform_auto_checkpoints(path)?
        {
            validator.handles.lock().push(handle);
        }

        #[cfg(feature = "metrics")]
        if let Some(handle) = node.spawn_rocksdb_metrics_polling() {
            validator.handles.lock().push(handle);
        }

        Ok(node)
    }

    /// Initializes a new prover node.
    pub async fn new_prover(
        node_ip: SocketAddr,
        account: Account<N>,
        trusted_peers: &[SocketAddr],
        genesis: Block<N>,
        node_data_dir: NodeDataDir,
        trusted_peers_only: bool,
        dev: Option<u16>,
        signal_handler: Arc<SignalHandler>,
    ) -> Result<Self> {
        Ok(Self::Prover(Arc::new(
            Prover::new(
                node_ip,
                account,
                trusted_peers,
                genesis,
                node_data_dir,
                trusted_peers_only,
                dev,
                signal_handler,
            )
            .await?,
        )))
    }

    /// Initializes a new client node.
    pub async fn new_client(
        node_ip: SocketAddr,
        rest_ip: Option<SocketAddr>,
        rest_rps: u32,
        rest_verification_limits: RestVerificationLimits,
        history_api_url: Option<String>,
        history_json: bool,
        account: Account<N>,
        trusted_peers: &[SocketAddr],
        genesis: Block<N>,
        cdn: Option<http::Uri>,
        storage_mode: StorageMode,
        node_data_dir: NodeDataDir,
        trusted_peers_only: bool,
        auto_db_checkpoints: Option<PathBuf>,
        dev: Option<u16>,
        signal_handler: Arc<SignalHandler>,
    ) -> Result<Self> {
        let client = Arc::new(
            Client::new(
                node_ip,
                rest_ip,
                rest_rps,
                rest_verification_limits,
                history_api_url,
                history_json,
                account,
                trusted_peers,
                genesis,
                cdn,
                storage_mode,
                node_data_dir,
                trusted_peers_only,
                dev,
                signal_handler,
            )
            .await?,
        );

        let node = Self::Client(client.clone());

        // Perform automatic ledger checkpoints.
        if let Some(path) = auto_db_checkpoints
            && let Some(handle) = node.perform_auto_checkpoints(path)?
        {
            client.handles.lock().push(handle);
        }

        #[cfg(feature = "metrics")]
        if let Some(handle) = node.spawn_rocksdb_metrics_polling() {
            client.handles.lock().push(handle);
        }

        Ok(node)
    }

    /// Initializes a new bootstrap client node.
    pub async fn new_bootstrap_client(
        listener_addr: SocketAddr,
        account: Account<N>,
        genesis_header: Header<N>,
        dev: Option<u16>,
    ) -> Result<Self> {
        Ok(Self::BootstrapClient(BootstrapClient::new(listener_addr, account, genesis_header, dev).await?))
    }

    /// Returns the node type.
    pub fn node_type(&self) -> NodeType {
        match self {
            Self::Validator(validator) => validator.node_type(),
            Self::Prover(prover) => prover.node_type(),
            Self::Client(client) => client.node_type(),
            Self::BootstrapClient(_) => NodeType::BootstrapClient,
        }
    }

    /// Returns the account private key of the node.
    pub fn private_key(&self) -> &PrivateKey<N> {
        match self {
            Self::Validator(node) => node.private_key(),
            Self::Prover(node) => node.private_key(),
            Self::Client(node) => node.private_key(),
            Self::BootstrapClient(node) => node.private_key(),
        }
    }

    /// Returns the account view key of the node.
    pub fn view_key(&self) -> &ViewKey<N> {
        match self {
            Self::Validator(node) => node.view_key(),
            Self::Prover(node) => node.view_key(),
            Self::Client(node) => node.view_key(),
            Self::BootstrapClient(node) => node.view_key(),
        }
    }

    /// Returns the account address of the node.
    pub fn address(&self) -> Address<N> {
        match self {
            Self::Validator(node) => node.address(),
            Self::Prover(node) => node.address(),
            Self::Client(node) => node.address(),
            Self::BootstrapClient(node) => node.address(),
        }
    }

    /// Returns `true` if the node is in development mode.
    pub fn is_dev(&self) -> bool {
        match self {
            Self::Validator(node) => node.is_dev(),
            Self::Prover(node) => node.is_dev(),
            Self::Client(node) => node.is_dev(),
            Self::BootstrapClient(node) => node.is_dev(),
        }
    }

    /// Returns a reference to the underlying peer pool.
    pub fn peer_pool(&self) -> &RwLock<HashMap<SocketAddr, Peer<N>>> {
        match self {
            Self::Validator(validator) => validator.router().peer_pool(),
            Self::Prover(prover) => prover.router().peer_pool(),
            Self::Client(client) => client.router().peer_pool(),
            Self::BootstrapClient(client) => client.peer_pool(),
        }
    }

    /// Get the underlying ledger (if any).
    pub fn ledger(&self) -> Option<&Ledger<N, ConsensusDB<N>>> {
        match self {
            Self::Validator(node) => Some(node.ledger()),
            Self::Prover(_) => None,
            Self::Client(node) => Some(node.ledger()),
            Self::BootstrapClient(_) => None,
        }
    }

    /// Returns `true` if the node is synced up to the latest block (within the given tolerance).
    pub fn is_block_synced(&self) -> bool {
        match self {
            Self::Validator(node) => node.is_block_synced(),
            Self::Prover(node) => node.is_block_synced(),
            Self::Client(node) => node.is_block_synced(),
            Self::BootstrapClient(_) => true,
        }
    }

    /// Returns the number of blocks this node is behind the greatest peer height,
    /// or `None` if not connected to peers yet.
    pub fn num_blocks_behind(&self) -> Option<u32> {
        match self {
            Self::Validator(node) => node.num_blocks_behind(),
            Self::Prover(node) => node.num_blocks_behind(),
            Self::Client(node) => node.num_blocks_behind(),
            Self::BootstrapClient(_) => Some(0),
        }
    }

    /// Calculates the current sync speed in blocks per second.
    /// Returns None if sync speed cannot be calculated (e.g., not syncing or insufficient data).
    pub fn get_sync_speed(&self) -> f64 {
        match self {
            Self::Validator(node) => node.get_sync_speed(),
            Self::Prover(node) => node.get_sync_speed(),
            Self::Client(node) => node.get_sync_speed(),
            Self::BootstrapClient(_) => 0.0,
        }
    }

    /// Shuts down the node.
    pub async fn shut_down(&self) {
        match self {
            Self::Validator(node) => node.shut_down().await,
            Self::Prover(node) => node.shut_down().await,
            Self::Client(node) => node.shut_down().await,
            Self::BootstrapClient(node) => node.shut_down().await,
        }
    }

    /// Waits until the node receives a signal.
    pub async fn wait_for_signals(&self, signal_handler: &SignalHandler) {
        match self {
            Self::Validator(node) => node.wait_for_signals(signal_handler).await,
            Self::Prover(node) => node.wait_for_signals(signal_handler).await,
            Self::Client(node) => node.wait_for_signals(signal_handler).await,
            Self::BootstrapClient(node) => node.wait_for_signals(signal_handler).await,
        }
    }

    /// Spawns a background task that periodically publishes RocksDB internal metrics.
    #[cfg(feature = "metrics")]
    pub fn spawn_rocksdb_metrics_polling(&self) -> Option<task::JoinHandle<()>> {
        let ledger = self.ledger()?.clone();
        Some(tokio::spawn(async move {
            let mut interval = tokio::time::interval(ROCKSDB_METRICS_INTERVAL);
            loop {
                interval.tick().await;
                ledger.vm().block_store().export_rocksdb_metrics();
            }
        }))
    }

    /// Periodically creates automated ledger checkpoints.
    pub fn perform_auto_checkpoints(&self, auto_checkpoint_path: PathBuf) -> Result<Option<task::JoinHandle<()>>> {
        self.ledger()
            .cloned()
            .map(|ledger| crate::checkpoints::spawn_auto_checkpoints(ledger, auto_checkpoint_path))
            .transpose()
    }
}
