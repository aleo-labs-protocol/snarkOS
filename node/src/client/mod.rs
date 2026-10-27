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

mod router;

use crate::{
    bft::{helpers::fmt_id, ledger_service::CoreLedgerService, spawn_blocking},
    cdn::CdnBlockSync,
    traits::NodeInterface,
};

use snarkos_account::Account;
use snarkos_node_network::{ConnectionMode, NodeType};
use snarkos_node_rest::{Rest, RestVerificationLimits};
use snarkos_node_router::{
    Heartbeat,
    Inbound,
    Outbound,
    Router,
    Routing,
    messages::{Message, UnconfirmedSolution, UnconfirmedTransaction},
};
use snarkos_node_sync::{BlockSync, Ping};
use snarkos_node_tcp::{
    P2P,
    protocols::{Disconnect, Handshake, OnConnect, Reading},
};
use snarkos_utilities::{NodeDataDir, SignalHandler, Stoppable};

use snarkvm::{
    console::network::Network,
    ledger::{
        Ledger,
        block::{Block, Header},
        puzzle::{Puzzle, Solution, SolutionID},
        store::ConsensusStorage,
    },
    prelude::{VM, block::Transaction},
};

use aleo_std::StorageMode;
use anyhow::{Context, Result};
use core::future::Future;
#[cfg(feature = "locktick")]
use locktick::parking_lot::Mutex;
use lru::LruCache;
#[cfg(not(feature = "locktick"))]
use parking_lot::Mutex;
use std::{
    net::SocketAddr,
    num::NonZeroUsize,
    sync::{
        Arc,
        atomic::{
            AtomicUsize,
            Ordering::{Acquire, Relaxed},
        },
    },
    time::Duration,
};
use tokio::{
    task::JoinHandle,
    time::{sleep, timeout},
};

/// The maximum number of solutions to verify in parallel.
/// Note: worst case memory to verify a solution is 0.5 GiB.
const MAX_PARALLEL_SOLUTION_VERIFICATIONS: usize = 20;
/// The capacity for storing unconfirmed deployments.
/// Note: This is an inbound queue capacity, not a Narwhal-enforced capacity.
const CAPACITY_FOR_DEPLOYMENTS: usize = 1 << 10;
/// The capacity for storing unconfirmed executions.
/// Note: This is an inbound queue capacity, not a Narwhal-enforced capacity.
const CAPACITY_FOR_EXECUTIONS: usize = 1 << 10;
/// The capacity for storing unconfirmed solutions.
/// Note: This is an inbound queue capacity, not a Narwhal-enforced capacity.
const CAPACITY_FOR_SOLUTIONS: usize = 1 << 10;

/// Transaction details needed for propagation.
/// We preserve the serialized transaction for faster propagation.
type TransactionContents<N> = (SocketAddr, UnconfirmedTransaction<N>, Transaction<N>);
/// Solution details needed for propagation.
/// We preserve the serialized solution for faster propagation.
type SolutionContents<N> = (SocketAddr, UnconfirmedSolution<N>, Solution<N>);

/// A client node is a full node, capable of querying with the network.
#[derive(Clone)]
pub struct Client<N: Network, C: ConsensusStorage<N>> {
    /// The ledger of the node.
    ledger: Ledger<N, C>,
    /// The router of the node.
    router: Router<N>,
    /// The REST server of the node.
    rest: Option<Rest<N, C, Self>>,
    /// The block synchronization logic.
    sync: Arc<BlockSync<N>>,
    /// The genesis block.
    genesis: Block<N>,
    /// The puzzle.
    puzzle: Puzzle<N>,
    /// The unconfirmed solutions queue.
    solution_queue: Arc<Mutex<LruCache<SolutionID<N>, SolutionContents<N>>>>,
    /// The unconfirmed deployments queue.
    deploy_queue: Arc<Mutex<LruCache<N::TransactionID, TransactionContents<N>>>>,
    /// The unconfirmed executions queue.
    execute_queue: Arc<Mutex<LruCache<N::TransactionID, TransactionContents<N>>>>,
    /// The amount of solutions currently being verified.
    num_verifying_solutions: Arc<AtomicUsize>,
    /// The amount of deployments currently being verified.
    num_verifying_deploys: Arc<AtomicUsize>,
    /// The amount of executions currently being verified.
    num_verifying_executions: Arc<AtomicUsize>,
    /// The spawned handles.
    pub(crate) handles: Arc<Mutex<Vec<JoinHandle<()>>>>,
    /// Keeps track of sending pings.
    ping: Arc<Ping<N>>,
    /// The signal handling logic.
    signal_handler: Arc<SignalHandler>,
}

impl<N: Network, C: ConsensusStorage<N>> Client<N, C> {
    /// Initializes a new client node.
    pub async fn new(
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
        dev: Option<u16>,
        signal_handler: Arc<SignalHandler>,
    ) -> Result<Self> {
        // Initialize the ledger.
        let ledger = {
            let storage_mode = storage_mode.clone();
            let genesis = genesis.clone();

            spawn_blocking!(Ledger::<N, C>::load(genesis, storage_mode))
        }
        .with_context(|| "Failed to initialize the ledger")?;

        // Later blocks finalized by this node are written as JSON beside the ledger.
        if history_json {
            ledger.vm().finalize_store().set_record_history_json(true);
        }

        // Initialize the ledger service.
        let ledger_service = Arc::new(CoreLedgerService::<N, C>::new(ledger.clone(), signal_handler.clone()));
        // Initialize the node router.
        let router = Router::new(
            node_ip,
            NodeType::Client,
            account,
            ledger_service.clone(),
            trusted_peers,
            Self::MAXIMUM_NUMBER_OF_PEERS as u16,
            trusted_peers_only,
            node_data_dir.clone(),
            dev.is_some(),
        )
        .await?;

        // Initialize the sync module.
        let sync = Arc::new(BlockSync::new(ledger_service.clone(), ConnectionMode::Router));

        // Set up the ping logic.
        let locators = sync.get_block_locators()?;
        let ping = Arc::new(Ping::new(router.clone(), locators));

        // Initialize the node.
        let mut node = Self {
            ledger: ledger.clone(),
            router,
            rest: None,
            sync: sync.clone(),
            genesis,
            ping,
            puzzle: ledger.puzzle().clone(),
            solution_queue: Arc::new(Mutex::new(LruCache::new(NonZeroUsize::new(CAPACITY_FOR_SOLUTIONS).unwrap()))),
            deploy_queue: Arc::new(Mutex::new(LruCache::new(NonZeroUsize::new(CAPACITY_FOR_DEPLOYMENTS).unwrap()))),
            execute_queue: Arc::new(Mutex::new(LruCache::new(NonZeroUsize::new(CAPACITY_FOR_EXECUTIONS).unwrap()))),
            num_verifying_solutions: Default::default(),
            num_verifying_deploys: Default::default(),
            num_verifying_executions: Default::default(),
            handles: Default::default(),
            signal_handler: signal_handler.clone(),
        };

        // Perform sync with CDN (if enabled).
        let cdn_sync = cdn.map(|base_url| {
            trace!("CDN sync is enabled");
            Arc::new(CdnBlockSync::new(base_url, ledger.clone(), signal_handler))
        });

        // Initialize the REST server.
        if let Some(rest_ip) = rest_ip {
            node.rest = Some(
                Rest::start(
                    rest_ip,
                    rest_rps,
                    history_api_url,
                    None,
                    ledger.clone(),
                    Arc::new(node.clone()),
                    cdn_sync.clone(),
                    sync,
                    rest_verification_limits,
                )
                .await?,
            );
        }

        // Set up everything else after CDN sync is done.
        if let Some(cdn_sync) = cdn_sync
            && let Err(error) = cdn_sync.wait().await.with_context(|| "Failed to synchronize from the CDN")
        {
            crate::log_clean_error(&storage_mode);
            node.shut_down().await;
            return Err(error);
        }

        // Initialize the routing.
        node.initialize_routing().await;
        // Initialize the sync module.
        node.initialize_sync();
        // Initialize solution verification.
        node.initialize_solution_verification();
        // Initialize deployment verification.
        node.initialize_deploy_verification();
        // Initialize execution verification.
        node.initialize_execute_verification();
        // Initialize the notification message loop.
        node.handles.lock().push(crate::start_notification_message_loop());
        // Return the node.
        Ok(node)
    }

    /// Returns the ledger.
    pub fn ledger(&self) -> &Ledger<N, C> {
        &self.ledger
    }

    /// Returns the REST server.
    pub fn rest(&self) -> &Option<Rest<N, C, Self>> {
        &self.rest
    }

    /// Returns the router.
    pub fn router(&self) -> &Router<N> {
        &self.router
    }
}

/// Sync-specific code.
impl<N: Network, C: ConsensusStorage<N>> Client<N, C> {
    /// The maximum time to wait for peer updates before timing out and attempting to issue new requests.
    /// This only exists as a fallback for the (unlikely) case a task does not get notified about updates.
    const MAX_SYNC_INTERVAL: Duration = Duration::from_secs(30);

    /// Spawns the tasks that performs the syncing logic for this client.
    fn initialize_sync(&self) {
        // Start the block request generation loop (outgoing).
        let self_ = self.clone();
        self.spawn(async move {
            while !self_.signal_handler.is_stopped() {
                // Wait for peer updates or timeout
                let _ = timeout(Self::MAX_SYNC_INTERVAL, self_.sync.wait_for_peer_update()).await;

                // Perform the sync routine.
                self_.try_issuing_block_requests().await;
            }

            info!("Stopped block request generation");
        });

        // Start the block response processing loop (incoming).
        let self_ = self.clone();
        self.spawn(async move {
            while !self_.signal_handler.is_stopped() {
                // Wait until there is something to do or until the timeout.
                let _ = timeout(Self::MAX_SYNC_INTERVAL, self_.sync.wait_for_block_responses()).await;

                // Perform the sync routine.
                self_.try_advancing_block_synchronization().await;

                // We perform no additional rate limiting here as
                // requests are already rate-limited.
            }

            debug!("Stopped block response processing");
        });
    }

    /// Client-side version of [`snarkvm_node_bft::Sync::try_advancing_block_synchronization`].
    async fn try_advancing_block_synchronization(&self) {
        let has_new_blocks = match self.sync.try_advancing_block_synchronization().await {
            Ok(val) => val,
            Err(err) => {
                error!("Block synchronization failed - {err}");
                return;
            }
        };

        // If there are new blocks, we need to update the block locators.
        if has_new_blocks {
            match self.sync.get_block_locators() {
                Ok(locators) => self.ping.update_block_locators(locators),
                Err(err) => error!("Failed to get block locators: {err}"),
            }
        }
    }

    /// Client-side version of `snarkvm_node_bft::Sync::try_block_sync()`.
    async fn try_issuing_block_requests(&self) {
        self.sync.try_issuing_block_requests(self.router()).await;
    }

    /// Initializes solution verification.
    fn initialize_solution_verification(&self) {
        // Start the solution verification loop.
        let node = self.clone();
        self.spawn(async move {
            loop {
                // If the Ctrl-C handler registered the signal, stop the node.
                if node.signal_handler.is_stopped() {
                    info!("Shutting down solution verification");
                    break;
                }

                // Determine if the queue contains txs to verify.
                let queue_is_empty = node.solution_queue.lock().is_empty();
                // Determine if our verification counter has space to verify new solutions.
                let counter_is_full = node.num_verifying_solutions.load(Acquire) >= MAX_PARALLEL_SOLUTION_VERIFICATIONS;

                // Sleep to allow the queue to be filled or solutions to be validated.
                if queue_is_empty || counter_is_full {
                    sleep(Duration::from_millis(50)).await;
                    continue;
                }

                // Try to verify solutions.
                let mut solution_queue = node.solution_queue.lock();
                while let Some((_, (peer_ip, serialized, solution))) = solution_queue.pop_lru() {
                    // Increment the verification counter.
                    let previous_counter = node.num_verifying_solutions.fetch_add(1, Relaxed);
                    let _node = node.clone();
                    // For each solution, spawn a task to verify it.
                    tokio::task::spawn_blocking(move || {
                        // Retrieve the latest epoch hash.
                        if let Ok(epoch_hash) = _node.ledger.latest_epoch_hash() {
                            // Check if the prover has reached their solution limit.
                            // While snarkVM will ultimately abort any excess solutions for safety, performing this check
                            // here prevents the to-be aborted solutions from propagating through the network.
                            let prover_address = solution.address();
                            if _node.ledger.is_solution_limit_reached(&prover_address, 0) {
                                debug!("Invalid Solution '{}' - Prover '{prover_address}' has reached their solution limit for the current epoch", fmt_id(solution.id()));
                            }
                            // Retrieve the latest proof target.
                            let proof_target = _node.ledger.latest_block().header().proof_target();
                            // Ensure that the solution is valid for the given epoch.
                            let is_valid = _node.puzzle.check_solution(&solution, epoch_hash, proof_target);
                            #[cfg(feature = "metrics")]
                            snarkos_node_metrics::record_solution_check("client_relay", is_valid.is_ok());

                            match is_valid {
                                // If the solution is valid, propagate the `UnconfirmedSolution`.
                                Ok(()) => {
                                    let message = Message::UnconfirmedSolution(serialized);
                                    // Propagate the "UnconfirmedSolution".
                                    _node.propagate(message, &[peer_ip]);
                                    #[cfg(feature = "metrics")]
                                    snarkos_node_metrics::increment_counter_1(
                                        snarkos_node_metrics::router::UNCONFIRMED_SOLUTIONS_PROPAGATED,
                                        "site",
                                        "client_verified",
                                    );
                                }
                                // If error occurs after the first 10 blocks of the epoch, log it as a warning, otherwise ignore.
                                Err(error) => {
                                    if _node.ledger.latest_height() % N::NUM_BLOCKS_PER_EPOCH > 10 {
                                        debug!("Failed to verify the solution from peer_ip {peer_ip} - {error}")
                                    }
                                }
                            }
                        } else {
                            warn!("Failed to retrieve the latest epoch hash.");
                        }
                        // Decrement the verification counter.
                        _node.num_verifying_solutions.fetch_sub(1, Relaxed);
                    });
                    // If we are already at capacity, don't verify more solutions.
                    if previous_counter + 1 >= MAX_PARALLEL_SOLUTION_VERIFICATIONS {
                        break;
                    }
                }
            }
        });
    }

    /// Initializes deploy verification.
    fn initialize_deploy_verification(&self) {
        // Start the deploy verification loop.
        let node = self.clone();
        self.spawn(async move {
            loop {
                // If the Ctrl-C handler registered the signal, stop the node.
                if node.signal_handler.is_stopped() {
                    info!("Shutting down deployment verification");
                    break;
                }

                // Determine if the queue contains txs to verify.
                let queue_is_empty = node.deploy_queue.lock().is_empty();
                // Determine if our verification counter has space to verify new txs.
                let counter_is_full =
                    node.num_verifying_deploys.load(Acquire) >= VM::<N, C>::MAX_PARALLEL_DEPLOY_VERIFICATIONS;

                // Sleep to allow the queue to be filled or transactions to be validated.
                if queue_is_empty || counter_is_full {
                    sleep(Duration::from_millis(50)).await;
                    continue;
                }

                // Try to verify deployments.
                while let Some((_, (peer_ip, serialized, transaction))) = node.deploy_queue.lock().pop_lru() {
                    // Increment the verification counter.
                    let previous_counter = node.num_verifying_deploys.fetch_add(1, Relaxed);
                    let _node = node.clone();
                    // For each deployment, spawn a task to verify it.
                    tokio::task::spawn_blocking(move || {
                        // First collect the state root.
                        let Some(state_root) = transaction.fee_transition().map(|t| t.global_state_root()) else {
                            debug!("Failed to access global state root for deployment from peer_ip {peer_ip}");
                            _node.num_verifying_deploys.fetch_sub(1, Relaxed);
                            return;
                        };
                        // Check if the state root is in the ledger.
                        if !_node.ledger().contains_state_root(&state_root).unwrap_or(false) {
                            debug!("Failed to find global state root for deployment from peer_ip {peer_ip}, propagating anyway");
                            // Propagate the `UnconfirmedTransaction`.
                            _node.propagate(Message::UnconfirmedTransaction(serialized), &[peer_ip]);
                            #[cfg(feature = "metrics")]
                            snarkos_node_metrics::increment_counter_1(
                                snarkos_node_metrics::router::UNCONFIRMED_TRANSACTIONS_PROPAGATED,
                                "site",
                                "client_unverified",
                            );
                            _node.num_verifying_deploys.fetch_sub(1, Relaxed);
                            return;
                            // Also skip the `check_transaction_basic` call if it is already propagated.
                        }
                        // Check the deployment.
                        #[cfg(feature = "metrics")]
                        let timer = snarkos_node_metrics::TransactionCheckTimer::start("client_relay", &transaction);
                        let result = _node.ledger.check_transaction_basic(&transaction, None, &mut rand::rng());
                        #[cfg(feature = "metrics")]
                        timer.finish(&result);
                        match result {
                            Ok(_) => {
                                // Propagate the `UnconfirmedTransaction`.
                                _node.propagate(Message::UnconfirmedTransaction(serialized), &[peer_ip]);
                                #[cfg(feature = "metrics")]
                                snarkos_node_metrics::increment_counter_1(
                                    snarkos_node_metrics::router::UNCONFIRMED_TRANSACTIONS_PROPAGATED,
                                    "site",
                                    "client_verified",
                                );
                            }
                            Err(error) => {
                                debug!("Failed to verify the deployment from peer_ip {peer_ip} - {error}");
                            }
                        }
                        // Decrement the verification counter.
                        _node.num_verifying_deploys.fetch_sub(1, Relaxed);
                    });
                    // If we are already at capacity, don't verify more deployments.
                    if previous_counter + 1 >= VM::<N, C>::MAX_PARALLEL_DEPLOY_VERIFICATIONS {
                        break;
                    }
                }
            }
        });
    }

    /// Initializes execute verification.
    fn initialize_execute_verification(&self) {
        // Start the execute verification loop.
        let node = self.clone();
        self.spawn(async move {
            loop {
                // If the Ctrl-C handler registered the signal, stop the node.
                if node.signal_handler.is_stopped() {
                    info!("Shutting down execution verification");
                    break;
                }

                // Determine if the queue contains txs to verify.
                let queue_is_empty = node.execute_queue.lock().is_empty();
                // Determine if our verification counter has space to verify new txs.
                let counter_is_full =
                    node.num_verifying_executions.load(Acquire) >= VM::<N, C>::MAX_PARALLEL_EXECUTE_VERIFICATIONS;

                // Sleep to allow the queue to be filled or transactions to be validated.
                if queue_is_empty || counter_is_full {
                    sleep(Duration::from_millis(50)).await;
                    continue;
                }

                // Try to verify executions.
                while let Some((_, (peer_ip, serialized, transaction))) = node.execute_queue.lock().pop_lru() {
                    // Increment the verification counter.
                    let previous_counter = node.num_verifying_executions.fetch_add(1, Relaxed);
                    let _node = node.clone();
                    // For each execution, spawn a task to verify it.
                    tokio::task::spawn_blocking(move || {
                        // First collect the state roots.
                        let state_roots = [
                            transaction.execution().map(|t| t.global_state_root()),
                            transaction.fee_transition().map(|t| t.global_state_root()),
                        ]
                        .into_iter()
                        .flatten();

                        for state_root in state_roots {
                            if !_node.ledger().contains_state_root(&state_root).unwrap_or(false) {
                                debug!("Failed to find global state root for execution from peer_ip {peer_ip}, propagating anyway");
                                // Propagate the `UnconfirmedTransaction`.
                                _node.propagate(Message::UnconfirmedTransaction(serialized), &[peer_ip]);
                                #[cfg(feature = "metrics")]
                                snarkos_node_metrics::increment_counter_1(
                                    snarkos_node_metrics::router::UNCONFIRMED_TRANSACTIONS_PROPAGATED,
                                    "site",
                                    "client_unverified",
                                );
                                _node.num_verifying_executions.fetch_sub(1, Relaxed);
                                return;
                                // Also skip the `check_transaction_basic` call if it is already propagated.
                            }
                        }
                        // Check the execution.
                        #[cfg(feature = "metrics")]
                        let timer = snarkos_node_metrics::TransactionCheckTimer::start("client_relay", &transaction);
                        let result = _node.ledger.check_transaction_basic(&transaction, None, &mut rand::rng());
                        #[cfg(feature = "metrics")]
                        timer.finish(&result);
                        match result {
                            Ok(_) => {
                                // Propagate the `UnconfirmedTransaction`.
                                _node.propagate(Message::UnconfirmedTransaction(serialized), &[peer_ip]);
                                #[cfg(feature = "metrics")]
                                snarkos_node_metrics::increment_counter_1(
                                    snarkos_node_metrics::router::UNCONFIRMED_TRANSACTIONS_PROPAGATED,
                                    "site",
                                    "client_verified",
                                );
                            }
                            Err(error) => {
                                debug!("Failed to verify the execution from peer_ip {peer_ip} - {error}");
                            }
                        }
                        // Decrement the verification counter.
                        _node.num_verifying_executions.fetch_sub(1, Relaxed);
                    });
                    // If we are already at capacity, don't verify more executions.
                    if previous_counter + 1 >= VM::<N, C>::MAX_PARALLEL_EXECUTE_VERIFICATIONS {
                        break;
                    }
                }
            }
        });
    }

    /// Spawns a task with the given future; it should only be used for long-running tasks.
    pub fn spawn<T: Future<Output = ()> + Send + 'static>(&self, future: T) {
        self.handles.lock().push(tokio::spawn(future));
    }
}

#[async_trait]
impl<N: Network, C: ConsensusStorage<N>> NodeInterface<N> for Client<N, C> {
    /// Shuts down the node.
    async fn shut_down(&self) {
        info!("Shutting down...");

        // Shut down the node.
        trace!("Shutting down the node...");

        // Shut down the REST instance.
        if let Some(rest) = &self.rest {
            trace!("Shutting down the REST server...");
            rest.shut_down();
        }

        // Abort the tasks.
        trace!("Shutting down the client...");
        self.handles.lock().iter().for_each(|handle| handle.abort());

        // Shut down the router.
        self.router.shut_down().await;

        info!("Node has shut down.");
    }
}
