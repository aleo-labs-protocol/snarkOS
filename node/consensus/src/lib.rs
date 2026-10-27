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

mod transactions_queue;
use transactions_queue::TransactionsQueue;

#[macro_use]
extern crate tracing;

#[cfg(feature = "metrics")]
extern crate snarkos_node_metrics as metrics;

use snarkos_account::Account;
use snarkos_node_bft::{
    BFT,
    MAX_BATCH_DELAY,
    Primary,
    helpers::{
        ConsensusReceiver,
        ConsensusSender,
        PrimaryReceiver,
        PrimarySender,
        Storage as NarwhalStorage,
        fmt_id,
        init_consensus_channels,
        init_primary_channels,
    },
    spawn_blocking,
};
use snarkos_node_bft_ledger_service::LedgerService;
use snarkos_node_bft_storage_service::BFTPersistentStorage;
use snarkos_node_sync::{BlockSync, Ping};
use snarkos_utilities::NodeDataDir;

use snarkvm::{
    ledger::{
        CheckBlockError,
        block::Transaction,
        narwhal::{BatchHeader, Data, Subdag, Transmission, TransmissionID},
        puzzle::{Solution, SolutionID},
    },
    prelude::*,
    utilities::flatten_error,
};

use aleo_std::StorageMode;
use anyhow::{Context, Result, bail};
use cfg_if::cfg_if;
use colored::Colorize;
use indexmap::IndexMap;
#[cfg(feature = "locktick")]
use locktick::parking_lot::{Mutex, RwLock};
use lru::LruCache;
#[cfg(not(feature = "locktick"))]
use parking_lot::{Mutex, RwLock};
use std::{future::Future, net::SocketAddr, num::NonZeroUsize, sync::Arc};
use tokio::{
    sync::{Notify, oneshot},
    task::JoinHandle,
};

#[cfg(feature = "metrics")]
use std::collections::HashMap;

/// The capacity of the queue reserved for deployments.
/// Note: This is an inbound queue capacity, not a Narwhal-enforced capacity.
const CAPACITY_FOR_DEPLOYMENTS: usize = 1 << 10;
/// The capacity of the queue reserved for executions.
/// Note: This is an inbound queue capacity, not a Narwhal-enforced capacity.
const CAPACITY_FOR_EXECUTIONS: usize = 1 << 10;
/// The capacity of the queue reserved for solutions.
/// Note: This is an inbound queue capacity, not a Narwhal-enforced capacity.
const CAPACITY_FOR_SOLUTIONS: usize = 1 << 10;
/// The **suggested** maximum number of deployments in each interval.
/// Note: This is an inbound queue limit, not a Narwhal-enforced limit.
const MAX_DEPLOYMENTS_PER_INTERVAL: usize = 1;

type BftRunArgs<N> = Arc<Mutex<Option<(ConsensusSender<N>, PrimaryReceiver<N>)>>>;

/// Wrapper around `BFT` that adds additional functionality, such as a mempool.
///
/// Consensus acts as a rate limiter to prevents workers in BFT from being overloaded.
/// Each worker maintains a ready queue (which is essentially also a mempool), but verifies transactions/solutions
/// before enqueuing them.
/// Consensus only passes more transactions/solutions to the BFT layer if its ready queues are not already full.
#[derive(Clone)]
pub struct Consensus<N: Network> {
    /// The ledger.
    ledger: Arc<dyn LedgerService<N>>,
    /// The BFT.
    bft: BFT<N>,
    /// The primary sender.
    primary_sender: PrimarySender<N>,
    /// The unconfirmed solutions queue.
    solutions_queue: Arc<Mutex<LruCache<SolutionID<N>, Solution<N>>>>,
    /// The unconfirmed transactions queue.
    transactions_queue: Arc<RwLock<TransactionsQueue<N>>>,
    /// The recently-seen unconfirmed solutions.
    seen_solutions: Arc<Mutex<LruCache<SolutionID<N>, ()>>>,
    /// The recently-seen unconfirmed transactions.
    seen_transactions: Arc<Mutex<LruCache<N::TransactionID, ()>>>,
    #[cfg(feature = "metrics")]
    transmissions_tracker: Arc<Mutex<HashMap<TransmissionID<N>, i64>>>,
    /// The spawned handles.
    handles: Arc<Mutex<Vec<JoinHandle<()>>>>,
    /// The ping logic.
    ping: Arc<Ping<N>>,
    /// The block sync logic.
    block_sync: Arc<BlockSync<N>>,
    /// Notifies when a block is committed, and relays it to the primary.
    block_commit_notify: Arc<Notify>,
    /// Holds the consensus receiver until [`Self::start_consensus_handlers`] is called.
    consensus_receiver: Arc<Mutex<Option<ConsensusReceiver<N>>>>,
    /// Holds the arguments for `BFT::run` until [`Self::start_consensus_handlers`] is called.
    bft_run_args: BftRunArgs<N>,
}

impl<N: Network> Consensus<N> {
    /// Initializes a new instance of consensus.
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        account: Account<N>,
        ledger: Arc<dyn LedgerService<N>>,
        block_sync: Arc<BlockSync<N>>,
        ip: Option<SocketAddr>,
        trusted_validators: &[SocketAddr],
        trusted_peers_only: bool,
        storage_mode: StorageMode,
        node_data_dir: NodeDataDir,
        ping: Arc<Ping<N>>,
        dev: Option<u16>,
        proposal_spent_limit_multiplier: u64,
    ) -> Result<Self> {
        // Initialize the primary channels.
        let (primary_sender, primary_receiver) = init_primary_channels::<N>();
        // Initialize the Narwhal transmissions.
        let transmissions = Arc::new(BFTPersistentStorage::open(storage_mode.clone())?);
        // Initialize the Narwhal storage.
        let storage = NarwhalStorage::new(ledger.clone(), transmissions, BatchHeader::<N>::MAX_GC_ROUNDS as u64)
            .with_context(|| "Failed to initialize the BFT storage")?;
        // Initialize the BFT.
        let bft = BFT::new(
            account,
            storage,
            ledger.clone(),
            block_sync.clone(),
            ip,
            trusted_validators,
            trusted_peers_only,
            node_data_dir,
            dev,
            proposal_spent_limit_multiplier,
        )?;
        // Initialize the consensus channels.
        let (consensus_sender, consensus_receiver) = init_consensus_channels();

        // Create a new instance of Consensus.
        let _self = Self {
            ledger,
            bft,
            block_sync,
            primary_sender,
            solutions_queue: Arc::new(Mutex::new(LruCache::new(NonZeroUsize::new(CAPACITY_FOR_SOLUTIONS).unwrap()))),
            transactions_queue: Default::default(),
            seen_solutions: Arc::new(Mutex::new(LruCache::new(NonZeroUsize::new(1 << 16).unwrap()))),
            seen_transactions: Arc::new(Mutex::new(LruCache::new(NonZeroUsize::new(1 << 16).unwrap()))),
            #[cfg(feature = "metrics")]
            transmissions_tracker: Default::default(),
            handles: Default::default(),
            ping: ping.clone(),
            block_commit_notify: Arc::new(Notify::new()),
            consensus_receiver: Arc::new(Mutex::new(Some(consensus_receiver))),
            bft_run_args: Arc::new(Mutex::new(Some((consensus_sender, primary_receiver)))),
        };

        Ok(_self)
    }

    /// Returns the underlying `BFT` struct.
    pub const fn bft(&self) -> &BFT<N> {
        &self.bft
    }

    pub fn contains_transaction(&self, transaction_id: &N::TransactionID) -> bool {
        self.transactions_queue.read().contains(transaction_id)
    }
}

impl<N: Network> Consensus<N> {
    /// Returns the number of unconfirmed transmissions in the BFT's workers (not in the mempool).
    pub fn num_unconfirmed_transmissions(&self) -> usize {
        self.bft.num_unconfirmed_transmissions()
    }

    /// Returns the number of unconfirmed ratifications in the BFT's workers (not in the mempool).
    pub fn num_unconfirmed_ratifications(&self) -> usize {
        self.bft.num_unconfirmed_ratifications()
    }

    /// Returns the number unconfirmed solutions in the BFT's workers (not in the mempool).
    pub fn num_unconfirmed_solutions(&self) -> usize {
        self.bft.num_unconfirmed_solutions()
    }

    /// Returns the number of unconfirmed transactions.
    pub fn num_unconfirmed_transactions(&self) -> usize {
        self.bft.num_unconfirmed_transactions()
    }
}

impl<N: Network> Consensus<N> {
    /// Returns the unconfirmed transmission IDs.
    pub fn unconfirmed_transmission_ids(&self) -> impl '_ + Iterator<Item = TransmissionID<N>> {
        self.worker_transmission_ids().chain(self.inbound_transmission_ids())
    }

    /// Returns the unconfirmed transmissions.
    pub fn unconfirmed_transmissions(&self) -> impl '_ + Iterator<Item = (TransmissionID<N>, Transmission<N>)> {
        self.worker_transmissions().chain(self.inbound_transmissions())
    }

    /// Returns the unconfirmed solutions.
    pub fn unconfirmed_solutions(&self) -> impl '_ + Iterator<Item = (SolutionID<N>, Data<Solution<N>>)> {
        self.worker_solutions().chain(self.inbound_solutions())
    }

    /// Returns the unconfirmed transactions.
    pub fn unconfirmed_transactions(&self) -> impl '_ + Iterator<Item = (N::TransactionID, Data<Transaction<N>>)> {
        self.worker_transactions().chain(self.inbound_transactions())
    }
}

impl<N: Network> Consensus<N> {
    /// Returns the worker transmission IDs.
    pub fn worker_transmission_ids(&self) -> impl '_ + Iterator<Item = TransmissionID<N>> {
        self.bft.worker_transmission_ids()
    }

    /// Returns the worker transmissions.
    pub fn worker_transmissions(&self) -> impl '_ + Iterator<Item = (TransmissionID<N>, Transmission<N>)> {
        self.bft.worker_transmissions()
    }

    /// Returns the worker solutions.
    pub fn worker_solutions(&self) -> impl '_ + Iterator<Item = (SolutionID<N>, Data<Solution<N>>)> {
        self.bft.worker_solutions()
    }

    /// Returns the worker transactions.
    pub fn worker_transactions(&self) -> impl '_ + Iterator<Item = (N::TransactionID, Data<Transaction<N>>)> {
        self.bft.worker_transactions()
    }
}

impl<N: Network> Consensus<N> {
    /// Returns the transmission IDs in the inbound queue.
    pub fn inbound_transmission_ids(&self) -> impl '_ + Iterator<Item = TransmissionID<N>> {
        self.inbound_transmissions().map(|(id, _)| id)
    }

    /// Returns the transmissions in the inbound queue.
    pub fn inbound_transmissions(&self) -> impl '_ + Iterator<Item = (TransmissionID<N>, Transmission<N>)> {
        self.inbound_transactions()
            .map(|(id, tx)| {
                (
                    TransmissionID::Transaction(id, tx.to_checksum::<N>().unwrap_or_default()),
                    Transmission::Transaction(tx),
                )
            })
            .chain(self.inbound_solutions().map(|(id, solution)| {
                (
                    TransmissionID::Solution(id, solution.to_checksum::<N>().unwrap_or_default()),
                    Transmission::Solution(solution),
                )
            }))
    }

    /// Returns the solutions in the inbound queue.
    pub fn inbound_solutions(&self) -> impl '_ + Iterator<Item = (SolutionID<N>, Data<Solution<N>>)> {
        // Return an iterator over the solutions in the inbound queue.
        self.solutions_queue.lock().clone().into_iter().map(|(id, solution)| (id, Data::Object(solution)))
    }

    /// Returns the transactions in the inbound queue.
    pub fn inbound_transactions(&self) -> impl '_ + Iterator<Item = (N::TransactionID, Data<Transaction<N>>)> {
        // Return an iterator over the deployment and execution transactions in the inbound queue.
        self.transactions_queue.read().transactions().map(|(id, tx)| (id, Data::Object(tx)))
    }
}

impl<N: Network> Consensus<N> {
    /// Adds the given unconfirmed solution to the memory pool, which will then eventually be passed
    /// to the BFT layer for inclusion in a batch.
    pub async fn add_unconfirmed_solution(&self, solution: Solution<N>) -> Result<()> {
        // Calculate the transmission checksum.
        let checksum = Data::<Solution<N>>::Buffer(solution.to_bytes_le()?.into()).to_checksum::<N>()?;
        // Queue the unconfirmed solution.
        {
            let solution_id = solution.id();

            // Check if the transaction was recently seen.
            if self.seen_solutions.lock().put(solution_id, ()).is_some() {
                // If the transaction was recently seen, return early.
                return Ok(());
            }
            // Check if the solution already exists in the ledger.
            if self.ledger.contains_transmission(&TransmissionID::Solution(solution_id, checksum))? {
                bail!("Solution '{}' exists in the ledger {}", fmt_id(solution_id), "(skipping)".dimmed());
            }
            // Add the solution to the memory pool.
            if self.solutions_queue.lock().put(solution_id, solution).is_some() {
                bail!("Solution '{}' exists in the memory pool", fmt_id(solution_id));
            }
            #[cfg(feature = "metrics")]
            {
                metrics::increment_gauge(metrics::consensus::UNCONFIRMED_SOLUTIONS, 1f64);
                let timestamp = snarkos_node_bft::helpers::now();
                self.transmissions_tracker.lock().insert(TransmissionID::Solution(solution_id, checksum), timestamp);
            }
            trace!("Received unconfirmed solution '{}' in the queue", fmt_id(solution_id));
        }

        // Try to process the unconfirmed solutions in the memory pool.
        self.process_unconfirmed_solutions().await
    }

    /// Processes unconfirmed solutions in the mempool, and passes them to the BFT layer
    /// (if sufficient space is available).
    async fn process_unconfirmed_solutions(&self) -> Result<()> {
        // If the memory pool of this node is full, return early.
        let num_unconfirmed_solutions = self.num_unconfirmed_solutions();
        let num_unconfirmed_transmissions = self.num_unconfirmed_transmissions();
        if num_unconfirmed_solutions >= N::MAX_SOLUTIONS
            || num_unconfirmed_transmissions >= Primary::<N>::MAX_TRANSMISSIONS_TOLERANCE
        {
            return Ok(());
        }
        // Retrieve the solutions.
        let solutions = {
            // Determine the available capacity.
            let capacity = N::MAX_SOLUTIONS.saturating_sub(num_unconfirmed_solutions);
            // Acquire the lock on the queue.
            let mut queue = self.solutions_queue.lock();
            // Determine the number of solutions to send.
            let num_solutions = queue.len().min(capacity);
            // Drain the solutions from the queue.
            (0..num_solutions).filter_map(|_| queue.pop_lru().map(|(_, solution)| solution)).collect::<Vec<_>>()
        };
        // Iterate over the solutions.
        for solution in solutions.into_iter() {
            let solution_id = solution.id();
            trace!("Adding unconfirmed solution '{}' to the memory pool...", fmt_id(solution_id));
            // Send the unconfirmed solution to the primary.
            match self.primary_sender.send_unconfirmed_solution(solution_id, Data::Object(solution)).await {
                Ok(true) => {}
                Ok(false) => debug!(
                    "Unable to add unconfirmed solution '{}' to the memory pool. Already exists.",
                    fmt_id(solution_id)
                ),
                Err(err) => {
                    let err = err.context(format!(
                        "Unable to add unconfirmed solution '{}' to the memory pool",
                        fmt_id(solution_id)
                    ));

                    // If the node is synced and the occurs after the first 10 blocks of an epoch, log it as a warning, otherwise trace it.
                    if self.bft.is_synced() && self.ledger.latest_block_height() % N::NUM_BLOCKS_PER_EPOCH > 10 {
                        warn!("{}", flatten_error(err));
                    } else {
                        trace!("{}", flatten_error(err));
                    }
                }
            }
        }
        Ok(())
    }

    /// Adds the given unconfirmed transaction to the memory pool, which will then eventually be passed
    /// to the BFT layer for inclusion in a batch.
    pub async fn add_unconfirmed_transaction(&self, transaction: Transaction<N>) -> Result<()> {
        // Calculate the transmission checksum.
        let checksum = Data::<Transaction<N>>::Buffer(transaction.to_bytes_le()?.into()).to_checksum::<N>()?;
        // Queue the unconfirmed transaction.
        {
            let transaction_id = transaction.id();

            // Check that the transaction is not a fee transaction.
            if transaction.is_fee() {
                bail!("Transaction '{}' is a fee transaction {}", fmt_id(transaction_id), "(skipping)".dimmed());
            }
            // Check if the transaction was recently seen.
            if self.seen_transactions.lock().put(transaction_id, ()).is_some() {
                // If the transaction was recently seen, return early.
                return Ok(());
            }
            // Check if the transaction already exists in the ledger.
            if self.ledger.contains_transmission(&TransmissionID::Transaction(transaction_id, checksum))? {
                bail!("Transaction '{}' exists in the ledger {}", fmt_id(transaction_id), "(skipping)".dimmed());
            }
            // Check that the transaction is not in the mempool.
            if self.contains_transaction(&transaction_id) {
                bail!("Transaction '{}' exists in the memory pool", fmt_id(transaction_id));
            }
            // Add the transaction to the memory pool.
            let priority_fee = transaction.priority_fee_amount()?;
            self.transactions_queue.write().insert(transaction_id, transaction, priority_fee)?;
            #[cfg(feature = "metrics")]
            {
                metrics::increment_gauge(metrics::consensus::UNCONFIRMED_TRANSACTIONS, 1f64);
                let timestamp = snarkos_node_bft::helpers::now();
                self.transmissions_tracker
                    .lock()
                    .insert(TransmissionID::Transaction(transaction_id, checksum), timestamp);
            }
            trace!("Received unconfirmed transaction '{}' in the queue", fmt_id(transaction_id));
        }

        // Try to process the unconfirmed transactions in the memory pool.
        self.process_unconfirmed_transactions().await
    }

    /// Processes unconfirmed transactions in the mempool, and passes them to the BFT layer
    /// (if sufficient space is available).
    async fn process_unconfirmed_transactions(&self) -> Result<()> {
        // If the memory pool of this node is full, return early.
        let num_unconfirmed_transmissions = self.num_unconfirmed_transmissions();
        if num_unconfirmed_transmissions >= Primary::<N>::MAX_TRANSMISSIONS_TOLERANCE {
            return Ok(());
        }
        // Retrieve the transactions.
        let transactions = {
            // Determine the available capacity.
            let capacity = Primary::<N>::MAX_TRANSMISSIONS_TOLERANCE.saturating_sub(num_unconfirmed_transmissions);
            // Acquire the lock on the transactions queue.
            let mut tx_queue = self.transactions_queue.write();
            // Determine the number of deployments to send.
            let num_deployments = tx_queue.deployments.len().min(capacity).min(MAX_DEPLOYMENTS_PER_INTERVAL);
            // Determine the number of executions to send.
            let num_executions = tx_queue.executions.len().min(capacity.saturating_sub(num_deployments));
            // Create an iterator which will select interleaved deployments and executions within the capacity.
            // Note: interleaving ensures we will never have consecutive invalid deployments blocking the queue.
            let selector_iter = (0..num_deployments).map(|_| true).interleave((0..num_executions).map(|_| false));
            // Drain the transactions from the queue, interleaving deployments and executions.
            let transactions = selector_iter
                .filter_map(
                    |select_deployment| {
                        if select_deployment { tx_queue.deployments.pop() } else { tx_queue.executions.pop() }
                    },
                )
                .map(|(_, tx)| tx)
                .collect_vec();
            #[cfg(feature = "metrics")]
            {
                metrics::gauge(
                    metrics::consensus::DEPLOYMENTS_PRIORITY_QUEUE_SIZE,
                    tx_queue.deployments.priority_len() as f64,
                );
                metrics::gauge(
                    metrics::consensus::DEPLOYMENTS_ZERO_FEE_QUEUE_SIZE,
                    tx_queue.deployments.zero_fee_len() as f64,
                );
                metrics::gauge(
                    metrics::consensus::EXECUTIONS_PRIORITY_QUEUE_SIZE,
                    tx_queue.executions.priority_len() as f64,
                );
                metrics::gauge(
                    metrics::consensus::EXECUTIONS_ZERO_FEE_QUEUE_SIZE,
                    tx_queue.executions.zero_fee_len() as f64,
                );
            }
            transactions
        };
        // Iterate over the transactions.
        for transaction in transactions.into_iter() {
            let transaction_id = transaction.id();
            // Determine the type of the transaction. The fee type is technically not possible here.
            let tx_type_str = match transaction {
                Transaction::Deploy(..) => "deployment",
                Transaction::Execute(..) => "execution",
                Transaction::Fee(..) => "fee",
            };
            trace!("Adding unconfirmed {tx_type_str} transaction '{}' to the memory pool...", fmt_id(transaction_id));
            // Send the unconfirmed transaction to the primary.
            match self.primary_sender.send_unconfirmed_transaction(transaction_id, Data::Object(transaction)).await {
                Ok(true) => {}
                Ok(false) => debug!(
                    "Unable to add unconfirmed {tx_type_str} transaction '{}' to the memory pool. Already exists.",
                    fmt_id(transaction_id)
                ),
                Err(err) => {
                    // If the BFT is synced, then log the warning.
                    if self.bft.is_synced() {
                        let err = err.context(format!(
                            "Unable to add unconfirmed {tx_type_str} transaction '{}' to the memory pool",
                            fmt_id(transaction_id)
                        ));
                        warn!("{}", flatten_error(err));
                    }
                }
            }
        }
        Ok(())
    }
}

impl<N: Network> Consensus<N> {
    /// Starts the BFT and consensus handlers.
    ///
    /// This consumes the deferred state stored during construction and must only be invoked once,
    /// after the node has finished syncing from the CDN, so that no blocks are committed during
    /// the initial sync.
    pub async fn start_consensus_handlers(&self) -> Result<()> {
        info!("Starting the consensus instance...");

        // Take the BFT run arguments stored during construction and start the BFT handlers.
        let Some((consensus_sender, primary_receiver)) = self.bft_run_args.lock().take() else {
            bail!("The consensus handlers have already been started");
        };
        self.bft
            .clone()
            .run(Some(self.ping.clone()), Some(consensus_sender), self.primary_sender.clone(), primary_receiver)
            .await?;

        // Take the consensus receiver stored during construction and start the consensus handlers.
        let Some(consensus_receiver) = self.consensus_receiver.lock().take() else {
            bail!("The consensus handlers have already been started");
        };
        let ConsensusReceiver { mut rx_consensus_subdag } = consensus_receiver;

        // Process the committed subdag and transmissions from the BFT.
        let self_ = self.clone();
        self.spawn(async move {
            while let Some((committed_subdag, transmissions, callback)) = rx_consensus_subdag.recv().await {
                self_.process_bft_subdag(committed_subdag, transmissions, callback).await;
            }
        });

        // Process the unconfirmed transactions in the memory pool.
        //
        // This loop wakes up either when a block is committed (signaled via notify) or after a timeout.
        // When a block is committed, the BFT workers have freed capacity for more transmissions.
        // The timeout serves as a fallback for startup or idle periods when blocks are not being produced.
        //
        // TODO(kaimast): wake up the loop after a proposal is created, not only when a block commits.
        let self_ = self.clone();
        self.spawn(async move {
            loop {
                // Wait for either a block commit notification or timeout.
                tokio::select! {
                    _ = self_.block_commit_notify.notified() => {}
                    _ = tokio::time::sleep(MAX_BATCH_DELAY) => {}
                }
                // Process the unconfirmed transactions in the memory pool.
                if let Err(err) = self_.process_unconfirmed_transactions().await {
                    warn!("{}", flatten_error(err.context("Cannot process unconfirmed transactions")));
                }
                // Process the unconfirmed solutions in the memory pool.
                if let Err(err) = self_.process_unconfirmed_solutions().await {
                    warn!("{}", flatten_error(err.context("Cannot process unconfirmed solutions")));
                }
            }
        });

        Ok(())
    }

    /// Attempts to build a new block from the given subDAG, and (tries to) advance the legder to it.
    ///
    /// # Returns
    /// - `Ok(true)` if the ledger was advanced to the next block.
    /// - `Ok(false)` if the block may be valide but the ledger already advanced.
    /// - `Err(anyhow::Error)` for incorrect blocks and any other error.
    async fn process_bft_subdag(
        &self,
        subdag: Subdag<N>,
        transmissions: IndexMap<TransmissionID<N>, Transmission<N>>,
        callback: oneshot::Sender<Result<bool>>,
    ) {
        // Try to advance to the next block.
        let self_ = self.clone();
        let transmissions_ = transmissions.clone();
        let result = spawn_blocking! { self_.try_advance_to_next_block(subdag, transmissions_).with_context(|| "Unable to advance to the next block") };

        // If the block failed to advance, reinsert the transmissions into the memory pool.
        match result {
            Ok(true) => {
                // On success, notify that the BFT workers have freed capacity for more transmissions.
                self.block_commit_notify.notify_one();
            }
            Ok(false) | Err(_) => self.reinsert_transmissions(transmissions).await,
        }

        callback.send(result).ok();
    }

    /// Attempts to advance the ledger to the next block, and updates the metrics (if enabled) accordingly.
    ///
    /// # Returns
    /// - `Ok(true)` if the ledger was advanced to the next block.
    /// - `Ok(false)` if the block may be valid but the ledger already advanced.
    /// - `Err(anyhow::Error)` for incorrect blocks and any other error.
    fn try_advance_to_next_block(
        &self,
        subdag: Subdag<N>,
        transmissions: IndexMap<TransmissionID<N>, Transmission<N>>,
    ) -> Result<bool> {
        #[cfg(feature = "metrics")]
        let start = subdag.leader_certificate().batch_header().timestamp();
        #[cfg(feature = "metrics")]
        let num_committed_certificates = subdag.values().map(|c| c.len()).sum::<usize>();
        #[cfg(feature = "metrics")]
        let current_block_timestamp = self.ledger.latest_block().header().metadata().timestamp();

        // Create the candidate next block.
        let ledger_update = self.ledger.begin_ledger_update()?;

        let prepare_instant = std::time::Instant::now();
        let block = match ledger_update.prepare_advance_to_next_quorum_block(subdag, transmissions) {
            Ok(block) => block,
            Err(CheckBlockError::BlockAlreadyExists { .. }) => {
                debug!("The given block hash already exists in the ledger");
                return Ok(false);
            }
            Err(CheckBlockError::InvalidHeight { .. }) => {
                debug!("The ledger advanced while we were constructing the next block");
                return Ok(false);
            }
            Err(CheckBlockError::InvalidRound { new, previous }) => {
                debug!("The subDAG round is too low. Expected >{previous}, got {new}");
                return Ok(false);
            }
            Err(err) => return Err(err.into_anyhow()),
        };
        let prepare_elapsed = prepare_instant.elapsed();
        trace!("prepare_advance_to_next_quorum_block took {:.3}s", prepare_elapsed.as_secs_f64());
        #[cfg(feature = "metrics")]
        metrics::histogram(metrics::consensus::PREPARE_ADVANCE_SECS, prepare_elapsed.as_secs_f64());

        let check_instant = std::time::Instant::now();
        cfg_if! {
            if #[cfg(feature = "test_network")] {
                // If we are using a hotswapped dev committee, skip checking the block.
                let result = if self.ledger.dev_committee_for_round(block.round())?.is_some() {
                    Ok(block)
                } else {
                    ledger_update.check_next_block(block)
                };
            } else {
                let result = ledger_update.check_next_block(block);
            }
        }

        let block = match result {
            Ok(block) => block,
            Err(CheckBlockError::BlockAlreadyExists { .. }) => {
                debug!("The given block hash already exists in the ledger");
                return Ok(false);
            }
            Err(CheckBlockError::InvalidHeight { .. }) => {
                debug!("The ledger advanced while we were constructing the next block");
                return Ok(false);
            }
            Err(CheckBlockError::InvalidRound { new, previous }) => {
                debug!("The subDAG round is too low. Expected >{previous}, got {new}");
                return Ok(false);
            }
            Err(err) => return Err(err.into_anyhow()),
        };

        let check_elapsed = check_instant.elapsed();
        trace!("check_next_block took {:.3}s", check_elapsed.as_secs_f64());
        #[cfg(feature = "metrics")]
        metrics::histogram(metrics::consensus::CHECK_NEXT_BLOCK_SECS, check_elapsed.as_secs_f64());

        let block_height = block.height();

        // Advance to the next block.
        let advance_instant = std::time::Instant::now();
        ledger_update.advance_to_next_block(&block)?;
        let advance_elapsed = advance_instant.elapsed();
        trace!("advance_to_next_block took {:.3}s", advance_elapsed.as_secs_f64());
        #[cfg(feature = "metrics")]
        metrics::histogram(metrics::consensus::ADVANCE_TO_NEXT_BLOCK_SECS, advance_elapsed.as_secs_f64());

        #[cfg(feature = "metrics")]
        // Fetch the committee lookback for the latest round.
        // Note: Do not abort here if this returns an error, because the committee is only needed for telemetry,
        // not for block advancement itself.
        let latest_committee = self.ledger.get_committee_lookback_for_round(self.ledger.latest_round());

        // If the next block starts a new epoch, clear the existing solutions.
        if block_height.is_multiple_of(N::NUM_BLOCKS_PER_EPOCH) {
            // Clear the solutions queue.
            self.solutions_queue.lock().clear();
            // Clear the worker solutions.
            self.bft.primary().clear_worker_solutions();
        }

        // Notify peers that we have a new block.
        match self.block_sync.get_block_locators() {
            Ok(locators) => self.ping.update_block_locators(locators),
            Err(err) => error!(
                "{}",
                flatten_error(err.context("Failed to generate new block locators after block advancement"))
            ),
        }

        // Make block sync aware of the new block.
        self.block_sync.set_sync_height(block_height);

        // TODO(kaimast): This should also remove any transmissions/solutions contained in the block from the mempool.
        // Removal currently happens when Consensus eventually passes them to the worker, which then just discards them.

        #[cfg(feature = "metrics")]
        {
            let now_utc = snarkos_node_bft::helpers::now_utc();
            let elapsed_secs = (now_utc.unix_timestamp() - start) as f64;
            let next_block_timestamp = block.header().metadata().timestamp();
            let next_block_utc = snarkos_node_bft::helpers::to_utc_datetime(next_block_timestamp);
            // Both endpoints are on-chain block timestamps (`i64` seconds by protocol design), so this
            // value can only ever be a whole number of seconds without a snarkVM protocol change.
            let block_latency = next_block_timestamp - current_block_timestamp;
            let block_lag = (now_utc - next_block_utc).whole_seconds() as f64;

            let proof_target = block.header().proof_target();
            let coinbase_target = block.header().coinbase_target();
            let cumulative_proof_target = block.header().cumulative_proof_target();

            // Calculate latency for all transmissions included in this block.
            metrics::add_transmission_latency_metric(&self.transmissions_tracker, &block);

            metrics::gauge(metrics::consensus::COMMITTED_CERTIFICATES, num_committed_certificates as f64);
            metrics::histogram(metrics::consensus::CERTIFICATE_COMMIT_LATENCY, elapsed_secs);
            metrics::histogram(metrics::consensus::BLOCK_LATENCY, block_latency as f64);
            metrics::histogram(metrics::consensus::BLOCK_LAG, block_lag);
            metrics::gauge(metrics::blocks::PROOF_TARGET, proof_target as f64);
            metrics::gauge(metrics::blocks::COINBASE_TARGET, coinbase_target as f64);
            metrics::gauge(metrics::blocks::CUMULATIVE_PROOF_TARGET, cumulative_proof_target as f64);

            // If telemetry is enabled, update participation scores.
            #[cfg(feature = "metrics")]
            {
                let telemetry = self.bft().primary().gateway().validator_telemetry();

                // Publish the vintage of the snapshot the scores are read from, and the number of
                // updates that never reached it. The scores are eventually consistent, so a reader
                // comparing them against block height needs both to tell a lagging snapshot and a
                // lossy one apart from an actual change in participation.
                metrics::gauge(
                    metrics::consensus::VALIDATOR_PARTICIPATION_GC_ROUND,
                    telemetry.published_gc_round() as f64,
                );
                metrics::gauge(metrics::consensus::VALIDATOR_PARTICIPATION_DROPPED, telemetry.num_dropped() as f64);

                match latest_committee {
                    Ok(latest_committee) => {
                        // Retrieve the individual participation scores.
                        let participation_scores = telemetry.get_participation_scores(&latest_committee);

                        // Export the certificate and signature participation scores as individual gauges.
                        for (address, (certificate_score, signature_score)) in participation_scores {
                            let address_str = address.to_string();
                            metrics::gauge_label(
                                metrics::consensus::VALIDATOR_CERTIFICATE_PARTICIPATION,
                                "validator_address",
                                address_str.clone(),
                                certificate_score,
                            );
                            metrics::gauge_label(
                                metrics::consensus::VALIDATOR_SIGNATURE_PARTICIPATION,
                                "validator_address",
                                address_str,
                                signature_score,
                            );
                        }
                    }
                    Err(err) => warn!("{}", flatten_error(err.context("Failed to get latest committee for telemetry"))),
                }
            }
        }

        Ok(true)
    }

    /// Reinserts the given transmissions into the memory pool.
    async fn reinsert_transmissions(&self, transmissions: IndexMap<TransmissionID<N>, Transmission<N>>) {
        // Iterate over the transmissions.
        for (transmission_id, transmission) in transmissions.into_iter() {
            // Reinsert the transmission into the memory pool.
            match self.reinsert_transmission(transmission_id, transmission).await {
                Ok(true) => {}
                Ok(false) => debug!(
                    "Unable to reinsert transmission {}:{} into the memory pool. Already exists.",
                    fmt_id(transmission_id),
                    fmt_id(transmission_id.checksum().unwrap_or_default()).dimmed()
                ),
                Err(err) => {
                    let err = err.context(format!(
                        "Unable to reinsert transmission {}.{} into the memory pool",
                        fmt_id(transmission_id),
                        fmt_id(transmission_id.checksum().unwrap_or_default()).dimmed()
                    ));
                    warn!("{}", flatten_error(err));
                }
            }
        }
    }

    /// Reinserts the given transmission into the memory pool.
    ///
    /// # Returns
    /// - `Ok(true)` if the transmission was added to the memory pool.
    /// - `Ok(false)` if the transmission was valid but already exists in the memory pool.
    /// - `Err(anyhow::Error)` if the transmission was invalid.
    async fn reinsert_transmission(
        &self,
        transmission_id: TransmissionID<N>,
        transmission: Transmission<N>,
    ) -> Result<bool> {
        // Initialize a callback sender and receiver.
        let (callback, callback_receiver) = oneshot::channel();
        // Send the transmission to the primary.
        match (transmission_id, transmission) {
            (TransmissionID::Ratification, Transmission::Ratification) => return Ok(true),
            (TransmissionID::Solution(solution_id, _), Transmission::Solution(solution)) => {
                // Send the solution to the primary.
                self.primary_sender.tx_unconfirmed_solution.send((solution_id, solution, callback)).await?;
            }
            (TransmissionID::Transaction(transaction_id, _), Transmission::Transaction(transaction)) => {
                // Send the transaction to the primary.
                self.primary_sender.tx_unconfirmed_transaction.send((transaction_id, transaction, callback)).await?;
            }
            _ => bail!("Mismatching `(transmission_id, transmission)` pair in consensus"),
        }
        // Await the callback.
        callback_receiver.await?
    }

    /// Spawns a task with the given future; it should only be used for long-running tasks.
    fn spawn<T: Future<Output = ()> + Send + 'static>(&self, future: T) {
        self.handles.lock().push(tokio::spawn(future));
    }

    /// Shuts down the consensus and BFT layers.
    pub async fn shut_down(&self) {
        info!("Shutting down consensus...");
        // Shut down the BFT.
        self.bft.shut_down().await;
        // Abort the tasks.
        self.handles.lock().iter().for_each(|handle| handle.abort());
    }
}
