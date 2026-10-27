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

mod proposal_task;
pub use proposal_task::ProposalTask;

use crate::{
    Gateway,
    MAX_BATCH_DELAY,
    MAX_LEADER_CERTIFICATE_DELAY,
    MAX_WORKERS,
    MIN_BATCH_DELAY,
    PRIMARY_PING_INTERVAL,
    Sync,
    Transport,
    WORKER_PING_INTERVAL,
    Worker,
    events::{BatchPropose, BatchSignature, Event},
    helpers::{
        PrimaryReceiver,
        PrimarySender,
        Proposal,
        ProposalCache,
        SignedProposals,
        Storage,
        assign_to_worker,
        assign_to_workers,
        fmt_id,
        init_sync_channels,
        init_worker_channels,
        now,
    },
    spawn_blocking,
    sync::SyncCallback,
};

use snarkos_account::Account;
use snarkos_node_bft_events::PrimaryPing;
use snarkos_node_bft_ledger_service::{LedgerService, deserialize_transaction_strict};
#[cfg(test)]
use snarkos_node_network::ConnectionMode;
use snarkos_node_network::PeerPoolHandling;
use snarkos_node_sync::{BlockSync, DUMMY_SELF_IP, Ping};
use snarkos_utilities::{CallbackHandle, NodeDataDir};

use snarkvm::{
    console::{
        prelude::*,
        types::{Address, Field},
    },
    ledger::{
        block::Transaction,
        narwhal::{BatchCertificate, BatchHeader, Data, Transmission, TransmissionID},
        puzzle::{Solution, SolutionID},
    },
    prelude::{Signature, committee::Committee},
    utilities::flatten_error,
};

use anyhow::Context;
use colored::Colorize;
use futures::stream::{FuturesUnordered, StreamExt};
use indexmap::{IndexMap, IndexSet};
#[cfg(feature = "locktick")]
use locktick::{
    parking_lot::{Mutex, RwLock},
    tokio::RwLock as TRwLock,
};
#[cfg(not(feature = "locktick"))]
use parking_lot::{Mutex, RwLock};
#[cfg(not(feature = "serial"))]
use rayon::prelude::*;
use std::{
    collections::{HashMap, HashSet},
    future::Future,
    net::SocketAddr,
    pin::Pin,
    sync::{Arc, OnceLock},
    time::Instant,
};
#[cfg(not(feature = "locktick"))]
use tokio::sync::RwLock as TRwLock;
use tokio::{sync::Notify, task::JoinHandle};
use tokio_util::sync::CancellationToken;

/// The state of the primary's batch proposal.
#[derive(Debug, Default, PartialEq, Eq)]
pub enum ProposedBatchState<N: Network> {
    /// No batch is currently being proposed.
    #[default]
    None,
    /// A batch is being proposed and awaiting signatures.
    Certifying(Box<Proposal<N>>),
    /// A batch has reached quorum and is being inserted into storage.
    /// Carries the batch ID so late-arriving signatures can be recognized and silently dropped.
    Certified(Field<N>),
}

impl<N: Network> ProposedBatchState<N> {
    /// Returns `true` if the primary has no active batch proposal.
    pub fn is_none(&self) -> bool {
        matches!(self, Self::None)
    }

    /// Returns `true` if a batch is currently being proposed (awaiting signatures).
    pub fn is_proposed(&self) -> bool {
        matches!(self, Self::Certifying(_))
    }

    /// Returns a reference to the in-progress proposal, or `None` if not in the `Certifying` state.
    pub fn as_proposal(&self) -> Option<&Proposal<N>> {
        match self {
            Self::Certifying(p) => Some(p.as_ref()),
            _ => None,
        }
    }
}

/// A helper type to keep track of the state of the primary's batch proposal.
pub type ProposedBatch<N> = RwLock<ProposedBatchState<N>>;

/// This callback trait allows listening to changes in the Primary, such as round advancement.
/// This is implemented by [`BFT`].
#[async_trait::async_trait]
pub trait PrimaryCallback<N: Network>: Send + std::marker::Sync {
    /// Asks the callback to if we can move to the next round.
    ///
    /// # Arguments
    /// * `current_round` - the round the Primary is in (to avoid race conditions)
    ///
    /// # Returns
    /// `true` if we moved to the next round.
    fn try_advance_to_next_round(&self, current_round: u64) -> bool;

    /// Add a certificated that was created by the primary or received from a peer.
    async fn add_new_certificate(&self, certificate: BatchCertificate<N>) -> Result<()>;
}

/// The primary logic of a node.
/// AleoBFT adopts a primary-worker architecture as described in the Narwhal and Tusk paper (Section 4.2).
#[derive(Clone)]
pub struct Primary<N: Network> {
    /// The sync module enables fetching data from other validators.
    sync: Sync<N>,
    /// The gateway allows talking to other nodes in the validator set.
    gateway: Gateway<N>,
    /// The storage.
    storage: Storage<N>,
    /// The ledger service.
    ledger: Arc<dyn LedgerService<N>>,
    /// The workers.
    workers: Arc<OnceLock<Vec<Worker<N>>>>,

    /// The primary callback (used by [`BFT`]).
    primary_callback: Arc<CallbackHandle<Arc<dyn PrimaryCallback<N>>>>,

    /// The batch proposal, if the primary is currently proposing a batch.
    proposed_batch: Arc<ProposedBatch<N>>,

    /// The instant at which the current batch was proposed (used to measure certification latency).
    /// (used for higher precision in the metrics compared to the batch timestamp)
    #[cfg(feature = "metrics")]
    batch_propose_start: Arc<Mutex<Option<Instant>>>,

    /// Holds the most recent round and timestamp that the primary proposed a batch for.
    /// TODO(kaimast): avoiding using an async lock here, so this can be merged with the `proposed_batch`,
    /// to have a unified `primary_state` field.
    latest_proposal_timestamp: Arc<TRwLock<Option<(u64, i64)>>>,

    /// The recently-signed batch proposals.
    signed_proposals: Arc<RwLock<SignedProposals<N>>>,

    /// Cancels the resends launched by the latest recheck of the pending proposal.
    /// Replaced (and the old one cancelled) on every recheck, so at most one set of resends is in flight.
    resend_token: Arc<Mutex<CancellationToken>>,

    /// The handles for all background tasks spawned by this primary.
    handles: Arc<Mutex<Vec<JoinHandle<()>>>>,

    /// The node configuration directory.
    node_data_dir: NodeDataDir,

    /// Manages proposal readiness state and drives the batch proposal loop.
    proposal_task: ProposalTask<N>,

    /// Used to wake up a the dedicated round-increment task, if we may be able to advance to the next round.
    /// This is used, so the timeout for round advancement is reset on every round increment.
    round_increment_notify: Arc<Notify>,

    /// Factor applied to the batch spend limit while this primary builds a proposal.
    proposal_spent_limit_multiplier: u64,
}

impl<N: Network> Primary<N> {
    /// The maximum number of unconfirmed transmissions to send to the primary.
    pub const MAX_TRANSMISSIONS_TOLERANCE: usize = BatchHeader::<N>::MAX_TRANSMISSIONS_PER_BATCH * 2;

    /// Initializes a new primary instance.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        account: Account<N>,
        storage: Storage<N>,
        ledger: Arc<dyn LedgerService<N>>,
        block_sync: Arc<BlockSync<N>>,
        ip: Option<SocketAddr>,
        trusted_validators: &[SocketAddr],
        trusted_peers_only: bool,
        node_data_dir: NodeDataDir,
        dev: Option<u16>,
        proposal_spent_limit_multiplier: u64,
    ) -> Result<Self> {
        // Initialize the gateway.
        let gateway = Gateway::new(
            account,
            storage.clone(),
            ledger.clone(),
            ip,
            trusted_validators,
            trusted_peers_only,
            node_data_dir.clone(),
            dev,
        )?;
        // Initialize the sync module.
        let sync = Sync::new(gateway.clone(), storage.clone(), ledger.clone(), block_sync);

        // Initialize the primary instance.
        Ok(Self {
            sync,
            gateway,
            storage,
            ledger,
            node_data_dir,
            workers: Default::default(),
            primary_callback: Default::default(),
            proposed_batch: Default::default(),
            #[cfg(feature = "metrics")]
            batch_propose_start: Default::default(),
            latest_proposal_timestamp: Default::default(),
            signed_proposals: Default::default(),
            resend_token: Default::default(),
            handles: Default::default(),
            proposal_task: Default::default(),
            round_increment_notify: Default::default(),
            proposal_spent_limit_multiplier,
        })
    }

    /// Load the proposal cache file and update the Primary state with the stored data.
    async fn load_proposal_cache(&self) -> Result<()> {
        // Fetch the signed proposals from the file system if it exists.
        match ProposalCache::<N>::exists(&self.node_data_dir) {
            // If the proposal cache exists, then process the proposal cache.
            true => match ProposalCache::<N>::load(self.gateway.account().address(), &self.node_data_dir) {
                Ok(proposal_cache) => {
                    // Extract the proposal and signed proposals.
                    let (latest_certificate_round, proposed_batch, signed_proposals, pending_certificates) =
                        proposal_cache.into();

                    *self.latest_proposal_timestamp.write().await = Some((latest_certificate_round, now()));
                    *self.proposed_batch.write() = match proposed_batch {
                        Some(p) => ProposedBatchState::Certifying(Box::new(p)),
                        None => ProposedBatchState::None,
                    };
                    *self.signed_proposals.write() = signed_proposals;

                    // Update the storage with the pending certificates.
                    for certificate in pending_certificates {
                        let batch_id = certificate.batch_id();
                        // We use a dummy IP because the node should not need to request from any peers.
                        // The storage should have stored all the transmissions. If not, we simply
                        // skip the certificate.
                        if let Err(err) = self.sync_with_certificate_from_peer(DUMMY_SELF_IP, certificate).await {
                            let err = err.context(format!(
                                "Failed to load stored certificate {} from proposal cache",
                                fmt_id(batch_id)
                            ));
                            warn!("{}", &flatten_error(err));
                        }
                    }
                    Ok(())
                }
                Err(err) => Err(err.context("Failed to read the signed proposals from the file system")),
            },
            // If the proposal cache does not exist, then return early.
            false => Ok(()),
        }
    }

    /// Run the primary instance.
    pub async fn run(
        &self,
        ping: Option<Arc<Ping<N>>>,
        primary_callback: Option<Arc<dyn PrimaryCallback<N>>>,
        sync_callback: Option<Arc<dyn SyncCallback<N>>>,
        primary_sender: PrimarySender<N>,
        primary_receiver: PrimaryReceiver<N>,
    ) -> Result<()> {
        info!("Starting the primary instance of the memory pool...");

        // Set the BFT sender.
        if let Some(callback) = primary_callback {
            self.primary_callback.set(callback)?;
        }

        // Construct a map of the worker senders.
        let mut worker_senders = IndexMap::new();
        // Construct a map for the workers.
        let mut workers = Vec::new();
        // Initialize the workers.
        for id in 0..MAX_WORKERS {
            // Construct the worker channels.
            let (tx_worker, rx_worker) = init_worker_channels();
            // Construct the worker instance.
            let worker = Worker::new(
                id,
                Arc::new(self.gateway.clone()),
                self.storage.clone(),
                self.ledger.clone(),
                self.proposed_batch.clone(),
            )?;
            // Run the worker instance.
            worker.run(rx_worker);
            // Add the worker to the list of workers.
            workers.push(worker);
            // Add the worker sender to the map.
            worker_senders.insert(id, tx_worker);
        }
        // Set the workers.
        if self.workers.set(workers).is_err() {
            bail!("Workers already set. `Primary::run` cannot be called more than once.");
        }

        // First, initialize the sync channels.
        let (sync_sender, sync_receiver) = init_sync_channels();
        // Next, initialize the sync module and sync the storage from ledger.
        self.sync.initialize(sync_callback)?;
        // Next, load and process the proposal cache before running the sync module.
        self.load_proposal_cache().await?;
        // Next, run the sync module.
        self.sync.run(ping, sync_receiver).await?;
        // Next, initialize the gateway.
        self.gateway.run(primary_sender, worker_senders, Some(sync_sender)).await;
        // Lastly, start the primary handlers.
        self.start_handlers(primary_receiver);

        Ok(())
    }

    /// Returns the current round.
    pub fn current_round(&self) -> u64 {
        self.storage.current_round()
    }

    /// Returns `true` if the primary is synced.
    pub fn is_synced(&self) -> bool {
        self.sync.is_synced()
    }

    /// Returns the gateway.
    pub const fn gateway(&self) -> &Gateway<N> {
        &self.gateway
    }

    /// Returns the storage.
    pub const fn storage(&self) -> &Storage<N> {
        &self.storage
    }

    /// Returns the ledger.
    pub const fn ledger(&self) -> &Arc<dyn LedgerService<N>> {
        &self.ledger
    }

    /// Returns the number of workers.
    pub fn num_workers(&self) -> u8 {
        u8::try_from(self.workers.get().expect("Primary is not running yet").len()).expect("Too many workers")
    }

    /// Returns the workers.
    pub fn workers(&self) -> &[Worker<N>] {
        self.workers.get().expect("Primary is not running yet")
    }
}

impl<N: Network> Primary<N> {
    /// Returns the number of unconfirmed transmissions.
    pub fn num_unconfirmed_transmissions(&self) -> usize {
        self.workers().iter().map(|worker| worker.num_transmissions()).sum()
    }

    /// Returns the number of unconfirmed ratifications.
    pub fn num_unconfirmed_ratifications(&self) -> usize {
        self.workers().iter().map(|worker| worker.num_ratifications()).sum()
    }

    /// Returns the number of unconfirmed solutions.
    pub fn num_unconfirmed_solutions(&self) -> usize {
        self.workers().iter().map(|worker| worker.num_solutions()).sum()
    }

    /// Returns the number of unconfirmed transactions.
    pub fn num_unconfirmed_transactions(&self) -> usize {
        self.workers().iter().map(|worker| worker.num_transactions()).sum()
    }
}

impl<N: Network> Primary<N> {
    /// Returns the worker transmission IDs.
    pub fn worker_transmission_ids(&self) -> impl '_ + Iterator<Item = TransmissionID<N>> {
        self.workers().iter().flat_map(|worker| worker.transmission_ids())
    }

    /// Returns the worker transmissions.
    pub fn worker_transmissions(&self) -> impl '_ + Iterator<Item = (TransmissionID<N>, Transmission<N>)> {
        self.workers().iter().flat_map(|worker| worker.transmissions())
    }

    /// Returns the worker solutions.
    pub fn worker_solutions(&self) -> impl '_ + Iterator<Item = (SolutionID<N>, Data<Solution<N>>)> {
        self.workers().iter().flat_map(|worker| worker.solutions())
    }

    /// Returns the worker transactions.
    pub fn worker_transactions(&self) -> impl '_ + Iterator<Item = (N::TransactionID, Data<Transaction<N>>)> {
        self.workers().iter().flat_map(|worker| worker.transactions())
    }
}

impl<N: Network> Primary<N> {
    /// Clears the worker solutions.
    pub fn clear_worker_solutions(&self) {
        self.workers().iter().for_each(Worker::clear_solutions);
    }
}

#[async_trait::async_trait]
impl<N: Network> proposal_task::BatchPropose for Primary<N> {
    fn current_round(&self) -> u64 {
        Primary::current_round(self)
    }

    /// Proposes the batch for the current round.
    ///
    /// This method performs the following steps:
    /// 1. Drain the workers.
    /// 2. Sign the batch.
    /// 3. Set the batch proposal in the primary.
    /// 4. Broadcast the batch header to all validators for signing.
    ///
    /// # Returns
    /// - `Ok(true)` if the batch was proposed.
    /// - `Ok(false)` if the batch was not proposed for a benign reason, e.g., the timestamp is too soon after the previous certificate.
    /// - `Err(err)` if an unexpected error occured.
    async fn propose_batch(&self) -> Result<bool> {
        // Ensure there are not concurrent executions of this function.
        //
        // Note, in the current design, this function is only invoked from the batch proposal task, and it is technically
        // not possible for there to be concurrent invocations of the function, but we keep this lock for now.
        let mut lock_guard = self.latest_proposal_timestamp.write().await;

        // Check if the proposed batch has expired, and clear it if it has expired.
        if let Err(err) = self
            .check_proposed_batch_for_expiration()
            .with_context(|| "Failed to check the proposed batch for expiration")
        {
            warn!("{}", flatten_error(&err));
            return Ok(false);
        }

        // Retrieve the current round.
        let round = self.current_round();
        // Compute the previous round.
        let previous_round = round.saturating_sub(1);

        // If the current round is 0, return early.
        // This can actually never happen, because of the invariant that the current round is never 0
        // (see [`StorageInner::current_round`]).
        ensure!(round > 0, "Round 0 cannot have transaction batches");

        // If the current storage round is below the latest proposal round, then return early.
        if let Some((latest_round, _)) = &*lock_guard
            && round < *latest_round
        {
            warn!("Cannot propose a batch for round {round} - the latest proposal cache round is {latest_round}");
            return Ok(false);
        }

        // If there is a batch being proposed or certified already, handle accordingly.
        match &*self.proposed_batch.read() {
            ProposedBatchState::Certifying(proposal) => {
                // Ensure that the storage is caught up to the proposal before proceeding to rebroadcast this.
                if round < proposal.round()
                    || proposal
                        .batch_header()
                        .previous_certificate_ids()
                        .iter()
                        .any(|id| !self.storage.contains_certificate(*id))
                {
                    warn!(
                        "Cannot propose a batch for round {} - the current storage (round {round}) is not caught up to the proposed batch.",
                        proposal.round(),
                    );
                    return Ok(false);
                }
                // Resends are only useful while the proposal's round is ongoing.
                // Note: if the round is over, the previous resends are already cancelled, as they are scoped to it.
                let round_token = self.storage.round_cancellation_token(proposal.round());
                if round_token.is_cancelled() {
                    debug!("Not resending batch proposal for round {} (round is over)", proposal.round());
                    return Ok(false);
                }
                // Retrieve the committee lookback for the proposal's round.
                let committee_lookback = self.ledger.get_committee_lookback_for_round(proposal.round())?;
                // Construct the event, and serialize the batch header once for all non-signers, rather than
                // once per peer in `Transport::send`.
                // Note: this is done inline, as the proposed batch lock is held (so it can't be awaited on),
                // and serializing a batch header is cheap.
                let mut event = Event::BatchPropose(proposal.batch_header().clone().into());
                if let Err(err) = event.serialize_payload() {
                    error!("Unable to serialize the batch proposal for round {} - {err}", proposal.round());
                    return Ok(false);
                }
                // Supersede the previous recheck's resends, now that their replacements are about to be spawned.
                // Note: this is done after the fallible steps above, so a failure keeps the previous resends alive.
                let token = round_token.child_token();
                std::mem::replace(&mut *self.resend_token.lock(), token.clone()).cancel();
                // Iterate through the non-signers.
                for address in proposal.nonsigners(&committee_lookback) {
                    // Resolve the address to the peer IP.
                    match self.gateway.resolver().read().get_peer_ip_for_address(address) {
                        // Resend the batch proposal to the validator for signing.
                        Some(peer_ip) => {
                            let (gateway, event_, round, token) =
                                (self.gateway.clone(), event.clone(), proposal.round(), token.clone());
                            self.spawn(async move {
                                debug!("Resending batch proposal for round {round} to peer '{peer_ip}'");
                                // Resend the batch proposal to the peer, unless the round ends first.
                                tokio::select! {
                                    biased;
                                    _ = token.cancelled() => {
                                        debug!("Stopped resending batch proposal for round {round} to peer '{peer_ip}' (superseded or round is over)");
                                    }
                                    result = gateway.send(peer_ip, event_) => {
                                        if result.is_none() {
                                            warn!("Failed to resend batch proposal for round {round} to peer '{peer_ip}'");
                                        }
                                    }
                                }
                            });
                        }
                        None => continue,
                    }
                }
                debug!("Proposed batch for round {} is still valid", proposal.round());
                return Ok(false);
            }
            // A batch is being certified; wait until it completes before proposing another.
            ProposedBatchState::Certified(_) => {
                debug!("Cannot propose a batch for round {round} - a batch is currently being certified");
                return Ok(false);
            }
            ProposedBatchState::None => {
                // No batch in progress, so it is save to propose a new one.
            }
        }

        #[cfg(feature = "metrics")]
        metrics::gauge(metrics::bft::PROPOSAL_ROUND, round as f64);

        // Ensure that the primary does not create a new proposal too quickly.
        if let Some((_, latest_timestamp)) = &*lock_guard
            && !self.check_own_proposal_timestamp(previous_round, *latest_timestamp, now())?
        {
            return Ok(false);
        }

        // Ensure the primary has not proposed a batch for this round before.
        if self.storage.contains_certificate_in_round_from(round, self.gateway.account().address()) {
            // If a BFT sender was provided, attempt to advance the current round.
            if let Some(cb) = &*self.primary_callback.get_ref() {
                match cb.try_advance_to_next_round(self.current_round()) {
                    true => (), // continue,
                    false => return Ok(false),
                }
            }
            debug!("Primary is safely skipping {}", format!("(round {round} was already certified)").dimmed());
            return Ok(false);
        }

        // Determine if the current round has been proposed.
        // Note: Do NOT make this judgment in advance before rebroadcast and round update. Rebroadcasting is
        // good for network reliability and should not be prevented for the already existing proposed_batch.
        // If a certificate already exists for the current round, an attempt should be made to advance the
        // round as early as possible.
        if let Some((latest_round, _)) = &*lock_guard
            && *latest_round == round
        {
            debug!("Primary is safely skipping a batch proposal - round {round} already proposed");
            return Ok(false);
        }

        // Retrieve the committee to check against.
        let committee_lookback = self.ledger.get_committee_lookback_for_round(round)?;
        // Check if the primary is connected to enough validators to reach quorum threshold.
        {
            // Retrieve the connected validator addresses.
            let mut connected_validators = self.gateway.connected_addresses();
            // Append the primary to the set.
            connected_validators.insert(self.gateway.account().address());
            // If quorum threshold is not reached, return early.
            if !committee_lookback.is_quorum_threshold_reached(&connected_validators) {
                debug!(
                    "Primary is safely skipping a batch proposal for round {round} {}",
                    "(please connect to more validators)".dimmed()
                );
                trace!("Primary is connected to {} validators", connected_validators.len() - 1);
                return Ok(false);
            }
        }

        // Retrieve the previous certificates.
        let previous_certificates = self.storage.get_certificates_for_round(previous_round);

        // Check if the batch is ready to be proposed.
        // Note: The primary starts at round 1, and round 0 contains no certificates, by definition.
        let mut is_ready = previous_round == 0;
        // If the previous round is not 0, check if the previous certificates have reached the quorum threshold.
        if previous_round > 0 {
            // Retrieve the committee lookback for the round.
            let Ok(previous_committee_lookback) = self.ledger.get_committee_lookback_for_round(previous_round) else {
                bail!("Cannot propose a batch for round {round}: the committee lookback is not known yet")
            };
            // Construct a set over the authors.
            let authors = previous_certificates.iter().map(BatchCertificate::author).collect();
            // Check if the previous certificates have reached the quorum threshold.
            if previous_committee_lookback.is_quorum_threshold_reached(&authors) {
                is_ready = true;
            }
            #[cfg(feature = "test_network")]
            {
                // If we are using a hotswapped dev committee, use simplified checks to more easily advance.
                if let Some(dev_committee) = self.ledger.dev_committee_for_round(previous_round)?
                    && round <= dev_committee.starting_round()
                {
                    is_ready = true;
                }
            }
        }
        // If the batch is not ready to be proposed, return early.
        if !is_ready {
            debug!(
                "Primary is safely skipping a batch proposal for round {round} {}",
                format!("(previous round {previous_round} has not reached quorum)").dimmed()
            );
            return Ok(false);
        }

        // Initialize the map of transmissions.
        let mut transmissions: IndexMap<_, _> = Default::default();
        // Track the total execution costs of the batch proposal as it is being constructed.
        let mut proposal_cost = 0u64;
        // Note: worker draining and transaction inclusion needs to be thought
        // through carefully when there is more than one worker. The fairness
        // provided by one worker (FIFO) is no longer guaranteed with multiple workers.
        debug_assert_eq!(MAX_WORKERS, 1);

        'outer: for worker in self.workers().iter() {
            let mut num_worker_transmissions = 0usize;

            while let Some((id, transmission)) = worker.remove_front() {
                // Check the selected transmissions are below the batch limit.
                if transmissions.len() >= BatchHeader::<N>::MAX_TRANSMISSIONS_PER_BATCH {
                    // Reinsert the transmission into the worker.
                    worker.insert_front(id, transmission);
                    break 'outer;
                }

                // Check the max transmissions per worker is not exceeded.
                if num_worker_transmissions >= Worker::<N>::MAX_TRANSMISSIONS_PER_WORKER {
                    // Reinsert the transmission into the worker.
                    worker.insert_front(id, transmission);
                    continue 'outer;
                }

                // Check if the ledger already contains the transmission.
                if self.ledger.contains_transmission(&id).unwrap_or(true) {
                    trace!("Proposing - Skipping transmission '{}' - Already in ledger", fmt_id(id));
                    continue;
                }

                // Check if storage already knows the transmission, either way.
                //
                // This is the one place that wants both states: a transmission already in storage is
                // already in the DAG, and an ID storage knows only as aborted was already decided by
                // a block, so re-proposing it yields a certificate that storage cannot serve at
                // commit time - the aborted marker answers the containment check, but
                // `get_transmission` still has nothing to hand back. Both are skipped
                // unconditionally - proposing an empty batch is valid, so there is no need to make
                // an exception for the first transmission.
                if self.storage.contains_retrievable_transmission(id) || self.storage.contains_aborted_transmission(id)
                {
                    trace!("Proposing - Skipping transmission '{}' - Already in storage", fmt_id(id));
                    continue;
                }

                // Check the transmission is still valid.
                match (id, transmission.clone()) {
                    (TransmissionID::Solution(solution_id, checksum), Transmission::Solution(solution)) => {
                        // Ensure the checksum matches. If not, skip the solution.
                        if !matches!(solution.to_checksum::<N>(), Ok(solution_checksum) if solution_checksum == checksum)
                        {
                            trace!("Proposing - Skipping solution '{}' - Checksum mismatch", fmt_id(solution_id));
                            continue;
                        }
                        // Check if the solution is still valid.
                        if let Err(e) = self.ledger.check_solution_basic(solution_id, solution).await {
                            trace!("Proposing - Skipping solution '{}' - {e}", fmt_id(solution_id));
                            continue;
                        }
                    }
                    (TransmissionID::Transaction(transaction_id, checksum), Transmission::Transaction(transaction)) => {
                        // Ensure the checksum matches. If not, skip the transaction.
                        if !matches!(transaction.to_checksum::<N>(), Ok(transaction_checksum) if transaction_checksum == checksum )
                        {
                            trace!("Proposing - Skipping transaction '{}' - Checksum mismatch", fmt_id(transaction_id));
                            continue;
                        }

                        // Deserialize the transaction. If the transaction exceeds the maximum size, then return an error.
                        let transaction = spawn_blocking!(deserialize_transaction_strict(transaction))?;

                        // Fetch the current block height and consensus version.
                        let current_block_height = self.ledger.latest_block_height();
                        let consensus_version = N::CONSENSUS_VERSION(current_block_height)?;

                        // Compute the transaction spent cost (in microcredits).
                        // Note: We purposefully discard this transaction if we are unable to compute the spent cost.
                        let Ok(cost) = self.ledger.transaction_spend_in_microcredits(&transaction, consensus_version)
                        else {
                            debug!(
                                "Proposing - Skipping and discarding transaction '{}' - Unable to compute transaction spent cost",
                                fmt_id(transaction_id)
                            );
                            continue;
                        };

                        // Check if the transaction is still valid.
                        if let Err(e) = self.ledger.check_transaction_basic(transaction_id, transaction).await {
                            trace!("Proposing - Skipping transaction '{}' - {e}", fmt_id(transaction_id));
                            continue;
                        }

                        // Compute the next proposal cost.
                        // Note: We purposefully discard this transaction if the proposal cost overflows.
                        let Some(next_proposal_cost) = proposal_cost.checked_add(cost) else {
                            debug!(
                                "Proposing - Skipping and discarding transaction '{}' - Proposal cost overflowed",
                                fmt_id(transaction_id)
                            );
                            continue;
                        };

                        // Check if the next proposal cost exceeds the batch proposal spend limit.
                        let batch_spend_limit = BatchHeader::<N>::batch_spend_limit(current_block_height);
                        let proposal_spend_limit =
                            batch_spend_limit.saturating_mul(self.proposal_spent_limit_multiplier);
                        if next_proposal_cost > proposal_spend_limit {
                            debug!(
                                "Proposing - Skipping transaction '{}' - Batch spend limit surpassed ({next_proposal_cost} > {proposal_spend_limit})",
                                fmt_id(transaction_id),
                            );

                            // Reinsert the transmission into the worker.
                            worker.insert_front(id, transmission);
                            break 'outer;
                        }

                        // Update the proposal cost.
                        proposal_cost = next_proposal_cost;
                    }

                    // Note: We explicitly forbid including ratifications,
                    // as the protocol currently does not support ratifications.
                    (TransmissionID::Ratification, Transmission::Ratification) => continue,
                    // All other combinations are clearly invalid.
                    _ => continue,
                }

                // If the transmission is valid, insert it into the proposal's transmission list.
                transmissions.insert(id, transmission);
                num_worker_transmissions = num_worker_transmissions.saturating_add(1);
            }
        }

        // Determine the current timestamp.
        let current_timestamp = now();

        /* Proceeding to sign & propose the batch. */
        info!("Proposing a batch with {} transmissions for round {round}...", transmissions.len());

        // Update the latest proposed round and timestamp.
        *lock_guard = Some((round, current_timestamp));
        // Retrieve the private key.
        let private_key = *self.gateway.account().private_key();
        // Retrieve the committee ID.
        let committee_id = committee_lookback.id();
        // Prepare the transmission IDs.
        let transmission_ids = transmissions.keys().copied().collect();
        // Prepare the previous batch certificate IDs.
        let previous_certificate_ids = previous_certificates.into_iter().map(|c| c.id()).collect();
        // Sign the batch header and construct the proposal.
        let (batch_header, proposal) = spawn_blocking!(BatchHeader::new(
            &private_key,
            round,
            current_timestamp,
            committee_id,
            transmission_ids,
            previous_certificate_ids,
            &mut rand::rng()
        ))
        .and_then(|batch_header| {
            Proposal::new(committee_lookback, batch_header.clone(), transmissions.clone())
                .map(|proposal| (batch_header, proposal))
        })
        .inspect_err(|_| {
            // On error, reinsert the transmissions and then propagate the error.
            if let Err(err) = self.reinsert_transmissions_into_workers(transmissions) {
                error!("{}", flatten_error(err.context("Failed to reinsert transmissions")));
            }
        })?;

        // Broadcast the batch to all validators for signing, until the round is over.
        // Note: `round` may be stale by now, as proposing awaits; peers still sign for the previous round,
        // so the broadcast is scoped to the current round instead.
        self.gateway.broadcast_until(
            Event::BatchPropose(batch_header.into()),
            self.storage.round_cancellation_token(self.current_round()),
        );
        // Store the proposal in memory.
        *self.proposed_batch.write() = ProposedBatchState::Certifying(Box::new(proposal));
        // Record the wall-clock time at which the batch was proposed.
        #[cfg(feature = "metrics")]
        {
            *self.batch_propose_start.lock() = Some(Instant::now());
        }

        Ok(true)
    }
}

impl<N: Network> Primary<N> {
    /// Processes a batch propose from a peer.
    ///
    /// This method performs the following steps:
    /// 1. Verify the batch.
    /// 2. Sign the batch.
    /// 3. Broadcast the signature back to the validator.
    ///
    /// If our primary is ahead of the peer, we will not sign the batch.
    /// If our primary is behind the peer, but within GC range, we will sync up to the peer's round, and then sign the batch.
    async fn process_batch_propose_from_peer(&self, peer_ip: SocketAddr, batch_propose: BatchPropose<N>) -> Result<()> {
        let BatchPropose { round: batch_round, batch_header } = batch_propose;

        // Deserialize the batch header.
        let batch_header = spawn_blocking!(batch_header.deserialize_blocking())?;
        // Ensure the round matches in the batch header.
        if batch_round != batch_header.round() {
            // Proceed to disconnect the validator.
            self.gateway.disconnect(peer_ip);
            bail!("Malicious peer - proposed round {batch_round}, but sent batch for round {}", batch_header.round());
        }

        // Retrieve the batch author.
        let batch_author = batch_header.author();

        // Ensure the batch proposal is from the validator.
        match self.gateway.resolve_to_aleo_addr(peer_ip) {
            // If the peer is a validator, then ensure the batch proposal is from the validator.
            Some(address) => {
                if address != batch_author {
                    // Proceed to disconnect the validator.
                    self.gateway.disconnect(peer_ip);
                    bail!("Malicious peer - proposed batch from a different validator ({batch_author})");
                }
            }
            None => bail!("Batch proposal from a disconnected validator"),
        }
        // Ensure the batch author is a current committee member.
        if !self.gateway.is_authorized_validator_address(batch_author) {
            // Proceed to disconnect the validator.
            self.gateway.disconnect(peer_ip);
            bail!("Malicious peer - proposed batch from a non-committee member ({batch_author})");
        }
        // Ensure the batch proposal is not from the current primary.
        if self.gateway.account().address() == batch_author {
            bail!("Invalid peer - proposed batch from myself ({batch_author})");
        }

        // Ensure that the batch proposal's committee ID matches the expected committee ID.
        // This may happen when the network forks. A transaction referencing a
        // state root from one side of the fork will be aborted on the other
        // side of the fork. This leads to a different view of stake and
        // therefore a different view of the committee ID.
        let expected_committee_id = self.ledger.get_committee_lookback_for_round(batch_round)?.id();
        if expected_committee_id != batch_header.committee_id() {
            // Proceed to disconnect the validator.
            self.gateway.disconnect(peer_ip);
            bail!(
                "Malicious peer - proposed batch has a different committee ID ({expected_committee_id} != {})",
                batch_header.committee_id()
            );
        }

        // Retrieve the cached round and batch ID for this validator.
        if let Some((signed_round, signed_batch_id, signature)) =
            self.signed_proposals.read().get(&batch_author).copied()
        {
            // If the signed round is ahead of the peer's batch round, do not sign the proposal.
            // Note: while this may be valid behavior, additional formal analysis and testing will need to be done before allowing it.
            if signed_round > batch_header.round() {
                bail!(
                    "Peer ({batch_author}) proposed a batch for a previous round ({}), latest signed round: {signed_round}",
                    batch_header.round()
                );
            }

            // If the round matches and the batch ID differs, then the validator is malicious.
            if signed_round == batch_header.round() && signed_batch_id != batch_header.batch_id() {
                bail!("Peer ({batch_author}) proposed another batch for the same round ({signed_round})");
            }
            // If the round and batch ID matches, then skip signing the batch a second time.
            // Instead, rebroadcast the cached signature to the peer.
            if signed_round == batch_header.round() && signed_batch_id == batch_header.batch_id() {
                let gateway = self.gateway.clone();
                self.spawn(async move {
                    debug!("Resending a signature for a batch in round {batch_round} from '{peer_ip}'");
                    let event = Event::BatchSignature(BatchSignature::new(batch_header.batch_id(), signature));
                    // Resend the batch signature to the peer.
                    if gateway.send(peer_ip, event).await.is_none() {
                        warn!("Failed to resend a signature for a batch in round {batch_round} to '{peer_ip}'");
                    }
                });
                // Return early.
                return Ok(());
            }
        }

        // Ensure that the batch header doesn't already exist in storage.
        // Note this is already checked in `check_batch_header`, however we can return early here without creating a blocking task.
        if self.storage.contains_batch(batch_header.batch_id()) {
            debug!(
                "Primary is safely skipping a batch proposal from '{peer_ip}' - {}",
                format!("batch for round {batch_round} already exists in storage").dimmed()
            );
            return Ok(());
        }

        // Compute the previous round.
        let previous_round = batch_round.saturating_sub(1);
        // Ensure that the peer did not propose a batch too quickly.
        if let Err(err) = self.check_peer_proposal_timestamp(previous_round, batch_author, batch_header.timestamp()) {
            // Proceed to disconnect the validator.
            self.gateway.disconnect(peer_ip);
            return Err(err.context(format!("Malicious behavior of peer '{peer_ip}'")));
        }

        // Ensure the batch header does not contain any ratifications.
        if batch_header.contains(TransmissionID::Ratification) {
            // Proceed to disconnect the validator.
            self.gateway.disconnect(peer_ip);
            bail!(
                "Malicious peer - proposed batch contains an unsupported ratification transmissionID from '{peer_ip}'",
            );
        }

        // If the peer is ahead, use the batch header to sync up to the peer.
        let mut missing_transmissions = self.sync_with_batch_header_from_peer::<true>(peer_ip, &batch_header).await?;

        // Check that the transmission ids match and are not fee transactions.
        if let Err(err) = cfg_iter_mut!(&mut missing_transmissions).try_for_each(|(transmission_id, transmission)| {
            // If the transmission is not well-formed, then return early.
            self.ledger.ensure_transmission_is_well_formed(*transmission_id, transmission)
        }) {
            let err = err.context(format!(
                "Batch propose at round {batch_round} from '{peer_ip}' contains an invalid transmission"
            ));
            debug!("{}", flatten_error(err));
            return Ok(());
        }

        // Ensure the batch is for the current round.
        // This method must be called after fetching previous certificates (above),
        // and prior to checking the batch header (below).
        if let Err(e) = self.ensure_is_signing_round(batch_round) {
            // If the primary is not signing for the peer's round, then return early.
            debug!("{e} from '{peer_ip}'");
            return Ok(());
        }

        // Ensure the batch header from the peer is valid.
        let (storage, header) = (self.storage.clone(), batch_header.clone());

        // Check the batch header, and return early if it already exists in storage.
        let Some(missing_transmissions) =
            spawn_blocking!(storage.check_batch_header(&header, missing_transmissions, Default::default()))?
        else {
            return Ok(());
        };

        // Inserts the missing transmissions into the workers.
        self.insert_missing_transmissions_into_workers(peer_ip, missing_transmissions.into_iter())?;

        /* Proceeding to sign the batch. */

        // Retrieve the batch ID.
        let batch_id = batch_header.batch_id();
        // Sign the batch ID.
        let account = self.gateway.account().clone();
        let signature = spawn_blocking!(account.sign(&[batch_id], &mut rand::rng()))?;

        // Ensure the proposal has not already been signed.
        //
        // Note: Due to the need to sync the batch header with the peer, it is possible
        // for the primary to receive the same 'BatchPropose' event again, whereby only
        // one instance of this handler should sign the batch. This check guarantees this.
        if !self.cache_signed_proposal(batch_author, batch_round, batch_id, signature) {
            return Ok(());
        }

        // Broadcast the signature back to the validator.
        let self_ = self.clone();
        self.spawn(async move {
            let event = Event::BatchSignature(BatchSignature::new(batch_id, signature));
            // Send the batch signature to the peer.
            if self_.gateway.send(peer_ip, event).await.is_some() {
                debug!("Signed a batch for round {batch_round} from '{peer_ip}'");
            }
        });

        Ok(())
    }

    /// Records that this primary signed `batch_id` at `batch_round`, on behalf of `batch_author`,
    /// returning whether the proposal may be signed at all.
    ///
    /// For correctness we must endorse at most one batch per author per round, and the cached round
    /// is the only record of that. So the cached round must never move backwards: if it did, for
    /// example due to a race, we would forget having signed the newer round and could go on to
    /// endorse a second, conflicting batch for it. Two signatures over different batch IDs in one
    /// round are enough to build conflicting certificates, and so to fork the DAG.
    ///
    /// This runs under a single write lock, and either atomically caches the proposal and returns
    /// `true`, or leaves the cache untouched and returns `false` to say the proposal must not be
    /// signed.
    ///
    /// `false` is also returned when the cached round already equals `batch_round`, even if the
    /// cached batch ID matches: exactly one signature is produced per author per round, and it stays
    /// in the cache, so a repeat of a proposal we already signed is answered from the cache rather
    /// than signed afresh.
    fn cache_signed_proposal(
        &self,
        batch_author: Address<N>,
        batch_round: u64,
        batch_id: Field<N>,
        signature: Signature<N>,
    ) -> bool {
        match self.signed_proposals.write().0.entry(batch_author) {
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                // Only ever advance the cached round.
                if entry.get().0 >= batch_round {
                    return false;
                }
                entry.insert((batch_round, batch_id, signature));
                true
            }
            // If the validator has not signed a batch before, then continue.
            std::collections::hash_map::Entry::Vacant(entry) => {
                // Cache the round, batch ID, and signature for this validator.
                entry.insert((batch_round, batch_id, signature));
                true
            }
        }
    }

    /// Attempts to add a peer's `signature` for `batch_id` to the current proposal.
    ///
    /// Consumes `state` and always returns the (possibly updated) state alongside a result so that
    /// the caller can restore it unconditionally, keeping `proposed_batch` consistent even on the
    /// error path.
    ///
    /// # Returns
    /// * `(Ok(Some(proposal)), Certified(id))` — quorum reached; caller should certify.
    /// * `(Ok(None), <restored state>)` — signature accepted or silently dropped; nothing to do.
    /// * `(Err(e), <restored state>)` — signature rejected; caller should propagate the error.
    fn add_signature_to_batch(
        &self,
        state: ProposedBatchState<N>,
        peer_ip: SocketAddr,
        batch_id: Field<N>,
        signature: Signature<N>,
    ) -> (Result<Option<Proposal<N>>>, ProposedBatchState<N>) {
        match state {
            ProposedBatchState::Certifying(mut proposal) if proposal.batch_id() == batch_id => {
                // This signature is for our currently active proposal.
                // Use an inner closure to keep `?` ergonomics while returning a tuple.
                let inner: Result<bool> = (|| {
                    let committee_lookback = self.ledger.get_committee_lookback_for_round(proposal.round())?;
                    let Some(signer) = self.gateway.resolve_to_aleo_addr(peer_ip) else {
                        bail!("Signature is from a disconnected validator");
                    };
                    let new_signature = proposal.add_signature(signer, signature, &committee_lookback)?;
                    if new_signature {
                        info!("Received a batch signature for round {} from '{peer_ip}'", proposal.round());
                        Ok(proposal.is_quorum_threshold_reached(&committee_lookback))
                    } else {
                        debug!(
                            "Received duplicated signature from '{peer_ip}' for batch \
                                {batch_id} in round {round}",
                            round = proposal.round()
                        );
                        Ok(false)
                    }
                })();
                match inner {
                    Ok(true) => {
                        let certified_id = proposal.batch_id();
                        (Ok(Some(*proposal)), ProposedBatchState::Certified(certified_id))
                    }
                    Ok(false) => (Ok(None), ProposedBatchState::Certifying(proposal)),
                    Err(e) => (Err(e), ProposedBatchState::Certifying(proposal)),
                }
            }
            ProposedBatchState::Certifying(proposal) => {
                // Certifying a different proposal — check if batch_id is already in storage.
                if self.storage.contains_batch(batch_id) {
                    debug!(
                        "Primary is safely skipping a batch signature from {peer_ip} for \
                            round {} - batch is already certified",
                        proposal.round()
                    );
                    (Ok(None), ProposedBatchState::Certifying(proposal))
                } else {
                    let expected_id = proposal.batch_id();
                    let round = proposal.round();
                    (
                        Err(anyhow!("Unknown batch ID '{batch_id}', expected '{expected_id}' for round {round}")),
                        ProposedBatchState::Certifying(proposal),
                    )
                }
            }
            ProposedBatchState::Certified(id) if id == batch_id => {
                // Quorum already reached; late-arriving signature is harmless.
                debug!(
                    "Skipping batch signature from {peer_ip} for batch '{batch_id}' - \
                        already received sufficient signatures"
                );
                (Ok(None), ProposedBatchState::Certified(id))
            }
            ProposedBatchState::Certified(id) => {
                let result = if self.storage.contains_batch(batch_id) {
                    // This is most likely not malicious, but could indicate connectivity issues.
                    warn!("Received signature for an older batch {batch_id}");
                    Ok(None)
                } else {
                    Err(anyhow!("Unknown batch ID '{batch_id}'"))
                };

                (result, ProposedBatchState::Certified(id))
            }
            ProposedBatchState::None => {
                let result = if self.storage.contains_batch(batch_id) {
                    // This is most likely not malicious, but could indicate connectivity issues.
                    warn!("Received signature for an older batch {batch_id}");
                    Ok(None)
                } else {
                    Err(anyhow!("Unknown batch ID '{batch_id}'"))
                };

                (result, ProposedBatchState::None)
            }
        }
    }

    /// Processes a batch signature from a peer.
    ///
    /// This method performs the following steps:
    /// 1. Ensure the proposed batch has not expired.
    /// 2. Verify the signature, ensuring it corresponds to the proposed batch.
    /// 3. Store the signature.
    /// 4. Certify the batch if enough signatures have been received.
    /// 5. Broadcast the batch certificate to all validators.
    async fn process_batch_signature_from_peer(
        &self,
        peer_ip: SocketAddr,
        batch_signature: BatchSignature<N>,
    ) -> Result<()> {
        // Ensure the proposed batch has not expired, and clear the proposed batch if it has expired.
        self.check_proposed_batch_for_expiration()?;

        // Retrieve the signature and timestamp.
        let BatchSignature { batch_id, signature } = batch_signature;

        // Retrieve the signer.
        let signer = signature.to_address();

        // Ensure the batch signature is signed by the validator.
        match self.gateway.resolve_to_aleo_addr(peer_ip) {
            // If the peer is a validator, then ensure the batch signature is from the validator.
            Some(address) => {
                if address != signer {
                    // Proceed to disconnect the validator.
                    self.gateway.disconnect(peer_ip);
                    bail!("Malicious peer - batch signature is from a different validator ({signer})");
                }
            }
            None => bail!("Batch signature from a disconnected validator"),
        }
        // Ensure the batch signature is not from the current primary.
        if self.gateway.account().address() == signer {
            bail!("Invalid peer - received a batch signature from myself ({signer})");
        }

        let self_ = self.clone();
        let Some(proposal) = spawn_blocking!({
            // Acquire the write lock.
            let mut proposed_batch = self_.proposed_batch.write();

            let (result, new_state) =
                self_.add_signature_to_batch(std::mem::take(&mut *proposed_batch), peer_ip, batch_id, signature);
            *proposed_batch = new_state;
            result
        })?
        else {
            return Ok(());
        };

        /* Proceeding to certify the batch. */

        info!("Quorum threshold reached - Preparing to certify our batch for round {}...", proposal.round());

        // Retrieve the committee lookback for the round.
        let committee_lookback = self.ledger.get_committee_lookback_for_round(proposal.round())?;
        // Store the certified batch and broadcast it to all validators.
        // If there was an error storing the certificate, reinsert the transmissions back into the ready queue.
        if let Err(e) = self.store_and_broadcast_certificate(&proposal, &committee_lookback).await {
            // Reinsert the transmissions back into the ready queue for the next proposal.
            self.reinsert_transmissions_into_workers(proposal.into_transmissions())?;
            return Err(e);
        }

        #[cfg(feature = "metrics")]
        metrics::increment_gauge(metrics::bft::CERTIFIED_BATCHES, 1.0);
        Ok(())
    }

    /// Processes a batch certificate from a peer.
    ///
    /// This method performs the following steps:
    /// 1. Stores the given batch certificate, after ensuring it is valid.
    /// 2. If there are enough certificates to reach quorum threshold for the current round,
    ///    then proceed to advance to the next round.
    async fn process_batch_certificate_from_peer(
        &self,
        peer_ip: SocketAddr,
        certificate: BatchCertificate<N>,
    ) -> Result<()> {
        // Ensure the batch certificate is from an authorized validator.
        if !self.gateway.is_authorized_validator_ip(peer_ip) {
            // Proceed to disconnect the validator.
            self.gateway.disconnect(peer_ip);
            bail!("Malicious peer - Received a batch certificate from an unauthorized validator IP ({peer_ip})");
        }
        // Ensure storage does not already contain the certificate.
        if self.storage.contains_certificate(certificate.id()) {
            return Ok(());
        // Otherwise, ensure ephemeral storage contains the certificate.
        } else if !self.storage.contains_unprocessed_certificate(certificate.id()) {
            self.storage.insert_unprocessed_certificate(certificate.clone())?;
        }

        // Retrieve the batch certificate author.
        let author = certificate.author();
        // Retrieve the batch certificate round.
        let certificate_round = certificate.round();
        // Retrieve the batch certificate committee ID.
        let committee_id = certificate.committee_id();

        // Ensure the batch certificate is not from the current primary.
        if self.gateway.account().address() == author {
            bail!("Received a batch certificate for myself ({author})");
        }

        // Ensure that the incoming certificate is valid.
        self.storage.check_incoming_certificate(&certificate)?;

        // Store the certificate, after ensuring it is valid above.
        // The following call recursively fetches and stores
        // the previous certificates referenced from this certificate.
        // It is critical to make the following call this after validating the certificate above.
        // The reason is that a sequence of malformed certificates,
        // with references to previous certificates with non-decreasing rounds,
        // cause the recursive fetching of certificates to crash the validator due to resource exhaustion.
        // Note that if the following call, if not returning an error, guarantees the backward closure of the DAG
        // (i.e. that all the referenced previous certificates are in the DAG before storing this one),
        // then all the validity checks in [`Storage::check_certificate`] should be redundant.
        // TODO: eliminate those redundant checks
        self.sync_with_certificate_from_peer(peer_ip, certificate).await?;

        // If there are enough certificates to reach quorum threshold for the certificate round,
        // then proceed to advance to the next round.

        // Retrieve the committee lookback.
        let committee_lookback = self.ledger.get_committee_lookback_for_round(certificate_round)?;

        // Retrieve the certificate authors.
        let authors = self.storage.get_certificate_authors_for_round(certificate_round);
        // Check if the certificates have reached the quorum threshold.
        let is_quorum = committee_lookback.is_quorum_threshold_reached(&authors);

        // Ensure that the batch certificate's committee ID matches the expected committee ID.
        let expected_committee_id = committee_lookback.id();
        if expected_committee_id != committee_id {
            // Proceed to disconnect the validator.
            self.gateway.disconnect(peer_ip);
            bail!("Batch certificate has a different committee ID ({expected_committee_id} != {committee_id})");
        }

        // Determine if we are currently proposing a round that is relevant.
        // Note: This is important, because while our peers have advanced,
        // they may not be proposing yet, and thus still able to sign our proposed batch.
        let should_advance = match &*self.latest_proposal_timestamp.read().await {
            // We advance if the proposal round is less than the current round that was just certified.
            Some((latest_round, _)) => *latest_round < certificate_round,
            // If there's no proposal, we consider advancing.
            None => true,
        };

        // Retrieve the current round.
        let current_round = self.current_round();

        // Determine whether to advance to the next round.
        if is_quorum && should_advance && certificate_round >= current_round {
            // If we have reached the quorum threshold and the round should advance, then proceed to the next round.
            self.round_increment_notify.notify_one();
        }
        Ok(())
    }
}

impl<N: Network> Primary<N> {
    /// Starts the primary handlers.
    ///
    /// For each receiver in the `primary_receiver` struct, there will be a dedicated task
    /// that awaits new data and handles it accordingly.
    /// Additionally, this spawns a task that periodically issues PrimaryPings and one that
    /// tries to move to the next round when triggered (e.g. after a certificate is stored) or on a timeout.
    ///
    /// This function is called exactly once, in `Self::run()`.
    fn start_handlers(&self, primary_receiver: PrimaryReceiver<N>) {
        let PrimaryReceiver {
            mut rx_batch_propose,
            mut rx_batch_signature,
            mut rx_batch_certified,
            mut rx_primary_ping,
            mut rx_unconfirmed_solution,
            mut rx_unconfirmed_transaction,
        } = primary_receiver;

        // Start the primary ping sender.
        let self_ = self.clone();
        self.spawn(async move {
            loop {
                // Sleep briefly.
                tokio::time::sleep(PRIMARY_PING_INTERVAL).await;

                // Retrieve the block locators.
                let self__ = self_.clone();
                let block_locators = match spawn_blocking!(self__.sync.get_block_locators()) {
                    Ok(block_locators) => block_locators,
                    Err(e) => {
                        warn!("Failed to retrieve block locators - {e}");
                        continue;
                    }
                };

                // Retrieve the latest certificate of the primary.
                let primary_certificate = {
                    // Retrieve the primary address.
                    let primary_address = self_.gateway.account().address();

                    // Iterate backwards from the latest round to find the primary certificate.
                    let mut certificate = None;
                    let mut current_round = self_.current_round();
                    while certificate.is_none() {
                        // If the current round is 0, then break the while loop.
                        if current_round == 0 {
                            break;
                        }
                        // Retrieve the primary certificates.
                        if let Some(primary_certificate) =
                            self_.storage.get_certificate_for_round_with_author(current_round, primary_address)
                        {
                            certificate = Some(primary_certificate);
                        // If the primary certificate was not found, decrement the round.
                        } else {
                            current_round = current_round.saturating_sub(1);
                        }
                    }

                    // Determine if the primary certificate was found.
                    match certificate {
                        Some(certificate) => certificate,
                        // Skip this iteration of the loop (do not send a primary ping).
                        None => continue,
                    }
                };

                // Construct the primary ping.
                let primary_ping = PrimaryPing::from((<Event<N>>::VERSION, block_locators, primary_certificate));
                // Broadcast the event.
                self_.gateway.broadcast(Event::PrimaryPing(primary_ping));
            }
        });

        // Start the primary ping handler.
        let self_ = self.clone();
        self.spawn(async move {
            while let Some((peer_ip, primary_certificate)) = rx_primary_ping.recv().await {
                trace!("Processing new primary ping from '{peer_ip}'");

                // Spawn a task to process the primary certificate.
                {
                    let self__ = self_.clone();
                    self_.spawn(async move {
                        // Deserialize the primary certificate in the primary ping.
                        let Ok(primary_certificate) = spawn_blocking!(primary_certificate.deserialize_blocking())
                        else {
                            warn!("Failed to deserialize primary certificate in 'PrimaryPing' from '{peer_ip}'");
                            return;
                        };
                        // Process the primary certificate.
                        let id = fmt_id(primary_certificate.id());
                        let round = primary_certificate.round();
                        if let Err(e) = self__.process_batch_certificate_from_peer(peer_ip, primary_certificate).await {
                            debug!("Cannot process a primary certificate '{id}' at round {round} in a 'PrimaryPing' from '{peer_ip}' - {e}");
                        }
                    });
                }
            }
        });

        // Start the worker ping(s).
        let self_ = self.clone();
        self.spawn(async move {
            loop {
                tokio::time::sleep(WORKER_PING_INTERVAL).await;

                // Broadcast the worker ping(s).
                for worker in self_.workers() {
                    worker.broadcast_ping();
                }
            }
        });

        // Start the batch proposal task.
        let proposal_task = self.proposal_task.clone();
        let self_ = self.clone();
        self.spawn(async move { proposal_task.run(self_).await });

        // Start the proposed batch handler.
        let self_ = self.clone();
        self.spawn(async move {
            while let Some((peer_ip, batch_propose)) = rx_batch_propose.recv().await {
                // Spawn a task to process the proposed batch.
                let self__ = self_.clone();
                self_.spawn(async move {
                    // Process the batch proposal.
                    let round = batch_propose.round;
                    if let Err(err) = self__.process_batch_propose_from_peer(peer_ip, batch_propose).await {
                        let err = err.context(format!("Cannot sign a batch at round {round} from '{peer_ip}'"));
                        warn!("{}", flatten_error(err));
                    }
                });
            }
        });

        // Start the batch signature handler.
        let self_ = self.clone();
        self.spawn(async move {
            while let Some((peer_ip, batch_signature)) = rx_batch_signature.recv().await {
                // Process the batch signature.
                // Note: Do NOT spawn a task around this function call. Processing signatures from peers
                // is a critical path, and we should only store the minimum required number of signatures.
                // In addition, spawning a task can cause concurrent processing of signatures (even with a lock),
                // which means the RwLock for the proposed batch must become a 'tokio::sync' to be safe.
                let id = fmt_id(batch_signature.batch_id);
                if let Err(err) = self_.process_batch_signature_from_peer(peer_ip, batch_signature).await {
                    let err = err.context(format!("Cannot store a signature for batch '{id}' from '{peer_ip}'"));
                    warn!("{}", flatten_error(err));
                }
            }
        });

        // Start the certified batch handler.
        let self_ = self.clone();
        self.spawn(async move {
            while let Some((peer_ip, batch_certificate)) = rx_batch_certified.recv().await {
                // Spawn a task to process the batch certificate.
                let self__ = self_.clone();
                self_.spawn(async move {
                    // Deserialize the batch certificate.
                    let Ok(batch_certificate) = spawn_blocking!(batch_certificate.deserialize_blocking()) else {
                        warn!("Failed to deserialize the batch certificate from '{peer_ip}'");
                        return;
                    };
                    // Process the batch certificate.
                    let id = fmt_id(batch_certificate.id());
                    let round = batch_certificate.round();
                    if let Err(err) = self__.process_batch_certificate_from_peer(peer_ip, batch_certificate).await {
                        warn!(
                            "{}",
                            flatten_error(err.context(format!(
                                "Cannot store a certificate '{id}' for round {round} from '{peer_ip}'"
                            )))
                        );
                    }
                });
            }
        });

        // This task tries to move to the next round when triggered (e.g. after a certificate is stored)
        // or after a timeout, so we are not stuck on a previous round despite having quorum.
        let self_ = self.clone();
        self.spawn(async move {
            loop {
                let round_start = Instant::now();
                let current_round = self_.current_round();

                // Inner loop: wait and try to increment while we're still in the same round.
                while self_.current_round() == current_round {
                    let mut futures: Vec<Pin<Box<dyn Future<Output = ()> + Send>>> =
                        vec![Box::pin(self_.round_increment_notify.notified())];

                    if let Some(remaining_delay) = MAX_BATCH_DELAY.checked_sub(round_start.elapsed())
                        && !remaining_delay.is_zero()
                    {
                        futures.push(Box::pin(tokio::time::sleep(remaining_delay)));
                    }
                    // Always ensure a wakeup no later than MAX_LEADER_CERTIFICATE_DELAY so that
                    // try_advance_to_next_round is called after the leader-certificate timer
                    // expires, even when no further certificates arrive (e.g. an even round where
                    // the elected leader was absent and quorum was reached without their cert).
                    futures.push(Box::pin(tokio::time::sleep(MAX_LEADER_CERTIFICATE_DELAY)));
                    let _ = futures::future::select_all(futures).await;

                    let next_round = current_round.saturating_add(1);
                    let is_quorum_threshold_reached = {
                        let authors = self_.storage.get_certificate_authors_for_round(current_round);
                        if authors.is_empty() {
                            continue;
                        }
                        let Ok(committee_lookback) = self_.ledger.get_committee_lookback_for_round(current_round)
                        else {
                            warn!("Failed to retrieve the committee lookback for round {current_round}");
                            continue;
                        };
                        committee_lookback.is_quorum_threshold_reached(&authors)
                    };

                    if is_quorum_threshold_reached {
                        debug!("Quorum threshold reached for round {current_round}");
                        if let Err(err) = self_.try_increment_to_the_next_round(next_round).await {
                            warn!("{}", flatten_error(err.context("Failed to increment to the next round")));
                        }
                    }
                }
            }
        });

        // Start a handler to process new unconfirmed solutions.
        let self_ = self.clone();
        self.spawn(async move {
            while let Some((solution_id, solution, callback)) = rx_unconfirmed_solution.recv().await {
                // Compute the checksum for the solution.
                let Ok(checksum) = solution.to_checksum::<N>() else {
                    error!("Failed to compute the checksum for the unconfirmed solution");
                    continue;
                };
                // Compute the worker ID.
                let Ok(worker_id) = assign_to_worker((solution_id, checksum), self_.num_workers()) else {
                    error!("Unable to determine the worker ID for the unconfirmed solution");
                    continue;
                };
                let self__ = self_.clone();
                self_.spawn(async move {
                    // Retrieve the worker.
                    let worker = &self__.workers()[worker_id as usize];
                    // Process the unconfirmed solution.
                    let result = worker.process_unconfirmed_solution(solution_id, solution).await;
                    // Send the result to the callback.
                    callback.send(result).ok();
                });
            }
        });

        // Start a handler to process new unconfirmed transactions.
        let self_ = self.clone();
        self.spawn(async move {
            while let Some((transaction_id, transaction, callback)) = rx_unconfirmed_transaction.recv().await {
                trace!("Primary - Received an unconfirmed transaction '{}'", fmt_id(transaction_id));
                // Compute the checksum for the transaction.
                let Ok(checksum) = transaction.to_checksum::<N>() else {
                    error!("Failed to compute the checksum for the unconfirmed transaction");
                    continue;
                };
                // Compute the worker ID.
                let Ok(worker_id) = assign_to_worker::<N>((&transaction_id, &checksum), self_.num_workers()) else {
                    error!("Unable to determine the worker ID for the unconfirmed transaction");
                    continue;
                };
                let self__ = self_.clone();
                self_.spawn(async move {
                    // Retrieve the worker.
                    let worker = &self__.workers().get(worker_id as usize).expect("Invalid worker ID");
                    // Process the unconfirmed transaction.
                    let result = worker.process_unconfirmed_transaction(transaction_id, transaction).await;
                    // Send the result to the callback.
                    callback.send(result).ok();
                });
            }
        });
    }

    /// Checks if the proposed batch is expired, and clears the proposed batch if it has expired.
    ///
    /// The proposal is inspected and cleared under a single write lock, so that the proposal which
    /// is discarded is exactly the one that was found to be expired.
    fn check_proposed_batch_for_expiration(&self) -> Result<()> {
        // Read the current round before taking the lock below, so that no other lock is acquired
        // while it is held. The round only ever increases, so reading it early is conservative: at
        // worst a proposal that just expired is left for the next call to clear.
        let current_round = self.current_round();

        // Take the proposed batch only if it has expired; taking it leaves the state cleared.
        // A batch being certified is not considered expired.
        let expired = {
            let mut proposed_batch = self.proposed_batch.write();
            let is_expired = matches!(
                &*proposed_batch,
                ProposedBatchState::Certifying(proposal) if proposal.round() < current_round
            );
            is_expired.then(|| std::mem::take(&mut *proposed_batch))
        };

        // Reinsert the transmissions once the lock above has been released, since this acquires the
        // workers' locks in turn.
        if let Some(ProposedBatchState::Certifying(proposal)) = expired {
            debug!("Cleared expired proposal for round {}", proposal.round());
            self.reinsert_transmissions_into_workers(proposal.into_transmissions())?;
        }
        Ok(())
    }

    /// Increments to the next round.
    async fn try_increment_to_the_next_round(&self, next_round: u64) -> Result<()> {
        // If the next round is within GC range, then iterate to the penultimate round.
        if self.current_round() + self.storage.max_gc_rounds() >= next_round {
            let mut fast_forward_round = self.current_round();
            // Iterate until the penultimate round is reached.
            while fast_forward_round < next_round.saturating_sub(1) {
                // Update to the next round in storage.
                fast_forward_round = self.storage.increment_to_next_round(fast_forward_round)?;
                // Clear the proposed batch.
                *self.proposed_batch.write() = ProposedBatchState::None;
            }
        }

        // Retrieve the current round.
        let current_round = self.current_round();
        // Attempt to advance to the next round.
        if current_round < next_round {
            // If a BFT sender was provided, send the current round to the BFT.
            let is_ready = if let Some(cb) = self.primary_callback.get() {
                cb.try_advance_to_next_round(current_round)
            }
            // Otherwise, handle the Narwhal case.
            else {
                // Update to the next round in storage.
                self.storage.increment_to_next_round(current_round)?;
                // Set 'is_ready' to 'true'.
                true
            };

            // Notify the proposal task if the new round is ready.
            if is_ready {
                debug!("Primary is ready to propose the next round");
                self.proposal_task.signal();
            } else {
                debug!("Primary is not ready to propose the next round");
            }
        }
        Ok(())
    }

    /// Ensures the primary is signing for the specified batch round.
    /// This method is used to ensure: for a given round, as soon as the primary starts proposing,
    /// it will no longer sign for the previous round (as it has enough previous certificates to proceed).
    fn ensure_is_signing_round(&self, batch_round: u64) -> Result<()> {
        // Retrieve the current round.
        let current_round = self.current_round();
        // Ensure the batch round is within GC range of the current round.
        if current_round + self.storage.max_gc_rounds() <= batch_round {
            bail!("Round {batch_round} is too far in the future")
        }
        // Ensure the batch round is at or one before the current round.
        // Intuition: Our primary has moved on to the next round, but has not necessarily started proposing,
        // so we can still sign for the previous round. If we have started proposing, the next check will fail.
        if current_round > batch_round + 1 {
            bail!("Primary is on round {current_round}, and no longer signing for round {batch_round}")
        }
        // Check if the primary is still signing for the batch round.
        if let ProposedBatchState::Certifying(proposal) = &*self.proposed_batch.read()
            && proposal.round() > batch_round
        {
            bail!("Our primary at round {} is no longer signing for round {batch_round}", proposal.round())
        }
        Ok(())
    }

    /// Ensure the primary is not creating batch proposals too frequently.
    /// This checks that the certificate timestamp for the previous round is within the expected range.
    fn check_peer_proposal_timestamp(&self, previous_round: u64, author: Address<N>, timestamp: i64) -> Result<()> {
        ensure!(author != self.gateway.account().address(), "Peer cannot propose a batch that is authored by myself");

        // Retrieve the timestamp of the previous timestamp to check against.
        let previous_timestamp = match self.storage.get_certificate_for_round_with_author(previous_round, author) {
            // Ensure that the previous certificate was created at least `MIN_BATCH_DELAY` seconds ago.
            Some(certificate) => certificate.timestamp(),
            // If we do not see a previous certificate for the author, then proceed optimistically.
            None => return Ok(()),
        };

        // Determine the elapsed time since the previous timestamp.
        let elapsed = timestamp
            .checked_sub(previous_timestamp)
            .ok_or_else(|| anyhow!("Timestamp cannot be before the previous certificate at round {previous_round}"))?;
        // Ensure that the previous certificate was created at least `MIN_BATCH_DELAY` seconds ago.
        match elapsed < MIN_BATCH_DELAY.as_secs() as i64 {
            true => bail!("Timestamp is too soon after the previous certificate at round {previous_round}"),
            false => Ok(()),
        }
    }

    /// Ensure the primary is not creating batch proposals too frequently.
    /// This checks that the certificate timestamp for the previous round is within the expected range.
    ///
    /// # Returns
    /// - `Ok(true)` if the timestamp allows a new proposal.
    /// - `Ok(false)` if the timestamp is valid but too soon after the previous proposal.
    /// - `Err(err)` if an unexpected error occured, such as the timestamp being before the previous certificate.
    fn check_own_proposal_timestamp(
        &self,
        previous_round: u64,
        previous_timestamp: i64,
        timestamp: i64,
    ) -> Result<bool> {
        // Determine the elapsed time since the previous timestamp.
        let elapsed = timestamp
            .checked_sub(previous_timestamp)
            .ok_or_else(|| anyhow!("Timestamp cannot be before the previous certificate at round {previous_round}"))?;

        Ok(elapsed >= MIN_BATCH_DELAY.as_secs() as i64)
    }

    /// Stores the certified batch and broadcasts it to all validators, returning the certificate.
    async fn store_and_broadcast_certificate(&self, proposal: &Proposal<N>, committee: &Committee<N>) -> Result<()> {
        // Create the batch certificate and transmissions.
        let (certificate, transmissions) = tokio::task::block_in_place(|| proposal.to_certificate(committee))?;

        // Convert the transmissions into a HashMap.
        // Note: Do not change the `Proposal` to use a HashMap. The ordering there is necessary for safety.
        let transmissions = transmissions.into_iter().collect::<HashMap<_, _>>();

        // Store some metadata about the certified batch.
        let round = certificate.round();
        let num_transmissions = certificate.transmission_ids().len();

        // Store the certified batch.
        let (storage, certificate_) = (self.storage.clone(), certificate.clone());
        tokio::task::spawn_blocking(move || {
            storage.insert_certificate(certificate_, transmissions, Default::default())
        })
        .await??;
        debug!("Stored a batch certificate for round {}", certificate.round());
        // The batch is now in storage, so late-arriving signatures can find it via contains_batch.
        // Transition from Certified back to None.
        *self.proposed_batch.write() = ProposedBatchState::None;

        // Broadcast the certified batch to all validators.
        self.gateway.broadcast(Event::BatchCertified(certificate.clone().into()));

        // If a BFT sender was provided, send the certificate to the BFT.
        if let Some(cb) = self.primary_callback.get() {
            // Await the callback to continue.
            cb.add_new_certificate(certificate).await.with_context(|| {
                format!("Failed to insert our newly certified batch for round {round} into the DAG")
            })?;
        }

        // Log the certified batch.
        info!("Our batch with {num_transmissions} transmissions for round {round} was certified!");

        // Record the certification latency (time from batch proposal to certification).
        #[cfg(feature = "metrics")]
        if let Some(start) = self.batch_propose_start.lock().take() {
            metrics::histogram(metrics::bft::BATCH_CERTIFICATION_LATENCY, start.elapsed().as_secs_f64());
        }

        // Wake up the round increment task to re-check quorum.
        self.round_increment_notify.notify_one();

        Ok(())
    }

    /// Inserts the missing transmissions from the proposal into the workers.
    fn insert_missing_transmissions_into_workers(
        &self,
        peer_ip: SocketAddr,
        transmissions: impl Iterator<Item = (TransmissionID<N>, Transmission<N>)>,
    ) -> Result<()> {
        // Insert the transmissions into the workers.
        assign_to_workers(self.workers(), transmissions, |worker, transmission_id, transmission| {
            worker.process_transmission_from_peer(peer_ip, transmission_id, transmission);
        })
    }

    /// Re-inserts the transmissions from the proposal into the workers.
    fn reinsert_transmissions_into_workers(
        &self,
        transmissions: IndexMap<TransmissionID<N>, Transmission<N>>,
    ) -> Result<()> {
        // Re-insert the transmissions into the workers.
        assign_to_workers(self.workers(), transmissions.into_iter(), |worker, transmission_id, transmission| {
            worker.reinsert(transmission_id, transmission);
        })
    }

    /// Recursively stores a given batch certificate, after ensuring:
    ///   - Ensure the round matches the committee round.
    ///   - Ensure the address is a member of the committee.
    ///   - Ensure the timestamp is within range.
    ///   - Ensure we have all of the transmissions.
    ///   - Ensure we have all of the previous certificates.
    ///   - Ensure the previous certificates are for the previous round (i.e. round - 1).
    ///   - Ensure the previous certificates have reached the quorum threshold.
    ///   - Ensure we have not already signed the batch ID.
    #[async_recursion::async_recursion]
    async fn sync_with_certificate_from_peer(
        &self,
        peer_ip: SocketAddr,
        certificate: BatchCertificate<N>,
    ) -> Result<()> {
        // Retrieve the batch header.
        let batch_header = certificate.batch_header();
        // Retrieve the batch round.
        let batch_round = batch_header.round();

        // If the certificate round is outdated, do not store it.
        if batch_round <= self.storage.gc_round() {
            return Ok(());
        }
        // If the certificate already exists in storage, return early.
        if self.storage.contains_certificate(certificate.id()) {
            return Ok(());
        }

        // If the peer is ahead, use the batch header to sync up to the peer.
        let missing_transmissions = self.sync_with_batch_header_from_peer::<false>(peer_ip, batch_header).await?;

        // Store the batch certificate. The same certificate can reach this point from several
        // tasks at once (e.g. a primary ping, a certified batch, and previous-certificate
        // fetches of other batches); the loser of that race gets a benign error here and has
        // nothing left to do, since the winning task already forwards it to the BFT.
        let (storage, certificate_) = (self.storage.clone(), certificate.clone());
        match tokio::task::spawn_blocking(move || {
            storage.insert_certificate(certificate_, missing_transmissions, Default::default())
        })
        .await
        {
            Ok(Ok(_)) => {} // continue
            Ok(Err(err)) if err.is_benign() => {
                trace!("Skipping insertion for certificate '{}' - {err}", fmt_id(certificate.id()));
                return Ok(());
            }
            Ok(Err(err)) => return Err(anyhow!("{err}")),
            Err(err) => return Err(anyhow!("[tokio::spawn_blocking] {err}")),
        }

        debug!("Stored a batch certificate for round {batch_round} from '{peer_ip}'");
        // If a BFT sender was provided, send the round and certificate to the BFT.
        if let Some(cb) = self.primary_callback.get() {
            cb.add_new_certificate(certificate).await.with_context(|| "Failed to update the DAG from sync")?;
        }
        // Wake the round-increment task to re-check quorum.
        self.round_increment_notify.notify_one();

        Ok(())
    }

    /// Recursively syncs using the given batch header.
    async fn sync_with_batch_header_from_peer<const CHECK_PREVIOUS_CERTIFICATES: bool>(
        &self,
        peer_ip: SocketAddr,
        batch_header: &BatchHeader<N>,
    ) -> Result<HashMap<TransmissionID<N>, Transmission<N>>> {
        // Retrieve the batch round.
        let batch_round = batch_header.round();

        // If the certificate round is outdated, do not store it.
        if batch_round <= self.storage.gc_round() {
            bail!("Round {batch_round} is too far in the past")
        }

        // Determine if quorum threshold is reached on the batch round.
        let is_quorum_threshold_reached = {
            let authors = self.storage.get_certificate_authors_for_round(batch_round);
            let committee_lookback = self.ledger.get_committee_lookback_for_round(batch_round)?;
            committee_lookback.is_quorum_threshold_reached(&authors)
        };

        // Check if our primary should move to the next round.
        // Note: Checking that quorum threshold is reached is important for mitigating a race condition,
        // whereby Narwhal requires N-f, however the BFT only requires f+1. Without this check, the primary
        // will advance to the next round assuming f+1, not N-f, which can lead to a network stall.
        let is_behind_schedule = is_quorum_threshold_reached && batch_round > self.current_round();
        // Check if our primary is far behind the peer.
        let is_peer_far_in_future = batch_round > self.current_round() + self.storage.max_gc_rounds();
        // If our primary is far behind the peer, update our committee to the batch round.
        if is_behind_schedule || is_peer_far_in_future {
            // If the batch round is greater than the current committee round, update the committee.
            self.try_increment_to_the_next_round(batch_round)
                .await
                .with_context(|| "Failed to fast forward current round")?;
        }

        // Ensure the primary has all of the transmissions.
        let missing_transmissions_handle = self.fetch_missing_transmissions(peer_ip, batch_header);

        // Ensure the primary has all of the previous certificates.
        let missing_previous_certificates_handle = self.fetch_missing_previous_certificates(peer_ip, batch_header);

        // Wait for the missing transmissions and previous certificates to be fetched.
        let (missing_transmissions, missing_previous_certificates) = tokio::try_join!(
            missing_transmissions_handle,
            missing_previous_certificates_handle,
        ).with_context(|| format!("Failed to fetch missing transmissions and previous certificates for round {batch_round} from '{peer_ip}"))?;

        // Iterate through the missing previous certificates sequentially.
        // This is done sequentially to avoid requesting a large number of certificates from peers all at once.
        // TODO (raychu86): Optimize this by parallelizing requests, but avoiding duplicated requests since certificates are likely shared across batches.
        for batch_certificate in missing_previous_certificates {
            // Check if the missing previous certificate is valid. This is only
            // needed if we are processing an incoming batch header from a peer.
            // For incoming certificates, validity is assured by checking the
            // root certificate in `process_batch_certificate_from_peer`.
            if CHECK_PREVIOUS_CERTIFICATES {
                self.storage.check_incoming_certificate(&batch_certificate)?;
            }
            // Store the batch certificate (recursively fetching any missing previous certificates).
            self.sync_with_certificate_from_peer(peer_ip, batch_certificate).await?;
        }
        Ok(missing_transmissions)
    }

    /// Fetches any missing transmissions for the specified batch header.
    /// If a transmission does not exist, it will be fetched from the specified peer IP.
    async fn fetch_missing_transmissions(
        &self,
        peer_ip: SocketAddr,
        batch_header: &BatchHeader<N>,
    ) -> Result<HashMap<TransmissionID<N>, Transmission<N>>> {
        // If the round is <= the GC round, return early.
        if batch_header.round() <= self.storage.gc_round() {
            return Ok(Default::default());
        }

        // Ensure this batch ID is new, otherwise return early.
        if self.storage.contains_batch(batch_header.batch_id()) {
            trace!("Batch for round {} from peer has already been processed", batch_header.round());
            return Ok(Default::default());
        }

        // Retrieve the workers.
        let workers = self.workers.clone();

        // Initialize a list for the transmissions.
        let mut fetch_transmissions = FuturesUnordered::new();

        // Retrieve the number of workers.
        let num_workers = self.num_workers();
        // Iterate through the transmission IDs.
        for transmission_id in batch_header.transmission_ids() {
            // If the transmission cannot be retrieved from storage, proceed to fetch it.
            //
            // Note that an ID storage knows only as aborted is deliberately fetched too: storage
            // holds no payload for it, so treating the aborted marker as "already have it" would
            // accept a certificate committing to a transmission this node can never materialize.
            // A batch is an availability claim by its author, so ask for the bytes rather than
            // excuse them; the request goes to that author, which is the peer claiming to hold
            // them. If it cannot serve them, the fetch fails and the batch is not signed.
            if !self.storage.contains_retrievable_transmission(*transmission_id) {
                // Determine the worker ID.
                let Ok(worker_id) = assign_to_worker(*transmission_id, num_workers) else {
                    bail!("Unable to assign transmission ID '{transmission_id}' to a worker")
                };
                // Retrieve the worker.
                let Some(worker) = workers.get().expect("No workers set").get(worker_id as usize) else {
                    bail!("Unable to find worker {worker_id}")
                };
                // Push the callback onto the list.
                fetch_transmissions.push(worker.get_or_fetch_transmission(peer_ip, *transmission_id));
            }
        }

        // Initialize a set for the transmissions.
        let mut transmissions = HashMap::with_capacity(fetch_transmissions.len());
        // Wait for all of the transmissions to be fetched.
        while let Some(result) = fetch_transmissions.next().await {
            // Retrieve the transmission.
            let (transmission_id, transmission) = result?;
            // Insert the transmission into the set.
            transmissions.insert(transmission_id, transmission);
        }
        // Return the transmissions.
        Ok(transmissions)
    }

    /// Fetches any missing previous certificates for the specified batch header from the specified peer.
    // `BatchCertificate` contains a `OnceLock` cache which does not affect its `Hash` or `Eq`.
    #[allow(clippy::mutable_key_type)]
    async fn fetch_missing_previous_certificates(
        &self,
        peer_ip: SocketAddr,
        batch_header: &BatchHeader<N>,
    ) -> Result<HashSet<BatchCertificate<N>>> {
        // Retrieve the round.
        let round = batch_header.round();
        // If the previous round is 0, or is <= the GC round, return early.
        if round == 1 || round <= self.storage.gc_round() + 1 {
            return Ok(Default::default());
        }

        // Fetch the missing previous certificates.
        #[allow(clippy::mutable_key_type)]
        let missing_previous_certificates =
            self.fetch_missing_certificates(peer_ip, round, batch_header.previous_certificate_ids()).await?;
        if !missing_previous_certificates.is_empty() {
            debug!(
                "Fetched {} missing previous certificates for round {round} from '{peer_ip}'",
                missing_previous_certificates.len(),
            );
        }
        // Return the missing previous certificates.
        Ok(missing_previous_certificates)
    }

    /// Fetches any missing certificates for the specified batch header from the specified peer.
    // `BatchCertificate` contains a `OnceLock` cache which does not affect its `Hash` or `Eq`.
    #[allow(clippy::mutable_key_type)]
    async fn fetch_missing_certificates(
        &self,
        peer_ip: SocketAddr,
        round: u64,
        certificate_ids: &IndexSet<Field<N>>,
    ) -> Result<HashSet<BatchCertificate<N>>> {
        // Initialize a list for the missing certificates.
        let mut fetch_certificates = FuturesUnordered::new();
        // Initialize a set for the missing certificates.
        #[allow(clippy::mutable_key_type)]
        let mut missing_certificates = HashSet::default();
        // Iterate through the certificate IDs.
        for certificate_id in certificate_ids {
            // Check if the certificate already exists in the ledger.
            if self.ledger.contains_certificate(certificate_id)? {
                continue;
            }
            // Check if the certificate already exists in storage.
            if self.storage.contains_certificate(*certificate_id) {
                continue;
            }
            // If we have not fully processed the certificate yet, store it.
            if let Some(certificate) = self.storage.get_unprocessed_certificate(*certificate_id) {
                missing_certificates.insert(certificate);
            } else {
                // If we do not have the certificate, request it.
                trace!("Primary - Found a new certificate ID for round {round} from '{peer_ip}'");
                // TODO (howardwu): Limit the number of open requests we send to a peer.
                // Send an certificate request to the peer.
                fetch_certificates.push(self.sync.send_certificate_request(peer_ip, *certificate_id));
            }
        }

        // If there are no certificates to fetch, return early with the existing unprocessed certificates.
        match fetch_certificates.is_empty() {
            true => return Ok(missing_certificates),
            false => trace!(
                "Fetching {} missing certificates for round {round} from '{peer_ip}'...",
                fetch_certificates.len(),
            ),
        }

        // Wait for all of the missing certificates to be fetched.
        while let Some(result) = fetch_certificates.next().await {
            // Insert the missing certificate into the set.
            missing_certificates.insert(result?);
        }
        // Return the missing certificates.
        Ok(missing_certificates)
    }
}

impl<N: Network> Primary<N> {
    /// Spawns a task with the given future; it should only be used for long-running tasks.
    fn spawn<T: Future<Output = ()> + Send + 'static>(&self, future: T) {
        let mut handles = self.handles.lock();
        handles.retain(|handle| !handle.is_finished());
        handles.push(tokio::spawn(future));
    }

    /// Shuts down the primary.
    pub async fn shut_down(&self) {
        info!("Shutting down the primary...");
        // Remove the callback.
        self.primary_callback.clear();
        // Shut down the sync service.
        self.sync.shut_down().await;
        // Shut down the workers.
        self.workers().iter().for_each(|worker| worker.shut_down());
        // Abort the tasks.
        self.handles.lock().drain(..).for_each(|handle| handle.abort());
        // Save the current proposal cache to disk.
        let proposal_cache = {
            // Only persist a Certifying batch; a Certified batch will appear in pending_certificates.
            // Note: it is guaranteed that there are no concurrent accesses to `proposed_batch` as all
            // background tasks already terminated at this point.
            let proposal = match std::mem::replace(&mut *self.proposed_batch.write(), ProposedBatchState::None) {
                ProposedBatchState::Certifying(p) => Some(*p),
                _ => None,
            };
            let signed_proposals = self.signed_proposals.read().clone();
            let latest_round = proposal
                .as_ref()
                .map(Proposal::round)
                .unwrap_or(self.latest_proposal_timestamp.read().await.map(|(round, _)| round).unwrap_or(0));
            let pending_certificates = self.storage.get_pending_certificates();
            ProposalCache::new(latest_round, proposal, signed_proposals, pending_certificates)
        };
        if let Err(err) = proposal_cache.store(&self.node_data_dir) {
            error!("{}", flatten_error(err.context("Failed to store the current proposal cache")));
        }
        // Close the gateway.
        self.gateway.shut_down().await;
    }
}

#[cfg(test)]
mod tests {
    use super::{proposal_task::BatchPropose as _, *};

    use snarkos_node_bft_ledger_service::MockLedgerService;
    use snarkos_node_bft_storage_service::BFTMemoryService;
    use snarkos_node_sync::BlockSync;
    use snarkvm::{
        ledger::{
            committee::{Committee, MIN_VALIDATOR_STAKE},
            test_helpers::sample_execution_transaction_with_fee,
        },
        prelude::{Address, Signature},
    };

    use bytes::Bytes;
    use indexmap::IndexSet;
    use rand::RngExt;

    type CurrentNetwork = snarkvm::prelude::MainnetV0;

    fn sample_committee(rng: &mut TestRng) -> (Vec<(SocketAddr, Account<CurrentNetwork>)>, Committee<CurrentNetwork>) {
        // Create a committee containing the primary's account.
        const COMMITTEE_SIZE: usize = 4;
        let mut accounts = Vec::with_capacity(COMMITTEE_SIZE);
        let mut members = IndexMap::new();

        for i in 0..COMMITTEE_SIZE {
            let socket_addr = format!("127.0.0.1:{}", 5000 + i).parse().unwrap();
            let account = Account::new(rng).unwrap();

            members.insert(account.address(), (MIN_VALIDATOR_STAKE, true, rng.random_range(0..100)));
            accounts.push((socket_addr, account));
        }

        (accounts, Committee::<CurrentNetwork>::new(1, members).unwrap())
    }

    // Returns a primary and a list of accounts in the configured committee.
    fn primary_with_committee(
        account_index: usize,
        accounts: &[(SocketAddr, Account<CurrentNetwork>)],
        committee: Committee<CurrentNetwork>,
        height: u32,
    ) -> Primary<CurrentNetwork> {
        let ledger = Arc::new(MockLedgerService::new_at_height(committee, height));
        let storage = Storage::new(ledger.clone(), Arc::new(BFTMemoryService::new()), 10).unwrap();

        // Initialize the primary.
        let account = accounts[account_index].1.clone();
        let block_sync = Arc::new(BlockSync::new(ledger.clone(), ConnectionMode::Gateway));
        let primary =
            Primary::new(account, storage, ledger, block_sync, None, &[], false, NodeDataDir::new_test(None), None, 1)
                .unwrap();

        // Construct a worker instance.
        let worker = Worker::new(
            0, // id
            Arc::new(primary.gateway.clone()),
            primary.storage.clone(),
            primary.ledger.clone(),
            primary.proposed_batch.clone(),
        )
        .unwrap();
        let _ = primary.workers.set(vec![worker]);
        for a in accounts.iter().skip(account_index) {
            primary.gateway.insert_connected_peer(a.0, a.0, a.1.address());
        }

        primary
    }

    fn primary_without_handlers(
        rng: &mut TestRng,
    ) -> (Primary<CurrentNetwork>, Vec<(SocketAddr, Account<CurrentNetwork>)>) {
        let (accounts, committee) = sample_committee(rng);
        let primary = primary_with_committee(
            0, // index of primary's account
            &accounts,
            committee,
            CurrentNetwork::CONSENSUS_HEIGHT(ConsensusVersion::V1).unwrap(),
        );

        (primary, accounts)
    }

    // Creates a mock solution.
    fn sample_unconfirmed_solution(rng: &mut TestRng) -> (SolutionID<CurrentNetwork>, Data<Solution<CurrentNetwork>>) {
        // Sample a random fake solution ID.
        let solution_id = rng.random::<u64>().into();
        // Vary the size of the solutions.
        let size = rng.random_range(1024..10 * 1024);
        // Sample random fake solution bytes.
        let vec: Vec<u8> = (0..size).map(|_| rng.random::<u8>()).collect();
        let solution = Data::Buffer(Bytes::from(vec));
        // Return the solution ID and solution.
        (solution_id, solution)
    }

    // Samples a test transaction.
    fn sample_unconfirmed_transaction(
        rng: &mut TestRng,
    ) -> (<CurrentNetwork as Network>::TransactionID, Data<Transaction<CurrentNetwork>>) {
        let transaction = sample_execution_transaction_with_fee(false, rng, 0);
        let id = transaction.id();

        (id, Data::Object(transaction))
    }

    // Creates a batch proposal with one solution and one transaction.
    fn create_test_proposal(
        author: &Account<CurrentNetwork>,
        committee: Committee<CurrentNetwork>,
        round: u64,
        previous_certificate_ids: IndexSet<Field<CurrentNetwork>>,
        timestamp: i64,
        num_transactions: u64,
        rng: &mut TestRng,
    ) -> Proposal<CurrentNetwork> {
        let mut transmission_ids = IndexSet::new();
        let mut transmissions = IndexMap::new();

        // Prepare the solution and insert into the sets.
        let (solution_id, solution) = sample_unconfirmed_solution(rng);
        let solution_checksum = solution.to_checksum::<CurrentNetwork>().unwrap();
        let solution_transmission_id = (solution_id, solution_checksum).into();
        transmission_ids.insert(solution_transmission_id);
        transmissions.insert(solution_transmission_id, Transmission::Solution(solution));

        // Prepare the transactions and insert into the sets.
        for _ in 0..num_transactions {
            let (transaction_id, transaction) = sample_unconfirmed_transaction(rng);
            let transaction_checksum = transaction.to_checksum::<CurrentNetwork>().unwrap();
            let transaction_transmission_id = (&transaction_id, &transaction_checksum).into();
            transmission_ids.insert(transaction_transmission_id);
            transmissions.insert(transaction_transmission_id, Transmission::Transaction(transaction));
        }

        // Retrieve the private key.
        let private_key = author.private_key();
        // Sign the batch header.
        let batch_header = BatchHeader::new(
            private_key,
            round,
            timestamp,
            committee.id(),
            transmission_ids,
            previous_certificate_ids,
            rng,
        )
        .unwrap();
        // Construct the proposal.
        Proposal::new(committee, batch_header, transmissions).unwrap()
    }

    // Creates a signature of the primary's current proposal for each committee member (excluding
    // the primary).
    fn peer_signatures_for_proposal(
        primary: &Primary<CurrentNetwork>,
        accounts: &[(SocketAddr, Account<CurrentNetwork>)],
        rng: &mut TestRng,
    ) -> Vec<(SocketAddr, BatchSignature<CurrentNetwork>)> {
        // Each committee member signs the batch.
        let mut signatures = Vec::with_capacity(accounts.len() - 1);
        for (socket_addr, account) in accounts {
            if account.address() == primary.gateway.account().address() {
                continue;
            }
            let batch_id = primary.proposed_batch.read().as_proposal().unwrap().batch_id();
            let signature = account.sign(&[batch_id], rng).unwrap();
            signatures.push((*socket_addr, BatchSignature::new(batch_id, signature)));
        }

        signatures
    }

    /// Creates a signature of the batch ID for each committee member (excluding the primary).
    fn peer_signatures_for_batch(
        primary_address: Address<CurrentNetwork>,
        accounts: &[(SocketAddr, Account<CurrentNetwork>)],
        batch_id: Field<CurrentNetwork>,
        rng: &mut TestRng,
    ) -> IndexSet<Signature<CurrentNetwork>> {
        let mut signatures = IndexSet::new();
        for (_, account) in accounts {
            if account.address() == primary_address {
                continue;
            }
            let signature = account.sign(&[batch_id], rng).unwrap();
            signatures.insert(signature);
        }
        signatures
    }

    // Creates a batch certificate.
    fn create_batch_certificate(
        primary_address: Address<CurrentNetwork>,
        accounts: &[(SocketAddr, Account<CurrentNetwork>)],
        round: u64,
        previous_certificate_ids: IndexSet<Field<CurrentNetwork>>,
        rng: &mut TestRng,
    ) -> (BatchCertificate<CurrentNetwork>, HashMap<TransmissionID<CurrentNetwork>, Transmission<CurrentNetwork>>) {
        let timestamp = now();

        let author =
            accounts.iter().find(|&(_, acct)| acct.address() == primary_address).map(|(_, acct)| acct.clone()).unwrap();
        let private_key = author.private_key();

        let committee_id = Field::rand(rng);
        let (solution_id, solution) = sample_unconfirmed_solution(rng);
        let (transaction_id, transaction) = sample_unconfirmed_transaction(rng);
        let solution_checksum = solution.to_checksum::<CurrentNetwork>().unwrap();
        let transaction_checksum = transaction.to_checksum::<CurrentNetwork>().unwrap();

        let solution_transmission_id = (solution_id, solution_checksum).into();
        let transaction_transmission_id = (&transaction_id, &transaction_checksum).into();

        let transmission_ids = [solution_transmission_id, transaction_transmission_id].into();
        let transmissions = [
            (solution_transmission_id, Transmission::Solution(solution)),
            (transaction_transmission_id, Transmission::Transaction(transaction)),
        ]
        .into();

        let batch_header = BatchHeader::new(
            private_key,
            round,
            timestamp,
            committee_id,
            transmission_ids,
            previous_certificate_ids,
            rng,
        )
        .unwrap();
        let signatures = peer_signatures_for_batch(primary_address, accounts, batch_header.batch_id(), rng);
        let certificate = BatchCertificate::<CurrentNetwork>::from(batch_header, signatures).unwrap();
        (certificate, transmissions)
    }

    // Create a certificate chain up to, but not including, the specified round in the primary storage.
    fn store_certificate_chain(
        primary: &Primary<CurrentNetwork>,
        accounts: &[(SocketAddr, Account<CurrentNetwork>)],
        round: u64,
        rng: &mut TestRng,
    ) -> IndexSet<Field<CurrentNetwork>> {
        let mut previous_certificates = IndexSet::<Field<CurrentNetwork>>::new();
        let mut next_certificates = IndexSet::<Field<CurrentNetwork>>::new();
        for cur_round in 1..round {
            for (_, account) in accounts.iter() {
                let (certificate, transmissions) = create_batch_certificate(
                    account.address(),
                    accounts,
                    cur_round,
                    previous_certificates.clone(),
                    rng,
                );
                next_certificates.insert(certificate.id());
                assert!(primary.storage.insert_certificate(certificate, transmissions, Default::default()).is_ok());
            }

            assert!(primary.storage.increment_to_next_round(cur_round).is_ok());
            previous_certificates = next_certificates;
            next_certificates = IndexSet::<Field<CurrentNetwork>>::new();
        }

        previous_certificates
    }

    // Insert the account socket addresses into the resolver so that
    // they are recognized as "connected".
    fn map_account_addresses(primary: &Primary<CurrentNetwork>, accounts: &[(SocketAddr, Account<CurrentNetwork>)]) {
        // First account is primary, which doesn't need to resolve.
        for (addr, acct) in accounts.iter().skip(1) {
            primary.gateway.resolver().write().insert_peer(*addr, *addr, Some(acct.address()));
        }
    }

    #[test_log::test(tokio::test)]
    async fn test_propose_batch() {
        let mut rng = TestRng::default();
        let (primary, _) = primary_without_handlers(&mut rng);

        // Check there is no batch currently proposed.
        assert!(primary.proposed_batch.read().is_none());

        // Generate a solution and a transaction.
        let (solution_id, solution) = sample_unconfirmed_solution(&mut rng);
        let (transaction_id, transaction) = sample_unconfirmed_transaction(&mut rng);

        // Store it on one of the workers.
        primary.workers()[0].process_unconfirmed_solution(solution_id, solution).await.unwrap();
        primary.workers()[0].process_unconfirmed_transaction(transaction_id, transaction).await.unwrap();

        // Try to propose a batch again. This time, it should succeed.
        assert!(primary.propose_batch().await.is_ok());
        assert!(primary.proposed_batch.read().is_proposed());
    }

    /// A transmission that storage only knows as aborted was already decided by a block, and storage
    /// has nothing to hand back for it at commit time, so it must be skipped when proposing -
    /// including when it is the first item drained from the worker, which used to bypass the storage
    /// check entirely.
    #[test_log::test(tokio::test)]
    async fn test_propose_batch_skips_an_aborted_first_transmission() {
        let mut rng = TestRng::default();
        let (primary, accounts) = primary_without_handlers(&mut rng);
        let peer_ip = accounts[1].0;

        // Queue a certificate's transmissions on the worker before recording them as aborted.
        let (certificate, transmissions) =
            create_batch_certificate(accounts[1].1.address(), &accounts, 1, Default::default(), &mut rng);
        for (transmission_id, transmission) in &transmissions {
            primary.workers()[0].process_transmission_from_peer(peer_ip, *transmission_id, transmission.clone());
        }
        assert_eq!(primary.workers()[0].num_transmissions(), transmissions.len());

        // Record the queued transmission IDs as aborted in storage.
        let aborted_transmissions = transmissions.keys().copied().collect::<HashSet<_>>();
        primary.storage.insert_certificate(certificate, Default::default(), aborted_transmissions.clone()).unwrap();
        for transmission_id in &aborted_transmissions {
            assert!(primary.storage.contains_aborted_transmission(*transmission_id));
            assert!(!primary.storage.contains_retrievable_transmission(*transmission_id));
        }

        // Every aborted ID is skipped, leaving an empty proposal rather than an uncommittable one.
        assert!(primary.propose_batch().await.unwrap());
        assert!(primary.proposed_batch.read().as_proposal().unwrap().transmissions().is_empty());
        assert_eq!(primary.workers()[0].num_transmissions(), 0);
    }

    #[test_log::test(tokio::test)]
    async fn test_propose_batch_with_no_transmissions() {
        let mut rng = TestRng::default();
        let (primary, _) = primary_without_handlers(&mut rng);

        // Check there is no batch currently proposed.
        assert!(primary.proposed_batch.read().is_none());

        // Try to propose a batch with no transmissions.
        assert!(primary.propose_batch().await.is_ok());
        assert!(primary.proposed_batch.read().is_proposed());
    }

    #[test_log::test(tokio::test)]
    async fn test_propose_batch_in_round() {
        let round = 3;
        let mut rng = TestRng::default();
        let (primary, accounts) = primary_without_handlers(&mut rng);

        // Fill primary storage.
        store_certificate_chain(&primary, &accounts, round, &mut rng);

        // Sleep for a while to ensure the primary is ready to propose the next round.
        tokio::time::sleep(MIN_BATCH_DELAY).await;

        // Generate a solution and a transaction.
        let (solution_id, solution) = sample_unconfirmed_solution(&mut rng);
        let (transaction_id, transaction) = sample_unconfirmed_transaction(&mut rng);

        // Store it on one of the workers.
        primary.workers()[0].process_unconfirmed_solution(solution_id, solution).await.unwrap();
        primary.workers()[0].process_unconfirmed_transaction(transaction_id, transaction).await.unwrap();

        // Propose a batch again. This time, it should succeed.
        assert!(primary.propose_batch().await.is_ok());
        assert!(primary.proposed_batch.read().is_proposed());
    }

    /// The previous round must reach quorum before the primary is allowed to propose the next
    /// round. This is now the sole local safeguard against proposing without adequate DAG state -
    /// `is_synced()` (peer-height-based) no longer gates this decision.
    #[test_log::test(tokio::test)]
    async fn test_propose_batch_without_previous_round_quorum() {
        let mut rng = TestRng::default();
        let (primary, accounts) = primary_without_handlers(&mut rng);

        // Insert a single round-1 certificate, authored by only one other committee member - well
        // below the quorum threshold (3 of the 4 validators).
        let (certificate, transmissions) =
            create_batch_certificate(accounts[1].1.address(), &accounts, 1, Default::default(), &mut rng);
        primary.storage.insert_certificate(certificate, transmissions, Default::default()).unwrap();

        // Advance storage's round pointer to 2, as `store_certificate_chain` does internally.
        // This simulates the round pointer moving ahead (e.g. via a sync-applied block) without
        // the local certificate set actually supporting it.
        primary.storage.increment_to_next_round(1).unwrap();
        assert_eq!(primary.current_round(), 2);

        // The primary must refuse to propose: it does not have quorum for the previous round.
        assert!(!primary.propose_batch().await.unwrap());
        assert!(primary.proposed_batch.read().is_none());
    }

    #[test_log::test(tokio::test)]
    async fn test_propose_batch_skip_transmissions_from_previous_certificates() {
        let round = 3;
        let prev_round = round - 1;
        let mut rng = TestRng::default();
        let (primary, accounts) = primary_without_handlers(&mut rng);
        let peer_account = &accounts[1];
        let peer_ip = peer_account.0;

        // Fill primary storage.
        store_certificate_chain(&primary, &accounts, round, &mut rng);

        // Get transmissions from previous certificates.
        let previous_certificate_ids: IndexSet<_> = primary.storage.get_certificate_ids_for_round(prev_round);

        // Track the number of transmissions in the previous round.
        let mut num_transmissions_in_previous_round = 0;

        // Generate a solution and a transaction.
        let (solution_commitment, solution) = sample_unconfirmed_solution(&mut rng);
        let (transaction_id, transaction) = sample_unconfirmed_transaction(&mut rng);
        let solution_checksum = solution.to_checksum::<CurrentNetwork>().unwrap();
        let transaction_checksum = transaction.to_checksum::<CurrentNetwork>().unwrap();

        // Store it on one of the workers.
        primary.workers()[0].process_unconfirmed_solution(solution_commitment, solution).await.unwrap();
        primary.workers()[0].process_unconfirmed_transaction(transaction_id, transaction).await.unwrap();

        // Check that the worker has 2 transmissions.
        assert_eq!(primary.workers()[0].num_transmissions(), 2);

        // Create certificates for the current round and add the transmissions to the worker before inserting the certificate to storage.
        for (_, account) in accounts.iter() {
            let (certificate, transmissions) = create_batch_certificate(
                account.address(),
                &accounts,
                round,
                previous_certificate_ids.clone(),
                &mut rng,
            );

            // Add the transmissions to the worker.
            for (transmission_id, transmission) in transmissions.iter() {
                primary.workers()[0].process_transmission_from_peer(peer_ip, *transmission_id, transmission.clone());
            }

            // Insert the certificate to storage.
            num_transmissions_in_previous_round += transmissions.len();
            primary.storage.insert_certificate(certificate, transmissions, Default::default()).unwrap();
        }

        // Sleep for a while to ensure the primary is ready to propose the next round.
        tokio::time::sleep(MIN_BATCH_DELAY).await;

        // Advance to the next round.
        assert!(primary.storage.increment_to_next_round(round).is_ok());

        // Check that the worker has `num_transmissions_in_previous_round + 2` transmissions.
        assert_eq!(primary.workers()[0].num_transmissions(), num_transmissions_in_previous_round + 2);

        // Propose the batch.
        assert!(primary.propose_batch().await.is_ok());

        // Check that the proposal only contains the new transmissions that were not in previous certificates.
        let proposed_transmissions = primary.proposed_batch.read().as_proposal().unwrap().transmissions().clone();
        assert_eq!(proposed_transmissions.len(), 2);
        assert!(proposed_transmissions.contains_key(&TransmissionID::Solution(solution_commitment, solution_checksum)));
        assert!(
            proposed_transmissions.contains_key(&TransmissionID::Transaction(transaction_id, transaction_checksum))
        );
    }

    #[test_log::test(tokio::test)]
    async fn test_propose_batch_over_spend_limit() {
        let mut rng = TestRng::default();

        // Create a primary to test spend limit backwards compatibility with V4.
        let (accounts, committee) = sample_committee(&mut rng);
        let primary = primary_with_committee(
            0,
            &accounts,
            committee.clone(),
            CurrentNetwork::CONSENSUS_HEIGHT(ConsensusVersion::V4).unwrap(),
        );

        // Check there is no batch currently proposed.
        assert!(primary.proposed_batch.read().is_none());
        // Check the workers are empty.
        primary.workers().iter().for_each(|worker| assert!(worker.transmissions().is_empty()));

        // Generate a solution and transactions.
        let (solution_id, solution) = sample_unconfirmed_solution(&mut rng);
        primary.workers()[0].process_unconfirmed_solution(solution_id, solution).await.unwrap();

        for _i in 0..5 {
            let (transaction_id, transaction) = sample_unconfirmed_transaction(&mut rng);
            // Store it on one of the workers.
            primary.workers()[0].process_unconfirmed_transaction(transaction_id, transaction).await.unwrap();
        }

        // Try to propose a batch again. This time, it should succeed.
        assert!(primary.propose_batch().await.is_ok());
        // Expect 2/5 transactions to be included in the proposal in addition to the solution.
        assert_eq!(primary.proposed_batch.read().as_proposal().unwrap().transmissions().len(), 3);
        // Check the transmissions were correctly drained from the workers.
        assert_eq!(primary.workers().iter().map(|worker| worker.transmissions().len()).sum::<usize>(), 3);
    }

    #[test_log::test(tokio::test)]
    async fn test_batch_propose_from_peer() {
        let mut rng = TestRng::default();
        let (primary, accounts) = primary_without_handlers(&mut rng);

        // Create a valid proposal with an author that isn't the primary.
        let round = 1;
        let peer_account = &accounts[1];
        let peer_ip = peer_account.0;
        let timestamp = now() + MIN_BATCH_DELAY.as_secs() as i64;
        let proposal = create_test_proposal(
            &peer_account.1,
            primary.ledger.current_committee().unwrap(),
            round,
            Default::default(),
            timestamp,
            1,
            &mut rng,
        );

        // Make sure the primary is aware of the transmissions in the proposal.
        for (transmission_id, transmission) in proposal.transmissions() {
            primary.workers()[0].process_transmission_from_peer(peer_ip, *transmission_id, transmission.clone())
        }

        // The author must be known to resolver to pass propose checks.
        primary.gateway.resolver().write().insert_peer(peer_ip, peer_ip, Some(peer_account.1.address()));

        // Try to process the batch proposal from the peer, should succeed.
        assert!(
            primary.process_batch_propose_from_peer(peer_ip, (*proposal.batch_header()).clone().into()).await.is_ok()
        );
        // The primary signed the proposal.
        assert!(primary.signed_proposals.read().get(&peer_account.1.address()).is_some());
    }

    /// A transmission that storage knows only as aborted still has to be fetched from the peer:
    /// storage holds no payload for it, so skipping the fetch is what leaves a certificate stored
    /// with a transmission this node can never materialize.
    #[test_log::test(tokio::test)]
    async fn test_fetch_missing_transmissions_fetches_an_aborted_transmission() {
        let mut rng = TestRng::default();
        let (primary, accounts) = primary_without_handlers(&mut rng);
        let peer_ip = accounts[1].0;

        // A proposal from a peer, whose transmissions the worker holds - as the peer's would.
        let round = 1;
        let proposal = create_test_proposal(
            &accounts[1].1,
            primary.ledger.current_committee().unwrap(),
            round,
            Default::default(),
            now() + MIN_BATCH_DELAY.as_secs() as i64,
            1,
            &mut rng,
        );
        for (transmission_id, transmission) in proposal.transmissions() {
            primary.workers()[0].process_transmission_from_peer(peer_ip, *transmission_id, transmission.clone());
        }

        // Record one of the referenced IDs as aborted, as an earlier certificate would have.
        let aborted_transmission_id = *proposal.transmissions().keys().next().unwrap();
        let (certificate, transmissions) =
            create_batch_certificate(accounts[2].1.address(), &accounts, round, Default::default(), &mut rng);
        primary
            .storage
            .insert_certificate(certificate, transmissions, [aborted_transmission_id].into_iter().collect())
            .unwrap();
        assert!(primary.storage.contains_aborted_transmission(aborted_transmission_id));
        assert!(!primary.storage.contains_retrievable_transmission(aborted_transmission_id));

        // The aborted ID is fetched like any other, rather than excused by its marker.
        let fetched = primary.fetch_missing_transmissions(peer_ip, proposal.batch_header()).await.unwrap();
        assert_eq!(
            fetched.get(&aborted_transmission_id),
            proposal.transmissions().get(&aborted_transmission_id),
            "the aborted transmission ID was skipped, so its payload will never reach storage"
        );
    }

    /// The worker must accept a transmission whose ID storage knows only as aborted: the aborted
    /// marker holds no payload, so discarding the bytes throws away what a fetch just produced and
    /// leaves this node unable to serve them to anyone else.
    #[test_log::test(tokio::test)]
    async fn test_worker_retains_a_transmission_for_an_aborted_id() {
        let mut rng = TestRng::default();
        let (primary, accounts) = primary_without_handlers(&mut rng);
        let peer_ip = accounts[1].0;

        // Record a certificate's transmission IDs as aborted, keeping the transmissions in hand.
        let (certificate, transmissions) =
            create_batch_certificate(accounts[1].1.address(), &accounts, 1, Default::default(), &mut rng);
        let aborted_transmissions = transmissions.keys().copied().collect::<HashSet<_>>();
        primary.storage.insert_certificate(certificate, Default::default(), aborted_transmissions).unwrap();

        let (transmission_id, transmission) = transmissions.iter().next().unwrap();
        assert!(primary.storage.contains_aborted_transmission(*transmission_id));
        assert!(!primary.storage.contains_retrievable_transmission(*transmission_id));

        // The worker takes the bytes rather than treating the aborted marker as already holding them.
        primary.workers()[0].process_transmission_from_peer(peer_ip, *transmission_id, transmission.clone());
        assert_eq!(
            primary.workers()[0].get_transmission(*transmission_id),
            Some(transmission.clone()),
            "the worker discarded the payload, so it cannot be served or reused"
        );
    }

    /// A proposal referencing a transmission that storage knows only as aborted is signed like any
    /// other, once the bytes have been obtained from its author.
    ///
    /// The aborted marker is not a reason to withhold a signature: it only means this node holds no
    /// payload, and the author of a batch is claiming it can serve one. Refusing here would be a
    /// unilateral judgment made from local sync state, which an honest peer that has not yet seen
    /// the aborting block would trip. A proposal whose author cannot serve the bytes fails earlier,
    /// at the fetch, like any other proposal with a missing transmission.
    #[test_log::test(tokio::test)]
    async fn test_batch_propose_from_peer_with_an_aborted_transmission() {
        let mut rng = TestRng::default();
        let (primary, accounts) = primary_without_handlers(&mut rng);

        // Create a valid proposal with an author that isn't the primary.
        let round = 1;
        let peer_account = &accounts[1];
        let peer_ip = peer_account.0;
        let timestamp = now() + MIN_BATCH_DELAY.as_secs() as i64;
        let proposal = create_test_proposal(
            &peer_account.1,
            primary.ledger.current_committee().unwrap(),
            round,
            Default::default(),
            timestamp,
            1,
            &mut rng,
        );

        // Make sure the primary is aware of the transmissions in the proposal.
        for (transmission_id, transmission) in proposal.transmissions() {
            primary.workers()[0].process_transmission_from_peer(peer_ip, *transmission_id, transmission.clone())
        }

        // The author must be known to resolver to pass propose checks.
        primary.gateway.resolver().write().insert_peer(peer_ip, peer_ip, Some(peer_account.1.address()));

        // Record one of the proposed transmission IDs as aborted, as an earlier certificate would have.
        let aborted_transmission_id = *proposal.transmissions().keys().next().unwrap();
        let (certificate, transmissions) =
            create_batch_certificate(accounts[2].1.address(), &accounts, round, Default::default(), &mut rng);
        primary
            .storage
            .insert_certificate(certificate, transmissions, [aborted_transmission_id].into_iter().collect())
            .unwrap();
        assert!(primary.storage.contains_aborted_transmission(aborted_transmission_id));
        assert!(!primary.storage.contains_retrievable_transmission(aborted_transmission_id));

        // The payload is available from the author, so the proposal is signed like any other.
        primary.process_batch_propose_from_peer(peer_ip, (*proposal.batch_header()).clone().into()).await.unwrap();
        assert!(primary.signed_proposals.read().get(&peer_account.1.address()).is_some());
    }

    /// A proposal for the current round is left in place; once the round advances past it, the same
    /// proposal is cleared and its transmissions are returned to the workers.
    #[test_log::test(tokio::test)]
    async fn test_check_proposed_batch_for_expiration() {
        let mut rng = TestRng::default();
        let (primary, accounts) = primary_without_handlers(&mut rng);

        // Advance storage to the round the proposal is made for.
        let round = 3;
        let previous_certificates = store_certificate_chain(&primary, &accounts, round, &mut rng);
        assert_eq!(primary.current_round(), round);

        // Propose a batch for the current round.
        let proposal = create_test_proposal(
            &accounts[0].1,
            primary.ledger.current_committee().unwrap(),
            round,
            previous_certificates,
            now(),
            1,
            &mut rng,
        );
        let batch_id = proposal.batch_id();
        let transmission_ids: Vec<_> = proposal.transmissions().keys().copied().collect();
        assert!(!transmission_ids.is_empty());
        *primary.proposed_batch.write() = ProposedBatchState::Certifying(Box::new(proposal));

        // The proposal is for the current round, so it is not expired and must be left alone.
        primary.check_proposed_batch_for_expiration().unwrap();
        assert_eq!(primary.proposed_batch.read().as_proposal().unwrap().batch_id(), batch_id);

        // Advance the round past the proposal.
        primary.storage.increment_to_next_round(round).unwrap();
        assert!(primary.current_round() > round);

        // The proposal is now stale, so it is cleared and its transmissions go back to the workers.
        primary.check_proposed_batch_for_expiration().unwrap();
        assert!(primary.proposed_batch.read().is_none());
        for transmission_id in transmission_ids {
            assert!(primary.workers()[0].contains_transmission(transmission_id));
        }
    }

    /// Each recheck of a pending proposal cancels the resends of the previous recheck, and the
    /// latest resends are cancelled once the round is over.
    #[test_log::test(tokio::test)]
    async fn test_resend_supersedes_previous_resends() {
        let mut rng = TestRng::default();
        let (primary, accounts) = primary_without_handlers(&mut rng);

        // Store a pending proposal for the current round.
        let round = 3;
        let previous_certificates = store_certificate_chain(&primary, &accounts, round, &mut rng);
        let proposal = create_test_proposal(
            &accounts[0].1,
            primary.ledger.current_committee().unwrap(),
            round,
            previous_certificates,
            now(),
            1,
            &mut rng,
        );
        *primary.proposed_batch.write() = ProposedBatchState::Certifying(Box::new(proposal));

        // The first recheck resends the proposal.
        assert!(!primary.propose_batch().await.unwrap());
        let first = primary.resend_token.lock().clone();
        assert!(!first.is_cancelled());

        // The second recheck supersedes the first one's resends.
        assert!(!primary.propose_batch().await.unwrap());
        let second = primary.resend_token.lock().clone();
        assert!(first.is_cancelled());
        assert!(!second.is_cancelled());

        // Advancing the round cancels the latest resends.
        primary.storage.increment_to_next_round(round).unwrap();
        assert!(second.is_cancelled());
    }

    /// The signed-proposal cache advances, and only advances: an older round must never overwrite a
    /// newer one, and a round already cached must never be signed twice.
    #[test_log::test(tokio::test)]
    async fn test_signed_proposal_cache_never_moves_backwards() {
        let mut rng = TestRng::default();
        let (primary, accounts) = primary_without_handlers(&mut rng);
        let account = primary.gateway.account().clone();
        let author = accounts[1].1.address();

        // The first proposal seen for a validator is cached.
        let id_2 = Field::rand(&mut rng);
        let sig_2 = account.sign(&[id_2], &mut rng).unwrap();
        assert!(primary.cache_signed_proposal(author, 2, id_2, sig_2));
        assert_eq!(primary.signed_proposals.read().get(&author).copied().unwrap(), (2, id_2, sig_2));

        // A newer round advances the cache.
        let id_3 = Field::rand(&mut rng);
        let sig_3 = account.sign(&[id_3], &mut rng).unwrap();
        assert!(primary.cache_signed_proposal(author, 3, id_3, sig_3));
        assert_eq!(primary.signed_proposals.read().get(&author).copied().unwrap(), (3, id_3, sig_3));

        // A conflicting batch for the cached round is refused, and leaves the cache intact.
        let conflicting_id = Field::rand(&mut rng);
        let conflicting_sig = account.sign(&[conflicting_id], &mut rng).unwrap();
        assert!(!primary.cache_signed_proposal(author, 3, conflicting_id, conflicting_sig));
        assert_eq!(primary.signed_proposals.read().get(&author).copied().unwrap(), (3, id_3, sig_3));

        // The regression: a handler for an older round must not downgrade the cache.
        let stale_id = Field::rand(&mut rng);
        let stale_sig = account.sign(&[stale_id], &mut rng).unwrap();
        assert!(!primary.cache_signed_proposal(author, 2, stale_id, stale_sig));
        assert_eq!(
            primary.signed_proposals.read().get(&author).copied().unwrap(),
            (3, id_3, sig_3),
            "a handler for an older round downgraded the signed-proposal cache"
        );

        // Entries for other validators are independent.
        let other_author = accounts[2].1.address();
        let other_id = Field::rand(&mut rng);
        let other_sig = account.sign(&[other_id], &mut rng).unwrap();
        assert!(primary.cache_signed_proposal(other_author, 1, other_id, other_sig));
        assert_eq!(primary.signed_proposals.read().get(&author).copied().unwrap().0, 3);
    }

    #[test_log::test(tokio::test)]
    async fn test_batch_propose_from_peer_in_round() {
        let round = 2;
        let mut rng = TestRng::default();
        let (primary, accounts) = primary_without_handlers(&mut rng);

        // Generate certificates.
        let previous_certificates = store_certificate_chain(&primary, &accounts, round, &mut rng);

        // Create a valid proposal with an author that isn't the primary.
        let peer_account = &accounts[1];
        let peer_ip = peer_account.0;
        let timestamp = now() + MIN_BATCH_DELAY.as_secs() as i64;
        let proposal = create_test_proposal(
            &peer_account.1,
            primary.ledger.current_committee().unwrap(),
            round,
            previous_certificates,
            timestamp,
            1,
            &mut rng,
        );

        // Make sure the primary is aware of the transmissions in the proposal.
        for (transmission_id, transmission) in proposal.transmissions() {
            primary.workers()[0].process_transmission_from_peer(peer_ip, *transmission_id, transmission.clone())
        }

        // The author must be known to resolver to pass propose checks.
        primary.gateway.resolver().write().insert_peer(peer_ip, peer_ip, Some(peer_account.1.address()));

        // Try to process the batch proposal from the peer, should succeed.
        primary.process_batch_propose_from_peer(peer_ip, (*proposal.batch_header()).clone().into()).await.unwrap();
    }

    #[test_log::test(tokio::test)]
    async fn test_batch_propose_from_peer_wrong_round() {
        let mut rng = TestRng::default();
        let (primary, accounts) = primary_without_handlers(&mut rng);

        // Create a valid proposal with an author that isn't the primary.
        let round = 1;
        let peer_account = &accounts[1];
        let peer_ip = peer_account.0;
        let timestamp = now() + MIN_BATCH_DELAY.as_secs() as i64;
        let proposal = create_test_proposal(
            &peer_account.1,
            primary.ledger.current_committee().unwrap(),
            round,
            Default::default(),
            timestamp,
            1,
            &mut rng,
        );

        // Make sure the primary is aware of the transmissions in the proposal.
        for (transmission_id, transmission) in proposal.transmissions() {
            primary.workers()[0].process_transmission_from_peer(peer_ip, *transmission_id, transmission.clone())
        }

        // The author must be known to resolver to pass propose checks.
        primary.gateway.resolver().write().insert_peer(peer_ip, peer_ip, Some(peer_account.1.address()));

        // Try to process the batch proposal from the peer, should error.
        assert!(
            primary
                .process_batch_propose_from_peer(peer_ip, BatchPropose {
                    round: round + 1,
                    batch_header: Data::Object(proposal.batch_header().clone())
                })
                .await
                .is_err()
        );
    }

    #[test_log::test(tokio::test)]
    async fn test_batch_propose_from_peer_in_round_wrong_round() {
        let round = 4;
        let mut rng = TestRng::default();
        let (primary, accounts) = primary_without_handlers(&mut rng);

        // Generate certificates.
        let previous_certificates = store_certificate_chain(&primary, &accounts, round, &mut rng);

        // Create a valid proposal with an author that isn't the primary.
        let peer_account = &accounts[1];
        let peer_ip = peer_account.0;
        let timestamp = now() + MIN_BATCH_DELAY.as_secs() as i64;
        let proposal = create_test_proposal(
            &peer_account.1,
            primary.ledger.current_committee().unwrap(),
            round,
            previous_certificates,
            timestamp,
            1,
            &mut rng,
        );

        // Make sure the primary is aware of the transmissions in the proposal.
        for (transmission_id, transmission) in proposal.transmissions() {
            primary.workers()[0].process_transmission_from_peer(peer_ip, *transmission_id, transmission.clone())
        }

        // The author must be known to resolver to pass propose checks.
        primary.gateway.resolver().write().insert_peer(peer_ip, peer_ip, Some(peer_account.1.address()));

        // Try to process the batch proposal from the peer, should error.
        assert!(
            primary
                .process_batch_propose_from_peer(peer_ip, BatchPropose {
                    round: round + 1,
                    batch_header: Data::Object(proposal.batch_header().clone())
                })
                .await
                .is_err()
        );
    }

    /// Tests that the minimum batch delay is enforced as expected, i.e., that proposals with timestamps that are too close to the previous proposal are rejected.
    #[test_log::test(tokio::test)]
    async fn test_batch_propose_from_peer_with_past_timestamp() {
        let round = 2;
        let mut rng = TestRng::default();
        let (primary, accounts) = primary_without_handlers(&mut rng);

        // Generate certificates.
        let previous_certificates = store_certificate_chain(&primary, &accounts, round, &mut rng);

        // Create a valid proposal with an author that isn't the primary.
        let peer_account = &accounts[1];
        let peer_ip = peer_account.0;

        // Use a timestamp that is too early.
        // Set it to something that is less than the minimum batch delay
        // Note, that the minimum delay is currently 1, so this will be equal to the last timestamp
        let last_timestamp = primary
            .storage
            .get_certificate_for_round_with_author(round - 1, peer_account.1.address())
            .expect("No previous proposal exists")
            .timestamp();
        let invalid_timestamp = last_timestamp + (MIN_BATCH_DELAY.as_secs() as i64) - 1;

        let proposal = create_test_proposal(
            &peer_account.1,
            primary.ledger.current_committee().unwrap(),
            round,
            previous_certificates,
            invalid_timestamp,
            1,
            &mut rng,
        );

        // Make sure the primary is aware of the transmissions in the proposal.
        for (transmission_id, transmission) in proposal.transmissions() {
            primary.workers()[0].process_transmission_from_peer(peer_ip, *transmission_id, transmission.clone())
        }

        // The author must be known to resolver to pass propose checks.
        primary.gateway.resolver().write().insert_peer(peer_ip, peer_ip, Some(peer_account.1.address()));

        // Try to process the batch proposal from the peer, should error.
        assert!(
            primary.process_batch_propose_from_peer(peer_ip, (*proposal.batch_header()).clone().into()).await.is_err()
        );
    }

    #[test_log::test(tokio::test)]
    async fn test_propose_batch_with_storage_round_behind_proposal_lock() {
        let round = 3;
        let mut rng = TestRng::default();
        let (primary, _) = primary_without_handlers(&mut rng);

        // Check there is no batch currently proposed.
        assert!(primary.proposed_batch.read().is_none());

        // Generate a solution and a transaction.
        let (solution_id, solution) = sample_unconfirmed_solution(&mut rng);
        let (transaction_id, transaction) = sample_unconfirmed_transaction(&mut rng);

        // Store it on one of the workers.
        primary.workers()[0].process_unconfirmed_solution(solution_id, solution).await.unwrap();
        primary.workers()[0].process_unconfirmed_transaction(transaction_id, transaction).await.unwrap();

        // Set the proposal lock to a round ahead of the storage.
        let (old_proposal_round, old_proposal_timestamp) = primary
            .latest_proposal_timestamp
            .read()
            .await
            .map(|(round, timestamp)| (round, timestamp))
            .unwrap_or((0, 0));
        *primary.latest_proposal_timestamp.write().await =
            Some((round + 1, old_proposal_timestamp + MIN_BATCH_DELAY.as_secs() as i64));

        // Propose a batch and enforce that it fails.
        assert!(primary.propose_batch().await.is_ok());
        assert!(primary.proposed_batch.read().is_none());

        // Set the proposal lock back to the old round.
        *primary.latest_proposal_timestamp.write().await = Some((old_proposal_round, old_proposal_timestamp));

        // Try to propose a batch again. This time, it should succeed.
        assert!(primary.propose_batch().await.is_ok());
        assert!(primary.proposed_batch.read().is_proposed());
    }

    #[test_log::test(tokio::test)]
    async fn test_propose_batch_with_storage_round_behind_proposal() {
        let round = 5;
        let mut rng = TestRng::default();
        let (primary, accounts) = primary_without_handlers(&mut rng);

        // Generate previous certificates.
        let previous_certificates = store_certificate_chain(&primary, &accounts, round, &mut rng);

        // Create a valid proposal.
        let timestamp = now();
        let proposal = create_test_proposal(
            primary.gateway.account(),
            primary.ledger.current_committee().unwrap(),
            round + 1,
            previous_certificates,
            timestamp,
            1,
            &mut rng,
        );

        // Store the proposal on the primary.
        *primary.proposed_batch.write() = ProposedBatchState::Certifying(Box::new(proposal));

        // Try to propose a batch will terminate early because the storage is behind the proposal.
        assert!(primary.propose_batch().await.is_ok());
        assert!(primary.proposed_batch.read().is_proposed());
        assert!(primary.proposed_batch.read().as_proposal().unwrap().round() > primary.current_round());
    }

    #[test_log::test(tokio::test(flavor = "multi_thread"))]
    async fn test_batch_signature_from_peer() {
        let mut rng = TestRng::default();
        let (primary, accounts) = primary_without_handlers(&mut rng);
        map_account_addresses(&primary, &accounts);

        // Create a valid proposal.
        let round = 1;
        let timestamp = now() + MIN_BATCH_DELAY.as_secs() as i64;
        let proposal = create_test_proposal(
            primary.gateway.account(),
            primary.ledger.current_committee().unwrap(),
            round,
            Default::default(),
            timestamp,
            1,
            &mut rng,
        );

        // Store the proposal on the primary.
        *primary.proposed_batch.write() = ProposedBatchState::Certifying(Box::new(proposal));

        // Each committee member signs the batch.
        let signatures = peer_signatures_for_proposal(&primary, &accounts, &mut rng);

        // Have the primary process the signatures.
        for (socket_addr, signature) in signatures {
            primary.process_batch_signature_from_peer(socket_addr, signature).await.unwrap();
        }

        // Check the certificate was created and stored by the primary.
        assert!(primary.storage.contains_certificate_in_round_from(round, primary.gateway.account().address()));
        // Manually attempt round advancement (because the handler is not running).
        primary.try_increment_to_the_next_round(round + 1).await.unwrap();
        // Check the round was incremented.
        assert_eq!(primary.current_round(), round + 1);
    }

    #[test_log::test(tokio::test(flavor = "multi_thread"))]
    async fn test_batch_signature_from_peer_in_round() {
        let round = 5;
        let mut rng = TestRng::default();
        let (primary, accounts) = primary_without_handlers(&mut rng);
        map_account_addresses(&primary, &accounts);

        // Generate certificates.
        let previous_certificates = store_certificate_chain(&primary, &accounts, round, &mut rng);

        // Create a valid proposal.
        let timestamp = now();
        let proposal = create_test_proposal(
            primary.gateway.account(),
            primary.ledger.current_committee().unwrap(),
            round,
            previous_certificates,
            timestamp,
            1,
            &mut rng,
        );

        // Store the proposal on the primary.
        *primary.proposed_batch.write() = ProposedBatchState::Certifying(Box::new(proposal));

        // Each committee member signs the batch.
        let signatures = peer_signatures_for_proposal(&primary, &accounts, &mut rng);

        // Have the primary process the signatures.
        for (socket_addr, signature) in signatures {
            primary.process_batch_signature_from_peer(socket_addr, signature).await.unwrap();
        }

        // Check the certificate was created and stored by the primary.
        assert!(primary.storage.contains_certificate_in_round_from(round, primary.gateway.account().address()));
        // Manually attempt round advancement (because the handler is not running).
        primary.try_increment_to_the_next_round(round + 1).await.unwrap();
        // Check the round was incremented.
        assert_eq!(primary.current_round(), round + 1);
    }

    #[test_log::test(tokio::test)]
    async fn test_batch_signature_from_peer_no_quorum() {
        let mut rng = TestRng::default();
        let (primary, accounts) = primary_without_handlers(&mut rng);
        map_account_addresses(&primary, &accounts);

        // Create a valid proposal.
        let round = 1;
        let timestamp = now() + MIN_BATCH_DELAY.as_secs() as i64;
        let proposal = create_test_proposal(
            primary.gateway.account(),
            primary.ledger.current_committee().unwrap(),
            round,
            Default::default(),
            timestamp,
            1,
            &mut rng,
        );

        // Store the proposal on the primary.
        *primary.proposed_batch.write() = ProposedBatchState::Certifying(Box::new(proposal));

        // Each committee member signs the batch.
        let signatures = peer_signatures_for_proposal(&primary, &accounts, &mut rng);

        // Have the primary process only one signature, mimicking a lack of quorum.
        let (socket_addr, signature) = signatures.first().unwrap();
        primary.process_batch_signature_from_peer(*socket_addr, *signature).await.unwrap();

        // Check the certificate was not created and stored by the primary.
        assert!(!primary.storage.contains_certificate_in_round_from(round, primary.gateway.account().address()));
        // Check the round was incremented.
        assert_eq!(primary.current_round(), round);
    }

    #[test_log::test(tokio::test)]
    async fn test_batch_signature_from_peer_in_round_no_quorum() {
        let round = 7;
        let mut rng = TestRng::default();
        let (primary, accounts) = primary_without_handlers(&mut rng);
        map_account_addresses(&primary, &accounts);

        // Generate certificates.
        let previous_certificates = store_certificate_chain(&primary, &accounts, round, &mut rng);

        // Create a valid proposal.
        let timestamp = now() + MIN_BATCH_DELAY.as_secs() as i64;
        let proposal = create_test_proposal(
            primary.gateway.account(),
            primary.ledger.current_committee().unwrap(),
            round,
            previous_certificates,
            timestamp,
            1,
            &mut rng,
        );

        // Store the proposal on the primary.
        *primary.proposed_batch.write() = ProposedBatchState::Certifying(Box::new(proposal));

        // Each committee member signs the batch.
        let signatures = peer_signatures_for_proposal(&primary, &accounts, &mut rng);

        // Have the primary process only one signature, mimicking a lack of quorum.
        let (socket_addr, signature) = signatures.first().unwrap();
        primary.process_batch_signature_from_peer(*socket_addr, *signature).await.unwrap();

        // Check the certificate was not created and stored by the primary.
        assert!(!primary.storage.contains_certificate_in_round_from(round, primary.gateway.account().address()));
        // Check the round was incremented.
        assert_eq!(primary.current_round(), round);
    }

    // Tests that a late-arriving signature for a batch that is currently being certified
    // (ProposedBatchState::Certified) is silently dropped without error.
    // This exercises the race condition where proposed_batch.take() has been called but
    // insert_certificate has not yet completed.
    #[test_log::test(tokio::test)]
    async fn test_batch_signature_from_peer_batch_being_certified() {
        let mut rng = TestRng::default();
        let (primary, accounts) = primary_without_handlers(&mut rng);
        map_account_addresses(&primary, &accounts);

        // Create a valid proposal.
        let round = 1;
        let timestamp = now() + MIN_BATCH_DELAY.as_secs() as i64;
        let proposal = create_test_proposal(
            primary.gateway.account(),
            primary.ledger.current_committee().unwrap(),
            round,
            Default::default(),
            timestamp,
            1,
            &mut rng,
        );
        let batch_id = proposal.batch_id();

        // Simulate the race: the batch has been taken for certification but not yet stored.
        *primary.proposed_batch.write() = ProposedBatchState::Certified(batch_id);

        // Send a late signature for the batch being certified.
        let (socket_addr, account) =
            accounts.iter().find(|(_, a)| a.address() != primary.gateway.account().address()).unwrap();
        let signature = account.sign(&[batch_id], &mut rng).unwrap();
        let batch_signature = BatchSignature::new(batch_id, signature);

        // The signature should be accepted without error (silently dropped).
        assert!(primary.process_batch_signature_from_peer(*socket_addr, batch_signature).await.is_ok());
        // The batch state is unchanged (still BeingCertified — no new proposal was set).
        assert!(matches!(&*primary.proposed_batch.read(), ProposedBatchState::Certified(id) if *id == batch_id));
    }

    // Tests that a signature for a completely unknown batch ID is rejected even when another
    // batch is being certified. The BeingCertified state only suppresses errors for its own ID.
    #[test_log::test(tokio::test)]
    async fn test_batch_signature_from_peer_unknown_id_while_certifying() {
        let mut rng = TestRng::default();
        let (primary, accounts) = primary_without_handlers(&mut rng);
        map_account_addresses(&primary, &accounts);

        // Create two proposals so we have two distinct batch IDs.
        let round = 1;
        let timestamp = now() + MIN_BATCH_DELAY.as_secs() as i64;
        let proposal_a = create_test_proposal(
            primary.gateway.account(),
            primary.ledger.current_committee().unwrap(),
            round,
            Default::default(),
            timestamp,
            1,
            &mut rng,
        );
        let proposal_b = create_test_proposal(
            primary.gateway.account(),
            primary.ledger.current_committee().unwrap(),
            round,
            Default::default(),
            timestamp,
            1,
            &mut rng,
        );
        let batch_id_a = proposal_a.batch_id();
        let batch_id_b = proposal_b.batch_id();
        assert_ne!(batch_id_a, batch_id_b);

        // Simulate certifying batch A.
        *primary.proposed_batch.write() = ProposedBatchState::Certified(batch_id_a);

        // Send a signature for batch B (a genuinely unknown ID).
        let (socket_addr, account) =
            accounts.iter().find(|(_, a)| a.address() != primary.gateway.account().address()).unwrap();
        let signature = account.sign(&[batch_id_b], &mut rng).unwrap();
        let batch_signature = BatchSignature::new(batch_id_b, signature);

        // The signature is for a genuinely unknown ID — should be rejected with an error.
        assert!(primary.process_batch_signature_from_peer(*socket_addr, batch_signature).await.is_err());
    }

    // Tests the "already certified" path: a signature arrives after the batch is fully in
    // storage and the primary has moved on to a new proposal.
    #[test_log::test(tokio::test(flavor = "multi_thread"))]
    async fn test_batch_signature_from_peer_already_certified() {
        let mut rng = TestRng::default();
        let (primary, accounts) = primary_without_handlers(&mut rng);
        map_account_addresses(&primary, &accounts);

        // Create and certify a batch so it lands in storage.
        let round = 1;
        let timestamp = now() + MIN_BATCH_DELAY.as_secs() as i64;
        let old_proposal = create_test_proposal(
            primary.gateway.account(),
            primary.ledger.current_committee().unwrap(),
            round,
            Default::default(),
            timestamp,
            1,
            &mut rng,
        );
        let old_batch_id = old_proposal.batch_id();
        *primary.proposed_batch.write() = ProposedBatchState::Certifying(Box::new(old_proposal));
        let signatures = peer_signatures_for_proposal(&primary, &accounts, &mut rng);
        for (socket_addr, signature) in signatures {
            primary.process_batch_signature_from_peer(socket_addr, signature).await.unwrap();
        }
        // The batch is now in storage.
        assert!(primary.storage.contains_certificate_in_round_from(round, primary.gateway.account().address()));

        // Simulate a new proposal being active.
        let new_proposal = create_test_proposal(
            primary.gateway.account(),
            primary.ledger.current_committee().unwrap(),
            round,
            Default::default(),
            timestamp,
            1,
            &mut rng,
        );
        assert_ne!(new_proposal.batch_id(), old_batch_id);
        *primary.proposed_batch.write() = ProposedBatchState::Certifying(Box::new(new_proposal));

        // Send a late signature for the already-certified old batch.
        let (socket_addr, account) =
            accounts.iter().find(|(_, a)| a.address() != primary.gateway.account().address()).unwrap();
        let signature = account.sign(&[old_batch_id], &mut rng).unwrap();
        let batch_signature = BatchSignature::new(old_batch_id, signature);

        // Should be silently accepted (already certified path).
        assert!(primary.process_batch_signature_from_peer(*socket_addr, batch_signature).await.is_ok());
    }

    #[test_log::test(tokio::test)]
    async fn test_insert_certificate_with_aborted_transmissions() {
        let round = 3;
        let prev_round = round - 1;
        let mut rng = TestRng::default();
        let (primary, accounts) = primary_without_handlers(&mut rng);
        let peer_account = &accounts[1];
        let peer_ip = peer_account.0;

        // Fill primary storage.
        store_certificate_chain(&primary, &accounts, round, &mut rng);

        // Get transmissions from previous certificates.
        let previous_certificate_ids: IndexSet<_> = primary.storage.get_certificate_ids_for_round(prev_round);

        // Generate a solution and a transaction.
        let (solution_commitment, solution) = sample_unconfirmed_solution(&mut rng);
        let (transaction_id, transaction) = sample_unconfirmed_transaction(&mut rng);

        // Store it on one of the workers.
        primary.workers()[0].process_unconfirmed_solution(solution_commitment, solution).await.unwrap();
        primary.workers()[0].process_unconfirmed_transaction(transaction_id, transaction).await.unwrap();

        // Check that the worker has 2 transmissions.
        assert_eq!(primary.workers()[0].num_transmissions(), 2);

        // Create certificates for the current round.
        let account = accounts[0].1.clone();
        let (certificate, transmissions) =
            create_batch_certificate(account.address(), &accounts, round, previous_certificate_ids.clone(), &mut rng);
        let certificate_id = certificate.id();

        // Randomly abort some of the transmissions.
        let mut aborted_transmissions = HashSet::new();
        let mut transmissions_without_aborted = HashMap::new();
        for (transmission_id, transmission) in transmissions.clone() {
            match rng.random::<bool>() || aborted_transmissions.is_empty() {
                true => {
                    // Insert the aborted transmission.
                    aborted_transmissions.insert(transmission_id);
                }
                false => {
                    // Insert the transmission without the aborted transmission.
                    transmissions_without_aborted.insert(transmission_id, transmission);
                }
            };
        }

        // Add the non-aborted transmissions to the worker.
        for (transmission_id, transmission) in transmissions_without_aborted.iter() {
            primary.workers()[0].process_transmission_from_peer(peer_ip, *transmission_id, transmission.clone());
        }

        // Check that inserting the transmission with missing transmissions fails.
        assert!(
            primary
                .storage
                .check_certificate(&certificate, transmissions_without_aborted.clone(), Default::default())
                .is_err()
        );
        assert!(
            primary
                .storage
                .insert_certificate(certificate.clone(), transmissions_without_aborted.clone(), Default::default())
                .is_err()
        );

        // Insert the certificate to storage.
        primary
            .storage
            .insert_certificate(certificate, transmissions_without_aborted, aborted_transmissions.clone())
            .unwrap();

        // Ensure the certificate exists in storage.
        assert!(primary.storage.contains_certificate(certificate_id));
        // Ensure that the aborted transmission IDs exist in storage.
        for aborted_transmission_id in aborted_transmissions {
            assert!(primary.storage.contains_aborted_transmission(aborted_transmission_id));
            assert!(primary.storage.get_transmission(aborted_transmission_id).is_none());
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_sync_with_same_certificate_concurrently() {
        let round = 3;
        let mut rng = TestRng::default();
        let (primary, accounts) = primary_without_handlers(&mut rng);
        let peer_ip = accounts[1].0;

        // Fill primary storage.
        let previous_certificate_ids = store_certificate_chain(&primary, &accounts, round, &mut rng);

        for (_, account) in accounts.iter() {
            let (certificate, transmissions) = create_batch_certificate(
                account.address(),
                &accounts,
                round,
                previous_certificate_ids.clone(),
                &mut rng,
            );
            let certificate_id = certificate.id();
            for (transmission_id, transmission) in transmissions {
                primary.workers()[0].process_transmission_from_peer(peer_ip, transmission_id, transmission);
            }

            // Every concurrent attempt to store the same certificate must succeed.
            let tasks: Vec<_> = (0..8)
                .map(|_| {
                    let (primary, certificate) = (primary.clone(), certificate.clone());
                    tokio::spawn(async move { primary.sync_with_certificate_from_peer(peer_ip, certificate).await })
                })
                .collect();
            for task in tasks {
                task.await.unwrap().unwrap();
            }
            assert!(primary.storage.contains_certificate(certificate_id));
        }
    }

    // -----------------------------------------------------------------------
    // add_signature_to_batch
    // -----------------------------------------------------------------------

    /// State is `None` and the batch is not in storage — returns an error, state stays `None`.
    #[test]
    fn test_add_signature_to_batch_none_state() {
        let mut rng = TestRng::default();
        let (primary, accounts) = primary_without_handlers(&mut rng);

        let peer_ip = accounts[1].0;
        let batch_id = Field::rand(&mut rng);
        let signature = accounts[1].1.sign(&[batch_id], &mut rng).unwrap();

        let (result, new_state) =
            primary.add_signature_to_batch(ProposedBatchState::None, peer_ip, batch_id, signature);

        assert!(result.is_err());
        assert_eq!(new_state, ProposedBatchState::None);
    }

    /// State is `Certified` with a matching batch ID — silently dropped, state restored.
    #[test]
    fn test_add_signature_to_batch_certified_matching_id() {
        let mut rng = TestRng::default();
        let (primary, accounts) = primary_without_handlers(&mut rng);

        let peer_ip = accounts[1].0;
        let batch_id = Field::rand(&mut rng);
        let signature = accounts[1].1.sign(&[batch_id], &mut rng).unwrap();

        let (result, new_state) =
            primary.add_signature_to_batch(ProposedBatchState::Certified(batch_id), peer_ip, batch_id, signature);

        assert!(result.unwrap().is_none());
        assert_eq!(new_state, ProposedBatchState::Certified(batch_id));
    }

    /// State is `Certified` with a *different* batch ID — error returned, state becomes `None`.
    #[test]
    fn test_add_signature_to_batch_certified_different_id() {
        let mut rng = TestRng::default();
        let (primary, accounts) = primary_without_handlers(&mut rng);

        let peer_ip = accounts[1].0;
        let certified_id = Field::rand(&mut rng);
        let other_id = Field::rand(&mut rng);
        let signature = accounts[1].1.sign(&[other_id], &mut rng).unwrap();

        let (result, new_state) =
            primary.add_signature_to_batch(ProposedBatchState::Certified(certified_id), peer_ip, other_id, signature);

        assert!(result.is_err());
        assert_eq!(new_state, ProposedBatchState::Certified(certified_id));
    }

    /// State is `Certifying` for a *different* batch ID that **is already in storage** — silently
    /// dropped, state restored.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_add_signature_to_batch_certifying_different_id_in_storage() {
        let round = 1;
        let mut rng = TestRng::default();
        let (primary, accounts) = primary_without_handlers(&mut rng);
        map_account_addresses(&primary, &accounts);

        // Create a proposal owned by the primary.
        let proposal = create_test_proposal(
            primary.gateway.account(),
            primary.ledger.current_committee().unwrap(),
            round,
            Default::default(),
            now(),
            0,
            &mut rng,
        );
        let proposal_batch_id = proposal.batch_id();

        // Create and store a *different* certificate so `contains_batch` returns true for it.
        let (certificate, transmissions) =
            create_batch_certificate(accounts[1].1.address(), &accounts, round, Default::default(), &mut rng);
        let stored_batch_id = certificate.batch_id();
        primary.storage.insert_certificate(certificate, transmissions, Default::default()).unwrap();

        let peer_ip = accounts[1].0;
        let signature = accounts[1].1.sign(&[stored_batch_id], &mut rng).unwrap();

        let (result, new_state) = primary.add_signature_to_batch(
            ProposedBatchState::Certifying(Box::new(proposal)),
            peer_ip,
            stored_batch_id,
            signature,
        );

        assert!(result.unwrap().is_none());
        // State is restored with the original proposal.
        assert_eq!(new_state.as_proposal().unwrap().batch_id(), proposal_batch_id);
    }

    /// State is `Certifying` for a *different* batch ID that is **not in storage** — error
    /// returned, state restored.
    #[test]
    fn test_add_signature_to_batch_certifying_different_id_unknown() {
        let mut rng = TestRng::default();
        let (primary, accounts) = primary_without_handlers(&mut rng);

        let proposal = create_test_proposal(
            primary.gateway.account(),
            primary.ledger.current_committee().unwrap(),
            1,
            Default::default(),
            now(),
            0,
            &mut rng,
        );
        let proposal_batch_id = proposal.batch_id();

        let peer_ip = accounts[1].0;
        let unknown_id = Field::rand(&mut rng);
        let signature = accounts[1].1.sign(&[unknown_id], &mut rng).unwrap();

        let (result, new_state) = primary.add_signature_to_batch(
            ProposedBatchState::Certifying(Box::new(proposal)),
            peer_ip,
            unknown_id,
            signature,
        );

        assert!(result.is_err());
        assert_eq!(new_state.as_proposal().unwrap().batch_id(), proposal_batch_id);
    }

    /// Matching batch ID, valid signature, quorum **not yet** reached — state stays `Certifying`.
    #[test]
    fn test_add_signature_to_batch_certifying_matching_no_quorum() {
        let mut rng = TestRng::default();
        let (primary, accounts) = primary_without_handlers(&mut rng);
        map_account_addresses(&primary, &accounts);

        let proposal = create_test_proposal(
            primary.gateway.account(),
            primary.ledger.current_committee().unwrap(),
            1,
            Default::default(),
            now(),
            0,
            &mut rng,
        );
        let batch_id = proposal.batch_id();

        // Only one peer signs — not enough for quorum.
        let peer_ip = accounts[1].0;
        let signature = accounts[1].1.sign(&[batch_id], &mut rng).unwrap();

        let (result, new_state) = primary.add_signature_to_batch(
            ProposedBatchState::Certifying(Box::new(proposal)),
            peer_ip,
            batch_id,
            signature,
        );

        assert!(result.unwrap().is_none());
        assert_eq!(new_state.as_proposal().unwrap().batch_id(), batch_id);
    }

    /// Matching batch ID, all peers sign — quorum reached, proposal extracted and state becomes
    /// `Certified`.
    #[test]
    fn test_add_signature_to_batch_certifying_matching_quorum_reached() {
        let mut rng = TestRng::default();
        let (primary, accounts) = primary_without_handlers(&mut rng);
        map_account_addresses(&primary, &accounts);

        let proposal = create_test_proposal(
            primary.gateway.account(),
            primary.ledger.current_committee().unwrap(),
            1,
            Default::default(),
            now(),
            0,
            &mut rng,
        );
        let batch_id = proposal.batch_id();

        // Add all peer signatures one by one until quorum is reached.
        let peers: Vec<_> =
            accounts.iter().filter(|(_, a)| a.address() != primary.gateway.account().address()).collect();
        let mut state = ProposedBatchState::Certifying(Box::new(proposal));
        let mut final_result = None;

        for (peer_ip, peer_account) in &peers {
            let signature = peer_account.sign(&[batch_id], &mut rng).unwrap();
            let (result, new_state) = primary.add_signature_to_batch(state, *peer_ip, batch_id, signature);
            state = new_state;
            if result.as_ref().unwrap().is_some() {
                final_result = Some(result);
                break;
            }
        }

        // Quorum must have been reached with the committee's peers.
        let proposal = final_result.expect("quorum should be reached").unwrap().unwrap();
        assert_eq!(proposal.batch_id(), batch_id);
        assert_eq!(state, ProposedBatchState::Certified(batch_id));
    }
}
