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
    MAX_LEADER_CERTIFICATE_DELAY,
    helpers::{ConsensusSender, DAG, PrimaryReceiver, PrimarySender, Storage, fmt_id, now},
    primary::{Primary, PrimaryCallback},
    sync::SyncCallback,
};

use snarkos_account::Account;
use snarkos_node_bft_ledger_service::LedgerService;
use snarkos_node_sync::{BlockSync, Ping};
use snarkos_utilities::NodeDataDir;

use snarkvm::{
    console::account::Address,
    ledger::{
        block::Transaction,
        committee::Committee,
        narwhal::{BatchCertificate, Data, Subdag, Transmission, TransmissionID},
        puzzle::{Solution, SolutionID},
    },
    prelude::{Field, Network, Result, bail, ensure},
    utilities::flatten_error,
};

use anyhow::Context;
use indexmap::{IndexMap, IndexSet};
#[cfg(feature = "locktick")]
use locktick::parking_lot::RwLock;
#[cfg(feature = "locktick")]
use locktick::tokio::Mutex;
#[cfg(not(feature = "locktick"))]
use parking_lot::RwLock;
use std::{
    collections::{BTreeMap, HashSet},
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicI64, Ordering},
    },
};
#[cfg(not(feature = "locktick"))]
use tokio::sync::Mutex;
use tokio::sync::{OnceCell, oneshot};

#[derive(Clone)]
pub struct BFT<N: Network> {
    /// The primary for this node.
    primary: Primary<N>,
    /// The DAG of batches from which we build the blockchain.
    dag: Arc<RwLock<DAG<N>>>,
    /// The batch certificate of the leader from the current even round, if one was present.
    leader_certificate: Arc<RwLock<Option<BatchCertificate<N>>>>,
    /// The timer for the leader certificate to be received.
    leader_certificate_timer: Arc<AtomicI64>,
    /// The consensus sender.
    consensus_sender: Arc<OnceCell<ConsensusSender<N>>>,
    /// Ensures only one call to `commit_leader_certificate` runs at a time.
    ///
    /// Without this, a second certificate crossing the availability threshold while the consensus
    /// callback for a prior commit is still in-flight would re-walk already-committed rounds
    /// (because `last_committed_round` hasn't been updated yet), causing duplicate subdag commits.
    commit_lock: Arc<Mutex<()>>,
}

impl<N: Network> BFT<N> {
    /// Initializes a new instance of the BFT.
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
        Ok(Self {
            primary: Primary::new(
                account,
                storage,
                ledger,
                block_sync,
                ip,
                trusted_validators,
                trusted_peers_only,
                node_data_dir,
                dev,
                proposal_spent_limit_multiplier,
            )?,
            dag: Default::default(),
            leader_certificate: Default::default(),
            leader_certificate_timer: Default::default(),
            consensus_sender: Default::default(),
            commit_lock: Default::default(),
        })
    }

    /// Run the BFT instance.
    ///
    /// This will return as soon as all required tasks are spawned.
    /// The function must not be called more than once per instance.
    pub async fn run(
        &mut self,
        ping: Option<Arc<Ping<N>>>,
        consensus_sender: Option<ConsensusSender<N>>,
        primary_sender: PrimarySender<N>,
        primary_receiver: PrimaryReceiver<N>,
    ) -> Result<()> {
        info!("Starting the BFT instance...");
        // Set up callbacks.
        let primary_callback = Some(Arc::new(self.clone()) as Arc<dyn PrimaryCallback<N>>);

        let sync_callback = Some(Arc::new(self.clone()) as Arc<dyn SyncCallback<N>>);

        // Next, run the primary instance.
        self.primary.run(ping, primary_callback, sync_callback, primary_sender, primary_receiver).await?;

        // Lastly, set the consensus sender.
        // Note: This ensures that the BFT does not advance the ledger during initial syncing.
        if let Some(consensus_sender) = consensus_sender {
            self.consensus_sender.set(consensus_sender).expect("Consensus sender already set");
        }
        Ok(())
    }

    /// Returns `true` if the primary is synced.
    pub fn is_synced(&self) -> bool {
        self.primary.is_synced()
    }

    /// Returns the primary.
    pub const fn primary(&self) -> &Primary<N> {
        &self.primary
    }

    /// Returns the storage.
    pub const fn storage(&self) -> &Storage<N> {
        self.primary.storage()
    }

    /// Returns the ledger.
    pub fn ledger(&self) -> &Arc<dyn LedgerService<N>> {
        self.primary.ledger()
    }

    /// Returns the leader of the current even round, if one was present.
    pub fn leader(&self) -> Option<Address<N>> {
        self.leader_certificate.read().as_ref().map(|certificate| certificate.author())
    }

    /// Returns the certificate of the leader from the current even round, if one was present.
    pub const fn leader_certificate(&self) -> &Arc<RwLock<Option<BatchCertificate<N>>>> {
        &self.leader_certificate
    }
}

impl<N: Network> BFT<N> {
    /// Returns the number of unconfirmed transmissions.
    pub fn num_unconfirmed_transmissions(&self) -> usize {
        self.primary.num_unconfirmed_transmissions()
    }

    /// Returns the number of unconfirmed ratifications.
    pub fn num_unconfirmed_ratifications(&self) -> usize {
        self.primary.num_unconfirmed_ratifications()
    }

    /// Returns the number of solutions.
    pub fn num_unconfirmed_solutions(&self) -> usize {
        self.primary.num_unconfirmed_solutions()
    }

    /// Returns the number of unconfirmed transactions.
    pub fn num_unconfirmed_transactions(&self) -> usize {
        self.primary.num_unconfirmed_transactions()
    }
}

impl<N: Network> BFT<N> {
    /// Returns the worker transmission IDs.
    pub fn worker_transmission_ids(&self) -> impl '_ + Iterator<Item = TransmissionID<N>> {
        self.primary.worker_transmission_ids()
    }

    /// Returns the worker transmissions.
    pub fn worker_transmissions(&self) -> impl '_ + Iterator<Item = (TransmissionID<N>, Transmission<N>)> {
        self.primary.worker_transmissions()
    }

    /// Returns the worker solutions.
    pub fn worker_solutions(&self) -> impl '_ + Iterator<Item = (SolutionID<N>, Data<Solution<N>>)> {
        self.primary.worker_solutions()
    }

    /// Returns the worker transactions.
    pub fn worker_transactions(&self) -> impl '_ + Iterator<Item = (N::TransactionID, Data<Transaction<N>>)> {
        self.primary.worker_transactions()
    }
}

#[async_trait::async_trait]
impl<N: Network> PrimaryCallback<N> for BFT<N> {
    /// Notification that a new round has started.
    ///
    /// # Arguments
    /// * `current_round` - the round the caller is in (to avoid race conditions)
    ///
    /// # Returns
    /// `true` if the BFT moved to the next round.
    fn try_advance_to_next_round(&self, current_round: u64) -> bool {
        // Ensure the current round is at least the storage round (this is a sanity check).
        let storage_round = self.storage().current_round();
        if current_round < storage_round {
            debug!(
                "BFT is safely skipping an update for round {current_round}, as storage is at round {storage_round}"
            );
            return false;
        }

        // Determine if the BFT is ready to update to the next round.
        let is_ready = match current_round.is_multiple_of(2) {
            true => self.update_leader_certificate_to_even_round(current_round),
            false => self.is_leader_quorum_or_nonleaders_available(current_round),
        };

        // Log whether the round is going to update.
        if current_round.is_multiple_of(2) {
            // Determine if there is a leader certificate.
            if let Some(leader_certificate) = self.leader_certificate.read().as_ref() {
                // Ensure the state of the leader certificate is consistent with the BFT being ready.
                if !is_ready {
                    trace!(is_ready, "BFT - A leader certificate was found, but 'is_ready' is false");
                }
                // Log the leader election.
                let leader_round = leader_certificate.round();
                match leader_round == current_round {
                    true => {
                        info!("Round {current_round} elected a leader - {}", leader_certificate.author());
                        #[cfg(feature = "metrics")]
                        metrics::increment_counter(metrics::bft::LEADERS_ELECTED);
                    }
                    false => warn!("BFT failed to elect a leader for round {current_round} (!= {leader_round})"),
                }
            } else {
                match is_ready {
                    true => info!("Round {current_round} reached quorum without a leader"),
                    false => info!(dim = true, "Round {current_round} did not elect a leader (yet)"),
                }
            }
        }

        // If the BFT is ready, then update to the next round.
        if is_ready {
            // Update to the next round in storage.
            if let Err(err) = self
                .storage()
                .increment_to_next_round(current_round)
                .with_context(|| format!("BFT failed to increment to the next round from round {current_round}"))
            {
                warn!("{}", &flatten_error(err));
                return false;
            }

            #[cfg(feature = "metrics")]
            {
                let start = self.leader_certificate_timer.load(Ordering::SeqCst);
                // Only log if the timer was set, otherwise we get a time difference since the EPOCH.
                if start > 0 {
                    let end = now();
                    let elapsed = std::time::Duration::from_secs((end - start) as u64);
                    metrics::histogram(metrics::bft::COMMIT_ROUNDS_LATENCY, elapsed.as_secs_f64());
                }
            }

            // Update the timer for the leader certificate.
            self.leader_certificate_timer.store(now(), Ordering::SeqCst);
        }

        is_ready
    }

    /// Notification about a new certificated generated by `Primary` or received by the `Primary` from a peer.
    async fn add_new_certificate(&self, certificate: BatchCertificate<N>) -> Result<()> {
        // ### First, insert the certificate into the DAG. ###
        // Retrieve the round of the new certificate to add to the DAG.
        let certificate_round = certificate.round();

        // Insert the certificate into the DAG.
        self.dag.write().insert(certificate);

        // ### Second, determine if a new leader certificate can be committed. ###
        let commit_round = certificate_round.saturating_sub(1);

        // Leaders are elected in even rounds.
        // If the previous round is odd, the current round cannot commit any leader certs.
        // Similarly, no leader certificate can be committed for round zero.
        if !commit_round.is_multiple_of(2) || commit_round < 2 {
            return Ok(());
        }
        // If the commit round is at or below the last committed round, return early.
        if commit_round <= self.dag.read().last_committed_round() {
            return Ok(());
        }

        /* Proceeding to check if the leader is ready to be committed. */
        trace!("Checking if the leader is ready to be committed for round {commit_round}...");

        // Retrieve the committee lookback for the commit round.
        let committee_lookback = self.ledger().get_committee_lookback_for_round(commit_round).with_context(|| {
            format!("BFT failed to retrieve the committee with lag for commit round {commit_round}")
        })?;

        // Either retrieve the cached leader or compute it.
        let leader = match self.ledger().latest_leader() {
            Some((cached_round, cached_leader)) if cached_round == commit_round => cached_leader,
            _ => {
                // Compute the leader for the commit round.
                let computed_leader = committee_lookback
                    .get_leader(commit_round)
                    .with_context(|| format!("BFT failed to compute the leader for commit round {commit_round}"))?;

                // Cache the computed leader.
                self.ledger().update_latest_leader(commit_round, computed_leader);

                computed_leader
            }
        };

        // Retrieve the leader certificate for the commit round.
        let Some(leader_certificate) = self.dag.read().get_certificate_for_round_with_author(commit_round, leader)
        else {
            trace!("BFT did not find the leader certificate for commit round {commit_round} yet");
            return Ok(());
        };
        // Retrieve all of the certificates for the **certificate** round.
        let certificates = self.dag.read().get_certificates_for_round(certificate_round).with_context(|| {
            format!("BFT failed to retrieve the certificates for certificate round {certificate_round}")
        })?;

        // Retrieve the committee lookback for the certificate round (i.e. the round just after the commit round).
        let certificate_committee_lookback =
            self.ledger().get_committee_lookback_for_round(certificate_round).with_context(|| {
                format!("BFT failed to retrieve the committee lookback for certificate round {certificate_round}")
            })?;

        // Construct a set over the authors who included the leader's certificate in the certificate round.
        let authors = certificates
            .values()
            .filter_map(|c| match c.previous_certificate_ids().contains(&leader_certificate.id()) {
                true => Some(c.author()),
                false => None,
            })
            .collect();

        // Check if the leader is ready to be committed.
        if certificate_committee_lookback.is_availability_threshold_reached(&authors) {
            info!("Proceeding to commit round {commit_round} with leader '{}'", fmt_id(leader));
            self.commit_leader_certificate(leader_certificate).await
        } else {
            trace!(
                "BFT is not ready to commit round {commit_round} with leader '{}' - Availability threshold has not been reached yet",
                fmt_id(leader)
            );
            Ok(())
        }
    }
}

#[async_trait::async_trait]
impl<N: Network> SyncCallback<N> for BFT<N> {
    // Notification about a new certificate detected by the `Sync` instance after fetching a new block.
    fn add_certificate_from_sync(&self, certificate: BatchCertificate<N>) {
        self.dag.write().insert(certificate);
    }

    // Commits a certificate into the DAG.
    fn commit_certificate_from_sync(&self, certificate: &BatchCertificate<N>) {
        self.dag.write().commit(certificate, self.storage().max_gc_rounds());
    }
}

impl<N: Network> BFT<N> {
    /// Updates the leader certificate to the current even round,
    /// returning `true` if the BFT is ready to update to the next round.
    ///
    /// This method runs on every even round, by determining the leader of the current even round,
    /// and setting the leader certificate to their certificate in the round, if they were present.
    fn update_leader_certificate_to_even_round(&self, even_round: u64) -> bool {
        // Retrieve the current round.
        let current_round = self.storage().current_round();
        // Ensure the current round matches the given round.
        if current_round != even_round {
            warn!("BFT storage (at round {current_round}) is out of sync with the current even round {even_round}");
            return false;
        }

        // If the current round is odd, return false.
        if !current_round.is_multiple_of(2) || current_round < 2 {
            error!("BFT cannot update the leader certificate in an odd round");
            return false;
        }

        // Retrieve the certificates for the current round.
        let current_certificates = self.storage().get_certificates_for_round(current_round);
        // If there are no current certificates, set the leader certificate to 'None', and return early.
        if current_certificates.is_empty() {
            // Set the leader certificate to 'None'.
            *self.leader_certificate.write() = None;
            return false;
        }

        // Retrieve the committee lookback of the current round.
        let committee_lookback = match self.ledger().get_committee_lookback_for_round(current_round) {
            Ok(committee) => committee,
            Err(err) => {
                let err = err.context(format!(
                    "BFT failed to retrieve the committee lookback for the even round {current_round}"
                ));
                warn!("{}", &flatten_error(err));
                return false;
            }
        };
        // Determine the leader of the current round.
        let leader = match self.ledger().latest_leader() {
            Some((cached_round, cached_leader)) if cached_round == current_round => cached_leader,
            _ => {
                // Compute the leader for the current round.
                let computed_leader = match committee_lookback.get_leader(current_round) {
                    Ok(leader) => leader,
                    Err(err) => {
                        let err =
                            err.context(format!("BFT failed to compute the leader for the even round {current_round}"));
                        error!("{}", &flatten_error(err));
                        return false;
                    }
                };

                // Cache the computed leader.
                self.ledger().update_latest_leader(current_round, computed_leader);

                computed_leader
            }
        };
        // Find and set the leader certificate, if the leader was present in the current even round.
        let leader_certificate = current_certificates.iter().find(|certificate| certificate.author() == leader);
        *self.leader_certificate.write() = leader_certificate.cloned();

        self.is_even_round_ready_for_next_round(current_certificates, committee_lookback, current_round)
    }

    /// Returns 'true' if the quorum threshold `(N - f)` is reached for this round under one of the following conditions:
    ///  - If the leader certificate is set for the current even round.
    ///  - The timer for the leader certificate has expired.
    fn is_even_round_ready_for_next_round(
        &self,
        certificates: IndexSet<BatchCertificate<N>>,
        committee: Committee<N>,
        current_round: u64,
    ) -> bool {
        // Retrieve the authors for the current round.
        let authors = certificates.into_iter().map(|c| c.author()).collect();
        // Check if quorum threshold is reached.
        if !committee.is_quorum_threshold_reached(&authors) {
            trace!("BFT failed to reach quorum threshold in even round {current_round}");
            return false;
        }
        // If the leader certificate is set for the current even round, return 'true'.
        if let Some(leader_certificate) = self.leader_certificate.read().as_ref()
            && leader_certificate.round() == current_round
        {
            return true;
        }
        // If the timer has expired, and we can achieve quorum threshold (N - f) without the leader, return 'true'.
        if self.is_timer_expired() {
            debug!("BFT (timer expired) - Advancing from round {current_round} to the next round (without the leader)");
            return true;
        }
        // Otherwise, return 'false'.
        false
    }

    /// Returns `true` if the timer for the leader certificate has expired.
    ///
    /// This is always true for a new BFT instance.
    fn is_timer_expired(&self) -> bool {
        self.leader_certificate_timer.load(Ordering::SeqCst) + MAX_LEADER_CERTIFICATE_DELAY.as_secs() as i64 <= now()
    }

    /// Returns 'true' if the quorum threshold `(N - f)` is reached for this round under one of the following conditions:
    ///  - The leader certificate is `None`.
    ///  - The leader certificate is not included up to availability threshold `(f + 1)` (in the previous certificates of the current round).
    ///  - The leader certificate timer has expired.
    fn is_leader_quorum_or_nonleaders_available(&self, odd_round: u64) -> bool {
        // Retrieve the current round.
        let current_round = self.storage().current_round();
        // Ensure the current round matches the given round.
        if current_round != odd_round {
            warn!("BFT storage (at round {current_round}) is out of sync with the current odd round {odd_round}");
            return false;
        }
        // If the current round is even, return false.
        if current_round % 2 != 1 {
            error!("BFT does not compute stakes for the leader certificate in an even round");
            return false;
        }
        // Retrieve the certificates for the current round.
        let current_certificates = self.storage().get_certificates_for_round(current_round);
        // Retrieve the committee lookback for the current round.
        let committee_lookback = match self.ledger().get_committee_lookback_for_round(current_round) {
            Ok(committee) => committee,
            Err(err) => {
                let err = err.context(format!(
                    "BFT failed to retrieve the committee lookback for the odd round {current_round}"
                ));
                error!("{}", &flatten_error(err));
                return false;
            }
        };
        // Retrieve the authors of the current certificates.
        let authors = current_certificates.clone().into_iter().map(|c| c.author()).collect();
        // Check if quorum threshold is reached.
        if !committee_lookback.is_quorum_threshold_reached(&authors) {
            trace!("BFT failed reach quorum threshold in odd round {current_round}.");
            return false;
        }
        // Retrieve the leader certificate.
        let Some(leader_certificate) = self.leader_certificate.read().clone() else {
            // If there is no leader certificate for the previous round, return 'true'.
            return true;
        };
        // Compute the stake for the leader certificate.
        let (stake_with_leader, stake_without_leader) = self.compute_stake_for_leader_certificate(
            leader_certificate.id(),
            current_certificates,
            &committee_lookback,
        );
        // Return 'true' if any of the following conditions hold:
        stake_with_leader >= committee_lookback.availability_threshold()
            || stake_without_leader >= committee_lookback.quorum_threshold()
            || self.is_timer_expired()
    }

    /// Computes the amount of stake that has & has not signed for the leader certificate.
    fn compute_stake_for_leader_certificate(
        &self,
        leader_certificate_id: Field<N>,
        current_certificates: IndexSet<BatchCertificate<N>>,
        current_committee: &Committee<N>,
    ) -> (u64, u64) {
        // If there are no current certificates, return early.
        if current_certificates.is_empty() {
            return (0, 0);
        }

        // Initialize a tracker for the stake with the leader.
        let mut stake_with_leader = 0u64;
        // Initialize a tracker for the stake without the leader.
        let mut stake_without_leader = 0u64;
        // Iterate over the current certificates.
        for certificate in current_certificates {
            // Retrieve the stake for the author of the certificate.
            let stake = current_committee.get_stake(certificate.author());
            // Determine if the certificate includes the leader.
            match certificate.previous_certificate_ids().iter().any(|id| *id == leader_certificate_id) {
                // If the certificate includes the leader, add the stake to the stake with the leader.
                true => stake_with_leader = stake_with_leader.saturating_add(stake),
                // If the certificate does not include the leader, add the stake to the stake without the leader.
                false => stake_without_leader = stake_without_leader.saturating_add(stake),
            }
        }
        // Return the stake with the leader, and the stake without the leader.
        (stake_with_leader, stake_without_leader)
    }
}

impl<N: Network> BFT<N> {
    /// Commits the leader certificate, and all previous leader certificates since the last committed round.
    async fn commit_leader_certificate(&self, leader_certificate: BatchCertificate<N>) -> Result<()> {
        #[cfg(feature = "metrics")]
        let start = std::time::Instant::now();
        #[cfg(debug_assertions)]
        trace!("Attempting to commit leader certificate for round {}...", leader_certificate.round());

        // Serialize all commits so that `last_committed_round` is up-to-date before the next call
        // re-walks the DAG, preventing duplicate subdag commits.
        let _commit_guard = self.commit_lock.lock().await;

        // Fetch the leader round.
        let latest_leader_round = leader_certificate.round();

        // Determine the list of all previous leader certificates since the last committed round.
        // The order of the leader certificates is from **newest** to **oldest**.
        let mut leader_certificates = vec![leader_certificate.clone()];
        // Whether the consensus callback should be skipped (true when the round is already committed).
        // When `latest_leader_round == last_committed_round` the round was already committed by a
        // concurrent call that beat us to the lock, or by a prior session whose DAG state was
        // reconstructed without populating `recently_committed`.  In both cases we still re-run
        // DFS + GC to ensure `recently_committed` and `gc_round` are populated, but we must NOT
        // send a duplicate subdag to the consensus callback.
        let skip_consensus;
        {
            // Read-lock the DAG.
            // We need to hold the lock, so we do not later fail to re-acquire it.
            let dag = self.dag.read();

            // Re-check under the lock: another call may have committed this round while we were waiting.
            if latest_leader_round < dag.last_committed_round() {
                trace!("Skipping already-committed leader round {latest_leader_round}");
                return Ok(());
            }
            skip_consensus = latest_leader_round == dag.last_committed_round();

            #[cfg(debug_assertions)]
            trace!("Attempting to commit leader certificate for round {}...", latest_leader_round);

            let mut current_certificate = leader_certificate;
            for round in (dag.last_committed_round() + 2..=latest_leader_round.saturating_sub(2)).rev().step_by(2) {
                // Retrieve the previous committee for the leader round.
                let previous_committee_lookback =
                    self.ledger().get_committee_lookback_for_round(round).with_context(|| {
                        format!("BFT failed to retrieve a previous committee lookback for the even round {round}")
                    })?;

                // Either retrieve the cached leader or compute it.
                let leader = match self.ledger().latest_leader() {
                    Some((cached_round, cached_leader)) if cached_round == round => cached_leader,
                    _ => {
                        // Compute the leader for the commit round.
                        let computed_leader = previous_committee_lookback
                            .get_leader(round)
                            .with_context(|| format!("BFT failed to compute the leader for the even round {round}"))?;

                        // Cache the computed leader.
                        self.ledger().update_latest_leader(round, computed_leader);

                        computed_leader
                    }
                };
                // Retrieve the previous leader certificate.
                let Some(previous_certificate) = dag.get_certificate_for_round_with_author(round, leader) else {
                    continue;
                };
                // Determine if there is a path between the previous certificate and the current certificate.
                if dag.is_linked(previous_certificate.clone(), current_certificate.clone())? {
                    // Add the previous leader certificate to the list of certificates to commit.
                    leader_certificates.push(previous_certificate.clone());
                    // Update the current certificate to the previous leader certificate.
                    current_certificate = previous_certificate;
                } else {
                    #[cfg(debug_assertions)]
                    trace!(
                        "Skipping anchor for round {round} as it is not linked to the most recent committed leader certificate"
                    );
                }
            }
        }

        // Iterate over the leader certificates to commit.
        for leader_certificate in leader_certificates.into_iter().rev() {
            // Retrieve the leader certificate round.
            let leader_round = leader_certificate.round();
            // Compute the commit subdag.
            let commit_subdag =
                self.order_dag_with_dfs(leader_certificate).with_context(|| "BFT failed to order the DAG with DFS")?;
            // Initialize a map for the deduped transmissions.
            let mut transmissions = IndexMap::new();
            // Initialize a map for the deduped transaction ids.
            let mut seen_transaction_ids = IndexSet::new();
            // Initialize a map for the deduped solution ids.
            let mut seen_solution_ids = IndexSet::new();
            // Start from the oldest leader certificate.
            for certificate in commit_subdag.values().flatten() {
                // Retrieve the transmissions.
                for transmission_id in certificate.transmission_ids() {
                    // If the transaction ID or solution ID already exists in the map, skip it.
                    // Note: This additional check is done to ensure that we do not include duplicate
                    // transaction IDs or solution IDs that may have a different transmission ID.
                    match transmission_id {
                        TransmissionID::Solution(solution_id, _) => {
                            // If the solution already exists, skip it.
                            if seen_solution_ids.contains(&solution_id) {
                                continue;
                            }
                        }
                        TransmissionID::Transaction(transaction_id, _) => {
                            // If the transaction already exists, skip it.
                            if seen_transaction_ids.contains(transaction_id) {
                                continue;
                            }
                        }
                        TransmissionID::Ratification => {
                            bail!("Ratifications are currently not supported in the BFT.")
                        }
                    }
                    // If the transmission already exists in the map, skip it.
                    if transmissions.contains_key(transmission_id) {
                        continue;
                    }
                    // If the transmission already exists in the ledger, skip it.
                    // Note: On failure to read from the ledger, we skip including this transmission, out of safety.
                    if self.ledger().contains_transmission(transmission_id).unwrap_or(true) {
                        continue;
                    }
                    // Retrieve the transmission.
                    // Note: If this fails, we have to fetch the block/certificate again. As there is no logic here to sync the transmission.
                    let transmission = self.storage().get_transmission(*transmission_id).with_context(|| {
                        format!(
                            "BFT failed to retrieve transmission '{}.{}' from round {}",
                            fmt_id(transmission_id),
                            fmt_id(transmission_id.checksum().unwrap_or_default()),
                            certificate.round()
                        )
                    })?;
                    // Insert the transaction ID or solution ID into the map.
                    match transmission_id {
                        TransmissionID::Solution(id, _) => {
                            seen_solution_ids.insert(id);
                        }
                        TransmissionID::Transaction(id, _) => {
                            seen_transaction_ids.insert(id);
                        }
                        TransmissionID::Ratification => {}
                    }
                    // Add the transmission to the set.
                    transmissions.insert(*transmission_id, transmission);
                }
            }
            // Trigger consensus, as this will build a new block for the ledger.
            // Construct the subdag.
            let subdag = Subdag::from(commit_subdag.clone())?;
            // Retrieve the anchor round.
            let anchor_round = subdag.anchor_round();
            // Retrieve the number of transmissions.
            let num_transmissions = transmissions.len();
            // Retrieve metadata about the subdag.
            let subdag_metadata = subdag.iter().map(|(round, c)| (*round, c.len())).collect::<Vec<_>>();

            // Ensure the subdag anchor round matches the leader round.
            ensure!(
                anchor_round == leader_round,
                "BFT failed to commit - the subdag anchor round {anchor_round} does not match the leader round {leader_round}",
            );

            let mut ledger_already_advanced = false;

            // Trigger consensus (skipped if the round was already committed by a prior call).
            if !skip_consensus && let Some(consensus_sender) = self.consensus_sender.get() {
                // Initialize a callback sender and receiver.
                let (callback_sender, callback_receiver) = oneshot::channel();

                // Send the subdag and transmissions to consensus.
                consensus_sender.tx_consensus_subdag.send((subdag, transmissions, callback_sender)).await?;

                // Await the callback to continue.
                match callback_receiver.await {
                    Ok(Ok(true)) => (),
                    Ok(Ok(false)) => ledger_already_advanced = true,
                    Ok(Err(err)) => {
                        let err = err.context(format!("BFT failed to advance the subdag for round {anchor_round}"));
                        error!("{}", &flatten_error(err));
                        return Ok(());
                    }
                    Err(err) => {
                        let err: anyhow::Error = err.into();
                        let err = err.context(format!("BFT failed to receive the callback for round {anchor_round}"));
                        warn!("{}", flatten_error(err));
                        return Ok(());
                    }
                }

                // Update subdag/DAG-density metrics.
                #[cfg(feature = "metrics")]
                {
                    metrics::histogram(metrics::bft::SUBDAG_ROUNDS_PER_BLOCK, subdag_metadata.len() as f64);
                    for (_, certs_in_round) in &subdag_metadata {
                        metrics::histogram(metrics::bft::SUBDAG_CERTIFICATES_PER_ROUND, *certs_in_round as f64);
                    }
                    for certificate in commit_subdag.values().flatten() {
                        metrics::histogram(
                            metrics::bft::SUBDAG_CERTIFICATE_SIGNATURES,
                            certificate.signatures().len() as f64,
                        );
                        metrics::histogram(
                            metrics::bft::SUBDAG_CERTIFICATE_PREVIOUS_REFS,
                            certificate.previous_certificate_ids().len() as f64,
                        );
                    }
                }
            }

            if ledger_already_advanced {
                debug!(
                    "Committing a subDAG with anchor round {anchor_round} to the DAG only, as the ledger already contains it",
                );
            } else {
                info!(
                    "Committing a subDAG with anchor round {anchor_round} and {num_transmissions} transmissions: {subdag_metadata:?}",
                );
            }

            // Update the DAG, as the ledger contains a block for this subdag.
            {
                let mut dag_write = self.dag.write();
                let mut count = 0;
                for certificate in commit_subdag.values().flatten() {
                    dag_write.commit(certificate, self.storage().max_gc_rounds());
                    count += 1;
                }

                trace!("Committed {count} certificates to the DAG");
            }

            // Update the validator telemetry.
            #[cfg(feature = "metrics")]
            self.primary().gateway().validator_telemetry().insert_subdag(&Subdag::from(commit_subdag)?);
        }

        // Perform garbage collection based on the latest committed leader round.
        // The protocol guarantees that validators commit the same anchors in the same order,
        // but they may do so in different chunks of anchors,
        // where 'chunk' refers to the vector of certificates that the loop just above iterates over.
        // Doing garbage collection at the end of each chunk (as we do here),
        // as opposed to after each certificate in the chunk (if we moved this call inside the loop, at the end),
        // may give raise to a discrepancy between the DAGs of different validators who commit different chunks:
        // one validator may have more certificates than the other, not yet garbage collected.
        // However, when `order_dag_with_dfs()` collects the sub-DAG to commit from an anchor,
        // it excludes certificates that are below the GC round,
        // so the possible aforementioned discrepancy between DAGs should not affect the consensus.
        // That exclusion in `order_dag_with_dfs()` is critical to prevent forking,
        // so long as garbage collection is done after each chunk.
        // If garbage collection were done after each committed certificate,
        // that exclusion in `order_dag_with_dfs()` should be unnecessary.
        self.storage()
            .garbage_collect_certificates(latest_leader_round)
            .with_context(|| "BFT failed to garbage collect certificates")?;

        #[cfg(feature = "metrics")]
        metrics::histogram(metrics::bft::COMMIT_LEADER_CERTIFICATE_LATENCY, start.elapsed().as_secs_f64());
        Ok(())
    }

    /// Returns the subdag of batch certificates to commit.
    fn order_dag_with_dfs(
        &self,
        leader_certificate: BatchCertificate<N>,
    ) -> Result<BTreeMap<u64, IndexSet<BatchCertificate<N>>>> {
        // Initialize a map for the certificates to commit.
        let mut commit = BTreeMap::<u64, IndexSet<_>>::new();
        // Initialize a set for the already ordered certificates.
        let mut already_ordered = HashSet::new();
        // Initialize a buffer for the certificates to order.
        let mut buffer = vec![leader_certificate];
        // Iterate over the certificates to order.
        while let Some(certificate) = buffer.pop() {
            // Insert the certificate into the map.
            commit.entry(certificate.round()).or_default().insert(certificate.clone());

            // Check if the previous certificate is below the GC round.
            // This is currently a critical check to prevent forking,
            // as explained in the comment at the end of `commit_leader_certificate()`,
            // just before the call to garbage collection.
            let previous_round = certificate.round().saturating_sub(1);
            if previous_round + self.storage().max_gc_rounds() <= self.dag.read().last_committed_round() {
                continue;
            }
            // Iterate over the previous certificate IDs.
            // Note: Using '.rev()' ensures we remain order-preserving (i.e. "left-to-right" on each level),
            // because this 'while' loop uses 'pop()' to retrieve the next certificate to order.
            for previous_certificate_id in certificate.previous_certificate_ids().iter().rev() {
                // If the previous certificate is already ordered, continue.
                if already_ordered.contains(previous_certificate_id) {
                    continue;
                }
                // If the previous certificate was recently committed, continue.
                if self.dag.read().is_recently_committed(previous_round, *previous_certificate_id) {
                    continue;
                }
                // If the previous certificate already exists in the ledger, continue.
                if self.ledger().contains_certificate(previous_certificate_id).unwrap_or(false) {
                    continue;
                }

                // Retrieve the previous certificate.
                let previous_certificate = {
                    // Start by retrieving the previous certificate from the DAG.
                    match self.dag.read().get_certificate_for_round_with_id(previous_round, *previous_certificate_id) {
                        // If the previous certificate is found, return it.
                        Some(previous_certificate) => previous_certificate,
                        // If the previous certificate is not found, retrieve it from the storage.
                        None => match self.storage().get_certificate(*previous_certificate_id) {
                            // If the previous certificate is found, return it.
                            Some(previous_certificate) => previous_certificate,
                            // Otherwise, the previous certificate is missing, and throw an error.
                            None => bail!(
                                "Missing previous certificate {} for round {previous_round}",
                                fmt_id(previous_certificate_id)
                            ),
                        },
                    }
                };
                // Insert the previous certificate into the set of already ordered certificates.
                already_ordered.insert(previous_certificate.id());
                // Insert the previous certificate into the buffer.
                buffer.push(previous_certificate);
            }
        }
        // Ensure we only retain certificates that are above the GC round.
        commit.retain(|round, _| round + self.storage().max_gc_rounds() > self.dag.read().last_committed_round());
        // Return the certificates to commit.
        Ok(commit)
    }

    /// Shuts down the BFT.
    pub async fn shut_down(&self) {
        info!("Shutting down the BFT...");
        // Shut down the primary.
        self.primary.shut_down().await;
    }
}

#[cfg(test)]
impl<N: Network> BFT<N> {
    /// Returns the latest round a leader certificate was committed to.
    /// (only used in unit tests)
    pub fn testing_only_latest_committed_round(&self) -> u64 {
        self.dag.read().last_committed_round()
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        BFT,
        MAX_LEADER_CERTIFICATE_DELAY,
        PrimaryCallback,
        helpers::{Storage, dag::test_helpers::mock_dag_with_modified_last_committed_round},
        sync::SyncCallback,
    };

    use snarkos_account::Account;
    use snarkos_node_bft_ledger_service::{LedgerService, MockLedgerService};
    use snarkos_node_bft_storage_service::BFTMemoryService;
    use snarkos_node_network::ConnectionMode;
    use snarkos_node_sync::BlockSync;
    use snarkos_utilities::NodeDataDir;

    use snarkvm::{
        console::account::{Address, PrivateKey},
        ledger::{
            committee::{
                Committee,
                test_helpers::{sample_committee, sample_committee_for_round, sample_committee_for_round_and_members},
            },
            narwhal::{
                BatchCertificate,
                batch_certificate::test_helpers::{
                    sample_batch_certificate,
                    sample_batch_certificate_for_round,
                    sample_batch_certificate_for_round_with_committee,
                },
            },
        },
        utilities::TestRng,
    };

    use anyhow::Result;
    use indexmap::{IndexMap, IndexSet};
    use std::sync::Arc;

    type CurrentNetwork = snarkvm::console::network::MainnetV0;

    /// Samples a new test instance, with an optional committee round and the given maximum GC rounds.
    fn sample_test_instance(
        committee_round: Option<u64>,
        max_gc_rounds: u64,
        rng: &mut TestRng,
    ) -> (
        Committee<CurrentNetwork>,
        Account<CurrentNetwork>,
        Arc<MockLedgerService<CurrentNetwork>>,
        Storage<CurrentNetwork>,
    ) {
        let committee = match committee_round {
            Some(round) => sample_committee_for_round(round, rng),
            None => sample_committee(rng),
        };
        let account = Account::new(rng).unwrap();
        let ledger = Arc::new(MockLedgerService::new(committee.clone()));
        let transmissions = Arc::new(BFTMemoryService::new());
        let storage = Storage::new(ledger.clone(), transmissions, max_gc_rounds).unwrap();

        (committee, account, ledger, storage)
    }

    // Helper function to set up BFT for testing.
    fn initialize_bft(
        account: Account<CurrentNetwork>,
        storage: Storage<CurrentNetwork>,
        ledger: Arc<MockLedgerService<CurrentNetwork>>,
    ) -> anyhow::Result<BFT<CurrentNetwork>> {
        // Create the block synchronization logic.
        let block_sync = Arc::new(BlockSync::new(ledger.clone(), ConnectionMode::Gateway));
        // Initialize the BFT.
        BFT::new(
            account.clone(),
            storage.clone(),
            ledger.clone(),
            block_sync,
            None,
            &[],
            false,
            NodeDataDir::new_test(None),
            None,
            1,
        )
    }

    #[test]
    #[tracing_test::traced_test]
    fn test_is_leader_quorum_odd() -> Result<()> {
        let rng = &mut TestRng::default();

        // Sample batch certificates.
        let mut certificates = IndexSet::new();
        certificates.insert(snarkvm::ledger::narwhal::batch_certificate::test_helpers::sample_batch_certificate_for_round_with_previous_certificate_ids(1, IndexSet::new(), rng));
        certificates.insert(snarkvm::ledger::narwhal::batch_certificate::test_helpers::sample_batch_certificate_for_round_with_previous_certificate_ids(1, IndexSet::new(), rng));
        certificates.insert(snarkvm::ledger::narwhal::batch_certificate::test_helpers::sample_batch_certificate_for_round_with_previous_certificate_ids(1, IndexSet::new(), rng));
        certificates.insert(snarkvm::ledger::narwhal::batch_certificate::test_helpers::sample_batch_certificate_for_round_with_previous_certificate_ids(1, IndexSet::new(), rng));

        // Initialize the committee.
        let committee = snarkvm::ledger::committee::test_helpers::sample_committee_for_round_and_members(
            1,
            vec![
                certificates[0].author(),
                certificates[1].author(),
                certificates[2].author(),
                certificates[3].author(),
            ],
            rng,
        );

        // Initialize the ledger.
        let ledger = Arc::new(MockLedgerService::new(committee.clone()));
        // Initialize the storage.
        let storage = Storage::new(ledger.clone(), Arc::new(BFTMemoryService::new()), 10).unwrap();
        // Initialize the account.
        let account = Account::new(rng)?;
        // Initialize the BFT.
        let bft = initialize_bft(account.clone(), storage.clone(), ledger.clone())?;
        assert!(bft.is_timer_expired());
        // Ensure this call succeeds on an odd round.
        let result = bft.is_leader_quorum_or_nonleaders_available(1);
        // If timer has expired but quorum threshold is not reached, return 'false'.
        assert!(!result);
        // Insert certificates into storage.
        for certificate in certificates.iter() {
            storage.testing_only_insert_certificate_testing_only(certificate.clone());
        }
        // Ensure this call succeeds on an odd round.
        let result = bft.is_leader_quorum_or_nonleaders_available(1);
        assert!(result); // no previous leader certificate
        // Set the leader certificate.
        let leader_certificate = sample_batch_certificate(rng);
        *bft.leader_certificate.write() = Some(leader_certificate);
        // Ensure this call succeeds on an odd round.
        let result = bft.is_leader_quorum_or_nonleaders_available(1);
        assert!(result); // should now fall through to the end of function

        Ok(())
    }

    #[test]
    #[tracing_test::traced_test]
    fn test_is_leader_quorum_even_out_of_sync() -> Result<()> {
        let rng = &mut TestRng::default();

        // Sample the test instance.
        let (committee, account, ledger, storage) = sample_test_instance(Some(1), 10, rng);
        assert_eq!(committee.starting_round(), 1);
        assert_eq!(storage.current_round(), 1);
        assert_eq!(storage.max_gc_rounds(), 10);

        // Set up the BFT logic.
        let bft = initialize_bft(account.clone(), storage.clone(), ledger.clone())?;
        assert!(bft.is_timer_expired());

        // Store is at round 1, and we are checking for round 2.
        // Ensure this call fails on an even round.
        let result = bft.is_leader_quorum_or_nonleaders_available(2);
        assert!(!result);
        Ok(())
    }

    #[test]
    #[tracing_test::traced_test]
    fn test_is_leader_quorum_even() -> Result<()> {
        let rng = &mut TestRng::default();

        // Sample the test instance.
        let (committee, account, ledger, storage) = sample_test_instance(Some(2), 10, rng);
        assert_eq!(committee.starting_round(), 2);
        assert_eq!(storage.current_round(), 2);
        assert_eq!(storage.max_gc_rounds(), 10);

        // Set up the BFT logic.
        let bft = initialize_bft(account.clone(), storage.clone(), ledger.clone())?;
        assert!(bft.is_timer_expired());

        // Ensure this call fails on an even round.
        let result = bft.is_leader_quorum_or_nonleaders_available(2);
        assert!(!result);
        Ok(())
    }

    #[test]
    #[tracing_test::traced_test]
    fn test_is_even_round_ready() -> Result<()> {
        let rng = &mut TestRng::default();

        // Sample batch certificates.
        let mut certificates = IndexSet::new();
        certificates.insert(sample_batch_certificate_for_round(2, rng));
        certificates.insert(sample_batch_certificate_for_round(2, rng));
        certificates.insert(sample_batch_certificate_for_round(2, rng));
        certificates.insert(sample_batch_certificate_for_round(2, rng));

        // Initialize the committee.
        let committee = snarkvm::ledger::committee::test_helpers::sample_committee_for_round_and_members(
            2,
            vec![
                certificates[0].author(),
                certificates[1].author(),
                certificates[2].author(),
                certificates[3].author(),
            ],
            rng,
        );

        // Initialize the ledger.
        let ledger = Arc::new(MockLedgerService::new(committee.clone()));
        // Initialize the storage.
        let storage = Storage::new(ledger.clone(), Arc::new(BFTMemoryService::new()), 10).unwrap();
        // Initialize the account.
        let account = Account::new(rng)?;

        // Set up the BFT logic.
        let bft = initialize_bft(account.clone(), storage.clone(), ledger.clone())?;
        assert!(bft.is_timer_expired());

        // Set the leader certificate.
        let leader_certificate = sample_batch_certificate_for_round(2, rng);
        *bft.leader_certificate.write() = Some(leader_certificate);
        let result = bft.is_even_round_ready_for_next_round(IndexSet::new(), committee.clone(), 2);
        // If leader certificate is set but quorum threshold is not reached, we are not ready for the next round.
        assert!(!result);
        // Once quorum threshold is reached, we are ready for the next round.
        let result = bft.is_even_round_ready_for_next_round(certificates.clone(), committee.clone(), 2);
        assert!(result);

        // Initialize a new BFT.
        let bft_timer = initialize_bft(account.clone(), storage.clone(), ledger.clone())?;
        // If the leader certificate is not set and the timer has not expired, we are not ready for the next round.
        let result = bft_timer.is_even_round_ready_for_next_round(certificates.clone(), committee.clone(), 2);
        if !bft_timer.is_timer_expired() {
            assert!(!result);
        }
        // Wait for the timer to expire.
        std::thread::sleep(MAX_LEADER_CERTIFICATE_DELAY);
        // Once the leader certificate timer has expired and quorum threshold is reached, we are ready to advance to the next round.
        let result = bft_timer.is_even_round_ready_for_next_round(certificates.clone(), committee.clone(), 2);
        if bft_timer.is_timer_expired() {
            assert!(result);
        } else {
            assert!(!result);
        }

        Ok(())
    }

    #[test]
    #[tracing_test::traced_test]
    fn test_update_leader_certificate_odd() -> Result<()> {
        let rng = &mut TestRng::default();

        // Sample the test instance.
        let (_, account, ledger, storage) = sample_test_instance(None, 10, rng);
        assert_eq!(storage.max_gc_rounds(), 10);

        // Initialize the BFT.
        let bft = initialize_bft(account.clone(), storage.clone(), ledger.clone())?;
        assert!(bft.is_timer_expired());

        // Ensure this call fails on an odd round.
        let result = bft.update_leader_certificate_to_even_round(1);
        assert!(!result);
        Ok(())
    }

    #[test]
    #[tracing_test::traced_test]
    fn test_update_leader_certificate_bad_round() -> Result<()> {
        let rng = &mut TestRng::default();

        // Sample the test instance.
        let (_, account, ledger, storage) = sample_test_instance(None, 10, rng);
        assert_eq!(storage.max_gc_rounds(), 10);

        // Initialize the BFT.
        let bft = initialize_bft(account.clone(), storage.clone(), ledger.clone())?;

        // Ensure this call succeeds on an even round.
        let result = bft.update_leader_certificate_to_even_round(6);
        assert!(!result);
        Ok(())
    }

    #[test]
    #[tracing_test::traced_test]
    fn test_update_leader_certificate_even() -> Result<()> {
        let rng = &mut TestRng::default();

        // Set the current round.
        let current_round = 3;

        // Sample the certificates.
        let (_, certificates) = snarkvm::ledger::narwhal::batch_certificate::test_helpers::sample_batch_certificate_with_previous_certificates(
            current_round,
            rng,
        );

        // Initialize the committee.
        let committee = snarkvm::ledger::committee::test_helpers::sample_committee_for_round_and_members(
            2,
            vec![
                certificates[0].author(),
                certificates[1].author(),
                certificates[2].author(),
                certificates[3].author(),
            ],
            rng,
        );

        // Initialize the ledger.
        let ledger = Arc::new(MockLedgerService::new(committee.clone()));

        // Initialize the storage.
        let transmissions = Arc::new(BFTMemoryService::new());
        let storage = Storage::new(ledger.clone(), transmissions, 10).unwrap();
        storage.testing_only_insert_certificate_testing_only(certificates[0].clone());
        storage.testing_only_insert_certificate_testing_only(certificates[1].clone());
        storage.testing_only_insert_certificate_testing_only(certificates[2].clone());
        storage.testing_only_insert_certificate_testing_only(certificates[3].clone());
        assert_eq!(storage.current_round(), 2);

        // Retrieve the leader certificate.
        let leader = committee.get_leader(2).unwrap();
        let leader_certificate = storage.get_certificate_for_round_with_author(2, leader).unwrap();

        // Initialize the BFT.
        let account = Account::new(rng)?;
        let bft = initialize_bft(account.clone(), storage.clone(), ledger.clone())?;

        // Set the leader certificate.
        *bft.leader_certificate.write() = Some(leader_certificate);

        // Update the leader certificate.
        // Ensure this call succeeds on an even round.
        let result = bft.update_leader_certificate_to_even_round(2);
        assert!(result);

        Ok(())
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_order_dag_with_dfs() -> Result<()> {
        let rng = &mut TestRng::default();

        // Sample the test instance.
        let (_, account, ledger, _) = sample_test_instance(Some(1), 10, rng);

        // Initialize the round parameters.
        let previous_round = 2; // <- This must be an even number for the DAG structure expected below.
        let current_round = previous_round + 1;

        // Sample the current certificate and previous certificates.
        let (certificate, previous_certificates) = snarkvm::ledger::narwhal::batch_certificate::test_helpers::sample_batch_certificate_with_previous_certificates(
            current_round,
            rng,
        );

        /* Test GC */

        // Ensure the function succeeds in returning only certificates above GC.
        {
            // Initialize the storage.
            let storage = Storage::new(ledger.clone(), Arc::new(BFTMemoryService::new()), 1).unwrap();
            // Initialize the BFT.
            let bft = initialize_bft(account.clone(), storage.clone(), ledger.clone())?;

            // Insert a mock DAG in the BFT.
            *bft.dag.write() = crate::helpers::dag::test_helpers::mock_dag_with_modified_last_committed_round(3);

            // Insert the previous certificates into the BFT.
            for certificate in previous_certificates.clone() {
                bft.add_certificate_from_sync(certificate);
            }

            // Ensure this call succeeds and returns all given certificates.
            let result = bft.order_dag_with_dfs(certificate.clone());
            assert!(result.is_ok());
            let candidate_certificates = result.unwrap().into_values().flatten().collect::<Vec<_>>();
            assert_eq!(candidate_certificates.len(), 1);
            let expected_certificates = vec![certificate.clone()];
            assert_eq!(
                candidate_certificates.iter().map(|c| c.id()).collect::<Vec<_>>(),
                expected_certificates.iter().map(|c| c.id()).collect::<Vec<_>>()
            );
            assert_eq!(candidate_certificates, expected_certificates);
        }

        /* Test normal case */

        // Ensure the function succeeds in returning all given certificates.
        {
            // Initialize the storage.
            let storage = Storage::new(ledger.clone(), Arc::new(BFTMemoryService::new()), 1).unwrap();
            // Initialize the BFT.
            let bft = initialize_bft(account.clone(), storage.clone(), ledger.clone())?;

            // Insert a mock DAG in the BFT.
            *bft.dag.write() = crate::helpers::dag::test_helpers::mock_dag_with_modified_last_committed_round(2);

            // Insert the previous certificates into the BFT.
            for certificate in previous_certificates.clone() {
                bft.add_certificate_from_sync(certificate);
            }

            // Ensure this call succeeds and returns all given certificates.
            let result = bft.order_dag_with_dfs(certificate.clone());
            assert!(result.is_ok());
            let candidate_certificates = result.unwrap().into_values().flatten().collect::<Vec<_>>();
            assert_eq!(candidate_certificates.len(), 5);
            let expected_certificates = vec![
                previous_certificates[0].clone(),
                previous_certificates[1].clone(),
                previous_certificates[2].clone(),
                previous_certificates[3].clone(),
                certificate,
            ];
            assert_eq!(
                candidate_certificates.iter().map(|c| c.id()).collect::<Vec<_>>(),
                expected_certificates.iter().map(|c| c.id()).collect::<Vec<_>>()
            );
            assert_eq!(candidate_certificates, expected_certificates);
        }

        Ok(())
    }

    #[test]
    #[tracing_test::traced_test]
    fn test_order_dag_with_dfs_fails_on_missing_previous_certificate() -> Result<()> {
        let rng = &mut TestRng::default();

        // Sample the test instance.
        let (committee, account, ledger, storage) = sample_test_instance(Some(1), 1, rng);
        assert_eq!(committee.starting_round(), 1);
        assert_eq!(storage.current_round(), 1);
        assert_eq!(storage.max_gc_rounds(), 1);

        // Initialize the round parameters.
        let previous_round = 2; // <- This must be an even number for the DAG structure expected below.
        let current_round = previous_round + 1;

        // Sample the current certificate and previous certificates.
        let (certificate, previous_certificates) = snarkvm::ledger::narwhal::batch_certificate::test_helpers::sample_batch_certificate_with_previous_certificates(
            current_round,
            rng,
        );
        // Construct the previous certificate IDs.
        let previous_certificate_ids: IndexSet<_> = previous_certificates.iter().map(|c| c.id()).collect();

        /* Test missing previous certificate. */

        // Initialize the BFT.
        let bft = initialize_bft(account.clone(), storage.clone(), ledger.clone())?;

        // The expected error message.
        let error_msg = format!(
            "Missing previous certificate {} for round {previous_round}",
            crate::helpers::fmt_id(previous_certificate_ids[3]),
        );

        // Ensure this call fails on a missing previous certificate.
        let result = bft.order_dag_with_dfs(certificate);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().to_string(), error_msg);
        Ok(())
    }

    #[tokio::test]
    async fn test_bft_gc_on_commit() -> Result<()> {
        let rng = &mut TestRng::default();

        // Initialize the round parameters.
        let max_gc_rounds = 1;
        let committee_round = 0;
        let commit_round = 2;
        let current_round = commit_round + 1;

        // Sample the certificates.
        let (_, certificates) = snarkvm::ledger::narwhal::batch_certificate::test_helpers::sample_batch_certificate_with_previous_certificates(
            current_round,
            rng,
        );

        // Initialize the committee.
        let committee = snarkvm::ledger::committee::test_helpers::sample_committee_for_round_and_members(
            committee_round,
            vec![
                certificates[0].author(),
                certificates[1].author(),
                certificates[2].author(),
                certificates[3].author(),
            ],
            rng,
        );

        // Initialize the ledger.
        let ledger = Arc::new(MockLedgerService::new(committee.clone()));

        // Initialize the storage.
        let transmissions = Arc::new(BFTMemoryService::new());
        let storage = Storage::new(ledger.clone(), transmissions, max_gc_rounds).unwrap();
        // Insert the certificates into the storage.
        for certificate in certificates.iter() {
            storage.testing_only_insert_certificate_testing_only(certificate.clone());
        }

        // Get the leader certificate.
        let leader = committee.get_leader(commit_round).unwrap();
        let leader_certificate = storage.get_certificate_for_round_with_author(commit_round, leader).unwrap();

        // Initialize the BFT.
        let account = Account::new(rng)?;
        let bft = initialize_bft(account.clone(), storage.clone(), ledger.clone())?;

        // Create an empty mock DAG with last committed round set to `commit_round`.
        *bft.dag.write() = mock_dag_with_modified_last_committed_round(commit_round);

        // Ensure that the `gc_round` has not been updated yet.
        assert_eq!(bft.storage().gc_round(), committee_round.saturating_sub(max_gc_rounds));

        // Insert the certificates into the BFT.
        for certificate in certificates {
            bft.add_certificate_from_sync(certificate);
        }

        // Commit the leader certificate.
        bft.commit_leader_certificate(leader_certificate).await.unwrap();

        // Ensure that the `gc_round` has been updated.
        assert_eq!(bft.storage().gc_round(), commit_round - max_gc_rounds);

        Ok(())
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_sync_bft_dag_at_bootup() -> Result<()> {
        let rng = &mut TestRng::default();

        // Initialize the round parameters.
        let max_gc_rounds = 1;
        let committee_round = 0;
        let commit_round = 2;
        let current_round = commit_round + 1;

        // Sample the current certificate and previous certificates.
        let (_, certificates) = snarkvm::ledger::narwhal::batch_certificate::test_helpers::sample_batch_certificate_with_previous_certificates(
            current_round,
            rng,
        );

        // Initialize the committee.
        let committee = snarkvm::ledger::committee::test_helpers::sample_committee_for_round_and_members(
            committee_round,
            vec![
                certificates[0].author(),
                certificates[1].author(),
                certificates[2].author(),
                certificates[3].author(),
            ],
            rng,
        );

        // Initialize the ledger.
        let ledger = Arc::new(MockLedgerService::new(committee.clone()));

        // Initialize the storage.
        let storage = Storage::new(ledger.clone(), Arc::new(BFTMemoryService::new()), max_gc_rounds).unwrap();
        // Insert the certificates into the storage.
        for certificate in certificates.iter() {
            storage.testing_only_insert_certificate_testing_only(certificate.clone());
        }

        // Get the leader certificate.
        let leader = committee.get_leader(commit_round).unwrap();
        let leader_certificate = storage.get_certificate_for_round_with_author(commit_round, leader).unwrap();

        // Initialize the BFT.
        let account = Account::new(rng)?;
        let bft = initialize_bft(account.clone(), storage.clone(), ledger.clone())?;

        // Insert a mock DAG in the BFT.
        *bft.dag.write() = crate::helpers::dag::test_helpers::mock_dag_with_modified_last_committed_round(commit_round);

        // Insert the previous certificates into the BFT.
        for certificate in certificates.clone() {
            bft.add_certificate_from_sync(certificate);
        }

        // Commit the leader certificate.
        bft.commit_leader_certificate(leader_certificate.clone()).await.unwrap();

        // Simulate a bootup of the BFT.

        // Initialize a new instance of storage.
        let storage_2 = Storage::new(ledger.clone(), Arc::new(BFTMemoryService::new()), max_gc_rounds).unwrap();
        // Initialize a new instance of BFT.
        let bootup_bft = initialize_bft(account.clone(), storage_2, ledger)?;

        // Sync the BFT DAG at bootup.
        for cert in certificates.iter() {
            bootup_bft.add_certificate_from_sync(cert.clone());
            bootup_bft.commit_certificate_from_sync(cert);
        }

        // Check that the BFT starts from the same last committed round.
        assert_eq!(bft.dag.read().last_committed_round(), bootup_bft.dag.read().last_committed_round());

        // Ensure that both BFTs have committed the leader certificate.
        assert!(bft.dag.read().is_recently_committed(leader_certificate.round(), leader_certificate.id()));
        assert!(bootup_bft.dag.read().is_recently_committed(leader_certificate.round(), leader_certificate.id()));

        // Check the state of the bootup BFT.
        for certificate in certificates {
            let certificate_round = certificate.round();
            let certificate_id = certificate.id();
            // Check that the bootup BFT has committed the certificates.
            assert!(bootup_bft.dag.read().is_recently_committed(certificate_round, certificate_id));
            // Check that the bootup BFT does not contain the certificates in its graph, because
            // it should not need to order them again in subsequent subdags.
            assert!(!bootup_bft.dag.read().contains_certificate_in_round(certificate_round, certificate_id));
        }

        Ok(())
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_sync_bft_dag_at_bootup_shutdown() -> Result<()> {
        /*
        1. Run one uninterrupted BFT on a set of certificates for 2 leader commits.
        2. Run a separate bootup BFT that syncs with a set of pre shutdown certificates, and then commits a second leader normally over a set of post shutdown certificates.
        3. Observe that the uninterrupted BFT and the bootup BFT end in the same state.
        */

        let rng = &mut TestRng::default();

        // Initialize the round parameters.
        let max_gc_rounds = snarkvm::ledger::narwhal::BatchHeader::<CurrentNetwork>::MAX_GC_ROUNDS as u64;
        let committee_round = 0;
        let commit_round = 2;
        let current_round = commit_round + 1;
        let next_round = current_round + 1;

        // Sample 5 rounds of batch certificates starting at the genesis round from a static set of 4 authors.
        let (round_to_certificates_map, committee) = {
            let private_keys = [
                PrivateKey::new(rng).unwrap(),
                PrivateKey::new(rng).unwrap(),
                PrivateKey::new(rng).unwrap(),
                PrivateKey::new(rng).unwrap(),
            ];
            let addresses = vec![
                Address::try_from(private_keys[0])?,
                Address::try_from(private_keys[1])?,
                Address::try_from(private_keys[2])?,
                Address::try_from(private_keys[3])?,
            ];
            let committee = snarkvm::ledger::committee::test_helpers::sample_committee_for_round_and_members(
                committee_round,
                addresses,
                rng,
            );
            // Initialize a mapping from the round number to the set of batch certificates in the round.
            let mut round_to_certificates_map: IndexMap<
                u64,
                IndexSet<snarkvm::ledger::narwhal::BatchCertificate<CurrentNetwork>>,
            > = IndexMap::new();
            let mut previous_certificates = IndexSet::with_capacity(4);
            // Initialize the genesis batch certificates.
            for _ in 0..4 {
                previous_certificates.insert(sample_batch_certificate(rng));
            }
            for round in 0..commit_round + 3 {
                let mut current_certificates = IndexSet::new();
                let previous_certificate_ids: IndexSet<_> = if round == 0 || round == 1 {
                    IndexSet::new()
                } else {
                    previous_certificates.iter().map(|c| c.id()).collect()
                };
                let transmission_ids =
                    snarkvm::ledger::narwhal::transmission_id::test_helpers::sample_transmission_ids(rng)
                        .into_iter()
                        .collect::<IndexSet<_>>();
                let timestamp = time::OffsetDateTime::now_utc().unix_timestamp();
                let committee_id = committee.id();
                for (i, private_key_1) in private_keys.iter().enumerate() {
                    let batch_header = snarkvm::ledger::narwhal::BatchHeader::new(
                        private_key_1,
                        round,
                        timestamp,
                        committee_id,
                        transmission_ids.clone(),
                        previous_certificate_ids.clone(),
                        rng,
                    )
                    .unwrap();
                    let mut signatures = IndexSet::with_capacity(4);
                    for (j, private_key_2) in private_keys.iter().enumerate() {
                        if i != j {
                            signatures.insert(private_key_2.sign(&[batch_header.batch_id()], rng).unwrap());
                        }
                    }
                    let certificate =
                        snarkvm::ledger::narwhal::BatchCertificate::from(batch_header, signatures).unwrap();
                    current_certificates.insert(certificate);
                }
                // Update the mapping.
                round_to_certificates_map.insert(round, current_certificates.clone());
                previous_certificates = current_certificates.clone();
            }
            (round_to_certificates_map, committee)
        };

        // Initialize the ledger.
        let ledger = Arc::new(MockLedgerService::new(committee.clone()));
        // Initialize the storage.
        let storage = Storage::new(ledger.clone(), Arc::new(BFTMemoryService::new()), max_gc_rounds).unwrap();
        // Get the leaders for the next 2 commit rounds.
        let leader = committee.get_leader(commit_round).unwrap();
        let next_leader = committee.get_leader(next_round).unwrap();
        // Insert the pre shutdown certificates into the storage.
        let mut pre_shutdown_certificates: Vec<snarkvm::ledger::narwhal::BatchCertificate<CurrentNetwork>> = Vec::new();
        for i in 1..=commit_round {
            let certificates = (*round_to_certificates_map.get(&i).unwrap()).clone();
            if i == commit_round {
                // Only insert the leader certificate for the commit round.
                let leader_certificate = certificates.iter().find(|certificate| certificate.author() == leader);
                if let Some(c) = leader_certificate {
                    pre_shutdown_certificates.push(c.clone());
                }
                continue;
            }
            pre_shutdown_certificates.extend(certificates);
        }
        for certificate in pre_shutdown_certificates.iter() {
            storage.testing_only_insert_certificate_testing_only(certificate.clone());
        }
        // Insert the post shutdown certificates into the storage.
        let mut post_shutdown_certificates: Vec<snarkvm::ledger::narwhal::BatchCertificate<CurrentNetwork>> =
            Vec::new();
        for j in commit_round..=commit_round + 2 {
            let certificate = (*round_to_certificates_map.get(&j).unwrap()).clone();
            post_shutdown_certificates.extend(certificate);
        }
        for certificate in post_shutdown_certificates.iter() {
            storage.testing_only_insert_certificate_testing_only(certificate.clone());
        }
        // Get the leader certificates.
        let leader_certificate = storage.get_certificate_for_round_with_author(commit_round, leader).unwrap();
        let next_leader_certificate = storage.get_certificate_for_round_with_author(next_round, next_leader).unwrap();

        // Initialize the BFT without bootup.
        let account = Account::new(rng)?;
        let bft = initialize_bft(account.clone(), storage.clone(), ledger.clone())?;

        // Insert a mock DAG in the BFT without bootup.
        *bft.dag.write() = crate::helpers::dag::test_helpers::mock_dag_with_modified_last_committed_round(0);

        // Insert the certificates into the BFT without bootup.
        for certificate in pre_shutdown_certificates.clone() {
            assert!(bft.add_new_certificate(certificate).await.is_ok());
        }

        // Insert the post shutdown certificates into the BFT without bootup.
        for certificate in post_shutdown_certificates.clone() {
            assert!(bft.add_new_certificate(certificate).await.is_ok());
        }
        // Commit the second leader certificate.
        let commit_subdag = bft.order_dag_with_dfs(next_leader_certificate.clone()).unwrap();
        let commit_subdag_metadata = commit_subdag.iter().map(|(round, c)| (*round, c.len())).collect::<Vec<_>>();
        bft.commit_leader_certificate(next_leader_certificate.clone()).await.unwrap();

        // Simulate a bootup of the BFT.

        // Initialize a new instance of storage.
        let bootup_storage = Storage::new(ledger.clone(), Arc::new(BFTMemoryService::new()), max_gc_rounds).unwrap();

        // Initialize a new instance of BFT with bootup.
        let bootup_bft = initialize_bft(account.clone(), bootup_storage.clone(), ledger.clone())?;

        // Sync the BFT DAG at bootup.
        for cert in pre_shutdown_certificates.iter() {
            bootup_bft.add_certificate_from_sync(cert.clone());
            bootup_bft.commit_certificate_from_sync(cert);
        }

        // Insert the post shutdown certificates to the storage and BFT with bootup.
        for certificate in post_shutdown_certificates.iter() {
            bootup_bft.storage().testing_only_insert_certificate_testing_only(certificate.clone());
        }
        for certificate in post_shutdown_certificates.clone() {
            assert!(bootup_bft.add_new_certificate(certificate).await.is_ok());
        }
        // Commit the second leader certificate.
        let commit_subdag_bootup = bootup_bft.order_dag_with_dfs(next_leader_certificate.clone()).unwrap();
        let commit_subdag_metadata_bootup =
            commit_subdag_bootup.iter().map(|(round, c)| (*round, c.len())).collect::<Vec<_>>();
        let committed_certificates_bootup = commit_subdag_bootup.values().flatten();
        bootup_bft.commit_leader_certificate(next_leader_certificate.clone()).await.unwrap();

        // Check that the final state of both BFTs is the same.

        // Check that both BFTs start from the same last committed round.
        assert_eq!(bft.dag.read().last_committed_round(), bootup_bft.dag.read().last_committed_round());

        // Ensure that both BFTs have committed the leader certificates.
        assert!(bft.dag.read().is_recently_committed(leader_certificate.round(), leader_certificate.id()));
        assert!(bft.dag.read().is_recently_committed(next_leader_certificate.round(), next_leader_certificate.id()));
        assert!(bootup_bft.dag.read().is_recently_committed(leader_certificate.round(), leader_certificate.id()));
        assert!(
            bootup_bft.dag.read().is_recently_committed(next_leader_certificate.round(), next_leader_certificate.id())
        );

        // Check that the bootup BFT has committed the pre shutdown certificates.
        for certificate in pre_shutdown_certificates.clone() {
            let certificate_round = certificate.round();
            let certificate_id = certificate.id();
            // Check that both BFTs have committed the certificates.
            assert!(bft.dag.read().is_recently_committed(certificate_round, certificate_id));
            assert!(bootup_bft.dag.read().is_recently_committed(certificate_round, certificate_id));
            // Check that the bootup BFT does not contain the certificates in its graph, because
            // it should not need to order them again in subsequent subdags.
            assert!(!bft.dag.read().contains_certificate_in_round(certificate_round, certificate_id));
            assert!(!bootup_bft.dag.read().contains_certificate_in_round(certificate_round, certificate_id));
        }

        // Check that that the bootup BFT has committed the subdag stemming from the second leader certificate in consensus.
        for certificate in committed_certificates_bootup.clone() {
            let certificate_round = certificate.round();
            let certificate_id = certificate.id();
            // Check that the both BFTs have committed the certificates.
            assert!(bft.dag.read().is_recently_committed(certificate_round, certificate_id));
            assert!(bootup_bft.dag.read().is_recently_committed(certificate_round, certificate_id));
            // Check that the bootup BFT does not contain the certificates in its graph, because
            // it should not need to order them again in subsequent subdags.
            assert!(!bft.dag.read().contains_certificate_in_round(certificate_round, certificate_id));
            assert!(!bootup_bft.dag.read().contains_certificate_in_round(certificate_round, certificate_id));
        }

        // Check that the commit subdag metadata for the second leader is the same for both BFTs.
        assert_eq!(commit_subdag_metadata_bootup, commit_subdag_metadata);

        Ok(())
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_sync_bft_dag_at_bootup_dfs() -> Result<()> {
        /*
        1. Run a bootup BFT that syncs with a set of pre shutdown certificates.
        2. Add post shutdown certificates to the bootup BFT.
        2. Observe that in the commit subdag of the second leader certificate, there are no repeated vertices from the pre shutdown certificates.
        */

        let rng = &mut TestRng::default();

        // Initialize the round parameters.
        let max_gc_rounds = snarkvm::ledger::narwhal::BatchHeader::<CurrentNetwork>::MAX_GC_ROUNDS as u64;
        let committee_round = 0;
        let commit_round = 2;
        let current_round = commit_round + 1;
        let next_round = current_round + 1;

        // Sample 5 rounds of batch certificates starting at the genesis round from a static set of 4 authors.
        let (round_to_certificates_map, committee) = {
            let private_keys = [
                PrivateKey::new(rng).unwrap(),
                PrivateKey::new(rng).unwrap(),
                PrivateKey::new(rng).unwrap(),
                PrivateKey::new(rng).unwrap(),
            ];
            let addresses = vec![
                Address::try_from(private_keys[0])?,
                Address::try_from(private_keys[1])?,
                Address::try_from(private_keys[2])?,
                Address::try_from(private_keys[3])?,
            ];
            let committee = snarkvm::ledger::committee::test_helpers::sample_committee_for_round_and_members(
                committee_round,
                addresses,
                rng,
            );
            // Initialize a mapping from the round number to the set of batch certificates in the round.
            let mut round_to_certificates_map: IndexMap<
                u64,
                IndexSet<snarkvm::ledger::narwhal::BatchCertificate<CurrentNetwork>>,
            > = IndexMap::new();
            let mut previous_certificates = IndexSet::with_capacity(4);
            // Initialize the genesis batch certificates.
            for _ in 0..4 {
                previous_certificates.insert(sample_batch_certificate(rng));
            }
            for round in 0..=commit_round + 2 {
                let mut current_certificates = IndexSet::new();
                let previous_certificate_ids: IndexSet<_> = if round == 0 || round == 1 {
                    IndexSet::new()
                } else {
                    previous_certificates.iter().map(|c| c.id()).collect()
                };
                let transmission_ids =
                    snarkvm::ledger::narwhal::transmission_id::test_helpers::sample_transmission_ids(rng)
                        .into_iter()
                        .collect::<IndexSet<_>>();
                let timestamp = time::OffsetDateTime::now_utc().unix_timestamp();
                let committee_id = committee.id();
                for (i, private_key_1) in private_keys.iter().enumerate() {
                    let batch_header = snarkvm::ledger::narwhal::BatchHeader::new(
                        private_key_1,
                        round,
                        timestamp,
                        committee_id,
                        transmission_ids.clone(),
                        previous_certificate_ids.clone(),
                        rng,
                    )
                    .unwrap();
                    let mut signatures = IndexSet::with_capacity(4);
                    for (j, private_key_2) in private_keys.iter().enumerate() {
                        if i != j {
                            signatures.insert(private_key_2.sign(&[batch_header.batch_id()], rng).unwrap());
                        }
                    }
                    let certificate =
                        snarkvm::ledger::narwhal::BatchCertificate::from(batch_header, signatures).unwrap();
                    current_certificates.insert(certificate);
                }
                // Update the mapping.
                round_to_certificates_map.insert(round, current_certificates.clone());
                previous_certificates = current_certificates.clone();
            }
            (round_to_certificates_map, committee)
        };

        // Initialize the ledger.
        let ledger = Arc::new(MockLedgerService::new(committee.clone()));
        // Initialize the storage.
        let storage = Storage::new(ledger.clone(), Arc::new(BFTMemoryService::new()), max_gc_rounds).unwrap();
        // Get the leaders for the next 2 commit rounds.
        let leader = committee.get_leader(commit_round).unwrap();
        let next_leader = committee.get_leader(next_round).unwrap();
        // Insert the pre shutdown certificates into the storage.
        let mut pre_shutdown_certificates: Vec<snarkvm::ledger::narwhal::BatchCertificate<CurrentNetwork>> = Vec::new();
        for i in 1..=commit_round {
            let certificates = (*round_to_certificates_map.get(&i).unwrap()).clone();
            if i == commit_round {
                // Only insert the leader certificate for the commit round.
                let leader_certificate = certificates.iter().find(|certificate| certificate.author() == leader);
                if let Some(c) = leader_certificate {
                    pre_shutdown_certificates.push(c.clone());
                }
                continue;
            }
            pre_shutdown_certificates.extend(certificates);
        }
        for certificate in pre_shutdown_certificates.iter() {
            storage.testing_only_insert_certificate_testing_only(certificate.clone());
        }
        // Initialize the bootup BFT.
        let account = Account::new(rng)?;
        let bootup_bft = initialize_bft(account.clone(), storage.clone(), ledger.clone())?;

        // Insert a mock DAG in the BFT without bootup.
        *bootup_bft.dag.write() = crate::helpers::dag::test_helpers::mock_dag_with_modified_last_committed_round(0);
        // Sync the BFT DAG at bootup.
        for cert in pre_shutdown_certificates.iter() {
            bootup_bft.add_certificate_from_sync(cert.clone());
            bootup_bft.commit_certificate_from_sync(cert);
        }

        // Insert the post shutdown certificates into the storage.
        let mut post_shutdown_certificates: Vec<snarkvm::ledger::narwhal::BatchCertificate<CurrentNetwork>> =
            Vec::new();
        for j in commit_round..=commit_round + 2 {
            let certificate = (*round_to_certificates_map.get(&j).unwrap()).clone();
            post_shutdown_certificates.extend(certificate);
        }
        for certificate in post_shutdown_certificates.iter() {
            storage.testing_only_insert_certificate_testing_only(certificate.clone());
        }

        // Insert the post shutdown certificates into the DAG.
        for certificate in post_shutdown_certificates.clone() {
            assert!(bootup_bft.add_new_certificate(certificate).await.is_ok());
        }

        // Get the next leader certificate to commit.
        let next_leader_certificate = storage.get_certificate_for_round_with_author(next_round, next_leader).unwrap();
        let commit_subdag = bootup_bft.order_dag_with_dfs(next_leader_certificate).unwrap();
        let committed_certificates = commit_subdag.values().flatten();

        // Check that none of the certificates synced from the bootup appear in the subdag for the next commit round.
        for pre_shutdown_certificate in pre_shutdown_certificates.clone() {
            for committed_certificate in committed_certificates.clone() {
                assert_ne!(pre_shutdown_certificate.id(), committed_certificate.id());
            }
        }
        Ok(())
    }

    /// Tests that a leader certificate can be committed by sufficient endorsements in a succeeding leader certificate.
    #[test_log::test(tokio::test)]
    async fn test_commit_via_is_linked() {
        let rng = &mut TestRng::default();

        let committee_round = 0;
        let leader_round_1 = 2;
        let leader_round_2 = 4; // subsequent even round
        let max_gc_rounds = 50;

        // Create a committee with four members.
        let num_authors = 4;
        let private_keys: Vec<_> = (0..num_authors).map(|_| PrivateKey::new(rng).unwrap()).collect();
        let addresses: Vec<_> = private_keys.iter().map(|pkey| Address::try_from(pkey).unwrap()).collect();

        let committee = sample_committee_for_round_and_members(committee_round, addresses.clone(), rng);
        let ledger = Arc::new(MockLedgerService::new(committee.clone()));
        let storage = Storage::new(ledger.clone(), Arc::new(BFTMemoryService::new()), max_gc_rounds).unwrap();
        let bft = initialize_bft(Account::new(rng).unwrap(), storage.clone(), ledger.clone()).unwrap();

        let mut certificates_by_round: IndexMap<u64, IndexSet<BatchCertificate<CurrentNetwork>>> = IndexMap::new();

        // Round 1
        let round1_certs: IndexSet<_> = (0..num_authors)
            .map(|idx| {
                let author = &private_keys[idx];
                let endorsements: Vec<_> = private_keys
                    .iter()
                    .enumerate()
                    .filter_map(|(other_idx, pkey)| if idx == other_idx { None } else { Some(*pkey) })
                    .collect();

                sample_batch_certificate_for_round_with_committee(1, IndexSet::new(), author, &endorsements[..], rng)
            })
            .collect();
        certificates_by_round.insert(1, round1_certs.clone());

        let leader1 = ledger.get_committee_for_round(leader_round_1 + 1).unwrap().get_leader(leader_round_1).unwrap();
        let mut leader1_certificate = None;

        let round2_certs: IndexSet<_> = (0..num_authors)
            .map(|idx| {
                let author = &private_keys[idx];
                let endorsements: Vec<_> = private_keys
                    .iter()
                    .enumerate()
                    .filter_map(|(other_idx, pkey)| if idx == other_idx { None } else { Some(*pkey) })
                    .collect();
                let cert = sample_batch_certificate_for_round_with_committee(
                    leader_round_1,
                    round1_certs.iter().map(|c| c.id()).collect(),
                    author,
                    &endorsements[..],
                    rng,
                );

                if cert.author() == leader1 {
                    leader1_certificate = Some(cert.clone());
                }
                cert
            })
            .collect();
        certificates_by_round.insert(leader_round_1, round2_certs.clone());

        let round3_certs: IndexSet<_> = (0..num_authors)
            .map(|idx| {
                let author = &private_keys[idx];
                let endorsements: Vec<_> = private_keys
                    .iter()
                    .enumerate()
                    .filter_map(|(other_idx, pkey)| if idx == other_idx { None } else { Some(*pkey) })
                    .collect();

                let previous_certificate_ids: IndexSet<_> = round2_certs
                    .iter()
                    .filter_map(|cert| {
                        // Only have the leader endorse the previous round's leader certificate.
                        if cert.author() == leader1 && cert.author() != addresses[idx] { None } else { Some(cert.id()) }
                    })
                    .collect();

                sample_batch_certificate_for_round_with_committee(
                    leader_round_1 + 1,
                    previous_certificate_ids,
                    author,
                    &endorsements[..],
                    rng,
                )
            })
            .collect();
        certificates_by_round.insert(leader_round_1 + 1, round3_certs.clone());

        // Ensure the first leader's certificate is not committed yet.
        let leader_certificate_1 = leader1_certificate.unwrap();
        assert!(
            !bft.dag.read().is_recently_committed(leader_round_1, leader_certificate_1.id()),
            "Leader certificate 1 should not be committed yet"
        );
        assert_eq!(bft.dag.read().last_committed_round(), 0);

        let leader2 = ledger.get_committee_for_round(leader_round_2 + 1).unwrap().get_leader(leader_round_2).unwrap();
        let round4_certs: IndexSet<_> = (0..num_authors)
            .map(|idx| {
                let endorsements: Vec<_> = private_keys
                    .iter()
                    .enumerate()
                    .filter_map(|(other_idx, pkey)| if idx == other_idx { None } else { Some(*pkey) })
                    .collect();

                sample_batch_certificate_for_round_with_committee(
                    leader_round_2,
                    round3_certs.iter().map(|c| c.id()).collect(),
                    &private_keys[idx],
                    &endorsements[..],
                    rng,
                )
            })
            .collect();
        certificates_by_round.insert(leader_round_2, round4_certs.clone());

        // Insert all certificates into the storage and DAG.
        for certificate in certificates_by_round.into_iter().flat_map(|(_, certs)| certs) {
            storage.testing_only_insert_certificate_testing_only(certificate.clone());
            bft.add_certificate_from_sync(certificate);
        }

        let leader_certificate_2 = storage.get_certificate_for_round_with_author(leader_round_2, leader2).unwrap();

        assert!(
            bft.dag.read().is_linked(leader_certificate_1.clone(), leader_certificate_2.clone()).unwrap(),
            "Leader certificate 1 should be linked to leader certificate 2"
        );

        // Explicitely commit leader certificate 2.
        bft.commit_leader_certificate(leader_certificate_2.clone()).await.unwrap();

        // Leader certificate 1 should be committed transitively when committing the leader certificate 2.
        assert!(
            bft.dag.read().is_recently_committed(leader_round_1, leader_certificate_1.id()),
            "Leader certificate for round 2 should be committed when committing at round 4"
        );

        // Leader certificate 2 should be committed as the above call was successful.
        assert!(
            bft.dag.read().is_recently_committed(leader_round_2, leader_certificate_2.id()),
            "Leader certificate for round 4 should be committed"
        );

        assert_eq!(bft.dag.read().last_committed_round(), 4);
    }

    #[test_log::test(tokio::test)]
    async fn test_commit_via_is_linked_with_skipped_anchor() {
        let rng = &mut TestRng::default();

        let committee_round = 0;
        let leader_round_1 = 2;
        let leader_round_2 = 4;
        let max_gc_rounds = 50;

        let num_authors = 4;
        let private_keys: Vec<_> = (0..num_authors).map(|_| PrivateKey::new(rng).unwrap()).collect();
        let addresses: Vec<_> = private_keys.iter().map(|pkey| Address::try_from(pkey).unwrap()).collect();

        let committee = sample_committee_for_round_and_members(committee_round, addresses.clone(), rng);
        let ledger = Arc::new(MockLedgerService::new(committee.clone()));
        let storage = Storage::new(ledger.clone(), Arc::new(BFTMemoryService::new()), max_gc_rounds).unwrap();
        let bft = initialize_bft(Account::new(rng).unwrap(), storage.clone(), ledger.clone()).unwrap();

        let mut certificates_by_round: IndexMap<u64, IndexSet<BatchCertificate<CurrentNetwork>>> = IndexMap::new();

        // Round 1
        let round1_certs: IndexSet<_> = (0..num_authors)
            .map(|idx| {
                let author = &private_keys[idx];
                let endorsements: Vec<_> = private_keys
                    .iter()
                    .enumerate()
                    .filter_map(|(other_idx, pkey)| if idx == other_idx { None } else { Some(*pkey) })
                    .collect();

                sample_batch_certificate_for_round_with_committee(1, IndexSet::new(), author, &endorsements[..], rng)
            })
            .collect();
        certificates_by_round.insert(1, round1_certs.clone());

        let leader1 = ledger.get_committee_for_round(leader_round_1 + 1).unwrap().get_leader(leader_round_1).unwrap();
        let mut leader1_certificate = None;

        let round2_certs: IndexSet<_> = (0..num_authors)
            .map(|idx| {
                let author = &private_keys[idx];
                let endorsements: Vec<_> = private_keys
                    .iter()
                    .enumerate()
                    .filter_map(|(other_idx, pkey)| if idx == other_idx { None } else { Some(*pkey) })
                    .collect();
                let cert = sample_batch_certificate_for_round_with_committee(
                    leader_round_1,
                    round1_certs.iter().map(|c| c.id()).collect(),
                    author,
                    &endorsements[..],
                    rng,
                );

                if cert.author() == leader1 {
                    leader1_certificate = Some(cert.clone());
                }
                cert
            })
            .collect();
        certificates_by_round.insert(leader_round_1, round2_certs.clone());

        let round3_certs: IndexSet<_> = (0..num_authors)
            .map(|idx| {
                let author = &private_keys[idx];
                let endorsements: Vec<_> = private_keys
                    .iter()
                    .enumerate()
                    .filter_map(|(other_idx, pkey)| if idx == other_idx { None } else { Some(*pkey) })
                    .collect();

                let previous_certificate_ids: IndexSet<_> = round2_certs
                    .iter()
                    .filter_map(|cert| {
                        // Only have the leader endorse the previous round's leader certificate.
                        if cert.author() == leader1 && cert.author() != addresses[idx] { None } else { Some(cert.id()) }
                    })
                    .collect();

                sample_batch_certificate_for_round_with_committee(
                    leader_round_1 + 1,
                    previous_certificate_ids,
                    author,
                    &endorsements[..],
                    rng,
                )
            })
            .collect();
        certificates_by_round.insert(leader_round_1 + 1, round3_certs.clone());

        // Ensure the first leader's certificate is not committed yet.
        let leader_certificate_1 = leader1_certificate.unwrap();
        assert!(
            !bft.dag.read().is_recently_committed(leader_round_1, leader_certificate_1.id()),
            "Leader certificate 1 should not be committed yet"
        );

        let leader2 = ledger.get_committee_for_round(leader_round_2 + 1).unwrap().get_leader(leader_round_2).unwrap();
        let round4_certs: IndexSet<_> = (0..num_authors)
            .map(|idx| {
                let endorsements: Vec<_> = private_keys
                    .iter()
                    .enumerate()
                    .filter_map(|(other_idx, pkey)| if idx == other_idx { None } else { Some(*pkey) })
                    .collect();

                // Do not create a path to the previous leader certificate.
                let previous_certificate_ids: IndexSet<_> = round3_certs
                    .iter()
                    .filter_map(|cert| if cert.author() == leader1 { None } else { Some(cert.id()) })
                    .collect();

                sample_batch_certificate_for_round_with_committee(
                    leader_round_2,
                    previous_certificate_ids,
                    &private_keys[idx],
                    &endorsements[..],
                    rng,
                )
            })
            .collect();
        certificates_by_round.insert(leader_round_2, round4_certs.clone());

        // Insert all certificates into the storage and DAG.
        for certificate in certificates_by_round.into_iter().flat_map(|(_, certs)| certs) {
            storage.testing_only_insert_certificate_testing_only(certificate.clone());
            bft.add_certificate_from_sync(certificate);
        }

        let leader_certificate_2 = storage.get_certificate_for_round_with_author(leader_round_2, leader2).unwrap();

        assert!(
            !bft.dag.read().is_linked(leader_certificate_1.clone(), leader_certificate_2.clone()).unwrap(),
            "Leader certificate 1 should not be linked to leader certificate 2"
        );
        assert_eq!(bft.dag.read().last_committed_round(), 0);

        // Explicitely commit leader certificate 2.
        bft.commit_leader_certificate(leader_certificate_2.clone()).await.unwrap();

        // Leader certificate 1 should be committed transitively when committing the leader certificate 2.
        assert!(
            !bft.dag.read().is_recently_committed(leader_round_1, leader_certificate_1.id()),
            "Leader certificate for round 2 should not be committed when committing at round 4"
        );

        // Leader certificate 2 should be committed as the above call was successful.
        assert!(
            bft.dag.read().is_recently_committed(leader_round_2, leader_certificate_2.id()),
            "Leader certificate for round 4 should be committed"
        );
        assert_eq!(bft.dag.read().last_committed_round(), 4);
    }
}
