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

#[cfg(not(test))]
use crate::Gateway;
use crate::{
    MAX_FETCH_TIMEOUT,
    MAX_WORKERS,
    ProposedBatch,
    ProposedBatchState,
    Transport,
    events::{Event, TransmissionRequest, TransmissionResponse},
    helpers::{Pending, Ready, Storage, WorkerReceiver, fmt_id, max_redundant_requests},
    spawn_blocking,
};
use snarkos_node_bft_ledger_service::{LedgerService, deserialize_transaction_strict};
use snarkvm::{
    console::prelude::*,
    ledger::{
        Transaction,
        narwhal::{BatchHeader, Data, Transmission, TransmissionID},
        puzzle::{Solution, SolutionID},
    },
};

use anyhow::Context;
use indexmap::{IndexMap, IndexSet};
#[cfg(feature = "locktick")]
use locktick::parking_lot::{Mutex, RwLock};
#[cfg(not(feature = "locktick"))]
use parking_lot::{Mutex, RwLock};
use rand::seq::IteratorRandom;

use std::{future::Future, net::SocketAddr, sync::Arc};
use tokio::{sync::oneshot, task::JoinHandle, time::timeout};

/// A worker's main role is maintaining a queue of verified ("ready") transmissions,
/// which will eventually be fetched by the primary when the primary generates a new batch.
#[derive(Clone)]
pub struct Worker<N: Network> {
    /// The worker ID.
    id: u8,
    /// The gateway.
    #[cfg(not(test))]
    gateway: Arc<Gateway<N>>,
    #[cfg(test)]
    gateway: Arc<dyn Transport<N>>,
    /// The storage.
    storage: Storage<N>,
    /// The ledger service.
    ledger: Arc<dyn LedgerService<N>>,
    /// The proposed batch.
    proposed_batch: Arc<ProposedBatch<N>>,
    /// The ready queue.
    ready: Arc<RwLock<Ready<N>>>,
    /// The pending transmissions queue.
    pending: Arc<Pending<TransmissionID<N>, Transmission<N>>>,
    /// The spawned handles.
    handles: Arc<Mutex<Vec<JoinHandle<()>>>>,
}

impl<N: Network> Worker<N> {
    /// Initializes a new worker instance.
    pub fn new(
        id: u8,
        #[cfg(not(test))] gateway: Arc<Gateway<N>>,
        #[cfg(test)] gateway: Arc<dyn Transport<N>>,
        storage: Storage<N>,
        ledger: Arc<dyn LedgerService<N>>,
        proposed_batch: Arc<ProposedBatch<N>>,
    ) -> Result<Self> {
        // Ensure the worker ID is valid.
        ensure!(id < MAX_WORKERS, "Invalid worker ID '{id}'");
        // Return the worker.
        Ok(Self {
            id,
            gateway,
            storage,
            ledger,
            proposed_batch,
            ready: Default::default(),
            pending: Default::default(),
            handles: Default::default(),
        })
    }

    /// Run the worker instance.
    pub fn run(&self, receiver: WorkerReceiver<N>) {
        info!("Starting worker instance {} of the memory pool...", self.id);
        // Start the worker handlers.
        self.start_handlers(receiver);
    }

    /// Returns the worker ID.
    pub const fn id(&self) -> u8 {
        self.id
    }

    /// Returns a reference to the pending transmissions queue.
    pub fn pending(&self) -> &Arc<Pending<TransmissionID<N>, Transmission<N>>> {
        &self.pending
    }
}

impl<N: Network> Worker<N> {
    /// The maximum number of transmissions allowed in a worker.
    pub const MAX_TRANSMISSIONS_PER_WORKER: usize =
        BatchHeader::<N>::MAX_TRANSMISSIONS_PER_BATCH / MAX_WORKERS as usize;
    /// The maximum number of transmissions allowed in a worker ping.
    pub const MAX_TRANSMISSIONS_PER_WORKER_PING: usize = BatchHeader::<N>::MAX_TRANSMISSIONS_PER_BATCH / 10;

    /// Returns the number of transmissions in the ready queue.
    pub fn num_transmissions(&self) -> usize {
        self.ready.read().num_transmissions()
    }

    /// Returns the number of ratifications in the ready queue.
    pub fn num_ratifications(&self) -> usize {
        self.ready.read().num_ratifications()
    }

    /// Returns the number of solutions in the ready queue.
    pub fn num_solutions(&self) -> usize {
        self.ready.read().num_solutions()
    }

    /// Returns the number of transactions in the ready queue.
    pub fn num_transactions(&self) -> usize {
        self.ready.read().num_transactions()
    }
}

impl<N: Network> Worker<N> {
    /// Returns the transmission IDs in the ready queue.
    pub fn transmission_ids(&self) -> IndexSet<TransmissionID<N>> {
        self.ready.read().transmission_ids()
    }

    /// Returns the transmissions in the ready queue.
    pub fn transmissions(&self) -> IndexMap<TransmissionID<N>, Transmission<N>> {
        self.ready.read().transmissions()
    }

    /// Returns the solutions in the ready queue.
    pub fn solutions(&self) -> impl '_ + Iterator<Item = (SolutionID<N>, Data<Solution<N>>)> {
        self.ready.read().solutions().into_iter()
    }

    /// Returns the transactions in the ready queue.
    pub fn transactions(&self) -> impl '_ + Iterator<Item = (N::TransactionID, Data<Transaction<N>>)> {
        self.ready.read().transactions().into_iter()
    }
}

impl<N: Network> Worker<N> {
    /// Clears the solutions from the ready queue.
    pub(super) fn clear_solutions(&self) {
        self.ready.write().clear_solutions()
    }
}

impl<N: Network> Worker<N> {
    // Helper to print the transmission ID and checksum (if any).
    fn format_transmission_id(&self, transmission_id: TransmissionID<N>) -> String {
        if let Some(checksum) = transmission_id.checksum() {
            // fmt_id will suffix with `..`, so we should not use `.`  as a separator.
            format!("{}:{}", fmt_id(transmission_id), fmt_id(checksum))
        } else {
            fmt_id(transmission_id)
        }
    }

    /// Returns `true` if the transmission ID exists in the ready queue, proposed batch, storage, or ledger.
    ///
    /// Note that the storage check is the retrievable one: an ID that BFT storage knows only as
    /// aborted holds no payload, so the worker still wants the bytes - to retain what a fetch just
    /// produced, and to serve them to peers. The ledger check is deliberately the wider one, since
    /// an ID the ledger knows - confirmed, rejected, or aborted - must not re-enter the queue.
    pub fn contains_transmission(&self, transmission_id: impl Into<TransmissionID<N>>) -> bool {
        let transmission_id = transmission_id.into();
        // Check if the transmission ID exists in the ready queue, proposed batch, storage, or ledger.
        self.ready.read().contains(transmission_id)
            || matches!(&*self.proposed_batch.read(), ProposedBatchState::Certifying(p) if p.contains_transmission(transmission_id))
            || self.storage.contains_retrievable_transmission(transmission_id)
            || self.ledger.contains_transmission(&transmission_id).unwrap_or(false)
    }

    /// Returns the transmission if it exists in the ready queue, proposed batch, storage.
    ///
    /// Note: We explicitly forbid retrieving a transmission from the ledger, as transmissions
    /// in the ledger are not guaranteed to be invalid for the current batch.
    pub fn get_transmission(&self, transmission_id: TransmissionID<N>) -> Option<Transmission<N>> {
        // Check if the transmission ID exists in the ready queue.
        if let Some(transmission) = self.ready.read().get(transmission_id) {
            return Some(transmission);
        }
        // Check if the transmission ID exists in storage.
        if let Some(transmission) = self.storage.get_transmission(transmission_id) {
            return Some(transmission);
        }
        // Check if the transmission ID exists in the proposed batch.
        if let Some(transmission) = match &*self.proposed_batch.read() {
            ProposedBatchState::Certifying(p) => p.get_transmission(transmission_id),
            _ => None,
        } {
            return Some(transmission.clone());
        }
        None
    }

    /// Returns the transmissions if it exists in the worker, or requests it from the specified peer.
    pub async fn get_or_fetch_transmission(
        &self,
        peer_ip: SocketAddr,
        transmission_id: TransmissionID<N>,
    ) -> Result<(TransmissionID<N>, Transmission<N>)> {
        // Attempt to get the transmission from the worker.
        if let Some(transmission) = self.get_transmission(transmission_id) {
            return Ok((transmission_id, transmission));
        }
        // Send a transmission request to the peer.
        let (candidate_id, transmission) = self.send_transmission_request(peer_ip, transmission_id).await?;
        // Ensure the transmission ID matches.
        ensure!(candidate_id == transmission_id, "Invalid transmission ID");
        // Return the transmission.
        Ok((transmission_id, transmission))
    }

    /// Inserts the transmission at the front of the ready queue.
    pub(crate) fn insert_front(&self, key: TransmissionID<N>, value: Transmission<N>) {
        self.ready.write().insert_front(key, value);
    }

    /// Removes and returns the transmission at the front of the ready queue.
    pub(crate) fn remove_front(&self) -> Option<(TransmissionID<N>, Transmission<N>)> {
        self.ready.write().remove_front()
    }

    /// Reinserts the specified transmission into the ready queue.
    pub(crate) fn reinsert(&self, transmission_id: TransmissionID<N>, transmission: Transmission<N>) -> bool {
        // Check if the transmission ID exists.
        if !self.contains_transmission(transmission_id) {
            // Insert the transmission into the ready queue.
            return self.ready.write().insert(transmission_id, transmission);
        }
        false
    }

    /// Broadcasts a worker ping event.
    pub(crate) fn broadcast_ping(&self) {
        // Retrieve the transmission IDs.
        let transmission_ids = self
            .ready
            .read()
            .transmission_ids()
            .into_iter()
            .sample(&mut rand::rng(), Self::MAX_TRANSMISSIONS_PER_WORKER_PING)
            .into_iter()
            .collect::<IndexSet<_>>();

        // Broadcast the ping event.
        if !transmission_ids.is_empty() {
            self.gateway.broadcast(Event::WorkerPing(transmission_ids.into()));
        }
    }
}

impl<N: Network> Worker<N> {
    /// Handles the incoming transmission ID from a worker ping event.
    fn process_transmission_id_from_ping(&self, peer_ip: SocketAddr, transmission_id: TransmissionID<N>) {
        // Check if the transmission ID exists.
        if self.contains_transmission(transmission_id) {
            return;
        }
        // If the ready queue is full, then skip this transmission.
        // Note: We must prioritize the unconfirmed solutions and unconfirmed transactions, not transmissions.
        if self.ready.read().num_transmissions() > Self::MAX_TRANSMISSIONS_PER_WORKER {
            return;
        }
        // Attempt to fetch the transmission from the peer.
        let self_ = self.clone();
        tokio::spawn(async move {
            // Send a transmission request to the peer.
            match self_.send_transmission_request(peer_ip, transmission_id).await {
                // If the transmission was fetched, then process it.
                Ok((candidate_id, transmission)) => {
                    // Ensure the transmission ID matches.
                    if candidate_id == transmission_id {
                        // Insert the transmission into the ready queue.
                        // Note: This method checks `contains_transmission` again, because by the time the transmission is fetched,
                        // it could have already been inserted into the ready queue.
                        self_.process_transmission_from_ping(peer_ip, transmission_id, transmission);
                    }
                }
                // If the transmission was not fetched, then attempt to fetch it again.
                Err(e) => {
                    warn!(
                        "Worker {} - Failed to fetch transmission '{}' from '{peer_ip}' (ping) - {e}",
                        self_.id,
                        self_.format_transmission_id(transmission_id),
                    );
                }
            }
        });
    }

    /// Handles a transmission fetched in response to a worker ping event.
    ///
    /// Unlike [`Worker::process_transmission_from_peer`], this enforces the ready-queue capacity
    /// limit (atomically with the insertion), dropping the transmission if the queue is full.
    ///
    /// This is required to close a time-of-check-to-time-of-use race: the capacity check in
    /// `process_transmission_id_from_ping` runs before the transmission is fetched asynchronously,
    /// so without a re-check at insertion time, many concurrent fetches could each observe a
    /// non-full ready queue and then collectively exceed `MAX_TRANSMISSIONS_PER_WORKER`.
    ///
    /// Note: We must prioritize the unconfirmed solutions and unconfirmed transactions, not
    /// transmissions fetched from peer pings, which is why the limit is enforced on this path.
    fn process_transmission_from_ping(
        &self,
        peer_ip: SocketAddr,
        transmission_id: TransmissionID<N>,
        transmission: Transmission<N>,
    ) {
        self.insert_transmission_from_peer(peer_ip, transmission_id, transmission, true);
    }

    /// Handles the incoming transmission from a peer.
    ///
    /// Note: This does *not* enforce the ready-queue capacity limit, because this path also
    /// materializes the transmissions of a peer's batch proposal prior to signing it (see
    /// `Primary::insert_missing_transmissions_into_workers`); dropping a transmission there would
    /// cause the node to sign a batch whose transmissions it does not hold. The capacity limit is
    /// enforced only on the worker-ping path, via [`Worker::process_transmission_from_ping`].
    pub(crate) fn process_transmission_from_peer(
        &self,
        peer_ip: SocketAddr,
        transmission_id: TransmissionID<N>,
        transmission: Transmission<N>,
    ) {
        self.insert_transmission_from_peer(peer_ip, transmission_id, transmission, false);
    }

    /// Inserts a peer-provided transmission into the ready queue.
    ///
    /// If `enforce_capacity_limit` is `true`, the transmission is dropped when the ready queue is
    /// already full. This must only be set for the worker-ping fetch path; callers that must retain
    /// the transmission (e.g. materializing a peer's batch proposal before signing it) must leave it
    /// `false`. See [`Worker::process_transmission_from_ping`] and
    /// [`Worker::process_transmission_from_peer`].
    fn insert_transmission_from_peer(
        &self,
        peer_ip: SocketAddr,
        transmission_id: TransmissionID<N>,
        transmission: Transmission<N>,
        enforce_capacity_limit: bool,
    ) {
        // If the transmission ID already exists, then do not store it.
        if self.contains_transmission(transmission_id) {
            return;
        }
        // Ensure the transmission ID and transmission type matches.
        let is_well_formed = match (&transmission_id, &transmission) {
            (TransmissionID::Solution(_, _), Transmission::Solution(_)) => true,
            (TransmissionID::Transaction(_, _), Transmission::Transaction(_)) => true,
            // Note: We explicitly forbid inserting ratifications into the ready queue,
            // as the protocol currently does not support ratifications.
            (TransmissionID::Ratification, Transmission::Ratification) => false,
            // All other combinations are clearly invalid.
            _ => false,
        };
        // If the transmission type does not match the transmission ID, then do not store it.
        if !is_well_formed {
            return;
        }
        // If the transmission is a deserialized execution, capture it so it can be verified
        // immediately below (only if it is actually inserted). This takes heavy transaction
        // verification out of the hot path during block generation.
        let execution_to_verify = match (transmission_id, &transmission) {
            (TransmissionID::Transaction(tx_id, _), Transmission::Transaction(Data::Object(tx))) if tx.is_execute() => {
                Some((tx_id, tx.clone()))
            }
            _ => None,
        };
        // Insert the transmission into the ready queue. When the capacity limit is enforced, the
        // check is performed atomically with the insertion (i.e. under a single write lock), so that
        // concurrent inserts cannot collectively exceed `MAX_TRANSMISSIONS_PER_WORKER`.
        let inserted = {
            let mut ready = self.ready.write();
            // If the ready queue is full, then skip this transmission.
            if enforce_capacity_limit && ready.num_transmissions() > Self::MAX_TRANSMISSIONS_PER_WORKER {
                return;
            }
            ready.insert(transmission_id, transmission)
        };
        // If the transmission was newly inserted, then process it further.
        if inserted {
            // Eagerly verify a newly-inserted execution to warm the verification cache.
            if let Some((tx_id, tx)) = execution_to_verify {
                let self_ = self.clone();
                tokio::spawn(async move {
                    let _ = self_.ledger.check_transaction_basic(tx_id, tx).await;
                });
            }
            trace!(
                "Worker {} - Added transmission '{}' from '{peer_ip}'",
                self.id,
                self.format_transmission_id(transmission_id),
            );
        }
    }

    /// Handles the incoming unconfirmed solution.
    /// Note: This method assumes the incoming solution is valid and does not exist in the ledger.
    ///
    /// # Returns
    /// - `Ok(true)` if the solution was added to the ready queue.
    /// - `Ok(false)` if the solution was valid but already exists in the ready queue.
    /// - `Err(anyhow::Error)` if the solution is invalid.
    pub(crate) async fn process_unconfirmed_solution(
        &self,
        solution_id: SolutionID<N>,
        solution: Data<Solution<N>>,
    ) -> Result<bool> {
        // Construct the transmission.
        let transmission = Transmission::Solution(solution.clone());
        // Compute the checksum.
        let checksum = solution.to_checksum::<N>()?;
        // Construct the transmission ID.
        let transmission_id = TransmissionID::Solution(solution_id, checksum);
        // Remove the solution ID from the pending queue.
        self.pending.remove(transmission_id, Some(transmission.clone()));
        // Check if the solution exists.
        if self.contains_transmission(transmission_id) {
            return Ok(false);
        }
        // Check that the solution is well-formed and unique.
        self.ledger.check_solution_basic(solution_id, solution).await?;
        // Adds the solution to the ready queue.
        if self.ready.write().insert(transmission_id, transmission) {
            trace!(
                "Worker {} - Added unconfirmed solution '{}'",
                self.id,
                self.format_transmission_id(transmission_id),
            );
        }
        Ok(true)
    }

    /// Handles the incoming unconfirmed transaction.
    ///
    /// # Returns
    /// - `Ok(true)` if the transaction was added to the ready queue.
    /// - `Ok(false)` if the transaction was valid but already exists in the ready queue.
    /// - `Err(anyhow::Error)` if the transaction was invalid.
    pub(crate) async fn process_unconfirmed_transaction(
        &self,
        transaction_id: N::TransactionID,
        transaction: Data<Transaction<N>>,
    ) -> Result<bool> {
        // Construct the transmission.
        let transmission = Transmission::Transaction(transaction.clone());
        // Compute the checksum.
        let checksum = transaction.to_checksum::<N>()?;
        // Construct the transmission ID.
        let transmission_id = TransmissionID::Transaction(transaction_id, checksum);
        // Remove the transaction from the pending queue.
        self.pending.remove(transmission_id, Some(transmission.clone()));
        // Check if the transaction ID exists.
        if self.contains_transmission(transmission_id) {
            return Ok(false);
        }
        // Deserialize the transaction. If the transaction exceeds the maximum size, then return an error.
        let transaction = spawn_blocking!(deserialize_transaction_strict(transaction))?;

        // Check that the transaction is well-formed and unique.
        self.ledger.check_transaction_basic(transaction_id, transaction).await?;
        // Adds the transaction to the ready queue.
        if self.ready.write().insert(transmission_id, transmission) {
            trace!(
                "Worker {} - Added unconfirmed transaction '{}'",
                self.id,
                self.format_transmission_id(transmission_id),
            );
        }
        Ok(true)
    }
}

impl<N: Network> Worker<N> {
    /// Starts the worker handlers.
    fn start_handlers(&self, receiver: WorkerReceiver<N>) {
        let WorkerReceiver { mut rx_worker_ping, mut rx_transmission_request, mut rx_transmission_response } = receiver;

        // Start the pending queue expiration loop.
        let self_ = self.clone();
        self.spawn(async move {
            loop {
                // Sleep briefly.
                tokio::time::sleep(MAX_FETCH_TIMEOUT).await;

                // Remove the expired pending certificate requests.
                let self__ = self_.clone();
                let _ = spawn_blocking!({
                    self__.pending.clear_expired_callbacks();
                    Ok(())
                });
            }
        });

        // Process the ping events.
        let self_ = self.clone();
        self.spawn(async move {
            while let Some((peer_ip, transmission_id)) = rx_worker_ping.recv().await {
                self_.process_transmission_id_from_ping(peer_ip, transmission_id);
            }
        });

        // Process the transmission requests.
        let self_ = self.clone();
        self.spawn(async move {
            while let Some((peer_ip, transmission_request)) = rx_transmission_request.recv().await {
                self_.send_transmission_response(peer_ip, transmission_request);
            }
        });

        // Process the transmission responses.
        let self_ = self.clone();
        self.spawn(async move {
            while let Some((peer_ip, transmission_response)) = rx_transmission_response.recv().await {
                // Process the transmission response.
                let self__ = self_.clone();
                let _ = spawn_blocking!({
                    self__.finish_transmission_request(peer_ip, transmission_response);
                    Ok(())
                });
            }
        });
    }

    /// Sends a transmission request to the specified peer.
    async fn send_transmission_request(
        &self,
        peer_ip: SocketAddr,
        transmission_id: TransmissionID<N>,
    ) -> Result<(TransmissionID<N>, Transmission<N>)> {
        // Initialize a oneshot channel.
        let (callback_sender, callback_receiver) = oneshot::channel();
        // Determine how many sent requests are pending.
        let num_sent_requests = self.pending.num_sent_requests(transmission_id);
        // Determine if we've already sent a request to the peer.
        let contains_peer_with_sent_request = self.pending.contains_peer_with_sent_request(transmission_id, peer_ip);
        // Determine the maximum number of redundant requests.
        let num_redundant_requests = max_redundant_requests(self.ledger.clone(), self.storage.current_round())?;
        // Establish whether the peers who already got the request collectively hold sufficient stake.
        #[cfg(test)]
        let stake_redundancy_reached = || Ok::<_, anyhow::Error>(true);
        #[cfg(not(test))]
        let stake_redundancy_reached = || self.pending.request_stake_redundancy_reached(&self.gateway, transmission_id);
        // Determine if we should send a transmission request to the peer.
        // Each peer can only receive one request at a time.
        // We send at most `num_redundant_requests` requests, unless the stake redundancy factor hasn't been reached.
        let should_send_request = !contains_peer_with_sent_request
            && (num_sent_requests < num_redundant_requests || !stake_redundancy_reached()?);

        // Insert the transmission ID into the pending queue.
        self.pending.insert(transmission_id, peer_ip, Some((callback_sender, should_send_request)));

        // If the number of requests is less than or equal to the the redundancy factor, send the transmission request to the peer.
        if should_send_request {
            trace!("Requesting transmission {} from peer '{peer_ip}'", self.format_transmission_id(transmission_id));
            // Send the transmission request to the peer.
            if self.gateway.send(peer_ip, Event::TransmissionRequest(transmission_id.into())).await.is_none() {
                bail!(
                    "Unable to fetch transmission {} - failed to send request",
                    self.format_transmission_id(transmission_id)
                )
            }
        } else {
            debug!(
                "Skipped sending request for transmission {} to '{peer_ip}' ({num_sent_requests} redundant requests)",
                self.format_transmission_id(transmission_id)
            );
        }

        // Wait for the transmission to be fetched.
        let transmission = timeout(MAX_FETCH_TIMEOUT, callback_receiver)
            .await
            .with_context(|| {
                format!("Unable to fetch transmission {} (timeout)", self.format_transmission_id(transmission_id))
            })?
            .with_context(|| {
                format!("Unable to fetch transmission {}", self.format_transmission_id(transmission_id))
            })?;

        Ok((transmission_id, transmission))
    }

    /// Handles the incoming transmission response.
    /// This method ensures the transmission response is well-formed and matches the transmission ID.
    fn finish_transmission_request(&self, peer_ip: SocketAddr, response: TransmissionResponse<N>) {
        let TransmissionResponse { transmission_id, mut transmission } = response;
        // Check if the peer IP exists in the pending queue for the given transmission ID.
        let exists = self.pending.get_peers(transmission_id).unwrap_or_default().contains(&peer_ip);
        // If the peer IP exists, finish the pending request.
        if exists {
            // Ensure the transmission is not a fee and matches the transmission ID.
            match self.ledger.ensure_transmission_is_well_formed(transmission_id, &mut transmission) {
                Ok(()) => {
                    trace!(
                        "Received valid transmission response from peer '{peer_ip}' for transmission '{}'",
                        self.format_transmission_id(transmission_id)
                    );
                    // Remove the transmission ID from the pending queue.
                    self.pending.remove(transmission_id, Some(transmission));
                }
                Err(err) => warn!("Failed to finish transmission response from peer '{peer_ip}': {err}"),
            };
        }
    }

    /// Sends the requested transmission to the specified peer.
    fn send_transmission_response(&self, peer_ip: SocketAddr, request: TransmissionRequest<N>) {
        let TransmissionRequest { transmission_id } = request;
        // Attempt to retrieve the transmission.
        if let Some(transmission) = self.get_transmission(transmission_id) {
            // Send the transmission response to the peer.
            let self_ = self.clone();
            tokio::spawn(async move {
                self_.gateway.send(peer_ip, Event::TransmissionResponse((transmission_id, transmission).into())).await;
            });
        }
    }

    /// Spawns a task with the given future; it should only be used for long-running tasks.
    fn spawn<T: Future<Output = ()> + Send + 'static>(&self, future: T) {
        self.handles.lock().push(tokio::spawn(future));
    }

    /// Shuts down the worker.
    pub(crate) fn shut_down(&self) {
        trace!("Shutting down worker {}...", self.id);
        // Abort the tasks.
        self.handles.lock().iter().for_each(|handle| handle.abort());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::helpers::CALLBACK_EXPIRATION_IN_SECS;
    use snarkos_node_bft_ledger_service::{BeginLedgerUpdateError, LedgerService, LedgerUpdateService};
    use snarkos_node_bft_storage_service::BFTMemoryService;
    use snarkvm::{
        console::{network::Network, types::Field},
        ledger::{
            Block,
            CheckBlockError,
            PendingBlock,
            committee::Committee,
            narwhal::{BatchCertificate, Transmission, TransmissionID},
            test_helpers::sample_execution_transaction_with_fee,
        },
        prelude::Address,
    };

    use bytes::Bytes;
    use mockall::mock;
    use rand::RngExt;
    use std::{io, ops::Range, time::Duration};

    type CurrentNetwork = snarkvm::prelude::MainnetV0;

    const ITERATIONS: usize = 100;

    mock! {
        Gateway<N: Network> {}
        #[async_trait]
        impl<N:Network> Transport<N> for Gateway<N> {
            fn broadcast(&self, event: Event<N>);
            fn broadcast_until(&self, event: Event<N>, token: tokio_util::sync::CancellationToken);
            async fn send(&self, peer_ip: SocketAddr, event: Event<N>) -> Option<oneshot::Receiver<io::Result<()>>>;
        }
    }

    mock! {
        #[derive(Debug)]
        Ledger<N: Network> {}
        #[async_trait]
        impl<N: Network> LedgerService<N> for Ledger<N> {
            fn latest_round(&self) -> u64;
            fn latest_block_height(&self) -> u32;
            fn latest_block(&self) -> Block<N>;
            fn latest_restrictions_id(&self) -> Field<N>;
            fn latest_leader(&self) -> Option<(u64, Address<N>)>;
            fn update_latest_leader(&self, round: u64, leader: Address<N>);
            fn contains_block_height(&self, height: u32) -> bool;
            fn get_block_height(&self, hash: &N::BlockHash) -> Result<u32>;
            fn get_block_hash(&self, height: u32) -> Result<N::BlockHash>;
            fn get_block_round(&self, height: u32) -> Result<u64>;
            fn get_block(&self, height: u32) -> Result<Block<N>>;
            fn get_blocks(&self, heights: Range<u32>) -> Result<Vec<Block<N>>>;
            fn get_solution(&self, solution_id: &SolutionID<N>) -> Result<Option<Solution<N>>>;
            fn get_unconfirmed_transaction(&self, transaction_id: N::TransactionID) -> Result<Option<Transaction<N>>>;
            fn get_batch_certificate(&self, certificate_id: &Field<N>) -> Result<BatchCertificate<N>>;
            fn current_committee(&self) -> Result<Committee<N>>;
            fn get_committee_for_round(&self, round: u64) -> Result<Committee<N>>;
            fn get_committee_lookback_for_round(&self, round: u64) -> Result<Committee<N>>;
            fn contains_certificate(&self, certificate_id: &Field<N>) -> Result<bool>;
            fn contains_transmission(&self, transmission_id: &TransmissionID<N>) -> Result<bool>;
            fn ensure_transmission_is_well_formed(
                &self,
                transmission_id: TransmissionID<N>,
                transmission: &mut Transmission<N>,
            ) -> Result<()>;
            async fn check_solution_basic(
                &self,
                solution_id: SolutionID<N>,
                solution: Data<Solution<N>>,
            ) -> Result<()>;
            async fn check_transaction_basic(
                &self,
                transaction_id: N::TransactionID,
                transaction: Transaction<N>,
            ) -> Result<()>;
            fn check_block_subdag(&self, _block: Block<N>, _prefix: &[PendingBlock<N>]) -> Result<PendingBlock<N>, CheckBlockError<N>>;
            fn begin_ledger_update<'a>(&'a self) -> Result<Box<dyn LedgerUpdateService<N> + 'a>, BeginLedgerUpdateError>;
            fn transaction_spend_in_microcredits(&self, transaction: &Transaction<N>, consensus_version: ConsensusVersion) -> Result<u64>;
            fn is_stopped(&self) -> bool;
        }
    }

    #[tokio::test]
    async fn test_max_redundant_requests() {
        let num_nodes: u16 = CurrentNetwork::MAX_CERTIFICATES.first().unwrap().1;

        let rng = &mut TestRng::default();
        // Sample a committee.
        let committee =
            snarkvm::ledger::committee::test_helpers::sample_committee_for_round_and_size(0, num_nodes, rng);
        let committee_clone = committee.clone();
        // Setup the mock ledger.
        let mut mock_ledger = MockLedger::default();
        mock_ledger.expect_current_committee().returning(move || Ok(committee.clone()));
        mock_ledger.expect_get_committee_lookback_for_round().returning(move |_| Ok(committee_clone.clone()));
        mock_ledger.expect_contains_transmission().returning(|_| Ok(false));
        mock_ledger.expect_check_solution_basic().returning(|_, _| Ok(()));
        let ledger: Arc<dyn LedgerService<CurrentNetwork>> = Arc::new(mock_ledger);

        // Ensure the maximum number of redundant requests is correct and consistent across iterations.
        assert_eq!(max_redundant_requests(ledger, 0).unwrap(), 6, "Update me if the formula changes");
    }

    #[tokio::test]
    async fn test_process_transmission() {
        let rng = &mut TestRng::default();
        // Sample a committee.
        let committee = snarkvm::ledger::committee::test_helpers::sample_committee(rng);
        let committee_clone = committee.clone();
        // Setup the mock gateway and ledger.
        let gateway = MockGateway::default();
        let mut mock_ledger = MockLedger::default();
        mock_ledger.expect_current_committee().returning(move || Ok(committee.clone()));
        mock_ledger.expect_get_committee_lookback_for_round().returning(move |_| Ok(committee_clone.clone()));
        mock_ledger.expect_contains_transmission().returning(|_| Ok(false));
        mock_ledger.expect_check_solution_basic().returning(|_, _| Ok(()));
        let ledger: Arc<dyn LedgerService<CurrentNetwork>> = Arc::new(mock_ledger);
        // Initialize the storage.
        let storage = Storage::<CurrentNetwork>::new(ledger.clone(), Arc::new(BFTMemoryService::new()), 1).unwrap();

        // Create the Worker.
        let worker = Worker::new(0, Arc::new(gateway), storage, ledger, Default::default()).unwrap();
        let data =
            |rng: &mut TestRng| Data::Buffer(Bytes::from((0..512).map(|_| rng.random::<u8>()).collect::<Vec<_>>()));
        let transmission_id = TransmissionID::Solution(
            rng.random::<u64>().into(),
            rng.random::<<CurrentNetwork as Network>::TransmissionChecksum>(),
        );
        let peer_ip = SocketAddr::from(([127, 0, 0, 1], 1234));
        let transmission = Transmission::Solution(data(rng));

        // Process the transmission.
        worker.process_transmission_from_peer(peer_ip, transmission_id, transmission.clone());
        assert!(worker.contains_transmission(transmission_id));
        assert!(worker.ready.read().contains(transmission_id));
        assert_eq!(worker.get_transmission(transmission_id), Some(transmission));
        // Take the transmission from the ready set.
        assert!(worker.ready.write().remove_front().is_some());
        assert!(!worker.ready.read().contains(transmission_id));
    }

    #[tokio::test]
    async fn test_process_transmission_from_ping_respects_capacity() {
        let rng = &mut TestRng::default();
        // Sample a committee.
        let committee = snarkvm::ledger::committee::test_helpers::sample_committee(rng);
        let committee_clone = committee.clone();
        // Setup the mock gateway and ledger.
        let gateway = MockGateway::default();
        let mut mock_ledger = MockLedger::default();
        mock_ledger.expect_current_committee().returning(move || Ok(committee.clone()));
        mock_ledger.expect_get_committee_lookback_for_round().returning(move |_| Ok(committee_clone.clone()));
        mock_ledger.expect_contains_transmission().returning(|_| Ok(false));
        let ledger: Arc<dyn LedgerService<CurrentNetwork>> = Arc::new(mock_ledger);
        // Initialize the storage.
        let storage = Storage::<CurrentNetwork>::new(ledger.clone(), Arc::new(BFTMemoryService::new()), 1).unwrap();

        // Create the Worker.
        let worker = Worker::new(0, Arc::new(gateway), storage, ledger, Default::default()).unwrap();
        let data =
            |rng: &mut TestRng| Data::Buffer(Bytes::from((0..512).map(|_| rng.random::<u8>()).collect::<Vec<_>>()));
        let peer_ip = SocketAddr::from(([127, 0, 0, 1], 1234));

        // Emulate the asynchronous worker-ping fetch path inserting far more transmissions than the
        // per-worker limit. Each fetched transmission is inserted via `process_transmission_from_ping`,
        // exactly as it would be when a `send_transmission_request` future resolves. Without an insertion
        // time capacity check, the TOCTOU in `process_transmission_id_from_ping` lets these accumulate
        // without bound.
        let num_to_insert = Worker::<CurrentNetwork>::MAX_TRANSMISSIONS_PER_WORKER * 4;
        for _ in 0..num_to_insert {
            let transmission_id = TransmissionID::Solution(
                rng.random::<u64>().into(),
                rng.random::<<CurrentNetwork as Network>::TransmissionChecksum>(),
            );
            let transmission = Transmission::Solution(data(rng));
            worker.process_transmission_from_ping(peer_ip, transmission_id, transmission);
        }

        // The worker-ping path must not grow the ready queue beyond the per-worker limit.
        // At most one extra transmission is tolerated (the insertion that crosses the boundary).
        assert!(
            worker.num_transmissions() <= Worker::<CurrentNetwork>::MAX_TRANSMISSIONS_PER_WORKER + 1,
            "ready queue exceeded the per-worker limit: {} > {}",
            worker.num_transmissions(),
            Worker::<CurrentNetwork>::MAX_TRANSMISSIONS_PER_WORKER + 1,
        );
    }

    #[tokio::test]
    async fn test_process_transmission_from_peer_ignores_capacity() {
        let rng = &mut TestRng::default();
        // Sample a committee.
        let committee = snarkvm::ledger::committee::test_helpers::sample_committee(rng);
        let committee_clone = committee.clone();
        // Setup the mock gateway and ledger.
        let gateway = MockGateway::default();
        let mut mock_ledger = MockLedger::default();
        mock_ledger.expect_current_committee().returning(move || Ok(committee.clone()));
        mock_ledger.expect_get_committee_lookback_for_round().returning(move |_| Ok(committee_clone.clone()));
        mock_ledger.expect_contains_transmission().returning(|_| Ok(false));
        let ledger: Arc<dyn LedgerService<CurrentNetwork>> = Arc::new(mock_ledger);
        // Initialize the storage.
        let storage = Storage::<CurrentNetwork>::new(ledger.clone(), Arc::new(BFTMemoryService::new()), 1).unwrap();

        // Create the Worker.
        let worker = Worker::new(0, Arc::new(gateway), storage, ledger, Default::default()).unwrap();
        let data =
            |rng: &mut TestRng| Data::Buffer(Bytes::from((0..512).map(|_| rng.random::<u8>()).collect::<Vec<_>>()));
        let peer_ip = SocketAddr::from(([127, 0, 0, 1], 1234));

        // The batch-materialization path (`process_transmission_from_peer`) must retain every
        // transmission, even beyond the per-worker limit, so the node never signs a peer's batch
        // whose transmissions it does not hold.
        let num_to_insert = Worker::<CurrentNetwork>::MAX_TRANSMISSIONS_PER_WORKER * 4;
        for _ in 0..num_to_insert {
            let transmission_id = TransmissionID::Solution(
                rng.random::<u64>().into(),
                rng.random::<<CurrentNetwork as Network>::TransmissionChecksum>(),
            );
            let transmission = Transmission::Solution(data(rng));
            worker.process_transmission_from_peer(peer_ip, transmission_id, transmission);
        }

        // Every transmission was retained.
        assert_eq!(worker.num_transmissions(), num_to_insert);
    }

    #[tokio::test]
    async fn test_send_transmission() {
        let rng = &mut TestRng::default();
        // Sample a committee.
        let committee = snarkvm::ledger::committee::test_helpers::sample_committee(rng);
        let committee_clone = committee.clone();
        // Setup the mock gateway and ledger.
        let mut gateway = MockGateway::default();
        gateway.expect_send().returning(|_, _| {
            let (_tx, rx) = oneshot::channel();
            Some(rx)
        });
        let mut mock_ledger = MockLedger::default();
        mock_ledger.expect_current_committee().returning(move || Ok(committee.clone()));
        mock_ledger.expect_get_committee_lookback_for_round().returning(move |_| Ok(committee_clone.clone()));
        mock_ledger.expect_ensure_transmission_is_well_formed().returning(|_, _| Ok(()));
        let ledger: Arc<dyn LedgerService<CurrentNetwork>> = Arc::new(mock_ledger);
        // Initialize the storage.
        let storage = Storage::<CurrentNetwork>::new(ledger.clone(), Arc::new(BFTMemoryService::new()), 1).unwrap();

        // Create the Worker.
        let worker = Worker::new(0, Arc::new(gateway), storage, ledger, Default::default()).unwrap();
        let transmission_id = TransmissionID::Solution(
            rng.random::<u64>().into(),
            rng.random::<<CurrentNetwork as Network>::TransmissionChecksum>(),
        );
        let worker_ = worker.clone();
        let peer_ip = SocketAddr::from(([127, 0, 0, 1], 1234));
        let _ = worker_.send_transmission_request(peer_ip, transmission_id).await;
        assert!(worker.pending.contains(transmission_id));
        let peer_ip = SocketAddr::from(([127, 0, 0, 1], 1234));
        // Fake the transmission response.
        worker.finish_transmission_request(peer_ip, TransmissionResponse {
            transmission_id,
            transmission: Transmission::Solution(Data::Buffer(Bytes::from(vec![0; 512]))),
        });
        // Check the transmission was removed from the pending set.
        assert!(!worker.pending.contains(transmission_id));
    }

    #[tokio::test]
    async fn test_process_solution_ok() {
        let rng = &mut TestRng::default();
        // Sample a committee.
        let committee = snarkvm::ledger::committee::test_helpers::sample_committee(rng);
        let committee_clone = committee.clone();
        // Setup the mock gateway and ledger.
        let mut gateway = MockGateway::default();
        gateway.expect_send().returning(|_, _| {
            let (_tx, rx) = oneshot::channel();
            Some(rx)
        });
        let mut mock_ledger = MockLedger::default();
        mock_ledger.expect_current_committee().returning(move || Ok(committee.clone()));
        mock_ledger.expect_get_committee_lookback_for_round().returning(move |_| Ok(committee_clone.clone()));
        mock_ledger.expect_contains_transmission().returning(|_| Ok(false));
        mock_ledger.expect_check_solution_basic().returning(|_, _| Ok(()));
        let ledger: Arc<dyn LedgerService<CurrentNetwork>> = Arc::new(mock_ledger);
        // Initialize the storage.
        let storage = Storage::<CurrentNetwork>::new(ledger.clone(), Arc::new(BFTMemoryService::new()), 1).unwrap();

        // Create the Worker.
        let worker = Worker::new(0, Arc::new(gateway), storage, ledger, Default::default()).unwrap();
        let solution = Data::Buffer(Bytes::from((0..512).map(|_| rng.random::<u8>()).collect::<Vec<_>>()));
        let solution_id = rng.random::<u64>().into();
        let solution_checksum = solution.to_checksum::<CurrentNetwork>().unwrap();
        let transmission_id = TransmissionID::Solution(solution_id, solution_checksum);
        let worker_ = worker.clone();
        let peer_ip = SocketAddr::from(([127, 0, 0, 1], 1234));
        let _ = worker_.send_transmission_request(peer_ip, transmission_id).await;
        assert!(worker.pending.contains(transmission_id));
        let result = worker.process_unconfirmed_solution(solution_id, solution).await;
        assert!(result.is_ok());
        assert!(!worker.pending.contains(transmission_id));
        assert!(worker.ready.read().contains(transmission_id));
    }

    #[tokio::test]
    async fn test_process_solution_nok() {
        let rng = &mut TestRng::default();
        // Sample a committee.
        let committee = snarkvm::ledger::committee::test_helpers::sample_committee(rng);
        let committee_clone = committee.clone();
        // Setup the mock gateway and ledger.
        let mut gateway = MockGateway::default();
        gateway.expect_send().returning(|_, _| {
            let (_tx, rx) = oneshot::channel();
            Some(rx)
        });
        let mut mock_ledger = MockLedger::default();
        mock_ledger.expect_current_committee().returning(move || Ok(committee.clone()));
        mock_ledger.expect_get_committee_lookback_for_round().returning(move |_| Ok(committee_clone.clone()));
        mock_ledger.expect_contains_transmission().returning(|_| Ok(false));
        mock_ledger.expect_check_solution_basic().returning(|_, _| Err(anyhow!("")));
        let ledger: Arc<dyn LedgerService<CurrentNetwork>> = Arc::new(mock_ledger);
        // Initialize the storage.
        let storage = Storage::<CurrentNetwork>::new(ledger.clone(), Arc::new(BFTMemoryService::new()), 1).unwrap();

        // Create the Worker.
        let worker = Worker::new(0, Arc::new(gateway), storage, ledger, Default::default()).unwrap();
        let solution_id = rng.random::<u64>().into();
        let solution = Data::Buffer(Bytes::from((0..512).map(|_| rng.random::<u8>()).collect::<Vec<_>>()));
        let checksum = solution.to_checksum::<CurrentNetwork>().unwrap();
        let transmission_id = TransmissionID::Solution(solution_id, checksum);
        let worker_ = worker.clone();
        let peer_ip = SocketAddr::from(([127, 0, 0, 1], 1234));
        let _ = worker_.send_transmission_request(peer_ip, transmission_id).await;
        assert!(worker.pending.contains(transmission_id));
        let result = worker.process_unconfirmed_solution(solution_id, solution).await;
        assert!(result.is_err());
        assert!(!worker.pending.contains(transmission_id));
        assert!(!worker.ready.read().contains(transmission_id));
    }

    #[tokio::test]
    async fn test_process_transaction_ok() {
        let rng = &mut TestRng::default();
        // Sample a committee.
        let committee = snarkvm::ledger::committee::test_helpers::sample_committee(rng);
        let committee_clone = committee.clone();
        // Setup the mock gateway and ledger.
        let mut gateway = MockGateway::default();
        gateway.expect_send().returning(|_, _| {
            let (_tx, rx) = oneshot::channel();
            Some(rx)
        });
        let mut mock_ledger = MockLedger::default();
        mock_ledger.expect_current_committee().returning(move || Ok(committee.clone()));
        mock_ledger.expect_get_committee_lookback_for_round().returning(move |_| Ok(committee_clone.clone()));
        mock_ledger.expect_contains_transmission().returning(|_| Ok(false));
        mock_ledger.expect_check_transaction_basic().returning(|_, _| Ok(()));
        let ledger: Arc<dyn LedgerService<CurrentNetwork>> = Arc::new(mock_ledger);
        // Initialize the storage.
        let storage = Storage::<CurrentNetwork>::new(ledger.clone(), Arc::new(BFTMemoryService::new()), 1).unwrap();

        // Create the Worker.
        let worker = Worker::new(0, Arc::new(gateway), storage, ledger, Default::default()).unwrap();
        let transaction = sample_execution_transaction_with_fee(false, rng, 0);
        let transaction_id = transaction.id();
        let transaction_data = Data::Object(transaction);
        let checksum = transaction_data.to_checksum::<CurrentNetwork>().unwrap();
        let transmission_id = TransmissionID::Transaction(transaction_id, checksum);
        let worker_ = worker.clone();
        let peer_ip = SocketAddr::from(([127, 0, 0, 1], 1234));
        let _ = worker_.send_transmission_request(peer_ip, transmission_id).await;
        assert!(worker.pending.contains(transmission_id));
        let result = worker.process_unconfirmed_transaction(transaction_id, transaction_data).await;
        assert!(result.is_ok());
        assert!(!worker.pending.contains(transmission_id));
        assert!(worker.ready.read().contains(transmission_id));
    }

    #[tokio::test]
    async fn test_process_transaction_nok() {
        let mut rng = &mut TestRng::default();
        // Sample a committee.
        let committee = snarkvm::ledger::committee::test_helpers::sample_committee(rng);
        let committee_clone = committee.clone();
        // Setup the mock gateway and ledger.
        let mut gateway = MockGateway::default();
        gateway.expect_send().returning(|_, _| {
            let (_tx, rx) = oneshot::channel();
            Some(rx)
        });
        let mut mock_ledger = MockLedger::default();
        mock_ledger.expect_current_committee().returning(move || Ok(committee.clone()));
        mock_ledger.expect_get_committee_lookback_for_round().returning(move |_| Ok(committee_clone.clone()));
        mock_ledger.expect_contains_transmission().returning(|_| Ok(false));
        mock_ledger.expect_check_transaction_basic().returning(|_, _| Err(anyhow!("")));
        let ledger: Arc<dyn LedgerService<CurrentNetwork>> = Arc::new(mock_ledger);
        // Initialize the storage.
        let storage = Storage::<CurrentNetwork>::new(ledger.clone(), Arc::new(BFTMemoryService::new()), 1).unwrap();

        // Create the Worker.
        let worker = Worker::new(0, Arc::new(gateway), storage, ledger, Default::default()).unwrap();
        let transaction_id: <CurrentNetwork as Network>::TransactionID = Field::<CurrentNetwork>::rand(&mut rng).into();
        let transaction = Data::Buffer(Bytes::from((0..512).map(|_| rng.random::<u8>()).collect::<Vec<_>>()));
        let checksum = transaction.to_checksum::<CurrentNetwork>().unwrap();
        let transmission_id = TransmissionID::Transaction(transaction_id, checksum);
        let worker_ = worker.clone();
        let peer_ip = SocketAddr::from(([127, 0, 0, 1], 1234));
        let _ = worker_.send_transmission_request(peer_ip, transmission_id).await;
        assert!(worker.pending.contains(transmission_id));
        let result = worker.process_unconfirmed_transaction(transaction_id, transaction).await;
        assert!(result.is_err());
        assert!(!worker.pending.contains(transmission_id));
        assert!(!worker.ready.read().contains(transmission_id));
    }

    #[tokio::test]
    async fn test_flood_transmission_requests() {
        let rng = &mut TestRng::default();
        // Sample a committee.
        let committee = snarkvm::ledger::committee::test_helpers::sample_committee(rng);
        let committee_clone = committee.clone();
        // Setup the mock gateway and ledger.
        let mut gateway = MockGateway::default();
        gateway.expect_send().returning(|_, _| {
            let (_tx, rx) = oneshot::channel();
            Some(rx)
        });
        let mut mock_ledger = MockLedger::default();
        mock_ledger.expect_current_committee().returning(move || Ok(committee.clone()));
        mock_ledger.expect_get_committee_lookback_for_round().returning(move |_| Ok(committee_clone.clone()));
        mock_ledger.expect_contains_transmission().returning(|_| Ok(false));
        mock_ledger.expect_check_transaction_basic().returning(|_, _| Ok(()));
        let ledger: Arc<dyn LedgerService<CurrentNetwork>> = Arc::new(mock_ledger);
        // Initialize the storage.
        let storage = Storage::<CurrentNetwork>::new(ledger.clone(), Arc::new(BFTMemoryService::new()), 1).unwrap();

        // Create the Worker.
        let worker = Worker::new(0, Arc::new(gateway), storage, ledger, Default::default()).unwrap();
        let transaction = sample_execution_transaction_with_fee(false, rng, 0);
        let transaction_id = transaction.id();
        let transaction_data = Data::Object(transaction);
        let checksum = transaction_data.to_checksum::<CurrentNetwork>().unwrap();
        let transmission_id = TransmissionID::Transaction(transaction_id, checksum);

        // Determine the number of redundant requests are sent.
        let num_redundant_requests =
            max_redundant_requests(worker.ledger.clone(), worker.storage.current_round()).unwrap();
        let num_flood_requests = num_redundant_requests * 10;
        let mut peer_ips =
            (0..num_flood_requests).map(|i| SocketAddr::from(([127, 0, 0, 1], 1234 + i as u16))).collect_vec();
        let first_peer_ip = peer_ips[0];

        // Flood the pending queue with transmission requests.
        for i in 1..=num_flood_requests {
            let worker_ = worker.clone();
            let peer_ip = peer_ips.pop().unwrap();
            tokio::spawn(async move {
                let _ = worker_.send_transmission_request(peer_ip, transmission_id).await;
            });
            tokio::time::sleep(Duration::from_millis(10)).await;
            // Check that the number of sent requests does not exceed the maximum number of redundant requests.
            assert!(worker.pending.num_sent_requests(transmission_id) <= num_redundant_requests);
            assert_eq!(worker.pending.num_callbacks(transmission_id), i);
        }
        // Check that the number of sent requests does not exceed the maximum number of redundant requests.
        assert_eq!(worker.pending.num_sent_requests(transmission_id), num_redundant_requests);
        assert_eq!(worker.pending.num_callbacks(transmission_id), num_flood_requests);

        // Let all the requests expire.
        tokio::time::sleep(Duration::from_secs(CALLBACK_EXPIRATION_IN_SECS as u64 + 1)).await;
        assert_eq!(worker.pending.num_sent_requests(transmission_id), 0);
        assert_eq!(worker.pending.num_callbacks(transmission_id), 0);

        // Flood the pending queue with transmission requests again, this time to a single peer
        for i in 1..=num_flood_requests {
            let worker_ = worker.clone();
            tokio::spawn(async move {
                let _ = worker_.send_transmission_request(first_peer_ip, transmission_id).await;
            });
            tokio::time::sleep(Duration::from_millis(10)).await;
            assert!(worker.pending.num_sent_requests(transmission_id) <= num_redundant_requests);
            assert_eq!(worker.pending.num_callbacks(transmission_id), i);
        }
        // Check that the number of sent requests does not exceed the maximum number of redundant requests.
        assert_eq!(worker.pending.num_sent_requests(transmission_id), 1);
        assert_eq!(worker.pending.num_callbacks(transmission_id), num_flood_requests);

        // Check that fulfilling a transmission request clears the pending queue.
        let result = worker.process_unconfirmed_transaction(transaction_id, transaction_data).await;
        assert!(result.is_ok());
        assert_eq!(worker.pending.num_sent_requests(transmission_id), 0);
        assert_eq!(worker.pending.num_callbacks(transmission_id), 0);
        assert!(!worker.pending.contains(transmission_id));
        assert!(worker.ready.read().contains(transmission_id));
    }

    #[tokio::test]
    async fn test_storage_gc_on_initialization() {
        let rng = &mut TestRng::default();

        for _ in 0..ITERATIONS {
            // Mock the ledger round.
            let max_gc_rounds = rng.random_range(50..=100);
            let latest_ledger_round = rng.random_range((max_gc_rounds + 1)..1000);
            let expected_gc_round = latest_ledger_round - max_gc_rounds;

            // Sample a committee.
            let committee =
                snarkvm::ledger::committee::test_helpers::sample_committee_for_round(latest_ledger_round, rng);

            // Setup the mock gateway and ledger.
            let mut mock_ledger = MockLedger::default();
            mock_ledger.expect_current_committee().returning(move || Ok(committee.clone()));

            let ledger: Arc<dyn LedgerService<CurrentNetwork>> = Arc::new(mock_ledger);
            // Initialize the storage.
            let storage =
                Storage::<CurrentNetwork>::new(ledger.clone(), Arc::new(BFTMemoryService::new()), max_gc_rounds)
                    .unwrap();

            // Ensure that the storage GC round is correct.
            assert_eq!(storage.gc_round(), expected_gc_round);
        }
    }
}

#[cfg(test)]
mod prop_tests {
    use super::*;
    use crate::Gateway;
    use snarkos_node_bft_ledger_service::MockLedgerService;
    use snarkvm::{
        console::account::Address,
        ledger::committee::{Committee, MIN_VALIDATOR_STAKE},
    };

    use rand::RngExt;
    use test_strategy::proptest;

    type CurrentNetwork = snarkvm::prelude::MainnetV0;

    // Initializes a new test committee.
    fn new_test_committee(n: u16) -> Committee<CurrentNetwork> {
        let mut members = IndexMap::with_capacity(n as usize);
        for i in 0..n {
            // Sample the address.
            let rng = &mut TestRng::fixed(i as u64);
            let address = Address::new(rng.random());
            info!("Validator {i}: {address}");
            members.insert(address, (MIN_VALIDATOR_STAKE, false, rng.random_range(0..100)));
        }
        // Initialize the committee.
        Committee::<CurrentNetwork>::new(1u64, members).unwrap()
    }

    #[proptest]
    fn worker_initialization(
        #[strategy(0..MAX_WORKERS)] id: u8,
        gateway: Gateway<CurrentNetwork>,
        storage: Storage<CurrentNetwork>,
    ) {
        let committee = new_test_committee(4);
        let ledger: Arc<dyn LedgerService<CurrentNetwork>> = Arc::new(MockLedgerService::new(committee));
        let worker = Worker::new(id, Arc::new(gateway), storage, ledger, Default::default()).unwrap();
        assert_eq!(worker.id(), id);
    }

    #[proptest]
    fn invalid_worker_id(
        #[strategy(MAX_WORKERS..)] id: u8,
        gateway: Gateway<CurrentNetwork>,
        storage: Storage<CurrentNetwork>,
    ) {
        let committee = new_test_committee(4);
        let ledger: Arc<dyn LedgerService<CurrentNetwork>> = Arc::new(MockLedgerService::new(committee));
        let worker = Worker::new(id, Arc::new(gateway), storage, ledger, Default::default());
        // TODO once Worker implements Debug, simplify this with `unwrap_err`
        if let Err(error) = worker {
            assert_eq!(error.to_string(), format!("Invalid worker ID '{id}'"));
        }
    }
}
