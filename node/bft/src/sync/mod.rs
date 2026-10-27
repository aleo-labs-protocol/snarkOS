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
    Gateway,
    MAX_FETCH_TIMEOUT,
    Transport,
    events::{CertificateRequest, CertificateResponse, Event},
    helpers::{Pending, Storage, SyncReceiver, fmt_id, max_redundant_requests},
    ledger_service::{BeginLedgerUpdateError, LedgerService},
    spawn_blocking,
};

use snarkos_node_sync::{BftSyncMode, BlockSync, InsertBlockResponseError, Ping, locators::BlockLocators};
use snarkos_utilities::CallbackHandle;

use snarkvm::{
    console::{
        network::{ConsensusVersion, Network},
        types::Field,
    },
    ledger::{CheckBlockError, PendingBlock, authority::Authority, block::Block, narwhal::BatchCertificate},
    utilities::{cfg_into_iter, cfg_iter, ensure_equals, flatten_error},
};

use anyhow::{Context, Result, anyhow, bail, ensure};
#[cfg(feature = "locktick")]
use locktick::{parking_lot::Mutex, tokio::Mutex as TMutex};
#[cfg(not(feature = "locktick"))]
use parking_lot::Mutex;
#[cfg(not(feature = "serial"))]
use rayon::prelude::*;
use std::{
    collections::{HashMap, HashSet, VecDeque},
    future::Future,
    net::SocketAddr,
    ops::Deref,
    sync::Arc,
    time::Duration,
};
#[cfg(not(feature = "locktick"))]
use tokio::sync::Mutex as TMutex;
use tokio::{sync::oneshot, task::JoinHandle};

/// This callback trait allows listening to synchronization updates, such as discorvering new `BatchCertificate`s.
/// This is currently used by BFT.
#[async_trait::async_trait]
pub trait SyncCallback<N: Network>: Send + std::marker::Sync {
    // Adds a new certificate to the DAG.
    fn add_certificate_from_sync(&self, certificate: BatchCertificate<N>);

    // Commits a certificate into the DAG.
    fn commit_certificate_from_sync(&self, certificate: &BatchCertificate<N>);
}

/// Block synchronization logic for validators.
///
/// Synchronization works differently for nodes that act as validators in AleoBFT;
/// In the common case, validators generate blocks after receiving an anchor block that has been accepted
/// by a supermajority of the committee instead of fetching entire blocks from other nodes.
/// However, if a validator does not have an up-to-date DAG, it might still fetch entire blocks from other nodes.
///
/// This struct also manages fetching certificates from other validators during normal operation,
/// and blocks when falling behind.
///
/// Finally, `Sync` handles synchronization of blocks with the validator's local storage:
/// it loads blocks from the storage on startup and writes new blocks to the storage after discovering them.
#[derive(Clone)]
pub struct Sync<N: Network> {
    /// The gateway enables communication with other validators.
    gateway: Gateway<N>,
    /// The storage.
    storage: Storage<N>,
    /// The ledger service.
    ledger: Arc<dyn LedgerService<N>>,
    /// The block synchronization logic.
    block_sync: Arc<BlockSync<N>>,
    /// The pending certificates queue.
    pending: Arc<Pending<Field<N>, BatchCertificate<N>>>,
    /// The sync callback (used by [`BFT`]).
    sync_callback: Arc<CallbackHandle<Arc<dyn SyncCallback<N>>>>,
    /// Handles to the spawned background tasks.
    handles: Arc<Mutex<Vec<JoinHandle<()>>>>,
    /// The response lock.
    response_lock: Arc<TMutex<()>>,

    /// The latest block responses.
    ///
    /// This is used in [`Sync::sync_storage_with_block()`] to accumulate blocks
    /// whose addition to the ledger is deferred until certain checks pass.
    /// Blocks need to be processed in order, hence a BTree map.
    ///
    /// Whenever a new block is added to this map, BlockSync::set_sync_height needs to be called.
    pending_blocks: Arc<Mutex<VecDeque<PendingBlock<N>>>>,
}

impl<N: Network> Sync<N> {
    /// The maximum time to wait for peer updates before timing out and attempting to issue new requests.
    /// This only exists as a fallback for the (unlikely) case a task does not get notified about updates.
    const MAX_SYNC_INTERVAL: Duration = Duration::from_secs(30);

    /// Initializes a new sync instance.
    pub fn new(
        gateway: Gateway<N>,
        storage: Storage<N>,
        ledger: Arc<dyn LedgerService<N>>,
        block_sync: Arc<BlockSync<N>>,
    ) -> Self {
        // Validators start in fast-sync mode until they confirm they are within the GC range.
        block_sync.set_bft_sync_mode(BftSyncMode::Fast);

        // Return the sync instance.
        Self {
            gateway,
            storage,
            ledger,
            block_sync,
            pending: Default::default(),
            sync_callback: Default::default(),
            handles: Default::default(),
            response_lock: Default::default(),
            pending_blocks: Default::default(),
        }
    }

    /// Initializes the sync module and sync the storage with the ledger at bootup.
    pub fn initialize(&self, sync_callback: Option<Arc<dyn SyncCallback<N>>>) -> Result<()> {
        // If a callback was provided, set it.
        if let Some(callback) = sync_callback {
            self.sync_callback.set(callback).with_context(|| "Failed to set sync callback")?;
        }

        info!("Syncing storage with the ledger...");

        // Sync the storage with the ledger.
        self.sync_storage_with_ledger_at_bootup()
            .with_context(|| "Syncing storage with the ledger at bootup failed")?;

        debug!("Finished initial block synchronization at startup");
        Ok(())
    }

    /// Starts the sync module.
    ///
    /// When this function returns successfully, the sync module will have spawned background tasks
    /// that fetch blocks from other validators.
    pub async fn run(&self, ping: Option<Arc<Ping<N>>>, sync_receiver: SyncReceiver<N>) -> Result<()> {
        info!("Starting the sync module...");

        // Start the block request generation loop (outgoing).
        let self_ = self.clone();
        self.spawn(async move {
            loop {
                // Wait for peer updates or timeout
                let _ = tokio::time::timeout(Self::MAX_SYNC_INTERVAL, self_.block_sync.wait_for_peer_update()).await;

                // Issue block requests to peers.
                self_.try_issuing_block_requests().await;

                // Rate limiting happens in [`BlockSync::try_issuing_block_requests`] and no additional sleeps are needed here.
            }
        });

        // Start the block response processing loop (incoming).
        let self_ = self.clone();
        let ping = ping.clone();
        self.spawn(async move {
            loop {
                // Wait until there is something to do or until the timeout.
                let _ =
                    tokio::time::timeout(Self::MAX_SYNC_INTERVAL, self_.block_sync.wait_for_block_responses()).await;

                let ping = ping.clone();
                let self_ = self_.clone();
                let hdl = tokio::spawn(async move {
                    self_.try_advancing_block_synchronization(&ping).await;
                });

                if let Err(err) = hdl.await
                    && let Ok(panic) = err.try_into_panic()
                {
                    error!("Sync block advancement panicked: {panic:?}");
                }

                // We perform no additional rate limiting here as
                // requests are already rate-limited.
            }
        });

        // Start the pending queue expiration loop.
        let self_ = self.clone();
        self.spawn(async move {
            loop {
                // Sleep briefly.
                tokio::time::sleep(MAX_FETCH_TIMEOUT).await;

                // Remove the expired pending transmission requests.
                let self__ = self_.clone();
                let _ = spawn_blocking!({
                    self__.pending.clear_expired_callbacks();
                    Ok(())
                });
            }
        });

        /* Set up callbacks for events from the Gateway */

        // Retrieve the sync receiver.
        let SyncReceiver {
            mut rx_block_sync_insert_block_response,
            mut rx_block_sync_remove_peer,
            mut rx_block_sync_update_peer_locators,
            mut rx_certificate_request,
            mut rx_certificate_response,
        } = sync_receiver;

        // Process the block sync request to advance with sync blocks.
        // Each iteration of this loop is triggered by an incoming [`BlockResponse`],
        // which is initially handled by [`Gateway::inbound()`],
        // which calls [`SyncSender::advance_with_sync_blocks()`],
        // which calls [`tx_block_sync_advance_with_sync_blocks.send()`],
        // which causes the `rx_block_sync_advance_with_sync_blocks.recv()` call below to return.
        let self_ = self.clone();
        self.spawn(async move {
            while let Some((peer_ip, blocks, latest_consensus_version, callback)) =
                rx_block_sync_insert_block_response.recv().await
            {
                let result = self_.insert_block_response(peer_ip, blocks, latest_consensus_version).await;

                //TODO remove this once channels are gone
                if let Err(err) = &result {
                    if err.is_benign() {
                        trace!("Failed to insert block response from '{peer_ip}' - {err}");
                    } else {
                        warn!("Failed to insert block response from '{peer_ip}' - {err}");
                    }
                }

                callback.send(result).ok();
            }
        });

        // Process the block sync request to remove the peer.
        let self_ = self.clone();
        self.spawn(async move {
            while let Some((peer_ip, tx)) = rx_block_sync_remove_peer.recv().await {
                self_.remove_peer(peer_ip);
                tx.send(()).ok();
            }
        });

        // Process each block sync request to update peer locators.
        // Each iteration of this loop is triggered by an incoming [`PrimaryPing`],
        // which is initially handled by [`Gateway::inbound()`],
        // which calls [`SyncSender::update_peer_locators()`],
        // which calls [`tx_block_sync_update_peer_locators.send()`],
        // which causes the `rx_block_sync_update_peer_locators.recv()` call below to return.
        let self_ = self.clone();
        self.spawn(async move {
            while let Some((peer_ip, locators, callback)) = rx_block_sync_update_peer_locators.recv().await {
                let self_clone = self_.clone();
                tokio::spawn(async move {
                    callback.send(self_clone.update_peer_locators(peer_ip, locators)).ok();
                });
            }
        });

        // Process each certificate request.
        // Each iteration of this loop is triggered by an incoming [`CertificateRequest`],
        // which is initially handled by [`Gateway::inbound()`],
        // which calls [`tx_certificate_request.send()`],
        // which causes the `rx_certificate_request.recv()` call below to return.
        let self_ = self.clone();
        self.spawn(async move {
            while let Some((peer_ip, certificate_request)) = rx_certificate_request.recv().await {
                self_.send_certificate_response(peer_ip, certificate_request);
            }
        });

        // Process each certificate response.
        // Each iteration of this loop is triggered by an incoming [`CertificateResponse`],
        // which is initially handled by [`Gateway::inbound()`],
        // which calls [`tx_certificate_response.send()`],
        // which causes the `rx_certificate_response.recv()` call below to return.
        let self_ = self.clone();
        self.spawn(async move {
            while let Some((peer_ip, certificate_response)) = rx_certificate_response.recv().await {
                self_.finish_certificate_request(peer_ip, certificate_response);
            }
        });

        Ok(())
    }

    /// BFT-specific version of `Client::try_issuing_block_requests()`.
    ///
    /// This method handles timeout removal, checks if block sync is possible,
    /// and issues block requests to peers.
    async fn try_issuing_block_requests(&self) {
        self.block_sync.try_issuing_block_requests(&self.gateway).await;
    }
}

// Callbacks used when receiving messages from the Gateway
impl<N: Network> Sync<N> {
    /// We received a block response and can (possibly) advance synchronization.
    async fn insert_block_response(
        &self,
        peer_ip: SocketAddr,
        blocks: Vec<Block<N>>,
        latest_consensus_version: Option<ConsensusVersion>,
    ) -> Result<(), InsertBlockResponseError<N>> {
        self.block_sync.insert_block_responses(peer_ip, blocks, latest_consensus_version)

        // No need to advance block sync here, as the new response will
        // notify the incoming task.
    }

    /// We received new peer locators during a Ping.
    fn update_peer_locators(&self, peer_ip: SocketAddr, locators: BlockLocators<N>) -> Result<()> {
        self.block_sync.update_peer_locators(peer_ip, &locators)
    }

    /// A peer disconnected.
    fn remove_peer(&self, peer_ip: SocketAddr) {
        self.block_sync.remove_peer(&peer_ip)
    }
}

// Methods to manage storage.
impl<N: Network> Sync<N> {
    /// Syncs the storage with the ledger at bootup.
    ///
    /// This is called when starting the validator and after finishing a sync without BFT.
    fn sync_storage_with_ledger_at_bootup(&self) -> Result<()> {
        let mut pending_blocks = self.pending_blocks.lock();
        let latest_ledger_block = self.ledger.latest_block();

        // Remove any obsolete pending blocks.
        while let Some(block) = pending_blocks.front()
            && block.height() <= latest_ledger_block.height()
        {
            pending_blocks.pop_front();
        }

        let latest_block: &Block<N> = pending_blocks.back().map(|block| block.deref()).unwrap_or(&latest_ledger_block);
        let max_height = latest_block.height();

        // Determine the maximum number of blocks corresponding to rounds
        // that would not have been garbage collected, i.e. that would be kept in storage.
        // Since at most one block is created every two rounds,
        // this is half of the maximum number of rounds kept in storage.
        let max_gc_blocks = u32::try_from(self.storage.max_gc_rounds())?.saturating_div(2);

        // Determine the earliest height of blocks corresponding to rounds kept in storage,
        // conservatively set to the block height minus the maximum number of blocks calculated above.
        // By virtue of the BFT protocol, we can guarantee that all GC range blocks will be loaded.
        let gc_height = max_height.saturating_sub(max_gc_blocks);

        // Retrieve the DAGs of all recent blocks..
        let ledger_blocks = self.ledger.get_blocks(gc_height..(latest_ledger_block.height() + 1))?;

        let blocks = ledger_blocks.iter().chain(pending_blocks.iter().map(|block| block.deref()));
        debug!("Syncing storage with ledger and pending blocks from height {gc_height} to {max_height}...");

        /* Sync storage */

        // Sync the height with the block.
        self.storage.sync_height_with_block(latest_block.height());
        // Sync the round with the block.
        self.storage.sync_round_with_block(latest_block.round());
        // Perform GC on the latest block round.
        self.storage
            .garbage_collect_certificates(latest_block.round())
            .with_context(|| "Failed to garbage collect certificates")?;

        // Add the blocks to the BFT storage.
        for block in blocks {
            if let Authority::Quorum(subdag) = block.authority() {
                // If the block authority is a sub-DAG, then sync the batch certificates with the block.
                // Note that the block authority is always a sub-DAG in production;
                // beacon signatures are only used for testing,
                // and as placeholder (irrelevant) block authority in the genesis block.
                // Reconstruct the unconfirmed transactions.
                let unconfirmed_transactions = cfg_iter!(block.transactions())
                    .filter_map(|tx| {
                        tx.to_unconfirmed_transaction().map(|unconfirmed| (unconfirmed.id(), unconfirmed)).ok()
                    })
                    .collect::<HashMap<_, _>>();

                // Iterate over the certificates.
                for certificates in subdag.values().cloned() {
                    cfg_into_iter!(certificates).try_for_each(|certificate| {
                        // The block was already verified when it was added to the
                        // ledger, so we do not have to re-check its certificates here.
                        let trusted_ledger_certificate = true;
                        self.storage
                            .sync_certificate_with_block(
                                block,
                                certificate,
                                &unconfirmed_transactions,
                                trusted_ledger_certificate,
                            )
                            .with_context(|| format!("Failed to sync certificate with block {}", block.height()))
                    })?;
                }

                // Update the validator telemetry.
                #[cfg(feature = "metrics")]
                self.gateway.validator_telemetry().insert_subdag(subdag);
            }
        }

        // Add all certificates to the BFT DAG, and update the committed round.
        if let Some(cb) = self.sync_callback.get() {
            for block in ledger_blocks.into_iter() {
                if let Authority::Quorum(subdag) = block.authority() {
                    for round in subdag.values() {
                        for cert in round {
                            cb.add_certificate_from_sync(cert.clone());
                            cb.commit_certificate_from_sync(cert);
                        }
                    }
                }
            }

            // Pending blocks have not been committed yet.
            for block in pending_blocks.iter() {
                if let Authority::Quorum(subdag) = block.authority() {
                    for round in subdag.values() {
                        for cert in round {
                            cb.add_certificate_from_sync(cert.clone());
                        }
                    }
                }
            }
        }

        self.block_sync.set_sync_height(max_height);

        Ok(())
    }

    /// Returns which height we are synchronized to.
    /// If there are queued block responses, this might be higher than the latest block in the ledger.
    fn compute_sync_height(&self) -> u32 {
        let ledger_height = self.ledger.latest_block_height();
        let mut pending_blocks = self.pending_blocks.lock();

        // Remove any old responses.
        while let Some(b) = pending_blocks.front()
            && b.height() <= ledger_height
        {
            pending_blocks.pop_front();
        }

        // Ensure the returned value is always greater or equal than ledger height.
        pending_blocks.back().map(|b| b.height()).unwrap_or(0).max(ledger_height)
    }

    /// BFT-version of [`snarkos_node_client::Client::try_advancing_block_synchronization`].
    async fn try_advancing_block_synchronization(&self, ping: &Option<Arc<Ping<N>>>) {
        // Process block responses and advance the ledger.
        let new_blocks = match self
            .try_advancing_block_synchronization_inner()
            .await
            .with_context(|| "Block synchronization failed")
        {
            Ok(new_blocks) => new_blocks,
            Err(err) => {
                error!("{}", &flatten_error(err));
                false
            }
        };

        if let Some(ping) = &ping
            && new_blocks
        {
            match self.get_block_locators() {
                Ok(locators) => ping.update_block_locators(locators),
                Err(err) => error!("Failed to update block locators: {err}"),
            }
        }
    }

    /// Aims to advance synchronization using any recent block responses received from peers.
    ///
    /// This is the validator's version of `BlockSync::try_advancing_block_synchronization`
    /// and is called periodically at runtime.
    ///
    /// This returns Ok(true) if we successfully advanced the ledger by at least one new block.
    ///
    /// A key difference to `BlockSync`'s versions is that it will only add blocks to the ledger once they have been confirmed by the network.
    /// If blocks are not confirmed yet, they will be kept in [`Self::pending_blocks`].
    /// It will also pass certificates from synced blocks to the BFT module so that consensus can progress as expected
    /// (see [`Self::sync_storage_with_block`] for more details).
    async fn try_advancing_block_synchronization_inner(&self) -> Result<bool> {
        // Acquire the response lock.
        let _lock = self.response_lock.lock().await;

        // For sanity, set the sync height again.
        // (if the sync height is already larger or equal, this is a noop)
        let ledger_height = self.ledger.latest_block_height();
        self.block_sync.set_sync_height(ledger_height);

        // Retrieve the maximum block height of the peers.
        let tip = self
            .block_sync
            .find_sync_peers()
            .map(|(sync_peers, _)| *sync_peers.values().max().unwrap_or(&0))
            .unwrap_or(0);

        // Determine the maximum number of blocks corresponding to rounds
        // that would not have been garbage collected, i.e. that would be kept in storage.
        // Since at most one block is created every two rounds,
        // this is half of the maximum number of rounds kept in storage.
        let max_gc_blocks = u32::try_from(self.storage.max_gc_rounds())?.saturating_div(2);

        // Updates sync state and returns the error (if any).
        let cleanup = |start_height, current_height, error| {
            let new_blocks = current_height > start_height;

            // Make the underlying `BlockSync` instance aware of the new sync height.
            if new_blocks {
                self.block_sync.set_sync_height(current_height);
            }

            if let Some(err) = error { Err(err) } else { Ok(new_blocks) }
        };

        // Determine the earliest height of blocks corresponding to rounds kept in storage,
        // conservatively set to the block height minus the maximum number of blocks calculated above.
        // By virtue of the BFT protocol, we can guarantee that all GC range blocks will be loaded.
        let max_gc_height = tip.saturating_sub(max_gc_blocks);

        // Retrieve the current height, based on the ledger height and the
        // (unconfirmed) blocks that are already queued up.
        let start_height = self.compute_sync_height();

        // A node that has entered fast-sync must complete the transition via
        // `sync_storage_with_ledger_at_bootup` before it is allowed to use the BFT/DAG path.
        // Without this guard a drop in the reported peer tip could shrink `max_gc_height` and
        // make the outer `within_gc` check flip to `true` prematurely, bypassing the bootup routine.
        let within_gc = start_height >= max_gc_height;

        if within_gc {
            // For the (unlikely) case that network tip decreased, check here as well if sync mode has switched.
            let previous = self.block_sync.set_bft_sync_mode(BftSyncMode::Dag);
            let was_in_fast_sync = previous == Some(BftSyncMode::Fast);

            if was_in_fast_sync {
                debug!("Finished catching up with the network. Switching to DAG sync.");
                self.sync_storage_with_ledger_at_bootup()?;
            }

            // The height is incremented as blocks are added.
            let mut current_height = start_height;
            trace!(
                "Try advancing blocks responses with DAG updates (starting at block {next_height}, current sync speed is {speed})",
                next_height = current_height + 1,
                speed = self.block_sync.get_sync_speed(),
            );

            // If we already were within GC or successfully caught up with GC, try to advance BFT normally again.
            loop {
                let next_height = current_height + 1;
                let Some(block) = self.block_sync.peek_next_block(next_height) else {
                    break;
                };
                info!("Trying to sync next block at height {} with the BFT...", block.height());
                // Sync the storage with the block.
                match self.sync_storage_with_block(block, true).await {
                    Ok(_) => {
                        // Update the current height if sync succeeds.
                        current_height = next_height;
                    }
                    Err(err) => {
                        // Mark the current height as processed in block_sync.
                        self.block_sync.remove_block_response(next_height);
                        return cleanup(start_height, current_height, Some(err));
                    }
                }
            }

            cleanup(start_height, current_height, None)
        } else {
            let previous = self.block_sync.set_bft_sync_mode(BftSyncMode::Fast);
            let was_in_dag_sync = previous == Some(BftSyncMode::Dag);
            if was_in_dag_sync {
                // Peers may have advanced faster than this node is syncing, so it is reverting back to fast sync.
                warn!(
                    "Node is switching from DAG sync back to fast sync. The network tip may have advanced faster than this node is syncing."
                );
            }

            // For fast sync, blocks still go through `pending_blocks` and the availability threshold check,
            // but certificates are *not* inserted into the BFT DAG (see `sync_storage_with_block` with `within_gc_range = false`).
            let mut current_height = start_height;

            trace!(
                "Try advancing block responses without updating the DAG (starting at block {next_height})",
                next_height = current_height + 1
            );

            // Try to advance the ledger *to tip* without updating the BFT,
            // The BFT will only be updated if we reached the GC range after adding the new blocks.
            loop {
                let next_height = current_height + 1;

                let Some(block) = self.block_sync.peek_next_block(next_height) else {
                    break;
                };
                info!("Syncing the ledger to block {}...", block.height());

                // Sync the ledger with the block without BFT.
                match self.sync_storage_with_block(block, false).await {
                    Ok(_) => {
                        // Update the current height if sync succeeds.
                        current_height = next_height;
                        self.block_sync.count_request_completed();
                    }
                    Err(err) => {
                        // Mark the current height as processed in block_sync.
                        self.block_sync.remove_block_response(next_height);
                        return cleanup(start_height, current_height, Some(err));
                    }
                }
            }

            // Sync the storage with the ledger if we should transition to the BFT sync.
            let within_gc = current_height >= max_gc_height;
            if within_gc {
                info!("Finished catching up with the network. Switching back to DAG sync.");
                self.block_sync.set_bft_sync_mode(BftSyncMode::Dag);
                self.sync_storage_with_ledger_at_bootup().with_context(|| "BFT sync (with bootup routine) failed")?;
            }

            cleanup(start_height, current_height, None)
        }
    }

    /// Helper function for [`Self::sync_storage_with_block`].
    /// It syncs the batch certificates with the BFT, if the block's authority is a sub-DAG.
    ///
    /// Note that the block authority is always a sub-DAG in production; beacon signatures are only used for testing,
    /// and as placeholder (irrelevant) block authority in the genesis block.
    fn add_block_subdag_to_bft(&self, block: &Block<N>) -> Result<()> {
        // Nothing to do if this is a beacon block
        let Authority::Quorum(subdag) = block.authority() else {
            return Ok(());
        };

        // Reconstruct the unconfirmed transactions.
        let unconfirmed_transactions = cfg_iter!(block.transactions())
            .filter_map(|tx| tx.to_unconfirmed_transaction().map(|unconfirmed| (unconfirmed.id(), unconfirmed)).ok())
            .collect::<HashMap<_, _>>();

        // Iterate over the certificates.
        for certificates in subdag.values() {
            cfg_into_iter!(certificates.clone()).try_for_each(|certificate| -> Result<()> {
                // Sync the batch certificate with the block.
                // Make sure to perform full verification of the certificate here.
                let trusted_ledger_certificate = false;
                self.storage
                    .sync_certificate_with_block(
                        block,
                        certificate.clone(),
                        &unconfirmed_transactions,
                        trusted_ledger_certificate,
                    )
                    .with_context(|| format!("Failed to sync certificate with block {}", block.height()))
            })?;
        }

        // Sync the BFT DAG with the block's certificates.
        if let Some(cb) = self.sync_callback.get() {
            for round in subdag.values() {
                for certificate in round {
                    cb.add_certificate_from_sync(certificate.clone());
                }
            }
        }

        Ok(())
    }

    /// Helper function for [`Self::sync_storage_with_block`].
    ///
    /// It checks that successor of a given block contains enough votes to commit it.
    /// This can only return `Ok(true)` if the certificates of the block's successor were added to the storage.
    fn is_block_availability_threshold_reached(
        &self,
        block: &PendingBlock<N>,
        successors: &[PendingBlock<N>],
    ) -> Result<bool> {
        // Fetch the leader certificate and the relevant rounds.
        let leader_certificate = match block.authority() {
            Authority::Quorum(subdag) => subdag.leader_certificate().clone(),
            _ => bail!("Received a block with an unexpected authority type."),
        };
        let commit_round = leader_certificate.round();
        let certificate_round =
            commit_round.checked_add(1).ok_or_else(|| anyhow!("Integer overflow on round number"))?;

        // Get the committee lookback for the round just after the leader.
        let certificate_committee_lookback = self.ledger.get_committee_lookback_for_round(certificate_round)?;

        // Construct a set over the authors, at the round just after the leader,
        // who included the leader's certificate in their previous certificate IDs.
        let authors = successors
            .iter()
            .filter_map(|successor| {
                let Authority::Quorum(subdag) = successor.authority() else {
                    return None;
                };

                subdag.get(&certificate_round)
            })
            .flatten()
            .filter_map(|certificate| {
                if certificate.previous_certificate_ids().contains(&leader_certificate.id()) {
                    Some(certificate.author())
                } else {
                    None
                }
            })
            .collect::<HashSet<_>>();

        // Check if the leader is ready to be committed.
        if certificate_committee_lookback.is_availability_threshold_reached(&authors) {
            trace!(
                "Block {hash} at height {height} has reached availability threshold",
                hash = block.hash(),
                height = block.height()
            );
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Advances the ledger by the given block and updates the storage accordingly.
    ///
    /// This also updates the DAG, and uses the DAG to ensure that the block's leader certificate
    /// meets the voter availability threshold (i.e. > f voting stake)
    /// or is reachable via a DAG path from a later leader certificate that does.
    /// Since performing this check requires DAG certificates from later blocks,
    /// the block is stored in `Sync::pending_blocks`,
    /// and its addition to the ledger is deferred until the check passes.
    /// Several blocks may be stored in `Sync::pending_blocks`
    /// before they can be all checked and added to the ledger.
    ///
    /// # Usage
    /// This function assumes that blocks are passed in order, i.e.,
    /// that the given block is a direct successor of the block that was last passed to this function.
    async fn sync_storage_with_block(&self, new_block: Block<N>, within_gc_range: bool) -> Result<()> {
        let new_block_height = new_block.height();

        // If this block has already been processed, return early.
        // TODO(kaimast): Should we remove the response here?
        if self.ledger.contains_block_height(new_block.height()) {
            debug!("Ledger is already synced with block at height {new_block_height}. Will not sync.",);
            return Ok(());
        }

        // Append the certificates to the storage, if the block is within the GC range.
        if within_gc_range {
            self.add_block_subdag_to_bft(&new_block)?;
        }

        // This optimistically performs updates to the pending block set.
        let _self = self.clone();

        spawn_blocking!({
            while !_self.try_sync_storage_with_block(&new_block, within_gc_range)? {
                trace!("Retrying to sync storage with block at height {new_block_height}");
            }

            Ok(())
        })
    }

    /// Tries to sync the storage with the given block.
    ///
    /// # Arguments
    /// - `new_block`: The new block to sync the storage with.
    /// - `within_gc_range`: Whether the block is within the GC range.
    ///
    ///  # Returns
    /// - Ok(true) if the storage was synced with the block, or a pending block already exists for the given height.
    /// - Ok(false) if the block, or one of the pending blocks, is out of order.
    /// - Err(anyhow::Error) if any other error occured.
    fn try_sync_storage_with_block(&self, new_block: &Block<N>, within_gc_range: bool) -> Result<bool> {
        // Acquire the pending blocks lock.
        let mut pending_blocks = self.pending_blocks.lock();

        if let Some(tail) = pending_blocks.back() {
            if tail.height() >= new_block.height() {
                debug!(
                    "A unconfirmed block is queued already for height {height}. \
                    Will not sync.",
                    height = new_block.height()
                );
                return Ok(true);
            }

            ensure_equals!(tail.height() + 1, new_block.height(), "Got an out-of-order block");
        }

        // Fetch the latest block height.
        let ledger_block_height = self.ledger.latest_block_height();
        let new_block_height = new_block.height();

        // Clear any older pending blocks.
        // TODO(kaimast): ensure there are no dangling block requests
        while let Some(pending_block) = pending_blocks.front() {
            if pending_block.height() > ledger_block_height {
                break;
            }

            trace!(
                "Pending block {hash} at height {height} became obsolete",
                hash = pending_block.hash(),
                height = pending_block.height()
            );
            pending_blocks.pop_front();
        }

        // Check the block against the chain of pending blocks and append it on success.
        let new_block = match self.ledger.check_block_subdag(new_block.clone(), pending_blocks.make_contiguous()) {
            Ok(new_block) => new_block,
            // Retry if one of the pending blocks became obsolete.
            Err(CheckBlockError::InvalidPrefix { index, .. }) => {
                let height = pending_blocks.get(index).with_context(|| "Invalid prefix index")?.height();
                debug!("Pending block at height {height} became obsolete. Will retry with updated prefix.",);

                while let Some(pending_block) = pending_blocks.front()
                    && pending_block.height() <= height
                {
                    trace!("Removing obsolete pending block at height {}.", pending_block.height());
                    pending_blocks.pop_front();
                }

                return Ok(false);
            }
            // If the ledger already advanced, consider it a success.
            Err(CheckBlockError::BlockAlreadyExists { .. })
            | Err(CheckBlockError::InvalidHeight { .. })
            | Err(CheckBlockError::InvalidRound { .. }) => {
                debug!(
                    "Tried to sync storage with block at height {new_block_height}, but it was already in the ledger."
                );
                return Ok(true);
            }
            // Any other error should be returned to the caller.
            Err(err) => return Err(err.into_anyhow()),
        };

        trace!(
            "Adding new pending block {hash} at height {height}",
            hash = new_block.hash(),
            height = new_block.height()
        );
        pending_blocks.push_back(new_block);

        // Fetch the latest block height.
        let ledger_block_height = self.ledger.latest_block_height();

        // We can only commit a pending block when there are at least two, as a successor with sufficient votes is required.
        let Some(penultimate_index) = pending_blocks.len().checked_sub(1) else {
            return Ok(true);
        };

        // Now, figure out if and which pending block we can commit.
        // To do this effectively and because commits are transitive,
        // we iterate in reverse so that we can stop at the first successful check.
        //
        // Note, that if the storage already contains certificates for the round after new block,
        // the availability threshold for the new block could also be reached.
        let commit_height = 'outer: {
            let pending_blocks = pending_blocks.make_contiguous();
            for index in (0..penultimate_index).rev() {
                let block = &pending_blocks[index];
                let successors = &pending_blocks[index + 1..];

                // This check assumes that the pending blocks are properly linked together, based on the fact that,
                // to generate the sequence of `PendingBlocks`, each block needs to successfully be processed by `Ledger::check_block_subdag`.
                // As a result, the safety of this piece of code relies on the correctness `Ledger::check_block_subdag`,
                // which is tested in `snarkvm/ledger/tests/pending_block.rs`.
                if self
                    .is_block_availability_threshold_reached(block, successors)
                    .with_context(|| "Availability threshold check failed")?
                {
                    break 'outer block.height();
                }
            }

            trace!("No pending block are ready to be committed ({} block(s) are pending)", pending_blocks.len());
            return Ok(true);
        };

        let ledger_update = match self.ledger.begin_ledger_update() {
            Ok(update) => update,
            Err(BeginLedgerUpdateError::ShuttingDown) => {
                info!("BlockSync cannot advance the ledger any more. The node is shutting down.");
                return Ok(true);
            }
            Err(err) => {
                return Err(anyhow!("Unexpected error when beginning ledger update: {err}"));
            }
        };

        let start_height = ledger_block_height + 1;
        ensure!(commit_height >= start_height, "Invalid commit height");
        let num_blocks = (commit_height - start_height + 1) as usize;

        // Create a more detailed log message if we are committing more than one block at a time.
        if num_blocks > 1 {
            trace!(
                "Attempting to commit {chain_length} pending block(s) starting at height {start_height}.",
                chain_length = pending_blocks.len(),
            );
        }

        for pending_block in pending_blocks.drain(0..num_blocks) {
            let hash = pending_block.hash();
            let height = pending_block.height();
            let storage = self.storage.clone();

            let block = match ledger_update.check_block_content(pending_block) {
                Ok(block) => block,
                Err(CheckBlockError::InvalidHeight { .. })
                | Err(CheckBlockError::BlockAlreadyExists { .. })
                | Err(CheckBlockError::InvalidRound { .. }) => {
                    // If the block was outdated, stop here and request a retry.
                    // The outdated pending block has already been removed (due to the `drain` call above)
                    debug!("Pending block at height {height} became obsolete. Will retry with updated prefix.");
                    return Ok(false);
                }
                Err(err) => {
                    return Err(err
                        .into_anyhow()
                        .context(format!("Failed to check contents of pending block {hash} at height {height}")));
                }
            };

            trace!("Adding pending block {hash} at height {height} to the ledger");
            ledger_update.advance_to_next_block(&block)?;
            // Sync the height with the block.
            storage.sync_height_with_block(block.height());
            // Sync the round with the block.
            storage.sync_round_with_block(block.round());

            if within_gc_range
                && let Some(cb) = self.sync_callback.get()
                && let Authority::Quorum(subdag) = block.authority()
            {
                for round in subdag.values() {
                    for certificate in round {
                        cb.commit_certificate_from_sync(certificate);
                    }
                }
            }
        }

        Ok(true)
    }
}

// Methods to assist with the block sync module.
impl<N: Network> Sync<N> {
    /// Returns `true` if the node is synced and has connected peers.
    pub fn is_synced(&self) -> bool {
        self.block_sync.is_block_synced()
    }

    /// Returns the number of blocks the node is behind the greatest peer height.
    pub fn num_blocks_behind(&self) -> Option<u32> {
        self.block_sync.num_blocks_behind()
    }

    /// Returns the current block locators of the node.
    pub fn get_block_locators(&self) -> Result<BlockLocators<N>> {
        self.block_sync.get_block_locators()
    }
}

// Methods to assist with fetching batch certificates from peers.
impl<N: Network> Sync<N> {
    /// Sends a certificate request to the specified peer.
    pub async fn send_certificate_request(
        &self,
        peer_ip: SocketAddr,
        certificate_id: Field<N>,
    ) -> Result<BatchCertificate<N>> {
        // Initialize a oneshot channel.
        let (callback_sender, callback_receiver) = oneshot::channel();
        // Determine how many sent requests are pending.
        let num_sent_requests = self.pending.num_sent_requests(certificate_id);
        // Determine if we've already sent a request to the peer.
        let contains_peer_with_sent_request = self.pending.contains_peer_with_sent_request(certificate_id, peer_ip);
        // Determine the maximum number of redundant requests.
        let num_redundant_requests = max_redundant_requests(self.ledger.clone(), self.storage.current_round())?;
        // Establish whether the peers who already got the request collectively hold sufficient stake.
        let stake_redundancy_reached = || self.pending.request_stake_redundancy_reached(&self.gateway, certificate_id);
        // Determine if we should send a certificate request to the peer.
        // Each peer can only receive one request at a time.
        // We send at most `num_redundant_requests` requests, unless the stake redundancy factor hasn't been reached.
        let should_send_request = !contains_peer_with_sent_request
            && (num_sent_requests < num_redundant_requests || !stake_redundancy_reached()?);

        // Insert the certificate ID into the pending queue.
        self.pending.insert(certificate_id, peer_ip, Some((callback_sender, should_send_request)));

        // If the number of requests is less than or equal to the redundancy factor, send the certificate request to the peer.
        if should_send_request {
            // Send the certificate request to the peer.
            if self.gateway.send(peer_ip, Event::CertificateRequest(certificate_id.into())).await.is_none() {
                bail!("Unable to fetch batch certificate {certificate_id} (failed to send request)")
            }
        } else {
            debug!(
                "Skipped sending request for certificate {} to '{peer_ip}' ({num_sent_requests} redundant requests)",
                fmt_id(certificate_id)
            );
        }
        // Wait for the certificate to be fetched.
        // TODO (raychu86): Consider making the timeout dynamic based on network traffic and/or the number of validators.
        tokio::time::timeout(MAX_FETCH_TIMEOUT, callback_receiver)
            .await
            .with_context(|| format!("Unable to fetch batch certificate {} (timeout)", fmt_id(certificate_id)))?
            .with_context(|| format!("Unable to fetch batch certificate {}", fmt_id(certificate_id)))
    }

    /// Handles the incoming certificate request.
    fn send_certificate_response(&self, peer_ip: SocketAddr, request: CertificateRequest<N>) {
        // Attempt to retrieve the certificate.
        if let Some(certificate) = self.storage.get_certificate(request.certificate_id) {
            // Send the certificate response to the peer.
            let self_ = self.clone();
            tokio::spawn(async move {
                let _ = self_.gateway.send(peer_ip, Event::CertificateResponse(certificate.into())).await;
            });
        }
    }

    /// Handles the incoming certificate response.
    /// This method ensures the certificate response is well-formed and matches the certificate ID.
    fn finish_certificate_request(&self, peer_ip: SocketAddr, response: CertificateResponse<N>) {
        let certificate = response.certificate;
        // Check if the peer IP exists in the pending queue for the given certificate ID.
        let exists = self.pending.get_peers(certificate.id()).unwrap_or_default().contains(&peer_ip);
        // If the peer IP exists, finish the pending request.
        if exists {
            // Ensure the certificate is valid before notifying any of the callbacks.
            // This protects against a malicious invalid certificate response.
            if let Err(error) = self.storage.check_incoming_certificate(&certificate) {
                warn!("Skipping invalid certificate {} from '{peer_ip}' - {error}", fmt_id(certificate.id()));
                return;
            }
            // Remove the certificate ID from the pending queue.
            self.pending.remove(certificate.id(), Some(certificate));
        }
    }
}

impl<N: Network> Sync<N> {
    /// Spawns a task with the given future; it should only be used for long-running tasks.
    fn spawn<T: Future<Output = ()> + Send + 'static>(&self, future: T) {
        self.handles.lock().push(tokio::spawn(future));
    }

    /// Shuts down the primary.
    pub async fn shut_down(&self) {
        info!("Shutting down the sync module...");
        // Remove the callback.
        self.sync_callback.clear();
        // Acquire the response lock.
        let _lock = self.response_lock.lock().await;
        // Abort the tasks.
        self.handles.lock().iter().for_each(|handle| handle.abort());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::{
        BFT,
        helpers::now,
        ledger_service::{CoreLedgerService, MockLedgerService},
        storage_service::BFTMemoryService,
    };

    use snarkos_account::Account;
    use snarkos_node_network::ConnectionMode;
    use snarkos_node_sync::BlockSync;
    use snarkos_utilities::{NodeDataDir, SimpleStoppable};

    use snarkvm::{
        console::{
            account::{Address, PrivateKey},
            network::MainnetV0,
        },
        ledger::{
            narwhal::{BatchCertificate, BatchHeader, Subdag},
            store::{ConsensusStore, helpers::memory::ConsensusMemory},
        },
        prelude::{Ledger, VM},
        utilities::TestRng,
    };

    use aleo_std::StorageMode;
    use indexmap::IndexSet;
    use rand::RngExt;
    use std::{collections::BTreeMap, sync::OnceLock};

    type CurrentNetwork = MainnetV0;
    type CurrentLedger = Ledger<CurrentNetwork, ConsensusMemory<CurrentNetwork>>;
    type CurrentConsensusStore = ConsensusStore<CurrentNetwork, ConsensusMemory<CurrentNetwork>>;

    /// Create four blocks, where only the last one contains enough certificates to advance the ledger.
    async fn setup_commit_chain(rng: &mut TestRng) -> (Block<CurrentNetwork>, Vec<Block<CurrentNetwork>>) {
        static CHAIN_CACHE: OnceLock<(Block<CurrentNetwork>, Vec<Block<CurrentNetwork>>)> = OnceLock::new();

        // Use cached version if it exists.
        if let Some((genesis, blocks)) = CHAIN_CACHE.get() {
            return (genesis.clone(), blocks.clone());
        }

        // Initialize the round parameters.
        let max_gc_rounds = BatchHeader::<CurrentNetwork>::MAX_GC_ROUNDS as u64;

        // The first round of the first block.
        let first_round: u64 = 1;
        // The total number of blocks we test
        let num_blocks = 3;
        // The last round of the last block.
        let last_round = first_round + num_blocks * 2;
        // The first round that has at least N-f certificates referencing the anchor from the previous round.
        // This is also the last round we use in the test.
        let first_threshold_round = 5;

        // Initialize the store.
        let store = CurrentConsensusStore::open(StorageMode::new_test(None)).unwrap();
        let account: Account<CurrentNetwork> = Account::new(rng).unwrap();

        // Create a genesis block with a seeded RNG to reproduce the same genesis private keys.
        let seed: u64 = rng.random();
        let vm = VM::from(store).unwrap();
        let genesis_pk = *account.private_key();
        let genesis = spawn_blocking!(vm.genesis_beacon(&genesis_pk, &mut TestRng::from_seed(seed))).unwrap();

        // Extract the private keys from the genesis committee by using the same RNG to sample private keys.
        let genesis_rng = &mut TestRng::from_seed(seed);
        let private_keys = [
            *account.private_key(),
            PrivateKey::new(genesis_rng).unwrap(),
            PrivateKey::new(genesis_rng).unwrap(),
            PrivateKey::new(genesis_rng).unwrap(),
        ];

        // Initialize the ledger with the genesis block.
        let genesis_clone = genesis.clone();
        let ledger = spawn_blocking!(CurrentLedger::load(genesis_clone, StorageMode::new_test(None))).unwrap();
        // Initialize the ledger.
        let core_ledger = Arc::new(CoreLedgerService::new(ledger.clone(), SimpleStoppable::new()));

        // Sample 5 rounds of batch certificates starting at the genesis round from a static set of 4 authors.
        let (round_to_certificates_map, committee) = {
            let addresses = [
                Address::try_from(private_keys[0]).unwrap(),
                Address::try_from(private_keys[1]).unwrap(),
                Address::try_from(private_keys[2]).unwrap(),
                Address::try_from(private_keys[3]).unwrap(),
            ];

            let committee = ledger.latest_committee().unwrap();

            // Initialize a mapping from the round number to the set of batch certificates in the round.
            let mut round_to_certificates_map: HashMap<u64, IndexSet<BatchCertificate<CurrentNetwork>>> =
                HashMap::new();
            let mut previous_certificates: IndexSet<BatchCertificate<CurrentNetwork>> = IndexSet::with_capacity(4);

            for round in first_round..=last_round {
                let mut current_certificates = IndexSet::new();
                let previous_certificate_ids: IndexSet<_> = if round == 0 || round == 1 {
                    IndexSet::new()
                } else {
                    previous_certificates.iter().map(|c| c.id()).collect()
                };

                let committee_id = committee.id();

                // Determine if there was a leader in the previous round.
                let is_certificate_round = !round.is_multiple_of(2);
                let prev_leader = if is_certificate_round && let Some(prev_round) = round.checked_sub(1) {
                    Some(committee.get_leader(prev_round).unwrap())
                } else {
                    None
                };

                // Generate all certificates for the round.
                for (i, private_key) in private_keys.iter().enumerate() {
                    let previous_leader_index =
                        addresses.iter().position(|&addr| prev_leader.is_some_and(|prev_leader| addr == prev_leader));

                    // For the first two blocks non-leaders will not reference the leader certificate.
                    // This means, while there was an anchor in the previous round, it is not committed until later.
                    let previous_certs = if let Some(previous_leader_index) = previous_leader_index
                        && round < first_threshold_round
                        && i != previous_leader_index
                    {
                        // Remove the reference to the previous leader certificate.
                        previous_certificate_ids
                            .iter()
                            .cloned()
                            .enumerate()
                            .filter(|(idx, _)| *idx != previous_leader_index)
                            .map(|(_, id)| id)
                            .collect()
                    } else {
                        previous_certificate_ids.clone()
                    };

                    let batch_header = BatchHeader::new(
                        private_key,
                        round,
                        now(),
                        committee_id,
                        Default::default(),
                        previous_certs,
                        rng,
                    )
                    .unwrap();

                    // Sign the batch header.
                    let mut signatures = IndexSet::with_capacity(4);
                    for (j, private_key_2) in private_keys.iter().enumerate() {
                        if i != j {
                            signatures.insert(private_key_2.sign(&[batch_header.batch_id()], rng).unwrap());
                        }
                    }
                    current_certificates.insert(BatchCertificate::from(batch_header, signatures).unwrap());
                }

                // Update the map of certificates.
                round_to_certificates_map.insert(round, current_certificates.clone());
                previous_certificates = current_certificates;
            }
            (round_to_certificates_map, committee)
        };

        // Initialize the storage.
        let storage = Storage::new(core_ledger.clone(), Arc::new(BFTMemoryService::new()), max_gc_rounds).unwrap();

        // Create a list of all certificates.
        let certificates: Vec<_> =
            round_to_certificates_map.into_values().flat_map(|certificates| certificates.into_iter()).collect();

        // insert all certificates into storage.
        for certificate in certificates.iter() {
            storage.testing_only_insert_certificate_testing_only(certificate.clone());
        }

        // Create the blocks
        let mut previous_leader_cert = None;
        let mut blocks = vec![];

        for block_height in 1..=num_blocks {
            let leader_round = block_height * 2;

            let leader = committee.get_leader(leader_round).unwrap();
            let leader_certificate = storage.get_certificate_for_round_with_author(leader_round, leader).unwrap();

            let mut subdag_map: BTreeMap<u64, IndexSet<BatchCertificate<CurrentNetwork>>> = BTreeMap::new();
            let mut leader_cert_map = IndexSet::new();
            leader_cert_map.insert(leader_certificate.clone());

            let previous_cert_map = storage.get_certificates_for_round(leader_round - 1);

            subdag_map.insert(leader_round, leader_cert_map.clone());
            subdag_map.insert(leader_round - 1, previous_cert_map.clone());

            if leader_round > 2 {
                let previous_commit_cert_map: IndexSet<_> = storage
                    .get_certificates_for_round(leader_round - 2)
                    .into_iter()
                    .filter(|cert| {
                        if let Some(previous_leader_cert) = &previous_leader_cert {
                            cert != previous_leader_cert
                        } else {
                            true
                        }
                    })
                    .collect();
                subdag_map.insert(leader_round - 2, previous_commit_cert_map);
            }

            let subdag = Subdag::from(subdag_map.clone()).unwrap();
            previous_leader_cert = Some(leader_certificate);

            let core_ledger = core_ledger.clone();
            let block = spawn_blocking!({
                let ledger_update = core_ledger.begin_ledger_update()?;
                let block = ledger_update.prepare_advance_to_next_quorum_block(subdag, Default::default())?;
                ledger_update.advance_to_next_block(&block)?;
                Ok(block)
            })
            .unwrap();

            blocks.push(block);
        }

        CHAIN_CACHE.get_or_init(|| (genesis, blocks)).clone()
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_commit_chain_with_bft() {
        let rng = &mut TestRng::default();

        let (genesis, mut blocks) = setup_commit_chain(rng).await;
        let max_gc_rounds = BatchHeader::<CurrentNetwork>::MAX_GC_ROUNDS as u64;

        // ### Test that sync works as expected ###
        let storage_mode = StorageMode::new_test(None);

        // Create a new ledger to test with, but use the existing storage
        // so that the certificates exist.
        let syncing_ledger = {
            let storage_mode = storage_mode.clone();
            Arc::new(CoreLedgerService::new(
                spawn_blocking!(CurrentLedger::load(genesis, storage_mode)).unwrap(),
                SimpleStoppable::new(),
            ))
        };

        let account = Account::new(rng).unwrap();
        let syncing_storage =
            Storage::new(syncing_ledger.clone(), Arc::new(BFTMemoryService::new()), max_gc_rounds).unwrap();
        let gateway = Gateway::new(
            account.clone(),
            syncing_storage.clone(),
            syncing_ledger.clone(),
            None,
            &[],
            false,
            NodeDataDir::new_test(None),
            None,
        )
        .unwrap();

        let block_sync = Arc::new(BlockSync::new(syncing_ledger.clone(), ConnectionMode::Gateway));
        let sync = Sync::new(gateway.clone(), syncing_storage.clone(), syncing_ledger.clone(), block_sync.clone());

        let syncing_bft = BFT::new(
            account.clone(),
            syncing_storage.clone(),
            syncing_ledger.clone(),
            block_sync,
            None,
            &[],
            false,
            NodeDataDir::new_test(None),
            None,
            1,
        )
        .unwrap();

        sync.initialize(Some(Arc::new(syncing_bft.clone()))).unwrap();

        // -- Run test -- //

        let last_block = blocks.pop().unwrap();

        // Insert the blocks into the new sync module
        for block in blocks {
            sync.sync_storage_with_block(block, true).await.unwrap();
            // Availability threshold is not met, so we should not advance yet.
            assert_eq!(syncing_bft.testing_only_latest_committed_round(), 0);
        }

        // Only for the final block, the availability threshold is met,
        // because certificates for the subsequent round are already in storage.
        sync.sync_storage_with_block(last_block, true).await.unwrap();

        // Ensure the leaders are committed.
        // (blocks are not created as there is no active consensus instance)
        assert_eq!(syncing_bft.testing_only_latest_committed_round(), 4);
    }

    /// Verifies that after syncing blocks, the Sync module updates storage (BFT ledger) accordingly:
    /// every certificate from each block's subDAG is present in storage, and height/round are updated.
    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_sync_updates_storage_with_block_certificates() {
        let rng = &mut TestRng::default();

        let (genesis, blocks) = setup_commit_chain(rng).await;
        let max_gc_rounds = BatchHeader::<CurrentNetwork>::MAX_GC_ROUNDS as u64;
        let storage_mode = StorageMode::new_test(None);

        let syncing_ledger = Arc::new(CoreLedgerService::new(
            spawn_blocking!(CurrentLedger::load(genesis, storage_mode)).unwrap(),
            SimpleStoppable::new(),
        ));

        let account = Account::new(rng).unwrap();
        let syncing_storage =
            Storage::new(syncing_ledger.clone(), Arc::new(BFTMemoryService::new()), max_gc_rounds).unwrap();
        let gateway = Gateway::new(
            account.clone(),
            syncing_storage.clone(),
            syncing_ledger.clone(),
            None,
            &[],
            false,
            NodeDataDir::new_test(None),
            None,
        )
        .unwrap();

        let block_sync = Arc::new(BlockSync::new(syncing_ledger.clone(), ConnectionMode::Gateway));
        let sync = Sync::new(gateway.clone(), syncing_storage.clone(), syncing_ledger.clone(), block_sync.clone());

        let syncing_bft = BFT::new(
            account.clone(),
            syncing_storage.clone(),
            syncing_ledger.clone(),
            block_sync,
            None,
            &[],
            false,
            NodeDataDir::new_test(None),
            None,
            1,
        )
        .unwrap();

        sync.initialize(Some(Arc::new(syncing_bft.clone()))).unwrap();

        // Sync all blocks in order.
        for block in &blocks {
            sync.sync_storage_with_block(block.clone(), true).await.unwrap();
        }

        // The last block stays pending (no successor to satisfy availability threshold).
        // Only committed blocks have their certificates in the ledger.
        let committed_blocks = &blocks[..blocks.len().saturating_sub(1)];

        // Assert Sync updated the underlying ledger accordingly: every certificate from each
        // committed block's subDAG is present in the ledger, and ledger height/round are updated.
        for block in committed_blocks {
            let Authority::Quorum(subdag) = block.authority() else {
                continue;
            };
            for certificates in subdag.values() {
                for cert in certificates {
                    assert!(
                        syncing_ledger.contains_certificate(&cert.id()).unwrap_or(false),
                        "Sync should have committed block {} so certificate is in the ledger",
                        block.height()
                    );
                }
            }
        }

        // Ledger height and round should match the latest committed block (not the tip).
        let last_committed_block = committed_blocks.last().unwrap();
        assert_eq!(
            syncing_ledger.latest_block_height(),
            last_committed_block.height(),
            "Ledger height should match last committed block"
        );
        assert_eq!(
            syncing_ledger.latest_block().round(),
            last_committed_block.round(),
            "Ledger round should match last committed block"
        );
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_commit_chain_with_swich_to_bft() {
        let rng = &mut TestRng::default();
        let (genesis, mut blocks) = setup_commit_chain(rng).await;
        let max_gc_rounds = BatchHeader::<CurrentNetwork>::MAX_GC_ROUNDS as u64;
        let storage_mode = StorageMode::new_test(None);

        // Create a new ledger to test with, but use the existing storage
        // so that the certificates exist.
        let syncing_ledger = {
            let storage_mode = storage_mode.clone();
            Arc::new(CoreLedgerService::new(
                spawn_blocking!(CurrentLedger::load(genesis, storage_mode)).unwrap(),
                SimpleStoppable::new(),
            ))
        };

        let account = Account::new(rng).unwrap();
        let syncing_storage =
            Storage::new(syncing_ledger.clone(), Arc::new(BFTMemoryService::new()), max_gc_rounds).unwrap();
        let gateway = Gateway::new(
            account.clone(),
            syncing_storage.clone(),
            syncing_ledger.clone(),
            None,
            &[],
            false,
            NodeDataDir::new_test(None),
            None,
        )
        .unwrap();

        let block_sync = Arc::new(BlockSync::new(syncing_ledger.clone(), ConnectionMode::Gateway));
        let sync = Sync::new(gateway.clone(), syncing_storage.clone(), syncing_ledger.clone(), block_sync.clone());

        let syncing_bft = BFT::new(
            account.clone(),
            syncing_storage.clone(),
            syncing_ledger.clone(),
            block_sync,
            None,
            &[],
            false,
            NodeDataDir::new_test(None),
            None,
            1,
        )
        .unwrap();

        sync.initialize(Some(Arc::new(syncing_bft.clone()))).unwrap();

        // -- Run test -- //
        let last_block = blocks.pop().unwrap();

        // Insert all but the last block into the sync module
        // These are added without BFT.
        for block in blocks {
            sync.sync_storage_with_block(block, false).await.unwrap();

            // Availability threshold is not met, so we should not advance yet.
            assert_eq!(syncing_ledger.latest_block_height(), 0);
        }

        // -- Switch to BFT --
        sync.sync_storage_with_ledger_at_bootup().unwrap();

        // Ensure blocks did not commit yet.
        assert_eq!(syncing_ledger.latest_block_height(), 0);
        assert_eq!(syncing_bft.testing_only_latest_committed_round(), 0);

        // Only for the final block, the availability threshold is met,
        // because certificates for the subsequent round are already in storage.
        sync.sync_storage_with_block(last_block, true).await.unwrap();

        // Ensure blocks 1 and 2 were added to the ledger.
        // Unlike with normal sync, the ledger is advanced by Sync when pending blocks are committed.
        assert_eq!(syncing_bft.testing_only_latest_committed_round(), 4);
    }

    /// Tests that a node can correctly revert from DAG sync back to fast sync.
    ///
    /// This mirrors `test_commit_chain_with_swich_to_bft` in the opposite direction:
    /// the first blocks are processed in DAG-sync mode (within GC range), then the
    /// final block is processed in fast-sync mode (outside GC range, no DAG updates).
    ///
    /// The ledger should still advance correctly because `pending_blocks` and the
    /// availability-threshold check are shared between both modes.
    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_commit_chain_with_switch_to_fast_sync() {
        let rng = &mut TestRng::default();
        let (genesis, mut blocks) = setup_commit_chain(rng).await;
        let max_gc_rounds = BatchHeader::<CurrentNetwork>::MAX_GC_ROUNDS as u64;
        let storage_mode = StorageMode::new_test(None);

        let syncing_ledger = {
            let storage_mode = storage_mode.clone();
            Arc::new(CoreLedgerService::new(
                spawn_blocking!(CurrentLedger::load(genesis, storage_mode)).unwrap(),
                SimpleStoppable::new(),
            ))
        };

        let account = Account::new(rng).unwrap();
        let syncing_storage =
            Storage::new(syncing_ledger.clone(), Arc::new(BFTMemoryService::new()), max_gc_rounds).unwrap();
        let gateway = Gateway::new(
            account.clone(),
            syncing_storage.clone(),
            syncing_ledger.clone(),
            None,
            &[],
            false,
            NodeDataDir::new_test(None),
            None,
        )
        .unwrap();

        let block_sync = Arc::new(BlockSync::new(syncing_ledger.clone(), ConnectionMode::Gateway));
        let sync = Sync::new(gateway.clone(), syncing_storage.clone(), syncing_ledger.clone(), block_sync.clone());

        let syncing_bft = BFT::new(
            account.clone(),
            syncing_storage.clone(),
            syncing_ledger.clone(),
            block_sync,
            None,
            &[],
            false,
            NodeDataDir::new_test(None),
            None,
            1,
        )
        .unwrap();

        sync.initialize(Some(Arc::new(syncing_bft.clone()))).unwrap();

        // -- Run test -- //
        let last_block = blocks.pop().unwrap();

        // Insert all but the last block in DAG-sync mode (within GC range).
        // Certificates are inserted into the BFT DAG but the availability threshold
        // is not yet met, so the ledger should not advance.
        for block in blocks {
            sync.sync_storage_with_block(block, true).await.unwrap();
            assert_eq!(syncing_ledger.latest_block_height(), 0);
        }

        // -- Switch back to fast sync (simulate the network tip dropping below the GC boundary) --

        // The final block is processed in fast-sync mode: no DAG updates.
        // The pending_blocks chain now has enough successors to confirm the availability
        // threshold for block 2, so the ledger advances to height 2.
        // Block 3 (the fast-sync one) remains pending — it needs a further successor
        // with enough votes to be confirmed.
        sync.sync_storage_with_block(last_block, false).await.unwrap();

        // Blocks 1 and 2 should have been committed to the ledger.
        assert_eq!(syncing_ledger.latest_block_height(), 2);
        assert!(syncing_ledger.contains_block_height(1));
        assert!(syncing_ledger.contains_block_height(2));

        // The BFT committed round is 0: the last block was processed in fast-sync mode so
        // its certificates were never passed to the BFT DAG, meaning the BFT itself did not
        // advance its committed round beyond the initial state.
        assert_eq!(syncing_bft.testing_only_latest_committed_round(), 0);
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_commit_chain_without_bft() {
        let rng = &mut TestRng::default();
        let (genesis, mut blocks) = setup_commit_chain(rng).await;
        let max_gc_rounds = BatchHeader::<CurrentNetwork>::MAX_GC_ROUNDS as u64;
        let storage_mode = StorageMode::new_test(None);

        // Create a new ledger to test with, but use the existing storage
        // so that the certificates exist.
        let syncing_ledger = {
            let storage_mode = storage_mode.clone();
            Arc::new(CoreLedgerService::new(
                spawn_blocking!(CurrentLedger::load(genesis, storage_mode)).unwrap(),
                SimpleStoppable::new(),
            ))
        };

        let account = Account::new(rng).unwrap();
        let syncing_storage =
            Storage::new(syncing_ledger.clone(), Arc::new(BFTMemoryService::new()), max_gc_rounds).unwrap();
        let gateway = Gateway::new(
            account.clone(),
            syncing_storage.clone(),
            syncing_ledger.clone(),
            None,
            &[],
            false,
            NodeDataDir::new_test(None),
            None,
        )
        .unwrap();

        let block_sync = Arc::new(BlockSync::new(syncing_ledger.clone(), ConnectionMode::Gateway));
        let sync = Sync::new(gateway.clone(), syncing_storage.clone(), syncing_ledger.clone(), block_sync.clone());

        let syncing_bft = BFT::new(
            account.clone(),
            syncing_storage.clone(),
            syncing_ledger.clone(),
            block_sync,
            None,
            &[],
            false,
            NodeDataDir::new_test(None),
            None,
            1,
        )
        .unwrap();

        sync.initialize(Some(Arc::new(syncing_bft.clone()))).unwrap();

        // -- Run test -- //
        let last_block = blocks.pop().unwrap();

        // Insert all but the last block into the sync module
        for block in blocks {
            sync.sync_storage_with_block(block, false).await.unwrap();

            // Availability threshold is not met, so we should not advance yet.
            assert_eq!(syncing_ledger.latest_block_height(), 0);
        }

        // Only for the final block, the availability threshold is met,
        // because certificates for the subsequent round are already in storage.
        sync.sync_storage_with_block(last_block, false).await.unwrap();
        assert_eq!(syncing_ledger.latest_block_height(), 2);

        // Ensure blocks 1 and 2 were added to the ledger.
        assert!(syncing_ledger.contains_block_height(1));
        assert!(syncing_ledger.contains_block_height(2));
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_pending_certificates() -> anyhow::Result<()> {
        let rng = &mut TestRng::default();
        // Initialize the round parameters.
        let max_gc_rounds = BatchHeader::<CurrentNetwork>::MAX_GC_ROUNDS as u64;
        let commit_round = 2;

        // Initialize the store.
        let store = CurrentConsensusStore::open(StorageMode::new_test(None)).unwrap();
        let account: Account<CurrentNetwork> = Account::new(rng)?;

        // Create a genesis block with a seeded RNG to reproduce the same genesis private keys.
        let seed: u64 = rng.random();
        let vm = VM::from(store).unwrap();
        let genesis_pk = *account.private_key();
        let genesis = spawn_blocking!(vm.genesis_beacon(&genesis_pk, &mut TestRng::from_seed(seed))).unwrap();

        // Extract the private keys from the genesis committee by using the same RNG to sample private keys.
        let genesis_rng = &mut TestRng::from_seed(seed);
        let private_keys = [
            *account.private_key(),
            PrivateKey::new(genesis_rng)?,
            PrivateKey::new(genesis_rng)?,
            PrivateKey::new(genesis_rng)?,
        ];

        // Initialize the ledger with the genesis block.
        let core_ledger = {
            let ledger = spawn_blocking!(CurrentLedger::load(genesis, StorageMode::new_test(None))).unwrap();
            Arc::new(CoreLedgerService::new(ledger.clone(), SimpleStoppable::new()))
        };

        // Sample rounds of batch certificates starting at the genesis round from a static set of 4 authors.
        let (round_to_certificates_map, committee) = {
            // Initialize the committee.
            let committee = core_ledger.current_committee().unwrap();
            // Initialize a mapping from the round number to the set of batch certificates in the round.
            let mut round_to_certificates_map: HashMap<u64, IndexSet<BatchCertificate<CurrentNetwork>>> =
                HashMap::new();
            let mut previous_certificates: IndexSet<BatchCertificate<CurrentNetwork>> = IndexSet::with_capacity(4);

            for round in 0..=commit_round + 8 {
                let mut current_certificates = IndexSet::new();
                let previous_certificate_ids: IndexSet<_> = if round == 0 || round == 1 {
                    IndexSet::new()
                } else {
                    previous_certificates.iter().map(|c| c.id()).collect()
                };
                let committee_id = committee.id();
                // Create a certificate for each validator.
                for (i, private_key_1) in private_keys.iter().enumerate() {
                    let batch_header = BatchHeader::new(
                        private_key_1,
                        round,
                        now(),
                        committee_id,
                        Default::default(),
                        previous_certificate_ids.clone(),
                        rng,
                    )
                    .unwrap();
                    // Sign the batch header.
                    let mut signatures = IndexSet::with_capacity(4);
                    for (j, private_key_2) in private_keys.iter().enumerate() {
                        if i != j {
                            signatures.insert(private_key_2.sign(&[batch_header.batch_id()], rng).unwrap());
                        }
                    }
                    current_certificates.insert(BatchCertificate::from(batch_header, signatures).unwrap());
                }

                // Update the map of certificates.
                round_to_certificates_map.insert(round, current_certificates.clone());
                previous_certificates = current_certificates.clone();
            }
            (round_to_certificates_map, committee)
        };

        // Initialize the storage.
        let storage = Storage::new(core_ledger.clone(), Arc::new(BFTMemoryService::new()), max_gc_rounds).unwrap();
        // Insert certificates into storage.
        let mut certificates: Vec<BatchCertificate<CurrentNetwork>> = Vec::new();
        for i in 1..=commit_round + 8 {
            let c = (*round_to_certificates_map.get(&i).unwrap()).clone();
            certificates.extend(c);
        }
        for certificate in certificates.clone().iter() {
            storage.testing_only_insert_certificate_testing_only(certificate.clone());
        }

        let leader_round_1 = commit_round;
        let leader_1 = committee.get_leader(leader_round_1).unwrap();
        let leader_certificate = storage.get_certificate_for_round_with_author(commit_round, leader_1).unwrap();
        let mut subdag_map: BTreeMap<u64, IndexSet<BatchCertificate<CurrentNetwork>>> = BTreeMap::new();

        // Create subdag for block 1.
        let subdag_1 = {
            let mut leader_cert_map = IndexSet::new();
            leader_cert_map.insert(leader_certificate.clone());
            let mut previous_cert_map = IndexSet::new();
            for cert in storage.get_certificates_for_round(commit_round - 1) {
                previous_cert_map.insert(cert);
            }
            subdag_map.insert(commit_round, leader_cert_map.clone());
            subdag_map.insert(commit_round - 1, previous_cert_map.clone());
            Subdag::from(subdag_map.clone())?
        };

        let core_ledger_cpy = core_ledger.clone();
        spawn_blocking!({
            // Create block 1.
            let update1 = core_ledger_cpy.begin_ledger_update()?;
            let block_1 = update1.prepare_advance_to_next_quorum_block(subdag_1, Default::default())?;

            // Insert block 1.
            update1.advance_to_next_block(&block_1)?;

            Ok(())
        })?;

        // Prepare DAG for block 2.
        let mut subdag_map_2: BTreeMap<u64, IndexSet<BatchCertificate<CurrentNetwork>>> = BTreeMap::new();
        let subdag_2 = {
            let leader_round_2 = commit_round + 2;
            let leader_2 = committee.get_leader(leader_round_2).unwrap();
            let leader_certificate_2 = storage.get_certificate_for_round_with_author(leader_round_2, leader_2).unwrap();
            let mut leader_cert_map_2 = IndexSet::new();
            leader_cert_map_2.insert(leader_certificate_2.clone());
            let mut previous_cert_map_2 = IndexSet::new();
            for cert in storage.get_certificates_for_round(leader_round_2 - 1) {
                previous_cert_map_2.insert(cert);
            }
            subdag_map_2.insert(leader_round_2, leader_cert_map_2.clone());
            subdag_map_2.insert(leader_round_2 - 1, previous_cert_map_2.clone());
            Subdag::from(subdag_map_2.clone())?
        };

        let core_ledger_cpy = core_ledger.clone();
        spawn_blocking!({
            let update2 = core_ledger_cpy.begin_ledger_update()?;

            // Create block 2.
            let block_2 = update2.prepare_advance_to_next_quorum_block(subdag_2, Default::default())?;

            // Insert block 2.
            update2.advance_to_next_block(&block_2)?;

            Ok(())
        })?;

        // Prepare DAG for block 3.
        let leader_round_3 = commit_round + 4;
        let leader_3 = committee.get_leader(leader_round_3).unwrap();
        let leader_certificate_3 = storage.get_certificate_for_round_with_author(leader_round_3, leader_3).unwrap();

        // Prepare DAG for block 3.
        let mut subdag_map_3: BTreeMap<u64, IndexSet<BatchCertificate<CurrentNetwork>>> = BTreeMap::new();
        let subdag_3 = {
            let mut leader_cert_map_3 = IndexSet::new();
            leader_cert_map_3.insert(leader_certificate_3.clone());
            let mut previous_cert_map_3 = IndexSet::new();
            for cert in storage.get_certificates_for_round(leader_round_3 - 1) {
                previous_cert_map_3.insert(cert);
            }
            subdag_map_3.insert(leader_round_3, leader_cert_map_3.clone());
            subdag_map_3.insert(leader_round_3 - 1, previous_cert_map_3.clone());
            Subdag::from(subdag_map_3.clone())?
        };

        let core_ledger_cpy = core_ledger.clone();
        spawn_blocking!({
            let update3 = core_ledger_cpy.begin_ledger_update()?;

            // Create block 3
            let block_3 = update3.prepare_advance_to_next_quorum_block(subdag_3, Default::default())?;

            // Insert block 3.
            update3.advance_to_next_block(&block_3)?;

            Ok(())
        })?;

        /*
            Check that the pending certificates are computed correctly.
        */

        // Retrieve the pending certificates.
        let pending_certificates = storage.get_pending_certificates();
        // Check that all of the pending certificates are not contained in the ledger.
        for certificate in pending_certificates.clone() {
            assert!(!core_ledger.contains_certificate(&certificate.id()).unwrap_or(false));
        }
        // Initialize an empty set to be populated with the committed certificates in the block subdags.
        let mut committed_certificates: IndexSet<BatchCertificate<CurrentNetwork>> = IndexSet::new();
        {
            let subdag_maps = [&subdag_map, &subdag_map_2, &subdag_map_3];
            for subdag in subdag_maps.iter() {
                for subdag_certificates in subdag.values() {
                    committed_certificates.extend(subdag_certificates.iter().cloned());
                }
            }
        };
        // Create the set of candidate pending certificates as the set of all certificates minus the set of the committed certificates.
        let mut candidate_pending_certificates: IndexSet<BatchCertificate<CurrentNetwork>> = IndexSet::new();
        for certificate in certificates.clone() {
            if !committed_certificates.contains(&certificate) {
                candidate_pending_certificates.insert(certificate);
            }
        }
        // Check that the set of pending certificates is equal to the set of candidate pending certificates.
        assert_eq!(pending_certificates, candidate_pending_certificates);

        Ok(())
    }

    /// A certificate ID commits to the batch header only, so a malicious peer can answer a
    /// certificate request with the genuine header paired with signatures that are cryptographically
    /// valid but do not come from the committee. Such a response still matches the requested ID and
    /// therefore reaches `finish_certificate_request`.
    ///
    /// Ensure it neither clears the pending entry nor fires the callbacks, since `Pending::remove`
    /// notifies *every* waiting peer; otherwise a single malicious validator could fail the fetch
    /// for all of them, and the later response from an honest peer would find no pending entry.
    #[tokio::test]
    async fn test_finish_certificate_request_rejects_invalid_certificate() -> anyhow::Result<()> {
        let rng = &mut TestRng::default();
        let max_gc_rounds = BatchHeader::<CurrentNetwork>::MAX_GC_ROUNDS as u64;

        // Sample a committee of 5 validators, and keep their private keys.
        let (committee, private_keys) =
            snarkvm::ledger::committee::test_helpers::sample_committee_and_keys_for_round(0, 5, rng);
        let committee_id = committee.id();

        // Initialize the ledger, storage, gateway, and sync.
        let ledger = Arc::new(MockLedgerService::new(committee));
        let storage = Storage::new(ledger.clone(), Arc::new(BFTMemoryService::new()), max_gc_rounds)?;
        let account = Account::new(rng)?;
        let gateway = Gateway::new(
            account,
            storage.clone(),
            ledger.clone(),
            None,
            &[],
            false,
            NodeDataDir::new_test(None),
            None,
        )?;
        let block_sync = Arc::new(BlockSync::new(ledger.clone(), ConnectionMode::Gateway));
        let sync = Sync::new(gateway, storage, ledger.clone(), block_sync);

        // Construct a batch header at round 1, authored by a committee member.
        let batch_header =
            BatchHeader::new(&private_keys[0], 1, now(), committee_id, Default::default(), IndexSet::new(), rng)?;
        let certificate_id = batch_header.batch_id();

        // The genuine certificate, endorsed by the rest of the committee.
        let mut valid_signatures = IndexSet::new();
        for private_key in private_keys.iter().skip(1) {
            valid_signatures.insert(private_key.sign(&[certificate_id], rng)?);
        }
        let valid_certificate = BatchCertificate::from(batch_header.clone(), valid_signatures)?;

        // The forged certificate: the same header, so the same ID, but endorsed by keys outside the
        // committee. The signatures are cryptographically valid, so this survives deserialization.
        let mut invalid_signatures = IndexSet::new();
        for _ in 0..4 {
            invalid_signatures.insert(PrivateKey::<CurrentNetwork>::new(rng)?.sign(&[certificate_id], rng)?);
        }
        let invalid_certificate = BatchCertificate::from(batch_header, invalid_signatures)?;
        assert_eq!(invalid_certificate.id(), valid_certificate.id(), "the attack requires a matching ID");

        // Register a pending request for the certificate from a malicious and an honest peer.
        let malicious_peer: SocketAddr = "127.0.0.1:1234".parse().unwrap();
        let honest_peer: SocketAddr = "127.0.0.1:5678".parse().unwrap();
        let (malicious_sender, _malicious_receiver) = oneshot::channel();
        let (honest_sender, honest_receiver) = oneshot::channel();
        sync.pending.insert(certificate_id, malicious_peer, Some((malicious_sender, true)));
        sync.pending.insert(certificate_id, honest_peer, Some((honest_sender, true)));
        assert_eq!(sync.pending.num_callbacks(certificate_id), 2);

        // The forged response must leave the pending entry, and both callbacks, untouched.
        sync.finish_certificate_request(malicious_peer, invalid_certificate.into());
        assert!(sync.pending.contains(certificate_id), "the forged response cleared the pending entry");
        assert_eq!(sync.pending.num_callbacks(certificate_id), 2, "the forged response consumed a callback");

        // The honest response is still able to complete the fetch.
        sync.finish_certificate_request(honest_peer, valid_certificate.clone().into());
        assert!(!sync.pending.contains(certificate_id));
        assert_eq!(honest_receiver.await?, valid_certificate);

        Ok(())
    }
}
