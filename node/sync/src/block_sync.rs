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
    helpers::{PeerPair, PrepareSyncRequest, SyncRequest},
    locators::BlockLocators,
};
use snarkos_node_bft_ledger_service::{BeginLedgerUpdateError, LedgerService};
use snarkos_node_network::ConnectionMode;
use snarkos_node_router::messages::DataBlocks;
use snarkos_node_sync_communication_service::CommunicationService;
use snarkos_node_sync_locators::{CHECKPOINT_INTERVAL, NUM_RECENT_BLOCKS};

use snarkvm::{
    console::network::{ConsensusVersion, Network},
    ledger::{Block, CheckBlockError},
    utilities::flatten_error,
};

use anyhow::{Context, Result, anyhow, bail, ensure};
use indexmap::{IndexMap, IndexSet};
use itertools::Itertools;
#[cfg(feature = "locktick")]
use locktick::parking_lot::{Mutex, RwLock};
#[cfg(feature = "locktick")]
use locktick::tokio::Mutex as TMutex;
#[cfg(not(feature = "locktick"))]
use parking_lot::Mutex;
#[cfg(not(feature = "locktick"))]
use parking_lot::RwLock;
use rand::seq::{IteratorRandom, SliceRandom};
use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque, hash_map},
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
    time::{Duration, Instant},
};
#[cfg(not(feature = "locktick"))]
use tokio::sync::Mutex as TMutex;
use tokio::sync::Notify;
use tracing::info;

mod helpers;
use helpers::rangify_heights;

mod sync_state;
pub use sync_state::BftSyncMode;
use sync_state::SyncState;

mod metrics;
use metrics::BlockSyncMetrics;

// The redundancy factor decreases the possibility of a malicious peers sending us an invalid block locator
// by requiring multiple peers to advertise the same (prefix of) block locators.
// However, we do not use this in production yet.
#[cfg(not(test))]
pub const REDUNDANCY_FACTOR: usize = 1;
#[cfg(test)]
pub const REDUNDANCY_FACTOR: usize = 3;

/// The time nodes wait between issuing batches of block requests to avoid triggering spam detection.
///
/// The current rate limit for all messages is around 160k  per second (see [`Gateway::max_cache_events`]).
/// This constant limits number of block requests to a much lower 100 per second.
///
// TODO(kaimast): base rate limits on how many requests were sent to each peer instead.
pub const BLOCK_REQUEST_BATCH_DELAY: Duration = Duration::from_millis(10);

const EXTRA_REDUNDANCY_FACTOR: usize = REDUNDANCY_FACTOR * 3;
const NUM_SYNC_CANDIDATE_PEERS: usize = REDUNDANCY_FACTOR * 5;

const BLOCK_REQUEST_TIMEOUT: Duration = Duration::from_secs(60);

/// The maximum number of outstanding block requests.
/// Once a node hits this limit, it will not issue any new requests until existing requests time out or receive responses.
const MAX_BLOCK_REQUESTS: usize = 50; // 50 requests

/// The maximum number of blocks tolerated before the primary is considered behind its peers.
pub const MAX_BLOCKS_BEHIND: u32 = 1; // blocks

/// This is a dummy IP address that is used to represent the local node.
/// Note: This here does not need to be a real IP address, but it must be unique/distinct from all other connections.
pub const DUMMY_SELF_IP: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 0);

/// The map of failed block requests.
type FailedRequests<H> = BTreeMap<u32, (Option<H>, Option<H>)>;

/// Handle to an outstanding requested, containing the request itself and its timestamp.
/// This does not contain the response so that checking for responses does not require iterating over all requests.
#[derive(Clone)]
struct OutstandingRequest<N: Network> {
    request: SyncRequest<N>,
    timestamp: Instant,
    /// The corresponding response (if any).
    /// This is guaranteed to be Some if sync_ips for the given request are empty.
    response: Option<Block<N>>,
}

/// Information about a block request (used for the REST API).
#[derive(Clone, serde::Serialize)]
pub struct BlockRequestInfo {
    /// Seconds since the request was created
    elapsed: u64,
    /// Has the request been responded to?
    done: bool,
}

/// Summary of completed all in-flight requests.
#[derive(Clone, serde::Serialize)]
pub struct BlockRequestsSummary {
    outstanding: String,
    completed: String,
}

#[derive(thiserror::Error, Debug)]
pub enum InsertBlockResponseError<N: Network> {
    #[error("Empty block response")]
    EmptyBlockResponse,
    #[error("The peer did not send a consensus version")]
    NoConsensusVersion,
    #[error(
        "The peer's consensus version for height {last_height} is ahead of ours: expected {expected_version}, got {peer_version}"
    )]
    ConsensusVersionAhead { peer_version: ConsensusVersion, expected_version: ConsensusVersion, last_height: u32 },
    #[error(
        "The peer's consensus version for height {last_height} is behind ours: expected {expected_version}, got {peer_version}"
    )]
    ConsensusVersionBehind { peer_version: ConsensusVersion, expected_version: ConsensusVersion, last_height: u32 },
    #[error("Block Sync already advanced to block {height}")]
    BlockSyncAlreadyAdvanced { height: u32 },
    #[error("No such request for height {height}")]
    NoSuchRequest { height: u32 },
    #[error("Invalid block hash for height {height} from '{peer_ip}': expected {expected_hash}, got {actual_hash}")]
    InvalidBlockHash { height: u32, peer_ip: SocketAddr, expected_hash: N::BlockHash, actual_hash: N::BlockHash },
    #[error(
        "The previous block hash in candidate block {height} from '{peer_ip}' is incorrect: expected {expected}, but got {actual}"
    )]
    InvalidPreviousBlockHash { height: u32, peer_ip: SocketAddr, expected: N::BlockHash, actual: N::BlockHash },
    #[error("Candidate block {height} from '{peer_ip}' is malformed")]
    MalformedBlock { height: u32, peer_ip: SocketAddr },
    #[error("The sync pool did not request block {height} from '{peer_ip}'")]
    WrongSyncPeer { height: u32, peer_ip: SocketAddr },
    #[error("{}", flatten_error(.0))]
    Other(#[from] anyhow::Error),
}

impl<N: Network> InsertBlockResponseError<N> {
    /// Returns `true` if the error does not indicate malicious or faulty behavior.
    pub fn is_benign(&self) -> bool {
        matches!(self, Self::NoSuchRequest { .. } | Self::BlockSyncAlreadyAdvanced { .. })
    }

    // Returns true if the peer's consensus version is ahead of ours.
    pub fn is_consensus_version_ahead(&self) -> bool {
        matches!(self, Self::ConsensusVersionAhead { .. })
    }

    // Returns true if the peer's consensus version is behind ours or missing.
    pub fn is_consensus_version_behind(&self) -> bool {
        matches!(self, Self::ConsensusVersionBehind { .. } | Self::NoConsensusVersion)
    }
}

impl<N: Network> OutstandingRequest<N> {
    /// Get a reference to the IPs of peers that have not responded to the request (yet).
    fn sync_ips(&self) -> &IndexSet<SocketAddr> {
        let (_, _, sync_ips) = &self.request;
        sync_ips
    }

    /// Get a mutable reference to the IPs of peers that have not responded to the request (yet).
    fn sync_ips_mut(&mut self) -> &mut IndexSet<SocketAddr> {
        let (_, _, sync_ips) = &mut self.request;
        sync_ips
    }
}

/// A struct that keeps track of synchronizing blocks with other nodes.
///
/// It generates requests to send to other peers and processes responses to those requests.
/// The struct also keeps track of block locators, which indicate which peers it can fetch blocks from.
///
/// # Notes
/// - The actual network communication happens in `snarkos_node::Client` (for clients and provers) and in `snarkos_node_bft::Sync` (for validators).
///
/// - Validators only sync from other nodes using this struct if they fall behind, e.g.,
///   because they experience a network partition.
///   In the common case, validators will generate blocks from the DAG after an anchor certificate has been approved
///   by a supermajority of the committee.
pub struct BlockSync<N: Network> {
    /// The ledger.
    ledger: Arc<dyn LedgerService<N>>,

    /// The connection mode of this node (Gateway for validators, Router for clients/provers).
    connection_mode: ConnectionMode,

    /// The map of peer IP to their block locators.
    /// The block locators are consistent with the ledger and every other peer's block locators.
    locators: RwLock<HashMap<SocketAddr, BlockLocators<N>>>,

    /// The map of peer-to-peer to their common ancestor.
    /// This map is used to determine which peers to request blocks from.
    ///
    /// Lock ordering: when locking both, `common_ancestors` and `locators`, `common_ancestors` must be locked first.
    common_ancestors: RwLock<IndexMap<PeerPair, u32>>,

    /// The block requests in progress and their responses.
    requests: RwLock<BTreeMap<u32, OutstandingRequest<N>>>,

    /// The boolean indicator of whether the node is synced up to the latest block (within the given tolerance).
    ///
    /// Lock ordering: if you lock `sync_state` and `requests`, you must lock `sync_state` first.
    sync_state: RwLock<SyncState>,

    /// The lock used to ensure that [`Self::advance_with_sync_blocks()`] is called by one task at a time.
    advance_with_sync_blocks_lock: TMutex<()>,

    /// Gets notified when there was an update to the locators or a peer disconnected.
    peer_notify: Notify,

    /// Gets notified when we received a new block response.
    response_notify: Notify,

    /// Tracks sync speed
    metrics: BlockSyncMetrics,

    /// Meta lock that ensures no new block requests are created while a peer is removed.
    prepare_requests_lock: Mutex<()>,

    /// Tracks failed requests that need to be re-issued.
    failed_requests: Mutex<FailedRequests<N::BlockHash>>,

    /// Tracks the last time each peer delivered a successful block response.
    ///
    /// Used in `handle_block_request_timeouts` to avoid banning peers that are actively
    /// responding but cannot keep up with the request rate.
    last_response_at: Mutex<HashMap<SocketAddr, Instant>>,
}

impl<N: Network> BlockSync<N> {
    /// Initializes a new block sync module.
    pub fn new(ledger: Arc<dyn LedgerService<N>>, connection_mode: ConnectionMode) -> Self {
        // Make sync state aware of the blocks that already exist on disk at startup.
        let sync_state = SyncState::new_with_height(ledger.latest_block_height());

        Self {
            ledger,
            connection_mode,
            sync_state: RwLock::new(sync_state),
            peer_notify: Default::default(),
            response_notify: Default::default(),
            locators: Default::default(),
            requests: Default::default(),
            common_ancestors: Default::default(),
            advance_with_sync_blocks_lock: Default::default(),
            metrics: Default::default(),
            prepare_requests_lock: Default::default(),
            failed_requests: Default::default(),
            last_response_at: Default::default(),
        }
    }

    /// Blocks until something about a peer changes,
    /// or block request has been fully processed (either successfully or unsuccessfully).
    ///
    /// Used by the outgoing task.
    ///
    /// # Concurrency
    /// Only one task can wait on this at a time.
    pub async fn wait_for_peer_update(&self) {
        self.peer_notify.notified().await
    }

    /// Blocks until there is a new response to a block request.
    ///
    /// Used by the incoming task.
    ///
    /// # Concurrency
    /// Only one task can wait on this at a time.
    pub async fn wait_for_block_responses(&self) {
        self.response_notify.notified().await
    }

    /// Returns `true` if the node is synced up to the latest block (within the given tolerance).
    #[inline]
    pub fn is_block_synced(&self) -> bool {
        self.sync_state.read().is_block_synced()
    }

    /// Returns the number of blocks the node is behind the greatest peer height,
    /// or `None` if no peers are connected yet.
    #[inline]
    pub fn num_blocks_behind(&self) -> Option<u32> {
        self.sync_state.read().num_blocks_behind()
    }

    /// Returns the greatest block height of any connected peer.
    #[inline]
    pub fn greatest_peer_block_height(&self) -> Option<u32> {
        self.sync_state.read().get_greatest_peer_height()
    }

    /// Returns the current sync height of this node.
    /// The sync height is always greater or equal to the ledger height.
    #[inline]
    pub fn get_sync_height(&self) -> u32 {
        self.sync_state.read().get_sync_height()
    }

    /// Returns the BFT sync mode (fast or DAG), or `None` if no BFT layer is attached.
    #[inline]
    pub fn get_bft_sync_mode(&self) -> Option<BftSyncMode> {
        self.sync_state.read().get_bft_sync_mode()
    }

    /// Sets the BFT sync mode. Should only be called by the BFT layer.
    ///
    /// # Returns
    /// The previous BFT sync mode (if any).
    #[inline]
    pub fn set_bft_sync_mode(&self, mode: BftSyncMode) -> Option<BftSyncMode> {
        self.sync_state.write().set_bft_sync_mode(mode)
    }

    /// Returns the number of blocks we requested from peers, but have not received yet.
    #[inline]
    pub fn num_outstanding_block_requests(&self) -> usize {
        self.requests.read().iter().filter(|(_, e)| !e.sync_ips().is_empty()).count()
    }

    /// The total number of block request, including the ones that have been answered already but not processed yet.
    #[inline]
    pub fn num_total_block_requests(&self) -> usize {
        self.requests.read().len()
    }

    //// Returns the latest locator height for all known peers.
    pub fn get_peer_heights(&self) -> HashMap<SocketAddr, u32> {
        self.locators.read().iter().map(|(addr, locators)| (*addr, locators.latest_locator_height())).collect()
    }

    //// Returns information about all in-flight block requests.
    pub fn get_block_requests_info(&self) -> BTreeMap<u32, BlockRequestInfo> {
        self.requests
            .read()
            .iter()
            .map(|(height, request)| {
                (*height, BlockRequestInfo {
                    done: request.sync_ips().is_empty(),
                    elapsed: request.timestamp.elapsed().as_secs(),
                })
            })
            .collect()
    }

    /// Returns a summary of all in-flight requests.
    pub fn get_block_requests_summary(&self) -> BlockRequestsSummary {
        let requests = self.requests.read();
        let completed = requests.iter().filter_map(|(h, e)| if e.sync_ips().is_empty() { Some(*h) } else { None });
        let outstanding = requests.iter().filter_map(|(h, e)| if !e.sync_ips().is_empty() { Some(*h) } else { None });

        BlockRequestsSummary { completed: rangify_heights(completed), outstanding: rangify_heights(outstanding) }
    }

    pub fn get_sync_speed(&self) -> f64 {
        self.metrics.get_sync_speed()
    }
}

// Helper functions needed for testing
#[cfg(test)]
impl<N: Network> BlockSync<N> {
    /// Returns the latest block height of the given peer IP.
    fn get_peer_height(&self, peer_ip: &SocketAddr) -> Option<u32> {
        self.locators.read().get(peer_ip).map(|locators| locators.latest_locator_height())
    }

    /// Returns the common ancestor for the given peer pair, if it exists.
    fn get_common_ancestor(&self, peer_a: SocketAddr, peer_b: SocketAddr) -> Option<u32> {
        self.common_ancestors.read().get(&PeerPair(peer_a, peer_b)).copied()
    }

    /// Returns the block request for the given height, if it exists.
    fn get_block_request(&self, height: u32) -> Option<SyncRequest<N>> {
        self.requests.read().get(&height).map(|e| e.request.clone())
    }

    /// Returns the timestamp of the last time the block was requested, if it exists.
    fn get_block_request_timestamp(&self, height: u32) -> Option<Instant> {
        self.requests.read().get(&height).map(|e| e.timestamp)
    }
}

impl<N: Network> BlockSync<N> {
    /// Returns the block locators.
    #[inline]
    pub fn get_block_locators(&self) -> Result<BlockLocators<N>> {
        // Retrieve the latest block height.
        let latest_height = self.ledger.latest_block_height();

        // Initialize the recents map.
        // TODO: generalize this for RECENT_INTERVAL > 1, or remove this comment if we hardwire that to 1
        let mut recents = IndexMap::with_capacity(NUM_RECENT_BLOCKS);
        // Retrieve the recent block hashes.
        for height in latest_height.saturating_sub((NUM_RECENT_BLOCKS - 1) as u32)..=latest_height {
            recents.insert(height, self.ledger.get_block_hash(height)?);
        }

        // Initialize the checkpoints map.
        let mut checkpoints = IndexMap::with_capacity((latest_height / CHECKPOINT_INTERVAL + 1).try_into()?);
        // Retrieve the checkpoint block hashes.
        for height in (0..=latest_height).step_by(CHECKPOINT_INTERVAL as usize) {
            checkpoints.insert(height, self.ledger.get_block_hash(height)?);
        }

        // Construct the block locators.
        BlockLocators::new(recents, checkpoints)
    }

    /// Send a batch of block requests.
    pub async fn send_block_requests<C: CommunicationService>(
        &self,
        communication: &C,
        sync_peers: &IndexMap<SocketAddr, BlockLocators<N>>,
        requests: &[(u32, PrepareSyncRequest<N>)],
    ) -> bool {
        let (start_height, max_num_sync_ips) = match requests.first() {
            Some((height, (_, _, max_num_sync_ips))) => (*height, *max_num_sync_ips),
            None => {
                warn!("Block sync failed - no block requests");
                return false;
            }
        };

        // Use a randomly sampled subset of the sync IPs.
        let sync_ips: IndexSet<_> =
            sync_peers.keys().copied().sample(&mut rand::rng(), max_num_sync_ips).into_iter().collect();

        // Calculate the end height.
        let end_height = start_height.saturating_add(requests.len() as u32);

        // A peer may have disconnected after we prepared this batch; inserting requests that
        // reference them would leave those requests never cleaned by remove_peer. Hold the same
        // lock that remove_peer uses so we're atomic: check that all selected peers are still
        // connected, then insert, without a disconnect slipping in between.
        {
            let _prepare_requests_lock = self.prepare_requests_lock.lock();
            let all_still_connected = {
                let locators = self.locators.read();
                sync_ips.iter().all(|ip| locators.contains_key(ip))
            };

            if !all_still_connected {
                trace!(
                    "Skipping block request batch for heights {start_height}-{inclusive_end}: at least one of the selected peer(s) has disconnected",
                    inclusive_end = end_height.saturating_sub(1)
                );
                return false;
            }

            // Insert the chunk of block requests (still holding lock so remove_peer cannot run).
            for (height, (hash, previous_hash, _)) in requests.iter() {
                // Insert the block request into the sync pool using the sync IPs from the last block request in the chunk.
                if let Err(err) = self.insert_block_request(*height, (*hash, *previous_hash, sync_ips.clone())) {
                    let err = err.context(format!("Failed to insert block request for height {height}"));
                    warn!("{}", flatten_error(&err));
                    return false;
                }
            }
        }

        debug!("Sending {len} block requests to peer(s) at {peers:?}", len = requests.len(), peers = sync_ips);

        /* Send the block request to the peers */

        // Construct the message.
        let message = C::prepare_block_request(start_height, end_height);

        // Send the message to the peers.
        let mut tasks = Vec::with_capacity(sync_ips.len());
        for sync_ip in sync_ips {
            let sender = communication.send(sync_ip, message.clone()).await;
            let task = tokio::spawn(async move {
                // Ensure the request is sent successfully.
                match sender {
                    Some(sender) => {
                        if let Err(err) = sender.await {
                            warn!("Failed to send block request to peer '{sync_ip}': {err}");
                            false
                        } else {
                            true
                        }
                    }
                    None => {
                        warn!("Failed to send block request to peer '{sync_ip}': no such peer");
                        false
                    }
                }
            });

            tasks.push(task);
        }

        // Wait for all sends to finish at the same time.
        for result in futures::future::join_all(tasks).await {
            let success = match result {
                Ok(success) => success,
                Err(err) => {
                    error!("tokio join error: {err}");
                    false
                }
            };

            // If sending fails for any peer, remove the block request from the sync pool.
            if !success {
                // Remove the entire block request from the sync pool.
                let mut requests = self.requests.write();
                for height in start_height..end_height {
                    requests.remove(&height);
                }
                // Break out of the loop.
                return false;
            }
        }
        true
    }

    /// Handles timeouts, checks if block sync is possible, prepares block requests,
    /// and sends them via the given [`CommunicationService`].
    ///
    /// Callers typically call this in a loop after waiting for peer updates, e.g.
    /// `timeout(MAX_SYNC_INTERVAL, self.wait_for_peer_update())`.
    pub async fn try_issuing_block_requests<C: CommunicationService>(&self, communication: &C) {
        self.handle_block_request_timeouts();

        // Clients fetch the tip even while within the validator sync tolerance.
        if self.connection_mode == ConnectionMode::Gateway && self.is_block_synced() {
            trace!("Node is already synced. Will not issue new block requests");
            return;
        }

        if !self.sync_state.read().can_issue_new_block_requests() && self.failed_requests.lock().is_empty() {
            trace!("Nothing to sync. Will not issue new block requests");
            return;
        }

        let batches = self.prepare_block_requests();

        if batches.is_empty() {
            let total_requests = self.num_total_block_requests();
            let num_outstanding = self.num_outstanding_block_requests();
            if total_requests != 0 {
                trace!(
                    "Not block synced yet, but there are still {total_requests} in-flight requests. {num_outstanding} are still awaiting responses."
                );
            } else {
                debug!(
                    "Not block synced yet, and there are no outstanding block requests or \
                 new block requests to send"
                );
            }
        } else {
            for (block_requests, sync_peers) in batches {
                for requests in block_requests.chunks(DataBlocks::<N>::MAXIMUM_NUMBER_OF_BLOCKS as usize) {
                    if !self.send_block_requests(communication, &sync_peers, requests).await {
                        break;
                    }
                    tokio::time::sleep(BLOCK_REQUEST_BATCH_DELAY).await;
                }
            }
        }
    }

    /// Inserts a new block response from the given peer IP.
    ///
    /// Returns an error if the block was malformed, or we already received a different block for this height.
    /// This function also removes all block requests from the given peer IP on failure.
    ///
    /// Note, that this only queues the response. After this, you most likely want to call `Self::try_advancing_block_synchronization`.
    ///
    #[inline]
    pub fn insert_block_responses(
        &self,
        peer_ip: SocketAddr,
        blocks: Vec<Block<N>>,
        latest_consensus_version: Option<ConsensusVersion>,
    ) -> Result<(), InsertBlockResponseError<N>> {
        // Attempt to insert the block responses, and break if we encounter an error.
        let result = 'outer: {
            let Some(last_height) = blocks.as_slice().last().map(|b| b.height()) else {
                break 'outer Err(InsertBlockResponseError::EmptyBlockResponse);
            };

            let expected_consensus_version =
                N::CONSENSUS_VERSION(last_height).map_err(InsertBlockResponseError::Other)?;

            // Perform consensus version check, if possible.
            // This check is only enabled after nodes have reached V12.
            if expected_consensus_version >= ConsensusVersion::V12 {
                if let Some(peer_version) = latest_consensus_version {
                    if peer_version != expected_consensus_version {
                        break 'outer Err(if peer_version > expected_consensus_version {
                            InsertBlockResponseError::ConsensusVersionAhead {
                                peer_version,
                                expected_version: expected_consensus_version,
                                last_height,
                            }
                        } else {
                            InsertBlockResponseError::ConsensusVersionBehind {
                                peer_version,
                                expected_version: expected_consensus_version,
                                last_height,
                            }
                        });
                    }
                } else {
                    break 'outer Err(InsertBlockResponseError::NoConsensusVersion);
                }
            }

            // Insert the candidate blocks into the sync pool.
            for block in blocks {
                if let Err(error) = self.insert_block_response(peer_ip, block) {
                    break 'outer Err(error);
                }
            }

            Ok(())
        };

        // On failure, remove all block requests to the peer.
        if result.is_err() {
            self.remove_block_requests_to_peer(&peer_ip);
        }

        // Return the result.
        result
    }

    /// Returns the next block for the given `next_height` if the request is complete,
    /// or `None` otherwise. This does not remove the block from the `responses` map.
    #[inline]
    pub fn peek_next_block(&self, next_height: u32) -> Option<Block<N>> {
        // Determine if the request is complete:
        // either there is no request for `next_height`, or the request has no peer socket addresses left.
        if let Some(entry) = self.requests.read().get(&next_height) {
            let is_complete = entry.sync_ips().is_empty();
            if !is_complete {
                return None;
            }

            // If the request is complete, return the block from the responses, if there is one.
            if entry.response.is_none() {
                warn!("Request for height {next_height} is complete but no response exists");
            }
            entry.response.clone()
        } else {
            None
        }
    }

    /// Attempts to advance synchronization by processing completed block responses.
    ///
    /// Returns true, if new blocks were added to the ledger.
    ///
    /// # Usage
    /// This is only called in [`Client::try_block_sync`] and should not be called concurrently by multiple tasks.
    /// Validators do not call this function, and instead invoke
    /// [`snarkos_node_bft::Sync::try_advancing_block_synchronization`] which also updates the BFT state.
    #[inline]
    pub async fn try_advancing_block_synchronization(&self) -> Result<bool> {
        // Acquire the lock to ensure this function is called only once at a time.
        // If the lock is already acquired, return early.
        //
        // Note: This lock should not be needed anymore as there is only one place we call it from,
        // but we keep it for now out of caution.
        // TODO(kaimast): remove this eventually.
        let Ok(_lock) = self.advance_with_sync_blocks_lock.try_lock() else {
            trace!("Skipping attempt to advance block synchronziation as it is already in progress");
            return Ok(false);
        };

        // Start with the current height.
        let mut current_height = self.ledger.latest_block_height();
        let start_height = current_height;
        trace!(
            "Try advancing with block responses (at block {current_height}, current sync speed is {})",
            self.get_sync_speed()
        );

        loop {
            let next_height = current_height + 1;

            let Some(block) = self.peek_next_block(next_height) else {
                break;
            };

            // Ensure the block height matches.
            if block.height() != next_height {
                warn!("Block height mismatch: expected {}, found {}", current_height + 1, block.height());
                break;
            }

            let ledger = self.ledger.clone();

            let (advanced, stop) = tokio::task::spawn_blocking(move || {
                let ledger_update = match ledger.begin_ledger_update() {
                    Ok(update) => update,
                    Err(BeginLedgerUpdateError::ShuttingDown) => {
                        info!("BlockSync cannot advance the ledger any more. The node is shutting down.");
                        return Ok((false, true));
                    }
                    Err(err) => {
                        return Err(anyhow!("Unexpected error when beginning ledger update: {err}"));
                    }
                };

                // Try to check the next block and advance to it.
                let block = match ledger_update.check_next_block(block) {
                    Ok(block) => block,
                    Err(CheckBlockError::InvalidHeight { .. })
                    | Err(CheckBlockError::BlockAlreadyExists { .. })
                    | Err(CheckBlockError::InvalidRound { .. }) => {
                        debug!("Skipping a block at height {next_height}. The ledger already advanced",);
                        return Ok((false, false));
                    }
                    Err(err) => {
                        warn!("{err}");
                        return Err(err.into_anyhow());
                    }
                };

                ledger_update.advance_to_next_block(&block).with_context(|| {
                    format!(
                        "Failed to advance to next block (height: {height}, hash: {hash})",
                        height = block.height(),
                        hash = block.hash(),
                    )
                })?;

                Ok((true, false))
            })
            .await??;

            // Only count successful advances.
            // We may not always advance, for example, if the block was already added to the ledger.
            if advanced {
                self.count_request_completed();
            }

            // Remove the block response.
            self.remove_block_response(next_height);

            // If the node is shutting down, exit the loop.
            if stop {
                break;
            }

            // Update the latest height.
            current_height = next_height;
        }

        if current_height > start_height {
            self.set_sync_height(current_height);
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

impl<N: Network> BlockSync<N> {
    /// Returns the sync peers with their latest heights, and their minimum common ancestor, if the node can sync.
    /// This function returns peers that are consistent with each other, and have a block height
    /// that is greater than the ledger height of this node.
    ///
    /// # Locking
    /// This will read-lock `common_ancestors` and `sync_state`, but not at the same time.
    pub fn find_sync_peers(&self) -> Option<(IndexMap<SocketAddr, u32>, u32)> {
        // Retrieve the current sync height.
        let current_height = self.get_sync_height();

        if let Some((sync_peers, min_common_ancestor)) = self.find_sync_peers_inner(current_height) {
            // Map the locators into the latest height.
            let sync_peers =
                sync_peers.into_iter().map(|(ip, locators)| (ip, locators.latest_locator_height())).collect();
            // Return the sync peers and their minimum common ancestor.
            Some((sync_peers, min_common_ancestor))
        } else {
            None
        }
    }

    /// Updates the block locators and common ancestors for the given peer IP.
    ///
    /// This function does not need to check that the block locators are well-formed,
    /// because that is already done in [`BlockLocators::new()`], as noted in [`BlockLocators`].
    ///
    /// This function does **not** check
    /// that the block locators are consistent with the peer's previous block locators or other peers' block locators.
    pub fn update_peer_locators(&self, peer_ip: SocketAddr, locators: &BlockLocators<N>) -> Result<()> {
        let connection_mode = self.connection_mode;
        // -- First, update the locators entry for the given peer IP. --
        // We perform this update atomically, and drop the lock as soon as we are done with the update.
        match self.locators.write().entry(peer_ip) {
            hash_map::Entry::Occupied(mut e) => {
                // Return early if the block locators did not change.
                if e.get() == locators {
                    return Ok(());
                }

                let old_height = e.get().latest_locator_height();
                let new_height = locators.latest_locator_height();

                if old_height > new_height {
                    debug!("Block height for peer {peer_ip} decreased from {old_height} to {new_height}",);
                }
                e.insert(locators.clone());
            }
            hash_map::Entry::Vacant(e) => {
                e.insert(locators.clone());
            }
        }

        // -- Second, compute the common ancestor with this node. --
        let new_local_ancestor = {
            let mut ancestor = 0;
            // Attention: Please do not optimize this loop, as it performs fork-detection. In addition,
            // by iterating upwards, it also early-terminates malicious block locators at the *first* point
            // of bifurcation in their ledger history, which is a critical safety guarantee provided here.
            for (height, hash) in locators.clone().into_iter() {
                if let Ok(ledger_hash) = self.ledger.get_block_hash(height) {
                    match ledger_hash == hash {
                        true => ancestor = height,
                        false => {
                            warn!(
                                "[{connection_mode}] Detected fork between this node and peer \"{peer_ip}\" at height {height}"
                            );
                            break;
                        }
                    }
                }
            }
            ancestor
        };

        // -- Third, compute the common ancestor with every other peer, and determine if this peer is forked from others. --
        // Do not hold write lock to `common_ancestors` here, because this can take a while with many peers.
        let ancestor_updates: Vec<_> = self
            .locators
            .read()
            .iter()
            .filter_map(|(other_ip, other_locators)| {
                // Skip if the other peer is the given peer.
                if other_ip == &peer_ip {
                    return None;
                }
                // Compute the common ancestor with the other peer.
                let mut ancestor = 0;
                for (height, hash) in other_locators.clone().into_iter() {
                    if let Some(expected_hash) = locators.get_hash(height) {
                        match expected_hash == hash {
                            true => ancestor = height,
                            false => {
                                debug!(
                                    "[{connection_mode}] Detected fork between peers \"{other_ip}\" and \"{peer_ip}\" at height {height}"
                                );
                                break;
                            }
                        }
                    }
                }

                Some((PeerPair(peer_ip, *other_ip), ancestor))
            })
            .collect();

        // -- Forth, update the map of common ancestors. --
        // Scope the lock, so it is dropped before locking `sync_state`.
        {
            let mut common_ancestors = self.common_ancestors.write();
            common_ancestors.insert(PeerPair(DUMMY_SELF_IP, peer_ip), new_local_ancestor);

            for (peer_pair, new_ancestor) in ancestor_updates.into_iter() {
                common_ancestors.insert(peer_pair, new_ancestor);
            }
        }

        // -- Finally, update sync state. --
        if let Some(greatest_peer_height) = self.locators.read().values().map(|l| l.latest_locator_height()).max() {
            let mut sync_state = self.sync_state.write();
            sync_state.set_greatest_peer_height(greatest_peer_height);
        } else {
            error!("Got new block locators but greatest peer height is zero.");
        }

        // Even if the greatest peer height did not change, we still received new block locators
        // that the sync loop might need to proceed.
        self.peer_notify.notify_one();

        Ok(())
    }

    /// TODO (howardwu): Remove the `common_ancestor` entry. But check that this is safe
    ///  (that we don't rely upon it for safety when we re-connect with the same peer).
    /// Removes the peer from the sync pool, if they exist.
    pub fn remove_peer(&self, peer_ip: &SocketAddr) {
        trace!("Removing peer {peer_ip} from block sync");

        // Ensure no new block requests are issued to this peer, while it is disconnecting.
        let _prepare_requests_lock = self.prepare_requests_lock.lock();

        // Remove the locators entry for the given peer IP.
        self.locators.write().remove(peer_ip);
        // Remove all common ancestor entries for this peers.
        self.common_ancestors.write().retain(|pair, _| !pair.contains(peer_ip));
        // Drop the last-response timestamp so a reconnecting peer starts fresh.
        self.last_response_at.lock().remove(peer_ip);
        // Remove all block requests to the peer.
        self.remove_block_requests_to_peer(peer_ip);

        {
            // Do not lock sync state and locators at the same time.
            let max_height = self.locators.read().values().map(|l| l.latest_locator_height()).max();
            let mut sync_state = self.sync_state.write();

            // Update sync state, because the greatest peer height may have decreased.
            if let Some(greatest_peer_height) = max_height {
                sync_state.set_greatest_peer_height(greatest_peer_height);
            } else {
                // There are no more peers left.
                sync_state.clear_greatest_peer_height();
            }
        }

        // Notify the sync loop that something changed.
        self.peer_notify.notify_one();
    }
}

// Helper type for prepare_block_requests
pub type BlockRequestBatch<N> = (Vec<(u32, PrepareSyncRequest<N>)>, IndexMap<SocketAddr, BlockLocators<N>>);

impl<N: Network> BlockSync<N> {
    /// Returns a list of block requests and the sync peers, if the node needs to sync.
    ///
    /// You usually want to call `remove_timed_out_block_requests` before invoking this function.
    ///
    /// # Returns
    /// * An empty vector, if there is no work to be done.
    /// * Otherwise, a vector of block request batches, each with a contiguous range of heights.
    ///
    /// # Concurrency
    /// This should be called by at most one task at a time.
    ///
    /// # Usage
    ///  - For validators, the primary spawns exactly one task that periodically calls
    ///    `bft::Sync::try_issuing_block_requests`. There is no possibility of concurrent calls to it.
    ///  - For clients, `Client::initialize_sync` spawn exactly one task that periodically calls
    ///    `Client::try_issuing_block_requests` which calls this function.
    ///  - Provers do not call this function.
    pub fn prepare_block_requests(&self) -> Vec<BlockRequestBatch<N>> {
        let _block_requests_lock = self.prepare_requests_lock.lock();

        // Used to print more information when we max out on requests.
        let print_requests = || {
            if tracing::enabled!(tracing::Level::TRACE) {
                let summary = self.get_block_requests_summary();

                if summary.completed.is_empty() {
                    trace!("There are no completed requests that have not been processed yet.");
                } else {
                    trace!("The following requests are complete but not processed yet: {:?}", summary.completed);
                }

                if summary.outstanding.is_empty() {
                    trace!("There are no outstanding requests.");
                } else {
                    trace!("The following requests are still outstanding: {:?}", summary.outstanding);
                }
            }
        };

        // Do not hold lock here as, currently, `find_sync_peers_inner` can take a while.
        let current_height = self.get_sync_height();

        // Determine if there are any failed requests that need to be re-issued.
        //
        // The entries are only removed once the requests are successfully re-issued.
        let mut failed_requests = self.failed_requests.lock();

        // Ensure none of the failed requests are obsolete.
        while let Some(height) = failed_requests.keys().next()
            && *height <= current_height
        {
            failed_requests.pop_first();
        }

        // Re-issue the remaining failed requests.
        if !failed_requests.is_empty() {
            trace!("There are {} failed requests that need to be re-issued.", failed_requests.len());

            // Convert the set of failed requests into one or multiple continuous ranges.
            let iter = failed_requests.iter();
            let mut batches: VecDeque<Vec<(u32, _, _)>> = VecDeque::new();

            for (height, (hash, previous_hash)) in iter {
                if let Some(prev_batch) = batches.back_mut() {
                    if let Some((last_height, _, _)) = prev_batch.last()
                        && *last_height + 1 != *height
                    {
                        // We need to start a new batch.
                        batches.push_back(vec![(*height, *hash, *previous_hash)]);
                    } else {
                        // We can add the request to the current batch.
                        prev_batch.push((*height, *hash, *previous_hash));
                    }
                } else {
                    // First batch.
                    batches.push_back(vec![(*height, *hash, *previous_hash)]);
                }
            }

            let mut result = vec![];
            while let Some(batch) = batches.pop_front() {
                // SAFETY: Batches are guaranteed to be non-empty.
                let start_height = batch.first().unwrap().0;
                let end_height = batch.last().unwrap().0 + 1;

                // Set the maximum number of blocks, so that they do not exceed the end height.
                let max_new_blocks_to_request = end_height - start_height;

                let Some((sync_peers, min_common_ancestor)) = self.find_sync_peers_inner(start_height) else {
                    // This generally shouldn't happen, because there cannot be outstanding requests when no peers are connected.
                    warn!("Cannot re-request blocks because no or not enough peers are connected");
                    return result;
                };

                // Retrieve the greatest block height of any connected peer.
                let Some(greatest_peer_height) = sync_peers.values().map(|l| l.latest_locator_height()).max() else {
                    // This should never happen because `sync_peers` is guaranteed to be non-empty.
                    error!(
                        "Cannot re-request blocks because no or not enough peers with sufficient height are connected"
                    );
                    return result;
                };

                // (Try to) construct the requests.
                let requests = self.construct_requests(
                    &sync_peers,
                    start_height.saturating_sub(1),
                    min_common_ancestor,
                    max_new_blocks_to_request,
                    greatest_peer_height,
                );

                // Only remove from failed_requests the heights we actually re-issued.
                // (If construct_requests returned empty we must not drop these failed requests.)
                for (height, _) in &requests {
                    failed_requests.remove(height);
                }

                result.push((requests, sync_peers));
            }

            return result;
        }

        // Ensure to not exceed the maximum number of outstanding block requests.
        let max_outstanding_block_requests =
            (MAX_BLOCK_REQUESTS as u32) * (DataBlocks::<N>::MAXIMUM_NUMBER_OF_BLOCKS as u32);

        // Ensure there is a finite bound on the number of block respnoses we receive, that have not been processed yet.
        let max_total_requests = 4 * max_outstanding_block_requests;

        let max_new_blocks_to_request =
            max_outstanding_block_requests.saturating_sub(self.num_outstanding_block_requests() as u32);

        // Prepare the block requests and sync peers, or returns an empty result if there is nothing to request.
        if self.num_total_block_requests() >= max_total_requests as usize {
            trace!(
                "We are already requested at least {max_total_requests} blocks that have not been fully processed yet. Will not issue more."
            );

            print_requests();
            vec![]
        } else if max_new_blocks_to_request == 0 {
            trace!(
                "Already reached the maximum number of outstanding blocks ({max_outstanding_block_requests}). Will not issue more."
            );

            print_requests();
            vec![]
        } else if let Some((sync_peers, min_common_ancestor)) = self.find_sync_peers_inner(current_height) {
            // Retrieve the greatest block height of any connected peer.
            // We do not need to update the sync state here, as that already happens when the block locators are received.
            let greatest_peer_height = sync_peers.values().map(|l| l.latest_locator_height()).max().unwrap_or(0);

            // Construct the list of block requests.
            let requests = self.construct_requests(
                &sync_peers,
                current_height,
                min_common_ancestor,
                max_new_blocks_to_request,
                greatest_peer_height,
            );

            if !requests.is_empty() {
                trace!(
                    "Generated new block requests for the following heights: {}",
                    rangify_heights(requests.iter().map(|(h, _)| *h))
                );
            }

            vec![(requests, sync_peers)]
        } else if self.requests.read().is_empty() {
            // This can happen during a race condition where the node just finished syncing.
            // It does not make sense to log or change the sync status here.
            // Checking the sync status here also does not make sense, as the node might as well have switched back
            //  from `synced` to `syncing` between calling `find_sync_peers_inner` and this line.

            vec![]
        } else {
            // This happens if we already requested all advertised blocks.
            trace!("No new blocks can be requested, but there are still outstanding requests.");

            print_requests();
            vec![]
        }
    }

    /// Should only be called by validators when they successfully process a block request.
    /// (for other nodes this will be automatically called internally)
    ///
    /// TODO(kaimast): remove this public function once the sync logic is fully unified `BlockSync`.
    pub fn count_request_completed(&self) {
        self.metrics.count_request_completed();
    }

    /// Set the sync height to a the given value.
    /// This is a no-op if `new_height` is equal or less to the current sync height.
    pub fn set_sync_height(&self, new_height: u32) {
        // Scope state lock to avoid locking state and metrics at the same time.
        let fully_synced = {
            let mut state = self.sync_state.write();
            state.set_sync_height(new_height);
            !state.can_issue_new_block_requests()
        };

        if fully_synced {
            self.metrics.mark_fully_synced();
        }
    }

    /// Inserts a block request for the given height.
    ///
    /// With a single task issuing block requests, a height should not already be in the requests
    /// map: heights in `failed_requests` were removed from `requests` when added there, and
    /// `construct_requests` skips heights already in `requests`. If this returns "already in
    /// requests map", logging the existing entry (e.g. has response? peers still in locators?)
    /// may help diagnose why the height was included in the batch.
    fn insert_block_request(&self, height: u32, (hash, previous_hash, sync_ips): SyncRequest<N>) -> Result<()> {
        // Ensure the block request does not already exist.
        self.check_block_request(height)?;
        // Ensure the sync IPs are not empty.
        ensure!(!sync_ips.is_empty(), "Cannot insert a block request with no sync IPs");
        // Insert the block request.
        self.requests.write().insert(height, OutstandingRequest {
            request: (hash, previous_hash, sync_ips),
            timestamp: Instant::now(),
            response: None,
        });
        Ok(())
    }

    /// Inserts the given block response, after checking that the request exists and the response is well-formed.
    /// On success, this function removes the peer IP from the request sync peers and inserts the response.
    fn insert_block_response(&self, peer_ip: SocketAddr, block: Block<N>) -> Result<(), InsertBlockResponseError<N>> {
        // Retrieve the block height.
        let height = block.height();
        let mut requests = self.requests.write();

        if self.ledger.contains_block_height(height) {
            return Err(InsertBlockResponseError::BlockSyncAlreadyAdvanced { height });
        }

        let Some(entry) = requests.get_mut(&height) else {
            return Err(InsertBlockResponseError::NoSuchRequest { height });
        };

        // Retrieve the request entry for the candidate block.
        let (expected_hash, expected_previous_hash, sync_ips) = &entry.request;

        // Ensure the candidate block hash matches the expected hash.
        if let Some(expected_hash) = expected_hash
            && block.hash() != *expected_hash
        {
            return Err(InsertBlockResponseError::InvalidBlockHash {
                height,
                peer_ip,
                expected_hash: *expected_hash,
                actual_hash: block.hash(),
            });
        }
        // Ensure the previous block hash matches if it exists.
        if let Some(expected_previous_hash) = expected_previous_hash
            && block.previous_hash() != *expected_previous_hash
        {
            return Err(InsertBlockResponseError::InvalidPreviousBlockHash {
                height,
                peer_ip,
                expected: *expected_previous_hash,
                actual: block.previous_hash(),
            });
        }
        // Ensure the sync pool requested this block from the given peer.
        if !sync_ips.contains(&peer_ip) {
            return Err(InsertBlockResponseError::WrongSyncPeer { height, peer_ip });
        }

        // Remove the peer IP from the request entry.
        entry.sync_ips_mut().swap_remove(&peer_ip);

        if let Some(existing_block) = &entry.response {
            // If the candidate block was already present, ensure it is the same block.
            if block != *existing_block {
                return Err(InsertBlockResponseError::MalformedBlock { height, peer_ip });
            }
        } else {
            entry.response = Some(block.clone());
        }

        trace!("Received a new and valid block response for height {height}");

        // Record that this peer is actively responding. Used by `handle_block_request_timeouts`
        // to avoid banning peers that are slow but making progress.
        self.last_response_at.lock().insert(peer_ip, Instant::now());

        // Notify the sync loop that something changed.
        self.response_notify.notify_one();

        Ok(())
    }

    /// Checks that a block request for the given height does not already exist.
    fn check_block_request(&self, height: u32) -> Result<()> {
        // Ensure the block height is not already in the ledger.
        if self.ledger.contains_block_height(height) {
            bail!("Failed to add block request, as block {height} exists in the ledger");
        }
        // Ensure the block height is not already requested.
        if self.requests.read().contains_key(&height) {
            bail!("Failed to add block request, as block {height} exists in the requests map");
        }

        Ok(())
    }

    /// Removes the block request and response for the given height
    /// This may only be called after `peek_next_block`, which checked if the request for the given height was complete.
    ///
    /// Precondition: This may only be called after `peek_next_block` has returned `Some`,
    /// which has checked if the request for the given height is complete
    /// and there is a block with the given `height` in the `responses` map.
    pub fn remove_block_response(&self, height: u32) {
        // Remove the request entry for the given height.
        if let Some(e) = self.requests.write().remove(&height) {
            trace!(
                "Block request for height {height} was completed in {}ms (sync speed is {})",
                e.timestamp.elapsed().as_millis(),
                self.get_sync_speed()
            );

            // Notify the sending task that less requests are in-flight.
            self.peer_notify.notify_one();
        }
    }

    /// Removes all block requests for the given peer IP.
    ///
    /// This is used when disconnecting from a peer or when a peer sends invalid block responses.
    fn remove_block_requests_to_peer(&self, peer_ip: &SocketAddr) {
        trace!("Block sync is removing all block requests to peer {peer_ip}...");
        let mut heights = vec![];
        let mut removed_requests = vec![];

        // Remove the peer IP from the requests map. If any request entry is now empty,
        // and its corresponding response entry is also empty, then remove that request entry altogether.
        self.requests.write().retain(|height, e| {
            let had_peer = e.sync_ips_mut().swap_remove(peer_ip);

            if had_peer && e.response.is_none() {
                trace!("Removed outstanding block request to peer {peer_ip} at height {height}");
                heights.push(*height);
            }

            // Only remove requests that were sent to this peer, that have no other peer that can respond instead,
            // and that were not completed yet.
            let retain = !had_peer || !e.sync_ips().is_empty() || e.response.is_some();
            if !retain {
                // Record the request to be re-issued.
                let (hash, previous_hash, _) = &e.request;
                removed_requests.push((*height, (*hash, *previous_hash)));
            }
            retain
        });

        if !heights.is_empty() {
            debug!(
                "Removed outstanding block requests to disconnecting peer '{peer_ip}' at heights: {}. {} were fully removed.",
                rangify_heights(heights),
                removed_requests.len(),
            );
        }

        // Mark all requests that were removed as failed.
        if !removed_requests.is_empty() {
            let mut failed_requests = self.failed_requests.lock();
            for (height, e) in removed_requests.into_iter() {
                let prev = failed_requests.insert(height, e);
                if prev.is_some() {
                    warn!(
                        "Failed to mark block request at height {height} as failed, as it already exists in the failed requests map"
                    );
                }
            }
        }

        // No need to remove responses here, because requests with responses will be retained.
    }

    /// Removes block requests that have timed out, i.e, requests we sent that did not receive a response in time.
    ///
    /// Timed-out requests will be marked as "failed" and re-issued on the next call to `prepare_block_requests`.
    pub fn handle_block_request_timeouts(&self) {
        // Snapshot last-response times before locking `requests`. A request whose assigned peer
        // has responded within `BLOCK_REQUEST_TIMEOUT` is not timed out, even if its own timer
        // has elapsed — the peer is keeping up with a backlog and timing this request out would
        // just churn it through `failed_requests` and lose its place in the queue.
        let responsive_peers: HashSet<SocketAddr> = {
            let last_response_at = self.last_response_at.lock();
            let now = Instant::now();
            last_response_at
                .iter()
                .filter_map(|(peer, t)| (now.duration_since(*t) <= BLOCK_REQUEST_TIMEOUT).then_some(*peer))
                .collect()
        };

        // Avoid locking `locators` and `requests` at the same time.
        let (timed_out_requests, peers_to_ban) = {
            // Acquire the write lock on the requests map.
            let mut requests = self.requests.write();

            // Retrieve the current time.
            let now = Instant::now();

            // Retrieve the current block height
            let current_height = self.ledger.latest_block_height();

            // Track the number of timed out block requests (only used to print a log message).
            let mut timed_out_requests = vec![];

            // Track which peers should be banned due to unresponsiveness.
            let mut peers_to_ban: HashSet<SocketAddr> = HashSet::new();

            // Remove timed out block requests.
            requests.retain(|height, e| {
                let is_obsolete = *height <= current_height;
                // Determine if the duration since the request timestamp has exceeded the request timeout.
                let timer_elapsed = now.duration_since(e.timestamp) > BLOCK_REQUEST_TIMEOUT;
                // Determine if the request is complete.
                let is_complete = e.sync_ips().is_empty() && e.response.is_some();
                // If any assigned peer is still actively responding, the request is not stuck.
                let has_responsive_peer = e.sync_ips().iter().any(|ip| responsive_peers.contains(ip));

                // Determine if the request has timed out.
                let is_timeout = timer_elapsed && !is_complete && !has_responsive_peer;

                // Retain if this is not a timeout and is not obsolete.
                let retain = !is_timeout && !is_obsolete;

                if is_timeout {
                    trace!("Block request at height {height} has timed out: timer_elapsed={timer_elapsed}, is_complete={is_complete}, is_obsolete={is_obsolete}");

                    // Increment the number of timed out block requests.
                    let (hash, previous_hash, _) = &e.request;
                    timed_out_requests.push((*height, (*hash, *previous_hash)));
                } else if is_obsolete {
                    trace!("Block request at height {height} became obsolete (current_height={current_height})");
                }

                // If the request timed out, also remove and ban given peer.
                if is_timeout {
                    for peer_ip in e.sync_ips().iter() {
                        peers_to_ban.insert(*peer_ip);
                    }
                }

                retain
            });

            if !timed_out_requests.is_empty() {
                debug!(
                    "{num} block requests timed out: {list}",
                    num = timed_out_requests.len(),
                    list = rangify_heights(timed_out_requests.iter().map(|(height, _)| *height))
                );
            }

            (timed_out_requests, peers_to_ban)
        };

        // Mark the non-obsolete requests that timed out as failed.
        if !timed_out_requests.is_empty() {
            let mut failed_requests = self.failed_requests.lock();
            for (height, e) in timed_out_requests.into_iter() {
                let prev = failed_requests.insert(height, e);
                if prev.is_some() {
                    warn!(
                        "Failed to mark block request at height {height} as failed, as it already exists in the failed requests map"
                    );
                }
            }
        }

        // Remove and ban the unresponsive peers. The `has_responsive_peer` check inside `retain`
        // above already guarantees that a request only counts as timed out when none of its
        // assigned peers have responded recently, so every peer in this set is unresponsive.
        for peer_ip in peers_to_ban {
            self.remove_peer(&peer_ip);
            // TODO: Uncomment this when we have a more rigorous analysis and testing of peer banning.
            // peer_pool_handler.ip_ban_peer(peer_ip, Some("timed out on block requests"));
        }
    }

    /// Finds the peers to sync from and the shared common ancestor, starting at the give height.
    ///
    /// Unlike [`Self::find_sync_peers`] this does not only return the latest locators height, but the full BlockLocators for each peer.
    /// Returns `None` if there are no peers to sync from.
    ///
    /// # Locking
    /// This function will read-lock `common_ancestors`.
    fn find_sync_peers_inner(&self, current_height: u32) -> Option<(IndexMap<SocketAddr, BlockLocators<N>>, u32)> {
        // Retrieve the latest ledger height.
        let latest_ledger_height = self.ledger.latest_block_height();

        // Pick a set of peers above the latest ledger height, and include their locators.
        // This will sort the peers by locator height in descending order.
        let candidate_locators: IndexMap<_, _> = self
            .locators
            .read()
            .iter()
            .filter(|(_, locators)| locators.latest_locator_height() > current_height)
            .sorted_by(|(_, a), (_, b)| b.latest_locator_height().cmp(&a.latest_locator_height()))
            .take(NUM_SYNC_CANDIDATE_PEERS)
            .map(|(peer_ip, locators)| (*peer_ip, locators.clone()))
            .collect();

        // Case 0: If there are no candidate peers, return `None`.
        if candidate_locators.is_empty() {
            trace!("Found no sync peers with height greater {current_height}");
            return None;
        }

        // TODO (howardwu): Change this to the highest cumulative weight for Phase 3.
        // Case 1: If all of the candidate peers share a common ancestor below the latest ledger height,
        // then pick the peer with the highest height, and find peers (up to extra redundancy) with
        // a common ancestor above the block request range. Set the end height to their common ancestor.

        // Determine the threshold number of peers to sync from.
        let threshold_to_request = candidate_locators.len().min(REDUNDANCY_FACTOR);

        // Breaks the loop when the first threshold number of peers are found, biasing for the peer with the highest height
        // and a cohort of peers who share a common ancestor above this node's latest ledger height.
        for (idx, (peer_ip, peer_locators)) in candidate_locators.iter().enumerate() {
            // The height of the common ancestor shared by all selected peers.
            let mut min_common_ancestor = peer_locators.latest_locator_height();

            // The peers we will synchronize from.
            // As the previous iteration did not succeed, restart with the next candidate peers.
            let mut sync_peers = vec![(*peer_ip, peer_locators.clone())];

            // Try adding other peers consistent with this one to the sync peer set.
            for (other_ip, other_locators) in candidate_locators.iter().skip(idx + 1) {
                // Check if these two peers have a common ancestor above the latest ledger height.
                if let Some(common_ancestor) = self.common_ancestors.read().get(&PeerPair(*peer_ip, *other_ip)) {
                    // If so, then check that their block locators are consistent.
                    if *common_ancestor > latest_ledger_height && peer_locators.is_consistent_with(other_locators) {
                        // If their common ancestor is less than the minimum common ancestor, then update it.
                        min_common_ancestor = min_common_ancestor.min(*common_ancestor);

                        // Add the other peer to the list of sync peers.
                        sync_peers.push((*other_ip, other_locators.clone()));
                    }
                }
            }

            // If we have enough sync peers above the latest ledger height, finish and return them.
            if min_common_ancestor > latest_ledger_height && sync_peers.len() >= threshold_to_request {
                // Shuffle the sync peers prior to returning. This ensures the rest of the stack
                // does not rely on the order of the sync peers, and that the sync peers are not biased.
                sync_peers.shuffle(&mut rand::rng());

                // Collect into an IndexMap and return.
                return Some((sync_peers.into_iter().collect(), min_common_ancestor));
            }
        }

        // If there is not enough peers with a minimum common ancestor above the latest ledger height, return None.
        None
    }

    /// Given the sync peers and their minimum common ancestor, return a list of block requests.
    ///
    /// # Returns
    /// The list of block requests, ordered by height.
    fn construct_requests(
        &self,
        sync_peers: &IndexMap<SocketAddr, BlockLocators<N>>,
        sync_height: u32,
        min_common_ancestor: u32,
        max_blocks_to_request: u32,
        greatest_peer_height: u32,
    ) -> Vec<(u32, PrepareSyncRequest<N>)> {
        // Compute the start height for the block requests.
        let start_height = {
            let requests = self.requests.read();
            let ledger_height = self.ledger.latest_block_height();

            // Do not issue requests for blocks already contained in the ledger.
            let mut start_height = ledger_height.max(sync_height + 1);

            // Do not issue requests that already exist.
            while requests.contains_key(&start_height) {
                start_height += 1;
            }

            start_height
        };

        // If the minimum common ancestor is below the start height, then return early.
        if min_common_ancestor < start_height {
            if start_height < greatest_peer_height {
                trace!(
                    "No request to construct. Height for the next block request is {start_height}, but minimum common block locator ancestor is only {min_common_ancestor} (sync_height={sync_height} greatest_peer_height={greatest_peer_height})"
                );
            }
            return Default::default();
        }

        // Compute the end height for the block request.
        let end_height = (min_common_ancestor + 1).min(start_height + max_blocks_to_request);

        // Construct the block hashes to request.
        let mut request_hashes = IndexMap::with_capacity((start_height..end_height).len());
        // Track the largest number of sync IPs required for any block request in the sequence of requests.
        let mut max_num_sync_ips = 1;

        for height in start_height..end_height {
            // Ensure the current height is not in the ledger or already requested.
            if self.check_block_request(height).is_err() {
                // If the sequence of block requests is interrupted, then return early.
                // Otherwise, continue until the first start height that is new.
                match request_hashes.is_empty() {
                    true => continue,
                    false => break,
                }
            }

            // Construct the block request.
            let (hash, previous_hash, num_sync_ips, is_honest) = construct_request(height, sync_peers);

            // Handle the dishonest case.
            if !is_honest {
                // TODO (howardwu): Consider performing an integrity check on peers (to disconnect).
                warn!("Detected dishonest peer(s) when preparing block request");
                // If there are not enough peers in the dishonest case, then return early.
                if sync_peers.len() < num_sync_ips {
                    break;
                }
            }

            // Update the maximum number of sync IPs.
            max_num_sync_ips = max_num_sync_ips.max(num_sync_ips);

            // Append the request.
            request_hashes.insert(height, (hash, previous_hash));
        }

        // Construct the requests with the same sync ips.
        request_hashes
            .into_iter()
            .map(|(height, (hash, previous_hash))| (height, (hash, previous_hash, max_num_sync_ips)))
            .collect()
    }
}

/// If any peer is detected to be dishonest in this function, it will not set the hash or previous hash,
/// in order to allow the caller to determine what to do.
fn construct_request<N: Network>(
    height: u32,
    sync_peers: &IndexMap<SocketAddr, BlockLocators<N>>,
) -> (Option<N::BlockHash>, Option<N::BlockHash>, usize, bool) {
    let mut hash = None;
    let mut hash_redundancy: usize = 0;
    let mut previous_hash = None;
    let mut is_honest = true;

    for peer_locators in sync_peers.values() {
        if let Some(candidate_hash) = peer_locators.get_hash(height) {
            match hash {
                // Increment the redundancy count if the hash matches.
                Some(hash) if hash == candidate_hash => hash_redundancy += 1,
                // Some peer is dishonest.
                Some(_) => {
                    hash = None;
                    hash_redundancy = 0;
                    previous_hash = None;
                    is_honest = false;
                    break;
                }
                // Set the hash if it is not set.
                None => {
                    hash = Some(candidate_hash);
                    hash_redundancy = 1;
                }
            }
        }
        if let Some(candidate_previous_hash) = peer_locators.get_hash(height.saturating_sub(1)) {
            match previous_hash {
                // Increment the redundancy count if the previous hash matches.
                Some(previous_hash) if previous_hash == candidate_previous_hash => (),
                // Some peer is dishonest.
                Some(_) => {
                    hash = None;
                    hash_redundancy = 0;
                    previous_hash = None;
                    is_honest = false;
                    break;
                }
                // Set the previous hash if it is not set.
                None => previous_hash = Some(candidate_previous_hash),
            }
        }
    }

    // Note that we intentionally do not just pick the peers that have the hash we have chosen,
    // to give stronger confidence that we are syncing during times when the network is consistent/stable.
    let num_sync_ips = {
        // Extra redundant peers - as the block hash was dishonest.
        if !is_honest {
            // Choose up to the extra redundancy factor in sync peers.
            EXTRA_REDUNDANCY_FACTOR
        }
        // No redundant peers - as we have redundancy on the block hash.
        else if hash.is_some() && hash_redundancy >= REDUNDANCY_FACTOR {
            // Choose one sync peer.
            1
        }
        // Redundant peers - as we do not have redundancy on the block hash.
        else {
            // Choose up to the redundancy factor in sync peers.
            REDUNDANCY_FACTOR
        }
    };

    (hash, previous_hash, num_sync_ips, is_honest)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::locators::{
        CHECKPOINT_INTERVAL,
        NUM_RECENT_BLOCKS,
        test_helpers::{sample_block_locators, sample_block_locators_with_fork},
    };

    use snarkos_node_bft_ledger_service::MockLedgerService;
    use snarkvm::{
        ledger::committee::Committee,
        prelude::{Field, TestRng},
    };

    use indexmap::{IndexSet, indexset};
    #[cfg(feature = "locktick")]
    use locktick::parking_lot::RwLock;
    #[cfg(not(feature = "locktick"))]
    use parking_lot::RwLock;
    use rand::RngExt;
    use std::{
        collections::BTreeSet,
        net::{IpAddr, Ipv4Addr},
    };

    type CurrentNetwork = snarkvm::prelude::MainnetV0;

    /// Returns the peer IP for the sync pool.
    fn sample_peer_ip(id: u16) -> SocketAddr {
        assert_ne!(id, 0, "The peer ID must not be 0 (reserved for local IP in testing)");
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), id)
    }

    /// Returns a sample committee.
    fn sample_committee() -> Committee<CurrentNetwork> {
        let rng = &mut TestRng::default();
        snarkvm::ledger::committee::test_helpers::sample_committee(rng)
    }

    /// Returns the ledger service, initialized to the given height.
    fn sample_ledger_service(height: u32) -> MockLedgerService<CurrentNetwork> {
        MockLedgerService::new_at_height(sample_committee(), height)
    }

    /// Returns the sync pool, with the ledger initialized to the given height.
    fn sample_sync_at_height(height: u32) -> BlockSync<CurrentNetwork> {
        BlockSync::<CurrentNetwork>::new(Arc::new(sample_ledger_service(height)), ConnectionMode::Router)
    }

    /// Returns a vector of randomly sampled block heights in [0, max_height].
    ///
    /// The maximum value will always be included in the result.
    fn generate_block_heights(max_height: u32, num_values: usize) -> Vec<u32> {
        assert!(num_values > 0, "Cannot generate an empty vector");
        assert!((max_height as usize) >= num_values);

        let mut rng = TestRng::default();

        let mut heights: Vec<u32> = (0..(max_height - 1)).sample(&mut rng, num_values);

        heights.push(max_height);

        heights
    }

    /// Returns a duplicate (deep copy) of the sync pool with a different ledger height.
    fn duplicate_sync_at_new_height(sync: &BlockSync<CurrentNetwork>, height: u32) -> BlockSync<CurrentNetwork> {
        BlockSync::<CurrentNetwork> {
            failed_requests: Default::default(),
            peer_notify: Notify::new(),
            response_notify: Default::default(),
            ledger: Arc::new(sample_ledger_service(height)),
            connection_mode: sync.connection_mode,
            locators: RwLock::new(sync.locators.read().clone()),
            common_ancestors: RwLock::new(sync.common_ancestors.read().clone()),
            requests: RwLock::new(sync.requests.read().clone()),
            sync_state: RwLock::new(sync.sync_state.read().clone()),
            advance_with_sync_blocks_lock: Default::default(),
            metrics: Default::default(),
            prepare_requests_lock: Default::default(),
            last_response_at: Default::default(),
        }
    }

    /// Checks that the sync pool (starting at genesis) returns the correct requests.
    fn check_prepare_block_requests(
        sync: BlockSync<CurrentNetwork>,
        min_common_ancestor: u32,
        peers: IndexSet<SocketAddr>,
    ) {
        let rng = &mut TestRng::default();

        // Check test assumptions are met.
        assert_eq!(sync.ledger.latest_block_height(), 0, "This test assumes the sync pool is at genesis");

        // Determine the number of peers within range of this sync pool.
        let num_peers_within_recent_range_of_ledger = {
            // If no peers are within range, then set to 0.
            if min_common_ancestor >= NUM_RECENT_BLOCKS as u32 {
                0
            }
            // Otherwise, manually check the number of peers within range.
            else {
                peers.iter().filter(|peer_ip| sync.get_peer_height(peer_ip).unwrap() < NUM_RECENT_BLOCKS as u32).count()
            }
        };

        // Prepare the block requests.
        let mut batches = sync.prepare_block_requests();

        // If there are no peers, then there should be no requests.
        if peers.is_empty() {
            assert!(batches.is_empty());
            return;
        }

        let (requests, sync_peers) = batches.pop().unwrap();

        // Otherwise, there should be requests.
        let expected_num_requests = core::cmp::min(min_common_ancestor as usize, MAX_BLOCK_REQUESTS);
        assert_eq!(requests.len(), expected_num_requests);

        for (idx, (height, (hash, previous_hash, num_sync_ips))) in requests.into_iter().enumerate() {
            // Construct the sync IPs.
            let sync_ips: IndexSet<_> = sync_peers.keys().sample(rng, num_sync_ips).into_iter().copied().collect();
            assert_eq!(height, 1 + idx as u32);
            assert_eq!(hash, Some((Field::<CurrentNetwork>::from_u32(height)).into()));
            assert_eq!(previous_hash, Some((Field::<CurrentNetwork>::from_u32(height - 1)).into()));

            if num_peers_within_recent_range_of_ledger >= REDUNDANCY_FACTOR {
                assert_eq!(sync_ips.len(), 1);
            } else {
                assert_eq!(sync_ips.len(), num_peers_within_recent_range_of_ledger);
                assert_eq!(sync_ips, peers);
            }
        }
    }

    /// Tests that height and hash values are set correctly using many different maximum block heights.
    #[test]
    fn test_latest_block_height() {
        for height in generate_block_heights(100_001, 5000) {
            let sync = sample_sync_at_height(height);
            // Check that the latest block height is the maximum height.
            assert_eq!(sync.ledger.latest_block_height(), height);

            // Check the hash to height mapping
            assert_eq!(sync.ledger.get_block_height(&(Field::<CurrentNetwork>::from_u32(0)).into()).unwrap(), 0);
            assert_eq!(
                sync.ledger.get_block_height(&(Field::<CurrentNetwork>::from_u32(height)).into()).unwrap(),
                height
            );
        }
    }

    #[test]
    fn test_get_block_hash() {
        for height in generate_block_heights(100_001, 5000) {
            let sync = sample_sync_at_height(height);

            // Check the height to hash mapping
            assert_eq!(sync.ledger.get_block_hash(0).unwrap(), (Field::<CurrentNetwork>::from_u32(0)).into());
            assert_eq!(sync.ledger.get_block_hash(height).unwrap(), (Field::<CurrentNetwork>::from_u32(height)).into());
        }
    }

    #[test]
    fn test_prepare_block_requests() {
        for num_peers in 0..111 {
            println!("Testing with {num_peers} peers");

            let sync = sample_sync_at_height(0);

            let mut peers = indexset![];

            for peer_id in 1..=num_peers {
                // Add a peer.
                sync.update_peer_locators(sample_peer_ip(peer_id), &sample_block_locators(10)).unwrap();
                // Add the peer to the set of peers.
                peers.insert(sample_peer_ip(peer_id));
            }

            // If all peers are ahead, then requests should be prepared.
            check_prepare_block_requests(sync, 10, peers);
        }
    }

    #[test]
    fn test_prepare_block_requests_with_leading_fork_at_11() {
        let sync = sample_sync_at_height(0);

        // Intuitively, peer 1's fork is above peer 2 and peer 3's height.
        // So from peer 2 and peer 3's perspective, they don't even realize that peer 1 is on a fork.
        // Thus, you can sync up to block 10 from any of the 3 peers.

        // When there are NUM_REDUNDANCY peers ahead, and 1 peer is on a leading fork at 11,
        // then the sync pool should request blocks 1..=10 from the NUM_REDUNDANCY peers.
        // This is safe because the leading fork is at 11, and the sync pool is at 0,
        // so all candidate peers are at least 10 blocks ahead of the sync pool.

        // Add a peer (fork).
        let peer_1 = sample_peer_ip(1);
        sync.update_peer_locators(peer_1, &sample_block_locators_with_fork(20, 11)).unwrap();

        // Add a peer.
        let peer_2 = sample_peer_ip(2);
        sync.update_peer_locators(peer_2, &sample_block_locators(10)).unwrap();

        // Add a peer.
        let peer_3 = sample_peer_ip(3);
        sync.update_peer_locators(peer_3, &sample_block_locators(10)).unwrap();

        // Prepare the block requests.
        let (requests, _) = sync.prepare_block_requests().pop().unwrap();
        assert_eq!(requests.len(), 10);

        // Check the requests.
        for (idx, (height, (hash, previous_hash, num_sync_ips))) in requests.into_iter().enumerate() {
            assert_eq!(height, 1 + idx as u32);
            assert_eq!(hash, Some((Field::<CurrentNetwork>::from_u32(height)).into()));
            assert_eq!(previous_hash, Some((Field::<CurrentNetwork>::from_u32(height - 1)).into()));
            assert_eq!(num_sync_ips, 1); // Only 1 needed since we have redundancy factor on this (recent locator) hash.
        }
    }

    #[test]
    fn test_prepare_block_requests_with_leading_fork_at_10() {
        let rng = &mut TestRng::default();
        let sync = sample_sync_at_height(0);

        // Intuitively, peer 1's fork is at peer 2 and peer 3's height.
        // So from peer 2 and peer 3's perspective, they recognize that peer 1 has forked.
        // Thus, you don't have NUM_REDUNDANCY peers to sync to block 10.
        //
        // Now, while you could in theory sync up to block 9 from any of the 3 peers,
        // we choose not to do this as either side is likely to disconnect from us,
        // and we would rather wait for enough redundant peers before syncing.

        // When there are NUM_REDUNDANCY peers ahead, and 1 peer is on a leading fork at 10,
        // then the sync pool should not request blocks as 1 peer conflicts with the other NUM_REDUNDANCY-1 peers.
        // We choose to sync with a cohort of peers that are *consistent* with each other,
        // and prioritize from descending heights (so the highest peer gets priority).

        // Add a peer (fork).
        let peer_1 = sample_peer_ip(1);
        sync.update_peer_locators(peer_1, &sample_block_locators_with_fork(20, 10)).unwrap();

        // Add a peer.
        let peer_2 = sample_peer_ip(2);
        sync.update_peer_locators(peer_2, &sample_block_locators(10)).unwrap();

        // Add a peer.
        let peer_3 = sample_peer_ip(3);
        sync.update_peer_locators(peer_3, &sample_block_locators(10)).unwrap();

        // Prepare the block requests.
        let batches = sync.prepare_block_requests();
        assert!(batches.is_empty());

        // When there are NUM_REDUNDANCY+1 peers ahead, and 1 is on a fork, then there should be block requests.

        // Add a peer.
        let peer_4 = sample_peer_ip(4);
        sync.update_peer_locators(peer_4, &sample_block_locators(10)).unwrap();

        // Prepare the block requests.
        let (requests, sync_peers) = sync.prepare_block_requests().pop().unwrap();
        assert_eq!(requests.len(), 10);

        // Check the requests.
        for (idx, (height, (hash, previous_hash, num_sync_ips))) in requests.into_iter().enumerate() {
            // Construct the sync IPs.
            let sync_ips: IndexSet<_> = sync_peers.keys().sample(rng, num_sync_ips).into_iter().copied().collect();
            assert_eq!(height, 1 + idx as u32);
            assert_eq!(hash, Some((Field::<CurrentNetwork>::from_u32(height)).into()));
            assert_eq!(previous_hash, Some((Field::<CurrentNetwork>::from_u32(height - 1)).into()));
            assert_eq!(sync_ips.len(), 1); // Only 1 needed since we have redundancy factor on this (recent locator) hash.
            assert_ne!(sync_ips[0], peer_1); // It should never be the forked peer.
        }
    }

    #[test]
    fn test_prepare_block_requests_with_trailing_fork_at_9() {
        let rng = &mut TestRng::default();
        let sync = sample_sync_at_height(0);

        // Peer 1 and 2 diverge from peer 3 at block 10. We only sync when there are NUM_REDUNDANCY peers
        // who are *consistent* with each other. So if you add a 4th peer that is consistent with peer 1 and 2,
        // then you should be able to sync up to block 10, thereby biasing away from peer 3.

        // Add a peer (fork).
        let peer_1 = sample_peer_ip(1);
        sync.update_peer_locators(peer_1, &sample_block_locators(10)).unwrap();

        // Add a peer.
        let peer_2 = sample_peer_ip(2);
        sync.update_peer_locators(peer_2, &sample_block_locators(10)).unwrap();

        // Add a peer.
        let peer_3 = sample_peer_ip(3);
        sync.update_peer_locators(peer_3, &sample_block_locators_with_fork(20, 10)).unwrap();

        // Prepare the block requests.
        let batches = sync.prepare_block_requests();
        assert!(batches.is_empty());

        // When there are NUM_REDUNDANCY+1 peers ahead, and peer 3 is on a fork, then there should be block requests.

        // Add a peer.
        let peer_4 = sample_peer_ip(4);
        sync.update_peer_locators(peer_4, &sample_block_locators(10)).unwrap();

        // Prepare the block requests.
        let (requests, sync_peers) = sync.prepare_block_requests().pop().unwrap();
        assert_eq!(requests.len(), 10);

        // Check the requests.
        for (idx, (height, (hash, previous_hash, num_sync_ips))) in requests.into_iter().enumerate() {
            // Construct the sync IPs.
            let sync_ips: IndexSet<_> = sync_peers.keys().sample(rng, num_sync_ips).into_iter().copied().collect();
            assert_eq!(height, 1 + idx as u32);
            assert_eq!(hash, Some((Field::<CurrentNetwork>::from_u32(height)).into()));
            assert_eq!(previous_hash, Some((Field::<CurrentNetwork>::from_u32(height - 1)).into()));
            assert_eq!(sync_ips.len(), 1); // Only 1 needed since we have redundancy factor on this (recent locator) hash.
            assert_ne!(sync_ips[0], peer_3); // It should never be the forked peer.
        }
    }

    #[test]
    fn test_insert_block_requests() {
        let rng = &mut TestRng::default();
        let sync = sample_sync_at_height(0);

        // Add a peer.
        sync.update_peer_locators(sample_peer_ip(1), &sample_block_locators(10)).unwrap();

        // Prepare the block requests.
        let (requests, sync_peers) = sync.prepare_block_requests().pop().unwrap();
        assert_eq!(requests.len(), 10);

        for (height, (hash, previous_hash, num_sync_ips)) in requests.clone() {
            // Construct the sync IPs.
            let sync_ips: IndexSet<_> = sync_peers.keys().sample(rng, num_sync_ips).into_iter().copied().collect();
            // Insert the block request.
            sync.insert_block_request(height, (hash, previous_hash, sync_ips.clone())).unwrap();
            // Check that the block requests were inserted.
            assert_eq!(sync.get_block_request(height), Some((hash, previous_hash, sync_ips)));
            assert!(sync.get_block_request_timestamp(height).is_some());
        }

        for (height, (hash, previous_hash, num_sync_ips)) in requests.clone() {
            // Construct the sync IPs.
            let sync_ips: IndexSet<_> = sync_peers.keys().sample(rng, num_sync_ips).into_iter().copied().collect();
            // Check that the block requests are still inserted.
            assert_eq!(sync.get_block_request(height), Some((hash, previous_hash, sync_ips)));
            assert!(sync.get_block_request_timestamp(height).is_some());
        }

        for (height, (hash, previous_hash, num_sync_ips)) in requests {
            // Construct the sync IPs.
            let sync_ips: IndexSet<_> = sync_peers.keys().sample(rng, num_sync_ips).into_iter().copied().collect();
            // Ensure that the block requests cannot be inserted twice.
            sync.insert_block_request(height, (hash, previous_hash, sync_ips.clone())).unwrap_err();
            // Check that the block requests are still inserted.
            assert_eq!(sync.get_block_request(height), Some((hash, previous_hash, sync_ips)));
            assert!(sync.get_block_request_timestamp(height).is_some());
        }
    }

    #[test]
    fn test_insert_block_requests_fails() {
        let sync = sample_sync_at_height(9);

        // Add a peer.
        sync.update_peer_locators(sample_peer_ip(1), &sample_block_locators(10)).unwrap();

        // Inserting a block height that is already in the ledger should fail.
        sync.insert_block_request(9, (None, None, indexset![sample_peer_ip(1)])).unwrap_err();
        // Inserting a block height that is not in the ledger should succeed.
        sync.insert_block_request(10, (None, None, indexset![sample_peer_ip(1)])).unwrap();
    }

    /// Returns the peer heights for `test_update_peer_locators`, in ascending order.
    ///
    /// These are every 17th height below `5 * NUM_RECENT_BLOCKS`, plus the heights next to each of `boundaries`.
    /// Some pairs of them are `NUM_RECENT_BLOCKS - 1`, `NUM_RECENT_BLOCKS` and `NUM_RECENT_BLOCKS + 1` apart,
    /// both near genesis and across a checkpoint.
    fn sample_peer_locator_heights() -> Vec<u32> {
        let recent = NUM_RECENT_BLOCKS as u32;
        let mut heights: BTreeSet<u32> = (0..5 * recent).step_by(17).collect();
        let boundaries =
            [recent, 2 * recent, CHECKPOINT_INTERVAL, CHECKPOINT_INTERVAL + recent, 2 * CHECKPOINT_INTERVAL];
        for boundary in boundaries {
            heights.extend(boundary - 2..=boundary + 1);
        }
        heights.into_iter().collect()
    }

    #[test]
    fn test_update_peer_locators() {
        let sync = sample_sync_at_height(0);
        let heights = sample_peer_locator_heights();

        // Test 2 peers.
        let peer1_ip = sample_peer_ip(1);
        for &peer1_height in &heights {
            sync.update_peer_locators(peer1_ip, &sample_block_locators(peer1_height)).unwrap();
            assert_eq!(sync.get_peer_height(&peer1_ip), Some(peer1_height));

            let peer2_ip = sample_peer_ip(2);
            for &peer2_height in &heights {
                sync.update_peer_locators(peer2_ip, &sample_block_locators(peer2_height)).unwrap();
                assert_eq!(sync.get_peer_height(&peer2_ip), Some(peer2_height));

                // Compute the distance between the peers.
                let distance = peer1_height.abs_diff(peer2_height);

                // Check the common ancestor.
                if distance < NUM_RECENT_BLOCKS as u32 {
                    let expected_ancestor = core::cmp::min(peer1_height, peer2_height);
                    assert_eq!(sync.get_common_ancestor(peer1_ip, peer2_ip), Some(expected_ancestor));
                    assert_eq!(sync.get_common_ancestor(peer2_ip, peer1_ip), Some(expected_ancestor));
                } else {
                    let min_checkpoints =
                        core::cmp::min(peer1_height / CHECKPOINT_INTERVAL, peer2_height / CHECKPOINT_INTERVAL);
                    let expected_ancestor = min_checkpoints * CHECKPOINT_INTERVAL;
                    assert_eq!(sync.get_common_ancestor(peer1_ip, peer2_ip), Some(expected_ancestor));
                    assert_eq!(sync.get_common_ancestor(peer2_ip, peer1_ip), Some(expected_ancestor));
                }
            }
        }
    }

    #[test]
    fn test_remove_peer() {
        let sync = sample_sync_at_height(0);

        let peer_ip = sample_peer_ip(1);
        sync.update_peer_locators(peer_ip, &sample_block_locators(100)).unwrap();
        assert_eq!(sync.get_peer_height(&peer_ip), Some(100));

        sync.remove_peer(&peer_ip);
        assert_eq!(sync.get_peer_height(&peer_ip), None);

        sync.update_peer_locators(peer_ip, &sample_block_locators(200)).unwrap();
        assert_eq!(sync.get_peer_height(&peer_ip), Some(200));

        sync.remove_peer(&peer_ip);
        assert_eq!(sync.get_peer_height(&peer_ip), None);
    }

    #[test]
    fn test_locators_insert_remove_insert() {
        let sync = sample_sync_at_height(0);

        let peer_ip = sample_peer_ip(1);
        sync.update_peer_locators(peer_ip, &sample_block_locators(100)).unwrap();
        assert_eq!(sync.get_peer_height(&peer_ip), Some(100));

        sync.remove_peer(&peer_ip);
        assert_eq!(sync.get_peer_height(&peer_ip), None);

        sync.update_peer_locators(peer_ip, &sample_block_locators(200)).unwrap();
        assert_eq!(sync.get_peer_height(&peer_ip), Some(200));
    }

    #[test]
    fn test_requests_insert_remove_insert() {
        let rng = &mut TestRng::default();
        let sync = sample_sync_at_height(0);

        // Add a peer.
        let peer_ip = sample_peer_ip(1);
        sync.update_peer_locators(peer_ip, &sample_block_locators(10)).unwrap();

        // Prepare the block requests.
        let (requests, sync_peers) = sync.prepare_block_requests().pop().unwrap();
        assert_eq!(requests.len(), 10);

        for (height, (hash, previous_hash, num_sync_ips)) in requests.clone() {
            // Construct the sync IPs.
            let sync_ips: IndexSet<_> = sync_peers.keys().sample(rng, num_sync_ips).into_iter().copied().collect();
            // Insert the block request.
            sync.insert_block_request(height, (hash, previous_hash, sync_ips.clone())).unwrap();
            // Check that the block requests were inserted.
            assert_eq!(sync.get_block_request(height), Some((hash, previous_hash, sync_ips)));
            assert!(sync.get_block_request_timestamp(height).is_some());
        }

        // Remove the peer.
        sync.remove_peer(&peer_ip);

        for (height, _) in requests {
            // Check that the block requests were removed.
            assert_eq!(sync.get_block_request(height), None);
            assert!(sync.get_block_request_timestamp(height).is_none());
        }

        // As there is no peer, it should not be possible to prepare block requests.
        let batches = sync.prepare_block_requests();
        assert!(batches.is_empty());

        // Add the peer again.
        sync.update_peer_locators(peer_ip, &sample_block_locators(10)).unwrap();

        // Prepare the block requests.
        let (requests, _) = sync.prepare_block_requests().pop().unwrap();
        assert_eq!(requests.len(), 10);

        for (height, (hash, previous_hash, num_sync_ips)) in requests {
            // Construct the sync IPs.
            let sync_ips: IndexSet<_> = sync_peers.keys().sample(rng, num_sync_ips).into_iter().copied().collect();
            // Insert the block request.
            sync.insert_block_request(height, (hash, previous_hash, sync_ips.clone())).unwrap();
            // Check that the block requests were inserted.
            assert_eq!(sync.get_block_request(height), Some((hash, previous_hash, sync_ips)));
            assert!(sync.get_block_request_timestamp(height).is_some());
        }
    }

    #[test]
    fn test_obsolete_block_requests() {
        let rng = &mut TestRng::default();
        let sync = sample_sync_at_height(0);

        // Ensure the locator always includes at least one block,
        // that is not the genesis block.
        // Otherwise there are no block requests to construct.
        let locator_height = rng.random_range(1..50);

        // Add a peer.
        let locators = sample_block_locators(locator_height);
        sync.update_peer_locators(sample_peer_ip(1), &locators).unwrap();

        // Construct block requests
        let (requests, sync_peers) = sync.prepare_block_requests().pop().unwrap();
        assert_eq!(requests.len(), locator_height as usize);

        // Add the block requests to the sync module.
        for (height, (hash, previous_hash, num_sync_ips)) in requests.clone() {
            // Construct the sync IPs.
            let sync_ips: IndexSet<_> = sync_peers.keys().sample(rng, num_sync_ips).into_iter().copied().collect();
            // Insert the block request.
            sync.insert_block_request(height, (hash, previous_hash, sync_ips.clone())).unwrap();
            // Check that the block requests were inserted.
            assert_eq!(sync.get_block_request(height), Some((hash, previous_hash, sync_ips)));
            assert!(sync.get_block_request_timestamp(height).is_some());
        }

        // Duplicate a new sync module with a different height to simulate block advancement.
        // This range needs to be inclusive, so that the range is never empty,
        // even with a locator height of 0.
        let ledger_height = rng.random_range(0..=locator_height);
        let new_sync = duplicate_sync_at_new_height(&sync, ledger_height);

        // Check that the number of requests is the same.
        assert_eq!(new_sync.requests.read().len(), requests.len());

        // Remove timed out block requests.
        new_sync.handle_block_request_timeouts();

        // Check that the number of requests is reduced based on the ledger height.
        assert_eq!(new_sync.requests.read().len(), (locator_height - ledger_height) as usize);
    }

    #[test]
    fn test_timed_out_block_request() {
        let sync = sample_sync_at_height(0);
        let peer_ip = sample_peer_ip(1);
        let locators = sample_block_locators(10);
        let block_hash = locators.get_hash(1);

        sync.update_peer_locators(peer_ip, &locators).unwrap();

        let timestamp = Instant::now() - BLOCK_REQUEST_TIMEOUT - Duration::from_secs(1);

        // Add a timed-out request
        sync.requests.write().insert(1, OutstandingRequest {
            request: (block_hash, None, [peer_ip].into()),
            timestamp,
            response: None,
        });

        assert_eq!(sync.requests.read().len(), 1);
        assert_eq!(sync.locators.read().len(), 1);

        // Remove timed out block requests.
        sync.handle_block_request_timeouts();

        // let ban_list = c.peers_to_ban.write();
        // assert_eq!(ban_list.len(), 1);
        // assert_eq!(ban_list.iter().next(), Some(&peer_ip));

        assert!(sync.requests.read().is_empty());
        assert!(sync.locators.read().is_empty());
    }

    #[test]
    fn test_reissue_timed_out_block_request() {
        let sync = sample_sync_at_height(0);
        let peer_ip1 = sample_peer_ip(1);
        let peer_ip2 = sample_peer_ip(2);
        let peer_ip3 = sample_peer_ip(3);

        let locators = sample_block_locators(10);
        let block_hash1 = locators.get_hash(1);
        let block_hash2 = locators.get_hash(2);

        sync.update_peer_locators(peer_ip1, &locators).unwrap();
        sync.update_peer_locators(peer_ip2, &locators).unwrap();
        sync.update_peer_locators(peer_ip3, &locators).unwrap();

        assert_eq!(sync.locators.read().len(), 3);

        let timestamp = Instant::now() - BLOCK_REQUEST_TIMEOUT - Duration::from_secs(1);

        // Add a timed-out request
        sync.requests.write().insert(1, OutstandingRequest {
            request: (block_hash1, None, [peer_ip1].into()),
            timestamp,
            response: None,
        });

        // Add a timed-out request
        sync.requests.write().insert(2, OutstandingRequest {
            request: (block_hash2, None, [peer_ip2].into()),
            timestamp: Instant::now(),
            response: None,
        });

        assert_eq!(sync.requests.read().len(), 2);

        // Remove timed out block requests.
        sync.handle_block_request_timeouts();

        // let ban_list = c.peers_to_ban.write();
        // assert_eq!(ban_list.len(), 1);
        // assert_eq!(ban_list.iter().next(), Some(&peer_ip1));

        assert_eq!(sync.requests.read().len(), 1);
        assert_eq!(sync.locators.read().len(), 2);

        let failed_requests = sync.failed_requests.lock();
        assert_eq!(failed_requests.len(), 1);

        let (height, (hash, _)) = failed_requests.iter().next().unwrap();
        assert_eq!(*height, 1);
        assert_eq!(*hash, block_hash1);
        /*
        assert_eq!(new_sync_ips.len(), 2);

        // Make sure the removed peer is not in the sync_peer set.
        let mut iter = new_sync_ips.iter();
        assert_ne!(iter.next().unwrap().0, &peer_ip1);
        assert_ne!(iter.next().unwrap().0, &peer_ip1);*/
    }
}
