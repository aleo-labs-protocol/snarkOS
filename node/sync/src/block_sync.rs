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
use snarkos_node_router::{MINIMUM_NUMBER_OF_PEERS, messages::DataBlocks};
use snarkos_node_sync_communication_service::CommunicationService;
use snarkos_node_sync_locators::{CHECKPOINT_INTERVAL, NUM_RECENT_BLOCKS};

use snarkvm::{
    console::network::{ConsensusVersion, Network},
    ledger::{
        Block,
        CheckBlockError,
        store::{AUTHORITY_RETENTION_BLOCKS, oldest_retained_authority_height},
    },
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

/// The duration after which a node that cannot request its needed blocks from any peer
/// (see [`BlockSync::sync_stuck_below_peer_floors`]) should give up and shut down.
pub const STUCK_SYNC_SHUTDOWN_THRESHOLD: Duration = Duration::from_secs(5 * 60);

/// Returns `true` if a peer at `peer_tip` is assumed to still serve block `height`: every node is
/// assumed to prune authority data older than `AUTHORITY_RETENTION_BLOCKS` blocks, so a peer at
/// `tip` serves `tip - (AUTHORITY_RETENTION_BLOCKS - 1)..=tip`.
///
/// A peer's advertised tip is a snapshot, so the peer may have pruned a little more by the time a
/// request arrives. No slack is applied: such a request gets an empty response and is re-issued.
fn peer_can_serve(peer_tip: u32, height: u32) -> bool {
    peer_tip.saturating_sub(AUTHORITY_RETENTION_BLOCKS - 1) <= height
}

/// Returns `true` if a peer at `peer_tip`, with the given learned floor, is assumed to serve `height`.
fn can_serve(peer_tip: u32, learned_floor: Option<&u32>, height: u32) -> bool {
    peer_can_serve(peer_tip, height) && learned_floor.is_none_or(|floor| *floor <= height)
}

/// Returns `true` if the range `start_height..end_height` reaches below this node's authority
/// retention window when its tip is `latest_height`. Genesis is never pruned.
pub fn range_below_retention_window(start_height: u32, end_height: u32, latest_height: u32) -> bool {
    let first_prunable = start_height.max(1);
    end_height > first_prunable && first_prunable < oldest_retained_authority_height(latest_height)
}

/// Returns the blocks to answer a block request for `start_height..end_height` with: the blocks
/// from `get_blocks`, or none if this node has pruned the range or it reaches past the tip (see
/// [`DataBlocks::ensure_response_is_well_formed`]). A range below this node's retention window is
/// answered empty without reading storage. A read error within the window is returned, so that the
/// requester sees a disconnect rather than a "pruned" signal.
pub fn blocks_or_empty<N: Network>(
    start_height: u32,
    end_height: u32,
    latest_height: impl Fn() -> u32,
    get_blocks: impl FnOnce() -> Result<Vec<Block<N>>>,
) -> Result<DataBlocks<N>> {
    if range_below_retention_window(start_height, end_height, latest_height()) {
        debug!("Cannot serve block request {start_height}..{end_height} - below the retention window");
        return Ok(DataBlocks(vec![]));
    }
    get_blocks().map(DataBlocks).or_else(|error| {
        // Re-read the tip: the range may have been pruned since the check above.
        let latest_height = latest_height();
        if end_height > latest_height.saturating_add(1)
            || range_below_retention_window(start_height, end_height, latest_height)
        {
            debug!("Cannot serve block request {start_height}..{end_height} - {}", flatten_error(&error));
            Ok(DataBlocks(vec![]))
        } else {
            Err(error)
        }
    })
}

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
    #[error("Empty block response (this could indicate the peer cannot serve the requested blocks)")]
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
        matches!(self, Self::NoSuchRequest { .. } | Self::BlockSyncAlreadyAdvanced { .. } | Self::EmptyBlockResponse)
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

    /// Since when, and at which height, sync has been stuck because the peers have pruned the blocks
    /// this node needs (see [`Self::sync_stuck_below_peer_floors`]).
    stuck_since: Mutex<Option<(Instant, u32)>>,

    /// When the node last warned that every peer ahead has pruned the blocks it needs, while too few
    /// peers corroborate it for [`Self::sync_stuck_below_peer_floors`] to report sync as stuck.
    last_pruned_peers_warning: Mutex<Option<Instant>>,

    /// The lowest height each peer may still serve, learned from its empty block responses. An empty
    /// response to a request from `h` means `h` is below the peer's floor or past its tip; peers only
    /// prune from below, so a floor only rises. Empty responses are never penalised. A peer behind its
    /// advertised locators may be excluded until it reconnects (accepted).
    ///
    /// Locking: `learned_floors` is locked after `locators` (and after `prepare_requests_lock`). An
    /// entry is only created while holding `locators` and seeing the peer in it, so that it cannot
    /// outlive `remove_peer`. A floor that rises after `select_sync_ips` read it costs one more empty
    /// response.
    learned_floors: Mutex<HashMap<SocketAddr, u32>>,

    /// Whether the node only connects to trusted peers (see [`Self::sync_stuck_below_peer_floors`]).
    trusted_peers_only: bool,
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
            stuck_since: Default::default(),
            last_pruned_peers_warning: Default::default(),
            learned_floors: Default::default(),
            trusted_peers_only: false,
        }
    }

    /// Sets whether the node only connects to trusted peers.
    pub fn with_trusted_peers_only(self, trusted_peers_only: bool) -> Self {
        Self { trusted_peers_only, ..self }
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
    /// Returns the latest locator height of the given peer.
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

    /// Returns up to `max_num_sync_ips` of `sync_peers`, randomly sampled, that are still connected
    /// and assumed to serve `start_height`, or `None` if none qualify or fewer than
    /// min(`max_num_sync_ips`, still connected) do. Call it while holding `prepare_requests_lock`.
    fn select_sync_ips(
        &self,
        sync_peers: &IndexMap<SocketAddr, BlockLocators<N>>,
        start_height: u32,
        max_num_sync_ips: usize,
    ) -> Option<IndexSet<SocketAddr>> {
        let locators = self.locators.read();
        let learned_floors = self.learned_floors.lock();
        let num_connected = sync_peers.keys().filter(|ip| locators.contains_key(*ip)).count();
        let sync_ips: IndexSet<_> = sync_peers
            .keys()
            .filter(|ip| {
                locators.get(*ip).is_some_and(|locators| {
                    can_serve(locators.latest_locator_height(), learned_floors.get(*ip), start_height)
                })
            })
            .copied()
            .sample(&mut rand::rng(), max_num_sync_ips)
            .into_iter()
            .collect();
        (sync_ips.len() >= max_num_sync_ips.min(num_connected).max(1)).then_some(sync_ips)
    }

    /// Queues prepared block requests that were not sent for re-issue, except heights that are
    /// already requested.
    fn requeue_requests(&self, requests: &[(u32, PrepareSyncRequest<N>)]) {
        let outstanding: Vec<u32> = {
            let outstanding = self.requests.read();
            requests.iter().map(|(height, _)| *height).filter(|height| outstanding.contains_key(height)).collect()
        };
        let mut failed_requests = self.failed_requests.lock();
        for (height, (hash, previous_hash, _)) in requests {
            if !outstanding.contains(height) {
                failed_requests.insert(*height, (*hash, *previous_hash));
            }
        }
    }

    /// Send a batch of block requests. A chunk that is not sent is queued for re-issue.
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

        // Calculate the end height.
        let end_height = start_height.saturating_add(requests.len() as u32);

        // Hold `prepare_requests_lock` (shared with remove_peer), so that selecting peers that are
        // still connected and inserting the requests are atomic.
        let sync_ips = {
            let _prepare_requests_lock = self.prepare_requests_lock.lock();
            let Some(sync_ips) = self.select_sync_ips(sync_peers, start_height, max_num_sync_ips) else {
                trace!(
                    "Skipping block request batch for heights {start_height}-{inclusive_end}: too few prepared peers can still serve it",
                    inclusive_end = end_height.saturating_sub(1)
                );
                self.requeue_requests(requests);
                return false;
            };

            // Insert the chunk of block requests (still holding lock so remove_peer cannot run).
            for (i, (height, (hash, previous_hash, _))) in requests.iter().enumerate() {
                // Insert the block request into the sync pool using the sync IPs from the last block request in the chunk.
                if let Err(err) = self.insert_block_request(*height, (*hash, *previous_hash, sync_ips.clone())) {
                    let err = err.context(format!("Failed to insert block request for height {height}"));
                    warn!("{}", flatten_error(&err));
                    // Undo the requests inserted for this chunk, which will not be sent.
                    let mut outstanding = self.requests.write();
                    for (height, _) in &requests[..i] {
                        outstanding.remove(height);
                    }
                    drop(outstanding);
                    self.requeue_requests(requests);
                    return false;
                }
            }
            sync_ips
        };

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
                {
                    let mut outstanding = self.requests.write();
                    for height in start_height..end_height {
                        outstanding.remove(&height);
                    }
                }
                self.requeue_requests(requests);
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
                let mut chunks = block_requests.chunks(DataBlocks::<N>::MAXIMUM_NUMBER_OF_BLOCKS as usize);
                while let Some(requests) = chunks.next() {
                    if !self.send_block_requests(communication, &sync_peers, requests).await {
                        // Queue the rest of the batch for re-issue.
                        chunks.for_each(|requests| self.requeue_requests(requests));
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
    /// `start_height` is the first height of the request that the response answers.
    #[inline]
    pub fn insert_block_responses(
        &self,
        peer_ip: SocketAddr,
        start_height: u32,
        blocks: Vec<Block<N>>,
        latest_consensus_version: Option<ConsensusVersion>,
    ) -> Result<(), InsertBlockResponseError<N>> {
        // Attempt to insert the block responses, and break if we encounter an error.
        let result = 'outer: {
            let Some(last_height) = blocks.as_slice().last().map(|b| b.height()) else {
                self.record_empty_block_response(peer_ip, start_height);
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

    /// Raises the learned floor of `peer_ip` above `start_height`, after an empty response to a
    /// request from `start_height`.
    fn record_empty_block_response(&self, peer_ip: SocketAddr, start_height: u32) {
        // Holding `locators` keeps a concurrent `remove_peer` from leaving this peer's entry behind.
        let locators = self.locators.read();
        if locators.contains_key(&peer_ip) {
            let mut learned_floors = self.learned_floors.lock();
            let floor = learned_floors.entry(peer_ip).or_default();
            *floor = (*floor).max(start_height.saturating_add(1));
        }
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
    /// Peers are not filtered by their retention floor: callers use this for the network tip (BFT
    /// sync mode, prover puzzle requests), which does not depend on which blocks a peer still serves.
    ///
    /// # Locking
    /// This will read-lock `common_ancestors` and `sync_state`, but not at the same time.
    pub fn find_sync_peers(&self) -> Option<(IndexMap<SocketAddr, u32>, u32)> {
        // Retrieve the current sync height.
        let current_height = self.get_sync_height();

        if let Some((sync_peers, min_common_ancestor)) = self.find_sync_peers_inner(current_height, None) {
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
        self.learned_floors.lock().remove(peer_ip);
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
        // Every height derived below is relative to the retention height, so that the re-request and
        // new-request paths agree (see `retention_height`).
        let current_height = self.retention_height();

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

                // Re-issue from the first height not already requested (the batch is above
                // `current_height`, see above), up to the end of the batch. The heights before it
                // are outstanding, so they leave the failed requests; if none remain, the batch is
                // obsolete.
                let start = self.first_unrequested_height(start_height);
                for height in start_height..start.min(end_height) {
                    failed_requests.remove(&height);
                }
                let max_new_blocks_to_request = end_height.saturating_sub(start);
                if max_new_blocks_to_request == 0 {
                    continue;
                }

                let Some((sync_peers, min_common_ancestor)) = self.find_sync_peers_inner(current_height, Some(start))
                else {
                    // No connected peer can serve this range: they disconnected, pruned it (see
                    // `sync_stuck_below_peer_floors`), or have not reached it yet. The requests stay
                    // queued until a peer that has reached `start` connects.
                    debug!("Cannot re-request blocks {start}..{end_height}: no connected peer has them");
                    continue;
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
                    start,
                    min_common_ancestor,
                    max_new_blocks_to_request,
                    greatest_peer_height,
                );

                // Only remove from failed_requests the heights we actually re-issued; a prepared range
                // that is not sent is put back (see `requeue_requests`). (If construct_requests
                // returned empty we must not drop these failed requests.)
                for (height, _) in &requests {
                    failed_requests.remove(height);
                }

                if !requests.is_empty() {
                    result.push((requests, sync_peers));
                }
            }

            // If no batch was re-issued, fall through to new requests.
            if !result.is_empty() {
                return result;
            }
        }

        // Ensure to not exceed the maximum number of outstanding block requests.
        let max_outstanding_block_requests =
            (MAX_BLOCK_REQUESTS as u32) * (DataBlocks::<N>::MAXIMUM_NUMBER_OF_BLOCKS as u32);

        // Ensure there is a finite bound on the number of block respnoses we receive, that have not been processed yet.
        let max_total_requests = 4 * max_outstanding_block_requests;

        let max_new_blocks_to_request =
            max_outstanding_block_requests.saturating_sub(self.num_outstanding_block_requests() as u32);

        // The first height to request: the next one this node needs, or the first above it not
        // already requested.
        let start_height = self.first_unrequested_height(current_height.saturating_add(1));

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
        } else if let Some((sync_peers, min_common_ancestor)) =
            self.find_sync_peers_inner(current_height, Some(start_height))
        {
            // Retrieve the greatest block height of any connected peer.
            // We do not need to update the sync state here, as that already happens when the block locators are received.
            let greatest_peer_height = sync_peers.values().map(|l| l.latest_locator_height()).max().unwrap_or(0);

            // Construct the list of block requests.
            let requests = self.construct_requests(
                &sync_peers,
                start_height,
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

    /// Returns `(peers ahead of height, peers with locators)` if at least one peer is ahead of
    /// `height` and every such peer is assumed to have pruned block `height + 1`.
    fn peers_ahead_all_pruned(&self, height: u32) -> Option<(usize, usize)> {
        let locators = self.locators.read();
        let learned_floors = self.learned_floors.lock();
        let mut num_ahead = 0;
        for (peer_ip, locators) in locators.iter() {
            let tip = locators.latest_locator_height();
            if tip <= height {
                continue;
            }
            if can_serve(tip, learned_floors.get(peer_ip), height.saturating_add(1)) {
                return None;
            }
            num_ahead += 1;
        }
        (num_ahead > 0).then_some((num_ahead, locators.len()))
    }

    /// Returns how long sync has made no progress because the peers have pruned the blocks this
    /// node needs, if that is currently the case.
    ///
    /// The peers must corroborate it: a strict majority of the peers that sent locators are ahead with
    /// an assumed floor above the next needed block, and, unless the node only connects to trusted
    /// peers, at least `MINIMUM_NUMBER_OF_PEERS` peers sent locators. A lone peer fabricating a
    /// far-ahead tip is a minority among honest peers at this height, so it cannot trigger a
    /// shutdown. With fewer peers, a warning is logged instead.
    ///
    /// The condition is evaluated on every call, so a window armed earlier cannot outlive it. The
    /// window is armed when the condition first holds, restarted if the ledger height advances while
    /// it holds, and cleared when it stops holding.
    pub fn sync_stuck_below_peer_floors(&self) -> Option<Duration> {
        // The majority rule leaves no peer eligible for new requests, so only existing requests need
        // a separate check: one still awaiting a response, or one for the next block, means sync can
        // still progress. Answered requests above an unservable gap do not count.
        let height = self.retention_height();
        let can_progress = {
            let requests = self.requests.read();
            requests.contains_key(&height.saturating_add(1))
                || requests.values().any(|r| r.response.is_none() && !r.sync_ips().is_empty())
        };
        let pruned = if can_progress { None } else { self.peers_ahead_all_pruned(height) };
        // A strict majority, written without division so it is exact.
        let is_stuck = pruned.is_some_and(|(num_ahead, num_peers)| {
            (self.trusted_peers_only || num_peers >= MINIMUM_NUMBER_OF_PEERS) && num_ahead * 2 > num_peers
        });
        self.warn_if_pruned_without_majority(pruned.is_some() && !is_stuck, height);

        let mut stuck_since = self.stuck_since.lock();
        match *stuck_since {
            _ if !is_stuck => {
                *stuck_since = None;
                None
            }
            Some((since, armed_height)) if height <= armed_height => Some(since.elapsed()),
            previous => {
                if previous.is_none() {
                    warn!(
                        "The blocks this node needs are older than what the connected peers retain; block synchronization cannot progress"
                    );
                }
                *stuck_since = Some((Instant::now(), height));
                Some(Duration::ZERO)
            }
        }
    }

    /// Should only be called by validators when they successfully process a block request.
    /// (for other nodes this will be automatically called internally)
    ///
    /// TODO(kaimast): remove this public function once the sync logic is fully unified `BlockSync`.
    pub fn count_request_completed(&self) {
        self.metrics.count_request_completed();
    }

    /// Warns, every `STUCK_SYNC_SHUTDOWN_THRESHOLD` while `warn` holds, that every peer ahead has
    /// pruned the next needed block but too few peers corroborate it to shut down.
    fn warn_if_pruned_without_majority(&self, warn: bool, height: u32) {
        let mut last_warning = self.last_pruned_peers_warning.lock();
        if !warn {
            *last_warning = None;
        } else if last_warning.is_none_or(|at| at.elapsed() >= STUCK_SYNC_SHUTDOWN_THRESHOLD) {
            *last_warning = Some(Instant::now());
            warn!(
                "Every peer ahead of this node has pruned block {}, and too few peers corroborate it to shut down \
                automatically. Sync will not progress unless a peer that retains the block connects: restart the \
                node to sync from the CDN, or restore a recent ledger snapshot.",
                height.saturating_add(1)
            );
        }
    }

    /// Returns the height whose successor the retention floor is checked against: the higher of the
    /// ledger and sync heights, so that a sync height not yet updated after blocks were added by
    /// other means (e.g. CDN sync) cannot make every peer look pruned.
    fn retention_height(&self) -> u32 {
        self.ledger.latest_block_height().max(self.get_sync_height())
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
            drop(failed_requests);
            // Wake the request loop to re-issue them.
            self.peer_notify.notify_one();
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
    /// Returns `None` if there are no peers to sync from. With `first_needed`, the first block to be
    /// requested, peers that have not reached it, or are assumed to have pruned it by their tip or
    /// their learned floor, are excluded.
    ///
    /// # Locking
    /// This function will read-lock `common_ancestors`.
    fn find_sync_peers_inner(
        &self,
        current_height: u32,
        first_needed: Option<u32>,
    ) -> Option<(IndexMap<SocketAddr, BlockLocators<N>>, u32)> {
        // Retrieve the latest ledger height.
        let latest_ledger_height = self.ledger.latest_block_height();

        // Pick a set of peers above the latest ledger height, and include their locators.
        // This will sort the peers by locator height in descending order.
        // Peers that have not reached the first needed block, or are assumed to have pruned its
        // authority data, are excluded.
        let candidate_locators: IndexMap<_, _> = {
            let locators = self.locators.read();
            let learned_floors = self.learned_floors.lock();
            locators
                .iter()
                .filter(|(peer_ip, locators)| {
                    let peer_tip = locators.latest_locator_height();
                    peer_tip > current_height
                        && first_needed.is_none_or(|height| {
                            peer_tip >= height && can_serve(peer_tip, learned_floors.get(*peer_ip), height)
                        })
                })
                .sorted_by(|(_, a), (_, b)| b.latest_locator_height().cmp(&a.latest_locator_height()))
                .take(NUM_SYNC_CANDIDATE_PEERS)
                .map(|(peer_ip, locators)| (*peer_ip, locators.clone()))
                .collect()
        };

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

    /// Returns the first height at or above `height` that is not already requested. Callers pass a
    /// height above `retention_height`, so the result is above the ledger height.
    fn first_unrequested_height(&self, height: u32) -> u32 {
        let requests = self.requests.read();
        let mut height = height;
        while requests.contains_key(&height) {
            height += 1;
        }
        height
    }

    /// Given the sync peers and their minimum common ancestor, return a list of block requests
    /// starting at `start_height` (see `first_unrequested_height`).
    ///
    /// # Returns
    /// The list of block requests, ordered by height.
    fn construct_requests(
        &self,
        sync_peers: &IndexMap<SocketAddr, BlockLocators<N>>,
        start_height: u32,
        min_common_ancestor: u32,
        max_blocks_to_request: u32,
        greatest_peer_height: u32,
    ) -> Vec<(u32, PrepareSyncRequest<N>)> {
        // If the minimum common ancestor is below the start height, then return early.
        if min_common_ancestor < start_height {
            if start_height < greatest_peer_height {
                trace!(
                    "No request to construct. Height for the next block request is {start_height}, but minimum common block locator ancestor is only {min_common_ancestor} (greatest_peer_height={greatest_peer_height})"
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
            stuck_since: Default::default(),
            last_pruned_peers_warning: Default::default(),
            learned_floors: Default::default(),
            trusted_peers_only: false,
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

    /// Tests that an empty block response, the peer's explicit "cannot serve this range" signal,
    /// is treated as benign and re-issues the peer's outstanding requests.
    #[test]
    fn test_empty_block_response_is_benign_and_reissues_requests() {
        let rng = &mut TestRng::default();
        let sync = sample_sync_at_height(0);
        let peer_ip = sample_peer_ip(1);

        // Add a peer and construct outstanding block requests assigned to it.
        sync.update_peer_locators(peer_ip, &sample_block_locators(10)).unwrap();
        let (requests, sync_peers) = sync.prepare_block_requests().pop().unwrap();
        assert!(!requests.is_empty());
        for (height, (hash, previous_hash, num_sync_ips)) in requests.clone() {
            // Construct the sync IPs.
            let sync_ips: IndexSet<_> = sync_peers.keys().sample(rng, num_sync_ips).into_iter().copied().collect();
            // Insert the block request.
            sync.insert_block_request(height, (hash, previous_hash, sync_ips)).unwrap();
        }
        let num_requests = sync.requests.read().len();
        assert_eq!(num_requests, requests.len());

        // The peer responds with an empty block response, indicating it cannot serve the range.
        let error = sync.insert_block_responses(peer_ip, 1, vec![], None).unwrap_err();
        assert!(matches!(error, InsertBlockResponseError::EmptyBlockResponse));
        // An explicit "cannot serve" is not a protocol violation.
        assert!(error.is_benign());

        // The requests to the peer were removed and marked for re-issue to other peers.
        assert!(sync.requests.read().is_empty());
        assert_eq!(sync.failed_requests.lock().len(), num_requests);
    }

    /// Sends an empty block response from `peer_ip` to a request from `height`.
    fn empty_response_to(
        sync: &BlockSync<CurrentNetwork>,
        peer_ip: SocketAddr,
        height: u32,
    ) -> InsertBlockResponseError<CurrentNetwork> {
        sync.insert_block_responses(peer_ip, height, vec![], None).unwrap_err()
    }

    /// Returns the learned floor of `peer_ip`.
    fn learned_floor(sync: &BlockSync<CurrentNetwork>, peer_ip: SocketAddr) -> Option<u32> {
        sync.learned_floors.lock().get(&peer_ip).copied()
    }

    /// Tests that an empty response raises the peer's learned floor just above the request it answers,
    /// and is never penalised.
    #[test]
    fn test_empty_block_response_raises_learned_floor() {
        let sync = sample_sync_at_height(1_000);
        let peer_ip = sample_peer_ip(1);
        sync.update_peer_locators(peer_ip, &sample_block_locators(1_000 + AUTHORITY_RETENTION_BLOCKS)).unwrap();
        assert!(!sync.prepare_block_requests().is_empty());

        // The peer pruned block 1,001 since its last ping.
        assert!(empty_response_to(&sync, peer_ip, 1_001).is_benign());
        assert_eq!(learned_floor(&sync, peer_ip), Some(1_002));
        sync.failed_requests.lock().clear();
        assert!(sync.prepare_block_requests().is_empty(), "the peer is not asked for block 1,001 again");

        // The floor only rises, and further empty responses stay benign.
        assert!(empty_response_to(&sync, peer_ip, 1_010).is_benign());
        assert!(empty_response_to(&sync, peer_ip, 1_005).is_benign());
        assert_eq!(learned_floor(&sync, peer_ip), Some(1_011));

        // Disconnecting clears it.
        sync.remove_peer(&peer_ip);
        assert_eq!(learned_floor(&sync, peer_ip), None);
    }

    /// Tests that a peer stays eligible for blocks at or above its learned floor.
    #[test]
    fn test_peer_eligible_at_learned_floor() {
        let sync = sample_sync_at_height(1_000);
        let peer_ip = sample_peer_ip(1);
        sync.update_peer_locators(peer_ip, &sample_block_locators(1_100)).unwrap();
        empty_response_to(&sync, peer_ip, 1_001);

        assert!(sync.find_sync_peers_inner(1_000, Some(1_001)).is_none());
        assert!(sync.find_sync_peers_inner(1_000, Some(1_002)).is_some());
        // The network tip is not filtered.
        assert!(sync.find_sync_peers().is_some());
    }

    /// Tests that peers are filtered by the first height to request, not the next ledger height:
    /// during catch-up the two are far apart, and a peer whose assumed floor lies between them can
    /// serve the request.
    #[test]
    fn test_sync_peers_filtered_by_first_unrequested_height() {
        let sync = sample_sync_at_height(1_000);
        // A peer that serves block 1,001 holds the outstanding requests for 1,001..=1,050.
        sync.update_peer_locators(sample_peer_ip(1), &sample_block_locators(1_000 + AUTHORITY_RETENTION_BLOCKS))
            .unwrap();
        for height in 1_001..=1_050 {
            sync.insert_block_request(height, (None, None, indexset![sample_peer_ip(1)])).unwrap();
        }
        // The peer's assumed floor is 1,030: above 1,001, below the next request at 1,051.
        let peer_ip = sample_peer_ip(2);
        sync.update_peer_locators(peer_ip, &sample_block_locators(1_029 + AUTHORITY_RETENTION_BLOCKS)).unwrap();
        assert!(!sync.find_sync_peers_inner(1_000, Some(1_001)).unwrap().0.contains_key(&peer_ip));

        let (requests, sync_peers) = sync.prepare_block_requests().pop().unwrap();
        assert_eq!(requests.first().unwrap().0, 1_051);
        assert!(sync_peers.contains_key(&peer_ip));
    }

    /// Tests that a failed batch wholly below the ledger height is dropped, even while the sync height
    /// lags the ledger, and new requests start at the first height the node needs.
    #[test]
    fn test_failed_batch_below_ledger_is_dropped() {
        let sync = BlockSync::<CurrentNetwork>::new(Arc::new(sample_ledger_service(0)), ConnectionMode::Router);
        let sync = BlockSync { ledger: Arc::new(sample_ledger_service(1_000)), ..sync };
        assert_eq!(sync.get_sync_height(), 0);
        sync.failed_requests.lock().extend((990..=999).map(|height| (height, (None, None))));
        // The peer's assumed floor is 1,001: above the batch start, at the first height needed.
        let peer_ip = sample_peer_ip(1);
        sync.update_peer_locators(peer_ip, &sample_block_locators(1_000 + AUTHORITY_RETENTION_BLOCKS)).unwrap();

        let (requests, sync_peers) = sync.prepare_block_requests().pop().unwrap();
        assert!(sync.failed_requests.lock().is_empty());
        assert_eq!(requests.first().unwrap().0, 1_001);
        // The new-request path ran: it requests more than the dropped batch's 10 blocks.
        assert!(requests.len() > 10);
        assert!(sync_peers.contains_key(&peer_ip));
    }

    /// Tests that a failed batch is re-issued from its first height not already requested, and only
    /// up to its end; its outstanding heights leave the failed requests, and a batch that is
    /// outstanding in full is dropped.
    #[test]
    fn test_failed_batch_reissued_from_first_unrequested_height() {
        let sync = sample_sync_at_height(1_000);
        let peer_ip = sample_peer_ip(1);
        sync.update_peer_locators(peer_ip, &sample_block_locators(1_100)).unwrap();
        sync.failed_requests.lock().extend((1_001..=1_010).map(|height| (height, (None, None))));
        for height in 1_001..=1_005 {
            sync.insert_block_request(height, (None, None, indexset![peer_ip])).unwrap();
        }

        let (requests, _) = sync.prepare_block_requests().pop().unwrap();
        assert_eq!(requests.iter().map(|(height, _)| *height).collect::<Vec<_>>(), (1_006..=1_010).collect::<Vec<_>>());
        assert!(sync.failed_requests.lock().is_empty());

        // A batch outstanding in full is dropped, and new requests follow above it.
        sync.failed_requests.lock().extend((1_001..=1_005).map(|height| (height, (None, None))));
        for height in 1_006..=1_010 {
            sync.insert_block_request(height, (None, None, indexset![peer_ip])).unwrap();
        }
        let (requests, _) = sync.prepare_block_requests().pop().unwrap();
        assert!(sync.failed_requests.lock().is_empty());
        assert_eq!(requests.first().unwrap().0, 1_011);
    }

    /// Tests that a failed batch is re-issued only by peers that have reached its start: a peer
    /// below it would pull the cohort's common ancestor below the batch and stall it.
    #[test]
    fn test_failed_batch_excludes_peers_below_its_start() {
        let sync = sample_sync_at_height(1_000);
        let (peer_a, peer_b) = (sample_peer_ip(1), sample_peer_ip(2));
        sync.update_peer_locators(peer_a, &sample_block_locators(1_100)).unwrap();
        sync.update_peer_locators(peer_b, &sample_block_locators(1_020)).unwrap();
        sync.failed_requests.lock().extend((1_050..=1_060).map(|height| (height, (None, None))));

        let (requests, sync_peers) = sync.prepare_block_requests().pop().unwrap();
        assert_eq!(requests.iter().map(|(height, _)| *height).collect::<Vec<_>>(), (1_050..=1_060).collect::<Vec<_>>());
        assert_eq!(sync_peers.keys().collect::<Vec<_>>(), vec![&peer_a]);
        assert!(sync.failed_requests.lock().is_empty());
        // Without a first needed height, both peers are sync peers.
        assert_eq!(sync.find_sync_peers().unwrap().0.len(), 2);
    }

    /// Tests that a failed batch no connected peer has reached stays queued and does not block new
    /// requests from the peers below it.
    #[test]
    fn test_failed_batch_above_all_peers_falls_through() {
        let sync = sample_sync_at_height(1_000);
        add_peers(&sync, 1, 2, 1_040);
        // The peer that served 1,050..=1,060 timed out and was removed.
        sync.failed_requests.lock().extend((1_050..=1_060).map(|height| (height, (None, None))));

        let (requests, _) = sync.prepare_block_requests().pop().unwrap();
        assert_eq!(requests.first().unwrap().0, 1_001);
        assert_eq!(sync.failed_requests.lock().len(), 11);
    }

    /// Tests that a failed batch no peer cohort can cover yet stays queued and does not block new
    /// requests.
    #[test]
    fn test_failed_batch_without_requests_falls_through() {
        let sync = sample_sync_at_height(1_000);
        let (peer_a, peer_b) = (sample_peer_ip(1), sample_peer_ip(2));
        sync.update_peer_locators(peer_a, &sample_block_locators(1_100)).unwrap();
        sync.update_peer_locators(peer_b, &sample_block_locators(1_060)).unwrap();
        // Synthetic: consistent locators cannot have an ancestor below their overlap. The override
        // only forces construct_requests to return nothing for the batch.
        sync.common_ancestors.write().insert(PeerPair(peer_a, peer_b), 1_020);
        sync.failed_requests.lock().extend((1_050..=1_060).map(|height| (height, (None, None))));

        let (requests, _) = sync.prepare_block_requests().pop().unwrap();
        assert_eq!(requests.first().unwrap().0, 1_001);
        assert_eq!(sync.failed_requests.lock().len(), 11);
    }

    /// Tests that a prepared batch is not sent to a peer whose learned floor rose above its start.
    #[test]
    fn test_prepared_batch_skips_peer_with_risen_floor() {
        let sync = sample_sync_at_height(1_000);
        let peer_ip = sample_peer_ip(1);
        sync.update_peer_locators(peer_ip, &sample_block_locators(1_100)).unwrap();
        let (requests, sync_peers) = sync.prepare_block_requests().pop().unwrap();
        let start_height = requests.first().unwrap().0;
        assert!(sync.select_sync_ips(&sync_peers, start_height, 1).is_some());

        // The peer answers empty for an earlier request from the same height.
        empty_response_to(&sync, peer_ip, start_height);
        assert!(sync.select_sync_ips(&sync_peers, start_height, 1).is_none());
    }

    /// Tests that a prepared batch needing several peers is skipped when fewer connected peers can
    /// still serve it.
    #[test]
    fn test_prepared_batch_needs_enough_peers() {
        let sync = sample_sync_at_height(1_000);
        add_peers(&sync, 1, 3, 1_100);
        let sync_peers: IndexMap<_, _> = (1..=3).map(|id| (sample_peer_ip(id), sample_block_locators(1_100))).collect();
        assert_eq!(sync.select_sync_ips(&sync_peers, 1_001, 3).unwrap().len(), 3);

        empty_response_to(&sync, sample_peer_ip(1), 1_001);
        assert!(sync.select_sync_ips(&sync_peers, 1_001, 3).is_none());
        assert_eq!(sync.select_sync_ips(&sync_peers, 1_001, 2).unwrap().len(), 2);

        // A disconnected peer does not count towards the peers the batch needs.
        sync.remove_peer(&sample_peer_ip(1));
        assert_eq!(sync.select_sync_ips(&sync_peers, 1_001, 3).unwrap().len(), 2);
        // With no connected peer, the batch is skipped.
        sync.remove_peer(&sample_peer_ip(2));
        sync.remove_peer(&sample_peer_ip(3));
        assert!(sync.select_sync_ips(&sync_peers, 1_001, 3).is_none());
    }

    /// Tests that requests that were prepared but not sent are queued for re-issue, except heights
    /// already requested.
    #[test]
    fn test_requeue_requests() {
        let sync = sample_sync_at_height(0);
        let peer_ip = sample_peer_ip(1);
        sync.insert_block_request(2, (None, None, [peer_ip].into_iter().collect())).unwrap();
        let requests: Vec<_> = (1..=3).map(|height| (height, (None, None, 1))).collect();
        sync.requeue_requests(&requests);
        assert_eq!(sync.failed_requests.lock().keys().copied().collect::<Vec<_>>(), vec![1, 3]);
    }

    /// Tests that peers ahead whose learned floors are above the next block arm the stuck signal, or
    /// raise the warning with too few peers.
    #[test]
    fn test_learned_floors_arm_stuck_signal() {
        for num_peers in [MINIMUM_NUMBER_OF_PEERS, 1] {
            let sync = sample_sync_at_height(1_000);
            add_peers(&sync, 1, num_peers, 1_100);
            assert!(sync.sync_stuck_below_peer_floors().is_none());

            for id in 1..=num_peers as u16 {
                empty_response_to(&sync, sample_peer_ip(id), 1_001);
            }
            let armed = num_peers >= MINIMUM_NUMBER_OF_PEERS;
            assert_eq!(sync.sync_stuck_below_peer_floors().is_some(), armed);
            assert_eq!(sync.last_pruned_peers_warning.lock().is_some(), !armed);
        }
    }

    /// Tests the boundary of the assumed peer floor: `peer_tip − (AUTHORITY_RETENTION_BLOCKS − 1)`
    /// must be at or below the next needed block for the peer to be eligible.
    #[test]
    fn test_floor_boundary() {
        // The next needed block is 1,001. A peer at tip `1_000 + AUTHORITY_RETENTION_BLOCKS` has an
        // assumed floor of exactly that height: eligible.
        let sync = sample_sync_at_height(1_000);
        sync.update_peer_locators(sample_peer_ip(1), &sample_block_locators(1_000 + AUTHORITY_RETENTION_BLOCKS))
            .unwrap();
        assert!(!sync.prepare_block_requests().is_empty());
        assert!(sync.sync_stuck_below_peer_floors().is_none());

        // A peer one block further ahead has an assumed floor one block higher: no longer eligible.
        let sync = sample_sync_at_height(1_000);
        sync.update_peer_locators(sample_peer_ip(1), &sample_block_locators(1_001 + AUTHORITY_RETENTION_BLOCKS))
            .unwrap();
        assert!(sync.prepare_block_requests().is_empty());
    }

    /// Tests that a range below this node's retention window is answered empty without reading
    /// storage on a pruning node, and that a storage error is answered empty.
    #[test]
    fn test_blocks_or_empty() {
        let latest = 2 * AUTHORITY_RETENTION_BLOCKS;
        let read = std::cell::Cell::new(false);
        let blocks = blocks_or_empty::<CurrentNetwork>(
            1,
            3,
            || latest,
            || {
                read.set(true);
                Ok(vec![])
            },
        );
        assert!(blocks.unwrap().0.is_empty());
        // Without `prune-authority-data` in snarkVM, every block is retained and storage is read.
        assert_eq!(read.get(), oldest_retained_authority_height(latest) == 0);

        // A read error within the window is returned; past the tip, the response is empty.
        assert!(blocks_or_empty::<CurrentNetwork>(latest, latest + 1, || latest, || bail!("storage error")).is_err());
        let blocks = blocks_or_empty::<CurrentNetwork>(latest, latest + 2, || latest, || bail!("missing block"));
        assert!(blocks.unwrap().0.is_empty());

        // A range pruned between the check and the read is answered empty on a pruning node.
        let tip = std::cell::Cell::new(latest);
        let next_tip = || tip.replace(latest + AUTHORITY_RETENTION_BLOCKS);
        let blocks = blocks_or_empty::<CurrentNetwork>(latest, latest + 1, next_tip, || bail!("pruned"));
        assert_eq!(blocks.is_ok(), oldest_retained_authority_height(latest) > 0);
    }

    #[test]
    fn test_peer_can_serve() {
        const R: u32 = AUTHORITY_RETENTION_BLOCKS;
        assert!(peer_can_serve(R - 1, 0));
        assert!(peer_can_serve(R, 1));
        assert!(!peer_can_serve(R, 0));
        assert!(peer_can_serve(2 * R, R + 1));
        assert!(!peer_can_serve(2 * R, R));
    }

    /// Tests that `find_sync_peers` reports peers that are far ahead even when they have pruned the
    /// blocks this node needs, since its callers use it for the network tip, while request building
    /// still excludes them.
    #[test]
    fn test_find_sync_peers_ignores_retention_floor() {
        let sync = sample_sync_at_height(0);
        add_peers(&sync, 1, MINIMUM_NUMBER_OF_PEERS, 2 * AUTHORITY_RETENTION_BLOCKS);

        let (sync_peers, _) = sync.find_sync_peers().expect("far-ahead peers are sync peers");
        assert_eq!(sync_peers.len(), MINIMUM_NUMBER_OF_PEERS);
        assert!(sync.prepare_block_requests().is_empty());
    }

    /// Adds `num_peers` peers with IDs from `first_id` on, all at tip `tip`.
    fn add_peers(sync: &BlockSync<CurrentNetwork>, first_id: u16, num_peers: usize, tip: u32) {
        for id in first_id..first_id + num_peers as u16 {
            sync.update_peer_locators(sample_peer_ip(id), &sample_block_locators(tip)).unwrap();
        }
    }

    /// Tests when peers that have pruned the needed blocks arm the stuck signal: only at least
    /// `MINIMUM_NUMBER_OF_PEERS` peers with locators, of which a strict majority have pruned them.
    #[test]
    fn test_stuck_signal_requires_pruned_peer_majority() {
        const HEIGHT: u32 = 1_000;
        // (peers that pruned the needed blocks, peers at this node's height, armed)
        let cases = [
            (1, 2, false), // A lone fabricated tip among honest peers.
            (2, 2, false), // Half is not a strict majority.
            (2, 0, false), // Too few peers.
            (2, 1, true),
            (3, 0, true),
        ];
        for (num_pruned, num_level, armed) in cases {
            let sync = sample_sync_at_height(HEIGHT);
            add_peers(&sync, 1, num_pruned, HEIGHT + 2 * AUTHORITY_RETENTION_BLOCKS);
            add_peers(&sync, 1 + num_pruned as u16, num_level, HEIGHT);

            assert!(sync.prepare_block_requests().is_empty());
            assert_eq!(sync.sync_stuck_below_peer_floors().is_some(), armed, "{num_pruned} pruned, {num_level} level");
        }
    }

    /// Tests that too few peers to arm the stuck signal, all ahead and pruned, raise the warning
    /// instead, and that a peer that retains the needed blocks clears it.
    #[test]
    fn test_pruned_peers_without_majority_warn() {
        let sync = sample_sync_at_height(1_000);
        add_peers(&sync, 1, 1, 1_000 + 2 * AUTHORITY_RETENTION_BLOCKS);
        assert!(sync.sync_stuck_below_peer_floors().is_none());
        assert!(sync.last_pruned_peers_warning.lock().is_some());

        add_peers(&sync, 2, 1, 1_000 + AUTHORITY_RETENTION_BLOCKS);
        assert!(sync.sync_stuck_below_peer_floors().is_none());
        assert!(sync.last_pruned_peers_warning.lock().is_none());
    }

    /// Tests that under `trusted_peers_only` a lone peer, ahead and pruned, arms the stuck signal
    /// without the warning, while otherwise it only warns.
    #[test]
    fn test_trusted_peers_only_drops_minimum_peers() {
        for trusted_peers_only in [true, false] {
            let sync = sample_sync_at_height(1_000).with_trusted_peers_only(trusted_peers_only);
            add_peers(&sync, 1, 1, 1_000 + 2 * AUTHORITY_RETENTION_BLOCKS);
            assert_eq!(sync.sync_stuck_below_peer_floors().is_some(), trusted_peers_only);
            assert_eq!(sync.last_pruned_peers_warning.lock().is_some(), !trusted_peers_only);
        }
    }

    /// Tests that a sync height left behind the ledger (e.g. after CDN sync) does not make peers that
    /// retain the next ledger block look pruned.
    #[test]
    fn test_stale_sync_height_uses_ledger_height() {
        let sync = BlockSync::<CurrentNetwork>::new(Arc::new(sample_ledger_service(0)), ConnectionMode::Router);
        let sync = BlockSync { ledger: Arc::new(sample_ledger_service(1_000)), ..sync };
        assert_eq!(sync.get_sync_height(), 0);
        add_peers(&sync, 1, MINIMUM_NUMBER_OF_PEERS, 1_000 + AUTHORITY_RETENTION_BLOCKS);

        assert!(!sync.prepare_block_requests().is_empty());
        assert!(sync.sync_stuck_below_peer_floors().is_none());
    }

    /// Tests that requests already answered, but waiting on an unservable gap below them, do not
    /// keep the stuck signal from arming, while an answer for the next block or a request still
    /// awaiting a response does.
    #[test]
    fn test_answered_requests_do_not_block_stuck_signal() {
        let sync = sample_sync_at_height(0);
        add_peers(&sync, 1, MINIMUM_NUMBER_OF_PEERS, 2 * AUTHORITY_RETENTION_BLOCKS);

        let genesis = {
            use snarkvm::prelude::FromBytes;
            Block::<CurrentNetwork>::from_bytes_le(CurrentNetwork::genesis_bytes()).unwrap()
        };
        let request = |sync_ips: IndexSet<SocketAddr>, response| OutstandingRequest {
            request: (None, None, sync_ips),
            timestamp: Instant::now(),
            response,
        };

        // An answered request above the gap, even with a redundant peer yet to respond.
        sync.requests.write().insert(6, request([sample_peer_ip(1)].into_iter().collect(), Some(genesis.clone())));
        assert!(sync.sync_stuck_below_peer_floors().is_some());

        // An answered request for the next block.
        sync.requests.write().insert(1, request(IndexSet::new(), Some(genesis)));
        assert!(sync.sync_stuck_below_peer_floors().is_none());
        sync.requests.write().remove(&1);

        // A request still awaiting a peer's response.
        sync.requests.write().insert(7, request([sample_peer_ip(1)].into_iter().collect(), None));
        assert!(sync.sync_stuck_below_peer_floors().is_none());
    }

    /// Tests the stuck-signal lifecycle: never set without peers; set once peers have pruned the
    /// needed blocks, keeping its start while there is no progress; restarted by progress; cleared
    /// when a peer that retains the blocks appears, and when the pruned peers disconnect without any
    /// new request round.
    #[test]
    fn test_stuck_signal_lifecycle() {
        let sync = sample_sync_at_height(0);
        assert!(sync.prepare_block_requests().is_empty());
        assert!(sync.sync_stuck_below_peer_floors().is_none());

        add_peers(&sync, 1, MINIMUM_NUMBER_OF_PEERS, 2 * AUTHORITY_RETENTION_BLOCKS);
        assert!(sync.sync_stuck_below_peer_floors().is_some());
        std::thread::sleep(Duration::from_millis(20));
        let stuck_for = sync.sync_stuck_below_peer_floors().unwrap();
        assert!(stuck_for >= Duration::from_millis(20), "the window kept its start");

        // Progress restarts the window.
        sync.set_sync_height(1);
        assert!(sync.sync_stuck_below_peer_floors().unwrap() < stuck_for);

        // A peer that retains the needed blocks clears the signal.
        let retaining_peer = sample_peer_ip(MINIMUM_NUMBER_OF_PEERS as u16 + 1);
        sync.update_peer_locators(retaining_peer, &sample_block_locators(AUTHORITY_RETENTION_BLOCKS / 10)).unwrap();
        assert!(!sync.prepare_block_requests().is_empty());
        assert!(sync.sync_stuck_below_peer_floors().is_none());

        // So does the pruned peers disconnecting.
        sync.remove_peer(&retaining_peer);
        assert!(sync.sync_stuck_below_peer_floors().is_some());
        for id in 1..=MINIMUM_NUMBER_OF_PEERS as u16 {
            sync.remove_peer(&sample_peer_ip(id));
        }
        assert!(sync.sync_stuck_below_peer_floors().is_none());
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
