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

use super::MAX_BLOCKS_BEHIND;

use std::{cmp::Ordering, time::Instant};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SyncStatus {
    Unsynced, // Never synced or no peers
    Syncing,  // In progress
    Synced,   // Fully synced with peers
}

/// Whether the BFT layer is using fast-sync (outside the GC range) or DAG sync (within GC range).
///
/// This is `None` for nodes without a BFT layer (clients, provers).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BftSyncMode {
    /// Block-based synchronization when outside the GC range.
    /// Certificates are not inserted into the DAG.
    Fast,
    /// DAG-based synchronization when within the GC range.
    /// Certificates are inserted into the DAG and consensus runs normally.
    Dag,
}

#[derive(Clone)]
pub(super) struct SyncState {
    /// The height we synced to already
    /// Note: This can be greater than the current ledger height,
    ///       if blocks are not fully committed yet
    sync_height: u32,
    /// The largest height of a peer's block locator.
    /// Is `None` if we never received a peer locator.
    greatest_peer_height: Option<u32>,
    /// Are we synced?
    /// Allows keeping track of when the sync state changes.
    status: SyncStatus,
    /// Last time the sync state changed
    last_change: Instant,
    /// The BFT sync mode (fast or DAG), set by the BFT layer.
    /// `None` for nodes without a BFT layer (clients, provers).
    bft_sync_mode: Option<BftSyncMode>,
}

impl Default for SyncState {
    fn default() -> Self {
        // `status` is set to `Synced` by default to ensure validators of a newly created chain generate blocks.
        Self {
            sync_height: 0,
            greatest_peer_height: None,
            status: SyncStatus::Synced,
            last_change: Instant::now(),
            bft_sync_mode: None,
        }
    }
}

impl SyncState {
    /// Initialize the sync state at the given height.
    /// Useful, when starting a node that already has blocks in its local storage.
    pub fn new_with_height(height: u32) -> Self {
        Self { sync_height: height, ..Default::default() }
    }

    /// Did we catch up with the greatest known peer height?
    /// This will return false if we never synced from a peer.
    pub fn is_block_synced(&self) -> bool {
        self.status == SyncStatus::Synced
    }

    /// Returns `true` if there a blocks to sync from other nodes.
    /// Returns `false` if the node has fully caught up with the rest of the network.
    pub fn can_issue_new_block_requests(&self) -> bool {
        // Return true if sync state is false even if we there are no known blocks to fetch,
        // because otherwise nodes will never  switch to synced at startup.
        if let Some(num_behind) = self.num_blocks_behind() {
            num_behind > 0
        } else {
            debug!("Cannot block sync: the node has not received block locators yet");
            false
        }
    }

    /// Returns the sync height (this is always greater or equal than the ledger height).
    pub fn get_sync_height(&self) -> u32 {
        self.sync_height
    }

    // Compute the number of blocks that we are behind by.
    // Returns None, if there is no known peer height.
    pub fn num_blocks_behind(&self) -> Option<u32> {
        self.greatest_peer_height.map(|peer_height| peer_height.saturating_sub(self.sync_height))
    }

    /// Returns the greatest block height of any connected peer.
    pub fn get_greatest_peer_height(&self) -> Option<u32> {
        self.greatest_peer_height
    }

    /// Returns the BFT sync mode, or `None` if no BFT layer is attached.
    pub fn get_bft_sync_mode(&self) -> Option<BftSyncMode> {
        self.bft_sync_mode
    }

    /// Sets the BFT sync mode.
    ///
    /// # Returns
    /// The previous BFT sync mode (if any).
    pub fn set_bft_sync_mode(&mut self, mode: BftSyncMode) -> Option<BftSyncMode> {
        let prev = self.bft_sync_mode;
        self.bft_sync_mode = Some(mode);
        prev
    }

    /// Update the height we are synced to.
    /// If the value is lower than the current height, the sync height remains unchanged.
    pub fn set_sync_height(&mut self, sync_height: u32) {
        if sync_height <= self.sync_height {
            return;
        }

        trace!("Sync height increased from {old_height} to {sync_height}", old_height = self.sync_height);
        self.sync_height = sync_height;
        self.update_is_block_synced();
    }

    /// Update the greatest known height of a connected peer.
    pub fn set_greatest_peer_height(&mut self, peer_height: u32) {
        if let Some(old_height) = self.greatest_peer_height {
            match old_height.cmp(&peer_height) {
                Ordering::Equal => return,
                Ordering::Greater => warn!("Greatest peer height reduced from {old_height} to {peer_height}"),
                Ordering::Less => trace!("Greatest peer height increased from {old_height} to {peer_height}"),
            }
        }

        self.greatest_peer_height = Some(peer_height);
        self.update_is_block_synced();
    }

    /// Remove the greatest peer height (used when all peers disconnect).
    pub fn clear_greatest_peer_height(&mut self) {
        // No-op if there is no change.
        if self.greatest_peer_height.is_none() {
            return;
        }

        self.greatest_peer_height = None;
        self.update_is_block_synced();
    }

    /// Updates the state of `is_block_synced` for the sync module.
    fn update_is_block_synced(&mut self) {
        trace!(
            "Updating is_block_synced: greatest_peer_height={greatest_peer:?}, current_height={current}, status={status:?}",
            greatest_peer = self.greatest_peer_height,
            current = self.sync_height,
            status = self.status,
        );

        let num_blocks_behind = self.num_blocks_behind();
        let old_status = self.status;

        // If there are no block locators, we consider ourselves synced.
        // Otherwise, validators will never propose certificates.
        let new_status = match num_blocks_behind {
            Some(num) if num <= MAX_BLOCKS_BEHIND => SyncStatus::Synced,
            Some(_) => SyncStatus::Syncing,
            None => SyncStatus::Unsynced,
        };

        // Return early if the state is unchanged
        if new_status == old_status {
            return;
        }

        // Measure how long sync took.
        let now = Instant::now();
        let elapsed = now.saturating_duration_since(self.last_change).as_secs();

        self.status = new_status;
        self.last_change = now;

        match self.status {
            SyncStatus::Synced => {
                if old_status == SyncStatus::Syncing {
                    let elapsed =
                        if elapsed < 60 { format!("{elapsed} seconds") } else { format!("{} minutes", elapsed / 60) };

                    debug!("Block sync state changed to \"synced\". It took {elapsed} to catch up with the network.");
                } else {
                    // If we move directly from unsynced to synced, it means we connected to a peer with a lower height.
                    // In this case it does not make sense to print how long sync took.
                    debug!("Block sync state changed to \"synced\".");
                }
            }
            SyncStatus::Syncing => {
                // num_blocks_behind should never be None at this point,
                // but we still use `unwrap_or` just in case.
                let behind_msg = num_blocks_behind.map(|n| n.to_string()).unwrap_or("unknown".to_string());

                debug!("Block sync state changed to \"syncing\". We are {behind_msg} blocks behind.");
            }
            SyncStatus::Unsynced => {
                debug!("Block sync state changed to \"unsynced\". Connect more peers to resume block sync.");
            }
        }

        // Update the `IS_SYNCED` metric.
        #[cfg(feature = "metrics")]
        metrics::gauge(metrics::bft::IS_SYNCED, self.status == SyncStatus::Synced);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A peer height that is far enough ahead to put the node out of sync.
    const WAY_AHEAD: u32 = 1_000;

    #[test]
    fn a_fresh_state_is_synced_so_a_new_chain_can_produce_blocks() {
        let state = SyncState::default();

        // This default is load-bearing: a validator on a newly created chain has no peer locators,
        // and would never propose a certificate if it started out unsynced.
        assert_eq!(state.status, SyncStatus::Synced);
        assert!(state.is_block_synced());
        assert_eq!(state.get_sync_height(), 0);
        assert_eq!(state.get_greatest_peer_height(), None);
        assert_eq!(state.get_bft_sync_mode(), None);
    }

    #[test]
    fn new_with_height_starts_synced_at_the_given_height() {
        let state = SyncState::new_with_height(42);

        assert_eq!(state.get_sync_height(), 42);
        assert!(state.is_block_synced());
        assert_eq!(state.get_greatest_peer_height(), None);
    }

    #[test]
    fn num_blocks_behind_is_unknown_until_a_peer_locator_arrives() {
        let mut state = SyncState::new_with_height(10);
        assert_eq!(state.num_blocks_behind(), None);

        state.set_greatest_peer_height(15);
        assert_eq!(state.num_blocks_behind(), Some(5));
    }

    #[test]
    fn num_blocks_behind_saturates_when_the_node_leads_every_peer() {
        let mut state = SyncState::new_with_height(100);
        state.set_greatest_peer_height(90);

        // The node is ahead, not behind by a wrapped-around amount.
        assert_eq!(state.num_blocks_behind(), Some(0));
        assert!(state.is_block_synced());
    }

    #[test]
    fn the_sync_height_only_moves_forward() {
        let mut state = SyncState::new_with_height(10);

        state.set_sync_height(20);
        assert_eq!(state.get_sync_height(), 20);

        // Equal and lower values are ignored; the sync height must not rewind when blocks are
        // still being committed.
        state.set_sync_height(20);
        assert_eq!(state.get_sync_height(), 20);
        state.set_sync_height(5);
        assert_eq!(state.get_sync_height(), 20);
    }

    #[test]
    fn the_greatest_peer_height_may_move_backwards() {
        let mut state = SyncState::default();

        state.set_greatest_peer_height(100);
        assert_eq!(state.get_greatest_peer_height(), Some(100));

        // Unlike the sync height, this one is not monotonic: it tracks the peers we can currently
        // see, so it must be able to fall when the tallest peer disconnects.
        state.set_greatest_peer_height(50);
        assert_eq!(state.get_greatest_peer_height(), Some(50));
    }

    #[test]
    fn the_status_flips_at_the_max_blocks_behind_boundary() {
        let mut state = SyncState::default();

        // Exactly `MAX_BLOCKS_BEHIND` behind still counts as synced...
        state.set_greatest_peer_height(MAX_BLOCKS_BEHIND);
        assert_eq!(state.num_blocks_behind(), Some(MAX_BLOCKS_BEHIND));
        assert_eq!(state.status, SyncStatus::Synced);
        assert!(state.is_block_synced());

        // ...one block further behind does not.
        state.set_greatest_peer_height(MAX_BLOCKS_BEHIND + 1);
        assert_eq!(state.status, SyncStatus::Syncing);
        assert!(!state.is_block_synced());
    }

    #[test]
    fn catching_up_with_the_network_returns_the_state_to_synced() {
        let mut state = SyncState::default();

        state.set_greatest_peer_height(WAY_AHEAD);
        assert_eq!(state.status, SyncStatus::Syncing);

        state.set_sync_height(WAY_AHEAD);
        assert_eq!(state.status, SyncStatus::Synced);
        assert!(state.is_block_synced());
    }

    #[test]
    fn losing_every_peer_marks_the_node_unsynced() {
        let mut state = SyncState::default();
        state.set_greatest_peer_height(WAY_AHEAD);
        state.set_sync_height(WAY_AHEAD);
        assert_eq!(state.status, SyncStatus::Synced);

        state.clear_greatest_peer_height();

        // With no peer height there is nothing to compare against, which is distinct from being
        // caught up.
        assert_eq!(state.get_greatest_peer_height(), None);
        assert_eq!(state.num_blocks_behind(), None);
        assert_eq!(state.status, SyncStatus::Unsynced);
        assert!(!state.is_block_synced());
    }

    #[test]
    fn clearing_an_absent_peer_height_leaves_the_state_alone() {
        let mut state = SyncState::default();

        // The early return matters: without it, a node that never saw a peer would be knocked out
        // of its initial `Synced` state by a routine disconnect sweep.
        state.clear_greatest_peer_height();

        assert_eq!(state.status, SyncStatus::Synced);
        assert!(state.is_block_synced());
    }

    #[test]
    fn no_block_requests_are_issued_before_any_peer_locator_arrives() {
        let state = SyncState::new_with_height(10);

        assert!(!state.can_issue_new_block_requests());
    }

    #[test]
    fn block_requests_are_issued_while_any_blocks_remain() {
        let mut state = SyncState::default();
        state.set_greatest_peer_height(WAY_AHEAD);

        assert!(state.can_issue_new_block_requests());
    }

    #[test]
    fn block_requests_stop_once_the_node_draws_level() {
        let mut state = SyncState::default();
        state.set_greatest_peer_height(WAY_AHEAD);
        state.set_sync_height(WAY_AHEAD);

        assert!(!state.can_issue_new_block_requests());
    }

    #[test]
    fn a_synced_node_still_requests_the_blocks_within_the_tolerance() {
        let mut state = SyncState::default();
        state.set_greatest_peer_height(MAX_BLOCKS_BEHIND);

        // Being "synced" is a tolerance, not a guarantee of equality: the node reports itself
        // synced while still fetching the last block or so.
        assert!(state.is_block_synced());
        assert!(state.can_issue_new_block_requests());
    }

    #[test]
    fn the_bft_sync_mode_reports_the_previous_value_when_set() {
        let mut state = SyncState::default();

        // `None` distinguishes nodes without a BFT layer from those that have not chosen a mode.
        assert_eq!(state.set_bft_sync_mode(BftSyncMode::Fast), None);
        assert_eq!(state.get_bft_sync_mode(), Some(BftSyncMode::Fast));

        assert_eq!(state.set_bft_sync_mode(BftSyncMode::Dag), Some(BftSyncMode::Fast));
        assert_eq!(state.get_bft_sync_mode(), Some(BftSyncMode::Dag));
    }

    #[test]
    fn the_bft_sync_mode_is_independent_of_the_sync_status() {
        let mut state = SyncState::default();
        state.set_bft_sync_mode(BftSyncMode::Dag);

        state.set_greatest_peer_height(WAY_AHEAD);
        assert_eq!(state.status, SyncStatus::Syncing);
        assert_eq!(state.get_bft_sync_mode(), Some(BftSyncMode::Dag));

        state.clear_greatest_peer_height();
        assert_eq!(state.status, SyncStatus::Unsynced);
        assert_eq!(state.get_bft_sync_mode(), Some(BftSyncMode::Dag));
    }
}
