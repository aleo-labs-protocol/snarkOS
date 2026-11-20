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

use crate::{Gateway, MAX_FETCH_TIMEOUT};
use snarkos_node_bft_ledger_service::LedgerService;
use snarkos_node_network::PeerPoolHandling;
use snarkos_utilities::locks::parking_lot::RwLock;
use snarkvm::{
    console::network::{Network, consensus_config_value},
    prelude::Result,
};

use anyhow::{anyhow, bail};
use std::{
    collections::{HashMap, HashSet},
    hash::Hash,
    net::SocketAddr,
    sync::Arc,
};
use time::OffsetDateTime;
use tokio::sync::oneshot;

/// The maximum number of seconds to wait before expiring a callback.
/// We ensure that we don't truncate `MAX_FETCH_TIMEOUT` when converting to seconds.
pub(crate) const CALLBACK_EXPIRATION_IN_SECS: i64 = MAX_FETCH_TIMEOUT.as_secs() as i64;

/// Returns the maximum number of redundant requests for the number of validators in the specified round.
pub fn max_redundant_requests<N: Network>(ledger: Arc<dyn LedgerService<N>>, round: u64) -> Result<usize> {
    // Determine the number of validators in the committee lookback for the given round.
    let num_validators =
        if let Ok(n) = ledger.get_committee_lookback_for_round(round).map(|committee| committee.num_members()) {
            n
        } else {
            let max_committee_size = consensus_config_value!(N, MAX_CERTIFICATES, ledger.latest_block_height())
                .ok_or_else(|| anyhow!("Couldn't obtain MAX_CERTIFICATES"))?;
            max_committee_size as usize
        };

    // Note: It is adequate to set this value to the availability threshold,
    // as with high probability one will respond honestly (in the best and worst case
    // with stake spread across the validators evenly and unevenly, respectively).
    Ok(1 + num_validators.saturating_div(3))
}

#[derive(Debug)]
pub struct Pending<T: PartialEq + Eq + Hash, V: Clone> {
    /// The map of pending `items` to a map of `peer IPs` and their optional `callback` queue.
    /// Each callback has a timeout and a flag indicating if it is associated with a sent request.
    pending: RwLock<HashMap<T, HashMap<SocketAddr, Vec<(oneshot::Sender<V>, i64, bool)>>>>,
}

impl<T: Copy + Clone + PartialEq + Eq + Hash, V: Clone> Default for Pending<T, V> {
    /// Initializes a new instance of the pending queue.
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Copy + Clone + PartialEq + Eq + Hash, V: Clone> Pending<T, V> {
    /// Initializes a new instance of the pending queue.
    pub fn new() -> Self {
        Self { pending: Default::default() }
    }

    /// Returns `true` if the pending queue is empty.
    pub fn is_empty(&self) -> bool {
        self.pending.read().is_empty()
    }

    /// Returns the number of pending in the pending queue.
    pub fn len(&self) -> usize {
        self.pending.read().len()
    }

    /// Returns `true` if the pending queue contains the specified `item`.
    pub fn contains(&self, item: impl Into<T>) -> bool {
        self.pending.read().contains_key(&item.into())
    }

    /// Returns `true` if the pending queue contains the specified `item` for the specified `peer IP`.
    pub fn contains_peer(&self, item: impl Into<T>, peer_ip: SocketAddr) -> bool {
        self.pending.read().get(&item.into()).is_some_and(|peer_ips| peer_ips.contains_key(&peer_ip))
    }

    /// Returns `true` if the pending queue contains the specified `item` for the specified `peer IP` with a sent request.
    pub fn contains_peer_with_sent_request(&self, item: impl Into<T>, peer_ip: SocketAddr) -> bool {
        self.pending.read().get(&item.into()).is_some_and(|peer_ips| {
            peer_ips
                .get(&peer_ip)
                .map(|callbacks| callbacks.iter().any(|(_, _, request_sent)| *request_sent))
                .unwrap_or(false)
        })
    }

    /// Returns the peer IPs for the specified `item`.
    pub fn get_peers(&self, item: impl Into<T>) -> Option<HashSet<SocketAddr>> {
        self.pending.read().get(&item.into()).map(|map| map.keys().cloned().collect())
    }

    /// Returns the peer IPs for the specified `item` who have received a request for it.
    pub fn get_sent_peers(&self, item: impl Into<T>) -> Option<HashSet<SocketAddr>> {
        self.pending.read().get(&item.into()).map(|map| {
            map.iter().filter_map(|(addr, cbs)| cbs.iter().any(|(_, _, sent)| *sent).then_some(*addr)).collect()
        })
    }

    /// Returns the number of pending callbacks for the specified `item`.
    pub fn num_callbacks(&self, item: impl Into<T>) -> usize {
        let item = item.into();
        let now = OffsetDateTime::now_utc().unix_timestamp();
        // Clear the callbacks that have expired.
        self.clear_expired_callbacks_for_item(now, item);
        // Return the number of live callbacks.
        self.pending.read().get(&item).map_or(0, |peers| peers.values().fold(0, |acc, v| acc.saturating_add(v.len())))
    }

    /// Returns the number of pending sent requests for the specified `item`.
    pub fn num_sent_requests(&self, item: impl Into<T>) -> usize {
        let item = item.into();
        let now = OffsetDateTime::now_utc().unix_timestamp();
        // Clear the callbacks that have expired.
        self.clear_expired_callbacks_for_item(now, item);
        // Return the number of live callbacks.
        self.pending
            .read()
            .get(&item)
            .map_or(0, |peers| peers.values().flatten().filter(|(_, _, request_sent)| *request_sent).count())
    }

    /// Inserts the specified `item` and `peer IP` to the pending queue,
    /// returning `true` if the `peer IP` was newly-inserted into the entry for the `item`.
    ///
    /// In addition, an optional `callback` may be provided, that is triggered upon removal.
    /// Note: The callback, if provided, is **always** inserted into the callback queue.
    pub fn insert(
        &self,
        item: impl Into<T>,
        peer_ip: SocketAddr,
        callback: Option<(oneshot::Sender<V>, bool)>,
    ) -> bool {
        let item = item.into();
        let now = OffsetDateTime::now_utc().unix_timestamp();
        // Insert the peer IP and optional callback into the pending queue.
        let result = {
            // Acquire the pending lock.
            let mut pending = self.pending.write();

            // Insert a peer into the pending queue.
            let entry = pending.entry(item).or_default();

            // Check if the peer IP is already present in the entry.
            let is_new_peer = !entry.contains_key(&peer_ip);

            // Get the entry for the peer IP.
            let peer_entry = entry.entry(peer_ip).or_default();

            // If a callback is provided, insert it into the callback queue.
            if let Some((callback, request_sent)) = callback {
                peer_entry.push((callback, now, request_sent));
            }

            is_new_peer
        };

        // Clear the callbacks that have expired.
        self.clear_expired_callbacks_for_item(now, item);

        // Return the result.
        result
    }

    /// Removes the specified `item` from the pending queue.
    /// If the `item` exists and is removed, the peer IPs are returned.
    /// If the `item` does not exist, `None` is returned.
    pub fn remove(&self, item: impl Into<T>, callback_value: Option<V>) -> Option<HashSet<SocketAddr>> {
        let item = item.into();
        // Remove the item from the pending queue and process any remaining callbacks.
        match self.pending.write().remove(&item) {
            Some(callbacks) => {
                // Get the peer IPs.
                let peer_ips = callbacks.keys().copied().collect();
                // Process the callbacks.
                if let Some(callback_value) = callback_value {
                    // Send a notification to the callback.
                    for (callback, _, _) in callbacks.into_values().flat_map(|callbacks| callbacks.into_iter()) {
                        callback.send(callback_value.clone()).ok();
                    }
                }
                // Return the peer IPs.
                Some(peer_ips)
            }
            None => None,
        }
    }

    /// Removes the callbacks for the specified `item` that have expired.
    pub fn clear_expired_callbacks_for_item(&self, now: i64, item: impl Into<T>) {
        let item = item.into();

        // Acquire the pending lock.
        let mut pending = self.pending.write();

        // Clear the callbacks that have expired.
        if let Some(peer_map) = pending.get_mut(&item) {
            // Iterate over each peer IP for the item and filter out expired callbacks.
            for (_, callbacks) in peer_map.iter_mut() {
                callbacks.retain(|(_, timestamp, _)| now - *timestamp <= CALLBACK_EXPIRATION_IN_SECS);
            }

            // Remove peer IPs that no longer have any callbacks.
            peer_map.retain(|_, callbacks| !callbacks.is_empty());

            // If there are no more remaining callbacks for the item across all peer IPs, remove the item from pending.
            if peer_map.is_empty() {
                pending.remove(&item);
            }
        }
    }

    /// Removes the callbacks for all items have that expired.
    pub fn clear_expired_callbacks(&self) {
        let now = OffsetDateTime::now_utc().unix_timestamp();
        // Acquire the pending lock once for write access.
        let mut pending = self.pending.write();

        // Iterate over all items in pending to modify the data structure in-place.
        pending.retain(|_, peer_map| {
            // Iterate over each peer IP for the item and filter out expired callbacks.
            for (_, callbacks) in peer_map.iter_mut() {
                callbacks.retain(|(_, timestamp, _)| now - *timestamp <= CALLBACK_EXPIRATION_IN_SECS);
            }

            // Remove peer IPs that no longer have any callbacks.
            peer_map.retain(|_, callbacks| !callbacks.is_empty());

            // Keep the item in the pending map only if there are callbacks left.
            !peer_map.is_empty()
        });
    }

    /// Ensures that the combined stake of the peers that have received the request is
    /// greater than the redundancy threshold (33%).
    pub fn request_stake_redundancy_reached<N: Network>(&self, gateway: &Gateway<N>, desired_item: T) -> Result<bool> {
        let committee = match gateway.ledger().current_committee() {
            Ok(c) => c,
            Err(err) => {
                bail!("Failed to get current committee: {err}");
            }
        };

        let total_stake = committee.total_stake();
        let pending_providers = self.get_sent_peers(desired_item);
        let mut stake_of_providers = 0;
        for addr in pending_providers.iter().flatten() {
            let Some(aleo_addr) = gateway.resolve_to_aleo_addr(*addr) else { continue };
            let individual_stake = committee.get_stake(aleo_addr);
            stake_of_providers += individual_stake;
            if stake_of_providers as f64 / total_stake as f64 > 0.33 {
                return Ok(true);
            }
        }

        Ok(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use snarkvm::{ledger::narwhal::TransmissionID, prelude::TestRng};

    use rand::RngExt;
    use std::{thread, time::Duration};

    type CurrentNetwork = snarkvm::prelude::MainnetV0;

    const ITERATIONS: usize = 100;

    #[test]
    fn test_pending() {
        let rng = &mut TestRng::default();

        // Initialize the ready queue.
        let pending = Pending::<TransmissionID<CurrentNetwork>, ()>::new();

        // Check initially empty.
        assert!(pending.is_empty());
        assert_eq!(pending.len(), 0);

        // Initialize the solution IDs.
        let solution_id_1 = TransmissionID::Solution(
            rng.random::<u64>().into(),
            rng.random::<<CurrentNetwork as Network>::TransmissionChecksum>(),
        );
        let solution_id_2 = TransmissionID::Solution(
            rng.random::<u64>().into(),
            rng.random::<<CurrentNetwork as Network>::TransmissionChecksum>(),
        );
        let solution_id_3 = TransmissionID::Solution(
            rng.random::<u64>().into(),
            rng.random::<<CurrentNetwork as Network>::TransmissionChecksum>(),
        );
        let solution_id_4 = TransmissionID::Solution(
            rng.random::<u64>().into(),
            rng.random::<<CurrentNetwork as Network>::TransmissionChecksum>(),
        );

        // Initialize the SocketAddrs.
        let addr_1 = SocketAddr::from(([127, 0, 0, 1], 1234));
        let addr_2 = SocketAddr::from(([127, 0, 0, 1], 2345));
        let addr_3 = SocketAddr::from(([127, 0, 0, 1], 3456));
        let addr_4 = SocketAddr::from(([127, 0, 0, 1], 4567));

        // Initialize the callbacks.
        let (callback_sender_1, _) = oneshot::channel();
        let (callback_sender_2, _) = oneshot::channel();
        let (callback_sender_3, _) = oneshot::channel();
        let (callback_sender_4, _) = oneshot::channel();

        // Insert the solution IDs.
        assert!(pending.insert(solution_id_1, addr_1, Some((callback_sender_1, true))));
        assert!(pending.insert(solution_id_2, addr_2, Some((callback_sender_2, true))));
        assert!(pending.insert(solution_id_3, addr_3, Some((callback_sender_3, true))));
        // Add a callback without a sent request.
        assert!(pending.insert(solution_id_4, addr_4, Some((callback_sender_4, false))));

        // Check the number of SocketAddrs.
        assert_eq!(pending.len(), 4);
        assert!(!pending.is_empty());

        // Check the items.
        let ids = [solution_id_1, solution_id_2, solution_id_3];
        let peers = [addr_1, addr_2, addr_3];

        for i in 0..3 {
            let id = ids[i];
            assert!(pending.contains(id));
            assert!(pending.contains_peer(id, peers[i]));
            assert!(pending.contains_peer_with_sent_request(id, peers[i]));
        }
        // Ensure the last item does not have a sent request.
        assert!(pending.contains_peer(solution_id_4, addr_4));
        assert!(!pending.contains_peer_with_sent_request(solution_id_4, addr_4));

        let unknown_id = TransmissionID::Solution(
            rng.random::<u64>().into(),
            rng.random::<<CurrentNetwork as Network>::TransmissionChecksum>(),
        );
        assert!(!pending.contains(unknown_id));

        // Check get.
        assert_eq!(pending.get_peers(solution_id_1), Some(HashSet::from([addr_1])));
        assert_eq!(pending.get_peers(solution_id_2), Some(HashSet::from([addr_2])));
        assert_eq!(pending.get_peers(solution_id_3), Some(HashSet::from([addr_3])));
        assert_eq!(pending.get_peers(solution_id_4), Some(HashSet::from([addr_4])));
        assert_eq!(pending.get_peers(unknown_id), None);

        // Check remove.
        assert!(pending.remove(solution_id_1, None).is_some());
        assert!(pending.remove(solution_id_2, None).is_some());
        assert!(pending.remove(solution_id_3, None).is_some());
        assert!(pending.remove(solution_id_4, None).is_some());
        assert!(pending.remove(unknown_id, None).is_none());

        // Check empty again.
        assert!(pending.is_empty());
    }

    #[test]
    fn test_expired_callbacks() {
        let rng = &mut TestRng::default();

        // Initialize the ready queue.
        let pending = Pending::<TransmissionID<CurrentNetwork>, ()>::new();

        // Check initially empty.
        assert!(pending.is_empty());
        assert_eq!(pending.len(), 0);

        // Initialize the solution ID.
        let solution_id_1 = TransmissionID::Solution(
            rng.random::<u64>().into(),
            rng.random::<<CurrentNetwork as Network>::TransmissionChecksum>(),
        );

        // Initialize the SocketAddrs.
        let addr_1 = SocketAddr::from(([127, 0, 0, 1], 1234));
        let addr_2 = SocketAddr::from(([127, 0, 0, 1], 2345));
        let addr_3 = SocketAddr::from(([127, 0, 0, 1], 3456));

        // Initialize the callbacks.
        let (callback_sender_1, _) = oneshot::channel();
        let (callback_sender_2, _) = oneshot::channel();
        let (callback_sender_3, _) = oneshot::channel();

        // Insert the solution ID.
        assert!(pending.insert(solution_id_1, addr_1, Some((callback_sender_1, true))));
        assert!(pending.insert(solution_id_1, addr_2, Some((callback_sender_2, true))));

        // Sleep for a few seconds.
        thread::sleep(Duration::from_secs(CALLBACK_EXPIRATION_IN_SECS as u64 - 1));

        assert!(pending.insert(solution_id_1, addr_3, Some((callback_sender_3, true))));

        // Check that the number of callbacks has not changed.
        assert_eq!(pending.num_callbacks(solution_id_1), 3);

        // Wait for 2 seconds.
        thread::sleep(Duration::from_secs(2));

        // Ensure that the expired callbacks have been removed.
        assert_eq!(pending.num_callbacks(solution_id_1), 1);

        // Wait for ` CALLBACK_EXPIRATION_IN_SECS` seconds.
        thread::sleep(Duration::from_secs(CALLBACK_EXPIRATION_IN_SECS as u64));

        // Ensure that the expired callbacks have been removed.
        assert_eq!(pending.num_callbacks(solution_id_1), 0);
    }

    #[test]
    fn test_num_sent_requests() {
        let rng = &mut TestRng::default();

        // Initialize the ready queue.
        let pending = Pending::<TransmissionID<CurrentNetwork>, ()>::new();

        for _ in 0..ITERATIONS {
            // Generate a solution ID.
            let solution_id = TransmissionID::Solution(
                rng.random::<u64>().into(),
                rng.random::<<CurrentNetwork as Network>::TransmissionChecksum>(),
            );
            // Check if the number of sent requests is correct.
            let mut expected_num_sent_requests = 0;
            for i in 0..ITERATIONS {
                // Generate a peer address.
                let addr = SocketAddr::from(([127, 0, 0, 1], i as u16));
                // Initialize a callback.
                let (callback_sender, _) = oneshot::channel();
                // Randomly determine if the callback is associated with a sent request.
                let is_sent_request = rng.random();
                // Increment the expected number of sent requests.
                if is_sent_request {
                    expected_num_sent_requests += 1;
                }
                // Insert the solution ID.
                assert!(pending.insert(solution_id, addr, Some((callback_sender, is_sent_request))));
            }
            // Ensure that the number of sent requests is correct.
            assert_eq!(pending.num_sent_requests(solution_id), expected_num_sent_requests);
        }
    }

    #[test]
    fn test_expired_items() {
        let rng = &mut TestRng::default();

        // Initialize the ready queue.
        let pending = Pending::<TransmissionID<CurrentNetwork>, ()>::new();

        // Check initially empty.
        assert!(pending.is_empty());
        assert_eq!(pending.len(), 0);

        // Initialize the solution IDs.
        let solution_id_1 = TransmissionID::Solution(
            rng.random::<u64>().into(),
            rng.random::<<CurrentNetwork as Network>::TransmissionChecksum>(),
        );
        let solution_id_2 = TransmissionID::Solution(
            rng.random::<u64>().into(),
            rng.random::<<CurrentNetwork as Network>::TransmissionChecksum>(),
        );

        // Initialize the SocketAddrs.
        let addr_1 = SocketAddr::from(([127, 0, 0, 1], 1234));
        let addr_2 = SocketAddr::from(([127, 0, 0, 1], 2345));
        let addr_3 = SocketAddr::from(([127, 0, 0, 1], 3456));

        // Initialize the callbacks.
        let (callback_sender_1, _) = oneshot::channel();
        let (callback_sender_2, _) = oneshot::channel();
        let (callback_sender_3, _) = oneshot::channel();

        // Insert the commitments.
        assert!(pending.insert(solution_id_1, addr_1, Some((callback_sender_1, true))));
        assert!(pending.insert(solution_id_1, addr_2, Some((callback_sender_2, true))));
        assert!(pending.insert(solution_id_2, addr_3, Some((callback_sender_3, true))));

        // Ensure that the items have not been expired yet.
        assert_eq!(pending.num_callbacks(solution_id_1), 2);
        assert_eq!(pending.num_callbacks(solution_id_2), 1);
        assert_eq!(pending.len(), 2);

        // Wait for ` CALLBACK_EXPIRATION_IN_SECS + 1` seconds.
        thread::sleep(Duration::from_secs(CALLBACK_EXPIRATION_IN_SECS as u64 + 1));

        // Expire the pending callbacks.
        pending.clear_expired_callbacks();

        // Ensure that the items have been expired.
        assert_eq!(pending.num_callbacks(solution_id_1), 0);
        assert_eq!(pending.num_callbacks(solution_id_2), 0);
        assert!(pending.is_empty());
    }
}

#[cfg(test)]
mod prop_tests {
    use super::*;

    use test_strategy::{Arbitrary, proptest};

    #[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
    pub struct Item {
        pub id: usize,
    }

    #[derive(Arbitrary, Clone, Debug)]
    pub struct PendingInput {
        #[strategy(1..5_000usize)]
        pub count: usize,
    }

    impl PendingInput {
        pub fn to_pending(&self) -> Pending<Item, ()> {
            let pending = Pending::<Item, ()>::new();
            for i in 0..self.count {
                pending.insert(
                    Item { id: i },
                    SocketAddr::from(([127, 0, 0, 1], i as u16)),
                    Some((oneshot::channel().0, true)),
                );
            }
            pending
        }
    }

    #[proptest]
    fn test_pending_proptest(input: PendingInput) {
        let pending = input.to_pending();
        assert_eq!(pending.len(), input.count);
        assert!(!pending.is_empty());
        assert!(!pending.contains(Item { id: input.count + 1 }));
        assert_eq!(pending.get_peers(Item { id: input.count + 1 }), None);
        assert!(pending.remove(Item { id: input.count + 1 }, None).is_none());
        for i in 0..input.count {
            assert!(pending.contains(Item { id: i }));
            let peer_ip = SocketAddr::from(([127, 0, 0, 1], i as u16));
            assert!(pending.contains_peer(Item { id: i }, peer_ip));
            assert_eq!(pending.get_peers(Item { id: i }), Some(HashSet::from([peer_ip])));
            assert!(pending.remove(Item { id: i }, None).is_some());
        }
        assert!(pending.is_empty());
    }
}
