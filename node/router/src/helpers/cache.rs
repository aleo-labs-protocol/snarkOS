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

use crate::messages::BlockRequest;
use snarkvm::prelude::{Network, puzzle::SolutionID};

use core::hash::Hash;
use linked_hash_map::LinkedHashMap;
#[cfg(feature = "locktick")]
use locktick::parking_lot::RwLock;
#[cfg(not(feature = "locktick"))]
use parking_lot::RwLock;
use std::{
    collections::{HashMap, HashSet, VecDeque},
    net::{IpAddr, SocketAddr},
};
use time::{Duration, OffsetDateTime};

/// The maximum number of items to store in a cache map.
const MAX_CACHE_SIZE: usize = 1 << 17;

/// A helper containing the peer IP and solution ID.
type SolutionKey<N> = (SocketAddr, SolutionID<N>);
/// A helper containing the peer IP and transaction ID.
type TransactionKey<N> = (SocketAddr, <N as Network>::TransactionID);

#[derive(Debug)]
pub struct Cache<N: Network> {
    /// The map of peer connections to their recent timestamps.
    seen_inbound_connections: RwLock<HashMap<IpAddr, VecDeque<OffsetDateTime>>>,
    /// The map of peer IPs to their recent timestamps.
    seen_inbound_messages: RwLock<HashMap<SocketAddr, VecDeque<OffsetDateTime>>>,
    /// The map of peer IPs to their recent timestamps.
    seen_inbound_puzzle_requests: RwLock<HashMap<SocketAddr, VecDeque<OffsetDateTime>>>,
    /// The map of peer IPs to their recent timestamps.
    seen_inbound_block_requests: RwLock<HashMap<SocketAddr, VecDeque<OffsetDateTime>>>,
    /// The map of peer IPs to their recent unconfirmed solution timestamps.
    seen_inbound_unconfirmed_solutions: RwLock<HashMap<SocketAddr, VecDeque<OffsetDateTime>>>,
    /// The map of solution IDs to their last seen timestamp.
    seen_inbound_solutions: RwLock<LinkedHashMap<SolutionKey<N>, OffsetDateTime>>,
    /// The map of transaction IDs to their last seen timestamp.
    seen_inbound_transactions: RwLock<LinkedHashMap<TransactionKey<N>, OffsetDateTime>>,
    /// The map of peer IPs to their block requests.
    seen_outbound_block_requests: RwLock<HashMap<SocketAddr, HashSet<BlockRequest>>>,
    /// The map of peer IPs to the number of puzzle requests.
    seen_outbound_puzzle_requests: RwLock<HashMap<SocketAddr, u32>>,
    /// The map of solution IDs to their last seen timestamp.
    seen_outbound_solutions: RwLock<LinkedHashMap<SolutionKey<N>, OffsetDateTime>>,
    /// The map of transaction IDs to their last seen timestamp.
    seen_outbound_transactions: RwLock<LinkedHashMap<TransactionKey<N>, OffsetDateTime>>,
    /// The map of peer IPs to the number of sent peer requests.
    seen_outbound_peer_requests: RwLock<HashMap<SocketAddr, u32>>,
}

impl<N: Network> Default for Cache<N> {
    /// Initializes a new instance of the cache.
    fn default() -> Self {
        Self::new()
    }
}

impl<N: Network> Cache<N> {
    const INBOUND_BLOCK_REQUEST_INTERVAL: i64 = 60;
    const INBOUND_PUZZLE_REQUEST_INTERVAL: i64 = 60;
    const INBOUND_UNCONFIRMED_SOLUTION_INTERVAL: i64 = 60;

    /// Initializes a new instance of the cache.
    pub fn new() -> Self {
        Self {
            seen_inbound_connections: Default::default(),
            seen_inbound_messages: Default::default(),
            seen_inbound_puzzle_requests: Default::default(),
            seen_inbound_block_requests: Default::default(),
            seen_inbound_unconfirmed_solutions: Default::default(),
            seen_inbound_solutions: RwLock::new(LinkedHashMap::with_capacity(MAX_CACHE_SIZE)),
            seen_inbound_transactions: RwLock::new(LinkedHashMap::with_capacity(MAX_CACHE_SIZE)),
            seen_outbound_block_requests: Default::default(),
            seen_outbound_puzzle_requests: Default::default(),
            seen_outbound_solutions: RwLock::new(LinkedHashMap::with_capacity(MAX_CACHE_SIZE)),
            seen_outbound_transactions: RwLock::new(LinkedHashMap::with_capacity(MAX_CACHE_SIZE)),
            seen_outbound_peer_requests: Default::default(),
        }
    }
}

impl<N: Network> Cache<N> {
    /// Inserts a new timestamp for the given peer connection, returning the number of recent connection requests.
    pub fn insert_inbound_connection(&self, peer_ip: IpAddr, interval_in_secs: i64) -> usize {
        Self::retain_and_insert(&self.seen_inbound_connections, peer_ip, interval_in_secs)
    }

    /// Inserts a new timestamp for the given peer message, returning the number of recent messages.
    pub fn insert_inbound_message(&self, peer_ip: SocketAddr, interval_in_secs: i64) -> usize {
        Self::retain_and_insert(&self.seen_inbound_messages, peer_ip, interval_in_secs)
    }

    /// Inserts a new timestamp for the given peer IP, returning the number of recent requests.
    pub fn insert_inbound_puzzle_request(&self, peer_ip: SocketAddr) -> usize {
        Self::retain_and_insert(&self.seen_inbound_puzzle_requests, peer_ip, Self::INBOUND_PUZZLE_REQUEST_INTERVAL)
    }

    /// Inserts a new timestamp for the given peer IP, returning the number of recent block requests.
    pub fn insert_inbound_block_request(&self, peer_ip: SocketAddr) -> usize {
        Self::retain_and_insert(&self.seen_inbound_block_requests, peer_ip, Self::INBOUND_BLOCK_REQUEST_INTERVAL)
    }

    /// Inserts a new timestamp for the given peer IP, returning the number of recent unconfirmed solutions.
    pub fn insert_inbound_unconfirmed_solution(&self, peer_ip: SocketAddr) -> usize {
        Self::retain_and_insert(
            &self.seen_inbound_unconfirmed_solutions,
            peer_ip,
            Self::INBOUND_UNCONFIRMED_SOLUTION_INTERVAL,
        )
    }

    /// Inserts a solution ID into the cache, returning the previously seen timestamp if it existed.
    pub fn insert_inbound_solution(&self, peer_ip: SocketAddr, solution_id: SolutionID<N>) -> Option<OffsetDateTime> {
        Self::refresh_and_insert(&self.seen_inbound_solutions, (peer_ip, solution_id))
    }

    /// Inserts a transaction ID into the cache, returning the previously seen timestamp if it existed.
    pub fn insert_inbound_transaction(
        &self,
        peer_ip: SocketAddr,
        transaction: N::TransactionID,
    ) -> Option<OffsetDateTime> {
        Self::refresh_and_insert(&self.seen_inbound_transactions, (peer_ip, transaction))
    }
}

impl<N: Network> Cache<N> {
    /// Returns `true` if the cache contains any inbound block requests for the given peer.
    pub fn contains_inbound_block_request(&self, peer_ip: &SocketAddr) -> bool {
        Self::retain(&self.seen_inbound_block_requests, *peer_ip, Self::INBOUND_BLOCK_REQUEST_INTERVAL) > 0
    }

    /// Returns the number of recent block requests for the given peer.
    pub fn num_outbound_block_requests(&self, peer_ip: &SocketAddr) -> usize {
        self.seen_outbound_block_requests.read().get(peer_ip).map(|r| r.len()).unwrap_or(0)
    }

    /// Returns `true` if the cache contains the given block request for the specified peer.
    pub fn contains_outbound_block_request(&self, peer_ip: &SocketAddr, request: &BlockRequest) -> bool {
        self.seen_outbound_block_requests.read().get(peer_ip).map(|r| r.contains(request)).unwrap_or(false)
    }

    /// Inserts the block request for the given peer IP, returning the number of recent requests.
    pub fn insert_outbound_block_request(&self, peer_ip: SocketAddr, request: BlockRequest) -> usize {
        let mut map_write = self.seen_outbound_block_requests.write();
        let requests = map_write.entry(peer_ip).or_default();
        requests.insert(request);
        requests.len()
    }

    /// Removes the block request for the given peer IP, returning `true` if the request was present.
    pub fn remove_outbound_block_request(&self, peer_ip: SocketAddr, request: &BlockRequest) -> bool {
        let mut map_write = self.seen_outbound_block_requests.write();
        if let Some(requests) = map_write.get_mut(&peer_ip) { requests.remove(request) } else { false }
    }

    /// Returns `true` if the cache contains a puzzle request from the given peer.
    pub fn contains_outbound_puzzle_request(&self, peer_ip: &SocketAddr) -> bool {
        self.seen_outbound_puzzle_requests.read().get(peer_ip).map(|r| *r > 0).unwrap_or(false)
    }

    /// Increment the peer IP's number of puzzle requests, returning the updated number of puzzle requests.
    pub fn increment_outbound_puzzle_requests(&self, peer_ip: SocketAddr) -> u32 {
        Self::increment_counter(&self.seen_outbound_puzzle_requests, peer_ip)
    }

    /// Decrement the peer IP's number of puzzle requests, returning the updated number of puzzle requests.
    pub fn decrement_outbound_puzzle_requests(&self, peer_ip: SocketAddr) -> u32 {
        Self::decrement_counter(&self.seen_outbound_puzzle_requests, peer_ip)
    }

    /// Inserts a solution ID into the cache, returning the previously seen timestamp if it existed.
    pub fn insert_outbound_solution(&self, peer_ip: SocketAddr, solution_id: SolutionID<N>) -> Option<OffsetDateTime> {
        Self::refresh_and_insert(&self.seen_outbound_solutions, (peer_ip, solution_id))
    }

    /// Inserts a transaction ID into the cache, returning the previously seen timestamp if it existed.
    pub fn insert_outbound_transaction(
        &self,
        peer_ip: SocketAddr,
        transaction: N::TransactionID,
    ) -> Option<OffsetDateTime> {
        Self::refresh_and_insert(&self.seen_outbound_transactions, (peer_ip, transaction))
    }

    /// Returns `true` if the cache contains a peer request from the given peer.
    pub fn contains_outbound_peer_request(&self, peer_ip: SocketAddr) -> bool {
        self.seen_outbound_peer_requests.read().get(&peer_ip).map(|r| *r > 0).unwrap_or(false)
    }

    /// Increment the peer IP's number of peer requests, returning the updated number of peer requests.
    pub fn increment_outbound_peer_requests(&self, peer_ip: SocketAddr) -> u32 {
        Self::increment_counter(&self.seen_outbound_peer_requests, peer_ip)
    }

    /// Decrement the peer IP's number of peer requests, returning the updated number of peer requests.
    pub fn decrement_outbound_peer_requests(&self, peer_ip: SocketAddr) -> u32 {
        Self::decrement_counter(&self.seen_outbound_peer_requests, peer_ip)
    }

    /// Removes the given peer's outbound block-request entries.
    ///
    /// Other per-peer caches are not pruned here; their stale entries are reclaimed
    /// periodically by [`Cache::clear_stale_entries`].
    pub fn clear_peer_entries(&self, peer_ip: SocketAddr) {
        self.seen_outbound_block_requests.write().remove(&peer_ip);
    }

    /// Removes fully-expired entries from the inbound rate-limit caches, bounding their memory growth.
    ///
    /// An entry whose timestamps are *all* older than its interval carries no rate-limit weight, so
    /// dropping it is observationally a no-op — a peer's live limit is never reset, and fresh data is
    /// never evicted. Intended to be called periodically (e.g. from the heartbeat).
    ///
    /// `connection_interval_in_secs` and `message_interval_in_secs` are the windows for the connection
    /// and message caches, which the caller owns rather than this cache.
    pub fn clear_stale_entries(&self, connection_interval_in_secs: i64, message_interval_in_secs: i64) {
        Self::clear_expired_entries(&self.seen_inbound_connections, connection_interval_in_secs);
        Self::clear_expired_entries(&self.seen_inbound_messages, message_interval_in_secs);
        Self::clear_expired_entries(&self.seen_inbound_puzzle_requests, Self::INBOUND_PUZZLE_REQUEST_INTERVAL);
        Self::clear_expired_entries(&self.seen_inbound_block_requests, Self::INBOUND_BLOCK_REQUEST_INTERVAL);
        Self::clear_expired_entries(
            &self.seen_inbound_unconfirmed_solutions,
            Self::INBOUND_UNCONFIRMED_SOLUTION_INTERVAL,
        );
    }
}

impl<N: Network> Cache<N> {
    /// Insert a new timestamp for the given key, returning the number of recent entries.
    fn retain_and_insert<K: Eq + Hash + Clone>(
        map: &RwLock<HashMap<K, VecDeque<OffsetDateTime>>>,
        key: K,
        interval_in_secs: i64,
    ) -> usize {
        // Fetch the current timestamp.
        let now = OffsetDateTime::now_utc();

        let mut map_write = map.write();
        // Load the entry for the key.
        let timestamps = map_write.entry(key).or_default();
        // Insert the new timestamp.
        timestamps.push_back(now);
        // Retain only the timestamps that are within the recent interval.
        while timestamps.front().is_some_and(|t| now - *t > Duration::seconds(interval_in_secs)) {
            timestamps.pop_front();
        }
        // Return the frequency of recent requests.
        timestamps.len()
    }

    /// Returns the number of recent entries.
    fn retain<K: Eq + Hash + Clone>(
        map: &RwLock<HashMap<K, VecDeque<OffsetDateTime>>>,
        key: K,
        interval_in_secs: i64,
    ) -> usize {
        // Fetch the current timestamp.
        let now = OffsetDateTime::now_utc();

        let mut map_write = map.write();
        // Load the entry for the key.
        let timestamps = map_write.entry(key).or_default();
        // Retain only the timestamps that are within the recent interval.
        while timestamps.front().is_some_and(|t| now - *t > Duration::seconds(interval_in_secs)) {
            timestamps.pop_front();
        }
        // Return the frequency of recent requests.
        timestamps.len()
    }

    /// Increments the key's counter in the map, returning the updated counter.
    fn increment_counter<K: Hash + Eq>(map: &RwLock<HashMap<K, u32>>, key: K) -> u32 {
        let mut map_write = map.write();
        // Load the entry for the key, and increment the counter.
        let entry = map_write.entry(key).or_default();
        *entry = entry.saturating_add(1);
        // Return the updated counter.
        *entry
    }

    /// Decrements the key's counter in the map, returning the updated counter.
    fn decrement_counter<K: Copy + Hash + Eq>(map: &RwLock<HashMap<K, u32>>, key: K) -> u32 {
        let mut map_write = map.write();
        // Load the entry for the key, and decrement the counter.
        let entry = map_write.entry(key).or_default();
        let value = entry.saturating_sub(1);
        // If the entry is 0, remove the entry.
        if *entry == 0 {
            map_write.remove(&key);
        } else {
            *entry = value;
        }
        // Return the updated counter.
        value
    }

    /// Updates the map by enforcing the maximum cache size.
    fn refresh<K: Eq + Hash, V>(map: &RwLock<LinkedHashMap<K, V>>) {
        let mut map_write = map.write();
        while map_write.len() >= MAX_CACHE_SIZE {
            map_write.pop_front();
        }
    }

    /// Updates the map by enforcing the maximum cache size, and inserts the given key.
    /// Returns the previously seen timestamp if it existed.
    fn refresh_and_insert<K: Eq + Hash>(
        map: &RwLock<LinkedHashMap<K, OffsetDateTime>>,
        key: K,
    ) -> Option<OffsetDateTime> {
        // Insert the key, and return the previous timestamp if it existed.
        let previous_timestamp = map.write().insert(key, OffsetDateTime::now_utc());
        // Refresh the cache.
        Self::refresh(map);
        // Return the previous timestamp.
        previous_timestamp
    }

    /// Clears expired entries from the map; pops the expired entries from the front of the deque and if the deque is empty, removes the key.
    fn clear_expired_entries<K: Eq + Hash>(map: &RwLock<HashMap<K, VecDeque<OffsetDateTime>>>, interval_in_secs: i64) {
        let mut map_write = map.write();
        let now = OffsetDateTime::now_utc();
        map_write.retain(|_, timestamps| {
            while timestamps.front().is_some_and(|t| now - *t > Duration::seconds(interval_in_secs)) {
                timestamps.pop_front();
            }

            // If the deque is empty, remove the key (returning false to remove the key)
            !timestamps.is_empty()
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use snarkvm::prelude::MainnetV0;

    use std::net::Ipv4Addr;

    type CurrentNetwork = MainnetV0;

    #[test]
    fn test_inbound_block_request() {
        let cache = Cache::<CurrentNetwork>::default();
        let peer_ip = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 1234);

        // Check that the cache is empty.
        assert_eq!(cache.seen_inbound_block_requests.read().len(), 0);

        // Insert a block request..
        assert_eq!(cache.insert_inbound_block_request(peer_ip), 1);

        // Check that the cache contains the block request.
        assert!(cache.contains_inbound_block_request(&peer_ip));

        // Insert another block request for the same peer.
        assert_eq!(cache.insert_inbound_block_request(peer_ip), 2);

        // Check that the cache contains the block requests.
        assert!(cache.contains_inbound_block_request(&peer_ip));
    }

    #[test]
    fn test_inbound_unconfirmed_solution() {
        let cache = Cache::<CurrentNetwork>::default();
        let peer_ip = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 1234);

        assert_eq!(cache.insert_inbound_unconfirmed_solution(peer_ip), 1);
        assert_eq!(cache.insert_inbound_unconfirmed_solution(peer_ip), 2);
    }

    #[test]
    fn test_inbound_solution() {
        let cache = Cache::<CurrentNetwork>::default();
        let peer_ip = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 1234);
        let solution_id = SolutionID::<CurrentNetwork>::from(123456789);

        // Check that the cache is empty.
        assert_eq!(cache.seen_inbound_solutions.read().len(), 0);

        // Insert a solution.
        assert!(cache.insert_inbound_solution(peer_ip, solution_id).is_none());

        // Check that the cache contains the solution.
        assert_eq!(cache.seen_inbound_solutions.read().len(), 1);

        // Insert the same solution again.
        assert!(cache.insert_inbound_solution(peer_ip, solution_id).is_some());

        // Check that the cache still contains the solution.
        assert_eq!(cache.seen_inbound_solutions.read().len(), 1);
    }

    #[test]
    fn test_inbound_transaction() {
        let cache = Cache::<CurrentNetwork>::default();
        let peer_ip = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 1234);
        let transaction = Default::default();

        // Check that the cache is empty.
        assert_eq!(cache.seen_inbound_transactions.read().len(), 0);

        // Insert a transaction.
        assert!(cache.insert_inbound_transaction(peer_ip, transaction).is_none());

        // Check that the cache contains the transaction.
        assert_eq!(cache.seen_inbound_transactions.read().len(), 1);

        // Insert the same transaction again.
        assert!(cache.insert_inbound_transaction(peer_ip, transaction).is_some());

        // Check that the cache still contains the transaction.
        assert_eq!(cache.seen_inbound_transactions.read().len(), 1);
    }

    #[test]
    fn test_outbound_solution() {
        let cache = Cache::<CurrentNetwork>::default();
        let peer_ip = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 1234);
        let solution_id = SolutionID::<CurrentNetwork>::from(123456789);

        // Check that the cache is empty.
        assert_eq!(cache.seen_outbound_solutions.read().len(), 0);

        // Insert a solution.
        assert!(cache.insert_outbound_solution(peer_ip, solution_id).is_none());

        // Check that the cache contains the solution.
        assert_eq!(cache.seen_outbound_solutions.read().len(), 1);

        // Insert the same solution again.
        assert!(cache.insert_outbound_solution(peer_ip, solution_id).is_some());

        // Check that the cache still contains the solution.
        assert_eq!(cache.seen_outbound_solutions.read().len(), 1);
    }

    #[test]
    fn test_outbound_transaction() {
        let cache = Cache::<CurrentNetwork>::default();
        let peer_ip = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 1234);
        let transaction = Default::default();

        // Check that the cache is empty.
        assert_eq!(cache.seen_outbound_transactions.read().len(), 0);

        // Insert a transaction.
        assert!(cache.insert_outbound_transaction(peer_ip, transaction).is_none());

        // Check that the cache contains the transaction.
        assert_eq!(cache.seen_outbound_transactions.read().len(), 1);

        // Insert the same transaction again.
        assert!(cache.insert_outbound_transaction(peer_ip, transaction).is_some());

        // Check that the cache still contains the transaction.
        assert_eq!(cache.seen_outbound_transactions.read().len(), 1);
    }

    #[test]
    fn test_outbound_peer_request() {
        let cache = Cache::<CurrentNetwork>::default();
        let peer_ip = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 1234);

        // Check the cache is empty.
        assert!(cache.seen_outbound_peer_requests.read().is_empty());
        assert!(!cache.contains_outbound_peer_request(peer_ip));

        // Increment the peer requests.
        assert_eq!(cache.increment_outbound_peer_requests(peer_ip), 1);

        // Check the cache contains the peer request.
        assert!(cache.contains_outbound_peer_request(peer_ip));

        // Increment the peer requests again for the same peer IP.
        assert_eq!(cache.increment_outbound_peer_requests(peer_ip), 2);

        // Check the cache still contains the peer request.
        assert!(cache.contains_outbound_peer_request(peer_ip));

        // Decrement the peer requests.
        assert_eq!(cache.decrement_outbound_peer_requests(peer_ip), 1);

        // Decrement the peer requests again.
        assert_eq!(cache.decrement_outbound_peer_requests(peer_ip), 0);

        // Check the cache is empty.
        assert!(!cache.contains_outbound_peer_request(peer_ip));
    }

    #[test]
    fn test_clear_stale_entries_removes_expired_keys() {
        let cache = Cache::<CurrentNetwork>::default();
        // A timestamp well outside any rate-limit window.
        let old = OffsetDateTime::now_utc() - Duration::seconds(120);

        // seed the cache with 1000 distinct peers, each with a single entry.
        {
            let mut map = cache.seen_inbound_messages.write();
            for port in 1..=1000u16 {
                let peer_ip = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), port);
                map.insert(peer_ip, VecDeque::from([old]));
            }
        }

        // realistic connection/message windows; 120s ≫ 5s so they're all expired
        cache.clear_stale_entries(10, 5);

        // all entries should have been removed, not just emptied
        assert!(cache.seen_inbound_messages.read().is_empty());
    }

    #[test]
    fn test_clear_stale_entries_preserves_fresh_entries() {
        let cache = Cache::<CurrentNetwork>::default();
        let peer_ip = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 1234);

        // Record three recent messages (count climbs to 3).
        assert_eq!(cache.insert_inbound_message(peer_ip, 5), 1);
        assert_eq!(cache.insert_inbound_message(peer_ip, 5), 2);
        assert_eq!(cache.insert_inbound_message(peer_ip, 5), 3);

        // Sweep with realistic windows; these entries are sub-second old — well
        // inside the 5s message window — so they must be retained.
        cache.clear_stale_entries(10, 5);
        // All three timestamps survive: nothing evicted, count not reset.
        assert!(cache.seen_inbound_messages.read().get(&peer_ip).is_some_and(|v| v.len() == 3));
        // One more message, count climbs to 4.
        assert_eq!(cache.insert_inbound_message(peer_ip, 5), 4);
    }

    #[test]
    fn test_clear_stale_entries_trims_expired_but_keeps_active_keys() {
        let cache = Cache::<CurrentNetwork>::default();
        let peer_ip = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 1234);

        // two named timestamps, one expired one not
        let old = OffsetDateTime::now_utc() - Duration::seconds(120);
        let fresh = OffsetDateTime::now_utc();
        {
            let mut map = cache.seen_inbound_messages.write();
            map.insert(peer_ip, VecDeque::from([old, fresh]));
        }

        // with 5s message window, old (120s) is expired -> popped; the loop then hits fresh (not expired) -> stops,
        // the key is non-empty and thus not removed
        cache.clear_stale_entries(10, 5);

        // key kept, trimmed to one entry
        assert!(cache.seen_inbound_messages.read().get(&peer_ip).is_some_and(|v| v.len() == 1));
        // and the only entry is the fresh one
        assert!(
            cache.seen_inbound_messages.read().get(&peer_ip).is_some_and(|v| v.front().is_some_and(|t| *t == fresh))
        );
    }

    #[test]
    fn test_clear_stale_entries_sweeps_all_inbound_maps() {
        let cache = Cache::<CurrentNetwork>::default();
        let peer_ip = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 1234);
        let old = OffsetDateTime::now_utc() - Duration::seconds(120);
        {
            let mut map = cache.seen_inbound_messages.write();
            map.insert(peer_ip, VecDeque::from([old]));
            let mut map = cache.seen_inbound_block_requests.write();
            map.insert(peer_ip, VecDeque::from([old]));
            let mut map = cache.seen_inbound_puzzle_requests.write();
            map.insert(peer_ip, VecDeque::from([old]));
            let mut map = cache.seen_inbound_unconfirmed_solutions.write();
            map.insert(peer_ip, VecDeque::from([old]));
            let mut map = cache.seen_inbound_connections.write();
            map.insert(peer_ip.ip(), VecDeque::from([old]));
        }

        assert_eq!(cache.seen_inbound_messages.read().len(), 1);
        assert_eq!(cache.seen_inbound_block_requests.read().len(), 1);
        assert_eq!(cache.seen_inbound_puzzle_requests.read().len(), 1);
        assert_eq!(cache.seen_inbound_unconfirmed_solutions.read().len(), 1);
        assert_eq!(cache.seen_inbound_connections.read().len(), 1);

        // 120s is older than every window of (10, 5) and the internal Self::INBOUND_*_INTERVAL constants (60). So all 5 must reap.
        cache.clear_stale_entries(10, 5);

        // assert that all maps are empty
        assert!(cache.seen_inbound_messages.read().is_empty());
        assert!(cache.seen_inbound_block_requests.read().is_empty());
        assert!(cache.seen_inbound_puzzle_requests.read().is_empty());
        assert!(cache.seen_inbound_unconfirmed_solutions.read().is_empty());
        assert!(cache.seen_inbound_connections.read().is_empty());
    }
}
