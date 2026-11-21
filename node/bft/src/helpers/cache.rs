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

use crate::events::BlockRequest;
use snarkvm::{console::types::Field, ledger::narwhal::TransmissionID, prelude::Network};

use core::hash::Hash;
use snarkos_utilities::locks::parking_lot::RwLock;
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    net::{IpAddr, SocketAddr},
};
use time::OffsetDateTime;

#[derive(Debug)]
pub struct Cache<N: Network> {
    /// The ordered timestamp map of peer connections and cache hits.
    seen_inbound_connections: RwLock<BTreeMap<i64, HashMap<IpAddr, u32>>>,
    /// The ordered timestamp map of peer IPs and cache hits.
    seen_inbound_events: RwLock<BTreeMap<i64, HashMap<SocketAddr, u32>>>,
    /// The ordered timestamp map of certificate IDs and cache hits.
    seen_inbound_certificates: RwLock<BTreeMap<i64, HashMap<Field<N>, u32>>>,
    /// The ordered timestamp map of transmission IDs and cache hits.
    seen_inbound_transmissions: RwLock<BTreeMap<i64, HashMap<TransmissionID<N>, u32>>>,
    /// The ordered timestamp map of inbound block requests and cache hits.
    seen_inbound_block_requests: RwLock<BTreeMap<i64, HashMap<SocketAddr, u32>>>,
    /// The ordered timestamp map of peer IPs and their cache hits on outbound events.
    seen_outbound_events: RwLock<BTreeMap<i64, HashMap<SocketAddr, u32>>>,
    /// The ordered timestamp map of peer IPs and their cache hits on certificate requests.
    seen_outbound_certificates: RwLock<BTreeMap<i64, HashMap<SocketAddr, u32>>>,
    /// The ordered timestamp map of peer IPs and their cache hits on transmission requests.
    seen_outbound_transmissions: RwLock<BTreeMap<i64, HashMap<SocketAddr, u32>>>,
    /// The map of IPs to the number of validators requests.
    seen_outbound_validators_requests: RwLock<HashMap<SocketAddr, u32>>,
    /// The ordered timestamp map of outbound block requests and cache hits.
    seen_outbound_block_requests: RwLock<HashMap<SocketAddr, HashSet<BlockRequest>>>,
}

impl<N: Network> Default for Cache<N> {
    /// Initializes a new instance of the cache.
    fn default() -> Self {
        Self::new()
    }
}

impl<N: Network> Cache<N> {
    /// Initializes a new instance of the cache.
    pub fn new() -> Self {
        Self {
            seen_inbound_connections: Default::default(),
            seen_inbound_events: Default::default(),
            seen_inbound_certificates: Default::default(),
            seen_inbound_transmissions: Default::default(),
            seen_inbound_block_requests: Default::default(),
            seen_outbound_events: Default::default(),
            seen_outbound_certificates: Default::default(),
            seen_outbound_transmissions: Default::default(),
            seen_outbound_validators_requests: Default::default(),
            seen_outbound_block_requests: Default::default(),
        }
    }
}

impl<N: Network> Cache<N> {
    /// Inserts a new timestamp for the given peer connection, returning the number of recent connection requests.
    pub fn insert_inbound_connection(&self, peer_ip: IpAddr, interval_in_secs: i64) -> usize {
        Self::retain_and_insert(&self.seen_inbound_connections, peer_ip, interval_in_secs)
    }

    /// Inserts a new timestamp for the given peer, returning the number of recent events.
    pub fn insert_inbound_event(&self, peer_ip: SocketAddr, interval_in_secs: i64) -> usize {
        Self::retain_and_insert(&self.seen_inbound_events, peer_ip, interval_in_secs)
    }

    /// Inserts a certificate ID into the cache, returning the number of recent events.
    pub fn insert_inbound_certificate(&self, key: Field<N>, interval_in_secs: i64) -> usize {
        Self::retain_and_insert(&self.seen_inbound_certificates, key, interval_in_secs)
    }

    /// Inserts a transmission ID into the cache, returning the number of recent events.
    pub fn insert_inbound_transmission(&self, key: TransmissionID<N>, interval_in_secs: i64) -> usize {
        Self::retain_and_insert(&self.seen_inbound_transmissions, key, interval_in_secs)
    }

    /// Inserts a block request into the cache, returning the number of recent events.
    pub fn insert_inbound_block_request(&self, key: SocketAddr, interval_in_secs: i64) -> usize {
        Self::retain_and_insert(&self.seen_inbound_block_requests, key, interval_in_secs)
    }
}

impl<N: Network> Cache<N> {
    /// Inserts a new timestamp for the given peer, returning the number of recent events.
    pub fn insert_outbound_event(&self, peer_ip: SocketAddr, interval_in_secs: i64) -> usize {
        Self::retain_and_insert(&self.seen_outbound_events, peer_ip, interval_in_secs)
    }

    /// Inserts a new timestamp for the given peer, returning the number of recent events.
    pub fn insert_outbound_certificate(&self, peer_ip: SocketAddr, interval_in_secs: i64) -> usize {
        Self::retain_and_insert(&self.seen_outbound_certificates, peer_ip, interval_in_secs)
    }

    /// Inserts a new timestamp for the given peer, returning the number of recent events.
    pub fn insert_outbound_transmission(&self, peer_ip: SocketAddr, interval_in_secs: i64) -> usize {
        Self::retain_and_insert(&self.seen_outbound_transmissions, peer_ip, interval_in_secs)
    }
}

impl<N: Network> Cache<N> {
    /// Returns `true` if the cache contains a validators request from the given IP.
    pub fn contains_outbound_validators_request(&self, peer_ip: SocketAddr) -> bool {
        self.seen_outbound_validators_requests.read().get(&peer_ip).map(|r| *r > 0).unwrap_or(false)
    }

    /// Increment the IP's number of validators requests, returning the updated number of validators requests.
    pub fn increment_outbound_validators_requests(&self, peer_ip: SocketAddr) -> u32 {
        Self::increment_counter(&self.seen_outbound_validators_requests, peer_ip)
    }

    /// Decrement the IP's number of validators requests, returning the updated number of validators requests.
    pub fn decrement_outbound_validators_requests(&self, peer_ip: SocketAddr) -> u32 {
        Self::decrement_counter(&self.seen_outbound_validators_requests, peer_ip)
    }

    /// Clears the the IP's number of validator requests.
    pub fn clear_outbound_validators_requests(&self, peer_ip: SocketAddr) {
        self.seen_outbound_validators_requests.write().remove(&peer_ip);
    }

    /// Inserts the block request for the given peer.
    pub fn insert_outbound_block_request(&self, peer_ip: SocketAddr, request: BlockRequest) {
        self.seen_outbound_block_requests.write().entry(peer_ip).or_default().insert(request);
    }

    /// Removes the block request for the given peer. Returns whether the request was present.
    pub fn remove_outbound_block_request(&self, peer_ip: SocketAddr, request: &BlockRequest) -> bool {
        self.seen_outbound_block_requests
            .write()
            .get_mut(&peer_ip)
            .map(|requests| requests.remove(request))
            .unwrap_or(false)
    }

    /// Clears the peer's number of outbound block requests.
    pub fn clear_outbound_block_requests(&self, peer_ip: SocketAddr) {
        self.seen_outbound_block_requests.write().remove(&peer_ip);
    }
}

impl<N: Network> Cache<N> {
    /// Insert a new timestamp for the given key, returning the number of recent entries.
    fn retain_and_insert<K: Copy + Clone + PartialEq + Eq + Hash>(
        map: &RwLock<BTreeMap<i64, HashMap<K, u32>>>,
        key: K,
        interval_in_secs: i64,
    ) -> usize {
        // Fetch the current timestamp.
        let now = OffsetDateTime::now_utc().unix_timestamp();

        // Get the write lock.
        let mut map_write = map.write();
        // Insert the new timestamp and increment the frequency for the key.
        *map_write.entry(now).or_default().entry(key).or_default() += 1;
        // Calculate the cutoff time for the entries to retain.
        let cutoff = now.saturating_sub(interval_in_secs);
        // Obtain the oldest timestamp from the map; it's guaranteed to exist at this point.
        let (oldest, _) = map_write.first_key_value().unwrap();
        // Track the number of cache hits of the key.
        let mut cache_hits = 0;
        // If the oldest timestamp is above the cutoff value, all the entries can be retained.
        if cutoff <= *oldest {
            for cache_keys in map_write.values() {
                cache_hits += *cache_keys.get(&key).unwrap_or(&0);
            }
        } else {
            // Extract the subtree after interval (i.e. non-expired entries)
            let retained = map_write.split_off(&cutoff);
            // Clear all the expired entries.
            map_write.clear();
            // Reinsert the entries into map and sum the frequency of recent requests for `key` while looping.
            for (time, cache_keys) in retained {
                cache_hits += *cache_keys.get(&key).unwrap_or(&0);
                map_write.insert(time, cache_keys);
            }
        }
        // Return the frequency.
        cache_hits as usize
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use snarkvm::prelude::MainnetV0;

    use std::{net::Ipv4Addr, thread, time::Duration};

    type CurrentNetwork = MainnetV0;

    trait Input {
        fn input() -> Self;
    }

    impl Input for IpAddr {
        fn input() -> Self {
            IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))
        }
    }

    impl Input for SocketAddr {
        fn input() -> Self {
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 1234)
        }
    }

    impl Input for Field<CurrentNetwork> {
        fn input() -> Self {
            Field::from_u8(1)
        }
    }

    impl Input for TransmissionID<CurrentNetwork> {
        fn input() -> Self {
            TransmissionID::Transaction(Default::default(), Default::default())
        }
    }

    const INTERVAL_IN_SECS: i64 = 3;

    macro_rules! test_cache_fields {
        ($($name:ident),*) => {
            $(
                paste::paste! {
                    #[test]
                    fn [<test_seen_ $name s>]() {
                        let cache = Cache::<CurrentNetwork>::default();
                        let input = Input::input();

                        // Check that the cache is empty.
                        assert!(cache.[<seen_ $name s>].read().is_empty());

                        // Insert an input, recent events should be 1.
                        assert_eq!(cache.[<insert_ $name>](input, INTERVAL_IN_SECS), 1);
                        // Wait for 1s so that the next entry doesn't overwrite the first one.
                        thread::sleep(Duration::from_secs(1));
                        // Insert an input, recent events should be 2.
                        assert_eq!(cache.[<insert_ $name>](input, INTERVAL_IN_SECS), 2);
                        // Wait for 1s so that the next entry doesn't overwrite the first one.
                        thread::sleep(Duration::from_secs(1));
                        // Insert an input, recent events should be 3.
                        assert_eq!(cache.[<insert_ $name>](input, INTERVAL_IN_SECS), 3);

                        // Check that the cache contains the input for 3 entries.
                        assert_eq!(cache.[<seen_ $name s>].read().len(), 3);

                        // Insert the input again with a small interval, causing one entry to be removed.
                        cache.[<insert_ $name>](input, 1);
                        // Check that the cache contains the input for 2 entries.
                        assert_eq!(cache.[<seen_ $name s>].read().len(), 2);

                        // Insert the input again with a large interval, causing nothing to be removed.
                        cache.[<insert_ $name>](input, 10);
                        // Check that the cache contains the input for 2 entries.
                        assert_eq!(cache.[<seen_ $name s>].read().len(), 2);

                        // Wait for the input to expire.
                        thread::sleep(Duration::from_secs(INTERVAL_IN_SECS as u64 + 1));

                        // Insert an input again, recent events should be 1.
                        assert_eq!(cache.[<insert_ $name>](input, INTERVAL_IN_SECS), 1);

                        // Check that the cache contains the input for 1 entry.
                        assert_eq!(cache.[<seen_ $name s>].read().len(), 1);

                        // Check that the cache still contains the input.
                        let counts: u32 = cache.[<seen_ $name s>].read().values().map(|hash_map| hash_map.get(&input).unwrap_or(&0)).cloned().sum();
                        assert_eq!(counts, 1);

                        // Check that the cache contains the input and 1 timestamp entry.
                        assert_eq!(cache.[<seen_ $name s>].read().len(), 1);
                    }
                }
            )*
        }
    }

    test_cache_fields! {
       inbound_connection,
       inbound_event,
       inbound_certificate,
       inbound_transmission,
       outbound_event,
       outbound_certificate,
       outbound_transmission
    }

    #[test]
    fn test_seen_outbound_validators_requests() {
        let cache = Cache::<CurrentNetwork>::default();
        let input = Input::input();

        // Check the map is empty.
        assert!(!cache.contains_outbound_validators_request(input));

        // Insert some requests.
        for _ in 0..3 {
            cache.increment_outbound_validators_requests(input);
            assert!(cache.contains_outbound_validators_request(input));
        }

        // Remove a request.
        cache.decrement_outbound_validators_requests(input);
        assert!(cache.contains_outbound_validators_request(input));

        // Clear all requests.
        cache.clear_outbound_validators_requests(input);
        assert!(!cache.contains_outbound_validators_request(input));
    }
}
