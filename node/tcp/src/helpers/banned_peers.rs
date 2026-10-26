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

use std::{collections::HashMap, net::IpAddr, time::Instant};

#[cfg(feature = "locktick")]
use locktick::parking_lot::RwLock;
#[cfg(not(feature = "locktick"))]
use parking_lot::RwLock;

/// Contains the ban details for a banned peer.
#[derive(Clone)]
pub struct BanDetails {
    /// The time when the ban was created.
    banned_at: Instant,
}

impl BanDetails {
    /// Creates a new ban at the given time.
    pub fn new() -> Self {
        Self { banned_at: Instant::now() }
    }
}

impl Default for BanDetails {
    fn default() -> Self {
        Self::new()
    }
}

/// Contains the set of peers currently banned by IP.
#[derive(Default)]
pub struct BannedPeers(RwLock<HashMap<IpAddr, BanDetails>>);

impl BannedPeers {
    /// Check whether the given IP address is currently banned.
    pub fn is_ip_banned(&self, ip: &IpAddr) -> bool {
        self.0.read().contains_key(ip)
    }

    /// Get all banned IPs.
    pub fn get_banned_ips(&self) -> Vec<IpAddr> {
        self.0.read().keys().cloned().collect()
    }

    /// Get ban config for the given IP address.
    pub fn get_ban_config(&self, ip: IpAddr) -> Option<BanDetails> {
        self.0.read().get(&ip).cloned()
    }

    /// Insert or update a banned IP.
    pub fn update_ip_ban(&self, ip: IpAddr) {
        self.0.write().insert(ip, BanDetails::default());
    }

    /// Remove the expired entries
    pub fn remove_old_bans(&self, ban_time_in_secs: u64) {
        self.0.write().retain(|_, ban_config| ban_config.banned_at.elapsed().as_secs() < ban_time_in_secs);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::{
        net::{Ipv4Addr, Ipv6Addr},
        str::FromStr,
        thread::sleep,
        time::Duration,
    };

    /// A ban window long enough that nothing expires during a test.
    const NEVER_EXPIRES: u64 = u64::MAX;

    fn ipv4(last_octet: u8) -> IpAddr {
        IpAddr::V4(Ipv4Addr::new(1, 2, 3, last_octet))
    }

    #[test]
    fn an_ip_starts_out_unbanned() {
        let banned_peers = BannedPeers::default();

        assert!(!banned_peers.is_ip_banned(&ipv4(1)));
        assert!(banned_peers.get_ban_config(ipv4(1)).is_none());
        assert!(banned_peers.get_banned_ips().is_empty());
    }

    #[test]
    fn banning_an_ip_is_visible_through_every_accessor() {
        let banned_peers = BannedPeers::default();
        banned_peers.update_ip_ban(ipv4(1));

        // All three read paths must agree; the ban check and the peer-list filtering in
        // `snarkos-node-network` use different ones.
        assert!(banned_peers.is_ip_banned(&ipv4(1)));
        assert!(banned_peers.get_ban_config(ipv4(1)).is_some());
        assert_eq!(banned_peers.get_banned_ips(), vec![ipv4(1)]);
    }

    #[test]
    fn bans_apply_only_to_the_banned_ip() {
        let banned_peers = BannedPeers::default();
        banned_peers.update_ip_ban(ipv4(1));

        assert!(!banned_peers.is_ip_banned(&ipv4(2)));
        assert!(!banned_peers.is_ip_banned(&IpAddr::V6(Ipv6Addr::LOCALHOST)));
    }

    #[test]
    fn bans_are_keyed_by_the_literal_ip_not_its_canonical_form() {
        let banned_peers = BannedPeers::default();

        // The two spellings of one IPv4 host: the native form, and the IPv4-mapped IPv6 form a
        // dual-stack listener reports.
        let native = ipv4(4);
        let mapped = IpAddr::from_str("::ffff:1.2.3.4").unwrap();

        banned_peers.update_ip_ban(native);

        // Unlike the per-IP *connection* limit, which buckets both spellings together via
        // `canonical_ip`, the ban list keys on the address exactly as it was handed in. A ban
        // therefore only matches lookups that use the same spelling.
        //
        // This matters because bans are imposed from two different address sources: the router's
        // handshake bans the accepted socket's peer address, while `Peering::ip_ban_peer` bans the
        // peer's advertised listener address.
        assert!(banned_peers.is_ip_banned(&native));
        assert!(!banned_peers.is_ip_banned(&mapped));
        assert_eq!(banned_peers.get_banned_ips(), vec![native]);
    }

    #[test]
    fn re_banning_an_ip_refreshes_the_ban_rather_than_duplicating_it() {
        let banned_peers = BannedPeers::default();

        banned_peers.update_ip_ban(ipv4(1));
        let first_ban = banned_peers.get_ban_config(ipv4(1)).unwrap().banned_at;

        sleep(Duration::from_millis(10));
        banned_peers.update_ip_ban(ipv4(1));
        let second_ban = banned_peers.get_ban_config(ipv4(1)).unwrap().banned_at;

        // Re-banning restarts the clock, so a peer that keeps tripping the ban stays banned. It
        // also must not create a second entry for the same IP.
        assert!(second_ban > first_ban);
        assert_eq!(banned_peers.get_banned_ips().len(), 1);
    }

    #[test]
    fn remove_old_bans_retains_unexpired_bans() {
        let banned_peers = BannedPeers::default();
        banned_peers.update_ip_ban(ipv4(1));
        banned_peers.update_ip_ban(ipv4(2));

        banned_peers.remove_old_bans(NEVER_EXPIRES);

        assert!(banned_peers.is_ip_banned(&ipv4(1)));
        assert!(banned_peers.is_ip_banned(&ipv4(2)));
    }

    #[test]
    fn remove_old_bans_evicts_expired_bans() {
        let banned_peers = BannedPeers::default();
        banned_peers.update_ip_ban(ipv4(1));
        banned_peers.update_ip_ban(ipv4(2));

        // A zero-second window expires every ban immediately: the retention check is
        // `elapsed < ban_time_in_secs`, which no entry can satisfy.
        banned_peers.remove_old_bans(0);

        assert!(!banned_peers.is_ip_banned(&ipv4(1)));
        assert!(!banned_peers.is_ip_banned(&ipv4(2)));
        assert!(banned_peers.get_banned_ips().is_empty());
    }

    #[test]
    fn remove_old_bans_evicts_only_the_expired_entries() {
        let banned_peers = BannedPeers::default();

        // An entry banned far enough in the past to fall outside a one-second window...
        banned_peers
            .0
            .write()
            .insert(ipv4(1), BanDetails { banned_at: Instant::now().checked_sub(Duration::from_secs(60)).unwrap() });
        // ...alongside one that was just banned.
        banned_peers.update_ip_ban(ipv4(2));

        banned_peers.remove_old_bans(1);

        assert!(!banned_peers.is_ip_banned(&ipv4(1)));
        assert!(banned_peers.is_ip_banned(&ipv4(2)));
    }

    #[test]
    fn remove_old_bans_is_a_no_op_on_an_empty_list() {
        let banned_peers = BannedPeers::default();

        banned_peers.remove_old_bans(1);

        assert!(banned_peers.get_banned_ips().is_empty());
    }
}
