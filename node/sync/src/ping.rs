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

use crate::locators::BlockLocators;
use snarkos_node_router::Router;
use snarkos_utilities::locks::parking_lot::Mutex;
use snarkvm::prelude::Network;

use std::{
    collections::BTreeMap,
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{sync::Notify, time::timeout};

/// Internal state of the ping logic
///
/// Essentially, ping keeps an ordered map `next_ping` of time(rs) to peer IPs.
/// When a new peer connects or a Pong message is received, an entry in next ping is created
/// for when a peer should next be pinged.
///
/// TODO (kaimast): maybe keep track of the last ping too, to not trigger spam detection?
struct PingInner<N: Network> {
    /// The next time we should ping a peer.
    next_ping: BTreeMap<Instant, SocketAddr>,
    /// The most recent block locators.
    /// (or None if this node does not offer block sync)
    block_locators: Option<BlockLocators<N>>,
}

/// Manages sending Ping messages to all connected peers.
pub struct Ping<N: Network> {
    router: Router<N>,
    inner: Arc<Mutex<PingInner<N>>>,
    notify: Arc<Notify>,
}

impl<N: Network> PingInner<N> {
    fn new(block_locators: Option<BlockLocators<N>>) -> Self {
        Self { block_locators, next_ping: Default::default() }
    }
}

impl<N: Network> Ping<N> {
    /// The duration in seconds to wait between sending ping requests to a peer.
    const MAX_PING_INTERVAL: Duration = Duration::from_secs(20);

    /// Create a new instance of the ping logic.
    /// There should only be one per node.
    ///
    /// # Usage
    /// Initialize this with the most up-to-date block locators and call
    /// update_block_locators, whenever a new block is received/created.
    pub fn new(router: Router<N>, block_locators: BlockLocators<N>) -> Self {
        let notify = Arc::new(Notify::default());
        let inner = Arc::new(Mutex::new(PingInner::new(Some(block_locators))));

        {
            let inner = inner.clone();
            let router_ = router.clone();
            let notify = notify.clone();

            router.spawn(async move {
                Self::ping_task(&inner, &router_, &notify).await;
            });
        }

        Self { inner, router, notify }
    }

    /// Same as [`Self::new`] but for nodes that peers cannot sync from
    /// such as provers.
    pub fn new_nosync(router: Router<N>) -> Self {
        let notify = Arc::new(Notify::default());
        let inner = Arc::new(Mutex::new(PingInner::new(None)));

        {
            let inner = inner.clone();
            let router_ = router.clone();
            let notify = notify.clone();

            router.spawn(async move {
                Self::ping_task(&inner, &router_, &notify).await;
            });
        }

        Self { inner, router, notify }
    }

    /// Notify the ping logic that we received a Pong response.
    pub fn on_pong_received(&self, peer_ip: SocketAddr) {
        let now = Instant::now();
        let mut inner = self.inner.lock();

        inner.next_ping.insert(now + Self::MAX_PING_INTERVAL, peer_ip);

        // self.notify.notify() is not needed as ping_task wakes up every MAX_PING_INTERVAL
    }

    /// Notify the ping logic that a new peer connected.
    pub fn on_peer_connected(&self, peer_ip: SocketAddr) {
        // Send the first ping.
        let locators = self.inner.lock().block_locators.clone();
        if !self.router.send_ping(peer_ip, locators, self.router.oldest_retained_height()) {
            warn!("Peer {peer_ip} connected and immediately disconnected?");
        }
    }

    /// Notify the ping logic that new blocks were created or synced.
    pub fn update_block_locators(&self, locators: BlockLocators<N>) {
        self.inner.lock().block_locators = Some(locators);

        // wake up the ping task
        self.notify.notify_one();
    }

    /// Background task that periodically sends out new ping messages.
    async fn ping_task(inner: &Mutex<PingInner<N>>, router: &Router<N>, notify: &Notify) {
        let mut new_block = false;

        loop {
            if router.ledger().is_stopped() {
                break;
            }

            // Do not hold the lock while waiting.
            let sleep_time = {
                let oldest_retained = router.oldest_retained_height();
                let mut inner = inner.lock();
                let now = Instant::now();

                // Ping peers.
                if new_block {
                    Self::ping_all_peers(&mut inner, router, oldest_retained);
                    new_block = false;
                } else {
                    Self::ping_expired_peers(now, &mut inner, router, oldest_retained);
                }

                // Figure out how long to sleep.
                if let Some((time, _)) = inner.next_ping.first_key_value() {
                    time.saturating_duration_since(now)
                } else {
                    Self::MAX_PING_INTERVAL
                }
            };

            // wait to be woke up, either by timer or notify
            if timeout(sleep_time, notify.notified()).await.is_ok() {
                // If the timer is not expired, it means we got woken up by a new block.
                new_block = true;
            }
        }
    }

    /// Ping all peers that have an expired timer.
    fn ping_expired_peers(now: Instant, inner: &mut PingInner<N>, router: &Router<N>, oldest_retained: Option<u32>) {
        loop {
            // Find next peer to contact.
            let peer_ip = {
                let Some((time, peer_ip)) = inner.next_ping.first_key_value() else {
                    return;
                };

                if *time > now {
                    return;
                }

                *peer_ip
            };

            // Send new ping
            let locators = inner.block_locators.clone();
            let success = router.send_ping(peer_ip, locators.clone(), oldest_retained);
            inner.next_ping.pop_first();

            if !success {
                trace!("Failed to send ping to peer {peer_ip}. Disconnected.");
            }
        }
    }

    /// Ping all known peers.
    fn ping_all_peers(inner: &mut PingInner<N>, router: &Router<N>, oldest_retained: Option<u32>) {
        let peers: Vec<SocketAddr> = inner.next_ping.values().copied().collect();
        inner.next_ping.clear();

        for peer_ip in peers {
            let locators = inner.block_locators.clone();
            let success = router.send_ping(peer_ip, locators, oldest_retained);

            if !success {
                trace!("Failed to send ping to peer {peer_ip}. Disconnected.");
            }
        }
    }
}
