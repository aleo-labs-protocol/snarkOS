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

mod codec;
mod handshake;
mod network;

use crate::tcp::{self, Tcp};
use snarkos_account::Account;
use snarkos_node_network::{ConnectionMode, Peer, Resolver};
use snarkos_node_tcp::{P2P, protocols::*};
use snarkos_utilities::SignalHandler;
use snarkvm::{
    ledger::committee::Committee,
    prelude::{Address, Field, Header, Network, PrivateKey, ViewKey},
    synthesizer::Restrictions,
};

#[cfg(feature = "locktick")]
use locktick::{parking_lot::RwLock, tokio::Mutex as TMutex};
#[cfg(not(feature = "locktick"))]
use parking_lot::RwLock;
use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    ops::Deref,
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant},
};
#[cfg(not(feature = "locktick"))]
use tokio::sync::Mutex as TMutex;

#[derive(Clone)]
pub struct BootstrapClient<N: Network>(Arc<InnerBootstrapClient<N>>);

impl<N: Network> Deref for BootstrapClient<N> {
    type Target = Arc<InnerBootstrapClient<N>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

// A tuple holding the validator's Aleo address, and its connection mode.
type KnownValidatorInfo<N> = (Address<N>, ConnectionMode);

pub struct InnerBootstrapClient<N: Network> {
    tcp: Tcp,
    peer_pool: RwLock<HashMap<SocketAddr, Peer<N>>>,
    known_validators: RwLock<HashMap<SocketAddr, KnownValidatorInfo<N>>>,
    resolver: RwLock<Resolver<N>>,
    account: Account<N>,
    genesis_header: Header<N>,
    genesis_hash: N::BlockHash,
    restrictions_id: Field<N>,
    http_client: reqwest::Client,
    latest_committee: TMutex<(HashSet<Address<N>>, Instant)>,
    dev: Option<u16>,
}

impl<N: Network> BootstrapClient<N> {
    // The interval for validator committee refreshes.
    const COMMITTEE_REFRESH_TIME: Duration = Duration::from_secs(20);
    // The maximum amount of time per connection.
    const CONNECTION_LIFETIME: Duration = Duration::from_secs(15);
    // The maximum number of connected peers.
    const MAX_PEERS: u16 = 1_000;

    pub async fn new(
        listener_addr: SocketAddr,
        account: Account<N>,
        genesis_header: Header<N>,
        genesis_hash: N::BlockHash,
        dev: Option<u16>,
    ) -> anyhow::Result<Self> {
        // Initialize the TCP stack.
        let tcp = Tcp::new(tcp::Config::new(listener_addr, Self::MAX_PEERS));
        // Initialize the peer pool.
        let peer_pool = Default::default();
        // Initialize a collection of validators.
        let known_validators = Default::default();
        // Load the restrictions ID.
        let restrictions_id = Restrictions::load()?.restrictions_id();
        // Create a resolver.
        let resolver = Default::default();
        // Create an HTTP client to obtain the current committee.
        let http_client = reqwest::Client::new();
        // Prepare a placeholder committee, ensuring that it's insta-outdated.
        let latest_committee = TMutex::new((Default::default(), Instant::now() - Self::COMMITTEE_REFRESH_TIME));

        // Construct and return the bootstrap client.
        let inner = InnerBootstrapClient {
            tcp,
            peer_pool,
            known_validators,
            resolver,
            account,
            genesis_header,
            genesis_hash,
            restrictions_id,
            http_client,
            latest_committee,
            dev,
        };
        let node = BootstrapClient(Arc::new(inner));

        // Enable the TCP protocols.
        node.enable_handshake().await;
        node.enable_reading().await;
        node.enable_writing().await;
        node.enable_disconnect().await;
        node.enable_on_connect().await;
        // Enable the TCP listener. Note: This must be called after the above protocols.
        node.tcp().enable_listener().await.expect("Failed to enable the TCP listener");

        Ok(node)
    }

    /// Returns the account address of the node.
    pub fn address(&self) -> Address<N> {
        self.account.address()
    }

    /// Returns the account private key of the node.
    pub fn private_key(&self) -> &PrivateKey<N> {
        self.account.private_key()
    }

    /// Returns the account view key of the node.
    pub fn view_key(&self) -> &ViewKey<N> {
        self.account.view_key()
    }

    /// Returns the listener IP address from the connected peer address.
    pub fn resolve_to_listener(&self, connected_addr: SocketAddr) -> Option<SocketAddr> {
        self.resolver.read().get_listener(connected_addr)
    }

    /// Returns `true` if the node is in development mode.
    pub fn is_dev(&self) -> bool {
        self.dev.is_some()
    }

    /// Returns the current validator committee or updates it from the explorer, if
    /// we are capable of obtaining it from the network.
    pub async fn get_or_update_committee(&self) -> anyhow::Result<Option<HashSet<Address<N>>>> {
        // Development testing may include a list of committee Aleo addresses loaded from the environment.
        if cfg!(feature = "test") || self.is_dev() {
            match std::env::var("TEST_COMMITTEE_ADDRS") {
                Ok(aleo_addrs) => {
                    let dev_committee =
                        aleo_addrs.split(',').map(|addr| Address::<N>::from_str(addr).unwrap()).collect();
                    return Ok(Some(dev_committee));
                }
                Err(err) => {
                    warn!("Failed to load committee peers from environment: {err}");
                    return Ok(None);
                }
            }
        }

        let now = Instant::now();
        let (committee, timestamp) = &mut *self.latest_committee.lock().await;
        if now - *timestamp >= Self::COMMITTEE_REFRESH_TIME {
            debug!("Updating the validator committee");
            *timestamp = now;
            let committe_query_addr =
                format!("https://api.explorer.provable.com/v2/{}/committee/latest", N::SHORT_NAME);
            let response = self.http_client.get(committe_query_addr).send().await?;
            debug!("Received response from the explorer: {:?}", response);
            let json = response.text().await?;
            let full_committee = Committee::from_str(&json)?;
            *committee = full_committee.members().keys().copied().collect();
            debug!("The validator committee has {} members now", committee.len());

            Ok(Some(committee.clone()))
        } else {
            Ok(Some(committee.clone()))
        }
    }

    // Return the known addresses of current committee members, or all known
    // validators if the committee info is unavailable.
    pub async fn get_validator_addrs(&self) -> HashMap<SocketAddr, KnownValidatorInfo<N>> {
        // First, collect info on all the validators we had connected to before.
        let mut known_validators = self.known_validators.read().clone();
        // If the committee info is available, prune non-committee members.
        match self.get_or_update_committee().await {
            Ok(Some(committee)) => {
                known_validators.retain(|_, (aleo_addr, _)| committee.contains(aleo_addr));
                known_validators
            }
            Ok(None) => known_validators,
            Err(error) => {
                error!("Couldn't update the validator committee: {error}");
                known_validators
            }
        }
    }

    /// Shuts down the bootstrap client.
    pub async fn shut_down(&self) {
        info!("Shutting down the bootstrap client...");

        // Shut down the low-level network features.
        self.tcp.shut_down().await;
    }

    /// Blocks until a shutdown signal was received or manual shutdown was triggered.
    pub async fn wait_for_signals(&self, handler: &SignalHandler) {
        handler.wait_for_signals().await;

        // If the node is already initialized, then shut it down.
        self.shut_down().await;
    }
}
