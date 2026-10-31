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

use crate::common::{
    CurrentNetwork,
    TranslucentLedgerService,
    utils::{fire_unconfirmed_solutions, fire_unconfirmed_transactions},
};

use snarkos_account::Account;
use snarkos_node_bft::{
    BFT,
    MAX_BATCH_DELAY,
    MEMORY_POOL_PORT,
    Primary,
    helpers::{PrimarySender, Storage, init_primary_channels},
};
use snarkos_node_bft_storage_service::BFTMemoryService;
use snarkos_node_network::{ConnectionMode, PeerPoolHandling};
use snarkos_node_sync::BlockSync;
use snarkos_utilities::{NodeDataDir, SimpleStoppable};

use snarkvm::{
    console::{
        account::{Address, PrivateKey},
        algorithms::{BHP256, Hash},
        network::Network,
    },
    ledger::{
        Ledger,
        block::Block,
        committee::{Committee, MIN_VALIDATOR_STAKE},
        narwhal::BatchHeader,
        store::{ConsensusStore, helpers::memory::ConsensusMemory},
    },
    prelude::{CryptoRng, FromBytes, Rng, TestRng, ToBits, ToBytes, VM},
    utilities::to_bytes_le,
};

use aleo_std::StorageMode;
use indexmap::IndexMap;
use itertools::Itertools;
#[cfg(feature = "locktick")]
use locktick::parking_lot::Mutex;
#[cfg(not(feature = "locktick"))]
use parking_lot::Mutex;
use std::{
    collections::HashMap,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    ops::RangeBounds,
    sync::{Arc, OnceLock},
    time::Duration,
};
use tokio::{task::JoinHandle, time::sleep};
use tracing::*;

/// The configuration for the test network.
#[derive(Clone, Copy, Debug)]
pub struct TestNetworkConfig {
    /// The number of nodes to spin up.
    pub num_nodes: u16,
    /// If this is set to `true`, the BFT protocol is started on top of Narwhal.
    pub bft: bool,
    /// If this is set to `true`, all nodes are connected to each other (when they're first
    /// started).
    pub connect_all: bool,
    /// If `Some(i)` is set, the cannons will fire every `i` milliseconds.
    pub fire_transmissions: Option<u64>,
    /// If this is set to `true`, the number of connections is logged every 5 seconds.
    pub log_connections: bool,
}

/// A test network.
#[derive(Clone)]
pub struct TestNetwork {
    /// The configuration for the test network.
    pub config: TestNetworkConfig,
    /// A map of node IDs to validators in the network.
    pub validators: HashMap<u16, TestValidator>,
}

/// A test validator.
#[derive(Clone)]
pub struct TestValidator {
    /// The ID of the validator.
    pub id: u16,
    /// The primary instance. When the BFT is enabled this is a clone of the BFT primary.
    pub primary: Primary<CurrentNetwork>,
    /// The channel sender of the primary.
    pub primary_sender: Option<PrimarySender<CurrentNetwork>>,
    /// The BFT instance. This is only set if the BFT is enabled.
    pub bft: OnceLock<BFT<CurrentNetwork>>,
    /// The tokio handles of all long-running tasks associated with the validator (incl. cannons).
    pub handles: Arc<Mutex<Vec<JoinHandle<()>>>>,
}

pub type CurrentLedger = Ledger<CurrentNetwork, ConsensusMemory<CurrentNetwork>>;

impl TestValidator {
    pub fn fire_transmissions(&mut self, interval_ms: u64) {
        let solution_handle = fire_unconfirmed_solutions(self.primary_sender.as_mut().unwrap(), self.id, interval_ms);
        let transaction_handle =
            fire_unconfirmed_transactions(self.primary_sender.as_mut().unwrap(), self.id, interval_ms);

        self.handles.lock().push(solution_handle);
        self.handles.lock().push(transaction_handle);
    }

    pub fn log_connections(&mut self) {
        let self_clone = self.clone();
        self.handles.lock().push(tokio::task::spawn(async move {
            loop {
                let connections = self_clone.primary.gateway().connected_peers();
                info!("{} connections", connections.len());
                for connection in connections {
                    debug!("  {}", connection);
                }
                sleep(Duration::from_secs(5)).await;
            }
        }));
    }
}

impl TestNetwork {
    // Creates a new test network with the given configuration.
    pub fn new(config: TestNetworkConfig) -> Self {
        let mut rng = TestRng::default();

        let (accounts, committee) = new_test_committee(config.num_nodes, &mut rng);
        let bonded_balances: IndexMap<_, _> = committee
            .members()
            .iter()
            .map(|(address, (amount, _, _))| (*address, (*address, *address, *amount)))
            .collect();
        let gen_key = *accounts[0].private_key();
        let public_balance_per_validator = (CurrentNetwork::STARTING_SUPPLY
            - (config.num_nodes as u64) * MIN_VALIDATOR_STAKE)
            / (config.num_nodes as u64);
        let mut balances = IndexMap::<Address<CurrentNetwork>, u64>::new();
        for account in accounts.iter() {
            balances.insert(account.address(), public_balance_per_validator);
        }

        let mut validators = HashMap::with_capacity(config.num_nodes as usize);
        for (id, account) in accounts.into_iter().enumerate() {
            let gen_ledger =
                genesis_ledger(gen_key, committee.clone(), balances.clone(), bonded_balances.clone(), &mut rng);
            let ledger = Arc::new(TranslucentLedgerService::new(gen_ledger, SimpleStoppable::new()));
            let storage = Storage::new(
                ledger.clone(),
                Arc::new(BFTMemoryService::new()),
                BatchHeader::<CurrentNetwork>::MAX_GC_ROUNDS as u64,
            )
            .unwrap();
            // Initialize the block synchronization logic.
            let block_sync = Arc::new(BlockSync::new(ledger.clone(), ConnectionMode::Gateway));
            let (primary, bft) = if config.bft {
                let bft = BFT::<CurrentNetwork>::new(
                    account,
                    storage,
                    ledger,
                    block_sync,
                    Some(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), MEMORY_POOL_PORT + id as u16)),
                    &[],
                    false,
                    NodeDataDir::new_test(None),
                    None,
                    1,
                )
                .unwrap();
                (bft.primary().clone(), Some(bft))
            } else {
                let primary = Primary::<CurrentNetwork>::new(
                    account,
                    storage,
                    ledger,
                    block_sync,
                    Some(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), MEMORY_POOL_PORT + id as u16)),
                    &[],
                    false,
                    NodeDataDir::new_test(None),
                    None,
                    1,
                )
                .unwrap();
                (primary, None)
            };

            let test_validator = TestValidator {
                id: id as u16,
                primary,
                primary_sender: None,
                bft: OnceLock::new(),
                handles: Default::default(),
            };
            if let Some(bft) = bft {
                assert!(test_validator.bft.set(bft).is_ok());
            }
            validators.insert(id as u16, test_validator);
        }

        Self { config, validators }
    }

    // Starts each node in the network.
    pub async fn start(&mut self) {
        for validator in self.validators.values_mut() {
            let (primary_sender, primary_receiver) = init_primary_channels();
            validator.primary_sender = Some(primary_sender.clone());

            // let ledger_service = validator.primary.ledger().clone();
            // let sync = BlockSync::new(BlockSyncMode::Gateway, ledger_service);
            // sync.try_block_sync(validator.primary.gateway()).await.unwrap();

            if let Some(bft) = validator.bft.get_mut() {
                // Setup the channels and start the bft.
                bft.run(None, None, primary_sender, primary_receiver).await.unwrap();
            } else {
                // Setup the channels and start the primary.
                validator.primary.run(None, None, None, primary_sender, primary_receiver).await.unwrap();
            }

            if let Some(interval_ms) = self.config.fire_transmissions {
                validator.fire_transmissions(interval_ms);
            }

            if self.config.log_connections {
                validator.log_connections();
            }
        }

        if self.config.connect_all {
            self.connect_all().await;
        }
    }

    // Starts the solution and transaction cannons for node.
    pub fn fire_transmissions_at(&mut self, id: u16, interval_ms: u64) {
        self.validators.get_mut(&id).unwrap().fire_transmissions(interval_ms);
    }

    // Connects a node to another node.
    pub async fn connect_validators(&self, first_id: u16, second_id: u16) {
        let first_validator = self.validators.get(&first_id).unwrap();
        let second_validator_ip = self.validators.get(&second_id).unwrap().primary.gateway().local_ip();
        let gateway = first_validator.primary.gateway();
        let handle = gateway.connect(second_validator_ip).expect("connection attempt failed");
        // Await the full TCP + handshake completion instead of relying on a fixed sleep.
        handle.await.unwrap().expect("connecting validators failed");
    }

    // Connects all nodes to each other.
    pub async fn connect_all(&self) {
        for (validator, other_validator) in self.validators.values().tuple_combinations() {
            // Connect to the node.
            let ip = other_validator.primary.gateway().local_ip();
            let _ = validator.primary.gateway().connect(ip);
            // Give the connection time to be established.
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }

    // Connects a specific node to all other nodes.
    pub async fn connect_one(&self, id: u16) {
        let target_validator = self.validators.get(&id).unwrap();
        let target_ip = target_validator.primary.gateway().local_ip();
        for validator in self.validators.values() {
            if validator.id != id {
                // Connect to the node.
                let _ = validator.primary.gateway().connect(target_ip);
                // Give the connection time to be established.
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        }
    }

    // Disconnects N nodes from all other nodes.
    pub async fn disconnect(&self, num_nodes: u16) {
        for validator in self.validators.values().take(num_nodes as usize) {
            for peer_ip in validator.primary.gateway().connected_peers().iter() {
                validator.primary.gateway().disconnect(*peer_ip);
            }
        }

        // Give the connections time to be closed.
        sleep(Duration::from_millis(100)).await;
    }

    // Disconnects a specific node from all other nodes.
    pub async fn disconnect_one(&self, id: u16) {
        let target_validator = self.validators.get(&id).unwrap();
        for peer_ip in target_validator.primary.gateway().connected_peers().iter() {
            target_validator.primary.gateway().disconnect(*peer_ip);
        }

        // Give the connections time to be closed.
        sleep(Duration::from_millis(100)).await;
    }

    // Checks if a Byzantine fault-tolerant quorum of validators has reached the given round.
    // Assuming `N = 3f + 1 + k`, where `0 <= k < 3`, and '/' denotes integer division,
    // then `N - (N-1)/3 = 2N/3 + 1 = 2f + 1 + (2k+2)/3 = 2f + 1 + k = N - f`.
    pub fn is_round_reached(&self, round: u64) -> bool {
        let quorum_threshold = self.validators.len() - (self.validators.len() - 1) / 3;
        self.validators.values().filter(|v| v.primary.current_round() >= round).count() >= quorum_threshold
    }

    // Checks if all the nodes have stopped progressing.
    pub async fn is_halted(&self) -> bool {
        let halt_round = self.validators.values().map(|v| v.primary.current_round()).max().unwrap();
        sleep(MAX_BATCH_DELAY * 2).await;
        self.validators.values().all(|v| v.primary.current_round() <= halt_round)
    }

    // Checks if the committee is coherent in storage for all nodes (not quorum) over a range of
    // rounds.
    pub fn is_committee_coherent<T>(&self, rounds_range: T) -> bool
    where
        T: RangeBounds<u64> + IntoIterator<Item = u64>,
    {
        for round in rounds_range.into_iter() {
            let mut last: Option<Committee<CurrentNetwork>> = None;
            for validator in self.validators.values() {
                // Round might be in future, in case validator didn't get to it.
                if let Ok(committee) = validator.primary.ledger().get_committee_for_round(round) {
                    match last.clone() {
                        None => last = Some(committee),
                        Some(first) => {
                            if first != committee {
                                return false;
                            }
                        }
                    }
                }
            }
        }

        true
    }

    // Checks if the certificates are coherent in storage for all nodes (not quorum) over a range
    // of rounds.
    pub fn is_certificate_round_coherent<T>(&self, rounds_range: T) -> bool
    where
        T: RangeBounds<u64> + IntoIterator<Item = u64>,
    {
        rounds_range.into_iter().all(|round| {
            self.validators.values().map(|v| v.primary.storage().get_certificates_for_round(round)).dedup().count() == 1
        })
    }
}

// Initializes a new test committee.
pub fn new_test_committee(n: u16, rng: &mut TestRng) -> (Vec<Account<CurrentNetwork>>, Committee<CurrentNetwork>) {
    let mut accounts = Vec::with_capacity(n as usize);
    let mut members = IndexMap::with_capacity(n as usize);
    for i in 0..n {
        // Sample the account.
        let account = Account::new(rng).unwrap();
        info!("Validator {}: {}", i, account.address());

        members.insert(account.address(), (MIN_VALIDATOR_STAKE, false, rng.random_range(0..100)));
        accounts.push(account);
    }
    // Initialize the committee.
    let committee = Committee::<CurrentNetwork>::new(0u64, members).unwrap();

    (accounts, committee)
}

fn genesis_cache() -> &'static Mutex<HashMap<Vec<u8>, Block<CurrentNetwork>>> {
    static CACHE: OnceLock<Mutex<HashMap<Vec<u8>, Block<CurrentNetwork>>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

pub fn genesis_block(
    genesis_private_key: PrivateKey<CurrentNetwork>,
    committee: Committee<CurrentNetwork>,
    public_balances: IndexMap<Address<CurrentNetwork>, u64>,
    bonded_balances: IndexMap<Address<CurrentNetwork>, (Address<CurrentNetwork>, Address<CurrentNetwork>, u64)>,
    rng: &mut (impl Rng + CryptoRng),
) -> Block<CurrentNetwork> {
    // Initialize the store.
    let store = ConsensusStore::<_, ConsensusMemory<_>>::open(StorageMode::new_test(None)).unwrap();
    // Initialize a new VM.
    let vm = VM::from(store).unwrap();
    // Initialize the genesis block.
    vm.genesis_quorum(&genesis_private_key, committee, public_balances, bonded_balances, rng).unwrap()
}

pub fn genesis_ledger(
    genesis_private_key: PrivateKey<CurrentNetwork>,
    committee: Committee<CurrentNetwork>,
    public_balances: IndexMap<Address<CurrentNetwork>, u64>,
    bonded_balances: IndexMap<Address<CurrentNetwork>, (Address<CurrentNetwork>, Address<CurrentNetwork>, u64)>,
    rng: &mut (impl Rng + CryptoRng),
) -> CurrentLedger {
    let cache_key =
        to_bytes_le![genesis_private_key, committee, public_balances.iter().collect::<Vec<(_, _)>>()].unwrap();
    // Initialize the genesis block on the first call; other callers
    // will wait for it on the mutex.
    let block = genesis_cache()
        .lock()
        .entry(cache_key.clone())
        .or_insert_with(|| {
            let hasher = BHP256::<CurrentNetwork>::setup("aleo.dev.block").unwrap();
            let file_name = hasher.hash(&cache_key.to_bits_le()).unwrap().to_string() + ".genesis";
            let file_path = std::env::temp_dir().join(file_name);
            if file_path.exists() {
                let buffer = std::fs::read(file_path).unwrap();
                return Block::from_bytes_le(&buffer).unwrap();
            }

            let block = genesis_block(genesis_private_key, committee, public_balances, bonded_balances, rng);
            std::fs::write(&file_path, block.to_bytes_le().unwrap()).unwrap();
            block
        })
        .clone();
    // Initialize the ledger with the genesis block.
    CurrentLedger::load(block, StorageMode::new_test(None)).unwrap()
}
