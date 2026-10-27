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

#[macro_use]
extern crate tracing;

#[cfg(feature = "metrics")]
extern crate snarkos_node_metrics as metrics;

use snarkos_account::Account;
use snarkos_node_bft::{
    BFT,
    MEMORY_POOL_PORT,
    Primary,
    helpers::{ConsensusReceiver, PrimarySender, Storage, init_consensus_channels, init_primary_channels},
};
use snarkos_node_bft_ledger_service::TranslucentLedgerService;
use snarkos_node_bft_storage_service::BFTMemoryService;
use snarkos_node_network::ConnectionMode;
use snarkos_node_sync::BlockSync;
use snarkos_utilities::{NodeDataDir, SimpleStoppable};

use aleo_std::StorageMode;
use snarkvm::{
    console::{account::PrivateKey, algorithms::BHP256, types::Address},
    ledger::{
        Block,
        Ledger,
        block::Transaction,
        committee::{Committee, MIN_VALIDATOR_STAKE},
        narwhal::{BatchHeader, Data},
        puzzle::{Solution, SolutionID},
        store::{ConsensusStore, helpers::memory::ConsensusMemory},
    },
    prelude::{Field, Hash, Network, Uniform, VM},
    utilities::{FromBytes, TestRng, ToBits, ToBytes, to_bytes_le},
};

use ::bytes::Bytes;
use anyhow::{Error, Result, anyhow, ensure};
use axum::{
    Router,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
};

use axum_extra::response::ErasedJson;
use clap::{Parser, ValueEnum};
use indexmap::IndexMap;
use rand::{CryptoRng, Rng, RngExt, SeedableRng};
use std::{
    collections::HashMap,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::PathBuf,
    str::FromStr,
    sync::{Arc, Mutex, OnceLock},
};
use tokio::{net::TcpListener, sync::oneshot};
use tracing_subscriber::{
    layer::{Layer, SubscriberExt},
    util::SubscriberInitExt,
};

type CurrentNetwork = snarkvm::prelude::MainnetV0;

/**************************************************************************************************/

/// Initializes the logger.
pub fn initialize_logger(verbosity: u8) {
    let verbosity_str = match verbosity {
        0 => "info",
        1 => "debug",
        2..=4 => "trace",
        _ => "info",
    };

    // Filter out undesirable logs. (unfortunately EnvFilter cannot be cloned)
    let filter = tracing_subscriber::EnvFilter::from_str(verbosity_str)
        .unwrap()
        .add_directive("mio=off".parse().unwrap())
        .add_directive("tokio_util=off".parse().unwrap())
        .add_directive("hyper=off".parse().unwrap())
        .add_directive("reqwest=off".parse().unwrap())
        .add_directive("want=off".parse().unwrap())
        .add_directive("h2=off".parse().unwrap());

    let filter = if verbosity > 3 {
        filter.add_directive("snarkos_node_bft::gateway=trace".parse().unwrap())
    } else {
        filter.add_directive("snarkos_node_bft::gateway=off".parse().unwrap())
    };

    let filter = if verbosity > 4 {
        filter.add_directive("snarkos_node_tcp=trace".parse().unwrap())
    } else {
        filter.add_directive("snarkos_node_tcp=off".parse().unwrap())
    };

    // Initialize tracing.
    let _ = tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::Layer::default().with_target(verbosity > 2).with_filter(filter))
        .try_init();
}

/**************************************************************************************************/

/// Starts the BFT instance.
pub async fn start_bft(
    node_id: u16,
    num_nodes: u16,
    peers: HashMap<u16, SocketAddr>,
) -> Result<(BFT<CurrentNetwork>, PrimarySender<CurrentNetwork>)> {
    // Initialize the primary channels.
    let (sender, receiver) = init_primary_channels();
    // Initialize the components.
    let (committee, account) = initialize_components(node_id, num_nodes)?;
    // Initialize the translucent ledger service.
    let ledger = create_ledger(&account, num_nodes, committee, node_id);
    // Initialize the storage.
    let storage = Storage::new(
        ledger.clone(),
        Arc::new(BFTMemoryService::new()),
        BatchHeader::<CurrentNetwork>::MAX_GC_ROUNDS as u64,
    )?;
    // Initialize the gateway IP and storage mode.
    let ip = match peers.get(&node_id) {
        Some(ip) => Some(*ip),
        None => Some(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), MEMORY_POOL_PORT + node_id)),
    };
    let node_data_dir = NodeDataDir::new_test(None);
    // Initialize the trusted validators.
    let trusted_validators = trusted_validators(node_id, num_nodes, peers);
    let trusted_peers_only = false;
    // Initialize the consensus channels.
    let (consensus_sender, consensus_receiver) = init_consensus_channels::<CurrentNetwork>();
    // Initialize the consensus receiver handler.
    consensus_handler(consensus_receiver);
    // Initialize the BFT instance.
    let block_sync = Arc::new(BlockSync::new(ledger.clone(), ConnectionMode::Gateway));
    let mut bft = BFT::<CurrentNetwork>::new(
        account,
        storage,
        ledger,
        block_sync,
        ip,
        &trusted_validators,
        trusted_peers_only,
        node_data_dir,
        None,
        1,
    )?;
    // Run the BFT instance.
    bft.run(None, Some(consensus_sender), sender.clone(), receiver).await?;
    // Retrieve the BFT's primary.
    let primary = bft.primary();
    // Handle OS signals.
    handle_signals(primary);
    // Return the BFT instance.
    Ok((bft, sender))
}

/// Starts the primary instance.
pub async fn start_primary(
    node_id: u16,
    num_nodes: u16,
    peers: HashMap<u16, SocketAddr>,
) -> Result<(Primary<CurrentNetwork>, PrimarySender<CurrentNetwork>)> {
    // Initialize the primary channels.
    let (sender, receiver) = init_primary_channels();
    // Initialize the components.
    let (committee, account) = initialize_components(node_id, num_nodes)?;
    // Initialize the translucent ledger service.
    let ledger = create_ledger(&account, num_nodes, committee, node_id);
    // Initialize the storage.
    let storage = Storage::new(
        ledger.clone(),
        Arc::new(BFTMemoryService::new()),
        BatchHeader::<CurrentNetwork>::MAX_GC_ROUNDS as u64,
    )?;
    // Initialize the gateway IP and storage mode.
    let ip = match peers.get(&node_id) {
        Some(ip) => Some(*ip),
        None => Some(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), MEMORY_POOL_PORT + node_id)),
    };
    let node_data_dir = NodeDataDir::new_test(None);
    // Initialize the trusted validators.
    let trusted_validators = trusted_validators(node_id, num_nodes, peers);
    let trusted_peers_only = false;
    // Initialize the primary instance.
    let block_sync = Arc::new(BlockSync::new(ledger.clone(), ConnectionMode::Gateway));
    let primary = Primary::<CurrentNetwork>::new(
        account,
        storage,
        ledger,
        block_sync,
        ip,
        &trusted_validators,
        trusted_peers_only,
        node_data_dir,
        None,
        1,
    )?;
    // Run the primary instance.
    primary.run(None, None, None, sender.clone(), receiver).await?;
    // Handle OS signals.
    handle_signals(&primary);
    // Return the primary instance.
    Ok((primary, sender))
}

/// Initialize the translucent ledger service.
fn create_ledger(
    account: &Account<CurrentNetwork>,
    num_nodes: u16,
    committee: Committee<snarkvm::prelude::MainnetV0>,
    node_id: u16,
) -> Arc<TranslucentLedgerService<snarkvm::prelude::MainnetV0, ConsensusMemory<snarkvm::prelude::MainnetV0>>> {
    let gen_key = account.private_key();
    let public_balance_per_validator =
        (CurrentNetwork::STARTING_SUPPLY - (num_nodes as u64) * MIN_VALIDATOR_STAKE) / (num_nodes as u64);
    let mut balances = IndexMap::<Address<CurrentNetwork>, u64>::new();
    for address in committee.members().keys() {
        balances.insert(*address, public_balance_per_validator);
    }
    let mut rng = TestRng::default();
    let gen_ledger = genesis_ledger(*gen_key, committee.clone(), balances.clone(), node_id, &mut rng);
    Arc::new(TranslucentLedgerService::new(gen_ledger, SimpleStoppable::new()))
}

pub type CurrentLedger = Ledger<CurrentNetwork, ConsensusMemory<CurrentNetwork>>;

fn genesis_cache() -> &'static Mutex<HashMap<Vec<u8>, Block<CurrentNetwork>>> {
    static CACHE: OnceLock<Mutex<HashMap<Vec<u8>, Block<CurrentNetwork>>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn genesis_block(
    genesis_private_key: PrivateKey<CurrentNetwork>,
    committee: Committee<CurrentNetwork>,
    public_balances: IndexMap<Address<CurrentNetwork>, u64>,
    rng: &mut impl CryptoRng,
) -> Block<CurrentNetwork> {
    // Initialize the store.
    let store = ConsensusStore::<_, ConsensusMemory<_>>::open(StorageMode::new_test(None)).unwrap();
    // Initialize a new VM.
    let vm = VM::from(store).unwrap();
    // Initialize the genesis block.
    let bonded_balances: IndexMap<_, _> =
        committee.members().iter().map(|(address, (amount, _, _))| (*address, (*address, *address, *amount))).collect();
    vm.genesis_quorum(&genesis_private_key, committee, public_balances, bonded_balances, rng).unwrap()
}

fn genesis_ledger(
    genesis_private_key: PrivateKey<CurrentNetwork>,
    committee: Committee<CurrentNetwork>,
    public_balances: IndexMap<Address<CurrentNetwork>, u64>,
    node_id: u16,
    rng: &mut impl CryptoRng,
) -> CurrentLedger {
    let cache_key =
        to_bytes_le![genesis_private_key, committee, public_balances.iter().collect::<Vec<(_, _)>>()].unwrap();
    // Initialize the genesis block on the first call; other callers
    // will wait for it on the mutex.
    let block = genesis_cache()
        .lock()
        .unwrap()
        .entry(cache_key.clone())
        .or_insert_with(|| {
            let hasher = BHP256::<CurrentNetwork>::setup("aleo.dev.block").unwrap();
            let file_name = hasher.hash(&cache_key.to_bits_le()).unwrap().to_string() + ".genesis";
            let file_path = std::env::temp_dir().join(file_name);
            if file_path.exists() {
                let buffer = std::fs::read(file_path).unwrap();
                return Block::from_bytes_le(&buffer).unwrap();
            }

            let block = genesis_block(genesis_private_key, committee, public_balances, rng);
            std::fs::write(&file_path, block.to_bytes_le().unwrap()).unwrap();
            block
        })
        .clone();
    // Initialize the ledger with the genesis block.
    CurrentLedger::load(block, aleo_std::StorageMode::Development(node_id)).unwrap()
}

/// Initializes the components of the node.
fn initialize_components(node_id: u16, num_nodes: u16) -> Result<(Committee<CurrentNetwork>, Account<CurrentNetwork>)> {
    // Ensure that the node ID is valid.
    ensure!(node_id < num_nodes, "Node ID {node_id} must be less than {num_nodes}");

    // Sample a account.
    let account = Account::new(&mut rand_chacha::ChaChaRng::seed_from_u64(node_id as u64))?;
    println!("\n{account}\n");

    // Initialize a map for the committee members.
    let mut members = IndexMap::with_capacity(num_nodes as usize);
    // Add the validators as members.
    for i in 0..num_nodes {
        // Sample the account.
        let account = Account::new(&mut rand_chacha::ChaChaRng::seed_from_u64(i as u64))?;
        // Add the validator.
        members.insert(account.address(), (MIN_VALIDATOR_STAKE, false, i as u8));
        println!("  Validator {}: {}", i, account.address());
    }
    println!();

    // Initialize the committee.
    let committee = Committee::<CurrentNetwork>::new(0u64, members)?;
    // Return the committee and account.
    Ok((committee, account))
}

/// Handles the consensus receiver.
fn consensus_handler(receiver: ConsensusReceiver<CurrentNetwork>) {
    let ConsensusReceiver { mut rx_consensus_subdag } = receiver;

    tokio::task::spawn(async move {
        while let Some((subdag, transmissions, callback)) = rx_consensus_subdag.recv().await {
            // Determine the amount of time to sleep for the subdag.
            let subdag_ms = subdag.values().flatten().count();
            // Determine the amount of time to sleep for the transmissions.
            let transmissions_ms = transmissions.len() * 25;
            // Add a constant delay.
            let constant_ms = 100;
            // Compute the total amount of time to sleep.
            let sleep_ms = (subdag_ms + transmissions_ms + constant_ms) as u64;
            // Sleep for the determined amount of time.
            tokio::time::sleep(std::time::Duration::from_millis(sleep_ms)).await;
            // Call the callback.
            callback.send(Ok(true)).ok();
        }
    });
}

/// Returns the trusted validators.
fn trusted_validators(node_id: u16, num_nodes: u16, peers: HashMap<u16, SocketAddr>) -> Vec<SocketAddr> {
    // Initialize a vector for the trusted nodes.
    let mut trusted = Vec::with_capacity(num_nodes as usize);
    // Iterate through the nodes.
    for i in 0..num_nodes {
        // Initialize the gateway IP.
        let ip = match peers.get(&i) {
            Some(ip) => *ip,
            None => SocketAddr::from_str(&format!("127.0.0.1:{}", MEMORY_POOL_PORT + i)).unwrap(),
        };
        // If the node is not the current node, add it to the trusted nodes.
        if i != node_id {
            trusted.push(ip);
        }
    }
    // Return the trusted nodes.
    trusted
}

/// Handles OS signals for the node to intercept and perform a clean shutdown.
/// Note: Only Ctrl-C is supported; it should work on both Unix-family systems and Windows.
fn handle_signals(primary: &Primary<CurrentNetwork>) {
    let node = primary.clone();
    tokio::task::spawn(async move {
        match tokio::signal::ctrl_c().await {
            Ok(()) => {
                node.shut_down().await;
                std::process::exit(0);
            }
            Err(error) => error!("tokio::signal::ctrl_c encountered an error: {}", error),
        }
    });
}

/**************************************************************************************************/

/// Fires *fake* unconfirmed solutions at the node.
fn fire_unconfirmed_solutions(sender: &PrimarySender<CurrentNetwork>, node_id: u16, interval_ms: u64) {
    let tx_unconfirmed_solution = sender.tx_unconfirmed_solution.clone();
    tokio::task::spawn(async move {
        // This RNG samples the *same* fake solutions for all nodes.
        let mut shared_rng = rand_chacha::ChaChaRng::seed_from_u64(123456789);
        // This RNG samples *different* fake solutions for each node.
        let mut unique_rng = rand_chacha::ChaChaRng::seed_from_u64(node_id as u64);

        // A closure to generate a solution ID and solution.
        fn sample(mut rng: impl Rng) -> (SolutionID<CurrentNetwork>, Data<Solution<CurrentNetwork>>) {
            // Sample a random fake solution ID.
            let solution_id = rng.random::<u64>().into();
            // Sample random fake solution bytes.
            let solution = Data::Buffer(Bytes::from((0..1024).map(|_| rng.random::<u8>()).collect::<Vec<_>>()));
            // Return the ID and solution.
            (solution_id, solution)
        }

        // Initialize a counter.
        let mut counter = 0;

        loop {
            // Sample a random fake solution ID and solution.
            let (solution_id, solution) =
                if counter % 2 == 0 { sample(&mut shared_rng) } else { sample(&mut unique_rng) };
            // Initialize a callback sender and receiver.
            let (callback, callback_receiver) = oneshot::channel();
            // Send the fake solution.
            if let Err(e) = tx_unconfirmed_solution.send((solution_id, solution, callback)).await {
                error!("Failed to send unconfirmed solution: {e}");
            }
            let _ = callback_receiver.await;
            // Increment the counter.
            counter += 1;
            // Sleep briefly.
            tokio::time::sleep(std::time::Duration::from_millis(interval_ms)).await;
        }
    });
}

/// Fires *fake* unconfirmed transactions at the node.
fn fire_unconfirmed_transactions(sender: &PrimarySender<CurrentNetwork>, node_id: u16, interval_ms: u64) {
    let tx_unconfirmed_transaction = sender.tx_unconfirmed_transaction.clone();
    tokio::task::spawn(async move {
        // This RNG samples the *same* fake transactions for all nodes.
        let mut shared_rng = rand_chacha::ChaChaRng::seed_from_u64(123456789);
        // This RNG samples *different* fake transactions for each node.
        let mut unique_rng = rand_chacha::ChaChaRng::seed_from_u64(node_id as u64);

        // A closure to generate an ID and transaction.
        fn sample(
            mut rng: impl Rng,
        ) -> (<CurrentNetwork as Network>::TransactionID, Data<Transaction<CurrentNetwork>>) {
            // Sample a random fake transaction ID.
            let id = Field::<CurrentNetwork>::rand(&mut rng).into();
            // Sample random fake transaction bytes.
            let transaction = Data::Buffer(Bytes::from((0..1024).map(|_| rng.random::<u8>()).collect::<Vec<_>>()));
            // Return the ID and transaction.
            (id, transaction)
        }

        // Initialize a counter.
        let mut counter = 0;

        loop {
            // Sample a random fake transaction ID and transaction.
            let (id, transaction) = if counter % 2 == 0 { sample(&mut shared_rng) } else { sample(&mut unique_rng) };
            // Initialize a callback sender and receiver.
            let (callback, callback_receiver) = oneshot::channel();
            // Send the fake transaction.
            if let Err(e) = tx_unconfirmed_transaction.send((id, transaction, callback)).await {
                error!("Failed to send unconfirmed transaction: {e}");
            }
            let _ = callback_receiver.await;
            // Increment the counter.
            counter += 1;
            // Sleep briefly.
            tokio::time::sleep(std::time::Duration::from_millis(interval_ms)).await;
        }
    });
}

/**************************************************************************************************/

/// An enum of error handlers for the REST API server.
pub struct RestError(pub String);

impl IntoResponse for RestError {
    fn into_response(self) -> Response {
        (StatusCode::INTERNAL_SERVER_ERROR, format!("Something went wrong: {}", self.0)).into_response()
    }
}

#[derive(Clone)]
struct NodeState {
    bft: Option<BFT<CurrentNetwork>>,
    primary: Primary<CurrentNetwork>,
}

/// Returns the leader of the previous round, if one was present.
async fn get_leader(State(node): State<NodeState>) -> Result<ErasedJson, RestError> {
    match &node.bft {
        Some(bft) => Ok(ErasedJson::pretty(bft.leader())),
        None => Err(RestError("BFT is not enabled".into())),
    }
}

/// Returns the current round.
async fn get_current_round(State(node): State<NodeState>) -> Result<ErasedJson, RestError> {
    Ok(ErasedJson::pretty(node.primary.current_round()))
}

/// Returns the certificates for the given round.
async fn get_certificates_for_round(
    State(node): State<NodeState>,
    Path(round): Path<u64>,
) -> Result<ErasedJson, RestError> {
    Ok(ErasedJson::pretty(node.primary.storage().get_certificates_for_round(round)))
}

/// Starts up a local server for monitoring the node.
async fn start_server(bft: Option<BFT<CurrentNetwork>>, primary: Primary<CurrentNetwork>, node_id: u16) {
    // Initialize the routes.
    let router = Router::new()
        .route("/", get(|| async { "Hello, World!" }))
        .route("/leader", get(get_leader))
        .route("/round/current", get(get_current_round))
        .route("/certificates/:round", get(get_certificates_for_round))
        // Pass in the `NodeState` to access state.
        .with_state(NodeState { bft, primary });

    // Construct the IP address and port.
    let addr = format!("127.0.0.1:{}", 3000 + node_id);

    // Run the server.
    info!("Starting the server at '{addr}'...");
    let rest_addr: SocketAddr = addr.parse().unwrap();
    let rest_listener = TcpListener::bind(rest_addr).await.unwrap();
    axum::serve(rest_listener, router.into_make_service_with_connect_info::<SocketAddr>()).await.unwrap();
}

/**************************************************************************************************/

/// The operating mode of the node.
#[derive(Debug, Clone, ValueEnum)]
enum Mode {
    /// Runs the node with the Narwhal memory pool protocol.
    Narwhal,
    /// Runs the node with the Bullshark BFT protocol (on top of Narwhal).
    Bft,
}

/// A simple CLI for the node.
#[derive(Parser, Debug)]
struct Args {
    /// The mode to run the node in.
    #[arg(long)]
    mode: Mode,
    /// The ID of the node.
    #[arg(long, value_name = "ID")]
    id: u16,
    /// The number of nodes in the network.
    #[arg(long, value_name = "N")]
    num_nodes: u16,
    /// If set, the path to the file containing the committee peers.
    #[arg(long, value_name = "PATH")]
    peers: Option<PathBuf>,
    /// Enables the solution cannons, and optionally the interval in ms to run them on.
    #[arg(long, value_name = "INTERVAL_MS")]
    fire_solutions: Option<Option<u64>>,
    /// Enables the transaction cannons, and optionally the interval in ms to run them on.
    #[arg(long, value_name = "INTERVAL_MS")]
    fire_transactions: Option<Option<u64>>,
    /// Enables the solution and transaction cannons, and optionally the interval in ms to run them on.
    #[arg(long, value_name = "INTERVAL_MS")]
    fire_transmissions: Option<Option<u64>>,
    /// Enables the metrics exporter.
    #[clap(long, default_value = "false")]
    metrics: bool,
}

/// A helper method to parse the peers provided to the CLI.
fn parse_peers(peers_string: String) -> Result<HashMap<u16, SocketAddr>, Error> {
    // Expect list of peers in the form of `node_id=ip:port`, one per line.
    let mut peers = HashMap::new();
    for peer in peers_string.lines() {
        let mut split = peer.split('=');
        let node_id = u16::from_str(split.next().ok_or_else(|| anyhow!("Bad Format"))?)?;
        let addr: String = split.next().ok_or_else(|| anyhow!("Bad Format"))?.parse()?;
        let ip = SocketAddr::from_str(addr.as_str())?;
        peers.insert(node_id, ip);
    }
    Ok(peers)
}

/**************************************************************************************************/

#[tokio::main]
async fn main() -> Result<()> {
    initialize_logger(1);

    let args = Args::parse();

    let peers = match args.peers {
        Some(path) => parse_peers(std::fs::read_to_string(path)?)?,
        None => Default::default(),
    };

    // Initialize an optional BFT holder.
    let mut bft_holder = None;

    // Start the node.
    let (primary, sender) = match args.mode {
        Mode::Bft => {
            // Start the BFT.
            let (bft, sender) = start_bft(args.id, args.num_nodes, peers).await?;
            // Set the BFT holder.
            bft_holder = Some(bft.clone());
            // Return the primary and sender.
            (bft.primary().clone(), sender)
        }
        Mode::Narwhal => start_primary(args.id, args.num_nodes, peers).await?,
    };

    // The default interval to fire transmissions at.
    const DEFAULT_INTERVAL_MS: u64 = 450; // ms

    // Fire unconfirmed solutions.
    match (args.fire_transmissions, args.fire_solutions) {
        // Note: We allow the user to overload the solutions rate, even when the 'fire-transmissions' flag is enabled.
        (Some(rate), _) | (_, Some(rate)) => {
            fire_unconfirmed_solutions(&sender, args.id, rate.unwrap_or(DEFAULT_INTERVAL_MS));
        }
        _ => (),
    };

    // Fire unconfirmed transactions.
    match (args.fire_transmissions, args.fire_transactions) {
        // Note: We allow the user to overload the transactions rate, even when the 'fire-transmissions' flag is enabled.
        (Some(rate), _) | (_, Some(rate)) => {
            fire_unconfirmed_transactions(&sender, args.id, rate.unwrap_or(DEFAULT_INTERVAL_MS));
        }
        _ => (),
    };

    // Initialize the metrics.
    #[cfg(feature = "metrics")]
    if args.metrics {
        info!("Initializing metrics...");
        metrics::initialize_metrics(SocketAddr::from_str(&format!("0.0.0.0:{}", 9000 + args.id)).ok());
    }

    // Start the monitoring server.
    start_server(bft_holder, primary, args.id).await;
    // // Note: Do not move this.
    // std::future::pending::<()>().await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_peers_empty() -> Result<(), Error> {
        let peers = parse_peers("".to_owned())?;
        assert_eq!(peers.len(), 0);
        Ok(())
    }

    #[test]
    fn parse_peers_ok() -> Result<(), Error> {
        let s = r#"0=192.168.1.176:5000
1=192.168.1.176:5001
2=192.168.1.176:5002
3=192.168.1.176:5003"#;
        let peers = parse_peers(s.to_owned())?;
        assert_eq!(peers.len(), 4);
        Ok(())
    }

    #[test]
    fn parse_peers_bad_id() -> Result<(), Error> {
        let s = "A=192.168.1.176:5000";
        let peers = parse_peers(s.to_owned());
        assert!(peers.is_err());
        Ok(())
    }

    #[test]
    fn parse_peers_bad_format() -> Result<(), Error> {
        let s = "foo";
        let peers = parse_peers(s.to_owned());
        assert!(peers.is_err());
        Ok(())
    }
}
