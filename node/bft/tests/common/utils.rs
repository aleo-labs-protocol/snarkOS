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

use crate::common::{CurrentNetwork, TranslucentLedgerService, primary};

use snarkos_account::Account;
use snarkos_node_bft::{
    Gateway,
    Worker,
    helpers::{PrimarySender, Storage},
    storage_service::BFTMemoryService,
};
use snarkos_utilities::{NodeDataDir, SimpleStoppable, locks::parking_lot::RwLock};

use snarkvm::{
    console::account::Address,
    ledger::{
        committee::Committee,
        narwhal::{BatchHeader, Data},
        store::helpers::memory::ConsensusMemory,
    },
    prelude::{
        Field,
        Network,
        TestRng,
        Uniform,
        block::Transaction,
        committee::MIN_VALIDATOR_STAKE,
        puzzle::{Solution, SolutionID},
    },
};

use std::{str::FromStr, sync::Arc, time::Duration};

use ::bytes::Bytes;
use indexmap::IndexMap;
use rand::{Rng, RngExt};
use tokio::{sync::oneshot, task::JoinHandle, time::sleep};
use tracing::*;
use tracing_subscriber::{
    layer::{Layer, SubscriberExt},
    util::SubscriberInitExt,
};

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
        filter.add_directive("snarkos_node_tcp=trace".parse().unwrap())
    } else {
        filter.add_directive("snarkos_node_tcp=off".parse().unwrap())
    };

    // Initialize tracing.
    let _ = tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::Layer::default().with_target(verbosity > 2).with_filter(filter))
        .try_init();
}

/// Fires *fake* unconfirmed solutions at the node.
pub fn fire_unconfirmed_solutions(
    sender: &PrimarySender<CurrentNetwork>,
    node_id: u16,
    interval_ms: u64,
) -> JoinHandle<()> {
    let tx_unconfirmed_solution = sender.tx_unconfirmed_solution.clone();
    tokio::task::spawn(async move {
        // This RNG samples the *same* fake solutions for all nodes.
        let mut shared_rng = TestRng::fixed(123456789);
        // This RNG samples *different* fake solutions for each node.
        let mut unique_rng = TestRng::fixed(node_id as u64);

        // A closure to generate a solution ID and solution.
        async fn sample(mut rng: impl Rng) -> (SolutionID<CurrentNetwork>, Data<Solution<CurrentNetwork>>) {
            // Sample a random fake solution ID.
            let solution_id = rng.random::<u64>().into();
            // Sample random fake solution bytes.
            let mut vec = vec![0u8; 1024];
            rng.fill_bytes(&mut vec);
            let solution = Data::Buffer(Bytes::from(vec));
            // Return the solution ID and solution.
            (solution_id, solution)
        }

        // Initialize a counter.
        let mut counter = 0;

        loop {
            // Sample a random fake solution ID and solution.
            let (solution_id, solution) =
                if counter % 2 == 0 { sample(&mut shared_rng).await } else { sample(&mut unique_rng).await };
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
            sleep(Duration::from_millis(interval_ms)).await;
        }
    })
}

/// Fires *fake* unconfirmed transactions at the node.
pub fn fire_unconfirmed_transactions(
    sender: &PrimarySender<CurrentNetwork>,
    node_id: u16,
    interval_ms: u64,
) -> JoinHandle<()> {
    let tx_unconfirmed_transaction = sender.tx_unconfirmed_transaction.clone();
    tokio::task::spawn(async move {
        // This RNG samples the *same* fake transactions for all nodes.
        let mut shared_rng = TestRng::fixed(123456789);
        // This RNG samples *different* fake transactions for each node.
        let mut unique_rng = TestRng::fixed(node_id as u64);

        // A closure to generate an ID and transaction.
        fn sample(
            mut rng: impl Rng,
        ) -> (<CurrentNetwork as Network>::TransactionID, Data<Transaction<CurrentNetwork>>) {
            // Sample a random fake transaction ID.
            let id = Field::<CurrentNetwork>::rand(&mut rng).into();
            // Sample random fake transaction bytes.
            let mut vec = vec![0u8; 1024];
            rng.fill_bytes(&mut vec);
            let transaction = Data::Buffer(Bytes::from(vec));
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
            sleep(Duration::from_millis(interval_ms)).await;
        }
    })
}

/// Samples a new ledger with the given number of nodes.
pub fn sample_ledger(
    accounts: &[Account<CurrentNetwork>],
    committee: &Committee<CurrentNetwork>,
    rng: &mut TestRng,
) -> Arc<TranslucentLedgerService<CurrentNetwork, ConsensusMemory<CurrentNetwork>>> {
    let num_nodes = committee.num_members();
    let bonded_balances: IndexMap<_, _> =
        committee.members().iter().map(|(address, (amount, _, _))| (*address, (*address, *address, *amount))).collect();
    let gen_key = *accounts[0].private_key();
    let public_balance_per_validator =
        (CurrentNetwork::STARTING_SUPPLY - (num_nodes as u64) * MIN_VALIDATOR_STAKE) / (num_nodes as u64);
    let mut balances = IndexMap::<Address<CurrentNetwork>, u64>::new();
    for account in accounts.iter() {
        balances.insert(account.address(), public_balance_per_validator);
    }

    let gen_ledger =
        primary::genesis_ledger(gen_key, committee.clone(), balances.clone(), bonded_balances.clone(), rng);
    Arc::new(TranslucentLedgerService::new(gen_ledger, SimpleStoppable::new()))
}

/// Samples a new storage with the given ledger.
pub fn sample_storage<N: Network>(ledger: Arc<TranslucentLedgerService<N, ConsensusMemory<N>>>) -> Storage<N> {
    Storage::new(ledger, Arc::new(BFTMemoryService::new()), BatchHeader::<N>::MAX_GC_ROUNDS as u64).unwrap()
}

/// Samples a new gateway with the given ledger.
pub fn sample_gateway<N: Network>(
    account: Account<N>,
    storage: Storage<N>,
    ledger: Arc<TranslucentLedgerService<N, ConsensusMemory<N>>>,
) -> Gateway<N> {
    // Initialize the gateway.
    Gateway::new(account, storage, ledger, None, &[], false, NodeDataDir::new_test(None), None).unwrap()
}

/// Samples a new worker with the given ledger.
pub fn sample_worker<N: Network>(
    id: u8,
    account: Account<N>,
    ledger: Arc<TranslucentLedgerService<N, ConsensusMemory<N>>>,
) -> Worker<N> {
    // Sample a storage.
    let storage = sample_storage(ledger.clone());
    // Sample a gateway.
    let gateway = sample_gateway(account, storage.clone(), ledger.clone());
    // Sample a dummy proposed batch.
    let proposed_batch = Arc::new(RwLock::new(Default::default()));
    // Construct the worker instance.
    Worker::new(id, Arc::new(gateway.clone()), storage.clone(), ledger, proposed_batch).unwrap()
}
