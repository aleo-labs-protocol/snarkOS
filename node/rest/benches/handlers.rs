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

//! Benchmarks for the synchronous `Ledger`/`VM` calls made by `node/rest`'s REST API handlers
//!
//! Each group benchmarks the same `Ledger`/`VM` call the corresponding handler in
//! `src/routes.rs` makes directly (not through Axum) so the numbers reflect only the cost
//! that would actually move to a blocking task, not unrelated JSON-serialization or routing
//! overhead.
//!
//! The ledger fixtures here are small and built locally (a handful of blocks, a modest mapping
//! sweep), not the real 40-validator/250-height devnet snapshot CI downloads for the existing
//! HTTP-level REST benchmark (`.github/workflows/benchmarks.yml`). The goal is a fast,
//! network-free signal for which handlers are worth wrapping, not throughput at production
//! scale.

use aleo_std::StorageMode;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use snarkvm::{
    console::collections::U64,
    ledger::store::{
        ConsensusStore,
        helpers::{memory::ConsensusMemory, rocksdb::ConsensusDB},
    },
    prelude::*,
    utilities::TestRng,
};

use std::str::FromStr;

type CurrentNetwork = MainnetV0;
type CurrentStorage = ConsensusMemory<CurrentNetwork>;

/// `backup_database` is a RocksDB checkpoint operation; it has no meaning against the in-memory
/// `ConsensusMemory` storage every other group uses (it errors "Unavailable in memory-only
/// mode"), so its own bench needs a real on-disk ledger instead.
type DbStorage = ConsensusDB<CurrentNetwork>;

/// Mapping sizes swept for the `get_mapping_value` / `get_mapping_values` benches. Smaller than
/// snarkVM's own `ledger/benches/bonded_mapping.rs` sweep (10..100_000), since this is checking
/// whether a REST point-lookup's cost scales with mapping size at all, not characterizing the
/// storage layer itself.
const MAPPING_SIZES: &[usize] = &[10, 1_000, 10_000];

/// Commitment counts swept for `get_state_paths_for_commitments`. Bounded by the number of
/// commitments genesis actually produces (see `sample_commitments`), which are cycled to reach
/// larger counts -- repeats don't change the per-lookup cost this exists to measure.
const COMMITMENT_COUNTS: &[usize] = &[1, 4, 16];

/// Builds a genesis block bound to `private_key`, and the records from it that key can spend.
///
/// `node/rest`'s own tests (`sample_rest()` in `src/lib.rs`) load the real network's genesis via
/// `snarkos_node_router::test_helpers::sample_genesis_block`, which has no known private key --
/// fine for exercising routes that don't touch account state, but useless here, since several
/// handlers need a transaction to check or a program to deploy. This instead builds a fresh test
/// genesis the same way snarkVM's own `ledger/benches/transaction.rs` (`initialize_vm`) does, so
/// the private key's records are ours to spend.
fn genesis_with_records(
    private_key: &PrivateKey<CurrentNetwork>,
    rng: &mut TestRng,
) -> (Block<CurrentNetwork>, Vec<Record<CurrentNetwork, Plaintext<CurrentNetwork>>>) {
    let vm =
        VM::<CurrentNetwork, CurrentStorage>::from(ConsensusStore::open(StorageMode::new_test(None)).unwrap()).unwrap();
    let genesis = vm.genesis_beacon(private_key, rng).unwrap();

    let view_key = ViewKey::try_from(private_key).unwrap();
    let records = genesis
        .transitions()
        .cloned()
        .flat_map(Transition::into_records)
        .map(|(_, record)| record.decrypt(&view_key).unwrap())
        .collect();

    (genesis, records)
}

/// A ledger with no state beyond genesis, plus a private key and its spendable genesis records.
///
/// Used by the benches that don't need chain depth or a deployed program of their own (mapping
/// sweeps, transaction-verification benches that build their own never-applied transactions).
#[allow(clippy::type_complexity)]
fn fresh_ledger(
    rng: &mut TestRng,
) -> (
    Ledger<CurrentNetwork, CurrentStorage>,
    PrivateKey<CurrentNetwork>,
    Vec<Record<CurrentNetwork, Plaintext<CurrentNetwork>>>,
) {
    let private_key = PrivateKey::<CurrentNetwork>::new(rng).unwrap();
    let (genesis, records) = genesis_with_records(&private_key, rng);
    let ledger = Ledger::<CurrentNetwork, CurrentStorage>::load(genesis, StorageMode::new_test(None)).unwrap();
    (ledger, private_key, records)
}

/// A small sample program, deployed and advanced into a fresh ledger, plus a few more empty
/// blocks so height-based lookups aren't all at height 1. Backs the program-lookup and
/// block/committee-lookup benches (`get_program`, `get_block_header`, `find_block_hash`, etc.).
struct ChainFixture {
    ledger: Ledger<CurrentNetwork, CurrentStorage>,
    program_id: ProgramID<CurrentNetwork>,
    /// A transaction ID known to be in the ledger (the genesis credits mint), and its height.
    transaction_id: <CurrentNetwork as Network>::TransactionID,
    transaction_height: u32,
    /// Commitments known to be in the ledger's commitment tree (from genesis), for the
    /// state-path benches. Captured from genesis specifically, not `latest_block()` -- the
    /// blocks advanced after it are empty and have none of their own.
    genesis_commitments: Vec<Field<CurrentNetwork>>,
}

/// Advances `ledger` by one empty beacon block.
fn advance_empty_block(
    ledger: &Ledger<CurrentNetwork, CurrentStorage>,
    private_key: &PrivateKey<CurrentNetwork>,
    rng: &mut TestRng,
) {
    let block = ledger.prepare_advance_to_next_beacon_block(private_key, vec![], vec![], vec![], rng).unwrap();
    ledger.advance_to_next_block(&block).unwrap();
}

fn build_chain_fixture(rng: &mut TestRng) -> ChainFixture {
    let (ledger, private_key, _records) = fresh_ledger(rng);

    // A transaction already in the ledger (the genesis credits mint), for `get_transaction`-style
    // benches -- there's no need to build a new one just to look it up.
    let transaction_id: <CurrentNetwork as Network>::TransactionID =
        (*ledger.latest_block().transactions().iter().next().expect("genesis has a transaction").id()).into();
    let transaction_height = ledger.latest_height();

    // Captured before any further blocks are advanced -- they're empty, so `latest_block()`
    // wouldn't have any commitments of its own once they've landed.
    let genesis_commitments: Vec<Field<CurrentNetwork>> = ledger.latest_block().commitments().copied().collect();
    assert!(!genesis_commitments.is_empty(), "genesis must have commitments to benchmark against");

    // Deploy a small program, and advance it into the chain, so the program-lookup handlers have
    // something real to find. Paid with a public fee (no fee record): genesis funds every
    // validator's public balance too, and a private fee's change output would need to be a
    // Version 1 record pre-Consensus V8, which only `vm`'s own `#[cfg(test)]` builds produce --
    // not available to an external crate like this one without breaking the real-genesis-loading
    // tests that share this crate's dev-dependency feature set (see `node/rest/Cargo.toml`).
    let program = Program::<CurrentNetwork>::from_str(
        r"
program rest_bench_handlers.aleo;

function hello:
    input r0 as u32.private;
    input r1 as u32.private;
    add r0 r1 into r2;
    output r2 as u32.private;
",
    )
    .unwrap();
    let program_id = *program.id();

    let deploy_tx = ledger.vm().deploy(&private_key, &program, None, 600_000, None, rng).unwrap();
    let block =
        ledger.prepare_advance_to_next_beacon_block(&private_key, vec![], vec![], vec![deploy_tx], rng).unwrap();
    ledger.advance_to_next_block(&block).unwrap();

    // A handful more empty blocks, so `get_block_header`/`find_block_hash`/`get_committee` etc.
    // aren't only ever exercised at height 0 or 1.
    for _ in 0..10 {
        advance_empty_block(&ledger, &private_key, rng);
    }

    ChainFixture { ledger, program_id, transaction_id, transaction_height, genesis_commitments }
}

/// Benchmarks `get_block_header`, `find_block_hash`, `get_state_root`, `get_committee` /
/// `latest_committee`, `get_transaction` / `get_confirmed_transaction`, and
/// `get_delegators_for_validator` -- the point-lookup handlers that read block/committee/
/// transaction state directly off the ledger with no `spawn_blocking` today.
fn bench_block_and_committee_lookups(c: &mut Criterion) {
    let rng = &mut TestRng::default();
    let fixture = build_chain_fixture(rng);
    let ledger = &fixture.ledger;
    let latest_height = ledger.latest_height();

    let mut group = c.benchmark_group("block_and_committee_lookups");

    group.bench_function("get_header", |b| b.iter(|| ledger.get_header(latest_height).unwrap()));
    group.bench_function("find_block_hash", |b| b.iter(|| ledger.find_block_hash(&fixture.transaction_id).unwrap()));
    group.bench_function("get_state_root", |b| b.iter(|| ledger.get_state_root(latest_height).unwrap()));
    group.bench_function("get_committee", |b| b.iter(|| ledger.get_committee(latest_height).unwrap()));
    group.bench_function("latest_committee", |b| b.iter(|| ledger.latest_committee().unwrap()));
    group.bench_function("get_transaction", |b| b.iter(|| ledger.get_transaction(fixture.transaction_id).unwrap()));
    group.bench_function("get_confirmed_transaction", |b| {
        b.iter(|| ledger.get_confirmed_transaction(fixture.transaction_id).unwrap())
    });
    group.bench_function("get_height_from_hash", |b| {
        let hash = ledger.get_hash(fixture.transaction_height).unwrap();
        b.iter(|| ledger.get_height(&hash).unwrap())
    });

    group.finish();
}

/// Benchmarks `get_program`, `get_latest_edition_for_program`, and
/// `get_program_amendment_count`-style lookups against a program that's actually deployed.
fn bench_program_lookups(c: &mut Criterion) {
    let rng = &mut TestRng::default();
    let fixture = build_chain_fixture(rng);
    let ledger = &fixture.ledger;

    let mut group = c.benchmark_group("program_lookups");

    group.bench_function("get_program", |b| b.iter(|| ledger.get_program(fixture.program_id).unwrap()));
    group.bench_function("get_latest_edition_for_program", |b| {
        b.iter(|| ledger.get_latest_edition_for_program(&fixture.program_id).unwrap())
    });

    group.finish();
}

/// Benchmarks `get_state_path_for_commitment` and `get_state_paths_for_commitments` -- Merkle
/// path computation, not just a storage read, and the one route (`get_state_paths_for_commitments`)
/// that today wraps the wrong half of its work in `spawn_blocking`.
fn bench_state_path_lookups(c: &mut Criterion) {
    let rng = &mut TestRng::default();
    let fixture = build_chain_fixture(rng);
    let ledger = &fixture.ledger;
    let available_commitments = &fixture.genesis_commitments;

    let mut group = c.benchmark_group("state_path_lookups");

    group.bench_function("get_state_path_for_commitment", |b| {
        b.iter(|| ledger.get_state_path_for_commitment(&available_commitments[0]).unwrap())
    });

    for &count in COMMITMENT_COUNTS {
        // Cycle through the available commitments to reach larger swept counts: the lookup cost
        // being measured is per-commitment and independent, so repeats are fine here.
        let commitments: Vec<Field<CurrentNetwork>> =
            available_commitments.iter().cycle().take(count).copied().collect();

        group.bench_with_input(
            BenchmarkId::new("get_state_paths_for_commitments", count),
            &commitments,
            |b, commitments| b.iter(|| ledger.get_state_paths_for_commitments(commitments).unwrap()),
        );
    }

    group.finish();
}

/// Benchmarks `get_mapping_value` and `get_mapping_values` against `credits.aleo`'s `bonded`
/// mapping, swept across a few sizes -- the same technique snarkVM's own
/// `ledger/benches/bonded_mapping.rs` uses (there sweeping 10..100_000 entries), to see whether
/// mapping size measurably changes a REST point-lookup's cost.
fn bench_mapping_lookups(c: &mut Criterion) {
    let rng = &mut TestRng::default();
    let (ledger, _private_key, _records) = fresh_ledger(rng);

    let credits_program_id = ProgramID::<CurrentNetwork>::from_str("credits.aleo").unwrap();
    let bonded_mapping = Identifier::<CurrentNetwork>::from_str("bonded").unwrap();
    let validator_identifier = Identifier::<CurrentNetwork>::from_str("validator").unwrap();
    let microcredits_identifier = Identifier::<CurrentNetwork>::from_str("microcredits").unwrap();

    let validator_address =
        Address::<CurrentNetwork>::try_from(&PrivateKey::<CurrentNetwork>::new(rng).unwrap()).unwrap();

    // A key known to be present at every swept size (inserted first, never dropped as later
    // sizes only add more entries after it).
    let key_private_key = PrivateKey::<CurrentNetwork>::new(rng).unwrap();
    let key_address = Address::<CurrentNetwork>::try_from(&key_private_key).unwrap();
    let key = Plaintext::from(Literal::Address(key_address));

    let mut group = c.benchmark_group("mapping_lookups");

    for &size in MAPPING_SIZES {
        let mut entries: Vec<(Plaintext<CurrentNetwork>, Value<CurrentNetwork>)> = Vec::with_capacity(size);
        entries.push((
            key.clone(),
            Value::Plaintext(Plaintext::Struct(
                indexmap::indexmap! {
                    validator_identifier => Plaintext::from(Literal::Address(validator_address)),
                    microcredits_identifier => Plaintext::from(Literal::U64(U64::new(1_000_000))),
                },
                Default::default(),
            )),
        ));
        for i in 1..size as u64 {
            let staker_address =
                Address::<CurrentNetwork>::try_from(&PrivateKey::try_from(Field::from_u64(i)).unwrap()).unwrap();
            let bonded_state = indexmap::indexmap! {
                validator_identifier => Plaintext::from(Literal::Address(validator_address)),
                microcredits_identifier => Plaintext::from(Literal::U64(U64::new(1_000_000))),
            };
            entries.push((
                Plaintext::from(Literal::Address(staker_address)),
                Value::Plaintext(Plaintext::Struct(bonded_state, Default::default())),
            ));
        }
        ledger.vm().finalize_store().replace_mapping(credits_program_id, bonded_mapping, entries).unwrap();

        group.bench_with_input(BenchmarkId::new("get_mapping_value", size), &size, |b, _| {
            b.iter(|| {
                ledger.vm().finalize_store().get_value_confirmed(credits_program_id, bonded_mapping, &key).unwrap()
            })
        });
        group.bench_with_input(BenchmarkId::new("get_mapping_values", size), &size, |b, _| {
            b.iter(|| ledger.vm().finalize_store().get_mapping_confirmed(credits_program_id, bonded_mapping).unwrap())
        });
    }

    group.finish();
}

/// Advances `ledger` to the latest consensus version with empty blocks.
///
/// A ledger at genesis is at `ConsensusVersion::V1`, which verifies with Varuna V1, a verifier the
/// network no longer runs. Under `test_consensus_heights` each version from V2 onward lasts one
/// block, so only the latest version stays put across the blocks a benchmark adds.
#[cfg(feature = "test_consensus_heights")]
fn advance_to_latest(
    ledger: &Ledger<CurrentNetwork, CurrentStorage>,
    private_key: &PrivateKey<CurrentNetwork>,
    rng: &mut TestRng,
) {
    let latest = ConsensusVersion::latest();
    let target = CurrentNetwork::CONSENSUS_HEIGHT(latest).unwrap();
    while ledger.latest_height() < target {
        let block = ledger.prepare_advance_to_next_beacon_block(private_key, vec![], vec![], vec![], rng).unwrap();
        ledger.advance_to_next_block(&block).unwrap();
    }
    let consensus_version = CurrentNetwork::CONSENSUS_VERSION(ledger.latest_height()).unwrap();
    assert_eq!(consensus_version, latest, "height {} is {consensus_version:?}", ledger.latest_height());
    eprintln!(
        "transaction_verification: height {}, {consensus_version:?}, Varuna {:?}",
        ledger.latest_height(),
        snarkvm::console::network::varuna_version_from_consensus(consensus_version)
    );
}

/// Benchmarks `check_transaction_basic` (called from `transaction_broadcast`) for a deploy and an
/// execute transaction, mirroring snarkVM's `cached_verify` benches in `ledger/benches/transaction.rs`
/// -- `Ledger::check_transaction_basic` forwards to `vm.check_transaction`. Each ledger is first
/// advanced to the latest consensus version, so the verifier is the one the network runs.
///
/// `vm.check_transaction` caches each transaction it accepts and skips proof verification on a
/// repeat, so after the first iteration these benches time a cache hit, not verification. snarkVM's
/// `ledger/benches/verify_transaction.rs` times verification itself.
///
/// Each transaction is built fresh, checked, but never applied to its ledger. Both pay with a
/// public fee, not a fee record -- see the comment on the equivalent deploy in
/// `build_chain_fixture` for why.
#[cfg(feature = "test_consensus_heights")]
fn bench_transaction_verification(c: &mut Criterion) {
    let mut group = c.benchmark_group("transaction_verification");

    {
        let rng = &mut TestRng::default();
        let (ledger, private_key, _records) = fresh_ledger(rng);
        advance_to_latest(&ledger, &private_key, rng);
        let program = Program::<CurrentNetwork>::from_str(
            r"
program rest_bench_verify_deploy.aleo;

constructor:
    assert.eq edition 0u16;

function hello:
    input r0 as u32.private;
    input r1 as u32.private;
    add r0 r1 into r2;
    output r2 as u32.private;
",
        )
        .unwrap();
        let deploy_tx = ledger.vm().deploy(&private_key, &program, None, 600_000, None, rng).unwrap();

        group.bench_function("check_transaction_basic(deploy) - cached_verify", |b| {
            b.iter(|| ledger.check_transaction_basic(&deploy_tx, None, rng).unwrap())
        });
    }

    {
        let rng = &mut TestRng::default();
        let (ledger, private_key, _records) = fresh_ledger(rng);
        advance_to_latest(&ledger, &private_key, rng);
        let address = Address::try_from(&private_key).unwrap();

        let inputs = [
            Value::<CurrentNetwork>::from_str(&address.to_string()).unwrap(),
            Value::<CurrentNetwork>::from_str("1u64").unwrap(),
        ]
        .into_iter();
        let execute_authorization =
            ledger.vm().authorize(&private_key, "credits.aleo", "transfer_public", inputs, rng).unwrap();
        let execution_id = execute_authorization.to_execution_id().unwrap();
        let fee_authorization =
            ledger.vm().authorize_fee_public(&private_key, 300_000, 1_000, execution_id, rng).unwrap();
        let execute_tx = ledger
            .vm()
            .execute_authorization(execute_authorization.replicate(), Some(fee_authorization.replicate()), None, rng)
            .unwrap();

        group.bench_function("check_transaction_basic(execute transfer_public) - cached_verify", |b| {
            b.iter(|| ledger.check_transaction_basic(&execute_tx, None, rng).unwrap())
        });
    }

    group.finish();
}

/// Benchmarks `db_backup`'s RocksDB-checkpoint call (`Ledger::backup_database`) -- whole-DB
/// filesystem I/O. Needs a real on-disk ledger: `backup_database` is a RocksDB checkpoint, which
/// is meaningless against the in-memory storage every other group uses.
fn bench_db_backup(c: &mut Criterion) {
    let rng = &mut TestRng::default();
    let private_key = PrivateKey::<CurrentNetwork>::new(rng).unwrap();
    let (genesis, _records) = genesis_with_records(&private_key, rng);
    let ledger = Ledger::<CurrentNetwork, DbStorage>::load(genesis, StorageMode::new_test(None)).unwrap();

    let mut group = c.benchmark_group("db_backup");
    group.bench_function("backup_database", |b| {
        b.iter_batched(
            || {
                // The checkpoint path must not exist yet -- `backup_database` creates it.
                let suffix = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .expect("system clock is before the epoch")
                    .as_nanos();
                std::env::temp_dir().join(format!("snarkos-rest-bench-backup-{}-{suffix}", std::process::id()))
            },
            |dir| {
                ledger.backup_database(&dir).unwrap();
                std::fs::remove_dir_all(&dir).ok();
            },
            criterion::BatchSize::LargeInput,
        )
    });
    group.finish();
}

criterion_group!(
    handlers,
    bench_block_and_committee_lookups,
    bench_program_lookups,
    bench_state_path_lookups,
    bench_mapping_lookups,
    bench_db_backup,
);
#[cfg(feature = "test_consensus_heights")]
criterion_group!(verification, bench_transaction_verification);

#[cfg(feature = "test_consensus_heights")]
criterion_main!(handlers, verification);
#[cfg(not(feature = "test_consensus_heights"))]
criterion_main!(handlers);
