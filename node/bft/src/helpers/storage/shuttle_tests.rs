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

//! Tests of [`Storage`] under the `shuttle` scheduler; see `snarkos_utilities::locks`.
//! Run them with `cargo shuttle-test -p snarkos-node-bft --lib shuttle_tests`.
//!
//! Each test samples its certificates once. It then runs its closure many times, each time on a
//! new storage and with another order of the steps of its threads; see [`check`]. A failure
//! prints the schedule that replays it, which `shuttle::replay` takes. The random scheduler also
//! prints its seed, which `SHUTTLE_RANDOM_SEED` takes.
//!
//! Hazard: shuttle switches threads only at the types that the cfg swaps. A thread that samples
//! or signs inside a closure makes each run slow and tests nothing more.

use super::{tests::sample_transmissions, *};
use snarkos_node_bft_ledger_service::MockLedgerService;
use snarkos_node_bft_storage_service::BFTMemoryService;
use snarkos_utilities::shuttle::{self, thread};
use snarkvm::{
    ledger::{
        committee::{Committee, test_helpers::sample_committee_and_keys_for_round},
        narwhal::batch_certificate::test_helpers::sample_batch_certificate_for_round_with_committee,
    },
    prelude::{PrivateKey, TestRng},
};

type CurrentNetwork = snarkvm::prelude::MainnetV0;
type Certificate = BatchCertificate<CurrentNetwork>;
type Transmissions = HashMap<TransmissionID<CurrentNetwork>, Transmission<CurrentNetwork>>;
type InsertResult = Result<(), CheckCertificateError>;

/// The number of thread orders that each test of this module runs with each scheduler.
const ITERATIONS: usize = 5_000;

/// The depth of the PCT scheduler, which changes the priorities of the threads at
/// `PCT_DEPTH - 1` points.
const PCT_DEPTH: usize = 3;

/// The number of committee members, which is the smallest one that tolerates a fault.
const NUM_MEMBERS: usize = 4;

/// A committee, and the means to sample the certificates of its members.
struct Fixture {
    committee: Committee<CurrentNetwork>,
    private_keys: Vec<PrivateKey<CurrentNetwork>>,
    rng: TestRng,
}

impl Fixture {
    fn new() -> Self {
        let mut rng = TestRng::default();
        let (committee, private_keys) = sample_committee_and_keys_for_round(0, NUM_MEMBERS, &mut rng);
        Self { committee, private_keys, rng }
    }

    /// Samples a certificate that the member `author` proposes in `round` and that every other
    /// member signs, with its transmissions.
    fn certificate(
        &mut self,
        author: usize,
        round: u64,
        previous_certificate_ids: IndexSet<Field<CurrentNetwork>>,
    ) -> (Certificate, Transmissions) {
        let author = self.private_keys[author];
        let signers: Vec<_> = self.private_keys.iter().copied().filter(|key| *key != author).collect();
        let certificate = sample_batch_certificate_for_round_with_committee(
            round,
            previous_certificate_ids,
            &author,
            &signers,
            &mut self.rng,
        );
        let (transmissions, _) = sample_transmissions(&certificate, &mut self.rng);
        (certificate, transmissions)
    }
}

/// Runs `test` under two schedulers, each with new seeds and for `iterations` thread orders.
///
/// The PCT scheduler runs one thread until a random point, and then prefers another one. It
/// finds the orders in which one thread stops between two particular steps while another one
/// runs to its end. The random scheduler picks a thread at every step, so its threads advance
/// together, and it almost never produces those orders.
pub(crate) fn check(iterations: usize, test: impl Fn() + Send + Sync + 'static) {
    let test = Arc::new(test);
    let pct_test = test.clone();
    shuttle::check_pct(move || pct_test(), iterations, PCT_DEPTH);
    shuttle::check_random(move || test(), iterations);
}

/// Returns an empty storage whose GC round is 0.
fn new_storage(committee: &Committee<CurrentNetwork>, max_gc_rounds: u64) -> Storage<CurrentNetwork> {
    let ledger = Arc::new(MockLedgerService::new(committee.clone()));
    Storage::new(ledger, Arc::new(BFTMemoryService::new()), max_gc_rounds).unwrap()
}

/// Starts a thread that inserts the certificate the way the primary does.
fn spawn_insert(
    storage: &Storage<CurrentNetwork>,
    (certificate, transmissions): &(Certificate, Transmissions),
) -> thread::JoinHandle<InsertResult> {
    let (storage, certificate, transmissions) = (storage.clone(), certificate.clone(), transmissions.clone());
    thread::spawn(move || storage.insert_certificate(certificate, transmissions, Default::default()))
}

/// Starts a thread that collects the garbage of the rounds that are out of range in `round`.
fn spawn_garbage_collection(storage: &Storage<CurrentNetwork>, round: u64) -> thread::JoinHandle<()> {
    let storage = storage.clone();
    thread::spawn(move || storage.garbage_collect_certificates(round).unwrap())
}

/// Starts a thread that reads the certificate maps while the other threads change them.
fn spawn_reader(storage: &Storage<CurrentNetwork>) -> thread::JoinHandle<()> {
    let storage = storage.clone();
    thread::spawn(move || storage.assert_maps_agree())
}

/// Asserts that one of the two insertions stored its certificate, and that the other one failed
/// with an error that `is_expected`.
fn assert_one_inserted(results: &[InsertResult; 2], is_expected: impl Fn(&CheckCertificateError) -> bool) {
    let is_one_inserted = match results {
        [Ok(()), Err(error)] | [Err(error), Ok(())] => is_expected(error),
        _ => false,
    };
    assert!(is_one_inserted, "{results:?}");
}

/// Asserts what must hold once no thread changes the storage: the maps agree, no round at or
/// below the GC round is stored, and the transmissions are the ones of the stored certificates.
fn assert_at_rest(storage: &Storage<CurrentNetwork>) {
    storage.assert_maps_agree();

    let gc_round = storage.gc_round();
    for (round, _) in storage.rounds_iter() {
        assert!(round > gc_round, "round {round} is stored at GC round {gc_round}");
    }

    let transmissions: HashMap<_, _> = storage.transmissions_iter().collect();
    for (transmission_id, (_, certificate_ids)) in &transmissions {
        assert!(!certificate_ids.is_empty(), "{transmission_id} belongs to no certificate");
        for certificate_id in certificate_ids {
            assert!(
                storage.contains_certificate(*certificate_id),
                "{transmission_id} belongs to a missing certificate"
            );
        }
    }
    for (certificate_id, certificate) in storage.certificates_iter() {
        for transmission_id in certificate.transmission_ids() {
            let stored = transmissions.get(transmission_id).is_some_and(|(_, ids)| ids.contains(&certificate_id));
            assert!(stored, "{transmission_id} of certificate {certificate_id} is missing");
        }
    }
}

/// Two tasks, such as the primary and the block sync, insert the same certificate.
/// One of them stores it, and the other one learns that it is stored.
#[test]
fn same_certificate_inserted_twice() {
    let mut fixture = Fixture::new();
    let certificate = fixture.certificate(0, 1, Default::default());
    let committee = fixture.committee;

    check(ITERATIONS, move || {
        let storage = new_storage(&committee, 1);

        let inserts = [spawn_insert(&storage, &certificate), spawn_insert(&storage, &certificate)];
        let reader = spawn_reader(&storage);
        let results = inserts.map(|insert| insert.join().unwrap());
        reader.join().unwrap();

        assert_one_inserted(&results, |error| matches!(error, CheckCertificateError::SameCertificate { .. }));
        assert!(storage.contains_certificate(certificate.0.id()));
        assert_at_rest(&storage);
    });
}

/// Two tasks insert different certificates of one author for one round.
/// The storage keeps one of them, and nothing of the other one.
#[test]
fn two_certificates_of_one_author_and_round() {
    let mut fixture = Fixture::new();
    let certificates = [fixture.certificate(0, 1, Default::default()), fixture.certificate(0, 1, Default::default())];
    assert_ne!(certificates[0].0.id(), certificates[1].0.id());
    let committee = fixture.committee;

    check(ITERATIONS, move || {
        let storage = new_storage(&committee, 1);

        let inserts = [spawn_insert(&storage, &certificates[0]), spawn_insert(&storage, &certificates[1])];
        let reader = spawn_reader(&storage);
        let results = inserts.map(|insert| insert.join().unwrap());
        reader.join().unwrap();

        assert_one_inserted(&results, |error| matches!(error, CheckCertificateError::SameAuthorAndRound { .. }));
        for ((certificate, _), result) in certificates.iter().zip(&results) {
            assert_eq!(storage.contains_certificate(certificate.id()), result.is_ok());
        }
        assert_at_rest(&storage);
    });
}

/// A task inserts a certificate while garbage collection passes its round.
/// The storage does not keep the certificate, whichever of the two comes first.
#[test]
fn insertion_during_garbage_collection_of_its_round() {
    let mut fixture = Fixture::new();
    let certificate = fixture.certificate(0, 1, Default::default());
    let committee = fixture.committee;

    check(ITERATIONS, move || {
        let storage = new_storage(&committee, 1);

        let insert = spawn_insert(&storage, &certificate);
        let garbage_collection = spawn_garbage_collection(&storage, 2);
        let reader = spawn_reader(&storage);
        let result = insert.join().unwrap();
        garbage_collection.join().unwrap();
        reader.join().unwrap();

        assert!(matches!(result, Ok(()) | Err(CheckCertificateError::RoundTooLow { .. })), "{result:?}");
        assert_eq!(storage.gc_round(), 1);
        assert!(!storage.contains_certificate(certificate.0.id()));
        assert_eq!(storage.transmissions_iter().count(), 0);
        assert_at_rest(&storage);
    });
}

/// A task inserts a certificate while garbage collection removes the round before it, which holds
/// the certificates that it refers to. The certificate is above the GC round, so it is stored.
#[test]
#[ignore = "fails: the insertion checks the previous certificates against a GC round that it read before the sweep"]
fn insertion_during_garbage_collection_of_the_previous_round() {
    let mut fixture = Fixture::new();
    let previous_certificates: Vec<_> =
        (0..NUM_MEMBERS).map(|author| fixture.certificate(author, 1, Default::default())).collect();
    let previous_certificate_ids = previous_certificates.iter().map(|(certificate, _)| certificate.id()).collect();
    let certificate = fixture.certificate(0, 2, previous_certificate_ids);
    let committee = fixture.committee;

    check(ITERATIONS, move || {
        let storage = new_storage(&committee, 1);
        for (previous_certificate, transmissions) in &previous_certificates {
            storage
                .insert_certificate(previous_certificate.clone(), transmissions.clone(), Default::default())
                .unwrap();
        }

        let insert = spawn_insert(&storage, &certificate);
        let garbage_collection = spawn_garbage_collection(&storage, 2);
        let reader = spawn_reader(&storage);
        let result = insert.join().unwrap();
        garbage_collection.join().unwrap();
        reader.join().unwrap();

        assert!(result.is_ok(), "{result:?}");
        assert_eq!(storage.gc_round(), 1);
        assert_eq!(storage.rounds_iter().map(|(round, _)| round).collect::<Vec<_>>(), [2]);
        assert_at_rest(&storage);
    });
}
